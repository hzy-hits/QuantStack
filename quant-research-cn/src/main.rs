#![recursion_limit = "256"]

mod analytics;
mod config;
mod enrichment;
mod fetcher;
mod filtering;
mod reporting;
mod storage;

use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate};
use clap::{Parser, Subcommand};
use duckdb::Connection;
use tracing::info;

#[derive(Parser)]
#[command(name = "quant-cn", about = "A-share quantitative research pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(long, default_value = "config.yaml")]
    config: String,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize database schema and fetch historical data
    Init {
        /// How many years of history to backfill (default: 2)
        #[arg(long, default_value = "2")]
        years: u32,
        /// Also backfill moneyflow (slower, ~30 min for 2yr)
        #[arg(long)]
        with_flow: bool,
    },
    /// Run full daily pipeline: fetch → analytics → filter → render
    Run {
        #[arg(long)]
        date: Option<String>,
        #[arg(long)]
        skip_fetch: bool,
    },
    /// Fetch data only (no analytics)
    Fetch {
        #[arg(long)]
        date: Option<String>,
    },
    /// Run analytics only (assumes data is fresh)
    Analyze {
        #[arg(long)]
        date: Option<String>,
        #[arg(long)]
        module: Option<String>,
    },
    /// Enrich news with DeepSeek (structured extraction)
    Enrich {
        #[arg(long)]
        date: Option<String>,
    },
    /// Render payload from existing analytics
    Render {
        #[arg(long)]
        date: Option<String>,
    },
    /// Refresh review ledger from existing report snapshot without rerunning analytics
    ReviewBackfill {
        #[arg(long)]
        date_from: Option<String>,
        #[arg(long)]
        date_to: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("quant_cn=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let cfg = config::Settings::load(&cli.config)?;

    match cli.command {
        Command::Init { years, with_flow } => {
            info!("initializing database and fetching historical data");
            let db = storage::open(cfg.data.raw_path())?;
            storage::init_schema(&db)?;

            let client = fetcher::http::build_client()?;
            let token = &cfg.api.tushare_token;

            // Build universe first
            let symbols = fetcher::tushare::universe::fetch_universe(&client, token, &cfg).await?;
            info!(symbols = symbols.len(), "universe loaded for backfill");

            // Backfill prices + daily_basic (by symbol — fast)
            let today = config::resolve_date(None)?;
            let start = today - chrono::Duration::days(365 * years as i64);
            info!(%start, %today, years, symbols = symbols.len(), "starting historical backfill");

            let n = fetcher::tushare::prices::backfill_history(
                &client, token, &db, &symbols, start, today,
            )
            .await?;
            info!(rows = n, "prices + daily_basic backfill done");

            // Backfill index prices (000300.SH, 000016.SH, etc.)
            let ni =
                fetcher::tushare::prices::backfill_index(&client, token, &db, start, today).await?;
            info!(rows = ni, "index prices backfill done");

            // Backfill margin_detail (融资融券) — last 60 days for z-score baseline
            let margin_start = today - chrono::Duration::days(60);
            let nm =
                fetcher::tushare::prices::backfill_margin(&client, token, &db, margin_start, today)
                    .await?;
            info!(rows = nm, "margin_detail backfill done");

            // Backfill Shibor (daily rates) — last 180 days
            let shibor_start = today - chrono::Duration::days(180);
            let ns = fetcher::tushare::macro_cn::backfill_shibor(
                &client,
                token,
                &db,
                shibor_start,
                today,
            )
            .await?;
            info!(rows = ns, "shibor backfill done");

            // Fetch LPR + monthly macro indicators
            fetcher::tushare::macro_cn::fetch_lpr(&client, token, &db, today).await?;
            let macro_series: Vec<(String, String)> = cfg
                .r#macro
                .series
                .iter()
                .map(|s| (s.id.clone(), s.name.clone()))
                .collect();
            fetcher::tushare::macro_cn::fetch_macro_indicators(&client, token, &db, &macro_series)
                .await?;

            // Optional: backfill moneyflow
            if with_flow {
                // Moneyflow backfill — only last 6 months (flow data is large)
                let flow_start = today - chrono::Duration::days(180);
                let nf = fetcher::tushare::prices::backfill_moneyflow(
                    &client, token, &db, flow_start, today,
                )
                .await?;
                info!(rows = nf, "moneyflow backfill done");
            }

            info!("init complete");
        }
        Command::Run { date, skip_fetch } => {
            let as_of = config::resolve_date(date.as_deref())?;
            info!(%as_of, "pipeline start");

            if !skip_fetch {
                {
                    let raw_db = storage::open(cfg.data.raw_path())?;
                    let (tushare_rows, akshare_rows) = tokio::join!(
                        fetcher::tushare::fetch_all(&raw_db, &cfg, as_of),
                        fetcher::akshare::fetch_all(&raw_db, &cfg, as_of),
                    );
                    info!(tushare = ?tushare_rows, akshare = ?akshare_rows, "fetch complete");
                }
            }

            stage_raw_to_research(&cfg)?;

            {
                let research_db = storage::open(cfg.data.research_path())?;
                let n_enriched = enrichment::news::enrich_news(&research_db, &cfg, as_of).await?;
                let n_themes = enrichment::themes::enrich_themes(&research_db, &cfg, as_of).await?;
                info!(
                    enriched = n_enriched,
                    themes = n_themes,
                    "deepseek enrichment done"
                );

                analytics::run_all(&research_db, &cfg, as_of)?;
                let shadow_full_rows =
                    filtering::materialize_shadow_full(&research_db, &cfg, as_of)?;
                analytics::run_module(
                    &research_db,
                    &cfg,
                    as_of,
                    "shadow_option_alpha_calibration",
                )?;
                info!("analytics complete");
                info!(
                    rows = shadow_full_rows,
                    "shadow_full shortlist pricing complete"
                );
            }

            promote_research_to_report(&cfg)?;

            {
                let report_db = storage::open(cfg.data.report_path())?;
                let notable = filtering::build_notable_items(&report_db, &cfg, as_of)?;
                info!(notable = notable.len(), "filter complete");
                let reviewed = analytics::report_review::materialize_report_review(
                    &report_db, &cfg, as_of, &notable,
                )?;
                info!(rows = reviewed, "report review ledger refreshed");
                report_db.execute_batch("CHECKPOINT")?;
                let path = reporting::render_payload(&report_db, &cfg, as_of, &notable)?;
                info!(%path, "payload ready");
            }
        }
        Command::Fetch { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let raw_db = storage::open(cfg.data.raw_path())?;
            let (t, a) = tokio::join!(
                fetcher::tushare::fetch_all(&raw_db, &cfg, as_of),
                fetcher::akshare::fetch_all(&raw_db, &cfg, as_of),
            );
            info!(tushare = ?t, akshare = ?a, "fetch complete");
        }
        Command::Analyze { date, module } => {
            let as_of = config::resolve_date(date.as_deref())?;
            if let Some(module_name) = module.as_deref() {
                if matches!(module_name, "algorithm_postmortem" | "algorithm_review") {
                    ensure_report_snapshot(&cfg)?;
                    let report_db = storage::open(cfg.data.report_path())?;
                    analytics::run_module(&report_db, &cfg, as_of, module_name)?;
                    report_db.execute_batch("CHECKPOINT")?;
                    info!("analytics complete");
                    return Ok(());
                }
                if module_name == "shadow_full" {
                    prepare_incremental_research(&cfg)?;
                    let research_db = storage::open(cfg.data.research_path())?;
                    let rows = filtering::materialize_shadow_full(&research_db, &cfg, as_of)?;
                    research_db.execute_batch("CHECKPOINT")?;
                    promote_research_to_report(&cfg)?;
                    info!(rows = rows, "shadow_full shortlist pricing complete");
                    info!("analytics complete");
                    return Ok(());
                }
                prepare_incremental_research(&cfg)?;
                let research_db = storage::open(cfg.data.research_path())?;
                let shadow_full_rows = if matches!(
                    module_name,
                    "shadow_option_alpha_calibration" | "shadow_option_alpha"
                ) {
                    let rows = filtering::materialize_shadow_full(&research_db, &cfg, as_of)?;
                    analytics::run_module(&research_db, &cfg, as_of, module_name)?;
                    rows
                } else {
                    analytics::run_module(&research_db, &cfg, as_of, module_name)?;
                    filtering::materialize_shadow_full(&research_db, &cfg, as_of)?
                };
                info!(
                    rows = shadow_full_rows,
                    "shadow_full shortlist pricing complete"
                );
            } else {
                stage_raw_to_research(&cfg)?;
                let research_db = storage::open(cfg.data.research_path())?;
                analytics::run_all(&research_db, &cfg, as_of)?;
                let shadow_full_rows =
                    filtering::materialize_shadow_full(&research_db, &cfg, as_of)?;
                analytics::run_module(
                    &research_db,
                    &cfg,
                    as_of,
                    "shadow_option_alpha_calibration",
                )?;
                info!(
                    rows = shadow_full_rows,
                    "shadow_full shortlist pricing complete"
                );
            }
            promote_research_to_report(&cfg)?;
            info!("analytics complete");
        }
        Command::Enrich { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            prepare_incremental_research(&cfg)?;
            let research_db = storage::open(cfg.data.research_path())?;
            let n = enrichment::news::enrich_news(&research_db, &cfg, as_of).await?;
            let t = enrichment::themes::enrich_themes(&research_db, &cfg, as_of).await?;
            drop(research_db);
            promote_research_to_report(&cfg)?;
            info!(enriched = n, themes = t, "enrichment complete");
        }
        Command::Render { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            ensure_report_snapshot(&cfg)?;
            let report_db = storage::open(cfg.data.report_path())?;
            let notable = filtering::build_notable_items(&report_db, &cfg, as_of)?;
            let reviewed = analytics::report_review::materialize_report_review(
                &report_db, &cfg, as_of, &notable,
            )?;
            info!(rows = reviewed, "report review ledger refreshed");
            report_db.execute_batch("CHECKPOINT")?;
            let path = reporting::render_payload(&report_db, &cfg, as_of, &notable)?;
            info!(%path, "payload ready");
        }
        Command::ReviewBackfill { date_from, date_to } => {
            let end_date = config::resolve_date(date_to.as_deref())?;
            let start_date = match date_from.as_deref() {
                Some(raw) => config::resolve_date(Some(raw))?,
                None => end_date - Duration::days(45),
            };
            ensure_report_snapshot(&cfg)?;
            let report_db = storage::open(cfg.data.report_path())?;
            let review_dates = load_review_dates(&report_db, start_date, end_date)?;
            let mut refreshed = 0usize;
            for review_date in review_dates {
                ensure_review_backfill_analytics(&report_db, &cfg, review_date)?;
                let notable = filtering::build_notable_items(&report_db, &cfg, review_date)?;
                let rows = analytics::report_review::materialize_report_review(
                    &report_db,
                    &cfg,
                    review_date,
                    &notable,
                )?;
                refreshed += rows;
                info!(%review_date, rows, "review ledger refreshed");
            }
            report_db.execute_batch("CHECKPOINT")?;
            info!(
                %start_date,
                %end_date,
                refreshed,
                "review backfill complete"
            );
        }
    }

    Ok(())
}

fn load_review_dates(
    db: &Connection,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<NaiveDate>> {
    let mut stmt = db.prepare(
        "SELECT DISTINCT CAST(trade_date AS VARCHAR)
         FROM prices
         WHERE trade_date >= CAST(? AS DATE)
           AND trade_date <= CAST(? AS DATE)
         ORDER BY trade_date",
    )?;
    let rows = stmt.query_map(
        duckdb::params![start_date.to_string(), end_date.to_string()],
        |row| row.get::<_, String>(0),
    )?;

    let mut dates = Vec::new();
    for row in rows {
        let raw = row?;
        let trimmed = raw.trim();
        let date_part = trimmed.get(0..10).unwrap_or(trimmed);
        dates.push(NaiveDate::parse_from_str(date_part, "%Y-%m-%d")?);
    }
    Ok(dates)
}

fn ensure_review_backfill_analytics(
    db: &Connection,
    cfg: &config::Settings,
    as_of: NaiveDate,
) -> Result<()> {
    for module in [
        "flow_audit",
        "setup_alpha",
        "continuation_vs_fade",
        "limit_move_radar",
        "open_execution_gate",
    ] {
        if analytics_module_rows(db, as_of, module)? == 0 {
            analytics::run_module(db, cfg, as_of, module)?;
        }
    }
    Ok(())
}

fn analytics_module_rows(db: &Connection, as_of: NaiveDate, module: &str) -> Result<i64> {
    db.query_row(
        "SELECT COUNT(*) FROM analytics WHERE as_of = CAST(? AS DATE) AND module = ?",
        duckdb::params![as_of.to_string(), module],
        |row| row.get::<_, i64>(0),
    )
    .map_err(Into::into)
}

fn stage_raw_to_research(cfg: &config::Settings) -> Result<()> {
    if !storage::exists(cfg.data.raw_path()) {
        anyhow::bail!(
            "raw database not found at {}. Run `quant-cn fetch` or `quant-cn init` first.",
            cfg.data.raw_path()
        );
    }
    storage::copy_database(cfg.data.raw_path(), cfg.data.research_path()).with_context(|| {
        format!(
            "failed to stage raw snapshot from {} to {}",
            cfg.data.raw_path(),
            cfg.data.research_path()
        )
    })?;
    storage::restore_report_review_history(cfg.data.report_path(), cfg.data.research_path())
        .with_context(|| {
            format!(
                "failed to restore report review history from {} into {}",
                cfg.data.report_path(),
                cfg.data.research_path()
            )
        })?;
    info!(
        raw = cfg.data.raw_path(),
        research = cfg.data.research_path(),
        "raw snapshot staged into research db"
    );
    Ok(())
}

fn prepare_incremental_research(cfg: &config::Settings) -> Result<()> {
    if storage::exists(cfg.data.research_path()) {
        storage::restore_report_review_history(cfg.data.report_path(), cfg.data.research_path())
            .with_context(|| {
                format!(
                    "failed to restore report review history from {} into existing research snapshot {}",
                    cfg.data.report_path(),
                    cfg.data.research_path()
                )
            })?;
        info!(
            research = cfg.data.research_path(),
            "reusing existing research snapshot for incremental analysis with report review history"
        );
        return Ok(());
    }

    if storage::exists(cfg.data.report_path()) {
        storage::copy_database(cfg.data.report_path(), cfg.data.research_path()).with_context(
            || {
                format!(
                    "failed to seed research snapshot from report snapshot {} -> {}",
                    cfg.data.report_path(),
                    cfg.data.research_path()
                )
            },
        )?;
        info!(
            report = cfg.data.report_path(),
            research = cfg.data.research_path(),
            "report snapshot seeded into research db for incremental analysis"
        );
        return Ok(());
    }

    stage_raw_to_research(cfg)
}

fn promote_research_to_report(cfg: &config::Settings) -> Result<()> {
    if !storage::exists(cfg.data.research_path()) {
        anyhow::bail!(
            "research database not found at {}. Run `quant-cn analyze` or `quant-cn run` first.",
            cfg.data.research_path()
        );
    }
    storage::copy_database(cfg.data.research_path(), cfg.data.report_path()).with_context(
        || {
            format!(
                "failed to promote report snapshot from {} to {}",
                cfg.data.research_path(),
                cfg.data.report_path()
            )
        },
    )?;
    info!(
        research = cfg.data.research_path(),
        report = cfg.data.report_path(),
        "research snapshot promoted to report db"
    );
    Ok(())
}

fn ensure_report_snapshot(cfg: &config::Settings) -> Result<()> {
    if storage::exists(cfg.data.report_path()) {
        return Ok(());
    }
    if storage::exists(cfg.data.research_path()) {
        return promote_research_to_report(cfg);
    }
    anyhow::bail!(
        "report snapshot not found at {} and research snapshot not found at {}. Run `quant-cn analyze` or `quant-cn run` first.",
        cfg.data.report_path(),
        cfg.data.research_path()
    )
}

#[cfg(test)]
mod tests {
    use super::prepare_incremental_research;
    use crate::{config::Settings, storage};
    use anyhow::Result;
    use duckdb::params;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("quant-cn-{name}-{nonce}"))
    }

    #[test]
    fn prepare_incremental_research_preserves_existing_research_snapshot() -> Result<()> {
        let root = temp_test_dir("enrich-preserve-research");
        fs::create_dir_all(&root)?;

        let raw_path = root.join("raw.duckdb");
        let research_path = root.join("research.duckdb");
        let report_path = root.join("report.duckdb");
        fs::write(&raw_path, b"raw-snapshot")?;
        fs::write(&research_path, b"research-snapshot")?;

        let config_path = root.join("config.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
api:
  tushare_token: ""
runtime:
  timezone: Asia/Shanghai
  random_seed: 1
universe:
  benchmark: 000300.SH
  scan:
    csi300: true
    csi500: false
    csi1000: false
    sse50: false
  asset_classes:
    sector_etfs: false
    bond_etfs: false
    commodity_etfs: false
    cross_border: false
  filters:
    min_avg_volume_shares: 0
    min_price: 0.0
output:
  max_notable_items: 10
  min_notable_items: 1
data:
  raw_db_path: "{raw}"
  research_db_path: "{research}"
  report_db_path: "{report}"
  dev_db_path: "{dev}"
  constituent_refresh_days: 7
signals:
  momentum_windows: [5, 20]
  atr_period: 14
reporting:
  anthropic_model: test
  anthropic_temperature: 0.0
  max_tokens: 512
"#,
                raw = raw_path.display(),
                research = research_path.display(),
                report = report_path.display(),
                dev = root.join("dev.duckdb").display(),
            ),
        )?;

        let cfg = Settings::load(config_path.to_str().expect("valid config path"))?;
        prepare_incremental_research(&cfg)?;

        assert_eq!(fs::read(&research_path)?, b"research-snapshot");
        assert_eq!(fs::read(&raw_path)?, b"raw-snapshot");

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn prepare_incremental_research_restores_review_history_into_existing_research() -> Result<()> {
        let root = temp_test_dir("enrich-restore-review-history");
        fs::create_dir_all(&root)?;

        let raw_path = root.join("raw.duckdb");
        let research_path = root.join("research.duckdb");
        let report_path = root.join("report.duckdb");
        let dev_path = root.join("dev.duckdb");
        fs::write(&raw_path, b"raw-snapshot")?;

        let research_db = storage::open(research_path.to_str().expect("valid research path"))?;
        research_db.execute_batch("CHECKPOINT")?;
        drop(research_db);

        let report_db = storage::open(report_path.to_str().expect("valid report path"))?;
        report_db.execute(
            "INSERT INTO report_decisions (
                report_date, session, symbol, selection_status, rank_order,
                report_bucket, signal_direction, signal_confidence, composite_score,
                execution_mode, execution_score, max_chase_gap_pct, pullback_trigger_pct,
                setup_score, continuation_score, fade_risk, reference_close, details_json
             ) VALUES (
                CAST('2026-04-24' AS DATE), 'am', '603444.SH', 'selected', 1,
                'core', 'long', 'high', 0.8, 'trade', 0.7, 2.0, -1.5,
                0.72, 0.68, 0.22, 397.43, '{}'
             )",
            params![],
        )?;
        report_db.execute_batch("CHECKPOINT")?;
        drop(report_db);

        let config_path = root.join("config.yaml");
        fs::write(
            &config_path,
            format!(
                r#"
api:
  tushare_token: ""
runtime:
  timezone: Asia/Shanghai
  random_seed: 1
universe:
  benchmark: 000300.SH
  scan:
    csi300: true
    csi500: false
    csi1000: false
    sse50: false
  asset_classes:
    sector_etfs: false
    bond_etfs: false
    commodity_etfs: false
    cross_border: false
  filters:
    min_avg_volume_shares: 0
    min_price: 0.0
output:
  max_notable_items: 10
  min_notable_items: 1
data:
  raw_db_path: "{raw}"
  research_db_path: "{research}"
  report_db_path: "{report}"
  dev_db_path: "{dev}"
  constituent_refresh_days: 7
signals:
  momentum_windows: [5, 20]
  atr_period: 14
reporting:
  anthropic_model: test
  anthropic_temperature: 0.0
  max_tokens: 512
"#,
                raw = raw_path.display(),
                research = research_path.display(),
                report = report_path.display(),
                dev = dev_path.display(),
            ),
        )?;

        let cfg = Settings::load(config_path.to_str().expect("valid config path"))?;
        prepare_incremental_research(&cfg)?;

        let research_db = storage::open(research_path.to_str().expect("valid research path"))?;
        let count: i64 = research_db.query_row(
            "SELECT COUNT(*) FROM report_decisions WHERE symbol = '603444.SH'",
            params![],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}
