mod config;
mod fetcher;
mod storage;
mod analytics;
mod enrichment;
mod filtering;
mod reporting;

use anyhow::Result;
use clap::{Parser, Subcommand};
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
    let db = storage::open(&cfg.data.db_path)?;

    match cli.command {
        Command::Init { years, with_flow } => {
            info!("initializing database and fetching historical data");
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
            ).await?;
            info!(rows = n, "prices + daily_basic backfill done");

            // Backfill index prices (000300.SH, 000016.SH, etc.)
            let ni = fetcher::tushare::prices::backfill_index(
                &client, token, &db, start, today,
            ).await?;
            info!(rows = ni, "index prices backfill done");

            // Backfill margin_detail (融资融券) — last 60 days for z-score baseline
            let margin_start = today - chrono::Duration::days(60);
            let nm = fetcher::tushare::prices::backfill_margin(
                &client, token, &db, margin_start, today,
            ).await?;
            info!(rows = nm, "margin_detail backfill done");

            // Backfill Shibor (daily rates) — last 180 days
            let shibor_start = today - chrono::Duration::days(180);
            let ns = fetcher::tushare::macro_cn::backfill_shibor(
                &client, token, &db, shibor_start, today,
            ).await?;
            info!(rows = ns, "shibor backfill done");

            // Fetch LPR + monthly macro indicators
            fetcher::tushare::macro_cn::fetch_lpr(&client, token, &db, today).await?;
            let macro_series: Vec<(String, String)> = cfg.r#macro.series.iter()
                .map(|s| (s.id.clone(), s.name.clone()))
                .collect();
            fetcher::tushare::macro_cn::fetch_macro_indicators(
                &client, token, &db, &macro_series,
            ).await?;

            // Optional: backfill moneyflow
            if with_flow {
                // Moneyflow backfill — only last 6 months (flow data is large)
                let flow_start = today - chrono::Duration::days(180);
                let nf = fetcher::tushare::prices::backfill_moneyflow(
                    &client, token, &db, flow_start, today,
                ).await?;
                info!(rows = nf, "moneyflow backfill done");
            }

            info!("init complete");
        }
        Command::Run { date, skip_fetch } => {
            let as_of = config::resolve_date(date.as_deref())?;
            info!(%as_of, "pipeline start");

            if !skip_fetch {
                // Phase 1: Data ingestion
                let (tushare_rows, akshare_rows) = tokio::join!(
                    fetcher::tushare::fetch_all(&db, &cfg, as_of),
                    fetcher::akshare::fetch_all(&db, &cfg, as_of),
                );
                info!(tushare = ?tushare_rows, akshare = ?akshare_rows, "fetch complete");
            }

            // Phase 1.5: Enrich with DeepSeek (news sentiment + theme clustering)
            let n_enriched = enrichment::news::enrich_news(&db, &cfg, as_of).await?;
            let n_themes = enrichment::themes::enrich_themes(&db, &cfg, as_of).await?;
            info!(enriched = n_enriched, themes = n_themes, "deepseek enrichment done");

            // Phase 2: Analytics
            analytics::run_all(&db, &cfg, as_of)?;

            // Phase 3: Filter
            let notable = filtering::build_notable_items(&db, &cfg, as_of)?;
            info!(notable = notable.len(), "filter complete");

            // Phase 4: Render payload
            let path = reporting::render_payload(&db, &cfg, as_of, &notable)?;
            info!(%path, "payload ready");
        }
        Command::Fetch { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let (t, a) = tokio::join!(
                fetcher::tushare::fetch_all(&db, &cfg, as_of),
                fetcher::akshare::fetch_all(&db, &cfg, as_of),
            );
            info!(tushare = ?t, akshare = ?a, "fetch complete");
        }
        Command::Analyze { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            analytics::run_all(&db, &cfg, as_of)?;
            info!("analytics complete");
        }
        Command::Enrich { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let n = enrichment::news::enrich_news(&db, &cfg, as_of).await?;
            let t = enrichment::themes::enrich_themes(&db, &cfg, as_of).await?;
            info!(enriched = n, themes = t, "enrichment complete");
        }
        Command::Render { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let notable = filtering::build_notable_items(&db, &cfg, as_of)?;
            let path = reporting::render_payload(&db, &cfg, as_of, &notable)?;
            info!(%path, "payload ready");
        }
    }

    Ok(())
}
