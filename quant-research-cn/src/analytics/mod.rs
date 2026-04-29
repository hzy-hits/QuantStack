pub mod algorithm_postmortem;
pub mod announcement;
pub mod bayes;
pub mod breakout;
pub mod continuation_vs_fade;
pub mod flow;
pub mod flow_audit;
pub mod headline_gate;
pub mod hmm;
pub mod limit_move_radar;
pub mod limit_up_model;
pub mod macro_gate;
pub mod mean_reversion;
pub mod momentum;
pub mod open_execution_gate;
pub mod paper_trade_ev;
pub mod price_features;
pub mod report_review;
pub mod rv;
pub mod sector_rotation;
pub mod setup_alpha;
pub mod shadow_calibration;
pub mod shadow_option;
pub mod shadow_option_alpha_calibration;
pub mod unlock;
pub mod vol_hmm;

use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Utc};
use duckdb::Connection;
use serde_json::json;
use std::time::Instant;
use tracing::info;

use crate::config::Settings;

/// Run all analytics modules. Order matters: some modules depend on others.
pub fn run_all(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<()> {
    info!("analytics start");
    for module in [
        "momentum",
        "announcement",
        "flow",
        "flow_audit",
        "unlock",
        "shadow_option",
        "hmm",
        "vol_hmm",
        "mean_reversion",
        "breakout",
        "sector_rotation",
        "price_features",
        "setup_alpha",
        "continuation_vs_fade",
        "limit_move_radar",
        "limit_up_model",
        "open_execution_gate",
        "shadow_option_alpha_calibration",
        "macro_gate",
    ] {
        run_module(db, cfg, as_of, module)?;
    }

    info!("analytics complete");
    Ok(())
}

pub fn run_module(db: &Connection, cfg: &Settings, as_of: NaiveDate, module: &str) -> Result<()> {
    info!(module, %as_of, "analytics module start");
    let stage = format!("analytics::{module}");
    let started_at = Utc::now().naive_utc();
    let timer = Instant::now();
    let result = with_transaction(db, &stage, |db| run_module_inner(db, cfg, as_of, module));
    let ended_at = Utc::now().naive_utc();
    let duration_ms = timer.elapsed().as_millis().min(i64::MAX as u128) as i64;
    match result {
        Ok(rows) => {
            record_stage_run(
                db,
                as_of,
                &stage,
                &started_at.to_string(),
                &ended_at.to_string(),
                duration_ms,
                rows as i64,
                "ok",
                None,
            );
            info!(module, %as_of, rows, duration_ms, "analytics module complete");
            Ok(())
        }
        Err(err) => {
            record_stage_run(
                db,
                as_of,
                &stage,
                &started_at.to_string(),
                &ended_at.to_string(),
                duration_ms,
                0,
                "failed",
                Some(json!({ "error": err.to_string() }).to_string()),
            );
            Err(err)
        }
    }
}

fn run_module_inner(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
    module: &str,
) -> Result<usize> {
    let rows = match module {
        "momentum" => {
            let n = momentum::compute(db, cfg, as_of)?;
            info!(rows = n, "momentum complete");
            n
        }
        "announcement" | "announcement_risk" => {
            let n = announcement::compute(db, as_of)?;
            info!(rows = n, "announcement_risk complete");
            n
        }
        "flow" | "flow_score" => {
            let n = flow::compute(db, cfg, as_of)?;
            info!(rows = n, "flow_score complete");
            n
        }
        "flow_audit" => {
            let n = flow_audit::compute(db, as_of)?;
            info!(rows = n, "flow_audit complete");
            n
        }
        "unlock" | "unlock_risk" => {
            let n = unlock::compute(db, cfg, as_of)?;
            info!(rows = n, "unlock_risk complete");
            n
        }
        "shadow_option" | "shadow_fast" => {
            let n = shadow_option::compute_fast(db, cfg, as_of)?;
            info!(rows = n, "shadow_fast complete");
            n
        }
        "hmm" | "hmm_regime" => {
            let n = hmm::compute(db, cfg, as_of)?;
            info!(rows = n, "hmm_regime complete");
            n
        }
        "vol_hmm" => {
            let n = vol_hmm::compute(db, cfg, as_of)?;
            info!(rows = n, "vol_hmm complete");
            n
        }
        "mean_reversion" => {
            let n = mean_reversion::compute(db, as_of)?;
            info!(rows = n, "mean_reversion complete");
            n
        }
        "breakout" => {
            let n = breakout::compute(db, as_of)?;
            info!(rows = n, "breakout complete");
            n
        }
        "sector_rotation" => {
            let n = sector_rotation::compute(db, as_of)?;
            info!(rows = n, "sector_rotation complete");
            n
        }
        "price_features" => {
            let n = price_features::compute(db, as_of)?;
            info!(rows = n, "price_features complete");
            n
        }
        "setup_alpha" => {
            let n = setup_alpha::compute(db, as_of)?;
            info!(rows = n, "setup_alpha complete");
            n
        }
        "continuation_vs_fade" => {
            let n = continuation_vs_fade::compute(db, as_of)?;
            info!(rows = n, "continuation_vs_fade complete");
            n
        }
        "limit_move_radar" => {
            let n = limit_move_radar::compute(db, as_of)?;
            info!(rows = n, "limit_move_radar complete");
            n
        }
        "limit_up_model" => {
            let n = limit_up_model::compute(db, as_of)?;
            info!(rows = n, "limit_up_model complete");
            n
        }
        "open_execution_gate" => {
            let n = open_execution_gate::compute(db, as_of)?;
            info!(rows = n, "open_execution_gate complete");
            n
        }
        "shadow_option_alpha_calibration" | "shadow_option_alpha" => {
            let n = shadow_option_alpha_calibration::compute(db, as_of)?;
            info!(rows = n, "shadow_option_alpha calibration complete");
            n
        }
        "paper_trade_ev" | "strategy_ev" => {
            let n = paper_trade_ev::compute(db, as_of)?;
            info!(rows = n, "paper_trade_ev complete");
            n
        }
        "algorithm_postmortem" | "algorithm_review" => {
            let n = algorithm_postmortem::compute(db, as_of)?;
            info!(rows = n, "algorithm_postmortem complete");
            n
        }
        "macro_gate" => {
            let n = macro_gate::compute(db, cfg, as_of)?;
            info!(rows = n, "macro_gate complete");
            n
        }
        other => {
            return Err(anyhow!(
                "unknown analytics module `{}`. supported: momentum, announcement, flow, flow_audit, unlock, shadow_option, hmm, vol_hmm, mean_reversion, breakout, sector_rotation, price_features, setup_alpha, continuation_vs_fade, limit_move_radar, limit_up_model, open_execution_gate, shadow_option_alpha_calibration, paper_trade_ev, algorithm_postmortem, macro_gate",
                other
            ));
        }
    };
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn record_stage_run(
    db: &Connection,
    as_of: NaiveDate,
    stage: &str,
    started_at: &str,
    ended_at: &str,
    duration_ms: i64,
    rows_written: i64,
    status: &str,
    detail_json: Option<String>,
) {
    let _ = db.execute(
        "INSERT OR REPLACE INTO pipeline_stage_runs (
            as_of, stage, started_at, ended_at, duration_ms, rows_written,
            status, cache_hit, detail_json
        ) VALUES (CAST(? AS DATE), ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), ?, ?, ?, FALSE, ?)",
        duckdb::params![
            as_of.to_string(),
            stage,
            started_at,
            ended_at,
            duration_ms,
            rows_written,
            status,
            detail_json,
        ],
    );
}

fn with_transaction<T, F>(db: &Connection, label: &str, f: F) -> Result<T>
where
    F: FnOnce(&Connection) -> Result<T>,
{
    db.execute_batch("BEGIN TRANSACTION")?;
    match f(db) {
        Ok(value) => {
            db.execute_batch("COMMIT")?;
            info!(transaction = label, "analytics transaction committed");
            Ok(value)
        }
        Err(err) => {
            let _ = db.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}
