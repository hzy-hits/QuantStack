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
use chrono::NaiveDate;
use duckdb::Connection;
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
        "open_execution_gate",
        "shadow_option_alpha_calibration",
        "macro_gate",
    ] {
        with_transaction(db, &format!("analytics::{module}"), |db| {
            run_module_inner(db, cfg, as_of, module)?;
            Ok(())
        })?;
    }

    info!("analytics complete");
    Ok(())
}

pub fn run_module(db: &Connection, cfg: &Settings, as_of: NaiveDate, module: &str) -> Result<()> {
    info!(module, %as_of, "analytics module start");
    with_transaction(db, &format!("analytics::{module}"), |db| {
        run_module_inner(db, cfg, as_of, module)?;
        Ok(())
    })?;
    info!(module, %as_of, "analytics module complete");
    Ok(())
}

fn run_module_inner(db: &Connection, cfg: &Settings, as_of: NaiveDate, module: &str) -> Result<()> {
    match module {
        "momentum" => {
            let n = momentum::compute(db, cfg, as_of)?;
            info!(rows = n, "momentum complete");
        }
        "announcement" | "announcement_risk" => {
            let n = announcement::compute(db, as_of)?;
            info!(rows = n, "announcement_risk complete");
        }
        "flow" | "flow_score" => {
            let n = flow::compute(db, cfg, as_of)?;
            info!(rows = n, "flow_score complete");
        }
        "flow_audit" => {
            let n = flow_audit::compute(db, as_of)?;
            info!(rows = n, "flow_audit complete");
        }
        "unlock" | "unlock_risk" => {
            let n = unlock::compute(db, cfg, as_of)?;
            info!(rows = n, "unlock_risk complete");
        }
        "shadow_option" | "shadow_fast" => {
            let n = shadow_option::compute_fast(db, cfg, as_of)?;
            info!(rows = n, "shadow_fast complete");
        }
        "hmm" | "hmm_regime" => {
            let n = hmm::compute(db, cfg, as_of)?;
            info!(rows = n, "hmm_regime complete");
        }
        "vol_hmm" => {
            let n = vol_hmm::compute(db, cfg, as_of)?;
            info!(rows = n, "vol_hmm complete");
        }
        "mean_reversion" => {
            let n = mean_reversion::compute(db, as_of)?;
            info!(rows = n, "mean_reversion complete");
        }
        "breakout" => {
            let n = breakout::compute(db, as_of)?;
            info!(rows = n, "breakout complete");
        }
        "sector_rotation" => {
            let n = sector_rotation::compute(db, as_of)?;
            info!(rows = n, "sector_rotation complete");
        }
        "price_features" => {
            let n = price_features::compute(db, as_of)?;
            info!(rows = n, "price_features complete");
        }
        "setup_alpha" => {
            let n = setup_alpha::compute(db, as_of)?;
            info!(rows = n, "setup_alpha complete");
        }
        "continuation_vs_fade" => {
            let n = continuation_vs_fade::compute(db, as_of)?;
            info!(rows = n, "continuation_vs_fade complete");
        }
        "limit_move_radar" => {
            let n = limit_move_radar::compute(db, as_of)?;
            info!(rows = n, "limit_move_radar complete");
        }
        "open_execution_gate" => {
            let n = open_execution_gate::compute(db, as_of)?;
            info!(rows = n, "open_execution_gate complete");
        }
        "shadow_option_alpha_calibration" | "shadow_option_alpha" => {
            let n = shadow_option_alpha_calibration::compute(db, as_of)?;
            info!(rows = n, "shadow_option_alpha calibration complete");
        }
        "paper_trade_ev" | "strategy_ev" => {
            let n = paper_trade_ev::compute(db, as_of)?;
            info!(rows = n, "paper_trade_ev complete");
        }
        "algorithm_postmortem" | "algorithm_review" => {
            let n = algorithm_postmortem::compute(db, as_of)?;
            info!(rows = n, "algorithm_postmortem complete");
        }
        "macro_gate" => {
            let n = macro_gate::compute(db, cfg, as_of)?;
            info!(rows = n, "macro_gate complete");
        }
        other => {
            return Err(anyhow!(
                "unknown analytics module `{}`. supported: momentum, announcement, flow, flow_audit, unlock, shadow_option, hmm, vol_hmm, mean_reversion, breakout, sector_rotation, price_features, setup_alpha, continuation_vs_fade, limit_move_radar, open_execution_gate, shadow_option_alpha_calibration, paper_trade_ev, algorithm_postmortem, macro_gate",
                other
            ));
        }
    }
    Ok(())
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
