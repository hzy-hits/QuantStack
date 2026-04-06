pub mod bayes;
pub mod momentum;
pub mod announcement;
pub mod flow;
pub mod hmm;
pub mod rv;
pub mod vol_hmm;
pub mod unlock;
pub mod macro_gate;
pub mod sector_rotation;
pub mod mean_reversion;
pub mod breakout;

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

use crate::config::Settings;

/// Run all analytics modules. Order matters: some modules depend on others.
pub fn run_all(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<()> {
    info!("analytics start");

    // Axiom 1+2: Conditional probability + Bayesian updating
    let n_mom = momentum::compute(db, cfg, as_of)?;
    info!(rows = n_mom, "momentum complete");

    // A-share specific: 业绩预告 risk
    let n_ann = announcement::compute(db, as_of)?;
    info!(rows = n_ann, "announcement_risk complete");

    // A-share specific: flow score (北向 + 融资 + 大宗 + 龙虎)
    let n_flow = flow::compute(db, cfg, as_of)?;
    info!(rows = n_flow, "flow_score complete");

    // A-share specific: 限售解禁 risk
    let n_unlock = unlock::compute(db, cfg, as_of)?;
    info!(rows = n_unlock, "unlock_risk complete");

    // Axiom 3: Latent states (HMM on benchmark returns)
    let n_hmm = hmm::compute(db, cfg, as_of)?;
    info!(rows = n_hmm, "hmm_regime complete");

    // Axiom 3b: Volatility regime HMM (on OHLC log-variance)
    let n_vol_hmm = vol_hmm::compute(db, cfg, as_of)?;
    info!(rows = n_vol_hmm, "vol_hmm complete");

    // Mean-reversion signals (RSI, MA distance, Bollinger position)
    let n_mr = mean_reversion::compute(db, as_of)?;
    info!(rows = n_mr, "mean_reversion complete");

    // Breakout detection (squeeze + volume + range break)
    let n_bo = breakout::compute(db, as_of)?;
    info!(rows = n_bo, "breakout complete");

    // Sector rotation (industry-level momentum + flow)
    let n_sector = sector_rotation::compute(db, as_of)?;
    info!(rows = n_sector, "sector_rotation complete");

    // Macro gate (Axiom 4 overlay)
    let n_macro = macro_gate::compute(db, cfg, as_of)?;
    info!(rows = n_macro, "macro_gate complete");

    info!("analytics complete");
    Ok(())
}
