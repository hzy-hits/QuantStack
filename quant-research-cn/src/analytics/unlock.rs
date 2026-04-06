/// Unlock risk — A-share specific (no US equivalent).
///
/// P(5D return < -2% | unlock_ratio, days_to_unlock) via Beta-Binomial
///
/// unlock_ratio = shares_unlocking / float_shares
/// Buckets: small (<1%), medium (1-5%), large (>5%)
/// Window: [as_of, as_of + lookahead_days]
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use crate::config::Settings;
use super::bayes::BetaBinomial;

const MODULE: &str = "unlock";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnlockSize {
    Small,   // < 1% of float — index 0
    Medium,  // 1-5%          — index 1
    Large,   // > 5%          — index 2
}

impl UnlockSize {
    fn as_i32(self) -> i32 {
        match self {
            Self::Small => 0,
            Self::Medium => 1,
            Self::Large => 2,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }

    /// Seed wins/losses that encode historical priors for each bucket.
    ///
    /// These represent the stylized empirical observation that larger unlocks
    /// are more likely to cause short-term drops (selling pressure).
    /// - Small: roughly 50/50 (no signal) → (3, 3)
    /// - Medium: slightly >50% drop → (3, 4) i.e. 4 "drops" out of 7
    /// - Large: ~70% drop → (2, 5) i.e. 5 "drops" out of 7
    ///
    /// Note: wins = times price did NOT drop >= 2%, losses = times it DID drop.
    /// So p_drop = posterior beta / (alpha + beta) after update.
    /// We model it the other way: wins = drops, losses = no-drops, so
    /// posterior.mean = P(drop).
    fn seed_wins_losses(self) -> (usize, usize) {
        match self {
            Self::Small => (3, 3),   // ~50% drop probability
            Self::Medium => (4, 3),  // ~57% drop probability
            Self::Large => (5, 2),   // ~71% drop probability
        }
    }
}

pub fn classify_unlock_size(float_ratio: f64) -> UnlockSize {
    if float_ratio < 1.0 {
        UnlockSize::Small
    } else if float_ratio < 5.0 {
        UnlockSize::Medium
    } else {
        UnlockSize::Large
    }
}

struct UnlockRow {
    ts_code: String,
    float_date: String,
    _float_share: Option<f64>,
    float_ratio: Option<f64>,
}

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let lookahead = cfg.signals.unlock_lookahead_days as i64;
    let window_end = (as_of + chrono::Duration::days(lookahead)).to_string();
    let date_str = as_of.to_string();

    // ── Step 1: Query upcoming unlocks within lookahead window ─────
    let mut stmt = db.prepare(
        "SELECT ts_code, CAST(float_date AS VARCHAR) AS float_date, float_share, float_ratio
         FROM share_unlock
         WHERE float_date >= ? AND float_date <= ?
         ORDER BY float_date",
    )?;

    let upcoming: Vec<UnlockRow> = stmt
        .query_map([&date_str, &window_end], |row| {
            Ok(UnlockRow {
                ts_code: row.get::<_, String>(0)?,
                float_date: row.get::<_, String>(1)?,
                _float_share: row.get::<_, Option<f64>>(2)?,
                float_ratio: row.get::<_, Option<f64>>(3)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    if upcoming.is_empty() {
        info!("no upcoming unlocks in window, skipping unlock_risk");
        return Ok(0);
    }

    // ── Step 2: Try historical calibration from actual price data ──
    // Look at past unlock events and their 5D forward returns.
    // If we have enough history, use it to refine the seed priors.
    let hist_start = (as_of - chrono::Duration::days(365)).to_string();
    let mut hist_extra: [(usize, usize); 3] = [(0, 0); 3]; // [small, medium, large] → (wins, losses)

    // Query historical unlock events that already occurred
    let hist_query = db.prepare(
        "SELECT u.ts_code, u.float_date, u.float_ratio,
                p_after.close AS close_after, p_before.close AS close_before
         FROM share_unlock u
         LEFT JOIN prices p_before
           ON p_before.ts_code = u.ts_code
           AND p_before.trade_date = (
               SELECT MAX(trade_date) FROM prices
               WHERE ts_code = u.ts_code AND trade_date <= u.float_date
           )
         LEFT JOIN prices p_after
           ON p_after.ts_code = u.ts_code
           AND p_after.trade_date = (
               SELECT MIN(trade_date) FROM prices
               WHERE ts_code = u.ts_code AND trade_date >= u.float_date + INTERVAL 5 DAY
           )
         WHERE u.float_date >= ? AND u.float_date < ?
           AND u.float_ratio IS NOT NULL",
    );

    match hist_query {
        Ok(mut hist_stmt) => {
            let rows: Vec<(Option<f64>, Option<f64>, Option<f64>)> = hist_stmt
                .query_map([&hist_start, &date_str], |row| {
                    Ok((
                        row.get::<_, Option<f64>>(2)?, // float_ratio
                        row.get::<_, Option<f64>>(3)?, // close_after
                        row.get::<_, Option<f64>>(4)?, // close_before
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            for (ratio_opt, after_opt, before_opt) in &rows {
                if let (Some(ratio), Some(after), Some(before)) =
                    (ratio_opt, after_opt, before_opt)
                {
                    if *before <= 0.0 {
                        continue;
                    }
                    let ret_5d = (after - before) / before;
                    let size = classify_unlock_size(*ratio);
                    let idx = size.as_i32() as usize;
                    if ret_5d < -0.02 {
                        hist_extra[idx].0 += 1; // win = drop occurred
                    } else {
                        hist_extra[idx].1 += 1; // loss = no drop
                    }
                }
            }
            info!(
                hist_events = rows.len(),
                "unlock historical calibration loaded"
            );
        }
        Err(e) => {
            // Historical calibration query may fail if tables are empty or
            // the JOIN is unsupported. Fall back to seed priors only.
            warn!(err = %e, "historical unlock calibration unavailable, using seed priors");
        }
    }

    // ── Step 3: Compute posteriors per size bucket ─────────────────
    // Combine seed priors with any historical data we found.
    let mut posteriors = Vec::with_capacity(3);
    for size in [UnlockSize::Small, UnlockSize::Medium, UnlockSize::Large] {
        let (seed_w, seed_l) = size.seed_wins_losses();
        let idx = size.as_i32() as usize;
        let total_wins = seed_w + hist_extra[idx].0;
        let total_losses = seed_l + hist_extra[idx].1;

        let bb = BetaBinomial::new(); // Beta(2,2)
        let post = bb.update(total_wins, total_losses);
        posteriors.push(post);
    }

    // ── Step 4: Write analytics for each upcoming unlock ──────────
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut count = 0usize;
    for u in &upcoming {
        let ratio = match u.float_ratio {
            Some(r) if r > 0.0 => r,
            _ => {
                warn!(ts_code = %u.ts_code, "missing float_ratio, skipping");
                continue;
            }
        };

        let size = classify_unlock_size(ratio);
        let idx = size.as_i32() as usize;
        let post = &posteriors[idx];

        // Compute days to unlock
        let float_date = NaiveDate::parse_from_str(&u.float_date, "%Y-%m-%d")
            .unwrap_or(as_of);
        let days_to_unlock = (float_date - as_of).num_days().max(0);

        let detail = format!(
            r#"{{"horizon":"5D","unlock_size":"{}","days_to_unlock":{},"float_ratio":{:.2},"sample_size":{},"ci_lower":{:.4},"ci_upper":{:.4},"prior":"Beta(2,2)"}}"#,
            size.label(),
            days_to_unlock,
            ratio,
            post.n,
            post.ci_low,
            post.ci_high,
        );

        // p_drop — P(5D return < -2% | unlock_size)
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "p_drop",
            post.mean,
            detail,
        ])?;
        // p_drop_ci_low
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "p_drop_ci_low",
            post.ci_low,
            serde_null(),
        ])?;
        // p_drop_ci_high
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "p_drop_ci_high",
            post.ci_high,
            serde_null(),
        ])?;
        // p_drop_n
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "p_drop_n",
            post.n as f64,
            serde_null(),
        ])?;
        // unlock_size
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "unlock_size",
            size.as_i32() as f64,
            serde_null(),
        ])?;
        // days_to_unlock
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "days_to_unlock",
            days_to_unlock as f64,
            serde_null(),
        ])?;
        // float_ratio
        insert_stmt.execute(duckdb::params![
            u.ts_code,
            date_str,
            MODULE,
            "float_ratio",
            ratio,
            serde_null(),
        ])?;

        count += 1;
    }

    info!(
        upcoming = upcoming.len(),
        written = count,
        "unlock_risk computed"
    );
    Ok(count)
}

fn serde_null() -> Option<String> {
    None
}
