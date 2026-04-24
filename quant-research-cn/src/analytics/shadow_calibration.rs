use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde::Serialize;

const SESSION: &str = "daily";
const LOOKBACK_DAYS: i64 = 45;
pub const POSITIVE_SHADOW_THRESHOLD: f64 = 0.35;
pub const BASE_SHADOW_WEIGHT: f64 = 0.10;
pub const BASE_SHADOW_PASS1_RESERVE: usize = 18;

#[derive(Debug, Clone, Serialize)]
pub struct ShadowCalibrationSummary {
    pub lookback_days: i64,
    pub positive_threshold: f64,
    pub total_reviewed: usize,
    pub selected_reviewed: usize,
    pub ignored_reviewed: usize,
    pub selected_shadow_covered: usize,
    pub ignored_shadow_covered: usize,
    pub selected_positive: usize,
    pub ignored_positive: usize,
    pub selected_avg_shadow_rank: f64,
    pub ignored_avg_shadow_rank: f64,
    pub selected_positive_avg_shadow_rank: f64,
    pub ignored_positive_avg_shadow_rank: f64,
    pub captured_avg_shadow_rank: f64,
    pub missed_avg_shadow_rank: f64,
    pub false_positive_avg_shadow_rank: f64,
    pub stale_avg_shadow_rank: f64,
    pub selected_positive_capture_rate: f64,
    pub selected_positive_false_positive_rate: f64,
    pub ignored_positive_missed_rate: f64,
    pub recall_gap: f64,
    pub quality_gap: f64,
    pub recommended_weight: f64,
    pub recommended_reserve: usize,
}

impl Default for ShadowCalibrationSummary {
    fn default() -> Self {
        Self {
            lookback_days: LOOKBACK_DAYS,
            positive_threshold: POSITIVE_SHADOW_THRESHOLD,
            total_reviewed: 0,
            selected_reviewed: 0,
            ignored_reviewed: 0,
            selected_shadow_covered: 0,
            ignored_shadow_covered: 0,
            selected_positive: 0,
            ignored_positive: 0,
            selected_avg_shadow_rank: 0.0,
            ignored_avg_shadow_rank: 0.0,
            selected_positive_avg_shadow_rank: 0.0,
            ignored_positive_avg_shadow_rank: 0.0,
            captured_avg_shadow_rank: 0.0,
            missed_avg_shadow_rank: 0.0,
            false_positive_avg_shadow_rank: 0.0,
            stale_avg_shadow_rank: 0.0,
            selected_positive_capture_rate: 0.0,
            selected_positive_false_positive_rate: 0.0,
            ignored_positive_missed_rate: 0.0,
            recall_gap: 0.0,
            quality_gap: 0.0,
            recommended_weight: BASE_SHADOW_WEIGHT,
            recommended_reserve: BASE_SHADOW_PASS1_RESERVE,
        }
    }
}

#[derive(Debug)]
struct ShadowReviewRow {
    symbol: String,
    selection_status: String,
    label: String,
    shadow_rank_score: f64,
    shadow_iv_30d: f64,
}

pub fn exclude_symbol_from_recall_metrics(symbol: &str) -> bool {
    symbol.starts_with("300") || symbol.starts_with("301")
}

pub fn summarize_shadow_calibration(db: &Connection, as_of: NaiveDate) -> ShadowCalibrationSummary {
    let cutoff = (as_of - Duration::days(LOOKBACK_DAYS)).to_string();
    let sql = "
        SELECT
            d.symbol,
            d.selection_status,
            COALESCE(p.label, '') AS label,
            COALESCE(CAST(json_extract(d.details_json, '$.shadow_rank_score') AS DOUBLE), 0.0) AS shadow_rank_score,
            COALESCE(CAST(json_extract(d.details_json, '$.shadow_iv_30d') AS DOUBLE), 0.0) AS shadow_iv_30d
        FROM report_decisions d
        INNER JOIN alpha_postmortem p
          ON p.report_date = d.report_date
         AND p.session = d.session
         AND p.symbol = d.symbol
         AND p.selection_status = d.selection_status
        WHERE d.session = ?
          AND d.report_date >= CAST(? AS DATE)
          AND d.report_date < CAST(? AS DATE)";

    let mut stmt = match db.prepare(sql) {
        Ok(stmt) => stmt,
        Err(_) => return ShadowCalibrationSummary::default(),
    };

    let rows = match stmt.query_map(duckdb::params![SESSION, cutoff, as_of.to_string()], |row| {
        Ok(ShadowReviewRow {
            symbol: row.get::<_, String>(0)?,
            selection_status: row.get::<_, String>(1)?,
            label: row.get::<_, String>(2)?,
            shadow_rank_score: row.get::<_, f64>(3).unwrap_or(0.0),
            shadow_iv_30d: row.get::<_, f64>(4).unwrap_or(0.0),
        })
    }) {
        Ok(rows) => rows,
        Err(_) => return ShadowCalibrationSummary::default(),
    };

    let rows: Vec<ShadowReviewRow> = rows
        .filter_map(|r| r.ok())
        .filter(|row| !exclude_symbol_from_recall_metrics(&row.symbol))
        .collect();
    if rows.is_empty() {
        return ShadowCalibrationSummary::default();
    }

    let selected_rows: Vec<&ShadowReviewRow> = rows
        .iter()
        .filter(|row| row.selection_status == "selected")
        .collect();
    let ignored_rows: Vec<&ShadowReviewRow> = rows
        .iter()
        .filter(|row| row.selection_status == "ignored")
        .collect();

    let selected_shadow_covered: Vec<&ShadowReviewRow> = selected_rows
        .iter()
        .copied()
        .filter(|row| row.shadow_iv_30d > 0.0)
        .collect();
    let ignored_shadow_covered: Vec<&ShadowReviewRow> = ignored_rows
        .iter()
        .copied()
        .filter(|row| row.shadow_iv_30d > 0.0)
        .collect();

    let selected_positive: Vec<&ShadowReviewRow> = selected_rows
        .iter()
        .copied()
        .filter(|row| row.shadow_rank_score >= POSITIVE_SHADOW_THRESHOLD)
        .collect();
    let ignored_positive: Vec<&ShadowReviewRow> = ignored_rows
        .iter()
        .copied()
        .filter(|row| row.shadow_rank_score >= POSITIVE_SHADOW_THRESHOLD)
        .collect();

    let captured_rows: Vec<&ShadowReviewRow> =
        rows.iter().filter(|row| row.label == "captured").collect();
    let missed_rows: Vec<&ShadowReviewRow> = rows
        .iter()
        .filter(|row| row.label == "missed_alpha")
        .collect();
    let false_positive_rows: Vec<&ShadowReviewRow> = rows
        .iter()
        .filter(|row| row.label == "false_positive")
        .collect();
    let stale_rows: Vec<&ShadowReviewRow> = rows
        .iter()
        .filter(|row| {
            matches!(
                row.label.as_str(),
                "alpha_already_paid" | "good_signal_bad_timing"
            )
        })
        .collect();

    let selected_positive_capture_rate = ratio(
        count_label(&selected_positive, "captured"),
        selected_positive.len(),
    );
    let selected_positive_false_positive_rate = ratio(
        count_label(&selected_positive, "false_positive"),
        selected_positive.len(),
    );
    let ignored_positive_missed_rate = ratio(
        count_label(&ignored_positive, "missed_alpha"),
        ignored_positive.len(),
    );

    let selected_positive_avg_shadow_rank = avg_shadow_rank(&selected_positive);
    let ignored_positive_avg_shadow_rank = avg_shadow_rank(&ignored_positive);
    let captured_avg_shadow_rank = avg_shadow_rank(&captured_rows);
    let false_positive_avg_shadow_rank = avg_shadow_rank(&false_positive_rows);

    let recall_gap =
        (ignored_positive_avg_shadow_rank - selected_positive_avg_shadow_rank).max(0.0);
    let quality_gap = (captured_avg_shadow_rank - false_positive_avg_shadow_rank).max(0.0);
    let recall_pressure = (0.65 * ignored_positive_missed_rate + 0.55 * recall_gap).clamp(0.0, 1.0);
    let quality_support =
        (0.60 * selected_positive_capture_rate + 0.40 * quality_gap).clamp(0.0, 1.0);
    let false_positive_penalty = (0.80 * selected_positive_false_positive_rate
        + 0.40 * (false_positive_avg_shadow_rank - captured_avg_shadow_rank).max(0.0))
    .clamp(0.0, 1.0);

    let recommended_weight = (BASE_SHADOW_WEIGHT + 0.05 * recall_pressure + 0.03 * quality_support
        - 0.04 * false_positive_penalty)
        .clamp(0.08, 0.18);
    let recommended_reserve =
        (BASE_SHADOW_PASS1_RESERVE as f64 + 14.0 * recall_pressure + 5.0 * quality_support
            - 4.0 * false_positive_penalty)
            .round()
            .clamp(14.0, 34.0) as usize;

    ShadowCalibrationSummary {
        lookback_days: LOOKBACK_DAYS,
        positive_threshold: POSITIVE_SHADOW_THRESHOLD,
        total_reviewed: rows.len(),
        selected_reviewed: selected_rows.len(),
        ignored_reviewed: ignored_rows.len(),
        selected_shadow_covered: selected_shadow_covered.len(),
        ignored_shadow_covered: ignored_shadow_covered.len(),
        selected_positive: selected_positive.len(),
        ignored_positive: ignored_positive.len(),
        selected_avg_shadow_rank: avg_shadow_rank(&selected_rows),
        ignored_avg_shadow_rank: avg_shadow_rank(&ignored_rows),
        selected_positive_avg_shadow_rank,
        ignored_positive_avg_shadow_rank,
        captured_avg_shadow_rank,
        missed_avg_shadow_rank: avg_shadow_rank(&missed_rows),
        false_positive_avg_shadow_rank,
        stale_avg_shadow_rank: avg_shadow_rank(&stale_rows),
        selected_positive_capture_rate,
        selected_positive_false_positive_rate,
        ignored_positive_missed_rate,
        recall_gap,
        quality_gap,
        recommended_weight,
        recommended_reserve,
    }
}

fn avg_shadow_rank(rows: &[&ShadowReviewRow]) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    rows.iter().map(|row| row.shadow_rank_score).sum::<f64>() / rows.len() as f64
}

fn count_label(rows: &[&ShadowReviewRow], label: &str) -> usize {
    rows.iter().filter(|row| row.label == label).count()
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

#[cfg(test)]
mod tests {
    use super::exclude_symbol_from_recall_metrics;

    #[test]
    fn chinext_symbols_are_excluded_from_recall_metrics() {
        assert!(exclude_symbol_from_recall_metrics("300363.SZ"));
        assert!(exclude_symbol_from_recall_metrics("301396.SZ"));
        assert!(!exclude_symbol_from_recall_metrics("603135.SH"));
        assert!(!exclude_symbol_from_recall_metrics("002170.SZ"));
    }
}
