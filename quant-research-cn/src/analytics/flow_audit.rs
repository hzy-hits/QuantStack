use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "flow_audit";

#[derive(Debug)]
struct FlowRow {
    ts_code: String,
    small_net: f64,
    medium_net: f64,
    large_net: f64,
    extra_large_net: f64,
    net_mf_amount_raw: f64,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let rows = load_rows(db, as_of)?;
    if rows.is_empty() {
        return Ok(0);
    }

    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS flow_audit_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM flow_audit_stage;",
    )?;

    let mut count = 0usize;
    {
        let mut appender = db.appender("flow_audit_stage")?;
        for row in rows {
            let large_plus_extra = row.large_net + row.extra_large_net;
            let component_sum =
                row.small_net + row.medium_net + row.large_net + row.extra_large_net;
            let raw_vs_large_diff = row.net_mf_amount_raw - large_plus_extra;
            let raw_abs = row.net_mf_amount_raw.abs();
            let large_abs = large_plus_extra.abs();
            let tolerance = 3_000.0_f64.max(0.25 * raw_abs.max(large_abs));
            let sign_conflict = row.net_mf_amount_raw.abs() >= 1_000.0
                && large_abs >= 1_000.0
                && row.net_mf_amount_raw.signum() != large_plus_extra.signum();
            let magnitude_conflict = raw_vs_large_diff.abs() > tolerance;
            let flow_conflict = sign_conflict || magnitude_conflict;
            let flow_confirmed = match (
                flow_conflict,
                large_plus_extra > 0.0,
                row.extra_large_net >= 0.0,
            ) {
                (false, true, true) => 1.0,
                _ => 0.0,
            };

            let detail = json!({
                "small_net": round2(row.small_net),
                "medium_net": round2(row.medium_net),
                "large_net": round2(row.large_net),
                "extra_large_net": round2(row.extra_large_net),
                "large_plus_extra_net": round2(large_plus_extra),
                "component_sum_net": round2(component_sum),
                "net_mf_amount_raw": round2(row.net_mf_amount_raw),
                "raw_vs_large_plus_extra_diff": round2(raw_vs_large_diff),
                "reconcile_tolerance": round2(tolerance),
                "sign_conflict": sign_conflict,
                "magnitude_conflict": magnitude_conflict,
                "flow_conflict_flag": flow_conflict,
                "flow_confirmed": flow_confirmed > 0.5,
                "data_scope": "tushare_moneyflow_components_audit",
            })
            .to_string();

            for (metric, value) in [
                ("small_net", row.small_net),
                ("medium_net", row.medium_net),
                ("large_net", row.large_net),
                ("extra_large_net", row.extra_large_net),
                ("large_plus_extra_net", large_plus_extra),
                ("component_sum_net", component_sum),
                ("net_mf_amount_raw", row.net_mf_amount_raw),
                ("raw_vs_large_plus_extra_diff", raw_vs_large_diff),
                ("flow_conflict_flag", if flow_conflict { 1.0 } else { 0.0 }),
                ("flow_confirmed", flow_confirmed),
            ] {
                appender.append_row(duckdb::params![
                    &row.ts_code,
                    &date_str,
                    MODULE,
                    metric,
                    value,
                    &detail
                ])?;
                count += 1;
            }
        }
    }

    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = ?",
        duckdb::params![date_str.clone(), MODULE],
    )?;
    db.execute_batch(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         SELECT ts_code, CAST(as_of AS DATE), module, metric, value, detail
         FROM flow_audit_stage",
    )?;

    info!(rows = count, "flow_audit complete");
    Ok(count)
}

fn load_rows(db: &Connection, as_of: NaiveDate) -> Result<Vec<FlowRow>> {
    let date_str = as_of.to_string();
    let mut stmt = db.prepare(
        "SELECT
            ts_code,
            COALESCE(buy_sm_amount, 0) - COALESCE(sell_sm_amount, 0) AS small_net,
            COALESCE(buy_md_amount, 0) - COALESCE(sell_md_amount, 0) AS medium_net,
            COALESCE(buy_lg_amount, 0) - COALESCE(sell_lg_amount, 0) AS large_net,
            COALESCE(buy_elg_amount, 0) - COALESCE(sell_elg_amount, 0) AS extra_large_net,
            COALESCE(net_mf_amount, 0) AS net_mf_amount_raw
         FROM moneyflow
         WHERE trade_date = (
             SELECT MAX(trade_date) FROM moneyflow WHERE trade_date <= CAST(? AS DATE)
         )",
    )?;
    let rows = stmt.query_map(duckdb::params![date_str], |row| {
        Ok(FlowRow {
            ts_code: row.get::<_, String>(0)?,
            small_net: row.get::<_, f64>(1).unwrap_or(0.0),
            medium_net: row.get::<_, f64>(2).unwrap_or(0.0),
            large_net: row.get::<_, f64>(3).unwrap_or(0.0),
            extra_large_net: row.get::<_, f64>(4).unwrap_or(0.0),
            net_mf_amount_raw: row.get::<_, f64>(5).unwrap_or(0.0),
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}
