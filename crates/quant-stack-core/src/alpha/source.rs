use super::{BulletinItem, TradeRow};
use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::{params, params_from_iter, Connection};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::Path;

pub(super) fn table_exists(con: &Connection, table: &str) -> Result<bool> {
    let mut stmt = con.prepare(
        "SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_name = ?",
    )?;
    let count = stmt.query_row(params![table], |row| row.get::<_, i64>(0))?;
    Ok(count > 0)
}

fn table_columns(con: &Connection, table: &str) -> Result<HashSet<String>> {
    let sql = format!("PRAGMA table_info('{}')", table.replace('\'', "''"));
    let mut stmt = con.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut out = HashSet::new();
    for row in rows {
        out.insert(row?);
    }
    Ok(out)
}

fn sql_str(alias: &str, column: &str, columns: &HashSet<String>, output: &str) -> String {
    if columns.contains(column) {
        format!("CAST({alias}.{column} AS VARCHAR) AS {output}")
    } else {
        format!("NULL AS {output}")
    }
}

fn sql_double(alias: &str, column: &str, columns: &HashSet<String>, output: &str) -> String {
    if columns.contains(column) {
        format!("CAST({alias}.{column} AS DOUBLE) AS {output}")
    } else {
        format!("NULL AS {output}")
    }
}

fn sql_i64(alias: &str, column: &str, columns: &HashSet<String>, output: &str) -> String {
    if columns.contains(column) {
        format!("CAST({alias}.{column} AS BIGINT) AS {output}")
    } else {
        format!("NULL AS {output}")
    }
}

fn sql_bool(
    alias: &str,
    column: &str,
    columns: &HashSet<String>,
    output: &str,
    default: &str,
) -> String {
    if columns.contains(column) {
        format!("CAST({alias}.{column} AS BOOLEAN) AS {output}")
    } else {
        format!("{default} AS {output}")
    }
}

fn sql_coalesce_str(refs: &[(&str, &str, &HashSet<String>)], output: &str) -> String {
    let parts: Vec<String> = refs
        .iter()
        .filter(|(_, column, columns)| columns.contains(*column))
        .map(|(alias, column, _)| format!("CAST({alias}.{column} AS VARCHAR)"))
        .collect();
    match parts.len() {
        0 => format!("NULL AS {output}"),
        1 => format!("{} AS {output}", parts[0]),
        _ => format!("COALESCE({}) AS {output}", parts.join(", ")),
    }
}

fn rows_from_query(con: &Connection, sql: &str, params: &[&str]) -> Result<Vec<TradeRow>> {
    let mut stmt = con.prepare(sql)?;
    let mapped = stmt.query_map(params_from_iter(params.iter()), |row| {
        Ok(TradeRow {
            report_date: row.get::<_, Option<String>>(0)?,
            evaluation_date: row.get::<_, Option<String>>(1)?,
            symbol: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            selection_status: row.get::<_, Option<String>>(3)?,
            rank_order: row.get::<_, Option<i64>>(4)?,
            report_bucket: row.get::<_, Option<String>>(5)?,
            signal_direction: row.get::<_, Option<String>>(6)?,
            signal_confidence: row.get::<_, Option<String>>(7)?,
            headline_mode: row.get::<_, Option<String>>(8)?,
            execution_mode: row.get::<_, Option<String>>(9)?,
            composite_score: row.get::<_, Option<f64>>(10)?,
            rr_ratio: row.get::<_, Option<f64>>(11)?,
            primary_reason: row.get::<_, Option<String>>(12)?,
            details_json: row.get::<_, Option<String>>(13)?,
            action_intent: row.get::<_, Option<String>>(14)?,
            executable: row.get::<_, Option<bool>>(15)?,
            return_pct: row.get::<_, Option<f64>>(16)?,
            best_possible_ret_pct: row.get::<_, Option<f64>>(17)?,
            stale_chase: row.get::<_, Option<bool>>(18)?,
            no_fill_reason: row.get::<_, Option<String>>(19)?,
            label: row.get::<_, Option<String>>(20)?,
            calibration_bucket: row.get::<_, Option<String>>(21)?,
            ..Default::default()
        })
    })?;
    let mut rows = Vec::new();
    for row in mapped {
        rows.push(row?);
    }
    Ok(rows)
}

fn completed_cutoff(as_of: NaiveDate, horizon_days: i64) -> NaiveDate {
    as_of - Duration::days(horizon_days.max(0))
}

pub(super) fn load_evaluated_trades(
    db_path: &Path,
    market: &str,
    as_of: NaiveDate,
    lookback_days: i64,
    horizon_days: i64,
) -> Result<(Vec<TradeRow>, String)> {
    let cutoff = completed_cutoff(as_of, horizon_days);
    let start = cutoff - Duration::days(lookback_days);
    let mut evaluated_through = cutoff.to_string();
    if !db_path.exists() {
        return Ok((Vec::new(), evaluated_through));
    }
    let con = Connection::open(db_path)?;
    let mut rows = Vec::new();
    if table_exists(&con, "algorithm_postmortem")? {
        rows = load_algorithm_postmortem_rows(&con, start, cutoff, as_of)?;
    }
    if (rows.is_empty() || !rows.iter().any(is_fill))
        && table_exists(&con, "report_decisions")?
        && table_exists(&con, "report_outcomes")?
    {
        rows = load_report_outcome_rows(&con, start, cutoff, as_of)?;
    }
    for row in &mut rows {
        row.market = market.to_string();
        let (policy_id, policy_label) = row_policy(row, market, horizon_days);
        row.policy_id = policy_id;
        row.policy_label = policy_label;
        row.return_pct = round_opt(row.return_pct, 6);
    }
    if let Some(max_date) = rows.iter().filter_map(|r| r.report_date.as_deref()).max() {
        evaluated_through = evaluated_through.min(max_date.to_string());
    }
    Ok((rows, evaluated_through))
}

fn load_algorithm_postmortem_rows(
    con: &Connection,
    start: NaiveDate,
    cutoff: NaiveDate,
    as_of: NaiveDate,
) -> Result<Vec<TradeRow>> {
    let a_cols = table_columns(con, "algorithm_postmortem")?;
    let d_cols = if table_exists(con, "report_decisions")? {
        table_columns(con, "report_decisions")?
    } else {
        HashSet::new()
    };
    let join = if d_cols.is_empty() {
        ""
    } else {
        "LEFT JOIN report_decisions d
           ON a.report_date = d.report_date
          AND a.session = d.session
          AND a.symbol = d.symbol
          AND a.selection_status = d.selection_status"
    };
    let select = vec![
        sql_str("a", "report_date", &a_cols, "report_date"),
        sql_str("a", "evaluation_date", &a_cols, "evaluation_date"),
        sql_str("a", "symbol", &a_cols, "symbol"),
        sql_str("a", "selection_status", &a_cols, "selection_status"),
        sql_i64("d", "rank_order", &d_cols, "rank_order"),
        sql_coalesce_str(
            &[
                ("d", "report_bucket", &d_cols),
                ("a", "report_bucket", &a_cols),
            ],
            "report_bucket",
        ),
        sql_coalesce_str(
            &[
                ("d", "signal_direction", &d_cols),
                ("a", "direction", &a_cols),
            ],
            "signal_direction",
        ),
        sql_str("d", "signal_confidence", &d_cols, "signal_confidence"),
        sql_str("d", "headline_mode", &d_cols, "headline_mode"),
        sql_str("d", "execution_mode", &d_cols, "execution_mode"),
        sql_double("d", "composite_score", &d_cols, "composite_score"),
        sql_double("d", "rr_ratio", &d_cols, "rr_ratio"),
        sql_str("d", "primary_reason", &d_cols, "primary_reason"),
        sql_coalesce_str(
            &[
                ("d", "details_json", &d_cols),
                ("a", "detail_json", &a_cols),
            ],
            "details_json",
        ),
        sql_str("a", "action_intent", &a_cols, "action_intent"),
        sql_bool("a", "executable", &a_cols, "executable", "FALSE"),
        sql_double("a", "realized_pnl_pct", &a_cols, "return_pct"),
        sql_double(
            "a",
            "best_possible_ret_pct",
            &a_cols,
            "best_possible_ret_pct",
        ),
        sql_bool("a", "stale_chase", &a_cols, "stale_chase", "FALSE"),
        sql_str("a", "no_fill_reason", &a_cols, "no_fill_reason"),
        sql_str("a", "label", &a_cols, "label"),
        sql_str("a", "calibration_bucket", &a_cols, "calibration_bucket"),
    ];
    let sql = format!(
        "SELECT {}
         FROM algorithm_postmortem a
         {}
         WHERE a.report_date >= CAST(? AS DATE)
           AND a.report_date <= CAST(? AS DATE)
           AND (a.evaluation_date IS NULL OR a.evaluation_date <= CAST(? AS DATE))
         ORDER BY a.report_date, a.symbol",
        select.join(", "),
        join
    );
    rows_from_query(
        &con,
        &sql,
        &[&start.to_string(), &cutoff.to_string(), &as_of.to_string()],
    )
}

fn load_report_outcome_rows(
    con: &Connection,
    start: NaiveDate,
    cutoff: NaiveDate,
    as_of: NaiveDate,
) -> Result<Vec<TradeRow>> {
    let d_cols = table_columns(con, "report_decisions")?;
    let o_cols = table_columns(con, "report_outcomes")?;
    let return_expr = if o_cols.contains("hold_3d_ret_pct") {
        "CAST(o.hold_3d_ret_pct AS DOUBLE)".to_string()
    } else if o_cols.contains("next_close_ret_pct") {
        "CAST(CASE WHEN lower(COALESCE(d.signal_direction, '')) IN ('short', 'bearish') THEN -o.next_close_ret_pct ELSE o.next_close_ret_pct END AS DOUBLE)".to_string()
    } else if o_cols.contains("best_up_2d_pct") {
        "CAST(o.best_up_2d_pct AS DOUBLE)".to_string()
    } else {
        "NULL".to_string()
    };
    let data_ready = if o_cols.contains("data_ready") {
        "AND COALESCE(o.data_ready, TRUE)"
    } else {
        ""
    };
    let select = vec![
        sql_str("d", "report_date", &d_cols, "report_date"),
        sql_str("o", "evaluation_date", &o_cols, "evaluation_date"),
        sql_str("d", "symbol", &d_cols, "symbol"),
        sql_str("d", "selection_status", &d_cols, "selection_status"),
        sql_i64("d", "rank_order", &d_cols, "rank_order"),
        sql_str("d", "report_bucket", &d_cols, "report_bucket"),
        sql_str("d", "signal_direction", &d_cols, "signal_direction"),
        sql_str("d", "signal_confidence", &d_cols, "signal_confidence"),
        sql_str("d", "headline_mode", &d_cols, "headline_mode"),
        sql_str("d", "execution_mode", &d_cols, "execution_mode"),
        sql_double("d", "composite_score", &d_cols, "composite_score"),
        sql_double("d", "rr_ratio", &d_cols, "rr_ratio"),
        sql_str("d", "primary_reason", &d_cols, "primary_reason"),
        sql_str("d", "details_json", &d_cols, "details_json"),
        "NULL AS action_intent".to_string(),
        "TRUE AS executable".to_string(),
        format!("{return_expr} AS return_pct"),
        "NULL AS best_possible_ret_pct".to_string(),
        "FALSE AS stale_chase".to_string(),
        "NULL AS no_fill_reason".to_string(),
        "NULL AS label".to_string(),
        "NULL AS calibration_bucket".to_string(),
    ];
    let sql = format!(
        "SELECT {}
         FROM report_decisions d
         INNER JOIN report_outcomes o
           ON d.report_date = o.report_date
          AND d.session = o.session
          AND d.symbol = o.symbol
          AND d.selection_status = o.selection_status
         WHERE d.report_date >= CAST(? AS DATE)
           AND d.report_date <= CAST(? AS DATE)
           AND (o.evaluation_date IS NULL OR o.evaluation_date <= CAST(? AS DATE))
           {}
         ORDER BY d.report_date, d.symbol",
        select.join(", "),
        data_ready
    );
    rows_from_query(
        &con,
        &sql,
        &[&start.to_string(), &cutoff.to_string(), &as_of.to_string()],
    )
}

pub(super) fn load_current_candidates(
    db_path: &Path,
    market: &str,
    as_of: NaiveDate,
    horizon_days: i64,
) -> Result<Vec<TradeRow>> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let con = Connection::open(db_path)?;
    if !table_exists(&con, "report_decisions")? {
        return Ok(Vec::new());
    }
    let d_cols = table_columns(&con, "report_decisions")?;
    let select = vec![
        sql_str("d", "report_date", &d_cols, "report_date"),
        "NULL AS evaluation_date".to_string(),
        sql_str("d", "symbol", &d_cols, "symbol"),
        sql_str("d", "selection_status", &d_cols, "selection_status"),
        sql_i64("d", "rank_order", &d_cols, "rank_order"),
        sql_str("d", "report_bucket", &d_cols, "report_bucket"),
        sql_str("d", "signal_direction", &d_cols, "signal_direction"),
        sql_str("d", "signal_confidence", &d_cols, "signal_confidence"),
        sql_str("d", "headline_mode", &d_cols, "headline_mode"),
        sql_str("d", "execution_mode", &d_cols, "execution_mode"),
        sql_double("d", "composite_score", &d_cols, "composite_score"),
        sql_double("d", "rr_ratio", &d_cols, "rr_ratio"),
        sql_str("d", "primary_reason", &d_cols, "primary_reason"),
        sql_str("d", "details_json", &d_cols, "details_json"),
        "NULL AS action_intent".to_string(),
        "NULL AS executable".to_string(),
        "NULL AS return_pct".to_string(),
        "NULL AS best_possible_ret_pct".to_string(),
        "FALSE AS stale_chase".to_string(),
        "NULL AS no_fill_reason".to_string(),
        "NULL AS label".to_string(),
        "NULL AS calibration_bucket".to_string(),
    ];
    let order = if d_cols.contains("rank_order") {
        "COALESCE(d.rank_order, 999999)"
    } else {
        "999999"
    };
    let sql = format!(
        "SELECT {}
         FROM report_decisions d
         WHERE d.report_date = CAST(? AS DATE)
         ORDER BY {}, d.symbol",
        select.join(", "),
        order
    );
    let mut rows = rows_from_query(&con, &sql, &[&as_of.to_string()])?;
    for row in &mut rows {
        row.market = market.to_string();
        let (policy_id, policy_label) = row_policy(row, market, horizon_days);
        row.policy_id = policy_id;
        row.policy_label = policy_label;
    }
    Ok(rows)
}

pub(super) fn load_options_alpha_candidates(
    db_path: &Path,
    market: &str,
    as_of: NaiveDate,
) -> Result<Vec<BulletinItem>> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let con = Connection::open(db_path)?;
    match market {
        "us" => load_us_options_alpha_candidates(&con, as_of),
        "cn" => load_cn_shadow_options_alpha_candidates(&con, as_of),
        _ => Ok(Vec::new()),
    }
}

fn load_us_options_alpha_candidates(
    con: &Connection,
    as_of: NaiveDate,
) -> Result<Vec<BulletinItem>> {
    if !table_exists(con, "options_alpha")? {
        return Ok(Vec::new());
    }
    let mut stmt = con.prepare(
        "SELECT symbol, directional_edge, vol_edge, vrp_edge, flow_edge,
                liquidity_gate, expression, reason, detail_json
         FROM options_alpha
         WHERE as_of = CAST(? AS DATE)
           AND expression IN ('stock_long', 'call_spread', 'put_spread')
           AND liquidity_gate = 'pass'
         ORDER BY
           ABS(COALESCE(directional_edge, 0)) + ABS(COALESCE(vol_edge, 0)) DESC,
           symbol
         LIMIT 20",
    )?;
    let mapped = stmt.query_map(params![as_of.to_string()], |row| {
        let symbol: String = row.get(0)?;
        let directional_edge: Option<f64> = row.get(1)?;
        let vol_edge: Option<f64> = row.get(2)?;
        let vrp_edge: Option<f64> = row.get(3)?;
        let flow_edge: Option<f64> = row.get(4)?;
        let liquidity_gate: Option<String> = row.get(5)?;
        let expression: Option<String> = row.get(6)?;
        let reason: Option<String> = row.get(7)?;
        let detail_json: Option<String> = row.get(8)?;
        let detail = parse_json(detail_json.as_deref());
        Ok(BulletinItem {
            market: "us".to_string(),
            symbol,
            section: "options_alpha".to_string(),
            policy_id: "us:options_alpha:real_options".to_string(),
            policy_label: "US real-options alpha".to_string(),
            report_bucket: Some("options_alpha".to_string()),
            signal_direction: match expression.as_deref() {
                Some("put_spread") => Some("short".to_string()),
                Some("stock_long" | "call_spread") => Some("long".to_string()),
                _ => Some("neutral".to_string()),
            },
            signal_confidence: Some("WATCH".to_string()),
            headline_mode: None,
            execution_mode: expression.clone(),
            reason: reason.unwrap_or_else(|| "real-options edge candidate".to_string()),
            blockers: Vec::new(),
            details: json!({
                "source": "real_options",
                "expression": expression,
                "directional_edge": round_opt(directional_edge, 4),
                "vol_edge": round_opt(vol_edge, 4),
                "vrp_edge": round_opt(vrp_edge, 4),
                "flow_edge": round_opt(flow_edge, 4),
                "liquidity_gate": liquidity_gate,
                "option_context": detail,
            }),
        })
    })?;
    let mut out = Vec::new();
    for row in mapped {
        out.push(row?);
    }
    Ok(out)
}

fn load_cn_shadow_options_alpha_candidates(
    con: &Connection,
    as_of: NaiveDate,
) -> Result<Vec<BulletinItem>> {
    if !table_exists(con, "analytics")? {
        return Ok(Vec::new());
    }
    let date_str = as_of.to_string();
    let mut stmt = con.prepare(
        "WITH base AS (
             SELECT
                 a.ts_code,
                 MAX(CASE WHEN a.metric = 'shadow_alpha_prob' THEN a.value END) AS shadow_alpha_prob,
                 MAX(CASE WHEN a.metric = 'entry_quality_score' THEN a.value END) AS entry_quality_score,
                 MAX(CASE WHEN a.metric = 'stale_chase_risk' THEN a.value END) AS stale_chase_risk,
                 MAX(CASE WHEN a.metric = 'calibration_bucket' THEN a.value END) AS calibration_bucket,
                 MAX(a.detail) AS detail_json
             FROM analytics a
             WHERE a.as_of = CAST(? AS DATE)
               AND a.module = 'shadow_option_alpha'
             GROUP BY a.ts_code
         ),
         fast AS (
             SELECT
                 ts_code,
                 MAX(CASE WHEN metric = 'shadow_iv_30d' THEN value END) AS shadow_iv_30d,
                 MAX(CASE WHEN metric = 'downside_stress' THEN value END) AS downside_stress
             FROM analytics
             WHERE as_of = CAST(? AS DATE)
               AND module = 'shadow_fast'
             GROUP BY ts_code
         ),
         full_metrics AS (
             SELECT
                 ts_code,
                 MAX(CASE WHEN metric = 'shadow_touch_90_3m' THEN value END) AS shadow_touch_90_3m,
                 MAX(CASE WHEN metric = 'shadow_skew_90_3m' THEN value END) AS shadow_skew_90_3m
             FROM analytics
             WHERE as_of = CAST(? AS DATE)
               AND module = 'shadow_full'
             GROUP BY ts_code
         )
         SELECT
             base.ts_code,
             base.shadow_alpha_prob,
             base.entry_quality_score,
             base.stale_chase_risk,
             base.calibration_bucket,
             base.detail_json,
             fast.shadow_iv_30d,
             fast.downside_stress,
             full_metrics.shadow_touch_90_3m,
             full_metrics.shadow_skew_90_3m
         FROM base
         LEFT JOIN fast ON fast.ts_code = base.ts_code
         LEFT JOIN full_metrics ON full_metrics.ts_code = base.ts_code
         WHERE COALESCE(base.shadow_alpha_prob, 0) >= 0.30
           AND COALESCE(base.entry_quality_score, 0) >= 0.38
           AND COALESCE(base.stale_chase_risk, 1) <= 0.40
         ORDER BY
           COALESCE(base.shadow_alpha_prob, 0) + COALESCE(base.entry_quality_score, 0)
           - COALESCE(base.stale_chase_risk, 0) DESC,
           base.ts_code
         LIMIT 20",
    )?;
    let mapped = stmt.query_map(params![&date_str, &date_str, &date_str], |row| {
        let symbol: String = row.get(0)?;
        let alpha_prob: Option<f64> = row.get(1)?;
        let entry_quality: Option<f64> = row.get(2)?;
        let stale_risk: Option<f64> = row.get(3)?;
        let calibration_bucket: Option<f64> = row.get(4)?;
        let detail_json: Option<String> = row.get(5)?;
        let shadow_iv_30d: Option<f64> = row.get(6)?;
        let downside_stress: Option<f64> = row.get(7)?;
        let touch_90: Option<f64> = row.get(8)?;
        let skew_90: Option<f64> = row.get(9)?;
        let expression = if alpha_prob.unwrap_or(0.0) >= 0.65
            && entry_quality.unwrap_or(0.0) >= 0.58
            && stale_risk.unwrap_or(1.0) <= 0.45
        {
            "stock_long_shadow_confirmed"
        } else {
            "wait"
        };
        Ok(BulletinItem {
            market: "cn".to_string(),
            symbol,
            section: "options_alpha".to_string(),
            policy_id: "cn:options_alpha:shadow_options".to_string(),
            policy_label: "CN shadow-options alpha".to_string(),
            report_bucket: Some("options_alpha".to_string()),
            signal_direction: Some("long".to_string()),
            signal_confidence: Some("WATCH".to_string()),
            headline_mode: None,
            execution_mode: Some(expression.to_string()),
            reason:
                "A-share shadow-option risk/convexity check; not a real single-name option trade"
                    .to_string(),
            blockers: if expression == "wait" {
                vec!["wait for equity gate / pullback confirmation".to_string()]
            } else {
                Vec::new()
            },
            details: json!({
                "source": "shadow_options",
                "expression": expression,
                "shadow_alpha_prob": round_opt(alpha_prob, 4),
                "entry_quality_score": round_opt(entry_quality, 4),
                "stale_chase_risk": round_opt(stale_risk, 4),
                "shadow_iv_30d": round_opt(shadow_iv_30d, 4),
                "downside_stress": round_opt(downside_stress, 4),
                "shadow_touch_90_3m": round_opt(touch_90, 4),
                "shadow_skew_90_3m": round_opt(skew_90, 4),
                "calibration_bucket": round_opt(calibration_bucket, 4),
                "shadow_context": parse_json(detail_json.as_deref()),
            }),
        })
    })?;
    let mut out = Vec::new();
    for row in mapped {
        out.push(row?);
    }
    Ok(out)
}

fn parse_json(raw: Option<&str>) -> Value {
    raw.and_then(|text| serde_json::from_str::<Value>(text).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

pub(super) fn parse_details(row: &TradeRow) -> Value {
    row.details_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter(|value| value.is_object())
        .unwrap_or_else(|| json!({}))
}

pub(super) fn details_object(details: &Value, key: &str) -> Value {
    details
        .get(key)
        .filter(|v| v.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}))
}

pub(super) fn normalize_bucket(value: Option<&str>) -> String {
    let text = value
        .unwrap_or("unknown")
        .trim()
        .to_lowercase()
        .replace(['-', '_'], " ");
    match text.as_str() {
        "core" | "core book" => "core".to_string(),
        "range core" => "range_core".to_string(),
        "tactical continuation" => "tactical_continuation".to_string(),
        "event tape" | "tactical event tape" => "event_tape".to_string(),
        "theme rotation" => "theme_rotation".to_string(),
        "appendix" | "appendix radar" | "radar" => "radar".to_string(),
        _ => text.replace(' ', "_"),
    }
}

pub(super) fn normalize_direction(value: Option<&str>) -> String {
    match value.unwrap_or("neutral").trim().to_lowercase().as_str() {
        "long" | "bull" | "bullish" | "up" => "long".to_string(),
        "short" | "bear" | "bearish" | "down" => "short".to_string(),
        _ => "neutral".to_string(),
    }
}

pub(super) fn normalize_confidence(value: Option<&str>) -> String {
    match value.unwrap_or("unknown").trim().to_uppercase().as_str() {
        "HIGH" | "MODERATE" => "high_mod".to_string(),
        "WATCH" => "watch".to_string(),
        "LOW" | "NO_SIGNAL" | "NONE" => "low".to_string(),
        other => other.to_lowercase(),
    }
}

pub(super) fn normalize_execution(value: Option<&str>) -> String {
    match value.unwrap_or("unknown").trim().to_lowercase().as_str() {
        "trade" | "trade_now" | "executable" | "executable_now" | "main_signal" => {
            "executable_now".to_string()
        }
        "wait" | "wait_pullback" | "pullback" => "wait_pullback".to_string(),
        "avoid" | "do_not_chase" | "stale_chase" => "do_not_chase".to_string(),
        "observe" | "observation" | "directional_observation" => "observe".to_string(),
        other => other.to_string(),
    }
}

fn row_policy(row: &TradeRow, market: &str, horizon_days: i64) -> (String, String) {
    let details = parse_details(row);
    let gate = details_object(&details, "main_signal_gate");
    let bucket = normalize_bucket(
        row.report_bucket
            .as_deref()
            .or_else(|| gate.get("report_bucket").and_then(Value::as_str)),
    );
    let direction = normalize_direction(
        row.signal_direction
            .as_deref()
            .or_else(|| gate.get("direction").and_then(Value::as_str)),
    );
    let confidence = normalize_confidence(row.signal_confidence.as_deref());
    let execution = normalize_execution(
        row.execution_mode
            .as_deref()
            .or(row.action_intent.as_deref())
            .or_else(|| gate.get("execution_action").and_then(Value::as_str))
            .or_else(|| gate.get("execution_mode").and_then(Value::as_str))
            .or_else(|| gate.get("action_intent").and_then(Value::as_str)),
    );
    let policy_id =
        format!("{market}:{bucket}:{direction}:{confidence}:{execution}:h{horizon_days}");
    let label = format!(
        "{} {} {} {} {} {}D",
        market.to_uppercase(),
        bucket.replace('_', " "),
        direction,
        confidence.replace('_', "/"),
        execution.replace('_', " "),
        horizon_days
    );
    (policy_id, label)
}

pub(super) fn round_opt(value: Option<f64>, digits: i32) -> Option<f64> {
    value.filter(|v| v.is_finite()).map(|v| {
        let factor = 10_f64.powi(digits);
        (v * factor).round() / factor
    })
}

pub(super) fn round_value(value: f64, digits: i32) -> f64 {
    round_opt(Some(value), digits).unwrap_or(value)
}

pub(super) fn is_fill(row: &TradeRow) -> bool {
    row.return_pct.is_some() && row.executable.unwrap_or(true)
}

pub(super) fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

pub(super) fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    } else {
        Some(sorted[mid])
    }
}
