use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const SESSION: &str = "daily";
const LOOKBACK_DAYS: i64 = 45;
const DIRECTION_THRESHOLD_PCT: f64 = 2.0;

#[derive(Debug)]
struct ReviewRow {
    report_date: NaiveDate,
    symbol: String,
    selection_status: String,
    report_bucket: String,
    evaluation_date: NaiveDate,
    direction: String,
    signal_confidence: String,
    execution_mode: String,
    max_chase_gap_pct: Option<f64>,
    next_open: Option<f64>,
    next_close: Option<f64>,
    best_up_2d_pct: Option<f64>,
    best_down_2d_pct: Option<f64>,
    gap_vs_chase_limit: Option<f64>,
    data_ready: bool,
    alpha_label: Option<String>,
    decision_detail: Option<String>,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    materialize_algorithm_postmortem(db, as_of)
}

pub fn materialize_algorithm_postmortem(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    ensure_schema(db)?;
    let cutoff = as_of - Duration::days(LOOKBACK_DAYS);
    db.execute(
        "DELETE FROM algorithm_postmortem
         WHERE report_date >= CAST(? AS DATE)
           AND report_date <= CAST(? AS DATE)
           AND session = ?",
        duckdb::params![cutoff.to_string(), as_of.to_string(), SESSION],
    )?;

    let rows = load_rows(db, cutoff, as_of)?;
    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO algorithm_postmortem (
            report_date, session, symbol, selection_status, evaluation_date,
            action_label, action_source, direction, direction_right, executable,
            fill_price, exit_price, realized_pnl_pct, best_possible_ret_pct,
            stale_chase, no_fill_reason, label, feedback_action, feedback_weight,
            action_intent, calibration_bucket, regime_bucket, fill_quality,
            detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    let mut inserted = 0usize;
    for row in rows {
        let best_possible =
            directional_best_ret(&row.direction, row.best_up_2d_pct, row.best_down_2d_pct);
        let direction_right = is_direction_right(best_possible);
        let (action_label, action_source) = infer_action(&row);
        let gap_ratio = row.gap_vs_chase_limit.unwrap_or(0.0);
        let stale_chase = action_label == "DO_NOT_CHASE"
            || matches!(action_label, "TRADE_NOW" | "WAIT_PULLBACK") && gap_ratio >= 1.0;

        let (executable, fill_price, exit_price, realized_pnl_pct, no_fill_reason) =
            fill_result(&row, action_label, stale_chase);
        let label = classify(
            &row,
            action_label,
            direction_right,
            stale_chase,
            executable,
            realized_pnl_pct,
            best_possible,
        );
        let (feedback_action, feedback_weight) = feedback(label);
        let action_intent = action_intent(action_label);
        let regime_bucket = regime_bucket(&row);
        let calibration_bucket = calibration_bucket(
            &row.report_bucket,
            &row.signal_confidence,
            &row.execution_mode,
            action_intent,
            &regime_bucket,
        );
        let fill_quality = fill_quality(
            action_intent,
            row.data_ready,
            executable,
            stale_chase,
            realized_pnl_pct,
            no_fill_reason,
        );
        let detail = json!({
            "alpha_postmortem_label": row.alpha_label,
            "report_bucket": row.report_bucket,
            "signal_confidence": row.signal_confidence,
            "execution_mode": row.execution_mode,
            "action_intent": action_intent,
            "calibration_bucket": calibration_bucket,
            "regime_bucket": regime_bucket,
            "fill_quality": fill_quality,
            "max_chase_gap_pct": row.max_chase_gap_pct,
            "gap_vs_chase_limit": row.gap_vs_chase_limit,
            "direction_threshold_pct": DIRECTION_THRESHOLD_PCT,
        })
        .to_string();

        insert.execute(duckdb::params![
            row.report_date.to_string(),
            SESSION,
            row.symbol,
            row.selection_status,
            row.evaluation_date.to_string(),
            action_label,
            action_source,
            row.direction,
            direction_right,
            executable,
            fill_price,
            exit_price,
            realized_pnl_pct,
            best_possible,
            stale_chase,
            no_fill_reason,
            label,
            feedback_action,
            feedback_weight,
            action_intent,
            calibration_bucket,
            regime_bucket,
            fill_quality,
            detail,
        ])?;
        inserted += 1;
    }

    info!(rows = inserted, "algorithm_postmortem complete");
    Ok(inserted)
}

fn load_rows(db: &Connection, cutoff: NaiveDate, as_of: NaiveDate) -> Result<Vec<ReviewRow>> {
    let mut stmt = db.prepare(
        "SELECT
            CAST(d.report_date AS VARCHAR),
            d.symbol,
            d.selection_status,
            COALESCE(d.report_bucket, 'CORE BOOK'),
            CAST(o.evaluation_date AS VARCHAR),
            d.signal_direction,
            d.signal_confidence,
            d.execution_mode,
            d.max_chase_gap_pct,
            o.next_open,
            o.next_close,
            o.best_up_2d_pct,
            o.best_down_2d_pct,
            o.gap_vs_chase_limit,
            o.data_ready,
            p.label,
            d.details_json
         FROM report_decisions d
         INNER JOIN report_outcomes o
           ON o.report_date = d.report_date
          AND o.session = d.session
          AND o.symbol = d.symbol
          AND o.selection_status = d.selection_status
         LEFT JOIN alpha_postmortem p
           ON p.report_date = d.report_date
          AND p.session = d.session
          AND p.symbol = d.symbol
          AND p.selection_status = d.selection_status
         WHERE d.session = ?
           AND d.report_date >= CAST(? AS DATE)
           AND d.report_date <= CAST(? AS DATE)
           AND o.data_ready = TRUE",
    )?;
    let rows = stmt.query_map(
        duckdb::params![SESSION, cutoff.to_string(), as_of.to_string()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6).unwrap_or_default(),
                row.get::<_, String>(7).unwrap_or_default(),
                row.get::<_, Option<f64>>(8)?,
                row.get::<_, Option<f64>>(9)?,
                row.get::<_, Option<f64>>(10)?,
                row.get::<_, Option<f64>>(11)?,
                row.get::<_, Option<f64>>(12)?,
                row.get::<_, Option<f64>>(13)?,
                row.get::<_, bool>(14).unwrap_or(false),
                row.get::<_, Option<String>>(15).ok().flatten(),
                row.get::<_, Option<String>>(16).ok().flatten(),
            ))
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        let (
            report_date,
            symbol,
            selection_status,
            report_bucket,
            evaluation_date,
            direction,
            signal_confidence,
            execution_mode,
            max_chase_gap_pct,
            next_open,
            next_close,
            best_up_2d_pct,
            best_down_2d_pct,
            gap_vs_chase_limit,
            data_ready,
            alpha_label,
            decision_detail,
        ) = row?;
        out.push(ReviewRow {
            report_date: parse_sql_date(&report_date)?,
            symbol,
            selection_status,
            report_bucket,
            evaluation_date: parse_sql_date(&evaluation_date)?,
            direction,
            signal_confidence,
            execution_mode,
            max_chase_gap_pct,
            next_open,
            next_close,
            best_up_2d_pct,
            best_down_2d_pct,
            gap_vs_chase_limit,
            data_ready,
            alpha_label,
            decision_detail,
        });
    }
    Ok(out)
}

fn ensure_schema(db: &Connection) -> Result<()> {
    db.execute_batch(
        "ALTER TABLE algorithm_postmortem ADD COLUMN IF NOT EXISTS action_intent VARCHAR;
         ALTER TABLE algorithm_postmortem ADD COLUMN IF NOT EXISTS calibration_bucket VARCHAR;
         ALTER TABLE algorithm_postmortem ADD COLUMN IF NOT EXISTS regime_bucket VARCHAR;
         ALTER TABLE algorithm_postmortem ADD COLUMN IF NOT EXISTS fill_quality VARCHAR;",
    )?;
    Ok(())
}

fn action_intent(action_label: &str) -> &'static str {
    match action_label {
        "TRADE_NOW" => "TRADE",
        "OBSERVE" => "OBSERVE",
        "DO_NOT_CHASE" | "RISK_AVOID" => "AVOID",
        _ => "WAIT",
    }
}

fn parse_detail_json(raw: &Option<String>) -> Option<serde_json::Value> {
    raw.as_ref()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
}

fn action_from_main_signal_gate(row: &ReviewRow) -> Option<(&'static str, &'static str)> {
    let parsed = parse_detail_json(&row.decision_detail)?;
    let gate = parsed.get("main_signal_gate")?.as_object()?;
    let status = gate
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();
    let intent = gate
        .get("action_intent")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_uppercase();

    if status == "pass" {
        return Some(("TRADE_NOW", "main_signal_gate"));
    }
    let hard_blockers = gate
        .get("blockers")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|blocker| {
                    let text = blocker.trim().to_lowercase();
                    !text.starts_with("headline_gate_") && !text.contains("headline gate")
                })
                .count()
        })
        .unwrap_or(0);
    if hard_blockers == 0 {
        return None;
    }
    match intent.as_str() {
        "AVOID" => Some(("DO_NOT_CHASE", "main_signal_gate")),
        "WAIT" => Some(("WAIT_PULLBACK", "main_signal_gate")),
        _ => Some(("OBSERVE", "main_signal_gate")),
    }
}

fn regime_bucket(row: &ReviewRow) -> String {
    let parsed = parse_detail_json(&row.decision_detail);
    if let Some(value) = parsed.as_ref() {
        for key in ["regime", "trend_regime", "headline_mode"] {
            if let Some(text) = value.get(key).and_then(|v| v.as_str()) {
                if !text.trim().is_empty() {
                    return text.trim().to_lowercase();
                }
            }
        }
        for key in ["execution_gate", "headline_gate"] {
            if let Some(obj) = value.get(key).and_then(|v| v.as_object()) {
                for nested_key in ["trend_regime", "regime", "mode"] {
                    if let Some(text) = obj.get(nested_key).and_then(|v| v.as_str()) {
                        if !text.trim().is_empty() {
                            return text.trim().to_lowercase();
                        }
                    }
                }
            }
        }
    }
    "unknown".to_string()
}

fn calibration_bucket(
    report_bucket: &str,
    signal_confidence: &str,
    execution_mode: &str,
    action_intent: &str,
    regime_bucket: &str,
) -> String {
    format!(
        "lane={}|confidence={}|regime={}|execution={}|intent={}",
        report_bucket.trim().to_lowercase(),
        signal_confidence.trim().to_lowercase(),
        regime_bucket.trim().to_lowercase(),
        execution_mode.trim().to_lowercase(),
        action_intent.trim().to_lowercase()
    )
}

fn fill_quality(
    action_intent: &str,
    data_ready: bool,
    executable: bool,
    stale_chase: bool,
    realized_pnl_pct: Option<f64>,
    no_fill_reason: Option<&'static str>,
) -> &'static str {
    if action_intent != "TRADE" {
        return no_fill_reason.unwrap_or("not_trade");
    }
    if !data_ready {
        return "unresolved";
    }
    if stale_chase {
        return "stale_chase";
    }
    if !executable {
        return no_fill_reason.unwrap_or("no_fill");
    }
    match realized_pnl_pct {
        Some(v) if v >= 0.5 => "captured",
        Some(v) if v <= -1.0 => "bad_fill",
        Some(_) => "flat_fill",
        None => "no_pnl",
    }
}

fn infer_action(row: &ReviewRow) -> (&'static str, &'static str) {
    if row.selection_status != "selected" {
        return ("WAIT", "not_selected");
    }
    if let Some(action) = action_from_main_signal_gate(row) {
        return action;
    }
    if row.report_bucket != "CORE BOOK" {
        return ("OBSERVE", "report_bucket");
    }
    if row.direction == "bearish" {
        return ("RISK_AVOID", "long_only_bearish");
    }
    if row.direction != "bullish" {
        return ("WAIT", "neutral_signal");
    }
    if row.execution_mode == "do_not_chase" {
        return ("DO_NOT_CHASE", "execution_gate");
    }
    if row.gap_vs_chase_limit.unwrap_or(0.0) >= 1.0 {
        return ("DO_NOT_CHASE", "gap_vs_chase_limit");
    }
    if row.execution_mode == "wait_pullback" {
        return ("WAIT_PULLBACK", "execution_gate");
    }
    ("TRADE_NOW", "execution_gate")
}

fn fill_result(
    row: &ReviewRow,
    action_label: &str,
    stale_chase: bool,
) -> (
    bool,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<&'static str>,
) {
    if action_label != "TRADE_NOW" {
        let reason = match action_label {
            "OBSERVE" => "report_bucket_observation",
            "WAIT_PULLBACK" => "pullback_not_observable",
            "DO_NOT_CHASE" => "do_not_chase",
            "RISK_AVOID" => "risk_avoid",
            _ => "not_actionable",
        };
        return (false, None, None, None, Some(reason));
    }
    if stale_chase {
        return (false, None, None, None, Some("gap_exceeded_chase_limit"));
    }
    let Some(fill_price) = row.next_open.filter(|v| *v > 0.0) else {
        return (false, None, None, None, Some("missing_next_open"));
    };
    let Some(exit_price) = row.next_close.filter(|v| *v > 0.0) else {
        return (
            false,
            Some(fill_price),
            None,
            None,
            Some("missing_next_close"),
        );
    };
    let realized = Some((exit_price / fill_price - 1.0) * 100.0);
    (true, Some(fill_price), Some(exit_price), realized, None)
}

fn classify(
    row: &ReviewRow,
    action_label: &str,
    direction_right: bool,
    stale_chase: bool,
    executable: bool,
    realized_pnl_pct: Option<f64>,
    best_possible_ret_pct: Option<f64>,
) -> &'static str {
    if !row.data_ready {
        return "unresolved";
    }
    if row.selection_status == "ignored" {
        if direction_right {
            return "missed_alpha";
        }
        return "correct_ignore";
    }

    if action_label == "OBSERVE" {
        if direction_right {
            return "observed_alpha";
        }
        return "correct_observe";
    }

    if action_label == "TRADE_NOW" && executable {
        let realized = realized_pnl_pct.unwrap_or(0.0);
        if realized >= 0.5 {
            return "won_and_executable";
        }
        if realized <= -1.0 {
            return "false_positive_executable";
        }
        if direction_right && best_possible_ret_pct.unwrap_or(0.0) >= DIRECTION_THRESHOLD_PCT {
            return "right_but_poor_exit";
        }
        return "flat_no_edge";
    }

    if direction_right {
        if stale_chase {
            "stale_chase"
        } else {
            "right_but_no_fill"
        }
    } else {
        "correct_avoid"
    }
}

fn directional_best_ret(
    direction: &str,
    best_up_2d_pct: Option<f64>,
    best_down_2d_pct: Option<f64>,
) -> Option<f64> {
    match direction {
        "bearish" => best_down_2d_pct.map(|v| -v),
        "bullish" => best_up_2d_pct,
        _ => None,
    }
}

fn is_direction_right(best_possible_ret_pct: Option<f64>) -> bool {
    best_possible_ret_pct
        .map(|v| v >= DIRECTION_THRESHOLD_PCT)
        .unwrap_or(false)
}

fn feedback(label: &str) -> (Option<&'static str>, Option<f64>) {
    match label {
        "won_and_executable" => (Some("reward_executable_capture"), Some(0.5)),
        "false_positive_executable" => (Some("penalize_false_positive"), Some(1.1)),
        "stale_chase" | "right_but_no_fill" => (Some("penalize_stale_chase"), Some(0.9)),
        "missed_alpha" => (Some("boost_recall"), Some(1.0)),
        "correct_avoid" => (Some("reward_avoid"), Some(0.3)),
        _ => (None, None),
    }
}

fn parse_sql_date(raw: &str) -> Result<NaiveDate> {
    let trimmed = raw.trim();
    let date_part = trimmed.get(0..10).unwrap_or(trimmed);
    Ok(NaiveDate::parse_from_str(date_part, "%Y-%m-%d")?)
}

#[cfg(test)]
mod tests {
    use super::compute;
    use chrono::NaiveDate;
    use duckdb::Connection;

    fn setup_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(crate::storage::schema::CREATE_TABLES)
            .unwrap();
        db
    }

    fn insert_decision(db: &Connection, symbol: &str, selection_status: &str, mode: &str) {
        insert_decision_with_bucket(db, symbol, selection_status, mode, "CORE BOOK");
    }

    fn insert_decision_with_bucket(
        db: &Connection,
        symbol: &str,
        selection_status: &str,
        mode: &str,
        report_bucket: &str,
    ) {
        insert_decision_with_bucket_and_detail(
            db,
            symbol,
            selection_status,
            mode,
            report_bucket,
            "{}",
        );
    }

    fn insert_decision_with_bucket_and_detail(
        db: &Connection,
        symbol: &str,
        selection_status: &str,
        mode: &str,
        report_bucket: &str,
        details_json: &str,
    ) {
        db.execute(
            "INSERT INTO report_decisions (
                report_date, session, symbol, selection_status, rank_order,
                report_bucket, signal_direction, signal_confidence, composite_score, execution_mode,
                max_chase_gap_pct, reference_close, details_json
            ) VALUES ('2026-04-20', 'daily', ?, ?, 1, ?, 'bullish', 'HIGH', 0.7, ?, 2.0, 10.0, ?)",
            duckdb::params![symbol, selection_status, report_bucket, mode, details_json],
        )
        .unwrap();
    }

    fn insert_outcome(
        db: &Connection,
        symbol: &str,
        selection_status: &str,
        next_open: f64,
        next_close: f64,
        best_up_2d_pct: f64,
        gap_vs_limit: f64,
    ) {
        db.execute(
            "INSERT INTO report_outcomes (
                report_date, session, symbol, selection_status, evaluation_date,
                next_trade_date, reference_close, next_open, next_close,
                best_high_2d, worst_low_2d, next_open_ret_pct, next_close_ret_pct,
                best_up_2d_pct, best_down_2d_pct, gap_vs_chase_limit, data_ready
            ) VALUES (
                '2026-04-20', 'daily', ?, ?, '2026-04-24', '2026-04-21',
                10.0, ?, ?, 10.8, 9.8, 0.0, 3.0, ?, -2.0, ?, TRUE
            )",
            duckdb::params![
                symbol,
                selection_status,
                next_open,
                next_close,
                best_up_2d_pct,
                gap_vs_limit
            ],
        )
        .unwrap();
    }

    #[test]
    fn executable_uses_next_open_fill() {
        let db = setup_db();
        insert_decision(&db, "000001.SZ", "selected", "executable");
        insert_outcome(&db, "000001.SZ", "selected", 10.0, 10.4, 4.0, 0.5);

        let rows = compute(&db, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()).unwrap();

        assert_eq!(rows, 1);
        let row: (String, bool, String) = db
            .query_row(
                "SELECT action_label, executable, label FROM algorithm_postmortem WHERE symbol = '000001.SZ'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "TRADE_NOW".to_string(),
                true,
                "won_and_executable".to_string()
            )
        );
    }

    #[test]
    fn do_not_chase_is_stale_not_executable_capture() {
        let db = setup_db();
        insert_decision(&db, "000002.SZ", "selected", "do_not_chase");
        insert_outcome(&db, "000002.SZ", "selected", 10.3, 10.8, 8.0, 1.4);

        compute(&db, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()).unwrap();

        let row: (String, bool, bool, String) = db
            .query_row(
                "SELECT action_label, executable, stale_chase, label FROM algorithm_postmortem WHERE symbol = '000002.SZ'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "DO_NOT_CHASE".to_string(),
                false,
                true,
                "stale_chase".to_string()
            )
        );
    }

    #[test]
    fn non_core_selected_bucket_is_observation_not_trade_instruction() {
        let db = setup_db();
        insert_decision_with_bucket(&db, "000004.SZ", "selected", "executable", "THEME ROTATION");
        insert_outcome(&db, "000004.SZ", "selected", 10.0, 10.5, 6.0, 1.3);

        compute(&db, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()).unwrap();

        let row: (String, String, bool, bool, String, Option<String>, String) = db
            .query_row(
                "SELECT action_label, action_intent, executable, stale_chase, label, feedback_action, fill_quality
                 FROM algorithm_postmortem WHERE symbol = '000004.SZ'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "OBSERVE".to_string(),
                "OBSERVE".to_string(),
                false,
                false,
                "observed_alpha".to_string(),
                None,
                "report_bucket_observation".to_string(),
            )
        );
    }

    #[test]
    fn headline_only_main_signal_blocker_does_not_override_trade_default() {
        let db = setup_db();
        insert_decision_with_bucket_and_detail(
            &db,
            "000005.SZ",
            "selected",
            "executable",
            "CORE BOOK",
            r#"{"main_signal_gate":{"status":"blocked","role":"directional_observation","action_intent":"OBSERVE","blockers":["headline_gate_uncertain"]}}"#,
        );
        insert_outcome(&db, "000005.SZ", "selected", 10.0, 10.5, 6.0, 0.2);

        compute(&db, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()).unwrap();

        let row: (String, String, String, String, Option<String>) = db
            .query_row(
                "SELECT action_label, action_source, action_intent, label, feedback_action
                 FROM algorithm_postmortem WHERE symbol = '000005.SZ'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "TRADE_NOW".to_string(),
                "execution_gate".to_string(),
                "TRADE".to_string(),
                "won_and_executable".to_string(),
                Some("reward_executable_capture".to_string()),
            )
        );
    }

    #[test]
    fn ignored_follow_through_is_missed_alpha() {
        let db = setup_db();
        insert_decision(&db, "000003.SZ", "ignored", "executable");
        insert_outcome(&db, "000003.SZ", "ignored", 10.0, 10.3, 5.0, 0.2);

        compute(&db, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()).unwrap();

        let row: (String, String) = db
            .query_row(
                "SELECT action_label, label FROM algorithm_postmortem WHERE symbol = '000003.SZ'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("WAIT".to_string(), "missed_alpha".to_string()));
    }
}
