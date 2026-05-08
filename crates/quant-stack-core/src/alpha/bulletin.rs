use super::{
    source::{
        details_object, normalize_bucket, normalize_confidence, normalize_direction,
        normalize_execution, parse_details,
    },
    AlphaBulletin, BulletinItem, PolicyCandidate, TradeRow,
};
use chrono::NaiveDate;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

fn main_signal_gate(details: &Value) -> Value {
    details_object(details, "main_signal_gate")
}

fn headline_mode(row: &TradeRow, gate: &Value) -> Option<String> {
    row.headline_mode
        .as_deref()
        .or_else(|| gate.get("headline_mode").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_lowercase())
}

fn is_headline_blocker(value: &str) -> bool {
    let text = value.trim().to_lowercase();
    text.starts_with("headline_gate_") || text.contains("headline gate")
}

fn row_fields_pass(row: &TradeRow, gate: &Value) -> bool {
    let bucket = row
        .report_bucket
        .as_deref()
        .or_else(|| gate.get("report_bucket").and_then(Value::as_str));
    let direction = row
        .signal_direction
        .as_deref()
        .or_else(|| gate.get("direction").and_then(Value::as_str));
    let execution = row
        .execution_mode
        .as_deref()
        .or_else(|| gate.get("execution_action").and_then(Value::as_str))
        .or_else(|| gate.get("execution_mode").and_then(Value::as_str))
        .or_else(|| gate.get("action_intent").and_then(Value::as_str));

    normalize_bucket(bucket) == "core"
        && normalize_confidence(row.signal_confidence.as_deref()) == "high_mod"
        && matches!(normalize_direction(direction).as_str(), "long" | "short")
        && normalize_execution(execution) == "executable_now"
}

fn gate_passes(row: &TradeRow) -> bool {
    let details = parse_details(row);
    let gate = main_signal_gate(&details);
    if gate.is_object() && !gate.as_object().unwrap().is_empty() {
        let has_hard_blocker = gate
            .get("blockers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(|blocker| !is_headline_blocker(blocker));
        if has_hard_blocker {
            return false;
        }
        if gate.get("status").and_then(Value::as_str) == Some("pass")
            && gate.get("role").and_then(Value::as_str) == Some("main_signal")
        {
            return true;
        }
        return row_fields_pass(row, &gate);
    }
    row_fields_pass(row, &gate)
}

fn detail_f64(details: &Value, key: &str) -> Option<f64> {
    details.get(key).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
    })
}

fn cn_momentum_values(row: &TradeRow) -> (Option<f64>, Option<f64>) {
    let details = parse_details(row);
    (
        detail_f64(&details, "ret_5d"),
        detail_f64(&details, "ret_20d"),
    )
}

fn is_cn_lukewarm_momentum(row: &TradeRow) -> bool {
    if row.market != "cn" {
        return false;
    }
    let (ret_5d, _) = cn_momentum_values(row);
    ret_5d.is_some_and(|value| value > 3.0 && value < 8.0)
}

fn cn_execution_quality_blockers(row: &TradeRow) -> Vec<String> {
    if row.market != "cn" {
        return Vec::new();
    }
    let mut blockers = Vec::new();
    let bucket = normalize_bucket(row.report_bucket.as_deref());
    let score = row.composite_score;
    let rank = row.rank_order;
    let (ret_5d, ret_20d) = cn_momentum_values(row);

    if bucket == "radar" {
        blockers.push("cn_radar_not_tradeable".to_string());
    }
    if is_cn_lukewarm_momentum(row) {
        blockers.push("cn_lukewarm_momentum_3_8d".to_string());
    }
    if ret_20d.is_none() || ret_5d.is_none() {
        blockers.push("cn_missing_momentum_quality".to_string());
    }
    if score.is_some_and(|value| value < 0.50) {
        blockers.push("cn_score_below_0_50".to_string());
    }
    if rank.is_some_and(|value| value > 10) {
        blockers.push("cn_rank_outside_top10".to_string());
    }

    let strong_trend = ret_20d.is_some_and(|value| value >= 25.0)
        && ret_5d.is_some_and(|value| value >= 8.0 || value <= 3.0);
    let high_quality_score = score.is_some_and(|value| value >= 0.55)
        && ret_20d.is_some_and(|value| value >= 10.0)
        && !is_cn_lukewarm_momentum(row);
    if !strong_trend && !high_quality_score {
        blockers.push("cn_not_strong_trend_continuation".to_string());
    }

    dedupe(blockers)
}

fn cn_execution_quality_passes(row: &TradeRow) -> bool {
    cn_execution_quality_blockers(row).is_empty()
}

fn tactical_gate_passes(row: &TradeRow) -> bool {
    let details = parse_details(row);
    let gate = main_signal_gate(&details);
    let bucket = row
        .report_bucket
        .as_deref()
        .or_else(|| gate.get("report_bucket").and_then(Value::as_str));
    let direction = row
        .signal_direction
        .as_deref()
        .or_else(|| gate.get("direction").and_then(Value::as_str));
    let execution = row
        .execution_mode
        .as_deref()
        .or_else(|| gate.get("execution_action").and_then(Value::as_str))
        .or_else(|| gate.get("execution_mode").and_then(Value::as_str));
    normalize_bucket(bucket) == "theme_rotation"
        && normalize_confidence(row.signal_confidence.as_deref()) == "high_mod"
        && matches!(normalize_direction(direction).as_str(), "long" | "short")
        && normalize_execution(execution) == "executable_now"
}

fn probation_gate_passes(row: &TradeRow, blockers: &[String]) -> bool {
    let details = parse_details(row);
    let gate = main_signal_gate(&details);
    let bucket = row
        .report_bucket
        .as_deref()
        .or_else(|| gate.get("report_bucket").and_then(Value::as_str));
    let direction = row
        .signal_direction
        .as_deref()
        .or_else(|| gate.get("direction").and_then(Value::as_str));
    let execution = row
        .execution_mode
        .as_deref()
        .or_else(|| gate.get("execution_action").and_then(Value::as_str))
        .or_else(|| gate.get("execution_mode").and_then(Value::as_str))
        .or_else(|| gate.get("action_intent").and_then(Value::as_str));
    let bucket = normalize_bucket(bucket);
    let execution = normalize_execution(execution);
    let direction = normalize_direction(direction);
    let confidence = normalize_confidence(row.signal_confidence.as_deref());
    let rank_ok = row.rank_order.map_or(true, |rank| rank <= 10);
    let score_ok = row.composite_score.map_or(true, |score| {
        if row.market == "cn" {
            score >= 0.50
        } else {
            score >= 0.35
        }
    });
    let rr_ok = row.rr_ratio.map_or(true, |rr| rr >= 0.6);
    let has_sizing_or_rr =
        row.market == "cn" || row.rr_ratio.is_some() || gate.get("max_size").is_some();
    let penalty_disallowed = details
        .get("selection_penalties")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|penalty| {
            matches!(
                penalty,
                "low_dollar_volume" | "low_price" | "small_cap" | "poor_options"
            )
        });
    let disallowed = blockers.iter().any(|blocker| {
        matches!(
            blocker.as_str(),
            "confidence_low"
                | "confidence_no_signal"
                | "confidence_watch"
                | "neutral_direction"
                | "direction_neutral"
                | "direction_bearish"
                | "stale chase"
                | "stale_chase_or_do_not_chase"
                | "execution_do_not_chase"
                | "exhaustion_downgrade"
                | "fade_risk_above_core"
                | "no fill risk"
        )
    });
    let cn_quality_ok = row.market != "cn" || cn_execution_quality_passes(row);
    let bucket_ok = bucket == "core" || (row.market == "cn" && bucket == "theme_rotation");
    bucket_ok
        && direction == "long"
        && confidence == "high_mod"
        && execution == "executable_now"
        && rank_ok
        && score_ok
        && rr_ok
        && has_sizing_or_rr
        && cn_quality_ok
        && !penalty_disallowed
        && !disallowed
}

fn select_tactical_policy(candidates: &[PolicyCandidate]) -> Option<String> {
    candidates
        .iter()
        .filter(|candidate| {
            candidate.policy_id.contains(":theme_rotation:")
                && candidate.fail_reasons.len() == 1
                && candidate.fail_reasons[0] == "policy_bucket_not_core"
        })
        .max_by(|left, right| {
            left.stability_score
                .partial_cmp(&right.stability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|candidate| candidate.policy_id.clone())
}

fn market_ev_status(market: &str, selected_policy: Option<&String>) -> String {
    if market == "cn" {
        "cn_direct".to_string()
    } else if selected_policy.is_some() {
        "passed".to_string()
    } else {
        "failed".to_string()
    }
}

fn has_factor_lab_prior(row: &TradeRow) -> bool {
    let details = parse_details(row);
    let mut parts = vec![
        row.primary_reason.clone().unwrap_or_default(),
        row.report_bucket.clone().unwrap_or_default(),
        details
            .get("factor_lab")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    ];
    for key in [
        "lab_factor",
        "lab_is_fresh",
        "shadow_alpha_score",
        "shadow_rank_score",
    ] {
        if let Some(value) = details.get(key) {
            parts.push(value.to_string());
        }
    }
    let haystack = parts.join(" ").to_lowercase();
    ["factor lab", "lab_", "lab factor", "shadow_", "true"]
        .iter()
        .any(|token| haystack.contains(token))
}

fn candidate_blockers(row: &TradeRow, selected_policy_id: Option<&String>) -> Vec<String> {
    let details = parse_details(row);
    let gate = main_signal_gate(&details);
    let mut blockers = Vec::new();
    if let Some(items) = gate.get("blockers").and_then(Value::as_array) {
        blockers.extend(
            items
                .iter()
                .filter_map(Value::as_str)
                .filter(|blocker| !is_headline_blocker(blocker))
                .map(ToString::to_string),
        );
    }
    if row.market != "cn" {
        match selected_policy_id {
            None => blockers.push("EV unknown: no stable champion policy".to_string()),
            Some(policy) if row.policy_id != *policy => {
                blockers.push("EV unknown: outside selected champion policy".to_string())
            }
            _ => {}
        }
    }
    if matches!(
        normalize_bucket(row.report_bucket.as_deref()).as_str(),
        "radar" | "appendix" | "theme_rotation"
    ) {
        blockers.push("strategy/out-of-scope".to_string());
    }
    let execution = normalize_execution(
        row.execution_mode
            .as_deref()
            .or_else(|| gate.get("execution_mode").and_then(Value::as_str)),
    );
    if execution == "wait_pullback" {
        blockers.push("no fill risk".to_string());
    } else if execution == "do_not_chase" {
        blockers.push("stale chase".to_string());
    }
    if row.rr_ratio.is_some_and(|rr| rr < 1.5) {
        blockers.push("RR insufficient".to_string());
    }
    blockers.extend(cn_execution_quality_blockers(row));
    if blockers.is_empty() && !gate_passes(row) {
        blockers.push("main signal gate blocked".to_string());
    }
    dedupe(blockers)
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for value in values {
        if !value.is_empty() && seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

fn bulletin_item(
    row: &TradeRow,
    section: &str,
    reason: String,
    blockers: Vec<String>,
) -> BulletinItem {
    let details = parse_details(row);
    let gate = main_signal_gate(&details);
    let headline = headline_mode(row, &gate);
    BulletinItem {
        market: row.market.clone(),
        symbol: row.symbol.clone(),
        section: section.to_string(),
        policy_id: row.policy_id.clone(),
        policy_label: row.policy_label.clone(),
        report_bucket: row.report_bucket.clone(),
        signal_direction: row.signal_direction.clone(),
        signal_confidence: row.signal_confidence.clone(),
        headline_mode: headline.clone(),
        execution_mode: row.execution_mode.clone(),
        reason,
        blockers,
        details: json!({
            "rank_order": row.rank_order,
            "composite_score": row.composite_score,
            "selection_status": row.selection_status,
            "ret_5d": detail_f64(&details, "ret_5d"),
            "ret_20d": detail_f64(&details, "ret_20d"),
            "trend_prob": detail_f64(&details, "trend_prob"),
            "main_signal_gate": gate,
            "headline_context": {
                "mode": headline,
                "role": "context_only"
            },
        }),
    }
}

pub(super) fn build_bulletin(
    as_of: NaiveDate,
    evaluated_through: BTreeMap<String, String>,
    selected_policies: BTreeMap<String, Option<String>>,
    candidates_by_market: &BTreeMap<String, Vec<PolicyCandidate>>,
    current_by_market: &BTreeMap<String, Vec<TradeRow>>,
    options_by_market: &BTreeMap<String, Vec<BulletinItem>>,
) -> AlphaBulletin {
    let mut execution_alpha = Vec::new();
    let mut probation_alpha = Vec::new();
    let mut probation_candidates: BTreeMap<String, Vec<BulletinItem>> = BTreeMap::new();
    let mut tactical_alpha = Vec::new();
    let mut options_alpha = Vec::new();
    let mut recall_alpha = Vec::new();
    let mut blocked_alpha = Vec::new();
    let tactical_policies: BTreeMap<String, Option<String>> = candidates_by_market
        .iter()
        .map(|(market, candidates)| (market.clone(), select_tactical_policy(candidates)))
        .collect();
    let ev_status: BTreeMap<String, String> = candidates_by_market
        .keys()
        .map(|market| {
            let selected_policy = selected_policies.get(market).and_then(|p| p.as_ref());
            (market.clone(), market_ev_status(market, selected_policy))
        })
        .collect();
    for (market, rows) in current_by_market {
        let selected_policy = selected_policies.get(market).and_then(|p| p.as_ref());
        let tactical_policy = tactical_policies.get(market).and_then(|p| p.as_ref());
        for row in rows {
            let blockers = candidate_blockers(row, selected_policy);
            if (market == "cn" && gate_passes(row) && blockers.is_empty())
                || (selected_policy.is_some_and(|policy| row.policy_id == *policy)
                    && gate_passes(row))
            {
                let reason = if market == "cn" {
                    "CN direct execution gate passed; 30-day stable champion gate is not an execution blocker"
                } else {
                    "selected champion policy with passing execution gate; headline context is advisory"
                };
                execution_alpha.push(bulletin_item(
                    row,
                    "execution_alpha",
                    reason.to_string(),
                    Vec::new(),
                ));
            } else if tactical_policy.is_some_and(|policy| row.policy_id == *policy)
                && tactical_gate_passes(row)
            {
                tactical_alpha.push(bulletin_item(
                    row,
                    "tactical_alpha",
                    "stable theme-rotation policy; tactical follow-through only, not CORE BOOK execution alpha"
                        .to_string(),
                    vec![
                        "strategy/out-of-scope for core execution".to_string(),
                        "use pullback/liquidity confirmation".to_string(),
                    ],
                ));
            } else {
                if probation_gate_passes(row, &blockers) {
                    probation_candidates.entry(market.clone()).or_default().push(
                        bulletin_item(
                            row,
                            "probation_alpha",
                            if market == "cn" {
                                "Probation Alpha: CN strong-trend continuation only; avoid lukewarm 5D momentum; 0.25R/0.5R max trial only"
                            } else {
                                "Probation Alpha: 0.25R/0.5R max trial only; EV/stability gate is not strong enough for formal Fresh Entry"
                            }
                            .to_string(),
                            blockers.clone(),
                        ),
                    );
                }
                if has_factor_lab_prior(row)
                    || selected_policy.is_some_and(|policy| row.policy_id == *policy)
                {
                    recall_alpha.push(bulletin_item(
                        row,
                        "recall_alpha",
                        "Factor Lab research prior / recall lead; not promoted to Execution Alpha"
                            .to_string(),
                        blockers,
                    ));
                } else {
                    let reason = if blockers.is_empty() {
                        "outside execution-alpha scope".to_string()
                    } else {
                        blockers.join("; ")
                    };
                    blocked_alpha.push(bulletin_item(row, "blocked_alpha", reason, blockers));
                }
            }
        }
        let has_execution = execution_alpha.iter().any(|item| item.market == *market);
        if !has_execution {
            let mut seen = HashSet::new();
            if let Some(candidates) = probation_candidates.get_mut(market) {
                candidates.sort_by(|left, right| {
                    let left_rank = left
                        .details
                        .get("rank_order")
                        .and_then(Value::as_i64)
                        .unwrap_or(i64::MAX);
                    let right_rank = right
                        .details
                        .get("rank_order")
                        .and_then(Value::as_i64)
                        .unwrap_or(i64::MAX);
                    left_rank.cmp(&right_rank)
                });
                probation_alpha.extend(
                    candidates
                        .iter()
                        .filter(|item| seen.insert(item.symbol.clone()))
                        .take(2)
                        .cloned(),
                );
            }
        }
        if let Some(items) = options_by_market.get(market) {
            options_alpha.extend(items.iter().cloned());
        }
    }
    AlphaBulletin {
        as_of: as_of.to_string(),
        evaluated_through,
        ev_status,
        selected_policies,
        tactical_policies,
        stability: candidates_by_market.clone(),
        execution_alpha,
        probation_alpha,
        tactical_alpha,
        options_alpha,
        recall_alpha,
        blocked_alpha,
    }
}

pub fn render_market_bulletin_md(bulletin: &AlphaBulletin, market: &str) -> String {
    let market_upper = market.to_uppercase();
    let selected = bulletin
        .selected_policies
        .get(market)
        .and_then(|v| v.as_deref())
        .unwrap_or("none");
    let tactical = bulletin
        .tactical_policies
        .get(market)
        .and_then(|v| v.as_deref())
        .unwrap_or("none");
    let evaluated = bulletin
        .evaluated_through
        .get(market)
        .map(String::as_str)
        .unwrap_or("unknown");
    let ev_status = bulletin
        .ev_status
        .get(market)
        .map(String::as_str)
        .unwrap_or_else(|| {
            if selected == "none" {
                "failed"
            } else {
                "passed"
            }
        });
    let ev_note = match ev_status {
        "passed" => {
            "stable champion selected; Execution Alpha may be emitted only for matching current candidates"
        }
        "failed" => {
            "stable gate evaluated; no champion policy passed, so Setup/Recall names remain review-only"
        }
        "cn_direct" => {
            "A-share 30-day stable champion gate is bypassed; current execution gate, liquidity, scope, and risk blockers control promotion"
        }
        "pending" => {
            "stable gate not evaluated yet; do not treat pending as no champion or EV failure"
        }
        _ => "stable gate status unknown; do not promote candidates without explicit pass",
    };
    let mut lines = vec![
        format!("## {market_upper} Stable Alpha Bulletin"),
        String::new(),
        format!("- as_of: {}", bulletin.as_of),
        format!("- evaluated_through: {evaluated}"),
        format!("- ev_status: `{ev_status}`"),
        format!("- selected_policy: `{selected}`"),
        format!("- tactical_policy: `{tactical}`"),
        format!("- ev_note: {ev_note}"),
        "- headline: advisory context only, not an execution blocker".to_string(),
        String::new(),
    ];
    let execution_empty = if market == "cn" {
        "None. No current CN candidate passed the execution gate."
    } else {
        "None. No current candidate passed both the stability champion and execution gates."
    };
    render_section(
        &mut lines,
        "Equity Execution Alpha",
        &bulletin.execution_alpha,
        market,
        execution_empty,
    );
    render_section(
        &mut lines,
        "Probation Alpha / Trial Tickets",
        &bulletin.probation_alpha,
        market,
        "None. No blocked candidate met the small-size probation screen.",
    );
    render_section(
        &mut lines,
        "Tactical / Theme Rotation Alpha",
        &bulletin.tactical_alpha,
        market,
        "None. No stable non-core theme-rotation candidate passed the tactical screen.",
    );
    render_section(
        &mut lines,
        "Options / Shadow Options Alpha",
        &bulletin.options_alpha,
        market,
        "None. No real-options or shadow-options candidate passed the daily options-alpha screen.",
    );
    render_section(
        &mut lines,
        "Recall Alpha",
        &bulletin.recall_alpha,
        market,
        "None. No Factor Lab research prior / recall lead requires follow-up.",
    );
    render_section(
        &mut lines,
        "Blocked / Out-of-scope Alpha",
        &bulletin.blocked_alpha,
        market,
        "None. No blocked current candidates were found.",
    );
    format!("{}\n", lines.join("\n").trim_end())
}

fn render_section(
    lines: &mut Vec<String>,
    title: &str,
    items: &[BulletinItem],
    market: &str,
    empty: &str,
) {
    lines.push(format!("### {title}"));
    lines.push(String::new());
    let market_items: Vec<&BulletinItem> = items
        .iter()
        .filter(|item| item.market == market)
        .take(20)
        .collect();
    if market_items.is_empty() {
        lines.push(format!("- {empty}"));
        lines.push(String::new());
        return;
    }
    for item in market_items {
        if item.section == "options_alpha" {
            render_options_item(lines, item);
            continue;
        }
        let blockers = if item.blockers.is_empty() {
            String::new()
        } else {
            format!(" Blockers: {}.", item.blockers.join(", "))
        };
        let headline = item
            .headline_mode
            .as_deref()
            .map(|mode| format!(" Headline `{mode}` is context only."))
            .unwrap_or_default();
        lines.push(format!(
            "- `{}` - {}. Policy `{}`; lane `{}`; confidence `{}`.{}{}",
            item.symbol,
            item.reason,
            item.policy_id,
            item.report_bucket.as_deref().unwrap_or("unknown"),
            item.signal_confidence.as_deref().unwrap_or("unknown"),
            headline,
            blockers
        ));
    }
    lines.push(String::new());
}

fn render_options_item(lines: &mut Vec<String>, item: &BulletinItem) {
    let blockers = if item.blockers.is_empty() {
        String::new()
    } else {
        format!(" Blockers: {}.", item.blockers.join(", "))
    };
    let source = item
        .details
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let expression = item
        .details
        .get("expression")
        .and_then(Value::as_str)
        .or(item.execution_mode.as_deref())
        .unwrap_or("unknown");
    let edge_text = if source == "real_options" {
        format!(
            " directional_edge `{}`; vol_edge `{}`; vrp_edge `{}`; flow_edge `{}`.",
            value_text(item.details.get("directional_edge")),
            value_text(item.details.get("vol_edge")),
            value_text(item.details.get("vrp_edge")),
            value_text(item.details.get("flow_edge"))
        )
    } else {
        format!(
            " shadow_alpha_prob `{}`; entry_quality `{}`; stale_chase_risk `{}`.",
            value_text(item.details.get("shadow_alpha_prob")),
            value_text(item.details.get("entry_quality_score")),
            value_text(item.details.get("stale_chase_risk"))
        )
    };
    lines.push(format!(
        "- `{}` - {}. Expression `{}`; source `{}`;{}{}",
        item.symbol, item.reason, expression, source, edge_text, blockers
    ));
}

fn value_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Null) | None => "-".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str, rank: i64, score: f64, confidence: &str) -> TradeRow {
        TradeRow {
            market: "us".to_string(),
            symbol: symbol.to_string(),
            report_bucket: Some("core".to_string()),
            signal_direction: Some("long".to_string()),
            signal_confidence: Some(confidence.to_string()),
            execution_mode: Some("executable_now".to_string()),
            rank_order: Some(rank),
            composite_score: Some(score),
            rr_ratio: Some(0.8),
            policy_id: "us:core:long:high_mod:executable_now:h3".to_string(),
            policy_label: "US core long high/mod executable now 3D".to_string(),
            ..TradeRow::default()
        }
    }

    fn cn_theme_row(symbol: &str, rank: i64, score: f64, ret_5d: f64, ret_20d: f64) -> TradeRow {
        TradeRow {
            market: "cn".to_string(),
            symbol: symbol.to_string(),
            report_bucket: Some("THEME ROTATION".to_string()),
            signal_direction: Some("long".to_string()),
            signal_confidence: Some("MODERATE".to_string()),
            execution_mode: Some("executable".to_string()),
            rank_order: Some(rank),
            composite_score: Some(score),
            policy_id: "cn:theme_rotation:long:high_mod:executable:h2".to_string(),
            policy_label: "CN theme rotation long high/mod executable 2D".to_string(),
            details_json: Some(
                json!({
                    "lab_is_fresh": true,
                    "ret_5d": ret_5d,
                    "ret_20d": ret_20d,
                    "trend_prob": 0.58
                })
                .to_string(),
            ),
            ..TradeRow::default()
        }
    }

    #[test]
    fn build_bulletin_surfaces_small_size_probation_alpha_without_champion_policy() {
        let mut selected = BTreeMap::new();
        selected.insert("us".to_string(), None);
        let mut evaluated = BTreeMap::new();
        evaluated.insert("us".to_string(), "2026-05-04".to_string());
        let mut candidates = BTreeMap::new();
        candidates.insert("us".to_string(), Vec::<PolicyCandidate>::new());
        let mut current = BTreeMap::new();
        current.insert("us".to_string(), vec![row("DDOG", 1, 0.52, "HIGH")]);
        let options = BTreeMap::new();

        let bulletin = build_bulletin(
            NaiveDate::from_ymd_opt(2026, 5, 7).unwrap(),
            evaluated,
            selected,
            &candidates,
            &current,
            &options,
        );

        assert!(bulletin.execution_alpha.is_empty());
        assert_eq!(bulletin.probation_alpha[0].section, "probation_alpha");
        assert_eq!(bulletin.probation_alpha[0].symbol, "DDOG");
        assert!(bulletin.probation_alpha[0]
            .reason
            .contains("0.25R/0.5R max trial"));
        assert_eq!(bulletin.blocked_alpha[0].symbol, "DDOG");
    }

    #[test]
    fn probation_alpha_rejects_low_confidence_rows() {
        let blockers = vec!["confidence_low".to_string()];
        assert!(!probation_gate_passes(
            &row("DVA", 1, 0.52, "LOW"),
            &blockers
        ));
    }

    #[test]
    fn probation_alpha_rejects_low_liquidity_penalties() {
        let mut row = row("ABSI", 1, 0.52, "HIGH");
        row.details_json = Some(
            json!({
                "selection_penalties": ["low_dollar_volume"],
                "main_signal_gate": {
                    "max_size": "0.10R/name",
                    "execution_action": "executable_now"
                }
            })
            .to_string(),
        );

        assert!(!probation_gate_passes(&row, &[]));
    }

    #[test]
    fn cn_probation_rejects_lukewarm_momentum() {
        let row = cn_theme_row("600703.SH", 3, 0.56, 5.5, 20.0);
        let blockers = cn_execution_quality_blockers(&row);

        assert!(blockers.contains(&"cn_lukewarm_momentum_3_8d".to_string()));
        assert!(!probation_gate_passes(&row, &blockers));
    }

    #[test]
    fn cn_probation_allows_strong_trend_continuation() {
        let row = cn_theme_row("603002.SH", 3, 0.52, 12.0, 55.0);

        assert!(cn_execution_quality_blockers(&row).is_empty());
        assert!(probation_gate_passes(&row, &[]));
    }
}
