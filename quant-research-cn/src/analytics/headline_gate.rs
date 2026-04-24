use chrono::NaiveDate;
use duckdb::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct HeadlineSignalSummary {
    pub direction: String,
    pub trend_prob: Option<f64>,
    pub report_bucket: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HeadlineGateInputs {
    pub p_ret_positive: Option<f64>,
    pub brier_score: Option<f64>,
    pub calibration_n: u64,
    pub regime_duration_days: i64,
    pub trend_prob_min: Option<f64>,
    pub trend_prob_max: Option<f64>,
    pub trend_prob_span: Option<f64>,
    pub direction_concentration: Option<f64>,
    pub dominant_direction: String,
    pub vol_hmm_regime: Option<String>,
    pub macro_vol_state: Option<String>,
    pub gate_multiplier: Option<f64>,
    pub vol_macro_conflict: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct HeadlineGateSummary {
    pub mode: String,
    pub bias: String,
    pub allow_directional_regime: bool,
    pub reporting_rule: String,
    pub reasons: Vec<String>,
    pub inputs: HeadlineGateInputs,
}

pub fn summarize_headline_gate(
    db: &Connection,
    as_of: NaiveDate,
    signals: &[HeadlineSignalSummary],
) -> HeadlineGateSummary {
    let date_str = as_of.to_string();
    let p_ret_positive = query_metric_value(db, &date_str, "hmm", "p_ret_positive");
    let brier_score = query_metric_value(db, &date_str, "hmm", "brier_score");
    let regime_duration_days =
        query_metric_value(db, &date_str, "hmm", "regime_duration").unwrap_or(0.0) as i64;
    let p_bull_detail = query_metric_detail(db, &date_str, "hmm", "p_bull");
    let calibration_n = parse_json_u64(p_bull_detail.as_deref(), "n").unwrap_or(0);

    let gate_multiplier = query_metric_value(db, &date_str, "macro_gate", "gate_multiplier");
    let gate_detail = query_metric_detail(db, &date_str, "macro_gate", "gate_multiplier");
    let macro_vol_state = parse_json_string(gate_detail.as_deref(), "vol_regime");

    let vol_hmm_detail = query_metric_detail(db, &date_str, "vol_hmm", "p_high_vol");
    let vol_hmm_regime = parse_json_string(vol_hmm_detail.as_deref(), "regime");

    let directional_focus: Vec<&HeadlineSignalSummary> = {
        let core_directional: Vec<&HeadlineSignalSummary> = signals
            .iter()
            .filter(|item| item.report_bucket == "CORE BOOK" && item.direction != "neutral")
            .collect();
        if core_directional.is_empty() {
            signals
                .iter()
                .filter(|item| item.direction != "neutral")
                .collect()
        } else {
            core_directional
        }
    };

    let bullish = directional_focus
        .iter()
        .filter(|item| item.direction == "bullish")
        .count();
    let bearish = directional_focus
        .iter()
        .filter(|item| item.direction == "bearish")
        .count();
    let total_directional = directional_focus.len();
    let dominant_direction = if bullish > bearish {
        "bullish".to_string()
    } else if bearish > bullish {
        "bearish".to_string()
    } else {
        "neutral".to_string()
    };
    let direction_concentration = if total_directional > 0 {
        Some((bullish.max(bearish) as f64) / (total_directional as f64))
    } else {
        None
    };

    let trend_probs: Vec<f64> = signals.iter().filter_map(|item| item.trend_prob).collect();
    let trend_prob_min = trend_probs.iter().copied().reduce(f64::min);
    let trend_prob_max = trend_probs.iter().copied().reduce(f64::max);
    let trend_prob_span = match (trend_prob_min, trend_prob_max) {
        (Some(min_v), Some(max_v)) => Some((max_v - min_v).abs()),
        _ => None,
    };

    let vol_macro_conflict = match (vol_hmm_regime.as_deref(), macro_vol_state.as_deref()) {
        (Some(vol_hmm), Some(macro_vol)) => {
            let vol_hmm_low = vol_hmm.contains("low");
            let vol_hmm_high = vol_hmm.contains("high") || vol_hmm.contains("panic");
            let macro_low = macro_vol == "calm" || macro_vol == "low";
            let macro_high = macro_vol == "elevated" || macro_vol == "high" || macro_vol == "panic";
            (vol_hmm_low && macro_high) || (vol_hmm_high && macro_low)
        }
        _ => false,
    };

    let edge = p_ret_positive.map(|p| (p - 0.5).abs());
    let mut reasons: Vec<String> = Vec::new();

    if calibration_n > 0 && calibration_n < 20 {
        reasons.push(format!("HMM 校准样本过少 (n={})", calibration_n));
    }
    if let Some(edge_v) = edge {
        if edge_v < 0.03 {
            reasons.push(format!("p_ret_positive 靠近 0.5 (edge={:.3})", edge_v));
        }
    } else {
        reasons.push("缺少 p_ret_positive".to_string());
    }
    if let Some(brier) = brier_score {
        if brier >= 0.24 {
            reasons.push(format!("Brier 接近 coin flip ({:.3})", brier));
        }
    }
    if regime_duration_days < 3 {
        reasons.push(format!("regime_duration 过短 ({}天)", regime_duration_days));
    }
    if let Some(span) = trend_prob_span {
        if span < 0.03 {
            reasons.push(format!("trend_prob 横截面极差过小 (span={:.3})", span));
        }
    }
    if let Some(concentration) = direction_concentration {
        if total_directional >= 4 && concentration > 0.75 {
            reasons.push(format!(
                "方向集中度过高 (dominant={} {:.0}%)",
                dominant_direction,
                concentration * 100.0
            ));
        }
    }
    if vol_macro_conflict {
        reasons.push("vol HMM 与 macro gate 方向冲突".to_string());
    }

    let mode = if p_ret_positive.is_none()
        || edge.unwrap_or(0.0) < 0.03
        || brier_score.map(|v| v >= 0.24).unwrap_or(false)
        || calibration_n < 20
        || trend_prob_span.map(|v| v < 0.03).unwrap_or(false)
        || direction_concentration
            .map(|v| total_directional >= 4 && v > 0.75)
            .unwrap_or(false)
        || vol_macro_conflict
    {
        "uncertain"
    } else if edge.unwrap_or(0.0) >= 0.06
        && regime_duration_days >= 3
        && trend_prob_span.map(|v| v >= 0.05).unwrap_or(true)
        && direction_concentration.map(|v| v <= 0.65).unwrap_or(true)
        && !vol_macro_conflict
    {
        "trend"
    } else {
        "range"
    };

    if reasons.is_empty() {
        reasons.push(match mode {
            "trend" => "方向性边际、持续性和横截面分化均满足阈值".to_string(),
            "range" => "存在局部机会，但不足以 headline 成单边市场".to_string(),
            _ => "方向性优势不足，必须降级为不确定模式".to_string(),
        });
    }

    let bias = match p_ret_positive {
        Some(p) if p >= 0.53 && mode != "uncertain" => "bullish".to_string(),
        Some(p) if p <= 0.47 && mode != "uncertain" => "bearish".to_string(),
        _ => "neutral".to_string(),
    };

    HeadlineGateSummary {
        mode: mode.to_string(),
        bias,
        allow_directional_regime: mode == "trend",
        reporting_rule: match mode {
            "trend" => "允许 headline 成趋势，但仍需附失效条件。".to_string(),
            "range" => "禁止 headline 成牛/熊，只能写区间、轮动与触发条件。".to_string(),
            _ => "禁止 headline 成牛/熊，也禁止把主书写成单边方向。".to_string(),
        },
        reasons,
        inputs: HeadlineGateInputs {
            p_ret_positive,
            brier_score,
            calibration_n,
            regime_duration_days,
            trend_prob_min,
            trend_prob_max,
            trend_prob_span,
            direction_concentration,
            dominant_direction,
            vol_hmm_regime,
            macro_vol_state,
            gate_multiplier,
            vol_macro_conflict,
        },
    }
}

fn query_metric_value(db: &Connection, date_str: &str, module: &str, metric: &str) -> Option<f64> {
    db.query_row(
        "SELECT value FROM analytics
         WHERE ts_code = '_MARKET'
           AND as_of = (
               SELECT MAX(as_of) FROM analytics
               WHERE ts_code = '_MARKET'
                 AND as_of <= CAST(? AS DATE)
                 AND module = ?
                 AND metric = ?
           )
           AND module = ?
           AND metric = ?",
        duckdb::params![date_str, module, metric, module, metric],
        |row| row.get::<_, f64>(0),
    )
    .ok()
}

fn query_metric_detail(
    db: &Connection,
    date_str: &str,
    module: &str,
    metric: &str,
) -> Option<String> {
    db.query_row(
        "SELECT detail FROM analytics
         WHERE ts_code = '_MARKET'
           AND as_of = (
               SELECT MAX(as_of) FROM analytics
               WHERE ts_code = '_MARKET'
                 AND as_of <= CAST(? AS DATE)
                 AND module = ?
                 AND metric = ?
           )
           AND module = ?
           AND metric = ?",
        duckdb::params![date_str, module, metric, module, metric],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
}

fn parse_json_string(detail: Option<&str>, key: &str) -> Option<String> {
    let detail = detail?;
    let value: serde_json::Value = serde_json::from_str(detail).ok()?;
    value.get(key)?.as_str().map(ToOwned::to_owned)
}

fn parse_json_u64(detail: Option<&str>, key: &str) -> Option<u64> {
    let detail = detail?;
    let value: serde_json::Value = serde_json::from_str(detail).ok()?;
    value.get(key)?.as_u64()
}
