/// Payload renderer — generates structured Markdown for 4 Claude agent consumption.
///
/// Outputs 3 split files:
///   1. `{date}_payload_macro.md`      → 宏观分析师 + 风险分析师
///   2. `{date}_payload_structural.md`  → 量化分析师 + 风险分析师
///   3. `{date}_payload_events.md`      → 事件分析师
///
/// The payload is the program's output. ALL arithmetic is computed upstream.
/// Agents NEVER touch arithmetic — every number they see is pre-computed.
use anyhow::Result;
use chrono::{FixedOffset, NaiveDate, Utc};
use duckdb::Connection;
use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;
use tracing::{info, warn};

use crate::analytics::headline_gate::{
    summarize_headline_gate, HeadlineGateSummary, HeadlineSignalSummary,
};
use crate::analytics::shadow_calibration::summarize_shadow_calibration;
use crate::config::Settings;
use crate::filtering::notable::NotableItem;

/// Precision rules reminder prepended to every payload file.
const PRECISION_RULES: &str = "\
> **精度规则 (Precision Rules)**
> - 禁止 P=1.00 或 P=0.00 (不允许确定性声明)
> - 所有概率保留3位小数
> - 必须注明样本量 (n=)
> - 必须注明数据时滞 (月度指标滞后说明)
> - 区分先验概率 / 条件概率 / 后验概率\n";

// ═════════════════════════════════════════════════════════════════════════════
// Public entry point
// ═════════════════════════════════════════════════════════════════════════════

pub fn render_payload(
    db: &Connection,
    _cfg: &Settings,
    as_of: NaiveDate,
    notable: &[NotableItem],
) -> Result<String> {
    let shanghai = FixedOffset::east_opt(8 * 3600).unwrap();
    let generated_at = Utc::now()
        .with_timezone(&shanghai)
        .format("%Y-%m-%d %H:%M:%S CST")
        .to_string();

    let date_str = as_of.to_string();
    std::fs::create_dir_all("reports")?;

    // ── File 1: Macro ─────────────────────────────────────────────────────
    let macro_md = render_macro(db, &date_str, &generated_at, notable)?;
    let macro_path = format!("reports/{}_payload_macro.md", as_of);
    std::fs::write(&macro_path, &macro_md)?;
    info!(path = %macro_path, bytes = macro_md.len(), "macro payload written");

    // ── File 2: Structural ────────────────────────────────────────────────
    let structural_md = render_structural(db, &date_str, &generated_at, notable)?;
    let structural_path = format!("reports/{}_payload_structural.md", as_of);
    std::fs::write(&structural_path, &structural_md)?;
    info!(path = %structural_path, bytes = structural_md.len(), "structural payload written");

    // ── File 3: Events ────────────────────────────────────────────────────
    let events_md = render_events(db, &date_str, &generated_at)?;
    let events_path = format!("reports/{}_payload_events.md", as_of);
    std::fs::write(&events_path, &events_md)?;
    info!(path = %events_path, bytes = events_md.len(), "events payload written");

    // Return the macro path as the "main" payload path (for backward compat)
    Ok(macro_path)
}

// ═════════════════════════════════════════════════════════════════════════════
// File 1: Macro — 宏观环境
// ═════════════════════════════════════════════════════════════════════════════

fn render_macro(
    db: &Connection,
    date_str: &str,
    generated_at: &str,
    notable: &[NotableItem],
) -> Result<String> {
    let mut md = String::with_capacity(32 * 1024);
    let headline_gate = compute_headline_gate(db, date_str, notable);

    writeln!(md, "# A股量化研究 Payload — 宏观环境")?;
    writeln!(md, "## 生成时间: {}", generated_at)?;
    writeln!(md, "## 交易日: {}", date_str)?;
    writeln!(md)?;
    writeln!(md, "{}", PRECISION_RULES)?;

    // ── 大盘概览 ──────────────────────────────────────────────────────────
    render_benchmark_overview(&mut md, db, date_str)?;

    // ── Headline Gate ────────────────────────────────────────────────────
    render_headline_gate(&mut md, &headline_gate)?;

    // ── HMM 市场状态 ──────────────────────────────────────────────────────
    render_hmm_state(&mut md, db, date_str)?;

    // ── 波动率HMM ─────────────────────────────────────────────────────────
    render_vol_hmm(&mut md, db, date_str)?;

    // ── 宏观网关 ──────────────────────────────────────────────────────────
    render_macro_gate(&mut md, db, date_str)?;

    // ── 宏观数据 ──────────────────────────────────────────────────────────
    render_macro_data(&mut md, db, date_str)?;

    // ── 北向资金 ──────────────────────────────────────────────────────────
    render_northbound_flow(&mut md, db, date_str)?;

    // ── 行业资金流向 ──────────────────────────────────────────────────────
    render_sector_fund_flow(&mut md, db, date_str)?;

    // ── 概念板块热点 ──────────────────────────────────────────────────────
    render_concept_board(&mut md, db, date_str)?;

    // ── 概念主题聚类 (DeepSeek) ──────────────────────────────────────────
    render_theme_clusters(&mut md, db, date_str)?;

    // ── ETF期权活跃度 ────────────────────────────────────────────────────
    render_etf_options(&mut md, db, date_str)?;

    // ── 板块轮动 ──────────────────────────────────────────────────────
    render_sector_rotation(&mut md, db, date_str)?;

    // ── 跨市场信号 ──────────────────────────────────────────────────────
    render_cross_market(&mut md, db, date_str)?;

    Ok(md)
}

fn render_benchmark_overview(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 大盘概览")?;
    writeln!(md)?;

    let benchmarks = [
        ("000300.SH", "沪深300"),
        ("000016.SH", "上证50"),
        ("399006.SZ", "创业板指"),
    ];

    let sql = "SELECT close, pct_chg, vol
               FROM prices
               WHERE ts_code = ? AND trade_date = (
                   SELECT MAX(trade_date) FROM prices
                   WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
               )";

    for (code, label) in benchmarks {
        match db.prepare(sql).and_then(|mut stmt| {
            stmt.query_row(duckdb::params![code, code, date_str], |row| {
                Ok((
                    row.get::<_, Option<f64>>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                ))
            })
        }) {
            Ok((close, pct_chg, vol)) => {
                writeln!(
                    md,
                    "- **{}** ({}): close={}, pct_chg={}%, vol={}",
                    label,
                    code,
                    fmt_f64(close),
                    fmt_f64(pct_chg),
                    fmt_vol(vol),
                )?;
            }
            Err(_) => {
                writeln!(md, "- **{}** ({}): 数据缺失", label, code)?;
            }
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_hmm_state(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### HMM 市场状态")?;
    writeln!(md)?;

    // Query all HMM metrics from analytics
    let sql = "SELECT metric, value, detail
               FROM analytics
               WHERE ts_code = '_MARKET'
                 AND module = 'hmm'
                 AND as_of = (
                     SELECT MAX(as_of) FROM analytics
                     WHERE ts_code = '_MARKET'
                       AND as_of <= CAST(? AS DATE)
                       AND module = 'hmm'
                 )
               ORDER BY metric";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, f64, Option<String>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- HMM 数据缺失 (样本不足或模块未运行)")?;
            } else {
                // Extract regime label from p_bull detail JSON
                let mut regime_label = String::new();
                let mut n_samples = 0u64;
                if let Some((_, _, Some(detail))) = rows.iter().find(|(m, _, _)| m == "p_bull") {
                    // Parse "regime":"consolidation" from JSON
                    if let Some(start) = detail.find("\"regime\":\"") {
                        let rest = &detail[start + 10..];
                        if let Some(end) = rest.find('"') {
                            regime_label = rest[..end].to_string();
                        }
                    }
                    if let Some(start) = detail.find("\"n\":") {
                        let rest = &detail[start + 4..];
                        let num_str: String =
                            rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                        n_samples = num_str.parse().unwrap_or(0);
                    }
                }

                for (metric, value, _detail) in &rows {
                    match metric.as_str() {
                        "p_bull" => {
                            if n_samples > 0 {
                                writeln!(md, "- P(bull) = {:.3} (n={}个交易日)", value, n_samples)?;
                            } else {
                                writeln!(md, "- P(bull) = {:.3}", value)?;
                            }
                            if !regime_label.is_empty() {
                                writeln!(md, "- current_regime = {}", regime_label)?;
                            }
                        }
                        "p_ret_positive" => {
                            writeln!(md, "- p_ret_positive = {:.4}", value)?;
                        }
                        "regime_duration" => {
                            writeln!(md, "- regime_duration = {} 天", *value as i64)?;
                        }
                        "brier_score" => writeln!(md, "- brier_score = {:.4}", value)?,
                        "hit_rate" => writeln!(md, "- hit_rate = {:.3}", value)?,
                        _ => {}
                    }
                }
            }
        }
        Err(_) => {
            writeln!(md, "- HMM 数据缺失")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_vol_hmm(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 波动率HMM (Vol Regime)")?;
    writeln!(md)?;

    let sql = "SELECT metric, value, detail
               FROM analytics
               WHERE ts_code = '_MARKET'
                 AND module = 'vol_hmm'
                 AND as_of = (
                     SELECT MAX(as_of) FROM analytics
                     WHERE ts_code = '_MARKET'
                       AND as_of <= CAST(? AS DATE)
                       AND module = 'vol_hmm'
                 )
               ORDER BY CASE metric
                          WHEN 'p_high_vol' THEN 1
                          WHEN 'p_high_vol_tomorrow' THEN 2
                          WHEN 'rv_tobit_20d' THEN 3
                          WHEN 'rv_raw_20d' THEN 4
                          WHEN 'limit_censor_ratio_20d' THEN 5
                          WHEN 'limit_up_count_20d' THEN 6
                          WHEN 'limit_down_count_20d' THEN 7
                          WHEN 'vol_regime_duration' THEN 8
                          ELSE 99
                        END,
                        metric";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, f64, Option<String>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- 波动率HMM 数据缺失")?;
            } else {
                // Parse detail from p_high_vol
                let mut regime_label = String::new();
                let mut source_label = String::new();
                let mut vol_low = 0.0;
                let mut vol_high = 0.0;
                let mut n_samples = 0u64;
                if let Some((_, _, Some(detail))) = rows.iter().find(|(m, _, _)| m == "p_high_vol")
                {
                    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(detail) {
                        source_label = obj
                            .get("source")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        regime_label = obj
                            .get("regime")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        if let Some(arr) = obj.get("vol_approx").and_then(|v| v.as_array()) {
                            vol_low = arr.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                            vol_high = arr.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        }
                        n_samples = obj.get("n").and_then(|v| v.as_u64()).unwrap_or(0);
                    }
                }

                for (metric, value, _) in &rows {
                    match metric.as_str() {
                        "p_high_vol" => {
                            writeln!(md, "- P(high_vol) = {:.3} (n={})", value, n_samples)?;
                            if source_label == "cross_section_limit_tobit" {
                                writeln!(md, "- 波动输入 = 全市场个股涨跌停修正 Tobit 方差")?;
                            }
                            if !regime_label.is_empty() {
                                writeln!(md, "- vol_regime = {}", regime_label)?;
                            }
                            if vol_low > 0.0 || vol_high > 0.0 {
                                writeln!(
                                    md,
                                    "- 典型波动率: low={:.1}%, high={:.1}%",
                                    vol_low, vol_high
                                )?;
                            }
                        }
                        "p_high_vol_tomorrow" => {
                            writeln!(md, "- P(high_vol 明日) = {:.3}", value)?;
                        }
                        "rv_tobit_20d" => {
                            writeln!(md, "- RV(Tobit涨跌停修正,20d) = {:.2}% (年化)", value)?;
                        }
                        "rv_raw_20d" => {
                            writeln!(md, "- RV(raw横截面,20d) = {:.2}% (年化)", value)?;
                        }
                        "limit_censor_ratio_20d" => {
                            writeln!(md, "- 涨跌停截尾比例(20d) = {:.2}%", value * 100.0)?;
                        }
                        "limit_up_count_20d" => {
                            writeln!(md, "- 涨停截尾样本(20d) = {} 个", *value as i64)?;
                        }
                        "limit_down_count_20d" => {
                            writeln!(md, "- 跌停截尾样本(20d) = {} 个", *value as i64)?;
                        }
                        "vol_regime_duration" => {
                            writeln!(md, "- vol_regime_duration = {} 天", *value as i64)?;
                        }
                        _ => {}
                    }
                }
            }
        }
        Err(_) => {
            writeln!(md, "- 波动率HMM 数据缺失")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_macro_gate(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 宏观网关")?;
    writeln!(md)?;

    let sql = "SELECT metric, value, detail
               FROM analytics
               WHERE ts_code = '_MARKET'
                 AND module = 'macro_gate'
                 AND as_of = (
                     SELECT MAX(as_of) FROM analytics
                     WHERE ts_code = '_MARKET'
                       AND as_of <= CAST(? AS DATE)
                       AND module = 'macro_gate'
                 )
               ORDER BY metric";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, f64, Option<String>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- 宏观网关数据缺失")?;
            } else {
                for (metric, value, detail) in &rows {
                    match metric.as_str() {
                        "gate_multiplier" => {
                            writeln!(md, "- gate_multiplier = {:.2}", value)?;
                            // Parse detail JSON if available
                            if let Some(d) = detail {
                                if let Ok(obj) = serde_json::from_str::<serde_json::Value>(d) {
                                    if let Some(vr) = obj.get("vol_regime").and_then(|v| v.as_str())
                                    {
                                        writeln!(md, "- 波动率状态 = {}", vr)?;
                                    }
                                    if let Some(yc) =
                                        obj.get("yield_curve").and_then(|v| v.as_str())
                                    {
                                        writeln!(md, "- 利差状态 = {}", yc)?;
                                    }
                                    if let Some(va) = obj.get("vol_ann").and_then(|v| v.as_f64()) {
                                        writeln!(md, "- 实现波动率(年化) = {:.2}%", va)?;
                                    }
                                    if let Some(source) =
                                        obj.get("vol_source").and_then(|v| v.as_str())
                                    {
                                        let label = if source == "vol_hmm_tobit" {
                                            "vol_hmm Tobit涨跌停修正"
                                        } else {
                                            source
                                        };
                                        writeln!(md, "- 波动率口径 = {}", label)?;
                                    }
                                    if let Some(sp) = obj.get("spread").and_then(|v| v.as_f64()) {
                                        writeln!(md, "- LPR-Shibor利差 = {:.3}", sp)?;
                                    }
                                }
                            }
                        }
                        "realized_vol_ann" => {
                            // Already printed via gate_multiplier detail; skip duplication
                        }
                        "vol_regime" => {
                            let label = match *value as i32 {
                                0 => "calm",
                                1 => "elevated",
                                2 => "panic",
                                _ => "unknown",
                            };
                            writeln!(md, "- vol_regime_code = {} ({})", *value as i32, label)?;
                        }
                        "yield_curve" => {
                            let label = match *value as i32 {
                                0 => "normal",
                                1 => "flat",
                                2 => "steep",
                                _ => "unknown",
                            };
                            writeln!(md, "- yield_curve_code = {} ({})", *value as i32, label)?;
                        }
                        "market_opt_stress" => {
                            writeln!(md, "- 期权市场压力 z = {:.2}", value)?;
                        }
                        other => {
                            writeln!(md, "- {} = {:.4}", other, value)?;
                        }
                    }
                }
            }
        }
        Err(_) => {
            writeln!(md, "- 宏观网关数据缺失")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_macro_data(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 宏观数据")?;
    writeln!(md)?;
    writeln!(md, "| 指标 | 系列ID | 最新值 | 日期 | 说明 |")?;
    writeln!(md, "|------|--------|--------|------|------|")?;

    // Query the most recent value for each series on or before as_of.
    // Filter out legacy M00xxxxx series (stale data, never fetched by current pipeline).
    let sql = "WITH latest AS (
                   SELECT series_id, series_name, value, date,
                          ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) AS rn
                   FROM macro_cn
                   WHERE date <= CAST(? AS DATE)
                     AND series_id NOT LIKE 'M0%'
               )
               SELECT series_id, series_name, value, CAST(date AS VARCHAR)
               FROM latest
               WHERE rn = 1
               ORDER BY series_id";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, Option<String>, Option<f64>, String)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (无数据) | - | - | - | - |")?;
            } else {
                for (series_id, name, value, date) in &rows {
                    let cadence = macro_cadence(series_id);
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        name.as_deref().unwrap_or(series_id),
                        series_id,
                        fmt_f64(value.as_ref().copied()),
                        date,
                        cadence,
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "macro_cn query failed");
            writeln!(md, "| (查询失败) | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    writeln!(
        md,
        "> 注: 月度指标按 payload 中的 series_id 与日期展示；PMI_MFG 必须标注官方/第三方口径与参考期，若日期明显早于报告月，只能写成滞后/待核验，不能当作当月官方 PMI。"
    )?;
    writeln!(md)?;
    Ok(())
}

fn render_northbound_flow(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 北向资金 (近5日) — 仅供叙事参考，非量化信号")?;
    writeln!(md)?;
    writeln!(md, "| 交易日 | 买入(亿) | 卖出(亿) | 净流入(亿) | 来源 |")?;
    writeln!(md, "|--------|----------|----------|------------|------|")?;

    let sql = "SELECT CAST(trade_date AS VARCHAR), buy_amount, sell_amount, net_amount, source
               FROM northbound_flow
               WHERE trade_date <= CAST(? AS DATE)
               ORDER BY trade_date DESC
               LIMIT 5";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<String>,
            )> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<f64>>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (无数据) | - | - | - | - |")?;
            } else {
                // Check if all 'total' rows have null net_amount
                let totals_all_null = rows
                    .iter()
                    .filter(|(_, _, _, _, src)| src.as_deref() == Some("total"))
                    .all(|(_, _, _, net, _)| net.is_none());
                for (date, buy, sell, net, source) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        date,
                        fmt_f64_yi(*buy),
                        fmt_f64_yi(*sell),
                        fmt_f64_yi(*net),
                        source.as_deref().unwrap_or("-"),
                    )?;
                }
                if totals_all_null {
                    writeln!(md)?;
                    writeln!(
                        md,
                        "> ⚠ 北向资金净流入数据为空 — Tushare moneyflow_hsgt 返回null"
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "northbound_flow query failed");
            writeln!(md, "| (查询失败) | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_sector_fund_flow(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 行业资金流向 (AKShare)")?;
    writeln!(md)?;
    writeln!(
        md,
        "> 口径: AKShare sector_fund_flow / 东方财富行业资金流；main_net_in 按元转亿元展示。该表不等同申万一级、数据宝或其他公开资金口径；若外部口径冲突，正文只能称为“本系统/AKShare口径”。"
    )?;
    writeln!(md)?;
    writeln!(
        md,
        "| 数据日 | 行业 | 涨跌幅% | 主力净流入(亿) | 主力净占比% |"
    )?;
    writeln!(
        md,
        "|--------|------|---------|---------------|------------|"
    )?;

    let sql = "SELECT CAST(trade_date AS VARCHAR), sector_name, pct_chg, main_net_in, main_net_pct
               FROM sector_fund_flow
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM sector_fund_flow
                   WHERE trade_date <= CAST(? AS DATE)
               )
               ORDER BY ABS(main_net_in) DESC
               LIMIT 10";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, Option<f64>, Option<f64>, Option<f64>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (无数据) | - | - | - | - |")?;
            } else {
                for (trade_date, name, pct, net_in, net_pct) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        trade_date,
                        name,
                        fmt_pct(*pct),
                        fmt_f64_yuan_to_yi(*net_in),
                        fmt_pct(*net_pct),
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "sector_fund_flow query failed");
            writeln!(md, "| (查询失败) | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_concept_board(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 概念板块热点 (AKShare)")?;
    writeln!(md)?;
    writeln!(md, "| 概念 | 涨跌幅% | 上涨/下跌 | 领涨股 |")?;
    writeln!(md, "|------|---------|-----------|--------|")?;

    let sql = "SELECT board_name, pct_chg, up_count, down_count, lead_stock
               FROM concept_board
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM concept_board
                   WHERE trade_date <= CAST(? AS DATE)
               )
               ORDER BY pct_chg DESC
               LIMIT 10";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                Option<f64>,
                Option<i32>,
                Option<i32>,
                Option<String>,
            )> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<f64>>(1)?,
                        row.get::<_, Option<i32>>(2)?,
                        row.get::<_, Option<i32>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (无数据) | - | - | - |")?;
            } else {
                for (name, pct, up, down, lead) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {}/{} | {} |",
                        name,
                        fmt_pct(*pct),
                        up.unwrap_or(0),
                        down.unwrap_or(0),
                        lead.as_deref().unwrap_or("-"),
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "concept_board query failed");
            writeln!(md, "| (查询失败) | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_theme_clusters(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 概念主题聚类 (DeepSeek)")?;
    writeln!(md)?;

    let sql = "SELECT theme_name, description, boards, avg_pct_chg
               FROM theme_clusters
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM theme_clusters
                   WHERE trade_date <= CAST(? AS DATE)
               )
               ORDER BY avg_pct_chg DESC";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, Option<String>, Option<String>, Option<f64>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- (未生成主题聚类)")?;
            } else {
                for (name, desc, boards_json, avg_pct) in &rows {
                    writeln!(md, "**{} ({:+.2}%)**", name, avg_pct.unwrap_or(0.0),)?;
                    if let Some(d) = desc {
                        writeln!(md, "  {}", d)?;
                    }
                    if let Some(bj) = boards_json {
                        if let Ok(boards) = serde_json::from_str::<Vec<String>>(bj) {
                            writeln!(md, "  板块: {}", boards.join("、"))?;
                        }
                    }
                    writeln!(md)?;
                }
            }
        }
        Err(_) => {
            writeln!(md, "- (theme_clusters表不可用)")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_etf_options(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### ETF期权活跃度")?;
    writeln!(md)?;

    let sql = "SELECT
                   SUM(COALESCE(vol, 0)) AS total_vol,
                   SUM(COALESCE(oi, 0)) AS total_oi,
                   SUM(COALESCE(amount, 0)) AS total_amount,
                   COUNT(*) AS n_contracts
               FROM opt_daily
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM opt_daily
                   WHERE trade_date <= CAST(? AS DATE)
               )";

    match db.prepare(sql).and_then(|mut stmt| {
        stmt.query_row(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, Option<f64>>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })
    }) {
        Ok((vol, oi, amount, n)) => {
            writeln!(md, "- 合约数: {}", n.unwrap_or(0))?;
            writeln!(md, "- 总成交量: {}", fmt_f64(vol))?;
            writeln!(md, "- 总持仓量: {}", fmt_f64(oi))?;
            writeln!(md, "- 总成交额: {}", fmt_f64(amount))?;
            if let (Some(v), Some(o)) = (vol, oi) {
                if o > 1e-6 {
                    writeln!(md, "- 换手率 (vol/oi): {:.2}", v / o)?;
                }
            }
        }
        Err(_) => {
            writeln!(md, "- ETF期权数据缺失")?;
        }
    }

    let curve_sql = "SELECT ts_code, metric, value, detail
                     FROM analytics
                     WHERE as_of = ? AND module = 'shadow_fast'
                       AND ts_code LIKE '_SHADOW_CURVE_%'
                     ORDER BY ts_code, metric";
    if let Ok(mut stmt) = db.prepare(curve_sql) {
        let rows: Vec<(String, String, f64, Option<String>)> = stmt
            .query_map(duckdb::params![date_str], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();
        if !rows.is_empty() {
            writeln!(md)?;
            writeln!(md, "**影子波动市场曲线 (model-free IV)**")?;
            writeln!(md)?;
            writeln!(md, "| 代理 | 30D | 60D | 90D | 数据日期 |")?;
            writeln!(md, "|------|-----|-----|-----|----------|")?;

            let mut grouped: std::collections::BTreeMap<String, (String, String, f64, f64, f64)> =
                std::collections::BTreeMap::new();
            for (ts_code, metric, value, detail) in rows {
                let entry = grouped
                    .entry(ts_code)
                    .or_insert_with(|| ("-".to_string(), "-".to_string(), 0.0, 0.0, 0.0));
                if let Some(d) = detail.as_deref() {
                    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(d) {
                        entry.0 = obj
                            .get("label")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string();
                        entry.1 = obj
                            .get("source_trade_date")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string();
                    }
                }
                match metric.as_str() {
                    "shadow_iv_30d" => entry.2 = value,
                    "shadow_iv_60d" => entry.3 = value,
                    "shadow_iv_90d" => entry.4 = value,
                    _ => {}
                }
            }

            for (_, (label, source_date, iv30, iv60, iv90)) in grouped {
                writeln!(
                    md,
                    "| {} | {:.1}% | {:.1}% | {:.1}% | {} |",
                    label, iv30, iv60, iv90, source_date
                )?;
            }
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_sector_rotation(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 板块轮动（行业动量）")?;
    writeln!(md)?;

    // Query sector rotation data from analytics
    let sql = "SELECT ts_code, value, detail FROM analytics
               WHERE as_of = ? AND module = 'sector_rotation' AND metric = 'ret_5d'
               ORDER BY value DESC";

    let mut stmt = match db.prepare(sql) {
        Ok(s) => s,
        Err(_) => {
            writeln!(md, "无板块轮动数据")?;
            writeln!(md)?;
            return Ok(());
        }
    };

    let rows: Vec<(String, f64, Option<String>)> =
        match stmt.query_map(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1).unwrap_or(0.0),
                row.get::<_, Option<String>>(2).ok().flatten(),
            ))
        }) {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        };

    if rows.is_empty() {
        writeln!(md, "无板块轮动数据（stock_basic.industry 为空？）")?;
        writeln!(md)?;
        return Ok(());
    }

    // Show top-10 (strongest momentum) and bottom-10 (weakest)
    writeln!(md, "**领涨行业 (Top 10)**")?;
    writeln!(md)?;
    writeln!(
        md,
        "| 行业 | 5D均涨幅 | 20D均涨幅 | 资金流分 | 动量z | 轮动分 | 股票数 |"
    )?;
    writeln!(
        md,
        "|------|---------|---------|---------|------|-------|--------|"
    )?;

    let top10 = rows.iter().take(10);
    for (ts_code, ret_5d, detail) in top10 {
        let industry = ts_code.strip_prefix("_SECTOR_").unwrap_or(ts_code);
        if let Some(d) = detail {
            if let Ok(j) = serde_json::from_str::<serde_json::Value>(d) {
                let ret_20d = j.get("ret_20d").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let flow = j.get("flow_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let mom_z = j.get("momentum_z").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let n = j.get("n_stocks").and_then(|v| v.as_i64()).unwrap_or(0);

                // Query rotation_score
                let rot = db.query_row(
                    "SELECT value FROM analytics WHERE ts_code = ? AND as_of = ? AND module = 'sector_rotation' AND metric = 'rotation_score'",
                    duckdb::params![ts_code, date_str],
                    |r| r.get::<_, f64>(0),
                ).unwrap_or(0.0);

                writeln!(
                    md,
                    "| {} | {:.1}% | {:.1}% | {:.3} | {:.2} | {:.2} | {} |",
                    industry, ret_5d, ret_20d, flow, mom_z, rot, n
                )?;
            }
        }
    }
    writeln!(md)?;

    if rows.len() > 10 {
        writeln!(md, "**领跌行业 (Bottom 5)**")?;
        writeln!(md)?;
        writeln!(
            md,
            "| 行业 | 5D均涨幅 | 20D均涨幅 | 资金流分 | 动量z | 股票数 |"
        )?;
        writeln!(md, "|------|---------|---------|---------|------|--------|")?;

        let bot5 = rows.iter().rev().take(5);
        for (ts_code, ret_5d, detail) in bot5 {
            let industry = ts_code.strip_prefix("_SECTOR_").unwrap_or(ts_code);
            if let Some(d) = detail {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(d) {
                    let ret_20d = j.get("ret_20d").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let flow = j.get("flow_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let mom_z = j.get("momentum_z").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let n = j.get("n_stocks").and_then(|v| v.as_i64()).unwrap_or(0);
                    writeln!(
                        md,
                        "| {} | {:.1}% | {:.1}% | {:.3} | {:.2} | {} |",
                        industry, ret_5d, ret_20d, flow, mom_z, n
                    )?;
                }
            }
        }
        writeln!(md)?;
    }

    Ok(())
}

fn render_cross_market(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 跨市场信号")?;
    writeln!(md)?;

    // ── 黄金 (SGE) ─────────────────────────────────────────────────────────
    // Compute pct_change as current close vs previous session average price.
    // This matches SGE-style daily move more closely than close/close for T+D.
    writeln!(md, "**黄金 (上海金交所)**")?;
    writeln!(md, "> 涨跌幅口径: 当日 close / 前一交易日 price_avg - 1")?;
    let sge_sql = "WITH ranked AS (
                       SELECT ts_code, trade_date, close, vol,
                              LAG(price_avg) OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_price_avg,
                              ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                       FROM sge_daily
                       WHERE trade_date <= CAST(? AS DATE)
                   )
                   SELECT ts_code, CAST(trade_date AS VARCHAR), close,
                          CASE WHEN prev_price_avg > 0
                               THEN (close / prev_price_avg - 1.0) * 100.0
                               ELSE NULL END AS pct_chg,
                          vol
                   FROM ranked
                   WHERE rn = 1
                   ORDER BY vol DESC
                   LIMIT 3";

    match db.prepare(sge_sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, Option<f64>, Option<f64>, Option<f64>)> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- 数据缺失")?;
            } else {
                for (code, trade_date, close, pct, vol) in &rows {
                    writeln!(
                        md,
                        "- {} ({}): close={}, pct_chg={}%, vol={}",
                        code,
                        trade_date,
                        fmt_f64(*close),
                        fmt_f64(*pct),
                        fmt_f64(*vol),
                    )?;
                }
            }
        }
        Err(_) => writeln!(md, "- 黄金数据缺失")?,
    }
    writeln!(md)?;

    // ── 可转债 ────────────────────────────────────────────────────────────
    writeln!(md, "**可转债 (聚合)**")?;
    let cb_sql = "SELECT
                      COUNT(*) AS n,
                      AVG(cb_over_rate) AS avg_premium,
                      SUM(amount) AS total_amount,
                      AVG(CASE WHEN close > 0 AND cb_value > 0 THEN close / cb_value - 1.0 ELSE NULL END) AS avg_cb_premium
                  FROM cb_daily
                  WHERE trade_date = (
                      SELECT MAX(trade_date) FROM cb_daily
                      WHERE trade_date <= CAST(? AS DATE)
                  )";

    match db.prepare(cb_sql).and_then(|mut stmt| {
        stmt.query_row(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<f64>>(3)?,
            ))
        })
    }) {
        Ok((n, avg_prem, total_amt, avg_cb_prem)) => {
            writeln!(md, "- 交易数量: {}", n.unwrap_or(0))?;
            writeln!(md, "- 平均转股溢价率: {}%", fmt_f64(avg_prem))?;
            writeln!(md, "- 总成交额: {}", fmt_f64(total_amt))?;
            writeln!(md, "- 平均价格/转股价值溢价: {}%", fmt_pct(avg_cb_prem))?;
        }
        Err(_) => writeln!(md, "- 可转债数据缺失")?,
    }
    writeln!(md)?;

    // ── 期货 (top movers) ────────────────────────────────────────────────
    // Deduplicate: generic/continuous contracts (LL.DCE, VL.DCE) share
    // identical close/vol/oi with specific dated contracts (L2603.DCE).
    // Group by (close, vol, oi) and prefer the specific contract (has digits).
    // Also filter out illiquid contracts (vol < 100).
    writeln!(md, "**期货 (涨跌幅前5)**")?;
    let fut_sql = "WITH latest AS (
                       SELECT ts_code, close, settle, vol, oi,
                              CASE WHEN settle > 0
                                   THEN (close / settle - 1.0) * 100.0
                                   ELSE 0 END AS pct_chg,
                              regexp_matches(ts_code, '\\d{4}\\.') AS is_specific
                       FROM fut_daily
                       WHERE trade_date = (
                           SELECT MAX(trade_date) FROM fut_daily
                           WHERE trade_date <= CAST(? AS DATE)
                       )
                       AND COALESCE(vol, 0) > 100
                   ),
                   deduped AS (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY close, vol, oi
                                  ORDER BY is_specific DESC, ts_code
                              ) AS rn
                       FROM latest
                   )
                   SELECT ts_code, close, settle, pct_chg, vol, oi
                   FROM deduped
                   WHERE rn = 1
                   ORDER BY ABS(pct_chg) DESC
                   LIMIT 5";

    match db.prepare(fut_sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
            )> = stmt
                .query_map(duckdb::params![date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<f64>>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- 数据缺失")?;
            } else {
                writeln!(md, "| 合约 | 收盘 | 涨跌% | 成交量 | 持仓量 |")?;
                writeln!(md, "|------|------|-------|--------|--------|")?;
                for (code, close, _settle, pct, vol, oi) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        code,
                        fmt_f64(*close),
                        fmt_pct(*pct),
                        fmt_f64(*vol),
                        fmt_f64(*oi),
                    )?;
                }
            }
        }
        Err(_) => writeln!(md, "- 期货数据缺失")?,
    }
    writeln!(md)?;
    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════
// File 2: Structural — 结构化信号
// ═════════════════════════════════════════════════════════════════════════════

fn render_structural(
    db: &Connection,
    date_str: &str,
    generated_at: &str,
    notable: &[NotableItem],
) -> Result<String> {
    let mut md = String::with_capacity(64 * 1024);
    let headline_gate = compute_headline_gate(db, date_str, notable);

    writeln!(md, "# A股量化研究 Payload — 结构化信号")?;
    writeln!(md, "## 生成时间: {}", generated_at)?;
    writeln!(md, "## 交易日: {}", date_str)?;
    writeln!(md)?;
    writeln!(md, "{}", PRECISION_RULES)?;

    render_headline_gate(&mut md, &headline_gate)?;
    render_shadow_calibration(&mut md, db, date_str)?;
    render_alpha_bulletin(&mut md, date_str, "cn")?;
    render_setup_alpha_summary(&mut md, db, date_str, notable)?;

    // ── Report lanes ──────────────────────────────────────────────────────
    writeln!(md, "### 报告分层总览")?;
    writeln!(md)?;
    writeln!(
        md,
        "- 共 {} 个结构信号，按 `CORE BOOK / RANGE CORE / TACTICAL CONTINUATION / THEME ROTATION / RADAR` 五层组织，避免把区间主书、战术续涨、主题轮动与主报告主线混在一起。",
        notable.len(),
    )?;
    writeln!(
        md,
        "- `CORE BOOK` = 主报告正文，优先承载高置信、方向明确、可代表市场主线的信号。"
    )?;
    writeln!(
        md,
        "- `RANGE CORE` = 区间复核层；Headline Gate 只作背景解释，是否可执行仍看主信号、execution gate、RR 和追价约束。"
    )?;
    writeln!(
        md,
        "- `TACTICAL CONTINUATION` = 战术观察层；市场 headline 不清晰时要弱化主线叙事，但不能把 headline 当作单独否决器。"
    )?;
    writeln!(
        md,
        "- `THEME ROTATION` = 主题轮动与资金主线观察，适合写成板块/篮子，不宜直接当作单一主书结论。"
    )?;
    writeln!(
        md,
        "- `RADAR` = 边缘信号与持续跟踪名单，只保留最值得复核的残余观察项。"
    )?;
    if headline_gate.mode != "trend" {
        writeln!(
            md,
            "- 当前 Headline Gate = `{}`，它只约束市场叙事强度，不单独决定个股是否可执行；个股执行仍服从主信号、execution gate、RR 和追价约束。",
            headline_gate.mode.to_uppercase(),
        )?;
    }
    writeln!(md)?;

    for bucket in [
        "CORE BOOK",
        "RANGE CORE",
        "TACTICAL CONTINUATION",
        "THEME ROTATION",
        "RADAR",
    ] {
        let bucket_items: Vec<&NotableItem> = notable
            .iter()
            .filter(|item| item.report_bucket == bucket)
            .collect();
        if bucket_items.is_empty() {
            continue;
        }

        writeln!(md, "### {}", bucket)?;
        writeln!(md)?;
        writeln!(md, "- {}", report_bucket_description(bucket))?;
        writeln!(md, "- 条目数: {}", bucket_items.len())?;
        writeln!(md)?;

        for item in bucket_items {
            render_notable_item(&mut md, db, date_str, item, &headline_gate)?;
        }
    }

    render_report_bucket_distribution(&mut md, notable)?;

    // ── Confidence Distribution ──────────────────────────────────────────
    render_confidence_distribution(&mut md, notable)?;

    // ── Alpha Postmortem ────────────────────────────────────────────────
    render_postmortem_summary(&mut md, db, date_str)?;
    render_algorithm_postmortem_summary(&mut md, db, date_str)?;

    Ok(md)
}

#[derive(Clone, Debug)]
struct SetupAlphaView {
    ts_code: String,
    name: String,
    bucket: &'static str,
    lane: String,
    execution_mode: String,
    ret_1d: f64,
    ret_5d: f64,
    ret_20d: f64,
    trend_prob: f64,
    setup_score: f64,
    event_score: f64,
    priced_in_score: f64,
    pullback_price: Option<f64>,
    reason: &'static str,
}

fn render_setup_alpha_summary(
    md: &mut String,
    db: &Connection,
    date_str: &str,
    notable: &[NotableItem],
) -> Result<()> {
    if notable.is_empty() {
        return Ok(());
    }

    let daily_pct = query_setup_daily_pct_map(db, date_str, notable);
    let views: Vec<SetupAlphaView> = notable
        .iter()
        .map(|item| setup_alpha_view(item, daily_pct.get(&item.ts_code).copied()))
        .collect();
    let early = setup_bucket(&views, "early_accumulation");
    let pullback = setup_bucket(&views, "pullback_reset");
    let breakout = setup_bucket(&views, "breakout_acceptance");
    let post_event = setup_bucket(&views, "post_event_second_day");
    let blocked = setup_bucket(&views, "blocked_chase");

    writeln!(md, "## Setup Alpha / Anti-Chase")?;
    writeln!(md)?;
    writeln!(
        md,
        "该段由系统在叙事前计算，专门区分“还没过热的布局/回踩复核”和“已经追高或定价过满”的候选。它不是买入清单；Execution Alpha 仍必须通过主信号、execution gate、RR 与 A股执行约束。"
    )?;
    writeln!(md)?;
    writeln!(md, "| 分组 | 数量 | 报告用法 |")?;
    writeln!(md, "|------|-----:|----------|")?;
    writeln!(
        md,
        "| Early accumulation | {} | 只写成 setup alpha / 回踩确认，不能写成追涨。 |",
        early.len()
    )?;
    writeln!(
        md,
        "| Pullback / reset | {} | 等参考回踩价或结构重置后再复核。 |",
        pullback.len()
    )?;
    writeln!(
        md,
        "| Breakout acceptance | {} | 已涨但趋势/承接/事件确认仍支持延续；不能机械归为追高。 |",
        breakout.len()
    )?;
    writeln!(
        md,
        "| Post-event second day | {} | 事件已公开，必须看次日承接，不能追首日反应。 |",
        post_event.len()
    )?;
    writeln!(
        md,
        "| Blocked chase / priced-in | {} | 只能放风险回避/观望，不得升级到做多或战术延续。 |",
        blocked.len()
    )?;
    writeln!(md)?;
    writeln!(
        md,
        "**硬约束**: Headline Gate 只是上下文；涨幅不是原罪，只有涨幅缺少 trend_prob/承接/fade risk/execution 确认，或 execution_mode=do_not_chase，才进 Blocked Chase。"
    )?;
    writeln!(md)?;

    render_setup_bucket(
        md,
        "Early Accumulation",
        &early,
        "本期没有干净的早期布局候选。",
        5,
    )?;
    render_setup_bucket(
        md,
        "Pullback / Reset",
        &pullback,
        "本期没有需要等待回踩重置的高支持候选。",
        5,
    )?;
    render_setup_bucket(
        md,
        "Breakout Acceptance",
        &breakout,
        "本期没有已经拉升但仍获得趋势确认的突破候选。",
        5,
    )?;
    render_setup_bucket(
        md,
        "Post-Event Second Day",
        &post_event,
        "本期没有通过反追高过滤的事件次日候选。",
        5,
    )?;
    render_setup_bucket(
        md,
        "Blocked Chase / Priced-In",
        &blocked,
        "本期没有触发 stale-chase 过滤的候选。",
        8,
    )?;

    Ok(())
}

fn setup_bucket<'a>(views: &'a [SetupAlphaView], bucket: &str) -> Vec<&'a SetupAlphaView> {
    let mut rows: Vec<&SetupAlphaView> =
        views.iter().filter(|view| view.bucket == bucket).collect();
    rows.sort_by(|a, b| {
        b.setup_score
            .partial_cmp(&a.setup_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                a.priced_in_score
                    .partial_cmp(&b.priced_in_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                b.event_score
                    .partial_cmp(&a.event_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    rows
}

fn render_setup_bucket(
    md: &mut String,
    title: &str,
    rows: &[&SetupAlphaView],
    empty: &str,
    limit: usize,
) -> Result<()> {
    writeln!(md, "### {}", title)?;
    writeln!(md)?;
    if rows.is_empty() {
        writeln!(md, "- {}", empty)?;
        writeln!(md)?;
        return Ok(());
    }

    writeln!(
        md,
        "| 代码 | 名称 | lane | execution | 1D% | 5D% | 20D% | trend_prob | setup | priced-in | 回踩价 | 原因 |"
    )?;
    writeln!(
        md,
        "|------|------|------|-----------|-----|-----|------|------------|-------|-----------|--------|------|"
    )?;
    for row in rows.iter().take(limit) {
        writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.ts_code,
            row.name,
            row.lane,
            row.execution_mode,
            fmt_pct(Some(row.ret_1d)),
            fmt_pct(Some(row.ret_5d)),
            fmt_pct(Some(row.ret_20d)),
            fmt_opt_f64(Some(row.trend_prob), 3),
            fmt_opt_f64(Some(row.setup_score), 3),
            fmt_opt_f64(Some(row.priced_in_score), 2),
            fmt_f64(row.pullback_price),
            row.reason
        )?;
    }
    writeln!(md)?;
    Ok(())
}

fn query_setup_daily_pct_map(
    db: &Connection,
    date_str: &str,
    notable: &[NotableItem],
) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    let sql = "SELECT pct_chg
               FROM prices
               WHERE ts_code = ?
                 AND trade_date = (
                     SELECT MAX(trade_date) FROM prices
                     WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
                 )";

    let Ok(mut stmt) = db.prepare(sql) else {
        return map;
    };
    for item in notable {
        if let Ok(Some(pct_chg)) = stmt.query_row(
            duckdb::params![&item.ts_code, &item.ts_code, date_str],
            |row| row.get::<_, Option<f64>>(0),
        ) {
            map.insert(item.ts_code.clone(), pct_chg);
        }
    }
    map
}

fn setup_alpha_view(item: &NotableItem, daily_pct_chg: Option<f64>) -> SetupAlphaView {
    let ret_1d = daily_pct_chg
        .or_else(|| detail_f64_opt(&item.detail, "ret_1d"))
        .unwrap_or(0.0);
    let ret_5d = detail_f64(&item.detail, "ret_5d", 0.0);
    let ret_20d = detail_f64(&item.detail, "ret_20d", 0.0);
    let trend_prob = detail_f64(&item.detail, "trend_prob", 0.5);
    let setup_score = detail_f64(&item.detail, "setup_score", 0.0);
    let setup_direction = detail_str(&item.detail, "setup_direction");
    let execution_mode = detail_str(&item.detail, "execution_mode")
        .unwrap_or("executable")
        .to_string();
    let continuation_score = detail_f64(&item.detail, "continuation_score", 0.0);
    let pullback_price = detail_f64_opt(&item.detail, "pullback_price");
    let stale_chase_risk = detail_f64(&item.detail, "stale_chase_risk", 0.0);
    let fade_risk = detail_f64(&item.detail, "fade_risk", 0.0);
    let event_score = item.event_score;
    let limit_up_like = if item.name.contains("ST") {
        ret_1d >= 4.8
    } else {
        ret_1d >= 9.5
    };
    let mut priced_in_score = cn_priced_in_score(
        &execution_mode,
        ret_5d,
        ret_20d,
        trend_prob,
        stale_chase_risk,
        fade_risk,
    );
    if limit_up_like {
        priced_in_score = priced_in_score.max(0.82);
    }
    let bullish_setup =
        setup_direction == Some("bullish") || item.signal.direction.eq_ignore_ascii_case("bullish");
    let not_hot = priced_in_score < 0.62;
    let breakout_supported = bullish_setup
        && execution_mode != "do_not_chase"
        && ret_5d >= 6.0
        && ret_20d >= 10.0
        && ret_5d <= 18.0
        && ret_20d <= 38.0
        && trend_prob >= 0.56
        && setup_score >= 0.55
        && continuation_score >= 0.48
        && fade_risk <= 0.38
        && stale_chase_risk <= 0.55
        && priced_in_score < 0.82
        && !limit_up_like;

    let (bucket, reason) = if limit_up_like {
        ("blocked_chase", "涨停次日盘口风险，缺少封单/换手确认")
    } else if execution_mode == "do_not_chase" || priced_in_score >= 0.82 {
        ("blocked_chase", "涨幅/追价风险已兑现")
    } else if breakout_supported {
        ("breakout_acceptance", "已拉升但趋势承接仍确认")
    } else if execution_mode == "wait_pullback"
        || (pullback_price.is_some()
            && setup_score >= 0.50
            && (0.50..0.72).contains(&priced_in_score))
    {
        ("pullback_reset", "需要回踩重置后复核")
    } else if priced_in_score >= 0.72 {
        ("blocked_chase", "拉升缺少足够趋势确认")
    } else if bullish_setup
        && not_hot
        && setup_score >= 0.55
        && (-2.0..=6.0).contains(&ret_5d)
        && ret_20d <= 12.0
        && fade_risk <= 0.45
    {
        ("early_accumulation", "结构蓄势但尚未过热")
    } else if event_score >= 0.50 && not_hot && ret_5d <= 8.0 && ret_20d <= 20.0 {
        ("post_event_second_day", "事件已公开，等次日承接")
    } else {
        ("other", "非 setup-alpha 候选")
    };

    SetupAlphaView {
        ts_code: item.ts_code.clone(),
        name: item.name.clone(),
        bucket,
        lane: item.report_bucket.clone(),
        execution_mode,
        ret_1d,
        ret_5d,
        ret_20d,
        trend_prob,
        setup_score,
        event_score,
        priced_in_score,
        pullback_price,
        reason,
    }
}

fn cn_priced_in_score(
    execution_mode: &str,
    ret_5d: f64,
    ret_20d: f64,
    trend_prob: f64,
    stale_chase_risk: f64,
    fade_risk: f64,
) -> f64 {
    let mut score = stale_chase_risk
        .clamp(0.0, 1.0)
        .max(fade_risk.clamp(0.0, 1.0) * 0.85);
    if execution_mode == "do_not_chase" {
        score = score.max(0.90);
    } else if execution_mode == "wait_pullback" {
        score = score.max(0.50);
    }
    if ret_20d >= 20.0 {
        score = score.max(((ret_20d - 20.0) / 35.0).clamp(0.0, 1.0));
    }
    if ret_5d >= 9.0 {
        score = score.max(((ret_5d - 9.0) / 18.0).clamp(0.0, 1.0));
    }
    if ret_5d >= 7.0 && trend_prob <= 0.50 {
        score = score.max(0.74);
    }
    if ret_20d >= 15.0 && trend_prob <= 0.52 {
        score = score.max(0.72);
    }
    (score * 1000.0).round() / 1000.0
}

fn detail_f64(detail: &serde_json::Value, key: &str, default: f64) -> f64 {
    detail
        .get(key)
        .and_then(|value| value.as_f64())
        .unwrap_or(default)
}

fn detail_f64_opt(detail: &serde_json::Value, key: &str) -> Option<f64> {
    detail.get(key).and_then(|value| value.as_f64())
}

fn detail_str<'a>(detail: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    detail.get(key).and_then(|value| value.as_str())
}

fn render_notable_item(
    md: &mut String,
    db: &Connection,
    date_str: &str,
    item: &NotableItem,
    headline_gate: &HeadlineGateSummary,
) -> Result<()> {
    writeln!(
        md,
        "#### {} {} — {} [{}] | {}",
        item.ts_code,
        if item.name.is_empty() { "" } else { &item.name },
        item.signal.confidence,
        item.signal.direction,
        item.report_bucket,
    )?;
    writeln!(md)?;
    writeln!(md, "- **报告层级**: {}", item.report_bucket,)?;
    writeln!(md, "- **层级定位**: {}", item.report_reason,)?;

    let detail = &item.detail;
    let main_gate = detail.get("main_signal_gate").and_then(|v| v.as_object());
    let mut main_gate_pass = false;
    let mut main_gate_blockers = "none".to_string();
    if let Some(gate) = main_gate {
        let status = gate.get("status").and_then(|v| v.as_str()).unwrap_or("-");
        let role = gate.get("role").and_then(|v| v.as_str()).unwrap_or("-");
        let intent = gate
            .get("action_intent")
            .and_then(|v| v.as_str())
            .unwrap_or("OBSERVE");
        main_gate_pass = status == "pass";
        main_gate_blockers = gate
            .get("blockers")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str())
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "none".to_string());
        writeln!(
            md,
            "- **主信号门槛**: {} | role={} | intent={} | blockers={}",
            status.to_uppercase(),
            role,
            intent,
            main_gate_blockers,
        )?;
    }

    // ── 价格 ──────────────────────────────────────────────────────────────
    let ret_5d = detail.get("ret_5d").and_then(|v| v.as_f64());
    let ret_20d = detail.get("ret_20d").and_then(|v| v.as_f64());

    // Fetch latest close + valuation from prices + daily_basic
    let (close, pe, pb, total_mv) = query_stock_valuation(db, &item.ts_code, date_str);

    writeln!(
        md,
        "- **价格**: 5D ret={}%, 20D ret={}%, 最新价={}",
        fmt_opt_f64(ret_5d, 2),
        fmt_opt_f64(ret_20d, 2),
        fmt_f64(close),
    )?;
    writeln!(
        md,
        "- **估值**: PE={}, PB={}, 总市值={}亿",
        fmt_f64(pe),
        fmt_f64(pb),
        fmt_f64_yi(total_mv),
    )?;

    // ── 动量 ──────────────────────────────────────────────────────────────
    let trend_prob = detail.get("trend_prob").and_then(|v| v.as_f64());
    let trend_prob_n = detail.get("trend_prob_n").and_then(|v| v.as_f64());

    // Query regime and vol_bucket from analytics
    let (regime, vol_bucket, ci_low, ci_high) = query_momentum_detail(db, &item.ts_code, date_str);

    writeln!(
        md,
        "- **动量**: trend_prob={} [{}, {}] (n={}, regime={}, vol_bucket={})",
        fmt_opt_f64(trend_prob, 3),
        fmt_opt_f64(ci_low, 3),
        fmt_opt_f64(ci_high, 3),
        fmt_opt_f64(trend_prob_n, 0),
        regime.as_deref().unwrap_or("-"),
        vol_bucket.as_deref().unwrap_or("-"),
    )?;

    // ── 信息分 (flow components) ──────────────────────────────────────────
    let flow_components = query_flow_components(db, &item.ts_code, date_str);

    writeln!(md, "- **信息分**: information_score={:.3}", item.flow_score)?;
    for (metric, value) in &flow_components {
        match metric.as_str() {
            "large_flow_z" => writeln!(md, "  - 大单流向 z={:.2}", value)?,
            "tape_z" => writeln!(md, "  - 异动信号 z={:.2}", value)?,
            "margin_z" => writeln!(md, "  - 融资 z={:.2}", value)?,
            "block_z" => writeln!(md, "  - 大宗 z={:.2}", value)?,
            "insider_z" => writeln!(md, "  - 内部人 z={:.2}", value)?,
            "event_clock" => writeln!(md, "  - 事件时钟={:.2}", value)?,
            "market_vol_z" => writeln!(md, "  - 市场波动 z={:.2}", value)?,
            _ => {}
        }
    }

    // ── 业绩预告 ──────────────────────────────────────────────────────────
    let p_upside = detail.get("p_upside").and_then(|v| v.as_f64());
    if p_upside.is_some() {
        let forecast = query_latest_forecast(db, &item.ts_code, date_str);
        if let Some((ftype, p_min, p_max)) = forecast {
            writeln!(
                md,
                "- **业绩预告**: type={}, p_upside={}, p_change=[{}%, {}%]",
                ftype,
                fmt_opt_f64(p_upside, 3),
                fmt_opt_f64(p_min, 1),
                fmt_opt_f64(p_max, 1),
            )?;
        }
    }

    // ── 解禁 ──────────────────────────────────────────────────────────────
    let p_drop = detail.get("p_drop").and_then(|v| v.as_f64());
    let unlock_days = detail.get("unlock_days").and_then(|v| v.as_f64());
    let float_ratio = detail.get("float_ratio").and_then(|v| v.as_f64());
    if let (Some(days), Some(ratio), Some(pd)) = (unlock_days, float_ratio, p_drop) {
        writeln!(
            md,
            "- **解禁**: days={:.0}, ratio={:.2}%, p_drop={:.3}",
            days, ratio, pd,
        )?;
    }

    // ── 综合 ──────────────────────────────────────────────────────────────
    writeln!(
        md,
        "- **综合**: composite={:.3}, magnitude={:.3}, momentum={:.3}, event={:.3}, flow={:.3}, cross_asset={:.3}",
        item.composite_score,
        item.magnitude_score,
        item.momentum_score,
        item.event_score,
        item.flow_score,
        item.cross_asset_score,
    )?;

    // ── 资金流 (moneyflow) ───────────────────────────────────────────────
    let mf = query_moneyflow(db, &item.ts_code, date_str);
    if let Some((net_mf, elg_net)) = mf {
        writeln!(
            md,
            "- **资金流**: net_mf_amount={:.2}万, 超大单净流入={:.2}万",
            net_mf, elg_net,
        )?;
    }

    // ── 融资 (margin_detail) ────────────────────────────────────────────
    let margin = query_margin_detail(db, &item.ts_code, date_str);
    if let Some((rzye, delta_rzye)) = margin {
        writeln!(
            md,
            "- **融资**: rzye={}亿, 5D变化={}亿",
            fmt_f64_yuan_to_yi(Some(rzye)),
            fmt_f64_yuan_to_yi(Some(delta_rzye)),
        )?;
    }

    // ── 影子期权 ──────────────────────────────────────────────────────────
    let shadow_iv_30d = detail.get("shadow_iv_30d").and_then(|v| v.as_f64());
    let shadow_iv_60d = detail.get("shadow_iv_60d").and_then(|v| v.as_f64());
    let shadow_iv_90d = detail.get("shadow_iv_90d").and_then(|v| v.as_f64());
    let downside_stress = detail.get("downside_stress").and_then(|v| v.as_f64());
    let shadow_proxy = detail.get("shadow_proxy").and_then(|v| v.as_str());
    if shadow_iv_30d.is_some() || shadow_iv_60d.is_some() || shadow_iv_90d.is_some() {
        writeln!(
            md,
            "- **影子波动**: 30D={}%, 60D={}%, 90D={}%, downside_stress={}, proxy={}",
            fmt_opt_f64(shadow_iv_30d, 1),
            fmt_opt_f64(shadow_iv_60d, 1),
            fmt_opt_f64(shadow_iv_90d, 1),
            fmt_opt_f64(downside_stress, 3),
            shadow_proxy.unwrap_or("-"),
        )?;
    }

    let shadow_put_90 = detail.get("shadow_put_90_3m").and_then(|v| v.as_f64());
    let shadow_touch_90 = detail.get("shadow_touch_90_3m").and_then(|v| v.as_f64());
    let shadow_floor_1 = detail
        .get("shadow_floor_1sigma_3m")
        .and_then(|v| v.as_f64());
    let shadow_floor_2 = detail
        .get("shadow_floor_2sigma_3m")
        .and_then(|v| v.as_f64());
    let shadow_skew = detail.get("shadow_skew_90_3m").and_then(|v| v.as_f64());
    if shadow_put_90.is_some() || shadow_touch_90.is_some() {
        writeln!(
            md,
            "- **影子期权**: 3M 90%Put={}, 触及90%概率={}%, 1σ底={}, 2σ底={}, downside_skew={} vol pts",
            fmt_opt_f64(shadow_put_90, 3),
            shadow_touch_90.map(|v| v * 100.0).map(|v| format!("{:.1}", v)).unwrap_or_else(|| "-".to_string()),
            fmt_f64(shadow_floor_1),
            fmt_f64(shadow_floor_2),
            fmt_opt_f64(shadow_skew, 2),
        )?;
    }

    let shadow_option_alpha = detail.get("shadow_option_alpha");
    if let Some(alpha) = shadow_option_alpha {
        let diagnostic_zh = alpha
            .get("diagnostic_zh")
            .and_then(|v| v.as_str())
            .unwrap_or("只等回踩");
        let sample_count = alpha
            .get("sample_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let latest_sample = alpha
            .get("latest_sample_date")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let capture_ci = alpha
            .get("capture_rate_interval")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                let lo = arr.first().and_then(|v| v.as_f64())?;
                let hi = arr.get(1).and_then(|v| v.as_f64())?;
                Some(format!("{:.1}-{:.1}%", lo * 100.0, hi * 100.0))
            })
            .unwrap_or_else(|| "-".to_string());
        writeln!(
            md,
            "- **影子期权校准**: {}；alpha={}, 追价失效风险={}，样本n={}，命中率区间={}，最近样本={}",
            diagnostic_zh,
            fmt_opt_f64(alpha.get("shadow_alpha_prob").and_then(|v| v.as_f64()), 3),
            fmt_opt_f64(alpha.get("stale_chase_risk").and_then(|v| v.as_f64()), 3),
            sample_count,
            capture_ci,
            latest_sample,
        )?;
    }

    // ── 布局结构 / 执行层 ────────────────────────────────────────────────
    let setup_score = detail.get("setup_score").and_then(|v| v.as_f64());
    let setup_direction = detail.get("setup_direction").and_then(|v| v.as_str());
    let continuation_score = detail.get("continuation_score").and_then(|v| v.as_f64());
    let continuation_direction = detail
        .get("continuation_direction")
        .and_then(|v| v.as_str());
    let fade_risk = detail.get("fade_risk").and_then(|v| v.as_f64());
    let execution_score = detail.get("execution_score").and_then(|v| v.as_f64());
    let execution_mode = detail.get("execution_mode").and_then(|v| v.as_str());
    let max_chase_gap_pct = detail.get("max_chase_gap_pct").and_then(|v| v.as_f64());
    let pullback_trigger_pct = detail.get("pullback_trigger_pct").and_then(|v| v.as_f64());
    let pullback_price = detail.get("pullback_price").and_then(|v| v.as_f64());

    if setup_score.is_some() || continuation_score.is_some() || execution_score.is_some() {
        writeln!(
            md,
            "- **布局结构**: 蓄势得分={}（{}），延续倾向={}（{}），回吐风险={}",
            fmt_opt_f64(setup_score, 3),
            direction_zh(setup_direction),
            fmt_opt_f64(continuation_score, 3),
            direction_zh(continuation_direction),
            fmt_opt_f64(fade_risk, 3)
        )?;
        writeln!(
            md,
            "- **次日执行**: {}",
            execution_summary_sentence(
                &item.report_bucket,
                &headline_gate.mode,
                execution_mode,
                execution_score,
                max_chase_gap_pct,
                pullback_trigger_pct,
                pullback_price,
                main_gate_pass,
                &main_gate_blockers,
            )
        )?;
    }

    writeln!(md)?;
    Ok(())
}

fn render_report_bucket_distribution(md: &mut String, notable: &[NotableItem]) -> Result<()> {
    writeln!(md, "### 报告层级分布")?;
    writeln!(md)?;
    writeln!(md, "| 层级 | 数量 | 占比 |")?;
    writeln!(md, "|------|------|------|")?;

    let total = notable.len() as f64;
    let mut counts = [0usize; 5]; // CORE BOOK, RANGE CORE, TACTICAL CONTINUATION, THEME ROTATION, RADAR
    for item in notable {
        match item.report_bucket.as_str() {
            "CORE BOOK" => counts[0] += 1,
            "RANGE CORE" => counts[1] += 1,
            "TACTICAL CONTINUATION" => counts[2] += 1,
            "THEME ROTATION" => counts[3] += 1,
            _ => counts[4] += 1,
        }
    }

    let labels = [
        "CORE BOOK",
        "RANGE CORE",
        "TACTICAL CONTINUATION",
        "THEME ROTATION",
        "RADAR",
    ];
    for (i, label) in labels.iter().enumerate() {
        let pct = if total > 0.0 {
            counts[i] as f64 / total * 100.0
        } else {
            0.0
        };
        writeln!(md, "| {} | {} | {:.1}% |", label, counts[i], pct)?;
    }
    writeln!(md)?;
    Ok(())
}

fn direction_zh(direction: Option<&str>) -> &'static str {
    match direction.unwrap_or("neutral") {
        "bullish" => "偏多",
        "bearish" => "偏空",
        _ => "中性",
    }
}

fn execution_mode_sentence(mode: Option<&str>) -> &'static str {
    match mode.unwrap_or("executable") {
        "do_not_chase" => "当前更像情绪拉伸后的高波区，不适合机械追价",
        "wait_pullback" => "方向和结构还在，但更适合等回踩后再评估",
        _ => "当前价位仍有一定可执行性，可结合主线强弱择机跟进",
    }
}

fn execution_summary_sentence(
    report_bucket: &str,
    headline_mode: &str,
    mode: Option<&str>,
    execution_score: Option<f64>,
    max_chase_gap_pct: Option<f64>,
    pullback_trigger_pct: Option<f64>,
    pullback_price: Option<f64>,
    main_gate_pass: bool,
    main_gate_blockers: &str,
) -> String {
    if !main_gate_pass {
        let lane = match report_bucket {
            "CORE BOOK" => "主书候选",
            "RANGE CORE" => "区间复核",
            "TACTICAL CONTINUATION" => "战术观察",
            "THEME ROTATION" => "主题轮动观察",
            "RADAR" => "雷达观察",
            _ => "复核",
        };
        return format!(
            "主信号门槛未通过（blockers={}），{}层不输出买入/追价指令；执行得分={}，仅记录回踩复核约 {}%，参考复核价={}。A股 T+1 下止损不是硬止损，涨跌停可能导致不可成交；最终研报只写观察与失效条件，不写入场、止盈或T1/T2。",
            main_gate_blockers,
            lane,
            fmt_opt_f64(execution_score, 3),
            fmt_opt_f64(pullback_trigger_pct, 2),
            fmt_opt_f64(pullback_price, 2)
        );
    }

    let headline_note = if headline_mode != "trend" {
        format!(
            "Headline Gate={} 仅作辅助上下文，不单独否决；",
            headline_mode.to_uppercase()
        )
    } else {
        String::new()
    };

    match mode.unwrap_or("executable") {
        "do_not_chase" => format!(
            "{}{}；执行得分={}，当前不建议新开仓，至少等待约 {}% 的回踩后再评估，参考回踩价={}；A股 T+1 与涨跌停约束下不得把静态止损写成硬止损",
            headline_note,
            execution_mode_sentence(mode),
            fmt_opt_f64(execution_score, 3),
            fmt_opt_f64(pullback_trigger_pct, 2),
            fmt_opt_f64(pullback_price, 2)
        ),
        "wait_pullback" => format!(
            "{}{}；执行得分={}，当前不宜直接追价，优先观察约 {}% 的回踩，参考回踩价={}；A股 T+1 与涨跌停约束下不得把静态止损写成硬止损",
            headline_note,
            execution_mode_sentence(mode),
            fmt_opt_f64(execution_score, 3),
            fmt_opt_f64(pullback_trigger_pct, 2),
            fmt_opt_f64(pullback_price, 2)
        ),
        _ => format!(
            "{}{}；执行得分={}，可接受追价上限约 {}%，更理想的回踩触发约 {}%，参考回踩价={}；A股 T+1 与涨跌停约束下需写清次日处理线而非硬止损",
            headline_note,
            execution_mode_sentence(mode),
            fmt_opt_f64(execution_score, 3),
            fmt_opt_f64(max_chase_gap_pct, 2),
            fmt_opt_f64(pullback_trigger_pct, 2),
            fmt_opt_f64(pullback_price, 2)
        ),
    }
}

fn render_confidence_distribution(md: &mut String, notable: &[NotableItem]) -> Result<()> {
    writeln!(md, "### 综合评分分布")?;
    writeln!(md)?;
    writeln!(md, "| 置信度 | 数量 | 占比 |")?;
    writeln!(md, "|--------|------|------|")?;

    let total = notable.len() as f64;
    let mut counts = [0usize; 4]; // HIGH, MODERATE, WATCH, LOW
    for item in notable {
        match item.signal.confidence.as_str() {
            "HIGH" => counts[0] += 1,
            "MODERATE" => counts[1] += 1,
            "WATCH" => counts[2] += 1,
            _ => counts[3] += 1,
        }
    }

    let labels = ["HIGH", "MODERATE", "WATCH", "LOW"];
    for (i, label) in labels.iter().enumerate() {
        let pct = if total > 0.0 {
            counts[i] as f64 / total * 100.0
        } else {
            0.0
        };
        writeln!(md, "| {} | {} | {:.1}% |", label, counts[i], pct)?;
    }
    writeln!(md)?;
    Ok(())
}

fn render_postmortem_summary(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    let as_of = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let cutoff = (as_of - chrono::Duration::days(20)).to_string();

    let mut count_stmt = db.prepare(
        "SELECT selection_status, label, COUNT(*)
         FROM alpha_postmortem
         WHERE session = 'daily'
           AND evaluation_date >= CAST(? AS DATE)
           AND evaluation_date < CAST(? AS DATE)
           AND symbol NOT LIKE '300%'
           AND symbol NOT LIKE '301%'
         GROUP BY selection_status, label",
    )?;

    let rows = count_stmt.query_map(duckdb::params![cutoff, date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    let mut selected_total = 0i64;
    let mut ignored_total = 0i64;
    let mut captured = 0i64;
    let mut stale = 0i64;
    let mut false_positive = 0i64;
    let mut missed = 0i64;

    for row in rows {
        let (selection_status, label, count) = row?;
        match selection_status.as_str() {
            "selected" => selected_total += count,
            "ignored" => ignored_total += count,
            _ => {}
        }
        match label.as_str() {
            "captured" => captured += count,
            "alpha_already_paid" | "good_signal_bad_timing" => stale += count,
            "false_positive" => false_positive += count,
            "missed_alpha" => missed += count,
            _ => {}
        }
    }

    if selected_total + ignored_total == 0 {
        return Ok(());
    }

    let recent_hits = load_postmortem_examples(
        db,
        "label = 'captured' AND symbol NOT LIKE '300%' AND symbol NOT LIKE '301%'",
        duckdb::params![cutoff, date_str],
    )?;
    let recent_missed = load_postmortem_examples(
        db,
        "label = 'missed_alpha' AND symbol NOT LIKE '300%' AND symbol NOT LIKE '301%'",
        duckdb::params![cutoff, date_str],
    )?;
    let recent_stale = load_postmortem_examples(
        db,
        "label IN ('alpha_already_paid', 'good_signal_bad_timing') AND symbol NOT LIKE '300%' AND symbol NOT LIKE '301%'",
        duckdb::params![cutoff, date_str],
    )?;
    let (actionable_missed_count, actionable_missed_examples) =
        load_actionable_missed_examples(db, &cutoff, date_str)?;

    writeln!(md, "### Alpha Postmortem")?;
    writeln!(md)?;
    writeln!(
        md,
        "- 近20天已复盘信号共 {} 条，其中入选 {} 条、忽略 {} 条。",
        selected_total + ignored_total,
        selected_total,
        ignored_total
    )?;
    writeln!(
        md,
        "- 当前复盘口径**暂不纳入创业板/301 系列**，避免把策略明确不做的样本混进主策略召回率。"
    )?;
    writeln!(
        md,
        "- 结果分布：抓到 alpha {} 条，时点偏晚/alpha 已兑现 {} 条，误报 {} 条，忽略后继续走强 {} 条。",
        captured, stale, false_positive, missed
    )?;
    if actionable_missed_count > 0 {
        writeln!(
            md,
            "- 其中 **{} 条** 属于“可执行但被压在观察层”的漏选：非创业板、方向仍为多头、执行模式仍在 `executable/wait_pullback`，但最终留在 `THEME ROTATION`。",
            actionable_missed_count
        )?;
        if !actionable_missed_examples.is_empty() {
            writeln!(
                md,
                "- 这类可执行漏选的代表：{}",
                actionable_missed_examples.join("；")
            )?;
        }
    }
    if missed > captured {
        writeln!(
            md,
            "- 当前主要短板是 **漏掉 alpha 多于抓到 alpha**；这一批 `missed_alpha` 已回流到 Factor Lab 反馈层，用来抬高召回和降低错杀。"
        )?;
    } else if stale > 0 {
        writeln!(
            md,
            "- 当前更像 **时点偏晚**：方向不一定错，但不少信号在入书时已经接近被市场兑现，次日更应强调不追价和等回踩。"
        )?;
    } else {
        writeln!(
            md,
            "- 最近复盘没有显示明显的系统性时滞，但仍要优先看执行门槛，避免把结构性机会写成机械追价。"
        )?;
    }
    if !recent_hits.is_empty() {
        writeln!(md, "- 最近抓到的例子：{}", recent_hits.join("；"))?;
    }
    if !recent_missed.is_empty() {
        writeln!(md, "- 最近漏掉的例子：{}", recent_missed.join("；"))?;
    }
    if !recent_stale.is_empty() {
        writeln!(md, "- 最近时点偏晚的例子：{}", recent_stale.join("；"))?;
    }
    writeln!(md)?;
    Ok(())
}

fn render_algorithm_postmortem_summary(
    md: &mut String,
    db: &Connection,
    date_str: &str,
) -> Result<()> {
    let as_of = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let cutoff = (as_of - chrono::Duration::days(20)).to_string();

    let mut stmt = db.prepare(
        "SELECT selection_status, label, executable, realized_pnl_pct, best_possible_ret_pct
         FROM algorithm_postmortem
         WHERE session = 'daily'
           AND evaluation_date >= CAST(? AS DATE)
           AND evaluation_date < CAST(? AS DATE)
           AND symbol NOT LIKE '300%'
           AND symbol NOT LIKE '301%'",
    )?;
    let rows = stmt.query_map(duckdb::params![cutoff, date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, bool>(2).unwrap_or(false),
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;

    let mut reviewed = 0usize;
    let mut selected = 0usize;
    let mut executable = 0usize;
    let mut executable_wins = 0usize;
    let mut stale = 0usize;
    let mut right_no_fill = 0usize;
    let mut missed = 0usize;
    let mut false_executable = 0usize;
    let mut realized_sum = 0.0;
    let mut realized_n = 0usize;

    for row in rows {
        let (selection_status, label, is_executable, realized, _best) = row?;
        reviewed += 1;
        if selection_status == "selected" {
            selected += 1;
        }
        if is_executable {
            executable += 1;
            if realized.unwrap_or(0.0) > 0.0 {
                executable_wins += 1;
            }
        }
        if let Some(v) = realized {
            realized_sum += v;
            realized_n += 1;
        }
        match label.as_str() {
            "stale_chase" => stale += 1,
            "right_but_no_fill" => right_no_fill += 1,
            "missed_alpha" => missed += 1,
            "false_positive_executable" => false_executable += 1,
            _ => {}
        }
    }

    if reviewed == 0 {
        return Ok(());
    }

    let win_rate = if executable > 0 {
        executable_wins as f64 / executable as f64 * 100.0
    } else {
        0.0
    };
    let avg_realized = if realized_n > 0 {
        Some(realized_sum / realized_n as f64)
    } else {
        None
    };

    writeln!(md, "### Algorithm Postmortem")?;
    writeln!(md)?;
    writeln!(
        md,
        "- 近20天算法动作复盘 {} 条：selected {} 条，其中实际按次日开盘可执行 {} 条。",
        reviewed, selected, executable
    )?;
    writeln!(
        md,
        "- 可执行动作胜率 {:.1}%，平均次日收盘近似收益 {}%；误报可执行 {} 条。",
        win_rate,
        fmt_opt_f64(avg_realized, 2),
        false_executable
    )?;
    writeln!(
        md,
        "- 执行层问题：追价失效 {} 条，方向对但无可观测回踩成交 {} 条；未入选后继续走强 {} 条。",
        stale, right_no_fill, missed
    )?;
    writeln!(
        md,
        "- 这层复盘按“报告动作是否可成交”计分，避免把 `do_not_chase/wait_pullback` 的方向正确误记成已捕获交易收益。"
    )?;
    writeln!(md)?;
    Ok(())
}

fn render_shadow_calibration(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    let as_of = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let summary = summarize_shadow_calibration(db, as_of);
    if summary.total_reviewed == 0 {
        return Ok(());
    }

    writeln!(md, "### Shadow Calibration")?;
    writeln!(md)?;
    writeln!(
        md,
        "- 回看近{}天已复盘信号 {} 条：selected {} 条，ignored {} 条。",
        summary.lookback_days,
        summary.total_reviewed,
        summary.selected_reviewed,
        summary.ignored_reviewed,
    )?;
    writeln!(
        md,
        "- `selected` 的平均 shadow_rank={:.3}，`ignored` 的平均 shadow_rank={:.3}；只看正向 shadow 名字时，selected={:.3}，ignored={:.3}。",
        summary.selected_avg_shadow_rank,
        summary.ignored_avg_shadow_rank,
        summary.selected_positive_avg_shadow_rank,
        summary.ignored_positive_avg_shadow_rank,
    )?;
    writeln!(
        md,
        "- 正向 shadow（threshold={:.2}）里：selected 的 capture_rate={:.1}%，false_positive_rate={:.1}%；ignored 的 missed_alpha_rate={:.1}%。",
        summary.positive_threshold,
        summary.selected_positive_capture_rate * 100.0,
        summary.selected_positive_false_positive_rate * 100.0,
        summary.ignored_positive_missed_rate * 100.0,
    )?;
    writeln!(
        md,
        "- 标签均值对比：captured={:.3}，missed_alpha={:.3}，false_positive={:.3}，stale={:.3}。",
        summary.captured_avg_shadow_rank,
        summary.missed_avg_shadow_rank,
        summary.false_positive_avg_shadow_rank,
        summary.stale_avg_shadow_rank,
    )?;
    writeln!(
        md,
        "- 当前 live 校准：shadow_weight={:.3}（base=0.100），shadow_pass1_reserve={}（base=18），recall_gap={:.3}，quality_gap={:.3}。",
        summary.recommended_weight,
        summary.recommended_reserve,
        summary.recall_gap,
        summary.quality_gap,
    )?;
    if summary.ignored_positive_avg_shadow_rank > summary.selected_positive_avg_shadow_rank {
        writeln!(
            md,
            "- 诊断结论：系统对正向 shadow 支撑的名字仍有 **低配/漏选** 倾向，所以当前权重和 reserve 已按 recall 压力上调。"
        )?;
    } else if summary.selected_positive_false_positive_rate > summary.selected_positive_capture_rate
    {
        writeln!(
            md,
            "- 诊断结论：shadow 支撑的入选名里误报偏多，所以 live 权重会更保守，避免把高波错当 alpha。"
        )?;
    } else {
        writeln!(
            md,
            "- 诊断结论：shadow 层当前更像有效的筛选加分项，live 权重维持温和上调而不是激进放大。"
        )?;
    }
    writeln!(md)?;
    Ok(())
}

fn load_postmortem_examples<P>(
    db: &Connection,
    label_filter: &str,
    params: P,
) -> Result<Vec<String>>
where
    P: duckdb::Params,
{
    let sql = format!(
        "SELECT symbol, label, best_ret_pct, next_close_ret_pct
         FROM alpha_postmortem
         WHERE session = 'daily'
           AND evaluation_date >= CAST(? AS DATE)
           AND evaluation_date < CAST(? AS DATE)
           AND {}
         ORDER BY COALESCE(best_ret_pct, -999) DESC, evaluation_date DESC
         LIMIT 3",
        label_filter
    );
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(params, |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (symbol, label, best_ret_pct, next_close_ret_pct) = row?;
        let note = match label.as_str() {
            "captured" => format!(
                "{}（2日最佳{}%，次日收盘{}%）",
                symbol,
                fmt_opt_f64(best_ret_pct, 1),
                fmt_opt_f64(next_close_ret_pct, 1)
            ),
            "missed_alpha" => format!(
                "{}（忽略后2日最佳{}%）",
                symbol,
                fmt_opt_f64(best_ret_pct, 1)
            ),
            _ => format!(
                "{}（次日收盘{}%，2日最佳{}%）",
                symbol,
                fmt_opt_f64(next_close_ret_pct, 1),
                fmt_opt_f64(best_ret_pct, 1)
            ),
        };
        out.push(note);
    }
    Ok(out)
}

fn load_actionable_missed_examples(
    db: &Connection,
    cutoff: &str,
    date_str: &str,
) -> Result<(usize, Vec<String>)> {
    let mut count_stmt = db.prepare(
        "SELECT COUNT(DISTINCT p.symbol)
         FROM alpha_postmortem p
         INNER JOIN report_decisions d
           ON p.report_date = d.report_date
          AND p.session = d.session
          AND p.symbol = d.symbol
          AND p.selection_status = d.selection_status
         WHERE p.session = 'daily'
           AND p.report_date >= CAST(? AS DATE)
           AND p.report_date < CAST(? AS DATE)
           AND p.label = 'missed_alpha'
           AND p.symbol NOT LIKE '300%'
           AND p.symbol NOT LIKE '301%'
           AND d.report_bucket = 'THEME ROTATION'
           AND d.signal_confidence IN ('HIGH', 'MODERATE')
           AND d.execution_mode IN ('executable', 'wait_pullback')",
    )?;
    let actionable_missed_count = count_stmt
        .query_row(duckdb::params![cutoff, date_str], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_or(0) as usize;

    let mut stmt = db.prepare(
        "WITH ranked AS (
            SELECT
                p.symbol,
                MAX(COALESCE(p.best_ret_pct, 0.0)) AS best_ret_pct,
                ANY_VALUE(d.report_bucket) AS report_bucket,
                ANY_VALUE(d.signal_confidence) AS signal_confidence,
                ANY_VALUE(d.execution_mode) AS execution_mode
            FROM alpha_postmortem p
            INNER JOIN report_decisions d
              ON p.report_date = d.report_date
             AND p.session = d.session
             AND p.symbol = d.symbol
             AND p.selection_status = d.selection_status
            WHERE p.session = 'daily'
              AND p.report_date >= CAST(? AS DATE)
              AND p.report_date < CAST(? AS DATE)
              AND p.label = 'missed_alpha'
              AND p.symbol NOT LIKE '300%'
              AND p.symbol NOT LIKE '301%'
              AND d.report_bucket = 'THEME ROTATION'
              AND d.signal_confidence IN ('HIGH', 'MODERATE')
              AND d.execution_mode IN ('executable', 'wait_pullback')
            GROUP BY p.symbol
         )
         SELECT symbol, best_ret_pct, report_bucket, signal_confidence, execution_mode
         FROM ranked
         ORDER BY best_ret_pct DESC, symbol
         LIMIT 3",
    )?;
    let rows = stmt.query_map(duckdb::params![cutoff, date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, f64>(1).unwrap_or(0.0),
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;

    let mut examples = Vec::new();
    for row in rows {
        let (symbol, best_ret_pct, report_bucket, signal_confidence, execution_mode) = row?;
        examples.push(format!(
            "{}（{} / {} / {}，2日最佳{}%）",
            symbol,
            report_bucket,
            signal_confidence,
            execution_mode,
            fmt_opt_f64(Some(best_ret_pct), 1)
        ));
    }
    Ok((actionable_missed_count, examples))
}

fn compute_headline_gate(
    db: &Connection,
    date_str: &str,
    notable: &[NotableItem],
) -> HeadlineGateSummary {
    let as_of = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let signals: Vec<HeadlineSignalSummary> = notable
        .iter()
        .map(|item| HeadlineSignalSummary {
            direction: item.signal.direction.clone(),
            trend_prob: item.detail.get("trend_prob").and_then(|v| v.as_f64()),
            report_bucket: item.report_bucket.clone(),
        })
        .collect();
    summarize_headline_gate(db, as_of, &signals)
}

fn render_headline_gate(md: &mut String, gate: &HeadlineGateSummary) -> Result<()> {
    writeln!(md, "### Headline Gate")?;
    writeln!(md)?;
    writeln!(
        md,
        "- mode = {} | bias = {} | directional_regime_allowed = {}",
        gate.mode, gate.bias, gate.allow_directional_regime,
    )?;
    writeln!(md, "- reporting_rule = {}", gate.reporting_rule)?;
    writeln!(
        md,
        "- p_ret_positive = {} | brier_score = {} | calibration_n = {} | regime_duration = {}天",
        fmt_opt_f64(gate.inputs.p_ret_positive, 4),
        fmt_opt_f64(gate.inputs.brier_score, 4),
        gate.inputs.calibration_n,
        gate.inputs.regime_duration_days,
    )?;
    writeln!(
        md,
        "- trend_prob_range = {} ~ {} (span={})",
        fmt_opt_f64(gate.inputs.trend_prob_min, 3),
        fmt_opt_f64(gate.inputs.trend_prob_max, 3),
        fmt_opt_f64(gate.inputs.trend_prob_span, 3),
    )?;
    writeln!(
        md,
        "- direction_concentration = {} | dominant_direction = {}",
        gate.inputs
            .direction_concentration
            .map(|v| format!("{:.1}%", v * 100.0))
            .unwrap_or_else(|| "-".to_string()),
        gate.inputs.dominant_direction,
    )?;
    writeln!(
        md,
        "- vol_hmm = {} | macro_vol_state = {} | gate_multiplier = {} | vol_macro_conflict = {}",
        gate.inputs.vol_hmm_regime.as_deref().unwrap_or("-"),
        gate.inputs.macro_vol_state.as_deref().unwrap_or("-"),
        fmt_opt_f64(gate.inputs.gate_multiplier, 2),
        gate.inputs.vol_macro_conflict,
    )?;
    writeln!(md, "- reasons:")?;
    for reason in &gate.reasons {
        writeln!(md, "  - {}", reason)?;
    }
    writeln!(md)?;
    Ok(())
}

fn render_alpha_bulletin(md: &mut String, date_str: &str, market: &str) -> Result<()> {
    let file_name = format!("alpha_bulletin_{}.md", market);
    let candidates = [
        format!(
            "reports/review_dashboard/strategy_backtest/{}/{}",
            date_str, file_name
        ),
        format!(
            "../reports/review_dashboard/strategy_backtest/{}/{}",
            date_str, file_name
        ),
        format!(
            "../quant-research-cn/reports/review_dashboard/strategy_backtest/{}/{}",
            date_str, file_name
        ),
    ];

    for candidate in candidates {
        let path = Path::new(&candidate);
        if !path.exists() {
            continue;
        }
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    return Ok(());
                }
                writeln!(md, "{}", trimmed)?;
                writeln!(md)?;
                return Ok(());
            }
            Err(err) => {
                warn!(path = %candidate, error = %err, "alpha bulletin read failed");
                return Ok(());
            }
        }
    }
    Ok(())
}

fn report_bucket_description(bucket: &str) -> &'static str {
    match bucket {
        "CORE BOOK" => "主报告正文层。优先看这里来形成 house view。",
        "RANGE CORE" => {
            "区间复核层。headline 仍偏 uncertain，只能记录回踩复核、确认条件和失效条件；不得作为买入清单。"
        }
        "TACTICAL CONTINUATION" => {
            "战术观察层。只保留少量 continuation 复核对象；不得给开仓/追价指令。"
        }
        "THEME ROTATION" => "主题轮动层。更适合写成行业/概念/资金主线，而不是单一主书押注。",
        _ => "雷达层。用于保留边缘但仍值得持续跟踪的信号。",
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// File 3: Events — 事件与新闻
// ═════════════════════════════════════════════════════════════════════════════

fn render_events(db: &Connection, date_str: &str, generated_at: &str) -> Result<String> {
    let mut md = String::with_capacity(32 * 1024);

    writeln!(md, "# A股量化研究 Payload — 事件与新闻")?;
    writeln!(md, "## 生成时间: {}", generated_at)?;
    writeln!(md, "## 交易日: {}", date_str)?;
    writeln!(md)?;
    writeln!(md, "{}", PRECISION_RULES)?;

    // ── 近期业绩预告 ──────────────────────────────────────────────────────
    render_recent_forecasts(&mut md, db, date_str)?;

    // ── 即将解禁 ──────────────────────────────────────────────────────────
    render_upcoming_unlocks(&mut md, db, date_str)?;

    // ── 财报披露日历 ──────────────────────────────────────────────────────
    render_disclosure_calendar(&mut md, db, date_str)?;

    // ── 个股新闻 (DeepSeek enriched) ──────────────────────────────────────
    render_enriched_news(&mut md, db, date_str)?;

    // ── 新闻情感汇总 ──────────────────────────────────────────────────────
    render_sentiment_summary(&mut md, db, date_str)?;

    // ── 股东增减持 ──────────────────────────────────────────────────────
    render_holder_trades(&mut md, db, date_str)?;

    // ── 回购 ──────────────────────────────────────────────────────────────
    render_repurchase(&mut md, db, date_str)?;

    Ok(md)
}

fn render_recent_forecasts(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 近期业绩预告 (7日内)")?;
    writeln!(md)?;
    writeln!(md, "| 代码 | 公告日 | 类型 | 变动幅度 | 摘要 |")?;
    writeln!(md, "|------|--------|------|----------|------|")?;

    let sql = "SELECT ts_code, CAST(ann_date AS VARCHAR), forecast_type,
                      CONCAT(COALESCE(CAST(ROUND(p_change_min, 1) AS VARCHAR), '?'),
                             '% ~ ',
                             COALESCE(CAST(ROUND(p_change_max, 1) AS VARCHAR), '?'),
                             '%') AS change_range,
                      COALESCE(summary, '-')
               FROM forecast
               WHERE ann_date >= CAST(? AS DATE) - INTERVAL '7 days'
                 AND ann_date <= CAST(? AS DATE)
               ORDER BY ann_date DESC
               LIMIT 30";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, String, String, String)> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (近7日无业绩预告) | - | - | - | - |")?;
            } else {
                for (code, ann, ftype, range, summary) in &rows {
                    // Truncate summary to 50 chars
                    let short_summary: String = summary.chars().take(50).collect();
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        code, ann, ftype, range, short_summary,
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "forecast query failed");
            writeln!(md, "| (查询失败) | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_upcoming_unlocks(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 即将解禁 (30日内)")?;
    writeln!(md)?;
    writeln!(
        md,
        "| 代码 | 解禁日 | 解禁比例% | 解禁股数(万股) | 持有人 | 类型 |"
    )?;
    writeln!(
        md,
        "|------|--------|----------|---------------|--------|------|"
    )?;

    let sql = "SELECT ts_code, CAST(float_date AS VARCHAR), float_ratio,
                      float_share, holder_name, share_type
               FROM share_unlock
               WHERE float_date >= CAST(? AS DATE)
                 AND float_date <= CAST(? AS DATE) + INTERVAL '30 days'
               ORDER BY float_date, float_ratio DESC
               LIMIT 30";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                String,
                Option<f64>,
                Option<f64>,
                Option<String>,
                Option<String>,
            )> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (30日内无解禁) | - | - | - | - | - |")?;
            } else {
                for (code, fdate, ratio, shares, holder, stype) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} | {} |",
                        code,
                        fdate,
                        fmt_pct(*ratio),
                        fmt_f64_wan(*shares),
                        holder.as_deref().unwrap_or("-"),
                        stype.as_deref().unwrap_or("-"),
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "share_unlock query failed");
            writeln!(md, "| (查询失败) | - | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_disclosure_calendar(md: &mut String, db: &Connection, _date_str: &str) -> Result<()> {
    writeln!(md, "### 财报披露日历")?;
    writeln!(md)?;
    writeln!(md, "| 代码 | 报告期 | 预约日 | 实际日 |")?;
    writeln!(md, "|------|--------|--------|--------|")?;

    // Get the latest quarter end date
    let sql = "SELECT ts_code, CAST(end_date AS VARCHAR),
                      COALESCE(pre_date, '-'),
                      COALESCE(actual_date, '-')
               FROM disclosure_date
               WHERE end_date = (
                   SELECT MAX(end_date) FROM disclosure_date
               )
               ORDER BY COALESCE(actual_date, pre_date, '9999')
               LIMIT 30";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, String, String)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (无数据) | - | - | - |")?;
            } else {
                for (code, end, pre, actual) in &rows {
                    writeln!(md, "| {} | {} | {} | {} |", code, end, pre, actual)?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "disclosure_date query failed");
            writeln!(md, "| (查询失败) | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_enriched_news(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 个股新闻 (DeepSeek enriched)")?;
    writeln!(md)?;

    let sql = "SELECT ts_code, published_at, headline, event_type, sentiment,
                      relevance, summary_one_line
               FROM news_enriched
               WHERE TRY_CAST(published_at AS TIMESTAMP) >= CAST(CAST(? AS DATE) - INTERVAL '3 days' AS TIMESTAMP)
                 AND TRY_CAST(published_at AS TIMESTAMP) < CAST(CAST(? AS DATE) + INTERVAL '1 day' AS TIMESTAMP)
               ORDER BY relevance DESC, published_at DESC
               LIMIT 40";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<f64>,
                Option<String>,
            )> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- (近3日无enriched新闻)")?;
            } else {
                for (code, pub_at, headline, event_type, sentiment, relevance, summary) in &rows {
                    writeln!(
                        md,
                        "- **{}** [{}] [{}] rel={:.2}",
                        code,
                        event_type.as_deref().unwrap_or("other"),
                        sentiment.as_deref().unwrap_or("neutral"),
                        relevance.unwrap_or(0.0),
                    )?;
                    writeln!(
                        md,
                        "  {}",
                        summary
                            .as_deref()
                            .unwrap_or(headline.as_deref().unwrap_or("-")),
                    )?;
                    writeln!(md, "  时间: {}", pub_at)?;
                }
            }
        }
        Err(_) => {
            // news_enriched table may not exist yet — fall back to stock_news
            writeln!(md, "*(enriched表不可用, 回退到原始新闻)*")?;
            writeln!(md)?;
            render_raw_stock_news(md, db, date_str)?;
        }
    }
    writeln!(md)?;

    // Also render raw stock_news for supplementary coverage
    writeln!(md, "### 原始新闻 (AKShare stock_news, 近3日)")?;
    writeln!(md)?;
    render_raw_stock_news(md, db, date_str)?;

    Ok(())
}

fn render_raw_stock_news(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    let sql = "SELECT ts_code, publish_time, title, source
               FROM stock_news
               WHERE TRY_CAST(publish_time AS TIMESTAMP) >= CAST(CAST(? AS DATE) - INTERVAL '3 days' AS TIMESTAMP)
                 AND TRY_CAST(publish_time AS TIMESTAMP) < CAST(CAST(? AS DATE) + INTERVAL '1 day' AS TIMESTAMP)
               ORDER BY publish_time DESC
               LIMIT 30";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, String, Option<String>)> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- (无原始新闻)")?;
            } else {
                for (code, pub_time, title, source) in &rows {
                    writeln!(
                        md,
                        "- **{}**: {} — 来源: {}, 时间: {}",
                        code,
                        title,
                        source.as_deref().unwrap_or("-"),
                        pub_time,
                    )?;
                }
            }
        }
        Err(_) => {
            writeln!(md, "- (stock_news表不可用)")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_sentiment_summary(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 新闻情感汇总 (DeepSeek)")?;
    writeln!(md)?;

    let sql = "SELECT ts_code,
                      COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) AS pos,
                      COUNT(CASE WHEN sentiment = 'neutral' THEN 1 END) AS neu,
                      COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) AS neg,
                      COUNT(CASE WHEN sentiment = 'negative'
                                  AND sentiment_confidence >= 0.7 THEN 1 END) AS high_neg
               FROM news_enriched
               WHERE TRY_CAST(published_at AS TIMESTAMP) >= CAST(CAST(? AS DATE) - INTERVAL '3 days' AS TIMESTAMP)
                 AND TRY_CAST(published_at AS TIMESTAMP) < CAST(CAST(? AS DATE) + INTERVAL '1 day' AS TIMESTAMP)
               GROUP BY ts_code
               ORDER BY neg DESC, high_neg DESC";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, i64, i64, i64, i64)> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "- (无情感数据)")?;
            } else {
                writeln!(md, "| 代码 | 正面 | 中性 | 负面 | 高置信负面 |")?;
                writeln!(md, "|------|------|------|------|-----------|")?;
                for (code, pos, neu, neg, high_neg) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} |",
                        code, pos, neu, neg, high_neg,
                    )?;
                }
            }
        }
        Err(_) => {
            writeln!(md, "- (news_enriched表不可用)")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_holder_trades(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 股东增减持 (近7日)")?;
    writeln!(md)?;
    writeln!(
        md,
        "| 代码 | 公告日 | 股东 | 类型 | 增减持 | 变动比例% | 变动后持股% |"
    )?;
    writeln!(
        md,
        "|------|--------|------|------|--------|----------|------------|"
    )?;

    let sql = "SELECT ts_code, CAST(ann_date AS VARCHAR), holder_name,
                      COALESCE(holder_type, '-'),
                      COALESCE(in_de, '-'),
                      change_ratio,
                      after_ratio
               FROM stk_holdertrade
               WHERE ann_date >= CAST(? AS DATE) - INTERVAL '7 days'
                 AND ann_date <= CAST(? AS DATE)
               ORDER BY ann_date DESC, ABS(COALESCE(change_ratio, 0)) DESC
               LIMIT 20";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(
                String,
                String,
                String,
                String,
                String,
                Option<f64>,
                Option<f64>,
            )> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                        row.get::<_, Option<f64>>(6)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (近7日无增减持) | - | - | - | - | - | - |")?;
            } else {
                for (code, ann, holder, htype, in_de, change_ratio, after_ratio) in &rows {
                    // Truncate holder name to 10 chars
                    let short_holder: String = holder.chars().take(10).collect();
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} | {} | {} |",
                        code,
                        ann,
                        short_holder,
                        htype,
                        in_de,
                        fmt_pct(*change_ratio),
                        fmt_pct(*after_ratio),
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "stk_holdertrade query failed");
            writeln!(md, "| (查询失败) | - | - | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

fn render_repurchase(md: &mut String, db: &Connection, date_str: &str) -> Result<()> {
    writeln!(md, "### 回购 (近7日)")?;
    writeln!(md)?;
    writeln!(
        md,
        "| 代码 | 公告日 | 进度 | 到期日 | 数量(万股) | 金额(万) |"
    )?;
    writeln!(
        md,
        "|------|--------|------|--------|-----------|---------|"
    )?;

    let sql = "SELECT ts_code, CAST(ann_date AS VARCHAR),
                      COALESCE(proc, '-'),
                      COALESCE(exp_date, '-'),
                      vol, amount
               FROM repurchase
               WHERE ann_date >= CAST(? AS DATE) - INTERVAL '7 days'
                 AND ann_date <= CAST(? AS DATE)
               ORDER BY ann_date DESC
               LIMIT 15";

    match db.prepare(sql) {
        Ok(mut stmt) => {
            let rows: Vec<(String, String, String, String, Option<f64>, Option<f64>)> = stmt
                .query_map(duckdb::params![date_str, date_str], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, Option<f64>>(5)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();

            if rows.is_empty() {
                writeln!(md, "| (近7日无回购) | - | - | - | - | - |")?;
            } else {
                for (code, ann, proc_, exp, vol, amount) in &rows {
                    writeln!(
                        md,
                        "| {} | {} | {} | {} | {} | {} |",
                        code,
                        ann,
                        proc_,
                        exp,
                        fmt_f64_wan(*vol),
                        fmt_f64_wan(*amount),
                    )?;
                }
            }
        }
        Err(e) => {
            warn!(err = %e, "repurchase query failed");
            writeln!(md, "| (查询失败) | - | - | - | - | - |")?;
        }
    }
    writeln!(md)?;
    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════
// Helper queries for notable items
// ═════════════════════════════════════════════════════════════════════════════

/// Fetch close, PE, PB, total_mv for a stock.
fn query_stock_valuation(
    db: &Connection,
    ts_code: &str,
    date_str: &str,
) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    let sql = "SELECT p.close, d.pe, d.pb, d.total_mv
               FROM prices p
               LEFT JOIN daily_basic d ON p.ts_code = d.ts_code AND p.trade_date = d.trade_date
               WHERE p.ts_code = ?
                 AND p.trade_date = (
                     SELECT MAX(trade_date) FROM prices
                     WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
                 )";

    db.prepare(sql)
        .and_then(|mut stmt| {
            stmt.query_row(duckdb::params![ts_code, ts_code, date_str], |row| {
                Ok((
                    row.get::<_, Option<f64>>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, Option<f64>>(3)?,
                ))
            })
        })
        .unwrap_or((None, None, None, None))
}

/// Fetch momentum detail: regime, vol_bucket, CI from analytics.
fn query_momentum_detail(
    db: &Connection,
    ts_code: &str,
    date_str: &str,
) -> (Option<String>, Option<String>, Option<f64>, Option<f64>) {
    let sql = "SELECT metric, value, detail
               FROM analytics
               WHERE ts_code = ? AND as_of = ? AND module = 'momentum'
               ORDER BY metric";

    let mut regime = None;
    let mut vol_bucket = None;
    let mut ci_low = None;
    let mut ci_high = None;

    if let Ok(mut stmt) = db.prepare(sql) {
        let rows: Vec<(String, f64, Option<String>)> = stmt
            .query_map(duckdb::params![ts_code, date_str], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map(|r| r.filter_map(|x| x.ok()).collect())
            .unwrap_or_default();

        for (metric, value, detail) in &rows {
            match metric.as_str() {
                "regime" => {
                    regime = Some(
                        match *value as i32 {
                            0 => "trending",
                            1 => "mean_reverting",
                            _ => "noisy",
                        }
                        .to_string(),
                    );
                }
                "vol_bucket" => {
                    vol_bucket = Some(
                        match *value as i32 {
                            0 => "low",
                            1 => "mid",
                            _ => "high",
                        }
                        .to_string(),
                    );
                }
                "trend_prob" => {
                    // Parse CI from detail JSON if available
                    if let Some(d) = detail {
                        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(d) {
                            ci_low = obj.get("ci_low").and_then(|v| v.as_f64());
                            ci_high = obj.get("ci_high").and_then(|v| v.as_f64());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    (regime, vol_bucket, ci_low, ci_high)
}

/// Fetch flow component z-scores from analytics.
fn query_flow_components(db: &Connection, ts_code: &str, date_str: &str) -> Vec<(String, f64)> {
    let sql = "SELECT metric, value
               FROM analytics
               WHERE ts_code = ? AND as_of = ? AND module = 'flow'
                 AND metric != 'information_score'
               ORDER BY metric";

    db.prepare(sql)
        .and_then(|mut stmt| {
            let rows = stmt.query_map(duckdb::params![ts_code, date_str], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
        .unwrap_or_default()
}

/// Fetch the latest forecast for a stock.
fn query_latest_forecast(
    db: &Connection,
    ts_code: &str,
    date_str: &str,
) -> Option<(String, Option<f64>, Option<f64>)> {
    let sql = "SELECT forecast_type, p_change_min, p_change_max
               FROM forecast
               WHERE ts_code = ? AND ann_date <= CAST(? AS DATE)
               ORDER BY ann_date DESC
               LIMIT 1";

    db.prepare(sql)
        .and_then(|mut stmt| {
            stmt.query_row(duckdb::params![ts_code, date_str], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                ))
            })
        })
        .ok()
}

/// Fetch latest moneyflow: net_mf_amount, super-large net.
fn query_moneyflow(db: &Connection, ts_code: &str, date_str: &str) -> Option<(f64, f64)> {
    let sql = "SELECT net_mf_amount,
                      COALESCE(buy_elg_amount, 0) - COALESCE(sell_elg_amount, 0) AS elg_net
               FROM moneyflow
               WHERE ts_code = ? AND trade_date = (
                   SELECT MAX(trade_date) FROM moneyflow
                   WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
               )";

    db.prepare(sql)
        .and_then(|mut stmt| {
            stmt.query_row(duckdb::params![ts_code, ts_code, date_str], |row| {
                Ok((
                    row.get::<_, Option<f64>>(0)?.unwrap_or(0.0),
                    row.get::<_, Option<f64>>(1)?.unwrap_or(0.0),
                ))
            })
        })
        .ok()
}

/// Fetch margin detail: rzye today + 5D delta.
fn query_margin_detail(db: &Connection, ts_code: &str, date_str: &str) -> Option<(f64, f64)> {
    let sql = "WITH ranked AS (
                   SELECT rzye,
                          ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                   FROM margin_detail
                   WHERE ts_code = ? AND trade_date <= CAST(? AS DATE)
                   LIMIT 6
               )
               SELECT
                   (SELECT rzye FROM ranked WHERE rn = 1),
                   COALESCE((SELECT rzye FROM ranked WHERE rn = 1), 0)
                 - COALESCE((SELECT rzye FROM ranked WHERE rn = 6), 0)";

    db.prepare(sql)
        .and_then(|mut stmt| {
            stmt.query_row(duckdb::params![ts_code, date_str], |row| {
                Ok((
                    row.get::<_, Option<f64>>(0)?.unwrap_or(0.0),
                    row.get::<_, Option<f64>>(1)?.unwrap_or(0.0),
                ))
            })
        })
        .ok()
}

// ═════════════════════════════════════════════════════════════════════════════
// Formatting utilities
// ═════════════════════════════════════════════════════════════════════════════

/// Format an Option<f64> with a given number of decimal places.
fn fmt_opt_f64(v: Option<f64>, decimals: usize) -> String {
    match v {
        Some(val) => format!("{:.prec$}", val, prec = decimals),
        None => "-".to_string(),
    }
}

/// Format Option<f64> to 2 decimal places, or "-" if None.
fn fmt_f64(v: Option<f64>) -> String {
    match v {
        Some(val) => format!("{:.2}", val),
        None => "-".to_string(),
    }
}

/// Format percentage (already in % units): "1.23" or "-".
fn fmt_pct(v: Option<f64>) -> String {
    match v {
        Some(val) => format!("{:.2}", val),
        None => "-".to_string(),
    }
}

/// Format volume — large numbers get K/M suffix.
fn fmt_vol(v: Option<f64>) -> String {
    match v {
        Some(val) if val >= 1_000_000.0 => format!("{:.1}M", val / 1_000_000.0),
        Some(val) if val >= 1_000.0 => format!("{:.1}K", val / 1_000.0),
        Some(val) => format!("{:.0}", val),
        None => "-".to_string(),
    }
}

/// Format f64 as "亿" (divide by 10000 if already in 万 units).
/// The Tushare moneyflow amounts are in 万 (10K), but northbound_flow amounts
/// and total_mv in daily_basic are in 万. For display we convert to 亿 (/10000).
fn fmt_f64_yi(v: Option<f64>) -> String {
    match v {
        Some(val) => format!("{:.2}", val / 10000.0),
        None => "-".to_string(),
    }
}

/// Format f64 as 亿 from raw yuan amounts.
fn fmt_f64_yuan_to_yi(v: Option<f64>) -> String {
    match v {
        Some(val) => format!("{:.2}", val / 100_000_000.0),
        None => "-".to_string(),
    }
}

/// Format f64 as 万 (10K units, Tushare default for many monetary fields).
fn fmt_f64_wan(v: Option<f64>) -> String {
    match v {
        Some(val) => format!("{:.2}", val),
        None => "-".to_string(),
    }
}

/// Classify macro series cadence for footer notes.
fn macro_cadence(series_id: &str) -> &'static str {
    match series_id {
        // Monthly indicators
        "CPI" | "CPI_YOY" | "CPIAUCSL" => "月度, 通常滞后约3-4周",
        "PPI" | "PPI_YOY" => "月度, 通常滞后约3-4周",
        "PMI" | "PMI_MFG" => "制造业PMI, 月度; 必须标注官方/第三方口径与日期",
        "M2" | "M2_YOY" => "月度, 通常滞后约3-4周",
        // Social financing, credit
        "TSF" | "社融" => "月度, 通常滞后约3-4周",
        // Daily/weekly rates
        "M0009970" | "SHIBOR" | "Shibor" | "SHIBOR_ON" => "日度",
        "M0062063" | "LPR" | "LPR_1Y" => "月度(每月20日)",
        // Quarterly
        "GDP" => "季度, 滞后~45天",
        _ => "-",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cn_priced_in_score, execution_summary_sentence, macro_cadence, report_bucket_description,
        setup_alpha_view,
    };
    use crate::filtering::notable::{NotableItem, Signal};
    use serde_json::json;

    #[test]
    fn uncertain_headline_is_context_not_execution_blocker() {
        let summary = execution_summary_sentence(
            "TACTICAL CONTINUATION",
            "uncertain",
            Some("executable"),
            Some(0.708),
            Some(2.20),
            Some(0.99),
            Some(3.81),
            true,
            "none",
        );

        assert!(summary.contains("仅作辅助上下文，不单独否决"));
        assert!(summary.contains("可接受追价上限"));
        assert!(summary.contains("参考回踩价=3.81"));
    }

    #[test]
    fn blocked_main_signal_gate_suppresses_trade_instructions() {
        let summary = execution_summary_sentence(
            "CORE BOOK",
            "trend",
            Some("executable"),
            Some(0.708),
            Some(2.20),
            Some(0.99),
            Some(3.81),
            false,
            "execution_score_below_core",
        );

        assert!(summary.contains("主信号门槛未通过"));
        assert!(summary.contains("不输出买入/追价指令"));
        assert!(summary.contains("T+1"));
        assert!(summary.contains("止损不是硬止损"));
        assert!(summary.contains("不写入场、止盈或T1/T2"));
        assert!(!summary.contains("可接受追价上限"));
    }

    #[test]
    fn macro_cadence_labels_current_cn_series_ids() {
        assert!(macro_cadence("PMI_MFG").contains("制造业PMI"));
        assert!(macro_cadence("CPI_YOY").contains("3-4周"));
        assert_eq!(macro_cadence("SHIBOR_ON"), "日度");
        assert_eq!(macro_cadence("LPR_1Y"), "月度(每月20日)");
    }

    #[test]
    fn range_core_description_is_review_not_buy_list() {
        let description = report_bucket_description("RANGE CORE");

        assert!(description.contains("复核层"));
        assert!(description.contains("不得作为买入清单"));
    }

    #[test]
    fn high_return_without_trend_confirmation_is_chase_risk() {
        let score = cn_priced_in_score("executable", 10.0, 24.0, 0.48, 0.10, 0.20);

        assert!(score >= 0.66);
    }

    #[test]
    fn confirmed_breakout_is_not_mechanically_blocked() {
        let item = NotableItem {
            ts_code: "000001.SZ".to_string(),
            name: "测试银行".to_string(),
            composite_score: 0.50,
            magnitude_score: 0.30,
            event_score: 0.40,
            momentum_score: 0.55,
            flow_score: 0.45,
            cross_asset_score: 0.20,
            report_bucket: "TACTICAL CONTINUATION".to_string(),
            report_reason: "unit test".to_string(),
            signal: Signal {
                confidence: "HIGH".to_string(),
                direction: "bullish".to_string(),
                horizon: "5D".to_string(),
            },
            detail: json!({
                "ret_5d": 10.0,
                "ret_20d": 24.0,
                "trend_prob": 0.62,
                "setup_score": 0.66,
                "setup_direction": "bullish",
                "continuation_score": 0.58,
                "execution_mode": "executable",
                "fade_risk": 0.20,
                "stale_chase_risk": 0.10
            }),
        };

        let view = setup_alpha_view(&item, None);

        assert_eq!(view.bucket, "breakout_acceptance");
        assert!(view.priced_in_score < 0.72);
    }
}
