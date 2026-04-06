/// Tushare Pro API client — modular fetcher.
///
/// All endpoints use a unified POST interface:
///   POST https://api.tushare.pro
///   { "api_name": "...", "token": "...", "params": {...}, "fields": "..." }
///
/// Rate limit: ~200 req/min at 2000-credit tier → 500ms delay between calls.
pub mod prices;
pub mod fundamental;
pub mod flow;
pub mod event;
pub mod market;
pub mod universe;
pub mod macro_cn;

use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};
use tracing::info;

use crate::config::Settings;

pub const API_URL: &str = "https://api.tushare.pro";
pub const RATE_DELAY_MS: u64 = 500;

// ── API types ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct TushareRequest {
    pub api_name: String,
    pub token: String,
    pub params: serde_json::Value,
    pub fields: String,
}

#[derive(Deserialize)]
pub struct TushareResponse {
    pub code: i64,
    pub data: Option<TushareData>,
    pub msg: Option<String>,
}

#[derive(Deserialize)]
pub struct TushareData {
    pub fields: Vec<String>,
    pub items: Vec<Vec<serde_json::Value>>,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Convert Tushare date "YYYYMMDD" → DuckDB date "YYYY-MM-DD"
pub fn ts_date(raw: &str) -> String {
    if raw.len() == 8 {
        format!("{}-{}-{}", &raw[0..4], &raw[4..6], &raw[6..8])
    } else {
        raw.to_string()
    }
}

/// Extract string from JSON Value and convert date format
pub fn ts_date_val(v: &serde_json::Value) -> String {
    ts_date(v.as_str().unwrap_or_default())
}

/// Extract optional string field; returns None-safe string
pub fn str_val(v: &serde_json::Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

// ── Generic query helper ─────────────────────────────────────────────────────

pub async fn query(
    client: &reqwest::Client,
    token: &str,
    api_name: &str,
    params: serde_json::Value,
    fields: &str,
) -> Result<Vec<Vec<serde_json::Value>>> {
    let req = TushareRequest {
        api_name: api_name.to_string(),
        token: token.to_string(),
        params,
        fields: fields.to_string(),
    };

    let resp: TushareResponse = super::http::send_with_retry(|| {
        client.post(API_URL).json(&req)
    })
    .await?
    .json()
    .await?;

    if resp.code != 0 {
        return Err(anyhow!(
            "tushare api_name={} error code={} msg={}",
            api_name,
            resp.code,
            resp.msg.unwrap_or_default()
        ));
    }

    sleep(Duration::from_millis(RATE_DELAY_MS)).await;

    Ok(resp.data.map(|d| d.items).unwrap_or_default())
}

/// Generic fetch-and-store: query Tushare → iterate rows → execute INSERT.
/// `min_cols` = minimum columns per row to accept.
/// `inserter` is called for each valid row.
pub async fn fetch_and_store<F>(
    client: &reqwest::Client,
    token: &str,
    api_name: &str,
    params: serde_json::Value,
    fields: &str,
    min_cols: usize,
    mut inserter: F,
) -> Result<usize>
where
    F: FnMut(&[serde_json::Value]) -> Result<()>,
{
    let rows = query(client, token, api_name, params, fields).await?;
    let mut total = 0;
    for row in &rows {
        if row.len() < min_cols {
            continue;
        }
        inserter(row)?;
        total += 1;
    }
    Ok(total)
}

// ── Public fetch_all orchestrator ────────────────────────────────────────────

pub async fn fetch_all(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
) -> Result<usize> {
    let client = super::http::build_client()?;
    let token = &cfg.api.tushare_token;

    // Fetch index constituents → build symbol list
    let symbols = universe::fetch_universe(&client, token, cfg).await?;
    info!(symbols = symbols.len(), "universe built");

    let mut total = 0usize;

    // ── Prices & valuation ──
    total += prices::fetch_daily_prices(&client, token, db, as_of).await?;
    total += prices::fetch_daily_basic(&client, token, db, as_of).await?;
    total += prices::fetch_index_daily(&client, token, db, as_of).await?;

    // ── Fundamental (per-symbol queries — expensive, run weekly) ──
    // Financial statements require ts_code param (Tushare limitation).
    // 332 symbols × 4 endpoints × 500ms = ~11 min. Only run on init or weekly.
    // TODO: check run_log staleness and only fetch when stale.
    // For now, skip in daily fetch. Use `quant-cn init` for bulk load.
    //
    // total += fundamental::fetch_income(&client, token, db, as_of).await?;
    // total += fundamental::fetch_balancesheet(&client, token, db, as_of).await?;
    // total += fundamental::fetch_cashflow(&client, token, db, as_of).await?;
    // total += fundamental::fetch_fina_indicator(&client, token, db, as_of).await?;
    total += fundamental::fetch_dividend(&client, token, db, as_of).await?;

    // ── Flow (资金流向) ──
    total += flow::fetch_moneyflow(&client, token, db, as_of).await?;
    // Margin detail: backfill last 5 calendar days (Tushare updates with delay)
    for offset in 0..5 {
        let d = as_of - chrono::Duration::days(offset);
        // Skip weekends
        let dow = d.format("%u").to_string().parse::<u32>().unwrap_or(0);
        if dow >= 6 {
            continue;
        }
        // Skip if we already have data for this date
        let existing: i64 = db.query_row(
            "SELECT COUNT(*) FROM margin_detail WHERE trade_date = ?",
            duckdb::params![d.to_string()],
            |row| row.get(0),
        ).unwrap_or(0);
        if existing > 0 {
            continue;
        }
        total += flow::fetch_margin_detail(&client, token, db, d).await?;
    }
    total += flow::fetch_hsgt_flow(&client, token, db, as_of).await?;
    total += flow::fetch_hsgt_top10(&client, token, db, as_of).await?;
    total += flow::fetch_hk_hold(&client, token, db, as_of).await?;

    // ── Events ──
    total += event::fetch_forecast(&client, token, db, as_of).await?;
    total += event::fetch_block_trade(&client, token, db, as_of).await?;
    // top_list (龙虎榜) removed — requires >2000 Tushare credits, always returns 0 rows
    total += event::fetch_share_unlock(&client, token, db, as_of).await?;
    total += event::fetch_disclosure_date(&client, token, db, as_of).await?;
    total += event::fetch_stk_holdertrade(&client, token, db, as_of).await?;
    // pledge_detail requires ts_code — per-symbol query, skip in daily
    // total += event::fetch_pledge_detail(&client, token, db, as_of).await?;
    total += event::fetch_repurchase(&client, token, db, as_of).await?;
    total += event::fetch_stk_holdernumber(&client, token, db, as_of).await?;

    // ── Market (derivatives, gold, futures) ──
    total += market::fetch_opt_daily(&client, token, db, as_of).await?;
    total += market::fetch_sge_daily(&client, token, db, as_of).await?;
    total += market::fetch_cb_daily(&client, token, db, as_of).await?;
    total += market::fetch_fut_daily(&client, token, db, as_of).await?;
    total += market::fetch_fut_holding(&client, token, db, as_of).await?;

    // ── Macro data (Shibor daily, LPR monthly, macro indicators) ──
    total += macro_cn::fetch_shibor(&client, token, db, as_of).await?;
    total += macro_cn::fetch_lpr(&client, token, db, as_of).await?;
    let macro_series: Vec<(String, String)> = cfg.r#macro.series.iter()
        .map(|s| (s.id.clone(), s.name.clone()))
        .collect();
    total += macro_cn::fetch_macro_indicators(&client, token, db, &macro_series).await?;

    // ── Universe metadata (low-frequency, weekly refresh) ──
    total += universe::fetch_stock_basic(&client, token, db).await?;
    total += universe::fetch_industry_classify(&client, token, db).await?;
    total += market::fetch_opt_basic(&client, token, db).await?;

    Ok(total)
}
