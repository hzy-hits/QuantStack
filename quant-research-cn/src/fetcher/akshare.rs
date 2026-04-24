/// AKShare HTTP bridge client.
///
/// AKShare is Python-only. We call a lightweight FastAPI sidecar
/// at http://localhost:8321 that wraps the AKShare functions we need.
///
/// Endpoints unique to AKShare (not available on Tushare ≤2000 credits):
///   - /concept_boards   概念板块行情 (~400 boards, thematic rotation)
///   - /sector_fund_flow  行业资金流向 (sector-level flow ranking)
///   - /stock_news        个股新闻 (per-stock news for enrichment)
///
/// If the bridge is not running, all fetches return Ok(0) — non-fatal.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::Settings;

const BRIDGE_BASE: &str = "http://localhost:8321";

pub async fn fetch_all(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let client = match super::http::build_client() {
        Ok(c) => c,
        Err(e) => {
            warn!("akshare bridge client build failed: {}", e);
            return Ok(0);
        }
    };

    // Check if bridge is running
    let health = client.get(format!("{}/health", BRIDGE_BASE)).send().await;
    if health.is_err() {
        warn!("⚠ akshare bridge NOT running at {} — sector_fund_flow, concept_board, stock_news will all be EMPTY. Start bridge: cd bridge && uvicorn akshare_bridge:app --port 8321", BRIDGE_BASE);
        return Ok(0);
    }

    let mut total = 0usize;
    let date_str = as_of.format("%Y-%m-%d").to_string();

    total += fetch_concept_boards(&client, db, &date_str)
        .await
        .unwrap_or_else(|e| {
            warn!("concept_boards fetch failed: {}", e);
            0
        });

    total += fetch_sector_fund_flow(&client, db, &date_str)
        .await
        .unwrap_or_else(|e| {
            warn!("sector_fund_flow fetch failed: {}", e);
            0
        });

    total += fetch_stock_news(&client, db, cfg)
        .await
        .unwrap_or_else(|e| {
            warn!("stock_news fetch failed: {}", e);
            0
        });

    info!(rows = total, "akshare fetch complete");
    Ok(total)
}

// ── Concept Boards (概念板块) ────────────────────────────────────────────────

#[derive(Deserialize)]
struct ConceptBoardRow {
    board_name: Option<String>,
    board_code: Option<String>,
    pct_chg: Option<f64>,
    turnover_rate: Option<f64>,
    total_mv: Option<f64>,
    amount: Option<f64>,
    up_count: Option<i32>,
    down_count: Option<i32>,
    lead_stock: Option<String>,
    lead_pct: Option<f64>,
}

async fn fetch_concept_boards(
    client: &reqwest::Client,
    db: &Connection,
    date_str: &str,
) -> Result<usize> {
    let url = format!("{}/concept_boards", BRIDGE_BASE);
    let rows: Vec<ConceptBoardRow> = client.get(&url).send().await?.json().await?;

    let mut total = 0;
    for row in &rows {
        let name = match &row.board_name {
            Some(n) if !n.is_empty() => n,
            _ => continue,
        };
        db.execute(
            "INSERT OR REPLACE INTO concept_board
                (trade_date, board_name, board_code, pct_chg, turnover_rate,
                 total_mv, amount, up_count, down_count, lead_stock, lead_pct)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                date_str,
                name,
                row.board_code.as_deref().unwrap_or_default(),
                row.pct_chg,
                row.turnover_rate,
                row.total_mv,
                row.amount,
                row.up_count,
                row.down_count,
                row.lead_stock.as_deref().unwrap_or_default(),
                row.lead_pct,
            ],
        )?;
        total += 1;
    }
    info!(rows = total, "concept_board (概念板块) fetched");
    Ok(total)
}

// ── Sector Fund Flow (行业资金流向) ──────────────────────────────────────────

#[derive(Deserialize)]
struct SectorFundFlowRow {
    sector_name: Option<String>,
    pct_chg: Option<f64>,
    main_net_in: Option<f64>,
    main_net_pct: Option<f64>,
    super_net_in: Option<f64>,
    big_net_in: Option<f64>,
    mid_net_in: Option<f64>,
    small_net_in: Option<f64>,
}

async fn fetch_sector_fund_flow(
    client: &reqwest::Client,
    db: &Connection,
    date_str: &str,
) -> Result<usize> {
    let url = format!(
        "{}/sector_fund_flow?indicator=%E4%BB%8A%E6%97%A5",
        BRIDGE_BASE
    ); // "今日" URL-encoded
    let rows: Vec<SectorFundFlowRow> = client.get(&url).send().await?.json().await?;

    let mut total = 0;
    for row in &rows {
        let name = match &row.sector_name {
            Some(n) if !n.is_empty() => n,
            _ => continue,
        };
        db.execute(
            "INSERT OR REPLACE INTO sector_fund_flow
                (trade_date, sector_name, pct_chg, main_net_in, main_net_pct,
                 super_net_in, big_net_in, mid_net_in, small_net_in)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                date_str,
                name,
                row.pct_chg,
                row.main_net_in,
                row.main_net_pct,
                row.super_net_in,
                row.big_net_in,
                row.mid_net_in,
                row.small_net_in,
            ],
        )?;
        total += 1;
    }
    info!(rows = total, "sector_fund_flow (行业资金流向) fetched");
    Ok(total)
}

// ── Stock News (个股新闻) ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct StockNewsRow {
    title: Option<String>,
    content: Option<String>,
    publish_time: Option<String>,
    source: Option<String>,
    url: Option<String>,
}

async fn fetch_stock_news(
    client: &reqwest::Client,
    db: &Connection,
    cfg: &Settings,
) -> Result<usize> {
    // Fetch news for watchlist symbols only (keeps call count manageable).
    // Each call returns ~20 items, with ~0.5s latency.
    // 11 watchlist symbols × 0.5s ≈ 6 seconds.
    let symbols = &cfg.universe.watchlist;
    if symbols.is_empty() {
        return Ok(0);
    }

    let mut total = 0usize;
    for ts_code in symbols {
        // Extract bare code: "600519.SH" → "600519"
        let bare = ts_code.split('.').next().unwrap_or(ts_code);

        let url = format!("{}/stock_news?symbol={}", BRIDGE_BASE, bare);
        let resp = client.get(&url).send().await?;

        if !resp.status().is_success() {
            warn!(symbol = bare, "stock_news returned error, skipping");
            continue;
        }

        let rows: Vec<StockNewsRow> = resp.json().await?;
        for row in &rows {
            let title = match &row.title {
                Some(t) if !t.is_empty() => t,
                _ => continue,
            };
            let publish_time = row.publish_time.as_deref().unwrap_or_default();
            if publish_time.is_empty() {
                continue;
            }

            db.execute(
                "INSERT OR REPLACE INTO stock_news
                    (ts_code, publish_time, title, content, source, url)
                 VALUES (?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    ts_code,
                    publish_time,
                    title,
                    row.content.as_deref().unwrap_or_default(),
                    row.source.as_deref().unwrap_or_default(),
                    row.url.as_deref().unwrap_or_default(),
                ],
            )?;
            total += 1;
        }

        // Small delay between symbols to avoid hammering 东方财富
        sleep(Duration::from_millis(300)).await;
    }
    info!(
        rows = total,
        symbols = symbols.len(),
        "stock_news (个股新闻) fetched"
    );
    Ok(total)
}
