use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use super::{fetch_and_store, query, ts_date_val};

/// Key index codes for A-share market benchmarks.
const INDEX_CODES: &[&str] = &[
    "000300.SH", // CSI 300 (沪深300)
    "000016.SH", // SSE 50 (上证50)
    "399006.SZ", // ChiNext (创业板指)
    "000905.SH", // CSI 500 (中证500)
    "000852.SH", // CSI 1000 (中证1000)
];

pub async fn fetch_daily_prices(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "daily",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        11,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO prices
                    (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    row[0].as_str().unwrap_or_default(),
                    ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                    row[10].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "daily prices fetched");
    Ok(total)
}

pub async fn fetch_daily_basic(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "daily_basic",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps_ttm,total_mv,circ_mv",
        10,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO daily_basic
                    (ts_code, trade_date, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps_ttm, total_mv, circ_mv)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    row[0].as_str().unwrap_or_default(),
                    ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "daily_basic fetched");
    Ok(total)
}

/// Fetch index daily prices (沪深300, 上证50, 创业板指, etc.) into the prices table.
/// Uses Tushare `index_daily` API which is separate from `daily` (individual stocks).
pub async fn fetch_index_daily(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let mut total = 0usize;

    for &idx_code in INDEX_CODES {
        let n = fetch_and_store(
            client, token, "index_daily",
            serde_json::json!({ "ts_code": idx_code, "start_date": &date, "end_date": &date }),
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
            11,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO prices
                        (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        row[0].as_str().unwrap_or_default(),
                        ts_date_val(&row[1]),
                        row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                        row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                        row[10].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await.unwrap_or_else(|e| {
            warn!(index = idx_code, error = %e, "index_daily fetch failed");
            0
        });
        total += n;
    }
    info!(rows = total, "index_daily prices fetched");
    Ok(total)
}

/// Backfill index prices over a date range.
pub async fn backfill_index(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize> {
    let start_str = start.format("%Y%m%d").to_string();
    let end_str = end.format("%Y%m%d").to_string();
    let mut total = 0usize;

    for &idx_code in INDEX_CODES {
        let n = fetch_and_store(
            client, token, "index_daily",
            serde_json::json!({
                "ts_code": idx_code,
                "start_date": &start_str,
                "end_date": &end_str,
            }),
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
            11,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO prices
                        (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        row[0].as_str().unwrap_or_default(),
                        ts_date_val(&row[1]),
                        row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                        row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                        row[10].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await.unwrap_or_else(|e| {
            warn!(index = idx_code, error = %e, "index backfill failed");
            0
        });
        total += n;
        info!(index = idx_code, rows = n, "index backfill done");
    }
    info!(rows = total, "all index backfill complete");
    Ok(total)
}

/// Backfill margin_detail (融资融券) by iterating trade dates.
pub async fn backfill_margin(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize> {
    let trade_dates = fetch_trade_calendar(client, token, start, end).await?;

    // Check existing
    let existing: std::collections::HashSet<String> = {
        let mut stmt = db.prepare(
            "SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM margin_detail
             WHERE trade_date >= ? AND trade_date <= ?",
        )?;
        let mut rows = stmt.query(duckdb::params![
            start.format("%Y-%m-%d").to_string(),
            end.format("%Y-%m-%d").to_string(),
        ])?;
        let mut set = std::collections::HashSet::new();
        while let Some(row) = rows.next()? {
            let d: String = row.get(0)?;
            set.insert(d);
        }
        set
    };

    let to_fetch: Vec<NaiveDate> = trade_dates
        .into_iter()
        .filter(|d| !existing.contains(&d.format("%Y-%m-%d").to_string()))
        .collect();

    info!(
        to_fetch = to_fetch.len(),
        already_have = existing.len(),
        "margin backfill plan"
    );

    let mut total = 0usize;
    let n = to_fetch.len();
    for (i, date) in to_fetch.iter().enumerate() {
        let n1 = super::flow::fetch_margin_detail(client, token, db, *date)
            .await
            .unwrap_or_else(|e| {
                warn!(date = %date, error = %e, "margin backfill failed");
                0
            });
        total += n1;

        if (i + 1) % 20 == 0 || i + 1 == n {
            info!(
                progress = format!("{}/{}", i + 1, n),
                rows = total,
                "margin backfill"
            );
        }
    }

    Ok(total)
}

// ── Historical backfill ────────────────────────────────────────────────────

/// Fetch trading calendar from Tushare and return sorted list of trading dates.
async fn fetch_trade_calendar(
    client: &reqwest::Client,
    token: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<NaiveDate>> {
    let rows = query(
        client,
        token,
        "trade_cal",
        serde_json::json!({
            "exchange": "SSE",
            "start_date": start.format("%Y%m%d").to_string(),
            "end_date": end.format("%Y%m%d").to_string(),
            "is_open": 1,
        }),
        "cal_date",
    )
    .await?;

    let mut dates: Vec<NaiveDate> = rows
        .iter()
        .filter_map(|r| {
            r.first()
                .and_then(|v| v.as_str())
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        })
        .collect();
    dates.sort();
    info!(trading_days = dates.len(), "trade calendar loaded");
    Ok(dates)
}

/// Backfill historical prices + daily_basic by iterating symbols with date ranges.
/// This is MUCH faster than iterating dates (~5 min for 332 symbols × 2yr)
/// because each API call returns ~484 rows (1 symbol × all dates) instead of
/// ~5350 rows (all symbols × 1 date).
pub async fn backfill_history(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize> {
    let start_str = start.format("%Y%m%d").to_string();
    let end_str = end.format("%Y%m%d").to_string();

    // Check which symbols already have sufficient data
    let mut already_done = std::collections::HashSet::new();
    {
        let mut stmt = db.prepare(
            "SELECT ts_code, COUNT(*) as n FROM prices
             WHERE trade_date >= ? AND trade_date <= ?
             GROUP BY ts_code HAVING n >= 200",
        )?;
        let mut rows = stmt.query(duckdb::params![
            start.format("%Y-%m-%d").to_string(),
            end.format("%Y-%m-%d").to_string(),
        ])?;
        while let Some(row) = rows.next()? {
            let code: String = row.get(0)?;
            already_done.insert(code);
        }
    }

    let to_fetch: Vec<&String> = symbols
        .iter()
        .filter(|s| !already_done.contains(s.as_str()))
        .collect();

    info!(
        total_symbols = symbols.len(),
        already_have = already_done.len(),
        to_fetch = to_fetch.len(),
        "backfill plan (by symbol)"
    );

    if to_fetch.is_empty() {
        info!("all symbols already have sufficient history");
        return Ok(0);
    }

    let mut grand_total = 0usize;
    let total = to_fetch.len();

    for (i, ts_code) in to_fetch.iter().enumerate() {
        // Fetch prices for this symbol over entire date range
        let n1 = fetch_and_store(
            client, token, "daily",
            serde_json::json!({
                "ts_code": ts_code,
                "start_date": &start_str,
                "end_date": &end_str,
            }),
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
            11,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO prices
                        (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        row[0].as_str().unwrap_or_default(),
                        ts_date_val(&row[1]),
                        row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                        row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                        row[10].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await.unwrap_or_else(|e| {
            warn!(ts_code = %ts_code, error = %e, "prices backfill failed");
            0
        });

        // Fetch daily_basic for this symbol
        let n2 = fetch_and_store(
            client, token, "daily_basic",
            serde_json::json!({
                "ts_code": ts_code,
                "start_date": &start_str,
                "end_date": &end_str,
            }),
            "ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps_ttm,total_mv,circ_mv",
            10,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO daily_basic
                        (ts_code, trade_date, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps_ttm, total_mv, circ_mv)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        row[0].as_str().unwrap_or_default(),
                        ts_date_val(&row[1]),
                        row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                        row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await.unwrap_or_else(|e| {
            warn!(ts_code = %ts_code, error = %e, "daily_basic backfill failed");
            0
        });

        grand_total += n1 + n2;

        // Progress log every 20 symbols
        if (i + 1) % 20 == 0 || i + 1 == total {
            let pct = ((i + 1) as f64 / total as f64 * 100.0) as u32;
            info!(
                progress = format!("{}/{} ({}%)", i + 1, total, pct),
                rows = grand_total,
                symbol = %ts_code,
                "backfill progress"
            );
        }
    }

    info!(
        rows = grand_total,
        symbols = total,
        "historical backfill complete"
    );
    Ok(grand_total)
}

/// Backfill moneyflow for historical dates (needed for flow score z-scores).
pub async fn backfill_moneyflow(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize> {
    let trade_dates = fetch_trade_calendar(client, token, start, end).await?;

    // Check existing
    let existing: std::collections::HashSet<String> = {
        let mut stmt = db.prepare(
            "SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM moneyflow
             WHERE trade_date >= ? AND trade_date <= ?",
        )?;
        let mut rows = stmt.query(duckdb::params![
            start.format("%Y-%m-%d").to_string(),
            end.format("%Y-%m-%d").to_string(),
        ])?;
        let mut set = std::collections::HashSet::new();
        while let Some(row) = rows.next()? {
            let d: String = row.get(0)?;
            set.insert(d);
        }
        set
    };

    let to_fetch: Vec<NaiveDate> = trade_dates
        .into_iter()
        .filter(|d| !existing.contains(&d.format("%Y-%m-%d").to_string()))
        .collect();

    info!(
        to_fetch = to_fetch.len(),
        already_have = existing.len(),
        "moneyflow backfill plan"
    );

    let mut total = 0usize;
    let n = to_fetch.len();
    for (i, date) in to_fetch.iter().enumerate() {
        let n1 = super::flow::fetch_moneyflow(client, token, db, *date)
            .await
            .unwrap_or_else(|e| {
                warn!(date = %date, error = %e, "moneyflow backfill failed");
                0
            });
        total += n1;

        if (i + 1) % 20 == 0 || i + 1 == n {
            info!(
                progress = format!("{}/{}", i + 1, n),
                rows = total,
                "moneyflow backfill"
            );
        }
    }

    Ok(total)
}
