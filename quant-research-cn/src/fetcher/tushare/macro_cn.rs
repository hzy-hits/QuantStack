/// Macro data fetcher — Shibor, LPR, CPI, PPI, PMI, 社融
///
/// Three APIs:
/// - `shibor`     — daily interbank rates (ON, 1W, 2W, 1M, 3M, 6M, 9M, 1Y)
/// - `shibor_lpr` — monthly LPR (1Y, 5Y)
/// - `cn_m`       — monthly macro indicators (CPI, PPI, PMI, 社融)
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

use super::{fetch_and_store, ts_date_val, str_val};

/// Fetch daily Shibor rates and store as macro_cn rows.
/// Each tenor becomes a separate series_id: SHIBOR_ON, SHIBOR_1W, etc.
pub async fn fetch_shibor(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let start = (as_of - chrono::Duration::days(5)).format("%Y%m%d").to_string();
    let end = as_of.format("%Y%m%d").to_string();

    let tenors = ["on", "1w", "2w", "1m", "3m", "6m", "9m", "1y"];
    let fields_str = format!("date,{}", tenors.join(","));

    let mut all = 0usize;
    let total = fetch_and_store(
        client, token, "shibor",
        serde_json::json!({ "start_date": &start, "end_date": &end }),
        &fields_str,
        tenors.len() + 1, // date + N tenors
        |row| {
            let date = ts_date_val(&row[0]);
            for (i, tenor) in tenors.iter().enumerate() {
                if let Some(val) = row[i + 1].as_f64() {
                    let series_id = format!("SHIBOR_{}", tenor.to_uppercase());
                    let series_name = format!("Shibor {}", tenor);
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, ?, ?, ?)",
                        duckdb::params![date, &series_id, &series_name, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;
    let _ = total;
    info!(rows = all, "shibor (银行间拆放利率) fetched");
    Ok(all)
}

/// Fetch monthly LPR rate.
pub async fn fetch_lpr(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    // LPR updates monthly, look back 60 days to catch the latest
    let start = (as_of - chrono::Duration::days(60)).format("%Y%m%d").to_string();
    let end = as_of.format("%Y%m%d").to_string();

    let mut all = 0usize;
    let total = fetch_and_store(
        client, token, "shibor_lpr",
        serde_json::json!({ "start_date": &start, "end_date": &end }),
        "date,1y",
        2,
        |row| {
            let date = ts_date_val(&row[0]);
            if let Some(val) = row[1].as_f64() {
                db.execute(
                    "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                     VALUES (?, 'LPR_1Y', 'LPR 1年', ?)",
                    duckdb::params![date, val],
                )?;
                all += 1;
            }
            Ok(())
        },
    ).await?;
    let _ = total;
    info!(rows = all, "shibor_lpr (LPR) fetched");
    Ok(all)
}

/// Fetch key macro indicators from dedicated Tushare APIs.
/// - cn_cpi: CPI (nt_yoy = 全国同比%)
/// - cn_ppi: PPI (ppi_yoy = 工业品出厂价格同比%)
/// - cn_pmi: PMI (PMI010000 = 制造业PMI)
/// - cn_m:   Money supply (m2_yoy = M2同比%)
pub async fn fetch_macro_indicators(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    _series: &[(String, String)], // config series (kept for compatibility)
) -> Result<usize> {
    let mut all = 0usize;

    // ── CPI ──
    let _ = fetch_and_store(
        client, token, "cn_cpi",
        serde_json::json!({}),
        "month,nt_yoy",
        2,
        |row| {
            let month_str = str_val(&row[0]);
            if let Some(date_str) = month_to_date(&month_str) {
                if let Some(val) = row[1].as_f64() {
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, 'CPI_YOY', 'CPI 同比 (%)', ?)",
                        duckdb::params![&date_str, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;

    // ── PPI ──
    let _ = fetch_and_store(
        client, token, "cn_ppi",
        serde_json::json!({}),
        "month,ppi_yoy",
        2,
        |row| {
            let month_str = str_val(&row[0]);
            if let Some(date_str) = month_to_date(&month_str) {
                if let Some(val) = row[1].as_f64() {
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, 'PPI_YOY', 'PPI 同比 (%)', ?)",
                        duckdb::params![&date_str, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;

    // ── PMI (manufacturing) — field MONTH (uppercase!) + PMI010000 ──
    let _ = fetch_and_store(
        client, token, "cn_pmi",
        serde_json::json!({}),
        "MONTH,PMI010000",
        2,
        |row| {
            let month_str = str_val(&row[0]);
            if let Some(date_str) = month_to_date(&month_str) {
                if let Some(val) = row[1].as_f64() {
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, 'PMI_MFG', 'PMI 制造业', ?)",
                        duckdb::params![&date_str, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;

    // ── M2 money supply ──
    let _ = fetch_and_store(
        client, token, "cn_m",
        serde_json::json!({}),
        "month,m2_yoy",
        2,
        |row| {
            let month_str = str_val(&row[0]);
            if let Some(date_str) = month_to_date(&month_str) {
                if let Some(val) = row[1].as_f64() {
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, 'M2_YOY', 'M2 同比 (%)', ?)",
                        duckdb::params![&date_str, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;

    info!(rows = all, "macro indicators (CPI/PPI/PMI/M2) fetched");
    Ok(all)
}

/// Convert "202601" → "2026-01-01" (first of month)
fn month_to_date(month: &str) -> Option<String> {
    if month.len() >= 6 {
        let year = &month[..4];
        let mon = &month[4..6];
        Some(format!("{}-{}-01", year, mon))
    } else {
        None
    }
}

/// Backfill Shibor for a date range (for init).
pub async fn backfill_shibor(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<usize> {
    let start_str = start.format("%Y%m%d").to_string();
    let end_str = end.format("%Y%m%d").to_string();

    let tenors = ["on", "1w", "2w", "1m", "3m", "6m", "9m", "1y"];
    let fields_str = format!("date,{}", tenors.join(","));

    let mut all = 0usize;
    let total = fetch_and_store(
        client, token, "shibor",
        serde_json::json!({ "start_date": &start_str, "end_date": &end_str }),
        &fields_str,
        tenors.len() + 1,
        |row| {
            let date = ts_date_val(&row[0]);
            for (i, tenor) in tenors.iter().enumerate() {
                if let Some(val) = row[i + 1].as_f64() {
                    let series_id = format!("SHIBOR_{}", tenor.to_uppercase());
                    let series_name = format!("Shibor {}", tenor);
                    db.execute(
                        "INSERT OR REPLACE INTO macro_cn (date, series_id, series_name, value)
                         VALUES (?, ?, ?, ?)",
                        duckdb::params![date, &series_id, &series_name, val],
                    )?;
                    all += 1;
                }
            }
            Ok(())
        },
    ).await?;
    let _ = total;
    info!(rows = all, "shibor backfill complete");
    Ok(all)
}
