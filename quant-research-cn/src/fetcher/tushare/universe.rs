use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use tracing::info;

use crate::config::Settings;
use super::{fetch_and_store, query, ts_date_val, str_val};

pub async fn fetch_universe(
    client: &reqwest::Client,
    token: &str,
    cfg: &Settings,
) -> Result<Vec<String>> {
    let mut symbols = Vec::new();

    let indices = [
        (cfg.universe.scan.csi300, "399300.SZ"),
        (cfg.universe.scan.csi500, "000905.SH"),
        (cfg.universe.scan.csi1000, "000852.SH"),
        (cfg.universe.scan.sse50, "000016.SH"),
    ];

    for (enabled, index_code) in &indices {
        if !*enabled {
            continue;
        }
        let rows = query(
            client, token, "index_weight",
            serde_json::json!({ "index_code": index_code }),
            "con_code",
        ).await?;

        for row in &rows {
            if let Some(code) = row.first().and_then(|v| v.as_str()) {
                symbols.push(code.to_string());
            }
        }
        info!(index = index_code, constituents = rows.len(), "index loaded");
    }

    // Add watchlist
    for sym in &cfg.universe.watchlist {
        if !symbols.contains(sym) {
            symbols.push(sym.clone());
        }
    }

    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

/// Fetch stock_basic: names, industry, market for all listed stocks.
/// This is a reference table — no date parameter needed.
pub async fn fetch_stock_basic(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
) -> Result<usize> {
    let total = fetch_and_store(
        client, token, "stock_basic",
        serde_json::json!({ "list_status": "L" }),
        "ts_code,symbol,name,area,industry,market,list_date,list_status",
        8,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO stock_basic
                    (ts_code, symbol, name, area, industry, market, list_date, list_status)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), str_val(&row[1]), str_val(&row[2]),
                    str_val(&row[3]), str_val(&row[4]), str_val(&row[5]),
                    str_val(&row[6]), str_val(&row[7]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "stock_basic (股票名称/行业) fetched");
    Ok(total)
}

pub async fn fetch_industry_classify(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
) -> Result<usize> {
    // Shenwan L1 industry classification
    let total = fetch_and_store(
        client, token, "index_classify",
        serde_json::json!({ "level": "L1", "src": "SW2021" }),
        "index_code,industry_name,level,is_pub,parent_code",
        5,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO industry_classify
                    (index_code, industry_name, level, is_pub)
                 VALUES (?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), str_val(&row[1]), str_val(&row[2]), str_val(&row[3]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "industry_classify (申万行业) fetched");
    Ok(total)
}

pub async fn fetch_fund_portfolio(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    // Fund holdings are quarterly — find latest quarter end
    let (y, m) = (as_of.year(), as_of.month());
    let (qy, qm) = match m {
        1..=3 => (y - 1, 12),
        4..=6 => (y, 3),
        7..=9 => (y, 6),
        _ => (y, 9),
    };
    let end_date = NaiveDate::from_ymd_opt(qy, qm, if qm == 12 { 31 } else if qm == 6 { 30 } else { 30 }).unwrap();
    let end_str = end_date.format("%Y%m%d").to_string();

    let total = fetch_and_store(
        client, token, "fund_portfolio",
        serde_json::json!({ "end_date": &end_str }),
        "ts_code,ann_date,end_date,symbol,mkv,amount,stk_mkv_ratio",
        7,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO fund_portfolio
                    (ts_code, ann_date, end_date, symbol, mkv, amount, stk_mkv_ratio)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]), ts_date_val(&row[2]),
                    str_val(&row[3]),
                    row[4].as_f64(), row[5].as_f64(), row[6].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, end_date = %end_date, "fund_portfolio (公募持仓) fetched");
    Ok(total)
}
