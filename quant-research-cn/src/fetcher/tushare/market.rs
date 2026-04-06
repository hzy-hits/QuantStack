use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

use super::{fetch_and_store, ts_date_val, str_val};

pub async fn fetch_opt_daily(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let mut all = 0usize;

    // Fetch SSE + SZSE equity options
    for exchange in &["SSE", "SZSE"] {
        let total = fetch_and_store(
            client, token, "opt_daily",
            serde_json::json!({ "trade_date": &date, "exchange": exchange }),
            "ts_code,trade_date,exchange,pre_settle,pre_close,open,high,low,close,settle,vol,amount,oi",
            13,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO opt_daily
                        (ts_code, trade_date, exchange, pre_settle, pre_close, open, high, low, close, settle, vol, amount, oi)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        str_val(&row[0]), ts_date_val(&row[1]), str_val(&row[2]),
                        row[3].as_f64(), row[4].as_f64(), row[5].as_f64(), row[6].as_f64(),
                        row[7].as_f64(), row[8].as_f64(), row[9].as_f64(), row[10].as_f64(),
                        row[11].as_f64(), row[12].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "opt_daily (期权日线) fetched");
    Ok(all)
}

/// Fetch option contract metadata (strike, expiry, call/put type).
/// This is a reference table that changes infrequently — refresh weekly.
pub async fn fetch_opt_basic(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
) -> Result<usize> {
    let mut all = 0usize;

    for exchange in &["SSE", "SZSE"] {
        let total = fetch_and_store(
            client, token, "opt_basic",
            serde_json::json!({ "exchange": exchange }),
            "ts_code,name,call_put,exercise_price,maturity_date,list_date,delist_date,opt_code,per_unit,exercise_type",
            10,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO opt_basic
                        (ts_code, name, call_put, exercise_price, maturity_date, list_date, delist_date, opt_code, per_unit, exercise_type)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        str_val(&row[0]), str_val(&row[1]), str_val(&row[2]),
                        row[3].as_f64(), ts_date_val(&row[4]), ts_date_val(&row[5]),
                        ts_date_val(&row[6]), str_val(&row[7]), row[8].as_f64(),
                        str_val(&row[9]),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "opt_basic (期权合约) fetched");
    Ok(all)
}

pub async fn fetch_sge_daily(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "sge_daily",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,close,open,high,low,price_avg,change,pct_change,vol,amount,oi",
        12,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO sge_daily
                    (ts_code, trade_date, close, open, high, low, price_avg, change, pct_change, vol, amount, oi)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                    row[10].as_f64(), row[11].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "sge_daily (黄金现货) fetched");
    Ok(total)
}

pub async fn fetch_cb_daily(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "cb_daily",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,close,open,high,low,vol,amount,cb_value,cb_over_rate",
        10,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO cb_daily
                    (ts_code, trade_date, close, open, high, low, vol, amount, cb_value, cb_over_rate)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "cb_daily (可转债) fetched");
    Ok(total)
}

pub async fn fetch_fut_daily(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "fut_daily",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,open,high,low,close,settle,vol,amount,oi",
        10,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO fut_daily
                    (ts_code, trade_date, open, high, low, close, settle, vol, amount, oi)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "fut_daily (期货日线) fetched");
    Ok(total)
}

pub async fn fetch_fut_holding(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "fut_holding",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,broker,vol,vol_chg,long_hld,long_chg,short_hld,short_chg",
        9,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO fut_holding
                    (ts_code, trade_date, broker, vol, vol_chg, long_hld, long_chg, short_hld, short_chg)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]), str_val(&row[2]),
                    row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "fut_holding (期货持仓排名) fetched");
    Ok(total)
}
