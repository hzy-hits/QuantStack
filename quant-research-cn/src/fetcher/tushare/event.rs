use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use tracing::info;

use super::{fetch_and_store, query, ts_date, ts_date_val, str_val};

pub async fn fetch_forecast(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let mut all = 0usize;
    for days_back in 0..7 {
        let date = (as_of - chrono::Duration::days(days_back)).format("%Y%m%d").to_string();
        let rows = query(
            client, token, "forecast",
            serde_json::json!({ "ann_date": &date }),
            "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,summary",
        ).await?;
        for row in &rows {
            if row.len() < 9 { continue; }
            db.execute(
                "INSERT OR REPLACE INTO forecast
                    (ts_code, ann_date, end_date, forecast_type, p_change_min, p_change_max, net_profit_min, net_profit_max, summary)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]), ts_date_val(&row[2]),
                    str_val(&row[3]),
                    row[4].as_f64(), row[5].as_f64(), row[6].as_f64(), row[7].as_f64(),
                    str_val(&row[8]),
                ],
            )?;
            all += 1;
        }
    }
    info!(rows = all, "forecast (业绩预告) fetched");
    Ok(all)
}

pub async fn fetch_block_trade(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "block_trade",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,price,vol,amount,buyer,seller",
        7,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO block_trade
                    (ts_code, trade_date, price, vol, amount, buyer, seller, premium)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(),
                    str_val(&row[5]), str_val(&row[6]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "block_trade (大宗交易) fetched");
    Ok(total)
}

pub async fn fetch_top_list(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "top_list",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,reason,buy,sell,net_rate,broker",
        7,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO top_list
                    (ts_code, trade_date, reason, buy_amount, sell_amount, net_amount, broker_name)
                 VALUES (?, ?, ?, ?, ?, NULL, ?)",
                duckdb::params![
                    str_val(&row[0]), ts_date_val(&row[1]), str_val(&row[2]),
                    row[3].as_f64(), row[4].as_f64(),
                    str_val(&row[6]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "top_list (龙虎榜) fetched");
    Ok(total)
}

pub async fn fetch_share_unlock(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let start = as_of.format("%Y%m%d").to_string();
    let end = (as_of + chrono::Duration::days(60)).format("%Y%m%d").to_string();

    let total = fetch_and_store(
        client, token, "share_float",
        serde_json::json!({ "start_date": &start, "end_date": &end }),
        "ts_code,ann_date,float_date,float_share,float_ratio,holder_name,share_type",
        7,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO share_unlock
                    (ts_code, ann_date, float_date, float_share, float_ratio, holder_name, share_type)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]),
                    row[1].as_str().map(|s| ts_date(s)),
                    ts_date_val(&row[2]),
                    row[3].as_f64(), row[4].as_f64(),
                    str_val(&row[5]), str_val(&row[6]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "share_unlock (限售解禁) fetched");
    Ok(total)
}

pub async fn fetch_disclosure_date(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    // Get the latest reporting period end_date (quarter end)
    let end_date = latest_quarter_end(as_of);
    let total = fetch_and_store(
        client, token, "disclosure_date",
        serde_json::json!({ "end_date": end_date.format("%Y%m%d").to_string() }),
        "ts_code,ann_date,end_date,pre_date,actual_date,modify_date",
        6,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO disclosure_date
                    (ts_code, ann_date, end_date, pre_date, actual_date, modify_date)
                 VALUES (?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]),
                    row[1].as_str().map(|s| ts_date(s)),
                    ts_date_val(&row[2]),
                    row[3].as_str().map(|s| ts_date(s)),
                    row[4].as_str().map(|s| ts_date(s)),
                    row[5].as_str().map(|s| ts_date(s)),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "disclosure_date (财报日历) fetched");
    Ok(total)
}

pub async fn fetch_stk_holdertrade(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let mut all = 0usize;
    for days_back in 0..7 {
        let date = (as_of - chrono::Duration::days(days_back)).format("%Y%m%d").to_string();
        let total = fetch_and_store(
            client, token, "stk_holdertrade",
            serde_json::json!({ "ann_date": &date }),
            "ts_code,ann_date,holder_name,holder_type,in_de,change_vol,change_ratio,after_share,after_ratio",
            9,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO stk_holdertrade
                        (ts_code, ann_date, holder_name, holder_type, in_de, change_vol, change_ratio, after_share, after_ratio)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        str_val(&row[0]), ts_date_val(&row[1]),
                        str_val(&row[2]), str_val(&row[3]), str_val(&row[4]),
                        row[5].as_f64(), row[6].as_f64(), row[7].as_f64(), row[8].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "stk_holdertrade (股东增减持) fetched");
    Ok(all)
}

pub async fn fetch_pledge_detail(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    // Pledge detail — recent 7 days by ts_code is impractical; query by date not supported.
    // Use broad query — Tushare returns up to 5000 recent records.
    let total = fetch_and_store(
        client, token, "pledge_detail",
        serde_json::json!({}),
        "ts_code,ann_date,holder_name,pledge_amount,start_date,end_date,is_release",
        7,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO pledge_detail
                    (ts_code, ann_date, holder_name, pledge_amount, start_date, end_date, is_release)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    str_val(&row[0]),
                    row[1].as_str().map(|s| ts_date(s)),
                    str_val(&row[2]),
                    row[3].as_f64(),
                    row[4].as_str().map(|s| ts_date(s)),
                    row[5].as_str().map(|s| ts_date(s)),
                    str_val(&row[6]),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "pledge_detail (股权质押) fetched");
    Ok(total)
}

pub async fn fetch_repurchase(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let mut all = 0usize;
    for days_back in 0..7 {
        let date = (as_of - chrono::Duration::days(days_back)).format("%Y%m%d").to_string();
        let total = fetch_and_store(
            client, token, "repurchase",
            serde_json::json!({ "ann_date": &date }),
            "ts_code,ann_date,end_date,proc,exp_date,vol,amount",
            7,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO repurchase
                        (ts_code, ann_date, end_date, proc, exp_date, vol, amount)
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        str_val(&row[0]), ts_date_val(&row[1]),
                        row[2].as_str().map(|s| ts_date(s)),
                        str_val(&row[3]),
                        row[4].as_str().map(|s| ts_date(s)),
                        row[5].as_f64(), row[6].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "repurchase (回购) fetched");
    Ok(all)
}

pub async fn fetch_stk_holdernumber(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let mut all = 0usize;
    for days_back in 0..7 {
        let date = (as_of - chrono::Duration::days(days_back)).format("%Y%m%d").to_string();
        let total = fetch_and_store(
            client, token, "stk_holdernumber",
            serde_json::json!({ "ann_date": &date }),
            "ts_code,ann_date,end_date,holder_num",
            4,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO stk_holdernumber
                        (ts_code, ann_date, end_date, holder_num)
                     VALUES (?, ?, ?, ?)",
                    duckdb::params![
                        str_val(&row[0]), ts_date_val(&row[1]), ts_date_val(&row[2]),
                        row[3].as_f64(),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "stk_holdernumber (股东户数) fetched");
    Ok(all)
}

/// Find the latest quarter-end date before as_of.
fn latest_quarter_end(as_of: NaiveDate) -> NaiveDate {
    let (y, m) = (as_of.year(), as_of.month());
    let (qy, qm) = match m {
        1..=3 => (y - 1, 12),
        4..=6 => (y, 3),
        7..=9 => (y, 6),
        _ => (y, 9),
    };
    NaiveDate::from_ymd_opt(qy, qm, if qm == 12 { 31 } else if qm == 6 { 30 } else if qm == 9 { 30 } else { 31 }).unwrap()
}
