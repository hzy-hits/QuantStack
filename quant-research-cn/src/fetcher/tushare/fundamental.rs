use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use tracing::info;

use super::{fetch_and_store, query, ts_date, ts_date_val};

/// Financial statement endpoints require `period` (end_date) param, not `ann_date`.
/// We query the latest reporting period.
fn latest_period(as_of: NaiveDate) -> String {
    let (y, m) = (as_of.year(), as_of.month());
    let (qy, qm) = match m {
        1..=3 => (y - 1, 12),
        4..=6 => (y, 3),
        7..=9 => (y, 6),
        _ => (y, 9),
    };
    let qd = if qm == 12 {
        31
    } else if qm == 6 {
        30
    } else if qm == 9 {
        30
    } else {
        31
    };
    NaiveDate::from_ymd_opt(qy, qm, qd)
        .unwrap()
        .format("%Y%m%d")
        .to_string()
}

pub async fn fetch_income(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let period = latest_period(as_of);
    let rows = query(
        client,
        token,
        "income",
        serde_json::json!({ "period": &period }),
        "ts_code,ann_date,end_date,revenue,n_income,basic_eps,diluted_eps",
    )
    .await?;
    let mut all = 0usize;
    for row in &rows {
        if row.len() < 7 {
            continue;
        }
        db.execute(
            "INSERT OR REPLACE INTO income
                (ts_code, ann_date, end_date, revenue, n_income, basic_eps, diluted_eps)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row[0].as_str().unwrap_or_default(),
                row[1].as_str().map(|s| ts_date(s)),
                ts_date_val(&row[2]),
                row[3].as_f64(),
                row[4].as_f64(),
                row[5].as_f64(),
                row[6].as_f64(),
            ],
        )?;
        all += 1;
    }
    info!(rows = all, period = %period, "income (利润表) fetched");
    Ok(all)
}

pub async fn fetch_balancesheet(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let period = latest_period(as_of);
    let rows = query(
        client,
        token,
        "balancesheet",
        serde_json::json!({ "period": &period }),
        "ts_code,ann_date,end_date,total_assets,total_liab,total_hldr_eqy_exc_min_int",
    )
    .await?;
    let mut all = 0usize;
    for row in &rows {
        if row.len() < 6 {
            continue;
        }
        db.execute(
            "INSERT OR REPLACE INTO balancesheet
                (ts_code, ann_date, end_date, total_assets, total_liab, total_hldr_eqy_exc_min_int)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row[0].as_str().unwrap_or_default(),
                row[1].as_str().map(|s| ts_date(s)),
                ts_date_val(&row[2]),
                row[3].as_f64(),
                row[4].as_f64(),
                row[5].as_f64(),
            ],
        )?;
        all += 1;
    }
    info!(rows = all, "balancesheet (资产负债表) fetched");
    Ok(all)
}

pub async fn fetch_cashflow(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let period = latest_period(as_of);
    let rows = query(
        client,
        token,
        "cashflow",
        serde_json::json!({ "period": &period }),
        "ts_code,ann_date,end_date,n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act",
    )
    .await?;
    let mut all = 0usize;
    for row in &rows {
        if row.len() < 6 {
            continue;
        }
        db.execute(
            "INSERT OR REPLACE INTO cashflow
                (ts_code, ann_date, end_date, n_cashflow_act, n_cashflow_inv_act, n_cash_flows_fnc_act)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row[0].as_str().unwrap_or_default(),
                row[1].as_str().map(|s| ts_date(s)),
                ts_date_val(&row[2]),
                row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
            ],
        )?;
        all += 1;
    }
    info!(rows = all, "cashflow (现金流量表) fetched");
    Ok(all)
}

pub async fn fetch_fina_indicator(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let period = latest_period(as_of);
    let rows = query(
        client, token, "fina_indicator",
        serde_json::json!({ "period": &period }),
        "ts_code,ann_date,end_date,roe,roa,debt_to_assets,current_ratio,quick_ratio,eps,bps,cfps,netprofit_yoy,or_yoy",
    ).await?;
    let mut all = 0usize;
    for row in &rows {
        if row.len() < 13 {
            continue;
        }
        db.execute(
            "INSERT OR REPLACE INTO fina_indicator
                (ts_code, ann_date, end_date, roe, roa, debt_to_assets, current_ratio, quick_ratio, eps, bps, cfps, netprofit_yoy, or_yoy)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                row[0].as_str().unwrap_or_default(),
                row[1].as_str().map(|s| ts_date(s)),
                ts_date_val(&row[2]),
                row[3].as_f64(), row[4].as_f64(), row[5].as_f64(), row[6].as_f64(),
                row[7].as_f64(), row[8].as_f64(), row[9].as_f64(), row[10].as_f64(),
                row[11].as_f64(), row[12].as_f64(),
            ],
        )?;
        all += 1;
    }
    info!(rows = all, "fina_indicator (财务指标) fetched");
    Ok(all)
}

pub async fn fetch_dividend(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let mut all = 0usize;
    for days_back in (0..30).step_by(10) {
        let date = (as_of - chrono::Duration::days(days_back))
            .format("%Y%m%d")
            .to_string();
        let total = fetch_and_store(
            client, token, "dividend",
            serde_json::json!({ "ann_date": &date }),
            "ts_code,end_date,ann_date,div_proc,stk_div,cash_div,cash_div_tax,record_date,ex_date,pay_date",
            10,
            |row| {
                db.execute(
                    "INSERT OR REPLACE INTO dividend
                        (ts_code, end_date, ann_date, div_proc, stk_div, cash_div, cash_div_tax, record_date, ex_date, pay_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        row[0].as_str().unwrap_or_default(),
                        ts_date_val(&row[1]), ts_date_val(&row[2]),
                        row[3].as_str().unwrap_or_default(),
                        row[4].as_f64(), row[5].as_f64(), row[6].as_f64(),
                        row[7].as_str().map(|s| super::ts_date(s)),
                        row[8].as_str().map(|s| super::ts_date(s)),
                        row[9].as_str().map(|s| super::ts_date(s)),
                    ],
                )?;
                Ok(())
            },
        ).await?;
        all += total;
    }
    info!(rows = all, "dividend (分红送转) fetched");
    Ok(all)
}
