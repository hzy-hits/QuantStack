use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate};
use duckdb::Connection;
use tracing::info;

use super::{fetch_and_store, query, str_val, ts_date_val};

fn yi_to_yuan(v: Option<f64>) -> Option<f64> {
    v.map(|x| x * 100_000_000.0)
}

pub async fn fetch_moneyflow(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "moneyflow",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount",
        20,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO moneyflow
                    (ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
                     buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
                     buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
                     buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
                     net_mf_vol, net_mf_amount)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    row[0].as_str().unwrap_or_default(),
                    ts_date_val(&row[1]),
                    row[2].as_f64(), row[3].as_f64(), row[4].as_f64(), row[5].as_f64(),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                    row[10].as_f64(), row[11].as_f64(), row[12].as_f64(), row[13].as_f64(),
                    row[14].as_f64(), row[15].as_f64(), row[16].as_f64(), row[17].as_f64(),
                    row[18].as_f64(), row[19].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "moneyflow (个股资金流向) fetched");
    Ok(total)
}

pub async fn fetch_industry_moneyflow_ths(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let fields = "trade_date,ts_code,industry,lead_stock,close,pct_change,company_num,pct_change_stock,close_price,net_buy_amount,net_sell_amount,net_amount";

    for offset in 0..7 {
        let d = as_of - Duration::days(offset);
        if d.weekday().number_from_monday() >= 6 {
            continue;
        }
        let date = d.format("%Y%m%d").to_string();
        let rows = query(
            client,
            token,
            "moneyflow_ind_ths",
            serde_json::json!({ "trade_date": &date }),
            fields,
        )
        .await?;
        if rows.is_empty() {
            continue;
        }

        let mut total = 0usize;
        for row in &rows {
            if row.len() < 12 {
                continue;
            }
            let trade_date = ts_date_val(&row[0]);
            let sector_name = str_val(&row[2]);
            if sector_name.is_empty() {
                continue;
            }

            let net = yi_to_yuan(row[11].as_f64());
            let buy = yi_to_yuan(row[9].as_f64());
            let sell = yi_to_yuan(row[10].as_f64());
            let net_pct = match (net, buy, sell) {
                (Some(n), Some(b), Some(s)) if (b + s).abs() > f64::EPSILON => {
                    Some(n / (b + s) * 100.0)
                }
                _ => None,
            };

            db.execute(
                "INSERT OR REPLACE INTO sector_fund_flow
                    (trade_date, sector_name, pct_chg, main_net_in, main_net_pct,
                     super_net_in, big_net_in, mid_net_in, small_net_in)
                 VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)",
                duckdb::params![trade_date, sector_name, row[5].as_f64(), net, net_pct],
            )?;
            total += 1;
        }

        info!(
            rows = total,
            trade_date = %date,
            "sector_fund_flow filled from Tushare moneyflow_ind_ths"
        );
        return Ok(total);
    }

    info!("sector_fund_flow Tushare fallback returned no recent rows");
    Ok(0)
}

pub async fn fetch_margin_detail(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client,
        token,
        "margin_detail",
        serde_json::json!({ "trade_date": &date }),
        "ts_code,trade_date,rzye,rzmre,rzche,rqye,rqmcl,rqchl",
        8,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO margin_detail
                    (ts_code, trade_date, rzye, rzmre, rzche, rqye, rqmcl, rqchl)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    row[0].as_str().unwrap_or_default(),
                    ts_date_val(&row[1]),
                    row[2].as_f64(),
                    row[3].as_f64(),
                    row[4].as_f64(),
                    row[5].as_f64(),
                    row[6].as_f64(),
                    row[7].as_f64(),
                ],
            )?;
            Ok(())
        },
    )
    .await?;
    info!(rows = total, "margin_detail (融资融券) fetched");
    Ok(total)
}

pub async fn fetch_hsgt_flow(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client,
        token,
        "moneyflow_hsgt",
        serde_json::json!({ "trade_date": &date }),
        "trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money",
        7,
        |row| {
            let hgt = row[3].as_f64(); // 沪股通
            let sgt = row[4].as_f64(); // 深股通
                                       // Prefer API's north_money; if null, compute from hgt + sgt
                                       // only when BOTH legs are present (partial sum would undercount)
            let north_total = row[5].as_f64().or_else(|| match (hgt, sgt) {
                (Some(h), Some(s)) => Some(h + s),
                _ => None,
            });

            // Store northbound total
            db.execute(
                "INSERT OR REPLACE INTO northbound_flow
                    (trade_date, buy_amount, sell_amount, net_amount, source)
                 VALUES (?, NULL, NULL, ?, 'total')",
                duckdb::params![ts_date_val(&row[0]), north_total],
            )?;
            // Store Shanghai connect
            db.execute(
                "INSERT OR REPLACE INTO northbound_flow
                    (trade_date, buy_amount, sell_amount, net_amount, source)
                 VALUES (?, NULL, NULL, ?, 'sh_connect')",
                duckdb::params![ts_date_val(&row[0]), hgt],
            )?;
            // Store Shenzhen connect
            db.execute(
                "INSERT OR REPLACE INTO northbound_flow
                    (trade_date, buy_amount, sell_amount, net_amount, source)
                 VALUES (?, NULL, NULL, ?, 'sz_connect')",
                duckdb::params![ts_date_val(&row[0]), sgt],
            )?;
            Ok(())
        },
    )
    .await?;
    info!(
        rows = total * 3,
        "northbound_flow (北向资金) fetched via Tushare"
    );
    Ok(total)
}

pub async fn fetch_hsgt_top10(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client, token, "hsgt_top10",
        serde_json::json!({ "trade_date": &date }),
        "trade_date,ts_code,name,close,rank,market_type,amount,net_amount,buy,sell",
        10,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO hsgt_top10
                    (trade_date, ts_code, name, close, rank, market_type, amount, net_amount, buy, sell)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    ts_date_val(&row[0]),
                    str_val(&row[1]), str_val(&row[2]),
                    row[3].as_f64(), row[4].as_i64(),
                    str_val(&row[5]),
                    row[6].as_f64(), row[7].as_f64(), row[8].as_f64(), row[9].as_f64(),
                ],
            )?;
            Ok(())
        },
    ).await?;
    info!(rows = total, "hsgt_top10 (北向十大成交股) fetched");
    Ok(total)
}

pub async fn fetch_hk_hold(
    client: &reqwest::Client,
    token: &str,
    db: &Connection,
    as_of: NaiveDate,
) -> Result<usize> {
    let date = as_of.format("%Y%m%d").to_string();
    let total = fetch_and_store(
        client,
        token,
        "hk_hold",
        serde_json::json!({ "trade_date": &date }),
        "trade_date,ts_code,name,vol,ratio,exchange",
        6,
        |row| {
            db.execute(
                "INSERT OR REPLACE INTO hk_hold
                    (trade_date, ts_code, name, vol, ratio, exchange)
                 VALUES (?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    ts_date_val(&row[0]),
                    str_val(&row[1]),
                    str_val(&row[2]),
                    row[3].as_f64(),
                    row[4].as_f64(),
                    str_val(&row[5]),
                ],
            )?;
            Ok(())
        },
    )
    .await?;
    info!(rows = total, "hk_hold (陆股通持股) fetched");
    Ok(total)
}
