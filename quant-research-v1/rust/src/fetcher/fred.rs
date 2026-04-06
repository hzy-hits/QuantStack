use anyhow::Result;
use duckdb::Connection;
use serde::Deserialize;
use tracing::{info, warn};
use chrono::Local;

const BASE: &str = "https://api.stlouisfed.org/fred/series/observations";

// Human-readable labels for FRED series IDs
const SERIES: &[(&str, &str)] = &[
    ("FEDFUNDS",      "Fed Funds Rate"),
    ("DGS10",         "10Y Treasury Yield"),
    ("BAMLH0A0HYM2",  "HY Credit Spread"),
    ("VIXCLS",        "VIX (Fear Index)"),
    ("T10Y2Y",        "10Y-2Y Yield Spread"),
    ("UNRATE",        "Unemployment Rate"),
    ("CPIAUCSL",      "CPI YoY Inflation Rate (%)"),
];

#[derive(Deserialize)]
struct FredResponse {
    observations: Vec<Observation>,
}

#[derive(Deserialize)]
struct Observation {
    date: String,
    value: String,
}

fn store_cpi_yoy(
    con: &Connection,
    observations: &[Observation],
    series_id: &str,
    series_name: &str,
    min_store_date: &str,
) -> Result<usize> {
    let mut parsed = Vec::with_capacity(observations.len());
    for obs in observations {
        // FRED returns "." for missing values
        let val: f64 = match obs.value.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        parsed.push((obs.date.clone(), val));
    }

    let mut stored = 0usize;
    for idx in 12..parsed.len() {
        let (date, current) = (&parsed[idx].0, parsed[idx].1);
        if date.as_str() < min_store_date {
            continue;
        }

        let prior = parsed[idx - 12].1;
        if prior == 0.0 {
            continue;
        }

        let yoy = (current / prior - 1.0) * 100.0;
        con.execute(
            "INSERT OR REPLACE INTO macro_daily (date, series_id, series_name, value)
             VALUES (?, ?, ?, ?)",
            duckdb::params![date, series_id, series_name, yoy],
        )?;
        stored += 1;
    }

    Ok(stored)
}

pub async fn fetch_macro(
    con: &Connection,
    api_key: &str,
    init: bool,
) -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let today = Local::now().date_naive();
    let start_date = if init {
        today - chrono::Duration::days(730)
    } else {
        today - chrono::Duration::days(90)
    };
    let today_str = today.format("%Y-%m-%d").to_string();
    let start = start_date.format("%Y-%m-%d").to_string();

    let mut total = 0usize;

    for (series_id, series_name) in SERIES {
        let request_start = if *series_id == "CPIAUCSL" {
            (start_date - chrono::Duration::days(400)).format("%Y-%m-%d").to_string()
        } else {
            start.clone()
        };

        let resp = super::http::send_with_retry(|| {
            client.get(BASE).query(&[
                ("series_id",         *series_id),
                ("api_key",           api_key),
                ("file_type",         "json"),
                ("observation_start", request_start.as_str()),
                ("observation_end",   today_str.as_str()),
                ("sort_order",        "asc"),
            ])
        }).await?;

        let body: FredResponse = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                warn!("fred parse error series={} err={}", series_id, e);
                continue;
            }
        };

        let stored_rows = if *series_id == "CPIAUCSL" {
            store_cpi_yoy(con, &body.observations, series_id, series_name, &start)?
        } else {
            let mut stored = 0usize;
            for obs in &body.observations {
                // FRED returns "." for missing values
                let val: f64 = match obs.value.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                con.execute(
                    "INSERT OR REPLACE INTO macro_daily (date, series_id, series_name, value)
                     VALUES (?, ?, ?, ?)",
                    duckdb::params![obs.date, series_id, series_name, val],
                )?;
                stored += 1;
            }
            stored
        };
        total += stored_rows;

        info!(
            "fred series={} name='{}' fetched_rows={} stored_rows={}",
            series_id,
            series_name,
            body.observations.len(),
            stored_rows,
        );

        // FRED is generous but don't hammer it
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    Ok(total)
}
