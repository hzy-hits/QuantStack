use anyhow::Result;
use duckdb::Connection;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use chrono::{Utc, Local, NaiveDate};

const BASE: &str = "https://finnhub.io/api/v1";
// Stay under 60 req/s — one request per ~18ms, conservatively 1 per 20ms
const RATE_DELAY_MS: u64 = 1100; // Finnhub free: 60 req/min → 1 req per 1100ms ≈ 54 req/min

#[derive(Deserialize, Debug)]
struct NewsItem {
    headline: Option<String>,
    summary: Option<String>,
    source: Option<String>,
    url: Option<String>,
    datetime: Option<i64>,
}

#[derive(Deserialize, Debug)]
struct EarningsItem {
    period: Option<String>,
    estimate: Option<f64>,
    actual: Option<f64>,
    #[serde(rename = "surprisePercent")]
    surprise_percent: Option<f64>,
}

#[derive(Deserialize, Debug)]
struct EarningsCalendarResponse {
    #[serde(rename = "earningsCalendar")]
    earnings_calendar: Option<Vec<CalendarItem>>,
}

#[derive(Deserialize, Debug)]
struct CalendarItem {
    symbol: Option<String>,
    date: Option<String>,
    // Finnhub returns quarter as an integer (1-4), not a string.
    // Use serde_json::Value to avoid silent deserialization failure on type mismatch.
    quarter: Option<serde_json::Value>,
    year: Option<serde_json::Value>,
    #[serde(rename = "epsEstimate")]
    eps_estimate: Option<f64>,
}

fn parse_json_with_warning<T>(body: &str, context: &str) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    match serde_json::from_str(body) {
        Ok(parsed) => Some(parsed),
        Err(err) => {
            let body_prefix: String = body.chars().take(200).collect();
            warn!(
                "finnhub parse error context={} err={} body_prefix={:?}",
                context, err, body_prefix,
            );
            None
        }
    }
}

async fn get(client: &reqwest::Client, endpoint: &str, params: &[(&str, &str)]) -> Result<String> {
    let url = format!("{}{}", BASE, endpoint);
    let resp = super::http::send_with_retry(|| client.get(&url).query(params)).await?;
    sleep(Duration::from_millis(RATE_DELAY_MS)).await;
    Ok(resp.text().await?)
}

/// Fetch company news for each symbol from [now - days_back .. now].
/// Stores into news_items table. Returns total rows inserted.
pub async fn fetch_news(
    con: &Connection,
    symbols: &[String],
    api_key: &str,
    days_back: u32,
) -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let to = Local::now().format("%Y-%m-%d").to_string();
    let from = (Local::now() - chrono::Duration::days(days_back as i64))
        .format("%Y-%m-%d")
        .to_string();

    let mut total = 0usize;

    // Market-wide general news (one call, not per symbol)
    {
        let body = get(&client, "/news", &[
            ("category", "general"),
            ("token", api_key),
        ]).await?;
        let items: Vec<NewsItem> =
            parse_json_with_warning(&body, "general news").unwrap_or_default();
        total += upsert_news(con, None, &items)?;
        info!("finnhub general news rows={}", items.len());
    }

    // Per-symbol company news
    for sym in symbols {
        let body = get(&client, "/company-news", &[
            ("symbol", sym.as_str()),
            ("from", from.as_str()),
            ("to", to.as_str()),
            ("token", api_key),
        ]).await?;

        let context = format!("company news symbol={}", sym);
        let items: Vec<NewsItem> =
            parse_json_with_warning(&body, &context).unwrap_or_default();
        let n = upsert_news(con, Some(sym), &items)?;
        total += n;
        info!("finnhub company news symbol={} rows={}", sym, n);
    }

    Ok(total)
}

fn upsert_news(con: &Connection, symbol: Option<&str>, items: &[NewsItem]) -> Result<usize> {
    let mut count = 0;
    for item in items {
        let url = match &item.url {
            Some(u) if !u.is_empty() => u.clone(),
            _ => continue,
        };
        let headline = item.headline.as_deref().unwrap_or("").to_string();
        if headline.is_empty() {
            continue;
        }

        let published_at = item.datetime.map(|ts| {
            chrono::DateTime::<Utc>::from_timestamp(ts, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_default()
        }).unwrap_or_default();

        let sym_val = symbol.unwrap_or("MARKET");

        con.execute(
            "INSERT OR REPLACE INTO news_items
                (symbol, headline, summary, source, url, published_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![
                sym_val,
                headline,
                item.summary.as_deref().unwrap_or(""),
                item.source.as_deref().unwrap_or(""),
                url,
                published_at,
            ],
        )?;
        count += 1;
    }
    Ok(count)
}

/// Fetch S&P 500 and Nasdaq 100 historical constituent changes from Finnhub.
/// Stores add/remove events into index_changes table.
pub async fn fetch_index_changes(
    con: &Connection,
    api_key: &str,
) -> Result<usize> {
    #[derive(Deserialize, Debug)]
    struct ConstituentDiff {
        symbol: Option<String>,
        #[serde(rename = "type")]
        change_type: Option<String>,
        date: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    struct IndexChangesResponse {
        #[serde(rename = "constituentDiffs")]
        constituent_diffs: Option<Vec<ConstituentDiff>>,
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let indices = ["^GSPC", "^NDX"];
    let mut total = 0usize;

    for index in &indices {
        let body = get(&client, "/index/historical-constituents", &[
            ("symbol", index),
            ("token", api_key),
        ]).await?;

        let context = format!("index changes index={}", index);
        let diffs = parse_json_with_warning::<IndexChangesResponse>(&body, &context)
            .and_then(|r| r.constituent_diffs)
            .unwrap_or_default();

        for diff in &diffs {
            let sym = match &diff.symbol {
                Some(s) if !s.is_empty() => s.clone(),
                _ => continue,
            };
            let change_type = match &diff.change_type {
                Some(t) => t.clone(),
                None => continue,
            };
            let change_date = match &diff.date {
                Some(d) => d.clone(),
                None => continue,
            };
            con.execute(
                "INSERT OR REPLACE INTO index_changes
                    (index_symbol, symbol, change_type, change_date)
                 VALUES (?, ?, ?, ?)",
                duckdb::params![index, sym, change_type, change_date],
            )?;
            total += 1;
        }
        info!("finnhub index changes index={} rows={}", index, diffs.len());
    }

    Ok(total)
}

/// Fetch historical EPS + upcoming earnings calendar from Finnhub.
pub async fn fetch_earnings(
    con: &Connection,
    symbols: &[String],
    api_key: &str,
    init: bool,
) -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let mut total = 0usize;

    // Historical EPS (init only — Finnhub /stock/earnings)
    if init {
        for sym in symbols {
            let body = get(&client, "/stock/earnings", &[
                ("symbol", sym.as_str()),
                ("limit", "20"),
                ("token", api_key),
            ]).await?;

            let context = format!("historical earnings symbol={}", sym);
            let items: Vec<EarningsItem> =
                parse_json_with_warning(&body, &context).unwrap_or_default();
            for item in &items {
                let period = match &item.period {
                    Some(p) => p.clone(),
                    None => continue,
                };
                // Parse period as date (format: "2024-12-31")
                let report_date = match NaiveDate::parse_from_str(&period, "%Y-%m-%d") {
                    Ok(d) => d.to_string(),
                    Err(_) => continue,
                };

                // Only store rows with actual EPS (historical, not upcoming)
                if item.actual.is_none() {
                    continue;
                }

                // Derive fiscal year and quarter from the period end date
                let fiscal_year = period.get(0..4).and_then(|y| y.parse::<i32>().ok());
                let fiscal_quarter = period
                    .get(5..7)
                    .and_then(|m| m.parse::<u32>().ok())
                    .filter(|month| (1..=12).contains(month))
                    .map(|month| ((month - 1) / 3 + 1) as i32);

                con.execute(
                    "INSERT OR REPLACE INTO earnings_calendar
                        (symbol, report_date, fiscal_period, fiscal_year, fiscal_quarter, estimate_eps, actual_eps, surprise_pct)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        sym,
                        report_date,
                        period,
                        fiscal_year,
                        fiscal_quarter,
                        item.estimate,
                        item.actual,
                        item.surprise_percent,
                    ],
                )?;
                total += 1;
            }
            info!("finnhub earnings history symbol={} rows={}", sym, items.len());
        }
    }

    // Upcoming earnings calendar (always run)
    {
        let today = Local::now().format("%Y-%m-%d").to_string();
        let ahead = (Local::now() + chrono::Duration::days(14))
            .format("%Y-%m-%d")
            .to_string();

        let body = get(&client, "/calendar/earnings", &[
            ("from", today.as_str()),
            ("to", ahead.as_str()),
            ("token", api_key),
        ]).await?;

        let cal: EarningsCalendarResponse =
            parse_json_with_warning(&body, "earnings calendar")
                .unwrap_or(EarningsCalendarResponse { earnings_calendar: None });

        for item in cal.earnings_calendar.unwrap_or_default() {
            let sym = match &item.symbol {
                Some(s) => s.clone(),
                None => continue,
            };
            let date = match &item.date {
                Some(d) => d.clone(),
                None => continue,
            };
            let fiscal_quarter = item.quarter.as_ref()
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .map(|n| n as i32);

            let fiscal_year = item.year.as_ref()
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .map(|n| n as i32);

            let fiscal_period = match (fiscal_quarter, fiscal_year) {
                (Some(q), Some(y)) => format!("Q{} {}", q, y),
                _ => String::new(),
            };

            con.execute(
                "INSERT OR REPLACE INTO earnings_calendar
                    (symbol, report_date, fiscal_period, fiscal_year, fiscal_quarter, estimate_eps, actual_eps, surprise_pct)
                 VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)",
                duckdb::params![
                    sym,
                    date,
                    fiscal_period,
                    fiscal_year,
                    fiscal_quarter,
                    item.eps_estimate,
                ],
            )?;
            total += 1;
        }
        info!("finnhub earnings calendar fetched");
    }

    Ok(total)
}
