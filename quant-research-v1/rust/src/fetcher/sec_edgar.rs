/// SEC EDGAR 8-K filing fetcher.
/// Free, no API key — requires User-Agent header only.
/// Rate limit: 10 req/s (we do 3/s to be safe).
///
/// Flow:
///   1. CIK lookup: data.sec.gov/submissions/CIK{cik}.json
///   2. Filter recent 8-K filings within days_back
///   3. Store: symbol, cik, form_type, filed_date, items, filing_url
use anyhow::Result;
use duckdb::Connection;
use serde::Deserialize;
use tracing::{info, warn};
use chrono::Local;

const SUBMISSIONS_BASE: &str = "https://data.sec.gov/submissions";
const COMPANY_TICKERS: &str = "https://www.sec.gov/files/company_tickers.json";
const USER_AGENT: &str = "quant-research-v1 research@example.com";

// 8-K item descriptions — what each item means in plain English
fn item_description(item: &str) -> &'static str {
    match item {
        "1.01" => "Material Definitive Agreement",
        "1.02" => "Termination of Material Agreement",
        "1.03" => "Bankruptcy or Receivership",
        "1.05" => "Material Cybersecurity Incident",
        "2.01" => "Completion of Acquisition or Disposition",
        "2.02" => "Results of Operations (Earnings)",
        "2.03" => "Creation of Direct Financial Obligation",
        "2.06" => "Material Impairment",
        "3.01" => "Notice of Delisting",
        "4.01" => "Changes in Auditor",
        "5.01" => "Changes in Control",
        "5.02" => "Directors / Officers Changes",
        "5.03" => "Amendments to Bylaws",
        "7.01" => "Regulation FD Disclosure",
        "8.01" => "Other Events",
        "9.01" => "Financial Statements and Exhibits",
        _ => "Other",
    }
}

#[derive(Deserialize)]
struct TickerEntry {
    cik_str: u64,
    ticker: String,
}

#[derive(Deserialize)]
struct Submissions {
    cik: String,
    filings: FilingsWrapper,
}

#[derive(Deserialize)]
struct FilingsWrapper {
    recent: RecentFilings,
}

#[derive(Deserialize)]
struct RecentFilings {
    #[serde(rename = "accessionNumber", default)]
    accession_number: Vec<String>,
    #[serde(rename = "filingDate", default)]
    filing_date: Vec<String>,
    #[serde(default)]
    form: Vec<String>,
    #[serde(rename = "primaryDocument", default)]
    primary_document: Vec<String>,
    // items can have null entries — use Option to avoid deserialization failure
    #[serde(default)]
    items: Vec<Option<String>>,
}

pub async fn fetch_filings(
    con: &Connection,
    symbols: &[String],
    days_back: u32,
) -> Result<usize> {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;

    // Build ticker → CIK map
    let cik_map = fetch_cik_map(&client).await?;
    info!("sec_edgar cik_map loaded entries={}", cik_map.len());

    let cutoff = Local::now() - chrono::Duration::days(days_back as i64);
    let cutoff_date = cutoff.format("%Y-%m-%d").to_string();

    let mut total = 0usize;

    for sym in symbols {
        let cik = match cik_map.get(sym.to_uppercase().as_str()) {
            Some(c) => c,
            None => {
                warn!("sec_edgar no cik found symbol={}", sym);
                continue;
            }
        };

        let cik_padded = format!("{:010}", cik);
        let url = format!("{}/CIK{}.json", SUBMISSIONS_BASE, cik_padded);

        let resp = match super::http::send_with_retry(|| client.get(&url)).await {
            Ok(r) => r,
            Err(e) => {
                warn!("sec_edgar fetch error symbol={} err={}", sym, e);
                continue;
            }
        };

        let subs: Submissions = match resp.json().await {
            Ok(s) => s,
            Err(e) => {
                warn!("sec_edgar parse error symbol={} err={}", sym, e);
                continue;
            }
        };

        let recent = &subs.filings.recent;
        let n = recent.form.len();

        for i in 0..n {
            let form = &recent.form[i];
            // Only 8-K for now (material events) — could add 10-Q, 10-K
            if form != "8-K" {
                continue;
            }

            let filed = &recent.filing_date[i];
            // Only within days_back window
            if filed.as_str() < cutoff_date.as_str() {
                continue;
            }

            let accession_raw = &recent.accession_number[i];
            let accession_nodash = accession_raw.replace('-', "");
            // Correct SEC filing index URL:
            // https://www.sec.gov/Archives/edgar/data/{cik}/{nodash_folder}/{dashed}-index.htm
            // The folder uses no-dashes; the index filename uses the dashed accession number.
            let filing_url = format!(
                "https://www.sec.gov/Archives/edgar/data/{}/{}/{}-index.htm",
                subs.cik,
                accession_nodash,
                accession_raw,
            );

            // Items may have null entries — unwrap Option<String>
            let raw_items = recent.items.get(i)
                .and_then(|opt| opt.as_deref())
                .unwrap_or("");
            let items_parsed: Vec<String> = raw_items
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            // Build human-readable item descriptions
            let item_descs: Vec<String> = items_parsed.iter()
                .map(|item| format!("Item {} — {}", item, item_description(item)))
                .collect();

            let items_json = serde_json::to_string(&item_descs).unwrap_or_default();

            // Primary description = most significant item
            let description = item_descs.first().cloned().unwrap_or_else(|| "8-K filing".to_string());

            con.execute(
                "INSERT OR REPLACE INTO sec_filings
                    (symbol, cik, accession_number, form_type, filed_date, items, description, filing_url)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    sym,
                    cik_padded,
                    accession_raw,
                    form,
                    filed,
                    items_json,
                    description,
                    filing_url,
                ],
            )?;
            total += 1;
        }

        info!("sec_edgar symbol={} filings_stored_in_window={}", sym, total);

        // 3 req/s — well within 10 req/s limit
        tokio::time::sleep(tokio::time::Duration::from_millis(333)).await;
    }

    Ok(total)
}

async fn fetch_cik_map(client: &reqwest::Client) -> Result<std::collections::HashMap<String, u64>> {
    let resp = super::http::send_with_retry(|| client.get(COMPANY_TICKERS)).await?;
    // Response is {"0": {"cik_str": 320193, "ticker": "AAPL", ...}, "1": {...}, ...}
    let raw: std::collections::HashMap<String, TickerEntry> = resp.json().await?;
    let map = raw.into_values()
        .map(|e| (e.ticker.to_uppercase(), e.cik_str))
        .collect();
    Ok(map)
}
