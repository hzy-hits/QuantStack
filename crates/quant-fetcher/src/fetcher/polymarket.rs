/// Polymarket Gamma API — free, no API key required.
/// Fetches active prediction markets related to macro/finance events.
/// These give "crowd probability" for macro events like Fed rate cuts,
/// recession odds, CPI beats, etc. — useful context for Claude's reasoning.
use anyhow::Result;
use duckdb::Connection;
use serde::Deserialize;
use tracing::{info, warn};

const GAMMA_API: &str = "https://gamma-api.polymarket.com/markets";

// Tags/keywords to filter for finance-relevant markets
const FINANCE_KEYWORDS: &[&str] = &[
    "fed", "federal reserve", "rate cut", "rate hike", "fomc",
    "recession", "cpi", "inflation", "gdp", "unemployment",
    "interest rate", "treasury", "yield", "dollar", "oil", "gold",
    "bitcoin", "crypto", "s&p", "nasdaq", "dow",
];

// Exclusion keywords — reject markets matching these even if finance tags match
const EXCLUDE_KEYWORDS: &[&str] = &[
    "nba", "nfl", "nhl", "mlb", "soccer", "football", "basketball",
    "baseball", "hockey", "tennis", "ufc", "mma", "boxing",
    "warriors", "lakers", "celtics", "yankees", "super bowl",
    "world cup", "champions league", "premier league",
    "up or down", // short-term binary price bets (noise)
];

#[derive(Deserialize, Debug)]
struct Market {
    id: Option<String>,
    question: Option<String>,
    #[serde(rename = "groupItemTagId")]
    group_item_tag_id: Option<serde_json::Value>,
    outcomes: Option<serde_json::Value>,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<serde_json::Value>,
    volume: Option<serde_json::Value>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    #[serde(rename = "tags")]
    tags: Option<Vec<Tag>>,
}

#[derive(Deserialize, Debug)]
struct Tag {
    label: Option<String>,
    slug: Option<String>,
}

pub async fn fetch_markets(con: &Connection) -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let mut total = 0usize;
    let mut offset = 0usize;
    let limit = 100usize;

    loop {
        let limit_str = limit.to_string();
        let offset_str = offset.to_string();
        let resp = super::http::send_with_retry(|| {
            client.get(GAMMA_API).query(&[
                ("active",    "true"),
                ("closed",    "false"),
                ("limit",     limit_str.as_str()),
                ("offset",    offset_str.as_str()),
                ("order",     "volume"),
                ("ascending", "false"),
            ])
        }).await?;

        let markets: Vec<Market> = match resp.json().await {
            Ok(m) => m,
            Err(e) => {
                warn!("polymarket parse error err={}", e);
                break;
            }
        };

        if markets.is_empty() {
            break;
        }

        let mut found_finance = 0;
        for market in &markets {
            let question = match &market.question {
                Some(q) => q.clone(),
                None => continue,
            };
            let question_lower = question.to_lowercase();

            // Check tags first (more reliable than keyword matching in title),
            // then fall back to keyword scan of the question text
            let finance_tag_slugs = [
                "economics", "finance", "crypto", "commodities",
                "interest-rates", "federal-reserve", "inflation", "markets",
            ];
            let has_finance_tag = market.tags.as_ref().map(|tags| {
                tags.iter().any(|t| {
                    let slug  = t.slug.as_deref().unwrap_or("").to_lowercase();
                    let label = t.label.as_deref().unwrap_or("").to_lowercase();
                    finance_tag_slugs.iter().any(|s| slug.contains(s) || label.contains(s))
                        || FINANCE_KEYWORDS.iter().any(|kw| slug.contains(kw) || label.contains(kw))
                })
            }).unwrap_or(false);

            let is_finance = has_finance_tag
                || FINANCE_KEYWORDS.iter().any(|kw| question_lower.contains(kw));
            if !is_finance {
                continue;
            }

            // Reject sports and noise even if tagged as "markets"
            if EXCLUDE_KEYWORDS.iter().any(|kw| question_lower.contains(kw)) {
                continue;
            }

            let market_id = match &market.id {
                Some(id) => id.clone(),
                None => continue,
            };

            // Parse outcome prices paired with outcome labels (not by position)
            let (p_yes, p_no, raw_outcomes) =
                parse_outcome_prices(&market.outcomes, &market.outcome_prices);

            // Parse volume
            let volume_usd = parse_volume(&market.volume);

            // Parse end date (ISO format)
            let end_date = market.end_date.as_deref()
                .and_then(|d| d.split('T').next())
                .unwrap_or("")
                .to_string();

            // Category from tags
            let category = market.tags.as_ref()
                .and_then(|tags| tags.first())
                .and_then(|t| t.label.clone())
                .unwrap_or_else(|| "macro".to_string());

            con.execute(
                "INSERT OR REPLACE INTO polymarket_events
                    (market_id, fetch_date, question, category, p_yes, p_no, raw_outcomes, volume_usd, end_date)
                 VALUES (?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    market_id,
                    market.question.as_deref().unwrap_or(""),
                    category,
                    p_yes,
                    p_no,
                    raw_outcomes,
                    volume_usd,
                    end_date,
                ],
            )?;
            found_finance += 1;
            total += 1;
        }

        info!(
            "polymarket offset={} fetched={} finance_relevant={}",
            offset, markets.len(), found_finance
        );

        // If we got fewer than limit, we've exhausted results
        if markets.len() < limit {
            break;
        }

        // Stop after 500 markets to keep it fast — top by volume is most useful
        offset += limit;
        if offset >= 500 {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }

    info!("polymarket total finance markets stored={}", total);
    Ok(total)
}

fn parse_json_value_array(value: &Option<serde_json::Value>) -> Option<Vec<serde_json::Value>> {
    match value {
        Some(serde_json::Value::Array(arr)) => Some(arr.clone()),
        Some(serde_json::Value::String(s)) => serde_json::from_str::<Vec<serde_json::Value>>(s).ok(),
        _ => None,
    }
}

fn parse_json_string_array(value: &Option<serde_json::Value>) -> Option<Vec<String>> {
    match value {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        Some(serde_json::Value::String(s)) => serde_json::from_str::<Vec<String>>(s).ok(),
        _ => None,
    }
}

fn parse_price_value(value: &serde_json::Value) -> Option<f64> {
    if let Some(s) = value.as_str() { s.parse().ok() } else { value.as_f64() }
}

/// Parse outcome prices paired with labels so p_yes/p_no are correct for any binary market.
/// For non-binary or unlabeled markets returns (None, None) but still emits raw_outcomes JSON.
fn parse_outcome_prices(
    outcomes: &Option<serde_json::Value>,
    prices: &Option<serde_json::Value>,
) -> (Option<f64>, Option<f64>, Option<String>) {
    let parsed_outcomes = parse_json_string_array(outcomes);
    let parsed_prices   = parse_json_value_array(prices);

    let raw_outcomes = match (&parsed_outcomes, &parsed_prices) {
        (None, None) => None,
        _ => Some(serde_json::json!({
            "outcomes": &parsed_outcomes,
            "outcomePrices": &parsed_prices,
        }).to_string()),
    };

    let (labels, price_vals) = match (parsed_outcomes, parsed_prices) {
        (Some(l), Some(p)) if l.len() == p.len() => (l, p),
        _ => return (None, None, raw_outcomes),
    };

    // Only handle binary markets with an explicit "yes" label
    if labels.len() != 2 {
        return (None, None, raw_outcomes);
    }

    let yes_idx: Vec<usize> = labels.iter().enumerate()
        .filter_map(|(i, l)| l.to_ascii_lowercase().contains("yes").then_some(i))
        .collect();

    if yes_idx.len() != 1 {
        return (None, None, raw_outcomes);
    }

    let yi = yes_idx[0];
    let ni = 1 - yi;
    let p_yes = price_vals.get(yi).and_then(parse_price_value);
    let p_no  = price_vals.get(ni).and_then(parse_price_value);

    (p_yes, p_no, raw_outcomes)
}

fn parse_volume(vol: &Option<serde_json::Value>) -> Option<f64> {
    match vol {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}
