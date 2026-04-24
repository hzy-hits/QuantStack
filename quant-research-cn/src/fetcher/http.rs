/// Shared HTTP retry helper — identical logic to US pipeline.
/// Retries on 429/503/500 and timeout/connect errors.
use anyhow::{anyhow, Result};
use reqwest::StatusCode;
use tokio::time::{sleep, Duration};
use tracing::warn;

const MAX_RETRIES: u32 = 3;

pub fn build_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()?)
}

pub async fn send_with_retry<F>(build: F) -> Result<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let mut attempt = 0u32;
    loop {
        let resp = match build().send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() || e.is_connect() => {
                if attempt >= MAX_RETRIES {
                    return Err(anyhow!(
                        "HTTP request failed after {} retries: {}",
                        MAX_RETRIES,
                        e
                    ));
                }
                let wait_secs = 1u64 << attempt;
                warn!(
                    "http network_error err={} attempt={}/{} retry_in={}s",
                    e,
                    attempt + 1,
                    MAX_RETRIES,
                    wait_secs
                );
                sleep(Duration::from_secs(wait_secs.min(60))).await;
                attempt += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let status = resp.status();

        if status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::SERVICE_UNAVAILABLE
            || status == StatusCode::INTERNAL_SERVER_ERROR
        {
            if attempt >= MAX_RETRIES {
                return Err(anyhow!("HTTP {} after {} retries", status, MAX_RETRIES));
            }
            let wait_secs = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(1u64 << attempt);
            warn!(
                "http rate_limited status={} wait_secs={} attempt={}/{}",
                status,
                wait_secs,
                attempt + 1,
                MAX_RETRIES
            );
            sleep(Duration::from_secs(wait_secs.min(60))).await;
            attempt += 1;
            continue;
        }

        if !status.is_success() {
            return Err(anyhow!("HTTP error status={}", status));
        }

        return Ok(resp);
    }
}
