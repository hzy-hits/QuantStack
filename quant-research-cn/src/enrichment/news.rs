/// News enrichment — async concurrent DeepSeek calls for structured extraction.
///
/// Input:  raw news headlines/summaries from DuckDB
/// Output: event_type, sentiment, relevance, key_metrics → stored back in DuckDB
///
/// Design principles:
/// - DeepSeek does EXTRACTION only (read text → output structured JSON)
/// - NEVER asks DeepSeek to reason, analyze, or predict
/// - All calls are async concurrent (tokio::Semaphore controls parallelism)
/// - Idempotent: already-enriched news is skipped
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::Settings;
use super::llm::DeepSeekClient;

const SYSTEM_PROMPT: &str = r#"你是金融新闻结构化标注工具。从新闻文本中抽取以下字段，输出JSON。
不要推理、分析或预测，只提取文本中明确存在的信息。

输出格式:
{
  "event_type": "earnings|m_and_a|regulatory|product|lawsuit|management|buyback|dividend|rating|macro|other",
  "sentiment": "positive|negative|neutral",
  "sentiment_confidence": 0.0-1.0,
  "relevance": 0.0-1.0,
  "key_entities": ["公司名", ...],
  "key_metrics": {"metric_name": value, ...},
  "summary_one_line": "一句话摘要"
}

规则:
- event_type: 从上面的闭集中选一个
- sentiment: 只看文本表面语气，不要推测市场反应
- relevance: 对该公司股价的信息量，0=无关 1=重大事件
- key_metrics: 只提取文本中明确出现的数字（营收、利润、增速等），没有就空对象
- summary_one_line: 不超过30字"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsEnrichment {
    pub event_type: String,
    pub sentiment: String,
    pub sentiment_confidence: f64,
    pub relevance: f64,
    pub key_entities: Vec<String>,
    pub key_metrics: serde_json::Value,
    pub summary_one_line: String,
}

struct RawNews {
    ts_code: String,
    headline: String,
    content: String,
    published_at: String,
}

/// Enrich all un-processed news from the last N days.
pub async fn enrich_news(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
) -> Result<usize> {
    if !cfg.enrichment.enabled || cfg.api.deepseek_key.is_empty() {
        info!("news enrichment disabled or no deepseek key");
        return Ok(0);
    }

    let client = DeepSeekClient::new(
        &cfg.api.deepseek_key,
        &cfg.enrichment.model,
        cfg.enrichment.concurrency,
    )?;

    // Ensure enrichment table exists
    db.execute_batch("
        CREATE TABLE IF NOT EXISTS news_enriched (
            ts_code              VARCHAR NOT NULL,
            published_at         VARCHAR NOT NULL,
            headline             VARCHAR,
            event_type           VARCHAR,
            sentiment            VARCHAR,
            sentiment_confidence DOUBLE,
            relevance            DOUBLE,
            key_entities         VARCHAR,
            key_metrics          VARCHAR,
            summary_one_line     VARCHAR,
            enriched_at          TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (ts_code, published_at, headline)
        );
    ")?;

    // Load raw news not yet enriched (last 7 days)
    let cutoff = (as_of - chrono::Duration::days(7)).to_string();
    let raw_news = load_unenriched_news(db, &cutoff)?;

    if raw_news.is_empty() {
        info!("no news to enrich");
        return Ok(0);
    }

    info!(count = raw_news.len(), "enriching news with DeepSeek");

    // Fire all requests concurrently (semaphore limits parallelism)
    let mut handles = Vec::new();
    for news in &raw_news {
        let client = client.clone();
        let user_msg = format!(
            "股票: {}\n标题: {}\n内容: {}",
            news.ts_code,
            news.headline,
            &news.content[..news.content.len().min(1000)], // truncate to save tokens
        );

        let ts_code = news.ts_code.clone();
        let headline = news.headline.clone();
        let published_at = news.published_at.clone();

        handles.push(tokio::spawn(async move {
            let result = client.complete(SYSTEM_PROMPT, &user_msg).await;
            (ts_code, headline, published_at, result)
        }));
    }

    // Collect results and write to DB
    let mut total = 0;
    for handle in handles {
        let (ts_code, headline, published_at, result) = handle.await?;
        match result {
            Ok(json_str) => {
                match serde_json::from_str::<NewsEnrichment>(&json_str) {
                    Ok(enrichment) => {
                        store_enrichment(db, &ts_code, &headline, &published_at, &enrichment)?;
                        total += 1;
                    }
                    Err(e) => {
                        warn!(
                            ts_code = ts_code,
                            err = %e,
                            raw = json_str,
                            "failed to parse DeepSeek response"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(ts_code = ts_code, err = %e, "DeepSeek call failed");
            }
        }
    }

    info!(enriched = total, skipped = raw_news.len() - total, "news enrichment complete");
    Ok(total)
}

fn load_unenriched_news(db: &Connection, cutoff: &str) -> Result<Vec<RawNews>> {
    // Query AKShare stock_news table (populated by bridge)
    let has_stock_news = db
        .prepare("SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_news'")
        .and_then(|mut s| s.query_map([], |_| Ok(())).map(|r| r.count()))
        .unwrap_or(0)
        > 0;

    if has_stock_news {
        let mut stmt = db.prepare(
            "SELECT n.ts_code, n.title, COALESCE(n.content, n.title), n.publish_time
             FROM stock_news n
             LEFT JOIN news_enriched e
               ON n.ts_code = e.ts_code AND n.publish_time = e.published_at AND n.title = e.headline
             WHERE n.publish_time >= ?
               AND e.ts_code IS NULL
             ORDER BY n.publish_time DESC
             LIMIT 200"
        )?;

        let items: Vec<RawNews> = stmt
            .query_map([cutoff], |row| {
                Ok(RawNews {
                    ts_code: row.get(0)?,
                    headline: row.get(1)?,
                    content: row.get(2)?,
                    published_at: row.get(3)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        return Ok(items);
    }

    // Fallback: enrich forecast (业绩预告) summaries if no stock_news
    let mut stmt = db.prepare(
        "SELECT f.ts_code,
                f.forecast_type || ': ' || COALESCE(f.summary, ''),
                COALESCE(f.summary, f.forecast_type),
                CAST(f.ann_date AS VARCHAR)
         FROM forecast f
         LEFT JOIN news_enriched e
           ON f.ts_code = e.ts_code AND CAST(f.ann_date AS VARCHAR) = e.published_at
         WHERE f.ann_date >= ?
           AND e.ts_code IS NULL
         LIMIT 100"
    )?;

    let items: Vec<RawNews> = stmt
        .query_map([cutoff], |row| {
            Ok(RawNews {
                ts_code: row.get(0)?,
                headline: row.get(1)?,
                content: row.get(2)?,
                published_at: row.get(3)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(items)
}

fn store_enrichment(
    db: &Connection,
    ts_code: &str,
    headline: &str,
    published_at: &str,
    e: &NewsEnrichment,
) -> Result<()> {
    db.execute(
        "INSERT OR REPLACE INTO news_enriched
            (ts_code, published_at, headline, event_type, sentiment,
             sentiment_confidence, relevance, key_entities, key_metrics, summary_one_line)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            ts_code,
            published_at,
            headline,
            e.event_type,
            e.sentiment,
            e.sentiment_confidence,
            e.relevance,
            serde_json::to_string(&e.key_entities).unwrap_or_default(),
            e.key_metrics.to_string(),
            e.summary_one_line,
        ],
    )?;
    Ok(())
}
