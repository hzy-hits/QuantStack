/// Concept board theme clustering via DeepSeek — structured extraction only.
///
/// Takes today's top-20 concept boards by pct_chg, sends to DeepSeek
/// to group into 2-3 investment themes. Single API call per day.
///
/// Output stored in `theme_clusters` table for rendering in macro payload.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde::Deserialize;
use tracing::{info, warn};

use super::llm::DeepSeekClient;
use crate::config::Settings;

const SYSTEM_PROMPT: &str = r#"你是A股概念板块主题归类工具。将下面的概念板块按投资主题分组。
不要推理、分析或预测。只按名称和相关性进行分组。

输出格式:
{"themes": [
  {
    "theme_name": "主题名称(≤10字)",
    "description": "一句话描述(≤30字)",
    "boards": ["板块1", "板块2", ...],
    "avg_pct_chg": 1.23
  }
]}

规则:
- 分2-3个主题
- 每个板块只归入一个主题
- avg_pct_chg: 该主题下板块的平均涨跌幅(你计算)
- 不要预测、不要给出投资建议
- 输出必须为合法json"#;

#[derive(Deserialize)]
struct ThemeClusterResult {
    themes: Vec<ThemeItem>,
}

#[derive(Deserialize)]
struct ThemeItem {
    theme_name: String,
    description: String,
    boards: Vec<String>,
    avg_pct_chg: f64,
}

/// Enrich concept boards with theme clustering.
pub async fn enrich_themes(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    if !cfg.enrichment.enabled || cfg.api.deepseek_key.is_empty() {
        info!("theme enrichment disabled or no deepseek key");
        return Ok(0);
    }

    let date_str = as_of.to_string();

    // Check if already enriched today (idempotent)
    let already = db
        .prepare("SELECT COUNT(*) FROM theme_clusters WHERE trade_date = CAST(? AS DATE)")
        .and_then(|mut s| {
            s.query_map(duckdb::params![&date_str], |row| row.get::<_, i64>(0))
                .map(|r| r.filter_map(|v| v.ok()).next().unwrap_or(0))
        })
        .unwrap_or(0);

    if already > 0 {
        info!(
            existing = already,
            "theme_clusters already enriched today, skipping"
        );
        return Ok(already as usize);
    }

    // Load top-20 concept boards by pct_chg
    let sql = "SELECT board_name, pct_chg, up_count, down_count, lead_stock, lead_pct
               FROM concept_board
               WHERE trade_date = (
                   SELECT MAX(trade_date) FROM concept_board
                   WHERE trade_date <= CAST(? AS DATE)
               )
               ORDER BY pct_chg DESC
               LIMIT 20";

    let boards: Vec<(String, f64, i32, i32, String, f64)> = match db.prepare(sql) {
        Ok(mut stmt) => stmt
            .query_map(duckdb::params![&date_str], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?.unwrap_or(0.0),
                    row.get::<_, Option<i32>>(2)?.unwrap_or(0),
                    row.get::<_, Option<i32>>(3)?.unwrap_or(0),
                    row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                    row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                ))
            })?
            .filter_map(|r| r.ok())
            .collect(),
        Err(e) => {
            warn!("concept_board table not available: {}", e);
            return Ok(0);
        }
    };

    if boards.is_empty() {
        info!("no concept board data to cluster");
        return Ok(0);
    }

    // Build user prompt
    let mut user_msg = String::from("以下是今日涨幅前20的概念板块：\n\n");
    for (i, (name, pct, up, down, lead, lead_pct)) in boards.iter().enumerate() {
        user_msg.push_str(&format!(
            "{}. {} 涨跌幅:{:+.2}% 上涨:{} 下跌:{} 领涨:{} {:+.2}%\n",
            i + 1,
            name,
            pct,
            up,
            down,
            lead,
            lead_pct,
        ));
    }

    let client = DeepSeekClient::new(
        &cfg.api.deepseek_key,
        &cfg.enrichment.model,
        cfg.enrichment.concurrency,
    )?;

    let json_str = client.complete(SYSTEM_PROMPT, &user_msg).await?;
    let result: ThemeClusterResult = match serde_json::from_str(&json_str) {
        Ok(r) => r,
        Err(e) => {
            warn!(err = %e, raw = json_str, "failed to parse theme clustering response");
            return Ok(0);
        }
    };

    // Ensure table exists
    db.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS theme_clusters (
            trade_date    DATE NOT NULL,
            theme_name    VARCHAR NOT NULL,
            description   VARCHAR,
            boards        VARCHAR,
            avg_pct_chg   DOUBLE,
            enriched_at   TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (trade_date, theme_name)
        );
    ",
    )?;

    let mut total = 0;
    for theme in &result.themes {
        let boards_json = serde_json::to_string(&theme.boards).unwrap_or_default();
        db.execute(
            "INSERT OR REPLACE INTO theme_clusters
                (trade_date, theme_name, description, boards, avg_pct_chg)
             VALUES (CAST(? AS DATE), ?, ?, ?, ?)",
            duckdb::params![
                &date_str,
                &theme.theme_name,
                &theme.description,
                &boards_json,
                theme.avg_pct_chg,
            ],
        )?;
        total += 1;
    }

    info!(themes = total, "theme_clusters enriched");
    Ok(total)
}
