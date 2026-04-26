use crate::alpha::{AlphaBulletin, BulletinItem};
use anyhow::Result;
use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyReportModel {
    pub as_of: String,
    pub market: String,
    pub session: String,
    pub alpha_bulletin: ReportAlphaBulletin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportAlphaBulletin {
    pub selected_policy: Option<String>,
    pub tactical_policy: Option<String>,
    pub evaluated_through: Option<String>,
    pub execution_alpha: Vec<BulletinItem>,
    pub tactical_alpha: Vec<BulletinItem>,
    pub options_alpha: Vec<BulletinItem>,
    pub recall_alpha: Vec<BulletinItem>,
    pub blocked_alpha: Vec<BulletinItem>,
}

pub fn build_report_model(
    bulletin: &AlphaBulletin,
    market: &str,
    session: &str,
) -> DailyReportModel {
    DailyReportModel {
        as_of: bulletin.as_of.clone(),
        market: market.to_string(),
        session: session.to_string(),
        alpha_bulletin: ReportAlphaBulletin {
            selected_policy: bulletin
                .selected_policies
                .get(market)
                .cloned()
                .unwrap_or(None),
            tactical_policy: bulletin
                .tactical_policies
                .get(market)
                .cloned()
                .unwrap_or(None),
            evaluated_through: bulletin.evaluated_through.get(market).cloned(),
            execution_alpha: filter_market(&bulletin.execution_alpha, market),
            tactical_alpha: filter_market(&bulletin.tactical_alpha, market),
            options_alpha: filter_market(&bulletin.options_alpha, market),
            recall_alpha: filter_market(&bulletin.recall_alpha, market),
            blocked_alpha: filter_market(&bulletin.blocked_alpha, market),
        },
    }
}

fn filter_market(items: &[BulletinItem], market: &str) -> Vec<BulletinItem> {
    items
        .iter()
        .filter(|item| item.market == market)
        .cloned()
        .collect()
}

pub fn write_models_from_history(
    history_db: &Path,
    as_of: &str,
    markets: &[String],
    session: &str,
    reports_dir: &Path,
) -> Result<usize> {
    if !history_db.exists() {
        anyhow::bail!("history db does not exist: {}", history_db.display());
    }
    fs::create_dir_all(reports_dir)?;
    let con = Connection::open(history_db)?;
    let mut stmt = con.prepare(
        "SELECT market, model_json
         FROM daily_report_model
         WHERE as_of = CAST(? AS DATE)
           AND session = ?",
    )?;
    let rows = stmt.query_map(params![as_of, session], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let wanted: std::collections::HashSet<String> = markets.iter().cloned().collect();
    let mut written = 0usize;
    for row in rows {
        let (market, model_json) = row?;
        if !wanted.is_empty() && !wanted.contains(&market) {
            continue;
        }
        fs::write(
            reports_dir.join(format!("{as_of}_report_model_{market}_{session}.json")),
            format!("{}\n", model_json.trim()),
        )?;
        written += 1;
    }
    Ok(written)
}
