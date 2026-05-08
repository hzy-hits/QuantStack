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
    pub ev_status: Option<String>,
    pub selected_policy: Option<String>,
    pub tactical_policy: Option<String>,
    pub evaluated_through: Option<String>,
    pub execution_alpha: Vec<BulletinItem>,
    #[serde(default)]
    pub probation_alpha: Vec<BulletinItem>,
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
            ev_status: bulletin.ev_status.get(market).cloned(),
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
            probation_alpha: filter_market(&bulletin.probation_alpha, market),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpha::{AlphaBulletin, PolicyCandidate};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn item(market: &str, section: &str, symbol: &str) -> BulletinItem {
        BulletinItem {
            market: market.to_string(),
            symbol: symbol.to_string(),
            section: section.to_string(),
            policy_id: format!("{market}:core:long:high_mod:executable_now:h3"),
            policy_label: "test policy".to_string(),
            report_bucket: Some("core".to_string()),
            signal_direction: Some("long".to_string()),
            signal_confidence: Some("HIGH".to_string()),
            headline_mode: Some("uncertain".to_string()),
            execution_mode: Some("executable_now".to_string()),
            reason: "test".to_string(),
            blockers: Vec::new(),
            details: json!({}),
        }
    }

    #[test]
    fn report_model_filters_bulletin_items_by_market() {
        let mut selected_policies = BTreeMap::new();
        selected_policies.insert(
            "us".to_string(),
            Some("us:core:long:high_mod:executable_now:h3".to_string()),
        );
        selected_policies.insert("cn".to_string(), None);

        let mut tactical_policies = BTreeMap::new();
        tactical_policies.insert("us".to_string(), None);
        tactical_policies.insert("cn".to_string(), None);

        let mut evaluated_through = BTreeMap::new();
        evaluated_through.insert("us".to_string(), "2026-05-04".to_string());
        evaluated_through.insert("cn".to_string(), "2026-05-05".to_string());

        let mut ev_status = BTreeMap::new();
        ev_status.insert("us".to_string(), "passed".to_string());
        ev_status.insert("cn".to_string(), "failed".to_string());

        let bulletin = AlphaBulletin {
            as_of: "2026-05-07".to_string(),
            evaluated_through,
            ev_status,
            selected_policies,
            tactical_policies,
            stability: BTreeMap::<String, Vec<PolicyCandidate>>::new(),
            execution_alpha: vec![
                item("us", "execution_alpha", "AAPL"),
                item("cn", "execution_alpha", "000001.SZ"),
            ],
            probation_alpha: vec![
                item("us", "probation_alpha", "DDOG"),
                item("cn", "probation_alpha", "300625.SZ"),
            ],
            tactical_alpha: vec![item("us", "tactical_alpha", "MSFT")],
            options_alpha: vec![
                item("us", "options_alpha", "SPY"),
                item("cn", "options_alpha", "600000.SH"),
            ],
            recall_alpha: vec![item("cn", "recall_alpha", "000002.SZ")],
            blocked_alpha: vec![
                item("us", "blocked_alpha", "TSLA"),
                item("cn", "blocked_alpha", "000003.SZ"),
            ],
        };

        let model = build_report_model(&bulletin, "us", "post");

        assert_eq!(model.market, "us");
        assert_eq!(model.session, "post");
        assert_eq!(model.alpha_bulletin.ev_status.as_deref(), Some("passed"));
        assert_eq!(
            model.alpha_bulletin.selected_policy.as_deref(),
            Some("us:core:long:high_mod:executable_now:h3")
        );
        assert_eq!(model.alpha_bulletin.execution_alpha.len(), 1);
        assert_eq!(model.alpha_bulletin.execution_alpha[0].symbol, "AAPL");
        assert_eq!(model.alpha_bulletin.probation_alpha.len(), 1);
        assert_eq!(model.alpha_bulletin.probation_alpha[0].symbol, "DDOG");
        assert_eq!(model.alpha_bulletin.options_alpha.len(), 1);
        assert_eq!(model.alpha_bulletin.options_alpha[0].symbol, "SPY");
        assert_eq!(model.alpha_bulletin.recall_alpha.len(), 0);
        assert_eq!(model.alpha_bulletin.blocked_alpha.len(), 1);
        assert_eq!(model.alpha_bulletin.blocked_alpha[0].symbol, "TSLA");
    }
}
