use anyhow::Result;
use chrono::NaiveDate;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Settings {
    pub api: ApiConfig,
    pub runtime: RuntimeConfig,
    pub universe: UniverseConfig,
    pub output: OutputConfig,
    pub data: DataConfig,
    pub signals: SignalsConfig,
    pub reporting: ReportingConfig,
    #[serde(default)]
    pub r#macro: MacroConfig,
    #[serde(default)]
    pub enrichment: EnrichmentConfig,
}

#[derive(Deserialize, Clone)]
pub struct ApiConfig {
    pub tushare_token: String,
    #[serde(default)]
    pub deepseek_key: String,
}

#[derive(Deserialize, Clone, Default)]
pub struct EnrichmentConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_deepseek_model")]
    pub model: String,
}

fn default_true() -> bool {
    true
}
fn default_concurrency() -> usize {
    10
}
fn default_deepseek_model() -> String {
    "deepseek-chat".to_string()
}

#[derive(Deserialize, Clone)]
pub struct RuntimeConfig {
    pub timezone: String,
    pub random_seed: u64,
}

#[derive(Deserialize, Clone)]
pub struct UniverseConfig {
    pub benchmark: String,
    pub scan: ScanConfig,
    pub asset_classes: AssetClassConfig,
    #[serde(default)]
    pub watchlist: Vec<String>,
    pub filters: FilterConfig,
}

#[derive(Deserialize, Clone)]
pub struct ScanConfig {
    pub csi300: bool,
    pub csi500: bool,
    pub csi1000: bool,
    pub sse50: bool,
}

#[derive(Deserialize, Clone)]
pub struct AssetClassConfig {
    pub sector_etfs: bool,
    pub bond_etfs: bool,
    pub commodity_etfs: bool,
    pub cross_border: bool,
}

#[derive(Deserialize, Clone)]
pub struct FilterConfig {
    pub min_avg_volume_shares: u64,
    pub min_price: f64,
}

#[derive(Deserialize, Clone)]
pub struct OutputConfig {
    pub max_notable_items: usize,
    pub min_notable_items: usize,
}

#[derive(Deserialize, Clone)]
pub struct DataConfig {
    #[serde(default)]
    pub db_path: String,
    #[serde(default = "default_raw_db_path")]
    pub raw_db_path: String,
    #[serde(default = "default_research_db_path")]
    pub research_db_path: String,
    #[serde(default = "default_report_db_path")]
    pub report_db_path: String,
    #[serde(default = "default_dev_db_path")]
    pub dev_db_path: String,
    #[serde(default)]
    pub use_dev_for_research: bool,
    pub constituent_refresh_days: u32,
}

fn default_raw_db_path() -> String {
    "data/quant_cn_raw.duckdb".to_string()
}

fn default_research_db_path() -> String {
    "data/quant_cn_research.duckdb".to_string()
}

fn default_report_db_path() -> String {
    "data/quant_cn_report.duckdb".to_string()
}

fn default_dev_db_path() -> String {
    "data/quant_cn_dev.duckdb".to_string()
}

#[derive(Deserialize, Clone)]
pub struct SignalsConfig {
    pub momentum_windows: Vec<u32>,
    pub atr_period: u32,
    #[serde(default = "default_ma_filter")]
    pub ma_filter_window: u32,
    #[serde(default = "default_flow_halflife")]
    pub flow_ewma_halflife: u32,
    #[serde(default = "default_unlock_lookahead")]
    pub unlock_lookahead_days: u32,
}

fn default_ma_filter() -> u32 {
    120
}
fn default_flow_halflife() -> u32 {
    10
}
fn default_unlock_lookahead() -> u32 {
    30
}

#[derive(Deserialize, Clone, Default)]
pub struct MacroConfig {
    #[serde(default)]
    pub series: Vec<MacroSeries>,
}

#[derive(Deserialize, Clone)]
pub struct MacroSeries {
    pub id: String,
    pub name: String,
}

#[derive(Deserialize, Clone)]
pub struct ReportingConfig {
    pub anthropic_model: String,
    pub anthropic_temperature: f64,
    pub max_tokens: u32,
    #[serde(default)]
    pub recipients: Vec<String>,
}

impl Settings {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let settings: Settings = serde_yaml::from_str(&content)?;
        Ok(settings)
    }
}

impl DataConfig {
    pub fn raw_path(&self) -> &str {
        if self.raw_db_path.is_empty() && !self.db_path.is_empty() {
            &self.db_path
        } else {
            &self.raw_db_path
        }
    }

    pub fn research_path(&self) -> &str {
        if self.use_dev_for_research {
            &self.dev_db_path
        } else if self.research_db_path.is_empty() && !self.db_path.is_empty() {
            &self.db_path
        } else {
            &self.research_db_path
        }
    }

    pub fn report_path(&self) -> &str {
        if self.report_db_path.is_empty() && !self.db_path.is_empty() {
            &self.db_path
        } else {
            &self.report_db_path
        }
    }
}

/// Resolve as_of date: explicit string or today in Shanghai timezone.
pub fn resolve_date(date_str: Option<&str>) -> Result<NaiveDate> {
    match date_str {
        Some(s) => Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d")?),
        None => {
            use chrono::{FixedOffset, Utc};
            let shanghai = FixedOffset::east_opt(8 * 3600).unwrap();
            Ok(Utc::now().with_timezone(&shanghai).date_naive())
        }
    }
}
