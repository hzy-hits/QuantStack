use serde::Deserialize;
use std::path::PathBuf;
use tracing::warn;

pub const DEFAULT_LOOKBACK_DAYS: i64 = 90;
pub const DEFAULT_SLIPPAGE_PCT: f64 = 0.18;
pub const DEFAULT_WIN_THRESHOLD_PCT: f64 = 0.50;
pub const DEFAULT_LOSS_THRESHOLD_PCT: f64 = -1.00;
pub const DEFAULT_EV_LCB_80_Z: f64 = 1.2816;
pub const DEFAULT_EV_LCB_95_Z: f64 = 1.6449;
pub const DEFAULT_MIN_SAMPLES: usize = 8;
pub const DEFAULT_MIN_FILLS: usize = 4;
pub const DEFAULT_MIN_FILL_RATE: f64 = 0.35;
pub const DEFAULT_MIN_EV_PCT: f64 = 0.15;
pub const DEFAULT_MIN_EV_LCB_80_PCT: f64 = 0.0;
pub const DEFAULT_MAX_TAIL_LOSS_PCT: f64 = 5.5;

#[derive(Debug, Clone)]
pub struct StrategyParams {
    pub paper_trade_ev: PaperTradeEvParams,
}

impl StrategyParams {
    pub fn load() -> Self {
        match load_from_default_paths() {
            Some(params) => params,
            None => Self::default(),
        }
    }
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            paper_trade_ev: PaperTradeEvParams::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaperTradeEvParams {
    pub lookback_days: i64,
    pub slippage_pct: f64,
    pub win_threshold_pct: f64,
    pub loss_threshold_pct: f64,
    pub ev_lcb_80_z: f64,
    pub ev_lcb_95_z: f64,
    pub min_samples: usize,
    pub min_fills: usize,
    pub min_fill_rate: f64,
    pub min_ev_pct: f64,
    pub min_ev_lcb_80_pct: f64,
    pub max_tail_loss_pct: f64,
    pub source: String,
}

impl Default for PaperTradeEvParams {
    fn default() -> Self {
        Self {
            lookback_days: DEFAULT_LOOKBACK_DAYS,
            slippage_pct: DEFAULT_SLIPPAGE_PCT,
            win_threshold_pct: DEFAULT_WIN_THRESHOLD_PCT,
            loss_threshold_pct: DEFAULT_LOSS_THRESHOLD_PCT,
            ev_lcb_80_z: DEFAULT_EV_LCB_80_Z,
            ev_lcb_95_z: DEFAULT_EV_LCB_95_Z,
            min_samples: DEFAULT_MIN_SAMPLES,
            min_fills: DEFAULT_MIN_FILLS,
            min_fill_rate: DEFAULT_MIN_FILL_RATE,
            min_ev_pct: DEFAULT_MIN_EV_PCT,
            min_ev_lcb_80_pct: DEFAULT_MIN_EV_LCB_80_PCT,
            max_tail_loss_pct: DEFAULT_MAX_TAIL_LOSS_PCT,
            source: "built_in_default".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct StrategyParamsArtifact {
    paper_trade_ev: Option<PaperTradeEvArtifact>,
}

#[derive(Debug, Deserialize)]
struct PaperTradeEvArtifact {
    runtime_params: Option<RuntimeParams>,
}

#[derive(Debug, Deserialize)]
struct RuntimeParams {
    selected: Option<String>,
    default: Option<PaperTradeEvParamSet>,
    candidate: Option<PaperTradeEvParamSet>,
    selected_params: Option<PaperTradeEvParamSet>,
    activation: Option<Activation>,
}

#[derive(Debug, Deserialize)]
struct Activation {
    use_candidate: Option<bool>,
    default_oos_ev_lcb_80_pct: Option<f64>,
    candidate_oos_ev_lcb_80_pct: Option<f64>,
    min_improvement_pct: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PaperTradeEvParamSet {
    lookback_days: Option<i64>,
    slippage_pct: Option<f64>,
    win_threshold_pct: Option<f64>,
    loss_threshold_pct: Option<f64>,
    ev_lcb_80_z: Option<f64>,
    ev_lcb_95_z: Option<f64>,
    min_samples: Option<usize>,
    min_fills: Option<usize>,
    min_fill_rate: Option<f64>,
    min_ev_pct: Option<f64>,
    min_ev_lcb_80_pct: Option<f64>,
    max_tail_loss_pct: Option<f64>,
    provenance: Option<String>,
}

fn load_from_default_paths() -> Option<StrategyParams> {
    for path in candidate_paths() {
        if !path.exists() {
            continue;
        }
        match std::fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_yaml::from_str::<StrategyParamsArtifact>(&raw).ok())
            .and_then(|artifact| params_from_artifact(artifact, path.display().to_string()))
        {
            Some(params) => return Some(params),
            None => warn!(path = %path.display(), "strategy params artifact ignored"),
        }
    }
    None
}

fn candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(path) = std::env::var("QUANT_CN_STRATEGY_PARAMS") {
        paths.push(PathBuf::from(path));
    }
    paths.extend([
        PathBuf::from("config/strategy_params.generated.yaml"),
        PathBuf::from("quant-research-cn/config/strategy_params.generated.yaml"),
        PathBuf::from(
            "../factor-lab/runtime/strategy_calibration/cn/strategy_params.generated.yaml",
        ),
        PathBuf::from("factor-lab/runtime/strategy_calibration/cn/strategy_params.generated.yaml"),
    ]);
    paths
}

fn params_from_artifact(
    artifact: StrategyParamsArtifact,
    source: String,
) -> Option<StrategyParams> {
    let runtime = artifact.paper_trade_ev?.runtime_params?;
    let selected = runtime
        .selected
        .as_deref()
        .unwrap_or("default")
        .to_ascii_lowercase();
    let candidate_allowed = runtime
        .activation
        .as_ref()
        .map(candidate_activation_is_valid)
        .unwrap_or(false);

    let mut params = PaperTradeEvParams::default();
    let chosen = if selected == "candidate" && candidate_allowed {
        runtime
            .candidate
            .as_ref()
            .or(runtime.selected_params.as_ref())
    } else {
        runtime.default.as_ref().or(runtime
            .selected_params
            .as_ref()
            .filter(|_| selected == "default"))
    };
    if let Some(chosen) = chosen {
        apply_param_set(&mut params, chosen);
        params.source = format!(
            "{}:{}",
            if selected == "candidate" && candidate_allowed {
                "calibrated_walk_forward"
            } else {
                "artifact_default"
            },
            source
        );
        return Some(StrategyParams {
            paper_trade_ev: params,
        });
    }
    None
}

fn candidate_activation_is_valid(activation: &Activation) -> bool {
    if !activation.use_candidate.unwrap_or(false) {
        return false;
    }
    let default_lcb = activation
        .default_oos_ev_lcb_80_pct
        .unwrap_or(f64::NEG_INFINITY);
    let candidate_lcb = activation
        .candidate_oos_ev_lcb_80_pct
        .unwrap_or(f64::NEG_INFINITY);
    let min_improvement = activation.min_improvement_pct.unwrap_or(0.05);
    candidate_lcb.is_finite()
        && default_lcb.is_finite()
        && candidate_lcb > 0.0
        && candidate_lcb >= default_lcb + min_improvement
}

fn apply_param_set(params: &mut PaperTradeEvParams, set: &PaperTradeEvParamSet) {
    if let Some(value) = set.lookback_days.filter(|v| *v > 0) {
        params.lookback_days = value;
    }
    if let Some(value) = finite_non_negative(set.slippage_pct) {
        params.slippage_pct = value;
    }
    if let Some(value) = set.win_threshold_pct.filter(|v| v.is_finite()) {
        params.win_threshold_pct = value;
    }
    if let Some(value) = set.loss_threshold_pct.filter(|v| v.is_finite()) {
        params.loss_threshold_pct = value;
    }
    if let Some(value) = finite_positive(set.ev_lcb_80_z) {
        params.ev_lcb_80_z = value;
    }
    if let Some(value) = finite_positive(set.ev_lcb_95_z) {
        params.ev_lcb_95_z = value;
    }
    if let Some(value) = set.min_samples {
        params.min_samples = value;
    }
    if let Some(value) = set.min_fills {
        params.min_fills = value;
    }
    if let Some(value) = finite_non_negative(set.min_fill_rate) {
        params.min_fill_rate = value;
    }
    if let Some(value) = set.min_ev_pct.filter(|v| v.is_finite()) {
        params.min_ev_pct = value;
    }
    if let Some(value) = set.min_ev_lcb_80_pct.filter(|v| v.is_finite()) {
        params.min_ev_lcb_80_pct = value;
    }
    if let Some(value) = finite_positive(set.max_tail_loss_pct) {
        params.max_tail_loss_pct = value;
    }
    if let Some(provenance) = set.provenance.as_ref().filter(|v| !v.is_empty()) {
        params.source = provenance.clone();
    }
}

fn finite_positive(value: Option<f64>) -> Option<f64> {
    value.filter(|v| v.is_finite() && *v > 0.0)
}

fn finite_non_negative(value: Option<f64>) -> Option<f64> {
    value.filter(|v| v.is_finite() && *v >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_from_yaml(raw: &str) -> PaperTradeEvParams {
        let artifact: StrategyParamsArtifact = serde_yaml::from_str(raw).unwrap();
        params_from_artifact(artifact, "test.yaml".to_string())
            .unwrap()
            .paper_trade_ev
    }

    #[test]
    fn inactive_candidate_keeps_default_params() {
        let params = load_from_yaml(
            r#"
paper_trade_ev:
  runtime_params:
    selected: candidate
    default:
      min_ev_pct: 0.15
      min_samples: 8
    candidate:
      min_ev_pct: -0.10
      min_samples: 4
      provenance: calibrated_walk_forward
    activation:
      use_candidate: false
      default_oos_ev_lcb_80_pct: -0.20
      candidate_oos_ev_lcb_80_pct: 0.10
"#,
        );
        assert_eq!(params.min_ev_pct, DEFAULT_MIN_EV_PCT);
        assert_eq!(params.min_samples, DEFAULT_MIN_SAMPLES);
    }

    #[test]
    fn active_candidate_requires_positive_improved_lcb() {
        let params = load_from_yaml(
            r#"
paper_trade_ev:
  runtime_params:
    selected: candidate
    default:
      min_ev_pct: 0.15
      min_samples: 8
    candidate:
      min_ev_pct: -0.10
      min_samples: 4
      provenance: calibrated_walk_forward
    activation:
      use_candidate: true
      min_improvement_pct: 0.05
      default_oos_ev_lcb_80_pct: 0.02
      candidate_oos_ev_lcb_80_pct: 0.09
"#,
        );
        assert_eq!(params.min_ev_pct, -0.10);
        assert_eq!(params.min_samples, 4);
        assert!(params.source.starts_with("calibrated_walk_forward:"));
    }
}
