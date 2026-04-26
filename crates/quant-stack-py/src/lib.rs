use chrono::NaiveDate;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use quant_stack_core::alpha::{self, AlphaEvalConfig};
use quant_stack_core::report_model;
use std::path::PathBuf;

fn parse_date(date: &str) -> PyResult<NaiveDate> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|err| PyValueError::new_err(format!("invalid date {date}: {err}")))
}

fn normalize_markets(markets: Option<Vec<String>>) -> Vec<String> {
    markets
        .unwrap_or_else(|| vec!["us".to_string(), "cn".to_string()])
        .into_iter()
        .map(|market| market.trim().to_lowercase())
        .filter(|market| !market.is_empty())
        .collect()
}

fn json_to_py(py: Python<'_>, value: &impl serde::Serialize) -> PyResult<Py<PyAny>> {
    let raw = serde_json::to_string(value)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to serialize result: {err}")))?;
    let json = py.import("json")?;
    Ok(json.call_method1("loads", (raw,))?.unbind())
}

#[pyfunction]
#[pyo3(signature = (
    date,
    markets=None,
    lookback_days=30,
    history_db="data/strategy_backtest_history.duckdb",
    output_root="reports/review_dashboard/strategy_backtest",
    us_db="quant-research-v1/data/quant.duckdb",
    cn_db="quant-research-cn/data/quant_cn_report.duckdb",
    us_horizon_days=3,
    cn_horizon_days=2,
    auto_select=true,
    emit_bulletin=true,
    write_project_copies=false
))]
fn evaluate_alpha(
    py: Python<'_>,
    date: &str,
    markets: Option<Vec<String>>,
    lookback_days: i64,
    history_db: &str,
    output_root: &str,
    us_db: &str,
    cn_db: &str,
    us_horizon_days: i64,
    cn_horizon_days: i64,
    auto_select: bool,
    emit_bulletin: bool,
    write_project_copies: bool,
) -> PyResult<Py<PyAny>> {
    let config = AlphaEvalConfig {
        as_of: parse_date(date)?,
        markets: normalize_markets(markets),
        lookback_days,
        auto_select,
        emit_bulletin,
        history_db: PathBuf::from(history_db),
        output_root: PathBuf::from(output_root),
        us_db: PathBuf::from(us_db),
        cn_db: PathBuf::from(cn_db),
        us_horizon_days,
        cn_horizon_days,
        write_project_copies,
    };
    let bulletin = alpha::evaluate(&config)?;
    json_to_py(py, &bulletin)
}

#[pyfunction]
#[pyo3(signature = (db="data/strategy_backtest_history.duckdb", check=false))]
fn migrate(db: &str, check: bool) -> PyResult<()> {
    alpha::migrate(&PathBuf::from(db), check)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (
    date,
    markets=None,
    session="post",
    history_db="data/strategy_backtest_history.duckdb",
    reports_dir="reports"
))]
fn write_report_models(
    date: &str,
    markets: Option<Vec<String>>,
    session: &str,
    history_db: &str,
    reports_dir: &str,
) -> PyResult<usize> {
    Ok(report_model::write_models_from_history(
        &PathBuf::from(history_db),
        date,
        &normalize_markets(markets),
        session,
        &PathBuf::from(reports_dir),
    )?)
}

#[pymodule]
fn quant_stack_py(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_function(wrap_pyfunction!(evaluate_alpha, module)?)?;
    module.add_function(wrap_pyfunction!(migrate, module)?)?;
    module.add_function(wrap_pyfunction!(write_report_models, module)?)?;
    Ok(())
}
