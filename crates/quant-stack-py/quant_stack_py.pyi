from __future__ import annotations

from typing import Any


def evaluate_alpha(
    date: str,
    markets: list[str] | None = None,
    lookback_days: int = 30,
    history_db: str = "data/strategy_backtest_history.duckdb",
    output_root: str = "reports/review_dashboard/strategy_backtest",
    us_db: str = "quant-research-v1/data/quant.duckdb",
    cn_db: str = "quant-research-cn/data/quant_cn_report.duckdb",
    us_horizon_days: int = 3,
    cn_horizon_days: int = 2,
    auto_select: bool = True,
    emit_bulletin: bool = True,
    write_project_copies: bool = False,
) -> dict[str, Any]: ...


def migrate(
    db: str = "data/strategy_backtest_history.duckdb",
    check: bool = False,
) -> None: ...


def write_report_models(
    date: str,
    markets: list[str] | None = None,
    session: str = "post",
    history_db: str = "data/strategy_backtest_history.duckdb",
    reports_dir: str = "reports",
) -> int: ...


__version__: str
