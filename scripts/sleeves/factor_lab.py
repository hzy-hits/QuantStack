"""Factor Lab sleeve loaders and gates."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

import generate_main_strategy_v2_report as v2

from .base import (
    Sleeve,
    deflated_lcb80_pct,
    fmt_pct,
    probabilistic_positive_mean,
    rolling_oos_min_lcb80_pct,
    rows_as_dicts,
    table_exists,
    top5_pnl_share,
)

FACTOR_LAB_CORR_BLOCK_THRESHOLD = 0.85
FACTOR_LAB_MIN_PROB_POSITIVE = 0.80
FACTOR_LAB_GATE_MODE = "opportunity"
FACTOR_LAB_AUTO_PROD_CONTRACTS = {
    "daily_price_overlay": "action_overlay",
}


def factor_lab_role(report_contract: str, money_readiness: str) -> str:
    contract = str(report_contract or "research_only")
    readiness = str(money_readiness or "research_only")
    if contract == "fresh_buy_gate" and readiness in {"money_ready", "money_candidate"}:
        return "money"
    if contract in {"action_overlay", "setup_overlay", "risk_warning", "hold_overlay"}:
        return "overlay"
    return "research"


def resolve_factor_lab_production_contract(
    report_contract: str,
    money_readiness: str,
    sleeve_id: str,
) -> tuple[str, str, str | None]:
    """Promoted daily-price factors are production overlays unless explicitly downgraded.

    Older Factor Lab rows defaulted to `research_only` for parser compatibility.
    Once such a factor has promoted sleeve returns, treat the daily-price sleeve
    as an executable overlay input; Alpha Factory records weak evidence as
    opportunity flags instead of blocking the sleeve.
    """
    contract = str(report_contract or "research_only").strip().lower()
    readiness = str(money_readiness or "research_only").strip().lower()
    sleeve = str(sleeve_id or "").strip().lower()
    if contract == "research_only" and sleeve in FACTOR_LAB_AUTO_PROD_CONTRACTS:
        promoted_contract = FACTOR_LAB_AUTO_PROD_CONTRACTS[sleeve]
        return promoted_contract, "money_candidate", f"auto_prod_contract={promoted_contract}"
    return contract, readiness, None


def factor_lab_money_status(
    *,
    label: str,
    rows: list[dict[str, Any]],
    report_contract: str,
    money_readiness: str,
    n_trials: int,
    min_money_n: int,
) -> tuple[str, str]:
    role = factor_lab_role(report_contract, money_readiness)
    metrics = v2.compute_metrics(label, rows).to_dict()
    double_cost = v2.compute_metrics(label + " double-cost", rows, return_key="double_cost_return_pct").to_dict()
    top_share = top5_pnl_share(rows)
    prob_positive = probabilistic_positive_mean(metrics)
    deflated_lcb = deflated_lcb80_pct(metrics, n_trials)
    rolling_lcb = rolling_oos_min_lcb80_pct(rows)
    opportunity_flags: list[str] = []
    if int(metrics.get("n") or 0) < min_money_n:
        opportunity_flags.append("sample_thin")
    if (metrics.get("lcb80_pct") or 0.0) <= 0.0:
        opportunity_flags.append("lcb80<=0")
    if top_share is not None and top_share > 0.30:
        opportunity_flags.append("top5_pnl_share>30%")
    if (double_cost.get("lcb80_pct") or 0.0) <= 0.0:
        opportunity_flags.append("double_cost_lcb80<=0")
    if prob_positive is not None and prob_positive < FACTOR_LAB_MIN_PROB_POSITIVE:
        opportunity_flags.append("prob_positive<80%")
    if deflated_lcb is not None and deflated_lcb <= 0.0:
        opportunity_flags.append("deflated_lcb80<=0")
    if rolling_lcb is not None and rolling_lcb <= 0.0:
        opportunity_flags.append("rolling_oos_min_lcb80<=0")
    note = (
        f"contract={report_contract}; readiness={money_readiness}; mode={FACTOR_LAB_GATE_MODE}; "
        f"double_cost_lcb80={fmt_pct(double_cost.get('lcb80_pct'))}; "
        f"top5_pnl_share={fmt_pct((top_share or 0.0) * 100.0) if top_share is not None else '-'}; "
        f"n_trials={max(int(n_trials or 1), 1)}; "
        f"prob_positive={fmt_pct(prob_positive * 100.0) if prob_positive is not None else '-'}; "
        f"deflated_lcb80={fmt_pct(deflated_lcb)}; "
        f"rolling_oos_min_lcb80={fmt_pct(rolling_lcb)}"
    )
    if opportunity_flags:
        note = f"{note}; opportunity_flags={','.join(opportunity_flags)}"
    if role == "research":
        return "research_only", note
    if role == "overlay":
        return "report_overlay", note
    return "money_candidate", note


def factor_lab_trial_counts_by_market(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    if table_exists(con, "factor_experiment_ledger"):
        rows = rows_as_dicts(
            con,
            """
            SELECT market, COUNT(DISTINCT experiment_id) AS n_trials
            FROM factor_experiment_ledger
            WHERE market IS NOT NULL
              AND CAST(ts AS DATE) <= CAST(? AS DATE)
            GROUP BY market
            """,
            [as_of.isoformat()],
        )
        counts.update(
            {
                str(row.get("market") or ""): max(int(row.get("n_trials") or 0), 1)
                for row in rows
                if row.get("market")
            }
        )

    if table_exists(con, "factor_registry"):
        rows = rows_as_dicts(
            con,
            """
            SELECT market, COUNT(*) AS n_trials
            FROM factor_registry
            WHERE market IS NOT NULL
            GROUP BY market
            """,
            [],
        )
        for row in rows:
            market = str(row.get("market") or "")
            if not market:
                continue
            counts[market] = max(counts.get(market, 1), int(row.get("n_trials") or 1))

    return counts


def load_factor_lab_sleeves(
    factor_lab_db: Path,
    start: date,
    as_of: date,
    min_money_n: int,
) -> list[Sleeve]:
    if not factor_lab_db.exists():
        return []
    con = duckdb.connect(str(factor_lab_db), read_only=True)
    try:
        if not table_exists(con, "factor_sleeve_returns"):
            return []
        trial_counts = factor_lab_trial_counts_by_market(con, as_of)
        rows = rows_as_dicts(
            con,
            """
            SELECT return_date, market, factor_id, sleeve_id, factor_name,
                   report_contract, money_readiness, direction, bucket,
                   gross_return_pct, daily_return_pct, cost_adjusted_return_pct,
                   cost_pct, n_names
            FROM factor_sleeve_returns
            WHERE return_date >= CAST(? AS DATE)
              AND return_date <= CAST(? AS DATE)
              AND bucket = 'top_quintile_long'
            ORDER BY market, factor_id, return_date
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    grouped: dict[str, list[dict[str, Any]]] = {}
    meta: dict[str, dict[str, Any]] = {}
    for row in rows:
        factor_id = str(row.get("factor_id") or "")
        if not factor_id:
            continue
        grouped.setdefault(factor_id, []).append(
            {
                "report_date": v2.as_iso(row.get("return_date")),
                "symbol": factor_id,
                "return_pct": v2.round_or_none(row.get("cost_adjusted_return_pct")),
                "gross_return_pct": v2.round_or_none(row.get("gross_return_pct")),
                "double_cost_return_pct": (
                    v2.round_or_none(row.get("daily_return_pct"))
                    - 2.0 * v2.round_or_none(row.get("cost_pct"))
                    if v2.round_or_none(row.get("daily_return_pct")) is not None
                    and v2.round_or_none(row.get("cost_pct")) is not None
                    else None
                ),
                "n_names": row.get("n_names"),
            }
        )
        meta[factor_id] = row

    sleeves: list[Sleeve] = []
    for factor_id, factor_rows in grouped.items():
        info = meta[factor_id]
        name = str(info.get("factor_name") or factor_id)
        market = str(info.get("market") or "")
        registry_contract = str(info.get("report_contract") or "research_only")
        registry_readiness = str(info.get("money_readiness") or "research_only")
        sleeve_contract = str(info.get("sleeve_id") or "")
        contract, readiness, auto_prod_note = resolve_factor_lab_production_contract(
            registry_contract,
            registry_readiness,
            sleeve_contract,
        )
        n_trials = trial_counts.get(market, 1)
        money_status, note = factor_lab_money_status(
            label=f"Factor Lab {name} top-quintile sleeve",
            rows=factor_rows,
            report_contract=contract,
            money_readiness=readiness,
            n_trials=n_trials,
            min_money_n=min_money_n,
        )
        if auto_prod_note:
            note = f"{note}; legacy_contract={registry_contract}; {auto_prod_note}"
        sleeves.append(
            Sleeve(
                sleeve_id=(
                    f"factor_lab_{factor_id}"
                    if factor_id.startswith(f"{market}_")
                    else f"factor_lab_{market}_{factor_id}"
                ),
                market=market,
                label=f"Factor Lab {name}",
                signal_rule=(
                    f"promoted factor {info.get('sleeve_id')}; contract={contract}; "
                    "oriented long-only top quintile, 5D forward return averaged to daily"
                ),
                horizon="5D forward, daily averaged",
                data_status="factor_sleeve_returns",
                money_status=money_status,
                notes=note,
                rows=factor_rows,
                source_factor_id=factor_id,
            )
        )
    return sleeves
