#!/usr/bin/env python3
"""Build an alpha-sleeve scorecard across the existing quant-stack ledgers.

This report is intentionally a control-plane layer. It does not invent a new
trading model; it asks which existing return streams have enough evidence,
which ones are just diagnostics, and which data gaps prevent a sleeve from
becoming money-ready.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb


SCRIPT_DIR = Path(__file__).resolve().parent
STACK_ROOT = SCRIPT_DIR.parents[0]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_main_strategy_v2_backtest as v2  # noqa: E402
from sleeves import build_sleeves  # noqa: E402
from sleeves.base import Sleeve, daily_series, fmt_num, fmt_pct, pearson_corr  # noqa: E402
from sleeves.factor_lab import FACTOR_LAB_CORR_BLOCK_THRESHOLD  # noqa: E402
from sleeves.portfolio_hedge import build_portfolio_hedged_backtest  # noqa: E402


DEFAULT_START = "2026-03-01"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "alpha_factory"
DEFAULT_FACTOR_LAB_DB = STACK_ROOT / "factor-lab" / "data" / "factor_lab.duckdb"
PROMOTION_GATE_VERSION = "calibrated_window_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run alpha sleeve scorecard backtest.")
    parser.add_argument("--date", default=None, help="Report date. Defaults to latest available DB date.")
    parser.add_argument("--start", default=DEFAULT_START, help="Backtest start date.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--us-db", type=Path, default=STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb")
    parser.add_argument("--cn-db", type=Path, default=STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb")
    parser.add_argument("--factor-lab-db", type=Path, default=DEFAULT_FACTOR_LAB_DB)
    parser.add_argument("--min-money-n", type=int, default=20)
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def latest_report_date(us_db: Path, cn_db: Path) -> date:
    return v2.infer_report_date(us_db, cn_db)


def build_correlation_payload(sleeves: list[Sleeve]) -> dict[str, Any]:
    series = {s.sleeve_id: daily_series(s.rows) for s in sleeves}
    matrix: list[dict[str, Any]] = []
    pair_values: list[float] = []
    for left in sleeves:
        for right in sleeves:
            corr = 1.0 if left.sleeve_id == right.sleeve_id else pearson_corr(series[left.sleeve_id], series[right.sleeve_id])
            matrix.append(
                {
                    "sleeve_a": left.sleeve_id,
                    "sleeve_b": right.sleeve_id,
                    "corr": v2.round_or_none(corr),
                    "overlap_days": len(set(series[left.sleeve_id]) & set(series[right.sleeve_id])),
                }
            )
            if left.sleeve_id < right.sleeve_id and corr is not None:
                pair_values.append(abs(corr))
    avg_abs_corr = statistics.fmean(pair_values) if pair_values else None
    n = len([s for s in sleeves if daily_series(s.rows)])
    n_eff = None
    if n > 0:
        rho = avg_abs_corr if avg_abs_corr is not None else 0.0
        n_eff = n / (1.0 + (n - 1) * rho)
    return {
        "matrix": matrix,
        "avg_abs_corr": v2.round_or_none(avg_abs_corr),
        "n_eff_all": v2.round_or_none(n_eff),
    }


def combo_rows_from_daily(series_by_id: dict[str, dict[str, float]], ids: list[str]) -> list[dict[str, Any]]:
    all_dates = sorted(set().union(*(set(series_by_id.get(sid, {})) for sid in ids))) if ids else []
    rows: list[dict[str, Any]] = []
    for dt in all_dates:
        values = [series_by_id[sid][dt] for sid in ids if dt in series_by_id.get(sid, {})]
        if values:
            rows.append({"report_date": dt, "symbol": "combo", "return_pct": statistics.fmean(values)})
    return rows


def enrich_relationship_metrics(metrics: list[dict[str, Any]], sleeves: list[Sleeve], correlations: dict[str, Any]) -> None:
    matrix = correlations.get("matrix", [])
    max_corr: dict[str, float | None] = {row["sleeve_id"]: None for row in metrics}
    for row in matrix:
        left = row.get("sleeve_a")
        right = row.get("sleeve_b")
        corr = v2.round_or_none(row.get("corr"))
        if left == right or corr is None:
            continue
        for sid in (left, right):
            prior = max_corr.get(sid)
            max_corr[sid] = abs(corr) if prior is None else max(prior, abs(corr))

    series_by_id = {s.sleeve_id: daily_series(s.rows) for s in sleeves}
    eligible_ids = [
        row["sleeve_id"]
        for row in metrics
        if row["money_status"] in {"money_candidate", "stock_trade"}
    ]

    for row in metrics:
        sid = row["sleeve_id"]
        row["max_abs_corr"] = v2.round_or_none(max_corr.get(sid))
        base_ids = [item for item in eligible_ids if item != sid]
        with_ids = list(dict.fromkeys([*base_ids, sid]))
        base_metrics = v2.compute_metrics("base blend", combo_rows_from_daily(series_by_id, base_ids)).to_dict()
        with_metrics = v2.compute_metrics("with sleeve blend", combo_rows_from_daily(series_by_id, with_ids)).to_dict()
        base_sharpe = base_metrics.get("daily_sharpe")
        with_sharpe = with_metrics.get("daily_sharpe")
        if with_sharpe is None:
            row["marginal_daily_sharpe_delta"] = None
        elif base_sharpe is None:
            row["marginal_daily_sharpe_delta"] = v2.round_or_none(with_sharpe)
        else:
            row["marginal_daily_sharpe_delta"] = v2.round_or_none(float(with_sharpe) - float(base_sharpe))


def apply_factor_lab_relationship_gates(metrics: list[dict[str, Any]]) -> None:
    """Annotate Factor Lab relationship risks without suppressing opportunities."""
    for row in metrics:
        sleeve_id = str(row.get("sleeve_id") or "")
        if not sleeve_id.startswith("factor_lab_") or row.get("money_status") != "money_candidate":
            continue

        blockers: list[str] = []
        max_corr = v2.round_or_none(row.get("max_abs_corr"))
        marginal = v2.round_or_none(row.get("marginal_daily_sharpe_delta"))
        if max_corr is not None and max_corr >= FACTOR_LAB_CORR_BLOCK_THRESHOLD:
            blockers.append(f"corr>={FACTOR_LAB_CORR_BLOCK_THRESHOLD:.2f}")
        if marginal is not None and marginal <= 0:
            blockers.append("marginal_sharpe<=0")

        if blockers:
            row["notes"] = f"{row.get('notes') or ''}; portfolio_flags={','.join(blockers)}".strip("; ")


def sync_sleeve_statuses_from_metrics(sleeves: list[Sleeve], metrics: list[dict[str, Any]]) -> None:
    by_id = {row["sleeve_id"]: row for row in metrics}
    for sleeve in sleeves:
        row = by_id.get(sleeve.sleeve_id)
        if not row:
            continue
        sleeve.money_status = str(row.get("money_status") or sleeve.money_status)
        sleeve.notes = str(row.get("notes") or sleeve.notes)


def build_calibration_payload(metrics: list[dict[str, Any]], sleeves: list[Sleeve]) -> dict[str, Any]:
    by_id = {sleeve.sleeve_id: sleeve for sleeve in sleeves}
    rows: list[dict[str, Any]] = []
    for row in metrics:
        sleeve = by_id.get(str(row.get("sleeve_id") or ""))
        sleeve_rows = sleeve.rows if sleeve else []
        full_confirm = sum(1 for item in sleeve_rows if item.get("confirm_quality") == "full_confirm")
        proxy_confirm = sum(1 for item in sleeve_rows if item.get("confirm_quality") in {"proxy_confirm", "price_volume_proxy"})
        has_confirm_dimension = full_confirm > 0 or proxy_confirm > 0
        n = int(row.get("n") or 0)
        active_dates = int(row.get("active_dates") or 0)
        min_n = min(100, max(20, active_dates))
        min_active_dates = min(20, max(8, active_dates // 2 if active_dates else 8))
        min_full_confirm = min(20, max(8, full_confirm // 2)) if has_confirm_dimension else 0
        rows.append(
            {
                "sleeve_id": row.get("sleeve_id"),
                "market": row.get("market"),
                "n": n,
                "active_dates": active_dates,
                "n_with_full_confirm": full_confirm,
                "n_with_proxy_confirm": proxy_confirm,
                "has_confirm_dimension": has_confirm_dimension,
                "calibrated_min_n": min_n,
                "calibrated_min_active_dates": min_active_dates,
                "calibrated_min_full_confirm": min_full_confirm,
                "lcb80_pct": row.get("lcb80_pct"),
                "daily_sharpe": row.get("daily_sharpe"),
                "win_rate": row.get("win_rate"),
                "top5_pnl_share": row.get("top5_pnl_share"),
                "max_abs_corr": row.get("max_abs_corr"),
                "marginal_daily_sharpe_delta": row.get("marginal_daily_sharpe_delta"),
            }
        )
    return {
        "gate_version": PROMOTION_GATE_VERSION,
        "description": "Coverage-aware calibration from the current historical window; gates use full-confirm counts where the sleeve has a confirm dimension.",
        "rows": rows,
    }


def _promotion_blockers(row: dict[str, Any], calibration: dict[str, Any]) -> list[str]:
    if row.get("money_status") not in {"money_candidate", "stock_trade"}:
        return [f"money_status={row.get('money_status') or 'missing'}"]
    blockers: list[str] = []
    n = int(row.get("n") or 0)
    active = int(row.get("active_dates") or 0)
    min_n = int(calibration.get("calibrated_min_n") or 20)
    min_active = int(calibration.get("calibrated_min_active_dates") or 8)
    if n < min_n:
        blockers.append(f"n<{min_n}")
    if active < min_active:
        blockers.append(f"active_dates<{min_active}")
    lcb80 = v2.round_or_none(row.get("lcb80_pct"))
    if lcb80 is None or lcb80 <= 0:
        blockers.append("lcb80<=0")
    win_rate = v2.round_or_none(row.get("win_rate"))
    if win_rate is None or win_rate < 0.52:
        blockers.append("win_rate<52%")
    daily_sharpe = v2.round_or_none(row.get("daily_sharpe"))
    if daily_sharpe is None or daily_sharpe <= 0:
        blockers.append("daily_sharpe<=0")
    top_share = v2.round_or_none(row.get("top5_pnl_share"))
    if top_share is not None and top_share > 0.60:
        blockers.append("top5_pnl_share>60%")
    # Correlation is reported as a portfolio diagnostic. It should not block a
    # sleeve by itself because overlays and explanatory diagnostics can be
    # highly correlated with the production sleeve they explain.
    marginal = v2.round_or_none(row.get("marginal_daily_sharpe_delta"))
    if marginal is not None and marginal < -0.10:
        blockers.append("marginal_sharpe_delta<-0.10")
    if calibration.get("has_confirm_dimension"):
        full_confirm = int(calibration.get("n_with_full_confirm") or 0)
        min_full = int(calibration.get("calibrated_min_full_confirm") or 0)
        if min_full and full_confirm < min_full:
            blockers.append(f"full_confirm<{min_full}")
    return blockers


def build_promotion_contract(metrics: list[dict[str, Any]], calibration: dict[str, Any], payload_as_of: str, start: str) -> list[dict[str, Any]]:
    calibration_by_id = {row.get("sleeve_id"): row for row in calibration.get("rows") or []}
    rows: list[dict[str, Any]] = []
    for row in metrics:
        sleeve_id = str(row.get("sleeve_id") or "")
        cal = calibration_by_id.get(sleeve_id) or {}
        blockers = _promotion_blockers(row, cal)
        status = "promoted" if not blockers else "rejected"
        snapshot = {
            "gate_version": PROMOTION_GATE_VERSION,
            "blockers": blockers,
            "metrics": {
                key: row.get(key)
                for key in [
                    "n",
                    "active_dates",
                    "lcb80_pct",
                    "win_rate",
                    "daily_sharpe",
                    "top5_pnl_share",
                    "max_abs_corr",
                    "marginal_daily_sharpe_delta",
                ]
            },
            "calibration": cal,
        }
        rows.append(
            {
                "as_of": payload_as_of,
                "start_date": start,
                "market": row.get("market"),
                "sleeve_id": sleeve_id,
                "status": status,
                "gate_version": PROMOTION_GATE_VERSION,
                "created_by": "alpha_sleeve_backtest",
                "promoted_at": datetime.now().isoformat(timespec="seconds") if status == "promoted" else None,
                "gates_snapshot_json": json.dumps(snapshot, ensure_ascii=True, sort_keys=True, default=str),
                "blockers": blockers,
            }
        )
    return rows


def apply_promotion_contract_to_metrics(metrics: list[dict[str, Any]], promotions: list[dict[str, Any]]) -> None:
    by_id = {row.get("sleeve_id"): row for row in promotions}
    for row in metrics:
        if row.get("money_status") not in {"money_candidate", "stock_trade"}:
            continue
        promotion = by_id.get(row.get("sleeve_id")) or {}
        if promotion.get("status") == "promoted":
            row["promotion_status"] = "promoted"
            continue
        blockers = promotion.get("blockers") or ["missing_promotion_contract"]
        row["promotion_status"] = "rejected"
        row["money_status"] = "blocked_promotion_gate"
        row["notes"] = f"{row.get('notes') or ''}; promotion_blockers={','.join(blockers)}".strip("; ")


def build_combo_payload(sleeves: list[Sleeve]) -> dict[str, Any]:
    eligible = [
        sleeve
        for sleeve in sleeves
        if sleeve.money_status in {"money_candidate", "stock_trade"}
    ]
    per_sleeve = {s.sleeve_id: daily_series(s.rows) for s in eligible}
    all_dates = sorted(set().union(*(set(values) for values in per_sleeve.values()))) if per_sleeve else []
    combo_rows: list[dict[str, Any]] = []
    for dt in all_dates:
        values = [series[dt] for series in per_sleeve.values() if dt in series]
        if values:
            combo_rows.append({"report_date": dt, "symbol": "combo", "return_pct": statistics.fmean(values)})
    metrics = v2.compute_metrics("Equal-weight viable sleeve daily blend", combo_rows).to_dict()
    return {
        "eligible_sleeves": [s.sleeve_id for s in eligible],
        "metrics": metrics,
        "daily_returns": daily_series(combo_rows),
    }


def render_metrics_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Sleeve | Market | n | Days | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Max corr | Marginal Sharpe | Top5 PnL | Mean breadth | Money status | Data status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['market']} | {row['n']} | {row['active_dates']} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0.0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_num(row.get('trade_sharpe'), 2)} | {fmt_num(row.get('daily_sharpe'), 2)} | "
            f"{fmt_num(row.get('max_abs_corr'), 2)} | {fmt_num(row.get('marginal_daily_sharpe_delta'), 2)} | "
            f"{fmt_pct((row.get('top5_pnl_share') or 0.0) * 100.0) if row.get('top5_pnl_share') is not None else '-'} | "
            f"{fmt_num(row.get('mean_daily_breadth'), 1)} | {row['money_status']} | {row['data_status']} |"
        )
    return lines + [""]


def render_factor_lab_table(rows: list[dict[str, Any]], correlations: dict[str, Any]) -> list[str]:
    factor_rows = [row for row in rows if str(row.get("sleeve_id") or "").startswith("factor_lab_")]
    if not factor_rows:
        return []
    lines = [
        "## Factor Lab Promoted Factor Sleeves",
        "",
        "| Factor sleeve | Market | n | LCB80 | Daily Sharpe | Max corr | N_eff context | Marginal Sharpe | Money status | Notes |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in factor_rows:
        lines.append(
            f"| {row['label']} | {row['market']} | {row['n']} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_num(row.get('max_abs_corr'), 2)} | "
            f"{fmt_num(correlations.get('n_eff_all'), 2)} | {fmt_num(row.get('marginal_daily_sharpe_delta'), 2)} | "
            f"{row['money_status']} | {row.get('notes') or '-'} |"
        )
    return lines + [""]


def render_calibration_table(calibration: dict[str, Any]) -> list[str]:
    rows = calibration.get("rows") or []
    if not rows:
        return []
    lines = [
        "## Calibration And Promotion Contract",
        "",
        "Calibration is reported before promotion. Sleeves with full-confirm/proxy-confirm dimensions are evaluated separately so short data coverage does not masquerade as proof.",
        "",
        "| Sleeve | n | Active days | Full confirm | Proxy confirm | Min n | Min days | Min full | LCB80 | Daily Sharpe | Promotion | Blockers |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    promotions = {row.get("sleeve_id"): row for row in calibration.get("promotions") or []}
    for row in rows:
        promo = promotions.get(row.get("sleeve_id")) or {}
        blockers = ", ".join(promo.get("blockers") or [])
        lines.append(
            f"| {row.get('sleeve_id')} | {row.get('n')} | {row.get('active_dates')} | "
            f"{row.get('n_with_full_confirm')} | {row.get('n_with_proxy_confirm')} | "
            f"{row.get('calibrated_min_n')} | {row.get('calibrated_min_active_dates')} | "
            f"{row.get('calibrated_min_full_confirm')} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {promo.get('status') or '-'} | {blockers or '-'} |"
        )
    return lines + [""]


def render_corr_matrix(sleeves: list[dict[str, Any]], correlations: dict[str, Any]) -> list[str]:
    ids = [row["sleeve_id"] for row in sleeves]
    labels = {row["sleeve_id"]: row["sleeve_id"] for row in sleeves}
    corr_map = {
        (row["sleeve_a"], row["sleeve_b"]): row.get("corr")
        for row in correlations.get("matrix", [])
    }
    lines = ["| Sleeve | " + " | ".join(ids) + " |", "|---|" + "|".join(["---:"] * len(ids)) + "|"]
    for left in ids:
        cells = [labels[left]]
        for right in ids:
            cells.append(fmt_num(corr_map.get((left, right)), 2))
        lines.append("| " + " | ".join(cells) + " |")
    return lines + [""]


def render_report(payload: dict[str, Any]) -> str:
    metrics = payload["sleeves"]
    money = [row for row in metrics if row["money_status"] in {"money_candidate", "stock_trade", "report_overlay"}]
    blocked = [
        row
        for row in metrics
        if row["money_status"] in {
            "blocked_negative_or_unproven",
            "blocked_concentrated_pnl",
            "blocked_double_cost_lcb80",
            "blocked_prob_sharpe",
            "blocked_deflated_sharpe",
            "blocked_rolling_oos",
            "blocked_factor_lab_portfolio_gate",
            "blocked_promotion_gate",
            "no_data",
        }
    ]
    combo = payload["combo"]["metrics"]
    if money:
        conclusion = (
            f"{len(money)} sleeves have positive money/report evidence; equal-weight viable daily blend "
            f"LCB80 {fmt_pct(combo.get('lcb80_pct'))}, daily Sharpe {fmt_num(combo.get('daily_sharpe'), 2)}."
        )
    else:
        conclusion = "No current opportunity sleeve has usable rows; research-only diagnostics stay informational."
    lines = [
        f"# Alpha Factory Sleeve Backtest - {payload['as_of']}",
        "",
        f"**One-line conclusion:** {conclusion}",
        "",
        "## Coverage",
        "",
        f"- Range: `{payload['start']}` to `{payload['as_of']}`.",
        f"- Sleeves evaluated: `{len(metrics)}`.",
        f"- Average absolute sleeve correlation: `{fmt_num(payload['correlations'].get('avg_abs_corr'), 2)}`.",
        f"- Effective independent sleeve count N_eff: `{fmt_num(payload['correlations'].get('n_eff_all'), 2)}`.",
        f"- Viable money/trade blend sleeves: `{', '.join(payload['combo'].get('eligible_sleeves') or []) or '-'}`.",
        "",
        "## Sleeve Scorecard",
        "",
    ]
    lines += render_metrics_table(metrics)
    lines += render_calibration_table(payload.get("calibration") or {})
    lines += render_factor_lab_table(metrics, payload["correlations"])
    lines += [
        "## Correlation Budget",
        "",
        "Correlation is computed on same-date average sleeve returns. Sparse event diagnostics with few overlapping dates should be read as research context, not optimized weights.",
        "",
    ]
    lines += render_corr_matrix(metrics, payload["correlations"])
    lines += [
        "## Equal-Weight Viable Sleeve Blend",
        "",
        "This blend excludes report overlays so the CN mainline is not double-counted against its own explanatory filters.",
        "The blend is a short-window diagnostic, not a production allocation; size still follows each sleeve's money_status and live execution ledger.",
        "",
        "| n | Active days | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Max DD |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| {combo.get('n', 0)} | {combo.get('active_dates', 0)} | {fmt_pct(combo.get('avg_pct'))} | "
        f"{fmt_pct(combo.get('lcb80_pct'))} | "
        f"{fmt_pct((combo.get('win_rate') or 0.0) * 100.0) if combo.get('win_rate') is not None else '-'} | "
        f"{fmt_num(combo.get('trade_sharpe'), 2)} | {fmt_num(combo.get('daily_sharpe'), 2)} | "
        f"{fmt_pct(combo.get('max_drawdown_pct'))} |",
        "",
    ]
    hedge = payload.get("portfolio_hedge") or {}
    hedge_summary = hedge.get("summary") or {}
    hedge_net = hedge_summary.get("net") or {}
    hedge_unhedged = hedge_summary.get("unhedged") or {}
    hedge_leg = hedge_summary.get("hedge_leg") or {}
    lines += [
        "## Historical Beta-Hedged Portfolio",
        "",
        "Historical hedge ledger uses existing money/stock-trade sleeves and the same beta hedge selector as the current-day portfolio overlay.",
        "Returns are R PnL proxies: `long_return_r - beta_hedge_return_r - hedge_cost_r`.",
        "",
        "| Book | n | Avg R | LCB80 R | Win | Total R | Max DD R |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| Unhedged long | {hedge_unhedged.get('n', 0)} | {fmt_num(hedge_unhedged.get('avg_r'), 5)} | "
        f"{fmt_num(hedge_unhedged.get('lcb80_r'), 5)} | "
        f"{fmt_pct((hedge_unhedged.get('win_rate') or 0.0) * 100.0) if hedge_unhedged.get('win_rate') is not None else '-'} | "
        f"{fmt_num(hedge_unhedged.get('total_r'), 4)} | {fmt_num(hedge_unhedged.get('max_drawdown_r'), 4)} |",
        f"| Beta hedge leg | {hedge_leg.get('n', 0)} | {fmt_num(hedge_leg.get('avg_r'), 5)} | "
        f"{fmt_num(hedge_leg.get('lcb80_r'), 5)} | "
        f"{fmt_pct((hedge_leg.get('win_rate') or 0.0) * 100.0) if hedge_leg.get('win_rate') is not None else '-'} | "
        f"{fmt_num(hedge_leg.get('total_r'), 4)} | {fmt_num(hedge_leg.get('max_drawdown_r'), 4)} |",
        f"| Hedged net | {hedge_net.get('n', 0)} | {fmt_num(hedge_net.get('avg_r'), 5)} | "
        f"{fmt_num(hedge_net.get('lcb80_r'), 5)} | "
        f"{fmt_pct((hedge_net.get('win_rate') or 0.0) * 100.0) if hedge_net.get('win_rate') is not None else '-'} | "
        f"{fmt_num(hedge_net.get('total_r'), 4)} | {fmt_num(hedge_net.get('max_drawdown_r'), 4)} |",
        "",
        f"- Ledger rows: `{len(hedge.get('ledger') or [])}`.",
        f"- Daily aggregate rows: `{len(hedge.get('daily') or [])}`.",
        f"- Eligible sleeves: `{', '.join(hedge_summary.get('eligible_sleeves') or []) or '-'}`.",
        "",
        "## Money Readiness",
        "",
    ]
    if money:
        for row in money:
            lines.append(
                f"- `{row['sleeve_id']}`: {row['money_status']}; LCB80 {fmt_pct(row.get('lcb80_pct'))}; "
                f"n={row.get('n')}; note={row.get('notes')}"
            )
    else:
        lines.append("- No money-ready sleeve.")
    if blocked:
        lines.append("")
        lines.append("Blocked / no-data sleeves:")
        for row in blocked:
            lines.append(f"- `{row['sleeve_id']}`: {row['money_status']}; data={row['data_status']}; note={row['notes']}")
    lines += [
        "",
        "## Data Gaps",
        "",
        "- US filings: `sec_filings` is only form/item metadata. Tender/merger/CEF alpha needs a document parser and payoff table.",
        "- CN events: `forecast` exists, but cash-choice/tender/absorption-merger terms are not yet normalized into event payoff rows.",
        "- CN convertibles: `cb_daily` has price/value/premium, but lacks forced-redemption, putback, conversion-window, rating, and remaining-size fields.",
        "- Microstructure: no auction/order-book replay is present here, so 1-5D residual stat-arb and limit-up execution stay research/radar.",
        "- Options: US options/flow quality is auxiliary evidence for stock trades; option leg PnL is diagnostic, not a stock blocker.",
        "",
        "## Commands",
        "",
        "```bash",
        f"python scripts/run_alpha_sleeve_backtest.py --date {payload['as_of']} --start {payload['start']}",
        f"python scripts/run_main_strategy_v2_backtest.py --date {payload['as_of']} --start {payload['start']}",
        f"python scripts/run_cn_log_denoise_backtest.py --date {payload['as_of']} --start {payload['start']}",
        "```",
        "",
    ]
    return "\n".join(lines)


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE alpha_sleeve_metrics (
                as_of DATE, start_date DATE, sleeve_id VARCHAR, market VARCHAR, label VARCHAR,
                signal_rule VARCHAR, horizon VARCHAR, n INTEGER, active_dates INTEGER,
                avg_pct DOUBLE, median_pct DOUBLE, win_rate DOUBLE, lcb80_pct DOUBLE,
                lcb95_pct DOUBLE, trade_sharpe DOUBLE, daily_sharpe DOUBLE,
                max_drawdown_pct DOUBLE, top5_pnl_share DOUBLE, mean_daily_breadth DOUBLE,
                max_abs_corr DOUBLE, marginal_daily_sharpe_delta DOUBLE,
                money_status VARCHAR, data_status VARCHAR, notes VARCHAR
            )
            """
        )
        metric_rows = []
        for row in payload["sleeves"]:
            metric_rows.append(
                [
                    payload["as_of"],
                    payload["start"],
                    row.get("sleeve_id"),
                    row.get("market"),
                    row.get("label"),
                    row.get("signal_rule"),
                    row.get("horizon"),
                    row.get("n"),
                    row.get("active_dates"),
                    row.get("avg_pct"),
                    row.get("median_pct"),
                    row.get("win_rate"),
                    row.get("lcb80_pct"),
                    row.get("lcb95_pct"),
                    row.get("trade_sharpe"),
                    row.get("daily_sharpe"),
                    row.get("max_drawdown_pct"),
                    row.get("top5_pnl_share"),
                    row.get("mean_daily_breadth"),
                    row.get("max_abs_corr"),
                    row.get("marginal_daily_sharpe_delta"),
                    row.get("money_status"),
                    row.get("data_status"),
                    row.get("notes"),
                ]
            )
        con.executemany(
            "INSERT INTO alpha_sleeve_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metric_rows,
        )
        con.execute(
            """
            CREATE TABLE alpha_sleeve_correlation (
                as_of DATE, sleeve_a VARCHAR, sleeve_b VARCHAR, corr DOUBLE, overlap_days INTEGER
            )
            """
        )
        con.executemany(
            "INSERT INTO alpha_sleeve_correlation VALUES (?, ?, ?, ?, ?)",
            [
                [payload["as_of"], row["sleeve_a"], row["sleeve_b"], row.get("corr"), row.get("overlap_days")]
                for row in payload["correlations"]["matrix"]
            ],
        )
        con.execute(
            """
            CREATE TABLE alpha_sleeve_daily_returns (
                as_of DATE, sleeve_id VARCHAR, return_date DATE, return_pct DOUBLE
            )
            """
        )
        daily_rows = []
        for sleeve_id, series in payload["daily_returns"].items():
            for dt, ret in series.items():
                daily_rows.append([payload["as_of"], sleeve_id, dt, ret])
        con.executemany("INSERT INTO alpha_sleeve_daily_returns VALUES (?, ?, ?, ?)", daily_rows)
        con.execute(
            """
            CREATE TABLE alpha_sleeve_calibration (
                as_of DATE, sleeve_id VARCHAR, market VARCHAR, n INTEGER,
                active_dates INTEGER, n_with_full_confirm INTEGER,
                n_with_proxy_confirm INTEGER, has_confirm_dimension BOOLEAN,
                calibrated_min_n INTEGER, calibrated_min_active_dates INTEGER,
                calibrated_min_full_confirm INTEGER, lcb80_pct DOUBLE,
                daily_sharpe DOUBLE, win_rate DOUBLE, top5_pnl_share DOUBLE,
                max_abs_corr DOUBLE, marginal_daily_sharpe_delta DOUBLE
            )
            """
        )
        calibration_rows = []
        for row in (payload.get("calibration") or {}).get("rows") or []:
            calibration_rows.append(
                [
                    payload["as_of"],
                    row.get("sleeve_id"),
                    row.get("market"),
                    row.get("n"),
                    row.get("active_dates"),
                    row.get("n_with_full_confirm"),
                    row.get("n_with_proxy_confirm"),
                    row.get("has_confirm_dimension"),
                    row.get("calibrated_min_n"),
                    row.get("calibrated_min_active_dates"),
                    row.get("calibrated_min_full_confirm"),
                    row.get("lcb80_pct"),
                    row.get("daily_sharpe"),
                    row.get("win_rate"),
                    row.get("top5_pnl_share"),
                    row.get("max_abs_corr"),
                    row.get("marginal_daily_sharpe_delta"),
                ]
            )
        con.executemany(
            "INSERT INTO alpha_sleeve_calibration VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            calibration_rows,
        )
        con.execute(
            """
            CREATE TABLE promoted_sleeves (
                as_of DATE, start_date DATE, market VARCHAR, sleeve_id VARCHAR,
                status VARCHAR, gate_version VARCHAR, created_by VARCHAR,
                promoted_at TIMESTAMP, gates_snapshot_json VARCHAR
            )
            """
        )
        promotion_rows = []
        for row in payload.get("promoted_sleeves") or []:
            promotion_rows.append(
                [
                    row.get("as_of"),
                    row.get("start_date"),
                    row.get("market"),
                    row.get("sleeve_id"),
                    row.get("status"),
                    row.get("gate_version"),
                    row.get("created_by"),
                    row.get("promoted_at"),
                    row.get("gates_snapshot_json"),
                ]
            )
        con.executemany(
            "INSERT INTO promoted_sleeves VALUES (CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), ?)",
            promotion_rows,
        )
        con.execute(
            """
            CREATE TABLE portfolio_hedged_backtest (
                as_of DATE, return_date DATE, market VARCHAR, sleeve_id VARCHAR,
                long_return_r DOUBLE, beta_hedge_return_r DOUBLE, hedge_cost_r DOUBLE,
                net_return_r DOUBLE, gross_long_r DOUBLE, hedge_notional_r DOUBLE,
                net_beta_r DOUBLE, benchmark VARCHAR, detail_json VARCHAR
            )
            """
        )
        hedge_daily_rows = []
        for row in (payload.get("portfolio_hedge") or {}).get("daily") or []:
            hedge_daily_rows.append(
                [
                    row.get("as_of"),
                    row.get("return_date"),
                    row.get("market"),
                    row.get("sleeve_id"),
                    row.get("long_return_r"),
                    row.get("beta_hedge_return_r"),
                    row.get("hedge_cost_r"),
                    row.get("net_return_r"),
                    row.get("gross_long_r"),
                    row.get("hedge_notional_r"),
                    row.get("net_beta_r"),
                    row.get("benchmark"),
                    row.get("detail_json"),
                ]
            )
        if hedge_daily_rows:
            con.executemany(
                "INSERT INTO portfolio_hedged_backtest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                hedge_daily_rows,
            )
        con.execute(
            """
            CREATE TABLE portfolio_hedge_ledger (
                as_of DATE, signal_date DATE, entry_date DATE, exit_date DATE,
                return_date DATE, market VARCHAR, sleeve_id VARCHAR, symbol_or_basket VARCHAR,
                long_r DOUBLE, hedge_instrument VARCHAR, hedge_r DOUBLE, beta DOUBLE,
                beta_raw DOUBLE, beta_corr DOUBLE, long_ret_pct DOUBLE, hedge_ret_pct DOUBLE,
                long_return_r DOUBLE, beta_hedge_return_r DOUBLE, hedge_cost_r DOUBLE,
                net_return_r DOUBLE, gross_long_r DOUBLE, hedge_notional_r DOUBLE,
                net_beta_r DOUBLE, reason_json VARCHAR
            )
            """
        )
        hedge_ledger_rows = []
        for row in (payload.get("portfolio_hedge") or {}).get("ledger") or []:
            hedge_ledger_rows.append(
                [
                    row.get("as_of"),
                    row.get("signal_date"),
                    row.get("entry_date"),
                    row.get("exit_date"),
                    row.get("return_date"),
                    row.get("market"),
                    row.get("sleeve_id"),
                    row.get("symbol_or_basket"),
                    row.get("long_r"),
                    row.get("hedge_instrument"),
                    row.get("hedge_r"),
                    row.get("beta"),
                    row.get("beta_raw"),
                    row.get("beta_corr"),
                    row.get("long_ret_pct"),
                    row.get("hedge_ret_pct"),
                    row.get("long_return_r"),
                    row.get("beta_hedge_return_r"),
                    row.get("hedge_cost_r"),
                    row.get("net_return_r"),
                    row.get("gross_long_r"),
                    row.get("hedge_notional_r"),
                    row.get("net_beta_r"),
                    row.get("reason_json"),
                ]
            )
        if hedge_ledger_rows:
            con.executemany(
                "INSERT INTO portfolio_hedge_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                hedge_ledger_rows,
            )
    finally:
        con.close()


def _metric_float(value: Any) -> float | None:
    return v2.round_or_none(value)


def _note_metric_value(notes: str, name: str) -> float | None:
    marker = f"{name}="
    if marker not in notes:
        return None
    value = notes.split(marker, 1)[1].split(";", 1)[0].strip()
    if value in {"-", ""}:
        return None
    if value.endswith("%"):
        value = value[:-1]
    return _metric_float(value)


def factor_lab_money_gate_blockers(row: dict[str, Any]) -> list[str]:
    notes = str(row.get("notes") or "")
    blockers: list[str] = []

    lcb80 = _metric_float(row.get("lcb80_pct"))
    if lcb80 is not None and lcb80 <= 0:
        blockers.append("lcb80<=0")

    double_cost_lcb80 = _metric_float(row.get("double_cost_lcb80_pct"))
    if double_cost_lcb80 is None:
        double_cost_lcb80 = _note_metric_value(notes, "double_cost_lcb80")
    if double_cost_lcb80 is not None and double_cost_lcb80 <= 0:
        blockers.append("double_cost_lcb80<=0")

    top_share = _metric_float(row.get("top5_pnl_share"))
    if top_share is not None and top_share > 0.30:
        blockers.append("top5_pnl_share>30%")

    max_corr = _metric_float(row.get("max_abs_corr"))
    if max_corr is not None and max_corr >= FACTOR_LAB_CORR_BLOCK_THRESHOLD:
        blockers.append(f"corr>={FACTOR_LAB_CORR_BLOCK_THRESHOLD:.2f}")

    marginal = _metric_float(row.get("marginal_daily_sharpe_delta"))
    if marginal is not None and marginal <= 0:
        blockers.append("marginal_sharpe<=0")

    for marker in ("opportunity_flags=", "portfolio_flags="):
        if marker in notes:
            parsed = notes.split(marker, 1)[1].split(";", 1)[0]
            blockers.extend(item.strip() for item in parsed.split(",") if item.strip())

    return list(dict.fromkeys(blockers))


def alpha_factory_status_for_factor_lab(row: dict[str, Any]) -> tuple[str, list[str]]:
    status = str(row.get("money_status") or "research_only")
    blockers = factor_lab_money_gate_blockers(row)
    if status == "money_candidate":
        if blockers:
            return "blocked", blockers
        return "pass", []
    if status == "report_overlay":
        if blockers:
            return "blocked", blockers
        return "overlay_allowed", []
    if status == "research_only":
        return "research_only", ["report_contract_research_only"]
    if status == "research_only_sample_thin":
        return "blocked", ["sample_thin"]
    if status == "blocked_negative_or_unproven":
        return "blocked", ["lcb80<=0"]
    if status == "blocked_concentrated_pnl":
        return "blocked", ["top5_pnl_share>30%"]
    if status == "blocked_double_cost_lcb80":
        return "blocked", ["double_cost_lcb80<=0"]
    if status == "blocked_prob_sharpe":
        return "blocked", ["prob_positive<80%"]
    if status == "blocked_deflated_sharpe":
        return "blocked", ["deflated_lcb80<=0"]
    if status == "blocked_rolling_oos":
        return "blocked", ["rolling_oos_min_lcb80<=0"]
    if status == "blocked_factor_lab_portfolio_gate":
        notes = str(row.get("notes") or "")
        marker = "blockers="
        if marker in notes:
            parsed = notes.split(marker, 1)[1].split(";", 1)[0]
            blockers = [item.strip() for item in parsed.split(",") if item.strip()]
            return "blocked", blockers or ["portfolio_gate"]
        return "blocked", ["portfolio_gate"]
    return status, []


def _note_metric(notes: str, name: str) -> float | None:
    marker = f"{name}="
    if marker not in notes:
        return None
    value = notes.split(marker, 1)[1].split(";", 1)[0].strip()
    if value in {"-", ""}:
        return None
    if value.endswith("%"):
        value = value[:-1]
    return v2.round_or_none(value)


def write_factor_lab_money_gate_audit(factor_lab_db: Path, payload: dict[str, Any]) -> None:
    factor_rows = [
        row
        for row in payload.get("sleeves", [])
        if str(row.get("sleeve_id") or "").startswith("factor_lab_")
        and row.get("factor_id")
    ]
    if not factor_rows or not factor_lab_db.exists():
        return

    con = duckdb.connect(str(factor_lab_db))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS factor_money_gate_daily (
                as_of DATE NOT NULL,
                market VARCHAR NOT NULL,
                factor_id VARCHAR NOT NULL,
                sleeve_id VARCHAR NOT NULL,
                report_contract VARCHAR DEFAULT 'research_only',
                money_readiness VARCHAR DEFAULT 'research_only',
                alpha_factory_status VARCHAR NOT NULL,
                money_status VARCHAR NOT NULL,
                n INTEGER,
                lcb80_pct DOUBLE,
                double_cost_lcb80_pct DOUBLE,
                top5_pnl_share DOUBLE,
                max_abs_corr DOUBLE,
                marginal_daily_sharpe_delta DOUBLE,
                blockers_json VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (as_of, market, factor_id)
            )
            """
        )
        audit_rows = []
        registry_updates = []
        for row in factor_rows:
            notes = str(row.get("notes") or "")
            report_contract = "research_only"
            money_readiness = "research_only"
            if "contract=" in notes:
                report_contract = notes.split("contract=", 1)[1].split(";", 1)[0].strip() or report_contract
            if "readiness=" in notes:
                money_readiness = notes.split("readiness=", 1)[1].split(";", 1)[0].strip() or money_readiness
            alpha_status, blockers = alpha_factory_status_for_factor_lab(row)
            if alpha_status in {"overlay_allowed", "pass"} and report_contract != "research_only":
                registry_updates.append(
                    [
                        report_contract,
                        money_readiness,
                        row.get("market"),
                        row.get("factor_id"),
                    ]
                )
            audit_rows.append(
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("factor_id"),
                    row.get("sleeve_id"),
                    report_contract,
                    money_readiness,
                    alpha_status,
                    row.get("money_status"),
                    row.get("n"),
                    row.get("lcb80_pct"),
                    _note_metric(notes, "double_cost_lcb80"),
                    row.get("top5_pnl_share"),
                    row.get("max_abs_corr"),
                    row.get("marginal_daily_sharpe_delta"),
                    json.dumps(blockers, ensure_ascii=True, sort_keys=True),
                    notes,
                ]
            )
        con.executemany(
            """
            INSERT OR REPLACE INTO factor_money_gate_daily (
                as_of, market, factor_id, sleeve_id, report_contract,
                money_readiness, alpha_factory_status, money_status, n,
                lcb80_pct, double_cost_lcb80_pct, top5_pnl_share, max_abs_corr,
                marginal_daily_sharpe_delta, blockers_json, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            audit_rows,
        )
        if registry_updates:
            try:
                con.executemany(
                    """
                    UPDATE factor_registry
                    SET report_contract=?,
                        money_readiness=?
                    WHERE market=?
                      AND factor_id=?
                      AND status='promoted'
                    """,
                    registry_updates,
                )
            except duckdb.Error:
                pass
    finally:
        con.close()


def run(args: argparse.Namespace) -> dict[str, Any]:
    start = parse_date(args.start)
    as_of = parse_date(args.date) if args.date else latest_report_date(args.us_db, args.cn_db)
    sleeves = build_sleeves(args.us_db, args.cn_db, args.factor_lab_db, start, as_of, args.min_money_n)
    metrics = [sleeve.metrics_dict() for sleeve in sleeves]
    correlations = build_correlation_payload(sleeves)
    enrich_relationship_metrics(metrics, sleeves, correlations)
    apply_factor_lab_relationship_gates(metrics)
    calibration = build_calibration_payload(metrics, sleeves)
    promoted_sleeves = build_promotion_contract(metrics, calibration, as_of.isoformat(), start.isoformat())
    calibration["promotions"] = promoted_sleeves
    apply_promotion_contract_to_metrics(metrics, promoted_sleeves)
    sync_sleeve_statuses_from_metrics(sleeves, metrics)
    combo = build_combo_payload(sleeves)
    portfolio_hedge = build_portfolio_hedged_backtest(sleeves, args.us_db, args.cn_db, start, as_of)
    payload = {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "sleeves": metrics,
        "calibration": calibration,
        "promoted_sleeves": promoted_sleeves,
        "correlations": correlations,
        "combo": combo,
        "portfolio_hedge": portfolio_hedge,
        "daily_returns": {s.sleeve_id: daily_series(s.rows) for s in sleeves},
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    output_dir = args.output_root / payload["as_of"]
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "alpha_factory_backtest.md").write_text(render_report(payload), encoding="utf-8")
    (output_dir / "alpha_factory_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    write_duckdb(output_dir / "alpha_factory_backtest.duckdb", payload)
    write_factor_lab_money_gate_audit(args.factor_lab_db, payload)
    return payload


def main() -> None:
    args = parse_args()
    payload = run(args)
    print(
        "Alpha Factory sleeve backtest written: "
        f"{args.output_root / payload['as_of'] / 'alpha_factory_backtest.md'}"
    )


if __name__ == "__main__":
    main()
