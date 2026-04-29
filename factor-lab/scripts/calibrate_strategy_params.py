#!/usr/bin/env python3
"""Generate strategy parameter provenance and calibration artifacts.

This script is intentionally lightweight enough for the weekend maintenance job:
it reads completed paper-trade / model tables, writes a YAML artifact, and marks
which parameters are statistical, cost assumptions, walk-forward calibrated, or
legacy heuristics still waiting to be replaced.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.paths import FACTOR_LAB_ROOT, QUANT_CN_REPORT_DB, QUANT_CN_ROOT


EV80_Z = 1.2816
EV95_Z = 1.6449


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table],
        ).fetchone()[0]
        > 0
    )


def latest_as_of(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    if not table_exists(con, table):
        return None
    row = con.execute(f"SELECT MAX(CAST(as_of AS VARCHAR)) FROM {table}").fetchone()
    return row[0] if row and row[0] else None


def fetch_strategy_ev(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    as_of = latest_as_of(con, "strategy_ev")
    if not as_of:
        return {"status": "missing", "as_of": None, "families": []}
    rows = con.execute(
        """
        SELECT
            strategy_key, strategy_family, samples, fills, fill_rate, win_rate_bayes,
            ev_pct, ev_lcb_80_pct, ev_lcb_95_pct, risk_unit_pct,
            ev_norm_score, ev_norm_lcb_80, eligible, fail_reasons
        FROM strategy_ev
        WHERE as_of = CAST(? AS DATE)
        ORDER BY ev_lcb_80_pct DESC NULLS LAST, ev_pct DESC NULLS LAST
        LIMIT 30
        """,
        [as_of],
    ).fetchall()
    families = []
    for row in rows:
        families.append(
            {
                "strategy_key": row[0],
                "strategy_family": row[1],
                "samples": row[2],
                "fills": row[3],
                "fill_rate": row[4],
                "win_rate_bayes": row[5],
                "ev_pct": row[6],
                "ev_lcb_80_pct": row[7],
                "ev_lcb_95_pct": row[8],
                "risk_unit_pct": row[9],
                "ev_norm_score": row[10],
                "ev_norm_lcb_80": row[11],
                "eligible": bool(row[12]),
                "fail_reasons": row[13],
            }
        )
    return {"status": "ok", "as_of": as_of, "families": families}


def fetch_limit_up_model(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    as_of = latest_as_of(con, "limit_up_model_performance")
    if not as_of:
        return {"status": "missing", "as_of": None}
    perf = con.execute(
        """
        SELECT model_state, train_start, train_end, train_samples, train_positives,
               auc, brier, top_decile_hit_rate, top_decile_lift,
               failed_board_rate, avg_next_ret_pct
        FROM limit_up_model_performance
        WHERE as_of = CAST(? AS DATE)
        """,
        [as_of],
    ).fetchone()
    decision_counts: list[dict[str, Any]] = []
    if table_exists(con, "limit_up_model_predictions"):
        for state, count in con.execute(
            """
            SELECT decision_state, COUNT(*)
            FROM limit_up_model_predictions
            WHERE as_of = CAST(? AS DATE)
            GROUP BY decision_state
            ORDER BY COUNT(*) DESC
            """,
            [as_of],
        ).fetchall():
            decision_counts.append({"decision_state": state, "count": count})
    return {
        "status": "ok",
        "as_of": as_of,
        "model_state": perf[0],
        "train_start": str(perf[1]) if perf[1] is not None else None,
        "train_end": str(perf[2]) if perf[2] is not None else None,
        "train_samples": perf[3],
        "train_positives": perf[4],
        "auc": perf[5],
        "brier": perf[6],
        "top_decile_hit_rate": perf[7],
        "top_decile_lift": perf[8],
        "failed_board_rate": perf[9],
        "avg_next_ret_pct": perf[10],
        "decision_counts": decision_counts,
    }


def parameter_registry() -> list[dict[str, Any]]:
    return [
        {
            "file": "quant-research-cn/src/analytics/paper_trade_ev.rs",
            "params": [
                {
                    "name": "LOOKBACK_DAYS",
                    "value": 90,
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                    "notes": "Should be selected by OOS EV LCB, not by taste.",
                },
                {
                    "name": "SLIPPAGE_PCT",
                    "value": 0.18,
                    "provenance": "cost_assumption",
                    "migration": "broker_cost_and_fill_model",
                    "notes": "Needs real fill/slippage audit when available.",
                },
                {
                    "name": "WIN_THRESHOLD_PCT",
                    "value": 0.50,
                    "provenance": "legacy_heuristic",
                    "migration": "strategy_family_specific_outcome_bins",
                },
                {
                    "name": "LOSS_THRESHOLD_PCT",
                    "value": -1.00,
                    "provenance": "legacy_heuristic",
                    "migration": "strategy_family_specific_outcome_bins",
                },
                {
                    "name": "EV80_Z",
                    "value": EV80_Z,
                    "provenance": "statistical",
                    "notes": "One-sided 80% normal lower-confidence z.",
                },
                {
                    "name": "EV95_Z",
                    "value": EV95_Z,
                    "provenance": "statistical",
                    "notes": "One-sided 95% normal lower-confidence z.",
                },
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/limit_up_model.rs",
            "params": [
                {
                    "name": "TRAIN_LOOKBACK_DAYS",
                    "value": 540,
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                },
                {
                    "name": "COST_PCT",
                    "value": 0.20,
                    "provenance": "cost_assumption",
                    "migration": "limit_strategy_fill_cost_audit",
                },
                {
                    "name": "NEGATIVE_SAMPLE_DENOM",
                    "value": 24,
                    "provenance": "statistical",
                    "notes": "Rare-event class-balance control for logistic training.",
                },
            ],
        },
        {
            "file": "quant-research-cn/src/filtering/notable.rs",
            "params": [
                {
                    "name": "composite/report/chase weights",
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                    "notes": "Should not be treated as alpha proof.",
                }
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/continuation_vs_fade.rs",
            "params": [
                {
                    "name": "continuation/fade score weights",
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                }
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/open_execution_gate.rs",
            "params": [
                {
                    "name": "open execution score thresholds",
                    "provenance": "legacy_heuristic",
                    "migration": "fill_model_and_mae_calibration",
                }
            ],
        },
    ]


def build_artifact(market: str, db_path: Path) -> dict[str, Any]:
    if market != "cn":
        raise ValueError("Only --market cn is wired for this calibration artifact today.")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market": market,
            "source_db": str(db_path),
            "principles": [
                "Report ranking should use realized strategy EV, not opaque setup score.",
                "Every numeric threshold must carry provenance.",
                "Legacy heuristics are allowed only as defaults until walk-forward calibration replaces them.",
                "Execution Alpha requires positive EV lower confidence bound, not just high hit rate.",
            ],
            "ev80_lcb": {
                "definition": "one-sided 80% lower confidence bound of strategy EV",
                "formula": "ev_lcb_80 = ev_pct - 1.2816 * max(realized_std_pct, risk_unit_pct) / sqrt(fills)",
                "z": EV80_Z,
                "interpretation": "A conservative haircut on average EV. It is not win rate and not a guarantee.",
                "release_rule": "ev_lcb_80_pct > 0 means the strategy remains positive after an 80% confidence haircut.",
            },
            "paper_trade_ev": {
                "current_release_rule": {
                    "min_ev_pct": 0.15,
                    "min_ev_lcb_80_pct": 0.0,
                    "min_fill_rate": 0.35,
                    "provenance": "mixed: statistical confidence bound plus legacy engineering guards",
                },
                "latest": fetch_strategy_ev(con),
            },
            "limit_up_model": fetch_limit_up_model(con),
            "parameter_registry": parameter_registry(),
        }
    finally:
        con.close()


def output_paths(market: str) -> list[Path]:
    paths = [
        FACTOR_LAB_ROOT
        / "runtime"
        / "strategy_calibration"
        / market
        / "strategy_params.generated.yaml"
    ]
    if market == "cn":
        paths.append(QUANT_CN_ROOT / "config" / "strategy_params.generated.yaml")
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate strategy parameter calibration artifact.")
    parser.add_argument("--market", choices=["cn"], default="cn")
    parser.add_argument("--dry-run", action="store_true", help="Print artifact without writing files.")
    parser.add_argument("--output", type=Path, help="Optional single output path.")
    args = parser.parse_args()

    db_path = QUANT_CN_REPORT_DB if args.market == "cn" else None
    if db_path is None or not db_path.exists():
        print(f"ERROR: report database not found: {db_path}", file=sys.stderr)
        return 1

    artifact = build_artifact(args.market, db_path)
    text = yaml.safe_dump(artifact, sort_keys=False, allow_unicode=True)
    if args.dry_run:
        print(text)
        return 0

    targets = [args.output] if args.output else output_paths(args.market)
    for path in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
