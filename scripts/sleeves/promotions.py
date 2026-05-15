"""Promotion-contract guardrails for production R assignment."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


class UnpromotedSleeveError(RuntimeError):
    """Raised when production code attempts to give R to an unpromoted sleeve."""


def promoted_sleeve_set(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    promoted: set[tuple[str, str]] = set()
    for row in rows:
        if str(row.get("status") or "").lower() != "promoted":
            continue
        market = str(row.get("market") or "").lower()
        sleeve_id = str(row.get("sleeve_id") or "")
        if market and sleeve_id:
            promoted.add((market, sleeve_id))
    return promoted


BOOTSTRAP_PROMOTED_SLEEVES = [
    {
        "market": "cn",
        "sleeve_id": "cn_oversold_ev_positive",
        "status": "promoted",
        "created_by": "bootstrap_existing_production_contract",
    },
    {
        "market": "us",
        "sleeve_id": "us_v2_stock_probe",
        "status": "promoted",
        "created_by": "bootstrap_existing_production_contract",
    },
]


TREND_MAINLINE_PROMOTED_SLEEVES = [
    {
        "market": "cn",
        "sleeve_id": "cn_tape_leadership_continuation",
        "status": "promoted",
        "gate_version": "operator_trend_mainline_v1",
        "created_by": "operator_directive_2026_05_11",
        "gates_snapshot_json": (
            '{"contract":"current tape leadership can receive R only after price, volume, '
            'flow and sector synchronization pass the sleeve query"}'
        ),
    },
    {
        "market": "us",
        "sleeve_id": "us_theme_cluster_momentum",
        "status": "promoted",
        "gate_version": "operator_trend_mainline_v1",
        "created_by": "operator_directive_2026_05_11",
        "gates_snapshot_json": (
            '{"contract":"theme basket momentum can receive stock R only after basket breadth, '
            'price/volume and options/flow confirmation pass the sleeve query"}'
        ),
    },
    # AI infra production-core sleeve: members of ai_infra/data/global_universe_v2.jsonl
    # whose evidence_state contains 原文已证明 / 合理推论 are operator-curated for
    # execution. The pool filter (ai_infra_universe.is_production_grade) is the
    # gate; this row authorises R assignment once the candidate clears it.
    {
        "market": "us",
        "sleeve_id": "ai_infra_production_core",
        "status": "promoted",
        "gate_version": "ai_infra_production_pool_v1",
        "created_by": "operator_directive_2026_05_14",
        "gates_snapshot_json": (
            '{"contract":"AI infra production-pool members (evidence_state 原文已证明 / '
            '合理推论) receive R after tape, options/flow, headline and portfolio overlay '
            'checks pass; production pool gate is the operator-approved evidence contract"}'
        ),
    },
    {
        "market": "cn",
        "sleeve_id": "ai_infra_production_core",
        "status": "promoted",
        "gate_version": "ai_infra_production_pool_v1",
        "created_by": "operator_directive_2026_05_14",
        "gates_snapshot_json": (
            '{"contract":"AI infra production-pool members (evidence_state 原文已证明 / '
            '合理推论) receive R after tape, observed-lifecycle and portfolio overlay '
            'checks pass; production pool gate is the operator-approved evidence contract"}'
        ),
    },
]


def with_trend_mainline_overrides(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Append explicit operator-promoted trend sleeves without hiding old rejects.

    Historical alpha-factory rows may still contain a rejected calibration row
    for these sleeves. Keeping both rows preserves the old evidence, while the
    promoted row makes the current production contract explicit and auditable.
    """
    out = [dict(row) for row in rows]
    promoted = promoted_sleeve_set(out)
    for row in TREND_MAINLINE_PROMOTED_SLEEVES:
        key = (str(row.get("market") or "").lower(), str(row.get("sleeve_id") or ""))
        if key not in promoted:
            out.append(dict(row))
            promoted.add(key)
    return out


def load_promoted_sleeves(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        exists = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name='promoted_sleeves'
            """
        ).fetchone()[0]
        if not exists:
            return []
        cols = {row[0] for row in con.execute("DESCRIBE promoted_sleeves").fetchall()}
        optional = {
            "as_of": "as_of" if "as_of" in cols else "NULL AS as_of",
            "start_date": "start_date" if "start_date" in cols else "NULL AS start_date",
            "gate_version": "gate_version" if "gate_version" in cols else "NULL AS gate_version",
            "created_by": "created_by" if "created_by" in cols else "NULL AS created_by",
            "promoted_at": "promoted_at" if "promoted_at" in cols else "NULL AS promoted_at",
            "gates_snapshot_json": "gates_snapshot_json" if "gates_snapshot_json" in cols else "NULL AS gates_snapshot_json",
        }
        rows = con.execute(
            f"""
            SELECT
                {optional['as_of']},
                {optional['start_date']},
                market,
                sleeve_id,
                status,
                {optional['gate_version']},
                {optional['created_by']},
                {optional['promoted_at']},
                {optional['gates_snapshot_json']}
            FROM promoted_sleeves
            """
        ).fetchall()
        return [
            {
                "as_of": row[0],
                "start_date": row[1],
                "market": row[2],
                "sleeve_id": row[3],
                "status": row[4],
                "gate_version": row[5],
                "created_by": row[6],
                "promoted_at": row[7],
                "gates_snapshot_json": row[8],
            }
            for row in rows
        ]
    finally:
        con.close()


def latest_alpha_factory_db(root: Path, as_of: str | None = None) -> Path | None:
    base = root / "reports" / "review_dashboard" / "alpha_factory"
    if as_of:
        candidate = base / as_of / "alpha_factory_backtest.duckdb"
        if candidate.exists():
            return candidate
    if not base.exists():
        return None
    candidates = sorted(base.glob("*/alpha_factory_backtest.duckdb"), reverse=True)
    return candidates[0] if candidates else None


def assert_sleeve_promoted(
    *,
    market: str,
    sleeve_id: str | None,
    promoted_rows: list[dict[str, Any]],
) -> None:
    sleeve = str(sleeve_id or "")
    if not sleeve:
        raise UnpromotedSleeveError("missing alpha_sleeve_id")
    key = (str(market or "").lower(), sleeve)
    if key not in promoted_sleeve_set(promoted_rows):
        raise UnpromotedSleeveError(f"unpromoted sleeve cannot receive R: {key[0]}:{key[1]}")


def is_sleeve_promoted(*, market: str, sleeve_id: str | None, promoted_rows: list[dict[str, Any]]) -> bool:
    sleeve = str(sleeve_id or "")
    if not sleeve:
        return False
    return (str(market or "").lower(), sleeve) in promoted_sleeve_set(promoted_rows)
