"""CN ingestion freshness gate.

Reads ops/fetch_sources.yaml (policy) + fetch_state from the CN hot DB (actual),
classifies each source fresh/stale by criticality. Exit 0 if no critical source
is stale; exit 1 (fail-closed) otherwise. On critical-stale, optionally emails an
operator (only with --alert AND QUANT_OPERATOR_EMAIL set; never in shadow/tests).

Usage:
    python3 ops/freshness_gate.py --market cn
    python3 ops/freshness_gate.py --market cn --alert
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import yaml

STACK_ROOT = Path(__file__).resolve().parents[1]
REGISTRY = STACK_ROOT / "ops" / "fetch_sources.yaml"
CN_HOT = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"


def evaluate_freshness(sources, state_by_fetcher, today):
    """Return (ok, critical_stale, optional_stale).

    A source is stale if: no fetch_state row, status != 'ok', as_of missing, or
    as_of older than today - max_staleness_days. ok == no critical source stale.
    """
    critical_stale, optional_stale = [], []
    for s in sources:
        row = state_by_fetcher.get(s["source"])
        as_of = row.get("as_of") if row else None
        stale = (
            row is None
            or row.get("status") != "ok"
            or as_of is None
            or (today - as_of).days > int(s["max_staleness_days"])
        )
        if stale:
            if s["criticality"] == "critical":
                critical_stale.append(s["source"])
            else:
                optional_stale.append(s["source"])
    return (len(critical_stale) == 0, critical_stale, optional_stale)


def _load_state(hot_path):
    import duckdb
    if not Path(hot_path).exists():
        return {}
    con = duckdb.connect(str(hot_path), read_only=True)
    try:
        try:
            rows = con.execute(
                "SELECT fetcher, status, as_of FROM fetch_state WHERE market='cn'"
            ).fetchall()
        except duckdb.CatalogException:
            return {}  # table not created yet (no worker has run)
        return {r[0]: {"status": r[1], "as_of": r[2]} for r in rows}
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["cn"])
    ap.add_argument("--alert", action="store_true", help="email operator on critical-stale")
    ap.add_argument("--hot", default=str(CN_HOT))
    args = ap.parse_args()

    sources = (yaml.safe_load(REGISTRY.read_text(encoding="utf-8")) or {}).get(args.market, [])
    state = _load_state(args.hot)
    today = dt.datetime.now().date()
    ok, critical_stale, optional_stale = evaluate_freshness(sources, state, today)

    print(f"freshness {args.market}: ok={ok} critical_stale={critical_stale} optional_stale={optional_stale}")
    if not ok and args.alert:
        operator = os.environ.get("QUANT_OPERATOR_EMAIL", "").strip()
        if operator:
            try:
                sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
                from quant_bot.delivery.gmail import send_alert_email
                send_alert_email(
                    operator,
                    f"[检修] CN 取数关键源过期: {', '.join(critical_stale)}",
                    f"freshness gate fail-closed at {today}. critical_stale={critical_stale}. "
                    f"CN report suppressed. Check fetch workers + consolidate.",
                )
                print(f"alert sent to {operator}")
            except Exception as e:  # noqa: BLE001 — alert must never crash the gate
                print(f"alert send failed: {e}", file=sys.stderr)
        else:
            print("QUANT_OPERATOR_EMAIL unset; skipping alert email", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
