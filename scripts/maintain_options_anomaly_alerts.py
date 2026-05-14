"""Daily options anomaly alerts ledger + idempotent source-queue annotator.

Two outputs, both keyed by `(as_of, ticker, side)` so re-runs are safe:

1. `ai_infra/reports/ai_book_options_alerts.jsonl` — append-only history of
   every options anomaly that crosses the alert threshold. Each line is a
   compact JSON record { as_of, ticker, side, score, vol_oi, pc_z, skew_z,
   spot, vol, oi }. Downstream tooling (cron monitors, weekly review) can
   diff/tail this file.

2. `ai_infra/reports/source_verification_queue_v1.csv` — `counterevidence`
   column gets an inline tag like
       `[options-flow-alert 2026-05-13: squeeze_score=124973, vol_oi=11.69]`
   for any ticker in the queue whose score exceeds the threshold. The tag is
   idempotent: re-runs replace the existing same-date tag rather than
   stacking duplicates. The original counterevidence prose is preserved.

Alert thresholds (configurable via CLI):
- squeeze: score ≥ 5000 OR (vol_oi ≥ 3.0 AND score ≥ 1500)
- pressure: score ≥ 3000 OR (vol_oi ≥ 3.0 AND score ≥ 1000)

Hard rule (per methodology): an options anomaly is **tape / crowding** context
only. The queue annotation is a *note*, not a promotion.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RADAR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_options_anomaly_radar"
DEFAULT_ALERTS_JSONL = STACK_ROOT / "ai_infra" / "reports" / "ai_book_options_alerts.jsonl"
DEFAULT_QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"

ALERT_TAG_PATTERN = re.compile(r"\s*\|\s*\[options-flow-alert (?P<date>\d{4}-\d{2}-\d{2})[^\]]*\]")


def _parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _qualifies_squeeze(score: float, vol_oi: float | None, *, score_floor: float, score_strong_oi: float, vol_oi_floor: float) -> bool:
    if score >= score_floor:
        return True
    if vol_oi is not None and vol_oi >= vol_oi_floor and score >= score_strong_oi:
        return True
    return False


def _qualifies_pressure(score: float, vol_oi: float | None, *, score_floor: float, score_strong_oi: float, vol_oi_floor: float) -> bool:
    return _qualifies_squeeze(score, vol_oi, score_floor=score_floor, score_strong_oi=score_strong_oi, vol_oi_floor=vol_oi_floor)


def collect_alerts(
    radar_csv: Path,
    *,
    squeeze_score_floor: float = 5000.0,
    pressure_score_floor: float = 3000.0,
    squeeze_strong_oi_score: float = 1500.0,
    pressure_strong_oi_score: float = 1000.0,
    vol_oi_floor: float = 3.0,
) -> list[dict[str, Any]]:
    if not radar_csv.exists():
        return []
    out: list[dict[str, Any]] = []
    with radar_csv.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = (row.get("symbol") or "").strip().upper()
            if not ticker:
                continue
            as_of = (row.get("as_of") or "").strip()
            spot = _parse_float(row.get("spot_close"))
            pc_z = _parse_float(row.get("pc_ratio_z"))
            skew_z = _parse_float(row.get("skew_z"))
            squeeze = _parse_float(row.get("short_squeeze_score")) or 0.0
            call_vol = int(row.get("far_otm_call_volume") or 0)
            call_oi = int(row.get("far_otm_call_oi") or 0)
            call_vol_oi = _parse_float(row.get("far_otm_call_vol_oi_ratio"))
            if _qualifies_squeeze(
                squeeze, call_vol_oi,
                score_floor=squeeze_score_floor,
                score_strong_oi=squeeze_strong_oi_score,
                vol_oi_floor=vol_oi_floor,
            ):
                out.append({
                    "as_of": as_of,
                    "ticker": ticker,
                    "side": "squeeze",
                    "score": round(squeeze, 2),
                    "vol": call_vol,
                    "oi": call_oi,
                    "vol_oi": round(call_vol_oi, 3) if call_vol_oi is not None else None,
                    "pc_z": round(pc_z, 3) if pc_z is not None else None,
                    "skew_z": round(skew_z, 3) if skew_z is not None else None,
                    "spot": round(spot, 2) if spot is not None else None,
                })
            pressure = _parse_float(row.get("selling_pressure_score")) or 0.0
            put_vol = int(row.get("far_otm_put_volume") or 0)
            put_oi = int(row.get("far_otm_put_oi") or 0)
            put_vol_oi = _parse_float(row.get("far_otm_put_vol_oi_ratio"))
            if _qualifies_pressure(
                pressure, put_vol_oi,
                score_floor=pressure_score_floor,
                score_strong_oi=pressure_strong_oi_score,
                vol_oi_floor=vol_oi_floor,
            ):
                out.append({
                    "as_of": as_of,
                    "ticker": ticker,
                    "side": "pressure",
                    "score": round(pressure, 2),
                    "vol": put_vol,
                    "oi": put_oi,
                    "vol_oi": round(put_vol_oi, 3) if put_vol_oi is not None else None,
                    "pc_z": round(pc_z, 3) if pc_z is not None else None,
                    "skew_z": round(skew_z, 3) if skew_z is not None else None,
                    "spot": round(spot, 2) if spot is not None else None,
                })
    out.sort(key=lambda r: (r["as_of"], r["ticker"], r["side"]))
    return out


def merge_alerts_jsonl(path: Path, new_alerts: list[dict[str, Any]]) -> tuple[int, int]:
    """Append new alert rows; idempotent on (as_of, ticker, side). Returns (added, refreshed)."""
    existing: dict[tuple[str, str, str], dict[str, Any]] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = (str(row.get("as_of") or ""), str(row.get("ticker") or "").upper(), str(row.get("side") or ""))
                if all(key):
                    existing[key] = row
    added = 0
    refreshed = 0
    for alert in new_alerts:
        key = (alert["as_of"], alert["ticker"], alert["side"])
        prior = existing.get(key)
        if prior is None:
            existing[key] = alert
            added += 1
        elif prior != alert:
            existing[key] = alert
            refreshed += 1
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(existing.values(), key=lambda r: (r.get("as_of") or "", r.get("ticker") or "", r.get("side") or ""))
    with path.open("w", encoding="utf-8") as handle:
        for row in ordered:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return added, refreshed


def _format_tag(alert: dict[str, Any]) -> str:
    bits = [f"{alert['side']}_score={alert['score']:.0f}"]
    if alert.get("vol_oi") is not None:
        bits.append(f"vol_oi={alert['vol_oi']:.2f}")
    if alert.get("pc_z") is not None:
        bits.append(f"pc_z={alert['pc_z']:+.2f}")
    if alert.get("skew_z") is not None:
        bits.append(f"skew_z={alert['skew_z']:+.2f}")
    return f" | [options-flow-alert {alert['as_of']}: {', '.join(bits)}]"


def annotate_queue(queue_path: Path, alerts: list[dict[str, Any]]) -> tuple[int, int]:
    """Idempotently merge today's alerts into the queue counterevidence column.

    Returns (rows_touched, distinct_tickers_touched).
    """
    if not queue_path.exists() or not alerts:
        return 0, 0

    with queue_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    queue_index: dict[str, list[int]] = {}
    for idx, row in enumerate(rows):
        for piece in (row.get("ticker") or "").split("/"):
            queue_index.setdefault(piece.strip().upper(), []).append(idx)

    # Group alerts by ticker so a name gets a single combined tag per date.
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    as_of_dates: set[str] = set()
    for alert in alerts:
        by_ticker.setdefault(alert["ticker"], []).append(alert)
        as_of_dates.add(alert["as_of"])

    touched_rows = 0
    touched_tickers = 0
    for ticker, ticker_alerts in by_ticker.items():
        matches = queue_index.get(ticker)
        if not matches:
            continue
        touched_tickers += 1
        tag_payload = "; ".join(
            f"{a['side']}_score={a['score']:.0f}"
            + (f" vol_oi={a['vol_oi']:.2f}" if a.get("vol_oi") is not None else "")
            for a in sorted(ticker_alerts, key=lambda a: a["side"])
        )
        # All alerts share a date (this script runs per-day).
        as_of = ticker_alerts[0]["as_of"]
        tag = f" | [options-flow-alert {as_of}: {tag_payload}]"
        for idx in matches:
            row = rows[idx]
            base = row.get("counterevidence") or ""
            # Drop any prior tag for the *same date* so re-runs do not stack duplicates.
            cleaned = re.sub(
                rf"\s*\|\s*\[options-flow-alert {re.escape(as_of)}[^\]]*\]", "", base
            ).rstrip()
            row["counterevidence"] = cleaned + tag
            touched_rows += 1

    backup = queue_path.with_suffix(queue_path.suffix + ".bak")
    shutil.copy2(queue_path, backup)
    with queue_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return touched_rows, touched_tickers


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--radar-root", type=Path, default=DEFAULT_RADAR_ROOT)
    parser.add_argument("--radar-csv", type=Path, default=None)
    parser.add_argument("--alerts-jsonl", type=Path, default=DEFAULT_ALERTS_JSONL)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--no-queue-annotation", action="store_true",
                        help="Skip mutating the queue; only refresh the alerts JSONL.")
    parser.add_argument("--squeeze-score-floor", type=float, default=5000.0)
    parser.add_argument("--pressure-score-floor", type=float, default=3000.0)
    parser.add_argument("--vol-oi-floor", type=float, default=3.0)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    radar_csv = args.radar_csv or (args.radar_root / as_of / "options_anomaly.csv")
    if not radar_csv.exists():
        print(f"warn: radar CSV missing at {radar_csv}; nothing to do", file=sys.stderr)
        return 0

    alerts = collect_alerts(
        radar_csv,
        squeeze_score_floor=args.squeeze_score_floor,
        pressure_score_floor=args.pressure_score_floor,
        vol_oi_floor=args.vol_oi_floor,
    )
    added, refreshed = merge_alerts_jsonl(args.alerts_jsonl, alerts)
    queue_rows = 0
    queue_tickers = 0
    if not args.no_queue_annotation:
        queue_rows, queue_tickers = annotate_queue(args.queue, alerts)
    print(
        f"options alerts ledger: alerts={len(alerts)} jsonl_added={added} jsonl_refreshed={refreshed}; "
        f"queue annotated rows={queue_rows} tickers={queue_tickers}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
