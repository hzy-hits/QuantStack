#!/usr/bin/env python3
"""Fetch and normalize one free OnclickMedia historical options-chain sample.

This is intentionally a probe, not production ingestion. It verifies that the
free API can supply the fields needed by our existing `options_chain_quotes`
schema: bid/ask/mid, volume, open interest, IV and Greeks.
"""
from __future__ import annotations

import argparse
import csv
import json
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "free_options_data_probe"
BASE_URL = "https://api.onclickmedia.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", default="SPY")
    parser.add_argument("--date", dest="as_of", default="2025-01-13")
    parser.add_argument("--expiration", default="2025-01-17")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def fetch_json(path: str, params: dict[str, str]) -> Any:
    query = urllib.parse.urlencode(params, safe="+")
    url = f"{BASE_URL}{path}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "quant-stack-onclick-probe/1.0"})
    with urllib.request.urlopen(request, timeout=45) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw), url


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _days_to_exp(as_of: str, expiration: str) -> int:
    return (date.fromisoformat(expiration) - date.fromisoformat(as_of)).days


def normalize_rows(rows: list[dict[str, Any]], current_price: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        greeks = row.get("greeks") or {}
        option_type = str(row.get("type") or "").lower()
        as_of = str(row.get("date") or "")
        expiration = str(row.get("expiration") or "")
        strike = _float(row.get("strike"))
        if not as_of or not expiration or option_type not in {"call", "put"} or strike is None:
            continue
        out.append(
            {
                "symbol": str(row.get("symbol") or "").upper(),
                "as_of": as_of,
                "expiry": expiration,
                "days_to_exp": _days_to_exp(as_of, expiration),
                "current_price": current_price,
                "contract_symbol": row.get("contract_id"),
                "option_type": option_type,
                "strike": strike,
                "bid": _float(row.get("bid")),
                "ask": _float(row.get("ask")),
                "mid": _float(row.get("mark")),
                "last_price": _float(row.get("last")),
                "volume": _int(row.get("volume")),
                "open_interest": _int(row.get("open_interest")),
                "implied_volatility": _float(greeks.get("implied_volatility")),
                "delta": _float(greeks.get("delta")),
                "gamma": _float(greeks.get("gamma")),
                "theta": _float(greeks.get("theta")),
                "vega": _float(greeks.get("vega")),
                "source": "onclickmedia_free_probe",
            }
        )
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    ticker = args.ticker.upper()
    as_of = args.as_of
    expiration = args.expiration
    out_dir = args.output_root / ticker / as_of / expiration
    out_dir.mkdir(parents=True, exist_ok=True)

    stock_rows, stock_url = fetch_json(
        "/stock-data/",
        {"ticker": ticker, "from": as_of, "to": as_of, "data": "all", "output": "json"},
    )
    if not stock_rows:
        raise SystemExit(f"no stock rows for {ticker} {as_of}")
    current_price = float(stock_rows[0]["close"])

    raw_by_type: dict[str, list[dict[str, Any]]] = {}
    urls: dict[str, str] = {"stock": stock_url}
    for option_type in ("call", "put"):
        rows, url = fetch_json(
            "/options/",
            {
                "ticker": ticker,
                "date": as_of,
                "expiration": expiration,
                "type": option_type,
                "data": "all+date",
                "output": "json-v1",
            },
        )
        raw_by_type[option_type] = rows
        urls[option_type] = url
        (out_dir / f"raw_{option_type}.json").write_text(
            json.dumps(rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    (out_dir / "raw_stock.json").write_text(
        json.dumps(stock_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    normalized = normalize_rows(raw_by_type["call"] + raw_by_type["put"], current_price)
    write_csv(out_dir / "normalized_options_chain_quotes.csv", normalized)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "OnclickMedia free API",
        "ticker": ticker,
        "as_of": as_of,
        "expiration": expiration,
        "current_price": current_price,
        "urls": urls,
        "raw_counts": {key: len(value) for key, value in raw_by_type.items()},
        "normalized_rows": len(normalized),
        "gamma_nonnull": sum(1 for row in normalized if row.get("gamma") is not None),
        "open_interest_total": sum(int(row.get("open_interest") or 0) for row in normalized),
        "fields": list(normalized[0].keys()) if normalized else [],
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
