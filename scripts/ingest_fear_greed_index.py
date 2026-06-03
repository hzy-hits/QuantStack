"""Fetch a daily Fear & Greed index reading for the macro context block.

Two-source design:

1. **Primary**: CNN public Fear & Greed dataset
   (`https://production.dataviz.cnn.io/index/fearandgreed/graphdata`). Returns
   a 0-100 score plus rating and component sub-indicators. No auth.

2. **Fallback**: internal three-factor proxy computed from the existing US
   DuckDB (`prices_daily`) — used when CNN is unreachable:
       - VIX percentile over the trailing 252 sessions, inverted
       - SPY close vs 50-day EMA distance
       - SPY 5-day return percentile vs trailing year
   Each component is normalised to [0, 100] and equal-weighted.

Output is cached for 1 hour at
`reports/review_dashboard/fear_greed/<as-of>/fear_greed.{json}` and consumed
by the daily report renderer.

Methodology constraint: the index is **macro / crowding context** only.
Per `ai_infra/docs/company-financials-market-options-methodology.md`, neither
K-line nor crowding signals can promote a name; they only support timing/risk.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "fear_greed"
CNN_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
CACHE_TTL_SECONDS = 3_600
CNN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.cnn.com/markets/fear-and-greed",
    "Origin": "https://www.cnn.com",
}


def _rating_for(score: float) -> str:
    if score <= 25:
        return "Extreme Fear"
    if score <= 45:
        return "Fear"
    if score <= 55:
        return "Neutral"
    if score <= 75:
        return "Greed"
    return "Extreme Greed"


def _parse_cnn_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    f_and_g = payload.get("fear_and_greed") or {}
    score = f_and_g.get("score")
    if score is None:
        return None
    try:
        score = float(score)
    except (TypeError, ValueError):
        return None
    return {
        "source": "cnn",
        "score": round(score, 2),
        "rating": f_and_g.get("rating") or _rating_for(score),
        "previous_close": f_and_g.get("previous_close"),
        "previous_1_week": f_and_g.get("previous_1_week"),
        "previous_1_month": f_and_g.get("previous_1_month"),
        "previous_1_year": f_and_g.get("previous_1_year"),
        "timestamp": f_and_g.get("timestamp"),
        "components": {
            name: payload.get(name) for name in (
                "market_momentum_sp500",
                "stock_price_strength",
                "stock_price_breadth",
                "put_call_options",
                "market_volatility_vix",
                "safe_haven_demand",
                "junk_bond_demand",
            )
        },
    }


def _try_cnn_urllib(timeout: int = 8) -> tuple[dict[str, Any] | None, str | None]:
    try:
        req = urllib.request.Request(CNN_URL, headers=CNN_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = json.loads(response.read())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        return None, f"urllib:{exc.__class__.__name__}: {exc}"
    parsed = _parse_cnn_payload(payload)
    if parsed is None:
        return None, "urllib:missing fear_and_greed.score"
    return parsed, None


def _try_cnn_curl(timeout: int = 12) -> tuple[dict[str, Any] | None, str | None]:
    cmd = [
        "curl",
        "-L",
        "--silent",
        "--show-error",
        "--max-time",
        str(timeout),
    ]
    for key, value in CNN_HEADERS.items():
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(CNN_URL)
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout + 2)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"curl:{exc.__class__.__name__}: {exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        return None, f"curl:returncode={proc.returncode}: {detail[-1] if detail else 'no output'}"
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        snippet = proc.stdout.strip().replace("\n", " ")[:120]
        return None, f"curl:JSONDecodeError: {exc}; body={snippet!r}"
    parsed = _parse_cnn_payload(payload)
    if parsed is None:
        return None, "curl:missing fear_and_greed.score"
    return parsed, None


def _try_cnn(timeout: int = 8) -> dict[str, Any] | None:
    errors: list[str] = []
    data, error = _try_cnn_urllib(timeout=timeout)
    if data is not None:
        return data
    if error:
        errors.append(error)

    data, error = _try_cnn_curl(timeout=max(timeout, 12))
    if data is not None:
        return data
    if error:
        errors.append(error)

    print(f"warn: CNN F&G fetch failed ({' | '.join(errors)}); falling back to proxy", file=sys.stderr)
    return None


def _percentile(values: list[float], target: float) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    below = sum(1 for v in sorted_vals if v <= target)
    return (below / n) * 100.0


def _load_close_series(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date, lookback_days: int = 400) -> list[tuple[date, float]]:
    rows = con.execute(
        """
        SELECT date, close
        FROM prices_daily
        WHERE symbol = ?
          AND date >= CAST(? AS DATE)
          AND date <= CAST(? AS DATE)
          AND close IS NOT NULL
        ORDER BY date
        """,
        [symbol, (as_of - timedelta(days=lookback_days)).isoformat(), as_of.isoformat()],
    ).fetchall()
    out: list[tuple[date, float]] = []
    for d, close in rows:
        if isinstance(d, str):
            try:
                d = date.fromisoformat(d)
            except ValueError:
                continue
        out.append((d, float(close)))
    return out


def _compute_proxy(us_db: Path, as_of: date) -> dict[str, Any] | None:
    if not us_db.exists():
        return None
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        vix = _load_close_series(con, "^VIX", as_of)
        spy = _load_close_series(con, "SPY", as_of)
    finally:
        con.close()
    if len(vix) < 60 or len(spy) < 60:
        return None

    components: dict[str, Any] = {}

    # 1. VIX percentile, inverted. Low VIX → high greed.
    vix_recent = [v for _, v in vix]
    vix_now = vix_recent[-1]
    vix_pct = _percentile(vix_recent[-252:], vix_now)
    vix_score = round(100.0 - (vix_pct or 50.0), 2)
    components["vix"] = {
        "level": round(vix_now, 2),
        "percentile_252d": round(vix_pct or 50.0, 2),
        "score": vix_score,
    }

    # 2. SPY vs 50d EMA distance. Above EMA → greed.
    spy_closes = [c for _, c in spy]
    alpha = 2.0 / (50 + 1)
    ema = spy_closes[0]
    for v in spy_closes[1:]:
        ema = alpha * v + (1 - alpha) * ema
    dist_pct = (spy_closes[-1] / ema - 1.0) * 100.0
    # Map roughly [-6%, +6%] → [0, 100]
    spy_ema_score = round(max(0.0, min(100.0, 50.0 + dist_pct * (50.0 / 6.0))), 2)
    components["spy_vs_ema50"] = {
        "ema50": round(ema, 2),
        "close": round(spy_closes[-1], 2),
        "distance_pct": round(dist_pct, 2),
        "score": spy_ema_score,
    }

    # 3. SPY 5d return percentile.
    rets = []
    for i in range(5, len(spy_closes)):
        prev = spy_closes[i - 5]
        if prev > 0:
            rets.append((spy_closes[i] - prev) / prev * 100.0)
    if rets:
        latest_ret = rets[-1]
        ret_pct = _percentile(rets[-252:], latest_ret)
    else:
        latest_ret = 0.0
        ret_pct = 50.0
    spy_5d_score = round(ret_pct or 50.0, 2)
    components["spy_5d_return"] = {
        "value_pct": round(latest_ret, 2),
        "percentile_252d": round(ret_pct or 50.0, 2),
        "score": spy_5d_score,
    }

    final_score = round((vix_score + spy_ema_score + spy_5d_score) / 3.0, 2)
    return {
        "source": "proxy",
        "source_note": "CNN official feed unavailable; internal VIX/SPY proxy only.",
        "score": final_score,
        "rating": _rating_for(final_score),
        "previous_close": None,
        "previous_1_week": None,
        "previous_1_month": None,
        "previous_1_year": None,
        "timestamp": as_of.isoformat(),
        "components": components,
    }


def _cache_path(output_root: Path, as_of: str) -> Path:
    return output_root / as_of / "fear_greed.json"


def _cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age < CACHE_TTL_SECONDS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-cnn", action="store_true", help="Skip CNN; use proxy only.")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cache; refetch.")
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of_text = args.as_of or cst.date().isoformat()
    as_of = date.fromisoformat(as_of_text)
    cache = _cache_path(args.output_root, as_of_text)

    if not args.no_cache and _cache_is_fresh(cache):
        print(f"Fear & Greed cached: {cache}")
        return 0

    data = None if args.no_cnn else _try_cnn()
    if data is None:
        data = _compute_proxy(args.us_db, as_of)
    if data is None:
        print("error: neither CNN nor proxy could produce a Fear & Greed score", file=sys.stderr)
        return 1

    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(
        f"Fear & Greed written: {cache}; source={data['source']} score={data['score']} rating={data['rating']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
