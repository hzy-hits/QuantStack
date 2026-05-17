"""Capitulation radar — the mirror of Wedge/Victim/Confirmation.

Wedge/Victim/Confirm reads the bubble *popping* (defensive). This radar reads
the *bottom* — selling exhaustion after a crash, when the convex move is to
stop pressing shorts and buy convex upside.

Five auto-computed signals (operator essay, 2026-05-17). Any 3 firing →
historically ~70% odds of a rebound over the next 3 months (Mar 2020, Oct
2022, Nov 2008):

  1. VIX peak-rollover : ^VIX hit 40+ recently and is now back below 30
  2. HY OAS peak       : credit spreads spiked then stopped widening
  3. Put/Call extreme  : cross-sectional median put/call >= 1.2 (panic)
  4. Volume exhaustion : SPY volume peaked then contracted (sellers spent)
  5. High-beta leadership : the highest-beta junk leads — dash-for-trash

This radar only computes. The risk-regime engine consumes its `capitulation`
flag as the 5th state (CAPITULATION) — stop pressing, flip to convex long.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_AI_INFRA_ROOT = STACK_ROOT / "ai_infra"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "capitulation_radar"

# Thresholds — one place so tests and operators can see them.
VIX_PANIC = 40.0           # VIX must have touched at/above this recently
VIX_CALMED = 30.0          # ... and now be back below this
HY_OAS_STRESS = 5.5        # HY OAS peak must have reached this (%)
HY_OAS_OFF_PEAK = 0.5      # ... and now be at least this far below the peak
PC_EXTREME = 1.2           # cross-sectional median put/call at/above = panic
VOL_EXHAUSTION_RATIO = 0.7 # latest SPY volume below this × the recent peak
BETA_LEADERSHIP_SPREAD = 2.0  # top-beta 5d return minus bottom-beta, pct points
CAPITULATION_MIN_SIGNALS = 3


def _sig_vix(vix_series: list[float]) -> dict[str, Any]:
    if len(vix_series) < 5:
        return {"fired": False, "detail": "VIX history insufficient"}
    peak = max(vix_series)
    now = vix_series[-1]
    fired = peak >= VIX_PANIC and now < VIX_CALMED
    return {
        "fired": fired,
        "detail": f"VIX peak {peak:.1f} / now {now:.1f} "
        f"(panic≥{VIX_PANIC:.0f} then <{VIX_CALMED:.0f})",
    }


def _sig_hy_oas(oas_series: list[float]) -> dict[str, Any]:
    if len(oas_series) < 5:
        return {"fired": False, "detail": "HY OAS history insufficient"}
    peak = max(oas_series)
    now = oas_series[-1]
    fired = peak >= HY_OAS_STRESS and now <= peak - HY_OAS_OFF_PEAK
    return {
        "fired": fired,
        "detail": f"HY OAS peak {peak:.2f}% / now {now:.2f}% "
        f"(stress≥{HY_OAS_STRESS:.1f}, off-peak≥{HY_OAS_OFF_PEAK:.1f})",
    }


def _sig_put_call(pc_median: float | None) -> dict[str, Any]:
    if pc_median is None:
        return {"fired": False, "detail": "put/call unavailable"}
    fired = pc_median >= PC_EXTREME
    return {
        "fired": fired,
        "detail": f"median put/call {pc_median:.2f} (panic≥{PC_EXTREME:.1f})",
    }


def _sig_volume(spy_vol: list[float]) -> dict[str, Any]:
    if len(spy_vol) < 8:
        return {"fired": False, "detail": "SPY volume history insufficient"}
    peak = max(spy_vol)
    peak_idx = spy_vol.index(peak)
    now = spy_vol[-1]
    last3 = spy_vol[-3:]
    contracting = last3[0] >= last3[1] >= last3[2]
    # peak must be in the past (not the last 2 bars) and volume now drained.
    fired = (
        peak_idx <= len(spy_vol) - 3
        and contracting
        and now <= VOL_EXHAUSTION_RATIO * peak
    )
    return {
        "fired": fired,
        "detail": f"SPY vol peak {peak:,.0f} / now {now:,.0f} "
        f"({'contracting' if contracting else 'not contracting'})",
    }


def _sig_beta_leadership(beta_rows: list[tuple[float, float]]) -> dict[str, Any]:
    """beta_rows = list of (beta, 5d_return_pct). High-beta junk leads = dash-for-trash."""
    rows = [(b, r) for b, r in beta_rows if b is not None and r is not None]
    if len(rows) < 8:
        return {"fired": False, "detail": "beta sample insufficient"}
    rows.sort(key=lambda x: x[0])
    q = max(1, len(rows) // 4)
    low_beta = rows[:q]
    high_beta = rows[-q:]
    low_ret = statistics.mean(r for _, r in low_beta)
    high_ret = statistics.mean(r for _, r in high_beta)
    spread = high_ret - low_ret
    # Dash-for-trash = junk actually RALLYING — require the high-beta cohort
    # to be positive, not merely "less negative" than defensives.
    fired = spread >= BETA_LEADERSHIP_SPREAD and high_ret > 0.0
    return {
        "fired": fired,
        "detail": f"high-beta 5d {high_ret:+.1f}% vs low-beta {low_ret:+.1f}% "
        f"(spread {spread:+.1f}pp, lead≥{BETA_LEADERSHIP_SPREAD:.1f} & high>0)",
    }


def evaluate_capitulation(
    vix_series: list[float],
    oas_series: list[float],
    pc_median: float | None,
    spy_vol: list[float],
    beta_rows: list[tuple[float, float]],
) -> dict[str, Any]:
    """Pure core: 5 signals → fired count → capitulation flag."""
    signals = {
        "vix_peak_rollover": _sig_vix(vix_series),
        "hy_oas_peak": _sig_hy_oas(oas_series),
        "put_call_extreme": _sig_put_call(pc_median),
        "volume_exhaustion": _sig_volume(spy_vol),
        "high_beta_leadership": _sig_beta_leadership(beta_rows),
    }
    fired = sorted(k for k, v in signals.items() if v["fired"])
    return {
        "signals": signals,
        "fired_count": len(fired),
        "fired_signals": fired,
        "capitulation": len(fired) >= CAPITULATION_MIN_SIGNALS,
        "min_signals": CAPITULATION_MIN_SIGNALS,
    }


# ── DB layer ─────────────────────────────────────────────────────────────────

def _close_series(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date, n: int) -> list[float]:
    rows = con.execute(
        "SELECT close FROM prices_daily WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT ?",
        [symbol, as_of.isoformat(), n],
    ).fetchall()
    return [float(r[0]) for r in reversed(rows) if r[0] is not None]


def _volume_series(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date, n: int) -> list[float]:
    rows = con.execute(
        "SELECT volume FROM prices_daily WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT ?",
        [symbol, as_of.isoformat(), n],
    ).fetchall()
    return [float(r[0]) for r in reversed(rows) if r[0] is not None]


def _hy_oas_series(con: duckdb.DuckDBPyConnection, as_of: date, n: int) -> list[float]:
    rows = con.execute(
        "SELECT value FROM macro_daily WHERE series_id = 'BAMLH0A0HYM2' AND date <= ? "
        "ORDER BY date DESC LIMIT ?",
        [as_of.isoformat(), n],
    ).fetchall()
    return [float(r[0]) for r in reversed(rows) if r[0] is not None]


def _pc_median(con: duckdb.DuckDBPyConnection, as_of: date) -> float | None:
    row = con.execute(
        "SELECT MEDIAN(pc_ratio_raw) FROM options_sentiment "
        "WHERE as_of = (SELECT MAX(as_of) FROM options_sentiment WHERE as_of <= ?) "
        "AND pc_ratio_raw IS NOT NULL",
        [as_of.isoformat()],
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _returns(closes: list[float]) -> list[float]:
    return [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(1, len(closes))
        if closes[i - 1]
    ]


def _beta_rows(
    con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date
) -> list[tuple[float, float]]:
    """For each symbol: (60d beta vs SPY, trailing 5d return %)."""
    spy = _close_series(con, "SPY", as_of, 65)
    spy_ret = _returns(spy)
    if len(spy_ret) < 30:
        return []
    var_spy = statistics.pvariance(spy_ret) if len(spy_ret) > 1 else 0.0
    out: list[tuple[float, float]] = []
    for sym in symbols:
        closes = _close_series(con, sym, as_of, 65)
        if len(closes) < 30:
            continue
        ret = _returns(closes)
        n = min(len(ret), len(spy_ret))
        if n < 30 or var_spy <= 0:
            continue
        a, b = ret[-n:], spy_ret[-n:]
        mean_a = statistics.mean(a)
        mean_b = statistics.mean(b)
        cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b)) / n
        beta = cov / var_spy
        ret_5d = (closes[-1] / closes[-6] - 1.0) * 100.0 if len(closes) >= 6 else None
        if ret_5d is not None:
            out.append((beta, ret_5d))
    return out


def _ai_universe_us_symbols(ai_infra_root: Path) -> list[str]:
    src = STACK_ROOT / "quant-research-v1" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        from quant_bot.analytics import ai_infra_universe as gate  # type: ignore
        return sorted(gate.records_by_symbol("US", ai_infra_root=ai_infra_root, pool="research"))
    except Exception:  # noqa: BLE001
        return []


def render_markdown(as_of: str, result: dict[str, Any]) -> str:
    cap = result["capitulation"]
    head = "**CAPITULATION 触发**" if cap else "未触发(市场未进入抛售衰竭)"
    lines = [
        f"# Capitulation Radar — 抄底信号 — {as_of}",
        "",
        f"## {result['fired_count']}/5 信号触发 — {head}",
        "",
        f"≥{result['min_signals']} 个同时触发 = 历史上未来 3 个月反弹概率约 70%。",
        "",
        "| 信号 | 触发 | 读数 |",
        "|---|---|---|",
    ]
    label = {
        "vix_peak_rollover": "VIX 见顶回落",
        "hy_oas_peak": "HY OAS 见顶",
        "put_call_extreme": "Put/Call 极值",
        "volume_exhaustion": "成交量峰后缩量",
        "high_beta_leadership": "高 beta 垃圾股领涨",
    }
    for key, sig in result["signals"].items():
        mark = "✅" if sig["fired"] else "—"
        lines.append(f"| {label.get(key, key)} | {mark} | {sig['detail']} |")
    lines += [
        "",
        "状态接入风控引擎:≥3 触发 → 第 5 状态 CAPITULATION(停止 press,翻多凸性)。",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-infra-root", type=Path, default=DEFAULT_AI_INFRA_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of_text = args.as_of or cst.date().isoformat()
    as_of = date.fromisoformat(as_of_text)

    if not args.us_db.exists():
        print(f"error: US db missing at {args.us_db}", file=sys.stderr)
        return 2

    symbols = _ai_universe_us_symbols(args.ai_infra_root)
    con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        vix = _close_series(con, "^VIX", as_of, 32)
        oas = _hy_oas_series(con, as_of, 42)
        pc = _pc_median(con, as_of)
        spy_vol = _volume_series(con, "SPY", as_of, 16)
        beta_rows = _beta_rows(con, symbols, as_of)
    finally:
        con.close()

    result = evaluate_capitulation(vix, oas, pc, spy_vol, beta_rows)
    out_dir = args.output_root / as_of_text
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": as_of_text,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **result,
    }
    (out_dir / "capitulation_radar.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / "capitulation_radar.md").write_text(
        render_markdown(as_of_text, result), encoding="utf-8"
    )
    print(
        f"capitulation radar: {result['fired_count']}/5 fired "
        f"(capitulation={result['capitulation']}) → {out_dir / 'capitulation_radar.json'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
