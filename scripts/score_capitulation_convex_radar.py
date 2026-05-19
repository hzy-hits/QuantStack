"""Capitulation convex radar — the upside mirror of victim_put.

victim_put expresses the downside (buy OTM puts on bubble victims). This
expresses the *upside* at a wash-out bottom — the convex long.

Two parts:

A. Convex value buys (computed every day): production-pool names (evidence
   already 原文已证明 / 合理推论) that are deeply oversold. Convexity comes
   from the stock itself — downside is already priced into the pessimism,
   the repair leg is non-linear. No options needed.

B. Capitulation convex calls (only when the capitulation radar fires ≥3/5):
   for the highest-beta AI-infra wash-out names, scan the chain for LEAPS
   calls (DTE ≥ 221, delta 0.30-0.55) and call debit spreads. Long-dated on
   purpose — at a VIX-40 bottom IV is rich, so a naked near-dated call bleeds
   vega as IV mean-reverts. LEAPS / debit spreads keep the convexity. The
   radar NEVER suggests a naked short-dated call.

Output: `capitulation_radar/{as_of}/convex_longs.{json,md}`.
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

from lib.radar_io import resolve_as_of, write_radar_outputs

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_AI_INFRA_ROOT = STACK_ROOT / "ai_infra"
DEFAULT_RADAR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "capitulation_radar"

# Oversold thresholds for the convex value-buy list.
OVERSOLD_EMA50_PCT = -15.0    # price >15% below its EMA50
OVERSOLD_DRAWDOWN_PCT = -25.0  # >25% off the 60d high
# LEAPS call window — long-dated on purpose to shed vega.
LEAPS_MIN_DTE = 221
CALL_DELTA_LOW = 0.30
CALL_DELTA_HIGH = 0.55
MIN_OPEN_INTEREST = 50
HIGH_BETA_QUANTILE = 0.75      # top quartile by beta = the dash-for-trash cohort


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


def _is_oversold(px: float, ema50: float | None, high_60d: float | None) -> tuple[bool, dict]:
    """A name is oversold (convex value entry) if it is far below its EMA50
    OR deep in a drawdown from its 60-day high."""
    vs_ema = ((px - ema50) / ema50 * 100.0) if ema50 else None
    drawdown = ((px - high_60d) / high_60d * 100.0) if high_60d else None
    oversold = (
        (vs_ema is not None and vs_ema <= OVERSOLD_EMA50_PCT)
        or (drawdown is not None and drawdown <= OVERSOLD_DRAWDOWN_PCT)
    )
    return oversold, {
        "px_vs_ema50_pct": round(vs_ema, 2) if vs_ema is not None else None,
        "drawdown_60d_pct": round(drawdown, 2) if drawdown is not None else None,
    }


def _close_series(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date, n: int) -> list[float]:
    rows = con.execute(
        "SELECT close FROM prices_daily WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT ?",
        [symbol, as_of.isoformat(), n],
    ).fetchall()
    return [float(r[0]) for r in reversed(rows) if r[0] is not None]


def build_convex_value_buys(
    con: duckdb.DuckDBPyConnection, production: dict[str, dict[str, Any]], as_of: date
) -> list[dict[str, Any]]:
    """Production-pool (evidence-confirmed) names that are deeply oversold."""
    out: list[dict[str, Any]] = []
    for symbol, record in sorted(production.items()):
        closes = _close_series(con, symbol, as_of, 70)
        if len(closes) < 50:
            continue
        px = closes[-1]
        ema50 = _ema(closes[-60:], 50)
        high_60d = max(closes[-60:]) if len(closes) >= 60 else max(closes)
        oversold, metrics = _is_oversold(px, ema50, high_60d)
        if not oversold:
            continue
        out.append({
            "symbol": symbol,
            "company": record.get("company"),
            "px": round(px, 2),
            "evidence_state": record.get("evidence_state"),
            "convexity": "convex",
            "convex_reason": "已证据确认 + 深度超跌 — 下行被悲观定价,修复腿非线性",
            **metrics,
        })
    out.sort(key=lambda r: (r.get("drawdown_60d_pct") or 0.0))
    return out


def compute_beta(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, float]:
    spy = _close_series(con, "SPY", as_of, 65)
    spy_ret = [
        (spy[i] - spy[i - 1]) / spy[i - 1] for i in range(1, len(spy)) if spy[i - 1]
    ]
    if len(spy_ret) < 30:
        return {}
    var_spy = statistics.pvariance(spy_ret)
    if var_spy <= 0:
        return {}
    betas: dict[str, float] = {}
    for sym in symbols:
        closes = _close_series(con, sym, as_of, 65)
        if len(closes) < 30:
            continue
        ret = [(closes[i] - closes[i - 1]) / closes[i - 1]
               for i in range(1, len(closes)) if closes[i - 1]]
        n = min(len(ret), len(spy_ret))
        if n < 30:
            continue
        a, b = ret[-n:], spy_ret[-n:]
        ma, mb = statistics.mean(a), statistics.mean(b)
        cov = sum((x - ma) * (y - mb) for x, y in zip(a, b)) / n
        betas[sym] = cov / var_spy
    return betas


def find_leaps_calls(
    con: duckdb.DuckDBPyConnection, symbol: str, as_of: date
) -> list[dict[str, Any]]:
    """Liquid long-dated calls (DTE ≥ 221, delta 0.30-0.55) — convex by
    construction (loss capped at premium, non-linear upside)."""
    rows = con.execute(
        """
        SELECT contract_symbol, expiry, days_to_exp, strike, current_price,
               bid, ask, mid, last_price, volume, open_interest,
               implied_volatility, delta
        FROM options_chain_quotes
        WHERE symbol = ? AND as_of = ? AND option_type = 'call'
          AND days_to_exp >= ? AND delta IS NOT NULL
          AND delta BETWEEN ? AND ?
          AND open_interest >= ?
        ORDER BY days_to_exp DESC, ABS(delta - 0.40)
        LIMIT 5
        """,
        [symbol, as_of.isoformat(), LEAPS_MIN_DTE, CALL_DELTA_LOW,
         CALL_DELTA_HIGH, MIN_OPEN_INTEREST],
    ).fetchall()
    cols = ["contract_symbol", "expiry", "days_to_exp", "strike", "current_price",
            "bid", "ask", "mid", "last_price", "volume", "open_interest",
            "implied_volatility", "delta"]
    out = []
    for r in rows:
        d = dict(zip(cols, r, strict=True))
        prem = d.get("mid") or d.get("last_price")
        if prem and prem > 0:
            d["premium_est"] = round(float(prem), 2)
            if d.get("current_price"):
                d["cost_pct_of_spot"] = round(float(prem) / float(d["current_price"]) * 100, 2)
        d["convexity"] = "convex"
        out.append(d)
    return out


def _production_universe(ai_infra_root: Path) -> dict[str, dict[str, Any]]:
    src = STACK_ROOT / "quant-research-v1" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        from quant_bot.analytics import ai_infra_universe as gate  # type: ignore
        return gate.records_by_symbol("US", ai_infra_root=ai_infra_root, pool="production")
    except Exception:  # noqa: BLE001
        return {}


def _ai_universe_us(ai_infra_root: Path) -> list[str]:
    src = STACK_ROOT / "quant-research-v1" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        from quant_bot.analytics import ai_infra_universe as gate  # type: ignore
        return sorted(gate.records_by_symbol("US", ai_infra_root=ai_infra_root, pool="research"))
    except Exception:  # noqa: BLE001
        return []


def load_capitulation(as_of: str, radar_root: Path) -> dict[str, Any] | None:
    path = radar_root / as_of / "capitulation_radar.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def render_markdown(as_of: str, payload: dict[str, Any]) -> str:
    cap = payload.get("capitulation_triggered")
    value_buys = payload.get("convex_value_buys") or []
    calls = payload.get("capitulation_convex_calls") or []
    lines = [
        f"# Capitulation Convex Radar — 凸性向上 — {as_of}",
        "",
        "victim_put 的镜像 —— 抄底场景的凸性表达。损失锁死,上行非线性。",
        "",
        "## A. 凸性价值买点(每日)",
        "",
        "已证据确认(原文已证明/合理推论)且深度超跌的生产池名字。",
        "凸性来自正股本身:下行已被悲观定价,修复腿非线性。",
        "",
    ]
    if value_buys:
        lines.append("| Symbol | Company | Px | vs EMA50 | 60d 回撤 | Evidence |")
        lines.append("|---|---|---:|---:|---:|---|")
        for r in value_buys:
            lines.append(
                f"| {r['symbol']} | {r.get('company') or '-'} | {r['px']} | "
                f"{r.get('px_vs_ema50_pct')}% | {r.get('drawdown_60d_pct')}% | "
                f"{(r.get('evidence_state') or '')[:36]} |"
            )
    else:
        lines.append("_当前没有生产池名字进入深度超跌区(无凸性价值买点)。_")
    lines += [
        "",
        "## B. Capitulation 凸性 call",
        "",
    ]
    if not cap:
        n = payload.get("capitulation_fired_count", 0)
        lines.append(
            f"_抄底雷达未触发({n}/5)。凸性 call 建议器仅在 CAPITULATION 状态激活 —— "
            "vol 峰值之外买 call 没有凸性优势。_"
        )
    elif calls:
        lines.append("最高 beta wash-out 名字的 LEAPS call(DTE≥221,delta 0.30-0.55,削 vega)。")
        lines.append("")
        lines.append("| Symbol | β | Contract | Expiry | DTE | Strike | Δ | IV | Premium | Cost%Spot |")
        lines.append("|---|---:|---|---|---:|---:|---:|---:|---:|---:|")
        for entry in calls:
            for c in entry.get("contracts") or []:
                iv = c.get("implied_volatility")
                iv_s = f"{iv:.2f}" if iv is not None else "n/a"
                delta = c.get("delta")
                delta_s = f"{delta:.2f}" if delta is not None else "n/a"
                lines.append(
                    f"| {entry['symbol']} | {entry.get('beta')} | "
                    f"{c.get('contract_symbol')} | {c.get('expiry')} | "
                    f"{c.get('days_to_exp')} | {c.get('strike')} | "
                    f"{delta_s} | {iv_s} | "
                    f"{c.get('premium_est')} | {c.get('cost_pct_of_spot')}% |"
                )
    else:
        lines.append("_CAPITULATION 已触发,但高 beta 名字暂无符合条件的 LEAPS call(DTE≥221)。_")
    lines += [
        "",
        "纪律:绝不裸近月 call(IV 均值回归时 vega 衰减吃掉 delta);只用 LEAPS / debit spread。",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-infra-root", type=Path, default=DEFAULT_AI_INFRA_ROOT)
    parser.add_argument("--radar-root", type=Path, default=DEFAULT_RADAR_ROOT)
    parser.add_argument("--top-beta", type=int, default=6,
                        help="How many top-beta names to scan for LEAPS calls.")
    args = parser.parse_args()

    as_of, as_of_text = resolve_as_of(args.as_of)
    if not args.us_db.exists():
        print(f"error: US db missing at {args.us_db}", file=sys.stderr)
        return 2

    cap = load_capitulation(as_of_text, args.radar_root) or {}
    triggered = bool(cap.get("capitulation"))
    fired_count = int(cap.get("fired_count", 0))

    production = _production_universe(args.ai_infra_root)
    ai_us = _ai_universe_us(args.ai_infra_root)

    con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        value_buys = build_convex_value_buys(con, production, as_of)
        convex_calls: list[dict[str, Any]] = []
        if triggered:
            betas = compute_beta(con, ai_us, as_of)
            ranked = sorted(betas.items(), key=lambda kv: kv[1], reverse=True)
            for sym, beta in ranked[: args.top_beta]:
                contracts = find_leaps_calls(con, sym, as_of)
                if contracts:
                    convex_calls.append({
                        "symbol": sym, "beta": round(beta, 2), "contracts": contracts,
                    })
    finally:
        con.close()

    payload = {
        "as_of": as_of_text,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "capitulation_triggered": triggered,
        "capitulation_fired_count": fired_count,
        "convex_value_buys": value_buys,
        "capitulation_convex_calls": convex_calls,
    }
    out_dir = write_radar_outputs(
        args.radar_root, as_of_text, "convex_longs",
        payload, render_markdown(as_of_text, payload),
    )
    print(
        f"capitulation convex radar: value_buys={len(value_buys)} "
        f"convex_calls={len(convex_calls)} (capitulation={triggered}) → {out_dir}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
