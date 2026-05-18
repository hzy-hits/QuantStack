"""Backtest the AI-infra strategy's risk-regime overlay.

The daily pipeline has never been backtested — we only ever verified that
today's output looks sane. This is the first real historical simulation.

What it tests (and what it honestly cannot):

  CAN  : does the risk-regime overlay add value? Hold the AI-infra
         production basket, but scale exposure each day by the regime R
         multiplier (US engine for the US basket, CN-native engine for the
         CN basket). Compare vs naive always-1.0x hold, and vs a benchmark
         (SMH for US, 沪深300 for CN).
  CANNOT: validate the evidence gate or universe membership over time —
         there is no point-in-time history of evidence_state, so the
         basket is taken as a FIXED set (today's production pool). This is
         a survivorship-style limitation, disclosed in the output.

The regime per historical day is recomputed with the exact production
classifiers (score_bubble_hedge_radar + score_risk_regime_engine /
score_cn_risk_regime), so the backtest exercises the real gate logic.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = STACK_ROOT / "scripts"
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
for p in (str(SCRIPT_DIR), str(QUANT_V1_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

import score_bubble_hedge_radar as bh  # noqa: E402
import score_cn_risk_regime as cnr  # noqa: E402
from score_risk_regime_engine import classify_regime  # noqa: E402
from quant_bot.analytics import ai_infra_universe  # noqa: E402

DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_backtest"
TRADING_DAYS = 252


# ── return / equity math ─────────────────────────────────────────────────────

def _equity(returns: list[float]) -> list[float]:
    eq, cur = [], 1.0
    for r in returns:
        cur *= (1.0 + r)
        eq.append(cur)
    return eq


def _stats(returns: list[float]) -> dict[str, Any]:
    if not returns:
        return {"n": 0}
    eq = _equity(returns)
    total = eq[-1] - 1.0
    n = len(returns)
    ann = (eq[-1] ** (TRADING_DAYS / n) - 1.0) if n > 0 and eq[-1] > 0 else None
    mean = statistics.mean(returns)
    std = statistics.pstdev(returns) if n > 1 else 0.0
    sharpe = (mean / std * math.sqrt(TRADING_DAYS)) if std > 0 else None
    peak, max_dd = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        max_dd = min(max_dd, v / peak - 1.0)
    hit = sum(1 for r in returns if r > 0) / n
    return {
        "n": n,
        "total_return_pct": round(total * 100, 2),
        "annualised_pct": round(ann * 100, 2) if ann is not None else None,
        "sharpe": round(sharpe, 2) if sharpe is not None else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "hit_rate_pct": round(hit * 100, 1),
    }


# ── price loading ────────────────────────────────────────────────────────────

def _closes(con, symbol_col: str, table: str, symbol: str, start: date, end: date) -> dict[date, float]:
    rows = con.execute(
        f"SELECT {'date' if table == 'prices_daily' else 'trade_date'}, close "
        f"FROM {table} WHERE {symbol_col} = ? AND "
        f"{'date' if table == 'prices_daily' else 'trade_date'} BETWEEN ? AND ? "
        f"ORDER BY 1",
        [symbol, start.isoformat(), end.isoformat()],
    ).fetchall()
    return {r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])): float(r[1])
            for r in rows if r[1] is not None}


def _basket_daily_returns(
    con, table: str, symbol_col: str, names: list[str], dates: list[date]
) -> list[float | None]:
    """Equal-weight basket return per day — names present on both d-1 and d."""
    series = {s: _closes(con, symbol_col, table, s, dates[0], dates[-1]) for s in names}
    out: list[float | None] = [None]
    for i in range(1, len(dates)):
        d, prev = dates[i], dates[i - 1]
        rets = []
        for s in names:
            c = series.get(s, {})
            if d in c and prev in c and c[prev]:
                rets.append(c[d] / c[prev] - 1.0)
        out.append(statistics.mean(rets) if rets else None)
    return out


def _index_returns(closes: dict[date, float], dates: list[date]) -> list[float | None]:
    out: list[float | None] = [None]
    for i in range(1, len(dates)):
        d, prev = dates[i], dates[i - 1]
        if d in closes and prev in closes and closes[prev]:
            out.append(closes[d] / closes[prev] - 1.0)
        else:
            out.append(None)
    return out


# ── regime reconstruction per historical day ─────────────────────────────────

def _us_regime_mult(us_con, d: date) -> tuple[float, str]:
    wedge = bh.build_wedge_layer(us_con, d)
    confirm = bh.build_confirmation_layer(us_con, d)
    decision = classify_regime(
        [w.__dict__ for w in wedge], confirm.__dict__, [], capitulation=None
    )
    return decision.r_multiplier, decision.state


def _move_at(us_con, d: date) -> tuple[float | None, float | None]:
    rows = us_con.execute(
        "SELECT close FROM prices_daily WHERE symbol = '^MOVE' AND date <= ? "
        "ORDER BY date DESC LIMIT 21",
        [d.isoformat()],
    ).fetchall()
    vals = [float(r[0]) for r in reversed(rows) if r[0] is not None]
    if not vals:
        return (None, None)
    level = vals[-1]
    chg = ((vals[-1] - vals[-21]) / vals[-21] * 100.0) if len(vals) > 20 and vals[-21] else None
    return (level, chg)


def _cn_regime_mult(cn_con, us_con, d: date) -> tuple[float, str]:
    gem = cnr._tape_signals(cn_con, cnr.GEM_INDEX, d)
    hs300 = cnr._tape_signals(cn_con, cnr.HS300_INDEX, d)
    move_level, move_chg = _move_at(us_con, d)
    signals = {
        "gem_above_ema20": gem["above_ema20"], "gem_above_ema50": gem["above_ema50"],
        "hs300_above_ema50": hs300["above_ema50"],
        "north_20d_sum": cnr._north_20d_sum(cn_con, d),
        "margin_chg_20d_pct": cnr._margin_trend(cn_con, d),
        "us_move_level": move_level, "us_move_chg_20d": move_chg,
    }
    decision = cnr.classify_cn_regime(signals)
    return decision.r_multiplier, decision.state


# ── simulation ───────────────────────────────────────────────────────────────

def _simulate(
    dates: list[date],
    basket_ret: list[float | None],
    regime_mult: list[float],
    regime_state: list[str],
    bench_ret: list[float | None],
) -> dict[str, Any]:
    """gated = yesterday's regime mult × today's basket return (no look-ahead)."""
    gated, naive, bench = [], [], []
    for i in range(1, len(dates)):
        br = basket_ret[i]
        if br is None:
            continue
        mult = regime_mult[i - 1]  # prior-day regime sizes today
        gated.append(mult * br)
        naive.append(br)
        bench.append(bench_ret[i] if bench_ret[i] is not None else 0.0)
    state_dist: dict[str, int] = {}
    for s in regime_state:
        state_dist[s] = state_dist.get(s, 0) + 1
    return {
        "gated_regime_overlay": _stats(gated),
        "naive_always_hold": _stats(naive),
        "benchmark": _stats(bench),
        "avg_exposure": round(statistics.mean(regime_mult), 3) if regime_mult else None,
        "regime_day_distribution": state_dist,
    }


def _verdict(sim: dict[str, Any]) -> str:
    """Honest verdict — Sharpe (risk-adjusted return) is the primary test.

    Cutting drawdown while LOWERING Sharpe is not a win: it trades return
    for drawdown at a bad exchange rate. Only a Sharpe improvement (or a
    flat Sharpe with a much smaller drawdown) counts as the overlay adding
    value.
    """
    g, n = sim["gated_regime_overlay"], sim["naive_always_hold"]
    if not g.get("n") or not n.get("n"):
        return "数据不足,无法判定。"
    gs, ns = g.get("sharpe"), n.get("sharpe")
    gd, nd = g.get("max_drawdown_pct"), n.get("max_drawdown_pct")
    facts = []
    if gs is not None and ns is not None:
        facts.append(f"夏普 {gs} vs 无脑 {ns}")
    if gd is not None and nd is not None:
        facts.append(f"最大回撤 {gd}% vs 无脑 {nd}%")
    fact = "; ".join(facts)
    if gs is None or ns is None:
        return f"夏普缺失,无法判定({fact})。"
    if gs > ns + 0.05:
        return f"regime overlay 提升风险调整收益,有价值({fact})。"
    if gs >= ns - 0.05 and gd is not None and nd is not None and gd > nd + 5.0:
        return f"regime overlay 夏普持平但显著压低回撤,可接受({fact})。"
    return ("regime overlay 降低了夏普 —— 在用收益换回撤,且换得不划算。"
            f"闸门(尤其 PRESS)过度防御/需重新调参({fact})。")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        default=date(2024, 6, 1))
    parser.add_argument("--end", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    end = args.end or date.today()

    us_basket = sorted(ai_infra_universe.records_by_symbol("US", pool="production"))
    cn_basket = sorted(ai_infra_universe.records_by_symbol("CN", pool="production"))
    print(f"production basket — US {len(us_basket)} / CN {len(cn_basket)} "
          f"(PIT-limited: fixed at today's pool)")

    result: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window": {"start": args.start.isoformat(), "end": end.isoformat()},
        "pit_caveat": "Basket is today's production pool held fixed — the "
                      "evidence gate / membership is NOT backtested point-in-time.",
        "us_basket": us_basket, "cn_basket": cn_basket,
    }

    # ── US ──
    us_con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        smh = _closes(us_con, "symbol", "prices_daily", "SMH", args.start, end)
        dates = sorted(d for d in smh if args.start <= d <= end)
        if len(dates) > 30:
            print(f"US backtest: {len(dates)} trading days, computing regime ...")
            basket_ret = _basket_daily_returns(us_con, "prices_daily", "symbol", us_basket, dates)
            bench_ret = _index_returns(smh, dates)
            mults, states = [], []
            for d in dates:
                m, s = _us_regime_mult(us_con, d)
                mults.append(m)
                states.append(s)
            result["us"] = _simulate(dates, basket_ret, mults, states, bench_ret)
            result["us"]["verdict"] = _verdict(result["us"])
        else:
            result["us"] = {"error": "insufficient US history"}
    finally:
        us_con.close()

    # ── CN ──
    cn_con = duckdb.connect(str(args.cn_db), read_only=True)
    us_con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        hs300 = _closes(cn_con, "ts_code", "prices", "000300.SH", args.start, end)
        dates = sorted(d for d in hs300 if args.start <= d <= end)
        if len(dates) > 30:
            print(f"CN backtest: {len(dates)} trading days, computing regime ...")
            basket_ret = _basket_daily_returns(cn_con, "prices", "ts_code", cn_basket, dates)
            bench_ret = _index_returns(hs300, dates)
            mults, states = [], []
            for d in dates:
                m, s = _cn_regime_mult(cn_con, us_con, d)
                mults.append(m)
                states.append(s)
            result["cn"] = _simulate(dates, basket_ret, mults, states, bench_ret)
            result["cn"]["verdict"] = _verdict(result["cn"])
        else:
            result["cn"] = {"error": "insufficient CN history"}
    finally:
        cn_con.close()
        us_con.close()

    out_dir = args.output_root / end.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "ai_infra_backtest.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    (out_dir / "ai_infra_backtest.md").write_text(_render_md(result), encoding="utf-8")
    print(f"backtest written: {out_dir / 'ai_infra_backtest.md'}")
    for mkt in ("us", "cn"):
        m = result.get(mkt) or {}
        if "verdict" in m:
            print(f"  {mkt.upper()}: {m['verdict']}")
    return 0


def _render_md(result: dict[str, Any]) -> str:
    w = result["window"]
    lines = [
        f"# AI-infra 策略回测 — risk-regime overlay — {w['start']}..{w['end']}",
        "",
        f"> PIT 局限：{result['pit_caveat']}",
        "",
        "测试问题:risk-regime overlay(按状态缩放仓位)比无脑 always-hold 强吗?",
        "强 = 闸门有价值;不强 = 闸门太松/纯拖累。",
        "",
    ]
    for mkt, label, bench in (("us", "美股 (基准 SMH)", "SMH"), ("cn", "A股 (基准 沪深300)", "沪深300")):
        m = result.get(mkt) or {}
        lines.append(f"## {label}")
        lines.append("")
        if "error" in m:
            lines += [f"_{m['error']}_", ""]
            continue
        lines.append("| 口径 | 总收益 | 年化 | 夏普 | 最大回撤 | 胜率 |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for key, name in (("gated_regime_overlay", "regime overlay(本策略)"),
                           ("naive_always_hold", "无脑 always-hold 篮子"),
                           ("benchmark", f"基准 {bench}")):
            s = m.get(key) or {}
            lines.append(
                f"| {name} | {s.get('total_return_pct')}% | {s.get('annualised_pct')}% | "
                f"{s.get('sharpe')} | {s.get('max_drawdown_pct')}% | {s.get('hit_rate_pct')}% |"
            )
        lines.append("")
        lines.append(f"- 平均仓位暴露(regime 乘数均值): {m.get('avg_exposure')}")
        lines.append(f"- regime 各状态天数: {m.get('regime_day_distribution')}")
        lines.append(f"- **判定**: {m.get('verdict')}")
        lines.append("")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
