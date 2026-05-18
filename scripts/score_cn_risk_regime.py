"""CN-native risk regime — A-shares get their own Hedge/Wedge/Confirm/Press.

The US risk-regime engine reads SMH / TLT / MOVE / VIX. Applying its R
multiplier to the CN basket is wrong: A-shares run their own tape and
liquidity cycle. A US semis EMA break should not freeze CN adds when the
A-share board is in its own healthy uptrend.

But the *wedge* (rates / MOVE) IS global — it transmits to A-shares via
northbound flow (US rates spike → foreign money pulls back from A-shares).
So the CN regime keeps the US wedge layer and swaps the tape/flow layer for
CN-native signals:

  tape  : 创业板指 399006 + 沪深300 000300 — EMA20 / EMA50 structure
  flow  : 北向资金 20d net direction + 两融余额 20d trend
  wedge : US MOVE (kept — transmits to A-shares)

States (precedence PRESS > CONFIRM > WEDGE > HEDGE):
  PRESS   : 创业板/沪深300 sustained ≥3-day EMA50 loss — tape broken → 0.35x
            (defensive core, not full flat — see US engine backtest note)
  CONFIRM : 创业板 lost EMA20 (holds EMA50), OR 北向净流出 + 两融见顶 → 0.4x
  WEDGE   : 北向 20d 净流出, OR US MOVE wedge biting                → 0.6x
  HEDGE   : default                                               → 1.0x

Writes cn_risk_regime.json — run_main_strategy_v2 consumes it and gates the
CN basket separately from the US basket.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = STACK_ROOT / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from score_risk_regime_engine import RegimeDecision, R_MULTIPLIER  # noqa: E402

DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
DEFAULT_BUBBLE_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "bubble_hedge_radar"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "cn_risk_regime"

GEM_INDEX = "399006.SZ"   # 创业板指 — the AI/growth-heavy board
HS300_INDEX = "000300.SH"  # 沪深300 — broad-market breadth

# Thresholds.
MARGIN_DERISK_PCT = -2.0   # 两融余额 20d change at/below = leverage coming down
US_MOVE_CALM = 80.0        # US MOVE at/above + rising = rates wedge transmitting
CN_PRESS_CONFIRM_DAYS = 3  # EMA50 break must hold this many consecutive closes
                           # before CN PRESS fires — hysteresis vs single-day
                           # whipsaw. A 1-2 day dip is CONFIRM, not PRESS.


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def _ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _ema50_streak(closes: list[float]) -> int | None:
    """Signed consecutive-day run of closes above(+)/below(-) EMA50."""
    if len(closes) < 50:
        return None
    ema = _ema_series(closes, 50)
    above = closes[-1] > ema[-1]
    streak = 0
    for i in range(len(closes) - 1, -1, -1):
        if (closes[i] > ema[i]) == above:
            streak += 1
        else:
            break
    return streak if above else -streak


def classify_cn_regime(signals: dict[str, Any]) -> RegimeDecision:
    """Pure classifier: CN tape/flow signals + US wedge → regime + R multiplier."""
    gem_e20 = signals.get("gem_above_ema20")
    gem_e50 = signals.get("gem_above_ema50")
    hs300_e50 = signals.get("hs300_above_ema50")
    gem_streak = signals.get("gem_ema50_streak")
    hs300_streak = signals.get("hs300_ema50_streak")
    north_20d = signals.get("north_20d_sum")
    margin_chg = signals.get("margin_chg_20d_pct")
    move_level = signals.get("us_move_level")
    move_chg = signals.get("us_move_chg_20d")

    # PRESS hysteresis: an EMA50 break must hold CN_PRESS_CONFIRM_DAYS before
    # PRESS fires. A 1-2 day dip below EMA50 is a fresh break → tape_breaking
    # → CONFIRM (0.4x), not PRESS (0.0x). Legacy signals without the streak
    # fall back to the single-day bool.
    if gem_streak is not None or hs300_streak is not None:
        gs = gem_streak if gem_streak is not None else 1
        hs = hs300_streak if hs300_streak is not None else 1
        tape_broken = (gs <= -CN_PRESS_CONFIRM_DAYS) or (hs <= -CN_PRESS_CONFIRM_DAYS)
        tape_breaking = ((-CN_PRESS_CONFIRM_DAYS < gs < 0)
                         or (-CN_PRESS_CONFIRM_DAYS < hs < 0))
    else:
        tape_broken = (gem_e50 is False) or (hs300_e50 is False)
        tape_breaking = False
    tape_soft = (gem_e20 is False) and (gem_e50 is True)
    flow_out = north_20d is not None and north_20d < 0.0
    margin_derisk = margin_chg is not None and margin_chg <= MARGIN_DERISK_PCT
    us_wedge = (
        move_level is not None and move_level >= US_MOVE_CALM
        and move_chg is not None and move_chg > 0.0
    )

    sig = {
        "gem_above_ema20": gem_e20, "gem_above_ema50": gem_e50,
        "hs300_above_ema50": hs300_e50,
        "gem_ema50_streak": gem_streak, "hs300_ema50_streak": hs300_streak,
        "north_20d_sum": north_20d, "margin_chg_20d_pct": margin_chg,
        "us_move_level": move_level, "us_move_chg_20d": move_chg,
        "tape_broken": tape_broken, "tape_breaking": tape_breaking,
        "tape_soft": tape_soft,
        "flow_out": flow_out, "margin_derisk": margin_derisk, "us_wedge": us_wedge,
    }

    if tape_broken:
        if gem_streak is not None and gem_streak <= -CN_PRESS_CONFIRM_DAYS:
            which = f"创业板连续 {abs(gem_streak)} 日"
        elif hs300_streak is not None and hs300_streak <= -CN_PRESS_CONFIRM_DAYS:
            which = f"沪深300连续 {abs(hs300_streak)} 日"
        else:
            which = "创业板" if gem_e50 is False else "沪深300"
        return RegimeDecision(
            state="press", r_multiplier=R_MULTIPLIER["press"], new_adds_allowed=False,
            hedge_directive="A股 tape 破位；冻结新加仓，减仓可用股指期货对冲。",
            victim_action="A股无 victim-put 层；破位后等企稳信号,不抄落刀。",
            rationale=f"CN PRESS：{which}收于 EMA50 下,A股趋势确认破位。核心仓位减至 0.35x、冻结新加仓。",
            signals=sig,
        )
    if tape_breaking or tape_soft or (flow_out and margin_derisk):
        if tape_breaking:
            reason = (f"创业板/沪深300 跌破 EMA50 未满 {CN_PRESS_CONFIRM_DAYS} "
                      "日确认(fresh break,先减码不清仓)")
        elif tape_soft:
            reason = "创业板失守 EMA20(仍站 EMA50)"
        else:
            reason = "北向净流出叠加两融见顶回落"
        return RegimeDecision(
            state="confirm", r_multiplier=R_MULTIPLIER["confirm"], new_adds_allowed=True,
            hedge_directive="减少新加仓；准备 trim;股指期货对冲 beta。",
            victim_action="A股观望;等 EMA50 与北向同时转好再加。",
            rationale=f"CN CONFIRM：{reason}。A股新加仓 scale 到 0.4x。",
            signals=sig,
        )
    if flow_out or us_wedge:
        bits = []
        if flow_out:
            bits.append(f"北向 20d 净流出 {north_20d:,.0f}")
        if us_wedge:
            bits.append(f"美债 MOVE {move_level:.0f}↑ 经北向传导")
        return RegimeDecision(
            state="wedge", r_multiplier=R_MULTIPLIER["wedge"], new_adds_allowed=True,
            hedge_directive="保留对冲;tape 仍完整,继续买但减码。",
            victim_action="A股观察;关注北向是否持续流出。",
            rationale=f"CN WEDGE：{'、'.join(bits)}。A股新加仓 scale 到 0.6x。",
            signals=sig,
        )
    note = "创业板/沪深300 站上 EMA50,北向净流入,两融未见顶"
    return RegimeDecision(
        state="hedge", r_multiplier=R_MULTIPLIER["hedge"], new_adds_allowed=True,
        hedge_directive="A股 tape 与资金面健康;保留少量股指对冲即可。",
        victim_action="A股无动作。",
        rationale=f"CN HEDGE：{note}。A股新加仓 full size。",
        signals=sig,
    )


# ── DB layer ─────────────────────────────────────────────────────────────────

def _index_closes(con: duckdb.DuckDBPyConnection, ts_code: str, as_of: date, n: int) -> list[float]:
    rows = con.execute(
        "SELECT close FROM prices WHERE ts_code = ? AND trade_date <= ? "
        "ORDER BY trade_date DESC LIMIT ?",
        [ts_code, as_of.isoformat(), n],
    ).fetchall()
    return [float(r[0]) for r in reversed(rows) if r[0] is not None]


def _north_20d_sum(con: duckdb.DuckDBPyConnection, as_of: date) -> float | None:
    rows = con.execute(
        "SELECT net_amount FROM northbound_flow WHERE trade_date <= ? "
        "ORDER BY trade_date DESC LIMIT 20",
        [as_of.isoformat()],
    ).fetchall()
    vals = [float(r[0]) for r in rows if r[0] is not None]
    return sum(vals) if vals else None


def _margin_trend(con: duckdb.DuckDBPyConnection, as_of: date) -> float | None:
    """两融总融资余额 20-trading-day % change."""
    rows = con.execute(
        "SELECT trade_date, SUM(rzye) AS total FROM margin_detail "
        "WHERE trade_date <= ? GROUP BY trade_date ORDER BY trade_date DESC LIMIT 22",
        [as_of.isoformat()],
    ).fetchall()
    totals = [float(r[1]) for r in rows if r[1] is not None]
    if len(totals) < 21 or not totals[20]:
        return None
    return (totals[0] / totals[20] - 1.0) * 100.0


def _tape_signals(con: duckdb.DuckDBPyConnection, ts_code: str, as_of: date) -> dict[str, Any]:
    closes = _index_closes(con, ts_code, as_of, 60)
    if len(closes) < 50:
        return {"close": None, "above_ema20": None, "above_ema50": None,
                "ema50_streak": None}
    last = closes[-1]
    ema20 = _ema(closes[-20:], 20)
    ema50 = _ema(closes, 50)
    return {
        "close": round(last, 1),
        "ema20": round(ema20, 1) if ema20 else None,
        "ema50": round(ema50, 1) if ema50 else None,
        "above_ema20": (last > ema20) if ema20 else None,
        "above_ema50": (last > ema50) if ema50 else None,
        "ema50_streak": _ema50_streak(closes),
    }


def _us_wedge(as_of: str, bubble_root: Path) -> tuple[float | None, float | None]:
    path = bubble_root / as_of / "bubble_hedge.json"
    if not path.exists():
        return (None, None)
    try:
        conf = (json.loads(path.read_text(encoding="utf-8")).get("confirmation") or {})
    except (OSError, json.JSONDecodeError):
        return (None, None)
    return (conf.get("move_level"), conf.get("move_chg_20d"))


def build_cn_signals(cn_db: Path, as_of: date, bubble_root: Path) -> dict[str, Any]:
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        gem = _tape_signals(con, GEM_INDEX, as_of)
        hs300 = _tape_signals(con, HS300_INDEX, as_of)
        north_20d = _north_20d_sum(con, as_of)
        margin_chg = _margin_trend(con, as_of)
    finally:
        con.close()
    move_level, move_chg = _us_wedge(as_of.isoformat(), bubble_root)
    return {
        "gem_close": gem["close"], "gem_above_ema20": gem["above_ema20"],
        "gem_above_ema50": gem["above_ema50"],
        "gem_ema50_streak": gem["ema50_streak"],
        "hs300_close": hs300["close"], "hs300_above_ema50": hs300["above_ema50"],
        "hs300_ema50_streak": hs300["ema50_streak"],
        "north_20d_sum": round(north_20d, 1) if north_20d is not None else None,
        "margin_chg_20d_pct": round(margin_chg, 2) if margin_chg is not None else None,
        "us_move_level": move_level, "us_move_chg_20d": move_chg,
    }


def render_markdown(as_of: str, decision: RegimeDecision) -> str:
    label = {
        "hedge": "HEDGE（A股常驻基线）", "wedge": "WEDGE（楔子传导）",
        "confirm": "CONFIRM（破位预警）", "press": "PRESS（确认破位）",
    }.get(decision.state, decision.state)
    s = decision.signals
    return "\n".join([
        f"# CN 风控引擎 — A股 Hedge/Wedge/Confirm/Press — {as_of}",
        "",
        f"## 当前状态：**{label}** ｜ A股新加仓 R 乘数 `{decision.r_multiplier:.2f}x`",
        "",
        f"- 判定：{decision.rationale}",
        f"- 对冲指引：{decision.hedge_directive}",
        "",
        "## 信号读数",
        "",
        "| 信号 | 值 | 触发 |",
        "|---|---|---|",
        f"| 创业板站上 EMA20 | {s.get('gem_above_ema20')} | A股 near line |",
        f"| 创业板站上 EMA50 | {s.get('gem_above_ema50')} | A股趋势线(触发器) |",
        f"| 创业板 EMA50 连续天数 | {s.get('gem_ema50_streak')} | "
        f"≤-{CN_PRESS_CONFIRM_DAYS}=确认破位/PRESS; <0 未满=CONFIRM |",
        f"| 沪深300站上 EMA50 | {s.get('hs300_above_ema50')} | 宽基趋势 |",
        f"| 沪深300 EMA50 连续天数 | {s.get('hs300_ema50_streak')} | "
        f"≤-{CN_PRESS_CONFIRM_DAYS}=确认破位 |",
        f"| 北向 20日净流入 | {s.get('north_20d_sum')} | <0=外资流出 |",
        f"| 两融余额 20日变化 | {s.get('margin_chg_20d_pct')}% | ≤-2%=去杠杆 |",
        f"| 美债 MOVE | {s.get('us_move_level')} | ≥80上行=楔子传导 |",
        "",
        "状态转移: PRESS > CONFIRM > WEDGE > HEDGE。CN regime 只 gate A股篮子,"
        "美股篮子由美股 regime 独立 gate。楔子层(MOVE)对中美共用。",
    ]) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--bubble-root", type=Path, default=DEFAULT_BUBBLE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of_text = args.as_of or cst.date().isoformat()
    as_of = date.fromisoformat(as_of_text)
    if not args.cn_db.exists():
        print(f"error: CN db missing at {args.cn_db}", file=sys.stderr)
        return 2

    signals = build_cn_signals(args.cn_db, as_of, args.bubble_root)
    decision = classify_cn_regime(signals)

    out_dir = args.output_root / as_of_text
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": as_of_text,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "framework": "cn_native_hedge_wedge_confirm_press",
        **decision.as_dict(),
        "input_signals": signals,
    }
    (out_dir / "cn_risk_regime.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / "cn_risk_regime.md").write_text(
        render_markdown(as_of_text, decision), encoding="utf-8"
    )
    print(f"CN risk regime: {decision.state.upper()} "
          f"(R x{decision.r_multiplier:.2f}) → {out_dir / 'cn_risk_regime.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
