"""Hedge / Wedge / Confirm / Press risk-regime engine.

The bubble_hedge_radar produces the *descriptive* Wedge / Victim / Confirmation
layers. This engine turns them into a single discrete daily regime state plus a
hard R multiplier that gates AI-infra basket sizing.

Framework (operator essay, Hedge-Wedge-Confirm-Press):

  HEDGE   — baseline. Tape healthy, wedge not biting, sentiment not extreme.
            AI-infra new R: full (1.0x).
  WEDGE   — the trend that kills the bubble is biting (rates up / credit
            widening / AI↔rates correlation flips negative). Tape still intact.
            AI-infra new R: scaled (0.6x); hold the wedge hedge (TBT / TLT
            put-spread).
  CONFIRM — warning. Either the tape lost its near line (EMA20) but holds
            EMA50, or sentiment is at an extreme-greed top while the wedge is
            already biting. Pre-break.
            AI-infra new R: scaled hard (0.4x); freeze adds to stretched
            victims; prep the trim list.
  PRESS   — confirmed break: SMH sustained ≥3-day EMA50 loss, or an explicit
            trendline break. AI-infra R: trimmed to a defensive core (0.35x)
            — NOT a full liquidation. The AI basket V-recovers too hard for
            0.0x to pay (backtested: 0.0x Sharpe 1.19 < 0.35x 1.50). Freeze
            new adds; activate victim puts; press the victim shortlist.

The engine reads `bubble_hedge.json` (single source of truth — never re-queries
the DB) and writes `risk_regime.json` + `risk_regime.md`. The R multiplier is
consumed by generate_main_strategy_v2_report.py when it sizes the AI-infra basket.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from lib.radar_io import resolve_as_of, write_radar_outputs

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUBBLE_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "bubble_hedge_radar"
DEFAULT_CAPITULATION_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "capitulation_radar"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "risk_regime"

# Regime → R multiplier on AI-infra basket new adds.
R_MULTIPLIER = {
    "hedge": 1.0,
    "wedge": 0.6,
    "confirm": 0.4,
    "press": 0.35,  # defensive core, NOT full flat — the AI basket
                    # V-recovers; 0.0x misses the bounce (backtested).
    # CAPITULATION is the recovery state — selling exhausted, flip to convex
    # long. New adds reopen at full size with a high-beta / convex tilt.
    "capitulation": 1.0,
}

# Signal thresholds — kept here so tests and operators can see them in one place.
TLT_WEDGE_DRAWDOWN_PCT = -2.0   # TLT 20d return at/below this = rates biting
HYG_CREDIT_STRESS_PCT = -1.0    # HYG 20d return at/below this = credit widening
CORR_FLIP_THRESHOLD = 0.5       # SMH↔TLT 20d corr at/above this = rates-dominated
                                # regime. Stocks and bonds normally move OPPOSITE
                                # (negative corr); when rates drive both they fall
                                # together → positive corr = "bonds in the stonks".
PRESS_CONFIRM_DAYS = 3          # EMA50 break must hold this many consecutive
                                # closes before PRESS fires. A 1-2 day dip is
                                # a fresh, unconfirmed break → CONFIRM, not
                                # PRESS — hysteresis that kills the whipsaw.
GREED_EXTREME = 75.0            # Fear & Greed at/above this = extreme greed
FEAR_EXTREME = 30.0             # Fear & Greed at/below this = fear
MOVE_CALM = 80.0                # MOVE below this = Treasury market calm / risk-on
MOVE_VIX_RATES_STRESS = 6.0     # MOVE/VIX at/above this = rates vol dominates stress


def _smoothstep(value: float | None, lo: float, hi: float) -> float:
    """0→1 ramp between lo and hi (smooth, clamped); None → 0."""
    if value is None:
        return 0.0
    t = max(0.0, min(1.0, (value - lo) / (hi - lo)))
    return t * t * (3.0 - 2.0 * t)


def wedge_pressure(*, corr: float | None, move_level: float | None, move_rising: bool,
                   tlt_ret_20d: float | None, hyg_ret_20d: float | None) -> float:
    """0..1 continuous wedge pressure.

    The binary wedge triggers have cliff edges: 2026-06-10 read corr 0.446
    (<0.5), MOVE 77 (<80), TLT -0.51% (>-2%), HYG -0.45% (>-1%) — every signal
    marginally un-tripped → full 1.00x. This score turns the same inputs into
    a continuous discount ramp starting BELOW each binary threshold.
    """
    corr_p = _smoothstep(corr, 0.35, CORR_FLIP_THRESHOLD)
    move_p = _smoothstep(move_level, 70.0, MOVE_CALM) * (1.0 if move_rising else 0.5)
    tlt_p = _smoothstep(-(tlt_ret_20d if tlt_ret_20d is not None else 0.0),
                        1.0, -TLT_WEDGE_DRAWDOWN_PCT)
    hyg_p = _smoothstep(-(hyg_ret_20d if hyg_ret_20d is not None else 0.0),
                        0.5, -HYG_CREDIT_STRESS_PCT)
    return round(0.35 * corr_p + 0.25 * move_p + 0.25 * tlt_p + 0.15 * hyg_p, 4)


def continuous_multiplier(*, state: str, base_multiplier: float, pressure: float) -> float:
    """Shadow multiplier: only the full-size hedge state gets the continuous
    discount (other states are already discounted); floor = wedge tier."""
    if state != "hedge":
        return base_multiplier
    return round(max(R_MULTIPLIER["wedge"], base_multiplier * (1.0 - 0.35 * pressure)), 2)


def context_dampener(*, mrs: float | None, mrs_quadrant: str | None,
                     negative_accel_count: int) -> float:
    """0.70..1.0 dampener from the engine's own sibling layers.

    MRS quadrant III (SPX down + P/C up, historically the worst forward
    bucket) at depressed MRS, and broad index negative-gamma acceleration,
    each pull the shadow multiplier down. Missing data is neutral (1.0).
    """
    dampener = 1.0
    if mrs is not None and mrs <= -0.5 and str(mrs_quadrant or "").strip().upper() == "III":
        dampener *= 0.85
    dampener *= max(0.85, 1.0 - 0.05 * max(0, int(negative_accel_count)))
    return round(max(0.70, dampener), 4)


GAMMA_CONTEXT_SYMBOLS = ("SPY", "QQQ", "SMH")
US_DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def load_context_signals(as_of: str) -> dict[str, Any]:
    """Best-effort MRS + index-gamma context for the shadow dampener.

    Any failure (missing DB, missing options history, locked file) degrades
    to neutral inputs — the shadow multiplier must never break the engine.
    """
    out: dict[str, Any] = {
        "mrs": None, "mrs_quadrant": None, "negative_accel_count": 0, "available": False,
    }
    try:
        from datetime import date as _date

        from sections.gamma_spring import build_gamma_spring_snapshot
        from sections.market_regime import build_market_regime_score

        day = _date.fromisoformat(as_of)
        mrs = build_market_regime_score(US_DB_PATH, day)
        if mrs.get("available"):
            out["mrs"] = mrs.get("mrs")
            out["mrs_quadrant"] = mrs.get("quadrant")
        snapshot = build_gamma_spring_snapshot(US_DB_PATH, list(GAMMA_CONTEXT_SYMBOLS), day)
        count = 0
        for row in snapshot.get("rows") or []:
            if str(row.get("gex_flip_regime") or "").startswith("negative_acceleration"):
                count += 1
        out["negative_accel_count"] = count
        out["available"] = True
    except Exception as exc:  # noqa: BLE001 — context is shadow-only, never fatal.
        print(f"  [warn] regime context signals unavailable: {exc}", file=sys.stderr)
    return out


@dataclass(frozen=True)
class RegimeDecision:
    state: str
    r_multiplier: float
    new_adds_allowed: bool
    hedge_directive: str
    victim_action: str
    rationale: str
    signals: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _wedge_value(wedge_rows: list[dict[str, Any]], symbol: str, field: str) -> float | None:
    for row in wedge_rows:
        if str(row.get("symbol")) == symbol:
            value = row.get(field)
            return float(value) if value is not None else None
    return None


def classify_regime(
    wedge_rows: list[dict[str, Any]],
    confirmation: dict[str, Any],
    victims: list[dict[str, Any]],
    capitulation: dict[str, Any] | None = None,
) -> RegimeDecision:
    """Pure classifier: bubble-hedge layers → discrete regime + R multiplier.

    Precedence: CAPITULATION > PRESS > CONFIRM > WEDGE > HEDGE. CAPITULATION
    is checked first — once selling exhaustion is confirmed (capitulation
    radar ≥3/5) you stop pressing shorts, regardless of how broken the tape
    looks. The capitulation signals themselves only exist post-crash.
    """
    tlt_20d = _wedge_value(wedge_rows, "TLT", "ret_20d_pct")
    hyg_20d = _wedge_value(wedge_rows, "HYG", "ret_20d_pct")
    corr = confirmation.get("ai_book_vs_tlt_corr_20d")
    corr = float(corr) if corr is not None else None
    fg = confirmation.get("fear_greed_score")
    fg = float(fg) if fg is not None else None
    smh_above_ema20 = confirmation.get("smh_above_ema20")
    smh_above_ema50 = confirmation.get("smh_above_ema50")
    trendline_break = bool(confirmation.get("trendline_break"))
    ema50_streak = confirmation.get("smh_ema50_streak")
    ema50_streak = int(ema50_streak) if ema50_streak is not None else None
    move_level = confirmation.get("move_level")
    move_level = float(move_level) if move_level is not None else None
    move_chg_20d = confirmation.get("move_chg_20d")
    move_chg_20d = float(move_chg_20d) if move_chg_20d is not None else None
    vix_level = confirmation.get("vix_level")
    vix_level = float(vix_level) if vix_level is not None else None
    move_vix_ratio = confirmation.get("move_vix_ratio")
    move_vix_ratio = float(move_vix_ratio) if move_vix_ratio is not None else None

    # MOVE is the most direct wedge gauge: Treasury implied vol. It bites
    # before TLT price does. Two MOVE-based wedge triggers:
    #   - MOVE >= 80 AND rising (20d) → bond vol leaving the calm zone
    #   - MOVE/VIX >= 6 → rates vol dominates the cross-asset stress picture
    move_wedge = (
        (move_level is not None and move_level >= MOVE_CALM
         and move_chg_20d is not None and move_chg_20d > 0.0)
        or (move_vix_ratio is not None and move_vix_ratio >= MOVE_VIX_RATES_STRESS)
    )
    wedge_biting = (
        (tlt_20d is not None and tlt_20d <= TLT_WEDGE_DRAWDOWN_PCT)
        or (corr is not None and corr >= CORR_FLIP_THRESHOLD)
        or (hyg_20d is not None and hyg_20d <= HYG_CREDIT_STRESS_PCT)
        or move_wedge
    )
    # PRESS hysteresis: when the EMA50 streak is available, PRESS only fires
    # once the break is *sustained* for PRESS_CONFIRM_DAYS. A 1-2 day dip
    # below EMA50 is a fresh, unconfirmed break → tape_breaking → CONFIRM
    # (0.4x), not PRESS (0.0x). Legacy payloads without the streak fall back
    # to the single-day bool.
    if ema50_streak is not None:
        tape_broken = (ema50_streak <= -PRESS_CONFIRM_DAYS) or trendline_break
        tape_breaking = -PRESS_CONFIRM_DAYS < ema50_streak < 0
    else:
        tape_broken = (smh_above_ema50 is False) or trendline_break
        tape_breaking = False
    tape_soft = (smh_above_ema20 is False) and (smh_above_ema50 is True)
    greed_extreme = fg is not None and fg >= GREED_EXTREME
    fear_extreme = fg is not None and fg <= FEAR_EXTREME

    signals = {
        "tlt_ret_20d_pct": tlt_20d,
        "hyg_ret_20d_pct": hyg_20d,
        "smh_tlt_corr_20d": corr,
        "fear_greed_score": fg,
        "move_level": move_level,
        "move_chg_20d": move_chg_20d,
        "vix_level": vix_level,
        "move_vix_ratio": move_vix_ratio,
        "smh_above_ema20": smh_above_ema20,
        "smh_above_ema50": smh_above_ema50,
        "trendline_break": trendline_break,
        "smh_ema50_streak": ema50_streak,
        "wedge_biting": wedge_biting,
        "move_wedge": move_wedge,
        "tape_broken": tape_broken,
        "tape_breaking": tape_breaking,
        "tape_soft": tape_soft,
        "greed_extreme": greed_extreme,
        "fear_extreme": fear_extreme,
        "victim_count": len(victims),
        "top_victim": (victims[0].get("symbol") if victims else None),
        "capitulation_fired": (capitulation or {}).get("fired_count", 0),
        "capitulation_triggered": bool((capitulation or {}).get("capitulation")),
    }

    top_victim = victims[0].get("symbol") if victims else "—"

    # CAPITULATION — checked first. Selling exhaustion confirmed → stop
    # pressing shorts, flip to convex long.
    if capitulation and capitulation.get("capitulation"):
        fired = capitulation.get("fired_count", 0)
        names = "、".join(capitulation.get("fired_signals") or [])
        return RegimeDecision(
            state="capitulation",
            r_multiplier=R_MULTIPLIER["capitulation"],
            new_adds_allowed=True,
            hedge_directive="停止 press shorts；回补空头；wash-out 确认后保留少量保险即可。",
            victim_action="victim 名单转向：对最高 beta 的 wash-out 名字买 LEAPS call / call debit spread（凸性向上）。",
            rationale=f"CAPITULATION：抄底雷达 {fired}/5 触发（{names}）。"
            "停止做空，新加仓重开至 full size，basket 往高 beta 倾斜。",
            signals=signals,
        )

    if tape_broken:
        if ema50_streak is not None and ema50_streak <= -PRESS_CONFIRM_DAYS:
            reason = (f"SMH 连续 {abs(ema50_streak)} 日收于 EMA50 下"
                      f"(≥{PRESS_CONFIRM_DAYS} 日确认破位)")
        elif smh_above_ema50 is False:
            reason = "SMH 跌破 EMA50"
        else:
            reason = "SMH EMA50 trendline 破位"
        return RegimeDecision(
            state="press",
            r_multiplier=R_MULTIPLIER["press"],
            new_adds_allowed=False,
            hedge_directive="维持 wedge 头寸；趋势已确认破位，可压 specific victim shorts。",
            victim_action=f"启动 victim put：从 shortlist 头部 ({top_victim}) 开始挑被 wedge 拖下的标的。",
            rationale=f"PRESS：{reason}。核心仓位减至 0.35x、冻结新加仓,进入压制阶段。",
            signals=signals,
        )

    if tape_breaking or tape_soft or (greed_extreme and wedge_biting):
        if tape_breaking:
            reason = (f"SMH 跌破 EMA50 第 {abs(ema50_streak)} 日,未满 "
                      f"{PRESS_CONFIRM_DAYS} 日确认(fresh break,先减码不清仓)")
        elif tape_soft:
            reason = "SMH 失守 EMA20 但仍站 EMA50（near line 走软，未破位）"
        else:
            reason = f"Extreme Greed ({fg:.0f}/100) 叠加 wedge 生效"
        return RegimeDecision(
            state="confirm",
            r_multiplier=R_MULTIPLIER["confirm"],
            new_adds_allowed=True,
            hedge_directive="加 TLT put-spread；准备 trim 名单；不要在 Greed 顶点追入。",
            victim_action=f"victim shortlist ({top_victim}) 仅监控，等 EMA50 破位再 press。",
            rationale=f"CONFIRM：{reason}。AI-infra 新加仓 scale 到 0.4x，冻结对 stretched 名字的加仓。",
            signals=signals,
        )

    if wedge_biting:
        bits = []
        if tlt_20d is not None and tlt_20d <= TLT_WEDGE_DRAWDOWN_PCT:
            bits.append(f"TLT 20d {tlt_20d:+.1f}%")
        if corr is not None and corr >= CORR_FLIP_THRESHOLD:
            bits.append(f"SMH↔TLT corr {corr:+.2f}(同向=利率主导)")
        if hyg_20d is not None and hyg_20d <= HYG_CREDIT_STRESS_PCT:
            bits.append(f"HYG 20d {hyg_20d:+.1f}%")
        if move_wedge:
            if move_vix_ratio is not None and move_vix_ratio >= MOVE_VIX_RATES_STRESS:
                bits.append(f"MOVE/VIX {move_vix_ratio:.1f}(利率波动主导)")
            else:
                bits.append(f"MOVE {move_level:.0f}↑{move_chg_20d:+.0f}%(债市波动离开平静区)")
        return RegimeDecision(
            state="wedge",
            r_multiplier=R_MULTIPLIER["wedge"],
            new_adds_allowed=True,
            hedge_directive="持有 TBT / short TLT put-spread 作为 wedge；tape 仍完整，继续买但减码。",
            victim_action="victim shortlist 观察即可；wedge 未传导到 tape 之前不 press。",
            rationale=f"WEDGE：{', '.join(bits)}。AI-infra 新加仓 scale 到 0.6x。",
            signals=signals,
        )

    note = "tape 健康，wedge 未咬合"
    if fear_extreme:
        note += f"；F&G 进入 Fear ({fg:.0f}) — 若随后 SMH 破 EMA50 即转 Press"
    return RegimeDecision(
        state="hedge",
        r_multiplier=R_MULTIPLIER["hedge"],
        new_adds_allowed=True,
        hedge_directive="保留默认 hedge size（少量 SPX/HYG 空头或 put-spread）。",
        victim_action="victim shortlist 仅维护，不动作。",
        rationale=f"HEDGE：{note}。AI-infra 新加仓 full size。",
        signals=signals,
    )


def load_bubble_hedge(as_of: str, bubble_root: Path) -> dict[str, Any] | None:
    path = bubble_root / as_of / "bubble_hedge.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_capitulation(as_of: str, root: Path) -> dict[str, Any] | None:
    path = root / as_of / "capitulation_radar.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def render_markdown(as_of: str, decision: RegimeDecision) -> str:
    state_label = {
        "hedge": "HEDGE（常驻基线）",
        "wedge": "WEDGE（楔子咬合）",
        "confirm": "CONFIRM（破位预警）",
        "press": "PRESS（确认压制）",
        "capitulation": "CAPITULATION（抛售衰竭，翻多凸性）",
    }[decision.state]
    s = decision.signals
    lines = [
        f"# 风控引擎 — Hedge/Wedge/Confirm/Press/Capitulation — {as_of}",
        "",
        f"## 当前状态：**{state_label}**",
        "",
        f"- **AI-infra 新加仓 R 乘数**: `{decision.r_multiplier:.2f}x`"
        + ("（冻结）" if not decision.new_adds_allowed else ""),
        f"- **判定**: {decision.rationale}",
        f"- **对冲指引**: {decision.hedge_directive}",
        f"- **Victim 动作**: {decision.victim_action}",
        "",
        "## 信号读数",
        "",
        "| 信号 | 值 | 触发 |",
        "|---|---|---|",
        f"| MOVE (美债波动) | {_fmt(s.get('move_level'))} | <80 平静 / ≥80 上行=楔子 |",
        f"| MOVE 20d 变化 | {_fmt(s.get('move_chg_20d'), '%')} | 债市波动趋势 |",
        f"| VIX (股市波动) | {_fmt(s.get('vix_level'))} | 股市情绪 |",
        f"| MOVE/VIX 比 | {_fmt(s.get('move_vix_ratio'))} | 4-6 中性 / >6 利率主导 |",
        f"| TLT 20d 收益 | {_fmt(s['tlt_ret_20d_pct'], '%')} | rates wedge |",
        f"| HYG 20d 收益 | {_fmt(s['hyg_ret_20d_pct'], '%')} | credit 紧缩 |",
        f"| SMH↔TLT 20d 相关 | {_fmt(s['smh_tlt_corr_20d'])} | 相关性翻转 |",
        f"| Fear & Greed | {_fmt(s['fear_greed_score'])} | 情绪极值 |",
        f"| SMH 站上 EMA20 | {s['smh_above_ema20']} | near line(触发器) |",
        f"| SMH 站上 EMA50 | {s['smh_above_ema50']} | 趋势线(触发器) |",
        f"| EMA50 连续天数 | {s.get('smh_ema50_streak') if s.get('smh_ema50_streak') is not None else 'n/a'} | "
        f"≤-{PRESS_CONFIRM_DAYS}=确认破位/PRESS; <0 但未满=CONFIRM |",
        f"| Trendline break | {s['trendline_break']} | 破位确认 |",
        f"| 抄底雷达 | {_fmt(s.get('capitulation_fired'))}/5 | ≥3=抛售衰竭/翻多 |",
        "",
        "派生标志: "
        + ", ".join(
            f"{k}={s[k]}"
            for k in ("wedge_biting", "move_wedge", "tape_soft", "tape_breaking",
                      "tape_broken", "greed_extreme", "fear_extreme",
                      "capitulation_triggered")
        ),
        "",
        "状态转移顺序: CAPITULATION > PRESS > CONFIRM > WEDGE > HEDGE。",
        "R 乘数只作用于 AI-infra basket 的**新加仓**；既有头寸由各自 risk plan 管理。",
    ]
    return "\n".join(lines) + "\n"


def _fmt(value: Any, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return f"{value:+.2f}{suffix}" if suffix == "%" else f"{value:.3f}"
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--bubble-root", type=Path, default=DEFAULT_BUBBLE_ROOT)
    parser.add_argument("--capitulation-root", type=Path, default=DEFAULT_CAPITULATION_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    _, as_of = resolve_as_of(args.as_of)

    bubble = load_bubble_hedge(as_of, args.bubble_root)
    if bubble is None:
        print(
            f"error: bubble_hedge.json missing for {as_of} under {args.bubble_root}; "
            "run score_bubble_hedge_radar.py first",
            file=sys.stderr,
        )
        return 2

    # Capitulation radar is optional — absent on a normal day, the engine
    # simply never reaches the CAPITULATION state.
    capitulation = load_capitulation(as_of, args.capitulation_root)

    decision = classify_regime(
        bubble.get("wedge") or [],
        bubble.get("confirmation") or {},
        bubble.get("victims") or [],
        capitulation=capitulation,
    )

    signals = decision.signals or {}
    pressure = wedge_pressure(
        corr=signals.get("smh_tlt_corr_20d"),
        move_level=signals.get("move_level"),
        move_rising=bool((signals.get("move_chg_20d") or 0) > 0),
        tlt_ret_20d=signals.get("tlt_ret_20d_pct"),
        hyg_ret_20d=signals.get("hyg_ret_20d_pct"),
    )
    r_cont = continuous_multiplier(
        state=decision.state, base_multiplier=decision.r_multiplier, pressure=pressure)
    context = load_context_signals(as_of)
    dampener = context_dampener(
        mrs=context.get("mrs"),
        mrs_quadrant=context.get("mrs_quadrant"),
        negative_accel_count=int(context.get("negative_accel_count") or 0),
    )
    if decision.state == "hedge":
        r_cont = round(max(R_MULTIPLIER["wedge"], r_cont * dampener), 2)

    payload = {
        "as_of": as_of,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "framework": "hedge_wedge_confirm_press",
        **decision.as_dict(),
        # Shadow continuous multiplier (docs/plans/2026-06-10-production-hardening.md
        # Task 6). Consumers read r_multiplier; flip via QUANT_REGIME_CONTINUOUS=1
        # only after backtest A/B + >=10 trading days of shadow diff review.
        "wedge_pressure": pressure,
        "r_multiplier_continuous": r_cont,
        "r_multiplier_source": "binary",
        "context_dampener": dampener,
        "context_signals": context,
    }
    if os.environ.get("QUANT_REGIME_CONTINUOUS") == "1":
        payload["r_multiplier"] = r_cont
        payload["r_multiplier_source"] = "continuous"
    out_dir = write_radar_outputs(
        args.output_root, as_of, "risk_regime", payload, render_markdown(as_of, decision),
    )
    print(
        f"risk regime: {decision.state.upper()} "
        f"(R x{decision.r_multiplier:.2f}, shadow continuous x{r_cont:.2f} "
        f"pressure {pressure:.2f}) → {out_dir / 'risk_regime.json'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
