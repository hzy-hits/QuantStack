"""Bubble hedge radar — apply the Hedge-Wedge-Confirm-Press framework.

Inspired by the "Wedge/Victim/Confirmation" essay. The framework: when an
AI book is exposed to a possible AI-equity bubble, the discipline is not to
short the bubble. Instead:

  A) Wedge      — long the trend that kills it (rates / credit)
  B) Victim     — find the convex-to-downside name *next to* the bubble
  C) Confirmation — wait for trendline break + fundamentals + vol regime shift,
                    then press the specific shorts.

This radar gives the operator a single dashboard that reads each of the three
layers off the existing data:

- Wedge layer:
    TLT / IEF / SHY / TBT (rates), HYG / LQD / JNK (credit),
    XLF (banks), Canadian banks (BMO/RY/TD/BNS/CM), housing (XHB/ITB).
    Trailing 5d/20d/60d returns + AI book rolling beta to TLT.

- Victim layer:
    Inside the AI universe, score names by how convex-to-downside they look:
      - tape stretch: close vs EMA21 / EMA50 distance
      - evidence weakness: ai_infra_evidence_state pending or counterevidence dense
      - leverage proxy: NeoCloud / data-center developer / GPU-rental modules
      - rate sensitivity: high beta to TLT (long-duration cash flow exposure)
    Highest aggregate score = most convex-down victim candidate.

- Confirmation layer:
    AI book equity proxy (SMH) vs EMA20/EMA50/EMA200,
    AI book daily return correlation with TLT (rolling 20d), F&G regime,
    Recent trendline break flag.

Methodology rule: this radar is **risk management context**, not a buy/short
signal. The methodology still forbids shorting names that aren't in source-
review queue with G0-G4 evidence; this radar tells the operator where the
risk concentrations are.

Output: reports/review_dashboard/bubble_hedge_radar/<date>/bubble_hedge.{csv,md,json}
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_AI_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "bubble_hedge_radar"

WEDGE_INSTRUMENTS: list[tuple[str, str, str]] = [
    # (symbol, group, role)
    ("TLT",  "rates",   "20y Treasury — long-duration core hedge"),
    ("IEF",  "rates",   "7-10y Treasury"),
    ("SHY",  "rates",   "1-3y Treasury (short end)"),
    ("TBT",  "rates",   "Short 20y Treasury (rate-up bet)"),
    ("^TNX", "rates",   "10y yield index (level)"),
    ("HYG",  "credit",  "High-yield credit"),
    ("JNK",  "credit",  "High-yield credit (alt)"),
    ("LQD",  "credit",  "IG corporate"),
    ("XLF",  "banks",   "US financials ETF"),
    ("BMO",  "ca_bank", "BMO — CA mortgage exposure"),
    ("RY",   "ca_bank", "Royal Bank of Canada"),
    ("TD",   "ca_bank", "TD Bank"),
    ("BNS",  "ca_bank", "Bank of Nova Scotia"),
    ("CM",   "ca_bank", "CIBC"),
    ("XHB",  "housing", "US homebuilders ETF"),
    ("ITB",  "housing", "US home construction ETF"),
]

AI_BOOK_PROXY_SYMBOLS = ("SMH", "QQQ", "SPY")


@dataclass(frozen=True)
class WedgeRow:
    symbol: str
    group: str
    role: str
    latest_close: float | None
    ret_5d_pct: float | None
    ret_20d_pct: float | None
    ret_60d_pct: float | None
    rolling_beta_vs_smh_20d: float | None


@dataclass(frozen=True)
class VictimRow:
    symbol: str
    company: str
    evidence_state: str
    bfs_depth: str
    module: str
    px_vs_ema21_pct: float | None
    px_vs_ema50_pct: float | None
    beta_vs_tlt_20d: float | None  # negative = long-duration sensitive
    convex_score: float
    reasons: list[str]


@dataclass(frozen=True)
class ConfirmationState:
    smh_close: float | None
    smh_ema20: float | None
    smh_ema50: float | None
    smh_ema200: float | None
    smh_above_ema20: bool | None
    smh_above_ema50: bool | None
    smh_above_ema200: bool | None
    ai_book_vs_tlt_corr_20d: float | None
    fear_greed_score: float | None
    fear_greed_rating: str | None
    trendline_break: bool
    # Cross-asset volatility — MOVE is the most direct wedge gauge (Treasury
    # implied vol); VIX is equity vol; the ratio reads which side stress is on.
    move_level: float | None = None
    move_chg_20d: float | None = None
    vix_level: float | None = None
    move_vix_ratio: float | None = None


def _load_ai_universe(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            for piece in (row.get("ticker") or "").split("/"):
                t = piece.strip().upper()
                if t and t.isalpha() and len(t) <= 5:
                    out.add(t)
    return out


def _load_universe_records(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            for piece in (row.get("ticker") or "").split("/"):
                t = piece.strip().upper()
                if t:
                    out[t] = row
    return out


def _series(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date, days: int = 260) -> list[tuple[date, float]]:
    rows = con.execute(
        """
        SELECT date, close FROM prices_daily
        WHERE symbol=? AND date >= CAST(? AS DATE) AND date <= CAST(? AS DATE) AND close IS NOT NULL
        ORDER BY date
        """,
        [symbol, (as_of - timedelta(days=days * 2)).isoformat(), as_of.isoformat()],
    ).fetchall()
    out: list[tuple[date, float]] = []
    for d, c in rows:
        if isinstance(d, str):
            try:
                d = date.fromisoformat(d)
            except ValueError:
                continue
        out.append((d, float(c)))
    return out


def _trailing_return(closes: list[float], periods: int) -> float | None:
    if len(closes) <= periods or closes[-1 - periods] == 0:
        return None
    return (closes[-1] / closes[-1 - periods] - 1.0) * 100.0


def _daily_returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for prev, cur in zip(closes, closes[1:]):
        if prev > 0:
            out.append(cur / prev - 1.0)
    return out


def _rolling_beta(asset: list[float], market: list[float], window: int = 20) -> float | None:
    if len(asset) < window or len(market) < window:
        return None
    a = asset[-window:]
    m = market[-window:]
    mean_a = sum(a) / window
    mean_m = sum(m) / window
    var_m = sum((x - mean_m) ** 2 for x in m)
    cov = sum((a[i] - mean_a) * (m[i] - mean_m) for i in range(window))
    if var_m <= 0:
        return None
    return max(-5.0, min(5.0, cov / var_m))


def _rolling_corr(a: list[float], b: list[float], window: int = 20) -> float | None:
    if len(a) < window or len(b) < window:
        return None
    a = a[-window:]
    b = b[-window:]
    mean_a = sum(a) / window
    mean_b = sum(b) / window
    var_a = sum((x - mean_a) ** 2 for x in a)
    var_b = sum((x - mean_b) ** 2 for x in b)
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(window))
    if var_a <= 0 or var_b <= 0:
        return None
    denom = math.sqrt(var_a * var_b)
    return max(-1.0, min(1.0, cov / denom))


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def build_wedge_layer(con: duckdb.DuckDBPyConnection, as_of: date) -> list[WedgeRow]:
    smh = _series(con, "SMH", as_of)
    smh_returns = _daily_returns([c for _, c in smh])
    out: list[WedgeRow] = []
    for sym, group, role in WEDGE_INSTRUMENTS:
        s = _series(con, sym, as_of)
        if not s:
            out.append(WedgeRow(sym, group, role, None, None, None, None, None))
            continue
        closes = [c for _, c in s]
        ret_5d = _trailing_return(closes, 5)
        ret_20d = _trailing_return(closes, 20)
        ret_60d = _trailing_return(closes, 60)
        returns = _daily_returns(closes)
        beta = _rolling_beta(returns, smh_returns, window=20)
        out.append(WedgeRow(sym, group, role, closes[-1], ret_5d, ret_20d, ret_60d, beta))
    return out


def _victim_module_weight(module: str) -> float:
    """Module categories that are convex-down on a rate-driven AI rerating."""
    m = (module or "").lower()
    # NeoCloud / DC developer / GPU rental: most levered to AI capex cycle
    if "neocloud" in m or "data center developer" in m or "gpu-as-a-service" in m or "gpu rental" in m:
        return 1.0
    # Wafer-scale / advanced packaging / IPO names: long-duration cash flows
    if "wafer" in m or "advanced packaging" in m or "abf" in m or "hbm" in m:
        return 0.7
    # Optical / SerDes / EDA: medium
    if "optical" in m or "serdes" in m or "eda" in m or "ip royalty" in m:
        return 0.5
    return 0.3


def _victim_score(
    rec: dict[str, Any],
    px_vs_ema21: float | None,
    px_vs_ema50: float | None,
    beta_vs_tlt: float | None,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    evidence_state = str(rec.get("evidence_state") or "")
    if "原文已证明" not in evidence_state and "合理推论" not in evidence_state:
        score += 25
        reasons.append("evidence_pending")
    counter = str(rec.get("counterevidence") or "")
    counter_items = len([p for p in counter.replace("，", ",").replace("；", ",").split(",") if p.strip()])
    if counter_items >= 3:
        score += 10
        reasons.append(f"counter_items_{counter_items}")
    module_weight = _victim_module_weight(str(rec.get("module") or ""))
    score += module_weight * 30
    if module_weight >= 0.7:
        reasons.append("convex_module")
    if px_vs_ema21 is not None and px_vs_ema21 > 15.0:
        score += 15
        reasons.append(f"stretched_ema21_{px_vs_ema21:+.0f}%")
    if px_vs_ema50 is not None and px_vs_ema50 > 25.0:
        score += 10
        reasons.append(f"stretched_ema50_{px_vs_ema50:+.0f}%")
    # Negative beta to TLT = long-duration sensitive (rates up → stock down)
    if beta_vs_tlt is not None and beta_vs_tlt < -0.3:
        score += 10
        reasons.append(f"rate_sensitive_beta_tlt_{beta_vs_tlt:+.2f}")
    return round(score, 1), reasons


def build_victim_layer(
    con: duckdb.DuckDBPyConnection,
    ai_universe: set[str],
    universe_records: dict[str, dict[str, Any]],
    as_of: date,
    top_n: int = 15,
) -> list[VictimRow]:
    tlt = _series(con, "TLT", as_of)
    tlt_returns = _daily_returns([c for _, c in tlt])
    rows: list[VictimRow] = []
    for symbol in sorted(ai_universe):
        rec = universe_records.get(symbol) or {}
        if not rec:
            continue
        # Only consider US-tradable single-ticker rows for victim scoring;
        # foreign tickers are skipped because we don't reliably have their prices.
        if "." in symbol or len(symbol) > 5:
            continue
        s = _series(con, symbol, as_of)
        if len(s) < 60:
            continue
        closes = [c for _, c in s]
        ema21 = _ema(closes, 21)[-1]
        ema50 = _ema(closes, 50)[-1]
        px = closes[-1]
        px_vs_ema21 = (px / ema21 - 1.0) * 100.0 if ema21 else None
        px_vs_ema50 = (px / ema50 - 1.0) * 100.0 if ema50 else None
        returns = _daily_returns(closes)
        beta_tlt = _rolling_beta(returns, tlt_returns, window=20)
        score, reasons = _victim_score(rec, px_vs_ema21, px_vs_ema50, beta_tlt)
        rows.append(VictimRow(
            symbol=symbol,
            company=str(rec.get("company") or ""),
            evidence_state=str(rec.get("evidence_state") or ""),
            bfs_depth=str(rec.get("bfs_depth") or ""),
            module=str(rec.get("module") or ""),
            px_vs_ema21_pct=round(px_vs_ema21, 2) if px_vs_ema21 is not None else None,
            px_vs_ema50_pct=round(px_vs_ema50, 2) if px_vs_ema50 is not None else None,
            beta_vs_tlt_20d=round(beta_tlt, 3) if beta_tlt is not None else None,
            convex_score=score,
            reasons=reasons,
        ))
    rows.sort(key=lambda r: -r.convex_score)
    return rows[:top_n]


def _load_fear_greed(as_of: date) -> tuple[float | None, str | None]:
    path = STACK_ROOT / "reports" / "review_dashboard" / "fear_greed" / as_of.isoformat() / "fear_greed.json"
    if not path.exists():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, None
    return data.get("score"), data.get("rating")


def build_confirmation_layer(con: duckdb.DuckDBPyConnection, as_of: date) -> ConfirmationState:
    smh = _series(con, "SMH", as_of)
    smh_closes = [c for _, c in smh]
    smh_ema20 = _ema(smh_closes, 20)[-1] if smh_closes else None
    smh_ema50 = _ema(smh_closes, 50)[-1] if smh_closes else None
    smh_ema200 = _ema(smh_closes, 200)[-1] if len(smh_closes) >= 200 else None
    smh_last = smh_closes[-1] if smh_closes else None
    tlt = _series(con, "TLT", as_of)
    tlt_returns = _daily_returns([c for _, c in tlt])
    smh_returns = _daily_returns(smh_closes)
    corr_20 = _rolling_corr(smh_returns, tlt_returns, window=20)
    fg_score, fg_rating = _load_fear_greed(as_of)

    trendline_break = False
    if smh_closes and smh_ema50 is not None:
        # Simple flag: 2-day close BELOW EMA50 after being above it
        recent = smh_closes[-5:]
        ema_recent = _ema(smh_closes, 50)[-5:]
        below_now = recent[-1] < ema_recent[-1] if recent else False
        was_above = any(r > e for r, e in zip(recent[:-2], ema_recent[:-2]))
        trendline_break = below_now and was_above

    # Cross-asset volatility: MOVE (Treasury vol) + VIX (equity vol).
    move_series = [c for _, c in _series(con, "^MOVE", as_of)]
    vix_series = [c for _, c in _series(con, "^VIX", as_of)]
    move_level = move_series[-1] if move_series else None
    vix_level = vix_series[-1] if vix_series else None
    move_chg_20d = None
    if len(move_series) > 20 and move_series[-21]:
        move_chg_20d = (move_series[-1] - move_series[-21]) / move_series[-21] * 100.0
    move_vix_ratio = (
        move_level / vix_level if (move_level and vix_level) else None
    )

    return ConfirmationState(
        smh_close=round(smh_last, 2) if smh_last is not None else None,
        smh_ema20=round(smh_ema20, 2) if smh_ema20 is not None else None,
        smh_ema50=round(smh_ema50, 2) if smh_ema50 is not None else None,
        smh_ema200=round(smh_ema200, 2) if smh_ema200 is not None else None,
        smh_above_ema20=(smh_last > smh_ema20) if (smh_last and smh_ema20) else None,
        smh_above_ema50=(smh_last > smh_ema50) if (smh_last and smh_ema50) else None,
        smh_above_ema200=(smh_last > smh_ema200) if (smh_last and smh_ema200) else None,
        ai_book_vs_tlt_corr_20d=round(corr_20, 3) if corr_20 is not None else None,
        fear_greed_score=fg_score,
        fear_greed_rating=fg_rating,
        trendline_break=trendline_break,
        move_level=round(move_level, 2) if move_level is not None else None,
        move_chg_20d=round(move_chg_20d, 2) if move_chg_20d is not None else None,
        vix_level=round(vix_level, 2) if vix_level is not None else None,
        move_vix_ratio=round(move_vix_ratio, 2) if move_vix_ratio is not None else None,
    )


def derive_guidance(
    wedge: list[WedgeRow],
    victims: list[VictimRow],
    confirm: ConfirmationState,
) -> list[str]:
    """Map state → operator guidance per the Hedge-Wedge-Confirm-Press framework."""
    notes: list[str] = []
    # Wedge state: rates trending up = wedge biting; TLT 20d return negative is the proxy
    tlt = next((w for w in wedge if w.symbol == "TLT"), None)
    if tlt and tlt.ret_20d_pct is not None and tlt.ret_20d_pct <= -2.0:
        notes.append(
            f"**Wedge: 利率上行进行中** — TLT 20d {tlt.ret_20d_pct:+.1f}%。"
            "AI book 的 long-duration cash-flow 估值受压。指引：保留 TBT / short TLT put-spread 作为 wedge。"
        )
    elif tlt and tlt.ret_20d_pct is not None and tlt.ret_20d_pct >= 2.0:
        notes.append(
            f"**Wedge: 利率回落** — TLT 20d {tlt.ret_20d_pct:+.1f}%。Wedge 暂未生效，"
            "AI rerating 风险降温，但留少量保险 (TBT 小仓位)。"
        )
    else:
        notes.append("**Wedge: 利率横盘** — TLT 20d 波动 < 2%；保留默认 hedge size。")

    hyg = next((w for w in wedge if w.symbol == "HYG"), None)
    if hyg and hyg.ret_20d_pct is not None and hyg.ret_20d_pct <= -1.0:
        notes.append(
            f"**Credit 紧缩信号** — HYG 20d {hyg.ret_20d_pct:+.1f}%；"
            "AI 重资产 (NeoCloud / DC developer) 融资成本预期上升，是 Victim 候选放大器。"
        )

    if confirm.fear_greed_score and confirm.fear_greed_score >= 75:
        notes.append(
            f"**Confirmation: Fear & Greed 进入 Extreme Greed ({confirm.fear_greed_score:.0f}/100)** — "
            "顺风时段，但 vol 已贵；不要在 Greed 顶点追入；准备 trim 名单。"
        )
    elif confirm.fear_greed_score and confirm.fear_greed_score <= 30:
        notes.append(
            f"**Confirmation: F&G 进入 Fear ({confirm.fear_greed_score:.0f}/100)** — "
            "若同时 SMH 跌破 EMA50 → 这是 Press 的窗口。"
        )

    if confirm.smh_above_ema50 is False:
        notes.append(
            f"**Confirmation: SMH 跌破 EMA50 ({confirm.smh_close:.2f} vs {confirm.smh_ema50:.2f})** — "
            "AI tape 趋势线确认破位；可以 Press specific victim shorts。"
        )
    if confirm.trendline_break:
        notes.append("**Confirmation: SMH 出现 EMA50 trendline 破位** — 风险升级，操作员复核 victim 池。")

    if confirm.ai_book_vs_tlt_corr_20d is not None and confirm.ai_book_vs_tlt_corr_20d <= -0.5:
        notes.append(
            f"**Correlation flip** — SMH ↔ TLT 20d 相关 {confirm.ai_book_vs_tlt_corr_20d:+.2f}；"
            "强负相关 = AI 对利率敏感性放大，wedge 直接生效。"
        )

    if victims:
        top = victims[0]
        notes.append(
            f"**Victim shortlist 头部**: {top.symbol} ({top.company}) — score {top.convex_score:.1f}；"
            f"reasons: {', '.join(top.reasons[:4])}。"
            f"指引：仅在 Confirmation 进入 Press 阶段后，从此名单开始挑被 wedge 拖下的目标。"
        )

    notes.append(
        "**总策略**: Hedge → Wedge → Confirm → Press。不要 short 正在 parabolic 的 AI 名字；"
        "通过 TBT / TLT put-spread 持有 wedge 头寸；耐心等待 Confirmation；然后才从 Victim shortlist 精挑。"
    )
    return notes


def render_markdown(
    target: date,
    wedge: list[WedgeRow],
    victims: list[VictimRow],
    confirm: ConfirmationState,
    guidance: list[str],
) -> str:
    lines: list[str] = [
        f"# Bubble Hedge Radar - {target.isoformat()}",
        "",
        "应用 Hedge-Wedge-Confirm-Press 框架 (Tier1 essay)：不要 short 正在 parabolic 的 AI；",
        "找 wedge (利率/信用) long、找 victim (旁边的脆弱标的) 准备 short、等 confirmation (趋势线破位)。",
        "",
        "## 操作指引 (per current state)",
        "",
    ]
    for note in guidance:
        lines.append(f"- {note}")
    lines.append("")

    lines += [
        "## A) Wedge layer — 利率 / 信用 / 银行 / 房产",
        "",
        "| Symbol | Group | Role | Close | 5d | 20d | 60d | β vs SMH 20d |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in wedge:
        close = f"{row.latest_close:.2f}" if row.latest_close is not None else "-"
        r5 = f"{row.ret_5d_pct:+.2f}%" if row.ret_5d_pct is not None else "-"
        r20 = f"{row.ret_20d_pct:+.2f}%" if row.ret_20d_pct is not None else "-"
        r60 = f"{row.ret_60d_pct:+.2f}%" if row.ret_60d_pct is not None else "-"
        beta = f"{row.rolling_beta_vs_smh_20d:+.2f}" if row.rolling_beta_vs_smh_20d is not None else "-"
        lines.append(
            f"| {row.symbol} | {row.group} | {row.role} | {close} | {r5} | {r20} | {r60} | {beta} |"
        )
    lines.append("")

    lines += [
        "## B) Victim shortlist — AI universe 内的脆弱标的",
        "",
        "Convex-to-downside 评分（高分 = 更脆弱）。组合权重：",
        "- evidence_pending +25 / counter_items≥3 +10 / convex_module +21 / stretched_ema21>15% +15 / stretched_ema50>25% +10 / rate_sensitive (β vs TLT < -0.3) +10",
        "",
        "| Symbol | Company | Depth | Module | Evidence | px vs EMA21 | px vs EMA50 | β vs TLT | Score | Reasons |",
        "|---|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for v in victims:
        ema21 = f"{v.px_vs_ema21_pct:+.1f}%" if v.px_vs_ema21_pct is not None else "-"
        ema50 = f"{v.px_vs_ema50_pct:+.1f}%" if v.px_vs_ema50_pct is not None else "-"
        beta = f"{v.beta_vs_tlt_20d:+.2f}" if v.beta_vs_tlt_20d is not None else "-"
        ev = v.evidence_state[:40] if v.evidence_state else "—"
        lines.append(
            f"| {v.symbol} | {v.company[:24]} | {v.bfs_depth} | {v.module[:30]} | {ev} | "
            f"{ema21} | {ema50} | {beta} | {v.convex_score:.1f} | {', '.join(v.reasons[:4])} |"
        )
    lines.append("")

    lines += [
        "## C) Confirmation layer — 趋势 / 相关性 / Fear & Greed",
        "",
        f"- SMH close: {confirm.smh_close} (EMA20 {confirm.smh_ema20} / EMA50 {confirm.smh_ema50} / EMA200 {confirm.smh_ema200})",
        f"- SMH 站上 EMA20 / EMA50 / EMA200: "
        f"{'YES' if confirm.smh_above_ema20 else 'NO'} / "
        f"{'YES' if confirm.smh_above_ema50 else 'NO'} / "
        f"{'YES' if confirm.smh_above_ema200 else 'NO'}",
        f"- SMH ↔ TLT 20d correlation: {confirm.ai_book_vs_tlt_corr_20d}",
        f"- Fear & Greed: {confirm.fear_greed_score} ({confirm.fear_greed_rating})",
        f"- EMA50 trendline 破位: {'YES (Confirmation 触发)' if confirm.trendline_break else 'NO'}",
        "",
        "## 用法",
        "",
        "1. 每天看 A) wedge 是否在生效（TLT 20d ≤ -2% 通常 = rates 进攻状态）。",
        "2. 看 B) victim 头部分（score > 60）作为预备 short 池。",
        "3. **不要主动 short** 任何 victim，直到 C) confirmation 触发（SMH 跌破 EMA50 + F&G 从 Greed 滑出）。",
        "4. Confirmation 触发后才从 victim shortlist 挑 1-2 个；wedge 仓位先行（TBT / TLT put-spread）。",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(
    target: date,
    wedge: list[WedgeRow],
    victims: list[VictimRow],
    confirm: ConfirmationState,
    guidance: list[str],
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "bubble_hedge.md").write_text(
        render_markdown(target, wedge, victims, confirm, guidance), encoding="utf-8"
    )
    # JSON state for downstream
    payload = {
        "as_of": target.isoformat(),
        "wedge": [w.__dict__ for w in wedge],
        "victims": [v.__dict__ for v in victims],
        "confirmation": confirm.__dict__,
        "guidance": guidance,
    }
    (out_dir / "bubble_hedge.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-universe", type=Path, default=DEFAULT_AI_UNIVERSE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--top-n", type=int, default=15)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of_text = args.as_of or cst.date().isoformat()
    as_of = date.fromisoformat(as_of_text)

    ai_universe = _load_ai_universe(args.ai_universe)
    universe_records = _load_universe_records(args.ai_universe)
    con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        wedge = build_wedge_layer(con, as_of)
        victims = build_victim_layer(con, ai_universe, universe_records, as_of, top_n=args.top_n)
        confirm = build_confirmation_layer(con, as_of)
    finally:
        con.close()
    guidance = derive_guidance(wedge, victims, confirm)
    out_dir = args.output_root / as_of_text
    write_outputs(as_of, wedge, victims, confirm, guidance, out_dir)
    print(
        f"Bubble hedge radar written: {out_dir / 'bubble_hedge.md'}; "
        f"wedge={len(wedge)} victims={len(victims)} guidance={len(guidance)} "
        f"trendline_break={confirm.trendline_break}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
