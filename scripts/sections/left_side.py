"""左侧观察池 sections (Phase B.5).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Contains:
  - REGIME_TILT_TABLE constant + regime_left_right_tilt + render_regime_tilt_header
  - cn_left_side_watch_rows + render_cn_left_side_watch_section
  - render_us_left_side_section (reads us_mean_reversion_radar CSV)
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from lib.fmt import clean_table_text, fmt_pct, round_or_none


REGIME_TILT_TABLE = {
    "hedge":        {"right": 75, "left": 25, "limit": 8,  "stance": "右侧动量优先，左侧仅观察"},
    "wedge":        {"right": 60, "left": 40, "limit": 12, "stance": "rates/credit 楔形，左右平衡"},
    "confirm":      {"right": 40, "left": 60, "limit": 18, "stance": "下行确认，左侧候选权重抬升"},
    "press":        {"right": 25, "left": 75, "limit": 24, "stance": "press 期，左侧 mean-reversion 主场"},
    "capitulation": {"right": 15, "left": 85, "limit": 30, "stance": "panic 出清，左侧 oversold 黄金窗口"},
}


def regime_left_right_tilt(state: str | None) -> dict[str, Any]:
    key = str(state or "hedge").strip().lower()
    return REGIME_TILT_TABLE.get(key, REGIME_TILT_TABLE["hedge"])


def render_regime_tilt_header(payload: dict[str, Any], *, regime_key: str = "risk_regime") -> tuple[list[str], int]:
    """Return (markdown header lines, recommended left-side display limit)."""
    regime = payload.get(regime_key) or {}
    state = str(regime.get("state") or "hedge").lower()
    mult = round_or_none(regime.get("r_multiplier"))
    tilt = regime_left_right_tilt(state)
    mult_txt = f"{mult:.2f}x" if mult is not None else "-"
    state_zh = {
        "hedge": "防守-基准", "wedge": "rates/credit 楔形",
        "confirm": "下行确认", "press": "press 期", "capitulation": "panic 出清",
    }.get(state, state)
    lines = [
        f"- 当前 tape:**{state_zh}** (R 乘子 {mult_txt});broad_signal 内部权重已切到 右 {tilt['right']}% / 左 {tilt['left']}%,{tilt['stance']}。",
    ]
    return lines, int(tilt["limit"])


def cn_left_side_watch_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return CN ranker rows that look like left-side (oversold/contrarian) ideas."""
    ranker = (payload.get("cn_opportunity_ranker") or {})
    sources = (ranker.get("all_rows") or [])
    if not sources:
        sources = (payload.get("cn") or {}).get("ranked_watch_rows") or []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sources:
        family = str(row.get("strategy_family") or row.get("policy") or "")
        sleeve = str(row.get("alpha_sleeve_id") or "")
        is_left_side = (
            family == "oversold_contrarian"
            or sleeve.startswith("cn_oversold")
            or family.endswith("_oversold_reversion")
            or "oversold" in sleeve
        )
        if not is_left_side:
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(row)
    out.sort(key=lambda r: (-(round_or_none(r.get("ev_lcb80_pct")) or -999.0), r.get("rank") or 9_999))
    return out


def render_cn_left_side_watch_section(payload: dict[str, Any], *, limit: int | None = None) -> list[str]:
    rows = cn_left_side_watch_rows(payload)
    tilt_lines, regime_limit = render_regime_tilt_header(payload, regime_key="cn_risk_regime")
    effective_limit = limit if limit is not None else regime_limit
    lines = [
        "## A 股左侧观察池",
        "",
        "强 tape 期主线左侧 sleeve 会被压到 0R,这里保留所有 EV-positive 的超跌候选 —— 哪些进做左右混合,操作员自己判断。",
        *tilt_lines,
        "",
    ]
    if not rows:
        lines += [
            "今天 CN producer 没有 oversold_contrarian 候选,池子是空的。连续多日为空就要回头查 `strategy_model_dataset` 是不是真的在生成左侧数据。",
            "",
        ]
        return lines
    lines += [
        "| Symbol | Name | EV LCB80 | 1D | 5D | Pool | State | Reason |",
        "|---|---|---:|---:|---:|---|---|---|",
    ]
    for row in rows[:effective_limit]:
        ev = row.get("ev_lcb80_pct")
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("symbol") or "-"),
                    clean_table_text(row.get("name") or "-", 22),
                    fmt_pct(ev) if ev is not None else "-",
                    fmt_pct(row.get("pct_chg")) if row.get("pct_chg") is not None else "-",
                    fmt_pct(row.get("ret_5d_pct")) if row.get("ret_5d_pct") is not None else "-",
                    str(row.get("ai_infra_current_pool") or "-"),
                    str(row.get("state") or "-"),
                    clean_table_text(str(row.get("ev_action_reason") or row.get("handling_reason") or "-"), 60),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _latest_dated_subdir(root: Path, as_of: str) -> str | None:
    """Latest YYYY-MM-DD subdir under root with name <= as_of, else None."""
    if not root.exists():
        return None
    candidates = sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and len(d.name) == 10 and d.name <= as_of
    )
    return candidates[-1] if candidates else None


def render_us_left_side_section(
    payload: dict[str, Any],
    *,
    us_mean_reversion_root: Path,
    limit: int | None = None,
) -> list[str]:
    """Surface today's US mean-reversion candidates (left-side picks).

    Display limit auto-adjusts with risk_regime: hedge=8, capitulation=30.
    Takes us_mean_reversion_root explicitly (passed from main's STACK_ROOT context).
    """
    tilt_lines, regime_limit = render_regime_tilt_header(payload, regime_key="risk_regime")
    effective_limit = limit if limit is not None else regime_limit
    lines = [
        "## US 左侧观察池",
        "",
        "右侧动量之外的超跌反弹候选:AI universe 里站在 EMA21 下方的名字,按下破幅度排序。这层不进 actionable R,仅作观察。",
        *tilt_lines,
        "",
    ]
    as_of = str(payload.get("as_of") or "")
    us_status = payload.get("us_market_data_status") or {}
    effective_as_of = (
        str(us_status.get("effective_us_market_date") or "")
        or str(us_status.get("prices_daily_latest_date") or "")
        or as_of
    )
    fallback_date = _latest_dated_subdir(us_mean_reversion_root, effective_as_of) if effective_as_of else None
    if fallback_date is None:
        lines += ["- mean-reversion radar 当日未产出", ""]
        return lines
    path = us_mean_reversion_root / fallback_date / "mean_reversion_radar.csv"
    if not path.exists():
        lines += ["- mean-reversion radar 当日未产出", ""]
        return lines

    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    ai_rows = [r for r in rows if str(r.get("in_ai_universe") or "").lower() == "yes"]
    pullback = []
    for r in ai_rows:
        try:
            dist = float(r.get("dist_close_ema21_pct") or 0.0)
        except (TypeError, ValueError):
            continue
        if dist <= 0.0:
            r["_dist_ema21"] = dist
            pullback.append(r)

    if not pullback:
        lines += [
            f"- 数据日 {fallback_date}: AI universe 内**没有名字站在 EMA21 下方**",
            "  (整个篮子都在均线之上 → 今日无左侧机会,全篮子右侧)",
            "",
        ]
        return lines

    pullback.sort(key=lambda r: (
        0 if str(r.get("is_mean_reversion_candidate") or "").lower() == "yes" else 1,
        r["_dist_ema21"],
    ))
    lines += [
        f"- 数据日: {fallback_date} | AI universe 跌破 EMA21 候选: {len(pullback)}",
        "",
        "| Symbol | Company | 5d | 20d | vs EMA21 | vs EMA50 | Cand? | Reason |",
        "|---|---|---:|---:|---:|---:|:---:|---|",
    ]
    for r in pullback[:effective_limit]:
        cand = "✓" if str(r.get("is_mean_reversion_candidate") or "").lower() == "yes" else "-"
        lines.append(
            f"| {r.get('symbol','-')} | {clean_table_text(r.get('company_name','-'), 22)} | "
            f"{r.get('ret_5d_pct','-')}% | {r.get('ret_20d_pct','-')}% | "
            f"{r.get('dist_close_ema21_pct','-')}% | {r.get('dist_close_ema50_pct','-')}% | "
            f"{cand} | {clean_table_text(r.get('reasons') or r.get('valuation_signal') or '-', 50)} |"
        )
    lines.append("")
    return lines
