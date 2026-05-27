"""Options anomaly + tenor section + private helpers (Phase B.6).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Contains the 2 section renderers + all
options-specific private helpers (parse/format/related-context).
"""
from __future__ import annotations

from typing import Any

from lib.fmt import (
    action_label, clean_table_text, display_tenor_name, fmt_r,
    report_safe_options_context, symbol_key as _symbol_key,
)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if value == "":
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_option_float(value: Any, *, decimals: int = 0) -> str:
    parsed = _parse_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:,.{decimals}f}"


def _format_tenor_ratio_text(evidence: dict[str, Any], *, limit: int = 80) -> str:
    tenors = evidence.get("tenors") or []
    ratios = evidence.get("ratios") or []
    parts: list[str] = []
    for tenor, ratio in zip(tenors, ratios):
        parsed = _parse_float(ratio)
        if parsed is None:
            continue
        parts.append(f"{display_tenor_name(tenor)} {parsed:.1f}x")
    return clean_table_text(" / ".join(parts) or "-", limit)


def _tenor_ratio_at(evidence: dict[str, Any], tenor_name: str) -> str:
    tenors = [str(item or "").lower() for item in (evidence.get("tenors") or [])]
    ratios = evidence.get("ratios") or []
    try:
        idx = tenors.index(tenor_name.lower())
    except ValueError:
        return "-"
    parsed = _parse_float(ratios[idx] if idx < len(ratios) else None)
    return f"{parsed:.1f}x" if parsed is not None else "-"


def _tenor_ref_call_text(evidence: dict[str, Any]) -> str:
    long_call = _parse_float(evidence.get("long_horizon_far_otm_call"))
    monthly_call = _parse_float(evidence.get("monthly_far_otm_call"))
    if long_call is not None:
        return f"LEAPS/long {long_call:,.0f}"
    if monthly_call is not None:
        return f"monthly {monthly_call:,.0f}"
    return "-"


def _options_tenor_reading(signal: dict[str, Any]) -> str:
    pattern = str(signal.get("pattern") or "")
    evidence = signal.get("evidence") or {}
    if pattern == "gamma_trap":
        weekly = _format_option_float(evidence.get("weekly_far_otm_call"))
        monthly = _format_option_float(evidence.get("monthly_far_otm_call"))
        return f"短端 call wall: weekly call {weekly} vs monthly {monthly}; squeeze/timing risk"
    if pattern == "insider_tilt_long_dated_calls":
        long_call = _format_option_float(evidence.get("long_horizon_far_otm_call"))
        weekly = _format_option_float(evidence.get("weekly_far_otm_call"))
        return f"LEAPS/long-dated call concentration: long {long_call} vs weekly {weekly}"
    if pattern == "bullish_conviction_stack":
        return f"multi-tenor call/put tilt: {_format_tenor_ratio_text(evidence)}"
    if pattern == "bearish_stack":
        return f"multi-tenor put/call hedge: {_format_tenor_ratio_text(evidence)}"
    return report_safe_options_context(signal.get("guidance") or pattern, 120)


def _options_symbol(row: dict[str, Any]) -> str:
    return _symbol_key(row.get("symbol") or row.get("ticker"))


def _options_symbol_aliases(row: dict[str, Any]) -> set[str]:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("symbol", "ticker", "primary_ticker", "ticker_aliases")
    )
    for sep in ("/", ",", ";", "|"):
        text = text.replace(sep, " ")
    return {piece.upper().strip() for piece in text.split() if piece.strip()}


def _options_related_context(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}

    def ensure(symbol: str) -> dict[str, str]:
        symbol = _symbol_key(symbol)
        entry = lookup.setdefault(
            symbol,
            {
                "status": "outside current report",
                "module": "-",
                "readiness": "-",
                "action": "do not promote from options alone",
            },
        )
        return entry

    decision = payload.get("production_decision_summary") or {}
    for row in decision.get("actionable") or []:
        if _symbol_key(row.get("market")) != "US":
            continue
        symbol = _symbol_key(row.get("symbol"))
        if not symbol:
            continue
        entry = ensure(symbol)
        entry["status"] = f"production {fmt_r(row.get('size_r'))}"
        entry["readiness"] = clean_table_text(
            row.get("evidence_state") or row.get("ai_infra_evidence_state") or "-",
            42,
        )
        entry["action"] = action_label(row.get("action"))

    for row in decision.get("watch") or []:
        if _symbol_key(row.get("market")) != "US":
            continue
        symbol = _symbol_key(row.get("symbol"))
        if not symbol:
            continue
        entry = ensure(symbol)
        if entry.get("status") == "outside current report":
            entry["status"] = f"watch/{row.get('state') or '0R'}"
            entry["action"] = "watch only / 0R"

    for row in (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []:
        symbol = _symbol_key(row.get("symbol"))
        if not symbol:
            continue
        entry = ensure(symbol)
        if entry.get("status") == "outside current report":
            tier = row.get("production_tier") or row.get("state") or "ranker"
            entry["status"] = f"ranker/{tier}"
        if entry.get("module") == "-":
            entry["module"] = clean_table_text(row.get("ai_infra_module") or row.get("module") or "-", 36)
        if entry.get("readiness") == "-":
            entry["readiness"] = clean_table_text(
                row.get("ai_infra_evidence_state") or row.get("evidence_state") or "-", 42
            )
        if entry.get("action") == "do not promote from options alone":
            entry["action"] = action_label(row.get("production_action") or row.get("size_hint") or "watch only")

    for row in ((payload.get("source_review_calendar") or {}).get("us") or {}).get("rows") or []:
        for symbol in _options_symbol_aliases(row):
            entry = ensure(symbol)
            if entry.get("status") == "outside current report":
                pool = row.get("current_pool") or "source-review"
                tier = row.get("readiness_tier") or "unscored"
                entry["status"] = f"source-review/{pool}"
                entry["action"] = "evidence review first / 0R"
                entry["readiness"] = tier
            if entry.get("module") == "-":
                entry["module"] = clean_table_text(row.get("module") or "-", 36)
            if entry.get("readiness") == "-":
                entry["readiness"] = clean_table_text(
                    row.get("readiness_tier") or row.get("evidence_state") or "-",
                    42,
                )
    return lookup


def _options_related_guidance(status: str, squeeze_score: float, pressure_score: float) -> str:
    if "production" in status:
        if pressure_score > squeeze_score:
            return "production stock: put pressure risk flag"
        return "production stock: call pressure timing flag"
    if "ranker" in status or "watch" in status:
        return "rank/watch: 0R option context"
    if "source-review" in status:
        return "source-review queue: 0R"
    return "outside report: research expansion"


def _options_tenor_related_guidance(status: str, pattern: Any) -> str:
    pattern_text = str(pattern or "")
    if "production" in status:
        if pattern_text == "gamma_trap":
            return "production stock: short-term timing/risk flag"
        if pattern_text == "bearish_stack":
            return "production stock: hedge-pressure flag"
        return "production stock: timing context"
    if "ranker" in status or "watch" in status:
        return "rank/watch: 0R option context"
    if "source-review" in status:
        return "source-review queue: 0R"
    return "outside report: research expansion"


def render_options_tenor_section(payload: dict[str, Any], *, top_n: int = 12) -> list[str]:
    signals = payload.get("options_tenor_signals") or []
    lines = [
        "## US 期权定位 — weekly / LEAPS / put-call",
        "",
        "- 数据源: `options_chain_quotes` 按 DTE 切桶 (weekly 0-9 / biweekly 10-21 / monthly 22-50 / quarterly 51-120 / half_year 121-220 / LEAPS 221+)。",
        "- 用途: 看短端 gamma、LEAPS/远月定位、跨 tenor 的 call/put 或 put/call 倾斜；本节是 0R option context。",
        "- 详细 per-ticker tenor 拆分见 `reports/review_dashboard/us_options_tenor_radar/<date>/options_tenor.md`。",
        "",
    ]
    if not signals:
        lines += ["- 今日无跨 tenor 信号触发。", ""]
        return lines
    lines += [
        "| Symbol | Pattern | Score | Weekly call | Ref/long call | LEAPS ratio | Tenor ratio | Reading |",
        "|---|---|---:|---:|---|---:|---|---|",
    ]
    sorted_sigs = sorted(signals, key=lambda s: -(s.get("score") or 0.0))
    for sig in sorted_sigs[:top_n]:
        try:
            score = float(sig.get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        evidence = sig.get("evidence") or {}
        weekly_call = _format_option_float(evidence.get("weekly_far_otm_call"))
        ref_call = _tenor_ref_call_text(evidence)
        leaps_ratio = _tenor_ratio_at(evidence, "leaps")
        tenor_ratio = _format_tenor_ratio_text(evidence, limit=72)
        reading = _options_tenor_reading(sig)
        lines.append(
            f"| {sig.get('symbol')} | {sig.get('pattern')} | {score:.1f} | "
            f"{weekly_call} | {ref_call} | {leaps_ratio} | {tenor_ratio} | "
            f"{clean_table_text(reading, 110)} |"
        )
    lines.append("")
    related_lookup = _options_related_context(payload)
    by_symbol: dict[str, dict[str, Any]] = {}
    for sig in sorted_sigs:
        symbol = _symbol_key(sig.get("symbol"))
        if not symbol:
            continue
        try:
            score = float(sig.get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        existing = by_symbol.get(symbol)
        if existing is None or score > existing["score"]:
            by_symbol[symbol] = {
                "symbol": symbol,
                "pattern": sig.get("pattern") or "-",
                "score": score,
                "guidance": _options_tenor_reading(sig),
            }
    lines += [
        "### AI-infra 映射",
        "",
        "- 把 weekly/LEAPS/put-call 信号映射回生产票、ranker 观察票和 source-review 队列；Action 是股票侧或研究侧状态。",
        "",
        "| Symbol | Pattern | Score | Report status | Action |",
        "|---|---|---:|---|---|",
    ]
    for row in sorted(by_symbol.values(), key=lambda r: -r["score"])[:top_n]:
        context = related_lookup.get(row["symbol"]) or {}
        status = context.get("status") or "outside current report"
        lines.append(
            f"| {row['symbol']} | {row['pattern']} | {row['score']:.1f} | "
            f"{clean_table_text(status, 36)} | "
            f"{clean_table_text(_options_tenor_related_guidance(status, row['pattern']), 64)} |"
        )
    if not by_symbol:
        lines.append("| - | - | - | - | - |")
    lines.append("")
    return lines


def render_options_anomaly_section(payload: dict[str, Any], *, top_n: int = 8) -> list[str]:
    rows = payload.get("options_anomaly_rows") or []
    if not rows:
        return [
            "## US 期权异常 (far-OTM call/put) — tape/timing 用",
            "",
            "- 当日 AI universe 内无符合阈值的远 OTM 异常 (Σvol ≥ 200, |delta| ≤ 0.20)。",
            "- 工件: `reports/review_dashboard/us_options_anomaly_radar/<date>/options_anomaly.{csv,md}`",
            "",
        ]
    squeeze = sorted(rows, key=lambda r: -(_parse_float(r.get("short_squeeze_score")) or 0.0))[:top_n]
    pressure = sorted(rows, key=lambda r: -(_parse_float(r.get("selling_pressure_score")) or 0.0))[:top_n]
    related_lookup = _options_related_context(payload)
    lines: list[str] = [
        "## US 期权异常 — far-OTM call/put",
        "",
        "- 读法: call-heavy 看 short-squeeze / dealer hedge pressure；put-heavy 看 downside hedge / selling-pressure。",
        "- 表内 P/C raw 是 put volume / call volume；PC z 和 skew z 用来判断这次异动相对历史是否异常。",
        "- Execution: option flow 本节记为 0R context；股票 R 仍看上方可交易名单。",
        "",
        "### Short-Squeeze (call-heavy)",
        "",
        "| Symbol | Spot | Call Vol | Call Vol/OI | Put Vol | P/C raw | PC z | Skew z | Squeeze |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    any_squeeze = False
    for row in squeeze:
        score = _parse_float(row.get("short_squeeze_score")) or 0.0
        if score <= 0:
            continue
        any_squeeze = True
    if not any_squeeze:
        lines.append("| - | - | - | - | - | - | - | - | _今日无 squeeze 候选_ |")
    else:
        for row in squeeze:
            score = _parse_float(row.get("short_squeeze_score")) or 0.0
            if score <= 0:
                continue
            pc_z = _parse_float(row.get("pc_ratio_z"))
            sk_z = _parse_float(row.get("skew_z"))
            pc_raw = _parse_float(row.get("pc_ratio_raw"))
            lines.append(
                f"| {_options_symbol(row)} | "
                f"{_format_option_float(row.get('spot_close'), decimals=2)} | "
                f"{_format_option_float(row.get('far_otm_call_volume'))} | "
                f"{_format_option_float(row.get('far_otm_call_vol_oi_ratio'), decimals=2)} | "
                f"{_format_option_float(row.get('far_otm_put_volume'))} | "
                f"{(f'{pc_raw:.2f}' if pc_raw is not None else '-')} | "
                f"{(f'{pc_z:+.2f}' if pc_z is not None else '-')} | "
                f"{(f'{sk_z:+.2f}' if sk_z is not None else '-')} | "
                f"{score:,.0f} |"
            )
    lines.append("")
    lines += [
        "### Selling-Pressure (put-heavy)",
        "",
        "| Symbol | Spot | Put Vol | Put Vol/OI | Call Vol | P/C raw | PC z | Skew z | Pressure |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    any_pressure = False
    for row in pressure:
        score = _parse_float(row.get("selling_pressure_score")) or 0.0
        if score <= 0:
            continue
        any_pressure = True
        spot = _parse_float(row.get("spot_close"))
        pc_z = _parse_float(row.get("pc_ratio_z"))
        sk_z = _parse_float(row.get("skew_z"))
        pc_raw = _parse_float(row.get("pc_ratio_raw"))
        lines.append(
            f"| {_options_symbol(row)} | "
            f"{_format_option_float(spot, decimals=2)} | "
            f"{_format_option_float(row.get('far_otm_put_volume'))} | "
            f"{_format_option_float(row.get('far_otm_put_vol_oi_ratio'), decimals=2)} | "
            f"{_format_option_float(row.get('far_otm_call_volume'))} | "
            f"{(f'{pc_raw:.2f}' if pc_raw is not None else '-')} | "
            f"{(f'{pc_z:+.2f}' if pc_z is not None else '-')} | "
            f"{(f'{sk_z:+.2f}' if sk_z is not None else '-')} | "
            f"{score:,.0f} |"
        )
    if not any_pressure:
        lines.append("| - | - | - | - | - | - | - | - | _今日无 selling-pressure 候选_ |")
    lines.append("")
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in [*squeeze, *pressure]:
        symbol = _options_symbol(row)
        if not symbol:
            continue
        squeeze_score = _parse_float(row.get("short_squeeze_score")) or 0.0
        pressure_score = _parse_float(row.get("selling_pressure_score")) or 0.0
        existing = by_symbol.get(symbol)
        if existing is None or max(squeeze_score, pressure_score) > max(
            existing["squeeze_score"], existing["pressure_score"]
        ):
            by_symbol[symbol] = {
                "symbol": symbol,
                "squeeze_score": squeeze_score,
                "pressure_score": pressure_score,
            }
    related_rows = sorted(
        by_symbol.values(),
        key=lambda row: -max(row["squeeze_score"], row["pressure_score"]),
    )[:top_n]
    lines += [
        "### Related AI-infra names to watch",
        "",
        "- 把 call/put 异动映射回 production、ranker 和 source-review 状态，方便区分执行票与研究票。",
        "",
        "| Symbol | Alert | Report status | AI module | Readiness | Action |",
        "|---|---|---|---|---|---|",
    ]
    for row in related_rows:
        context = related_lookup.get(row["symbol"]) or {}
        status = context.get("status") or "outside current report"
        alert = f"S {row['squeeze_score']:,.0f} / P {row['pressure_score']:,.0f}"
        guidance = _options_related_guidance(status, row["squeeze_score"], row["pressure_score"])
        context_action = context.get("action") or ""
        action_text = (
            f"{context_action}; {guidance}"
            if context_action and context_action != "do not promote from options alone"
            else guidance
        )
        lines.append(
            f"| {row['symbol']} | {alert} | {clean_table_text(status, 36)} | "
            f"{clean_table_text(context.get('module') or '-', 36)} | "
            f"{clean_table_text(context.get('readiness') or '-', 42)} | "
            f"{clean_table_text(action_text, 60)} |"
        )
    if not related_rows:
        lines.append("| - | - | - | - | - | - |")
    lines.append("")
    return lines
