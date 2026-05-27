"""Long-form report wrappers (Phase B.15).

Extracted from scripts/generate_main_strategy_v2_report.py — these are
the "produce-a-full-standalone-md-file" variants of corresponding _section
renderers. Each is a thin header+section wrapper that returns str.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_bool, fmt_num, fmt_pct, fmt_r

from sections.audits_calendars import (
    render_portfolio_risk_overlay_section,
    render_option_shadow_ledger_section,
)
from sections.ai_supercycle import (
    render_ai_supercycle_evidence_section,
    render_ai_supply_chain_relationships_section,
    render_ai_supercycle_value_radar_section,
)
from sections.rankers import (
    render_ai_supercycle_layer_attribution_section,
    render_ai_lab_quality_index_section,
)


def render_profit_readiness(payload: dict[str, Any]) -> str:
    readiness = payload.get("profit_readiness") or {}
    summary = readiness.get("summary") or {}
    lines = [
        f"# Profit Readiness - {payload['as_of']}",
        "",
        "This report translates research edges into money-readiness blockers. It does not guarantee profit; it shows what still prevents research EV from becoming controlled live PnL.",
        "",
        f"- Money-ready lines: `{summary.get('money_ready_lines', 0)}`",
        f"- Today bias: {summary.get('today_bias') or '-'}",
        f"- Highest priority blocker: {summary.get('highest_priority_blocker') or '-'}",
        "",
        "| Priority | Area | State | Allowed now | Evidence | Blocker | Next step |",
        "|---:|---|---|---|---|---|---|",
    ]
    for row in readiness.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('allowed_now')} | {row.get('evidence')} | {row.get('blocker')} | {row.get('next_step')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_pipeline_requirements_audit(payload: dict[str, Any]) -> str:
    audit = payload.get("pipeline_requirements_audit") or {}
    summary = audit.get("summary") or {}
    lines = [
        f"# Pipeline Requirements Audit - {payload['as_of']}",
        "",
        "This is the production-contract audit for the current pipeline. A fail here means the report may rank names, but should not pretend the row is executable.",
        "",
        f"- Fail count: `{summary.get('fail_count', 0)}`",
        f"- Top blocker: {summary.get('top_blocker') or '-'}",
        f"- Production bias: {summary.get('production_bias') or '-'}",
        "",
        "| Priority | Area | State | Evidence | Requirement | Next change |",
        "|---:|---|---|---|---|---|",
    ]
    for row in audit.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('evidence')} | {row.get('requirement')} | {row.get('next_change')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_portfolio_risk_overlay(payload: dict[str, Any]) -> str:
    overlay = payload.get("portfolio_risk_overlay") or {}
    summary = overlay.get("summary") or {}
    lines = [
        f"# Portfolio Risk Overlay - {payload['as_of']}",
        "",
        f"- Candidates: {summary.get('candidate_count', 0)}",
        f"- Long alpha R: {fmt_num(summary.get('long_alpha_r'), 4)}",
        f"- Planned beta hedge R: {fmt_num(summary.get('beta_hedge_r'), 4)}",
        f"- Net beta R after hedge: {fmt_num(summary.get('net_beta_r'), 4)}",
        f"- VaR95 R proxy: {fmt_num(summary.get('var95_r_proxy'), 4)}",
        f"- Hedged VaR95 R proxy: {fmt_num(summary.get('hedged_var95_r_proxy'), 4)}",
        "",
        "| Market | Symbol | State | Sector | Base R | Long R | Hedge | Beta | Net beta R | Auto | Shadow haircut | Reasons |",
        "|---|---|---|---|---:|---:|---|---:|---:|---|---:|---|",
    ]
    attribution = summary.get("risk_attribution") or {}
    if summary.get("hedge_basis_risk"):
        lines.insert(
            8,
            f"- Hedge basis risk: hedged VaR proxy is +{fmt_num(attribution.get('basis_risk_delta_r'), 4)}R above unhedged; hedge lowers market beta, not single-name/basis risk.",
        )
    for row in overlay.get("rows") or []:
        lines.append(
            f"| {row.get('market')} | {row.get('symbol')} | {row.get('state')} | {row.get('sector')} | "
            f"{fmt_num(row.get('base_r'), 4)} | {fmt_num(row.get('final_r'), 4)} | "
            f"{row.get('hedge_instrument') or '-'} {fmt_num(row.get('hedge_notional_r'), 4)}R | {fmt_num(row.get('hedge_beta'), 2)} | "
            f"{fmt_r(row.get('net_beta_r'))} | {fmt_bool(bool(row.get('auto_eligible')))} | "
            f"{fmt_num(row.get('shadow_option_haircut'), 2)} | {', '.join(row.get('risk_reasons') or [])} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_option_shadow_ledger(payload: dict[str, Any]) -> str:
    ledger = payload.get("option_shadow_ledger") or {}
    overall = ((ledger.get("summary") or {}).get("overall_long") or {})
    real = ((ledger.get("summary") or {}).get("real_bid_ask_options") or {})
    all_real = ((ledger.get("summary") or {}).get("all_options_alpha_real_bid_ask") or {})
    lines = [
        f"# US Option Shadow Ledger - {payload['as_of']}",
        "",
        f"- Real bid/ask leg rows: {ledger.get('real_bid_ask_resolved_count', 0)}",
        f"- All options_alpha real bid/ask rows: {ledger.get('all_real_bid_ask_resolved_count', 0)} resolved / {ledger.get('all_real_bid_ask_unresolved_count', 0)} unresolved",
        f"- Proxy rows: {ledger.get('proxy_resolved_count', 0)}",
        f"- Stock proxy rows: {ledger.get('stock_proxy_resolved_count', 0)}",
        f"- Unresolved rows: {ledger.get('unresolved_count', 0)}",
        f"- Rows with persisted legs: {ledger.get('rows_with_legs', 0)}",
        f"- Real bid/ask LCB80: {fmt_pct(real.get('lcb80_pct'))}",
        f"- All options_alpha real bid/ask LCB80: {fmt_pct(all_real.get('lcb80_pct'))}",
        f"- Overall long-expression LCB80: {fmt_pct(overall.get('lcb80_pct'))}",
        "",
        "| Date | Symbol | Expression | Pricing mode | Real bid/ask | Return | Reason |",
        "|---|---|---|---|---|---:|---|",
    ]
    for row in (ledger.get("rows") or [])[:40]:
        lines.append(
            f"| {row.get('report_date')} | {row.get('symbol')} | {row.get('expression')} | "
            f"{row.get('pricing_mode')} | {fmt_bool(bool(row.get('real_bid_ask_resolved')))} | "
            f"{fmt_pct(row.get('return_pct'))} | {row.get('reason')} |"
        )
    real_rows = ledger.get("real_bid_ask_rows") or []
    if real_rows:
        lines += [
            "",
            "## All options_alpha Real Bid/Ask Spreads",
            "",
            "| Date | Exit | Symbol | Expression | Resolved | Return | Reason |",
            "|---|---|---|---|---|---:|---|",
        ]
        for row in real_rows[:40]:
            lines.append(
                f"| {row.get('report_date')} | {row.get('evaluation_date') or '-'} | {row.get('symbol')} | "
                f"{row.get('expression')} | {fmt_bool(bool(row.get('real_bid_ask_resolved')))} | "
                f"{fmt_pct(row.get('return_pct'))} | {row.get('reason')} |"
            )
    return "\n".join(lines).rstrip() + "\n"


def render_ai_supercycle_evidence(payload: dict[str, Any]) -> str:
    lines = ["# AI Supercycle Evidence Ledger", ""]
    summary = (payload.get("ai_supercycle_evidence_ledger") or {}).get("summary") or {}
    lines += [
        f"- rows: {summary.get('rows', 0)}",
        f"- source-linked supply evidence: {summary.get('source_linked', 0)}",
        f"- missing/needs primary confirmation: {summary.get('needs_primary_confirmation', 0)}",
        "",
    ]
    lines += render_ai_supercycle_evidence_section(payload, limit=50)
    return "\n".join(lines).rstrip() + "\n"


def render_ai_supply_chain_relationships(payload: dict[str, Any]) -> str:
    return "\n".join(
        ["# AI Supply Chain Relationship Ledger", "", *render_ai_supply_chain_relationships_section(payload, limit=80)]
    ).rstrip() + "\n"


def render_ai_supercycle_layer_attribution(payload: dict[str, Any]) -> str:
    return "\n".join(
        ["# AI Supercycle Layer Attribution", "", *render_ai_supercycle_layer_attribution_section(payload, limit=80)]
    ).rstrip() + "\n"


def render_ai_lab_quality_index(payload: dict[str, Any]) -> str:
    return "\n".join(["# AI Lab Quality Index", "", *render_ai_lab_quality_index_section(payload, limit=50)]).rstrip() + "\n"


def render_ai_supercycle_value_radar(payload: dict[str, Any]) -> str:
    lines = ["# AI Supercycle 10x Value Radar", ""]
    summary = (payload.get("ai_supercycle_value_radar") or {}).get("summary") or {}
    lines += [
        f"- rows: {summary.get('rows', 0)}",
        f"- deep_dive_now: {summary.get('deep_dive_now', 0)}",
        f"- evidence_first: {summary.get('evidence_first', 0)}",
        f"- avoid_until_resolved: {summary.get('avoid_until_resolved', 0)}",
        f"- contract: {summary.get('contract') or '-'}",
        "",
    ]
    lines += render_ai_supercycle_value_radar_section(payload, limit=60)
    return "\n".join(lines).rstrip() + "\n"
