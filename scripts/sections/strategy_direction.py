"""Strategy direction board + guardrails (Phase B.16).

Extracted from scripts/generate_main_strategy_v2_report.py. Renders the
strategy-family ranking board, adjustment-rules note, profit-guardrails
table, and full standalone strategy-direction report.

Takes `strategy_mode` kwarg (default "opportunity") so main can override.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_pct


def render_strategy_direction_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Rank | Role | Market | Strategy family | Tier | Max size | LCB80 | Freshness | Current | Why |",
        "|---:|---|---|---|---|---|---:|---|---|---|",
    ]
    for row in rows:
        current = (
            f"EA={row.get('current_execution_alpha', 0)}, "
            f"PEV={row.get('current_positive_ev_setup', 0)}, "
            f"watch={row.get('current_blocked', 0)}"
        )
        freshness_days = row.get("freshness_days")
        freshness = f"{row.get('freshness_state') or '-'}"
        if freshness_days:
            freshness += f" / {freshness_days}D"
        lines.append(
            f"| {row.get('rank')} | {row.get('role')} | {row.get('market')} | "
            f"{row.get('strategy_family')} | {row.get('tier')} | {row.get('max_size')} | "
            f"{fmt_pct(row.get('post_cost_lcb80_pct'))} | {freshness} | {current} | {row.get('reason')} |"
        )
    lines.append("")
    return lines


def render_adjustment_rules(strategy_mode: str = "opportunity") -> list[str]:
    return [
        "## Adjustment Rules",
        "",
        f"- Mode: `{strategy_mode}`. Execution rows must come from promoted sleeves or observed-lifecycle probability, plus the production ranker tier.",
        "- CN strong-market primary sleeve is tape leadership; broad oversold stays secondary/watch unless market regime fits.",
        "- CN narrative filter excludes daily-consumption names, boosts AI infra and hard-asset/energy/heavy-industry leaders, and deprioritizes internet/software.",
        "- US theme-cluster momentum is the main stock trade sleeve; legacy HIGH/MOD single-name rows are ranked watch only.",
        "- US options/flow are auxiliary stock-ranking evidence; missing option leg ledger must not block stock trades.",
        "- Broad-market A-share diagnostics are not AI-infra sleeves and must not block or promote AI-infra R.",
        "- Legacy families are comparison baselines, not fresh-entry production sleeves.",
        "",
    ]


def render_strategy_direction(payload: dict[str, Any], strategy_mode: str = "opportunity") -> str:
    rows = payload.get("strategy_direction") or []
    primary = next((row for row in rows if row.get("role") == "primary"), {})
    secondary = next((row for row in rows if row.get("role") in {"secondary_stock_trade", "secondary_probe"}), {})
    radar = next((row for row in rows if row.get("role") == "radar"), {})
    lines = [
        f"# Strategy Direction Board - {payload['as_of']}",
        "",
        "## Current Decision Snapshot",
        "",
        (
            f"Primary: {primary.get('market', '-')} {primary.get('strategy_family', '-')} "
            f"({primary.get('tier', '-')}); secondary: {secondary.get('market', '-')} "
            f"{secondary.get('strategy_family', '-')} ({secondary.get('tier', '-')}); "
            f"diagnostic: {radar.get('strategy_family', 'broad-market rows')} stays outside AI-infra sizing. "
            "This is the current ranked state, not a fixed strategy allocation."
        ),
        "",
        "## Daily Board",
        "",
    ]
    lines += render_strategy_direction_table(rows)
    lines += render_adjustment_rules(strategy_mode)
    lines += [
        "## Daily Questions",
        "",
        "1. Which family has the best post-cost LCB80 today?",
        "2. Is the edge fresh, decaying, or expired?",
        "3. What tier is allowed now: 0R, stock_trade, conditional, or normal?",
        "",
        "## Promotion Ladder",
        "",
        "- `0R`: negative/unknown EV or missing execution evidence.",
        "- `stock_trade`: positive after-cost evidence with current ranked setup; options are auxiliary evidence, not the traded instrument.",
        "- `conditional`: positive LCB80, fresh enough, current setup exists, and execution constraints define a capped entry plan.",
        "- `normal`: reserved for larger samples, stable freshness, and live/slippage evidence.",
        "",
        "## Kill Switches",
        "",
    ]
    for row in rows:
        if row.get("role") in {"primary", "secondary_stock_trade", "secondary_probe", "shadow_validation", "radar"}:
            lines.append(f"- {row.get('market')} {row.get('strategy_family')}: {row.get('kill_switch')}")
    return "\n".join(lines).rstrip() + "\n"


def render_profit_guardrails(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Market | Profit state | Max auto size | Why | Kill switch |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('market')} | {row.get('profit_state')} | {row.get('max_auto_size')} | "
            f"{row.get('why')} | {row.get('kill_switch')} |"
        )
    lines.append("")
    return lines
