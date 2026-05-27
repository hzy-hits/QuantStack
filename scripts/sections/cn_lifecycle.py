"""CN lifecycle section (Phase B.17).

Depends on render_cn_lifecycle_table from sections.tables (B.11).
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_pct
from sections.tables import render_cn_lifecycle_table


def render_cn_lifecycle_section(cn: dict[str, Any]) -> list[str]:
    lifecycle = cn.get("lifecycle") or {}
    policy = lifecycle.get("policy") or {}
    summary = lifecycle.get("summary") or {}
    v2 = summary.get("v2_ev_positive") or {}
    v2_dedup = summary.get("v2_ev_positive_dedup") or {}
    all_rows = summary.get("all_oversold_diagnostic") or {}
    all_dedup = summary.get("all_oversold_diagnostic_dedup") or {}
    lines = [
        "## A 股生命周期研究 / CN Lifecycle",
        "",
        "A 股主线现在优先 price-first tape leadership。`cn_tape_leadership_continuation` 是强市场主执行层；`cn_oversold_ev_positive` 和 `cn_observed_lifecycle_prob` 只在弱/震荡市场或具备相对强度时做 secondary。同一日期同一股票可能有多个 strategy_key 变体，去重口径按最高 EV LCB80 保留一条。",
        "",
        f"- Lifecycle state: `{policy.get('state') or '-'}`",
        f"- Best bucket: `{policy.get('best_bucket') or '-'}`; bucket LCB80 {fmt_pct(policy.get('best_bucket_lcb80_pct'))}",
        f"- Max hold: `T+{policy.get('max_hold_days') or '-'}`",
        f"- V2 EV-positive: n `{v2.get('n', 0)}`, avg {fmt_pct(v2.get('avg_pct'))}, LCB80 {fmt_pct(v2.get('lcb80_pct'))}",
        f"- V2 EV-positive dedup: n `{v2_dedup.get('n', 0)}`, avg {fmt_pct(v2_dedup.get('avg_pct'))}, LCB80 {fmt_pct(v2_dedup.get('lcb80_pct'))}",
        f"- All oversold diagnostic: n `{all_rows.get('n', 0)}`, avg {fmt_pct(all_rows.get('avg_pct'))}, LCB80 {fmt_pct(all_rows.get('lcb80_pct'))}",
        f"- All oversold diagnostic dedup: n `{all_dedup.get('n', 0)}`, avg {fmt_pct(all_dedup.get('avg_pct'))}, LCB80 {fmt_pct(all_dedup.get('lcb80_pct'))}",
        f"- Exit rule: {policy.get('first_review')}; {policy.get('follow_through_rule')}; {policy.get('time_stop')}",
        "- CN hold overlay: execution sleeve names get T+1 review, T+3 runner check, and T+5 max-hold review; non-sleeve rows stay rank-only.",
        "",
    ]
    lines += render_cn_lifecycle_table(lifecycle.get("by_hold_bucket") or [], "EV-positive Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("by_hold_bucket_dedup") or [], "EV-positive Hold Buckets Deduped By Date/Symbol")
    lines += render_cn_lifecycle_table(lifecycle.get("all_oversold_by_hold_bucket") or [], "All Oversold Diagnostic Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("all_oversold_by_hold_bucket_dedup") or [], "All Oversold Diagnostic Hold Buckets Deduped By Date/Symbol")
    lines += render_cn_lifecycle_table(lifecycle.get("by_execution_mode") or [], "EV-positive By Execution Mode")
    lines += render_cn_lifecycle_table(lifecycle.get("by_execution_mode_dedup") or [], "EV-positive By Execution Mode Deduped")
    return lines
