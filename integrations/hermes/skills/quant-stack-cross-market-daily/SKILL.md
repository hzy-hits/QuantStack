---
name: quant-stack-cross-market-daily
description: |
  Write quant-stack cross-market daily reports from frozen US/CN strategy facts.
  Use for A-share pre-market / US post-market, A-share post-market / US pre-market,
  or any request to make the daily report agent-driven instead of pipeline-script-driven.
version: 0.1.0
platforms: [linux]
metadata:
  hermes:
    tags: [quant-stack, market, daily-report, cn, us, cross-market]
    category: finance
---

# Quant-Stack Cross-Market Daily

This skill is the Hermes-side entrypoint for the cross-market daily report.
It keeps Hermes core narrow: quant-stack exposes CLI commands and frozen fact
packets; the Hermes lead editor chooses which facts matter and writes the
narrative.

The primary path is Hermes dynamic orchestration. Do not start from the legacy
`quant-research-v1/prompts/` or `quant-research-cn/prompts/` extractor /
narrator templates for cross-market reports. Those files may remain as
single-market compatibility fallback until the old cron paths are retired, but
they are not the design center for this workflow.

## Core Contract

- Fetch workers collect and stage data; they do not write narrative.
- Compute bricks freeze R, actions, risk regimes, gates, evidence state, and
  data freshness into quant-stack artifacts.
- The lead editor reads frozen packet facts through `tool_manifest`, chooses
  the useful facts heuristically, and writes the report.
- Validators reject invented facts, stale/missing inputs, production delivery
  markers, and causality drift.
- The hard causal direction is always **US -> CN**.

PM nuance: A-share post-market action is feedback on the prior US-to-CN
transmission. It must not raise, cut, or otherwise drive US pre-market
positioning.

## Commands

Run from the quant-stack repo on Oracle:

```bash
cd /home/ubuntu/quant-stack
quant-research-v1/.venv/bin/python scripts/agents/run_cross_market_daily_shadow.py \
  --slot am \
  --cn-date YYYY-MM-DD
```

```bash
cd /home/ubuntu/quant-stack
quant-research-v1/.venv/bin/python scripts/agents/run_cross_market_daily_shadow.py \
  --slot pm \
  --cn-date YYYY-MM-DD
```

Use `--agent-backend off` for deterministic shadow output. Use
`--agent-backend hermes` for the real Hermes lead-editor path. Use
`--agent-backend auto` only as a legacy Codex/DeepSeek fallback while the old
single-market narrator stack is being retired.

The command writes:

- `reports/review_dashboard/main_strategy_v2/<cn-date>/cross_market_<slot>_shadow.md`
- `cross_market_<slot>_shadow_packet.json`
- `cross_market_<slot>_shadow_trajectory.jsonl`
- `cross_market_<slot>_shadow.meta.json`

## Agent Workflow

1. Resolve the slot:
   - AM: US post-market facts -> CN pre-market plan.
   - PM: CN post-market feedback + US pre-market context, with no CN-to-US causality.
2. Run the shadow command if the packet/report does not already exist for the
   requested date.
3. Read `cross_market_<slot>_shadow_packet.json`.
4. Treat `tool_manifest` as the available skill/MCP-like tool surface. Select
   facts opportunistically; do not convert `coverage_checklist` into fixed
   section headings.
5. Use finance-search MCP tools when useful:
   - `quant_stack_daily_snapshot`
   - `quant_stack_spine_triage`
   - `quant_stack_task_status`
   - `quant_stack_validate_main_strategy_v2`
   - `get_market_snapshot` for US cash indices, US equity futures
     (S&P/Nasdaq/Dow/Russell), Europe/Asia country indices, oil, gold,
     USD/CNH, and China/STAR index temperature.
   - `newsnow_radar`, `search_news`, and `research_brief` for current macro,
     geopolitical, AI, semiconductor, and China-market catalysts.
   - `quant_stack_ranker` and `quant_stack_symbol_context` for CN
     semiconductor / AI hardware mapping, including 科创板 / STAR Market
     names when available.
6. Preserve packet numbers, tickers, dates, R values, and source paths exactly.
7. Write in the style of a market execution diary: strong topical headline,
   cause-effect chain first, compact tables only when they clarify execution,
   then invalidation and next checks.
8. Omit unavailable feeds/symbols/news from the public report. Do not print
   missing-data lists, internal tool diagnostics, prompt text, or reviewer notes.
9. The reviewer/editor pass must return one merged public Markdown report, not
   separate US and CN reports.
10. Public reports must include a compact global market temperature when the
    data is returned: oil, gold, at least one US equity future, and several
    non-US country/region indices.

## Style Reference

The writing style should be inspired by this Boist market diary post:

https://boist.org/2026/06/22/2026%e5%b9%b46%e6%9c%8821%e6%97%a5%ef%bc%9a%e9%9f%a9%e6%97%a5%e8%82%a1%e5%b8%82%e5%86%8d%e5%88%9b%e7%ba%aa%e5%bd%95%ef%bc%8c%e6%9d%a0%e6%9d%86etf%e5%a6%82%e6%97%a5%e4%b8%ad%e5%a4%a9%ef%bc%9btokenmaxxi/

Use it as style inspiration only. Do not copy its text, claims, or market
facts into quant-stack reports.

## Guardrails

- Do not invent prices, news, catalysts, R, tickers, or actions.
- Do not write `CN -> US`.
- Do not imply A-shares guide US execution.
- Do not treat main-board A-shares as the whole CN universe; consider 科创板 /
  STAR Market semiconductor and AI-hardware names through ranker or symbol
  context when available.
- Do not expose tool logs, MCP names, packet JSON, prompt/thinking process,
  missing-data lists, or delivery internals in the public report.
- Do not send `--market all prod` or kitchen-ticket artifacts as production
  email.
- Do not edit Hermes core for this workflow; extend quant-stack CLI/skill/MCP
  edges instead.
