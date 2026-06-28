# Cross-Market Agent Daily Refactor

Date: 2026-06-29
Status: shadow implementation started

## Product Shape

Daily reports should move from single-market emails to two cross-market reports:

1. **AM report, 07:30 Asia/Shanghai**
   - Combines US post-market state with CN pre-market plan.
   - Causality is one-way: US market structure guides A-share risk, sector priority, and execution limits.

2. **PM report, after A-share close and before US open**
   - Combines A-share post-market feedback with US pre-market context.
   - A-share action is feedback on prior US-to-CN transmission, not a driver of US positioning.

Hard rule: **US can guide CN; CN must not drive US execution.**

## Architecture Target

The target is not to concatenate two existing reports. The target is:

- fetch workers write staging data and freshness state;
- compute bricks read canonical DBs and frozen JSON artifacts;
- quant-stack exposes frozen facts as a skill/MCP-like tool surface;
- an agent lead editor heuristically chooses which tools/facts matter, checks cross-market transmission, and writes the report;
- deterministic validators guard facts, tickers, R, market scope, and delivery state;
- delivery sends only validated report types, never the internal kitchen-ticket artifact.

Hermes integration should follow the narrow-waist model already used on the Oracle host:

- do not grow Hermes core for quant logic;
- expose quant-stack as CLI commands plus a future skill/MCP surface;
- keep fetch workers, compute bricks, and validators inside quant-stack;
- let the Hermes lead editor agent use the available tools opportunistically, like a skill, rather than executing a hard-coded report section list.

Versioned skill entrypoint:

- `integrations/hermes/skills/quant-stack-cross-market-daily/SKILL.md`

Fixed items are fact sources, validation, delivery state, and **US -> CN** causality. Non-fixed items are section order, narrative angle, tool selection, and emphasis.

## Narrator Cutover

The cross-market report must not be another fixed extractor -> merge prompt
pipeline. The old files under `quant-research-v1/prompts/` and
`quant-research-cn/prompts/` remain only for legacy single-market compatibility.
They are not the primary cross-market report path.

Primary cross-market writer:

```bash
python3 scripts/agents/run_cross_market_daily_shadow.py \
  --slot am \
  --cn-date YYYY-MM-DD \
  --agent-backend hermes
```

or:

```bash
python3 scripts/agents/run_cross_market_daily_shadow.py \
  --slot pm \
  --cn-date YYYY-MM-DD \
  --agent-backend hermes
```

Hermes runs with `quant-stack-cross-market-daily` preloaded and can use the
existing `finance-search` MCP server. Useful finance-search tools include:

- `quant_stack_daily_snapshot`
- `quant_stack_spine_triage`
- `quant_stack_task_status`
- `quant_stack_validate_main_strategy_v2`

The agent may choose which tools to call. The code provides a packet,
guardrails, and validation; it does not prescribe a section-by-section
narrator template.

Fallback policy: if Hermes is unavailable, the runner may use
`--fallback-backend auto` to call the legacy packet-only LLM writer. That path
is a resilience fallback, not the primary report architecture, and it still
cannot use the old fixed extractor/merge prompt templates as the cross-market
source of truth.

## Style Target

Reference style: Boist's market diary post from 2026-06-22:

<https://boist.org/2026/06/22/2026%e5%b9%b46%e6%9c%8821%e6%97%a5%ef%bc%9a%e9%9f%a9%e6%97%a5%e8%82%a1%e5%b8%82%e5%86%8d%e5%88%9b%e7%ba%aa%e5%bd%95%ef%bc%8c%e6%9d%a0%e6%9d%86etf%e5%a6%82%e6%97%a5%e4%b8%ad%e5%a4%a9%ef%bc%9btokenmaxxi/>

Use it as writing inspiration only, not as copied text. The desired report style is a market execution diary:

- open with a strong topical headline and the day's dominant market story;
- explain cause and effect before tables;
- connect macro, sector leadership, positioning/leverage, event risk, and execution implications;
- keep tables compact and use them only for trade facts or scenario thresholds;
- end with what changed, why it matters, what would invalidate it, and what to check next.

## Current Shadow Step

Implemented shadow runner:

```bash
python3 scripts/agents/run_cross_market_daily_shadow.py --slot am --cn-date YYYY-MM-DD --us-date YYYY-MM-DD
python3 scripts/agents/run_cross_market_daily_shadow.py --slot pm --cn-date YYYY-MM-DD --us-date YYYY-MM-DD
```

It reads existing `reports/review_dashboard/main_strategy_v2/<date>/` artifacts and writes:

- `cross_market_am_shadow.md`
- `cross_market_pm_shadow.md`
- matching `*_packet.json`
- matching `*_trajectory.jsonl`
- matching `*.meta.json`

Default mode is deterministic shadow only: no LLM call, no email, no production delivery.

The packet now includes:

- `agent_operating_mode`: Hermes-style heuristic lead editor contract;
- `data_boundary`: fetch worker / compute brick / agent editor / validator responsibilities;
- `tool_manifest`: skill/MCP-like read and output tools exposed to the agent;
- `coverage_checklist`: acceptance criteria, explicitly not a fixed section template;
- `style_brief`: Boist-inspired execution-diary style constraints.

## Required Refactor Before Production

The existing CN morning pipeline produces CN Main Strategy V2 artifacts during `cn.morning` at 08:30, so a 07:30 AM cross-market report cannot depend on that delivery pipeline. Production promotion requires a new split:

1. **Data workers**
   - US fetch workers and CN fetch workers run independently.
   - Fetch state and freshness gates are recorded before report generation.

2. **Compute bricks**
   - Freeze US post-market facts after US close.
   - Freeze CN pre-market facts before 07:30 without sending a CN-only report.
   - Freeze CN post-market facts after A-share close.
   - Freeze US pre-market facts before the US pre-market report.

3. **Agent spine**
   - AM spine: `US post facts -> CN pre plan`.
   - PM spine: `CN post feedback + US pre context`, with no CN-to-US causality.
   - Agent output writes shadow first, then validator-gated production.

4. **Delivery**
   - New deliverable report type: cross-market AM / PM.
   - Existing `--market all prod` remains forbidden for kitchen-ticket reports.

## Promotion Gate

Before scheduling production email:

- shadow reports generated for at least several trading days;
- validator rejects CN-to-US causality language;
- AM packet exists by 07:30 without depending on `cn.morning` email delivery;
- no report sends if freshness, compute, or agent validation fails;
- production recipients receive only the cross-market deliverable, not old single-market report artifacts.
