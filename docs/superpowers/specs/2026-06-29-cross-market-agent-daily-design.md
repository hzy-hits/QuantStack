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
- an agent lead editor chooses emphasis, checks cross-market transmission, and writes the report;
- deterministic validators guard facts, tickers, R, market scope, and delivery state;
- delivery sends only validated report types, never the internal kitchen-ticket artifact.

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
