# Report Quality Audit

This audit translates the goal "reports should make sense and guide trading"
into concrete checks. It is not enough for a pipeline to finish; the report must
separate executable tickets from research, show evidence, state blockers, and
avoid stale or non-tradeable recommendations.

## Success Criteria

A daily report is useful only if it answers these questions:

1. What is the market regime and how reliable is that estimate?
2. Are there any executable fresh-entry tickets?
3. If no, why did the system block them?
4. Which names are only setup/watch/research priors?
5. What would invalidate an existing or watched position?
6. Which signals are stale, sample-poor, or historically weak?
7. Are options/shadow-options presented as evidence rather than account orders?
8. Is Factor Lab clearly labeled as research-only unless promoted?

## Evidence Checked

Commands run on 2026-05-08 Asia/Shanghai:

- `python3 -m unittest quant-research-v1/tests/test_strategy_backtest_gate.py`
  passed 22 tests.
- `cargo test -p quant-stack-core --lib` passed 5 tests covering policy
  threshold/scope failures, champion hysteresis, and report-model market
  filtering.
- DuckDB inspection found:
  - root `data/strategy_backtest_history.duckdb`: `daily_report_model` 22 rows,
    `execution_gate_results` 2493 rows.
  - US 2026-05-07 post report DB:
    `report_decisions` 7800 rows, `report_outcomes` 7800 rows,
    `algorithm_postmortem` 7800 rows, `options_alpha` 3581 rows.
  - CN report DB:
    `report_decisions` 7855 rows, `report_outcomes` 7647 rows,
    `algorithm_postmortem` 7646 rows, `analytics` 2,979,024 rows.

## Latest US Report: 2026-05-07 Post

File: `quant-research-v1/reports/2026-05-07_report_zh_post.md`

What makes sense:

- The headline is explicit: high-level state is "高位震荡，0新买".
- The report says `Equity Execution Alpha=None` and `fresh-entry=0`, so it does
  not force a trade when the gate is empty.
- It separates positions, fresh entries, setup/watch, missed alpha radar, risk
  avoidance, and Factor Lab.
- It flags missing `My Book Overlay`, so account-specific actions are not
  fabricated.
- It labels Factor Lab as "research prior / recall lead, not a trade
  instruction".
- It calls out weak evidence, for example HMM sample reliability and "stable EV
  failed".

Weaknesses:

- "漏alpha" is acknowledged, but the report needs a persistent scorecard that
  quantifies opportunity cost and the mechanism that would have caught the miss.

Root gate evidence:

- After running `target/release/quant-stack alpha evaluate --date 2026-05-07
  --markets us,cn --lookback-days 30 --auto-select --emit-bulletin`, the root
  shared history DB had 32 US policies, 0 eligible, 0 selected.
- The closest US policy was `us:core:long:high_mod:executable_now:h3`, with 86
  fills and 0.382756% average trade return, failing `avg_trade_pct<=0.4`.

Trading conclusion:

- The report is conservative and actionable: no new trade, manage existing
  positions by risk lines, and only watch second-day acceptance.

## Latest A-share Report: 2026-05-07 Evening

File: `quant-research-cn/reports/2026-05-07_report_zh_evening.md`

What makes sense:

- The report says no formal execution signal even though there are many
  candidates.
- It gives counts: 209 candidates, 79 paper trades, 127 observe-only.
- It states that the stable 30-day/EV gate did not release a formal candidate.
- It includes a口径审计 table with sample, fills, EV, EV80 lower bound, and
  conclusion.
- It separates formal execution, review-layer alpha, tactical continuation,
  risk avoidance, observation, Factor Lab, and scenarios.
- It correctly notes A-share T+1 and limit-up/down execution limitations.

Root gate evidence:

- For `2026-05-07`, `alpha_maturity_daily` had 23 CN policies, 0 eligible, 0
  selected.
- The closest policy was `cn:core:long:high_mod:executable_now:h2`, with 25
  fills and average trade return about 0.018%, failing `fills<50`,
  `active_buckets<15`, and `avg_trade_pct<=0.3`.
- `daily_alpha_bulletin` for that date contained CN `blocked_alpha`,
  `options_alpha`, and `recall_alpha`, but no `execution_alpha`.

Trading conclusion:

- The report is consistent with the gate: no formal buy list; only review
  candidates and next-open/auction confirmation.

## Report Contract

Every final report should include these sections or their market-specific
equivalent:

- One-line market conclusion.
- Signal scorecard / previous recommendation review.
- Market state with sample sizes or calibration caveats.
- Alpha status or fresh-entry tickets.
- Setup/watch/review layer.
- Missed alpha or recall layer.
- Risk avoidance and invalidation lines.
- Factor Lab section labeled as research-only unless promoted.
- Scenarios and next 3-7 day watch points.
- Disclaimer.

## Immediate Quality Gates

Before sending production mail:

- Report file exists and is non-empty.
- Current date/session payload exists.
- Shared `daily_report_model` exists for the market/session, or the report must
  explicitly say the shared gate has no formal execution alpha.
- If `execution_alpha` is empty, the report must not contain a fresh-entry buy
  list.
- If Factor Lab is appended, it must contain "research prior" or equivalent
  wording and must not change the headline.
- Options/shadow-options rows must be labeled as listed-option evidence for US
  or shadow-risk evidence for CN.
- Macro inputs with stale dates must show the source date.

## Open Gaps

- US and CN are both producing meaningful reports, but the US shared root alpha
  gate status should be embedded visibly in every final report, not only present
  in the root history DB.
- `quant-stack-core` now has direct tests for policy thresholds, champion
  hysteresis, and report model filtering; bulletin section classification should
  still get fixture tests.
- Missed-alpha accounting should graduate from prose into a persisted table with
  cause buckets and follow-up actions.
- A-share `open_execution_gate` needs auction/minute data before it can be
  treated as a true intraday execution model.
