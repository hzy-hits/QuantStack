# Quant Stack

Quant Stack is the shared control plane for the US equities and China A-share
research systems. The rule is deliberately narrow:

**market producers compute facts, Quant Stack judges alpha maturity, reports
narrate the result.**

The active product direction is narrower than a general market research stack:
Quant Stack is an AI-infra specialist fund pipeline (reorg completed 2026-05).
Broad market data is still allowed for benchmark, hedge, liquidity and macro
context, but production stock candidates should come from the `ai_infra`
universe or source-reviewed promotion path.

The system is not a broker and does not place orders. `Execution Alpha` means a
candidate passed historical stability and execution constraints for the daily
research bulletin; position sizing and live trading remain outside this repo.

## Current Shape

```text
ai_infra source review / universe / relationship ledger
                              |
                              v
US producer               CN producer
quant-research-v1         quant-research-cn
     |                         |
     v                         v
report_decisions + outcomes + algorithm_postmortem
     |                         |
     +----------- quant-stack core -----------+
                 stable alpha gate
                 champion/challenger selection
                 options/shadow-options alpha
                 report model + bulletin
                              |
                              v
                  daily markdown + Gmail delivery
```

The producer layer remains market-specific because raw inputs are different:
US uses Finnhub/FRED/SEC/Polymarket/yfinance/CBOE; A-share uses
Tushare/AKShare, local calendars, flow data, announcements, unlocks, and
ETF-option-derived shadow diagnostics.

Everything after the producer boundary is shared:

```text
review ledger -> stable alpha gate -> alpha bulletin -> report model -> delivery
```

## Daily Commands

Run the shared alpha gate and write both market bulletins:

```bash
target/release/quant-stack alpha evaluate \
  --date 2026-04-24 \
  --lookback-days 30 \
  --auto-select \
  --emit-bulletin
```

Run the post-producer daily control plane:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --run-producers \
  --with-narrative \
  --lookback-days 30
```

For A-shares, `daily` now owns the production control flow that used to live in
`quant-research-cn/scripts/daily_pipeline.sh`: AKShare bridge check, `quant-cn`
producer, Factor Lab import, pre-alpha render, stable alpha bulletin, final
render, Factor Lab research-prior append, payload snapshots, chart generation,
Codex agent report generation, delivery, and post-email review maintenance.
The shell script remains as a compatibility wrapper/reference path, not the
preferred cron entry.

Run the full US report pipeline through the Rust state machine:

```bash
target/release/quant-stack us-daily \
  --stack-root . \
  --session post \
  --delivery-mode test \
  --test-recipient you@example.com \
  2026-04-24
```

`us-daily` owns the US control-flow states: preflight, lock, data producer,
Factor Lab refresh/import, payload split, Factor Lab injection, agents, report
validation, and delivery. `quant-research-v1/scripts/run_full.sh` is now only a
compatibility wrapper around this command.

Send test email to the configured test recipient only:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --run-producers \
  --with-narrative \
  --send-reports \
  --delivery-mode test
```

Production delivery is explicit:

```bash
target/release/quant-stack daily \
  --date YYYY-MM-DD \
  --markets us,cn \
  --session post \
  --run-producers \
  --with-narrative \
  --send-reports \
  --delivery-mode prod
```

Use `--delivery-dry-run` before any delivery change. Test mode resolves to one
test recipient by default; prod uses `reporting.recipients` from each market
config.

## Alpha Bulletin

The shared bulletin has the same sections for both markets:

- `Equity Execution Alpha`: selected champion policy plus execution gate pass.
- `Tactical / Theme Rotation Alpha`: stable non-core theme/rotation candidates;
  visible for research, not treated as core execution.
- `Options / Shadow Options Alpha`: US real options expressions and A-share
  shadow-option risk/convexity diagnostics.
- `Recall Alpha`: research priors and recall leads that did not pass execution.
- `Blocked / Out-of-scope Alpha`: explicit blocker reasons such as EV unknown,
  no fill, stale chase, low R:R, strategy scope, or execution score failure.

Headline/news state is context only. It is included in the report so a human can
read the market tape, but it is not allowed to veto a candidate by itself.

## Stability Gate

The gate evaluates rolling historical outcomes by policy. Selection only uses
completed evaluation windows:

```text
report_date <= as_of - horizon
evaluation_date <= as_of
```

This avoids incomplete outcomes and future leakage. Policies must pass market
thresholds before they can become the daily champion.

Initial thresholds:

| Market | Fills | Active Buckets | Avg Trade | Median | Strict Win | Max DD | Top Winner Share |
|---|---:|---:|---:|---:|---:|---:|---:|
| US | 20 | 10 | > 0.40% | >= 0 | > 45% | > -25% | <= 45% |
| CN | 50 | 15 | > 0.30% | >= 0 | > 43% | > -8% | <= 25% |

Champion/challenger hysteresis prevents daily churn: if the incumbent is still
eligible, a challenger must beat its stability score by 15% before replacement.

## A-Share Review Repair

The A-share review ledger needs execution analytics to exist before historical
postmortems are rebuilt. `quant-cn review-backfill` now ensures these modules
are present for each review date:

- `setup_alpha`
- `continuation_vs_fade`
- `open_execution_gate`

This prevents historical `algorithm_postmortem` from collapsing into all
`OBSERVE/WAIT` rows because of missing `execution_score`.

## Project Layout

```text
quant-stack/
├── ops/                      # task registry (tasks.yaml), unified cron runner, logs, state
├── crates/
│   ├── quant-stack-core      # shared alpha gate, bulletin, report model
│   ├── quant-stack-cli       # root daily control plane (target/release/quant-stack)
│   ├── quant-fetcher         # US data fetcher (Finnhub/FRED/SEC/Polymarket)
│   └── quant-stack-py        # thin PyO3 bindings for tests/notebooks/legacy Python
├── scripts/                  # ~36 research.* component tasks, radars, report pipeline,
│   ├── lib/ sleeves/ agents/ #   shared helpers, alpha sleeves, LLM narrator agents
│   └── generate_main_strategy_v2_report.py  # main report generator (refactor in progress)
├── ai_infra/                 # AI-infra research OS: universe, evidence, BFS discovery
├── quant-research-v1/        # US producer, report, agents, delivery
├── quant-research-cn/        # A-share producer, report, agents, delivery
├── factor-lab/               # research factor discovery + paper trading
├── data/                     # shared DuckDB (strategy backtest history)
├── reports/                  # review_dashboard + intraday (gitignored)
└── docs/                     # operating docs (see index below) + docs/archive/
```

## Operating Docs

权威入口与阅读顺序(2026-06-10 整理;归档规则见 [docs/archive/](docs/archive/README.md)):

**现状权威(先读这两份):**

- [Architecture Reference](docs/ARCHITECTURE.md): single source of truth for
  what runs where — 数据层 / Rust 编排 / `research.*` 组件 / narrator 叙事层 /
  已知坑与操作速查。
- [Agent Operating Manual](AGENTS.md): agent 首入口;AI-infra 专业户目标、
  source-review 晋级路径、universe 边界、执行规则。

**边界与合同(改代码前查):**

- [Module Boundaries](docs/MODULE_BOUNDARIES.md): what each market, shared
  crate, Factor Lab, options, and reporting module owns.
- [AI Supercycle Pipeline Contract](docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md):
  production-candidate, source-evidence, and report constraints.
- [Report Delivery Contract](docs/REPORT_DELIVERY_CONTRACT.md): research
  artifacts vs user-facing daily reports.
- [Decisions Log](docs/DECISIONS.md): 正式决策记录(改交易行为的开关翻转
  必须在此留痕;复议条件齐备才重开)。

**投资框架与研究备忘:**

- [AI Infra Investment Mandate](docs/AI_INFRA_INVESTMENT_MANDATE.md):
  投资哲学与边界。
- [AI Infra Quant Fund Integration](docs/AI_INFRA_QUANT_FUND_INTEGRATION.md):
  `ai_infra` 研究 OS 与量化系统的集成关系。
- [AI Infra Research Mainlines](docs/AI_INFRA_RESEARCH_MAINLINES.md):
  产业链主线研究框架(与 `ai_infra/` 冲突时以后者为准)。
- [HBM / CoWoS Deep Dive](docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md):
  source-review gated 研究备忘。
- [Factor Family OOS Review 2026-06-07](docs/FACTOR_FAMILY_OOS_REVIEW_2026-06-07.md):
  因子家族 OOS 失败审查与隔离决定。

**进行中的计划:**

- [Refactor Plan](REFACTOR_PLAN.md): 拆分
  `scripts/generate_main_strategy_v2_report.py`(11,105 → 7,581 行,进行中)。
- [Production Hardening Plan](docs/plans/2026-06-10-production-hardening.md):
  执行 gate 硬断言 / narrator 校验强化 / ops depends_on / 晋级 alpha 告警 /
  setup 闸门决策 / regime 连续化(2026-06-10 review 的 6 项修复)。

**归档:** 已完成或被取代的 plan/audit/handoff 在
[docs/archive/](docs/archive/README.md)(CLAUDE_HANDOFF、
PROJECT_CONSOLIDATION_PLAN、PHASE_D_PLAN、AI_INFRA_SPECIALIST_PIPELINE_REORG、
ALPHA_SLEEVE_ENGINEERING_PLAN 及三份 2026-05-08 审计),**不反映现状**。

## Verification

Useful checks after changes:

```bash
python -m unittest quant-research-v1/tests/test_strategy_backtest_gate.py
cargo build -p quant-stack-cli --release
cargo test -p quant-stack-core --lib
(cd quant-research-cn && cargo test filtering::notable)
```

Full alpha smoke for the latest validated date:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --send-reports \
  --delivery-mode test \
  --delivery-dry-run
```

## Production Notes

- Default delivery mode is `test`; prod must be explicit.
- `QUANT_DELIVERY_MODE=prod` enables full recipient lists in wrapper scripts.
- `QUANT_TEST_RECIPIENT=email@example.com` overrides the test recipient.
- Daily pipeline order matters: producers -> CN Factor Lab import/render ->
  alpha bulletin -> report model -> CN final render/agents -> delivery ->
  review maintenance.

## License

Private research tool. Not for redistribution.
