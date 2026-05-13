# Claude Handoff

版本日期：2026-05-13

这份文档用于把当前 Quant Stack 项目交给 Claude 或其他 agent 接手。先读本文件，再按下面的顺序读详细文档。

## 仓库状态

本地路径：

```bash
/home/ivena/coding/quant-stack
```

GitHub remote：

```bash
git@github.com:hzy-hits/QuantStack.git
```

当前分支：

```bash
main
```

当前已推送到 `origin/main`。最新关键提交：

```text
53507f2 Enforce AI infra specialist pipeline
1c3d7a1 Wire factor lab into AI infra discovery
b6d29f9 Add AI infra research workbench
```

如果在新机器接手：

```bash
git clone git@github.com:hzy-hits/QuantStack.git
cd QuantStack
git pull origin main
```

如果在当前机器接手：

```bash
cd /home/ivena/coding/quant-stack
git status -sb
```

## 一句话目标

Quant Stack 正在从泛市场股票扫描器，收敛成 AI Infra 专门基金研究和量化管线。

核心链路：

```text
ai_infra 原文研究 / BFS 发现
  -> source-review 晋级
  -> AI universe / relationship ledger
  -> 美股和 A 股量化特征、K线、期权/影子期权
  -> sleeve 排序和组合权重
  -> 对 SPY / QQQ / SMH / 上证 / 深成指等 benchmark 做归因
  -> 每日报告
```

## 必读顺序

1. `AGENTS.md`
2. `README.md`
3. `docs/AI_INFRA_SPECIALIST_PIPELINE_REORG.md`
4. `docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md`
5. `docs/AI_INFRA_QUANT_FUND_INTEGRATION.md`
6. `docs/MODULE_BOUNDARIES.md`
7. `docs/PROJECT_CONSOLIDATION_PLAN.md`
8. `ai_infra/START_HERE.md`
9. `ai_infra/docs/README.md`
10. `ai_infra/docs/fund-management-philosophy.md`
11. `ai_infra/docs/llm-dependency-bfs-framework.md`
12. `ai_infra/docs/company-financials-market-options-methodology.md`

## 当前架构

主要模块：

| 路径 | 职责 |
| --- | --- |
| `ai_infra/` | AI Infra BFS、原文证据、source-review、universe、研究队列。 |
| `factor-lab/` | 发现新公司、读财报/消息/产业链材料、生成 research hypothesis 和 `DATA_REQUIREMENTS`。 |
| `quant-research-v1/` | 美股 producer，只在 AI universe 或已晋级 promotion output 内排序。 |
| `quant-research-cn/` | A 股 producer，把海外 AI Infra 瓶颈映射到 A 股/港股。 |
| `scripts/sleeves/` | US/CN/Factor Lab/promotions/portfolio hedge sleeve 逻辑。 |
| `scripts/run_main_strategy_v2_backtest.py` | Main Strategy V2 report 和 AI specialist report rendering。 |
| `scripts/send_production_decision_report.py` | 报告发送和 market/report heading 防串线。 |
| `scripts/audit_production_basket_ai_universe.py` | 对最新 main_strategy_v2 报告做 AI universe 合规审计 (`production_basket` 必须 100% `ai_infra_universe=True`)。 |
| `ops/` | root task registry、cron 渲染、task runner、review packet。 |
| `crates/` | Rust shared control plane 和 CLI。 |

## 硬边界

1. 量化扫描只允许在 AI universe 或 source-reviewed promotion output 内做 production candidate 排序。
2. 新公司不能直接进 ranker、日报 production candidates 或真实仓位建议。
3. 新公司必须先进入 `ai_infra` expansion/source-review queue，再补 evidence card、原文来源和 counterevidence。
4. 没有原文证据，不能写成 source-confirmed supplier/customer relationship。
5. K线只能说明 trend、pricing、crowding、risk，不能证明产业链关系。
6. 期权只能说明 volatility、event pricing、liquidity、crowding clue，不能提升 evidence status。
7. `SPY`、`QQQ`、`SMH`、`000001.SH`、`399001.SZ`、`399006.SZ`、`000300.SH` 等只能做 benchmark、hedge、macro context，不能成为 production stock candidate。
8. 美股报告公司名保持英文原名；A 股/港股可以用中文名。
9. 每日报告必须分清 production candidates、watch/research-only、benchmark、hedge、macro context。
10. 不要提交 ignored 的本地私有数据、source cache、reports、logs、DuckDB、credentials、token、target、venv。

## 新公司晋级流程

任何新 ticker，例如用户提到的 DGXX/dgxx，都走同一路径：

```text
new ticker / company lead
  -> ai_infra expansion candidate
  -> source-review queue
  -> evidence card + original sources + counterevidence
  -> relationship ledger / global universe promotion
  -> market data and factor computation
  -> sleeve ranking
  -> production candidate or watch/research-only
```

如果公司不在 `ai_infra/data/global_universe_v2.jsonl`、US alpha queue、source verification queue 或 promotion output 中，不能直接加入交易候选。

## 日报合同

日报应该包含：

1. AI book state。
2. Production candidates。
3. Watch / research-only candidates。
4. Earnings calendar。
5. Source-review calendar (`AI Infra Source Review Calendar`，数据源 `ai_infra/reports/source_verification_queue_v1.csv`)。
6. Benchmark snapshot (`US Benchmark Snapshot`：SPY/QQQ/SMH/IWM/DIA；`A股 Benchmark Snapshot`：000300.SH/399006.SZ/399001.SZ/000001.SH)，包含 1D/5D/20D/60D/YTD 走势。
7. Portfolio risk、hedge state、concentration。
8. Open `DATA_REQUIREMENTS`。

生成的 artifact 文件 (在 `reports/review_dashboard/main_strategy_v2/<date>/`)：

- `earnings_calendar.md` / `.json`
- `source_review_calendar.md` / `.json`
- `benchmark_attribution.md` / `.json`
- `us_daily_report.md`、`cn_daily_report.md`
- `us_opportunity_ranker.json` / `cn_opportunity_ranker.json`（含 `ai_infra_gate.contract = ai_infra_universe_only` 和 `production_basket` 数组）

## 常用命令

生成 BFS supply-chain discovery queue：

```bash
python3 ai_infra/scripts/generate_bfs_supply_chain_discovery_queue.py
```

生成 expansion candidates：

```bash
python3 ai_infra/scripts/generate_expansion_candidates.py
```

检查 AI supercycle readiness：

```bash
python3 scripts/verify_ai_supercycle_readiness.py --as-of 2026-05-13 --strict
```

运行 Main Strategy V2 report：

```bash
python3 scripts/run_main_strategy_v2_backtest.py --as-of 2026-05-13
```

核心 Python smoke tests：

```bash
python3 -m unittest \
  quant-research-v1.tests.test_main_strategy_v2_backtest \
  tests.test_ai_infra_bfs_discovery_queue \
  tests.test_ai_infra_expansion_lane \
  tests.test_send_production_decision_report \
  tests.test_ai_supercycle_readiness \
  tests.test_phase_0_6_guardrails \
  tests.test_cn_tape_supercycle_layers \
  tests.test_source_review_calendar \
  tests.test_benchmark_attribution \
  tests.test_audit_production_basket_ai_universe \
  quant-research-v1.tests.test_ai_infra_universe \
  quant-research-v1.tests.test_us_opportunity_ranker \
  quant-research-v1.tests.test_cn_opportunity_ranker
```

跑完报告之后检查 AI universe 合规：

```bash
python3 scripts/audit_production_basket_ai_universe.py --as-of 2026-05-13
```

Factor Lab tests 需要从 `factor-lab/` 目录跑：

```bash
cd factor-lab
python3 -m unittest \
  tests.test_ai_infra_context \
  tests.test_ai_supply_chain_discovery \
  tests.test_autoresearch_session
```

Rust checks：

```bash
cargo check -p quant-stack-cli
(cd quant-research-cn && cargo check)
```

## 最近验证结果

最近一次整理 (2026-05-13 接续 handoff) 已经跑过：

```text
Python smoke tests (50 tests): pass — includes new
  tests.test_source_review_calendar (4)
  tests.test_benchmark_attribution (2)
  tests.test_audit_production_basket_ai_universe (4)
Factor Lab tests (from factor-lab/): pass (12)
cargo check -p quant-stack-cli: pass
verify_ai_supercycle_readiness.py --as-of 2026-05-13 --strict:
  ready_with_warnings pass/warn/fail=10/1/0
audit_production_basket_ai_universe.py --as-of 2026-05-13: pass
```

## 已完成的下一步 (2026-05-13)

1. `scripts/audit_production_basket_ai_universe.py` 加 `tests.test_audit_production_basket_ai_universe`，断言每个 production_basket 行 `ai_infra_universe=True`、`ai_infra_current_pool` 非空、`ai_infra_gate.contract=ai_infra_universe_only`。在 `quant-research-v1/tests/test_us_opportunity_ranker.py` 和 `test_cn_opportunity_ranker.py` 中加同样的 production_basket 断言。
2. `scripts/run_main_strategy_v2_backtest.py`：
   - 新增 `build_source_review_calendar` / `render_source_review_calendar_section`，读取 `ai_infra/reports/source_verification_queue_v1.csv`，写入 `source_review_calendar.md`/`.json`，并在 US/CN/combined 三个报告里渲染。
   - 新增 `build_benchmark_attribution` / `render_benchmark_attribution_section`，对 US (SPY/QQQ/SMH/IWM/DIA) 和 CN (000300.SH/399006.SZ/399001.SZ/000001.SH) 输出 1D/5D/20D/60D/YTD 表，写入 `benchmark_attribution.md`/`.json`。
3. `ops/tasks.yaml` 新增 `research.main_strategy_v2_report` (12:10 CST) 和 `research.production_basket_audit` (12:15 CST)。
4. `ops/review_packet.sh` 调用审计脚本，输出 `production_basket_audit.md` 到 review packet。

## 建议的下一步

1. 继续自动化 source-review 晋级：从 filings/transcripts/source-linked news 自动生成 evidence card 和 `expansion_candidates_promoted_v1.csv` 行。
2. 把 Factor Lab 的 `DATA_REQUIREMENTS` 和 `ai_infra/reports/source_verification_queue_v1.csv` 做更明确的 artifact contract（schema、字段必填、tier 字典）。
3. CN 缺数据的指数 (`399001.SZ`、`000001.SH`) 应在 cn producer 中补 ingestion，让 benchmark snapshot 不再有 missing 行。
4. Benchmark attribution 当前只是 snapshot；下一步计算 production_basket 的 daily return 和 SPY/QQQ/SMH 的相对 alpha（lookback 20/60 天）。
5. Add SMH 等 AI 专属 benchmark 到 hedge selector 的 `US_HEDGE_BENCHMARKS`，让 hedge 路径也能拣 AI semi beta。
6. 给 `audit_production_basket_ai_universe.py` 加 `--strict` 选项，把 watch 列表里的 non-AI 名字也一并审计。

## 接手时不要做的事

- 不要把 `ai_infra/reports/`、`ai_infra/evidence/`、`ai_infra/notes/`、`ai_infra/data/source_cache/` 强行加入 git。
- 不要提交 `config.yaml`、`credentials.json`、`token.json`、DuckDB、logs、target、venv。
- 不要把 broad market screener 结果直接接到 production ranker。
- 不要用 K线或期权异动证明 supplier/customer relationship。
- 不要在没有 source review 的情况下把 DGXX 这类新公司塞进交易候选。

## 给 Claude 的启动提示

可以把下面这段直接交给 Claude：

```text
请接手 Quant Stack 仓库。

本地路径：/home/ivena/coding/quant-stack
GitHub：git@github.com:hzy-hits/QuantStack.git
分支：main
最新关键提交：53507f2 Enforce AI infra specialist pipeline

先读 CLAUDE_HANDOFF.md，然后按它的“必读顺序”阅读 AGENTS.md、README.md、docs/AI_INFRA_SPECIALIST_PIPELINE_REORG.md、docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md、docs/AI_INFRA_QUANT_FUND_INTEGRATION.md、docs/MODULE_BOUNDARIES.md 和 ai_infra/START_HERE.md。

目标是把 Quant Stack 继续收敛成 AI Infra 专门基金管线：
1. 量化扫描只在 ai_infra universe 或 source-reviewed promotion output 内排序。
2. Factor Lab / autoresearch 负责发现新公司、读财报/消息/原文、生成 DATA_REQUIREMENTS 和 source-review queue。
3. 新公司不能直接进入 production ranker，必须先 evidence card / source review / relationship ledger 晋级。
4. 日报要包含 production candidates、watch/research-only、财报日历、source-review calendar 和 benchmark attribution。
5. 美股公司名保持英文原名，A 股/港股可以显示中文名。

改动前先运行 git status -sb。不要提交 ignored 的本地私有数据、reports、logs、DuckDB、source_cache、credentials、token、target、venv。改动后运行相关 tests / smoke checks，并写清验证结果。
```
