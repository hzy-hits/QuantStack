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
| `scripts/audit_production_basket_ai_universe.py` | 对最新 main_strategy_v2 报告做 AI universe 合规审计 (`production_basket` 必须 100% `ai_infra_universe=True`)；输出 basket / pool / depth 覆盖矩阵。 |
| `scripts/score_source_review_readiness.py` | 读 `source_verification_queue_v1.csv`，按 G0-G4 gate 打分 → `reports/review_dashboard/ai_infra_source_review_readiness/<date>/source_review_readiness.{csv,md}`，tier ∈ {ready_for_promotion, evidence_partial, pending_human_review, blocked_by_counterevidence, g0_blocked, unscored}。 |
| `scripts/ingest_cn_index_prices.py` | 用 AKShare 把 `000001.SH`/`399001.SZ`/`399006.SZ`/`000300.SH` 补到 `quant-research-cn/data/quant_cn_report.duckdb` `prices` 表。 |
| `scripts/ingest_satellite_index_prices.py` | 用 yfinance 把 `^TWII`/`^N225`/`^KS11`/`^AEX` 和 `EWT/EWJ/EWY/EWN` ETF 补到 `quant-research-v1/data/quant.duckdb` `prices_daily` 表。 |
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
- `source_review_calendar.md` / `.json`：US/CN AI Infra source review queue (含 readiness tier 列)。
- `satellite_pool_report.md` / `.json`：卫星资产池 (TW/JP/KR/EU/IL) 50+ 名字按 region + BFS depth 拆分，含 readiness tier。
- `benchmark_attribution.md` / `.json`：US (SPY/QQQ/SMH/IWM/DIA)、CN (000300.SH/399006.SZ/399001.SZ/000001.SH)、Satellite (^TWII/^N225/^KS11/^AEX + EWT/EWJ/EWY/EWN) 三张表。
- `us_daily_report.md`、`cn_daily_report.md`、`main_strategy_v2_backtest.md` (combined，含 satellite + 卫星 benchmark)。
- `us_opportunity_ranker.json` / `cn_opportunity_ranker.json`（含 `ai_infra_gate.contract = ai_infra_universe_only` 和 `production_basket` 数组）

另外的 review-dashboard 输出：

- `reports/review_dashboard/ai_infra_source_review_readiness/<date>/source_review_readiness.{csv,md}`：G0-G4 readiness ledger，由 `scripts/score_source_review_readiness.py` 维护。

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
  tests.test_satellite_pool_report \
  tests.test_score_source_review_readiness \
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

最近一次整理 (2026-05-13 第二批) 已经跑过：

```text
Python smoke tests (60 tests): pass — includes
  tests.test_source_review_calendar (4)
  tests.test_benchmark_attribution (3)
  tests.test_audit_production_basket_ai_universe (4)
  tests.test_satellite_pool_report (3)
  tests.test_score_source_review_readiness (6)
Factor Lab tests (from factor-lab/): pass (12)
cargo check -p quant-stack-cli: pass
verify_ai_supercycle_readiness.py --as-of 2026-05-13 --strict:
  ready_with_warnings pass/warn/fail=10/1/0
audit_production_basket_ai_universe.py --as-of 2026-05-13: pass
  US basket=10, pools=核心池/候选/雷达池 mix
  CN basket=0 (no current AI A股 in production)
ingest_cn_index_prices.py: 4 indices × 658 rows ingested
ingest_satellite_index_prices.py: 8 satellite benchmarks × ~250 rows ingested
score_source_review_readiness.py: 146 rows scored,
  ready_for_promotion=5, evidence_partial=3, pending_human_review=126,
  blocked_by_counterevidence=1, unscored=11
```

## 已完成的下一步 (2026-05-13)

1. `scripts/audit_production_basket_ai_universe.py` 加 `tests.test_audit_production_basket_ai_universe`，断言每个 production_basket 行 `ai_infra_universe=True`、`ai_infra_current_pool` 非空、`ai_infra_gate.contract=ai_infra_universe_only`。在 `quant-research-v1/tests/test_us_opportunity_ranker.py` 和 `test_cn_opportunity_ranker.py` 中加同样的 production_basket 断言。
2. `scripts/run_main_strategy_v2_backtest.py`：
   - 新增 `build_source_review_calendar` / `render_source_review_calendar_section`，读取 `ai_infra/reports/source_verification_queue_v1.csv`，写入 `source_review_calendar.md`/`.json`，并在 US/CN/combined 三个报告里渲染。
   - 新增 `build_benchmark_attribution` / `render_benchmark_attribution_section`，对 US (SPY/QQQ/SMH/IWM/DIA) 和 CN (000300.SH/399006.SZ/399001.SZ/000001.SH) 输出 1D/5D/20D/60D/YTD 表，写入 `benchmark_attribution.md`/`.json`。
3. `ops/tasks.yaml` 新增 `research.main_strategy_v2_report` (12:10 CST) 和 `research.production_basket_audit` (12:15 CST)。
4. `ops/review_packet.sh` 调用审计脚本，输出 `production_basket_audit.md` 到 review packet。

## 第二批已完成 (2026-05-13 续)

1. `scripts/score_source_review_readiness.py` 落地 G0-G4 gate，按方法论给 source-verification queue 打 readiness tier；同一逻辑内联进 `build_source_review_calendar`，daily report 的 Source Review 表多了一列 Readiness。
2. `scripts/run_main_strategy_v2_backtest.py` 新增 `build_satellite_pool_report` / `render_satellite_pool_report_section`：卫星资产池 50 个名字按 Taiwan/Japan/Korea/Europe/Israel 拆分、按 BFS depth 汇总、按 readiness tier 分类。落 `satellite_pool_report.{md,json}` 并进入 combined report。
3. `scripts/ingest_cn_index_prices.py` 用 AKShare 把 `000001.SH`/`399001.SZ`/`399006.SZ`/`000300.SH` 共 658 行 / 指数 补进 CN db；benchmark snapshot 不再有 missing rows。
4. `scripts/ingest_satellite_index_prices.py` 用 yfinance 把 `^TWII`/`^N225`/`^KS11`/`^AEX` 与 `EWT/EWJ/EWY/EWN` ETF 镜像补进 US db；benchmark 章节新增 “Satellite Benchmark Snapshot” 表。
5. `audit_production_basket_ai_universe.py` 输出 `by_current_pool` / `by_bfs_depth` 覆盖矩阵，供 ops cron 和 review packet 追踪 satellite vs core 的混合比例。
6. `ops/tasks.yaml` 新增 `research.cn_index_ingest` (06:00 CST)、`research.satellite_index_ingest` (06:05 CST)、`research.source_review_readiness` (12:08 CST)。

## 建议的下一步

1. Evidence card 自动化：把 `expansion_candidates_v1.csv` 和 readiness ledger 联动；`ready_for_promotion` tier 自动生成 evidence card 模板草稿落到 `ai_infra/evidence/`。
2. Factor Lab `DATA_REQUIREMENTS` ↔ source_verification_queue 的明确 schema 合同（必填字段、tier 字典、向量化校验）。
3. Benchmark attribution 升级：从单纯 snapshot 升级到 AI book 的 daily return 和相对 benchmark 的 alpha/beta/IR（lookback 20/60 天，per market）。
4. 卫星名字 (TSM/ASML/ASX/IBM/Samsung 等可用 ADR 的) 加入 US opportunity ranker 的 AI universe gate；当前 production basket 还局限在美股本地名字。
5. 给 `audit_production_basket_ai_universe.py` 加 `--strict` 选项，把 watch 列表里的 non-AI 名字也一并审计。
6. CN 指数也覆盖 `000016.SH`（上证50）、`399905.SZ`（中证500），让 hedge selector 能匹配 size-style hedge。
7. 把 readiness ledger 喂回 `expansion_candidates` 晋级脚本：`ready_for_promotion` 自动进入 promotion 候选；`blocked_by_counterevidence` 自动落到 watch-only。

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
