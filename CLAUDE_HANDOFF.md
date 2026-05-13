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
| `scripts/score_ten_x_candidates.py` | 用 source-verification queue + readiness gates + yfinance 市值，筛 sub-$50B D2-D3 弹性候选，输出 `reports/review_dashboard/ai_infra_ten_x_radar/<date>/ten_x_candidates.{csv,md}`。 |
| `scripts/scaffold_evidence_cards_from_readiness.py` | 对 `ready_for_promotion` / `evidence_partial` 行，按 source-evidence-template 生成 evidence card 草稿，落到 `reports/review_dashboard/ai_infra_evidence_card_drafts/<date>/<ticker>.md` 并写 INDEX。 |
| `scripts/derive_promotion_plan_from_readiness.py` | 把 readiness ledger 翻成 promote_now / watch_with_review / research_only / reject_until_resolved 推荐表，落到 `reports/review_dashboard/ai_infra_promotion_plan/<date>/promotion_plan.{csv,md}`。 |
| `scripts/apply_promotion_plan.py` | 人工确认后，把 `promote_now` 行追加到 `ai_infra/reports/expansion_candidates_promoted_v1.csv`。默认 dry-run，需 `--confirm`；append-only，写前自动备份 `.bak`。 |
| `scripts/maintain_promotion_history.py` | 把 daily `promotion_plan.csv` 累积到 `ai_infra/reports/promotion_history.csv`；按 `(as_of, primary_ticker)` 幂等更新。长期追踪 promote/reject 决策。 |
| `scripts/score_mean_reversion_radar.py` | 美股 top-100 市值股票均值回归 radar：大盘涨但个股跌、跌破 EMA21、EMA21 slope 朝下 → 候选。叠加 AI universe 标记。落 `reports/review_dashboard/us_mean_reversion_radar/<date>/mean_reversion_radar.{csv,md}`。 |
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

最近一次整理 (2026-05-13 第五批) 已经跑过：

```text
Python smoke tests (93 tests): pass — adds vs 87:
  tests.test_maintain_promotion_history (4)
  tests.test_score_mean_reversion_radar (2)
Factor Lab tests: pass (12)
cargo check -p quant-stack-cli: pass
verify_ai_supercycle_readiness.py --strict: 10/1/0
audit --strict: US 5/5, CN 1/1
ema_tape_overlay.md: 222 symbols indexed, 85 with metrics;
  Top US bull rising: MU/688008.SH/AMD/DDOG/AKAM slope>+13%/5d, MU px+31% vs EMA21.
ten_x_candidates.md: 4 bull-rising leaders surfaced
  (AAOI +5.81%/5d, MOD +4.67%, NTAP +2.92%, CAMT +2.63%).
promotion_history.csv: 146 rows on 2026-05-13.
us_mean_reversion_radar: 14 candidates / 100, 1 AI-universe overlap (ANET -16.3%/5d).
```

旧批次结果保留：

最近一次整理 (2026-05-13 第四批) 已经跑过：

```text
Python smoke tests (87 tests): pass — adds vs 75:
  tests.test_ema_tape_overlay (6)
  tests.test_apply_promotion_plan (5)
  tests.test_benchmark_attribution +1 (risk block)
Factor Lab tests: pass (12)
cargo check -p quant-stack-cli: pass
verify_ai_supercycle_readiness.py --strict: 10/1/0
audit --strict: US basket=5/all_rows=5, CN basket=1/all_rows=1
ema overlay (US universe): COHR/LITE/AAOI/MU/CSCO all `bull; rising;`,
  MU +31.3% vs EMA21 (stretched), CSCO +9.3%.
AI Book risk block (US 5-name basket):
  max drawdown 20d=-7.25%, 60d=-17.31%; ATR20 avg 4.38%;
  pairwise corr 20d mean 0.50/max 0.81; 60d mean 0.46/max 0.84.
apply_promotion_plan.py: dry-run + --confirm path validated, append-only
  with .bak backup; existing-symbol skip works.
```

旧批次结果保留：

最近一次整理 (2026-05-13 第三批) 已经跑过：

```text
Python smoke tests (75 tests): pass — includes (new vs prior)
  tests.test_score_ten_x_candidates (4)
  tests.test_ai_infra_universe_satellite_adr (3)
  tests.test_scaffold_evidence_cards (3)
  tests.test_derive_promotion_plan (2)
  tests.test_benchmark_attribution +2 (AI book section)
  tests.test_audit_production_basket_ai_universe +1 (strict mode)
Factor Lab tests (from factor-lab/): pass (12)
cargo check -p quant-stack-cli: pass
verify_ai_supercycle_readiness.py --as-of 2026-05-13 --strict:
  ready_with_warnings pass/warn/fail=10/1/0
audit_production_basket_ai_universe.py --as-of 2026-05-13 --strict:
  US basket=5 / all_rows=5 (strict pass)
  CN basket=1 / all_rows=1 (strict pass; 600584.SH)
score_ten_x_candidates.py --as-of 2026-05-13:
  35 candidates, 24 with market cap; top elasticity:
  COHU $2.3B/D3 (91.5), CAMT $8.1B/D3 (84.5), SPXC $10B/D3-D4 (77.5),
  FORM/ONTO/RMBS/NVMI/BESI.AS $10-25B/D3 (74.5).
score_source_review_readiness.py: 146 rows;
  ready_for_promotion=5, evidence_partial=3, pending_human_review=126,
  blocked_by_counterevidence=1, unscored=11.
scaffold_evidence_cards_from_readiness.py: 8 drafts written.
derive_promotion_plan_from_readiness.py: 5 promote_now, 3 watch,
  126 research_only, 1 reject, 0 g0, 11 needs_template_fill.
ingest_cn_index_prices.py: 6 indices ingested (added 000016.SH, 399905.SZ).
AI Book vs Benchmark (US, 5-name basket, 60d):
  alpha 0.59-1.35%/d, beta 1.26-2.76, IR 0.21-0.31 across SPY/QQQ/SMH/IWM/DIA.
AI Book vs Benchmark (CN, 1-name basket, 60d):
  alpha 0.12-0.35%/d, beta 1.17-2.06, IR 0.02-0.14 across 000300/399006/000016/399905.
```

## 已完成的下一步 (2026-05-13)

1. `scripts/audit_production_basket_ai_universe.py` 加 `tests.test_audit_production_basket_ai_universe`，断言每个 production_basket 行 `ai_infra_universe=True`、`ai_infra_current_pool` 非空、`ai_infra_gate.contract=ai_infra_universe_only`。在 `quant-research-v1/tests/test_us_opportunity_ranker.py` 和 `test_cn_opportunity_ranker.py` 中加同样的 production_basket 断言。
2. `scripts/run_main_strategy_v2_backtest.py`：
   - 新增 `build_source_review_calendar` / `render_source_review_calendar_section`，读取 `ai_infra/reports/source_verification_queue_v1.csv`，写入 `source_review_calendar.md`/`.json`，并在 US/CN/combined 三个报告里渲染。
   - 新增 `build_benchmark_attribution` / `render_benchmark_attribution_section`，对 US (SPY/QQQ/SMH/IWM/DIA) 和 CN (000300.SH/399006.SZ/399001.SZ/000001.SH) 输出 1D/5D/20D/60D/YTD 表，写入 `benchmark_attribution.md`/`.json`。
3. `ops/tasks.yaml` 新增 `research.main_strategy_v2_report` (12:10 CST) 和 `research.production_basket_audit` (12:15 CST)。
4. `ops/review_packet.sh` 调用审计脚本，输出 `production_basket_audit.md` 到 review packet。

## 第五批已完成 (2026-05-13 续 4)

1. **Per-symbol EMA artifact** — `render_ema_tape_overlay_markdown` 把 `payload["ema_tape_overlay"]` 单独落 `ema_tape_overlay.{md,json}`，按 cross_state (bull/tangled/bear) → 5d slope 排序。当前 US head: MU/688008.SH/AMD/DDOG/AKAM 全部 `bull` + slope >+13%/5d。
2. **Ten-x radar + EMA tape** — `score_ten_x_candidates.py` 自动读取同 as-of 的 `ema_tape_overlay.json`；候选行多 `ema_cross_state` / `ema_slope_5d_pct` / `ema_dist_close_ema21_pct` 三列；新增 **Top Leaders (bull; rising)** 段，只列 EMA21 > EMA50 + slope > 0.5% + close above EMA21 的名字。当前 4 个 leader: AAOI (+5.81%/5d, +18.9% vs EMA21), MOD, NTAP, CAMT。
3. **Promotion history ledger** — `scripts/maintain_promotion_history.py` 把每日 promotion_plan 追加到 `ai_infra/reports/promotion_history.csv`，按 `(as_of, primary_ticker)` 幂等。当前已记录 146 行。`tests/test_maintain_promotion_history.py` 覆盖 4 case。
4. **US top-100 mean-reversion radar** — `scripts/score_mean_reversion_radar.py`：从 `company_profile` 拿最新 market_cap 取 top 100，叠加 prices_daily 算 5d/20d return + EMA21/50 + slope；触发条件 SPY/QQQ 5d ≥+1% 且 个股 5d ≤-2% 且 px<EMA21 ≥2% 且 EMA21 5d slope <0。当前 14 个候选，1 个与 AI universe 重合 (`ANET` 阿瑞斯塔网络 -16.3%/5d)。`tests/test_score_mean_reversion_radar.py` 覆盖 2 case。
5. **Ops cron 扩展** — 新增 `research.mean_reversion_radar` (12:20 CST) 和 `research.promotion_history` (12:13 CST)。

## 第四批已完成 (2026-05-13 续 3)

1. **EMA 21/50 tape overlay** — `build_ema_tape_overlay` 计算 EMA21/EMA50/cross_state/recent_cross/5d slope/距 EMA21/EMA50 pct。路由 `*.SH/*.SZ` → CN db，其他 → US db。Source-review calendar 和 satellite pool 表多 Tape 列 (例：`bull; rising; px +13.8% vs EMA21`)。方法论允许 K-line 做 tape/crowding/risk，不证基本面。`payload["ema_tape_overlay"]` 保留完整 metrics。`tests/test_ema_tape_overlay.py` 覆盖 6 case。
2. **EWT/EWJ/EWY 进 US_HEDGE_BENCHMARKS** — `scripts/lib/hedge.py` 加 3 个 region ETF；hedge selector 可在 satellite-heavy book 上挑区域 ETF。当前 basket 还是按 SPY/QQQ/SMH 选，区域 ETF 已加入候选。
3. **AI Book risk block** — `_max_drawdown_pct` / `_atr_proxy` / `_pairwise_corr` 三个 helper，在 `build_benchmark_attribution.ai_book[market]` 加 `risk` 子段 (max_drawdown 20d/60d, avg_atr20_pct, pairwise_corr 20d/60d 含 mean/max/min/n_pairs)。Render 在 AI Book vs Benchmark 表下面加 `### Risk block`。当前 US 5-name basket: drawdown 20d/60d = -7.25%/-17.31%, ATR20=4.38%, 20d corr mean 0.50/max 0.81。
4. **promotion plan → expansion_candidates_promoted 闭环** — `scripts/apply_promotion_plan.py`：读 `promotion_plan.csv`，只对 `recommendation=promote_now` 行操作；默认 dry-run；`--confirm` + 可选 `--tickers` 子集才 append；写前 backup `.bak`；按 symbol skip 已存在行。`tests/test_apply_promotion_plan.py` 覆盖 5 case (dry-run/confirm/tickers/skip-existing/missing-plan)。

## 第三批已完成 (2026-05-13 续 2)

1. **10x 候选 radar** — `scripts/score_ten_x_candidates.py` + `tests/test_score_ten_x_candidates.py`。从 readiness ledger 起步，叠加 yfinance 市值，按 `mcap < $50B AND BFS depth ∈ {D2,D2-D3,D3,D3-D4} AND counter≤3 项` 过滤，按 elasticity score 排序。当前 top: COHU $2.3B/D3 (91.5), CAMT $8.1B/D3 (84.5), SPXC $10B/D3-D4 (77.5), FORM/ONTO/RMBS/NVMI/BESI.AS $10-25B (74.5)。Market cap 结果带 7 天 cache (`reports/review_dashboard/ai_infra_ten_x_radar/market_cap_cache.json`)，避免重复打 yfinance。
2. **ADR satellites into US ranker** — `quant-research-v1/src/quant_bot/analytics/ai_infra_universe.py` 新增 `SATELLITE_US_ADRS` 显式映射 (现仅 `ASML.AS → ASML`)，叠加现有 `2330.TW / TSM` 形式的 alias 处理。`tests/test_ai_infra_universe_satellite_adr.py` 覆盖。US universe 现含 7 个卫星 ADR：TSM、ASX、ASML、ABB、CAMT、NVMI、TSEM。
3. **CN 指数扩展** — `ingest_cn_index_prices.py` + run_main_strategy_v2 都增加 `000016.SH` (上证50) 和 `399905.SZ` (中证500)。CN benchmark snapshot 现有 6 行全数据。
4. **audit `--strict`** — `audit_production_basket_ai_universe.py` 新增 `--strict`，在 `production_basket` 之外也校验 `all_rows`，避免 watch/research-only 行绕过 AI universe gate。`tests/test_audit_production_basket_ai_universe.py` 补 strict 模式 case。新增 cron `research.ten_x_radar_strict` (12:16 CST)。
5. **Evidence card 自动草稿** — `scripts/scaffold_evidence_cards_from_readiness.py` 按 source-evidence-template.md 给每个 `ready_for_promotion` 或 `evidence_partial` 行生成模板，预填 BFS path / source checklist / 证据 anchor 行 / counterevidence / upgrade conditions，并写 `INDEX.md`。当前给出 8 张草稿 (TSM/NVDA/AMZN/GOOGL/CRWV/AVGO/MRVL/ORCL)。`tests/test_scaffold_evidence_cards.py` 覆盖。
6. **readiness → promotion 闭环** — `scripts/derive_promotion_plan_from_readiness.py`：readiness tier 映射到 `promote_now` / `watch_with_review` / `research_only` / `reject_until_resolved` / `gate_g0_no_promotion` / `needs_template_fill`，生成 `promotion_plan.{csv,md}`。不修改 universe 文件，只产生人工 review 用的建议。`tests/test_derive_promotion_plan.py` 覆盖。当前 5 promote_now / 3 watch / 126 research_only / 1 reject / 0 g0 / 11 needs_template_fill。
7. **AI book alpha/beta/IR** — `build_benchmark_attribution` 接受 `us_basket` / `cn_basket`，新增 `_ai_book_return_series` (equal-weight 篮子日 return) 和 `_compute_alpha_beta` (active return / daily alpha / beta / IR)，window=20/60 天。US/CN daily report 各加 **AI Book vs Benchmark** 段。当前 US 5-name basket 60d beta vs SMH ≈ 1.26, IR 0.21；CN 1-name basket (600584.SH) 60d beta vs 沪深300 ≈ 2.06, IR 0.10。`tests/test_benchmark_attribution.py` 加 case。
8. **Ops cron 接入** — 新增 `research.ten_x_radar` (12:12)、`research.evidence_card_drafts` (12:09)、`research.promotion_plan` (12:11)、`research.ten_x_radar_strict` (12:16)。

## 第二批已完成 (2026-05-13 续)

1. `scripts/score_source_review_readiness.py` 落地 G0-G4 gate，按方法论给 source-verification queue 打 readiness tier；同一逻辑内联进 `build_source_review_calendar`，daily report 的 Source Review 表多了一列 Readiness。
2. `scripts/run_main_strategy_v2_backtest.py` 新增 `build_satellite_pool_report` / `render_satellite_pool_report_section`：卫星资产池 50 个名字按 Taiwan/Japan/Korea/Europe/Israel 拆分、按 BFS depth 汇总、按 readiness tier 分类。落 `satellite_pool_report.{md,json}` 并进入 combined report。
3. `scripts/ingest_cn_index_prices.py` 用 AKShare 把 `000001.SH`/`399001.SZ`/`399006.SZ`/`000300.SH` 共 658 行 / 指数 补进 CN db；benchmark snapshot 不再有 missing rows。
4. `scripts/ingest_satellite_index_prices.py` 用 yfinance 把 `^TWII`/`^N225`/`^KS11`/`^AEX` 与 `EWT/EWJ/EWY/EWN` ETF 镜像补进 US db；benchmark 章节新增 “Satellite Benchmark Snapshot” 表。
5. `audit_production_basket_ai_universe.py` 输出 `by_current_pool` / `by_bfs_depth` 覆盖矩阵，供 ops cron 和 review packet 追踪 satellite vs core 的混合比例。
6. `ops/tasks.yaml` 新增 `research.cn_index_ingest` (06:00 CST)、`research.satellite_index_ingest` (06:05 CST)、`research.source_review_readiness` (12:08 CST)。

## 建议的下一步

1. **SEC EDGAR 财报抽取** — 留给 Factor Lab 探索（用户已决定本仓库不直接拉接口）。
2. **Factor Lab `DATA_REQUIREMENTS` schema 合同**。
3. **海外指数 ingestion 扩展** — `^HSI`/`^STI`/`^FTSE`/`^IBEX`。
4. **AKShare bridge 完整集成** — `cn_index_ingest` 直接走 producer 的 FastAPI 桥。
5. **Mean-reversion radar follow-up** — 给每个候选添加 `next_earnings_date` (避开财报前)，并在 EMA50 之上 / 估值合理 (PE < 行业中位数) 加额外筛选。
6. **Tape leader vs mean-reversion 交叉** — 把 ten-x bull-rising leaders 和 mean-reversion 候选并入同一对比页，方便操作员同时看「领头」和「滞后」两个池。
7. **Promotion history 回测** — `promotion_history.csv` 配合 prices_daily，回算 promote_now 之后 5/20/60 日的相对 SPY 收益，做 alpha tracking。
8. **mean-reversion radar 加价值/估值层** — 当前只看 tape，加 PE/PS/FCF 比较增加 conviction。
9. **A 股 mean-reversion 镜像** — 仿照 US radar 给 A 股 top 100 (沪深300 + 中证500) 也做同样的均值回归 radar。

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
