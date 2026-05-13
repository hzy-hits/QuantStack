# Alpha Sleeve Engineering Plan

Date: 2026-05-09

## Objective

把交易直觉工程化成可以复盘、可以晋级、可以给 R 的 sleeve，而不是继续只改 prompt 或只在当天报告里做主观解释。

本计划只做三件事：

1. A股新增历史可验证 sleeve：`cn_tape_leadership_continuation`
2. 美股新增历史可验证 sleeve：`us_theme_cluster_momentum`
3. 组合层历史回测改成：`long alpha return - beta hedge return`

核心原则：新闻不是 A股第一信号；主题不是美股单票 HIGH/MOD 硬拦；组合收益必须按 hedge 后净 PnL 复盘。

## Cross-Plan Ownership

`PROJECT_CONSOLIDATION_PLAN.md` 拥有 ops/task registry、cron 统一入口、review packet、risk path tagging，以及既有 alpha sleeve 文件的零行为拆分。

本计划拥有 Phase 0.6 之后的交易工程内容：guardrail tests、historical hedge ledger、`cn_tape_leadership_continuation`、`us_theme_cluster_momentum`、`promoted_sleeves` 晋级写入和生产断言。

依赖关系：

- Phase 0' = Project Consolidation Phase 0-3 完成。
- 模块拆分是 Project Consolidation Phase 4 的交付物；本计划只在已拆分的 `scripts/sleeves/` 内新增 sleeve/hedge 文件。
- `scripts/lib/hedge.py` 只在本计划 Phase 1 historical hedge ledger 中抽出，因为此时才有当前 overlay 与历史 ledger 两个真实调用方。

## Current Diagnosis

当前不是完全没有 alpha，而是 alpha 工程化没有跟上交易观察。

A股当前已把 production ranker 改成 price/flow first，但历史执行 sleeve 仍主要来自 oversold/lifecycle。结果是：

- 能晋级的样本集中在 `cn_oversold_ev_positive` / observed lifecycle。
- 强趋势延续、价格先动、成交放大、板块同步这类更接近 A股真实 tape 的机会，还没有被做成历史可验证 sleeve。
- 新闻仍容易在报告心智里占据过高权重，但 A股新闻经常是滞后标签，不应该作为 fresh entry 的主驱动。

美股当前已有 joint signal，能看到 CRCL、NVTS、RKLB、GFS、HIMS、RGTI、AXON、COIN 这类主题/flow 名字，但生产合同仍只有 `us_v2_stock_probe` 可以给交易层：

- `scripts/run_main_strategy_v2_backtest.py:474` 的 `us_alpha_factory_sleeve_id()` 只认 LOW/core/executable/trending。
- 高 joint score 的主题票经常被留在 ranked watch。
- 美股真实 alpha 往往是 theme/basket repricing，不是单票 HIGH/MOD 规则能直接表达。

组合层已经有当前日 portfolio overlay，但历史收益还没有按净 hedge PnL 复盘：

- `scripts/run_main_strategy_v2_backtest.py:2972` 当前只做当日 risk overlay。
- `scripts/run_alpha_sleeve_backtest.py` 已写 `alpha_sleeve_daily_returns`，但没有 `portfolio_hedged_backtest` / `portfolio_hedge_ledger`。
- 所以当前还不能回答“如果按报告结果执行，并按 beta hedge 后，过去净收益是多少”。

## Engineering Architecture

在新增 sleeve 之前，先做一次零行为变更的拆分。`scripts/run_alpha_sleeve_backtest.py` 已经承担 CLI、渲染、落库、11 个 sleeve 构造和统计逻辑；继续往里面加 `cn_tape_leadership_continuation`、`us_theme_cluster_momentum`、hedge ledger，会把 review 面积推到不可维护区间。

目标结构：

```text
scripts/
  run_alpha_sleeve_backtest.py   # CLI, orchestration, render/write only
  sleeves/
    __init__.py                  # build_sleeves(), public registry
    base.py                      # Sleeve dataclass, make_sleeve(), shared metrics helpers
    cn_oversold.py               # existing CN oversold/log denoise sleeves
    cn_forecast.py               # existing CN forecast sleeves
    cn_cb.py                     # existing convertible bond sleeves
    us_v2.py                     # existing US V2 stock sleeve
    us_legacy.py                 # existing US legacy baseline
    us_options.py                # existing US option shadow sleeve
    us_filings.py                # existing US SEC filing sleeve
    cn_tape_leadership.py        # new
    us_theme_cluster.py          # new
    portfolio_hedge.py           # historical hedge ledger and hedged portfolio metrics
  lib/
    hedge.py                     # shared beta hedge selection/math extracted from main_strategy_v2
```

零行为拆分验收：

- 拆分前后 `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01` 的 sleeve metrics、daily returns、correlation output 必须一致。
- `scripts/run_main_strategy_v2_backtest.py` 继续能生成相同的当前日 portfolio overlay。
- 新模块不改变任何现有 sleeve 的 money_status、role、metrics。

Hedge 逻辑不能新写第二份。当前 `run_main_strategy_v2_backtest.py` 已有 `_select_beta_hedge` / portfolio overlay 相关逻辑；只有在 historical hedge ledger 落地时才抽到 `scripts/lib/hedge.py`，再由当前日 overlay 和历史 ledger 共用，避免两个 hedge 实现漂移。

## Investigation Snapshot

### A股 data coverage

数据库：`quant-research-cn/data/quant_cn_report.duckdb`

可用于 `cn_tape_leadership_continuation` 的表：

| Table | Rows | Date coverage | Use |
| --- | ---: | --- | --- |
| `prices` | 375332 | 2023-01-03 to 2026-05-08 | 价格、1/5/20日收益、成交额、成交量、突破/回撤 |
| `daily_basic` | 1189732 | 2023-01-03 to 2026-05-08 | 换手率、量比、市值、估值、流动性过滤 |
| `moneyflow` | 1016775 | 2025-07-14 to 2026-05-08 | 大单/超大单资金流、主动资金确认 |
| `margin_detail` | 797217 | 2025-07-14 to 2026-05-07 | 融资变化，作为次级确认 |
| `analytics` | 3193525 | 2026-03-12 to 2026-05-08 | 已有 flow/sector_rotation 等衍生指标 |
| `sector_fund_flow` | 7959 | 2026-03-13 to 2026-05-08 | 板块资金、板块涨跌确认 |
| `concept_board` | 12088 | 2026-03-13 onward | 概念板块涨跌、涨跌家数、领涨股 |
| `theme_clusters` | 72 | 2026-03-13 onward | 已有概念主题聚类，可作为解释/标签 |
| `news_enriched` / `stock_news` | 656 / 1097 | 2026-03-05 to 2026-05-08 | 滞后新闻标签、风险否决，不作主信号 |
| `fut_daily` | 37343 | 2026-03-11 to 2026-05-08 | IM/IC/IF/IH hedge return |

注意：`price_features` 只有 2026-05-06 到 2026-05-08，不能作为 3月/4月历史 sleeve 的直接来源。历史特征必须先从 `prices`、`daily_basic`、`moneyflow`、`sector_fund_flow` 回填到持久化 `cn_tape_features`，再由 Python 回测读取。

现有可复用代码：

- `quant-research-cn/src/analytics/sector_rotation.rs` 已按 `stock_basic.industry` 计算行业 5D/20D 动量和 flow score。
- `quant-research-cn/src/enrichment/themes.rs` 已把概念板块聚成主题，但目前是解释层，不是交易 sleeve。
- 拆分后 `scripts/sleeves/__init__.py::build_sleeves()` 是新增 sleeve 的注册入口。

### 美股 data coverage

数据库：`quant-research-v1/data/quant.duckdb`

可用于 `us_theme_cluster_momentum` 的表：

| Table | Rows | Date coverage | Use |
| --- | ---: | --- | --- |
| `prices_daily` | 432182 | 2024-03-07 to 2026-05-08 | 个股和 ETF 价格、theme basket return、SPY/QQQ/IWM hedge |
| `report_decisions` | 8040 | 2026-03-10 to 2026-05-08 | 当天报告候选、bucket/confidence/execution/details_json |
| `report_outcomes` | 8040 | 2026-03-10 to 2026-05-08 | 3日持有收益 |
| `options_alpha` | 3882 | 2026-04-23 to 2026-05-08 | options/flow edge、liquidity gate、expression |
| `options_analysis` | 26735 | 2026-03-08 to 2026-05-08 | 期权辅助特征，早于 `options_alpha` |
| `options_chain_quotes` | 512814 | 2026-05-01 to 2026-05-08 | 真实 bid/ask leg PnL，但历史覆盖短 |
| `news_items` | 62990 | 2020-06-08 to 2026-05-08 | 新闻关键词和主题 fresh catalyst |
| `company_profile` | 8439 | 2026-03-10 to 2026-05-08 | sector/industry、基本面标签 |
| `universe_constituents` | 503 | 2026-05-04 | SP500 sector tags，覆盖有限 |

现有可复用代码：

- `quant-research-v1/src/quant_bot/analytics/clustering.py` 已有相关性聚类，可用于 theme 内 independent bet / concentration 检查。
- `report_decisions.details_json` 已有 `sub_scores.magnitude/event/momentum/options/cross_asset` 和 `execution_gate` 字段，可直接参与 theme cluster scoring。
- `scripts/run_main_strategy_v2_backtest.py:455` 仍按单票 LOW/core/trending 做 V2 policy，这是美股主题票被卡住的主要代码入口。

### Factor Lab data

数据库：`factor-lab/data/factor_lab.duckdb`

可用表：

- `factor_sleeve_returns`: 1498 rows
- `factor_experiment_ledger`: 4187 rows
- `factor_registry`: 24 rows
- `daily_candidates`: 138 rows
- `paper_picks`: 200 rows
- `paper_returns`: 10 rows

这些数据目前可作为 overlay/diagnostic，但不应该替代本计划的两个新 sleeve。原因是 Factor Lab 当前多是 daily price overlay / research-only 因子，不等于 A股 tape leadership 或美股 theme basket 的完整交易定义。

### Coverage-aware metrics

promotion gate 不能只看一个混合样本。当前数据覆盖不均匀：

- A股 `sector_fund_flow` / `concept_board` / `theme_clusters` 从 2026-03-13 才开始。
- 美股 `options_alpha` 从 2026-04-23 才开始。
- 美股 `options_chain_quotes` 从 2026-05-01 才开始，只适合真实 bid/ask 诊断，不适合完整 3月/4月 sleeve gate。

每个新 sleeve 必须输出分层样本：

- `n_full_history`: 全窗口候选数。
- `n_with_full_confirm`: 所有核心确认源齐全的候选数。
- `n_with_proxy_confirm`: 使用降级代理确认的候选数。
- `n_missing_confirm`: 缺少关键确认源的候选数。
- `coverage_start`: 当前 sleeve 真正具备 full confirm 的起始日期。
- `metrics_full_history`: 全窗口趋势参考。
- `metrics_full_confirm`: 正式晋级 gate 只看这一组。
- `metrics_proxy_confirm`: 只判断方向，不允许单独晋级。

A股在 2026-03-13 之前没有完整板块确认，CN tape 的正式 gate 应用在 `has_sector_confirm` 子集。美股在 2026-04-23 之前没有完整 `options_alpha`，US theme 的正式 options-confirmed gate 应用在 `has_options_confirm` 子集；早期 proxy 样本只能证明主题方向，不证明 options/flow confirmed alpha。

## Sleeve 1: `cn_tape_leadership_continuation`

### Hypothesis

A股很多可交易机会是价格和成交先动，新闻后到。真正要捕捉的是：

价格先突破或强趋势延续、成交/换手放大、主动资金确认、板块/行业同步。新闻只作为滞后标签和风险否决，不作为 fresh entry 的主信号。

### Candidate construction

信号日为 `d`，只能使用 `d` 收盘前已知数据。候选进入 next tradable session，回测 T+1/T+3/T+5 close exits。

基础 universe：

- `stock_basic.list_status = 'L'`
- 排除 ST / 退市整理 / 明显不可交易名称。
- 默认排除北交所；科创/创业可保留但单独标记，报告执行层可按账户权限过滤。
- 20日平均成交额满足流动性下限，例如 `avg_amount_20d >= 2e8` RMB；小票可进研究层但不直接给 R。
- 市值和换手做极端过滤，避免纯庄股和极端流动性断层。

价格 leadership 条件，至少满足一种：

1. Breakout leader：
   - `ret_5d >= 8%` 或 `ret_20d >= 18%`
   - close 位于 20日价格区间上 70% 分位以上，或创 20日新高附近
   - 当日收益位于全市场前 25% 或本行业前 20%
2. Re-acceleration leader：
   - `ret_20d >= 15%`
   - 最近 2-3 日完成浅回撤后重新转强
   - 当日成交额/成交量重新放大
3. Sector leader：
   - 所属行业/概念板块进入 top momentum/flow 分位
   - 个股在板块内 5日收益或当日收益排名靠前

成交与流动性确认：

- `amount_1d / avg_amount_20d >= 1.3`，或 `daily_basic.volume_ratio >= 1.4`
- `turnover_rate` 不过低；极端过高换手视为 blowoff risk。
- 连续缩量上涨不晋级，只进观察。

资金流确认：

- `moneyflow` 中大单/超大单净流入为正，或大单净流入占成交额分位靠前。
- 若 `analytics.module='flow'` 的 `information_score` 可用，则要求行业内或全市场相对为正。
- `margin_detail` 只作为次级确认，不作必需条件。

板块同步确认：

- `stock_basic.industry` 维度：行业内上涨家数占比、行业 5D return、行业 flow score 至少一项进入前分位。
- `sector_fund_flow` 维度：`main_net_pct > 0` 或 `super_net_in > 0` 优先。
- `concept_board` / `theme_clusters` 只用于解释和主题归因，不做单独买入理由。

新闻处理：

- 正面新闻不加核心分，只标记为 `lagging_news_confirmed`。
- 信号日前已有重大负面、监管、减持、财报暴雷类新闻，直接风险否决。
- 信号日后才出现的新闻只能用于复盘标签，不允许回填进信号分。

### Bad-ticket filters

必须专门过滤历史里最容易坏的票：

- 温吞动量：`ret_5d` 大约 2%-8%、`ret_20d` 不强、成交没有放大、板块没有同步。这类不晋级。
- Late/chase：20日涨幅过大、连续涨停后换手衰竭、放量长上影、尾盘拉升但资金不确认。
- 单点异动：只有个股涨，没有行业/概念共振。
- 新闻追涨：只有新闻热度，没有价格/成交/资金领先。
- 小票庄股：成交额不足、换手极端、资金流单日异常但不可持续。

### Return labels

每个候选至少生成这些标签：

- `entry_date`: next tradable date
- `entry_price`: next open；若 next open 缺失，用 next close 并标记 `entry_proxy=close`
- `exit_1d_close_ret_pct`
- `exit_3d_close_ret_pct`
- `exit_5d_close_ret_pct`
- `mfe_5d_pct`
- `mae_5d_pct`
- `cost_adjusted_return_pct`: 默认单边/双边成本都要可切换，晋级看 double-cost 后 LCB

### Promotion gates

`cn_tape_leadership_continuation` 不能因为看起来合理就给 R。晋级需要：

- `metrics_full_confirm.n >= 100`；如果 2026-03-13 到 2026-05-08 的 full-confirm 样本不足，先保持 research，不用 proxy 样本硬凑晋级
- active trading days >= 20
- double-cost 后 `lcb80_pct > 0`
- beta-adjusted residual `lcb80_pct > 0`
- max drawdown 优于 -8% 到 -10% 区间，具体阈值由样本波动决定
- top5 PnL share <= 30%-35%
- 与 CN oversold 整族 daily return corr 不超过 0.80，至少包括 `cn_oversold_ev_positive`、`cn_oversold_residual_z_action`、`cn_oversold_deep_log20_setup`
- 温吞动量子桶必须显著弱于强 tape 子桶，否则说明定义太松
- `n_with_full_confirm`、`n_with_proxy_confirm`、`n_missing_confirm` 必须在报告里显式展示，不能把不同数据质量混在一个 LCB 里

### Implementation hooks

Python 不负责重算全部 A股派生特征。Rust 侧先把可复用 tape 特征持久化，Python 只做 query、candidate filter 和 return labeling。

新增持久化表建议：`cn_tape_features`

| Column | Meaning |
| --- | --- |
| `trade_date` | 交易日 |
| `ts_code` | 股票代码 |
| `industry` | 行业 |
| `ret_1d_pct` / `ret_5d_pct` / `ret_20d_pct` | 个股动量 |
| `amount_ratio_20d` | 当日成交额 / 20日均成交额 |
| `volume_ratio_20d` | 当日成交量 / 20日均成交量 |
| `turnover_rate` | 换手率 |
| `large_flow_net_pct` | 大单/超大单净流入占比 |
| `flow_information_score` | 已有 flow score 或降级代理 |
| `industry_ret_5d_pct` / `industry_ret_20d_pct` | 行业动量 |
| `industry_breadth_pct` | 行业内上涨占比 |
| `industry_flow_score` | 行业资金流 |
| `relative_industry_rank` | 个股在行业内强度排名 |
| `has_sector_confirm` | 板块/行业确认源是否齐全 |
| `confirm_quality` | `full` / `proxy` / `missing` |

Rust 接入：

- 新增 `quant-research-cn/src/analytics/tape_leadership.rs` 或扩展现有 `sector_rotation.rs`。
- 写入 `cn_tape_features`，按 `trade_date × ts_code` 增量可重跑。
- `price_features` 只有 3 天历史，这说明派生特征回填管线本身是前置任务；不能让 Python 每次回测临时重跑一套不同逻辑。

拆分后新增函数放在 `scripts/sleeves/cn_tape_leadership.py`：

- `query_cn_tape_leadership_returns(cn_db, start, as_of) -> list[dict]`
- `load_cn_tape_features(cn_db, start, as_of)`
- `label_cn_tape_returns(...)`

然后在 `scripts/sleeves/__init__.py::build_sleeves()` 中注册：

```python
sleeves.append(
    make_sleeve(
        sleeve_id="cn_tape_leadership_continuation",
        market="cn",
        label="CN tape leadership continuation",
        signal_rule="price leads, volume expands, flow confirms, sector breadth confirms; news is lagging label only",
        horizon="1/3/5 sessions",
        data_status=tape_status,
        role="research_until_promoted",
        notes=tape_notes,
        rows=cn_tape_rows,
        min_money_n=min_money_n,
    )
)
```

生产接入点：

- `scripts/run_main_strategy_v2_backtest.py:summarize_cn()` 增加 current-day tape candidate emitter。
- `quant-research-v1/src/quant_bot/analytics/cn_opportunity_ranker.py` 允许 `alpha_sleeve_id == "cn_tape_leadership_continuation"` 的候选进入 execution layer。
- A股 prompts 只写规则合同：新闻是滞后标签；不得把新闻候选升级为 fresh entry。

## Sleeve 2: `us_theme_cluster_momentum`

### Hypothesis

美股很多 alpha 是主题级别 repricing。单票 HIGH/MOD、单票 event、单票 R:R 太容易把 theme tape 打散。正确表达应是：

主题强 + basket breadth 强 + 价格强 + options/flow 强，才允许主题内股票给 R。

### Theme taxonomy

第一版用稳定 taxonomy，避免每天 LLM 乱改主题：

- `crypto_stablecoin`: CRCL, COIN, HOOD, MSTR, CLSK, MARA 等
- `ai_semis_infra`: NVTS, GFS, ARM, AMD, AVGO, MRVL, SMCI 等
- `space_defense`: RKLB, LUNR, ASTS, AXON, PLTR 等
- `quantum`: RGTI, IONQ, QBTS, QUBT 等
- `nuclear_power_grid`: OKLO, SMR, VST, CEG, NRG 等
- `software_ai_apps`: APP, SNOW, DDOG, NET, CRWD 等
- `healthcare_consumer_ai`: HIMS, TEM, GLP-1/telehealth 相关篮子

taxonomy 来源：

- `data/us_theme_seed_map.yaml`，作为第一优先级，不把主题成员硬编码进 Python。
- `company_profile.sector/industry` 作为行业标签。
- `news_items` headline keyword 和 `report_decisions.primary_reason/details_json` 作为动态补充。
- 相关性聚类 `quant-research-v1/src/quant_bot/analytics/clustering.py` 用来检查同主题内是否只是一个独立风险。

Seed map schema：

```yaml
themes:
  - theme_id: crypto_stablecoin
    label: Crypto stablecoin rails
    inception_date: 2026-03-01
    benchmark: IWM
    aliases: ["stablecoin", "crypto rails", "digital assets"]
    members: ["CRCL", "COIN", "HOOD", "MSTR", "CLSK", "MARA"]
```

规则：

- 新主题/成员调整改 YAML，不改代码。
- 回测按 git 历史追溯 seed map 版本；如果未来需要 event-sourced theme definitions，再把 YAML 迁移成带 effective_date 的表。
- LLM 只能建议 YAML diff，不能在运行时动态越权新增可交易 theme。

### Candidate construction

信号日 `d` 生成 theme basket，而不是先生成单票 fresh entry。

主题晋级条件：

- theme candidate 数 >= 3；特殊高流动性主题可允许 2 个核心票，但必须标记 concentration risk。
- theme 5D 或 3D basket return > SPY/QQQ/IWM 中合适 benchmark。
- theme breadth：主题内上涨占比 >= 60%，或 top names 同步创短期强势。
- options/flow confirmation：
  - `options_alpha.flow_edge` / `directional_edge` / `liquidity_gate` 可用时参与；
  - `options_alpha` 不可用的早期样本，用 `options_analysis` 和 report `details_json.sub_scores.options` 降级代理；
  - options 只是确认，不是交易本体。
- report joint evidence：
  - `sub_scores.magnitude/event/momentum/options/cross_asset` 的主题均值或 top-k 均值达到阈值。
  - 单票 `execution_gate` 若显示 extreme chase / bad R:R，可以降低该票权重，但不直接否定整个主题。

个股分配：

- 每个 theme 取 top 3-8 names。
- 单票权重上限 25%-35%。
- 同一相关性 cluster 的合计权重上限 50%。
- 主题 basket 通过后，主题内股票才允许给 R；不再要求每个单票都通过旧 HIGH/MOD 硬拦。

### Bad-ticket filters

- 只有一只股票涨的 single-name event，不算 theme。
- options 分数高但股价弱、主题 breadth 弱，不晋级。
- 新闻热但价格没确认，不晋级。
- ret_5d 已极端拉升且 next open gap 超过 expected move，降级为 no-chase/radar。
- 主题 basket 的收益如果由单票贡献超过 50%，降级为 concentration watch。

### Return labels

每个 theme/day 生成 basket return：

- `theme_id`
- `members`
- `member_weights`
- `entry_date`: next tradable date
- `entry_prices`
- `exit_3d_close_ret_pct`
- `exit_5d_close_ret_pct`
- `benchmark`: SPY/QQQ/IWM 或 sector ETF proxy
- `excess_ret_pct`: basket return - benchmark return
- `cost_adjusted_return_pct`
- `top_contributor_share`
- `theme_breadth`

### Promotion gates

`us_theme_cluster_momentum` 晋级需要：

- `metrics_full_confirm` 中 theme/day baskets >= 30，或 active days >= 15 且样本覆盖多个主题；proxy-only 样本不允许单独晋级
- cost-adjusted basket `lcb80_pct > 0`
- benchmark-excess `lcb80_pct > 0`
- hit rate >= 55%
- max drawdown 优于 -8% 到 -10%
- top5 PnL share <= 35%
- 单一 theme 的 PnL share <= 50%，否则只允许该 theme 内部 limited R
- options/flow 缺失样本单独报告，不能混同成真实 options-confirmed alpha
- 与 `us_v2_stock_probe`、`us_legacy_high_mod`、`us_option_shadow_long` 的 daily return corr 不超过 0.80，证明这是新增 theme alpha，不是旧单票规则重命名
- 必须输出 `n_with_full_confirm`、`n_with_proxy_confirm`、`n_missing_confirm` 和 `has_options_confirm` 子窗口 metrics

### Implementation hooks

新增函数放在 `scripts/sleeves/us_theme_cluster.py`：

- `query_us_theme_cluster_returns(us_db, start, as_of) -> list[dict]`
- `load_us_theme_seed_map(path="data/us_theme_seed_map.yaml")`
- `score_us_theme_day(...)`
- `label_us_theme_basket_returns(...)`

然后在 `scripts/sleeves/__init__.py::build_sleeves()` 中注册：

```python
sleeves.append(
    make_sleeve(
        sleeve_id="us_theme_cluster_momentum",
        market="us",
        label="US theme cluster momentum basket",
        signal_rule="theme breadth + price strength + options/flow confirmation; trade stock basket, not option PnL",
        horizon="3/5 sessions",
        data_status=theme_status,
        role="research_until_promoted",
        notes=theme_notes,
        rows=us_theme_rows,
        min_money_n=min_money_n,
    )
)
```

生产接入点：

- `scripts/run_main_strategy_v2_backtest.py:us_alpha_factory_sleeve_id()` 增加 theme membership 判断。
- `quant-research-v1/src/quant_bot/analytics/us_opportunity_ranker.py` 允许 `alpha_sleeve_id == "us_theme_cluster_momentum"` 的股票进入 stock trade tier。
- `scripts/run_main_strategy_v2_backtest.py:summarize_us()` 输出 theme_id、basket score、theme hedge benchmark、theme concentration risk。
- prompts 只要求 agents 给出 theme/tape/flow 证据，不允许 agents 自己绕过 sleeve promotion gate。

## Workstream 3: Historical Hedged Portfolio PnL

### Problem

当前报告有当日 beta hedge overlay，但历史回测只看 sleeve long returns。真实组合要回答的是：

```text
net alpha return = long alpha return - beta hedge return - costs
```

也就是：如果当天执行报告里的 long alpha，同时做 beta hedge，历史净收益、回撤、风险归因是什么。

### Required ledger

新增两个 DuckDB 表，建议写入 alpha factory backtest 输出 DB。

`portfolio_hedged_backtest`：

| Column | Meaning |
| --- | --- |
| `as_of` | 回测生成日 |
| `return_date` | 收益日期 |
| `market` | cn/us/global |
| `sleeve_id` | 来源 sleeve |
| `long_return_r` | long alpha book 当日 R 收益 |
| `beta_hedge_return_r` | hedge leg 当日 R 收益，按 long beta exposure * benchmark return |
| `net_return_r` | `long_return_r - beta_hedge_return_r - hedge_cost_r` |
| `gross_long_r` | 当日 long gross R |
| `hedge_notional_r` | hedge notional R |
| `net_beta_r` | hedge 后剩余 beta |
| `benchmark` | SPY/QQQ/IWM/IM/IC/IF/IH |
| `detail_json` | beta、cost、basis risk、member details |

`portfolio_hedge_ledger`：

| Column | Meaning |
| --- | --- |
| `signal_date` | 信号日 |
| `entry_date` | 入场日 |
| `exit_date` | 出场日 |
| `market` | cn/us |
| `sleeve_id` | sleeve |
| `symbol_or_basket` | 单票或 basket id |
| `long_r` | long R |
| `hedge_instrument` | hedge instrument |
| `hedge_r` | hedge R |
| `beta` | estimated beta |
| `long_ret_pct` | long return |
| `hedge_ret_pct` | hedge instrument return |
| `net_r` | 净 R |
| `reason_json` | hedge 选择原因、风险归因 |

### Hedge selection

A股：

- 首选 `IM.CFX` / `IC.CFX` 对中小盘成长和主题票。
- `IF.CFX` / `IH.CFX` 用于大盘权重或金融/消费类。
- 若 `fut_daily` 当日缺失，使用可用的最近 benchmark 或降级为 unhedged with missing hedge flag。

美股：

- `QQQ` 用于 growth/AI/software/semis。
- `IWM` 用于 high beta small/mid cap themes，例如 space/quantum/spec growth。
- `SPY` 用于 broad market hedge。
- `DIA` 只作为低 beta/工业权重备选。

### Beta model

第一版不要过度复杂，先做可审计：

- 用过去 60 个交易日 sleeve/basket return vs benchmark return 做 rolling beta。
- 样本不足时：
  - 单票使用 symbol vs benchmark 60D beta。
  - basket 使用成员等权组合 vs benchmark beta。
  - 仍不足时用市场默认 beta，并标记 `beta_proxy=true`。
- beta floor 沿用当前代码思想：
  - CN floor: 0.35
  - US floor: 0.30
- hedge ratio：
  - CN 默认 0.70
  - US 默认 0.50
  - theme/sleeve 可根据 drawdown 和 residual alpha 调整，但必须落 ledger。

### Portfolio metrics

报告必须同时显示：

- unhedged long return
- beta hedge return
- hedged net return
- max drawdown unhedged vs hedged
- daily Sharpe unhedged vs hedged
- beta residual
- VaR proxy
- basis risk warning
- contribution by sleeve
- contribution by market
- contribution by theme/sector

如果 hedged 后 alpha 消失，说明原来的收益主要是 beta；如果 hedged 后仍有正 LCB，才是真正可执行 alpha。

### Implementation hooks

Hedge ledger 先于新 sleeve 实现，直接使用现有 sleeves 验证符号、beta floor、benchmark 选择和聚合一致性。不要等 CN tape / US theme 都写完后再叠 hedge，否则 bug 会混在新信号和新 hedge 两层里。

共享 hedge 模块：

- 从 `scripts/run_main_strategy_v2_backtest.py` 抽取当前 `_select_beta_hedge`、beta floor、hedge ratio、benchmark selection 到 `scripts/lib/hedge.py`。
- 当前日 `build_portfolio_risk_overlay()` 和历史 `portfolio_hedge.py` 都调用同一个 hedge selector。
- 抽取前后当前日 overlay 输出必须一致。

新增函数放在 `scripts/sleeves/portfolio_hedge.py`：

- `build_portfolio_hedged_backtest(sleeves, us_db, cn_db, start, as_of)`
- `load_cn_hedge_returns(cn_db, start, as_of)`
- `load_us_hedge_returns(us_db, start, as_of)`
- `estimate_rolling_beta(long_returns, hedge_returns, lookback=60)`；如现有实现可抽取则复用，不新增漂移版本
- `write_portfolio_hedged_tables(...)`

渲染接入：

- `scripts/run_alpha_sleeve_backtest.py` 的 markdown summary 增加 hedged portfolio table。
- `scripts/run_main_strategy_v2_backtest.py` 读取历史 hedged evidence，把当前日 R 计划和历史净 alpha 证据连起来。
- `portfolio_risk_overlay.md` 保留当前日计划，但必须引用历史净 hedge PnL。

## Promotion Contract Enforcement

“不允许 agent 越权升级”必须落到代码，不只写在 prompt。

新增 DuckDB 表：`promoted_sleeves`

| Column | Meaning |
| --- | --- |
| `sleeve_id` | sleeve id |
| `market` | cn/us |
| `promoted_at` | 晋级时间 |
| `effective_start` | 可用于生产的起始日期 |
| `status` | `promoted` / `watch` / `retired` |
| `gates_snapshot_json` | 晋级时 metrics、coverage、correlation、hedged PnL 快照 |
| `created_by` | `alpha_sleeve_backtest` / manual review |

运行时硬约束：

- `summarize_us()` / `summarize_cn()` 在设置 `give_r=true` 或 production trade tier 前，必须断言 `alpha_sleeve_id` 在 `promoted_sleeves` 且 `status='promoted'`。
- 未晋级 sleeve 只能进入 watch/research/radar，不能给 R。
- 如果代码路径尝试给未晋级 sleeve R，直接 raise，不能静默降级。
- prompt 只能引用 promoted sleeve contract；agent 文本不能绕过 `promoted_sleeves` 表。

这张表是报告层和 agent 层之间的不可绕过边界。后续任何新 sleeve 都必须先写历史回测、hedged PnL、晋级快照，再进入这张表。

## Implementation Phases

### Phase 0: Plan and data audit

Status: done by this document.

Deliverables:

- 数据覆盖清单。
- 代码接入点。
- sleeve 定义和晋级 gates。
- hedged PnL ledger schema。

### Phase 0.5: Zero-behavior module split

Owner: Project Consolidation Phase 4. This section is retained as a dependency record, not a second owner.

Status 2026-05-09: complete for `scripts/run_alpha_sleeve_backtest.py`.

Deliverables:

- `scripts/sleeves/base.py`
- `scripts/sleeves/__init__.py`
- existing sleeves moved into market/domain modules
- `scripts/run_alpha_sleeve_backtest.py` reduced to orchestration/render/write

Acceptance:

- 拆分前后 `alpha_sleeve_metrics`、`alpha_sleeve_daily_returns`、`alpha_sleeve_correlation` 完全一致。
- 当前日 portfolio overlay 输出不变。
- 没有新增 sleeve，没有改变任何 gate。

Verification:

- Baseline: `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01 --output-root /tmp/alpha_sleeve_split_baseline`
- After split: `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01 --output-root /tmp/alpha_sleeve_split_after`
- Markdown output identical.
- JSON identical except `generated_at`.
- DuckDB tables `alpha_sleeve_metrics`, `alpha_sleeve_correlation`, `alpha_sleeve_daily_returns` identical.

### Phase 0.6: TDD guardrails first

Status 2026-05-09: complete for the initial red-green guardrails.

这些测试先写，允许先红，再实现到绿。

Required red-green tests:

- look-ahead：构造 `news_publish_ts > signal_close_ts` 的 A股新闻，断言它不进入 candidate score。
- hedge sign：合成 `long_ret_pct=+5%`、`benchmark_ret=+5%`、`beta=1.0`、`hedge_ratio=0.5`，断言 net = +2.5%；反向跌市断言 hedge 后 net 优于 unhedged long。
- single-name fake theme：合成一个 theme 只有一只票上涨，断言 theme basket 不晋级。
- ledger consistency：`sum(portfolio_hedge_ledger.net_r by date) == portfolio_hedged_backtest.net_return_r`。
- promotion contract：未写入 `promoted_sleeves` 的 sleeve 如果尝试 `give_r=true`，断言 raise。

### Phase 1: Historical hedge ledger using existing sleeves

Status 2026-05-09: complete for existing money/stock-trade sleeves.

先用现有 sleeves 验证 hedge ledger，不等待新 sleeve。

Deliverables:

- `portfolio_hedged_backtest`
- `portfolio_hedge_ledger`
- current hedge selector extracted and reused
- unhedged vs hedged metrics for existing sleeves
- sleeve/market contribution and basis risk diagnostics

Acceptance:

- 报告可回答：
  - 现有 long alpha gross return 是多少？
  - beta hedge 吃掉/贡献多少？
  - hedge 后净 alpha LCB 是否大于 0？
  - 哪个 sleeve 是收益来源，哪个 sleeve 是风险来源？
- Hedge sign tests pass。
- Ledger 聚合一致性 tests pass。

Verification:

- `python3 -m unittest tests/test_phase_0_6_guardrails.py`
- `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01 --output-root /tmp/alpha_sleeve_hedge_20260509`
- DuckDB output includes `portfolio_hedge_ledger` and `portfolio_hedged_backtest`.
- `sum(portfolio_hedge_ledger.net_return_r)` matches global `portfolio_hedged_backtest.net_return_r`.

### Phase 2: A股 tape feature persistence + research sleeve

Deliverables:

- Calibration run first: emit sub-bucket distribution without hard gates, then set `n`, active-day, hit-rate, and LCB gates from observed sample quality.
- Rust 持久化 `cn_tape_features`。
- `scripts/sleeves/cn_tape_leadership.py`
- `cn_tape_leadership_continuation` in alpha sleeve backtest
- coverage-aware metrics：`full_history` / `has_sector_confirm` / `proxy_confirm`
- 子桶报告：
  - breakout leader
  - re-acceleration leader
  - sector leader
  - lukewarm rejected
  - late/chase rejected

Acceptance:

- 运行 `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01`
- 看到 `cn_tape_leadership_continuation` metrics。
- 看到 `n_with_full_confirm` / `n_with_proxy_confirm` / `n_missing_confirm`。
- 如果 LCB 不过，不进生产 R；但必须显示坏在什么子桶。
- CN tape 与 CN oversold 整族相关性检查通过。

### Phase 3: 美股 theme seed YAML + research sleeve

Deliverables:

- Calibration run first: emit theme/breadth/options-confirm distribution without hard gates, then set promotion gates from observed sample quality.
- `data/us_theme_seed_map.yaml`
- `scripts/sleeves/us_theme_cluster.py`
- `us_theme_cluster_momentum` basket return labels
- theme-level metrics、concentration diagnostics、coverage-aware options confirm metrics

Acceptance:

- 运行 `python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01`
- 看到 `us_theme_cluster_momentum` metrics。
- 能解释 CRCL/COIN、NVTS/GFS、RKLB/RGTI 等当前主题名为什么过去能或不能给 R。
- single-name fake theme test pass。
- 与 US existing sleeves 的相关性检查通过。

### Phase 4: New sleeves into verified hedge ledger

Deliverables:

- `cn_tape_leadership_continuation` hedged metrics
- `us_theme_cluster_momentum` hedged metrics
- per-sleeve hedge benchmark choice and beta diagnostics

Acceptance:

- 新 sleeve 的 unhedged 和 hedged PnL 同时可见。
- 如果 hedge 后 LCB 变负，不能晋级生产 R。
- 如果 hedge 后仍有正 residual alpha，才允许进入 promotion review。

### Phase 5: Production promotion wiring

Only after Phase 1/2/3/4 pass gates.

Deliverables:

- `promoted_sleeves` 表。
- `alpha_sleeve_backtest` owns writes into `promoted_sleeves` when a sleeve passes promotion gates; manual review can override only by inserting a row with `created_by='manual_review'` and full `gates_snapshot_json`.
- `summarize_cn()` 接入 `cn_tape_leadership_continuation`，但给 R 前必须查 `promoted_sleeves`。
- `summarize_us()` 接入 `us_theme_cluster_momentum`，但给 R 前必须查 `promoted_sleeves`。
- `run_main_strategy_v2_backtest.py` must fail fast at startup if a current production R path lacks a promoted sleeve contract.
- CN/US opportunity ranker 允许 promoted sleeve 给 R。
- prompts 更新为引用 sleeve contract，而不是让 agent 自己发明交易理由。

Acceptance:

- 当前报告里候选不会因为新闻或旧 HIGH/MOD 自动升级。
- 只有历史 sleeve 晋级、写入 `promoted_sleeves`、且当前信号满足同一规则，才给 R。
- 未晋级 sleeve 尝试给 R 会 raise。

### Phase 6: Full validation

Required tests:

- A股 tape feature 不允许使用未来数据。
- A股新闻后验标签不影响 candidate score。
- A股 lukewarm momentum 被过滤。
- 美股 theme basket 不允许 single-name event 伪装成 theme。
- 美股 options/flow missing 不得伪装成 confirmed。
- Hedged PnL 符号正确：market up 时 short beta hedge 应减少 net return，market down 时应保护 net return。
- Ledger row count 和 daily return 聚合一致。
- Promotion contract 不能被 prompt 或 report code 绕过。

Validation commands:

```bash
python3 -m py_compile scripts/run_alpha_sleeve_backtest.py scripts/run_main_strategy_v2_backtest.py
python3 -m unittest quant-research-v1/tests/test_cn_opportunity_ranker.py
python3 -m unittest quant-research-v1/tests/test_main_strategy_v2_backtest.py
python3 scripts/run_alpha_sleeve_backtest.py --start 2026-03-01
python3 scripts/run_main_strategy_v2_backtest.py --start 2026-03-01
```

## Non-Negotiable Rules

- 不新增一个漂亮报告板块来假装解决问题；必须新增历史可验证 sleeve。
- 不用 probe/tiny/small 作为逃避真实风险的语言。研究层可以不交易，但生产层必须说明给多少 R、为什么给、历史 hedge 后净收益如何。
- A股新闻是滞后标签和风险否决，不是主买入信号。
- 美股 options 是辅助决策，不是当前股票 sleeve 的交易本体；真实 bid/ask option PnL ledger 另行作为诊断/验证。
- 任何 fresh entry 必须能从历史 sleeve 晋级链路追溯，并且 `alpha_sleeve_id` 必须存在于 `promoted_sleeves`；不允许 agent 靠一句主观判断越权升级。
- 所有历史回测必须扣成本、检查 top PnL concentration、检查 beta residual、检查 drawdown。
- Hedge selector 只有一份共享实现，当前日 overlay 和历史 ledger 必须共用。
- Seed map 是数据，不是代码；US theme membership 不硬编码到 Python 函数里。
- A股 tape 派生特征是持久化表，不在 Python 回测里临时重算另一套逻辑。

## First Implementation Order

实际开发顺序应该是：

1. 先完成 Project Consolidation Phase 0-4：ops 收口、review packet、risk path map、`scripts/sleeves/` 零行为拆分。
2. 先写 TDD guardrail：look-ahead、hedge sign、single-name fake theme、ledger consistency、promotion contract。
3. 用现有 sleeves 先做 `portfolio_hedged_backtest` / `portfolio_hedge_ledger`，此时抽出 `scripts/lib/hedge.py` 并验证 hedge 数学和符号。
4. 先跑 CN/US calibration，不带硬 gate 看分布。
5. Rust 侧回填 `cn_tape_features`，Python 做 `cn_tape_leadership_continuation` research sleeve。
6. 落 `data/us_theme_seed_map.yaml`，Python 做 `us_theme_cluster_momentum` research sleeve。
7. 把两个新 sleeve 接入已验证的 hedge ledger，看 hedged LCB。
8. 只有新 sleeve 的 hedged LCB 过线并写入 `promoted_sleeves`，再改 `run_main_strategy_v2_backtest.py` 和 CN/US ranker 给生产 R。

这样顺序的原因很简单：先让工程边界和 hedge ledger 可靠，再证明新 sleeve 有 hedge 后净 alpha，最后才让报告给 R。否则还是把交易直觉写进 prompt，但系统没有真正学会执行和风控。
