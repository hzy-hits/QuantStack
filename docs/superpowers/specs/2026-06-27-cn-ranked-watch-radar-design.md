# CN 「0R Ranked Watch 雷达」— Design Spec

Date: 2026-06-27 · 状态:待评审 · 作者:operator + Claude

## 目标

让 CN 日报(早/晚盘邮件)里**看得见**那批 ranker 已经算出排名、但落在 0R `active_watch`
档(rank ~11-20,"prepare order but wait for price")的名字——尤其是**科创板**——
而**不改任何分档 / 执行 / 证据门 / money 逻辑**。即:自然晕位 + 0R 可见。

触发背景:科创板(688)已在 Rust universe 闸门(`scan.star`)与 Python ranker
(`enforce_expand`)全程跑通,但它们当天最高只排到 0R `active_watch` 档,而该档既不进
`可交易名单`(被 `build_portfolio_risk_overlay` 的 `state ∈ {Execution Alpha, Positive EV Setup}`
硬过滤),也不在任何被 CN 叙事器切片的章节里 → 邮件正文完全看不到。本设计补一个被叙事器
切片的可见层。

## 范围

**做**:新增一个通用「0R Ranked Watch 雷达」report section,展示所有 `production_tier ==
"active_watch"` 的 ranker 行(科创板作为其中一类**高亮标记**),并把该 section 的标题接入
CN 叙事器的结构切片表,使其内容进入邮件正文。

**不做**:
- 不改 `production_tier()` 分档逻辑、不改 `build_portfolio_risk_overlay` 执行硬过滤、
  不改证据门 `is_production_grade`、不改 R/size 分配。科创板真排进 top-10 → 自然进
  `可交易名单`(现有代码已支持,无 688 过滤);没进 → 只在本雷达 0R 可见。**零资金影响。**
- 不做 US 侧(US 报告结构独立,本次只动 CN `cn_daily.py`)。
- 不针对科创板单独建段(选 b:通用 0R 雷达,科创板高亮),避免单挑一个板。

## 现状(已勘查,带真实位置)

- `可交易名单` 由 `scripts/reports/cn_daily.py:80` 的 `m.market_action_table(actions)` 渲染,
  `actions = m.market_actions(payload, "CN")`(`cn_daily.py:42`),只含可执行行。
- 0R 那一档来自 ranker:`payload["cn_opportunity_ranker"]["all_rows"]`,字段含
  `production_tier`、`rank`、`rank_score`、`pct_chg`、`ret_5d`、`ev_lcb80_pct`、
  `size_hint`、`reason`、`name`、`symbol`、`industry`、`ai_infra_current_pool`。
- tier 取值(2026-06-26 实测 134 行):`bench_ranked=114 / active_watch=10 /
  secondary_stock_trade=5 / top_stock_trade=5`。`state` 全为 `"AI Infra Universe Watch"`
  → **过滤用 `production_tier`,不用 `state`**。
- 当日 `active_watch` 的 10 行里 2 个科创板:`688233.SH`(rank 18)、`688535.SH`(rank 20);
  其余 688 名 rank>20 落 `bench_ranked`。
- CN 叙事器 `scripts/agents/run_cn_narrator.py:48-50` 的 `_STRUCTURAL_HEADERS =
  ["概率最优", "可交易名单", "逐票复核", "左侧观察池", "CN Realized Horizon Edge", ...]`
  决定哪些章节被切进 quant payload → 进邮件。新段标题必须含其中一个 key,否则进不了邮件。
- `左侧观察池`(`scripts/sections/left_side.py:49-74`)只收 `oversold_contrarian` 家族,
  动量/题材类的 `active_watch` 不在其内 → 不能复用。

## 设计

### 组件 1 — 新 renderer `render_cn_ranked_watch_radar_section(payload)`
- 新文件 `scripts/sections/cn_ranked_watch.py`(与现有 `sections/*.py` 同构,纯渲染、无副作用)。
- 读 `payload.get("cn_opportunity_ranker", {}).get("all_rows", [])`。
- **过滤**:`str(row.get("production_tier")) == "active_watch"`。(只这一档;`bench_ranked`
  是基准填充、`*_trade` 已进可交易名单,都不收。)
- **排序**:`rank` 升序(None 排末尾)。
- **板标记**:由 symbol 前缀推断 `board(symbol)`:`688→科创板`、`300/301→创业板`、
  `8/4/920→北交所`、其余→`主板`。科创板行**加视觉标记**(板列前缀 `★` 或加粗),其它板正常。
- **展示列**:`Rank | Symbol | Name | 板 | Score | 1D | 5D | EV LCB80 | Size | Reason`,
  其中 1D=`pct_chg`、5D=`ret_5d`、Size=`size_hint`(应为 0R)、Reason=`reason`(截断 60)。
- **显示上限**:固定 `RADAR_LIMIT = 20`(active_watch 当日约 10 行,留余量;无需随 regime)。
- **空池占位**:`"今天没有 active_watch 0R 候选(名字要么进了可交易名单,要么落到 bench)。"`。
- 标题行固定 `## 0R 观察雷达 (Ranked Watch)`,后跟一句说明:
  `"这一档是 ranker 已排名、但未达执行线的 0R 候选(prepare but wait);★ 为科创板。不占资金,仅观察。"`。

### 组件 2 — 接入 CN 报告主体
- `scripts/reports/cn_daily.py`:在 `render_market_selection_rationale(...)`(逐票复核,line 81)
  之后、`render_cn_left_side_watch_section`(line 82)之前,插入
  `lines += render_cn_ranked_watch_radar_section(payload)`,并在文件头部 import。

### 组件 3 — 接入叙事器切片(让它进邮件正文)
- `scripts/agents/run_cn_narrator.py`:`_STRUCTURAL_HEADERS` 追加 `"0R 观察雷达"`(slice key,
  以 substring 命中 `## 0R 观察雷达 (Ranked Watch)`;与 `左侧观察池` 无 substring 冲突)。
- (可选,低风险)在 `quant` 提取器 / merge prompt 加一句轻提示:
  `"若结构 payload 里有 0R 观察雷达条目(尤其科创板 ★),在『今日交易清单』的观察部分点名,
  但不得升级成执行/做多。"` —— 仅影响叙事措辞,不影响数字。

## 数据流

```
cn_opportunity_ranker.all_rows
   └─(filter production_tier=="active_watch")→ render_cn_ranked_watch_radar_section
        └─ cn_daily.py 插入 "## 0R 观察雷达 (Ranked Watch)" 段
             └─ run_cn_narrator _STRUCTURAL_HEADERS 切片 → quant payload
                  └─ 叙事器写进邮件「今日交易清单/观察」或「观察与风险」
```

## 测试

- **单测**(`tests/` 下,pytest;`render_cn_ranked_watch_radar_section`):
  - 假 payload 含 4 行:`688233`(active_watch)、`600519`(active_watch 主板)、
    `688019`(bench_ranked)、`600000`(top_stock_trade)。断言:
    - 输出恰含前 2 行(active_watch),`688019`/`600000` 不出现;
    - `688233` 行板列带 `★`/科创板标记,`600519` 不带;
    - 表头与说明句存在;按 rank 升序。
  - 空 active_watch → 出占位句,不抛异常。
- **切片回归**:构造含 `## 0R 观察雷达 (Ranked Watch)` 的 md,断言
  `_slice_md_sections(md, _STRUCTURAL_HEADERS)` 命中该段且不误吞 `左侧观察池`。
- **冒烟**:`python3 scripts/generate_main_strategy_v2_report.py --date 2026-06-26
  --ai-infra-mode enforce_expand`(test 模式,不发邮件),断言生成的 `cn_daily_report.md`
  含 `## 0R 观察雷达` 且其中出现 `688233`、`688535`。

## 回滚

删掉 `cn_daily.py` 的一行插入 + `_STRUCTURAL_HEADERS` 的一个 key(+ 可选 prompt 句)即回到现状;
新文件 `sections/cn_ranked_watch.py` 留着无副作用。纯展示层,无数据/分档改动。

## 风险

- **active_watch 偶发为空**:占位句已处理;连续多日空说明 ranker 没产出 active_watch,
  与本段无关(查 ranker)。
- **叙事器仍可能不点名科创板**:切片保证数据进 payload,但最终措辞由叙事器定;可选 prompt
  句提高命中率,但不强制(守"叙事自由"原则)。若必须保证逐字出现,那是另一处更强约束的改动,
  本设计不做。
- **与 `只观察或不碰` 轻微重叠**:两段定位不同(0R 雷达=ranker 已排名的 0R 候选;
  只观察或不碰=回避项),允许并存,不做去重。

## 自查

- 单一职责:一个纯渲染 section + 一处插入 + 一个切片 key。不碰分档/执行/证据门。
- 命名一致:`production_tier=="active_watch"`、`render_cn_ranked_watch_radar_section`、
  slice key `"0R 观察雷达"`、标题 `## 0R 观察雷达 (Ranked Watch)` 全文一致。
- 范围聚焦,与 STAR coverage spec(universe 闸门)不重叠——那是"让 688 进得来",
  本设计是"让已进来但 0R 的名字看得见"。
