# 科创板 (STAR Market) Coverage — Design Spec

Date: 2026-06-25 · 状态:待评审 · 作者:operator + Claude

## 目标

让**科创板(688)**名字进入 CN 概率流水线 + 日报,使 AI-infra 篮子里已有的科创板持仓拿到完整 CN 分析,并扫描科创50 头部。config 门控、opt-in、可逆。

## 范围

**做**:放开 688 排除(config 门控)、加科创50 扫描、篮子名字经 watchlist 兜底、给新标的补价格历史。
**不做**:科创100(预留 `scan.kc100` 开关,本次不实现)、任何分析模块重写(微观结构已板块感知)、ai_infra→CN universe 的全量合并(超范围)。

## 现状(已勘查,带真实位置)

- **主闸门**:`quant-research-cn/src/fetcher/tushare/universe.rs:9` `fn is_tradable_a_share(code) -> bool { !code.starts_with("688") }`,在 index 扫描(:42)和 watchlist(:56)两处把 688 挡掉。
- **第二道过滤**:`filtering/notable.rs::load_candidates`(约 :1956)的 SQL WHERE 子句 `NOT LIKE '688%'` 也独立排除了 688。该过滤在 `scan.star` 实现后已加门控(star=true 时 WHERE 片段省略);详见下方 2026-06-25 修订说明。
- **ScanConfig**(`src/config.rs:65-68`):`csi300 / csi500 / csi1000 / sse50`,无科创板。
- **微观结构已处理**:`src/analytics/rv.rs::price_limit_pct(ts_code, name)` 对 688 返回 **20.0**(±20%),并有单测 `test_price_limit_rules`(`assert_eq!(price_limit_pct("688001.SH", …), 20.0)`)。波动 censoring(`infer_censor_side` / tobit)与 `limit_move_radar` 都用它。**无需改。**
- **北向**已对所有 CN 股票移除(`flow.rs`:Tushare 返回全 NULL),非 STAR 特有。flow 现为 margin 驱动。
- **ai_infra/global_universe** 已含 **23 个 688 名字**(688008/688012/…);它们出现在 main_strategy_v2 报告层,但**不在 CN 分析 universe**(=index 扫描 + watchlist,二者都排 688)→ 即无 momentum/flow/regime/limit 等 CN 分析背书。
- **补价格脚本现成**:`scripts/backfill_cn_prices.py`(新标的默认只有 ~45 行,需补)。

## 设计

### 组件 1 — 配置开关(`src/config.rs` + `config.example.yaml`)
- `ScanConfig` 新增 `pub star: bool`(serde 默认 `false`)。
- `config.example.yaml` 的 `universe.scan` 加 `star: false   # 科创板(688),±20% 涨跌幅`。
- 预留(不实现):`kc100`(科创100,000698.SH)留作后续一行开关。

### 组件 2 — 放开闸门(`src/fetcher/tushare/universe.rs`)
- `is_tradable_a_share` 改为接受开关:
  ```rust
  fn is_tradable_a_share(code: &str, allow_star: bool) -> bool {
      allow_star || !code.starts_with("688")
  }
  ```
- 两处调用(:42 index 扫描、:56 watchlist)传 `cfg.universe.scan.star`。
- **门控语义**:`star=false`(默认)→ 行为与现状完全一致(688 排除);`star=true` → 688 放行。

### 组件 3 — 科创50 扫描(同文件 indices 数组)
- indices 数组加一项 `(cfg.universe.scan.star, "000688.SH")`(科创50 指数代码)。仅当 `star=true` 拉其 `index_weight` 成分。

### 组件 4 — 篮子兜底(文档 + 操作,无新代码)
- 闸门放开后,watchlist 里的 688 名字即可进 universe。**ai_infra 的 24 个科创板名字中,不在科创50 成分里的,操作员加入 `config.yaml` 的 `universe.watchlist`** → 保证篮子持仓全覆盖,与科创50 成员无关。

### 组件 5 — 补价格历史(操作步骤)
- 启用 `star` 后,新纳入的 STAR 标的历史只有 ~45 行(`n<60` 会被分析静默丢弃)→ 跑 `python3 scripts/backfill_cn_prices.py` 补齐,再跑首次真实日报。

### 不动
涨跌幅(已 ±20%)、波动 censoring、flow、regime、证据门、报告渲染、limit_up_model / limit_move_radar —— 全部沿用,STAR 名字像其它 CN 股票一样流过。

## 测试

- **Rust 单测**(`universe.rs` 内 `#[cfg(test)]`):
  - `is_tradable_a_share("688981", false) == false`;`("688981", true) == true`;主板/创业板码(`600519`、`300750`)在两种开关下都 `== true`。
- `price_limit_pct` 已有 `test_price_limit_rules`(688=20.0)——本设计不动它,但 spec 引用它作为"微观结构已覆盖"的证据。

## 验证(启用后,test 模式)

1. `config.yaml` 设 `universe.scan.star: true`;`./target/release/quant-cn run`(或 cn.morning dry/test)。
2. 确认 universe 含 688 名字、跑完无 panic;`scripts/backfill_cn_prices.py` 已补新标的。
3. CN 报告出现科创板标的,其 limit/vol 信号按 ±20% 计(对比 `price_limit_pct`)。
4. ai_infra 的 24 个 STAR 名字在 CN 分析里有 momentum/flow/regime 输出(此前没有)。

## 回滚

`universe.scan.star: false`(默认)→ 688 重新排除,行为回到现状。纯 config,无需改码/重编。

## 风险

- **新 STAR 标的无 backfill → 历史太薄被丢**:启用后首跑前必须 `backfill_cn_prices.py`。
- **标的增多 → CN 流水线变长 + Tushare 调用增**:科创50 ~50 个,温和;Tushare 500ms 限速下多 ~25s。
- **科创50 成员漂移**:某 ai_infra STAR 名字不在科创50 → 不被扫到,除非进 watchlist(组件 4 兜底)。
- **科创板停牌/新股**:`index_weight` 可能含极新上市标的,价格历史极薄 → 同 backfill 缓解;`n<60` 静默丢弃是既有行为。

## 已知边界(主板专属通道,设计保留,不受 scan.star 门控)

`scan.star: true` 后 688 名字进入主候选/notable 路径和日报,但以下两处**仍按设计排除 688**:

- `notable.rs::is_mainboard_exploration_symbol`(约 :1046):超卖探索通道,仅针对主板,科创板不在范围。
- `shadow_option_alpha_calibration.rs:270` `NOT LIKE '688%'`:Shadow-option-alpha 校准通道,科创板无对应期权合约,主板专属。

## 修订说明(2026-06-25)

原设计文档"别处无再排除 688"不准确。`filtering/notable.rs::load_candidates` 的 SQL WHERE 子句`AND lp.ts_code NOT LIKE '688%'` 是独立的第二道过滤,在主闸门之外将 688 名字从候选池中丢弃。该行为导致即使 `scan.star: true` 放开了 universe 闸门,688 名字也无法进入 notable 报告。

修复内容:为 `load_candidates` 增加 `scan_star: bool` 参数,当 `scan_star=true` 时省略 WHERE 片段;当 `false`(默认)时行为与修复前完全一致。`prepare_candidates` 传 `cfg.universe.scan.star`。两条主板专属通道(见上方"已知边界")保持不变。

## 自查

- 范围聚焦单一功能(STAR 纳入),不重写分析;与 portability/agent-native spec 不重叠。
- 命名一致:`is_tradable_a_share(code, allow_star)`、`scan.star`、科创50=`000688.SH`。
- 默认 `false` → 零行为变更直到 opt-in;微观结构复用现成 `price_limit_pct`(已测)。
