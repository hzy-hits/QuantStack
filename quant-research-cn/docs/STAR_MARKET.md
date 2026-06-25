# 科创板 (STAR Market) Coverage

科创板(688)默认**不在** CN scan universe(历史上被 `is_tradable_a_share` 排除)。
本功能用 `universe.scan.star` 开关 opt-in 纳入。

## 启用
1. `config.yaml` 的 `universe.scan` 设 `star: true`。
2. **必须先补价格历史**(新标的默认只有 ~45 行,`n<60` 会被分析静默丢弃):
   `python3 scripts/backfill_cn_prices.py`
3. 跑一次 CN 流水线(test 模式)确认无 panic、688 名字进报告:
   `./target/release/quant-cn run`

启用后扫描 **科创50(000688.SH)** 成分;微观结构(±20% 涨跌幅)由
`src/analytics/rv.rs::price_limit_pct` 已正确处理(688 → 20.0),无需额外配置。

## AI-infra 篮子兜底
`ai_infra/data/global_universe_v2.jsonl` 含 24 个 688 名字。**不在科创50 成分里的**,
加入 `config.yaml` 的 `universe.watchlist`——`star: true` 后 watchlist 的 688 也会放行,
保证篮子持仓全覆盖,与科创50 成员无关。

## 验证
- universe 含 688 名字;`quant-cn run` 无 panic。
- CN 报告出现科创板标的,limit/vol 信号按 ±20% 计。
- 此前无 CN 分析的 ai_infra STAR 名字现在有 momentum/flow/regime 输出。

## 回滚
`universe.scan.star: false`(默认)→ 688 重新排除,回到现状。纯 config,无需重编。

## 后续(未实现)
科创100(`000698.SH`)留作第二个开关 `scan.kc100`,需要中盘广度时再加。
