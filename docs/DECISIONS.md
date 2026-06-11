# Decisions Log

逆向时间序。每条:日期 / 决定 / 证据 / 复议条件。
活文档不得引用本文件之外的口头决定;改交易行为的开关翻转必须在这里留痕。

## 2026-06-10 — setup 进场闸门:仅保留为回测对照腿,不得接入生产 sizing

- **决定**: `scripts/score_entry_setup.py` 维持 research-only。生产执行表
  (main_strategy_v2)不使用"等回调再进"的 setup 闸门。
- **证据**: `ai_infra_strategy_backtest` 2024-06→2026-06:setup 闸门腿夏普
  US 1.99 vs 全篮子 2.13(回撤 -43.0% vs -31.3%),CN 1.57 vs 1.89——
  这批强趋势票等回调反而错过主升。
- **复议条件**: 仅当出现 regime 条件化变体(例如只在 WEDGE/PRESS 状态启用
  setup 闸门)且 A/B 回测夏普与回撤同时不劣于基线时,才重新评估。
