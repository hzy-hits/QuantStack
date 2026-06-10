# CLAUDE.md — Quant Stack

进仓库先读(顺序):

1. `docs/ARCHITECTURE.md` — 现状唯一权威:什么在跑、在哪跑、数据层、narrator 叙事层、已知坑、操作速查。
2. `AGENTS.md` — 目标、硬边界(AI universe 准入、证据门)、执行规则。
3. 改代码前查 `docs/MODULE_BOUNDARIES.md` 和两份合同(`docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md`、`docs/REPORT_DELIVERY_CONTRACT.md`)。

完整文档索引在根 README 的 Operating Docs 段;已完成的历史计划在 `docs/archive/`,**不反映现状**。

## 运行事实(速记)

- 所有定时任务在 `ops/tasks.yaml` 注册,`ops/run_task.sh <task_id>` 执行;crontab 由 `ops/render_cron.py` 生成,不要手改 crontab。
- 美股日报链:`us.premarket`(20:00)/ `us.postmarket`(05:00)+ 12:25 `research.main_strategy_v2_report`;A股:`cn.morning`(08:30)/ `cn.evening`(18:00),走 Rust `target/release/quant-stack`。
- 报告正文由 LLM narrator 生成(backend=codex CLI)。**codex 配额耗尽当天 US/CN 日报会断供,没有 fallback**;恢复后补:`python3 scripts/agents/run_us_narrator.py --date YYYY-MM-DD --overwrite`。
- `reports/` 和各 DuckDB 不进 git;DuckDB 单写者,手动跑任务可能与正在跑的 cron 撞文件锁。
- WSL2 cron 不补跑;`ops.catch_up` 每 15 分钟补当天漏跑的 research/factor/paper 任务。

## 改动纪律

- 凌乱 worktree 不直接推 main;发布走专门分支 + `ops/review_packet.sh` 生成审核包。
- 改 Rust:根 workspace `cargo build --release` 一次全编(只有 `factor-lab/rust-bootstrap` 独立)。
- 验证命令见 README 的 Verification 段和 `smoke-check.sh`。
- 改了管线入口或 agent 规则,同步更新 `AGENTS.md` / `docs/ARCHITECTURE.md`。
