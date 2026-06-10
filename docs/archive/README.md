# Docs Archive

已完成或被取代的计划/审计/交接文档。**这里的内容不再反映现状**,只作历史记录;
现状的唯一权威是 `docs/ARCHITECTURE.md`(运行时)与 `docs/MODULE_BOUNDARIES.md`(职责边界)。

归档于 2026-06-10(文档整合,见根 README 的 Operating Docs 索引)。

| 文件 | 原位置 | 状态 | 取代者 |
| --- | --- | --- | --- |
| `CLAUDE_HANDOFF.md` | 根 | 2026-05-13 时代的 agent 批次交接日志 | `AGENTS.md` + `docs/ARCHITECTURE.md` |
| `PHASE_D_PLAN.md` | 根 | 已完成:US narrator 已上线(codex 为主;2026-06-10 补上 DeepSeek 自动 fallback) | `scripts/agents/run_us_narrator.py` + `codex_backend.py` |
| `PROJECT_CONSOLIDATION_PLAN.md` | docs/ | Phase 0-4 完成;Phase 5(物理搬目录)明确搁置;`depends_on` 提案未实现 | `ops/`(tasks.yaml / run_task.py / review_packet.sh) |
| `AI_INFRA_SPECIALIST_PIPELINE_REORG.md` | docs/ | 已完成:AI-infra 专业户改造落地 | `docs/ARCHITECTURE.md` §1/§7 |
| `ALPHA_SLEEVE_ENGINEERING_PLAN.md` | docs/ | sleeve 模块建成(`scripts/sleeves/`),回测休眠 | `docs/ARCHITECTURE.md` §12 |
| `SIMPLIFICATION_AUDIT.md` | docs/ | 2026-05-08 点状审计,结论已消化 | `REFACTOR_PLAN.md`(进行中) |
| `REPORT_QUALITY_AUDIT.md` | docs/ | 2026-05-08 点状审计,早于 narrator 架构 | `docs/REPORT_DELIVERY_CONTRACT.md` |
| `LOOPHOLES_AND_FIXES.md` | docs/ | 修复清单,所列项均已实现 | —(历史记录) |

归档规则:完成或停滞超过一个月的 plan/audit 移入此目录,标题下加
`> **ARCHIVED <date>** — <状态/取代者>` 头;活文档不得引用归档文档作为现状依据。
