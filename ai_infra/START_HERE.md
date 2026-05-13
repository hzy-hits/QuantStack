# AI Infra Project Workbench

状态：项目已从 ChatGPT Pro 草稿阶段进入本地 universe + 原文核验阶段。

边界：本项目是研究系统，不是投资建议、买卖建议、目标价或实际仓位建议。

公开仓库只包含方法论、脚本、模板和安全样例。完整 `notes/`、`reports/`、`evidence/`、SQLite、完整 universe 和组合相关内容默认在本地或 private repo。

## 当前要做什么

优先做 `Batch 1` 的公司原文核验。每家公司需要一张 evidence card，下一步是补公司原文、交易所公告、监管文件、官网技术资料或上下游交叉披露。

当前第一入口：

1. [docs/fund-management-philosophy.md](docs/fund-management-philosophy.md)
   看基金管理哲学和研究系统抽象。
2. [docs/llm-dependency-bfs-framework.md](docs/llm-dependency-bfs-framework.md)
   看 D0-D5 dependency BFS 分层。
3. [docs/research-checklist.md](docs/research-checklist.md)
   看公司/模块判断基线。
4. [docs/source-evidence-template.md](docs/source-evidence-template.md)
   看普通 company evidence card 模板。
5. [docs/company-financials-market-options-methodology.md](docs/company-financials-market-options-methodology.md)
   看公司财报、K线、期权三层研究方法；明确 K线/期权只能做市场和风险层，不能替代原文证据。
6. [docs/credit-financing-evidence-card-template.md](docs/credit-financing-evidence-card-template.md) 和 [docs/firm-power-evidence-card-template.md](docs/firm-power-evidence-card-template.md)
   看 Credit/CDS 与 Nuclear/Firm Power 两类横向证据卡。
7. [docs/github-repo-operating-model.md](docs/github-repo-operating-model.md)、[docs/public-private-boundary.md](docs/public-private-boundary.md)、[docs/data-security-rules.md](docs/data-security-rules.md)
   看 GitHub 仓库化和公开/私有边界。
8. [scripts](scripts) 和 [data/seed/global_universe_sample.jsonl](data/seed/global_universe_sample.jsonl)
   用公开样例验证本地 pipeline。

## 项目结构

| 目录 | 定位 | 读法 |
| --- | --- | --- |
| [docs](docs) | 稳定方法论和模板 | 优先读 BFS、checklist、evidence template |
| `notes` | ChatGPT Pro 输出、运行记录、阶段性整合 | private，本地保留，作为线索，不直接当证据 |
| [data](data) | 本地数据层 | public 只保留样例；完整 JSONL + SQLite 私有 |
| `reports` | 生成的研究仪表盘和 CSV | private，本地生成 |
| `evidence` | 原文证据卡 | private 或脱敏后公开 |
| [scripts](scripts) | 可复跑脚本 | 重建 universe、核验队列和 evidence cards |

## 核心规则

- 没有 BFS depth，不做公司结论。
- 没有原文出处，不写 `原文已证明`。
- `D1-D3` 是主战场；`D4-D5` 只做雷达，除非能证明反向卡住 `D0-D2`。
- ChatGPT Pro 输出只能作为候选和问题清单，不能作为事实依据。
- 所有记录默认 `pending_original_source_verification`，直到 evidence card 被原文补齐。
- Credit / financing 和 firm power 使用专门 evidence card，不混进普通半导体卡片字段。

## 可复跑命令

```bash
python3 scripts/build_universe_system.py
python3 scripts/generate_source_verification_queue.py
python3 scripts/scaffold_evidence_cards.py
python3 scripts/generate_us_alpha_mining_queue.py
```

说明：第三条默认不覆盖已存在的 evidence card，避免覆盖人工补充。

也可以直接用：

```bash
make rebuild
make verify
```

## GitHub 仓库化

完整研究库推荐使用 private repo。
详细方案见 [docs/github-repo-operating-model.md](docs/github-repo-operating-model.md)、[docs/public-private-boundary.md](docs/public-private-boundary.md)、[docs/data-security-rules.md](docs/data-security-rules.md)。

## Batch 1 资产池

| 资产池 | 数量 | 代表方向 |
| --- | ---: | --- |
| 中国资产池 | 8 | AI server、PCB/CCL、液冷、光互连、网络设备、封装设备 |
| 美国资产池 | 8 | GPU、custom ASIC、CPO/optics、power/thermal、EDA/IP |
| 卫星资产池 | 8 | HBM、TSMC/CoWoS、ABF substrate、ASIC design、先进封装设备 |

## 下一步

1. 先补 3-5 张 evidence card，验证模板是否好用。
2. 固化 evidence card 的 `原文已证明 / 合理推论 / 主要反证 / 当前动作`。
3. 再接 ETF holdings、免费价格数据、SEC 13F/N-PORT。
