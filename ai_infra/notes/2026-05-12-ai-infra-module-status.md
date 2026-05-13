# AI Infra 模块推进状态

日期：2026-05-12  
项目：ai super cycle

## 已完成第一批 Pro 输出抓取

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| HBM 结构性超级周期 | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md) |
| CoWoS / 2.5D / Advanced Packaging | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-cowos-advanced-packaging.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-cowos-advanced-packaging.md) |
| AI / HBM Testing / Metrology | 已抓取，待原文核验；有上下文污染 | [2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md) |

## 第二批已抓取

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| 800G / 1.6T / CPO / Silicon Photonics | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md) |
| Scale-up Fabric / Custom ASIC | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md) |
| 电力设备 / 液冷 / 热管理 | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-power-cooling-thermal.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-power-cooling-thermal.md) |

## 第三批已抓取

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| NeoCloud 经济模型 | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-neocloud-economics.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-neocloud-economics.md) |
| 非美材料 / 设备隐形冠军 | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-non-us-hidden-champions.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-non-us-hidden-champions.md) |
| 存储超级周期反证 | 已抓取，待原文核验 | [2026-05-12-chatgpt-pro-storage-supercycle-refutation.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-storage-supercycle-refutation.md) |

## A 股主板映射已抓取

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| A 股主板映射总图 v1 | 已抓取，待原文核验；需按 D0-D5 BFS 重整 | [2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md) |

## BFS 结论建模已抓取

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| LLM Dependency BFS 结论建模 | 已抓取，待整合入 v2 工作流 | [2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md) |

## 全球可交易候选池已落库

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| AI Infra 全球可交易候选池深度研究 v2 | 已用本地 JSONL 落库；146 条记录；待原文核验 | [global_universe_v2.jsonl](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/data/global_universe_v2.jsonl) |
| SQLite 研究库 | 已生成；companies / dependency_edges / research_signals / scores 均为 146 行 | [ai_infra_universe.sqlite](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/data/ai_infra_universe.sqlite) |
| 静态仪表盘 | 已生成；包含资产池、BFS、分池、D2-D3、D4-D5、排除池和核验优先级 | [ai_infra_universe_dashboard_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/ai_infra_universe_dashboard_v1.md) |
| 构建脚本 | 已生成；可从 JSONL 重建 SQLite、CSV、dashboard | [build_universe_system.py](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/scripts/build_universe_system.py) |

## 美股 alpha 挖掘已启动

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| US alpha mining queue v1 | 已生成；30 条；P0_us_alpha 12 条，P1_verify 9 条，P2_large_cap_context 9 条 | [us_alpha_mining_queue_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/us_alpha_mining_queue_v1.md) |
| US alpha mining CSV | 已生成；用于筛选和后续落库 | [us_alpha_mining_queue_v1.csv](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/us_alpha_mining_queue_v1.csv) |
| US alpha evidence card 草稿 | 已生成；已有 batch1 卡片的不重复生成 | [evidence/us_alpha](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/evidence/us_alpha) |
| COHR 样板卡 | 已补 Q3 FY2026 release、SEC 10-Q、investor presentation，保持候选/核心候选；待核验 AI-only mix、客户集中、commitment 条款和 FCF 转化 | [014-COHR-Coherent.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/evidence/batch1/014-COHR-Coherent.md) |
| US alpha mining status | 已生成当前状态和下一步顺序 | [us_alpha_mining_status_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/us_alpha_mining_status_v1.md) |

## 基金哲学与 GitHub 仓库化

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| Fund philosophy | 已抽象成本项目稳定方法论；定义 D0-D5 BFS fund philosophy、证据层、反证层和组合层 | [fund-management-philosophy.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/fund-management-philosophy.md) |
| GitHub repo operating model | 已写 private repo 推荐、提交范围、`.gitignore`、首次建仓库、新电脑恢复命令和数据安全规则 | [github-repo-operating-model.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/github-repo-operating-model.md) |
| ChatGPT Pro fund philosophy review | 已收到输出并落盘；核心结论是 `source-backed AI Infra research OS`，缺口是基金工程、数据契约、组合与风险闭环 | [2026-05-12-chatgpt-pro-fund-philosophy-review.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-fund-philosophy-review.md), [fund_philosophy_pro_delta_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/fund_philosophy_pro_delta_v1.md) |
| Repo engineering P0 | 已新增 disclaimer、Makefile、public/private boundary、data security rules、private leak test | [DISCLAIMER.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/DISCLAIMER.md), [Makefile](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/Makefile), [test_no_private_data_leak.py](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/tests/test_no_private_data_leak.py) |

## 原文核验队列已生成

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| 全量 source verification queue v1 | 已生成；146 条；P0/P1/P2/P3/P4 分层；仍全部为 pending original-source verification | [source_verification_queue_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/source_verification_queue_v1.md) |
| 全量 CSV | 已生成；用于后续脚本化补原文、证据状态和核验结论 | [source_verification_queue_v1.csv](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/source_verification_queue_v1.csv) |
| Batch 1 | 已生成；24 家，每个资产池各 8 家；用于第一轮 evidence card | [source_verification_batch1.csv](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/source_verification_batch1.csv) |
| 核验队列脚本 | 已生成；从 SQLite 重建 queue 和 batch1 | [generate_source_verification_queue.py](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/scripts/generate_source_verification_queue.py) |
| Batch 1 evidence cards | 已生成；24 张草稿卡，等待逐家公司补原文链接和证据 | [evidence/batch1](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/evidence/batch1) |
| Evidence card 脚本 | 已生成；从 batch1 CSV 重建卡片草稿 | [scaffold_evidence_cards.py](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/scripts/scaffold_evidence_cards.py) |

## BFS 供应链扩展与 agent pipeline 已启动

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| BFS supply-chain discovery queue v1 | 已生成；146 条 seed task；用于从现有公司向上游/下游/peer supplier 扩展美国、日韩台欧以色列候选 | [bfs_supply_chain_discovery_queue_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/reports/bfs_supply_chain_discovery_queue_v1.md), [bfs_supply_chain_discovery_queue_v1.csv](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/reports/bfs_supply_chain_discovery_queue_v1.csv) |
| BFS supply-chain discovery script | 已新增；从 `data/global_universe_v2.jsonl` 生成 discovery queue 和 ChatGPT Pro prompt | [generate_bfs_supply_chain_discovery_queue.py](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/scripts/generate_bfs_supply_chain_discovery_queue.py) |
| ChatGPT Pro BFS supply-chain discovery prompt | 已提交 Pro；等待输出完成后抓取落盘 | [2026-05-13-chatgpt-pro-bfs-supply-chain-discovery-prompt.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/notes/2026-05-13-chatgpt-pro-bfs-supply-chain-discovery-prompt.md) |
| HBM BFS source-backed discovery v2 | 已抓取，待原文核验；约 43k 字符，包含 source checklist、HBM bottleneck chain、候选/radar 表、agent pipeline 和搜索 query 模板 | [2026-05-13-chatgpt-pro-hbm-bfs-source-backed-v2.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/notes/2026-05-13-chatgpt-pro-hbm-bfs-source-backed-v2.md), [prompt](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/notes/2026-05-13-chatgpt-pro-hbm-bfs-source-backed-v2-prompt.md) |

## 公司财报 / K线 / 期权研究方法已提交

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| Company financials + K-line + options methodology | 已提交 Pro；等待输出完成后抓取落盘 | [2026-05-13-chatgpt-pro-company-financials-market-options-methodology-prompt.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/ai_infra/notes/2026-05-13-chatgpt-pro-company-financials-market-options-methodology-prompt.md) |

## D5 反证雷达已补充

| 模块 | 状态 | 本地文件 |
| --- | --- | --- |
| CDS / Credit Risk Radar | 已生成；覆盖 NeoCloud、IDC、REIT、信用代理和债务/租赁/利息/合同质量指标 | [credit_cds_radar_queue_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/credit_cds_radar_queue_v1.md) |
| Nuclear / Firm Power Radar | 已生成；覆盖核电运营商、uranium/HALEU、SMR、燃气轮机、电网和核能 ETF proxy | [nuclear_firm_power_queue_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/nuclear_firm_power_queue_v1.md) |
| ChatGPT Pro Credit / Nuclear 输出 | 已收到用户粘贴结果并整理为待核验线索；已形成 delta 判断 | [2026-05-12-chatgpt-pro-credit-cds-nuclear-radar.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-credit-cds-nuclear-radar.md), [credit_nuclear_pro_delta_v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/reports/credit_nuclear_pro_delta_v1.md) |
| 专用 evidence card 模板 | 已补 credit / financing 与 firm power 两类模板，避免和普通半导体证据卡混用字段 | [credit-financing-evidence-card-template.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/credit-financing-evidence-card-template.md), [firm-power-evidence-card-template.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/firm-power-evidence-card-template.md) |

## 尚未启动

| 模块 | 建议启动顺序 | 备注 |
| --- | --- | --- |
| AI Infra 总反证仪表盘 | 第四批 | 等各方向结果回来后再汇总，避免过早抽象 |

## 当前优先级判断

短期优先级已经从“等待 Pro 输出”切到“Batch 1 原文核验 + 少量 credit / firm power 样板卡”。24 张 evidence card 草稿已经建好；下一步逐家公司补最新年报/季报/投资者材料/官网技术资料，并把每条事实归类为原文已证明、合理推论、待核验或反证。Credit / nuclear 不作为第三主线扩张，而是先用 CRWV/ORCL/APLD/IREN/WULF 和 CEG/TLN/GEV/Siemens Energy/MHI 验证 evidence card 字段。完成后再接入 ETF holdings、免费价格数据、SEC 13F/N-PORT 作为第二阶段资金和风险信号。
