# reports

这里放由脚本生成或半自动整理的研究输出。它们是当前工作入口，但仍需要原文核验。

## 当前入口

| 文件 | 用途 |
| --- | --- |
| [ai_infra_universe_dashboard_v1.md](ai_infra_universe_dashboard_v1.md) | 146 条 universe 的静态仪表盘 |
| [universe_coverage_assessment_v1.md](universe_coverage_assessment_v1.md) | 当前公司池够不够全的覆盖评估 |
| [cds_nuclear_gap_radar_v1.md](cds_nuclear_gap_radar_v1.md) | CDS/信用风险和核电/firm power 的缺口雷达 |
| [credit_nuclear_pro_delta_v1.md](credit_nuclear_pro_delta_v1.md) | ChatGPT Pro credit / nuclear 输出的整合判断和执行顺序 |
| [fund_philosophy_pro_delta_v1.md](fund_philosophy_pro_delta_v1.md) | ChatGPT Pro 对基金哲学和 repo 工程的审稿 delta |
| [credit_cds_radar_queue_v1.md](credit_cds_radar_queue_v1.md) | Credit/CDS 可执行核验队列 |
| [credit_cds_radar_queue_v1.csv](credit_cds_radar_queue_v1.csv) | Credit/CDS 队列表 |
| [nuclear_firm_power_queue_v1.md](nuclear_firm_power_queue_v1.md) | Nuclear/firm power 可执行核验队列 |
| [nuclear_firm_power_queue_v1.csv](nuclear_firm_power_queue_v1.csv) | Nuclear/firm power 队列表 |
| [research_mvp_plan_v1.md](research_mvp_plan_v1.md) | 9 家公司研究 MVP 的范围和验收标准 |
| [research_mvp_status_v1.md](research_mvp_status_v1.md) | MVP 当前状态和样板卡经验 |
| [us_alpha_mining_queue_v1.md](us_alpha_mining_queue_v1.md) | 美股 D2-D3 / D3 alpha sleeve 挖掘队列 |
| [us_alpha_mining_queue_v1.csv](us_alpha_mining_queue_v1.csv) | 美股 alpha mining 可筛选表 |
| [us_alpha_mining_status_v1.md](us_alpha_mining_status_v1.md) | 美股 alpha 挖掘当前状态和下一步 |
| [source_verification_queue_v1.md](source_verification_queue_v1.md) | 原文核验任务队列和 Batch 1 |
| [source_verification_queue_v1.csv](source_verification_queue_v1.csv) | 146 条全量核验任务 |
| [source_verification_batch1.csv](source_verification_batch1.csv) | 第一轮 24 家核验名单 |

## CSV 分池

| 文件 | 用途 |
| --- | --- |
| [core_candidates.csv](core_candidates.csv) | 核心/高优先级研究候选 |
| [d2_d3_candidates.csv](d2_d3_candidates.csv) | D2-D3 高弹性候选 |
| [china_asset_pool.csv](china_asset_pool.csv) | 中国资产池 |
| [us_asset_pool.csv](us_asset_pool.csv) | 美国资产池 |
| [satellite_pool.csv](satellite_pool.csv) | 日韩台欧以色列卫星池 |
| [radar_and_excluded.csv](radar_and_excluded.csv) | 雷达和排除记录 |

## 使用规则

- 这里的评分是研究优先级评分，不是投资评分。
- 这里的候选池不是买入清单。
- 修改上游 JSONL 或评分逻辑后，重跑 `scripts/build_universe_system.py` 和 `scripts/generate_source_verification_queue.py`。
