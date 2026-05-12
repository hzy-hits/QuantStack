# data

这里放本地研究数据层。公开仓库只保留 schema/样例和说明；完整 universe、SQLite、行情、券商、组合和私有研究数据不进 public repo。

| 文件 | 用途 |
| --- | --- |
| [seed/global_universe_sample.jsonl](seed/global_universe_sample.jsonl) | 可公开的最小样例，用来验证脚本能跑通 |
| `global_universe_v2.jsonl` | 私有完整 universe seed，本地存在但不提交 |
| `ai_infra_universe.sqlite` | 本地 SQLite 数据库，构建产物，不提交 |

## SQLite 表

| 表 | 用途 |
| --- | --- |
| `companies` | ticker、market、asset_pool、company、mcap_bucket、bfs_depth、module、current_pool |
| `dependency_edges` | dependency_path、dependency_edge、overseas_bottleneck、up_downstream |
| `research_signals` | evidence_state、etf_clue、smart_money_clue、counterevidence、trading_reach、verification_status |
| `scores` | bfs_score、pool_score、evidence_score、edge_score、risk_penalty、total_score、score_bucket |

## 重建

```bash
python3 scripts/build_universe_system.py
```

如果本地存在 `data/global_universe_v2.jsonl`，脚本默认使用完整私有 universe；否则使用公开样例 `data/seed/global_universe_sample.jsonl`。

不要手工编辑 SQLite；修改输入或脚本后重建。
