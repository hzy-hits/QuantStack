# scripts

这里放可复跑脚本，全部使用 Python 标准库。

| 脚本 | 用途 |
| --- | --- |
| [build_universe_system.py](build_universe_system.py) | 从 JSONL 构建 SQLite、CSV 和 universe dashboard |
| [generate_source_verification_queue.py](generate_source_verification_queue.py) | 从 SQLite 生成原文核验队列和 Batch 1 |
| [scaffold_evidence_cards.py](scaffold_evidence_cards.py) | 从 Batch 1 CSV 生成 evidence card 草稿 |
| [generate_us_alpha_mining_queue.py](generate_us_alpha_mining_queue.py) | 从 SQLite 生成美股 alpha mining 队列和 evidence card 草稿 |

## 常用命令

```bash
python3 scripts/build_universe_system.py
python3 scripts/generate_source_verification_queue.py
python3 scripts/scaffold_evidence_cards.py
python3 scripts/generate_us_alpha_mining_queue.py
```

## 原则

- 脚本输出不代表事实结论，只代表研究队列和优先级。
- 不接 IBKR，不自动交易。
- 不生成买卖建议、目标价或实际仓位建议。
