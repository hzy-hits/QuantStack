# evidence

这里放公司级原文证据卡。只有 evidence card 里被原文证明的内容，才可以进入正式研究结论。

## 当前批次

| 目录 | 用途 |
| --- | --- |
| [batch1](batch1) | 第一轮 24 家 P0 公司 evidence card 草稿 |
| [credit](credit) | Credit / financing 反证卡 |
| [firm_power](firm_power) | Nuclear / firm power 电力交付反证卡 |
| [us_alpha](us_alpha) | 美股 alpha sleeve evidence card 草稿 |

## 写卡规则

- `原文已证明`: 只写公司原文、监管文件、交易所公告、官网技术资料或上下游交叉披露能证明的事实。
- `合理推论`: 必须写清楚从哪条原文事实推出来。
- `待原文核验`: Pro 输出、媒体线索、ETF 线索、未经核实的客户关系都放这里。
- `主要反证`: 客户集中、价格战、供给过剩、融资压力、技术替代、毛利率不跟随收入等。

## 生成草稿

```bash
python3 scripts/scaffold_evidence_cards.py
```

默认不会覆盖已存在的卡片，避免覆盖人工补充。
