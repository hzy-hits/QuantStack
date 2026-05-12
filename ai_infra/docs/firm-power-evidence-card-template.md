# Nuclear / Firm Power Evidence Card Template

用途：用于 utility、IPP、existing nuclear fleet、nuclear restart、SMR、uranium/fuel、gas turbine、grid equipment、onsite power、battery/grid support 等 AI data center power 相关资产。
边界：这张卡是电力交付反证卡，不是投资建议、买卖建议、目标价或仓位建议。

## 基本信息

| 字段 | 内容 |
| --- | --- |
| 公司 / asset |  |
| ticker / security |  |
| BFS 位置 | D2 power constraint / D3 power equipment / D5 energy |
| AI 连接假设 |  |
| 当前分层 | 核心 / 候选 / 雷达 / proxy / 排除 |
| 证据状态 | pending_original_source_verification |

## 原始来源

| 来源类型 | 链接 / 文件 | 日期 | 用途 |
| --- | --- | --- | --- |
| 10-K / 20-F / annual report |  |  | generation, backlog, debt, risk factors |
| 10-Q / interim report |  |  | latest financials and project updates |
| PPA / 8-K / customer announcement |  |  | named customer, MW, term, economics |
| FERC / RTO / utility filing |  |  | interconnect, tariff, cost allocation |
| NRC / ONR / DOE filing |  |  | nuclear licensing and fuel path |
| investor presentation / earnings call |  |  | management commentary, timeline, backlog |

## AI Customer / Contracted Capacity

| 指标 | 原文位置 | 原文已证明 | 仍不能证明 | 备注 |
| --- | --- | --- | --- | --- |
| Named AI/data center customer |  |  |  | customer confirmation preferred |
| Contracted MW / GW |  |  |  | ramp schedule |
| Delivery date / energization date |  |  |  |  |
| PPA term |  |  |  |  |
| PPA type / take-or-pay |  |  |  | physical / virtual / capacity |
| PPA price / escalator / REC |  |  |  | if disclosed |
| Counterparty credit quality |  |  |  | guarantee / prepayment / backstop |

## Grid / Interconnect / Regulatory

| 指标 | 原文位置 | 原文已证明 | 红旗 / 反证 |
| --- | --- | --- | --- |
| Interconnect queue status |  |  | 排队太长或被驳回 |
| Transmission rights / cost allocation |  |  | 成本不可回收 |
| Substation / grid upgrade status |  |  |  |
| FERC / RTO docket status |  |  |  |
| State PUC / utility approval |  |  |  |
| NRC / ONR / DOE milestone |  |  |  |
| Local permit / environmental constraint |  |  |  |

## Plant / Asset / Fuel

| 指标 | 原文位置 | 原文已证明 | 红旗 / 反证 |
| --- | --- | --- | --- |
| Plant capacity / fleet generation |  |  |  |
| Capacity factor / outage |  |  |  |
| Restart cost / license extension |  |  |  |
| Gas turbine backlog / delivery slot |  |  |  |
| Fuel supply |  |  |  |
| Uranium / conversion / LEU / HALEU / TRISO |  |  | fuel path 不清 |
| Project capex / financing |  |  | funding gap |
| Revenue / backlog entry |  |  | only press release |

## 分层规则

| 分层 | 最低要求 |
| --- | --- |
| D2/D3 候选 | named AI/data center customer + MW/GW + term + interconnect/regulatory path |
| D5 雷达 | 只有 power / nuclear / uranium 主题，AI 传导仍间接 |
| Proxy | ETF、commodity、macro grid data，只做资金和环境线索 |
| 排除 | 只有新闻稿或概念映射，缺合同、MW、监管或燃料路径 |

## 结论分层

| 层级 | 内容 |
| --- | --- |
| 原文已证明 |  |
| 合理推论 |  |
| 待原文核验 |  |
| 主要反证 |  |
| 当前动作 | 保持核心 / 升级候选 / 降级雷达 / 排除 |
