# AI Infra Research MVP Plan v1

状态：MVP 启动方案  
边界：研究流程验证，不是投资建议、买卖建议、目标价或仓位建议。

## 为什么先做 MVP

当前 universe 已有 146 家，直接全量研究会失控。正确做法是先用少量公司验证四件事：

1. 能不能稳定找到原文。
2. evidence card 模板是否能承载真实事实和反证。
3. BFS depth / dependency edge / score 是否真的能区分核心、候选、雷达。
4. 研究结论能不能从“叙事”收敛到“收入、订单、产能、毛利、客户、技术路线”。

MVP 成功后，再扩大到 24 家 Batch 1；Batch 1 成功后，再处理 146 家全量 universe。

## MVP 范围

先做 9 家，每个资产池 3 家。

| 资产池 | 公司 | 为什么选 |
| --- | --- | --- |
| 中国资产池 | 沪电股份、英维克、工业富联 | 覆盖 PCB/CCL、液冷、AI server/rack |
| 美国资产池 | Coherent、NVIDIA、Vertiv | 覆盖 optics/CPO、GPU/CUDA、data center power/thermal |
| 卫星资产池 | SK hynix、TSMC、TOWA | 覆盖 HBM、CoWoS/foundry、advanced packaging equipment |

## 输出物

每家公司一张 evidence card，位置在 [evidence/batch1](../evidence/batch1)。

每张卡必须包含：

| 区块 | 最低要求 |
| --- | --- |
| 原文来源登记 | 至少 2 条公司原文或监管/交易所来源；找不到要说明 |
| 原文证据 | 至少填收入/业务模块、盈利或毛利、订单/backlog/产能/客户/产品证据中的 3 类 |
| 结论分层 | 必须区分原文已证明、合理推论、待原文核验、主要反证 |
| 当前动作 | 保持候选 / 降为雷达 / 继续核验，并说明原因 |

## 原文优先级

| 优先级 | 来源 |
| --- | --- |
| P0 | 公司 annual report / 10-K / 20-F / 年报 |
| P0 | 最新 quarterly results / earnings release / 季报 |
| P1 | earnings call transcript / investor presentation |
| P1 | 公司官网产品页 / 技术资料 / capacity announcement |
| P2 | 交易所公告 / 监管文件 / 上下游交叉披露 |
| 禁用 | 媒体、券商、社交平台、ChatGPT Pro 输出直接当事实 |

## 验收标准

MVP 不要求所有信息都找全，但要求每家公司能回答：

1. AI Infra 相关性是原文证明，还是合理推论？
2. 财务传导是否已经出现：收入、订单、backlog、毛利率、产能、客户任一维度？
3. 主要反证是什么：客户集中、价格战、供给过剩、项目延期、技术替代、毛利不跟随收入？
4. 这家公司在当前 universe 里应保持 P0、降到 P1/P2，还是移入雷达？

## 并行分工

| Worker | 资产池 | 文件 |
| --- | --- | --- |
| China | 中国资产池 | 沪电股份、英维克、工业富联 |
| US | 美国资产池 | Coherent、NVIDIA、Vertiv |
| Satellite | 卫星资产池 | SK hynix、TSMC、TOWA |

## MVP 后的动作

如果 9 张卡能跑通：

1. 扩到 Batch 1 全部 24 家。
2. 给 `source_verification_queue_v1.csv` 增加核验状态字段更新流程。
3. 开始接 ETF holdings 和价格数据，但仍不做真实交易。

如果 9 张卡跑不通：

1. 先修 evidence card 模板。
2. 降低一次性公司数量。
3. 优先做披露质量最高的美国大盘和台湾/日本核心供应链。
