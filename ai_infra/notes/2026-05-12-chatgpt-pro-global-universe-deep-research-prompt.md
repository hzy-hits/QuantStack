# ChatGPT Pro 提问稿：AI Infra 全球候选 Universe 深度研究

日期：2026-05-12  
用途：让 ChatGPT Pro 基于本项目已有研究文档和 D0-D5 BFS 建模，生成全球 AI Infra 可交易候选池。

## 研究目标

构建一个可以后续用于“自建 AI Infra 主题基金”的全球候选 universe：

- 主战场一：中国资产池，A 股主板 + 港股；A 股排除创业板 `300/301`、科创板 `688/689`、北交所。
- 主战场二：美国资产池，美股大盘、美股中小盘、ADR、ETF。
- 卫星池：欧洲、日本、韩国、台湾、以色列及其他可交易小中盘。
- D0 需求源头优先看 OpenAI、Anthropic、Google DeepMind / Gemini；Kimi、DeepSeek 作为中国/开源/国产链补充。

重点不是推荐买卖，而是建立：

公司 universe
  -> D0-D5 BFS depth
  -> dependency edge
  -> 对应海外瓶颈
  -> 对应公司 / 客户 / 供应链
  -> 证据等级
  -> ETF / 资金覆盖线索
  -> 原文核验清单
  -> 进入核心池 / 候选池 / 雷达池 / 排除池

## 提问词

```text
会话标题：AI Infra 全球可交易候选池深度研究

我正在做 AI super cycle / AI Infra 产业链研究，目标是后续构建一个类似“自建 AI Infra 主题基金”的研究 universe，但现在不是投资建议，也不要给买卖建议或目标价。

请基于 D0-D5 LLM Dependency BFS 框架，做一个全球可交易候选池深度研究。

核心框架：

D0：LLM 核心源头
- OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI。
- 定义模型能力、token demand、训练节奏、推理规模和 AI 产品形态。

D1：模型强依赖一阶
- NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
- LLM Lab 训练、推理和产品化不可绕开的直接依赖。

D2：一阶依赖的直接瓶颈
- HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。

D3：二阶瓶颈供应商
- HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件。

D4-D5：深层雷达
- 材料、气体、化学品、真空、洁净、能源、电网、融资、监管。
- 只做雷达，除非能证明反向卡住 D0-D2，否则不升级为核心。

研究重点：
- 主看 D1-D3。
- 最深看到 D5。
- D4-D5 默认只做雷达。
- 超过 D5 默认排除。

覆盖市场：
1. A 股主板：排除创业板 300/301、科创板 688/689、北交所；
2. 美股 / ADR；
3. 港股；
4. 欧洲；
5. 日本；
6. 韩国；
7. 台湾；
8. 以色列及其他可交易小中盘。

请特别关注：
- 100B 美元市值以下的中小盘；
- 不是 AI 公司、但可能被 AI 规格升级拉动的隐形供应链；
- HBM / CoWoS / testing / substrate / optics / CPO / silicon photonics / power / cooling / grid / NeoCloud / storage supercycle；
- 是否被 ETF 覆盖，尤其是 AI、semiconductor、robotics、smart grid、infrastructure、nuclear/uranium、Japan/Korea/Taiwan/Europe regional ETF；
- 是否可能被 13F / 主动基金 / 主题 ETF / smart money 逐步发现。

严格要求：
1. 不给买卖建议；
2. 不给目标价；
3. 不要从股票热门概念出发，要从 D0-D5 BFS dependency path 出发；
4. 每家公司必须标注 BFS depth；
5. 每家公司必须标注 dependency edge：客户边 / BOM 边 / 产能边 / 技术边 / 现金流边 / 反证边；
6. 每家公司必须标注证据状态：原文已证明 / 合理推论 / 待原文核验 / 主要反证；
7. 所有收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线，都必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游交叉披露核验。ChatGPT 输出只能作为线索；
8. 对 A 股必须排除创业板、科创板、北交所；
9. 对欧日韩小盘要特别标注流动性、交易可达性、ADR/本地市场、IBKR 是否通常可交易这一类后续需要核验的问题；
10. 对每个市场都要分核心池 / 候选池 / 雷达池 / 排除池。

请输出：

1. 全球 AI Infra BFS universe 总表
   字段包括：
   - 市场 / 国家；
   - ticker；
   - 公司；
   - 市值区间；
   - BFS depth；
   - 对应模块；
   - dependency path；
   - dependency edge；
   - 对应海外瓶颈；
   - 对应上下游公司；
   - 证据状态；
   - ETF 覆盖线索；
   - smart money / 13F / 主动基金线索；
   - 主要反证；
   - 当前分池。

2. 按 BFS depth 分层的全球公司池
   - D1 龙头；
   - D2 直接瓶颈；
   - D3 高弹性候选；
   - D4-D5 雷达；
   - 排除 / 只做观察。

3. 按市场分层的公司池
   - A 股主板；
   - 美股 / ADR；
   - 港股；
   - 欧洲；
   - 日本；
   - 韩国；
   - 台湾；
   - 以色列 / 其他。

4. 100B 美元市值以下候选池
   - 哪些可能是真正的 D2-D3 高弹性；
   - 哪些只是主题映射；
   - 哪些需要先用原文核验。

5. ETF 覆盖研究
   - 哪些 ETF 可能覆盖这些公司；
   - ETF 类型：AI / semiconductor / robotics / smart grid / infrastructure / nuclear / regional；
   - 如何下载 holdings；
   - 如何计算 ETF coverage score；
   - 哪些公司可能被多个主题 ETF 共同覆盖。

6. smart money 跟踪框架
   - 13F；
   - 13D / 13G；
   - N-PORT；
   - ETF flows；
   - options OI / IV / skew；
   - insider / buyback / strategic investment；
   - 哪些只适合做线索，不能当事实。

7. 组合构建初步框架
   - 如何把这个 universe 变成 paper portfolio；
   - core beta / satellite alpha / option overlay / cash；
   - 如何控制单股、单模块、单市场、单因子、单主题风险；
   - 如何估算 beta vs SPY / QQQ / SMH / AI Infra custom basket；
   - 如何用 Greeks 管理期权暴露；
   - 不要给具体买入建议，只给方法和模板。

8. 下一步原文核验清单
   - 每个模块先核验哪些公司；
   - 每个公司先找哪些原文；
   - 哪些指标决定升级 / 降级 / 排除。

请输出尽量表格化，便于我保存为 markdown 和后续写脚本抓数据。
```
