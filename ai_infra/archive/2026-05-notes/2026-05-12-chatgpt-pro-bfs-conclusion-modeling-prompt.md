# ChatGPT Pro 提问稿：LLM Dependency BFS 结论建模

日期：2026-05-12  
用途：让 ChatGPT Pro 基于 D0-D5 BFS 框架，生成可落地的 AI Infra 研究结论模型。

## 提问词

```text
会话标题：LLM Dependency BFS 结论建模

我正在做一个 AI super cycle / AI Infra 产业链研究项目。现在希望把研究框架从“泛 AI Infra 分层”升级成“从 LLM Lab 出发的 dependency BFS”。

核心框架如下：

D0：LLM 核心源头
- OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI。
- 它们定义模型能力、训练节奏、推理规模、token 需求和 AI 产品形态。

D1：模型强依赖一阶
- NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
- 这些是 LLM Lab 训练、推理和产品化不可绕开的直接依赖。

D2：一阶依赖的直接瓶颈
- HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- 这些直接限制 D1 交付、性能、成本或扩张速度。

D3：二阶瓶颈供应商
- HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件。
- 这些限制 D2 放量，可能是高弹性小中盘机会。

D4-D5：深层雷达
- 材料、气体、化学品、真空、洁净、能源、电网、融资、监管。
- 只做雷达，除非能证明反向卡住 D0-D2，否则不升级为核心。

研究原则：
- 主看 D1-D3，最深看到 D5。
- D4-D5 只做雷达；超过 D5 默认排除。
- A 股映射也必须标注 BFS 深度，优先看能映射到 D1-D3 的公司；超过 D3 的公司只能进观察池，除非有强原文证据证明其反向卡住 D0-D2。
- 不给买卖建议，不给目标价。
- 所有涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线的数据，都必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游交叉披露核验。ChatGPT 输出只作为线索。

请帮我生成一套“AI Infra 研究结论建模体系”，要求能直接落地到 markdown 文档和公司研究卡片中。

请输出：

1. 研究对象的数据模型
   - 一个 company/module card 应该有哪些字段？
   - 如何记录 BFS depth、上游/下游、边关系、证据等级、反证、财报指标？

2. BFS 边关系模型
   - 客户边、BOM 边、产能边、技术边、现金流边、反证边分别如何定义？
   - 每种边需要什么原始证据？
   - 哪些边是强证据，哪些只是弱线索？

3. 结论分层模型
   - 如何把公司分成：核心池、候选池、雷达池、排除池？
   - 如何区分：D1 龙头、D2 直接瓶颈、D3 高弹性瓶颈、D4-D5 雷达约束、主题映射？
   - 如何避免把远端能源/材料公司误判成 AI 核心标的？

4. 评分模型
   请给一个 100 分或 5 星评分体系，至少包含：
   - BFS depth 权重；
   - 需求真实性；
   - 供给瓶颈强度；
   - 议价权 / 毛利率传导；
   - 财务传导到 FCF；
   - 技术替代风险；
   - 客户集中风险；
   - 证据质量；
   - 反证清晰度；
   - 10 倍弹性 / 指数增长可能性。

5. 结论模板
   请给出几个标准结论句模板，例如：
   - “核心瓶颈，待原文核验”；
   - “D3 高弹性候选，但证据仍不足”；
   - “D4 雷达项，不能作为核心结论”；
   - “收入相关但毛利/FCF 不传导”；
   - “主题映射，暂时排除”。

6. A 股映射规则
   - 如何把海外 D1-D3 瓶颈映射到非科创板、非创业板 A 股主板？
   - A 股公司必须回答哪些问题？
   - 如何处理 D4-D5 的 A 股公司？
   - 如何避免 A 股概念先行？

7. 研究流程
   请给出实际执行顺序：
   - 从 D0 token demand / model roadmap 出发；
   - 到 D1 GPU/cloud/TPU；
   - 到 D2 HBM/CoWoS/networking/power；
   - 到 D3 equipment/test/material/substrate/optics；
   - 再到 A 股映射；
   - 最后做原始出处核验和反证仪表盘。

8. 产出格式
   请给出可以直接复制到 markdown 的：
   - 总图表格；
   - 公司卡片模板；
   - 证据卡片模板；
   - A 股映射表模板；
   - 季度复盘表模板；
   - 排除池记录模板。

请严格区分：
- 原文已证明；
- 合理推论；
- 待原文核验；
- 主要反证。

请不要输出股票买卖建议，不要给目标价。
```
