# AI Infra BFS Fund Philosophy

状态：working philosophy, not investment advice
用途：把本项目抽象成一套可复用的主题基金研究和组合管理方法。

## 一句话定义

我们不是在做“AI 概念股清单”，而是在做一个从 `LLM 需求源头` 出发、沿基础设施强依赖做 BFS、用原文证据和反证层筛选的 AI Infra 主题基金研究系统。

```text
D0 LLM demand
  -> D1 compute / cloud / ASIC / software stack
  -> D2 HBM / CoWoS / optics / networking / power / cooling / rack
  -> D3 equipment / test / substrate / IP / components
  -> D4-D5 materials / energy / grid / financing / regulation as radar
```

## 核心哲学

### 1. 从需求源头出发，而不是从股票故事出发

研究根节点是 OpenAI、Anthropic、Google DeepMind / Gemini 等 LLM Lab。

我们关心的是：

- 模型训练和推理是否持续扩大 token、FLOPS、memory bandwidth、network bandwidth 和 power demand。
- 这种需求如何传导到 GPU/TPU/ASIC、HBM、CoWoS、光互连、数据中心、电力和冷却。
- 哪些公司不是“讲 AI”，而是被上游真实 CapEx、订单、客户、产能和技术路线拉动。

### 2. 用 BFS 控制研究深度

默认只重点研究 `D1-D3`。

| 层级 | 作用 | 组合含义 |
| --- | --- | --- |
| D0 | 需求源头 | 需求验证，不一定可交易 |
| D1 | 强依赖一阶 | core beta 和需求锚 |
| D2 | 直接瓶颈 | 高确定性 + 中高弹性 |
| D3 | 二阶瓶颈供应商 | alpha sleeve 主战场 |
| D4-D5 | 深层雷达 | 只做反证或升级候选 |

`D4-D5` 不是不能投，而是必须证明它能反向卡住 `D0-D2`，否则只放雷达。

### 3. 先证明需求，再谈估值

AI Infra 更接近跨时代基础设施周期，不是普通 PE 均值回归。

传统估值仍然重要，但优先级低于这些问题：

- 需求来自真实客户还是新闻稿？
- 供给瓶颈是否扩产慢、认证长、技术难？
- 单位价值量是否随 AI 集群代际上升？
- 毛利率、订单、backlog、FCF 是否跟着上升？
- 反证是否可被及时观察？

估值用于控制风险，不用于否定一条已经被原文证明的结构性主线。

### 4. 原文证据高于模型输出

ChatGPT Pro、媒体、券商、社交平台只能提供线索。

能进入结论的证据必须来自：

- 年报、10-K、20-F、10-Q、季报；
- earnings release、earnings call transcript；
- investor presentation；
- 交易所公告、SEC/监管文件；
- 公司官网技术资料；
- 客户或上下游交叉披露；
- FERC/NRC/RTO/DOE 等专业监管或官方资料。

所有事实必须标成：

| 证据状态 | 含义 |
| --- | --- |
| 原文已证明 | 原始来源可直接证明 |
| 合理推论 | 由原文事实推出，链条写清楚 |
| 待原文核验 | Pro / 新闻 / 市场线索 |
| 主要反证 | 可能推翻 thesis 的事实 |

### 5. Alpha 来自“真实瓶颈 + 小市值弹性 + 未充分覆盖”

本项目寻找的不是所有大盘 AI beta，而是：

```text
强 D0/D1 需求
  + D2/D3 真实瓶颈
  + 供给侧慢扩产
  + 客户认证强
  + 单位价值量上升
  + 财务传导开始出现
  + 市值/覆盖度仍有弹性
```

典型方向：

- optics / CPO / InP / lasers；
- retimer / SerDes / AEC / CXL / PCIe；
- HBM / CoWoS / advanced packaging equipment；
- test / probe / inspection / metrology；
- power / cooling / switchgear；
- selected NeoCloud / powered land, only after credit card passes。

### 6. 反证层是系统的一部分

本项目不只找多头理由，也主动维护两类横向反证层：

| 反证层 | 关键问题 |
| --- | --- |
| Credit / CDS / Financing | backlog 是否转现金流，还是被 debt、lease、interest、depreciation、customer concentration 吞掉？ |
| Nuclear / Firm Power / Grid | 数据中心是否真的能拿到电、并网、PPA、监管许可和燃料？ |

如果融资或电力交付恶化，D1-D3 再强也可能被延迟或重估。

### 7. 组合是研究的输出，不是研究的起点

组合层目标不是“保证收益最大、风险最小”，而是在可承受回撤内提高对真瓶颈的暴露。

初始结构：

| 层 | 作用 |
| --- | --- |
| Core beta | AI Infra 大盘 beta，如 semis/cloud/AI infra ETF |
| Alpha basket | D2-D3 小中盘候选，需 evidence card 通过 |
| China sleeve | A 股主板 + 港股映射，按 BFS 深度和原文验证进入 |
| Satellite markets | 日韩台欧以色列隐形供应链 |
| Credit / power radar | 风险约束，不直接当主题加仓理由 |
| Cash / T-bills | 控制回撤和等待事件 |

## 项目产物

| 层 | 文件 |
| --- | --- |
| 方法论 | `docs/llm-dependency-bfs-framework.md`, `docs/research-checklist.md` |
| 数据层 | public 样例 `data/seed/global_universe_sample.jsonl`; private 完整 `data/global_universe_v2.jsonl` 和 SQLite |
| 队列 | private generated reports |
| 证据卡 | private evidence cards 或脱敏公开样例 |
| 组合框架 | private notes / paper portfolio specs |

## 当前原则

1. 没有 BFS depth，不进入公司结论。
2. 没有原文，不写“原文已证明”。
3. D1-D3 是主战场；D4-D5 默认雷达。
4. 每完成 3-5 张 evidence card，回头更新 score 和 pool。
5. 在 evidence card 批量完成前，不生成真实仓位建议。
