# LLM Dependency BFS 研究框架

日期：2026-05-12
用途：从 OpenAI / Anthropic / Google DeepMind 等 LLM 核心需求源头出发，用 BFS 控制 AI Infra 产业链研究深度。

## 核心判断

AI Infra 研究不应该把所有“AI 相关”公司放在同一层。更合理的方式是：

> 以 LLM Lab 为根节点，把它们训练、推理和产品化强依赖的公司列为一阶，再沿着供应链继续向上游 BFS。

研究优先级：

- `D0-D1`：核心和一阶，必须长期跟踪。
- `D2-D3`：重点研究区，最可能出现高弹性瓶颈和 10 倍候选。
- `D4-D5`：深层雷达区，用来发现隐形瓶颈、供给约束和周期风险。
- `D6+`：默认不进入主线，除非能证明它反向卡住 D0-D2。

## 分层定义

| BFS 深度 | 名称 | 定义 | 例子 | 研究权重 |
| --- | --- | --- | --- | --- |
| `D0` | LLM 核心需求源头 | 直接定义模型能力、训练节奏、推理规模和 token 需求 | OpenAI、Anthropic、Google DeepMind、Meta、xAI、Mistral、DeepSeek | 最高，但很多未上市 |
| `D1` | 模型强依赖一阶 | LLM Lab 训练/推理不可绕开的直接依赖 | NVIDIA GPU/CUDA/NVLink、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle、CoreWeave、frontier training/inference stack | 最高 |
| `D2` | 一阶依赖的直接瓶颈 | 直接限制 D1 交付、性能或成本的供应链 | HBM、CoWoS/advanced packaging、TSMC leading-edge、AI server/rack、InfiniBand/Ethernet switch/NIC、800G/1.6T optics、data center power/cooling | 很高 |
| `D3` | 二阶瓶颈供应商 | 限制 D2 放量的设备、材料、测试、基板、IP、连接和工程能力 | HBM equipment/test/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、CXL/PCIe retimer、EDA/IP、电力设备关键部件 | 最高弹性区 |
| `D4` | 深层制造与材料 | 支撑 D3 的制造工具、化学品、气体、真空、洁净、衬底、精密加工 | photoresist、CMP slurry、etch gas、vacuum valve、UPW、SOI/InP/GaAs substrate、specialty chemicals | 雷达区 |
| `D5` | 基础资源与约束 | 更底层的能源、矿物、土地、并网、监管、融资和区域供给 | 电网、天然气、核能、铀燃料、工业金属、土地/水资源、项目融资、保险 | 远端约束区 |

## 研究范围控制

### 重点只看三阶以内

常规研究主线只进入 `D0-D3`：

1. `D0`：LLM Lab 的模型路线、token 需求、训练/推理节奏。
2. `D1`：GPU/TPU/ASIC、云算力、训练/推理软件栈。
3. `D2`：HBM、CoWoS、网络、光互连、数据中心电力/冷却、AI server/rack。
4. `D3`：设备、测试、基板、光子、材料、EDA/IP、连接器等瓶颈。

真正寻找高弹性资产时，重点不是只看 `D1` 龙头，而是看：

> `D2-D3` 中被 D0 token 需求和 D1 集群扩张放大、但供给侧扩张慢、客户认证强、单机价值量上升的环节。

### 最深看到五阶

`D4-D5` 只做雷达，不直接当核心结论：

- 用来解释为什么某个 D2/D3 环节扩产慢。
- 用来发现还未被市场充分映射的上游瓶颈。
- 用来识别周期风险、地缘风险、能源/并网/融资约束。
- 只有当 D4/D5 公司能被证明反向卡住 D0-D2 的交付，才升级为核心研究对象。

## BFS 边的定义

一个公司或环节能进入下一层，必须满足至少一个边关系：

| 边类型 | 问题 | 证据 |
| --- | --- | --- |
| 客户边 | 谁直接买它？是否是上一层公司或上一层的关键供应商？ | 客户披露、订单、长约、收入分部 |
| BOM 边 | 它是否进入上一层产品的关键 BOM？ | 产品规格、拆解、供应链披露 |
| 产能边 | 它是否限制上一层产能释放？ | backlog、交期、扩产周期、客户认证 |
| 技术边 | 它是否决定上一层性能、功耗、带宽、良率？ | 技术白皮书、标准、产品参数 |
| 现金流边 | 上一层 CapEx 是否传导到它的收入、毛利、FCF？ | 财报、订单、CapEx 对应关系 |
| 反证边 | 若它出问题，上一层是否延迟、降性能或涨成本？ | 风险因素、供应约束、客户表述 |

如果边关系只能靠“市场说它 AI 概念”成立，就不能进入 BFS 主图。

## 公司优先级规则

| 优先级 | 条件 |
| --- | --- |
| 核心池 | `D0-D3`，且有原文证据证明客户、订单、收入、产品或技术路线与 LLM 训练/推理强相关 |
| 候选池 | `D2-D4`，逻辑强但原文证据仍不足 |
| 雷达池 | `D4-D5`，可能是隐形约束但尚未证明传导 |
| 排除池 | `D6+` 或边关系不清，只靠主题映射 |

## A 股映射中的应用

A 股研究必须先把海外瓶颈放进 BFS 图，再映射 A 股：

```text
OpenAI / Anthropic / Google Gemini token demand
  -> D1: NVIDIA GPU / Google TPU / hyperscaler AI cloud
  -> D2: HBM / CoWoS / optical / AI server PCB / data center power
  -> D3: HBM test / substrate / high-speed CCL / liquid cooling components / optical components
  -> A 股主板候选
```

A 股公司必须回答：

1. 它对应 BFS 哪一层？
2. 它连接到哪个海外瓶颈？
3. 它对应哪个海外公司或环节？
4. 证据是客户、产品、订单、收入，还是只是叙事？
5. 是否在三阶以内？如果超过三阶，为什么仍值得看？

默认规则：

- `D1-D3` A 股映射优先。
- `D4-D5` 只做雷达。
- 超过 `D5` 默认排除。

## ChatGPT Pro 提问模板

```text
请用 BFS 方式研究 LLM / AI Infra 产业链。

根节点是 LLM 核心需求源头：OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI 等。

请按 BFS 深度输出：
- D0：LLM 核心需求源头；
- D1：模型强依赖的一阶公司和环节，例如 NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈；
- D2：一阶依赖的直接瓶颈，例如 HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling；
- D3：二阶瓶颈供应商，例如 HBM equipment/test/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、CXL/PCIe retimer、液冷组件、电力设备关键部件；
- D4-D5：更深层材料、设备、化学品、气体、真空、洁净、能源、电网、融资、监管，只作为雷达区。

研究重点放在 D1-D3，最深看到 D5。每家公司或环节必须说明：
1. BFS 深度；
2. 与上一层的边关系；
3. 证据类型：客户、BOM、产能、技术、现金流、反证；
4. 原始出处清单；
5. 主要反证。

不要输出买卖建议，不给目标价。没有原文证据的内容标记为待核验。
```
