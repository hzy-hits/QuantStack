# A 股主板映射启动方案

日期：2026-05-12  
状态：准备启动 ChatGPT Pro，会先做候选映射，不做买卖结论

## 是否可以启动

可以启动，但研究阶段只能定义为：

> 从海外 AI Infra 已识别瓶颈回映射到非科创板、非创业板 A 股主板候选池。

不能定义为：

> A 股 AI 标的推荐、买卖建议、目标价或结论性排序。

原因：

- ChatGPT Pro 已完成 HBM、CoWoS、Testing、光互连、Scale-up / Custom ASIC、电力液冷、NeoCloud、非美隐形冠军、存储反证九个方向的第一轮信息扩展。
- 这些输出已经足够形成“海外瓶颈 → A 股环节”的映射骨架。
- 但九个方向仍未完成逐条原始出处核验，所以 A 股阶段必须保持“候选池 + 核验清单”性质。

## 启动边界

本阶段只允许做三件事：

1. 建立 A 股主板候选池。
2. 标注每家公司对应的海外瓶颈环节和海外可比公司。
3. 列出必须核验的原始出处、财报指标和主要反证。

本阶段不做：

- 买入 / 卖出建议。
- 目标价。
- 直接排名为“最值得买”。
- 仅因 A 股热门概念纳入。
- 科创板 `688xxx` 和创业板 `300xxx` 标的。

## A 股映射优先顺序

本阶段采用 `LLM Dependency BFS`：

- `D0`：OpenAI / Anthropic / Google DeepMind 等 LLM 核心需求源头。
- `D1`：模型强依赖一阶，例如 NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave。
- `D2`：HBM、CoWoS、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- `D3`：设备、测试、基板、光子、EDA/IP、连接、液冷组件、电力设备关键部件。
- `D4-D5`：更深层材料、能源、电网、融资、监管，只作为雷达。

A 股主板优先映射 `D1-D3`。超过 `D3` 的公司必须证明它能反向卡住 `D0-D2`，否则只进观察池。

### 第一层：海外瓶颈确定性最高

| 海外瓶颈 | A 股主板映射方向 | 核验重点 |
| --- | --- | --- |
| HBM / CoWoS / advanced packaging | 封装测试、先进封装材料、半导体设备 | 是否有 HPC / AI / Chiplet / advanced packaging 收入或订单 |
| AI server PCB / CCL | 高速 PCB、高速低损耗 CCL、服务器 PCB | AI server / HPC / 交换机板收入、客户、毛利率、扩产 |
| 800G / 1.6T 光互连 | 光模块、光器件、光芯片、数据中心光纤 | 800G / 1.6T / CPO / 硅光收入或客户认证 |
| 数据中心电力 / 液冷 | UPS、HVDC、液冷、温控、变压器、开关 | AIDC 订单、液冷收入、backlog、毛利率、交付周期 |

### 第二层：海外链条重要但 A 股主板直接性较弱

| 海外瓶颈 | A 股主板映射方向 | 核验重点 |
| --- | --- | --- |
| AI server / rack integration | 服务器 ODM、网络设备、系统集成 | 收入是否增长但利润率低，是否受客户议价压制 |
| Custom ASIC / scale-up fabric | 高速连接器、线缆、交换机、服务器网络 | 是否有数据中心客户和高速规格产品 |
| NeoCloud / AI data center | 算力租赁、智算中心、IDC、能源配套 | 是否有真实客户合同、利用率和现金流，而非项目公告 |
| 存储超级周期 | 存储封测、存储模组、PCB / 控制器相关 | 是否真实关联 HBM / enterprise SSD，而非消费存储周期 |

## Pro 会话提问词

```text
请基于本项目已经完成的 AI Infra 海外产业链研究，把海外瓶颈环节映射到非科创板、非创业板 A 股主板公司。

重要前提：
1. 这不是投资建议，不给买卖建议，不给目标价；
2. 不要从 A 股热门概念出发；
3. 必须先列海外瓶颈环节，再映射 A 股公司；
4. 排除创业板 300xxx 和科创板 688xxx；
5. 每家公司都要标注“BFS 深度 D1-D5 / 对应海外瓶颈 / 对应海外公司 / A 股产业链位置 / 需要核验的原始出处 / 财报指标 / 主要反证”；
6. 所有涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线的判断，都只能作为待原文核验线索，必须回到年报、半年报、交易所公告、投资者关系活动记录、公司官网技术资料验证；
7. 不要把 A 股新闻稿或券商摘要当事实。

请采用 LLM Dependency BFS：
- D0：OpenAI、Anthropic、Google DeepMind / Gemini 等 LLM 核心需求源头；
- D1：模型强依赖一阶，例如 NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave；
- D2：一阶依赖的直接瓶颈，例如 HBM、CoWoS、AI server/rack、networking、800G/1.6T optics、data center power/cooling；
- D3：二阶瓶颈供应商，例如测试、设备、基板、光子、EDA/IP、连接、液冷组件、电力设备关键部件；
- D4-D5：更深层材料、能源、电网、融资、监管，只作为雷达。
研究重点放在 D1-D3，最深看到 D5。超过 D3 的 A 股公司必须说明为什么仍值得看。

海外已形成的瓶颈主线：
- HBM / HBM equipment / HBM testing / substrate / materials；
- CoWoS / 2.5D / advanced packaging / OSAT / inspection；
- AI server PCB / CCL / high-speed switch PCB；
- 800G / 1.6T / CPO / silicon photonics / optical components；
- Scale-up fabric / custom ASIC / PCIe-CXL / high-speed interconnect；
- AI data center power / UPS / HVDC / transformer / switchgear / liquid cooling / thermal management；
- NeoCloud / GPU-as-a-Service / AI data center economics；
- storage supercycle：HBM 最纯，server DRAM 混合，NAND/eSSD 需要严格区分传统周期。

请输出：
1. A 股主板 AI Infra 映射总表；
2. 按海外瓶颈分层的候选池；
3. 每家公司最需要核验的原始出处；
4. 每家公司最可能的误判点；
5. 第一优先级、第二优先级、观察/排除池；
6. 下一步原始出处核验清单。
```

## 本地候选池入口

现有本地预筛文件：

- [2026-05-12-a-share-mainboard-ai-infra-watchlist.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-a-share-mainboard-ai-infra-watchlist.md)

该文件仍然只能作为本地预筛，不作为结论。Pro 新输出回来后，需要生成 `A股映射 v2`，把每家公司重新绑定到海外瓶颈和核验任务上。
