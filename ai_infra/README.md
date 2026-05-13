# ai super cycle: AI Infra 产业链研究

## 当前入口

每天继续项目时，先看 [START_HERE.md](START_HERE.md)。

公开仓库只保存方法论、脚本、模板和可复现的安全样例。完整 ChatGPT Pro 输出、运行日志、候选排序、evidence card、dashboard、SQLite、完整 universe、组合和券商相关数据默认保留在本地/private repo。

快速入口：

| 入口 | 用途 |
| --- | --- |
| [START_HERE.md](START_HERE.md) | 当前工作台和读文档顺序 |
| [docs/fund-management-philosophy.md](docs/fund-management-philosophy.md) | 研究哲学和基金工程抽象 |
| [docs/llm-dependency-bfs-framework.md](docs/llm-dependency-bfs-framework.md) | D0-D5 dependency BFS 框架 |
| [docs/research-checklist.md](docs/research-checklist.md) | 公司/模块判断基线 |
| [docs/source-evidence-template.md](docs/source-evidence-template.md) | 原始出处 evidence card 模板 |
| [docs/company-financials-market-options-methodology.md](docs/company-financials-market-options-methodology.md) | 公司财报、K线、期权三层研究方法 |
| [scripts/build_universe_system.py](scripts/build_universe_system.py) | universe 落库和 dashboard 生成脚本 |
| [data/seed/global_universe_sample.jsonl](data/seed/global_universe_sample.jsonl) | 公开安全的最小样例数据 |

## 项目目标

本项目的核心目标是：通过持续调用 ChatGPT Pro，系统研究 AI Infra 产业链，寻找能够与 AI Infra 时代一起实现资产指数级增长、甚至具备 10 倍增长潜力的产业环节和公司。

这里的研究重点不是泛泛讨论 AI 应用，而是从 Gemini、ChatGPT、Claude 等前沿 LLM 产品倒推整个基础设施链条：谁提供基础模型，谁提供训练和推理框架，谁提供算力，谁制造芯片和封装，谁建设云和数据中心，谁提供网络、光模块、电力和能源。

所有涉及实际数据的判断，必须先找到原始出处再进入结论层。收入、订单、backlog、产能、ASP、毛利率、CapEx、库存、客户结构、出货量和技术路线，优先来自公司财报、年报、10-K/20-F、earnings release、earnings call transcript、investor presentation、交易所公告、监管文件和公司官网技术资料。媒体报道、券商摘要、社交平台和 ChatGPT Pro 输出只能作为线索，不能直接作为证据。

## 研究哲学

AI super cycle 更接近跨时代的大基建周期，而不是普通行业景气轮动。研究起点不应该是“当前 PE 是否便宜”，而应该是：

- AI 是否正在重建下一代计算、电力、网络、存储和制造基础设施。
- 这个环节是否承担真实资本开支，而不是只承接主题叙事。
- 需求是否来自模型能力、推理使用量、数据中心建设和云厂商 CapEx 的长期扩张。
- 供给瓶颈是否能形成多年瓶颈租金，例如 HBM、先进封装、电力接入、高速网络、液冷、特种材料和关键设备。
- 利润池是否从旧计算周期迁移到新的 AI Infra 层。

PE、EV/EBITDA、P/S 等传统估值指标仍然要看，但它们不是第一性判断。它们主要用于回答：市场是否已经把未来多年增长全部定价、下修时会有多大风险、以及同一产业链内谁的风险回报更好。不能因为早期 PE 高就否定一个真正的基础设施重估周期，也不能因为 PE 低就把弱相关公司误判成 AI Infra 核心资产。

## 起点假设

当前最上游、最直接的需求源头是 LLM 基础模型提供商：

- OpenAI
- Anthropic
- Google / DeepMind

这些公司本身不只是应用公司，也是 AI Infra 需求的直接定义者。它们的模型规模、推理成本、训练节奏、产品形态和多模态路线，会向上游传导出对 GPU/TPU、HBM、网络、数据中心、电力、冷却、封装、先进制程和软件栈的需求。

另一个需要重点验证的假设是：存储超级周期可能是 AI Infra 指数增长的早期信号。原因是 LLM 训练和推理同时消耗高带宽内存、服务器 DRAM、企业级 SSD、网络缓存和数据中心存储系统。如果 HBM、DRAM、NAND、SSD 控制器、封装材料和存储设备公司同时出现价格、出货、毛利率和 CapEx 扩张，可能意味着 AI Infra 需求已经从 GPU 龙头向更宽的供应链扩散。

但这个假设不能直接当结论。需要区分三种情况：

- 真正的 AI 拉动：HBM、服务器 DRAM、企业 SSD、先进封装材料、测试设备等被训练/推理集群直接拉动。
- 传统存储周期反转：价格从底部回升，但核心驱动可能只是供需周期，不一定是 AI。
- AI 叙事映射：公司被市场贴上 AI 标签，但收入、订单和客户结构尚未证明相关性。

## 研究分层

本项目采用 `LLM Dependency BFS` 作为主分层方法：以 OpenAI、Anthropic、Google DeepMind / Gemini 等 LLM Lab 为根节点，把模型训练、推理和产品化的强依赖列为一阶，再沿供应链向上游展开。重点研究三阶以内，最深看到五阶。

| BFS 深度 | 名称 | 例子 | 研究权重 |
| --- | --- | --- | --- |
| `D0` | LLM 核心需求源头 | OpenAI、Anthropic、Google DeepMind、Meta、xAI | 需求定义层 |
| `D1` | 模型强依赖一阶 | NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈 | 最高 |
| `D2` | 一阶依赖直接瓶颈 | HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling | 很高 |
| `D3` | 二阶瓶颈供应商 | HBM equipment/test/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、CXL/PCIe retimer、液冷组件 | 高弹性重点区 |
| `D4` | 深层制造与材料 | 化学品、气体、真空、洁净、衬底、精密加工 | 雷达区 |
| `D5` | 基础资源与约束 | 能源、电网、土地、水、融资、监管 | 远端约束区 |

常规研究只把 `D1-D3` 作为主战场。`D4-D5` 用来解释扩产慢、供给约束和隐形瓶颈，除非能证明反向卡住 `D0-D2`，否则不升级为核心标的。

## 本地数据层

全球可交易候选池 v2 在本地已落成研究数据层，作为后续原文核验、ETF 覆盖、smart money、价格因子和 paper portfolio 的底座。公开仓库不提交完整候选池和生成结果，只提交可复现脚本和安全样例。

| 文件 | 用途 |
| --- | --- |
| [scripts/build_universe_system.py](scripts/build_universe_system.py) | 从 JSONL 重建 SQLite、CSV 和 dashboard 的标准库脚本 |
| [data/seed/global_universe_sample.jsonl](data/seed/global_universe_sample.jsonl) | 公开安全样例，用于验证脚本 |
| `data/global_universe_v2.jsonl` | 私有完整 universe seed，本地存在但不提交 |
| `data/ai_infra_universe.sqlite` | 本地 SQLite 数据库，构建产物，不提交 |
| `reports/*.md / reports/*.csv` | 本地生成的 dashboard 和队列，不提交到 public repo |
| [scripts/generate_source_verification_queue.py](scripts/generate_source_verification_queue.py) | 从 SQLite 生成原文核验队列 |
| [scripts/scaffold_evidence_cards.py](scripts/scaffold_evidence_cards.py) | 从 batch1 CSV 生成逐家公司 evidence card 草稿 |
| `evidence/` | 本地 evidence card 草稿和已核验卡片，不提交到 public repo |

重建命令：

```bash
python3 scripts/build_universe_system.py
python3 scripts/generate_source_verification_queue.py
python3 scripts/scaffold_evidence_cards.py
```

如果本地有 `data/global_universe_v2.jsonl`，脚本会使用完整 universe；如果没有，则用公开样例跑通流程。

这套评分是研究优先级评分，不是投资评分。所有记录默认 `pending_original_source_verification`，不能直接作为买卖依据。

## 公司财报、K线和期权

项目已将公司研究拆成三层：原文证据层、市场行为层、期权风险层。原文证据层判断公司是否真的从 AI Infra 链条获得收入、订单、毛利率、现金流或产能传导；K线层只判断市场是否开始定价、是否拥挤和流动性风险；期权层只判断波动、事件、流动性和尾部风险。

完整规则见 [docs/company-financials-market-options-methodology.md](docs/company-financials-market-options-methodology.md)。核心约束是：K线和期权不能提升 evidence status，也不能把弱证据公司升级为核心研究池。

### 1. LLM 基础模型与产品层

研究对象：

- OpenAI / ChatGPT
- Anthropic / Claude
- Google / Gemini
- 可扩展观察：Meta、xAI、Mistral、DeepSeek、Cohere 等

核心问题：

- 谁在定义下一代模型能力边界？
- 模型能力提升主要依赖参数规模、数据、训练算法、推理架构，还是工具调用和产品闭环？
- 不同模型公司的基础设施依赖有何差异？
- 模型能力变化如何转化为上游 CapEx 和算力需求？

### 2. LLM 学术与工业界框架层

研究对象：

- PyTorch
- JAX / XLA
- TensorFlow
- Triton
- CUDA 软件生态
- Hugging Face、vLLM、Ray、Megatron、DeepSpeed 等训练/推理生态

这里可以建立一个“论文因子”：

- 跟踪 NeurIPS、ICML、ICLR、ACL、CVPR、MLSys、OSDI、SOSP 等顶会论文。
- 观察 AI Lab 与高校合作关系，例如 OpenAI、Anthropic、Google DeepMind、Meta、NVIDIA、Microsoft、Stanford、Berkeley、MIT、CMU 等。
- 记录论文中反复出现的软件框架、硬件假设、训练瓶颈、推理优化方法。
- 判断哪些技术变化会改变产业链价值分配，例如 MoE、长上下文、推理时计算、多模态、AI Agent、低精度计算、KV cache 优化。

这一层不是直接投资标的层，而是产业变化的早期信号层。

### 3. AI 算力与加速器层

研究对象：

- NVIDIA GPU / CUDA
- Google TPU
- AMD GPU
- Cerebras
- AWS Trainium / Inferentia
- Groq、Tenstorrent、SambaNova 等 AI 加速器候选

核心问题：

- 训练和推理分别消耗什么类型的算力？
- NVIDIA 的护城河来自芯片、CUDA、网络、系统集成，还是客户生态？
- TPU、Cerebras、ASIC 是否会在某些工作负载中分流 NVIDIA？
- 推理需求扩大后，算力价值链是否会从训练 GPU 转向推理芯片、内存、网络和电力？

### 4. GPU 龙头及直接上游供应链

重点从 NVIDIA 往上游拆：

- HBM：SK Hynix、Micron、Samsung
- 先进封装 / CoWoS：TSMC、ASE、Amkor 等
- 基板与材料：Ibiden、Unimicron、Shinko、AXTI 等候选
- EDA / IP：Synopsys、Cadence、Arm
- 半导体设备：ASML、Applied Materials、Lam Research、KLA、Tokyo Electron
- 晶圆代工：TSMC、Samsung Foundry、Intel Foundry

核心问题：

- 真正限制 GPU 出货的是晶圆、HBM、CoWoS、基板、封测，还是供电和数据中心交付？
- 哪些环节有价格权，哪些只是周期性产能扩张？
- 龙头公司订单增长会如何传导到二级、三级供应链？

### 5. NeoCloud 与云算力提供商层

研究对象：

- Microsoft Azure
- AWS
- Google Cloud
- Oracle Cloud
- CoreWeave
- Nebius / NBIS
- Lambda、Crusoe 等候选

核心问题：

- 谁能拿到 GPU，谁能快速建设数据中心，谁能把算力卖出高利用率？
- NeoCloud 是长期新平台，还是 GPU 短缺周期中的阶段性套利？
- 云厂商的 CapEx 如何转化为收入、折旧压力和自由现金流？
- OpenAI / Anthropic / Google 的算力合作结构分别如何影响云厂商？

### 6. 侧向芯片、网络与互联层

研究对象：

- CPU：AMD、Intel、Arm
- 交换芯片 / 定制 ASIC：Broadcom / AVGO、Marvell
- 网络设备：Arista、Cisco、NVIDIA Mellanox
- 光互联与 DSP：Broadcom、Marvell、Coherent、Lumentum 等

核心问题：

- AI 集群瓶颈是否从单卡算力转向网络、内存带宽和机架级互联？
- Ethernet、InfiniBand、UALink 等路线如何竞争？
- AVGO 这类公司在 AI 定制芯片和网络芯片中的真实弹性有多大？

### 7. 半导体制造、封测、设备和基础材料层

研究对象：

- 先进制程与晶圆代工
- 先进封装
- 测试设备与封测服务
- 化学品、气体、衬底、光刻胶、硅片、化合物半导体材料

候选公司需要后续逐一验证，包括但不限于：

- TSMC、ASE、Amkor
- ASML、AMAT、LRCX、KLAC、TEL
- AXTI 等材料/衬底候选

核心问题：

- AI 需求会不会改变传统半导体周期？
- 先进封装和测试是否成为比晶圆制造更强的瓶颈？
- 哪些材料环节真正被 AI 拉动，哪些只是主题联想？

### 8. CPO、光模块与高速互联层

研究对象：

- Coherent / COHR
- Lumentum / LITE
- Applied Optoelectronics / AAOI
- Fabrinet、InnoLight 等候选
- CPO、硅光、800G/1.6T 光模块、光电共封装生态

核心问题：

- AI 数据中心网络升级是否会持续推高高速光模块需求？
- CPO 是近期业绩驱动，还是更远期技术路线？
- 光模块公司是否具备持续定价权，还是容易陷入周期和客户集中风险？

### 9. 数据中心、电力、冷却与能源层

研究对象：

- 数据中心运营商
- 电力设备、变压器、配电、UPS、冷却系统
- 燃料电池、核电、铀燃料、SMR 等

候选公司：

- Bloom Energy / BE
- Centrus Energy / LEU
- 新核能与电力基础设施相关公司候选

核心问题：

- AI 数据中心瓶颈是否会从 GPU 转向电力接入和能源供给？
- 哪些电力公司和设备公司能真实捕获 AI CapEx？
- 核能、SMR、燃料电池是长期确定性，还是短期叙事？

### 10. 日韩欧小盘与隐形供应链层

大市值公司更容易被市场识别，但真正具备高弹性的标的可能分布在日韩欧的小盘和中盘公司中，尤其是材料、设备、激光、光子、化合物半导体、封装测试和精密制造环节。

候选方向：

- SOI / Soitec：SOI 晶圆、FD-SOI、RF-SOI、SmartSiC 等材料平台。
- IQE：化合物半导体外延片，可能关联 VCSEL、光通信、射频、传感和先进光电。
- SIVE / Sivers Semiconductors 等候选：毫米波、光子、通信芯片或相关小盘，需要先校验具体公司与代码。
- LPK / LPKF Laser & Electronics：激光微加工、先进封装、PCB、玻璃/半导体加工相关设备候选。
- TOWA：日本封装设备，可能与先进封装、molding、compression molding、AI 芯片封装扩张相关。
- 可扩展日韩欧方向：DISCO、Advantest、Tokyo Electron、Lasertec、SCREEN、Shinko、Ibiden、JSR、Shin-Etsu、SUMCO、Merck KGaA、ASM International、BESI、VAT Group、Aixtron 等。

核心问题：

- 这些公司到底处于 AI Infra 的哪一级供应链：直接瓶颈、二阶受益、周期反转，还是主题映射？
- 它们的客户是否包括 TSMC、Samsung、SK Hynix、Micron、NVIDIA 供应链、光模块厂、封测厂或云数据中心链条？
- AI 相关收入是否能从财报、订单、产能扩张、客户 CapEx 或产品路线中验证？
- 小盘弹性来自长期需求、产能瓶颈、技术壁垒，还是低流动性下的主题重估？

这个层级的研究要特别强调反证：

- 公司没有明确 AI 客户或 AI 收入拆分。
- 收入增长主要来自传统消费电子、汽车、工业周期，而非 AI 数据中心。
- 毛利率和订单没有改善，只是估值跟随 AI 主题上涨。
- 技术路线可能被替代，例如 CPO、硅光、先进封装或材料路线切换。

## 研究方法

详细研究判断基线见：[AI Infra 研究判断基线 / Checklist](docs/research-checklist.md)。

ChatGPT Pro 产出的模块补全保留在本地/private notes 中，只能作为线索，不作为原文证据。

### 1. 先画产业链地图

每次调研先要求 ChatGPT Pro 输出：

- 产业链分层
- 每层核心公司
- 每层价值量和议价权
- 每层主要瓶颈
- 每层观察指标

### 2. 再做单层深挖

每次只研究一层，例如：

- HBM 是否是 AI 训练和推理的长期瓶颈？
- CoWoS 产能约束会持续多久？
- NeoCloud 的商业模式是否可持续？
- CPO 是否会成为下一轮 AI Infra 主线？
- BE、LEU 这类能源标的到底和 AI 数据中心有多强相关性？

### 3. 再做公司卡片

每家公司形成同一模板：

- 公司处于产业链哪一层
- AI 收入占比和增长弹性
- 上游和下游是谁
- 核心驱动
- 主要风险
- 财报中应该看什么指标
- 是否可能是 10 倍股，还是只能作为指数型配置

### 4. 建立评分框架

候选公司按以下维度打分：

- AI 需求相关度
- 产业链位置
- 议价权
- 供需缺口
- 利润弹性
- 客户集中风险
- 技术替代风险
- 资本开支压力
- 估值压力
- 10 倍增长可能性

## GPT Pro 提问路线

### 第一阶段：总图

提示词方向：

> 我正在研究 AI Infra super cycle，请从 Gemini、ChatGPT、Claude 这三个 LLM 产品倒推完整产业链。请按基础模型、框架、算力芯片、HBM、先进封装、云/NeoCloud、网络、光模块、半导体制造、电力能源分层，列出代表公司、核心瓶颈、价值量、议价权、风险和观察指标。

### 第二阶段：关键瓶颈

逐层追问：

- HBM 是否是 AI Infra 最强瓶颈？
- CoWoS 和先进封装的瓶颈持续多久？
- AI 推理扩张会改变 GPU、HBM、网络和电力的价值分配吗？
- NeoCloud 是否能长期赚钱？
- CPO / 光模块是否具备下一轮弹性？
- AI 数据中心电力约束会如何传导到 BE、LEU、核能和电力设备？
- 存储超级周期是 AI Infra 指数增长的开始，还是传统半导体周期反弹？
- 日韩欧小盘材料、激光、光子和封装设备公司，哪些是核心受益，哪些只是主题映射？

### 第三阶段：论文因子

提示词方向：

> 请帮我建立一个 AI Lab/高校顶会论文因子。目标是从 NeurIPS、ICML、ICLR、MLSys 等论文中识别未来 1-3 年 AI Infra 产业链变化信号。请列出应该跟踪的关键词、实验硬件、框架、作者机构、公司合作关系，以及这些信号如何映射到 GPU、HBM、网络、CPO、云和电力产业链。

### 第四阶段：公司筛选

提示词方向：

> 请把以下公司放入 AI Infra 产业链中，判断它们是核心受益、二阶受益、主题映射还是弱相关：NVDA、AVGO、AMD、TSM、ASML、AMAT、LRCX、KLAC、MU、COHR、LITE、AAOI、ORCL、MSFT、AMZN、GOOGL、NBIS、BE、LEU、AXTI。请说明每家公司真实驱动、观察指标和主要反证。

### 第五阶段：10 倍股与指数增长

提示词方向：

> 如果目标是在 AI Infra super cycle 中寻找 10 倍增长或长期指数级增长资产，请不要直接推荐股票，而是给出筛选框架。哪些产业链环节更可能出现 10 倍弹性？哪些更可能是稳定复利？哪些只是短期叙事？请给出判断标准和反证指标。

### 第六阶段：非美小盘与存储周期验证

提示词方向：

> 请围绕日韩欧小盘 AI Infra 供应链建立研究框架。候选包括 SOI/Soitec、IQE、Sivers/SIVE、LPKF/LPK、TOWA，以及日本、韩国、欧洲的半导体材料、激光、光子、封装、测试、化工和精密设备公司。请按产业链位置、客户关系、AI 相关收入证据、技术壁垒、周期风险、10 倍弹性可能性和反证指标做表格。

> 请分析“存储超级周期是否是 AI Infra 指数增长的开始”。请拆分 HBM、DRAM、NAND、企业 SSD、存储控制器、先进封装、测试设备和材料环节，区分 AI 真实拉动、传统周期反转和 AI 叙事映射，并列出需要跟踪的财报指标和产业指标。

## 项目文档

- [docs/research-checklist.md](docs/research-checklist.md)：AI Infra 公司/模块判断基线。
- [docs/source-evidence-template.md](docs/source-evidence-template.md)：原始出处证据卡片模板。
- [docs/credit-financing-evidence-card-template.md](docs/credit-financing-evidence-card-template.md)：Credit / CDS / financing risk evidence card 模板。
- [docs/firm-power-evidence-card-template.md](docs/firm-power-evidence-card-template.md)：Nuclear / firm power evidence card 模板。
- [docs/chatgpt-pro-agent-prompts.md](docs/chatgpt-pro-agent-prompts.md)：ChatGPT Pro 多会话研究任务包。
- [docs/llm-dependency-bfs-framework.md](docs/llm-dependency-bfs-framework.md)：从 LLM Lab 出发的 D0-D5 BFS 产业链研究框架。
- [docs/public-private-boundary.md](docs/public-private-boundary.md)：公开/私有边界。
- [docs/data-security-rules.md](docs/data-security-rules.md)：数据安全规则。

## 当前工作原则

- 不把主题热度等同于真实产业链受益。
- 每个标的必须能回答“它如何从 LLM 需求中赚钱”。
- 每个观点都要能落到财报指标、产能指标、订单指标或技术路线，并记录原始出处。
- 涉及数字时先校验原文：确认公司、期间、口径、单位、币种、同比/环比、GAAP/non-GAAP、segment 定义，再做产业链推论。
- 不能用二手摘要替代公司原文；如果暂时只有二手来源，必须标记为“待原文核验”。
- GPT Pro 用于生成框架和问题清单，最终判断需要用财报、公告、产业报告和技术资料交叉验证。
- 项目目标是形成自己的 AI Infra 投资/产业研究地图，而不是一次性问答。
