# ChatGPT Pro 模块补全目录 v1

来源：专用自动化 Chrome profile，ChatGPT Pro，`ai super cycle` 项目，`AI Infra 产业链分析` 会话。

人工校正：本文件只作为模块线索，不作为证据。已按项目目标删去非核心企业应用落地项；后续凡涉及财务、订单、产能和客户数据，必须回到公司原始披露核验。

## 一级模块清单

| 模块 | 核心子环节 / 公司地区 | 和 LLM 的关系 | 关键验证指标 | 主要反证 |
| --- | --- | --- | --- | --- |
| 数据供应链 | 数据授权、标注、合成数据、版权清洗；Scale AI、Appen、TELUS、Adobe/Shutterstock | 训练数据质量决定模型上限 | 数据授权合同、合成数据占比、RLHF 量 | 模型转向小数据/自博弈 |
| LLMOps / 推理编排 | 网关、路由、缓存、限流、A/B eval；Datadog、Cloudflare、LangSmith、Helicone | 降低每 token 成本 | token 量、延迟、缓存命中率 | 模型厂商内置吞噬 |
| 模型评测与安全 | eval、红队、guardrail、内容安全；Scale、HiddenLayer、Lakera、Robust Intelligence | AI 上线前置合规层 | 企业采购、监管要求 | 开源工具 commoditize |
| 模型压缩 / 编译优化 | 量化、蒸馏、稀疏、MoE routing；OctoML、Modular、Anyscale、Neural Magic | 同算力跑更多推理 | tokens/GPU、吞吐提升 | GPU 供给过剩削弱需求 |
| 推理专用芯片 | ASIC、NPU、边缘 AI；Groq、Tenstorrent、SambaNova、Hailo、Untether | 推理成本曲线核心 | 单 token 能耗、客户 POC 转量产 | CUDA 生态锁死 |
| Custom ASIC / XPU | Broadcom、Marvell、Alchip、GUC、Socionext | 超大厂自研降低 NVIDIA 依赖 | tape-out 数、NRE、CoWoS 配额 | 设计失败或客户集中 |
| Chiplet / 先进互连 IP | UCIe、SerDes、HBM PHY、CXL IP；Arm、Rambus、Credo、Alphawave、Synopsys | AI 芯片 I/O 与带宽瓶颈 | IP 授权、SerDes 速率、royalty | 集成方案减少外购 IP |
| Scale-up 互连 | NVLink、UALink、PCIe Gen6/7、CXL、retimer；Astera Labs、Parade、Montage、Credo | 机柜内 GPU 池化 | attach rate、lane 数、ASP | NVLink 封闭生态独占 |
| DPU / SmartNIC | NVIDIA BlueField、AMD Pensando、Marvell、Napatech | 网络/安全/存储卸载 | DPU attach rate、NIC 带宽 | CPU/GPU 直接处理 |
| AI 服务器 ODM/OEM | Quanta、Wiwynn、Wistron、Foxconn、Inventec、Supermicro、Celestica | GPU 变成可交付集群 | AI 服务器收入、毛利、交付周期 | 代工毛利被压缩 |
| 机柜级电源 | 48V 架构、VRM、power module、BBU；Delta、Lite-On、Vicor、Infineon、Monolithic | 高功率机柜供电瓶颈 | kW/rack、转换效率、订单 | 架构标准变化 |
| 液冷 / 热管理 | cold plate、CDU、泵阀、TIM、浸没液；Vertiv、CoolIT、Aavid、Boyd、Ferrotec | 高功率 AI 机柜必需 | 液冷渗透率、CDU 出货 | 风冷延寿、价格战 |
| PCB / CCL / 背板 | AI 服务器 PCB、高速材料；Unimicron、Ibiden、Shinko、Nan Ya、Tripod、ITEQ | 高速信号与电源完整性 | 层数、低损耗材料占比 | 良率提升导致 ASP 下滑 |
| ABF / 封装基板细分 | Ajinomoto、Ibiden、Shinko、Unimicron、Kinsus、AT&S | GPU/HBM 封装底座 | ABF 供需、交期、价格 | CoWoS 外瓶颈转移 |
| 测试 / ATE / Burn-in | Advantest、Teradyne、FormFactor、Chroma、MPI、Cohu | HBM/GPU 良率与交付 | HBM 测试时长、tester 订单 | 测试时间缩短 |
| 光器件深水区 | EML/DFB 激光、TIA、driver、FAU、TFLN；Sivers、IQE、Lumentum、Macom、Hamamatsu | 800G/1.6T 网络升级 | 1.6T 放量、硅光渗透 | 铜缆/DAC 延寿 |
| Fab 基础设施 | 洁净室、真空、气体、除害、化学品；ULVAC、Edwards、Ebara、DAS、Air Liquide、Linde | 先进制程扩产配套 | fab capex、设备交期 | 晶圆厂延后扩建 |
| 特种材料 / 化工 | 光刻胶、CMP slurry、precursor、PI；JSR、TOK、Shin-Etsu、Resonac、Merck KGaA、Soulbrain | 制程良率与封装材料 | 单晶圆材料成本、认证 | 客户自有/替代材料 |
| 电网接入与电力设备 | 变压器、开关柜、UPS、储能；Hitachi Energy、ABB、Siemens、Hyosung、LS Electric | 数据中心建设硬约束 | 交期、backlog、grid queue | 电力审批放缓 |

## 优先深挖主题

1. HBM 外溢链：测试、封装基板、材料、良率设备。
2. 机柜级电源：48V、VRM、BBU、GaN/SiC 功率器件。
3. 液冷与热管理：CDU、冷板、泵阀、TIM、浸没液。
4. AI 网络深水区：retimer、SerDes、DPU、光器件、硅光。
5. Custom ASIC/XPU 供应链：Alchip、GUC、Broadcom、Marvell、Socionext。
6. AI 服务器 ODM 与高速 PCB/CCL：台湾、日本、奥地利链条。
7. Fab 基础设施/特气/真空/除害/洁净室：日韩欧隐形供应商。
8. 数据中心电网接入：变压器、开关柜、UPS、储能、燃气/核能配套。

## 下一轮 Prompt 标题

1. 请拆解 HBM 外溢供应链：测试、封装、材料、设备标的池。
2. AI 机柜电源从 12V 到 48V：产业链、公司、验证指标。
3. 液冷/热管理是否会出现 10 倍弹性小盘股？
4. AI 网络非 GPU 链：SerDes、retimer、DPU、光器件全图。
5. Custom ASIC/XPU 会如何重构 NVIDIA 之外的供应链？
6. 台湾 AI 服务器 ODM、PCB、CCL、基板公司系统梳理。
7. 日韩欧半导体材料/设备隐形冠军清单。
8. 数据中心电力瓶颈：变压器、UPS、储能、核能与燃气链条。
