# ChatGPT Pro 输出草稿：Scale-up Fabric / Custom ASIC

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 12:18:41 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88/c/6a02a4e5-9ccc-83ea-aa14-4f3a36f4a977  
项目：ai super cycle

## 使用边界

- 本文件是 ChatGPT Pro 对项目研究任务的输出草稿。
- 不作为投资建议、买卖建议或已验证结论。
- 所有涉及财报数字、订单、backlog、CapEx、产能、价格、客户关系、技术路线的内容，必须回到公司原始披露或权威原文核验后才能进入正式研究结论。
- 本文件保留模型输出中的可疑点，后续核验时应优先处理。

## 原始提问

```text
主题5：Scale-up Fabric / Custom ASIC / XPU 供应链研究。

请基于本项目已上传的 md 来源文件，研究 hyperscaler 自研 ASIC、scale-up fabric、PCIe/CXL、NVLink、UALink、Ethernet/InfiniBand 如何改变 AI Infra 价值分配。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 不要简单说“ASIC 替代 GPU”，要拆训练/推理、scale-up/scale-out、software moat、HBM/CoWoS/network 依赖。

重点公司：
NVIDIA、Broadcom、Marvell、Astera Labs、Credo、Arista、Cisco、Rambus、Alphawave、Arm、AMD、Alchip、GUC、Faraday、Synopsys、Cadence、TSMC、ASE。

输出：原始出处清单、Google TPU / AWS Trainium / Microsoft Maia / Meta MTIA 供应链拆解、GPU vs ASIC 价值转移、rack-scale connectivity 价值链、财报验证指标、反证指标。
展开
```

## ChatGPT Pro 输出

根据 2026-05-12 项目 md 的研究框架，本主题属于第一优先级里的 “Custom ASIC 供应链和 scale-up connectivity”，核心问题是 hyperscaler 自研 ASIC 是否把价值扩散到 ASIC design、EDA/IP、HBM/CoWoS、PCIe/CXL 与 network fabric，而不是简单替代 GPU。项目 checklist 明确要求先列原始出处、再区分事实/推论/待核验/反证；涉及收入、订单、客户、产能、毛利率、CapEx、出货量等数据，不能把媒体或模型记忆当结论。

research-checklist

 

2026-05-12-ai-super-cycle-resea…

1. 原始出处清单：先核验这些，再进入结论
主题	必须核验的原始出处	用来证明什么	不能证明什么
Google TPU	Alphabet 10-K/10-Q、Google Cloud TPU/Ironwood/TPU 8t/8i 官方技术资料、Google Cloud Next/Hot Chips/MLSys 资料、GCP AI Hypercomputer 文档	TPU 路线、训练/推理定位、ICI interconnect、HBM/软件栈、GCP 可用性	不能自动证明 Broadcom、TSMC、HBM vendor、OSAT、ODM 的具体份额；这些必须由供应商或客户原文交叉证明
AWS Trainium / Inferentia	Amazon 10-K/10-Q、AWS Trainium 官方页、EC2 Trn2/Trn3 实例页、AWS re:Invent 技术演讲、Neuron SDK 文档	Trainium 芯片规格、NeuronLink、EFA、HBM、训练/推理价格性能、Anthropic/Rainier 相关披露	不能直接证明外部 ASIC 设计服务、foundry、HBM vendor、封装厂份额
Microsoft Maia	Microsoft 10-K/10-Q、Azure Maia 100/200 官方博客、Hot Chips Maia 100 技术资料、Azure AI infrastructure 文档、Microsoft earnings call	Maia 100/200 架构、TSMC/CoWoS-S 是否已披露、Ethernet scale-up、液冷、软件栈、Azure/OpenAI 工作负载	不能证明未披露 HBM vendor、OSAT、ODM、retimer/AEC 供应商
Meta MTIA	Meta 10-K/10-Q、Meta AI/Engineering MTIA 官方博客、ISCA/Hot Chips MTIA papers、Meta infrastructure disclosures	MTIA 用于 Meta 内部 workloads、推荐/广告/LLM 测试、部署规模、软件/工作负载边界	不能证明 Broadcom/TSMC/封装/HBM 的具体商业关系，除非 Meta 或供应商原文披露
NVIDIA GPU / NVLink / InfiniBand / Spectrum-X	NVIDIA 10-K/10-Q、earnings call、GB300 NVL72、Vera Rubin/NVL72、NVLink、Quantum-X800、Spectrum-X、ConnectX/BlueField 官方资料	GPU 系统级价值、NVLink scale-up、InfiniBand/Ethernet scale-out、NIC/DPU、HBM/液冷/rack 架构	不能证明客户最终采用率和单位经济；需要客户 CapEx、订单、network revenue、gross margin 交叉验证
UALink / PCIe / CXL / Ethernet standards	UALink Consortium specs、PCI-SIG PCIe 6/7 specs、CXL Consortium specs、Ultra Ethernet Consortium/OCP Ethernet scale-up workstream、OIF/IEEE/OCP specs	开放 scale-up / host I/O / memory pooling 标准路线、速率、拓扑、生态成员	标准发布不等于量产采用；必须看芯片、系统、客户部署
Broadcom	10-K/10-Q、earnings release/call、Tomahawk 6/Ultra、Jericho、CPO/optical DSP、custom silicon/XPU 官方资料	AI networking、custom AI accelerator、switch ASIC、retimer/AEC/PCIe/optical DSP 是否进收入与毛利	若未点名客户，不能把 Google/Meta/OpenAI/Anthropic 等客户关系当事实
Marvell	10-K/10-Q、custom ASIC、HBM custom compute、PCIe/CXL、optical DSP、OCP 资料	custom ASIC、SerDes、CXL/PCIe、optical DSP、switch/NIC 与 AI infra 关系	不能证明具体 hyperscaler ASIC 设计 win，除非公司披露
Astera Labs / Credo	Astera 10-K/10-Q、Credo 10-K/10-Q、产品资料、earnings call	PCIe/CXL retimer/switch、AEC、高速连接芯片是否被 AI rack attach 拉动	不能证明每个客户 rack 的 attach rate，除非客户/公司披露
Arista / Cisco	10-K/10-Q、investor presentation、AI networking product docs、earnings call	800G Ethernet AI cluster、front-end/back-end AI network、EOS/SONiC/UEC/OCP 生态	不能证明所有增长来自 AI；需拆 cloud titan、enterprise、service provider
Rambus / Alphawave / Arm / Synopsys / Cadence	年报/20-F/10-K、IP product briefs、EDA backlog/RPO、design win commentary	HBM PHY/controller、CXL、SerDes、Arm IP、EDA/verification 在 XPU 设计中的价值	不能证明单一 ASIC 客户量产收入，除非合同/royalty 披露
Alchip / GUC / Faraday	年报、月营收、法说会、客户集中度、NRE/turnkey/mass production 拆分、advanced node commentary	台湾 ASIC design service 是否受 custom XPU tape-out 拉动	客户名、CoWoS/HBM allocation、最终芯片归属通常高度不透明，默认待核验
TSMC / ASE	TSMC annual report、quarterly results、technology symposium、CoWoS/SoIC/advanced packaging 资料；ASE annual report、advanced packaging/test disclosures	N3/N2、CoWoS、SoIC、advanced packaging/test 是否成为 GPU/ASIC 共用瓶颈	不能证明某 hyperscaler ASIC 具体产能份额，除非客户或供应商原文披露

项目 md 已把网络互连拆成 scale-up fabric、scale-out fabric、InfiniBand、Spectrum-X Ethernet、Ethernet switch ASIC、NIC/DPU、PCIe retimer、CXL switch、NVLink、UALink、AEC cable，并把关键验证指标列为 800G/1.6T ports、switch radix、latency、packet loss、power/bit、AEC attach、PCIe/CXL generation、NVLink/UALink adoption 等。

2026-05-12-ai-super-cycle-resea…

2. Google TPU / AWS Trainium / Microsoft Maia / Meta MTIA 供应链拆解
2.1 Google TPU
层级	状态	研究判断
原文已证明	Google 官方称 Ironwood 是第七代 TPU，面向 inference，并可扩展到 9,216 个液冷芯片，通过 ICI interconnect 连接，接近 10MW 级系统；Google 2026 技术博客还披露 TPU 8t/8i 与 AI Hypercomputer 软件栈 JAX、PyTorch、vLLM、XLA、Pathways 集成。
blog.google
+1
	Google TPU 不是单颗芯片问题，而是 “ASIC + ICI + HBM + liquid-cooled pod + compiler/runtime + GCP service” 的系统级平台。
合理推论	Google 自研 ASIC 会把一部分价值从 merchant GPU 转为内部平台收益；但它仍然消耗 HBM、先进制程、先进封装、EDA/IP、SerDes/interconnect、测试与数据中心电力。	对外部供应链的受益点更可能在 foundry、HBM、advanced packaging、EDA/IP、SerDes、测试、rack power/cooling，而不是 “TPU 本体毛利”。
待原文核验	Broadcom 是否参与 TPU ASIC physical design / custom silicon；TSMC 节点与 CoWoS 份额；HBM vendor；OSAT/测试/ODM；retimer/AEC/optics supplier。	这些不能用媒体或市场传闻写入结论，必须由 Alphabet、Broadcom、TSMC、memory vendor、OSAT 或监管文件交叉证明。
主要反证	TPU 若主要用于 Google 内部 workload，公开供应链收入可能被内化；若 GCP 客户 adoption 不高，TPU 对外部生态的价值分配有限；若 NVIDIA GPU 在训练和通用推理继续保持软件优势，TPU 分流可能只限于 Google workloads。	重点反证是 GCP TPU utilization、客户采用、软件生态、非 Google workload 迁移难度。
2.2 AWS Trainium
层级	状态	研究判断
原文已证明	AWS 官方 Trn2 UltraServers 使用 NeuronLink 连接 64 个 Trainium2 chips，并披露 Trn2 instance/UltraServer 的 HBM3、EFAv3、FP8 compute 与 scale-out networking；AWS Trainium 官方页还称 Trainium3 是 AWS 第一颗 3nm AI chip，并披露 HBM3e memory 与 bandwidth 指标。
Amazon Web Services, Inc.
+1
	AWS 的路径是 “Trainium chip + NeuronLink scale-up + EFA scale-out + Neuron software + EC2/Bedrock”。它更像垂直云平台优化，而不是独立芯片销售。
合理推论	Trainium 成功会把一部分推理/训练 token economics 价值内化到 AWS，同时外溢到 HBM、3nm foundry、advanced packaging、EFA/NIC/networking、rack integration。	对 Broadcom/Marvell/Alchip/GUC 等外部 ASIC 服务商的直接受益不能默认成立，AWS Annapurna 自研能力强，供应链归属需原文验证。
待原文核验	Trainium2/3 foundry、advanced packaging、HBM vendor、substrate、test、ODM、EFA/NIC components、NeuronLink PHY/IP 供应商。	供应链拆解目前只能按功能层拆，不能按公司份额确认。
主要反证	Neuron 软件生态若跟不上 PyTorch/CUDA 工作流，Trainium 可能局限在 AWS 内部和少数客户；如果 GPU 价格下降或供给充足，Trainium 的 price-performance 优势需重新验证；若 Anthropic/AWS workload 未如期规模化，Trainium volume 会受影响。	重点看 Trn instances utilization、Bedrock adoption、Neuron developer traction、AWS AI CapEx 回收。
2.3 Microsoft Maia
层级	状态	研究判断
原文已证明	Microsoft Azure 博客披露 Maia 100 是 Azure 自研 AI accelerator，使用 TSMC 5nm 和先进封装，面向云端 AI workloads；同一资料披露 Maia 100 采用自定义 rack-level power、Ethernet-based network protocol、液冷，并与 PyTorch、ONNX Runtime、Triton 等软件栈集成。
Azure
	Maia 明确是 “silicon to software to systems” 的垂直系统，不只是芯片替代。
原文已证明	Hot Chips Maia 100 技术资料披露 Maia 100 package/interposer technology 为 TSMC CoWoS-S，HBM2E 64GB、1.8TB/s，后端网络 12×400GbE，host PCIe Gen5×8。
热芯片 2024
+1
	这是少数可从原文直接确认 TSMC CoWoS-S 与 HBM/400GbE 的 hyperscaler ASIC。
原文已证明	Microsoft 2026 Maia 200 官方资料称 Maia 200 面向 compute-intensive inference，采用 3nm，使用 scalable networking architecture，并可通过 standard Ethernet 扩展到 6,144 个 accelerators。
Source
+1
	Microsoft 路线对 Ethernet scale-up、SerDes、retimer/AEC、switching、liquid cooling 的验证价值较高。
合理推论	Maia 对 NVIDIA 的影响更可能先发生在 Microsoft/OpenAI/Azure 内部 inference economics，而不是全面替代训练 GPU。	对外部供应链最重要的是 TSMC/CoWoS、HBM、Ethernet PHY/SerDes、EDA/IP、liquid cooling、rack integration。
待原文核验	HBM vendor、OSAT、test、substrate、retimer/AEC、Ethernet switch/NIC 供应商；Maia 200 的具体 foundry/packaging/HBM vendor。	Maia 100 的 TSMC CoWoS-S 已有技术资料锚点，但供应商清单仍需扩展核验。
主要反证	Maia 若只支持有限模型或内部 workload，外部生态价值有限；Ethernet scale-up 若延迟/一致性不足，可能难以替代 NVLink 域；OpenAI workload 若继续高度依赖 NVIDIA，Maia 分流有限。	重点看 Azure AI margin、OpenAI/Microsoft workload mix、Maia deployment regions、SDK adoption。
2.4 Meta MTIA
层级	状态	研究判断
原文已证明	Meta 官方称 MTIA 是 Meta 自研 custom-made chips，服务 Meta AI workloads，下一代 MTIA 相比 v1 有性能提升，并帮助支撑 ranking 与 recommendation ads models。
AI.Meta
	MTIA 目前更像 Meta 内部推荐/广告/推理效率优化平台，不应直接等同于 frontier training GPU 替代。
原文已证明	Meta 2026 官方博客称前两代 MTIA，即 MTIA 100 与 MTIA 200，已有 ISCA 论文披露，并称已在生产中部署数十万颗 MTIA chips，接入多个内部 production models，并用 Llama 做过测试。
AI.Meta
	这强化了 “内部规模化推理 ASIC” 的真实性，但不自动证明外部供应链公司收入。
合理推论	Meta ASIC 若继续扩展，可能把 recommendation、ranking、部分 Llama inference 的单位成本降低，同时加大对 HBM/DRAM、networking、EDA/IP、advanced node、rack power 的需求。	对 merchant GPU 的影响更偏 inference/offload，不是 frontier training 的直接替代。
待原文核验	MTIA foundry、封装、HBM/DRAM、ASIC design partner、networking、server/ODM、test supplier。	Broadcom/TSMC 等关系不能用市场记忆直接下结论。
主要反证	Meta 训练 frontier Llama 仍可能依赖 GPU；MTIA 可能局限在 Meta 内部推荐模型；若内部模型变化快，ASIC 灵活性不如 GPU。	重点看 Meta CapEx、AI infrastructure commentary、MTIA deployment 和 NVIDIA/AMD GPU procurement 并行程度。
3. GPU vs ASIC：价值转移不是“替代”，而是分工变化
维度	原文已证明	合理推论	待核验	主要反证
训练	NVIDIA GB300 NVL72 是液冷 rack-scale 架构，集成 72 个 Blackwell Ultra GPU、36 个 Grace CPU，并使用 NVLink、Quantum-X800 InfiniBand 或 Spectrum-X Ethernet、ConnectX-8 SuperNIC。
NVIDIA
	Frontier training 对 GPU 的通用性、CUDA/NCCL、NVLink/NVSwitch、InfiniBand/Spectrum-X、HBM 容量/带宽依赖仍强。ASIC 更适合 hyperscaler 在稳定、大规模、可控 workload 上优化 TCO。	各 hyperscaler 训练 workload 中 GPU/TPU/Trainium/Maia/MTIA 的真实占比。	CUDA/NVLink 生态继续扩大；ASIC 编译器、调度、模型支持不足；训练模型架构变化太快导致 ASIC amortization 失败。
推理	Google Ironwood、AWS Trainium3、Microsoft Maia 200 都把 inference / token economics / energy efficiency 作为核心叙事。
blog.google
+2
Amazon Web Services, Inc.
+2
	推理是 ASIC 最可能分流 GPU 的区域，尤其是高吞吐、稳定模型、内部 workloads、推荐/广告/agent serving。	每 token 成本、tokens/W、latency、batching、compiler/runtime 稳定性、客户 adoption。	推理模型快速迭代、长上下文/MoE/多模态变化快，GPU 灵活性胜出；GPU 供给改善导致 ASIC TCO 优势收窄。
Scale-up	NVIDIA 用 NVLink/NVSwitch 建立封闭高带宽低延迟域；Google 用 ICI，AWS 用 NeuronLink，Microsoft Maia 走 Ethernet-based / standard Ethernet scale-up，UALink 试图提供开放 accelerator-to-accelerator 标准。UALink 200G 1.0 官方定义 accelerator 与 switch 之间的低延迟高带宽互连，并支持 pod 内最高 1,024 accelerators。
UALink Consortium
	价值从单卡 FLOPS 转向 rack/pod 内通信、collective operations、memory movement、power/bit。	UALink、Scale-Up Ethernet、CXL memory pooling 的实际 silicon 和系统量产。	NVLink 继续锁住最大训练/推理系统；开放标准延迟、功耗、可靠性、软件栈不达标。
Scale-out	NVIDIA Quantum-X800 是 800Gb/s InfiniBand 平台，含 switch、ConnectX SuperNIC、cables/transceivers，并支持 SHARP、adaptive routing、congestion control；Spectrum-X 是 AI Ethernet 平台，面向 AI fabrics 和 RDMA/RoCE。
NVIDIA
+1
	AI cluster 扩大后，Ethernet/InfiniBand、switch ASIC、NIC/DPU、optics、AEC、telemetry 的价值量上升。	Arista/Cisco/Broadcom/Marvell/NVIDIA 在 AI backend vs frontend 的真实端口份额。	Ethernet 价格战、光模块 ASP 下行、客户自研网络、网络不是主要瓶颈。
HBM / CoWoS / foundry	Maia 100 已披露 TSMC CoWoS-S 与 HBM2E；AWS Trainium2/3 披露 HBM3/HBM3e；NVIDIA GB300 披露高容量 HBM3E 架构。
NVIDIA
+3
热芯片 2024
+3
Amazon Web Services, Inc.
+3
	ASIC 并不会消除 HBM/CoWoS/先进制程需求，反而可能把更多 hyperscaler silicon 拉进相同瓶颈池。价值从 GPU 芯片毛利部分外溢到 HBM、CoWoS、EDA/IP、SerDes、测试、基板。	每个 ASIC 的 HBM stack count、CoWoS/SoIC 用量、die size、yield、wafer allocation。	HBM/CoWoS 成为所有 GPU/ASIC 共同瓶颈，导致 ASIC 无法快速放量；封装成本压缩 ASIC TCO 优势。
Software moat	Microsoft Maia 披露 PyTorch、ONNX Runtime、Triton；Google TPU 8t/8i 披露 JAX、PyTorch、vLLM、XLA、Pathways；AWS Trainium 支持 PyTorch/JAX 和 Neuron stack。
Azure
+2
Google Cloud
+2
	软件栈决定 ASIC 能否从内部 workload 扩展到外部客户。GPU 护城河不只在芯片，而在 CUDA/NCCL/TensorRT/network/system orchestration。	SDK developer adoption、model coverage、kernel availability、failure rate、debuggability。	ASIC 软件生态弱，开发者迁移成本高；GPU utilization 改善后单位成本下降。

结论分层：原文已证明的是 hyperscaler ASIC 真实存在且向 rack/pod/network/software 系统化发展；合理推论是价值会从单颗 GPU 扩散到 HBM、CoWoS、EDA/IP、SerDes、retimer/AEC、switch/NIC、rack power/cooling；待核验的是每一家供应商的实际客户、份额、NRE 与量产收入；主要反证是 GPU 软件/系统 moat 继续压倒开放 ASIC 生态，或者 ASIC 放量被 HBM/CoWoS/软件卡住。

4. Rack-scale connectivity 价值链
层级	价值链功能	代表公司	状态判断
Package / die-to-die / HBM interface	HBM controller/PHY、UCIe/die-to-die、chiplet fabric、RDL/interposer、CoWoS/SoIC、substrate	Rambus、Alphawave、Synopsys、Cadence、Arm、Marvell、Broadcom、TSMC、ASE、Alchip、GUC、Faraday	高确定需求，供应商份额待核验。 Rambus 官方披露 HBM4/HBM4E controller IP 面向 AI/ML、graphics、HPC，最高速率覆盖 HBM4E/4/3E/3。
Rambus
+1

Chip-to-chip scale-up	NVLink/NVSwitch、ICI、NeuronLink、UALink、Scale-Up Ethernet、collectives / all-reduce offload	NVIDIA、Google、AWS、Microsoft、Broadcom、AMD、Astera、Cisco、Synopsys	最关键价值迁移层。 Broadcom Tomahawk Ultra 官方称面向 HPC 和 AI scale-up，支持 sub-400ns XPU-to-XPU latency，并把 in-network collectives 放入 switch chip。
Broadcom Inc.

Host I/O / PCIe / CXL	PCIe retimer/switch、CXL memory expansion/pooling、PCIe Gen6/7、rack-level memory pooling	Astera Labs、Marvell、Broadcom、Rambus、Alphawave、Synopsys、Cadence	AI rack attach-rate 是核心。 CXL 4.0 官方称 bandwidth 从 64GT/s 到 128GT/s，并增加 bundled ports 与 memory RAS；PCI-SIG 披露 PCIe 7.0 已于 2025 年发布给成员，128GT/s、x16 双向最高 512GB/s。
Compute Express Link -
+1

Scale-out backend network	InfiniBand、Spectrum-X Ethernet、Tomahawk/Jericho/Silicon One/Teralynx、NIC/DPU/SuperNIC、RoCE、UEC	NVIDIA、Broadcom、Marvell、Arista、Cisco、AMD Pensando	从“网络设备”升级为 AI workload completion time 变量。 Arista 官方称其 800G AI spine 和 distributed Etherlink 可支持大规模 accelerator 网络，并强调开放 Ethernet/IP 生态。
Arista Networks

Electrical / optical physical layer	SerDes、retimer、AEC、DAC、optical DSP、800G/1.6T optics、CPO/NPO、connectors	Credo、Marvell、Broadcom、Astera、Coherent、Lumentum、Arista/Cisco ecosystem、Amphenol、TE、Molex	功耗/距离/密度驱动价值。 Broadcom OFC 2026 原文列出 400G/lane optical DSP、200G/lane Ethernet retimers/AEC、PCIe Gen6 switch/retimer、102.4T Ethernet switch with CPO 等 AI connectivity portfolio。
Broadcom Inc.

System / OS / telemetry	EOS、SONiC、Cumulus、Mission Control、fabric manager、telemetry、congestion control	Arista、Cisco、NVIDIA、Broadcom ecosystem、Marvell ecosystem	软件化网络是毛利与锁定来源。 单纯硬件端口增长不够，需验证 software attach、support contracts、AI cluster reliability。

项目 md 已把 Custom ASIC/XPU、chiplet/先进互连 IP、scale-up 互连列为独立模块，并把 tape-out 数、NRE、CoWoS 配额、IP 授权、SerDes 速率、lane 数、attach rate、ASP 作为关键验证指标。

2026-05-12-chatgpt-pro-module-m…

5. 公司层面的价值捕获假设
公司	可能捕获的价值	原文已证明 / 合理推论 / 待核验
NVIDIA	GPU、NVLink/NVSwitch、ConnectX/SuperNIC、Quantum-X800 InfiniBand、Spectrum-X Ethernet、BlueField、software stack	原文已证明：GB300 NVL72 是 72 GPU rack-scale、NVLink 130TB/s、HBM3E、ConnectX-8、InfiniBand/Spectrum-X 系统。合理推论：NVIDIA 的 moat 是 full-stack，而不是单卡。
Broadcom	Custom XPU、Tomahawk/Jericho switch ASIC、Scale-Up Ethernet、CPO、optical DSP、retimer/AEC、PCIe	原文已证明：Tomahawk 6 production volume、102.4Tbps、面向 scale-out 和 scale-up AI networks；Tomahawk Ultra 面向 AI scale-up，支持 in-network collectives。
Broadcom Inc.
+1
 待核验：具体 hyperscaler custom XPU 客户和收入拆分。
Marvell	Custom ASIC、SerDes、PCIe/CXL、optical DSP、AEC、switch/NIC、advanced packaging	原文已证明：Marvell custom ASIC 页面披露 5nm/3nm data infrastructure IP、SerDes、PCIe Gen6/CXL 3.0、Arm subsystems、multi-chip packaging；公司主页称 custom silicon 和 interconnect/network switch 服务 AI infrastructure。
Marvell Technology
+1
 待核验：具体客户和 AI ASIC ramps。
Astera Labs	PCIe/CXL retimer、connectivity platform、rack-level connectivity	待原文核验为主：需要用 10-K/10-Q 证明 AI cloud customer revenue、product mix、客户集中度、gross margin。SEC 10-K 是首要出处。
美国证券交易委员会

Credo	AEC、SerDes、DSP、高速电连接	待原文核验为主：需看 10-K、AEC data center revenue、客户集中度、800G/1.6T mix。Credo IR 已列 Form 10-K 原始文件。
Credo Technology Group

Arista	AI Ethernet systems、EOS、800G spine、distributed AI fabric	原文已证明：Arista 官方披露 800G AI spine、Etherlink、EOS、开放 Ethernet/IP AI networking。待核验：AI revenue、cloud titan 客户、gross margin、supply constraints。
Arista Networks

Cisco	Silicon One、Ethernet systems、UALink/UEC/OCP ecosystem、optics	合理推论：受益于 Ethernet AI fabric 标准化。待核验：AI backend revenue、Silicon One design wins、webscale 客户。
Rambus	HBM controller/PHY、CXL/memory interface IP	原文已证明：HBM controller IP 覆盖 HBM4E/4/3E/3，面向 AI/ML/HPC。待核验：royalty、license revenue、design win 转化。
Rambus

Alphawave	SerDes IP、chiplets、custom silicon、connectivity DSP	原文已证明：公司披露 Silicon IP、chiplets、custom silicon、data center connectivity。待核验：AI customer design wins、royalty、现金流质量。
Alphawave Semi

Arm	CPU subsystem、Neoverse、SoC IP、royalty	合理推论：hyperscaler ASIC 和 Grace/Vera 类 CPU+accelerator 系统增加 Arm IP relevance。待核验：data center royalty、license backlog、customer concentration。
AMD	Instinct GPU、ROCm、EPYC、Pensando、UALink/open ecosystem	合理推论：既是 GPU challenger，也是开放 scale-up 生态参与者。待核验：ROCm adoption、MI300/MI350/MI400 revenue、UALink design wins。
Alchip / GUC / Faraday	ASIC design service、NRE、turnkey、advanced node design	待原文核验：不能默认绑定 Google/AWS/MSFT/Meta。必须看 NRE、客户集中、N3/N2/CoWoS/HBM commentary、mass production revenue。
Synopsys / Cadence	EDA、verification、IP、SerDes/CXL/PCIe/HBM design enablement	合理推论：custom ASIC tape-out 增加 EDA/verification/IP demand。待核验：EDA backlog/RPO、AI chip design customer commentary、IP revenue。
TSMC	N3/N2 foundry、CoWoS/SoIC、advanced packaging	原文已证明部分来自 Maia 100 技术资料：Maia 100 使用 TSMC N5 与 CoWoS-S。合理推论：GPU 和 ASIC 都竞争先进节点/CoWoS。
ASE	OSAT、advanced packaging、test	待原文核验：需看 advanced packaging/test revenue、AI/HPC commentary、customer concentration。
6. 财报验证指标
公司/环节	必看指标	强信号	弱信号 / 误判风险
NVIDIA	Data Center compute vs networking revenue、gross margin、NVLink/NVSwitch、ConnectX/IB/Ethernet attach、HBM/Blackwell/Rubin supply、inventory	networking revenue 与 GPU 出货同步上升；NVL rack 系统交付放量；毛利率稳定	GPU 出货强但 networking attach 低；客户转向白盒 Ethernet；margin 被系统复杂度压缩
Broadcom	AI semiconductor revenue、custom XPU revenue、Tomahawk/Jericho/SUE/retimer/optical DSP、customer concentration、backlog	custom ASIC 与 AI Ethernet 同时增长；Tomahawk 6/Ultra 量产拉动毛利	只有 AI 叙事，无客户/收入拆分；switch ASIC 价格战；单一 hyperscaler 集中
Marvell	Data center revenue、custom silicon NRE/production、optical DSP、PCIe/CXL/retimer/AEC、gross margin	NRE 转量产，custom ASIC + interconnect 同步增长	NRE 一次性；AI revenue 被普通 data center recovery 混淆
Astera	PCIe/CXL product revenue、hyperscaler concentration、gross margin、inventory、new protocol attach	PCIe/CXL attach rate 随 AI rack 提升，毛利率不塌	客户集中、库存提前拉货、retimer 被集成/替代
Credo	AEC revenue、800G/1.6T mix、customer concentration、gross margin、operating leverage	AEC 成为液冷 AI rack 默认连接方案，收入和毛利同步提升	AEC ASP 下滑；CPO/NPO 或直接铜/光方案替代
Arista / Cisco	AI Ethernet revenue、400G/800G port mix、cloud titan revenue、backlog、gross margin、supply constraints	backend AI network 明确放量，software/support attach 提升	enterprise refresh 被误认为 AI；switch gross margin 被供应链成本挤压
Rambus / Alphawave / Arm	IP license、royalty、HBM/CXL/SerDes design wins、royalty conversion、customer concentration	AI accelerator IP design win 进入 royalty，license + royalty 双增	只有 license 没有量产 royalty；客户自研 IP 替代
Synopsys / Cadence	EDA backlog/RPO、verification、IP revenue、advanced node/custom ASIC activity	custom ASIC tape-out 增加，verification complexity 拉动长期合同	AI 设计需求被大客户内部化；EDA 已充分反映
Alchip / GUC / Faraday	NRE、turnkey mass production、advanced node mix、customer concentration、working capital、gross margin	NRE 转高毛利量产，先进节点客户增加	单一客户失败；NRE 不能转 production；毛利率被 foundry/客户压缩
TSMC	HPC revenue、N3/N2 utilization、CoWoS/SoIC capacity、CapEx、gross margin	GPU + ASIC 同时抢先进节点与 CoWoS，产能利用率高	CoWoS 扩产后价格下降；客户砍单；先进节点 capex 回收慢
ASE	Advanced packaging/test revenue、AI/HPC customer commentary、CapEx、utilization、margin	OSAT 从传统封测升级到 advanced packaging/test bottleneck	大部分先进封装价值被 TSMC 内化；ASE 只拿低毛利后段量
7. 反证指标：哪些信号会推翻本主题

ASIC 放量失败：Google/AWS/Microsoft/Meta 自研芯片只停留在小规模内部 workload，未出现持续部署、外部客户 adoption 或明确 CapEx/收入传导。

软件 moat 反向强化 GPU：CUDA/NCCL/TensorRT、NVLink/NVSwitch、NVIDIA networking 继续降低端到端成本，hyperscaler ASIC 的编译器、runtime、debugging、model coverage 无法追上。

Scale-up 开放标准未量产：UALink、Scale-Up Ethernet、CXL memory pooling 有规格但没有大规模 silicon/rack deployment；实际低延迟 all-reduce、collectives、fault isolation 不及 NVLink。

HBM / CoWoS 成为共同瓶颈：ASIC 与 GPU 都被同一 HBM/CoWoS/advanced packaging 约束，ASIC 不能靠自研绕开瓶颈，反而增加供应链竞争。

网络价值被价格战吞掉：800G/1.6T port 出货增长，但 Arista/Cisco/Broadcom/Marvell/Credo/Astera 毛利率下行，说明价值量上升没有转化为利润池迁移。

NRE 无法转量产：Alchip/GUC/Faraday/Marvell/Broadcom 等若只有 NRE 或 design activity，没有 recurring production revenue 和毛利率改善，custom ASIC 主题容易变成一次性项目。

客户集中风险暴露：Astera、Credo、ASIC design service、switch ASIC 或 IP 公司若收入高度依赖 1–2 个 hyperscaler，一旦客户架构切换，收入和库存会快速反转。

AI CapEx 放缓或利用率不足：hyperscaler CapEx 增速放缓、cloud margin 被折旧压垮、AI cluster utilization 不足，会同时打击 GPU、ASIC、network、retimer、AEC、CoWoS、HBM。

CPO/NPO/光互连路线改变电连接价值：若 CPO/NPO 提前成熟，AEC、retimer、front-panel optics、部分 copper backplane 的价值量假设需要重估；反过来若 CPO 延后，pluggable optics 和 AEC 延寿。

训练/推理 workload 结构变化：小模型、蒸馏、量化、KV cache 优化、MoE routing 改善若让每 token hardware intensity 下降快于 token demand 增长，整个 rack-scale connectivity 弹性会下降。项目 md 也明确把软件效率变化列为可能降低硬件需求的反证。

2026-05-12-ai-super-cycle-resea…

8. 核心结论

本主题的正确表述不是 “ASIC 替代 GPU”，而是：

Hyperscaler 自研 ASIC 把 AI Infra 价值从单一 merchant accelerator 扩展为多层系统竞争：训练端 NVIDIA full-stack moat 仍强；推理端 hyperscaler ASIC 更容易获得内部 TCO 优势；scale-up fabric、scale-out network、HBM、CoWoS、EDA/IP、SerDes、retimer/AEC、switch/NIC 和 rack power/cooling 成为共同增量利润池。

因此，下一步不应直接问 “谁替代 NVIDIA”，而应逐季核验四件事：第一，ASIC 是否从 NRE 进入量产；第二，HBM/CoWoS/advanced node 是否跟随 ASIC 放量；第三，rack-scale connectivity 的 attach rate 和毛利率是否上升；第四，开放 fabric 是否真正挑战 NVLink，而不是只停留在规格发布。
