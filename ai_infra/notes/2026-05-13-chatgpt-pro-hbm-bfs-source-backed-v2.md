# ChatGPT Pro HBM BFS Source-Backed Discovery v2

Status: ChatGPT Pro output, pending original-source verification
Conversation URL: https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a03e57e-b87c-83ea-8f35-a9e9f85c5bd7
Captured at: 2026-05-13

1. Source checklist first

证据状态口径：本次把公司年报、10-K/20-F、季报、earnings release、earnings call、investor presentation、监管公告、公司技术页作为优先来源；媒体、券商、数据库或模型输出只能作为线索，不能直接进入结论。项目 checklist 也要求记录来源、报告期、口径、原文能证明什么、不能证明什么，并把推导判断单独标为“推论”。

research-checklist

公司 / 组别	应核验的公司原文来源	必查关键词 / 指标	本轮证据状态
OpenAI / Stargate	OpenAI infrastructure posts；Oracle / SoftBank / site partner cross-disclosure	Stargate, AI data center, GW, GPU, HBM, capex, supplier, timeline	原文已证明：OpenAI 已披露 Stargate 多站点和 GW/投资承诺；对 HBM 供应商的直接传导仍是合理推论 / 待原文核验。
OpenAI

Microsoft / Azure	FY26 Q3 earnings, 10-Q, call transcript, capex notes	AI infrastructure, GPU, short-lived assets, capex, cloud margin, RPO	原文已证明：FY26 Q3 capex 大幅用于 cloud/AI infrastructure；HBM 传导为合理推论，需通过 NVIDIA / memory vendor 交叉披露核验。
微软
+1

Alphabet / Google / DeepMind / Gemini	Alphabet Q1 2026 earnings, 10-Q, Cloud Next technical pages, TPU pages	technical infrastructure, TPU, Ironwood, AI inference, servers, capex, HBM, HBM4	原文已证明：Alphabet 披露 Q1 2026 capex 主要投向 AI technical infrastructure；Google 披露 Ironwood TPU 与 AI inference 定位。HBM attach 仍需 TPU / supplier 原文交叉核验。
abc.xyz
+1

Meta	Q1 2026 earnings release / call / 10-Q	AI infrastructure, capex guidance, data center cost, component pricing, GPUs, HBM	原文已证明：Meta 上调 2026 capex 区间并提及组件价格和数据中心成本；对 HBM vendors 的供应链边仍为合理推论。
Meta

Amazon / AWS	Q1 2026 earnings, shareholder letter, AWS infrastructure pages	AWS AI revenue run rate, Trainium, Inferentia, capex, HBM, supplier	原文已证明：AWS AI revenue run-rate 和 AWS segment 指标有官方披露；Trainium/HBM 具体物料边仍需芯片/供应商技术页核验。
ir.aboutamazon.com
+1

NVIDIA	GB200 / GB300 NVL72 product pages, architecture pages, 10-K, GTC materials	HBM3E, HBM capacity, bandwidth, NVL72, Blackwell Ultra, rack memory	原文已证明：GB200/GB300 系统级 HBM 容量和带宽是 AI accelerator 对 HBM 需求的强锚点。
NVIDIA
+1

AMD	Instinct product pages, earnings call, Samsung/Micron/SK cross-disclosures	MI350 / MI400 / MI455X, HBM3E, HBM4, qualification, supply agreement	待原文核验：需用 AMD product page / Samsung HBM4 MOU / memory vendor call 核验具体 HBM4 attach 与供应链。Samsung 已披露与 AMD MI455X 的 HBM4 primary supply alignment。
Samsung Semiconductor Global

SK hynix	FY2025 / FY2026 Q1 earnings, annual report, IR deck, newsroom / product pages	HBM3E, HBM4, 12-high, 16-high, MR-MUF, hybrid bonding, capacity, sold-out, capex, gross margin	原文已证明：公司披露 HBM3E、12-high HBM4 samples、HBM4 development / production timeline；MR-MUF 与 hybrid bonding 需继续从 IR/tech 原文核验。
SK hynix
+1

Samsung Electronics	annual report, quarterly results, HBM3E / HBM4 product pages, advanced packaging pages	HBM3E, HBM4, 12-layer, 4nm logic base die, TC-NCF, bandwidth, qualification	原文已证明：Samsung HBM3E / HBM4 产品页说明堆叠、带宽、TC-NCF / logic base die 等；客户 qualification 和收入弹性仍需 earnings call 核验。
Samsung Semiconductor Global
+1

Micron	10-K, quarterly earnings, HBM3E / HBM4 product pages, investor deck	HBM3E 8H/12H, HBM4 12H, bandwidth, power, NVIDIA Vera Rubin, capex, HBM revenue	原文已证明：Micron HBM3E / HBM4 产品页和 release 证明 HBM4 36GB 12-high、>2.8TB/s、Vera Rubin 设计目标；收入/毛利率需继续核验财报。
美光科技
+1

TSMC	annual report, CoWoS technology pages, tech symposium, earnings call	CoWoS, reticle size, interposer, HBM stacks, N12/N3 HBM4 base die, capacity, 2026/2027/2028 roadmap	原文已证明：TSMC 披露 CoWoS 5.5 reticle、9.5 reticle、14 reticle 路线及 HBM stack 扩展；CoWoS capacity 是 HBM 主线关键交叉瓶颈。
台积电
+2
台积电
+2

ASE	annual report, quarterly call, technology pages / blogs	2.5D, interposer, FOCoS, FCBGA, chiplet, HBM, AI/HPC	合理推论：ASE 官方材料证明 2.5D / FOCoS / chiplet / AI-HPC 包装位置；HBM 专属收入、客户和 capacity 为待原文核验。
ASE
+1

Amkor	10-K, quarterly release, investor deck, advanced packaging event pages	2.5D, AI processor, HBM, advanced packaging, capacity, utilization	合理推论 / 部分原文已证明：Amkor 披露 AI processor 2.5D ramp 和 HBM/advanced packaging 技术活动；具体 HBM revenue / customer 为待原文核验。
Amkor Technology
+1

Hanmi Semiconductor	annual report, earnings release, product pages, order announcements	TC BONDER, DUAL TC BONDER, HBM4, AI, big die TC bonder, backlog, customer	原文已证明：Hanmi 官方产品页直接把 TC bonder 定位于 HBM / HBM4 / AI 2.5D。
汉美塞米
+1

ASMPT	annual report, earnings release, product page, investor presentation	TCB, Thermo-Compression Bonding, advanced logic, HBM, TAM, backlog, gross margin	原文已证明：ASMPT 披露 TCB revenue 高增长和 advanced logic / HBM 推动；FIREBIRD TCB 产品页对应 2.5D/3D/HPC/AI。
ASMPT
+1

BESI	annual report, Q4/FY result, investor presentation	hybrid bonding, 2.5D, AI datacenter, photonics, orders, backlog, gross margin	原文已证明：BESI 披露 2.5D AI computing / datacenter 和 hybrid bonding orders。
Besi

SUSS MicroTec	annual report, quarterly result, investor presentation, product pages	temporary bonding, advanced packaging, AI chip modules, lithography, order intake, margin	合理推论 / 待原文核验：已看到官方 FY2025 financial source；需在年报 / presentation 内逐项核验 HBM / AI chip modules 关键词。
SUSS
+1

TOWA	annual report, quarterly presentation, product releases	compression molding, HBM, MR-MUF, MUF, narrow gap, HBM4 certification, AI accelerator	原文已证明：TOWA 官方材料把 compression molding / MUF 与 HBM / generative AI semiconductor 绑定，并披露 HBM4 certification 线索。
Towajapan
+1

DISCO	annual report, technical briefs, product pages	grinding, thinning, dicing, edge trimming, TSV reveal, laser saw, ultra-thinned memory, HBM	原文已证明：DISCO 技术资料直接说明 HBM process 的 trimming / thinning / dicing / die lamination，以及 AI package 中 HBM + interposer 结构。
迪斯可
+1

Advantest	annual report, securities report, technical briefing, financial briefing	memory tester, SoC tester, HBM, known-good-die, AI, TSMC/Samsung customer group, backlog	原文已证明：Advantest 官方资料直接把 HBM / AI 与 memory tester、KGD、tester demand 连接。
株式会社アドバンテスト
+1

Teradyne	10-K, earnings call, memory / SoC test product pages	memory test, AI accelerator, HBM, GPU/ASIC final test, backlog, customer concentration	待原文核验：属于测试候选，但本轮未看到公司原文直接证明 HBM 暴露。
FormFactor	10-K, investor presentation, probe-card product pages	probe card, HBM, wafer probe, SK hynix, hyperscaler AI infrastructure, revenue	原文已证明：FormFactor 官方/IR 资料直接提 HBM probe-card demand 和 SK hynix HBM probe-card customer proof。
FormFactor, Inc.
+1

Chroma	annual report, product pages, investor materials	ATE, power test, memory test, HBM, AI server, handler, backlog	待原文核验：需区分 Chroma ATE 的 AI/HBM test 与泛电子测试。
MPI	annual report, probe card product pages, investor presentation	probe card, wafer probe, high-frequency, memory probe, HBM	合理推论 / 待原文核验：公司原文证明 probe card 业务；HBM 专属收入/客户仍需核验。
mpi-corporation.com

WinWay	annual report, product pages, investor materials	vertical probe card, WLCSP probe, HBM, high-performance wafer probing, memory	待原文核验：产品原文证明 probe card 能力；HBM 直接关联需查年报/IR。
WinWay Technology Co., Ltd.
+1

Leeno	annual report, product pages, investor materials	IC test socket, memory module test socket, AI, high pin count, HBM socket	合理推论 / 待原文核验：公司原文证明 test socket / AI system-level socket；HBM socket 直接证据需补。
leeno.com
+1

ISC / TSE	annual reports, product pages, investor presentations	test socket, probe card, memory test, HBM, customer, backlog	待原文核验：不能把韩国 test interface 泛化为 HBM 受益，需看 HBM / high bandwidth memory 原文关键词。
Camtek	annual report, earnings, product page	advanced packaging, inspection, metrology, memory, HBM, CoWoS, revenue mix	原文已证明：Camtek 官方定位覆盖 advanced packaging / memory inspection；HBM 专属收入仍需财报核验。
Camtek

Nova	annual report, quarterly result, investor presentation	metrology, DRAM, advanced packaging, hybrid bonding, HBM, AI	原文已证明：Nova 披露 GAA / DRAM / advanced packaging 由 AI 驱动，investor deck 提到 AP / HBM process。
Nova
+1

Onto Innovation	10-K, quarterly release, investor presentation	advanced packaging, inspection, metrology, panel, HBM, AI packaging	待原文核验：需查 10-K / deck 中 HBM / advanced packaging revenue 与订单。
KLA	10-K, investor day, advanced packaging process-control pages	advanced packaging, GPU + HBM package, interposer, substrate, yield, inspection	原文已证明：KLA 官方材料说明 AI chip package 由 GPU + HBM + interposer / substrate 组成，且 AP process control 是其覆盖方向。
KLA
+1

Ibiden	annual report, integrated report, quarterly result	IC package substrate, ABF, AI, GPU, HBM package, capacity, ASP, capex	待原文核验：ABF substrate 候选，但本轮未看到直接 HBM 原文。
Shinko Electric	annual report / financial results / product pages	package substrate, molded underfill, flip-chip, HBM, AI, CPO	合理推论 / 待原文核验：产品页证明 package / molded underfill / CPO 能力；HBM收入需核验。
shinko.co.jp

Unimicron	annual report, monthly sales, investor deck	ABF, IC substrate, AI server, GPU, HBM, capacity, margin	待原文核验：不能只因 ABF 主题归入 HBM，需要原文 AI/HPC substrate 证据。
Nan Ya PCB	annual report, investor deck, ABF roadmap	ABF substrate, AI, HPC, CoWoS, HBM, capacity, ASP	待原文核验：已有 IR / ABF roadmap 来源线索，但 HBM 直接边仍需查。
nanyapcb.com.tw
+1

Kinsus	annual report, investor deck, product pages	ABF, IC substrate, AI/HPC, GPU package, HBM	待原文核验：公司原文显示 ABF business contact；需 HBM / AI substrate 原文证据。
kinsus.com.tw

Ajinomoto / Ajinomoto Fine-Techno	annual report, ABF technology pages	ABF, buildup film, high-performance semiconductor, CPU/GPU, data center, capacity	原文已证明：ABF 是高性能半导体封装关键材料；对 HBM package 的直接收入仍是合理推论。
ajinomoto.com
+1

Resonac	annual report, technology pages, packaging materials releases	advanced packaging, NCF, TIM, underfill, mold, HBM, chiplet, capacity	原文已证明 / 合理推论：Resonac 官方资料证明 advanced packaging materials / NCF / TIM；HBM direct customer 需核验。
resonac.com
+1

Namics	official product pages, annual / parent disclosure if available	underfill, capillary underfill, flip chip, advanced packaging, HBM, narrow gap	原文已证明 / 合理推论：Namics 原文证明 underfill / AP packaging；HBM 专属收入与客户需核验。
namics-corp.com
+1

Sumitomo Bakelite	annual report, semiconductor materials pages	molding compound, encapsulation, underfill, HBM, compression molding, advanced package	待原文核验：材料候选，不得在未核验前列为 HBM 核心受益。
Rambus	10-K, HBM controller / PHY product pages	HBM4 controller, HBM4E, HBM3E, bandwidth, AI accelerator, PHY, IP licensing	原文已证明：Rambus 官方产品页直接覆盖 HBM4 / HBM4E controller IP 与 AI accelerator。
Rambus
+1

Marvell	10-K, custom silicon pages, HBM compute architecture	custom HBM compute, AI accelerator, HBM capacity, memory interface power, memory vendors	原文已证明：Marvell 官方披露 custom HBM compute architecture 与 SK hynix / Samsung / Micron 相关生态。
marvell.com

Astera Labs	10-K, product pages, investor deck	rack-scale AI connectivity, PCIe/CXL, memory vendors, HBM adjacent, retimer	合理推论：AI rack connectivity 与 memory vendor interoperability 有原文线索；非 HBM 直接供应，需防泛化。
ASTERA LABS, INC.

Broadcom	10-K, AI custom ASIC disclosure, product pages, TSMC cross-disclosure	custom ASIC, HBM controller/PHY, CoWoS, AI networking, packaging	待原文核验：AI ASIC 相关性强，但 HBM / packaging 具体边必须回公司 10-K / call / TSMC cross-disclosure。
2. HBM bottleneck chain：BFS 主线

项目原始研究地图把 AI Infra 描述为从 token demand 到 accelerator、HBM / storage、advanced packaging / test / substrate、materials / power 的多层约束系统；该框架适合用 BFS 从 D0 需求源头向 D2-D4 供应瓶颈扩展。

2026-05-12-ai-super-cycle-resea…

BFS 链条节点	BFS depth	为什么会被 HBM 放大	代表公司	需要核验的原文指标	Evidence state	主要反证
AI accelerator demand	D0-D1	LLM 训练/推理集群扩大后，GPU/TPU/ASIC 不只是需要算力，也需要更高带宽和更大容量的本地高带宽内存；NVIDIA GB200 / GB300 NVL72 原文已把系统级 HBM 容量、带宽与 rack-scale AI 绑定。
NVIDIA
+1
	OpenAI, Microsoft, Google, Meta, AWS, NVIDIA, AMD, Broadcom / Marvell ASIC ecosystem	cloud capex, RPO, AI infra capex, GPU shipments, HBM capacity per accelerator, rack HBM TB, accelerator roadmap	原文已证明：accelerator 与 HBM 绑定；合理推论：D0 token growth → HBM vendor revenue	AI capex 放缓；推理效率提升快于 token 增长；ASIC 降低 HBM bytes/FLOP；HBM per accelerator 下行
HBM3E / HBM4 capacity	D2	HBM 由 DRAM wafer、TSV、stacking、base die、test、packaging 共同限制；HBM4/12-high/16-high提高每颗 accelerator 的容量和带宽，也提高封装、测试、良率难度。SK hynix、Samsung、Micron 都有 HBM3E/HBM4 官方路线或产品披露。
SK hynix
+2
Samsung Semiconductor Global
+2
	SK hynix, Samsung, Micron	HBM revenue, HBM bit shipment, HBM ASP, HBM3E/HBM4 qualification, 12-high/16-high, wafer allocation, capex, gross margin	原文已证明：产品路线；待原文核验：各家产能、份额、售罄周期、毛利率	Samsung / Micron 供给补齐导致 ASP 下行；HBM4 良率提升快于预期；AI accelerator 出货放缓
Wafer thinning / dicing / grinding	D3	HBM 堆叠需要薄化 die、TSV reveal、dicing、die lamination；stack 越高，对厚度均匀性、洁净度、边缘缺陷控制越敏感。DISCO 官方技术资料明确列出 HBM process 中 edge trimming、thinning、TSV reveal、dicing、die lamination。
迪斯可
	DISCO, Tokyo Seimitsu / Accretech, Tazmo, Shibaura Mechatronics	HBM-related shipments, grinder / dicer / laser saw orders, memory customer capex, ultra-thinned memory exposure, backlog	原文已证明：DISCO 工艺链；其他多为待原文核验	HBM yield 改善降低单位设备需求；客户内制 / 替代工艺；memory capex 下行
TC bonding / MR-MUF / hybrid bonding	D3	HBM stack 高度增加后，die-to-die bonding、warpage、thermal、gap-fill 更难；HBM4/16-high 可能推动 TC bonding、MR-MUF、hybrid bonding 或改良 underfill/molding 路线。Hanmi、ASMPT、BESI、TOWA、Samsung、SK hynix 原文均提供部分技术锚点。
SK hynix
+4
汉美塞米
+4
ASMPT
+4
	Hanmi, ASMPT, BESI, Samsung Advanced Packaging, SK hynix, TOWA, SUSS	TC bonder orders, hybrid bonding orders, MR-MUF / TC-NCF adoption, HBM4/HBM4E qualification, cycle time, placement accuracy, backlog, gross margin	原文已证明：TCB / hybrid bonding / HBM 关系；合理推论：HBM4 stack height 拉动设备价值量	技术路线切换使某类设备价值量下降；memory vendor 内制；订单一次性前置；客户认证失败
Known-good-die / HBM test time	D3	HBM 堆叠前后都要筛出 known-good-die，否则多 die stack 或 CoWoS package 中任一失效都会放大损失；HBM4 转换可能增加 memory tester / wafer probe 复杂度。Advantest 官方 Q&A 直接讨论 KGD、HBM4 转换和 memory tester demand。
株式会社アドバンテスト
	Advantest, Teradyne, Chroma, FormFactor, MPI, WinWay, Leeno, ISC, TSE	memory tester revenue, SoC tester revenue, HBM test time, utilization, probe card ASP, socket attach, customer concentration	原文已证明：Advantest / FormFactor HBM test/probe；其他多为待原文核验	测试时间缩短；tester order 提前透支；HBM 产能过剩；客户减少外部 probe/socket 采购
Probe card / socket / handler	D3	HBM wafer probe 和高密度封装测试提高 probe card 精度、针数、热/机械可靠性要求；FormFactor 原文直接提 HBM probe-card demand 及 SK hynix HBM probe card relationship。
FormFactor, Inc.
+1
	FormFactor, MPI, WinWay, Leeno, ISC, TSE, Chroma, Cohu	probe card revenue by segment, HBM probe card orders, socket pin count, handler throughput, gross margin, memory customer exposure	原文已证明：FormFactor；合理推论 / 待原文核验：其他 probe/socket 厂	probe card ASP 未提升；HBM 只由少数大厂内部供应；客户换 supplier；测试架构变化
CoWoS / interposer / ABF substrate	D2-D3	HBM 不单独卖给 AI accelerator，最终要与 GPU/ASIC 通过 interposer / CoWoS / substrate 集成；HBM stack 数和 package reticle size 上升会拉动 CoWoS capacity、interposer、ABF 层数和良率难度。TSMC 官方路线显示 CoWoS 从 5.5 reticle 到 9.5 / 14 reticle，HBM stack 数增加。
台积电
+2
台积电
+2
	TSMC, ASE, Amkor, Samsung AP, Ibiden, Shinko, Unimicron, Nan Ya PCB, Kinsus, Ajinomoto	CoWoS capacity, reticle size, HBM stacks/package, interposer capacity, ABF capacity/ASP, substrate yield, capex, backlog	原文已证明：TSMC CoWoS/HBM stack roadmaps；substrate suppliers 多为合理推论 / 待原文核验	CoWoS 扩产后瓶颈转移；substrate 供给过剩；glass substrate / alternative interposer 改变 ABF 用量
Advanced packaging inspection / metrology	D3	HBM + GPU / ASIC 大封装中 defect cost 高，chiplet、hybrid bonding、interposer、substrate 都增加 inspection / metrology 内容；KLA、Nova、Camtek 官方资料把 AP / memory / HBM-related process control 与 AI package 连接。
KLA
+2
Nova
+2
	Camtek, Nova, KLA, Onto Innovation, Lasertec, Koh Young, Nextin	AP inspection revenue, metrology revenue, HBM / hybrid bonding process exposure, book-to-bill, AI customer orders	原文已证明：KLA/Nova/Camtek 的 AP / memory 方向；待原文核验：HBM 专属收入	AP inspection 被集成进既有设备；订单随 WFE cycle 波动；客户内部化；yield 稳定后单位检查下降
Underfill / molding / thermal / materials	D3-D4	HBM stack height、narrow gap、thermal density 上升会放大 MR-MUF、underfill、molding compound、TIM、NCF、ABF 等材料规格；Samsung 提 TC-NCF，TOWA 提 HBM narrow-gap MUF，Resonac / Namics / Ajinomoto 有 AP materials / ABF / underfill 官方材料。
ajinomoto.com
+4
Samsung Semiconductor Global
+4
Towajapan
+4
	TOWA, Resonac, Namics, Sumitomo Bakelite, Ajinomoto, Samsung / SK hynix material ecosystem	underfill / MUF / NCF revenue, material ASP, qualification, HBM4 material adoption, thermal resistance, capacity, gross margin	原文已证明：材料技术位置；合理推论 / 待原文核验：HBM-specific revenue	材料被大客户压价；替代材料路线；HBM stack redesign 降低用量；材料收入太分散无法形成利润弹性
3. Candidate expansion table：候选 / radar 公司

排序规则：D2-D3 优先；D4-D5 只做 radar，除非能证明它们反向卡住 HBM / CoWoS / accelerator 出货。证据状态严格区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”。

#	company	ticker / exchange	country	BFS depth	HBM supply-chain node	dependency edge	evidence state	primary sources to verify	upgrade condition	downgrade / refutation condition
1	NVIDIA	NVDA / NASDAQ	US	D1-D2	AI accelerator HBM attach	GB200/GB300 → HBM3E capacity / CoWoS	原文已证明：rack HBM capacity / bandwidth disclosed.
NVIDIA
+1
	10-K, product pages, GTC decks	HBM TB/rack and HBM stacks/GPU continue rising	accelerator demand slows or architecture reduces HBM intensity
2	SK hynix	000660 / KRX	Korea	D2	HBM vendor	HBM3E/HBM4 supply → AI accelerator	原文已证明：HBM3E/HBM4 roadmap/product milestones.
SK hynix
	annual report, FY2026 Q1 earnings, IR deck	HBM4 / 16-high ramp, long-term capacity sold out, margin expansion	HBM ASP decline, qualification loss, supply overbuild
3	Samsung Electronics	005930 / KRX	Korea	D2	HBM vendor + AP	HBM3E/HBM4 + TC-NCF / base die	原文已证明：HBM3E/HBM4 product tech; customer qualification still to verify.
Samsung Semiconductor Global
+1
	annual report, quarterly call, HBM pages	HBM4 qualification with major AI accelerator customers	repeated qualification delays; HBM margin below peers
4	Micron	MU / NASDAQ	US	D2	HBM vendor	HBM3E/HBM4 → NVIDIA / AI accelerators	原文已证明：HBM3E/HBM4 product specs and Vera Rubin target.
Micron Technology
+1
	10-K, quarterly call, HBM product pages	HBM4 high-volume ramp with leading AI platforms	capacity too small or margin dilution from capex
5	TSMC	TSM / NYSE; 2330 / TWSE	Taiwan	D2	CoWoS / interposer / foundry	HBM stacks integrated with GPU/ASIC on CoWoS	原文已证明：CoWoS reticle-size roadmap and HBM stack scaling.
台积电
+1
	annual report, earnings call, tech symposium	CoWoS capacity remains booked and package sizes rise	CoWoS supply catches demand; packaging ASP compresses
6	Samsung Advanced Packaging	005930 / KRX	Korea	D2-D3	advanced packaging / HBM integration	Samsung HBM + logic/package ecosystem	合理推论：Samsung product tech proves HBM capability; AP revenue edge pending	annual report, AP tech pages, quarterly call	Samsung wins AI accelerator package/HBM integrated share	AP not externally monetized or remains captive
7	ASE Technology	ASX / NYSE; 3711 / TWSE	Taiwan	D2-D3	2.5D / FOCoS / FCBGA	OSAT capacity for AI/HPC chiplets + HBM	合理推论：official AP/FOCoS/2.5D proof; HBM revenue pending.
ASE
+1
	annual report, earnings, technology pages	explicit AI/HBM advanced packaging revenue/backlog	only low-margin assembly; no HBM-specific capacity
8	Amkor	AMKR / NASDAQ	US	D2-D3	2.5D OSAT	AI processor ramp + HBM packaging	合理推论 / 部分原文已证明：2.5D AI processor ramp disclosed.
Amkor Technology
	10-K, quarterly release, investor deck	sustained AI processor 2.5D utilization and margin lift	utilization falls or AP revenue not margin-accretive
9	Hanmi Semiconductor	042700 / KRX	Korea	D3	HBM TC bonder	HBM4 / stacked die bonding	原文已证明：HBM / HBM4 TC bonder official pages.
汉美塞米
+1
	annual report, product pages, order disclosures	multi-customer HBM4 orders and backlog visibility	customer concentration, TCB route share declines
10	ASMPT	0522 / HKEX	HK / Singapore	D3	TCB equipment	advanced logic / HBM bonding	原文已证明：TCB revenue and HBM/advanced logic TAM disclosed.
ASMPT
	annual report, earnings, product pages	HBM-driven TCB orders grow beyond one cycle	TCB order pull-forward; gross margin dilution
11	BESI	BESI / Euronext Amsterdam	Netherlands	D3	hybrid bonding / AP assembly	HBM4 / chiplet / 2.5D AI datacenter	原文已证明：2.5D AI datacenter and hybrid bonding orders disclosed.
Besi
	annual report, quarterly release, investor deck	hybrid bonding moves from logic to HBM-related volume	hybrid bonding adoption delayed or limited to niche
12	SUSS MicroTec	SMHN / Xetra	Germany	D3	temporary bonding / AP lithography	wafer-level AP steps for HBM/CoWoS	合理推论 / 待原文核验：financial source found, HBM keywords need document read.
SUSS
	annual report, investor presentation, product pages	explicit AI chip module / HBM process orders	AP demand is generic, not HBM-specific
13	TOWA	6315 / TSE	Japan	D3	molding / MUF / compression molding	HBM narrow-gap fill and HBM4 certification	原文已证明：HBM / MUF / HBM4 certification line in official materials.
Towajapan
+1
	annual report, quarterly presentation, product pages	HBM4 / HBM5 molding becomes bottleneck with high margins	molding commoditizes or route bypasses TOWA equipment
14	DISCO	6146 / TSE	Japan	D3	grinding / dicing / thinning	HBM die thinning, TSV reveal, dicing	原文已证明：HBM process steps in official tech brief.
迪斯可
	annual report, technical briefs, shipment data	HBM / AI memory drives tool mix and ASP	memory capex downturn; tool demand only generic
15	Advantest	6857 / TSE	Japan	D3	memory / SoC test	HBM KGD, tester demand, HBM4 transition	原文已证明：HBM/KGD/tester demand official Q&A/briefing.
株式会社アドバンテスト
+1
	annual securities report, financial briefing	HBM4 test time/complexity expands tester TAM	test time reduction; order pull-forward
16	Teradyne	TER / NASDAQ	US	D3	SoC / memory test	AI accelerator final test and possible HBM test	待原文核验	10-K, earnings call, product pages	direct HBM / AI memory tester disclosure	only broad semiconductor beta, no HBM exposure
17	FormFactor	FORM / NASDAQ	US	D3	HBM probe card	wafer probe for HBM KGD	原文已证明：HBM probe-card demand and SK hynix proof.
FormFactor, Inc.
+1
	10-K, investor presentation, product pages	HBM probe revenue grows with stack complexity	probe card ASP/revenue not sustained
18	MPI	6223 / TWSE	Taiwan	D3	probe card	wafer-level probe for memory / AP	合理推论 / 待原文核验：probe card capability; HBM direct pending.
mpi-corporation.com
	annual report, product pages, IR deck	explicit HBM probe card customer/design win	generic probe card cycle only
19	WinWay	6515 / TWSE	Taiwan	D3	probe card / wafer test	high-performance wafer probing	待原文核验：product capability not enough for HBM proof.
WinWay Technology Co., Ltd.
	annual report, investor deck, product pages	HBM / AI package probe card revenue disclosed	no HBM keyword; margin follows generic semis
20	Leeno	058470 / KOSDAQ	Korea	D3	socket / test interface	high-pin-count AI / memory module testing	合理推论 / 待原文核验：AI/socket product hints; HBM direct pending.
leeno.com
	annual report, product pages, IR	HBM socket / memory interface orders disclosed	exposed mainly to mobile/commodity test
21	ISC	095340 / KOSDAQ	Korea	D3	test socket	HBM / memory package test interface	待原文核验	annual report, product pages, IR	direct HBM socket/customer proof	no HBM keyword; socket commoditization
22	TSE	131290 / KOSDAQ	Korea	D3	probe / test interface	memory / HBM test interface	待原文核验	annual report, product pages, IR	HBM probe/socket order disclosure	only generic memory-cycle exposure
23	Chroma ATE	2360 / TWSE	Taiwan	D3	ATE / power test	AI accelerator / memory module test	待原文核验	annual report, product pages, investor deck	explicit HBM / AI package test revenue	test exposure unrelated to HBM
24	Camtek	CAMT / NASDAQ	Israel	D3	AP inspection	HBM / AP package inspection and metrology	原文已证明：AP/memory inspection positioning; HBM revenue pending.
Camtek
	20-F, quarterly call, product pages	AP / HBM inspection revenue disclosed	AP demand slows or tool commoditizes
25	Nova	NVMI / NASDAQ	Israel	D3	metrology	DRAM / AP / hybrid bonding process control	原文已证明：AI-driven DRAM/AP and HBM process mention.
Nova
+1
	20-F, quarterly release, investor deck	HBM / hybrid bonding metrology orders scale	AP metrology is cyclical and non-HBM
26	Onto Innovation	ONTO / NASDAQ	US	D3	inspection / metrology	AP / panel / interposer yield	待原文核验	10-K, earnings, product pages	HBM/CoWoS AP tool order evidence	broad WFE beta only
27	KLA	KLAC / NASDAQ	US	D3-D4	AP process control	GPU+HBM package, interposer, substrate yield	原文已证明：AI chip package process-control framing.
KLA
+1
	10-K, investor day, AP portfolio	AP/HBM becomes material growth segment	large-cap WFE beta overwhelms HBM signal
28	Ibiden	4062 / TSE	Japan	D3	ABF substrate	GPU/ASIC/HBM package substrate	待原文核验 / 合理推论	integrated report, earnings, product pages	AI/HPC substrate capacity sold out with margin lift	substrate overcapacity; no HBM-specific exposure
29	Shinko Electric	原 6967 / TSE status to verify	Japan	D3	package substrate / molded underfill	flip-chip package and module assembly for AI/HBM	合理推论 / 待原文核验：package tech proof; HBM revenue pending.
shinko.co.jp
	financial results, product pages, ownership status	explicit AI/HBM substrate orders	delisting/ownership limits visibility; no HBM proof
30	Unimicron	3037 / TWSE	Taiwan	D3	ABF / IC substrate	AI accelerator package substrate	待原文核验	annual report, monthly sales, investor deck	AI/HBM ABF mix and margin lift disclosed	AI server PCB not HBM substrate; ASP down
31	Nan Ya PCB	8046 / TWSE	Taiwan	D3	ABF substrate	HBM/CoWoS package substrate	待原文核验：IR/roadmap source found.
nanyapcb.com.tw
+1
	annual report, ABF roadmap, investor deck	ABF capacity tied to AI/HBM packaging	parent/PCB cycle dominates; no HBM link
32	Kinsus	3189 / TWSE	Taiwan	D3	ABF substrate	GPU/ASIC/HBM package substrate	待原文核验	annual report, investor deck, product pages	AI/HPC ABF orders and margin inflection	no direct HBM/AI substrate disclosure
33	AT&S	ATS / Vienna	Austria	D3	IC substrate / ABF	high-end processor substrate for AI packages	待原文核验	annual report, CMD, product pages	AI/HPC substrate capacity linked to HBM/CoWoS	overcapacity, weak utilization, generic substrate cycle
34	Ajinomoto	2802 / TSE	Japan	D4	ABF material	ABF film for high-performance package substrates	原文已证明：ABF for high-performance semiconductors; HBM-specific edge inferred.
ajinomoto.com
+1
	annual report, ABF tech pages	ABF shortages constrain AI package substrate	ABF remains diversified and not bottleneck
35	Resonac	4004 / TSE	Japan	D4	AP materials / NCF / TIM	HBM / chiplet thermal and bonding materials	原文已证明 / 合理推论：AP materials proof; HBM revenue pending.
resonac.com
	annual report, material pages, R&D releases	HBM4 materials qualification and capacity expansion	materials too small/commoditized; no pricing power
36	Namics	private	Japan	D4	underfill	flip-chip / AP underfill, narrow-gap package	原文已证明 / 合理推论：underfill/AP proof; HBM direct pending.
namics-corp.com
	product pages, parent disclosure if any	HBM/MUF/underfill customer qualification	private visibility low; underfill commoditized
37	Sumitomo Bakelite	4203 / TSE	Japan	D4	molding / encapsulation	HBM/AP molding compound	待原文核验	annual report, semiconductor materials pages	explicit advanced package / HBM molding sales	no HBM keyword; broad materials exposure
38	Rambus	RMBS / NASDAQ	US	D2-D3	HBM controller IP	HBM4/HBM4E controller/PHY for AI accelerators	原文已证明：HBM4/HBM4E controller IP product pages.
Rambus
+1
	10-K, product briefs, licensing disclosures	AI ASIC HBM4/4E IP wins scale royalties	customers internalize IP; no material revenue
39	Marvell	MRVL / NASDAQ	US	D2-D3	custom HBM compute / ASIC	HBM memory interface + custom AI silicon	原文已证明：custom HBM compute architecture official disclosure.
marvell.com
	10-K, investor day, product pages	custom ASIC ramps with HBM-rich architectures	hyperscaler concentration; ASIC cycle volatility
40	Broadcom	AVGO / NASDAQ	US	D2-D3	custom ASIC / networking	AI ASICs requiring HBM/CoWoS	待原文核验	10-K, earnings call, AI ASIC disclosures	explicit HBM/CoWoS design win disclosure	AI networking grows but HBM edge not provable
41	Astera Labs	ALAB / NASDAQ	US	D3	rack connectivity / memory-adjacent	PCIe/CXL/rack-scale AI connectivity around HBM accelerators	合理推论：rack-scale AI connectivity source; not HBM direct.
ASTERA LABS, INC.
	10-K, product pages, investor deck	memory-vendor / accelerator platform attach expands	connectivity not constrained by HBM, valuation theme only
42	GUC	3443 / TWSE	Taiwan	D2-D3	ASIC design / HBM IP on CoWoS	AI ASIC + HBM controller/PHY + CoWoS	原文已证明 / cross-disclosure：TSMC CoWoS page links GUC HBM IP ecosystem.
3DFabric
	annual report, product pages, TSMC cross-disclosure	AI ASIC tape-outs with HBM/CoWoS scale	customer concentration or NRE not recurring
43	Alchip	3661 / TWSE	Taiwan	D2-D3	ASIC design service	AI ASIC with HBM/CoWoS	待原文核验	annual report, investor deck, customer disclosures	HBM-rich AI ASIC design wins disclosed	single customer risk; HBM not proven
44	Faraday	3035 / TWSE	Taiwan	D2-D3	ASIC / IP	possible HBM/advanced package ASIC support	待原文核验	annual report, product pages, IR	explicit HBM/CoWoS ASIC program	mainly non-AI ASIC, low HBM exposure
45	Synopsys	SNPS / NASDAQ	US	D3-D4	EDA / IP	HBM PHY/controller, 3DIC design flow	待原文核验	10-K, IP product pages, 3DIC pages	HBM4/3DIC IP revenue / design wins visible	EDA growth too broad to isolate HBM
46	Cadence	CDNS / NASDAQ	US	D3-D4	EDA / 3DIC	HBM/CoWoS package design and verification	待原文核验	10-K, investor deck, 3DIC product pages	explicit HBM/AI accelerator package design wins	generic EDA exposure only
47	Applied Materials	AMAT / NASDAQ	US	D4	WFE / advanced packaging tools	DRAM/HBM wafer process + packaging	待原文核验 / 主要反证：large-cap WFE beta risk	10-K, earnings, AP product pages	HBM-specific process/materials orders quantified	generic WFE cycle overwhelms HBM signal
48	Lam Research	LRCX / NASDAQ	US	D4	DRAM etch/deposition	HBM wafer process	待原文核验 / 主要反证	10-K, earnings, DRAM/HBM commentary	HBM/advanced DRAM process intensity quantified	commodity memory capex cycle
49	Tokyo Electron	8035 / TSE	Japan	D4	WFE	DRAM/HBM process tools	待原文核验 / 主要反证	annual report, earnings, product pages	HBM/EUV DRAM process intensity drives orders	broad WFE cycle; not HBM-specific
50	Shibaura Mechatronics	6590 / TSE	Japan	D3-D4	AP / bonding / packaging equipment	HBM / 2.5D packaging process	待原文核验	annual report, product pages, IR	HBM/CoWoS tool order proof	generic equipment exposure
51	Tazmo	6266 / TSE	Japan	D3-D4	AP equipment / wafer process	HBM / advanced package process support	待原文核验	annual report, product pages, IR	direct HBM/AP equipment orders	no HBM exposure; cyclical semicap
52	Tokyo Seimitsu / Accretech	7729 / TSE	Japan	D3	dicing / probing / metrology	HBM thinning/dicing/probe adjacency	待原文核验	annual report, product pages, IR	HBM process or AI memory shipment evidence	broad semiconductor tool cycle
53	Nextin	348210 / KOSDAQ	Korea	D3-D4	inspection	DRAM / AP inspection radar	待原文核验	annual report, product pages, IR	HBM/DRAM inspection customer proof	not HBM; low visibility
54	Koh Young	098460 / KOSDAQ	Korea	D3	inspection	AP / package inspection radar	待原文核验	annual report, product pages	AI/HBM package inspection proof	SMT/industrial inspection dominates
55	VAT Group	VACN / SIX	Switzerland	D4-D5	vacuum valve	upstream WFE capacity for DRAM/HBM	待原文核验 / 主要反证：too indirect	annual report, semicap customer commentary	HBM-driven WFE bottleneck causes valve backlog	broad WFE cyclicality, no reverse bottleneck
56	Soitec	SOI / Euronext Paris	France	D4-D5 radar	engineered substrates	possible AI / silicon photonics / advanced substrate adjacency	主要反证：not HBM direct unless original proof	annual report, product pages	direct AI/HBM packaging material link found	remains RF/auto/SiC, not HBM
57	IQE	IQE / LSE	UK	D4-D5 radar	compound epi	optical / AI networking adjacency, not HBM	主要反证：not HBM chain unless CPO/laser link proven	annual report, product pages	CPO/InP laser source proves AI cluster bottleneck	optical theme only, no HBM edge
58	Coherent	COHR / NASDAQ	US	D3-D4 adjacent	optical / laser	AI networking, not HBM	主要反证：US adjacent only if packaging/test/thermal link proven	10-K, datacom product pages	HBM package / CPO packaging direct relation appears	pure optics, outside current HBM BFS
4. How to discover more companies automatically：财报挖 HBM 供应链 agent pipeline
4.1 Filing-reader agent

输入字段

field	内容
company_name	公司名
ticker_exchange	ticker / exchange
filing_type	annual report / 10-K / 20-F / quarterly / earnings release / transcript / investor presentation
filing_url	原文链接
report_period	报告期
keyword_set	HBM3E, HBM4, 12-high, 16-high, TC bonding, MR-MUF, hybrid bonding, KGD, probe card, memory test, CoWoS, ABF, underfill, molding, capacity, backlog, qualification, gross margin
segment_map	公司 segment / product lines
language	English / Korean / Japanese / Chinese / German / Hebrew

输出字段

field	内容
source_card_id	company-period-source hash
exact_keywords_found	原文出现的关键词
quoted_snippets	短摘录，不超过必要长度
metric_table	revenue, orders, backlog, capex, gross margin, inventory, customer concentration
proves	原文直接证明什么
does_not_prove	原文不能证明什么
evidence_state	原文已证明 / 合理推论 / 待原文核验 / 主要反证
next_sources	需要继续读的文件

prompt 模板

你是 HBM 供应链 filing-reader agent。
只读取给定公司原文，不使用二手摘要。
任务：
1. 搜索以下关键词：{keyword_set}
2. 抽取所有与 HBM / CoWoS / advanced packaging / memory test / probe / ABF / materials 相关的原文段落。
3. 对每个段落判断：
   - 原文已证明什么；
   - 不能证明什么；
   - 是否只是泛半导体 / 泛 AI / 泛存储周期。
4. 输出 source evidence card：
   company, ticker, filing_type, period, source_url, exact_quote, metric, metric_scope, evidence_state, refutation_notes。
禁止把没有出现的客户、收入、订单、产能编造成事实。

失败模式

failure mode	防错规则
只读到 IR 摘要，没读年报/10-K	标为“待原文核验”，不得升级
HBM 出现在行业介绍，不是公司收入	标为“合理推论”或“主题线索”
company name collision	例如 Hanmi Semiconductor vs Hanmi Financial；Shinko Electric vs Shinko Shoji，必须用 ticker / exchange 校验
segment 太宽	“semiconductor equipment revenue” 不能等于 “HBM equipment revenue”
日/韩/德文翻译误差	保留原文关键词和英文译名，必要时二次人工校验
4.2 Product-page reader agent

输入字段

field	内容
company_name	公司名
product_url	产品页
product_category	TC bonder / hybrid bonding / memory tester / probe card / ABF / underfill / metrology / grinder
target_node	HBM BFS node
keyword_set	HBM, HBM4, AI, HPC, CoWoS, 2.5D, TSV, high bandwidth memory, AP

输出字段

field	内容
product_name	产品名
process_step	对应工艺步骤
specification	placement accuracy, cycle time, bandwidth, thermal resistance, pin count, etc.
direct_hbm_evidence	yes/no + quote
ai_hpc_evidence	yes/no + quote
commercial_evidence_missing	customer, revenue, orders, margin
evidence_state	原文已证明 / 合理推论 / 待原文核验

prompt 模板

你是 HBM product-page reader agent。
读取公司产品页，只回答：
1. 产品属于 HBM BFS 哪个节点？
2. 原文是否直接写了 HBM / HBM4 / AI / HPC / CoWoS / memory test？
3. 如果只写了 advanced packaging / semiconductor / high-performance，而没有 HBM，必须标为“合理推论”或“待原文核验”。
4. 提取规格参数，并说明这些参数为什么可能与 HBM stack height、KGD、CoWoS、ABF 或 thermal 有关。
5. 列出还缺哪些商业证据：订单、客户、收入、毛利率、产能、qualification。

失败模式

failure mode	防错规则
产品页是 marketing copy	不能证明收入或订单
“AI” 出现在页面	不能自动升级为 HBM 受益
“advanced packaging” 太宽	需继续找 HBM / CoWoS / 2.5D / memory customer
产品已停产或旧版本	需核验发布日期和 current product status
4.3 Entity extraction agent

输入字段

field	内容
source_cards	filing/product-page reader 输出
text_chunks	原文段落
ontology	BFS node taxonomy
ticker_dictionary	公司名、ticker、交易所、别名
process_dictionary	HBM process terms

输出字段

field	内容
company_entities	公司
product_entities	产品 / 设备 / 材料
process_entities	TC bonding, MR-MUF, CoWoS, probe, ABF
customer_supplier_edges	customer / supplier / partner / ecosystem edge
ambiguity_flags	同名、子公司、私有公司、已退市、母子公司
confidence_score	0-1

prompt 模板

你是 HBM entity extraction agent。
从文本中抽取所有实体：
- 公司、子公司、客户、供应商、合作伙伴；
- 产品、设备、材料、工艺；
- BFS 节点和 dependency edge。
要求：
1. 每个实体必须附带原文来源 ID。
2. 如果原文没有说明 customer/supplier 关系，只能标为 ecosystem co-mention。
3. 对 ticker / exchange 不确定的公司标记 ambiguity_flag。
4. 输出 JSON：entities, aliases, process_terms, edges, confidence, missing_verification。

失败模式

failure mode	防错规则
co-mention 被误判为供应关系	edge_type 必须区分 supplier / customer / partner / co-mention
子公司和母公司混淆	加 ultimate_parent 字段
私有公司没有 ticker	ticker = private / N.A.
韩国/日本公司英文名不统一	建 alias map
4.4 Dependency classifier agent

输入字段

field	内容
entity_edges	entity extraction 输出
evidence_cards	source evidence cards
bfs_taxonomy	D0-D5 node definitions
refutation_rules	泛半导体、泛材料、泛存储过滤规则

输出字段

field	内容
bfs_depth	D0 / D1 / D2 / D3 / D4 / D5
node	HBM vendor / TCB / grinding / probe / CoWoS / ABF / material
dependency_edge	upstream → downstream
bottleneck_type	A 真瓶颈 / B 中瓶颈 / C 周期瓶颈 / D 伪瓶颈
evidence_state	原文已证明 / 合理推论 / 待原文核验 / 主要反证
upgrade_condition	升级条件
downgrade_condition	反证条件

prompt 模板

你是 HBM dependency classifier agent。
根据 evidence cards 判断公司属于 BFS 哪一层：
D0 demand, D1 accelerator/cloud, D2 HBM/CoWoS/foundry, D3 equipment/test/probe/substrate/metrology/material, D4 upstream WFE/materials/radar, D5 macro/power/finance/regulatory。
分类规则：
- 如果原文直接写 HBM / HBM4 / CoWoS / memory test / probe card / ABF + 公司产品，才可标为“原文已证明”。
- 如果只证明产品可用于 advanced packaging，但未写 HBM，标为“合理推论”。
- 如果只知道公司在该行业，没有原文，标为“待原文核验”。
- 如果公司主要是泛半导体 beta、泛材料、泛存储周期，标为“主要反证”。
输出：company, ticker, country, bfs_depth, node, dependency_edge, evidence_state, upgrade_condition, downgrade_condition。

失败模式

failure mode	防错规则
把 D4 上游泛 WFE 升级成 D2 瓶颈	必须证明 reverse constraint：没有该环节会卡住 HBM/CoWoS
把收入增长归因 AI	必须有 segment / customer / product / keyword 原文
把所有 ABF 供应商归入 HBM	必须证明 AI/HPC package substrate mix 或客户
忽略反证	每条边必须有 downgrade condition
4.5 Evidence card writer agent

输入字段

field	内容
verified_sources	原文来源
metrics	revenue, orders, backlog, margin, capex, capacity
classified_edges	dependency classifier 输出
quote_bank	原文摘录
unresolved_items	未核验点

输出字段

field	内容
evidence_card	标准证据卡
conclusion_layering	原文已证明 / 合理推论 / 待原文核验 / 主要反证
quarterly_monitor	下一季度要看什么
source_gap	还缺什么原文

prompt 模板

你是 HBM evidence card writer agent。
为 {company} 写一张原始出处证据卡。
必须包括：
1. 基本信息：company, ticker, country, BFS depth, node, source type, period, release date, link。
2. 原文证据表：metric, quote, source location, what it proves, what it does not prove, scope note。
3. 结论分层：
   - 原文已证明；
   - 合理推论；
   - 待原文核验；
   - 主要反证。
4. 下一步核验：最重要的 3 个原文来源和关键词。
禁止输出买卖建议、目标价或估值结论。

失败模式

failure mode	防错规则
卡片只写结论没有原文	拒绝输出“原文已证明”
指标口径混乱	标注 GAAP/non-GAAP、segment、币种、期间
把 customer rumor 当事实	必须是客户或供应商 cross-disclosure
漏掉不能证明什么	每条 quote 必填 does_not_prove
4.6 Refutation reviewer agent

输入字段

field	内容
evidence_cards	evidence card writer 输出
candidate_table	候选公司表
industry_counter_signals	HBM pricing, capex, qualification, CoWoS capacity, tester utilization
competitor_list	同节点竞争者

输出字段

field	内容
refutation_dashboard	反证仪表盘
red_flags	证据红旗
downgrade_triggers	降级条件
alternative_explanations	传统周期 / 库存 / 泛半导体 beta
priority_next_checks	下一轮核验优先级

prompt 模板

你是 HBM refutation reviewer agent。
你的任务不是强化叙事，而是找反证。
对每个公司/节点问：
1. 是否可能只是泛半导体设备 beta？
2. 是否可能只是传统存储周期反转？
3. 是否缺少 HBM 客户、订单、收入、毛利率证据？
4. 是否有技术路线替代：TCB → hybrid bonding、MR-MUF → other underfill、CoWoS → alternative package、ABF → glass substrate？
5. 如果 HBM 供给过剩、AI capex 放缓、NVIDIA/ASIC roadmap 改变，该公司先受损还是仍是瓶颈？
输出：company, strongest evidence, weakest evidence, refutation, downgrade trigger, next source to read。

失败模式

failure mode	防错规则
只列风险不列可验证指标	每个风险要有可跟踪原文指标
不区分结构性与周期性	标注 structural / cyclical / narrative
忽略客户集中	必查 customer concentration 和 backlog quality
忽略路线切换	必查 HBM4 / HBM4E 技术路线
5. Search queries：可执行搜索模板

以下 query 用来找公司原文、IR、产品页和 cross-disclosure。优先加 site:company.com、site:investors.company.com、site:sec.gov、filetype:pdf，避免先读媒体。

site:skhynix.com HBM4 12-high MR-MUF hybrid bonding investor presentation
site:skhynix.com HBM3E HBM4 annual report capacity gross margin
site:skhynix.com HBM4 16-high MR-MUF hybrid bonding
site:semiconductor.samsung.com HBM4 12H 16H TC NCF bandwidth
site:samsung.com HBM3E HBM4 earnings call qualification capacity
site:micron.com HBM4 12-high NVIDIA Vera Rubin annual report
site:micron.com HBM3E 12-high power bandwidth investor presentation
site:nvidia.com GB300 HBM3E memory NVL72 bandwidth
site:nvidia.com GB200 NVL72 HBM3E 13.4TB bandwidth
site:tsmc.com CoWoS HBM stacks reticle size annual report
site:tsmc.com CoWoS 14 reticle 20 HBM stacks 2028
site:tsmc.com HBM4 base die N12 N3 CoWoS
site:aseglobal.com HBM CoWoS 2.5D FOCoS investor presentation
site:amkor.com HBM 2.5D AI processor advanced packaging annual report
site:amkor.com HBM advanced packaging investor presentation AI
site:hanmisemi.com HBM4 TC BONDER annual report backlog
site:hanmisemi.com DUAL TC BONDER HBM AI 2.5D
site:asmpt.com TCB HBM annual results investor presentation
site:asmpt.com FIREBIRD TCB HBM HPC AI
site:besi.com hybrid bonding HBM 2.5D AI datacenter orders
site:besi.com annual report hybrid bonding HBM AI 2.5D
site:suss.com HBM temporary bonding advanced packaging AI chip modules
site:suss.com investor presentation HBM advanced packaging lithography
site:towajapan.co.jp HBM4 MR-MUF compression molding investor presentation
site:towajapan.co.jp HBM molding MUF generative AI semiconductor
site:disco.co.jp HBM thinning dicing grinding TSV reveal technical brief
site:disco.co.jp ultra-thinned memory HBM laser saw annual report
site:advantest.com HBM4 known good die memory tester financial briefing
site:advantest.com HBM memory tester investor presentation AI
site:teradyne.com HBM memory test AI accelerator annual report
site:formfactor.com HBM probe card SK hynix investor presentation
site:formfactor.com HBM wafer probe known good die probe card
site:mpi.com.tw HBM probe card investor presentation
site:winwayglobal.com HBM probe card annual report
site:leeno.com HBM test socket memory module AI
site:isc21.kr HBM test socket investor presentation
site:tse21.com HBM probe card memory test investor presentation
site:chromaate.com HBM memory test AI accelerator investor presentation
site:camtek.com HBM advanced packaging inspection annual report
site:camtek.com CoWoS HBM inspection metrology AI
site:novami.com HBM advanced packaging hybrid bonding investor presentation
site:ontoinnovation.com HBM advanced packaging inspection metrology annual report
site:kla.com HBM GPU interposer advanced packaging process control
site:ibiden.com ABF substrate HBM AI annual report
site:ibiden.com IC package substrate AI GPU HBM investor presentation
site:shinko.co.jp HBM package substrate molded underfill AI
site:unimicron.com ABF substrate HBM AI CoWoS investor presentation
site:nanyapcb.com.tw ABF substrate HBM CoWoS capacity
site:kinsus.com.tw ABF substrate HBM AI investor presentation
site:ajinomoto.com ABF high-performance semiconductor data center AI
site:resonac.com HBM NCF TIM underfill advanced packaging annual report
site:namics.co.jp HBM underfill advanced packaging narrow gap
site:sumibe.co.jp HBM molding compound advanced packaging annual report
site:rambus.com HBM4 controller PHY AI accelerator product brief
site:marvell.com custom HBM compute architecture Micron Samsung SK hynix
site:broadcom.com HBM controller custom AI ASIC CoWoS
site:guc-asic.com HBM PHY controller CoWoS AI ASIC
site:alchip.com HBM CoWoS AI ASIC investor presentation
site:cadence.com HBM4 PHY 3D-IC CoWoS AI accelerator
site:synopsys.com HBM4 PHY controller 3DIC AI accelerator
site:sec.gov 10-K HBM memory test customer concentration
site:sec.gov 10-K probe card HBM AI infrastructure
site:sec.gov 10-K advanced packaging HBM CoWoS inspection
site:sec.gov 10-K ABF substrate AI HBM capacity
6. 当前主线结论分层

原文已证明

HBM 是 AI accelerator 的直接 D2 瓶颈之一：NVIDIA GB200/GB300 系统级 HBM 容量和带宽、SK hynix / Samsung / Micron HBM3E-HBM4 产品路线、TSMC CoWoS-HBM stack roadmap 都有公司原文支持。
台积电
+4
NVIDIA
+4
SK hynix
+4

合理推论

HBM4、12-high、16-high、CoWoS reticle size 扩大，会把瓶颈从 memory vendor 外溢到 TC bonding、hybrid bonding、wafer thinning/dicing、KGD/memory test、probe card、ABF substrate、inspection/metrology、underfill/molding/thermal materials。该推论由多个节点的产品原文支持，但不是每家公司都已证明 HBM revenue 或订单。项目框架也强调，真正弹性往往来自被 GPU 集群规模放大、扩产慢、认证强、单位价值量上升的瓶颈环节。

2026-05-12-ai-super-cycle-resea…

待原文核验

多数 D3-D4 小中盘公司仍缺三个关键证据：第一，HBM / HBM4 / CoWoS / AI accelerator 是否出现在公司原文；第二，是否有订单、backlog、capacity、qualification 或客户交叉披露；第三，收入和毛利率是否真的因 HBM 上升，而不是传统 memory / WFE / substrate 周期修复。

主要反证

如果 HBM 供给过剩、Samsung/Micron 快速补足导致 ASP 下行、CoWoS 扩产解除封装瓶颈、HBM4 技术路线降低某类设备/材料价值量、tester/probe 订单只是一次性 pull-forward，或 AI capex 放缓，则 D3-D4 高弹性链条会从“结构性瓶颈”降级为“周期 beta / 主题映射”。项目 checklist 中也明确要求把 HBM、server DRAM、eSSD、NAND、controller/test/material 拆开，不能把所有 memory 上涨都归因于 AI。

research-checklist
