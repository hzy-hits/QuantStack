# ChatGPT Pro 输出草稿：CoWoS / Advanced Packaging 瓶颈

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 11:52:09 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a029ee4-a864-83ea-b7d1-344f38f40444  
项目：ai super cycle

## 使用边界

- 本文件是 ChatGPT Pro 对项目研究任务的输出草稿。
- 不作为投资建议、买卖建议或已验证结论。
- 所有涉及财报数字、订单、backlog、CapEx、产能、价格、客户关系、技术路线的内容，必须回到公司原始披露或权威原文核验后才能进入正式研究结论。
- 本文件保留模型输出中的可疑点，后续核验时应优先处理。

## 原始提问

```text
主题2：CoWoS / 2.5D / Advanced Packaging 产能瓶颈研究。

请基于本项目已上传的 md 来源文件，研究 CoWoS / 2.5D / advanced packaging 是否仍是 AI 芯片出货瓶颈，以及瓶颈是否会从 TSMC 转向 substrate、interposer、设备、材料、测试。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 重点围绕 AI accelerator、HBM、CoWoS、2.5D、hybrid bonding、TCB、ABF/substrate。

重点公司：
TSMC、ASE、Amkor、Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、AT&S、BESI、SUSS、ASMPT、TOWA、Disco、Camtek、Nova。

输出：原始出处清单、CoWoS 供应链地图、各环节供给瓶颈、HBM4/HBM4E 对设备/材料影响、财报验证指标、反证指标。
展开
```

## ChatGPT Pro 输出

0. 证据边界

我在本项目已上传文件中没有找到可直接作为“公司原始出处”的 CoWoS / 2.5D / advanced packaging md 证据；已检索到的文件主要是无关论文、个人项目 README、交易截图，以及一份偏 CPO/光子供应链的二级整理材料，不足以支撑本题结论。因此下面按你的要求先列“必须核验的公司原始出处”，并把结论分为 原文已证明 / 合理推论 / 待原文核验 / 主要反证。我没有把媒体转述或模型记忆当作结论。

1. 需要核验的公司原始出处清单
公司 / 环节	需要优先核验的原始出处	当前证据状态	结论用途
TSMC	季度 earnings call transcript、annual report、advanced packaging / CoWoS 技术页、capex commentary	原文已证明	判断 CoWoS / advanced packaging 是否仍是瓶颈的核心原文。TSMC 原文称 AI demand 强、supply very tight、advanced packaging capacity very tight，并提到与 OSAT partners 扩产。TSMC 也说明大尺寸 CoWoS 仍是当前主供给形态，CoPoS 仍在 pilot / production 过渡阶段。
台积电
+2
台积电
+2

ASE	ASE earnings presentation、annual report / 20-F、advanced packaging / LEAP / test commentary	待原文核验	必须核验其先进封装、测试、AI 相关 capex / backlog / utilization。当前没有足够可引用原文，不把 ASE 当已证结论。
Amkor	Q1 2026 results、earnings presentation、Form 10-K	原文已证明	可证明 OSAT 侧 advanced packaging / test 是 AI/HPC 供应链的一部分，并且 Amkor advanced products、computing、capex 指引在扩张。
Amkor Technology
+2
Amkor Technology
+2

Ibiden	FY2025/26 results、2026–2028 investment plan、corporate profile	原文已证明	可证明 AI / high-performance server 用 IC package substrate 是明确扩产对象，且大尺寸、多层、fine circuit、component embedding 是技术方向。
Ibiden
+2
Ibiden
+2

Shinko	FY2024 financial results、IR / delisting disclosure、post-delisting owner disclosure	原文已证明，但最新性受限	是 substrate / package 端的重要反证之一：FY2024 flip-chip package 仍受服务器复苏延迟等影响；且 2025 年退市后公开持续披露可见度下降。
信光株式会社
+1

Unimicron	Annual report、quarterly presentation、MOPS / investor call、ABF / FC-BGA capacity disclosure	待原文核验	市场常把它放入 AI ABF substrate 瓶颈，但本轮未取得足够原文，不纳入“已证明”。
Nan Ya PCB	Annual report、monthly sales、quarterly presentation、ABF / BT / CCL commentary	待原文核验	需核验 ABF substrate、CCL / copper foil / glass fabric 是否成为 AI package 限制。
Kinsus	Financial statement、monthly revenue、annual meeting materials、ABF / FC-BGA expansion disclosures	部分原文，细节待核验	可见 2026 年月度营收明显增长，但 AI / ABF 高阶 substrate 具体利用率、产能和瓶颈需进一步原文核验。
kinsus.com.tw
+1

AT&S	Q3 FY2025/26 results、full-year outlook、glass core substrate release	原文已证明	关键材料 / substrate 原文：AT&S 明确提示 E-glass / T-glass 风险，称 T-glass 对大尺寸复杂 IC substrate 不可或缺，并提示 H2 FY2026/27 可能无法完全满足升级后的客户要求；同时推进 glass core substrate。
AT&S
+1

BESI	Q1 2026 results、hybrid bonding / TCB product disclosures	原文已证明	可证明 hybrid bonding 设备订单显著上升，且 2.5D AI computing applications 拉动出货。
Besi
+1

SUSS	Q1 2026 results、2026 investor presentation、TBDB / UV scanner / hybrid bonding disclosures	原文已证明	可证明 AI chip module value chain 拉动订单；CoWoS 相关 UV projection scanner、temporary bonding / debonding、hybrid bonding 与 HBM4/HBM4E/HBM5 路线相关。
SUSS
+3
SUSS
+3
SUSS
+3

ASMPT	FY2025 results、Q1 2026 results、TCB product / 1H2025 commentary	原文已证明	可证明 TCB 是 AI / HBM 关键设备环节；ASMPT 称 TCB revenue 创纪录、HBM4 12H 已获多家客户订单，HBM4 16H 开发推进，fluxless TCB 处于 qualification。
ASMPT
+3
ASMPT
+3
ASMPT
+3

TOWA	FY2025 results、molding / compression molding / singulation product disclosure	原文已证明	可证明 AI-related logic / HBM 拉动 molding equipment，且下一代 AI logic / PLP、compression equipment 是后续验证点。
Towajapan
+3
Towajapan
+3
Towajapan
+3

DISCO	FY2025 Q4 results、shipment notes、FY2026 Q1 outlook	原文已证明	可证明 generative AI 和 OSAT demand 拉动 dicer / grinder / consumables，且出货值处于高位。
迪斯科
+2
迪斯科
+2

Camtek	Order releases、annual report / 20-F、advanced packaging inspection disclosures	原文已证明	可证明 OSAT CoWoS-like packaging、HBM、chiplets、hybrid bonding、micro-bump inspection 需求增强。
Camtek
+3
Camtek
+3
Camtek
+3

Nova	FY2025 results、20-F、investor presentation	原文已证明	可证明 advanced packaging、hybrid bonding、TSV / RDL、panel-level metrology、chemical metrology 的 process control intensity 提升。
Nova
+2
Nova
+2

HBM 原厂：Micron / Samsung / SK hynix	HBM4 / HBM4E launch、sampling、ramp、custom HBM disclosures、JEDEC HBM4 standard	Micron / Samsung 原文已证明；SK hynix 待核验	Micron 已称 HBM4 12H 36GB 进入 high-volume production 用于 NVIDIA Vera Rubin；Samsung 称 HBM4E 在 2026 年下半年 sampling。SK hynix 相关原文仍需补齐。
Samsung Global Newsroom
+3
Micron Technology
+3
美光科技
+3
2. 核心结论：CoWoS 仍是瓶颈，但瓶颈不是简单“从 TSMC 转走”
2.1 原文已证明

第一，TSMC / CoWoS / advanced packaging 仍是 AI accelerator 出货的一级瓶颈。
TSMC 原文对 2026 年 AI demand、capacity、advanced packaging 的措辞很强：AI demand robust，supply very tight，advanced packaging capacity very tight，并且明确提到正在与 OSAT partners 扩大 advanced packaging capacity。TSMC 还强调当前主力仍是大尺寸 CoWoS，CoPoS / panel-level 形态还处于 pilot 到后续量产阶段。因此，“CoWoS 已经不再是瓶颈”这个说法缺乏原文支持。
台积电
+2
台积电
+2

第二，瓶颈已从单一 TSMC CoWoS capacity，扩散成多节点并联约束：ABF / substrate、T-glass、TCB、hybrid bonding、temporary bonding / debonding、dicing / grinding、molding、inspection / metrology。
Ibiden 原文把 2026–2028 年约 5000 亿日元投资指向 AI server / high-performance server 用 high-performance IC package substrate；AT&S 原文则直接提示 T-glass 对 large-format / complex IC substrates 不可或缺，且 H2 FY2026/27 存在无法完全满足升级客户要求的风险。这说明 substrate / 材料端已经有原文级瓶颈信号。
Ibiden
+2
Ibiden
+2

第三，设备端的瓶颈信号很强，尤其是 TCB、hybrid bonding、temporary bonding / debonding、CoWoS lithography / scanner、molding、dicing / grinding、inspection / metrology。
BESI Q1 2026 将收入和订单增长部分归因于 2.5D AI computing 和 hybrid bonding systems；SUSS Q1 2026 order intake 同比大增，且投资者材料把 CoWoS-related UV projection scanner、TBDB、hybrid bonding 与 HBM4/HBM4E/HBM5 路线绑定；ASMPT FY2025 披露 TCB revenue 创纪录、HBM4 12H 订单来自多家玩家、HBM4 16H 开发推进。
ASMPT
+5
Besi
+5
SUSS
+5

第四，测试 / inspection / metrology 不是“可忽略尾端”，但“纯 final test 产能瓶颈”还未被充分证明。
Camtek 的原文订单直接指向 OSAT 的 CoWoS-like packaging for AI，以及 HBM、chiplets、hybrid bonding、micro-bumps；Nova 原文显示 advanced packaging、hybrid bonding、TSV / RDL、chemical metrology 的 process control 强度上升。Amkor 原文证明 advanced packaging / test 是 AI/HPC OSAT 供应链的一部分，但本轮还没有足够原文证明 final test 本身已经替代 CoWoS 成为主瓶颈。
Amkor Technology
+4
Camtek
+4
Camtek
+4

2.2 合理推论

更准确的表述不是“瓶颈从 TSMC 转向 substrate / interposer / 设备 / 材料 / 测试”，而是：

TSMC 仍是总装配能力和先进制程交付的核心 gatekeeper；但随着 TSMC 扩 CoWoS、OSAT 参与、HBM4/HBM4E 推进，新增限制更可能出现在高阶 substrate、T-glass / glass core、TCB / hybrid bonding 设备、temporary bonding、dicing / grinding、molding、inspection / metrology、以及部分 test cell。

这个推论来自两类原文的交叉：一边是 TSMC 说 advanced packaging capacity very tight 并需要 OSAT partners；另一边是 substrate 和设备公司同时出现 AI / HBM / 2.5D / advanced packaging 订单、capex、shipment、backlog 的增长信号。
ASMPT
+4
台积电
+4
Ibiden
+4

2.3 待原文核验

以下不能直接下结论：

ASE 是否已成为 CoWoS / advanced packaging 的新增硬瓶颈。 需要 ASE 原文披露 LEAP、advanced packaging、test utilization、AI customer program、capex allocation。

Unimicron / Nan Ya PCB / Kinsus 是否已经成为 ABF substrate 的主瓶颈。 Kinsus 可见月度收入增长，但 AI / ABF 高阶层数、yield、客户拉货、产能利用率仍需公司原文。

Interposer 是否从 TSMC 转向第三方瓶颈。 TSMC CoWoS-S 原文说明 silicon interposer 是 CoWoS 平台核心，但本轮未取得足够原文证明外部 interposer supplier 已替代 TSMC 成为瓶颈。
3DFabric
+1

Final test 是否是独立主瓶颈。 Amkor / Camtek / Nova 支持 OSAT、inspection、metrology 重要性上升，但 final test hours / ATE / burn-in / SLT 是否卡 AI accelerator 出货，需要 Advantest / Teradyne / Amkor / ASE / chip customer 原文进一步核验。

2.4 主要反证

最重要反证是 TSMC 对短期材料风险的表态。
TSMC Q1 2026 原文称 materials supply risk “mostly mitigated”，目前没有看到 near-term materials impact。这与“材料已经全面成为当前主瓶颈”的说法相冲突。因此材料端应拆成两层：短期全局材料瓶颈未被 TSMC 证明；但 T-glass / high-end substrate 材料在 AT&S 原文中已有中期风险信号。
台积电
+1

Shinko 是 substrate 端另一个反证。
Shinko FY2024 原文称 AI 需求增长、AI 半导体 capex 强，但 flip-chip package 销售仍因服务器复苏延迟等因素下降。这说明 substrate / package 不是全行业同步短缺，瓶颈更可能集中在特定客户、特定规格、特定高层数 / 大尺寸 / 高速材料，而不是所有 FC-BGA / ABF 产能都紧缺。
信光株式会社

3. CoWoS / 2.5D / advanced packaging 供应链地图
层级	关键工艺 / 物料	相关公司	当前瓶颈判断
AI accelerator logic die	N3 / N2 / reticle-size die、HBM base die	TSMC	仍是一级瓶颈。 TSMC 表示 AI demand 强、N3 / N2 / capacity very tight，并强调大型 AI superchips 的机械、热、warpage 挑战。
台积电
+1

HBM stack	HBM3E → HBM4 → HBM4E，12H / 16H，higher I/O、finer pitch、thinner die	Micron、Samsung、SK hynix 待补	HBM 本身仍是并列瓶颈。 Micron HBM4 进入量产，Samsung HBM4E 2026 H2 sampling，说明 HBM4/HBM4E 路线正在进入真实 supply-chain ramp。
Micron Technology
+2
Samsung Global Newsroom
+2

Silicon interposer / RDL / TSV	CoWoS-S silicon interposer、RDL、TIV、large interposer	TSMC, SUSS, Nova	TSMC 控制的 2.5D 核心环节仍紧。 TSMC CoWoS-S 原文强调 silicon interposer、高密度互连、logic chiplets + HBM cubes；Nova / SUSS 则反映 RDL / TSV / lithography / metrology 强度提升。
Nova
+3
3DFabric
+3
台积电
+3

ABF / FC-BGA substrate	大尺寸、高层数、fine circuit、T-glass、glass core	Ibiden, Shinko, Unimicron, Nan Ya PCB, Kinsus, AT&S	高阶 substrate 是明确候选瓶颈。 Ibiden 扩 AI server substrate；AT&S 提示 T-glass 风险；Shinko 提供反证，显示不是所有 substrate 都紧。
Ibiden
+2
AT&S
+2

Assembly / OSAT	CoWoS assembly、HDFO、2.5D integration、test	TSMC, ASE, Amkor	TSMC 主导 + OSAT 扩张。 TSMC 明确与 OSAT partners 增加 advanced packaging capacity；Amkor advanced packaging / test 和 AI datacenter computing 程序在扩。ASE 待核验。
台积电
+2
Amkor Technology
+2

TCB / hybrid bonding	Chip-to-substrate、chip-to-wafer、HBM stacking、logic / memory bonding	BESI, ASMPT, SUSS	设备瓶颈强。 BESI hybrid bonding 订单显著上升；ASMPT TCB 在 HBM4 12H / 16H 与 logic 端推进；SUSS 把 hybrid bonding 与 HBM4E/HBM5 路线绑定。
SUSS
+3
Besi
+3
ASMPT
+3

Temporary bonding / debonding、thinning	HBM DRAM thinning、wafer support、stacking 前处理	SUSS, DISCO	HBM4/HBM4E 层数增加会放大该环节需求。 SUSS 称 TBDB 对 HBM die thinning 仍关键，层数增加会触发更多 TBDB capacity；DISCO 出货受 generative AI / OSAT demand 拉动。
SUSS
+1

Molding / compression / singulation	HBM / AI logic 封装保护、warpage control、PLP	TOWA, ASMPT	后段封装能力重要性上升。 TOWA 称 AI-related logic / HBM 拉动 molding equipment；ASMPT TCB 产品强调 warpage / thermal management。
Towajapan
+1

Inspection / metrology / chemical process control	Micro-bump、hybrid bonding pre/post、TSV/RDL、panel-level、plating chemistry	Camtek, Nova	质量控制可能成为隐性产能约束。 Camtek 获 CoWoS-like AI packaging OSAT 订单；Nova 指出 advanced packaging 从 2D FO 到 2.5D interposer 到 3D HB 会提高 process control intensity。
Camtek
+2
Camtek
+2
4. 各环节供给瓶颈判断
环节	当前判断	证据等级	关键理由
TSMC CoWoS / advanced packaging	仍是瓶颈	原文已证明	TSMC 直接称 advanced packaging capacity very tight，并与 OSAT partners 扩产。
台积电

AI accelerator front-end wafer / base die	与 CoWoS 并列紧张	原文已证明	TSMC 对 N3 / N2 / AI demand / capacity 的表述显示前端并未完全宽松。
台积电
+1

HBM	仍是并列瓶颈，但不是本题唯一焦点	原文已证明	Micron / Samsung 原文显示 HBM4 / HBM4E 正进入 ramp / sampling；HBM4 性能和接口升级会加重封装侧复杂度。
Micron Technology
+2
美光科技
+2

ABF / high-end substrate	很可能成为 CoWoS 扩产后的下一层瓶颈	原文已证明 + 合理推论	Ibiden 大额投资 AI server substrate；AT&S 提示 T-glass 风险；但 Shinko 反证说明瓶颈不均匀。
Ibiden
+2
AT&S
+2

Interposer	TSMC 内部 CoWoS 能力仍是主要 gate；外部 interposer 转移未证	待原文核验	CoWoS-S 原文证明 silicon interposer 是核心，但未证明第三方 interposer 已是独立主瓶颈。
3DFabric
+1

TCB	明确瓶颈候选	原文已证明	ASMPT TCB revenue 创纪录、HBM4 12H 订单、HBM4 16H 开发；BESI 也覆盖 TCB / 2.5D equipment。
ASMPT
+2
ASMPT
+2

Hybrid bonding	中期瓶颈候选增强	原文已证明	BESI hybrid bonding systems 订单显著增加；SUSS 指出 hybrid bonding 将逐步替代 TCB 用于 DRAM stacking，并用于 HBM / CPO / SoIC。
Besi
+2
SUSS
+2

Temporary bonding / debonding	HBM 层数增加下的关键设备瓶颈	原文已证明	SUSS 明确 TBDB 对 HBM die thinning 仍关键，层数增加触发更多 capacity。
SUSS

Dicing / grinding / consumables	设备与耗材双重受益，可能限制节拍	原文已证明	DISCO 称 generative AI / OSAT demand 拉动 equipment shipment，耗材因设备高利用率创高。
迪斯科

Molding / compression / singulation	后段封装瓶颈候选	原文已证明	TOWA 原文称 AI-related logic / HBM 拉动 molding equipment，下一代 AI logic / PLP 有望带来 compression demand。
Towajapan
+1

Inspection / metrology	隐性 bottleneck 候选，尤其是 micro-bump / hybrid bonding / RDL / TSV	原文已证明	Camtek CoWoS-like AI OSAT 订单、Nova advanced packaging process control 强度提升。
Camtek
+1

材料	短期全局瓶颈未证明；T-glass / high-end substrate 材料中期风险已证明	混合证据	TSMC 称短期材料风险 mostly mitigated；AT&S 称 T-glass 可能无法满足 H2 FY2026/27 升级客户要求。
台积电
+1

Final test / burn-in / SLT	候选瓶颈，但本轮证据不足	待原文核验	Amkor 证明 test 是 OSAT 服务组成部分，但 final test 是否卡 AI accelerator 出货需要更多 ATE / SLT / burn-in 原文。
Amkor Technology
5. HBM4 / HBM4E 对设备和材料的影响
HBM4 / HBM4E 变化	对 CoWoS / 2.5D 封装的影响	受影响环节 / 公司
更高 bandwidth、更宽 interface、更高 I/O	bump / interconnect 密度上升，RDL / TSV / interposer / substrate routing 更难	TSMC, SUSS, Nova, Camtek
12H → 16H、更薄 die、更高 stack count	die thinning、temporary bonding / debonding、warpage control、stack yield 更关键	SUSS, DISCO, ASMPT, TOWA
HBM4 仍大量使用 TCB，HBM4E/HBM5 逐步导入 hybrid bonding	TCB 不是立刻消失，而是与 hybrid bonding 共存；hybrid bonding 中期弹性更强	ASMPT, BESI, SUSS
更大 package / 更大 interposer / 更多 HBM cubes	CoWoS 尺寸、substrate 尺寸、T-glass / glass core、thermal / mechanical stress 更重要	TSMC, Ibiden, AT&S, Unimicron, Nan Ya PCB, Kinsus
先进封装 process control 强度提升	pre/post hybrid bonding、micro-bump、TSV/RDL、plating chemistry、panel-level metrology 需求上升	Camtek, Nova
热管理与翘曲更难	underfill、mold compound、TIM、compression molding、substrate flatness / CTE matching 成为良率变量	TOWA, ASMPT, AT&S, TSMC

Micron 原文显示 HBM4 36GB 12H 已进入 high-volume production 用于 NVIDIA Vera Rubin，Samsung 原文显示 HBM4E 2026 H2 sampling；SUSS 和 ASMPT 的原文则把 HBM4/HBM4E 的工艺升级直接映射到 TBDB、TCB、hybrid bonding、thinner dies、higher stack count、finer pitch 等设备需求。
ASMPT
+3
Micron Technology
+3
Samsung Global Newsroom
+3

6. 财报验证指标
6.1 TSMC

重点看：

advanced packaging capacity 是否继续被描述为 “very tight”；

CoWoS capacity、CoWoS-L / CoWoS-S、SoIC、CoPoS / panel-level packaging 的量产节奏；

HPC / AI accelerator revenue CAGR、AI customer pull-in、customer prepayment；

capex 中 advanced packaging / specialty / backend 相关比例；

N3 / N2 / HBM base die capacity 是否仍被点名紧张；

是否继续强调 OSAT partners 共同扩产。
台积电
+2
台积电
+2

6.2 OSAT：ASE / Amkor

重点看：

advanced packaging revenue mix；

AI / HPC / datacenter computing program ramp；

packaging vs test revenue split；

capex guidance、equipment lead time、utilization；

HDFO、2.5D integration、CoWoS-like、LEAP、test capacity 的具体订单和客户进度。Amkor 已披露 advanced products、computing、capex guidance 与 advanced packaging / test 相关信息；ASE 仍需原文补齐。
Amkor Technology
+1

6.3 Substrate：Ibiden / Shinko / Unimicron / Nan Ya PCB / Kinsus / AT&S

重点看：

ABF / FC-BGA 高阶 substrate revenue mix；

AI server / GPU / ASIC / CPU substrate 出货；

substrate 面积、层数、fine circuit、embedded component、large body size；

T-glass、ABF film、glass fabric、copper foil、resin availability；

capacity expansion start-of-production、ramp yield、utilization；

substrate ASP / gross margin 是否改善；

inventory / WIP 是否积压或消化；

客户是否升级规格导致原材料 / 产能无法满足。Ibiden 和 AT&S 已提供较强原文；Shinko 提供反证；Unimicron / Nan Ya PCB / Kinsus 仍需补足具体 AI substrate 原文。
kinsus.com.tw
+4
Ibiden
+4
Ibiden
+4

6.4 设备：BESI / SUSS / ASMPT / TOWA / DISCO

重点看：

book-to-bill、order intake、backlog、shipment value；

TCB、hybrid bonding、temporary bonding / debonding、UV projection scanner、molding、compression、dicing、grinding 的订单拆分；

HBM3E vs HBM4 vs HBM4E 设备项目节奏；

tool lead time、客户 qualification、AOR / process-of-record；

gross margin 是否受 initial delivery cost、new system ramp、capacity expansion 影响；

installed base、service / consumables revenue。
迪斯科
+4
Besi
+4
SUSS
+4

6.5 Inspection / metrology：Camtek / Nova

重点看：

CoWoS-like / HBM / chiplet / hybrid bonding 订单；

micro-bump inspection、wafer-level inspection、RDL / TSV / panel-level metrology；

chemical metrology for plating / advanced packaging；

OSAT vs foundry vs IDM 客户结构；

tool shipment timing 与 customer acceptance。
美国证券交易委员会
+3
Camtek
+3
Camtek
+3

7. 反证指标

以下指标若连续出现，会削弱“CoWoS / advanced packaging 仍是主要瓶颈”或“瓶颈扩散到 substrate / equipment / test”的判断：

TSMC 不再使用 advanced packaging capacity very tight / supply very tight 这类措辞，并开始说明 CoWoS lead time 明显缩短。

TSMC capex pull-in 减弱，advanced packaging / CoWoS / OSAT partner 扩产节奏放缓。

AI accelerator 客户不再抱怨 CoWoS / HBM / packaging allocation，而是转为需求端库存修正。

Amkor / ASE advanced packaging utilization 下滑，capex 下修，test / packaging backlog 下降。

Ibiden / AT&S / Kinsus / Unimicron / Nan Ya PCB 的高阶 ABF substrate 毛利率、ASP、利用率不改善或回落，同时没有 T-glass / ABF film 紧张表述。

AT&S 后续撤回 T-glass bottleneck 风险，或称 additional suppliers 已完全满足 upgraded customer requirements。

BESI / SUSS / ASMPT 订单回落，尤其是 hybrid bonding、TCB、TBDB、CoWoS scanner 订单取消或延后。

DISCO 出货 / consumables 不再由 generative AI / OSAT 拉动，说明 dicing / grinding 不再处于高利用状态。

Camtek / Nova advanced packaging / HBM / hybrid bonding 订单放缓，说明 inspection / metrology 未形成约束。

Shinko 类反证扩大：若更多 substrate 公司披露 server / flip-chip package 需求延迟、库存调整、价格压力，则说明 substrate 瓶颈只存在于少数高阶规格，而不是行业级短缺。
信光株式会社

8. 最终判断

结论：CoWoS / 2.5D / advanced packaging 仍是 AI accelerator 出货瓶颈，但瓶颈形态已经从“TSMC 单点 CoWoS capacity”演化为“TSMC 主瓶颈 + OSAT / substrate / equipment / metrology / 材料并联瓶颈”。

最强已证链条是：

AI accelerator demand 强 → TSMC advanced packaging capacity very tight → 大尺寸 CoWoS 仍是主供给 → TSMC 需要 OSAT partners → HBM4/HBM4E 提高 I/O、层数、die thinning、bonding 和良率难度 → 高阶 substrate / T-glass / TCB / hybrid bonding / TBDB / dicing / molding / inspection / metrology 同步成为扩产约束。

但要避免过度外推：**材料端短期全局短缺未被 TSMC 原文证明；interposer 外部转移未被证明；final test 成为主瓶颈也未被充分证明。**目前更稳健的研究框架是把这些环节作为财报验证对象，而不是先验结论。
