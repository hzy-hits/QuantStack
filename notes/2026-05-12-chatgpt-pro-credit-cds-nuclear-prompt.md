# ChatGPT Pro Prompt: Credit/CDS + Nuclear/Firm Power Radar

状态：prompt draft for ChatGPT Pro  
日期：2026-05-12

## Prompt

我们在做一个 `ai super cycle / AI Infra` 研究项目。当前主框架是从 LLM 核心需求源头出发做 `D0-D5 LLM Dependency BFS`：

- D0：OpenAI、Anthropic、Google DeepMind / Gemini 等 LLM 需求源头；
- D1：GPU/CUDA、TPU、custom ASIC、hyperscaler/NeoCloud、训练/推理软件栈；
- D2：HBM、CoWoS、TSMC、AI server/rack、networking、800G/1.6T optics、data center power/cooling；
- D3：HBM equipment/test/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力关键部件；
- D4-D5：材料、气体、真空、洁净、能源、电网、融资、监管等深层雷达。

我们已经建立了一个 146 家全球可交易 universe，并且启动了 D1-D3 的原文核验 MVP。但现在发现两个缺口：

1. **Credit/CDS 没有成体系**  
   现有只是零散写了融资、债务、客户集中，没有完整的信用风险队列。我们需要把 CDS、公司债、bond spread、debt maturity、lease liabilities、interest expense、GPU residual value、customer contract quality、take-or-pay、prepayment、backlog quality 纳入 AI Infra 反证系统。

2. **Nuclear / firm power 覆盖太浅**  
   现有只有 LEU、CCJ、BE、GEV、Siemens Energy 等少量雷达，缺 CEG、VST、TLN、NRG、OKLO、SMR、BWXT 等关键观察对象。我们需要判断 AI data center 的电力约束是否会传导到 nuclear、PPA、firm power、gas turbine、SMR、uranium、HALEU、grid transmission。

请你做一个深度研究框架，不是投资建议，不给买卖建议，不给目标价。

请回答：

## 1. Credit / CDS / Financing Risk Radar

请建立一个 AI Infra credit risk 研究框架：

- 哪些公司/资产类型必须纳入：NeoCloud、AI data center developers、IDC/colo REIT、hyperscalers、power equipment、utilities、GPU leasing、private credit；
- 哪些指标最关键：CDS spread、bond yield/OAS、debt maturity wall、lease liabilities、interest coverage、capex commitments、OCF/FCF、GPU residual value、depreciation policy、customer concentration、contract terms；
- 如果拿不到单名 CDS，应该用哪些免费/可获得 proxy：公司债、ETF、CDX IG/HY、HYG/LQD、credit ETF flows、SEC debt footnotes、convertible bonds、options IV/skew；
- 哪些信号说明 AI Infra 从结构性投资变成信用泡沫；
- 哪些信号反而说明重资产扩张仍然健康；
- 如何把 Credit/CDS radar 和我们现有 D0-D5 BFS 结合；
- 输出一个公司/资产池表，包括：
  - ticker / asset；
  - 类型；
  - BFS 位置；
  - 为什么相关；
  - 应找的原文；
  - 应核验指标；
  - 升级条件；
  - 降级条件；
  - 是否适合进入核心、候选、雷达或只做 proxy。

候选至少覆盖：
CoreWeave、Nebius、Oracle、Applied Digital、IREN、Core Scientific、TeraWulf、GDS、Kingsoft Cloud、Equinix、Digital Realty、Microsoft、Amazon、Google、Meta，以及 HYG、LQD、CDX IG/HY 这类信用 proxy。

## 2. Nuclear / Firm Power / Grid Radar

请建立一个 AI data center firm power 研究框架：

- 为什么 AI data center 需要 24/7 firm power；
- 哪些层级是近端瓶颈，哪些是远端叙事：
  - transformer / switchgear；
  - grid interconnect / transmission；
  - gas turbine；
  - nuclear restart / existing nuclear fleet；
  - SMR / advanced nuclear；
  - uranium / nuclear fuel / HALEU；
  - fuel cell / onsite power；
  - battery storage；
- 哪些指标最关键：contracted MW/GW、PPA price、data center customer contract、interconnect queue、capacity factor、license extension、SMR licensing milestone、fuel supply contract、turbine backlog、cash runway；
- 哪些公司应纳入观察：
  - CEG、VST、TLN、NRG；
  - GEV、Siemens Energy、Mitsubishi Heavy；
  - LEU、CCJ、Kazatomprom；
  - OKLO、SMR、BWXT、Rolls-Royce SMR；
  - BE、FLNC、PWR；
  - URA、NLR 等 ETF proxy；
- 哪些是 AI Infra 真瓶颈，哪些只是 commodity / policy / long-duration option；
- 如何判断一个核电/firm power 公司能否从 D5 雷达升级到候选。

## 3. Integration

请把这两个模块整合到我们的 AI Infra 研究系统里：

- 是否应该影响 D1-D3 主线优先级；
- 是否应该单独建 credit/nuclear evidence card；
- 每张 evidence card 应该核验哪些不同于普通半导体公司的指标；
- 哪些公司应优先做 MVP；
- 哪些应该只做 watchlist；
- 如何避免把核电、信用、能源叙事误当成 AI Infra 直接受益。

请输出尽量表格化，便于保存为 Markdown 和后续落库。所有事实必须标注证据状态：原文已证明 / 合理推论 / 待原文核验 / 主要反证。
