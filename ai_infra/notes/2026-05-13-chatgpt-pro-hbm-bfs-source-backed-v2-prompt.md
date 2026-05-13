# ChatGPT Pro Prompt: HBM BFS Source-Backed Discovery v2

你是 AI Infra 产业链研究助手。目标不是投资建议，不给买卖建议、不做目标价。

这次只研究一个主题：**HBM 当前瓶颈主线与供应链扩展**。

不要回答模型身份问题。不要泛泛写“框架草稿”。请严格按 source-backed research 输出。

## 项目框架

从 D0 LLM demand 出发做 dependency BFS：

- D0: OpenAI / Anthropic / Google DeepMind / Gemini / Meta / xAI 等 LLM demand。
- D1: GPU/TPU/ASIC/cloud/software stack。
- D2: HBM、CoWoS、leading-edge foundry、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- D3: HBM test/equipment/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer/AEC、液冷组件、电力设备关键部件。
- D4-D5: 材料、气体、化学品、真空、洁净、能源、电网、融资、监管，只做 radar，除非能证明反向卡住 D0-D2。

## Seed companies

请从这些 seed 出发扩展供应链，不要局限于这些公司：

- HBM vendors: SK hynix, Samsung Electronics, Micron
- Foundry / packaging anchor: TSMC, ASE, Amkor, Samsung advanced packaging
- HBM / packaging equipment: Hanmi Semiconductor, ASMPT, BESI, SUSS MicroTec, TOWA, DISCO
- Testing / probe / inspection: Advantest, Teradyne, FormFactor, Chroma, MPI, WinWay, Leeno, ISC, TSE, Camtek, Nova, Onto Innovation, KLA
- Substrate / ABF / materials: Ibiden, Shinko, Unimicron, Nan Ya PCB, Kinsus, Ajinomoto, Resonac, Namics, Sumitomo Bakelite
- US adjacent: ALAB, RMBS, MRVL, AVGO, MU, COHR only if directly relevant to HBM / memory interface / packaging / test

## Required output

### 1. Source checklist first

先列出应该核验的公司原文来源，不要先下结论。

按公司列：

- annual report / 10-K / 20-F / quarterly report
- earnings release / earnings call
- investor presentation
- official product page / technology page
- customer or supplier cross-disclosure

每条标注应该找什么关键词，例如 HBM3E, HBM4, 12-high, 16-high, TC bonding, MR-MUF, hybrid bonding, known-good-die, probe card, memory test, CoWoS, interposer, ABF, underfill, molding, capacity, backlog, qualification, gross margin。

### 2. HBM bottleneck chain

把当前 HBM 主线拆成 BFS 链条：

AI accelerator demand
-> HBM3E / HBM4 capacity
-> wafer thinning / dicing / grinding
-> TC bonding / MR-MUF / hybrid bonding
-> known-good-die / HBM test time
-> probe card / socket / handler
-> CoWoS / interposer / ABF substrate
-> advanced packaging inspection / metrology
-> underfill / molding / thermal / materials

每一环说明：

- BFS depth
- 为什么会被 HBM 放大
- 代表公司
- 需要核验的原文指标
- 主要反证

### 3. Candidate expansion table

输出一个表格，至少包含 40 个候选 / radar 公司，重点覆盖美国、日本、韩国、台湾、欧洲、以色列。

字段：

- company
- ticker / exchange
- country
- BFS depth
- HBM supply-chain node
- dependency edge
- evidence state: 原文已证明 / 合理推论 / 待原文核验 / 主要反证
- primary sources to verify
- upgrade condition
- downgrade / refutation condition

要求：

- D2-D3 重点放前面。
- D4-D5 只作为 radar，除非能证明反向卡住 D0-D2。
- 不要把泛半导体设备、泛材料、泛存储周期直接当 HBM 受益。

### 4. How to discover more companies automatically

请给出“读财报挖 HBM 供应链公司”的 agent pipeline 设计：

- filing-reader agent
- product-page reader agent
- entity extraction agent
- dependency classifier agent
- evidence card writer agent
- refutation reviewer agent

每个 agent 给出输入、输出字段、prompt 模板、失败模式。

### 5. Search queries

给出可执行搜索 query 模板，至少 30 条，例如：

- `site:company.com HBM4 TC bonding annual report`
- `site:company.com HBM probe card investor presentation`
- `site:company.com CoWoS ABF substrate capacity`
- `site:sec.gov 10-K HBM memory test customer concentration`

## Evidence rules

所有内容必须标注：

- 原文已证明
- 合理推论
- 待原文核验
- 主要反证

如果没有看到原文，请写“待原文核验”，不要编造成已证明事实。
