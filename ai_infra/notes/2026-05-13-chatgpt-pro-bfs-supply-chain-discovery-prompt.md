# ChatGPT Pro Prompt: BFS Supply-Chain Discovery Agent Design

你是 AI Infra 产业链研究助手。目标不是投资建议，不给买卖建议、不做目标价，而是帮我把现有 AI Infra universe 从 seed companies 扩展成 source-backed supply-chain discovery system。

## 项目框架

从 D0 LLM 需求源头出发做 dependency BFS：

- D0: OpenAI / Anthropic / Google DeepMind / Gemini / Meta / xAI 等 LLM demand。
- D1: GPU/TPU/ASIC/cloud/software stack。
- D2: HBM、CoWoS、leading-edge foundry、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- D3: HBM test/equipment/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer/AEC、液冷组件、电力设备关键部件。
- D4-D5: 材料、气体、化学品、真空、洁净、能源、电网、融资、监管，只做 radar，除非能证明反向卡住 D0-D2。

研究重点：美国公司 + 日本/韩国/台湾/欧洲/以色列公司。不要重复泛泛 AI 概念股，要沿供应链边找更多可交易或可观察公司。

## 现有 seed universe 摘要

主题计数：{'advanced_packaging_substrate': 14, 'compute_gpu_asic': 30, 'hbm_memory_storage': 33, 'networking_fabric': 11, 'optics_photonics_cpo': 17, 'other': 6, 'power_cooling_grid': 30, 'testing_metrology': 3, 'materials_equipment': 2}

Seed companies:
- US | KLA | KLA | D3-D4 | advanced_packaging_substrate | Inspection/metrology | 核心beta/候选
- US | NVMI | Nova | D3 | advanced_packaging_substrate | Metrology | 候选池
- US | ONTO | Onto Innovation | D3 | advanced_packaging_substrate | Metrology / inspection | 候选池
- US | AMD | AMD | D1 | compute_gpu_asic | GPU/CPU AI compute | 核心/候选池
- US | AMZN | Amazon | D0-D1 | compute_gpu_asic | AWS + Trainium + Anthropic | 核心beta
- US | AVGO | Broadcom | D1-D2 | compute_gpu_asic | Custom ASIC + networking + CPO | 核心池
- US | CORZ | Core Scientific | D1-D4 | compute_gpu_asic | AI hosting / power campuses | 雷达/候选池
- US | CRWV | CoreWeave | D1-D4 | compute_gpu_asic | NeoCloud / GPU-as-a-Service | 候选池
- US | FORM | FormFactor | D3 | compute_gpu_asic | Probe card | 候选池
- US | GOOGL | Alphabet / Google | D0-D1 | compute_gpu_asic | Gemini + TPU + GCP | 核心beta
- US | IREN | IREN | D1-D4 | compute_gpu_asic | AI cloud + powered land from crypto mining | 候选/雷达池
- US | META | Meta Platforms | D0-D1 | compute_gpu_asic | Open-source frontier + AI products + internal infra | 核心beta/需求验证
- US | MRVL | Marvell | D1-D3 | compute_gpu_asic | Custom silicon + optical DSP + networking | 候选池
- US | MSFT | Microsoft | D0-D1 | compute_gpu_asic | Azure + OpenAI demand aggregator | 核心beta
- US | MU | Micron | D2 | compute_gpu_asic | HBM + DRAM + eSSD | 核心/候选池
- US | NBIS | Nebius | D1-D4 | compute_gpu_asic | NeoCloud / AI cloud | 候选池
- US | NVDA | NVIDIA | D1 | compute_gpu_asic | GPU/CUDA + networking + rack-scale systems | 核心池
- US | ORCL | Oracle | D1 | compute_gpu_asic | AI cloud / Stargate cloud capacity | 候选池
- US | SNPS | Synopsys | D3 | compute_gpu_asic | EDA/IP | 核心beta/候选
- US | ALAB | Astera Labs | D3 | hbm_memory_storage | PCIe/CXL retimer + connectivity | 候选池
- US | CAMT | Camtek | D3 | hbm_memory_storage | Advanced packaging inspection | 核心候选
- US | NTAP | NetApp | D3-D4 | hbm_memory_storage | Enterprise/data center storage | 雷达/候选池
- US | PSTG | Pure Storage | D3 | hbm_memory_storage | AI storage systems | 候选池
- US | RMBS | Rambus | D3 | hbm_memory_storage | Memory interface IP | 候选池
- US | STX | Seagate | D3-D4 | hbm_memory_storage | Nearline HDD/object storage | 雷达/候选池
- US | TER | Teradyne | D3 | hbm_memory_storage | Semiconductor ATE | 候选池
- US | WDC | Western Digital / SanDisk | D3-D4 | hbm_memory_storage | NAND/eSSD/storage | 候选/雷达池
- US | AAOI | Applied Optoelectronics | D2-D3 | networking_fabric | Datacom optical modules | 候选/雷达池
- US | ANET | Arista Networks | D2 | networking_fabric | AI Ethernet networking | 核心/候选池
- US | CRDO | Credo | D3 | networking_fabric | AEC/SerDes/connectivity | 候选池
- US | CSCO | Cisco | D2-D4 | networking_fabric | Networking + optics + security | 候选/雷达池
- US | COHR | Coherent | D2-D3 | optics_photonics_cpo | 800G/1.6T optics + lasers | 候选/核心候选
- US | FN | Fabrinet | D2-D3 | optics_photonics_cpo | Optical manufacturing | 候选池
- US | LITE | Lumentum | D2-D3 | optics_photonics_cpo | Lasers/datacom optics | 候选池
- US | MTSI | MACOM | D3 | optics_photonics_cpo | RF/photonic components | 候选池
- US | SMTC | Semtech | D3 | optics_photonics_cpo | Signal integrity / optical DSP adjacent | 雷达/候选池
- US | CDNS | Cadence | D3 | other | EDA/simulation | 核心beta/候选
- US | APLD | Applied Digital | D1-D4 | power_cooling_grid | AI data center developer | 候选/雷达池
- US | ETN | Eaton | D2-D4 | power_cooling_grid | Electrical equipment | 核心/候选池
- US | MOD | Modine | D2-D3 | power_cooling_grid | Thermal management | 候选池
- US | POWL | Powell Industries | D3-D4 | power_cooling_grid | Electrical switchgear | 候选池
- US | SPXC | SPX Technologies | D3-D4 | power_cooling_grid | Cooling/HVAC/electrical niches | 雷达/候选池
- US | VRT | Vertiv | D2 | power_cooling_grid | AI data center power/thermal | 核心池
- satellite_non_us | 009150.KS | Samsung Electro-Mechanics | D3 | advanced_packaging_substrate | Substrate / components | 候选池
- satellite_non_us | 2802.T | Ajinomoto | D3 | advanced_packaging_substrate | ABF material | 候选池
- satellite_non_us | 3189.TW | Kinsus | D3 | advanced_packaging_substrate | Substrate | 雷达/候选池
- satellite_non_us | 3711.TW / ASX | ASE Technology | D2-D3 | advanced_packaging_substrate | OSAT / advanced packaging | 候选池
- satellite_non_us | 8046.TW | Nan Ya PCB | D3 | advanced_packaging_substrate | Substrate / PCB | 候选池
- satellite_non_us | SUSS.DE | SÜSS MicroTec | D3 | advanced_packaging_substrate | Advanced packaging tools | 核心候选
- satellite_non_us | 000660.KS | SK hynix | D2 | compute_gpu_asic | HBM/DRAM | 核心池
- satellite_non_us | 005930.KS | Samsung Electronics | D2-D4 | compute_gpu_asic | HBM/DRAM/foundry/packaging | 候选池
- satellite_non_us | 2330.TW / TSM | TSMC | D2 | compute_gpu_asic | Leading-edge foundry + CoWoS | 核心池
- satellite_non_us | 2382.TW | Quanta | D2 | compute_gpu_asic | AI server ODM | 候选池
- satellite_non_us | 3037.TW | Unimicron | D3 | compute_gpu_asic | ABF substrate / PCB | 核心候选
- satellite_non_us | 3443.TW | GUC | D3 | compute_gpu_asic | ASIC design service | 候选池
- satellite_non_us | 3661.TW | Alchip | D3 | compute_gpu_asic | ASIC design service | 核心候选
- satellite_non_us | 4062.T | Ibiden | D3 | compute_gpu_asic | ABF substrate | 核心候选
- satellite_non_us | 6857.T | Advantest | D3 | compute_gpu_asic | ATE / memory & SoC test | 核心候选
- satellite_non_us | 042700.KQ | Hanmi Semiconductor | D3 | hbm_memory_storage | HBM bonding equipment | 核心候选
- satellite_non_us | 058470.KQ | Leeno | D3 | hbm_memory_storage | Test sockets/probes | 候选池
- satellite_non_us | 095340.KQ | ISC | D3 | hbm_memory_storage | Test sockets | 候选池
- satellite_non_us | 131290.KQ | TSE | D3 | hbm_memory_storage | Probe/test interface | 雷达/候选池
- satellite_non_us | 2360.TW | Chroma | D3 | hbm_memory_storage | Power/semiconductor test | 候选池
- satellite_non_us | 4004.T | Resonac | D3-D4 | hbm_memory_storage | Packaging/semiconductor materials | 候选池
- satellite_non_us | 6146.T | DISCO | D3 | hbm_memory_storage | Dicing/grinding/thinning | 核心候选
- satellite_non_us | 6223.TW | MPI | D3 | hbm_memory_storage | Probe card/test interface | 候选池
- satellite_non_us | 6315.T | TOWA | D3 | hbm_memory_storage | Molding/advanced packaging equipment | 核心候选
- satellite_non_us | 6515.TW | WinWay | D3 | hbm_memory_storage | Probe card / test socket | 候选池
- satellite_non_us | 8299.TWO | Phison | D3 | hbm_memory_storage | SSD controller | 候选池
- satellite_non_us | BESI.AS | BE Semiconductor Industries | D3 | hbm_memory_storage | Hybrid bonding / advanced packaging equipment | 核心候选
- satellite_non_us | 2301.TW | Lite-On | D3 | optics_photonics_cpo | Power supply / optoelectronics | 候选/雷达池
- satellite_non_us | 6669.TW | Wiwynn | D2 | other | AI server ODM | 候选池
- satellite_non_us | 2308.TW | Delta Electronics | D2-D3 | power_cooling_grid | Power supply + thermal | 核心候选
- satellite_non_us | ABBN.SW / ABB | ABB | D2-D4 | power_cooling_grid | Grid/electrification/automation | 核心/候选池
- satellite_non_us | IFX.DE | Infineon | D3-D4 | power_cooling_grid | Power semiconductors | 候选/雷达池
- satellite_non_us | MTRS.ST | Munters | D3 | power_cooling_grid | Thermal management | 候选/雷达池
- satellite_non_us | SU.PA | Schneider Electric | D2-D4 | power_cooling_grid | Data center electrical equipment | 核心池
- US | DOCN | DigitalOcean | D1-D3 | compute_gpu_asic | SMB cloud / AI cloud services | 雷达池
- US | COHU | Cohu | D3 | hbm_memory_storage | Test handlers / sockets | 雷达池
- US | TSEM | Tower Semiconductor | D3-D4 | optics_photonics_cpo | Specialty foundry / SiPh | 雷达池
- US | WULF | TeraWulf | D1-D4 | power_cooling_grid | Powered data center / HPC transition | 雷达池
- satellite_non_us | IQE.L | IQE | D3-D4 | optics_photonics_cpo | Compound semiconductor epi | 雷达池
- satellite_non_us | SIVE.ST | Sivers Semiconductors | D3-D4 | optics_photonics_cpo | Photonics/mmWave | 雷达池
- satellite_non_us | ALFA.ST | Alfa Laval | D3-D4 | power_cooling_grid | Heat exchangers / thermal | 雷达池

## 任务 A：从 seed 出发扩展供应链

请按以下主题分别扩展更多美国、日本、韩国、台湾、欧洲、以色列候选公司：

1. compute_gpu_asic
2. hbm_memory_storage
3. advanced_packaging_substrate
4. testing_metrology
5. networking_fabric
6. optics_photonics_cpo
7. power_cooling_grid
8. neocloud_data_center
9. materials_equipment

对每个主题输出：

- 已有 seed 中最重要的 10 个起点；
- 还缺哪些供应链子环节；
- 应该如何从 annual report / 10-K / 20-F / earnings call / product pages / customer cross-disclosures 中找新公司；
- 新候选公司清单：ticker、exchange/country、BFS depth、dependency edge、为什么相关、需要核验的原文、主要反证；
- 明确标记哪些只是 radar，哪些可以进入 D1-D3 候选。

## 任务 B：自动化 agent pipeline 设计

请设计一个本地脚本/agent 系统，用于“读财报挖供应链公司”。要求：

1. 输入：seed universe JSONL，每条有 ticker/company/market_country/bfs_depth/module/dependency_path/current_pool。
2. 数据源优先级：company annual report、10-K/20-F/10-Q、earnings release/call transcript、investor presentation、company product pages、customer/supplier cross-disclosures、SEC/交易所公告。
3. Pipeline 阶段：
   - security master / ticker normalization；
   - filing/source discovery；
   - PDF/HTML/text extraction；
   - entity extraction：customers、suppliers、competitors、equipment、materials、capacity、backlog、capex、RPO、lead time；
   - dependency edge classifier；
   - evidence card generator；
   - dedupe/entity linking；
   - candidate scoring；
   - refutation dashboard update。
4. 输出：SQLite schema、JSONL schema、CSV queue、Markdown evidence card。
5. 每个 agent 的 prompt：filing-reader、entity-linker、dependency-classifier、evidence-card-writer、refutation-reviewer。
6. 给出伪代码或 Python 标准库 MVP 设计，不接 IBKR、不自动交易。

## 任务 C：搜索/核验 query 模板

请给出用于每个主题的可执行搜索 query 模板，例如：

- `site:company.com annual report AI data center HBM supplier`
- `10-K customer concentration AI data center backlog`
- `investor presentation CoWoS substrate capacity`
- `OFC 1.6T CPO silicon photonics customer qualification`

## 证据规则

所有输出必须分为：

- 原文已证明；
- 合理推论；
- 待原文核验；
- 主要反证。

不要把媒体、模型记忆、券商摘要当成事实。不要输出投资建议、买入/卖出、目标价或实际仓位。
