# AI Infra Universe Dashboard v1

**状态**: research-priority dashboard; all records default to `pending_original_source_verification`.

**不是投资建议**: 本报告不生成买卖建议、目标价或实际仓位建议。评分只用于安排研究优先级。

## 文件

- 输入 JSONL: `data/global_universe_v2.jsonl`
- SQLite: `data/ai_infra_universe.sqlite`
- 生成 CSV: `core_candidates.csv`, `d2_d3_candidates.csv`, `china_asset_pool.csv`, `us_asset_pool.csv`, `satellite_pool.csv`, `radar_and_excluded.csv`

## 总览

- 总记录数: **146**
- 数据质量状态: **pending_original_source_verification**
- 研究重点: **D1-D3**；D4-D5 默认雷达，除非能证明反向卡住 D0-D2。

### 资产池分布

| 分类 | 数量 |
| --- | --- |
| 卫星资产池 | 50 |
| 美国资产池 | 48 |
| 中国资产池 | 48 |

### BFS 深度分布

| 分类 | 数量 |
| --- | --- |
| D3 | 51 |
| D4 | 24 |
| D2-D3 | 15 |
| D3-D4 | 15 |
| D2 | 10 |
| D1-D4 | 8 |
| D2-D4 | 7 |
| D4-D5 | 5 |
| D0-D1 | 4 |
| D1 | 3 |
| D1-D3 | 2 |
| D1-D2 | 1 |
| D3-D5 | 1 |

### 分池分布

| 分类 | 数量 |
| --- | --- |
| 候选池 | 41 |
| 雷达/候选池 | 21 |
| 雷达池 | 20 |
| 候选/雷达池 | 19 |
| 核心候选 | 13 |
| 排除池 | 8 |
| 核心池 | 6 |
| 核心/候选池 | 5 |
| 核心beta | 3 |
| 候选/核心候选 | 3 |
| 核心beta/候选 | 3 |
| 核心beta/雷达 | 2 |
| 核心beta/需求验证 | 1 |
| 排除/雷达池 | 1 |

### 评分桶分布

| 分类 | 数量 |
| --- | --- |
| core_review | 84 |
| radar | 27 |
| high_priority | 26 |
| exclude | 9 |

## 评分规则

- `bfs_score`: D2-D3 最高；D4-D5 降为雷达；超过 D5 默认排除。
- `pool_score`: 核心池/核心候选加权；雷达降权；排除池强惩罚。
- `evidence_score`: 原文已证明最高，合理推论次之，待原文核验保留但不视为已证明。
- `edge_score`: dependency path、dependency edge、海外瓶颈、上下游关系越清楚，研究优先级越高。
- `risk_penalty`: 反证、排除条件、客户集中、融资/债务、价格战、供给过剩、流动性等降分。

## 中国资产池 Top Candidates

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 002463.SZ | 沪电股份 | A股主板 | D3 | AI server PCB/high-speed board | 核心候选 | 100 | core_review |
| 002837.SZ | 英维克 | A股主板 | D2-D3 | Data center cooling/liquid cooling | 候选/核心候选 | 100 | core_review |
| 601138.SH | 工业富联 | A股主板 | D2 | AI server/rack manufacturing | 候选/核心候选 | 100 | core_review |
| 000988.SZ | 华工科技 | A股主板 | D2-D3 | Optics/laser/equipment | 候选池 | 99 | core_review |
| 0522.HK | ASMPT | H股 | D3 | Packaging equipment / TCB | 核心候选 | 98 | core_review |
| 002281.SZ | 光迅科技 | A股主板 | D2-D3 | Optical modules/components | 候选池 | 96 | core_review |
| 000063.SZ / 0763.HK | 中兴通讯 | A股主板/H股 | D2-D3 | Networking/telecom/DC equipment | 候选/雷达池 | 95 | core_review |
| 000938.SZ | 紫光股份 | A股主板 | D2-D3 | Network/server/cloud infra | 候选/雷达池 | 95 | core_review |
| 002916.SZ | 深南电路 | A股主板 | D3 | PCB / substrate / packaging support | 候选池 | 93 | core_review |
| 600183.SH | 生益科技 | A股主板 | D3 | CCL / high-speed materials | 候选池 | 93 | core_review |
| 600584.SH | 长电科技 | A股主板 | D3 | OSAT / advanced packaging | 候选池 | 93 | core_review |
| 000977.SZ | 浪潮信息 | A股主板 | D2 | AI server OEM | 候选池 | 92 | core_review |
| 002185.SZ | 华天科技 | A股主板 | D3 | OSAT | 雷达/候选池 | 92 | core_review |
| 002436.SZ | 兴森科技 | A股主板 | D3 | IC substrate/PCB | 雷达/候选池 | 92 | core_review |
| 0992.HK | 联想集团 | H股 | D2 | AI server/OEM | 候选池 | 92 | core_review |

## 美国资产池 Top Candidates

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| COHR | Coherent | US | D2-D3 | 800G/1.6T optics + lasers | 候选/核心候选 | 100 | core_review |
| NVDA | NVIDIA | US | D1 | GPU/CUDA + networking + rack-scale systems | 核心池 | 100 | core_review |
| VRT | Vertiv | US | D2 | AI data center power/thermal | 核心池 | 100 | core_review |
| AVGO | Broadcom | US | D1-D2 | Custom ASIC + networking + CPO | 核心池 | 99 | core_review |
| FN | Fabrinet | US | D2-D3 | Optical manufacturing | 候选池 | 96 | core_review |
| MOD | Modine | US | D2-D3 | Thermal management | 候选池 | 96 | core_review |
| RMBS | Rambus | US | D3 | Memory interface IP | 候选池 | 96 | core_review |
| SNPS | Synopsys | US | D3 | EDA/IP | 核心beta/候选 | 95 | core_review |
| ALAB | Astera Labs | US | D3 | PCIe/CXL retimer + connectivity | 候选池 | 93 | core_review |
| CRDO | Credo | US | D3 | AEC/SerDes/connectivity | 候选池 | 93 | core_review |
| FORM | FormFactor | US | D3 | Probe card | 候选池 | 93 | core_review |
| LITE | Lumentum | US | D2-D3 | Lasers/datacom optics | 候选池 | 93 | core_review |
| MRVL | Marvell | US | D1-D3 | Custom silicon + optical DSP + networking | 候选池 | 93 | core_review |
| MTSI | MACOM | US | D3 | RF/photonic components | 候选池 | 93 | core_review |
| ONTO | Onto Innovation | US | D3 | Metrology / inspection | 候选池 | 93 | core_review |

## 卫星资产池 Top Candidates

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 000660.KS | SK hynix | 韩国 | D2 | HBM/DRAM | 核心池 | 100 | core_review |
| 2308.TW | Delta Electronics | 台湾 | D2-D3 | Power supply + thermal | 核心候选 | 100 | core_review |
| 2330.TW / TSM | TSMC | 台湾 | D2 | Leading-edge foundry + CoWoS | 核心池 | 100 | core_review |
| 3037.TW | Unimicron | 台湾 | D3 | ABF substrate / PCB | 核心候选 | 100 | core_review |
| 3661.TW | Alchip | 台湾 | D3 | ASIC design service | 核心候选 | 100 | core_review |
| 4062.T | Ibiden | 日本 | D3 | ABF substrate | 核心候选 | 100 | core_review |
| 6146.T | DISCO | 日本 | D3 | Dicing/grinding/thinning | 核心候选 | 100 | core_review |
| 6315.T | TOWA | 日本 | D3 | Molding/advanced packaging equipment | 核心候选 | 100 | core_review |
| 6857.T | Advantest | 日本 | D3 | ATE / memory & SoC test | 核心候选 | 100 | core_review |
| BESI.AS | BE Semiconductor Industries | 欧洲 | D3 | Hybrid bonding / advanced packaging equipment | 核心候选 | 100 | core_review |
| 3711.TW / ASX | ASE Technology | 台湾 | D2-D3 | OSAT / advanced packaging | 候选池 | 99 | core_review |
| 042700.KQ | Hanmi Semiconductor | 韩国 | D3 | HBM bonding equipment | 核心候选 | 98 | core_review |
| CAMT | Camtek | 以色列/US | D3 | Advanced packaging inspection | 核心候选 | 98 | core_review |
| SUSS.DE | SÜSS MicroTec | 欧洲 | D3 | Advanced packaging tools | 核心候选 | 98 | core_review |
| 2802.T | Ajinomoto | 日本 | D3 | ABF material | 候选池 | 96 | core_review |

## D2-D3 高弹性候选清单

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 002463.SZ | 沪电股份 | A股主板 | D3 | AI server PCB/high-speed board | 核心候选 | 100 | core_review |
| 002837.SZ | 英维克 | A股主板 | D2-D3 | Data center cooling/liquid cooling | 候选/核心候选 | 100 | core_review |
| 601138.SH | 工业富联 | A股主板 | D2 | AI server/rack manufacturing | 候选/核心候选 | 100 | core_review |
| 000660.KS | SK hynix | 韩国 | D2 | HBM/DRAM | 核心池 | 100 | core_review |
| 2308.TW | Delta Electronics | 台湾 | D2-D3 | Power supply + thermal | 核心候选 | 100 | core_review |
| 2330.TW / TSM | TSMC | 台湾 | D2 | Leading-edge foundry + CoWoS | 核心池 | 100 | core_review |
| 3037.TW | Unimicron | 台湾 | D3 | ABF substrate / PCB | 核心候选 | 100 | core_review |
| 3661.TW | Alchip | 台湾 | D3 | ASIC design service | 核心候选 | 100 | core_review |
| 4062.T | Ibiden | 日本 | D3 | ABF substrate | 核心候选 | 100 | core_review |
| 6146.T | DISCO | 日本 | D3 | Dicing/grinding/thinning | 核心候选 | 100 | core_review |
| 6315.T | TOWA | 日本 | D3 | Molding/advanced packaging equipment | 核心候选 | 100 | core_review |
| 6857.T | Advantest | 日本 | D3 | ATE / memory & SoC test | 核心候选 | 100 | core_review |
| BESI.AS | BE Semiconductor Industries | 欧洲 | D3 | Hybrid bonding / advanced packaging equipment | 核心候选 | 100 | core_review |
| COHR | Coherent | US | D2-D3 | 800G/1.6T optics + lasers | 候选/核心候选 | 100 | core_review |
| VRT | Vertiv | US | D2 | AI data center power/thermal | 核心池 | 100 | core_review |
| 000988.SZ | 华工科技 | A股主板 | D2-D3 | Optics/laser/equipment | 候选池 | 99 | core_review |
| 3711.TW / ASX | ASE Technology | 台湾 | D2-D3 | OSAT / advanced packaging | 候选池 | 99 | core_review |
| AVGO | Broadcom | US | D1-D2 | Custom ASIC + networking + CPO | 核心池 | 99 | core_review |
| 0522.HK | ASMPT | H股 | D3 | Packaging equipment / TCB | 核心候选 | 98 | core_review |
| 042700.KQ | Hanmi Semiconductor | 韩国 | D3 | HBM bonding equipment | 核心候选 | 98 | core_review |
| CAMT | Camtek | 以色列/US | D3 | Advanced packaging inspection | 核心候选 | 98 | core_review |
| SUSS.DE | SÜSS MicroTec | 欧洲 | D3 | Advanced packaging tools | 核心候选 | 98 | core_review |
| 002281.SZ | 光迅科技 | A股主板 | D2-D3 | Optical modules/components | 候选池 | 96 | core_review |
| 2802.T | Ajinomoto | 日本 | D3 | ABF material | 候选池 | 96 | core_review |
| FN | Fabrinet | US | D2-D3 | Optical manufacturing | 候选池 | 96 | core_review |
| MOD | Modine | US | D2-D3 | Thermal management | 候选池 | 96 | core_review |
| RMBS | Rambus | US | D3 | Memory interface IP | 候选池 | 96 | core_review |
| 000063.SZ / 0763.HK | 中兴通讯 | A股主板/H股 | D2-D3 | Networking/telecom/DC equipment | 候选/雷达池 | 95 | core_review |
| 000938.SZ | 紫光股份 | A股主板 | D2-D3 | Network/server/cloud infra | 候选/雷达池 | 95 | core_review |
| SNPS | Synopsys | US | D3 | EDA/IP | 核心beta/候选 | 95 | core_review |
| 002916.SZ | 深南电路 | A股主板 | D3 | PCB / substrate / packaging support | 候选池 | 93 | core_review |
| 600183.SH | 生益科技 | A股主板 | D3 | CCL / high-speed materials | 候选池 | 93 | core_review |
| 600584.SH | 长电科技 | A股主板 | D3 | OSAT / advanced packaging | 候选池 | 93 | core_review |
| 009150.KS | Samsung Electro-Mechanics | 韩国 | D3 | Substrate / components | 候选池 | 93 | core_review |
| 058470.KQ | Leeno | 韩国 | D3 | Test sockets/probes | 候选池 | 93 | core_review |
| 2360.TW | Chroma | 台湾 | D3 | Power/semiconductor test | 候选池 | 93 | core_review |
| 3443.TW | GUC | 台湾 | D3 | ASIC design service | 候选池 | 93 | core_review |
| 6223.TW | MPI | 台湾 | D3 | Probe card/test interface | 候选池 | 93 | core_review |
| 8046.TW | Nan Ya PCB | 台湾 | D3 | Substrate / PCB | 候选池 | 93 | core_review |
| 8299.TWO | Phison | 台湾 | D3 | SSD controller | 候选池 | 93 | core_review |

## D4-D5 雷达清单

这些标的默认不进入核心候选，除非后续原文证据证明它们能反向卡住 D0-D2。

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SU.PA | Schneider Electric | 欧洲 | D2-D4 | Data center electrical equipment | 核心池 | 91 | core_review |
| CRWV | CoreWeave | US | D1-D4 | NeoCloud / GPU-as-a-Service | 候选池 | 86 | core_review |
| 005930.KS | Samsung Electronics | 韩国 | D2-D4 | HBM/DRAM/foundry/packaging | 候选池 | 83 | core_review |
| KLA | KLA | US | D3-D4 | Inspection/metrology | 核心beta/候选 | 83 | core_review |
| ABBN.SW / ABB | ABB | 欧洲 | D2-D4 | Grid/electrification/automation | 核心/候选池 | 80 | core_review |
| ETN | Eaton | US | D2-D4 | Electrical equipment | 核心/候选池 | 80 | core_review |
| 002335.SZ | 科华数据 | A股主板 | D2-D4 | UPS/IDC/power | 候选/雷达池 | 79 | high_priority |
| 002518.SZ | 科士达 | A股主板 | D2-D4 | UPS/power electronics | 候选/雷达池 | 79 | high_priority |
| 4004.T | Resonac | 日本 | D3-D4 | Packaging/semiconductor materials | 候选池 | 79 | high_priority |
| CSCO | Cisco | US | D2-D4 | Networking + optics + security | 候选/雷达池 | 79 | high_priority |
| IFX.DE | Infineon | 欧洲 | D3-D4 | Power semiconductors | 候选/雷达池 | 78 | high_priority |
| NTAP | NetApp | US | D3-D4 | Enterprise/data center storage | 雷达/候选池 | 78 | high_priority |
| SPXC | SPX Technologies | US | D3-D4 | Cooling/HVAC/electrical niches | 雷达/候选池 | 78 | high_priority |
| NBIS | Nebius | US | D1-D4 | NeoCloud / AI cloud | 候选池 | 76 | high_priority |
| POWL | Powell Industries | US | D3-D4 | Electrical switchgear | 候选池 | 76 | high_priority |
| STX | Seagate | US | D3-D4 | Nearline HDD/object storage | 雷达/候选池 | 75 | high_priority |
| WDC | Western Digital / SanDisk | US | D3-D4 | NAND/eSSD/storage | 候选/雷达池 | 75 | high_priority |
| 000021.SZ | 深科技 | A股主板 | D3-D4 | EMS/storage packaging/test | 雷达池 | 73 | high_priority |
| TSEM | Tower Semiconductor | 以色列/US | D3-D4 | Specialty foundry / SiPh | 雷达池 | 73 | high_priority |
| IREN | IREN | US | D1-D4 | AI cloud + powered land from crypto mining | 候选/雷达池 | 72 | high_priority |
| 3896.HK / KC | 金山云 | H股/ADR | D1-D4 | Cloud / AI cloud | 雷达池 | 70 | high_priority |
| 603912.SH | 佳力图 | A股主板 | D3-D4 | Data center cooling | 雷达池 | 70 | high_priority |
| ALFA.ST | Alfa Laval | 欧洲 | D3-D4 | Heat exchangers / thermal | 雷达池 | 70 | high_priority |
| SIVE.ST | Sivers Semiconductors | 欧洲 | D3-D4 | Photonics/mmWave | 雷达池 | 70 | high_priority |
| 9698.HK / GDS | 万国数据 GDS | H股/ADR | D1-D4 | IDC / AI data center | 候选/雷达池 | 68 | high_priority |
| APLD | Applied Digital | US | D1-D4 | AI data center developer | 候选/雷达池 | 68 | high_priority |
| CORZ | Core Scientific | US | D1-D4 | AI hosting / power campuses | 雷达/候选池 | 68 | high_priority |
| IQE.L | IQE | 欧洲 | D3-D4 | Compound semiconductor epi | 雷达池 | 67 | high_priority |
| 8035.T | Tokyo Electron | 日本 | D4 | WFE | 核心beta/雷达 | 66 | radar |
| WULF | TeraWulf | US | D1-D4 | Powered data center / HPC transition | 雷达池 | 66 | high_priority |
| ASML.AS | ASML | 欧洲 | D4 | EUV lithography | 核心beta/雷达 | 64 | radar |
| 002028.SZ | 思源电气 | A股主板 | D4 | Switchgear/transformer/electrical equipment | 候选池 | 62 | radar |
| 002902.SZ | 铭普光磁 | A股主板 | D3-D5 | Optical/magnetic components | 雷达池 | 62 | radar |
| 0728.HK / 601728.SH | 中国电信 | H股/A股 | D4 | IDC/cloud/network | 雷达/候选池 | 60 | radar |
| 0941.HK / 600941.SH | 中国移动 | H股/A股 | D4 | IDC/network/AI infra buyer | 雷达/候选池 | 60 | radar |

## 排除池和待重分类清单

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 300249.SZ | 依米康 | A股创业板 | D3-D4 | Cooling/IDC | 排除池 | 0 | exclude |
| 300308.SZ | 中际旭创 | A股创业板 | D2-D3 | Optical modules | 排除池 | 0 | exclude |
| 300394.SZ | 天孚通信 | A股创业板 | D3 | Optical components | 排除池 | 0 | exclude |
| 300476.SZ | 胜宏科技 | A股创业板 | D3 | PCB | 排除池 | 0 | exclude |
| 300502.SZ | 新易盛 | A股创业板 | D2-D3 | Optical modules | 排除池 | 0 | exclude |
| 300666.SZ | 江丰电子 | A股创业板 | D4 | Semiconductor materials | 排除池 | 0 | exclude |
| 603986.SH | 兆易创新 | A股主板 | D4-D5 | Memory/MCU | 排除/雷达池 | 0 | exclude |
| 688008.SH | 澜起科技 | A股科创板 | D3 | Memory interface/CXL | 排除池 | 0 | exclude |
| 688012.SH | 中微公司 | A股科创板 | D4 | Semiconductor equipment | 排除池 | 0 | exclude |

## 下一批原文核验优先级

优先从高分且仍为 `待原文核验` 的记录开始，先核验订单、收入、backlog、CapEx、客户、产能、毛利率和技术路线。

| Ticker | Company | Market | BFS | Module | Pool | Score | Bucket |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 002463.SZ | 沪电股份 | A股主板 | D3 | AI server PCB/high-speed board | 核心候选 | 100 | core_review |
| 002837.SZ | 英维克 | A股主板 | D2-D3 | Data center cooling/liquid cooling | 候选/核心候选 | 100 | core_review |
| 601138.SH | 工业富联 | A股主板 | D2 | AI server/rack manufacturing | 候选/核心候选 | 100 | core_review |
| 000660.KS | SK hynix | 韩国 | D2 | HBM/DRAM | 核心池 | 100 | core_review |
| 2308.TW | Delta Electronics | 台湾 | D2-D3 | Power supply + thermal | 核心候选 | 100 | core_review |
| 3037.TW | Unimicron | 台湾 | D3 | ABF substrate / PCB | 核心候选 | 100 | core_review |
| 3661.TW | Alchip | 台湾 | D3 | ASIC design service | 核心候选 | 100 | core_review |
| 4062.T | Ibiden | 日本 | D3 | ABF substrate | 核心候选 | 100 | core_review |
| 6146.T | DISCO | 日本 | D3 | Dicing/grinding/thinning | 核心候选 | 100 | core_review |
| 6315.T | TOWA | 日本 | D3 | Molding/advanced packaging equipment | 核心候选 | 100 | core_review |
| 6857.T | Advantest | 日本 | D3 | ATE / memory & SoC test | 核心候选 | 100 | core_review |
| BESI.AS | BE Semiconductor Industries | 欧洲 | D3 | Hybrid bonding / advanced packaging equipment | 核心候选 | 100 | core_review |
| COHR | Coherent | US | D2-D3 | 800G/1.6T optics + lasers | 候选/核心候选 | 100 | core_review |
| VRT | Vertiv | US | D2 | AI data center power/thermal | 核心池 | 100 | core_review |
| 000988.SZ | 华工科技 | A股主板 | D2-D3 | Optics/laser/equipment | 候选池 | 99 | core_review |
| 3711.TW / ASX | ASE Technology | 台湾 | D2-D3 | OSAT / advanced packaging | 候选池 | 99 | core_review |
| AVGO | Broadcom | US | D1-D2 | Custom ASIC + networking + CPO | 核心池 | 99 | core_review |
| 0522.HK | ASMPT | H股 | D3 | Packaging equipment / TCB | 核心候选 | 98 | core_review |
| 042700.KQ | Hanmi Semiconductor | 韩国 | D3 | HBM bonding equipment | 核心候选 | 98 | core_review |
| CAMT | Camtek | 以色列/US | D3 | Advanced packaging inspection | 核心候选 | 98 | core_review |
| SUSS.DE | SÜSS MicroTec | 欧洲 | D3 | Advanced packaging tools | 核心候选 | 98 | core_review |
| 002281.SZ | 光迅科技 | A股主板 | D2-D3 | Optical modules/components | 候选池 | 96 | core_review |
| 2802.T | Ajinomoto | 日本 | D3 | ABF material | 候选池 | 96 | core_review |
| FN | Fabrinet | US | D2-D3 | Optical manufacturing | 候选池 | 96 | core_review |
| MOD | Modine | US | D2-D3 | Thermal management | 候选池 | 96 | core_review |
| RMBS | Rambus | US | D3 | Memory interface IP | 候选池 | 96 | core_review |
| 000063.SZ / 0763.HK | 中兴通讯 | A股主板/H股 | D2-D3 | Networking/telecom/DC equipment | 候选/雷达池 | 95 | core_review |
| 000938.SZ | 紫光股份 | A股主板 | D2-D3 | Network/server/cloud infra | 候选/雷达池 | 95 | core_review |
| SNPS | Synopsys | US | D3 | EDA/IP | 核心beta/候选 | 95 | core_review |
| 002916.SZ | 深南电路 | A股主板 | D3 | PCB / substrate / packaging support | 候选池 | 93 | core_review |

## SQLite 复现查询

```sql
SELECT c.ticker, c.company, c.asset_pool, c.bfs_depth, c.module, c.current_pool, s.total_score, s.score_bucket
FROM companies c
JOIN scores s USING (ticker)
WHERE s.score_bucket IN ('core_review', 'high_priority')
ORDER BY s.total_score DESC, c.asset_pool, c.ticker;
```

```sql
SELECT c.ticker, c.company, c.market_country, c.bfs_depth, c.module, c.current_pool, s.total_score
FROM companies c
JOIN scores s USING (ticker)
WHERE c.bfs_depth IN ('D2', 'D2-D3', 'D3', 'D1-D2', 'D1-D3')
  AND c.current_pool NOT LIKE '%排除%'
ORDER BY s.total_score DESC;
```
