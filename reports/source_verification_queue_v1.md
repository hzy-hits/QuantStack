# AI Infra Source Verification Queue v1

状态：原文核验任务队列，所有公司仍为 `pending_original_source_verification`。

边界：这是研究优先级和证据核验清单，不是投资建议、买卖建议、目标价或仓位建议。

## 总览

- 总记录数：146
- P0 第一批：68
- P1 跟进批：13
- P2/P3 雷达：56
- P4 排除/暂缓：9

### Tier 分布

| Tier | 数量 |
| --- | --- |
| P0_first_batch | 68 |
| P2_radar_if_blocks_d2 | 30 |
| P3_deep_radar | 25 |
| P1_d1_d3_followup | 13 |
| P4_excluded_or_hold | 9 |
| P3_low_priority | 1 |

### 资产池分布

| 资产池 | 数量 |
| --- | --- |
| 卫星资产池 | 50 |
| 中国资产池 | 48 |
| 美国资产池 | 48 |

## P0 第一批核验

P0 选择逻辑：BFS 最深不超过 D3、总分至少 90、且不在排除池。先用这些公司建立原文证据卡片和核验节奏。

### Batch 1 建议先做

为了避免一次铺太散，第一轮先从每个资产池各取 8 家 P0 公司做证据卡，覆盖中国、美国和卫星市场。

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 002463.SZ | 沪电股份 | A股主板 | 中国资产池 | D3 | AI server PCB/high-speed board | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 2 | 002837.SZ | 英维克 | A股主板 | 中国资产池 | D2-D3 | Data center cooling/liquid cooling | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 3 | 601138.SH | 工业富联 | A股主板 | 中国资产池 | D2 | AI server/rack manufacturing | 100 | annual report / quarterly results / customer concentration note / inventory and margin disclosure | AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix |
| 17 | 000988.SZ | 华工科技 | A股主板 | 中国资产池 | D2-D3 | Optics/laser/equipment | 99 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 20 | 0522.HK | ASMPT | H股 | 中国资产池 | D3 | Packaging equipment / TCB | 98 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 24 | 002281.SZ | 光迅科技 | A股主板 | 中国资产池 | D2-D3 | Optical modules/components | 96 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 29 | 000063.SZ / 0763.HK | 中兴通讯 | A股主板/H股 | 中国资产池 | D2-D3 | Networking/telecom/DC equipment | 95 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 30 | 000938.SZ | 紫光股份 | A股主板 | 中国资产池 | D2-D3 | Network/server/cloud infra | 95 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 14 | COHR | Coherent | US | 美国资产池 | D2-D3 | 800G/1.6T optics + lasers | 100 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 15 | NVDA | NVIDIA | US | 美国资产池 | D1 | GPU/CUDA + networking + rack-scale systems | 100 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration |
| 16 | VRT | Vertiv | US | 美国资产池 | D2 | AI data center power/thermal | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 19 | AVGO | Broadcom | US | 美国资产池 | D1-D2 | Custom ASIC + networking + CPO | 99 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 26 | FN | Fabrinet | US | 美国资产池 | D2-D3 | Optical manufacturing | 96 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 27 | MOD | Modine | US | 美国资产池 | D2-D3 | Thermal management | 96 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 28 | RMBS | Rambus | US | 美国资产池 | D3 | Memory interface IP | 96 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 31 | SNPS | Synopsys | US | 美国资产池 | D3 | EDA/IP | 95 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 4 | 000660.KS | SK hynix | 韩国 | 卫星资产池 | D2 | HBM/DRAM | 100 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 5 | 2308.TW | Delta Electronics | 台湾 | 卫星资产池 | D2-D3 | Power supply + thermal | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 6 | 2330.TW / TSM | TSMC | 台湾 | 卫星资产池 | D2 | Leading-edge foundry + CoWoS | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 7 | 3037.TW | Unimicron | 台湾 | 卫星资产池 | D3 | ABF substrate / PCB | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 8 | 3661.TW | Alchip | 台湾 | 卫星资产池 | D3 | ASIC design service | 100 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 9 | 4062.T | Ibiden | 日本 | 卫星资产池 | D3 | ABF substrate | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 10 | 6146.T | DISCO | 日本 | 卫星资产池 | D3 | Dicing/grinding/thinning | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 11 | 6315.T | TOWA | 日本 | 卫星资产池 | D3 | Molding/advanced packaging equipment | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |

### 中国资产池 P0

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 002463.SZ | 沪电股份 | A股主板 | 中国资产池 | D3 | AI server PCB/high-speed board | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 2 | 002837.SZ | 英维克 | A股主板 | 中国资产池 | D2-D3 | Data center cooling/liquid cooling | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 3 | 601138.SH | 工业富联 | A股主板 | 中国资产池 | D2 | AI server/rack manufacturing | 100 | annual report / quarterly results / customer concentration note / inventory and margin disclosure | AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix |
| 17 | 000988.SZ | 华工科技 | A股主板 | 中国资产池 | D2-D3 | Optics/laser/equipment | 99 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 20 | 0522.HK | ASMPT | H股 | 中国资产池 | D3 | Packaging equipment / TCB | 98 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 24 | 002281.SZ | 光迅科技 | A股主板 | 中国资产池 | D2-D3 | Optical modules/components | 96 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 29 | 000063.SZ / 0763.HK | 中兴通讯 | A股主板/H股 | 中国资产池 | D2-D3 | Networking/telecom/DC equipment | 95 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 30 | 000938.SZ | 紫光股份 | A股主板 | 中国资产池 | D2-D3 | Network/server/cloud infra | 95 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 32 | 002916.SZ | 深南电路 | A股主板 | 中国资产池 | D3 | PCB / substrate / packaging support | 93 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 33 | 600183.SH | 生益科技 | A股主板 | 中国资产池 | D3 | CCL / high-speed materials | 93 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 34 | 600584.SH | 长电科技 | A股主板 | 中国资产池 | D3 | OSAT / advanced packaging | 93 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 52 | 000977.SZ | 浪潮信息 | A股主板 | 中国资产池 | D2 | AI server OEM | 92 | annual report / quarterly results / customer concentration note / inventory and margin disclosure | AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix |

### 美国资产池 P0

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 14 | COHR | Coherent | US | 美国资产池 | D2-D3 | 800G/1.6T optics + lasers | 100 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 15 | NVDA | NVIDIA | US | 美国资产池 | D1 | GPU/CUDA + networking + rack-scale systems | 100 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration |
| 16 | VRT | Vertiv | US | 美国资产池 | D2 | AI data center power/thermal | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 19 | AVGO | Broadcom | US | 美国资产池 | D1-D2 | Custom ASIC + networking + CPO | 99 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 26 | FN | Fabrinet | US | 美国资产池 | D2-D3 | Optical manufacturing | 96 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 27 | MOD | Modine | US | 美国资产池 | D2-D3 | Thermal management | 96 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 28 | RMBS | Rambus | US | 美国资产池 | D3 | Memory interface IP | 96 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 31 | SNPS | Synopsys | US | 美国资产池 | D3 | EDA/IP | 95 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 43 | ALAB | Astera Labs | US | 美国资产池 | D3 | PCIe/CXL retimer + connectivity | 93 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 44 | CRDO | Credo | US | 美国资产池 | D3 | AEC/SerDes/connectivity | 93 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 45 | FORM | FormFactor | US | 美国资产池 | D3 | Probe card | 93 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 46 | LITE | Lumentum | US | 美国资产池 | D2-D3 | Lasers/datacom optics | 93 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |

### 卫星资产池 P0

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | 000660.KS | SK hynix | 韩国 | 卫星资产池 | D2 | HBM/DRAM | 100 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 5 | 2308.TW | Delta Electronics | 台湾 | 卫星资产池 | D2-D3 | Power supply + thermal | 100 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 6 | 2330.TW / TSM | TSMC | 台湾 | 卫星资产池 | D2 | Leading-edge foundry + CoWoS | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 7 | 3037.TW | Unimicron | 台湾 | 卫星资产池 | D3 | ABF substrate / PCB | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 8 | 3661.TW | Alchip | 台湾 | 卫星资产池 | D3 | ASIC design service | 100 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration |
| 9 | 4062.T | Ibiden | 日本 | 卫星资产池 | D3 | ABF substrate | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 10 | 6146.T | DISCO | 日本 | 卫星资产池 | D3 | Dicing/grinding/thinning | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 11 | 6315.T | TOWA | 日本 | 卫星资产池 | D3 | Molding/advanced packaging equipment | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 12 | 6857.T | Advantest | 日本 | 卫星资产池 | D3 | ATE / memory & SoC test | 100 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 13 | BESI.AS | BE Semiconductor Industries | 欧洲 | 卫星资产池 | D3 | Hybrid bonding / advanced packaging equipment | 100 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 18 | 3711.TW / ASX | ASE Technology | 台湾 | 卫星资产池 | D2-D3 | OSAT / advanced packaging | 99 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 21 | 042700.KQ | Hanmi Semiconductor | 韩国 | 卫星资产池 | D3 | HBM bonding equipment | 98 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |

## 核验工作法

每家公司先做一张证据卡：

- `原文来源`: annual report / quarterly results / earnings call / investor presentation / company product page / exchange filing。
- `已证明事实`: 只写原文可证明的收入、订单、backlog、产能、毛利率、客户、技术路线。
- `合理推论`: 明确写出从哪条原文事实推出来，不能混同为已证明。
- `主要反证`: 客户集中、价格战、供给过剩、融资压力、技术路线变化、毛利率不跟随收入等。
- `结论动作`: 升级 / 保持候选 / 降为雷达 / 排除。

## P1 跟进批 Top 30

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 69 | 603083.SH | 剑桥科技 | A股主板 | 中国资产池 | D2-D3 | Optical modules | 89 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 70 | 131290.KQ | TSE | 韩国 | 卫星资产池 | D3 | Probe/test interface | 89 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 71 | 3189.TW | Kinsus | 台湾 | 卫星资产池 | D3 | Substrate | 89 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 72 | AAOI | Applied Optoelectronics | US | 美国资产池 | D2-D3 | Datacom optical modules | 89 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 73 | MU | Micron | US | 美国资产池 | D2 | HBM + DRAM + eSSD | 89 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 74 | 002384.SZ | 东山精密 | A股主板 | 中国资产池 | D3 | PCB/precision components | 87 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 75 | 002913.SZ | 奥士康 | A股主板 | 中国资产池 | D3 | PCB | 87 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 76 | COHU | Cohu | US | 美国资产池 | D3 | Test handlers / sockets | 84 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 77 | DOCN | DigitalOcean | US | 美国资产池 | D1-D3 | SMB cloud / AI cloud services | 81 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 78 | ORCL | Oracle | US | 美国资产池 | D1 | AI cloud / Stargate cloud capacity | 81 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 79 | META | Meta Platforms | US | 美国资产池 | D0-D1 | Open-source frontier + AI products + internal infra | 78 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 80 | AMD | AMD | US | 美国资产池 | D1 | GPU/CPU AI compute | 77 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration |
| 81 | MSFT | Microsoft | US | 美国资产池 | D0-D1 | Azure + OpenAI demand aggregator | 77 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |

## P2/P3 雷达 Top 30

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 82 | SU.PA | Schneider Electric | 欧洲 | 卫星资产池 | D2-D4 | Data center electrical equipment | 91 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 83 | CRWV | CoreWeave | US | 美国资产池 | D1-D4 | NeoCloud / GPU-as-a-Service | 86 | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap | data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration |
| 84 | 005930.KS | Samsung Electronics | 韩国 | 卫星资产池 | D2-D4 | HBM/DRAM/foundry/packaging | 83 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 85 | KLA | KLA | US | 美国资产池 | D3-D4 | Inspection/metrology | 83 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 86 | ABBN.SW / ABB | ABB | 欧洲 | 卫星资产池 | D2-D4 | Grid/electrification/automation | 80 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 87 | ETN | Eaton | US | 美国资产池 | D2-D4 | Electrical equipment | 80 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 88 | 002335.SZ | 科华数据 | A股主板 | 中国资产池 | D2-D4 | UPS/IDC/power | 79 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 89 | 002518.SZ | 科士达 | A股主板 | 中国资产池 | D2-D4 | UPS/power electronics | 79 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 90 | 4004.T | Resonac | 日本 | 卫星资产池 | D3-D4 | Packaging/semiconductor materials | 79 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 91 | CSCO | Cisco | US | 美国资产池 | D2-D4 | Networking + optics + security | 79 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 92 | IFX.DE | Infineon | 欧洲 | 卫星资产池 | D3-D4 | Power semiconductors | 78 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 93 | NTAP | NetApp | US | 美国资产池 | D3-D4 | Enterprise/data center storage | 78 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 94 | SPXC | SPX Technologies | US | 美国资产池 | D3-D4 | Cooling/HVAC/electrical niches | 78 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 95 | NBIS | Nebius | US | 美国资产池 | D1-D4 | NeoCloud / AI cloud | 76 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 96 | POWL | Powell Industries | US | 美国资产池 | D3-D4 | Electrical switchgear | 76 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 97 | STX | Seagate | US | 美国资产池 | D3-D4 | Nearline HDD/object storage | 75 | annual report / quarterly results / customer concentration note / inventory and margin disclosure | AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix |
| 98 | WDC | Western Digital / SanDisk | US | 美国资产池 | D3-D4 | NAND/eSSD/storage | 75 | annual report / quarterly results / customer concentration note / inventory and margin disclosure | AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix |
| 99 | 000021.SZ | 深科技 | A股主板 | 中国资产池 | D3-D4 | EMS/storage packaging/test | 73 | annual report / quarterly results / earnings call / investor presentation / product and application pages | AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin |
| 100 | TSEM | Tower Semiconductor | 以色列/US | 卫星资产池 | D3-D4 | Specialty foundry / SiPh | 73 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 101 | IREN | IREN | US | 美国资产池 | D1-D4 | AI cloud + powered land from crypto mining | 72 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 102 | 3896.HK / KC | 金山云 | H股/ADR | 中国资产池 | D1-D4 | Cloud / AI cloud | 70 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 103 | 603912.SH | 佳力图 | A股主板 | 中国资产池 | D3-D4 | Data center cooling | 70 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 104 | ALFA.ST | Alfa Laval | 欧洲 | 卫星资产池 | D3-D4 | Heat exchangers / thermal | 70 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 105 | SIVE.ST | Sivers Semiconductors | 欧洲 | 卫星资产池 | D3-D4 | Photonics/mmWave | 70 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 106 | 9698.HK / GDS | 万国数据 GDS | H股/ADR | 中国资产池 | D1-D4 | IDC / AI data center | 68 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 107 | APLD | Applied Digital | US | 美国资产池 | D1-D4 | AI data center developer | 68 | 10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures | backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF |
| 108 | CORZ | Core Scientific | US | 美国资产池 | D1-D4 | AI hosting / power campuses | 68 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 109 | IQE.L | IQE | 欧洲 | 卫星资产池 | D3-D4 | Compound semiconductor epi | 67 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 110 | 8035.T | Tokyo Electron | 日本 | 卫星资产池 | D4 | WFE | 66 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 111 | WULF | TeraWulf | US | 美国资产池 | D1-D4 | Powered data center / HPC transition | 66 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |

## P4 排除或暂缓

| Rank | Ticker | Company | Market | Asset Pool | BFS | Module | Score | Sources | Metrics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 138 | 300249.SZ | 依米康 | A股创业板 | 中国资产池 | D3-D4 | Cooling/IDC | 0 | annual report / quarterly results / backlog/orders / investor presentation / product page | data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion |
| 139 | 300308.SZ | 中际旭创 | A股创业板 | 中国资产池 | D2-D3 | Optical modules | 0 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 140 | 300394.SZ | 天孚通信 | A股创业板 | 中国资产池 | D3 | Optical components | 0 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 141 | 300476.SZ | 胜宏科技 | A股创业板 | 中国资产池 | D3 | PCB | 0 | annual report / quarterly results / presentation / capacity expansion announcement / product page | AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin |
| 142 | 300502.SZ | 新易盛 | A股创业板 | 中国资产池 | D2-D3 | Optical modules | 0 | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| 143 | 300666.SZ | 江丰电子 | A股创业板 | 中国资产池 | D4 | Semiconductor materials | 0 | annual report / segment disclosure / capacity expansion note / technical product page | AI/HBM/CoWoS/SiPh material exposure; customer qualification; ASP; utilization; margin; capacity |
| 144 | 603986.SH | 兆易创新 | A股主板 | 中国资产池 | D4-D5 | Memory/MCU | 0 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 145 | 688008.SH | 澜起科技 | A股科创板 | 中国资产池 | D3 | Memory interface/CXL | 0 | annual report / quarterly results / earnings call / investor presentation / product roadmap | HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap |
| 146 | 688012.SH | 中微公司 | A股科创板 | 中国资产池 | D4 | Semiconductor equipment | 0 | annual report / quarterly results / earnings call / investor presentation / company product page | AI-related revenue; orders/backlog; capacity; gross margin; customer concentration; technical roadmap |

## 建议下一步

1. 先从每个资产池各抽 5-8 个 P0 公司做 evidence card。
2. 每张卡只接受公司原文、交易所公告、监管文件、官网技术资料或上下游交叉披露。
3. 完成第一批后再接 ETF holdings、免费价格数据、SEC 13F/N-PORT。
