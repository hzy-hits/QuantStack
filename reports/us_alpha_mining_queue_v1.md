# US Alpha Mining Queue v1

状态：executable mining queue, pending original-source verification
边界：这是研究优先级队列，不是买入清单、投资建议、目标价或仓位建议。

## 使用方式

先做 `P0_us_alpha`，每家公司只回答一个问题：原文是否证明它能从 D1-D3 AI Infra 瓶颈中拿到收入、毛利或现金流？

## Priority Counts

| priority | count |
| --- | ---: |
| P0_us_alpha | 12 |
| P1_verify | 9 |
| P2_large_cap_context | 9 |

## Queue

| rank | priority | cluster | ticker | company | BFS | module | score | card |
| ---: | --- | --- | --- | --- | --- | --- | ---: | --- |
| 1 | P0_us_alpha | optics_connectivity | COHR | Coherent | D2-D3 | 800G/1.6T optics + lasers | 100 | evidence/batch1/014-COHR-Coherent.md |
| 2 | P0_us_alpha | optics_connectivity | FN | Fabrinet | D2-D3 | Optical manufacturing | 96 | evidence/batch1/026-FN-Fabrinet.md |
| 3 | P0_us_alpha | power_thermal | MOD | Modine | D2-D3 | Thermal management | 96 | evidence/batch1/027-MOD-Modine.md |
| 4 | P0_us_alpha | ip_storage_eda | RMBS | Rambus | D3 | Memory interface IP | 96 | evidence/batch1/028-RMBS-Rambus.md |
| 5 | P0_us_alpha | optics_connectivity | ALAB | Astera Labs | D3 | PCIe/CXL retimer + connectivity | 93 | evidence/us_alpha/005-ALAB-Astera-Labs.md |
| 6 | P0_us_alpha | optics_connectivity | CRDO | Credo | D3 | AEC/SerDes/connectivity | 93 | evidence/us_alpha/006-CRDO-Credo.md |
| 7 | P0_us_alpha | test_metrology | FORM | FormFactor | D3 | Probe card | 93 | evidence/us_alpha/007-FORM-FormFactor.md |
| 8 | P0_us_alpha | optics_connectivity | LITE | Lumentum | D2-D3 | Lasers/datacom optics | 93 | evidence/us_alpha/008-LITE-Lumentum.md |
| 9 | P0_us_alpha | optics_connectivity | MTSI | MACOM | D3 | RF/photonic components | 93 | evidence/us_alpha/009-MTSI-MACOM.md |
| 10 | P0_us_alpha | test_metrology | ONTO | Onto Innovation | D3 | Metrology / inspection | 93 | evidence/us_alpha/010-ONTO-Onto-Innovation.md |
| 11 | P0_us_alpha | ip_storage_eda | PSTG | Pure Storage | D3 | AI storage systems | 93 | evidence/us_alpha/011-PSTG-Pure-Storage.md |
| 12 | P0_us_alpha | test_metrology | TER | Teradyne | D3 | Semiconductor ATE | 93 | evidence/us_alpha/012-TER-Teradyne.md |
| 13 | P1_verify | optics_connectivity | SMTC | Semtech | D3 | Signal integrity / optical DSP adjacent | 92 | evidence/us_alpha/013-SMTC-Semtech.md |
| 14 | P1_verify | optics_connectivity | AAOI | Applied Optoelectronics | D2-D3 | Datacom optical modules | 89 | evidence/us_alpha/014-AAOI-Applied-Optoelectronics.md |
| 15 | P1_verify | neocloud_powered_land | CRWV | CoreWeave | D1-D4 | NeoCloud / GPU-as-a-Service | 86 | evidence/us_alpha/015-CRWV-CoreWeave.md |
| 16 | P1_verify | test_metrology | COHU | Cohu | D3 | Test handlers / sockets | 84 | evidence/us_alpha/016-COHU-Cohu.md |
| 17 | P1_verify | neocloud_powered_land | DOCN | DigitalOcean | D1-D3 | SMB cloud / AI cloud services | 81 | evidence/us_alpha/017-DOCN-DigitalOcean.md |
| 18 | P1_verify | ip_storage_eda | NTAP | NetApp | D3-D4 | Enterprise/data center storage | 78 | evidence/us_alpha/018-NTAP-NetApp.md |
| 19 | P1_verify | power_thermal | SPXC | SPX Technologies | D3-D4 | Cooling/HVAC/electrical niches | 78 | evidence/us_alpha/019-SPXC-SPX-Technologies.md |
| 20 | P1_verify | neocloud_powered_land | NBIS | Nebius | D1-D4 | NeoCloud / AI cloud | 76 | evidence/us_alpha/020-NBIS-Nebius.md |
| 21 | P1_verify | power_thermal | POWL | Powell Industries | D3-D4 | Electrical switchgear | 76 | evidence/us_alpha/021-POWL-Powell-Industries.md |
| 22 | P2_large_cap_context | optics_connectivity | NVDA | NVIDIA | D1 | GPU/CUDA + networking + rack-scale systems | 100 | evidence/batch1/015-NVDA-NVIDIA.md |
| 23 | P2_large_cap_context | power_thermal | VRT | Vertiv | D2 | AI data center power/thermal | 100 | evidence/batch1/016-VRT-Vertiv.md |
| 24 | P2_large_cap_context | optics_connectivity | AVGO | Broadcom | D1-D2 | Custom ASIC + networking + CPO | 99 | evidence/batch1/019-AVGO-Broadcom.md |
| 25 | P2_large_cap_context | ip_storage_eda | SNPS | Synopsys | D3 | EDA/IP | 95 | evidence/batch1/031-SNPS-Synopsys.md |
| 26 | P2_large_cap_context | optics_connectivity | MRVL | Marvell | D1-D3 | Custom silicon + optical DSP + networking | 93 |  |
| 27 | P2_large_cap_context | optics_connectivity | AMZN | Amazon | D0-D1 | AWS + Trainium + Anthropic | 92 |  |
| 28 | P2_large_cap_context | optics_connectivity | ANET | Arista Networks | D2 | AI Ethernet networking | 92 |  |
| 29 | P2_large_cap_context | ip_storage_eda | CDNS | Cadence | D3 | EDA/simulation | 92 |  |
| 30 | P2_large_cap_context | optics_connectivity | GOOGL | Alphabet / Google | D0-D1 | Gemini + TPU + GCP | 92 |  |

## Cluster Playbooks

### optics_connectivity

- Why: AI clusters need more east-west bandwidth; this bucket tests whether optics/connectivity suppliers capture D2-D3 value.
- Questions: What share of revenue is datacenter/AI? Are 800G/1.6T/CPO/AEC products qualified and ramping? Is margin rising despite price pressure?
- Sources: 10-K/10-Q; latest earnings release; investor presentation; product qualification pages; customer/hyperscaler references.
- Upgrade: Original sources show AI datacenter mix, qualified products, durable customer demand, and margin support.
- Downgrade: Revenue is telecom recovery, one-customer ramp, ASP compression, or CPO timing slips.

### power_thermal

- Why: AI data centers are constrained by power delivery and heat removal; this bucket tests time-to-power bottlenecks.
- Questions: How much backlog/revenue is data-center related? Are lead times tight? Does gross margin improve with AI demand?
- Sources: 10-K/10-Q; earnings release; backlog/order disclosures; investor presentation; product pages.
- Upgrade: Backlog, book-to-bill, data center mix, lead time, and margin all improve from AI/DC demand.
- Downgrade: Orders are pulled forward, data centers slip, cooling/power products commoditize, or customer pricing pressure rises.

### ip_storage_eda

- Why: AI ASIC, memory bandwidth, and data pipelines can create value in IP, EDA, and storage layers.
- Questions: Is the revenue connected to AI chips, HBM/CXL/DDR, enterprise AI storage, or just generic tech spend?
- Sources: 10-K/10-Q; earnings release; investor presentation; product roadmap; customer/design-win commentary.
- Upgrade: Original sources show AI/HPC design wins, royalties, enterprise AI storage demand, or memory-interface pull-through.
- Downgrade: Exposure is generic software/storage/EDA beta, customer ROI is weak, or revenue timing is one-off.

### test_metrology

- Why: AI accelerators, HBM, and advanced packaging raise test, probe, and inspection complexity.
- Questions: Is growth tied to HBM/GPU/advanced packaging rather than broad semi beta? Are orders and margins improving?
- Sources: 10-K/10-Q; earnings release; investor presentation; product/application pages; customer segment commentary.
- Upgrade: Company filings tie revenue/orders to HBM, AI accelerators, advanced packaging, or high-complexity probe/test.
- Downgrade: Revenue is broad WFE/test cycle recovery with no AI/HBM mix disclosure, or test time declines.

### neocloud_powered_land

- Why: NeoCloud and powered land have high upside but must pass credit and utilization tests.
- Questions: Are contracts take-or-pay? Is utilization high? Do debt, leases, depreciation, and interest stay manageable?
- Sources: 10-K/20-F/10-Q; S-1 if relevant; debt and lease footnotes; customer contracts; power/site disclosures.
- Upgrade: Backlog converts to cash revenue, utilization is high, customer quality is clear, and leverage is manageable.
- Downgrade: Customer concentration, weak contract terms, GPU residual risk, power delays, debt/interest, or negative FCF dominate.

