# AI Infra Universe Coverage Assessment v1

状态：coverage check, pending original-source verification  
日期：2026-05-12

结论：当前 146 家公司已经足够启动第一轮原文核验，但还不是完整 universe。它覆盖了 AI Infra 主链的关键公开市场代表，但在 D4-D5 深层雷达、ETF 工具、软件/数据层、数据中心 REIT/utility、部分日韩欧材料化工小盘上还需要后续补齐。

边界：本文只评估研究 universe 覆盖，不是投资建议、买卖建议、目标价或仓位建议。

## 当前覆盖面

| 维度 | 覆盖情况 |
| --- | --- |
| 总记录 | 146 |
| 中国资产池 | 48 |
| 美国资产池 | 48 |
| 卫星资产池 | 50 |
| D1-D3 主战场 | 已有足够代表公司，可以开始核验 |
| D4-D5 深层雷达 | 有代表，但不完整 |
| Batch 1 | 24 家，每个资产池各 8 家 |

## 已经覆盖得比较好的方向

| 模块 | 当前判断 |
| --- | --- |
| GPU / cloud / custom ASIC | 已有 NVDA、AVGO、AMD、GOOGL、MSFT、AMZN、ORCL、CRWV、NBIS 等主线 |
| HBM / DRAM / memory | 已有 SK hynix、Samsung、Micron 等代表 |
| CoWoS / leading-edge / OSAT | 已有 TSMC、ASE、ASMPT、BESI、SUSS、TOWA、DISCO、Hanmi、长电、通富、华天等线索 |
| ABF / substrate / PCB / CCL | 已有 Ibiden、Unimicron、Nan Ya PCB、Kinsus、沪电、深南、生益、兴森等 |
| Optical / CPO / 800G / 1.6T | 已有 COHR、LITE、FN、AAOI、MRVL、MTSI、CRDO、光迅、华工、剑桥等 |
| Power / cooling / electrical | 已有 VRT、ETN、Schneider、ABB、Delta、MOD、POWL、英维克、科华、科士达、思源等 |
| AI server / ODM | 已有 工业富联、浪潮、联想、Quanta、Wiwynn 等 |
| Test / probe / metrology | 已有 Advantest、Teradyne、FormFactor、Leeno、ISC、MPI、WinWay、Camtek、Nova、Onto 等 |

## 主要缺口

| 缺口 | 为什么不算致命 | 后续处理 |
| --- | --- | --- |
| ETF 工具本身未作为独立 instrument 入库 | 当前重点是公司 universe，不是组合工具表 | 第二阶段单独建 `etf_universe`，覆盖 SMH、SOXX、XSD、AIQ、GRID、PAVE、URA 等 |
| 数据中心 REIT / colo / land bank 不完整 | 已有 GDS、APLD、IREN、CORZ、CRWV 等线索，但传统 colo 代表不足 | 补 EQIX、DLR 等作为 D2-D4 / cash-flow benchmark |
| Utility / power producer / nuclear 仍偏少 | 当前已有 GEV、BE、CCJ、LEU、ABB、Schneider、ETN 等，但电力资产链还不全 | 作为 D5 雷达补 CEG、VST、TLN、NRG、OKLO、SMR、BWXT 等候选线索后再核验 |
| CDS / credit risk 没有成体系 | 目前只在 NeoCloud 反证里提到融资/债务，没有形成信用仪表盘 | 新增 `CDS / Credit Risk Radar`，跟踪 CDS/bond spread、debt maturity、lease liabilities、interest coverage、GPU residual |
| 美国半导体设备 broad beta 不完整 | 已有 KLA、ASML、TEL、ASM Int、Ebara 等，缺 AMAT、LRCX 这类 broad WFE | 可补为 D4 beta / benchmark，不优先放 Batch 1 |
| 日韩欧材料/化工/气体小盘不完整 | D4 默认雷达，不应抢占 D1-D3 核验资源 | 后续补 Shin-Etsu、SUMCO、JSR、Tokyo Ohka、Fujifilm、Soulbrain、Dongjin、SK Materials 等候选 |
| 软件/数据/推理栈公开公司覆盖不足 | 本项目主线是 AI Infra 物理瓶颈，不是 SaaS 应用 | 后续单独建 software infra radar，覆盖 Databricks/private、Cloudflare、Snowflake、Elastic、MongoDB 等 |
| A 股创业板/科创板被排除 | 这是用户约束，不是产业链不存在 | 若交易约束变化，再把中际旭创、新易盛、天孚通信、澜起、中微等从排除池重评 |
| 私有公司缺失 | OpenAI、Anthropic、Core private suppliers、CoolIT/Boyd 等不可直接交易或披露不足 | 只作为需求源头和上下游交叉验证线索 |

## 判断

当前公司池不需要继续无止境扩张。现在更重要的是把 `Batch 1` 的 24 家做成原文证据卡，确认这套 BFS + 评分系统能否把“主题公司”筛成“有真实订单/收入/瓶颈/毛利传导”的公司。

建议节奏：

1. 先核验 `source_verification_batch1.csv` 的 24 家。
2. 每完成 5-8 张 evidence card，更新一次 score 和 pool。
3. 同时开一个 `gap radar`，只补明显缺失的 ETF、REIT/colo、utility/nuclear、broad WFE、日韩欧材料化工。
4. 不把 D4-D5 缺口升级为核心，除非能证明它反向卡住 D0-D2。

## 最终回答

够用，但不够全。  
够用在于：它已经覆盖 D1-D3 主战场和三大资产池，可以开始严肃核验。  
不够全在于：ETF 工具、数据中心 REIT、utility/nuclear、CDS/credit risk、broad WFE、日韩欧材料化工、软件 infra 还需要作为第二阶段 gap radar 补齐。
