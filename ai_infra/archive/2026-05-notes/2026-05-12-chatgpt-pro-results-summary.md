# ChatGPT Pro 结果摘要：HBM / CoWoS / Testing

日期：2026-05-12  
状态：Pro 输出摘要，待原始出处核验

## 本轮文件

| 主题 | 文件 | 状态 |
| --- | --- | --- |
| HBM 结构性超级周期 | [2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md) | 已抓取，待核验 |
| CoWoS / Advanced Packaging | [2026-05-12-chatgpt-pro-cowos-advanced-packaging.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-cowos-advanced-packaging.md) | 已抓取，待核验 |
| AI / HBM Testing / Metrology | [2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md) | 已抓取，存在上下文污染标注 |

## 初步可用判断

### HBM

Pro 的核心判断是：HBM 已经从单纯供需错配，转向高端 AI accelerator 的长期结构性内存标准，但仍可能有周期、ASP 下行和供给过剩风险。

优先核验：

- NVIDIA / AMD accelerator 规格中 HBM 容量和带宽的代际变化。
- JEDEC HBM4 标准。
- SK hynix、Samsung、Micron 关于 HBM3E / HBM4 / HBM4E 的量产、送样、客户认证和收入口径。
- Hanmi、ASMPT、BESI、SUSS、TOWA、DISCO、Advantest、Teradyne 对 HBM 设备/测试的原文披露。

### CoWoS / Advanced Packaging

Pro 的核心判断是：CoWoS 仍是一级瓶颈，但瓶颈不是简单从 TSMC 转走，而是形成 TSMC capacity、ABF/substrate、TCB/hybrid bonding、TBDB、dicing/grinding、molding、inspection/metrology 的多点约束。

优先核验：

- TSMC 对 advanced packaging capacity tightness、CoWoS 扩产、OSAT partner 的原文表述。
- Ibiden、AT&S、Shinko、Unimicron、Nan Ya PCB、Kinsus 对 AI substrate / ABF / glass / T-glass 的披露。
- ASMPT、BESI、SUSS、TOWA、DISCO、Camtek、Nova 对 HBM / 2.5D / hybrid bonding / inspection 的订单和收入口径。

### AI / HBM Testing

Pro 的核心判断是：AI/HBM 确实把部分测试、探针、量测/检测变成高弹性瓶颈，但不能把所有半导体测试公司都自动映射为 AI 受益。

可用拆分：

- ATE 测试时间/复杂度：Advantest、Teradyne。
- Probe card / socket / interface：Technoprobe、FormFactor、MPI、WinWay、ISC、Leeno、TSE。
- Advanced packaging inspection / metrology：Camtek、Nova、Onto、KLA、Lasertec。

质量问题：

- 该输出开头出现 `insurance` 上下文污染，应降低初始信任。
- KLA、Nova、Onto、Lasertec 属于更宽口径 process control / metrology，不应直接等同为 HBM 专用瓶颈。
- 需要严格拆 revenue mix，尤其是 Chroma、Leeno、TSE、KLA、Lasertec。

## 下一步建议

第一步先不要继续启动更多 Pro 会话。先做一轮 source-backed 核验，把这 3 个主题里重复出现、且最可能有弹性的交叉点证实或证伪：

1. `HBM4 / 16-high` 是否提高 TCB、hybrid bonding、temporary bonding、molding、testing 价值量。
2. `CoWoS 扩产` 后下一层瓶颈到底是 ABF/substrate、TBDB、TCB/hybrid bonding、inspection，还是 test。
3. `HBM testing / probe card / metrology` 里哪些公司有直接 AI/HBM 收入证据，哪些只是宽口径半导体 beta。

建议第一个 source-backed research note 从 `ASMPT / BESI / SUSS / TOWA / Advantest / Camtek / Onto` 中选 2-3 家开始，因为它们在三份输出中重复出现，且更贴近 HBM/CoWoS 外溢链。
