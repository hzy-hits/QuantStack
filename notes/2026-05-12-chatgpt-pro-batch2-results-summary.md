# ChatGPT Pro 第二批结果摘要：光互连 / Custom ASIC / 电力液冷

日期：2026-05-12  
状态：Pro 输出摘要，待原始出处核验

## 本轮文件

| 主题 | 文件 | 状态 |
| --- | --- | --- |
| 800G / 1.6T / CPO / Silicon Photonics | [2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md) | 已抓取，待核验 |
| Scale-up Fabric / Custom ASIC | [2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md) | 已抓取，待核验 |
| 电力设备 / 液冷 / 热管理 | [2026-05-12-chatgpt-pro-power-cooling-thermal.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-power-cooling-thermal.md) | 已抓取，待核验 |

## 初步可用判断

### 光互连 / CPO / 硅光

Pro 的核心判断是：光互连正在从电信周期扩展为 AI cluster 算力基础设施周期的一部分，但只有能被东西向流量、scale-out / scale-up fabric、1.6T pluggable、LPO/LRO/TRO、CPO/ELS、SiPh 直接验证的公司才算 AI Infra 暴露。

优先核验：

- Coherent、Lumentum：InP / EML / CW laser / 1.6T 相关产品是否进入收入。
- Broadcom、Marvell、Credo、Semtech、MACOM：DSP、TIA/driver、LPO/LRO/CPO、AI networking 收入口径。
- Sivers、IQE、Tower、GlobalFoundries、Ayar Labs、POET：CPO / ELS / SiPh / InP 小盘链条是否有真实订单和现金流。
- Corning：AI 数据中心光连接长约和产能扩张是否进入订单/backlog。

主要反证：

- CPO 延后，pluggable optics 生命周期拉长。
- LPO/LRO/TRO 降低功耗，使 CPO 迟迟不放量。
- 铜缆/AEC 在短距连接中延寿。
- 光模块 ASP 快速下行，收入涨但毛利率不涨。

### Scale-up Fabric / Custom ASIC

Pro 的核心判断是：hyperscaler ASIC 不是简单替代 GPU，而是把价值分配从单卡扩散到 ASIC design、EDA/IP、HBM、CoWoS、SerDes、retimer/AEC、switch/NIC、rack power/cooling。

优先核验：

- Google TPU、AWS Trainium、Microsoft Maia、Meta MTIA 的官方技术资料和财报 CapEx/AI infra 表述。
- Broadcom、Marvell custom silicon / XPU / AI networking 原文。
- Astera、Credo、Rambus、Alphawave、Arm、Synopsys、Cadence 的 PCIe/CXL/SerDes/HBM IP 暴露。
- Alchip、GUC、Faraday 的 NRE、turnkey、mass production、客户集中和先进节点收入。

主要反证：

- 自研 ASIC 主要内化在 hyperscaler，不外溢给公开供应链。
- CUDA / NVLink / NVIDIA full-stack 继续压倒开放 ASIC 生态。
- ASIC 放量被 HBM、CoWoS、软件栈或客户工作负载限制。

### 电力设备 / 液冷 / 热管理

Pro 的核心判断是：AI 数据中心电力链条要从需求源头、数据中心开发、grid interconnect、substation、transformer、switchgear、UPS/PDU、rack power、liquid cooling、heat rejection 逐层验证，不能把泛能源叙事等同于 AI 受益。

优先核验：

- Vertiv、Schneider、Eaton：data center orders、backlog、book-to-bill、gross margin、liquid cooling attach。
- Siemens Energy、ABB/Hitachi Energy：Grid / transformer / switchgear backlog 和 data center 订单可见度。
- Delta、Lite-On、AcBel、Vicor、MPS、Infineon：AI server / rack power / 48V / power module 收入口径。
- Munters、Alfa Laval、Modine、CoolIT、Boyd：数据中心冷却订单、液冷渗透、CDU/cold plate/heat exchanger 认证。

主要反证：

- 电网许可而不是设备供给成为真正瓶颈。
- 订单提前透支，客户推迟数据中心建设。
- 液冷标准化后 ASP 下行。
- 供应商扩产导致 backlog/lead time 缓解。

## 下一步建议

第三批继续跑：

1. NeoCloud 经济模型：CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、Hut 8、TeraWulf、Core Scientific。
2. 非美材料 / 设备隐形冠军：日本、韩国、台湾、欧洲、以色列材料、设备、测试、真空、洁净、封装、基板。
3. 存储超级周期反证：严格区分 HBM、server DRAM、enterprise SSD、commodity NAND、SSD controller。

仍然不要启动 A 股映射。A 股应等美股、日韩、欧洲标的链条完成后再后置映射。
