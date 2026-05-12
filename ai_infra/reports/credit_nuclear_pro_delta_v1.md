# Credit/CDS + Nuclear/Firm Power Pro Delta v1

日期：2026-05-12  
状态：ChatGPT Pro output reviewed, pending original-source verification  
边界：本文件是对 Pro 输出的项目整合判断，不是投资建议、买卖建议、目标价或仓位建议。

## 结论

这份 Pro 输出是有用的，但它不是新的主线。它把两个之前偏散的模块固定成 AI Infra 的横向反证层：

| 模块 | 正确位置 | 作用 | 当前动作 |
| --- | --- | --- | --- |
| Credit / CDS / Financing Risk | D1-D5 横向反证层 | 判断 NeoCloud、AI data center、IDC、REIT、电力设备等重资产扩张是否变成融资泡沫 | 建 credit evidence card 和 quarterly dashboard |
| Nuclear / Firm Power / Grid | D2-D5 电力交付反证层 | 判断 AI data center 的供电、并网、PPA、监管和燃料是否反向卡住 D0-D2 | 建 firm power evidence card 和 quarterly dashboard |

主线优先级不变：`D1-D3` 仍然先看 GPU/ASIC/cloud、HBM/CoWoS、optics/networking、server/rack、power/cooling、testing/equipment/materials。Credit 和 firm power 是准入门槛和反证系统，不是把所有核电、铀、SMR、信用 ETF 都升成核心。

## 这次 Pro 输出带来的新增规则

1. 所有重资产 AI Infra 公司都要过一张 credit card。
   - 适用：CoreWeave、Oracle、Applied Digital、IREN、TeraWulf、Core Scientific、GDS、Equinix、Digital Realty、Nebius，以及后续 NeoCloud / data center developer。
   - 先看：RPO/backlog quality、lease liabilities、debt maturity、interest expense、OCF/FCF、GPU depreciation、customer concentration、contract terms。

2. 所有 nuclear / firm power 公司默认 D5 雷达。
   - 只有出现 named AI/data center customer、MW/GW、PPA term、interconnect、regulatory milestone、fuel path、financing path、revenue/backlog entry，才允许升级为 D2/D3 候选。
   - CEG、TLN 可以优先做卡；OKLO、SMR、LEU、CCJ、URA、NLR 仍要防止远期叙事误升核心。

3. 无单名 CDS 时先用 proxy。
   - 免费/可得优先级：FRED IG/HY OAS、HYG/LQD、公司债/convert、SEC debt footnotes、ETF flows、options IV/skew。
   - Proxy 只做融资环境和风险偏好，不证明公司 AI 基本面。

4. 电力链条要拆近端和远端。
   - 近端：transformer、switchgear、substation、UPS/PDU、gas turbine、grid interconnect，和 AI data center 交付更直接。
   - 中远端：existing nuclear PPA、SMR、uranium、HALEU、fuel cycle，必须靠合同和监管 milestone 升级。

## 对现有本地队列的影响

现有本地队列已经覆盖了核心骨架：

| 本地文件 | 已覆盖 | 需要补的点 |
| --- | --- | --- |
| `reports/credit_cds_radar_queue_v1.md` | CRWV、NBIS、ORCL、APLD、IREN、CORZ、WULF、GDS、KC、EQIX、DLR、HYG、LQD、CDX IG/HY | 增加 FRED IG/HY OAS、convert screen、hyperscaler credit anchor、private credit / infra funds 作为 proxy/watchlist |
| `reports/nuclear_firm_power_queue_v1.md` | CEG、VST、TLN、GEV、LEU、CCJ、NRG、OKLO、SMR、BWXT、BE、FLNC、PWR、URA、NLR | 增加 Mitsubishi Heavy、Siemens Energy、Kazatomprom、Rolls-Royce SMR、NERC/FERC/PJM 作为非公司证据源 |
| `notes/2026-05-12-chatgpt-pro-credit-cds-nuclear-radar.md` | Pro 输出整理版 | 只作为线索，不直接写入“原文已证明” |

## 推荐执行顺序

| 顺序 | 任务 | 产出 |
| ---: | --- | --- |
| 1 | 建 credit card 模板并先做 CRWV / ORCL / APLD / IREN / WULF | 验证 RPO、lease、debt、interest、FCF 字段能否跑通 |
| 2 | 建 firm power card 模板并先做 CEG / TLN / GEV / Siemens Energy / MHI | 验证 MW、PPA、interconnect、regulatory、fuel 字段能否跑通 |
| 3 | 建 quarterly dashboard 模板 | `credit-dashboard-quarterly.md` 与 `firm-power-dashboard-quarterly.md` |
| 4 | 把 proxy 接入第二阶段数据层 | FRED OAS、HYG/LQD、ETF holdings、SEC filings、13F/N-PORT |
| 5 | 回填到 D0-D5 BFS | 判断哪些 D5 雷达真的能反向卡住 D0-D2 |

## 当前判断

这份结果应该采纳为项目方法论补丁。它最重要的价值不是新增股票名，而是把“高增长重资产 AI Infra”拆成两个可以证伪的问题：

- **融资反证**：收入和 backlog 是否真的转化为现金流，还是被债务、租赁、折旧、利息和客户集中吞掉？
- **电力反证**：数据中心是否真的能拿到电、并网、PPA、监管许可和燃料，而不是只有新闻稿？

下一步不应该把核电和 CDS 扩成第三主线；应该先做少量 evidence card 样板，把这两个模块作为 D1-D3 研究的风控约束。
