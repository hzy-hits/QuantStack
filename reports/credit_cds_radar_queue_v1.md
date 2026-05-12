# Credit / CDS Radar Queue v1

状态：executable radar queue, pending original-source verification  
边界：信用风险研究，不是投资建议、买卖建议、目标价或仓位建议。

## 研究目的

用信用市场和资产负债表验证 AI Infra 重资产扩张是否可持续。这个 radar 不负责找上涨叙事，而是负责找融资端反证。

核心问题：

- backlog 是否能变成现金流？
- 债务、租赁义务和利息费用是否吞掉毛利？
- GPU 残值和折旧年限是否过于乐观？
- 客户集中和合同条款是否足够强？
- 信用市场是否比股票市场更早否定 AI infra 叙事？

## 队列

| Priority | Ticker | Company | Asset Type | Why It Matters | Metrics To Verify | Upgrade Signal | Downgrade Signal |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | CRWV | CoreWeave | NeoCloud / GPUaaS | GPU租赁、AI cloud backlog、重资产融资是 AI infra 信用风险核心样本 | revenue backlog quality; debt maturity; interest expense; lease liabilities; capex commitments; customer concentration; GPU depreciation/useful life; operating cash flow | backlog has strong take-or-pay/prepayment terms, utilization high, interest coverage improving, debt maturity manageable | backlog not cash-backed, customer concentration high, interest expense/depreciation outrun gross profit, GPU residual assumptions weaken |
| P0 | NBIS | Nebius | NeoCloud / AI cloud | 欧洲/全球 AI cloud 扩张样本，融资和利用率决定是否是真平台还是重资产周期 | AI cloud revenue; capex; debt; lease liabilities; utilization; customer quality; power secured; cash burn | secured customers and power convert into revenue with manageable leverage | capex and debt rise faster than revenue; customer quality/utilization unclear |
| P0 | ORCL | Oracle | Hyperscaler / AI cloud | 大型云厂商里 AI infra capex、租赁和债务压力较突出，能代表 hyperscaler 信用扩张边界 | cloud infrastructure revenue; RPO/backlog; capex; debt; remaining performance obligations; depreciation; operating cash flow | AI cloud demand converts to RPO/revenue while FCF and credit metrics remain resilient | AI capex/depreciation/debt pressure outruns cloud margin expansion |
| P0 | APLD | Applied Digital | AI data center developer | AI 数据中心开发商样本，项目融资、客户合同和施工交付是核心风险 | project financing; customer contracts; construction milestones; capex; debt maturity; interest expense; cash runway | signed customers and financing align with project delivery and cash collection | construction delays, financing gaps, customer concentration, dilution/debt pressure |
| P0 | IREN | IREN | Powered land / AI cloud transition | BTC mining 转 AI DC 的典型样本，需要区分 power asset 和 crypto beta | AI cloud revenue; BTC revenue mix; power capacity; customer contracts; capex; debt; GPU utilization | AI contracts and power capacity produce non-BTC recurring revenue with controlled leverage | BTC cycle dominates; AI contracts weak; financing or execution risk rises |
| P0 | CORZ | Core Scientific | AI hosting / power campuses | 破产重组后转 AI hosting/power campuses，信用结构本身就是研究重点 | hosting backlog; customer concentration; debt; lease obligations; power campuses; capex; FCF | long-term customer contracts and improved balance sheet support AI hosting conversion | debt/capex pressure returns; customer concentration; BTC economics dominate |
| P1 | WULF | TeraWulf | Powered data center / HPC transition | powered land + BTC/HPC transition样本，适合观察融资和电力资产价值 | power capacity; AI/HPC contracts; BTC exposure; debt maturity; capex; cash burn | firm AI/HPC contracts validate power asset conversion | BTC exposure dominates; AI customers unproven; financing gap |
| P1 | 9698.HK / GDS | GDS | IDC / AI data center | 中国/亚洲 IDC 重资产样本，融资成本、利用率和客户结构是核心 | net debt; interest expense; utilization; customer concentration; capex; occupancy; maturity wall | AI/data center demand improves utilization and cash flow while leverage stabilizes | debt maturity, low utilization, refinancing cost or customer churn pressure |
| P1 | 3896.HK / KC | Kingsoft Cloud | Cloud / AI cloud | 云服务亏损/毛利修复样本，适合验证 AI cloud 是否改善现金流 | AI/cloud revenue; gross margin; operating cash flow; debt/cash; customer concentration | AI demand improves margin and cash burn without aggressive leverage | losses persist; AI revenue not separable; financing or competition pressure |
| P1 | EQIX | Equinix | Data center REIT / colo | 全球 colo benchmark，用于对比 NeoCloud/IDC 的成熟资产现金流和杠杆 | AFFO; net debt/EBITDA; occupancy; bookings; capex; interconnection revenue; debt maturity | AI/colo demand improves bookings while leverage and AFFO remain healthy | capex rises, occupancy/margins weaken, debt costs pressure AFFO |
| P1 | DLR | Digital Realty | Data center REIT / colo | 批发数据中心/colo benchmark，适合看 AI demand 对 REIT 现金流和融资的传导 | leasing volume; renewal spreads; development pipeline; debt maturity; AFFO; capex | AI leasing converts to cash yield and stable funding | development capex and debt costs pressure returns |
| P2 | HYG | iShares iBoxx High Yield Corporate Bond ETF | credit proxy | 高收益信用 proxy，用于判断 AI infra 高杠杆资产环境 | yield; spread proxy; flows; drawdown; duration; sector exposure | spreads stable/tight while AI infra financing remains open | spreads widen, high-yield issuance weakens, refinancing risk rises |
| P2 | LQD | iShares Investment Grade Corporate Bond ETF | credit proxy | 投资级信用 proxy，用于观察 hyperscaler/utility 融资环境 | yield; spread; duration; flows; corporate issuance backdrop | IG spreads stable and rates supportive | IG spreads widen or rates pressure long-duration capex |
| P2 | CDX IG/HY | CDX credit indexes | credit index | 如果能拿到数据，用作最直接信用风险仪表盘 | CDX IG spread; CDX HY spread; weekly changes; stress regime | credit market confirms AI infra financing remains open | credit stress appears before equity narrative breaks |

## 第一批最该看

1. `CRWV`: backlog、债务、客户集中、GPU 折旧。
2. `ORCL`: AI cloud capex、debt、RPO、FCF。
3. `APLD / IREN / CORZ`: powered land 和 AI hosting 是否能脱离 BTC beta。
4. `EQIX / DLR`: 成熟 data center REIT benchmark。

## 输出标准

每家公司后续 evidence card 必须补：

- 最新 10-K / 10-Q / annual report；
- debt maturity；
- interest expense；
- lease liabilities；
- capex commitments；
- cash flow；
- customer concentration；
- backlog / contract quality；
- 当前 credit action：保持雷达 / 升级候选 / 降级。
