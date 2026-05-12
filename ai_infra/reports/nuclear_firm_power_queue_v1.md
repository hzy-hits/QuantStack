# Nuclear / Firm Power Radar Queue v1

状态：executable radar queue, pending original-source verification  
边界：核电和 firm power 研究，不是投资建议、买卖建议、目标价或仓位建议。

## 研究目的

判断 AI data center 的电力约束是否会从近端电力设备进一步传导到核电、firm power、PPA、燃气轮机、铀和核燃料。

核心问题：

- 是否有真实数据中心客户合同或 PPA？
- 是否能在 AI infra 投资周期内实际供电？
- 收入和现金流是否已经体现？
- 是电力真实瓶颈，还是远期政策/能源叙事？

## 队列

| Priority | Ticker | Company | Asset Type | Why It Matters | Metrics To Verify | Upgrade Signal | Downgrade Signal |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | CEG | Constellation Energy | nuclear power / 24-7 clean power | 美国核电与数据中心 PPA 最直接候选之一，用来验证 AI DC 是否推动核电资产重估 | nuclear generation; contracted power; data center/customer PPAs; capacity factor; power prices; capex; license extensions | data center contracts or PPAs clearly improve long-term revenue visibility | no AI/data center contract evidence; power price or regulatory risk dominates |
| P0 | VST | Vistra | merchant power / nuclear / gas | merchant power + nuclear/gas exposure，适合观察 AI load 对电价和 firm power 的传导 | nuclear/gas generation; hedge book; power prices; data center exposure; FCF; capital returns | AI load supports power price/contracts and cash flow durability | commodity power beta dominates; no data center linkage; regulatory/capacity market risk |
| P0 | TLN | Talen Energy | nuclear + data center campus | nuclear-adjacent data center power narrative代表，需核验合同、监管和项目可行性 | nuclear plant output; data center contract terms; interconnect; regulatory approvals; debt; FCF | contracted AI/data center load with regulatory path and visible cash flow | project blocked/delayed; contract economics weak; debt/refinancing pressure |
| P0 | GEV | GE Vernova | gas turbines / grid | 燃气轮机和电网设备是 AI DC 更近端 firm power 约束 | gas turbine backlog; grid backlog; data center customer exposure; margin; lead times | data center/grid demand raises backlog and margin with long lead times | non-AI utility cycle dominates; project delays or margin pressure |
| P0 | LEU | Centrus Energy | nuclear fuel / HALEU | HALEU/核燃料是先进核能远端约束，但距离 AI DC 很远 | HALEU production milestones; contract backlog; DOE funding; cash burn; licensing | contracted HALEU milestones and funding support advanced nuclear supply chain | regulatory/project delays; cash burn; AI linkage remains indirect |
| P0 | CCJ | Cameco | uranium / nuclear fuel | 铀和核燃料供给链代表，但要防止只是 commodity beta | contracted volumes; realized price; production; long-term contracts; fuel services exposure | long-term contract book and nuclear demand strengthen structural supply story | spot uranium commodity cycle dominates; AI/data center linkage too indirect |
| P1 | NRG | NRG Energy | power producer / retail | 美国电力负荷增长和数据中心需求的电力零售/发电观察对象 | generation mix; load growth; retail margins; data center exposure; FCF; debt | data center load improves contract/retail economics with visible cash flow | consumer/retail power risk dominates; AI linkage weak |
| P1 | OKLO | Oklo | advanced nuclear / SMR | SMR/advanced nuclear 叙事代表，必须严查 licensing、cash runway 和客户协议 | licensing milestones; project timeline; customer agreements; cash runway; capex needs | licensed project with credible customer/offtake and funded timeline | pre-revenue narrative, licensing delay, financing dilution, no near-term AI power relevance |
| P1 | SMR | NuScale Power | SMR | 上市 SMR 代表，适合验证 SMR 是否可在 AI infra 时间尺度内贡献供电 | licensing status; project pipeline; customer commitments; cash burn; technology milestones | credible near-term project and customer economics emerge | project cancellations/delays, cash burn, weak customer commitments |
| P1 | BWXT | BWX Technologies | nuclear components / services | 核供应链和组件服务，比纯 SMR 叙事更接近实际订单 | nuclear operations backlog; commercial nuclear exposure; government mix; margin; capacity | commercial nuclear/SMR components show backlog growth tied to capacity additions | government/defense dominates; AI/data center linkage indirect |
| P1 | BE | Bloom Energy | on-site power / fuel cells | 数据中心 onsite power 替代路线，需核验燃料成本和客户经济性 | data center customer contracts; system margin; fuel assumptions; backlog; cash flow | data center contracts prove scalable economics and positive cash conversion | fuel cost, maintenance, financing or margin pressure weakens economics |
| P2 | FLNC | Fluence Energy | battery storage / grid | 储能是 firm power 辅助，不是核电；适合作为 grid stability 雷达 | backlog; grid/data center exposure; gross margin; working capital; project execution | storage backlog tied to data center/grid constraints with margin improvement | project margin volatility; utility cycle dominates; AI linkage weak |
| P2 | PWR | Quanta Services | transmission / grid construction | 输电和变电施工是 AI load 的物理约束之一 | backlog; electric power segment revenue; margin; customer mix; project duration | grid backlog and utility capex visibly accelerate from data center load | broad utility cycle dominates; labor/cost inflation erodes margin |
| P2 | URA | Global X Uranium ETF | ETF / uranium basket | 核燃料主题 ETF，用作资金和持仓雷达，不是公司事实 | holdings; weights; AUM; flows; uranium exposure; regional concentration | helps identify uranium basket coverage and passive flows | ETF flow does not prove AI demand or company fundamentals |
| P2 | NLR | VanEck Uranium and Nuclear ETF | ETF / nuclear basket | 核能主题 ETF，用于找核电/核燃料覆盖公司 | holdings; weights; AUM; nuclear utility/fuel mix | helps identify nuclear/fuel companies and passive coverage | ETF inclusion is not fundamental evidence |

## 第一批最该看

1. `CEG / VST / TLN`: 是否有 data center PPA 或核电/firm power 直接合同。
2. `GEV`: gas turbine 和 grid backlog 是否被 AI data center 需求推高。
3. `LEU / CCJ`: 核燃料是否只是 commodity beta，还是有可验证长期合同。
4. `OKLO / SMR`: SMR 是否只是远期 option，监管和现金流能否支撑。

## 输出标准

每家公司后续 evidence card 必须补：

- 最新 10-K / 10-Q / annual report；
- generation capacity / nuclear fleet / fuel contracts；
- PPA / customer contracts；
- regulatory / licensing milestones；
- capex and cash runway；
- power price or contract economics；
- 当前 radar action：保持雷达 / 升级候选 / 排除叙事。
