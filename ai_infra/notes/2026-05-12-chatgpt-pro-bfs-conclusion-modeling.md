# ChatGPT Pro 输出草稿：LLM Dependency BFS 结论建模

状态：ChatGPT Pro output, pending original-source verification  
捕获时间：2026-05-12 15:02:00 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02c9da-db2c-83ea-8327-056b6b76d51d

> 使用边界：本文只作为研究结论建模线索，不作为投资建议或买卖建议。涉及收入、订单、backlog、CapEx、毛利率、产能、ASP、客户关系、技术路线等实际结论，必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游原始披露核验后，才能进入主研究结论。

## 原始提问

会话标题：LLM Dependency BFS 结论建模

我正在做一个 AI super cycle / AI Infra 产业链研究项目。现在希望把研究框架从“泛 AI Infra 分层”升级成“从 LLM Lab 出发的 dependency BFS”。

核心框架如下：

D0：LLM 核心源头
- OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI。
- 它们定义模型能力、训练节奏、推理规模、token 需求和 AI 产品形态。

D1：模型强依赖一阶
- NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
- 这些是 LLM Lab 训练、推理和产品化不可绕开的直接依赖。

D2：一阶依赖的直接瓶颈
- HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- 这些直接限制 D1 交付、性能、成本或扩张速度。

D3：二阶瓶颈供应商
- HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件。
- 这些限制 D2 放量，可能是高弹性小中盘机会。

D4-D5：深层雷达
- 材料、气体、化学品、真空、洁净、能源、电网、融资、监管。
- 只做雷达，除非能证明反向卡住 D0-D2，否则不升级为核心。

研究原则：
- 主看 D1-D3，最深看到 D5。
- D4-D5 只做雷达；超过 D5 默认排除。
- A 股映射也必须标注 BFS 深度，优先看能映射到 D1-D3 的公司；超过 D3 的公司只能进观察池，除非有强原文证据证明其反向卡住 D0-D2。
- 不给买卖建议，不给目标价。
- 所有涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线的数据，都必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游交叉披露核验。ChatGPT 输出只作为线索。

请帮我生成一套“AI Infra 研究结论建模体系”，要求能直接落地到 markdown 文档和公司研究卡片中。

请输出：

1. 研究对象的数据模型
   - 一个 company/module card 应该有哪些字段？
   - 如何记录 BFS depth、上游/下游、边关系、证据等级、反证、财报指标？

2. BFS 边关系模型
   - 客户边、BOM 边、产能边、技术边、现金流边、反证边分别如何定义？
   - 每种边需要什么原始证据？
   - 哪些边是强证据，哪些只是弱线索？

3. 结论分层模型
   - 如何把公司分成：核心池、候选池、雷达池、排除池？
   - 如何区分：D1 龙头、D2 直接瓶颈、D3 高弹性瓶颈、D4-D5 雷达约束、主题映射？
   - 如何避免把远端能源/材料公司误判成 AI 核心标的？

4. 评分模型
   请给一个 100 分或 5 星评分体系，至少包含：
   - BFS depth 权重；
   - 需求真实性；
   - 供给瓶颈强度；
   - 议价权 / 毛利率传导；
   - 财务传导到 FCF；
   - 技术替代风险；
   - 客户集中风险；
   - 证据质量；
   - 反证清晰度；
   - 10 倍弹性 / 指数增长可能性。

5. 结论模板
   请给出几个标准结论句模板，例如：
   - “核心瓶颈，待原文核验”；
   - “D3 高弹性候选，但证据仍不足”；
   - “D4 雷达项，不能作为核心结论”；
   - “收入相关但毛利/FCF 不传导”；
   - “主题映射，暂时排除”。

6. A 股映射规则
   - 如何把海外 D1-D3 瓶颈映射到非科创板、非创业板 A 股主板？
   - A 股公司必须回答哪些问题？
   - 如何处理 D4-D5 的 A 股公司？
   - 如何避免 A 股概念先行？

7. 研究流程
   请给出实际执行顺序：
   - 从 D0 token demand / model roadmap 出发；
   - 到 D1 GPU/cloud/TPU；
   - 到 D2 HBM/CoWoS/networking/power；
   - 到 D3 equipment/test/material/substrate/optics；
   - 再到 A 股映射；
   - 最后做原始出处核验和反证仪表盘。

8. 产出格式
   请给出可以直接复制到 markdown 的：
   - 总图表格；
   - 公司卡片模板；
   - 证据卡片模板；
   - A 股映射表模板；
   - 季度复盘表模板；
   - 排除池记录模板。

请严格区分：
- 原文已证明；
- 合理推论；
- 待原文核验；
- 主要反证。

请不要输出股票买卖建议，不要给目标价。
展开

## ChatGPT Pro 输出

AI Infra 研究结论建模体系：LLM Dependency BFS 版

这套体系的核心是：把公司/模块当成节点，把“LLM Lab 需求如何传导到该节点”当成 BFS 路径，把客户、BOM、产能、技术、现金流、反证都当成边。
结论不能从“AI 主题”直接生成，只能从“路径 + 边证据 + 财务传导 + 反证”生成。

底层约束沿用你现有项目原则：涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系、价格、出货、技术路线的数据，必须回到公司原始披露、监管文件、官网技术资料或上下游交叉披露；媒体、券商、数据库和 ChatGPT 输出只能作为线索。

research-checklist

 研究流程也应保持“先写假设、先列原始出处、再定位产业链、找客户/订单、看瓶颈和财务传导、列反证、打分、季度复核”的顺序。

research-checklist

0. 总体建模原则
0.1 一句话定义
AI Infra BFS 研究 = 从 D0 LLM Lab 的 token / model roadmap 出发，
沿 dependency edge 向上游做 BFS，
识别 D1-D3 中能真实约束模型训练、推理、交付、成本或扩张速度的节点，
并用原始出处验证其是否能转化为收入、毛利率、FCF 或长期瓶颈租金。
0.2 四类证据状态必须固定写入每张卡片
状态	定义	是否可进入核心结论
原文已证明	公司原始披露、交易所公告、监管文件、官网技术资料、客户/供应商交叉披露能直接证明	可以
合理推论	基于原文已证明事实做出的产业链、技术或财务推导	可以，但必须单独标注
待原文核验	来自媒体、券商、数据库、ChatGPT、市场传闻、未核验截图	不得进入核心判断
主要反证	能削弱或推翻需求、瓶颈、议价权、财务传导、技术路线的证据或指标	必须并列展示
0.3 BFS 方向

默认使用两套方向：

Dependency BFS 方向：
D0 LLM Lab → D1 直接依赖 → D2 直接瓶颈 → D3 二阶瓶颈 → D4-D5 深层雷达

Revenue / Cash-flow 方向：
供应商 → 客户 → 终端 AI Infra 需求

研究时必须同时写清楚：

公司为什么处在这个 BFS depth？
它是被谁依赖？
它向谁收钱？
它能否反向卡住 D0-D2？
1. 研究对象的数据模型
1.1 Node 类型
NodeType:
- Lab：OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI
- Platform：GPU、TPU、ASIC、Cloud、CUDA、ROCm、训练/推理软件栈
- Bottleneck：HBM、CoWoS、TSMC leading-edge、networking、power、cooling
- Supplier：设备、材料、基板、测试、光器件、电力部件
- Company：上市公司或私有公司
- Product：具体产品，例如 HBM3E、CoWoS-L、800G optical module、CDU、TC bonder
- Evidence：原始出处证据卡
- CounterEvidence：反证卡

AI Infra 不是单线产业链，而是从 token 需求传导到算力、HBM/存储、网络、先进封装、晶圆制造、材料、数据中心、电力、融资和监管的多层约束系统。

2026-05-12-ai-super-cycle-resea…

 因此 company/module card 不应只记录“所属赛道”，而应记录其在 BFS 图中的位置和边关系。

1.2 Company / Module Card 字段
A. 基本身份字段
字段	说明
card_id	唯一编号，例如 AIINFRA-HBM-TEST-ADVANTEST-001
object_type	company / module / product / technology route
company_name	公司名称
ticker / exchange	股票代码、交易所；A 股需标明主板/科创板/创业板/北交所
region	美国 / 日本 / 韩国 / 台湾 / 欧洲 / 中国大陆 / 其他
coverage_status	初筛 / 已建卡 / 已核验 / 季度复盘 / 排除
public_private	listed / private / subsidiary / non-investable
primary_module	HBM、CoWoS、Optics、Power、Cooling、ASIC、Testing 等
secondary_modules	可多选
latest_update	最近更新时间
analyst_note_owner	负责研究人或会话编号
B. BFS 深度字段
字段	说明
bfs_depth_primary	D0 / D1 / D2 / D3 / D4 / D5 / >D5
bfs_depth_effective	经过证据校正后的有效深度
bfs_path	例如 OpenAI → Azure → NVIDIA GPU → HBM → HBM test equipment → Company
path_length	路径边数
path_confidence	strong / medium / weak / pending
depth_reason	为什么是这个深度
reverse_blocking_claim	是否声称能反向卡住 D0-D2
reverse_blocking_evidence	支撑反向卡住的证据
depth_upgrade_trigger	由 D4/D5 升级到核心的条件
depth_downgrade_trigger	降级为主题映射或排除的条件
BFS depth 判定规则
Depth	定义	默认处理
D0	LLM 核心源头：模型、产品、token demand 定义者	需求源头，不直接当供应链标的
D1	模型训练/推理不可绕开的直接依赖：GPU/TPU/ASIC、云、CUDA/软件栈	核心池优先
D2	直接限制 D1 交付、性能、成本或扩张的瓶颈：HBM、CoWoS、leading-edge、networking、power/cooling	核心池优先
D3	限制 D2 放量的供应商：测试、设备、基板、InP laser、SiPh、retimer、液冷部件、电力关键部件	高弹性候选重点
D4	材料、气体、化学品、真空、洁净、电网部件、能源链条	雷达池，除非有强反向卡点证据
D5	更深层资金、监管、土地、许可、能源资源、供应链基础设施	雷达池
>D5	过远主题、泛能源、泛材料、泛软件、泛制造	默认排除
C. 产业链关系字段
字段	说明
upstream_nodes	该公司依赖的上游：材料、设备、能源、客户预算等
downstream_nodes	该公司服务的下游：GPU 厂、HBM 厂、OSAT、云厂商、数据中心等
direct_customer_evidence	直接客户证据
indirect_customer_evidence	间接客户证据
bom_position	是否进入关键 BOM / 平台 / 标准
capacity_dependency	是否存在产能瓶颈
technology_dependency	是否依赖特定技术路线
financial_dependency	需求是否进入订单、收入、backlog、毛利、FCF
substitution_map	替代供应商和替代技术
platform_lock_in	是否被 CUDA、NVLink、CoWoS、HBM 规格、客户认证周期等锁定
D. 证据字段
字段	说明
evidence_summary	简短证据摘要
evidence_cards	关联 Evidence Card 编号
strongest_evidence_type	年报 / 季报 / earnings call / investor deck / 技术白皮书 / 客户披露 / 供应商披露
source_quality_score	1-5
source_freshness	报告期和发布日期
direct_quote_available	是否有可引用原文
metric_scope	指标口径：segment、GAAP/non-GAAP、币种、单位、同比/环比
evidence_gap	缺什么原始证据
verification_next_step	下一步核验问题
E. 财报与经营指标字段
字段	说明
ai_related_revenue	AI 相关收入；没有原文则写“待原文核验”
ai_revenue_share	AI 收入占比
segment_revenue	对应 segment revenue
gross_margin	毛利率
operating_margin	经营利润率
capex	CapEx
inventory	存货
backlog / RPO / orders	订单、积压订单、剩余履约义务
book_to_bill	设备/工业公司重点
ASP / shipment / capacity	价格、出货、产能
FCF	自由现金流
ROIC / payback	投资回报和回收周期
customer_concentration	客户集中度
working_capital	营运资金占用
debt / lease liability	重资产或 NeoCloud 必填
depreciation	折旧压力
utilization	云、数据中心、设备利用率
F. 结论字段
字段	说明
one_sentence_thesis	一句话研究假设
conclusion_tier	核心池 / 候选池 / 雷达池 / 排除池
conclusion_label	D1 龙头 / D2 直接瓶颈 / D3 高弹性瓶颈 / D4-D5 雷达约束 / 主题映射
conclusion_status	原文已证明 / 合理推论 / 待原文核验 / 被反证削弱
score_100	100 分制
star_rating	1-5 星
main_positive_evidence	三条最强支持证据
main_counterevidence	三条主要反证
trigger_to_upgrade	升级条件
trigger_to_downgrade	降级条件
no_buy_sell_note	固定写明：不构成买卖建议，不给目标价
1.3 Module Card 字段

模块卡和公司卡类似，但重点不是单家公司，而是一类 bottleneck。

字段	说明
module_name	HBM / CoWoS / AI Testing / 800G Optics / Liquid Cooling 等
bfs_depth_range	主要分布在 D1-D3 / D2-D4 等
dependency_path	从 D0 到该模块的路径
why_it_matters	为什么会影响 LLM 训练/推理
key_subsegments	子环节
representative_companies	公司池
bottleneck_type	产能 / 良率 / 技术 / 认证 / 资本 / 电力 / 监管
unit_value_driver	每 GPU、每 rack、每 MW、每 wafer、每 HBM stack 的价值量变化
demand_metrics	需求指标
supply_metrics	供给指标
price_metrics	ASP、毛利率、长约、预付款等
counter_dashboard	反证指标
a_share_mapping_potential	是否适合映射 A 股
conclusion	结构性增长 / 周期反转 / 主题映射 / 待核验
2. BFS 边关系模型
2.1 Edge 基本字段
YAML
edge_id:
from_node:
to_node:
edge_type:
dependency_direction:
cashflow_direction:
bfs_depth_from:
bfs_depth_to:
evidence_status: 原文已证明 / 合理推论 / 待原文核验 / 反证
evidence_level: E0 / E1 / E2 / E3 / E4
edge_strength: strong / medium / weak / negative
causality_score_1_5:
specificity_score_1_5:
financial_transmission_score_1_5:
counterevidence_score_-5_0:
source_cards:
last_verified_date:
2.2 证据等级
等级	来源	用途	结论权限
E0	公司年报、10-K/20-F、季报、earnings release、earnings call、交易所公告、监管文件、官网技术资料	核心证据	可支持核心结论
E1	客户/供应商交叉披露、产业协会、标准组织、技术规格、认证文件	强辅助证据	可支持边关系
E2	公司新闻稿、投资者演示、capital markets day	中等证据	可支持候选结论
E3	媒体、券商、数据库、行业访谈	弱线索	待原文核验
E4	ChatGPT、社交媒体、论坛、未核验笔记	线索	不得入结论
2.3 六类 BFS 边
2.3.1 客户边 Customer Edge
项目	内容
定义	A 的产品/服务被 B 采购、认证、使用，A 的收入可能由 B 的 AI Infra CapEx 或产品放量驱动
BFS 含义	证明下游需求能传导到该公司
强证据	客户合同、长约、预付款、backlog、RPO、客户集中度披露、客户/供应商双方交叉确认、明确 design win、量产认证
弱线索	“strategic cooperation”、“AI customer interest”、“潜在客户很多”、未点名大客户、只说 data center opportunity
常见误判	有客户 ≠ 有 AI 收入；有合作 ≠ 有量产；有量产 ≠ 有毛利率提升
强度判断	有金额、有期间、有产品、有客户、有报告期 = strong；只有方向性表述 = weak
2.3.2 BOM 边 BOM Edge
项目	内容
定义	某产品、材料、芯片、设备、光器件、电源模块进入 AI GPU/ASIC/server/rack/data center 的关键 BOM 或技术规格
BFS 含义	证明该节点是物理或技术必需项
强证据	官方产品规格、技术白皮书、平台架构图、客户官网资料、标准组织文件、可验证 teardown、供应商规格书与客户产品对应
弱线索	“可用于 AI server”、“支持高速互联”、“适合数据中心”
常见误判	泛用产品被包装成 AI BOM；支持某标准但未被客户采用
强度判断	绑定明确平台/代际规格/客户资格 = strong；泛兼容 = weak
2.3.3 产能边 Capacity Edge
项目	内容
定义	A 的产能、良率、交期、设备供应、材料供应限制 B 的交付或扩张
BFS 含义	证明 A 可能成为瓶颈租金来源
强证据	产能 sold out、客户预付款、长期供货协议、扩产周期、lead time、book-to-bill、设备订单、管理层明确瓶颈说明
弱线索	行业短缺传闻、涨价传闻、公司扩产但未说明客户/产品
常见误判	周期性短缺被当成结构性瓶颈；产能扩张反而削弱瓶颈
强度判断	客户锁产能 + 长交期 + 毛利率改善 = strong
2.3.4 技术边 Technology Edge
项目	内容
定义	某技术路线是实现性能、功耗、带宽、良率、散热、可靠性目标的必要条件
BFS 含义	证明该节点不是普通供应商，而是架构升级的受益者
强证据	官方架构资料、路线图、标准组织文件、客户技术白皮书、量产认证、论文/技术资料与产品路线对应
弱线索	专利、demo、概念发布、实验室成果、未量产 POC
常见误判	技术领先但商业化慢；路线可能被替代
强度判断	已被下一代平台采用 = strong；只是潜在路线 = weak
2.3.5 现金流边 Cash-flow Edge
项目	内容
定义	需求从订单、backlog、收入、毛利率、经营现金流、FCF、ROIC 体现到财务结果
BFS 含义	证明“产业链相关”能转化为股东经济性，但不代表买卖建议
强证据	segment revenue、AI/data center revenue、gross margin、ASP、book-to-bill、CapEx 回收、FCF 改善、客户预付款
弱线索	总收入增长、股价上涨、管理层泛泛提 AI
常见误判	收入涨但毛利率不涨；收入涨但库存/CapEx/应收吞掉现金流
强度判断	收入 + 毛利率 + FCF 同向改善 = strong
2.3.6 反证边 CounterEvidence Edge
项目	内容
定义	任何削弱需求、瓶颈、议价权、财务传导或技术路线的证据
BFS 含义	防止远端节点被叙事越级
强证据	公司下调指引、客户延迟 CapEx、毛利率下滑、库存上升、订单取消、替代技术量产、客户转单
弱线索	传闻、未核验媒体、单一客户评论
常见误判	只写利多，不写可证伪条件
强度判断	能直接推翻核心假设 = fatal；只能提醒不确定性 = medium

你现有 checklist 已经要求每个主题写反证，例如客户 CapEx 放缓、GPU 供给放开、HBM 过剩、ASIC 替代、推理成本下降、并网瓶颈、换供应商和毛利率不提升等。

research-checklist

2.4 边强度分类
Edge Strength	条件
Strong	E0/E1 证据；点名产品/客户/报告期/金额/技术规格；能连接到财务或产能
Medium	E0/E1 但未给金额，或 E2 有较明确产品/客户/路线
Weak	E3/E4、泛 AI 表述、无客户、无产品、无财务
Negative	原文反证、财务恶化、路线替代、客户流失、产能过剩
3. 结论分层模型
3.1 四个研究池
池子	进入条件	退出/降级条件
核心池	D1-D2，或 D3 但有强反向卡点；至少 2 条 strong edge；有原始出处；财务传导可见；反证可跟踪	关键边降为 weak；毛利/FCF 不传导；客户需求放缓；技术路线替代
候选池	D2-D3 为主；路径合理；至少 1 条 strong edge 或多条 medium edge；财务传导正在出现	原文核验失败；客户/订单不成立；竞争扩产削弱瓶颈
雷达池	D4-D5；或 D3 证据不足；有可能反向卡住 D0-D2，但尚未证明	证据长期停留在主题层；无产品、无客户、无收入
排除池	>D5；仅主题映射；无客户/BOM/财务证据；或主要反证已推翻假设	出现新的强原文证据后可重开卡
3.2 五类结论标签
标签	典型 Depth	定义	默认池子
D1 龙头	D1	LLM Lab 训练/推理不可绕开的直接平台：GPU/TPU/ASIC、云、CUDA/软件栈	核心池
D2 直接瓶颈	D2	直接限制 D1 交付、性能、成本或扩张速度：HBM、CoWoS、networking、power/cooling	核心池
D3 高弹性瓶颈	D3	限制 D2 放量的供应商，可能因小收入基数、认证壁垒、扩产慢而具备高弹性	候选池/核心池
D4-D5 雷达约束	D4-D5	材料、气体、能源、电网、监管、融资等深层约束	雷达池
主题映射	任意	只与 AI 概念相关，不能证明客户、BOM、产能或财务传导	排除池
3.3 如何避免把远端能源/材料公司误判成 AI 核心标的

远端能源、材料、化学品、气体、洁净、真空、电网公司默认不能因为“AI 需要电/材料/产能”进入核心池。必须回答：

1. 它是否是 D0-D2 的可证明反向卡点？
2. 有没有点名 AI data center、HBM、CoWoS、leading-edge、800G/1.6T、液冷 rack 的客户或产品证据？
3. 是否存在长交期、认证周期、产能锁定、预付款、backlog 或涨价？
4. 收入和毛利率是否已经被该需求拉动？
5. 如果没有它，下游是否真的无法交付，还是可以换供应商/换路线？

项目现有优先级也明确提醒：HBM 设备/测试/材料/基板、光互连/CPO/InP/激光/硅光、电力设备/液冷/热管理、Custom ASIC 供应链和非美隐形供应链是优先方向；而泛 memory、泛能源/SMR/核能、泛半导体设备大盘 beta、低毛利 AI server 组装需要特别谨慎。

2026-05-12-ai-super-cycle-resea…

4. 评分模型：100 分 + 5 星
4.1 100 分制
维度	权重	高分标准
BFS depth 与路径完整性	15	D1-D3；路径短；每条边有强证据；能说明从 D0 到公司的传导
需求真实性	15	有客户、订单、backlog、产品用途、AI/data center revenue、客户 CapEx 证据
供给瓶颈强度	12	扩产慢、认证长、良率难、技术门槛高、客户愿意锁产能
议价权 / 毛利率传导	10	ASP 上行、毛利率改善、竞争格局集中、差异化强
财务传导到 FCF	12	收入、毛利、营运现金流、FCF、ROIC 同步改善；CapEx 可回收
技术替代风险	8	路线被替代概率低；公司能跟随 HBM4、CPO、液冷、ASIC 等路线变化
客户集中风险	6	客户集中可控；合同期限、客户信用、复购、不可替代性较强
证据质量	10	E0/E1 为主；原文口径清晰；二手信息仅作线索
反证清晰度	5	有 3-5 个可证伪指标；知道什么情况会推翻结论
10 倍弹性 / 指数增长可能性	7	小收入基数、AI 占比从低到高、单位价值量上升、瓶颈租金、市场未充分识别

合计：100 分。

4.2 星级
星级	分数	结论
5★	85-100	核心瓶颈或强 D3 高弹性候选
4★	70-84	核心候选，需继续核验财务或反证
3★	55-69	雷达/重点观察
2★	40-54	主题相关但证据不足
1★	<40	排除或暂不投入深挖
4.3 强制封顶规则
情况	最高分
没有 E0/E1 原文证据	65
只有媒体/券商/ChatGPT 线索	55
D4-D5 且无反向卡住 D0-D2 证据	60
只有收入增长、无毛利率/FCF 传导	70
客户高度集中且无长约/认证/不可替代证据	75
技术路线可能被替代且公司无跟随能力	65
明确反证已出现，例如订单取消、毛利率恶化、库存高企	50
仅 AI 概念映射，无客户/BOM/财务证据	40

你现有评分基线已包含 AI 需求相关度、供给瓶颈、议价权、持续性、财务传导、技术护城河、基建周期位置、资产重估空间、反证清晰度，并用分数将公司/模块放入不同研究池。

research-checklist

 这里是在该框架上加入 BFS depth 和 10 倍弹性权重。

5. 标准结论句模板
5.1 核心瓶颈，待原文核验
【结论状态：核心瓶颈，待原文核验】
[公司/模块] 当前可暂列为 D[depth] [模块] 核心瓶颈候选。
合理推论是：如果 D0 LLM Lab 的 [训练/推理/token/多模态/agent] 需求继续上行，
则 D1 [GPU/TPU/ASIC/cloud] 对 [HBM/CoWoS/networking/power] 的依赖会传导至 [公司/模块]。
但目前 [收入/订单/backlog/客户/毛利率/产能] 仍缺少 E0/E1 原文证据，
结论不得升级为“原文已证明”。
主要反证是：[反证 1]、[反证 2]、[反证 3]。
5.2 D3 高弹性候选，但证据仍不足
【结论状态：D3 高弹性候选，但证据仍不足】
[公司] 位于 D3，属于 [D2 瓶颈] 的二阶供应商。
它的弹性来自 [收入基数小/认证周期长/单位价值量上升/客户切换难/产能扩张慢]。
目前合理推论是 [逻辑链]，但原文尚未证明 [AI 相关收入/客户订单/毛利率传导]。
在 [原文证据] 出现前，只能放入候选池，不能放入核心池。
5.3 D4 雷达项，不能作为核心结论
【结论状态：D4-D5 雷达项】
[公司/模块] 与 AI Infra 的关系位于 D[depth]，属于深层约束或远端供应链。
AI 需求可能通过 [电力/材料/化学品/气体/洁净/融资/监管] 间接传导，
但目前不能证明其反向卡住 D0-D2。
除非出现 [长约/客户点名/产能锁定/backlog/技术认证/交期瓶颈] 的原始证据，
否则只能进入雷达池，不能作为核心 AI Infra 结论。
5.4 收入相关但毛利/FCF 不传导
【结论状态：收入相关，但利润池捕获不足】
[公司] 的收入与 [AI server / data center / GPU platform / cloud CapEx] 相关，
但原文证据显示或尚未证明毛利率、经营利润率、FCF、ROIC 能同步改善。
因此当前更像 [低附加值集成/代工/周期性出货/重资产扩张]，
不能仅凭收入增长定义为核心瓶颈。
下一步必须核验 [gross margin / ASP / inventory / CapEx / FCF / customer concentration]。
5.5 主题映射，暂时排除
【结论状态：主题映射，暂时排除】
[公司] 目前只有 [AI 概念/媒体报道/互动平台/管理层泛泛表述]，
缺少客户边、BOM 边、产能边、技术边或现金流边的 E0/E1 证据。
其 BFS 路径无法从 D0-D3 被可靠连接，
暂列排除池。
重新进入研究池的条件是：[明确客户/产品/订单/收入/毛利率/产能] 原文证据出现。
5.6 原文已证明 + 合理推论组合句
【原文已证明】
原文证明：[公司] 在 [报告期] 披露了 [指标]，口径为 [口径]。

【合理推论】
基于该指标和 [客户/BOM/技术/产能] 边，可以推论 [公司] 可能受益于 D[depth] [瓶颈] 放量。

【待原文核验】
尚未证明：[AI 收入占比/客户名称/订单金额/毛利率传导/产能锁定]。

【主要反证】
若 [客户 CapEx 放缓/技术路线替代/毛利率不提升/库存上升/订单取消]，则该结论降级。
6. A 股映射规则
6.1 总原则

A 股映射必须从海外 D1-D3 瓶颈出发，而不是从 A 股概念出发。

正确顺序：
海外 D0-D3 瓶颈 → 具体产品/技术/BOM/产能卡点 → 中国主板公司是否真实供应 → 原始出处核验 → BFS depth 标注 → 评分

错误顺序：
A 股 AI 概念 → 找故事 → 反向拼产业链 → 直接放入核心池
6.2 非科创板、非创业板 A 股主板筛选流程
步骤	操作
1	先锁定海外瓶颈：HBM、CoWoS、800G/1.6T、CPO、液冷、电力设备、AI testing、ABF/基板等
2	把海外瓶颈拆成具体物料、设备、工艺、标准、客户
3	建立中国供应商长名单
4	过滤交易板块：仅保留沪深主板，剔除科创板、创业板、北交所
5	阅读年报、招股书、公告、投资者关系记录、官网技术资料
6	确认是否有产品、客户、收入、产能、订单、毛利率证据
7	标注 BFS depth：D1-D3 优先；D4-D5 默认雷达
8	打分并放入核心池/候选池/雷达池/排除池
6.3 A 股公司必须回答的问题
1. 公司到底卖什么？产品是否对应 HBM、CoWoS、AI server、800G/1.6T、液冷、电力设备、测试、基板等具体瓶颈？
2. 该产品处于 BFS 的 D几？
3. 下游客户是谁？是否直接或间接服务 GPU、HBM、OSAT、云厂商、AI data center？
4. 是否有原文证明 AI/data center/HBM/CoWoS/高速网络/液冷相关收入？
5. 收入占比是多少？是否只是很小试样或概念产品？
6. 毛利率是否提升？还是只增加低毛利收入？
7. 是否有 backlog、订单、产能扩张、长约、预付款？
8. 客户认证周期是否长？客户切换是否困难？
9. 是否存在海外龙头替代、国产替代失败、技术路线变化风险？
10. CapEx、存货、应收账款是否吞掉现金流？
11. 是否存在“大客户压价、收入增长但利润不增长”的风险？
12. 是否能用 3-5 个反证指标季度复核？
6.4 D4-D5 A 股公司处理

D4-D5 公司进入 A 股映射时，只能默认放入雷达池，除非满足至少两个条件：

1. 有原文证明其产品被 D1-D2 关键客户或 D2 瓶颈供应商使用；
2. 有长交期、产能锁定、订单、backlog、价格上涨或客户认证证据；
3. 有财务传导：相关 segment 收入、毛利率或 FCF 改善；
4. 有反向卡点证据：没有它，下游交付会延迟或成本显著上升；
5. 替代供应商少，扩产周期长。
6.5 避免 A 股概念先行的硬规则
红旗	处理
只在互动平台说“公司产品可用于 AI”	排除池或待核验
年报没有 AI/data center/HBM/高速网络/液冷相关产品或客户	排除池
收入来自消费电子、传统工业、汽车周期，却被包装成 AI	雷达或排除
只有国产替代叙事，没有客户和订单	雷达
收入增长但毛利率下降、应收/存货大增	降级
D4-D5 远端材料/能源公司没有反向卡点证据	雷达，不得核心
公司产品是通用件，无客户认证、无技术壁垒	排除或低分
7. 研究流程：实际执行顺序
7.1 总流程
Step 0：建立 D0 token demand / model roadmap 假设
Step 1：映射 D1 GPU / TPU / ASIC / cloud / software stack
Step 2：识别 D2 HBM / CoWoS / networking / power / cooling 直接瓶颈
Step 3：向 D3 拆解 equipment / test / substrate / optics / liquid cooling / power components
Step 4：D4-D5 深层雷达，只记录能反向卡住 D0-D2 的约束
Step 5：海外公司池评分
Step 6：A 股主板映射
Step 7：原始出处核验
Step 8：反证仪表盘与季度复盘
7.2 Step 0：从 D0 token demand / model roadmap 出发

目标不是预测某个 Lab 的胜负，而是提取需求向量。

需求向量	对下游影响
训练规模增加	GPU/TPU、HBM、CoWoS、networking、power
推理 token 增长	GPU/ASIC、HBM/DRAM、KV cache、networking、eSSD、power
long context	HBM、DRAM、KV cache、network、storage
MoE / agent loop	networking、scheduler、inference stack、memory
多模态 / video	storage、eSSD、network、GPU、power
inference cost pressure	ASIC、software stack、power efficiency、liquid cooling
sovereign AI	cloud/data center、电力、GPU supply、local suppliers
7.3 Step 1：D1 GPU / Cloud / TPU / Software Stack

要回答：

1. D0 Lab 依赖哪类 D1 平台？
2. 是 GPU、TPU、ASIC、云资源、CUDA/software stack，还是混合？
3. 哪些 D1 平台具备不可替代性？
4. D1 的真正约束是什么：芯片、HBM、CoWoS、网络、电力、机房、融资？

D1 不是终点，而是继续向上游 BFS 的入口。

7.4 Step 2：D2 直接瓶颈

重点看：

D2 瓶颈	关键问题
HBM	是否限制 GPU/ASIC 性能和出货；HBM3E/HBM4 路线；容量/带宽/stack 数
CoWoS / advanced packaging	是否限制 AI accelerator 交付；interposer、ABF、TCB、hybrid bonding
Leading-edge foundry	wafer allocation、yield、先进节点产能
Networking	800G/1.6T、InfiniBand/Ethernet、switch ASIC、NIC/DPU
Power / cooling	rack kW、液冷、UPS、PDU、transformer、grid interconnect
AI server / rack	是否只是低毛利集成，还是 rack-scale 交付瓶颈
7.5 Step 3：D3 二阶瓶颈供应商

优先找高弹性细分：

D3 方向	需要核验
HBM test / probe / socket	test time、tester orders、probe card ASP、客户认证
TCB / hybrid bonding / molding	HBM4、advanced packaging 设备需求
ABF / substrate / interposer	层数、尺寸、交期、客户锁产能
InP laser / SiPh / optics	800G/1.6T、CPO、external light source
Retimer / SerDes / CXL / scale-up	attach rate、lane count、platform design win
Liquid cooling components	CDU、cold plate、pump、valve、TIM、rack qualification
Power components	transformer、switchgear、UPS、PDU、48V、VRM、SiC/GaN

现有研究地图也把 HBM 设备/测试/材料、光互连、电力液冷、Custom ASIC/scale-up connectivity、非美隐形供应链列为第一优先级。

2026-05-12-ai-super-cycle-resea…

7.6 Step 4：D4-D5 深层雷达

D4-D5 不主动升级为核心。只记录：

1. 是否有直接 AI data center / HBM / CoWoS / optics / power 关联；
2. 是否有长交期或供应限制；
3. 是否有客户锁定或合同；
4. 是否有财务传导；
5. 是否能反向卡住 D0-D2。
7.7 Step 5：A 股映射
海外 D2/D3 瓶颈 → 中国同类产品/设备/材料/部件 → 主板公司 → 年报/公告核验 → BFS depth → 评分 → 结论池

重点不是找“AI 概念股”，而是找：

海外瓶颈在中国主板是否有可验证的供应商；
这些供应商是否有客户、收入、产能、毛利率和订单证据；
如果没有，只能进入雷达池或排除池。
7.8 Step 6：原始出处核验和反证仪表盘

每季度复核：

层级	复核指标
D0	token growth、model roadmap、API pricing、产品使用量
D1	GPU/TPU/ASIC 出货、云 CapEx、RPO、utilization、software efficiency
D2	HBM ASP/bit shipment、CoWoS capacity、network ports、power backlog、liquid cooling attach
D3	equipment orders、tester revenue、probe card ASP、substrate lead time、laser capacity
财务	revenue、gross margin、CapEx、inventory、FCF、ROIC
反证	CapEx 放缓、订单取消、价格下跌、技术替代、客户转单、库存上升
8. 可直接复制的 Markdown 产出格式
8.1 总图表格模板
Markdown
# AI Infra BFS 总图

| BFS Depth | 模块 | 从 D0 出发的 dependency path | 关键边类型 | 代表公司/节点 | 关键验证指标 | 证据状态 | 主要反证 | 结论池 |
|---|---|---|---|---|---|---|---|---|
| D0 | LLM Lab / token demand | OpenAI / Anthropic / Google / Meta / xAI | 需求源头 |  | token、模型路线、API usage、推理成本 | 待原文核验 | token 价格下降、企业 ROI 不足、小模型替代 | 需求源头 |
| D1 | GPU / TPU / ASIC / Cloud / Software | D0 → D1 | 客户边、技术边、现金流边 |  | CapEx、RPO、GPU fleet、utilization、software moat |  | ASIC 替代、GPU 供给过剩、云毛利承压 | 核心池 |
| D2 | HBM / CoWoS / Networking / Power / Cooling | D0 → D1 → D2 | BOM 边、产能边、技术边 |  | HBM revenue、CoWoS capacity、800G/1.6T ports、power backlog |  | 扩产过快、ASP 下行、路线替代 | 核心池 |
| D3 | Test / Equipment / Substrate / Optics / Components | D0 → D1 → D2 → D3 | 产能边、技术边、现金流边 |  | tester orders、probe card ASP、ABF lead time、laser yield |  | 客户内化、订单透支、技术切换 | 候选池 |
| D4 | Materials / Gas / Vacuum / Clean / Grid parts | D0 → D1 → D2 → D3 → D4 | 技术边、产能边、反证边 |  | material ASP、qualification、lead time、customer evidence |  | 泛材料叙事、收入分散、替代供应商 | 雷达池 |
| D5 | Energy / Regulation / Finance / Land / Permits | D0 → ... → D5 | 现金流边、监管边、反证边 |  | PPA、interconnect queue、cost of debt、permits |  | 周期太长、监管风险、客户需求波动 | 雷达池 |
| >D5 | 远端主题 | 路径过长或无法证明 | 弱线索 |  |  | 弱 | 无法反向卡住 D0-D2 | 排除池 |
8.2 公司卡片模板
Markdown
# [公司名] AI Infra BFS Company Card

## 0. 基本信息

| 字段 | 内容 |
|---|---|
| 公司 |  |
| Ticker / Exchange |  |
| 地区 |  |
| 板块限制 | A 股需标明：主板 / 科创板 / 创业板 / 北交所 |
| 覆盖状态 | 初筛 / 已建卡 / 已核验 / 季度复盘 / 排除 |
| 最近更新 |  |
| 不构成投资建议 | 是 |

## 1. 一句话研究假设

```text
如果 D0 LLM Lab 的 [训练/推理/token/多模态/agent] 需求持续增长，
那么 [公司] 会因为 [D几模块/瓶颈/技术壁垒/客户认证/产能约束]，
获得 [收入/毛利率/FCF/ROIC/长期瓶颈租金] 的改善。
2. BFS 定位
字段	内容
Primary BFS Depth	D0 / D1 / D2 / D3 / D4 / D5 / >D5
Effective BFS Depth
Dependency Path	D0 → D1 → D2 → D3 → 公司
处于该 depth 的理由
是否能反向卡住 D0-D2	是 / 否 / 待核验
反向卡点证据
升级条件
降级条件
3. 上下游与边关系
Edge Type	From	To	证据状态	Edge Strength	原始出处 / 证据卡	备注
客户边			原文已证明 / 合理推论 / 待核验 / 反证	strong / medium / weak / negative
BOM 边
产能边
技术边
现金流边
反证边
4. 原文已证明
指标 / 事实	原文来源	报告期	原文能证明什么	不能证明什么	口径备注

5. 合理推论
推论	依据	推论强度	需要补充的原文
		strong / medium / weak
6. 待原文核验
待核验事项	需要找的原文	优先级	若无法证明则如何处理
AI 相关收入	年报 / 季报 / earnings call / segment note	高	降级
客户关系	客户/供应商交叉披露 / 公告	高	降级
订单 / backlog	财报 / 公告 / call transcript	高	降级
毛利率传导	财报 / segment margin	高	降级
产能瓶颈	CapEx / lead time / book-to-bill	中	维持候选或雷达
7. 财务传导
指标	当前值	报告期	趋势	AI 相关性	证据状态	备注
Revenue
AI / data center revenue
Gross margin
Operating margin
CapEx
Inventory
Backlog / RPO / Orders
ASP / Shipment / Capacity
FCF
ROIC / payback
8. 主要反证
反证	触发条件	观察指标	原始出处	影响
客户 CapEx 放缓				降级 / 排除
毛利率不提升				降级
技术路线替代				降级 / 排除
订单取消或延迟				降级
库存 / 应收恶化				降级
9. 评分
维度	权重	得分	依据
BFS depth 与路径完整性	15
需求真实性	15
供给瓶颈强度	12
议价权 / 毛利率传导	10
财务传导到 FCF	12
技术替代风险	8
客户集中风险	6
证据质量	10
反证清晰度	5
10 倍弹性 / 指数增长可能性	7
总分	100
10. 结论
字段	内容
结论池	核心池 / 候选池 / 雷达池 / 排除池
结论标签	D1 龙头 / D2 直接瓶颈 / D3 高弹性瓶颈 / D4-D5 雷达约束 / 主题映射
结论状态	原文已证明 / 合理推论 / 待原文核验 / 被反证削弱
标准结论句
下一步核验

---

## 8.3 证据卡片模板

```markdown
# Evidence Card: [主题 / 公司 / 指标]

## 基本信息

| 字段 | 内容 |
|---|---|
| evidence_id |  |
| 研究主题 |  |
| 公司 / 证券代码 |  |
| BFS Depth |  |
| 产业链位置 |  |
| 报告期 |  |
| 原始来源类型 | 年报 / 季报 / earnings release / earnings call / investor presentation / 监管公告 / 技术资料 / 客户披露 / 供应商披露 |
| 发布日期 |  |
| 原始来源链接 |  |
| 证据等级 | E0 / E1 / E2 / E3 / E4 |

## 原文证据

| 指标 | 原文位置 | 原文能证明什么 | 不能证明什么 | 口径备注 |
|---|---|---|---|---|
| Revenue / segment revenue |  |  |  | GAAP/non-GAAP、币种、期间 |
| Gross margin / operating margin |  |  |  | 产品 mix、一次性项目 |
| CapEx / inventory / FCF |  |  |  | 现金流与扩产周期 |
| Backlog / RPO / orders |  |  |  | 是否可取消、是否已进收入 |
| ASP / shipment / capacity |  |  |  | 单位、同比/环比 |
| Customer / product evidence |  |  |  | 客户是否直接披露 |

## 结论分层

| 层级 | 内容 |
|---|---|
| 原文已证明 |  |
| 合理推论 |  |
| 待原文核验 |  |
| 主要反证 |  |

## 影响的 BFS 边

| Edge ID | Edge Type | From | To | 影响 |
|---|---|---|---|---|
|  | 客户边 / BOM 边 / 产能边 / 技术边 / 现金流边 / 反证边 |  |  | strengthen / weaken / invalidate |

## 下一步核验

1.
2.
3.
8.4 A 股映射表模板
Markdown
# A 股主板映射表：从海外 D1-D3 瓶颈出发

| 海外瓶颈 | BFS Depth | 具体产品/技术 | 海外代表节点 | 中国主板候选公司 | Ticker | 是否主板 | 对应产品 | 客户证据 | 收入证据 | 毛利/FCF 证据 | 证据状态 | 结论池 | 主要反证 | 下一步核验 |
|---|---:|---|---|---|---|---|---|---|---|---|---|---|---|---|
| HBM test | D3 | memory tester / probe card / socket |  |  |  | 是 / 否 |  | 原文已证明 / 待核验 |  |  |  | 候选 / 雷达 / 排除 |  |  |
| CoWoS / advanced packaging | D2-D3 | TCB / hybrid bonding / substrate / underfill |  |  |  |  |  |  |  |  |  |  |  |  |
| 800G / 1.6T optics | D2-D3 | optical module / laser / DSP / SiPh |  |  |  |  |  |  |  |  |  |  |  |  |
| Liquid cooling | D2-D3 | CDU / cold plate / pump / valve / TIM |  |  |  |  |  |  |  |  |  |  |  |  |
| Power equipment | D2-D3 | transformer / UPS / PDU / switchgear |  |  |  |  |  |  |  |  |  |  |  |  |
| Materials / gas / chemical | D4 | ABF / gas / slurry / precursor / UPW |  |  |  |  |  |  |  |  |  | 雷达 |  |  |
8.5 季度复盘表模板
Markdown
# AI Infra BFS 季度复盘表

| 日期 | 公司/模块 | BFS Depth | 上季度结论池 | 本季度结论池 | 分数变化 | 新增原文证据 | 新增反证 | 财务传导变化 | 边关系变化 | 升级/降级理由 | 下一季度核验 |
|---|---|---:|---|---|---:|---|---|---|---|---|---|
|  |  | D1/D2/D3/D4/D5 | 核心/候选/雷达/排除 |  |  |  |  | Revenue / GM / CapEx / FCF | 客户/BOM/产能/技术/现金流/反证 |  |  |
反证仪表盘模板
Markdown
# AI Infra 反证仪表盘

| 层级 | 反证问题 | 指标 | 原始出处 | 绿灯 | 黄灯 | 红灯 | 当前状态 | 影响范围 |
|---|---|---|---|---|---|---|---|---|
| D0 | token demand 是否低于预期 | token volume / API usage / pricing | Lab / cloud 原文 |  |  |  |  | D1-D3 |
| D1 | GPU/TPU/ASIC 是否供过于求 | GPU supply、cloud utilization、RPO conversion | 公司财报 |  |  |  |  | D2-D3 |
| D2 | HBM 是否供给过剩 | HBM ASP、bit shipment、inventory | memory vendor 原文 |  |  |  |  | HBM 链 |
| D2 | CoWoS 瓶颈是否缓解 | capacity、lead time、packaging revenue | foundry / OSAT 原文 |  |  |  |  | packaging 链 |
| D2 | 网络升级是否放缓 | 800G/1.6T shipment、ASP | optics / switch 公司原文 |  |  |  |  | optics 链 |
| D2-D3 | 电力/液冷订单是否兑现 | backlog、book-to-bill、gross margin | power/cooling 公司原文 |  |  |  |  | power/cooling |
| D3 | 测试/设备订单是否透支 | tester orders、book-to-bill、revenue | equipment 公司原文 |  |  |  |  | testing/equipment |
| 财务 | 收入是否转化为 FCF | FCF、CapEx、inventory、AR | 公司财报 |  |  |  |  | 全部 |
8.6 排除池记录模板
Markdown
# 排除池记录

| 公司/模块 | 原 BFS Depth | 排除日期 | 排除原因 | 缺失的关键边 | 主要反证 | 重新进入条件 | 备注 |
|---|---:|---|---|---|---|---|---|
|  | D4 / D5 / >D5 |  | 仅主题映射 / 无客户证据 / 无财务传导 / 技术路线被替代 / 毛利率不传导 | 客户边 / BOM 边 / 产能边 / 技术边 / 现金流边 |  | 出现 E0/E1 原文证据 |  |
9. 最终落地规则：一张卡片只能有一种主结论

每个 company/module card 最后必须落到以下格式：

结论池：核心池 / 候选池 / 雷达池 / 排除池
BFS depth：D0 / D1 / D2 / D3 / D4 / D5 / >D5
结论标签：D1 龙头 / D2 直接瓶颈 / D3 高弹性瓶颈 / D4-D5 雷达约束 / 主题映射
结论状态：原文已证明 / 合理推论 / 待原文核验 / 被反证削弱
最高证据等级：E0 / E1 / E2 / E3 / E4
分数：__/100
星级：_★
一句话结论：
主要支持证据：
主要反证：
下一步核验：

最重要的硬门槛：

没有 BFS depth，不写公司结论。
没有 edge evidence，不写核心结论。
没有原始出处，不写“原文已证明”。
没有毛利率/FCF 传导，不把收入增长等同于利润池捕获。
没有反证，不进入核心池。
D4-D5 没有反向卡住 D0-D2 的证据，只能做雷达。
超过 D5，默认排除。

这套模型的实际用途，是把 AI Infra 研究从“产业链分层图”升级为“可证伪的 dependency graph”。它不会给买卖建议，也不生成目标价；它只回答：这个公司/模块离 LLM Lab 的真实需求有多近，依赖边是否被原文证明，瓶颈是否能形成利润池，反证是否足够清晰。
