# ChatGPT Pro 提问稿 v2：AI Infra 全球候选 Universe 深度研究

日期：2026-05-12  
用途：让 ChatGPT Pro 基于本项目已有研究文档和 D0-D5 BFS 建模，生成全球 AI Infra 可交易候选池，为后续自建主题基金做 universe。

## 核心修正

- 我们的角色是假设自己当基金经理，构建一个可跟踪、可回测、可组合的 AI Infra 主题基金 universe。
- A 股和港股视为同一中国资产池，和美股/美股中小盘/ETF 一起构成主战场。
- 日韩欧以色列中小盘作为卫星池，用来找 D3-D4 隐形供应链和早期发现机会。
- 需求源头从全球 top LLM Lab 出发，但优先看前三条链：
  - OpenAI；
  - Anthropic；
  - Google DeepMind / Gemini；
  - Kimi / DeepSeek 作为中国 / 开源 / 国产链补充，先不作为主线权重最高的需求源。

## 完整提问词

会话标题：AI Infra 全球可交易候选池深度研究 v2

我正在做 AI super cycle / AI Infra 产业链研究，目标是后续构建一个类似“自建 AI Infra 主题基金”的研究 universe。现在不是投资建议，也不要给买卖建议或目标价。

请基于 D0-D5 LLM Dependency BFS 框架，做一个全球可交易候选池深度研究。

重要定位：

1. 我们的角色是假设自己当基金经理，目标是建立可跟踪、可回测、可组合、可风控的 AI Infra 主题基金 universe。
2. A 股和港股视为同一个中国资产池。
3. 美股、美股中小盘、美国 ETF 是另一大主战场。
4. 中国资产池 + 美国资产池是组合大头。
5. 日本、韩国、欧洲、以色列中小盘是卫星池，重点用于发现 D3-D4 隐形供应链，不要喧宾夺主。
6. 出发点是全球 top LLM Lab 的产业链，但优先看 OpenAI、Anthropic、Google DeepMind / Gemini 三条链。Kimi、DeepSeek 可以作为中国 / 开源 / 国产链补充，先随缘，不要让它们压过前三条主线。

核心框架：

D0：LLM 核心源头
- 主线：OpenAI、Anthropic、Google DeepMind / Gemini。
- 补充：Meta、xAI、Kimi、DeepSeek、Mistral 等。
- D0 定义模型能力、token demand、训练节奏、推理规模和 AI 产品形态。

D1：模型强依赖一阶
- NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
- LLM Lab 训练、推理和产品化不可绕开的直接依赖。

D2：一阶依赖的直接瓶颈
- HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。

D3：二阶瓶颈供应商
- HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件。

D4-D5：深层雷达
- 材料、气体、化学品、真空、洁净、能源、电网、融资、监管。
- 只做雷达，除非能证明反向卡住 D0-D2，否则不升级为核心。

研究重点：
- 主看 D1-D3。
- 最深看到 D5。
- D4-D5 默认只做雷达。
- 超过 D5 默认排除。

覆盖市场和权重思路：

1. 中国资产池：A 股主板 + 港股。
   - A 股主板排除创业板 300/301、科创板 688/689、北交所；
   - 港股纳入 H 股、红筹、港股科技/半导体/电力/IDC 相关公司；
   - 中国资产池重点看服务器、PCB/CCL、光互连、电力液冷、封测、设备材料、IDC/算力、国产替代中真正能连接 D1-D3 的公司。

2. 美国资产池：美股大型股、美股中小盘、ADR、ETF。
   - 大型股作为 core beta：GPU、cloud、semiconductor、AI infra platforms；
   - 中小盘寻找 D2-D3 alpha：optics、CPO、networking、power、cooling、testing、materials、NeoCloud；
   - ETF 用于覆盖 beta 和验证 smart money / passive flow。

3. 卫星资产池：日本、韩国、台湾、欧洲、以色列。
   - 重点看 D3-D4 隐形供应链；
   - 包括 HBM/CoWoS 设备、测试、基板、材料、光子、InP、SiPh、真空、洁净、电力液冷；
   - 标注 IBKR 交易可达性、ADR/本地交易、流动性、币种和监管风险，后续需要核验。

请特别关注：
- 100B 美元市值以下的中小盘；
- 不是 AI 公司、但可能被 AI 规格升级拉动的隐形供应链；
- HBM / CoWoS / testing / substrate / optics / CPO / silicon photonics / power / cooling / grid / NeoCloud / storage supercycle；
- 是否被 ETF 覆盖，尤其是 AI、semiconductor、robotics、smart grid、infrastructure、nuclear/uranium、Japan/Korea/Taiwan/Europe regional ETF；
- 是否可能被 13F / 主动基金 / 主题 ETF / smart money 逐步发现。

严格要求：

1. 不给买卖建议；
2. 不给目标价；
3. 不要从股票热门概念出发，要从 D0-D5 BFS dependency path 出发；
4. 每家公司必须标注 BFS depth；
5. 每家公司必须标注 dependency edge：客户边 / BOM 边 / 产能边 / 技术边 / 现金流边 / 反证边；
6. 每家公司必须标注证据状态：原文已证明 / 合理推论 / 待原文核验 / 主要反证；
7. 所有收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线，都必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游交叉披露核验。ChatGPT 输出只能作为线索；
8. 对 A 股必须排除创业板、科创板、北交所；
9. 对港股要标注是否与 A 股 / 美股有重复上市或同一集团关系；
10. 对欧日韩台以色列小盘要标注流动性、交易可达性、ADR/本地市场、IBKR 是否通常可交易这一类后续需要核验的问题；
11. 对每个市场都要分核心池 / 候选池 / 雷达池 / 排除池。

请输出：

1. 全球 AI Infra BFS universe 总表
   字段包括：
   - 市场 / 国家；
   - ticker；
   - 公司；
   - 市值区间；
   - BFS depth；
   - 对应模块；
   - dependency path；
   - dependency edge；
   - 对应海外瓶颈；
   - 对应上下游公司；
   - 证据状态；
   - ETF 覆盖线索；
   - smart money / 13F / 主动基金线索；
   - 主要反证；
   - 当前分池。

2. 按资产池分层的候选池
   - 中国资产池：A 股主板 + 港股；
   - 美国资产池：美股大盘 + 美股中小盘 + ETF；
   - 卫星资产池：日本、韩国、台湾、欧洲、以色列。

3. 按 BFS depth 分层的全球公司池
   - D1 龙头；
   - D2 直接瓶颈；
   - D3 高弹性候选；
   - D4-D5 雷达；
   - 排除 / 只做观察。

4. 100B 美元市值以下候选池
   - 哪些可能是真正的 D2-D3 高弹性；
   - 哪些只是主题映射；
   - 哪些需要先用原文核验；
   - 哪些可能被 ETF / 主动资金逐步发现。

5. ETF 覆盖研究
   - 哪些 ETF 可能覆盖这些公司；
   - ETF 类型：AI / semiconductor / robotics / smart grid / infrastructure / nuclear / regional；
   - 如何下载 holdings；
   - 如何计算 ETF coverage score；
   - 哪些公司可能被多个主题 ETF 共同覆盖；
   - 哪些 ETF 更适合做 core beta，哪些只适合作为信息源。

6. smart money 跟踪框架
   - 13F；
   - 13D / 13G；
   - N-PORT；
   - ETF flows；
   - options OI / IV / skew；
   - insider / buyback / strategic investment；
   - 哪些只适合做线索，不能当事实。

7. 组合构建初步框架
   - 如何把这个 universe 变成 paper portfolio；
   - core beta / satellite alpha / option overlay / cash；
   - 中国资产池和美国资产池如何作为大头；
   - 日韩欧以色列中小盘如何作为卫星；
   - 如何控制单股、单模块、单市场、单因子、单主题风险；
   - 如何估算 beta vs SPY / QQQ / SMH / AI Infra custom basket；
   - 如何用 Greeks 管理期权暴露；
   - 不要给具体买入建议，只给方法和模板。

8. 下一步原文核验清单
   - 每个模块先核验哪些公司；
   - 每个公司先找哪些原文；
   - 哪些指标决定升级 / 降级 / 排除。

请输出尽量表格化，便于我保存为 markdown 和后续写脚本抓数据。
