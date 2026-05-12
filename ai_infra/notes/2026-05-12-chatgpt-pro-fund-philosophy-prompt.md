# ChatGPT Pro Prompt: AI Infra BFS Fund Philosophy Review

状态：submitted to ChatGPT Pro, pending output  
用途：让 ChatGPT Pro 从基金管理、研究系统、GitHub 仓库化和可迁移性角度批判我们的框架。

ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a0304d9-5d04-83ea-9656-6c26dbbf1e38

提交时间：2026-05-12

```text
我们正在建立一个名为 `ai-super-cycle` 的 AI Infra 主题基金研究系统。请从投资研究方法论、基金工程、风险控制和 GitHub 仓库化角度，批判并改进下面这套框架。

边界：
- 这不是投资建议，不需要买卖建议、目标价或实际仓位。
- 请不要泛泛讨论 AI 概念股。
- 请把重点放在：框架是否自洽、是否可证伪、是否适合长期复用、是否适合 GitHub 仓库化、是否能在新电脑恢复。

我们的核心哲学：

1. 研究起点不是股票故事，而是 D0 LLM 需求源头：
   OpenAI、Anthropic、Google DeepMind / Gemini 等。

2. 用 LLM Dependency BFS 控制研究深度：
   - D0：LLM 核心需求源头；
   - D1：模型强依赖一阶，如 GPU/CUDA、TPU、custom ASIC、cloud、training/inference stack；
   - D2：一阶依赖直接瓶颈，如 HBM、CoWoS、TSMC、AI server/rack、networking、800G/1.6T optics、data center power/cooling；
   - D3：二阶瓶颈供应商，如 HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件；
   - D4-D5：材料、气体、化学品、真空、洁净、能源、电网、融资、监管。默认只做雷达，除非能证明反向卡住 D0-D2。

3. 主战场是 D1-D3，尤其 D2-D3 中被 LLM token demand 和 AI cluster expansion 放大、但供给侧扩产慢、客户认证强、单位价值量上升的公司。

4. 任何公司结论必须回到原始来源：
   10-K、20-F、10-Q、年报、季报、earnings release、earnings call、investor presentation、监管文件、公司官网技术资料、客户或上下游交叉披露。ChatGPT Pro 和媒体只能作为线索。

5. 每家公司必须建 evidence card，字段包括：
   BFS depth、dependency edge、source links、revenue、segment revenue、orders/backlog、capacity、gross margin、FCF、inventory、customer concentration、technology roadmap、原文已证明、合理推论、待原文核验、主要反证。

6. 组合结构设想：
   - Core beta：SMH / SOXX / QQQ / AIQ 等；
   - Alpha basket：D2-D3 小中盘候选；
   - China sleeve：A 股主板 + 港股映射；
   - Satellite markets：日韩台欧以色列隐形供应链；
   - Credit / power radar：融资和电力交付反证层；
   - Cash / T-bills：控制回撤和等待事件。

7. 反证层：
   - Credit/CDS：RPO/backlog 是否转现金流，还是被 debt、lease、interest、depreciation、customer concentration 吞掉；
   - Nuclear/Firm Power/Grid：AI data center 是否真的能拿到 MW、PPA、interconnect、regulatory milestone、fuel path 和 financing path。

8. 本地工程已经有：
   - `data/global_universe_v2.jsonl`
   - `data/ai_infra_universe.sqlite`
   - `scripts/build_universe_system.py`
   - `scripts/generate_source_verification_queue.py`
   - `scripts/generate_us_alpha_mining_queue.py`
   - `reports/ai_infra_universe_dashboard_v1.md`
   - `reports/us_alpha_mining_queue_v1.md`
   - `evidence/batch1`
   - `evidence/us_alpha`
   - `docs/llm-dependency-bfs-framework.md`
   - `docs/research-checklist.md`

请输出：

1. 这套框架本质上是什么？用 3-5 个准确的概念命名。
2. 它和传统主题投资、PE/估值驱动、动量交易、宏观资产配置的区别是什么？
3. 它最大的优点和最大的盲区是什么？
4. 如果要把它变成 GitHub repo，推荐的目录结构、README 结构、分支策略、数据安全规则是什么？
5. 哪些内容适合 public，哪些必须 private？
6. 新电脑 clone 后，最小恢复命令应该是什么？
7. 未来要变成真正 paper portfolio / fund engine，还缺哪些模块？
8. 请给出一版更好的 repo tagline、README opening 和 research principles。

不要给股票买卖建议，不要给目标价。
```
