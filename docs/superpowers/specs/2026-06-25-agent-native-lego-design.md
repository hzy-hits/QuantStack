# Agent-Native Quant-Stack — "Lego" Design Spec

Date: 2026-06-25 · 状态:待评审 · 作者:operator + Claude

## 目标

把 quant-stack 的 LLM 层从**"把 agent 当 LLM API 用"**(固定 6+1 / 4+1 extractor 静态扇出)重构成**"agent 当 agent 用"**:一组确定性**积木(brick=工具)**,一个**lead agent** 按当天事实**动态组合 subagent** 去搭,沿一条**主线 playbook** 走、按需偏离。对内取代静态 narrator,对外让整套系统**作为一个 subagent** 被父 agent(Hermes / Telegram 里的你)委派。

## 范围

**做**:积木层(MCP 工具)、subagent 目录、spine playbook、护栏、trajectory 审计、分阶段落地(shadow→promote)。
**不做(明确排除)**:重建任何确定性 compute(分析模块/证据门/regime/main_strategy_v2 原样保留,只被包成 brick);改 portability(那是前置 spec,见 `2026-06-24-portability-refactor-design.md`);factor-lab(已按决策 A 移除,不在积木内)。

**前置依赖**:portability 落地(数据/部署可移植)。本 spec 的 MCP server 跑在迁移后的 compute 主机上。

## 哲学(修订铁律)

> agent 自主**编排和判断**(调哪些积木、派哪些 subagent、什么值得说);但**任何数字只来自 brick 的真实执行**,绝不脑补;**每次派发与工具调用进 trajectory**,可回放;**证据门 / money gate 长在 brick 里、不在 agent 手里**,绕不过;最终报告仍过**确定性结构校验**。

这条同时解放 agency 又保住原系统四样:确定性数字、可审计、反捏造、成本封顶。

## 架构

```
┌ 底板 baseplate(冻结事实,只读)────────────────────────────────┐
│  DuckDB canonical + reports/review_dashboard/<component>/<date>/ │
└─────────────────────────────────────────────────────────────────┘
        ▲ 只读
┌ 积木 bricks(确定性工具,返真值,§Brick 清单)──────────────────┐
│  query bricks(读已算好的产物) + trigger bricks(包 run_task)   │
└─────────────────────────────────────────────────────────────────┘
        ▲ 被调用
┌ subagent(按需派发的专才,§Subagent 目录)─────────────────────┐
│  options-anomaly 调查 / 证据复核 / regime 解释 / 单票深挖 / 跨日 diff │
└─────────────────────────────────────────────────────────────────┘
        ▲ 派发 + 合成
┌ lead agent + 主线 playbook(§Spine)───────────────────────────┐
│  看事实 → triage → 动态派 subagent → 合成 → 结构校验 → 交付     │
└─────────────────────────────────────────────────────────────────┘

两层用途:
  对内 = lead 在积木上组合 subagent 产出每日决策报告(取代静态 extractor)
  对外 = 整个 MCP server 作为一个 subagent,被 Hermes / Telegram 委派
```

## Brick 清单(确定性工具,源自 2026-06-24 workflow 综述 §4)

所有 query brick **只读** DuckDB / 优先读冻结 JSON(零延迟、免锁)。trigger brick 包 `ops/run_task.sh`,处理 exit-75(running/blocked)。

### 读 brick — Universe / 证据
| brick | 入参 | 返回 | 背靠 |
|---|---|---|---|
| `get_production_basket` | market?, date? | 过证据门的可交易名单 | `ai_infra/reports/production_universe_v1.jsonl` |
| `get_evidence_state` | symbol | evidence_state 头 token + is_production_grade | `ai_infra_universe.py` |
| `list_pending_verification` | market?, limit? | 待原文核验名单 | `global_universe_v2.jsonl` |
| `get_membership_history` | symbol | PIT 进出账本 | `universe_membership_history.jsonl` |

### 读 brick — Regime
| `get_us_regime` / `get_cn_regime` | date? | 5 态 + R 乘数 + signals | `risk_regime|cn_risk_regime/<date>/*.json` |
| `get_fear_greed` | date? | score/rating/components | `fear_greed/<date>/fear_greed.json` |

### 读 brick — 期权 / 对冲雷达
| `get_options_anomaly_radar` / `get_options_tenor_signals` | date? | 异常/多 tenor 信号 | `us_options_*radar/<date>/*` |
| `get_bubble_hedge_state` | date? | wedge/victim/confirm | `bubble_hedge_radar/<date>/bubble_hedge.json` |
| `get_victim_puts` | date?, symbol? | OTM put 合约(delta/DTE/cost%) | `bubble_hedge_radar/<date>/victim_puts.json` |
| `get_capitulation_state` / `get_convex_longs` | date? | 抄底 5 信号 / 凸性 long | `capitulation_radar/<date>/*` |
| `get_gamma_spring` | date?, symbols? | GEX/dealer walls | `main_strategy_v2/<date>/gamma_spring.json` |

### 读 brick — 主策略 / 报告 / 数据
| `get_today_report` | date? | production_decision_summary | `main_strategy_v2/<date>/main_strategy_v2_backtest.json` |
| `get_us_ranker_row` / `get_cn_ranker_row` | symbol, date? | rank/score/action/size_r/evidence | `*_opportunity_ranker.json` |
| `get_portfolio_risk_overlay` | date? | base_r/final_r/hedge/net_beta | `portfolio_risk_overlay.json` |
| `get_symbol_analytics` | symbol, date? | momentum/earnings/options/kalman join | `quant.duckdb` 多表 |
| `get_task_status` / `get_task_log` | task_id | last_success/failure + 日志尾 | `ops/state/*.json`, `ops/logs/*` |

### trigger brick(写,需确认门 + `QUANT_DELIVERY_MODE=test` 防误发)
`trigger_task(task_id, date?)`、`trigger_us_daily(date, session, dry_run)`、`trigger_cn_daily(date?, session?)`、`trigger_main_strategy(date?)`、`trigger_narrator(market, date)`、`trigger_radar(name, date)`。

## Subagent 目录(按需派发,**不是固定角色**)

| subagent | 何时派 | 用哪些 brick | 产出 |
|---|---|---|---|
| `regime-explainer` | regime 切换 / R 变动 | get_*_regime, get_fear_greed | regime 决策叙述 + 仓位含义 |
| `basket-evidence-auditor` | 有名进/出篮子 or 证据降级 | get_production_basket, get_evidence_state, get_membership_history | 篮子变动 + 证据理由 |
| `options-anomaly-investigator` | options/tenor radar 触发 | get_options_*, get_gamma_spring, get_symbol_analytics | 异常票深挖 |
| `hedge-strategist` | bubble_hedge=Wedge/Victim/Press | get_bubble_hedge_state, get_victim_puts | 对冲建议(读 brick 给的真实合约) |
| `capitulation-strategist` | capitulation ≥3/5 | get_capitulation_state, get_convex_longs | 凸性翻多建议 |
| `single-name-deep-dive` | 异动/被点名标的子集 | get_symbol_analytics, get_*_ranker_row | 单票多维解读 |
| `cross-day-diff` | 每日 or 被问"变了啥" | get_today_report(today vs prev) | 与昨日的实质差异 |
| `completeness-critic` | 合成前最后一步 | 全部 | "漏了什么模态/未验证的声明" |

lead 按当天 triage 结果**决定 spawn 哪几个、几个**(平淡日少、事件日多),受预算上限约束。

## Spine playbook(主线 + 偏离规则)

**默认每日流程**:
1. **载事实**:`get_us_regime`+`get_cn_regime`+`get_production_basket`+`get_today_report`(全 brick,无 LLM)。
2. **Triage**:找出今日"值得说"的子集——异动票、触发的 radar、证据变动、R 变化。
3. **动态派发**:对被标记子集派对应 subagent(数量随事件量伸缩)。
4. **合成**:lead agent 把 subagent 结论 + 事实组成决策报告(数字全来自 brick)。
5. **结构校验**(确定性):章节/表数/无捏造 ticker/数据血缘——沿用 US narrator 现有 3 轮修复回路。
6. **交付 + 留痕**:多 sink 投递(邮箱主 + personal-agent 推送,见 §交付/触发/Sink)+ 写 trajectory。

**偏离规则(agency 所在)**:
- capitulation 触发 → 额外 spawn `capitulation-strategist`;
- 有证据降级 → spawn `basket-evidence-auditor`;
- bubble_hedge=Press → spawn `hedge-strategist` 并在报告置顶;
- 平淡日(无 radar 触发、无篮子变动)→ 砍掉多数 subagent,产出精简版。

## 交付 / 触发 / Sink(可靠性边界)

**两个触发口,同一套机器:**
- **scheduled(关键路径)**:cron(`us.postmarket` / `cn.morning` / `cn.evening` …)内部跑 lead-agent + spine,产出日报。**自给自足,不依赖任何外部 agent。**
- **on-demand(交互)**:父 agent / Telegram 即兴调 brick 或整套("现在跑一版 / NVDA 为啥在篮子里")。

**多 sink 投递(同一份报告 fan-out):**
- **email(确定性,主 sink)**:现有 Gmail 投递(`send_production_decision_report.py`),关键路径,必达。
- **personal-agent push(增强 sink)**:Hermes → Telegram 推送同一份报告 + 失败告警。

**可靠性边界(硬约束):**
- 日报的**生成与投递不依赖个人 agent 可用性**——Hermes 宕机,邮箱照常收到日报。
- personal agent = **消费者 + 交互前端**,**绝不在每日关键投递路径上**。
- "对外作为 subagent" 服务的是**交互 / 即兴**,不是每日关键投递。
- 两个 sink **相互独立、各自重试**:Telegram 不可达不得阻塞或拖垮 email;反之亦然。

## 护栏

- **数字只来自 brick**:agent 无算术工具;所有数值经 brick 真实执行返回。
- **Trajectory(DuckDB 表 `agent_trajectory`)**:`(date, market, run_id, step_idx, agent, tool, args_json, result_digest, rationale, ts)`——可回放"它当时为什么这么判断"。
- **硬预算**:每市场每次 `max_subagents`、`max_tool_calls`、`wall_clock`、token/$ 上限;超了中止 + 告警(防跑飞/烧订阅)。
- **门长在 brick 里**:`is_production_grade` / money gate 在 server 端强制,agent 拿到的就是已过门的数据,**绕不过**。
- **结构校验保留**:最终报告过确定性 validator(交付契约),CN 侧补齐 US 已有的校验(修现有不对称)。
- **代码解释器沙箱**(若启用临场计算):低权限用户、只读 DuckDB、网络白名单、CPU/内存/超时、工作目录即弃。

## 运行时 / 后端

- **父 agent**:Hermes(主干 + 记忆 + cron + Telegram)委派 MCP server;或直接用 Codex/Claude SDK 跑 lead+subagent。可切换。
- **后端路由**(沿用现状 + 早前决策):lead/subagent 重推理 → Codex 订阅(主)→ Claude 订阅(回退);高频抽取 → DeepSeek API。
- **诚实风险**:订阅跑无人值守 cron agent 有 ToS/限速尾部风险;回退链 + DeepSeek 分流缓解。

## 迁移 / 适配

- MCP server 路径从 `QUANT_STACK_ROOT` 解析(reports/DB 都是 runtime、gitignored)。
- 读 brick 一律 `read_only=True`;CN 读 `quant_cn_report.duckdb`(非写中 research 副本);必要时复制临时快照再读。
- 尊重两份契约(`AI_SUPERCYCLE_PIPELINE_CONTRACT` / `REPORT_DELIVERY_CONTRACT`)+ 证据门语义(头 token 权威)+ 永不外发 `*_backtest.md`。

## 分阶段落地(shadow → promote,先一个市场)

- **Phase 1 — 积木层(零风险)**:把 §Brick 清单的**读 brick** 实现为一个 MCP server(纯读)。立刻可用:Telegram/CLI 即兴查询。不碰每日报告。
- **Phase 2 — lead + spine(shadow)**:实现 lead agent + spine playbook + 2–3 个 subagent,**US 市场、影子模式**——和现有 narrator **并行**产出,只 diff 不发。
- **Phase 3 — 审计 + 护栏达标**:trajectory 落表 + 预算上限 + 结构校验对齐;US 影子质量追平现 narrator 后,**promote 为主**,静态 extractor 留作 fallback。
- **Phase 4 — trigger + 对外 subagent**:加 trigger brick(确认门)+ Hermes/Telegram 交互("今天 A 股怎样 / NVDA 为啥在篮子里")。
- **Phase 5 — CN 对齐 + 退役静态 extractor**:CN 走同一套;补 CN 结构校验;退役 4+1 静态扇出。

每阶段验证:Phase 1 brick 返值与底层产物逐字段一致;Phase 2 影子报告与现报告 diff 人工评审;Phase 3 trajectory 可回放 + 成本在预算内 + validator 通过;promote 前连续 N 天质量不劣于现 narrator。

## 风险

- **质量回归**:动态组合可能漏掉静态版稳定覆盖的内容 → 影子并行 + `completeness-critic` + N 天对比兜底。
- **成本漂移**:subagent 派发失控 → 硬预算 + triage 收敛。
- **可审计弱化**:自由编排难追 → trajectory 全留痕(已选"可审计不强求逐字节")。
- **CN 校验缺口**:现状 CN narrator 无结构校验 → Phase 5 补齐前不 promote CN。

## 待评审后确认的开放问题

1. 父运行时:Hermes 当主干,还是 Codex/Claude SDK 直接跑 lead?
2. 一个 MCP server 还是按域多个?
3. subagent 派发机制:SDK subagent / Workflow 扇出 / `codex exec`?
4. 静态 extractor 是 promote 后保留为 fallback,还是直接退役?
5. CN 结构校验补齐排在 Phase 3 还是 Phase 5?

## 自查

- 范围聚焦(只做编排层,不重建 compute);与 portability spec 不重叠(那个管数据/部署,这个管 LLM 编排)。
- brick/subagent/trigger 命名前后一致;背靠产物均引自真实路径(workflow 综述 §2/§4)。
- 护栏覆盖原系统四大保证;factor-lab 已排除(决策 A)。
