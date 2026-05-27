# Phase D — US Agent + Narrator Architecture

**目标**: US daily report 走 `program → payload → 4 extractor + 1 narrator → final report`,对齐 CN 端架构(`quant-research-cn/prompts/`)。

**当前状态**: US daily 100% 由 `scripts/generate_main_strategy_v2_report.py` 硬拼 markdown。CN 已有 5 prompt 跑 DeepSeek 走 agent-narrator。

**承诺**: 不破坏现有 daily 输出。新 agent narrator 是**新增层**,可通过 flag 切换。programmatic render 保留为 fallback。

---

## 架构对照

```
当前 (US):
  build_payload(data) → render_us_standalone_report(payload) → us_daily_report.md
                                                                    ↑ 硬编码叙事

CN 现有 + Phase D 目标 (US):
  build_payload(data) → payload.{json,md}     ← Step 1: 数据层
       ↓
  ┌─ macro-analyst (DeepSeek)    ── 提取 MRS/regime/fear-greed/SPX-P/C
  ├─ event-analyst (DeepSeek)    ── 提取 earnings/Serenity flips/NVDA invests
  ├─ quant-analyst (DeepSeek)    ── 提取 ranker top/IV view/概率最优
  └─ risk-analyst (DeepSeek)     ── 提取 headline_risk/options anomaly/gate
       ↓
  merge-agent / narrator (DeepSeek) ── 4 extractor + payload digest → 完整中文报告
       ↓
  us_daily_report.md  ← LLM 写的人话,不再是程序拼的模板
```

---

## D.0 — 5 个 prompt 文件

| 文件 | 角色 | 输入 | 输出 |
|---|---|---|---|
| `quant-research-v1/prompts/us-macro-analyst.md` | 提取 macro/regime/sentiment 信号 | payload digest 的 risk_regime + fear_greed + MRS + bubble_hedge | 固定格式表格 + 3 句判断 |
| `quant-research-v1/prompts/us-event-analyst.md` | 提取 events/news/Serenity | earnings_calendar + serenity_crosscheck + nvda_investments + news_scored | 固定格式 + 3 句 |
| `quant-research-v1/prompts/us-quant-analyst.md` | 提取 production picks 信号 | us_opportunity_ranker + options_verdicts + tenor_signals + probability_picks | 固定格式 + 3 句 |
| `quant-research-v1/prompts/us-risk-analyst.md` | 提取 risk/gate 信号 | options_anomaly + headline_risk + execution_gate + portfolio_risk_overlay + left_side | 固定格式 + 3 句 |
| `quant-research-v1/prompts/us-merge-agent.md` | 唯一允许"形成观点 + 写叙事" | 4 提取器 + payload digest | 完整美股日报(1500-2500 字) |

**约束(参考 quant-research-cn 范式)**:
- 4 个 extractor: **"只提取,不叙事"** 硬约束 (`不做叙事`, `不给交易建议`, `禁用词列表`)
- merge-agent: **唯一**有权写观点、做方向裁决,但**不能升级**交易权限(rank/tier 必须从 payload 来)
- 所有数字必须来自 payload,LLM 不可自己算
- 输出统一中文(参照 CN 风格)

## D.1 — Payload emitter

- 在 `scripts/generate_main_strategy_v2_report.py` 加 `--emit-payload-only` flag
- 输出:
  - `reports/.../us_payload.json`: 完整结构化数据
  - `reports/.../us_payload.md`: 数据型 markdown(只有表+数字,无叙事文本)
- 这个 mode 跳过所有 `render_*_section` 文本,只输出 payload digest

## D.2 — Agent 调度器

新文件: `scripts/agents/run_us_narrator.py`
- 复用 `run_us_headline_agent.py` 的 DeepSeek client 模式
- 4 个 extractor **并行调用**(asyncio + Semaphore,参考 CN 的 enrichment/news.rs)
- 收集 4 个 extractor 输出后,串联给 narrator
- narrator 输出写入 `us_daily_report.md`(覆盖 programmatic 版本)

```python
async def run():
    payload = load_payload(args.date)
    extractor_outputs = await asyncio.gather(
        call_extractor("macro", payload),
        call_extractor("event", payload),
        call_extractor("quant", payload),
        call_extractor("risk", payload),
    )
    narrative = await call_narrator(payload, extractor_outputs)
    write_us_daily_report(narrative)
```

## D.3 — Cron 集成

- `quant-research-v1/scripts/run_daily.py` 加 `emit_us_narrator` step,跑在 `generate_main_strategy_v2_report` 之后
- env var `US_USE_AGENT_NARRATOR=1` 触发 agent narrator;default 仍跑 programmatic
- 跑 agent 之后,fallback 到 programmatic 如果 LLM 失败(零 break)

## D.4 — 验证

- Side-by-side 跑 7 天:programmatic vs agent narrator
- 人工 review:数字一致性 / 叙事质量 / 长度
- Cost 跟踪:每天 5 × API call(4 extractor + 1 narrator),DeepSeek ~$0.001/call,每天 ~$0.005
- 稳定性:验证 LLM 输出 schema 严格,fallback 路径可靠

## D.5 — 切换

- 默认切到 agent narrator(env var 默认 = 1)
- programmatic render 改为 `--legacy-render` flag(保留,但不默认调用)
- 6 个月后(数据充分时)可考虑移除 programmatic 路径

---

## 风险

| 风险 | 缓解 |
|---|---|
| LLM 输出数字不一致 / 幻觉 | 4 extractor 的 system prompt 严格要求"只能复述 payload 数字";narrator 也禁止改数字 |
| LLM 升级交易权限 | system prompt 硬约束 "ev_status / production_tier 必须从 payload 复制,不可改" |
| API down → 报告写不出 | fallback 到 programmatic;cron 重试 + 告警 |
| Agent 输出长度爆炸 | system prompt 限定 1500-2500 字 |
| 成本失控 | 监控每天 API 调用次数;DeepSeek 单价低,5 call/day < $0.01 |
| 中英文夹杂 | system prompt 强制"主体中文,只保留 ticker/技术词英文" |

---

## 执行顺序(commits)

1. D.0 写 5 个 prompt(本会话第一步)
2. D.1 加 `--emit-payload-only` flag + payload.json emitter
3. D.2 写 `run_us_narrator.py`(skeleton + DeepSeek client)
4. D.2.1 实际跑通 4 extractor 并行
5. D.2.2 跑通 narrator
6. D.3 wire 进 daily cron
7. D.4 跑 side-by-side 比对
8. D.5 切换默认

每步可独立 commit 并 verify。
