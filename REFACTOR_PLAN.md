# Refactor Plan — `scripts/generate_main_strategy_v2_report.py`

**当前状态 (2026-05-27)**: 11,105 行 / 289 顶层函数 / 65 个 `render_*_section` / 30 个 `build_*`.

**目标**: 拆成可维护的模块化结构,最终让 US 报告也跑 agent + narrator 架构(对齐 CN 端 `quant-research-cn/prompts/` 模式)。

**核心承诺**: 每一步 commit 都 **byte-identical**(允许差异:`generated_at` 时间戳行) 跑通 daily 报告。任何步骤 break 输出立刻 revert。

---

## 验证协议(每个 phase 必走)

```bash
# 1. baseline 捕获(改前)
python3 scripts/generate_main_strategy_v2_report.py --date 2026-05-27 \
    --ai-infra-mode enforce_expand --cn-db /tmp/quant_cn_snapshot.duckdb
cp reports/.../us_daily_report.md /tmp/us_before.md
cp reports/.../cn_daily_report.md /tmp/cn_before.md

# 2. 改

# 3. regen + diff(忽略时间戳)
python3 scripts/generate_main_strategy_v2_report.py --date 2026-05-27 \
    --ai-infra-mode enforce_expand --cn-db /tmp/quant_cn_snapshot.duckdb
diff <(grep -v 'generated_at\|2026-05-27T[0-9]' /tmp/us_before.md) \
     <(grep -v 'generated_at\|2026-05-27T[0-9]' reports/.../us_daily_report.md)
# 必须空输出。CN 同理。

# 4. import sanity
python3 -c "import importlib.util,sys; \
  spec=importlib.util.spec_from_file_location('m','scripts/generate_main_strategy_v2_report.py'); \
  m=importlib.util.module_from_spec(spec); sys.modules['m']=m; spec.loader.exec_module(m); \
  print('OK')"
```

---

## Phase A — 抽 utility 到 `scripts/lib/`(零行为变化)

**目标**: 把主文件里的通用 helpers 抽出去,降低主文件耦合度,为后续 section 抽取打基础。

| Sub-phase | 内容 | 风险 | 状态 |
|---|---|---|---|
| **A.0** | DB helpers 抽到 `scripts/lib/db_helpers.py`:`_connect_ro / table_exists / table_columns / rows_as_dicts / placeholders` | 低 | TODO |
| **A.1** | Format helpers 抽到 `scripts/lib/fmt.py`:`round_or_none / fmt_pct / fmt_num / fmt_r / fmt_rate_pct / clean_table_text / parse_date / as_iso / safe_json_loads / normalize_symbol` | 低 | TODO |
| **A.2** | LLM agent helpers(已存在的 headline_agent)与 score_strategy helpers 整理到 `scripts/lib/agents.py` | 低 | TODO |

**验证**: 每 sub-phase 后必走完整验证协议;一处 diff 立即 revert。

---

## Phase B — 抽 section 到 `scripts/sections/`(零行为变化)

**目标**: 65 个 `render_*_section` 按主题分到独立模块。**只抽 build_* + render_*,不改文本**。

| Sub-phase | 模块 | 包含函数 | 状态 |
|---|---|---|---|
| **B.0** | `scripts/sections/serenity.py` | `build_serenity_crosscheck / render_serenity_crosscheck_section` | TODO(本会话执行) |
| **B.1** | `scripts/sections/market_regime.py` | `build_market_regime_score / render_market_regime_score_section` | TODO |
| **B.2** | `scripts/sections/probability_picks.py` | `_pick_probability_stock / _pick_probability_leaps / _pick_probability_short / render_us_probability_picks_section` | TODO |
| **B.3** | `scripts/sections/top10_daily.py` | `render_us_top10_daily_section + _tenor_signals_by_sym` | TODO |
| **B.4** | `scripts/sections/iv_view.py` | `_iv_action_hint / render_iv_view_section + build_options_verdicts` | TODO |
| **B.5** | `scripts/sections/left_side.py` | `render_us_left_side_section / render_cn_left_side_watch_section / cn_left_side_watch_rows / regime_left_right_tilt / render_regime_tilt_header` | TODO |
| **B.6** | `scripts/sections/regime.py` | `render_risk_regime_section / build_risk_regime` 等 | TODO |
| **B.7-Bn** | 其余 ~58 个 `render_*_section` 按主题分模块 | TODO |

**抽取规则**:
1. 每模块只放 self-contained 的函数 + 必要的 private helpers
2. 共享 utility 一律从 `scripts/lib/` import
3. 主文件改成:`from scripts.sections.serenity import render_serenity_crosscheck_section`
4. 同 commit 删主文件里的原 def
5. Verify byte-identical

---

## Phase C — 分离 US / CN 渲染入口

**目标**: 主文件只做 orchestration,US 报告 / CN 报告各自有独立 entrypoint。

| Sub-phase | 内容 | 状态 |
|---|---|---|
| **C.0** | `scripts/reports/us_daily.py` — `render_us_standalone_report` | ✅ 93ecac6 (74 行) |
| **C.1** | `scripts/reports/cn_daily.py` — `render_cn_standalone_report` | ✅ 773d4e1 (63 行) |
| **C.2a** | `scripts/reports/combined.py` — `render_report` | ✅ 07b39b4 (156 行) |
| **C.2b** | `scripts/reports/factorlab.py` — `render_factorlab_brief` | ✅ e9cbffd (66 行) |

**实际成果**:4 个 render entry 全部抽出,monolith 7585 → 7228 (-357 行,-4.7%)。

**< 2000 行目标未达**:需要 Phase B.20+ 继续抽 ~50 个 `build_*` 函数(它们是 payload 构造逻辑,不是 render)。Phase C 范围本来就只覆盖 render 层。

---

## Phase D — Agent + Narrator 化(对齐 CN 架构)

**目标**: US 也走 `program → payload.md → 4 extractor + narrator → 最终报告` 模式。

| Sub-phase | 内容 | 风险 | 状态 |
|---|---|---|---|
| **D.0** | 写 4 个 US extractor prompt(macro / event / quant / risk),放 `quant-research-v1/prompts/`(parallel 给 CN 端) | 中 | TODO |
| **D.1** | 写 US narrator/merge prompt(参考 `quant-research-cn/prompts/merge-agent.md`) | 中 | TODO |
| **D.2** | 写 `scripts/agents/run_us_narrator.py` 调度脚本:`payload.md → 4 extractor 并发 → narrator → final` | 中 | TODO |
| **D.3** | `generate_main_strategy_v2_report.py` 改成只输出 `payload.md`(不再硬编码叙事文本) | 高 | TODO(需 dedicated sprint) |
| **D.4** | Daily cron 改成:`program → payload → agent narrator` 串联 | 高 | TODO |

---

## Phase E — Cleanup + Documentation

| Sub-phase | 内容 | 状态 |
|---|---|---|
| **E.0** | 添加 `tests/test_section_render.py` — 每 section 一个 golden snapshot test | TODO |
| **E.1** | 在 `AGENTS.md` 增加 "如何添加新 section" 文档 | TODO |
| **E.2** | 删除主文件里所有 dead code(>50 行未引用函数) | TODO |

---

## 风险登记 & rollback

| 风险 | 缓解 |
|---|---|
| 抽 section 时 import path 错 → 主文件 crash | 每 phase 单独 commit,`git revert <hash>` 单步回滚 |
| 函数被多处引用,只改一处 → 字段对不上 | 每 phase 跑 `grep -rn "function_name" scripts/` 确认所有 callsite |
| 时间戳差异掩盖了真 diff | 验证协议明确 grep -v 时间戳,只看结构 diff |
| Phase D 重写 65 个 render_* 文本 → 报告完全长不一样 | Phase D 单独 sprint,不和 A/B/C 混做 |
| Daily cron 跑挂 → 当天没报告 | 每 phase commit 前确认 daily cron 仍能跑(import sanity + small smoke test) |

---

## 本会话执行范围

仅做 **A.0**(DB helpers 抽出) + **B.0**(serenity section 抽出),作为 proof of concept。

如果两步都 byte-identical 通过验证,证明方法学可行,后续 phase 按同样模板继续。

如果任一步骤失败,立即 revert + 检查方法学,而不是带 bug 推进。
