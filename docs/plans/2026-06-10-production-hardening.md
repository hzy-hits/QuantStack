# Production Hardening Plan(执行gate / narrator校验 / depends_on / 晋级KPI / setup闸门 / regime连续化)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 2026-06-10 review 确认的 6 个缺陷(备份任务单列、不在本计划):无价格候选可成交、narrator 校验只查存在性、cron 依赖靠分钟错峰、晋级 alpha 无告警、setup 闸门无正式决策、regime 引擎悬崖阈值。

**Architecture:** 每个 Task 独立可交付、独立 commit、TDD。Task 1-5 是确定性修复(当天可全部落地);Task 6 改交易行为,分三阶段(连续化公式 → MRS/Gamma 输入 → A/B+影子运行),默认 flag 关闭,经回测和影子对比后才翻默认。

**Tech Stack:** Python 3.13(miniconda,根 scripts/)、unittest(根 tests/,`python3 -m unittest tests.<mod>`)、DuckDB、ops/tasklib.py 任务注册表。

**约定:** 所有命令在 `~/coding/quant-stack` 执行。每个 Task 结束跑该 Task 的测试 + `python3 -m unittest tests.test_catch_up tests.test_us_narrator_prompt_contract`(防回归),然后 commit。

---

### Task 1: 执行 gate 硬断言 — missing price/stop → 强制 0R(ABB 案)

**现状:** `scripts/generate_main_strategy_v2_report.py:4090` 在收盘价缺失时给出 `status="missing_price"` 的 trade_plan(entry/stop/target 全 None),但 `build_production_decision_summary`(同文件 :2738)只在 :2813 把它拼成文字后缀 `"; plan blocker ..."`,候选仍进 `actionable` 执行表(2026-06-10 的 ABB 以 0.0441R、无止损成交)。函数里已有现成的 `blocked_execution` 列表和 `execution_blocked_0r` 状态(:2765-2775,用于全局 gate 阻断)——复用它做按行硬阻断。

**Files:**
- Modify: `scripts/generate_main_strategy_v2_report.py`(US 分支 stop/target 解析之后、约 :2795-2813 区域)
- Create: `tests/test_production_decision_gate.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_production_decision_gate.py
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load():
    spec = importlib.util.spec_from_file_location("gen_msv2_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gen = _load()


def _payload(plan: dict) -> dict:
    return {
        "portfolio_risk_overlay": {
            "rows": [{
                "market": "US", "symbol": "ABB", "name": "ABB Ltd",
                "final_r": 0.0441, "state": "actionable",
            }]
        },
        "us_trade_plan": {"ABB": plan},
        "profit_guardrails": [],
        "gamma_spring": {"rows": []},
    }


class MissingPriceGateTest(unittest.TestCase):
    def _run(self, plan: dict) -> dict:
        with mock.patch.object(gen, "evaluate_us_execution_gate",
                               return_value={"allowed": True, "top_blocker": "", "top_warning": ""}), \
             mock.patch.object(gen, "_load_virtual_holdings", return_value={}):
            return gen.build_production_decision_summary(_payload(plan))

    def test_missing_price_plan_is_blocked_not_actionable(self) -> None:
        decision = self._run({
            "status": "missing_price", "entry": None, "stop": None, "target": None,
            "latest_date": None,
            "rule": "missing US prices_daily close; no mechanical stock plan",
        })
        actionable_syms = [r.get("symbol") for r in decision.get("actionable") or []]
        blocked = decision.get("blocked_execution") or []
        self.assertNotIn("ABB", actionable_syms)
        self.assertTrue(any(r.get("symbol") == "ABB" and r.get("state") == "execution_blocked_0r"
                            for r in blocked))

    def test_ok_plan_stays_actionable(self) -> None:
        decision = self._run({
            "status": "ok", "entry": 52.1, "stop": 48.97, "target": 57.31,
            "latest_date": "2026-06-09", "rule": "entry=close; stop=-6%; target=+10%",
        })
        actionable_syms = [r.get("symbol") for r in decision.get("actionable") or []]
        self.assertIn("ABB", actionable_syms)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m unittest tests.test_production_decision_gate -v`
Expected: `test_missing_price_plan_is_blocked_not_actionable` FAIL(ABB 仍在 actionable)。
注意:若 `_payload` 缺字段导致 import/AttributeError,按报错补最小键,不改断言。

- [ ] **Step 3: 实现按行硬阻断**

在 US 分支解析出 `stop_value` / `target_value` 之后、`plan_suffix` 计算之前插入:

```python
            plan_status = (trade_plan or {}).get("status")
            if plan_status and plan_status != "ok" and not stop_value:
                blocked_execution.append({
                    "market": "US",
                    "symbol": row.get("symbol"),
                    "name": row.get("name") or ranked.get("name") or "",
                    "state": "execution_blocked_0r",
                    "reason": clean_table_text(
                        f"no executable plan ({(trade_plan or {}).get('rule') or plan_status}); "
                        "run scripts/backfill price ingest before sizing", 160),
                })
                continue
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m unittest tests.test_production_decision_gate -v`
Expected: 2 passed。

- [ ] **Step 5: 回归 + 实跑验证**

Run: `python3 -m unittest tests.test_us_daily_data_calibration tests.test_validate_main_strategy_v2_reports`
Run: `python3 scripts/generate_main_strategy_v2_report.py --date 2026-06-10 --ai-infra-mode enforce_expand`
Expected: 报告生成;`reports/review_dashboard/main_strategy_v2/2026-06-10/` 的执行表中 ABB 出现在 blocked(0R),`us_r` 合计相应减少(0.441R → 0.3969R,9 票)。

- [ ] **Step 6: Commit**

```bash
git add scripts/generate_main_strategy_v2_report.py tests/test_production_decision_gate.py
git commit -m "Block US candidates without executable price plan from the execution table"
```

---

### Task 2: Narrator 结构校验强化(只许 6 个 H2、禁 emoji、禁内部字段名)

**现状:** `scripts/agents/run_us_narrator.py:853` `validate_structured_us_report` 只查 6 段存在、≥4 表、关键 marker。style guard(:53/:64/:65)的"只写 6 个二级标题 / 不使用 emoji / 不出现内部字段名"只活在 prompt 里。codex 自觉性高没暴露;DeepSeek fallback 时代需要校验器兜住(修复回路会自动消化新规则)。

**Files:**
- Modify: `scripts/agents/run_us_narrator.py`(validate_structured_us_report,:853-878;文件头加 `import re`)
- Create: `tests/test_validate_structured_us_report.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_validate_structured_us_report.py
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "agents" / "run_us_narrator.py"


def _load():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    sys.path.insert(0, str(REPO_ROOT / "scripts" / "agents"))
    spec = importlib.util.spec_from_file_location("us_narrator_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


nar = _load()

_TABLE = "| A | B |\n|---|---|\n| x | y |\n"
AS_OF = "2026-06-10"


def _valid_report() -> str:
    return "\n".join([
        f"# 美股量化日报 — {AS_OF}",
        "## 策略主线", "正文。",
        "## 市场结构", "US Realized Horizon Edge 历史持有周期复盘。", _TABLE,
        "## 交易计划", "Production candidates 正式执行表。", _TABLE,
        "## 风险与反证", "IV/HV 与 Gamma v3 证据,Congressional 无 artifact。", _TABLE,
        "## 催化与复核", _TABLE,
        "## 附注", "不构成投资建议。",
    ])


class StructuredReportGuardTest(unittest.TestCase):
    def test_clean_report_passes(self) -> None:
        nar.validate_structured_us_report(_valid_report(), AS_OF, None)

    def test_extra_h2_section_rejected(self) -> None:
        text = _valid_report() + "\n## 🎲 今日概率最优\n临时段落。"
        with self.assertRaisesRegex(RuntimeError, "unexpected H2"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_emoji_rejected(self) -> None:
        text = _valid_report().replace("正文。", "正文 ✓🎯。")
        with self.assertRaisesRegex(RuntimeError, "emoji"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_internal_field_name_rejected(self) -> None:
        text = _valid_report().replace("正文。", "stable_alpha_gate 未放行,payload 显示……")
        with self.assertRaisesRegex(RuntimeError, "internal"):
            nar.validate_structured_us_report(text, AS_OF, None)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m unittest tests.test_validate_structured_us_report -v`
Expected: 后 3 个用例 FAIL(当前校验器不抛错)。

- [ ] **Step 3: 实现三条新规则**

在 `validate_structured_us_report` 末尾(payload lineage 检查之前)加:

```python
    allowed_h2 = {
        "## 策略主线", "## 市场结构", "## 交易计划",
        "## 风险与反证", "## 催化与复核", "## 附注",
    }
    extra_h2 = [ln.strip() for ln in text.splitlines()
                if ln.strip().startswith("## ") and ln.strip() not in allowed_h2]
    if extra_h2:
        raise RuntimeError(f"US narrator output has unexpected H2 sections: {extra_h2[:5]}")
    emoji_re = re.compile(r"[\U0001F000-\U0001FAFF☀-➿⬀-⯿️✓✗]")
    found = emoji_re.findall(text)
    if found:
        raise RuntimeError(f"US narrator output contains emoji/decoration: {sorted(set(found))[:8]}")
    banned_internal = [
        "提取器", "payload", "digest", "merge-agent", "user_msg", "system prompt",
        "stable_alpha_gate", "ev_status", "production_decision_summary",
        "execution_blocked_0r", "active_watch", "ranked_watch", "gpt-5.5", "deepseek",
    ]
    hits = [token for token in banned_internal if token in text]
    if hits:
        raise RuntimeError(f"US narrator output leaks internal field names: {hits}")
```

注:emoji 区间含 `✓`(U+2713)与 `⚠`(U+26A0,在 2600-27BF 内)——payload 来源字符串(如 `⚠待核验`)会触发修复回路改写成文字,这是 style guard 的本意。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m unittest tests.test_validate_structured_us_report -v`
Expected: 4 passed。

- [ ] **Step 5: 真实样本回归**

Run: `python3 scripts/agents/run_us_narrator.py --date 2026-06-10 --overwrite`
Expected: 成功(可能多走 1-2 轮 repair——新规则第一次对 DeepSeek 生效);产物无 emoji、恰好 6 个 H2。若 repair 轮耗尽失败,把失败样本存入 `/tmp/` 检查是哪条规则,必要时收窄 banned 列表(例如移除误伤词),不放松 H2/emoji 两条。

- [ ] **Step 6: Commit**

```bash
git add scripts/agents/run_us_narrator.py tests/test_validate_structured_us_report.py
git commit -m "Enforce section exclusivity, emoji ban, and internal-field ban in US narrator validation"
```

---

### Task 3: ops 任务依赖 — depends_on 落地

**现状:** `ops/tasks.yaml` 的先后关系全写在注释里("Runs AFTER bubble_hedge_radar (12:14)");`PROJECT_CONSOLIDATION_PLAN`(已归档)的 schema 提案里有 `depends_on` 但从未实现。任何上游变慢,下游静默用旧数据。

**设计:** 依赖检查放 `ops/tasklib.py`(run_task 与 catch_up 共用);未满足时 `run_task.py` 以 rc=75 退出并写 `last_blocked` state(不写 last_failure);由于 last_success 仍是旧的,`ops.catch_up` 15 分钟后自然重试——无须新增重试机制,只需 catch_up 按依赖排序。依赖"满足"定义:dep 的 `last_success.finished_at` 在今天(CST);若 dep 今天本来就不被 cron 调度(周末/节假日),视为满足。

**Files:**
- Modify: `ops/tasklib.py`、`ops/run_task.py:84`(锁之后、last_start 之前)、`ops/catch_up.py`(find_missed 排序)、`ops/tasks.yaml`
- Create: `tests/test_task_dependencies.py`(tests/test_catch_up.py 已存在,沿用其测试基建模式)

- [ ] **Step 1: 写失败测试**

```python
# tests/test_task_dependencies.py
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_under_test", REPO_ROOT / "ops" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tasklib = _load("tasklib")
CST = timezone(timedelta(hours=8))


class DependencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.state_dir = Path(tempfile.mkdtemp())

    def _write_success(self, task_id: str, finished: datetime) -> None:
        (self.state_dir / f"{task_id}.last_success.json").write_text(
            json.dumps({"task_id": task_id, "finished_at": finished.isoformat()}), encoding="utf-8")

    def test_unmet_when_dep_has_no_success_today(self) -> None:
        now = datetime(2026, 6, 10, 12, 25, tzinfo=CST)
        self._write_success("research.bubble_hedge_radar", now - timedelta(days=1))
        task = {"task_id": "research.risk_regime_engine",
                "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "17 12 * * 1-5"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        unmet = tasklib.unmet_dependencies(task, registry=registry, state_dir=self.state_dir, now=now)
        self.assertEqual(unmet, ["research.bubble_hedge_radar"])

    def test_met_when_dep_succeeded_today(self) -> None:
        now = datetime(2026, 6, 10, 12, 25, tzinfo=CST)
        self._write_success("research.bubble_hedge_radar", now - timedelta(minutes=10))
        task = {"task_id": "research.risk_regime_engine",
                "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "17 12 * * 1-5"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        self.assertEqual(tasklib.unmet_dependencies(task, registry=registry,
                                                    state_dir=self.state_dir, now=now), [])

    def test_dep_not_scheduled_today_counts_as_met(self) -> None:
        # 周六:dep 只在工作日跑,不该阻塞周末任务
        now = datetime(2026, 6, 13, 10, 30, tzinfo=CST)  # Saturday
        task = {"task_id": "weekly.us", "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "30 9 * * 6"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        self.assertEqual(tasklib.unmet_dependencies(task, registry=registry,
                                                    state_dir=self.state_dir, now=now), [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m unittest tests.test_task_dependencies -v`
Expected: AttributeError(tasklib 无 unmet_dependencies)。

- [ ] **Step 3: 实现 tasklib.unmet_dependencies**

把 `ops/catch_up.py` 的 `_parse_field`/`_matches`(:38-:74)迁移到 `ops/tasklib.py`(catch_up 改为 `from tasklib import ...` 引用,保留原行为),然后加:

```python
def _scheduled_today(task: dict[str, Any], now: datetime) -> bool:
    exprs = [task.get("schedule")] if task.get("schedule") else list(task.get("schedules") or [])
    for expr in exprs:
        if not expr or str(expr).startswith("@"):
            continue
        minute, hour, dom, month, dow = str(expr).split()
        if (_matches_field(dom, now.day, 1, 31) and _matches_field(month, now.month, 1, 12)
                and _matches_field(dow, now.isoweekday() % 7, 0, 6)):
            return True
    return False


def unmet_dependencies(task: dict[str, Any], *, registry: dict[str, dict[str, Any]],
                       state_dir: Path, now: datetime) -> list[str]:
    unmet: list[str] = []
    for dep_id in task.get("depends_on") or []:
        dep = registry.get(dep_id)
        if dep is None or not _scheduled_today(dep, now):
            continue
        success_path = state_dir / f"{dep_id}.last_success.json"
        try:
            finished = datetime.fromisoformat(
                json.loads(success_path.read_text(encoding="utf-8"))["finished_at"])
        except (OSError, ValueError, KeyError):
            unmet.append(dep_id)
            continue
        if finished.astimezone(CST_TZ).date() != now.astimezone(CST_TZ).date():
            unmet.append(dep_id)
    return unmet
```

(`_matches_field` 为从 catch_up 迁来的单字段匹配 helper;`CST_TZ = timezone(timedelta(hours=8))` 在 tasklib 顶部定义。)

- [ ] **Step 4: run_task.py 接入(锁后、last_start 前)**

```python
    unmet = tasklib.unmet_dependencies(
        task, registry=tasklib.tasks(), state_dir=STACK_ROOT / "ops" / "state",
        now=datetime.now(tasklib.CST_TZ))
    if unmet:
        print(f"task blocked on dependencies: {task['task_id']} <- {','.join(unmet)}", file=sys.stderr)
        write_state(task, "last_blocked",
                    {"task_id": task["task_id"], "blocked_at": utc_now(), "unmet": unmet})
        return 75
```

支持 `RUN_TASK_IGNORE_DEPS=1` 环境变量旁路(手动补跑用):包一层 `if not os.environ.get("RUN_TASK_IGNORE_DEPS")`。

- [ ] **Step 5: catch_up 按依赖排序**

`find_missed` 返回前做拓扑排序(missed 集合内,依赖在前;无环假设,循环兜底直接原序返回):

```python
def _order_by_dependency(missed: list[tuple[str, datetime]],
                         registry: dict[str, dict[str, Any]]) -> list[tuple[str, datetime]]:
    pending = {task_id for task_id, _ in missed}
    ordered: list[tuple[str, datetime]] = []
    remaining = list(missed)
    for _ in range(len(missed) + 1):
        progressed = False
        for item in list(remaining):
            deps = set((registry.get(item[0]) or {}).get("depends_on") or [])
            if not deps & pending:
                ordered.append(item)
                pending.discard(item[0])
                remaining.remove(item)
                progressed = True
        if not progressed:
            return ordered + remaining
    return ordered
```

- [ ] **Step 6: tasks.yaml 声明依赖(只声明注释里已写明的链)**

```yaml
# 在对应 task 下新增 depends_on 键:
research.victim_put_suggestions:    depends_on: [research.bubble_hedge_radar]
research.capitulation_radar:        depends_on: [research.bubble_hedge_radar]
research.capitulation_convex_radar: depends_on: [research.capitulation_radar]
research.risk_regime_engine:        depends_on: [research.bubble_hedge_radar, research.capitulation_radar]
research.cn_risk_regime:            depends_on: [research.bubble_hedge_radar]
research.cn_ai_evidence_verify:     depends_on: [research.ai_infra_promotion_plan]
research.production_universe_refresh: depends_on: [research.source_review_readiness]
research.main_strategy_v2_report:   depends_on: [research.risk_regime_engine, research.production_universe_refresh]
research.production_basket_audit:   depends_on: [research.main_strategy_v2_report]
```

- [ ] **Step 7: 跑测试 + 干跑验证**

Run: `python3 -m unittest tests.test_task_dependencies tests.test_catch_up -v`
Expected: 全过。
Run: `ops/run_task.sh --dry-run research.main_strategy_v2_report && python3 ops/catch_up.py --dry-run`
Expected: dry-run 正常列出命令;catch_up 列表顺序满足依赖在前。

- [ ] **Step 8: Commit**

```bash
git add ops/tasklib.py ops/run_task.py ops/catch_up.py ops/tasks.yaml tests/test_task_dependencies.py
git commit -m "Implement depends_on for ops tasks; catch_up replays in dependency order"
```

---

### Task 4: 晋级闸门 5d alpha 告警

**现状:** `scripts/backtest_promotion_history.py` 每日 12:20 产出 trailing 4 周 IR(2026-06-10:5d IR -0.21,N=75),但没人看也没告警。

**决策规则(写进代码注释与 md):** trailing 4 周 5d 口径 N≥40 且 IR ≤ -0.10 → 告警;连续两周告警 → 操作员人工冻结 promote_now(本计划只做可见性,自动冻结是后续决定)。

**Files:**
- Modify: `scripts/backtest_promotion_history.py`(`_aggregate_trailing` 之后、md 渲染处,约 :319 起)
- Create: `tests/test_promotion_alpha_alert.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_promotion_alpha_alert.py
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "backtest_promotion_history.py"


def _load():
    spec = importlib.util.spec_from_file_location("promo_bt_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bt = _load()


class PromotionAlphaAlertTest(unittest.TestCase):
    def test_alert_fires_on_negative_ir_with_enough_n(self) -> None:
        trailing_5d = [("2026-W22", {"n": 65, "mean_active_pct": -0.68, "hit_rate_pct": 41.5, "ir": -0.13}),
                       ("2026-W23", {"n": 75, "mean_active_pct": -1.15, "hit_rate_pct": 38.7, "ir": -0.21})]
        alert = bt.promotion_alpha_alert(trailing_5d)
        self.assertIsNotNone(alert)
        self.assertEqual(alert["level"], "warning")
        self.assertEqual(alert["week"], "2026-W23")

    def test_no_alert_on_small_sample_or_positive_ir(self) -> None:
        self.assertIsNone(bt.promotion_alpha_alert([("2026-W23", {"n": 12, "ir": -0.5,
                                                                  "mean_active_pct": -2.0, "hit_rate_pct": 30.0})]))
        self.assertIsNone(bt.promotion_alpha_alert([("2026-W23", {"n": 80, "ir": 0.05,
                                                                  "mean_active_pct": 0.2, "hit_rate_pct": 51.0})]))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m unittest tests.test_promotion_alpha_alert -v`
Expected: AttributeError(无 promotion_alpha_alert)。

- [ ] **Step 3: 实现告警函数 + 接线**

```python
ALERT_IR_THRESHOLD = -0.10
ALERT_MIN_N = 40


def promotion_alpha_alert(trailing_5d: list[tuple[str, dict[str, Any]]]) -> dict[str, Any] | None:
    """trailing 4w 5d: N>=40 且 IR<=-0.10 → warning。连续两周 warning = 操作员冻结 promote_now。"""
    if not trailing_5d:
        return None
    week, agg = trailing_5d[-1]
    n, ir = int(agg.get("n") or 0), agg.get("ir")
    if n >= ALERT_MIN_N and ir is not None and ir <= ALERT_IR_THRESHOLD:
        return {"level": "warning", "week": week, "horizon": "5d", "n": n, "ir": ir,
                "mean_active_pct": agg.get("mean_active_pct"),
                "rule": f"trailing-4w 5d IR<={ALERT_IR_THRESHOLD} with N>={ALERT_MIN_N}"}
    return None
```

main 渲染处:`alert = promotion_alpha_alert(trailing[5])`;非空时 (a) md 顶部插 `> **[ALERT]** promote_now 5d active IR {ir} (N={n}) — 连续两周告警须人工冻结晋级`,(b) 写 `out_dir / "alert.json"`,(c) `print(f"[ALERT] promotion alpha ...")`(进 ops 日志可 grep)。

- [ ] **Step 4: 跑测试 + 实跑**

Run: `python3 -m unittest tests.test_promotion_alpha_alert -v` → 2 passed。
Run: `python3 scripts/backtest_promotion_history.py --as-of 2026-06-10`
Expected: 当前数据(IR -0.21,N 75)触发告警,`reports/review_dashboard/ai_infra_promotion_alpha/2026-06-10/alert.json` 落盘。

- [ ] **Step 5: Commit**

```bash
git add scripts/backtest_promotion_history.py tests/test_promotion_alpha_alert.py
git commit -m "Alert when trailing promotion 5d active IR breaches threshold"
```

---

### Task 5: Setup 进场闸门 — 正式决策记录(轻量)

**现状:** `score_entry_setup` 只被 `run_ai_infra_strategy_backtest.py` 作为对照腿引用,**不在生产决策链上**;回测连续判定它拖累夏普(US 1.99 vs 2.13,CN 1.57 vs 1.89)。需要的是一条正式决策记录,防止未来被无意接入生产。

**Files:**
- Create: `docs/DECISIONS.md`
- Modify: `scripts/run_ai_infra_strategy_backtest.py`(md 输出中对照腿标注)、`README.md`(Operating Docs 索引加一行)

- [ ] **Step 1: 写决策记录**

```markdown
# Decisions Log

逆向时间序。每条:日期 / 决定 / 证据 / 复议条件。

## 2026-06-10 — setup 进场闸门:仅保留为回测对照腿,不得接入生产 sizing
- **决定**: `scripts/score_entry_setup.py` 维持 research-only。生产执行表(main_strategy_v2)
  不使用"等回调再进"setup 闸门。
- **证据**: `ai_infra_strategy_backtest` 2024-06→2026-06:setup 闸门腿夏普 US 1.99 vs
  全篮子 2.13(回撤 -43.0% vs -31.3%),CN 1.57 vs 1.89——强趋势票等回调反而错过主升。
- **复议条件**: 仅当出现 regime 条件化变体(例如只在 WEDGE/PRESS 启用 setup 闸门)且
  A/B 回测夏普与回撤同时不劣于基线时,才重新评估。
```

- [ ] **Step 2: 回测输出标注**

`run_ai_infra_strategy_backtest.py` 渲染"进场setup闸门"行的地方,行尾追加 `(research-only 对照腿,生产未采用 — docs/DECISIONS.md 2026-06-10)`。README 的 Operating Docs"边界与合同"小节加一行 `- [Decisions Log](docs/DECISIONS.md): 正式决策记录(复议条件齐备才重开)。`

- [ ] **Step 3: 验证 + Commit**

Run: `python3 scripts/run_ai_infra_strategy_backtest.py`(~1-2 分钟)
Expected: md 输出带新标注;数字与上次一致。

```bash
git add docs/DECISIONS.md scripts/run_ai_infra_strategy_backtest.py README.md
git commit -m "Record setup-entry-gate decision: research-only, rejected for production sizing"
```

---

### Task 6: Regime 引擎连续化(三阶段,默认 flag 关)

**现状:** `scripts/score_risk_regime_engine.py` 全二元阈值(corr≥0.5、MOVE≥80、TLT≤-2%、HYG≤-1%,:60-62)。2026-06-10 四项分别 0.446 / 77 / -0.51% / -0.45%——全部"刚好"未触发 → HEDGE 满仓 1.00x,而同日 MRS 在 III 象限、SPY/QQQ/SMH 全负 Gamma 加速。

**设计原则:** 改交易行为必须 (a) 新旧值并行输出、(b) 默认读旧值、(c) 回测 + 影子运行后人工翻 flag。

#### 6a: 连续 wedge 压力分 → r_multiplier_continuous(并行输出,不改消费方)

**Files:**
- Modify: `scripts/score_risk_regime_engine.py`
- Create: `tests/test_regime_continuous.py`

- [ ] **Step 1: 写失败测试(用 2026-06-10 真实读数定锚)**

```python
# tests/test_regime_continuous.py
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "regime_under_test", REPO_ROOT / "scripts" / "score_risk_regime_engine.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


eng = _load()


class ContinuousRegimeTest(unittest.TestCase):
    def test_smoothstep_bounds(self) -> None:
        self.assertEqual(eng._smoothstep(None, 0.35, 0.5), 0.0)
        self.assertEqual(eng._smoothstep(0.30, 0.35, 0.5), 0.0)
        self.assertEqual(eng._smoothstep(0.60, 0.35, 0.5), 1.0)
        self.assertAlmostEqual(eng._smoothstep(0.425, 0.35, 0.5), 0.5, places=2)

    def test_2026_06_10_cliff_day_gets_dampened(self) -> None:
        # 当天真实信号:corr 0.446 / MOVE 77 升 / TLT -0.51% / HYG -0.45%
        pressure = eng.wedge_pressure(corr=0.446, move_level=77.0, move_rising=True,
                                      tlt_ret_20d=-0.51, hyg_ret_20d=-0.45)
        self.assertGreater(pressure, 0.30)
        mult = eng.continuous_multiplier(state="hedge", base_multiplier=1.0, pressure=pressure)
        self.assertLess(mult, 0.90)
        self.assertGreaterEqual(mult, 0.60)

    def test_calm_day_stays_full_size(self) -> None:
        pressure = eng.wedge_pressure(corr=0.10, move_level=60.0, move_rising=False,
                                      tlt_ret_20d=1.2, hyg_ret_20d=0.5)
        self.assertLess(pressure, 0.05)
        self.assertAlmostEqual(
            eng.continuous_multiplier(state="hedge", base_multiplier=1.0, pressure=pressure),
            1.0, places=2)

    def test_non_hedge_states_unchanged(self) -> None:
        self.assertEqual(eng.continuous_multiplier(state="wedge", base_multiplier=0.60, pressure=0.9), 0.60)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑测试确认失败** → `python3 -m unittest tests.test_regime_continuous -v`

- [ ] **Step 3: 实现(纯函数 + JSON 并行字段)**

```python
def _smoothstep(value: float | None, lo: float, hi: float) -> float:
    if value is None:
        return 0.0
    t = max(0.0, min(1.0, (value - lo) / (hi - lo)))
    return t * t * (3.0 - 2.0 * t)


def wedge_pressure(*, corr: float | None, move_level: float | None, move_rising: bool,
                   tlt_ret_20d: float | None, hyg_ret_20d: float | None) -> float:
    """0..1 连续 wedge 压力。阈值悬崖(全部'刚好'未触发)在这里变成连续扣减。"""
    corr_p = _smoothstep(corr, 0.35, CORR_FLIP_THRESHOLD)            # 0.35→0.5
    move_p = _smoothstep(move_level, 70.0, 80.0) * (1.0 if move_rising else 0.5)
    tlt_p = _smoothstep(-(tlt_ret_20d or 0.0), 1.0, -TLT_WEDGE_DRAWDOWN_PCT)   # 1%→2%
    hyg_p = _smoothstep(-(hyg_ret_20d or 0.0), 0.5, 1.0)
    return round(0.35 * corr_p + 0.25 * move_p + 0.25 * tlt_p + 0.15 * hyg_p, 4)


def continuous_multiplier(*, state: str, base_multiplier: float, pressure: float) -> float:
    """只在 hedge(满仓)态做连续扣减;其余状态本身已是折扣值。下限 = wedge 档 0.60。"""
    if state != "hedge":
        return base_multiplier
    return round(max(R_MULTIPLIER["wedge"], base_multiplier * (1.0 - 0.35 * pressure)), 2)
```

`classify_regime` 调用处(main 内拿到 decision 后):算 `pressure`、`r_cont = continuous_multiplier(...)`,写进输出 JSON:`"wedge_pressure"`, `"r_multiplier_continuous"`, `"wedge_pressure_components"`;当 `os.environ.get("QUANT_REGIME_CONTINUOUS") == "1"` 时令 `r_multiplier = r_cont`(消费方无感切换)。markdown 渲染加一行 `连续乘数(影子): {r_cont}x (pressure {pressure})`。

- [ ] **Step 4: 跑测试 + 实跑** → 测试全过;`python3 scripts/score_risk_regime_engine.py --as-of 2026-06-10` 输出 JSON 同时含 `r_multiplier: 1.0` 与 `r_multiplier_continuous: ~0.84`。

- [ ] **Step 5: Commit** → `git commit -m "Regime engine: continuous wedge pressure shadow multiplier behind flag"`

#### 6b: MRS 象限 + Gamma 加速进入 dampener(影子)

- [ ] **Step 1: 抽取 MRS 计算**。`grep -n "market_regime_score" scripts/generate_main_strategy_v2_report.py` 定位构建函数,把纯计算部分(输入:SPY 动量、P/C 及其 5d 变化、恐惧水平;输出 mrs/mrs_bucket/quadrant)搬到 `scripts/lib/market_regime.py`(REFACTOR_PLAN Phase A 同款手法,**零行为变更**:generate 脚本改 import,跑 `--date 2026-06-10` 前后 md diff 仅时间戳)。
- [ ] **Step 2: regime 引擎接入(容错)**。`score_risk_regime_engine.py` 通过 `scripts/lib/market_regime.py` 算当日 MRS(读 quant.duckdb,read_only;数据缺失 → None,不扣减),并 `from sections.gamma_spring import build_gamma_spring_snapshot` 取 SPY/QQQ/SMH 状态:

```python
def context_dampener(*, mrs: float | None, mrs_quadrant: str | None,
                     negative_accel_count: int) -> float:
    d = 1.0
    if mrs is not None and mrs <= -0.5 and (mrs_quadrant or "").upper().startswith("III"):
        d *= 0.85
    d *= max(0.85, 1.0 - 0.05 * max(0, negative_accel_count))
    return round(max(0.70, d), 4)
```

  `r_multiplier_continuous` 改为 `continuous_multiplier(...) * context_dampener(...)`(仍 floor 0.60,仍影子字段)。单元测试:III 象限 + 3 指数负加速 → dampener 0.85*0.85≈0.72;数据缺失 → 1.0。
- [ ] **Step 3: Commit** → `git commit -m "Regime shadow multiplier: MRS quadrant and gamma acceleration dampeners"`

#### 6c: 验证与切换(不在本计划内自动执行)

- [ ] **回测 A/B**:`run_ai_infra_strategy_backtest.py` 加 `--regime-variant continuous`(wedge 连续分可全窗口回测;MRS/P-C 历史仅 ~400 天、Gamma 历史不可得 → 这两项只进影子,不进回测,md 里明写口径)。验收:连续变体 US/CN 夏普 ≥ 基线 -0.05,maxDD ≤ 基线。
- [ ] **影子运行 ≥ 10 个交易日**:每日 cron 输出双乘数;`grep r_multiplier_continuous ops/logs/research.risk_regime_engine.log` 汇总对比表。
- [ ] **人工决定翻 flag**:回测 + 影子都通过后,由操作员在 `ops/tasks.yaml` 的 `research.risk_regime_engine` 加 `env: {QUANT_REGIME_CONTINUOUS: "1"}` 并在 `docs/DECISIONS.md` 记录。**本计划不自动翻。**

---

## 执行顺序与回归门

1. Task 1 → 2 → 4 → 5(互相独立,确定性,当天可完成)
2. Task 3(ops 改动,完成后观察一个完整 cron 日)
3. Task 6a → 6b(影子)→ 6c(等回测与影子数据,人工决定)

每个 Task commit 前的全局回归:
`python3 -m unittest tests.test_narrator_backend_fallback tests.test_catch_up tests.test_us_narrator_prompt_contract tests.test_validate_main_strategy_v2_reports`
全部完成后跑 `./smoke-check.sh` 并观察次日 cron 三份日报正常落地。
