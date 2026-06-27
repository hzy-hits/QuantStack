# CN 0R Ranked Watch 雷达 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface ranker rows in the 0R `active_watch` tier (incl. 科创板, ★-flagged) in the CN daily report and wire the new section into the CN narrator's slice list so it reaches the email body.

**Architecture:** One pure-render section module (`scripts/sections/cn_ranked_watch.py`) reads `payload["cn_opportunity_ranker"]["all_rows"]`, filters `production_tier == "active_watch"`, renders a markdown table. It is inserted into `scripts/reports/cn_daily.py` and its header substring `"0R 观察雷达"` is added to `_STRUCTURAL_HEADERS` in `scripts/agents/run_cn_narrator.py` so the narrator slices it into the email's quant payload. No tiering / execution / evidence-gate / R logic is touched.

**Tech Stack:** Python 3.11, stdlib + repo `lib.fmt` helpers. Tests: `unittest` run under `quant-research-v1/.venv/bin/python -m pytest` (pytest 9.0.2). The standalone report generator runs under base `python3` (has tushare/duckdb/yaml).

## Global Constraints

- Filter is exactly `str(row.get("production_tier")) == "active_watch"` — NOT `state` (in `all_rows`, `state` is uniformly `"AI Infra Universe Watch"`).
- Section header is exactly `## 0R 观察雷达 (Ranked Watch)`; narrator slice key is exactly `"0R 观察雷达"` (substring; must not collide with existing `左侧观察池`).
- Render module is pure: no I/O, no DB, no network, no mutation of `payload`.
- Do NOT modify `production_tier()`, `build_portfolio_risk_overlay`, `is_production_grade`, `market_actions`, or any R/size allocation.
- `sections/` is a package (`scripts/sections/__init__.py` exists); import as `from sections.cn_ranked_watch import ...` with `scripts/` on `sys.path`.
- 科创板 = symbol digit-prefix `688`; flag those rows only.
- Repo `lib.fmt` provides `fmt_num(value, digits)`, `fmt_pct(value)`, `clean_table_text(text, width)`.

---

### Task 1: Render module `scripts/sections/cn_ranked_watch.py` + unit tests

**Files:**
- Create: `scripts/sections/cn_ranked_watch.py`
- Test: `tests/test_cn_ranked_watch.py`

**Interfaces:**
- Consumes: nothing (first task). Reads `payload["cn_opportunity_ranker"]["all_rows"]` (list of dict with keys `symbol`, `name`, `production_tier`, `rank`, `rank_score`, `pct_chg`, `ev_lcb80_pct`, `size_hint`, `reason`).
- Produces:
  - `board_label(symbol: str) -> str` → one of `科创板 / 创业板 / 北交所 / 主板`.
  - `cn_ranked_watch_rows(payload: dict) -> list[dict]` → active_watch rows, rank-ascending.
  - `render_cn_ranked_watch_radar_section(payload: dict) -> list[str]` → markdown lines.

- [ ] **Step 1: Write the failing test** — create `tests/test_cn_ranked_watch.py`:

```python
"""Tests for the CN 0R Ranked Watch radar section."""
from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    name = "sections.cn_ranked_watch"
    if name in sys.modules:
        return sys.modules[name]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    return importlib.import_module(name)


def _payload():
    return {
        "cn_opportunity_ranker": {
            "all_rows": [
                {"symbol": "688535.SH", "name": "科创甲", "production_tier": "active_watch",
                 "rank": 20, "rank_score": 70.7, "pct_chg": 1.2,
                 "ev_lcb80_pct": 0.5, "size_hint": "0R", "reason": "wait for price"},
                {"symbol": "688233.SH", "name": "科创乙", "production_tier": "active_watch",
                 "rank": 18, "rank_score": 70.87, "pct_chg": -2.0,
                 "ev_lcb80_pct": 0.3, "size_hint": "0R", "reason": "prepare"},
                {"symbol": "600519.SH", "name": "主板丙", "production_tier": "active_watch",
                 "rank": 15, "rank_score": 72.0, "pct_chg": 0.5,
                 "ev_lcb80_pct": 0.4, "size_hint": "0R", "reason": "watch"},
                {"symbol": "688019.SH", "name": "bench科创", "production_tier": "bench_ranked",
                 "rank": 50, "rank_score": 60.1},
                {"symbol": "600000.SH", "name": "可执行", "production_tier": "top_stock_trade",
                 "rank": 1, "rank_score": 80.0},
            ]
        }
    }


class CnRankedWatchRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_board_label(self) -> None:
        b = self.module.board_label
        self.assertEqual(b("688981.SH"), "科创板")
        self.assertEqual(b("300750.SZ"), "创业板")
        self.assertEqual(b("301236.SZ"), "创业板")
        self.assertEqual(b("600519.SH"), "主板")
        self.assertEqual(b("000001.SZ"), "主板")
        self.assertEqual(b("830799.BJ"), "北交所")
        self.assertEqual(b("920819.BJ"), "北交所")

    def test_only_active_watch_rows_rank_ascending(self) -> None:
        rows = self.module.cn_ranked_watch_rows(_payload())
        self.assertEqual([r["symbol"] for r in rows], ["600519.SH", "688233.SH", "688535.SH"])

    def test_render_flags_star_and_excludes_non_active_watch(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section(_payload()))
        self.assertIn("## 0R 观察雷达 (Ranked Watch)", md)
        self.assertIn("★科创板", md)        # 688 flagged
        self.assertIn("688233.SH", md)
        self.assertIn("688535.SH", md)
        self.assertIn("600519.SH", md)       # mainboard active_watch present
        self.assertNotIn("688019.SH", md)    # bench_ranked excluded
        self.assertNotIn("600000.SH", md)    # executable excluded

    def test_empty_pool_placeholder(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section(
            {"cn_opportunity_ranker": {"all_rows": []}}))
        self.assertIn("## 0R 观察雷达 (Ranked Watch)", md)
        self.assertIn("没有 active_watch 0R 候选", md)

    def test_missing_ranker_key_is_safe(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section({}))
        self.assertIn("没有 active_watch 0R 候选", md)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_cn_ranked_watch.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'sections.cn_ranked_watch'` (module not created yet).

- [ ] **Step 3: Write the module** — create `scripts/sections/cn_ranked_watch.py`:

```python
"""0R Ranked Watch 雷达 section (Phase D).

Surfaces ranker rows that landed in the 0R `active_watch` tier — names the
ranker scored and ranked but that did NOT clear the execution line
(prepare-order-but-wait-for-price). 科创板 (688) rows are flagged with ★.

Pure rendering: no I/O, no DB, no payload mutation. Reads
payload["cn_opportunity_ranker"]["all_rows"] only.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_num, fmt_pct

RADAR_LIMIT = 20


def board_label(symbol: str) -> str:
    """A-share board from ticker digit-prefix (digits before any market suffix)."""
    digits = "".join(ch for ch in str(symbol) if ch.isdigit())
    if digits.startswith("688"):
        return "科创板"
    if digits.startswith(("300", "301")):
        return "创业板"
    if digits.startswith(("4", "8", "920")):
        return "北交所"
    return "主板"


def cn_ranked_watch_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Ranker rows in the 0R active_watch tier, rank-ascending (None ranks last)."""
    ranker = payload.get("cn_opportunity_ranker") or {}
    rows = [
        row
        for row in (ranker.get("all_rows") or [])
        if str(row.get("production_tier")) == "active_watch"
    ]
    rows.sort(key=lambda r: (r.get("rank") is None, r.get("rank") or 9_999))
    return rows


def render_cn_ranked_watch_radar_section(payload: dict[str, Any]) -> list[str]:
    rows = cn_ranked_watch_rows(payload)
    lines = [
        "## 0R 观察雷达 (Ranked Watch)",
        "",
        "这一档是 ranker 已排名、但未达执行线的 0R 候选(prepare but wait);★ 为科创板。不占资金,仅观察。",
        "",
    ]
    if not rows:
        lines += [
            "今天没有 active_watch 0R 候选(名字要么进了可交易名单,要么落到 bench)。",
            "",
        ]
        return lines
    lines += [
        "| Rank | Symbol | Name | 板 | Score | 1D | EV LCB80 | Size | Reason |",
        "|---:|---|---|---|---:|---:|---:|---|---|",
    ]
    for row in rows[:RADAR_LIMIT]:
        board = board_label(row.get("symbol") or "")
        board_cell = f"★{board}" if board == "科创板" else board
        ev = row.get("ev_lcb80_pct")
        pct = row.get("pct_chg")
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("rank")) if row.get("rank") is not None else "-",
                    str(row.get("symbol") or "-"),
                    clean_table_text(row.get("name") or "-", 22),
                    board_cell,
                    fmt_num(row.get("rank_score"), 2),
                    fmt_pct(pct) if pct is not None else "-",
                    fmt_pct(ev) if ev is not None else "-",
                    str(row.get("size_hint") or "0R"),
                    clean_table_text(str(row.get("reason") or "-"), 60),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_cn_ranked_watch.py -q`
Expected: PASS (5 passed), output pristine.

- [ ] **Step 5: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add scripts/sections/cn_ranked_watch.py tests/test_cn_ranked_watch.py
git commit -m "feat(cn): 0R Ranked Watch radar section (active_watch tier, 科创板 ★-flagged)"
```

---

### Task 2: Wire section into CN report + narrator slice list (+ smoke)

**Files:**
- Modify: `scripts/reports/cn_daily.py` (import block lines 6-18; insertion after line 81 `render_market_selection_rationale(...)`, before line 82 `render_cn_left_side_watch_section`)
- Modify: `scripts/agents/run_cn_narrator.py` (`_STRUCTURAL_HEADERS`, lines 48-50)
- Test: `tests/test_cn_ranked_watch_wiring.py`

**Interfaces:**
- Consumes: `render_cn_ranked_watch_radar_section(payload)` from Task 1.
- Produces: `cn_daily_report.md` containing a `## 0R 观察雷达 (Ranked Watch)` section; `run_cn_narrator._STRUCTURAL_HEADERS` containing `"0R 观察雷达"`.

- [ ] **Step 1: Write the failing wiring test** — create `tests/test_cn_ranked_watch_wiring.py`:

```python
"""Wiring tests: radar section is rendered by cn_daily and sliced by the CN narrator."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


class CnRankedWatchWiringTests(unittest.TestCase):
    def test_cn_daily_imports_and_inserts_radar(self) -> None:
        src = (STACK_ROOT / "scripts" / "reports" / "cn_daily.py").read_text(encoding="utf-8")
        self.assertIn("from sections.cn_ranked_watch import render_cn_ranked_watch_radar_section", src)
        self.assertIn("render_cn_ranked_watch_radar_section(payload)", src)

    def test_narrator_structural_headers_include_radar_key(self) -> None:
        src = (STACK_ROOT / "scripts" / "agents" / "run_cn_narrator.py").read_text(encoding="utf-8")
        m = re.search(r"_STRUCTURAL_HEADERS\s*=\s*\[(.*?)\]", src, re.DOTALL)
        self.assertIsNotNone(m, "_STRUCTURAL_HEADERS list not found")
        self.assertIn("0R 观察雷达", m.group(1))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_cn_ranked_watch_wiring.py -q`
Expected: FAIL — both assertions miss (import line and slice key not yet added).

- [ ] **Step 3: Add the import to `scripts/reports/cn_daily.py`** — after line 18 (`from sections.selection_rationale import render_market_selection_rationale`) add:

```python
from sections.cn_ranked_watch import render_cn_ranked_watch_radar_section
```

- [ ] **Step 4: Insert the section render in `scripts/reports/cn_daily.py`** — locate:

```python
    lines += render_market_selection_rationale(payload, actions, "CN")
    lines += render_cn_left_side_watch_section(payload)
```

and insert the radar line between them:

```python
    lines += render_market_selection_rationale(payload, actions, "CN")
    lines += render_cn_ranked_watch_radar_section(payload)
    lines += render_cn_left_side_watch_section(payload)
```

- [ ] **Step 5: Add the slice key in `scripts/agents/run_cn_narrator.py`** — change `_STRUCTURAL_HEADERS` (lines 48-50). Current:

```python
_STRUCTURAL_HEADERS = [
    "概率最优", "可交易名单", "逐票复核", "左侧观察池", "CN Realized Horizon Edge",
]
```

to:

```python
_STRUCTURAL_HEADERS = [
    "概率最优", "可交易名单", "逐票复核", "0R 观察雷达", "左侧观察池", "CN Realized Horizon Edge",
]
```

- [ ] **Step 6: Run the wiring test to verify it passes**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_cn_ranked_watch_wiring.py -q`
Expected: PASS (2 passed).

- [ ] **Step 7: Run the full radar test file to confirm no regression**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_cn_ranked_watch.py tests/test_cn_ranked_watch_wiring.py tests/test_cn_left_side_watch.py -q`
Expected: PASS (all), output pristine.

- [ ] **Step 8: Smoke — regenerate CN report (no email) and confirm 科创板 in the section**

Run (base python3 has tushare/duckdb; test mode, writes review_dashboard only, sends nothing):
```bash
cd /home/ivena/coding/quant-stack
python3 scripts/generate_main_strategy_v2_report.py --date 2026-06-26 --ai-infra-mode enforce_expand
grep -n "## 0R 观察雷达" reports/review_dashboard/main_strategy_v2/2026-06-26/cn_daily_report.md
grep -oE "688[0-9]{3}" reports/review_dashboard/main_strategy_v2/2026-06-26/cn_daily_report.md | sort -u
```
Expected: the header line is found; 688 names include `688233` and `688535` (the two 06-26 active_watch 科创板). If the section renders the empty-pool placeholder instead, STOP and report — it means active_watch was empty for that date (re-check ranker output), do not force.

- [ ] **Step 9: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add scripts/reports/cn_daily.py scripts/agents/run_cn_narrator.py tests/test_cn_ranked_watch_wiring.py
git commit -m "feat(cn): wire 0R Ranked Watch radar into CN report + narrator slice list"
```

---

## Self-Review

- **Spec coverage:**
  - 组件1 renderer → Task 1 (module + tests). Filter `production_tier=="active_watch"`, rank-asc, 科创板 ★, empty placeholder, RADAR_LIMIT=20 — all in Task 1 Step 3.
  - 组件2 接入主体 → Task 2 Steps 3-4 (import + insertion point exactly between 逐票复核 rationale and 左侧观察池).
  - 组件3 接入叙事器切片 → Task 2 Step 5 (`"0R 观察雷达"` into `_STRUCTURAL_HEADERS`).
  - 测试(单测 + 切片回归 + 冒烟) → Task 1 Step 1, Task 2 Step 1, Task 2 Step 8.
  - "不做"(不碰分档/执行/证据门/R) → honored; no such file is modified.
  - **Deviation from spec:** spec listed a `5D` column (`ret_5d`); the plan DROPS it. Reason: `all_rows` exposes `ret_5d` (raw) not `ret_5d_pct`, and its unit is ambiguous vs `pct_chg`; including it risks a wrong-magnitude display. `1D`(`pct_chg`) + `Score` + `EV LCB80` already serve "0R visibility". This is a deliberate YAGNI/correctness trim, not a gap.
- **Placeholder scan:** none — every step has concrete code/commands and expected output.
- **Type consistency:** `render_cn_ranked_watch_radar_section(payload) -> list[str]`, `cn_ranked_watch_rows(payload) -> list[dict]`, `board_label(symbol) -> str`, slice key `"0R 观察雷达"`, header `## 0R 观察雷达 (Ranked Watch)` — identical across Task 1, Task 2, and both test files.
- **Optional narrator prompt nudge** (spec 组件3 可选项) intentionally omitted from this plan (YAGNI; slicing already delivers the data to the payload). Can be added later if the narrator omits 科创板 in practice.
