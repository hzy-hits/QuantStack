"""Tests for the AI-infra strategy backtest — stats + honest verdict."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_ai_infra_strategy_backtest.py"


def _load_module():
    if "run_ai_infra_strategy_backtest" in sys.modules:
        return sys.modules["run_ai_infra_strategy_backtest"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("run_ai_infra_strategy_backtest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class StatsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_equity_compounds(self) -> None:
        eq = self.m._equity([0.1, -0.05, 0.2])
        self.assertAlmostEqual(eq[-1], 1.1 * 0.95 * 1.2, places=6)

    def test_stats_total_return(self) -> None:
        s = self.m._stats([0.1, 0.1])
        self.assertAlmostEqual(s["total_return_pct"], 21.0, places=1)

    def test_stats_max_drawdown_is_negative(self) -> None:
        # up 20%, then down 30% → drawdown from the peak.
        s = self.m._stats([0.2, -0.3])
        self.assertLess(s["max_drawdown_pct"], 0.0)
        self.assertAlmostEqual(s["max_drawdown_pct"], -30.0, places=1)

    def test_stats_empty(self) -> None:
        self.assertEqual(self.m._stats([])["n"], 0)


class VerdictTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def _sim(self, gated_sharpe, naive_sharpe, gated_dd=-20.0, naive_dd=-25.0):
        return {
            "gated_regime_overlay": {"n": 100, "sharpe": gated_sharpe,
                                     "max_drawdown_pct": gated_dd},
            "naive_always_hold": {"n": 100, "sharpe": naive_sharpe,
                                  "max_drawdown_pct": naive_dd},
        }

    def test_higher_sharpe_is_valuable(self) -> None:
        v = self.m._verdict(self._sim(1.6, 1.4))
        self.assertIn("有价值", v)

    def test_lower_sharpe_is_honestly_flagged(self) -> None:
        # The real backtest case: overlay cuts drawdown but lowers Sharpe.
        v = self.m._verdict(self._sim(1.17, 1.42))
        self.assertIn("降低了夏普", v)
        self.assertIn("过度防御", v)

    def test_flat_sharpe_big_dd_cut_is_acceptable(self) -> None:
        v = self.m._verdict(self._sim(1.40, 1.42, gated_dd=-12.0, naive_dd=-31.0))
        self.assertIn("可接受", v)

    def test_insufficient_data(self) -> None:
        v = self.m._verdict({"gated_regime_overlay": {"n": 0},
                             "naive_always_hold": {"n": 0}})
        self.assertIn("数据不足", v)


class MembershipLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def _snaps(self):
        from datetime import date
        return [
            (date(2026, 1, 10), frozenset({"A", "B"})),
            (date(2026, 2, 20), frozenset({"A", "C"})),
        ]

    def test_members_before_ledger_is_proxy_not_pit_clean(self) -> None:
        from datetime import date
        members, clean = self.m._members_on(self._snaps(), date(2025, 6, 1))
        self.assertFalse(clean)              # pre-ledger → proxy
        self.assertEqual(members, frozenset({"A", "C"}))  # latest snapshot

    def test_members_on_snapshot_date_is_pit_clean(self) -> None:
        from datetime import date
        members, clean = self.m._members_on(self._snaps(), date(2026, 1, 10))
        self.assertTrue(clean)
        self.assertEqual(members, frozenset({"A", "B"}))

    def test_members_between_snapshots_uses_earlier(self) -> None:
        from datetime import date
        members, clean = self.m._members_on(self._snaps(), date(2026, 2, 1))
        self.assertTrue(clean)
        self.assertEqual(members, frozenset({"A", "B"}))

    def test_members_after_last_snapshot(self) -> None:
        from datetime import date
        members, clean = self.m._members_on(self._snaps(), date(2026, 5, 1))
        self.assertTrue(clean)
        self.assertEqual(members, frozenset({"A", "C"}))

    def test_empty_ledger(self) -> None:
        from datetime import date
        members, clean = self.m._members_on([], date(2026, 1, 1))
        self.assertEqual(members, frozenset())
        self.assertFalse(clean)

    def test_load_membership_parses_and_filters(self) -> None:
        import tempfile
        from pathlib import Path
        content = (
            "# header comment\n"
            '{"snapshot_date":"2026-01-10","market":"US","pool":"production","symbols":["A","B"]}\n'
            '{"snapshot_date":"2026-02-20","market":"US","pool":"production","symbols":["A","C"]}\n'
            '{"snapshot_date":"2026-01-10","market":"CN","pool":"production","symbols":["X"]}\n'
            '{"snapshot_date":"2026-01-10","market":"US","pool":"research","symbols":["Z"]}\n'
        )
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "ledger.jsonl"
            p.write_text(content, encoding="utf-8")
            us = self.m._load_membership(p, "US")
        self.assertEqual(len(us), 2)             # research row excluded
        self.assertEqual(us[0][1], frozenset({"A", "B"}))


if __name__ == "__main__":
    unittest.main()
