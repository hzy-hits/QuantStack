from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from sections.congressional_trading import (  # noqa: E402
    build_congressional_trading_snapshot,
    render_congressional_trading_section,
)


class CongressionalTradingTests(unittest.TestCase):
    def _write_rows(self, root: Path, rows: list[dict]) -> None:
        day_dir = root / "2026-06-04"
        day_dir.mkdir(parents=True)
        (day_dir / "congressional_trading.json").write_text(
            json.dumps({"transactions": rows}, ensure_ascii=False),
            encoding="utf-8",
        )

    def test_multi_member_same_committee_buy_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_rows(
                root,
                [
                    {
                        "ticker": "MRVL",
                        "transaction_type": "Purchase",
                        "lawmaker": "Rep A",
                        "committee": "House AI Committee",
                        "transaction_date": "2026-05-01",
                        "disclosure_date": "2026-05-20",
                    },
                    {
                        "ticker": "MRVL",
                        "transaction_type": "Purchase",
                        "lawmaker": "Rep B",
                        "committee": "House AI Committee",
                        "transaction_date": "2026-05-12",
                        "disclosure_date": "2026-05-21",
                    },
                ],
            )

            snap = build_congressional_trading_snapshot(
                date(2026, 6, 4),
                artifact_root=root,
                ai_symbols={"MRVL"},
            )
            row = snap["rows"][0]

            self.assertEqual(row["symbol"], "MRVL")
            self.assertEqual(row["state"], "multi_member_buy_cluster")
            self.assertEqual(row["report_role"], "catalyst_watch")
            self.assertTrue(row["ai_universe_member"])
            self.assertEqual(snap["summary"]["multi_member_buy_clusters"], 1)

    def test_clustered_sells_are_risk_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_rows(
                root,
                [
                    {
                        "symbol": "NVDA",
                        "type": "Sale",
                        "representative": "Rep A",
                        "committee": "Defense",
                        "transaction_date": "2026-05-10",
                        "filed_date": "2026-06-01",
                    },
                    {
                        "symbol": "NVDA",
                        "type": "Sale",
                        "representative": "Rep B",
                        "committee": "Defense",
                        "transaction_date": "2026-05-20",
                        "filed_date": "2026-06-02",
                    },
                ],
            )

            snap = build_congressional_trading_snapshot(
                date(2026, 6, 4),
                artifact_root=root,
                ai_symbols={"NVDA"},
            )
            row = snap["rows"][0]

            self.assertEqual(row["state"], "clustered_sell_warning")
            self.assertEqual(row["report_role"], "risk_warning")
            self.assertLess(row["score"], 0)
            self.assertEqual(snap["summary"]["clustered_sell_warnings"], 1)

    def test_non_ai_symbol_stays_source_review_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_rows(
                root,
                [
                    {
                        "symbol": "XYZ",
                        "action": "Buy",
                        "lawmaker": "Rep A",
                        "committee": "Energy",
                        "transaction_date": "2026-05-28",
                        "disclosure_date": "2026-06-03",
                    }
                ],
            )

            snap = build_congressional_trading_snapshot(
                date(2026, 6, 4),
                artifact_root=root,
                ai_symbols={"NVDA"},
            )
            row = snap["rows"][0]

            self.assertEqual(row["state"], "fresh_disclosure_buy")
            self.assertEqual(row["report_role"], "source_review_candidate_only")
            self.assertFalse(row["ai_universe_member"])

    def test_missing_artifact_renders_no_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snap = build_congressional_trading_snapshot(
                date(2026, 6, 4),
                artifact_root=Path(tmp) / "missing",
                ai_symbols={"NVDA"},
            )
            text = "\n".join(render_congressional_trading_section({"congressional_trading": snap}))

            self.assertEqual(snap["status"], "no_data")
            self.assertIn("NO_CONGRESSIONAL_TRADING_DATA", text)
            self.assertIn("不改变 AI source evidence", text)


if __name__ == "__main__":
    unittest.main()
