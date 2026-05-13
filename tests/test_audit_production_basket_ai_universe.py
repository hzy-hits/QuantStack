"""Test the production-basket AI universe audit script."""
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = STACK_ROOT / "scripts" / "audit_production_basket_ai_universe.py"


def _write_payload(date_dir: Path, filename: str, payload: dict) -> None:
    date_dir.mkdir(parents=True, exist_ok=True)
    (date_dir / filename).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _run(as_of: str, root: Path, *, strict: bool = False) -> subprocess.CompletedProcess[str]:
    args = [sys.executable, str(SCRIPT), "--as-of", as_of, "--dashboard-root", str(root)]
    if strict:
        args.append("--strict")
    return subprocess.run(args, capture_output=True, text=True, check=False)


class AuditProductionBasketTests(unittest.TestCase):
    def test_passes_when_all_rows_are_ai_universe(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            date_dir = root / "2026-05-13"
            payload = {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [
                    {"symbol": "NVDA", "ai_infra_universe": True, "ai_infra_current_pool": "核心池"},
                ],
            }
            _write_payload(date_dir, "us_opportunity_ranker.json", payload)
            _write_payload(date_dir, "cn_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [],
            })

            result = _run("2026-05-13", root)
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

    def test_fails_when_non_ai_universe_row_in_basket(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            date_dir = root / "2026-05-13"
            _write_payload(date_dir, "us_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [{"symbol": "ZZZ"}],
            })
            _write_payload(date_dir, "cn_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [],
            })

            result = _run("2026-05-13", root)
            self.assertEqual(result.returncode, 1)
            self.assertIn("ZZZ", result.stdout)

    def test_fails_when_contract_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            date_dir = root / "2026-05-13"
            _write_payload(date_dir, "us_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "broad_market"},
                "production_basket": [],
            })
            _write_payload(date_dir, "cn_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [],
            })

            result = _run("2026-05-13", root)
            self.assertEqual(result.returncode, 1)
            self.assertIn("broad_market", result.stdout)

    def test_fails_when_dashboard_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            result = _run("2026-05-13", Path(tmp))
            self.assertEqual(result.returncode, 2)

    def test_strict_flags_non_ai_universe_row_in_all_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            date_dir = root / "2026-05-13"
            payload = {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [
                    {"symbol": "NVDA", "ai_infra_universe": True, "ai_infra_current_pool": "核心池"},
                ],
                "all_rows": [
                    {"symbol": "NVDA", "ai_infra_universe": True, "ai_infra_current_pool": "核心池"},
                    {"symbol": "DGXX", "ai_infra_universe": False},  # watch-only escaping gate
                ],
            }
            _write_payload(date_dir, "us_opportunity_ranker.json", payload)
            _write_payload(date_dir, "cn_opportunity_ranker.json", {
                "ai_infra_gate": {"contract": "ai_infra_universe_only"},
                "production_basket": [],
                "all_rows": [],
            })

            non_strict = _run("2026-05-13", root)
            self.assertEqual(non_strict.returncode, 0)
            strict = _run("2026-05-13", root, strict=True)
            self.assertEqual(strict.returncode, 1)
            self.assertIn("DGXX", strict.stdout)
            self.assertIn("watch/research-only", strict.stdout)


if __name__ == "__main__":
    unittest.main()
