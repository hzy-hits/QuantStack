from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path


STACK_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_cn_book_overlay.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_cn_book_overlay", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


overlay = _load_module()


class CnBookOverlayTests(unittest.TestCase):
    def test_overlay_surfaces_manual_micro_probe_without_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            v2_root = root / "main_strategy_v2"
            out_root = root / "cn_book_overlay"
            report_dir = v2_root / "2026-05-01"
            report_dir.mkdir(parents=True)
            (report_dir / "main_strategy_v2_backtest.json").write_text(
                json.dumps(
                    {
                        "as_of": "2026-05-01",
                        "cn": {
                            "current": [
                                {
                                    "symbol": "000892.SZ",
                                    "name": "欢瑞世纪",
                                    "state": "Execution Alpha",
                                    "observation_entry_zone": "4.91-4.94",
                                    "handling_line": 4.83,
                                    "first_target": 5.00,
                                    "time_exit": "T+1 review; hard max T+5",
                                    "lifecycle_action": "manual_probe_only_after_pullback; no open chase",
                                }
                            ]
                        },
                        "portfolio_risk_overlay": {
                            "rows": [
                                {
                                    "market": "CN",
                                    "symbol": "000892.SZ",
                                    "final_r": 0.05,
                                    "manual_probe_r": 0.05,
                                    "auto_eligible": False,
                                    "risk_reasons": ["cn_shadow_option_zero", "cn_manual_micro_probe_override"],
                                    "shadow_option_haircut": 0.0,
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            payload = overlay.run(
                Namespace(
                    date="2026-05-01",
                    activity_csv=None,
                    v2_root=v2_root,
                    output_root=out_root,
                    max_hold_days=5,
                )
            )
            self.assertEqual(payload["source_status"], "missing_activity_csv")
            self.assertEqual(payload["summary"]["manual_micro_probe_ready"], 1)
            self.assertEqual(payload["rows"][0]["action"], "manual_micro_probe_planned_entry_only")
            self.assertTrue((out_root / "2026-05-01" / "cn_book_overlay.md").exists())
            self.assertTrue((out_root / "2026-05-01" / "cn_book_overlay.json").exists())


if __name__ == "__main__":
    unittest.main()
