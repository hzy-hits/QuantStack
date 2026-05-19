"""Tests for the shared radar I/O harness."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "lib" / "radar_io.py"


def _load_module():
    if "radar_io" in sys.modules:
        return sys.modules["radar_io"]
    spec = importlib.util.spec_from_file_location("radar_io", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["radar_io"] = module
    spec.loader.exec_module(module)
    return module


class RadarIoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_resolve_as_of_explicit(self) -> None:
        d, text = self.m.resolve_as_of("2026-05-19")
        self.assertEqual(d, date(2026, 5, 19))
        self.assertEqual(text, "2026-05-19")

    def test_resolve_as_of_default_is_today_cst(self) -> None:
        d, text = self.m.resolve_as_of(None)
        self.assertIsInstance(d, date)
        self.assertEqual(d.isoformat(), text)

    def test_resolve_as_of_rejects_bad_date(self) -> None:
        with self.assertRaises(ValueError):
            self.m.resolve_as_of("not-a-date")

    def test_write_radar_outputs_creates_json_and_md(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = self.m.write_radar_outputs(
                root, "2026-05-19", "demo_radar",
                {"as_of": "2026-05-19", "score": 7}, "# demo\n",
            )
            self.assertEqual(out_dir, root / "2026-05-19")
            payload = json.loads((out_dir / "demo_radar.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["score"], 7)
            self.assertEqual((out_dir / "demo_radar.md").read_text(encoding="utf-8"), "# demo\n")

    def test_write_radar_outputs_json_is_sorted_unicode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = self.m.write_radar_outputs(
                Path(tmp), "2026-05-19", "demo", {"b": 1, "a": "中文"}, "x",
            )
            text = (out_dir / "demo.json").read_text(encoding="utf-8")
            self.assertIn("中文", text)                 # ensure_ascii=False
            self.assertLess(text.index('"a"'), text.index('"b"'))  # sort_keys


if __name__ == "__main__":
    unittest.main()
