"""Tests for the options anomaly alerts maintainer + queue annotator."""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "maintain_options_anomaly_alerts.py"


def _load_module():
    if "maintain_options_anomaly_alerts" in sys.modules:
        return sys.modules["maintain_options_anomaly_alerts"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("maintain_options_anomaly_alerts", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RADAR_HEADERS = [
    "symbol", "as_of", "spot_close",
    "far_otm_call_volume", "far_otm_call_oi", "far_otm_call_vol_oi_ratio",
    "far_otm_put_volume", "far_otm_put_oi", "far_otm_put_vol_oi_ratio",
    "pc_ratio_raw", "pc_ratio_z", "skew_z",
    "short_squeeze_score", "selling_pressure_score",
]

QUEUE_HEADERS = [
    "rank", "priority_tier", "ticker", "company", "market_country", "asset_pool",
    "bfs_depth", "module", "current_pool", "total_score", "score_bucket",
    "verification_status", "source_priority", "primary_sources_to_find",
    "metrics_to_verify", "upgrade_conditions", "downgrade_conditions",
    "evidence_state", "counterevidence", "dependency_path", "dependency_edge",
    "etf_clue", "smart_money_clue",
]


def _write_radar(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RADAR_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in RADAR_HEADERS})


def _write_queue(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUEUE_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in QUEUE_HEADERS})


def _radar_row(symbol: str, *, squeeze: float = 0.0, pressure: float = 0.0,
                call_vol: int = 0, put_vol: int = 0,
                call_voi: str | float | None = None, put_voi: str | float | None = None,
                as_of: str = "2026-05-13", spot: float = 100.0,
                pc_z: float | None = None, skew_z: float | None = None) -> dict[str, str]:
    return {
        "symbol": symbol, "as_of": as_of, "spot_close": str(spot),
        "far_otm_call_volume": str(call_vol), "far_otm_call_oi": "1000",
        "far_otm_call_vol_oi_ratio": "" if call_voi is None else str(call_voi),
        "far_otm_put_volume": str(put_vol), "far_otm_put_oi": "1000",
        "far_otm_put_vol_oi_ratio": "" if put_voi is None else str(put_voi),
        "pc_ratio_raw": "", "pc_ratio_z": "" if pc_z is None else str(pc_z),
        "skew_z": "" if skew_z is None else str(skew_z),
        "short_squeeze_score": str(squeeze), "selling_pressure_score": str(pressure),
    }


def _queue_row(ticker: str, counter: str = "default counter") -> dict[str, str]:
    return {
        "rank": "1", "priority_tier": "P2", "ticker": ticker, "company": f"{ticker} Inc",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D2", "module": "test", "current_pool": "候选池",
        "total_score": "75", "score_bucket": "radar_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "Find filings",
        "primary_sources_to_find": "10-K", "metrics_to_verify": "revenue",
        "upgrade_conditions": "AI demand", "downgrade_conditions": "drop",
        "evidence_state": "待原文核验", "counterevidence": counter,
        "dependency_path": "GPU -> X", "dependency_edge": "客户边",
        "etf_clue": "SMH", "smart_money_clue": "13F",
    }


class OptionsAlertsMaintainerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_collect_alerts_threshold_squeeze(self) -> None:
        with TemporaryDirectory() as tmp:
            radar = Path(tmp) / "radar.csv"
            _write_radar(radar, [
                _radar_row("STX", squeeze=124973, call_vol=10385, call_voi=11.69, pc_z=0.47, skew_z=-2.59),
                _radar_row("BORING", squeeze=200, call_vol=50, call_voi=0.1),  # below threshold
                _radar_row("TINY_VOL_OI", squeeze=1800, call_vol=200, call_voi=3.5),  # passes vol_oi gate
            ])
            alerts = self.module.collect_alerts(radar)
            tickers = {(a["ticker"], a["side"]) for a in alerts}
            self.assertIn(("STX", "squeeze"), tickers)
            self.assertIn(("TINY_VOL_OI", "squeeze"), tickers)
            self.assertNotIn(("BORING", "squeeze"), tickers)

    def test_collect_alerts_pressure_side(self) -> None:
        with TemporaryDirectory() as tmp:
            radar = Path(tmp) / "radar.csv"
            _write_radar(radar, [
                _radar_row("AAOI", pressure=5531, put_vol=12678, put_voi=0.87, pc_z=-0.20, skew_z=-0.91),
                _radar_row("MEH", pressure=200, put_vol=10, put_voi=0.1),
            ])
            alerts = self.module.collect_alerts(radar)
            sides = {(a["ticker"], a["side"]) for a in alerts}
            self.assertIn(("AAOI", "pressure"), sides)
            self.assertNotIn(("MEH", "pressure"), sides)

    def test_jsonl_merge_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            jsonl = Path(tmp) / "alerts.jsonl"
            alerts = [
                {"as_of": "2026-05-13", "ticker": "STX", "side": "squeeze", "score": 124973.0,
                 "vol": 10000, "oi": 1000, "vol_oi": 11.69, "pc_z": 0.4, "skew_z": -2.5, "spot": 800.0},
            ]
            self.module.merge_alerts_jsonl(jsonl, alerts)
            # Re-run: should not add a duplicate
            added, refreshed = self.module.merge_alerts_jsonl(jsonl, alerts)
            self.assertEqual((added, refreshed), (0, 0))
            with jsonl.open() as h:
                lines = [json.loads(line) for line in h if line.strip()]
            self.assertEqual(len(lines), 1)

    def test_queue_annotation_tag_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [
                _queue_row("STX", counter="HDD周期; AI relevance indirect"),
                _queue_row("OTHER", counter="competition pressure"),
            ])
            alerts = [{
                "as_of": "2026-05-13", "ticker": "STX", "side": "squeeze",
                "score": 124973.0, "vol": 10000, "oi": 1000, "vol_oi": 11.69,
                "pc_z": 0.4, "skew_z": -2.5, "spot": 800.0,
            }]
            self.module.annotate_queue(queue, alerts)
            with queue.open() as h:
                rows = {r["ticker"]: r for r in csv.DictReader(h)}
            stx = rows["STX"]
            self.assertIn("options-flow-alert 2026-05-13", stx["counterevidence"])
            self.assertIn("squeeze_score=124973", stx["counterevidence"])
            self.assertIn("HDD周期", stx["counterevidence"])  # base prose preserved
            self.assertEqual(rows["OTHER"]["counterevidence"], "competition pressure")
            # Re-run: tag should be replaced, not stacked
            self.module.annotate_queue(queue, alerts)
            with queue.open() as h:
                rows2 = {r["ticker"]: r for r in csv.DictReader(h)}
            tag_count = rows2["STX"]["counterevidence"].count("options-flow-alert 2026-05-13")
            self.assertEqual(tag_count, 1)

    def test_queue_annotation_combines_squeeze_and_pressure(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_queue_row("MU", counter="HBM cycle risk")])
            alerts = [
                {"as_of": "2026-05-13", "ticker": "MU", "side": "squeeze", "score": 40053.0,
                 "vol": 24913, "oi": 31681, "vol_oi": 0.79, "pc_z": -0.5, "skew_z": -1.7, "spot": 800.0},
                {"as_of": "2026-05-13", "ticker": "MU", "side": "pressure", "score": 3865.0,
                 "vol": 29876, "oi": 115466, "vol_oi": 0.26, "pc_z": -0.5, "skew_z": -1.7, "spot": 800.0},
            ]
            self.module.annotate_queue(queue, alerts)
            with queue.open() as h:
                rows = list(csv.DictReader(h))
            mu = rows[0]
            self.assertIn("squeeze_score=40053", mu["counterevidence"])
            self.assertIn("pressure_score=3865", mu["counterevidence"])
            self.assertEqual(mu["counterevidence"].count("options-flow-alert 2026-05-13"), 1)

    def test_queue_annotation_skips_unknown_tickers(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_queue_row("INQUEUE")])
            alerts = [{
                "as_of": "2026-05-13", "ticker": "NOTHERE", "side": "squeeze",
                "score": 50000.0, "vol": 5000, "oi": 1000, "vol_oi": 5.0,
                "pc_z": None, "skew_z": None, "spot": 50.0,
            }]
            touched_rows, touched_tickers = self.module.annotate_queue(queue, alerts)
            self.assertEqual((touched_rows, touched_tickers), (0, 0))


if __name__ == "__main__":
    unittest.main()
