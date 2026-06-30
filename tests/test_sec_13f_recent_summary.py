from __future__ import annotations

import importlib.util
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sec_13f_recent_summary.py"


def load_module():
    spec = importlib.util.spec_from_file_location("sec_13f_recent_summary", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_13f(path: Path, rows: list[tuple[str, str, int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(
        f"""
  <infoTable>
    <nameOfIssuer>{issuer}</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>{cusip}</cusip>
    <value>{value_thousands}</value>
    <shrsOrPrnAmt><sshPrnamt>{shares}</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
  </infoTable>"""
        for issuer, cusip, value_thousands, shares in rows
    )
    path.write_text(f"<informationTable>{body}\n</informationTable>\n", encoding="utf-8")


def test_recent_13f_summary_compares_previous_manager_filing(tmp_path: Path) -> None:
    module = load_module()
    manager = tmp_path / "BRK"
    previous_dir = manager / "0001"
    current_dir = manager / "0002"
    previous = previous_dir / "infotable.xml"
    current = current_dir / "infotable.xml"
    metadata = {
        "company": {"title": "BERKSHIRE HATHAWAY INC"},
        "cik": "0001067983",
        "form": "13F-HR",
        "filing_date": "2026-06-30",
        "report_date": "2026-03-31",
    }
    (previous_dir / "metadata.json").parent.mkdir(parents=True, exist_ok=True)
    (previous_dir / "metadata.json").write_text(module.json.dumps({**metadata, "accession_number": "0001"}), encoding="utf-8")
    (current_dir / "metadata.json").parent.mkdir(parents=True, exist_ok=True)
    (current_dir / "metadata.json").write_text(module.json.dumps({**metadata, "accession_number": "0002"}), encoding="utf-8")
    write_13f(previous, [("APPLE INC", "037833100", 100, 10), ("OLD CO", "000000001", 50, 5)])
    write_13f(current, [("APPLE INC", "037833100", 175, 12), ("NEW AI CO", "999999999", 200, 8)])

    old_ts = time.time() - 30 * 3600
    recent_ts = time.time() - 3600
    os.utime(previous, (old_ts, old_ts))
    os.utime(previous_dir / "metadata.json", (old_ts, old_ts))
    os.utime(current, (recent_ts, recent_ts))
    os.utime(current_dir / "metadata.json", (recent_ts, recent_ts))

    payload = module.summarize_recent([tmp_path], lookback_hours=12, limit_filings=4, top_n=5)

    assert payload["recent_file_count"] == 1
    filing = payload["filings"][0]
    assert filing["manager"] == "BERKSHIRE HATHAWAY INC"
    assert filing["baseline_found"] is True
    assert filing["new_positions_top5"][0]["issuer"] == "NEW AI CO"
    assert filing["increases_top5"][0]["issuer"] == "APPLE INC"
    assert filing["increases_top5"][0]["value_delta_usd"] == 75_000.0
    assert filing["decreases_top5"][0]["issuer"] == "OLD CO"
    assert filing["decreases_top5"][0]["value_delta_usd"] == -50_000.0


def test_recent_13f_markdown_uses_top5_labels(tmp_path: Path) -> None:
    module = load_module()
    current_dir = tmp_path / "MANAGER" / "0002"
    current = current_dir / "form13fInfoTable.xml"
    (current_dir / "metadata.json").parent.mkdir(parents=True, exist_ok=True)
    (current_dir / "metadata.json").write_text(
        module.json.dumps({"company": "TEST MANAGER", "cik": "1", "accession_number": "0002"}),
        encoding="utf-8",
    )
    write_13f(current, [("NEW AI CO", "999999999", 200, 8)])

    payload = module.summarize_recent([tmp_path], lookback_hours=12, limit_filings=4, top_n=5)
    text = module.render_markdown(payload)

    assert "SEC 13F 机构持仓快照" in text
    assert "TEST MANAGER" in text
    assert "NEW AI CO" in text
