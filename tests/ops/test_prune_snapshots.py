import datetime
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "prune_snapshots", Path(__file__).resolve().parents[2] / "ops" / "prune_snapshots.py"
)
prune = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prune)


def test_classify_keeps_recent_deletes_old_ignores_nonsnapshots():
    today = datetime.date(2026, 6, 25)
    names = [
        "quant_research_2026-06-24_pre.duckdb",   # 1 day old -> keep
        "quant_report_2026-06-24_post.duckdb",    # keep
        "quant_research_2026-06-10_post.duckdb",  # 15 days -> delete
        "quant_report_2026-06-01_pre.duckdb",     # delete
        "quant.duckdb",                           # canonical -> ignored
        "quant_report.duckdb",                    # canonical -> ignored
        "random_notes.txt",                       # ignored
    ]
    keep, delete = prune.classify_snapshots(names, today=today, keep_days=7)
    assert set(keep) == {
        "quant_research_2026-06-24_pre.duckdb",
        "quant_report_2026-06-24_post.duckdb",
    }
    assert set(delete) == {
        "quant_research_2026-06-10_post.duckdb",
        "quant_report_2026-06-01_pre.duckdb",
    }
    # canonical + non-snapshot never appear
    for n in ("quant.duckdb", "quant_report.duckdb", "random_notes.txt"):
        assert n not in keep and n not in delete


def test_boundary_exactly_keep_days_is_kept():
    today = datetime.date(2026, 6, 25)
    names = ["quant_research_2026-06-18_pre.duckdb"]  # exactly 7 days
    keep, delete = prune.classify_snapshots(names, today=today, keep_days=7)
    assert keep == ["quant_research_2026-06-18_pre.duckdb"]
    assert delete == []
