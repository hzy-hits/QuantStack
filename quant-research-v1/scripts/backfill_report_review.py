#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quant_bot.analytics.report_review import refresh_report_review, store_report_decisions
from quant_bot.analytics.risk_params import compute_risk_params
from quant_bot.config.settings import Settings
from quant_bot.filtering.notable import build_notable_items
from quant_bot.reporting.bundle import build_report_bundle, compute_headline_gate
from quant_bot.signals.classify import classify_all
from quant_bot.storage.db import (
    connect_readonly,
    connect_write,
    copy_database,
    init_schema,
    snapshot_path,
)


REPORT_RE = re.compile(r"(?P<as_of>\d{4}-\d{2}-\d{2})_report_zh_(?P<session>pre|post)\.md$")
ANALYSIS_MODULES = ("momentum_risk", "earnings_risk", "mean_reversion", "breakout", "overnight_gate")


def _discover_sessions(reports_dir: Path, date_from: date | None, date_to: date | None) -> list[tuple[date, str]]:
    sessions: list[tuple[date, str]] = []
    for path in sorted(reports_dir.glob("*_report_zh_*.md")):
        m = REPORT_RE.match(path.name)
        if not m:
            continue
        as_of = datetime.strptime(m.group("as_of"), "%Y-%m-%d").date()
        if date_from and as_of < date_from:
            continue
        if date_to and as_of > date_to:
            continue
        sessions.append((as_of, m.group("session")))
    return sessions


def _source_db_for_session(cfg: Settings, as_of: date, session: str) -> Path:
    session_report = snapshot_path(cfg.report_db_path_abs, as_of, session)
    if session_report.exists():
        return session_report
    session_research = snapshot_path(cfg.active_research_db_path_abs, as_of, session)
    if session_research.exists():
        return session_research
    return cfg.raw_db_path_abs


def _load_symbols(con, as_of: date, benchmark: str) -> list[str]:
    as_of_str = as_of.strftime("%Y-%m-%d")
    placeholders = ",".join(f"'{m}'" for m in ANALYSIS_MODULES)
    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM analysis_daily
        WHERE date = ?
          AND module_name IN ({placeholders})
        ORDER BY symbol
        """,
        [as_of_str],
    ).fetchall()
    symbols = [r[0] for r in rows if r and r[0]]
    if not symbols:
        rows = con.execute(
            """
            SELECT DISTINCT symbol
            FROM prices_daily
            WHERE date <= ?
            ORDER BY symbol
            """,
            [as_of_str],
        ).fetchall()
        symbols = [r[0] for r in rows if r and r[0]]
    if benchmark not in symbols:
        symbols.append(benchmark)
    return symbols


def _sync_review_dbs(cfg: Settings, as_of: date, session: str) -> None:
    copy_database(cfg.raw_db_path_abs, cfg.report_db_path_abs)
    session_report = snapshot_path(cfg.report_db_path_abs, as_of, session)
    if session_report != cfg.report_db_path_abs:
        copy_database(cfg.raw_db_path_abs, session_report)


def backfill_session(cfg: Settings, as_of: date, session: str) -> tuple[int, dict]:
    source_db = _source_db_for_session(cfg, as_of, session)
    benchmark = cfg.universe.benchmark
    core_symbols = set(cfg.universe.watchlist or [])
    selection_policy = cfg.selection.model_dump()

    with connect_readonly(source_db) as source_con:
        symbols = _load_symbols(source_con, as_of, benchmark)
        ranked_items = build_notable_items(
            source_con,
            as_of,
            symbols,
            benchmark=benchmark,
            max_items=max(120, cfg.output.max_notable_items),
            core_symbols=core_symbols,
            selection_policy=selection_policy,
        )
        notable = ranked_items[: cfg.output.max_notable_items]
        bundle = build_report_bundle(
            source_con,
            as_of,
            notable,
            benchmark=benchmark,
            universe={},
            dividend_dip_screen=[],
        )
        classify_all(ranked_items, bundle.get("market_context"))
        for item in bundle["notable_items"]:
            sig_conf = (item.get("signal") or {}).get("confidence")
            if sig_conf in {"HIGH", "MODERATE"}:
                item["risk_params"] = compute_risk_params(item)
        headline_gate = compute_headline_gate(bundle)

    selected_symbols = {item["symbol"] for item in notable}
    with connect_write(cfg.raw_db_path_abs) as raw_con:
        n_rows = store_report_decisions(
            raw_con,
            as_of,
            session,
            ranked_items,
            selected_symbols,
            headline_gate,
        )
        review = refresh_report_review(raw_con, as_of, session)
        raw_con.execute("CHECKPOINT")
    _sync_review_dbs(cfg, as_of, session)
    return n_rows, review


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill US report review ledgers from existing sessions.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--session", choices=["pre", "post"], default=None)
    parser.add_argument("--max-sessions", type=int, default=None)
    args = parser.parse_args()

    cfg = Settings.load(args.config)
    init_schema(cfg.raw_db_path_abs)
    init_schema(cfg.report_db_path_abs)

    date_from = datetime.strptime(args.date_from, "%Y-%m-%d").date() if args.date_from else None
    date_to = datetime.strptime(args.date_to, "%Y-%m-%d").date() if args.date_to else None

    reports_dir = Path(__file__).resolve().parents[1] / "reports"
    sessions = _discover_sessions(reports_dir, date_from, date_to)
    if args.session:
        sessions = [s for s in sessions if s[1] == args.session]
    if args.max_sessions is not None:
        sessions = sessions[-args.max_sessions :]

    if not sessions:
        print("No matching report sessions found.")
        return

    print(f"Backfilling {len(sessions)} sessions...")
    for as_of, session in sessions:
        try:
            n_rows, review = backfill_session(cfg, as_of, session)
            print(
                f"  {as_of} {session}: decisions={n_rows}, "
                f"selected_reviewed={review.get('selected_reviewed', 0)}, "
                f"ignored_reviewed={review.get('ignored_reviewed', 0)}"
            )
        except Exception as exc:
            print(f"  {as_of} {session}: FAILED - {exc}")


if __name__ == "__main__":
    main()
