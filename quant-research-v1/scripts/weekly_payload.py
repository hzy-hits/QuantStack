#!/usr/bin/env python3
"""Generate weekly summary payload from DuckDB.

Aggregates the past week's daily data into a concise weekly payload for agent analysis.

Usage:
    python scripts/weekly_payload.py                      # current week
    python scripts/weekly_payload.py --date 2026-03-15    # week ending on this date
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import exchange_calendars as xcals

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quant_bot.config.settings import Settings
from quant_bot.storage.db import connect_readonly


XNYS_CALENDAR = xcals.get_calendar("XNYS")
DAILY_REPORT_SESSION = "post"
DAILY_REPORT_MIN_SIZE = 200
DAILY_SUMMARY_MAX_LINES = 12


def _fmt(v, d=2):
    if v is None:
        return "N/A"
    return f"{v:+.{d}f}" if d else f"{v:.0f}"


def _pct(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return (cur / prev - 1.0) * 100.0


def resolve_week_bounds(as_of: date) -> tuple[date, date]:
    week_start = as_of - timedelta(days=as_of.weekday())
    week_end = week_start + timedelta(days=4)
    return week_start, week_end


def week_trading_dates(as_of: date) -> list[date]:
    week_start, week_end = resolve_week_bounds(as_of)
    sessions = XNYS_CALENDAR.sessions_in_range(week_start.isoformat(), week_end.isoformat())
    return [session.date() for session in sessions]


def daily_report_path(reports_dir: Path, report_date: date, session: str = DAILY_REPORT_SESSION) -> Path:
    return reports_dir / f"{report_date.isoformat()}_report_zh_{session}.md"


def collect_weekly_daily_reports(
    as_of: date,
    reports_dir: Path,
    *,
    session: str = DAILY_REPORT_SESSION,
    require_complete: bool = True,
) -> tuple[list[date], list[Path], list[Path]]:
    trading_days = week_trading_dates(as_of)
    found: list[Path] = []
    missing: list[Path] = []
    for trading_day in trading_days:
        path = daily_report_path(reports_dir, trading_day, session=session)
        if path.exists() and path.stat().st_size >= DAILY_REPORT_MIN_SIZE:
            found.append(path)
        else:
            missing.append(path)

    if require_complete and missing:
        missing_block = "\n".join(f"- {path.name}" for path in missing)
        raise RuntimeError(
            "weekly report aborted: not all trading-day daily reports are ready.\n"
            f"required_session={session}\n"
            f"expected_reports={len(trading_days)}\n"
            f"found_reports={len(found)}\n"
            f"missing_reports:\n{missing_block}"
        )

    return trading_days, found, missing


def _extract_daily_report_summary(report_path: Path, *, max_lines: int = DAILY_SUMMARY_MAX_LINES) -> list[str]:
    text = report_path.read_text(encoding="utf-8", errors="replace")
    summary: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("## Factor Lab", "### Factor Lab", "**Factor Lab")):
            break
        if line.startswith("*AI分析"):
            break
        if line.startswith("---"):
            continue
        summary.append(line)
        if len(summary) >= max_lines:
            break
    return summary


def build_weekly_daily_digest(
    as_of: date,
    reports_dir: Path,
    *,
    session: str = DAILY_REPORT_SESSION,
    require_complete: bool = True,
) -> list[str]:
    trading_days, found_reports, missing_reports = collect_weekly_daily_reports(
        as_of,
        reports_dir,
        session=session,
        require_complete=require_complete,
    )

    lines = [
        "## 0. Daily Report Coverage",
        "",
        f"- Required daily reports for this trading week ({session}-market): {len(trading_days)}",
        f"- Reports found: {len(found_reports)}/{len(trading_days)}",
        f"- Trading days covered: {', '.join(day.isoformat() for day in trading_days)}",
    ]
    if missing_reports:
        lines.append(
            "- Missing reports: "
            + ", ".join(path.name for path in missing_reports)
        )
    lines += ["", "## 0a. Daily Report Digest", ""]

    for report_path in found_reports:
        report_date = report_path.stem.split("_report_zh_")[0]
        lines.append(f"### {report_date} — {report_path.name}")
        lines.append("")
        for summary_line in _extract_daily_report_summary(report_path):
            lines.append(f"- {summary_line}")
        lines.append("")

    return lines


def generate_weekly_payload(
    as_of: date,
    db_path: Path,
    *,
    reports_dir: Path,
    require_daily_reports: bool = True,
) -> str:
    """Generate weekly payload markdown for the completed trading week containing as_of."""
    con = connect_readonly(db_path)

    week_start, week_end = resolve_week_bounds(as_of)

    week_start_str = week_start.isoformat()
    week_end_str = week_end.isoformat()
    prev_week_end = (week_start - timedelta(days=3)).isoformat()  # previous Friday

    lines = [
        f"# Weekly Research Summary — {week_start_str} to {week_end_str}",
        "",
        f"> **Generated:** {datetime.now().isoformat(timespec='minutes')}",
        "> **Cadence:** Weekly aggregation of daily pipeline data",
        "> **For the agent:** Synthesize the week's trends, not individual days.",
        "> Not financial advice. Research only.",
        "",
        "---",
        "",
    ]

    lines += build_weekly_daily_digest(
        week_end,
        reports_dir,
        require_complete=require_daily_reports,
    )
    lines += ["---", ""]

    # ── 1. Market Indices Weekly Performance ──────────────────────────────────
    lines += ["## 1. Market Indices — Weekly Performance", ""]
    indices = ["SPY", "QQQ", "IWM", "DIA", "^VIX"]
    lines += [
        "| Symbol | Mon Close | Fri Close | Weekly Δ% | Week High | Week Low |",
        "|--------|-----------|-----------|-----------|-----------|---------|",
    ]
    for sym in indices:
        rows = con.execute("""
            SELECT date, adj_close, high, low
            FROM prices_daily
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [sym, week_start_str, week_end_str]).fetchall()
        if not rows:
            lines.append(f"| {sym} | N/A | N/A | N/A | N/A | N/A |")
            continue
        mon_close = rows[0][1]
        fri_close = rows[-1][1]
        wk_high = max(r[2] for r in rows if r[2])
        wk_low = min(r[3] for r in rows if r[3])
        wk_ret = _pct(fri_close, mon_close)
        lines.append(
            f"| {sym} | {_fmt(mon_close)} | {_fmt(fri_close)} "
            f"| {_fmt(wk_ret)}% | {_fmt(wk_high)} | {_fmt(wk_low)} |"
        )
    lines += [""]

    # ── 2. Sector ETF Weekly Returns ──────────────────────────────────────────
    lines += ["## 2. Sector Performance", ""]
    sector_syms = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLU", "XLRE", "XLY", "XLP", "XLB", "XLC"]
    lines += ["| Sector | Weekly Δ% |", "|--------|----------|"]
    sector_data = []
    for sym in sector_syms:
        row = con.execute("""
            WITH ends AS (
                SELECT adj_close,
                       ROW_NUMBER() OVER (ORDER BY date ASC) AS rn_asc,
                       ROW_NUMBER() OVER (ORDER BY date DESC) AS rn_desc
                FROM prices_daily
                WHERE symbol = ? AND date >= ? AND date <= ?
            )
            SELECT
                (SELECT adj_close FROM ends WHERE rn_asc = 1) AS mon_close,
                (SELECT adj_close FROM ends WHERE rn_desc = 1) AS fri_close
        """, [sym, week_start_str, week_end_str]).fetchone()
        if row and row[0] and row[1]:
            ret = _pct(row[1], row[0])
            sector_data.append((sym, ret))
        else:
            sector_data.append((sym, None))
    sector_data.sort(key=lambda x: x[1] if x[1] is not None else -999, reverse=True)
    for sym, ret in sector_data:
        lines.append(f"| {sym} | {_fmt(ret)}% |")
    lines += [""]

    # ── 3. Top Movers ─────────────────────────────────────────────────────────
    lines += ["## 3. Top Weekly Movers (S&P 500 + Universe)", ""]
    movers = con.execute("""
        WITH week_returns AS (
            SELECT p1.symbol,
                   p2.adj_close / p1.adj_close - 1.0 AS weekly_ret
            FROM (
                SELECT symbol, adj_close
                FROM prices_daily
                WHERE date = (SELECT MAX(date) FROM prices_daily WHERE date <= ? AND adj_close IS NOT NULL)
                  AND adj_close IS NOT NULL AND adj_close > 1.0
            ) p1
            JOIN (
                SELECT symbol, adj_close
                FROM prices_daily
                WHERE date = (SELECT MAX(date) FROM prices_daily WHERE date <= ? AND adj_close IS NOT NULL)
                  AND adj_close IS NOT NULL
            ) p2 ON p1.symbol = p2.symbol
            WHERE p1.adj_close > 0
        )
        SELECT symbol, weekly_ret * 100 AS pct
        FROM week_returns
        ORDER BY weekly_ret DESC
    """, [prev_week_end, week_end_str]).fetchall()

    if movers:
        lines += ["### Top 10 Gainers", ""]
        lines += ["| Symbol | Weekly Δ% |", "|--------|----------|"]
        for sym, pct in movers[:10]:
            lines.append(f"| {sym} | {_fmt(pct)}% |")
        lines += ["", "### Top 10 Losers", ""]
        lines += ["| Symbol | Weekly Δ% |", "|--------|----------|"]
        for sym, pct in movers[-10:][::-1]:
            lines.append(f"| {sym} | {_fmt(pct)}% |")
        lines += [""]

    # ── 4. HMM Regime Summary ─────────────────────────────────────────────────
    lines += ["## 4. HMM Regime — Week Summary", ""]
    hmm_rows = con.execute("""
        SELECT date, module_name, json_extract_string(details, '$.regime') AS regime,
               CAST(json_extract(details, '$.p_bull') AS DOUBLE) AS p_bull,
               CAST(json_extract(details, '$.days_in_current') AS INTEGER) AS days
        FROM analysis_daily
        WHERE module_name = 'hmm_regime' AND date >= ? AND date <= ?
        ORDER BY date ASC
    """, [week_start_str, week_end_str]).fetchall()
    if hmm_rows:
        lines += ["| Date | Regime | P(bull) | Days in Regime |",
                  "|------|--------|---------|----------------|"]
        for r in hmm_rows:
            lines.append(f"| {r[0]} | {r[2] or '?'} | {_fmt(r[3], 3)} | {r[4] or '?'} |")
        # Regime transitions
        regimes = [r[2] for r in hmm_rows if r[2]]
        transitions = sum(1 for i in range(1, len(regimes)) if regimes[i] != regimes[i-1])
        lines += ["", f"Regime transitions this week: {transitions}"]
    else:
        lines += ["No HMM data for this week."]
    lines += [""]

    # ── 5. Macro Data ─────────────────────────────────────────────────────────
    lines += ["## 5. Macro Snapshot", ""]
    macro = con.execute("""
        SELECT series_id, value, date
        FROM macro_daily
        WHERE date = (SELECT MAX(date) FROM macro_daily WHERE date <= ?)
    """, [week_end_str]).fetchall()
    if macro:
        lines += ["| Series | Value | As-of |", "|--------|-------|-------|"]
        for sid, val, dt in macro:
            lines.append(f"| {sid} | {_fmt(val, 4)} | {dt} |")
    lines += [""]

    # ── 6. Polymarket Weekly Δ ────────────────────────────────────────────────
    lines += ["## 6. Polymarket — Weekly Probability Changes", ""]
    poly = con.execute("""
        WITH week_start AS (
            SELECT DISTINCT ON (market_id) market_id, p_yes AS p_start, question
            FROM polymarket_events
            WHERE fetch_date <= ? AND p_yes IS NOT NULL AND volume_usd >= 10000
            ORDER BY market_id, fetch_date ASC
        ),
        week_end AS (
            SELECT DISTINCT ON (market_id) market_id, p_yes AS p_end
            FROM polymarket_events
            WHERE fetch_date <= ? AND p_yes IS NOT NULL
            ORDER BY market_id, fetch_date DESC
        )
        SELECT ws.question, ws.p_start, we.p_end, (we.p_end - ws.p_start) AS delta
        FROM week_start ws
        JOIN week_end we ON ws.market_id = we.market_id
        WHERE ABS(we.p_end - ws.p_start) > 0.01
        ORDER BY ABS(delta) DESC
        LIMIT 10
    """, [week_start_str, week_end_str]).fetchall()
    if poly:
        lines += ["| Question | Week Start | Week End | Δ |",
                  "|----------|------------|----------|---|"]
        for q, ps, pe, d in poly:
            lines.append(f"| {q[:70]} | {_fmt(ps, 3)} | {_fmt(pe, 3)} | {_fmt(d, 3)} |")
    else:
        lines += ["No significant Polymarket probability changes this week."]
    lines += [""]

    # ── 7. S&P 500 Breadth ────────────────────────────────────────────────────
    lines += ["## 7. Market Breadth", ""]
    breadth = con.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE p2.adj_close > p1.adj_close) AS up,
            COUNT(*) FILTER (WHERE p2.adj_close < p1.adj_close) AS down
        FROM (
            SELECT symbol, adj_close FROM prices_daily
            WHERE date = (SELECT MAX(date) FROM prices_daily WHERE date <= ?)
              AND adj_close IS NOT NULL AND adj_close > 1.0
        ) p1
        JOIN (
            SELECT symbol, adj_close FROM prices_daily
            WHERE date = (SELECT MAX(date) FROM prices_daily WHERE date <= ?)
              AND adj_close IS NOT NULL
        ) p2 ON p1.symbol = p2.symbol
    """, [prev_week_end, week_end_str]).fetchone()
    if breadth:
        total, up, down = breadth
        lines += [
            f"- Universe scanned: {total}",
            f"- Weekly advancers: {up} ({up/total*100:.0f}%)" if total else "",
            f"- Weekly decliners: {down} ({down/total*100:.0f}%)" if total else "",
        ]
    lines += ["", "---", ""]

    # ── 8. Agent Instructions ─────────────────────────────────────────────────
    lines += [
        "## Agent Instructions — Weekly Report",
        "",
        "You are writing a **weekly research summary**, not a daily report.",
        "Focus on:",
        "- **Trends over the week** — not individual day moves",
        "- **Regime changes** — did the HMM state shift?",
        "- **Sector rotation** — which sectors led/lagged and why",
        "- **Macro narrative** — any data releases that moved markets",
        "- **Polymarket sentiment shifts** — crowd probability changes",
        "- **Next week outlook** — upcoming events, earnings, risk factors",
        "",
        "Output in Chinese (中文). ~2000 characters.",
        "Format: Executive summary → Market review → Sector analysis → Macro → Outlook.",
        "",
    ]

    con.close()
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate weekly payload")
    parser.add_argument("--date", default=None, help="Week ending date (default: today)")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db", default=None, help="Override source DB (default: raw canonical DB from config)")
    parser.add_argument("--reports-dir", default="reports", help="Directory containing daily report markdown files")
    parser.add_argument(
        "--allow-missing-daily-reports",
        action="store_true",
        help="Allow weekly payload generation even if some trading-day daily reports are missing",
    )
    args = parser.parse_args()

    if args.date:
        parts = args.date.split("-")
        as_of = date(int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        as_of = date.today()

    cfg = Settings.load(args.config)
    db_path = Path(args.db) if args.db else cfg.raw_db_path_abs
    reports_dir = Path(args.reports_dir)

    payload = generate_weekly_payload(
        as_of,
        db_path,
        reports_dir=reports_dir,
        require_daily_reports=not args.allow_missing_daily_reports,
    )
    output = Path(f"reports/{as_of}_weekly_payload.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload)
    print(f"Weekly payload: {output} ({len(payload)} bytes)")


if __name__ == "__main__":
    main()
