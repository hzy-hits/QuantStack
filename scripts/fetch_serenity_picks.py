"""Pull Serenity Analysis daily picks → serenity_picks table.

Source: https://analysissite.vercel.app/mentions
  Server-side rendered Next.js page; data is embedded in RSC payload
  inside the HTML. No public API exists, so we extract by walking
  balanced brackets from each {"ticker":"..."} occurrence.

Schema captured per ticker:
  ticker, ai_chain_segment, stance, current_view, confidence,
  priority_score, priority_bucket, latest_return_pct,
  first_mentioned_at, last_mentioned_at, baseline_price,
  ret_1w, ret_1m, ret_6m, ret_1y (extracted from nested 'returns')

Use cases:
  1. Universe gap detector — what's on Serenity but not in our universe
  2. Cross-validation — our ranker rank vs Serenity priority_score
  3. Realized-return reality check — we predict NVTS rank #1, they record +372%

Run: python3 scripts/fetch_serenity_picks.py [--date 2026-05-27]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[1]
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"
URL = "https://analysissite.vercel.app/mentions"


def fetch_html() -> str:
    r = requests.get(
        URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; quant-stack/serenity-fetcher)"},
        timeout=30,
    )
    r.raise_for_status()
    return r.text


def parse_picks(html: str) -> list[dict]:
    """Walk RSC-encoded payload, extracting per-ticker JSON objects."""
    rows: list[dict] = []
    for m in re.finditer(r'\{\\"ticker\\":\\"([A-Z][A-Z0-9.-]{0,8})\\"', html):
        start = m.start()
        depth, in_str, esc, end = 0, False, False, None
        for i in range(start, min(start + 6000, len(html))):
            c = html[i]
            if esc: esc = False; continue
            if c == '\\': esc = True; continue
            if c == '"' and not esc: in_str = not in_str; continue
            if in_str: continue
            if c == '{': depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1; break
        if end is None: continue
        raw = html[start:end]
        try:
            rec = json.loads(raw.encode().decode("unicode_escape"))
            rows.append(rec)
        except json.JSONDecodeError:
            continue
    # Dedupe by ticker (page repeats rows in multiple sections)
    seen, out = set(), []
    for r in rows:
        t = r.get("ticker")
        if t and t not in seen:
            seen.add(t); out.append(r)
    return out


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS serenity_picks (
            fetched_at TIMESTAMP,
            ticker VARCHAR NOT NULL,
            ai_chain_segment VARCHAR,
            stance VARCHAR,
            current_view VARCHAR,
            confidence VARCHAR,
            priority_bucket VARCHAR,
            priority_score DOUBLE,
            latest_return_pct DOUBLE,
            ret_1w DOUBLE,
            ret_1m DOUBLE,
            ret_6m DOUBLE,
            ret_1y DOUBLE,
            first_mentioned_at TIMESTAMP,
            last_mentioned_at TIMESTAMP,
            baseline_price DOUBLE,
            view_change VARCHAR,
            PRIMARY KEY (fetched_at, ticker)
        )
    """)


def flatten_returns(rec: dict) -> dict:
    out = {}
    rets = rec.get("returns") or {}
    for k in ("1w", "1m", "6m", "1y"):
        sub = rets.get(k) or {}
        v = sub.get("return_pct") if sub.get("status") == "ok" else None
        out[f"ret_{k}"] = float(v) if v is not None else None
    return out


def write_picks(con: duckdb.DuckDBPyConnection, picks: list[dict], fetched_at: datetime) -> int:
    n = 0
    for rec in picks:
        rets = flatten_returns(rec)
        try:
            con.execute("""
                INSERT OR REPLACE INTO serenity_picks
                (fetched_at, ticker, ai_chain_segment, stance, current_view,
                 confidence, priority_bucket, priority_score, latest_return_pct,
                 ret_1w, ret_1m, ret_6m, ret_1y,
                 first_mentioned_at, last_mentioned_at, baseline_price, view_change)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                fetched_at, rec["ticker"],
                rec.get("ai_chain_segment"), rec.get("stance"), rec.get("current_view"),
                rec.get("confidence"), rec.get("priority_bucket"),
                float(rec["priority_score"]) if rec.get("priority_score") is not None else None,
                float(rec["latest_return_pct"]) if rec.get("latest_return_pct") is not None else None,
                rets["ret_1w"], rets["ret_1m"], rets["ret_6m"], rets["ret_1y"],
                rec.get("first_mentioned_at"), rec.get("last_mentioned_at"),
                float(rec["baseline_price"]) if rec.get("baseline_price") is not None else None,
                str(rec.get("view_change") or "") or None,
            ])
            n += 1
        except (duckdb.Error, TypeError, ValueError) as e:
            print(f"  [warn] {rec.get('ticker')}: {e}", file=sys.stderr)
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    args = ap.parse_args()

    html = fetch_html()
    picks = parse_picks(html)
    if not picks:
        print("no picks parsed; site format may have changed"); return

    con = duckdb.connect(str(US_DB))
    init_schema(con)
    fetched_at = datetime.combine(date.fromisoformat(args.date), datetime.min.time())
    n = write_picks(con, picks, fetched_at)
    print(f"serenity_picks: {n} rows written for fetched_at={args.date}")
    # Quick stats
    stats = con.execute("""
        SELECT stance, COUNT(*) AS n, AVG(latest_return_pct) AS avg_ret
        FROM serenity_picks WHERE fetched_at = ?
        GROUP BY stance ORDER BY n DESC
    """, [fetched_at]).fetchall()
    for s in stats: print(f"  stance={s[0]:12} n={s[1]:3} avg_return={s[2]:+.1f}%")
    con.close()


if __name__ == "__main__":
    main()
