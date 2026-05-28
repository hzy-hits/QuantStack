"""Watch the TOS-exported CSV and upsert rows into DuckDB realtime_quotes.

CSV format (wide, written by VBA macro every 10s):
  timestamp,SPX_LAST,SPX_BID,SPX_ASK,SPX_ATM_CALL_IV,SPX_ATM_PUT_IV,SPX_PC_RATIO,SPX_FLIP,...
  2026-05-28T13:34:56Z,7520.50,7520.30,7520.70,0.1085,0.1872,1.07,7310,...

Each column is `<SYMBOL>_<FIELD>`. The watcher pivots wide → long format
and writes one row per (timestamp, symbol, field) into DuckDB.

Run as daemon:
  python3 scripts/realtime/csv_watcher.py --csv-path C:/TOS/snapshot.csv

For testing without TOS:
  python3 scripts/realtime/csv_watcher.py --csv-path /tmp/fake_snapshot.csv --poll-mode
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_quotes (
            ingested_at  TIMESTAMP NOT NULL,
            tos_time     TIMESTAMP,
            symbol       VARCHAR NOT NULL,
            field        VARCHAR NOT NULL,
            value        DOUBLE,
            PRIMARY KEY (ingested_at, symbol, field)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_realtime_symbol_time
        ON realtime_quotes (symbol, ingested_at)
        """
    )


def _parse_value(s: str) -> float | None:
    s = (s or "").strip()
    if not s or s.upper() in {"N/A", "NA", "ERROR", "#N/A"}:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_csv_row(header: list[str], values: list[str]) -> tuple[datetime | None, list[dict]]:
    """Pivot a wide CSV row into long-format rows."""
    if not header or not values or len(values) < len(header):
        return None, []
    # First column is timestamp
    ts_str = (values[0] or "").strip()
    tos_time: datetime | None = None
    if ts_str:
        try:
            tos_time = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except ValueError:
            tos_time = None
    rows: list[dict] = []
    for idx in range(1, len(header)):
        col = (header[idx] or "").strip()
        if "_" not in col:
            continue
        symbol, field = col.split("_", 1)
        v = _parse_value(values[idx])
        if v is None:
            continue
        rows.append({"symbol": symbol.upper(), "field": field.upper(), "value": v})
    return tos_time, rows


def _process_csv(csv_path: Path, con: duckdb.DuckDBPyConnection) -> int:
    """Read the CSV file, pivot, upsert. Returns rows written."""
    if not csv_path.exists():
        return 0
    text = csv_path.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        return 0
    lines = text.splitlines()
    if len(lines) < 2:
        return 0
    header = [c.strip() for c in lines[0].split(",")]
    # Use the LAST data row as the freshest snapshot
    values = [c.strip() for c in lines[-1].split(",")]
    tos_time, rows = _parse_csv_row(header, values)
    if not rows:
        return 0
    ingested_at = datetime.utcnow()
    for r in rows:
        con.execute(
            """
            INSERT OR REPLACE INTO realtime_quotes
              (ingested_at, tos_time, symbol, field, value)
            VALUES (?, ?, ?, ?, ?)
            """,
            [ingested_at, tos_time, r["symbol"], r["field"], r["value"]],
        )
    return len(rows)


def run(csv_path: Path, poll_seconds: float, quiet: bool) -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    _ensure_schema(con)

    last_mtime: float = 0.0
    last_total = 0
    print(f"[csv_watcher] watching {csv_path} poll_every={poll_seconds}s")
    while True:
        try:
            if csv_path.exists():
                mtime = csv_path.stat().st_mtime
                if mtime != last_mtime:
                    n = _process_csv(csv_path, con)
                    if n > 0:
                        last_total += n
                        if not quiet:
                            print(
                                f"[csv_watcher] {datetime.utcnow().isoformat(timespec='seconds')}Z "
                                f"upserted {n} fields (total {last_total})"
                            )
                        con.execute("CHECKPOINT")
                    last_mtime = mtime
            time.sleep(poll_seconds)
        except KeyboardInterrupt:
            print("[csv_watcher] stopped by user")
            break
        except OSError as e:
            print(f"[csv_watcher] read error: {e}; sleeping 5s")
            time.sleep(5)
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-path", required=True, help="Path to TOS-exported snapshot CSV")
    ap.add_argument("--poll-seconds", type=float, default=2.0,
                    help="Poll interval (default 2s — TOS macro writes every 10s, so 2s is fast enough)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    run(Path(args.csv_path), args.poll_seconds, args.quiet)


if __name__ == "__main__":
    main()
