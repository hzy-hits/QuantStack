"""Consolidate per-source staging DuckDBs into the hot DB (single writer).

Each fetch worker writes its own staging/{source}.duckdb. This script takes the
hot DB's exclusive write lock and merges every matching table via INSERT OR
REPLACE, including the fetch_state watermark. It is the ONLY writer to hot.

Usage:
    python3 scripts/consolidate_raw.py --market cn
    python3 scripts/consolidate_raw.py --market cn --hot <path> --staging-dir <dir>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))

from quant_bot.storage.db import connect_write  # noqa: E402

CN_HOT_DEFAULT = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
CN_STAGING_DIR_DEFAULT = STACK_ROOT / "quant-research-cn" / "data" / "staging"
CN_SOURCES = ["cn_tushare.duckdb", "cn_akshare.duckdb"]

# Consolidate owns the watermark table — ensure it exists in hot even if the hot
# DB predates it (mirrors quant-research-cn/src/storage/schema.rs fetch_state).
FETCH_STATE_DDL = """
CREATE TABLE IF NOT EXISTS fetch_state (
    market      VARCHAR NOT NULL,
    fetcher     VARCHAR NOT NULL,
    as_of       DATE,
    status      VARCHAR,
    row_count   BIGINT DEFAULT 0,
    fetched_at  TIMESTAMP DEFAULT current_timestamp,
    error       VARCHAR,
    PRIMARY KEY (market, fetcher)
);
"""


def consolidate_cn(hot_path: str, staging_paths: list[str]) -> int:
    """Merge each existing staging DB into hot via INSERT OR REPLACE. Returns rows merged."""
    con = connect_write(hot_path)  # exclusive fcntl lock on hot
    try:
        con.execute(FETCH_STATE_DDL)  # ensure watermark table exists before merge
        hot_tables = {
            r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        total = 0
        for i, sp in enumerate(staging_paths):
            if not Path(sp).exists():
                continue
            alias = f"s{i}"
            con.execute(f"ATTACH '{sp}' AS {alias} (READ_ONLY)")
            try:
                stg_tables = {
                    r[0] for r in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_catalog=?",
                        [alias],
                    ).fetchall()
                }
                for t in sorted(hot_tables & stg_tables):
                    n = con.execute(f"SELECT count(*) FROM {alias}.{t}").fetchone()[0]
                    if n == 0:
                        continue
                    con.execute(f"INSERT OR REPLACE INTO main.{t} SELECT * FROM {alias}.{t}")
                    total += n
            finally:
                con.execute(f"DETACH {alias}")
        return total
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["cn"])
    ap.add_argument("--hot", default=str(CN_HOT_DEFAULT))
    ap.add_argument("--staging-dir", default=str(CN_STAGING_DIR_DEFAULT))
    args = ap.parse_args()
    staging = [str(Path(args.staging_dir) / name) for name in CN_SOURCES]
    merged = consolidate_cn(args.hot, staging)
    present = len([s for s in staging if Path(s).exists()])
    print(f"consolidate cn: merged {merged} rows from {present} staging DB(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
