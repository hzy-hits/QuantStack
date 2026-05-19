"""Backfill full CN daily price history for AI-infra universe symbols.

The Rust fetcher only pulls prices forward from when a symbol enters the
universe, so names added during the AI-infra migration (the CXMT chain
especially) carry just ~45 rows. The reranker and EMA tape then score
them on far too little data.

This pulls full daily history from Tushare `daily` for every CN universe
symbol that is short, and INSERT-OR-REPLACEs it into prices (PK ts_code,
trade_date). Raw unadjusted OHLCV — matches the existing rows. Re-runnable
and idempotent; run it after adding new names to the universe.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = STACK_ROOT / "scripts"
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
for p in (str(SCRIPT_DIR), str(QUANT_V1_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

import verify_cn_ai_evidence as v  # noqa: E402  (reuse its Tushare _pro)
from quant_bot.analytics import ai_infra_universe as aiu  # noqa: E402

DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
START_DATE = "20230101"          # deep enough for EMA200 before the 2024-06 backtest
SHORT_ROW_THRESHOLD = 400        # fewer rows than this → backfill
_COLS = ("ts_code", "trade_date", "open", "high", "low", "close",
         "pre_close", "change", "pct_chg", "vol", "amount")


def _pull_daily(pro, ts_code: str, end: str, max_retries: int = 5):
    """Tushare daily OHLCV, with the same rate-limit backoff as fina_mainbz."""
    for attempt in range(max_retries):
        try:
            df = pro.daily(ts_code=ts_code, start_date=START_DATE, end_date=end)
            return df
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if ("每分钟" in msg or "rate" in msg.lower() or "limit" in msg.lower()) \
                    and attempt < max_retries - 1:
                time.sleep(60.0)
                continue
            print(f"  warn: daily({ts_code}) failed: {exc}", file=sys.stderr)
            return None
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--config", type=Path, default=v.DEFAULT_CONFIG)
    parser.add_argument("--all", action="store_true",
                        help="refresh every universe symbol, not just short ones")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    end = date.today().strftime("%Y%m%d")
    symbols = sorted(set(aiu.records_by_symbol("CN", pool="research")))
    con = duckdb.connect(str(args.cn_db))
    try:
        targets = []
        for s in symbols:
            n = con.execute("SELECT count(*) FROM prices WHERE ts_code=?", [s]).fetchone()[0]
            if args.all or n < SHORT_ROW_THRESHOLD:
                targets.append((s, n))
        print(f"CN universe {len(symbols)} symbols; {len(targets)} need backfill "
              f"(threshold {SHORT_ROW_THRESHOLD} rows)")
        if args.dry_run:
            for s, n in targets:
                print(f"  would backfill {s} (currently {n} rows)")
            return 0

        pro = v._pro(args.config)
        filled, failed = 0, []
        for s, n in targets:
            df = _pull_daily(pro, s, end)
            if df is None or df.empty:
                failed.append(s)
                continue
            rows = []
            for _, r in df.iterrows():
                td = str(r["trade_date"])
                iso = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
                rows.append((s, iso, r.get("open"), r.get("high"), r.get("low"),
                             r.get("close"), r.get("pre_close"), r.get("change"),
                             r.get("pct_chg"), r.get("vol"), r.get("amount")))
            con.execute("BEGIN")
            con.executemany(
                f"INSERT OR REPLACE INTO prices ({','.join(_COLS)}) "
                f"VALUES ({','.join(['?'] * len(_COLS))})",
                rows,
            )
            con.execute("COMMIT")
            filled += 1
            print(f"  {s}: {n} -> {len(rows)} rows")
            time.sleep(0.35)  # Tushare daily rate-limit courtesy

        print(f"\nbackfilled {filled}/{len(targets)} symbols; failed: {failed}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
