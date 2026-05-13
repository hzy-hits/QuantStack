"""Ingest canonical A-share index closes into the CN report DuckDB.

AGENTS.md lists `000001.SH` / `399001.SZ` / `399006.SZ` / `000300.SH` as the
canonical CN benchmarks. The daily report's Benchmark Snapshot section relies
on `prices.ts_code` rows for those indices. The standard CN producer pipeline
fetches stock prices via Tushare and AKShare but does not always backfill the
broad indices; this script closes that gap.

Behaviour:
- Read from AKShare directly using `stock_zh_index_daily`.
- Upsert into the `prices` table of the CN DuckDB with canonical `ts_code`
  suffixes (`SH` for Shanghai, `SZ` for Shenzhen).
- Compute `pre_close`, `change`, and `pct_chg` from the fetched OHLCV.
- Exit cleanly with a status message if AKShare is not installed or the
  network call fails; never insert null/silent placeholder rows.
- Idempotent: rows for an existing `(ts_code, trade_date)` are replaced.

Example:

    python3 scripts/ingest_cn_index_prices.py \
      --cn-db quant-research-cn/data/quant_cn_report.duckdb \
      --as-of 2026-05-13
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class IndexSpec:
    canonical: str        # ts_code stored in DuckDB
    akshare_symbol: str   # symbol passed to ak.stock_zh_index_daily
    label: str            # human-readable label


CANONICAL_INDICES: tuple[IndexSpec, ...] = (
    IndexSpec("000001.SH", "sh000001", "上证指数"),
    IndexSpec("399001.SZ", "sz399001", "深证成指"),
    IndexSpec("399006.SZ", "sz399006", "创业板指"),
    IndexSpec("000300.SH", "sh000300", "沪深300"),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cn-db",
        type=Path,
        default=Path("quant-research-cn/data/quant_cn_report.duckdb"),
        help="CN DuckDB path. Must already contain the prices table.",
    )
    parser.add_argument(
        "--as-of",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Upper-bound trade date (inclusive). Defaults to today.",
    )
    parser.add_argument(
        "--indices",
        nargs="*",
        default=[spec.canonical for spec in CANONICAL_INDICES],
        help="Which canonical ts_codes to ingest. Defaults to the four AGENTS.md benchmarks.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=730,
        help="Trailing days to upsert. Older history already in the DB is left alone.",
    )
    return parser.parse_args()


def _resolve_specs(requested: list[str]) -> list[IndexSpec]:
    by_canonical = {spec.canonical: spec for spec in CANONICAL_INDICES}
    out: list[IndexSpec] = []
    for code in requested:
        spec = by_canonical.get(code.upper())
        if spec is None:
            print(f"warn: unknown index {code!r}; skipping", file=sys.stderr)
            continue
        out.append(spec)
    return out


def _fetch_index_daily(spec: IndexSpec):
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except ImportError as exc:
        print(
            f"error: akshare not installed ({exc}); cannot ingest indices without the AKShare bridge",
            file=sys.stderr,
        )
        return None
    try:
        df = ak.stock_zh_index_daily(symbol=spec.akshare_symbol)
    except Exception as exc:  # noqa: BLE001
        print(f"error: AKShare fetch failed for {spec.canonical} ({spec.akshare_symbol}): {exc}", file=sys.stderr)
        return None
    return df


def _ensure_prices_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            ts_code VARCHAR,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            change DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            adj_factor DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )


def _upsert_index_rows(
    con: duckdb.DuckDBPyConnection,
    spec: IndexSpec,
    df,
    as_of: date,
    lookback_days: int,
) -> int:
    import math

    rows: list[tuple] = []
    lookback_start = date.fromordinal(max(1, as_of.toordinal() - lookback_days))
    sorted_df = df.sort_values("date")
    prev_close: float | None = None
    for record in sorted_df.itertuples(index=False):
        trade_date = record.date
        if hasattr(trade_date, "date"):
            trade_date = trade_date.date()
        if isinstance(trade_date, str):
            trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        if trade_date > as_of or trade_date < lookback_start:
            prev_close = float(record.close) if record.close is not None else prev_close
            continue
        close = float(record.close) if record.close is not None else None
        change = (close - prev_close) if (close is not None and prev_close is not None) else None
        pct = (change / prev_close * 100.0) if (change is not None and prev_close not in (None, 0)) else None
        if pct is not None and (math.isinf(pct) or math.isnan(pct)):
            pct = None
        rows.append(
            (
                spec.canonical,
                trade_date,
                float(record.open) if record.open is not None else None,
                float(record.high) if record.high is not None else None,
                float(record.low) if record.low is not None else None,
                close,
                prev_close,
                change,
                pct,
                float(record.volume) if record.volume is not None else None,
                None,  # amount: AKShare index API does not expose turnover in CNY directly.
                None,  # adj_factor: indices not adjusted.
            )
        )
        prev_close = close

    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO prices
            (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount, adj_factor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def main() -> int:
    args = _parse_args()
    as_of = args.as_of or date.today()
    specs = _resolve_specs(list(args.indices))
    if not specs:
        print("error: no valid indices requested", file=sys.stderr)
        return 2
    if not args.cn_db.exists():
        print(f"error: CN db missing at {args.cn_db}", file=sys.stderr)
        return 2

    con = duckdb.connect(str(args.cn_db))
    try:
        _ensure_prices_table(con)
        total_rows = 0
        per_index: list[tuple[str, int]] = []
        failures: list[str] = []
        for spec in specs:
            df = _fetch_index_daily(spec)
            if df is None or df.empty:
                failures.append(spec.canonical)
                continue
            inserted = _upsert_index_rows(con, spec, df, as_of, args.lookback_days)
            per_index.append((spec.canonical, inserted))
            total_rows += inserted
    finally:
        con.close()

    print(
        f"CN index ingest complete as-of {as_of.isoformat()}: "
        + ", ".join(f"{code}={count}" for code, count in per_index)
        + (f"; failed: {','.join(failures)}" if failures else "")
    )
    if failures and not per_index:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
