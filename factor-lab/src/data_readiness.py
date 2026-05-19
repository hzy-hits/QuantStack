from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb

from src.paths import QUANT_CN_DB, QUANT_US_DB


MARKET_SPECS = {
    "cn": {
        "db_path": QUANT_CN_DB,
        "table": "prices",
        "date_col": "trade_date",
    },
    "us": {
        "db_path": QUANT_US_DB,
        "table": "prices_daily",
        "date_col": "date",
    },
}


def _coerce_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def latest_trade_date(market: str) -> date | None:
    spec = MARKET_SPECS[market]
    con = duckdb.connect(str(spec["db_path"]), read_only=True)
    try:
        row = con.execute(
            f"SELECT MAX({spec['date_col']}) FROM {spec['table']}"
        ).fetchone()
    finally:
        con.close()
    return _coerce_date(row[0] if row else None)


def expected_us_data_date(now: datetime | None = None) -> date:
    ny_tz = ZoneInfo("America/New_York")
    current = now or datetime.now(ny_tz)
    if current.tzinfo is None:
        current = current.replace(tzinfo=ny_tz)
    return current.astimezone(ny_tz).date()


def market_data_ready(
    market: str,
    *,
    expected_date: date | str | None = None,
    max_staleness_days: int = 0,
) -> tuple[bool, date | None, date | None]:
    """Is the market's data fresh enough?

    `max_staleness_days` tolerates the latest data being up to N days older
    than `expected` — this covers weekends, holidays and the Monday gap
    (where the freshest US close is the prior Friday) without silently
    accepting a genuine multi-day pipeline outage.
    """
    latest = latest_trade_date(market)
    expected = _coerce_date(expected_date)

    if expected is None:
        expected = expected_us_data_date() if market == "us" else latest

    ready = (
        latest is not None and expected is not None
        and latest >= expected - timedelta(days=max(max_staleness_days, 0))
    )
    return ready, latest, expected


def main() -> int:
    parser = argparse.ArgumentParser(description="Check whether market data is fresh enough.")
    parser.add_argument("--market", choices=["cn", "us"], required=True)
    parser.add_argument("--expected-date", help="Required latest trade date (YYYY-MM-DD).")
    parser.add_argument("--max-staleness-days", type=int, default=0,
                        help="Tolerate the latest data being up to N days older "
                             "than expected (covers weekends / holidays).")
    parser.add_argument("--print-latest", action="store_true",
                        help="On ready, print only the latest trade date (for scripts).")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    ready, latest, expected = market_data_ready(
        args.market,
        expected_date=args.expected_date,
        max_staleness_days=args.max_staleness_days,
    )

    if args.print_latest:
        if ready and latest is not None:
            print(latest.isoformat())
        return 0 if ready else 1

    if not args.quiet:
        if ready:
            print(
                f"{args.market.upper()} data ready: latest={latest} expected>={expected}"
            )
        else:
            print(
                f"{args.market.upper()} data stale: latest={latest} expected>={expected}"
            )

    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
