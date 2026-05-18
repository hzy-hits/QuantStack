"""Repair the dead CN flow signals — pull 龙虎榜 / 北向 direct from Tushare.

Diagnosis (2026-05-18): the quant-research-cn fetch left `top_list` (龙虎榜)
at 0 rows and `northbound_flow` (北向) at 42 stale rows — ~26% of the CN
flow_score weight was computing on broken inputs. The Tushare token DOES
have access to these endpoints; the Rust/AKShare-bridge fetch was the bug.

This script bypasses that — pulls direct from Tushare and upserts:
- top_list      ← Tushare `top_list`        (龙虎榜 per-stock summary)
- northbound_flow ← Tushare `moneyflow_hsgt` (北向资金净流入)
- hsgt_top10    ← Tushare `hsgt_top10`       (沪深股通十大成交股)

Idempotent: each table's rows for the date range are deleted then re-inserted.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import yaml

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
DEFAULT_CONFIG = Path("/home/ivena/coding/rust/quant-research-cn/config.yaml")


def _pro(config_path: Path):
    import tushare as ts
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    token = (
        (cfg.get("api") or {}).get("tushare_token")
        or cfg.get("tushare_token")
        or (cfg.get("tushare") or {}).get("token")
    )
    if not token:
        raise RuntimeError(f"no tushare_token in {config_path}")
    return ts.pro_api(token)


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _to_date(yyyymmdd: str) -> date:
    return datetime.strptime(str(yyyymmdd), "%Y%m%d").date()


def _trade_dates(pro, start: date, end: date) -> list[date]:
    cal = pro.trade_cal(exchange="SSE", start_date=_ymd(start), end_date=_ymd(end), is_open="1")
    return sorted(_to_date(d) for d in cal["cal_date"])


def _num(value):
    try:
        if value is None:
            return None
        f = float(value)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def ingest_northbound(con, pro, start: date, end: date) -> int:
    df = pro.moneyflow_hsgt(start_date=_ymd(start), end_date=_ymd(end))
    if df.empty:
        print("  northbound: tushare returned 0 rows")
        return 0
    con.execute(
        "DELETE FROM northbound_flow WHERE trade_date BETWEEN ? AND ?",
        [start.isoformat(), end.isoformat()],
    )
    rows = 0
    for _, r in df.iterrows():
        north = _num(r.get("north_money"))  # 北向净流入 (百万元)
        con.execute(
            "INSERT INTO northbound_flow (trade_date, buy_amount, sell_amount, net_amount, source) "
            "VALUES (?, ?, ?, ?, ?)",
            [_to_date(r["trade_date"]).isoformat(), None, None, north, "tushare:moneyflow_hsgt"],
        )
        rows += 1
    return rows


def ingest_top_list(con, pro, dates: list[date]) -> int:
    total = 0
    for d in dates:
        try:
            df = pro.top_list(trade_date=_ymd(d))
        except Exception as exc:  # noqa: BLE001
            print(f"  top_list {d}: fetch failed ({exc})", file=sys.stderr)
            continue
        con.execute("DELETE FROM top_list WHERE trade_date = ?", [d.isoformat()])
        if df.empty:
            continue
        # A stock can hit 龙虎榜 multiple times a day (different reasons).
        # The table PK is (ts_code, trade_date, broker_name) and Tushare
        # top_list has no 营业部 — aggregate to one row per stock/day.
        agg: dict[str, dict] = {}
        for _, r in df.iterrows():
            code = r.get("ts_code")
            if not code:
                continue
            a = agg.setdefault(code, {"buy": 0.0, "sell": 0.0, "net": 0.0, "reasons": []})
            a["buy"] += _num(r.get("l_buy")) or 0.0
            a["sell"] += _num(r.get("l_sell")) or 0.0
            a["net"] += _num(r.get("net_amount")) or 0.0
            if r.get("reason"):
                a["reasons"].append(str(r["reason"]))
        for code, a in agg.items():
            con.execute(
                "INSERT INTO top_list (ts_code, trade_date, reason, buy_amount, "
                "sell_amount, net_amount, broker_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    code, d.isoformat(), "; ".join(dict.fromkeys(a["reasons"])),
                    a["buy"], a["sell"], a["net"], "",
                ],
            )
            total += 1
        time.sleep(0.12)  # gentle on the Tushare rate limit
    return total


def ingest_hsgt_top10(con, pro, dates: list[date]) -> int:
    total = 0
    for d in dates:
        try:
            df = pro.hsgt_top10(trade_date=_ymd(d))
        except Exception as exc:  # noqa: BLE001
            print(f"  hsgt_top10 {d}: fetch failed ({exc})", file=sys.stderr)
            continue
        con.execute("DELETE FROM hsgt_top10 WHERE trade_date = ?", [d.isoformat()])
        if df.empty:
            continue
        for _, r in df.iterrows():
            con.execute(
                "INSERT INTO hsgt_top10 (trade_date, ts_code, name, close, rank, "
                "market_type, amount, net_amount, buy, sell) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    d.isoformat(), r.get("ts_code"), r.get("name"),
                    _num(r.get("close")),
                    int(r["rank"]) if r.get("rank") is not None else None,
                    str(r.get("market_type") or ""),
                    _num(r.get("amount")), _num(r.get("net_amount")),
                    _num(r.get("buy")), _num(r.get("sell")),
                ],
            )
            total += 1
        time.sleep(0.12)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--start", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        default=None, help="Start date (default: 90 days back).")
    parser.add_argument("--end", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        default=None, help="End date (default: today).")
    args = parser.parse_args()

    end = args.end or date.today()
    start = args.start or (end - timedelta(days=90))
    if not args.cn_db.exists():
        print(f"error: CN db missing at {args.cn_db}", file=sys.stderr)
        return 2

    pro = _pro(args.config)
    dates = _trade_dates(pro, start, end)
    if not dates:
        print(f"no trading days between {start} and {end}", file=sys.stderr)
        return 1
    print(f"CN flow ingest {start}..{end} ({len(dates)} trading days)")

    con = duckdb.connect(str(args.cn_db))
    try:
        nb = ingest_northbound(con, pro, start, end)
        tl = ingest_top_list(con, pro, dates)
        h10 = ingest_hsgt_top10(con, pro, dates)
    finally:
        con.close()
    print(f"CN flow ingest done: northbound_flow={nb} top_list={tl} hsgt_top10={h10}")
    return 0 if (nb or tl or h10) else 1


if __name__ == "__main__":
    sys.exit(main())
