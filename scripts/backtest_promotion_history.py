"""Backtest promotion history vs SPY at 5 / 20 / 60 trading-day horizons.

The methodology forbids using K-line as evidence for supply-chain claims. But
once a name is on the `promote_now` ledger, we *can* fairly ask:

    did the promote_now signal generate alpha vs the broad market?

This script reads `ai_infra/reports/promotion_history.csv`, looks up each
ticker's close on the promotion date and on the next 5 / 20 / 60 trading
days, and compares to SPY's close on the same dates. Output covers:

- per-row: absolute return, SPY return, active return at 5d / 20d / 60d.
- aggregate by horizon: mean alpha, hit rate, IR.

Outputs land in
`reports/review_dashboard/ai_infra_promotion_alpha/<as-of>/promotion_alpha_ledger.{csv,md}`.

This is *read-only* on prices_daily and the history ledger. It never modifies
either file.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_HISTORY = STACK_ROOT / "ai_infra" / "reports" / "promotion_history.csv"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_promotion_alpha"

HORIZONS = (5, 20, 60)
SPY = "SPY"


@dataclass(frozen=True)
class BacktestRow:
    as_of: str
    primary_ticker: str
    company: str
    asset_pool: str
    readiness_tier: str
    base_close: float | None
    spy_base_close: float | None
    returns: dict[str, dict[str, float | None]]  # horizon → {ticker_ret, spy_ret, active}

    def as_dict(self) -> dict[str, str]:
        row: dict[str, str] = {
            "as_of": self.as_of,
            "primary_ticker": self.primary_ticker,
            "company": self.company,
            "asset_pool": self.asset_pool,
            "readiness_tier": self.readiness_tier,
            "base_close": f"{self.base_close:.4f}" if self.base_close is not None else "",
            "spy_base_close": f"{self.spy_base_close:.4f}" if self.spy_base_close is not None else "",
        }
        for horizon in (5, 20, 60):
            metrics = self.returns.get(f"{horizon}d", {})
            for key in ("ticker_ret_pct", "spy_ret_pct", "active_ret_pct"):
                value = metrics.get(key)
                row[f"{key}_{horizon}d"] = f"{value:.3f}" if value is not None else ""
        return row


def _load_history(path: Path, recommendation: str = "promote_now") -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [r for r in rows if (r.get("recommendation") or "") == recommendation]


def _load_close_series(con: duckdb.DuckDBPyConnection, symbols: list[str]) -> dict[str, list[tuple[date, float]]]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, date, close
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND close IS NOT NULL
        ORDER BY symbol, date
        """,
        symbols,
    ).fetchall()
    out: dict[str, list[tuple[date, float]]] = {}
    for sym, d, close in rows:
        if isinstance(d, str):
            try:
                d = date.fromisoformat(d)
            except ValueError:
                continue
        out[sym] = out.get(sym, []) + [(d, float(close))]
    return out


def _find_close_at_offset(series: list[tuple[date, float]], anchor: date, offset_days: int) -> tuple[date, float] | None:
    """Return the (date, close) pair for `offset_days` trading days after the
    first session at-or-after `anchor`. Used so weekend/holiday gaps don't
    skew the lookup.
    """
    if not series:
        return None
    anchor_idx = None
    for idx, (d, _) in enumerate(series):
        if d >= anchor:
            anchor_idx = idx
            break
    if anchor_idx is None:
        return None
    target_idx = anchor_idx + offset_days
    if target_idx >= len(series):
        return None
    return series[target_idx]


def _ret_pct(base: float | None, future: float | None) -> float | None:
    if base is None or future is None or base <= 0:
        return None
    return (future / base - 1.0) * 100.0


def backtest(history_csv: Path, us_db: Path) -> list[BacktestRow]:
    history_rows = _load_history(history_csv)
    if not history_rows:
        return []
    symbols = sorted({(row.get("primary_ticker") or "").strip().upper() for row in history_rows if row.get("primary_ticker")})
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        series = _load_close_series(con, symbols + [SPY])
    finally:
        con.close()

    spy_series = series.get(SPY, [])

    out: list[BacktestRow] = []
    for row in history_rows:
        ticker = (row.get("primary_ticker") or "").strip().upper()
        as_of_text = (row.get("as_of") or "").strip()
        if not ticker or not as_of_text:
            continue
        try:
            anchor = date.fromisoformat(as_of_text)
        except ValueError:
            continue
        ticker_series = series.get(ticker, [])
        base_pair = _find_close_at_offset(ticker_series, anchor, 0)
        spy_base_pair = _find_close_at_offset(spy_series, anchor, 0)
        base_close = base_pair[1] if base_pair else None
        spy_base_close = spy_base_pair[1] if spy_base_pair else None

        horizon_metrics: dict[str, dict[str, float | None]] = {}
        for horizon in HORIZONS:
            ticker_pair = _find_close_at_offset(ticker_series, anchor, horizon)
            spy_pair = _find_close_at_offset(spy_series, anchor, horizon)
            ticker_ret = _ret_pct(base_close, ticker_pair[1] if ticker_pair else None)
            spy_ret = _ret_pct(spy_base_close, spy_pair[1] if spy_pair else None)
            active = (
                ticker_ret - spy_ret
                if ticker_ret is not None and spy_ret is not None
                else None
            )
            horizon_metrics[f"{horizon}d"] = {
                "ticker_ret_pct": ticker_ret,
                "spy_ret_pct": spy_ret,
                "active_ret_pct": active,
            }
        out.append(
            BacktestRow(
                as_of=as_of_text,
                primary_ticker=ticker,
                company=row.get("company") or "",
                asset_pool=row.get("asset_pool") or "",
                readiness_tier=row.get("readiness_tier") or "",
                base_close=base_close,
                spy_base_close=spy_base_close,
                returns=horizon_metrics,
            )
        )
    out.sort(key=lambda r: (r.as_of, r.primary_ticker))
    return out


def _aggregate(rows: list[BacktestRow], horizon: int) -> dict[str, float | int | None]:
    key = f"{horizon}d"
    actives = [
        row.returns.get(key, {}).get("active_ret_pct")
        for row in rows
        if row.returns.get(key, {}).get("active_ret_pct") is not None
    ]
    if not actives:
        return {"n": 0, "mean_active_pct": None, "hit_rate_pct": None, "ir": None}
    mean = sum(actives) / len(actives)
    hits = sum(1 for a in actives if a > 0)
    var = sum((a - mean) ** 2 for a in actives) / len(actives)
    stdev = var ** 0.5 if var > 0 else 0.0
    ir = mean / stdev if stdev > 0 else None
    return {
        "n": len(actives),
        "mean_active_pct": round(mean, 3),
        "hit_rate_pct": round(hits / len(actives) * 100.0, 1),
        "ir": round(ir, 3) if ir is not None else None,
    }


def render_markdown(rows: list[BacktestRow], as_of: str) -> str:
    lines: list[str] = [
        f"# AI Infra Promotion Alpha Ledger - {as_of}",
        "",
        "- 数据源: `promotion_history.csv` (recommendation=promote_now) + `prices_daily` (SPY baseline)。",
        "- 用法: 检验 promote_now 名字相对 SPY 的 5d/20d/60d 表现，看 readiness gate 是否真的有 alpha。",
        "- 状态: read-only; 不修改 promotion_history 也不影响下游 ranker。",
        "",
        f"- promote_now 行数: {len(rows)}",
        "",
        "## Aggregate by Horizon",
        "",
        "| Horizon | N | Mean Active % | Hit Rate | IR |",
        "|---|---:|---:|---:|---:|",
    ]
    for horizon in HORIZONS:
        agg = _aggregate(rows, horizon)
        n = agg["n"]
        mean_active = agg["mean_active_pct"]
        hit_rate = agg["hit_rate_pct"]
        ir = agg["ir"]
        lines.append(
            f"| {horizon}d | {n} | "
            f"{mean_active:+.2f}% | "
            if mean_active is not None
            else f"| {horizon}d | {n} | - | "
        )
        # rebuild row cleanly
    # the streaming above is awkward; rebuild table:
    lines = lines[:-len(HORIZONS)]
    for horizon in HORIZONS:
        agg = _aggregate(rows, horizon)
        n = agg["n"] or 0
        mean_active = agg["mean_active_pct"]
        hit_rate = agg["hit_rate_pct"]
        ir = agg["ir"]
        active_text = f"{mean_active:+.2f}%" if mean_active is not None else "-"
        hit_text = f"{hit_rate:.1f}%" if hit_rate is not None else "-"
        ir_text = f"{ir:+.2f}" if ir is not None else "-"
        lines.append(f"| {horizon}d | {n} | {active_text} | {hit_text} | {ir_text} |")
    lines.append("")

    lines += [
        "## Per-Row Detail",
        "",
        "| As-of | Ticker | Company | Pool | 5d ret | SPY 5d | Active 5d | 20d ret | Active 20d | 60d ret | Active 60d |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if not rows:
        lines.append("| - | - | _暂无 promote_now 历史_ | - | - | - | - | - | - | - | - |")
    for row in rows[:60]:
        def fmt(value: float | None) -> str:
            return f"{value:+.2f}%" if value is not None else "-"
        m5 = row.returns.get("5d", {})
        m20 = row.returns.get("20d", {})
        m60 = row.returns.get("60d", {})
        lines.append(
            "| "
            + " | ".join(
                [
                    row.as_of,
                    row.primary_ticker,
                    (row.company or "-")[:24],
                    row.asset_pool or "-",
                    fmt(m5.get("ticker_ret_pct")),
                    fmt(m5.get("spy_ret_pct")),
                    fmt(m5.get("active_ret_pct")),
                    fmt(m20.get("ticker_ret_pct")),
                    fmt(m20.get("active_ret_pct")),
                    fmt(m60.get("ticker_ret_pct")),
                    fmt(m60.get("active_ret_pct")),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(rows: list[BacktestRow], out_dir: Path, as_of: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "as_of",
        "primary_ticker",
        "company",
        "asset_pool",
        "readiness_tier",
        "base_close",
        "spy_base_close",
    ]
    for horizon in HORIZONS:
        for key in ("ticker_ret_pct", "spy_ret_pct", "active_ret_pct"):
            fieldnames.append(f"{key}_{horizon}d")
    with (out_dir / "promotion_alpha_ledger.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
    (out_dir / "promotion_alpha_ledger.md").write_text(render_markdown(rows, as_of), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    if not args.history_csv.exists():
        print(f"warn: history not found at {args.history_csv}; emitting empty ledger", file=sys.stderr)
        rows: list[BacktestRow] = []
    elif not args.us_db.exists():
        print(f"error: US db missing at {args.us_db}", file=sys.stderr)
        return 2
    else:
        rows = backtest(args.history_csv, args.us_db)

    out_dir = args.output_root / as_of
    write_outputs(rows, out_dir, as_of)
    rows_with_5d = sum(
        1 for r in rows if (r.returns.get("5d") or {}).get("active_ret_pct") is not None
    )
    print(
        f"Promotion alpha ledger written: {out_dir / 'promotion_alpha_ledger.md'}; "
        f"rows={len(rows)}, with_5d_active={rows_with_5d}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
