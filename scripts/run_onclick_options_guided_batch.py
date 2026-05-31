#!/usr/bin/env python3
"""Run OnclickMedia options-guided trend backtests for a ticker basket."""
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "onclick_options_guided_trend_backtest"
BACKTEST_SCRIPT = STACK_ROOT / "scripts" / "backtest_onclick_options_guided_trend.py"
BENCHMARK_TICKERS = {"SPY", "QQQ", "SMH", "VOO", "IWM", "DIA"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tickers", nargs="*", default=["UNH", "VOO", "QQQ", "MU", "SNDK", "INTC"])
    parser.add_argument("--start", default="2024-10-01")
    parser.add_argument("--end", default="2025-05-30")
    parser.add_argument("--sleep-seconds", default="0")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def _fmt_pct(value: Any, digits: int = 2) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed * 100:+.{digits}f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed:.{digits}f}"


def run_one(ticker: str, args: argparse.Namespace) -> dict[str, Any]:
    ticker = ticker.upper()
    cmd = [
        sys.executable,
        str(BACKTEST_SCRIPT),
        "--ticker",
        ticker,
        "--start",
        args.start,
        "--end",
        args.end,
        "--sleep-seconds",
        str(args.sleep_seconds),
        "--output-root",
        str(args.output_root),
    ]
    proc = subprocess.run(cmd, cwd=STACK_ROOT, text=True, capture_output=True, check=False)
    run_dir = args.output_root / ticker / f"{args.start}_{args.end}"
    payload_path = run_dir / "backtest.json"
    row: dict[str, Any] = {
        "ticker": ticker,
        "status": "ok" if proc.returncode == 0 and payload_path.exists() else "failed",
        "run_dir": str(run_dir),
        "error": "",
    }
    if row["status"] != "ok":
        row["error"] = (proc.stderr or proc.stdout or "").strip()[-1000:]
        return row
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    row["payload"] = payload
    return row


def flatten_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    strategies = [
        "buy_hold",
        "ema200_below_buy",
        "ema200_above_buy",
        "trend_only",
        "gamma_v2_entry_alpha",
        "ivhv_timing",
        "ivhv_gamma_tanh",
        "ivhv_band_timing",
        "ivhv_band_gamma_tanh",
        "ivhv_band_gamma_v2",
    ]
    for result in results:
        ticker = result["ticker"]
        if result.get("status") != "ok":
            rows.append({"ticker": ticker, "strategy": "-", "status": "failed", "error": result.get("error")})
            continue
        payload = result["payload"]
        config = payload.get("config") or {}
        summary = payload.get("summary") or {}
        for strategy in strategies:
            stats = summary.get(strategy) or {}
            rows.append(
                {
                    "ticker": ticker,
                    "strategy": strategy,
                    "status": "ok",
                    "n_days": stats.get("n_days"),
                    "invested_days": stats.get("invested_days"),
                    "total_return": stats.get("total_return"),
                    "sharpe": stats.get("sharpe"),
                    "max_drawdown": stats.get("max_drawdown"),
                    "hit_rate": stats.get("hit_rate"),
                    "avg_exposure": stats.get("avg_exposure"),
                    "n_option_fetch_dates": config.get("n_option_fetch_dates"),
                    "n_contract_rows": config.get("n_contract_rows"),
                    "skipped_option_dates": config.get("skipped_option_dates"),
                    "split_adjustment_events": len(config.get("split_adjustment_events") or []),
                    "run_dir": result.get("run_dir"),
                }
            )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summarize_returns(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row.get("return") or 0.0) for row in rows]
    n = len(returns)
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0 if peak > 0 else 0.0)
    if n <= 1:
        sharpe = None
    else:
        mean = sum(returns) / n
        var = sum((ret - mean) ** 2 for ret in returns) / (n - 1)
        sharpe = None if var <= 0 else mean / math.sqrt(var) * math.sqrt(252.0)
    invested = [row for row in rows if bool(row.get("invested"))]
    return {
        "n_days": n,
        "invested_days": len(invested),
        "total_return": equity - 1.0,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "invested_hit_rate": (
            sum(1 for row in invested if float(row.get("return") or 0.0) > 0) / len(invested)
            if invested else None
        ),
    }


def basket_aggregate(results: list[dict[str, Any]], args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tickers = [
        result["ticker"]
        for result in results
        if result.get("status") == "ok" and result["ticker"].upper() not in BENCHMARK_TICKERS
    ]
    strategies = ["buy_hold", "trend_only", "gamma_v2_entry_alpha", "ivhv_band_timing", "ivhv_band_gamma_v2"]
    by_strategy: dict[str, dict[str, dict[str, dict[str, float]]]] = {strategy: {} for strategy in strategies}
    all_dates: set[str] = set()
    for ticker in tickers:
        path = args.output_root / ticker / f"{args.start}_{args.end}" / "strategy_daily.csv"
        if not path.exists():
            continue
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                strategy = row.get("strategy")
                if strategy not in by_strategy:
                    continue
                d = str(row.get("date") or "")
                if not d:
                    continue
                all_dates.add(d)
                by_strategy[strategy].setdefault(d, {})[ticker] = {
                    "return": float(row.get("net_return") or 0.0),
                    "exposure": float(row.get("exposure") or 0.0),
                }
    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        rows: list[dict[str, Any]] = []
        for d in sorted(all_dates):
            items = [by_strategy[strategy].get(d, {}).get(ticker) for ticker in tickers]
            returns = [(item or {}).get("return", 0.0) for item in items]
            exposures = [(item or {}).get("exposure", 0.0) for item in items]
            ret = sum(returns) / len(tickers) if tickers else 0.0
            invested = any(exposure > 0 for exposure in exposures)
            rows.append({"strategy": strategy, "date": d, "return": ret, "invested": invested})
            daily_rows.append({"strategy": strategy, "date": d, "return": ret, "invested": invested})
        summary_rows.append({"strategy": strategy, "mode": "fixed_weight", **summarize_returns(rows)})
    gamma_rows: list[dict[str, Any]] = []
    for d in sorted(all_dates):
        active = [
            by_strategy["gamma_v2_entry_alpha"].get(d, {}).get(ticker)
            for ticker in tickers
            if (by_strategy["gamma_v2_entry_alpha"].get(d, {}).get(ticker) or {}).get("exposure", 0.0) > 0
        ]
        ret = sum(item["return"] for item in active) / len(active) if active else 0.0
        row = {"strategy": "gamma_v2_entry_alpha", "date": d, "return": ret, "invested": bool(active)}
        gamma_rows.append(row)
        daily_rows.append({"strategy": "gamma_v2_active_equal", "date": d, "return": ret, "invested": bool(active)})
    summary_rows.append({"strategy": "gamma_v2_active_equal", "mode": "active_equal", **summarize_returns(gamma_rows)})
    return summary_rows, daily_rows


def render_markdown(results: list[dict[str, Any]], flat_rows: list[dict[str, Any]], args: argparse.Namespace) -> str:
    lines = [
        f"# OnclickMedia Options-Guided Batch - {args.start}..{args.end}",
        "",
        "Scope: research backtest only. Non-AI tickers and ETFs are benchmark/context rows, not production candidates.",
        "",
        "## Best Strategies By Ticker",
        "",
        "| Ticker | Best strategy | Total | Sharpe | Max DD | Invested days | Option dates | Contracts |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in flat_rows:
        by_ticker.setdefault(row["ticker"], []).append(row)
    for ticker in sorted(by_ticker):
        rows = [row for row in by_ticker[ticker] if row.get("status") == "ok" and row.get("sharpe") is not None]
        if not rows:
            err = next((row.get("error") for row in by_ticker[ticker] if row.get("error")), "failed")
            lines.append(f"| {ticker} | FAILED | - | - | - | - | - | - |")
            lines.append(f"<!-- {ticker}: {err} -->")
            continue
        best = max(rows, key=lambda row: float(row.get("sharpe") or -999.0))
        lines.append(
            f"| {ticker} | {best['strategy']} | {_fmt_pct(best.get('total_return'))} | {_fmt_num(best.get('sharpe'))} | "
            f"{_fmt_pct(best.get('max_drawdown'))} | {best.get('invested_days', 0)} | "
            f"{best.get('n_option_fetch_dates', 0)} | {best.get('n_contract_rows', 0)} |"
        )
    lines += [
        "",
        "## Strategy Matrix",
        "",
        "| Ticker | Strategy | Total | Sharpe | Max DD | Hit | Invested | Avg exposure |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    strategy_order = [
        "buy_hold",
        "ema200_below_buy",
        "trend_only",
        "gamma_v2_entry_alpha",
        "ivhv_timing",
        "ivhv_band_timing",
        "ivhv_band_gamma_tanh",
        "ivhv_band_gamma_v2",
    ]
    for ticker in sorted(by_ticker):
        for strategy in strategy_order:
            row = next((item for item in by_ticker[ticker] if item.get("strategy") == strategy), None)
            if not row or row.get("status") != "ok":
                continue
            lines.append(
                f"| {ticker} | {strategy} | {_fmt_pct(row.get('total_return'))} | {_fmt_num(row.get('sharpe'))} | "
                f"{_fmt_pct(row.get('max_drawdown'))} | {_fmt_pct(row.get('hit_rate'))} | "
                f"{row.get('invested_days', 0)} | {_fmt_num(row.get('avg_exposure'))} |"
            )
    lines += [
        "",
        "## Failed Tickers",
        "",
    ]
    failed = [result for result in results if result.get("status") != "ok"]
    if not failed:
        lines.append("- None")
    else:
        for result in failed:
            lines.append(f"- {result['ticker']}: {result.get('error') or 'failed'}")
    lines.append("")
    return "\n".join(lines)


def render_basket_markdown(rows: list[dict[str, Any]], args: argparse.Namespace) -> str:
    lines = [
        f"# OnclickMedia Basket Aggregate - {args.start}..{args.end}",
        "",
        "Universe: non-benchmark tickers from this batch. This is a research backtest; AI/source scope is controlled by the input ticker list.",
        "",
        "| Strategy | Mode | Days | Invested | Total | Sharpe | Max DD | Invested hit |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['strategy']} | {row['mode']} | {row.get('n_days', 0)} | {row.get('invested_days', 0)} | "
            f"{_fmt_pct(row.get('total_return'))} | {_fmt_num(row.get('sharpe'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | {_fmt_pct(row.get('invested_hit_rate'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    # Validate early for clearer run directory names.
    date.fromisoformat(args.start)
    date.fromisoformat(args.end)
    workers = max(1, min(int(args.workers), len(args.tickers) or 1))
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_one, ticker, args): ticker for ticker in args.tickers}
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda row: args.tickers.index(row["ticker"]) if row["ticker"] in args.tickers else 999)
    flat = flatten_rows(results)
    out_dir = args.output_root / "batch" / f"{args.start}_{args.end}_{'_'.join(t.upper() for t in args.tickers)}"
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(out_dir / "summary.csv", flat)
    basket_summary, basket_daily = basket_aggregate(results, args)
    write_csv(out_dir / "basket_aggregate.csv", basket_summary)
    write_csv(out_dir / "basket_aggregate_daily.csv", basket_daily)
    (out_dir / "summary.json").write_text(json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "summary.md").write_text(render_markdown(results, flat, args), encoding="utf-8")
    (out_dir / "basket_aggregate.md").write_text(render_basket_markdown(basket_summary, args), encoding="utf-8")
    print(json.dumps({"output_dir": str(out_dir), "rows": len(flat), "failed": [r["ticker"] for r in results if r.get("status") != "ok"]}, ensure_ascii=False, indent=2))
    return 1 if any(result.get("status") != "ok" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
