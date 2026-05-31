#!/usr/bin/env python3
"""Backtest Main Strategy V2 daily report actionables.

This is a signal-quality backtest for the daily report files under
reports/review_dashboard/main_strategy_v2/<date>. It evaluates the report's
production_decision_summary.actionable rows against local close-to-close price
data and also reruns the current production contract in memory to show what the
same reports would look like after today's gates. For live report generation,
call with --before-date <report_date> so the Realized Horizon Edge section only
uses already-published reports.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb"
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant_report.duckdb"
if not DEFAULT_US_DB.exists():
    DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"

BENCHMARKS = {
    "CN": ["000300.SH", "399006.SZ", "399001.SZ", "000001.SH", "000016.SH", "399905.SZ"],
    "US": ["SPY", "QQQ", "SMH", "IWM"],
}
PRIMARY_BENCHMARK = {"CN": "000300.SH", "US": "SPY"}
HORIZONS = (1, 3, 5, 10)


def load_generator_module():
    path = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"
    spec = importlib.util.spec_from_file_location("generate_main_strategy_v2_report_for_backtest", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    # The backtest is read-only. Rebuilding contract-gated summaries should not
    # advance the virtual holdings ledger.
    module._persist_virtual_holdings = lambda *args, **kwargs: None
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--through-date", default=None, help="Only include report dates <= this YYYY-MM-DD date.")
    parser.add_argument("--before-date", default=None, help="Only include report dates < this YYYY-MM-DD date.")
    parser.add_argument(
        "--price-through-date",
        default=None,
        help="Only use price rows <= this YYYY-MM-DD date. Defaults to --before-date or --through-date when provided.",
    )
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="Do not print the full markdown summary to stdout.")
    return parser.parse_args()


def report_paths(
    report_root: Path,
    through_date: str | None = None,
    before_date: str | None = None,
) -> list[Path]:
    paths = sorted(report_root.glob("*/main_strategy_v2_backtest.json"), key=lambda path: path.parent.name)
    if through_date:
        paths = [path for path in paths if path.parent.name <= through_date]
    if before_date:
        paths = [path for path in paths if path.parent.name < before_date]
    return paths


def symbol_key(value: Any) -> str:
    return str(value or "").strip().upper()


def load_report_actions(paths: list[Path], generator, us_db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_actions: list[dict[str, Any]] = []
    gated_actions: list[dict[str, Any]] = []
    for path in paths:
        report_date = path.parent.name
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_actions.extend(
            {**row, "report_date": report_date, "mode": "raw_report"}
            for row in ((payload.get("production_decision_summary") or {}).get("actionable") or [])
        )

        payload["strategy_alpha_bulletin"] = generator.load_strategy_alpha_bulletin(report_date)
        payload["us_market_data_status"] = generator.build_us_market_data_status(
            us_db, generator.parse_date(report_date)
        )
        payload["production_decision_summary"] = generator.build_production_decision_summary(payload)
        gated_actions.extend(
            {**row, "report_date": report_date, "mode": "contract_gated"}
            for row in ((payload.get("production_decision_summary") or {}).get("actionable") or [])
        )
    return raw_actions, gated_actions


def load_prices(
    db_path: Path,
    market: str,
    symbols: set[str],
    through_date: str | None = None,
) -> dict[str, list[tuple[str, float]]]:
    if not db_path.exists() or not symbols:
        return {}
    if market == "CN":
        table, sym_col, date_col = "prices", "ts_code", "trade_date"
    else:
        table, sym_col, date_col = "prices_daily", "symbol", "date"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ",".join("?" for _ in symbols)
        params: list[Any] = sorted(symbols)
        date_filter = ""
        if through_date:
            date_filter = f" AND CAST({date_col} AS VARCHAR) <= ?"
            params.append(through_date)
        rows = con.execute(
            f"""
            SELECT {sym_col} AS symbol, {date_col} AS d, close
            FROM {table}
            WHERE {sym_col} IN ({placeholders}) AND close IS NOT NULL{date_filter}
            ORDER BY symbol, d
            """,
            params,
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for sym, date_value, close in rows:
        out[str(sym).upper()].append((str(date_value), float(close)))
    return dict(out)


def forward_returns(
    prices: dict[str, list[tuple[str, float]]],
    symbol: str,
    report_date: str,
) -> dict[str, Any]:
    series = prices.get(symbol.upper()) or []
    base_idx = None
    for idx, (date_value, _close) in enumerate(series):
        if date_value >= report_date:
            base_idx = idx
            break
    if base_idx is None:
        return {"status": "missing_base"}

    base_date, base_close = series[base_idx]
    out: dict[str, Any] = {
        "status": "ok" if base_date == report_date else "stale_or_future_base",
        "base_date": base_date,
        "base_close": base_close,
    }
    for horizon in HORIZONS:
        if base_idx + horizon < len(series):
            target_date, target_close = series[base_idx + horizon]
            out[f"d{horizon}"] = target_date
            out[f"r{horizon}"] = target_close / base_close - 1.0
        else:
            out[f"d{horizon}"] = None
            out[f"r{horizon}"] = None
    next_five = series[base_idx + 1:base_idx + 6]
    if next_five:
        values = [close / base_close - 1.0 for _date, close in next_five]
        out["max_up_5"] = max(values)
        out["max_down_5"] = min(values)
    else:
        out["max_up_5"] = None
        out["max_down_5"] = None
    return out


def build_trade_rows(
    actions: list[dict[str, Any]],
    prices_by_market: dict[str, dict[str, list[tuple[str, float]]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action in actions:
        market = symbol_key(action.get("market"))
        symbol = symbol_key(action.get("symbol"))
        if market not in {"CN", "US"} or not symbol:
            continue
        row = {
            "mode": action.get("mode"),
            "report_date": action.get("report_date"),
            "market": market,
            "symbol": symbol,
            "name": action.get("name") or "",
            "size_r": float(action.get("size_r") or 0.0),
            "source": action.get("source") or "",
            "tier": action.get("tier") or "",
            "action": action.get("action") or "",
        }
        row.update(forward_returns(prices_by_market[market], symbol, str(action.get("report_date"))))
        rows.append(row)
    return rows


def summarize_trades(rows: list[dict[str, Any]], horizon: int) -> dict[str, Any] | None:
    key = f"r{horizon}"
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    weighted = [
        (float(row[key]), float(row.get("size_r") or 0.0))
        for row in rows
        if row.get(key) is not None and float(row.get("size_r") or 0.0) > 0
    ]
    if not values:
        return None
    weight_sum = sum(weight for _value, weight in weighted)
    return {
        "n": len(values),
        "avg": sum(values) / len(values),
        "median": statistics.median(values),
        "win_rate": sum(value > 0 for value in values) / len(values),
        "weighted_avg": (
            sum(value * weight for value, weight in weighted) / weight_sum if weight_sum > 0 else None
        ),
        "best": max(values),
        "worst": min(values),
    }


def weighted_date_return(rows: list[dict[str, Any]], horizon: int) -> float | None:
    key = f"r{horizon}"
    valid = [row for row in rows if row.get(key) is not None]
    if not valid:
        return None
    weight_sum = sum(float(row.get("size_r") or 0.0) for row in valid)
    if weight_sum > 0:
        return sum(float(row[key]) * float(row.get("size_r") or 0.0) for row in valid) / weight_sum
    return sum(float(row[key]) for row in valid) / len(valid)


def pct(value: Any) -> str:
    if value is None:
        return "-"
    try:
        parsed = float(value) * 100.0
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(parsed):
        return "-"
    return f"{parsed:+.2f}%"


def compact_pct(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return round(parsed * 100.0, 6) if math.isfinite(parsed) else None


def build_benchmark_rows(
    dates: list[str],
    prices_by_market: dict[str, dict[str, list[tuple[str, float]]]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for date_value in dates:
        for market, symbols in BENCHMARKS.items():
            for symbol in symbols:
                out[(date_value, market, symbol)] = forward_returns(
                    prices_by_market[market], symbol, date_value
                )
    return out


def build_summary(
    dates: list[str],
    raw_actions: list[dict[str, Any]],
    gated_actions: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    benchmark_rows: dict[tuple[str, str, str], dict[str, Any]],
    prices_by_market: dict[str, dict[str, list[tuple[str, float]]]],
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "dates": dates,
        "report_count": len(dates),
        "raw_action_count": len(raw_actions),
        "contract_gated_action_count": len(gated_actions),
        "by_mode_market": {},
        "by_date_1d": [],
        "benchmark_availability": {},
        "gate_delta_by_date": [],
        "worst_best_1d": {},
    }
    for mode in ("raw_report", "contract_gated"):
        for market in ("CN", "US"):
            subset = [row for row in trade_rows if row["mode"] == mode and row["market"] == market]
            key = f"{mode}:{market}"
            summary["by_mode_market"][key] = {
                "trades": len(subset),
                "missing_base": sum(1 for row in subset if row.get("status") == "missing_base"),
                "horizons": {
                    str(horizon): summarize_trades(subset, horizon) for horizon in HORIZONS
                },
            }

    for mode in ("raw_report", "contract_gated"):
        for date_value in dates:
            row_out: dict[str, Any] = {"mode": mode, "report_date": date_value}
            for market in ("CN", "US"):
                subset = [
                    row
                    for row in trade_rows
                    if row["mode"] == mode and row["market"] == market and row["report_date"] == date_value
                ]
                one_day = weighted_date_return(subset, 1)
                primary = PRIMARY_BENCHMARK[market]
                bench = benchmark_rows[(date_value, market, primary)].get("r1")
                row_out[f"{market.lower()}_n"] = len(subset)
                row_out[f"{market.lower()}_gross_r"] = round(sum(float(row.get("size_r") or 0.0) for row in subset), 6)
                row_out[f"{market.lower()}_weighted_1d"] = one_day
                row_out[f"{market.lower()}_primary_benchmark"] = primary
                row_out[f"{market.lower()}_benchmark_1d"] = bench
                row_out[f"{market.lower()}_active_1d"] = one_day - bench if one_day is not None and bench is not None else None
                row_out[f"{market.lower()}_missing_forward"] = sum(1 for row in subset if row.get("r1") is None)
            summary["by_date_1d"].append(row_out)

    raw_by = Counter((row["report_date"], row["market"]) for row in raw_actions)
    gated_by = Counter((row["report_date"], row["market"]) for row in gated_actions)
    for date_value in dates:
        for market in ("CN", "US"):
            raw_count = raw_by[(date_value, market)]
            gated_count = gated_by[(date_value, market)]
            if raw_count != gated_count:
                summary["gate_delta_by_date"].append(
                    {
                        "report_date": date_value,
                        "market": market,
                        "raw_count": raw_count,
                        "contract_gated_count": gated_count,
                    }
                )

    for market in ("CN", "US"):
        raw_subset = [
            row for row in trade_rows
            if row["mode"] == "raw_report" and row["market"] == market and row.get("r1") is not None
        ]
        summary["worst_best_1d"][market] = {
            "worst": sorted(raw_subset, key=lambda row: row["r1"])[:10],
            "best": sorted(raw_subset, key=lambda row: row["r1"], reverse=True)[:10],
        }

    for market, symbols in BENCHMARKS.items():
        for symbol in symbols:
            series = prices_by_market[market].get(symbol) or []
            summary["benchmark_availability"][f"{market}:{symbol}"] = {
                "rows": len(series),
                "first_date": series[0][0] if series else None,
                "last_date": series[-1][0] if series else None,
            }
    return summary


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Main Strategy V2 Daily Report Backtest - {summary['dates'][-1] if summary['dates'] else '-'}",
        "",
        "Close-to-close signal backtest for existing daily report actionables. This is not a live fill/PnL ledger.",
        "",
        f"- Report dates: {summary['dates'][0] if summary['dates'] else '-'} to {summary['dates'][-1] if summary['dates'] else '-'} ({summary['report_count']} reports)",
        f"- Raw report actions: {summary['raw_action_count']}",
        f"- Contract-gated actions: {summary['contract_gated_action_count']}",
        "",
        "## Summary By Market",
        "",
        "| Mode | Market | Trades | Missing Base | Horizon | N | Avg | R-weighted | Median | Win | Best | Worst |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, data in summary["by_mode_market"].items():
        mode, market = key.split(":")
        for horizon in HORIZONS:
            stats = data["horizons"].get(str(horizon))
            if not stats:
                continue
            lines.append(
                f"| {mode} | {market} | {data['trades']} | {data['missing_base']} | {horizon}D | "
                f"{stats['n']} | {pct(stats['avg'])} | {pct(stats['weighted_avg'])} | "
                f"{pct(stats['median'])} | {stats['win_rate']:.1%} | {pct(stats['best'])} | {pct(stats['worst'])} |"
            )

    lines += [
        "",
        "## Daily 1D Weighted Returns",
        "",
        "| Mode | Date | CN n/R | CN 1D | CN vs 000300 | US n/R | US 1D | US vs SPY |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["by_date_1d"]:
        lines.append(
            f"| {row['mode']} | {row['report_date']} | "
            f"{row['cn_n']} / {row['cn_gross_r']:.4f}R | {pct(row['cn_weighted_1d'])} | {pct(row['cn_active_1d'])} | "
            f"{row['us_n']} / {row['us_gross_r']:.4f}R | {pct(row['us_weighted_1d'])} | {pct(row['us_active_1d'])} |"
        )

    lines += [
        "",
        "## Gate Delta",
        "",
        "| Date | Market | Raw Count | Contract-gated Count |",
        "|---|---|---:|---:|",
    ]
    for row in summary["gate_delta_by_date"]:
        lines.append(
            f"| {row['report_date']} | {row['market']} | {row['raw_count']} | {row['contract_gated_count']} |"
        )
    if not summary["gate_delta_by_date"]:
        lines.append("| - | - | - | - |")

    lines += [
        "",
        "## Benchmark Data Availability",
        "",
        "| Market:Symbol | Rows | First | Last |",
        "|---|---:|---|---|",
    ]
    for key, row in summary["benchmark_availability"].items():
        lines.append(f"| {key} | {row['rows']} | {row['first_date'] or '-'} | {row['last_date'] or '-'} |")

    lines += ["", "## Worst / Best Raw 1D", ""]
    for market in ("CN", "US"):
        lines += [f"### {market}", "", "| Side | Date | Symbol | Return | Size |", "|---|---|---|---:|---:|"]
        for side in ("worst", "best"):
            for row in summary["worst_best_1d"][market][side][:10]:
                lines.append(
                    f"| {side} | {row['report_date']} | {row['symbol']} | {pct(row['r1'])} | {row['size_r']:.4f}R |"
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(output_dir: Path, summary: dict[str, Any], trade_rows: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report_action_backtest_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "report_action_backtest_summary.md").write_text(
        render_markdown(summary),
        encoding="utf-8",
    )
    csv_path = output_dir / "report_action_backtest_trades.csv"
    fieldnames = [
        "mode", "report_date", "market", "symbol", "name", "size_r", "source", "tier",
        "status", "base_date", "base_close",
        "r1", "d1", "r3", "d3", "r5", "d5", "r10", "d10", "max_up_5", "max_down_5",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(trade_rows)


def main() -> None:
    args = parse_args()
    generator = load_generator_module()
    paths = report_paths(args.report_root, args.through_date, args.before_date)
    if not paths:
        raise SystemExit(f"no report json files under {args.report_root}")
    dates = [path.parent.name for path in paths]
    raw_actions, gated_actions = load_report_actions(paths, generator, args.us_db)
    all_actions = raw_actions + gated_actions

    symbols_by_market: dict[str, set[str]] = {"CN": set(BENCHMARKS["CN"]), "US": set(BENCHMARKS["US"])}
    for action in all_actions:
        market = symbol_key(action.get("market"))
        symbol = symbol_key(action.get("symbol"))
        if market in symbols_by_market and symbol:
            symbols_by_market[market].add(symbol)

    price_through_date = args.price_through_date or args.before_date or args.through_date
    prices_by_market = {
        "CN": load_prices(args.cn_db, "CN", symbols_by_market["CN"], price_through_date),
        "US": load_prices(args.us_db, "US", symbols_by_market["US"], price_through_date),
    }
    trade_rows = build_trade_rows(all_actions, prices_by_market)
    benchmark_rows = build_benchmark_rows(dates, prices_by_market)
    summary = build_summary(
        dates,
        raw_actions,
        gated_actions,
        trade_rows,
        benchmark_rows,
        prices_by_market,
    )
    if not args.no_write:
        output_dir = args.output_dir or (
            STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2_report_backtest" / dates[-1]
        )
        write_outputs(output_dir, summary, trade_rows)
        print(f"wrote {output_dir}")
    if not args.quiet:
        print(render_markdown(summary))


if __name__ == "__main__":
    main()
