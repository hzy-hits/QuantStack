#!/usr/bin/env python3
"""Backtest Gamma v2 as a standalone US entry-alpha engine.

This is cross-sectional and can either scan the full local optionable universe
or only AI Infra names from global_universe_v2.jsonl. It reuses the production
Gamma v2 scoring function, then simulates close-to-next-close stock returns for
the top ranked entry signals.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
US_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(US_SRC) not in sys.path:
    sys.path.insert(0, str(US_SRC))

from quant_bot.analytics.us_opportunity_ranker import latest_gamma_v2_alpha  # noqa: E402


DEFAULT_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "gamma_v2_alpha_engine_backtest"
DEFAULT_AI_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"

BENCHMARKS = ("SPY", "QQQ", "SMH")
ETF_EXCLUDE = {
    "SPY",
    "QQQ",
    "SMH",
    "VOO",
    "VTI",
    "IWM",
    "DIA",
    "TLT",
    "IEF",
    "SHY",
    "HYG",
    "LQD",
    "GLD",
    "SLV",
    "USO",
    "UUP",
    "UVXY",
    "SVXY",
    "VIXY",
    "VXX",
    "BITO",
    "BITX",
    "BITI",
    "IBIT",
    "FBTC",
    "XLK",
    "XLF",
    "XLE",
    "XLY",
    "XLI",
    "XLV",
    "XLP",
    "XLU",
    "XLB",
    "XLRE",
    "XLC",
    "SOXX",
    "SOXL",
    "SQQQ",
    "TQQQ",
    "SPXL",
    "SPXS",
    "UPRO",
    "ARKK",
    "ARKQ",
    "ARKW",
    "ARKG",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 3, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 5, 29))
    parser.add_argument("--max-dte", type=int, default=45)
    parser.add_argument("--min-score", type=float, default=64.0)
    parser.add_argument("--top-n", default="5,10,20")
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--min-price", type=float, default=0.0)
    parser.add_argument("--min-avg-dollar-volume", type=float, default=0.0)
    parser.add_argument("--universe", choices=("all", "ai-infra"), default="all")
    parser.add_argument("--ai-universe-path", type=Path, default=DEFAULT_AI_UNIVERSE)
    parser.add_argument("--market-country", default="US")
    parser.add_argument("--include-etfs", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cur = con.execute(sql, params or [])
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fmt_pct(value: Any, digits: int = 2) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed * 100:+.{digits}f}%"


def fmt_num(value: Any, digits: int = 2) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed:.{digits}f}"


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def coverage(con: duckdb.DuckDBPyConnection, table: str, date_col: str) -> dict[str, Any]:
    row = con.execute(
        f"SELECT MIN({date_col}), MAX({date_col}), COUNT(*) FROM {table}"
    ).fetchone()
    return {"min": to_iso(row[0]), "max": to_iso(row[1]), "rows": int(row[2] or 0)}


def parse_top_ns(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed <= 0:
            raise ValueError("--top-n values must be positive")
        out.append(parsed)
    return sorted(set(out)) or [10]


def load_option_dates(con: duckdb.DuckDBPyConnection, start: date, end: date) -> list[date]:
    rows = con.execute(
        """
        SELECT DISTINCT as_of
        FROM options_chain_quotes
        WHERE as_of BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY as_of
        """,
        [start.isoformat(), end.isoformat()],
    ).fetchall()
    return [row[0] for row in rows]


def load_market_calendar(con: duckdb.DuckDBPyConnection, start: date, end: date) -> list[date]:
    rows = con.execute(
        """
        SELECT date
        FROM prices_daily
        WHERE symbol = 'SPY'
          AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND close IS NOT NULL
        ORDER BY date
        """,
        [start.isoformat(), (end + timedelta(days=10)).isoformat()],
    ).fetchall()
    return [row[0] for row in rows]


def stockish_symbol(symbol: str, *, include_etfs: bool) -> bool:
    symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z][A-Z0-9.]{0,5}", symbol):
        return False
    if not include_etfs and symbol in ETF_EXCLUDE:
        return False
    return True


def load_symbols(con: duckdb.DuckDBPyConnection, start: date, end: date, *, include_etfs: bool) -> list[str]:
    rows = con.execute(
        """
        SELECT symbol, COUNT(*) AS n_rows
        FROM options_chain_quotes
        WHERE as_of BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND gamma IS NOT NULL
          AND open_interest IS NOT NULL
        GROUP BY symbol
        HAVING COUNT(*) >= 20
        ORDER BY symbol
        """,
        [start.isoformat(), end.isoformat()],
    ).fetchall()
    return [str(symbol).upper() for symbol, _n in rows if stockish_symbol(str(symbol), include_etfs=include_etfs)]


def load_ai_infra_symbols(path: Path, *, market_country: str) -> set[str]:
    symbols: set[str] = set()
    if not path.exists():
        raise SystemExit(f"AI Infra universe not found: {path}")
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("market_country") or "").upper() != market_country.upper():
                continue
            symbol = str(row.get("ticker") or "").upper().strip()
            if symbol:
                symbols.add(symbol)
    return symbols


def load_tradable_symbols(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    start: date,
    end: date,
    *,
    min_price: float,
    min_avg_dollar_volume: float,
) -> list[str]:
    if not symbols or (min_price <= 0 and min_avg_dollar_volume <= 0):
        return symbols
    placeholders = ",".join(["?"] * len(symbols))
    rows = con.execute(
        f"""
        SELECT symbol,
               AVG(close) AS avg_close,
               MIN(close) AS min_close,
               AVG(close * COALESCE(volume, 0)) AS avg_dollar_volume
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND close IS NOT NULL
        GROUP BY symbol
        """,
        [*symbols, start.isoformat(), end.isoformat()],
    ).fetchall()
    eligible: set[str] = set()
    for symbol, _avg_close, min_close, avg_dollar_volume in rows:
        if min_price > 0 and (min_close is None or float(min_close) < min_price):
            continue
        if min_avg_dollar_volume > 0 and (
            avg_dollar_volume is None or float(avg_dollar_volume) < min_avg_dollar_volume
        ):
            continue
        eligible.add(str(symbol).upper())
    return [symbol for symbol in symbols if symbol in eligible]


def load_prices(con: duckdb.DuckDBPyConnection, symbols: list[str], start: date, end: date) -> dict[tuple[str, date], float]:
    if not symbols:
        return {}
    placeholders = ",".join(["?"] * len(symbols))
    rows = con.execute(
        f"""
        SELECT symbol, date, close
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND close IS NOT NULL
        """,
        [*symbols, start.isoformat(), (end + timedelta(days=10)).isoformat()],
    ).fetchall()
    return {(str(symbol).upper(), d): float(close) for symbol, d, close in rows if close and close > 0}


def summarize_returns(rows: list[dict[str, Any]], *, invested_key: str = "exposure") -> dict[str, Any]:
    returns = [float(row.get("return") or 0.0) for row in rows]
    n = len(returns)
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, equity / peak - 1.0)
    if n <= 1:
        sharpe = 0.0
    else:
        mean = sum(returns) / n
        var = sum((ret - mean) ** 2 for ret in returns) / (n - 1)
        sharpe = 0.0 if var <= 0 else mean / math.sqrt(var) * math.sqrt(252.0)
    invested = [row for row in rows if float(row.get(invested_key) or 0.0) > 0]
    total_return = equity - 1.0
    annualized = (equity ** (252.0 / n) - 1.0) if n and equity > 0 else 0.0
    return {
        "n_days": n,
        "invested_days": len(invested),
        "total_return": total_return,
        "annualized_return": annualized,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "hit_rate": (
            sum(1 for row in invested if float(row.get("return") or 0.0) > 0) / len(invested)
            if invested else None
        ),
        "avg_exposure": sum(float(row.get(invested_key) or 0.0) for row in rows) / n if n else 0.0,
        "avg_turnover": sum(float(row.get("turnover") or 0.0) for row in rows) / n if n else 0.0,
        "final_equity": equity,
    }


def turnover(prev: dict[str, float], current: dict[str, float]) -> float:
    names = set(prev) | set(current)
    return sum(abs(current.get(name, 0.0) - prev.get(name, 0.0)) for name in names)


def run_backtest(args: argparse.Namespace) -> dict[str, Any]:
    con = duckdb.connect(str(args.db), read_only=True)
    top_ns = parse_top_ns(args.top_n)
    price_cov = coverage(con, "prices_daily", "date")
    option_cov = coverage(con, "options_chain_quotes", "as_of")
    option_dates = load_option_dates(con, args.start, args.end)
    if not option_dates:
        raise SystemExit("No options_chain_quotes rows in requested window.")
    calendar = load_market_calendar(con, args.start, args.end)
    next_by_date = {calendar[idx]: calendar[idx + 1] for idx in range(len(calendar) - 1)}
    signal_dates = [d for d in option_dates if d in next_by_date]
    if not signal_dates:
        raise SystemExit("No option dates have a following SPY market date.")
    optionable_symbols = load_symbols(con, signal_dates[0], signal_dates[-1], include_etfs=args.include_etfs)
    ai_symbols: set[str] | None = None
    if args.universe == "ai-infra":
        ai_symbols = load_ai_infra_symbols(args.ai_universe_path, market_country=args.market_country)
        raw_symbols = [symbol for symbol in optionable_symbols if symbol in ai_symbols]
    else:
        raw_symbols = optionable_symbols
    symbols = load_tradable_symbols(
        con,
        raw_symbols,
        args.start,
        args.end,
        min_price=args.min_price,
        min_avg_dollar_volume=args.min_avg_dollar_volume,
    )
    price_symbols = sorted(set(symbols) | set(BENCHMARKS))
    prices = load_prices(con, price_symbols, args.start, args.end)
    cost = args.cost_bps / 10000.0

    strategy_rows: dict[str, list[dict[str, Any]]] = {f"gamma_v2_entry_top{n}": [] for n in top_ns}
    strategy_rows.update({f"{symbol}_buy_hold": [] for symbol in BENCHMARKS})
    prev_weights = {name: {} for name in strategy_rows}
    selections: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    candidate_counts: list[int] = []
    score_values: list[float] = []

    for as_of in signal_dates:
        next_date = next_by_date[as_of]
        gamma = latest_gamma_v2_alpha(con, symbols, as_of, max_dte=args.max_dte)
        candidates: list[dict[str, Any]] = []
        for symbol, fields in gamma.items():
            today = prices.get((symbol, as_of))
            nxt = prices.get((symbol, next_date))
            if not today or not nxt:
                continue
            score = float(fields.get("gamma_v2_alpha_score") or 0.0)
            if bool(fields.get("gamma_v2_entry_signal")) and score >= args.min_score:
                fwd_return = nxt / today - 1.0
                row = {
                    "date": as_of.isoformat(),
                    "next_date": next_date.isoformat(),
                    "symbol": symbol,
                    "close": today,
                    "next_close": nxt,
                    "fwd_return": fwd_return,
                    **fields,
                }
                candidates.append(row)
                score_values.append(score)
        candidates.sort(
            key=lambda row: (
                float(row.get("gamma_v2_alpha_score") or 0.0),
                float(row.get("gamma_v2_dealer_pressure_proxy") or 0.0),
                float(row.get("gamma_v2_wall_transition_score") or 0.0),
            ),
            reverse=True,
        )
        candidate_counts.append(len(candidates))
        for row in candidates:
            selections.append(row)

        daily_record = {
            "date": as_of.isoformat(),
            "next_date": next_date.isoformat(),
            "n_candidates": len(candidates),
        }
        for n in top_ns:
            name = f"gamma_v2_entry_top{n}"
            picked = candidates[:n]
            weights = {row["symbol"]: 1.0 / len(picked) for row in picked} if picked else {}
            day_turnover = turnover(prev_weights[name], weights)
            gross = sum(weights[row["symbol"]] * float(row["fwd_return"]) for row in picked)
            net = gross - day_turnover * cost
            strategy_rows[name].append(
                {
                    "date": as_of.isoformat(),
                    "next_date": next_date.isoformat(),
                    "return": net,
                    "gross_return": gross,
                    "turnover": day_turnover,
                    "exposure": sum(weights.values()),
                    "n_picks": len(picked),
                    "symbols": ",".join(row["symbol"] for row in picked),
                }
            )
            daily_record[f"{name}_return"] = net
            daily_record[f"{name}_n_picks"] = len(picked)
            daily_record[f"{name}_symbols"] = ",".join(row["symbol"] for row in picked)
            prev_weights[name] = weights

        for symbol in BENCHMARKS:
            name = f"{symbol}_buy_hold"
            today = prices.get((symbol, as_of))
            nxt = prices.get((symbol, next_date))
            weight = {symbol: 1.0} if today and nxt else {}
            day_turnover = turnover(prev_weights[name], weight)
            ret = (nxt / today - 1.0) if today and nxt else 0.0
            net = ret - day_turnover * cost
            strategy_rows[name].append(
                {
                    "date": as_of.isoformat(),
                    "next_date": next_date.isoformat(),
                    "return": net,
                    "gross_return": ret,
                    "turnover": day_turnover,
                    "exposure": sum(weight.values()),
                    "n_picks": len(weight),
                    "symbols": symbol if weight else "",
                }
            )
            daily_record[f"{name}_return"] = net
            prev_weights[name] = weight
        daily_rows.append(daily_record)

    summaries = {name: summarize_returns(rows) for name, rows in strategy_rows.items()}
    selected_counter = Counter(row["symbol"] for row in selections)
    top_selected = [
        {
            "symbol": symbol,
            "selected_days": count,
            "avg_next_return": (
                sum(float(row["fwd_return"]) for row in selections if row["symbol"] == symbol) / count
                if count else 0.0
            ),
            "avg_score": (
                sum(float(row["gamma_v2_alpha_score"]) for row in selections if row["symbol"] == symbol) / count
                if count else 0.0
            ),
        }
        for symbol, count in selected_counter.most_common(30)
    ]
    return {
        "config": {
            "requested_start": args.start.isoformat(),
            "requested_end": args.end.isoformat(),
            "actual_signal_start": signal_dates[0].isoformat(),
            "actual_signal_end": signal_dates[-1].isoformat(),
            "n_signal_days": len(signal_dates),
            "universe": args.universe,
            "ai_universe_path": str(args.ai_universe_path) if args.universe == "ai-infra" else None,
            "ai_universe_symbols": len(ai_symbols or []),
            "n_optionable_symbols": len(optionable_symbols),
            "n_raw_symbols": len(raw_symbols),
            "n_symbols": len(symbols),
            "top_n": top_ns,
            "cost_bps": args.cost_bps,
            "min_score": args.min_score,
            "max_dte": args.max_dte,
            "min_price": args.min_price,
            "min_avg_dollar_volume": args.min_avg_dollar_volume,
            "include_etfs": args.include_etfs,
            "ai_universe_filter": args.universe == "ai-infra",
            "source_evidence_filter": False,
        },
        "data_coverage": {
            "prices_daily": price_cov,
            "options_chain_quotes": option_cov,
        },
        "summary": summaries,
        "diagnostics": {
            "total_entry_candidates": len(selections),
            "avg_entry_candidates_per_day": (
                sum(candidate_counts) / len(candidate_counts) if candidate_counts else 0.0
            ),
            "max_entry_candidates_per_day": max(candidate_counts) if candidate_counts else 0,
            "avg_entry_score": sum(score_values) / len(score_values) if score_values else None,
            "top_selected": top_selected,
        },
        "daily": daily_rows,
        "selections": selections,
        "strategy_daily": strategy_rows,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(payload: dict[str, Any]) -> str:
    config = payload["config"]
    coverage_payload = payload["data_coverage"]
    summary = payload["summary"]
    diagnostics = payload["diagnostics"]
    lines = [
        f"# Gamma v2 Entry Alpha Engine Backtest - {config['requested_start']}..{config['requested_end']}",
        "",
        (
            "Scope: cross-sectional stock timing backtest. AI Infra universe filter is enabled; "
            "source-evidence state is inherited from global_universe_v2.jsonl."
            if config["ai_universe_filter"]
            else "Scope: cross-sectional stock timing backtest. AI universe and source-evidence filters are disabled."
        ),
        "Execution: close-to-next-close stock return, equal-weight top-N entry signals, no leverage.",
        "",
        "## Data Coverage",
        "",
        f"- prices_daily: {coverage_payload['prices_daily']['min']}..{coverage_payload['prices_daily']['max']} ({coverage_payload['prices_daily']['rows']} rows)",
        f"- options_chain_quotes: {coverage_payload['options_chain_quotes']['min']}..{coverage_payload['options_chain_quotes']['max']} ({coverage_payload['options_chain_quotes']['rows']} rows)",
        f"- requested: {config['requested_start']}..{config['requested_end']}",
        f"- actual signal window: {config['actual_signal_start']}..{config['actual_signal_end']} ({config['n_signal_days']} option dates)",
        f"- universe: {config['universe']}",
        f"- optionable symbols tested: {config['n_symbols']} (raw={config['n_raw_symbols']}, optionable_local={config['n_optionable_symbols']}, ai_universe={config['ai_universe_symbols']})",
        f"- tradable gate: min_price={fmt_num(config['min_price'])}, min_avg_dollar_volume={fmt_num(config['min_avg_dollar_volume'], 0)}",
        f"- filters: ai_universe={config['ai_universe_filter']}, source_evidence={config['source_evidence_filter']}",
        "",
        "## Strategy Results",
        "",
        "| Strategy | Days | Invested | Total | Ann. | Sharpe | Max DD | Hit | Avg exposure | Avg turnover |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    order = [f"gamma_v2_entry_top{n}" for n in config["top_n"]] + [f"{symbol}_buy_hold" for symbol in BENCHMARKS]
    for name in order:
        stats = summary.get(name) or {}
        lines.append(
            f"| {name} | {stats.get('n_days', 0)} | {stats.get('invested_days', 0)} | "
            f"{fmt_pct(stats.get('total_return'))} | {fmt_pct(stats.get('annualized_return'))} | "
            f"{fmt_num(stats.get('sharpe'))} | {fmt_pct(stats.get('max_drawdown'))} | "
            f"{fmt_pct(stats.get('hit_rate'))} | {fmt_num(stats.get('avg_exposure'))} | "
            f"{fmt_num(stats.get('avg_turnover'))} |"
        )
    lines += [
        "",
        "## Diagnostics",
        "",
        f"- total entry candidates: {diagnostics['total_entry_candidates']}",
        f"- avg entry candidates/day: {fmt_num(diagnostics['avg_entry_candidates_per_day'])}",
        f"- max entry candidates/day: {diagnostics['max_entry_candidates_per_day']}",
        f"- avg entry score: {fmt_num(diagnostics['avg_entry_score'])}",
        "",
        "## Most Selected Symbols",
        "",
        "| Symbol | Selected days | Avg next | Avg score |",
        "|---|---:|---:|---:|",
    ]
    for row in diagnostics.get("top_selected", [])[:20]:
        lines.append(
            f"| {row['symbol']} | {row['selected_days']} | {fmt_pct(row['avg_next_return'])} | {fmt_num(row['avg_score'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if args.start > args.end:
        raise SystemExit("--start must be <= --end")
    payload = run_backtest(args)
    variant = f"{args.start.isoformat()}_{args.end.isoformat()}"
    if args.universe != "all":
        variant += f"_{args.universe}"
    if args.min_price > 0 or args.min_avg_dollar_volume > 0 or args.include_etfs:
        variant += f"_px{args.min_price:g}_adv{args.min_avg_dollar_volume:g}"
        if args.include_etfs:
            variant += "_with_etfs"
    run_dir = args.output_root / variant
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "backtest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "backtest.md").write_text(render_markdown(payload), encoding="utf-8")
    write_csv(run_dir / "daily.csv", payload["daily"])
    write_csv(run_dir / "selections.csv", payload["selections"])
    flat_strategy_rows: list[dict[str, Any]] = []
    for strategy, rows in payload["strategy_daily"].items():
        for row in rows:
            flat_strategy_rows.append({"strategy": strategy, **row})
    write_csv(run_dir / "strategy_daily.csv", flat_strategy_rows)
    print(run_dir / "backtest.md")
    print(render_markdown(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
