"""US top-100 mean-reversion radar.

Premise: when the broad market is up but a large-cap is down — particularly if
its price is below its 21-day EMA and the EMA slope has flipped lower — the
relative weakness can be a mean-reversion signal worth tracking. This is a
*radar*, not an entry rule.

Pipeline:

1. Read latest market caps from `company_profile` in the US DuckDB, pick the
   top 100 by market cap. (`market_cap` is stored in **millions** of USD.)
2. Load 60+ trading days of prices from `prices_daily` for those 100 names
   plus the SPY/QQQ benchmarks.
3. Compute per name: trailing 5d/20d return, EMA21/EMA50, close-vs-EMA21
   distance, EMA21 5d slope.
4. Flag mean-reversion candidates when:
       * benchmark 5d return ≥ +1%
       * stock 5d return ≤ -2%
       * stock price < EMA21 by ≥ 2%
       * stock 20d return is in the lower half of the top-100 cohort
5. Cross-reference against the AI universe so the operator can tell if an
   underperformer is also a source-reviewed AI name.

Outputs land in
`reports/review_dashboard/us_mean_reversion_radar/<date>/mean_reversion_radar.{csv,md}`.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_AI_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_mean_reversion_radar"


BENCHMARKS = ("SPY", "QQQ")


@dataclass(frozen=True)
class RadarRow:
    rank: int
    symbol: str
    company_name: str
    sector: str
    market_cap_b: float
    latest_close: float | None
    ret_5d_pct: float | None
    ret_20d_pct: float | None
    ema21: float | None
    ema50: float | None
    slope_ema21_5d_pct: float | None
    dist_close_ema21_pct: float | None
    dist_close_ema50_pct: float | None
    in_ai_universe: bool
    is_mean_reversion_candidate: bool
    reasons: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "symbol": self.symbol,
            "company_name": self.company_name,
            "sector": self.sector,
            "market_cap_usd_b": f"{self.market_cap_b:.2f}",
            "latest_close": self.latest_close if self.latest_close is not None else "",
            "ret_5d_pct": f"{self.ret_5d_pct:.3f}" if self.ret_5d_pct is not None else "",
            "ret_20d_pct": f"{self.ret_20d_pct:.3f}" if self.ret_20d_pct is not None else "",
            "ema21": self.ema21 if self.ema21 is not None else "",
            "ema50": self.ema50 if self.ema50 is not None else "",
            "slope_ema21_5d_pct": f"{self.slope_ema21_5d_pct:.3f}" if self.slope_ema21_5d_pct is not None else "",
            "dist_close_ema21_pct": (
                f"{self.dist_close_ema21_pct:.3f}" if self.dist_close_ema21_pct is not None else ""
            ),
            "dist_close_ema50_pct": (
                f"{self.dist_close_ema50_pct:.3f}" if self.dist_close_ema50_pct is not None else ""
            ),
            "in_ai_universe": "yes" if self.in_ai_universe else "no",
            "is_mean_reversion_candidate": "yes" if self.is_mean_reversion_candidate else "no",
            "reasons": ";".join(self.reasons),
        }


def _load_top_n_universe(con: duckdb.DuckDBPyConnection, top_n: int) -> list[tuple[str, str, str, float]]:
    rows = con.execute(
        """
        WITH latest AS (
            SELECT symbol, company_name, sector, market_cap,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) AS rn
            FROM company_profile
            WHERE market_cap IS NOT NULL AND market_cap > 0
        )
        SELECT symbol, COALESCE(company_name, ''), COALESCE(sector, ''), market_cap
        FROM latest
        WHERE rn = 1
        ORDER BY market_cap DESC NULLS LAST
        LIMIT ?
        """,
        [top_n],
    ).fetchall()
    return [(r[0], r[1], r[2], float(r[3])) for r in rows]


def _load_prices(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date, lookback_days: int = 120) -> dict[str, list[tuple[date, float]]]:
    if not symbols:
        return {}
    start = as_of - timedelta(days=lookback_days * 2)
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, date, close
        FROM prices_daily
        WHERE date >= CAST(? AS DATE)
          AND date <= CAST(? AS DATE)
          AND symbol IN ({placeholders})
          AND close IS NOT NULL
        ORDER BY symbol, date
        """,
        [start.isoformat(), as_of.isoformat(), *symbols],
    ).fetchall()
    series: dict[str, list[tuple[date, float]]] = {}
    for sym, d, close in rows:
        if isinstance(d, str):
            d = date.fromisoformat(d)
        series.setdefault(sym, []).append((d, float(close)))
    return series


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def _trailing_return_pct(closes: list[float], periods: int) -> float | None:
    if not closes or len(closes) <= periods:
        return None
    base = closes[-1 - periods]
    if not base:
        return None
    return (closes[-1] / base - 1.0) * 100.0


def _load_ai_universe(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ticker_field = (row.get("ticker") or "").strip()
            if not ticker_field:
                continue
            for piece in ticker_field.split("/"):
                token = piece.strip().upper()
                if token and token.isalpha() and len(token) <= 5:
                    out.add(token)
    return out


def build_radar(
    *,
    us_db: Path,
    ai_universe_path: Path,
    as_of: date,
    top_n: int = 100,
) -> list[RadarRow]:
    if not us_db.exists():
        raise FileNotFoundError(f"US DuckDB missing: {us_db}")
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        top = _load_top_n_universe(con, top_n)
        symbols = [s for s, *_ in top]
        prices = _load_prices(con, symbols + list(BENCHMARKS), as_of)
    finally:
        con.close()

    benchmark_5d: dict[str, float | None] = {}
    for bench in BENCHMARKS:
        closes = [c for _, c in prices.get(bench, [])]
        benchmark_5d[bench] = _trailing_return_pct(closes, 5)

    ai_universe = _load_ai_universe(ai_universe_path)

    rows: list[RadarRow] = []
    cohort_20d: list[tuple[str, float]] = []
    cohort_metrics: dict[str, dict[str, Any]] = {}
    for rank, (symbol, company_name, sector, market_cap) in enumerate(top, start=1):
        series = prices.get(symbol) or []
        closes = [c for _, c in series]
        latest_close = closes[-1] if closes else None
        ret_5d = _trailing_return_pct(closes, 5)
        ret_20d = _trailing_return_pct(closes, 20)
        ema21 = _ema(closes, 21)
        ema50 = _ema(closes, 50)
        ema21_last = ema21[-1] if ema21 else None
        ema50_last = ema50[-1] if ema50 else None
        slope = None
        if len(ema21) >= 6 and ema21[-6] > 0:
            slope = (ema21[-1] - ema21[-6]) / ema21[-6] * 100.0
        dist21 = None
        dist50 = None
        if ema21_last and latest_close is not None:
            dist21 = (latest_close / ema21_last - 1.0) * 100.0
        if ema50_last and latest_close is not None:
            dist50 = (latest_close / ema50_last - 1.0) * 100.0
        if ret_20d is not None:
            cohort_20d.append((symbol, ret_20d))
        cohort_metrics[symbol] = {
            "rank": rank,
            "company_name": company_name,
            "sector": sector,
            "market_cap_b": market_cap / 1_000.0,  # company_profile stores millions.
            "latest_close": latest_close,
            "ret_5d_pct": ret_5d,
            "ret_20d_pct": ret_20d,
            "ema21": round(ema21_last, 4) if ema21_last else None,
            "ema50": round(ema50_last, 4) if ema50_last else None,
            "slope": slope,
            "dist21": dist21,
            "dist50": dist50,
        }

    cohort_20d.sort(key=lambda pair: pair[1])
    median_idx = max(0, len(cohort_20d) // 2 - 1)
    bottom_half_threshold = cohort_20d[median_idx][1] if cohort_20d else None

    spy_5d = benchmark_5d.get("SPY")
    qqq_5d = benchmark_5d.get("QQQ")
    market_up = (spy_5d or 0.0) >= 1.0 or (qqq_5d or 0.0) >= 1.0

    for symbol, info in cohort_metrics.items():
        reasons: list[str] = []
        ret_5d = info["ret_5d_pct"]
        ret_20d = info["ret_20d_pct"]
        dist21 = info["dist21"]
        slope = info["slope"]

        if market_up and ret_5d is not None and ret_5d <= -2.0:
            reasons.append(f"lagging_market_5d:{ret_5d:.1f}%")
        if dist21 is not None and dist21 <= -2.0:
            reasons.append(f"below_ema21:{dist21:.1f}%")
        if (
            bottom_half_threshold is not None
            and ret_20d is not None
            and ret_20d <= bottom_half_threshold
            and ret_20d <= 0.0
        ):
            reasons.append(f"bottom_half_20d:{ret_20d:.1f}%")
        # Slope must still be down — momentum hasn't turned yet.
        slope_negative = slope is not None and slope < 0
        is_candidate = (
            "lagging_market_5d" in ",".join(reasons)
            and "below_ema21" in ",".join(reasons)
            and slope_negative
        )
        if slope_negative:
            reasons.append(f"ema21_slope:{slope:.2f}%")

        rows.append(
            RadarRow(
                rank=info["rank"],
                symbol=symbol,
                company_name=info["company_name"],
                sector=info["sector"],
                market_cap_b=info["market_cap_b"],
                latest_close=info["latest_close"],
                ret_5d_pct=ret_5d,
                ret_20d_pct=ret_20d,
                ema21=info["ema21"],
                ema50=info["ema50"],
                slope_ema21_5d_pct=slope,
                dist_close_ema21_pct=dist21,
                dist_close_ema50_pct=info["dist50"],
                in_ai_universe=symbol in ai_universe,
                is_mean_reversion_candidate=is_candidate,
                reasons=reasons,
            )
        )

    rows.sort(key=lambda r: r.rank)
    return rows


def render_markdown(rows: list[RadarRow], as_of: str) -> str:
    candidates = [r for r in rows if r.is_mean_reversion_candidate]
    lines: list[str] = [
        f"# US Top-100 Mean-Reversion Radar - {as_of}",
        "",
        "- 数据源: `company_profile` (latest market_cap) + `prices_daily` + AI Infra universe。",
        "- 触发: 大盘 5d ≥ +1% 且 个股 5d ≤ -2% 且 收盘价低于 EMA21 ≥ 2% 且 EMA21 5d slope < 0。",
        "- 用法: *radar*，不是买入许可。是「市场涨而股票跑输」的均值回归潜在线索；需要叠加基本面 / 行业 / 事件再做决定。",
        "",
        f"- 总数: {len(rows)} 名；候选: {len(candidates)}；AI universe 重合: {sum(1 for r in candidates if r.in_ai_universe)}。",
        "",
    ]
    if candidates:
        lines += [
            "## Mean-Reversion Candidates",
            "",
            "| Rank | Symbol | Company | Sector | Mcap | 5d | 20d | px vs EMA21 | EMA21 slope | AI universe | Reasons |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
        for row in candidates[:40]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.rank),
                        row.symbol,
                        (row.company_name or "-")[:24],
                        (row.sector or "-")[:18],
                        f"${row.market_cap_b:.1f}B",
                        f"{row.ret_5d_pct:+.2f}%" if row.ret_5d_pct is not None else "-",
                        f"{row.ret_20d_pct:+.2f}%" if row.ret_20d_pct is not None else "-",
                        f"{row.dist_close_ema21_pct:+.2f}%" if row.dist_close_ema21_pct is not None else "-",
                        f"{row.slope_ema21_5d_pct:+.2f}%" if row.slope_ema21_5d_pct is not None else "-",
                        "yes" if row.in_ai_universe else "no",
                        ";".join(row.reasons),
                    ]
                )
                + " |"
            )
        lines.append("")
    else:
        lines += ["## Mean-Reversion Candidates", "", "_当日无名字满足全部触发条件。_", ""]

    lines += [
        "## Top-100 Reference Snapshot",
        "",
        "| Rank | Symbol | Company | Mcap | 5d | 20d | px vs EMA21 | EMA21 slope | AI |",
        "|---:|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:100]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.rank),
                    row.symbol,
                    (row.company_name or "-")[:24],
                    f"${row.market_cap_b:.1f}B",
                    f"{row.ret_5d_pct:+.2f}%" if row.ret_5d_pct is not None else "-",
                    f"{row.ret_20d_pct:+.2f}%" if row.ret_20d_pct is not None else "-",
                    f"{row.dist_close_ema21_pct:+.2f}%" if row.dist_close_ema21_pct is not None else "-",
                    f"{row.slope_ema21_5d_pct:+.2f}%" if row.slope_ema21_5d_pct is not None else "-",
                    "y" if row.in_ai_universe else "n",
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(rows: list[RadarRow], out_dir: Path, as_of: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # Match the keys produced by `RadarRow.as_dict` (which renames market_cap_b
    # to market_cap_usd_b and serialises floats as strings).
    fieldnames = [
        "rank",
        "symbol",
        "company_name",
        "sector",
        "market_cap_usd_b",
        "latest_close",
        "ret_5d_pct",
        "ret_20d_pct",
        "ema21",
        "ema50",
        "slope_ema21_5d_pct",
        "dist_close_ema21_pct",
        "dist_close_ema50_pct",
        "in_ai_universe",
        "is_mean_reversion_candidate",
        "reasons",
    ]
    with (out_dir / "mean_reversion_radar.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
    (out_dir / "mean_reversion_radar.md").write_text(render_markdown(rows, as_of), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-universe", type=Path, default=DEFAULT_AI_UNIVERSE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--top-n", type=int, default=100)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of_text = args.as_of or cst.date().isoformat()
    as_of = date.fromisoformat(as_of_text)

    rows = build_radar(
        us_db=args.us_db,
        ai_universe_path=args.ai_universe,
        as_of=as_of,
        top_n=args.top_n,
    )
    out_dir = args.output_root / as_of_text
    write_outputs(rows, out_dir, as_of_text)
    candidates = sum(1 for r in rows if r.is_mean_reversion_candidate)
    ai_overlap = sum(1 for r in rows if r.is_mean_reversion_candidate and r.in_ai_universe)
    print(
        f"Mean-reversion radar written: {out_dir / 'mean_reversion_radar.md'}; "
        f"top_n={len(rows)} candidates={candidates} ai_overlap={ai_overlap}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
