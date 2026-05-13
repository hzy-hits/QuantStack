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
    next_earnings_date: str | None
    days_to_earnings: int | None
    earnings_block: bool
    pe_ttm: float | None
    ps_ratio: float | None
    ev_ebitda: float | None
    sector_pe_median: float | None
    sector_ps_median: float | None
    valuation_signal: str  # cheap_vs_sector / fair / rich / unknown

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
            "next_earnings_date": self.next_earnings_date or "",
            "days_to_earnings": self.days_to_earnings if self.days_to_earnings is not None else "",
            "earnings_block": "yes" if self.earnings_block else "no",
            "pe_ttm": f"{self.pe_ttm:.2f}" if self.pe_ttm is not None else "",
            "ps_ratio": f"{self.ps_ratio:.2f}" if self.ps_ratio is not None else "",
            "ev_ebitda": f"{self.ev_ebitda:.2f}" if self.ev_ebitda is not None else "",
            "sector_pe_median": f"{self.sector_pe_median:.2f}" if self.sector_pe_median is not None else "",
            "sector_ps_median": f"{self.sector_ps_median:.2f}" if self.sector_ps_median is not None else "",
            "valuation_signal": self.valuation_signal,
        }


def _load_top_n_universe(
    con: duckdb.DuckDBPyConnection,
    top_n: int,
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        WITH latest AS (
            SELECT symbol, company_name, sector, market_cap,
                   pe_ttm, ps_ratio, ev_ebitda,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) AS rn
            FROM company_profile
            WHERE market_cap IS NOT NULL AND market_cap > 0
        )
        SELECT symbol, COALESCE(company_name, ''), COALESCE(sector, ''),
               market_cap, pe_ttm, ps_ratio, ev_ebitda
        FROM latest
        WHERE rn = 1
        ORDER BY market_cap DESC NULLS LAST
        LIMIT ?
        """,
        [top_n],
    ).fetchall()
    return [
        {
            "symbol": r[0],
            "company_name": r[1],
            "sector": r[2],
            "market_cap": float(r[3]),
            "pe_ttm": float(r[4]) if r[4] is not None else None,
            "ps_ratio": float(r[5]) if r[5] is not None else None,
            "ev_ebitda": float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]


def _load_next_earnings(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    window_days: int = 90,
) -> dict[str, date]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, MIN(report_date) AS next_date
        FROM earnings_calendar
        WHERE symbol IN ({placeholders})
          AND report_date IS NOT NULL
          AND CAST(report_date AS DATE) >= CAST(? AS DATE)
          AND CAST(report_date AS DATE) <= CAST(? AS DATE)
        GROUP BY symbol
        """,
        [*symbols, as_of.isoformat(), (as_of + timedelta(days=window_days)).isoformat()],
    ).fetchall()
    out: dict[str, date] = {}
    for sym, dt in rows:
        if dt is None:
            continue
        if isinstance(dt, str):
            try:
                dt = date.fromisoformat(dt)
            except ValueError:
                continue
        out[sym] = dt
    return out


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _sector_medians(profiles: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    by_sector: dict[str, list[dict[str, Any]]] = {}
    for entry in profiles:
        sector = entry.get("sector") or ""
        by_sector.setdefault(sector, []).append(entry)
    out: dict[str, dict[str, float | None]] = {}
    for sector, members in by_sector.items():
        pe_values = [m["pe_ttm"] for m in members if m.get("pe_ttm") and m["pe_ttm"] > 0]
        ps_values = [m["ps_ratio"] for m in members if m.get("ps_ratio") and m["ps_ratio"] > 0]
        out[sector] = {
            "pe_median": _median(pe_values),
            "ps_median": _median(ps_values),
        }
    return out


def _valuation_signal(
    pe_ttm: float | None,
    ps_ratio: float | None,
    sector_pe_median: float | None,
    sector_ps_median: float | None,
) -> str:
    if pe_ttm is None and ps_ratio is None:
        return "unknown"
    discount_pe: float | None = None
    discount_ps: float | None = None
    if pe_ttm is not None and sector_pe_median and sector_pe_median > 0:
        discount_pe = pe_ttm / sector_pe_median - 1.0
    if ps_ratio is not None and sector_ps_median and sector_ps_median > 0:
        discount_ps = ps_ratio / sector_ps_median - 1.0
    samples = [d for d in (discount_pe, discount_ps) if d is not None]
    if not samples:
        return "unknown"
    avg = sum(samples) / len(samples)
    if avg <= -0.15:
        return "cheap_vs_sector"
    if avg >= 0.30:
        return "rich_vs_sector"
    return "fair_vs_sector"


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
    earnings_block_days: int = 7,
) -> list[RadarRow]:
    if not us_db.exists():
        raise FileNotFoundError(f"US DuckDB missing: {us_db}")
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        profiles = _load_top_n_universe(con, top_n)
        symbols = [entry["symbol"] for entry in profiles]
        prices = _load_prices(con, symbols + list(BENCHMARKS), as_of)
        next_earnings = _load_next_earnings(con, symbols, as_of)
    finally:
        con.close()

    benchmark_5d: dict[str, float | None] = {}
    for bench in BENCHMARKS:
        closes = [c for _, c in prices.get(bench, [])]
        benchmark_5d[bench] = _trailing_return_pct(closes, 5)

    ai_universe = _load_ai_universe(ai_universe_path)
    sector_medians = _sector_medians(profiles)

    rows: list[RadarRow] = []
    cohort_20d: list[tuple[str, float]] = []
    cohort_metrics: dict[str, dict[str, Any]] = {}
    for rank, entry in enumerate(profiles, start=1):
        symbol = entry["symbol"]
        company_name = entry["company_name"]
        sector = entry["sector"]
        market_cap = entry["market_cap"]
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
        sector_stats = sector_medians.get(sector or "", {})
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
            "pe_ttm": entry.get("pe_ttm"),
            "ps_ratio": entry.get("ps_ratio"),
            "ev_ebitda": entry.get("ev_ebitda"),
            "sector_pe_median": sector_stats.get("pe_median"),
            "sector_ps_median": sector_stats.get("ps_median"),
            "valuation_signal": _valuation_signal(
                entry.get("pe_ttm"),
                entry.get("ps_ratio"),
                sector_stats.get("pe_median"),
                sector_stats.get("ps_median"),
            ),
            "next_earnings": next_earnings.get(symbol),
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
        if slope_negative:
            reasons.append(f"ema21_slope:{slope:.2f}%")
        # Earnings avoidance: skip names reporting within the next N trading
        # days because gap risk dwarfs any tape-driven mean-reversion edge.
        next_earnings = info.get("next_earnings")
        days_to_earnings = None
        earnings_block = False
        if isinstance(next_earnings, date):
            days_to_earnings = (next_earnings - as_of).days
            if 0 <= days_to_earnings <= earnings_block_days:
                earnings_block = True
                reasons.append(f"earnings_in_{days_to_earnings}d_block")

        is_candidate = (
            any(r.startswith("lagging_market_5d") for r in reasons)
            and any(r.startswith("below_ema21") for r in reasons)
            and slope_negative
            and not earnings_block
        )

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
                next_earnings_date=next_earnings.isoformat() if isinstance(next_earnings, date) else None,
                days_to_earnings=days_to_earnings,
                earnings_block=earnings_block,
                pe_ttm=info.get("pe_ttm"),
                ps_ratio=info.get("ps_ratio"),
                ev_ebitda=info.get("ev_ebitda"),
                sector_pe_median=info.get("sector_pe_median"),
                sector_ps_median=info.get("sector_ps_median"),
                valuation_signal=info.get("valuation_signal") or "unknown",
            )
        )

    rows.sort(key=lambda r: r.rank)
    return rows


def _render_candidate_table(rows: list[RadarRow], heading: str) -> list[str]:
    lines = [
        f"## {heading} ({len(rows)})",
        "",
        "| Rank | Symbol | Company | Sector | Mcap | 5d | 20d | px vs EMA21 | EMA21 slope | Next ER | Valuation | Reasons |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---|---|---|",
    ]
    if not rows:
        lines.append("| - | - | _无符合条件的名字_ | - | - | - | - | - | - | - | - | - |")
    for row in rows[:40]:
        er = row.next_earnings_date or "-"
        if row.days_to_earnings is not None and 0 <= row.days_to_earnings <= 30:
            er = f"{row.next_earnings_date} ({row.days_to_earnings}d)"
        valuation_chunks = []
        if row.pe_ttm is not None:
            valuation_chunks.append(f"PE {row.pe_ttm:.1f}")
        if row.sector_pe_median:
            valuation_chunks.append(f"sec {row.sector_pe_median:.1f}")
        valuation_chunks.append(row.valuation_signal)
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.rank),
                    row.symbol,
                    (row.company_name or "-")[:24],
                    (row.sector or "-")[:16],
                    f"${row.market_cap_b:.1f}B",
                    f"{row.ret_5d_pct:+.2f}%" if row.ret_5d_pct is not None else "-",
                    f"{row.ret_20d_pct:+.2f}%" if row.ret_20d_pct is not None else "-",
                    f"{row.dist_close_ema21_pct:+.2f}%" if row.dist_close_ema21_pct is not None else "-",
                    f"{row.slope_ema21_5d_pct:+.2f}%" if row.slope_ema21_5d_pct is not None else "-",
                    er,
                    " / ".join(valuation_chunks),
                    ";".join(row.reasons),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def render_markdown(rows: list[RadarRow], as_of: str) -> str:
    candidates = [r for r in rows if r.is_mean_reversion_candidate]
    ai_candidates = [r for r in candidates if r.in_ai_universe]
    non_ai_candidates = [r for r in candidates if not r.in_ai_universe]
    earnings_blocked = [r for r in rows if r.earnings_block]
    lines: list[str] = [
        f"# US Top-100 Mean-Reversion Radar - {as_of}",
        "",
        "- 数据源: `company_profile` (latest market_cap + PE/PS/EV) + `prices_daily` + `earnings_calendar` + AI Infra universe。",
        "- 触发: 大盘 5d ≥ +1% 且 个股 5d ≤ -2% 且 收盘价低于 EMA21 ≥ 2% 且 EMA21 5d slope < 0；近 7 天有财报的名字直接被屏蔽。",
        "- 用法: *radar*，不是买入许可。AI book 仍是绝对主力；这里只是 macro 滞后线索叠加 valuation gap。",
        "",
        f"- 总数: {len(rows)} 名；候选: {len(candidates)}（{len(ai_candidates)} AI / {len(non_ai_candidates)} 非 AI）；财报屏蔽: {len(earnings_blocked)}。",
        "",
    ]
    # AI book first — operator should pivot weight within the AI sleeve before
    # opportunistically looking at non-AI laggards.
    lines += _render_candidate_table(ai_candidates, "AI Universe Mean-Reversion (LEAD)")
    if non_ai_candidates:
        lines += _render_candidate_table(non_ai_candidates, "Non-AI Mean-Reversion (Context)")

    if earnings_blocked:
        lines += [
            "## Earnings-Blocked (skip until report)",
            "",
            "| Symbol | Company | Sector | Next Earnings | Days to ER | AI |",
            "|---|---|---|---|---:|---|",
        ]
        for row in earnings_blocked[:30]:
            lines.append(
                f"| {row.symbol} | {(row.company_name or '-')[:24]} | {(row.sector or '-')[:16]} | "
                f"{row.next_earnings_date or '-'} | {row.days_to_earnings if row.days_to_earnings is not None else '-'} | "
                f"{'y' if row.in_ai_universe else 'n'} |"
            )
        lines.append("")

    lines += [
        "## Top-100 Reference Snapshot",
        "",
        "| Rank | Symbol | Company | Mcap | 5d | 20d | px vs EMA21 | EMA21 slope | Valuation | AI |",
        "|---:|---|---|---:|---:|---:|---:|---:|---|---|",
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
                    row.valuation_signal,
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
        "next_earnings_date",
        "days_to_earnings",
        "earnings_block",
        "pe_ttm",
        "ps_ratio",
        "ev_ebitda",
        "sector_pe_median",
        "sector_ps_median",
        "valuation_signal",
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
