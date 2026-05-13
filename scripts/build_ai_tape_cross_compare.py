"""Cross-compare AI book leaders vs AI mean-reversion candidates on one page.

The AI book is the absolute lead of the daily research stack. This script
joins two artifacts already produced upstream:

- `reports/review_dashboard/ai_infra_ten_x_radar/<date>/ten_x_candidates.csv`
  (Top Leaders by `bull; rising` filter)
- `reports/review_dashboard/us_mean_reversion_radar/<date>/mean_reversion_radar.csv`
  (AI universe mean-reversion candidates)

…and emits a single
`reports/review_dashboard/ai_tape_cross_compare/<date>/ai_tape_cross_compare.md`
that an operator can read in one glance to decide where to lean inside the
AI sleeve: lean into bull-rising leaders, or rotate into AI laggards trading
below EMA21 while the market is up.

This script is read-only. It never modifies the radars themselves.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEN_X_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_ten_x_radar"
DEFAULT_MR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_mean_reversion_radar"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_tape_cross_compare"


@dataclass(frozen=True)
class LeaderRow:
    ticker: str
    company: str
    asset_pool: str
    bfs_depth: str
    market_cap_b: float | None
    cross_state: str
    slope_5d_pct: float | None
    dist_close_ema21_pct: float | None
    readiness_tier: str
    elasticity_score: float


@dataclass(frozen=True)
class LaggardRow:
    rank: int
    symbol: str
    company: str
    sector: str
    market_cap_b: float
    ret_5d_pct: float | None
    dist_close_ema21_pct: float | None
    slope_ema21_5d_pct: float | None
    valuation_signal: str
    next_earnings_date: str
    reasons: str


def _to_float(value: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _load_leaders(path: Path) -> list[LeaderRow]:
    if not path.exists():
        return []
    out: list[LeaderRow] = []
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            cross = row.get("ema_cross_state") or ""
            slope = _to_float(row.get("ema_slope_5d_pct"))
            if cross != "bull" or slope is None or slope <= 0.5:
                continue
            mcap = _to_float(row.get("market_cap_usd"))
            mcap_b = (mcap / 1e9) if mcap is not None else None
            elasticity = _to_float(row.get("elasticity_score")) or 0.0
            out.append(
                LeaderRow(
                    ticker=row.get("primary_ticker") or "",
                    company=row.get("company") or "",
                    asset_pool=row.get("asset_pool") or "",
                    bfs_depth=row.get("bfs_depth") or "",
                    market_cap_b=mcap_b,
                    cross_state=cross,
                    slope_5d_pct=slope,
                    dist_close_ema21_pct=_to_float(row.get("ema_dist_close_ema21_pct")),
                    readiness_tier=row.get("readiness_tier") or "",
                    elasticity_score=elasticity,
                )
            )
    out.sort(key=lambda r: -(r.slope_5d_pct or 0.0))
    return out


def _load_laggards(path: Path, ai_only: bool = True) -> list[LaggardRow]:
    if not path.exists():
        return []
    out: list[LaggardRow] = []
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("is_mean_reversion_candidate") != "yes":
                continue
            if ai_only and row.get("in_ai_universe") != "yes":
                continue
            try:
                rank = int(row.get("rank") or 0)
            except ValueError:
                rank = 0
            out.append(
                LaggardRow(
                    rank=rank,
                    symbol=row.get("symbol") or "",
                    company=row.get("company_name") or "",
                    sector=row.get("sector") or "",
                    market_cap_b=_to_float(row.get("market_cap_usd_b")) or 0.0,
                    ret_5d_pct=_to_float(row.get("ret_5d_pct")),
                    dist_close_ema21_pct=_to_float(row.get("dist_close_ema21_pct")),
                    slope_ema21_5d_pct=_to_float(row.get("slope_ema21_5d_pct")),
                    valuation_signal=row.get("valuation_signal") or "unknown",
                    next_earnings_date=row.get("next_earnings_date") or "",
                    reasons=row.get("reasons") or "",
                )
            )
    out.sort(key=lambda r: r.rank)
    return out


def render_markdown(
    leaders: list[LeaderRow],
    laggards: list[LaggardRow],
    as_of: str,
) -> str:
    lines: list[str] = [
        f"# AI Tape Cross-Compare - {as_of}",
        "",
        "AI book 是绝对主力。这页把同一天的两个 AI tape 角度并到一起，方便操作员决定:",
        "    - 加仓「bull; rising」头部 (10x radar leaders)",
        "    - 或者在 AI 大池子里捡跌破 EMA21 的滞后名字 (mean-reversion candidates)",
        "",
        "- 数据源: `ai_infra_ten_x_radar/<date>/ten_x_candidates.csv` + `us_mean_reversion_radar/<date>/mean_reversion_radar.csv`",
        "- 状态: read-only join, 不修改任何原 radar；只挑 AI universe 内的名字。",
        "",
        f"- AI 头部领涨 (bull; rising leaders): {len(leaders)}",
        f"- AI 滞后均值回归候选: {len(laggards)}",
        "",
        "## AI Tape Leaders (lean into momentum)",
        "",
        "| Ticker | Company | Pool | Depth | Mcap | Slope 5d | px vs EMA21 | Readiness | Elasticity |",
        "|---|---|---|---|---:|---:|---:|---|---:|",
    ]
    if not leaders:
        lines.append("| - | _今天没有 bull; rising 头部_ | - | - | - | - | - | - | - |")
    for row in leaders[:15]:
        mcap = f"${row.market_cap_b:.1f}B" if row.market_cap_b is not None else "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    row.ticker or "-",
                    (row.company or "-")[:24],
                    row.asset_pool or "-",
                    row.bfs_depth or "-",
                    mcap,
                    f"{row.slope_5d_pct:+.2f}%" if row.slope_5d_pct is not None else "-",
                    f"{row.dist_close_ema21_pct:+.2f}%" if row.dist_close_ema21_pct is not None else "-",
                    row.readiness_tier,
                    f"{row.elasticity_score:.1f}",
                ]
            )
            + " |"
        )
    lines.append("")

    lines += [
        "## AI Mean-Reversion (rotate weight into laggards)",
        "",
        "| Rank | Symbol | Company | Sector | Mcap | 5d | px vs EMA21 | EMA21 slope | Valuation | Next ER | Reasons |",
        "|---:|---|---|---|---:|---:|---:|---:|---|---|---|",
    ]
    if not laggards:
        lines.append("| - | - | _今天没有 AI universe 滞后候选_ | - | - | - | - | - | - | - | - |")
    for row in laggards[:15]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.rank),
                    row.symbol,
                    (row.company or "-")[:24],
                    (row.sector or "-")[:18],
                    f"${row.market_cap_b:.1f}B",
                    f"{row.ret_5d_pct:+.2f}%" if row.ret_5d_pct is not None else "-",
                    f"{row.dist_close_ema21_pct:+.2f}%" if row.dist_close_ema21_pct is not None else "-",
                    f"{row.slope_ema21_5d_pct:+.2f}%" if row.slope_ema21_5d_pct is not None else "-",
                    row.valuation_signal,
                    row.next_earnings_date or "-",
                    row.reasons,
                ]
            )
            + " |"
        )
    lines.append("")
    lines += [
        "## 用法提醒",
        "",
        "- 这两张表都是 *radar*。任何 promote 还是需要 evidence card + G0-G4。",
        "- 头部 (leaders) 适合做加仓 / 维持 size；滞后 (laggards) 适合做小仓位试错或观察。",
        "- 注意 Next ER 列：财报临近的名字不要直接进。",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(
    leaders: list[LeaderRow],
    laggards: list[LaggardRow],
    out_dir: Path,
    as_of: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "ai_tape_cross_compare.md").write_text(
        render_markdown(leaders, laggards, as_of), encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--ten-x-root", type=Path, default=DEFAULT_TEN_X_ROOT)
    parser.add_argument("--mr-root", type=Path, default=DEFAULT_MR_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    ten_x_csv = args.ten_x_root / as_of / "ten_x_candidates.csv"
    mr_csv = args.mr_root / as_of / "mean_reversion_radar.csv"
    leaders = _load_leaders(ten_x_csv)
    laggards = _load_laggards(mr_csv, ai_only=True)
    out_dir = args.output_root / as_of
    write_outputs(leaders, laggards, out_dir, as_of)
    print(
        f"AI tape cross-compare written: {out_dir / 'ai_tape_cross_compare.md'}; "
        f"leaders={len(leaders)} laggards={len(laggards)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
