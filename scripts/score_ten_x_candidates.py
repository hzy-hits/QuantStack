"""Rank AI Infra source-review candidates for 10x elasticity potential.

The methodology (`ai_infra/docs/llm-dependency-bfs-framework.md`,
`ai_infra/docs/company-financials-market-options-methodology.md`) says the
highest elasticity zone is `D2-D3` with strong customer/order/margin
transmission and a market cap small enough to allow re-rating. This script
operationalises that filter:

- Read `ai_infra/reports/source_verification_queue_v1.csv`.
- Apply the G0-G4 readiness gates from `score_source_review_readiness.py`.
- Fetch latest market cap from yfinance for each unique primary ticker.
- Keep rows where:
    * `mcap < cap_ceiling` (default $50B),
    * BFS depth ∈ {D2, D2-D3, D3, D3-D4},
    * readiness ≠ g0_blocked,
    * unresolved counterevidence ≤ 3 items.
- Score each survivor and emit ranked CSV + Markdown radar.

Hard rule: This is a **research radar**, not a buy list. Promotion still
requires a completed evidence card with primary source proof.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "scripts"))

# Reuse the canonical readiness logic so the gates stay in lockstep.
from score_source_review_readiness import score_queue as score_readiness_queue  # noqa: E402


DEFAULT_QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_ten_x_radar"
DEFAULT_CACHE = DEFAULT_OUTPUT_ROOT / "market_cap_cache.json"

ELIGIBLE_BFS_DEPTHS = {"D2", "D2-D3", "D3", "D3-D4"}
DEPTH_ELASTICITY_BONUS = {
    "D3": 35,
    "D3-D4": 28,
    "D2-D3": 22,
    "D2": 10,
    "D2-D4": 14,
}

MCAP_BUCKETS = (
    (5_000_000_000, 35, "<5B micro"),
    (10_000_000_000, 28, "5-10B small"),
    (25_000_000_000, 18, "10-25B mid"),
    (50_000_000_000, 8, "25-50B"),
)

US_LISTED_HINT = ".N "


@dataclass(frozen=True)
class TenXCandidate:
    rank: int | None
    primary_ticker: str
    ticker_field: str
    company: str
    asset_pool: str
    market_country: str
    bfs_depth: str
    module: str
    priority_tier: str
    readiness_tier: str
    readiness_score: float
    market_cap: float | None
    elasticity_score: float
    elasticity_signals: list[str]
    counter_items: int
    mcap_bucket: str
    notes: str
    evidence_state: str
    counterevidence: str
    primary_sources_to_find: str
    metrics_to_verify: str
    upgrade_conditions: str
    ema_cross_state: str | None = None
    ema_slope_5d_pct: float | None = None
    ema_dist_close_ema21_pct: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank if self.rank is not None else "",
            "primary_ticker": self.primary_ticker,
            "ticker_field": self.ticker_field,
            "company": self.company,
            "asset_pool": self.asset_pool,
            "market_country": self.market_country,
            "bfs_depth": self.bfs_depth,
            "module": self.module,
            "priority_tier": self.priority_tier,
            "readiness_tier": self.readiness_tier,
            "readiness_score": f"{self.readiness_score:.3f}",
            "market_cap_usd": self.market_cap if self.market_cap is not None else "",
            "mcap_bucket": self.mcap_bucket,
            "counter_items": self.counter_items,
            "elasticity_score": f"{self.elasticity_score:.2f}",
            "elasticity_signals": ";".join(self.elasticity_signals),
            "notes": self.notes,
            "evidence_state": self.evidence_state,
            "counterevidence": self.counterevidence,
            "primary_sources_to_find": self.primary_sources_to_find,
            "metrics_to_verify": self.metrics_to_verify,
            "upgrade_conditions": self.upgrade_conditions,
            "ema_cross_state": self.ema_cross_state or "",
            "ema_slope_5d_pct": self.ema_slope_5d_pct if self.ema_slope_5d_pct is not None else "",
            "ema_dist_close_ema21_pct": (
                self.ema_dist_close_ema21_pct if self.ema_dist_close_ema21_pct is not None else ""
            ),
        }

    def is_bull_rising_leader(self) -> bool:
        return (
            self.ema_cross_state == "bull"
            and (self.ema_slope_5d_pct or 0.0) > 0.5
        )


def _split_counter(value: str) -> int:
    raw = (value or "").replace("，", ",").replace("；", ",").replace(";", ",")
    return sum(1 for piece in raw.split(",") if piece.strip() and piece.strip() not in {"-", "—"})


def _primary_ticker_for(ticker_field: str) -> str:
    """Pick the most yfinance-friendly token from a ticker field.

    The queue uses entries like `2330.TW / TSM` or `3711.TW / ASX`. Prefer the
    US ADR alias when both forms are present; that maps best onto the existing
    US DuckDB and lets us pull market cap without IBKR.
    """
    aliases = [piece.strip() for piece in (ticker_field or "").split("/") if piece.strip()]
    if not aliases:
        return ticker_field.strip()
    # Prefer ADR / pure-letter US tickers.
    for alias in aliases:
        if alias.isupper() and "." not in alias and len(alias) <= 5:
            return alias
    return aliases[0]


def _mcap_bucket(market_cap: float | None) -> tuple[int, str]:
    if market_cap is None:
        return 0, "unknown"
    for ceiling, bonus, label in MCAP_BUCKETS:
        if market_cap < ceiling:
            return bonus, label
    return 0, ">=50B"


def _elasticity_score(
    bfs_depth: str,
    market_cap: float | None,
    readiness_score: float,
    counter_items: int,
) -> tuple[float, list[str], str]:
    signals: list[str] = []
    score = 0.0
    depth_bonus = DEPTH_ELASTICITY_BONUS.get(bfs_depth, 0)
    score += depth_bonus
    if depth_bonus:
        signals.append(f"depth_{bfs_depth}={depth_bonus}")

    mcap_bonus, mcap_label = _mcap_bucket(market_cap)
    score += mcap_bonus
    signals.append(f"mcap_{mcap_label}")

    readiness_bonus = round(readiness_score * 30, 1)
    score += readiness_bonus
    signals.append(f"readiness={readiness_bonus}")

    counter_penalty = counter_items * 4
    score -= counter_penalty
    if counter_penalty:
        signals.append(f"counter_minus_{counter_penalty}")

    return round(score, 2), signals, mcap_label


def _read_cache(cache_path: Path, max_age_days: int) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    fresh: dict[str, dict[str, Any]] = {}
    now = datetime.now(timezone.utc)
    for ticker, entry in payload.items():
        ts_text = entry.get("fetched_at") or ""
        try:
            ts = datetime.fromisoformat(ts_text.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if now - ts <= timedelta(days=max_age_days):
            fresh[ticker] = entry
    return fresh


def _write_cache(cache_path: Path, cache: dict[str, dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def _fetch_market_cap(ticker: str) -> float | None:
    try:
        import yfinance as yf  # type: ignore[import-not-found]
    except ImportError:
        return None
    try:
        t = yf.Ticker(ticker)
        info = getattr(t, "fast_info", None)
        mcap = None
        if info is not None:
            mcap = info.get("market_cap") if isinstance(info, dict) else getattr(info, "market_cap", None)
        if mcap is None:
            mcap = (t.info or {}).get("marketCap")
        if mcap is None:
            return None
        return float(mcap)
    except Exception:  # noqa: BLE001
        return None


def _load_ema_overlay(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _ema_for_candidate(overlay: dict[str, dict[str, Any]], primary_ticker: str, ticker_field: str) -> dict[str, Any]:
    """Match a candidate primary ticker (or any alias) against the EMA overlay."""
    if not overlay:
        return {}
    candidates = [primary_ticker.upper()] + [
        piece.strip().upper() for piece in (ticker_field or "").split("/") if piece.strip()
    ]
    for key in candidates:
        entry = overlay.get(key)
        if entry and isinstance(entry, dict):
            metrics = entry.get("metrics") or {}
            return {
                "cross_state": metrics.get("cross_state"),
                "slope_5d_pct": metrics.get("slope_21d_5d_pct"),
                "dist_close_ema21_pct": metrics.get("dist_close_ema21_pct"),
            }
    return {}


def collect_candidates(
    queue_path: Path,
    *,
    cache: dict[str, dict[str, Any]],
    fetch: bool,
    cap_ceiling: float,
    max_counter_items: int,
    ema_overlay: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[TenXCandidate], dict[str, dict[str, Any]]]:
    readiness_rows = score_readiness_queue(queue_path)
    # Re-read raw queue for the full string fields not preserved on ReadinessRow.
    with queue_path.open("r", encoding="utf-8") as handle:
        raw_rows = list(csv.DictReader(handle))
    raw_by_rank = {(row.get("rank"), row.get("ticker")): row for row in raw_rows}

    candidates: list[TenXCandidate] = []
    next_cache = dict(cache)
    for readiness in readiness_rows:
        depth = readiness.bfs_depth or ""
        if depth not in ELIGIBLE_BFS_DEPTHS:
            continue
        if readiness.readiness_tier == "g0_blocked":
            continue
        raw = raw_by_rank.get((str(readiness.rank) if readiness.rank is not None else "", readiness.ticker))
        if raw is None:
            # Fallback: match by ticker alone.
            raw = next((r for r in raw_rows if r.get("ticker") == readiness.ticker), {})
        counter_items = _split_counter(raw.get("counterevidence") or readiness.counterevidence)
        if counter_items > max_counter_items:
            continue
        primary_ticker = _primary_ticker_for(readiness.ticker)
        cache_entry = cache.get(primary_ticker) or next_cache.get(primary_ticker)
        if cache_entry is None and fetch:
            mcap = _fetch_market_cap(primary_ticker)
            cache_entry = {
                "ticker": primary_ticker,
                "market_cap": mcap,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            next_cache[primary_ticker] = cache_entry
        market_cap = (cache_entry or {}).get("market_cap")
        if market_cap is not None:
            try:
                market_cap = float(market_cap)
            except (TypeError, ValueError):
                market_cap = None
        if market_cap is None or math.isnan(market_cap):
            notes = "missing market cap; offline or unsupported ticker"
            if market_cap is None and not fetch:
                notes = "missing market cap (offline mode)"
            elasticity, signals, bucket = _elasticity_score(depth, None, readiness.evidence_score, counter_items)
            candidates.append(
                TenXCandidate(
                    rank=readiness.rank,
                    primary_ticker=primary_ticker,
                    ticker_field=readiness.ticker,
                    company=readiness.company,
                    asset_pool=readiness.asset_pool,
                    market_country=readiness.market_country,
                    bfs_depth=depth,
                    module=readiness.module,
                    priority_tier=readiness.priority_tier,
                    readiness_tier=readiness.readiness_tier,
                    readiness_score=readiness.evidence_score,
                    market_cap=None,
                    elasticity_score=elasticity,
                    elasticity_signals=signals,
                    counter_items=counter_items,
                    mcap_bucket=bucket,
                    notes=notes,
                    evidence_state=readiness.evidence_state,
                    counterevidence=readiness.counterevidence,
                    primary_sources_to_find=(raw or {}).get("primary_sources_to_find", ""),
                    metrics_to_verify=(raw or {}).get("metrics_to_verify", ""),
                    upgrade_conditions=(raw or {}).get("upgrade_conditions", ""),
                    **{
                        "ema_cross_state": _ema_for_candidate(ema_overlay or {}, primary_ticker, readiness.ticker).get("cross_state"),
                        "ema_slope_5d_pct": _ema_for_candidate(ema_overlay or {}, primary_ticker, readiness.ticker).get("slope_5d_pct"),
                        "ema_dist_close_ema21_pct": _ema_for_candidate(ema_overlay or {}, primary_ticker, readiness.ticker).get(
                            "dist_close_ema21_pct"
                        ),
                    },
                )
            )
            continue
        if market_cap >= cap_ceiling:
            continue
        elasticity, signals, bucket = _elasticity_score(depth, market_cap, readiness.evidence_score, counter_items)
        ema_lookup = _ema_for_candidate(ema_overlay or {}, primary_ticker, readiness.ticker)
        candidates.append(
            TenXCandidate(
                rank=readiness.rank,
                primary_ticker=primary_ticker,
                ticker_field=readiness.ticker,
                company=readiness.company,
                asset_pool=readiness.asset_pool,
                market_country=readiness.market_country,
                bfs_depth=depth,
                module=readiness.module,
                priority_tier=readiness.priority_tier,
                readiness_tier=readiness.readiness_tier,
                readiness_score=readiness.evidence_score,
                market_cap=market_cap,
                elasticity_score=elasticity,
                elasticity_signals=signals,
                counter_items=counter_items,
                mcap_bucket=bucket,
                notes="",
                evidence_state=readiness.evidence_state,
                counterevidence=readiness.counterevidence,
                primary_sources_to_find=(raw or {}).get("primary_sources_to_find", ""),
                metrics_to_verify=(raw or {}).get("metrics_to_verify", ""),
                upgrade_conditions=(raw or {}).get("upgrade_conditions", ""),
                ema_cross_state=ema_lookup.get("cross_state"),
                ema_slope_5d_pct=ema_lookup.get("slope_5d_pct"),
                ema_dist_close_ema21_pct=ema_lookup.get("dist_close_ema21_pct"),
            )
        )
    candidates.sort(key=lambda c: (c.market_cap is None, -c.elasticity_score, c.market_cap or 0.0))
    return candidates, next_cache


def _tape_text(cand: TenXCandidate) -> str:
    if cand.ema_cross_state is None:
        return "no_data"
    parts: list[str] = [cand.ema_cross_state]
    slope = cand.ema_slope_5d_pct
    if slope is not None:
        if slope > 0.5:
            parts.append("rising")
        elif slope < -0.5:
            parts.append("falling")
        else:
            parts.append("flat")
    dist = cand.ema_dist_close_ema21_pct
    if dist is not None:
        parts.append(f"px {dist:+.1f}%")
    return "; ".join(parts)


def render_markdown(candidates: list[TenXCandidate], as_of: str, cap_ceiling: float) -> str:
    with_mcap = [c for c in candidates if c.market_cap is not None]
    without_mcap = [c for c in candidates if c.market_cap is None]
    leaders = [c for c in with_mcap if c.is_bull_rising_leader()]
    leaders.sort(key=lambda c: -(c.ema_slope_5d_pct or 0.0))

    lines: list[str] = [
        f"# AI Infra 10x Candidate Radar - {as_of}",
        "",
        f"- 数据源: `ai_infra/reports/source_verification_queue_v1.csv` + readiness gates + yfinance market cap + EMA21/50 overlay.",
        f"- 过滤口径: mcap < ${cap_ceiling / 1e9:.0f}B, BFS depth ∈ {{D2, D2-D3, D3, D3-D4}}, readiness ≠ g0_blocked, counter-evidence ≤ 3 项。",
        "- 用法: 这是 *research radar*，不是买入许可。每个名字仍需 evidence card 完成 + 原文证据通过 G0-G4。",
        "",
        f"- 命中: {len(with_mcap)} 个有市值数据，{len(without_mcap)} 个市值缺失，其中 {len(leaders)} 个为 `bull; rising` 头部。",
        "",
    ]

    if leaders:
        lines += [
            "## Top Leaders (bull; rising)",
            "",
            "EMA21 在 EMA50 上方且 5d slope > 0.5%，价格当下站在 EMA21 之上 — tape leadership 状态。",
            "",
            "| Ticker | Company | Pool | Depth | Mcap | Slope 5d | px vs EMA21 | Readiness | Elasticity |",
            "|---|---|---|---|---:|---:|---:|---|---:|",
        ]
        for cand in leaders[:20]:
            mcap_text = f"${cand.market_cap / 1e9:.1f}B" if cand.market_cap is not None else "-"
            slope = cand.ema_slope_5d_pct
            dist = cand.ema_dist_close_ema21_pct
            lines.append(
                "| "
                + " | ".join(
                    [
                        cand.primary_ticker or "-",
                        cand.company or "-",
                        cand.asset_pool or "-",
                        cand.bfs_depth or "-",
                        mcap_text,
                        f"{slope:+.2f}%" if slope is not None else "-",
                        f"{dist:+.2f}%" if dist is not None else "-",
                        f"{cand.readiness_tier} ({cand.readiness_score:.2f})",
                        f"{cand.elasticity_score:.1f}",
                    ]
                )
                + " |"
            )
        lines.append("")

    lines += [
        "## Top Elasticity (mcap-bounded)",
        "",
        "| Rank | Ticker | Company | Pool | Depth | Mcap | Bucket | Readiness | Counter | Elasticity | Tape | Module |",
        "|---:|---|---|---|---|---:|---|---|---:|---:|---|---|",
    ]
    for cand in with_mcap[:40]:
        mcap_text = f"${cand.market_cap / 1e9:.1f}B" if cand.market_cap is not None else "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    str(cand.rank if cand.rank is not None else "-"),
                    cand.primary_ticker or "-",
                    cand.company or "-",
                    cand.asset_pool or "-",
                    cand.bfs_depth or "-",
                    mcap_text,
                    cand.mcap_bucket,
                    f"{cand.readiness_tier} ({cand.readiness_score:.2f})",
                    str(cand.counter_items),
                    f"{cand.elasticity_score:.1f}",
                    _tape_text(cand),
                    cand.module or "-",
                ]
            )
            + " |"
        )

    if without_mcap:
        lines += [
            "",
            "## Missing Market Cap (need data)",
            "",
            "| Rank | Ticker | Company | Pool | Depth | Module | Note |",
            "|---:|---|---|---|---|---|---|",
        ]
        for cand in without_mcap[:60]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(cand.rank if cand.rank is not None else "-"),
                        cand.primary_ticker or "-",
                        cand.company or "-",
                        cand.asset_pool or "-",
                        cand.bfs_depth or "-",
                        cand.module or "-",
                        cand.notes or "-",
                    ]
                )
                + " |"
            )
    return "\n".join(lines) + "\n"


def write_outputs(
    candidates: list[TenXCandidate],
    out_csv: Path,
    out_md: Path,
    as_of: str,
    cap_ceiling: float,
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank",
        "primary_ticker",
        "ticker_field",
        "company",
        "asset_pool",
        "market_country",
        "bfs_depth",
        "module",
        "priority_tier",
        "readiness_tier",
        "readiness_score",
        "market_cap_usd",
        "mcap_bucket",
        "counter_items",
        "elasticity_score",
        "elasticity_signals",
        "notes",
        "evidence_state",
        "counterevidence",
        "primary_sources_to_find",
        "metrics_to_verify",
        "upgrade_conditions",
        "ema_cross_state",
        "ema_slope_5d_pct",
        "ema_dist_close_ema21_pct",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for cand in candidates:
            writer.writerow(cand.as_dict())
    out_md.write_text(render_markdown(candidates, as_of, cap_ceiling), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--no-fetch", action="store_true", help="Skip yfinance calls; use cache only.")
    parser.add_argument("--cache-max-age-days", type=int, default=7)
    parser.add_argument("--cap-ceiling", type=float, default=50_000_000_000.0)
    parser.add_argument("--max-counter-items", type=int, default=3)
    parser.add_argument(
        "--ema-overlay",
        type=Path,
        default=None,
        help="Path to ema_tape_overlay.json (defaults to main_strategy_v2/<as-of>/ema_tape_overlay.json).",
    )
    args = parser.parse_args()

    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    out_dir = args.output_root / as_of
    cache = _read_cache(args.cache, args.cache_max_age_days)
    ema_overlay_path = args.ema_overlay or (
        STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of / "ema_tape_overlay.json"
    )
    ema_overlay = _load_ema_overlay(ema_overlay_path)
    candidates, next_cache = collect_candidates(
        args.queue,
        cache=cache,
        fetch=not args.no_fetch,
        cap_ceiling=args.cap_ceiling,
        max_counter_items=args.max_counter_items,
        ema_overlay=ema_overlay,
    )
    out_csv = out_dir / "ten_x_candidates.csv"
    out_md = out_dir / "ten_x_candidates.md"
    write_outputs(candidates, out_csv, out_md, as_of, args.cap_ceiling)
    if not args.no_fetch:
        _write_cache(args.cache, next_cache)
    with_mcap = sum(1 for c in candidates if c.market_cap is not None)
    print(
        f"10x candidate radar written: {out_md}; total={len(candidates)} mcap_known={with_mcap} "
        f"top_score={candidates[0].elasticity_score if candidates else 0}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
