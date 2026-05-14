"""US options anomaly radar — far-OTM call/put flow flags for AI universe.

User asked for a tape-level options factor that surfaces:
- abnormal far-OTM call accumulation → short-squeeze risk (smart money positioning?)
- abnormal far-OTM put accumulation → selling pressure / hedging surge
- abnormal put-call ratio z-score from sentiment ledger

Methodology constraint (per `company-financials-market-options-methodology.md`):
options data is *event/crowding/risk* context only. It does NOT promote a name
into the production basket; it informs sizing, entry, and risk for names
already in the AI universe.

Pipeline:
1. Pull `options_chain_quotes` rows for AI-universe symbols on the target date.
2. Classify each contract as far-OTM call / far-OTM put based on |delta|
   (defaults: |delta| ≤ 0.20). Fallback to strike/spot ratio when delta is
   missing.
3. Aggregate per symbol:
       - far_otm_call_volume / put_volume
       - far_otm_call_vol_oi_ratio: sum(vol) / max(1, sum(OI))
       - net_call_minus_put_volume
       - liquidity gate (require ≥ 200 contracts traded across far-OTM legs).
4. Join with `options_sentiment` (pc_ratio_z, skew_z) when available.
5. Score each side. Output a markdown radar with:
       - **Short-squeeze candidates** (call-heavy far-OTM flow)
       - **Selling-pressure candidates** (put-heavy far-OTM flow)

Outputs land under
`reports/review_dashboard/us_options_anomaly_radar/<date>/`.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_AI_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_options_anomaly_radar"


@dataclass(frozen=True)
class AnomalyRow:
    symbol: str
    as_of: str
    spot_close: float | None
    far_otm_call_volume: int
    far_otm_call_oi: int
    far_otm_call_vol_oi_ratio: float | None
    far_otm_put_volume: int
    far_otm_put_oi: int
    far_otm_put_vol_oi_ratio: float | None
    pc_ratio_raw: float | None
    pc_ratio_z: float | None
    skew_z: float | None
    short_squeeze_score: float
    selling_pressure_score: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "as_of": self.as_of,
            "spot_close": self.spot_close if self.spot_close is not None else "",
            "far_otm_call_volume": self.far_otm_call_volume,
            "far_otm_call_oi": self.far_otm_call_oi,
            "far_otm_call_vol_oi_ratio": (
                f"{self.far_otm_call_vol_oi_ratio:.3f}" if self.far_otm_call_vol_oi_ratio is not None else ""
            ),
            "far_otm_put_volume": self.far_otm_put_volume,
            "far_otm_put_oi": self.far_otm_put_oi,
            "far_otm_put_vol_oi_ratio": (
                f"{self.far_otm_put_vol_oi_ratio:.3f}" if self.far_otm_put_vol_oi_ratio is not None else ""
            ),
            "pc_ratio_raw": f"{self.pc_ratio_raw:.3f}" if self.pc_ratio_raw is not None else "",
            "pc_ratio_z": f"{self.pc_ratio_z:.3f}" if self.pc_ratio_z is not None else "",
            "skew_z": f"{self.skew_z:.3f}" if self.skew_z is not None else "",
            "short_squeeze_score": f"{self.short_squeeze_score:.2f}",
            "selling_pressure_score": f"{self.selling_pressure_score:.2f}",
        }


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
            for piece in (row.get("ticker") or "").split("/"):
                token = piece.strip().upper()
                if token and token.isalpha() and len(token) <= 5:
                    out.add(token)
    return out


def _resolve_as_of(con: duckdb.DuckDBPyConnection, as_of: date | None) -> date:
    if as_of is not None:
        return as_of
    row = con.execute("SELECT MAX(as_of) FROM options_chain_quotes").fetchone()
    if row and row[0]:
        d = row[0]
        return d if isinstance(d, date) else date.fromisoformat(str(d))
    raise RuntimeError("options_chain_quotes is empty")


def _is_far_otm(option_type: str, delta: float | None, strike: float | None, spot: float | None, delta_cap: float, otm_pct: float) -> bool:
    """Far-OTM if |delta| <= delta_cap; fallback to strike/spot ratio."""
    if delta is not None:
        if option_type == "call" and 0 <= delta <= delta_cap:
            return True
        if option_type == "put" and -delta_cap <= delta <= 0:
            return True
        return False
    if strike is None or spot is None or spot <= 0:
        return False
    if option_type == "call":
        return strike >= spot * (1.0 + otm_pct)
    if option_type == "put":
        return strike <= spot * (1.0 - otm_pct)
    return False


def build_radar(
    *,
    us_db: Path,
    ai_universe_path: Path,
    as_of: date | None = None,
    min_dte: int = 3,
    max_dte: int = 60,
    delta_cap: float = 0.20,
    otm_pct_fallback: float = 0.05,
    min_total_volume: int = 200,
) -> tuple[date, list[AnomalyRow]]:
    ai_universe = _load_ai_universe(ai_universe_path)
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        target = _resolve_as_of(con, as_of)
        # Pull AI-universe option chain for the target date.
        if not ai_universe:
            return target, []
        placeholders = ",".join("?" for _ in ai_universe)
        rows = con.execute(
            f"""
            SELECT symbol, option_type, strike, current_price, delta, volume, open_interest,
                   implied_volatility, days_to_exp
            FROM options_chain_quotes
            WHERE as_of = CAST(? AS DATE)
              AND symbol IN ({placeholders})
              AND days_to_exp BETWEEN ? AND ?
              AND volume IS NOT NULL AND volume > 0
            """,
            [target.isoformat(), *sorted(ai_universe), int(min_dte), int(max_dte)],
        ).fetchall()
        sentiment_rows = con.execute(
            f"""
            SELECT symbol, pc_ratio_raw, pc_ratio_z, skew_z
            FROM options_sentiment
            WHERE as_of = CAST(? AS DATE)
              AND symbol IN ({placeholders})
            """,
            [target.isoformat(), *sorted(ai_universe)],
        ).fetchall()
    finally:
        con.close()

    sentiment = {r[0]: r for r in sentiment_rows}
    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol, option_type, strike, spot, delta, volume, oi, iv, dte in rows:
        if not _is_far_otm(option_type, delta, strike, spot, delta_cap, otm_pct_fallback):
            continue
        bucket = by_symbol.setdefault(
            symbol,
            {
                "spot": None,
                "far_call_vol": 0, "far_call_oi": 0,
                "far_put_vol": 0, "far_put_oi": 0,
            },
        )
        if spot is not None:
            bucket["spot"] = spot
        v = int(volume or 0)
        o = int(oi or 0)
        if option_type == "call":
            bucket["far_call_vol"] += v
            bucket["far_call_oi"] += o
        elif option_type == "put":
            bucket["far_put_vol"] += v
            bucket["far_put_oi"] += o

    radar: list[AnomalyRow] = []
    for symbol, bucket in by_symbol.items():
        call_vol = bucket["far_call_vol"]
        put_vol = bucket["far_put_vol"]
        total = call_vol + put_vol
        if total < min_total_volume:
            continue
        call_oi = bucket["far_call_oi"]
        put_oi = bucket["far_put_oi"]
        call_ratio = (call_vol / call_oi) if call_oi > 0 else None
        put_ratio = (put_vol / put_oi) if put_oi > 0 else None
        sent = sentiment.get(symbol)
        pc_raw = sent[1] if sent else None
        pc_z = sent[2] if sent else None
        skew_z = sent[3] if sent else None

        # Squeeze: heavy far-OTM call volume + low call OI cushion + negative pc_ratio_z (bullish sentiment).
        squeeze = float(call_vol) * (call_ratio or 1.0)
        if pc_z is not None:
            squeeze *= max(0.5, 1.5 - pc_z)  # boost when pc_ratio_z low (bullish skew)
        # Pressure: heavy far-OTM put volume + high pc_ratio_z + rich skew.
        pressure = float(put_vol) * (put_ratio or 1.0)
        if pc_z is not None:
            pressure *= max(0.5, 0.5 + pc_z)

        radar.append(
            AnomalyRow(
                symbol=symbol,
                as_of=target.isoformat(),
                spot_close=float(bucket["spot"]) if bucket["spot"] is not None else None,
                far_otm_call_volume=call_vol,
                far_otm_call_oi=call_oi,
                far_otm_call_vol_oi_ratio=round(call_ratio, 4) if call_ratio is not None else None,
                far_otm_put_volume=put_vol,
                far_otm_put_oi=put_oi,
                far_otm_put_vol_oi_ratio=round(put_ratio, 4) if put_ratio is not None else None,
                pc_ratio_raw=round(pc_raw, 4) if pc_raw is not None else None,
                pc_ratio_z=round(pc_z, 4) if pc_z is not None else None,
                skew_z=round(skew_z, 4) if skew_z is not None else None,
                short_squeeze_score=round(squeeze, 2),
                selling_pressure_score=round(pressure, 2),
            )
        )
    return target, radar


def render_markdown(rows: list[AnomalyRow], as_of: str, *, top_n: int = 15) -> str:
    if not rows:
        return (
            f"# US Options Anomaly Radar - {as_of}\n\n"
            "- 数据源: `options_chain_quotes` + `options_sentiment` ∩ AI universe.\n"
            "- 今日 AI universe 内无符合阈值 (Σvol ≥ 200, |delta| ≤ 0.20, DTE 3-60) 的远 OTM 合约。\n"
        )

    squeeze = sorted(rows, key=lambda r: -r.short_squeeze_score)[:top_n]
    pressure = sorted(rows, key=lambda r: -r.selling_pressure_score)[:top_n]
    lines: list[str] = [
        f"# US Options Anomaly Radar - {as_of}",
        "",
        "- 数据源: `options_chain_quotes` + `options_sentiment` ∩ AI universe.",
        "- 远 OTM = |delta| ≤ 0.20 (delta 缺失时 fallback: strike 距现价 ≥ 5%).",
        "- DTE 区间 3-60 天；过滤总成交量 < 200 的样本。",
        "- 方法论提醒: 期权信号只用于 timing / risk / crowding；不能把名字晋级到 production basket。",
        "",
        "## Short-Squeeze Candidates (call-heavy far-OTM flow)",
        "",
        "解读: 大量远 OTM call 成交 + 低 OI 余量 + 看涨情绪 → 卖方需对冲 → 可能引发上行 squeeze。",
        "",
        "| Symbol | Spot | Far-OTM Call Vol | Vol/OI | Far-OTM Put Vol | PC z | Skew z | Squeeze Score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in squeeze:
        if row.short_squeeze_score <= 0:
            continue
        spot = f"{row.spot_close:.2f}" if row.spot_close is not None else "-"
        vol_oi = f"{row.far_otm_call_vol_oi_ratio:.2f}" if row.far_otm_call_vol_oi_ratio is not None else "-"
        lines.append(
            f"| {row.symbol} | {spot} | {row.far_otm_call_volume:,} | {vol_oi} | "
            f"{row.far_otm_put_volume:,} | "
            f"{row.pc_ratio_z:+.2f} | " if row.pc_ratio_z is not None else
            f"| {row.symbol} | {spot} | {row.far_otm_call_volume:,} | {vol_oi} | {row.far_otm_put_volume:,} | - | "
        )
    # Note: the above appended row is incomplete on purpose to keep the conditional
    # readable — finish it cleanly here:
    lines = [
        line + (f"{row_n.skew_z:+.2f}" if row_n.skew_z is not None else "-") + f" | {row_n.short_squeeze_score:,.0f} |"
        if i >= len(lines) - len(squeeze)  # only patch the appended rows
        else line
        for i, line in enumerate(lines)
        for row_n in [None]  # placeholder, ignore
    ] if False else lines  # disable the convoluted patch

    # ── Clean rewrite of squeeze table to avoid the patched mess above ────
    lines = [
        f"# US Options Anomaly Radar - {as_of}",
        "",
        "- 数据源: `options_chain_quotes` + `options_sentiment` ∩ AI universe.",
        "- 远 OTM = |delta| ≤ 0.20 (delta 缺失时 fallback: strike 距现价 ≥ 5%).",
        "- DTE 区间 3-60 天；过滤总成交量 < 200 的样本。",
        "- 方法论提醒: 期权信号只用于 timing / risk / crowding；不能把名字晋级到 production basket。",
        "",
        "## Short-Squeeze Candidates (call-heavy far-OTM flow)",
        "",
        "解读: 大量远 OTM call 成交 + 低 OI 余量 + 看涨情绪 → 卖方需对冲 → 可能引发上行 squeeze。",
        "",
        "| Symbol | Spot | Far-OTM Call Vol | Call Vol/OI | Far-OTM Put Vol | PC z | Skew z | Squeeze Score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in squeeze:
        if row.short_squeeze_score <= 0:
            continue
        spot = f"{row.spot_close:.2f}" if row.spot_close is not None else "-"
        vol_oi = f"{row.far_otm_call_vol_oi_ratio:.2f}" if row.far_otm_call_vol_oi_ratio is not None else "-"
        pc_z = f"{row.pc_ratio_z:+.2f}" if row.pc_ratio_z is not None else "-"
        sk_z = f"{row.skew_z:+.2f}" if row.skew_z is not None else "-"
        lines.append(
            f"| {row.symbol} | {spot} | {row.far_otm_call_volume:,} | {vol_oi} | "
            f"{row.far_otm_put_volume:,} | {pc_z} | {sk_z} | {row.short_squeeze_score:,.0f} |"
        )
    if not any(r.short_squeeze_score > 0 for r in squeeze):
        lines.append("| - | - | - | - | - | - | - | _今日无 squeeze 候选_ |")
    lines.append("")

    lines += [
        "## Selling-Pressure Candidates (put-heavy far-OTM flow)",
        "",
        "解读: 大量远 OTM put 成交 + 高 pc_ratio + IV skew 偏贵 → 对冲/做空升温 → 短期抛售压力。",
        "",
        "| Symbol | Spot | Far-OTM Put Vol | Put Vol/OI | Far-OTM Call Vol | PC z | Skew z | Pressure Score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in pressure:
        if row.selling_pressure_score <= 0:
            continue
        spot = f"{row.spot_close:.2f}" if row.spot_close is not None else "-"
        vol_oi = f"{row.far_otm_put_vol_oi_ratio:.2f}" if row.far_otm_put_vol_oi_ratio is not None else "-"
        pc_z = f"{row.pc_ratio_z:+.2f}" if row.pc_ratio_z is not None else "-"
        sk_z = f"{row.skew_z:+.2f}" if row.skew_z is not None else "-"
        lines.append(
            f"| {row.symbol} | {spot} | {row.far_otm_put_volume:,} | {vol_oi} | "
            f"{row.far_otm_call_volume:,} | {pc_z} | {sk_z} | {row.selling_pressure_score:,.0f} |"
        )
    if not any(r.selling_pressure_score > 0 for r in pressure):
        lines.append("| - | - | - | - | - | - | - | _今日无 selling-pressure 候选_ |")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(rows: list[AnomalyRow], out_dir: Path, as_of: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol", "as_of", "spot_close",
        "far_otm_call_volume", "far_otm_call_oi", "far_otm_call_vol_oi_ratio",
        "far_otm_put_volume", "far_otm_put_oi", "far_otm_put_vol_oi_ratio",
        "pc_ratio_raw", "pc_ratio_z", "skew_z",
        "short_squeeze_score", "selling_pressure_score",
    ]
    with (out_dir / "options_anomaly.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
    (out_dir / "options_anomaly.md").write_text(render_markdown(rows, as_of), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None, help="Date YYYY-MM-DD; defaults to latest in DB.")
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-universe", type=Path, default=DEFAULT_AI_UNIVERSE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-dte", type=int, default=3)
    parser.add_argument("--max-dte", type=int, default=60)
    parser.add_argument("--delta-cap", type=float, default=0.20)
    parser.add_argument("--otm-pct-fallback", type=float, default=0.05)
    parser.add_argument("--min-total-volume", type=int, default=200)
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    target, rows = build_radar(
        us_db=args.us_db,
        ai_universe_path=args.ai_universe,
        as_of=as_of,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        delta_cap=args.delta_cap,
        otm_pct_fallback=args.otm_pct_fallback,
        min_total_volume=args.min_total_volume,
    )
    out_dir = args.output_root / target.isoformat()
    write_outputs(rows, out_dir, target.isoformat())
    squeeze_count = sum(1 for r in rows if r.short_squeeze_score > 0)
    pressure_count = sum(1 for r in rows if r.selling_pressure_score > 0)
    print(
        f"Options anomaly radar written: {out_dir / 'options_anomaly.md'}; "
        f"target={target} rows={len(rows)} squeeze={squeeze_count} pressure={pressure_count}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
