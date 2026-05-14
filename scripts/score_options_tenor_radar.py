"""Multi-tenor options radar: read AI universe option chains across DTE buckets
(weekly → biweekly → monthly → quarterly → half-year → LEAPS) and emit:

1. Per-ticker × per-tenor call / put flow with vol-to-OI ratios.
2. Cross-tenor pattern detection that gives the operator concrete guidance,
   e.g. "weekly far-OTM call vs monthly normal = gamma squeeze setup".
3. A ranked list of names worth watching, with the reason in plain language.

Tenor buckets (DTE):
    weekly      0-9     gamma squeeze zone; retail / event hedging
    biweekly    10-21   event hedging, slightly smarter money
    monthly     22-50   3rd-Friday standard expiry; institutional baseline
    quarterly   51-120  thesis bets across one earnings cycle
    half_year   121-220 structural positioning, long-stock hedging
    leaps       221+    deep conviction or long-term insurance

Methodology rule: options are tape / event / crowding context only. Nothing
in this radar promotes a name into the production basket; the AI universe
gate + evidence card still own that decision (Codex review 2026-05-14).

Output: reports/review_dashboard/us_options_tenor_radar/<date>/options_tenor.{csv,md}
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
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_options_tenor_radar"


TENORS: list[tuple[str, int, int, str]] = [
    # (key, lo, hi, description shown in report)
    ("weekly",    0,    9,   "0-9d, gamma 区间，retail/event"),
    ("biweekly", 10,   21,   "10-21d, 二线 event 对冲"),
    ("monthly",  22,   50,   "22-50d, 第3周五月度，机构基线"),
    ("quarterly",51,  120,   "51-120d, 一个 earnings 周期 thesis bet"),
    ("half_year",121, 220,   "121-220d, 长期结构性 / 多头对冲"),
    ("leaps",    221, 9999,  "221d+, 深度长期信念 / 保险"),
]


@dataclass(frozen=True)
class TenorBucket:
    symbol: str
    tenor: str
    call_vol: int
    call_oi: int
    far_otm_call_vol: int
    far_otm_call_oi: int
    put_vol: int
    put_oi: int
    far_otm_put_vol: int
    far_otm_put_oi: int

    @property
    def pc_ratio(self) -> float | None:
        return (self.put_vol / self.call_vol) if self.call_vol > 0 else None

    @property
    def call_vol_oi(self) -> float | None:
        return (self.call_vol / self.call_oi) if self.call_oi > 0 else None

    @property
    def put_vol_oi(self) -> float | None:
        return (self.put_vol / self.put_oi) if self.put_oi > 0 else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "tenor": self.tenor,
            "call_vol": self.call_vol,
            "call_oi": self.call_oi,
            "far_otm_call_vol": self.far_otm_call_vol,
            "far_otm_call_oi": self.far_otm_call_oi,
            "put_vol": self.put_vol,
            "put_oi": self.put_oi,
            "far_otm_put_vol": self.far_otm_put_vol,
            "far_otm_put_oi": self.far_otm_put_oi,
            "pc_ratio": round(self.pc_ratio, 3) if self.pc_ratio is not None else "",
            "call_vol_oi": round(self.call_vol_oi, 3) if self.call_vol_oi is not None else "",
            "put_vol_oi": round(self.put_vol_oi, 3) if self.put_vol_oi is not None else "",
        }


@dataclass(frozen=True)
class CrossTenorSignal:
    symbol: str
    pattern: str
    score: float
    guidance: str
    evidence: dict[str, Any]


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


def _is_far_otm(option_type: str, delta: float | None, strike: float | None, spot: float | None,
                delta_cap: float = 0.20, otm_pct: float = 0.05) -> bool:
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


def _resolve_as_of(con: duckdb.DuckDBPyConnection, as_of: date | None) -> date:
    if as_of is not None:
        return as_of
    row = con.execute("SELECT MAX(as_of) FROM options_chain_quotes").fetchone()
    if not row or not row[0]:
        raise RuntimeError("options_chain_quotes is empty")
    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))


def collect_buckets(
    us_db: Path,
    ai_universe: set[str],
    as_of: date | None = None,
    min_total_volume: int = 50,
) -> tuple[date, list[TenorBucket], dict[str, float]]:
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        target = _resolve_as_of(con, as_of)
        if not ai_universe:
            return target, [], {}
        placeholders = ",".join("?" for _ in ai_universe)
        rows = con.execute(
            f"""
            SELECT symbol, option_type, strike, current_price, delta, volume, open_interest, days_to_exp
            FROM options_chain_quotes
            WHERE as_of = CAST(? AS DATE)
              AND symbol IN ({placeholders})
              AND days_to_exp >= 0
              AND volume IS NOT NULL AND volume > 0
            """,
            [target.isoformat(), *sorted(ai_universe)],
        ).fetchall()
    finally:
        con.close()

    accumulator: dict[tuple[str, str], dict[str, int]] = {}
    spot_per_symbol: dict[str, float] = {}
    for symbol, opt_type, strike, spot, delta, volume, oi, dte in rows:
        tenor = next((k for k, lo, hi, _ in TENORS if lo <= dte <= hi), None)
        if tenor is None:
            continue
        if spot is not None:
            spot_per_symbol.setdefault(symbol, float(spot))
        key = (symbol, tenor)
        bucket = accumulator.setdefault(
            key,
            {"call_vol": 0, "call_oi": 0, "far_otm_call_vol": 0, "far_otm_call_oi": 0,
             "put_vol": 0, "put_oi": 0, "far_otm_put_vol": 0, "far_otm_put_oi": 0},
        )
        v = int(volume or 0)
        o = int(oi or 0)
        far = _is_far_otm(opt_type, delta, strike, spot)
        if opt_type == "call":
            bucket["call_vol"] += v
            bucket["call_oi"] += o
            if far:
                bucket["far_otm_call_vol"] += v
                bucket["far_otm_call_oi"] += o
        elif opt_type == "put":
            bucket["put_vol"] += v
            bucket["put_oi"] += o
            if far:
                bucket["far_otm_put_vol"] += v
                bucket["far_otm_put_oi"] += o

    out: list[TenorBucket] = []
    for (symbol, tenor), bucket in accumulator.items():
        total = bucket["call_vol"] + bucket["put_vol"]
        if total < min_total_volume:
            continue
        out.append(TenorBucket(symbol=symbol, tenor=tenor, **bucket))
    out.sort(key=lambda b: (b.symbol, [t[0] for t in TENORS].index(b.tenor)))
    return target, out, spot_per_symbol


def detect_cross_tenor_signals(buckets: list[TenorBucket]) -> list[CrossTenorSignal]:
    """Produce per-symbol guidance based on how tenors stack."""
    by_symbol: dict[str, dict[str, TenorBucket]] = {}
    for bucket in buckets:
        by_symbol.setdefault(bucket.symbol, {})[bucket.tenor] = bucket

    signals: list[CrossTenorSignal] = []
    for symbol, tenor_map in by_symbol.items():
        weekly = tenor_map.get("weekly")
        biweekly = tenor_map.get("biweekly")
        monthly = tenor_map.get("monthly")
        quarterly = tenor_map.get("quarterly")
        half_year = tenor_map.get("half_year")
        leaps = tenor_map.get("leaps")

        # ── Bullish conviction stack: call/put > 1.5 across 3+ tenors ───────
        bullish_tenors = []
        for label, b in (("weekly", weekly), ("biweekly", biweekly), ("monthly", monthly),
                          ("quarterly", quarterly), ("half_year", half_year), ("leaps", leaps)):
            if b is None or b.call_vol < 100:
                continue
            ratio = b.call_vol / max(b.put_vol, 1)
            if ratio >= 1.5:
                bullish_tenors.append((label, ratio))
        if len(bullish_tenors) >= 3:
            signals.append(CrossTenorSignal(
                symbol=symbol,
                pattern="bullish_conviction_stack",
                score=sum(r for _, r in bullish_tenors),
                guidance=(
                    f"Call 多于 put 在 {len(bullish_tenors)} 个 tenor 出现 — 机构 + 短期资金方向一致；"
                    f"指引：维持或加仓但注意 vol 已被预期 priced in，期权对冲变贵"
                ),
                evidence={"tenors": [t for t, _ in bullish_tenors],
                          "ratios": [round(r, 2) for _, r in bullish_tenors]},
            ))

        # ── Bearish stack: put/call > 1.5 across 3+ tenors ─────────────────
        bearish_tenors = []
        for label, b in (("weekly", weekly), ("biweekly", biweekly), ("monthly", monthly),
                          ("quarterly", quarterly), ("half_year", half_year), ("leaps", leaps)):
            if b is None or b.put_vol < 100:
                continue
            ratio = b.put_vol / max(b.call_vol, 1)
            if ratio >= 1.5:
                bearish_tenors.append((label, ratio))
        if len(bearish_tenors) >= 3:
            signals.append(CrossTenorSignal(
                symbol=symbol,
                pattern="bearish_stack",
                score=sum(r for _, r in bearish_tenors),
                guidance=(
                    f"Put 多于 call 在 {len(bearish_tenors)} 个 tenor — 多空两端都在 hedge / short；"
                    f"指引：避免追多；如有多仓考虑 trim 或 collar"
                ),
                evidence={"tenors": [t for t, _ in bearish_tenors],
                          "ratios": [round(r, 2) for _, r in bearish_tenors]},
            ))

        # ── Gamma trap: weekly far-OTM call vol >> monthly far-OTM call vol ─
        if weekly and monthly and weekly.far_otm_call_vol > 0 and monthly.call_vol > 0:
            wk_far = weekly.far_otm_call_vol
            mo_far = monthly.far_otm_call_vol
            if wk_far >= max(mo_far, 1) * 5 and wk_far >= 500:
                signals.append(CrossTenorSignal(
                    symbol=symbol,
                    pattern="gamma_trap",
                    score=float(wk_far) / max(mo_far, 1),
                    guidance=(
                        f"本周远 OTM call 成交 ({wk_far:,}) 远超月度 ({mo_far:,}) — "
                        f"卖方对冲压力集中在短端 → **gamma squeeze setup**；"
                        f"指引：若 long，下周末前小心 vol crush；若 flat，可观察但不要在 IV 高点追入"
                    ),
                    evidence={"weekly_far_otm_call": wk_far, "monthly_far_otm_call": mo_far},
                ))

        # ── Insider tilt: half_year/quarterly far-OTM call dominant over weekly ─
        long_horizon_call = 0
        for b in (quarterly, half_year, leaps):
            if b:
                long_horizon_call += b.far_otm_call_vol
        weekly_call = weekly.far_otm_call_vol if weekly else 0
        if long_horizon_call >= 500 and long_horizon_call >= max(weekly_call, 1) * 2:
            signals.append(CrossTenorSignal(
                symbol=symbol,
                pattern="insider_tilt_long_dated_calls",
                score=float(long_horizon_call) / max(weekly_call, 1),
                guidance=(
                    f"长端 (quarterly+half_year+leaps) 远 OTM call ({long_horizon_call:,}) 占主导，"
                    f"远超本周 ({weekly_call:,}) — 有人押长期上行而非短期 squeeze；"
                    f"指引：若已 long，加 conviction；若没仓，等 evidence card 通过再小仓"
                ),
                evidence={"long_horizon_far_otm_call": long_horizon_call,
                          "weekly_far_otm_call": weekly_call},
            ))

        # ── Crash hedge: monthly/quarterly put dominant + weekly neutral ───
        long_put = 0
        for b in (monthly, quarterly, half_year):
            if b:
                long_put += b.far_otm_put_vol
        weekly_put = weekly.far_otm_put_vol if weekly else 0
        weekly_call_vol = weekly.call_vol if weekly else 0
        weekly_put_vol = weekly.put_vol if weekly else 0
        weekly_neutral = (
            weekly_call_vol > 0
            and 0.7 <= weekly_put_vol / max(weekly_call_vol, 1) <= 1.4
        )
        if long_put >= 500 and weekly_neutral and long_put >= max(weekly_put, 1) * 2:
            signals.append(CrossTenorSignal(
                symbol=symbol,
                pattern="institutional_crash_hedge",
                score=float(long_put) / max(weekly_put, 1),
                guidance=(
                    f"中长期 (monthly+quarterly+half_year) 远 OTM put ({long_put:,}) "
                    f"远超本周 put ({weekly_put:,}) 且本周 PC 接近 1 — "
                    f"机构在买长期保险，没在卖短期看涨；指引：trim 仓位或买短期 hedge"
                ),
                evidence={"long_horizon_far_otm_put": long_put,
                          "weekly_far_otm_put": weekly_put,
                          "weekly_pc_ratio": round(weekly_put_vol / max(weekly_call_vol, 1), 2)},
            ))

    signals.sort(key=lambda s: (s.symbol, -s.score))
    return signals


def render_markdown(
    target: date,
    buckets: list[TenorBucket],
    signals: list[CrossTenorSignal],
    spot_per_symbol: dict[str, float],
) -> str:
    lines: list[str] = [
        f"# US Options Tenor Radar - {target.isoformat()}",
        "",
        "- 数据源: `options_chain_quotes` ∩ AI universe，按 DTE 分桶。",
        "- 6 个 tenor: weekly(0-9) / biweekly(10-21) / monthly(22-50) / quarterly(51-120) / half_year(121-220) / leaps(221+)。",
        "- 期权信号是 **tape / crowding / event** 上下文，**不能晋级 production basket**。",
        "",
    ]

    has_leaps = any(b.tenor == "leaps" for b in buckets)
    if not has_leaps:
        lines.append("- 注: 当前快照 LEAPS (221+) 覆盖为空，可能是数据源截止 ~220 DTE；后续需要扩展 ingest。")
        lines.append("")

    # ── Cross-tenor signals (the actionable section) ───────────────────────
    lines += [
        "## 跨 Tenor 信号 (operator guidance)",
        "",
        "解读各 tenor 之间的相对强度，分类成 5 种 pattern：",
        "",
        "- `bullish_conviction_stack`: ≥3 个 tenor call 多于 put — 多空一致 long",
        "- `bearish_stack`: ≥3 个 tenor put 多于 call — 多空一致 short / hedge",
        "- `gamma_trap`: 本周远 OTM call 成交 ≥ 月度 5×，且 ≥500 张 — squeeze setup",
        "- `insider_tilt_long_dated_calls`: 长端远 OTM call 远超本周 — 有人押长期信念",
        "- `institutional_crash_hedge`: 中长期 put 远超本周 put 且本周 PC ≈1 — 机构在买保险",
        "",
    ]
    if signals:
        lines += [
            "| Symbol | Pattern | Score | 指引 |",
            "|---|---|---:|---|",
        ]
        for sig in signals[:30]:
            lines.append(
                f"| {sig.symbol} | {sig.pattern} | {sig.score:.1f} | {sig.guidance} |"
            )
    else:
        lines.append("- 今日无跨 tenor signal 触发。")
    lines.append("")

    # ── Per-ticker tenor breakdown ────────────────────────────────────────
    lines += [
        "## 每个 ticker 的 Tenor 拆分",
        "",
        "| Symbol | Spot | Tenor | Call Vol | Call Vol/OI | Put Vol | Put Vol/OI | P/C |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    tenor_order = {key: i for i, (key, _, _, _) in enumerate(TENORS)}
    sorted_buckets = sorted(buckets, key=lambda b: (b.symbol, tenor_order.get(b.tenor, 99)))
    last_symbol = None
    for bucket in sorted_buckets:
        spot = spot_per_symbol.get(bucket.symbol)
        spot_text = f"{spot:.2f}" if spot is not None else "-"
        if bucket.symbol != last_symbol:
            symbol_cell = bucket.symbol
            spot_cell = spot_text
            last_symbol = bucket.symbol
        else:
            symbol_cell = ""
            spot_cell = ""
        cvoi = f"{bucket.call_vol_oi:.2f}" if bucket.call_vol_oi is not None else "-"
        pvoi = f"{bucket.put_vol_oi:.2f}" if bucket.put_vol_oi is not None else "-"
        pc = f"{bucket.pc_ratio:.2f}" if bucket.pc_ratio is not None else "-"
        lines.append(
            f"| {symbol_cell} | {spot_cell} | {bucket.tenor} | {bucket.call_vol:,} | {cvoi} | "
            f"{bucket.put_vol:,} | {pvoi} | {pc} |"
        )
    lines.append("")

    lines += [
        "## Tenor 解读速查 (cheat sheet)",
        "",
    ]
    for key, lo, hi, desc in TENORS:
        lines.append(f"- **{key}** ({lo}-{hi}d): {desc}")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(target: date, buckets: list[TenorBucket], signals: list[CrossTenorSignal],
                   spot: dict[str, float], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # CSV: per-bucket flow
    bucket_fields = [
        "symbol", "tenor",
        "call_vol", "call_oi", "far_otm_call_vol", "far_otm_call_oi",
        "put_vol", "put_oi", "far_otm_put_vol", "far_otm_put_oi",
        "pc_ratio", "call_vol_oi", "put_vol_oi",
    ]
    with (out_dir / "options_tenor.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=bucket_fields)
        writer.writeheader()
        for bucket in buckets:
            writer.writerow(bucket.as_dict())
    # JSONL: cross-tenor signals
    with (out_dir / "options_tenor_signals.jsonl").open("w", encoding="utf-8") as handle:
        for sig in signals:
            handle.write(json.dumps({
                "symbol": sig.symbol,
                "pattern": sig.pattern,
                "score": sig.score,
                "guidance": sig.guidance,
                "evidence": sig.evidence,
            }, ensure_ascii=False, sort_keys=True) + "\n")
    # Markdown report
    (out_dir / "options_tenor.md").write_text(
        render_markdown(target, buckets, signals, spot), encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to latest in DB.")
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--ai-universe", type=Path, default=DEFAULT_AI_UNIVERSE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-total-volume", type=int, default=50)
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    ai_universe = _load_ai_universe(args.ai_universe)
    target, buckets, spot = collect_buckets(
        args.us_db, ai_universe, as_of=as_of, min_total_volume=args.min_total_volume,
    )
    signals = detect_cross_tenor_signals(buckets)
    out_dir = args.output_root / target.isoformat()
    write_outputs(target, buckets, signals, spot, out_dir)
    print(
        f"Options tenor radar written: {out_dir / 'options_tenor.md'}; "
        f"buckets={len(buckets)} signals={len(signals)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
