"""Connect bubble-hedge victim shortlist with the options chain.

Reads the latest `bubble_hedge.json` victim list and, for each name, scans
`options_chain_quotes` for liquid OTM puts in a target window:

- Target delta: -0.35 to -0.15 (the "convex hedge" zone — pays well on a
  2-3 sigma move down without bleeding ATM theta).
- Target DTE: 30-60 days when available, else the longest available expiry
  (LEAPS will populate once `options_max_expiries=12` propagates).
- Liquidity floor: open_interest >= 50, volume >= 1 OR open_interest >= 200.

Outputs the top 3 contracts per victim sorted by a composite "hedge value"
score (lower IV + better liquidity + closer to target delta = higher score).
Writes JSON + Markdown to
`reports/review_dashboard/bubble_hedge_radar/{as_of}/victim_puts.{json,md}`.

Why: bubble_hedge_radar.py flags WHICH stocks to short via puts; this script
tells the operator EXACTLY which contract to look at. Hedge expression is a
methodology requirement from the Hedge-Wedge-Confirm-Press essay — we never
short outright, we buy puts (defined max loss = premium).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_RADAR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "bubble_hedge_radar"

TARGET_DELTA_LOW = -0.35
TARGET_DELTA_HIGH = -0.15
TARGET_DELTA_CENTER = -0.25
TARGET_DTE_LOW = 30
TARGET_DTE_HIGH = 60
MIN_OPEN_INTEREST = 50
MIN_VOLUME_OR_OI = 200  # if volume==0 we still accept high-OI contracts


def _load_victims(as_of: str, radar_root: Path) -> list[dict[str, Any]]:
    path = radar_root / as_of / "bubble_hedge.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data.get("victims") or [])


def _fetch_puts(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: date,
    *,
    delta_low: float = TARGET_DELTA_LOW,
    delta_high: float = TARGET_DELTA_HIGH,
    dte_low: int | None = None,
    dte_high: int | None = None,
) -> list[dict[str, Any]]:
    """Query liquid OTM puts. dte filters are applied only when both set."""
    where = [
        "symbol = ?",
        "as_of = ?",
        "option_type = 'put'",
        "delta IS NOT NULL",
        "delta BETWEEN ? AND ?",
        "(volume >= 1 OR open_interest >= ?)",
        "open_interest >= ?",
    ]
    params: list[Any] = [symbol, as_of.isoformat(), delta_low, delta_high, MIN_VOLUME_OR_OI, MIN_OPEN_INTEREST]
    if dte_low is not None and dte_high is not None:
        where.append("days_to_exp BETWEEN ? AND ?")
        params.extend([dte_low, dte_high])
    rows = con.execute(
        f"""
        SELECT contract_symbol, expiry, days_to_exp, strike, current_price,
               bid, ask, mid, last_price, volume, open_interest, delta,
               implied_volatility
        FROM options_chain_quotes
        WHERE {' AND '.join(where)}
        ORDER BY days_to_exp, ABS(delta - ?), open_interest DESC
        LIMIT 30
        """,
        params + [TARGET_DELTA_CENTER],
    ).fetchall()
    cols = [
        "contract_symbol",
        "expiry",
        "days_to_exp",
        "strike",
        "current_price",
        "bid",
        "ask",
        "mid",
        "last_price",
        "volume",
        "open_interest",
        "delta",
        "implied_volatility",
    ]
    return [dict(zip(cols, r, strict=True)) for r in rows]


def _max_available_dte(con: duckdb.DuckDBPyConnection, symbol: str, as_of: date) -> int | None:
    row = con.execute(
        "SELECT MAX(days_to_exp) FROM options_chain_quotes WHERE symbol = ? AND as_of = ?",
        [symbol, as_of.isoformat()],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _premium(row: dict[str, Any]) -> float | None:
    """Best estimate of mid premium: prefer mid, then (bid+ask)/2, then last."""
    mid = row.get("mid")
    if mid is not None and mid > 0:
        return float(mid)
    bid, ask = row.get("bid"), row.get("ask")
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (float(bid) + float(ask)) / 2
    last = row.get("last_price")
    if last is not None and last > 0:
        return float(last)
    return None


def _score_contract(row: dict[str, Any]) -> float:
    """Composite hedge-value score: closer-to-target-delta, higher OI, lower IV."""
    delta = row.get("delta") or 0
    iv = row.get("implied_volatility") or 1.0
    oi = max(int(row.get("open_interest") or 0), 1)
    vol = max(int(row.get("volume") or 0), 0)
    delta_penalty = abs(delta - TARGET_DELTA_CENTER) * 10  # 0..2
    iv_penalty = min(max(iv, 0.0), 3.0)
    liquidity_bonus = (oi ** 0.4) / 10 + (vol ** 0.3) / 10
    return liquidity_bonus - delta_penalty - iv_penalty


def _decorate(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    spot = row.get("current_price") or 0
    strike = row.get("strike") or 0
    premium = _premium(row)
    out["premium_est"] = premium
    if premium is not None and spot:
        out["cost_pct_of_spot"] = round(premium / spot * 100, 3)
    if spot and strike:
        out["pct_otm"] = round((spot - strike) / spot * 100, 2)
    out["hedge_score"] = round(_score_contract(row), 3)
    if row.get("delta") is not None:
        out["delta_per_100_contracts"] = round(float(row["delta"]) * 100 * 100, 1)
    return out


def _suggest_for_victim(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: date,
) -> dict[str, Any]:
    primary = _fetch_puts(con, symbol, as_of, dte_low=TARGET_DTE_LOW, dte_high=TARGET_DTE_HIGH)
    used_window = f"{TARGET_DTE_LOW}-{TARGET_DTE_HIGH}d"
    fallback_note: str | None = None
    if not primary:
        max_dte = _max_available_dte(con, symbol, as_of)
        if max_dte is None:
            return {
                "symbol": symbol,
                "contracts": [],
                "dte_window": None,
                "note": "no chain data",
            }
        # widen to "anything DTE>=14" so we surface the longest expiry
        # available — operator can re-run once LEAPS land.
        primary = _fetch_puts(con, symbol, as_of, dte_low=14, dte_high=max_dte)
        used_window = f"14-{max_dte}d (fallback — primary window 30-60d empty)"
        if max_dte < TARGET_DTE_LOW:
            fallback_note = (
                f"chain shallow: max DTE = {max_dte}; LEAPS will populate once "
                f"options_max_expiries=12 propagates"
            )
    if not primary:
        return {
            "symbol": symbol,
            "contracts": [],
            "dte_window": used_window,
            "note": fallback_note or "no liquid OTM put in target delta range",
        }
    decorated = [_decorate(r) for r in primary]
    decorated.sort(key=lambda r: r["hedge_score"], reverse=True)
    return {
        "symbol": symbol,
        "contracts": decorated[:3],
        "dte_window": used_window,
        "note": fallback_note,
    }


def _render_md(payload: dict[str, Any]) -> str:
    as_of = payload.get("as_of", "")
    suggestions = payload.get("suggestions") or []
    lines = [
        f"# Victim Put-Option Hedge Suggestions — {as_of}",
        "",
        "Source: `bubble_hedge.json` victim shortlist + `options_chain_quotes`.",
        f"Target: delta in [{TARGET_DELTA_LOW:.2f}, {TARGET_DELTA_HIGH:.2f}], "
        f"DTE in [{TARGET_DTE_LOW}, {TARGET_DTE_HIGH}] days, OI ≥ {MIN_OPEN_INTEREST}.",
        "",
        "Hedge expression rule: buy puts. Never short outright. Premium = max loss.",
        "",
    ]
    if not suggestions:
        lines.append("_No victims found._")
        return "\n".join(lines) + "\n"
    for entry in suggestions:
        sym = entry.get("symbol")
        contracts = entry.get("contracts") or []
        note = entry.get("note") or ""
        window = entry.get("dte_window") or ""
        victim_score = entry.get("victim_score")
        score_tag = f"score={victim_score}" if victim_score is not None else ""
        head = f"## {sym}"
        if score_tag:
            head += f"  ({score_tag})"
        lines.append(head)
        lines.append("")
        if entry.get("victim_reasons"):
            lines.append(f"Reasons: {', '.join(entry['victim_reasons'])}")
            lines.append("")
        if not contracts:
            lines.append(f"_No qualifying puts found ({note or window})._")
            lines.append("")
            continue
        lines.append(f"_DTE window: {window}_")
        if note:
            lines.append(f"_Note: {note}_")
        lines.append("")
        lines.append(
            "| Contract | Expiry | DTE | Strike | Spot | %OTM | Premium | "
            "Cost%Spot | Δ | IV | Vol | OI | Hedge Score |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for c in contracts:
            iv = c.get("implied_volatility")
            iv_s = f"{iv:.2f}" if iv is not None else "n/a"
            prem = c.get("premium_est")
            prem_s = f"${prem:.2f}" if prem is not None else "n/a"
            cost = c.get("cost_pct_of_spot")
            cost_s = f"{cost:.2f}%" if cost is not None else "n/a"
            lines.append(
                f"| {c.get('contract_symbol')} | {c.get('expiry')} | "
                f"{c.get('days_to_exp')} | ${c.get('strike'):.2f} | "
                f"${c.get('current_price'):.2f} | {c.get('pct_otm')}% | "
                f"{prem_s} | {cost_s} | {c.get('delta'):.3f} | {iv_s} | "
                f"{c.get('volume')} | {c.get('open_interest')} | "
                f"{c.get('hedge_score')} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--radar-root", type=Path, default=DEFAULT_RADAR_ROOT)
    parser.add_argument(
        "--max-victims",
        type=int,
        default=8,
        help="Cap on how many top victims to process (sorted by convex_score).",
    )
    args = parser.parse_args()

    as_of_date = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    victims = _load_victims(args.as_of, args.radar_root)
    if not victims:
        print(f"warn: no victims found for {args.as_of} in {args.radar_root}", file=sys.stderr)
    victims_sorted = sorted(
        victims,
        key=lambda v: (v.get("convex_score") or v.get("victim_score") or 0),
        reverse=True,
    )[: args.max_victims]

    if not args.us_db.exists():
        print(f"error: US db missing at {args.us_db}", file=sys.stderr)
        return 2
    con = duckdb.connect(str(args.us_db), read_only=True)
    suggestions: list[dict[str, Any]] = []
    try:
        for v in victims_sorted:
            sym = str(v.get("symbol") or "").upper()
            if not sym:
                continue
            result = _suggest_for_victim(con, sym, as_of_date)
            result["victim_score"] = v.get("convex_score") or v.get("victim_score")
            result["victim_reasons"] = v.get("reasons") or []
            result["company"] = v.get("company")
            result["evidence_state"] = v.get("evidence_state")
            suggestions.append(result)
    finally:
        con.close()

    out_dir = args.radar_root / args.as_of
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": args.as_of,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "target_delta": [TARGET_DELTA_LOW, TARGET_DELTA_HIGH],
            "target_dte": [TARGET_DTE_LOW, TARGET_DTE_HIGH],
            "min_open_interest": MIN_OPEN_INTEREST,
            "min_volume_or_oi": MIN_VOLUME_OR_OI,
        },
        "suggestions": suggestions,
    }
    (out_dir / "victim_puts.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    (out_dir / "victim_puts.md").write_text(_render_md(payload), encoding="utf-8")
    n_with_contracts = sum(1 for s in suggestions if s.get("contracts"))
    print(
        f"victim_puts: wrote {len(suggestions)} victim suggestions "
        f"({n_with_contracts} with qualifying contracts) to {out_dir}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
