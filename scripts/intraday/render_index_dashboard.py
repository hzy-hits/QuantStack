"""Render markdown dashboard for index options intraday view.

Reads latest index_gex_snapshots and writes
reports/intraday/<date>/index_dashboard_<HHMM>.md

Structure:
  1. Top-line summary (which indices are in vol amplifier vs dampener mode)
  2. Per-index card (8 cards): spot, GEX, flip, skew, top OI strikes
  3. Strategy candidates (rule-based mapping from current metrics)
  4. Cross-venue note (ETF vs cash divergence)

Usage:
  python3 scripts/intraday/render_index_dashboard.py [--as-of YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
OUTPUT_ROOT = STACK_ROOT / "reports" / "intraday"

# Display order — biggest / most important first.
INDEX_ORDER = ["^SPX", "^NDX", "^RUT", "^XEO", "^XSP", "^XND", "^MRUT", "^VIX"]
BUCKET_ORDER = ["0DTE", "1DTE", "WEEK", "MONTH"]


def _fmt_gex(g: float | None) -> str:
    if g is None:
        return "—"
    abs_g = abs(g)
    if abs_g >= 1e12:
        return f"{g/1e12:+.2f}T"
    if abs_g >= 1e9:
        return f"{g/1e9:+.2f}B"
    return f"{g/1e6:+.2f}M"


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v*100:.{digits}f}%"


def _classify_gex_regime(net_gex: float | None, spot: float | None, flip: float | None) -> str:
    """Map (GEX, spot vs flip) to vol regime label.

    - Net positive AND spot near flip → dampener (vol seller territory)
    - Net negative AND spot ABOVE flip → mixed (downside risk amplifier)
    - Net negative AND spot BELOW flip → vol amplifier (any move accelerates)
    - Net positive AND spot ABOVE flip → strong dampener
    """
    if net_gex is None or net_gex == 0:
        return "neutral"
    if net_gex > 0:
        if spot and flip and spot > flip * 0.998:
            return "dampener+"      # strong vol seller setup
        return "dampener"
    # net_gex < 0
    if spot and flip and spot > flip * 1.005:
        return "watch_break"        # currently neutral but break of flip = amplifier
    return "amplifier"              # full short-gamma mode


def _strategy_for(symbol: str, bucket: str, m: dict[str, Any]) -> list[str]:
    """Rule-based strategy candidates for one (symbol, bucket) row.

    Outputs human-readable Chinese lines. Each line should pair with
    a triggering condition and a risk note. Narrator can override.
    """
    out: list[str] = []
    spot = m.get("spot")
    gex = m.get("net_dealer_gex")
    flip = m.get("gamma_flip_strike")
    skew_pts = m.get("skew_pts")  # decimal pp (e.g. 0.394 = +39.4pp)
    atm_iv = m.get("atm_iv")
    top_call_k = m.get("top_oi_call_strike")
    top_put_k = m.get("top_oi_put_strike")

    regime = _classify_gex_regime(gex, spot, flip)

    # A. Long-gamma play on short-gamma extreme
    if gex is not None and gex < -1e12 and bucket in ("0DTE", "1DTE"):
        out.append(
            f"**A 长 gamma**:dealer {bucket} 短 gamma {_fmt_gex(gex)} 极端,"
            f"自己买 ATM straddle/iron butterfly 0DTE → 任何方向 move 都赚 gamma。"
            f"风险:横盘日全亏 premium。"
        )

    # B. Skew fade
    if skew_pts is not None and abs(skew_pts) > 0.20 and bucket in ("0DTE", "1DTE"):
        if skew_pts > 0:
            out.append(
                f"**B Skew fade**:put IV - call IV = {skew_pts*100:.0f}pp(put 极贵),"
                f"卖 OTM put spread + 买 OTM call spread = 轻微 bullish vol seller。"
                f"风险:回调时 put IV 飙升放大亏损。"
            )
        else:
            out.append(
                f"**B Call skew fade**:call IV - put IV = {-skew_pts*100:.0f}pp(call 贵),"
                f"卖 OTM call spread → 收 squeeze premium。"
                f"风险:真有 squeeze 时 call 飙涨。"
            )

    # C. Pin trade (calls + puts both heavy near spot)
    if (
        top_call_k and top_put_k and spot
        and abs(top_call_k - spot) / spot <= 0.015
        and abs(top_put_k - spot) / spot <= 0.015
        and bucket == "1DTE"
    ):
        out.append(
            f"**C Pin strangle**:上方 {top_call_k:.0f} call wall + 下方 {top_put_k:.0f} put wall,"
            f"spot {spot:.0f} 卡在中间,卖 {top_put_k:.0f}/{top_call_k:.0f} strangle 收尾盘 dealer pin theta。"
            f"风险:dealer 短 gamma 反而推 move 击穿 wall。"
        )

    # D. Watch break of flip (warning, not trade)
    if regime == "watch_break" and flip and spot:
        gap_pct = (spot - flip) / spot * 100
        out.append(
            f"⚠️ **D 监控**:spot {spot:.0f} vs gamma flip {flip:.0f}(距离 {gap_pct:.2f}%),"
            f"**跌破 {flip:.0f} 进 vol amplifier mode** — 实时关注此 level。"
        )

    return out


def _load_latest(con: duckdb.DuckDBPyConnection, as_of_str: str) -> dict[tuple[str, str], dict[str, Any]]:
    rows = con.execute(
        """
        WITH ranked AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, dte_bucket ORDER BY snapshot_time DESC) AS rn
          FROM index_gex_snapshots
          WHERE as_of = ?
        )
        SELECT * FROM ranked WHERE rn = 1
        """,
        [as_of_str],
    ).fetchall()
    cols = [c[0] for c in con.description]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        rd = dict(zip(cols, r))
        out[(rd["symbol"], rd["dte_bucket"])] = rd
    return out


def _render(latest: dict[tuple[str, str], dict[str, Any]], as_of: date) -> list[str]:
    lines: list[str] = []
    lines.append(f"# 指数期权日内 view — {as_of}")
    lines.append("")
    snapshot_times = sorted({r["snapshot_time"] for r in latest.values()})
    if snapshot_times:
        lines.append(f"_快照:{snapshot_times[-1].isoformat(timespec='minutes')}  ·  数据延迟:CBOE CDN 约 15min_")
    lines.append("")

    # 1. Top line summary table
    lines.append("## 一句话总览(8 个 cash-settled index)")
    lines.append("")
    lines.append("| Index | spot | 1DTE GEX | Regime | Flip strike | Skew (1DTE put-call IV) |")
    lines.append("|---|---:|---:|:---:|---:|---:|")
    for sym in INDEX_ORDER:
        m = latest.get((sym, "1DTE"))
        if not m:
            lines.append(f"| {sym} | — | — | — | — | — |")
            continue
        regime = _classify_gex_regime(m.get("net_dealer_gex"), m.get("spot"), m.get("gamma_flip_strike"))
        regime_label = {
            "amplifier":   "🔴 amplifier",
            "watch_break": "🟡 watch break",
            "dampener":    "🟢 dampener",
            "dampener+":   "🟢 dampener+",
            "neutral":     "⚪ neutral",
        }.get(regime, regime)
        skew = m.get("skew_pts")
        skew_str = f"{skew*100:+.1f}pp" if skew is not None else "—"
        flip_str = f"{m.get('gamma_flip_strike'):.0f}" if m.get("gamma_flip_strike") else "—"
        lines.append(
            f"| **{sym}** | {m.get('spot'):.2f} | {_fmt_gex(m.get('net_dealer_gex'))} | "
            f"{regime_label} | {flip_str} | {skew_str} |"
        )
    lines.append("")

    # 2. Per-index cards (only for indices with meaningful 1DTE data)
    lines.append("## 各 index 详情(优先看 ^SPX / ^XSP / ^RUT,流动性最厚)")
    lines.append("")
    for sym in INDEX_ORDER:
        m1 = latest.get((sym, "1DTE"))
        if not m1 or not m1.get("chain_contracts"):
            continue
        spot = m1.get("spot") or 0
        lines.append(f"### {sym}  spot={spot:.2f}")
        lines.append("")
        # Per-bucket compact table
        lines.append("| DTE | Spot vol | Put vol | P/C | net GEX | Flip strike | ATM IV | Skew |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for b in BUCKET_ORDER:
            mb = latest.get((sym, b))
            if not mb:
                continue
            atm_iv = mb.get("atm_iv")
            skew_pp = mb.get("skew_pts")
            lines.append(
                f"| {b} | {mb.get('call_volume_total') or 0:,} | "
                f"{mb.get('put_volume_total') or 0:,} | "
                f"{mb.get('pc_vol_ratio') or 0:.2f} | "
                f"{_fmt_gex(mb.get('net_dealer_gex'))} | "
                f"{mb.get('gamma_flip_strike') or 0:.0f} | "
                f"{atm_iv*100:.1f}% | "
                f"{skew_pp*100:+.1f}pp |"
            ) if atm_iv and skew_pp is not None else lines.append(
                f"| {b} | {mb.get('call_volume_total') or 0:,} | "
                f"{mb.get('put_volume_total') or 0:,} | "
                f"{mb.get('pc_vol_ratio') or 0:.2f} | "
                f"{_fmt_gex(mb.get('net_dealer_gex'))} | "
                f"{mb.get('gamma_flip_strike') or 0:.0f} | — | — |"
            )
        # Top OI strikes
        top_c = m1.get("top_oi_call_strike")
        top_p = m1.get("top_oi_put_strike")
        if top_c or top_p:
            lines.append("")
            lines.append(f"- **关键 strike(1DTE,±2% spot):** "
                         f"call wall {top_c or '-'} (OI {m1.get('top_oi_call_oi') or '-'})  ·  "
                         f"put wall {top_p or '-'} (OI {m1.get('top_oi_put_oi') or '-'})")
        # Strategy candidates
        strategies = _strategy_for(sym, "1DTE", m1)
        if strategies:
            lines.append("")
            lines.append("**Strategy candidates:**")
            for s in strategies:
                lines.append(f"- {s}")
        lines.append("")

    # 3. Cross-venue note
    spx_1d = latest.get(("^SPX", "1DTE"))
    xsp_1d = latest.get(("^XSP", "1DTE"))
    if spx_1d and xsp_1d:
        lines.append("## 跨 venue 对比(^SPX 机构 vs ^XSP 散户化版)")
        lines.append("")
        lines.append(f"- ^SPX 1DTE GEX {_fmt_gex(spx_1d.get('net_dealer_gex'))} / skew {(spx_1d.get('skew_pts') or 0)*100:+.1f}pp")
        lines.append(f"- ^XSP 1DTE GEX {_fmt_gex(xsp_1d.get('net_dealer_gex'))} / skew {(xsp_1d.get('skew_pts') or 0)*100:+.1f}pp")
        spx_regime = _classify_gex_regime(spx_1d.get("net_dealer_gex"), spx_1d.get("spot"), spx_1d.get("gamma_flip_strike"))
        xsp_regime = _classify_gex_regime(xsp_1d.get("net_dealer_gex"), xsp_1d.get("spot"), xsp_1d.get("gamma_flip_strike"))
        if spx_regime == xsp_regime:
            lines.append(f"- 两个 venue regime 一致(都是 {spx_regime}),信号纯度高")
        else:
            lines.append(f"- ⚠️ regime 分歧:^SPX = {spx_regime} vs ^XSP = {xsp_regime} — 散户 venue 可能噪音,以 ^SPX 为准")
        lines.append("")

    # 4. Limitations footer
    lines.append("## 注意 / 局限")
    lines.append("")
    lines.append("- CBOE CDN quotes 15min 延迟,**非实时**;真要 scalp 需要 OPRA / Polygon / IBKR feed")
    lines.append("- GEX 模型假设 dealer 100% 短 call 长 put — 真实 dealer 仓位会偏离(尤其 high IV 期),作为方向指标用,不是精确 PnL")
    lines.append("- ^XND/^MRUT/^XEO 流动性薄,skew/GEX 噪音大,**不建议拿来交易**")
    lines.append("- ^VIX 期权 chain 已 fetch,但仅作 vol regime context,**不列为交易候选**")
    lines.append("- 这份 view 是数据快照 + 规则建议,不是策略,不构成投资建议")

    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=None)
    args = ap.parse_args()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)
    else:
        latest = con.execute("SELECT MAX(as_of) FROM index_gex_snapshots").fetchone()
        if not latest or not latest[0]:
            raise SystemExit("no index_gex_snapshots data — run compute_index_gex.py first")
        as_of = latest[0] if isinstance(latest[0], date) else date.fromisoformat(str(latest[0]))
    latest = _load_latest(con, as_of.isoformat())
    if not latest:
        raise SystemExit(f"no index_gex_snapshots rows for {as_of}")

    out_dir = OUTPUT_ROOT / as_of.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    snapshot_times = sorted({r["snapshot_time"] for r in latest.values()})
    hhmm = snapshot_times[-1].strftime("%H%M") if snapshot_times else datetime.utcnow().strftime("%H%M")
    out_path = out_dir / f"index_dashboard_{hhmm}.md"

    md = "\n".join(_render(latest, as_of)).rstrip() + "\n"
    out_path.write_text(md, encoding="utf-8")
    con.close()
    print(f"wrote {out_path} ({len(md)} bytes)")


if __name__ == "__main__":
    main()
