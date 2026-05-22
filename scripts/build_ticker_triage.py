"""Build a ticker triage doc for ai-infra basket — operator's one-page view.

Reads today's main_strategy_v2 artifacts + universe + options data; emits a
single markdown file under ai_infra/reports/ticker_triage_<date>.md.

Composition (per user's brief on 2026-05-22):
- Big table US top 25 by rank_score (one line per ticker, all dimensions)
- Big table CN top 15 by rank_score
- Detailed bio (~200-300 字) for top 10 US + top 5 CN names —
  each bio threads quant + options + evidence + 留/释/试错 verdict.

Run: python3 scripts/build_ticker_triage.py [--date 2026-05-22]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_universe() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for line in (ROOT / "ai_infra/data/global_universe_v2.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        ticker = (rec.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = rec
    return out


def evidence_tier(rec: dict) -> str:
    ev = (rec or {}).get("evidence_state") or ""
    head = ev.split(":")[0].strip()
    if "原文已证明" in head:
        return "证"
    if "合理推论" in head:
        return "推"
    if "待原文核验" in head or "原文需核验" in head:
        return "验"
    return "-"


def short(s, n=24):
    s = str(s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def fmt_pct(v, digits=0):
    if v is None:
        return "-"
    try:
        return f"{float(v):.{digits}f}%"
    except (TypeError, ValueError):
        return "-"


def fmt_num(v, d=2):
    if v is None:
        return "-"
    try:
        return f"{float(v):.{d}f}"
    except (TypeError, ValueError):
        return "-"


def regime_phrase(state: str) -> str:
    return {
        "hedge": "防守-基准", "wedge": "rates/credit 楔形",
        "confirm": "下行确认", "press": "press 期",
        "capitulation": "panic 出清",
    }.get(state, state)


def build_row_record(r, verdicts: dict, tenor_by_sym: dict, universe: dict, market: str) -> dict:
    sym = str(r.get("symbol") or "").upper()
    v = verdicts.get(sym) or {}
    tenor = tenor_by_sym.get(sym, [])
    uni = universe.get(sym) or {}
    sc = r.get("score_components") or {}
    bd = sc.get("broad_signal_breakdown") or {}
    iv_rk = v.get("iv_rank_pct")
    iv_ann = v.get("iv_ann")
    vrp = v.get("vrp")
    tenor_names = ", ".join(t.get("pattern", "") for t in tenor) if tenor else ""
    tenor_score = max((float(t.get("score") or 0.0) for t in tenor), default=0.0)
    return {
        "market": market,
        "sym": sym,
        "name": r.get("name") or uni.get("company") or "-",
        "rank": r.get("rank"),
        "rank_score": r.get("rank_score"),
        "tier": r.get("production_tier") or "-",
        "action": r.get("production_action") or "-",
        "sleeve": (r.get("alpha_sleeve_id") or r.get("strategy_family") or "-")
                  .replace("us_", "").replace("cn_", "").replace("_", " "),
        "broad": sc.get("broad_signal"),
        "momentum": bd.get("momentum_p_upside"),
        "breakout": bd.get("breakout"),
        "mr_bull": bd.get("mean_reversion_bull"),
        "mr_bear": bd.get("mean_reversion_bear_headwind"),
        "iv_ann": iv_ann,
        "iv_rk": iv_rk,
        "vrp": vrp,
        "pcz": v.get("pc_ratio_z"),
        "skz": v.get("skew_z"),
        "tenor_names": tenor_names,
        "tenor_score": tenor_score,
        "evidence": evidence_tier(uni),
        "ai_pool": r.get("ai_infra_current_pool") or "-",
        "module": uni.get("module") or "-",
        "narrative": r.get("narrative_group") or "-",
    }


def verdict_for_row(rec: dict) -> str:
    """One-word triage: 留 / 试错 / 观察 / 释放."""
    rs = float(rec.get("rank_score") or 0.0)
    action = (rec.get("action") or "").lower()
    if "buy_planned" in action or "production" in (rec.get("tier") or "").lower():
        return "**留**(执行候选)"
    if rs >= 60:
        return "**留**(rank ≥60,主线)"
    if rs >= 50:
        return "**试错**(rank 50-60,等触发)"
    if rs >= 40:
        return "观察(rank 40-50)"
    return "释放(rank < 40,无信号)"


def render_big_table(rows: list[dict], title: str) -> list[str]:
    lines = [f"## {title}", "",
             "| # | Symbol | Name | Sleeve | rank | broad | IV rank | VRP | Tenor 信号 | Evidence | 判定 |",
             "|---:|---|---|---|---:|---:|---:|---:|---|:---:|---|"]
    for i, r in enumerate(rows, 1):
        iv_s = f"{r['iv_rk']:.0f}%" if r["iv_rk"] is not None else "-"
        vrp_s = f"{r['vrp']*100:+.0f}pp" if r["vrp"] is not None else "-"
        broad_s = f"{r['broad']:.0f}" if r["broad"] is not None else "-"
        tenor_s = f"{short(r['tenor_names'], 28)}({r['tenor_score']:.0f})" if r["tenor_names"] else "-"
        v = verdict_for_row(r)
        lines.append(
            f"| {i} | **{r['sym']}** | {short(r['name'], 18)} | "
            f"{short(r['sleeve'], 22)} | {fmt_num(r['rank_score'], 1)} | "
            f"{broad_s} | {iv_s} | {vrp_s} | {short(tenor_s, 32)} | "
            f"{r['evidence']} | {v} |"
        )
    return lines + [""]


def bio_for_row(rec: dict, regime_state: str) -> str:
    """200-300 字 prose for one ticker, threading quant + options + evidence."""
    sym = rec["sym"]
    name = rec["name"]
    rank = rec["rank_score"]
    sleeve = rec["sleeve"]
    broad = rec["broad"]
    bd = (
        f"momentum {rec['momentum']:.0f}" if rec['momentum'] is not None else None,
        f"breakout {rec['breakout']:.0f}" if rec['breakout'] is not None else None,
        f"mean-rev(bull) {rec['mr_bull']:.0f}" if rec['mr_bull'] is not None else None,
        f"mean-rev(bear/headwind) {rec['mr_bear']:.0f}" if rec['mr_bear'] is not None else None,
    )
    bd_txt = ", ".join(x for x in bd if x)
    iv_rk = rec["iv_rk"]
    iv_ann = rec["iv_ann"]
    vrp = rec["vrp"]
    skz = rec["skz"]
    pcz = rec["pcz"]
    tenor = rec["tenor_names"]
    tenor_score = rec["tenor_score"]
    ev = rec["evidence"]
    module = short(rec["module"], 80)
    narrative = rec["narrative"]

    # Build narrative
    lines = []
    lines.append(f"### {sym} — {name}")
    lines.append("")

    # Quant block
    quant_bits = [f"今日 rank_score **{rank:.1f}**(sleeve `{sleeve.strip()}`)"]
    if broad is not None:
        quant_bits.append(f"broad_signal {broad:.0f}(在 {regime_phrase(regime_state)} regime 下:{bd_txt})")
    quant_bits.append(f"evidence 等级 **{ev}**")
    lines.append("- " + ";".join(quant_bits) + "。")

    # Options block
    if iv_ann is not None:
        opt_bits = [f"IV {iv_ann*100:.0f}%"]
        if iv_rk is not None:
            opt_bits.append(f"rank **{iv_rk:.0f}%**")
        if vrp is not None:
            opt_bits.append(f"VRP {vrp*100:+.0f}pp")
        if pcz is not None or skz is not None:
            opt_bits.append(
                f"PC z={pcz:+.1f} / Skew z={skz:+.1f}" if pcz is not None and skz is not None
                else (f"Skew z={skz:+.1f}" if skz is not None else f"PC z={pcz:+.1f}")
            )
        lines.append("- 期权侧 " + ", ".join(opt_bits) + "。")
    else:
        lines.append("- 期权侧暂无数据(options_sentiment 未覆盖)。")

    # Tenor signal
    if tenor:
        lines.append(f"- Tenor 异动:`{tenor}`(score {tenor_score:.0f}) —— 远月 OTM call 在堆积,机构在用 LEAPS 表达观点。")

    # Module / why-in-universe
    if module and module != "-":
        lines.append(f"- 入池理由(module 摘要):{module}")

    # Verdict / causal chain
    rationale = []
    if rank is not None:
        if rank >= 65:
            rationale.append("rank ≥65 = 量化主线票")
        elif rank >= 55:
            rationale.append("rank 中位 = 候选层")
        else:
            rationale.append(f"rank {rank:.0f} = 评分偏低")
    if iv_rk is not None:
        if iv_rk <= 20:
            rationale.append("IV rank ≤20%(LEAPS 候选)")
        elif iv_rk >= 80:
            rationale.append("IV rank ≥80%(只能股,不能期权)")
    if tenor and tenor_score >= 30:
        rationale.append(f"tenor 异动 {tenor_score:.0f}(机构远月堆 call)")
    if ev == "证":
        rationale.append("有原文证据")
    elif ev == "推":
        rationale.append("operator 强释,evidence 待补")

    verdict = verdict_for_row(rec).replace("**", "")
    if rationale:
        lines.append(f"- **结论 {verdict}**:" + ";".join(rationale) + "。")
    else:
        lines.append(f"- **结论 {verdict}**。")
    lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-05-22")
    args = ap.parse_args()
    date = args.date

    base = ROOT / f"reports/review_dashboard/main_strategy_v2/{date}"
    us_ranker = load_json(base / "us_opportunity_ranker.json")
    cn_ranker = load_json(base / "cn_opportunity_ranker.json")

    # options_verdicts / tenor_signals only live in the giant payload — pull
    # them from the report markdown's source: a payload pickle isn't dumped,
    # so we read options_anomaly + tenor from review_dashboard root directly.
    universe = load_universe()

    # Re-derive verdicts + tenor by re-running the same loaders the main report uses.
    import importlib.util, sys
    spec = importlib.util.spec_from_file_location("gen", ROOT / "scripts/generate_main_strategy_v2_report.py")
    gen = importlib.util.module_from_spec(spec); sys.modules["gen"] = gen; spec.loader.exec_module(gen)
    from datetime import date as date_cls
    y, m, d = map(int, date.split("-"))
    as_of = date_cls(y, m, d)
    us_db = ROOT / "quant-research-v1/data/quant.duckdb"
    syms = [str(r.get("symbol") or "").upper() for r in (us_ranker.get("all_rows") or [])[:25]]
    syms = [s for s in syms if s]
    verdicts = gen.build_options_verdicts(us_db, syms, as_of)
    tenor_signals = gen.load_options_tenor_signals(date)
    tenor_by_sym: dict[str, list] = {}
    for s in tenor_signals:
        tenor_by_sym.setdefault(str(s.get("symbol") or "").upper(), []).append(s)

    us_rows = [build_row_record(r, verdicts, tenor_by_sym, universe, "US")
               for r in (us_ranker.get("all_rows") or [])[:25]]
    cn_rows = [build_row_record(r, {}, {}, universe, "CN")
               for r in (cn_ranker.get("all_rows") or [])[:15]]

    # Read regime state today — pull from the IV view header which contains
    # a unique "当前 tape **<state>**" marker (not collidable with bubble_hedge).
    rep = (base / "us_daily_report.md").read_text(encoding="utf-8")
    regime_state = "hedge"
    import re as _re
    m = _re.search(r"当前 tape \*\*(\w+)\*\*", rep)
    if m:
        regime_state = m.group(1).lower()
    else:
        # fallback: any of the 5 canonical states inside parentheses
        for marker in ("capitulation", "press", "confirm", "wedge", "hedge"):
            if f"({marker})" in rep:
                regime_state = marker; break

    # ---- Assemble doc ----
    out: list[str] = [
        f"# Ticker Triage — {date}",
        "",
        f"操作员一页视图:US ranker top 25 + CN ranker top 15(共 40 票)。",
        f"今天 regime = **{regime_phrase(regime_state)}**。",
        "",
        "Evidence 列: **证**=原文已证明 / **推**=合理推论(operator-override) / **验**=待原文核验。",
        "判定列: **留**(保留主线) / **试错**(rank 50-60,等触发) / 观察 / 释放。",
        "",
    ]
    out += render_big_table(us_rows, "US — ranker top 25")
    out += render_big_table(cn_rows, "CN — ranker top 15")

    out += ["", "---", "", "## US 重点 10 名小传", ""]
    for rec in us_rows[:10]:
        out.append(bio_for_row(rec, regime_state))

    out += ["", "---", "", "## CN 重点 5 名小传", ""]
    for rec in cn_rows[:5]:
        out.append(bio_for_row(rec, regime_state))

    out += [
        "---",
        "",
        "## 决议汇总",
        "",
    ]
    keep_us = [r for r in us_rows if "留" in verdict_for_row(r)]
    trial_us = [r for r in us_rows if "试错" in verdict_for_row(r)]
    keep_cn = [r for r in cn_rows if "留" in verdict_for_row(r)]
    trial_cn = [r for r in cn_rows if "试错" in verdict_for_row(r)]
    out.append(f"- **US 留** ({len(keep_us)}): " + ", ".join(r["sym"] for r in keep_us))
    if trial_us:
        out.append(f"- **US 试错** ({len(trial_us)}): " + ", ".join(r["sym"] for r in trial_us))
    out.append(f"- **CN 留** ({len(keep_cn)}): " + ", ".join(r["sym"] for r in keep_cn))
    if trial_cn:
        out.append(f"- **CN 试错** ({len(trial_cn)}): " + ", ".join(r["sym"] for r in trial_cn))

    output_path = ROOT / f"ai_infra/reports/ticker_triage_{date}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {output_path}")
    print(f"  US rows: {len(us_rows)}  CN rows: {len(cn_rows)}")
    print(f"  US 留: {len(keep_us)} / 试错: {len(trial_us)}")
    print(f"  CN 留: {len(keep_cn)} / 试错: {len(trial_cn)}")


if __name__ == "__main__":
    main()
