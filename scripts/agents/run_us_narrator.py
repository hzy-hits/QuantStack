"""US daily report agent narrator — 4 extractor + 1 narrator pipeline.

Phase D.2 of PHASE_D_PLAN.md.

Architecture (mirrors quant-research-cn/src/main.rs agent flow):
    payload (loaded from existing JSON artifacts)
       ↓
    4 extractors in parallel (DeepSeek):
      macro / event / quant / risk
       ↓
    narrator (DeepSeek) — receives 4 extractor outputs + payload digest
       ↓
    us_daily_report_agent.md (sibling to programmatic us_daily_report.md
                              for side-by-side comparison until D.5)

Usage:
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27 --overwrite
        # overwrite existing us_daily_report.md (Phase D.5 default)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import requests
import yaml

ROOT = Path(__file__).resolve().parents[2]
CN_CONFIG = ROOT / "quant-research-cn" / "config.yaml"
PROMPTS_DIR = ROOT / "quant-research-v1" / "prompts"
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"


def load_deepseek_key() -> str:
    cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
    key = (cfg.get("api") or {}).get("deepseek_key")
    if not key:
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


def load_prompt(name: str) -> str:
    """Load us-{name}-analyst.md (or us-merge-agent.md if name='merge')."""
    if name == "merge":
        path = PROMPTS_DIR / "us-merge-agent.md"
    else:
        path = PROMPTS_DIR / f"us-{name}-analyst.md"
    if not path.exists():
        raise SystemExit(f"prompt missing: {path}")
    return path.read_text(encoding="utf-8")


def load_payload_artifacts(report_dir: Path) -> dict[str, Any]:
    """Read all *.json artifacts in report_dir + the existing us_daily_report.md."""
    out: dict[str, Any] = {}
    for jpath in report_dir.glob("*.json"):
        try:
            out[jpath.stem] = json.loads(jpath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
    md_path = report_dir / "us_daily_report.md"
    if md_path.exists():
        out["_us_daily_report_md"] = md_path.read_text(encoding="utf-8")
    return out


def build_macro_payload(art: dict[str, Any]) -> str:
    """Build a compact text payload for the macro extractor."""
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "风控引擎",
        "恐惧贪婪",
        "SPX × P/C",
        "Bubble Hedge",
    ])
    return _join_payload_sections("MACRO PAYLOAD", sections)


def build_event_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "Serenity",
        "财报日历",
        "美股财报",
        "Source Review",
    ])
    digest_lines = [sections]
    # NVDA investments are in DB not in report; pass a placeholder note
    digest_lines.append(
        "\n\n_NVDA investments table: refer to daily_news_digest_<date>.md if present._"
    )
    return _join_payload_sections("EVENT PAYLOAD", "\n".join(digest_lines))


def build_quant_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "🎲 今日概率最优",
        "🎯 今日只看这些",
        "可交易名单",
        "逐票复核",
        "US 期权 IV 视图",
        "US 期权定位",
        "美股生产排序",
        "AI Supercycle Layer Attribution",
    ])
    return _join_payload_sections("QUANT PAYLOAD", sections)


def build_news_payload(art: dict[str, Any], as_of: str) -> str:
    """Build news payload by querying news_scored + serenity_picks DIRECTLY from DB.

    Unlike other extractors that read sliced md sections, this one bypasses the
    programmatic report filters so the agent sees raw DeepSeek-scored news + raw
    Serenity stance snapshot.
    """
    if not US_DB.exists():
        return _join_payload_sections("NEWS PAYLOAD", "[US DB not found: " + str(US_DB) + "]")
    try:
        con = duckdb.connect(str(US_DB), read_only=True)
    except duckdb.IOException as exc:
        return _join_payload_sections("NEWS PAYLOAD", f"[DB open failed: {exc}]")

    out_parts: list[str] = []

    # 1) News scored — last 48h, subject_match=true, severity >= 1
    try:
        news_rows = con.execute(
            """
            SELECT symbol, severity, sentiment, event_type, summary_zh,
                   strftime(published_at, '%Y-%m-%d %H:%M') AS pub_str,
                   headline
            FROM news_scored
            WHERE subject_match = true
              AND severity >= 1
              AND scored_at >= NOW() - INTERVAL '48 hours'
            ORDER BY severity DESC, scored_at DESC
            LIMIT 40
            """
        ).fetchall()
    except duckdb.Error as exc:
        news_rows = []
        out_parts.append(f"[news_scored query failed: {exc}]")
    out_parts.append(f"## news_scored (last 48h, subject_match=true, severity>=1) — {len(news_rows)} rows")
    out_parts.append("| symbol | sev | sent | event_type | summary_zh | published | headline |")
    out_parts.append("|---|---:|:---:|:---:|---|---|---|")
    for r in news_rows[:25]:
        sym, sev, sent, etype, summary, pub, headline = r
        summary = (summary or "")[:80].replace("|", "/")
        headline = (headline or "")[:80].replace("|", "/")
        out_parts.append(f"| {sym} | {sev} | {sent} | {etype} | {summary} | {pub} | {headline} |")

    # 2) Serenity picks — current snapshot, focus on view_change + priority
    try:
        sere_rows = con.execute(
            """
            SELECT ticker, stance, ai_chain_segment, priority_score,
                   view_change, ret_1m, ret_6m
            FROM serenity_picks
            ORDER BY priority_score DESC
            LIMIT 50
            """
        ).fetchall()
    except duckdb.Error as exc:
        sere_rows = []
        out_parts.append(f"[serenity_picks query failed: {exc}]")
    out_parts.append("")
    out_parts.append(f"## serenity_picks (top by priority_score) — {len(sere_rows)} rows")
    out_parts.append("| ticker | stance | segment | prio | change_type | prev→now | ret_1m | ret_6m |")
    out_parts.append("|---|:---:|:---:|---:|:---:|---|---:|---:|")
    for r in sere_rows[:30]:
        tic, stance, seg, prio, vc_raw, r1m, r6m = r
        change_type = "?"
        prev_stance = ""
        cur_stance = ""
        if vc_raw:
            try:
                vc = vc_raw if isinstance(vc_raw, dict) else __import__("ast").literal_eval(vc_raw)
                change_type = vc.get("change_type", "?")
                prev_stance = vc.get("previous_stance", "")
                cur_stance = vc.get("current_stance", "")
            except (ValueError, SyntaxError):
                pass
        flip_str = f"{prev_stance}→{cur_stance}" if (prev_stance and cur_stance and prev_stance != cur_stance) else "—"
        r1m_str = f"{r1m:+.1f}%" if r1m is not None else ""
        r6m_str = f"{r6m:+.1f}%" if r6m is not None else ""
        out_parts.append(f"| {tic} | {stance} | {seg} | {prio:.0f} | {change_type} | {flip_str} | {r1m_str} | {r6m_str} |")

    # 3) Cross-ref hint: tickers appearing in BOTH sources today
    news_syms = {r[0] for r in news_rows if r[1] >= 2}
    sere_syms = {r[0] for r in sere_rows if r[3] >= 100}
    overlap = sorted(news_syms & sere_syms)
    out_parts.append("")
    out_parts.append(f"## overlap hint (news sev>=2 ∩ serenity prio>=100): {len(overlap)} tickers")
    out_parts.append(", ".join(overlap) if overlap else "[no overlap]")

    con.close()
    return _join_payload_sections("NEWS PAYLOAD", "\n".join(out_parts))


def build_options_payload(art: dict[str, Any], as_of: str) -> str:
    """Build options payload by querying chain quotes + sentiment + alpha DIRECTLY from DB.

    Like news payload, bypasses the md slice path so the agent sees raw
    short-DTE anomalies the programmatic report filters out (e.g. SPY/QQQ
    1DTE put surge at 1000x v/OI).
    """
    if not US_DB.exists():
        return _join_payload_sections("OPTIONS PAYLOAD", f"[US DB not found: {US_DB}]")
    try:
        con = duckdb.connect(str(US_DB), read_only=True)
    except duckdb.IOException as exc:
        return _join_payload_sections("OPTIONS PAYLOAD", f"[DB open failed: {exc}]")

    out_parts: list[str] = []

    # 1) Index short-DTE hedging — both ETFs (SPY/QQQ/IWM) and cash-settled
    # indices (^SPX/^NDX/^XSP/^RUT) where the latter have daily expiries
    # including 0DTE, much deeper institutional flow visibility.
    try:
        idx_rows = con.execute(
            """
            SELECT symbol, days_to_exp, expiry, option_type, strike, current_price,
                   volume, open_interest,
                   CASE WHEN open_interest > 0 THEN volume::DOUBLE/open_interest ELSE NULL END AS vol_oi,
                   mid
            FROM options_chain_quotes
            WHERE as_of = (SELECT MAX(as_of) FROM options_chain_quotes)
              AND symbol IN ('SPY', 'QQQ', 'IWM', '^SPX', '^NDX', '^XSP', '^RUT')
              AND days_to_exp BETWEEN 0 AND 7
              AND volume >= 5000
              AND open_interest >= 30
            ORDER BY (volume::DOUBLE / open_interest) DESC NULLS LAST
            LIMIT 30
            """
        ).fetchall()
    except duckdb.Error as exc:
        idx_rows = []
        out_parts.append(f"[index query failed: {exc}]")
    out_parts.append(f"## Index short-DTE hedging (SPY/QQQ/IWM ETF + ^SPX/^NDX/^XSP/^RUT cash, DTE 0-7, vol>=5000, v/OI>=30) — {len(idx_rows)} rows")
    out_parts.append("| symbol | DTE | expiry | type | strike | spot | OTM% | volume | OI | v/OI | mid |")
    out_parts.append("|---|---:|---|:---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in idx_rows[:15]:
        sym, dte, exp, otype, strike, spot, vol, oi, voi, mid = r
        otm_pct = ((strike - spot) / spot * 100) if spot else 0
        mid_s = f"{mid:.2f}" if mid is not None else "—"
        out_parts.append(
            f"| {sym} | {dte} | {exp} | {otype} | {strike:.2f} | {spot:.2f} | "
            f"{otm_pct:+.2f} | {vol:,} | {oi:,} | {voi:.1f} | {mid_s} |"
        )

    # 2) Per-symbol short-DTE anomalies (DTE ≤ 7, v/OI ≥ 50, exclude indices)
    try:
        sym_rows = con.execute(
            """
            SELECT symbol, days_to_exp, expiry, option_type, strike, current_price,
                   volume, open_interest,
                   CASE WHEN open_interest > 0 THEN volume::DOUBLE/open_interest ELSE NULL END AS vol_oi,
                   mid
            FROM options_chain_quotes
            WHERE as_of = (SELECT MAX(as_of) FROM options_chain_quotes)
              AND symbol NOT IN ('SPY', 'QQQ', 'IWM', 'GLD', 'SLV', 'TLT', 'USO', 'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC',
                                 '^SPX', '^NDX', '^XSP', '^RUT', '^VIX', 'DIA')
              AND days_to_exp BETWEEN 0 AND 7
              AND volume >= 1000
              AND open_interest >= 50
              AND (volume::DOUBLE / open_interest) >= 30
            ORDER BY (volume::DOUBLE / open_interest) DESC NULLS LAST
            LIMIT 30
            """
        ).fetchall()
    except duckdb.Error as exc:
        sym_rows = []
        out_parts.append(f"[symbol query failed: {exc}]")
    out_parts.append("")
    out_parts.append(f"## per-symbol short-DTE anomalies (DTE 0-7, vol>=1000, OI>=50, v/OI>=30) — {len(sym_rows)} rows")
    out_parts.append("| symbol | DTE | expiry | type | strike | spot | OTM% | volume | OI | v/OI |")
    out_parts.append("|---|---:|---|:---:|---:|---:|---:|---:|---:|---:|")
    for r in sym_rows[:20]:
        sym, dte, exp, otype, strike, spot, vol, oi, voi, mid = r
        otm_pct = ((strike - spot) / spot * 100) if spot else 0
        out_parts.append(
            f"| {sym} | {dte} | {exp} | {otype} | {strike:.2f} | {spot:.2f} | "
            f"{otm_pct:+.2f} | {vol:,} | {oi:,} | {voi:.1f} |"
        )

    # 3) options_alpha top directional_edge
    try:
        alpha_rows = con.execute(
            """
            SELECT symbol, directional_edge, vol_edge, vrp_edge, flow_edge,
                   liquidity_gate, expression, reason
            FROM options_alpha
            WHERE as_of = (SELECT MAX(as_of) FROM options_alpha)
              AND ABS(directional_edge) >= 0.5
              AND liquidity_gate = 'pass'
            ORDER BY ABS(directional_edge) DESC NULLS LAST
            LIMIT 20
            """
        ).fetchall()
    except duckdb.Error as exc:
        alpha_rows = []
        out_parts.append(f"[options_alpha query failed: {exc}]")
    out_parts.append("")
    out_parts.append(f"## options_alpha (|directional_edge|>=0.5, liq=pass) — {len(alpha_rows)} rows")
    out_parts.append("| symbol | dir | vol | vrp | flow | expression | reason |")
    out_parts.append("|---|---:|---:|---:|---:|:---:|---|")
    for r in alpha_rows[:15]:
        sym, de, ve, vre, fe, lg, expr, reason = r
        reason_s = (reason or "")[:60].replace("|", "/")
        out_parts.append(
            f"| {sym} | {de:+.3f} | {ve or 0:+.3f} | {vre or 0:+.3f} | "
            f"{fe or 0:+.3f} | {expr} | {reason_s} |"
        )

    # 4) options_sentiment extreme z-scores
    try:
        sent_rows = con.execute(
            """
            SELECT symbol, pc_ratio_z, pc_ratio_raw, skew_z, vrp_z, iv_ann, rv_ann
            FROM options_sentiment
            WHERE as_of = (SELECT MAX(as_of) FROM options_sentiment)
              AND (ABS(pc_ratio_z) >= 3.0 OR ABS(skew_z) >= 3.0)
            ORDER BY ABS(pc_ratio_z) + ABS(skew_z) DESC NULLS LAST
            LIMIT 25
            """
        ).fetchall()
    except duckdb.Error as exc:
        sent_rows = []
        out_parts.append(f"[options_sentiment query failed: {exc}]")
    out_parts.append("")
    out_parts.append(f"## options_sentiment extremes (|pc_z| or |skew_z| >= 3.0) — {len(sent_rows)} rows")
    out_parts.append("| symbol | pc_z | pc_raw | skew_z | vrp_z | iv_ann | rv_ann |")
    out_parts.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in sent_rows[:18]:
        sym, pcz, pcr, skz, vrpz, iv, rv = r
        out_parts.append(
            f"| {sym} | {pcz:+.2f} | {pcr or 0:.2f} | {skz:+.2f} | "
            f"{vrpz or 0:+.2f} | {iv or 0:.1f} | {rv or 0:.1f} |"
        )

    # 5) earnings catalyst overlay (next 7 days) for cross-reference
    try:
        er_rows = con.execute(
            """
            SELECT DISTINCT symbol, report_date::VARCHAR AS er_date
            FROM earnings_calendar
            WHERE report_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
            ORDER BY report_date, symbol
            LIMIT 40
            """
        ).fetchall()
    except duckdb.Error as exc:
        er_rows = []
    out_parts.append("")
    out_parts.append(f"## upcoming earnings (next 7d) for catalyst cross-ref — {len(er_rows)} rows")
    out_parts.append(", ".join(f"{r[0]}({r[1]})" for r in er_rows[:30]) or "[none]")

    con.close()
    return _join_payload_sections("OPTIONS PAYLOAD", "\n".join(out_parts))


def build_risk_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "US Production Gate",
        "US 期权异常",
        "US 左侧观察池",
        "组合风险覆盖",
        "Portfolio Risk Overlay",
    ])
    return _join_payload_sections("RISK PAYLOAD", sections)


def _slice_md_sections(md: str, headers: list[str]) -> str:
    """Extract sections (## starting with one of headers) from md."""
    out: list[str] = []
    cur_section: list[str] = []
    cur_match = False
    for line in md.split("\n"):
        if line.startswith("## "):
            if cur_match and cur_section:
                out.extend(cur_section)
                out.append("")
            cur_section = [line]
            cur_match = any(h in line for h in headers)
        else:
            cur_section.append(line)
    if cur_match and cur_section:
        out.extend(cur_section)
    return "\n".join(out)


def _join_payload_sections(label: str, sections: str) -> str:
    if not sections.strip():
        return f"# {label}\n\n[no relevant data sections found in payload]"
    return f"# {label}\n\n{sections}"


def call_deepseek(api_key: str, system: str, user: str, *,
                  temperature: float = 0.2, max_tokens: int = 1500) -> str | None:
    """Sync DeepSeek call. Returns content or None on error."""
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        r = requests.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except (requests.RequestException, KeyError, ValueError) as e:
        print(f"  [warn] DeepSeek call failed: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
        return None


async def call_extractor_async(api_key: str, name: str, payload_text: str) -> tuple[str, str]:
    """Async wrapper around sync requests call for parallel extractor invocation."""
    prompt = load_prompt(name)
    # Substitute {payload_*} placeholder with actual payload
    placeholder = "{payload_" + name + "}" if name not in ("merge",) else None
    if placeholder and placeholder in prompt:
        system_part = prompt.split(placeholder)[0]
        # Use the part before placeholder as system; payload as user
        system_msg = system_part
        user_msg = payload_text
    else:
        # Fallback: use the whole prompt as system, payload as user
        system_msg = prompt
        user_msg = payload_text

    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: call_deepseek(api_key, system_msg, user_msg, temperature=0.1, max_tokens=1200),
    )
    return name, response or f"[{name} extractor failed]"


async def run_extractors(api_key: str, art: dict[str, Any], as_of: str) -> dict[str, str]:
    """Run 6 extractors in parallel: macro / event / quant / risk / news / options."""
    payloads = {
        "macro": build_macro_payload(art),
        "event": build_event_payload(art),
        "quant": build_quant_payload(art),
        "risk": build_risk_payload(art),
        "news": build_news_payload(art, as_of),
        "options": build_options_payload(art, as_of),
    }
    tasks = [
        call_extractor_async(api_key, name, payload)
        for name, payload in payloads.items()
    ]
    results = await asyncio.gather(*tasks)
    return dict(results)


def call_narrator(api_key: str, extractor_outputs: dict[str, str],
                  art: dict[str, Any], as_of: str) -> str | None:
    """Single narrator call — receives extractor outputs + payload digest."""
    prompt = load_prompt("merge")
    payload_digest = art.get("_us_daily_report_md", "")[:30000]  # cap to avoid token blow-up
    user_msg = (
        f"### 宏观提取\n{extractor_outputs.get('macro', '[missing]')}\n\n"
        f"### 事件提取\n{extractor_outputs.get('event', '[missing]')}\n\n"
        f"### 量化提取\n{extractor_outputs.get('quant', '[missing]')}\n\n"
        f"### 风险提取\n{extractor_outputs.get('risk', '[missing]')}\n\n"
        f"### 新闻提取(DeepSeek 已打分 + Serenity 双源)\n{extractor_outputs.get('news', '[missing]')}\n\n"
        f"### 期权提取(短端 hedging + 综合定向 + sentiment 极端)\n{extractor_outputs.get('options', '[missing]')}\n\n"
        f"### Payload Digest(交叉验证用)\n{payload_digest}\n\n"
        f"### 任务\n请按 us-merge-agent.md 的输出格式,为日期 {as_of} 生成完整美股日报。"
    )
    return call_deepseek(api_key, prompt, user_msg, temperature=0.3, max_tokens=4500)


async def main_async(args) -> None:
    as_of = args.date
    report_dir = ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of
    if not report_dir.exists():
        raise SystemExit(f"report dir missing: {report_dir}")

    api_key = load_deepseek_key()
    art = load_payload_artifacts(report_dir)
    print(f"=== US narrator agent — {as_of} ===")
    print(f"  loaded {len(art)} artifacts; md size {len(art.get('_us_daily_report_md', ''))}")

    print("  running 6 extractors in parallel (macro/event/quant/risk/news/options)...")
    extractor_outputs = await run_extractors(api_key, art, as_of)
    for name, out in extractor_outputs.items():
        print(f"    {name}: {len(out)} chars")
        if args.dump_extractors:
            (report_dir / f"_extractor_{name}.md").write_text(out, encoding="utf-8")

    print("  calling narrator...")
    narrative = call_narrator(api_key, extractor_outputs, art, as_of)
    if not narrative:
        print("  narrator failed; not writing output")
        return

    out_name = "us_daily_report.md" if args.overwrite else "us_daily_report_agent.md"
    out_path = report_dir / out_name
    out_path.write_text(narrative, encoding="utf-8")
    print(f"  wrote {out_path} ({len(narrative)} chars)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite us_daily_report.md (Phase D.5 default)")
    ap.add_argument("--dump-extractors", action="store_true",
                    help="Also write _extractor_{name}.md for debugging")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
