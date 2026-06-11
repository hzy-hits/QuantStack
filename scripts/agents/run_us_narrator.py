"""US daily report Codex narrator — extractor + narrator pipeline.

Phase D.2 of docs/archive/PHASE_D_PLAN.md.

Architecture:
    payload (loaded from existing JSON artifacts)
       ↓
    6 Codex extractors in parallel:
      macro / event / quant / risk / news / options
       ↓
    Codex narrator — receives extractor outputs + payload digest
       ↓
    us_daily_report_agent.md + us_daily_report_agent.md.meta.json

Usage:
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27 --overwrite
        # overwrite existing us_daily_report.md (Phase D.5 default)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from codex_backend import (  # noqa: E402
    backend,
    call_llm,
    concurrency,
    runtime_backend_summary,
    runtime_model_summary,
)
from sections.gamma_spring import build_gamma_spring_snapshot, render_gamma_spring_section  # noqa: E402
from validate_main_strategy_v2_reports import validate_us_report_text_against_payload  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PROMPTS_DIR = ROOT / "quant-research-v1" / "prompts"
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def final_style_guard(as_of: str) -> str:
    return f"""
## 最终写作覆盖（最高优先级）

这份报告是发给人读的美股交易 memo，不是多 agent 调试日志。最终输出必须是“Codex 结构化研报”：先讲清楚今天的策略故事,再用量化、风控、新闻、期权/Gamma 去解释为什么这样交易。不要写成问答模板,也不要写成模型输出堆砌。

- 第一行固定为 `# 美股量化日报 — {as_of}`。
- 只写 6 个二级标题：`策略主线`、`市场结构`、`交易计划`、`风险与反证`、`催化与复核`、`附注`。
- `策略主线` 写成 2-4 个自然段,像交易员晨会复盘: 市场在演什么故事,为什么这导致今天的仓位选择,哪些证据支持,哪些证据反驳。不要写成模板问答。
- 首次出现术语要顺手翻译,但不要做术语表。例: `0R` 写成“不新增仓位风险(0R)”; `WEDGE` 写成“趋势强但利率/波动仍咬住的 WEDGE 状态”; `MRS` 写成“市场风险偏好分数 MRS”; `Gamma` 写成“期权仓位形成的支撑/压力区”。
- 全文至少包含 4 张 Markdown 表格，且表格必须使用 `|` 分隔列：
  1. `交易计划` 内必须有 Production candidates 表，列为 `Symbol / Decision / Size / Entry / Risk / Hedge / Why`。
  2. `风险与反证` 内必须有 Watch / 0R context 表，列为 `Symbol / Status / Reason / Next check`。
  3. `风险与反证` 内必须有 IV/HV 表，列为 `Symbol / IV/HV / IV rank / Context / Action`。
  4. `风险与反证` 内必须有 Gamma v3 表，列为 `Symbol / Gamma state / Dealer proxy / Wall / Management`。
- `市场结构` 先讲 regime/tape/资金风险的因果链,再给市场证据表。表里必须包含数据校准行：报告标签日期、US 收盘价数据截至、US 候选/执行数据日期、期权/Gamma 有效日、Fear/Greed source。若 US 数据状态是 previous_session，必须明写“不是当日美股已收盘数据”。
- `市场结构` 内必须保留 `US Realized Horizon Edge` 字样和对应小表或行,写成“历史持有周期复盘(US Realized Horizon Edge)”,说明 1D/3D/5D/10D 哪些周期真的赚钱。
- 如果某张表没有行，也要保留表头并写一行 `None | - | - | - | -`，不要把表格删掉。
- 不出现这些词或痕迹：提取器、payload、digest、merge-agent、ranker、模型名、system prompt、user_msg、英文分层名。
- 不使用 emoji 或装饰符号；最终报告只靠标题、紧凑表格和短段落组织信息。每张表的分隔行(`|---|`)列数必须与表头一致。
- 执行表中的票在正文其它位置只许用“持有/持有不加码/加码受限/止损收紧”语态；“仅观察”“不执行”只能用于未执行票。
- 不直接输出内部字段名：`stable_alpha_gate`、`ev_status`、`production_decision_summary`、`actionable`、`execution_blocked_0r`、`active_watch`、`ranked_watch`。要翻译成人话，例如“稳定策略门禁未放行”“只观察，不执行”。不要把 `WEDGE`、`MRS`、`0R` 当作结论本身。
- Fear/Greed 必须保留 source。source=proxy 时只能写 `Internal Fear/Greed proxy`，不得写成 `CNN Fear & Greed` 或 `CNN F&G`；source=cnn 时才可以写 CNN。
- `交易计划` 必须先给正式执行表；没有正式执行时，表格第一行写 `None` 并在表后自然解释“本期无可执行做多”。随后用一小段交易故事解释为什么高分观察票没有变成仓位,不要制造半执行清单。
- 期权和新闻只能解释股票决策和风险，不给期权合约、strike、到期日或期权买卖指令。IV/HV 只写成“健康带/事件溢价高/波动过低或过高”的股票上下文；Gamma Spring v3 是 GEX curve + 区间状态机,也是 `us_gamma_v2_alpha` 兼容 sleeve 的选股/入场主引擎之一,但必须翻译成“期权仓位支撑/压力区”和“追高/止损/等待”的股票管理语言，不写成期权买卖建议。
- Congressional Trading / 政策资金流只能写成催化或风险 overlay：同委员会多人买入提高观察优先级，刚披露交易强调时间窗口，集中卖出触发风险复核；它不证明 AI 供应链关系，也不能直接生成 R。
- 不限制字数。表格承载事实，段落承载裁决；每张表后最多写 2-4 句解释，不要把表格内容重复写成散文。
- 可以保留股票代码、R、beta、IV、VIX、P/C、EMA、SMH、SPY、QQQ 等必要缩写；其它机器状态一律翻译。
"""


def write_agent_report(report_dir: Path, out_name: str, narrative: str, as_of: str) -> Path:
    out_path = report_dir / out_name
    out_path.write_text(narrative, encoding="utf-8")
    meta = {
        "as_of": as_of,
        "backend": runtime_backend_summary(),
        "model": runtime_model_summary(),
        "reasoning_effort": os.environ.get("CODEX_REASONING_EFFORT", "high"),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "script": Path(__file__).name,
    }
    out_path.with_name(out_path.name + ".meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return out_path


def load_prompt(name: str) -> str:
    """Load us-{name}-analyst.md (or us-merge-agent.md if name='merge')."""
    if name == "merge":
        path = PROMPTS_DIR / "us-merge-agent.md"
    else:
        path = PROMPTS_DIR / f"us-{name}-analyst.md"
    if not path.exists():
        raise SystemExit(f"prompt missing: {path}")
    return path.read_text(encoding="utf-8")


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _call_llm_with_retries(
    system_msg: str,
    user_msg: str,
    *,
    label: str,
    temperature: float,
    max_tokens: int,
    attempts: int,
) -> str | None:
    last_error = ""
    for idx in range(1, max(1, attempts) + 1):
        attempt_label = label if attempts <= 1 else f"{label}:attempt{idx}"
        try:
            response = call_llm(
                system_msg,
                user_msg,
                label=attempt_label,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        except Exception as exc:  # noqa: BLE001 - retry any backend failure uniformly.
            last_error = str(exc)
            continue
        if response:
            return response
        last_error = "empty output"
    if last_error:
        print(f"  {label} failed after {attempts} attempt(s): {last_error}")
    return None


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
        "Congressional Trading",
        "政策资金流",
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
    programmatic report filters so the agent sees raw scored news + external
    research-source stance snapshots.
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

    # 1a) ETF index short-DTE — SPY/QQQ/IWM/DIA, rank by v/OI (retail-driven,
    # surges show new-open hedging when v/OI explodes vs small base OI).
    try:
        etf_rows = con.execute(
            """
            SELECT symbol, days_to_exp, expiry, option_type, strike, current_price,
                   volume, open_interest,
                   CASE WHEN open_interest > 0 THEN volume::DOUBLE/open_interest ELSE NULL END AS vol_oi,
                   mid
            FROM options_chain_quotes
            WHERE as_of = (SELECT MAX(as_of) FROM options_chain_quotes)
              AND symbol IN ('SPY', 'QQQ', 'IWM', 'DIA')
              AND days_to_exp BETWEEN 0 AND 7
              AND volume >= 5000
              AND open_interest >= 30
            ORDER BY (volume::DOUBLE / open_interest) DESC NULLS LAST
            LIMIT 20
            """
        ).fetchall()
    except duckdb.Error as exc:
        etf_rows = []
        out_parts.append(f"[ETF index query failed: {exc}]")
    out_parts.append(f"## Index ETF short-DTE (SPY/QQQ/IWM/DIA, DTE 0-7, vol>=5000, ranked by v/OI) — {len(etf_rows)} rows")
    out_parts.append("| symbol | DTE | expiry | type | strike | spot | OTM% | volume | OI | v/OI | mid |")
    out_parts.append("|---|---:|---|:---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in etf_rows[:12]:
        sym, dte, exp, otype, strike, spot, vol, oi, voi, mid = r
        otm_pct = ((strike - spot) / spot * 100) if spot else 0
        mid_s = f"{mid:.2f}" if mid is not None else "—"
        out_parts.append(
            f"| {sym} | {dte} | {exp} | {otype} | {strike:.2f} | {spot:.2f} | "
            f"{otm_pct:+.2f} | {vol:,} | {oi:,} | {voi:.1f} | {mid_s} |"
        )

    # 1b) Cash-settled index short-DTE — institutional-driven, much larger OI
    # so v/OI runs 10-50x instead of 100-1000x. Rank by absolute volume to
    # surface institutional positioning (won't be picked up by v/OI sort which
    # is dominated by retail ETF flow).
    try:
        cash_rows = con.execute(
            """
            SELECT symbol, days_to_exp, expiry, option_type, strike, current_price,
                   volume, open_interest,
                   CASE WHEN open_interest > 0 THEN volume::DOUBLE/open_interest ELSE NULL END AS vol_oi,
                   mid
            FROM options_chain_quotes
            WHERE as_of = (SELECT MAX(as_of) FROM options_chain_quotes)
              AND symbol IN ('^SPX', '^NDX', '^RUT', '^XEO', '^XSP', '^XND', '^MRUT', '^VIX')
              AND days_to_exp BETWEEN 0 AND 7
              AND volume >= 1000
            ORDER BY volume DESC NULLS LAST
            LIMIT 30
            """
        ).fetchall()
    except duckdb.Error as exc:
        cash_rows = []
        out_parts.append(f"[cash index query failed: {exc}]")
    out_parts.append("")
    out_parts.append(f"## Cash-settled index short-DTE (^SPX/^NDX/^XSP/^XND/^MRUT/^RUT/^XEO/^VIX, DTE 0-7, vol>=1000, ranked by volume) — {len(cash_rows)} rows")
    out_parts.append("| symbol | DTE | expiry | type | strike | spot | OTM% | volume | OI | v/OI | mid |")
    out_parts.append("|---|---:|---|:---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in cash_rows[:18]:
        sym, dte, exp, otype, strike, spot, vol, oi, voi, mid = r
        otm_pct = ((strike - spot) / spot * 100) if spot else 0
        mid_s = f"{mid:.2f}" if mid is not None else "—"
        voi_s = f"{voi:.1f}" if voi is not None else "—"
        out_parts.append(
            f"| {sym} | {dte} | {exp} | {otype} | {strike:.2f} | {spot:.2f} | "
            f"{otm_pct:+.2f} | {vol:,} | {oi:,} | {voi_s} | {mid_s} |"
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
              AND symbol NOT IN ('SPY', 'QQQ', 'IWM', 'DIA',
                                 'GLD', 'SLV', 'TLT', 'USO',
                                 'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC',
                                 '^SPX', '^NDX', '^RUT', '^XEO',
                                 '^XSP', '^XND', '^MRUT', '^VIX')
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

    # 4b) IV/HV regime: current implied vol vs realized vol, plus current IV
    # percentile inside the available options_sentiment history. This is a
    # stock-timing / risk context, not an option trade recommendation.
    try:
        ivhv_rows = con.execute(
            """
            WITH latest AS (
                SELECT MAX(as_of) AS d FROM options_sentiment
            ),
            hist AS (
                SELECT
                    symbol,
                    as_of,
                    iv_ann,
                    PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY iv_ann) AS iv_pr,
                    COUNT(*) OVER (PARTITION BY symbol) AS hist_n
                FROM options_sentiment
                WHERE iv_ann IS NOT NULL
                  AND as_of >= (SELECT d - INTERVAL '400 days' FROM latest)
            ),
            cur AS (
                SELECT
                    os.symbol,
                    os.iv_ann,
                    os.rv_ann,
                    os.vrp,
                    os.vrp_z,
                    os.pc_ratio_z,
                    os.skew_z,
                    h.iv_pr,
                    h.hist_n,
                    os.iv_ann / NULLIF(os.rv_ann, 0) AS iv_hv
                FROM options_sentiment os
                JOIN latest ON os.as_of = latest.d
                LEFT JOIN hist h ON h.symbol = os.symbol AND h.as_of = os.as_of
                WHERE os.iv_ann IS NOT NULL
                  AND os.rv_ann IS NOT NULL
                  AND os.rv_ann > 0
                  AND os.symbol NOT LIKE '^%'
                  AND os.symbol NOT IN (
                    'SPY','QQQ','IWM','DIA','GLD','SLV','TLT','USO',
                    'XLK','XLF','XLE','XLV','XLI','XLY','XLP','XLU','XLB','XLRE','XLC'
                  )
            )
            SELECT symbol, iv_ann, rv_ann, iv_hv, vrp, vrp_z, iv_pr, hist_n, pc_ratio_z, skew_z,
                   CASE
                       WHEN COALESCE(iv_pr, 0.5) <= 0.25 OR iv_hv <= 0.90 THEN 'low_iv'
                       WHEN COALESCE(iv_pr, 0.5) >= 0.75 OR iv_hv >= 1.35 THEN 'high_iv'
                       ELSE 'mid_iv'
                   END AS bucket
            FROM cur
            WHERE COALESCE(hist_n, 0) >= 10
              AND (
                COALESCE(iv_pr, 0.5) <= 0.25 OR iv_hv <= 0.90
                OR COALESCE(iv_pr, 0.5) >= 0.75 OR iv_hv >= 1.35
              )
            ORDER BY
              CASE WHEN bucket = 'low_iv' THEN 0 ELSE 1 END,
              CASE WHEN bucket = 'low_iv' THEN COALESCE(iv_pr, 0.5) END ASC NULLS LAST,
              CASE WHEN bucket = 'low_iv' THEN iv_hv END ASC NULLS LAST,
              CASE WHEN bucket = 'high_iv' THEN COALESCE(iv_pr, 0.5) END DESC NULLS LAST,
              CASE WHEN bucket = 'high_iv' THEN iv_hv END DESC NULLS LAST
            LIMIT 40
            """
        ).fetchall()
    except duckdb.Error as exc:
        ivhv_rows = []
        out_parts.append(f"[IV/HV query failed: {exc}]")

    def _pct(value: Any) -> str:
        return f"{float(value) * 100:.1f}%" if value is not None else "—"

    def _num(value: Any, digits: int = 2) -> str:
        return f"{float(value):.{digits}f}" if value is not None else "—"

    out_parts.append("")
    out_parts.append("## IV/HV regime (current implied vol vs realized vol, equities only)")
    out_parts.append("Low IV = IV rank ≤25% or IV/HV ≤0.90; High IV = IV rank ≥75% or IV/HV ≥1.35. Use as stock timing/risk context only.")
    out_parts.append("")
    low_rows = [r for r in ivhv_rows if r[10] == "low_iv"][:10]
    high_rows = [r for r in ivhv_rows if r[10] == "high_iv"][:10]
    out_parts.append("### Low IV / low IV-HV candidates")
    out_parts.append("| symbol | IV | HV | IV/HV | IV rank | hist_n | VRP | pc_z | skew_z | context |")
    out_parts.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    if not low_rows:
        out_parts.append("| — | — | — | — | — | — | — | — | — | no low-IV candidates |")
    for r in low_rows:
        sym, iv, rv, ivhv, vrp, vrpz, iv_pr, hist_n, pcz, skz, bucket = r
        context = "direction cost low; stock signal can use cheaper optionality context, not an option trade"
        out_parts.append(
            f"| {sym} | {_pct(iv)} | {_pct(rv)} | {_num(ivhv)} | {_pct(iv_pr)} | "
            f"{int(hist_n or 0)} | {_num(vrp, 3)} | {_num(pcz)} | {_num(skz)} | {context} |"
        )
    out_parts.append("")
    out_parts.append("### High IV / high IV-HV candidates")
    out_parts.append("| symbol | IV | HV | IV/HV | IV rank | hist_n | VRP | pc_z | skew_z | context |")
    out_parts.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    if not high_rows:
        out_parts.append("| — | — | — | — | — | — | — | — | — | no high-IV candidates |")
    for r in high_rows:
        sym, iv, rv, ivhv, vrp, vrpz, iv_pr, hist_n, pcz, skz, bucket = r
        context = "event premium/crowding high; prefer stock linear risk control, avoid treating vol as cheap"
        out_parts.append(
            f"| {sym} | {_pct(iv)} | {_pct(rv)} | {_num(ivhv)} | {_pct(iv_pr)} | "
            f"{int(hist_n or 0)} | {_num(vrp, 3)} | {_num(pcz)} | {_num(skz)} | {context} |"
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

    # Index-level skew term structure (^SPX/^NDX/SPY/QQQ/^XSP/^VIX), already
    # computed into the main payload by generate_main_strategy_v2_report.py.
    # Surfaces market-wide tail-risk pricing for the narrator's index read.
    idx_skew = (art.get("main_strategy_v2_backtest") or {}).get("index_skew") or {}
    if idx_skew:
        out_parts.append("")
        out_parts.append("## index-level skew term structure (front vs ~30d, OTM-put/call IV)")
        for sym, rec in idx_skew.items():
            fs, fd = rec.get("front_skew"), rec.get("front_dte")
            ts, td = rec.get("term_skew"), rec.get("term_dte")
            slope = rec.get("skew_slope")
            skz = rec.get("skew_z")
            if fs is None:
                continue
            line = (f"{sym}: front {fs:.2f}@{fd}d → ~30d {ts:.2f}@{td}d "
                    f"(slope {slope:+.2f})")
            if skz is not None:
                line += f", skew_z {skz:+.2f}"
            out_parts.append(line)

    try:
        gamma_date = date.fromisoformat(as_of)
    except ValueError:
        gamma_date = date.today()
    gamma_symbols = _gamma_spring_focus_symbols(art)
    gamma_snapshot = build_gamma_spring_snapshot(US_DB, gamma_symbols, gamma_date)
    gamma_lines = render_gamma_spring_section({"gamma_spring": gamma_snapshot}, limit=12)
    if gamma_lines:
        out_parts.append("")
        out_parts.append("\n".join(gamma_lines))

    return _join_payload_sections("OPTIONS PAYLOAD", "\n".join(out_parts))


def _gamma_spring_focus_symbols(art: dict[str, Any]) -> list[str]:
    payload = art.get("main_strategy_v2_backtest") or {}
    out = {"SPY", "QQQ", "SMH", "NVDA", "AMD", "MSFT", "AAPL", "TSLA"}

    def add(sym: Any) -> None:
        token = str(sym or "").upper().strip()
        if token:
            out.add(token)

    for row in ((payload.get("production_decision_summary") or {}).get("actionable") or []):
        if (row.get("market") or row.get("region") or "").lower() in {"us", "usa", ""}:
            add(row.get("symbol"))
    us_ranker = payload.get("us_opportunity_ranker") or {}
    for row in (us_ranker.get("production_basket") or [])[:25]:
        add(row.get("symbol"))
    for row in (us_ranker.get("all_rows") or [])[:40]:
        add(row.get("symbol"))
    for row in ((payload.get("us") or {}).get("current") or [])[:40]:
        add(row.get("symbol"))
    return sorted(out)


def build_risk_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "US Production Gate",
        "US 期权异常",
        "Congressional Trading",
        "政策资金流",
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


def _brief_value(value: Any, default: str = "-") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _brief_r(value: Any) -> str:
    try:
        return f"{float(value):.4g}R"
    except (TypeError, ValueError):
        return "-R"


def _brief_pct(value: Any) -> str:
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return "-"


def _plain_regime_label(state: str, multiplier: Any) -> str:
    state_l = (state or "").lower()
    mult = _brief_value(multiplier)
    if state_l == "wedge":
        return f"趋势仍强,但利率/波动约束还在,新加仓需要减码({mult}x)"
    if state_l == "confirm":
        return f"趋势和风险偏好相互确认,允许按规则加仓({mult}x)"
    if state_l == "press":
        return f"趋势强且回撤风险低,可以更积极执行({mult}x)"
    if state_l == "hedge":
        return f"风险偏防守,先控仓位和 beta({mult}x)"
    return f"{state or '未知状态'}({mult}x)"


def build_strategy_story_brief(art: dict[str, Any], as_of: str) -> str:
    """Trading-story brief that anchors the final narrator.

    This is not a fallback report. It is a small, deterministic bridge from
    model outputs to a coherent strategy narrative: market backdrop, execution
    constraint, candidate tension, and risk evidence. The Codex narrator still
    writes the final report.
    """
    payload = art.get("main_strategy_v2_backtest")
    if not isinstance(payload, dict):
        return "No payload available; preserve report structure and state data gaps clearly."
    decision = payload.get("production_decision_summary") or {}
    summary = decision.get("summary") or {}
    gate = summary.get("us_execution_gate") or {}
    status = payload.get("us_market_data_status") or {}
    regime = payload.get("risk_regime") or {}
    mrs = payload.get("market_regime_score") or {}
    ranker = payload.get("us_opportunity_ranker") or {}
    actions = [
        row for row in (decision.get("actionable") or [])
        if str(row.get("market") or "").upper() == "US"
    ]

    gate_allowed = bool(gate.get("allowed"))
    us_r = summary.get("us_r")
    if actions:
        action_line = f"最终执行表给出 {len(actions)} 个美股执行仓位,合计 {_brief_r(us_r)}"
    elif gate_allowed:
        action_line = f"美股门禁允许,但最终执行表没有给出仓位,美股合计 {_brief_r(us_r)}"
    else:
        action_line = f"美股最终执行为 {_brief_r(us_r)},也就是不新增美股仓位风险"

    blocker = (
        gate.get("top_blocker")
        or gate.get("top_warning")
        or summary.get("top_blocker")
        or "IV/HV + Gamma Spring v3 + risk regime 控制执行仓位"
    )
    regime_text = _plain_regime_label(str(regime.get("state") or ""), regime.get("r_multiplier"))
    mrs_line = ""
    if mrs.get("mrs") is not None:
        mrs_line = (
            f"市场风险偏好分数(MRS)={_brief_value(mrs.get('mrs'))}, "
            f"{mrs.get('mrs_bucket') or '-'}, {mrs.get('bucket_history') or '-'}。"
        )

    data_line = (
        f"报告标签 {as_of},美股收盘价/候选数据截至 "
        f"{status.get('effective_us_market_date') or status.get('prices_daily_latest_date') or '-'},"
        f"期权/Gamma 有效日 {status.get('options_chain_latest_as_of') or '-'}。"
    )
    if status.get("is_previous_session"):
        data_line += "这不是当日美股已收盘结果,只能作为下一次开盘前评估。"

    policy_flow = payload.get("congressional_trading") or {}
    policy_summary = policy_flow.get("summary") or {}
    policy_rows = policy_flow.get("rows") or []
    if policy_rows:
        top_policy = policy_rows[0]
        policy_line = (
            f"政策资金流有 {policy_summary.get('symbols', len(policy_rows))} 个 ticker 线索: "
            f"{top_policy.get('symbol')}={top_policy.get('state')}, "
            f"{top_policy.get('report_role')}。它只影响催化/风险复核,不能证明 AI 关系或直接生成 R。"
        )
    else:
        policy_line = "政策资金流没有可核验 artifact；不要引用单条社媒披露或传闻。"

    ranked_rows = ranker.get("all_rows") or []
    top_watch: list[str] = []
    for row in ranked_rows[:8]:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        ret_5d = _brief_pct(row.get("ret_5d_pct"))
        gamma_state = str(row.get("gamma_v3_flip_regime") or row.get("gamma_v2_management_signal") or "-")
        opt = str(row.get("options_quality_reason") or "-")
        if not gate_allowed:
            verdict = "观察"
        elif "trade" in str(row.get("production_tier") or "").lower():
            verdict = "执行候选"
        else:
            verdict = "观察"
        top_watch.append(f"- {symbol}: {verdict}; 5日 {ret_5d}; Gamma={gamma_state}; options={opt[:80]}")
    watch_block = "\n".join(top_watch) if top_watch else "- None: 没有可展示观察候选"

    return f"""
## 策略叙事底稿（最高优先级）

这份底稿只规定报告的因果链,不要照抄成问答。最终报告要像交易员晨会 memo,围绕“市场故事 -> 仓位取舍 -> 反证风险 -> 复核触发”展开。

### 主线冲突
美股 AI-infra tape 仍强,但最终仓位选择是: {action_line}。关键约束是: {blocker}。市场环境可以写成: {regime_text}。{mrs_line or ''}

### 数据口径
{data_line}

### 政策资金流
{policy_line}

### 观察名单的叙事位置
如果美股最终执行为 0R,下面这些高分票只能作为故事里的“候选张力”: 它们解释为什么市场有机会,但不能被写成仓位。

{watch_block}

### 写作约束
- 不要写模板问答。
- 不要写二元问答。
- 量化、风控、新闻、期权/Gamma 都要服务于同一条交易故事,不能各写各的。
- 技术词只作为证据,不要让 WEDGE/MRS/Gamma/R 乘数抢在故事前面。
""".strip()


# Style-guard enforcement (mirrors final_style_guard): emoji/decoration ban and
# internal-field-name ban. Repair loop consumes these errors automatically.
_EMOJI_RE = re.compile(r"[\U0001F000-\U0001FAFF☀-➿⬀-⯿️]")
_SYMBOL_CELL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def _is_table_separator(line: str) -> bool:
    body = line.strip().strip("|").replace("|", "").strip()
    return bool(body) and set(body) <= {"-", ":", " "}


def _table_separator_mismatches(text: str) -> list[str]:
    """Header rows whose separator row has a different column count."""
    lines = text.splitlines()
    bad: list[str] = []
    for idx in range(len(lines) - 1):
        cur, nxt = lines[idx].strip(), lines[idx + 1].strip()
        if (cur.startswith("|") and cur.endswith("|") and nxt.startswith("|")
                and _is_table_separator(nxt) and cur.count("|") != nxt.count("|")):
            bad.append(cur[:50])
    return bad


def _executed_symbols(text: str) -> set[str]:
    """First-column tickers of the 交易计划 production table."""
    in_plan = False
    out: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            in_plan = stripped == "## 交易计划"
            continue
        if not in_plan or not stripped.startswith("|") or _is_table_separator(stripped):
            continue
        first = stripped.strip("|").split("|")[0].strip().strip("*` ")
        if first in {"Symbol", "SYMBOL", "None"}:
            continue
        if _SYMBOL_CELL_RE.match(first):
            out.add(first)
    return out
_BANNED_INTERNAL_TOKENS = [
    "提取器", "payload", "digest", "merge-agent", "user_msg", "system prompt",
    "stable_alpha_gate", "ev_status", "production_decision_summary",
    "execution_blocked_0r", "active_watch", "ranked_watch",
]


def _markdown_table_count(text: str) -> int:
    lines = text.splitlines()
    count = 0
    for idx, line in enumerate(lines[:-1]):
        cur = line.strip()
        nxt = lines[idx + 1].strip()
        if cur.startswith("|") and cur.endswith("|") and nxt.startswith("|") and set(nxt.replace("|", "").strip()) <= {"-", ":"}:
            count += 1
    return count


def validate_structured_us_report(text: str, as_of: str, payload: dict[str, Any] | None = None) -> None:
    required = [
        f"# 美股量化日报 — {as_of}",
        "## 策略主线",
        "## 市场结构",
        "## 交易计划",
        "## 风险与反证",
        "## 催化与复核",
        "## 附注",
    ]
    missing = [marker for marker in required if marker not in text]
    if missing:
        raise RuntimeError(f"US narrator output missing required sections: {missing}")
    table_count = _markdown_table_count(text)
    if table_count < 4:
        raise RuntimeError(f"US narrator output has only {table_count} Markdown tables; expected >=4")
    for marker in ["IV/HV", "Gamma", "US Realized Horizon Edge", "Congressional"]:
        if marker not in text:
            raise RuntimeError(f"US narrator output missing required marker: {marker}")
    if not any(marker in text for marker in ["Production", "正式执行", "可执行做多"]):
        raise RuntimeError("US narrator output missing execution marker: Production/正式执行")
    allowed_h2 = {
        "## 策略主线", "## 市场结构", "## 交易计划",
        "## 风险与反证", "## 催化与复核", "## 附注",
    }
    extra_h2 = [ln.strip() for ln in text.splitlines()
                if ln.strip().startswith("## ") and ln.strip() not in allowed_h2]
    if extra_h2:
        raise RuntimeError(f"US narrator output has unexpected H2 sections: {extra_h2[:5]}")
    emoji_hits = _EMOJI_RE.findall(text)
    if emoji_hits:
        raise RuntimeError(
            f"US narrator output contains emoji/decoration: {sorted(set(emoji_hits))[:8]}")
    internal_hits = [token for token in _BANNED_INTERNAL_TOKENS if token in text]
    lowered = text.lower()
    internal_hits += [token for token in ("gpt-5.5", "deepseek") if token in lowered]
    if internal_hits:
        raise RuntimeError(f"US narrator output leaks internal field names: {internal_hits}")
    bad_separators = _table_separator_mismatches(text)
    if bad_separators:
        raise RuntimeError(
            f"US narrator output has table separator column mismatches: {bad_separators[:3]}")
    executed = _executed_symbols(text)
    if executed:
        for block in text.split("\n\n"):
            if ("仅观察" in block or "只观察" in block) and "不执行" in block:
                offenders = sorted(
                    sym for sym in executed
                    if re.search(rf"(?<![A-Z0-9]){re.escape(sym)}(?![A-Z0-9])", block))
                if offenders:
                    raise RuntimeError(
                        "US narrator output lists executed symbols under 仅观察/不执行 wording: "
                        f"{offenders} — executed positions must read 持有不加码, not 不执行")
    if payload:
        failures = validate_us_report_text_against_payload(payload, text, "us_narrator_output")
        if failures:
            details = "; ".join(f"{failure.code}: {failure.detail}" for failure in failures)
            raise RuntimeError(f"US narrator output violates data-lineage contract: {details}")


def _first_table(section: str, *, max_rows: int = 8) -> str:
    lines = section.splitlines()
    for idx, line in enumerate(lines):
        if not line.strip().startswith("|"):
            continue
        table: list[str] = []
        for raw in lines[idx:]:
            if not raw.strip().startswith("|"):
                break
            table.append(raw)
        if len(table) < 2:
            continue
        header = table[:2]
        rows = table[2 : 2 + max_rows]
        return "\n".join(header + rows)
    return ""


def _compact_section(section: str, *, max_lines: int = 16) -> str:
    out: list[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        out.append(line)
        if len(out) >= max_lines:
            break
    return "\n".join(out)


def build_layout_skeleton(art: dict[str, Any], as_of: str) -> str:
    """Build a compact structured draft from the programmatic report.

    The narrator still writes the final report, but this skeleton prevents the
    final merge from flattening good tables into prose.
    """
    md = art.get("_us_daily_report_md", "")
    market = "\n\n".join(
        part
        for part in [
            _compact_section(_slice_md_sections(md, ["风控引擎"]), max_lines=10),
            _compact_section(_slice_md_sections(md, ["恐惧贪婪"]), max_lines=10),
            _compact_section(_slice_md_sections(md, ["SPX × P/C"]), max_lines=10),
            _compact_section(_slice_md_sections(md, ["US Realized Horizon Edge"]), max_lines=12),
        ]
        if part
    )
    prod_table = _first_table(_slice_md_sections(md, ["可交易名单"]), max_rows=10)
    if not prod_table:
        prod_table = "| Symbol | Decision | Size | Entry | Risk | Hedge | Why |\n|---|---|---:|---|---|---|---|\n| None | - | - | - | - | - | - |"
    watch_table = _first_table(_slice_md_sections(md, ["远月 OTM"]), max_rows=6)
    if not watch_table:
        watch_table = _first_table(_slice_md_sections(md, ["US 左侧观察池"]), max_rows=6)
    if not watch_table:
        watch_table = "| Symbol | Status | Reason | Next check |\n|---|---|---|---|\n| None | - | - | - |"
    iv_table = _first_table(_slice_md_sections(md, ["US 期权 IV 视图"]), max_rows=8)
    if not iv_table:
        iv_table = "| Symbol | IV/HV | IV rank | Context | Action |\n|---|---:|---:|---|---|\n| None | - | - | - | - |"
    gamma_table = _first_table(_slice_md_sections(md, ["US Gamma Spring"]), max_rows=10)
    if not gamma_table:
        gamma_table = "| Symbol | Gamma state | Dealer proxy | Wall | Management |\n|---|---|---:|---|---|\n| None | - | - | - | - |"
    congressional_table = _first_table(_slice_md_sections(md, ["Congressional Trading", "政策资金流"]), max_rows=8)
    if not congressional_table:
        congressional_table = "| Symbol | Signal | Lawmakers / committee | Disclosure lag | Read-through | Report role |\n|---|---|---|---:|---|---|\n| None | NO_CONGRESSIONAL_TRADING_DATA | - | - | no verified artifact | context_only |"
    catalyst = "\n\n".join(
        part
        for part in [
            _compact_section(_slice_md_sections(md, ["Congressional Trading", "政策资金流"]), max_lines=14),
            _compact_section(_slice_md_sections(md, ["财报日历", "美股财报"]), max_lines=14),
            _compact_section(_slice_md_sections(md, ["Source Review", "source-review"]), max_lines=14),
        ]
        if part
    )
    return f"""
# 美股量化日报 — {as_of}

## 策略主线
写 2-4 个自然段,讲清楚市场故事、仓位取舍、反证风险。不要写模板问答,不要用未解释的 WEDGE、MRS、0R、Gamma 开头。

## 市场结构
先给 2-3 句 regime/tape/资金风险的因果链,再保留数据校准和关键市场证据。

{market or "| Metric | Value |\n|---|---|\n| Market | no market skeleton |"}

## 交易计划
必须先给 Production candidates 表。优先从下表改写成列: Symbol / Decision / Size / Entry / Risk / Hedge / Why。

{prod_table}

## 风险与反证
必须保留下面三类表。表前先用一段话说明这些证据如何支持或反驳交易故事,不要写成自动买入清单。

### Watch / 0R context
{watch_table}

### IV/HV
{iv_table}

### Gamma v3
{gamma_table}

### Congressional trading
{congressional_table}

## 催化与复核
必须给 Catalyst / review 表；若下面素材不是表格，整理成表格。政策资金流只写成催化/风险复核,不能写成 AI source evidence 或执行 R 来源。

{catalyst or "| Item | Date | Impact | Review |\n|---|---|---|---|\n| None | - | - | - |"}

## 附注
options / news 仅作为股票决策证据,不是这份报告的交易标的。不构成投资建议。
""".strip()


async def call_extractor_async(
    sem: asyncio.Semaphore, name: str, payload_text: str
) -> tuple[str, str]:
    """Async wrapper routing to the configured backend (codex by default)."""
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
    async with sem:
        response = await loop.run_in_executor(
            None,
            lambda: _call_llm_with_retries(
                system_msg,
                user_msg,
                label=f"extractor:{name}",
                temperature=0.1,
                max_tokens=1200,
                attempts=_env_int("US_NARRATOR_EXTRACTOR_RETRIES", 2),
            ),
        )
    if not response:
        raise RuntimeError(f"US {name} extractor returned empty Codex output")
    return name, response


async def run_extractors(art: dict[str, Any], as_of: str) -> dict[str, str]:
    """Run 6 extractors in parallel: macro / event / quant / risk / news / options."""
    payloads = {
        "macro": build_macro_payload(art),
        "event": build_event_payload(art),
        "quant": build_quant_payload(art),
        "risk": build_risk_payload(art),
        "news": build_news_payload(art, as_of),
        "options": build_options_payload(art, as_of),
    }
    sem = asyncio.Semaphore(concurrency())
    async def _run_one(name: str, payload: str) -> tuple[str, str]:
        try:
            return await call_extractor_async(sem, name, payload)
        except Exception as exc:  # noqa: BLE001 - one extractor should not kill the narrator.
            message = f"[{name} Codex extractor failed after retries: {exc}]"
            print(f"    {message}")
            return name, message

    tasks = [_run_one(name, payload) for name, payload in payloads.items()]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return dict(results)


def call_narrator(extractor_outputs: dict[str, str],
                  art: dict[str, Any], as_of: str) -> str | None:
    """Single narrator call — receives extractor outputs + payload digest."""
    prompt = load_prompt("merge") + "\n\n" + final_style_guard(as_of)
    layout_skeleton = build_layout_skeleton(art, as_of)
    story_brief = build_strategy_story_brief(art, as_of)
    payload = art.get("main_strategy_v2_backtest")
    payload = payload if isinstance(payload, dict) else None
    payload_digest = art.get("_us_daily_report_md", "")[:30000]  # cap to avoid token blow-up
    user_msg = (
        f"### 策略叙事底稿（最高优先级）\n{story_brief}\n\n"
        f"### 宏观提取\n{extractor_outputs.get('macro', '[missing]')}\n\n"
        f"### 事件提取\n{extractor_outputs.get('event', '[missing]')}\n\n"
        f"### 量化提取\n{extractor_outputs.get('quant', '[missing]')}\n\n"
        f"### 风险提取\n{extractor_outputs.get('risk', '[missing]')}\n\n"
        f"### 新闻提取\n{extractor_outputs.get('news', '[missing]')}\n\n"
        f"### 期权提取(短端 hedging + 综合定向 + sentiment 极端)\n{extractor_outputs.get('options', '[missing]')}\n\n"
        f"### 版式骨架（必须保留表格结构；你可以改写文字和裁决,但不能删表）\n{layout_skeleton}\n\n"
        f"### Payload Digest(交叉验证用)\n{payload_digest}\n\n"
        f"### 任务\n请按最终写作覆盖重写为日期 {as_of} 的完整美股日报。"
    )
    narrative = _call_llm_with_retries(
        prompt,
        user_msg,
        label="narrator:us",
        temperature=0.2,
        max_tokens=4500,
        attempts=_env_int("US_NARRATOR_RETRIES", 2),
    )
    if not narrative:
        return None
    try:
        validate_structured_us_report(narrative, as_of, payload)
        return narrative
    except RuntimeError as exc:
        repair_user_msg = (
            f"上一版美股日报结构不合格: {exc}\n\n"
            "请只修复结构和版式,保留事实,必须输出完整报告。"
            "不要解释错误,不要输出检查过程。\n\n"
            f"### 不合格上一版\n{narrative}\n\n"
            f"### 必须保留的版式骨架\n{layout_skeleton}\n\n"
            f"### 日期\n{as_of}"
        )
        # 3 rounds: the stricter style rules (H2 whitelist / emoji / internal
        # tokens) consume repair budget before lineage fixes land on DeepSeek.
        repair_attempts = _env_int("US_NARRATOR_REPAIR_RETRIES", 3)
        last_error = str(exc)
        for idx in range(1, repair_attempts + 1):
            repaired = _call_llm_with_retries(
                prompt,
                repair_user_msg,
                label=f"narrator:us:repair{idx}",
                temperature=0.1,
                max_tokens=4500,
                attempts=1,
            )
            if not repaired:
                continue
            try:
                validate_structured_us_report(repaired, as_of, payload)
                return repaired
            except RuntimeError as repair_exc:
                last_error = str(repair_exc)
                repair_user_msg = (
                    f"上一版美股日报结构仍不合格: {repair_exc}\n\n"
                    "继续只修复结构和版式,保留事实,必须输出完整报告。"
                    "不要解释错误,不要输出检查过程。\n\n"
                    f"### 不合格上一版\n{repaired}\n\n"
                    f"### 必须保留的版式骨架\n{layout_skeleton}\n\n"
                    f"### 日期\n{as_of}"
                )
        raise RuntimeError(f"US Codex narrator failed structured validation after repair: {last_error}")


async def main_async(args) -> None:
    as_of = args.date
    report_dir = ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of
    if not report_dir.exists():
        raise SystemExit(f"report dir missing: {report_dir}")

    art = load_payload_artifacts(report_dir)
    print(f"=== US narrator agent — {as_of} (backend={backend()}, concurrency={concurrency()}) ===")
    print(f"  loaded {len(art)} artifacts; md size {len(art.get('_us_daily_report_md', ''))}")

    print("  running 6 Codex extractors in parallel (macro/event/quant/risk/news/options)...")
    extractor_outputs = await run_extractors(art, as_of)
    for name, out in extractor_outputs.items():
        print(f"    {name}: {len(out)} chars")
        if args.dump_extractors:
            (report_dir / f"_extractor_{name}.md").write_text(out, encoding="utf-8")

    print("  calling narrator...")
    narrative = call_narrator(extractor_outputs, art, as_of)
    if not narrative:
        raise SystemExit("US Codex narrator failed; not writing output")

    out_name = "us_daily_report.md" if args.overwrite else "us_daily_report_agent.md"
    out_path = write_agent_report(report_dir, out_name, narrative, as_of)
    print(f"  wrote {out_path} ({len(narrative)} chars)")
    if args.overwrite:
        agent_path = write_agent_report(report_dir, "us_daily_report_agent.md", narrative, as_of)
        print(f"  synced {agent_path} ({len(narrative)} chars)")


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
