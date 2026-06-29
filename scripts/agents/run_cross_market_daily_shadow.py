#!/usr/bin/env python3
"""Build shadow cross-market daily reports from frozen Main Strategy V2 artifacts.

This is the first low-risk step toward the agent-native report shape:

* AM report: US post-market context -> CN pre-market plan.
* PM report: CN post-market feedback + US pre-market context.

The causality is intentionally asymmetric: US market structure can guide the
next CN session, while CN post-market action is treated as feedback/context and
must not drive US positioning.

The script is read-only with respect to data/compute artifacts. By default it
does not call an LLM and does not send email; it writes a deterministic shadow
report, a fact packet, and a small trajectory log. Use ``--agent-backend`` later
to let the lead editor agent rewrite the same packet.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_ROOT = ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
STYLE_REFERENCE_URL = (
    "https://boist.org/2026/06/22/"
    "2026%e5%b9%b46%e6%9c%8821%e6%97%a5%ef%bc%9a%e9%9f%a9%e6%97%a5"
    "%e8%82%a1%e5%b8%82%e5%86%8d%e5%88%9b%e7%ba%aa%e5%bd%95%ef%bc%8c"
    "%e6%9d%a0%e6%9d%86etf%e5%a6%82%e6%97%a5%e4%b8%ad%e5%a4%a9%ef%bc%9btokenmaxxi/"
)
GLOBAL_MARKET_SNAPSHOT_SYMBOLS = (
    "^GSPC,^IXIC,^DJI,^RUT,^VIX,SPY,QQQ,IWM,TLT,"
    "ES=F,NQ=F,YM=F,RTY=F,"
    "^STOXX50E,^FTSE,^GDAXI,^FCHI,"
    "^N225,^KS11,^HSI,^TWII,"
    "000001.SS,399001.SZ,399006.SZ,000688.SS,"
    "GC=F,CL=F,BZ=F,GLD,USDCNH=X,DX-Y.NYB,^TNX"
)
GLOBAL_MARKET_LABELS = {
    "^GSPC": "标普500",
    "^IXIC": "纳斯达克综合",
    "^DJI": "道琼斯",
    "^RUT": "罗素2000",
    "^VIX": "VIX波动率",
    "ES=F": "标普期货",
    "NQ=F": "纳指期货",
    "YM=F": "道指期货",
    "RTY=F": "罗素期货",
    "^STOXX50E": "欧洲STOXX50",
    "^FTSE": "英国FTSE100",
    "^GDAXI": "德国DAX",
    "^FCHI": "法国CAC40",
    "^N225": "日本日经225",
    "^KS11": "韩国KOSPI",
    "^HSI": "香港恒生",
    "^TWII": "台湾加权",
    "000001.SS": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000688.SS": "科创50",
    "GC=F": "黄金期货",
    "CL=F": "WTI原油期货",
    "BZ=F": "布伦特原油期货",
    "GLD": "黄金ETF",
    "USDCNH=X": "美元/离岸人民币",
    "DX-Y.NYB": "美元指数",
    "^TNX": "美国10年利率",
}
GLOBAL_MARKET_SNAPSHOT_GROUPS = (
    ("美股现货", ("^GSPC", "^IXIC", "^DJI", "^RUT", "^VIX")),
    ("美股期货", ("ES=F", "NQ=F", "YM=F", "RTY=F")),
    ("欧洲大盘", ("^STOXX50E", "^GDAXI", "^FTSE", "^FCHI")),
    ("亚洲大盘", ("^N225", "^KS11", "^HSI", "^TWII")),
    ("A股大盘", ("000001.SS", "399001.SZ", "399006.SZ", "000688.SS")),
    ("商品/汇率", ("CL=F", "BZ=F", "GC=F", "GLD", "USDCNH=X", "DX-Y.NYB", "^TNX")),
)
FORBIDDEN_PUBLIC_INDEX_MARKERS = ("Sensex", "Nifty", "印度指数", "印度Sensex", "印度")
GLOBAL_MARKET_LABEL_ALIASES = {
    "^GSPC": ("标普500",),
    "^IXIC": ("纳斯达克综合",),
    "^DJI": ("道琼斯",),
    "^RUT": ("罗素2000",),
    "^VIX": ("VIX",),
    "ES=F": ("标普期货",),
    "NQ=F": ("纳指期货",),
    "YM=F": ("道指期货",),
    "RTY=F": ("罗素期货",),
    "^STOXX50E": ("欧洲STOXX50", "STOXX50"),
    "^FTSE": ("英国FTSE100", "FTSE100"),
    "^GDAXI": ("德国DAX", "DAX"),
    "^FCHI": ("法国CAC40", "CAC40"),
    "^N225": ("日本日经225", "日经225"),
    "^KS11": ("韩国KOSPI", "KOSPI"),
    "^HSI": ("香港恒生", "恒生"),
    "^TWII": ("台湾加权",),
    "000001.SS": ("上证指数",),
    "399001.SZ": ("深证成指",),
    "399006.SZ": ("创业板指",),
    "000688.SS": ("科创50",),
}


@dataclass(frozen=True)
class MarketArtifact:
    market: str
    report_date: str
    report_dir: Path
    payload: dict[str, Any]
    markdown: str
    markdown_path: Path | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a cross-market daily shadow report.")
    parser.add_argument(
        "--slot",
        required=True,
        choices=["am", "pm"],
        help="am=US post -> CN pre; pm=CN post feedback + US pre context.",
    )
    parser.add_argument("--cn-date", default=None, help="CN report date. Defaults to today in Asia/Shanghai.")
    parser.add_argument("--us-date", default=None, help="US report date. Defaults from --slot and --cn-date.")
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None, help="Defaults to report-root/<cn-date>.")
    parser.add_argument(
        "--agent-backend",
        choices=["off", "auto", "hermes"],
        default="off",
        help="off writes deterministic shadow text; auto calls codex_backend.call_llm; hermes calls Hermes chat.",
    )
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--hermes-bin", default=os.environ.get("HERMES_BIN", "hermes"))
    parser.add_argument("--hermes-model", default=os.environ.get("HERMES_INFERENCE_MODEL", ""))
    parser.add_argument("--hermes-provider", default=os.environ.get("HERMES_PROVIDER", ""))
    parser.add_argument("--hermes-max-turns", type=int, default=int(os.environ.get("HERMES_MAX_TURNS", "16")))
    parser.add_argument(
        "--review-model",
        default=os.environ.get("CROSS_MARKET_REVIEW_MODEL", os.environ.get("HERMES_REVIEW_MODEL", "")),
        help="Optional model for the editor/reviewer pass; defaults to the writer model when unset.",
    )
    parser.add_argument(
        "--review-provider",
        default=os.environ.get("CROSS_MARKET_REVIEW_PROVIDER", os.environ.get("HERMES_REVIEW_PROVIDER", "")),
        help="Optional provider for the editor/reviewer pass; defaults to the writer provider when unset.",
    )
    parser.add_argument(
        "--review-max-turns",
        type=int,
        default=int(os.environ.get("CROSS_MARKET_REVIEW_MAX_TURNS", "6")),
        help="Max Hermes turns for the editor/reviewer pass.",
    )
    parser.add_argument(
        "--review-backend",
        choices=["off", "hermes"],
        default=os.environ.get("CROSS_MARKET_REVIEW_BACKEND", "hermes"),
        help="Optional second-pass editor/reviewer. Required for emailed Hermes reports.",
    )
    parser.add_argument(
        "--fallback-backend",
        choices=["none", "auto", "off"],
        default=os.environ.get("CROSS_MARKET_AGENT_FALLBACK", "auto"),
        help="Fallback when --agent-backend hermes fails: auto=legacy LLM packet writer, off=deterministic.",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        default=os.environ.get("CROSS_MARKET_SEND_EMAIL", "").lower() in {"1", "true", "yes"},
        help="Send the generated cross-market report after writing it.",
    )
    parser.add_argument(
        "--email-provider",
        choices=["gmail", "resend"],
        default=os.environ.get("QUANT_EMAIL_PROVIDER", "gmail"),
    )
    parser.add_argument(
        "--email-fallback-provider",
        choices=["none", "gmail"],
        default=os.environ.get("QUANT_EMAIL_FALLBACK_PROVIDER", "none"),
        help="Fallback provider used only when the primary live send fails.",
    )
    parser.add_argument(
        "--delivery-mode",
        choices=["test", "prod"],
        default=os.environ.get("QUANT_DELIVERY_MODE", "test"),
    )
    parser.add_argument("--test-recipient", default=os.environ.get("QUANT_TEST_RECIPIENT"))
    parser.add_argument("--delivery-dry-run", action="store_true")
    parser.add_argument(
        "--allow-duplicate-email",
        action="store_true",
        default=os.environ.get("QUANT_ALLOW_DUPLICATE_EMAIL", "").lower() in {"1", "true", "yes"},
        help="Bypass the slot/date/recipient delivery ledger. Use only for deliberate manual resends.",
    )
    parser.add_argument(
        "--finance-search-prefetch",
        choices=["on", "off"],
        default=os.environ.get("CROSS_MARKET_FINANCE_SEARCH_PREFETCH", "on"),
        help="Best-effort direct finance-search prefetch for global markets/news before Hermes writes.",
    )
    return parser.parse_args()


def parse_ymd(value: str) -> date:
    return date.fromisoformat(value[:10])


def is_session(day: date, calendar_name: str) -> bool:
    try:
        import exchange_calendars as xcals

        return bool(xcals.get_calendar(calendar_name).is_session(day.isoformat()))
    except Exception:
        return day.weekday() < 5


def previous_session(day: date, calendar_name: str) -> date:
    cur = day - timedelta(days=1)
    for _ in range(14):
        if is_session(cur, calendar_name):
            return cur
        cur -= timedelta(days=1)
    raise RuntimeError(f"unable to resolve previous {calendar_name} session before {day}")


def session_on_or_before(day: date, calendar_name: str) -> date:
    cur = day
    for _ in range(14):
        if is_session(cur, calendar_name):
            return cur
        cur -= timedelta(days=1)
    raise RuntimeError(f"unable to resolve {calendar_name} session on or before {day}")


def resolve_dates(slot: str, cn_date_raw: str | None, us_date_raw: str | None) -> tuple[str, str]:
    cn_day = parse_ymd(cn_date_raw) if cn_date_raw else datetime.now(LOCAL_TZ).date()
    if us_date_raw:
        us_day = parse_ymd(us_date_raw)
    elif slot == "am":
        us_day = previous_session(cn_day, "XNYS")
    else:
        us_day = session_on_or_before(cn_day, "XNYS")
    return cn_day.isoformat(), us_day.isoformat()


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def load_first_text(paths: list[Path]) -> tuple[str, Path | None]:
    for path in paths:
        if path.exists():
            return path.read_text(encoding="utf-8"), path
    return "", None


def load_market_artifact(report_root: Path, market: str, report_date: str) -> MarketArtifact:
    report_dir = report_root / report_date
    payload = load_json(report_dir / "main_strategy_v2_backtest.json")
    if market == "us":
        candidates = [
            report_dir / "us_daily_report_agent.md",
            report_dir / "us_daily_report.md",
        ]
    else:
        candidates = [
            report_dir / "cn_daily_report_agent.md",
            report_dir / "cn_daily_report.md",
        ]
    markdown, markdown_path = load_first_text(candidates)
    return MarketArtifact(
        market=market,
        report_date=report_date,
        report_dir=report_dir,
        payload=payload,
        markdown=markdown,
        markdown_path=markdown_path,
    )


def artifact_ready(artifact: MarketArtifact) -> bool:
    return bool(artifact.payload and artifact.markdown)


def load_cn_context_artifact(report_root: Path, slot: str, target_cn_date: str) -> tuple[MarketArtifact, str | None]:
    cn = load_market_artifact(report_root, "cn", target_cn_date)
    if slot != "am" or artifact_ready(cn):
        return cn, None

    fallback_day = previous_session(parse_ymd(target_cn_date), "XSHG")
    fallback = load_market_artifact(report_root, "cn", fallback_day.isoformat())
    if artifact_ready(fallback):
        note = (
            f"AM target {target_cn_date} has no same-day CN payload yet; "
            f"using previous CN session context {fallback.report_date}."
        )
        return fallback, note
    return cn, None


def load_us_context_artifact(report_root: Path, target_us_date: str) -> tuple[MarketArtifact, str | None]:
    us = load_market_artifact(report_root, "us", target_us_date)
    if artifact_ready(us):
        return us, None

    cur = previous_session(parse_ymd(target_us_date), "XNYS")
    for _ in range(10):
        fallback = load_market_artifact(report_root, "us", cur.isoformat())
        if artifact_ready(fallback):
            note = (
                f"US target {target_us_date} has no frozen payload yet; "
                f"using latest available US session context {fallback.report_date}."
            )
            return fallback, note
        cur = previous_session(cur, "XNYS")
    return us, None


def relative_display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def get_path(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def market_actions(payload: dict[str, Any], market: str) -> list[dict[str, Any]]:
    wanted = market.upper()
    rows = get_path(payload, "production_decision_summary", "actionable", default=[])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if str(row.get("market") or "").upper() == wanted]


def is_cn_star_symbol(symbol: Any) -> bool:
    return str(symbol or "").upper().startswith("688")


def cn_star_priority(row: dict[str, Any]) -> tuple[int, int, float, str]:
    tier = str(row.get("production_tier") or row.get("tier") or "").lower()
    action = str(row.get("production_action") or row.get("action") or "").lower()
    if tier in {"top_stock_trade", "secondary_stock_trade"} or action.startswith("buy"):
        bucket = 0
    elif tier == "active_watch":
        bucket = 1
    elif tier == "ranked_watch":
        bucket = 2
    else:
        bucket = 3
    try:
        rank = int(row.get("rank") or 999)
    except (TypeError, ValueError):
        rank = 999
    try:
        score = float(row.get("rank_score") or row.get("score") or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    return bucket, rank, -score, str(row.get("symbol") or "")


def compact_cn_star_candidate(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").upper()
    return {
        "symbol": symbol,
        "name": row.get("name") or symbol,
        "board": "科创板",
        "pipeline_stage": row.get("production_tier") or row.get("tier") or "",
        "action": row.get("production_action") or row.get("action") or row.get("lifecycle_action") or "",
        "rank": row.get("rank"),
        "rank_score": row.get("rank_score") or row.get("score"),
        "pct_chg": row.get("pct_chg"),
        "ret_5d_pct": row.get("ret_5d_pct"),
        "size_hint": row.get("size_hint") or row.get("size_r"),
        "entry": row.get("entry") or row.get("observation_entry_zone"),
        "handling_line": row.get("handling_line") or row.get("handle"),
        "target": row.get("first_target") or row.get("target"),
        "reason": row.get("reason") or row.get("gate_summary") or row.get("trigger"),
    }


def select_cn_star_pipeline_candidates(payload: dict[str, Any], *, limit: int = 8) -> list[dict[str, Any]]:
    ranker = payload.get("cn_opportunity_ranker") if isinstance(payload.get("cn_opportunity_ranker"), dict) else {}
    rows = ranker.get("all_rows") if isinstance(ranker.get("all_rows"), list) else []
    if not rows:
        rows = get_path(payload, "cn", "current", default=[])
    candidates = [
        row for row in rows
        if isinstance(row, dict) and is_cn_star_symbol(row.get("symbol"))
    ]
    candidates.sort(key=cn_star_priority)
    return [compact_cn_star_candidate(row) for row in candidates[:limit]]


def fmt(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def fmt_r(value: Any) -> str:
    try:
        return f"{float(value):.4f}R"
    except (TypeError, ValueError):
        return "-"


def summarize_artifact(artifact: MarketArtifact) -> dict[str, Any]:
    payload = artifact.payload
    summary = get_path(payload, "production_decision_summary", "summary", default={})
    if not isinstance(summary, dict):
        summary = {}
    market_key = artifact.market.lower()
    regime_key = "risk_regime" if market_key == "us" else "cn_risk_regime"
    regime = payload.get(regime_key) if isinstance(payload.get(regime_key), dict) else {}
    report_dates = payload.get("report_dates") if isinstance(payload.get("report_dates"), dict) else {}
    actions = market_actions(payload, artifact.market)
    out = {
        "market": artifact.market.upper(),
        "report_date": artifact.report_date,
        "source_markdown": relative_display(artifact.markdown_path) if artifact.markdown_path else "",
        "source_payload": relative_display(artifact.report_dir / "main_strategy_v2_backtest.json"),
        "r": summary.get(f"{market_key}_r"),
        "action_count": summary.get(f"{market_key}_action_count") or len(actions),
        "regime": regime.get("state") or regime.get("regime") or regime.get("label"),
        "r_multiplier": regime.get("r_multiplier") or regime.get("gate_multiplier"),
        "data_dates": report_dates,
        "actions": actions[:12],
        "markdown_excerpt": excerpt(artifact.markdown),
    }
    if market_key == "cn":
        out["star_pipeline_candidates"] = select_cn_star_pipeline_candidates(payload)
    return out


def excerpt(text: str, *, limit: int = 2600) -> str:
    clean = "\n".join(line.rstrip() for line in text.splitlines() if line.strip())
    if len(clean) <= limit:
        return clean
    return clean[:limit].rstrip() + "\n...[truncated]"


def action_table(actions: list[dict[str, Any]]) -> str:
    lines = [
        "| Market | Symbol | Size | State | Reason |",
        "|---|---|---:|---|---|",
    ]
    if not actions:
        lines.append("| - | None | - | - | - |")
        return "\n".join(lines)
    for row in actions[:10]:
        lines.append(
            "| {market} | {symbol} | {size} | {state} | {reason} |".format(
                market=fmt(row.get("market")),
                symbol=fmt(row.get("symbol")),
                size=fmt_r(row.get("size_r")),
                state=fmt(row.get("decision") or row.get("state") or row.get("status")),
                reason=fmt(row.get("reason") or row.get("why") or row.get("evidence_state")),
            )
        )
    return "\n".join(lines)


def build_tool_manifest(slot: str, cn_summary: dict[str, Any], us_summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": "read_us_main_strategy_v2_payload",
            "kind": "compute_brick",
            "market": "US",
            "source": us_summary.get("source_payload"),
            "returns": ["gross_r", "actions", "risk_regime", "data_dates"],
            "agent_use": "Find the US drivers that can constrain or relax the next CN session.",
        },
        {
            "name": "read_cn_main_strategy_v2_payload",
            "kind": "compute_brick",
            "market": "CN",
            "source": cn_summary.get("source_payload"),
            "returns": ["gross_r", "actions", "risk_regime", "data_dates"],
            "agent_use": (
                "AM: map US risk into CN execution. "
                "PM: audit how prior US-to-CN transmission behaved; do not create CN-to-US causality."
            ),
        },
        {
            "name": "read_us_daily_markdown",
            "kind": "narrative_brick",
            "market": "US",
            "source": us_summary.get("source_markdown"),
            "returns": ["existing_summary", "editor_notes"],
            "agent_use": "Use as prior narrative context only after checking packet facts.",
        },
        {
            "name": "read_cn_daily_markdown",
            "kind": "narrative_brick",
            "market": "CN",
            "source": cn_summary.get("source_markdown"),
            "returns": ["existing_summary", "editor_notes"],
            "agent_use": "Use as CN context; PM usage is feedback and postmortem only.",
        },
        {
            "name": "select_cross_market_transmission",
            "kind": "agent_reasoning_tool",
            "market": "US,CN",
            "source": "packet.cn + packet.us",
            "returns": ["dominant_us_driver", "cn_execution_implication", "invalidated_links"],
            "agent_use": "Heuristically decide the story spine from facts; this is not a fixed section template.",
        },
        {
            "name": "write_cross_market_daily",
            "kind": "agent_output_tool",
            "market": "US,CN",
            "source": "selected packet facts",
            "returns": ["markdown_report"],
            "agent_use": "Write one deliverable-style shadow report after fact selection and causality checks.",
        },
        {
            "name": "finance-search.quant_stack_daily_snapshot",
            "kind": "mcp_tool",
            "market": "US,CN",
            "source": "Hermes MCP server: finance-search",
            "returns": ["compact_state", "ranker_summary", "available_reports"],
            "agent_use": "Optional live read of frozen quant-stack state when the Hermes agent needs more context.",
        },
        {
            "name": "finance-search.quant_stack_data_capabilities",
            "kind": "mcp_tool",
            "market": "US,CN,macro",
            "source": "Hermes MCP server: finance-search",
            "returns": ["source_map", "fetch_worker_status", "available_data_bricks"],
            "agent_use": (
                "Use before writing when deciding which Tushare, AkShare, US market, FRED/Fed, "
                "and local artifact data bricks are worth loading."
            ),
        },
        {
            "name": "finance-search.quant_stack_spine_triage",
            "kind": "mcp_tool",
            "market": "US,CN",
            "source": "Hermes MCP server: finance-search",
            "returns": ["routes", "selected_symbols", "risk_bricks"],
            "agent_use": "Optional routing tool for dynamic lead-agent planning before writing.",
        },
        {
            "name": "finance-search.quant_stack_task_status",
            "kind": "mcp_tool",
            "market": "ops",
            "source": "Hermes MCP server: finance-search",
            "returns": ["task_state", "log_tail"],
            "agent_use": "Optional freshness and cron status check. Do not trigger delivery from this tool.",
        },
        {
            "name": "finance-search.quant_stack_validate_main_strategy_v2",
            "kind": "mcp_tool",
            "market": "US,CN",
            "source": "Hermes MCP server: finance-search",
            "returns": ["validator_result"],
            "agent_use": "Optional read-only validator guardrail before treating any report as deliverable.",
        },
        {
            "name": "finance-search.get_market_snapshot",
            "kind": "mcp_tool",
            "market": "global",
            "source": "Hermes MCP server: finance-search",
            "returns": ["close", "change_pct", "volume", "source"],
            "agent_use": (
                "Use for global market temperature: US cash indices, US equity futures, Europe/Asia "
                "country indices, VIX, oil, gold, USD/CNH, and China broad/STAR indices when available."
            ),
        },
        {
            "name": "finance-search.newsnow_radar",
            "kind": "mcp_tool",
            "market": "global",
            "source": "Hermes MCP server: finance-search",
            "returns": ["headline_candidates", "scores", "urls"],
            "agent_use": "Use before writing to find the few global headlines that explain risk appetite.",
        },
        {
            "name": "finance-search.search_news",
            "kind": "mcp_tool",
            "market": "global",
            "source": "Hermes MCP server: finance-search",
            "returns": ["deduped_news_items", "diagnostics"],
            "agent_use": "Use for timely macro, geopolitical, AI, semiconductor, and China market catalysts.",
        },
        {
            "name": "finance-search.research_brief",
            "kind": "mcp_tool",
            "market": "global",
            "source": "Hermes MCP server: finance-search",
            "returns": ["candidates", "gdelt", "market_snapshot"],
            "agent_use": "Optional compact evidence pack; use returned evidence, never print the internal gaps list.",
        },
        {
            "name": "finance-search.quant_stack_ranker",
            "kind": "mcp_tool",
            "market": "US,CN",
            "source": "Hermes MCP server: finance-search",
            "returns": ["ranker_rows", "symbols", "scores"],
            "agent_use": (
                "Use CN ranker rows to avoid main-board bias and include 科创板/STAR semiconductor names "
                "when verifiable."
            ),
        },
        {
            "name": "finance-search.quant_stack_symbol_context",
            "kind": "mcp_tool",
            "market": "US,CN",
            "source": "Hermes MCP server: finance-search",
            "returns": ["ranker_row", "gamma_context", "evidence_context"],
            "agent_use": "Use only for selected symbols that matter to the story; do not bulk-dump symbol context.",
        },
    ]


def build_agent_operating_mode(slot: str) -> dict[str, Any]:
    if slot == "am":
        objective = "Use US post-market facts to frame CN pre-market execution."
    else:
        objective = "Combine CN post-market feedback with US pre-market context without CN-to-US causality."
    return {
        "driver": "hermes_style_lead_editor",
        "mode": "heuristic_tool_use",
        "objective": objective,
        "contract": (
            "quant-stack freezes facts and exposes MCP/skill-like tools; "
            "the lead editor chooses which facts matter and how to narrate them."
        ),
        "fixed": [
            "fact sources",
            "US -> CN causal direction",
            "no invented numbers/tickers/news",
            "deliverable validation gates",
        ],
        "not_fixed": [
            "section order",
            "which available tools are worth using",
            "headline angle beyond the required report prefix",
            "narrative emphasis and length",
        ],
    }


def build_data_boundary() -> dict[str, Any]:
    return {
        "fetch_workers": "Own data collection, staging, freshness, and retry state. They do not write narrative.",
        "compute_bricks": "Read canonical data/artifacts and freeze R, actions, regimes, gates, and evidence facts.",
        "agent_editor": "Reads frozen facts through the tool manifest, selects emphasis, writes narrative, and never computes.",
        "validators": "Reject causality drift, invented facts, stale inputs, and production delivery markers.",
    }


def build_coverage_checklist(slot: str) -> list[str]:
    if slot == "am":
        return [
            "Open with the US/global market driver that most changes CN risk for the next open.",
            "Include a concise global market temperature when finance-search returns indices/futures/oil/gold/FX snapshots.",
            "Explain the US -> CN transmission path and where it can fail.",
            "Map AI/semiconductor signals into A-share execution, including 科创板/STAR candidates when verifiable.",
            "Translate the driver into CN execution limits, sector priority, and watch items.",
            "Show actionable US/CN facts only when they clarify the story.",
            "End with risk, invalidation, and next-session checks in investor-readable language.",
        ]
    return [
        "Review CN post-market action as feedback on prior US-to-CN transmission.",
        "Summarize global and US pre-market context from US/global facts, not from CN direction.",
        "Check whether 科创板/STAR semiconductor moves confirm or reject the prior US-to-CN read-through.",
        "State explicitly that CN feedback cannot raise or cut US positioning.",
        "Identify what the next US session can change for the following CN session.",
        "End with risk, invalidation, and next-session checks in investor-readable language.",
    ]


def build_style_brief() -> dict[str, Any]:
    return {
        "reference_name": "Boist market execution diary",
        "reference_url": STYLE_REFERENCE_URL,
        "use_as": "style inspiration only; do not copy wording or claims",
        "principles": [
            "Narrative first: start from the market story, not from a table.",
            "Use a strong topical headline and clear cause-effect chain.",
            "Blend macro, sector, positioning, leverage, and event risk into one thesis.",
            "Write like an execution diary: what changed, why it matters, what would invalidate it.",
            "Use compact bullets/tables only for trade facts or explicit scenario thresholds.",
        ],
    }


def build_external_context_requirements(slot: str) -> dict[str, Any]:
    if slot == "am":
        usage = "Use overnight US/global facts to frame A-share pre-market execution."
    else:
        usage = "Use global and US pre-market facts as context; CN action is feedback only."
    return {
        "usage": usage,
        "must_try_finance_search_tools": [
            {
                "tool": "finance-search.quant_stack_data_capabilities",
                "purpose": (
                    "Expose Quant Stack's fetch universe: Tushare/AkShare A-share workers, "
                    "US market data, FRED/Fed macro series, SEC filings, and frozen report artifacts."
                ),
            },
            {
                "tool": "finance-search.get_market_snapshot",
                "symbols": GLOBAL_MARKET_SNAPSHOT_SYMBOLS,
                "purpose": (
                    "Build a concise global market thermometer: US cash indices, S&P/Nasdaq/Dow/Russell "
                    "futures, Europe and Asia country indices, VIX/rates proxy, oil, gold, USD/CNH, "
                    "and China broad/STAR indices."
                ),
            },
            {
                "tool": "finance-search.newsnow_radar",
                "topic": "markets",
                "keywords": "Fed, inflation, oil, gold, China, AI, semiconductors, Nvidia, tariffs, geopolitics",
                "purpose": "Find the few headlines that explain risk appetite and sector rotation.",
            },
            {
                "tool": "finance-search.search_news",
                "queries": [
                    "global markets US futures oil gold Fed China AI semiconductors",
                    "Nvidia semiconductors AI supply chain Asia stocks",
                    "China A-share semiconductors STAR Market 科创板 半导体",
                ],
                "purpose": "Fill the market story with timely news only when source titles/URLs are returned.",
            },
            {
                "tool": "finance-search.research_brief",
                "topics": [
                    "global market risk pulse for US and China equities",
                    "AI semiconductor supply chain read-through to A-shares",
                ],
                "purpose": "Use only as an evidence pack; do not print its internal gaps section.",
            },
        ],
        "public_output_rule": (
            "The public report must include returned values for: oil, gold, at least one US equity future, "
            "and several non-US country/region benchmarks when available. Mention only returned, checkable "
            "headlines/snapshots. Every cited index or future must show its returned date, especially "
            "cross-timezone markets. If a feed or symbol is unavailable, omit it; do not print a missing-data "
            "list or tool failure note. Do not cite India/Sensex/Nifty indices."
        ),
    }


def build_cn_universe_requirement() -> dict[str, Any]:
    return {
        "scope": (
            "A-share semiconductor and AI hardware mapping must include concrete STAR Market/科创板 "
            "688xxx candidates as part of the CN selection pipeline, not merely as an index thermometer."
        ),
        "board_policy": [
            "Do not treat main-board A-shares as the whole CN universe.",
            "Select concrete 688xxx.SH names from packet.cn.star_pipeline_candidates when available.",
            "Use 0R/active_watch language when a STAR name is not executable yet, but still keep it in the A-share pipeline.",
            "Do not describe 科创板 only as a temperature gauge; tie it to named candidates and execution/watch stages.",
            "Use sector-level language only when no verifiable 688xxx symbol comes back; do not publish a missing-symbol list.",
        ],
        "finance_search_tools": [
            "finance-search.quant_stack_ranker(market='cn', limit=30)",
            "finance-search.quant_stack_symbol_context(symbol='<selected_cn_symbol>', market='cn')",
            "finance-search.search_news(query='科创板 半导体 AI 芯片 A股')",
        ],
    }


def compact_market_snapshot_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    symbols = snapshot.get("symbols") if isinstance(snapshot.get("symbols"), dict) else {}
    rows: list[dict[str, Any]] = []
    for symbol in snapshot.get("used_symbols") or []:
        item = symbols.get(symbol) if isinstance(symbols, dict) else None
        if not isinstance(item, dict) or not item.get("ok"):
            continue
        rows.append(
            {
                "symbol": symbol,
                "label": GLOBAL_MARKET_LABELS.get(symbol, symbol),
                "date": item.get("date"),
                "close": item.get("close"),
                "change_pct": item.get("change_pct"),
                "source": item.get("source"),
                "display": (
                    f"{GLOBAL_MARKET_LABELS.get(symbol, symbol)}({item.get('date') or 'date n/a'}): "
                    f"{item.get('close')} / {item.get('change_pct')}%"
                ),
            }
        )
    return rows


def public_index_marker_allowed(row: dict[str, Any]) -> bool:
    text = " ".join(str(row.get(key) or "") for key in ("symbol", "label", "display"))
    return not any(marker.lower() in text.lower() for marker in FORBIDDEN_PUBLIC_INDEX_MARKERS)


def fmt_market_value(value: Any) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or "-"
    if abs(number) < 10:
        text = f"{number:.4f}"
    else:
        text = f"{number:.2f}"
    return text.rstrip("0").rstrip(".")


def fmt_market_change_pct(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "-"
        if text.endswith("%"):
            return text
        try:
            value = float(text)
        except ValueError:
            return text
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or "-"


def render_market_snapshot_section(packet: dict[str, Any]) -> str:
    prefetch = packet.get("finance_search_prefetch") if isinstance(packet.get("finance_search_prefetch"), dict) else {}
    raw_rows = prefetch.get("market_rows") if isinstance(prefetch.get("market_rows"), list) else []
    rows = [row for row in raw_rows if isinstance(row, dict) and row.get("date") and public_index_marker_allowed(row)]
    if not rows:
        return ""

    by_symbol = {str(row.get("symbol") or ""): row for row in rows}
    consumed: set[str] = set()
    lines = [
        "## 全球市场温度",
        "| 类别 | 指标 | 日期 | 最新/收盘 | 涨跌幅 |",
        "|---|---|---|---:|---:|",
    ]
    for group, symbols in GLOBAL_MARKET_SNAPSHOT_GROUPS:
        for symbol in symbols:
            row = by_symbol.get(symbol)
            if not row:
                continue
            consumed.add(symbol)
            lines.append(
                "| {group} | {label} | {date} | {close} | {change} |".format(
                    group=group,
                    label=fmt(row.get("label") or symbol),
                    date=fmt(row.get("date")),
                    close=fmt_market_value(row.get("close")),
                    change=fmt_market_change_pct(row.get("change_pct")),
                )
            )
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if symbol in consumed:
            continue
        lines.append(
            "| 其他 | {label} | {date} | {close} | {change} |".format(
                label=fmt(row.get("label") or symbol),
                date=fmt(row.get("date")),
                close=fmt_market_value(row.get("close")),
                change=fmt_market_change_pct(row.get("change_pct")),
            )
        )
    return "\n".join(lines)


def market_snapshot_date_map(packet: dict[str, Any]) -> dict[str, str]:
    prefetch = packet.get("finance_search_prefetch") if isinstance(packet.get("finance_search_prefetch"), dict) else {}
    raw_rows = prefetch.get("market_rows") if isinstance(prefetch.get("market_rows"), list) else []
    date_map: dict[str, str] = {}
    for row in raw_rows:
        if not isinstance(row, dict) or not row.get("date") or not public_index_marker_allowed(row):
            continue
        symbol = str(row.get("symbol") or "")
        aliases = list(GLOBAL_MARKET_LABEL_ALIASES.get(symbol, ()))
        label = str(row.get("label") or "").strip()
        if label:
            aliases.append(label)
        for alias in aliases:
            if alias:
                date_map[alias] = str(row["date"])
    return dict(sorted(date_map.items(), key=lambda item: len(item[0]), reverse=True))


def annotate_market_snapshot_dates(report: str, packet: dict[str, Any]) -> str:
    date_map = market_snapshot_date_map(packet)
    if not date_map:
        return report.strip()
    text = report.strip()
    for marker, marker_date in date_map.items():
        suffix_guard = "波动率" if marker == "VIX" else ""
        suffix_guard_pattern = rf"(?!{re.escape(suffix_guard)})" if suffix_guard else ""
        pattern = re.compile(
            rf"{re.escape(marker)}{suffix_guard_pattern}(?!\(\b20\d{{2}}-\d{{2}}-\d{{2}}\b\))"
        )
        text = pattern.sub(f"{marker}({marker_date})", text)
    return text


def replace_market_snapshot_sections(report: str, section: str) -> str:
    lines = report.strip().splitlines()
    if not lines:
        return section
    section_lines = section.splitlines()
    output: list[str] = []
    inserted = False
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        if line.strip().startswith("## 全球市场温度"):
            if not inserted:
                if output and output[-1].strip():
                    output.append("")
                output.extend(section_lines)
                output.append("")
                inserted = True
            idx += 1
            while idx < len(lines) and not lines[idx].strip():
                idx += 1
            if idx < len(lines) and lines[idx].lstrip().startswith("|"):
                while idx < len(lines) and (not lines[idx].strip() or lines[idx].lstrip().startswith("|")):
                    idx += 1
            continue
        output.append(line)
        idx += 1
    return "\n".join(output).strip()


def ensure_market_snapshot_section(report: str, packet: dict[str, Any]) -> str:
    section = render_market_snapshot_section(packet)
    if not section:
        return report.strip()
    if "## 全球市场温度" in report:
        return replace_market_snapshot_sections(report, section)

    lines = report.strip().splitlines()
    if not lines:
        return section
    title_idx = next((idx for idx, line in enumerate(lines) if line.strip().startswith("# ")), 0)
    para_start = title_idx + 1
    while para_start < len(lines) and not lines[para_start].strip():
        para_start += 1
    if para_start >= len(lines):
        return "\n".join([lines[title_idx], "", section]).strip()
    insert_at = para_start + 1
    while insert_at < len(lines) and lines[insert_at].strip():
        insert_at += 1
    return "\n".join(lines[:insert_at] + ["", section, ""] + lines[insert_at:]).strip()


def fetch_finance_search_prefetch(*, window: str, timeout: int = 90) -> dict[str, Any]:
    root = Path(os.environ.get("FINANCE_SEARCH_AGENT_ROOT", "/home/ubuntu/services/finance-search-agent"))
    python = Path(os.environ.get("FINANCE_SEARCH_PYTHON", str(root / ".venv" / "bin" / "python")))
    server = root / "server.py"
    if not python.exists() or not server.exists():
        return {"ok": False, "error": "finance-search agent runtime not found"}
    code = r"""
import asyncio
import json
import sys

root = sys.argv[1]
symbols = sys.argv[2]
window = sys.argv[3]
sys.path.insert(0, root)
import server  # noqa: E402

def batched_snapshot(symbols_arg):
    requested = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    merged = {"symbols": {}, "requested_symbols": symbols_arg, "used_symbols": []}
    for idx in range(0, len(requested), 10):
        chunk = requested[idx:idx + 10]
        data = server.get_market_snapshot(symbols=",".join(chunk), period="5d")
        merged["symbols"].update(data.get("symbols") or {})
        merged["used_symbols"].extend(data.get("used_symbols") or chunk)
    return merged

async def main():
    snapshot = batched_snapshot(symbols)
    try:
        news = await server.search_news(
            query="global markets US futures oil gold Fed China AI semiconductors",
            window=window,
            limit=8,
            sources="newsnow,tavily,searxng,gdelt",
        )
    except Exception as exc:
        news = {"items": [], "error": str(exc)}
    print(json.dumps({"ok": True, "market_snapshot": snapshot, "news": news}, ensure_ascii=False))

asyncio.run(main())
"""
    try:
        result = subprocess.run(
            [str(python), "-c", code, str(root), GLOBAL_MARKET_SNAPSHOT_SYMBOLS, window],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": str(exc)}
    if result.returncode != 0:
        return {"ok": False, "error": ((result.stderr or "") + (result.stdout or ""))[-800:]}
    try:
        payload = json.loads((result.stdout or "").strip())
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"invalid finance-search JSON: {exc}"}
    snapshot = payload.get("market_snapshot") if isinstance(payload.get("market_snapshot"), dict) else {}
    payload["market_rows"] = compact_market_snapshot_rows(snapshot)
    news = payload.get("news") if isinstance(payload.get("news"), dict) else {}
    items = news.get("items") if isinstance(news.get("items"), list) else []
    payload["news_items"] = [
        {
            "title": item.get("title"),
            "source": item.get("source") or item.get("provider"),
            "published_at": item.get("published_at") or item.get("date"),
            "url": item.get("url"),
        }
        for item in items[:8]
        if isinstance(item, dict) and item.get("title")
    ]
    payload.pop("news", None)
    payload.pop("market_snapshot", None)
    return payload


def attach_finance_search_prefetch(packet: dict[str, Any], *, target_cn_date: str) -> None:
    target = parse_ymd(target_cn_date)
    window = "72h" if target.weekday() in {0, 5} else "24h"
    payload = fetch_finance_search_prefetch(window=window)
    packet["finance_search_prefetch"] = {
        "window": window,
        "source": "finance-search local agent read",
        "ok": bool(payload.get("ok")),
        "market_rows": payload.get("market_rows") or [],
        "news_items": payload.get("news_items") or [],
        "error": payload.get("error") if not payload.get("ok") else "",
        "public_use": (
            "Use market_rows and news_items as already-fetched evidence for the global market temperature. "
            "Do not mention this prefetch source, diagnostics, or errors in the public report."
        ),
    }


def build_packet(slot: str, cn: MarketArtifact, us: MarketArtifact) -> dict[str, Any]:
    cn_summary = summarize_artifact(cn)
    us_summary = summarize_artifact(us)
    if slot == "am":
        direction = "US post-market -> CN pre-market"
        lead_market = "US"
        target_market = "CN"
        cn_role = "execution_target"
        us_role = "causal_driver"
        thesis = "美股盘后风险、AI 主线和期权/Gamma 情绪先约束 A股开盘仓位。"
    else:
        direction = "CN post-market feedback + US pre-market context"
        lead_market = "US"
        target_market = "CN"
        cn_role = "feedback_only"
        us_role = "causal_driver"
        thesis = (
            "A股盘后只用来复盘上一轮美股主线在中国资产里的传导,不反向约束美股盘前仓位;"
            "美股盘前仍按美股自己的 regime、money gate、期权/Gamma 和证据门决策。"
        )
    return {
        "slot": slot,
        "direction": direction,
        "causal_direction": "US -> CN",
        "lead_market": lead_market,
        "target_market": target_market,
        "us_role": us_role,
        "cn_role": cn_role,
        "thesis": thesis,
        "agent_operating_mode": build_agent_operating_mode(slot),
        "data_boundary": build_data_boundary(),
        "tool_manifest": build_tool_manifest(slot, cn_summary, us_summary),
        "external_context_requirements": build_external_context_requirements(slot),
        "cn_universe_requirement": build_cn_universe_requirement(),
        "coverage_checklist": build_coverage_checklist(slot),
        "style_brief": build_style_brief(),
        "cn": cn_summary,
        "us": us_summary,
        "invariants": [
            "Numbers and tickers must come from packet facts only.",
            "No combined kitchen-ticket artifact may be sent as production email.",
            "US can guide CN; CN feedback must not drive US positioning.",
            "Cross-market links are guidance overlays, not proof of tradability.",
        ],
    }


def annotate_target_context(
    packet: dict[str, Any],
    *,
    target_cn_date: str,
    target_us_date: str,
    cn_context_note: str | None,
    us_context_note: str | None,
) -> None:
    packet["target_cn_date"] = target_cn_date
    packet["target_us_date"] = target_us_date
    packet["cn_context_date"] = packet["cn"]["report_date"]
    packet["us_context_date"] = packet["us"]["report_date"]
    if cn_context_note:
        packet["cn_context_note"] = cn_context_note
    if us_context_note:
        packet["us_context_note"] = us_context_note


def deterministic_report(packet: dict[str, Any]) -> str:
    slot = packet["slot"]
    cn = packet["cn"]
    us = packet["us"]
    if slot == "am":
        title = f"# 跨市场早报 — {cn['report_date']}"
        direction_title = "美股盘后 → A股盘前"
        target_title = "A股开盘执行影响"
    else:
        title = f"# 跨市场晚报 — {cn['report_date']}"
        direction_title = "A股盘后反馈 + 美股盘前"
        target_title = "不对称传导约束"

    return f"""{title}

## {direction_title}
{packet['thesis']}

| Item | US | CN |
|---|---:|---:|
| report_date | {fmt(us.get('report_date'))} | {fmt(cn.get('report_date'))} |
| gross R | {fmt_r(us.get('r'))} | {fmt_r(cn.get('r'))} |
| action_count | {fmt(us.get('action_count'))} | {fmt(cn.get('action_count'))} |
| regime | {fmt(us.get('regime'))} | {fmt(cn.get('regime'))} |
| R multiplier | {fmt(us.get('r_multiplier'))} | {fmt(cn.get('r_multiplier'))} |

## {target_title}
- 因果方向固定为: {packet['causal_direction']}。
- 美股是主导变量；A股按自己的 money gate 和 evidence gate 执行,并接受美股风险、AI 主线和期权/Gamma 的约束。
- A股盘后只能作为传导反馈和复盘材料,不得反向升降美股仓位或把美股观察票升成执行票。

## 美股交易事实
{action_table(us.get('actions') or [])}

## A股交易事实
{action_table(cn.get('actions') or [])}

## Agent 编辑输入摘要
### US source
{fmt(us.get('source_markdown'))}

{fmt(us.get('markdown_excerpt'))}

### CN source
{fmt(cn.get('source_markdown'))}

{fmt(cn.get('markdown_excerpt'))}

## 交付状态
- shadow_only: true
- agent_backend: off
- production_delivery: disabled
"""


def build_agent_messages(packet: dict[str, Any]) -> tuple[str, str]:
    if packet["slot"] == "am":
        title_rule = "# 跨市场早报"
        direction_rule = "美股盘后事实可约束 A股盘前: 仓位、行业优先级、风险线。"
    else:
        title_rule = "# 跨市场晚报"
        direction_rule = (
            "A股盘后只能作为美股主线向中国资产传导的反馈;不得写成 A股指导美股。"
            "美股盘前仍由美股自己的 regime、money gate、期权/Gamma 和证据门决定。"
        )
    system = f"""
你是 quant-stack 的 Hermes 风格跨市场 lead editor agent。你不是固定流水线脚本。
packet 里的 tool_manifest 是 MCP/skill-like 工具面;你可以启发式选择事实、顺序和叙事重点。
coverage_checklist 是验收清单,不是章节模板;不要机械照抄成固定小标题。

你只能使用用户给定 packet 里的数字、ticker、日期和结论。
禁止编造价格、R、ticker、新闻或仓位。输出一份中文跨市场日报,第一行必须以 `{title_rule}` 开头。
{direction_rule}
写法参考 packet.style_brief: 市场执行日记、强主题标题、先讲市场为何变化,再讲传导、执行和失效条件。
必须满足 coverage_checklist 和 invariants,但正文结构由你按证据自行组织。
"""
    user = json.dumps(packet, ensure_ascii=False, indent=2)
    return system, user


def build_hermes_prompt(packet: dict[str, Any]) -> str:
    if packet["slot"] == "am":
        title_rule = "# 跨市场早报"
        slot_rule = "AM: 美股盘后事实 -> A股盘前执行约束。"
    else:
        title_rule = "# 跨市场晚报"
        slot_rule = "PM: A股盘后只作为上一轮 US->CN 传导反馈;美股盘前仍由美股事实决定。"
    packet_json = json.dumps(packet, ensure_ascii=False, indent=2)
    return f"""
你是 Hermes lead editor agent,正在执行 quant-stack-cross-market-daily skill。

目标: 动态编排工具和事实,写出一份中文跨市场日报。你不是旧的 extractor/narrator 流水线,
不要使用 quant-research-v1/prompts 或 quant-research-cn/prompts 的固定章节模板。

工作方式:
- 先读下面 packet。packet 是 quant-stack 已冻结的事实砖和工具清单。
- 如果 packet.finance_search_prefetch.ok=true,必须优先使用其中的 market_rows 和 news_items 写全球市场温度;
  这些是脚本侧已经从 finance-search 取回的证据,不要再写成缺失或工具失败。
- 可以启发式使用 finance-search MCP 工具,尤其是:
  quant_stack_daily_snapshot, quant_stack_spine_triage, quant_stack_task_status,
  quant_stack_validate_main_strategy_v2, quant_stack_ranker, quant_stack_symbol_context,
  get_market_snapshot, newsnow_radar, search_news, research_brief。
- 写作前必须尝试构造“全球市场温度”:美股现货指数、美股期货(标普/纳指/道指/罗素期货),
  欧洲主要指数(如 STOXX/DAX/FTSE/CAC),亚洲主要指数(如日经/KOSPI/恒生/台湾),
  VIX、油、金、美元/离岸人民币、中国主要指数和科创板/STAR 指数;只使用工具实际返回的数据。
- 引用任何大盘指数或期货时,必须带返回日期,格式类似“德国DAX(2026-06-29)”或“纳指期货(2026-06-29)”;
  跨时区市场尤其不能只写“今天/隔夜”。不要引用印度、Sensex 或 Nifty 指数。
- 写作前必须尝试检索最新宏观/地缘/AI/半导体/中国市场新闻;只使用返回标题、来源或 URL 可核验的新闻。
- A股侧不得只看主板;必须用 packet.cn.star_pipeline_candidates 或 CN ranker/symbol_context
  选择具体科创板/688xxx.SH 标的。科创板不是温度计,它是 A股候选管线的一部分;
  如果候选仍是 active_watch/0R,也要写清具体代码、名称、等待条件和不能执行的原因。
- 如果工具、feed、symbol 或新闻没有返回可核验结果,公开报告里自然省略;不要写任何数据不可用、缺口、待补或工具失败说明。
- coverage_checklist 是验收清单,不是章节模板。
- 正文结构、标题角度、叙事顺序由你按证据决定。

硬约束:
- 第一行必须以 `{title_rule}` 开头。
- {slot_rule}
- 因果方向固定为美股事实约束 A股策略;不得把 A股盘后反馈写成会指导美股盘前或美股策略。
- 正文不要输出任何反向因果箭头标记。
- 不得编造给定事实或工具返回之外的价格、ticker、R、新闻、仓位和结论。
- 不得触发邮件、cron、生产投递或文件修改;最终只输出 markdown 报告文本。
- 公开正文不要出现工具日志、运行状态、prompt、system/user 角色、思考过程、审稿过程、JSON 或文件路径。
- 不要输出“以下是/我将/作为AI”这类自我说明;只输出读者可直接阅读的报告。
- 不要输出内部研究状态词: production、ranker、AI Infra universe、source evidence、source review、headline risk、beta hedge、money gate、regime、原文验证状态。

写作风格:
- 参考 packet.style_brief 和 Boist 市场日记风格: 强主题开场,先讲市场故事和因果链,
  再讲跨市场传导、仓位/风险约束、失效条件和下一步检查。
- 少用机械表格。表格只服务交易事实、风险阈值或情景边界。
- 少用黑话;R、Gamma、regime、money gate 第一次出现时必须用中文短语解释,否则改成中文说法。
- 允许有判断,但每个判断必须能追溯到 packet 或 MCP 返回。

packet:
```json
{packet_json}
```
""".strip()


def build_hermes_review_prompt(packet: dict[str, Any], draft: str) -> str:
    if packet["slot"] == "am":
        title_rule = "# 跨市场早报"
        slot_rule = "美股盘后事实约束 A股盘前计划。"
    else:
        title_rule = "# 跨市场晚报"
        slot_rule = "A股盘后只复盘上一轮美股到中国资产的传导,不指导美股盘前。"
    packet_json = json.dumps(packet, ensure_ascii=False, indent=2)
    return f"""
你是 quant-stack 跨市场日报的二审编辑。你不是继续分析,而是把下面 draft 改成可直接发送的一封中文日报。

二审目标:
- 输出一封合并日报,不是美股一封、A股一封,也不是多封邮件。
- 第一行必须以 `{title_rule}` 开头。
- {slot_rule}
- 美股是主导变量,A股按本域门禁执行;不得把 A股反馈写成会指导美股。
- 保留 draft/packet 里已有的 ticker、日期、R、价格线和结论;不得新增事实、价格、新闻或仓位。
- 把全球新闻/宏观/指数/期货/油金和 A股半导体/科创板线索整合成同一个故事;不要拆成美股报告+A股报告。
- 全球温度段必须保留:美股期货、油、金、至少多个非美国家/地区大盘指数;如果 draft 已遗漏,
  只能从 packet/工具返回中补入,不能虚构数字。
- 如果 packet.finance_search_prefetch 有 market_rows/news_items,优先使用这些已取回证据补齐全球温度,
  不要写成缺失、不可用或工具失败。
- 全球温度段引用任何大盘指数或期货时必须带返回日期,尤其是欧洲/亚洲/美国期货这类跨时区市场;
  格式类似“日经225(2026-06-29)”或“标普期货(2026-06-29)”。删除印度、Sensex、Nifty 指数。
- A股执行段必须从 packet.cn.star_pipeline_candidates 中选择至少一个具体 688xxx.SH 科创板标的;
  不得把科创板只写成“温度计”“观察指数”或泛泛的板块背景。
- 删除工具日志味、工程词和内部流程词:不要出现 MCP、packet、validator、shadow_only、production_delivery、cron、Resend、JSON、script、tool、血缘、本稿状态、prompt、system、user、draft、二审、审稿、思维过程、推理过程。
- 删除或翻译内部研究黑话:不要出现 production、ranker、AI Infra universe、source evidence、source review、evidence_state、headline risk、beta hedge、money gate、regime、原文验证状态。
- 删除所有数据不可用提示、缺口清单、待补证据清单、工具失败说明;没有可核验数据就自然省略。
- 不要出现“以下是”“我将”“作为AI”“根据你的要求”等自我说明。
- 语言要像给投资人看的盘前/盘后执行日记:先讲今天市场故事,再讲跨市场传导,最后讲执行线和失效条件。
- 少用黑话;如果必须保留 R 或 Gamma,第一次出现时加一句中文解释,其他英文内部状态改成中文。
- 少用表格;只有交易线、仓位线或风险线必须对齐时才用。
- 不要解释你做了什么,不要输出审稿意见,只输出最终 markdown。

参考事实 packet:
```json
{packet_json}
```

待二审 draft:
```markdown
{draft}
```
""".strip()


def clean_hermes_stdout(text: str) -> str:
    lines = [line.rstrip() for line in (text or "").splitlines()]
    lines = [line for line in lines if not line.strip().startswith("session_id:")]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and lines[0].strip().startswith("```"):
        lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)
        if lines and lines[-1].strip() == "```":
            lines.pop()
    return "\n".join(lines).strip()


def normalize_public_report_text(text: str, slot: str) -> str:
    expected_title = "# 跨市场早报" if slot == "am" else "# 跨市场晚报"
    title_token = expected_title.lstrip("# ").strip()
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        stripped = line.strip().strip("`")
        if stripped.startswith(expected_title):
            text = "\n".join(lines[idx:]).strip()
            break
        title_pos = stripped.find(title_token)
        if title_pos >= 0:
            suffix = stripped[title_pos + len(title_token) :].lstrip(" ：:-|｜")
            lines[idx] = f"{expected_title}：{suffix}" if suffix else expected_title
            text = "\n".join(lines[idx:]).strip()
            break
    else:
        text = f"{expected_title}\n\n{text.strip()}"
    replacements = {
        "packet": "事实清单",
        "MCP": "数据接口",
        "validator": "校验",
        "shadow_only": "",
        "production_delivery": "",
        "Resend": "邮件服务",
        "JSON": "结构化数据",
        "script": "流程",
        "tool": "数据接口",
        "AI Infra universe": "AI基础设施观察池",
        "AI Infra": "AI基础设施",
        "production执行层": "正式执行清单",
        "production layer": "正式执行清单",
        "production": "正式执行",
        "source evidence": "证据",
        "source review": "来源复核",
        "evidence_state": "证据状态",
        "ranker": "排序清单",
        "headline risk": "新闻风险",
        "beta hedge": "beta 对冲",
        "money gate": "资金门槛",
        "regime": "市场状态",
        "原文验证状态": "证据状态",
        "原文验证": "证据核验",
        "draft": "初稿",
        "审稿": "编辑",
        "二审": "编辑",
        "以下是": "",
        "我将": "",
        "作为AI": "",
    }
    for old, new in replacements.items():
        text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
    return text.strip()


def call_hermes_agent(
    packet: dict[str, Any],
    *,
    timeout: int,
    hermes_bin: str,
    model: str = "",
    provider: str = "",
    max_turns: int = 16,
) -> str:
    prompt = build_hermes_prompt(packet)
    cmd = [
        hermes_bin,
        "chat",
        "-Q",
        "-q",
        prompt,
        "--skills",
        "quant-stack-cross-market-daily",
        "--accept-hooks",
        "--max-turns",
        str(max_turns),
        "--source",
        "quant-stack-cron",
    ]
    if model:
        cmd.extend(["--model", model])
    if provider:
        cmd.extend(["--provider", provider])
    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"Hermes cross-market agent timed out after {timeout}s") from exc
    except OSError as exc:
        raise RuntimeError(f"Hermes cross-market agent launch failed: {exc}") from exc
    if result.returncode != 0:
        tail = ((result.stderr or "") + "\n" + (result.stdout or ""))[-1600:]
        raise RuntimeError(f"Hermes cross-market agent failed with exit={result.returncode}: {tail}")
    text = clean_hermes_stdout(result.stdout or "")
    if not text:
        raise RuntimeError("Hermes cross-market agent returned empty output")
    packet["_agent_backend"] = "hermes"
    packet["_agent_model"] = model or os.environ.get("HERMES_INFERENCE_MODEL", "")
    packet["_agent_provider"] = provider or os.environ.get("HERMES_PROVIDER", "")
    packet["_agent_tooling"] = "quant-stack-cross-market-daily skill + finance-search MCP"
    return text


def call_hermes_reviewer(
    packet: dict[str, Any],
    draft: str,
    *,
    timeout: int,
    hermes_bin: str,
    model: str = "",
    provider: str = "",
    max_turns: int = 8,
) -> str:
    prompt = build_hermes_review_prompt(packet, draft)
    cmd = [
        hermes_bin,
        "chat",
        "-Q",
        "-q",
        prompt,
        "--skills",
        "quant-stack-cross-market-daily",
        "--accept-hooks",
        "--max-turns",
        str(max_turns),
        "--source",
        "quant-stack-reviewer",
    ]
    if model:
        cmd.extend(["--model", model])
    if provider:
        cmd.extend(["--provider", provider])
    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"Hermes cross-market reviewer timed out after {timeout}s") from exc
    except OSError as exc:
        raise RuntimeError(f"Hermes cross-market reviewer launch failed: {exc}") from exc
    if result.returncode != 0:
        tail = ((result.stderr or "") + "\n" + (result.stdout or ""))[-1600:]
        raise RuntimeError(f"Hermes cross-market reviewer failed with exit={result.returncode}: {tail}")
    text = clean_hermes_stdout(result.stdout or "")
    if not text:
        raise RuntimeError("Hermes cross-market reviewer returned empty output")
    packet["_reviewer_backend"] = "hermes"
    packet["_reviewer_model"] = model or os.environ.get("HERMES_INFERENCE_MODEL", "")
    packet["_reviewer_provider"] = provider or os.environ.get("HERMES_PROVIDER", "")
    return text


def call_hermes_reviewer_with_fallback(
    packet: dict[str, Any],
    draft: str,
    *,
    timeout: int,
    hermes_bin: str,
    review_model: str = "",
    review_provider: str = "",
    fallback_model: str = "",
    fallback_provider: str = "",
    max_turns: int = 6,
) -> str:
    try:
        return call_hermes_reviewer(
            packet,
            draft,
            timeout=timeout,
            hermes_bin=hermes_bin,
            model=review_model,
            provider=review_provider,
            max_turns=max_turns,
        )
    except Exception as primary_exc:
        same_fallback = (
            (review_model or "") == (fallback_model or "")
            and (review_provider or "") == (fallback_provider or "")
        )
        if same_fallback:
            raise
        packet["_reviewer_primary_error"] = str(primary_exc)[-800:]
        return call_hermes_reviewer(
            packet,
            draft,
            timeout=timeout,
            hermes_bin=hermes_bin,
            model=fallback_model,
            provider=fallback_provider,
            max_turns=max(4, min(max_turns, 8)),
        )


def call_agent(packet: dict[str, Any], timeout: int) -> str:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from codex_backend import call_llm, runtime_backend_summary, runtime_model_summary

    system, user = build_agent_messages(packet)
    text = call_llm(
        system,
        user,
        label=f"cross-market:{packet['slot']}",
        temperature=0.25,
        max_tokens=2600,
        timeout=timeout,
    )
    if not text:
        raise RuntimeError("cross-market lead editor returned empty output")
    packet["_agent_backend"] = runtime_backend_summary()
    packet["_agent_model"] = runtime_model_summary()
    return text


def fallback_report(packet: dict[str, Any], backend: str, timeout: int, reason: Exception) -> tuple[str, str]:
    packet["_agent_primary_error"] = str(reason)[-800:]
    if backend == "auto":
        report = call_agent(packet, timeout)
        return report, f"fallback:{packet.get('_agent_backend') or 'auto'}"
    if backend == "off":
        packet["_agent_backend"] = "deterministic_shadow"
        packet["_agent_model"] = ""
        return deterministic_report(packet), "fallback:deterministic_shadow"
    raise reason


def contains_marker(text: str, token: str) -> bool:
    return token.lower() in text.lower()


def public_context_failures(text: str) -> list[str]:
    failures: list[str] = []
    forbidden_india_index_markers = ["Sensex", "Nifty", "印度指数", "印度Sensex"]
    for marker in forbidden_india_index_markers:
        if contains_marker(text, marker):
            failures.append(f"forbidden India index marker: {marker}")

    commodity_groups = {
        "gold": ["黄金", "gold", "GLD", "GC=F"],
        "oil": ["原油", "WTI", "Brent", "布伦特", "CL=F", "BZ=F"],
    }
    for label, markers in commodity_groups.items():
        if not any(contains_marker(text, marker) for marker in markers):
            failures.append(f"missing public global-market commodity marker: {label}")

    us_future_markers = [
        "美股期货",
        "标普期货",
        "纳指期货",
        "道指期货",
        "罗素期货",
        "S&P futures",
        "Nasdaq futures",
        "Dow futures",
        "ES=F",
        "NQ=F",
        "YM=F",
        "RTY=F",
    ]
    if not any(contains_marker(text, marker) for marker in us_future_markers):
        failures.append("missing public global-market marker: US equity futures")

    global_index_markers = [
        "日经",
        "Nikkei",
        "KOSPI",
        "恒生",
        "Hang Seng",
        "台湾",
        "加权指数",
        "STOXX",
        "DAX",
        "FTSE",
        "CAC",
        "欧洲",
        "日本",
        "韩国",
        "香港",
    ]
    hit_count = sum(1 for marker in global_index_markers if contains_marker(text, marker))
    if hit_count < 3:
        failures.append("missing public global-market breadth: non-US country/region indices")

    if not re.search(r"(?<!\d)688\d{3}\.SH(?![A-Z0-9])", text):
        failures.append("missing concrete public CN STAR/科创板 ticker: 688xxx.SH")
    for raw in text.splitlines():
        line = raw.strip()
        if contains_marker(line, "科创") and contains_marker(line, "温度计"):
            failures.append("STAR/科创板 must be an A-share candidate pipeline, not only a thermometer")
            break

    dated_market_markers = [
        "标普500",
        "纳斯达克综合",
        "道琼斯",
        "罗素2000",
        "VIX",
        "标普期货",
        "纳指期货",
        "道指期货",
        "罗素期货",
        "ES=F",
        "NQ=F",
        "YM=F",
        "RTY=F",
        "STOXX",
        "DAX",
        "FTSE",
        "CAC",
        "日经",
        "Nikkei",
        "KOSPI",
        "恒生",
        "Hang Seng",
        "台湾",
        "上证指数",
        "深证成指",
        "创业板指",
        "科创50",
    ]
    date_pattern = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")
    undated_lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        marker_hits = sum(1 for marker in dated_market_markers if contains_marker(line, marker))
        market_data_context = bool(re.search(r"[%％]|涨|跌|收|报|点|close|change", line, re.IGNORECASE))
        if marker_hits and (marker_hits >= 2 or market_data_context) and not date_pattern.search(line):
            undated_lines.append(line[:120])
    if undated_lines:
        failures.append(f"market index/future line missing returned date: {undated_lines[0]}")
    return failures


def validate_shadow_report(text: str, slot: str, *, public_delivery: bool = False) -> list[str]:
    failures: list[str] = []
    if slot == "am":
        required = ["# 跨市场早报", "美股", "A股"]
        expected_title = "# 跨市场早报"
    else:
        required = ["# 跨市场晚报", "A股", "美股"]
        expected_title = "# 跨市场晚报"
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    if not first_line.startswith(expected_title):
        failures.append(f"first non-empty line must start with: {expected_title}")
    for token in required:
        if token not in text:
            failures.append(f"missing required token: {token}")
    forbidden = [
        "production_delivery: enabled",
        "prod delivery",
        "A股盘后必须指导 美股盘前",
        "CN -> US",
        "validator | 未通过",
        "validator 未通过",
        "报告失败",
        "投递失败",
    ]
    for token in forbidden:
        if contains_marker(text, token):
            failures.append(f"forbidden production marker: {token}")
    if public_delivery:
        public_forbidden = [
            "MCP",
            "packet",
            "validator",
            "shadow_only",
            "production_delivery",
            "cron",
            "Resend",
            "JSON",
            "script",
            "tool",
            "工具调用",
            "工具失败",
            "血缘",
            "本稿状态",
            "prompt",
            "system prompt",
            "user prompt",
            "用户提示",
            "draft",
            "chain of thought",
            "思维过程",
            "推理过程",
            "作为AI",
            "以下是",
            "我将",
            "审稿",
            "二审",
            "数据缺口",
            "待补证据",
            "缺失数据",
            "数据缺失",
            "无法获取",
            "未获取",
            "N/A",
            "null",
            "暂无数据",
            "production",
            "source evidence",
            "source review",
            "evidence_state",
            "AI Infra universe",
            "AI Infra",
            "ranker",
            "headline risk",
            "beta hedge",
            "money gate",
            "regime",
            "原文验证状态",
            "原文验证",
            "# 美股日报",
            "# A股日报",
            "## 美股报告",
            "## A股报告",
        ]
        for token in public_forbidden:
            if contains_marker(text, token):
                failures.append(f"forbidden public-report marker: {token}")
        failures.extend(public_context_failures(text))
    return failures


def output_paths(output_dir: Path, slot: str) -> dict[str, Path]:
    prefix = f"cross_market_{slot}_shadow"
    return {
        "packet": output_dir / f"{prefix}_packet.json",
        "trajectory": output_dir / f"{prefix}_trajectory.jsonl",
        "report": output_dir / f"{prefix}.md",
        "meta": output_dir / f"{prefix}.meta.json",
    }


def snapshot_existing_outputs(output_dir: Path, slot: str) -> dict[Path, bytes | None]:
    snapshot: dict[Path, bytes | None] = {}
    for path in output_paths(output_dir, slot).values():
        try:
            snapshot[path] = path.read_bytes()
        except FileNotFoundError:
            snapshot[path] = None
    return snapshot


def restore_output_snapshot(snapshot: dict[Path, bytes | None]) -> None:
    for path, content in snapshot.items():
        if content is None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def write_outputs(output_dir: Path, packet: dict[str, Any], report: str, *, agent_backend: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = output_paths(output_dir, packet["slot"])
    packet_path = paths["packet"]
    trajectory_path = paths["trajectory"]
    report_path = paths["report"]
    meta_path = paths["meta"]

    packet_path.write_text(json.dumps(packet, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(report, encoding="utf-8")
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    trajectory = [
        {
            "ts": now,
            "step": "load_bricks",
            "tool": "main_strategy_v2_artifacts",
            "args": {"cn_date": packet["cn"]["report_date"], "us_date": packet["us"]["report_date"]},
            "result": {"cn_source": packet["cn"]["source_payload"], "us_source": packet["us"]["source_payload"]},
        },
        {
            "ts": now,
            "step": "lead_editor",
            "tool": agent_backend,
            "args": {"slot": packet["slot"], "direction": packet["direction"]},
            "result": {"report": str(report_path.relative_to(ROOT))},
        },
    ]
    if packet.get("_reviewer_backend"):
        trajectory.append(
            {
                "ts": now,
                "step": "editor_review",
                "tool": packet["_reviewer_backend"],
                "args": {
                    "slot": packet["slot"],
                    "provider": packet.get("_reviewer_provider") or "",
                    "model": packet.get("_reviewer_model") or "",
                },
                "result": {"report": str(report_path.relative_to(ROOT))},
            }
        )
    trajectory_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in trajectory) + "\n",
        encoding="utf-8",
    )
    meta = {
        "slot": packet["slot"],
        "direction": packet["direction"],
        "target_cn_date": packet.get("target_cn_date") or packet["cn"]["report_date"],
        "cn_date": packet["cn"]["report_date"],
        "us_date": packet["us"]["report_date"],
        "agent_backend": packet.get("_agent_backend") or agent_backend,
        "agent_model": packet.get("_agent_model") or "",
        "agent_provider": packet.get("_agent_provider") or "",
        "reviewer_backend": packet.get("_reviewer_backend") or "",
        "reviewer_model": packet.get("_reviewer_model") or "",
        "reviewer_provider": packet.get("_reviewer_provider") or "",
        "reviewer_primary_error": packet.get("_reviewer_primary_error") or "",
        "shadow_only": True,
        "generated_at": now,
        "script": Path(__file__).name,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report_path


def split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def email_subject(packet: dict[str, Any], delivery_mode: str) -> str:
    slot_label = "早报" if packet["slot"] == "am" else "晚报"
    target_date = packet.get("target_cn_date") or packet["cn"]["report_date"]
    prefix = "[TEST] " if delivery_mode == "test" else ""
    return f"{prefix}Hermes 跨市场{slot_label} - {target_date}"


def delivery_state_dir() -> Path:
    return Path(os.environ.get("CROSS_MARKET_DELIVERY_STATE_DIR", str(ROOT / "ops" / "state" / "cross_market_delivery")))


def delivery_recipient_key(delivery_mode: str, to: str | None, bcc: list[str] | None) -> str:
    if delivery_mode == "test":
        recipients = [to or "", *(bcc or [])]
        return "test:" + ",".join(sorted(part for part in recipients if part))
    return "prod:config-recipients"


def delivery_identity(packet: dict[str, Any], args: argparse.Namespace, subject: str, to: str | None, bcc: list[str] | None) -> dict[str, Any]:
    return {
        "slot": packet["slot"],
        "target_cn_date": packet.get("target_cn_date") or packet["cn"]["report_date"],
        "target_us_date": packet.get("target_us_date") or packet.get("us", {}).get("report_date"),
        "delivery_mode": args.delivery_mode,
        "recipient_key": delivery_recipient_key(args.delivery_mode, to, bcc),
        "subject": subject,
    }


def delivery_key(identity: dict[str, Any]) -> str:
    raw = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def reserve_delivery_once(packet: dict[str, Any], args: argparse.Namespace, subject: str, to: str | None, bcc: list[str] | None) -> tuple[Path | None, str, bool]:
    identity = delivery_identity(packet, args, subject, to, bcc)
    key = delivery_key(identity)
    if getattr(args, "allow_duplicate_email", False):
        return None, key, True
    state_dir = delivery_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / f"{key}.json"
    record = {
        **identity,
        "key": key,
        "status": "reserved",
        "reserved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path, key, False
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    return path, key, True


def complete_delivery_reservation(path: Path | None, *, ids: list[str], provider: str) -> None:
    if path is None:
        return
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        record = {}
    record.update(
        {
            "status": "sent",
            "provider": provider,
            "message_ids": ids,
            "sent_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
    )
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def release_delivery_reservation(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def send_email_if_requested(path: Path, packet: dict[str, Any], args: argparse.Namespace) -> list[str]:
    if not args.send_email:
        return []
    if args.delivery_dry_run:
        print(
            "delivery dry-run: "
            f"provider={args.email_provider} mode={args.delivery_mode} report={path}",
            file=sys.stderr,
        )
        return []

    recipients = split_recipients(args.test_recipient)
    to = None
    bcc: list[str] | None = None
    if args.delivery_mode == "test":
        if not recipients:
            raise SystemExit("test delivery needs --test-recipient or QUANT_TEST_RECIPIENT")
        to = recipients[0]
        bcc = recipients[1:]
    subject = email_subject(packet, args.delivery_mode)
    reservation_path, dedupe_key, should_send = reserve_delivery_once(packet, args, subject, to, bcc)
    if not should_send:
        print(f"cross-market email skipped: duplicate_delivery key={dedupe_key} record={reservation_path}")
        return []

    sys.path.insert(0, str(ROOT / "quant-research-v1" / "src"))
    from quant_bot.delivery.gmail import send_report_email, send_report_email_resend

    kwargs = {
        "report_path": path,
        "chart_paths": [],
        "to": to,
        "subject": subject,
        "bcc": bcc,
        "config_path": str(ROOT / "quant-research-v1" / "config.yaml"),
    }
    gmail_kwargs = {
        **kwargs,
        "credentials_path": ROOT / "quant-research-v1" / "credentials.json",
        "token_path": ROOT / "quant-research-v1" / "token.json",
    }
    if args.email_provider == "resend":
        try:
            ids = send_report_email_resend(**kwargs)
        except Exception as exc:
            if args.email_fallback_provider != "gmail":
                release_delivery_reservation(reservation_path)
                raise
            print(f"warn: Resend send failed; falling back to Gmail: {exc}", file=sys.stderr)
            try:
                ids = send_report_email(**gmail_kwargs)
            except Exception:
                release_delivery_reservation(reservation_path)
                raise
            complete_delivery_reservation(reservation_path, ids=ids, provider="gmail")
            print(f"cross-market email sent: provider=resend fallback=gmail ids={','.join(ids)}")
            return ids
    else:
        try:
            ids = send_report_email(**gmail_kwargs)
        except Exception:
            release_delivery_reservation(reservation_path)
            raise
    complete_delivery_reservation(reservation_path, ids=ids, provider=args.email_provider)
    print(f"cross-market email sent: provider={args.email_provider} ids={','.join(ids)}")
    return ids


def main() -> int:
    args = parse_args()
    cn_date, us_date = resolve_dates(args.slot, args.cn_date, args.us_date)
    report_root = args.report_root if args.report_root.is_absolute() else ROOT / args.report_root
    output_dir = args.output_dir or report_root / cn_date
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_snapshot = snapshot_existing_outputs(output_dir, args.slot)

    cn, cn_context_note = load_cn_context_artifact(report_root, args.slot, cn_date)
    us, us_context_note = load_us_context_artifact(report_root, us_date)
    missing = []
    if not cn.payload:
        missing.append(f"CN payload missing for {cn.report_date}: {cn.report_dir / 'main_strategy_v2_backtest.json'}")
    if not us.payload:
        missing.append(f"US payload missing for {us_date}: {us.report_dir / 'main_strategy_v2_backtest.json'}")
    if not cn.markdown:
        missing.append(f"CN markdown missing for {cn.report_date}: {cn.report_dir}")
    if not us.markdown:
        missing.append(f"US markdown missing for {us_date}: {us.report_dir}")
    if missing:
        raise SystemExit("\n".join(missing))

    packet = build_packet(args.slot, cn, us)
    annotate_target_context(
        packet,
        target_cn_date=cn_date,
        target_us_date=us_date,
        cn_context_note=cn_context_note,
        us_context_note=us_context_note,
    )
    if args.finance_search_prefetch == "on" and args.agent_backend == "hermes":
        attach_finance_search_prefetch(packet, target_cn_date=cn_date)
    if args.agent_backend == "off":
        report = deterministic_report(packet)
        backend_name = "deterministic_shadow"
    elif args.agent_backend == "hermes":
        try:
            report = call_hermes_agent(
                packet,
                timeout=args.timeout,
                hermes_bin=args.hermes_bin,
                model=args.hermes_model,
                provider=args.hermes_provider,
                max_turns=args.hermes_max_turns,
            )
            backend_name = packet.get("_agent_backend") or "hermes"
        except Exception as exc:
            if args.fallback_backend == "none":
                raise
            print(f"warn: Hermes agent failed; using {args.fallback_backend} fallback: {exc}", file=sys.stderr)
            report, backend_name = fallback_report(packet, args.fallback_backend, args.timeout, exc)
    else:
        report = call_agent(packet, args.timeout)
        backend_name = packet.get("_agent_backend") or "agent"

    if args.review_backend == "hermes" and args.agent_backend == "hermes":
        try:
            report = call_hermes_reviewer_with_fallback(
                packet,
                report,
                timeout=args.timeout,
                hermes_bin=args.hermes_bin,
                review_model=args.review_model or args.hermes_model,
                review_provider=args.review_provider or args.hermes_provider,
                fallback_model=args.hermes_model,
                fallback_provider=args.hermes_provider,
                max_turns=max(4, min(args.review_max_turns, 8)),
            )
        except Exception as exc:
            if args.send_email:
                raise RuntimeError(f"Hermes reviewer failed; refusing to email unreviewed report: {exc}") from exc
            print(f"warn: Hermes reviewer failed; keeping draft: {exc}", file=sys.stderr)

    if args.send_email:
        report = normalize_public_report_text(report, args.slot)
    report = annotate_market_snapshot_dates(report, packet)
    report = ensure_market_snapshot_section(report, packet)
    failures = validate_shadow_report(report, args.slot, public_delivery=args.send_email)
    if failures:
        restore_output_snapshot(output_snapshot)
        raise SystemExit("cross-market shadow validation failed:\n- " + "\n- ".join(failures))
    path = write_outputs(output_dir, packet, report, agent_backend=backend_name)
    print(f"cross-market {args.slot} shadow written: {path}")
    send_email_if_requested(path, packet, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
