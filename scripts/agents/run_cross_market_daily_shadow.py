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
MARKET_THERMOMETER_GROUPS = (
    ("美股大盘", ("^GSPC", "^IXIC", "^DJI", "^RUT")),
    ("A股大盘", ("000001.SS", "399001.SZ", "399006.SZ")),
    ("日韩大盘", ("^KS11", "^N225")),
)
MARKET_THERMOMETER_SYMBOLS = {
    symbol for _, symbols in MARKET_THERMOMETER_GROUPS for symbol in symbols
}
MARKET_TAIL_GROUPS = (
    ("美股波动/ETF", ("^VIX", "SPY", "QQQ", "IWM", "TLT")),
    ("美股期货", ("ES=F", "NQ=F", "YM=F", "RTY=F")),
    ("欧洲大盘", ("^STOXX50E", "^GDAXI", "^FTSE", "^FCHI")),
    ("亚洲补充", ("^HSI", "^TWII")),
    ("A股/科创参考", ("000688.SS",)),
    ("商品/汇率/利率", ("CL=F", "BZ=F", "GC=F", "GLD", "USDCNH=X", "DX-Y.NYB", "^TNX")),
)
MIN_PUBLIC_IV_RANK_OBS = 30
MANAGED_REPORT_SECTION_PREFIXES = (
    "## 全球市场温度",
    "## 全球温度",
    "## 宏观数据温度计",
    "## 顶部宏观数据温度计",
    "## 宏观事件 Headlines",
    "## 宏观事件与产业新闻",
    "## 宏观与产业",
    "## 可核验宏观与产业",
    "## 可核验宏观事件",
    "## 可核验新闻",
    "## 美股执行标的",
    "## 美股标的",
    "## 美股期权关注标的",
    "## 美股期权观察标的",
    "## 美股 OTM skew",
    "## SEC 13F 机构持仓快照",
    "## 附表：其他跨市场数据",
    "## 附表：外围资产",
    "## 附表：全球风险",
    "## 附表：全球市场",
    "## 尾部市场附表",
    "## 全球风险与跨资产",
    "## A股科创板候选管线",
    "## 科创板候选",
    "## 科创板不是",
)
EXECUTION_DIARY_SECTION_PREFIXES = (
    "## 跨市场主线",
    "## 传导到A股",
    "## 传到 A股",
    "## 今天的执行剧本",
    "## 下一交易窗口执行剧本",
    "## 失效条件和下一步检查",
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


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


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
    parser.add_argument(
        "--sec-13f-prefetch",
        choices=["on", "off"],
        default=os.environ.get("CROSS_MARKET_SEC_13F_PREFETCH", "on"),
        help="Best-effort local SEC 13F file summary for filings added in the recent window.",
    )
    parser.add_argument(
        "--sec-13f-lookback-hours",
        type=float,
        default=float(os.environ.get("CROSS_MARKET_SEC_13F_LOOKBACK_HOURS", "12")),
    )
    parser.add_argument(
        "--publish-openclaw",
        action="store_true",
        default=env_flag("QUANT_OPENCLAW_PUBLISH"),
        help="Publish the finished report to a remote OpenClaw inbox after validation.",
    )
    parser.add_argument(
        "--openclaw-mode",
        choices=["file", "agent", "message", "all"],
        default=os.environ.get("QUANT_OPENCLAW_MODE", "file"),
        help="OpenClaw publish mode. agent registers the report in an OpenClaw agent session.",
    )
    parser.add_argument("--openclaw-host", default=os.environ.get("QUANT_OPENCLAW_HOST", "100.109.146.30"))
    parser.add_argument("--openclaw-user", default=os.environ.get("QUANT_OPENCLAW_USER", "ivena"))
    parser.add_argument(
        "--openclaw-root",
        default=os.environ.get("QUANT_OPENCLAW_REMOTE_ROOT", "/home/ivena/.openclaw/quant-stack"),
    )
    parser.add_argument("--openclaw-identity-file", default=os.environ.get("QUANT_OPENCLAW_IDENTITY_FILE", ""))
    parser.add_argument("--openclaw-agent", default=os.environ.get("QUANT_OPENCLAW_AGENT", "main"))
    parser.add_argument("--openclaw-agent-session-key", default=os.environ.get("QUANT_OPENCLAW_AGENT_SESSION_KEY", ""))
    parser.add_argument(
        "--openclaw-agent-timeout",
        type=int,
        default=int(os.environ.get("QUANT_OPENCLAW_AGENT_TIMEOUT", "180")),
    )
    parser.add_argument(
        "--openclaw-agent-deliver",
        action="store_true",
        default=env_flag("QUANT_OPENCLAW_AGENT_DELIVER"),
        help="Allow OpenClaw agent replies to be delivered to a configured channel.",
    )
    parser.add_argument("--openclaw-reply-channel", default=os.environ.get("QUANT_OPENCLAW_REPLY_CHANNEL", ""))
    parser.add_argument("--openclaw-reply-account", default=os.environ.get("QUANT_OPENCLAW_REPLY_ACCOUNT", ""))
    parser.add_argument("--openclaw-reply-to", default=os.environ.get("QUANT_OPENCLAW_REPLY_TO", ""))
    parser.add_argument("--openclaw-message-channel", default=os.environ.get("QUANT_OPENCLAW_MESSAGE_CHANNEL", ""))
    parser.add_argument("--openclaw-message-target", default=os.environ.get("QUANT_OPENCLAW_MESSAGE_TARGET", ""))
    parser.add_argument(
        "--openclaw-allow-duplicate-event",
        action="store_true",
        default=env_flag("QUANT_OPENCLAW_ALLOW_DUPLICATE_EVENT"),
    )
    parser.add_argument(
        "--openclaw-required",
        action="store_true",
        default=env_flag("QUANT_OPENCLAW_REQUIRED"),
        help="Fail the daily task if OpenClaw publishing fails.",
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


def cn_pipeline_priority(row: dict[str, Any]) -> tuple[int, int, float, str]:
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


def compact_cn_pipeline_candidate(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").upper()
    return {
        "symbol": symbol,
        "name": row.get("name") or symbol,
        "board": "科创板" if is_cn_star_symbol(symbol) else row.get("board") or "",
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


def select_cn_pipeline_candidates(payload: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    ranker = payload.get("cn_opportunity_ranker") if isinstance(payload.get("cn_opportunity_ranker"), dict) else {}
    rows = ranker.get("all_rows") if isinstance(ranker.get("all_rows"), list) else []
    if not rows:
        rows = get_path(payload, "cn", "current", default=[])
    candidates = [row for row in rows if isinstance(row, dict) and row.get("symbol")]
    candidates.sort(key=cn_pipeline_priority)
    selected = candidates[:limit]
    if not any(is_cn_star_symbol(row.get("symbol")) for row in selected):
        star_rows = [row for row in candidates if is_cn_star_symbol(row.get("symbol"))]
        if star_rows:
            selected = (selected[: max(limit - 1, 0)] + star_rows[:1])[:limit]
    return [compact_cn_pipeline_candidate(row) for row in selected]


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


def compact_float(value: Any, digits: int = 4) -> float | None:
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def compact_numeric_range(value: Any) -> list[float] | None:
    if not isinstance(value, list) or len(value) < 2:
        return None
    low = compact_float(value[0], 2)
    high = compact_float(value[1], 2)
    if low is None or high is None:
        return None
    return [low, high]


def compact_gamma_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row.get("symbol"),
        "state": row.get("state"),
        "curve_state": row.get("gex_curve_state"),
        "flip_regime": row.get("gex_flip_regime"),
        "spot": compact_float(row.get("spot"), 2),
        "spot_date": row.get("spot_price_date"),
        "zero_gamma_band": compact_numeric_range(row.get("zero_gamma_band")),
        "positive_pin_zone": compact_numeric_range(row.get("positive_gex_pin_zone")),
        "negative_accel_zone": compact_numeric_range(row.get("negative_gex_accel_zone")),
        "call_wall": compact_float(row.get("call_wall_strike"), 2),
        "put_wall": compact_float(row.get("put_wall_strike"), 2),
        "dealer_pressure_proxy": compact_float(row.get("dealer_pressure_proxy"), 4),
        "management_signal": row.get("management_signal"),
    }


def compact_option_verdict(symbol: str, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "effective_date": row.get("effective_date") or row.get("requested_date"),
        "verdict": row.get("verdict"),
        "iv_ann": compact_float(row.get("iv_ann"), 4),
        "iv_rank_pct": compact_float(row.get("iv_rank_pct"), 2),
        "iv_rank_n": row.get("iv_rank_n"),
        "iv_hv": compact_float(row.get("iv_hv"), 4),
        "vrp": compact_float(row.get("vrp"), 4),
        "pc_ratio_z": compact_float(row.get("pc_ratio_z"), 4),
        "skew_z": compact_float(row.get("skew_z"), 4),
    }


def option_row_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("ticker") or "").upper()


def compact_option_anomaly_row(row: dict[str, Any]) -> dict[str, Any]:
    squeeze = compact_float(row.get("short_squeeze_score"), 2)
    pressure = compact_float(row.get("selling_pressure_score"), 2)
    if squeeze is not None and squeeze > 0 and (pressure is None or squeeze >= pressure):
        signal = "far-OTM call squeeze"
        score = squeeze
    elif pressure is not None and pressure > 0:
        signal = "far-OTM put pressure"
        score = pressure
    else:
        signal = "far-OTM flow"
        score = compact_float(row.get("score"), 2)
    return {
        "symbol": option_row_symbol(row),
        "effective_date": row.get("as_of") or row.get("source_date") or row.get("requested_date"),
        "signal": signal,
        "score": score,
        "spot": compact_float(row.get("spot_close"), 2),
        "far_otm_call_volume": compact_float(row.get("far_otm_call_volume"), 0),
        "far_otm_call_vol_oi_ratio": compact_float(row.get("far_otm_call_vol_oi_ratio"), 2),
        "far_otm_put_volume": compact_float(row.get("far_otm_put_volume"), 0),
        "far_otm_put_vol_oi_ratio": compact_float(row.get("far_otm_put_vol_oi_ratio"), 2),
        "pc_ratio_z": compact_float(row.get("pc_ratio_z"), 4),
        "skew_z": compact_float(row.get("skew_z"), 4),
    }


def compact_option_tenor_signal(row: dict[str, Any]) -> dict[str, Any]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    compact_evidence = {
        key: evidence.get(key)
        for key in (
            "weekly_far_otm_call",
            "monthly_far_otm_call",
            "long_horizon_far_otm_call",
            "weekly_far_otm_put",
            "long_horizon_far_otm_put",
            "weekly_pc_ratio",
            "tenors",
            "ratios",
        )
        if evidence.get(key) is not None
    }
    return {
        "symbol": option_row_symbol(row),
        "effective_date": row.get("as_of") or row.get("source_date") or row.get("requested_date"),
        "pattern": row.get("pattern"),
        "score": compact_float(row.get("score"), 2),
        "guidance": row.get("guidance"),
        "evidence": compact_evidence,
    }


def option_verdict_long_tenor(row: dict[str, Any]) -> bool:
    verdict = str(row.get("verdict") or "")
    return any(marker in verdict for marker in ("信仰久期长", "远月", "LEAPS", "leaps"))


def option_iv_rank_observation_count(row: dict[str, Any]) -> int:
    try:
        return int(row.get("iv_rank_n") or 0)
    except (TypeError, ValueError):
        return 0


def option_iv_rank_public_reliable(row: dict[str, Any]) -> bool:
    return option_iv_rank_observation_count(row) >= MIN_PUBLIC_IV_RANK_OBS


def option_attention_verdict_reason(symbol: str, row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    skew_z = compact_float(row.get("skew_z"), 4)
    iv_rank = compact_float(row.get("iv_rank_pct"), 2)
    iv_hv = compact_float(row.get("iv_hv"), 4)
    if skew_z is not None and abs(skew_z) >= 1.0:
        reasons.append("OTM skew 偏离")
    if iv_rank is not None and iv_rank <= 20 and option_iv_rank_public_reliable(row):
        reasons.append("LEAPS IV 低位")
    if option_verdict_long_tenor(row):
        reasons.append("远月 LEAPS 久期")
    if (
        (iv_rank is not None and iv_rank >= 70 and option_iv_rank_public_reliable(row))
        or (iv_hv is not None and iv_hv >= 1.25)
    ):
        reasons.append("高 IV 等回落")
    return reasons


def option_attention_reading(reason: str, row: dict[str, Any]) -> str:
    skew_z = compact_float(row.get("skew_z"), 4)
    iv_rank = compact_float(row.get("iv_rank_pct"), 2)
    iv_hv = compact_float(row.get("iv_hv"), 4)
    iv_rank_n = option_iv_rank_observation_count(row)
    if "OTM skew" in reason:
        if skew_z is not None and skew_z >= 1.0:
            return "put skew 抬升，仓位要等价格确认并收紧止损"
        if skew_z is not None and skew_z <= -1.0:
            return "skew 向上行/挤压侧偏，避免追高"
        return "skew 偏离，作为入场节奏约束"
    if "LEAPS IV" in reason:
        suffix = f"IV rank {iv_rank:.0f}%" if iv_rank is not None else "IV 低位"
        if iv_rank_n:
            suffix = f"{suffix}，样本N={iv_rank_n}"
        return f"{suffix}，只作远月方向成本观察"
    if "远月" in reason:
        return "远月久期存在，作为趋势信念观察"
    if "高 IV" in reason:
        if iv_rank is not None and iv_rank_n >= MIN_PUBLIC_IV_RANK_OBS:
            return f"IV rank {iv_rank:.0f}%（样本N={iv_rank_n}），等回落或用股票表达"
        if iv_hv is not None:
            return f"IV/HV {iv_hv:.2f}x，等波动回落或用股票表达"
        return "IV 分位偏高，避免把股票观点写成期权追价"
    return "期权读数只约束股票仓位和节奏"


def option_attention_candidate(
    symbol: str,
    verdicts: dict[str, Any],
    reason: str,
    *,
    priority: int,
    score: float = 0.0,
    reading: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    verdict = verdicts.get(symbol) if isinstance(verdicts.get(symbol), dict) else {}
    row = {**verdict, **(extra or {})}
    return {
        "_priority": priority,
        "_score": score,
        "symbol": symbol,
        "reason": reason,
        "effective_date": row.get("effective_date") or row.get("requested_date") or row.get("as_of"),
        "iv_ann": compact_float(row.get("iv_ann"), 4),
        "iv_rank_pct": compact_float(row.get("iv_rank_pct"), 2),
        "iv_rank_n": option_iv_rank_observation_count(row) or None,
        "iv_hv": compact_float(row.get("iv_hv"), 4),
        "pc_ratio_z": compact_float(row.get("pc_ratio_z"), 4),
        "skew_z": compact_float(row.get("skew_z"), 4),
        "reading": reading or option_attention_reading(reason, row),
    }


def merge_option_attention_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    def duplicate_reason(existing_reasons: list[str], new_reason: str) -> bool:
        if new_reason in existing_reasons:
            return True
        if new_reason.endswith("排行"):
            base = new_reason.removesuffix(" 排行")
            return any(reason.startswith(base) for reason in existing_reasons)
        return False

    def refine_reasons(existing_reasons: list[str], new_reason: str) -> list[str]:
        if new_reason.startswith("LEAPS IV 低位"):
            existing_reasons = [reason for reason in existing_reasons if reason != "LEAPS IV 排行"]
        if new_reason.startswith("OTM skew 偏离"):
            existing_reasons = [reason for reason in existing_reasons if reason != "OTM skew 排行"]
        if new_reason and not duplicate_reason(existing_reasons, new_reason):
            existing_reasons.append(new_reason)
        return existing_reasons

    merged: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: (item.get("_priority", 999), -float(item.get("_score") or 0.0), item["symbol"])):
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        existing = merged.get(symbol)
        if existing is None:
            merged[symbol] = dict(row)
            continue
        reasons = [part.strip() for part in str(existing.get("reason") or "").split("/") if part.strip()]
        new_reason = str(row.get("reason") or "").strip()
        reasons = refine_reasons(reasons, new_reason)
        existing["reason"] = " / ".join(reasons)
        reading = str(row.get("reading") or "").strip()
        if reading and reading not in str(existing.get("reading") or ""):
            existing["reading"] = f"{existing.get('reading')}; {reading}"
        for key in ("effective_date", "iv_ann", "iv_rank_pct", "iv_rank_n", "iv_hv", "pc_ratio_z", "skew_z"):
            if existing.get(key) is None and row.get(key) is not None:
                existing[key] = row.get(key)
    selected = list(merged.values())[:limit]
    for row in selected:
        row.pop("_priority", None)
        row.pop("_score", None)
    return selected


def select_us_options_attention(
    payload: dict[str, Any],
    actions: list[dict[str, Any]],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    verdicts = payload.get("options_verdicts") if isinstance(payload.get("options_verdicts"), dict) else {}
    candidates: list[dict[str, Any]] = []

    anomaly_rows = payload.get("options_anomaly_rows") if isinstance(payload.get("options_anomaly_rows"), list) else []
    for raw in anomaly_rows:
        if not isinstance(raw, dict):
            continue
        row = compact_option_anomaly_row(raw)
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        signal = str(row.get("signal") or "far-OTM flow")
        score = float(row.get("score") or 0.0)
        candidates.append(
            option_attention_candidate(
                symbol,
                verdicts,
                "远 OTM 异常",
                priority=0,
                score=score,
                reading=f"{signal} 触发，只作股票节奏/风险约束",
                extra=row,
            )
        )

    tenor_signals = payload.get("options_tenor_signals") if isinstance(payload.get("options_tenor_signals"), list) else []
    for raw in tenor_signals:
        if not isinstance(raw, dict):
            continue
        row = compact_option_tenor_signal(raw)
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        pattern = fmt(row.get("pattern"), default="tenor signal")
        score = float(row.get("score") or 0.0)
        candidates.append(
            option_attention_candidate(
                symbol,
                verdicts,
                "LEAPS tenor 异动",
                priority=5,
                score=score,
                reading=f"{pattern}，远月/跨 tenor 只作 0R context",
                extra=row,
            )
        )

    for symbol, raw in verdicts.items():
        if not isinstance(raw, dict):
            continue
        symbol_key = str(symbol or "").upper()
        if not symbol_key:
            continue
        reasons = option_attention_verdict_reason(symbol_key, raw)
        for reason in reasons:
            priority = 20
            score = 0.0
            if reason.startswith("OTM skew"):
                priority = 10
                score = abs(compact_float(raw.get("skew_z"), 4) or 0.0)
            elif reason.startswith("LEAPS IV"):
                priority = 15
                iv_rank = compact_float(raw.get("iv_rank_pct"), 2)
                score = 100.0 - (iv_rank if iv_rank is not None else 100.0)
            elif reason.startswith("远月"):
                priority = 18
                score = 1.0
            elif reason.startswith("高 IV"):
                priority = 30
                score = compact_float(raw.get("iv_rank_pct"), 2) or compact_float(raw.get("iv_hv"), 4) or 0.0
            candidates.append(
                option_attention_candidate(
                    symbol_key,
                    verdicts,
                    reason,
                    priority=priority,
                    score=score,
                    reading=option_attention_reading(reason, raw),
                )
            )

    if verdicts:
        ranked_by_iv = sorted(
            (
                (str(symbol).upper(), row, compact_float(row.get("iv_rank_pct"), 2))
                for symbol, row in verdicts.items()
                if isinstance(row, dict) and compact_float(row.get("iv_rank_pct"), 2) is not None
                and option_iv_rank_public_reliable(row)
                and float(compact_float(row.get("iv_rank_pct"), 2) or 999.0) <= 20.0
            ),
            key=lambda item: item[2] if item[2] is not None else 999.0,
        )
        for symbol, row, iv_rank in ranked_by_iv[:5]:
            candidates.append(
                option_attention_candidate(
                    symbol,
                    verdicts,
                    "LEAPS IV 排行",
                    priority=9,
                    score=100.0 - float(iv_rank or 100.0),
                    reading=option_attention_reading("LEAPS IV 排行", row),
                )
            )
        ranked_by_skew = sorted(
            (
                (str(symbol).upper(), row, abs(compact_float(row.get("skew_z"), 4) or 0.0))
                for symbol, row in verdicts.items()
                if isinstance(row, dict)
                and compact_float(row.get("skew_z"), 4) is not None
                and abs(float(compact_float(row.get("skew_z"), 4) or 0.0)) >= 1.0
            ),
            key=lambda item: -item[2],
        )
        for symbol, row, score in ranked_by_skew[:5]:
            candidates.append(
                option_attention_candidate(
                    symbol,
                    verdicts,
                    "OTM skew 排行",
                    priority=10,
                    score=score,
                    reading=option_attention_reading("OTM skew 排行", row),
                )
            )

    return merge_option_attention_rows(candidates, limit=limit)


def compact_us_option_context(payload: dict[str, Any], actions: list[dict[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    action_symbols = [
        str(row.get("symbol") or "").upper()
        for row in actions
        if row.get("symbol")
    ]
    requested_symbols = []
    for symbol in ["SPY", "QQQ", "SMH", *action_symbols]:
        if symbol and symbol not in requested_symbols:
            requested_symbols.append(symbol)

    gamma = payload.get("gamma_spring") if isinstance(payload.get("gamma_spring"), dict) else {}
    gamma_rows = gamma.get("rows") if isinstance(gamma.get("rows"), list) else []
    gamma_by_symbol = {
        str(row.get("symbol") or "").upper(): row
        for row in gamma_rows
        if isinstance(row, dict) and row.get("symbol")
    }
    selected_gamma = [
        compact_gamma_row(gamma_by_symbol[symbol])
        for symbol in requested_symbols
        if symbol in gamma_by_symbol
    ][:limit]

    ledger = payload.get("option_shadow_ledger") if isinstance(payload.get("option_shadow_ledger"), dict) else {}
    ledger_summary = ledger.get("summary") if isinstance(ledger.get("summary"), dict) else {}
    all_real = (
        ledger_summary.get("all_options_alpha_real_bid_ask")
        if isinstance(ledger_summary.get("all_options_alpha_real_bid_ask"), dict)
        else {}
    )
    overall_long = ledger_summary.get("overall_long") if isinstance(ledger_summary.get("overall_long"), dict) else {}

    anomaly_rows = payload.get("options_anomaly_rows") if isinstance(payload.get("options_anomaly_rows"), list) else []
    tenor_signals = payload.get("options_tenor_signals") if isinstance(payload.get("options_tenor_signals"), list) else []

    verdicts = payload.get("options_verdicts") if isinstance(payload.get("options_verdicts"), dict) else {}
    selected_verdicts = []
    for symbol in requested_symbols:
        row = verdicts.get(symbol)
        if isinstance(row, dict):
            selected_verdicts.append(compact_option_verdict(symbol, row))
        if len(selected_verdicts) >= limit:
            break

    return {
        "contract": "US options and Gamma data are stock risk/entry context, not option trade instructions.",
        "gamma_effective_date": gamma.get("effective_date") or gamma.get("as_of"),
        "gamma_sign_convention": gamma.get("sign_convention"),
        "gamma_rows": selected_gamma,
        "option_shadow_ledger": {
            "status": ledger.get("status"),
            "rows_with_legs": ledger.get("rows_with_legs"),
            "all_real_bid_ask_resolved_count": ledger.get("all_real_bid_ask_resolved_count"),
            "all_real_bid_ask_unresolved_count": ledger.get("all_real_bid_ask_unresolved_count"),
            "overall_long_lcb80_pct": compact_float(overall_long.get("lcb80_pct"), 2),
            "all_options_alpha_lcb80_pct": compact_float(all_real.get("lcb80_pct"), 2),
            "all_options_alpha_win_rate": compact_float(all_real.get("win_rate"), 4),
        },
        "options_anomaly_radar": {
            "row_count": len(anomaly_rows),
            "status": "no_trigger" if not anomaly_rows else "triggered",
        },
        "options_tenor_radar": {
            "signal_count": len(tenor_signals),
            "status": "no_signal" if not tenor_signals else "triggered",
        },
        "options_anomaly_rows": [
            compact_option_anomaly_row(row)
            for row in anomaly_rows
            if isinstance(row, dict) and option_row_symbol(row)
        ][:limit],
        "options_tenor_signals": [
            compact_option_tenor_signal(row)
            for row in tenor_signals
            if isinstance(row, dict) and option_row_symbol(row)
        ][:limit],
        "options_verdicts": selected_verdicts,
        "options_attention_watchlist": select_us_options_attention(payload, actions, limit=limit),
    }


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
        out["pipeline_candidates"] = select_cn_pipeline_candidates(payload)
    if market_key == "us":
        out["option_context"] = compact_us_option_context(payload, actions)
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


def option_context_markdown(context: dict[str, Any]) -> str:
    if not isinstance(context, dict) or not context:
        return "-"

    ledger = context.get("option_shadow_ledger") if isinstance(context.get("option_shadow_ledger"), dict) else {}
    anomaly = context.get("options_anomaly_radar") if isinstance(context.get("options_anomaly_radar"), dict) else {}
    tenor = context.get("options_tenor_radar") if isinstance(context.get("options_tenor_radar"), dict) else {}
    lines = [
        f"- Gamma 数据日: {fmt(context.get('gamma_effective_date'))}; 期权/Gamma 只作股票仓位、入场和风险节奏上下文。",
        (
            "- 期权验证: "
            f"legs={fmt(ledger.get('rows_with_legs'))}, "
            f"resolved={fmt(ledger.get('all_real_bid_ask_resolved_count'))}, "
            f"unresolved={fmt(ledger.get('all_real_bid_ask_unresolved_count'))}, "
            f"LCB80={fmt(ledger.get('all_options_alpha_lcb80_pct'))}%。"
        ),
        (
            "- 异常/tenor: "
            f"远 OTM 异常={fmt(anomaly.get('row_count'))}; "
            f"跨 tenor 信号={fmt(tenor.get('signal_count'))}。"
        ),
    ]
    gamma_rows = context.get("gamma_rows") if isinstance(context.get("gamma_rows"), list) else []
    if gamma_rows:
        lines.extend([
            "",
            "| Symbol | Gamma state | Curve | Spot | Zero gamma | Walls | Mgmt |",
            "|---|---|---|---:|---|---|---|",
        ])
        for row in gamma_rows[:8]:
            zero = row.get("zero_gamma_band")
            zero_text = "-".join(str(item) for item in zero) if isinstance(zero, list) else "-"
            walls = f"{fmt(row.get('call_wall'))}/{fmt(row.get('put_wall'))}"
            lines.append(
                "| {symbol} | {state} | {curve} | {spot} | {zero} | {walls} | {signal} |".format(
                    symbol=fmt(row.get("symbol")),
                    state=fmt(row.get("state")),
                    curve=fmt(row.get("curve_state")),
                    spot=fmt(row.get("spot")),
                    zero=zero_text,
                    walls=walls,
                    signal=fmt(row.get("management_signal")),
                )
            )
    verdicts = context.get("options_verdicts") if isinstance(context.get("options_verdicts"), list) else []
    if verdicts:
        lines.extend([
            "",
            "| Symbol | 期权定位 | 日期 | IV/HV | Skew z |",
            "|---|---|---|---:|---:|",
        ])
        for row in verdicts[:8]:
            lines.append(
                "| {symbol} | {verdict} | {date} | {iv_hv} | {skew} |".format(
                    symbol=fmt(row.get("symbol")),
                    verdict=fmt(row.get("verdict")),
                    date=fmt(row.get("effective_date")),
                    iv_hv=fmt(row.get("iv_hv")),
                    skew=fmt(row.get("skew_z")),
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
            "returns": ["gross_r", "actions", "risk_regime", "data_dates", "option_context"],
            "agent_use": "Find the US drivers, including options/Gamma context, that can constrain or relax the next CN session.",
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
                "Use for the managed market snapshot: top thermometer is limited to US broad indices, "
                "CN broad indices, KOSPI, and Nikkei 225; futures, Europe, VIX, oil, gold, FX, rates, "
                "and STAR index data belong in the tail appendix table when available."
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
        {
            "name": "finance-search.quant_stack_sec_13f_recent",
            "kind": "mcp_tool",
            "market": "US institutional holdings",
            "source": "Hermes MCP server: finance-search + local SEC 13F files",
            "returns": ["recent_13f_files", "new_positions_top5", "increases_top5", "decreases_top5"],
            "agent_use": (
                "Read local 13F information tables added in the last 12 hours. "
                "Use only as delayed institutional positioning context; do not treat 13F changes as real-time flow."
            ),
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
            "Include the managed macro thermometer: top only US broad indices, CN broad indices, KOSPI, Nikkei 225; put all other snapshots in the tail table.",
            "Use packet.us.option_context to explain US options/Gamma pressure, OTM skew / LEAPS IV watch names, signal absence, and stock risk limits.",
            "Explain the US -> CN transmission path and where it can fail.",
            "Map AI/semiconductor signals into A-share execution, including 科创板/STAR candidates when verifiable.",
            "Translate the driver into CN execution limits, sector priority, and watch items.",
            "Show actionable US/CN facts only when they clarify the story.",
            "End with risk, invalidation, and next-session checks in investor-readable language.",
        ]
    return [
        "Review CN post-market action as feedback on prior US-to-CN transmission.",
        "Summarize global and US pre-market context from US/global facts, not from CN direction; use the managed top thermometer plus tail appendix shape.",
        "Use packet.us.option_context to explain US options/Gamma pressure, OTM skew / LEAPS IV watch names, signal absence, and stock risk limits.",
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
                    "Build the managed market snapshot: top thermometer only has US broad indices, CN broad "
                    "indices, KOSPI, and Nikkei 225; US futures, Europe, VIX/rates proxy, oil, gold, USD/CNH, "
                    "Hong Kong/Taiwan, and STAR index data go to the report-tail appendix table."
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
            {
                "tool": "finance-search.quant_stack_sec_13f_recent",
                "window": "12h",
                "purpose": (
                    "Read local SEC 13F information-table files added in the last 12 hours and summarize "
                    "new positions, increases, and decreases top 5 by manager."
                ),
            },
        ],
        "public_output_rule": (
            "The public report must keep the top macro thermometer narrow: US broad indices, CN broad "
            "indices, KOSPI, and Nikkei 225 only. Returned values for oil, gold, at least one US equity "
            "future, Europe/other Asia, VIX, FX, rates, and STAR index data belong in the report-tail "
            "appendix table. Put macro/news headlines immediately under the thermometer when source "
            "titles are returned. Every cited index or future must show its returned date, especially "
            "cross-timezone markets. If a feed or symbol is unavailable, omit it; do not print a "
            "missing-data list or tool failure note. Do not cite India/Sensex/Nifty indices. If recent "
            "13F data is present, attach it as delayed institutional positioning context, not a trade signal."
        ),
    }


def build_cn_universe_requirement() -> dict[str, Any]:
    return {
        "scope": (
            "A-share semiconductor and AI hardware mapping must include concrete STAR Market/科创板 "
            "688xxx candidates inside the ordinary CN selection pipeline, not as a separate STAR-only block."
        ),
        "board_policy": [
            "Do not treat main-board A-shares as the whole CN universe.",
            "Select concrete 688xxx.SH names from packet.cn.actions and packet.cn.pipeline_candidates when available.",
            "Use 0R/active_watch language when a STAR name is not executable yet, but narrate it as part of the A-share pipeline.",
            "Do not describe 科创板 only as a temperature gauge; tie it to named candidates inside the A-share execution/watch story.",
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


def market_snapshot_public_rows(packet: dict[str, Any]) -> list[dict[str, Any]]:
    prefetch = packet.get("finance_search_prefetch") if isinstance(packet.get("finance_search_prefetch"), dict) else {}
    raw_rows = prefetch.get("market_rows") if isinstance(prefetch.get("market_rows"), list) else []
    return [
        row for row in raw_rows
        if isinstance(row, dict) and row.get("date") and public_index_marker_allowed(row)
    ]


def render_market_row(group: str, row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "")
    return "| {group} | {label} | {date} | {close} | {change} |".format(
        group=group,
        label=fmt(row.get("label") or symbol),
        date=fmt(row.get("date")),
        close=fmt_market_value(row.get("close")),
        change=fmt_market_change_pct(row.get("change_pct")),
    )


def render_market_table(title: str, grouped_symbols: tuple[tuple[str, tuple[str, ...]], ...], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""

    by_symbol = {str(row.get("symbol") or ""): row for row in rows}
    consumed: set[str] = set()
    lines = [
        title,
        "| 类别 | 指标 | 日期 | 最新/收盘 | 涨跌幅 |",
        "|---|---|---|---:|---:|",
    ]
    for group, symbols in grouped_symbols:
        for symbol in symbols:
            row = by_symbol.get(symbol)
            if not row:
                continue
            consumed.add(symbol)
            lines.append(render_market_row(group, row))
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if symbol in consumed:
            continue
        lines.append(render_market_row("其他", row))
    if len(lines) == 3:
        return ""
    return "\n".join(lines)


def render_market_snapshot_section(packet: dict[str, Any]) -> str:
    rows = [
        row for row in market_snapshot_public_rows(packet)
        if str(row.get("symbol") or "") in MARKET_THERMOMETER_SYMBOLS
    ]
    return render_market_table("## 宏观数据温度计", MARKET_THERMOMETER_GROUPS, rows)


def render_market_tail_section(packet: dict[str, Any]) -> str:
    rows = [
        row for row in market_snapshot_public_rows(packet)
        if str(row.get("symbol") or "") not in MARKET_THERMOMETER_SYMBOLS
    ]
    return render_market_table("## 附表：其他跨市场数据", MARKET_TAIL_GROUPS, rows)


def public_cell(value: Any, default: str = "-") -> str:
    text = fmt(value, default=default)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace("|", "/")


CONTENTFUL_NEWS_MARKERS = (
    "fed",
    "rate",
    "inflation",
    "ai",
    "chip",
    "semiconductor",
    "openai",
    "data center",
    "datacenter",
    "earnings",
    "gold",
    "silver",
    "oil",
    "tariff",
    "china",
    "chinese",
    "geopolit",
)


def is_contentful_macro_news(title: str) -> bool:
    lower = title.lower()
    if not any(marker in lower for marker in CONTENTFUL_NEWS_MARKERS):
        return False
    pure_move_patterns = (
        r"stock market today:?\s*(dow|s&p|nasdaq|futures|stocks)",
        r"^(dow|s&p 500|nasdaq|stock)\s+futures\s+(rise|fall|drop|dip|slip|edge)",
    )
    if any(re.search(pattern, lower) for pattern in pure_move_patterns):
        return any(marker in lower for marker in ("fed", "ai", "chip", "openai", "gold", "oil", "china", "data center"))
    return True


def translate_macro_news_title(title: Any) -> str:
    text = public_cell(title, default="")
    lower = text.lower()
    if "ai buildout costs" in lower and "fed" in lower:
        return "AI 建设成本和美联储利率预期压制科技风险偏好，纳指期货承压"
    if "chip rout" in lower and "openai" in lower:
        return "芯片链抛售叠加 OpenAI IPO 延后，科技股盘前继续承压"
    if "chip stock selloff" in lower:
        tickers = re.findall(r"\b[A-Z]{2,5}\b", text)
        suffix = f"，焦点股包括 {'/'.join(tickers[:5])}" if tickers else ""
        return f"芯片股抛售后的修复交易回到盘前视野{suffix}"
    if "megacap tech selloff" in lower:
        return "大型科技股抛售后，美股期货仍受 AI 和高估值压力牵制"
    if "chinese stocks" in lower and "data center" in lower:
        return "美国数据中心建设把部分中国供应链股票重新推到产业映射视野"
    if "stock market outlook" in lower and "ai earnings" in lower:
        return "美股后续方向仍取决于 AI 业绩兑现和美联储利率风险"
    if "gold falls" in lower and "macro" in lower:
        return "黄金回落，但利率和美元等宏观压力仍未消退"
    if "gold" in lower and "silver" in lower and "fed" in lower:
        return "美联储利率重定价压过避险需求，金银同步走弱"
    if "oil" in lower and ("brent" in lower or "wti" in lower):
        return "原油价格波动继续影响通胀预期和成长股估值"
    if "fed" in lower:
        return "美联储利率路径仍是全球风险资产的核心变量"
    if "rate" in lower or "inflation" in lower:
        return "利率和通胀预期继续影响风险资产定价"
    if "earnings" in lower:
        return "企业财报预期成为市场风险偏好的关键变量"
    if "ai" in lower or "semiconductor" in lower or "chip" in lower:
        return "AI 和半导体链条仍是跨市场风险偏好的主线"
    if "china" in lower or "chinese" in lower:
        return "中国资产和海外产业链映射进入外部新闻焦点"
    return "外部宏观和产业新闻继续影响跨市场风险偏好"


def render_macro_headline_section(packet: dict[str, Any]) -> str:
    prefetch = packet.get("finance_search_prefetch") if isinstance(packet.get("finance_search_prefetch"), dict) else {}
    raw_items = prefetch.get("news_items") if isinstance(prefetch.get("news_items"), list) else []
    items = [item for item in raw_items if isinstance(item, dict) and item.get("title")]
    filtered = [item for item in items if is_contentful_macro_news(str(item.get("title") or ""))]
    if filtered:
        items = filtered
    if not items:
        return ""

    lines = ["## 宏观事件与产业新闻"]
    seen: set[str] = set()
    for item in items:
        title = translate_macro_news_title(item.get("title"))
        key = re.sub(r"\W+", "", title.lower())
        if not title or key in seen:
            continue
        seen.add(key)
        source = public_cell(item.get("source"), default="")
        published_at = public_cell(item.get("published_at"), default="")
        meta = " / ".join(part for part in (source, published_at) if part)
        suffix = f"（{meta}）" if meta else ""
        lines.append(f"- {title}{suffix}")
        if len(lines) >= 6:
            break
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def public_us_action(value: Any) -> str:
    text = str(value or "").lower()
    if "gamma" in text:
        return "买入/持有，Gamma入场"
    if "options" in text:
        return "买入/持有，期权确认"
    if "buy" in text:
        return "买入/持有"
    return public_cell(value)


def extract_stop_target(risk_plan: Any) -> tuple[str, str]:
    text = str(risk_plan or "")
    match = re.search(r"stop\s+([0-9.]+);\s*target\s+([0-9.]+)", text, re.IGNORECASE)
    if not match:
        return "-", "-"
    return match.group(1), match.group(2)


def public_us_constraint(row: dict[str, Any]) -> str:
    source = str(row.get("source") or "")
    if "gamma" in source.lower() or "gamma" in str(row.get("action") or "").lower():
        prefix = "Gamma入场"
    elif "options" in str(row.get("action") or "").lower():
        prefix = "期权确认"
    else:
        prefix = "核心池"
    hedge = public_cell(row.get("hedge"), default="")
    hedge_r = fmt_r(row.get("hedge_notional_r")) if row.get("hedge_notional_r") is not None else ""
    hedge_text = f"; 对冲 {hedge} {hedge_r}".strip() if hedge else ""
    return public_cell(f"{prefix}{hedge_text}")


def public_cn_stage(row: dict[str, Any]) -> str:
    stage = str(row.get("pipeline_stage") or row.get("action") or "").lower()
    if stage in {"top_stock_trade", "secondary_stock_trade"} or stage.startswith("buy"):
        return "执行候选"
    if stage == "active_watch":
        return "观察候选"
    if stage in {"ranked_watch", "bench_ranked"}:
        return "备选候选"
    if stage:
        return public_cell(stage)
    return "候选"


def public_cn_reason(value: Any) -> str:
    text = public_cell(value, default="")
    if not text:
        return ""
    text = re.sub(r"\bAI\s+Infra\b", "AI基础设施", text, flags=re.IGNORECASE)
    text = re.sub(r"\bBFS\s+universe\s+member\b", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"rank\s+by\s+price,\s*flow,\s*news,\s*options\s+and\s+risk\s+before\s+any\s+R\.?",
        "",
        text,
        flags=re.IGNORECASE,
    )
    chunks: list[str] = []
    for raw in re.split(r"[;；/]+", text):
        chunk = raw.strip(" .。;；")
        lower = chunk.lower()
        if not chunk:
            continue
        if any(marker in lower for marker in ("bfs universe", "universe member", "rank by price", "before any r")):
            continue
        if chunk in {"AI基础设施", "AI基础设施观察池"}:
            chunk = "AI算力链候选"
        chunks.append(chunk)
    return "; ".join(dict.fromkeys(chunks))


def public_cn_wait_condition(row: dict[str, Any]) -> str:
    parts = [
        public_cn_reason(row.get("reason")),
        public_cell(row.get("entry"), default=""),
        public_cell(row.get("handling_line"), default=""),
        public_cell(row.get("target"), default=""),
    ]
    labels = ["", "入场", "处理", "目标"]
    output = []
    for label, value in zip(labels, parts):
        if not value:
            continue
        output.append(f"{label}{value}" if not label else f"{label}: {value}")
    return "; ".join(output) or "等待A股本域价格和量能确认"


def render_cn_pipeline_section(packet: dict[str, Any]) -> str:
    cn = packet.get("cn") if isinstance(packet.get("cn"), dict) else {}
    candidates = cn.get("pipeline_candidates") if isinstance(cn.get("pipeline_candidates"), list) else []
    rows = [row for row in candidates if isinstance(row, dict) and row.get("symbol")]
    if not rows:
        return ""

    lines = [
        "## A股执行与候选管线",
        "美股只能给A股风险预算、行业顺风和节奏约束；A股标的仍按本域管线的阶段、排序和等待条件处理。",
        "",
        "| 代码 | 名称 | 阶段 | 排序 | 分数 | 处理/等待条件 |",
        "|---|---|---|---:|---:|---|",
    ]
    for row in rows[:10]:
        lines.append(
            "| {symbol} | {name} | {stage} | {rank} | {score} | {condition} |".format(
                symbol=public_cell(row.get("symbol")),
                name=public_cell(row.get("name")),
                stage=public_cn_stage(row),
                rank=public_cell(row.get("rank")),
                score=public_cell(row.get("rank_score")),
                condition=public_cell(public_cn_wait_condition(row)),
            )
        )
    return "\n".join(lines)


def fmt_public_percent(value: Any) -> str:
    number = compact_float(value, 2)
    if number is None:
        return "-"
    return f"{number:.0f}%"


def fmt_public_iv_rank_sample(value: Any) -> str:
    count = option_iv_rank_observation_count({"iv_rank_n": value})
    if count <= 0:
        return "-"
    if count < MIN_PUBLIC_IV_RANK_OBS:
        return f"N={count}，仅参考"
    return f"N={count}"


def fmt_public_ratio(value: Any) -> str:
    number = compact_float(value, 2)
    if number is None:
        return "-"
    return f"{number:.2f}x"


def fmt_public_signed(value: Any) -> str:
    number = compact_float(value, 2)
    if number is None:
        return "-"
    return f"{number:+.2f}"


def render_us_action_section(packet: dict[str, Any]) -> str:
    us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
    actions = us.get("actions") if isinstance(us.get("actions"), list) else []
    actions = [row for row in actions if isinstance(row, dict) and row.get("symbol")]
    if not actions:
        return ""

    lines = [
        "## 美股执行标的",
        "美股标的沿用 US 管线；A股只能接受美股风险和主线约束，不能反向升降美股仓位。",
        "",
        "| Ticker | 动作 | R | 入口 | 止损 | 目标 | 期权/Gamma/对冲约束 |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in actions[:10]:
        stop, target = extract_stop_target(row.get("risk_plan"))
        lines.append(
            "| {symbol} | {action} | {size} | {entry} | {stop} | {target} | {constraint} |".format(
                symbol=public_cell(row.get("symbol")),
                action=public_us_action(row.get("action")),
                size=fmt_r(row.get("size_r")),
                entry=public_cell(row.get("entry")),
                stop=public_cell(stop),
                target=public_cell(target),
                constraint=public_us_constraint(row),
            )
        )
    return "\n".join(lines)


def fmt_public_money(value: Any) -> str:
    number = compact_float(value, 2)
    if number is None:
        return "-"
    sign = "-" if number < 0 else ""
    number = abs(number)
    if number >= 1_000_000_000:
        return f"{sign}${number / 1_000_000_000:.2f}B"
    if number >= 1_000_000:
        return f"{sign}${number / 1_000_000:.1f}M"
    if number >= 1_000:
        return f"{sign}${number / 1_000:.1f}K"
    return f"{sign}${number:.0f}"


def render_13f_items(rows: list[dict[str, Any]], *, change_key: str = "") -> str:
    parts: list[str] = []
    for row in rows[:5]:
        label = public_cell(row.get("issuer") or row.get("cusip"))
        value = fmt_public_money(row.get(change_key) if change_key else row.get("value_usd"))
        if label and value != "-":
            parts.append(f"{label}({value})")
    return "; ".join(parts) or "-"


def render_sec_13f_section(packet: dict[str, Any]) -> str:
    payload = packet.get("sec_13f_recent") if isinstance(packet.get("sec_13f_recent"), dict) else {}
    filings = payload.get("filings") if isinstance(payload.get("filings"), list) else []
    filings = [row for row in filings if isinstance(row, dict)]
    if not filings:
        return ""

    lookback = compact_float(payload.get("lookback_hours"), 1) or 12.0
    lines = [
        "## SEC 13F 机构持仓快照",
        (
            f"过去 {lookback:g} 小时本地新增 {fmt(payload.get('recent_file_count'))} 个 13F 持仓文件；"
            "13F 有季度滞后，只作为机构仓位线索，不当作实时资金流。"
        ),
        "",
        "| Manager | Filing/Report | Holdings | 新增Top5 | 增持Top5 | 减持Top5 |",
        "|---|---|---:|---|---|---|",
    ]
    for filing in filings[:6]:
        filing_date = public_cell(filing.get("filing_date"), default="")
        report_date = public_cell(filing.get("report_date"), default="")
        date_text = " / ".join(part for part in (filing_date, report_date) if part) or "-"
        lines.append(
            "| {manager} | {date} | {count} | {new} | {inc} | {dec} |".format(
                manager=public_cell(filing.get("manager")),
                date=date_text,
                count=public_cell(filing.get("holding_count")),
                new=public_cell(render_13f_items(filing.get("new_positions_top5") or [])),
                inc=public_cell(render_13f_items(filing.get("increases_top5") or [], change_key="value_delta_usd")),
                dec=public_cell(render_13f_items(filing.get("decreases_top5") or [], change_key="value_delta_usd")),
            )
        )
    return "\n".join(lines)


def ensure_sec_13f_section(report: str, packet: dict[str, Any]) -> str:
    section = render_sec_13f_section(packet)
    text = strip_managed_report_sections(report, prefixes=("## SEC 13F 机构持仓快照",))
    if not section:
        return text.strip()
    if "## 美股执行标的" in text:
        return insert_after_section(text, "## 美股执行标的", section)
    if "## 宏观事件与产业新闻" in text:
        return insert_after_section(text, "## 宏观事件与产业新闻", section)
    return insert_after_section(text, "## 宏观数据温度计", section)


def render_us_options_attention_section(packet: dict[str, Any]) -> str:
    us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
    context = us.get("option_context") if isinstance(us.get("option_context"), dict) else {}
    rows = context.get("options_attention_watchlist") if isinstance(context.get("options_attention_watchlist"), list) else []
    rows = [row for row in rows if isinstance(row, dict) and row.get("symbol")]
    if not rows:
        return ""

    anomaly = context.get("options_anomaly_radar") if isinstance(context.get("options_anomaly_radar"), dict) else {}
    tenor = context.get("options_tenor_radar") if isinstance(context.get("options_tenor_radar"), dict) else {}
    lines = [
        "## 美股期权关注标的（OTM skew / LEAPS IV）",
        (
            "这里不是期权下单清单；只把远 OTM skew、LEAPS/tenor 和 IV 分位映射回股票仓位、"
            "入场节奏、止损和等待条件。"
        ),
        (
            f"当日 far-OTM 异常 {fmt(anomaly.get('row_count'))} 条，"
            f"跨 tenor/LEAPS 信号 {fmt(tenor.get('signal_count'))} 条；"
            f"没有触发时仍保留 skew/IV 排行观察。IV rank 至少需要 {MIN_PUBLIC_IV_RANK_OBS} 个历史点，"
            "不足时只作背景，不作为低位/高位结论。"
        ),
        "",
        "| Ticker | 关注点 | 日期 | IV rank | 样本 | IV/HV | PC z | Skew z | 处理 |",
        "|---|---|---|---:|---|---:|---:|---:|---|",
    ]
    for row in rows[:10]:
        lines.append(
            "| {symbol} | {reason} | {date} | {iv_rank} | {iv_rank_n} | {iv_hv} | {pc_z} | {skew_z} | {reading} |".format(
                symbol=public_cell(row.get("symbol")),
                reason=public_cell(row.get("reason")),
                date=public_cell(row.get("effective_date")),
                iv_rank=fmt_public_percent(row.get("iv_rank_pct")),
                iv_rank_n=fmt_public_iv_rank_sample(row.get("iv_rank_n")),
                iv_hv=fmt_public_ratio(row.get("iv_hv")),
                pc_z=fmt_public_signed(row.get("pc_ratio_z")),
                skew_z=fmt_public_signed(row.get("skew_z")),
                reading=public_cell(row.get("reading")),
            )
        )
    return "\n".join(lines)


def packet_us_actions(packet: dict[str, Any]) -> list[dict[str, Any]]:
    us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
    actions = us.get("actions") if isinstance(us.get("actions"), list) else []
    return [row for row in actions if isinstance(row, dict) and row.get("symbol")]


def packet_cn_candidates(packet: dict[str, Any]) -> list[dict[str, Any]]:
    cn = packet.get("cn") if isinstance(packet.get("cn"), dict) else {}
    candidates = cn.get("pipeline_candidates") if isinstance(cn.get("pipeline_candidates"), list) else []
    return [row for row in candidates if isinstance(row, dict) and row.get("symbol")]


def packet_option_watchlist(packet: dict[str, Any]) -> list[dict[str, Any]]:
    us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
    context = us.get("option_context") if isinstance(us.get("option_context"), dict) else {}
    rows = context.get("options_attention_watchlist") if isinstance(context.get("options_attention_watchlist"), list) else []
    return [row for row in rows if isinstance(row, dict) and row.get("symbol")]


def first_star_candidate(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        if is_cn_star_symbol(row.get("symbol")):
            return row
    return None


def symbol_name(row: dict[str, Any]) -> str:
    symbol = public_cell(row.get("symbol"))
    name = public_cell(row.get("name"), default="")
    if name and name != "-" and name.upper() != symbol.upper():
        return f"{symbol}{name}"
    return symbol


def symbol_list(rows: list[dict[str, Any]], *, limit: int = 4, with_stage: bool = False) -> str:
    parts: list[str] = []
    for row in rows[:limit]:
        label = symbol_name(row)
        if with_stage:
            label = f"{label}（{public_cn_stage(row)}）"
        if label and label != "-":
            parts.append(label)
    return "、".join(parts) or "-"


def market_row_lookup(packet: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("symbol") or ""): row for row in market_snapshot_public_rows(packet)}


def market_row_phrase(row: dict[str, Any]) -> str:
    label = public_cell(row.get("label") or row.get("symbol"))
    date_text = public_cell(row.get("date"))
    change = fmt_market_change_pct(row.get("change_pct"))
    if change == "-":
        return f"{label}({date_text})"
    return f"{label}({date_text}) {change}"


def market_temperature_sentence(packet: dict[str, Any]) -> str:
    rows = market_row_lookup(packet)
    preferred = ("^GSPC", "^IXIC", "000001.SS", "399001.SZ", "^KS11", "^N225", "ES=F", "NQ=F")
    phrases = [market_row_phrase(rows[symbol]) for symbol in preferred if symbol in rows]
    if not phrases:
        return ""
    return "温度计里 " + "、".join(phrases[:6]) + "，跨时区读数只按表内日期解释。"


def us_execution_sentence(packet: dict[str, Any]) -> str:
    actions = packet_us_actions(packet)
    if not actions:
        return "美股执行池没有给出新的单名动作，A股端只接受指数、期货和风险预算约束。"
    parts = []
    for row in actions[:4]:
        symbol = public_cell(row.get("symbol"))
        action = public_us_action(row.get("action"))
        size = fmt_r(row.get("size_r"))
        parts.append(f"{symbol} {action}，{size}")
    return "美股执行池保留 " + "；".join(parts) + "。"


def option_watch_sentence(packet: dict[str, Any]) -> str:
    rows = packet_option_watchlist(packet)
    if rows:
        parts = [
            f"{public_cell(row.get('symbol'))}（{public_cell(row.get('reason'))}）"
            for row in rows[:4]
            if row.get("symbol")
        ]
        return "期权/Gamma 只约束股票仓位和入场节奏，当前重点看 " + "、".join(parts) + "。"
    us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
    context = us.get("option_context") if isinstance(us.get("option_context"), dict) else {}
    gamma_date = context.get("gamma_effective_date")
    date_part = f"（数据日 {public_cell(gamma_date)}）" if gamma_date else ""
    return f"期权/Gamma{date_part}没有提供可直接加仓的新增确认，但仍用于收紧股票入场和止损节奏。"


def cn_pipeline_sentence(packet: dict[str, Any]) -> str:
    rows = packet_cn_candidates(packet)
    if not rows:
        return "A股端没有新的候选扩散信号，下一轮只做美股主线到中国资产的复盘，不扩大执行范围。"
    star = first_star_candidate(rows)
    lead = symbol_list(rows, limit=4, with_stage=True)
    if star:
        condition = public_cn_wait_condition(star)
        return (
            f"A股候选先看 {lead}；科创板候选 {symbol_name(star)} 已在A股候选管线内，"
            f"等待条件是 {condition}。"
        )
    return f"A股候选先看 {lead}，全部按本域价格、量能和等待条件处理。"


def render_execution_diary_sections(packet: dict[str, Any], existing_report: str = "") -> str:
    market_line = market_temperature_sentence(packet)
    us_line = us_execution_sentence(packet)
    option_line = option_watch_sentence(packet)
    cn_line = cn_pipeline_sentence(packet)
    rows = packet_cn_candidates(packet)
    cn_focus = symbol_list(rows, limit=3, with_stage=False)
    us_focus = ", ".join(public_cell(row.get("symbol")) for row in packet_us_actions(packet)[:3]) or "-"

    sections = [
        (
            "## 跨市场主线",
            "\n".join(
                part
                for part in (
                    "美股仍是主导变量：它给A股的是风险预算、行业顺风和节奏约束，不是反向接受A股指挥。",
                    market_line,
                    us_line,
                    option_line,
                )
                if part
            ),
        ),
        (
            "## 传导到A股",
            "\n".join(
                [
                    cn_line,
                    "这条传导只从美股到A股：美股主线决定A股要优先检查哪些行业和价格带，A股盘后反馈只用于复盘上一轮映射是否成立。",
                ]
            ),
        ),
        (
            "## 今天的执行剧本",
            "\n".join(
                [
                    f"- 美股：{us_focus} 按各自入口、止损和目标线执行，Gamma 与期权读数只改变节奏和风险预算。",
                    f"- A股：{cn_focus} 先看本域入场区间、处理线和量能确认，不因为美股走强而追价。",
                    "- 风险：如果指数温度计转弱，先降单名暴露，再减少候选扩散。",
                ]
            ),
        ),
        (
            "## 失效条件和下一步检查",
            "\n".join(
                [
                    "- 美股大盘和期货若失去同步，A股只保留观察候选。",
                    "- 半导体、AI 或宏观新闻如果不能延续到价格和量能，688候选不升级。",
                    "- 下一次报告重点检查期权 skew/LEAPS 是否从观察变成确认，以及A股候选是否站上入场区间。",
                ]
            ),
        ),
    ]

    blocks: list[str] = []
    for heading, body in sections:
        if any(line.strip().startswith(prefix) for line in existing_report.splitlines() for prefix in (heading,)):
            continue
        blocks.append(f"{heading}\n{body}".strip())
    return "\n\n".join(blocks)


def market_snapshot_date_map(packet: dict[str, Any]) -> dict[str, str]:
    date_map: dict[str, str] = {}
    for row in market_snapshot_public_rows(packet):
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


def report_heading_matches(line: str, prefixes: tuple[str, ...]) -> bool:
    stripped = line.strip()
    return any(stripped.startswith(prefix) for prefix in prefixes)


def strip_managed_report_sections(
    report: str,
    prefixes: tuple[str, ...] = MANAGED_REPORT_SECTION_PREFIXES,
) -> str:
    lines = report.strip().splitlines()
    output: list[str] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        if report_heading_matches(line, prefixes):
            idx += 1
            while idx < len(lines):
                next_line = lines[idx]
                if next_line.strip().startswith("## ") and not report_heading_matches(next_line, prefixes):
                    break
                idx += 1
            continue
        output.append(line)
        idx += 1
    text = "\n".join(output).strip()
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def insert_after_opening_paragraph(report: str, block: str) -> str:
    block = block.strip()
    if not block:
        return report.strip()

    lines = report.strip().splitlines()
    if not lines:
        return block
    title_idx = next((idx for idx, line in enumerate(lines) if line.strip().startswith("# ")), 0)
    para_start = title_idx + 1
    while para_start < len(lines) and not lines[para_start].strip():
        para_start += 1
    if para_start >= len(lines):
        return "\n".join([lines[title_idx], "", block]).strip()
    insert_at = para_start + 1
    while insert_at < len(lines) and lines[insert_at].strip():
        insert_at += 1
    return "\n".join(lines[:insert_at] + ["", block, ""] + lines[insert_at:]).strip()


def insert_after_title(report: str, block: str) -> str:
    block = block.strip()
    if not block:
        return report.strip()

    lines = report.strip().splitlines()
    if not lines:
        return block
    title_idx = next((idx for idx, line in enumerate(lines) if line.strip().startswith("# ")), 0)
    insert_at = title_idx + 1
    while insert_at < len(lines) and not lines[insert_at].strip():
        insert_at += 1
    return "\n".join(lines[: title_idx + 1] + ["", block, ""] + lines[insert_at:]).strip()


def insert_after_section(report: str, heading_prefix: str, block: str) -> str:
    block = block.strip()
    if not block:
        return report.strip()
    lines = report.strip().splitlines()
    heading_idx = next((idx for idx, line in enumerate(lines) if line.strip().startswith(heading_prefix)), None)
    if heading_idx is None:
        return insert_after_opening_paragraph(report, block)
    insert_at = heading_idx + 1
    while insert_at < len(lines) and not lines[insert_at].strip().startswith("## "):
        insert_at += 1
    return "\n".join(lines[:insert_at] + ["", block, ""] + lines[insert_at:]).strip()


def insert_before_section(report: str, heading_prefix: str, block: str) -> str:
    block = block.strip()
    if not block:
        return report.strip()
    lines = report.strip().splitlines()
    heading_idx = next((idx for idx, line in enumerate(lines) if line.strip().startswith(heading_prefix)), None)
    if heading_idx is None:
        return "\n\n".join(part for part in (report.strip(), block) if part).strip()
    return "\n".join(lines[:heading_idx] + ["", block, ""] + lines[heading_idx:]).strip()


def ensure_market_snapshot_section(report: str, packet: dict[str, Any]) -> str:
    top_blocks = [
        block for block in (
            render_market_snapshot_section(packet),
            render_macro_headline_section(packet),
        )
        if block
    ]
    tail = render_market_tail_section(packet)
    if not top_blocks and not tail:
        return strip_managed_report_sections(report)

    text = strip_managed_report_sections(report)
    if top_blocks:
        text = insert_after_title(text, "\n\n".join(top_blocks))
    if tail:
        text = "\n\n".join(part for part in (text.strip(), tail.strip()) if part)
    return text.strip()


def ensure_us_action_section(report: str, packet: dict[str, Any]) -> str:
    section = render_us_action_section(packet)
    text = strip_managed_report_sections(report, prefixes=("## 美股执行标的", "## 美股标的"))
    if not section:
        return text.strip()
    if "## 宏观事件与产业新闻" in text:
        return insert_after_section(text, "## 宏观事件与产业新闻", section)
    if "## 宏观事件 Headlines" in text:
        return insert_after_section(text, "## 宏观事件 Headlines", section)
    return insert_after_section(text, "## 宏观数据温度计", section)


def ensure_us_options_attention_section(report: str, packet: dict[str, Any]) -> str:
    section = render_us_options_attention_section(packet)
    text = strip_managed_report_sections(
        report,
        prefixes=("## 美股期权关注标的", "## 美股期权观察标的", "## 美股 OTM skew"),
    )
    if not section:
        return text.strip()
    if "## 美股执行标的" in text:
        return insert_after_section(text, "## 美股执行标的", section)
    if "## 宏观事件与产业新闻" in text:
        return insert_after_section(text, "## 宏观事件与产业新闻", section)
    return insert_after_section(text, "## 宏观数据温度计", section)


def ensure_cn_pipeline_section(report: str, packet: dict[str, Any]) -> str:
    section = render_cn_pipeline_section(packet)
    text = strip_managed_report_sections(
        report,
        prefixes=("## A股执行与候选管线", "## A股执行观察", "## A股候选管线"),
    )
    if not section:
        return text.strip()
    if "## 美股期权关注标的" in text:
        return insert_after_section(text, "## 美股期权关注标的", section)
    if "## 美股执行标的" in text:
        return insert_after_section(text, "## 美股执行标的", section)
    return insert_after_section(text, "## 宏观事件与产业新闻", section)


def repair_star_pipeline_language(report: str, *, add_replacement: bool = True) -> str:
    output: list[str] = []
    replacement_added = False
    for raw in report.splitlines():
        line = raw.strip()
        if contains_marker(line, "科创") and contains_marker(line, "温度计"):
            if add_replacement and not replacement_added:
                output.append("科创板按A股候选管线处理，必须落到具体688标的、阶段和等待条件。")
                replacement_added = True
            continue
        output.append(raw)
    return "\n".join(output).strip()


def strip_star_only_tables(report: str) -> str:
    lines = report.splitlines()
    output: list[str] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        stripped = line.strip()
        if line.startswith("|") and contains_marker(line, "科创板候选"):
            idx += 1
            while idx < len(lines) and lines[idx].strip().startswith("|"):
                idx += 1
            while output and not output[-1].strip():
                output.pop()
            if idx < len(lines) and output and output[-1].strip():
                output.append("")
            continue
        output.append(line)
        idx += 1
    return re.sub(r"\n{3,}", "\n\n", "\n".join(output)).strip()


def ensure_cn_pipeline_language(report: str, packet: dict[str, Any]) -> str:
    text = strip_managed_report_sections(report, prefixes=("## A股科创板候选管线",))
    text = strip_star_only_tables(text)
    return repair_star_pipeline_language(text, add_replacement=False).strip()


def ensure_execution_diary_sections(report: str, packet: dict[str, Any]) -> str:
    block = render_execution_diary_sections(packet, existing_report=report)
    if not block:
        return report.strip()
    return insert_before_section(report, "## 附表：其他跨市场数据", block)


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


def fetch_sec_13f_recent_summary(*, lookback_hours: float, timeout: int = 45) -> dict[str, Any]:
    script = ROOT / "scripts" / "sec_13f_recent_summary.py"
    if not script.exists():
        return {"ok": False, "error": "sec 13f summary script not found", "filings": []}
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--lookback-hours",
                str(lookback_hours),
                "--json",
            ],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            cwd=ROOT,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": str(exc), "filings": []}
    if result.returncode != 0:
        return {"ok": False, "error": ((result.stderr or "") + (result.stdout or ""))[-800:], "filings": []}
    try:
        payload = json.loads((result.stdout or "").strip())
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"invalid sec 13f JSON: {exc}", "filings": []}
    if not isinstance(payload, dict):
        return {"ok": False, "error": "invalid sec 13f payload", "filings": []}
    payload["ok"] = True
    payload["public_use"] = (
        "If filings is non-empty, summarize new positions, increases, and decreases as delayed 13F institutional "
        "positioning context. If filings is empty, omit this topic from the public report."
    )
    return payload


def attach_sec_13f_recent(packet: dict[str, Any], *, lookback_hours: float) -> None:
    payload = fetch_sec_13f_recent_summary(lookback_hours=lookback_hours)
    packet["sec_13f_recent"] = payload


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

## 美股期权/Gamma 上下文
{option_context_markdown(us.get('option_context') or {})}

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
- 如果 packet.finance_search_prefetch.ok=true,必须优先使用其中的 market_rows 和 news_items 写宏观数据温度计和宏观事件 headlines;
  这些是脚本侧已经从 finance-search 取回的证据,不要再写成缺失或工具失败。
- 必须读取 packet.us.option_context。美股段要覆盖期权/Gamma 对股票仓位、入场节奏、止损收紧和风险预算的影响;
  如果有 options_attention_watchlist,要写出 OTM skew / LEAPS IV 值得关注的具体美股标的;
  如果 options_anomaly_radar 或 options_tenor_radar 是 no_trigger/no_signal,要自然写成“没有新增异常/tenor确认”,不要写成数据没跑。
- 如果 packet.sec_13f_recent.filings 非空,必须把过去12小时新增的 SEC 13F 文件解读为“季度滞后的机构仓位线索”,
  覆盖每个 manager 的新增持仓、增持、减持 Top5;如果为空,公开报告自然省略,不要写缺失提示。
- 可以启发式使用 finance-search MCP 工具,尤其是:
  quant_stack_daily_snapshot, quant_stack_spine_triage, quant_stack_task_status,
  quant_stack_validate_main_strategy_v2, quant_stack_ranker, quant_stack_symbol_context,
  quant_stack_sec_13f_recent, get_market_snapshot, newsnow_radar, search_news, research_brief。
- 写作前必须构造“宏观数据温度计”:顶部只放美股大盘、A股大盘、KOSPI、日经225;
  美股期货、欧洲、恒生/台湾、VIX、油、金、美元/离岸人民币、利率和科创50等其他数据放报告尾部附表。
- 引用任何大盘指数或期货时,必须带返回日期,格式类似“德国DAX(2026-06-29)”或“纳指期货(2026-06-29)”;
  跨时区市场尤其不能只写“今天/隔夜”。不要引用印度、Sensex 或 Nifty 指数。
- 写作前必须尝试检索最新宏观/地缘/AI/半导体/中国市场新闻;只使用返回标题、来源或 URL 可核验的新闻,
  并把宏观事件 headlines 放在顶部温度计之后。
- A股侧不得只看主板;必须用 packet.cn.actions、packet.cn.pipeline_candidates 或 CN ranker/symbol_context
  选择具体科创板/688xxx.SH 标的。科创板不是单独章节或温度计,它是 A股候选管线的一部分;
  如果候选仍是 active_watch/0R,也要在 A股执行/观察叙事里写清具体代码、名称、等待条件和不能执行的原因。
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
- 美股段必须融合 packet.us.option_context,包括 Gamma/期权定位/异常与 tenor 是否触发;它们是股票仓位和风险节奏证据,
  不是期权交易指令。如果 packet.us.option_context 有 options_attention_watchlist,保留其中 OTM skew / LEAPS IV 关注标的。
  如果 anomaly/tenor 为 0,写成没有新增确认,不要省略成“美股没发完”。
- 如果 packet.sec_13f_recent.filings 非空,保留 SEC 13F 机构持仓快照:每个 manager 的新增持仓、增持、减持 Top5。
  必须说明 13F 是季度滞后数据,只能作为机构仓位背景,不能写成实时资金流或今日买卖。
- 宏观数据温度计顶部只能保留美股大盘、A股大盘、KOSPI、日经225;美股期货、油、金、
  欧洲/其他亚洲、VIX、汇率、利率和科创50等只能放报告尾部附表。如果 draft 已遗漏,
  只能从 packet/工具返回中补入,不能虚构数字。
- 如果 packet.finance_search_prefetch 有 market_rows/news_items,优先使用这些已取回证据补齐温度计、宏观 headlines 和尾部附表,
  不要写成缺失、不可用或工具失败。
- 温度计和尾部附表引用任何大盘指数或期货时必须带返回日期,尤其是欧洲/亚洲/美国期货这类跨时区市场;
  格式类似“日经225(2026-06-29)”或“标普期货(2026-06-29)”。删除印度、Sensex、Nifty 指数。
- A股执行段必须从 packet.cn.actions、packet.cn.pipeline_candidates 或 CN ranker/symbol_context 中选择至少一个具体
  688xxx.SH 科创板标的,并把它融合进 A股执行/观察叙事;不要另起一张固定“科创板候选管线”表。
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


REVIEWER_NOTE_PREFIXES = (
    "主要改动",
    "修改说明",
    "改动说明",
    "编辑说明",
    "审稿说明",
    "二审说明",
    "本次修改",
)


def strip_reviewer_note_blocks(text: str) -> str:
    lines = text.splitlines()
    output: list[str] = []
    idx = 0
    while idx < len(lines):
        stripped = lines[idx].strip().strip("*# ：:")
        if any(stripped.startswith(prefix) for prefix in REVIEWER_NOTE_PREFIXES):
            while output and not output[-1].strip():
                output.pop()
            idx += 1
            while idx < len(lines) and not lines[idx].strip().startswith("#"):
                idx += 1
            if idx < len(lines) and output and output[-1].strip():
                output.append("")
            continue
        output.append(lines[idx])
        idx += 1
    return re.sub(r"\n{3,}", "\n\n", "\n".join(output)).strip()


def strip_diff_artifact_markers(text: str) -> str:
    lines = text.splitlines()
    has_plus_artifacts = sum(1 for line in lines if line.startswith("+")) >= 5
    has_delete_artifacts = any(
        line.startswith(("-#", "-|", "--"))
        or re.match(r"^-\d+[.)]\s", line)
        or re.match(r"^-[^\s-]", line)
        for line in lines
    )
    has_unified_diff = any(
        line.startswith(("diff --git ", "@@ "))
        or re.match(r"^(a|b)/+/", line)
        or contains_marker(line, "review diff")
        or bool(re.search(r"omitted \d+ diff line", line, re.IGNORECASE))
        for line in lines
    )
    if not (has_plus_artifacts or has_delete_artifacts or has_unified_diff):
        return text.strip()

    output: list[str] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        stripped = line.strip()
        if (
            line.startswith(("diff --git ", "@@ "))
            or re.match(r"^(a|b)/+/", line)
            or contains_marker(line, "review diff")
            or re.search(r"omitted \d+ diff line", line, re.IGNORECASE)
        ):
            idx += 1
            while idx < len(lines):
                next_line = lines[idx]
                if next_line.startswith("#") and not next_line.startswith(("---", "+++", "@@")):
                    break
                idx += 1
            continue
        if (
            line.startswith(("-#", "-|", "--"))
            or re.match(r"^-\d+[.)]\s", line)
            or re.match(r"^-[^\s-]", line)
            or line == "-"
        ):
            idx += 1
            continue
        if line == "+":
            output.append("")
        elif line.startswith("+") and not line.startswith("+++"):
            output.append(line[1:])
        elif line.startswith("---") or line.startswith("+++"):
            pass
        else:
            output.append(line)
        idx += 1
    return re.sub(r"\n{3,}", "\n\n", "\n".join(output)).strip()


def normalize_public_report_text(text: str, slot: str) -> str:
    text = strip_diff_artifact_markers(text)
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
    text = strip_reviewer_note_blocks(text)
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
    text = strip_reviewer_note_blocks(text)
    text = strip_duplicate_report_titles(text, slot)
    return text.strip()


def strip_duplicate_report_titles(text: str, slot: str) -> str:
    expected_title = "# 跨市场早报" if slot == "am" else "# 跨市场晚报"
    output: list[str] = []
    seen_title = False
    for line in text.splitlines():
        if line.strip().startswith(expected_title):
            if seen_title:
                continue
            seen_title = True
        output.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(output)).strip()


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


def public_context_failures(text: str, packet: dict[str, Any] | None = None) -> list[str]:
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

    if not any(contains_marker(text, marker) for marker in ("期权", "Gamma", "gamma")):
        failures.append("missing public US options/Gamma context")

    if packet:
        us = packet.get("us") if isinstance(packet.get("us"), dict) else {}
        actions = us.get("actions") if isinstance(us.get("actions"), list) else []
        us_symbols = [
            str(row.get("symbol") or "").upper()
            for row in actions
            if isinstance(row, dict) and row.get("symbol")
        ]
        missing_us_symbols = [symbol for symbol in us_symbols if symbol and symbol not in text]
        if missing_us_symbols:
            failures.append("missing public US action ticker(s): " + ", ".join(missing_us_symbols[:5]))

        context = us.get("option_context") if isinstance(us.get("option_context"), dict) else {}
        watch_rows = (
            context.get("options_attention_watchlist")
            if isinstance(context.get("options_attention_watchlist"), list)
            else []
        )
        watch_symbols = [
            str(row.get("symbol") or "").upper()
            for row in watch_rows
            if isinstance(row, dict) and row.get("symbol")
        ]
        missing_watch_symbols = [symbol for symbol in watch_symbols if symbol and symbol not in text]
        if missing_watch_symbols:
            failures.append("missing public US options watch ticker(s): " + ", ".join(missing_watch_symbols[:5]))

    if not re.search(r"(?<!\d)688\d{3}\.SH(?![A-Z0-9])", text):
        failures.append("missing concrete public CN STAR/科创板 ticker: 688xxx.SH")
    for raw in text.splitlines():
        line = raw.strip()
        if (
            raw.startswith("+")
            or raw.startswith(("-#", "-|", "--"))
            or re.match(r"^-\d+[.)]\s", raw)
            or re.match(r"^-[^\s-]", raw)
            or raw.startswith(("diff --git ", "@@ "))
            or re.match(r"^(a|b)/+/", raw)
            or contains_marker(line, "review diff")
            or re.search(r"omitted \d+ diff line", line, re.IGNORECASE)
        ):
            failures.append("public report contains diff artifact line")
            break
        if line.startswith("|") and contains_marker(line, "科创板候选"):
            failures.append("STAR/科创板 must be integrated into the A-share pipeline narrative, not a standalone table")
            break
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


def public_narrative_failures(text: str, packet: dict[str, Any] | None = None) -> list[str]:
    if not packet:
        return []

    failures: list[str] = []
    h2_count = sum(1 for line in text.splitlines() if line.strip().startswith("## "))
    if h2_count < 8:
        failures.append(f"public report too thin: expected at least 8 second-level sections, got {h2_count}")

    required_groups = {
        "cross-market main line": ("## 跨市场主线", "美股给出的真正信号", "传到 A股", "传导到A股"),
        "execution script": ("## 今天的执行剧本", "## 下一交易窗口执行剧本", "执行剧本"),
        "invalidation checks": ("## 失效条件", "下一步检查"),
    }
    for label, markers in required_groups.items():
        if not any(contains_marker(text, marker) for marker in markers):
            failures.append(f"missing public narrative section: {label}")
    return failures


def validate_shadow_report(
    text: str,
    slot: str,
    *,
    public_delivery: bool = False,
    packet: dict[str, Any] | None = None,
) -> list[str]:
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
    title_count = sum(1 for line in text.splitlines() if line.strip().startswith(expected_title))
    if title_count > 1:
        failures.append(f"public report contains duplicate top-level title: {expected_title}")
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
            "事实清单",
            "主要改动",
            "修改说明",
            "改动说明",
            "编辑说明",
            "本次修改",
            "待二审",
            "最终 markdown",
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
            "BFS universe",
            "universe member",
            "rank by price",
            "before any R",
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
            if token == "cron":
                if not re.search(r"(?<![A-Za-z])cron(?![A-Za-z])", text, flags=re.IGNORECASE):
                    continue
            elif not contains_marker(text, token):
                continue
            failures.append(f"forbidden public-report marker: {token}")
        failures.extend(public_context_failures(text, packet))
        failures.extend(public_narrative_failures(text, packet))
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
            "result": {"report": relative_display(report_path)},
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
                "result": {"report": relative_display(report_path)},
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


def report_title(path: Path) -> str:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text:
                return text.lstrip("#").strip()
    except FileNotFoundError:
        return ""
    return ""


def publish_openclaw_if_requested(path: Path, packet: dict[str, Any], args: argparse.Namespace) -> None:
    if not getattr(args, "publish_openclaw", False):
        return
    if args.delivery_dry_run:
        print(f"openclaw dry-run: mode={args.openclaw_mode} report={path}", file=sys.stderr)
        return

    helper = ROOT / "scripts" / "publish_report_to_openclaw.py"
    paths = output_paths(path.parent, packet["slot"])
    target_date = packet.get("target_cn_date") or packet["cn"]["report_date"]
    cmd = [
        sys.executable,
        str(helper),
        "--report-path",
        str(path),
        "--kind",
        "cross_market_daily",
        "--slot",
        packet["slot"],
        "--date",
        str(target_date),
        "--title",
        report_title(path),
        "--packet-path",
        str(paths["packet"]),
        "--meta-path",
        str(paths["meta"]),
        "--remote-host",
        args.openclaw_host,
        "--remote-user",
        args.openclaw_user,
        "--remote-root",
        args.openclaw_root,
        "--mode",
        args.openclaw_mode,
        "--agent",
        args.openclaw_agent,
        "--agent-timeout",
        str(args.openclaw_agent_timeout),
    ]
    if args.openclaw_identity_file:
        cmd.extend(["--identity-file", args.openclaw_identity_file])
    if args.openclaw_agent_session_key:
        cmd.extend(["--agent-session-key", args.openclaw_agent_session_key])
    if args.openclaw_agent_deliver:
        cmd.append("--agent-deliver")
    if args.openclaw_reply_channel:
        cmd.extend(["--reply-channel", args.openclaw_reply_channel])
    if args.openclaw_reply_account:
        cmd.extend(["--reply-account", args.openclaw_reply_account])
    if args.openclaw_reply_to:
        cmd.extend(["--reply-to", args.openclaw_reply_to])
    if args.openclaw_message_channel:
        cmd.extend(["--message-channel", args.openclaw_message_channel])
    if args.openclaw_message_target:
        cmd.extend(["--message-target", args.openclaw_message_target])
    if args.openclaw_allow_duplicate_event:
        cmd.append("--allow-duplicate-event")

    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=max(60, int(args.openclaw_agent_timeout) + 30),
            check=False,
        )
    except Exception as exc:
        if args.openclaw_required:
            raise
        print(f"warn: OpenClaw publish failed: {exc}", file=sys.stderr)
        return
    if result.returncode != 0:
        message = ((result.stderr or "") + "\n" + (result.stdout or "")).strip()[-1200:]
        if args.openclaw_required:
            raise RuntimeError(f"OpenClaw publish failed with exit={result.returncode}: {message}")
        print(f"warn: OpenClaw publish failed with exit={result.returncode}: {message}", file=sys.stderr)
        return
    tail = (result.stdout or "").strip().splitlines()[-1:] or ["ok"]
    print(f"cross-market openclaw published: {tail[0]}")


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
    if args.sec_13f_prefetch == "on" and args.agent_backend == "hermes":
        attach_sec_13f_recent(packet, lookback_hours=args.sec_13f_lookback_hours)
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
    report = ensure_us_action_section(report, packet)
    report = ensure_sec_13f_section(report, packet)
    report = ensure_us_options_attention_section(report, packet)
    report = ensure_cn_pipeline_section(report, packet)
    report = ensure_cn_pipeline_language(report, packet)
    report = ensure_execution_diary_sections(report, packet)
    if args.send_email:
        report = normalize_public_report_text(report, args.slot)
    report = strip_diff_artifact_markers(report)
    report = strip_duplicate_report_titles(report, args.slot)
    failures = validate_shadow_report(report, args.slot, public_delivery=args.send_email, packet=packet)
    if failures:
        restore_output_snapshot(output_snapshot)
        raise SystemExit("cross-market shadow validation failed:\n- " + "\n- ".join(failures))
    path = write_outputs(output_dir, packet, report, agent_backend=backend_name)
    print(f"cross-market {args.slot} shadow written: {path}")
    send_email_if_requested(path, packet, args)
    publish_openclaw_if_requested(path, packet, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
