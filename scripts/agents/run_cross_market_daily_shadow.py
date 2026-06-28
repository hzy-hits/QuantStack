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
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_ROOT = ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


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
        choices=["off", "auto"],
        default="off",
        help="off writes deterministic shadow text; auto calls codex_backend.call_llm.",
    )
    parser.add_argument("--timeout", type=int, default=900)
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
    return {
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
        "cn": cn_summary,
        "us": us_summary,
        "invariants": [
            "Numbers and tickers must come from packet facts only.",
            "No combined kitchen-ticket artifact may be sent as production email.",
            "US can guide CN; CN feedback must not drive US positioning.",
            "Cross-market links are guidance overlays, not proof of tradability.",
        ],
    }


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
        direction_rule = "美股盘后必须指导 A股盘前: 仓位、行业优先级、风险线。"
    else:
        title_rule = "# 跨市场晚报"
        direction_rule = (
            "A股盘后只能作为美股主线向中国资产传导的反馈;不得写成 A股指导美股。"
            "美股盘前仍由美股自己的 regime、money gate、期权/Gamma 和证据门决定。"
        )
    system = f"""
你是 quant-stack 的跨市场 lead editor agent。你只能使用用户给定 packet 里的数字、ticker、日期和结论。
禁止编造价格、R、ticker、新闻或仓位。输出一份中文跨市场日报,第一行必须以 `{title_rule}` 开头。
{direction_rule}
结构必须覆盖: 跨市场主线、US->CN 因果方向、A股反馈边界、交易清单、风险与复核、数据血缘。
"""
    user = json.dumps(packet, ensure_ascii=False, indent=2)
    return system, user


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


def validate_shadow_report(text: str, slot: str) -> list[str]:
    failures: list[str] = []
    if slot == "am":
        required = ["# 跨市场早报", "美股", "A股"]
    else:
        required = ["# 跨市场晚报", "A股", "美股"]
    for token in required:
        if token not in text:
            failures.append(f"missing required token: {token}")
    forbidden = ["production_delivery: enabled", "prod delivery", "A股盘后必须指导 美股盘前", "CN -> US"]
    for token in forbidden:
        if token in text:
            failures.append(f"forbidden production marker: {token}")
    return failures


def write_outputs(output_dir: Path, packet: dict[str, Any], report: str, *, agent_backend: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"cross_market_{packet['slot']}_shadow"
    packet_path = output_dir / f"{prefix}_packet.json"
    trajectory_path = output_dir / f"{prefix}_trajectory.jsonl"
    report_path = output_dir / f"{prefix}.md"
    meta_path = output_dir / f"{prefix}.meta.json"

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
    trajectory_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in trajectory) + "\n",
        encoding="utf-8",
    )
    meta = {
        "slot": packet["slot"],
        "direction": packet["direction"],
        "cn_date": packet["cn"]["report_date"],
        "us_date": packet["us"]["report_date"],
        "agent_backend": packet.get("_agent_backend") or agent_backend,
        "agent_model": packet.get("_agent_model") or "",
        "shadow_only": True,
        "generated_at": now,
        "script": Path(__file__).name,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report_path


def main() -> int:
    args = parse_args()
    cn_date, us_date = resolve_dates(args.slot, args.cn_date, args.us_date)
    report_root = args.report_root if args.report_root.is_absolute() else ROOT / args.report_root
    output_dir = args.output_dir or report_root / cn_date
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir

    cn = load_market_artifact(report_root, "cn", cn_date)
    us = load_market_artifact(report_root, "us", us_date)
    missing = []
    if not cn.payload:
        missing.append(f"CN payload missing for {cn_date}: {cn.report_dir / 'main_strategy_v2_backtest.json'}")
    if not us.payload:
        missing.append(f"US payload missing for {us_date}: {us.report_dir / 'main_strategy_v2_backtest.json'}")
    if not cn.markdown:
        missing.append(f"CN markdown missing for {cn_date}: {cn.report_dir}")
    if not us.markdown:
        missing.append(f"US markdown missing for {us_date}: {us.report_dir}")
    if missing:
        raise SystemExit("\n".join(missing))

    packet = build_packet(args.slot, cn, us)
    if args.agent_backend == "off":
        report = deterministic_report(packet)
        backend_name = "deterministic_shadow"
    else:
        report = call_agent(packet, args.timeout)
        backend_name = packet.get("_agent_backend") or "agent"
    failures = validate_shadow_report(report, args.slot)
    if failures:
        raise SystemExit("cross-market shadow validation failed:\n- " + "\n- ".join(failures))
    path = write_outputs(output_dir, packet, report, agent_backend=backend_name)
    print(f"cross-market {args.slot} shadow written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
