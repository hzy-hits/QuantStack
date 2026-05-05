#!/usr/bin/env python3
"""Build a CN book overlay from V2 candidates and optional live fill CSV.

This is deliberately a trading-control report, not a broker adapter. Without a
CSV it still shows the current A-share tickets and the exact live-data gap.
With a CSV it joins current positions to the V2 ticket/lifecycle/risk state.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_V2_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "cn_book_overlay"


@dataclass
class CnFill:
    symbol: str
    trade_date: str | None
    side: str
    qty: float
    price: float | None
    amount: float | None
    pnl: float | None


@dataclass
class CnPosition:
    symbol: str
    net_qty: float
    gross_buy: float
    realized_pnl: float
    first_buy: str | None
    last_trade: str | None
    fills: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate CN Book Overlay from V2 tickets and optional live fills.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument(
        "--activity-csv",
        type=Path,
        default=os.environ.get("QUANT_CN_ACTIVITY_CSV") or os.environ.get("CN_ACTIVITY_CSV"),
        help="Optional A-share broker fills CSV. Defaults to QUANT_CN_ACTIVITY_CSV or CN_ACTIVITY_CSV.",
    )
    parser.add_argument("--v2-root", type=Path, default=DEFAULT_V2_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-hold-days", type=int, default=5)
    return parser.parse_args()


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    return out if math.isfinite(out) else None


def pick(row: dict[str, Any], aliases: list[str]) -> Any:
    normalized = {str(k).strip().lower(): v for k, v in row.items()}
    for alias in aliases:
        key = alias.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def normalize_cn_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    if digits.startswith(("6", "9")):
        return f"{digits}.SH"
    return f"{digits}.SZ"


def normalize_side(value: Any) -> str:
    text = str(value or "").strip().lower()
    if any(token in text for token in ["buy", "买入", "证券买入", "b"]):
        return "buy"
    if any(token in text for token in ["sell", "卖出", "证券卖出", "s"]):
        return "sell"
    return text or "unknown"


def parse_cn_activity(path: Path | None) -> tuple[str, list[CnFill]]:
    if not path:
        return "missing_activity_csv", []
    if not path.exists():
        return "activity_csv_not_found", []
    fills: list[CnFill] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            return "activity_csv_empty", []
        for row in reader:
            symbol = normalize_cn_symbol(pick(row, ["symbol", "ts_code", "证券代码", "代码", "股票代码"]))
            if not symbol:
                continue
            side = normalize_side(pick(row, ["side", "买卖方向", "操作", "业务名称", "entrust_bs"]))
            qty = parse_number(pick(row, ["qty", "quantity", "成交数量", "数量", "发生数量"])) or 0.0
            price = parse_number(pick(row, ["price", "成交价格", "成交均价", "价格"]))
            amount = parse_number(pick(row, ["amount", "成交金额", "发生金额", "成交额"]))
            pnl = parse_number(pick(row, ["pnl", "realized_pnl", "浮动盈亏", "盈亏", "参考盈亏"]))
            trade_date = pick(row, ["trade_date", "date", "成交日期", "日期", "发生日期"])
            fills.append(
                CnFill(
                    symbol=symbol,
                    trade_date=str(trade_date)[:10] if trade_date else None,
                    side=side,
                    qty=abs(qty),
                    price=price,
                    amount=amount,
                    pnl=pnl,
                )
            )
    return "ok", fills


def summarize_positions(fills: list[CnFill]) -> dict[str, CnPosition]:
    by_symbol: dict[str, list[CnFill]] = defaultdict(list)
    for fill in fills:
        by_symbol[fill.symbol].append(fill)
    out: dict[str, CnPosition] = {}
    for symbol, rows in by_symbol.items():
        net_qty = 0.0
        gross_buy = 0.0
        realized_pnl = 0.0
        first_buy: str | None = None
        last_trade: str | None = None
        for row in rows:
            signed_qty = row.qty if row.side == "buy" else -row.qty if row.side == "sell" else 0.0
            net_qty += signed_qty
            if row.side == "buy":
                gross_buy += row.amount if row.amount is not None else row.qty * (row.price or 0.0)
                if row.trade_date and (first_buy is None or row.trade_date < first_buy):
                    first_buy = row.trade_date
            if row.pnl is not None:
                realized_pnl += row.pnl
            if row.trade_date and (last_trade is None or row.trade_date > last_trade):
                last_trade = row.trade_date
        out[symbol] = CnPosition(
            symbol=symbol,
            net_qty=round(net_qty, 4),
            gross_buy=round(gross_buy, 4),
            realized_pnl=round(realized_pnl, 4),
            first_buy=first_buy,
            last_trade=last_trade,
            fills=len(rows),
        )
    return out


def load_v2_payload(v2_root: Path, as_of: date) -> dict[str, Any]:
    path = v2_root / as_of.isoformat() / "main_strategy_v2_backtest.json"
    if not path.exists():
        return {"status": "missing_v2_report", "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "bad_v2_report", "path": str(path), "error": str(exc)}
    payload["status"] = "ok"
    payload["path"] = str(path)
    return payload


def build_ticket_map(v2_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tickets: dict[str, dict[str, Any]] = {}
    for row in ((v2_payload.get("cn") or {}).get("current") or []):
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            tickets[symbol] = dict(row)
    for row in ((v2_payload.get("portfolio_risk_overlay") or {}).get("rows") or []):
        if str(row.get("market") or "").upper() != "CN":
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        tickets.setdefault(symbol, {}).update(
            {
                "portfolio_final_r": row.get("final_r"),
                "manual_probe_r": row.get("manual_probe_r"),
                "auto_eligible": row.get("auto_eligible"),
                "risk_reasons": row.get("risk_reasons") or [],
                "shadow_option_haircut": row.get("shadow_option_haircut"),
            }
        )
    return tickets


def build_overlay(as_of: date, fills_status: str, positions: dict[str, CnPosition], v2_payload: dict[str, Any], max_hold_days: int) -> dict[str, Any]:
    tickets = build_ticket_map(v2_payload)
    symbols = sorted(set(positions) | set(tickets))
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        pos = positions.get(symbol)
        ticket = tickets.get(symbol, {})
        is_open = bool(pos and abs(pos.net_qty) > 1e-9)
        state = str(ticket.get("state") or "No Report Support")
        if state == "Execution Alpha" and (ticket.get("manual_probe_r") or 0.0) > 0:
            action = "manual_micro_probe_planned_entry_only"
            max_size = f"{ticket.get('manual_probe_r'):.2f}R"
        elif state == "Execution Alpha" and ticket.get("auto_eligible"):
            action = "planned_entry_probe"
            max_size = f"{ticket.get('portfolio_final_r') or 0.0:.2f}R"
        elif state == "Positive EV Setup" and (ticket.get("manual_probe_r") or 0.0) > 0:
            action = "manual_micro_probe_planned_entry_only"
            max_size = f"{ticket.get('manual_probe_r'):.2f}R"
        elif state == "Positive EV Setup":
            action = "watch_only_no_new_trade"
            max_size = "0R"
        elif is_open:
            action = "exit_or_reduce_no_valid_ticket"
            max_size = "0R new capital"
        else:
            action = "watch_only_no_new_trade"
            max_size = "0R"
        rows.append(
            {
                "symbol": symbol,
                "name": ticket.get("name") or "",
                "is_open": is_open,
                **(asdict(pos) if pos else {
                    "net_qty": 0.0,
                    "gross_buy": 0.0,
                    "realized_pnl": 0.0,
                    "first_buy": None,
                    "last_trade": None,
                    "fills": 0,
                }),
                "ticket_state": state,
                "action": action,
                "max_size": max_size,
                "observation_entry_zone": ticket.get("observation_entry_zone"),
                "handling_line": ticket.get("handling_line"),
                "first_target": ticket.get("first_target"),
                "time_exit": ticket.get("time_exit") or f"T+1 review; T+3 no +1R; hard max T+{max_hold_days}",
                "lifecycle_action": ticket.get("lifecycle_action"),
                "risk_reasons": ticket.get("risk_reasons") or [],
            }
        )
    return {
        "as_of": as_of.isoformat(),
        "source_status": fills_status,
        "v2_status": v2_payload.get("status"),
        "policy": {
            "no_ticket_no_trade": True,
            "max_hold_days": max_hold_days,
            "cn_manual_micro_probe_cap_r": 0.05,
            "required_fill_fields": ["symbol", "trade_date", "side", "qty", "price"],
        },
        "summary": {
            "positions": len(positions),
            "open_positions": sum(1 for pos in positions.values() if abs(pos.net_qty) > 1e-9),
            "ticket_rows": len(tickets),
            "manual_micro_probe_ready": sum(1 for row in rows if row["action"] == "manual_micro_probe_planned_entry_only"),
            "no_report_support_open": sum(1 for row in rows if row["is_open"] and row["ticket_state"] == "No Report Support"),
        },
        "rows": rows,
    }


def fmt_money(value: Any) -> str:
    number = parse_number(value)
    return "-" if number is None else f"{number:,.2f}"


def render_overlay(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "## CN Book Overlay",
        "",
        "A-share ticket and live-fill control layer. No ticket means no new trade; Execution Alpha still needs planned-entry/pullback and T+1/T+3/T+max lifecycle management.",
        "",
        f"- Source status: `{payload.get('source_status')}`",
        f"- Open positions: `{summary.get('open_positions', 0)}`",
        f"- Ticket rows: `{summary.get('ticket_rows', 0)}`",
        f"- Manual micro-probe ready: `{summary.get('manual_micro_probe_ready', 0)}`",
        f"- Open positions without report support: `{summary.get('no_report_support_open', 0)}`",
        "",
        "| Code | Name | Qty | Ticket | Action | Max size | Entry | Handling | Target | Time exit | Risk reasons |",
        "|---|---|---:|---|---|---:|---|---:|---:|---|---|",
    ]
    for row in payload.get("rows") or []:
        lines.append(
            f"| {row.get('symbol')} | {row.get('name') or '-'} | {fmt_money(row.get('net_qty'))} | "
            f"{row.get('ticket_state')} | {row.get('action')} | {row.get('max_size')} | "
            f"{row.get('observation_entry_zone') or '-'} | {fmt_money(row.get('handling_line'))} | "
            f"{fmt_money(row.get('first_target'))} | {row.get('time_exit') or '-'} | "
            f"{', '.join(row.get('risk_reasons') or []) or '-'} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(payload: dict[str, Any], output_root: Path) -> Path:
    out_dir = output_root / payload["as_of"]
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cn_book_overlay.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / "cn_book_overlay.md").write_text(render_overlay(payload), encoding="utf-8")
    return out_dir


def run(args: argparse.Namespace) -> dict[str, Any]:
    as_of = datetime.strptime(args.date, "%Y-%m-%d").date()
    fills_status, fills = parse_cn_activity(args.activity_csv)
    positions = summarize_positions(fills)
    v2_payload = load_v2_payload(args.v2_root, as_of)
    payload = build_overlay(as_of, fills_status, positions, v2_payload, args.max_hold_days)
    if args.activity_csv:
        payload["source_csv"] = str(args.activity_csv)
    payload["v2_report"] = v2_payload.get("path")
    out_dir = write_outputs(payload, args.output_root)
    print(f"CN Book Overlay written: {out_dir / 'cn_book_overlay.md'}")
    return payload


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
