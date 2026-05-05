#!/usr/bin/env python3
"""Build a personal book overlay from an IBKR activity CSV and daily signals.

The overlay is intentionally a throttle, not a broker integration. It answers:
which current positions have a valid daily-report ticket, which names are only
probe/watch, and which discretionary buys were outside the report permission
system.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "my_book_overlay"
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_ALPHA_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "strategy_backtest"


@dataclass
class Order:
    symbol: str
    currency: str
    trade_dt: datetime
    qty: float
    price: float | None
    close_price: float | None
    proceeds: float
    commission: float
    basis: float | None
    realized_pnl: float
    mtm_pnl: float
    code: str


@dataclass
class SymbolPosition:
    symbol: str
    orders: int
    buy_orders: int
    sell_orders: int
    first_buy: str | None
    last_buy: str | None
    last_trade: str | None
    net_qty: float
    gross_buy: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    pnl_pct_on_buy: float | None
    holding_days: int | None
    current_close: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate My Book Overlay for the US daily report.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument(
        "--activity-csv",
        type=Path,
        default=os.environ.get("QUANT_USER_ACTIVITY_CSV") or os.environ.get("IBKR_ACTIVITY_CSV"),
        help="IBKR activity statement CSV. Defaults to QUANT_USER_ACTIVITY_CSV or IBKR_ACTIVITY_CSV.",
    )
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--alpha-root", type=Path, default=DEFAULT_ALPHA_ROOT)
    parser.add_argument("--ticket-lookback-days", type=int, default=5)
    parser.add_argument("--max-new-names-per-week", type=int, default=3)
    parser.add_argument("--max-single-name-positions", type=int, default=6)
    parser.add_argument("--max-hold-days", type=int, default=30)
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def round_or_none(value: Any, digits: int = 4) -> float | None:
    fval = parse_number(value)
    return None if fval is None else round(fval, digits)


def safe_json_loads(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def parse_ibkr_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d, %H:%M:%S")


def parse_ibkr_activity(path: Path) -> tuple[list[Order], dict[str, dict[str, float]]]:
    orders: list[Order] = []
    performance: dict[str, dict[str, float]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.reader(fh):
            if (
                len(row) >= 16
                and row[0] == "交易"
                and row[1] == "Data"
                and row[2] == "Order"
                and row[3] == "股票"
            ):
                qty = parse_number(row[7])
                trade_dt = parse_ibkr_dt(row[6])
                if qty is None:
                    continue
                orders.append(
                    Order(
                        symbol=str(row[5]).strip().upper(),
                        currency=str(row[4]).strip().upper(),
                        trade_dt=trade_dt,
                        qty=qty,
                        price=parse_number(row[8]),
                        close_price=parse_number(row[9]),
                        proceeds=parse_number(row[10]) or 0.0,
                        commission=parse_number(row[11]) or 0.0,
                        basis=parse_number(row[12]),
                        realized_pnl=parse_number(row[13]) or 0.0,
                        mtm_pnl=parse_number(row[14]) or 0.0,
                        code=str(row[15]).strip(),
                    )
                )
                continue
            if (
                len(row) >= 16
                and row[0] == "已实现和未实现的表现总结"
                and row[1] == "Data"
                and row[2] == "股票"
                and row[3]
                and row[3] not in {"总数", "总计（全部资产）"}
            ):
                performance[str(row[3]).strip().upper()] = {
                    "realized_pnl": parse_number(row[9]) or 0.0,
                    "unrealized_pnl": parse_number(row[14]) or 0.0,
                    "total_pnl": parse_number(row[15]) or 0.0,
                }
    return orders, performance


def summarize_positions(orders: list[Order], performance: dict[str, dict[str, float]], as_of: date) -> list[SymbolPosition]:
    by_symbol: dict[str, list[Order]] = defaultdict(list)
    for order in orders:
        if order.currency == "USD" and order.symbol:
            by_symbol[order.symbol].append(order)

    positions: list[SymbolPosition] = []
    for symbol, rows in sorted(by_symbol.items()):
        rows = sorted(rows, key=lambda row: row.trade_dt)
        buys = [row for row in rows if row.qty > 0]
        sells = [row for row in rows if row.qty < 0]
        gross_buy = sum(-row.proceeds for row in buys)
        net_qty = sum(row.qty for row in rows)
        perf = performance.get(
            symbol,
            {
                "realized_pnl": sum(row.realized_pnl for row in rows),
                "unrealized_pnl": sum(row.mtm_pnl for row in rows if abs(net_qty) > 1e-9),
                "total_pnl": sum(row.realized_pnl for row in rows),
            },
        )
        first_buy = min((row.trade_dt for row in buys), default=None)
        last_buy = max((row.trade_dt for row in buys), default=None)
        last_trade = max((row.trade_dt for row in rows), default=None)
        close_prices = [row.close_price for row in rows if row.close_price is not None]
        exit_or_asof = as_of if abs(net_qty) > 1e-9 else (last_trade.date() if last_trade else as_of)
        holding_days = (exit_or_asof - first_buy.date()).days if first_buy else None
        total_pnl = float(perf.get("total_pnl") or 0.0)
        positions.append(
            SymbolPosition(
                symbol=symbol,
                orders=len(rows),
                buy_orders=len(buys),
                sell_orders=len(sells),
                first_buy=first_buy.isoformat() if first_buy else None,
                last_buy=last_buy.isoformat() if last_buy else None,
                last_trade=last_trade.isoformat() if last_trade else None,
                net_qty=round(net_qty, 6),
                gross_buy=round(gross_buy, 6),
                realized_pnl=round(float(perf.get("realized_pnl") or 0.0), 6),
                unrealized_pnl=round(float(perf.get("unrealized_pnl") or 0.0), 6),
                total_pnl=round(total_pnl, 6),
                pnl_pct_on_buy=round(total_pnl / gross_buy * 100.0, 6) if gross_buy > 0 else None,
                holding_days=holding_days,
                current_close=round_or_none(close_prices[-1]) if close_prices else None,
            )
        )
    return positions


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def normalize_bucket(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    if text in {"core", "core book"}:
        return "core"
    return text.replace(" ", "_") or "unknown"


def normalize_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"long", "bull", "bullish", "up"}:
        return "long"
    if text in {"short", "bear", "bearish", "down"}:
        return "short"
    return "neutral"


def normalize_confidence(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"HIGH", "MODERATE"}:
        return "high_mod"
    if text in {"LOW", "NO_SIGNAL", "NONE"}:
        return "low"
    return text.lower() or "unknown"


def normalize_execution(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"trade", "trade_now", "executable", "executable_now", "main_signal"}:
        return "executable_now"
    if text in {"wait", "wait_pullback", "pullback"}:
        return "wait_pullback"
    if text in {"avoid", "do_not_chase", "stale_chase"}:
        return "do_not_chase"
    return text or "unknown"


def trend_regime(details: dict[str, Any]) -> str:
    gate = details.get("execution_gate") or {}
    if isinstance(gate, dict) and (gate.get("trend_regime") or gate.get("regime")):
        return str(gate.get("trend_regime") or gate.get("regime")).strip().lower()
    momentum = details.get("momentum") or {}
    if isinstance(momentum, dict) and momentum.get("regime"):
        return str(momentum.get("regime")).strip().lower()
    return "unknown"


def report_v2_pass(row: dict[str, Any]) -> bool:
    details = safe_json_loads(row.get("details_json"))
    return (
        normalize_bucket(row.get("report_bucket")) == "core"
        and normalize_direction(row.get("signal_direction")) == "long"
        and normalize_confidence(row.get("signal_confidence")) in {"low", "high_mod"}
        and normalize_execution(row.get("execution_mode")) == "executable_now"
        and trend_regime(details) == "trending"
    )


def load_recent_report_tickets(
    db_path: Path,
    symbols: list[str],
    as_of: date,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    if not symbols or not db_path.exists():
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "report_decisions"):
            return {}
        placeholders = ",".join("?" for _ in symbols)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT
                d.report_date,
                d.symbol,
                d.selection_status,
                d.report_bucket,
                d.signal_direction,
                d.signal_confidence,
                d.execution_mode,
                d.rr_ratio,
                d.primary_reason,
                d.details_json
            FROM report_decisions d
            WHERE d.symbol IN ({placeholders})
              AND d.report_date <= CAST(? AS DATE)
              AND d.report_date >= CAST(? AS DATE)
            ORDER BY d.symbol, d.report_date DESC, COALESCE(d.rank_order, 999999)
            """,
            [*symbols, as_of.isoformat(), (as_of - timedelta(days=lookback_days)).isoformat()],
        )
    finally:
        con.close()

    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol or symbol in latest:
            continue
        row["trend_regime"] = trend_regime(safe_json_loads(row.get("details_json")))
        row["v2_pass"] = report_v2_pass(row)
        latest[symbol] = row
    return latest


def load_alpha_context(alpha_root: Path, as_of: date) -> dict[str, dict[str, Any]]:
    path = alpha_root / as_of.isoformat() / "alpha_bulletin.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    priority = {"execution_alpha": 40, "recall_alpha": 30, "blocked_alpha": 10}
    by_symbol: dict[str, dict[str, Any]] = {}
    for section in ["blocked_alpha", "recall_alpha", "execution_alpha"]:
        for item in payload.get(section, []) or []:
            if item.get("market") != "us":
                continue
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue
            current = by_symbol.get(symbol)
            if current and priority.get(str(current.get("section")), 0) >= priority[section]:
                continue
            by_symbol[symbol] = {
                "section": section,
                "policy_id": item.get("policy_id"),
                "reason": item.get("reason"),
                "blockers": item.get("blockers") or [],
                "report_bucket": item.get("report_bucket"),
                "signal_confidence": item.get("signal_confidence"),
            }
    return by_symbol


def monday_of_week(day: date) -> date:
    return day - timedelta(days=day.weekday())


def ticket_decision(
    position: SymbolPosition,
    ticket: dict[str, Any] | None,
    alpha: dict[str, Any] | None,
    week_start: date,
) -> dict[str, Any]:
    first_buy = datetime.fromisoformat(position.first_buy).date() if position.first_buy else None
    is_open = abs(position.net_qty) > 1e-9
    is_new_this_week = bool(first_buy and first_buy >= week_start)
    if alpha:
        section = str(alpha.get("section") or "")
        if section == "execution_alpha":
            state = "Execution Alpha"
            action = "hold_or_add_ticketed"
            max_size = "Execution Alpha size from daily risk rules"
            violation = None
        elif section == "recall_alpha" and "Positive EV Setup" in str(alpha.get("reason") or ""):
            state = "Positive EV Setup"
            action = "hold_probe_only"
            max_size = "0.10R/name stock probe; no options"
            violation = None
        else:
            state = "Legacy / Blocked"
            action = "freeze_adds_trim_or_exit_review" if is_open else "postmortem_only"
            max_size = "0R new capital"
            violation = "new_buy_without_trade_ticket" if is_new_this_week else None
        reason = str(alpha.get("reason") or "stable-alpha bulletin context")
        return {
            "ticket_state": state,
            "action": action,
            "max_size": max_size,
            "violation": violation,
            "reason": reason,
            "ticket_source": "alpha_bulletin",
            "policy_id": alpha.get("policy_id"),
            "ticket_date": None,
        }
    if ticket:
        if ticket.get("v2_pass"):
            return {
                "ticket_state": "Positive EV Setup",
                "action": "hold_probe_only",
                "max_size": "0.10R/name stock probe; no options",
                "violation": None,
                "reason": "recent report row passes V2 profit slice, but not promoted by alpha bulletin",
                "ticket_source": "report_decisions",
                "policy_id": None,
                "ticket_date": str(ticket.get("report_date")),
            }
        return {
            "ticket_state": "Legacy / Blocked",
            "action": "freeze_adds_trim_or_exit_review" if is_open else "postmortem_only",
            "max_size": "0R new capital",
            "violation": "new_buy_without_trade_ticket" if is_new_this_week else None,
            "reason": "recent report row exists, but V2 profit slice did not pass",
            "ticket_source": "report_decisions",
            "policy_id": None,
            "ticket_date": str(ticket.get("report_date")),
        }
    return {
        "ticket_state": "No Report Support",
        "action": "freeze_adds_write_ticket_or_exit_review" if is_open else "postmortem_only",
        "max_size": "0R new capital",
        "violation": "new_buy_without_trade_ticket" if is_new_this_week else None,
        "reason": "no daily report ticket in the lookback window",
        "ticket_source": "none",
        "policy_id": None,
        "ticket_date": None,
    }


def management_decision(row: dict[str, Any], max_hold_days: int) -> dict[str, Any]:
    """Convert a ticket permission row into a position-management decision."""
    is_open = bool(row.get("is_open"))
    holding_days_raw = parse_number(row.get("holding_days"))
    holding_days = int(holding_days_raw) if holding_days_raw is not None else None
    total_pnl = parse_number(row.get("total_pnl")) or 0.0
    ticket_state = str(row.get("ticket_state") or "")
    actionable_ticket = ticket_state in {"Execution Alpha", "Positive EV Setup"}

    if not is_open:
        return {
            "management_state": "closed_postmortem",
            "management_action": "review_closed_trade",
            "management_violation": None,
            "management_reason": "closed position; use it as behavior evidence, not a live action",
        }
    if holding_days is not None and holding_days > max_hold_days:
        return {
            "management_state": "time_stop_review",
            "management_action": "trim_or_exit_time_stop",
            "management_violation": "holding_days_above_max_hold",
            "management_reason": f"holding_days {holding_days} > {max_hold_days}D max hold window",
        }
    if total_pnl > 0:
        return {
            "management_state": "hold_winner",
            "management_action": "hold_no_add_without_fresh_ticket",
            "management_violation": None,
            "management_reason": f"profitable position inside {max_hold_days}D management window",
        }
    if total_pnl < 0 and actionable_ticket:
        return {
            "management_state": "ticketed_loser_review",
            "management_action": "hold_only_if_risk_line_intact",
            "management_violation": None,
            "management_reason": "losing ticketed position; keep only if daily invalidation price is intact",
        }
    if total_pnl < 0:
        return {
            "management_state": "exit_or_reduce_loser",
            "management_action": "trim_or_exit_review",
            "management_violation": "losing_position_without_actionable_ticket",
            "management_reason": "losing position without actionable daily-report support",
        }
    return {
        "management_state": "neutral_review",
        "management_action": "hold_or_exit_by_ticket",
        "management_violation": None,
        "management_reason": "flat PnL; ticket state decides whether capital stays active",
    }


def build_overlay(
    positions: list[SymbolPosition],
    tickets: dict[str, dict[str, Any]],
    alpha_context: dict[str, dict[str, Any]],
    as_of: date,
    *,
    ticket_lookback_days: int,
    max_new_names_per_week: int,
    max_single_name_positions: int,
    max_hold_days: int,
) -> dict[str, Any]:
    week_start = monday_of_week(as_of)
    rows: list[dict[str, Any]] = []
    for position in positions:
        decision = ticket_decision(
            position,
            tickets.get(position.symbol),
            alpha_context.get(position.symbol),
            week_start,
        )
        row = {**asdict(position), **decision, "is_open": abs(position.net_qty) > 1e-9}
        row.update(management_decision(row, max_hold_days))
        rows.append(row)

    open_rows = [row for row in rows if row["is_open"]]
    new_name_rows = [
        row
        for row in rows
        if row.get("first_buy")
        and datetime.fromisoformat(str(row["first_buy"])).date() >= week_start
    ]
    violations = [row for row in rows if row.get("violation")]
    frequency_violation = len(new_name_rows) > max_new_names_per_week
    position_count_violation = len(open_rows) > max_single_name_positions
    if frequency_violation:
        for row in new_name_rows[max_new_names_per_week:]:
            row["violation"] = "weekly_new_name_cap_exceeded"
            violations.append(row)
    if position_count_violation:
        for row in open_rows[max_single_name_positions:]:
            row["position_cap_note"] = "single-name position count cap exceeded"

    by_state = Counter(str(row["ticket_state"]) for row in rows)
    by_management = Counter(str(row["management_state"]) for row in open_rows)
    return {
        "as_of": as_of.isoformat(),
        "policy": {
            "no_ticket_no_trade": True,
            "ticket_lookback_days": ticket_lookback_days,
            "positive_ev_setup_max_size": "0.10R/name stock probe; no options",
            "legacy_blocked_max_size": "0R new capital",
            "weekly_new_name_cap": max_new_names_per_week,
            "single_name_position_cap": max_single_name_positions,
            "max_hold_days": max_hold_days,
        },
        "summary": {
            "symbols": len(rows),
            "open_positions": len(open_rows),
            "new_names_this_week": len(new_name_rows),
            "gross_buy": round(sum(float(row.get("gross_buy") or 0.0) for row in rows), 6),
            "total_pnl": round(sum(float(row.get("total_pnl") or 0.0) for row in rows), 6),
            "ticket_state_counts": dict(sorted(by_state.items())),
            "new_buy_without_ticket": sum(1 for row in rows if row.get("violation") == "new_buy_without_trade_ticket"),
            "weekly_new_name_cap_exceeded": frequency_violation,
            "position_count_cap_exceeded": position_count_violation,
            "management_state_counts": dict(sorted(by_management.items())),
            "time_stop_positions": sum(1 for row in open_rows if row.get("management_state") == "time_stop_review"),
            "hold_winner_positions": sum(1 for row in open_rows if row.get("management_state") == "hold_winner"),
            "exit_or_reduce_loser_positions": sum(
                1 for row in open_rows if row.get("management_state") == "exit_or_reduce_loser"
            ),
        },
        "rows": sorted(rows, key=lambda row: (not row["is_open"], str(row["ticket_state"]), str(row["symbol"]))),
    }


def holding_bucket(days: Any) -> str:
    value = parse_number(days)
    if value is None:
        return "unknown"
    if value <= 3:
        return "0-3D"
    if value <= 10:
        return "4-10D"
    if value <= 25:
        return "11-25D"
    if value <= 30:
        return "26-30D"
    return ">30D"


def order_count_bucket(orders: Any) -> str:
    value = parse_number(orders) or 0.0
    if value <= 1:
        return "one_order"
    if value == 2:
        return "two_orders"
    return "3plus_orders"


def size_bucket(gross_buy: Any) -> str:
    value = parse_number(gross_buy) or 0.0
    if value < 500:
        return "<500"
    if value < 1000:
        return "500-1000"
    if value < 2000:
        return "1000-2000"
    return ">=2000"


def summarize_position_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    gross_buy = sum(parse_number(row.get("gross_buy")) or 0.0 for row in rows)
    total_pnl = sum(parse_number(row.get("total_pnl")) or 0.0 for row in rows)
    pnl_pcts = [parse_number(row.get("pnl_pct_on_buy")) for row in rows if parse_number(row.get("pnl_pct_on_buy")) is not None]
    holding_days = [parse_number(row.get("holding_days")) for row in rows if parse_number(row.get("holding_days")) is not None]
    winners = [row for row in rows if (parse_number(row.get("total_pnl")) or 0.0) > 0]
    return {
        "symbols": len(rows),
        "winners": len(winners),
        "win_rate": round(len(winners) / len(rows) * 100.0, 4) if rows else None,
        "gross_buy": round(gross_buy, 6),
        "total_pnl": round(total_pnl, 6),
        "return_on_gross_buy": round(total_pnl / gross_buy * 100.0, 6) if gross_buy > 0 else None,
        "avg_pnl_pct": round(sum(pnl_pcts) / len(pnl_pcts), 6) if pnl_pcts else None,
        "avg_holding_days": round(sum(holding_days) / len(holding_days), 4) if holding_days else None,
    }


def summarize_by_bucket(rows: list[dict[str, Any]], field_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field_name) or "unknown")].append(row)
    return {bucket: summarize_position_group(grouped[bucket]) for bucket in sorted(grouped)}


def build_personal_alpha_research(
    positions: list[SymbolPosition],
    as_of: date,
    *,
    max_hold_days: int,
) -> dict[str, Any]:
    rows = [asdict(position) for position in positions if position.gross_buy > 0]
    for row in rows:
        row["holding_bucket"] = holding_bucket(row.get("holding_days"))
        row["order_count_bucket"] = order_count_bucket(row.get("orders"))
        row["size_bucket"] = size_bucket(row.get("gross_buy"))
        row["is_open"] = abs(parse_number(row.get("net_qty")) or 0.0) > 1e-9

    winners = [row for row in rows if (parse_number(row.get("total_pnl")) or 0.0) > 0]
    losers = [row for row in rows if (parse_number(row.get("total_pnl")) or 0.0) < 0]
    open_rows = [row for row in rows if row.get("is_open")]
    closed_rows = [row for row in rows if not row.get("is_open")]
    top_winners = sorted(rows, key=lambda row: parse_number(row.get("total_pnl")) or 0.0, reverse=True)[:8]
    top_losers = sorted(rows, key=lambda row: parse_number(row.get("total_pnl")) or 0.0)[:8]

    return {
        "as_of": as_of.isoformat(),
        "learned_policy": {
            "entry": "no ticket, no discretionary trade",
            "target_hold_window": f"11-{max_hold_days}D",
            "time_stop_days": max_hold_days,
            "winner_management": "hold winners inside the window; add only with a fresh daily-report ticket",
            "loser_management": "reduce or exit no-ticket losers; ticketed losers need an intact invalidation line",
            "options": "US options remain shadow PnL until post-cost ledger evidence is positive",
        },
        "summary": {
            "all": summarize_position_group(rows),
            "winners": summarize_position_group(winners),
            "losers": summarize_position_group(losers),
            "open": summarize_position_group(open_rows),
            "closed": summarize_position_group(closed_rows),
        },
        "by_holding_bucket": summarize_by_bucket(rows, "holding_bucket"),
        "by_order_count": summarize_by_bucket(rows, "order_count_bucket"),
        "by_size": summarize_by_bucket(rows, "size_bucket"),
        "top_winners": [
            {
                "symbol": row.get("symbol"),
                "holding_days": row.get("holding_days"),
                "orders": row.get("orders"),
                "gross_buy": row.get("gross_buy"),
                "total_pnl": row.get("total_pnl"),
                "pnl_pct_on_buy": row.get("pnl_pct_on_buy"),
            }
            for row in top_winners
        ],
        "top_losers": [
            {
                "symbol": row.get("symbol"),
                "holding_days": row.get("holding_days"),
                "orders": row.get("orders"),
                "gross_buy": row.get("gross_buy"),
                "total_pnl": row.get("total_pnl"),
                "pnl_pct_on_buy": row.get("pnl_pct_on_buy"),
            }
            for row in top_losers
        ],
    }


def fmt_money(value: Any) -> str:
    fval = parse_number(value)
    return "-" if fval is None else f"${fval:,.2f}"


def fmt_pct(value: Any) -> str:
    fval = parse_number(value)
    return "-" if fval is None else f"{fval:+.2f}%"


def fmt_days(value: Any) -> str:
    fval = parse_number(value)
    return "-" if fval is None else f"{fval:.0f}D"


def render_overlay(payload: dict[str, Any]) -> str:
    rows = payload.get("rows") or []
    summary = payload.get("summary") or {}
    lines = [
        "## My Book Overlay",
        "",
        "Personal trading permission layer computed from the IBKR activity CSV and the daily report gate. No ticket means no new discretionary buy.",
        "",
        f"- Open positions: `{summary.get('open_positions', 0)}`; cap `{(payload.get('policy') or {}).get('single_name_position_cap')}`",
        f"- New names this week: `{summary.get('new_names_this_week', 0)}`; cap `{(payload.get('policy') or {}).get('weekly_new_name_cap')}`",
        f"- New buys without valid trade ticket: `{summary.get('new_buy_without_ticket', 0)}`",
        f"- Time-stop reviews: `{summary.get('time_stop_positions', 0)}`; max hold `{(payload.get('policy') or {}).get('max_hold_days')}D`",
        f"- Hold winners inside window: `{summary.get('hold_winner_positions', 0)}`",
        f"- Exit/reduce no-ticket losers: `{summary.get('exit_or_reduce_loser_positions', 0)}`",
        f"- Total PnL in statement USD equities: `{fmt_money(summary.get('total_pnl'))}`",
        "",
        "| Symbol | Qty | Hold | PnL | PnL % | Ticket | Ticket action | Mgmt | Max size | Reason |",
        "|---|---:|---:|---:|---:|---|---|---|---|---|",
    ]
    for row in [r for r in rows if r.get("is_open")][:30]:
        reason_parts = [
            str(row.get("violation") or ""),
            str(row.get("management_violation") or ""),
            str(row.get("position_cap_note") or ""),
            str(row.get("management_reason") or ""),
            str(row.get("reason") or ""),
        ]
        reason = "; ".join(part for part in reason_parts if part)
        lines.append(
            "| {symbol} | {qty:g} | {hold} | {pnl} | {pnl_pct} | {ticket} | {action} | {mgmt} | {max_size} | {reason} |".format(
                symbol=row.get("symbol"),
                qty=float(row.get("net_qty") or 0.0),
                hold=fmt_days(row.get("holding_days")),
                pnl=fmt_money(row.get("total_pnl")),
                pnl_pct=fmt_pct(row.get("pnl_pct_on_buy")),
                ticket=row.get("ticket_state"),
                action=row.get("action"),
                mgmt=row.get("management_action"),
                max_size=row.get("max_size"),
                reason=reason.replace("|", "/")[:220],
            )
        )
    closed_or_recent = [r for r in rows if not r.get("is_open") and r.get("violation")]
    if closed_or_recent:
        lines += [
            "",
            "### Recent Ticket Violations",
            "",
            "| Symbol | First buy | PnL | Ticket | Violation |",
            "|---|---|---:|---|---|",
        ]
        for row in closed_or_recent[:20]:
            lines.append(
                f"| {row.get('symbol')} | {str(row.get('first_buy') or '')[:10]} | "
                f"{fmt_money(row.get('total_pnl'))} | {row.get('ticket_state')} | {row.get('violation')} |"
            )
    lines += ["", "---", ""]
    return "\n".join(lines)


def render_group_table(title: str, data: dict[str, dict[str, Any]], bucket_order: list[str] | None = None) -> list[str]:
    order = bucket_order or sorted(data)
    lines = [
        f"### {title}",
        "",
        "| Bucket | Symbols | Win rate | Total PnL | Return on gross | Avg hold |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for bucket in order:
        if bucket not in data:
            continue
        stats = data[bucket]
        lines.append(
            f"| {bucket} | {stats.get('symbols', 0)} | {fmt_pct(stats.get('win_rate'))} | "
            f"{fmt_money(stats.get('total_pnl'))} | {fmt_pct(stats.get('return_on_gross_buy'))} | "
            f"{fmt_days(stats.get('avg_holding_days'))} |"
        )
    return lines + [""]


def render_personal_alpha_research(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    all_stats = summary.get("all") or {}
    policy = payload.get("learned_policy") or {}
    lines = [
        "## Personal Alpha Research",
        "",
        "Behavior research derived from the IBKR activity CSV. This is used to shape daily-report position management, not to bypass the ticket gate.",
        "",
        f"- Symbols studied: `{all_stats.get('symbols', 0)}`",
        f"- Total PnL: `{fmt_money(all_stats.get('total_pnl'))}`; return on gross buy `{fmt_pct(all_stats.get('return_on_gross_buy'))}`",
        f"- Learned hold window: `{policy.get('target_hold_window')}`; time stop `{policy.get('time_stop_days')}D`",
        f"- Entry rule: `{policy.get('entry')}`",
        f"- Add rule: `{policy.get('winner_management')}`",
        "",
    ]
    lines += render_group_table(
        "Outcome By Holding Window",
        payload.get("by_holding_bucket") or {},
        ["0-3D", "4-10D", "11-25D", "26-30D", ">30D", "unknown"],
    )
    lines += render_group_table(
        "Outcome By Order Count",
        payload.get("by_order_count") or {},
        ["one_order", "two_orders", "3plus_orders"],
    )
    lines += render_group_table(
        "Outcome By Size",
        payload.get("by_size") or {},
        ["<500", "500-1000", "1000-2000", ">=2000"],
    )
    lines += [
        "### Top Winners",
        "",
        "| Symbol | Hold | Orders | Gross buy | PnL | PnL % |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in payload.get("top_winners") or []:
        lines.append(
            f"| {row.get('symbol')} | {fmt_days(row.get('holding_days'))} | {row.get('orders')} | "
            f"{fmt_money(row.get('gross_buy'))} | {fmt_money(row.get('total_pnl'))} | {fmt_pct(row.get('pnl_pct_on_buy'))} |"
        )
    lines += [
        "",
        "### Top Losers",
        "",
        "| Symbol | Hold | Orders | Gross buy | PnL | PnL % |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in payload.get("top_losers") or []:
        lines.append(
            f"| {row.get('symbol')} | {fmt_days(row.get('holding_days'))} | {row.get('orders')} | "
            f"{fmt_money(row.get('gross_buy'))} | {fmt_money(row.get('total_pnl'))} | {fmt_pct(row.get('pnl_pct_on_buy'))} |"
        )
    lines += ["", "---", ""]
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], output_root: Path, research: dict[str, Any] | None = None) -> Path:
    out_dir = output_root / payload["as_of"]
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "my_book_overlay.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / "my_book_overlay_us.md").write_text(render_overlay(payload), encoding="utf-8")
    if research is not None:
        (out_dir / "personal_alpha_research.json").write_text(
            json.dumps(research, indent=2, ensure_ascii=False, sort_keys=True, default=str),
            encoding="utf-8",
        )
        (out_dir / "personal_alpha_research.md").write_text(render_personal_alpha_research(research), encoding="utf-8")
    return out_dir


def run(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_iso_date(args.date)
    if not args.activity_csv:
        raise SystemExit("missing --activity-csv or QUANT_USER_ACTIVITY_CSV/IBKR_ACTIVITY_CSV")
    activity_csv = Path(args.activity_csv)
    if not activity_csv.exists():
        raise SystemExit(f"activity CSV not found: {activity_csv}")
    orders, performance = parse_ibkr_activity(activity_csv)
    positions = summarize_positions(orders, performance, as_of)
    symbols = [position.symbol for position in positions]
    tickets = load_recent_report_tickets(args.us_db, symbols, as_of, args.ticket_lookback_days)
    alpha_context = load_alpha_context(args.alpha_root, as_of)
    payload = build_overlay(
        positions,
        tickets,
        alpha_context,
        as_of,
        ticket_lookback_days=args.ticket_lookback_days,
        max_new_names_per_week=args.max_new_names_per_week,
        max_single_name_positions=args.max_single_name_positions,
        max_hold_days=args.max_hold_days,
    )
    research = build_personal_alpha_research(positions, as_of, max_hold_days=args.max_hold_days)
    payload["personal_alpha_research_summary"] = research.get("summary")
    payload["source_csv"] = str(activity_csv)
    payload["report_db"] = str(args.us_db)
    out_dir = write_outputs(payload, args.output_root, research)
    print(f"My Book Overlay written: {out_dir / 'my_book_overlay_us.md'}")
    return payload


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
