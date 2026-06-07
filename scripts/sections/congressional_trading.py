"""Congressional trading signal overlay for the US daily pipeline.

This section intentionally treats politician trades as policy/flow context:
they can raise catalyst priority or risk review urgency, but they are not AI
source evidence and they do not create production R by themselves.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONGRESSIONAL_TRADING_ROOT = (
    STACK_ROOT / "reports" / "review_dashboard" / "congressional_trading"
)


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("/", "-")
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _date_str(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _clean_cell(value: Any, max_len: int = 120) -> str:
    text = str(value if value is not None else "-").replace("|", "/").replace("\n", " ").strip()
    if len(text) > max_len:
        return text[: max_len - 1] + "…"
    return text or "-"


def _transaction_side(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if any(token in text for token in ["purchase", "buy", "acquisition", "acquired"]):
        return "buy"
    if any(token in text for token in ["sale", "sell", "sold", "disposal", "disposed"]):
        return "sell"
    if text in {"p"}:
        return "buy"
    if text in {"s", "s (partial)", "s (full)"}:
        return "sell"
    return None


def _normalize_committee(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        parts = [str(v).strip() for v in value if str(v or "").strip()]
        return ", ".join(parts) if parts else "unknown committee"
    text = str(value or "").strip()
    return text or "unknown committee"


def _normalize_transaction(row: dict[str, Any], as_of: date) -> dict[str, Any] | None:
    symbol = str(
        _first(row, "symbol", "ticker", "asset_symbol", "ticker_symbol", "stock") or ""
    ).upper().strip()
    if not symbol:
        return None
    side = _transaction_side(_first(row, "transaction_type", "type", "transaction", "action"))
    if side not in {"buy", "sell"}:
        return None
    transaction_date = _parse_date(
        _first(row, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    )
    disclosure_date = _parse_date(
        _first(row, "disclosure_date", "disclosureDate", "filed_date", "filing_date", "report_date", "reportDate")
    )
    if disclosure_date and disclosure_date > as_of:
        return None
    if not disclosure_date and transaction_date and transaction_date > as_of:
        return None
    lawmaker = str(
        _first(row, "lawmaker", "representative", "politician", "name", "member", "owner") or ""
    ).strip() or "unknown lawmaker"
    committee = _normalize_committee(_first(row, "committee", "committees", "committee_name", "committeeName"))
    lag_days = None
    if transaction_date and disclosure_date:
        lag_days = (disclosure_date - transaction_date).days
    source_url = str(_first(row, "source_url", "url", "filing_url", "filingUrl") or "").strip()
    source_name = str(_first(row, "source", "source_name", "provider") or "").strip()
    return {
        "symbol": symbol,
        "transaction_type": side,
        "lawmaker": lawmaker,
        "committee": committee,
        "transaction_date": _date_str(transaction_date),
        "disclosure_date": _date_str(disclosure_date),
        "anchor_date": _date_str(transaction_date or disclosure_date),
        "disclosure_lag_days": lag_days,
        "amount_low": _first(row, "amount_low", "amountLow", "min_amount", "minimum"),
        "amount_high": _first(row, "amount_high", "amountHigh", "max_amount", "maximum"),
        "source": source_name or "congressional_trading_artifact",
        "source_url": source_url,
        "raw_owner": _first(row, "owner", "asset_owner"),
    }


def _candidate_paths(root: Path, as_of: date) -> list[Path]:
    day = as_of.isoformat()
    return [
        root / day / "congressional_trading.json",
        root / day / "congressional_trading.jsonl",
        root / f"{day}.json",
        root / f"{day}.jsonl",
        root / "congressional_trading.json",
        root / "congressional_trading.jsonl",
    ]


def _read_artifact(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            item = json.loads(line)
            if isinstance(item, dict):
                rows.append(item)
        return rows
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ["transactions", "trades", "rows", "records", "data"]:
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _load_transactions(root: Path, as_of: date) -> tuple[str, str | None, list[dict[str, Any]]]:
    if not root.exists():
        return "missing", None, []
    for path in _candidate_paths(root, as_of):
        if not path.exists():
            continue
        rows = _read_artifact(path)
        return ("loaded" if rows else "empty", str(path), rows)
    return "missing", None, []


def _cluster_rows(rows: list[dict[str, Any]], side: str, window_days: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[tuple[date, dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        if row.get("transaction_type") != side:
            continue
        committee = str(row.get("committee") or "unknown committee").strip()
        if committee.lower() == "unknown committee":
            continue
        anchor = _parse_date(row.get("anchor_date"))
        if not anchor:
            continue
        grouped[committee].append((anchor, row))

    clusters: list[dict[str, Any]] = []
    for committee, dated_rows in grouped.items():
        dated_rows.sort(key=lambda item: item[0])
        for idx, (start, _row) in enumerate(dated_rows):
            in_window = [
                item_row
                for item_date, item_row in dated_rows[idx:]
                if (item_date - start).days <= window_days
            ]
            lawmakers = sorted({str(item.get("lawmaker") or "") for item in in_window if item.get("lawmaker")})
            if len(lawmakers) < 2:
                continue
            end_dates = [_parse_date(item.get("anchor_date")) for item in in_window]
            end_dates = [d for d in end_dates if d]
            clusters.append(
                {
                    "side": side,
                    "committee": committee,
                    "lawmakers": lawmakers,
                    "count": len(in_window),
                    "start_date": _date_str(start),
                    "end_date": _date_str(max(end_dates) if end_dates else start),
                }
            )
            break
    return clusters


def _summarize_symbol(
    symbol: str,
    rows: list[dict[str, Any]],
    *,
    as_of: date,
    ai_symbols: set[str],
    fresh_window_days: int,
    cluster_window_days: int,
) -> dict[str, Any]:
    buy_rows = [row for row in rows if row.get("transaction_type") == "buy"]
    sell_rows = [row for row in rows if row.get("transaction_type") == "sell"]
    disclosure_dates = [_parse_date(row.get("disclosure_date")) for row in rows]
    disclosure_dates = [d for d in disclosure_dates if d]
    latest_disclosure = max(disclosure_dates) if disclosure_dates else None
    fresh_disclosure_age_days = (as_of - latest_disclosure).days if latest_disclosure else None
    fresh_disclosure = (
        fresh_disclosure_age_days is not None and 0 <= fresh_disclosure_age_days <= fresh_window_days
    )
    buy_clusters = _cluster_rows(rows, "buy", cluster_window_days)
    sell_clusters = _cluster_rows(rows, "sell", cluster_window_days)
    lags = [row.get("disclosure_lag_days") for row in rows if isinstance(row.get("disclosure_lag_days"), int)]
    avg_lag = round(mean(lags), 1) if lags else None

    if sell_clusters:
        state = "clustered_sell_warning"
        score = -5.0
        read_through = "同委员会多人卖出优先视为政策/事件风险预警；新增仓位前必须让 news/risk 复核。"
    elif buy_clusters:
        state = "multi_member_buy_cluster"
        score = 4.0
        read_through = "同委员会多人短期买入是强催化线索；只能提高观察优先级，不能替代 AI evidence 或执行 R。"
    elif fresh_disclosure and buy_rows:
        state = "fresh_disclosure_buy"
        score = 2.0
        read_through = "刚披露买入有跟单窗口，但需要价格、新闻和期权/Gamma 同步确认。"
    elif fresh_disclosure and sell_rows:
        state = "fresh_disclosure_sell_warning"
        score = -2.5
        read_through = "刚披露卖出是风险复核触发，优先检查政策、财报和 headline 反证。"
    elif buy_rows or sell_rows:
        state = "stale_disclosure_context"
        score = 0.5 if buy_rows and not sell_rows else -0.5 if sell_rows and not buy_rows else 0.0
        read_through = "披露较旧，只能当背景线索；若靠近财报或重大新闻再进入事件复核。"
    else:
        state = "none"
        score = 0.0
        read_through = "无可用交易方向。"

    if fresh_disclosure and state not in {"fresh_disclosure_buy", "fresh_disclosure_sell_warning"}:
        score += 1.0 if buy_rows else -1.0
    ai_member = symbol in ai_symbols
    if not ai_member:
        report_role = "source_review_candidate_only"
    elif sell_clusters or state == "fresh_disclosure_sell_warning":
        report_role = "risk_warning"
    elif buy_clusters or state == "fresh_disclosure_buy":
        report_role = "catalyst_watch"
    else:
        report_role = "context_only"

    clusters = sell_clusters or buy_clusters
    lawmakers = sorted({str(row.get("lawmaker") or "") for row in rows if row.get("lawmaker")})
    committees = sorted({str(row.get("committee") or "") for row in rows if row.get("committee")})
    return {
        "symbol": symbol,
        "state": state,
        "score": round(score, 3),
        "buy_count": len(buy_rows),
        "sell_count": len(sell_rows),
        "unique_lawmakers": len(lawmakers),
        "lawmakers": lawmakers[:8],
        "committees": committees[:5],
        "committee_buy_clusters": buy_clusters,
        "committee_sell_clusters": sell_clusters,
        "cluster_summary": clusters[:3],
        "latest_disclosure_date": _date_str(latest_disclosure),
        "fresh_disclosure_age_days": fresh_disclosure_age_days,
        "fresh_disclosure": fresh_disclosure,
        "avg_disclosure_lag_days": avg_lag,
        "read_through": read_through,
        "report_role": report_role,
        "ai_universe_member": ai_member,
        "contract_note": "Congressional trading is policy/flow context only; it is not source evidence and cannot create production R.",
    }


def build_congressional_trading_snapshot(
    as_of: date,
    *,
    artifact_root: Path | None = None,
    ai_symbols: set[str] | None = None,
    lookback_days: int = 90,
    fresh_window_days: int = 7,
    cluster_window_days: int = 45,
) -> dict[str, Any]:
    """Build the congressional trading overlay from an optional local artifact."""
    root = artifact_root or DEFAULT_CONGRESSIONAL_TRADING_ROOT
    ai_symbols = {str(symbol or "").upper() for symbol in (ai_symbols or set()) if str(symbol or "").strip()}
    status, source_file, raw_rows = _load_transactions(root, as_of)
    normalized = [
        tx
        for row in raw_rows
        if isinstance(row, dict)
        for tx in [_normalize_transaction(row, as_of)]
        if tx is not None
    ]
    cutoff = as_of - timedelta(days=lookback_days)
    filtered = []
    for row in normalized:
        anchor = _parse_date(row.get("disclosure_date")) or _parse_date(row.get("anchor_date"))
        if anchor is None or anchor >= cutoff:
            filtered.append(row)

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in filtered:
        by_symbol[str(row.get("symbol") or "").upper()].append(row)

    rows = [
        _summarize_symbol(
            symbol,
            symbol_rows,
            as_of=as_of,
            ai_symbols=ai_symbols,
            fresh_window_days=fresh_window_days,
            cluster_window_days=cluster_window_days,
        )
        for symbol, symbol_rows in sorted(by_symbol.items())
    ]
    rows.sort(key=lambda row: (row["report_role"] != "risk_warning", -abs(float(row.get("score") or 0)), row["symbol"]))
    return {
        "as_of": as_of.isoformat(),
        "status": status if rows else ("no_data" if status in {"missing", "empty"} else "empty"),
        "source_file": source_file,
        "expected_artifacts": [str(path) for path in _candidate_paths(root, as_of)[:2]],
        "contract": (
            "Congressional trading is a policy/flow overlay. Multi-member same-committee buys can raise "
            "catalyst priority; clustered sells trigger risk review. It never upgrades AI evidence and never "
            "creates production R by itself."
        ),
        "params": {
            "lookback_days": lookback_days,
            "fresh_window_days": fresh_window_days,
            "cluster_window_days": cluster_window_days,
        },
        "summary": {
            "raw_rows": len(raw_rows),
            "normalized_rows": len(normalized),
            "rows_in_lookback": len(filtered),
            "symbols": len(rows),
            "multi_member_buy_clusters": sum(1 for row in rows if row.get("committee_buy_clusters")),
            "clustered_sell_warnings": sum(1 for row in rows if row.get("committee_sell_clusters")),
            "fresh_disclosures": sum(1 for row in rows if row.get("fresh_disclosure")),
            "non_ai_context_symbols": sum(1 for row in rows if not row.get("ai_universe_member")),
        },
        "rows": rows,
        "transactions": filtered[:500],
    }


def render_congressional_trading_section(payload: dict[str, Any], *, limit: int = 12) -> list[str]:
    snapshot = payload.get("congressional_trading") or {}
    summary = snapshot.get("summary") or {}
    lines = [
        "## Congressional Trading / 政策资金流",
        "",
        (
            "- Contract: 国会议员交易只作为政策/资金流和事件风险 overlay；不改变 AI source evidence，"
            "不把非 AI ticker 升级为 production candidate，也不直接生成 R。"
        ),
    ]
    status = snapshot.get("status") or "no_data"
    if status in {"missing", "empty", "no_data"} or not snapshot.get("rows"):
        expected = ", ".join(snapshot.get("expected_artifacts") or [])
        lines += [
            f"- Status: `NO_CONGRESSIONAL_TRADING_DATA`; expected artifact: {expected or '-'}",
            "- Read-through: 暂无可核验的国会议员交易 artifact，本日报不能引用 PelosiTracker、Quiver 或单条社媒披露作为事实。",
            "",
        ]
        return lines

    lines += [
        (
            f"- Source: `{snapshot.get('source_file') or '-'}`; rows={summary.get('rows_in_lookback', 0)}, "
            f"buy-clusters={summary.get('multi_member_buy_clusters', 0)}, "
            f"sell-warnings={summary.get('clustered_sell_warnings', 0)}, "
            f"fresh={summary.get('fresh_disclosures', 0)}."
        ),
        "",
        "| Symbol | Signal | Lawmakers / committee | Disclosure lag | Read-through | Report role |",
        "|---|---|---|---:|---|---|",
    ]
    for row in (snapshot.get("rows") or [])[:limit]:
        clusters = row.get("cluster_summary") or []
        if clusters:
            cluster = clusters[0]
            people = ", ".join(cluster.get("lawmakers") or row.get("lawmakers") or [])
            who = f"{people} / {cluster.get('committee') or '-'}"
        else:
            people = ", ".join(row.get("lawmakers") or [])
            committees = ", ".join(row.get("committees") or [])
            who = f"{people or '-'} / {committees or '-'}"
        lag = row.get("avg_disclosure_lag_days")
        lag_text = f"{lag}d" if lag is not None else "-"
        latest = row.get("latest_disclosure_date") or "-"
        if row.get("fresh_disclosure_age_days") is not None:
            latest = f"{latest} ({row.get('fresh_disclosure_age_days')}d fresh)"
        lines.append(
            "| "
            + " | ".join(
                [
                    _clean_cell(row.get("symbol"), 18),
                    _clean_cell(row.get("state"), 40),
                    _clean_cell(who, 90),
                    _clean_cell(f"{lag_text}; latest {latest}", 42),
                    _clean_cell(row.get("read_through"), 110),
                    _clean_cell(row.get("report_role"), 36),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines
