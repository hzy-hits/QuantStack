from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb

from src.paths import FACTOR_LAB_DB


DEFAULT_SLEEVE_ID = "daily_price_overlay"
DEFAULT_REPORT_CONTRACT = "research_only"
DEFAULT_MONEY_READINESS = "research_only"

REPORT_CONTRACTS = {
    "fresh_buy_gate",
    "action_overlay",
    "setup_overlay",
    "risk_warning",
    "hold_overlay",
    "research_only",
}

MONEY_READINESS = {
    "money_ready",
    "money_candidate",
    "research_only",
    "payoff_ledger_required",
    "blocked",
}


def normalize_sleeve_id(value: Any) -> str:
    text = str(value or DEFAULT_SLEEVE_ID).strip().lower()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text)
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return cleaned or DEFAULT_SLEEVE_ID


def normalize_report_contract(value: Any) -> str:
    text = str(value or DEFAULT_REPORT_CONTRACT).strip().lower()
    return text if text in REPORT_CONTRACTS else DEFAULT_REPORT_CONTRACT


def normalize_money_readiness(value: Any, report_contract: str | None = None) -> str:
    text = str(value or "").strip().lower()
    if text in MONEY_READINESS:
        return text
    if normalize_report_contract(report_contract) == "fresh_buy_gate":
        return "money_candidate"
    return DEFAULT_MONEY_READINESS


def json_dumps(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return "{}"
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return json.dumps({"text": stripped}, ensure_ascii=True, sort_keys=True)
        return json.dumps(parsed, ensure_ascii=True, sort_keys=True)
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def metadata_payload(
    *,
    sleeve_id: Any = None,
    mispricing_source: Any = None,
    forced_counterparty: Any = None,
    data_requirements: Any = None,
    failure_mode: Any = None,
    report_contract: Any = None,
    money_readiness: Any = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contract = normalize_report_contract(report_contract)
    readiness = normalize_money_readiness(money_readiness, contract)
    payload = {
        "sleeve_id": normalize_sleeve_id(sleeve_id),
        "mispricing_source": str(mispricing_source or "").strip(),
        "forced_counterparty": str(forced_counterparty or "").strip(),
        "data_requirements_json": json_dumps(data_requirements),
        "failure_mode": str(failure_mode or "").strip(),
        "report_contract": contract,
        "money_readiness": readiness,
    }
    metadata = dict(extra or {})
    metadata.update(
        {
            "sleeve_id": payload["sleeve_id"],
            "report_contract": contract,
            "money_readiness": readiness,
        }
    )
    if payload["mispricing_source"]:
        metadata["mispricing_source"] = payload["mispricing_source"]
    if payload["forced_counterparty"]:
        metadata["forced_counterparty"] = payload["forced_counterparty"]
    if payload["failure_mode"]:
        metadata["failure_mode"] = payload["failure_mode"]
    payload["metadata_json"] = json_dumps(metadata)
    return payload


def apply_factor_metadata_defaults(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = metadata_payload(
        sleeve_id=candidate.get("sleeve_id"),
        mispricing_source=candidate.get("mispricing_source"),
        forced_counterparty=candidate.get("forced_counterparty"),
        data_requirements=candidate.get("data_requirements_json")
        or candidate.get("data_requirements"),
        failure_mode=candidate.get("failure_mode"),
        report_contract=candidate.get("report_contract"),
        money_readiness=candidate.get("money_readiness"),
        extra=candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else None,
    )
    candidate.update(payload)
    return candidate


def _existing_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    except duckdb.Error:
        return set()


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def _ensure_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: dict[str, str],
) -> None:
    if not _table_exists(con, table):
        return
    existing = _existing_columns(con, table)
    for name, ddl in columns.items():
        if name not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def ensure_contract_tables(con: duckdb.DuckDBPyConnection) -> None:
    _ensure_columns(
        con,
        "factor_registry",
        {
            "sleeve_id": f"VARCHAR DEFAULT '{DEFAULT_SLEEVE_ID}'",
            "mispricing_source": "VARCHAR",
            "forced_counterparty": "VARCHAR",
            "data_requirements_json": "VARCHAR",
            "failure_mode": "VARCHAR",
            "report_contract": f"VARCHAR DEFAULT '{DEFAULT_REPORT_CONTRACT}'",
            "money_readiness": f"VARCHAR DEFAULT '{DEFAULT_MONEY_READINESS}'",
            "metadata_json": "VARCHAR",
        },
    )
    if _table_exists(con, "factor_registry"):
        con.execute(
            """
            UPDATE factor_registry
            SET sleeve_id = COALESCE(NULLIF(sleeve_id, ''), ?),
                report_contract = COALESCE(NULLIF(report_contract, ''), ?),
                money_readiness = COALESCE(NULLIF(money_readiness, ''), ?),
                data_requirements_json = COALESCE(NULLIF(data_requirements_json, ''), '{}'),
                metadata_json = COALESCE(NULLIF(metadata_json, ''), '{}')
            """,
            [DEFAULT_SLEEVE_ID, DEFAULT_REPORT_CONTRACT, DEFAULT_MONEY_READINESS],
        )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_experiment_ledger (
            experiment_id VARCHAR PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            session_id VARCHAR,
            market VARCHAR,
            factor_id VARCHAR,
            name VARCHAR,
            formula VARCHAR,
            stage VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            error VARCHAR,
            gates_passed BOOLEAN,
            gate_detail_json VARCHAR,
            oos_result VARCHAR,
            checks_status VARCHAR,
            decision VARCHAR,
            metrics_json VARCHAR,
            sleeve_id VARCHAR DEFAULT 'daily_price_overlay',
            report_contract VARCHAR DEFAULT 'research_only',
            money_readiness VARCHAR DEFAULT 'research_only',
            metadata_json VARCHAR,
            source VARCHAR,
            eval_seconds DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_sleeve_returns (
            return_date DATE NOT NULL,
            market VARCHAR NOT NULL,
            factor_id VARCHAR NOT NULL,
            sleeve_id VARCHAR NOT NULL,
            factor_name VARCHAR,
            report_contract VARCHAR DEFAULT 'research_only',
            money_readiness VARCHAR DEFAULT 'research_only',
            direction VARCHAR,
            bucket VARCHAR NOT NULL,
            gross_return_pct DOUBLE,
            daily_return_pct DOUBLE,
            cost_adjusted_return_pct DOUBLE,
            n_names INTEGER,
            top_bucket_count INTEGER,
            bottom_bucket_count INTEGER,
            cost_pct DOUBLE,
            method VARCHAR,
            detail_json VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (return_date, market, factor_id, bucket)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_health_daily (
            as_of DATE NOT NULL,
            market VARCHAR NOT NULL,
            factor_id VARCHAR NOT NULL,
            rolling_ic_14d DOUBLE,
            rolling_ic_20d DOUBLE,
            rolling_lcb80_14d DOUBLE,
            health_score DOUBLE,
            health_state VARCHAR,
            status_before VARCHAR,
            status_after VARCHAR,
            watch_count INTEGER,
            retire_reason VARCHAR,
            detail_json VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (as_of, market, factor_id)
        )
        """
    )


def factor_id_for(market: str, formula: str) -> str:
    digest = hashlib.sha256(f"{market}:{formula}".encode("utf-8")).hexdigest()[:16]
    return digest


def record_experiment_ledger(
    *,
    market: str | None,
    stage: str,
    status: str,
    formula: str = "",
    name: str = "",
    factor_id: str | None = None,
    session_id: str | None = None,
    error: str | None = None,
    gates_passed: bool | None = None,
    gate_detail: Any = None,
    oos_result: str | None = None,
    checks_status: str | None = None,
    decision: str | None = None,
    metrics: Any = None,
    metadata: dict[str, Any] | None = None,
    source: str | None = None,
    eval_seconds: float | None = None,
) -> str:
    FACTOR_LAB_DB.parent.mkdir(parents=True, exist_ok=True)
    normalized = metadata_payload(**(metadata or {}))
    fid = factor_id or (factor_id_for(market or "", formula) if formula else "")
    experiment_id = uuid.uuid4().hex
    con = duckdb.connect(str(FACTOR_LAB_DB))
    try:
        ensure_contract_tables(con)
        con.execute(
            """
            INSERT INTO factor_experiment_ledger (
                experiment_id, ts, session_id, market, factor_id, name, formula,
                stage, status, error, gates_passed, gate_detail_json, oos_result,
                checks_status, decision, metrics_json, sleeve_id, report_contract,
                money_readiness, metadata_json, source, eval_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                experiment_id,
                datetime.now(timezone.utc),
                session_id,
                market,
                fid,
                name,
                formula,
                stage,
                status,
                error,
                gates_passed,
                json_dumps(gate_detail),
                oos_result,
                checks_status,
                decision,
                json_dumps(metrics),
                normalized["sleeve_id"],
                normalized["report_contract"],
                normalized["money_readiness"],
                normalized["metadata_json"],
                source,
                eval_seconds,
            ],
        )
    finally:
        con.close()
    return experiment_id
