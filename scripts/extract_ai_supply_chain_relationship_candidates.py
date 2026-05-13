#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb"
DEFAULT_THEME_SEED = STACK_ROOT / "data" / "us_theme_seed_map.yaml"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_supply_chain_candidates"

RAW_RELATIONSHIP_FIELDS = [
    "relationship_id",
    "as_of",
    "market",
    "primary_symbol",
    "counterparty_symbol",
    "customer_symbol",
    "symbols",
    "layer",
    "relationship_type",
    "supply_chain_role",
    "bottleneck_focus",
    "source_name",
    "source_type",
    "source_url",
    "source_date",
    "confidence",
    "notes",
]
REVIEW_FIELDS = [
    "review_state",
    "candidate_score",
    "ai_terms",
    "relationship_terms",
    "counterparty_terms",
    "headline",
    "evidence_text",
    "source_table",
]

GENERIC_NOISE_PHRASES = {
    "money supply",
    "liquidity supply",
    "supply and demand",
    "oil supply",
    "gas supply",
    "food supply",
    "labor supply",
    "housing supply",
    "treasury supply",
    "供应量",
    "流动性供给",
    "货币供应",
}
NEGATIVE_RELATIONSHIP_NOISE = {
    "rumor",
    "rumour",
    "fake",
    "false claim",
    "fabricated",
    "made up",
    "hoax",
    "scam",
    "谣言",
    "传言",
    "编造",
    "造谣",
    "虚假",
    "被罚",
}
AI_TERMS = {
    "ai",
    "artificial intelligence",
    "generative ai",
    "agentic ai",
    "llm",
    "large language model",
    "inference",
    "gpu",
    "tpu",
    "accelerator",
    "ai server",
    "hbm",
    "dram",
    "nand",
    "ssd",
    "hdd",
    "storage",
    "datacenter",
    "data center",
    "cloud",
    "edge ai",
    "optical",
    "optics",
    "cpo",
    "coherent",
    "transceiver",
    "fiber",
    "ethernet",
    "semiconductor",
    "advanced packaging",
    "substrate",
    "wafer",
    "gallium arsenide",
    "gaas",
    "indium phosphide",
    "inp",
    "nuclear",
    "power purchase",
    "ppa",
    "grid",
    "supercomputer",
    "satellite",
    "orbital",
    "算力",
    "人工智能",
    "大模型",
    "推理",
    "芯片",
    "服务器",
    "数据中心",
    "光模块",
    "光通信",
    "光纤",
    "cpo",
    "存储",
    "半导体",
    "封装",
    "测试",
    "衬底",
    "晶圆",
    "电力",
    "核电",
    "电网",
    "超算",
    "卫星",
    "商业航天",
}
RELATIONSHIP_TERMS = {
    "partner",
    "partners",
    "partnership",
    "collaborate",
    "collaboration",
    "agreement",
    "contract",
    "selected",
    "chosen",
    "design win",
    "order",
    "orders",
    "purchase order",
    "supply agreement",
    "supplier",
    "supplies",
    "provide",
    "provides",
    "deploy",
    "deploys",
    "integrate",
    "integrates",
    "powered by",
    "power purchase agreement",
    "ppa",
    "中标",
    "订单",
    "合同",
    "协议",
    "合作",
    "战略合作",
    "供应商",
    "供应",
    "采购",
    "供货",
    "客户",
    "导入",
    "认证",
    "部署",
    "建设",
    "联合",
}
LAYER_TERMS = {
    "ai_labs_cloud_models": {
        "llm",
        "large language model",
        "model",
        "cloud",
        "pytorch",
        "deepmind",
        "open model",
        "大模型",
        "云",
        "模型",
    },
    "ai_compute_accelerators": {
        "gpu",
        "tpu",
        "accelerator",
        "ai server",
        "server",
        "foundry",
        "compute",
        "算力",
        "芯片",
        "服务器",
    },
    "ai_networking_optical_cpo": {
        "optical",
        "optics",
        "cpo",
        "coherent",
        "transceiver",
        "fiber",
        "ethernet",
        "switch",
        "光模块",
        "光通信",
        "光纤",
        "交换机",
    },
    "ai_memory_storage": {
        "hbm",
        "dram",
        "nand",
        "ssd",
        "hdd",
        "storage",
        "memory",
        "存储",
        "内存",
    },
    "ai_chip_equipment_materials_packaging": {
        "advanced packaging",
        "packaging",
        "test",
        "testing",
        "substrate",
        "wafer",
        "material",
        "gallium arsenide",
        "gaas",
        "indium phosphide",
        "inp",
        "semicap",
        "封装",
        "测试",
        "衬底",
        "晶圆",
        "材料",
    },
    "ai_datacenter_edge_infra": {
        "datacenter",
        "data center",
        "edge",
        "inference",
        "cooling",
        "cloudflare",
        "internet infrastructure",
        "数据中心",
        "边缘",
        "冷却",
    },
    "ai_power_nuclear_grid": {
        "power",
        "nuclear",
        "grid",
        "reactor",
        "power purchase",
        "ppa",
        "electricity",
        "电力",
        "核电",
        "电网",
        "变压器",
    },
    "space_connectivity_datacenter": {
        "space",
        "satellite",
        "orbital",
        "launch",
        "defense",
        "卫星",
        "商业航天",
        "航天",
    },
}
MANUAL_ALIASES = {
    "NVDA": {"nvidia", "英伟达"},
    "AMD": {"amd", "advanced micro devices"},
    "GOOGL": {"google", "alphabet", "deepmind"},
    "META": {"meta", "facebook", "pytorch"},
    "MSFT": {"microsoft", "azure", "微软"},
    "AMZN": {"amazon", "aws", "亚马逊"},
    "ORCL": {"oracle"},
    "ARM": {"arm holdings"},
    "NET": {"cloudflare"},
    "DELL": {"dell"},
    "HPE": {"hewlett packard enterprise", "hpe"},
    "MU": {"micron", "美光"},
    "WDC": {"western digital", "sandisk"},
    "STX": {"seagate", "希捷"},
    "NTAP": {"netapp"},
    "ANET": {"arista"},
    "AVGO": {"broadcom", "博通"},
    "MRVL": {"marvell"},
    "COHR": {"coherent"},
    "LITE": {"lumentum"},
    "CIEN": {"ciena"},
    "AAOI": {"applied optoelectronics"},
    "AXTI": {"axt", "axti"},
    "GLW": {"corning"},
    "CRWV": {"coreweave"},
    "IREN": {"iren"},
    "RXT": {"rackspace"},
    "CEG": {"constellation energy"},
    "SMCI": {"super micro", "supermicro"},
    "TSM": {"tsmc", "taiwan semiconductor"},
}


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def term_in_text(term: str, text: str) -> bool:
    needle = normalize_text(term)
    haystack = normalize_text(text)
    if not needle or not haystack:
        return False
    if re.search(r"[\u4e00-\u9fff]", needle):
        return needle in haystack
    return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack) is not None


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(20\d{2})[-/]?(\d{1,2})[-/]?(\d{1,2})", text)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return date(year, month, day)


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower(?)",
            [table],
        ).fetchone()[0]
    )


def rows_as_dicts(con: duckdb.DuckDBPyConnection, query: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(query, params)
    cols = [item[0] for item in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_symbol_aliases(seed_path: Path = DEFAULT_THEME_SEED) -> tuple[dict[str, set[str]], dict[str, dict[str, Any]]]:
    aliases: dict[str, set[str]] = {symbol: set(values) for symbol, values in MANUAL_ALIASES.items()}
    metadata: dict[str, dict[str, Any]] = {}
    if seed_path.exists():
        payload = yaml.safe_load(seed_path.read_text(encoding="utf-8")) or {}
        for theme in payload.get("themes") or []:
            for symbol in theme.get("members") or []:
                normalized = str(symbol or "").strip().upper()
                if not normalized:
                    continue
                if len(normalized) >= 4:
                    aliases.setdefault(normalized, set()).add(normalized.lower())
                else:
                    aliases.setdefault(normalized, set())
                metadata.setdefault(
                    normalized,
                    {
                        "layer": theme.get("supercycle_layer"),
                        "supply_chain_role": theme.get("supply_chain_role"),
                        "bottleneck_focus": theme.get("bottleneck_focus"),
                    },
                )
    for symbol in list(aliases):
        if len(symbol) >= 4:
            aliases[symbol].add(symbol.lower())
    return aliases, metadata


def matched_terms(terms: set[str], text: str) -> list[str]:
    return sorted(term for term in terms if term_in_text(term, text))


def relationship_type_for(hits: list[str]) -> str:
    hit_set = set(hits)
    if {"ppa", "power purchase agreement"} & hit_set:
        return "power_purchase_candidate"
    if {"contract", "order", "orders", "purchase order", "中标", "订单", "合同"} & hit_set:
        return "contract_or_order_candidate"
    if {"supplier", "supplies", "supply agreement", "供应商", "供货", "供应"} & hit_set:
        return "supplier_candidate"
    if {"partner", "partners", "partnership", "collaborate", "collaboration", "合作", "战略合作"} & hit_set:
        return "partnership_candidate"
    if {"deploy", "deploys", "integrate", "integrates", "部署", "导入", "认证"} & hit_set:
        return "deployment_or_integration_candidate"
    return "relationship_candidate_from_news"


def layer_for(text: str, primary_symbol: str, metadata: dict[str, dict[str, Any]]) -> str:
    scored: list[tuple[int, str]] = []
    for layer, terms in LAYER_TERMS.items():
        hits = matched_terms(terms, text)
        if hits:
            scored.append((len(hits), layer))
    if scored:
        scored.sort(key=lambda item: (-item[0], item[1]))
        return scored[0][1]
    return str((metadata.get(primary_symbol.upper()) or {}).get("layer") or "ai_supercycle_needs_classification")


def counterparties_for(
    text: str,
    primary_symbol: str,
    aliases: dict[str, set[str]],
) -> tuple[list[str], list[str]]:
    counterparties: list[str] = []
    hit_terms: list[str] = []
    for symbol, terms in aliases.items():
        if symbol.upper() == primary_symbol.upper():
            continue
        matched = [term for term in sorted(terms) if term_in_text(term, text)]
        if matched:
            counterparties.append(symbol.upper())
            hit_terms.extend(f"{symbol}:{term}" for term in matched[:3])
    return sorted(set(counterparties)), sorted(set(hit_terms))


def primary_alias_in_text(primary_symbol: str, text: str, aliases: dict[str, set[str]]) -> bool:
    terms = aliases.get(primary_symbol.upper()) or {primary_symbol.lower()}
    return any(term_in_text(term, text) for term in terms)


def source_date_text(value: Any) -> str:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else str(value or "")[:10]


def candidate_id(market: str, symbol: str, source_date: str, headline: str, url: str) -> str:
    basis = "|".join([market.upper(), symbol.upper(), source_date, normalize_text(headline), url])
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]
    return f"{market.lower()}_{symbol.lower()}_{source_date.replace('-', '')}_{digest}"


def build_candidate(
    row: dict[str, Any],
    market: str,
    as_of: date,
    aliases: dict[str, set[str]],
    metadata: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    primary_symbol = str(row.get("symbol") or "").strip().upper()
    headline = str(row.get("headline") or "").strip()
    summary = str(row.get("summary") or "").strip()
    if not primary_symbol or not headline:
        return None
    text = f"{headline} {summary}"
    if any(term_in_text(term, text) for term in GENERIC_NOISE_PHRASES):
        return None
    if any(term_in_text(term, text) for term in NEGATIVE_RELATIONSHIP_NOISE):
        return None
    ai_hits = matched_terms(AI_TERMS, text)
    relationship_hits = matched_terms(RELATIONSHIP_TERMS, text)
    if not ai_hits or not relationship_hits:
        return None
    counterparty_symbols, counterparty_hits = counterparties_for(text, primary_symbol, aliases)
    primary_bound = primary_alias_in_text(primary_symbol, headline, aliases)
    if market.upper() == "US" and not primary_bound:
        return None
    layer = layer_for(text, primary_symbol, metadata)
    if layer == "ai_supercycle_needs_classification":
        return None
    primary_meta = metadata.get(primary_symbol) or {}
    source_date = source_date_text(row.get("published_at"))
    symbols = sorted({primary_symbol, *counterparty_symbols})
    score = min(1.0, 0.25 + 0.06 * len(ai_hits) + 0.08 * len(relationship_hits) + 0.08 * len(counterparty_symbols))
    evidence_text = " ".join(part for part in [headline, summary] if part)
    return {
        "relationship_id": candidate_id(market, primary_symbol, source_date, headline, str(row.get("url") or "")),
        "as_of": as_of.isoformat(),
        "market": market.upper(),
        "primary_symbol": primary_symbol,
        "counterparty_symbol": counterparty_symbols[0] if counterparty_symbols else "",
        "customer_symbol": "",
        "symbols": ";".join(symbols),
        "layer": layer,
        "relationship_type": relationship_type_for(relationship_hits),
        "supply_chain_role": primary_meta.get("supply_chain_role") or f"{layer} relationship candidate",
        "bottleneck_focus": primary_meta.get("bottleneck_focus") or "",
        "source_name": row.get("source") or row.get("source_table") or "local_news",
        "source_type": "news_review_candidate",
        "source_url": row.get("url") or "",
        "source_date": source_date,
        "confidence": "unreviewed",
        "notes": "Auto-extracted relationship candidate. Human source review is required before promotion.",
        "review_state": "needs_human_source_review",
        "candidate_score": round(score, 4),
        "ai_terms": ";".join(ai_hits[:12]),
        "relationship_terms": ";".join(relationship_hits[:12]),
        "counterparty_terms": ";".join(counterparty_hits[:12]),
        "headline": headline,
        "evidence_text": evidence_text[:600],
        "source_table": row.get("source_table") or "",
    }


def query_us_news(db_path: Path, start: date, end: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "news_items"):
            return []
        return rows_as_dicts(
            con,
            """
            SELECT symbol, headline, summary, source, url, published_at, 'news_items' AS source_table
            FROM news_items
            WHERE published_at >= CAST(? AS TIMESTAMP)
              AND published_at < CAST(? AS TIMESTAMP)
            ORDER BY published_at DESC, symbol
            """,
            [start.isoformat(), (end + timedelta(days=1)).isoformat()],
        )
    finally:
        con.close()


def query_us_sec_filings(db_path: Path, start: date, end: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "sec_filings"):
            return []
        return rows_as_dicts(
            con,
            """
            SELECT symbol, form_type, filed_date, items, description, filing_url
            FROM sec_filings
            WHERE filed_date >= CAST(? AS DATE)
              AND filed_date <= CAST(? AS DATE)
              AND form_type IN ('8-K', '6-K')
              AND (
                    lower(coalesce(description, '') || ' ' || coalesce(items, '')) LIKE '%material definitive agreement%'
                 OR lower(coalesce(description, '') || ' ' || coalesce(items, '')) LIKE '%material agreement%'
                 OR lower(coalesce(description, '') || ' ' || coalesce(items, '')) LIKE '%other events%'
              )
            ORDER BY filed_date DESC, symbol
            """,
            [start.isoformat(), end.isoformat()],
        )
    finally:
        con.close()


def query_cn_news(db_path: Path, start: date, end: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    rows: list[dict[str, Any]] = []
    try:
        if table_exists(con, "stock_news"):
            rows.extend(
                rows_as_dicts(
                    con,
                    """
                    SELECT ts_code AS symbol, title AS headline, content AS summary, source, url,
                           TRY_CAST(publish_time AS TIMESTAMP) AS published_at,
                           'stock_news' AS source_table
                    FROM stock_news
                    WHERE TRY_CAST(publish_time AS TIMESTAMP) >= CAST(? AS TIMESTAMP)
                      AND TRY_CAST(publish_time AS TIMESTAMP) < CAST(? AS TIMESTAMP)
                    ORDER BY TRY_CAST(publish_time AS TIMESTAMP) DESC, ts_code
                    """,
                    [start.isoformat(), (end + timedelta(days=1)).isoformat()],
                )
            )
        if table_exists(con, "news_enriched"):
            rows.extend(
                rows_as_dicts(
                    con,
                    """
                    SELECT ts_code AS symbol, headline, summary_one_line AS summary,
                           event_type AS source, '' AS url,
                           TRY_CAST(published_at AS TIMESTAMP) AS published_at,
                           'news_enriched' AS source_table
                    FROM news_enriched
                    WHERE TRY_CAST(published_at AS TIMESTAMP) >= CAST(? AS TIMESTAMP)
                      AND TRY_CAST(published_at AS TIMESTAMP) < CAST(? AS TIMESTAMP)
                    ORDER BY TRY_CAST(published_at AS TIMESTAMP) DESC, ts_code
                    """,
                    [start.isoformat(), (end + timedelta(days=1)).isoformat()],
                )
            )
    finally:
        con.close()
    return rows


def build_sec_candidate(row: dict[str, Any], as_of: date, metadata: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    primary_symbol = str(row.get("symbol") or "").strip().upper()
    if not primary_symbol or primary_symbol not in metadata:
        return None
    primary_meta = metadata.get(primary_symbol) or {}
    layer = str(primary_meta.get("layer") or "ai_supercycle_needs_classification")
    if layer == "ai_supercycle_needs_classification":
        return None
    source_date = source_date_text(row.get("filed_date"))
    form_type = str(row.get("form_type") or "").strip()
    description = str(row.get("description") or "").strip()
    items = str(row.get("items") or "").strip()
    headline = f"{primary_symbol} {form_type} {description}".strip()
    evidence_text = f"{headline}. {items}".strip()
    return {
        "relationship_id": candidate_id("US", primary_symbol, source_date, headline, str(row.get("filing_url") or "")),
        "as_of": as_of.isoformat(),
        "market": "US",
        "primary_symbol": primary_symbol,
        "counterparty_symbol": "",
        "customer_symbol": "",
        "symbols": primary_symbol,
        "layer": layer,
        "relationship_type": "sec_material_agreement_candidate",
        "supply_chain_role": primary_meta.get("supply_chain_role") or f"{layer} SEC filing candidate",
        "bottleneck_focus": primary_meta.get("bottleneck_focus") or "",
        "source_name": f"SEC {form_type}".strip(),
        "source_type": "sec_filing_review_candidate",
        "source_url": row.get("filing_url") or "",
        "source_date": source_date,
        "confidence": "unreviewed",
        "notes": "SEC metadata candidate only. Open the filing and verify the counterparty/economic link before promotion.",
        "review_state": "needs_human_source_review",
        "candidate_score": 0.44 if primary_meta.get("layer") else 0.36,
        "ai_terms": layer,
        "relationship_terms": "material agreement",
        "counterparty_terms": "",
        "headline": headline,
        "evidence_text": evidence_text[:600],
        "source_table": "sec_filings",
    }


def extract_candidates(
    us_db: Path = DEFAULT_US_DB,
    cn_db: Path = DEFAULT_CN_DB,
    as_of: date | None = None,
    lookback_days: int = 14,
    seed_path: Path = DEFAULT_THEME_SEED,
) -> list[dict[str, Any]]:
    effective_as_of = as_of or date.today()
    start = effective_as_of - timedelta(days=max(lookback_days, 0))
    aliases, metadata = load_symbol_aliases(seed_path)
    candidates: list[dict[str, Any]] = []
    for row in query_us_news(us_db, start, effective_as_of):
        candidate = build_candidate(row, "US", effective_as_of, aliases, metadata)
        if candidate:
            candidates.append(candidate)
    for row in query_us_sec_filings(us_db, start, effective_as_of):
        candidate = build_sec_candidate(row, effective_as_of, metadata)
        if candidate:
            candidates.append(candidate)
    for row in query_cn_news(cn_db, start, effective_as_of):
        candidate = build_candidate(row, "CN", effective_as_of, aliases, metadata)
        if candidate:
            candidates.append(candidate)
    deduped: dict[str, dict[str, Any]] = {}
    for row in candidates:
        key = row["relationship_id"]
        if key not in deduped or row["candidate_score"] > deduped[key]["candidate_score"]:
            deduped[key] = row
    out = sorted(
        deduped.values(),
        key=lambda item: (
            str(item.get("market") or ""),
            -float(item.get("candidate_score") or 0.0),
            str(item.get("source_date") or ""),
            str(item.get("primary_symbol") or ""),
        ),
    )
    return out


def write_outputs(candidates: list[dict[str, Any]], output_dir: Path, as_of: date) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "ai_supply_chain_relationship_candidates.csv"
    json_path = output_dir / "ai_supply_chain_relationship_candidates.json"
    md_path = output_dir / "ai_supply_chain_relationship_candidates.md"
    fieldnames = RAW_RELATIONSHIP_FIELDS + REVIEW_FIELDS
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in candidates:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    payload = {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "contract": (
            "These are unreviewed news/SEC-metadata relationship candidates. They must not be inserted into "
            "data/ai_supply_chain_relationships.yaml until a human verifies source_url/source_type/confidence "
            "and marks the row source_confirmed."
        ),
        "summary": {
            "rows": len(candidates),
            "needs_human_source_review": sum(
                1 for row in candidates if row.get("review_state") == "needs_human_source_review"
            ),
            "markets": sorted({row.get("market") for row in candidates}),
        },
        "rows": candidates,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")
    md_path.write_text(render_review_brief(payload), encoding="utf-8")
    return csv_path, json_path, md_path


def clean_md(value: Any, limit: int = 120) -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > limit:
        return text[: max(limit - 3, 0)] + "..."
    return text


def _counter(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "-")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def render_review_brief(payload: dict[str, Any], limit: int = 35) -> str:
    rows = list(payload.get("rows") or [])
    summary = payload.get("summary") or {}
    lines = [
        f"# AI Supply Chain Relationship Candidates - {payload.get('as_of')}",
        "",
        "This is a review queue, not confirmed evidence. A row can be promoted only after the source is opened, the relationship is verified, `review_state` is changed to `source_confirmed`, `source_type` is changed from `*_review_candidate` to the real source type, and `confidence` is set to high or medium.",
        "CSV/JSON keep every raw evidence row; this Markdown brief deduplicates repeated headlines by primary/counterparty/layer/type so review starts with the most useful checks.",
        "",
        f"- rows: {summary.get('rows', 0)}",
        f"- markets: {', '.join(summary.get('markets') or []) or '-'}",
        f"- needs human review: {summary.get('needs_human_source_review', 0)}",
        "",
        "## Source Mix",
        "",
    ]
    for key, count in _counter(rows, "source_table").items():
        lines.append(f"- {key}: {count}")
    lines += ["", "## Layer Mix", ""]
    for key, count in _counter(rows, "layer").items():
        lines.append(f"- {key}: {count}")
    lines += [
        "",
        "## Highest-Priority Checks",
        "",
        "| Rank | Market | Primary | Counterparty | Layer | Type | Score | Source | Headline |",
        "|---:|---|---|---|---|---|---:|---|---|",
    ]
    representative: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("market") or ""),
            str(row.get("primary_symbol") or ""),
            str(row.get("counterparty_symbol") or ""),
            str(row.get("layer") or ""),
            str(row.get("relationship_type") or ""),
        )
        current = representative.get(key)
        if current is None or float(row.get("candidate_score") or 0.0) > float(current.get("candidate_score") or 0.0):
            representative[key] = row
    ranked = sorted(
        representative.values(),
        key=lambda row: (
            -float(row.get("candidate_score") or 0.0),
            str(row.get("market") or ""),
            str(row.get("primary_symbol") or ""),
        ),
    )
    for idx, row in enumerate(ranked[:limit], start=1):
        source_url = str(row.get("source_url") or "").strip()
        source = clean_md(row.get("source_name") or row.get("source_table"), 32)
        if source_url.startswith("http"):
            source = f"[{source}]({source_url})"
        lines.append(
            f"| {idx} | {row.get('market') or '-'} | {row.get('primary_symbol') or '-'} | "
            f"{row.get('counterparty_symbol') or '-'} | {clean_md(row.get('layer'), 38)} | "
            f"{clean_md(row.get('relationship_type'), 36)} | {float(row.get('candidate_score') or 0.0):.2f} | "
            f"{source} | {clean_md(row.get('headline'), 100)} |"
        )
    lines += [
        "",
        "## Promotion Checklist",
        "",
        "- Open `source_url` and confirm the actual counterparty/customer/supplier relationship.",
        "- Replace `source_type` with the real source type, for example `official_press_release`, `sec_filing`, `earnings_transcript`, or `credible_news`.",
        "- Set `review_state=source_confirmed` and `confidence=high` or `medium` only after verification.",
        "- Re-run `scripts/build_ai_supply_chain_relationships.py`; unreviewed rows must be rejected.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract unreviewed AI supply-chain relationship candidates from local US/CN news tables."
    )
    parser.add_argument("--as-of", default=date.today().isoformat(), help="As-of date, YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--us-db", default=DEFAULT_US_DB, type=Path)
    parser.add_argument("--cn-db", default=DEFAULT_CN_DB, type=Path)
    parser.add_argument("--seed", default=DEFAULT_THEME_SEED, type=Path)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT, type=Path)
    args = parser.parse_args()

    as_of = parse_date(args.as_of)
    if as_of is None:
        raise SystemExit(f"invalid --as-of date: {args.as_of}")
    candidates = extract_candidates(args.us_db, args.cn_db, as_of, args.lookback_days, args.seed)
    output_dir = args.output_root / as_of.isoformat()
    csv_path, json_path, md_path = write_outputs(candidates, output_dir, as_of)
    print(f"AI supply-chain relationship candidates written: {csv_path} rows={len(candidates)}")
    print(f"JSON: {json_path}")
    print(f"Review brief: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
