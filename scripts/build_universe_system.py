#!/usr/bin/env python3
"""Build the local AI Infra universe database and static dashboard.

This script intentionally uses only Python standard-library modules. It treats
the ChatGPT Pro JSONL export as a research universe seed, not as verified
investment data.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PRIVATE_INPUT = Path("data/global_universe_v2.jsonl")
PUBLIC_SAMPLE_INPUT = Path("data/seed/global_universe_sample.jsonl")
DEFAULT_INPUT = PRIVATE_INPUT if PRIVATE_INPUT.exists() else PUBLIC_SAMPLE_INPUT
REQUIRED_FIELDS = [
    "ticker",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "current_pool",
]

CORE_CSV_FIELDS = [
    "ticker",
    "company",
    "market_country",
    "asset_pool",
    "bfs_depth",
    "module",
    "current_pool",
    "total_score",
    "score_bucket",
    "evidence_state",
    "counterevidence",
]


@dataclass(frozen=True)
class Score:
    ticker: str
    bfs_score: int
    pool_score: int
    evidence_score: int
    edge_score: int
    risk_penalty: int
    total_score: int
    score_bucket: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build AI Infra universe research artifacts.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input JSONL universe file.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Output data directory.")
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"), help="Output reports directory.")
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Line {line_no} is not valid JSON: {exc}") from exc
            row = {str(k): "" if v is None else str(v).strip() for k, v in raw.items()}
            missing = [field for field in REQUIRED_FIELDS if not row.get(field)]
            if missing:
                raise ValueError(f"Line {line_no} missing required fields: {', '.join(missing)}")
            rows.append(row)
    return rows


def depth_numbers(depth: str) -> list[int]:
    return [int(value) for value in re.findall(r"D(\d+)", depth)]


def depth_range(depth: str) -> tuple[int, int]:
    values = depth_numbers(depth)
    if not values:
        return (99, 99)
    return (min(values), max(values))


def split_edge_text(text: str) -> list[str]:
    if not text:
        return []
    pieces = re.split(r"[、,，;；/|]+", text)
    return [piece.strip() for piece in pieces if piece.strip()]


def bfs_score(depth: str) -> int:
    low, high = depth_range(depth)
    if low == 99:
        return 20
    if high > 5:
        return 0
    if low == 0 and high <= 1:
        return 62
    if low == 1 and high == 1:
        return 68
    if low == 1 and high == 2:
        return 78
    if low == 1 and high == 3:
        return 82
    if low <= 1 and high >= 4:
        return 65
    if low == 2 and high == 2:
        return 88
    if low == 2 and high == 3:
        return 94
    if low == 2 and high == 4:
        return 72
    if low == 3 and high == 3:
        return 90
    if low == 3 and high == 4:
        return 70
    if low == 3 and high >= 5:
        return 55
    if low == 4 and high == 4:
        return 45
    if low == 4 and high >= 5:
        return 35
    return 50


def pool_score(current_pool: str) -> int:
    if "排除" in current_pool:
        return -35
    if "核心池" in current_pool or "核心候选" in current_pool:
        return 20
    if "核心beta" in current_pool:
        return 14
    if "候选" in current_pool and "雷达" in current_pool:
        return 8
    if "候选" in current_pool:
        return 12
    if "雷达" in current_pool:
        return 3
    return 0


def evidence_score(evidence_state: str) -> int:
    if "原文已证明" in evidence_state:
        return 20
    if "合理推论" in evidence_state:
        return 12
    if "待原文核验" in evidence_state:
        return 6
    if "原文需核验" in evidence_state:
        return 5
    if "排除" in evidence_state:
        return 0
    return 3 if evidence_state else 0


def edge_score(row: dict[str, str]) -> int:
    edge_count = len(set(split_edge_text(row.get("dependency_edge", ""))))
    if row.get("dependency_path"):
        edge_count += 1
    if row.get("overseas_bottleneck"):
        edge_count += 1
    if row.get("up_downstream"):
        edge_count += 1
    if edge_count == 0:
        return 0
    return min(20, 4 + edge_count * 4)


def risk_penalty(row: dict[str, str]) -> int:
    text = " ".join(
        [
            row.get("counterevidence", ""),
            row.get("evidence_state", ""),
            row.get("current_pool", ""),
            row.get("trading_reach", ""),
        ]
    )
    penalty = 0
    if row.get("counterevidence"):
        penalty += 5
    for keyword in [
        "排除",
        "创业板",
        "科创板",
        "高杠杆",
        "客户集中",
        "价格战",
        "供给过剩",
        "融资",
        "债务",
        "流动性",
        "技术路线",
        "毛利低",
        "周期",
        "监管",
        "地缘",
    ]:
        if keyword in text:
            penalty += 3
    _, high = depth_range(row.get("bfs_depth", ""))
    if high > 5:
        penalty += 50
    if "排除" in row.get("current_pool", ""):
        penalty += 40
    return min(80, penalty)


def score_bucket(row: dict[str, str], total_score: int) -> str:
    low, high = depth_range(row.get("bfs_depth", ""))
    current_pool = row.get("current_pool", "")
    if "排除" in current_pool or high > 5:
        return "exclude"
    if low >= 4:
        return "radar"
    if total_score >= 80:
        return "core_review"
    if total_score >= 65:
        return "high_priority"
    if total_score >= 50:
        return "radar"
    return "low_priority"


def build_score(row: dict[str, str]) -> Score:
    b_score = bfs_score(row.get("bfs_depth", ""))
    p_score = pool_score(row.get("current_pool", ""))
    ev_score = evidence_score(row.get("evidence_state", ""))
    e_score = edge_score(row)
    penalty = risk_penalty(row)
    total = round(0.70 * b_score + p_score + ev_score + e_score - penalty)
    total = max(0, min(100, total))
    return Score(
        ticker=row["ticker"],
        bfs_score=b_score,
        pool_score=p_score,
        evidence_score=ev_score,
        edge_score=e_score,
        risk_penalty=penalty,
        total_score=total,
        score_bucket=score_bucket(row, total),
    )


def connect_database(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE companies (
            ticker TEXT PRIMARY KEY,
            market_country TEXT NOT NULL,
            asset_pool TEXT NOT NULL,
            company TEXT NOT NULL,
            mcap_bucket TEXT,
            bfs_depth TEXT NOT NULL,
            module TEXT,
            current_pool TEXT NOT NULL
        );

        CREATE TABLE dependency_edges (
            ticker TEXT PRIMARY KEY,
            dependency_path TEXT,
            dependency_edge TEXT,
            overseas_bottleneck TEXT,
            up_downstream TEXT,
            FOREIGN KEY (ticker) REFERENCES companies(ticker)
        );

        CREATE TABLE research_signals (
            ticker TEXT PRIMARY KEY,
            evidence_state TEXT,
            etf_clue TEXT,
            smart_money_clue TEXT,
            counterevidence TEXT,
            trading_reach TEXT,
            verification_status TEXT NOT NULL,
            FOREIGN KEY (ticker) REFERENCES companies(ticker)
        );

        CREATE TABLE scores (
            ticker TEXT PRIMARY KEY,
            bfs_score INTEGER NOT NULL,
            pool_score INTEGER NOT NULL,
            evidence_score INTEGER NOT NULL,
            edge_score INTEGER NOT NULL,
            risk_penalty INTEGER NOT NULL,
            total_score INTEGER NOT NULL,
            score_bucket TEXT NOT NULL,
            FOREIGN KEY (ticker) REFERENCES companies(ticker)
        );

        CREATE INDEX idx_companies_asset_pool ON companies(asset_pool);
        CREATE INDEX idx_companies_bfs_depth ON companies(bfs_depth);
        CREATE INDEX idx_companies_current_pool ON companies(current_pool);
        CREATE INDEX idx_scores_total ON scores(total_score DESC);
        CREATE INDEX idx_scores_bucket ON scores(score_bucket);
        """
    )


def insert_rows(conn: sqlite3.Connection, rows: list[dict[str, str]], scores: dict[str, Score]) -> None:
    conn.executemany(
        """
        INSERT INTO companies (
            ticker, market_country, asset_pool, company, mcap_bucket,
            bfs_depth, module, current_pool
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["market_country"],
                row["asset_pool"],
                row["company"],
                row.get("mcap_bucket", ""),
                row["bfs_depth"],
                row.get("module", ""),
                row["current_pool"],
            )
            for row in rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO dependency_edges (
            ticker, dependency_path, dependency_edge, overseas_bottleneck, up_downstream
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row.get("dependency_path", ""),
                row.get("dependency_edge", ""),
                row.get("overseas_bottleneck", ""),
                row.get("up_downstream", ""),
            )
            for row in rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO research_signals (
            ticker, evidence_state, etf_clue, smart_money_clue,
            counterevidence, trading_reach, verification_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row.get("evidence_state", ""),
                row.get("etf_clue", ""),
                row.get("smart_money_clue", ""),
                row.get("counterevidence", ""),
                row.get("trading_reach", ""),
                "pending_original_source_verification",
            )
            for row in rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO scores (
            ticker, bfs_score, pool_score, evidence_score, edge_score,
            risk_penalty, total_score, score_bucket
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                score.ticker,
                score.bfs_score,
                score.pool_score,
                score.evidence_score,
                score.edge_score,
                score.risk_penalty,
                score.total_score,
                score.score_bucket,
            )
            for score in scores.values()
        ],
    )
    conn.commit()


def enrich_rows(rows: Iterable[dict[str, str]], scores: dict[str, Score]) -> list[dict[str, str]]:
    enriched: list[dict[str, str]] = []
    for row in rows:
        score = scores[row["ticker"]]
        item = dict(row)
        item["total_score"] = str(score.total_score)
        item["score_bucket"] = score.score_bucket
        item["verification_status"] = "pending_original_source_verification"
        enriched.append(item)
    return enriched


def sort_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(rows, key=lambda row: (-int(row["total_score"]), row["asset_pool"], row["ticker"]))


def has_d2_d3_focus(row: dict[str, str]) -> bool:
    low, high = depth_range(row.get("bfs_depth", ""))
    return high <= 3 and high >= 2 and low <= 3 and "排除" not in row.get("current_pool", "")


def has_d4_d5_radar(row: dict[str, str]) -> bool:
    _, high = depth_range(row.get("bfs_depth", ""))
    return high >= 4 or "雷达" in row.get("current_pool", "") or "排除" in row.get("current_pool", "")


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str] = CORE_CSV_FIELDS) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_csv_outputs(reports_dir: Path, rows: list[dict[str, str]]) -> None:
    rows_sorted = sort_rows(rows)
    write_csv(
        reports_dir / "core_candidates.csv",
        [
            row
            for row in rows_sorted
            if row["score_bucket"] in {"core_review", "high_priority"}
            and "排除" not in row.get("current_pool", "")
        ],
    )
    write_csv(reports_dir / "d2_d3_candidates.csv", [row for row in rows_sorted if has_d2_d3_focus(row)])
    write_csv(reports_dir / "china_asset_pool.csv", [row for row in rows_sorted if row["asset_pool"] == "中国资产池"])
    write_csv(reports_dir / "us_asset_pool.csv", [row for row in rows_sorted if row["asset_pool"] == "美国资产池"])
    write_csv(reports_dir / "satellite_pool.csv", [row for row in rows_sorted if row["asset_pool"] == "卫星资产池"])
    write_csv(reports_dir / "radar_and_excluded.csv", [row for row in rows_sorted if has_d4_d5_radar(row)])


def md_escape(value: object) -> str:
    text = str(value).replace("\n", " ").replace("|", "\\|")
    return text if len(text) <= 160 else text[:157] + "..."


def md_table(headers: list[str], rows: Iterable[Iterable[object]]) -> str:
    rows_list = [list(row) for row in rows]
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows_list:
        output.append("| " + " | ".join(md_escape(value) for value in row) + " |")
    return "\n".join(output)


def top_rows(rows: list[dict[str, str]], asset_pool: str, limit: int = 15) -> list[dict[str, str]]:
    return [
        row
        for row in sort_rows(rows)
        if row["asset_pool"] == asset_pool and row["score_bucket"] != "exclude"
    ][:limit]


def dashboard_table_rows(rows: list[dict[str, str]]) -> list[list[object]]:
    return [
        [
            row["ticker"],
            row["company"],
            row["market_country"],
            row["bfs_depth"],
            row["module"],
            row["current_pool"],
            row["total_score"],
            row["score_bucket"],
        ]
        for row in rows
    ]


def counter_table(counter: Counter[str]) -> str:
    return md_table(["分类", "数量"], counter.most_common())


def write_dashboard(path: Path, rows: list[dict[str, str]], source_path: Path, db_path: Path) -> None:
    total = len(rows)
    asset_pool_counts = Counter(row["asset_pool"] for row in rows)
    depth_counts = Counter(row["bfs_depth"] for row in rows)
    current_pool_counts = Counter(row["current_pool"] for row in rows)
    bucket_counts = Counter(row["score_bucket"] for row in rows)
    d2_d3_rows = [row for row in sort_rows(rows) if has_d2_d3_focus(row)][:40]
    d4_d5_rows = [
        row
        for row in sort_rows(rows)
        if depth_range(row.get("bfs_depth", ""))[1] >= 4 and row["score_bucket"] != "exclude"
    ][:35]
    excluded_rows = [row for row in sort_rows(rows) if row["score_bucket"] == "exclude" or "排除" in row["current_pool"]]
    pending_rows = [row for row in sort_rows(rows) if "待原文核验" in row.get("evidence_state", "")][:30]

    lines = [
        "# AI Infra Universe Dashboard v1",
        "",
        "**状态**: research-priority dashboard; all records default to `pending_original_source_verification`.",
        "",
        "**不是投资建议**: 本报告不生成买卖建议、目标价或实际仓位建议。评分只用于安排研究优先级。",
        "",
        "## 文件",
        "",
        f"- 输入 JSONL: `{source_path}`",
        f"- SQLite: `{db_path}`",
        "- 生成 CSV: `core_candidates.csv`, `d2_d3_candidates.csv`, `china_asset_pool.csv`, `us_asset_pool.csv`, `satellite_pool.csv`, `radar_and_excluded.csv`",
        "",
        "## 总览",
        "",
        f"- 总记录数: **{total}**",
        "- 数据质量状态: **pending_original_source_verification**",
        "- 研究重点: **D1-D3**；D4-D5 默认雷达，除非能证明反向卡住 D0-D2。",
        "",
        "### 资产池分布",
        "",
        counter_table(asset_pool_counts),
        "",
        "### BFS 深度分布",
        "",
        counter_table(depth_counts),
        "",
        "### 分池分布",
        "",
        counter_table(current_pool_counts),
        "",
        "### 评分桶分布",
        "",
        counter_table(bucket_counts),
        "",
        "## 评分规则",
        "",
        "- `bfs_score`: D2-D3 最高；D4-D5 降为雷达；超过 D5 默认排除。",
        "- `pool_score`: 核心池/核心候选加权；雷达降权；排除池强惩罚。",
        "- `evidence_score`: 原文已证明最高，合理推论次之，待原文核验保留但不视为已证明。",
        "- `edge_score`: dependency path、dependency edge、海外瓶颈、上下游关系越清楚，研究优先级越高。",
        "- `risk_penalty`: 反证、排除条件、客户集中、融资/债务、价格战、供给过剩、流动性等降分。",
        "",
        "## 中国资产池 Top Candidates",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(top_rows(rows, "中国资产池")),
        ),
        "",
        "## 美国资产池 Top Candidates",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(top_rows(rows, "美国资产池")),
        ),
        "",
        "## 卫星资产池 Top Candidates",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(top_rows(rows, "卫星资产池")),
        ),
        "",
        "## D2-D3 高弹性候选清单",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(d2_d3_rows),
        ),
        "",
        "## D4-D5 雷达清单",
        "",
        "这些标的默认不进入核心候选，除非后续原文证据证明它们能反向卡住 D0-D2。",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(d4_d5_rows),
        ),
        "",
        "## 排除池和待重分类清单",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(excluded_rows),
        ),
        "",
        "## 下一批原文核验优先级",
        "",
        "优先从高分且仍为 `待原文核验` 的记录开始，先核验订单、收入、backlog、CapEx、客户、产能、毛利率和技术路线。",
        "",
        md_table(
            ["Ticker", "Company", "Market", "BFS", "Module", "Pool", "Score", "Bucket"],
            dashboard_table_rows(pending_rows),
        ),
        "",
        "## SQLite 复现查询",
        "",
        "```sql",
        "SELECT c.ticker, c.company, c.asset_pool, c.bfs_depth, c.module, c.current_pool, s.total_score, s.score_bucket",
        "FROM companies c",
        "JOIN scores s USING (ticker)",
        "WHERE s.score_bucket IN ('core_review', 'high_priority')",
        "ORDER BY s.total_score DESC, c.asset_pool, c.ticker;",
        "```",
        "",
        "```sql",
        "SELECT c.ticker, c.company, c.market_country, c.bfs_depth, c.module, c.current_pool, s.total_score",
        "FROM companies c",
        "JOIN scores s USING (ticker)",
        "WHERE c.bfs_depth IN ('D2', 'D2-D3', 'D3', 'D1-D2', 'D1-D3')",
        "  AND c.current_pool NOT LIKE '%排除%'",
        "ORDER BY s.total_score DESC;",
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def validate_outputs(conn: sqlite3.Connection, expected_count: int) -> list[str]:
    checks: list[str] = []
    company_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    edge_count = conn.execute("SELECT COUNT(*) FROM dependency_edges").fetchone()[0]
    signal_count = conn.execute("SELECT COUNT(*) FROM research_signals").fetchone()[0]
    score_count = conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
    checks.append(f"companies={company_count}")
    checks.append(f"dependency_edges={edge_count}")
    checks.append(f"research_signals={signal_count}")
    checks.append(f"scores={score_count}")
    if company_count != expected_count:
        raise AssertionError(f"companies row count {company_count} != {expected_count}")
    if edge_count != expected_count:
        raise AssertionError(f"dependency_edges row count {edge_count} != {expected_count}")
    if signal_count != expected_count:
        raise AssertionError(f"research_signals row count {signal_count} != {expected_count}")
    if score_count != expected_count:
        raise AssertionError(f"scores row count {score_count} != {expected_count}")
    asset_pools = {
        row[0]
        for row in conn.execute("SELECT DISTINCT asset_pool FROM companies")
    }
    expected_pools = {"中国资产池", "美国资产池", "卫星资产池"}
    if not expected_pools.issubset(asset_pools):
        raise AssertionError(f"asset_pool missing expected categories: {expected_pools - asset_pools}")
    bad_d2d3 = conn.execute(
        """
        SELECT COUNT(*)
        FROM companies c
        JOIN scores s USING (ticker)
        WHERE c.current_pool LIKE '%排除%'
          AND s.score_bucket IN ('core_review', 'high_priority')
        """
    ).fetchone()[0]
    if bad_d2d3:
        raise AssertionError("Excluded rows entered high-priority buckets.")
    d4_d5_core = conn.execute(
        """
        SELECT COUNT(*)
        FROM companies c
        JOIN scores s USING (ticker)
        WHERE c.bfs_depth IN ('D4', 'D4-D5')
          AND s.score_bucket IN ('core_review', 'high_priority')
        """
    ).fetchone()[0]
    if d4_d5_core:
        raise AssertionError("D4/D4-D5 rows entered core/high-priority buckets.")
    pending_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM research_signals
        WHERE verification_status = 'pending_original_source_verification'
        """
    ).fetchone()[0]
    if pending_count != expected_count:
        raise AssertionError("Not all rows are marked pending_original_source_verification.")
    empty_edges = conn.execute(
        """
        SELECT COUNT(*)
        FROM dependency_edges
        WHERE COALESCE(dependency_path, '') = ''
          AND COALESCE(dependency_edge, '') = ''
          AND COALESCE(overseas_bottleneck, '') = ''
          AND COALESCE(up_downstream, '') = ''
        """
    ).fetchone()[0]
    if empty_edges:
        raise AssertionError(f"{empty_edges} rows have no dependency edge context.")
    return checks


def main() -> None:
    args = parse_args()
    input_path = args.input.expanduser().resolve()
    data_dir = args.data_dir
    reports_dir = args.reports_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    rows = read_jsonl(input_path)
    raw_copy = data_dir / "global_universe_v2.jsonl"
    shutil.copyfile(input_path, raw_copy)

    scores = {row["ticker"]: build_score(row) for row in rows}
    enriched = enrich_rows(rows, scores)

    db_path = data_dir / "ai_infra_universe.sqlite"
    conn = connect_database(db_path)
    try:
        create_schema(conn)
        insert_rows(conn, rows, scores)
        write_csv_outputs(reports_dir, enriched)
        write_dashboard(reports_dir / "ai_infra_universe_dashboard_v1.md", enriched, raw_copy, db_path)
        checks = validate_outputs(conn, len(rows))
    finally:
        conn.close()

    print(f"Built AI Infra universe artifacts from {input_path}")
    print(f"Rows: {len(rows)}")
    print(f"Raw copy: {raw_copy}")
    print(f"SQLite: {db_path}")
    print(f"Dashboard: {reports_dir / 'ai_infra_universe_dashboard_v1.md'}")
    print("Validation: " + ", ".join(checks))


if __name__ == "__main__":
    main()
