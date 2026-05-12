#!/usr/bin/env python3
"""Scaffold evidence-card markdown files for the first verification batch."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


DEFAULT_BATCH = Path("reports/source_verification_batch1.csv")
DEFAULT_OUTPUT_DIR = Path("evidence/batch1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create evidence-card drafts from source_verification_batch1.csv.")
    parser.add_argument("--batch", type=Path, default=DEFAULT_BATCH, help="Batch CSV file.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Evidence card output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing card files.")
    return parser.parse_args()


def slugify(value: str) -> str:
    value = value.strip().replace("/", "-").replace(" ", "-")
    value = re.sub(r"[^0-9A-Za-z._\-\u4e00-\u9fff]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "unknown"


def card_text(row: dict[str, str]) -> str:
    title = f"{row['ticker']} {row['company']} evidence card"
    return f"""# {title}

状态：draft evidence card, pending original-source verification

边界：这张卡只用于原文核验和研究分层，不是投资建议、买卖建议、目标价或仓位建议。

## 基本信息

| 字段 | 内容 |
| --- | --- |
| Rank | {row['rank']} |
| Priority tier | {row['priority_tier']} |
| 公司 / 证券代码 | {row['company']} / {row['ticker']} |
| 市场 / 资产池 | {row['market_country']} / {row['asset_pool']} |
| BFS depth | {row['bfs_depth']} |
| 产业链模块 | {row['module']} |
| 当前分池 | {row['current_pool']} |
| Universe score | {row['total_score']} / {row['score_bucket']} |
| 核验状态 | {row['verification_status']} |

## 依赖链假设

| 字段 | 内容 |
| --- | --- |
| Dependency path | {row['dependency_path']} |
| Dependency edge | {row['dependency_edge']} |
| ETF clue | {row['etf_clue']} |
| Smart money clue | {row['smart_money_clue']} |

## 本轮优先核验

| 项目 | 内容 |
| --- | --- |
| Source priority | {row['source_priority']} |
| Primary sources to find | {row['primary_sources_to_find']} |
| Metrics to verify | {row['metrics_to_verify']} |
| Upgrade conditions | {row['upgrade_conditions']} |
| Downgrade conditions | {row['downgrade_conditions']} |

## 原文来源登记

| 来源类型 | 链接 / 文件 | 发布日期 | 覆盖期间 | 备注 |
| --- | --- | --- | --- | --- |
| Annual report / 10-K / 20-F |  |  |  |  |
| Quarterly results / 10-Q / 6-K |  |  |  |  |
| Earnings call transcript |  |  |  |  |
| Investor presentation |  |  |  |  |
| Company product / technical page |  |  |  |  |
| Exchange filing / regulatory filing |  |  |  |  |
| Upstream/downstream cross-disclosure |  |  |  |  |

## 原文证据

| 指标 | 原文位置 | 原文能证明什么 | 不能证明什么 | 口径备注 |
| --- | --- | --- | --- | --- |
| Revenue / segment revenue |  |  |  | GAAP/non-GAAP、币种、期间 |
| Gross margin / operating margin |  |  |  | 产品 mix、一次性项目 |
| CapEx / inventory / FCF |  |  |  | 现金流与扩产周期 |
| Backlog / RPO / orders |  |  |  | 是否可取消、是否已进收入 |
| ASP / shipment / capacity |  |  |  | 单位、同比/环比 |
| Customer / product evidence |  |  |  | 客户是否直接披露 |
| Technical roadmap / qualification |  |  |  | 技术路线与量产时间 |

## 结论分层

| 层级 | 内容 |
| --- | --- |
| 原文已证明 |  |
| 合理推论 |  |
| 待原文核验 | {row['evidence_state']} |
| 主要反证 | {row['counterevidence']} |

## 研究判断

| 维度 | 评分 1-5 | 依据 |
| --- | --- | --- |
| AI 需求相关度 |  |  |
| 供给瓶颈 |  |  |
| 议价权 |  |  |
| 持续性 |  |  |
| 财务传导 |  |  |
| 技术护城河 |  |  |
| 估值空间 |  |  |
| 反证清晰度 |  |  |

## 当前动作

- [ ] 找到最新 annual report / 10-K / 20-F 或交易所年报。
- [ ] 找到最新季度 earnings release / investor presentation。
- [ ] 找到 earnings call transcript 或公司说明会材料。
- [ ] 核对收入、订单、backlog、产能、毛利率、客户关系和技术路线。
- [ ] 写清楚升级 / 保持候选 / 降为雷达 / 排除的条件。
"""


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(args.batch.open(encoding="utf-8")))
    created = 0
    skipped = 0
    for row in rows:
        filename = f"{int(row['rank']):03d}-{slugify(row['ticker'])}-{slugify(row['company'])}.md"
        path = args.output_dir / filename
        if path.exists() and not args.overwrite:
            skipped += 1
            continue
        path.write_text(card_text(row), encoding="utf-8")
        created += 1
    print(f"Evidence cards created={created}, skipped={skipped}, total_batch={len(rows)}")
    print(f"Output dir: {args.output_dir}")


if __name__ == "__main__":
    main()
