"""Verify CN AI-infra evidence from Tushare 主营业务构成 (fina_mainbz).

Diagnosis (2026-05-18): 16 CN names carried an identical boilerplate
evidence_state — `原文已证明: source-review ready_for_promotion 2026-05-15`
— a readiness-scorer tier copy-stamped as "原文已证明" with no real
original-source check. The production gate was theatre on the CN side.

This script does the verification FOR REAL. For each CN universe name it
pulls Tushare `fina_mainbz` (the company's own disclosed revenue-by-product
breakdown) and classifies each segment:

- AI-direct   : the segment name itself says 算力 / 数据中心 / 智算 /
                光模块 / 液冷 / CPO / AI服务器 ... — a disclosed AI line.
- AI-adjacent : 服务器 / 云计算 / 光通信 / PCB / 连接器 / 电源 ... —
                AI-exposed but the company did not break out "AI".
- none        : everything else.

Decision:
- a disclosed AI-direct segment with material share → 原文已证明 (with the
  real period / segment / 占比).
- AI-adjacent material share → 合理推论 (honest inference, head is pure
  合理推论 so it still clears the production gate).
- neither, or no product breakdown disclosed → 待原文核验 (downgrade — the
  fake stamps lose production status).

A .bak of global_universe_v2.jsonl is written before any change. Only CN
rows whose evidence_state is auto-stamped / default are rewritten; bespoke
operator evidence is left untouched. Run --dry-run first.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_CONFIG = Path("/home/ivena/coding/rust/quant-research-cn/config.yaml")

# Segment-name keywords. Lower-cased substring match against bz_item.
AI_DIRECT_KW = (
    "算力", "数据中心", "智算", "智能计算", "idc", "ai服务器", "人工智能",
    "cpo", "光模块", "光器件", "液冷", "gpu",
)
AI_ADJACENT_KW = (
    "服务器", "云计算", "光通信", "光纤", "通信设备", "通信网络", "网络设备",
    "pcb", "印制电路", "覆铜板", "连接器", "存储", "高端计算", "交换机",
    "散热", "温控", "电源", "半导体", "芯片", "封装", "通信",
)

# Materiality thresholds (share of total disclosed product revenue).
DIRECT_MATERIAL_SHARE = 0.10
ADJACENT_MATERIAL_SHARE = 0.20

# An evidence_state is "machine-owned" (safe to rewrite) if it is empty,
# the auto-promotion boilerplate, or a default pending state.
_REWRITABLE_MARKERS = ("source-review ready_for_promotion", "待原文核验",
                        "原文需核验", "证据不足", "ready_for_promotion")


def classify_segment(bz_item: str) -> str:
    """Classify a 主营分部 name → 'direct' / 'adjacent' / 'none'."""
    text = str(bz_item or "").lower()
    if not text:
        return "none"
    if any(kw in text for kw in AI_DIRECT_KW):
        return "direct"
    if any(kw in text for kw in AI_ADJACENT_KW):
        return "adjacent"
    return "none"


def decide_evidence(period: str, segments: list[tuple[str, float]]) -> tuple[str, dict]:
    """segments = list of (bz_item, bz_sales元). → (evidence_state, summary)."""
    total = sum(s for _, s in segments if s and s > 0)
    if total <= 0:
        return (
            f"待原文核验: fina_mainbz({period}) 无有效产品分部收入",
            {"verdict": "待原文核验", "period": period, "reason": "no_segment_revenue"},
        )
    direct = [(n, s) for n, s in segments if s and classify_segment(n) == "direct"]
    adjacent = [(n, s) for n, s in segments if s and classify_segment(n) == "adjacent"]
    direct_sales = sum(s for _, s in direct)
    adj_sales = sum(s for _, s in adjacent)
    direct_share = direct_sales / total
    adj_share = adj_sales / total

    def _yi(v: float) -> str:
        return f"{v / 1e8:.1f}亿"

    if direct and direct_share >= DIRECT_MATERIAL_SHARE:
        top = max(direct, key=lambda x: x[1])
        state = (
            f"原文已证明: {period} 主营分部「{top[0]}」收入{_yi(top[1])}"
            f"，AI直接分部占比{direct_share * 100:.0f}%"
        )
        verdict = "原文已证明"
    elif (direct and direct_sales > 0) or adj_share >= ADJACENT_MATERIAL_SHARE:
        seg = (max(direct, key=lambda x: x[1]) if direct
               else max(adjacent, key=lambda x: x[1]))
        share = direct_share if direct else adj_share
        state = (
            f"合理推论: {period} 主营分部「{seg[0]}」占比{share * 100:.0f}%"
            f"，AI 敞口需逐项拆分"
        )
        verdict = "合理推论"
    else:
        state = (
            f"待原文核验: fina_mainbz({period}) 未见明确 AI 分部"
            f"（direct {direct_share * 100:.0f}% / adjacent {adj_share * 100:.0f}%）"
        )
        verdict = "待原文核验"
    return state, {
        "verdict": verdict, "period": period,
        "direct_share": round(direct_share, 4), "adjacent_share": round(adj_share, 4),
        "direct_segments": [n for n, _ in direct],
        "adjacent_segments": [n for n, _ in adjacent],
    }


def _pro(config_path: Path):
    import tushare as ts
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    token = (
        (cfg.get("api") or {}).get("tushare_token")
        or cfg.get("tushare_token")
        or (cfg.get("tushare") or {}).get("token")
    )
    if not token:
        raise RuntimeError(f"no tushare_token in {config_path}")
    return ts.pro_api(token)


_RATE_LIMIT_HINTS = ("每分钟", "访问该接口", "rate", "limit", "频率", "超过")


def pull_mainbz(pro, ts_code: str, max_retries: int = 4) -> tuple[str, list[tuple[str, float]]] | None:
    """Latest by-product (bz_type='P') segments. None = fetch error.

    fina_mainbz is rate-limited on the standard Tushare tier — retry with a
    60s backoff when the error looks like a rate-limit rejection.
    """
    df = None
    for attempt in range(max_retries):
        try:
            df = pro.fina_mainbz(ts_code=ts_code, type="P")
            break
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if attempt < max_retries - 1 and any(h in msg for h in _RATE_LIMIT_HINTS):
                time.sleep(60)
                continue
            print(f"  warn: fina_mainbz({ts_code}) failed: {exc}", file=sys.stderr)
            return None
    if df is None or df.empty:
        return ("", [])
    latest = df["end_date"].max()
    rows = df[df["end_date"] == latest]
    segments: list[tuple[str, float]] = []
    for _, r in rows.iterrows():
        item = r.get("bz_item")
        sales = r.get("bz_sales")
        try:
            sales = float(sales) if sales is not None else 0.0
        except (TypeError, ValueError):
            sales = 0.0
        if item:
            segments.append((str(item), sales))
    return (str(latest), segments)


def _is_cn(ticker: str) -> bool:
    t = str(ticker or "").upper()
    return t.endswith(".SZ") or t.endswith(".SH")


def _rewritable(evidence_state: str) -> bool:
    s = str(evidence_state or "").strip()
    if not s:
        return True
    return any(m in s for m in _REWRITABLE_MARKERS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    if not args.universe.exists():
        print(f"error: universe missing at {args.universe}", file=sys.stderr)
        return 2

    rows = [json.loads(l) for l in args.universe.read_text(encoding="utf-8").splitlines() if l.strip()]
    cn_rows = [r for r in rows if _is_cn(r.get("ticker"))]
    print(f"CN universe rows: {len(cn_rows)}")

    pro = _pro(args.config)
    changes: list[tuple[str, str, str]] = []  # (ticker, old, new)
    skipped_bespoke = 0
    fetch_errors = 0
    by_verdict: dict[str, int] = {}

    for r in cn_rows:
        ticker = str(r.get("ticker") or "")
        primary = ticker.split("/")[0].strip()
        old = str(r.get("evidence_state") or "")
        # 排除池 rows — leave alone.
        if "排除" in str(r.get("current_pool") or "") or "排除" in old:
            continue
        if not _rewritable(old):
            skipped_bespoke += 1
            continue
        mainbz = pull_mainbz(pro, primary)
        time.sleep(0.8)
        if mainbz is None:
            fetch_errors += 1
            continue  # keep existing — retry next run
        period, segments = mainbz
        if not segments:
            new = "待原文核验: fina_mainbz 无产品分部披露,无法核验 AI 收入"
            verdict = "待原文核验"
        else:
            new, summary = decide_evidence(period, segments)
            verdict = summary["verdict"]
        by_verdict[verdict] = by_verdict.get(verdict, 0) + 1
        if new != old:
            changes.append((ticker, old, new))
            r["evidence_state"] = new

    print(f"\nverdict breakdown: {by_verdict}")
    print(f"changes: {len(changes)} | skipped bespoke: {skipped_bespoke} | "
          f"fetch errors: {fetch_errors}")
    for ticker, old, new in changes:
        print(f"  {ticker}")
        print(f"    - {old[:70]}")
        print(f"    + {new[:90]}")

    if args.dry_run:
        print("\n(dry-run — universe not modified)")
        return 0
    if not changes:
        print("nothing to rewrite")
        return 0

    if not args.no_backup:
        backup = args.universe.with_suffix(args.universe.suffix + ".bak")
        shutil.copy2(args.universe, backup)
        print(f"backup: {backup}")
    with args.universe.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"rewrote {args.universe} — {len(changes)} CN evidence_state updated "
          f"({datetime.now().isoformat(timespec='seconds')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
