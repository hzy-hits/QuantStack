"""US daily report agent narrator — 4 extractor + 1 narrator pipeline.

Phase D.2 of PHASE_D_PLAN.md.

Architecture (mirrors quant-research-cn/src/main.rs agent flow):
    payload (loaded from existing JSON artifacts)
       ↓
    4 extractors in parallel (DeepSeek):
      macro / event / quant / risk
       ↓
    narrator (DeepSeek) — receives 4 extractor outputs + payload digest
       ↓
    us_daily_report_agent.md (sibling to programmatic us_daily_report.md
                              for side-by-side comparison until D.5)

Usage:
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27
    python3 scripts/agents/run_us_narrator.py --date 2026-05-27 --overwrite
        # overwrite existing us_daily_report.md (Phase D.5 default)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
import yaml

ROOT = Path(__file__).resolve().parents[2]
CN_CONFIG = ROOT / "quant-research-cn" / "config.yaml"
PROMPTS_DIR = ROOT / "quant-research-v1" / "prompts"

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"


def load_deepseek_key() -> str:
    cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
    key = (cfg.get("api") or {}).get("deepseek_key")
    if not key:
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


def load_prompt(name: str) -> str:
    """Load us-{name}-analyst.md (or us-merge-agent.md if name='merge')."""
    if name == "merge":
        path = PROMPTS_DIR / "us-merge-agent.md"
    else:
        path = PROMPTS_DIR / f"us-{name}-analyst.md"
    if not path.exists():
        raise SystemExit(f"prompt missing: {path}")
    return path.read_text(encoding="utf-8")


def load_payload_artifacts(report_dir: Path) -> dict[str, Any]:
    """Read all *.json artifacts in report_dir + the existing us_daily_report.md."""
    out: dict[str, Any] = {}
    for jpath in report_dir.glob("*.json"):
        try:
            out[jpath.stem] = json.loads(jpath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
    md_path = report_dir / "us_daily_report.md"
    if md_path.exists():
        out["_us_daily_report_md"] = md_path.read_text(encoding="utf-8")
    return out


def build_macro_payload(art: dict[str, Any]) -> str:
    """Build a compact text payload for the macro extractor."""
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "风控引擎",
        "恐惧贪婪",
        "SPX × P/C",
        "Bubble Hedge",
    ])
    return _join_payload_sections("MACRO PAYLOAD", sections)


def build_event_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "Serenity",
        "财报日历",
        "美股财报",
        "Source Review",
    ])
    digest_lines = [sections]
    # NVDA investments are in DB not in report; pass a placeholder note
    digest_lines.append(
        "\n\n_NVDA investments table: refer to daily_news_digest_<date>.md if present._"
    )
    return _join_payload_sections("EVENT PAYLOAD", "\n".join(digest_lines))


def build_quant_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "🎲 今日概率最优",
        "🎯 今日只看这些",
        "可交易名单",
        "逐票复核",
        "US 期权 IV 视图",
        "US 期权定位",
        "美股生产排序",
        "AI Supercycle Layer Attribution",
    ])
    return _join_payload_sections("QUANT PAYLOAD", sections)


def build_risk_payload(art: dict[str, Any]) -> str:
    md = art.get("_us_daily_report_md", "")
    sections = _slice_md_sections(md, [
        "US Production Gate",
        "US 期权异常",
        "US 左侧观察池",
        "组合风险覆盖",
        "Portfolio Risk Overlay",
    ])
    return _join_payload_sections("RISK PAYLOAD", sections)


def _slice_md_sections(md: str, headers: list[str]) -> str:
    """Extract sections (## starting with one of headers) from md."""
    out: list[str] = []
    cur_section: list[str] = []
    cur_match = False
    for line in md.split("\n"):
        if line.startswith("## "):
            if cur_match and cur_section:
                out.extend(cur_section)
                out.append("")
            cur_section = [line]
            cur_match = any(h in line for h in headers)
        else:
            cur_section.append(line)
    if cur_match and cur_section:
        out.extend(cur_section)
    return "\n".join(out)


def _join_payload_sections(label: str, sections: str) -> str:
    if not sections.strip():
        return f"# {label}\n\n[no relevant data sections found in payload]"
    return f"# {label}\n\n{sections}"


def call_deepseek(api_key: str, system: str, user: str, *,
                  temperature: float = 0.2, max_tokens: int = 1500) -> str | None:
    """Sync DeepSeek call. Returns content or None on error."""
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        r = requests.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except (requests.RequestException, KeyError, ValueError) as e:
        print(f"  [warn] DeepSeek call failed: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
        return None


async def call_extractor_async(api_key: str, name: str, payload_text: str) -> tuple[str, str]:
    """Async wrapper around sync requests call for parallel extractor invocation."""
    prompt = load_prompt(name)
    # Substitute {payload_*} placeholder with actual payload
    placeholder = "{payload_" + name + "}" if name not in ("merge",) else None
    if placeholder and placeholder in prompt:
        system_part = prompt.split(placeholder)[0]
        # Use the part before placeholder as system; payload as user
        system_msg = system_part
        user_msg = payload_text
    else:
        # Fallback: use the whole prompt as system, payload as user
        system_msg = prompt
        user_msg = payload_text

    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: call_deepseek(api_key, system_msg, user_msg, temperature=0.1, max_tokens=1200),
    )
    return name, response or f"[{name} extractor failed]"


async def run_extractors(api_key: str, art: dict[str, Any]) -> dict[str, str]:
    """Run 4 extractors in parallel."""
    payloads = {
        "macro": build_macro_payload(art),
        "event": build_event_payload(art),
        "quant": build_quant_payload(art),
        "risk": build_risk_payload(art),
    }
    tasks = [
        call_extractor_async(api_key, name, payload)
        for name, payload in payloads.items()
    ]
    results = await asyncio.gather(*tasks)
    return dict(results)


def call_narrator(api_key: str, extractor_outputs: dict[str, str],
                  art: dict[str, Any], as_of: str) -> str | None:
    """Single narrator call — receives extractor outputs + payload digest."""
    prompt = load_prompt("merge")
    payload_digest = art.get("_us_daily_report_md", "")[:30000]  # cap to avoid token blow-up
    user_msg = (
        f"### 宏观提取\n{extractor_outputs.get('macro', '[missing]')}\n\n"
        f"### 事件提取\n{extractor_outputs.get('event', '[missing]')}\n\n"
        f"### 量化提取\n{extractor_outputs.get('quant', '[missing]')}\n\n"
        f"### 风险提取\n{extractor_outputs.get('risk', '[missing]')}\n\n"
        f"### Payload Digest(交叉验证用)\n{payload_digest}\n\n"
        f"### 任务\n请按 us-merge-agent.md 的输出格式,为日期 {as_of} 生成完整美股日报。"
    )
    return call_deepseek(api_key, prompt, user_msg, temperature=0.3, max_tokens=4500)


async def main_async(args) -> None:
    as_of = args.date
    report_dir = ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of
    if not report_dir.exists():
        raise SystemExit(f"report dir missing: {report_dir}")

    api_key = load_deepseek_key()
    art = load_payload_artifacts(report_dir)
    print(f"=== US narrator agent — {as_of} ===")
    print(f"  loaded {len(art)} artifacts; md size {len(art.get('_us_daily_report_md', ''))}")

    print("  running 4 extractors in parallel...")
    extractor_outputs = await run_extractors(api_key, art)
    for name, out in extractor_outputs.items():
        print(f"    {name}: {len(out)} chars")
        if args.dump_extractors:
            (report_dir / f"_extractor_{name}.md").write_text(out, encoding="utf-8")

    print("  calling narrator...")
    narrative = call_narrator(api_key, extractor_outputs, art, as_of)
    if not narrative:
        print("  narrator failed; not writing output")
        return

    out_name = "us_daily_report.md" if args.overwrite else "us_daily_report_agent.md"
    out_path = report_dir / out_name
    out_path.write_text(narrative, encoding="utf-8")
    print(f"  wrote {out_path} ({len(narrative)} chars)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite us_daily_report.md (Phase D.5 default)")
    ap.add_argument("--dump-extractors", action="store_true",
                    help="Also write _extractor_{name}.md for debugging")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
