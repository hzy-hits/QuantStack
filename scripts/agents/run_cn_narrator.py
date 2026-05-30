"""CN (A股) daily report agent narrator — mirrors run_us_narrator.py.

4 extractors (macro / quant / event / risk) + 1 narrator (merge), all driven
by the codex CLI (GPT-5.5) via codex_backend, reusing the existing CN prompts
in quant-research-cn/prompts/. DeepSeek remains the automatic fallback.

Input: the dashboard CN artifacts written by generate_main_strategy_v2_report.py
  reports/review_dashboard/main_strategy_v2/<date>/cn_daily_report.md  (+ *.json)
We slice that programmatic report into the macro / structural / events payloads
the CN prompts expect (same self-contained pattern as the US narrator, which
reads us_daily_report.md). US macro context is sliced from us_daily_report.md
when present.

Output: cn_daily_report_agent.md (sibling of cn_daily_report.md), which
send_production_decision_report.py prefers over the programmatic version.

Usage:
    python3 scripts/agents/run_cn_narrator.py --date 2026-05-28
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from codex_backend import backend, call_llm, concurrency  # noqa: E402
# Reuse the section slicer + payload join helpers from the US narrator.
from run_us_narrator import _join_payload_sections, _slice_md_sections  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
CN_CONFIG = ROOT / "quant-research-cn" / "config.yaml"
CN_PROMPTS_DIR = ROOT / "quant-research-cn" / "prompts"

# cn_daily_report.md ## section headers grouped by extractor payload.
_MACRO_HEADERS = [
    "CN 风控引擎", "今天先看哪些板块", "A股 Benchmark Snapshot", "A股 AI Book vs Benchmark",
]
_STRUCTURAL_HEADERS = [
    "概率最优", "可交易名单", "逐票复核", "左侧观察池", "CN Realized Horizon Edge",
]
_EVENTS_HEADERS = [
    "AI Supercycle Evidence", "10x Value Radar", "财报披露日历",
    "Source Review Calendar", "只观察或不碰",
]
_RISK_HEADERS = ["风险口径", "Risk block", "CN 风控引擎"]
# US macro backdrop the CN macro analyst cross-references.
_US_MACRO_HEADERS = ["Risk Regime", "Fear", "Market Regime", "风控引擎", "市场情绪"]


def load_deepseek_key() -> str | None:
    try:
        cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
        key = (cfg.get("api") or {}).get("deepseek_key")
    except OSError:
        key = None
    if not key and backend() == "deepseek":
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


def load_prompt(name: str) -> str:
    path = CN_PROMPTS_DIR / ("merge-agent.md" if name == "merge" else f"{name}-analyst.md")
    if not path.exists():
        raise SystemExit(f"CN prompt missing: {path}")
    return path.read_text(encoding="utf-8")


def load_artifacts(report_dir: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for jpath in report_dir.glob("cn_*.json"):
        try:
            out[jpath.stem] = json.loads(jpath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
    cn_md = report_dir / "cn_daily_report.md"
    out["_cn_md"] = cn_md.read_text(encoding="utf-8") if cn_md.exists() else ""
    us_md = report_dir / "us_daily_report.md"
    out["_us_md"] = us_md.read_text(encoding="utf-8") if us_md.exists() else ""
    return out


def build_payloads(art: dict[str, Any]) -> dict[str, str]:
    cn_md = art.get("_cn_md", "")
    us_md = art.get("_us_md", "")
    return {
        "macro": _join_payload_sections("CN MACRO PAYLOAD", _slice_md_sections(cn_md, _MACRO_HEADERS)),
        "us_macro": _join_payload_sections("US MACRO BACKDROP", _slice_md_sections(us_md, _US_MACRO_HEADERS)),
        "structural": _join_payload_sections("CN STRUCTURAL PAYLOAD", _slice_md_sections(cn_md, _STRUCTURAL_HEADERS)),
        "events": _join_payload_sections("CN EVENTS PAYLOAD", _slice_md_sections(cn_md, _EVENTS_HEADERS)),
    }


# Which placeholders each extractor prompt consumes.
_EXTRACTOR_PLACEHOLDERS = {
    "macro": ["payload_macro", "payload_us_macro", "prev_context"],
    "quant": ["payload_structural", "prev_context"],
    "event": ["payload_events", "prev_context"],
    "risk": ["payload_macro", "payload_structural", "prev_context"],
}


def _fill(prompt: str, mapping: dict[str, str]) -> str:
    for key, val in mapping.items():
        prompt = prompt.replace("{" + key + "}", val)
    return prompt


async def call_extractor_async(
    sem: asyncio.Semaphore, api_key: str | None, name: str, payloads: dict[str, str]
) -> tuple[str, str]:
    prompt = load_prompt(name)
    mapping = {
        "payload_macro": payloads["macro"],
        "payload_us_macro": payloads["us_macro"],
        "payload_structural": payloads["structural"],
        "payload_events": payloads["events"],
        "prev_context": "(本期无上一份报告上下文)",
    }
    # Only substitute the placeholders this prompt actually uses.
    use = {k: mapping[k] for k in _EXTRACTOR_PLACEHOLDERS[name] if k in mapping}
    system_msg = _fill(prompt, use)
    user_msg = "请严格按上述提取器指令,只输出结构化提取 + ## 判断,不要输出额外内容。"
    loop = asyncio.get_event_loop()
    async with sem:
        resp = await loop.run_in_executor(
            None,
            lambda: call_llm(
                system_msg, user_msg,
                label=f"cn-extractor:{name}", deepseek_key=api_key,
                temperature=0.1, max_tokens=1200,
            ),
        )
    return name, resp or f"[{name} extractor failed]"


async def run_extractors(api_key: str | None, payloads: dict[str, str]) -> dict[str, str]:
    sem = asyncio.Semaphore(concurrency())
    tasks = [call_extractor_async(sem, api_key, n, payloads) for n in ("macro", "quant", "event", "risk")]
    return dict(await asyncio.gather(*tasks))


def call_narrator(api_key: str | None, ext: dict[str, str], art: dict[str, Any], as_of: str) -> str | None:
    prompt = load_prompt("merge")
    full_payload = (art.get("_cn_md", "") or "")[:30000]
    filled = _fill(prompt, {
        "macro_output": ext.get("macro", "[missing]"),
        "quant_output": ext.get("quant", "[missing]"),
        "event_output": ext.get("event", "[missing]"),
        "risk_output": ext.get("risk", "[missing]"),
        "full_payload": full_payload,
        "prev_context": "(本期无上一份报告上下文)",
        "date": as_of,
    })
    user_msg = f"请按 merge-agent.md 的输出格式,为日期 {as_of} 生成完整 A股日报。"
    return call_llm(
        filled, user_msg, label="narrator:cn",
        deepseek_key=api_key, temperature=0.3, max_tokens=4500,
    )


async def main_async(args) -> None:
    as_of = args.date
    report_dir = ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of
    if not report_dir.exists():
        raise SystemExit(f"report dir missing: {report_dir}")
    api_key = load_deepseek_key()
    art = load_artifacts(report_dir)
    if not art.get("_cn_md"):
        raise SystemExit(f"cn_daily_report.md missing in {report_dir}")
    print(f"=== CN narrator agent — {as_of} (backend={backend()}, concurrency={concurrency()}) ===")
    print(f"  cn_md size {len(art.get('_cn_md',''))}, us_md size {len(art.get('_us_md',''))}")

    payloads = build_payloads(art)
    print("  running 4 extractors in parallel (macro/quant/event/risk)...")
    ext = await run_extractors(api_key, payloads)
    for name, out in ext.items():
        print(f"    {name}: {len(out)} chars")
        if args.dump_extractors:
            (report_dir / f"_cn_extractor_{name}.md").write_text(out, encoding="utf-8")

    print("  calling narrator...")
    narrative = call_narrator(api_key, ext, art, as_of)
    if not narrative:
        print("  narrator failed; not writing output")
        return
    out_name = "cn_daily_report.md" if args.overwrite else "cn_daily_report_agent.md"
    out_path = report_dir / out_name
    out_path.write_text(narrative, encoding="utf-8")
    print(f"  wrote {out_path} ({len(narrative)} chars)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--overwrite", action="store_true", help="Overwrite cn_daily_report.md")
    ap.add_argument("--dump-extractors", action="store_true")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
