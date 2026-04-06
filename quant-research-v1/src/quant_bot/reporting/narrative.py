"""Claude API narrative generation with number-grounding validation."""
from __future__ import annotations

import json
import re
from typing import Any

import structlog

log = structlog.get_logger()

SYSTEM_PROMPT = """You are the narrative layer for a US equities quantitative research report delivered after the close.

Use ONLY the supplied JSON payload. Do not invent numbers, tickers, dates, or rankings not in the payload.
If data is missing, say so and reduce confidence.
Write in a neutral, institutional tone: concise, factual, non-promotional. Not investment advice.
Return a JSON object matching this schema exactly:

{
  "executive_summary": ["sentence1", "sentence2", "sentence3"],
  "market_regime_narrative": "string",
  "top_signals_narrative": "string",
  "earnings_narrative": "string",
  "macro_narrative": "string",
  "risk_notes": "string",
  "missing_data_flags": []
}"""

DATA_PROMPT = """Trade date: {trade_date}
Task: Generate the daily quant research report narrative.

Rules:
1. Use only values in the payload. Preserve signs, units, and rankings exactly.
2. Use Bonferroni-corrected p-values in all language.
3. Executive summary: exactly 3 sentences. Sentence 1 = regime. Sentence 2 = top probability finding. Sentence 3 = risk note.
4. For probabilities: say "X.X% chance" not "likely" — be quantitative.
5. For risk: "daily ATR risk of $X.XX" not vague language.

Payload:
{payload}"""


def _extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\d+\.?\d*", text))


def _build_allowed_values(bundle: dict) -> set[str]:
    """Flatten all numeric values from the bundle for grounding validation."""
    allowed = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, (int, float)):
            allowed.add(str(round(obj, 1)))
            allowed.add(str(round(obj, 2)))
            allowed.add(str(int(obj)))
        elif isinstance(obj, str):
            for n in re.findall(r"\d+\.?\d*", obj):
                allowed.add(n)
        elif isinstance(obj, dict):
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(bundle)
    return allowed


def _validate_narrative(narrative: dict, bundle: dict) -> list[str]:
    """
    Check that numbers in the narrative exist in the bundle.
    Returns list of validation warnings (not errors — we log and continue).
    """
    allowed = _build_allowed_values(bundle)
    warnings = []

    all_text = " ".join(
        str(v) for v in narrative.values() if isinstance(v, str)
    )
    found_nums = _extract_numbers(all_text)
    ungrounded = found_nums - allowed - {"0", "1", "2", "3", "4", "5", "100"}

    if ungrounded:
        warnings.append(f"Possibly ungrounded numbers: {sorted(ungrounded)[:10]}")

    return warnings


def generate_narrative(
    bundle: dict,
    api_key: str,
    model: str = "claude-sonnet-4-6",
    temperature: float = 0.15,
    max_tokens: int = 2500,
) -> dict:
    """
    Call Claude to generate narrative from the bundle.
    Returns validated narrative dict.
    Falls back to a stub if Claude is unavailable.
    """
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        payload_str = json.dumps(bundle, indent=2, default=str)
        prompt = DATA_PROMPT.format(
            trade_date=bundle["meta"]["trade_date"],
            payload=payload_str,
        )

        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```\w*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```$", "", raw_text)

        narrative = json.loads(raw_text)
        warnings = _validate_narrative(narrative, bundle)
        if warnings:
            log.warning("narrative_validation_warnings", warnings=warnings)

        return narrative

    except Exception as e:
        log.error("narrative_generation_failed", error=str(e))
        return _fallback_narrative(bundle)


def _fallback_narrative(bundle: dict) -> dict:
    """Deterministic fallback when Claude is unavailable."""
    regime = bundle.get("market_regime", {}).get("label", "unknown")
    conf = bundle.get("market_regime", {}).get("confidence", 0)
    bench = bundle.get("benchmark", {}).get("daily_return_pct", 0) or 0
    n = len(bundle.get("top_signals", []))

    return {
        "executive_summary": [
            f"Market regime is {regime} with {conf*100:.0f}% confidence.",
            f"Benchmark returned {bench:+.1f}% today; {n} signals analyzed.",
            "Narrative generation unavailable — see data tables below.",
        ],
        "market_regime_narrative": f"Regime: {regime} ({conf*100:.0f}% confidence).",
        "top_signals_narrative": f"{n} signals computed. See table below.",
        "earnings_narrative": "See earnings table below.",
        "macro_narrative": "See macro snapshot below.",
        "risk_notes": "Claude narrative unavailable. All data is deterministic.",
        "missing_data_flags": ["claude_narrative_failed"],
    }
