"""When a setup fires, call LLM to write trade-idea narrative.

Input: Setup event (from setup_detector)
Output: dict {
  summary: str,           # 1-line gist for Telegram preview
  trade_ideas: list[      # 1-3 concrete trade ideas
    {direction, instrument_family, expiry_window, strike_logic,
     entry, stop, target, max_risk_R, confidence_label, reasoning}
  ],
  risk_notes: list[str],
  do_not_do: list[str],
}

The instrument family is one of:
  - long_gamma         (buy ATM straddle/iron butterfly)
  - skew_fade          (sell put spread + buy call spread)
  - pin_strangle       (sell OTM strangle around pin)
  - directional_call   (buy call spread)
  - directional_put    (buy put spread)
  - wait               (no trade, just monitor)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import yaml

STACK_ROOT = Path(__file__).resolve().parents[2]
CN_CONFIG = STACK_ROOT / "quant-research-cn" / "config.yaml"

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"


def _load_api_key() -> str:
    cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
    key = (cfg.get("api") or {}).get("deepseek_key")
    if not key:
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


SYSTEM_PROMPT = """你是美股指数期权日内交易顾问。一个 setup 已被触发,你的任务:

1. 读取触发事件 + 当前 state
2. 输出 1-3 个**具体 trade ideas**(JSON 结构,见格式说明)
3. 每个 idea 必须可执行(direction / instrument family / 入场 / 止损 / 目标 / 风险预算)
4. 写 risk notes(2-3 条) + do_not_do(2-3 条)

## 约束

- **只交易 European cash-settled indices**:^SPX / ^NDX / ^XSP / ^RUT / ^XEO / ^XND / ^MRUT(IRC 1256 60/40 税)
- **严禁**:SPY/QQQ/IWM(美式行权 assignment 风险)、个股期权
- 单笔 max_risk_R ≤ 0.5R(default 0.25R)
- 0DTE 仅在 short_gamma_amplifier 或 pin 场景才推荐
- DTE 1-2 是 sweet spot(time decay + 有 carry)
- 不要写 strike 具体数字(用 "ATM" / "OTM 1.0%" / "OTM put 1.5% / call 0.8%" 之类的相对描述)

## 输出格式(严格 JSON,no markdown)

{
  "setup_label": "spot 跌破 gamma flip → vol amplifier ON",
  "regime_now": "amplifier" | "watch_break" | "dampener" | "dampener+",
  "summary": "一句话总结(<= 30 字)",
  "trade_ideas": [
    {
      "direction": "long_vol" | "short_vol" | "directional_long" | "directional_short" | "vol_neutral_carry",
      "instrument_family": "long_gamma" | "skew_fade" | "pin_strangle" | "directional_call" | "directional_put" | "wait",
      "instrument": "^SPX",
      "expiry_window": "0DTE" | "1DTE" | "2-5DTE",
      "strike_logic": "ATM straddle" | "OTM put spread 1%/2%" | etc,
      "entry": "市价开仓 OR 等回踩 X",
      "stop_logic": "spot 反方向 0.5%" | "VIX < 14" | etc,
      "target_logic": "ATM IV 回归 mean" | "spot 移到 flip" | etc,
      "max_risk_R": 0.25,
      "confidence_label": "high" | "medium" | "low",
      "reasoning": "为什么这个 trade now (2-3 句话,引用具体 metric)"
    }
  ],
  "risk_notes": ["风险点 1", "风险点 2"],
  "do_not_do": ["不要做的事 1", "不要做的事 2"]
}
"""


def _call_deepseek(api_key: str, user_msg: str, timeout: int = 90) -> str | None:
    try:
        r = requests.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.2,
                "max_tokens": 1500,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except (requests.RequestException, KeyError, ValueError) as e:
        print(f"[llm_advisor] DeepSeek call failed: {e}", file=sys.stderr)
        return None


def _format_user_msg(setup: dict[str, Any], state: dict[str, Any]) -> str:
    return (
        f"## 触发的 Setup\n"
        f"- ts: {setup.get('ts')}\n"
        f"- symbol: {setup.get('symbol')}\n"
        f"- type: {setup.get('type')}\n"
        f"- summary: {setup.get('summary')}\n"
        f"- context: {json.dumps(setup.get('context') or {}, ensure_ascii=False)}\n\n"
        f"## 当前指数 state(从 realtime_quotes 读取最新)\n"
        f"```json\n{json.dumps(state, ensure_ascii=False, indent=2, default=str)}\n```\n\n"
        f"按 system prompt 输出 trade ideas JSON。"
    )


def _load_state(symbols: list[str]) -> dict[str, Any]:
    import duckdb

    DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
    con = duckdb.connect(str(DB_PATH), read_only=True)
    state: dict[str, Any] = {}
    for sym in symbols:
        rows = con.execute(
            """
            WITH ranked AS (
              SELECT field, value,
                     ROW_NUMBER() OVER (PARTITION BY field ORDER BY ingested_at DESC) AS rn
              FROM realtime_quotes WHERE symbol = ?
            )
            SELECT field, value FROM ranked WHERE rn = 1
            """,
            [sym],
        ).fetchall()
        if rows:
            state[sym] = {f: v for f, v in rows}
    # Also pull GEX snapshot
    try:
        gex_rows = con.execute(
            """
            WITH ranked AS (
              SELECT symbol, dte_bucket, net_dealer_gex, gamma_flip_strike,
                     atm_call_iv, atm_put_iv, skew_pts,
                     ROW_NUMBER() OVER (PARTITION BY symbol, dte_bucket ORDER BY snapshot_time DESC) AS rn
              FROM index_gex_snapshots
            )
            SELECT symbol, dte_bucket, net_dealer_gex, gamma_flip_strike,
                   atm_call_iv, atm_put_iv, skew_pts
            FROM ranked WHERE rn = 1
              AND symbol IN ('^SPX', '^NDX', '^XSP', '^RUT')
              AND dte_bucket IN ('1DTE', 'WEEK')
            """
        ).fetchall()
        if gex_rows:
            state["_gex"] = [
                {
                    "symbol": r[0], "bucket": r[1],
                    "net_gex_T": round((r[2] or 0) / 1e12, 2),
                    "flip": r[3], "call_iv": r[4], "put_iv": r[5], "skew_pp": (r[6] or 0) * 100,
                }
                for r in gex_rows
            ]
    except duckdb.Error:
        pass
    con.close()
    return state


def advise(setup: dict[str, Any]) -> dict[str, Any] | None:
    """Main entry: setup dict in, parsed advisor JSON dict out."""
    api_key = _load_api_key()
    watch_symbols = ["SPX", "NDX", "XSP", "RUT", "VIX"]
    state = _load_state(watch_symbols)
    user_msg = _format_user_msg(setup, state)
    response = _call_deepseek(api_key, user_msg)
    if not response:
        return None
    # Strip code fences if any
    text = response.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        print(f"[llm_advisor] JSON parse failed: {e}\nResponse: {text[:300]}", file=sys.stderr)
        return {"_raw": text}


def main() -> None:
    """CLI: pipe a setup JSON via stdin OR pass --setup-file."""
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup-file", help="JSON file with setup event")
    args = ap.parse_args()
    if args.setup_file:
        setup = json.loads(Path(args.setup_file).read_text())
    else:
        setup = json.loads(sys.stdin.read())
    result = advise(setup)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
