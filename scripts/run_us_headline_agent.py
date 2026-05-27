"""US headline agent — DeepSeek-based news classification + NVDA investment tracking.

Replaces (eventually) the hardcoded keyword matching in
us_opportunity_ranker.headline_risk() with LLM-derived structured scoring.

Pipeline:
    news_items (raw Finnhub headlines, US DB)
         ↓
    DeepSeek classification (per-symbol-article pair)
         ↓
    news_scored table (subject_match / sentiment / severity / event_type /
                       summary_zh / confidence)
         ↓
    headline_risk() reads from news_scored before falling back to keywords
         ↓
    daily_news_digest_<date>.md (operator-facing human summary)

Special task: scan ALL news for "NVIDIA invested in X" / "NVDA acquired Y" /
"NVDA equity stake" mentions, extract structured records into
nvda_investments table.

Usage:
    python3 scripts/run_us_headline_agent.py [--date 2026-05-27]
                                             [--lookback-days 7]
                                             [--max-news 200]
                                             [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"
CN_CONFIG = ROOT / "quant-research-cn" / "config.yaml"
DIGEST_DIR = ROOT / "ai_infra" / "reports"

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

SYSTEM_PROMPT = """你是金融新闻结构化分类工具,只做提取,不做推理。

输入:一只股票的 symbol + 一篇新闻的 headline + summary。

输出 JSON,严格按以下 schema:
{
  "subject_match": true | false,
  "sentiment": "positive" | "negative" | "neutral",
  "severity": 0.0 - 1.0,
  "event_type": "earnings|m_and_a|regulatory|product|lawsuit|management|rating|partnership|investment_received|investment_made|macro|other",
  "summary_zh": "≤30 字一句话",
  "confidence": 0.0 - 1.0,
  "nvda_investment": null | {
    "invested_company": "公司名",
    "ticker": "ticker if mentioned, else null",
    "amount_usd": null | 数字(美元金额),
    "percent_stake": null | 数字(0-100),
    "deal_type": "equity_stake | acquisition | strategic_investment | partnership | venture"
  }
}

字段规则:
- subject_match: 当且仅当 symbol 是 headline **主语/主题**时 true。如果 symbol 只出现在多 ticker tag 列表(例如 "AAA, BBB, MU plummet" 这种)或比喻文中作为对比对象,则 false。
- sentiment: 只看文本表面语气,不预测市场反应。
- severity: 0=无关 0.3=普通新闻 0.6=值得关注 0.8=重大事件 1.0=巨大冲击(bankruptcy/fraud/SEC investigation 等);只在 subject_match=true 时才能 ≥0.5。
- event_type 闭集选一。"investment_made" = 该 symbol 投资了别人;"investment_received" = 该 symbol 收到了投资。
- nvda_investment: 仅当 headline 或 summary 明确提及 "NVIDIA invested / NVDA stake / Nvidia acquired" 类 NVDA 对外投资/入股/收购信息时填写。NVDA 自身的产品/收益新闻 → null。

不要解释、不要 markdown、只输出严格 JSON。
"""


def load_deepseek_key() -> str:
    cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
    key = (cfg.get("api") or {}).get("deepseek_key")
    if not key:
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


def init_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS news_scored (
            symbol VARCHAR NOT NULL,
            url VARCHAR NOT NULL,
            published_at TIMESTAMP,
            headline VARCHAR,
            subject_match BOOLEAN,
            sentiment VARCHAR,
            severity DOUBLE,
            event_type VARCHAR,
            summary_zh VARCHAR,
            confidence DOUBLE,
            scored_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (symbol, url)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS nvda_investments (
            announce_date DATE,
            invested_company VARCHAR,
            ticker VARCHAR,
            amount_usd DOUBLE,
            percent_stake DOUBLE,
            deal_type VARCHAR,
            headline VARCHAR,
            url VARCHAR,
            source VARCHAR,
            confidence DOUBLE,
            extracted_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (announce_date, invested_company, url)
        )
    """)


def classify(client_session: requests.Session, api_key: str, symbol: str,
             headline: str, summary: str) -> dict | None:
    """One DeepSeek classification call. Returns None on failure."""
    user_msg = (
        f"symbol: {symbol}\n"
        f"headline: {headline}\n"
        f"summary: {summary[:600]}"
    )
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.0,
        "max_tokens": 400,
        "response_format": {"type": "json_object"},
    }
    try:
        r = client_session.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        return json.loads(content)
    except (requests.RequestException, KeyError, json.JSONDecodeError, ValueError) as e:
        print(f"  [warn] classify {symbol} failed: {type(e).__name__}: {str(e)[:100]}", file=sys.stderr)
        return None


def fetch_unscored_news(con: duckdb.DuckDBPyConnection, start: date, limit: int) -> list[dict]:
    """Load news_items not yet in news_scored, most-recent first."""
    rows = con.execute("""
        SELECT n.symbol, n.url, n.published_at, n.headline, n.summary, n.source
        FROM news_items n
        LEFT JOIN news_scored s
          ON n.symbol = s.symbol AND n.url = s.url
        WHERE CAST(n.published_at AS DATE) >= ?
          AND s.url IS NULL
          AND n.headline IS NOT NULL
        ORDER BY n.published_at DESC
        LIMIT ?
    """, [start.isoformat(), limit]).fetchall()
    cols = ["symbol", "url", "published_at", "headline", "summary", "source"]
    return [dict(zip(cols, r)) for r in rows]


def write_scored(con: duckdb.DuckDBPyConnection, row: dict, scored: dict) -> None:
    con.execute("""
        INSERT OR REPLACE INTO news_scored
        (symbol, url, published_at, headline, subject_match, sentiment,
         severity, event_type, summary_zh, confidence, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
    """, [
        row["symbol"], row["url"], row["published_at"], row["headline"],
        bool(scored.get("subject_match")),
        str(scored.get("sentiment") or "neutral"),
        float(scored.get("severity") or 0.0),
        str(scored.get("event_type") or "other"),
        str(scored.get("summary_zh") or "")[:300],
        float(scored.get("confidence") or 0.0),
    ])


def write_nvda_investment(con: duckdb.DuckDBPyConnection, row: dict,
                          nvda_inv: dict) -> bool:
    """Returns True if row was actually inserted (deduped by PK)."""
    company = str(nvda_inv.get("invested_company") or "").strip()
    if not company:
        return False
    pub = row.get("published_at")
    announce_date = pub.date() if hasattr(pub, "date") else None
    if announce_date is None:
        return False
    try:
        con.execute("""
            INSERT OR REPLACE INTO nvda_investments
            (announce_date, invested_company, ticker, amount_usd, percent_stake,
             deal_type, headline, url, source, confidence, extracted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [
            announce_date,
            company[:120],
            str(nvda_inv.get("ticker") or "")[:12] or None,
            float(nvda_inv["amount_usd"]) if nvda_inv.get("amount_usd") is not None else None,
            float(nvda_inv["percent_stake"]) if nvda_inv.get("percent_stake") is not None else None,
            str(nvda_inv.get("deal_type") or "other")[:30],
            row["headline"][:300],
            row["url"],
            row.get("source") or "?",
            0.8,
        ])
        return True
    except (duckdb.Error, TypeError, ValueError) as e:
        print(f"  [warn] nvda invest insert {company} failed: {e}", file=sys.stderr)
        return False


def render_daily_digest(con: duckdb.DuckDBPyConnection, as_of: date) -> str:
    """One-page operator digest: top severe news, NVDA investments, per-symbol counts."""
    lines: list[str] = [
        f"# US 新闻日报 - {as_of.isoformat()}",
        "",
        "**来源**: news_items + headline-agent(DeepSeek 分类)。每条都标 subject_match + sentiment + severity + event_type。",
        "",
    ]
    # 1. 今日高 severity + subject_match 新闻
    rows = con.execute("""
        SELECT symbol, headline, severity, sentiment, event_type, summary_zh, published_at
        FROM news_scored
        WHERE CAST(published_at AS DATE) >= ?
          AND subject_match = TRUE
          AND severity >= 0.5
        ORDER BY severity DESC, published_at DESC
        LIMIT 20
    """, [(as_of - timedelta(days=1)).isoformat()]).fetchall()
    if rows:
        lines += ["## 🔥 今日 high-severity 新闻 (subject_match)", "",
                  "| Symbol | sev | sent | event | 中文摘要 | 标题 |",
                  "|---|---:|:---:|:---:|---|---|"]
        for r in rows:
            sym, hl, sev, sent, ev, sm, _ = r
            emoji = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(sent, "⚪")
            lines.append(f"| **{sym}** | {sev:.2f} | {emoji} | {ev} | {sm or '-'} | {hl[:60]} |")
        lines.append("")
    else:
        lines += ["## 🔥 今日 high-severity 新闻", "- 没有 severity ≥ 0.5 的 subject_match 新闻。", ""]

    # 2. NVDA 投资追踪
    nvda_rows = con.execute("""
        SELECT announce_date, invested_company, ticker, amount_usd,
               percent_stake, deal_type, headline, url
        FROM nvda_investments
        WHERE announce_date >= ?
        ORDER BY announce_date DESC, amount_usd DESC NULLS LAST
        LIMIT 25
    """, [(as_of - timedelta(days=30)).isoformat()]).fetchall()
    if nvda_rows:
        lines += ["## 🎯 NVDA 最近 30 天投资/入股/收购追踪", "",
                  "| 日期 | 被投公司 | Ticker | 金额(USD) | 持股% | 类型 | 标题 |",
                  "|---|---|---|---:|---:|:---:|---|"]
        for r in nvda_rows:
            d, comp, tk, amt, pct, dt, hl, _ = r
            amt_s = f"${amt/1e9:.2f}B" if amt and amt >= 1e9 else (f"${amt/1e6:.1f}M" if amt else "-")
            pct_s = f"{pct:.1f}%" if pct else "-"
            lines.append(f"| {d} | {comp} | {tk or '-'} | {amt_s} | {pct_s} | {dt or '-'} | {hl[:50]} |")
        lines.append("")
    else:
        lines += ["## 🎯 NVDA 投资追踪", "- 最近 30 天没有抓到 NVDA 对外投资/入股新闻。", ""]

    # 3. Per-symbol counts (今天新评分了多少)
    cnt_today = con.execute("""
        SELECT symbol, COUNT(*) AS n,
               SUM(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END) AS pos,
               SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) AS neg,
               AVG(severity) AS avg_sev
        FROM news_scored
        WHERE CAST(scored_at AS DATE) = ?
          AND subject_match = TRUE
        GROUP BY symbol HAVING n >= 1
        ORDER BY avg_sev DESC, n DESC LIMIT 15
    """, [as_of.isoformat()]).fetchall()
    if cnt_today:
        lines += ["## 📰 今日新打分股票(subject_match,按 avg severity)", "",
                  "| Symbol | N | 🟢 | 🔴 | avg sev |",
                  "|---|---:|---:|---:|---:|"]
        for r in cnt_today:
            lines.append(f"| **{r[0]}** | {r[1]} | {r[2]} | {r[3]} | {r[4]:.2f} |")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--max-news", type=int, default=300)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    as_of = date.fromisoformat(args.date)
    start = as_of - timedelta(days=args.lookback_days)
    api_key = load_deepseek_key()

    con = duckdb.connect(str(US_DB))
    init_schemas(con)
    rows = fetch_unscored_news(con, start, args.max_news)
    print(f"=== US headline agent ({as_of.isoformat()}) ===")
    print(f"  unscored news in window {start} ~ {as_of}: {len(rows)}")
    if args.dry_run:
        for r in rows[:5]:
            print(f"  sample: {r['symbol']:6} {r['published_at']} {r['headline'][:80]}")
        con.close()
        return
    if not rows:
        print("  nothing to score, regenerating digest only")
    else:
        session = requests.Session()
        scored_n = nvda_n = 0
        for i, row in enumerate(rows, 1):
            scored = classify(session, api_key, row["symbol"], row["headline"], row["summary"] or "")
            if not scored:
                continue
            write_scored(con, row, scored)
            scored_n += 1
            nvda_inv = scored.get("nvda_investment")
            if nvda_inv:
                if write_nvda_investment(con, row, nvda_inv):
                    nvda_n += 1
            if i % 20 == 0:
                print(f"  progress {i}/{len(rows)}: scored={scored_n} nvda_invest_hits={nvda_n}")
            time.sleep(0.05)   # gentle rate limit
        print(f"  done: scored={scored_n} / {len(rows)}, nvda_invest hits={nvda_n}")

    DIGEST_DIR.mkdir(parents=True, exist_ok=True)
    digest = render_daily_digest(con, as_of)
    digest_path = DIGEST_DIR / f"daily_news_digest_{as_of.isoformat()}.md"
    digest_path.write_text(digest, encoding="utf-8")
    print(f"  digest written: {digest_path}")
    con.close()


if __name__ == "__main__":
    main()
