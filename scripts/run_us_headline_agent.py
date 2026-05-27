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

SYSTEM_PROMPT = """你是金融新闻结构化分类工具。只从输入文本提取事实,**禁止任何推理 / 猜测 / 补全**。

输入:一只股票的 symbol + 一篇新闻的 headline + summary。

输出严格 JSON:
{
  "subject_match": true | false,
  "sentiment": "positive" | "negative" | "neutral",
  "severity": 0 | 1 | 2 | 3,
  "event_type": "earnings|m_and_a|regulatory|product|lawsuit|management|rating|partnership|investment_received|investment_made|macro|other",
  "summary_zh": "≤30 字,只能复述 headline/summary 字面信息",
  "nvda_investment": null | {
    "invested_company": "公司名(必须在文本字面出现)",
    "ticker": null | "字符串(只有当文本里明确出现 ticker 字母才填,否则 null)",
    "amount_usd": null | 数字(只有当文本里有具体美元金额才填,不准从公司规模/常识推断),
    "percent_stake": null | 数字(只有当文本里有百分比才填),
    "deal_type": "equity_stake | acquisition | strategic_investment | partnership | venture"
  }
}

字段硬规则(违反即视为错误):
- subject_match=true 当且仅当 symbol 是 headline 的主语/主题。multi-ticker 列表("AAA, BBB, X plummet")、比喻文(Burry warning 类)、提及但不讨论 → false。
- severity 整数:0=无关; 1=普通新闻(rating/产品发布等); 2=值得关注(财报 beat/miss/大合同); 3=重大冲击(bankruptcy/fraud/SEC investigation/CEO 突然离职)。subject_match=false 时 severity 必须 = 0 或 1。
- sentiment 只看 headline 字面情绪,不推测市场反应。
- nvda_investment 防幻觉(严格):
    * 仅当 headline 或 summary 字面出现 "Nvidia" 或 "NVDA" 同时出现 "invest|stake|acqui|fund|partner|deploy" 字眼时才填
    * invested_company 必须是文本中字面出现的公司名,不准从 ticker/symbol/语境推断
    * amount_usd 必须文本中字面有数字+货币单位,不准从市值/常识/惯例填
    * 如果不能 100% 从字面验证,**必须填 null,不准猜**
    * 当前新闻的 symbol 自己 ≠ 被投公司时才填(避免循环)

不要解释、不要 markdown、只输出严格 JSON 对象。
"""


def validate_scored(s: dict) -> dict | None:
    """Schema validator — reject malformed LLM output instead of silent default.
    Returns sanitized dict or None if too malformed to use."""
    try:
        out = {
            "subject_match": bool(s.get("subject_match")),
            "sentiment": s.get("sentiment") if s.get("sentiment") in ("positive", "negative", "neutral") else None,
            "severity": s.get("severity") if isinstance(s.get("severity"), int) and 0 <= s["severity"] <= 3 else None,
            "event_type": s.get("event_type"),
            "summary_zh": str(s.get("summary_zh") or "")[:200],
            "nvda_investment": s.get("nvda_investment"),
        }
        # accept severity 0/1/2/3 only — float 0-1 from old prompt would be rejected
        if out["severity"] is None or out["sentiment"] is None:
            return None
        # enforce: subject_match=false implies severity ≤ 1
        if not out["subject_match"] and out["severity"] > 1:
            out["severity"] = 1
        return out
    except (TypeError, ValueError, AttributeError):
        return None


def validate_nvda_invest(nvda_inv: dict, headline: str, summary: str, source_symbol: str) -> dict | None:
    """Server-side anti-hallucination check for NVDA investment claims.

    Reject the claim unless:
      - text literally mentions Nvidia/NVDA + an investment keyword
      - invested_company name actually appears as substring in text
      - amount, if claimed, has a digit+$ pattern in text
    """
    if not isinstance(nvda_inv, dict):
        return None
    text = f"{headline} {summary}".lower()
    if "nvidia" not in text and "nvda" not in text:
        return None
    invest_kws = ("invest", "stake", "acqui", "fund", "partner", "deploy", "venture", "raise")
    if not any(kw in text for kw in invest_kws):
        return None
    company = str(nvda_inv.get("invested_company") or "").strip()
    if not company or len(company) < 2:
        return None
    # Reject Nvidia investing in itself — these are commentary articles
    # ("Nvidia Doubled Down on Its Holding") where LLM misread the subject.
    company_l = company.lower()
    if company_l in {"nvidia", "nvda", "nvidia corp", "nvidia corporation", "nvidia inc"}:
        return None
    # invested_company must literally appear in source text (case-insensitive)
    if company_l not in text:
        return None
    # disallow circular: source article is about the "invested" company being the same as the
    # source symbol's company name (e.g. MRVL article extracting itself as invested by NVDA
    # when text doesn't actually claim that)
    if source_symbol.upper() == str(nvda_inv.get("ticker") or "").upper() and "nvidia" not in headline.lower():
        # source is the supposed invested company itself + headline doesn't put Nvidia in it
        # → likely hallucination triggered by ranking/preview article
        return None
    # amount sanity: if claimed, there must be a $ or "billion"/"million" in text
    amt = nvda_inv.get("amount_usd")
    if amt is not None:
        if not any(token in text for token in ("$", "billion", "million", "亿", "万美元")):
            nvda_inv["amount_usd"] = None  # demote unverified amount
    return nvda_inv


def load_deepseek_key() -> str:
    cfg = yaml.safe_load(CN_CONFIG.read_text(encoding="utf-8"))
    key = (cfg.get("api") or {}).get("deepseek_key")
    if not key:
        raise SystemExit("DeepSeek key not found in quant-research-cn/config.yaml")
    return key


def init_schemas(con: duckdb.DuckDBPyConnection) -> None:
    """News-scored uses INT severity (0|1|2|3) + bool subject_match — no float
    confidence (was no signal — 85% returned 0.9). Idempotent: ALTER existing
    severity DOUBLE → SMALLINT if table predates this migration."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS news_scored (
            symbol VARCHAR NOT NULL,
            url VARCHAR NOT NULL,
            published_at TIMESTAMP,
            headline VARCHAR,
            subject_match BOOLEAN,
            sentiment VARCHAR,
            severity SMALLINT,
            event_type VARCHAR,
            summary_zh VARCHAR,
            scored_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (symbol, url)
        )
    """)
    # Migrate legacy DOUBLE severity → SMALLINT (round float to int bucket)
    cols = {r[0]: r[1] for r in con.execute("DESCRIBE news_scored").fetchall()}
    if cols.get("severity", "").upper() == "DOUBLE":
        con.execute("ALTER TABLE news_scored ALTER severity TYPE SMALLINT USING CAST(severity * 3 AS SMALLINT)")
    if "confidence" in cols:
        con.execute("ALTER TABLE news_scored DROP COLUMN confidence")
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
            verified BOOLEAN DEFAULT FALSE,
            extracted_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (announce_date, invested_company, url)
        )
    """)
    inv_cols = {r[0]: r[1] for r in con.execute("DESCRIBE nvda_investments").fetchall()}
    if "verified" not in inv_cols:
        con.execute("ALTER TABLE nvda_investments ADD COLUMN verified BOOLEAN DEFAULT FALSE")


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
         severity, event_type, summary_zh, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
    """, [
        row["symbol"], row["url"], row["published_at"], row["headline"],
        scored["subject_match"],
        scored["sentiment"],
        scored["severity"],
        str(scored.get("event_type") or "other"),
        scored.get("summary_zh") or "",
    ])


def write_nvda_investment(con: duckdb.DuckDBPyConnection, row: dict,
                          nvda_inv: dict) -> bool:
    """Returns True if row was actually inserted (deduped by PK).
    Caller MUST have validated nvda_inv via validate_nvda_invest() first."""
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
             deal_type, headline, url, source, verified, extracted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, current_timestamp)
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
          AND severity >= 2
        ORDER BY severity DESC, published_at DESC
        LIMIT 20
    """, [(as_of - timedelta(days=1)).isoformat()]).fetchall()
    if rows:
        sev_label = {0: "无关", 1: "普通", 2: "值得关注", 3: "重大"}
        lines += ["## 🔥 今日重大新闻 (subject_match, severity ≥ 2)", "",
                  "| Symbol | severity | sent | event | 中文摘要 | 标题 |",
                  "|---|:---:|:---:|:---:|---|---|"]
        for r in rows:
            sym, hl, sev, sent, ev, sm, _ = r
            emoji = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(sent, "⚪")
            lines.append(f"| **{sym}** | {sev}({sev_label.get(sev,'?')}) | {emoji} | {ev} | {sm or '-'} | {hl[:60]} |")
        lines.append("")
    else:
        lines += ["## 🔥 今日重大新闻", "- 没有 severity ≥ 2 的 subject_match 新闻。", ""]

    # 2. NVDA 投资追踪
    nvda_rows = con.execute("""
        SELECT announce_date, invested_company, ticker, amount_usd,
               percent_stake, deal_type, headline, url
        FROM nvda_investments
        WHERE announce_date >= ?
          AND verified = TRUE
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
               MAX(severity) AS max_sev
        FROM news_scored
        WHERE CAST(scored_at AS DATE) = ?
          AND subject_match = TRUE
        GROUP BY symbol HAVING n >= 1
        ORDER BY max_sev DESC, n DESC LIMIT 15
    """, [as_of.isoformat()]).fetchall()
    if cnt_today:
        lines += ["## 📰 今日新打分股票(subject_match,按 max severity)", "",
                  "| Symbol | N | 🟢 | 🔴 | max sev |",
                  "|---|---:|---:|---:|---:|"]
        for r in cnt_today:
            lines.append(f"| **{r[0]}** | {r[1]} | {r[2]} | {r[3]} | {r[4]} |")
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
        scored_n = invalid_n = nvda_n = nvda_rejected = 0
        for i, row in enumerate(rows, 1):
            raw = classify(session, api_key, row["symbol"], row["headline"], row["summary"] or "")
            if not raw:
                continue
            validated = validate_scored(raw)
            if validated is None:
                invalid_n += 1
                continue
            write_scored(con, row, validated)
            scored_n += 1
            nvda_inv_raw = raw.get("nvda_investment")
            if nvda_inv_raw:
                nvda_inv = validate_nvda_invest(
                    nvda_inv_raw, row["headline"], row["summary"] or "", row["symbol"]
                )
                if nvda_inv is None:
                    nvda_rejected += 1
                else:
                    if write_nvda_investment(con, row, nvda_inv):
                        nvda_n += 1
            if i % 20 == 0:
                print(f"  progress {i}/{len(rows)}: scored={scored_n} invalid={invalid_n} "
                      f"nvda_hits={nvda_n} nvda_rejected_hallucinations={nvda_rejected}")
            time.sleep(0.05)
        print(f"  done: scored={scored_n}, invalid={invalid_n}, "
              f"nvda_hits={nvda_n}, nvda_rejected_hallucinations={nvda_rejected}")

    DIGEST_DIR.mkdir(parents=True, exist_ok=True)
    digest = render_daily_digest(con, as_of)
    digest_path = DIGEST_DIR / f"daily_news_digest_{as_of.isoformat()}.md"
    digest_path.write_text(digest, encoding="utf-8")
    print(f"  digest written: {digest_path}")
    con.close()


if __name__ == "__main__":
    main()
