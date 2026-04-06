"""
Universe builder — turns config scope into an actual symbol list.

The config says WHAT to scan (S&P 500, sector ETFs, commodities).
This module fetches the actual constituent lists and builds the full universe.

Sources (all free):
  S&P 500:    Wikipedia (scraped via pandas read_html)
  Nasdaq 100: Wikipedia
  ETFs:       Hardcoded — there are only a handful of relevant ones
  Commodities: Hardcoded futures + ETF symbols

Constituent lists are cached in DuckDB and refreshed every N days.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import io
import duckdb
import pandas as pd
import requests
import structlog

log = structlog.get_logger()


# ── Fixed ETF/futures sets per asset class ────────────────────────────────────
# These don't change — representative instruments per class
SECTOR_ETFS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLE",   # Energy
    "XLV",   # Health Care
    "XLI",   # Industrials
    "XLU",   # Utilities
    "XLRE",  # Real Estate
    "XLY",   # Consumer Discretionary
    "XLP",   # Consumer Staples
    "XLB",   # Materials
    "XLC",   # Communication Services
]

BOND_ETFS = [
    "TLT",   # 20+ Year Treasury (long duration)
    "IEF",   # 7-10 Year Treasury
    "SHY",   # 1-3 Year Treasury (short duration)
    "HYG",   # High Yield Corporate (credit risk)
    "LQD",   # Investment Grade Corporate
]

COMMODITY_SYMBOLS = [
    "GLD",   # Gold ETF
    "SLV",   # Silver ETF
    "USO",   # US Oil Fund ETF
    "GC=F",  # Gold futures (front month)
    "CL=F",  # Crude Oil WTI futures
    "NG=F",  # Natural Gas futures
    "SI=F",  # Silver futures
]

INTERNATIONAL_ETFS = [
    "EEM",   # Emerging Markets
    "EFA",   # EAFE (Europe/Asia/Far East)
    "FXI",   # China Large Cap
    "VWO",   # Vanguard Emerging Markets
]

# ── Mandatory Tier 1 indices — always included regardless of config ────────────
# These are required for the Agents.md market context contract
MANDATORY_INDEX_SYMBOLS = [
    "SPY",   # S&P 500 (primary benchmark)
    "QQQ",   # Nasdaq 100
    "IWM",   # Russell 2000 (small cap)
    "DIA",   # Dow Jones Industrial Average
]

VOLATILITY_SYMBOLS = [
    "^VIX",  # CBOE VIX index
    "UVXY",  # 1.5x VIX short-term futures ETF
]

CRYPTO_ETFS = [
    "IBIT",  # iShares Bitcoin Trust
    "ETHA",  # iShares Ethereum Trust
]

SEMI_ETFS = [
    "SMH",   # VanEck Semiconductor ETF
    "SOXX",  # iShares Semiconductor ETF
]

BIOTECH_ETFS = [
    "XBI",   # SPDR S&P Biotech (equal-weight, small-cap bias)
    "IBB",   # iShares Biotechnology (market-cap weighted)
]

CHINA_INTERNET_ETFS = [
    "KWEB",  # KraneShares CSI China Internet ETF
]

INNOVATION_ETFS = [
    "ARKK",  # ARK Innovation ETF (disruptive tech + bio)
]


def _ensure_constituents_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS universe_constituents (
            symbol      VARCHAR NOT NULL,
            index_name  VARCHAR NOT NULL,   -- 'sp500' | 'nasdaq100'
            company     VARCHAR,
            sector      VARCHAR,
            fetched_date DATE NOT NULL,
            PRIMARY KEY (symbol, index_name)
        )
    """)


def _constituents_stale(
    con: duckdb.DuckDBPyConnection,
    index_name: str,
    refresh_days: int,
) -> bool:
    result = con.execute("""
        SELECT MAX(fetched_date) FROM universe_constituents
        WHERE index_name = ?
    """, [index_name]).fetchone()
    if result[0] is None:
        return True
    last_fetch = result[0]
    if isinstance(last_fetch, str):
        last_fetch = date.fromisoformat(last_fetch)
    return (date.today() - last_fetch).days >= refresh_days


def fetch_sp500_constituents(
    con: duckdb.DuckDBPyConnection,
    refresh_days: int = 7,
) -> list[str]:
    """
    Fetch S&P 500 constituents from Wikipedia.
    Cached in DB, refreshed every refresh_days.
    Returns list of ticker symbols.
    """
    _ensure_constituents_table(con)

    if not _constituents_stale(con, "sp500", refresh_days):
        rows = con.execute(
            "SELECT symbol FROM universe_constituents WHERE index_name = 'sp500'"
        ).fetchall()
        symbols = [r[0] for r in rows]
        log.info("sp500_constituents_from_cache", count=len(symbols))
        return symbols

    log.info("sp500_constituents_fetching_from_wikipedia")
    _HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; quant-research-bot/1.0)"}
    try:
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=_HEADERS, timeout=30,
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), header=0)
        df = tables[0]  # first table is the constituent list

        # Column names vary slightly — normalize
        df.columns = [c.strip() for c in df.columns]
        symbol_col = next(c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower())
        company_col = next((c for c in df.columns if "security" in c.lower() or "company" in c.lower()), None)
        sector_col = next((c for c in df.columns if "sector" in c.lower()), None)

        today = date.today().isoformat()

        rows_to_insert = []
        symbols = []
        for _, row in df.iterrows():
            sym = str(row[symbol_col]).strip().replace(".", "-")  # BRK.B → BRK-B for yfinance
            company = str(row[company_col]).strip() if company_col else ""
            sector  = str(row[sector_col]).strip() if sector_col else ""
            rows_to_insert.append((sym, company, sector, today))
            symbols.append(sym)

        # Atomic replace: DELETE + INSERT in a single transaction
        con.begin()
        try:
            con.execute("DELETE FROM universe_constituents WHERE index_name = 'sp500'")
            con.executemany("""
                INSERT OR REPLACE INTO universe_constituents
                    (symbol, index_name, company, sector, fetched_date)
                VALUES (?, 'sp500', ?, ?, ?)
            """, rows_to_insert)
            con.commit()
        except Exception:
            con.rollback()
            raise
        log.info("sp500_constituents_fetched", count=len(symbols))
        return symbols

    except Exception as e:
        log.warning("sp500_constituents_fetch_failed", error=str(e))
        # Fall back to cached even if stale
        rows = con.execute(
            "SELECT symbol FROM universe_constituents WHERE index_name = 'sp500'"
        ).fetchall()
        return [r[0] for r in rows]


def fetch_nasdaq100_constituents(
    con: duckdb.DuckDBPyConnection,
    refresh_days: int = 7,
) -> list[str]:
    """Fetch Nasdaq 100 constituents from Wikipedia."""
    _ensure_constituents_table(con)

    if not _constituents_stale(con, "nasdaq100", refresh_days):
        rows = con.execute(
            "SELECT symbol FROM universe_constituents WHERE index_name = 'nasdaq100'"
        ).fetchall()
        return [r[0] for r in rows]

    log.info("nasdaq100_constituents_fetching")
    _HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; quant-research-bot/1.0)"}
    try:
        resp = requests.get(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            headers=_HEADERS, timeout=30,
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), header=0)
        # Find the table with ticker symbols
        df = None
        for t in tables:
            cols = [c.lower() for c in t.columns]
            if any("ticker" in c or "symbol" in c for c in cols):
                df = t
                break

        if df is None:
            raise ValueError("Could not find Nasdaq 100 table")

        df.columns = [c.strip() for c in df.columns]
        symbol_col = next(c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower())
        company_col = next((c for c in df.columns if "company" in c.lower() or "security" in c.lower()), None)

        today = date.today().isoformat()

        rows_to_insert = []
        symbols = []
        for _, row in df.iterrows():
            sym = str(row[symbol_col]).strip()
            company = str(row[company_col]).strip() if company_col else ""
            rows_to_insert.append((sym, company, today))
            symbols.append(sym)

        # Atomic replace: DELETE + INSERT in a single transaction
        con.begin()
        try:
            con.execute("DELETE FROM universe_constituents WHERE index_name = 'nasdaq100'")
            con.executemany("""
                INSERT OR REPLACE INTO universe_constituents
                    (symbol, index_name, company, sector, fetched_date)
                VALUES (?, 'nasdaq100', ?, '', ?)
            """, rows_to_insert)
            con.commit()
        except Exception:
            con.rollback()
            raise

        log.info("nasdaq100_constituents_fetched", count=len(symbols))
        return symbols

    except Exception as e:
        log.warning("nasdaq100_fetch_failed", error=str(e))
        # Fall back to stale cache instead of empty list
        rows = con.execute(
            "SELECT symbol FROM universe_constituents WHERE index_name = 'nasdaq100'"
        ).fetchall()
        cached = [r[0] for r in rows]
        if cached:
            log.info("nasdaq100_using_stale_cache", count=len(cached))
        return cached


def build_universe(
    con: duckdb.DuckDBPyConnection,
    scan_sp500: bool = True,
    scan_nasdaq100: bool = False,
    include_sector_etfs: bool = True,
    include_semi_etfs: bool = True,
    include_biotech_etfs: bool = True,
    include_china_internet_etfs: bool = True,
    include_innovation_etfs: bool = True,
    include_bond_etfs: bool = True,
    include_commodities: bool = True,
    include_international: bool = True,
    include_volatility: bool = True,
    include_crypto_etfs: bool = False,
    watchlist: list[str] | None = None,
    constituent_refresh_days: int = 7,
    benchmark: str = "SPY",
    broad_screen_hits: list[str] | None = None,
) -> dict[str, list[str]]:
    """
    Build the full universe to scan.

    Returns a dict of {asset_class: [symbols]} so the pipeline knows
    which symbols belong to which class (used for cross-asset analysis).
    """
    universe: dict[str, list[str]] = {}

    # Equity indices (dynamic — fetched from Wikipedia)
    equity = []
    if scan_sp500:
        sp500 = fetch_sp500_constituents(con, constituent_refresh_days)
        equity.extend(sp500)
        universe["sp500"] = sp500
        log.info("universe_sp500", count=len(sp500))

    if scan_nasdaq100:
        ndx = fetch_nasdaq100_constituents(con, constituent_refresh_days)
        # Deduplicate — most Nasdaq 100 stocks are already in S&P 500
        ndx_new = [s for s in ndx if s not in set(equity)]
        equity.extend(ndx_new)
        universe["nasdaq100"] = ndx
        log.info("universe_nasdaq100_incremental", new_symbols=len(ndx_new))

    universe["equities"] = equity

    # Fixed asset classes
    if include_sector_etfs:
        universe["sector_etfs"] = SECTOR_ETFS

    if include_semi_etfs:
        universe["semi_etfs"] = SEMI_ETFS

    if include_biotech_etfs:
        universe["biotech_etfs"] = BIOTECH_ETFS

    if include_china_internet_etfs:
        universe["china_internet_etfs"] = CHINA_INTERNET_ETFS

    if include_innovation_etfs:
        universe["innovation_etfs"] = INNOVATION_ETFS

    if include_bond_etfs:
        universe["bond_etfs"] = BOND_ETFS

    if include_commodities:
        universe["commodities"] = COMMODITY_SYMBOLS

    if include_international:
        universe["international"] = INTERNATIONAL_ETFS

    if include_volatility:
        universe["volatility"] = VOLATILITY_SYMBOLS

    if include_crypto_etfs:
        universe["crypto_etfs"] = CRYPTO_ETFS

    # Broad screen hits — active symbols from Finnhub US universe
    if broad_screen_hits:
        universe["broad_screen"] = list(broad_screen_hits)
        log.info("universe_broad_screen", count=len(broad_screen_hits))

    # Manual watchlist — high-conviction names outside major indices
    if watchlist:
        universe["watchlist"] = list(watchlist)
        log.info("universe_watchlist", count=len(watchlist))

    # Mandatory Tier 1 indices — always present regardless of config
    universe["mandatory_indices"] = MANDATORY_INDEX_SYMBOLS

    # Deduplicated flat list (benchmark always included)
    seen = set()
    all_symbols = []
    for syms in universe.values():
        for s in syms:
            if s not in seen:
                seen.add(s)
                all_symbols.append(s)

    if benchmark not in seen:
        all_symbols.append(benchmark)

    universe["_all"] = all_symbols
    universe["_benchmark"] = benchmark

    log.info(
        "universe_built",
        total=len(all_symbols),
        equities=len(equity),
        etfs_and_other=len(all_symbols) - len(equity),
    )

    return universe


def get_symbol_metadata(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
) -> dict:
    """Return company name and sector for a symbol (from cached constituents)."""
    row = con.execute("""
        SELECT company, sector FROM universe_constituents
        WHERE symbol = ? LIMIT 1
    """, [symbol]).fetchone()
    if row:
        return {"company": row[0], "sector": row[1]}
    return {"company": symbol, "sector": ""}
