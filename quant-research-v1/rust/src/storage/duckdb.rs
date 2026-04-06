use anyhow::Result;
use duckdb::Connection;
use std::path::Path;

pub fn open(path: &str) -> Result<Connection> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let con = Connection::open(path)?;
    init_tables(&con)?;
    Ok(con)
}

fn init_tables(con: &Connection) -> Result<()> {
    con.execute_batch("
        CREATE TABLE IF NOT EXISTS news_items (
            symbol          VARCHAR NOT NULL,
            headline        VARCHAR NOT NULL,
            summary         VARCHAR,
            source          VARCHAR,
            url             VARCHAR NOT NULL,
            published_at    TIMESTAMP,
            fetched_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (symbol, url)
        );

        CREATE TABLE IF NOT EXISTS sec_filings (
            symbol           VARCHAR NOT NULL,
            cik              VARCHAR,
            accession_number VARCHAR NOT NULL,    -- unique SEC accession (e.g. 0001234-24-001234)
            form_type        VARCHAR NOT NULL,    -- 8-K, 10-Q, 10-K
            filed_date       DATE NOT NULL,
            items            VARCHAR,             -- JSON array of plain-English item descriptions
            description      VARCHAR,
            filing_url       VARCHAR,
            fetched_at       TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (symbol, accession_number)
        );

        CREATE TABLE IF NOT EXISTS polymarket_events (
            market_id       VARCHAR NOT NULL,
            fetch_date      DATE NOT NULL DEFAULT CURRENT_DATE,
            question        VARCHAR NOT NULL,
            category        VARCHAR,
            p_yes           DOUBLE,               -- probability 0-1 when binary market has explicit Yes label
            p_no            DOUBLE,
            raw_outcomes    VARCHAR,              -- raw JSON: outcomes labels + outcomePrices arrays
            volume_usd      DOUBLE,
            end_date        DATE,
            fetched_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (market_id, fetch_date)
        );

        CREATE TABLE IF NOT EXISTS macro_daily (
            date            DATE NOT NULL,
            series_id       VARCHAR NOT NULL,
            series_name     VARCHAR,              -- human-readable label
            value           DOUBLE,
            PRIMARY KEY (date, series_id)
        );

        CREATE TABLE IF NOT EXISTS earnings_calendar (
            symbol          VARCHAR NOT NULL,
            report_date     DATE NOT NULL,
            fiscal_period   VARCHAR,
            fiscal_year     INTEGER,
            fiscal_quarter  INTEGER,
            estimate_eps    DOUBLE,
            actual_eps      DOUBLE,
            surprise_pct    DOUBLE,
            PRIMARY KEY (symbol, report_date)
        );

        CREATE TABLE IF NOT EXISTS index_changes (
            index_symbol    VARCHAR NOT NULL,
            symbol          VARCHAR NOT NULL,
            change_type     VARCHAR NOT NULL,   -- 'add' | 'remove'
            change_date     DATE NOT NULL,
            fetched_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (index_symbol, symbol, change_date)
        );
    ")?;

    // Idempotent migrations for existing databases
    let _ = con.execute_batch("
        ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS fiscal_year INTEGER;
        ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS fiscal_quarter INTEGER;
    ");

    // Migration: polymarket_events PK(market_id) → PK(market_id, fetch_date)
    // Try adding the column; if it succeeds, the table had the old schema → recreate
    let added = con.execute_batch(
        "ALTER TABLE polymarket_events ADD COLUMN fetch_date DATE DEFAULT CURRENT_DATE;"
    );
    if added.is_ok() {
        // Column didn't exist → old schema. Recreate with new PK.
        let _ = con.execute_batch("
            ALTER TABLE polymarket_events RENAME TO _polymarket_old;
            CREATE TABLE polymarket_events (
                market_id       VARCHAR NOT NULL,
                fetch_date      DATE NOT NULL DEFAULT CURRENT_DATE,
                question        VARCHAR NOT NULL,
                category        VARCHAR,
                p_yes           DOUBLE,
                p_no            DOUBLE,
                raw_outcomes    VARCHAR,
                volume_usd      DOUBLE,
                end_date        DATE,
                fetched_at      TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (market_id, fetch_date)
            );
            INSERT INTO polymarket_events
                SELECT market_id, COALESCE(fetched_at::DATE, CURRENT_DATE),
                       question, category, p_yes, p_no, raw_outcomes,
                       volume_usd, end_date, fetched_at
                FROM _polymarket_old;
            DROP TABLE _polymarket_old;
        ");
    }

    Ok(())
}
