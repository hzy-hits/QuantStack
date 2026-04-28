use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use tracing::info;

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    db.execute_batch(crate::storage::schema::CREATE_TABLES)?;
    let date_str = as_of.to_string();
    let history_start = (as_of - Duration::days(90)).to_string();

    db.execute(
        "DELETE FROM price_features WHERE as_of = CAST(? AS DATE)",
        duckdb::params![date_str.clone()],
    )?;
    db.execute(
        "
        INSERT INTO price_features (
            as_of, ts_code, close_now, close_5d_ago, close_20d_ago,
            high_20d, low_20d, avg_vol_5, avg_vol_base, std5_ret, std20_ret,
            ret_5d, ret_20d, atr_pct_14, n_obs
        )
        WITH ranked AS (
            SELECT ts_code, trade_date, close, high, low, vol, pct_chg,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
              AND trade_date >= CAST(? AS DATE)
        ),
        agg AS (
            SELECT
                ts_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago,
                MAX(CASE WHEN rn = 21 THEN close END) AS close_20d_ago,
                MAX(CASE WHEN rn BETWEEN 2 AND 21 THEN high END) AS high_20d,
                MIN(CASE WHEN rn BETWEEN 2 AND 21 THEN low END) AS low_20d,
                AVG(CASE WHEN rn <= 5 THEN vol END) AS avg_vol_5,
                AVG(CASE WHEN rn BETWEEN 6 AND 20 THEN vol END) AS avg_vol_base,
                STDDEV_POP(CASE WHEN rn <= 5 THEN pct_chg END) AS std5_ret,
                STDDEV_POP(CASE WHEN rn <= 20 THEN pct_chg END) AS std20_ret,
                AVG(CASE WHEN rn <= 14 AND close > 0 THEN (high - low) / close * 100.0 END) AS atr_pct_14,
                COUNT(CASE WHEN rn <= 25 THEN 1 END) AS n_obs
            FROM ranked
            WHERE rn <= 25
            GROUP BY ts_code
        )
        SELECT
            CAST(? AS DATE) AS as_of,
            ts_code,
            close_now,
            close_5d_ago,
            close_20d_ago,
            high_20d,
            low_20d,
            avg_vol_5,
            avg_vol_base,
            std5_ret,
            std20_ret,
            CASE WHEN close_5d_ago > 0 THEN (close_now / close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            CASE WHEN close_20d_ago > 0 THEN (close_now / close_20d_ago - 1.0) * 100.0 ELSE 0 END AS ret_20d,
            COALESCE(atr_pct_14, 0) AS atr_pct_14,
            n_obs
        FROM agg
        WHERE close_now IS NOT NULL
          AND n_obs >= 6
        ",
        duckdb::params![date_str.clone(), history_start, date_str.clone()],
    )?;

    let rows = db.query_row(
        "SELECT COUNT(*) FROM price_features WHERE as_of = CAST(? AS DATE)",
        duckdb::params![date_str],
        |row| row.get::<_, i64>(0),
    )? as usize;
    info!(rows, %as_of, "price_features complete");
    Ok(rows)
}

pub fn ensure(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    db.execute_batch(crate::storage::schema::CREATE_TABLES)?;
    let date_str = as_of.to_string();
    let rows = db.query_row(
        "SELECT COUNT(*) FROM price_features WHERE as_of = CAST(? AS DATE)",
        duckdb::params![date_str],
        |row| row.get::<_, i64>(0),
    )? as usize;
    if rows > 0 {
        return Ok(rows);
    }
    compute(db, as_of)
}
