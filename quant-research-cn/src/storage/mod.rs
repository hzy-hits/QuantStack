pub mod schema;

use anyhow::Result;
use duckdb::Connection;
use std::path::Path;

pub fn open(path: &str) -> Result<Connection> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let con = Connection::open(path)?;
    init_schema(&con)?;
    Ok(con)
}

pub fn init_schema(con: &Connection) -> Result<()> {
    con.execute_batch(schema::CREATE_TABLES)?;
    Ok(())
}

pub fn copy_database(src: &str, dst: &str) -> Result<()> {
    let src_path = Path::new(src);
    if !src_path.exists() {
        anyhow::bail!("database snapshot source does not exist: {}", src);
    }

    let dst_path = Path::new(dst);
    if let Some(parent) = dst_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp = dst_path.with_extension("tmp");
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    std::fs::copy(src_path, &tmp)?;
    std::fs::rename(&tmp, dst_path)?;

    let src_wal = format!("{}.wal", src);
    let dst_wal = format!("{}.wal", dst);
    let src_wal_path = Path::new(&src_wal);
    let dst_wal_path = Path::new(&dst_wal);
    let dst_wal_tmp = format!("{}.tmp", dst_wal);
    let dst_wal_tmp_path = Path::new(&dst_wal_tmp);

    if src_wal_path.exists() {
        if dst_wal_tmp_path.exists() {
            std::fs::remove_file(dst_wal_tmp_path)?;
        }
        std::fs::copy(src_wal_path, dst_wal_tmp_path)?;
        std::fs::rename(dst_wal_tmp_path, dst_wal_path)?;
    } else if dst_wal_path.exists() {
        std::fs::remove_file(dst_wal_path)?;
    }

    Ok(())
}

pub fn exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn restore_report_review_history(src: &str, dst: &str) -> Result<()> {
    if src == dst || !exists(src) || !exists(dst) {
        return Ok(());
    }

    let con = Connection::open(dst)?;
    init_schema(&con)?;

    let escaped = src.replace('\'', "''");
    con.execute_batch(&format!("ATTACH '{}' AS report_history;", escaped))?;
    let result = con.execute_batch(
        "CREATE TABLE IF NOT EXISTS report_history.algorithm_postmortem (
            report_date            DATE NOT NULL,
            session                VARCHAR NOT NULL,
            symbol                 VARCHAR NOT NULL,
            selection_status       VARCHAR NOT NULL,
            evaluation_date        DATE NOT NULL,
            action_label           VARCHAR NOT NULL,
            action_source          VARCHAR,
            direction              VARCHAR,
            direction_right        BOOLEAN,
            executable             BOOLEAN,
            fill_price             DOUBLE,
            exit_price             DOUBLE,
            realized_pnl_pct       DOUBLE,
            best_possible_ret_pct  DOUBLE,
            stale_chase            BOOLEAN,
            no_fill_reason         VARCHAR,
            label                  VARCHAR NOT NULL,
            feedback_action        VARCHAR,
            feedback_weight        DOUBLE,
            action_intent          VARCHAR,
            calibration_bucket     VARCHAR,
            regime_bucket          VARCHAR,
            fill_quality           VARCHAR,
            detail_json            VARCHAR,
            PRIMARY KEY (report_date, session, symbol, selection_status)
         );

         INSERT OR REPLACE INTO report_decisions
         SELECT * FROM report_history.report_decisions;

         INSERT OR REPLACE INTO report_outcomes
         SELECT * FROM report_history.report_outcomes;

         INSERT OR REPLACE INTO alpha_postmortem
         SELECT * FROM report_history.alpha_postmortem;

         INSERT OR REPLACE INTO algorithm_postmortem
         SELECT * FROM report_history.algorithm_postmortem;

         DETACH report_history;",
    );
    result?;
    Ok(())
}
