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
    con.execute_batch(
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
         );",
    )?;
    let result = (|| -> Result<()> {
        copy_attached_table_compatible(&con, "report_decisions")?;
        copy_attached_table_compatible(&con, "report_outcomes")?;
        copy_attached_table_compatible(&con, "alpha_postmortem")?;
        copy_attached_table_compatible(&con, "algorithm_postmortem")?;
        copy_attached_table_compatible(&con, "paper_trades")?;
        copy_attached_table_compatible(&con, "strategy_ev")?;
        copy_attached_table_compatible(&con, "strategy_model_dataset")?;
        copy_attached_table_compatible(&con, "limit_move_radar_backtest")?;
        copy_attached_table_compatible(&con, "limit_up_model_dataset")?;
        copy_attached_table_compatible(&con, "limit_up_model_predictions")?;
        copy_attached_table_compatible(&con, "limit_up_model_performance")?;
        Ok(())
    })();
    con.execute_batch("DETACH report_history;")?;
    result?;
    Ok(())
}

fn copy_attached_table_compatible(con: &Connection, table: &str) -> Result<()> {
    if !attached_table_exists(con, "report_history", table)? {
        return Ok(());
    }

    let main_catalog = current_catalog(con)?;
    let target_columns = table_columns(con, &main_catalog, table)?;
    let source_columns = table_columns(con, "report_history", table)?;
    if target_columns.is_empty() || source_columns.is_empty() {
        return Ok(());
    }

    let target_list = target_columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let source_set = source_columns
        .iter()
        .map(|c| c.as_str())
        .collect::<std::collections::HashSet<_>>();
    let select_list = target_columns
        .iter()
        .map(|c| {
            if source_set.contains(c.as_str()) {
                format!("src.{}", quote_ident(c))
            } else if c == "created_at" {
                "current_timestamp".to_string()
            } else {
                "NULL".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO {table} ({target_list})
         SELECT {select_list}
         FROM report_history.{table} AS src",
        table = quote_ident(table),
        target_list = target_list,
        select_list = select_list,
    );
    con.execute_batch(&sql)?;
    Ok(())
}

fn attached_table_exists(con: &Connection, schema: &str, table: &str) -> Result<bool> {
    let count: i64 = con.query_row(
        "SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?",
        duckdb::params![schema, table],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn table_columns(con: &Connection, catalog: &str, table: &str) -> Result<Vec<String>> {
    let mut stmt = con.prepare(
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?
         ORDER BY ordinal_position",
    )?;
    let rows = stmt.query_map(duckdb::params![catalog, table], |row| {
        row.get::<_, String>(0)
    })?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn current_catalog(con: &Connection) -> Result<String> {
    Ok(con.query_row("SELECT current_database()", [], |row| row.get(0))?)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
