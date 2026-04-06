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
