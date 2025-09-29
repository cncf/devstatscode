use sqlx::Row;
use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};
use serde::{Deserialize, Serialize};
use regex::Regex;

/// Column configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    #[serde(rename = "table_regexp")]
    pub table_regexp: String,
    pub tag: String,
    pub column: String,
    #[serde(default)]
    pub hll: bool,
}

/// Collection of column configurations
#[derive(Debug, Serialize, Deserialize)]
pub struct Columns {
    pub columns: Vec<Column>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-columns")
        .version("0.1.0")
        .about("Ensure that specific TSDB series have all needed columns")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // If skip TSDB - nothing to do
    if ctx.skip_tsdb {
        info!("Skipping TSDB processing due to skip_tsdb flag");
        return Ok(());
    }

    // Connect to PostgreSQL database
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => {
            info!("Connected to PostgreSQL database");
            pool
        }
        Err(err) => {
            error!("Failed to connect to database: {}", err);
            return Err(err.into());
        }
    };

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read columns config
    let columns_yaml_path = if ctx.columns_yaml.is_empty() {
        let mut path = format!("{}metrics/", data_prefix);
        if !ctx.project.is_empty() {
            path.push_str(&format!("{}/", ctx.project));
        }
        path.push_str("columns.yaml");
        path
    } else {
        format!("{}{}", data_prefix, ctx.columns_yaml)
    };

    info!("Reading columns configuration from: {}", columns_yaml_path);

    let columns_content = match tokio::fs::read_to_string(&columns_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read columns YAML file '{}': {}", columns_yaml_path, err);
            return Err(err.into());
        }
    };

    let all_columns: Columns = serde_yaml::from_str(&columns_content)?;

    info!("Read {} column configs from '{}'", all_columns.columns.len(), columns_yaml_path);

    // Process each column configuration
    for (i, column) in all_columns.columns.iter().enumerate() {
        info!("Processing column config {}/{}: {:?}", i + 1, all_columns.columns.len(), column);

        if let Err(err) = process_column(&pool, &ctx, column).await {
            error!("Failed to process column config '{}': {}", column.tag, err);
            // Continue with other columns instead of failing completely
        }
    }

    info!("Column processing completed");
    Ok(())
}

async fn process_column(
    pool: &sqlx::PgPool,
    ctx: &Context,
    column: &Column,
) -> Result<()> {
    // Compile regex for table matching
    let table_regex = match Regex::new(&column.table_regexp) {
        Ok(regex) => regex,
        Err(err) => {
            error!("Invalid table regexp '{}': {}", column.table_regexp, err);
            return Err(err.into());
        }
    };

    // Get all table names that match the pattern
    let rows = sqlx::query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    .fetch_all(pool)
    .await?;

    let mut matching_tables = Vec::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        if table_regex.is_match(&table_name) {
            matching_tables.push(table_name);
        }
    }

    if matching_tables.is_empty() {
        info!("No tables match regexp '{}' for column '{}'", column.table_regexp, column.tag);
        return Ok(());
    }

    info!("Found {} matching tables for column '{}': {:?}", 
        matching_tables.len(), column.tag, matching_tables);

    // For each matching table, ensure the column exists
    for table_name in &matching_tables {
        if let Err(err) = ensure_column_exists(pool, ctx, table_name, column).await {
            error!("Failed to ensure column '{}' exists in table '{}': {}", 
                column.column, table_name, err);
            // Continue with other tables
        } else {
            info!("Column '{}' verified/created in table '{}'", column.column, table_name);
        }
    }

    Ok(())
}

async fn ensure_column_exists(
    pool: &sqlx::PgPool,
    ctx: &Context,
    table_name: &str,
    column: &Column,
) -> Result<()> {
    // Check if column already exists
    let rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND column_name = $2"
    )
    .bind(table_name)
    .bind(&column.column)
    .fetch_all(pool)
    .await?;

    if !rows.is_empty() {
        if ctx.debug > 0 {
            info!("Column '{}' already exists in table '{}'", column.column, table_name);
        }
        return Ok(());
    }

    // Column doesn't exist, create it
    let column_type = if column.hll { "hll" } else { "text" };
    let alter_sql = format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
        table_name, column.column, column_type
    );

    if ctx.q_out {
        info!("Executing SQL: {}", alter_sql);
    }

    match sqlx::query(&alter_sql).execute(pool).await {
        Ok(_) => {
            info!("Added column '{}' ({}) to table '{}'", column.column, column_type, table_name);
        }
        Err(err) => {
            error!("Failed to add column '{}' to table '{}': {}", column.column, table_name, err);
            return Err(err.into());
        }
    }

    Ok(())
}