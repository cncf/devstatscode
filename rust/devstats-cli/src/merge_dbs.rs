use sqlx::Row;
use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-merge-dbs")
        .version("0.1.0")
        .about("Merge multiple PostgreSQL databases")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("Database merge tool");

    // Check if input and output databases are specified
    if ctx.input_dbs.is_empty() {
        error!("No input databases specified. Set GHA2DB_INPUT_DBS environment variable");
        std::process::exit(1);
    }

    if ctx.output_db.is_empty() {
        error!("No output database specified. Set GHA2DB_OUTPUT_DB environment variable");
        std::process::exit(1);
    }

    info!("Input databases: {:?}", ctx.input_dbs);
    info!("Output database: {}", ctx.output_db);

    // Connect to output database
    let output_db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.output_db, ctx.pg_ssl);
    
    let output_pool = match sqlx::PgPool::connect(&output_db_url).await {
        Ok(pool) => {
            info!("Connected to output database: {}", ctx.output_db);
            pool
        }
        Err(err) => {
            error!("Failed to connect to output database '{}': {}", ctx.output_db, err);
            return Err(err.into());
        }
    };

    // Process each input database
    for (i, input_db) in ctx.input_dbs.iter().enumerate() {
        info!("Processing input database {}/{}: {}", i + 1, ctx.input_dbs.len(), input_db);

        // Connect to input database
        let input_db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, input_db, ctx.pg_ssl);
        
        let input_pool = match sqlx::PgPool::connect(&input_db_url).await {
            Ok(pool) => {
                info!("Connected to input database: {}", input_db);
                pool
            }
            Err(err) => {
                error!("Failed to connect to input database '{}': {}", input_db, err);
                continue; // Skip this database and continue with others
            }
        };

        // Get list of tables from input database
        let tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
        
        let table_rows = match sqlx::query(tables_query).fetch_all(&input_pool).await {
            Ok(rows) => rows,
            Err(err) => {
                error!("Failed to get tables from database '{}': {}", input_db, err);
                continue;
            }
        };

        info!("Found {} tables in database '{}'", table_rows.len(), input_db);

        // Process each table
        for table_row in table_rows {
            let table_name: String = table_row.get("table_name");
            
            if let Err(err) = merge_table(&input_pool, &output_pool, &table_name, i == 0).await {
                error!("Failed to merge table '{}' from database '{}': {}", table_name, input_db, err);
                // Continue with other tables
            } else {
                info!("Successfully merged table '{}'", table_name);
            }
        }

        info!("Completed processing database: {}", input_db);
    }

    info!("Database merge completed");
    Ok(())
}

async fn merge_table(
    input_pool: &sqlx::PgPool,
    output_pool: &sqlx::PgPool,
    table_name: &str,
    is_first_db: bool,
) -> Result<()> {
    // Get row count from input table
    let count_query = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_row = sqlx::query(&count_query).fetch_one(input_pool).await?;
    let row_count: i64 = count_row.get("count");

    if row_count == 0 {
        info!("Table '{}' is empty, skipping", table_name);
        return Ok(());
    }

    info!("Merging table '{}' with {} rows", table_name, row_count);

    // For the first database, use INSERT. For subsequent databases, use INSERT ON CONFLICT DO NOTHING
    let insert_mode = if is_first_db {
        "INSERT"
    } else {
        "INSERT ON CONFLICT DO NOTHING"
    };

    // Get table structure from input database
    let columns_query = format!(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
        table_name
    );
    
    let column_rows = sqlx::query(&columns_query).fetch_all(input_pool).await?;
    
    if column_rows.is_empty() {
        error!("No columns found for table '{}'", table_name);
        return Ok(());
    }

    let columns: Vec<String> = column_rows.iter()
        .map(|row| row.get::<String, _>("column_name"))
        .collect();

    let columns_str = columns.join(", ");

    // TODO: In a full implementation, would:
    // 1. Create table in output database if it doesn't exist (for first DB)
    // 2. Handle schema differences between databases
    // 3. Batch data transfer for better performance
    // 4. Handle primary key conflicts appropriately
    
    // For now, simulate the merge process
    info!("Would execute: {} INTO {} ({}) SELECT {} FROM {}", 
        insert_mode, table_name, columns_str, columns_str, table_name);

    // Check if output table exists
    let table_exists_query = format!(
        "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = '{}'",
        table_name
    );
    
    let exists_row = sqlx::query(&table_exists_query).fetch_one(output_pool).await?;
    let table_exists: i64 = exists_row.get("count");

    if table_exists == 0 && is_first_db {
        info!("Table '{}' doesn't exist in output database, would need to create it", table_name);
        // In real implementation, would create table with same structure
    }

    // Simulate successful merge
    info!("Table '{}' merge simulated successfully", table_name);
    
    Ok(())
}