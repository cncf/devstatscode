use devstats_core::{Context, Result};
use tracing::{info, error};
use std::collections::HashMap;
use sqlx::{Row, Column};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let start_time = std::time::Instant::now();

    // Check command line arguments exactly like Go version
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Required SQL file name [param1 value1 [param2 value2 ...]]");
        println!("Special replace 'qr' 'period,from,to' is used for {{{{period.alias.name}}}} replacements");
        std::process::exit(1);
    }

    // Initialize context from environment
    let ctx = Context::from_env()?;
    
    let sql_file = &args[1];
    let params: Vec<String> = args[2..].to_vec();

    // SQL arguments number - must be pairs
    if params.len() % 2 != 0 {
        error!("Must provide correct parameter value pairs. Got {} parameters", params.len());
        std::process::exit(1);
    }

    // Parse SQL arguments
    let mut replaces = HashMap::new();
    let mut i = 0;
    while i < params.len() {
        let key = &params[i];
        let mut value = params[i + 1].clone();
        
        // Support special "readfile:replacement.dat" mode
        if value.starts_with("readfile:") {
            let filename = &value[9..];
            if ctx.debug > 0 {
                info!("Reading file: {}", filename);
            }
            match tokio::fs::read_to_string(filename).await {
                Ok(content) => value = content,
                Err(err) => {
                    error!("Failed to read file '{}': {}", filename, err);
                    return Err(err.into());
                }
            }
        }
        
        replaces.insert(key.clone(), value);
        i += 2;
    }

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else if ctx.absolute {
        String::new()
    } else {
        ctx.data_dir.clone()
    };

    // Read SQL file
    let sql_file_path = format!("{}{}", data_prefix, sql_file);
    let mut sql_query = match tokio::fs::read_to_string(&sql_file_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read SQL file '{}': {}", sql_file_path, err);
            return Err(err.into());
        }
    };

    info!("Executing SQL file: {}", sql_file_path);

    // Apply replacements
    for (key, value) in &replaces {
        sql_query = sql_query.replace(key, value);
    }

    // Handle special explain mode
    if ctx.explain {
        sql_query = format!("EXPLAIN {}", sql_query);
    }

    if ctx.q_out || ctx.debug > 1 {
        info!("SQL Query:\n{}", sql_query);
    }

    // Connect to PostgreSQL database
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => pool,
        Err(err) => {
            error!("Failed to connect to database: {}", err);
            return Err(err.into());
        }
    };

    info!("Connected to PostgreSQL database");

    // Execute SQL query
    match sqlx::query(&sql_query).fetch_all(&pool).await {
        Ok(rows) => {
            info!("Query executed successfully, {} rows returned", rows.len());
            
            // Print results in a simple format
            if !rows.is_empty() {
                // Get column names from the first row
                if let Some(first_row) = rows.first() {
                    let columns = first_row.columns();
                    
                    // Print header
                    let header: Vec<&str> = columns.iter().map(|col| col.name()).collect();
                    println!("{}", header.join("\t"));
                    
                    // Print rows
                    for row in &rows {
                        let mut values = Vec::new();
                        for i in 0..columns.len() {
                            // Try to get value as string - this is a simplified approach
                            let value = match row.try_get::<Option<String>, _>(i) {
                                Ok(Some(v)) => v,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => {
                                    // Try as other types if string fails
                                    if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(i) {
                                        v.to_string()
                                    } else if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(i) {
                                        v.to_string()
                                    } else if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(i) {
                                        v.to_string()
                                    } else {
                                        "NULL".to_string()
                                    }
                                }
                            };
                            values.push(value);
                        }
                        println!("{}", values.join("\t"));
                    }
                }
            }
        }
        Err(err) => {
            error!("Failed to execute SQL query: {}", err);
            return Err(err.into());
        }
    }

    let elapsed = start_time.elapsed();
    if ctx.debug >= 0 {
        println!("Time: {:?}", elapsed);
    }

    Ok(())
}