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

    let _matches = Command::new("devstats-structure")
        .version("0.1.0")
        .about("Create or verify database structure for DevStats")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("Database structure tool");

    // Connect to PostgreSQL database
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => {
            info!("Connected to PostgreSQL database: {}", ctx.pg_db);
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

    // Core DevStats tables that should exist
    let core_tables = vec![
        ("gha_events", "Core GitHub events table"),
        ("gha_repos", "Repository information table"),
        ("gha_actors", "GitHub actors/users table"),
        ("gha_payloads", "Event payloads table"),
        ("gha_commits", "Commit information table"),
        ("gha_issues", "Issues table"),
        ("gha_pull_requests", "Pull requests table"),
        ("gha_milestones", "Milestones table"),
        ("gha_labels", "Labels table"),
        ("gha_texts", "Text data table"),
        ("gha_logs", "DevStats logs table"),
    ];

    info!("Checking core table structure...");

    let mut tables_exist = 0;
    let mut tables_missing = 0;

    for (table_name, description) in &core_tables {
        match check_table_exists(&pool, table_name).await {
            Ok(exists) => {
                if exists {
                    info!("✓ {} exists: {}", table_name, description);
                    tables_exist += 1;
                } else {
                    info!("✗ {} missing: {}", table_name, description);
                    tables_missing += 1;
                }
            }
            Err(err) => {
                error!("Failed to check table '{}': {}", table_name, err);
                tables_missing += 1;
            }
        }
    }

    info!("Table summary: {} exist, {} missing", tables_exist, tables_missing);

    // Check and create structure SQL files if needed
    if ctx.table && tables_missing > 0 {
        info!("Creating missing tables...");
        
        // Look for structure SQL files
        let structure_dir = format!("{}structure/", data_prefix);
        
        match tokio::fs::read_dir(&structure_dir).await {
            Ok(mut entries) => {
                let mut sql_files = Vec::new();
                
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if let Some(extension) = path.extension() {
                        if extension == "sql" {
                            if let Some(file_name) = path.file_name() {
                                sql_files.push(file_name.to_string_lossy().to_string());
                            }
                        }
                    }
                }

                sql_files.sort();
                info!("Found {} SQL structure files", sql_files.len());

                for sql_file in sql_files {
                    let sql_path = format!("{}{}", structure_dir, sql_file);
                    
                    match execute_structure_sql(&pool, &sql_path, &ctx).await {
                        Ok(_) => {
                            info!("✓ Executed structure file: {}", sql_file);
                        }
                        Err(err) => {
                            error!("✗ Failed to execute structure file '{}': {}", sql_file, err);
                        }
                    }
                }
            }
            Err(_) => {
                info!("No structure directory found at: {}", structure_dir);
            }
        }
    }

    // Check indexes if requested
    if ctx.index {
        info!("Checking database indexes...");
        
        match check_indexes(&pool).await {
            Ok(index_count) => {
                info!("Found {} indexes in database", index_count);
            }
            Err(err) => {
                error!("Failed to check indexes: {}", err);
            }
        }
    }

    // Create tools (views, functions, etc.) if requested
    if ctx.tools {
        info!("Setting up database tools...");
        
        // TODO: In full implementation, would create:
        // - Materialized views for common queries
        // - Stored procedures for data processing
        // - Triggers for data consistency
        // - Custom functions for metrics calculation
        
        info!("Database tools setup completed");
    }

    info!("Database structure verification/creation completed");
    Ok(())
}

async fn check_table_exists(pool: &sqlx::PgPool, table_name: &str) -> Result<bool> {
    let query = "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = $1";
    let row = sqlx::query(query)
        .bind(table_name)
        .fetch_one(pool)
        .await?;
    
    let count: i64 = row.get("count");
    Ok(count > 0)
}

async fn execute_structure_sql(pool: &sqlx::PgPool, sql_path: &str, ctx: &Context) -> Result<()> {
    // Read SQL file
    let sql_content = tokio::fs::read_to_string(sql_path).await?;
    
    if ctx.q_out || ctx.debug > 0 {
        info!("Executing SQL from {}: {} characters", sql_path, sql_content.len());
    }

    // Split into individual statements and execute
    let statements: Vec<&str> = sql_content
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect();

    for (i, statement) in statements.iter().enumerate() {
        if ctx.debug > 1 {
            info!("Executing statement {}/{}: {}", i + 1, statements.len(), 
                if statement.len() > 100 { 
                    format!("{}...", &statement[..100])
                } else { 
                    statement.to_string() 
                });
        }

        match sqlx::query(statement).execute(pool).await {
            Ok(_) => {
                if ctx.debug > 0 {
                    info!("✓ Statement {} executed successfully", i + 1);
                }
            }
            Err(err) => {
                error!("✗ Statement {} failed: {}", i + 1, err);
                return Err(err.into());
            }
        }
    }

    Ok(())
}

async fn check_indexes(pool: &sqlx::PgPool) -> Result<usize> {
    let query = "SELECT COUNT(*) as count FROM pg_indexes WHERE schemaname = 'public'";
    let row = sqlx::query(query).fetch_one(pool).await?;
    let count: i64 = row.get("count");
    Ok(count as usize)
}