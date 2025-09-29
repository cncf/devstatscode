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

    let _matches = Command::new("devstats-sqlitedb")
        .version("0.1.0")
        .about("SQLite database operations for DevStats")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("SQLite database tool");

    // Determine SQLite database path
    let sqlite_path = format!("{}/devstats.db", ctx.data_dir);
    info!("SQLite database path: {}", sqlite_path);

    // Connect to SQLite database
    let sqlite_url = format!("sqlite://{}", sqlite_path);
    
    let pool = match sqlx::SqlitePool::connect(&sqlite_url).await {
        Ok(pool) => {
            info!("Connected to SQLite database");
            pool
        }
        Err(err) => {
            error!("Failed to connect to SQLite database '{}': {}", sqlite_path, err);
            return Err(err.into());
        }
    };

    // Create basic tables if they don't exist
    let create_tables_sql = vec![
        r#"
        CREATE TABLE IF NOT EXISTS projects (
            name TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project) REFERENCES projects(name)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS sync_status (
            project TEXT PRIMARY KEY,
            last_sync DATETIME,
            status TEXT,
            message TEXT,
            FOREIGN KEY (project) REFERENCES projects(name)
        )
        "#,
    ];

    for (i, sql) in create_tables_sql.iter().enumerate() {
        match sqlx::query(sql).execute(&pool).await {
            Ok(_) => {
                info!("✓ Created/verified table {}/{}", i + 1, create_tables_sql.len());
            }
            Err(err) => {
                error!("✗ Failed to create table {}/{}: {}", i + 1, create_tables_sql.len(), err);
                return Err(err.into());
            }
        }
    }

    // Check current database content
    info!("Checking SQLite database content...");

    // Count projects
    match sqlx::query("SELECT COUNT(*) as count FROM projects").fetch_one(&pool).await {
        Ok(row) => {
            let count: i64 = row.get("count");
            info!("Projects in database: {}", count);
        }
        Err(err) => {
            error!("Failed to count projects: {}", err);
        }
    }

    // Count metrics
    match sqlx::query("SELECT COUNT(*) as count FROM metrics").fetch_one(&pool).await {
        Ok(row) => {
            let count: i64 = row.get("count");
            info!("Metrics records in database: {}", count);
        }
        Err(err) => {
            error!("Failed to count metrics: {}", err);
        }
    }

    // Show recent sync status
    match sqlx::query("SELECT project, last_sync, status FROM sync_status ORDER BY last_sync DESC LIMIT 10")
        .fetch_all(&pool).await {
        Ok(rows) => {
            if rows.is_empty() {
                info!("No sync status records found");
            } else {
                info!("Recent sync status:");
                for row in rows {
                    let project: String = row.get("project");
                    let last_sync: Option<chrono::NaiveDateTime> = row.get("last_sync");
                    let status: String = row.get("status");
                    
                    let sync_time = last_sync
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "Never".to_string());
                    
                    info!("  {}: {} ({})", project, status, sync_time);
                }
            }
        }
        Err(err) => {
            error!("Failed to get sync status: {}", err);
        }
    }

    // Example: Insert current project if specified
    if !ctx.project.is_empty() {
        info!("Updating project record for: {}", ctx.project);
        
        let insert_project_sql = "INSERT OR IGNORE INTO projects (name, display_name) VALUES (?, ?)";
        
        match sqlx::query(insert_project_sql)
            .bind(&ctx.project)
            .bind(&ctx.project) // Use same name as display name for now
            .execute(&pool).await {
            Ok(_) => {
                info!("✓ Project record updated");
            }
            Err(err) => {
                error!("Failed to update project record: {}", err);
            }
        }

        // Update sync status
        let update_sync_sql = r#"
            INSERT OR REPLACE INTO sync_status (project, last_sync, status, message) 
            VALUES (?, CURRENT_TIMESTAMP, 'running', 'SQLite tool executed')
        "#;
        
        match sqlx::query(update_sync_sql)
            .bind(&ctx.project)
            .execute(&pool).await {
            Ok(_) => {
                info!("✓ Sync status updated");
            }
            Err(err) => {
                error!("Failed to update sync status: {}", err);
            }
        }
    }

    // Database maintenance
    info!("Performing database maintenance...");
    
    // VACUUM to optimize database
    match sqlx::query("VACUUM").execute(&pool).await {
        Ok(_) => {
            info!("✓ Database vacuum completed");
        }
        Err(err) => {
            error!("Database vacuum failed: {}", err);
        }
    }

    // ANALYZE to update statistics
    match sqlx::query("ANALYZE").execute(&pool).await {
        Ok(_) => {
            info!("✓ Database analyze completed");
        }
        Err(err) => {
            error!("Database analyze failed: {}", err);
        }
    }

    info!("SQLite database operations completed");
    Ok(())
}