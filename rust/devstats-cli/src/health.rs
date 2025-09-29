use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};
use sqlx::Row;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-health")
        .version("0.1.0")
        .about("Health check for DevStats system")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("Starting DevStats health check...");

    // Test database connectivity
    info!("Testing database connectivity...");
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => {
            info!("✓ Database connection successful");
            pool
        }
        Err(err) => {
            error!("✗ Database connection failed: {}", err);
            std::process::exit(1);
        }
    };

    // Test basic query
    info!("Testing basic database query...");
    match sqlx::query("SELECT 1 as test_value, NOW() as current_time")
        .fetch_one(&pool)
        .await
    {
        Ok(row) => {
            let test_value: i32 = row.get("test_value");
            let current_time: chrono::DateTime<chrono::Utc> = row.get("current_time");
            info!("✓ Basic query successful: test_value={}, current_time={}", test_value, current_time);
        }
        Err(err) => {
            error!("✗ Basic query failed: {}", err);
            std::process::exit(1);
        }
    }

    // Test table existence (gha_events is usually a core table)
    info!("Testing table existence...");
    match sqlx::query(
        "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_name = 'gha_events'"
    )
    .fetch_one(&pool)
    .await
    {
        Ok(row) => {
            let table_count: i64 = row.get("table_count");
            if table_count > 0 {
                info!("✓ Core table 'gha_events' exists");
            } else {
                error!("✗ Core table 'gha_events' not found");
                std::process::exit(1);
            }
        }
        Err(err) => {
            error!("✗ Table existence check failed: {}", err);
            std::process::exit(1);
        }
    }

    // Test data availability (check if we have recent events)
    info!("Testing data availability...");
    match sqlx::query(
        "SELECT COUNT(*) as event_count FROM gha_events WHERE created_at > NOW() - INTERVAL '7 days'"
    )
    .fetch_one(&pool)
    .await
    {
        Ok(row) => {
            let event_count: i64 = row.get("event_count");
            if event_count > 0 {
                info!("✓ Recent data available: {} events in the last 7 days", event_count);
            } else {
                error!("⚠ No recent data: {} events in the last 7 days", event_count);
                // This is a warning, not a failure
            }
        }
        Err(err) => {
            error!("✗ Data availability check failed: {}", err);
            // This might be expected if gha_events doesn't exist yet, so don't exit
        }
    }

    // Check configuration
    info!("Checking configuration...");
    info!("✓ Database host: {}", ctx.pg_host);
    info!("✓ Database name: {}", ctx.pg_db);
    info!("✓ Data directory: {}", ctx.data_dir);
    
    if !ctx.project.is_empty() {
        info!("✓ Project: {}", ctx.project);
    }

    // Check environment variables
    info!("Checking critical environment variables...");
    let env_checks = vec![
        ("PG_HOST", std::env::var("PG_HOST").is_ok()),
        ("PG_DB", std::env::var("PG_DB").is_ok()),
        ("PG_USER", std::env::var("PG_USER").is_ok()),
        ("PG_PASS", std::env::var("PG_PASS").is_ok()),
    ];

    for (var, exists) in env_checks {
        if exists {
            info!("✓ {} is set", var);
        } else {
            info!("⚠ {} not set (using default)", var);
        }
    }

    info!("Health check completed successfully!");
    
    println!("\n=== DevStats Health Check Results ===");
    println!("Database Connection: ✓ OK");
    println!("Basic Queries: ✓ OK");
    println!("Table Structure: ✓ OK");
    println!("Configuration: ✓ OK");
    println!("\nSystem appears to be healthy and ready for DevStats operations.");

    Ok(())
}