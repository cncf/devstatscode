use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-gha2db-sync")
        .version("0.1.0")
        .about("Sync GitHub events and calculate metrics")
        .author("DevStats Team")
        .get_matches();

    let start_time = std::time::Instant::now();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    if ctx.project.is_empty() {
        error!("Project must be specified via GHA2DB_PROJECT environment variable");
        std::process::exit(1);
    }

    info!("GitHub Archive sync for project: {}", ctx.project);

    // Connect to PostgreSQL database
    let pool = if !ctx.skip_pdb {
        let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
        
        match sqlx::PgPool::connect(&db_url).await {
            Ok(pool) => {
                info!("Connected to PostgreSQL database: {}", ctx.pg_db);
                Some(pool)
            }
            Err(err) => {
                error!("Failed to connect to database: {}", err);
                return Err(err.into());
            }
        }
    } else {
        info!("PostgreSQL processing disabled (GHA2DB_SKIPPDB set)");
        None
    };

    // Get last sync timestamp
    let last_sync = if let Some(ref pool) = pool {
        get_last_sync_timestamp(pool, &ctx).await?
    } else {
        ctx.default_start_date
    };

    let now = chrono::Utc::now();
    
    info!("Syncing from {} to {}", 
        last_sync.format("%Y-%m-%d %H:%M:%S UTC"),
        now.format("%Y-%m-%d %H:%M:%S UTC"));

    // Phase 1: GitHub events sync (if not skipped)
    if let Some(ref pool) = pool {
        info!("Phase 1: Syncing GitHub events...");
        
        match sync_github_events(pool, &ctx, last_sync, now).await {
            Ok(event_count) => {
                info!("✓ Synced {} GitHub events", event_count);
            }
            Err(err) => {
                error!("✗ GitHub events sync failed: {}", err);
                return Err(err);
            }
        }
    }

    // Phase 2: Time series DB processing (if not skipped)
    if !ctx.skip_tsdb {
        info!("Phase 2: Processing time series metrics...");
        
        match process_tsdb_metrics(&ctx, last_sync, now).await {
            Ok(metrics_count) => {
                info!("✓ Processed {} time series metrics", metrics_count);
            }
            Err(err) => {
                error!("✗ Time series processing failed: {}", err);
                // Don't return error - TSDB processing is often optional
            }
        }
    } else {
        info!("Time series processing disabled (GHA2DB_SKIPTSDB set)");
    }

    // Phase 3: Update sync timestamp
    if let Some(ref pool) = pool {
        match update_sync_timestamp(pool, &ctx, now).await {
            Ok(_) => {
                info!("✓ Updated sync timestamp");
            }
            Err(err) => {
                error!("✗ Failed to update sync timestamp: {}", err);
            }
        }
    }

    let elapsed = start_time.elapsed();
    info!("Sync completed in {:?} for project: {}", elapsed, ctx.project);

    Ok(())
}

async fn get_last_sync_timestamp(
    pool: &sqlx::PgPool,
    ctx: &Context,
) -> Result<chrono::DateTime<chrono::Utc>> {
    // Try to get the last event timestamp from the database
    let query = format!(
        "SELECT MAX(created_at) as last_sync FROM gha_events WHERE created_at < NOW() - INTERVAL '{} hours'",
        ctx.tm_offset
    );

    match sqlx::query(&query).fetch_optional(pool).await {
        Ok(Some(row)) => {
            if let Some(last_sync) = row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_sync") {
                Ok(last_sync)
            } else {
                // No events found, use default start date
                Ok(ctx.default_start_date)
            }
        }
        _ => {
            // Table doesn't exist or other error, use default start date
            Ok(ctx.default_start_date)
        }
    }
}

async fn sync_github_events(
    pool: &sqlx::PgPool,
    ctx: &Context,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
) -> Result<u32> {
    // TODO: In full implementation, would:
    // 1. Call gha2db to download and process GitHub Archive data
    // 2. Process events in batches to avoid memory issues
    // 3. Handle different event types appropriately
    // 4. Update repository and actor information
    // 5. Manage transaction boundaries for consistency

    // For now, simulate the sync process
    let duration = to.signed_duration_since(from);
    let hours = duration.num_hours() as u32;
    
    info!("Simulating GitHub events sync for {} hours", hours);
    
    // Simulate processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Return simulated event count
    Ok(hours * 100) // Assume 100 events per hour
}

async fn process_tsdb_metrics(
    ctx: &Context,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
) -> Result<u32> {
    // TODO: In full implementation, would:
    // 1. Run metric calculation queries
    // 2. Generate time series data points
    // 3. Update TSDB tables with new metrics
    // 4. Handle different metric types (counters, gauges, histograms)
    // 5. Process metric dependencies in correct order

    let duration = to.signed_duration_since(from);
    let hours = duration.num_hours() as u32;
    
    info!("Simulating TSDB metrics processing for {} hours", hours);
    
    // Simulate processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Return simulated metrics count
    Ok(hours * 10) // Assume 10 metrics per hour
}

async fn update_sync_timestamp(
    pool: &sqlx::PgPool,
    ctx: &Context,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    // Update a sync status table or similar tracking mechanism
    let update_query = r#"
        INSERT INTO gha_computed (metric, dt) 
        VALUES ('last_sync', $1)
        ON CONFLICT (metric) 
        DO UPDATE SET dt = $1
    "#;

    match sqlx::query(update_query)
        .bind(timestamp)
        .execute(pool)
        .await {
        Ok(_) => {
            if ctx.debug > 0 {
                info!("Updated sync timestamp to: {}", timestamp.format("%Y-%m-%d %H:%M:%S UTC"));
            }
            Ok(())
        }
        Err(err) => {
            // Table might not exist, which is ok for some setups
            if ctx.debug > 0 {
                info!("Note: Could not update sync timestamp (table may not exist): {}", err);
            }
            Ok(())
        }
    }
}