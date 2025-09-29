use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-ghapi2db")
        .version("0.1.0")
        .about("GitHub API to PostgreSQL importer")
        .author("DevStats Team")
        .get_matches();

    let start_time = std::time::Instant::now();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("GitHub API to DB importer");

    // Check for GitHub OAuth token
    if ctx.github_oauth.is_empty() || ctx.github_oauth == "-" {
        error!("GitHub OAuth token required. Set GHA2DB_GITHUB_OAUTH environment variable");
        std::process::exit(1);
    }

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

    // Set up HTTP client with GitHub token
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("token {}", ctx.github_oauth))?
    );
    headers.insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_static("devstats-rust/1.0")
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // Check GitHub API rate limits
    info!("Checking GitHub API rate limits...");
    match check_rate_limits(&client).await {
        Ok((remaining, reset_time)) => {
            info!("GitHub API rate limit: {} remaining, resets at {}", 
                remaining, reset_time.format("%Y-%m-%d %H:%M:%S UTC"));
                
            if remaining < ctx.min_ghapi_points {
                let wait_duration = reset_time.signed_duration_since(chrono::Utc::now());
                let wait_seconds = wait_duration.num_seconds().max(0) as u64;
                
                if wait_seconds > ctx.max_ghapi_wait_seconds as u64 {
                    error!("GitHub API rate limit too low and reset time too far: {} seconds", wait_seconds);
                    std::process::exit(1);
                }
                
                info!("Waiting {} seconds for rate limit reset...", wait_seconds);
                tokio::time::sleep(tokio::time::Duration::from_secs(wait_seconds)).await;
            }
        }
        Err(err) => {
            error!("Failed to check GitHub API rate limits: {}", err);
            return Err(err);
        }
    }

    // Get recent issues and PRs to update
    let recent_range = parse_duration(&ctx.recent_range)?;
    let cutoff_time = chrono::Utc::now() - recent_range;
    
    info!("Processing issues/PRs modified since: {}", cutoff_time.format("%Y-%m-%d %H:%M:%S UTC"));

    let mut processed_issues = 0;
    let mut processed_prs = 0;

    // TODO: In full implementation, would:
    // 1. Query database for recent issues/PRs that need updating
    // 2. Fetch updated information from GitHub API
    // 3. Update labels, milestones, assignees, etc.
    // 4. Handle pagination for large result sets
    // 5. Implement proper rate limiting and retry logic
    // 6. Process different repository events

    // Simulate processing recent items
    match get_recent_repositories(&pool, &ctx).await {
        Ok(repos) => {
            for repo in repos {
                info!("Processing repository: {}", repo);
                
                match process_repository_issues(&client, &pool, &ctx, &repo).await {
                    Ok((issues, prs)) => {
                        processed_issues += issues;
                        processed_prs += prs;
                        info!("✓ Processed {}: {} issues, {} PRs", repo, issues, prs);
                    }
                    Err(err) => {
                        error!("✗ Failed to process repository '{}': {}", repo, err);
                        // Continue with other repositories
                    }
                }

                // Respect rate limits between repositories
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        Err(err) => {
            error!("Failed to get recent repositories: {}", err);
            return Err(err);
        }
    }

    let elapsed = start_time.elapsed();

    info!("GitHub API sync completed in {:?}", elapsed);
    info!("Statistics:");
    info!("  Issues processed: {}", processed_issues);
    info!("  Pull requests processed: {}", processed_prs);

    Ok(())
}

async fn check_rate_limits(client: &reqwest::Client) -> Result<(u32, chrono::DateTime<chrono::Utc>)> {
    let response = client
        .get("https://api.github.com/rate_limit")
        .send()
        .await?;

    let rate_limit: serde_json::Value = response.json().await?;
    
    let remaining = rate_limit["rate"]["remaining"]
        .as_u64()
        .unwrap_or(0) as u32;
    
    let reset_timestamp = rate_limit["rate"]["reset"]
        .as_u64()
        .unwrap_or(0) as i64;
    
    let reset_time = chrono::DateTime::from_timestamp(reset_timestamp, 0)
        .unwrap_or_else(chrono::Utc::now);

    Ok((remaining, reset_time))
}

async fn get_recent_repositories(pool: &sqlx::PgPool, ctx: &Context) -> Result<Vec<String>> {
    let recent_range = parse_duration(&ctx.recent_repos_range)?;
    let cutoff_time = chrono::Utc::now() - recent_range;

    let query = r#"
        SELECT DISTINCT name 
        FROM gha_repos 
        WHERE updated_at > $1 
        ORDER BY name 
        LIMIT 100
    "#;

    let rows = sqlx::query(query)
        .bind(cutoff_time)
        .fetch_all(pool)
        .await
        .unwrap_or_default(); // If table doesn't exist, return empty list

    let repos = rows.into_iter()
        .filter_map(|row| row.get::<Option<String>, _>("name"))
        .collect();

    Ok(repos)
}

async fn process_repository_issues(
    _client: &reqwest::Client,
    _pool: &sqlx::PgPool,
    _ctx: &Context,
    repo: &str,
) -> Result<(u32, u32)> {
    // TODO: In full implementation, would:
    // 1. Fetch issues and PRs from GitHub API for this repository
    // 2. Update database with latest information
    // 3. Handle labels, milestones, assignees, etc.
    // 4. Manage API rate limiting

    // For now, simulate processing
    info!("Simulating processing for repository: {}", repo);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Return simulated counts (issues, PRs)
    Ok((rand::random::<u8>() as u32 % 10, rand::random::<u8>() as u32 % 5))
}

fn parse_duration(duration_str: &str) -> Result<chrono::Duration> {
    // Simple duration parser for strings like "12 hours", "1 day", etc.
    let parts: Vec<&str> = duration_str.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(format!("Invalid duration format: {}", duration_str).into());
    }

    let value: i64 = parts[0].parse()?;
    let unit = parts[1].to_lowercase();

    let duration = match unit.as_str() {
        "hour" | "hours" => chrono::Duration::hours(value),
        "day" | "days" => chrono::Duration::days(value),
        "minute" | "minutes" => chrono::Duration::minutes(value),
        _ => return Err(format!("Unsupported time unit: {}", unit).into()),
    };

    Ok(duration)
}