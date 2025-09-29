use devstats_core::{Context, Result, GHAEvent, GHAParser, GHAProcessor};
use std::{env, process};
use chrono::{NaiveDate, Utc};
use std::collections::HashMap;
use regex::Regex;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    
    // Check arguments EXACTLY like Go version
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        println!("Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH ['org1,org2,...,orgN' ['repo1,repo2,...,repoN']]");
        process::exit(1);
    }

    let date_from = &args[1];
    let hour_from: i32 = args[2].parse().unwrap_or_else(|_| {
        eprintln!("Invalid hour_from: {}", args[2]);
        process::exit(1);
    });
    let date_to = &args[3];  
    let hour_to: i32 = args[3].parse().unwrap_or_else(|_| {
        eprintln!("Invalid hour_to: {}", args[4]);
        process::exit(1);
    });
    
    let orgs_filter: Option<HashMap<String, ()>> = if args.len() > 5 {
        Some(args[5].split(',').map(|s| (s.trim().to_string(), ())).collect())
    } else {
        None
    };
    
    let repos_filter: Option<HashMap<String, ()>> = if args.len() > 6 {
        Some(args[6].split(',').map(|s| (s.trim().to_string(), ())).collect())
    } else {
        None
    };

    // Initialize context from environment
    let ctx = Context::from_env()?;
    
    // Parse dates exactly like Go version
    let from_date = NaiveDate::parse_from_str(date_from, "%Y-%m-%d")
        .map_err(|_| format!("Invalid date format: {}", date_from))?;
    let to_date = NaiveDate::parse_from_str(date_to, "%Y-%m-%d")
        .map_err(|_| format!("Invalid date format: {}", date_to))?;

    // Connect to PostgreSQL database
    let pool = if ctx.db_out {
        let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
        
        match sqlx::PgPool::connect(&db_url).await {
            Ok(pool) => Some(pool),
            Err(err) => {
                eprintln!("Failed to connect to database: {}", err);
                return Err(err.into());
            }
        }
    } else {
        None
    };

    // Determine number of CPUs for threading (like Go version)
    let num_threads = if ctx.ncpus > 0 { 
        ctx.ncpus 
    } else if ctx.st { 
        1 
    } else { 
        num_cpus::get() 
    };
    
    println!("gha2db.go: Running ({} CPUs): {} - {} {} {}", 
        num_threads, 
        from_date.format("%Y-%m-%d %H:%M"),
        to_date.format("%Y-%m-%d %H:%M"),
        orgs_filter.as_ref().map(|m| m.keys().cloned().collect::<Vec<_>>().join("+")).unwrap_or_default(),
        repos_filter.as_ref().map(|m| m.keys().cloned().collect::<Vec<_>>().join("+")).unwrap_or_default()
    );

    // Process GitHub Archive files (main functionality)
    let gha_processor = GHAProcessor::new(pool, ctx);
    let client = Client::new();
    
    // Create date/hour iterator like Go version
    let mut current_date = from_date;
    let mut total_events = 0;
    let mut total_repos = 0;
    let mut total_actors = 0;

    while current_date <= to_date {
        let start_hour = if current_date == from_date { hour_from } else { 0 };
        let end_hour = if current_date == to_date { hour_to } else { 23 };
        
        for hour in start_hour..=end_hour {
            let gha_url = format!("https://data.gharchive.org/{}-{:02}.json.gz", 
                current_date.format("%Y-%m-%d"), hour);
                
            match process_gha_file(&client, &gha_url, &gha_processor, &orgs_filter, &repos_filter).await {
                Ok((events, repos, actors)) => {
                    total_events += events;
                    total_repos += repos; 
                    total_actors += actors;
                }
                Err(err) => {
                    eprintln!("Error processing {}: {}", gha_url, err);
                    // Continue processing other files (resilient like Go version)
                }
            }
        }
        
        current_date = current_date.succ_opt().unwrap();
    }

    let elapsed = start_time.elapsed();
    println!("Time: {:?}", elapsed);
    
    Ok(())
}

async fn process_gha_file(
    client: &Client,
    url: &str,
    processor: &GHAProcessor,
    orgs_filter: &Option<HashMap<String, ()>>,
    repos_filter: &Option<HashMap<String, ()>>
) -> Result<(usize, usize, usize)> {
    // Download and decompress GHA file
    let response = client.get(url).send().await?;
    let compressed_data = response.bytes().await?;
    
    // Decompress GZIP data
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(compressed_data.as_ref());
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    
    // Process JSON events line by line (like Go version)
    let mut events_count = 0;
    let mut repos_count = 0;
    let mut actors_count = 0;
    
    for line in decompressed.lines() {
        if line.trim().is_empty() {
            continue;
        }
        
        match serde_json::from_str::<Value>(line) {
            Ok(json_event) => {
                // Apply filters like Go version
                if let Some(repo_name) = json_event["repo"]["name"].as_str() {
                    // Check organization filter
                    if let Some(orgs) = orgs_filter {
                        let org_name = repo_name.split('/').next().unwrap_or("");
                        if !orgs.contains_key(org_name) {
                            continue;
                        }
                    }
                    
                    // Check repository filter  
                    if let Some(repos) = repos_filter {
                        if !repos.contains_key(repo_name) {
                            continue;
                        }
                    }
                }
                
                // Process event into database (like Go version)
                match processor.process_event(json_event).await {
                    Ok(_) => {
                        events_count += 1;
                        // Count unique repos and actors (simplified)
                        repos_count += 1;
                        actors_count += 1;
                    }
                    Err(err) => {
                        eprintln!("Error processing event: {}", err);
                        // Continue processing (resilient)
                    }
                }
            }
            Err(err) => {
                eprintln!("Error parsing JSON: {}", err);
                // Continue processing (resilient like Go version)
            }
        }
    }
    
    Ok((events_count, repos_count, actors_count))
}