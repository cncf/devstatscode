use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-splitcrons")
        .version("0.1.0")
        .about("Split cron jobs for DevStats")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("Cron splitting tool");

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read cron configuration (if it exists)
    let cron_file_path = format!("{}cron/crontab", data_prefix);
    
    let cron_content = match tokio::fs::read_to_string(&cron_file_path).await {
        Ok(content) => {
            info!("Loaded cron config from: {}", cron_file_path);
            content
        }
        Err(_) => {
            info!("No cron file found at: {}, creating example", cron_file_path);
            "# Example cron entries\n# 0 * * * * /path/to/sync\n".to_string()
        }
    };

    // Parse cron entries
    let lines: Vec<&str> = cron_content
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
        .collect();

    info!("Found {} cron entries to process", lines.len());

    if lines.is_empty() {
        info!("No cron entries found, nothing to split");
        return Ok(());
    }

    // Split cron entries into multiple files based on frequency or other criteria
    let mut hourly_entries = Vec::new();
    let mut daily_entries = Vec::new();
    let mut other_entries = Vec::new();

    for line in lines {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 5 {
            let minute = parts[0];
            let hour = parts[1];
            
            if minute.contains('*') && !hour.contains('*') {
                hourly_entries.push(line);
            } else if hour.contains('*') && minute != "*" {
                daily_entries.push(line);
            } else {
                other_entries.push(line);
            }
        } else {
            other_entries.push(line);
        }
    }

    // Write split cron files
    if !hourly_entries.is_empty() {
        let hourly_file = format!("{}cron/hourly.cron", data_prefix);
        let hourly_content = format!("# Hourly cron entries\n{}\n", hourly_entries.join("\n"));
        
        if let Err(err) = tokio::fs::write(&hourly_file, hourly_content).await {
            error!("Failed to write hourly cron file '{}': {}", hourly_file, err);
        } else {
            info!("Written {} hourly entries to {}", hourly_entries.len(), hourly_file);
        }
    }

    if !daily_entries.is_empty() {
        let daily_file = format!("{}cron/daily.cron", data_prefix);
        let daily_content = format!("# Daily cron entries\n{}\n", daily_entries.join("\n"));
        
        if let Err(err) = tokio::fs::write(&daily_file, daily_content).await {
            error!("Failed to write daily cron file '{}': {}", daily_file, err);
        } else {
            info!("Written {} daily entries to {}", daily_entries.len(), daily_file);
        }
    }

    if !other_entries.is_empty() {
        let other_file = format!("{}cron/other.cron", data_prefix);
        let other_content = format!("# Other cron entries\n{}\n", other_entries.join("\n"));
        
        if let Err(err) = tokio::fs::write(&other_file, other_content).await {
            error!("Failed to write other cron file '{}': {}", other_file, err);
        } else {
            info!("Written {} other entries to {}", other_entries.len(), other_file);
        }
    }

    info!("Cron splitting completed");
    Ok(())
}