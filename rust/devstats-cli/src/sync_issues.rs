use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let _matches = Command::new("devstats-sync-issues")
        .version("0.1.0")
        .about("Sync GitHub issues data")
        .author("DevStats Team")
        .get_matches();

    let ctx = Context::from_env()?;
    info!("Issues sync tool for project: {}", ctx.project);

    // TODO: In full implementation would sync issues from GitHub API
    info!("Would sync issues from GitHub API to database");
    Ok(())
}