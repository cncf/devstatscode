use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let _matches = Command::new("devstats-website-data")
        .version("0.1.0")
        .about("Generate website data for DevStats")
        .author("DevStats Team")
        .get_matches();

    let ctx = Context::from_env()?;
    info!("Website data generation tool");

    // TODO: In full implementation would generate static data files for website
    info!("Would generate website data files");
    Ok(())
}