use clap::Command;
use devstats_core::{Context, Result};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let _matches = Command::new("devstats-import-affs")
        .version("0.1.0")
        .about("Import developer affiliations")
        .author("DevStats Team")
        .get_matches();

    let ctx = Context::from_env()?;
    info!("Affiliations import tool");

    // TODO: In full implementation would import developer affiliations from JSON files
    info!("Would import affiliations from: {}", ctx.affiliations_json);
    Ok(())
}