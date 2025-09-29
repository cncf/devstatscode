use clap::Command;
use devstats_core::{Context, Result};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let _matches = Command::new("devstats-get-repos")
        .version("0.1.0")
        .about("Get and process repositories for DevStats")
        .author("DevStats Team")
        .get_matches();

    let ctx = Context::from_env()?;
    info!("Repository processing tool for project: {}", ctx.project);

    // TODO: In full implementation would clone/update all project repositories
    info!("Would process repositories from projects configuration");
    Ok(())
}