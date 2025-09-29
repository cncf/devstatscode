use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-vars")
        .version("0.1.0")
        .about("Process template variables for DevStats")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read vars config
    let vars_yaml_path = if ctx.vars_yaml.is_empty() {
        let mut path = format!("{}metrics/", data_prefix);
        if !ctx.project.is_empty() {
            path.push_str(&format!("{}/", ctx.project));
        }
        path.push_str(&ctx.vars_fn_yaml);
        path
    } else {
        ctx.vars_yaml.clone()
    };

    info!("Reading vars configuration from: {}", vars_yaml_path);

    // Read vars YAML file (if it exists)
    let vars_content = match tokio::fs::read_to_string(&vars_yaml_path).await {
        Ok(content) => {
            info!("Loaded vars from: {}", vars_yaml_path);
            content
        }
        Err(_) => {
            info!("No vars file found at: {}, using defaults", vars_yaml_path);
            "{}".to_string()
        }
    };

    // Parse YAML (simple key-value mapping)
    let vars: serde_yaml::Value = match serde_yaml::from_str(&vars_content) {
        Ok(vars) => vars,
        Err(err) => {
            error!("Failed to parse vars YAML: {}", err);
            return Err(err.into());
        }
    };

    info!("Variables processing:");

    // Display all variables
    if let serde_yaml::Value::Mapping(map) = vars {
        if map.is_empty() {
            info!("No variables defined");
        } else {
            for (key, value) in map {
                let key_str = key.as_str().unwrap_or("<invalid key>");
                let value_str = match value {
                    serde_yaml::Value::String(s) => s.clone(),
                    _ => format!("{:?}", value),
                };
                info!("  {}: {}", key_str, value_str);
            }
        }
    } else {
        info!("Variables file is not a mapping/dictionary");
    }

    // TODO: In full implementation, would:
    // 1. Process template files using these variables
    // 2. Generate SQL files with variable substitution
    // 3. Update database with computed metrics

    info!("Variables processing completed");
    Ok(())
}