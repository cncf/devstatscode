use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};
use serde::{Deserialize, Serialize};

/// Project configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    #[serde(rename = "shared_db", default)]
    pub shared_db: bool,
    #[serde(default)]
    pub disabled: bool,
    #[serde(rename = "order", default)]
    pub order: i32,
}

/// All projects configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct AllProjects {
    pub projects: std::collections::HashMap<String, Project>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-devstats")
        .version("0.1.0")
        .about("Sync all DevStats projects")
        .author("DevStats Team")
        .get_matches();

    let start_time = std::time::Instant::now();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // Determine data prefix and command prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    let cmd_prefix = if ctx.local_cmd {
        "./".to_string()
    } else {
        String::new()
    };

    // Read projects configuration
    let projects_yaml_path = format!("{}{}", data_prefix, ctx.projects_yaml);
    info!("Reading projects from: {}", projects_yaml_path);

    let projects_content = match tokio::fs::read_to_string(&projects_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read projects YAML file '{}': {}", projects_yaml_path, err);
            return Err(err.into());
        }
    };

    let all_projects: AllProjects = serde_yaml::from_str(&projects_content)?;

    // Filter and sort projects
    let mut enabled_projects: Vec<_> = all_projects.projects
        .iter()
        .filter(|(_, project)| !project.disabled)
        .collect();

    // Sort by order field
    enabled_projects.sort_by_key(|(_, project)| project.order);

    info!("Found {} enabled projects to sync", enabled_projects.len());

    // Process each project
    let mut success_count = 0;
    let mut failed_projects = Vec::new();

    for (project_name, project) in enabled_projects {
        info!("Processing project: {} (shared_db: {})", project_name, project.shared_db);

        // TODO: In full implementation, would call gha2db_sync for each project
        // For now, simulate the sync process
        match sync_project(&ctx, project_name, project, &cmd_prefix).await {
            Ok(_) => {
                info!("✓ Successfully synced project: {}", project_name);
                success_count += 1;
            }
            Err(err) => {
                error!("✗ Failed to sync project '{}': {}", project_name, err);
                failed_projects.push(project_name.clone());
            }
        }
    }

    let elapsed = start_time.elapsed();

    info!("DevStats sync completed in {:?}", elapsed);
    info!("Successfully synced: {} projects", success_count);
    
    if !failed_projects.is_empty() {
        error!("Failed to sync: {} projects: {:?}", failed_projects.len(), failed_projects);
        std::process::exit(1);
    }

    Ok(())
}

async fn sync_project(
    _ctx: &Context,
    project_name: &str,
    project: &Project,
    cmd_prefix: &str,
) -> Result<()> {
    // In the real implementation, this would execute:
    // - gha2db_sync for the project
    // - Handle shared databases appropriately
    // - Manage concurrent execution
    
    info!("Syncing project '{}' with command prefix '{}'", project_name, cmd_prefix);
    
    // Simulate sync work
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // TODO: Replace with actual gha2db_sync execution:
    // let sync_cmd = format!("{}gha2db_sync", cmd_prefix);
    // Execute the command with appropriate environment variables
    
    Ok(())
}