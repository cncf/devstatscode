use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};
use serde::{Deserialize, Serialize};

/// Annotation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotation {
    pub name: String,
    pub description: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

/// Collection of annotations
#[derive(Debug, Serialize, Deserialize)]
pub struct Annotations {
    pub annotations: Vec<Annotation>,
}

/// Project configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    #[serde(rename = "main_repo", default)]
    pub main_repo: String,
    #[serde(rename = "annotation_regexp", default)]
    pub annotation_regexp: String,
    #[serde(rename = "start_date")]
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "join_date")]
    pub join_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "incubating_date")]
    pub incubating_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "graduated_date")]
    pub graduated_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "archived_date")]
    pub archived_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "shared_db", default)]
    pub shared_db: bool,
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

    let _matches = Command::new("devstats-annotations")
        .version("0.1.0")
        .about("Insert TSDB annotations for DevStats")
        .author("DevStats Team")
        .get_matches();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // Needs GHA2DB_PROJECT variable set
    if ctx.project.is_empty() {
        error!("You have to set project via GHA2DB_PROJECT environment variable");
        std::process::exit(1);
    }

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read defined projects
    let projects_yaml_path = format!("{}{}", data_prefix, ctx.projects_yaml);
    let projects_content = match tokio::fs::read_to_string(&projects_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read projects YAML file '{}': {}", projects_yaml_path, err);
            return Err(err.into());
        }
    };

    let all_projects: AllProjects = serde_yaml::from_str(&projects_content)?;

    // Get current project's main repo and annotation regexp
    let project = match all_projects.projects.get(&ctx.project) {
        Some(proj) => proj,
        None => {
            error!("Project '{}' not found in '{}'", ctx.project, ctx.projects_yaml);
            std::process::exit(1);
        }
    };

    info!("Processing annotations for project: {}", ctx.project);
    
    if !project.main_repo.is_empty() {
        info!("Main repository: {}", project.main_repo);
        info!("Annotation regexp: {}", project.annotation_regexp);
        
        // TODO: In full implementation, would call GitHub API to get annotations
        // For now, just process static dates
        let mut annotations = Vec::new();
        
        if let Some(start_date) = project.start_date {
            annotations.push(Annotation {
                name: "Project start".to_string(),
                description: format!("{} - project starts", start_date.format("%Y-%m-%d")),
                date: start_date,
            });
        }
        
        if let Some(join_date) = project.join_date {
            annotations.push(Annotation {
                name: "Project join".to_string(),
                description: format!("{} - project joins foundation", join_date.format("%Y-%m-%d")),
                date: join_date,
            });
        }
        
        if let Some(incubating_date) = project.incubating_date {
            annotations.push(Annotation {
                name: "Incubating".to_string(),
                description: format!("{} - project becomes incubating", incubating_date.format("%Y-%m-%d")),
                date: incubating_date,
            });
        }
        
        if let Some(graduated_date) = project.graduated_date {
            annotations.push(Annotation {
                name: "Graduated".to_string(),
                description: format!("{} - project graduates", graduated_date.format("%Y-%m-%d")),
                date: graduated_date,
            });
        }
        
        if let Some(archived_date) = project.archived_date {
            annotations.push(Annotation {
                name: "Archived".to_string(),
                description: format!("{} - project archived", archived_date.format("%Y-%m-%d")),
                date: archived_date,
            });
        }
        
        info!("Found {} annotations to process", annotations.len());
        
        // TODO: In full implementation, would insert annotations into TSDB
        for annotation in &annotations {
            info!("Annotation: {} - {} ({})", 
                annotation.name, 
                annotation.description, 
                annotation.date.format("%Y-%m-%d %H:%M:%S UTC")
            );
        }
        
    } else if let Some(start_date) = project.start_date {
        info!("No main repo specified, using start date: {}", start_date.format("%Y-%m-%d"));
        
        // Create minimal annotation for projects without main repo
        let annotation = Annotation {
            name: "Project start".to_string(),
            description: format!("{} - project starts", start_date.format("%Y-%m-%d")),
            date: start_date,
        };
        
        info!("Annotation: {} - {} ({})", 
            annotation.name, 
            annotation.description, 
            annotation.date.format("%Y-%m-%d %H:%M:%S UTC")
        );
    } else {
        error!("Project '{}' has no main_repo and no start_date specified", ctx.project);
        std::process::exit(1);
    }

    info!("Annotations processing completed");
    Ok(())
}