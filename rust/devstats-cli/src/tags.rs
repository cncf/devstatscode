use clap::{Arg, Command};
use devstats_core::{Context, Result};
use tracing::{info, error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// TSDB tag configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub name: String,
    pub sql: String,
    #[serde(rename = "series_name")]
    pub series_name: String,
    #[serde(rename = "name_tag")]
    pub name_tag: String,
    #[serde(rename = "value_tag")]
    pub value_tag: String,
    #[serde(rename = "other_tags", default)]
    pub other_tags: HashMap<String, [String; 2]>,
    #[serde(default = "default_limit")]
    pub limit: i32,
    #[serde(default)]
    pub disabled: bool,
}

fn default_limit() -> i32 {
    127
}

/// Collection of TSDB tags
#[derive(Debug, Serialize, Deserialize)]
pub struct Tags {
    pub tags: Vec<Tag>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let matches = Command::new("devstats-tags")
        .version("0.1.0")
        .about("Insert TSDB tags for DevStats")
        .author("DevStats Team")
        .arg(
            Arg::new("project")
                .short('p')
                .long("project")
                .value_name("PROJECT")
                .help("Project name to process tags for")
        )
        .get_matches();

    // Initialize context from environment
    let mut ctx = Context::from_env()?;
    
    // Override project if provided via command line
    if let Some(project) = matches.get_one::<String>("project") {
        ctx.project = project.clone();
    }

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // Determine data prefix
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Construct tags YAML file path
    let tags_yaml_path = if ctx.tags_yaml.is_empty() {
        let mut path = format!("{}metrics/", data_prefix);
        if !ctx.project.is_empty() {
            path.push_str(&format!("{}/", ctx.project));
        }
        path.push_str("tags.yaml");
        path
    } else {
        format!("{}{}", data_prefix, ctx.tags_yaml)
    };

    info!("Reading tags configuration from: {}", tags_yaml_path);

    // Read and parse tags YAML file
    let tags_content = match tokio::fs::read_to_string(&tags_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read tags YAML file '{}': {}", tags_yaml_path, err);
            return Err(err.into());
        }
    };

    let all_tags: Tags = serde_yaml::from_str(&tags_content)?;

    info!("Found {} tag(s) to process", all_tags.tags.len());

    // Connect to PostgreSQL database
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => pool,
        Err(err) => {
            error!("Failed to connect to database: {}", err);
            return Err(err.into());
        }
    };

    info!("Connected to PostgreSQL database");

    // Process each tag
    for (i, tag) in all_tags.tags.iter().enumerate() {
        if tag.disabled {
            info!("Skipping disabled tag: {}", tag.name);
            continue;
        }

        info!("Processing tag {}/{}: {}", i + 1, all_tags.tags.len(), tag.name);

        if let Err(err) = process_tag(&pool, &ctx, tag, &data_prefix).await {
            error!("Failed to process tag '{}': {}", tag.name, err);
            // Continue with other tags instead of failing completely
        } else {
            info!("Successfully processed tag: {}", tag.name);
        }
    }

    info!("Tags processing completed");
    Ok(())
}

async fn calc_tags() -> Result<()> {
    // Environment context parse exactly like Go version
    let ctx = Context::from_env()?;
    
    // Skip TSDB processing if disabled
    if ctx.skip_tsdb {
        return Ok(());
    }

    // Connect to PostgreSQL database
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    
    let pool = match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => pool,
        Err(err) => {
            error!("Failed to connect to database: {}", err);
            return Err(err.into());
        }
    };

    // Determine data prefix - local or cron mode
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read tags to generate
    let tags_yaml_path = format!("{}{}", data_prefix, ctx.tags_yaml);
    let tags_content = match tokio::fs::read_to_string(&tags_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read tags YAML file '{}': {}", tags_yaml_path, err);
            return Err(err.into());
        }
    };

    let tags_config: Tags = match serde_yaml::from_str(&tags_content) {
        Ok(config) => config,
        Err(err) => {
            error!("Failed to parse tags YAML: {}", err);
            return Err(err.into());
        }
    };

    info!("Processing {} tags", tags_config.tags.len());

    // Process each tag configuration
    for (i, tag) in tags_config.tags.iter().enumerate() {
        if tag.disabled {
            info!("Skipping disabled tag: {}", tag.name);
            continue;
        }

        info!("Processing tag {}/{}: {}", i + 1, tags_config.tags.len(), tag.name);

        // Execute SQL query for this tag
        let sql_with_substitutions = substitute_sql_parameters(&tag.sql, &ctx);
        
        if ctx.q_out {
            info!("Executing SQL: {}", sql_with_substitutions);
        }

        match sqlx::query(&sql_with_substitutions).fetch_all(&pool).await {
            Ok(rows) => {
                info!("Tag '{}' query returned {} rows", tag.name, rows.len());
                
                // In full implementation, would insert TSDB data here
                // For now, just log the processing
                if ctx.debug > 0 {
                    for (row_idx, _row) in rows.iter().enumerate() {
                        if row_idx < 5 {  // Limit debug output
                            info!("  Row {}: processed", row_idx + 1);
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to execute SQL for tag '{}': {}", tag.name, err);
                return Err(err.into());
            }
        }
    }

    info!("Tags processing completed");
    Ok(())
}

fn substitute_sql_parameters(sql: &str, ctx: &Context) -> String {
    // Basic parameter substitution - in full implementation would handle all DevStats parameters
    sql.replace("{{from}}", &ctx.default_start_date.format("%Y-%m-%d").to_string())
       .replace("{{to}}", &chrono::Utc::now().format("%Y-%m-%d").to_string())
       .replace("{{period}}", "d")
}

async fn process_tag(
    pool: &sqlx::PgPool,
    ctx: &Context,
    tag: &Tag,
    data_prefix: &str,
) -> Result<()> {
    // Per project directory for SQL files
    let mut dir = "metrics/".to_string();
    if !ctx.project.is_empty() {
        dir = format!("metrics/{}/", ctx.project);
    }

    // Read SQL file
    let sql_file_path = format!("{}{}{}.sql", data_prefix, dir, tag.sql);
    let mut sql_query = match tokio::fs::read_to_string(&sql_file_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read SQL file '{}': {}", sql_file_path, err);
            return Err(err.into());
        }
    };

    // Handle excluding bots
    let exclude_bots_path = format!("{}util_sql/exclude_bots.sql", data_prefix);
    let exclude_bots = match tokio::fs::read_to_string(&exclude_bots_path).await {
        Ok(content) => content,
        Err(_) => {
            // If exclude_bots.sql doesn't exist, use empty string
            String::new()
        }
    };

    // Transform SQL
    let limit = if tag.limit <= 0 { 127 } else { tag.limit };
    sql_query = sql_query.replace("{{lim}}", &limit.to_string());
    sql_query = sql_query.replace("{{exclude_bots}}", &exclude_bots);

    if ctx.q_out {
        info!("Executing SQL for tag '{}': {}", tag.name, sql_query);
    }

    // Execute SQL query (simplified version - in full implementation would handle TS points)
    match sqlx::query(&sql_query).fetch_all(pool).await {
        Ok(rows) => {
            info!("Tag '{}': processed {} rows", tag.name, rows.len());
            
            // Drop current tags table if not skipping TSDB
            if !ctx.skip_tsdb {
                let table = format!("t{}", tag.series_name);
                let truncate_sql = format!("TRUNCATE TABLE IF EXISTS {}", table);
                
                if let Err(err) = sqlx::query(&truncate_sql).execute(pool).await {
                    // This might fail if table doesn't exist, which is fine
                    if ctx.debug > 0 {
                        info!("Note: failed to truncate table '{}': {}", table, err);
                    }
                }
            }
            
            // TODO: In a full implementation, would process each row and create TS points
            info!("Tag '{}' processing would continue with TS point creation", tag.name);
        }
        Err(err) => {
            error!("Failed to execute SQL for tag '{}': {}", tag.name, err);
            return Err(err.into());
        }
    }

    Ok(())
}
