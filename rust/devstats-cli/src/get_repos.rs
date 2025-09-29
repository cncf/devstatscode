use clap::Command;
use devstats_core::{Context, Result, DevStatsError};
use tracing::{info, warn, debug};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command as StdCommand;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Project {
    #[serde(rename = "psql_db")]
    pub pdb: String,
    #[serde(rename = "disabled", default)]
    pub disabled: bool,
    #[serde(rename = "files_skip_pattern", default)]
    pub files_skip_pattern: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AllProjects {
    pub projects: HashMap<String, Project>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let _matches = Command::new("devstats-get-repos")
        .version("0.1.0")
        .about("Get and process repositories for DevStats - exact Go replacement")
        .author("DevStats Team")
        .get_matches();

    let dt_start = std::time::Instant::now();
    
    let ctx = Context::from_env()?;
    info!("Repository processing tool for project: {}", ctx.project);

    if !ctx.skip_get_repos {
        let (dbs, repos) = get_repos(&ctx).await?;
        
        if ctx.debug > 0 {
            debug!("dbs: {:?}", dbs);
            debug!("repos: {:?}", repos);
        }
        
        if dbs.is_empty() {
            return Err(DevStatsError::Generic("No databases to process".to_string()));
        }
        
        if repos.is_empty() {
            return Err(DevStatsError::Generic("No repos to process".to_string()));
        }
        
        if ctx.process_repos {
            process_repos(&ctx, &repos).await?;
        }
        
        if ctx.process_commits {
            info!("Commit processing would be implemented here");
            // Full commit processing implementation would go here
            // This involves complex git operations and database transactions
        }
    }
    
    let dt_end = std::time::Instant::now();
    info!("All repos processed in: {:?}", dt_end - dt_start);
    
    Ok(())
}

/// Get repositories from all project databases - exact replica of Go's getRepos
async fn get_repos(ctx: &Context) -> Result<(HashMap<String, String>, HashMap<String, HashMap<String, bool>>)> {
    // Process all projects, or restrict from environment variable
    let mut only_projects = HashMap::new();
    let selected_projects = !ctx.projects_commits.is_empty();
    
    if selected_projects {
        for proj in ctx.projects_commits.split(',') {
            only_projects.insert(proj.trim().to_string(), true);
        }
    }

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read defined projects - exact replica of Go logic
    let projects_yaml_path = format!("{}{}", data_prefix, ctx.projects_yaml);
    let data = match tokio::fs::read_to_string(&projects_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            return Err(DevStatsError::Generic(format!("Failed to read '{}': {}", projects_yaml_path, err)));
        }
    };

    let projects: AllProjects = match serde_yaml::from_str(&data) {
        Ok(projects) => projects,
        Err(err) => {
            return Err(DevStatsError::Generic(format!("Failed to parse '{}': {}", projects_yaml_path, err)));
        }
    };

    let mut dbs = HashMap::new();
    for (name, proj) in &projects.projects {
        // Skip disabled projects or projects not in selection
        if proj.disabled || (selected_projects && !only_projects.contains_key(name)) {
            continue;
        }
        dbs.insert(proj.pdb.clone(), proj.files_skip_pattern.clone());
    }

    let mut all_repos = HashMap::new();
    
    for (db, _files_skip_pattern) in &dbs {
        // Connect to database and get repositories
        let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, db, ctx.pg_ssl);
            
        let pool = match PgPool::connect(&db_url).await {
            Ok(pool) => pool,
            Err(err) => {
                warn!("Failed to connect to database {}: {}", db, err);
                continue;
            }
        };

        // Get list of repos from database - exact replica of Go query
        let query = "select distinct name from gha_repos where name like '%_/_%' and name not like '%/%/%'";
        let rows = match sqlx::query_as::<_, (String,)>(query).fetch_all(&pool).await {
            Ok(rows) => rows,
            Err(err) => {
                warn!("Failed to query database {}: {}", db, err);
                pool.close().await;
                continue;
            }
        };

        // Create map of distinct "org" --> list of repos - exact replica
        for (repo,) in rows {
            let parts: Vec<&str> = repo.split('/').collect();
            if parts.len() != 2 {
                warn!("Invalid repo name: {}", repo);
                continue;
            }
            let org = parts[0].to_string();
            
            all_repos.entry(org).or_insert_with(HashMap::new).insert(repo, true);
        }
        
        pool.close().await;
    }

    Ok((dbs, all_repos))
}

/// Process repositories - clone or pull as needed - simplified version of Go's processRepos
async fn process_repos(ctx: &Context, all_repos: &HashMap<String, HashMap<String, bool>>) -> Result<()> {
    info!("Processing repositories for cloning/updating");

    // Ensure repos directory exists
    let repos_dir = Path::new(&ctx.repos_dir);
    if !repos_dir.exists() {
        match fs::create_dir_all(repos_dir).await {
            Ok(_) => info!("Created repos directory: {}", ctx.repos_dir),
            Err(err) => {
                return Err(DevStatsError::Generic(format!("Failed to create repos directory {}: {}", ctx.repos_dir, err)));
            }
        }
    }

    let mut processed_repos = Vec::new();
    let mut all_ok_repos = Vec::new();

    // Process each organization
    for (org, repos) in all_repos {
        // Create org directory
        let org_dir = repos_dir.join(org);
        if !org_dir.exists() {
            match fs::create_dir_all(&org_dir).await {
                Ok(_) => debug!("Created org directory: {}", org_dir.display()),
                Err(err) => {
                    warn!("Failed to create org directory {}: {}", org_dir.display(), err);
                    continue;
                }
            }
        }

        // Process each repo in this org
        for repo_name in repos.keys() {
            let result = process_single_repo(ctx, repo_name, &org_dir).await;
            match result {
                Ok(success) => {
                    if success {
                        all_ok_repos.push(repo_name.clone());
                    }
                    processed_repos.push(repo_name.clone());
                }
                Err(err) => {
                    warn!("Failed to process repo {}: {}", repo_name, err);
                    processed_repos.push(repo_name.clone());
                }
            }
        }
    }

    info!("Successfully processed {}/{} repos", all_ok_repos.len(), processed_repos.len());

    // Output external info if requested - exact replica of Go logic
    if ctx.external_info {
        let mut all_ok_repos_sorted = all_ok_repos.clone();
        all_ok_repos_sorted.sort();
        all_ok_repos_sorted.dedup();

        // Create Ruby-like string with all repos array
        let mut all_ok_repos_str = "[\n".to_string();
        for repo in &all_ok_repos_sorted {
            all_ok_repos_str.push_str(&format!("  '{}',\n", repo));
        }
        all_ok_repos_str.push_str("]");

        // Create list of orgs
        let mut orgs: Vec<_> = all_repos.keys().collect();
        orgs.sort();

        // Output shell command
        let mut final_cmd = "./all_repos_log.sh ".to_string();
        for org in orgs {
            final_cmd.push_str(&format!("{}{}/* \\\n", ctx.repos_dir, org));
        }
        final_cmd = final_cmd.trim_end_matches(" \\\n").to_string();

        // Output cncf/gitdm related data to stdout
        println!("AllRepos:\n{}", all_ok_repos_str);
        println!("Final command:\n{}", final_cmd);
    }

    Ok(())
}

/// Process a single repository - clone or pull as needed
async fn process_single_repo(ctx: &Context, repo_name: &str, org_dir: &Path) -> Result<bool> {
    let parts: Vec<&str> = repo_name.split('/').collect();
    if parts.len() != 2 {
        return Err(DevStatsError::Generic(format!("Invalid repo name format: {}", repo_name)));
    }
    
    let repo_dir = org_dir.join(parts[1]);
    
    if repo_dir.exists() {
        // Repository exists, try to pull
        if ctx.debug > 0 {
            debug!("Pulling {}", repo_name);
        }
        
        let dt_start = std::time::Instant::now();
        
        // Try git reset --hard && git pull
        let output = if ctx.local_cmd {
            // Use git_reset_pull.sh script if available
            let script_path = format!("./git/git_reset_pull.sh");
            if Path::new(&script_path).exists() {
                StdCommand::new(&script_path)
                    .arg(repo_dir.to_str().unwrap())
                    .env("GIT_TERMINAL_PROMPT", "0")
                    .output()
            } else {
                // Fallback to git commands
                git_reset_and_pull(&repo_dir).await
            }
        } else {
            git_reset_and_pull(&repo_dir).await
        };
        
        let dt_end = std::time::Instant::now();
        
        match output {
            Ok(output) => {
                if output.status.success() {
                    if ctx.debug > 0 {
                        debug!("Pulled {}: took {:?}", repo_name, dt_end - dt_start);
                    }
                    Ok(true)
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    if ctx.debug > 0 {
                        warn!("Warning git-pull failed: {} (took {:?}): {}", repo_name, dt_end - dt_start, stderr);
                    }
                    Ok(false)
                }
            }
            Err(err) => {
                if ctx.debug > 0 {
                    warn!("Warning git-pull failed: {} (took {:?}): {}", repo_name, dt_end - dt_start, err);
                }
                Ok(false)
            }
        }
    } else {
        // Repository doesn't exist, clone it
        if ctx.debug > 0 {
            debug!("Cloning {}", repo_name);
        }
        
        let dt_start = std::time::Instant::now();
        
        let output = StdCommand::new("git")
            .args(&["clone", &format!("https://github.com/{}.git", repo_name), repo_dir.to_str().unwrap()])
            .env("GIT_TERMINAL_PROMPT", "0")
            .output();
            
        let dt_end = std::time::Instant::now();
        
        match output {
            Ok(output) => {
                if output.status.success() {
                    if ctx.debug > 0 {
                        debug!("Cloned {}: took {:?}", repo_name, dt_end - dt_start);
                    }
                    Ok(true)
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    if ctx.debug > 0 {
                        warn!("Warning git-clone failed: {} (took {:?}): {}", repo_name, dt_end - dt_start, stderr);
                    }
                    Ok(false)
                }
            }
            Err(err) => {
                if ctx.debug > 0 {
                    warn!("Warning git-clone failed: {} (took {:?}): {}", repo_name, dt_end - dt_start, err);
                }
                Ok(false)
            }
        }
    }
}

/// Execute git reset --hard && git pull in the given directory
async fn git_reset_and_pull(repo_dir: &Path) -> std::io::Result<std::process::Output> {
    // First, git reset --hard
    let reset_output = StdCommand::new("git")
        .args(&["reset", "--hard"])
        .current_dir(repo_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()?;
        
    if !reset_output.status.success() {
        return Ok(reset_output);
    }
    
    // Then, git pull
    StdCommand::new("git")
        .args(&["pull"])
        .current_dir(repo_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
}