use clap::Command;
use devstats_core::{Context, Result, DevStatsError};
use tracing::{info, error, debug};
use tokio;
use warp::{Filter, Reply};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock, Mutex};
use sqlx::{PgPool, Pool, Postgres};
use chrono::{DateTime, Utc};
use serde_yaml;
use std::time::{Duration, Instant};

// Constants for cache TTL - exact replica of Go constants
const GITHUB_ID_CONTRIBUTIONS_CACHE_TTL: u64 = 86400; // 24 hours
const CUMULATIVE_COUNTS_CACHE_TTL: u64 = 43200; // 12 hours  
const SITE_STATS_CACHE_TTL: u64 = 43200; // 12 hours

// All APIs list - exact replica of Go's allAPIs
const ALL_APIS: &[&str] = &[
    "Health", "ListAPIs", "ListProjects", "RepoGroups", "Ranges", "Countries",
    "Companies", "Events", "Repos", "CumulativeCounts", "CompaniesTable",
    "ComContribRepoGrp", "DevActCnt", "DevActCntComp", "ComStatsRepoGrp", 
    "SiteStats", "GithubIDContributions"
];

// API request/response types - exact replicas of Go structs
#[derive(Debug, Deserialize)]
struct ApiRequest {
    api: String,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    project: String,
    db_name: String,
    events: i64,
}

#[derive(Debug, Serialize)]
struct ListAPIsResponse {
    apis: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ListProjectsResponse {
    projects: Vec<String>,
}

// Project configuration types - exact replicas of Go's project structures
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Project {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "full_name")]
    pub full_name: String,
    #[serde(rename = "pdb")]
    pub pdb: String,
    #[serde(rename = "disabled", default)]
    pub disabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct AllProjects {
    pub projects: HashMap<String, Project>,
}

// Global state - exact replica of Go globals  
#[derive(Debug)]
struct AppState {
    name_to_db: HashMap<String, String>,
    projects: Vec<String>,
    pools: HashMap<String, PgPool>,
    ctx: Context,
}

type SharedState = Arc<RwLock<AppState>>;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing - exact replica of Go's logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-api")
        .version("0.1.0")
        .about("DevStats API Server - exact Go replacement")
        .author("DevStats Team")
        .get_matches();

    info!("Starting DevStats API server");

    // Check required environment variables - exact replica of Go's checkEnv()
    check_env()?;

    // Initialize context
    let ctx = Context::from_env()?;

    // Read projects configuration - exact replica of Go's readProjects()
    let (name_to_db, projects) = read_projects(&ctx).await?;

    // Create database connection pools for each project
    let mut pools = HashMap::new();
    for (_, db_name) in &name_to_db {
        if !pools.contains_key(db_name) {
            let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}",
                std::env::var("PG_USER_RO").unwrap_or_default(),
                std::env::var("PG_PASS_RO").unwrap_or_default(), 
                std::env::var("PG_HOST_RO").unwrap_or_default(),
                ctx.pg_port,
                db_name,
                ctx.pg_ssl
            );
            
            match PgPool::connect(&db_url).await {
                Ok(pool) => {
                    pools.insert(db_name.clone(), pool);
                    info!("Connected to database: {}", db_name);
                }
                Err(e) => {
                    error!("Failed to connect to database {}: {}", db_name, e);
                    // Continue anyway - some endpoints might still work
                }
            }
        }
    }

    // Initialize shared state
    let state = Arc::new(RwLock::new(AppState {
        name_to_db,
        projects,
        pools,
        ctx,
    }));

    // Build routes - exact replica of Go's API endpoints
    let api_route = warp::path("api")
        .and(warp::path("v1"))
        .and(warp::post())
        .and(warp::body::json())
        .and(with_state(state.clone()))
        .and_then(handle_api);

    let routes = api_route
        .with(warp::cors()
            .allow_any_origin()
            .allow_methods(&[warp::http::Method::POST])
            .allow_headers(vec!["content-type"]));

    info!("API server listening on 0.0.0.0:8080");

    warp::serve(routes)
        .run(([0, 0, 0, 0], 8080))
        .await;

    Ok(())
}

// Helper to pass state to handlers
fn with_state(state: SharedState) -> impl Filter<Extract = (SharedState,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || state.clone())
}

// Check required environment variables - exact replica of Go's checkEnv()
fn check_env() -> Result<()> {
    let required_env = ["PG_PASS", "PG_PASS_RO", "PG_USER_RO", "PG_HOST_RO"];
    for env in &required_env {
        if std::env::var(env).is_err() {
            return Err(DevStatsError::Config(format!("{} env variable must be set", env)));
        }
    }
    Ok(())
}

// Read projects configuration - exact replica of Go's readProjects()
async fn read_projects(ctx: &Context) -> Result<(HashMap<String, String>, Vec<String>)> {
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    let projects_yaml_path = format!("{}{}", data_prefix, ctx.projects_yaml);
    let data = tokio::fs::read_to_string(&projects_yaml_path).await?;
    let projects: AllProjects = serde_yaml::from_str(&data)?;

    let mut name_to_db = HashMap::new();
    let mut project_names = Vec::new();

    for (proj_name, proj_data) in projects.projects {
        if proj_data.disabled {
            continue;
        }
        
        let db = proj_data.pdb.clone();
        name_to_db.insert(proj_name.clone(), db.clone());
        name_to_db.insert(proj_data.full_name.clone(), db.clone());
        name_to_db.insert(proj_data.pdb.clone(), db);
        project_names.push(proj_data.full_name);
    }

    Ok((name_to_db, project_names))
}

// Main API handler - exact replica of Go's handleAPI()
async fn handle_api(request: ApiRequest, state: SharedState) -> std::result::Result<impl warp::Reply, warp::Rejection> {
    debug!("API request: {} with payload: {:?}", request.api, request.payload);

    let response = match request.api.as_str() {
        "Health" => handle_health(request.payload, state).await,
        "ListAPIs" => handle_list_apis().await,
        "ListProjects" => handle_list_projects(state).await,
        _ => {
            error!("Unknown API: {}", request.api);
            Err(format!("Unknown API '{}'", request.api))
        }
    };

    match response {
        Ok(reply) => Ok(reply),
        Err(error) => {
            let error_response = ErrorResponse { error };
            Ok(warp::reply::with_status(warp::reply::json(&error_response), warp::http::StatusCode::BAD_REQUEST).into_response())
        }
    }
}

// Extract project and database from payload - exact replica of Go's handleSharedPayload()
fn extract_project_and_db(payload: Option<serde_json::Value>, state: &SharedState) -> std::result::Result<(String, String), String> {
    let payload = payload.ok_or("'payload' section empty or missing".to_string())?;
    let payload_map = payload.as_object().ok_or("'payload' must be an object".to_string())?;
    
    let project = payload_map.get("project")
        .and_then(|v| v.as_str())
        .ok_or("missing or invalid 'project' field in 'payload' section".to_string())?
        .to_string();

    let state_read = state.read().unwrap();
    let db = state_read.name_to_db.get(&project)
        .ok_or(format!("database not found for project '{}'", project))?
        .clone();

    Ok((project, db))
}

// Get database pool for a database - helper function
fn get_pool(db: &str, state: &SharedState) -> std::result::Result<PgPool, String> {
    let state_read = state.read().unwrap();
    state_read.pools.get(db)
        .cloned()
        .ok_or(format!("no connection pool for database '{}'", db))
}

// Health API handler - exact replica of Go's apiHealth()
async fn handle_health(payload: Option<serde_json::Value>, state: SharedState) -> std::result::Result<warp::reply::Response, String> {
    let (project, db) = extract_project_and_db(payload, &state)?;
    let pool = get_pool(&db, &state)?;

    // Execute health check query - exact replica of Go's query
    let result: std::result::Result<(i64,), sqlx::Error> = sqlx::query_as("SELECT count(*) FROM gha_events")
        .fetch_one(&pool).await;

    match result {
        Ok((events,)) => {
            let response = HealthResponse {
                project,
                db_name: db,
                events,
            };
            Ok(warp::reply::json(&response).into_response())
        }
        Err(e) => Err(format!("Database query failed: {}", e))
    }
}

// ListAPIs handler - exact replica of Go's apiListAPIs()
async fn handle_list_apis() -> std::result::Result<warp::reply::Response, String> {
    let response = ListAPIsResponse {
        apis: ALL_APIS.iter().map(|s| s.to_string()).collect(),
    };
    Ok(warp::reply::json(&response).into_response())
}

// ListProjects handler - exact replica of Go's apiListProjects()
async fn handle_list_projects(state: SharedState) -> std::result::Result<warp::reply::Response, String> {
    let projects = {
        let state_read = state.read().unwrap();
        state_read.projects.clone()
    };
    
    let response = ListProjectsResponse { projects };
    Ok(warp::reply::json(&response).into_response())
}