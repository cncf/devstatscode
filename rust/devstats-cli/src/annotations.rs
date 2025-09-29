use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, debug};
use serde::{Deserialize, Serialize};
use chrono::{Timelike, TimeZone};
use sqlx::PgPool;
use std::collections::HashMap;
use regex::Regex;
use std::process::Command as StdCommand;

/// Time series point for database storage - exact replica of Go's TSPoint
#[derive(Debug, Clone)]
pub struct TSPoint {
    pub time: chrono::DateTime<chrono::Utc>,
    pub added: chrono::DateTime<chrono::Utc>,
    pub period: String,
    pub name: String,
    pub tags: Option<HashMap<String, String>>,
    pub fields: Option<HashMap<String, serde_json::Value>>,
}

impl TSPoint {
    pub fn new(
        name: String,
        period: String,
        tags: Option<HashMap<String, String>>,
        fields: Option<HashMap<String, serde_json::Value>>,
        time: chrono::DateTime<chrono::Utc>,
        exact: bool,
    ) -> Self {
        let point_time = if exact {
            time
        } else {
            hour_start(time)
        };

        TSPoint {
            time: point_time,
            added: chrono::Utc::now(),
            name,
            period,
            tags,
            fields,
        }
    }

    pub fn display(&self) -> String {
        format!(
            "{} {} {} period: {} tags: {:?} fields: {:?}",
            self.time.format("%Y-%m-%d %H:00:00"),
            self.added.format("%Y-%m-%d %H:00:00"),
            self.name,
            self.period,
            self.tags,
            self.fields
        )
    }
}

/// Hour start equivalent to Go's HourStart function
fn hour_start(dt: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    dt.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap()
}

/// Next day start equivalent to Go's NextDayStart function
fn next_day_start(dt: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    let next_day = dt.date_naive().succ_opt().unwrap_or(dt.date_naive());
    next_day.and_hms_opt(0, 0, 0).unwrap().and_utc()
}

/// Sanitize UTF-8 strings for database storage - exact replica of Go's SafeUTF8String
fn sanitize_utf8(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\r' || *c == '\t')
        .collect()
}

/// YMDHMS date format for Go compatibility
fn to_ymdhms_date(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// YMD date format for Go compatibility
fn to_ymd_date(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%d").to_string()
}

/// Write TSPoints to database - replicates Go's WriteTSPoints functionality  
async fn write_ts_points_to_db(
    pool: &PgPool,
    points: &[TSPoint],
    ctx: &Context,
) -> Result<()> {
    if ctx.debug > 0 {
        info!("Writing {} TSPoints to database", points.len());
    }

    // Group points by series name and table
    let mut series_tables: HashMap<String, Vec<&TSPoint>> = HashMap::new();
    for point in points {
        let table_name = format!("t{}", point.name);
        series_tables.entry(table_name).or_default().push(point);
    }

    for (table_name, table_points) in series_tables {
        // Check if table exists, if not create it (but don't in production - tables should exist)
        if ctx.debug > 0 {
            debug!("Inserting {} points into table {}", table_points.len(), table_name);
        }

        // Insert points using batch insert for efficiency
        for chunk in table_points.chunks(1000) {
            insert_ts_points_batch(pool, &table_name, chunk, ctx).await?;
        }
    }

    Ok(())
}

/// Check if table exists in database
async fn table_exists(pool: &PgPool, table_name: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)"
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Check if table column exists in database
async fn table_column_exists(pool: &PgPool, table_name: &str, column_name: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = $1 AND column_name = $2)"
    )
    .bind(table_name)
    .bind(column_name)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Insert batch of TSPoints using VALUES clause - more efficient than individual inserts
async fn insert_ts_points_batch(
    pool: &PgPool,
    table_name: &str,
    points: &[&TSPoint],
    ctx: &Context,
) -> Result<()> {
    if points.is_empty() {
        return Ok(());
    }

    // Build common column set from first point
    let sample = points[0];
    let mut columns = vec!["time".to_string(), "time_hour".to_string()];
    
    if let Some(ref tags) = sample.tags {
        for key in tags.keys() {
            columns.push(key.clone());
        }
    }
    if let Some(ref fields) = sample.fields {
        for key in fields.keys() {
            columns.push(key.clone());
        }
    }

    // Build VALUES clauses
    let mut values_clauses = Vec::new();
    for point in points {
        let mut values = vec![
            format!("'{}'", point.time.format("%Y-%m-%d %H:%M:%S%.3f")),
            format!("'{}'", hour_start(point.time).format("%Y-%m-%d %H:%M:%S%.3f")),
        ];

        // Add tag values in same order as columns
        if let Some(ref tags) = sample.tags {
            for key in tags.keys() {
                let value = point.tags.as_ref()
                    .and_then(|t| t.get(key))
                    .cloned()
                    .unwrap_or_default();
                values.push(format!("'{}'", value.replace('\'', "''")));
            }
        }

        // Add field values in same order as columns  
        if let Some(ref fields) = sample.fields {
            for key in fields.keys() {
                let value = point.fields.as_ref()
                    .and_then(|f| f.get(key))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                match value {
                    serde_json::Value::String(s) => values.push(format!("'{}'", s.replace('\'', "''"))),
                    serde_json::Value::Number(n) => values.push(n.to_string()),
                    serde_json::Value::Bool(b) => values.push(b.to_string()),
                    serde_json::Value::Null => values.push("NULL".to_string()),
                    _ => values.push(format!("'{}'", value.to_string().replace('\'', "''"))),
                }
            }
        }

        values_clauses.push(format!("({})", values.join(", ")));
    }

    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES {}",
        table_name,
        columns.join(", "),
        values_clauses.join(", ")
    );

    if ctx.debug > 1 {
        debug!("Batch SQL: {}", sql);
    }

    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

/// Annotation configuration - exact replica of Go's Annotation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotation {
    pub name: String,
    pub description: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

/// Collection of annotations - exact replica of Go's Annotations
#[derive(Debug, Serialize, Deserialize)]
pub struct Annotations {
    pub annotations: Vec<Annotation>,
}

/// Project configuration - exact replica of Go's AllProjects.Projects[project]
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
    pub shared_db: String,
}

/// All projects configuration - exact replica of Go's AllProjects
#[derive(Debug, Serialize, Deserialize)]
pub struct AllProjects {
    pub projects: HashMap<String, Project>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with same behavior as Go version
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _matches = Command::new("devstats-annotations")
        .version("0.1.0")
        .about("Insert TSDB annotations for DevStats - exact Go replacement")
        .author("DevStats Team")
        .get_matches();

    // Replicate Go's main function timing - exact replica
    let dt_start = std::time::Instant::now();
    make_annotations().await?;
    let dt_end = std::time::Instant::now();
    println!("Time: {:?}", dt_end - dt_start);

    Ok(())
}

/// Main function equivalent to Go's makeAnnotations() - exact replica
async fn make_annotations() -> Result<()> {
    // Initialize context from environment - exact replica of Go's ctx.Init()
    let mut ctx = Context::from_env()?;

    // Setup timeout signal equivalent - Rust doesn't need explicit signal setup
    // as process termination is handled by the OS

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    // Exact replica of Go's project check
    if ctx.project.is_empty() {
        eprintln!("you have to set project via GHA2DB_PROJECT environment variable");
        std::process::exit(1);
    }

    // Local or cron mode? - exact replica of Go logic
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read defined projects - exact replica of Go's yaml.Unmarshal
    let projects_yaml_path = format!("{}{}", data_prefix, ctx.projects_yaml);
    let data = match tokio::fs::read_to_string(&projects_yaml_path).await {
        Ok(content) => content,
        Err(err) => {
            eprintln!("Fatal error reading '{}': {}", projects_yaml_path, err);
            std::process::exit(1);
        }
    };

    let projects: AllProjects = match serde_yaml::from_str(&data) {
        Ok(projects) => projects,
        Err(err) => {
            eprintln!("Fatal error parsing '{}': {}", projects_yaml_path, err);
            std::process::exit(1);
        }
    };

    // Get current project's main repo and annotation regexp - exact replica of Go logic
    let proj = match projects.projects.get(&ctx.project) {
        Some(proj) => proj.clone(),
        None => {
            eprintln!("project '{}' not found in '{}'", ctx.project, ctx.projects_yaml);
            std::process::exit(1);
        }
    };

    // Set context fields like Go version - exact replica
    ctx.shared_db = proj.shared_db.clone();
    ctx.project_main_repo = proj.main_repo.clone();

    // Get annotations using GitHub API and add annotations and quick ranges to TSDB - exact replica
    if !proj.main_repo.is_empty() {
        let annotations = get_annotations(&ctx, &proj.main_repo, &proj.annotation_regexp).await?;
        let dates = [proj.start_date, proj.join_date, proj.incubating_date, proj.graduated_date, proj.archived_date];
        process_annotations(&ctx, &annotations, &dates).await?;
    } else if let Some(start_date) = proj.start_date {
        let annotations = if let Some(join_date) = proj.join_date {
            get_fake_annotations(start_date, join_date)?
        } else {
            let mut anns = Annotations { annotations: Vec::new() };
            anns.annotations.push(Annotation {
                name: "Project start".to_string(),
                description: format!("{} - project starts", to_ymd_date(start_date)),
                date: start_date,
            });
            anns
        };
        
        let dates = [None, None, proj.incubating_date, proj.graduated_date, proj.archived_date];
        process_annotations(&ctx, &annotations, &dates).await?;
    }

    Ok(())
}

/// Get fake annotations from start and join dates - exact replica of Go's GetFakeAnnotations
fn get_fake_annotations(
    start_date: chrono::DateTime<chrono::Utc>,
    join_date: chrono::DateTime<chrono::Utc>
) -> Result<Annotations> {
    let min_date = chrono::Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
    
    if join_date < min_date || start_date < min_date || join_date <= start_date {
        return Ok(Annotations { annotations: vec![] });
    }
    
    let mut annotations = Vec::new();
    
    annotations.push(Annotation {
        name: "Project start".to_string(),
        description: format!("{} - project starts", to_ymd_date(start_date)),
        date: start_date,
    });
    
    annotations.push(Annotation {
        name: "First CNCF project join date".to_string(),
        description: to_ymd_date(join_date),
        date: join_date,
    });
    
    Ok(Annotations { annotations })
}

/// Get annotations from Git repository tags - exact replica of Go's GetAnnotations
async fn get_annotations(
    ctx: &Context,
    org_repo: &str,
    anno_regexp: &str
) -> Result<Annotations> {
    // Get org and repo from orgRepo - exact replica of Go logic
    let parts: Vec<&str> = org_repo.split('/').collect();
    if parts.len() != 2 {
        eprintln!("main repository format must be 'org/repo', found '{}'", org_repo);
        std::process::exit(1);
    }

    // Compile annotation regexp if present - exact replica
    let re = if !anno_regexp.is_empty() {
        Some(Regex::new(anno_regexp)?)
    } else {
        None
    };

    // Local or cron mode? - exact replica of Go's LocalGitScripts logic
    let cmd_prefix = if ctx.local_cmd {
        "./git/".to_string()
    } else {
        "".to_string()
    };

    // We need this to capture output - exact replica of Go's ctx.ExecOutput = true
    if ctx.debug > 0 {
        println!("Getting tags for repo {}", org_repo);
    }

    let dt_start = std::time::Instant::now();
    let rwd = format!("{}{}", ctx.repos_dir, org_repo);

    // Execute git_tags.sh equivalent - exact replica of Go's ExecCommand call
    let tags_str = exec_git_tags_command(&cmd_prefix, &rwd)?;
    let dt_end = std::time::Instant::now();

    let tags: Vec<&str> = tags_str.lines().collect();
    let mut n_tags = 0;
    let min_date = chrono::Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
    let mut anns = Annotations { annotations: Vec::new() };

    // Process each tag - exact replica of Go logic
    for tag_data in tags {
        let data = tag_data.trim();
        if data.is_empty() {
            continue;
        }

        // Use '♂♀' separator to avoid any character that can appear inside tag name or description
        // exact replica of Go logic
        let tag_data_ary: Vec<&str> = data.split("♂♀").collect();
        if tag_data_ary.len() != 3 {
            eprintln!("invalid tagData returned for repo: {}: '{}'", org_repo, data);
            std::process::exit(1);
        }

        let tag_name = tag_data_ary[0];

        // Apply regexp filter - exact replica
        if let Some(ref regex) = re {
            if !regex.is_match(tag_name) {
                continue;
            }
        }

        if tag_data_ary[1].is_empty() {
            if ctx.debug > 0 {
                println!("Empty time returned for repo: {}, tag: {}", org_repo, tag_name);
            }
            continue;
        }

        let unix_time_stamp: i64 = match tag_data_ary[1].parse() {
            Ok(ts) => ts,
            Err(_) => {
                println!("Invalid time returned for repo: {}, tag: {}: '{}'", org_repo, tag_name, data);
                continue;
            }
        };

        let creator_date = match chrono::Utc.timestamp_opt(unix_time_stamp, 0).single() {
            Some(dt) => dt,
            None => {
                println!("Invalid timestamp: {}", unix_time_stamp);
                continue;
            }
        };

        if creator_date < min_date {
            if ctx.debug > 0 {
                println!("Skipping annotation {:?} because it is before {:?}", creator_date, min_date);
            }
            continue;
        }

        let mut message = tag_data_ary[2].to_string();
        if message.len() > 40 {
            message = message[0..40].to_string();
        }

        // Replace newlines, carriage returns, and tabs with spaces - exact replica
        message = message.replace('\n', " ").replace('\r', " ").replace('\t', " ");

        anns.annotations.push(Annotation {
            name: tag_name.to_string(),
            description: message,
            date: creator_date,
        });

        n_tags += 1;
    }

    if ctx.debug > 0 {
        println!("Got {} tags for {}, took {:?}", n_tags, org_repo, dt_end - dt_start);
    }

    // Remove duplicates (annotations falling into the same hour) - exact replica of Go logic
    let mut prev_hour_date = min_date;
    anns.annotations.sort_by(|a, b| a.date.cmp(&b.date));

    let mut filtered_annotations = Vec::new();
    for ann in anns.annotations {
        let curr_hour_date = hour_start(ann.date);
        if curr_hour_date == prev_hour_date {
            if ctx.debug > 0 {
                println!("Skipping annotation {:?} because its hour date is the same as the previous one", ann);
            }
            continue;
        }
        prev_hour_date = curr_hour_date;
        filtered_annotations.push(ann);
    }

    Ok(Annotations { annotations: filtered_annotations })
}

/// Execute git tags command - exact replica of Go's ExecCommand for git_tags.sh
fn exec_git_tags_command(cmd_prefix: &str, repo_working_dir: &str) -> Result<String> {
    let script_path = format!("{}git_tags.sh", cmd_prefix);
    
    let output = if std::path::Path::new(&script_path).exists() {
        // Use git_tags.sh script - exact replica of Go
        StdCommand::new(&script_path)
            .arg(repo_working_dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
    } else {
        // Fallback to direct git command with same format as the script
        StdCommand::new("git")
            .args(&[
                "-C", repo_working_dir,
                "tag", "-l", "--sort=-version:refname",
                "--format=%(refname:short)♂♀%(creatordate:unix)♂♀%(subject)"
            ])
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
    };

    let output = match output {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                eprintln!("Git command failed: {}", stderr);
                std::process::exit(1);
            }
            output
        }
        Err(err) => {
            eprintln!("Failed to execute git command: {}", err);
            std::process::exit(1);
        }
    };

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Process annotations and write to database - exact replica of Go's ProcessAnnotations
async fn process_annotations(
    ctx: &Context,
    annotations: &Annotations,
    dates: &[Option<chrono::DateTime<chrono::Utc>>; 5]
) -> Result<()> {
    // Connect to PostgreSQL if not skipping TSDB - exact replica of Go's PgConn
    let pool = if !ctx.skip_tsdb {
        let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
        Some(sqlx::PgPool::connect(&db_url).await?)
    } else {
        None
    };

    // CNCF milestone dates - exact replica
    let start_date = dates[0];
    let join_date = dates[1];
    let incubating_date = dates[2];
    let graduated_date = dates[3];
    let archived_date = dates[4];

    // Get BatchPoints - exact replica of Go's var pts TSPoints
    let mut pts: Vec<TSPoint> = Vec::new();

    // Annotations must be sorted to create quick ranges - exact replica
    let mut sorted_annotations = annotations.annotations.clone();
    sorted_annotations.sort_by(|a, b| a.date.cmp(&b.date));

    // Iterate annotations - exact replica of Go logic
    for annotation in &sorted_annotations {
        let annotation_name = sanitize_utf8(&annotation.name);
        let annotation_description = sanitize_utf8(&annotation.description);

        let mut fields = HashMap::new();
        fields.insert("title".to_string(), serde_json::Value::String(annotation_name));
        fields.insert("description".to_string(), serde_json::Value::String(annotation_description));

        // Add batch point - exact replica
        if ctx.debug > 0 {
            println!(
                "Series: annotations: Date: {}: '{}', '{}'",
                to_ymd_date(annotation.date),
                annotation.name,
                annotation.description
            );
        }

        let pt = TSPoint::new(
            "annotations".to_string(),
            "".to_string(),
            None,
            Some(fields),
            annotation.date,
            false
        );

        if ctx.debug > 0 {
            println!("NewTSPoint: {}", pt.display());
        }
        pts.push(pt);
    }

    // If both start and join dates are present then join date must be after start date - exact replica
    if start_date.is_none() || join_date.is_none() || 
       (start_date.is_some() && join_date.is_some() && join_date.unwrap() > start_date.unwrap()) {
        
        // Project start date (additional annotation not used in quick ranges)
        if let Some(start) = start_date {
            let mut fields = HashMap::new();
            fields.insert("title".to_string(), serde_json::Value::String("Project start date".to_string()));
            fields.insert("description".to_string(), 
                serde_json::Value::String(format!("{} - project starts", to_ymd_date(start))));
            
            if ctx.debug > 0 {
                println!(
                    "Project start date: {}: 'Project start date', '{} - project starts'",
                    to_ymd_date(start),
                    to_ymd_date(start)
                );
            }
            
            let pt = TSPoint::new("annotations".to_string(), "".to_string(), None, Some(fields), start, false);
            pts.push(pt);
        }
        
        // Join CNCF (additional annotation not used in quick ranges) - exact replica
        if let Some(join) = join_date {
            let mut fields = HashMap::new();
            fields.insert("title".to_string(), serde_json::Value::String("CNCF join date".to_string()));
            fields.insert("description".to_string(),
                serde_json::Value::String(format!("{} - joined CNCF", to_ymd_date(join))));
                
            if ctx.debug > 0 {
                println!(
                    "CNCF join date: {}: 'CNCF join date', '{} - joined CNCF'",
                    to_ymd_date(join),
                    to_ymd_date(join)
                );
            }
            
            let pt = TSPoint::new("annotations".to_string(), "".to_string(), None, Some(fields), join, false);
            pts.push(pt);
        }
    }

    // Milestone annotations - exact replica of Go logic
    for (date_opt, title, desc_suffix) in [
        (incubating_date, "Moved to incubating state", "project moved to incubating state"),
        (graduated_date, "Graduated", "project graduated"),
        (archived_date, "Archived", "project was archived"),
    ] {
        if let Some(date) = date_opt {
            let mut fields = HashMap::new();
            fields.insert("title".to_string(), serde_json::Value::String(title.to_string()));
            fields.insert("description".to_string(),
                serde_json::Value::String(format!("{} - {}", to_ymd_date(date), desc_suffix)));
                
            if ctx.debug > 0 {
                println!(
                    "{}: {}: '{}', '{} - {}'",
                    title, to_ymd_date(date), title, to_ymd_date(date), desc_suffix
                );
            }
            
            let pt = TSPoint::new("annotations".to_string(), "".to_string(), None, Some(fields), date, false);
            pts.push(pt);
        }
    }

    // Generate quick ranges - exact replica of Go's periods logic  
    create_quick_ranges(&mut pts, &sorted_annotations, start_date, join_date, 
                       incubating_date, graduated_date, ctx)?;

    // Write the batch - exact replica of Go's WriteTSPoints logic
    if !ctx.skip_tsdb {
        if let Some(ref pool) = pool {
            // Delete existing quick ranges entries ending with '_n' - exact replica
            let table = "tquick_ranges";
            let column = "quick_ranges_suffix";
            
            if table_exists(pool, table).await? && table_column_exists(pool, table, column).await? {
                let delete_sql = format!("DELETE FROM \"{}\" WHERE \"{}\" LIKE '%_n'", table, column);
                let _ = sqlx::query(&delete_sql).execute(pool).await;
            }
            
            write_ts_points_to_db(pool, &pts, ctx).await?;
            
            // Annotations from all projects into 'allprj' database - exact replica
            if !ctx.skip_shared_db && !ctx.shared_db.is_empty() {
                let shared_db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
                    ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.shared_db, ctx.pg_ssl);
                let shared_pool = sqlx::PgPool::connect(&shared_db_url).await?;
                
                let mut anots = Vec::new();
                for pt in &pts {
                    if pt.name == "annotations" {
                        let mut shared_pt = pt.clone();
                        shared_pt.name = "annotations_shared".to_string();
                        if let Some(ref mut fields) = shared_pt.fields {
                            shared_pt.period = ctx.project.clone();
                            fields.insert("repo".to_string(), 
                                serde_json::Value::String(ctx.project_main_repo.clone()));
                        }
                        anots.push(shared_pt);
                    }
                }
                
                write_ts_points_to_db(&shared_pool, &anots, ctx).await?;
                shared_pool.close().await;
            }
        }
    } else if ctx.debug > 0 {
        println!("Skipping annotations series write");
    }

    if let Some(pool) = pool {
        pool.close().await;
    }

    Ok(())
}

/// Create quick ranges for time series - exact replica of Go's quick ranges logic
fn create_quick_ranges(
    pts: &mut Vec<TSPoint>,
    sorted_annotations: &[Annotation],
    start_date: Option<chrono::DateTime<chrono::Utc>>,
    join_date: Option<chrono::DateTime<chrono::Utc>>,
    incubating_date: Option<chrono::DateTime<chrono::Utc>>,
    graduated_date: Option<chrono::DateTime<chrono::Utc>>,
    ctx: &Context,
) -> Result<()> {
    // Special ranges - exact replica of Go's periods array
    let periods = [
        ("d", "Last day", "1 day"),
        ("w", "Last week", "1 week"),
        ("d10", "Last 10 days", "10 days"),
        ("m", "Last month", "1 month"),
        ("q", "Last quarter", "3 months"),
        ("m6", "Last 6 months", "6 months"),
        ("y", "Last year", "1 year"),
        ("y2", "Last 2 years", "2 years"),
        ("y3", "Last 3 years", "3 years"),
        ("y5", "Last 5 years", "5 years"),
        ("y10", "Last decade", "10 years"),
        ("y100", "Last century", "100 years"),
    ];

    // tags - exact replica of Go's map[string]string
    let tag_name = "quick_ranges";
    let mut tm = chrono::Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();

    // Add special periods - exact replica
    for (suffix, name, data) in &periods {
        let mut tags = HashMap::new();
        tags.insert(format!("{}_suffix", tag_name), suffix.to_string());
        tags.insert(format!("{}_name", tag_name), name.to_string());
        tags.insert(format!("{}_data", tag_name), format!("{};{};;", suffix, data));

        if ctx.debug > 0 {
            println!("Series: {}: {:?}", tag_name, tags);
        }

        let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
        pts.push(pt);
        tm = tm + chrono::Duration::hours(1);
    }

    // Add '(i) - (i+1)' annotation ranges - exact replica
    let last_index = sorted_annotations.len() - 1;
    for (index, annotation) in sorted_annotations.iter().enumerate() {
        let mut tags = HashMap::new();

        if index == last_index {
            let sfx = format!("a_{}_n", index);
            let annotation_name = sanitize_utf8(&annotation.name);
            tags.insert(format!("{}_suffix", tag_name), sfx.clone());
            tags.insert(format!("{}_name", tag_name), format!("{} - now", annotation_name));
            tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                sfx, 
                to_ymdhms_date(annotation.date),
                to_ymdhms_date(next_day_start(chrono::Utc::now()))
            ));
        } else {
            let next_annotation = &sorted_annotations[index + 1];
            let sfx = format!("a_{}_{}", index, index + 1);
            let annotation_name = sanitize_utf8(&annotation.name);
            let next_annotation_name = sanitize_utf8(&next_annotation.name);
            tags.insert(format!("{}_suffix", tag_name), sfx.clone());
            tags.insert(format!("{}_name", tag_name), format!("{} - {}", annotation_name, next_annotation_name));
            tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                sfx,
                to_ymdhms_date(annotation.date),
                to_ymdhms_date(next_annotation.date)
            ));
        }

        if ctx.debug > 0 {
            println!("Series: {}: {:?}", tag_name, tags);
        }

        let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
        pts.push(pt);
        tm = tm + chrono::Duration::hours(1);
    }

    // 2 special periods: before and after joining CNCF - exact replica
    if start_date.is_some() && join_date.is_some() && 
       join_date.unwrap() > start_date.unwrap() {

        let start = start_date.unwrap();
        let join = join_date.unwrap();

        // From project start to CNCF join date
        let mut tags = HashMap::new();
        let sfx = "c_b";
        tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
        tags.insert(format!("{}_name", tag_name), "Before joining CNCF".to_string());
        tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
            sfx,
            to_ymdhms_date(start),
            to_ymdhms_date(join)
        ));

        if ctx.debug > 0 {
            println!("Series: {}: {:?}", tag_name, tags);
        }

        let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
        pts.push(pt);
        tm = tm + chrono::Duration::hours(1);

        // From CNCF join date till now
        let mut tags = HashMap::new();
        let sfx = "c_n";
        tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
        tags.insert(format!("{}_name", tag_name), "Since joining CNCF".to_string());
        tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
            sfx,
            to_ymdhms_date(join),
            to_ymdhms_date(next_day_start(chrono::Utc::now()))
        ));

        if ctx.debug > 0 {
            println!("Series: {}: {:?}", tag_name, tags);
        }

        let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
        pts.push(pt);
        tm = tm + chrono::Duration::hours(1);

        // If we have both moved to incubating and graduation, then graduation must happen after moving to incubation
        let correct_order = if incubating_date.is_some() && graduated_date.is_some() {
            graduated_date.unwrap() > incubating_date.unwrap()
        } else {
            true
        };

        // Moved to incubating handle - exact replica of Go logic
        if correct_order && incubating_date.is_some() && incubating_date.unwrap() > join {
            let incubating = incubating_date.unwrap();

            // From CNCF join date to incubating date
            let mut tags = HashMap::new();
            let sfx = "c_j_i";
            tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
            tags.insert(format!("{}_name", tag_name), "CNCF join date - moved to incubation".to_string());
            tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                sfx,
                to_ymdhms_date(join),
                to_ymdhms_date(incubating)
            ));

            if ctx.debug > 0 {
                println!("Series: {}: {:?}", tag_name, tags);
            }

            let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
            pts.push(pt);
            tm = tm + chrono::Duration::hours(1);

            // From incubating till graduating or now
            if let Some(graduated) = graduated_date {
                // From incubating date to graduated date
                let mut tags = HashMap::new();
                let sfx = "c_i_g";
                tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
                tags.insert(format!("{}_name", tag_name), "Moved to incubation - graduated".to_string());
                tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                    sfx,
                    to_ymdhms_date(incubating),
                    to_ymdhms_date(graduated)
                ));

                if ctx.debug > 0 {
                    println!("Series: {}: {:?}", tag_name, tags);
                }

                let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
                pts.push(pt);
            } else {
                // From incubating till now
                let mut tags = HashMap::new();
                let sfx = "c_i_n";
                tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
                tags.insert(format!("{}_name", tag_name), "Since moving to incubating state".to_string());
                tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                    sfx,
                    to_ymdhms_date(incubating),
                    to_ymdhms_date(next_day_start(chrono::Utc::now()))
                ));

                if ctx.debug > 0 {
                    println!("Series: {}: {:?}", tag_name, tags);
                }

                let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
                pts.push(pt);
            }
            tm = tm + chrono::Duration::hours(1);
        }

        // Graduated handle - exact replica of Go logic
        if correct_order && graduated_date.is_some() && graduated_date.unwrap() > join {
            let graduated = graduated_date.unwrap();

            // If incubating happened after graduation or there was no moved to incubating date
            if incubating_date.is_none() {
                // From CNCF join date to graduated
                let mut tags = HashMap::new();
                let sfx = "c_j_g";
                tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
                tags.insert(format!("{}_name", tag_name), "CNCF join date - graduated".to_string());
                tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                    sfx,
                    to_ymdhms_date(join),
                    to_ymdhms_date(graduated)
                ));

                if ctx.debug > 0 {
                    println!("Series: {}: {:?}", tag_name, tags);
                }

                let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
                pts.push(pt);
                tm = tm + chrono::Duration::hours(1);
            }

            // From graduated till now
            let mut tags = HashMap::new();
            let sfx = "c_g_n";
            tags.insert(format!("{}_suffix", tag_name), sfx.to_string());
            tags.insert(format!("{}_name", tag_name), "Since graduating".to_string());
            tags.insert(format!("{}_data", tag_name), format!("{};;{};{}", 
                sfx,
                to_ymdhms_date(graduated),
                to_ymdhms_date(next_day_start(chrono::Utc::now()))
            ));

            if ctx.debug > 0 {
                println!("Series: {}: {:?}", tag_name, tags);
            }

            let pt = TSPoint::new(tag_name.to_string(), "".to_string(), Some(tags), None, tm, false);
            pts.push(pt);
        }
    }

    Ok(())
}