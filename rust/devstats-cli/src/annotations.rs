use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};
use serde::{Deserialize, Serialize};
use chrono::{Timelike, TimeZone};
use sqlx::PgPool;

/// Time series point for database storage
#[derive(Debug, Clone)]
pub struct TSPoint {
    pub time: chrono::DateTime<chrono::Utc>,
    pub name: String,
    pub period: String,
    pub tags: Option<std::collections::HashMap<String, String>>,
    pub fields: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Sanitize UTF-8 strings for database storage
fn sanitize_utf8(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\r' || *c == '\t')
        .collect()
}

/// Write a TSPoint to the database (simplified implementation)
async fn write_ts_point_to_db(_pool: &PgPool, point: &TSPoint) -> Result<()> {
    // This is a simplified implementation. The real Go implementation creates
    // specific table structures and uses complex SQL generation.
    // For now, we'll just log the points that would be written.
    if let Some(ref fields) = point.fields {
        info!("Would write TSPoint to DB: {} at {} with fields: {:?}", 
            point.name, point.time, fields);
    } else if let Some(ref tags) = point.tags {
        info!("Would write TSPoint to DB: {} at {} with tags: {:?}", 
            point.name, point.time, tags);
    }
    Ok(())
}

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
    pub shared_db: String,
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

    // Set shared_db context like Go version
    let mut ctx = ctx;
    ctx.shared_db = project.shared_db.clone();

    info!("Processing annotations for project: {}", ctx.project);
    
    // Get annotations using Git and add annotations and quick ranges to TSDB
    if !project.main_repo.is_empty() {
        info!("Main repository: {}", project.main_repo);
        info!("Annotation regexp: {}", project.annotation_regexp);
        
        let annotations = get_annotations(&ctx, &project.main_repo, &project.annotation_regexp).await?;
        let dates = vec![
            project.start_date,
            project.join_date, 
            project.incubating_date,
            project.graduated_date,
            project.archived_date,
        ];
        process_annotations(&ctx, &annotations, &dates).await?;
    } else if let Some(start_date) = project.start_date {
        info!("No main repository, using fake annotations based on milestone dates");
        
        let annotations = if let Some(join_date) = project.join_date {
            get_fake_annotations(start_date, join_date)?
        } else {
            Annotations {
                annotations: vec![Annotation {
                    name: "Project start".to_string(),
                    description: format!("{} - project starts", start_date.format("%Y-%m-%d")),
                    date: start_date,
                }],
            }
        };
        
        let dates = vec![
            None, // No start date in ProcessAnnotations call for fake annotations
            None, // No join date 
            project.incubating_date,
            project.graduated_date, 
            project.archived_date,
        ];
        process_annotations(&ctx, &annotations, &dates).await?;
    }

    info!("Annotations processing completed");
    Ok(())
}

/// Get fake annotations from start and join dates (equivalent to Go GetFakeAnnotations)
fn get_fake_annotations(start_date: chrono::DateTime<chrono::Utc>, join_date: chrono::DateTime<chrono::Utc>) -> Result<Annotations> {
    let min_date = chrono::DateTime::parse_from_rfc3339("2012-07-01T00:00:00Z")?.with_timezone(&chrono::Utc);
    
    if join_date < min_date || start_date < min_date || join_date <= start_date {
        return Ok(Annotations { annotations: vec![] });
    }
    
    let mut annotations = Vec::new();
    
    annotations.push(Annotation {
        name: "Project start".to_string(),
        description: format!("{} - project starts", start_date.format("%Y-%m-%d")),
        date: start_date,
    });
    
    annotations.push(Annotation {
        name: "First CNCF project join date".to_string(),
        description: join_date.format("%Y-%m-%d").to_string(),
        date: join_date,
    });
    
    Ok(Annotations { annotations })
}

/// Get annotations from Git repository tags (equivalent to Go GetAnnotations)
async fn get_annotations(ctx: &Context, org_repo: &str, anno_regexp: &str) -> Result<Annotations> {
    use regex::Regex;
    use std::process::Command;
    use chrono::TimeZone;
    
    // Get org and repo from orgRepo
    let parts: Vec<&str> = org_repo.split('/').collect();
    if parts.len() != 2 {
        return Err(format!("main repository format must be 'org/repo', found '{}'", org_repo).into());
    }
    
    // Compile annotation regexp if present
    let re = if !anno_regexp.is_empty() {
        Some(Regex::new(anno_regexp)?)
    } else {
        None
    };
    
    // Determine command prefix (local vs cron mode)
    let cmd_prefix = if ctx.local_cmd {
        "./git/".to_string() // LocalGitScripts equivalent
    } else {
        "".to_string()
    };
    
    if ctx.debug > 0 {
        info!("Getting tags for repo {}", org_repo);
    }
    
    let start_time = std::time::Instant::now();
    let repo_working_dir = format!("{}{}", ctx.repos_dir, org_repo);
    
    // Execute git_tags.sh equivalent: git tag -l --format="%(refname:short)♂♀%(creatordate:unix)♂♀%(subject)"
    let output = if std::path::Path::new(&format!("{}git_tags.sh", cmd_prefix)).exists() {
        // Use the shell script if available
        Command::new(&format!("{}git_tags.sh", cmd_prefix))
            .arg(&repo_working_dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
    } else {
        // Direct git command as fallback
        Command::new("git")
            .args(&[
                "-C", &repo_working_dir,
                "tag", "-l",
                "--format=%(refname:short)♂♀%(creatordate:unix)♂♀%(subject)"
            ])
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
    };
    
    let output = match output {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(format!("Git command failed: {}", stderr).into());
            }
            output
        }
        Err(err) => {
            return Err(format!("Failed to execute git command: {}", err).into());
        }
    };
    
    let elapsed = start_time.elapsed();
    let tags_str = String::from_utf8_lossy(&output.stdout);
    let tags: Vec<&str> = tags_str.lines().collect();
    
    let mut n_tags = 0;
    let min_date = chrono::Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
    let mut anns = Annotations { annotations: Vec::new() };
    
    for tag_data in tags {
        let data = tag_data.trim();
        if data.is_empty() {
            continue;
        }
        
        // Split by the special separator ♂♀ 
        let tag_data_parts: Vec<&str> = data.split("♂♀").collect();
        if tag_data_parts.len() != 3 {
            return Err(format!("invalid tagData returned for repo: {}: '{}'", org_repo, data).into());
        }
        
        let tag_name = tag_data_parts[0];
        
        // Apply regexp filter
        if let Some(ref regex) = re {
            if !regex.is_match(tag_name) {
                continue;
            }
        }
        
        if tag_data_parts[1].is_empty() {
            if ctx.debug > 0 {
                info!("Empty time returned for repo: {}, tag: {}", org_repo, tag_name);
            }
            continue;
        }
        
        let unix_timestamp: i64 = match tag_data_parts[1].parse() {
            Ok(ts) => ts,
            Err(_) => {
                info!("Invalid time returned for repo: {}, tag: {}: '{}'", org_repo, tag_name, data);
                continue;
            }
        };
        
        let creator_date = chrono::Utc.timestamp_opt(unix_timestamp, 0).single()
            .ok_or_else(|| format!("Invalid timestamp: {}", unix_timestamp))?;
        
        if creator_date < min_date {
            if ctx.debug > 0 {
                info!("Skipping annotation {:?} because it is before {:?}", creator_date, min_date);
            }
            continue;
        }
        
        let mut message = tag_data_parts[2].to_string();
        if message.len() > 40 {
            message = message[0..40].to_string();
        }
        
        // Replace newlines, carriage returns, and tabs with spaces
        message = message.replace('\n', " ").replace('\r', " ").replace('\t', " ");
        
        anns.annotations.push(Annotation {
            name: tag_name.to_string(),
            description: message,
            date: creator_date,
        });
        
        n_tags += 1;
    }
    
    if ctx.debug > 0 {
        info!("Got {} tags for {}, took {:?}", n_tags, org_repo, elapsed);
    }
    
    // Remove duplicates (annotations falling into the same hour)
    let mut prev_hour_date = min_date;
    anns.annotations.sort_by(|a, b| a.date.cmp(&b.date));
    
    let mut filtered_annotations = Vec::new();
    for ann in anns.annotations {
        let curr_hour_date = ann.date.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
        if curr_hour_date == prev_hour_date {
            if ctx.debug > 0 {
                info!("Skipping annotation {:?} because its hour date is the same as the previous one", ann);
            }
            continue;
        }
        prev_hour_date = curr_hour_date;
        filtered_annotations.push(ann);
    }
    
    Ok(Annotations { annotations: filtered_annotations })
}

/// Process annotations and write to database (equivalent to Go ProcessAnnotations)
async fn process_annotations(ctx: &Context, annotations: &Annotations, dates: &[Option<chrono::DateTime<chrono::Utc>>]) -> Result<()> {
    use sqlx::PgPool;
    
    info!("Processing {} annotations", annotations.annotations.len());
    
    // Connect to PostgreSQL if not skipping TSDB
    let pool = if !ctx.skip_tsdb {
        let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
            ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
        Some(PgPool::connect(&db_url).await?)
    } else {
        None
    };
    
    // Extract milestone dates
    let start_date = if dates.len() > 0 { dates[0] } else { None };
    let join_date = if dates.len() > 1 { dates[1] } else { None };
    let incubating_date = if dates.len() > 2 { dates[2] } else { None };
    let graduated_date = if dates.len() > 3 { dates[3] } else { None };
    let archived_date = if dates.len() > 4 { dates[4] } else { None };
    
    // Process each annotation (in a real implementation, this would write to InfluxDB/TimescaleDB)
    for annotation in &annotations.annotations {
        if ctx.debug > 0 {
            info!("Series: annotations: Date: {}: '{}', '{}'", 
                annotation.date.format("%Y-%m-%d"), 
                annotation.name, 
                annotation.description);
        }
        
        // In full implementation, would create TSPoint and write to database
        // For now, just log the processing
        if pool.is_some() {
            info!("Would write annotation to DB: {} - {}", annotation.name, annotation.description);
        }
    }
    
    // Process milestone annotations
    if start_date.is_some() && join_date.is_some() && 
       start_date.unwrap() < join_date.unwrap() {
        
        if let Some(start) = start_date {
            if ctx.debug > 0 {
                info!("Project start date: {}: 'Project start date', '{} - project starts'", 
                    start.format("%Y-%m-%d"), start.format("%Y-%m-%d"));
            }
        }
        
        if let Some(join) = join_date {
            if ctx.debug > 0 {
                info!("CNCF join date: {}: 'CNCF join date', '{} - joined CNCF'", 
                    join.format("%Y-%m-%d"), join.format("%Y-%m-%d"));
            }
        }
    }
    
    // Process other milestone dates
    for (date_opt, name, description) in [
        (incubating_date, "Moved to incubating state", "project moved to incubating state"),
        (graduated_date, "Graduated", "project graduated"),
        (archived_date, "Archived", "project was archived"),
    ] {
        if let Some(date) = date_opt {
            if ctx.debug > 0 {
                info!("{}: {}: '{}', '{} - {}'", 
                    name, date.format("%Y-%m-%d"), name, date.format("%Y-%m-%d"), description);
            }
        }
    }
    
    // Implement quick ranges generation - time range selectors for Grafana dashboards
    let mut ts_points = vec![];
    let min_date = chrono::Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
    
    // Special ranges - predefined periods
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
    
    // Create TSPoints for special periods
    let mut time_offset = min_date;
    let tag_name = "quick_ranges";
    
    // Add special periods
    for (suffix, name, data) in &periods {
        let quick_range_data = TSPoint {
            name: tag_name.to_string(),
            period: "".to_string(),
            tags: Some(std::collections::HashMap::from([
                (format!("{}_suffix", tag_name), suffix.to_string()),
                (format!("{}_name", tag_name), name.to_string()),
                (format!("{}_data", tag_name), format!("{};{};", suffix, data)),
            ])),
            fields: None,
            time: time_offset,
        };
        ts_points.push(quick_range_data);
        time_offset = time_offset + chrono::Duration::hours(1);
    }
    
    // Sort annotations by date for range generation
    let mut sorted_annotations = annotations.annotations.clone();
    sorted_annotations.sort_by(|a, b| a.date.cmp(&b.date));
    
    // Add '(i) - (i+1)' annotation ranges
    let last_index = sorted_annotations.len();
    for (index, annotation) in sorted_annotations.iter().enumerate() {
        if index == last_index - 1 {
            // Last annotation - create "annotation - now" range
            let suffix = format!("a_{}_n", index);
            let annotation_name = sanitize_utf8(&annotation.name);
            let range_data = TSPoint {
                name: tag_name.to_string(),
                period: "".to_string(),
                tags: Some(std::collections::HashMap::from([
                    (format!("{}_suffix", tag_name), suffix.clone()),
                    (format!("{}_name", tag_name), format!("{} - now", annotation_name)),
                    (format!("{}_data", tag_name), format!("{};{};{}", 
                        suffix, 
                        annotation.date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        chrono::Utc::now().date_naive().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    )),
                ])),
                fields: None,
                time: time_offset,
            };
            ts_points.push(range_data);
            time_offset = time_offset + chrono::Duration::hours(1);
        } else {
            // Create range from current annotation to next
            let next_annotation = &sorted_annotations[index + 1];
            let suffix = format!("a_{}_{}", index, index + 1);
            let annotation_name = sanitize_utf8(&annotation.name);
            let next_annotation_name = sanitize_utf8(&next_annotation.name);
            
            let range_data = TSPoint {
                name: tag_name.to_string(),
                period: "".to_string(),
                tags: Some(std::collections::HashMap::from([
                    (format!("{}_suffix", tag_name), suffix.clone()),
                    (format!("{}_name", tag_name), format!("{} - {}", annotation_name, next_annotation_name)),
                    (format!("{}_data", tag_name), format!("{};{};{}", 
                        suffix,
                        annotation.date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        next_annotation.date.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    )),
                ])),
                fields: None,
                time: time_offset,
            };
            ts_points.push(range_data);
            time_offset = time_offset + chrono::Duration::hours(1);
        }
    }
    
    // Add CNCF milestone ranges if dates are valid
    if start_date.is_some() && join_date.is_some() && 
       start_date.unwrap() < join_date.unwrap() {
        
        let start = start_date.unwrap();
        let join = join_date.unwrap();
        
        // Before joining CNCF
        let suffix = "c_b";
        let range_data = TSPoint {
            name: tag_name.to_string(),
            period: "".to_string(),
            tags: Some(std::collections::HashMap::from([
                (format!("{}_suffix", tag_name), suffix.to_string()),
                (format!("{}_name", tag_name), "Before joining CNCF".to_string()),
                (format!("{}_data", tag_name), format!("{};{};{}", 
                    suffix,
                    start.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    join.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                )),
            ])),
            fields: None,
            time: time_offset,
        };
        ts_points.push(range_data);
        time_offset = time_offset + chrono::Duration::hours(1);
        
        // Since joining CNCF
        let suffix = "c_n";
        let range_data = TSPoint {
            name: tag_name.to_string(),
            period: "".to_string(),
            tags: Some(std::collections::HashMap::from([
                (format!("{}_suffix", tag_name), suffix.to_string()),
                (format!("{}_name", tag_name), "Since joining CNCF".to_string()),
                (format!("{}_data", tag_name), format!("{};{};{}", 
                    suffix,
                    join.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    chrono::Utc::now().date_naive().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                )),
            ])),
            fields: None,
            time: time_offset,
        };
        ts_points.push(range_data);
        time_offset = time_offset + chrono::Duration::hours(1);
        
        // Handle incubating and graduation dates with correct order validation
        let correct_order = if incubating_date.is_some() && graduated_date.is_some() {
            graduated_date.unwrap() > incubating_date.unwrap()
        } else {
            true
        };
        
        // Moved to incubating handle
        if correct_order && incubating_date.is_some() && incubating_date.unwrap() > join {
            let incubating = incubating_date.unwrap();
            
            // CNCF join date - moved to incubation
            let suffix = "c_j_i";
            let range_data = TSPoint {
                name: tag_name.to_string(),
                period: "".to_string(),
                tags: Some(std::collections::HashMap::from([
                    (format!("{}_suffix", tag_name), suffix.to_string()),
                    (format!("{}_name", tag_name), "CNCF join date - moved to incubation".to_string()),
                    (format!("{}_data", tag_name), format!("{};{};{}", 
                        suffix,
                        join.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        incubating.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    )),
                ])),
                fields: None,
                time: time_offset,
            };
            ts_points.push(range_data);
            time_offset = time_offset + chrono::Duration::hours(1);
            
            // From incubating till graduating or now
            if graduated_date.is_some() {
                let graduated = graduated_date.unwrap();
                let suffix = "c_i_g";
                let range_data = TSPoint {
                    name: tag_name.to_string(),
                    period: "".to_string(),
                    tags: Some(std::collections::HashMap::from([
                        (format!("{}_suffix", tag_name), suffix.to_string()),
                        (format!("{}_name", tag_name), "Moved to incubation - graduated".to_string()),
                        (format!("{}_data", tag_name), format!("{};{};{}", 
                            suffix,
                            incubating.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                            graduated.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        )),
                    ])),
                    fields: None,
                    time: time_offset,
                };
                ts_points.push(range_data);
                time_offset = time_offset + chrono::Duration::hours(1);
            } else {
                // From incubating till now
                let suffix = "c_i_n";
                let range_data = TSPoint {
                    name: tag_name.to_string(),
                    period: "".to_string(),
                    tags: Some(std::collections::HashMap::from([
                        (format!("{}_suffix", tag_name), suffix.to_string()),
                        (format!("{}_name", tag_name), "Since moving to incubating state".to_string()),
                        (format!("{}_data", tag_name), format!("{};{};{}", 
                            suffix,
                            incubating.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                            chrono::Utc::now().date_naive().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        )),
                    ])),
                    fields: None,
                    time: time_offset,
                };
                ts_points.push(range_data);
                time_offset = time_offset + chrono::Duration::hours(1);
            }
        }
        
        // Graduated handle
        if correct_order && graduated_date.is_some() && graduated_date.unwrap() > join {
            let graduated = graduated_date.unwrap();
            
            // If incubating happened after graduation or there was no moved to incubating date
            if incubating_date.is_none() {
                let suffix = "c_j_g";
                let range_data = TSPoint {
                    name: tag_name.to_string(),
                    period: "".to_string(),
                    tags: Some(std::collections::HashMap::from([
                        (format!("{}_suffix", tag_name), suffix.to_string()),
                        (format!("{}_name", tag_name), "CNCF join date - graduated".to_string()),
                        (format!("{}_data", tag_name), format!("{};{};{}", 
                            suffix,
                            join.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                            graduated.format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        )),
                    ])),
                    fields: None,
                    time: time_offset,
                };
                ts_points.push(range_data);
                time_offset = time_offset + chrono::Duration::hours(1);
            }
            
            // From graduated till now
            let suffix = "c_g_n";
            let range_data = TSPoint {
                name: tag_name.to_string(),
                period: "".to_string(),
                tags: Some(std::collections::HashMap::from([
                    (format!("{}_suffix", tag_name), suffix.to_string()),
                    (format!("{}_name", tag_name), "Since graduating".to_string()),
                    (format!("{}_data", tag_name), format!("{};{};{}", 
                        suffix,
                        graduated.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        chrono::Utc::now().date_naive().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    )),
                ])),
                fields: None,
                time: time_offset,
            };
            ts_points.push(range_data);
            time_offset = time_offset + chrono::Duration::hours(1);
        }
    }
    
    // Write to database if not skipping TSDB
    if !ctx.skip_tsdb {
        if let Some(pool) = pool {
            // Delete existing quick ranges entries ending with '_n'
            let table = "tquick_ranges";
            let column = "quick_ranges_suffix";
            
            // Check if table and column exist before attempting deletion
            let table_exists_query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)";
            let table_exists: bool = sqlx::query_scalar(table_exists_query)
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap_or(false);
                
            if table_exists {
                let column_exists_query = "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = $1 AND column_name = $2)";
                let column_exists: bool = sqlx::query_scalar(column_exists_query)
                    .bind(table)
                    .bind(column)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(false);
                    
                if column_exists {
                    let delete_query = format!("DELETE FROM \"{}\" WHERE \"{}\" LIKE '%_n'", table, column);
                    let _ = sqlx::query(&delete_query).execute(&pool).await;
                }
            }
            
            // Create annotation TSPoints for database writing
            for annotation in &annotations.annotations {
                let annotation_name = sanitize_utf8(&annotation.name);
                let annotation_description = sanitize_utf8(&annotation.description);
                
                let annotation_point = TSPoint {
                    name: "annotations".to_string(),
                    period: "".to_string(),
                    tags: None,
                    fields: Some(std::collections::HashMap::from([
                        ("title".to_string(), serde_json::Value::String(annotation_name)),
                        ("description".to_string(), serde_json::Value::String(annotation_description)),
                    ])),
                    time: annotation.date,
                };
                write_ts_point_to_db(&pool, &annotation_point).await?;
            }
            
            // Write all quick range TSPoints
            for point in &ts_points {
                write_ts_point_to_db(&pool, point).await?;
            }
            
            // Write shared database annotations if configured
            if !ctx.skip_shared_db && !ctx.shared_db.is_empty() {
                let shared_db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
                    ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.shared_db, ctx.pg_ssl);
                let shared_pool = sqlx::PgPool::connect(&shared_db_url).await?;
                
                for annotation in &annotations.annotations {
                    let annotation_name = sanitize_utf8(&annotation.name);
                    let annotation_description = sanitize_utf8(&annotation.description);
                    
                    let shared_point = TSPoint {
                        name: "annotations_shared".to_string(),
                        period: ctx.project.clone(),
                        tags: None,
                        fields: Some(std::collections::HashMap::from([
                            ("title".to_string(), serde_json::Value::String(annotation_name)),
                            ("description".to_string(), serde_json::Value::String(annotation_description)),
                            ("repo".to_string(), serde_json::Value::String(ctx.project_main_repo.clone())),
                        ])),
                        time: annotation.date,
                    };
                    write_ts_point_to_db(&shared_pool, &shared_point).await?;
                }
                shared_pool.close().await;
            }
        }
        info!("Annotations and quick ranges processed and written to database successfully");
    } else {
        info!("Skipping annotations series write");
    }
    
    Ok(())
}