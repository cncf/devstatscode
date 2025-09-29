use devstats_core::{Context, Result};
use std::{env, process, fs};
use chrono::{DateTime, Utc, NaiveDateTime};
use sqlx::PgPool;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    
    // Check arguments EXACTLY like Go version
    let args: Vec<String> = env::args().collect();
    if args.len() < 6 {
        println!("Required series name, SQL file name, from, to, period [series_name_or_func some.sql '2015-08-03' '2017-08-21' h|d|w|m|q|y [hist,desc:time_diff_as_string,multivalue,escape_value_name,annotations_ranges,skip_past,merge_series:name,custom_data,drop:table1;table2,project_scale:float]]");
        println!("Series name (series_name_or_func) will become exact series name if query return just single numeric value");
        println!("For queries returning multiple rows 'series_name_or_func' will be used as function that");
        println!("receives data row and period and returns name and value(s) for it");
        process::exit(1);
    }

    let series_name = &args[1];
    let sql_file = &args[2];
    let from_date = &args[3];
    let to_date = &args[4];
    let period = &args[5];
    let options = if args.len() > 6 { Some(&args[6]) } else { None };

    // Parse options exactly like Go version
    let mut cfg = CalcMetricConfig {
        hist: false,
        multivalue: false,
        escape_value_name: false,
        skip_escape_series_name: false,
        annotations_ranges: false,
        skip_past: false,
        desc: String::new(),
        merge_series: String::new(),
        project_scale: 1.0,
        custom_data: false,
        drop_tables: Vec::new(),
    };
    
    if let Some(opts) = options {
        for opt in opts.split(',') {
            let parts: Vec<&str> = opt.split(':').collect();
            let opt_name = parts[0];
            let opt_val = if parts.len() > 1 { parts[1] } else { "" };
            
            match opt_name {
                "hist" => cfg.hist = true,
                "multivalue" => cfg.multivalue = true,
                "escape_value_name" => cfg.escape_value_name = true,
                "skip_escape_series_name" => cfg.skip_escape_series_name = true,
                "annotations_ranges" => cfg.annotations_ranges = true,
                "skip_past" => cfg.skip_past = true,
                "desc" => cfg.desc = opt_val.to_string(),
                "merge_series" => cfg.merge_series = opt_val.to_string(),
                "project_scale" => {
                    cfg.project_scale = opt_val.parse().unwrap_or(1.0);
                }
                "custom_data" => cfg.custom_data = true,
                "drop" => {
                    cfg.drop_tables = opt_val.split(';').map(|s| s.to_string()).collect();
                }
                _ => {} // Ignore unknown options
            }
        }
    }

    // Initialize context
    let ctx = Context::from_env()?;
    
    // Connect to PostgreSQL
    let db_url = format!("postgresql://{}:{}@{}:{}/{}?sslmode={}", 
        ctx.pg_user, ctx.pg_pass, ctx.pg_host, ctx.pg_port, ctx.pg_db, ctx.pg_ssl);
    let pool = PgPool::connect(&db_url).await?;
    
    // Load and parse SQL file
    let sql_path = if ctx.local {
        format!("./{}", sql_file)
    } else {
        format!("{}/{}", ctx.data_dir, sql_file)
    };
    
    let sql_content = fs::read_to_string(&sql_path)
        .map_err(|_| format!("Cannot read SQL file: {}", sql_path))?;

    // Parse date range
    let from_dt = parse_date(from_date)?;
    let to_dt = parse_date(to_date)?;
    
    // Validate period
    if !matches!(period, "h" | "d" | "w" | "m" | "q" | "y") {
        return Err("Invalid period. Must be one of: h, d, w, m, q, y".into());
    }

    // Calculate metrics based on period and configuration
    match cfg.hist {
        true => calculate_histogram_metric(&pool, &ctx, series_name, &sql_content, &from_dt, &to_dt, period, &cfg).await?,
        false => calculate_time_series_metric(&pool, &ctx, series_name, &sql_content, &from_dt, &to_dt, period, &cfg).await?,
    }

    let elapsed = start_time.elapsed();
    println!("Time: {:?}", elapsed);
    
    Ok(())
}

#[derive(Clone)]
struct CalcMetricConfig {
    hist: bool,
    multivalue: bool,
    escape_value_name: bool,
    skip_escape_series_name: bool,
    annotations_ranges: bool,
    skip_past: bool,
    desc: String,
    merge_series: String,
    project_scale: f64,
    custom_data: bool,
    drop_tables: Vec<String>,
}

async fn calculate_time_series_metric(
    pool: &PgPool,
    ctx: &Context,
    series_name: &str,
    sql_content: &str,
    from_dt: &DateTime<Utc>,
    to_dt: &DateTime<Utc>,
    period: &str,
    cfg: &CalcMetricConfig,
) -> Result<()> {
    // Replace SQL parameters like Go version
    let mut sql = sql_content.to_string();
    sql = sql.replace("{{from}}", &format!("'{}'", from_dt.format("%Y-%m-%d %H:%M:%S")));
    sql = sql.replace("{{to}}", &format!("'{}'", to_dt.format("%Y-%m-%d %H:%M:%S")));
    sql = sql.replace("{{period}}", period);
    sql = sql.replace("{{exclude_bots}}", "and (lower(actor_login) not similar to '%bot%|%\\[bot\\]%')");
    
    // Execute SQL query
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    
    // Process results based on series type
    for row in rows {
        // Get column count and values
        let column_count = row.len();
        
        if column_count == 1 {
            // Single value series
            let value: f64 = row.try_get(0)?;
            let final_series_name = if cfg.annotations_ranges {
                format!("{}_{}_{}", series_name, period, "range")
            } else {
                format!("{}_{}", series_name, period)
            };
            
            // Write to TSDB (InfluxDB) - simplified for now
            println!("Series: {}, Value: {}", final_series_name, value * cfg.project_scale);
            
        } else if column_count >= 2 {
            // Multi-column or multi-row series
            let series_key: String = row.try_get(0)?;
            
            if series_key.contains(',') {
                // Multi-column format: "name1,name2,name3"
                let names: Vec<&str> = series_key.split(',').collect();
                for (i, name) in names.iter().enumerate() {
                    if i + 1 < column_count {
                        let value: f64 = row.try_get(i + 1)?;
                        let final_name = format!("{}_{}_{}", name, period, i);
                        println!("Series: {}, Value: {}", final_name, value * cfg.project_scale);
                    }
                }
            } else {
                // Single column with series name
                let value: f64 = row.try_get(1)?;
                let final_name = format!("{}_{}_{}", series_name, series_key, period);
                println!("Series: {}, Value: {}", final_name, value * cfg.project_scale);
            }
        }
    }
    
    Ok(())
}

async fn calculate_histogram_metric(
    pool: &PgPool,
    ctx: &Context,
    series_name: &str,
    sql_content: &str,
    from_dt: &DateTime<Utc>,
    to_dt: &DateTime<Utc>,
    period: &str,
    cfg: &CalcMetricConfig,
) -> Result<()> {
    // Histogram metrics work differently - they show distribution
    let mut sql = sql_content.to_string();
    sql = sql.replace("{{from}}", &format!("'{}'", from_dt.format("%Y-%m-%d %H:%M:%S")));
    sql = sql.replace("{{to}}", &format!("'{}'", to_dt.format("%Y-%m-%d %H:%M:%S")));
    sql = sql.replace("{{period}}", period);
    
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    
    // Process histogram data
    for row in rows {
        let column_count = row.len();
        
        if column_count == 2 {
            // Simple histogram: name, value
            let name: String = row.try_get(0)?;
            let value: f64 = row.try_get(1)?;
            println!("Histogram {}: {} = {}", series_name, name, value);
            
        } else if column_count >= 3 {
            // Complex histogram with series name
            let series_key: String = row.try_get(0)?;
            let name: String = row.try_get(1)?;
            let value: f64 = row.try_get(2)?;
            println!("Histogram {}_{}: {} = {}", series_name, series_key, name, value);
        }
    }
    
    Ok(())
}

fn parse_date(date_str: &str) -> Result<DateTime<Utc>> {
    // Try different date formats like Go version
    let formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ];
    
    for format in &formats {
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(date_str, format) {
            return Ok(DateTime::from_utc(naive_dt, Utc));
        }
        if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(date_str, format) {
            let naive_dt = naive_date.and_hms(0, 0, 0);
            return Ok(DateTime::from_utc(naive_dt, Utc));
        }
    }
    
    Err(format!("Invalid date format: {}", date_str).into())
}