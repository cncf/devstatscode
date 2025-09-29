use clap::Command;
use devstats_core::{Result};
use tracing::{info, error};
use regex::Regex;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let matches = Command::new("devstats-replacer")
        .version("0.1.0")
        .about("Replace strings or regexps in files")
        .author("DevStats Team")
        .arg(clap::Arg::new("from")
            .help("String or regexp to replace from")
            .required(true)
            .index(1))
        .arg(clap::Arg::new("to")
            .help("String or regexp to replace to")
            .required(true)
            .index(2))
        .arg(clap::Arg::new("file")
            .help("File to process")
            .required(true)
            .index(3))
        .arg(clap::Arg::new("mode")
            .help("Mode: ss (string to string), rs (regexp to string), rr (regexp to regexp)")
            .required(true)
            .index(4))
        .get_matches();

    let from = matches.get_one::<String>("from").unwrap();
    let to = matches.get_one::<String>("to").unwrap();
    let file_path = matches.get_one::<String>("file").unwrap();
    let mode = matches.get_one::<String>("mode").unwrap();

    // Handle special case where "-" means empty string
    let from = if from == "-" { "" } else { from };
    let to = if to == "-" { "" } else { to };

    info!("Replacer mode: {}", mode);
    info!("From: '{}'", from);
    info!("To: '{}'", to);
    info!("File: '{}'", file_path);

    // Read file content
    let content = match tokio::fs::read_to_string(file_path).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read file '{}': {}", file_path, err);
            return Err(err.into());
        }
    };

    // Perform replacement based on mode
    let new_content = match mode.as_str() {
        "ss" | "ss0" => {
            // String to string replacement
            let replaced = content.replace(from, to);
            let count = (content.len() - replaced.len() + to.len()) / (from.len().max(1));
            
            if mode == "ss" && count == 0 {
                error!("No replacements made in mode 'ss' (strict mode)");
                std::process::exit(1);
            }
            
            info!("Made {} string replacements", count);
            replaced
        }
        "rs" | "rs0" => {
            // Regexp to string replacement
            let regex = match Regex::new(from) {
                Ok(regex) => regex,
                Err(err) => {
                    error!("Invalid regexp '{}': {}", from, err);
                    return Err(err.into());
                }
            };
            
            let replaced = regex.replace_all(&content, to);
            let count = regex.find_iter(&content).count();
            
            if mode == "rs" && count == 0 {
                error!("No replacements made in mode 'rs' (strict mode)");
                std::process::exit(1);
            }
            
            info!("Made {} regexp to string replacements", count);
            replaced.to_string()
        }
        "rr" | "rr0" => {
            // Regexp to regexp replacement (treat 'to' as replacement pattern)
            let regex = match Regex::new(from) {
                Ok(regex) => regex,
                Err(err) => {
                    error!("Invalid regexp '{}': {}", from, err);
                    return Err(err.into());
                }
            };
            
            let replaced = regex.replace_all(&content, to);
            let count = regex.find_iter(&content).count();
            
            if mode == "rr" && count == 0 {
                error!("No replacements made in mode 'rr' (strict mode)");
                std::process::exit(1);
            }
            
            info!("Made {} regexp to regexp replacements", count);
            replaced.to_string()
        }
        _ => {
            error!("Invalid mode '{}'. Supported modes: ss, ss0, rs, rs0, rr, rr0", mode);
            std::process::exit(1);
        }
    };

    // Write back to file
    if let Err(err) = tokio::fs::write(file_path, new_content).await {
        error!("Failed to write file '{}': {}", file_path, err);
        return Err(err.into());
    }

    info!("File '{}' processed successfully", file_path);
    Ok(())
}