use devstats_core::{Result};
use tracing::{info, error};
use regex::Regex;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // Check environment variables exactly like Go version
    let from = env::var("FROM").unwrap_or_default();
    if from.is_empty() {
        println!("You need to set 'FROM' env variable");
        std::process::exit(1);
    }

    let to = env::var("TO").unwrap_or_default();
    let no_to = env::var("NO_TO").unwrap_or_default();
    if to.is_empty() && no_to.is_empty() {
        println!("You need to set 'TO' env variable or specify NO_TO");
        std::process::exit(1);
    }

    let mode = env::var("MODE").unwrap_or_default();
    if mode.is_empty() {
        println!("You need to set 'MODE' env variable");
        std::process::exit(1);
    }

    // Check command line arguments exactly like Go version
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("You need to provide a file name");
        std::process::exit(1);
    }

    let filename = &args[1];

    // Handle special case where "-" means empty string
    let from = if from == "-" { "" } else { &from };
    let to = if to == "-" { "" } else { &to };

    // Read file content
    let content = match tokio::fs::read_to_string(filename).await {
        Ok(content) => content,
        Err(err) => {
            println!("Error: {}", err);
            std::process::exit(1);
        }
    };

    // Perform replacement based on mode
    let new_content = match mode.as_str() {
        "ss" | "ss0" => {
            // String to string replacement
            let replaced = content.replace(from, &to);
            let count = (content.len() - replaced.len() + to.len()) / (from.len().max(1));
            
            if mode == "ss" && count == 0 {
                println!("No replacements made in mode 'ss' (strict mode)");
                std::process::exit(1);
            }
            
            replaced
        }
        "rs" | "rs0" => {
            // Regexp to string replacement
            let regex = match Regex::new(from) {
                Ok(regex) => regex,
                Err(err) => {
                    println!("Error: {}", err);
                    std::process::exit(1);
                }
            };
            
            let replaced = regex.replace_all(&content, to);
            let count = regex.find_iter(&content).count();
            
            if mode == "rs" && count == 0 {
                println!("No replacements made in mode 'rs' (strict mode)");
                std::process::exit(1);
            }
            
            replaced.to_string()
        }
        "rr" | "rr0" => {
            // Regexp to regexp replacement (treat 'to' as replacement pattern)
            let regex = match Regex::new(from) {
                Ok(regex) => regex,
                Err(err) => {
                    println!("Error: {}", err);
                    std::process::exit(1);
                }
            };
            
            let replaced = regex.replace_all(&content, to);
            let count = regex.find_iter(&content).count();
            
            if mode == "rr" && count == 0 {
                println!("No replacements made in mode 'rr' (strict mode)");
                std::process::exit(1);
            }
            
            replaced.to_string()
        }
        _ => {
            println!("Unknown mode '{}'", mode);
            std::process::exit(1);
        }
    };

    // Write back to file
    if let Err(err) = tokio::fs::write(filename, &new_content).await {
        println!("Error: {}", err);
        std::process::exit(1);
    }

    // Output exactly like Go version
    println!("Hits: {}", filename);

    Ok(())
}