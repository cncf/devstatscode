use clap::Command;
use devstats_core::{Context, Result};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let matches = Command::new("devstats-tsplit")
        .version("0.1.0")
        .about("Split time series data for DevStats")
        .author("DevStats Team")
        .arg(clap::Arg::new("input")
            .help("Input time series data")
            .required(true)
            .index(1))
        .arg(clap::Arg::new("output-prefix")
            .help("Output file prefix")
            .required(true)
            .index(2))
        .get_matches();

    let input = matches.get_one::<String>("input").unwrap();
    let output_prefix = matches.get_one::<String>("output-prefix").unwrap();

    // Initialize context from environment
    let ctx = Context::from_env()?;

    if ctx.ctx_out {
        info!("Context: {:?}", ctx);
    }

    info!("Time series split tool");
    info!("Input: {}", input);
    info!("Output prefix: {}", output_prefix);

    // Read input data
    let content = match tokio::fs::read_to_string(input).await {
        Ok(content) => content,
        Err(err) => {
            error!("Failed to read input file '{}': {}", input, err);
            return Err(err.into());
        }
    };

    info!("Processing {} bytes of time series data", content.len());

    // TODO: In full implementation, would:
    // 1. Parse time series data format
    // 2. Split data based on time periods or other criteria
    // 3. Write multiple output files with appropriate naming

    // For now, demonstrate basic functionality
    let lines: Vec<&str> = content.lines().collect();
    info!("Input contains {} lines", lines.len());

    // Split into chunks (example: 1000 lines per file)
    let chunk_size = 1000;
    let chunks: Vec<_> = lines.chunks(chunk_size).collect();

    for (i, chunk) in chunks.iter().enumerate() {
        let output_file = format!("{}{:04}.txt", output_prefix, i);
        let chunk_content = chunk.join("\n");
        
        match tokio::fs::write(&output_file, chunk_content).await {
            Ok(_) => {
                info!("Written {} lines to {}", chunk.len(), output_file);
            }
            Err(err) => {
                error!("Failed to write to '{}': {}", output_file, err);
                return Err(err.into());
            }
        }
    }

    info!("Time series split completed: {} output files created", chunks.len());
    Ok(())
}