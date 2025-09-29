use devstats_core::{Result};
use std::env;
use std::io::{self, Read};

#[tokio::main]
async fn main() -> Result<()> {
    // Check environment variables exactly like Go version
    let kind = env::var("KIND").unwrap_or_default();
    if kind.is_empty() {
        println!("You need to specify kind via KIND=Graduated|Incubating|Sandbox");
        return Ok(());
    }

    let ssize = env::var("SIZE").unwrap_or_default();
    if ssize.is_empty() {
        println!("You need to specify size via SIZE=n (usually 9, 10, 11, 12)");
        return Ok(());
    }

    let size: usize = match ssize.parse() {
        Ok(s) => s,
        Err(err) => {
            println!("error: {:?}", err);
            return Ok(());
        }
    };

    // Read from stdin exactly like Go version
    let mut buffer = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut buffer) {
        println!("error: {:?}", err);
        return Ok(());
    }

    let debug = env::var("DEBUG").unwrap_or_default() == "1";

    // Call the tsplit function
    let result = tsplit(size, &kind, &buffer, debug);
    
    // Output result
    print!("{}", result);

    Ok(())
}

// Port of the Go tsplit function
fn tsplit(size: usize, _kind: &str, input: &str, debug: bool) -> String {
    let lines: Vec<&str> = input
        .split('\n')
        .filter(|line| !line.trim().is_empty())
        .collect();

    let mut offset = String::new();
    
    // Find offset from first <tr> line
    for line in &lines {
        if line.trim() == "<tr>" {
            if let Some(pos) = line.find("<tr>") {
                if pos > 0 {
                    offset = line[..pos].to_string();
                }
            }
            break;
        }
    }

    let mut link_lines = Vec::new();
    let mut image_lines = Vec::new();
    let mut in_links = false;
    let mut in_images = false;

    for line in &lines {
        let trimmed = line.trim();
        
        if trimmed.contains("<a ") {
            in_links = true;
            in_images = false;
        } else if trimmed.contains("<img ") {
            in_links = false;
            in_images = true;
        }

        if in_links && trimmed.starts_with("<td") {
            link_lines.push(*line);
        } else if in_images && trimmed.starts_with("<td") {
            image_lines.push(*line);
        }
    }

    if debug {
        eprintln!("Links {}:\n{}", link_lines.len(), link_lines.join("\n"));
        eprintln!("Images {}:\n{}", image_lines.len(), image_lines.join("\n"));
    }

    let mut out_lines = Vec::new();
    let mut from = 0;

    while from < link_lines.len() {
        let to = (from + size).min(link_lines.len());
        
        out_lines.push(format!("{}<tr>", offset));
        
        let last_to = to - 1;
        for i in from..to {
            let line = link_lines[i];
            
            if i == from && i == last_to {
                out_lines.push(line.replace("<td>", r#"<td class="cncf-bl cncf-br">"#));
            } else if i == from {
                out_lines.push(line.replace("<td>", r#"<td class="cncf-bl">"#));
            } else if i == last_to {
                out_lines.push(line.replace("<td>", r#"<td class="cncf-br">"#));
            } else {
                out_lines.push(line.to_string());
            }
        }
        
        out_lines.push(format!("{}</tr>", offset));
        out_lines.push(format!("{}<tr>", offset));
        
        for i in from..to {
            let line = image_lines[i];
            
            if i == from && i == last_to {
                out_lines.push(line.replace(r#"<td class="cncf-bb">"#, r#"<td class="cncf-bb cncf-bl cncf-br">"#));
            } else if i == from {
                out_lines.push(line.replace(r#"<td class="cncf-bb">"#, r#"<td class="cncf-bb cncf-bl">"#));
            } else if i == last_to {
                out_lines.push(line.replace(r#"<td class="cncf-bb">"#, r#"<td class="cncf-bb cncf-br">"#));
            } else {
                out_lines.push(line.to_string());
            }
        }
        
        out_lines.push(format!("{}</tr>", offset));
        
        from = to;
    }

    out_lines.join("\n")
}