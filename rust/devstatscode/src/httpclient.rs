//! Minimal HTTPS client — the `http.Get` subset the tools need (`webhook`
//! fetches the Travis CI public key). Redirects are followed (up to 10 like
//! Go) and non-2xx statuses are not errors (like Go's `http.Get`).

use std::io::Read;

/// A fetched response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Go `http.Get(url)`; the error is worded `Get "<url>": <cause>`.
pub fn get(url: &str) -> Result<HttpResponse, String> {
    let config = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(10)
        .build();
    let agent: ureq::Agent = config.into();
    let mut resp = agent
        .get(url)
        .call()
        .map_err(|e| format!("Get {:?}: {}", url, e))?;
    let status = resp.status().as_u16();
    let mut body = Vec::new();
    resp.body_mut()
        .as_reader()
        .read_to_end(&mut body)
        .map_err(|e| format!("Get {:?}: {}", url, crate::error::go_io_error_string(&e)))?;
    Ok(HttpResponse { status, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_from_local_server() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf);
            let body = "{\"config\": {}}";
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            );
        });
        let r = get(&format!("http://{}/config", addr)).unwrap();
        assert_eq!(r.status, 404);
        assert_eq!(r.body, b"{\"config\": {}}");
    }

    #[test]
    fn get_connection_refused() {
        let err = get("http://127.0.0.1:1/config").unwrap_err();
        assert!(
            err.starts_with("Get \"http://127.0.0.1:1/config\": "),
            "{err}"
        );
    }
}
