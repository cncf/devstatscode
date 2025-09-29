use std::fmt;

/// Custom error type for DevStats operations
#[derive(Debug)]
pub enum DevStatsError {
    /// Database connection or query errors
    Database(String),
    /// GitHub API related errors
    GitHub(String),
    /// Configuration errors
    Config(String),
    /// File I/O errors
    Io(std::io::Error),
    /// JSON parsing errors
    Json(serde_json::Error),
    /// YAML parsing errors
    Yaml(serde_yaml::Error),
    /// HTTP request errors
    Http(reqwest::Error),
    /// Time parsing errors
    Time(chrono::ParseError),
    /// Generic errors with message
    Generic(String),
}

impl fmt::Display for DevStatsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DevStatsError::Database(msg) => write!(f, "Database error: {}", msg),
            DevStatsError::GitHub(msg) => write!(f, "GitHub API error: {}", msg),
            DevStatsError::Config(msg) => write!(f, "Configuration error: {}", msg),
            DevStatsError::Io(err) => write!(f, "I/O error: {}", err),
            DevStatsError::Json(err) => write!(f, "JSON error: {}", err),
            DevStatsError::Yaml(err) => write!(f, "YAML error: {}", err),
            DevStatsError::Http(err) => write!(f, "HTTP error: {}", err),
            DevStatsError::Time(err) => write!(f, "Time parsing error: {}", err),
            DevStatsError::Generic(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DevStatsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DevStatsError::Io(err) => Some(err),
            DevStatsError::Json(err) => Some(err),
            DevStatsError::Yaml(err) => Some(err),
            DevStatsError::Http(err) => Some(err),
            DevStatsError::Time(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DevStatsError {
    fn from(err: std::io::Error) -> Self {
        DevStatsError::Io(err)
    }
}

impl From<serde_json::Error> for DevStatsError {
    fn from(err: serde_json::Error) -> Self {
        DevStatsError::Json(err)
    }
}

impl From<serde_yaml::Error> for DevStatsError {
    fn from(err: serde_yaml::Error) -> Self {
        DevStatsError::Yaml(err)
    }
}

impl From<reqwest::Error> for DevStatsError {
    fn from(err: reqwest::Error) -> Self {
        DevStatsError::Http(err)
    }
}

impl From<chrono::ParseError> for DevStatsError {
    fn from(err: chrono::ParseError) -> Self {
        DevStatsError::Time(err)
    }
}

impl From<sqlx::Error> for DevStatsError {
    fn from(err: sqlx::Error) -> Self {
        DevStatsError::Database(err.to_string())
    }
}

impl From<std::net::AddrParseError> for DevStatsError {
    fn from(err: std::net::AddrParseError) -> Self {
        DevStatsError::Generic(err.to_string())
    }
}

impl From<std::num::ParseIntError> for DevStatsError {
    fn from(err: std::num::ParseIntError) -> Self {
        DevStatsError::Generic(err.to_string())
    }
}

impl From<reqwest::header::InvalidHeaderValue> for DevStatsError {
    fn from(err: reqwest::header::InvalidHeaderValue) -> Self {
        DevStatsError::Generic(err.to_string())
    }
}

impl From<regex::Error> for DevStatsError {
    fn from(err: regex::Error) -> Self {
        DevStatsError::Generic(err.to_string())
    }
}

impl From<String> for DevStatsError {
    fn from(err: String) -> Self {
        DevStatsError::Generic(err)
    }
}

impl From<anyhow::Error> for DevStatsError {
    fn from(err: anyhow::Error) -> Self {
        DevStatsError::Generic(err.to_string())
    }
}

/// Result type alias for DevStats operations
pub type Result<T> = std::result::Result<T, DevStatsError>;