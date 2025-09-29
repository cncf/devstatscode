//! DevStats Core Library
//! 
//! This library contains the core functionality for DevStats, a GitHub analytics
//! and metrics system for CNCF projects. It provides shared types, database
//! connectivity, and utility functions used across all DevStats tools.

pub mod constants;
pub mod context;
pub mod error;
pub mod gha;

// Re-export commonly used items
pub use context::Context;
pub use error::{DevStatsError, Result};
pub use gha::{GHAEvent, GHAParser, GHAProcessor};
