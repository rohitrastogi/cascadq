//! Async Rust client for the CAScadq task queue.
//!
//! Faithful port of the Python `cascadq-client` with retry, backoff,
//! and background heartbeats for claimed tasks.

mod client;
mod config;
mod error;

pub use client::{CascadqClient, ClaimedTask};
pub use config::ClientConfig;
pub use error::CascadqError;
