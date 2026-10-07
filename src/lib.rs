//! Crawlers for openly licensed data. Each crawler is a binary under src/bin/. This crate holds what
//! they share (the store, and batches of shards) and each crawler's own modules.

/// Prints a line to stderr with a UTC timestamp.
macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("{} {}", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"), format!($($arg)*))
    };
}

pub mod github;
pub mod shards;
pub mod store;

/// Sent with every HTTP request, so the sites we crawl can tell who is asking.
pub const USER_AGENT: &str = concat!("the-crawler/", env!("CARGO_PKG_VERSION"));
