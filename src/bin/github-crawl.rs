use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use the_crawler::github::{discover, fetch, plan, status};

/// Crawls public GitHub repositories under permissive licenses: one source snapshot per repository.
#[derive(Parser)]
#[command(name = "github-crawl", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Turn seeds into checked repositories in the SQLite frontier, spending GraphQL points.
    Discover {
        /// The seeds file (YAML): searches, owners, repos, repo_files and the accept policy.
        #[arg(long)]
        seeds: PathBuf,
        #[arg(long, default_value = "data/github.sqlite")]
        db: PathBuf,
        /// Stop after spending this many points. A token gets 5,000 an hour.
        #[arg(long)]
        max_points: Option<i64>,
        /// Least time between two queries, in milliseconds.
        #[arg(long, default_value_t = 500)]
        pace_ms: u64,
        #[arg(long, env = "GITHUB_TOKEN", hide_env_values = true)]
        token: String,
        /// https://HOST/api for GitHub Enterprise Server.
        #[arg(long, env = "GITHUB_API_URL", default_value = "https://api.github.com")]
        api_url: String,
    },
    /// Cut accepted repositories that are in no batch yet into a new batch of shards.
    Plan {
        #[arg(long, default_value = "data/github.sqlite")]
        db: PathBuf,
        /// A local directory, or an s3:// or gs:// URL.
        #[arg(long)]
        store: String,
        /// Defaults to the UTC time, such as 20261007-215300.
        #[arg(long)]
        batch: Option<String>,
        #[arg(long, default_value_t = 100)]
        shard_size: usize,
        /// Take at most this many repositories, most-starred first.
        #[arg(long)]
        max_repos: Option<usize>,
    },
    /// Download the snapshots in this worker's shards of a batch. Rerun the same command to resume.
    Fetch {
        #[arg(long)]
        store: String,
        #[arg(long)]
        batch: String,
        /// This worker's number, from 0.
        #[arg(long, default_value_t = 0)]
        rank: usize,
        /// How many workers share the batch.
        #[arg(long, default_value_t = 1)]
        world: usize,
        /// Downloads this worker runs at once.
        #[arg(long, default_value_t = 1)]
        jobs: usize,
        /// Random pause after each download, between --pause-min-ms and --pause-max-ms.
        #[arg(long, default_value_t = 1000)]
        pause_min_ms: u64,
        #[arg(long, default_value_t = 3000)]
        pause_max_ms: u64,
        /// Download rate cap per job, in MB/s; 0 for none.
        #[arg(long, default_value_t = 10.0)]
        max_rate_mb: f64,
        /// Give up on tarballs larger than this.
        #[arg(long, default_value_t = 1024)]
        max_archive_mb: u64,
        /// Stop after this many failed requests in a row.
        #[arg(long, default_value_t = 6)]
        max_errors: u32,
        #[arg(
            long,
            env = "GITHUB_CODELOAD_URL",
            default_value = "https://codeload.github.com"
        )]
        codeload_url: String,
    },
    /// Frontier counts, GraphQL points left, and per-batch progress from the manifests.
    Status {
        #[arg(long, default_value = "data/github.sqlite")]
        db: PathBuf,
        #[arg(long)]
        store: Option<String>,
        /// When set, also show the token's GraphQL points.
        #[arg(long, env = "GITHUB_TOKEN", hide_env_values = true)]
        token: Option<String>,
        #[arg(long, env = "GITHUB_API_URL", default_value = "https://api.github.com")]
        api_url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Discover {
            seeds,
            db,
            max_points,
            pace_ms,
            token,
            api_url,
        } => {
            let pace = Duration::from_millis(pace_ms);
            discover::run(&discover::DiscoverArgs {
                seeds,
                db,
                api_url,
                token,
                max_points,
                pace,
            })
            .await
        }
        Command::Plan {
            db,
            store,
            batch,
            shard_size,
            max_repos,
        } => plan::run(&plan::PlanArgs {
            db,
            store,
            batch,
            shard_size,
            max_repos,
        })
        .await
        .map(|_| ()),
        Command::Fetch {
            store,
            batch,
            rank,
            world,
            jobs,
            pause_min_ms,
            pause_max_ms,
            max_rate_mb,
            max_archive_mb,
            max_errors,
            codeload_url,
        } => {
            fetch::run(fetch::FetchArgs {
                store,
                batch,
                rank,
                world,
                jobs,
                codeload_url,
                pause_min: Duration::from_millis(pause_min_ms),
                pause_max: Duration::from_millis(pause_max_ms),
                max_rate: (max_rate_mb > 0.0).then_some(max_rate_mb * 1e6),
                max_archive_bytes: max_archive_mb * 1024 * 1024,
                max_errors,
            })
            .await
        }
        Command::Status {
            db,
            store,
            token,
            api_url,
        } => {
            status::run(
                &db,
                store.as_deref(),
                token.as_deref().map(|t| (api_url.as_str(), t)),
            )
            .await
        }
    }
}
