//! youtube-crawl: discover, plan, fetch and track Creative Commons YouTube videos.

mod api;
mod config;
mod db;
mod discover;
mod egress;
mod fetch;
mod frontier;
mod plan;
mod status;
mod store;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, default_value = "config/youtube.yaml")]
    config: PathBuf,
    /// Overrides `store` in the config.
    #[arg(long)]
    store: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Spend today's API quota finding and checking Creative Commons videos (needs YOUTUBE_API_KEY).
    Discover {
        /// Also check these video ids (one per line; ids or watch URLs).
        #[arg(long)]
        ids_file: Option<PathBuf>,
        /// Skip search.list; only check ids and crawl channels.
        #[arg(long)]
        no_search: bool,
    },
    /// Cut accepted videos into a new batch of shards in the store.
    Plan {
        /// At most this many videos.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Download this worker's shards with yt-dlp.
    Fetch {
        #[arg(long, default_value_t = 0)]
        rank: usize,
        #[arg(long, default_value_t = 1)]
        world: usize,
        /// Concurrent downloads on this worker. They share its IP, so keep this low.
        #[arg(long, default_value_t = 1)]
        jobs: usize,
        /// Only this batch.
        #[arg(long)]
        batch: Option<String>,
        /// Proxy list (`<provider> <url>` per line); overrides fetch.proxies.
        #[arg(long)]
        proxies: Option<PathBuf>,
        /// Forget every IP's strikes, benches and retirements before starting.
        #[arg(long)]
        reset_egress: bool,
        /// Lease videos from the Postgres queue (DATABASE_URL) instead of reading shards; IPs
        /// are then shared with every other queue worker through Postgres.
        #[arg(long)]
        queue: bool,
        /// Queue mode: stop after this many videos.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Frontier counts, today's quota, and per-batch progress.
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut cfg = config::Config::load(&cli.config)?;
    if let Some(s) = cli.store {
        cfg.store = s;
    }
    match cli.command {
        Command::Discover {
            ids_file,
            no_search,
        } => {
            let f = frontier::Frontier::open(&db::url_from_env()?).await?;
            let opts = discover::Options {
                ids_file: ids_file.as_deref(),
                search: !no_search,
            };
            let r = discover::run(&cfg, &f, api::Http::from_env()?, opts).await?;
            println!(
                "discover: {} new ids, {} accepted, {} rejected, {} gone; {} channels, {} search pages; {} units{}",
                r.added,
                r.accepted,
                r.rejected,
                r.gone,
                r.channels,
                r.search_pages,
                r.quota_used,
                if r.out_of_quota {
                    "; quota exhausted, run again tomorrow"
                } else {
                    ""
                }
            );
        }
        Command::Plan { limit } => {
            let f = frontier::Frontier::open(&db::url_from_env()?).await?;
            let store = store::Store::open(&cfg.store)?;
            match plan::run(&f, &store, cfg.plan.shard_size, limit).await? {
                Some(b) => println!(
                    "plan: batch {} with {} videos in {} shards",
                    b.batch, b.videos, b.shards
                ),
                None => println!("plan: no accepted videos waiting"),
            }
        }
        Command::Fetch {
            rank,
            world,
            jobs,
            batch,
            proxies,
            reset_egress,
            queue,
            limit,
        } => {
            let store = store::Store::open(&cfg.store)?;
            let dl = fetch::YtDlp::new(&cfg.fetch)?;
            let list = match proxies.or(cfg.fetch.proxies.clone()) {
                Some(path) => egress::load_list(&path)?,
                None => vec![egress::Egress::direct()],
            };
            let r = if queue {
                let f = frontier::Frontier::new(
                    db::connect(&db::url_from_env()?, None, jobs * 2 + 2).await?,
                );
                let owner = worker_id();
                let pool =
                    egress::SharedPool::new(f.pool().clone(), list, &cfg.fetch, owner.clone())
                        .await?;
                if reset_egress {
                    pool.reset().await?;
                }
                announce(&pool, jobs).await?;
                let opts = fetch::QueueOptions { jobs, owner, limit };
                fetch::run_queue(&cfg.fetch, &store, &dl, &pool, &f, &opts).await?
            } else {
                let health = egress::Health::open(&cfg.fetch.egress_db)?;
                if reset_egress {
                    health.reset()?;
                }
                let pool = egress::Pool::new(list, &cfg.fetch, health, format!("{rank}/{world}"))?;
                announce(&pool, jobs).await?;
                let opts = fetch::Options {
                    rank,
                    world,
                    jobs,
                    batch,
                };
                fetch::run(&cfg.fetch, &store, &dl, &pool, &opts).await?
            };
            let shards = if queue {
                String::new()
            } else {
                format!("{} shards done; ", r.shards)
            };
            println!(
                "fetch: {shards}{} fetched, {} already had, {} unavailable, {} failed, {} strikes; {:.2} GB",
                r.fetched,
                r.skipped,
                r.unavailable,
                r.failed,
                r.strikes,
                r.bytes as f64 / 1e9
            );
        }
        Command::Status => {
            let store = store::Store::open(&cfg.store)?;
            print!("{}", status::run(&cfg, &store).await?);
        }
    }
    Ok(())
}

/// This process, as recorded on leases and attempts: host and pid.
fn worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "worker".into());
    format!("{host}-{}", std::process::id())
}

async fn announce<P: egress::IpPool>(pool: &P, jobs: usize) -> Result<()> {
    eprintln!(
        "fetch: {} egress IPs ({} retired)",
        pool.len(),
        pool.retired().await?
    );
    if jobs > pool.len() {
        eprintln!(
            "fetch: {jobs} jobs but {} IPs; an IP serves one video at a time",
            pool.len()
        );
    }
    Ok(())
}
