//! youtube-crawl: discover, plan, fetch and track Creative Commons YouTube videos.

mod api;
mod config;
mod discover;
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
            let f = frontier::Frontier::open(&cfg.frontier)?;
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
            let f = frontier::Frontier::open(&cfg.frontier)?;
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
        } => {
            let store = store::Store::open(&cfg.store)?;
            let dl = fetch::YtDlp::new(&cfg.fetch)?;
            let r = fetch::run(
                &cfg.fetch,
                &store,
                &dl,
                &fetch::Options {
                    rank,
                    world,
                    jobs,
                    batch,
                },
            )
            .await?;
            println!(
                "fetch: {} shards done; {} fetched, {} already had, {} unavailable, {} failed; {:.2} GB",
                r.shards,
                r.fetched,
                r.skipped,
                r.unavailable,
                r.failed,
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
