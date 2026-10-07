//! plan: cuts accepted repositories that no batch has taken yet into a new batch of shards.

use std::path::PathBuf;

use anyhow::Result;

use super::RepoMeta;
use super::frontier::Frontier;
use crate::shards::{self, BatchInfo};
use crate::store::Store;

pub struct PlanArgs {
    pub db: PathBuf,
    pub store: String,
    /// Defaults to the current UTC time, such as 20261007-215300.
    pub batch: Option<String>,
    pub shard_size: usize,
    /// Take at most this many repositories, most-starred first.
    pub max_repos: Option<usize>,
}

pub async fn run(args: &PlanArgs) -> Result<Option<BatchInfo>> {
    let mut db = Frontier::open(&args.db)?;
    let lines: Vec<RepoMeta> = db
        .unplanned(args.max_repos)?
        .iter()
        .filter_map(RepoMeta::from_repo)
        .collect();
    if lines.is_empty() {
        log!("nothing to plan: every accepted repository is in a batch already");
        return Ok(None);
    }
    let store = Store::open(&args.store)?;
    let batch = args.batch.clone().unwrap_or_else(shards::new_batch_name);
    // GitHub's disk usage includes history, so this overestimates the snapshots.
    let est_bytes = lines.iter().map(|m| m.size_kb.max(0) as u64 * 1024).sum();
    let info =
        shards::write_batch(&store, "github", &batch, &lines, args.shard_size, est_bytes).await?;
    // Marked only once the batch is complete. If this fails, the next plan takes the same
    // repositories again; that duplicates work but loses none.
    db.mark_planned(&lines.iter().map(|m| m.id).collect::<Vec<_>>(), &batch)?;
    log!(
        "planned batch {batch}: {} repositories in {} shards",
        info.items,
        info.shards
    );
    Ok(Some(info))
}
