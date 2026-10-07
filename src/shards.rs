//! Batches of shards, the unit of work every crawler hands from `plan` to `fetch`:
//!
//! ```text
//! batches/<batch>/shard-00000.jsonl     one item per line
//! batches/<batch>/batch.json            written last: a batch without it is incomplete
//! manifests/<batch>/shard-00000.jsonl   written by the worker that finishes the shard
//! ```
//!
//! Manifest lines carry at least `status` ("fetched" or "failed") and `bytes`.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::store::Store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInfo {
    pub batch: String,
    pub crawler: String,
    pub created_at: String,
    pub shards: usize,
    pub items: usize,
    /// The planner's estimate of what fetching the whole batch will store.
    pub est_bytes: u64,
}

pub fn shard_path(batch: &str, k: usize) -> String {
    format!("batches/{batch}/shard-{k:05}.jsonl")
}

pub fn manifest_path(batch: &str, k: usize) -> String {
    format!("manifests/{batch}/shard-{k:05}.jsonl")
}

fn batch_json_path(batch: &str) -> String {
    format!("batches/{batch}/batch.json")
}

/// A batch name that sorts by creation time, such as 20261007-215300.
pub fn new_batch_name() -> String {
    chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string()
}

/// The shards job `job` takes, when each of `world` workers runs `jobs` jobs: job j of worker
/// `rank` owns slot rank * jobs + j of world * jobs, and takes every shard k with k % slots == slot.
pub fn owned_shards(
    shards: usize,
    rank: usize,
    world: usize,
    jobs: usize,
    job: usize,
) -> impl Iterator<Item = usize> {
    let slots = world * jobs;
    (rank * jobs + job..shards).step_by(slots)
}

/// Writes `items` as a new batch of `shard_size`-item shards, then batch.json.
pub async fn write_batch<T: Serialize>(
    store: &Store,
    crawler: &str,
    batch: &str,
    items: &[T],
    shard_size: usize,
    est_bytes: u64,
) -> Result<BatchInfo> {
    if batch.is_empty() || batch.contains('/') {
        bail!("bad batch name {batch:?}");
    }
    if store.exists(&batch_json_path(batch)).await? {
        bail!("batch {batch} already exists");
    }
    let chunks: Vec<&[T]> = items.chunks(shard_size.max(1)).collect();
    for (k, chunk) in chunks.iter().enumerate() {
        let mut body = String::new();
        for item in *chunk {
            body.push_str(&serde_json::to_string(item)?);
            body.push('\n');
        }
        store.put(&shard_path(batch, k), body).await?;
    }
    let info = BatchInfo {
        batch: batch.to_owned(),
        crawler: crawler.to_owned(),
        created_at: chrono::Utc::now().to_rfc3339(),
        shards: chunks.len(),
        items: items.len(),
        est_bytes,
    };
    store
        .put(&batch_json_path(batch), serde_json::to_vec_pretty(&info)?)
        .await?;
    Ok(info)
}

pub async fn read_batch(store: &Store, batch: &str) -> Result<BatchInfo> {
    let bytes = store.get(&batch_json_path(batch)).await?.with_context(|| {
        format!("batch {batch} has no batch.json: it does not exist, or plan has not finished it")
    })?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing batch.json of {batch}"))
}

/// Complete batches (those with a batch.json), oldest first.
pub async fn list_batches(store: &Store) -> Result<Vec<BatchInfo>> {
    let mut out = Vec::new();
    for name in store.list_dir("batches").await? {
        if let Some(bytes) = store.get(&batch_json_path(&name)).await? {
            out.push(
                serde_json::from_slice(&bytes)
                    .with_context(|| format!("parsing batch.json of {name}"))?,
            );
        }
    }
    Ok(out)
}

#[derive(Debug, Default)]
pub struct Progress {
    pub shards_done: usize,
    /// Items in finished shards, by manifest status.
    pub items: BTreeMap<String, usize>,
    pub bytes: u64,
}

/// What the workers have finished in a batch, read from its manifests.
pub async fn progress(store: &Store, batch: &str) -> Result<Progress> {
    #[derive(Deserialize)]
    struct Line {
        status: String,
        #[serde(default)]
        bytes: u64,
    }
    let mut p = Progress::default();
    let dir = format!("manifests/{batch}");
    for name in store.list_dir(&dir).await? {
        let Some(bytes) = store.get(&format!("{dir}/{name}")).await? else {
            continue;
        };
        p.shards_done += 1;
        for line in bytes.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let line: Line =
                serde_json::from_slice(line).with_context(|| format!("parsing {dir}/{name}"))?;
            *p.items.entry(line.status).or_default() += 1;
            p.bytes += line.bytes;
        }
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_cover_every_shard_once() {
        let (shards, world, jobs) = (23, 3, 2);
        let mut seen = vec![0; shards];
        for rank in 0..world {
            for job in 0..jobs {
                for k in owned_shards(shards, rank, world, jobs, job) {
                    seen[k] += 1;
                }
            }
        }
        assert!(seen.iter().all(|&n| n == 1), "{seen:?}");
        assert_eq!(owned_shards(10, 1, 2, 2, 1).collect::<Vec<_>>(), vec![3, 7]);
    }

    #[tokio::test]
    async fn batches_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().to_str().unwrap()).unwrap();
        let items: Vec<u32> = (0..5).collect();
        let info = write_batch(&store, "test", "b1", &items, 2, 10)
            .await
            .unwrap();
        assert_eq!((info.shards, info.items), (3, 5));
        assert_eq!(
            store.get(&shard_path("b1", 2)).await.unwrap().unwrap(),
            b"4\n"
        );
        assert!(
            write_batch(&store, "test", "b1", &items, 2, 10)
                .await
                .is_err()
        );

        store
            .put(
                &manifest_path("b1", 0),
                "{\"status\":\"fetched\",\"bytes\":7}\n{\"status\":\"failed\"}\n",
            )
            .await
            .unwrap();
        let p = progress(&store, "b1").await.unwrap();
        assert_eq!((p.shards_done, p.bytes), (1, 7));
        assert_eq!(p.items.get("failed"), Some(&1));

        let batches = list_batches(&store).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(read_batch(&store, "b1").await.unwrap().shards, 3);
        assert!(read_batch(&store, "missing").await.is_err());
    }
}
