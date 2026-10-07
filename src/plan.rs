//! plan: cuts accepted, unplanned videos into a batch of shards in the store.

use anyhow::{Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::api::Video;
use crate::frontier::Frontier;
use crate::store::Store;

/// One line of a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    #[serde(flatten)]
    pub video: Video,
    pub source: String,
}

/// batches/<batch>/batch.json, written after every shard: a batch without it is incomplete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub batch: String,
    pub created_at: String,
    pub shards: usize,
    pub videos: usize,
    pub shard_size: usize,
}

pub fn shard_path(batch: &str, k: usize) -> String {
    format!("batches/{batch}/shard-{k:05}.jsonl")
}

pub fn manifest_path(batch: &str, k: usize) -> String {
    format!("manifests/{batch}/shard-{k:05}.jsonl")
}

/// Writes a new batch, or returns None when nothing is waiting. If this dies after batch.json
/// but before the frontier is updated, the next plan repeats those videos in another batch;
/// fetch skips any video that already has a record, so that costs nothing.
pub async fn run(
    frontier: &Frontier,
    store: &Store,
    shard_size: usize,
    limit: Option<usize>,
) -> Result<Option<Batch>> {
    if shard_size == 0 {
        bail!("plan.shard_size must be positive");
    }
    let videos = frontier.unplanned(limit)?;
    if videos.is_empty() {
        return Ok(None);
    }
    let now = Utc::now();
    let name = now.format("%Y%m%d-%H%M%S").to_string();
    if store.exists(&format!("batches/{name}/batch.json")).await? {
        bail!("batch {name} already exists; try again in a second");
    }
    let mut shards = 0;
    for (k, chunk) in videos.chunks(shard_size).enumerate() {
        let mut body = Vec::new();
        for (video, source) in chunk {
            serde_json::to_writer(
                &mut body,
                &Entry {
                    video: video.clone(),
                    source: source.clone(),
                },
            )?;
            body.push(b'\n');
        }
        store.put(&shard_path(&name, k), body).await?;
        shards += 1;
    }
    let batch = Batch {
        batch: name.clone(),
        created_at: now.to_rfc3339(),
        shards,
        videos: videos.len(),
        shard_size,
    };
    store
        .put(
            &format!("batches/{name}/batch.json"),
            serde_json::to_vec_pretty(&batch)?,
        )
        .await?;
    let ids: Vec<String> = videos.into_iter().map(|(v, _)| v.id).collect();
    frontier.set_batch(&ids, &name)?;
    Ok(Some(batch))
}

/// Every complete batch in the store, oldest first.
pub async fn batches(store: &Store) -> Result<Vec<Batch>> {
    let mut out = vec![];
    for name in store.dirs("batches").await? {
        if let Some(bytes) = store.get(&format!("batches/{name}/batch.json")).await? {
            out.push(serde_json::from_slice(&bytes)?);
        }
    }
    Ok(out)
}

pub async fn read_shard(store: &Store, batch: &str, k: usize) -> Result<Vec<Entry>> {
    let Some(bytes) = store.get(&shard_path(batch, k)).await? else {
        bail!("missing {}", shard_path(batch, k));
    };
    let mut out = vec![];
    for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        out.push(serde_json::from_slice(line)?);
    }
    Ok(out)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn video(id: &str) -> Video {
        Video {
            id: id.into(),
            title: format!("title {id}"),
            channel_id: "UC1".into(),
            channel_title: "chan".into(),
            published_at: "2024-01-01T00:00:00Z".into(),
            duration_s: 60,
            license: "creativeCommon".into(),
            live: "none".into(),
            age_restricted: false,
            made_for_kids: false,
            privacy: "public".into(),
            language: None,
        }
    }

    #[tokio::test]
    async fn writes_shards_then_batch() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().to_str().unwrap()).unwrap();
        let f = Frontier::memory().unwrap();
        let ids: Vec<String> = (0..5).map(|i| format!("v{i}")).collect();
        f.add_ids(&ids[..2], "expand:UC1").unwrap();
        f.add_ids(&ids[2..], "search:q").unwrap();
        for id in &ids {
            f.set_checked(&video(id), None).unwrap();
        }
        let b = run(&f, &store, 2, Some(4)).await.unwrap().unwrap();
        assert_eq!((b.shards, b.videos), (2, 4));
        let first: Vec<_> = read_shard(&store, &b.batch, 0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.video.id)
            .collect();
        assert_eq!(first, vec!["v2", "v3"]); // direct hits first
        let all = batches(&store).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(f.unplanned(None).unwrap().len(), 1);
        assert!(read_shard(&store, &b.batch, 2).await.is_err());
    }

    #[tokio::test]
    async fn nothing_to_plan() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().to_str().unwrap()).unwrap();
        assert!(
            run(&Frontier::memory().unwrap(), &store, 10, None)
                .await
                .unwrap()
                .is_none()
        );
    }
}
