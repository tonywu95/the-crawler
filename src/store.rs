//! The object store all output goes to: a local directory, or s3:// / gs:// through object_store.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
    root: Path,
    url: String,
}

impl Store {
    /// `location` is a URL (s3://bucket/prefix, gs://..., file:///abs) or a local path. Cloud
    /// credentials come from the environment (AWS_*, GOOGLE_*).
    pub fn open(location: &str) -> Result<Self> {
        let url = if location.contains("://") {
            Url::parse(location).with_context(|| format!("bad store URL {location}"))?
        } else {
            std::fs::create_dir_all(location).with_context(|| format!("creating {location}"))?;
            let abs = std::fs::canonicalize(location)?;
            Url::from_directory_path(&abs)
                .map_err(|()| anyhow::anyhow!("bad path {}", abs.display()))?
        };
        let (inner, root) = object_store::parse_url_opts(&url, std::env::vars())
            .with_context(|| format!("opening store {url}"))?;
        Ok(Self {
            inner: Arc::from(inner),
            root,
            url: url.to_string(),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn path(&self, rel: &str) -> Path {
        Path::from_iter(self.root.parts().chain(Path::from(rel).parts()))
    }

    pub async fn put(&self, rel: &str, data: Vec<u8>) -> Result<()> {
        self.inner
            .put(&self.path(rel), PutPayload::from(data))
            .await
            .with_context(|| format!("writing {rel}"))?;
        Ok(())
    }

    pub async fn get(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.get(&self.path(rel)).await {
            Ok(r) => Ok(Some(r.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {rel}")),
        }
    }

    pub async fn exists(&self, rel: &str) -> Result<bool> {
        match self.inner.head(&self.path(rel)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e).with_context(|| format!("checking {rel}")),
        }
    }

    /// The names directly under `rel` that are directories, sorted.
    pub async fn dirs(&self, rel: &str) -> Result<Vec<String>> {
        let list = match self.inner.list_with_delimiter(Some(&self.path(rel))).await {
            Ok(l) => l,
            Err(object_store::Error::NotFound { .. }) => return Ok(vec![]),
            Err(e) => return Err(e).with_context(|| format!("listing {rel}")),
        };
        let mut names: Vec<String> = list
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .collect();
        names.sort();
        Ok(names)
    }

    /// Streams a local file to `rel`, hashing it on the way. Returns (bytes, sha256 hex).
    pub async fn upload(&self, local: &std::path::Path, rel: &str) -> Result<(u64, String)> {
        let mut file = tokio::fs::File::open(local)
            .await
            .with_context(|| format!("opening {}", local.display()))?;
        let mut writer = BufWriter::new(self.inner.clone(), self.path(rel));
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut total = 0u64;
        let result: Result<()> = async {
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                writer.write_all(&buf[..n]).await?;
                total += n as u64;
            }
            writer.shutdown().await?;
            Ok(())
        }
        .await;
        if let Err(e) = result {
            let _ = writer.abort().await;
            return Err(e).with_context(|| format!("uploading {rel}"));
        }
        Ok((total, hex::encode(hasher.finalize())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("out").to_str().unwrap()).unwrap();
        assert_eq!(store.get("a/b.json").await.unwrap(), None);
        assert!(!store.exists("a/b.json").await.unwrap());
        assert!(store.dirs("a").await.unwrap().is_empty());
        store.put("a/b.json", b"{}".to_vec()).await.unwrap();
        store.put("a/c/d.json", b"x".to_vec()).await.unwrap();
        assert_eq!(store.get("a/b.json").await.unwrap().unwrap(), b"{}");
        assert!(store.exists("a/b.json").await.unwrap());
        assert_eq!(store.dirs("a").await.unwrap(), vec!["c"]);
        assert!(dir.path().join("out/a/c/d.json").exists());

        let src = dir.path().join("src.bin");
        std::fs::write(&src, b"hello").unwrap();
        let (n, sha) = store.upload(&src, "v/x.mp4").await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(
            sha,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            std::fs::read(dir.path().join("out/v/x.mp4")).unwrap(),
            b"hello"
        );
    }
}
