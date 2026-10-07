//! Where crawl output goes: a local directory, or an s3:// or gs:// bucket through object_store.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::io::AsyncWriteExt;

/// A crawl store. Paths passed to its methods are relative to where it was opened.
#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
    root: Path,
}

impl Store {
    /// Opens a local directory (created if missing), or an s3:// or gs:// URL. Cloud credentials
    /// come from the usual AWS_* and GOOGLE_* environment variables.
    pub fn open(location: &str) -> Result<Self> {
        if location.contains("://") {
            let url =
                url::Url::parse(location).with_context(|| format!("bad store URL {location}"))?;
            let env =
                std::env::vars().filter(|(k, _)| k.starts_with("AWS_") || k.starts_with("GOOGLE_"));
            let (store, root) = object_store::parse_url_opts(&url, env)
                .with_context(|| format!("opening store {location}"))?;
            Ok(Self {
                inner: Arc::from(store),
                root,
            })
        } else {
            std::fs::create_dir_all(location).with_context(|| format!("creating {location}"))?;
            let store = LocalFileSystem::new_with_prefix(location)?;
            Ok(Self {
                inner: Arc::new(store),
                root: Path::default(),
            })
        }
    }

    fn path(&self, rel: &str) -> Path {
        if self.root.as_ref().is_empty() {
            Path::from(rel)
        } else {
            Path::from(format!("{}/{rel}", self.root))
        }
    }

    pub async fn put(&self, rel: &str, bytes: impl Into<PutPayload>) -> Result<()> {
        self.inner
            .put(&self.path(rel), bytes.into())
            .await
            .with_context(|| format!("writing {rel}"))?;
        Ok(())
    }

    /// Streams a local file into the store, in parts when it is large.
    pub async fn put_file(&self, rel: &str, file: &std::path::Path) -> Result<()> {
        let mut src = tokio::fs::File::open(file).await?;
        let mut dst = BufWriter::new(self.inner.clone(), self.path(rel));
        if let Err(e) = tokio::io::copy(&mut src, &mut dst).await {
            dst.abort().await.ok();
            return Err(e).with_context(|| format!("writing {rel}"));
        }
        dst.shutdown()
            .await
            .with_context(|| format!("writing {rel}"))?;
        Ok(())
    }

    /// The object's bytes, or None if it does not exist.
    pub async fn get(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.get(&self.path(rel)).await {
            Ok(got) => Ok(Some(
                got.bytes()
                    .await
                    .with_context(|| format!("reading {rel}"))?
                    .to_vec(),
            )),
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

    /// Names of the objects and "directories" directly under `prefix`, sorted.
    pub async fn list_dir(&self, prefix: &str) -> Result<Vec<String>> {
        let listing = match self
            .inner
            .list_with_delimiter(Some(&self.path(prefix)))
            .await
        {
            Ok(listing) => listing,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("listing {prefix}")),
        };
        let dirs = listing.common_prefixes.iter();
        let files = listing.objects.iter().map(|o| &o.location);
        let mut names: Vec<String> = dirs
            .chain(files)
            .filter_map(|p| p.filename().map(str::to_owned))
            .collect();
        names.sort();
        Ok(names)
    }
}
