//! fetch: a worker downloads the snapshots in its shards of a batch, from codeload.github.com.
//!
//! Codeload serves `git archive` of any commit as a tarball, outside the API and its rate limits,
//! so workers need no token. Per repository the worker:
//!
//! 1. downloads codeload.github.com/<owner>/<name>/tar.gz/<sha> to a temporary file;
//! 2. checks the commit id `git archive` wrote into the tarball's pax header against <sha>, and
//!    finds a top-level license file whose text matches the license GitHub reported;
//! 3. stores the tarball, then the record. The record is written last: once it exists, the
//!    repository is done, and reruns skip it.
//!
//! A shard's manifest is written when every repository in it is fetched or has failed for good.

use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use super::RepoMeta;
use super::api::retry_after;
use super::license;
use crate::shards;
use crate::store::Store;

pub struct FetchArgs {
    pub store: String,
    pub batch: String,
    pub rank: usize,
    pub world: usize,
    pub jobs: usize,
    pub codeload_url: String,
    /// Random pause after each download, between these bounds.
    pub pause_min: Duration,
    pub pause_max: Duration,
    /// Download rate cap per job, in bytes a second.
    pub max_rate: Option<f64>,
    pub max_archive_bytes: u64,
    /// Stop the job after this many failed requests in a row.
    pub max_errors: u32,
}

/// Downloads of one repository before it is marked failed.
const ATTEMPTS: u32 = 3;
/// The most of a license file that is read (and kept in the record).
const MAX_LICENSE_BYTES: u64 = 256 * 1024;

/// What a worker keeps about a fetched repository, next to its tarball.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub repo: RepoMeta,
    /// Store path of the tarball.
    pub archive: String,
    pub archive_bytes: u64,
    pub archive_sha256: String,
    pub files: u64,
    pub uncompressed_bytes: u64,
    /// The top-level license file that confirmed `repo.license`, and its text, for attribution.
    pub license_file: String,
    pub license_text: String,
    pub fetched_at: String,
}

#[derive(Serialize)]
struct ManifestLine {
    id: i64,
    full_name: String,
    sha: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    bytes: u64,
}

impl ManifestLine {
    fn new(meta: &RepoMeta, status: &'static str, reason: Option<String>, bytes: u64) -> Self {
        Self {
            id: meta.id,
            full_name: meta.full_name.clone(),
            sha: meta.sha.clone(),
            status,
            reason,
            bytes,
        }
    }
}

enum Outcome {
    Fetched(Box<Record>),
    /// Failed for good: retrying would get the same answer.
    Failed(String),
    /// Worth retrying, after the wait the server asked for if it did.
    Transient(String, Option<Duration>),
}

#[derive(Default)]
struct Totals {
    shards: usize,
    fetched: usize,
    failed: usize,
    bytes: u64,
}

struct Worker {
    args: FetchArgs,
    store: Store,
    http: reqwest::Client,
    shards: usize,
}

pub async fn run(args: FetchArgs) -> Result<()> {
    if args.world == 0 || args.rank >= args.world || args.jobs == 0 {
        bail!("need 0 <= rank < world and jobs >= 1");
    }
    if args.pause_min > args.pause_max {
        bail!("the pause's minimum is above its maximum");
    }
    let store = Store::open(&args.store)?;
    let info = shards::read_batch(&store, &args.batch).await?;
    if info.crawler != "github" {
        bail!(
            "batch {} was planned by the {} crawler",
            info.batch,
            info.crawler
        );
    }
    let http = reqwest::Client::builder()
        .user_agent(crate::USER_AGENT)
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(120))
        .build()?;
    let jobs = args.jobs;
    let worker = Arc::new(Worker {
        args,
        store,
        http,
        shards: info.shards,
    });
    let mut set = tokio::task::JoinSet::new();
    for job in 0..jobs {
        let worker = worker.clone();
        set.spawn(async move { (job, worker.run_job(job).await) });
    }
    let (mut total, mut stopped) = (Totals::default(), 0);
    while let Some(joined) = set.join_next().await {
        match joined? {
            (_, Ok(t)) => {
                total.shards += t.shards;
                total.fetched += t.fetched;
                total.failed += t.failed;
                total.bytes += t.bytes;
            }
            (job, Err(e)) => {
                stopped += 1;
                log!("job {job} stopped: {e:#}");
            }
        }
    }
    log!(
        "finished {} shards: {} repositories fetched ({:.2} GB), {} failed",
        total.shards,
        total.fetched,
        total.bytes as f64 / 1e9,
        total.failed
    );
    if stopped > 0 {
        bail!("{stopped} of {jobs} jobs stopped early; rerun the same command to resume");
    }
    Ok(())
}

impl Worker {
    async fn run_job(&self, job: usize) -> Result<Totals> {
        let a = &self.args;
        let mut totals = Totals::default();
        let mut streak = 0;
        for k in shards::owned_shards(self.shards, a.rank, a.world, a.jobs, job) {
            let manifest = shards::manifest_path(&a.batch, k);
            if self.store.exists(&manifest).await? {
                continue;
            }
            let shard = self
                .store
                .get(&shards::shard_path(&a.batch, k))
                .await?
                .with_context(|| format!("batch {} has no shard {k}", a.batch))?;
            let mut lines = String::new();
            for line in shard.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
                let meta: RepoMeta =
                    serde_json::from_slice(line).with_context(|| format!("parsing shard {k}"))?;
                let done = self.repo(&meta, &mut streak).await?;
                match done.status {
                    "fetched" => totals.fetched += 1,
                    _ => totals.failed += 1,
                }
                totals.bytes += done.bytes;
                lines.push_str(&serde_json::to_string(&done)?);
                lines.push('\n');
            }
            self.store.put(&manifest, lines).await?;
            totals.shards += 1;
            log!("job {job}: shard {k} done");
        }
        Ok(totals)
    }

    /// Fetches one repository, unless its record shows that was done already. Errs, stopping the
    /// job, after `max_errors` failed requests in a row.
    async fn repo(&self, meta: &RepoMeta, streak: &mut u32) -> Result<ManifestLine> {
        if let Some(bytes) = self.store.get(&meta.record_path()).await? {
            let record: Record = serde_json::from_slice(&bytes).context("parsing a record")?;
            return Ok(ManifestLine::new(
                meta,
                "fetched",
                None,
                record.archive_bytes,
            ));
        }
        let mut attempt = 0;
        let line = loop {
            attempt += 1;
            match self.fetch(meta).await? {
                Outcome::Fetched(record) => {
                    *streak = 0;
                    log!(
                        "{}: {} bytes, {} files",
                        meta.full_name,
                        record.archive_bytes,
                        record.files
                    );
                    break ManifestLine::new(meta, "fetched", None, record.archive_bytes);
                }
                Outcome::Failed(reason) => {
                    *streak = 0;
                    log!("{}: {reason}", meta.full_name);
                    break ManifestLine::new(meta, "failed", Some(reason), 0);
                }
                Outcome::Transient(reason, asked) => {
                    *streak += 1;
                    if *streak >= self.args.max_errors {
                        bail!(
                            "{streak} failed requests in a row, the last for {}: {reason}",
                            meta.full_name
                        );
                    }
                    let wait = asked.unwrap_or_else(|| backoff(*streak));
                    log!("{}: {reason}; waiting {}s", meta.full_name, wait.as_secs());
                    tokio::time::sleep(wait).await;
                    if attempt >= ATTEMPTS {
                        break ManifestLine::new(
                            meta,
                            "failed",
                            Some(format!("gave_up:{reason}")),
                            0,
                        );
                    }
                }
            }
        };
        let pause = rand::random_range(self.args.pause_min..=self.args.pause_max);
        tokio::time::sleep(pause).await;
        Ok(line)
    }

    /// One attempt at a repository. Errs only on local or store failures.
    async fn fetch(&self, meta: &RepoMeta) -> Result<Outcome> {
        let url = format!(
            "{}/{}/tar.gz/{}",
            self.args.codeload_url.trim_end_matches('/'),
            meta.full_name,
            meta.sha
        );
        let mut resp = match self.http.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => return Ok(Outcome::Transient(format!("request failed: {e}"), None)),
        };
        let status = resp.status();
        if !status.is_success() {
            let reason = format!("HTTP {}", status.as_u16());
            return Ok(match status {
                StatusCode::NOT_FOUND | StatusCode::GONE => Outcome::Failed("not_found".into()),
                StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS => Outcome::Failed("blocked".into()),
                StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                    Outcome::Transient(reason, retry_after(resp.headers()))
                }
                s if s.is_server_error() => Outcome::Transient(reason, retry_after(resp.headers())),
                _ => Outcome::Failed(reason),
            });
        }

        let tmp = tempfile::NamedTempFile::new().context("creating a temporary file")?;
        let mut out = tokio::fs::File::from_std(tmp.reopen()?);
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let started = Instant::now();
        loop {
            let chunk = match resp.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) => return Ok(Outcome::Transient(format!("download cut off: {e}"), None)),
            };
            size += chunk.len() as u64;
            if size > self.args.max_archive_bytes {
                return Ok(Outcome::Failed("too_large".into()));
            }
            hasher.update(&chunk);
            out.write_all(&chunk).await?;
            if let Some(rate) = self.args.max_rate {
                let due = Duration::from_secs_f64(size as f64 / rate);
                tokio::time::sleep(due.saturating_sub(started.elapsed())).await;
            }
        }
        out.flush().await?;
        drop(out);

        let path = tmp.path().to_owned();
        let snapshot = match tokio::task::spawn_blocking(move || inspect(&path)).await? {
            Ok(snapshot) => snapshot,
            Err(e) => return Ok(Outcome::Transient(format!("unreadable tarball: {e}"), None)),
        };
        if snapshot.commit.as_deref() != Some(meta.sha.as_str()) {
            let got = snapshot.commit.as_deref().unwrap_or("none");
            return Ok(Outcome::Failed(format!("commit_mismatch:{got}")));
        }
        let (license_file, license_text) = match license::verify(&meta.license, &snapshot.licenses)
        {
            Ok(file) => file.clone(),
            Err(why) => return Ok(Outcome::Failed(format!("license_check:{why}"))),
        };

        self.store
            .put_file(&meta.archive_path(), tmp.path())
            .await?;
        let record = Record {
            repo: meta.clone(),
            archive: meta.archive_path(),
            archive_bytes: size,
            archive_sha256: hex::encode(hasher.finalize()),
            files: snapshot.files,
            uncompressed_bytes: snapshot.bytes,
            license_file,
            license_text,
            fetched_at: chrono::Utc::now().to_rfc3339(),
        };
        self.store
            .put(&meta.record_path(), serde_json::to_vec_pretty(&record)?)
            .await?;
        Ok(Outcome::Fetched(Box::new(record)))
    }
}

/// 30s, 60s, 120s, ... (at most 15 minutes) after the n-th failure in a row.
fn backoff(streak: u32) -> Duration {
    Duration::from_secs((30u64 << (streak.saturating_sub(1)).min(5)).min(900))
}

/// What a worker reads from a tarball before keeping it.
#[derive(Debug, Default)]
struct Snapshot {
    /// The commit `git archive` recorded in the pax global header.
    commit: Option<String>,
    files: u64,
    bytes: u64,
    /// Top-level license files: (name, text).
    licenses: Vec<(String, String)>,
}

fn inspect(path: &Path) -> std::io::Result<Snapshot> {
    let file = std::fs::File::open(path)?;
    let mut archive = tar::Archive::new(GzDecoder::new(BufReader::new(file)));
    let mut snap = Snapshot::default();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let kind = entry.header().entry_type();
        if kind.is_pax_global_extensions() {
            if let Some(extensions) = entry.pax_extensions()? {
                for ext in extensions {
                    let ext = ext?;
                    if ext.key() == Ok("comment") {
                        snap.commit = ext.value().ok().map(str::to_owned);
                    }
                }
            }
            continue;
        }
        if !kind.is_file() {
            continue;
        }
        snap.files += 1;
        snap.bytes += entry.size();
        // Entries are <repo>-<sha>/<path>; license files sit right under that top directory.
        let path = entry.path()?.into_owned();
        let mut parts = path.components();
        let (Some(_), Some(name), None) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let name = name.as_os_str().to_string_lossy().into_owned();
        if license::is_license_file(&name) {
            let mut text = Vec::new();
            (&mut entry)
                .take(MAX_LICENSE_BYTES)
                .read_to_end(&mut text)?;
            snap.licenses
                .push((name, String::from_utf8_lossy(&text).into_owned()));
        }
    }
    Ok(snap)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A tarball shaped like codeload's: a pax global header with the commit, then <name>-<sha>/...
    pub(crate) fn tarball(name: &str, sha: &str, files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        let record = format!("comment={sha}\n");
        // A pax record is "<length> <key>=<value>\n", the length counting its own digits.
        let mut len = record.len();
        while len != len.to_string().len() + 1 + record.len() {
            len = len.to_string().len() + 1 + record.len();
        }
        let pax = format!("{len} {record}");
        let mut header = tar::Header::new_ustar();
        header.set_entry_type(tar::EntryType::XGlobalHeader);
        header.set_size(pax.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, "pax_global_header", pax.as_bytes())
            .unwrap();
        for (path, body) in files {
            let mut header = tar::Header::new_ustar();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, format!("{name}-{sha}/{path}"), body.as_bytes())
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn reads_commit_and_license_files() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let tgz = tarball(
            "cat",
            sha,
            &[
                ("LICENSE", "MIT License"),
                ("src/LICENSE", "not top level"),
                ("src/main.rs", "fn main() {}"),
            ],
        );
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &tgz).unwrap();
        let snap = inspect(tmp.path()).unwrap();
        assert_eq!(snap.commit.as_deref(), Some(sha));
        assert_eq!(snap.files, 3);
        assert_eq!(
            snap.licenses,
            vec![("LICENSE".to_string(), "MIT License".to_string())]
        );
    }

    #[test]
    fn backs_off_exponentially() {
        assert_eq!(backoff(1), Duration::from_secs(30));
        assert_eq!(backoff(3), Duration::from_secs(120));
        assert_eq!(backoff(50), Duration::from_secs(900));
    }
}
