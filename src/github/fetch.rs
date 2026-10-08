//! fetch: a worker turns the repositories in its shards of a batch into file-level text records.
//!
//! Per repository, the worker downloads codeload.github.com/<owner>/<name>/tar.gz/<sha>, the
//! `git archive` of that commit, to a temporary file. Codeload is outside the API and its rate
//! limits, so workers need no token. Then it reads the tarball once:
//!
//! - the commit id `git archive` wrote into the pax header must be <sha>;
//! - a top-level license file must match the license GitHub reported (see license.rs);
//! - every other file goes through the checks in extract.rs, and the ones kept are staged.
//!
//! Only when both repository checks pass are its staged files added to the shard's output, so a
//! rejected repository leaves nothing behind. When every repository in the shard is done:
//!
//! ```text
//! files/<batch>/shard-00000.jsonl.gz     one kept file per line, with its repository's metadata
//! manifests/<batch>/shard-00000.jsonl    written last, one line per repository: the shard is done
//! ```
//!
//! A rerun skips shards that have a manifest, and redoes any other from its start.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

use super::api::retry_after;
use super::{RepoMeta, extract, license};
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
    /// Also store each tarball, under archives/.
    pub keep_archives: bool,
}

/// Downloads of one repository before it is marked failed.
const ATTEMPTS: u32 = 3;
/// The most of a license file that is read (and kept in the manifest).
const MAX_LICENSE_BYTES: u64 = 256 * 1024;

pub fn files_path(batch: &str, k: usize) -> String {
    format!("files/{batch}/shard-{k:05}.jsonl.gz")
}

/// One line of files/<batch>/shard-NNNNN.jsonl.gz: a source file and where it came from.
#[derive(Serialize)]
struct FileLine<'a> {
    repo: &'a str,
    repo_id: i64,
    commit: &'a str,
    license: &'a str,
    stars: i64,
    path: &'a str,
    language: &'static str,
    size: u64,
    /// The git blob id (sha1), for finding exact duplicates across repositories and crawls.
    blob_id: String,
    text: &'a str,
}

/// One line of manifests/<batch>/shard-NNNNN.jsonl: a repository and what became of it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoRecord {
    #[serde(flatten)]
    pub repo: RepoMeta,
    /// "fetched" or "failed".
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Files kept, and their bytes of text.
    pub files: u64,
    pub bytes: u64,
    /// Files not kept, by the reason (see extract.rs).
    pub dropped: BTreeMap<String, u64>,
    pub archive_bytes: u64,
    /// The top-level license file that confirmed `license`, and its text, for attribution.
    pub license_file: Option<String>,
    pub license_text: Option<String>,
    /// The crawler and version, which decide what was kept.
    pub crawler: String,
}

impl RepoRecord {
    fn failed(meta: &RepoMeta, reason: String, archive_bytes: u64) -> Self {
        Self {
            repo: meta.clone(),
            status: "failed".into(),
            reason: Some(reason),
            files: 0,
            bytes: 0,
            dropped: BTreeMap::new(),
            archive_bytes,
            license_file: None,
            license_text: None,
            crawler: crate::USER_AGENT.into(),
        }
    }
}

enum Outcome {
    Fetched(Box<RepoRecord>),
    /// Failed for good: retrying would get the same answer.
    Failed(Box<RepoRecord>),
    /// Worth retrying, after the wait the server asked for if it did.
    Transient(String, Option<Duration>),
}

#[derive(Default)]
struct Totals {
    shards: usize,
    fetched: usize,
    failed: usize,
    files: u64,
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
                total.files += t.files;
                total.bytes += t.bytes;
            }
            (job, Err(e)) => {
                stopped += 1;
                log!("job {job} stopped: {e:#}");
            }
        }
    }
    log!(
        "finished {} shards: {} repositories fetched, {} failed; {} files kept ({:.2} GB of text)",
        total.shards,
        total.fetched,
        total.failed,
        total.files,
        total.bytes as f64 / 1e9
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
            let out = Arc::new(Mutex::new(ShardOut::new()?));
            let mut lines = String::new();
            for line in shard.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
                let meta: RepoMeta =
                    serde_json::from_slice(line).with_context(|| format!("parsing shard {k}"))?;
                let record = self.repo(&meta, &out, &mut streak).await?;
                match record.status.as_str() {
                    "fetched" => totals.fetched += 1,
                    _ => totals.failed += 1,
                }
                totals.files += record.files;
                totals.bytes += record.bytes;
                lines.push_str(&serde_json::to_string(&record)?);
                lines.push('\n');
            }
            let out = Arc::into_inner(out)
                .expect("the shard's output has one owner")
                .into_inner()
                .unwrap();
            let gz = tokio::task::spawn_blocking(move || out.finish()).await??;
            self.store
                .put_file(&files_path(&a.batch, k), gz.path())
                .await?;
            self.store.put(&manifest, lines).await?;
            totals.shards += 1;
            log!("job {job}: shard {k} done");
        }
        Ok(totals)
    }

    /// Fetches one repository into the shard's output. Errs, stopping the job, after
    /// `max_errors` failed requests in a row.
    async fn repo(
        &self,
        meta: &RepoMeta,
        out: &Arc<Mutex<ShardOut>>,
        streak: &mut u32,
    ) -> Result<RepoRecord> {
        let mut attempt = 0;
        let record = loop {
            attempt += 1;
            match self.fetch(meta, out).await? {
                Outcome::Fetched(record) => {
                    *streak = 0;
                    let dropped: u64 = record.dropped.values().sum();
                    log!(
                        "{}: kept {} files ({} bytes), dropped {dropped}",
                        meta.full_name,
                        record.files,
                        record.bytes
                    );
                    break *record;
                }
                Outcome::Failed(record) => {
                    *streak = 0;
                    log!(
                        "{}: {}",
                        meta.full_name,
                        record.reason.as_deref().unwrap_or("failed")
                    );
                    break *record;
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
                        break RepoRecord::failed(meta, format!("gave_up:{reason}"), 0);
                    }
                }
            }
        };
        let pause = rand::random_range(self.args.pause_min..=self.args.pause_max);
        tokio::time::sleep(pause).await;
        Ok(record)
    }

    /// One attempt at a repository. Errs only on local or store failures.
    async fn fetch(&self, meta: &RepoMeta, out: &Arc<Mutex<ShardOut>>) -> Result<Outcome> {
        let failed = |reason: &str, bytes| {
            Outcome::Failed(Box::new(RepoRecord::failed(meta, reason.into(), bytes)))
        };
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
                StatusCode::NOT_FOUND | StatusCode::GONE => failed("not_found", 0),
                StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS => failed("blocked", 0),
                StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                    Outcome::Transient(reason, retry_after(resp.headers()))
                }
                s if s.is_server_error() => Outcome::Transient(reason, retry_after(resp.headers())),
                _ => failed(&format!("http_{}", status.as_u16()), 0),
            });
        }

        let tmp = NamedTempFile::new().context("creating a temporary file")?;
        let mut file = tokio::fs::File::from_std(tmp.reopen()?);
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
                return Ok(failed("too_large", size));
            }
            file.write_all(&chunk).await?;
            if let Some(rate) = self.args.max_rate {
                let due = Duration::from_secs_f64(size as f64 / rate);
                tokio::time::sleep(due.saturating_sub(started.elapsed())).await;
            }
        }
        file.flush().await?;
        drop(file);

        let (path, repo, out) = (tmp.path().to_owned(), meta.clone(), out.clone());
        let processed =
            tokio::task::spawn_blocking(move || process(&path, &repo, &mut out.lock().unwrap()))
                .await??;
        let kept = match processed {
            Processed::Kept(kept) => kept,
            Processed::Rejected(reason) => return Ok(failed(&reason, size)),
            Processed::Unreadable(e) => {
                return Ok(Outcome::Transient(format!("unreadable tarball: {e}"), None));
            }
        };
        if self.args.keep_archives {
            self.store
                .put_file(&meta.archive_path(), tmp.path())
                .await?;
        }
        Ok(Outcome::Fetched(Box::new(RepoRecord {
            repo: meta.clone(),
            status: "fetched".into(),
            reason: None,
            files: kept.files,
            bytes: kept.bytes,
            dropped: kept
                .dropped
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
            archive_bytes: size,
            license_file: Some(kept.license_file),
            license_text: Some(kept.license_text),
            crawler: crate::USER_AGENT.into(),
        })))
    }
}

/// 30s, 60s, 120s, ... (at most 15 minutes) after the n-th failure in a row.
fn backoff(streak: u32) -> Duration {
    Duration::from_secs((30u64 << (streak.saturating_sub(1)).min(5)).min(900))
}

/// A shard's file records, gzipped into a temporary file as repositories finish.
struct ShardOut {
    tmp: NamedTempFile,
    gz: GzEncoder<BufWriter<File>>,
    /// Blob ids of the files already in the shard: an exact duplicate is not kept twice.
    seen: HashSet<[u8; 20]>,
}

impl ShardOut {
    fn new() -> io::Result<Self> {
        let tmp = NamedTempFile::new()?;
        let gz = GzEncoder::new(
            BufWriter::new(tmp.reopen()?),
            flate2::Compression::default(),
        );
        Ok(Self {
            tmp,
            gz,
            seen: HashSet::new(),
        })
    }

    fn append(&mut self, staged: &mut Staged) -> io::Result<()> {
        io::copy(&mut File::open(staged.tmp.path())?, &mut self.gz)?;
        self.seen.extend(staged.new_ids.drain());
        Ok(())
    }

    fn finish(self) -> io::Result<NamedTempFile> {
        let Self { tmp, gz, .. } = self;
        gz.finish()?
            .into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()?;
        Ok(tmp)
    }
}

/// What reading one repository's tarball came to.
enum Processed {
    Kept(Kept),
    /// A repository check failed: the reason.
    Rejected(String),
    /// The tarball could not be read, probably a broken download.
    Unreadable(String),
}

struct Kept {
    files: u64,
    bytes: u64,
    dropped: BTreeMap<&'static str, u64>,
    license_file: String,
    license_text: String,
}

/// Reads the tarball, checks the commit and the license, and if both pass adds the repository's
/// kept files to `out`. Errs only if writing to `out` fails.
fn process(tarball: &Path, meta: &RepoMeta, out: &mut ShardOut) -> io::Result<Processed> {
    let mut staged = match stage(tarball, meta, &out.seen) {
        Ok(staged) => staged,
        Err(e) => return Ok(Processed::Unreadable(e.to_string())),
    };
    if staged.commit.as_deref() != Some(meta.sha.as_str()) {
        let got = staged.commit.as_deref().unwrap_or("none");
        return Ok(Processed::Rejected(format!("commit_mismatch:{got}")));
    }
    let (license_file, license_text) = match license::verify(&meta.license, &staged.licenses) {
        Ok(file) => file.clone(),
        Err(why) => return Ok(Processed::Rejected(format!("license_check:{why}"))),
    };
    out.append(&mut staged)?;
    Ok(Processed::Kept(Kept {
        files: staged.files,
        bytes: staged.bytes,
        dropped: staged.dropped,
        license_file,
        license_text,
    }))
}

/// One repository's kept files, staged (uncompressed) until its checks pass.
struct Staged {
    /// The commit `git archive` recorded in the pax global header.
    commit: Option<String>,
    /// Top-level license files: (name, text).
    licenses: Vec<(String, String)>,
    tmp: NamedTempFile,
    new_ids: HashSet<[u8; 20]>,
    files: u64,
    bytes: u64,
    dropped: BTreeMap<&'static str, u64>,
}

fn stage(tarball: &Path, meta: &RepoMeta, seen: &HashSet<[u8; 20]>) -> io::Result<Staged> {
    let mut archive = tar::Archive::new(GzDecoder::new(BufReader::new(File::open(tarball)?)));
    let tmp = NamedTempFile::new()?;
    let mut writer = BufWriter::new(tmp.reopen()?);
    let mut s = Staged {
        commit: None,
        licenses: Vec::new(),
        tmp,
        new_ids: HashSet::new(),
        files: 0,
        bytes: 0,
        dropped: BTreeMap::new(),
    };
    for entry in archive.entries()? {
        let mut entry = entry?;
        let kind = entry.header().entry_type();
        if kind.is_pax_global_extensions() {
            if let Some(extensions) = entry.pax_extensions()? {
                for ext in extensions {
                    let ext = ext?;
                    if ext.key() == Ok("comment") {
                        s.commit = ext.value().ok().map(str::to_owned);
                    }
                }
            }
            continue;
        }
        if !kind.is_file() {
            continue;
        }
        // Entries are <repo>-<sha>/<path>.
        let full = entry.path()?.into_owned();
        let parts: Vec<String> = full
            .components()
            .skip(1)
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        let path = parts.join("/");
        if parts.len() == 1 && license::is_license_file(&path) {
            let mut text = Vec::new();
            (&mut entry)
                .take(MAX_LICENSE_BYTES)
                .read_to_end(&mut text)?;
            s.licenses
                .push((path, String::from_utf8_lossy(&text).into_owned()));
            tally(&mut s.dropped, "license_file");
            continue;
        }
        let (language, kind) = match extract::check_path(&path) {
            Ok(found) => found,
            Err(reason) => {
                tally(&mut s.dropped, reason);
                continue;
            }
        };
        if let Err(reason) = extract::check_size(entry.size(), kind) {
            tally(&mut s.dropped, reason);
            continue;
        }
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut bytes)?;
        let text = match extract::check_content(&bytes, kind, &meta.license) {
            Ok(text) => text,
            Err(reason) => {
                tally(&mut s.dropped, reason);
                continue;
            }
        };
        let id = extract::blob_id(&bytes);
        if seen.contains(&id) || !s.new_ids.insert(id) {
            tally(&mut s.dropped, "duplicate");
            continue;
        }
        let line = FileLine {
            repo: &meta.full_name,
            repo_id: meta.id,
            commit: &meta.sha,
            license: &meta.license,
            stars: meta.stars,
            path: &path,
            language,
            size: bytes.len() as u64,
            blob_id: hex::encode(id),
            text,
        };
        serde_json::to_writer(&mut writer, &line)?;
        writer.write_all(b"\n")?;
        s.files += 1;
        s.bytes += bytes.len() as u64;
    }
    writer.flush()?;
    Ok(s)
}

fn tally(dropped: &mut BTreeMap<&'static str, u64>, reason: &'static str) {
    *dropped.entry(reason).or_default() += 1;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A tarball shaped like codeload's: a pax global header with the commit, then <name>-<sha>/...
    pub(crate) fn tarball(name: &str, sha: &str, files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder =
            tar::Builder::new(GzEncoder::new(Vec::new(), flate2::Compression::fast()));
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

    fn meta(sha: &str) -> RepoMeta {
        RepoMeta {
            id: 7,
            full_name: "octo/cat".into(),
            url: "https://github.com/octo/cat".into(),
            branch: "main".into(),
            sha: sha.into(),
            license: "MIT".into(),
            stars: 3,
            forks: 0,
            size_kb: 1,
            language: None,
            topics: vec![],
            description: None,
            archived: false,
            created_at: "2020-01-01T00:00:00Z".into(),
            pushed_at: None,
        }
    }

    #[test]
    fn stages_kept_files_and_counts_the_rest() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let tgz = tarball(
            "cat",
            sha,
            &[
                ("LICENSE", "MIT License"),
                ("src/main.rs", "fn main() {}\n"),
                ("src/copy.rs", "fn main() {}\n"),
                ("src/LICENSE", "not top level, and no language"),
                ("vendor/dep/lib.rs", "pub fn dep() {}\n"),
                ("logo.png", "\u{89}PNG"),
            ],
        );
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &tgz).unwrap();
        let staged = stage(tmp.path(), &meta(sha), &HashSet::new()).unwrap();
        assert_eq!(staged.commit.as_deref(), Some(sha));
        assert_eq!(
            staged.licenses,
            vec![("LICENSE".to_string(), "MIT License".to_string())]
        );
        assert_eq!((staged.files, staged.bytes), (1, 13));
        let dropped: Vec<_> = staged.dropped.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(
            dropped,
            vec![
                ("duplicate", 1),
                ("license_file", 1),
                ("unknown_type", 2),
                ("vendored", 1)
            ]
        );
        let line: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(staged.tmp.path()).unwrap().trim())
                .unwrap();
        assert_eq!(line["path"], "src/main.rs");
        assert_eq!(line["language"], "Rust");
        assert_eq!(line["text"], "fn main() {}\n");
        assert_eq!(
            line["blob_id"],
            hex::encode(extract::blob_id(b"fn main() {}\n"))
        );
    }

    #[test]
    fn rejects_repositories_whose_checks_fail() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let mut out = ShardOut::new().unwrap();
        let check = |files: &[(&str, &str)], tar_sha: &str, out: &mut ShardOut| {
            let tmp = NamedTempFile::new().unwrap();
            std::fs::write(tmp.path(), tarball("cat", tar_sha, files)).unwrap();
            match process(tmp.path(), &meta(sha), out).unwrap() {
                Processed::Kept(k) => format!("kept {}", k.files),
                Processed::Rejected(r) => r,
                Processed::Unreadable(e) => format!("unreadable {e}"),
            }
        };
        let mit = "Permission is hereby granted, free of charge, to any person obtaining a copy";
        let code = ("a.py", "print('hi')\n");
        assert_eq!(
            check(&[code], sha, &mut out),
            "license_check:no_license_file"
        );
        assert_eq!(
            check(&[("LICENSE", "GPL"), code], sha, &mut out),
            "license_check:text_mismatch"
        );
        assert_eq!(
            check(&[("LICENSE", mit), code], &"f".repeat(40), &mut out),
            format!("commit_mismatch:{}", "f".repeat(40))
        );
        assert_eq!(check(&[("LICENSE", mit), code], sha, &mut out), "kept 1");
        // The same file again is a duplicate of what the shard already holds.
        assert_eq!(check(&[("LICENSE", mit), code], sha, &mut out), "kept 0");

        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"not a tarball").unwrap();
        assert!(matches!(
            process(tmp.path(), &meta(sha), &mut out).unwrap(),
            Processed::Unreadable(_)
        ));

        let gz = out.finish().unwrap();
        let mut text = String::new();
        GzDecoder::new(File::open(gz.path()).unwrap())
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(
            text.lines().count(),
            1,
            "rejected repositories add nothing: {text}"
        );
    }

    #[test]
    fn backs_off_exponentially() {
        assert_eq!(backoff(1), Duration::from_secs(30));
        assert_eq!(backoff(3), Duration::from_secs(120));
        assert_eq!(backoff(50), Duration::from_secs(900));
    }
}
