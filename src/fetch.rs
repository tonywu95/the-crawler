//! fetch: a worker downloads its share of every batch's shards with yt-dlp.
//!
//! Worker `rank` of `world` runs `jobs` slots; slot s = rank × jobs + j takes the shards whose
//! index i (across all batches, oldest first) has i % (world × jobs) == s. Each download leases
//! an egress IP from the worker's pool (see egress.rs) for the whole video, so the watch page and
//! the media come through the same address. A bot check benches that IP and the video is tried
//! again on another; the worker stops when every IP is retired.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::config::Fetch;
use crate::egress::{IpPool, Use};
use crate::frontier::Frontier;
use crate::plan::{self, Entry};
use crate::store::Store;

/// What one yt-dlp run did. Files are whatever it left in the directory.
pub struct Outcome {
    pub success: bool,
    pub stderr: String,
}

pub trait Downloader {
    /// Downloads `entry` into the empty directory `dir`, through `proxy` if given.
    fn download(
        &self,
        entry: &Entry,
        dir: &Path,
        proxy: Option<&str>,
    ) -> impl Future<Output = Result<Outcome>>;
}

pub struct YtDlp {
    cfg: Fetch,
}

impl YtDlp {
    pub fn new(cfg: &Fetch) -> Result<Self> {
        for p in [&cfg.ytdlp, &cfg.deno] {
            if !p.exists() {
                bail!(
                    "{} not found; run `uv sync` or set fetch.ytdlp / fetch.deno",
                    p.display()
                );
            }
        }
        Ok(Self { cfg: cfg.clone() })
    }
}

impl Downloader for YtDlp {
    async fn download(&self, entry: &Entry, dir: &Path, proxy: Option<&str>) -> Result<Outcome> {
        let deno = std::path::absolute(&self.cfg.deno)?;
        let mut cmd = tokio::process::Command::new(&self.cfg.ytdlp);
        cmd.args([
            "--ignore-config",
            "--no-playlist",
            "--no-progress",
            "--no-overwrites",
            "--socket-timeout",
            "30",
        ])
        .arg("--js-runtimes")
        .arg(format!("deno:{}", deno.display()))
        .args(["-f", &self.cfg.format])
        // The page-side license check: a video whose page no longer says Creative Commons is
        // skipped with exit 0 and no files.
        .args(["--match-filters", "license~='(?i)creative commons'"])
        .args(["--limit-rate", &self.cfg.max_bytes_per_second.to_string()]);
        if !self.cfg.subtitles.is_empty() {
            cmd.args([
                "--write-subs",
                "--write-auto-subs",
                "--sub-format",
                "vtt",
                "--sub-langs",
            ])
            .arg(self.cfg.subtitles.join(","));
        }
        if let Some(p) = proxy {
            // Every request, the media included, goes through it: the media URL is signed for
            // the IP that loaded the page. The URL never reaches our logs (see Egress::redact).
            cmd.arg("--proxy").arg(p);
        }
        cmd.arg("-o")
            .arg(dir.join("%(id)s.%(ext)s"))
            .arg("--")
            .arg(entry.video.url())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let out = cmd
            .output()
            .await
            .with_context(|| format!("running {}", self.cfg.ytdlp.display()))?;
        Ok(Outcome {
            success: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum Failure {
    /// The proxy failed: connect, tunnel or authentication.
    Proxy(String),
    /// "Confirm you're not a bot", HTTP 429, rate limiting: the IP is being throttled.
    BotCheck(String),
    /// The video itself can't be had: private, removed, region-locked, members-only, age-gated.
    Unavailable(String),
    /// Anything else, including the page-side license check failing.
    Error(String),
}

pub fn classify(stderr: &str) -> Failure {
    let lower = stderr.to_lowercase();
    let reason = stderr
        .lines()
        .rev()
        .find(|l| l.starts_with("ERROR:"))
        .or_else(|| stderr.lines().rev().find(|l| !l.trim().is_empty()))
        .unwrap_or("no output")
        .chars()
        .take(300)
        .collect::<String>();
    const PROXY: &[&str] = &[
        "unable to connect to proxy",
        "tunnel connection failed",
        "proxyerror",
        "proxy authentication required",
        "socks",
    ];
    const BOT: &[&str] = &[
        "not a bot",
        "http error 429",
        "too many requests",
        "rate-limited",
        "rate limited",
    ];
    const GONE: &[&str] = &[
        "video unavailable",
        "private video",
        "this video is private",
        "has been removed",
        "no longer available",
        "has been terminated",
        "available in your country",
        "blocked it in your country",
        "members-only",
        "join this channel",
        "confirm your age",
        "inappropriate for some users",
        "premieres in",
        "live event will begin",
    ];
    if PROXY.iter().any(|p| lower.contains(p)) {
        Failure::Proxy(reason)
    } else if BOT.iter().any(|p| lower.contains(p)) {
        Failure::BotCheck(reason)
    } else if GONE.iter().any(|p| lower.contains(p)) {
        Failure::Unavailable(reason)
    } else {
        Failure::Error(reason)
    }
}

/// videos/<id[:2]>/<id>.json, written after the media: the video is done once it exists.
#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub url: String,
    pub title: String,
    pub channel_id: String,
    pub channel_title: String,
    pub channel_url: String,
    pub license: String,
    pub published_at: String,
    pub duration_s: u64,
    pub batch: String,
    pub shard: usize,
    pub worker: String,
    pub fetched_at: String,
    pub files: Vec<File>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct File {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// One line of manifests/<batch>/shard-NNNNN.jsonl.
#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestLine {
    pub id: String,
    /// fetched | unavailable | failed
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub bytes: u64,
    pub duration_s: u64,
}

pub fn video_dir(id: &str) -> String {
    format!("videos/{}", &id[..2.min(id.len())])
}

pub struct Options {
    pub rank: usize,
    pub world: usize,
    pub jobs: usize,
    pub batch: Option<String>,
}

#[derive(Debug, Default)]
pub struct Report {
    pub shards: usize,
    pub fetched: usize,
    pub skipped: usize,
    pub unavailable: usize,
    pub failed: usize,
    /// Bot checks, 429s and proxy failures; each video is retried on another IP.
    pub strikes: usize,
    pub bytes: u64,
}

/// State the slots share. They run on one task, so plain cells suffice.
struct Shared<'a, P> {
    cfg: &'a Fetch,
    store: &'a Store,
    worker: String,
    pool: &'a P,
    stop: RefCell<Option<String>>,
    last_strike: RefCell<String>,
    error_streak: Cell<u32>,
    report: RefCell<Report>,
}

pub async fn run<D: Downloader, P: IpPool>(
    cfg: &Fetch,
    store: &Store,
    downloader: &D,
    pool: &P,
    opts: &Options,
) -> Result<Report> {
    if opts.world == 0 || opts.jobs == 0 || opts.rank >= opts.world {
        bail!("need rank < world and jobs >= 1");
    }
    let mut shards = vec![];
    for b in plan::batches(store).await? {
        if opts.batch.as_ref().is_none_or(|want| *want == b.batch) {
            shards.extend((0..b.shards).map(|k| (b.batch.clone(), k)));
        }
    }
    let slots = opts.world * opts.jobs;
    let shared = Shared {
        cfg,
        store,
        worker: format!("{}/{}", opts.rank, opts.world),
        pool,
        stop: RefCell::new(None),
        last_strike: RefCell::new(String::new()),
        error_streak: Cell::new(0),
        report: RefCell::new(Report::default()),
    };
    let work = (0..opts.jobs).map(|j| {
        let slot = opts.rank * opts.jobs + j;
        let mine: Vec<_> = shards
            .iter()
            .enumerate()
            .filter(|(i, _)| i % slots == slot)
            .map(|(_, s)| s.clone())
            .collect();
        run_slot(&shared, downloader, mine)
    });
    for r in futures::future::join_all(work).await {
        r?;
    }
    if let Some(why) = shared.stop.borrow().as_ref() {
        bail!("worker stopped: {why}");
    }
    Ok(shared.report.into_inner())
}

pub struct QueueOptions {
    pub jobs: usize,
    /// This process, as recorded on its leases: host and pid.
    pub owner: String,
    /// Stop after leasing this many videos.
    pub limit: Option<usize>,
}

/// Queue mode: each slot leases one video at a time from Postgres until the queue is empty.
/// Failures are retried up to `max_attempts`, after `retry_delay_s`, by whichever worker leases
/// them next. Outcomes go to the frontier rather than to manifests.
pub async fn run_queue<D: Downloader, P: IpPool>(
    cfg: &Fetch,
    store: &Store,
    downloader: &D,
    pool: &P,
    frontier: &Frontier,
    opts: &QueueOptions,
) -> Result<Report> {
    if opts.jobs == 0 {
        bail!("need jobs >= 1");
    }
    let shared = Shared {
        cfg,
        store,
        worker: opts.owner.clone(),
        pool,
        stop: RefCell::new(None),
        last_strike: RefCell::new(String::new()),
        error_streak: Cell::new(0),
        report: RefCell::new(Report::default()),
    };
    let taken = Cell::new(0);
    let work = (0..opts.jobs).map(|_| queue_slot(&shared, downloader, frontier, opts, &taken));
    for r in futures::future::join_all(work).await {
        r?;
    }
    if let Some(why) = shared.stop.borrow().as_ref() {
        bail!("worker stopped: {why}");
    }
    Ok(shared.report.into_inner())
}

async fn queue_slot<D: Downloader, P: IpPool>(
    sh: &Shared<'_, P>,
    dl: &D,
    frontier: &Frontier,
    opts: &QueueOptions,
    taken: &Cell<usize>,
) -> Result<()> {
    let hold = std::time::Duration::from_secs(sh.cfg.video_lease_s.max(30));
    loop {
        if sh.stop.borrow().is_some() || opts.limit.is_some_and(|l| taken.get() >= l) {
            return Ok(());
        }
        let Some(leased) = frontier.lease(&opts.owner, 1, hold).await?.pop() else {
            return Ok(()); // the queue is empty
        };
        taken.set(taken.get() + 1);
        let entry = Entry {
            video: leased.video,
            source: leased.source,
        };
        let id = entry.video.id.clone();
        let result = tokio::select! {
            r = fetch_one(sh, dl, &entry, "queue", 0) => r,
            () = heartbeat(frontier, &id, &opts.owner, hold) => unreachable!(),
        };
        let line = match result {
            Ok(Some(line)) => line,
            Ok(None) => {
                // Stopping: hand the video straight back.
                let why = Some("worker stopped");
                frontier
                    .requeue(&id, &opts.owner, why, Default::default())
                    .await?;
                return Ok(());
            }
            Err(e) => {
                let _ = frontier
                    .requeue(&id, &opts.owner, Some("worker error"), Default::default())
                    .await;
                return Err(e);
            }
        };
        let reason = line.reason.as_deref();
        if line.status == "failed" && leased.attempts < sh.cfg.max_attempts as i32 {
            let delay = std::time::Duration::from_secs(sh.cfg.retry_delay_s);
            frontier.requeue(&id, &opts.owner, reason, delay).await?;
        } else {
            frontier
                .finish(&id, &line.status, reason, line.bytes)
                .await?;
        }
    }
}

/// Renews a video lease every third of its length; never returns.
async fn heartbeat(frontier: &Frontier, id: &str, owner: &str, hold: std::time::Duration) {
    loop {
        tokio::time::sleep(hold / 3).await;
        if let Err(e) = frontier.extend(id, owner, hold).await {
            eprintln!("fetch: renewing the lease on {id}: {e:#}");
        }
    }
}

async fn run_slot<D: Downloader, P: IpPool>(
    sh: &Shared<'_, P>,
    dl: &D,
    shards: Vec<(String, usize)>,
) -> Result<()> {
    for (batch, k) in shards {
        if sh.stop.borrow().is_some() {
            return Ok(());
        }
        let manifest = plan::manifest_path(&batch, k);
        if sh.store.exists(&manifest).await? {
            continue;
        }
        let mut lines = vec![];
        for entry in plan::read_shard(sh.store, &batch, k).await? {
            match fetch_one(sh, dl, &entry, &batch, k).await? {
                Some(line) => lines.push(line),
                None => return Ok(()), // stopped; the shard stays open for the next run
            }
        }
        let mut body = vec![];
        for line in &lines {
            serde_json::to_writer(&mut body, line)?;
            body.push(b'\n');
        }
        sh.store.put(&manifest, body).await?;
        sh.report.borrow_mut().shards += 1;
        eprintln!("fetch: {manifest} done ({} videos)", lines.len());
    }
    Ok(())
}

/// Fetches one video, retrying through bot-check backoff. None means the worker is stopping.
async fn fetch_one<D: Downloader, P: IpPool>(
    sh: &Shared<'_, P>,
    dl: &D,
    entry: &Entry,
    batch: &str,
    shard: usize,
) -> Result<Option<ManifestLine>> {
    let v = &entry.video;
    let record_path = format!("{}/{}.json", video_dir(&v.id), v.id);
    let line = |status: &str, reason: Option<String>, bytes| ManifestLine {
        id: v.id.clone(),
        status: status.into(),
        reason,
        bytes,
        duration_s: v.duration_s,
    };
    if let Some(bytes) = sh.store.get(&record_path).await? {
        let rec: Record = serde_json::from_slice(&bytes)?;
        sh.report.borrow_mut().skipped += 1;
        return Ok(Some(line(
            "fetched",
            None,
            rec.files.iter().map(|f| f.bytes).sum(),
        )));
    }
    loop {
        if sh.stop.borrow().is_some() {
            return Ok(None);
        }
        let Some(lease) = sh.pool.lease().await? else {
            if sh.pool.retired().await? == sh.pool.len() {
                stop(
                    sh,
                    format!(
                        "all {} egress IPs retired after {} strikes in a row (bot checks, 429s, proxy errors); last: {}",
                        sh.pool.len(),
                        sh.cfg.bot_check_limit,
                        sh.last_strike.borrow()
                    ),
                );
            }
            return Ok(None);
        };
        let label = lease.egress.label.clone();
        let dir = tempfile::tempdir()?;
        let outcome = match dl
            .download(entry, dir.path(), lease.egress.url.as_deref())
            .await
        {
            Ok(o) => Outcome {
                stderr: lease.egress.redact(&o.stderr),
                ..o
            },
            Err(e) => {
                sh.pool.release(lease, &v.id, Use::Failed).await?;
                return Err(e);
            }
        };
        let media = if outcome.success {
            find_media(dir.path(), &v.id)?
        } else {
            None
        };
        let (how, result) = match (outcome.success, media) {
            (true, Some(media)) => {
                let bytes = local_bytes(dir.path(), &v.id)?;
                let how = Use::Fetched {
                    bytes,
                    duration_s: v.duration_s,
                };
                // The IP is done once the files are local; it rests while we upload.
                sh.pool.release(lease, &v.id, how).await?;
                sh.error_streak.set(0);
                let bytes =
                    upload(sh, entry, dir.path(), &media, batch, shard, &record_path).await?;
                let mut r = sh.report.borrow_mut();
                r.fetched += 1;
                r.bytes += bytes;
                eprintln!(
                    "fetch: {} ok via {label}, {:.1} MB",
                    v.id,
                    bytes as f64 / 1e6
                );
                return Ok(Some(line("fetched", None, bytes)));
            }
            // Exit 0 without a file: the page did not show a Creative Commons license. If that
            // happens to every video, YouTube changed its page, so it counts toward the streak.
            (true, None) => (
                Use::Failed,
                error(
                    sh,
                    line("failed", Some("page license check failed".into()), 0),
                ),
            ),
            (false, _) => match classify(&outcome.stderr) {
                failure @ (Failure::BotCheck(_) | Failure::Proxy(_)) => {
                    let (how, reason) = match failure {
                        Failure::Proxy(r) => (Use::ProxyError, r),
                        Failure::BotCheck(r) => (Use::BotCheck, r),
                        _ => unreachable!(),
                    };
                    let note = sh
                        .pool
                        .release(lease, &v.id, how)
                        .await?
                        .unwrap_or_default();
                    sh.report.borrow_mut().strikes += 1;
                    eprintln!("fetch: {} via {label}: {reason}; {note}", v.id);
                    *sh.last_strike.borrow_mut() = reason;
                    continue; // the same video, on another IP or after the bench
                }
                Failure::Unavailable(reason) => {
                    sh.report.borrow_mut().unavailable += 1;
                    eprintln!("fetch: {} unavailable: {reason}", v.id);
                    (Use::Unavailable, line("unavailable", Some(reason), 0))
                }
                Failure::Error(reason) => (Use::Failed, error(sh, line("failed", Some(reason), 0))),
            },
        };
        sh.pool.release(lease, &v.id, how).await?;
        if sh.stop.borrow().is_some() {
            return Ok(None);
        }
        return Ok(Some(result));
    }
}

/// Bytes of the media and captions yt-dlp left for `id`.
fn local_bytes(dir: &Path, id: &str) -> Result<u64> {
    let mut total = 0;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{id}.")) && !name.ends_with(".part") {
            total += e.metadata()?.len();
        }
    }
    Ok(total)
}

fn error<P: IpPool>(sh: &Shared<'_, P>, line: ManifestLine) -> ManifestLine {
    let n = sh.error_streak.get() + 1;
    sh.error_streak.set(n);
    sh.report.borrow_mut().failed += 1;
    eprintln!(
        "fetch: {} failed: {}",
        line.id,
        line.reason.as_deref().unwrap_or("")
    );
    if n >= sh.cfg.error_limit {
        stop(
            sh,
            format!(
                "{n} failures in a row; last: {}",
                line.reason.as_deref().unwrap_or("")
            ),
        );
    }
    line
}

fn stop<P: IpPool>(sh: &Shared<'_, P>, why: String) {
    eprintln!("fetch: stopping: {why}");
    sh.stop.borrow_mut().get_or_insert(why);
    sh.pool.close();
}

/// The downloaded media file: <id>.<ext>, not a caption or partial file.
fn find_media(dir: &Path, id: &str) -> Result<Option<PathBuf>> {
    let mut best: Option<(u64, PathBuf)> = None;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(ext) = name.strip_prefix(&format!("{id}.")) else {
            continue;
        };
        if ext.contains('.') || matches!(ext, "vtt" | "part" | "ytdl" | "json" | "temp") {
            continue;
        }
        let len = e.metadata()?.len();
        if len > 0 && best.as_ref().is_none_or(|(l, _)| len > *l) {
            best = Some((len, e.path()));
        }
    }
    Ok(best.map(|(_, p)| p))
}

/// Uploads media, then captions, then the record. Returns the bytes uploaded.
async fn upload<P: IpPool>(
    sh: &Shared<'_, P>,
    entry: &Entry,
    dir: &Path,
    media: &Path,
    batch: &str,
    shard: usize,
    record_path: &str,
) -> Result<u64> {
    let v = &entry.video;
    let prefix = video_dir(&v.id);
    let mut local = vec![media.to_path_buf()];
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        let name = p.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with(&format!("{}.", v.id)) && name.ends_with(".vtt") {
            local.push(p);
        }
    }
    let mut files = vec![];
    for p in local {
        let rel = format!(
            "{prefix}/{}",
            p.file_name().unwrap_or_default().to_string_lossy()
        );
        let (bytes, sha256) = sh.store.upload(&p, &rel).await?;
        files.push(File {
            path: rel,
            bytes,
            sha256,
        });
    }
    let record = Record {
        id: v.id.clone(),
        url: v.url(),
        title: v.title.clone(),
        channel_id: v.channel_id.clone(),
        channel_title: v.channel_title.clone(),
        channel_url: format!("https://www.youtube.com/channel/{}", v.channel_id),
        license: v.license.clone(),
        published_at: v.published_at.clone(),
        duration_s: v.duration_s,
        batch: batch.into(),
        shard,
        worker: sh.worker.clone(),
        fetched_at: Utc::now().to_rfc3339(),
        files,
    };
    sh.store
        .put(record_path, serde_json::to_vec_pretty(&record)?)
        .await?;
    Ok(record.files.iter().map(|f| f.bytes).sum())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::egress::{self, Egress, Health, Pool};
    use crate::plan::tests::video;

    /// Plays a script per video id; the last step repeats. Records every call and the proxy it
    /// came through. Proxies whose URL contains "bad" always get a bot check.
    #[derive(Default)]
    struct FakeDl {
        script: HashMap<String, Vec<Step>>,
        calls: RefCell<Vec<String>>,
        proxies: RefCell<Vec<Option<String>>>,
    }

    #[derive(Clone, Copy)]
    enum Step {
        Ok,
        LicenseMiss,
        Fail(&'static str),
    }

    impl FakeDl {
        fn on(mut self, id: &str, steps: &[Step]) -> Self {
            self.script.insert(id.into(), steps.to_vec());
            self
        }
    }

    impl Downloader for FakeDl {
        async fn download(
            &self,
            entry: &Entry,
            dir: &Path,
            proxy: Option<&str>,
        ) -> Result<Outcome> {
            let id = &entry.video.id;
            let n = self.calls.borrow().iter().filter(|c| *c == id).count();
            self.calls.borrow_mut().push(id.clone());
            self.proxies.borrow_mut().push(proxy.map(str::to_string));
            if proxy.is_some_and(|p| p.contains("bad")) {
                return Ok(Outcome {
                    success: false,
                    stderr: format!("ERROR: [youtube] {id}: Sign in to confirm you're not a bot"),
                });
            }
            let steps = self
                .script
                .get(id)
                .map(Vec::as_slice)
                .unwrap_or(&[Step::Ok]);
            let step = steps[n.min(steps.len() - 1)];
            Ok(match step {
                Step::Ok => {
                    std::fs::write(dir.join(format!("{id}.mp4")), format!("video {id}"))?;
                    std::fs::write(dir.join(format!("{id}.en.vtt")), "WEBVTT")?;
                    Outcome {
                        success: true,
                        stderr: String::new(),
                    }
                }
                Step::LicenseMiss => Outcome {
                    success: true,
                    stderr: String::new(),
                },
                Step::Fail(msg) => Outcome {
                    success: false,
                    stderr: format!(
                        "WARNING: x via {}\nERROR: [youtube] {id}: {msg} (via {})\n",
                        proxy.unwrap_or("-"),
                        proxy.unwrap_or("-")
                    ),
                },
            })
        }
    }

    fn cfg() -> Fetch {
        Fetch {
            pause_min_s: 0.0,
            pause_max_s: 0.0,
            bot_check_backoff_s: 0,
            ..Fetch::default()
        }
    }

    fn pool(list: Vec<Egress>, c: &Fetch) -> Pool {
        Pool::new(list, c, Health::memory().unwrap(), "test".into()).unwrap()
    }

    /// fetch::run through the machine's own address, as before proxies.
    async fn run_direct<D: Downloader>(
        c: &Fetch,
        store: &Store,
        dl: &D,
        o: &Options,
    ) -> Result<Report> {
        run(c, store, dl, &pool(vec![Egress::direct()], c), o).await
    }

    fn opts(rank: usize, world: usize, jobs: usize) -> Options {
        Options {
            rank,
            world,
            jobs,
            batch: None,
        }
    }

    /// A store with one batch of `n` videos in shards of `size`.
    async fn setup(n: usize, size: usize) -> (tempfile::TempDir, Store, String) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().to_str().unwrap()).unwrap();
        let videos: Vec<_> = (0..n)
            .map(|i| (video(&format!("vid{i:08}")), "search:q".to_string()))
            .collect();
        let b = plan::write_batch(&store, &videos, size).await.unwrap();
        (dir, store, b.batch)
    }

    async fn manifest(store: &Store, batch: &str, k: usize) -> Vec<ManifestLine> {
        let Some(bytes) = store.get(&plan::manifest_path(batch, k)).await.unwrap() else {
            return vec![];
        };
        bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect()
    }

    #[test]
    fn classifies_stderr() {
        let bot = "ERROR: [youtube] abc: Sign in to confirm you\u{2019}re not a bot. Use --cookies";
        assert!(matches!(classify(bot), Failure::BotCheck(r) if r.starts_with("ERROR:")));
        assert!(matches!(
            classify("ERROR: unable to download: HTTP Error 429: Too Many Requests"),
            Failure::BotCheck(_)
        ));
        assert!(matches!(
            classify("ERROR: [youtube] x: Video unavailable. This video is private"),
            Failure::Unavailable(_)
        ));
        assert!(matches!(
            classify(
                "ERROR: [youtube] x: The uploader has not made this video available in your country"
            ),
            Failure::Unavailable(_)
        ));
        assert!(matches!(
            classify(
                "ERROR: [youtube] x: Unable to download API page: ('Unable to connect to proxy', OSError('Tunnel connection failed: 403 Forbidden'))"
            ),
            Failure::Proxy(_)
        ));
        assert!(matches!(
            classify("ERROR: [youtube] x: Postprocessing: Conversion failed!"),
            Failure::Error(_)
        ));
        assert!(matches!(
            classify("ERROR: [youtube] x: Join this channel to get access to members-only content"),
            Failure::Unavailable(_)
        ));
        assert_eq!(classify(""), Failure::Error("no output".into()));
        assert_eq!(
            classify("Traceback\nKeyError: 'x'\n"),
            Failure::Error("KeyError: 'x'".into())
        );
    }

    #[tokio::test]
    async fn fetches_everything_and_resumes_exactly() {
        let (_dir, store, batch) = setup(5, 2).await;
        let dl = FakeDl::default();
        let r = run_direct(&cfg(), &store, &dl, &opts(0, 1, 2))
            .await
            .unwrap();
        assert_eq!((r.shards, r.fetched, r.failed), (3, 5, 0));
        let id = "vid00000003";
        let rec: Record = serde_json::from_slice(
            &store
                .get(&format!("videos/vi/{id}.json"))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rec.files.len(), 2);
        assert_eq!(rec.files[0].path, format!("videos/vi/{id}.mp4"));
        assert_eq!(rec.files[1].path, format!("videos/vi/{id}.en.vtt"));
        assert_eq!(rec.files[0].bytes, format!("video {id}").len() as u64);
        assert_eq!(rec.batch, batch);
        assert_eq!(manifest(&store, &batch, 2).await.len(), 1);

        // Every shard has a manifest, so a rerun does nothing.
        let dl2 = FakeDl::default();
        let r = run_direct(&cfg(), &store, &dl2, &opts(0, 1, 2))
            .await
            .unwrap();
        assert_eq!(r.shards, 0);
        assert!(dl2.calls.borrow().is_empty());
    }

    #[tokio::test]
    async fn workers_split_shards() {
        let (_dir, store, batch) = setup(6, 1).await;
        let dl = FakeDl::default();
        let r = run_direct(&cfg(), &store, &dl, &opts(1, 3, 1))
            .await
            .unwrap();
        assert_eq!(r.shards, 2);
        assert_eq!(*dl.calls.borrow(), vec!["vid00000001", "vid00000004"]);
        assert!(store.exists(&plan::manifest_path(&batch, 4)).await.unwrap());
        assert!(!store.exists(&plan::manifest_path(&batch, 0)).await.unwrap());

        let dl = FakeDl::default();
        run_direct(&cfg(), &store, &dl, &opts(0, 2, 1))
            .await
            .unwrap(); // a different split: 0, 2, 4
        assert_eq!(*dl.calls.borrow(), vec!["vid00000000", "vid00000002"]); // 4 already done
    }

    #[tokio::test]
    async fn records_failures_in_the_manifest() {
        let (_dir, store, batch) = setup(3, 3).await;
        let dl = FakeDl::default()
            .on(
                "vid00000000",
                &[Step::Fail(
                    "Private video. Sign in if you've been granted access",
                )],
            )
            .on("vid00000001", &[Step::LicenseMiss]);
        let r = run_direct(&cfg(), &store, &dl, &opts(0, 1, 1))
            .await
            .unwrap();
        assert_eq!((r.fetched, r.unavailable, r.failed), (1, 1, 1));
        let m = manifest(&store, &batch, 0).await;
        let status: Vec<_> = m.iter().map(|l| l.status.as_str()).collect();
        assert_eq!(status, vec!["unavailable", "failed", "fetched"]);
        assert_eq!(m[1].reason.as_deref(), Some("page license check failed"));
        assert!(!store.exists("videos/vi/vid00000001.json").await.unwrap());
    }

    #[tokio::test]
    async fn backs_off_on_bot_checks_then_recovers() {
        let (_dir, store, _) = setup(2, 2).await;
        let bot = Step::Fail("Sign in to confirm you're not a bot");
        let dl = FakeDl::default().on("vid00000000", &[bot, bot, Step::Ok]);
        let r = run_direct(&cfg(), &store, &dl, &opts(0, 1, 1))
            .await
            .unwrap();
        assert_eq!(r.fetched, 2);
        assert_eq!(dl.calls.borrow().len(), 4);
    }

    #[tokio::test]
    async fn stops_after_bot_check_limit_and_leaves_shard_open() {
        let (_dir, store, batch) = setup(4, 2).await;
        let dl = FakeDl::default().on(
            "vid00000001",
            &[Step::Fail("HTTP Error 429: Too Many Requests")],
        );
        let e = run_direct(&cfg(), &store, &dl, &opts(0, 1, 1))
            .await
            .unwrap_err();
        assert!(
            e.to_string().contains("retired after 3 strikes in a row"),
            "{e}"
        );
        assert_eq!(dl.calls.borrow().len(), 4); // vid0 once, vid1 three times, then stop
        assert!(!store.exists(&plan::manifest_path(&batch, 0)).await.unwrap());

        // Next run: vid0 is skipped by its record, vid1 now works.
        let dl = FakeDl::default();
        let r = run_direct(&cfg(), &store, &dl, &opts(0, 1, 1))
            .await
            .unwrap();
        assert_eq!((r.skipped, r.fetched, r.shards), (1, 3, 2));
        assert_eq!(dl.calls.borrow()[0], "vid00000001");
        assert_eq!(manifest(&store, &batch, 0).await[0].status, "fetched");
    }

    #[tokio::test]
    async fn stops_after_error_limit() {
        let (_dir, store, _) = setup(10, 10).await;
        let mut dl = FakeDl::default();
        for i in 0..10 {
            dl = dl.on(&format!("vid{i:08}"), &[Step::LicenseMiss]);
        }
        let c = Fetch {
            error_limit: 4,
            ..cfg()
        };
        let e = run_direct(&c, &store, &dl, &opts(0, 1, 1))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("4 failures in a row"), "{e}");
        assert_eq!(dl.calls.borrow().len(), 4);
    }

    #[tokio::test]
    async fn rotates_across_proxies() {
        let (_dir, store, _) = setup(6, 6).await;
        let list =
            egress::parse_list("a http://10.0.0.1:1\nb http://10.0.0.2:1\nc http://10.0.0.3:1\n")
                .unwrap();
        let dl = FakeDl::default();
        let r = run(&cfg(), &store, &dl, &pool(list, &cfg()), &opts(0, 1, 1))
            .await
            .unwrap();
        assert_eq!(r.fetched, 6);
        let used: Vec<_> = dl
            .proxies
            .borrow()
            .iter()
            .map(|p| p.clone().unwrap())
            .collect();
        let want: Vec<_> = (0..6)
            .map(|i| format!("http://10.0.0.{}:1", i % 3 + 1))
            .collect();
        assert_eq!(used, want);
    }

    #[tokio::test]
    async fn retires_a_flagged_proxy_and_retries_elsewhere() {
        let (_dir, store, batch) = setup(4, 4).await;
        let list =
            egress::parse_list("bad http://bad.example:1\ngood http://good.example:1\n").unwrap();
        let c = Fetch {
            bot_check_limit: 2,
            ..cfg()
        };
        let health = Health::memory().unwrap();
        let p = Pool::new(list, &c, health, "test".into()).unwrap();
        let dl = FakeDl::default();
        let r = run(&c, &store, &dl, &p, &opts(0, 1, 1)).await.unwrap();
        assert_eq!((r.fetched, r.strikes, r.failed), (4, 2, 0));
        assert_eq!(p.retired().await.unwrap(), 1);
        // bad, good (v0); bad again (v1) retires it; then good for the rest.
        let used: Vec<_> = dl
            .proxies
            .borrow()
            .iter()
            .map(|p| p.clone().unwrap().contains("bad"))
            .collect();
        assert_eq!(used, vec![true, false, true, false, false, false]);
        assert!(
            manifest(&store, &batch, 0)
                .await
                .iter()
                .all(|l| l.status == "fetched")
        );
    }

    #[tokio::test]
    async fn stops_when_every_proxy_is_retired() {
        let (_dir, store, batch) = setup(2, 2).await;
        let list = egress::parse_list("x http://bad1:1\nx http://bad2:1\n").unwrap();
        let dl = FakeDl::default();
        let e = run(&cfg(), &store, &dl, &pool(list, &cfg()), &opts(0, 1, 1))
            .await
            .unwrap_err();
        assert!(
            e.to_string().contains("all 2 egress IPs retired after 3"),
            "{e}"
        );
        assert_eq!(dl.calls.borrow().len(), 6);
        assert!(!store.exists(&plan::manifest_path(&batch, 0)).await.unwrap());
    }

    #[tokio::test]
    async fn keeps_proxy_credentials_out_of_manifests() {
        let (_dir, store, batch) = setup(1, 1).await;
        let list = egress::parse_list("acme http://user:s3cret@gw.acme.net:7000\n").unwrap();
        let dl = FakeDl::default().on("vid00000000", &[Step::Fail("Postprocessing failed")]);
        run(&cfg(), &store, &dl, &pool(list, &cfg()), &opts(0, 1, 1))
            .await
            .unwrap();
        let m = manifest(&store, &batch, 0).await;
        let reason = m[0].reason.as_deref().unwrap();
        assert!(reason.contains("acme/gw.acme.net:7000#1"), "{reason}");
        assert!(!reason.contains("s3cret"), "{reason}");
    }

    /// A frontier with `n` accepted videos, and a scratch store.
    async fn queue_setup(n: usize) -> Option<(Frontier, tempfile::TempDir, Store)> {
        let f = crate::frontier::tests::frontier().await?;
        let ids: Vec<String> = (0..n).map(|i| format!("vid{i:08}")).collect();
        f.add_ids(&ids, "search:q").await.unwrap();
        for id in &ids {
            f.set_checked(&video(id), None).await.unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().to_str().unwrap()).unwrap();
        Some((f, dir, store))
    }

    fn queue_opts(owner: &str, jobs: usize) -> QueueOptions {
        QueueOptions {
            jobs,
            owner: owner.into(),
            limit: None,
        }
    }

    /// Two workers on one queue and one shared IP list fetch every video exactly once.
    #[tokio::test]
    async fn queue_workers_split_the_work() {
        let Some((f, _dir, store)) = queue_setup(8).await else {
            return;
        };
        let list = egress::parse_list(
            "a http://10.0.0.1:1\na http://10.0.0.2:1\nb http://10.0.0.3:1\nb http://10.0.0.4:1\n",
        )
        .unwrap();
        let c = cfg();
        let pa = egress::SharedPool::new(f.pool().clone(), list.clone(), &c, "a".into())
            .await
            .unwrap();
        let pb = egress::SharedPool::new(f.pool().clone(), list, &c, "b".into())
            .await
            .unwrap();
        let (da, db) = (FakeDl::default(), FakeDl::default());
        let (oa, ob) = (queue_opts("a", 2), queue_opts("b", 2));
        let (ra, rb) = tokio::join!(
            run_queue(&c, &store, &da, &pa, &f, &oa),
            run_queue(&c, &store, &db, &pb, &f, &ob),
        );
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert_eq!(ra.fetched + rb.fetched, 8);
        let mut calls: Vec<_> = da
            .calls
            .borrow()
            .iter()
            .chain(db.calls.borrow().iter())
            .cloned()
            .collect();
        calls.sort();
        calls.dedup();
        assert_eq!(calls.len(), 8);
        assert_eq!(
            da.calls.borrow().len() + db.calls.borrow().len(),
            8,
            "a video was fetched twice"
        );
        let p = f.progress().await.unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!((p[0].state.as_str(), p[0].videos), ("fetched", 8));
        assert!(store.exists("videos/vi/vid00000007.json").await.unwrap());
    }

    #[tokio::test]
    async fn queue_retries_failures_then_gives_up() {
        let Some((f, _dir, store)) = queue_setup(3).await else {
            return;
        };
        let c = Fetch {
            max_attempts: 2,
            retry_delay_s: 0,
            ..cfg()
        };
        let pool =
            egress::SharedPool::new(f.pool().clone(), vec![Egress::direct()], &c, "w".into())
                .await
                .unwrap();
        let dl = FakeDl::default()
            .on("vid00000000", &[Step::Fail("Postprocessing failed")])
            .on(
                "vid00000001",
                &[Step::Fail("Video unavailable. This video is private")],
            );
        let r = run_queue(&c, &store, &dl, &pool, &f, &queue_opts("w", 1))
            .await
            .unwrap();
        assert_eq!((r.fetched, r.unavailable, r.failed), (1, 1, 2));
        let tries = dl
            .calls
            .borrow()
            .iter()
            .filter(|c| *c == "vid00000000")
            .count();
        assert_eq!(tries, 2);
        let mut p: Vec<_> = f
            .progress()
            .await
            .unwrap()
            .into_iter()
            .map(|p| (p.state, p.videos))
            .collect();
        p.sort();
        assert_eq!(
            p,
            vec![
                ("failed".into(), 1),
                ("fetched".into(), 1),
                ("unavailable".into(), 1)
            ]
        );
    }

    #[test]
    fn picks_media_over_captions_and_partials() {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in [
            ("abc.en.vtt", "x"),
            ("abc.mp4.part", "xxxxxxxx"),
            ("abc.mp4", "xx"),
            ("other.mp4", "xxx"),
        ] {
            std::fs::write(dir.path().join(name), body).unwrap();
        }
        assert_eq!(
            find_media(dir.path(), "abc").unwrap().unwrap(),
            dir.path().join("abc.mp4")
        );
        assert!(find_media(dir.path(), "zzz").unwrap().is_none());
    }
}
