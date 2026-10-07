//! fetch: a worker downloads its share of every batch's shards with yt-dlp.
//!
//! Worker `rank` of `world` runs `jobs` slots; slot s = rank × jobs + j takes the shards whose
//! index i (across all batches, oldest first) has i % (world × jobs) == s. The slots share one
//! task and one IP, so a bot check seen by any of them pauses all of them.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::config::Fetch;
use crate::plan::{self, Entry};
use crate::store::Store;

/// What one yt-dlp run did. Files are whatever it left in the directory.
pub struct Outcome {
    pub success: bool,
    pub stderr: String,
}

pub trait Downloader {
    /// Downloads `entry` into the empty directory `dir`.
    fn download(&self, entry: &Entry, dir: &Path) -> impl Future<Output = Result<Outcome>>;
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
    async fn download(&self, entry: &Entry, dir: &Path) -> Result<Outcome> {
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
    if BOT.iter().any(|p| lower.contains(p)) {
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
    pub bytes: u64,
}

/// State the slots share. They run on one task, so plain cells suffice.
struct Shared<'a> {
    cfg: &'a Fetch,
    store: &'a Store,
    worker: String,
    stop: RefCell<Option<String>>,
    pause_until: Cell<Option<Instant>>,
    bot_streak: Cell<u32>,
    error_streak: Cell<u32>,
    report: RefCell<Report>,
}

pub async fn run<D: Downloader>(
    cfg: &Fetch,
    store: &Store,
    downloader: &D,
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
        stop: RefCell::new(None),
        pause_until: Cell::new(None),
        bot_streak: Cell::new(0),
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

async fn run_slot<D: Downloader>(
    sh: &Shared<'_>,
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
async fn fetch_one<D: Downloader>(
    sh: &Shared<'_>,
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
        if let Some(until) = sh.pause_until.get() {
            tokio::time::sleep_until(until).await;
        }
        if sh.stop.borrow().is_some() {
            return Ok(None);
        }
        let dir = tempfile::tempdir()?;
        let outcome = dl.download(entry, dir.path()).await?;
        let media = if outcome.success {
            find_media(dir.path(), &v.id)?
        } else {
            None
        };
        let result = match (outcome.success, media) {
            (true, Some(media)) => {
                sh.bot_streak.set(0);
                sh.error_streak.set(0);
                let bytes =
                    upload(sh, entry, dir.path(), &media, batch, shard, &record_path).await?;
                let mut r = sh.report.borrow_mut();
                r.fetched += 1;
                r.bytes += bytes;
                eprintln!("fetch: {} ok, {:.1} MB", v.id, bytes as f64 / 1e6);
                line("fetched", None, bytes)
            }
            // Exit 0 without a file: the page did not show a Creative Commons license. If that
            // happens to every video, YouTube changed its page, so it counts toward the streak.
            (true, None) => error(
                sh,
                line("failed", Some("page license check failed".into()), 0),
            ),
            (false, _) => match classify(&outcome.stderr) {
                Failure::BotCheck(reason) => {
                    let n = sh.bot_streak.get() + 1;
                    sh.bot_streak.set(n);
                    if n >= sh.cfg.bot_check_limit {
                        stop(sh, format!("{n} bot checks in a row; last: {reason}"));
                        return Ok(None);
                    }
                    let wait = Duration::from_secs(
                        sh.cfg
                            .bot_check_backoff_s
                            .saturating_mul(1 << (n - 1).min(16)),
                    );
                    eprintln!(
                        "fetch: {}: bot check ({reason}); all slots pause {wait:?}",
                        v.id
                    );
                    let until = Instant::now() + wait;
                    if sh.pause_until.get().is_none_or(|u| u < until) {
                        sh.pause_until.set(Some(until));
                    }
                    continue;
                }
                Failure::Unavailable(reason) => {
                    sh.bot_streak.set(0);
                    sh.report.borrow_mut().unavailable += 1;
                    eprintln!("fetch: {} unavailable: {reason}", v.id);
                    line("unavailable", Some(reason), 0)
                }
                Failure::Error(reason) => error(sh, line("failed", Some(reason), 0)),
            },
        };
        if sh.stop.borrow().is_some() {
            return Ok(None);
        }
        let (lo, hi) = (
            sh.cfg.pause_min_s,
            sh.cfg.pause_max_s.max(sh.cfg.pause_min_s),
        );
        tokio::time::sleep(Duration::from_secs_f64(rand::random_range(lo..=hi))).await;
        return Ok(Some(result));
    }
}

fn error(sh: &Shared<'_>, line: ManifestLine) -> ManifestLine {
    sh.bot_streak.set(0);
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

fn stop(sh: &Shared<'_>, why: String) {
    eprintln!("fetch: stopping: {why}");
    sh.stop.borrow_mut().get_or_insert(why);
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
async fn upload(
    sh: &Shared<'_>,
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
    use crate::frontier::Frontier;
    use crate::plan::tests::video;

    /// Plays a script per video id; the last step repeats. Records every call.
    #[derive(Default)]
    struct FakeDl {
        script: HashMap<String, Vec<Step>>,
        calls: RefCell<Vec<String>>,
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
        async fn download(&self, entry: &Entry, dir: &Path) -> Result<Outcome> {
            let id = &entry.video.id;
            let n = self.calls.borrow().iter().filter(|c| *c == id).count();
            self.calls.borrow_mut().push(id.clone());
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
                    stderr: format!("WARNING: x\nERROR: [youtube] {id}: {msg}\n"),
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
        let f = Frontier::memory().unwrap();
        let ids: Vec<String> = (0..n).map(|i| format!("vid{i:08}")).collect();
        f.add_ids(&ids, "search:q").unwrap();
        for id in &ids {
            f.set_checked(&video(id), None).unwrap();
        }
        let b = plan::run(&f, &store, size, None).await.unwrap().unwrap();
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
        let r = run(&cfg(), &store, &dl, &opts(0, 1, 2)).await.unwrap();
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
        let r = run(&cfg(), &store, &dl2, &opts(0, 1, 2)).await.unwrap();
        assert_eq!(r.shards, 0);
        assert!(dl2.calls.borrow().is_empty());
    }

    #[tokio::test]
    async fn workers_split_shards() {
        let (_dir, store, batch) = setup(6, 1).await;
        let dl = FakeDl::default();
        let r = run(&cfg(), &store, &dl, &opts(1, 3, 1)).await.unwrap();
        assert_eq!(r.shards, 2);
        assert_eq!(*dl.calls.borrow(), vec!["vid00000001", "vid00000004"]);
        assert!(store.exists(&plan::manifest_path(&batch, 4)).await.unwrap());
        assert!(!store.exists(&plan::manifest_path(&batch, 0)).await.unwrap());

        let dl = FakeDl::default();
        run(&cfg(), &store, &dl, &opts(0, 2, 1)).await.unwrap(); // a different split: 0, 2, 4
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
        let r = run(&cfg(), &store, &dl, &opts(0, 1, 1)).await.unwrap();
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
        let r = run(&cfg(), &store, &dl, &opts(0, 1, 1)).await.unwrap();
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
        let e = run(&cfg(), &store, &dl, &opts(0, 1, 1)).await.unwrap_err();
        assert!(e.to_string().contains("3 bot checks in a row"), "{e}");
        assert_eq!(dl.calls.borrow().len(), 4); // vid0 once, vid1 three times, then stop
        assert!(!store.exists(&plan::manifest_path(&batch, 0)).await.unwrap());

        // Next run: vid0 is skipped by its record, vid1 now works.
        let dl = FakeDl::default();
        let r = run(&cfg(), &store, &dl, &opts(0, 1, 1)).await.unwrap();
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
        let e = run(&c, &store, &dl, &opts(0, 1, 1)).await.unwrap_err();
        assert!(e.to_string().contains("4 failures in a row"), "{e}");
        assert_eq!(dl.calls.borrow().len(), 4);
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
