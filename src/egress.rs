//! Egress IPs for fetch: a proxy list, a pool that leases one IP per video, and a SQLite table of
//! each IP's health and every download attempt, kept on the worker.
//!
//! An IP serves one video at a time and rests between videos. A bot check, a 429 or a proxy
//! failure benches it (backoff doubling per strike in a row); `bot_check_limit` strikes in a row
//! retire it until `fetch --reset-egress`. With no proxy list the pool holds one entry, the
//! machine's own address, so a single worker behaves as before: pause, back off, then stop.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Notify;
use tokio::time::Instant;
use url::Url;

use crate::config::Fetch;

/// One way out: a proxy, or the machine's own address (`url` None).
#[derive(Clone)]
pub struct Egress {
    /// `provider/host:port`: safe to log and to store. The URL is not (it holds credentials).
    pub label: String,
    pub provider: String,
    pub url: Option<String>,
}

impl std::fmt::Debug for Egress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label)
    }
}

impl Egress {
    pub fn direct() -> Self {
        Self {
            label: "direct".into(),
            provider: "direct".into(),
            url: None,
        }
    }

    /// Replaces the proxy URL and its password wherever they appear (yt-dlp may echo them).
    pub fn redact(&self, text: &str) -> String {
        let Some(url) = &self.url else {
            return text.to_string();
        };
        let mut out = text.replace(url.as_str(), &self.label);
        if let Some(pw) = Url::parse(url)
            .ok()
            .and_then(|u| u.password().map(str::to_string))
            && !pw.is_empty()
        {
            out = out.replace(&pw, "***");
        }
        out
    }
}

/// Reads a proxy list: one per line, `<provider> <url>` or just `<url>` (provider "default").
/// URLs are http://, https://, socks5:// or socks5h://, with credentials if the provider needs
/// them. Blank lines and # comments are skipped.
pub fn load_list(path: &Path) -> Result<Vec<Egress>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let list = parse_list(&text).with_context(|| format!("in {}", path.display()))?;
    if list.is_empty() {
        bail!("{} lists no proxies", path.display());
    }
    Ok(list)
}

pub fn parse_list(text: &str) -> Result<Vec<Egress>> {
    let mut out: Vec<Egress> = vec![];
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (provider, raw) = match line.split_once(char::is_whitespace) {
            Some((p, u)) => (p.to_string(), u.trim()),
            None => ("default".to_string(), line),
        };
        // Never echo the line: it may hold a password.
        let url =
            Url::parse(raw).map_err(|_| anyhow::anyhow!("line {}: not a proxy URL", n + 1))?;
        if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h") {
            bail!(
                "line {}: scheme {} is not http, https, socks5 or socks5h",
                n + 1,
                url.scheme()
            );
        }
        let (Some(host), Some(port)) = (url.host_str(), url.port_or_known_default()) else {
            bail!("line {}: proxy URL needs a host and port", n + 1);
        };
        // Some providers hand out one gateway host:port and pick the IP from the username
        // (a session id), so the user name is part of the identity, though not of the label.
        let label = if url.username().is_empty() {
            format!("{provider}/{host}:{port}")
        } else {
            let k = out
                .iter()
                .filter(|e| e.label.starts_with(&format!("{provider}/{host}:{port}")))
                .count();
            format!("{provider}/{host}:{port}#{}", k + 1)
        };
        if out.iter().any(|e| e.label == label) {
            bail!("line {}: {label} is listed twice", n + 1);
        }
        out.push(Egress {
            label,
            provider,
            url: Some(raw.to_string()),
        });
    }
    Ok(out)
}

/// How one leased attempt ended.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Use {
    Fetched {
        bytes: u64,
        duration_s: u64,
    },
    /// The video can't be had; the IP did its job.
    Unavailable,
    /// Some other failure, not the IP's fault as far as we can tell.
    Failed,
    /// "Not a bot", 429, rate limited: a strike against the IP.
    BotCheck,
    /// The proxy itself failed (connect, tunnel, auth): also a strike.
    ProxyError,
}

impl Use {
    fn name(self) -> &'static str {
        match self {
            Use::Fetched { .. } => "fetched",
            Use::Unavailable => "unavailable",
            Use::Failed => "failed",
            Use::BotCheck => "bot_check",
            Use::ProxyError => "proxy_error",
        }
    }

    fn strike(self) -> bool {
        matches!(self, Use::BotCheck | Use::ProxyError)
    }
}

/// The health table and attempt log, in a SQLite file on the worker.
pub struct Health {
    db: Connection,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS ips (
    label         TEXT PRIMARY KEY,
    provider      TEXT NOT NULL,
    strikes       INTEGER NOT NULL DEFAULT 0,   -- bot checks or proxy errors in a row
    benched_until INTEGER NOT NULL DEFAULT 0,   -- unix seconds
    retired       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS attempts (
    ts         INTEGER NOT NULL,               -- unix seconds, when the download ended
    worker     TEXT NOT NULL,
    label      TEXT NOT NULL,
    provider   TEXT NOT NULL,
    video      TEXT NOT NULL,
    outcome    TEXT NOT NULL,                  -- fetched | unavailable | failed | bot_check | proxy_error
    bytes      INTEGER NOT NULL,
    seconds    REAL NOT NULL,                  -- time in yt-dlp
    duration_s INTEGER NOT NULL                -- video length, when fetched
);
CREATE INDEX IF NOT EXISTS attempts_provider ON attempts(provider, outcome);
";

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

impl Health {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        Self::init(Connection::open(path).with_context(|| format!("opening {}", path.display()))?)
    }

    #[cfg(test)]
    pub fn memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(db: Connection) -> Result<Self> {
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.execute_batch(SCHEMA)?;
        Ok(Self { db })
    }

    /// Forgets every strike, bench and retirement; the attempt log stays.
    pub fn reset(&self) -> Result<()> {
        self.db.execute("DELETE FROM ips", [])?;
        Ok(())
    }

    fn load(&self, label: &str) -> Result<Option<(u32, i64, bool)>> {
        Ok(self
            .db
            .query_row(
                "SELECT strikes, benched_until, retired FROM ips WHERE label = ?1",
                [label],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?)
    }

    fn save(&self, e: &Egress, strikes: u32, benched_until: i64, retired: bool) -> Result<()> {
        self.db.execute(
            "INSERT INTO ips (label, provider, strikes, benched_until, retired) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(label) DO UPDATE SET strikes = ?3, benched_until = ?4, retired = ?5",
            params![e.label, e.provider, strikes, benched_until, retired],
        )?;
        Ok(())
    }

    fn record(&self, worker: &str, e: &Egress, video: &str, how: Use, seconds: f64) -> Result<()> {
        let (bytes, duration_s) = match how {
            Use::Fetched { bytes, duration_s } => (bytes as i64, duration_s as i64),
            _ => (0, 0),
        };
        self.db.execute(
            "INSERT INTO attempts (ts, worker, label, provider, video, outcome, bytes, seconds, duration_s)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![unix_now(), worker, e.label, e.provider, video, how.name(), bytes, seconds, duration_s],
        )?;
        Ok(())
    }

    /// Per-provider numbers for comparing vendors: what the proxy trial is for. `prices` is
    /// USD per IP per month, by provider, for the $ per hour column.
    pub fn report(&self, prices: &BTreeMap<String, f64>) -> Result<String> {
        let mut st = self.db.prepare(
            "SELECT provider, COUNT(DISTINCT label), COUNT(*),
                    SUM(outcome = 'fetched'), SUM(outcome IN ('bot_check', 'proxy_error')),
                    SUM(bytes), SUM(CASE WHEN outcome = 'fetched' THEN seconds ELSE 0 END),
                    SUM(duration_s), MIN(ts), MAX(ts),
                    (SELECT COUNT(*) FROM ips WHERE ips.provider = attempts.provider AND retired = 1)
             FROM attempts GROUP BY provider ORDER BY provider",
        )?;
        let rows = st.query_map([], |r| {
            Ok(ProviderRow {
                provider: r.get(0)?,
                ips: r.get(1)?,
                attempts: r.get(2)?,
                fetched: r.get(3)?,
                strikes: r.get(4)?,
                bytes: r.get(5)?,
                seconds: r.get(6)?,
                duration_s: r.get(7)?,
                span_s: (r.get::<_, i64>(9)? - r.get::<_, i64>(8)?) as f64,
                retired: r.get(10)?,
            })
        })?;
        format_report(rows.collect::<rusqlite::Result<Vec<_>>>()?, prices)
    }
}

/// One provider's totals from the attempt log.
pub struct ProviderRow {
    pub provider: String,
    pub ips: i64,
    pub retired: i64,
    pub attempts: i64,
    pub fetched: i64,
    pub strikes: i64,
    pub bytes: i64,
    /// Seconds in yt-dlp across fetched attempts.
    pub seconds: f64,
    pub duration_s: i64,
    /// From the first attempt to the last.
    pub span_s: f64,
}

/// The vendor comparison the proxy trial is for. `prices` is USD per IP per month, by
/// provider, for the $ per hour column.
pub fn format_report(rows: Vec<ProviderRow>, prices: &BTreeMap<String, f64>) -> Result<String> {
    let mut out = String::new();
    writeln!(
        out,
        "  {:<14} {:>4} {:>7} {:>7} {:>8} {:>7} {:>7} {:>9} {:>8} {:>8}",
        "provider",
        "ips",
        "retired",
        "tries",
        "fetched",
        "strike%",
        "MB/s",
        "/IP/day",
        "hours",
        "$/hour"
    )?;
    for r in rows {
        let days = (r.span_s / 86_400.0).max(1.0 / 24.0);
        let hours = r.duration_s as f64 / 3600.0;
        let cost = prices
            .get(&r.provider)
            .filter(|_| hours > 0.0)
            .map(|p| format!("{:.3}", r.ips as f64 * p / 30.0 * days / hours))
            .unwrap_or_else(|| "-".into());
        writeln!(
            out,
            "  {:<14} {:>4} {:>7} {:>7} {:>8} {:>6.1}% {:>7.1} {:>9.0} {:>8.1} {:>8}",
            r.provider,
            r.ips,
            r.retired,
            r.attempts,
            r.fetched,
            100.0 * r.strikes as f64 / r.attempts.max(1) as f64,
            if r.seconds > 0.0 {
                r.bytes as f64 / 1e6 / r.seconds
            } else {
                0.0
            },
            r.fetched as f64 / r.ips.max(1) as f64 / days,
            hours,
            cost
        )?;
    }
    Ok(out)
}

/// One IP's state in the pool.
struct Ip {
    egress: Egress,
    busy: bool,
    retired: bool,
    strikes: u32,
    rest_until: Instant,
    last_used: Option<Instant>,
    /// Lease start times in the last hour, for the hourly cap.
    starts: VecDeque<Instant>,
}

#[derive(Debug, PartialEq)]
enum Pick {
    Take(usize),
    /// Nothing free before this time (None: nothing free until a lease is released).
    Wait(Option<Instant>),
    AllRetired,
}

const HOUR: Duration = Duration::from_secs(3600);

/// The least recently used IP that is free, rested and under its hourly cap.
fn pick(ips: &mut [Ip], now: Instant, cap: u32) -> Pick {
    if ips.iter().all(|ip| ip.retired) {
        return Pick::AllRetired;
    }
    let mut best: Option<(usize, Option<Instant>)> = None;
    let mut wait: Option<Instant> = None;
    for (i, ip) in ips.iter_mut().enumerate() {
        while ip
            .starts
            .front()
            .is_some_and(|t| now.duration_since(*t) >= HOUR)
        {
            ip.starts.pop_front();
        }
        if ip.busy || ip.retired {
            continue;
        }
        let capped = cap > 0 && ip.starts.len() >= cap as usize;
        let free_at = if capped {
            ip.rest_until.max(ip.starts[0] + HOUR)
        } else {
            ip.rest_until
        };
        if free_at > now {
            wait = Some(wait.map_or(free_at, |w| w.min(free_at)));
        } else if best.is_none_or(|(_, last)| ip.last_used < last) {
            best = Some((i, ip.last_used)); // None (never used) sorts first
        }
    }
    match best {
        Some((i, _)) => Pick::Take(i),
        None => Pick::Wait(wait),
    }
}

/// A leased IP. Give it back with `IpPool::release`.
pub struct Lease {
    index: usize,
    started: Instant,
    pub egress: Egress,
}

/// A source of egress IPs: this worker's own (`Pool`) or one shared by many workers through
/// Postgres (`SharedPool`).
pub trait IpPool {
    /// Waits for a free, rested IP. None when the pool is closed or every IP is retired.
    fn lease(&self) -> impl Future<Output = Result<Option<Lease>>>;
    /// Returns an IP: it rests after any outcome, and a strike benches or retires it. Returns
    /// a note for the log when it was benched or retired.
    fn release(
        &self,
        lease: Lease,
        video: &str,
        how: Use,
    ) -> impl Future<Output = Result<Option<String>>>;
    /// Makes every waiting `lease` return None.
    fn close(&self);
    fn len(&self) -> usize;
    fn retired(&self) -> impl Future<Output = Result<usize>>;
}

/// How an IP is paced and punished, the same for both pools.
#[derive(Clone)]
struct Rules {
    pause: (f64, f64),
    backoff: Duration,
    backoff_max: Duration,
    limit: u32,
    cap: u32,
}

impl Rules {
    fn new(cfg: &Fetch) -> Self {
        Self {
            pause: (cfg.pause_min_s, cfg.pause_max_s.max(cfg.pause_min_s)),
            backoff: Duration::from_secs(cfg.bot_check_backoff_s),
            backoff_max: Duration::from_secs(
                cfg.bot_check_backoff_max_s.max(cfg.bot_check_backoff_s),
            ),
            limit: cfg.bot_check_limit.max(1),
            cap: cfg.max_videos_per_ip_per_hour,
        }
    }

    /// After an attempt by an IP with `strikes` so far: (strikes, rest, retired, note).
    fn after(&self, label: &str, strikes: u32, how: Use) -> (u32, Duration, bool, Option<String>) {
        if !how.strike() {
            let (lo, hi) = self.pause;
            return (
                0,
                Duration::from_secs_f64(rand::random_range(lo..=hi)),
                false,
                None,
            );
        }
        let strikes = strikes + 1;
        if strikes >= self.limit {
            let note = format!("{label} retired after {strikes} strikes in a row");
            return (strikes, Duration::ZERO, true, Some(note));
        }
        let wait = self
            .backoff
            .saturating_mul(1 << (strikes - 1).min(16))
            .min(self.backoff_max);
        (
            strikes,
            wait,
            false,
            Some(format!("{label} benched for {wait:?}")),
        )
    }
}

pub struct Pool {
    ips: RefCell<Vec<Ip>>,
    released: Notify,
    closed: Cell<bool>,
    health: Health,
    worker: String,
    rules: Rules,
}

impl Pool {
    /// Restores each IP's strikes, bench and retirement from `health`.
    pub fn new(list: Vec<Egress>, cfg: &Fetch, health: Health, worker: String) -> Result<Self> {
        let (now, unix) = (Instant::now(), unix_now());
        let mut ips = vec![];
        for egress in list {
            let (strikes, until, retired) = health.load(&egress.label)?.unwrap_or((0, 0, false));
            let rest_until = now + Duration::from_secs((until - unix).max(0) as u64);
            ips.push(Ip {
                egress,
                busy: false,
                retired,
                strikes,
                rest_until,
                last_used: None,
                starts: VecDeque::new(),
            });
        }
        Ok(Self {
            ips: RefCell::new(ips),
            released: Notify::new(),
            closed: Cell::new(false),
            health,
            worker,
            rules: Rules::new(cfg),
        })
    }
}

impl IpPool for Pool {
    fn len(&self) -> usize {
        self.ips.borrow().len()
    }

    async fn retired(&self) -> Result<usize> {
        Ok(self.ips.borrow().iter().filter(|ip| ip.retired).count())
    }

    fn close(&self) {
        self.closed.set(true);
        self.released.notify_waiters();
    }

    async fn lease(&self) -> Result<Option<Lease>> {
        loop {
            if self.closed.get() {
                return Ok(None);
            }
            let now = Instant::now();
            let picked = pick(&mut self.ips.borrow_mut(), now, self.rules.cap);
            let wait = match picked {
                Pick::AllRetired => return Ok(None),
                Pick::Take(i) => {
                    let mut ips = self.ips.borrow_mut();
                    let ip = &mut ips[i];
                    ip.busy = true;
                    ip.last_used = Some(now);
                    ip.starts.push_back(now);
                    return Ok(Some(Lease {
                        index: i,
                        started: now,
                        egress: ip.egress.clone(),
                    }));
                }
                Pick::Wait(w) => w,
            };
            // Created before awaiting, with no await in between, so a release can't be missed.
            let released = self.released.notified();
            match wait {
                Some(t) => tokio::select! {
                    _ = tokio::time::sleep_until(t) => {}
                    _ = released => {}
                },
                None => released.await,
            }
        }
    }

    async fn release(&self, lease: Lease, video: &str, how: Use) -> Result<Option<String>> {
        let now = Instant::now();
        let seconds = now.duration_since(lease.started).as_secs_f64();
        let (strikes, rest, retired, note) = {
            let mut ips = self.ips.borrow_mut();
            let ip = &mut ips[lease.index];
            ip.busy = false;
            let (strikes, rest, retired, note) =
                self.rules.after(&ip.egress.label, ip.strikes, how);
            ip.strikes = strikes;
            ip.retired = retired;
            ip.rest_until = now + rest;
            (strikes, rest, retired, note)
        };
        self.health.save(
            &lease.egress,
            strikes,
            unix_now() + rest.as_secs() as i64,
            retired,
        )?;
        self.health
            .record(&self.worker, &lease.egress, video, how, seconds)?;
        self.released.notify_waiters();
        Ok(note)
    }
}

/// An IP pool shared by every worker through Postgres. Workers list the same proxies in their
/// own files (the URLs, with credentials, never reach the database); Postgres holds each IP's
/// lease, rest, strikes and retirement by label, and the attempt log.
pub struct SharedPool {
    db: deadpool_postgres::Pool,
    list: Vec<Egress>,
    labels: Vec<String>,
    owner: String,
    closed: Cell<bool>,
    rules: Rules,
    /// How long an IP lease lasts if its worker dies holding it.
    hold: Duration,
}

impl SharedPool {
    /// Registers the listed IPs (keeping any state they already have).
    pub async fn new(
        db: deadpool_postgres::Pool,
        list: Vec<Egress>,
        cfg: &Fetch,
        owner: String,
    ) -> Result<Self> {
        let labels: Vec<String> = list.iter().map(|e| e.label.clone()).collect();
        let providers: Vec<String> = list.iter().map(|e| e.provider.clone()).collect();
        db.get()
            .await?
            .execute(
                "INSERT INTO ips (label, provider) SELECT * FROM unnest($1::text[], $2::text[])
                 ON CONFLICT (label) DO NOTHING",
                &[&labels, &providers],
            )
            .await?;
        Ok(Self {
            db,
            list,
            labels,
            owner,
            closed: Cell::new(false),
            rules: Rules::new(cfg),
            hold: Duration::from_secs(3600),
        })
    }

    /// Forgets the strikes, benches and retirements of the listed IPs.
    pub async fn reset(&self) -> Result<()> {
        self.db
            .get()
            .await?
            .execute(
                "UPDATE ips SET strikes = 0, rest_until = now(), retired = false WHERE label = ANY($1)",
                &[&self.labels],
            )
            .await?;
        Ok(())
    }

    /// The vendor comparison from the shared attempt log.
    pub async fn report(
        db: &deadpool_postgres::Pool,
        prices: &BTreeMap<String, f64>,
    ) -> Result<String> {
        let rows = db
            .get()
            .await?
            .query(
                "SELECT provider, ips, retired, tries, fetched, strikes, bytes, fetch_seconds,
                        duration_s, span_s::float8
                 FROM egress_by_provider ORDER BY provider",
                &[],
            )
            .await?;
        let rows = rows
            .iter()
            .map(|r| ProviderRow {
                provider: r.get(0),
                ips: r.get(1),
                retired: r.get(2),
                attempts: r.get(3),
                fetched: r.get(4),
                strikes: r.get(5),
                bytes: r.get(6),
                seconds: r.get(7),
                duration_s: r.get(8),
                span_s: r.get::<_, Option<f64>>(9).unwrap_or(0.0),
            })
            .collect();
        format_report(rows, prices)
    }
}

impl IpPool for SharedPool {
    fn len(&self) -> usize {
        self.list.len()
    }

    async fn retired(&self) -> Result<usize> {
        let row = self
            .db
            .get()
            .await?
            .query_one(
                "SELECT COUNT(*) FROM ips WHERE label = ANY($1) AND retired",
                &[&self.labels],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as usize)
    }

    fn close(&self) {
        self.closed.set(true);
    }

    async fn lease(&self) -> Result<Option<Lease>> {
        loop {
            if self.closed.get() {
                return Ok(None);
            }
            let c = self.db.get().await?;
            let row = c
                .query_opt(
                    "UPDATE ips SET lease_owner = $1, last_used = now(),
                                    lease_until = now() + make_interval(secs => $4)
                     WHERE label = (
                         SELECT label FROM ips
                         WHERE label = ANY($2) AND NOT retired AND rest_until <= now()
                           AND (lease_until IS NULL OR lease_until < now())
                           AND ($3::bigint = 0 OR (SELECT COUNT(*) FROM attempts a
                                           WHERE a.label = ips.label
                                             AND a.ts > now() - interval '1 hour') < $3::bigint)
                         ORDER BY last_used NULLS FIRST, label
                         LIMIT 1 FOR UPDATE SKIP LOCKED)
                     RETURNING label",
                    &[
                        &self.owner,
                        &self.labels,
                        &(self.rules.cap as i64),
                        &self.hold.as_secs_f64(),
                    ],
                )
                .await?;
            if let Some(row) = row {
                let label: String = row.get(0);
                let index = self
                    .labels
                    .iter()
                    .position(|l| *l == label)
                    .expect("listed");
                return Ok(Some(Lease {
                    index,
                    started: Instant::now(),
                    egress: self.list[index].clone(),
                }));
            }
            // Nothing free: wait for the soonest rested IP, polling at least every 5 s since
            // other workers release IPs too.
            let row = c
                .query_one(
                    "SELECT COUNT(*) FILTER (WHERE NOT retired),
                            EXTRACT(EPOCH FROM MIN(rest_until - now())
                                    FILTER (WHERE NOT retired AND rest_until > now()))::float8
                     FROM ips WHERE label = ANY($1)",
                    &[&self.labels],
                )
                .await?;
            drop(c);
            if row.get::<_, i64>(0) == 0 {
                return Ok(None);
            }
            let soonest = row.get::<_, Option<f64>>(1).unwrap_or(5.0);
            tokio::time::sleep(Duration::from_secs_f64(soonest.clamp(0.2, 5.0))).await;
        }
    }

    async fn release(&self, lease: Lease, video: &str, how: Use) -> Result<Option<String>> {
        let seconds = lease.started.elapsed().as_secs_f64();
        let mut c = self.db.get().await?;
        let tx = c.transaction().await?;
        let strikes: i32 = tx
            .query_one(
                "SELECT strikes FROM ips WHERE label = $1 FOR UPDATE",
                &[&lease.egress.label],
            )
            .await?
            .get(0);
        let (strikes, rest, retired, note) =
            self.rules.after(&lease.egress.label, strikes as u32, how);
        tx.execute(
            "UPDATE ips SET strikes = $2, rest_until = now() + make_interval(secs => $3),
                            retired = $4, lease_owner = NULL, lease_until = NULL
             WHERE label = $1",
            &[
                &lease.egress.label,
                &(strikes as i32),
                &rest.as_secs_f64(),
                &retired,
            ],
        )
        .await?;
        let (bytes, duration_s) = match how {
            Use::Fetched { bytes, duration_s } => (bytes as i64, duration_s as i64),
            _ => (0, 0),
        };
        tx.execute(
            "INSERT INTO attempts (worker, label, provider, video, outcome, bytes, seconds, duration_s)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &self.owner,
                &lease.egress.label,
                &lease.egress.provider,
                &video,
                &how.name(),
                &bytes,
                &seconds,
                &duration_s,
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(note)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Fetch {
        Fetch {
            pause_min_s: 0.0,
            pause_max_s: 0.0,
            bot_check_backoff_s: 0,
            ..Fetch::default()
        }
    }

    fn list(n: usize) -> Vec<Egress> {
        parse_list(
            &(0..n)
                .map(|i| format!("acme http://10.0.0.{i}:8080\n"))
                .collect::<String>(),
        )
        .unwrap()
    }

    #[test]
    fn parses_lists_without_leaking_credentials() {
        let l = parse_list(
            "# trial\nacme http://user:s3cret@gw.acme.net:7000\nacme http://user2:s3cret@gw.acme.net:7000\n\nsocks5://10.1.2.3:1080\n",
        )
        .unwrap();
        let labels: Vec<_> = l.iter().map(|e| e.label.as_str()).collect();
        assert_eq!(
            labels,
            vec![
                "acme/gw.acme.net:7000#1",
                "acme/gw.acme.net:7000#2",
                "default/10.1.2.3:1080"
            ]
        );
        assert_eq!(format!("{:?}", l[0]), "acme/gw.acme.net:7000#1");
        let msg =
            "ERROR: Unable to connect to proxy http://user:s3cret@gw.acme.net:7000 (pw s3cret)";
        let red = l[0].redact(msg);
        assert!(!red.contains("s3cret"), "{red}");

        let e = parse_list("acme ftp://user:hunter2@x:21\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("line 1") && !e.contains("hunter2"), "{e}");
        let e = parse_list("acme not a url hunter2\n")
            .unwrap_err()
            .to_string();
        assert!(!e.contains("hunter2"), "{e}");
        assert!(parse_list("http://a:1\nhttp://a:1\n").is_err());
    }

    #[test]
    fn picks_least_recently_used_and_respects_cap() {
        let now = Instant::now();
        let mk = |last: Option<u64>, starts: usize| Ip {
            egress: Egress::direct(),
            busy: false,
            retired: false,
            strikes: 0,
            rest_until: now,
            last_used: last.map(|s| now - Duration::from_secs(s)),
            starts: (0..starts)
                .map(|k| now - Duration::from_secs(60 * (k as u64 + 1)))
                .collect(),
        };
        let mut ips = vec![mk(Some(10), 0), mk(Some(50), 0), mk(None, 0)];
        assert_eq!(pick(&mut ips, now, 0), Pick::Take(2));
        ips[2].busy = true;
        assert_eq!(pick(&mut ips, now, 0), Pick::Take(1));
        // Under a cap of 1 per hour, IPs that started a video in the last hour wait.
        let mut ips = vec![mk(Some(10), 1), mk(Some(50), 1)];
        match pick(&mut ips, now, 1) {
            Pick::Wait(Some(t)) => assert!(t > now + Duration::from_secs(3000)),
            other => panic!("{other:?}"),
        }
        let mut ips = vec![Ip {
            retired: true,
            ..mk(None, 0)
        }];
        assert_eq!(pick(&mut ips, now, 0), Pick::AllRetired);
    }

    #[tokio::test]
    async fn strikes_bench_then_retire_and_persist() {
        let c = Fetch {
            bot_check_limit: 2,
            bot_check_backoff_s: 600,
            ..cfg()
        };
        let pool = Pool::new(list(2), &c, Health::memory().unwrap(), "w".into()).unwrap();
        let a = pool.lease().await.unwrap().unwrap();
        let note = pool.release(a, "v1", Use::BotCheck).await.unwrap().unwrap();
        assert!(note.contains("benched for 600s"), "{note}");
        // The benched IP is skipped; the other one serves.
        let b = pool.lease().await.unwrap().unwrap();
        assert_eq!(b.egress.label, "acme/10.0.0.1:8080");
        let fetched = Use::Fetched {
            bytes: 5_000_000,
            duration_s: 120,
        };
        pool.release(b, "v1", fetched).await.unwrap();

        // Strikes reach the limit: retired, and saved. (Skip the 600 s bench.)
        pool.ips.borrow_mut()[0].rest_until = Instant::now();
        let a = pool.lease().await.unwrap().unwrap();
        assert_eq!(a.egress.label, "acme/10.0.0.0:8080");
        let note = pool
            .release(a, "v2", Use::ProxyError)
            .await
            .unwrap()
            .unwrap();
        assert!(note.contains("retired after 2 strikes"), "{note}");
        assert_eq!(pool.retired().await.unwrap(), 1);
        let (strikes, _, retired) = pool.health.load("acme/10.0.0.0:8080").unwrap().unwrap();
        assert_eq!((strikes, retired), (2, true));

        // A new pool on the same health table starts with it retired.
        let Pool { health, .. } = pool;
        let pool = Pool::new(list(2), &c, health, "w".into()).unwrap();
        assert_eq!(pool.retired().await.unwrap(), 1);
        let report = pool
            .health
            .report(&BTreeMap::from([("acme".into(), 1.5)]))
            .unwrap();
        assert!(report.contains("acme"), "{report}");
        assert!(report.contains("66.7%"), "{report}"); // 2 strikes in 3 tries
        pool.health.reset().unwrap();
        let pool = Pool::new(list(2), &c, pool.health, "w".into()).unwrap();
        assert_eq!(pool.retired().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn closing_wakes_waiters() {
        let pool = Pool::new(list(1), &cfg(), Health::memory().unwrap(), "w".into()).unwrap();
        let held = pool.lease().await.unwrap();
        let (got, ()) = tokio::join!(pool.lease(), async { pool.close() });
        assert!(got.unwrap().is_none());
        drop(held);
    }

    /// Two workers sharing three IPs through Postgres never hold the same one, and a strike or
    /// retirement by one is seen by the other.
    #[tokio::test]
    async fn shared_pool_coordinates_workers() {
        let Some(db) = crate::db::test::pool().await else {
            return;
        };
        let c = Fetch {
            bot_check_limit: 2,
            bot_check_backoff_s: 600,
            ..cfg()
        };
        let a = SharedPool::new(db.clone(), list(3), &c, "a".into())
            .await
            .unwrap();
        let b = SharedPool::new(db.clone(), list(3), &c, "b".into())
            .await
            .unwrap();
        let x = a.lease().await.unwrap().unwrap();
        let y = b.lease().await.unwrap().unwrap();
        let z = a.lease().await.unwrap().unwrap();
        let mut held = vec![
            x.egress.label.clone(),
            y.egress.label.clone(),
            z.egress.label.clone(),
        ];
        held.sort();
        held.dedup();
        assert_eq!(held.len(), 3, "an IP was leased twice");

        // y gets a bot check: benched for 600 s, so after x and z come back only they serve.
        let y_label = y.egress.label.clone();
        let note = b.release(y, "v1", Use::BotCheck).await.unwrap().unwrap();
        assert!(note.contains("benched for 600s"), "{note}");
        a.release(
            x,
            "v2",
            Use::Fetched {
                bytes: 10,
                duration_s: 60,
            },
        )
        .await
        .unwrap();
        a.release(z, "v3", Use::Unavailable).await.unwrap();
        for _ in 0..4 {
            let l = b.lease().await.unwrap().unwrap();
            assert_ne!(l.egress.label, y_label);
            b.release(l, "v", Use::Failed).await.unwrap();
        }

        // A second strike on every IP retires them all; both workers then get None.
        db.get()
            .await
            .unwrap()
            .execute("UPDATE ips SET rest_until = now(), strikes = 1", &[])
            .await
            .unwrap();
        for _ in 0..3 {
            let l = a.lease().await.unwrap().unwrap();
            a.release(l, "v", Use::BotCheck).await.unwrap();
        }
        assert_eq!(a.retired().await.unwrap(), 3);
        assert!(b.lease().await.unwrap().is_none());
        let report = SharedPool::report(&db, &BTreeMap::new()).await.unwrap();
        assert!(report.contains("acme"), "{report}");
        b.reset().await.unwrap();
        assert_eq!(a.retired().await.unwrap(), 0);
    }
}
