//! status: frontier counts and quota (where the frontier is), and per-batch progress from manifests.

use std::fmt::Write;

use anyhow::Result;

use crate::config::Config;
use crate::fetch::ManifestLine;
use crate::frontier::{Frontier, quota_day};
use crate::plan;
use crate::store::Store;

pub async fn run(cfg: &Config, store: &Store) -> Result<String> {
    let mut out = String::new();
    match std::env::var("DATABASE_URL") {
        Ok(url) => {
            let f = Frontier::open(&url).await?;
            let day = quota_day();
            let (used, search) = f.quota(&day).await?;
            writeln!(out, "frontier (DATABASE_URL)")?;
            writeln!(
                out,
                "  quota {day} (Pacific): {used} / {} used, {search} / {} on search",
                cfg.discover.daily_quota, cfg.discover.search_quota
            )?;
            for (label, n) in f.counts().await? {
                writeln!(out, "  {label:<40} {n:>9}")?;
            }
            let egress =
                crate::egress::SharedPool::report(f.pool(), &cfg.fetch.provider_prices).await?;
            if egress.lines().count() > 1 {
                writeln!(out, "egress (shared, Postgres)")?;
                out.push_str(&egress);
            }
            let progress = f.progress().await?;
            if !progress.is_empty() {
                writeln!(out, "queue")?;
                for p in progress {
                    writeln!(
                        out,
                        "  {:<12} {:>9} videos {:>9.1} GB {:>9.1} h",
                        p.state,
                        p.videos,
                        p.bytes as f64 / 1e9,
                        p.duration_s as f64 / 3600.0
                    )?;
                }
            }
        }
        Err(_) => writeln!(out, "frontier: DATABASE_URL not set")?,
    }
    if cfg.fetch.egress_db.exists() {
        let h = crate::egress::Health::open(&cfg.fetch.egress_db)?;
        writeln!(
            out,
            "egress {} (this worker)",
            cfg.fetch.egress_db.display()
        )?;
        out.push_str(&h.report(&cfg.fetch.provider_prices)?);
    }
    writeln!(out, "store {}", store.url())?;
    let batches = plan::batches(store).await?;
    if batches.is_empty() {
        writeln!(out, "  no batches yet")?;
    }
    let mut totals = Totals::default();
    for b in &batches {
        let mut t = Totals {
            videos: b.videos,
            ..Totals::default()
        };
        for k in 0..b.shards {
            let Some(bytes) = store.get(&plan::manifest_path(&b.batch, k)).await? else {
                continue;
            };
            t.shards_done += 1;
            for line in bytes.split(|c| *c == b'\n').filter(|l| !l.is_empty()) {
                let m: ManifestLine = serde_json::from_slice(line)?;
                match m.status.as_str() {
                    "fetched" => {
                        t.fetched += 1;
                        t.bytes += m.bytes;
                        t.seconds += m.duration_s;
                    }
                    "unavailable" => t.unavailable += 1,
                    _ => t.failed += 1,
                }
            }
        }
        writeln!(
            out,
            "  {} {}/{} shards  {}",
            b.batch,
            t.shards_done,
            b.shards,
            t.line()
        )?;
        totals.add(&t);
    }
    if batches.len() > 1 {
        writeln!(out, "  total {}", totals.line())?;
    }
    Ok(out)
}

#[derive(Default)]
struct Totals {
    videos: usize,
    shards_done: usize,
    fetched: usize,
    unavailable: usize,
    failed: usize,
    bytes: u64,
    seconds: u64,
}

impl Totals {
    fn add(&mut self, o: &Totals) {
        self.videos += o.videos;
        self.fetched += o.fetched;
        self.unavailable += o.unavailable;
        self.failed += o.failed;
        self.bytes += o.bytes;
        self.seconds += o.seconds;
    }

    fn line(&self) -> String {
        format!(
            "{} videos: {} fetched, {} unavailable, {} failed; {:.1} GB, {:.1} h",
            self.videos,
            self.fetched,
            self.unavailable,
            self.failed,
            self.bytes as f64 / 1e9,
            self.seconds as f64 / 3600.0
        )
    }
}
