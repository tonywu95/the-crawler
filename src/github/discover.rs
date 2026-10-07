//! discover: turns seeds into checked repositories in the frontier, spending GraphQL points.
//!
//! Named repositories are looked up first, 100 to a query. Then each search and owner is paged
//! through, 100 repositories to a query, saving the cursor after every page.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::DateTime;

use super::api::{GitHub, NAMES_PER_QUERY, Repo};
use super::config::{Accept, Config, Verdict};
use super::frontier::{Frontier, Seed};

pub struct DiscoverArgs {
    pub seeds: PathBuf,
    pub db: PathBuf,
    pub api_url: String,
    pub token: String,
    /// Stop after spending this many points.
    pub max_points: Option<i64>,
    /// Least time between two queries.
    pub pace: Duration,
}

/// GitHub search returns at most this many results per query.
const SEARCH_CAP: i64 = 1000;
/// Searches are split by creation date, between this and the time of the first split.
const GITHUB_EPOCH: i64 = 1_191_196_800; // 2007-10-01

pub async fn run(args: &DiscoverArgs) -> Result<()> {
    let cfg = Config::load(&args.seeds)?;
    let names = cfg.repo_names()?;
    let mut db = Frontier::open(&args.db)?;
    let added = db.add_seeds(&cfg.searches, &cfg.owners, &names)?;
    log!("{added} new seeds from {}", args.seeds.display());

    let gh = GitHub::new(&args.api_url, &args.token, args.pace)?;
    let mut d = Discover {
        gh,
        db,
        accept: cfg.accept,
        max_points: args.max_points,
    };
    let finished = d.run().await?;

    let s = d.db.stats()?;
    log!(
        "{} points spent; frontier has {} accepted ({} not in a batch), {} rejected",
        d.gh.points_used,
        s.accepted,
        s.unplanned,
        s.rejected
    );
    if !finished {
        log!("stopped at --max-points; rerun to continue where this left off");
    }
    Ok(())
}

struct Discover {
    gh: GitHub,
    db: Frontier,
    accept: Accept,
    max_points: Option<i64>,
}

impl Discover {
    /// Works through every pending name and seed. False if it stopped for the points budget.
    async fn run(&mut self) -> Result<bool> {
        loop {
            let names = self.db.pending_names(NAMES_PER_QUERY)?;
            if names.is_empty() {
                break;
            }
            if self.out_of_points() {
                return Ok(false);
            }
            let found = self.gh.repos_by_name(&names).await?;
            let results: Vec<_> = names
                .into_iter()
                .zip(found)
                .map(|(name, found)| (name, found.map(|repo| self.judge(repo))))
                .collect();
            let accepted = results
                .iter()
                .filter(|(_, r)| matches!(r, Ok((_, Ok(())))))
                .count();
            let missing = results.iter().filter(|(_, r)| r.is_err()).count();
            self.db.save_names(&results)?;
            log!(
                "looked up {} names: {accepted} accepted, {missing} not found",
                results.len()
            );
        }
        while let Some(seed) = self.db.next_seed()? {
            if self.out_of_points() {
                return Ok(false);
            }
            match seed.kind.as_str() {
                "search" => self.search_page(&seed).await?,
                "owner" => self.owner_page(&seed).await?,
                other => bail!("unknown seed kind {other:?}"),
            }
        }
        Ok(true)
    }

    fn out_of_points(&self) -> bool {
        self.max_points
            .is_some_and(|max| self.gh.points_used >= max)
    }

    fn judge(&self, repo: Repo) -> (Repo, Verdict) {
        let verdict = self.accept.check(&repo);
        (repo, verdict)
    }

    async fn search_page(&mut self, seed: &Seed) -> Result<()> {
        let q = search_query(seed);
        let page = self.gh.search(&q, seed.cursor.as_deref()).await?;
        let total = page.total.unwrap_or(0);
        let repos: Vec<_> = page.repos.into_iter().map(|r| self.judge(r)).collect();
        if seed.cursor.is_none() && total > SEARCH_CAP {
            if let Some(parts) = split_range(seed)? {
                log!("search {q:?} matches {total}: splitting it by creation date");
                // Split first: the narrower searches will find this page's repositories again.
                self.db.split_seed(seed, &parts)?;
                self.db.save_page(seed.id, &repos, None)?;
                return Ok(());
            }
            log!("search {q:?} matches {total}, and GitHub returns only the first {SEARCH_CAP}");
        }
        let accepted = repos.iter().filter(|(_, v)| v.is_ok()).count();
        let new = self.db.save_page(seed.id, &repos, page.next.as_deref())?;
        log!(
            "search {q:?}: {} repos, {accepted} accepted, {new} new (of {total})",
            repos.len()
        );
        Ok(())
    }

    async fn owner_page(&mut self, seed: &Seed) -> Result<()> {
        let Some(page) = self
            .gh
            .owner_repos(&seed.value, seed.cursor.as_deref(), self.accept.forks)
            .await?
        else {
            log!("owner {} not found", seed.value);
            self.db.save_page(seed.id, &[], None)?;
            return Ok(());
        };
        let repos: Vec<_> = page.repos.into_iter().map(|r| self.judge(r)).collect();
        let accepted = repos.iter().filter(|(_, v)| v.is_ok()).count();
        let new = self.db.save_page(seed.id, &repos, page.next.as_deref())?;
        log!(
            "owner {}: {} repos, {accepted} accepted, {new} new",
            seed.value,
            repos.len()
        );
        Ok(())
    }
}

/// The seed's query, limited to its creation-date range if it has one.
fn search_query(seed: &Seed) -> String {
    if seed.lo.is_empty() {
        seed.value.clone()
    } else {
        format!("{} created:{}..{}", seed.value, seed.lo, seed.hi)
    }
}

/// Halves a search's creation-date range (whole seconds, both ends inclusive). None if the query
/// sets its own `created:` or the range is a single second.
fn split_range(seed: &Seed) -> Result<Option<[(String, String); 2]>> {
    if seed.value.contains("created:") {
        return Ok(None);
    }
    let (lo, hi) = if seed.lo.is_empty() {
        (GITHUB_EPOCH, chrono::Utc::now().timestamp())
    } else {
        (
            DateTime::parse_from_rfc3339(&seed.lo)?.timestamp(),
            DateTime::parse_from_rfc3339(&seed.hi)?.timestamp(),
        )
    };
    if hi <= lo {
        return Ok(None);
    }
    let mid = lo + (hi - lo) / 2;
    Ok(Some([
        (stamp(lo)?, stamp(mid)?),
        (stamp(mid + 1)?, stamp(hi)?),
    ]))
}

/// A time as GitHub search writes it: 2020-01-01T00:00:00+00:00.
fn stamp(secs: i64) -> Result<String> {
    let Some(t) = DateTime::from_timestamp(secs, 0) else {
        bail!("time {secs} out of range")
    };
    Ok(t.format("%Y-%m-%dT%H:%M:%S+00:00").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(value: &str, lo: &str, hi: &str) -> Seed {
        Seed {
            id: 1,
            kind: "search".into(),
            value: value.into(),
            lo: lo.into(),
            hi: hi.into(),
            cursor: None,
        }
    }

    #[test]
    fn splits_searches_by_creation_date() {
        let s = seed(
            "license:mit",
            "2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:09+00:00",
        );
        assert_eq!(
            search_query(&s),
            "license:mit created:2020-01-01T00:00:00+00:00..2020-01-01T00:00:09+00:00"
        );
        let [a, b] = split_range(&s).unwrap().unwrap();
        assert_eq!(
            a,
            (
                "2020-01-01T00:00:00+00:00".into(),
                "2020-01-01T00:00:04+00:00".into()
            )
        );
        assert_eq!(
            b,
            (
                "2020-01-01T00:00:05+00:00".into(),
                "2020-01-01T00:00:09+00:00".into()
            )
        );

        let root = split_range(&seed("license:mit", "", "")).unwrap().unwrap();
        assert_eq!(root[0].0, "2007-10-01T00:00:00+00:00");

        let one = seed(
            "x",
            "2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00+00:00",
        );
        assert!(split_range(&one).unwrap().is_none());
        assert!(
            split_range(&seed("x created:>2020-01-01", "", ""))
                .unwrap()
                .is_none()
        );
    }
}
