//! The SQLite frontier `discover` fills and `plan` drains. Every write that finishes a page of
//! results also saves where the next page starts, so an interrupted discover resumes exactly.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use super::api::Repo;
use super::config::Verdict;

/// A repository looked up by name, with its verdict, or why the lookup found nothing.
pub type Lookup = Result<(Repo, Verdict), String>;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS seeds (
    id     INTEGER PRIMARY KEY,
    kind   TEXT NOT NULL,              -- 'search' or 'owner'
    value  TEXT NOT NULL,              -- the query, or the user or organization login
    lo     TEXT NOT NULL DEFAULT '',   -- a search split by date covers created:lo..hi
    hi     TEXT NOT NULL DEFAULT '',
    cursor TEXT,                       -- where the next page starts
    done   INTEGER NOT NULL DEFAULT 0,
    UNIQUE (kind, value, lo, hi)
);
CREATE TABLE IF NOT EXISTS names (     -- repositories named in the seeds, looked up by name
    name   TEXT PRIMARY KEY COLLATE NOCASE,
    looked INTEGER NOT NULL DEFAULT 0,
    reason TEXT                        -- why the lookup found nothing
);
CREATE TABLE IF NOT EXISTS repos (
    id         INTEGER PRIMARY KEY,    -- GitHub's databaseId: stable across renames
    full_name  TEXT NOT NULL COLLATE NOCASE,
    stars      INTEGER NOT NULL,
    accepted   INTEGER NOT NULL,
    reason     TEXT,                   -- why it was rejected
    raw        TEXT NOT NULL,          -- the repository as GraphQL returned it
    batch      TEXT,                   -- set by plan
    checked_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS repos_by_name ON repos (full_name);
CREATE INDEX IF NOT EXISTS repos_to_plan ON repos (accepted, batch, stars);
";

pub struct Frontier {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct Seed {
    pub id: i64,
    pub kind: String,
    pub value: String,
    pub lo: String,
    pub hi: String,
    pub cursor: Option<String>,
}

#[derive(Debug, Default)]
pub struct Stats {
    /// (kind, total, done)
    pub seeds: Vec<(String, i64, i64)>,
    pub names_pending: i64,
    pub names_missing: i64,
    pub accepted: i64,
    pub unplanned: i64,
    pub rejected: i64,
    /// The most common rejection reasons, with counts.
    pub reasons: Vec<(String, i64)>,
}

impl Frontier {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Adds seeds not already in the frontier. Returns how many were new.
    pub fn add_seeds(
        &mut self,
        searches: &[String],
        owners: &[String],
        names: &[String],
    ) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let mut added = 0;
        {
            let mut seed =
                tx.prepare("INSERT OR IGNORE INTO seeds (kind, value) VALUES (?1, ?2)")?;
            for q in searches {
                added += seed.execute(params!["search", q.trim()])?;
            }
            for login in owners {
                added += seed.execute(params!["owner", login.trim()])?;
            }
            let mut name = tx.prepare(
                "INSERT OR IGNORE INTO names (name) SELECT ?1 WHERE NOT EXISTS (SELECT 1 FROM repos WHERE full_name = ?1)",
            )?;
            for n in names {
                added += name.execute([n])?;
            }
        }
        tx.commit()?;
        Ok(added)
    }

    /// The oldest seed not yet done.
    pub fn next_seed(&self) -> Result<Option<Seed>> {
        self.conn
            .query_row("SELECT id, kind, value, lo, hi, cursor FROM seeds WHERE done = 0 ORDER BY id LIMIT 1", [], |r| {
                Ok(Seed { id: r.get(0)?, kind: r.get(1)?, value: r.get(2)?, lo: r.get(3)?, hi: r.get(4)?, cursor: r.get(5)? })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Records a page of a seed's results, and where the next page starts (None: the seed is done).
    /// Returns how many repositories were new.
    pub fn save_page(
        &mut self,
        seed: i64,
        repos: &[(Repo, Verdict)],
        next: Option<&str>,
    ) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let added = insert_repos(&tx, repos)?;
        tx.execute(
            "UPDATE seeds SET cursor = ?2, done = ?3 WHERE id = ?1",
            params![seed, next, next.is_none()],
        )?;
        tx.commit()?;
        Ok(added)
    }

    /// Replaces a search with narrower ones, each covering part of its creation-date range.
    pub fn split_seed(&mut self, seed: &Seed, parts: &[(String, String)]) -> Result<()> {
        let tx = self.conn.transaction()?;
        for (lo, hi) in parts {
            tx.execute(
                "INSERT OR IGNORE INTO seeds (kind, value, lo, hi) VALUES (?1, ?2, ?3, ?4)",
                params![seed.kind, seed.value, lo, hi],
            )?;
        }
        tx.execute("UPDATE seeds SET done = 1 WHERE id = ?1", [seed.id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn pending_names(&self, limit: usize) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM names WHERE looked = 0 ORDER BY rowid LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    /// Records the results of looking up `names`: a repository and its verdict, or why none was found.
    pub fn save_names(&mut self, results: &[(String, Lookup)]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let found: Vec<_> = results
            .iter()
            .filter_map(|(_, r)| r.as_ref().ok().cloned())
            .collect();
        let added = insert_repos(&tx, &found)?;
        for (name, result) in results {
            let reason = result.as_ref().err();
            tx.execute(
                "UPDATE names SET looked = 1, reason = ?2 WHERE name = ?1",
                params![name, reason],
            )?;
        }
        tx.commit()?;
        Ok(added)
    }

    /// Accepted repositories no batch has taken yet, most-starred first.
    pub fn unplanned(&self, limit: Option<usize>) -> Result<Vec<Repo>> {
        let limit = limit.map_or(-1, |l| l as i64);
        let mut stmt = self
            .conn
            .prepare("SELECT raw FROM repos WHERE accepted = 1 AND batch IS NULL ORDER BY stars DESC, id LIMIT ?1")?;
        let rows = stmt.query_map([limit], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for raw in rows {
            out.push(serde_json::from_str(&raw?).context("parsing a stored repository")?);
        }
        Ok(out)
    }

    pub fn mark_planned(&mut self, ids: &[i64], batch: &str) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE repos SET batch = ?2 WHERE id = ?1")?;
            for id in ids {
                stmt.execute(params![id, batch])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn stats(&self) -> Result<Stats> {
        let c = &self.conn;
        let count = |sql: &str| -> Result<i64> { Ok(c.query_row(sql, [], |r| r.get(0))?) };
        let mut seeds =
            c.prepare("SELECT kind, COUNT(*), SUM(done) FROM seeds GROUP BY kind ORDER BY kind")?;
        let seeds = seeds
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut reasons = c.prepare(
            "SELECT reason, COUNT(*) AS n FROM repos WHERE accepted = 0 GROUP BY reason ORDER BY n DESC LIMIT 10",
        )?;
        let reasons = reasons
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(Stats {
            seeds,
            names_pending: count("SELECT COUNT(*) FROM names WHERE looked = 0")?,
            names_missing: count("SELECT COUNT(*) FROM names WHERE reason IS NOT NULL")?,
            accepted: count("SELECT COUNT(*) FROM repos WHERE accepted = 1")?,
            unplanned: count("SELECT COUNT(*) FROM repos WHERE accepted = 1 AND batch IS NULL")?,
            rejected: count("SELECT COUNT(*) FROM repos WHERE accepted = 0")?,
            reasons,
        })
    }
}

/// Adds repositories not seen before; the first verdict on a repository stands.
fn insert_repos(conn: &Connection, repos: &[(Repo, Verdict)]) -> Result<usize> {
    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO repos (id, full_name, stars, accepted, reason, raw, checked_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    let now = chrono::Utc::now().to_rfc3339();
    let mut added = 0;
    for (repo, verdict) in repos {
        let Some(id) = repo.database_id else { continue };
        added += stmt.execute(params![
            id,
            repo.name_with_owner,
            repo.stargazer_count,
            verdict.is_ok(),
            verdict.as_ref().err(),
            serde_json::to_string(repo)?,
            now,
        ])?;
    }
    Ok(added)
}
