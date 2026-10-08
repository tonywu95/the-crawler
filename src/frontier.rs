//! The SQLite frontier: every video id discover has seen, the searches and channels it is working
//! through, and the quota spent per Pacific day.

use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};

use crate::api::Video;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS videos (
    id       TEXT PRIMARY KEY,
    source   TEXT NOT NULL,           -- search:<q> | channel:<id> | expand:<id> | seed | ids-file
    found_at TEXT NOT NULL,
    status   TEXT NOT NULL,           -- new | accepted | rejected | gone
    reason   TEXT,                    -- why rejected
    meta     TEXT,                    -- api::Video as JSON, once checked
    batch    TEXT                     -- the plan batch it went into
);
CREATE INDEX IF NOT EXISTS videos_status ON videos(status, batch);
CREATE TABLE IF NOT EXISTS searches (
    query    TEXT PRIMARY KEY,
    pages    INTEGER NOT NULL DEFAULT 0,
    next     TEXT,
    done     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS channels (
    channel  TEXT PRIMARY KEY,        -- as given: UC id or @handle
    kind     TEXT NOT NULL,           -- seed | expand
    resolved TEXT,                    -- the UC id, once resolved
    uploads  TEXT,                    -- uploads playlist, once resolved
    next     TEXT,
    found    INTEGER NOT NULL DEFAULT 0,
    done     INTEGER NOT NULL DEFAULT 0,
    note     TEXT
);
CREATE TABLE IF NOT EXISTS quota (
    day      TEXT PRIMARY KEY,        -- America/Los_Angeles date, when the API quota resets
    used     INTEGER NOT NULL DEFAULT 0,
    search   INTEGER NOT NULL DEFAULT 0
);
";

/// Today's date in the API's quota time zone.
pub fn quota_day() -> String {
    Utc::now()
        .with_timezone(&chrono_tz::America::Los_Angeles)
        .format("%Y-%m-%d")
        .to_string()
}

pub struct Frontier {
    db: Connection,
}

pub struct Channel {
    pub channel: String,
    pub kind: String,
    pub uploads: Option<String>,
    pub next: Option<String>,
    pub found: u32,
}

pub struct Search {
    pub query: String,
    pub pages: u32,
    pub next: Option<String>,
}

impl Frontier {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        Self::init(Connection::open(path)?)
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

    /// Adds ids not seen before as `new`. Returns how many were added.
    pub fn add_ids(&self, ids: &[String], source: &str) -> Result<usize> {
        let now = Utc::now().to_rfc3339();
        let tx = self.db.unchecked_transaction()?;
        let mut added = 0;
        {
            let mut st = tx.prepare(
                "INSERT OR IGNORE INTO videos (id, source, found_at, status) VALUES (?1, ?2, ?3, 'new')",
            )?;
            for id in ids {
                added += st.execute(params![id, source, now])?;
            }
        }
        tx.commit()?;
        Ok(added)
    }

    pub fn unchecked(&self, limit: usize) -> Result<Vec<String>> {
        let mut st = self
            .db
            .prepare("SELECT id FROM videos WHERE status = 'new' ORDER BY rowid LIMIT ?1")?;
        let ids = st
            .query_map([limit as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    }

    pub fn source(&self, id: &str) -> Result<Option<String>> {
        Ok(self
            .db
            .query_row("SELECT source FROM videos WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// Records a check: accepted when `rejection` is None.
    pub fn set_checked(&self, v: &Video, rejection: Option<&str>) -> Result<()> {
        let status = if rejection.is_some() {
            "rejected"
        } else {
            "accepted"
        };
        self.db.execute(
            "UPDATE videos SET status = ?2, reason = ?3, meta = ?4 WHERE id = ?1",
            params![v.id, status, rejection, serde_json::to_string(v)?],
        )?;
        Ok(())
    }

    pub fn set_gone(&self, id: &str) -> Result<()> {
        self.db
            .execute("UPDATE videos SET status = 'gone' WHERE id = ?1", [id])?;
        Ok(())
    }

    /// Accepted videos not yet in a batch: direct hits (search, seeds, seed channels) before
    /// channel-expansion hits, oldest find first.
    pub fn unplanned(&self, limit: Option<usize>) -> Result<Vec<(Video, String)>> {
        let mut st = self.db.prepare(
            "SELECT meta, source FROM videos WHERE status = 'accepted' AND batch IS NULL
             ORDER BY source LIKE 'expand:%', rowid LIMIT ?1",
        )?;
        let limit = limit.map_or(-1, |l| l as i64);
        let rows = st.query_map([limit], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = vec![];
        for row in rows {
            let (meta, source) = row?;
            out.push((serde_json::from_str(&meta)?, source));
        }
        Ok(out)
    }

    pub fn set_batch(&self, ids: &[String], batch: &str) -> Result<()> {
        let tx = self.db.unchecked_transaction()?;
        {
            let mut st = tx.prepare("UPDATE videos SET batch = ?2 WHERE id = ?1")?;
            for id in ids {
                st.execute(params![id, batch])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Adds a channel to crawl unless it is already known, by the name given or as a resolved
    /// handle. Returns whether it was added.
    pub fn add_channel(&self, channel: &str, kind: &str) -> Result<bool> {
        Ok(self.db.execute(
            "INSERT OR IGNORE INTO channels (channel, kind)
             SELECT ?1, ?2 WHERE NOT EXISTS (SELECT 1 FROM channels WHERE resolved = ?1)",
            params![channel, kind],
        )? > 0)
    }

    /// The next channel with uploads left to page through, seeds first.
    pub fn next_channel(&self) -> Result<Option<Channel>> {
        Ok(self
            .db
            .query_row(
                "SELECT channel, kind, uploads, next, found FROM channels WHERE done = 0
                 ORDER BY kind = 'expand', rowid LIMIT 1",
                [],
                |r| {
                    Ok(Channel {
                        channel: r.get(0)?,
                        kind: r.get(1)?,
                        uploads: r.get(2)?,
                        next: r.get(3)?,
                        found: r.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    /// Records what a channel resolved to. Another row for the same channel (a handle and its UC
    /// id both queued) is marked done so its uploads are crawled once.
    pub fn set_uploads(&self, channel: &str, id: &str, uploads: &str) -> Result<()> {
        self.db.execute(
            "UPDATE channels SET resolved = ?2, uploads = ?3 WHERE channel = ?1",
            params![channel, id, uploads],
        )?;
        self.db.execute(
            "UPDATE channels SET done = 1, note = 'same as ' || ?1
             WHERE channel != ?1 AND (channel = ?2 OR resolved = ?2) AND done = 0 AND uploads IS NULL",
            params![channel, id],
        )?;
        Ok(())
    }

    pub fn channel_page(&self, channel: &str, next: Option<&str>, found: u32) -> Result<()> {
        self.db.execute(
            "UPDATE channels SET next = ?2, found = found + ?3 WHERE channel = ?1",
            params![channel, next, found],
        )?;
        Ok(())
    }

    pub fn channel_done(&self, channel: &str, note: Option<&str>) -> Result<()> {
        self.db.execute(
            "UPDATE channels SET done = 1, note = ?2 WHERE channel = ?1",
            params![channel, note],
        )?;
        Ok(())
    }

    /// Searches still to run, in config order. New queries are added; existing ones keep progress.
    pub fn pending_searches(&self, queries: &[String]) -> Result<Vec<Search>> {
        let mut out = vec![];
        for q in queries {
            self.db
                .execute("INSERT OR IGNORE INTO searches (query) VALUES (?1)", [q])?;
            let s = self.db.query_row(
                "SELECT query, pages, next, done FROM searches WHERE query = ?1",
                [q],
                |r| {
                    Ok((
                        Search {
                            query: r.get(0)?,
                            pages: r.get(1)?,
                            next: r.get(2)?,
                        },
                        r.get::<_, bool>(3)?,
                    ))
                },
            )?;
            if !s.1 {
                out.push(s.0);
            }
        }
        Ok(out)
    }

    pub fn search_page(&self, query: &str, next: Option<&str>, done: bool) -> Result<()> {
        self.db.execute(
            "UPDATE searches SET pages = pages + 1, next = ?2, done = ?3 WHERE query = ?1",
            params![query, next, done],
        )?;
        Ok(())
    }

    /// (all units, search units) spent on `day`.
    pub fn quota(&self, day: &str) -> Result<(u64, u64)> {
        Ok(self
            .db
            .query_row(
                "SELECT used, search FROM quota WHERE day = ?1",
                [day],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .optional()?
            .unwrap_or((0, 0)))
    }

    pub fn add_quota(&self, day: &str, used: u64, search: u64) -> Result<()> {
        if used == 0 && search == 0 {
            return Ok(());
        }
        self.db.execute(
            "INSERT INTO quota (day, used, search) VALUES (?1, ?2, ?3)
             ON CONFLICT(day) DO UPDATE SET used = used + ?2, search = search + ?3",
            params![day, used as i64, search as i64],
        )?;
        Ok(())
    }

    /// Rows of (label, count) for status.
    pub fn counts(&self) -> Result<Vec<(String, i64)>> {
        let mut out = vec![];
        let mut q = |sql: &str| -> Result<()> {
            let mut st = self.db.prepare(sql)?;
            for row in st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
                out.push(row?);
            }
            Ok(())
        };
        q("SELECT 'videos ' || status, COUNT(*) FROM videos GROUP BY status ORDER BY status")?;
        q("SELECT 'accepted from ' || CASE WHEN source LIKE 'expand:%' THEN 'channel expansion'
                  WHEN source LIKE 'search:%' THEN 'search' WHEN source LIKE 'channel:%' THEN 'seed channels'
                  ELSE source END, COUNT(*)
           FROM videos WHERE status = 'accepted' GROUP BY 1 ORDER BY 1")?;
        q(
            "SELECT 'accepted, not planned', COUNT(*) FROM videos WHERE status = 'accepted' AND batch IS NULL",
        )?;
        q("SELECT 'rejected: ' || CASE WHEN instr(reason, ':') > 0 THEN substr(reason, 1, instr(reason, ':') - 1)
                  ELSE reason END, COUNT(*)
           FROM videos WHERE status = 'rejected' GROUP BY 1 ORDER BY 2 DESC")?;
        q(
            "SELECT 'searches ' || CASE done WHEN 1 THEN 'done' ELSE 'pending' END, COUNT(*) FROM searches GROUP BY done",
        )?;
        q("SELECT 'channels ' || kind || CASE done WHEN 1 THEN ' done' ELSE ' pending' END, COUNT(*)
           FROM channels GROUP BY kind, done")?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video(id: &str) -> Video {
        Video {
            id: id.into(),
            title: String::new(),
            channel_id: "c".into(),
            channel_title: String::new(),
            published_at: String::new(),
            duration_s: 60,
            license: "creativeCommon".into(),
            live: "none".into(),
            age_restricted: false,
            made_for_kids: false,
            privacy: "public".into(),
            language: None,
        }
    }

    #[test]
    fn ids_are_added_once() {
        let f = Frontier::memory().unwrap();
        assert_eq!(f.add_ids(&["a".into(), "b".into()], "seed").unwrap(), 2);
        assert_eq!(f.add_ids(&["b".into(), "c".into()], "search:x").unwrap(), 1);
        assert_eq!(f.source("b").unwrap().as_deref(), Some("seed"));
        assert_eq!(f.unchecked(10).unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn unplanned_puts_direct_hits_first() {
        let f = Frontier::memory().unwrap();
        f.add_ids(&["e1".into()], "expand:UC1").unwrap();
        f.add_ids(&["s1".into()], "search:q").unwrap();
        f.add_ids(&["r1".into()], "search:q").unwrap();
        for id in ["e1", "s1"] {
            f.set_checked(&video(id), None).unwrap();
        }
        f.set_checked(&video("r1"), Some("license:youtube"))
            .unwrap();
        let ids: Vec<_> = f
            .unplanned(None)
            .unwrap()
            .into_iter()
            .map(|(v, _)| v.id)
            .collect();
        assert_eq!(ids, vec!["s1", "e1"]);
        f.set_batch(&["s1".into()], "b1").unwrap();
        assert_eq!(f.unplanned(Some(5)).unwrap().len(), 1);
    }

    #[test]
    fn quota_accumulates_per_day() {
        let f = Frontier::memory().unwrap();
        f.add_quota("2026-10-07", 100, 100).unwrap();
        f.add_quota("2026-10-07", 3, 0).unwrap();
        assert_eq!(f.quota("2026-10-07").unwrap(), (103, 100));
        assert_eq!(f.quota("2026-10-08").unwrap(), (0, 0));
    }

    #[test]
    fn seeds_come_before_expansions() {
        let f = Frontier::memory().unwrap();
        f.add_channel("UCexp", "expand").unwrap();
        f.add_channel("@seed", "seed").unwrap();
        assert!(!f.add_channel("@seed", "expand").unwrap());
        assert_eq!(f.next_channel().unwrap().unwrap().channel, "@seed");
        f.channel_done("@seed", None).unwrap();
        assert_eq!(f.next_channel().unwrap().unwrap().channel, "UCexp");
    }

    #[test]
    fn a_handle_and_its_id_are_crawled_once() {
        let f = Frontier::memory().unwrap();
        f.add_channel("@seed", "seed").unwrap();
        f.add_channel("UC1", "expand").unwrap();
        f.set_uploads("@seed", "UC1", "UU1").unwrap();
        assert!(!f.add_channel("UC1", "expand").unwrap());
        f.channel_done("@seed", None).unwrap();
        assert!(f.next_channel().unwrap().is_none());
    }
}
