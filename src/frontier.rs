//! The frontier, in Postgres: every video id discover has seen, the searches and channels it is
//! working through, the quota spent per Pacific day, and the fetch queue that workers lease from.

use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use deadpool_postgres::Pool;

use crate::api::Video;

/// Today's date in the API's quota time zone.
pub fn quota_day() -> String {
    Utc::now()
        .with_timezone(&chrono_tz::America::Los_Angeles)
        .format("%Y-%m-%d")
        .to_string()
}

#[derive(Clone)]
pub struct Frontier {
    pool: Pool,
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

/// A video a worker holds a lease on.
pub struct Leased {
    pub video: Video,
    pub source: String,
    /// Including this one.
    pub attempts: i32,
}

/// One row of the fetch_progress view.
pub struct Progress {
    pub state: String,
    pub videos: i64,
    pub bytes: i64,
    pub duration_s: i64,
}

impl Frontier {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    pub async fn open(url: &str) -> Result<Self> {
        Ok(Self::new(crate::db::connect(url, None, 4).await?))
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    async fn client(&self) -> Result<deadpool_postgres::Client> {
        Ok(self.pool.get().await?)
    }

    /// Adds ids not seen before as `new`. Returns how many were added.
    pub async fn add_ids(&self, ids: &[String], source: &str) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let n = self
            .client()
            .await?
            .execute(
                "INSERT INTO videos (id, source)
                 SELECT DISTINCT ON (id) id, $2 FROM unnest($1::text[]) WITH ORDINALITY AS t(id, o)
                 ORDER BY id, o
                 ON CONFLICT (id) DO NOTHING",
                &[&ids, &source],
            )
            .await?;
        Ok(n as usize)
    }

    pub async fn unchecked(&self, limit: usize) -> Result<Vec<String>> {
        let rows = self
            .client()
            .await?
            .query(
                "SELECT id FROM videos WHERE status = 'new' ORDER BY seq LIMIT $1",
                &[&(limit as i64)],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    pub async fn source(&self, id: &str) -> Result<Option<String>> {
        let row = self
            .client()
            .await?
            .query_opt("SELECT source FROM videos WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    /// Records a check: accepted when `rejection` is None.
    pub async fn set_checked(&self, v: &Video, rejection: Option<&str>) -> Result<()> {
        let status = if rejection.is_some() {
            "rejected"
        } else {
            "accepted"
        };
        self.client()
            .await?
            .execute(
                "UPDATE videos SET status = $2, reason = $3, meta = $4::text::jsonb WHERE id = $1",
                &[&v.id, &status, &rejection, &serde_json::to_string(v)?],
            )
            .await?;
        Ok(())
    }

    pub async fn set_gone(&self, id: &str) -> Result<()> {
        self.client()
            .await?
            .execute("UPDATE videos SET status = 'gone' WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }

    /// Accepted videos in neither a batch nor the queue's results: direct hits (search, seeds,
    /// seed channels) before channel-expansion hits, oldest find first.
    pub async fn unplanned(&self, limit: Option<usize>) -> Result<Vec<(Video, String)>> {
        let limit = limit.map_or(i64::MAX, |l| l as i64);
        let rows = self
            .client()
            .await?
            .query(
                "SELECT meta::text, source FROM videos
                 WHERE status = 'accepted' AND batch IS NULL AND fetch_state IS NULL
                 ORDER BY source LIKE 'expand:%', seq LIMIT $1",
                &[&limit],
            )
            .await?;
        rows.iter()
            .map(|r| Ok((serde_json::from_str(r.get::<_, &str>(0))?, r.get(1))))
            .collect()
    }

    pub async fn set_batch(&self, ids: &[String], batch: &str) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE videos SET batch = $2 WHERE id = ANY($1)",
                &[&ids, &batch],
            )
            .await?;
        Ok(())
    }

    /// Adds a channel to crawl unless it is already known, by the name given or as a resolved
    /// handle. Returns whether it was added.
    pub async fn add_channel(&self, channel: &str, kind: &str) -> Result<bool> {
        let n = self
            .client()
            .await?
            .execute(
                "INSERT INTO channels (channel, kind)
                 SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM channels WHERE resolved = $1)
                 ON CONFLICT (channel) DO NOTHING",
                &[&channel, &kind],
            )
            .await?;
        Ok(n > 0)
    }

    /// The next channel with uploads left to page through, seeds first.
    pub async fn next_channel(&self) -> Result<Option<Channel>> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT channel, kind, uploads, next, found FROM channels WHERE NOT done
                 ORDER BY kind = 'expand', seq LIMIT 1",
                &[],
            )
            .await?;
        Ok(row.map(|r| Channel {
            channel: r.get(0),
            kind: r.get(1),
            uploads: r.get(2),
            next: r.get(3),
            found: r.get::<_, i32>(4) as u32,
        }))
    }

    /// Records what a channel resolved to. Another row for the same channel (a handle and its UC
    /// id both queued) is marked done so its uploads are crawled once.
    pub async fn set_uploads(&self, channel: &str, id: &str, uploads: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            "UPDATE channels SET resolved = $2, uploads = $3 WHERE channel = $1",
            &[&channel, &id, &uploads],
        )
        .await?;
        c.execute(
            "UPDATE channels SET done = true, note = 'same as ' || $1
             WHERE channel != $1 AND (channel = $2 OR resolved = $2) AND NOT done AND uploads IS NULL",
            &[&channel, &id],
        )
        .await?;
        Ok(())
    }

    pub async fn channel_page(&self, channel: &str, next: Option<&str>, found: u32) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE channels SET next = $2, found = found + $3 WHERE channel = $1",
                &[&channel, &next, &(found as i32)],
            )
            .await?;
        Ok(())
    }

    pub async fn channel_done(&self, channel: &str, note: Option<&str>) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE channels SET done = true, note = $2 WHERE channel = $1",
                &[&channel, &note],
            )
            .await?;
        Ok(())
    }

    /// Searches still to run, in config order. New queries are added; existing ones keep progress.
    pub async fn pending_searches(&self, queries: &[String]) -> Result<Vec<Search>> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO searches (query) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING",
            &[&queries],
        )
        .await?;
        let rows = c
            .query(
                "SELECT s.query, s.pages, s.next FROM unnest($1::text[]) WITH ORDINALITY AS q(query, o)
                 JOIN searches s USING (query) WHERE NOT s.done ORDER BY q.o",
                &[&queries],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| Search {
                query: r.get(0),
                pages: r.get::<_, i32>(1) as u32,
                next: r.get(2),
            })
            .collect())
    }

    pub async fn search_page(&self, query: &str, next: Option<&str>, done: bool) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE searches SET pages = pages + 1, next = $2, done = $3 WHERE query = $1",
                &[&query, &next, &done],
            )
            .await?;
        Ok(())
    }

    /// (all units, search units) spent on `day`.
    pub async fn quota(&self, day: &str) -> Result<(u64, u64)> {
        let row = self
            .client()
            .await?
            .query_opt("SELECT used, search FROM quota WHERE day = $1", &[&day])
            .await?;
        Ok(row.map_or((0, 0), |r| {
            (r.get::<_, i64>(0) as u64, r.get::<_, i64>(1) as u64)
        }))
    }

    pub async fn add_quota(&self, day: &str, used: u64, search: u64) -> Result<()> {
        if used == 0 && search == 0 {
            return Ok(());
        }
        self.client()
            .await?
            .execute(
                "INSERT INTO quota (day, used, search) VALUES ($1, $2, $3)
                 ON CONFLICT (day) DO UPDATE SET used = quota.used + $2, search = quota.search + $3",
                &[&day, &(used as i64), &(search as i64)],
            )
            .await?;
        Ok(())
    }

    /// Leases up to `n` queued videos to `owner` for `lease`: direct hits first, oldest first.
    /// A lease that runs out (the worker died) puts the video back in the queue.
    pub async fn lease(&self, owner: &str, n: usize, lease: Duration) -> Result<Vec<Leased>> {
        let rows = self
            .client()
            .await?
            .query(
                "WITH next AS (
                     SELECT id FROM videos
                     WHERE status = 'accepted' AND fetch_state IS NULL AND batch IS NULL
                       AND (lease_until IS NULL OR lease_until < now())
                     ORDER BY source LIKE 'expand:%', seq
                     LIMIT $2 FOR UPDATE SKIP LOCKED)
                 UPDATE videos v SET lease_owner = $1, attempts = v.attempts + 1,
                                     lease_until = now() + make_interval(secs => $3)
                 FROM next WHERE v.id = next.id
                 RETURNING v.meta::text, v.source, v.attempts, v.seq",
                &[&owner, &(n as i64), &lease.as_secs_f64()],
            )
            .await?;
        let mut out: Vec<(i64, Leased)> = rows
            .iter()
            .map(|r| {
                Ok((
                    r.get(3),
                    Leased {
                        video: serde_json::from_str(r.get::<_, &str>(0))?,
                        source: r.get(1),
                        attempts: r.get(2),
                    },
                ))
            })
            .collect::<Result<_>>()?;
        out.sort_by_key(|(seq, _)| *seq);
        Ok(out.into_iter().map(|(_, l)| l).collect())
    }

    /// Records a final outcome: fetched, unavailable or failed.
    pub async fn finish(
        &self,
        id: &str,
        state: &str,
        reason: Option<&str>,
        bytes: u64,
    ) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE videos SET fetch_state = $2, fetch_reason = $3, bytes = $4, fetched_at = now(),
                                   lease_owner = NULL, lease_until = NULL
                 WHERE id = $1",
                &[&id, &state, &reason, &(bytes as i64)],
            )
            .await?;
        Ok(())
    }

    /// Gives a lease back so the video can be tried again, by anyone, after `delay`.
    pub async fn requeue(
        &self,
        id: &str,
        owner: &str,
        reason: Option<&str>,
        delay: Duration,
    ) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE videos SET lease_owner = NULL, fetch_reason = $3,
                                   lease_until = now() + make_interval(secs => $4)
                 WHERE id = $1 AND lease_owner = $2",
                &[&id, &owner, &reason, &delay.as_secs_f64()],
            )
            .await?;
        Ok(())
    }

    /// Keeps a lease alive while its worker is still on the video.
    pub async fn extend(&self, id: &str, owner: &str, lease: Duration) -> Result<()> {
        self.client()
            .await?
            .execute(
                "UPDATE videos SET lease_until = now() + make_interval(secs => $3)
                 WHERE id = $1 AND lease_owner = $2",
                &[&id, &owner, &lease.as_secs_f64()],
            )
            .await?;
        Ok(())
    }

    pub async fn progress(&self) -> Result<Vec<Progress>> {
        let rows = self
            .client()
            .await?
            .query(
                "SELECT state, videos, bytes, duration_s FROM fetch_progress ORDER BY state",
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| Progress {
                state: r.get(0),
                videos: r.get(1),
                bytes: r.get(2),
                duration_s: r.get(3),
            })
            .collect())
    }

    /// Rows of (label, count) for status.
    pub async fn counts(&self) -> Result<Vec<(String, i64)>> {
        let c = self.client().await?;
        let mut out = vec![];
        for sql in [
            "SELECT 'videos ' || status, COUNT(*) FROM videos GROUP BY status ORDER BY status",
            "SELECT 'accepted from ' || CASE WHEN source LIKE 'expand:%' THEN 'channel expansion'
                    WHEN source LIKE 'search:%' THEN 'search' WHEN source LIKE 'channel:%' THEN 'seed channels'
                    ELSE source END AS k, COUNT(*)
             FROM videos WHERE status = 'accepted' GROUP BY k ORDER BY k",
            "SELECT 'accepted, not planned or queued-done', COUNT(*) FROM videos
             WHERE status = 'accepted' AND batch IS NULL AND fetch_state IS NULL",
            "SELECT 'rejected: ' || split_part(reason, ':', 1) AS k, COUNT(*)
             FROM videos WHERE status = 'rejected' GROUP BY k ORDER BY 2 DESC",
            "SELECT 'searches ' || CASE WHEN done THEN 'done' ELSE 'pending' END AS k, COUNT(*)
             FROM searches GROUP BY k",
            "SELECT 'channels ' || kind || CASE WHEN done THEN ' done' ELSE ' pending' END AS k, COUNT(*)
             FROM channels GROUP BY k ORDER BY k",
        ] {
            for r in c.query(sql, &[]).await? {
                out.push((r.get(0), r.get(1)));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub async fn frontier() -> Option<Frontier> {
        crate::db::test::pool().await.map(Frontier::new)
    }

    pub fn video(id: &str) -> Video {
        crate::plan::tests::video(id)
    }

    #[tokio::test]
    async fn ids_are_added_once() {
        let Some(f) = frontier().await else { return };
        assert_eq!(
            f.add_ids(&["a".into(), "b".into(), "a".into()], "seed")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            f.add_ids(&["b".into(), "c".into()], "search:x")
                .await
                .unwrap(),
            1
        );
        assert_eq!(f.source("b").await.unwrap().as_deref(), Some("seed"));
        assert_eq!(f.unchecked(10).await.unwrap(), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn unplanned_puts_direct_hits_first() {
        let Some(f) = frontier().await else { return };
        f.add_ids(&["e1".into()], "expand:UC1").await.unwrap();
        f.add_ids(&["s1".into()], "search:q").await.unwrap();
        f.add_ids(&["r1".into()], "search:q").await.unwrap();
        for id in ["e1", "s1"] {
            f.set_checked(&video(id), None).await.unwrap();
        }
        f.set_checked(&video("r1"), Some("license:youtube"))
            .await
            .unwrap();
        let ids: Vec<_> = f
            .unplanned(None)
            .await
            .unwrap()
            .into_iter()
            .map(|(v, _)| v.id)
            .collect();
        assert_eq!(ids, vec!["s1", "e1"]);
        f.set_batch(&["s1".into()], "b1").await.unwrap();
        assert_eq!(f.unplanned(Some(5)).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn quota_accumulates_per_day() {
        let Some(f) = frontier().await else { return };
        f.add_quota("2026-10-07", 100, 100).await.unwrap();
        f.add_quota("2026-10-07", 3, 0).await.unwrap();
        assert_eq!(f.quota("2026-10-07").await.unwrap(), (103, 100));
        assert_eq!(f.quota("2026-10-08").await.unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn seeds_come_before_expansions() {
        let Some(f) = frontier().await else { return };
        f.add_channel("UCexp", "expand").await.unwrap();
        f.add_channel("@seed", "seed").await.unwrap();
        assert!(!f.add_channel("@seed", "expand").await.unwrap());
        assert_eq!(f.next_channel().await.unwrap().unwrap().channel, "@seed");
        f.channel_done("@seed", None).await.unwrap();
        assert_eq!(f.next_channel().await.unwrap().unwrap().channel, "UCexp");
    }

    #[tokio::test]
    async fn a_handle_and_its_id_are_crawled_once() {
        let Some(f) = frontier().await else { return };
        f.add_channel("@seed", "seed").await.unwrap();
        f.add_channel("UC1", "expand").await.unwrap();
        f.set_uploads("@seed", "UC1", "UU1").await.unwrap();
        assert!(!f.add_channel("UC1", "expand").await.unwrap());
        f.channel_done("@seed", None).await.unwrap();
        assert!(f.next_channel().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn searches_keep_config_order_and_progress() {
        let Some(f) = frontier().await else { return };
        let q: Vec<String> = vec!["b".into(), "a".into()];
        let s = f.pending_searches(&q).await.unwrap();
        assert_eq!(
            s.iter().map(|s| s.query.as_str()).collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        f.search_page("b", Some("t2"), false).await.unwrap();
        f.search_page("a", None, true).await.unwrap();
        let s = f.pending_searches(&q).await.unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!((s[0].pages, s[0].next.as_deref()), (1, Some("t2")));
    }

    /// Two workers leasing at once never get the same video; an expired lease is taken again.
    #[tokio::test]
    async fn leases_are_exclusive_and_expire() {
        let Some(f) = frontier().await else { return };
        let ids: Vec<String> = (0..10).map(|i| format!("v{i}")).collect();
        f.add_ids(&ids[..3], "expand:UC1").await.unwrap();
        f.add_ids(&ids[3..], "search:q").await.unwrap();
        for id in &ids {
            f.set_checked(&video(id), None).await.unwrap();
        }
        let hour = Duration::from_secs(3600);
        let (a, b) = tokio::join!(f.lease("a", 4, hour), f.lease("b", 4, hour));
        let (a, b) = (a.unwrap(), b.unwrap());
        let mut got: Vec<_> = a.iter().chain(&b).map(|l| l.video.id.clone()).collect();
        assert_eq!(got.len(), 8);
        got.sort();
        got.dedup();
        assert_eq!(got.len(), 8, "a video was leased twice");
        // Direct hits went first: the two left are expansion hits.
        let rest = f.lease("c", 10, Duration::ZERO).await.unwrap();
        assert_eq!(rest.len(), 2);
        assert!(rest.iter().all(|l| l.source.starts_with("expand:")));
        // c's zero-length leases have run out: d can take them; nothing else is free.
        let again = f.lease("d", 10, hour).await.unwrap();
        assert_eq!(again.len(), 2);
        assert_eq!(again[0].attempts, 2);

        f.finish(&a[0].video.id, "fetched", None, 1234)
            .await
            .unwrap();
        f.requeue(&a[1].video.id, "a", Some("bot check"), Duration::ZERO)
            .await
            .unwrap();
        f.requeue(&a[2].video.id, "not-a", None, Duration::ZERO)
            .await
            .unwrap(); // not its lease
        f.requeue(&a[3].video.id, "a", None, hour).await.unwrap(); // back in an hour
        let retry = f.lease("e", 10, hour).await.unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].video.id, a[1].video.id);
        let p = f.progress().await.unwrap();
        let fetched = p.iter().find(|p| p.state == "fetched").unwrap();
        assert_eq!((fetched.videos, fetched.bytes), (1, 1234));
        assert!(
            f.unplanned(None)
                .await
                .unwrap()
                .iter()
                .all(|(v, _)| v.id != a[0].video.id)
        );
    }
}
