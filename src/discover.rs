//! discover: spends the day's API quota turning seeds into checked video ids.
//!
//! Order: check known ids → seed channels → channel uploads (checking after each channel) →
//! search (within search_quota) → channels found by search → check. Each step resumes where the
//! last run stopped, and running out of quota ends the run cleanly.

use std::path::Path;

use anyhow::{Context, Result};

use crate::api::{self, Api, SEARCH_COST, Transport};
use crate::config::Config;
use crate::frontier::{Channel, Frontier};

#[derive(Debug, Default)]
pub struct Report {
    pub added: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub gone: usize,
    pub channels: usize,
    pub search_pages: usize,
    pub quota_used: u64,
    pub out_of_quota: bool,
}

pub struct Options<'a> {
    pub ids_file: Option<&'a Path>,
    pub search: bool,
}

pub async fn run<T: Transport>(
    cfg: &Config,
    frontier: &Frontier,
    transport: T,
    opts: Options<'_>,
) -> Result<Report> {
    let day = crate::frontier::quota_day();
    let (used, _) = frontier.quota(&day).await?;
    let mut d = Discover {
        cfg,
        f: frontier,
        api: Api::new(transport, cfg.discover.daily_quota.saturating_sub(used)),
        day,
        report: Report::default(),
    };
    match d.steps(opts).await {
        Ok(()) => {}
        Err(e) if api::is_out_of_quota(&e) => d.report.out_of_quota = true,
        Err(e) => return Err(e),
    }
    Ok(d.report)
}

struct Discover<'a, T> {
    cfg: &'a Config,
    f: &'a Frontier,
    api: Api<T>,
    day: String,
    report: Report,
}

impl<T: Transport> Discover<'_, T> {
    async fn steps(&mut self, opts: Options<'_>) -> Result<()> {
        self.report.added += self.f.add_ids(&self.cfg.discover.videos, "seed").await?;
        if let Some(path) = opts.ids_file {
            self.report.added += self.f.add_ids(&read_ids(path)?, "ids-file").await?;
        }
        self.verify().await?;
        for c in &self.cfg.discover.channels {
            self.f.add_channel(c, "seed").await?;
        }
        self.channels().await?;
        if opts.search {
            self.search().await?;
        }
        self.channels().await?;
        self.verify().await
    }

    /// Records what the last call spent, whether or not it succeeded.
    async fn charge(&mut self, search: bool) -> Result<()> {
        let spent = self.api.take_spent();
        self.report.quota_used += spent;
        self.f
            .add_quota(&self.day, spent, if search { spent } else { 0 })
            .await
    }

    /// Checks every `new` id with videos.list, 50 per unit.
    async fn verify(&mut self) -> Result<()> {
        loop {
            let ids = self.f.unchecked(50).await?;
            if ids.is_empty() {
                return Ok(());
            }
            let r = self.api.videos(&ids).await;
            self.charge(false).await?;
            let videos = r?;
            for v in &videos {
                let rejection = v.rejection(&self.cfg.filters);
                self.f.set_checked(v, rejection.as_deref()).await?;
                if rejection.is_some() {
                    self.report.rejected += 1;
                    continue;
                }
                self.report.accepted += 1;
                let direct = self
                    .f
                    .source(&v.id)
                    .await?
                    .is_some_and(|s| !s.starts_with("expand:"));
                if self.cfg.discover.expand_channels && direct && !v.channel_id.is_empty() {
                    self.f.add_channel(&v.channel_id, "expand").await?;
                }
            }
            for id in ids.iter().filter(|id| !videos.iter().any(|v| &v.id == *id)) {
                self.f.set_gone(id).await?;
                self.report.gone += 1;
            }
        }
    }

    /// Pages through channel uploads, seeds first, checking the new ids after each channel.
    async fn channels(&mut self) -> Result<()> {
        loop {
            self.verify().await?;
            let Some(ch) = self.f.next_channel().await? else {
                return Ok(());
            };
            self.channel(ch).await?;
            self.report.channels += 1;
        }
    }

    async fn channel(&mut self, ch: Channel) -> Result<()> {
        let (id, uploads) = match ch.uploads {
            Some(u) => (ch.channel.clone(), u),
            None => {
                let r = self.api.channel(&ch.channel).await;
                self.charge(false).await?;
                match r? {
                    Some((id, uploads)) => {
                        self.f.set_uploads(&ch.channel, &id, &uploads).await?;
                        (id, uploads)
                    }
                    None => return self.f.channel_done(&ch.channel, Some("not found")).await,
                }
            }
        };
        let source = format!(
            "{}:{id}",
            if ch.kind == "seed" {
                "channel"
            } else {
                "expand"
            }
        );
        let (mut found, mut next) = (ch.found, ch.next);
        loop {
            if found >= self.cfg.discover.max_uploads_per_channel {
                return self
                    .f
                    .channel_done(&ch.channel, Some("max_uploads_per_channel"))
                    .await;
            }
            let r = self.api.playlist(&uploads, next.as_deref()).await;
            self.charge(false).await?;
            let page = r?;
            self.report.added += self.f.add_ids(&page.ids, &source).await?;
            found += page.ids.len() as u32;
            self.f
                .channel_page(&ch.channel, page.next.as_deref(), page.ids.len() as u32)
                .await?;
            next = page.next;
            if next.is_none() {
                return self.f.channel_done(&ch.channel, None).await;
            }
        }
    }

    /// Runs the configured queries, pages_per_query pages each, while search_quota lasts.
    async fn search(&mut self) -> Result<()> {
        let per_query = self.cfg.discover.pages_per_query;
        for s in self.f.pending_searches(&self.cfg.discover.queries).await? {
            let (mut pages, mut next) = (s.pages, s.next);
            while pages < per_query {
                let (_, search_used) = self.f.quota(&self.day).await?;
                if search_used + SEARCH_COST > self.cfg.discover.search_quota {
                    return Ok(());
                }
                let r = self.api.search(&s.query, next.as_deref()).await;
                self.charge(true).await?;
                let page = r?;
                self.report.added += self
                    .f
                    .add_ids(&page.ids, &format!("search:{}", s.query))
                    .await?;
                self.report.search_pages += 1;
                pages += 1;
                next = page.next;
                self.f
                    .search_page(
                        &s.query,
                        next.as_deref(),
                        next.is_none() || pages >= per_query,
                    )
                    .await?;
                if next.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Video ids, one per line, as bare ids or watch URLs. Blank lines and # comments are skipped.
pub fn read_ids(path: &Path) -> Result<Vec<String>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut ids = vec![];
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let id = video_id(line)
            .with_context(|| format!("{}:{}: not a video id: {line}", path.display(), n + 1))?;
        ids.push(id);
    }
    Ok(ids)
}

fn video_id(s: &str) -> Option<String> {
    let id = if let Some(i) = s.find("v=") {
        s[i + 2..].split(['&', '#']).next()?
    } else if let Some(i) = s.find("youtu.be/") {
        s[i + 9..].split(['?', '&', '#']).next()?
    } else {
        s
    };
    let ok = id.len() == 11
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    ok.then(|| id.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::api::fake::{Fake, item, quota_exceeded};

    fn config() -> Config {
        let mut c = Config::default();
        c.discover.queries = vec!["folding laundry".into()];
        c.discover.pages_per_query = 2;
        c
    }

    /// videos.list answers from a table of id → (license, channel); unknown ids are gone.
    fn videos_handler(
        table: HashMap<&'static str, (&'static str, &'static str)>,
    ) -> impl Fn(&HashMap<String, String>) -> (u16, String) {
        move |q| {
            let items: Vec<_> = q["id"]
                .split(',')
                .filter_map(|id| table.get(id).map(|(lic, ch)| item(id, lic, "PT2M", ch)))
                .collect();
            (200, json!({ "items": items }).to_string())
        }
    }

    fn opts() -> Options<'static> {
        Options {
            ids_file: None,
            search: true,
        }
    }

    #[tokio::test]
    async fn search_then_expand_channels() {
        let mut fake = Fake::default();
        fake.queue("search", 200, json!({"items": [{"id": {"videoId": "s1"}}, {"id": {"videoId": "s2"}}], "nextPageToken": "p2"}));
        fake.queue(
            "search",
            200,
            json!({"items": [{"id": {"videoId": "s3"}}], "nextPageToken": "p3"}),
        );
        fake.queue("channels", 200, json!({"items": [{"id": "UC1", "contentDetails": {"relatedPlaylists": {"uploads": "UU1"}}}]}));
        fake.queue("playlistItems", 200, json!({"items": [{"contentDetails": {"videoId": "s1"}}, {"contentDetails": {"videoId": "e1"}}], "nextPageToken": "n"}));
        fake.queue(
            "playlistItems",
            200,
            json!({"items": [{"contentDetails": {"videoId": "e2"}}]}),
        );
        fake.handle(
            "videos",
            videos_handler(HashMap::from([
                ("s1", ("creativeCommon", "UC1")),
                ("s2", ("youtube", "UC2")),
                ("e1", ("creativeCommon", "UC1")),
                ("e2", ("creativeCommon", "UC1")),
            ])),
        );
        let Some(f) = crate::frontier::tests::frontier().await else {
            return;
        };
        let r = run(&config(), &f, fake, opts()).await.unwrap();
        assert!(!r.out_of_quota);
        assert_eq!(r.search_pages, 2);
        assert_eq!((r.accepted, r.rejected, r.gone), (3, 1, 1)); // s3 is gone
        assert_eq!(r.channels, 1); // UC1 only: s2 was rejected
        // 2 searches + 1 channels + 2 playlist pages + videos.list calls
        let (used, search) = f.quota(&crate::frontier::quota_day()).await.unwrap();
        assert_eq!(search, 200);
        assert_eq!(used, r.quota_used);
        assert_eq!(f.source("e1").await.unwrap().as_deref(), Some("expand:UC1"));
        let planned: Vec<_> = f
            .unplanned(None)
            .await
            .unwrap()
            .into_iter()
            .map(|(v, _)| v.id)
            .collect();
        assert_eq!(planned, vec!["s1", "e1", "e2"]);

        // A second run has nothing left: searches hit pages_per_query, the channel is done.
        let mut fake = Fake::default();
        fake.handle("search", |_| panic!("searched again"));
        let r = run(&config(), &f, fake, opts()).await.unwrap();
        assert_eq!(r.quota_used, 0);
    }

    #[tokio::test]
    async fn search_quota_caps_search() {
        let mut fake = Fake::default();
        fake.handle("search", |_| {
            (
                200,
                json!({"items": [], "nextPageToken": "more"}).to_string(),
            )
        });
        let mut c = config();
        c.discover.pages_per_query = 50;
        c.discover.search_quota = 300;
        let Some(f) = crate::frontier::tests::frontier().await else {
            return;
        };
        let r = run(&c, &f, fake, opts()).await.unwrap();
        assert_eq!(r.search_pages, 3);
        assert!(!r.out_of_quota);
    }

    #[tokio::test]
    async fn stops_cleanly_out_of_quota_and_resumes() {
        let fake = Fake::default();
        fake.queue(
            "search",
            200,
            json!({"items": [{"id": {"videoId": "s1"}}], "nextPageToken": "p2"}),
        );
        fake.queue("search", 403, quota_exceeded());
        let Some(f) = crate::frontier::tests::frontier().await else {
            return;
        };
        let mut c = config();
        c.discover.expand_channels = false;
        let r = run(&c, &f, fake, opts()).await.unwrap();
        assert!(r.out_of_quota);
        assert_eq!(f.unchecked(10).await.unwrap(), vec!["s1"]);

        // The next run (next day, say) checks s1 first, then continues the query from p2.
        let mut fake = Fake::default();
        fake.handle(
            "videos",
            videos_handler(HashMap::from([("s1", ("creativeCommon", "UC1"))])),
        );
        fake.handle("search", |q| {
            assert_eq!(q["pageToken"], "p2");
            (200, json!({"items": []}).to_string())
        });
        let r = run(&c, &f, fake, opts()).await.unwrap();
        assert_eq!(r.accepted, 1);
        assert!(
            f.pending_searches(&c.discover.queries)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn respects_quota_already_spent_today() {
        let Some(f) = crate::frontier::tests::frontier().await else {
            return;
        };
        f.add_quota(&crate::frontier::quota_day(), 9_950, 0)
            .await
            .unwrap();
        let mut fake = Fake::default();
        fake.handle("search", |_| panic!("no quota left for search"));
        let r = run(&config(), &f, fake, opts()).await.unwrap();
        assert!(r.out_of_quota);
    }

    #[tokio::test]
    async fn caps_uploads_per_channel() {
        let mut fake = Fake::default();
        fake.queue("channels", 200, json!({"items": [{"id": "UC9", "contentDetails": {"relatedPlaylists": {"uploads": "UU9"}}}]}));
        let page = |n: usize| {
            (0..50)
                .map(|i| json!({"contentDetails": {"videoId": format!("v{n:02}{i:08}")}}))
                .collect::<Vec<_>>()
        };
        for n in 0..5 {
            fake.queue(
                "playlistItems",
                200,
                json!({"items": page(n), "nextPageToken": "more"}),
            );
        }
        fake.handle("videos", |_| (200, json!({"items": []}).to_string()));
        let mut c = config();
        c.discover.channels = vec!["@nine".into()];
        c.discover.max_uploads_per_channel = 120;
        let Some(f) = crate::frontier::tests::frontier().await else {
            return;
        };
        let r = run(
            &c,
            &f,
            fake,
            Options {
                ids_file: None,
                search: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.added, 150); // three pages of 50 reach 120
        assert_eq!(r.gone, 150);
        assert!(f.next_channel().await.unwrap().is_none());
    }

    #[test]
    fn reads_ids_and_urls() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ids.txt");
        std::fs::write(&p, "# list\nM4gD1WSo5mA\n\nhttps://www.youtube.com/watch?v=uGpuVWrhIzE&t=3\nhttps://youtu.be/dQw4w9WgXcQ?si=x\n").unwrap();
        assert_eq!(
            read_ids(&p).unwrap(),
            vec!["M4gD1WSo5mA", "uGpuVWrhIzE", "dQw4w9WgXcQ"]
        );
        std::fs::write(&p, "nope\n").unwrap();
        assert!(read_ids(&p).is_err());
    }
}
