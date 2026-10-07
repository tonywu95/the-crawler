//! YouTube Data API v3: the few list calls discover needs, with quota accounting.
//!
//! The key travels only in the X-Goog-Api-Key header, never in a URL, so it cannot leak into
//! logs or error messages.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Filters;

pub const SEARCH_COST: u64 = 100;
pub const LIST_COST: u64 = 1;
const RETRIES: u32 = 5;

/// The day's quota is used up (or would be by the next call). Discover stops cleanly on it.
#[derive(Debug)]
pub struct OutOfQuota;

impl std::fmt::Display for OutOfQuota {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("YouTube Data API quota exhausted for today")
    }
}

impl std::error::Error for OutOfQuota {}

pub fn is_out_of_quota(e: &anyhow::Error) -> bool {
    e.downcast_ref::<OutOfQuota>().is_some()
}

/// One GET against the API. `Http` is the real one; tests use a fake.
pub trait Transport {
    /// Returns the HTTP status and body. `endpoint` is e.g. "videos".
    fn get(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> impl Future<Output = Result<(u16, String)>>;
}

pub struct Http {
    client: reqwest::Client,
    key: String,
    base: String,
}

impl Http {
    pub fn from_env() -> Result<Self> {
        let key = std::env::var("YOUTUBE_API_KEY")
            .map_err(|_| anyhow!("YOUTUBE_API_KEY is not set (a key for YouTube Data API v3)"))?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("the-crawler/0.1")
            .build()?;
        Ok(Self {
            client,
            key,
            base: "https://www.googleapis.com/youtube/v3".into(),
        })
    }
}

impl Transport for Http {
    async fn get(&self, endpoint: &str, query: &[(&str, String)]) -> Result<(u16, String)> {
        let resp = self
            .client
            .get(format!("{}/{endpoint}", self.base))
            .header("X-Goog-Api-Key", &self.key)
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {endpoint}"))?;
        let status = resp.status().as_u16();
        Ok((status, resp.text().await?))
    }
}

/// What discover keeps of a videos.list item. Also the per-video line in a shard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Video {
    pub id: String,
    pub title: String,
    pub channel_id: String,
    pub channel_title: String,
    pub published_at: String,
    pub duration_s: u64,
    pub license: String,
    /// "none", "live" or "upcoming".
    pub live: String,
    pub age_restricted: bool,
    pub made_for_kids: bool,
    pub privacy: String,
    pub language: Option<String>,
}

impl Video {
    pub fn url(&self) -> String {
        format!("https://www.youtube.com/watch?v={}", self.id)
    }

    fn from_item(item: &Value) -> Option<Self> {
        let s = |v: &Value| v.as_str().unwrap_or_default().to_string();
        let snippet = &item["snippet"];
        let details = &item["contentDetails"];
        let status = &item["status"];
        Some(Self {
            id: item["id"].as_str()?.to_string(),
            title: s(&snippet["title"]),
            channel_id: s(&snippet["channelId"]),
            channel_title: s(&snippet["channelTitle"]),
            published_at: s(&snippet["publishedAt"]),
            duration_s: parse_duration(details["duration"].as_str().unwrap_or("")).unwrap_or(0),
            license: s(&status["license"]),
            live: snippet["liveBroadcastContent"]
                .as_str()
                .unwrap_or("none")
                .to_string(),
            age_restricted: details["contentRating"]["ytRating"] == "ytAgeRestricted",
            made_for_kids: status["madeForKids"].as_bool().unwrap_or(false),
            privacy: s(&status["privacyStatus"]),
            language: snippet["defaultAudioLanguage"]
                .as_str()
                .or(snippet["defaultLanguage"].as_str())
                .map(str::to_string),
        })
    }

    /// Why the filters reject this video, or None if they accept it.
    pub fn rejection(&self, f: &Filters) -> Option<String> {
        if !f.licenses.contains(&self.license) {
            return Some(format!("license:{}", self.license));
        }
        if self.privacy == "private" {
            return Some("private".into());
        }
        if f.skip_live && self.live != "none" {
            return Some(format!("live:{}", self.live));
        }
        if f.skip_age_restricted && self.age_restricted {
            return Some("age_restricted".into());
        }
        if f.skip_made_for_kids && self.made_for_kids {
            return Some("made_for_kids".into());
        }
        if self.duration_s < f.min_duration_s || self.duration_s > f.max_duration_s {
            return Some(format!("duration:{}", self.duration_s));
        }
        None
    }
}

/// ISO 8601 durations as the API writes them: PT1H2M3S, P1DT2H, PT0S.
pub fn parse_duration(s: &str) -> Option<u64> {
    let rest = s.strip_prefix('P')?;
    let (mut total, mut num, mut in_time) = (0u64, String::new(), false);
    for c in rest.chars() {
        match c {
            'T' => in_time = true,
            '0'..='9' => num.push(c),
            _ => {
                let n: u64 = num.parse().ok()?;
                num.clear();
                total += n * match (c, in_time) {
                    ('W', false) => 604_800,
                    ('D', false) => 86_400,
                    ('H', true) => 3_600,
                    ('M', true) => 60,
                    ('S', true) => 1,
                    _ => return None,
                };
            }
        }
    }
    num.is_empty().then_some(total)
}

/// A page of video ids and the token for the next page.
#[derive(Debug, Default)]
pub struct Page {
    pub ids: Vec<String>,
    pub next: Option<String>,
}

pub struct Api<T> {
    transport: T,
    /// Units this process may still spend today.
    remaining: u64,
    /// Units spent since the last `take_spent`.
    spent: u64,
    retry_base: Duration,
}

impl<T: Transport> Api<T> {
    pub fn new(transport: T, remaining: u64) -> Self {
        Self {
            transport,
            remaining,
            spent: 0,
            retry_base: Duration::from_secs(1),
        }
    }

    #[cfg(test)]
    pub fn without_retry_delay(mut self) -> Self {
        self.retry_base = Duration::ZERO;
        self
    }

    #[cfg(test)]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Units spent since the last call, for the caller to record.
    pub fn take_spent(&mut self) -> u64 {
        std::mem::take(&mut self.spent)
    }

    /// One call, charged `cost` per request sent (retries included). Ok(None) on 404.
    async fn call(
        &mut self,
        endpoint: &str,
        cost: u64,
        query: &[(&str, String)],
    ) -> Result<Option<Value>> {
        let mut attempt = 0;
        loop {
            if self.remaining < cost {
                return Err(OutOfQuota.into());
            }
            self.remaining -= cost;
            self.spent += cost;
            let (status, body) = match self.transport.get(endpoint, query).await {
                Ok(r) => r,
                Err(e) if attempt < RETRIES => {
                    eprintln!("api: {endpoint}: {e:#}; retrying");
                    attempt += 1;
                    tokio::time::sleep(self.retry_base * 2u32.pow(attempt)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if status == 200 {
                return Ok(Some(
                    serde_json::from_str(&body).with_context(|| format!("{endpoint}: bad JSON"))?,
                ));
            }
            if status == 404 {
                return Ok(None);
            }
            let reason = error_reason(&body);
            if reason == "quotaExceeded" || reason == "dailyLimitExceeded" {
                self.remaining = 0;
                return Err(OutOfQuota.into());
            }
            let transient = status >= 500
                || matches!(
                    reason.as_str(),
                    "rateLimitExceeded" | "userRateLimitExceeded"
                );
            if transient && attempt < RETRIES {
                attempt += 1;
                tokio::time::sleep(self.retry_base * 2u32.pow(attempt)).await;
                continue;
            }
            bail!("{endpoint}: HTTP {status} {reason}");
        }
    }

    /// videos.list for up to 50 ids. Ids missing from the result are gone or private.
    pub async fn videos(&mut self, ids: &[String]) -> Result<Vec<Video>> {
        assert!(ids.len() <= 50);
        let query = [
            ("part", "snippet,contentDetails,status".to_string()),
            ("id", ids.join(",")),
            ("maxResults", "50".into()),
        ];
        let Some(v) = self.call("videos", LIST_COST, &query).await? else {
            return Ok(vec![]);
        };
        Ok(items(&v).iter().filter_map(Video::from_item).collect())
    }

    /// One page of Creative Commons search results.
    pub async fn search(&mut self, q: &str, page: Option<&str>) -> Result<Page> {
        let mut query = vec![
            ("part", "id".to_string()),
            ("type", "video".into()),
            ("videoLicense", "creativeCommon".into()),
            ("maxResults", "50".into()),
            ("q", q.to_string()),
        ];
        if let Some(p) = page {
            query.push(("pageToken", p.to_string()));
        }
        let Some(v) = self.call("search", SEARCH_COST, &query).await? else {
            return Ok(Page::default());
        };
        let ids = items(&v)
            .iter()
            .filter_map(|i| i["id"]["videoId"].as_str().map(str::to_string))
            .collect();
        Ok(Page {
            ids,
            next: next_token(&v),
        })
    }

    /// A channel's (id, uploads playlist) from a UC id or an @handle. None if it does not exist.
    pub async fn channel(&mut self, channel: &str) -> Result<Option<(String, String)>> {
        let key = if channel.starts_with('@') {
            "forHandle"
        } else {
            "id"
        };
        let query = [
            ("part", "contentDetails".to_string()),
            (key, channel.to_string()),
        ];
        let Some(v) = self.call("channels", LIST_COST, &query).await? else {
            return Ok(None);
        };
        Ok(items(&v).first().and_then(|c| {
            let id = c["id"].as_str()?;
            let uploads = c["contentDetails"]["relatedPlaylists"]["uploads"].as_str()?;
            Some((id.to_string(), uploads.to_string()))
        }))
    }

    /// One page of a playlist's video ids.
    pub async fn playlist(&mut self, playlist: &str, page: Option<&str>) -> Result<Page> {
        let mut query = vec![
            ("part", "contentDetails".to_string()),
            ("playlistId", playlist.to_string()),
            ("maxResults", "50".into()),
        ];
        if let Some(p) = page {
            query.push(("pageToken", p.to_string()));
        }
        let Some(v) = self.call("playlistItems", LIST_COST, &query).await? else {
            return Ok(Page::default());
        };
        let ids = items(&v)
            .iter()
            .filter_map(|i| i["contentDetails"]["videoId"].as_str().map(str::to_string))
            .collect();
        Ok(Page {
            ids,
            next: next_token(&v),
        })
    }
}

fn items(v: &Value) -> &[Value] {
    v["items"].as_array().map(Vec::as_slice).unwrap_or_default()
}

fn next_token(v: &Value) -> Option<String> {
    v["nextPageToken"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn error_reason(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| {
            v["error"]["errors"][0]["reason"]
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub mod fake {
    //! A scripted Transport: each endpoint answers from a queue, then from a handler.

    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};

    use super::*;

    type Handler = Box<dyn Fn(&HashMap<String, String>) -> (u16, String)>;

    #[derive(Default)]
    pub struct Fake {
        queued: RefCell<HashMap<String, VecDeque<(u16, String)>>>,
        handlers: HashMap<String, Handler>,
        pub calls: RefCell<Vec<(String, HashMap<String, String>)>>,
    }

    impl Fake {
        pub fn queue(&self, endpoint: &str, status: u16, body: Value) {
            self.queued
                .borrow_mut()
                .entry(endpoint.into())
                .or_default()
                .push_back((status, body.to_string()));
        }

        pub fn handle(
            &mut self,
            endpoint: &str,
            f: impl Fn(&HashMap<String, String>) -> (u16, String) + 'static,
        ) {
            self.handlers.insert(endpoint.into(), Box::new(f));
        }
    }

    impl Transport for Fake {
        async fn get(&self, endpoint: &str, query: &[(&str, String)]) -> Result<(u16, String)> {
            let q: HashMap<String, String> = query
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            self.calls.borrow_mut().push((endpoint.into(), q.clone()));
            if let Some(r) = self
                .queued
                .borrow_mut()
                .get_mut(endpoint)
                .and_then(VecDeque::pop_front)
            {
                return Ok(r);
            }
            match self.handlers.get(endpoint) {
                Some(h) => Ok(h(&q)),
                None => Ok((404, String::new())),
            }
        }
    }

    /// A videos.list item.
    pub fn item(id: &str, license: &str, duration: &str, channel: &str) -> Value {
        serde_json::json!({
            "id": id,
            "snippet": {"title": format!("title {id}"), "channelId": channel, "channelTitle": format!("chan {channel}"),
                        "publishedAt": "2024-01-01T00:00:00Z", "liveBroadcastContent": "none"},
            "contentDetails": {"duration": duration},
            "status": {"license": license, "privacyStatus": "public", "madeForKids": false},
        })
    }

    pub fn quota_exceeded() -> Value {
        serde_json::json!({"error": {"code": 403, "errors": [{"reason": "quotaExceeded"}]}})
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::fake::*;
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("PT1H2M3S"), Some(3723));
        assert_eq!(parse_duration("PT45S"), Some(45));
        assert_eq!(parse_duration("P1DT1S"), Some(86_401));
        assert_eq!(parse_duration("P0D"), Some(0));
        assert_eq!(parse_duration("PT"), Some(0));
        assert_eq!(parse_duration("1H"), None);
        assert_eq!(parse_duration("PT5"), None);
        assert_eq!(parse_duration("P1H"), None);
    }

    #[test]
    fn filters() {
        let f = Filters::default();
        let v = Video::from_item(&item("a", "creativeCommon", "PT1M", "c")).unwrap();
        assert_eq!(v.rejection(&f), None);
        let std = Video {
            license: "youtube".into(),
            ..v.clone()
        };
        assert_eq!(std.rejection(&f).unwrap(), "license:youtube");
        let short = Video {
            duration_s: 5,
            ..v.clone()
        };
        assert_eq!(short.rejection(&f).unwrap(), "duration:5");
        let live = Video {
            live: "live".into(),
            ..v.clone()
        };
        assert_eq!(live.rejection(&f).unwrap(), "live:live");
        let kids = Video {
            made_for_kids: true,
            ..v.clone()
        };
        assert_eq!(kids.rejection(&f).unwrap(), "made_for_kids");
        let mut age = item("b", "creativeCommon", "PT1M", "c");
        age["contentDetails"]["contentRating"] = json!({"ytRating": "ytAgeRestricted"});
        assert_eq!(
            Video::from_item(&age).unwrap().rejection(&f).unwrap(),
            "age_restricted"
        );
    }

    #[tokio::test]
    async fn charges_and_parses() {
        let fake = Fake::default();
        fake.queue(
            "videos",
            200,
            json!({"items": [item("a", "creativeCommon", "PT10S", "c")]}),
        );
        fake.queue(
            "search",
            200,
            json!({"items": [{"id": {"videoId": "x"}}], "nextPageToken": "t2"}),
        );
        let mut api = Api::new(fake, 1000);
        let v = api.videos(&["a".into(), "gone".into()]).await.unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].duration_s, 10);
        let p = api.search("q", None).await.unwrap();
        assert_eq!(p.ids, vec!["x"]);
        assert_eq!(p.next.as_deref(), Some("t2"));
        assert_eq!(api.take_spent(), 101);
        assert_eq!(api.remaining(), 899);
        assert_eq!(api.take_spent(), 0);
    }

    #[tokio::test]
    async fn refuses_calls_over_budget() {
        let mut api = Api::new(Fake::default(), 99);
        assert!(is_out_of_quota(&api.search("q", None).await.unwrap_err()));
        assert_eq!(api.take_spent(), 0);
    }

    #[tokio::test]
    async fn quota_exceeded_stops() {
        let fake = Fake::default();
        fake.queue("videos", 403, quota_exceeded());
        let mut api = Api::new(fake, 1000);
        assert!(is_out_of_quota(
            &api.videos(&["a".into()]).await.unwrap_err()
        ));
        assert_eq!(api.remaining(), 0);
    }

    #[tokio::test]
    async fn retries_transient_errors() {
        let fake = Fake::default();
        fake.queue("videos", 503, json!({}));
        fake.queue(
            "videos",
            403,
            json!({"error": {"errors": [{"reason": "rateLimitExceeded"}]}}),
        );
        fake.queue("videos", 200, json!({"items": []}));
        let mut api = Api::new(fake, 1000).without_retry_delay();
        assert!(api.videos(&["a".into()]).await.unwrap().is_empty());
        assert_eq!(api.take_spent(), 3);
    }

    #[tokio::test]
    async fn other_errors_fail_without_retry() {
        let fake = Fake::default();
        fake.queue(
            "videos",
            400,
            json!({"error": {"errors": [{"reason": "badRequest"}]}}),
        );
        let mut api = Api::new(fake, 1000).without_retry_delay();
        let e = api.videos(&["a".into()]).await.unwrap_err();
        assert!(!is_out_of_quota(&e));
        assert!(e.to_string().contains("badRequest"));
    }

    #[tokio::test]
    async fn not_found_is_empty() {
        let mut api = Api::new(Fake::default(), 1000);
        assert!(api.playlist("UUx", None).await.unwrap().ids.is_empty());
        assert!(api.channel("@nobody").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn handles_use_for_handle() {
        let fake = Fake::default();
        fake.queue("channels", 200, json!({"items": [{"id": "UCx", "contentDetails": {"relatedPlaylists": {"uploads": "UUx"}}}]}));
        let mut api = Api::new(fake, 1000);
        assert_eq!(
            api.channel("@someone").await.unwrap(),
            Some(("UCx".into(), "UUx".into()))
        );
        let calls = api.transport.calls.borrow();
        assert_eq!(calls[0].1["forHandle"], "@someone");
    }
}
