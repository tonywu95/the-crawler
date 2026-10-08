//! config/youtube.yaml. Every field has a default, so a partial file (or none) works.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Where shards, media and manifests go: a local path, file://, s3:// or gs:// URL.
    pub store: String,
    pub discover: Discover,
    pub filters: Filters,
    pub plan: Plan,
    pub fetch: Fetch,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Discover {
    /// Units per day for the API key; resets at midnight Pacific.
    pub daily_quota: u64,
    /// The part of daily_quota that search.list (100 units a call) may spend.
    pub search_quota: u64,
    pub pages_per_query: u32,
    /// Crawl the uploads of every channel that has an accepted video.
    pub expand_channels: bool,
    pub max_uploads_per_channel: u32,
    pub queries: Vec<String>,
    /// Channel ids (UC...) or @handles.
    pub channels: Vec<String>,
    /// Video ids.
    pub videos: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Filters {
    /// Accepted values of the API's status.license.
    pub licenses: Vec<String>,
    pub min_duration_s: u64,
    pub max_duration_s: u64,
    pub skip_live: bool,
    pub skip_age_restricted: bool,
    pub skip_made_for_kids: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Plan {
    pub shard_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Fetch {
    pub ytdlp: PathBuf,
    pub deno: PathBuf,
    pub format: String,
    pub subtitles: Vec<String>,
    pub max_bytes_per_second: u64,
    /// An IP rests a random time in this range after each video.
    pub pause_min_s: f64,
    pub pause_max_s: f64,
    /// An IP is benched this long after a bot check, 429 or proxy failure; doubles on each one
    /// in a row, up to bot_check_backoff_max_s.
    pub bot_check_backoff_s: u64,
    pub bot_check_backoff_max_s: u64,
    /// Strikes in a row before an IP is retired. The worker stops once every IP is retired.
    pub bot_check_limit: u32,
    /// Other failures in a row before the worker stops.
    pub error_limit: u32,
    /// A proxy list, one per line: `<provider> <url>`. Unset: the machine's own address.
    /// Keep it out of git (it holds credentials); config/proxies*.txt is ignored.
    pub proxies: Option<PathBuf>,
    /// Videos one IP may start per hour; 0 for no cap.
    pub max_videos_per_ip_per_hour: u32,
    /// Per-IP health and the attempt log, on this worker.
    pub egress_db: PathBuf,
    /// USD per IP per month, by provider, for the $ per hour column in `status`.
    pub provider_prices: BTreeMap<String, f64>,
    /// Queue mode: how long a worker holds a video before it returns to the queue. Renewed
    /// while the worker is still on it, so this only matters when a worker dies.
    pub video_lease_s: u64,
    /// Queue mode: tries before a failing video is marked failed for good.
    pub max_attempts: u32,
    /// Queue mode: wait before a failed video may be tried again.
    pub retry_delay_s: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            store: "data/youtube".into(),
            discover: Discover::default(),
            filters: Filters::default(),
            plan: Plan::default(),
            fetch: Fetch::default(),
        }
    }
}

impl Default for Discover {
    fn default() -> Self {
        Self {
            daily_quota: 10_000,
            search_quota: 6_000,
            pages_per_query: 5,
            expand_channels: true,
            max_uploads_per_channel: 2_000,
            queries: vec![],
            channels: vec![],
            videos: vec![],
        }
    }
}

impl Default for Filters {
    fn default() -> Self {
        Self {
            licenses: vec!["creativeCommon".into()],
            min_duration_s: 10,
            max_duration_s: 3_600,
            skip_live: true,
            skip_age_restricted: true,
            skip_made_for_kids: true,
        }
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self { shard_size: 500 }
    }
}

impl Default for Fetch {
    fn default() -> Self {
        Self {
            ytdlp: ".venv/bin/yt-dlp".into(),
            deno: ".venv/bin/deno".into(),
            format: "bv[height<=720][vcodec^=avc1]/bv[height<=720]/b[height<=720]".into(),
            subtitles: vec!["en".into()],
            max_bytes_per_second: 10_000_000,
            pause_min_s: 4.0,
            pause_max_s: 12.0,
            bot_check_backoff_s: 300,
            bot_check_backoff_max_s: 86_400,
            bot_check_limit: 3,
            error_limit: 20,
            proxies: None,
            max_videos_per_ip_per_hour: 0,
            egress_db: "data/egress.sqlite".into(),
            provider_prices: BTreeMap::new(),
            video_lease_s: 1_800,
            max_attempts: 3,
            retry_delay_s: 600,
        }
    }
}

impl Config {
    /// Reads `path`, or returns the defaults when it does not exist.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_yaml_keeps_defaults() {
        let c: Config =
            serde_yaml::from_str("discover:\n  queries: [a]\nfetch:\n  error_limit: 3\n").unwrap();
        assert_eq!(c.discover.queries, vec!["a"]);
        assert_eq!(c.discover.daily_quota, 10_000);
        assert_eq!(c.fetch.error_limit, 3);
        assert_eq!(c.fetch.bot_check_limit, 3);
        assert_eq!(c.filters.licenses, vec!["creativeCommon"]);
    }

    #[test]
    fn unknown_keys_are_errors() {
        assert!(serde_yaml::from_str::<Config>("discover:\n  querys: [a]\n").is_err());
    }

    #[test]
    fn shipped_config_parses() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/youtube.yaml");
        let c = Config::load(&path).unwrap();
        assert!(!c.discover.queries.is_empty());
    }
}
