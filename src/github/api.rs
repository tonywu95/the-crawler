//! A small client for GitHub's GraphQL API: one request at a time, paced, waiting out rate limits.
//!
//! Every query asks for `rateLimit`, so the client always knows how many points the token has left
//! this hour and sleeps until the reset rather than running into the limit.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The fields fetched for every repository, however it was found.
const REPO_FRAGMENT: &str = "fragment Repo on Repository {
  databaseId nameWithOwner url description
  isFork isMirror isArchived isDisabled isEmpty isPrivate isLocked
  diskUsage stargazerCount forkCount createdAt pushedAt
  primaryLanguage { name }
  licenseInfo { spdxId }
  repositoryTopics(first: 10) { nodes { topic { name } } }
  defaultBranchRef { name target { oid } }
}";

const SEARCH_QUERY: &str = "query($q: String!, $after: String) {
  rateLimit { cost remaining resetAt }
  search(query: $q, type: REPOSITORY, first: 100, after: $after) {
    repositoryCount
    pageInfo { hasNextPage endCursor }
    nodes { ...Repo }
  }
}";

const OWNER_QUERY: &str = "query($login: String!, $after: String, $isFork: Boolean) {
  rateLimit { cost remaining resetAt }
  repositoryOwner(login: $login) {
    repositories(first: 100, after: $after, privacy: PUBLIC, ownerAffiliations: [OWNER], isFork: $isFork,
                 orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes { ...Repo }
    }
  }
}";

/// Most repositories one by-name query looks up, each under its own alias.
pub const NAMES_PER_QUERY: usize = 100;

/// Points left untouched each hour, so the token stays usable for other things.
const RESERVE_POINTS: i64 = 100;
const MAX_ATTEMPTS: u32 = 8;

/// A repository as GraphQL returns it (REPO_FRAGMENT). The frontier keeps this JSON as is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Repo {
    pub database_id: Option<i64>,
    pub name_with_owner: String,
    pub url: String,
    pub description: Option<String>,
    pub is_fork: bool,
    pub is_mirror: bool,
    pub is_archived: bool,
    pub is_disabled: bool,
    pub is_empty: bool,
    pub is_private: bool,
    pub is_locked: bool,
    /// KB on GitHub's disk, git history included: an upper bound on the snapshot's size.
    pub disk_usage: Option<i64>,
    pub stargazer_count: i64,
    pub fork_count: i64,
    pub created_at: String,
    pub pushed_at: Option<String>,
    pub primary_language: Option<Named>,
    pub license_info: Option<LicenseInfo>,
    pub repository_topics: Topics,
    pub default_branch_ref: Option<BranchRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Named {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LicenseInfo {
    /// "MIT", "Apache-2.0", ...; "NOASSERTION" when GitHub found a license it could not identify.
    pub spdx_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topics {
    pub nodes: Vec<TopicNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicNode {
    pub topic: Named,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchRef {
    pub name: String,
    pub target: Option<Target>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    pub oid: String,
}

impl Repo {
    pub fn spdx(&self) -> Option<&str> {
        self.license_info.as_ref()?.spdx_id.as_deref()
    }

    /// The default branch and the commit at its head when the repository was checked.
    pub fn head(&self) -> Option<(&str, &str)> {
        let branch = self.default_branch_ref.as_ref()?;
        Some((&branch.name, &branch.target.as_ref()?.oid))
    }
}

/// One page of a search or of an owner's repositories.
pub struct Page {
    /// How many repositories the search matches in all (search only).
    pub total: Option<i64>,
    pub repos: Vec<Repo>,
    /// The cursor for the next page, if there is one.
    pub next: Option<String>,
}

#[derive(Deserialize)]
struct Reply {
    #[serde(default)]
    data: Value,
    #[serde(default)]
    errors: Vec<GqlError>,
}

#[derive(Deserialize)]
struct GqlError {
    #[serde(rename = "type")]
    kind: Option<String>,
    message: String,
    #[serde(default)]
    path: Vec<Value>,
}

pub struct GitHub {
    http: reqwest::Client,
    url: String,
    token: String,
    /// Least time between the starts of two requests.
    pace: Duration,
    last_request: Option<Instant>,
    remaining: Option<i64>,
    reset_at: Option<DateTime<Utc>>,
    /// Points spent by this client so far.
    pub points_used: i64,
}

impl GitHub {
    /// `api_url` is https://api.github.com, or https://HOST/api for GitHub Enterprise Server.
    pub fn new(api_url: &str, token: &str, pace: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(crate::USER_AGENT)
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            http,
            url: format!("{}/graphql", api_url.trim_end_matches('/')),
            token: token.to_owned(),
            pace,
            last_request: None,
            remaining: None,
            reset_at: None,
            points_used: 0,
        })
    }

    /// Up to 100 repositories matching a search, starting after `after`.
    pub async fn search(&mut self, q: &str, after: Option<&str>) -> Result<Page> {
        let reply = self
            .query(
                &with_fragment(SEARCH_QUERY),
                json!({ "q": q, "after": after }),
            )
            .await?;
        let search = &reply.data["search"];
        Ok(Page {
            total: search["repositoryCount"].as_i64(),
            repos: parse_nodes(&search["nodes"])?,
            next: next_cursor(&search["pageInfo"]),
        })
    }

    /// Up to 100 public repositories `login` owns (forks only if `forks`), or None if there is no
    /// such user or organization.
    pub async fn owner_repos(
        &mut self,
        login: &str,
        after: Option<&str>,
        forks: bool,
    ) -> Result<Option<Page>> {
        let is_fork = if forks {
            Value::Null
        } else {
            Value::Bool(false)
        };
        let vars = json!({ "login": login, "after": after, "isFork": is_fork });
        let reply = self.query(&with_fragment(OWNER_QUERY), vars).await?;
        let repos = &reply.data["repositoryOwner"]["repositories"];
        if repos.is_null() {
            return Ok(None);
        }
        Ok(Some(Page {
            total: None,
            repos: parse_nodes(&repos["nodes"])?,
            next: next_cursor(&repos["pageInfo"]),
        }))
    }

    /// Looks up "owner/name"s (at most NAMES_PER_QUERY), following renames. Each result is the
    /// repository, or why there is none ("not_found", or GitHub's error type in lower case).
    pub async fn repos_by_name(&mut self, names: &[String]) -> Result<Vec<Result<Repo, String>>> {
        assert!(names.len() <= NAMES_PER_QUERY);
        let mut params = Vec::new();
        let mut fields = Vec::new();
        let mut vars = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let (owner, repo) = name
                .split_once('/')
                .with_context(|| format!("not owner/name: {name}"))?;
            params.push(format!("$o{i}: String!, $n{i}: String!"));
            fields.push(format!(
                "r{i}: repository(owner: $o{i}, name: $n{i}) {{ ...Repo }}"
            ));
            vars.insert(format!("o{i}"), owner.into());
            vars.insert(format!("n{i}"), repo.into());
        }
        let query = format!(
            "query({}) {{\n  rateLimit {{ cost remaining resetAt }}\n  {}\n}}",
            params.join(", "),
            fields.join("\n  ")
        );
        let reply = self
            .query(&with_fragment(&query), Value::Object(vars))
            .await?;
        let mut out = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let alias = format!("r{i}");
            let node = &reply.data[&alias];
            if node.is_null() {
                let kind = reply
                    .errors
                    .iter()
                    .find(|e| e.path.first().and_then(Value::as_str) == Some(alias.as_str()))
                    .and_then(|e| e.kind.as_deref())
                    .unwrap_or("NOT_FOUND");
                out.push(Err(kind.to_ascii_lowercase()));
            } else {
                out.push(Ok(serde_json::from_value(node.clone())
                    .with_context(|| format!("parsing {name}"))?));
            }
        }
        Ok(out)
    }

    async fn query(&mut self, query: &str, variables: Value) -> Result<Reply> {
        let body = json!({ "query": query, "variables": variables });
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.wait_turn().await;
            let resp = match self
                .http
                .post(&self.url)
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) if attempt < MAX_ATTEMPTS => {
                    backoff(attempt, &format!("GraphQL request failed: {e}")).await;
                    continue;
                }
                Err(e) => return Err(e).context("GraphQL request"),
            };
            let status = resp.status();
            let headers = resp.headers().clone();
            if status == StatusCode::UNAUTHORIZED {
                bail!("GitHub rejected the token (401): check GITHUB_TOKEN");
            }
            if status == StatusCode::FORBIDDEN || status == StatusCode::TOO_MANY_REQUESTS {
                let text = resp.text().await.unwrap_or_default();
                // Primary limits come with x-ratelimit-remaining: 0, secondary ones with
                // retry-after or only a message. Anything else is a real refusal.
                let wait = retry_after(&headers).or_else(|| reset_wait(&headers));
                let limited = wait.is_some() || text.to_ascii_lowercase().contains("rate limit");
                if !limited || attempt >= MAX_ATTEMPTS {
                    bail!("GitHub refused the query ({status}): {text}");
                }
                let wait = wait.unwrap_or(Duration::from_secs(60 << attempt.min(4)));
                log!("rate limited ({status}); waiting {}s", wait.as_secs());
                tokio::time::sleep(wait).await;
                continue;
            }
            if status.is_server_error() && attempt < MAX_ATTEMPTS {
                backoff(attempt, &format!("GraphQL returned {status}")).await;
                continue;
            }
            if !status.is_success() {
                bail!(
                    "GraphQL returned {status}: {}",
                    resp.text().await.unwrap_or_default()
                );
            }
            let reply: Reply = match resp.json().await {
                Ok(reply) => reply,
                Err(e) if attempt < MAX_ATTEMPTS => {
                    backoff(attempt, &format!("unreadable GraphQL reply: {e}")).await;
                    continue;
                }
                Err(e) => return Err(e).context("parsing GraphQL reply"),
            };
            if reply
                .errors
                .iter()
                .any(|e| e.kind.as_deref() == Some("RATE_LIMITED"))
                && attempt < MAX_ATTEMPTS
            {
                let wait = reset_wait(&headers).unwrap_or(Duration::from_secs(60));
                log!("GraphQL rate limit reached; waiting {}s", wait.as_secs());
                tokio::time::sleep(wait).await;
                continue;
            }
            if reply.data.is_null() {
                // Without data, the errors are about the whole query; GitHub reports its own
                // timeouts this way, so those are worth another try.
                let msg = reply
                    .errors
                    .iter()
                    .map(|e| e.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ");
                if attempt < MAX_ATTEMPTS && msg.to_ascii_lowercase().contains("timeout") {
                    backoff(attempt, &format!("GraphQL error: {msg}")).await;
                    continue;
                }
                bail!("GraphQL error: {msg}");
            }
            self.note_rate_limit(&reply.data["rateLimit"]);
            return Ok(reply);
        }
    }

    fn note_rate_limit(&mut self, rate: &Value) {
        self.points_used += rate["cost"].as_i64().unwrap_or(1);
        self.remaining = rate["remaining"].as_i64();
        self.reset_at = rate["resetAt"].as_str().and_then(|s| s.parse().ok());
    }

    /// Paces requests, and sleeps until the hourly reset once the points run low.
    async fn wait_turn(&mut self) {
        if let (Some(remaining), Some(reset_at)) = (self.remaining, self.reset_at)
            && remaining < RESERVE_POINTS
        {
            let wait =
                (reset_at - Utc::now()).to_std().unwrap_or_default() + Duration::from_secs(5);
            log!(
                "{remaining} GraphQL points left; waiting {}s for the reset at {reset_at}",
                wait.as_secs()
            );
            tokio::time::sleep(wait).await;
            self.remaining = None;
        }
        if let Some(last) = self.last_request {
            tokio::time::sleep(self.pace.saturating_sub(last.elapsed())).await;
        }
        self.last_request = Some(Instant::now());
    }
}

fn with_fragment(query: &str) -> String {
    format!("{query}\n{REPO_FRAGMENT}")
}

fn parse_nodes(nodes: &Value) -> Result<Vec<Repo>> {
    let nodes = nodes.as_array().map(Vec::as_slice).unwrap_or_default();
    nodes
        .iter()
        .filter(|n| !n.is_null())
        .map(|n| serde_json::from_value(n.clone()).context("parsing a repository from GraphQL"))
        .collect()
}

fn next_cursor(page_info: &Value) -> Option<String> {
    if page_info["hasNextPage"].as_bool() == Some(true) {
        page_info["endCursor"].as_str().map(str::to_owned)
    } else {
        None
    }
}

/// The wait a retry-after header asks for.
pub(crate) fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let secs: u64 = headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// The wait until x-ratelimit-reset, when x-ratelimit-remaining says the limit is used up.
fn reset_wait(headers: &HeaderMap) -> Option<Duration> {
    if headers.get("x-ratelimit-remaining")?.to_str().ok()? != "0" {
        return None;
    }
    let reset: i64 = headers
        .get("x-ratelimit-reset")?
        .to_str()
        .ok()?
        .parse()
        .ok()?;
    let secs = (reset - Utc::now().timestamp()).max(0) as u64;
    Some(Duration::from_secs(secs + 5))
}

/// Sleeps 5s, 10s, 20s, ... (at most 5 minutes) before retry `attempt`.
async fn backoff(attempt: u32, why: &str) {
    let wait = Duration::from_secs((5u64 << (attempt - 1).min(6)).min(300));
    log!("{why}; retrying in {}s", wait.as_secs());
    tokio::time::sleep(wait).await;
}
