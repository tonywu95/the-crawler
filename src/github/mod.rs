//! The GitHub crawler: public repositories under permissive licenses, kept as one source snapshot
//! per repository, a tarball of the default branch at the commit discover saw.

pub mod api;
pub mod config;
pub mod discover;
pub mod fetch;
pub mod frontier;
pub mod license;
pub mod plan;
pub mod status;
#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};

use api::Repo;

/// One line of a shard: what a worker needs to fetch a repository, and its metadata from discover.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepoMeta {
    /// GitHub's repository id, stable across renames and transfers.
    pub id: i64,
    pub full_name: String,
    pub url: String,
    pub branch: String,
    /// The commit to fetch: the head of `branch` when discover checked the repository.
    pub sha: String,
    /// The SPDX id GitHub detected.
    pub license: String,
    pub stars: i64,
    pub forks: i64,
    pub size_kb: i64,
    pub language: Option<String>,
    pub topics: Vec<String>,
    pub description: Option<String>,
    pub archived: bool,
    pub created_at: String,
    pub pushed_at: Option<String>,
}

impl RepoMeta {
    /// None unless the repository has what accepted ones always have: an id, a license and a head.
    pub fn from_repo(r: &Repo) -> Option<Self> {
        let (branch, sha) = r.head()?;
        Some(Self {
            id: r.database_id?,
            full_name: r.name_with_owner.clone(),
            url: r.url.clone(),
            branch: branch.to_owned(),
            sha: sha.to_owned(),
            license: r.spdx()?.to_owned(),
            stars: r.stargazer_count,
            forks: r.fork_count,
            size_kb: r.disk_usage.unwrap_or(0),
            language: r.primary_language.as_ref().map(|l| l.name.clone()),
            topics: r
                .repository_topics
                .nodes
                .iter()
                .map(|t| t.topic.name.clone())
                .collect(),
            description: r.description.clone(),
            archived: r.is_archived,
            created_at: r.created_at.clone(),
            pushed_at: r.pushed_at.clone(),
        })
    }

    /// repos/<id % 100>/<id>/<sha>: keyed by id so renames don't matter, and by commit so a later
    /// crawl of the same repository adds a snapshot instead of replacing one.
    fn stem(&self) -> String {
        format!("repos/{:02}/{}/{}", self.id % 100, self.id, self.sha)
    }

    pub fn archive_path(&self) -> String {
        format!("{}.tar.gz", self.stem())
    }

    /// The fetch record, written last: the repository is done once this exists.
    pub fn record_path(&self) -> String {
        format!("{}.json", self.stem())
    }
}
