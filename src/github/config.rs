//! The seeds file: where discover looks for repositories, and which ones it accepts.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use serde::Deserialize;

use super::api::Repo;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// GitHub search queries, such as "license:mit language:rust stars:>=50". A search that matches
    /// more than the 1,000 results GitHub returns is split by creation date until each part fits.
    #[serde(default)]
    pub searches: Vec<String>,
    /// Users and organizations whose public repositories to check.
    #[serde(default)]
    pub owners: Vec<String>,
    /// Repositories as owner/name (or github.com URLs).
    #[serde(default)]
    pub repos: Vec<String>,
    /// Files of repositories, one per line; # starts a comment. Relative to the seeds file.
    #[serde(default)]
    pub repo_files: Vec<PathBuf>,
    #[serde(default)]
    pub accept: Accept,
}

/// Ok if a repository is accepted, else why it was rejected.
pub type Verdict = Result<(), String>;

/// Which repositories discover accepts. Rejected ones stay in the frontier with the reason.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Accept {
    /// SPDX ids as GitHub reports them, compared ignoring case.
    pub licenses: Vec<String>,
    pub min_stars: i64,
    /// Upper bound on GitHub's disk usage for the repository, history included.
    pub max_size_mb: i64,
    pub forks: bool,
    pub mirrors: bool,
    pub archived: bool,
    /// Reject repositories last pushed before this date (YYYY-MM-DD).
    pub pushed_after: Option<NaiveDate>,
}

/// Permissive licenses: use, change and redistribution allowed, with at most an attribution notice.
pub const PERMISSIVE: &[&str] = &[
    "MIT",
    "MIT-0",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "0BSD",
    "Unlicense",
    "CC0-1.0",
    "Zlib",
    "BSL-1.0",
];

impl Default for Accept {
    fn default() -> Self {
        Self {
            licenses: PERMISSIVE.iter().map(|s| s.to_string()).collect(),
            min_stars: 0,
            max_size_mb: 1024,
            forks: false,
            mirrors: false,
            archived: true,
            pushed_after: None,
        }
    }
}

impl Accept {
    /// Ok if the repository should be fetched, else the reason it is rejected.
    pub fn check(&self, r: &Repo) -> Verdict {
        let flags = [
            (r.is_private, "private"),
            (r.is_disabled, "disabled"),
            (r.is_locked, "locked"),
            (r.is_empty, "empty"),
            (r.is_fork && !self.forks, "fork"),
            (r.is_mirror && !self.mirrors, "mirror"),
            (r.is_archived && !self.archived, "archived"),
        ];
        if let Some((_, reason)) = flags.iter().find(|(set, _)| *set) {
            return Err(reason.to_string());
        }
        match r.spdx() {
            None => return Err("license:none".into()),
            Some(id) if !self.licenses.iter().any(|l| l.eq_ignore_ascii_case(id)) => {
                return Err(format!("license:{id}"));
            }
            Some(_) => {}
        }
        if r.head().is_none() {
            return Err("no_branch".into());
        }
        if r.stargazer_count < self.min_stars {
            return Err("stars".into());
        }
        if r.disk_usage.unwrap_or(0) > self.max_size_mb * 1024 {
            return Err("too_large".into());
        }
        if let Some(after) = self.pushed_after {
            let pushed = r
                .pushed_at
                .as_deref()
                .and_then(|p| chrono::DateTime::parse_from_rfc3339(p).ok());
            if pushed.is_none_or(|p| p.date_naive() < after) {
                return Err("stale".into());
            }
        }
        Ok(())
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: Config =
            serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        if cfg.accept.licenses.is_empty() {
            bail!("accept.licenses is empty: nothing would be accepted");
        }
        let base = path.parent().unwrap_or(Path::new("."));
        for file in &mut cfg.repo_files {
            if file.is_relative() {
                *file = base.join(&*file);
            }
        }
        Ok(cfg)
    }

    /// Every repository named in `repos` and `repo_files`, as owner/name.
    pub fn repo_names(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for r in &self.repos {
            out.push(parse_name(r).with_context(|| format!("in repos: {r:?}"))?);
        }
        for file in &self.repo_files {
            let text = std::fs::read_to_string(file)
                .with_context(|| format!("reading {}", file.display()))?;
            for (i, line) in text.lines().enumerate() {
                let line = line.split('#').next().unwrap_or("").trim();
                if !line.is_empty() {
                    out.push(
                        parse_name(line)
                            .with_context(|| format!("{}:{}", file.display(), i + 1))?,
                    );
                }
            }
        }
        Ok(out)
    }
}

/// "owner/name", "https://github.com/owner/name" or "github.com/owner/name.git" → "owner/name".
pub fn parse_name(s: &str) -> Result<String> {
    let s = s.trim().trim_end_matches('/');
    let s = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
        .unwrap_or(s);
    let s = s.strip_prefix("github.com/").unwrap_or(s);
    let s = s.strip_suffix(".git").unwrap_or(s);
    let Some((owner, name)) = s.split_once('/') else {
        bail!("not owner/name")
    };
    let owner_ok =
        !owner.is_empty() && owner.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    let name_ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
    if !owner_ok || !name_ok {
        bail!("not owner/name");
    }
    Ok(format!("{owner}/{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn repo() -> Repo {
        serde_json::from_value(serde_json::json!({
            "databaseId": 42, "nameWithOwner": "octo/cat", "url": "https://github.com/octo/cat",
            "description": null, "isFork": false, "isMirror": false, "isArchived": false,
            "isDisabled": false, "isEmpty": false, "isPrivate": false, "isLocked": false,
            "diskUsage": 2048, "stargazerCount": 10, "forkCount": 1,
            "createdAt": "2020-01-01T00:00:00Z", "pushedAt": "2024-06-01T00:00:00Z",
            "primaryLanguage": {"name": "Rust"}, "licenseInfo": {"spdxId": "MIT"},
            "repositoryTopics": {"nodes": []},
            "defaultBranchRef": {"name": "main", "target": {"oid": "0123456789abcdef0123456789abcdef01234567"}}
        }))
        .unwrap()
    }

    #[test]
    fn accepts_and_rejects() {
        let accept = Accept::default();
        assert_eq!(accept.check(&repo()), Ok(()));

        let mut r = repo();
        r.is_fork = true;
        assert_eq!(accept.check(&r), Err("fork".into()));

        let mut r = repo();
        r.license_info = Some(super::super::api::LicenseInfo {
            spdx_id: Some("GPL-3.0".into()),
        });
        assert_eq!(accept.check(&r), Err("license:GPL-3.0".into()));
        r.license_info = None;
        assert_eq!(accept.check(&r), Err("license:none".into()));

        let mut r = repo();
        r.disk_usage = Some(2 * 1024 * 1024);
        assert_eq!(accept.check(&r), Err("too_large".into()));

        let strict = Accept {
            min_stars: 100,
            ..Accept::default()
        };
        assert_eq!(strict.check(&repo()), Err("stars".into()));

        let recent = Accept {
            pushed_after: NaiveDate::from_ymd_opt(2025, 1, 1),
            ..Accept::default()
        };
        assert_eq!(recent.check(&repo()), Err("stale".into()));

        let mut r = repo();
        r.default_branch_ref = None;
        assert_eq!(accept.check(&r), Err("no_branch".into()));
    }

    #[test]
    fn parses_names() {
        assert_eq!(parse_name("octo/cat").unwrap(), "octo/cat");
        assert_eq!(
            parse_name("https://github.com/octo/cat.js.git").unwrap(),
            "octo/cat.js"
        );
        assert_eq!(parse_name(" github.com/octo/cat/ ").unwrap(), "octo/cat");
        assert!(parse_name("octo").is_err());
        assert!(parse_name("octo/cat/tree/main").is_err());
        assert!(parse_name("oc to/cat").is_err());
    }

    #[test]
    fn loads_seeds() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("names.txt"),
            "# list\nocto/one\n\nocto/two  # trailing\n",
        )
        .unwrap();
        let seeds = dir.path().join("seeds.yaml");
        std::fs::write(
            &seeds,
            "repos: [octo/cat]\nrepo_files: [names.txt]\naccept:\n  min_stars: 5\n",
        )
        .unwrap();
        let cfg = Config::load(&seeds).unwrap();
        assert_eq!(
            cfg.repo_names().unwrap(),
            vec!["octo/cat", "octo/one", "octo/two"]
        );
        assert_eq!(cfg.accept.min_stars, 5);
        assert!(cfg.accept.licenses.iter().any(|l| l == "MIT"));

        std::fs::write(&seeds, "accept:\n  min_star: 5\n").unwrap();
        assert!(
            Config::load(&seeds).is_err(),
            "typos in the seeds file are errors"
        );
    }

    #[test]
    fn example_seeds_load_with_the_defaults() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("github-seeds.example.yaml");
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.repo_names().unwrap(), vec!["tokio-rs/tokio"]);
        assert_eq!(cfg.accept.licenses, Accept::default().licenses);
    }
}
