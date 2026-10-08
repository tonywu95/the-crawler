//! discover → plan → fetch → status against a fake GitHub (GraphQL and codeload).

use std::collections::BTreeMap;
use std::io::Read;
use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::fetch::tests::tarball;
use super::fetch::{RepoRecord, files_path};
use super::frontier::Frontier;
use super::{discover, fetch, plan, status};
use crate::store::Store;

const MIT: &str = "Copyright (c) 2024\n\nPermission is hereby granted, free of charge, to any person obtaining a copy";
const GPL: &str = "GNU GENERAL PUBLIC LICENSE\nVersion 3, 29 June 2007";

fn sha(id: i64) -> String {
    format!("{id:040x}")
}

fn repo(id: i64, license: &str) -> Value {
    json!({
        "databaseId": id, "nameWithOwner": format!("octo/r{id}"), "url": format!("https://github.com/octo/r{id}"),
        "description": "a repository", "isFork": false, "isMirror": false, "isArchived": false,
        "isDisabled": false, "isEmpty": false, "isPrivate": false, "isLocked": false,
        "diskUsage": 10, "stargazerCount": id, "forkCount": 0,
        "createdAt": "2020-01-01T00:00:00Z", "pushedAt": "2024-06-01T00:00:00Z",
        "primaryLanguage": {"name": "Rust"}, "licenseInfo": {"spdxId": license},
        "repositoryTopics": {"nodes": [{"topic": {"name": "cli"}}]},
        "defaultBranchRef": {"name": "main", "target": {"oid": sha(id)}}
    })
}

fn page(nodes: Vec<Value>, next: Option<&str>) -> Value {
    json!({ "pageInfo": { "hasNextPage": next.is_some(), "endCursor": next }, "nodes": nodes })
}

/// Answers the three queries discover sends.
struct FakeGraphql;

impl Respond for FakeGraphql {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let (query, vars) = (body["query"].as_str().unwrap(), &body["variables"]);
        let after = vars["after"].as_str();
        let mut data = json!({ "rateLimit": { "cost": 1, "remaining": 4999, "resetAt": "2030-01-01T00:00:00Z" } });
        let mut errors = Vec::new();
        if query.contains("search(") {
            let q = vars["q"].as_str().unwrap();
            assert!(
                q.starts_with("language:rust license:mit"),
                "searches are expanded per accepted license: {q}"
            );
            data["search"] = if !q.contains("created:") {
                // Too many to return: discover must split this by date.
                let mut p = page(vec![repo(100, "MIT")], None);
                p["repositoryCount"] = 1500.into();
                p
            } else {
                // The older half finds 101-103, the newer 201-203, two pages each.
                let base = if q.contains("created:2007-10-01T00:00:00+00:00..") {
                    100
                } else {
                    200
                };
                let mut third = repo(base + 3, "GPL-3.0");
                if base == 200 {
                    third = repo(base + 3, "MIT");
                    third["isFork"] = true.into();
                }
                let mut p = match after {
                    None => page(
                        vec![repo(base + 1, "MIT"), repo(base + 2, "MIT")],
                        Some("p2"),
                    ),
                    Some(_) => page(vec![third], None),
                };
                p["repositoryCount"] = 3.into();
                p
            };
        } else if query.contains("repositoryOwner(") {
            assert_eq!(vars["isFork"], false, "forks are filtered by GitHub");
            data["repositoryOwner"] = match (vars["login"].as_str().unwrap(), after) {
                ("octo-org", None) => {
                    json!({ "repositories": page(vec![repo(301, "MIT"), repo(302, "MIT")], Some("o2")) })
                }
                ("octo-org", Some(_)) => {
                    json!({ "repositories": page(vec![repo(303, "NOASSERTION")], None) })
                }
                _ => Value::Null,
            };
        } else {
            let mut i = 0;
            while let Some(name) = vars[format!("n{i}")].as_str() {
                if name == "cat" {
                    data[format!("r{i}")] = repo(401, "MIT");
                } else {
                    data[format!("r{i}")] = Value::Null;
                    errors.push(json!({ "type": "NOT_FOUND", "path": [format!("r{i}")], "message": "Could not resolve" }));
                }
                i += 1;
            }
        }
        ResponseTemplate::new(200).set_body_json(json!({ "data": data, "errors": errors }))
    }
}

/// Serves each repository's tarball: its own code, a file every repository shares, and files
/// that must not be kept. Some repositories are broken on purpose.
struct FakeCodeload;

impl Respond for FakeCodeload {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let parts: Vec<&str> = req.url.path().split('/').collect(); // ["", owner, name, "tar.gz", sha]
        let (name, commit) = (parts[2], parts[4]);
        let id: i64 = name.trim_start_matches('r').parse().unwrap();
        let own = format!("pub fn f{id}() -> u32 {{\n    {id}\n}}\n");
        let files = [
            ("LICENSE", MIT),
            ("src/lib.rs", own.as_str()),
            ("src/shared.rs", "pub const SHARED: u32 = 1;\n"),
            ("vendor/dep/dep.rs", "pub fn dep() {}\n"),
            (
                "src/gpl.c",
                "/* SPDX-License-Identifier: GPL-2.0 */\nint x = 1;\n",
            ),
            ("dist/app.min.js", "var a=1;\n"),
        ];
        let body = match id {
            201 => return ResponseTemplate::new(404),
            102 => tarball(
                name,
                commit,
                &[("COPYING", GPL), ("main.rs", "fn main() {}\n")],
            ),
            301 => tarball(name, &sha(999), &files),
            _ => tarball(name, commit, &files),
        };
        ResponseTemplate::new(200).set_body_bytes(body)
    }
}

fn gunzip_lines(bytes: &[u8]) -> Vec<Value> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn crawls_end_to_end() {
    let github = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(FakeGraphql)
        .mount(&github)
        .await;
    // 202 fails once with a 503 asking for an immediate retry, then works.
    Mock::given(method("GET"))
        .and(path(format!("/octo/r202/tar.gz/{}", sha(202))))
        .respond_with(ResponseTemplate::new(503).insert_header("retry-after", "0"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&github)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/octo/r[0-9]+/tar.gz/[0-9a-f]{40}$"))
        .respond_with(FakeCodeload)
        .mount(&github)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let seeds = dir.path().join("seeds.yaml");
    std::fs::write(
        &seeds,
        "searches: [\"language:rust\"]\nowners: [octo-org, ghost]\nrepos: [octo/cat, https://github.com/octo/missing]\naccept:\n  licenses: [MIT]\n",
    )
    .unwrap();
    let db = dir.path().join("frontier.sqlite");
    let store = dir.path().join("store").to_str().unwrap().to_owned();

    let discover_args = discover::DiscoverArgs {
        seeds: seeds.clone(),
        db: db.clone(),
        api_url: github.uri(),
        token: "t".into(),
        max_points: None,
        pace: Duration::ZERO,
    };
    discover::run(&discover_args).await.unwrap();
    let s = Frontier::open(&db).unwrap().stats().unwrap();
    assert_eq!(
        (s.accepted, s.rejected, s.names_missing),
        (8, 3, 1),
        "{s:?}"
    );
    assert_eq!(
        s.seeds,
        vec![("owner".into(), 2, 2), ("search".into(), 3, 3)]
    );
    let mut reasons = s.reasons.clone();
    reasons.sort();
    assert_eq!(
        reasons,
        vec![
            ("fork".into(), 1),
            ("license:GPL-3.0".into(), 1),
            ("license:NOASSERTION".into(), 1)
        ]
    );

    // Everything is done, so a rerun asks GitHub nothing.
    let asked = github.received_requests().await.unwrap().len();
    discover::run(&discover_args).await.unwrap();
    assert_eq!(github.received_requests().await.unwrap().len(), asked);

    let plan_args = plan::PlanArgs {
        db: db.clone(),
        store: store.clone(),
        batch: Some("b1".into()),
        shard_size: 3,
        max_repos: None,
    };
    let info = plan::run(&plan_args).await.unwrap().unwrap();
    assert_eq!((info.items, info.shards), (8, 3));
    let s = Store::open(&store).unwrap();
    let first_shard = s
        .get("batches/b1/shard-00000.jsonl")
        .await
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(first_shard)
            .unwrap()
            .starts_with("{\"id\":401,"),
        "most-starred first"
    );
    let again = plan::PlanArgs {
        batch: Some("b2".into()),
        ..plan_args
    };
    assert!(
        plan::run(&again).await.unwrap().is_none(),
        "everything is planned"
    );

    // Shards: [401, 302, 301], [202, 201, 102], [101, 100].
    let fetch_args = || fetch::FetchArgs {
        store: store.clone(),
        batch: "b1".into(),
        rank: 0,
        world: 1,
        jobs: 2,
        codeload_url: github.uri(),
        pause_min: Duration::ZERO,
        pause_max: Duration::ZERO,
        max_rate: None,
        max_archive_bytes: 1 << 20,
        max_errors: 6,
        keep_archives: true,
    };
    fetch::run(fetch_args()).await.unwrap();

    let mut repos = BTreeMap::new();
    let mut files = Vec::new();
    for k in 0..3 {
        let manifest = s
            .get(&format!("manifests/b1/shard-{k:05}.jsonl"))
            .await
            .unwrap()
            .unwrap();
        for line in String::from_utf8(manifest).unwrap().lines() {
            let record: RepoRecord = serde_json::from_str(line).unwrap();
            repos.insert(record.repo.id, record);
        }
        files.extend(gunzip_lines(
            &s.get(&files_path("b1", k)).await.unwrap().unwrap(),
        ));
    }
    let outcome = |id: i64| {
        let r = &repos[&id];
        format!(
            "{} {} {}",
            r.status,
            r.reason.as_deref().unwrap_or("-"),
            r.files
        )
    };
    assert_eq!(repos.len(), 8);
    // The first repository of each shard keeps the shared file; later ones drop it as a duplicate.
    for (id, kept) in [(401, 2), (302, 1), (202, 2), (101, 2), (100, 1)] {
        assert_eq!(outcome(id), format!("fetched - {kept}"));
    }
    assert_eq!(outcome(102), "failed license_check:text_mismatch 0");
    assert_eq!(outcome(201), "failed not_found 0");
    assert_eq!(
        outcome(301),
        format!("failed commit_mismatch:{} 0", sha(999))
    );

    let r302 = &repos[&302];
    let dropped: Vec<(&str, u64)> = r302.dropped.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    assert_eq!(
        dropped,
        vec![
            ("duplicate", 1),
            ("file_license", 1),
            ("generated", 1),
            ("license_file", 1),
            ("vendored", 1)
        ]
    );
    assert_eq!(r302.license_file.as_deref(), Some("LICENSE"));
    assert!(
        r302.license_text
            .as_deref()
            .unwrap()
            .contains("Permission is hereby granted")
    );

    assert_eq!(files.len(), 8);
    let mut paths: Vec<String> = files
        .iter()
        .map(|f| {
            format!(
                "{} {}",
                f["repo"].as_str().unwrap(),
                f["path"].as_str().unwrap()
            )
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "octo/r100 src/lib.rs",
            "octo/r101 src/lib.rs",
            "octo/r101 src/shared.rs",
            "octo/r202 src/lib.rs",
            "octo/r202 src/shared.rs",
            "octo/r302 src/lib.rs",
            "octo/r401 src/lib.rs",
            "octo/r401 src/shared.rs",
        ]
    );
    let f = files
        .iter()
        .find(|f| f["repo"] == "octo/r401" && f["path"] == "src/lib.rs")
        .unwrap();
    assert_eq!(
        (
            f["language"].as_str(),
            f["license"].as_str(),
            f["stars"].as_i64()
        ),
        (Some("Rust"), Some("MIT"), Some(401))
    );
    assert_eq!(
        (f["repo_id"].as_i64(), f["commit"].as_str()),
        (Some(401), Some(sha(401).as_str()))
    );
    assert_eq!(f["text"], "pub fn f401() -> u32 {\n    401\n}\n");
    assert_eq!(
        f["blob_id"],
        hex::encode(super::extract::blob_id(
            f["text"].as_str().unwrap().as_bytes()
        ))
    );

    // --keep-archives kept the tarballs of the repositories that passed, and only those.
    assert!(
        s.exists(&format!("archives/01/401/{}.tar.gz", sha(401)))
            .await
            .unwrap()
    );
    assert!(
        !s.exists(&format!("archives/02/102/{}.tar.gz", sha(102)))
            .await
            .unwrap()
    );

    // Every shard has a manifest, so a rerun downloads nothing.
    let asked = github.received_requests().await.unwrap().len();
    fetch::run(fetch_args()).await.unwrap();
    assert_eq!(github.received_requests().await.unwrap().len(), asked);

    status::run(&db, Some(&store), None).await.unwrap();

    // For schema validation: GRAPHQL_DUMP=path writes every GraphQL request discover sent.
    if let Ok(dump) = std::env::var("GRAPHQL_DUMP") {
        let requests = github.received_requests().await.unwrap();
        let bodies: Vec<String> = requests
            .iter()
            .filter(|r| r.url.path() == "/graphql")
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .collect();
        std::fs::write(dump, bodies.join("\n")).unwrap();
    }
}

#[tokio::test]
async fn fetch_stops_after_errors_in_a_row_and_resumes() {
    let github = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
        .mount(&github)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().to_str().unwrap().to_owned();
    let meta: Vec<super::RepoMeta> = [1, 2, 3]
        .iter()
        .map(|&id| {
            super::RepoMeta::from_repo(&serde_json::from_value(repo(id, "MIT")).unwrap()).unwrap()
        })
        .collect();
    crate::shards::write_batch(&Store::open(&store).unwrap(), "github", "b", &meta, 10, 0)
        .await
        .unwrap();
    let args = || fetch::FetchArgs {
        store: store.clone(),
        batch: "b".into(),
        rank: 0,
        world: 1,
        jobs: 1,
        codeload_url: github.uri(),
        pause_min: Duration::ZERO,
        pause_max: Duration::ZERO,
        max_rate: None,
        max_archive_bytes: 1 << 20,
        max_errors: 4,
        keep_archives: false,
    };
    let err = fetch::run(args()).await.unwrap_err();
    assert!(err.to_string().contains("rerun"), "{err:#}");
    // Repository 1 gave up after 3 attempts; the job stopped on repository 2's first.
    assert_eq!(github.received_requests().await.unwrap().len(), 4);
    let s = Store::open(&store).unwrap();
    assert!(
        !s.exists("manifests/b/shard-00000.jsonl").await.unwrap(),
        "an unfinished shard has no manifest"
    );
    assert!(!s.exists(&files_path("b", 0)).await.unwrap());

    // Once GitHub answers again, the same command does the whole shard.
    github.reset().await;
    Mock::given(method("GET"))
        .respond_with(FakeCodeload)
        .mount(&github)
        .await;
    fetch::run(args()).await.unwrap();
    let manifest = String::from_utf8(
        s.get("manifests/b/shard-00000.jsonl")
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest.matches("\"status\":\"fetched\"").count(),
        3,
        "{manifest}"
    );
    assert_eq!(
        gunzip_lines(&s.get(&files_path("b", 0)).await.unwrap().unwrap()).len(),
        4
    );
}
