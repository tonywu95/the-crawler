# the-crawler

Crawlers for openly licensed data, written in Rust. Their output feeds Astra's data mixes.

| Binary | Crawls | Status |
| --- | --- | --- |
| `youtube-crawl` | Creative Commons (CC BY) YouTube video | Designed below; no code yet |
| `github-crawl` | Public GitHub repositories under permissive licenses | Implemented |

Both have the same shape (`discover`, `plan`, `fetch`, `status`) and write the same batch, shard and manifest layout. That shared part is in `src/store.rs` and `src/shards.rs`. Give each crawler its own store (a directory, or a prefix such as `s3://bucket/github`), so their batches stay apart.

## YouTube crawler

### Design

Four subcommands of one binary, `youtube-crawl`:

| Command | Runs on | Does |
| --- | --- | --- |
| `discover` | one coordinator | Spends the day's YouTube Data API v3 quota. Turns seeds (search queries, channels, video ids, id files) into video ids, then checks each one with `videos.list`: license, duration, live, age and made-for-kids. Results go to a SQLite frontier. |
| `plan` | the coordinator | Cuts accepted, not-yet-planned videos into a new batch of shards: `batches/<batch>/shard-NNNNN.jsonl` in the store. |
| `fetch` | any number of workers | Worker `rank` of `world` (× `--jobs`) takes every shard k with k % slots == slot. It downloads each video with yt-dlp (subprocess) and writes the media, captions and a record. No coordinator is involved. |
| `status` | anywhere | Frontier counts, quota used today, and per-batch progress, GB and hours, read from manifests. |

Store layout (a local path, or s3:// / gs:// through `object_store`):

```
batches/<batch>/batch.json             written last by plan
batches/<batch>/shard-00000.jsonl      one video per line, metadata from the API
videos/<id[:2]>/<id>.mp4               video only, ≤720p, avc1 preferred
videos/<id[:2]>/<id>.en.vtt            uploaded and automatic captions
videos/<id[:2]>/<id>.json              fetch record, written last: the video is done once this exists
manifests/<batch>/shard-00000.jsonl    written when a shard finishes: one line per video, fetched or failed
```

Restarts are exact. A worker skips any shard that has a manifest and any video that has a record, so rerunning the same command picks up where it stopped.

### Rules it follows

- **License.** Only videos whose API `status.license` is `creativeCommon` (CC BY 3.0) are accepted. The worker checks again on the watch page just before downloading, and skips the video if the page no longer shows a Creative Commons license. Attribution (title, channel, URL) is kept in every record.
- **Politeness.** One worker per IP address by default, a random pause after each video, and a per-job rate cap. On "confirm you're not a bot" or HTTP 429 the worker backs off exponentially, then stops after a few in a row. No proxies, cookies, or other ways around YouTube's limits.
- **Quota.** search costs 100 units; videos.list, playlistItems.list and channels.list cost 1 per call of up to 50. The default quota is 10,000 units per day, reset at midnight Pacific. Checking a list of known ids costs about 1 unit per 50, so roughly 500k ids per day per key; search is the expensive path.

### Verified against YouTube (2026-10-07, yt-dlp 2026.08.19, deno 2.9.7)

- On a CC video (`M4gD1WSo5mA`), yt-dlp's `license` field is "Creative Commons Attribution license (reuse allowed)". On a standard-license video it is null, so the page check requires the CC string to be present rather than accepting "absent". Because a YouTube layout change could make that string vanish everywhere, misses count toward the worker's error streak.
- `-f "bv[height<=720][vcodec^=avc1]/bv[height<=720]/b[height<=720]"` selects format 136 (720p avc1) on that video, with no merge or ffmpeg needed.
- `--match-filters "license~='(?i)creative commons'"` lets the CC video through. On a standard-license video (`uGpuVWrhIzE`), yt-dlp prints "does not pass filter", exits 0 and writes nothing, so "exit 0 with no files" means the page check failed.

## GitHub crawler

### Design

Four subcommands of one binary, `github-crawl`:

| Command | Runs on | Does |
| --- | --- | --- |
| `discover` | one coordinator | Spends the token's GraphQL points. Turns seeds (search queries, users and organizations, repository names, name files) into checked repositories in a SQLite frontier: license, fork, mirror, size, stars, and the default branch's head commit. Searches that match more than GitHub's 1,000-result cap are split by creation date until each part fits. |
| `plan` | the coordinator | Cuts accepted, not-yet-planned repositories into a new batch of shards, most-starred first: `batches/<batch>/shard-NNNNN.jsonl` in the store. |
| `fetch` | any number of workers | Worker `rank` of `world` (× `--jobs`) takes every shard k with k % slots == slot. It downloads each repository's snapshot from codeload.github.com, checks it, and writes the tarball and a record. Workers need no token, and no coordinator is involved. |
| `status` | anywhere | Frontier counts and top rejection reasons, GraphQL points left, and per-batch progress and GB, read from manifests. |

Store layout (a local path, or s3:// / gs:// through `object_store`):

```
batches/<batch>/batch.json                 written last by plan
batches/<batch>/shard-00000.jsonl          one repository per line: id, name, commit, license, stars, language, topics, ...
repos/<id % 100>/<id>/<sha>.tar.gz         `git archive` of that commit, as codeload serves it
repos/<id % 100>/<id>/<sha>.json           fetch record, written last: the repository is done once this exists
manifests/<batch>/shard-00000.jsonl        written when a shard finishes: one line per repository, fetched or failed
```

Snapshots are keyed by GitHub's numeric repository id, which survives renames and transfers, and by commit, so a later crawl of the same repository adds a snapshot rather than replacing one. Restarts are exact, as for YouTube. Discover saves its cursor after every page of results. A worker skips any shard that has a manifest and any repository that has a record.

### Rules it follows

- **License.** By default, only repositories whose GitHub-detected license (`licenseInfo.spdxId`) is one of MIT, MIT-0, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, 0BSD, Unlicense, CC0-1.0, Zlib or BSL-1.0 are accepted. The list is the seeds file's `accept.licenses`. "NOASSERTION" (a license GitHub could not identify) and no license are rejected. Before keeping a snapshot, the worker checks again: the tarball must have a top-level license file (LICENSE, COPYING, LICENSE-MIT, ...) whose text matches that license. A missing or different license file means the snapshot is not stored. The record keeps that file's text, with the repository's name and URL, for attribution.
- **What a snapshot is.** The default branch at the commit discover saw, pinned by sha. The worker confirms the commit id in the tarball's pax header matches. Git submodules are not included (codeload leaves them out), so code under other repositories' licenses doesn't come along. Git LFS files are pointer files unless the repository opted in to including LFS objects in its archives.
- **Politeness.** Discover sends one query at a time, at least 500 ms apart. It sleeps until the hourly reset when fewer than 100 points are left, and waits out secondary rate limits (`retry-after`). Workers run one job per IP address by default, pause randomly for 1 to 3 s after each download, and cap each job at 10 MB/s. On HTTP 403, 429 or 5xx a worker backs off exponentially from 30 s, honoring `retry-after`. It stops after 6 failed requests in a row, leaving the unfinished shard without a manifest so a rerun retries it. No proxies or ways around GitHub's limits.
- **Points.** A token gets 5,000 GraphQL points an hour. Every query here costs about 1 point and returns up to 100 repositories: a page of search or owner results, or 100 repositories looked up by name. That is roughly 500k repositories checked per hour. `--max-points` caps a run.

### Verified against GitHub (2026-10-07)

- `codeload.github.com/<owner>/<name>/tar.gz/<sha>` serves that commit's `git archive`. The top directory is `<name>-<sha>/`, and the pax global header's `comment` is the full commit id. Checked by running `fetch` on this repository's own commit (`ad714df`): downloaded, commit matched, then rejected with `license_check:no_license_file` because this repository has no LICENSE. A made-up commit gets HTTP 404, recorded as `not_found`.
- The GraphQL queries discover sends (search, owner, batched lookups by name) were validated against GitHub's published schema (`@octokit/graphql-schema` 15.26.1): fields, enum arguments and variable types. They have not run against the live GraphQL API, because it was blocked where this was built. On a first real run, use a small `--max-points` and check the frontier with `status`.
- `cargo test` runs discover → plan → fetch → status against a fake GitHub. That covers search splitting, pagination, missing names and owners, a 503 retried after `retry-after`, a 404, a commit mismatch, a license-text mismatch, reruns that make no requests, and a worker that stops after errors in a row and resumes.

### Use

```bash
export GITHUB_TOKEN=...      # any token: reading public data needs no scopes
cp github-seeds.example.yaml github-seeds.yaml    # searches, owners, repos, accept policy
github-crawl discover --seeds github-seeds.yaml   # frontier in data/github.sqlite
github-crawl plan --store s3://bucket/github      # prints the batch name
github-crawl fetch --store s3://bucket/github --batch 20261007-215300 --rank 0 --world 4
github-crawl status --store s3://bucket/github
```

## Setup

```bash
uv sync                      # yt-dlp + deno into .venv/bin (YouTube only)
cargo build --release        # target/release/youtube-crawl and github-crawl
export YOUTUBE_API_KEY=...   # a Google Cloud project with YouTube Data API v3 enabled
export GITHUB_TOKEN=...      # for github-crawl discover and status
```

Cloud stores take credentials from the usual `AWS_*` and `GOOGLE_*` environment variables.
