# the-crawler

Crawlers for openly licensed data, written in Rust. Their output feeds Astra's data mixes.

| Binary | Crawls | Produces | Status |
| --- | --- | --- | --- |
| `youtube-crawl` | Creative Commons (CC BY) YouTube video | video and captions | Designed below; no code yet |
| `github-crawl` | Public GitHub repositories under permissive licenses | file-level source text | Implemented |

Each crawler is designed around its source. They share two pieces. `src/store.rs` writes to a local path, s3:// or gs://. `src/shards.rs` handles batches of shards and their manifests, which let any number of workers split a batch without a coordinator. Give each crawler its own store (a directory, or a prefix such as `s3://bucket/github`).

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

For a data mix, the useful unit is the source file, not the repository. `github-crawl` reads each accepted repository at a pinned commit. It keeps the files its authors wrote, in a known language, as readable text, under the repository's license. Each file becomes one JSON line.

### Output

```
files/<batch>/shard-00000.jsonl.gz       one kept file per line
manifests/<batch>/shard-00000.jsonl      one repository per line; written last, so it marks the shard done
batches/<batch>/...                      the plan: which repositories, at which commits
archives/<id % 100>/<id>/<sha>.tar.gz    the raw tarballs, only with fetch --keep-archives
```

A file line has these fields:

| Field | |
| --- | --- |
| `repo`, `repo_id` | owner/name, and GitHub's numeric id, which survives renames and transfers |
| `commit`, `license`, `stars` | the snapshot's commit, the repository's SPDX license, and its stars at discovery |
| `path`, `language`, `size` | language from the file name or extension: 94 languages and formats of code, docs and config |
| `blob_id` | the git blob sha1, the same id git, GitHub and Software Heritage use: a ready key for exact dedup across shards and crawls |
| `text` | the file, UTF-8 |

A manifest line has the repository's metadata from discovery (URL, branch, stars, forks, language, topics, description, dates), `status` and `reason`, files and bytes kept, and files dropped by reason. It also holds the license file's name and text, for attribution.

### Commands

| Command | Runs on | Does |
| --- | --- | --- |
| `discover` | one machine with a token | Spends the token's GraphQL points to turn seeds into checked repositories in a SQLite frontier (see Discovery). Saves its place after every page, so it can be stopped and rerun. |
| `plan` | the same machine | Cuts accepted repositories not yet in a batch into a new batch of shards, most-starred first. |
| `fetch` | any number of workers, no token | Worker `rank` of `world` (× `--jobs`) takes every shard k with k % slots == slot. For each repository it downloads `codeload.github.com/<owner>/<name>/tar.gz/<sha>`, runs the checks below, and adds the kept files to the shard's output. Codeload is not the API, so fetching spends no points. |
| `status` | anywhere | Frontier counts and top rejection reasons, GraphQL points left, and per-batch progress and GB of text, read from manifests. |

A rerun of `fetch` skips shards that have a manifest and redoes any other from its start. A repository that fails its checks adds nothing to the output.

### What is kept

Repository checks, all of which must pass:

- **License.** GitHub's detected license (`licenseInfo.spdxId`) must be on the seeds file's `accept.licenses`. The default list is MIT, MIT-0, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, 0BSD, Unlicense, CC0-1.0, Zlib and BSL-1.0. "NOASSERTION" (a license GitHub could not identify) and no license are rejected. The snapshot must also have a top-level license file (LICENSE, COPYING, LICENSE-MIT, ...) whose text matches that license, so a stale detection by GitHub doesn't let a relicensed repository through.
- **Commit.** The commit id `git archive` wrote into the tarball's pax header must be the commit discovery saw.
- **Kind of repository.** Forks, mirrors, empty, disabled and locked repositories are rejected at discovery. Submodules never come along, since codeload leaves them out.

Then file checks, in order. The first that fails is counted in the manifest's `dropped` under its reason:

| Reason | Drops |
| --- | --- |
| `vendored` | other people's code: anything under `node_modules/`, `vendor/`, `third_party/`, `deps/`, `external/`, `Pods/`, `site-packages/`, ... |
| `generated` | tool output: lock files, `*.min.js`, `*.pb.go`, `*_pb2.py`, directories named like `generated`, and files whose first 4 KB carry a generator marker (`@generated`, "generated … do not edit", "auto-generated") |
| `unknown_type` | anything outside the language table: images, binaries, archives, fonts, data dumps |
| `empty`, `too_big` | empty files, and files over 1 MiB (128 KiB for JSON, YAML, XML, TOML and INI) |
| `binary`, `not_utf8`, `lfs_pointer` | NUL bytes, invalid UTF-8, Git LFS pointer files |
| `file_license` | a file whose `SPDX-License-Identifier` can only be satisfied by licenses that are neither permissive nor the repository's own. Also code that opens with a GPL, LGPL, AGPL, MPL or EPL notice other than the repository's own. This is the copied-in GPL file in an MIT project. |
| `long_lines` | code and data with a line over 1,000 characters or lines averaging over 100: minified code, embedded data |
| `low_alnum` | under 25% alphanumeric characters, whitespace aside |
| `duplicate` | the same blob as a file already in this shard |
| `license_file` | top-level license files, which go into the manifest instead |

Left for downstream: exact dedup across shards (by `blob_id`), near-duplicate removal, scrubbing secrets and personal data, and quality scoring.

### Discovery

- **Searches** are GitHub search queries such as `language:rust stars:>=50`. A query without `license:` runs once per accepted license, so GitHub filters by license before returning anything. Most public repositories have no license, so this avoids spending points on results that would be rejected. A search matching more than the 1,000 results GitHub returns is split by creation date until each part fits.
- **Owners** (users and organizations), **repos** and **repo_files** (one owner/name per line) are looked up 100 to a query. For coverage beyond what search finds, write lists of names from another source, such as GH Archive, into a repo file.
- **Points.** A token gets 5,000 GraphQL points an hour. Each query costs about 1 point and returns up to 100 repositories, so roughly 500k repositories are checked per hour. `--max-points` caps a run.
- **Size.** `accept.max_size_mb` (default 10 GB) bounds GitHub's disk usage, which includes history. `fetch --max-archive-mb` (default 2 GB) bounds the download itself.

### Politeness

Discover sends one query at a time, at least 500 ms apart. It sleeps until the hourly reset when fewer than 100 points are left, and waits out secondary rate limits (`retry-after`). Workers run one job per IP address by default, pause randomly for 1 to 3 s after each download, and cap each job at 10 MB/s. On HTTP 403, 429 or 5xx a worker backs off exponentially from 30 s, honoring `retry-after`, and stops after 6 failed requests in a row. The unfinished shard is then redone on the next run. No proxies or ways around GitHub's limits.

### Verified (2026-10-08)

- **Against codeload.** `codeload.github.com/<owner>/<name>/tar.gz/<sha>` serves that commit's `git archive`. The top directory is `<name>-<sha>/`, and the pax header's `comment` is the full commit id. `fetch` was run live on this repository's commit `ad714df`: downloaded, commit matched, then rejected with `license_check:no_license_file`, since this repository has no LICENSE. A made-up commit gets HTTP 404, recorded as `not_found`.
- **On real code.** Seven crates (serde_json, tokio, ring, aws-lc-sys, object_store, rusqlite, tar) were made into git repositories, archived the way codeload does, and served to `fetch` locally. All seven passed the repository checks; 2,030 files were kept. aws-lc's `third_party/` (721 files) was dropped as vendored. Perl-generated assembly, lock files and generated headers were dropped as generated. Every SPDX header in ring and aws-lc is permissive (`Apache-2.0 OR ISC OR MIT-0` and the like), so none were dropped for `file_license`. 200 sampled `blob_id`s equal git's object ids. This run led to two fixes: directories named `generated*` count as generated, and whitespace no longer counts against `low_alnum` (it had dropped a 9-line tokio macro file).
- **GraphQL.** The queries discover sends (search, owner, lookups by name) are valid against GitHub's published schema (`@octokit/graphql-schema` 15.26.1): fields, enum arguments and variable types. They have not run against the live API, which was blocked where this was built. On a first real run, use a small `--max-points` and check the frontier with `status`.
- **Tests.** `cargo test` runs discover → plan → fetch → status against a fake GitHub. It covers search expansion and splitting, pagination, missing names and owners, a 503 retried after `retry-after`, a 404, commit and license-text mismatches, per-file drops, kept archives, reruns that make no requests, and a worker that stops after errors in a row and resumes. Unit tests cover every file check and SPDX expressions.

### Use

```bash
export GITHUB_TOKEN=...      # any token: reading public data needs no scopes
cp github-seeds.example.yaml github-seeds.yaml    # searches, owners, repos, accept policy
github-crawl discover --seeds github-seeds.yaml   # frontier in data/github.sqlite
github-crawl plan --store s3://bucket/github      # prints the batch name
github-crawl fetch --store s3://bucket/github --batch 20261008-120000 --rank 0 --world 4
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
