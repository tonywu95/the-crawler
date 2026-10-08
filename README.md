# the-crawler

A YouTube crawler for Creative Commons video, written in Rust. Its output feeds Astra's data mixes.

Status: all four commands are written and unit-tested against a fake API and a fake downloader. fetch can spread downloads over a proxy list (design phase 1), and many workers can share a Postgres work queue and IP pool (phase 2). Not yet run against the live API or YouTube.

## Design

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

## Rules it follows

- **License.** Only videos whose API `status.license` is `creativeCommon` (CC BY 3.0) are accepted. The worker checks again on the watch page just before downloading, and skips the video if the page no longer shows a Creative Commons license. Attribution (title, channel, URL) is kept in every record.
- **Politeness.** One video at a time per IP, a random pause after each, an optional hourly cap per IP, and a per-download rate cap. On "confirm you're not a bot" or HTTP 429 that IP is benched with exponential backoff and retired after a few in a row. Load can be spread over a proxy list (see the design doc); there are no accounts, cookies or CAPTCHA solving.
- **Quota.** search costs 100 units; videos.list, playlistItems.list and channels.list cost 1 per call of up to 50. The default quota is 10,000 units per day, reset at midnight Pacific. Checking a list of known ids costs about 1 unit per 50, so roughly 500k ids per day per key; search is the expensive path.

## Verified against YouTube (2026-10-07, yt-dlp 2026.08.19, deno 2.9.7)

- On a CC video (`M4gD1WSo5mA`), yt-dlp's `license` field is "Creative Commons Attribution license (reuse allowed)". On a standard-license video it is null, so the page check requires the CC string to be present rather than accepting "absent". Because a YouTube layout change could make that string vanish everywhere, misses count toward the worker's error streak.
- `-f "bv[height<=720][vcodec^=avc1]/bv[height<=720]/b[height<=720]"` selects format 136 (720p avc1) on that video, with no merge or ffmpeg needed.
- `--match-filters "license~='(?i)creative commons'"` lets the CC video through. On a standard-license video (`uGpuVWrhIzE`), yt-dlp prints "does not pass filter", exits 0 and writes nothing, so "exit 0 with no files" means the page check failed.

## Setup

```bash
uv sync                      # yt-dlp + deno into .venv/bin
cargo build --release
export YOUTUBE_API_KEY=...   # a Google Cloud project with YouTube Data API v3 enabled
```

Settings live in `config/youtube.yaml` (store, frontier path, queries, filters, fetch limits). Any key left out takes the default in `src/config.rs`.

## Running

Discover, plan, status and queue-mode fetch keep their state in Postgres at `DATABASE_URL` (an environment variable, since it holds a password). Any Postgres 13+ works; for a laptop, `docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw postgres:16`, then `export DATABASE_URL=postgres://postgres:pw@localhost/postgres`. Tables are created on first use.

```bash
B=target/release/youtube-crawl
$B discover                         # once a day: spends the quota; resumes where it stopped
$B discover --ids-file ids.txt      # also check known ids (ids or watch URLs, one per line)
$B plan                             # accepted videos → batches/<batch>/shard-*.jsonl
$B fetch                            # one worker, one download at a time
$B fetch --rank 2 --world 8         # worker 2 of 8, each on its own IP
$B fetch --proxies config/proxies.txt --jobs 8   # downloads spread over a proxy list
$B fetch --queue --jobs 8 --proxies config/proxies.txt   # lease work from Postgres (any number of workers)
$B status
```

There are two ways to hand out work:

- **Queue mode (`fetch --queue`)**, for many workers. Each download slot leases one accepted video at a time from Postgres (`FOR UPDATE SKIP LOCKED`), direct hits first. A lease lasts `video_lease_s` and is renewed while the worker is on the video, so a worker that dies only loses its videos for that long. A failed video goes back in the queue after `retry_delay_s` and is marked failed after `max_attempts` tries; fetched and unavailable videos are final. Workers can join or leave at any time, and each exits when the queue is empty. Proxy IPs are shared too: every worker lists the same proxies in its own file, and Postgres holds each IP's lease, rest, strikes and retirement by label, so two workers never use one IP at once and a retirement applies everywhere. The proxy URLs and passwords never reach the database.
- **Shard mode (`plan`, then `fetch`)**, for a laptop or a fixed set of machines without database access. `plan` writes shards to the store, and worker `rank` of `world` takes every shard k with k % (world × jobs) == its slot. IP health lives in `fetch.egress_db` on each worker.

`discover` runs in this order: check unchecked ids, crawl seed channels, crawl channels (checking new ids after each one), search up to `search_quota`, crawl the channels search turned up, then check again. When the quota runs out it stops cleanly. `plan` puts direct hits (search, seed ids, seed channels) ahead of channel-expansion hits.

`fetch` sends each download through an egress IP leased from a pool: the proxies in `fetch.proxies` (or `--proxies`), or the machine's own address if there are none. The lease covers the whole video, because YouTube signs the media URL for the IP that loaded the page. An IP serves one video at a time, so `--jobs` beyond the number of IPs adds nothing. After a video the IP rests `pause_min_s`–`pause_max_s` seconds, and `max_videos_per_ip_per_hour` caps it if set. A bot check, a 429 or a proxy failure is a strike: the IP is benched for `bot_check_backoff_s`, doubling per strike in a row up to `bot_check_backoff_max_s`, and the video is retried on another IP. After `bot_check_limit` strikes in a row the IP is retired, which persists across runs until `fetch --reset-egress`.

`fetch` exits non-zero when it stops early: when every IP is retired, or after `error_limit` other failures in a row. The shard it was on keeps no manifest, so the next run picks it up again.

The proxy list has one proxy per line, `<provider> <url>`, for example `acme http://user:pass@gw.acme.net:7000`; `config/proxies*.txt` is git-ignored. Logs, manifests and the health database name a proxy only by its label, `acme/gw.acme.net:7000#1`, never by its URL. The URL is passed to yt-dlp on its command line, so it is visible to other users of the same machine.

Every attempt (IP, provider, outcome, bytes, seconds in yt-dlp) goes into the `attempts` table in queue mode, or into `fetch.egress_db` on the worker in shard mode. `status` turns that into the proxy trial's comparison: for each provider, IPs, retired, tries, fetched, strike rate, MB/s, videos per IP per day, hours fetched and, given `provider_prices`, $ per hour of video. An unavailable video (private, removed, region-locked, members-only, age-gated) goes into the manifest as `unavailable` and does not count toward the error streak.

`status` also prints the queue (videos queued, leased, fetched, unavailable and failed, with GB and hours). The same numbers are SQL views, `fetch_progress` and `egress_by_provider`, for a dashboard.

`--store` overrides the store on the command line. For S3-compatible stores (R2 included), set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and, for R2, `AWS_ENDPOINT`. They are read from the environment.

## Containers

The `Dockerfile` builds a worker image: the binary, plus yt-dlp and deno pinned by `uv.lock`, on `python:3.12-slim-bookworm` (the build needs only Docker Hub, crates.io and PyPI). It defaults to `fetch --queue --jobs 4` and takes `DATABASE_URL`, store credentials and a mounted proxy list; the header comment has an example `docker run`.

## Tests

`cargo test` runs without network access: the API goes through a fake `Transport` and downloads through a fake `Downloader`. The frontier, queue and shared-pool tests need a Postgres: set `TEST_DATABASE_URL` (each test makes its own schema there, so use a throwaway database). Without it those tests print a note and pass without checking anything.
