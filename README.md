# the-crawler

A YouTube crawler for Creative Commons video, written in Rust. Its output feeds Astra's data mixes.

Status: scaffold only. The crawler below is designed, but no code has been written yet.

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
- **Politeness.** One worker per IP address by default, a random pause after each video, and a per-job rate cap. On "confirm you're not a bot" or HTTP 429 the worker backs off exponentially, then stops after a few in a row. No proxies, cookies, or other ways around YouTube's limits.
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
