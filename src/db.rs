//! The Postgres connection pool and schema shared by the coordinator and queue workers.
//!
//! The URL comes from DATABASE_URL (it holds a password, so not from the config file). TLS is
//! used when the server offers it (`sslmode=prefer`, the default) or required with
//! `sslmode=require`; certificates are checked against the Mozilla roots.

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};

/// Tables are created if missing, under an advisory lock so that many workers starting at once
/// don't race each other.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS videos (
    id           TEXT PRIMARY KEY,
    seq          BIGSERIAL,                  -- discovery order
    source       TEXT NOT NULL,              -- search:<q> | channel:<id> | expand:<id> | seed | ids-file
    found_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT NOT NULL DEFAULT 'new',-- new | accepted | rejected | gone
    reason       TEXT,                       -- why rejected
    meta         JSONB,                      -- api::Video, once checked
    batch        TEXT,                       -- the plan batch it went into (shard mode)
    fetch_state  TEXT,                       -- NULL (to do) | fetched | unavailable | failed (queue mode)
    fetch_reason TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    lease_owner  TEXT,
    lease_until  TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ,
    bytes        BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS videos_unchecked ON videos (seq) WHERE status = 'new';
CREATE INDEX IF NOT EXISTS videos_queue ON videos ((source LIKE 'expand:%'), seq)
    WHERE status = 'accepted' AND fetch_state IS NULL AND batch IS NULL;
CREATE TABLE IF NOT EXISTS searches (
    query TEXT PRIMARY KEY,
    pages INTEGER NOT NULL DEFAULT 0,
    next  TEXT,
    done  BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS channels (
    channel  TEXT PRIMARY KEY,               -- as given: UC id or @handle
    seq      BIGSERIAL,
    kind     TEXT NOT NULL,                  -- seed | expand
    resolved TEXT,                           -- the UC id, once resolved
    uploads  TEXT,
    next     TEXT,
    found    INTEGER NOT NULL DEFAULT 0,
    done     BOOLEAN NOT NULL DEFAULT false,
    note     TEXT
);
CREATE TABLE IF NOT EXISTS quota (
    day    TEXT PRIMARY KEY,                 -- America/Los_Angeles date, when the API quota resets
    used   BIGINT NOT NULL DEFAULT 0,
    search BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS ips (
    label       TEXT PRIMARY KEY,            -- provider/host:port; the URL stays in each worker's file
    provider    TEXT NOT NULL,
    strikes     INTEGER NOT NULL DEFAULT 0,  -- bot checks or proxy errors in a row
    rest_until  TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired     BOOLEAN NOT NULL DEFAULT false,
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    last_used   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS attempts (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker     TEXT NOT NULL,
    label      TEXT NOT NULL,
    provider   TEXT NOT NULL,
    video      TEXT NOT NULL,
    outcome    TEXT NOT NULL,                -- fetched | unavailable | failed | bot_check | proxy_error
    bytes      BIGINT NOT NULL,
    seconds    DOUBLE PRECISION NOT NULL,    -- time in yt-dlp
    duration_s BIGINT NOT NULL               -- video length, when fetched
);
CREATE INDEX IF NOT EXISTS attempts_label_ts ON attempts (label, ts);
-- Metrics, for status and dashboards.
CREATE OR REPLACE VIEW egress_by_provider AS
SELECT a.provider,
       COUNT(DISTINCT a.label)                                        AS ips,
       (SELECT COUNT(*) FROM ips WHERE ips.provider = a.provider AND retired) AS retired,
       COUNT(*)                                                       AS tries,
       COUNT(*) FILTER (WHERE outcome = 'fetched')                    AS fetched,
       COUNT(*) FILTER (WHERE outcome IN ('bot_check', 'proxy_error')) AS strikes,
       COALESCE(SUM(bytes), 0)::BIGINT                                AS bytes,
       COALESCE(SUM(seconds) FILTER (WHERE outcome = 'fetched'), 0)   AS fetch_seconds,
       COALESCE(SUM(duration_s), 0)::BIGINT                           AS duration_s,
       EXTRACT(EPOCH FROM MAX(ts) - MIN(ts))                          AS span_s
FROM attempts a GROUP BY a.provider;
CREATE OR REPLACE VIEW fetch_progress AS
SELECT COALESCE(fetch_state, CASE WHEN lease_until > now() THEN 'leased' ELSE 'queued' END) AS state,
       COUNT(*) AS videos,
       COALESCE(SUM(bytes), 0)::BIGINT AS bytes,
       COALESCE(SUM((meta->>'duration_s')::BIGINT), 0)::BIGINT AS duration_s
FROM videos WHERE status = 'accepted' AND batch IS NULL GROUP BY 1;
";

/// Opens a pool on `url`. With `schema`, every connection works in that schema (tests use a
/// fresh one each).
pub async fn connect(url: &str, schema: Option<&str>, max: usize) -> Result<Pool> {
    let mut cfg: tokio_postgres::Config = url
        .parse()
        .map_err(|_| anyhow::anyhow!("DATABASE_URL is not a valid Postgres URL"))?;
    if let Some(s) = schema {
        cfg.options(format!("-c search_path={s}"));
    }
    let tls_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_cfg);
    let mgr = Manager::from_config(
        cfg,
        tls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr).max_size(max).build()?;
    let mut client = pool.get().await.context("connecting to Postgres")?;
    let tx = client.transaction().await?;
    tx.batch_execute("SELECT pg_advisory_xact_lock(7710301)")
        .await?;
    if let Some(s) = schema {
        tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {s}"))
            .await?;
    }
    tx.batch_execute(SCHEMA).await.context("creating tables")?;
    tx.commit().await?;
    Ok(pool)
}

/// DATABASE_URL, or an error that says what it is for.
pub fn url_from_env() -> Result<String> {
    std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL is not set (the Postgres frontier, e.g. postgres://user:pass@host/crawler)"))
}

#[cfg(test)]
pub mod test {
    //! Each test gets its own schema in the database at TEST_DATABASE_URL. Without it the
    //! Postgres tests print a note and pass vacuously, so `cargo test` works anywhere.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static N: AtomicUsize = AtomicUsize::new(0);

    pub async fn pool() -> Option<Pool> {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL not set; skipping a Postgres test");
            return None;
        };
        let schema = format!(
            "t_{}_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
            rand::random::<u32>()
        );
        Some(
            connect(&url, Some(&schema), 8)
                .await
                .expect("test database"),
        )
    }
}
