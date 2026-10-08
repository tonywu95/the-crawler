//! status: the frontier's counts, the token's GraphQL points, and each batch's progress.

use std::path::Path;

use anyhow::Result;

use super::frontier::Frontier;
use crate::shards;
use crate::store::Store;

pub async fn run(db: &Path, store: Option<&str>, api: Option<(&str, &str)>) -> Result<()> {
    if db.exists() {
        let s = Frontier::open(db)?.stats()?;
        println!("frontier {}", db.display());
        let seeds: Vec<String> = s
            .seeds
            .iter()
            .map(|(kind, n, done)| format!("{kind} {done}/{n} done"))
            .collect();
        println!(
            "  seeds     {}",
            if seeds.is_empty() {
                "none".into()
            } else {
                seeds.join(", ")
            }
        );
        println!(
            "  names     {} to look up, {} not found",
            s.names_pending, s.names_missing
        );
        println!(
            "  repos     {} accepted ({} not in a batch), {} rejected",
            s.accepted, s.unplanned, s.rejected
        );
        if !s.reasons.is_empty() {
            let reasons: Vec<String> = s.reasons.iter().map(|(r, n)| format!("{r} {n}")).collect();
            println!("  rejected  {}", reasons.join(", "));
        }
    } else {
        println!("no frontier at {}", db.display());
    }

    if let Some((api_url, token)) = api {
        match graphql_points(api_url, token).await {
            Ok((remaining, limit, reset)) => {
                println!("graphql   {remaining} of {limit} points left, resets {reset}")
            }
            Err(e) => println!("graphql   unknown ({e:#})"),
        }
    }

    if let Some(location) = store {
        let store = Store::open(location)?;
        println!("batches in {location}");
        println!(
            "  {:<17} {:>13} {:>24} {:>18}",
            "batch", "shards done", "repos fetched/failed", "GB of text"
        );
        for b in shards::list_batches(&store).await? {
            let p = shards::progress(&store, &b.batch).await?;
            let count = |status: &str| p.items.get(status).copied().unwrap_or(0);
            println!(
                "  {:<17} {:>13} {:>24} {:>18}",
                b.batch,
                format!("{}/{}", p.shards_done, b.shards),
                format!("{}/{} of {}", count("fetched"), count("failed"), b.items),
                format!("{:.2}", p.bytes as f64 / 1e9),
            );
        }
    }
    Ok(())
}

/// The token's GraphQL points: (remaining, limit, reset time). Asking costs no points.
async fn graphql_points(api_url: &str, token: &str) -> Result<(i64, i64, String)> {
    let url = format!("{}/rate_limit", api_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .user_agent(crate::USER_AGENT)
        .build()?;
    let body: serde_json::Value = client
        .get(url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let g = &body["resources"]["graphql"];
    let reset = g["reset"]
        .as_i64()
        .and_then(|t| chrono::DateTime::from_timestamp(t, 0));
    Ok((
        g["remaining"].as_i64().unwrap_or(0),
        g["limit"].as_i64().unwrap_or(0),
        reset.map_or("at an unknown time".into(), |t| {
            t.format("%H:%M:%SZ").to_string()
        }),
    ))
}
