use laser_examples::{init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::*;
use laser_sdk::query::{ContentType, Projection, ProjectionBinding, Record};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use strum::Display;
use tracing::{info, warn};

// A load generator. A telemetry "firehose" that publishes millions of big,
// richly indexed observability events across many indexes, so LaserData Cloud
// can be driven with gigabytes of data. Unlike the other examples this one is
// built for volume rather than narrative. It runs many concurrent producers
// against the log and lets LaserData Cloud materialize each topic into its own
// queryable index.
//
// Every knob is read from the environment (see `Config`), so the same binary
// scales from a quick smoke run to a multi gigabyte soak:
//
//   # defaults: about 2M events across 8 org indexes, 4 KB payloads (about 8 GB)
//   just up && cargo run --release --example firehose
//
//   # bigger: about 10M events, 32 orgs, 16 producers, let LaserData Cloud project
//   LASER_FIREHOSE_MESSAGES=10000000 LASER_FIREHOSE_ORGS=32 \
//   LASER_FIREHOSE_CONCURRENCY=16 LASER_LOCAL_WORKER=0 \
//   cargo run --release --example firehose
//
// Indexes only materialize when LaserData Cloud consumes the projection commands
// (LaserData Cloud, or LaserData Cloud reachable from the connection). On a raw
// Apache Iggy server with no managed backend the publish path still runs at full speed (it
// exercises the log, not the read model) and the trailing analytics are skipped.

// One index per org, named `org_00`, `org_01`, and so on. This is the
// realistic multi-org shape: each org gets its own materialized index, so
// a modest org count already exercises LaserData Cloud maintaining many indexes
// at once. Query index names accept `[A-Za-z0-9_]` only (each name is used
// verbatim), so the separator is `_` rather than `.`.
const TOPIC_PREFIX: &str = "org_";

// Indexed columns LaserData Cloud materializes into each index table. `message_type` and
// `ts` are reserved fields that back `query.message_type(..)` and
// `query.time_range(..)`.
const FIELDS: &[&str] = &[
    "org",
    "service",
    "region",
    "host",
    "env",
    "severity",
    "message_type", // reserved
    "http_method",
    "status_code",
    "route",
    "user_id",
    "session_id",
    "trace_id",
    "latency_ms",
    "bytes_out",
    "ts", // reserved, epoch micros
];
const COUNT_RESULT: &str = "count";

// Fixed dimension vocabularies. The run replays identically and the indexes carry
// realistic cardinality without pulling in a random number generator crate.
const SERVICES: &[&str] = &[
    "checkout",
    "catalog",
    "search",
    "auth",
    "payments",
    "shipping",
    "inventory",
    "recommend",
    "notify",
    "gateway",
];
const REGIONS: &[&str] = &[
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-south-1",
    "ap-northeast-1",
];
const ENVIRONMENTS: &[&str] = &["prod", "staging", "dev"];
const HTTP_METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH", "DELETE"];
const ROUTES: &[&str] = &[
    "/home",
    "/product/42",
    "/search",
    "/cart",
    "/checkout",
    "/api/v1/orders",
    "/api/v1/users",
    "/healthz",
    "/metrics",
    "/login",
];
const STATUS_CODES: &[u16] = &[200, 201, 204, 301, 400, 401, 403, 404, 429, 500, 503];

// A fixed epoch base in microseconds keeps timestamps reproducible. Each event
// steps forward by a few seconds from here.
const BASE_TIMESTAMP_US: u64 = 1_900_000_000_000_000;
const MAX_STEP_US: u64 = 5_000_000; // up to 5 seconds between events on a shard

// What the event was, weighted to resemble real traffic.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum EventType {
    HttpRequest,
    DbQuery,
    CacheOp,
    QueuePublish,
    JobRun,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Severity {
    Debug,
    Info,
    Warn,
    Error,
}

// The structured body. The scalar dimensions double as indexed columns. The
// `attributes` map and `detail` blob pad the payload to the configured size, so
// the log (and LaserData Cloud's inline payloads) carry real bytes rather than toy
// records.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TelemetryEvent {
    org: String,
    service: String,
    region: String,
    host: String,
    env: String,
    severity: Severity,
    message_type: EventType,
    http_method: String,
    status_code: u16,
    route: String,
    user_id: String,
    session_id: String,
    trace_id: String,
    latency_ms: u32,
    bytes_out: u32,
    ts: u64,
    attributes: std::collections::BTreeMap<String, String>,
    detail: String,
}

// Run knobs, all read from the environment with sane defaults.
struct Config {
    orgs: usize,
    messages: u64,
    payload_bytes: usize,
    batch: usize,
    concurrency: usize,
    partitions: u32,
    register: bool,
    query: bool,
    progress_every: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            orgs: env_usize("LASER_FIREHOSE_ORGS", 8).max(1),
            messages: env_u64("LASER_FIREHOSE_MESSAGES", 2_000_000).max(1),
            payload_bytes: env_usize("LASER_FIREHOSE_PAYLOAD_BYTES", 4096),
            batch: env_usize("LASER_FIREHOSE_BATCH", 1000).max(1),
            concurrency: env_usize("LASER_FIREHOSE_CONCURRENCY", 12).max(1),
            partitions: env_usize("LASER_FIREHOSE_PARTITIONS", 8).max(1) as u32,
            register: env_bool("LASER_FIREHOSE_REGISTER", true),
            query: env_bool("LASER_FIREHOSE_QUERY", true),
            progress_every: env_u64("LASER_FIREHOSE_PROGRESS_EVERY", 100_000).max(1),
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

// A small xorshift64* pseudo random generator, one per producer task, seeded by
// the shard index so the whole run replays identically with no extra crate.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound.max(1)
    }

    fn pick<'a, T>(&mut self, choices: &'a [T]) -> &'a T {
        &choices[self.below(choices.len() as u64) as usize]
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let config = Config::from_env();
    let stream_name = stream_for("firehose");

    let approx_gigabytes = (config.messages as f64 * config.payload_bytes as f64) / 1e9;
    phase("firehose: warming up");
    info!(
        "plan: {} messages across {} org indexes, about {} B payloads (about {:.2} GB on the log), \
         batch {}, {} concurrent producers, {} partitions per topic",
        config.messages,
        config.orgs,
        config.payload_bytes,
        approx_gigabytes,
        config.batch,
        config.concurrency,
        config.partitions,
    );

    // `managed_query` so the trailing analytics work against the in-process worker
    // locally, or the `AGDX_QUERY` managed command on a deployment.
    let capabilities = Capabilities::OPEN.with_managed_query(true);
    let laser = laser(&stream_name, capabilities).await?;

    let topics: Vec<String> = (0..config.orgs)
        .map(|org| format!("{TOPIC_PREFIX}{org:02}"))
        .collect();

    // Create every topic, then register a projection and binding per topic so each
    // one materializes into its own index. Registration is plain control envelope
    // publishing. It is harmless on a raw server, and only materializes once a
    // LaserData Cloud consumes the projection commands.
    phase("provisioning topics and indexes");
    for topic in &topics {
        laser.ensure_topic(topic, config.partitions).await?;
    }
    if config.register {
        for topic in &topics {
            register_index(&laser, &stream_name, topic).await?;
        }
        info!(
            "registered {} projections, waiting for LaserData Cloud to create indexes",
            topics.len()
        );
        // Best effort. Give LaserData Cloud a moment to create the first index. With no
        // LaserData Cloud attached this short wait simply elapses and we publish anyway.
        wait_for_index(&laser, &topics[0], Duration::from_secs(15)).await;
    } else {
        info!("LASER_FIREHOSE_REGISTER is off, skipping projection registration (publish only)");
    }

    // Publish.
    phase("firing the hose");
    let published = Arc::new(AtomicU64::new(0));
    let bytes_published = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // Spread the total over the orgs, then run `concurrency` of them at a time
    // so a large run does not spawn unbounded work.
    let per_org = config.messages / config.orgs as u64;
    let remainder = config.messages % config.orgs as u64;
    let config = Arc::new(config);

    let mut window_start = 0usize;
    while window_start < topics.len() {
        let window_end = (window_start + config.concurrency).min(topics.len());
        let mut handles = Vec::new();
        for (offset, topic) in topics[window_start..window_end].iter().enumerate() {
            let shard = (window_start + offset) as u64;
            // The first orgs take the remainder so the totals add up exactly.
            let count = per_org + if shard < remainder { 1 } else { 0 };
            let laser = laser.clone();
            let topic = topic.clone();
            let config = config.clone();
            let published = published.clone();
            let bytes_published = bytes_published.clone();
            handles.push(tokio::spawn(async move {
                produce_shard(
                    &laser,
                    &topic,
                    shard,
                    count,
                    &config,
                    &published,
                    &bytes_published,
                )
                .await
            }));
        }
        for handle in handles {
            handle
                .await
                .map_err(|error| LaserError::Invalid(format!("producer task: {error}")))??;
        }
        window_start = window_end;
    }

    let elapsed = started.elapsed().as_secs_f64().max(1e-6);
    let total = published.load(Ordering::Relaxed);
    let total_bytes = bytes_published.load(Ordering::Relaxed);
    info!(
        "done: {total} messages, {:.2} GB payload in {:.1}s ({:.0} msg/s, {:.1} MB/s)",
        total_bytes as f64 / 1e9,
        elapsed,
        total as f64 / elapsed,
        (total_bytes as f64 / 1e6) / elapsed,
    );

    // Analytics, best effort.
    if config.query {
        phase("sample analytics over the firehose");
        run_sample_queries(&laser, &topics).await;
    }

    Ok(())
}

// Register one projection and binding so `topic` materializes into an index of
// the same name with our indexed columns. This mirrors the shared cloud
// projector path, inlined so we can register many indexes with a rich field set
// directly.
async fn register_index(laser: &Laser, stream_name: &str, topic: &str) -> Result<(), LaserError> {
    let projection_id = format!("{topic}.v1");
    let mut projection = Projection::builder(projection_id.clone())
        .name(topic)
        .version(1)
        .content_type(ContentType::Any)
        .index_only();
    for field in FIELDS {
        projection = projection.field(*field);
    }
    laser.projections().register(projection.build()).await?;

    let binding = ProjectionBinding::builder()
        .source(stream_name, topic)
        .allow(projection_id.clone())
        .default_projection(projection_id)
        .target_table(topic)
        .build();
    laser.bindings().apply(binding).await?;
    Ok(())
}

// Publish `count` events for one shard in batches of `config.batch`. Each event
// is a JSON body padded to `config.payload_bytes`, carrying its indexed columns
// inline so a query can return the whole record.
async fn produce_shard(
    laser: &Laser,
    topic: &str,
    shard: u64,
    count: u64,
    config: &Config,
    published: &AtomicU64,
    bytes_published: &AtomicU64,
) -> Result<(), LaserError> {
    let mut rng = Rng::new(0xD1B5_4A32 ^ shard.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut timestamp_us = BASE_TIMESTAMP_US + shard * MAX_STEP_US;
    let mut published_in_shard = 0u64;
    // Every record in this index belongs to one org (the shard), the
    // realistic multi-org shape.
    let org = format!("org-{shard:02}");

    while published_in_shard < count {
        let batch_size = config.batch.min((count - published_in_shard) as usize);
        let mut batch = laser.publish_batch(topic);
        let mut batch_bytes = 0u64;
        for _ in 0..batch_size {
            let event = build_event(&mut rng, &org, config.payload_bytes, &mut timestamp_us);
            let body =
                serde_json::to_vec(&event).map_err(|error| LaserError::Codec(error.to_string()))?;
            batch_bytes += body.len() as u64;
            // Body-first indexing: the projection's pointers extract every
            // column out of the JSON event, typed. No `agdx.idx.*` duplication
            // of the payload riding the wire at gigabyte volume.
            let record = Record::builder()
                .content_type(ContentType::Json)
                .inline_payload(true)
                .build();
            batch = batch.add_record(body, record);
        }
        batch.send().await?;
        published_in_shard += batch_size as u64;
        bytes_published.fetch_add(batch_bytes, Ordering::Relaxed);
        let total = published.fetch_add(batch_size as u64, Ordering::Relaxed) + batch_size as u64;
        if total % config.progress_every < batch_size as u64 {
            info!("published {total} of {} messages", config.messages);
        }
    }
    Ok(())
}

// Build one reproducible event for `org`, padded to roughly `payload_bytes`.
fn build_event(
    rng: &mut Rng,
    org: &str,
    payload_bytes: usize,
    timestamp_us: &mut u64,
) -> TelemetryEvent {
    let severity = match rng.below(100) {
        0..=64 => Severity::Info,
        65..=84 => Severity::Debug,
        85..=96 => Severity::Warn,
        _ => Severity::Error,
    };
    let message_type = match rng.below(100) {
        0..=59 => EventType::HttpRequest,
        60..=79 => EventType::DbQuery,
        80..=91 => EventType::CacheOp,
        92..=97 => EventType::QueuePublish,
        _ => EventType::JobRun,
    };
    let service = (*rng.pick(SERVICES)).to_string();
    let region = (*rng.pick(REGIONS)).to_string();
    let host = format!("{service}-{}", rng.below(64));
    let user_id = format!("u{}", rng.below(100_000));
    let session_id = format!("{:016x}", rng.next_u64());
    let trace_id = format!("{:016x}{:016x}", rng.next_u64(), rng.next_u64());

    // A few ride along attributes, then pad `detail` to reach the target size.
    let mut attributes = std::collections::BTreeMap::new();
    attributes.insert("sdk".to_string(), "laser-firehose".to_string());
    attributes.insert("schema".to_string(), "v1".to_string());
    attributes.insert("shard_host".to_string(), host.clone());

    // Approximate size of the record without `detail`, then pad the remainder
    // with filler to reach the target byte count.
    let base_size = 320usize + service.len() + region.len() + trace_id.len();
    let padding_len = payload_bytes.saturating_sub(base_size);
    let detail = build_filler(padding_len, rng);

    let event = TelemetryEvent {
        org: org.to_string(),
        service,
        region,
        host,
        env: (*rng.pick(ENVIRONMENTS)).to_string(),
        severity,
        message_type,
        http_method: (*rng.pick(HTTP_METHODS)).to_string(),
        status_code: *rng.pick(STATUS_CODES),
        route: (*rng.pick(ROUTES)).to_string(),
        user_id,
        session_id,
        trace_id,
        latency_ms: 1 + rng.below(2000) as u32,
        bytes_out: rng.below(1_000_000) as u32,
        ts: *timestamp_us,
        attributes,
        detail,
    };
    *timestamp_us += 1 + rng.below(MAX_STEP_US);
    event
}

// Printable filler of length `length`, varied enough that it does not compress to
// nothing, so payload sizes on disk stay honest.
fn build_filler(length: usize, rng: &mut Rng) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    let mut filler = String::with_capacity(length);
    for _ in 0..length {
        filler.push(ALPHABET[rng.below(ALPHABET.len() as u64) as usize] as char);
    }
    filler
}

// Poll until `topic`'s index exists (the query stops erroring), or the deadline
// elapses. Used as a short, non fatal nudge after registration.
async fn wait_for_index(laser: &Laser, topic: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if laser.query(topic).fetch().await.is_ok() {
            info!("index `{topic}` is live");
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    info!("index `{topic}` is not live yet (no managed backend attached?), publishing anyway");
}

// A few representative analytics the firehose makes possible. Best effort: if no
// LaserData Cloud materialized the indexes the queries error and we simply note it.
async fn run_sample_queries(laser: &Laser, topics: &[String]) {
    let topic = &topics[0];

    match laser.query(topic).fetch().await {
        Ok(result) => info!("index `{topic}` holds {} rows", result.page.total),
        Err(error) => {
            warn!(
                "query unavailable ({error}). Is LaserData Cloud materializing the indexes? Skipping analytics"
            );
            return;
        }
    }

    if let Ok(by_severity) = laser
        .query(topic)
        .count()
        .group_by(["severity"])
        .fetch()
        .await
    {
        info!("`{topic}` events by severity:");
        for row in &by_severity.rows {
            let severity = row.headers.get("severity").map_or("?", String::as_str);
            let count = row.headers.get(COUNT_RESULT).map_or("0", String::as_str);
            info!("  {severity:<6} {count}");
        }
    }

    if let Ok(slowest) = laser
        .query(topic)
        .order_desc("latency_ms")
        .limit(5)
        .fetch()
        .await
    {
        info!("`{topic}` slowest 5 requests:");
        for row in &slowest.rows {
            let route = row.headers.get("route").map_or("?", String::as_str);
            let latency = row.headers.get("latency_ms").map_or("?", String::as_str);
            info!("  {latency:>5}ms  {route}");
        }
    }

    // A cheap fan out. Total rows across every index, the headline scale number.
    let mut grand_total = 0u64;
    for index_topic in topics {
        if let Ok(result) = laser.query(index_topic).fetch().await {
            grand_total += result.page.total as u64;
        }
    }
    info!(
        "grand total across {} indexes: {grand_total} rows",
        topics.len()
    );
}
