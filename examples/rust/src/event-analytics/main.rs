use futures::StreamExt;
use iggy::prelude::*;
use laser_examples::{PARTITIONS, init_tracing, laser, phase, start_projector, stream_for};
use laser_sdk::prelude::*;
use laser_sdk::query::{ContentType, Record, WINDOW_START};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::{Duration, Instant};
use strum::Display;
use tracing::info;

// THE general-purpose example: one clickstream topic, every read model the
// platform offers layered over it, scaled by the shared volume knobs
// (`LASER_MESSAGES`, `LASER_BATCH` turn the same binary into a smoke test or
// a soak):
//
//   - HOT PATH    a consumer-group reader tails the raw log live while the
//                 producer streams, folding a rolling ops ticker (events
//                 seen, checkouts) with tick-to-read latency in mind.
//   - ANALYTICS   LaserData Cloud materializes a queryable index and answers the
//                 aggregates a dashboard needs (funnel, slowest routes,
//                 time windows).
//   - EXPORT      an independent reader tails the same log with a `Cursor`
//                 plus `StateStore` checkpoint, resuming exactly where it
//                 stopped after a restart.
//   - SCHEMAS     on a LaserData Cloud, a registered JSON Schema guards the
//                 index against malformed events (the binary schema-first
//                 path lives in the order-book example's Avro tape).

const TOPIC: &str = "clickstream";
const CHECKPOINT_KEY: &str = "clickstream-export-cursor";

// The validated ingest (LaserData Cloud only): events on this topic stamp a
// registered JSON Schema's id, so a malformed payload never materializes.
const GUARDED_TOPIC: &str = "clickstream_guarded";
const GUARDED_PROJECTION: &str = "clickstream_guarded.v1";
const EVENT_JSON_SCHEMA: &str = r#"{
    "type":"object",
    "required":["user_id","message_type","route","latency_ms","ts"],
    "properties":{
        "user_id":{"type":"string"},
        "message_type":{"type":"string","enum":["page_view","add_to_cart","checkout"]},
        "route":{"type":"string"},
        "latency_ms":{"type":"integer","minimum":0},
        "ts":{"type":"integer","minimum":0}
    }
}"#;

// Indexed columns LaserData Cloud materializes.
const USER_ID: &str = "user_id";
const MESSAGE_TYPE: &str = "message_type"; // reserved field, drives `query.message_type(..)`
const ROUTE: &str = "route";
const LATENCY_MS: &str = "latency_ms";
const TS: &str = "ts"; // reserved field (epoch micros), drives `query.time_range(..)`
const COLUMNS: &[&str] = &[USER_ID, MESSAGE_TYPE, ROUTE, LATENCY_MS, TS];
const COUNT_RESULT: &str = "count";

// A whole session of traffic across many visitors, generated deterministically.
const VISITORS: &[&str] = &[
    "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy", "mallory",
    "oscar",
];
const ROUTES: &[&str] = &[
    "/home",
    "/product/42",
    "/product/7",
    "/search",
    "/cart",
    "/checkout",
    "/pricing",
    "/docs",
];
// Total events and per-send chunk, on the shared volume knobs so one run
// scales from a smoke test to millions (`LASER_MESSAGES`, `LASER_BATCH`).
// Publishing in chunks rather than one giant call lets each `send_messages`
// land on a partition the balanced partitioner picks, so several chunks spread
// the load across the topic's partitions instead of piling onto one.
fn events_total() -> usize {
    laser_examples::messages(180) as usize
}

fn publish_chunk() -> usize {
    laser_examples::batch(30)
}

// A fixed epoch base (micros) so the run is deterministic. Events step forward
// by a random few seconds from here.
const BASE_US: u64 = 1_900_000_000_000_000;
const ONE_MINUTE_US: u64 = 60_000_000;
const STEP_MAX_US: u64 = 30_000_000; // up to 30s between events

const PROJECTOR_TIMEOUT: Duration = Duration::from_secs(60);
const PROJECTION_POLL: Duration = Duration::from_millis(150);

// What the visitor did. An enum with `strum::Display` + serde rename, so the
// indexed value and the JSON body can never disagree.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum EventType {
    PageView,
    AddToCart,
    Checkout,
}

// One clickstream event: who, what, where, how slow, and when.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    user_id: String,
    message_type: EventType,
    route: String,
    latency_ms: u32,
    ts: u64,
}

// A tiny deterministic PRNG (xorshift64*), so the clickstream looks like real
// traffic yet replays identically on every run without pulling in an rng crate.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    phase("warming up");

    // `managed_query` so the analytics half works: the example spawns its own
    // in-process projector locally. On LaserData Cloud the connect-time
    // `AGDX_HELLO` probe upgrades the read path to the `AGDX_QUERY` managed command.
    let capabilities = Capabilities::OPEN.with_query(true);
    let laser = laser(&stream_for("event-analytics"), capabilities).await?;
    laser.ensure_topic(TOPIC, PARTITIONS).await?;

    let events = clickstream();

    // Start the projector before publishing so no event is missed.
    phase("hot path: a live reader tails the stream while the producer runs");
    let projector = start_projector(&laser, TOPIC, ContentType::Json, COLUMNS).await?;
    let publisher = {
        let laser = laser.clone();
        let events = events.clone();
        tokio::spawn(async move { publish_clickstream(&laser, &events).await })
    };
    live_monitor(&laser, events.len()).await?;
    publisher
        .await
        .map_err(|error| LaserError::Invalid(format!("publisher task: {error}")))??;
    wait_for_projection(&laser, events.len()).await?;

    // Read model 1: ad-hoc analytics over the materialized index.
    phase("read model 1: ad-hoc analytics over the query layer");
    run_analytics(&laser).await?;

    // Read model 2: a resumable downstream reader over the same log.
    phase("read model 2: a resumable downstream reader");
    run_resumable_export(&laser, &InMemoryStore::new()).await?;

    // The validated-ingest coda (LaserData Cloud only): a registered JSON
    // Schema turns the loose clickstream contract into an enforced one. A
    // record stamping the schema's id has its decoded payload validated by
    // LaserData Cloud's projector. A malformed event is counted, optionally
    // dead-lettered, and never pollutes the index.
    if laser.capabilities().await.managed {
        phase("validated ingest: a JSON Schema guards the index");
        run_guarded_ingest(&laser).await?;
    } else {
        info!(
            "writer schemas live on LaserData Cloud, skipping validated ingest (needs LaserData Cloud)"
        );
    }

    projector.shutdown().await;
    Ok(())
}

// Register the Event JSON Schema (synchronous: LaserData Cloud validates it
// compiles and returns the allocated id), publish one well-formed and one
// malformed event both stamping the id, and show only the well-formed one
// materialized. The malformed one shows up in LaserData Cloud's `/health`
// `schema_decode_failures.mismatch` counter (and the DLQ when the policy
// says so) instead of silently corrupting the index.
async fn run_guarded_ingest(laser: &Laser) -> Result<(), LaserError> {
    let schema_id = laser
        .schemas()
        .register(SchemaSource::JsonSchema {
            schema: EVENT_JSON_SCHEMA.to_owned(),
        })
        .name("clickstream_event")
        .send()
        .await?;
    info!("LaserData Cloud allocated writer-schema id {schema_id} for the Event guard");

    laser.ensure_topic(GUARDED_TOPIC, PARTITIONS).await?;
    laser
        .projections()
        .register(
            Projection::builder(GUARDED_PROJECTION)
                .name("clickstream_guarded")
                .version(1)
                .content_type(ContentType::Json)
                .fields(COLUMNS.iter().copied())
                .field(TS)
                .build(),
        )
        .await?;
    laser
        .bindings()
        .apply(
            ProjectionBinding::builder()
                .source(stream_for("event-analytics"), GUARDED_TOPIC)
                .allow(GUARDED_PROJECTION)
                .default_projection(GUARDED_PROJECTION)
                .target_table(GUARDED_TOPIC)
                .build(),
        )
        .await?;
    wait_for_schema(laser, schema_id).await?;

    // Well-formed: passes the schema, materializes.
    let valid = Event {
        user_id: "alice".to_owned(),
        message_type: EventType::Checkout,
        route: "/checkout".to_owned(),
        latency_ms: 120,
        ts: BASE_US,
    };
    laser
        .publish(GUARDED_TOPIC)
        .json(&valid)?
        .schema_id(schema_id)
        .send()
        .await?;
    // Malformed: `latency_ms` is a string, violating the schema. It decodes
    // as JSON fine - only the validation catches it.
    laser
        .publish(GUARDED_TOPIC)
        .raw_bytes(
            br#"{"user_id":"mallory","message_type":"checkout","route":"/checkout","latency_ms":"fast","ts":1}"#.to_vec(),
            ContentType::Json,
        )
        .schema_id(schema_id)
        .send()
        .await?;

    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    loop {
        let total = laser
            .query(GUARDED_TOPIC)
            .fetch()
            .await
            .map(|result| result.page.total)
            .unwrap_or(0);
        if total >= 1 {
            // Give the projector a beat to (wrongly) materialize the
            // malformed event before pinning the count.
            tokio::time::sleep(Duration::from_secs(1)).await;
            let settled = laser
                .query(GUARDED_TOPIC)
                .fetch()
                .await
                .map(|result| result.page.total)
                .unwrap_or(0);
            if settled != 1 {
                return Err(LaserError::Invalid(
                    "the malformed event must not materialize".to_owned(),
                ));
            }
            info!(
                "guarded index holds {settled} row: the valid checkout landed, the malformed event was rejected by the JSON Schema and never materialized"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(
                "guarded event never materialized".to_owned(),
            ));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// The register reply carries a durable id, but the apply is asynchronous:
// read back until browse resolves it before the first publish against it.
async fn wait_for_schema(laser: &Laser, id: u32) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    while Instant::now() < deadline {
        if matches!(laser.schemas().get(id).await, Ok(Some(_))) {
            return Ok(());
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
    Err(LaserError::Invalid(format!(
        "schema `{id}` never appeared in the registry"
    )))
}

// How long the live reader waits for the next event before giving up with a
// diagnostic instead of hanging silently.
const LIVE_TIMEOUT: Duration = Duration::from_secs(15);
const LIVE_GROUP: &str = "event-analytics-live";
const LIVE_SNAPSHOT_EVERY: usize = 1_000;

// The hot path: a consumer-group reader folding a rolling ops ticker off the
// raw log while the producer streams. Offsets commit SERVER-SIDE on each
// poll (`AutoCommit::When(PollingMessages)`), the one commit mode that keeps
// the server's stored offset moving in lockstep with delivery, so a
// redelivered or re-polled batch can never starve the reader.
async fn live_monitor(laser: &Laser, expected: usize) -> Result<(), LaserError> {
    let mut consumer = laser
        .iggy_consumer_group(LIVE_GROUP, &stream_for("event-analytics"), TOPIC)?
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(
            IggyDuration::from_str("5ms")
                .map_err(|error| LaserError::Invalid(error.to_string()))?,
        )
        .batch_length(100)
        .build();
    consumer.init().await?;

    let mut seen = 0usize;
    let mut checkouts = 0usize;
    while seen < expected {
        let received = match tokio::time::timeout(LIVE_TIMEOUT, consumer.next()).await {
            Ok(Some(received)) => received?,
            Ok(None) => {
                return Err(LaserError::Invalid(format!(
                    "live feed ended after {seen}/{expected} events"
                )));
            }
            Err(_) => {
                return Err(LaserError::Invalid(format!(
                    "no event arrived for {}s after {seen}/{expected}. Either the producer task \
                     failed (its error surfaces right after this one) or the `{LIVE_GROUP}` \
                     consumer group is not receiving deliveries from this server",
                    LIVE_TIMEOUT.as_secs(),
                )));
            }
        };
        if let Ok(event) = serde_json::from_slice::<Event>(&received.message.payload)
            && matches!(event.message_type, EventType::Checkout)
        {
            checkouts += 1;
        }
        seen += 1;
        if seen.is_multiple_of(LIVE_SNAPSHOT_EVERY) || seen == expected {
            info!("live ticker: {seen}/{expected} events, {checkouts} checkouts");
        }
    }
    Ok(())
}

// A deterministic session: many visitors browsing, with page views the common case
// and checkouts the rare one, spaced a few seconds apart from a fixed base.
fn clickstream() -> Vec<Event> {
    let mut rng = Rng(0x1234_5678_9abc_def0);
    let mut ts = BASE_US;
    (0..events_total())
        .map(|_| {
            let user_id = VISITORS[rng.below(VISITORS.len() as u64) as usize].to_owned();
            // Weight the funnel: ~70% page views, ~22% add-to-cart, ~8% checkout.
            let message_type = match rng.below(100) {
                0..=69 => EventType::PageView,
                70..=91 => EventType::AddToCart,
                _ => EventType::Checkout,
            };
            let route = ROUTES[rng.below(ROUTES.len() as u64) as usize].to_owned();
            let latency_ms = 30 + rng.below(600) as u32;
            let event = Event {
                user_id,
                message_type,
                route,
                latency_ms,
                ts,
            };
            ts += 1 + rng.below(STEP_MAX_US);
            event
        })
        .collect()
}

// Publish the clickstream in chunks: each chunk is one `send_messages` call
// carrying its records with their own indexed columns, body inline so a query can
// decode the whole event. Many chunks rather than one request per event is the
// difference that matters on a rate-limited deployment, and the balanced
// partitioner spreads successive chunks across the topic's partitions.
async fn publish_clickstream(laser: &Laser, events: &[Event]) -> Result<(), LaserError> {
    let mut published = 0;
    for chunk in events.chunks(publish_chunk()) {
        let mut batch = laser.publish_batch(TOPIC);
        for event in chunk {
            // Body-first indexing: the projection's pointers extract every
            // column out of the JSON event, typed. No `agdx.idx.*` duplication
            // of the payload.
            let record = Record::builder()
                .content_type(ContentType::Json)
                .inline_payload(true)
                .build();
            batch = batch.add_record(
                serde_json::to_vec(event).map_err(|error| LaserError::Codec(error.to_string()))?,
                record,
            );
        }
        batch.send().await?;
        published += chunk.len();
        info!("published {published}/{} events to `{TOPIC}`", events.len());
    }
    Ok(())
}

// Poll until the projector has indexed every event, tolerant of a not-yet-created
// index while a remote LaserData Cloud applies the projection.
async fn wait_for_projection(laser: &Laser, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    let mut last = usize::MAX;
    loop {
        let total = laser
            .query(TOPIC)
            .fetch()
            .await
            .map(|result| result.page.total)
            .unwrap_or(0);
        if total != last {
            info!("projector materialized {total}/{expected} events");
            last = total;
        }
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} events before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// The analytics read model: the aggregates a dashboard asks of a clickstream.
async fn run_analytics(laser: &Laser) -> Result<(), LaserError> {
    // Funnel: how many events of each kind, grouped.
    let by_kind = laser
        .query(TOPIC)
        .count()
        .group_by([MESSAGE_TYPE])
        .fetch()
        .await?;
    info!("events by kind:");
    for row in &by_kind.rows {
        let kind = row.headers.get(MESSAGE_TYPE).map_or("?", String::as_str);
        let count = row.headers.get(COUNT_RESULT).map_or("0", String::as_str);
        info!("  {kind:<12} {count}");
    }

    // Slowest routes: order by latency, top 3.
    let slowest = laser
        .query(TOPIC)
        .order_desc(LATENCY_MS)
        .limit(3)
        .fetch()
        .await?;
    info!("slowest 3 routes:");
    for row in &slowest.rows {
        let route = row.headers.get(ROUTE).map_or("?", String::as_str);
        let latency = row.headers.get(LATENCY_MS).map_or("?", String::as_str);
        info!("  {latency:>5}ms  {route}");
    }

    // Checkouts only, via the reserved `message_type` field.
    let checkouts = laser
        .query(TOPIC)
        .message_type(EventType::Checkout.to_string())
        .count()
        .fetch()
        .await?;
    info!("checkouts: {}", scalar(&checkouts));

    // First 5 minutes of the session, via the reserved `ts` field and a time range.
    let first_window = laser
        .query(TOPIC)
        .time_range(BASE_US, BASE_US + 5 * ONE_MINUTE_US)
        .count()
        .fetch()
        .await?;
    info!("events in the first 5 minutes: {}", scalar(&first_window));

    // Per-minute event counts in ONE query via a tumbling window. Each result
    // row carries the bucket's lower edge under `window_start` plus the count.
    let per_minute = laser
        .query(TOPIC)
        .count()
        .window(TS, ONE_MINUTE_US)
        .fetch()
        .await?;
    info!("events per minute:");
    for row in &per_minute.rows {
        let bucket = row.headers.get(WINDOW_START).map_or("?", String::as_str);
        let count = row.headers.get(COUNT_RESULT).map_or("0", String::as_str);
        info!("  bucket {bucket}: {count}");
    }

    // Two metrics in one pass: mean latency and distinct routes per event kind.
    // `avg`/`count_distinct` are universal across backends (no capability gate).
    let by_kind_metrics = laser
        .query(TOPIC)
        .avg(LATENCY_MS)
        .count_distinct(ROUTE)
        .group_by([MESSAGE_TYPE])
        .fetch()
        .await?;
    info!("avg latency and distinct routes by kind:");
    for row in &by_kind_metrics.rows {
        let kind = row.headers.get(MESSAGE_TYPE).map_or("?", String::as_str);
        let avg = row.headers.get("avg").map_or("?", String::as_str);
        let routes = row
            .headers
            .get("count_distinct")
            .map_or("?", String::as_str);
        info!("  {kind:<12} avg={avg}ms routes={routes}");
    }
    Ok(())
}

// Read a single aggregate (`count`/`sum` with no group) off its one result row.
fn scalar(result: &QueryResult) -> i64 {
    result
        .rows
        .first()
        .and_then(|row| row.headers.get(COUNT_RESULT))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

// The resumable read model: a downstream export job tails the same log with a
// `Cursor`, persisting its offsets in a `StateStore` so a restart resumes from
// the checkpoint and processes only new events. `InMemoryStore` here.
// A `FileStore` or managed `laser.kv(..)` survives a real restart, same API.
async fn run_resumable_export(
    laser: &Laser,
    checkpoint: &impl StateStore,
) -> Result<(), LaserError> {
    let mut reader = laser.reader(TOPIC)?;
    let first = reader.poll().await?;
    info!(
        "export job read {} events, checkpointing offsets",
        first.len()
    );
    save_offsets(checkpoint, reader.offsets()).await?;

    // A brand-new cursor resumes from the saved offsets: a restart re-reads nothing.
    let mut resumed = laser
        .reader(TOPIC)?
        .from_offsets(load_offsets(checkpoint).await?);
    let again = resumed.poll().await?;
    info!(
        "after a restart, the export job re-read {} events (resumed from the checkpoint)",
        again.len()
    );
    Ok(())
}

async fn save_offsets(store: &impl StateStore, offsets: &[u64]) -> Result<(), LaserError> {
    store
        .set(
            CHECKPOINT_KEY,
            serde_json::to_vec(offsets).map_err(|error| LaserError::Codec(error.to_string()))?,
        )
        .await?;
    Ok(())
}

async fn load_offsets(store: &impl StateStore) -> Result<Vec<u64>, LaserError> {
    Ok(match store.get(CHECKPOINT_KEY).await? {
        Some(bytes) => {
            serde_json::from_slice(&bytes).map_err(|error| LaserError::Codec(error.to_string()))?
        }
        None => Vec::new(),
    })
}
