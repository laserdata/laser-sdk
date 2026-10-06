use laser_examples::{
    PARTITIONS, fresh_run, init_tracing, laser, managed_feature_ready, phase, start_projector,
    stream_for,
};
use laser_sdk::prelude::full::*;
use laser_sdk::schema_codecs::CompiledSchema;
use laser_sdk::stream::{ContentType, Record};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use strum::Display;
use tracing::info;

// A fleet telemetry tape with two readers on one connection, the shape a
// monitoring stack runs. Hosts stream CPU readings in real time and two read
// models consume them:
//   - the HOT path: a tuned Laser producer writes readings, and a live consumer
//     group folds them into a live fleet view. Straight off the log.
//   - the ANALYTICS path: the same readings are indexed to a queryable tape
//     LaserData Cloud materializes, and we run sample-weighted CPU aggregates
//     once the feed drains.

const FEED_TOPIC: &str = "metrics_feed"; // raw hot path
const TAPE_TOPIC: &str = "readings"; // queryable analytics tape
const FEED_GROUP: &str = "fleet-tape-builder";

// The schema-first tape (managed deployment): the same readings replay as raw
// Avro datums, decoded by a writer schema LaserData Cloud allocated an id for.
const AVRO_TAPE_TOPIC: &str = "readings_avro";
const AVRO_PROJECTION: &str = "readings_avro.v1";
const READING_AVRO_SCHEMA: &str = r#"{
    "type":"record","name":"HostReading",
    "fields":[
        {"name":"host","type":"string"},
        {"name":"cpu","type":"long"},
        {"name":"samples","type":"int"},
        {"name":"level","type":"string"},
        {"name":"cpu_total","type":"long"},
        {"name":"message_type","type":"string"},
        {"name":"ts","type":"long"}
    ]
}"#;
// Avro phase volume: enough to aggregate over, bounded so the cloud-gated
// coda stays quick even on a heavy soak.
const AVRO_READINGS_CAP: usize = 500;

// Indexed columns on the reading tape (the fields LaserData Cloud materializes).
const HOST: &str = "host";
const CPU: &str = "cpu";
const SAMPLES: &str = "samples";
const LEVEL: &str = "level";
const CPU_TOTAL: &str = "cpu_total";
// Reserved convention fields: every reading is a `reading` message stamped with
// a sample timestamp, so the reserved columns fill and the `message_type` /
// `time_range` query sugar works on the tape.
const MESSAGE_TYPE: &str = "message_type";
const TS: &str = "ts";
const COLUMNS: &[&str] = &[HOST, CPU, SAMPLES, LEVEL, CPU_TOTAL, MESSAGE_TYPE, TS];
// The grouped-sum result column the query layer returns.
const SUM_RESULT: &str = "sum";

// The reading count (on the shared volume knob `LASER_MESSAGES`, default 2000)
// streams to the live view in paced bursts (one snapshot per burst), then
// indexes to the tape in batches of `TAPE_BATCH` so the whole analytics write is
// a handful of `send_messages` calls instead of one request per reading. A burst
// gap keeps the live feed gentle, well under a free-tier deployment's ~100KB/s
// ceiling. Raise `LASER_MESSAGES` and shrink `BURST_GAP` against a local server.
const BURST: usize = 40;
const BURST_GAP: Duration = Duration::from_millis(120);
const TAPE_BATCH: usize = 100;

const PROJECTOR_TIMEOUT: Duration = Duration::from_secs(60);
const PROJECTION_POLL: Duration = Duration::from_millis(150);

// The starting load: a CPU percentage per host. The feed random-walks each
// from here.
const OPENING: &[(&str, i64)] = &[
    ("node-1", 42),
    ("node-2", 57),
    ("node-3", 31),
    ("node-4", 68),
    ("node-5", 49),
];

// A reading at or above this CPU percentage is `degraded`.
const DEGRADED_CPU: i64 = 80;

// The health level a reading reports. An enum with `strum::Display` + serde
// rename (not a bare string), so the indexed value and the JSON body cannot drift.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Level {
    Ok,
    Degraded,
}

// One reading: the CPU percentage a host averaged over `samples` samples.
// `cpu_total` is `cpu * samples`, so a sample-weighted mean is an exact integer
// division of two sums (never trust a float as an index key).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reading {
    host: String,
    cpu: i64,
    samples: u32,
    level: Level,
    cpu_total: i64,
    message_type: String,
    ts: u64,
}

impl Reading {
    fn new(host: &str, cpu: i64, samples: u32, ts: u64) -> Self {
        let level = if cpu >= DEGRADED_CPU {
            Level::Degraded
        } else {
            Level::Ok
        };
        Self {
            host: host.to_owned(),
            cpu,
            samples,
            level,
            cpu_total: cpu * i64::from(samples),
            message_type: "reading".to_owned(),
            ts,
        }
    }
}

// A tiny deterministic PRNG (xorshift64*), so the feed looks like a real random
// walk yet replays identically on every run without pulling in a rng crate.
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

// The fleet's running load: it draws the next reading by random-walking the
// last CPU percentage of a randomly chosen host.
struct Fleet {
    load: Vec<(&'static str, i64)>,
    rng: Rng,
    // Sample clock in epoch micros, stepped per reading from a fixed base so
    // the session replays identically.
    ts: u64,
}

impl Fleet {
    fn start() -> Self {
        Self {
            load: OPENING.to_vec(),
            rng: Rng(0x1234_5678_9abc_def0 | 1),
            ts: 1_900_000_000_000_000,
        }
    }

    // The next reading: pick a host, step its CPU by up to +/-7 points within
    // 0..=100, and size the sample window.
    fn next_reading(&mut self) -> Reading {
        let pick = self.below_len();
        let (host, cpu) = self.load[pick];
        let step = self.rng.below(15) as i64 - 7;
        let cpu = (cpu + step).clamp(0, 100);
        self.load[pick].1 = cpu;
        let samples = 1 + self.rng.below(500) as u32;
        self.ts += 1 + self.rng.below(50_000);
        Reading::new(host, cpu, samples, self.ts)
    }

    fn below_len(&mut self) -> usize {
        self.rng.below(self.load.len() as u64) as usize
    }
}

// A live fleet view folded from the feed: last CPU, cumulative samples, and
// cumulative weighted CPU per host, updated reading by reading so a rolling
// sample-weighted mean can be shown as the load moves.
#[derive(Default)]
struct FleetView {
    by_host: BTreeMap<String, HostLoad>,
}

#[derive(Default)]
struct HostLoad {
    last_cpu: i64,
    samples: u64,
    cpu_total: i128,
    degraded: u64,
}

impl FleetView {
    fn apply(&mut self, reading: &Reading) {
        let load = self.by_host.entry(reading.host.clone()).or_default();
        load.last_cpu = reading.cpu;
        load.samples += u64::from(reading.samples);
        load.cpu_total += i128::from(reading.cpu_total);
        if matches!(reading.level, Level::Degraded) {
            load.degraded += 1;
        }
    }

    fn snapshot(&self, readings: usize) {
        info!("fleet @ {readings} readings:");
        for (host, load) in &self.by_host {
            let mean = if load.samples > 0 {
                (load.cpu_total / i128::from(load.samples)) as i64
            } else {
                0
            };
            info!(
                "  {host:<7} last cpu {:>3}%  mean cpu {:>3}%  samples {:>8}  degraded {:>4}",
                load.last_cpu, mean, load.samples, load.degraded
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    phase("warming up");

    let data_stream = stream_for("fleet-tape");
    let laser = laser(&data_stream, Capabilities::OPEN).await?;
    fresh_run(&laser, &data_stream, async {
        laser.topic(FEED_TOPIC).ensure(PARTITIONS).await?;
        laser.topic(TAPE_TOPIC).ensure(PARTITIONS).await?;
        let query_available = laser.capabilities().await.query.available;

        // Start the projector before the feed opens so no reading is missed, then warm
        // the hot-path producer and consumer up front so the live phase below times
        // the fleet, not the one-off connection and consumer-group handshakes.
        let projector = if query_available {
            Some(start_projector(&laser, TAPE_TOPIC, ContentType::Json, COLUMNS).await?)
        } else {
            managed_feature_ready(false, "reading-tape analytics", "fleet-tape");
            None
        };
        let producer = build_feed_producer(&laser, &data_stream).await?;
        let mut consumer = build_fleet_consumer(&laser, &data_stream).await?;

        // Draw the whole session up front so the live feed and the tape index replay
        // the identical readings.
        let total = readings_total();
        let readings = generate_readings(total);

        phase("streaming a live telemetry feed");
        info!("streaming {total} readings across {} hosts", OPENING.len());
        let view = stream_live_view(producer, &mut consumer, &readings).await?;
        consumer.shutdown().await?;
        view.snapshot(total);

        phase("publishing the readings to the durable reading tape");
        // Capture the tape's head per partition before publishing, so the audit
        // below replays only this session's readings. The tape is durable: a re-run
        // against the same deployment appends a fresh session, and an audit from
        // offset zero would compare every session's readings against one session's.
        let tape_start = tape_head(&laser).await?;
        index_tape(&laser, &readings).await?;

        if query_available {
            phase("reading-tape analytics");
            wait_for_projection(&laser, total).await?;
            report_samples_and_mean(&laser).await?;
        }

        phase("typed tape audit: replay the log as `Reading` values");
        audit_tape(&laser, &readings, tape_start).await?;

        // The schema-first coda (managed deployment): the identical readings ride a
        // second tape as raw Avro datums. No `agdx.idx.*` headers this time, the
        // LaserData Cloud resolves the registered writer schema via `agdx.sid` and extracts
        // the indexed columns out of the binary bodies, and the weighted CPU totals
        // must come out the same as the JSON tape's.
        if laser.capabilities().await.managed {
            phase("schema-first tape: Avro readings decoded by a registered writer schema");
            avro_tape(&laser, &readings).await?;
        } else {
            info!("writer schemas need Laser Stack or LaserData Cloud, skipping the Avro tape");
        }

        if let Some(projector) = projector {
            projector.shutdown().await;
        }
        Ok(())
    })
    .await
}

// Tuned hot-path producer: balanced partitioning spreads the feed across
// partitions, bounded retries ride out a transient blip without dropping a reading.
async fn build_feed_producer(laser: &Laser, data_stream: &str) -> Result<Producer, LaserError> {
    laser
        .stream(data_stream)
        .topic(FEED_TOPIC)
        .producer()
        .routing(Routing::Balanced)
        .retries(Some(3), None)
        .partitions(PARTITIONS)
        .build()
        .await
}

// Low-latency hot-path consumer. Offsets commit SERVER-SIDE on each poll
// (`CommitPolicy::Polling`): the stored offset then moves in lockstep with
// delivery, the one commit mode that cannot starve the reader on a re-polled
// batch. A 1ms poll interval keeps reading-to-view latency tight without
// hammering the connection.
async fn build_fleet_consumer(laser: &Laser, data_stream: &str) -> Result<Consumer, LaserError> {
    laser
        .stream(data_stream)
        .topic(FEED_TOPIC)
        .consumer_group(FEED_GROUP)
        .consumer()
        .commit_policy(CommitPolicy::Polling)
        .start_at(ConsumerStart::Next)
        .poll_interval(Duration::from_millis(1))
        .batch_length(256)
        .build()
        .await
}

// Draw the session deterministically so both read models replay identical readings.
fn generate_readings(count: usize) -> Vec<Reading> {
    let mut fleet = Fleet::start();
    (0..count).map(|_| fleet.next_reading()).collect()
}

// Reading count, on the shared volume knob so one run scales from a smoke test to
// a soak (`LASER_MESSAGES`).
fn readings_total() -> usize {
    laser_examples::messages(2000) as usize
}

// How long the view reader waits for the next reading before giving up with a
// diagnostic instead of a silent hang.
const READING_TIMEOUT: Duration = Duration::from_secs(15);

// Stream the raw hot feed and fold arriving readings into the live view,
// snapshotting as the load moves. The producer paces bursts in its own
// task while the reader consumes whatever has arrived: the two sides are
// deliberately NOT in lockstep, so one duplicated or delayed delivery can
// never deadlock the loop.
async fn stream_live_view(
    producer: Producer,
    consumer: &mut Consumer,
    readings: &[Reading],
) -> Result<FleetView, LaserError> {
    let bursts: Vec<Vec<ProducerMessage>> = readings
        .chunks(BURST)
        .map(|burst| {
            burst
                .iter()
                .map(|reading| {
                    Ok(ProducerMessage::new(
                        serde_json::to_vec(reading)
                            .map_err(|error| LaserError::Codec(error.to_string()))?,
                    ))
                })
                .collect::<Result<Vec<_>, LaserError>>()
        })
        .collect::<Result<Vec<_>, LaserError>>()?;
    let feed = tokio::spawn(async move {
        for raw in bursts {
            producer.send_batch(raw).await?;
            tokio::time::sleep(BURST_GAP).await;
        }
        Ok::<(), LaserError>(())
    });

    let mut view = FleetView::default();
    let mut seen = 0usize;
    while seen < readings.len() {
        let received = match tokio::time::timeout(READING_TIMEOUT, consumer.next()).await {
            Ok(Some(received)) => received?,
            Ok(None) => {
                return Err(LaserError::Invalid(format!(
                    "feed ended after {seen}/{} readings",
                    readings.len()
                )));
            }
            Err(_) => {
                return Err(LaserError::Invalid(format!(
                    "no reading arrived for {}s after {seen}/{} readings. Either the feed task \
                     failed (its error surfaces right after this one) or the `{FEED_GROUP}` \
                     consumer group is not receiving deliveries from this server",
                    READING_TIMEOUT.as_secs(),
                    readings.len(),
                )));
            }
        };
        let reading: Reading = received.json()?;
        view.apply(&reading);
        seen += 1;
        if seen.is_multiple_of(BURST) {
            view.snapshot(seen);
        }
    }
    feed.await
        .map_err(|error| LaserError::Invalid(format!("feed task: {error}")))??;
    Ok(view)
}

// Index every reading to the queryable tape in batches of `TAPE_BATCH`: each batch is
// one `send_messages` call carrying its rows with their own indexed columns and
// inline bodies, so the whole analytics write is a handful of round trips rather
// than one per reading. That is the difference between a smooth run and hundreds
// of requests against a rate-limited deployment.
async fn index_tape(laser: &Laser, readings: &[Reading]) -> Result<(), LaserError> {
    let mut indexed = 0;
    for chunk in readings.chunks(TAPE_BATCH) {
        let tape = laser.topic(TAPE_TOPIC);
        let mut batch = tape.publish_batch();
        for reading in chunk {
            // Body-first indexing: the projection's pointers extract every
            // column out of the JSON reading, typed (integers stay
            // integers). No `agdx.idx.*` duplication of the payload.
            let record = Record::builder()
                .content_type(ContentType::Json)
                .inline_payload()
                .build();
            batch = batch.add_record(
                serde_json::to_vec(reading)
                    .map_err(|error| LaserError::Codec(error.to_string()))?,
                record,
            );
        }
        batch.send().await?;
        indexed += chunk.len();
        info!(
            "indexed {indexed}/{} readings to `{TAPE_TOPIC}`",
            readings.len()
        );
    }
    Ok(())
}

// Poll until the projector has indexed every reading, tolerant of a not-yet-created
// index while a remote LaserData Cloud applies the projection.
async fn wait_for_projection(laser: &Laser, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    let mut last = usize::MAX;
    loop {
        let total = laser
            .query(TAPE_TOPIC)
            .with_total()
            .fetch()
            .await
            .map(|result| result.page.total.unwrap_or(0) as usize)
            .unwrap_or(0);
        if total != last {
            info!("projector materialized {total}/{expected} readings");
            last = total;
        }
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} readings before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// Query the materialized tape: per-host sample counts, and the sample-weighted
// mean CPU derived from two grouped sums (mean = cpu_total / samples).
async fn report_samples_and_mean(laser: &Laser) -> Result<(), LaserError> {
    let start = Instant::now();
    let samples = laser
        .query(TAPE_TOPIC)
        .sum(SAMPLES)
        .group_by([HOST])
        .fetch()
        .await?;
    let cpu_total = laser
        .query(TAPE_TOPIC)
        .sum(CPU_TOTAL)
        .group_by([HOST])
        .fetch()
        .await?;

    let samples_by_host = group_totals(&samples);
    let cpu_total_by_host = group_totals(&cpu_total);

    info!(
        "tape analytics over {} samples (Laser query layer), {}ms:",
        samples_by_host.values().sum::<i64>(),
        start.elapsed().as_millis()
    );
    for (host, samples) in &samples_by_host {
        let cpu_total = cpu_total_by_host.get(host).copied().unwrap_or(0);
        let mean = if *samples > 0 {
            cpu_total / *samples
        } else {
            0
        };
        info!("  {host:<7} samples {samples:>8}  mean cpu {mean:>3}%");
    }
    Ok(())
}

// Collect a `sum(..).group_by([HOST])` result into `host -> total`. Each row
// carries the group key and aggregate in its typed result fields.
fn group_totals(result: &QueryResult) -> BTreeMap<String, i64> {
    result
        .rows
        .iter()
        .filter_map(|row| {
            let host = result.value_text(row, HOST)?;
            let total = result.value_i64(row, SUM_RESULT)?;
            Some((host, total))
        })
        .collect()
}

// The audit a monitoring stack runs against its own tape: replay the raw log as
// typed `Reading` values through one typed handle and recompute the weighted
// CPU totals the query layer just aggregated. `records` decodes each payload as it drains, a
// record that stopped decoding would surface with its exact log position, and
// the totals off the log must equal the projected view's.
// The tape's per-partition head right now, drained with a throwaway reader.
// The audit resumes from here so it never folds a prior run's session.
async fn tape_head(laser: &Laser) -> Result<Vec<u64>, LaserError> {
    let tape = laser.topic(TAPE_TOPIC).json::<Reading>();
    let mut reader = tape.records("fleet-tape-head")?;
    while reader.next().await.is_some() {}
    Ok(reader.offsets().to_vec())
}

async fn audit_tape(laser: &Laser, readings: &[Reading], from: Vec<u64>) -> Result<(), LaserError> {
    let tape = laser.topic(TAPE_TOPIC).json::<Reading>();
    let mut records = tape.records("fleet-tape-audit")?.from_offsets(from);
    let mut cpu_total_by_host: BTreeMap<String, i64> = BTreeMap::new();
    let mut audited = 0usize;
    while let Some(next) = records.next().await {
        let reading = next?.value;
        *cpu_total_by_host.entry(reading.host).or_default() += reading.cpu_total;
        audited += 1;
    }
    let expected: BTreeMap<String, i64> =
        readings
            .iter()
            .fold(BTreeMap::new(), |mut totals, reading| {
                *totals.entry(reading.host.clone()).or_default() += reading.cpu_total;
                totals
            });
    if cpu_total_by_host != expected {
        return Err(LaserError::Invalid(
            "the typed replay disagrees with the session's own weighted CPU totals".to_owned(),
        ));
    }
    info!(
        "audited {audited} readings off the log, every host's weighted CPU total matches the session"
    );
    Ok(())
}

// Register the HostReading writer schema (synchronous: LaserData Cloud validates the
// definition, allocates a collision-free id, and returns it), project the
// Avro topic by body pointers, publish a slice of the session as raw datums
// via the `schema-codecs` client-side encoder, and aggregate the decoded
// columns.
async fn avro_tape(laser: &Laser, readings: &[Reading]) -> Result<(), LaserError> {
    let schema_id = laser
        .schemas()
        .register(SchemaSource::Avro {
            schema: READING_AVRO_SCHEMA.to_owned(),
        })
        .name("fleet_reading")
        .send()
        .await?;
    info!("LaserData Cloud allocated writer-schema id {schema_id} for the HostReading schema");

    laser.topic(AVRO_TAPE_TOPIC).ensure(PARTITIONS).await?;
    laser
        .projections()
        .register(
            Projection::builder(AVRO_PROJECTION)
                .name("readings_avro")
                .version(1)
                .content_type(ContentType::Avro)
                .fields(COLUMNS.iter().copied())
                .build(),
        )
        .await?;
    laser
        .bindings()
        .apply(
            ProjectionBinding::builder()
                .source(stream_for("fleet-tape"), AVRO_TAPE_TOPIC)
                .allow(AVRO_PROJECTION)
                .default_projection(AVRO_PROJECTION)
                .index(AVRO_TAPE_TOPIC)
                .build(),
        )
        .await?;
    wait_for_schema(laser, schema_id).await?;

    // Compile once client-side: `.add_avro` then fails BEFORE publishing if a
    // body stops matching the registered schema, instead of a managed-side warn
    // the producer cannot see.
    let compiled = CompiledSchema::compile(&SchemaDef {
        id: schema_id,
        source: SchemaSource::Avro {
            schema: READING_AVRO_SCHEMA.to_owned(),
        },
        name: None,
        version: None,
    })?;
    let slice = &readings[..readings.len().min(AVRO_READINGS_CAP)];
    let tape = laser.topic(AVRO_TAPE_TOPIC);
    let mut request = tape.publish_batch().projection_ref(AVRO_PROJECTION);
    for reading in slice {
        request = request.add_avro(&compiled, schema_id, reading)?;
    }
    request.send().await?;
    info!("published {} readings as raw Avro datums", slice.len());

    wait_for_table(laser, AVRO_TAPE_TOPIC, slice.len()).await?;
    let per_host = laser
        .query(AVRO_TAPE_TOPIC)
        .sum(CPU_TOTAL)
        .group_by([HOST])
        .fetch()
        .await?;
    info!("weighted CPU total per host, aggregated over columns decoded out of Avro bodies:");
    for row in &per_host.rows {
        let host = per_host
            .value_text(row, HOST)
            .unwrap_or_else(|| "?".to_owned());
        let total = per_host
            .value_text(row, SUM_RESULT)
            .unwrap_or_else(|| "0".to_owned());
        info!("  {:<7} {:>14}", host, total);
    }
    Ok(())
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

// Poll until `expected` rows are materialized in `table`, tolerant of a
// not-yet-created table while LaserData Cloud applies the projection.
async fn wait_for_table(laser: &Laser, table: &str, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    loop {
        let total = laser
            .query(table)
            .with_total()
            .fetch()
            .await
            .map(|result| result.page.total.unwrap_or(0) as usize)
            .unwrap_or(0);
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} rows in `{table}` before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}
