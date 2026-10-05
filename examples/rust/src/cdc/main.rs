mod codecs;

use laser_examples::{
    fresh_run, init_tracing, laser, managed_feature_ready, phase, run_token, stream_for,
};
use laser_sdk::filters::{
    ConsumerFilter, FilterErrorReason, FilterExpr, FilteredReader, FilteredStart, MatchedRecord,
};
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
use laser_sdk::prelude::full::*;
use laser_sdk::query::CmpOp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

// A satellite fleet streams the change feed of its mission-ops database: every
// battery reading, orbit maneuver, and ground-station status flip. The anomaly
// desk only wants satellites entering safe mode or leaving the fleet. The
// desk's consumer group owns that filter: the server evaluates it next to the
// data, so the desk receives a handful of records out of hundreds, and
// everything else never leaves the broker.
const TOPIC: &str = "fleet_changes";
const ALERTS: &str = "fleet_alerts";
const PARTITIONS: u32 = 3;
const GROUP: &str = "anomaly-desk";
const SATELLITES: usize = 8;
const FEED_SIZE: usize = 240;
const ROUTINE: u8 = 1;
const CRITICAL: u8 = 2;
const READ_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for("cdc");
    let laser = laser(&stream, Capabilities::OPEN).await?;
    fresh_run(&laser, &stream, async {
        let capabilities = laser.capabilities().await;
        if !managed_feature_ready(capabilities.filters.catalog, "consumer group filters", "cdc")
        {
            return Ok(());
        }

        phase("publish a busy fleet change feed, keyed by satellite");
        let feed = fleet_feed();
        let topic = laser.topic(TOPIC);
        topic.ensure(PARTITIONS).await?;
        let mut published_bytes = 0;
        let mut by_key = BTreeMap::<&str, Vec<&FleetChange>>::new();
        for change in &feed.records {
            published_bytes += serde_json::to_vec(change)
                .map_err(|error| LaserError::Codec(error.to_string()))?
                .len();
            by_key.entry(change.key()).or_default().push(change);
        }
        for (key, changes) in by_key {
            let started = Instant::now();
            let mut batch = topic.publish_batch().partition_key(key);
            for change in &changes {
                batch = batch.add_json(change)?;
            }
            batch.send().await?;
            println!(
                "  {key}: {} records in {} ms",
                changes.len(),
                started.elapsed().as_millis()
            );
        }
        println!(
            "  {} records, {published_bytes} bytes: battery readings, maneuvers, station flips, and {} safe-mode or decommission events",
            feed.records.len(),
            feed.strict_matches
        );

        phase("create the anomaly desk group with its filter");
        let desk_name = format!("{GROUP}-{}", run_token());
        let desk = topic.consumer_group(&desk_name);
        let created = desk.create().filter(safe_mode_filter()).build().await?;
        let binding = created
            .filter
            .clone()
            .ok_or_else(|| LaserError::Invalid("the group was created unbound".to_owned()))?;
        println!(
            "  group {} ({}) runs revision {} of its own filter from now on",
            created.name, created.id, binding.revision
        );

        phase("consume as the group: the application names the group, the server runs its filter");
        let mut consumer = desk
            .consumer()
            .start_at(ConsumerStart::First)
            .commit_policy(CommitPolicy::Disabled)
            .build()
            .await?;
        let consumed = async {
            let mut delivered_bytes = 0;
            for _ in 0..feed.strict_matches {
                let message = consumer.next_within(READ_TIMEOUT).await?;
                let change: FleetChange = message.json()?;
                println!(
                    "  partition {} offset {}: {}",
                    message.partition_id,
                    message.position.offset,
                    change.describe()
                );
                delivered_bytes += message.payload.len();
                consumer.commit(&message).await?;
            }
            Ok::<usize, LaserError>(delivered_bytes)
        }
        .await;
        consumer.shutdown().await?;
        let delivered_bytes = consumed?;
        println!(
            "  delivered {} of {} records, {delivered_bytes} of {published_bytes} payload bytes: {:.1}% stayed on the broker",
            feed.strict_matches,
            feed.records.len(),
            100.0 * (published_bytes - delivered_bytes) as f64 / published_bytes.max(1) as f64
        );

        phase("page the matches again with the group reader and its own scan budget");
        let mut pager = desk
            .reader()?
            .start(FilteredStart::First)
            .count(10)
            .max_examined(100)
            .local_guard(true)
            .build()
            .await?;
        let paged = read_matches(&mut pager, feed.strict_matches).await;
        pager.close().await?;
        println!(
            "  the reader handed out {} matches in pages, each acknowledged after handling",
            paged?.len()
        );

        phase("test the group's filter against a battery update of a satellite already in safe mode");
        let still_safe = satellite_update(2, Mode::Safe, 58, Column::BatteryPct);
        let sample = serde_json::to_string(&still_safe)
            .map_err(|error| LaserError::Codec(error.to_string()))?;
        let tested = desk.filter().test(sample, Vec::new()).await?;
        println!("  strict, transitions only: {}", tested.explanation.verdict);

        phase("preview every partition, nothing is stored");
        for partition_id in 0..PARTITIONS {
            let preview = desk
                .filter()
                .preview(partition_id)
                .await?
                .max_records(10)
                .send()
                .await?;
            println!(
                "  partition {partition_id}: examined {}, matched {}, stopped at {}",
                preview.examined, preview.matched, preview.stop
            );
        }

        phase("route binary alerts on a header, their payload is never decoded");
        route_alerts(&laser, &stream).await?;
        codecs::run(&laser, &stream).await?;

        manage_revisions(&laser, &desk, &binding, feed.strict_matches).await
    })
    .await
}

// Binary alert frames carry their priority as a header. A pager group with a
// headers-only filter selects the critical ones without decoding a payload,
// so the alert topic can hold any format.
async fn route_alerts(laser: &Laser, stream: &str) -> Result<(), LaserError> {
    let alerts = laser.stream(stream).topic(ALERTS);
    alerts.ensure(1).await?;
    let producer = alerts.producer().build().await?;
    for (priority, satellite_id) in [
        (ROUTINE, "sat-001"),
        (CRITICAL, "sat-003"),
        (ROUTINE, "sat-004"),
        (CRITICAL, "sat-007"),
    ] {
        let mut frame = vec![0x0a, 0x07];
        frame.extend_from_slice(satellite_id.as_bytes());
        let message = ProducerMessage::new(frame).header(
            HeaderKey::try_from("priority")?,
            HeaderValue::from(priority),
        );
        producer.send_keyed(message, satellite_id).await?;
    }
    let pager_group = alerts.consumer_group(format!("{GROUP}-pager-{}", run_token()));
    pager_group
        .create()
        .filter(ConsumerFilter::headers_only(FilterExpr::header(
            "priority",
            CmpOp::Eq,
            i32::from(CRITICAL),
        )))
        .build()
        .await?;
    let mut pager = pager_group
        .reader()?
        .start(FilteredStart::First)
        .build()
        .await?;
    let paged = async {
        for _ in 0..2 {
            let record = tokio::time::timeout(READ_TIMEOUT, pager.next_record())
                .await
                .map_err(|_| LaserError::Timeout("the next critical alert"))??;
            println!(
                "  critical alert at offset {}: {} opaque bytes",
                record.offset,
                record.message.payload.len()
            );
            pager.ack(&record).await?;
        }
        Ok(())
    }
    .await;
    pager.close().await?;
    pager_group.filter().delete().await?;
    paged
}

// Draft a stricter revision on the desk's own filter, run the variant in its
// own group, pause and resume it, then release and delete both policies,
// also when a step fails.
async fn manage_revisions(
    laser: &Laser,
    desk: &ConsumerGroup,
    binding: &FilterBinding,
    expected: usize,
) -> Result<(), LaserError> {
    phase("draft a stricter revision: readers keep running the active one");
    let draft = desk
        .filter()
        .revise(
            binding.revision,
            ConsumerFilter::json(safe_mode_transition()),
        )
        .await?;
    let revisions = desk.filter().revisions(0, 10).await?;
    println!(
        "  revision {} drafted, the group lists {} revisions and still runs revision {}",
        draft.revision, revisions.total, binding.revision
    );

    phase("A/B: the transitions-only variant runs in its own group");
    let variant_name = format!("{GROUP}-transitions-{}", run_token());
    let variant = laser.topic(TOPIC).consumer_group(&variant_name);
    let variant_created = variant
        .create()
        .filter(ConsumerFilter::json(safe_mode_transition()))
        .build()
        .await?;
    let variant_binding = variant_created
        .filter
        .ok_or_else(|| LaserError::Invalid("the variant was created unbound".to_owned()))?;
    let mut reader = variant
        .reader()?
        .count(1)
        .local_guard(true)
        .start(FilteredStart::First)
        .build()
        .await?;
    let compared = async {
        let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
            .await
            .map_err(|_| LaserError::Timeout("the A/B record"))??;
        let change: FleetChange = first.json()?;
        println!("  {variant_name}: {}", change.describe());

        phase("pause the variant: new reads stop, in-flight work still acknowledges");
        variant
            .filter()
            .set_revision_enabled(variant_binding.revision, false)
            .await?;
        reader.ack(&first).await?;
        match reader.try_next_page().await {
            Err(error) if error.filter_reason() == Some(FilterErrorReason::RevisionDisabled) => {
                println!("  paused: the server refuses new reads with revision_disabled")
            }
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(LaserError::Invalid(
                    "a disabled revision kept reading".to_owned(),
                ));
            }
        }
        variant
            .filter()
            .set_revision_enabled(variant_binding.revision, true)
            .await?;
        read_matches(&mut reader, 1).await?;
        println!("  resumed: the desk handled {expected} broad events, the variant 2 transitions");
        Ok(())
    }
    .await;
    let closed = reader.close().await;
    compared.and(closed)?;

    phase("a group that runs a policy cannot be switched to another one");
    match desk
        .filter()
        .configure(ConsumerFilter::json(safe_mode_transition()))
        .await
    {
        Err(error) if error.filter_reason() == Some(FilterErrorReason::Conflict) => {
            println!("  refused with conflict: create a new group for another policy");
        }
        Err(error) => return Err(error),
        Ok(_) => {
            return Err(LaserError::Invalid(
                "a running policy was replaced".to_owned(),
            ));
        }
    }

    phase("release both policies");
    let released = desk.filter().release().await?;
    variant.filter().release().await?;
    println!(
        "  {} is unbound again and receives every record, its filter stays saved as revision {}",
        released.group.group, released.revision
    );

    phase("delete both filters: nothing of them stays in the catalog");
    desk.filter().delete().await?;
    variant.filter().delete().await?;
    println!("  deleted with every revision, the groups keep reading everything");
    Ok(())
}

// Read and acknowledge until `expected` safe-mode or decommission events arrived, decoding
// each one into the typed change it is.
async fn read_matches(
    reader: &mut FilteredReader,
    expected: usize,
) -> Result<Vec<MatchedRecord>, LaserError> {
    let mut delivered = Vec::with_capacity(expected);
    for _ in 0..expected {
        let record = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
            .await
            .map_err(|_| LaserError::Timeout("the next matching record"))??;
        let change: FleetChange = record.json()?;
        println!(
            "  partition {} offset {}: {}",
            record.partition_id,
            record.offset,
            change.describe()
        );
        reader.ack(&record).await?;
        delivered.push(record);
    }
    Ok(delivered)
}

// Strict: a satellite update that marks its mode as changed to safe, a
// telemetry report of safe mode, or a satellite leaving the fleet.
fn safe_mode_filter() -> ConsumerFilter {
    let satellites = || FilterExpr::pred("table", CmpOp::Eq, "satellites");
    ConsumerFilter::json(FilterExpr::any([
        safe_mode_transition(),
        FilterExpr::all([satellites(), FilterExpr::pred("op", CmpOp::Eq, "d")]),
        FilterExpr::all([
            FilterExpr::pred("event", CmpOp::Eq, "satellite.telemetry_changed"),
            FilterExpr::pred("fields.mode", CmpOp::Eq, "safe"),
        ]),
    ]))
}

fn safe_mode_transition() -> FilterExpr {
    FilterExpr::all([
        FilterExpr::pred("table", CmpOp::Eq, "satellites"),
        FilterExpr::pred("op", CmpOp::Eq, "u"),
        FilterExpr::pred("changed", CmpOp::Contains, "mode"),
        FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
    ])
}

/// One record of the change feed: a row change captured from the mission-ops
/// database, or a telemetry event a satellite reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum FleetChange {
    Row(RowChange),
    Telemetry(TelemetryEvent),
}

/// A captured row change, tagged by its table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "table", rename_all = "snake_case")]
enum RowChange {
    Satellites(Change<Satellite>),
    GroundStations(Change<GroundStation>),
}

/// The change-data-capture envelope of one row.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Change<Row> {
    op: Op,
    /// The columns the producer marks as changed on an update.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changed: Vec<Column>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<Row>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<RowKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Op {
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Column {
    Mode,
    BatteryPct,
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Satellite {
    id: String,
    name: String,
    mode: Mode,
    orbit: Orbit,
    battery_pct: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Mode {
    Nominal,
    Maneuver,
    Safe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Orbit {
    Leo,
    Meo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroundStation {
    id: String,
    status: StationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StationStatus {
    Online,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowKey {
    id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TelemetryEvent {
    event: TelemetryKind,
    satellite_id: String,
    fields: TelemetryFields,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TelemetryKind {
    #[serde(rename = "satellite.telemetry_changed")]
    TelemetryChanged,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TelemetryFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<Mode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    battery_pct: Option<u8>,
}

/// The generated feed, with what the anomaly desk expects from it.
struct Feed {
    records: Vec<FleetChange>,
    /// Records the strict filter selects: two safe-mode transitions, one
    /// telemetry report of safe mode, and one decommission.
    strict_matches: usize,
}

impl FleetChange {
    // The partition key: every change of one satellite or station stays in
    // order on one partition.
    fn key(&self) -> &str {
        match self {
            Self::Row(RowChange::Satellites(change)) => change
                .after
                .as_ref()
                .map(|satellite| satellite.id.as_str())
                .or_else(|| change.before.as_ref().map(|key| key.id.as_str()))
                .unwrap_or_default(),
            Self::Row(RowChange::GroundStations(change)) => change
                .after
                .as_ref()
                .map(|station| station.id.as_str())
                .unwrap_or_default(),
            Self::Telemetry(event) => &event.satellite_id,
        }
    }

    // What the desk sees, told from the typed record.
    fn describe(&self) -> String {
        match self {
            Self::Row(RowChange::Satellites(Change {
                op: Op::Delete,
                before: Some(key),
                ..
            })) => format!("{} left the fleet", key.id),
            Self::Row(RowChange::Satellites(Change {
                after: Some(satellite),
                ..
            })) => format!(
                "{} entered {} mode at {}% battery",
                satellite.name, satellite.mode, satellite.battery_pct
            ),
            Self::Telemetry(TelemetryEvent {
                satellite_id,
                fields: TelemetryFields {
                    mode: Some(mode), ..
                },
                ..
            }) => format!("{satellite_id} reported {mode} mode"),
            other => format!("{other:?}"),
        }
    }
}

// A deterministic feed: mostly battery telemetry, battery updates, orbit
// maneuvers, and ground-station status flips, with the four events the
// anomaly desk cares about spread through it.
fn fleet_feed() -> Feed {
    let mut modes = [Mode::Nominal; SATELLITES];
    let mut records = Vec::with_capacity(FEED_SIZE);
    let mut strict_matches = 0;
    for tick in 0..FEED_SIZE {
        let index = tick % SATELLITES;
        let battery = u8::try_from(90 - (tick * 7) % 40).unwrap_or(50);
        let change = if tick == FEED_SIZE / 4 || tick == 3 * FEED_SIZE / 4 {
            let entering = if tick == FEED_SIZE / 4 { 2 } else { 6 };
            modes[entering] = Mode::Safe;
            strict_matches += 1;
            satellite_update(entering, Mode::Safe, battery, Column::Mode)
        } else if tick == FEED_SIZE / 2 {
            strict_matches += 1;
            telemetry(4, Some(Mode::Safe), None)
        } else if tick == FEED_SIZE - 1 {
            strict_matches += 1;
            FleetChange::Row(RowChange::Satellites(Change {
                op: Op::Delete,
                changed: Vec::new(),
                after: None,
                before: Some(RowKey {
                    id: satellite(7, Mode::Nominal, 0).id,
                }),
            }))
        } else {
            match (tick % 10, modes[index]) {
                (0..=4, _) => telemetry(index, None, Some(battery)),
                (5..=7, mode) | (_, mode @ Mode::Safe) => {
                    satellite_update(index, mode, battery, Column::BatteryPct)
                }
                (8, _) => FleetChange::Row(RowChange::GroundStations(Change {
                    op: Op::Update,
                    changed: vec![Column::Status],
                    after: Some(GroundStation {
                        id: ["svalbard", "kiruna", "punta-arenas"][tick % 3].to_owned(),
                        status: if tick % 20 == 8 {
                            StationStatus::Maintenance
                        } else {
                            StationStatus::Online
                        },
                    }),
                    before: None,
                })),
                _ => satellite_update(index, Mode::Maneuver, battery, Column::Mode),
            }
        };
        records.push(change);
    }
    Feed {
        records,
        strict_matches,
    }
}

fn satellite_update(index: usize, mode: Mode, battery: u8, changed: Column) -> FleetChange {
    FleetChange::Row(RowChange::Satellites(Change {
        op: Op::Update,
        changed: vec![changed],
        after: Some(satellite(index, mode, battery)),
        before: None,
    }))
}

fn telemetry(index: usize, mode: Option<Mode>, battery_pct: Option<u8>) -> FleetChange {
    FleetChange::Telemetry(TelemetryEvent {
        event: TelemetryKind::TelemetryChanged,
        satellite_id: satellite(index, Mode::Nominal, 0).id,
        fields: TelemetryFields { mode, battery_pct },
    })
}

fn satellite(index: usize, mode: Mode, battery_pct: u8) -> Satellite {
    Satellite {
        id: format!("sat-{:03}", index + 1),
        name: format!("Kestrel-{}", index + 1),
        mode,
        orbit: if index.is_multiple_of(2) {
            Orbit::Leo
        } else {
            Orbit::Meo
        },
        battery_pct,
    }
}
