mod codecs;

use laser_examples::{
    fresh_run, init_tracing, laser, managed_feature_ready, phase, run_token, stream_for,
};
use laser_sdk::filters::{
    ConsumerFilter, FilterErrorReason, FilterExpr, FilterGroupRef, FilterRef, FilteredReader,
    FilteredStart, MatchedRecord,
};
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
use laser_sdk::prelude::full::*;
use laser_sdk::query::CmpOp;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// A satellite fleet streams the change feed of its mission-ops database: every
// battery reading, orbit maneuver, and ground-station status flip. The anomaly
// desk only wants satellites entering safe mode or leaving the fleet. The
// server evaluates the filter next to the data, so the desk receives a handful
// of records out of hundreds, and everything else never leaves the broker.
const TOPIC: &str = "fleet_changes";
const ALERTS: &str = "fleet_alerts";
const PARTITIONS: u32 = 3;
const GROUP: &str = "anomaly-desk";
const BACKFILL: &str = "safe-mode-backfill";
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
        if !managed_feature_ready(capabilities.filters.native, "consumer filters", "cdc") {
            return Ok(());
        }

        phase("publish a busy fleet change feed, keyed by satellite");
        let feed = fleet_feed();
        let topic = laser.topic(TOPIC);
        topic.ensure(PARTITIONS).await?;
        let mut published_bytes = 0;
        for change in &feed.records {
            published_bytes += serde_json::to_vec(change)
                .map_err(|error| LaserError::Codec(error.to_string()))?
                .len();
            topic
                .publish()
                .partition_key(change.key())
                .json(change)?
                .send()
                .await?;
        }
        println!(
            "  {} records, {published_bytes} bytes: battery readings, maneuvers, station flips, and {} safe-mode or decommission events",
            feed.records.len(),
            feed.strict_matches
        );

        phase("read only the safe-mode or decommission events, no catalog needed");
        let mut backfill = laser
            .filters()
            .reader(&stream, TOPIC)
            .consumer(BACKFILL)
            .inline(safe_mode_filter())
            .start(FilteredStart::First)
            .build()
            .await?;
        let delivered = read_matches(&mut backfill, feed.strict_matches).await;
        backfill.close().await?;
        let delivered = delivered?;
        let delivered_bytes: usize = delivered
            .iter()
            .map(|record| record.message.payload.len())
            .sum();
        println!(
            "  delivered {} of {} records, {delivered_bytes} of {published_bytes} payload bytes: {:.1}% stayed on the broker",
            delivered.len(),
            feed.records.len(),
            100.0 * (published_bytes - delivered_bytes) as f64 / published_bytes.max(1) as f64
        );

        phase("test both filters against a battery update of a satellite already in safe mode");
        let still_safe = satellite_update(2, Mode::Safe, 58, Column::BatteryPct);
        let sample = serde_json::to_string(&still_safe)
            .map_err(|error| LaserError::Codec(error.to_string()))?;
        for (name, filter) in [
            ("strict, transitions only", safe_mode_filter()),
            ("values only, current state", safe_mode_values_filter()),
        ] {
            let tested = laser
                .filters()
                .test(FilterRef::Inline(filter), sample.clone(), Vec::new())
                .await?;
            println!("  {name}: {}", tested.explanation.verdict);
        }

        phase("preview every partition, nothing is stored");
        for partition_id in 0..PARTITIONS {
            let preview = laser
                .filters()
                .preview(
                    &stream,
                    TOPIC,
                    partition_id,
                    FilterRef::Inline(safe_mode_filter()),
                )
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
        codecs::run(&laser, &stream, capabilities.filters.catalog).await?;

        if !capabilities.filters.catalog {
            println!("  the saved-filter catalog needs a managed plane, skipping group bindings");
            return Ok(());
        }
        manage_group(&laser, &stream, feed.strict_matches).await
    })
    .await
}

// Binary alert frames carry their priority as a header. A headers-only filter
// selects the critical ones without decoding a payload, so the alert topic can
// hold any format.
async fn route_alerts(laser: &Laser, stream: &str) -> Result<(), LaserError> {
    let alerts = laser.topic(ALERTS);
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
    let mut pager = laser
        .filters()
        .reader(stream, ALERTS)
        .consumer(format!("{BACKFILL}-pager"))
        .inline(ConsumerFilter::headers_only(FilterExpr::header(
            "priority",
            CmpOp::Eq,
            i32::from(CRITICAL),
        )))
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
    paged
}

// Save both filters, bind the anomaly desk to the strict one, consume as the
// group, then release everything this run created, also when a step fails.
async fn manage_group(laser: &Laser, stream: &str, expected: usize) -> Result<(), LaserError> {
    phase("save both filters in the catalog");
    let filters = laser.filters();
    let mut saved = Vec::new();
    let mut bindings = Vec::new();
    let outcome = async {
        let strict = filters
            .register(
                format!("sats-safe-mode-{}", run_token()),
                safe_mode_filter(),
                "Satellites entering safe mode, reporting it, or leaving the fleet",
            )
            .await?;
        saved.push(strict.filter_id);
        let values = filters
            .register(
                format!("sats-safe-mode-values-{}", run_token()),
                safe_mode_values_filter(),
                "Every update of a satellite whose current mode is safe",
            )
            .await?;
        saved.push(values.filter_id);
        println!(
            "  strict is filter {} revision {}, values only is filter {} revision {}",
            strict.filter_id, strict.revision, values.filter_id, values.revision
        );

        phase("bind the anomaly desk group to the strict filter");
        let group = FilterGroupRef {
            stream: stream.to_owned(),
            topic: TOPIC.to_owned(),
            group: GROUP.to_owned(),
        };
        let bound = filters
            .create_consumer_group(group, strict.filter_id, strict.revision)
            .await?;
        println!("  {GROUP} runs revision {} from now on", bound.revision);
        let group_id = bound.identity.group_id;
        bindings.push(bound);

        phase("consume as the group and acknowledge");
        let mut desk = filters
            .reader(stream, TOPIC)
            .group_id(group_id)
            .local_guard(true)
            .start(FilteredStart::First)
            .build()
            .await?;
        let handled = read_matches(&mut desk, expected).await;
        desk.close().await?;
        println!(
            "  the desk handled {} safe-mode or decommission events",
            handled?.len()
        );

        phase("A/B: a second revision runs in its own group");
        let second = filters
            .revise(
                strict.filter_id,
                strict.revision,
                ConsumerFilter::json(safe_mode_transition()),
            )
            .await?;
        let variant_group = FilterGroupRef {
            stream: stream.to_owned(),
            topic: TOPIC.to_owned(),
            group: format!("{GROUP}-transitions"),
        };
        let variant_binding = filters
            .create_consumer_group(variant_group, second.filter_id, second.revision)
            .await?;
        let variant_id = variant_binding.identity.group_id;
        bindings.push(variant_binding);
        let mut variant = filters
            .reader(stream, TOPIC)
            .group_id(variant_id)
            .count(1)
            .local_guard(true)
            .start(FilteredStart::First)
            .build()
            .await?;
        let compared = async {
            let first = tokio::time::timeout(READ_TIMEOUT, variant.next_record())
                .await
                .map_err(|_| LaserError::Timeout("the A/B record"))??;
            let change: FleetChange = first.json()?;
            println!("  revision {}: {}", second.revision, change.describe());
            filters
                .set_revision_enabled(second.filter_id, second.revision, false)
                .await?;
            variant.ack(&first).await?;
            match variant.try_next_page().await {
                Err(error)
                    if error.filter_reason() == Some(FilterErrorReason::RevisionDisabled) =>
                {
                    println!("  paused: new reads stop, in-flight work can still be acknowledged")
                }
                Err(error) => return Err(error),
                Ok(_) => {
                    return Err(LaserError::Invalid(
                        "a disabled revision kept reading".to_owned(),
                    ));
                }
            }
            filters
                .set_revision_enabled(second.filter_id, second.revision, true)
                .await?;
            read_matches(&mut variant, 1).await?;
            println!(
                "  A/B groups handled {expected} broad events and 2 transitions independently"
            );
            Ok(())
        }
        .await;
        let closed = variant.close().await;
        compared.and(closed)?;

        phase("a bound filter cannot be deleted");
        match filters.delete(strict.filter_id).await {
            Err(error) if error.filter_reason() == Some(FilterErrorReason::Conflict) => {
                println!("  refused with conflict while {GROUP} is bound");
                Ok(())
            }
            Err(error) => Err(error),
            Ok(()) => Err(LaserError::Invalid("a bound filter was deleted".to_owned())),
        }
    }
    .await;

    phase("unbind, archive, delete");
    let mut cleanup = Ok(());
    for binding in bindings {
        cleanup = cleanup.and(filters.unbind_binding(binding).await.map(|_| ()));
    }
    for filter_id in saved {
        cleanup = cleanup.and(filters.archive(filter_id).await);
        cleanup = cleanup.and(filters.delete(filter_id).await);
    }
    outcome.and(cleanup)?;
    println!("  both filters are gone, their names are never reused");
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

// Values only: any update of a satellite whose current mode is safe.
fn safe_mode_values_filter() -> ConsumerFilter {
    ConsumerFilter::json(FilterExpr::all([
        FilterExpr::pred("table", CmpOp::Eq, "satellites"),
        FilterExpr::pred("op", CmpOp::Eq, "u"),
        FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
    ]))
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
