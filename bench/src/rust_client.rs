use std::collections::BTreeMap;
use std::time::Instant;

use laser_sdk::laser::Laser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::metrics::{ProcessDelta, ProcessSnapshot};
use crate::report::OutcomeCounts;

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_rust_client_driver
)]
pub enum RustClientDriver {
    RustStartup,
}

fn invalid_rust_client_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported Rust client driver `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RustClientPhaseProcess {
    pub phase: String,
    pub name: String,
    pub delta: ProcessDelta,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RustClientStartupSummary {
    pub connect_and_negotiate_ns: u64,
    pub topology_setup_ns: u64,
    pub first_publish_ack_ns: u64,
    pub warmed_publish_ack_ns: u64,
    pub outcomes: OutcomeCounts,
    pub processes: Vec<RustClientPhaseProcess>,
    pub configuration: Value,
}

pub struct RustClientStartupRun<'a> {
    pub connection_string: &'a str,
    pub seed: u64,
    pub payload_bytes: usize,
    pub partitions: u32,
    pub monitored_processes: &'a [(String, u32)],
}

/// Measure cold Rust client setup separately from the first and warmed SDK operations.
///
/// # Errors
///
/// Returns an error when process accounting, connection, topology setup, publish acknowledgement, or replay validation fails.
pub async fn run_rust_client_startup(
    run: RustClientStartupRun<'_>,
) -> Result<RustClientStartupSummary, BenchError> {
    let RustClientStartupRun {
        connection_string,
        seed,
        payload_bytes,
        partitions,
        monitored_processes,
    } = run;
    let mut processes = Vec::new();
    let before = capture_processes(monitored_processes)?;
    let started = Instant::now();
    let laser = Laser::connect(connection_string)
        .await
        .map_err(|error| startup_error("connect and negotiate", &error))?;
    let connect_and_negotiate_ns = elapsed_ns(started);
    finish_processes(before, "connect_and_negotiate", &mut processes)?;

    let stream = format!("bench-rust-startup-{seed:016x}");
    let topic = laser.stream(&stream).topic("records");
    let before = capture_processes(monitored_processes)?;
    let started = Instant::now();
    topic
        .ensure(partitions)
        .await
        .map_err(|error| startup_error("topology setup", &error))?;
    let topology_setup_ns = elapsed_ns(started);
    finish_processes(before, "topology_setup", &mut processes)?;

    let first_payload = payload(payload_bytes, seed, 0);
    let warmed_payload = payload(payload_bytes, seed, 1);
    let before = capture_processes(monitored_processes)?;
    let started = Instant::now();
    topic
        .send(first_payload.clone(), BTreeMap::new(), None)
        .await
        .map_err(|error| startup_error("first publish", &error))?;
    let first_publish_ack_ns = elapsed_ns(started);
    finish_processes(before, "first_publish", &mut processes)?;

    let before = capture_processes(monitored_processes)?;
    let started = Instant::now();
    topic
        .send(warmed_payload.clone(), BTreeMap::new(), None)
        .await
        .map_err(|error| startup_error("warmed publish", &error))?;
    let warmed_publish_ack_ns = elapsed_ns(started);
    finish_processes(before, "warmed_publish", &mut processes)?;

    let mut cursor = topic
        .replay()
        .map_err(|error| startup_error("open replay cursor", &error))?;
    let messages = cursor
        .poll()
        .await
        .map_err(|error| startup_error("validate startup records", &error))?;
    let valid = messages.len() == 2
        && messages[0].payload == first_payload
        && messages[1].payload == warmed_payload;
    let outcomes = OutcomeCounts {
        offered: 2,
        dispatched: 2,
        completed: 2,
        successful: u64::from(valid) * 2,
        failed: u64::from(!valid) * 2,
        ..OutcomeCounts::default()
    };
    if !valid {
        return Err(BenchError::Invalid(
            "Rust startup replay did not return the two acknowledged records in order".to_owned(),
        ));
    }
    Ok(RustClientStartupSummary {
        connect_and_negotiate_ns,
        topology_setup_ns,
        first_publish_ack_ns,
        warmed_publish_ack_ns,
        outcomes,
        processes,
        configuration: json!({
            "client": "rust",
            "runtime": "tokio_multi_thread",
            "connection_boundary": "Laser_connect_including_capability_negotiation",
            "topology_boundary": "stream_and_topic_ensure",
            "first_operation_boundary": "first_Laser_topic_send_to_publish_acknowledgement",
            "warmed_operation_boundary": "second_Laser_topic_send_to_publish_acknowledgement",
            "server_state": "already_running",
            "process_import_time": "not_applicable_to_in_process_rust_client",
        }),
    })
}

fn payload(size: usize, seed: u64, sequence: u64) -> Vec<u8> {
    let mut state = seed ^ sequence.rotate_left(17);
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn capture_processes(
    monitored_processes: &[(String, u32)],
) -> Result<Vec<(String, ProcessSnapshot)>, BenchError> {
    monitored_processes
        .iter()
        .map(|(name, pid)| Ok((name.clone(), ProcessSnapshot::capture(*pid)?)))
        .collect()
}

fn finish_processes(
    before: Vec<(String, ProcessSnapshot)>,
    phase: &str,
    measurements: &mut Vec<RustClientPhaseProcess>,
) -> Result<(), BenchError> {
    for (name, snapshot) in before {
        let later = ProcessSnapshot::capture(snapshot.pid)?;
        measurements.push(RustClientPhaseProcess {
            phase: phase.to_owned(),
            name,
            delta: snapshot.delta(later)?,
        });
    }
    Ok(())
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn startup_error(phase: &str, error: impl std::fmt::Display) -> BenchError {
    BenchError::Invalid(format!("Rust client startup {phase} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_equal_seed_when_generating_startup_payload_then_should_be_deterministic() {
        assert_eq!(payload(64, 7, 1), payload(64, 7, 1));
        assert_ne!(payload(64, 7, 0), payload(64, 7, 1));
    }
}
