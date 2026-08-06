use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::Stdio;
use std::time::Duration;

use laser_sdk::iggy::prelude::SystemClient as _;
use laser_sdk::laser::Laser;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::BenchError;
use crate::metrics::{CgroupSnapshot, ProcessSnapshot};

const PLANE_METRICS_PATH: &str = "/metrics";
const MAX_HTTP_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TelemetrySeries {
    pub interval_millis: u64,
    pub observer_connection: String,
    pub points: Vec<TelemetryPoint>,
    pub perf: BTreeMap<String, PerfMeasurement>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TelemetryPoint {
    pub elapsed_ns: u64,
    pub processes: BTreeMap<String, ProcessSnapshot>,
    pub cgroups: BTreeMap<String, CgroupSnapshot>,
    pub iggy: serde_json::Value,
    pub plane: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PerfMeasurement {
    pub available: bool,
    pub scope: String,
    pub counters: BTreeMap<String, PerfCounter>,
    pub unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PerfCounter {
    pub value: f64,
    pub unit: String,
}

struct PerfObserver {
    name: String,
    child: Child,
}

pub struct TelemetrySampler {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<TelemetrySeries, BenchError>>,
}

impl TelemetrySampler {
    /// Start a low-frequency observer on a dedicated VSR connection.
    ///
    /// # Errors
    ///
    /// Returns an error when the observer cannot connect or the sampling interval is zero.
    pub async fn start(
        connection_string: &str,
        processes: Vec<(String, u32)>,
        plane_address: Option<SocketAddr>,
        interval: Duration,
        enable_perf: bool,
    ) -> Result<Self, BenchError> {
        if interval.is_zero() {
            return Err(BenchError::Invalid(
                "telemetry interval must be nonzero".to_owned(),
            ));
        }
        let laser = Laser::connect(connection_string).await.map_err(|error| {
            BenchError::Invalid(format!("telemetry VSR connection failed: {error}"))
        })?;
        let (perf_observers, perf) = start_perf_observers(&processes, enable_perf);
        let (stop, stop_rx) = oneshot::channel();
        let task = tokio::spawn(sample_until_stopped(
            laser,
            processes,
            plane_address,
            interval,
            stop_rx,
            perf_observers,
            perf,
        ));
        Ok(Self { stop, task })
    }

    /// Stop sampling after one final observation and return the complete series.
    ///
    /// # Errors
    ///
    /// Returns an error when a scrape failed or the sampling task could not complete.
    pub async fn stop(self) -> Result<TelemetrySeries, BenchError> {
        let _ = self.stop.send(());
        self.task
            .await
            .map_err(|error| BenchError::Invalid(format!("telemetry task failed: {error}")))?
    }
}

async fn sample_until_stopped(
    laser: Laser,
    processes: Vec<(String, u32)>,
    plane_address: Option<SocketAddr>,
    interval: Duration,
    mut stop: oneshot::Receiver<()>,
    perf_observers: Vec<PerfObserver>,
    mut perf: BTreeMap<String, PerfMeasurement>,
) -> Result<TelemetrySeries, BenchError> {
    let started = Instant::now();
    let mut points = vec![sample(&laser, &processes, plane_address, started).await?];
    let mut ticker = interval_at(started + interval, interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut stop => {
                points.push(sample(&laser, &processes, plane_address, started).await?);
                for observer in perf_observers {
                    let (name, measurement) = finish_perf_observer(observer).await?;
                    perf.insert(name, measurement);
                }
                return Ok(TelemetrySeries {
                    interval_millis: duration_millis(interval)?,
                    observer_connection: "dedicated_tcp_vsr".to_owned(),
                    points,
                    perf,
                });
            }
            _ = ticker.tick() => {
                points.push(sample(&laser, &processes, plane_address, started).await?);
            }
        }
    }
}

fn start_perf_observers(
    processes: &[(String, u32)],
    enabled: bool,
) -> (Vec<PerfObserver>, BTreeMap<String, PerfMeasurement>) {
    let mut observers = Vec::new();
    let mut measurements = BTreeMap::new();
    for (name, pid) in processes {
        if !enabled {
            measurements.insert(name.clone(), unavailable_perf("disabled by suite manifest"));
            continue;
        }
        let pid = pid.to_string();
        let child = Command::new("perf")
            .args([
                "stat",
                "--no-big-num",
                "-x",
                ";",
                "-e",
                "task-clock,cycles,instructions,cache-misses,context-switches,cpu-migrations",
                "-p",
                &pid,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn();
        match child {
            Ok(child) => observers.push(PerfObserver {
                name: name.clone(),
                child,
            }),
            Err(error) => {
                measurements.insert(name.clone(), unavailable_perf(&error.to_string()));
            }
        }
    }
    (observers, measurements)
}

async fn finish_perf_observer(
    observer: PerfObserver,
) -> Result<(String, PerfMeasurement), BenchError> {
    if let Some(pid) = observer.child.id() {
        let result = unsafe { libc::kill(i32::try_from(pid).unwrap_or(i32::MAX), libc::SIGINT) };
        if result != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(BenchError::Invalid(format!(
                "failed to stop perf observer for {}: {}",
                observer.name,
                std::io::Error::last_os_error()
            )));
        }
    }
    let output = observer.child.wait_with_output().await.map_err(|error| {
        BenchError::Invalid(format!(
            "perf observer for {} failed: {error}",
            observer.name
        ))
    })?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok((observer.name, parse_perf(&stderr)))
}

fn parse_perf(output: &str) -> PerfMeasurement {
    let mut counters = BTreeMap::new();
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let fields = line.split(';').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 3 || fields[0].starts_with('<') {
            continue;
        }
        if let Ok(value) = fields[0].parse::<f64>() {
            counters.insert(
                fields[2].to_owned(),
                PerfCounter {
                    value,
                    unit: fields[1].to_owned(),
                },
            );
        }
    }
    if counters.is_empty() {
        let reason = output
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("perf returned no counters");
        return unavailable_perf(reason);
    }
    PerfMeasurement {
        available: true,
        scope: "telemetry_sampler_lifetime".to_owned(),
        counters,
        unavailable_reason: None,
    }
}

fn unavailable_perf(reason: &str) -> PerfMeasurement {
    PerfMeasurement {
        available: false,
        scope: "telemetry_sampler_lifetime".to_owned(),
        counters: BTreeMap::new(),
        unavailable_reason: Some(reason.chars().take(240).collect()),
    }
}

async fn sample(
    laser: &Laser,
    processes: &[(String, u32)],
    plane_address: Option<SocketAddr>,
    started: Instant,
) -> Result<TelemetryPoint, BenchError> {
    let mut process_snapshots = BTreeMap::new();
    let mut cgroups = BTreeMap::new();
    for (name, pid) in processes {
        process_snapshots.insert(name.clone(), ProcessSnapshot::capture(*pid)?);
        if let Some(snapshot) = CgroupSnapshot::capture(*pid)? {
            cgroups.insert(name.clone(), snapshot);
        }
    }
    let iggy = laser
        .client()
        .get_stats()
        .await
        .map_err(|error| BenchError::Invalid(format!("Iggy stats scrape failed: {error}")))?;
    let plane = match plane_address {
        Some(address) => Some(fetch_json(address, PLANE_METRICS_PATH).await?),
        None => None,
    };
    Ok(TelemetryPoint {
        elapsed_ns: elapsed_ns(started.elapsed())?,
        processes: process_snapshots,
        cgroups,
        iggy: serde_json::to_value(iggy)?,
        plane,
    })
}

async fn fetch_json(address: SocketAddr, path: &str) -> Result<serde_json::Value, BenchError> {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|error| {
            BenchError::Invalid(format!("plane metrics connection failed: {error}"))
        })?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .map_err(|error| BenchError::Invalid(format!("plane metrics request failed: {error}")))?;
    let mut response = Vec::new();
    stream
        .take(MAX_HTTP_RESPONSE_BYTES)
        .read_to_end(&mut response)
        .await
        .map_err(|error| BenchError::Invalid(format!("plane metrics response failed: {error}")))?;
    parse_json_response(&response)
}

fn parse_json_response(response: &[u8]) -> Result<serde_json::Value, BenchError> {
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| BenchError::Invalid("plane metrics response has no body".to_owned()))?;
    let headers = &response[..separator];
    let status = headers
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    if !(status.starts_with(b"HTTP/1.1 200") || status.starts_with(b"HTTP/1.0 200")) {
        return Err(BenchError::Invalid(format!(
            "plane metrics returned `{}`",
            String::from_utf8_lossy(status).trim()
        )));
    }
    Ok(serde_json::from_slice(&response[separator + 4..])?)
}

fn duration_millis(duration: Duration) -> Result<u64, BenchError> {
    duration
        .as_millis()
        .try_into()
        .map_err(|_| BenchError::Invalid("telemetry interval exceeds u64 milliseconds".to_owned()))
}

fn elapsed_ns(duration: Duration) -> Result<u64, BenchError> {
    duration.as_nanos().try_into().map_err(|_| {
        BenchError::Invalid("telemetry elapsed time exceeds u64 nanoseconds".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_successful_http_response_when_parsed_then_should_return_json_body() {
        let response =
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{\"projector_lag\":0}";
        let parsed = parse_json_response(response).expect("valid response should parse");
        assert_eq!(parsed["projector_lag"], 0);
    }

    #[test]
    fn given_failed_http_response_when_parsed_then_should_reject_status() {
        let response = b"HTTP/1.1 503 Service Unavailable\r\n\r\n{}";
        let error = parse_json_response(response).expect_err("failed status should be rejected");
        assert!(error.to_string().contains("503 Service Unavailable"));
    }

    #[test]
    fn given_perf_delimited_output_when_parsed_then_should_retain_typed_counters() {
        let output = "12.5;msec;task-clock;1;100.00;\n42;;cycles;1;100.00;";
        let measurement = parse_perf(output);

        assert!(measurement.available);
        assert!((measurement.counters["cycles"].value - 42.0).abs() < f64::EPSILON);
        assert_eq!(measurement.counters["task-clock"].unit, "msec");
    }
}
