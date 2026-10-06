use crate::capabilities::HelloOutcome;
use crate::laser::Laser;
use iggy::prelude::Client;
use laser_wire::clients::{ProducerLatency, ProducerPresence, ProducerStatistics};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CAPACITY: usize = 32;
const SAMPLES: usize = 64;

#[derive(Default)]
pub(crate) struct ProducerRegistry {
    state: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    records: Vec<Weak<Mutex<Record>>>,
    task: Option<tokio::task::JoinHandle<()>>,
    closed: bool,
}

impl Drop for RegistryState {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct Record {
    statistics: ProducerStatistics,
    latencies: [u64; SAMPLES],
    samples: u64,
}

#[derive(Clone)]
pub(crate) struct ProducerRecorder(Arc<Mutex<Record>>);

pub(crate) struct ProducerObservation {
    recorder: ProducerRecorder,
    started: Instant,
    records: u64,
    bytes: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl ProducerRecorder {
    pub(crate) fn new(laser: &Laser, stream: String, topic: String, confirmed: bool) -> Self {
        let timestamp = now();
        let recorder = Self(Arc::new(Mutex::new(Record {
            statistics: ProducerStatistics {
                instance_id: ulid::Ulid::generate().to_string(),
                stream,
                topic,
                first_activity_millis: timestamp,
                last_activity_millis: timestamp,
                submitted_records: 0,
                submitted_payload_bytes: 0,
                confirmed_records: confirmed.then_some(0),
                confirmed_payload_bytes: confirmed.then_some(0),
                retries: None,
                failed_calls: 0,
                last_success_millis: None,
                latency: ProducerLatency::default(),
            },
            latencies: [0; SAMPLES],
            samples: 0,
        })));
        if laser.current_capabilities().hello != HelloOutcome::Answered {
            return recorder;
        }
        let Some(connection) = laser.connection_string() else {
            return recorder;
        };
        let interval = std::env::var("LASER_PRODUCER_TELEMETRY_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_000);
        if interval == 0 {
            return recorder;
        }
        let interval = interval.clamp(1_000, 60_000);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return recorder;
        };
        let registry = laser.producer_statistics_registry();
        let mut state = registry.state.lock().unwrap_or_else(|p| p.into_inner());
        state.records.retain(|record| record.strong_count() > 0);
        let capacity = std::env::var("LASER_PRODUCER_TELEMETRY_MAX_PRODUCERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(CAPACITY)
            .min(CAPACITY);
        if state.closed || state.records.len() >= capacity {
            return recorder;
        }
        state.records.push(Arc::downgrade(&recorder.0));
        if state.task.as_ref().is_none_or(|task| task.is_finished()) {
            let connection = connection.to_owned();
            let shared = Arc::downgrade(registry);
            state.task = Some(runtime.spawn(async move {
                let mut observer = Observer(None);
                loop {
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                    let Some(records) = shared.upgrade() else {
                        break;
                    };
                    let producers = records.snapshot();
                    drop(records);
                    if producers.is_empty() {
                        if let Some(client) = observer.0.take() {
                            let _ = client.client().shutdown().await;
                        }
                        continue;
                    }
                    if observer.0.is_none() {
                        observer.0 = match tokio::time::timeout(
                            Duration::from_secs(3),
                            Laser::connect(&connection),
                        )
                        .await
                        {
                            Ok(Ok(client))
                                if client.current_capabilities().hello
                                    == HelloOutcome::Answered =>
                            {
                                Some(client)
                            }
                            _ => None,
                        };
                    }
                    let Some(client) = &observer.0 else {
                        continue;
                    };
                    let payload = ProducerPresence {
                        producer_presence_version: 1,
                        observed_at_millis: now(),
                        expires_after_millis: interval * 3,
                        producers,
                    };
                    let Ok(bytes) = laser_wire::framing::encode_named(&payload) else {
                        continue;
                    };
                    if bytes.len() > laser_wire::limits::MAX_CLIENT_METADATA {
                        continue;
                    }
                    let sent = tokio::time::timeout(
                        Duration::from_secs(3),
                        client.send_raw_with_response(
                            laser_wire::codes::AGDX_SET_CLIENT_METADATA_CODE,
                            bytes,
                        ),
                    )
                    .await;
                    if !matches!(sent, Ok(Ok(_)))
                        && let Some(client) = observer.0.take()
                    {
                        let _ = client.client().shutdown().await;
                    }
                }
                if let Some(client) = observer.0.take() {
                    let _ = client.client().shutdown().await;
                }
            }));
        }
        recorder
    }

    pub(crate) fn begin(&self, records: u64, bytes: u64) -> ProducerObservation {
        let mut record = self.0.lock().unwrap_or_else(|p| p.into_inner());
        record.statistics.submitted_records =
            record.statistics.submitted_records.saturating_add(records);
        record.statistics.submitted_payload_bytes = record
            .statistics
            .submitted_payload_bytes
            .saturating_add(bytes);
        record.statistics.last_activity_millis = now();
        ProducerObservation {
            recorder: self.clone(),
            started: Instant::now(),
            records,
            bytes,
        }
    }

    pub(crate) fn snapshot(&self) -> ProducerStatistics {
        let record = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut statistics = record.statistics.clone();
        let percentile = |numerator: u64| {
            if record.samples == 0 {
                return None;
            }
            let rank = (u128::from(record.samples) * u128::from(numerator)).div_ceil(1000);
            let mut cumulative = 0u128;
            for (index, count) in record.latencies.iter().enumerate() {
                cumulative += u128::from(*count);
                if cumulative >= rank {
                    return Some(1u64 << index);
                }
            }
            None
        };
        statistics.latency = ProducerLatency {
            samples: record.samples,
            p50_micros: percentile(500),
            p99_micros: (record.samples >= 100).then(|| percentile(990)).flatten(),
            p999_micros: (record.samples >= 1000).then(|| percentile(999)).flatten(),
        };
        statistics
    }
}

impl ProducerObservation {
    pub(crate) fn finish(self, success: bool) {
        let mut record = self.recorder.0.lock().unwrap_or_else(|p| p.into_inner());
        let statistics = &mut record.statistics;
        statistics.last_activity_millis = now();
        if success {
            statistics.last_success_millis = Some(statistics.last_activity_millis);
            if let Some(count) = &mut statistics.confirmed_records {
                *count = count.saturating_add(self.records);
            }
            if let Some(bytes) = &mut statistics.confirmed_payload_bytes {
                *bytes = bytes.saturating_add(self.bytes);
            }
        } else {
            statistics.failed_calls = statistics.failed_calls.saturating_add(1);
        }
        let micros = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let bucket = (64 - micros.saturating_sub(1).leading_zeros() as usize).min(SAMPLES - 1);
        record.latencies[bucket] = record.latencies[bucket].saturating_add(1);
        record.samples = record.samples.saturating_add(1);
    }
}

struct Observer(Option<Laser>);

impl Drop for Observer {
    fn drop(&mut self) {
        if let Some(client) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = client.client().shutdown().await;
            });
        }
    }
}

impl ProducerRegistry {
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.records.clear();
        if let Some(task) = state.task.take() {
            task.abort();
        }
    }
    fn snapshot(&self) -> Vec<ProducerStatistics> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.records.retain(|record| record.strong_count() > 0);
        state
            .records
            .iter()
            .filter_map(Weak::upgrade)
            .map(ProducerRecorder)
            .map(|record| record.snapshot())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorder(confirmed: bool) -> ProducerRecorder {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        ProducerRecorder::new(&laser, "stream".to_owned(), "topic".to_owned(), confirmed)
    }

    #[test]
    fn given_success_and_failure_when_recorded_then_logical_counts_do_not_count_attempts() {
        let recorder = recorder(true);
        recorder.begin(3, 12).finish(true);
        recorder.begin(2, 8).finish(false);
        let statistics = recorder.snapshot();
        assert_eq!(statistics.submitted_records, 5);
        assert_eq!(statistics.submitted_payload_bytes, 20);
        assert_eq!(statistics.confirmed_records, Some(3));
        assert_eq!(statistics.confirmed_payload_bytes, Some(12));
        assert_eq!(statistics.failed_calls, 1);
        assert_eq!(statistics.retries, None);
        assert_eq!(statistics.latency.samples, 2);
        assert_eq!(statistics.latency.p99_micros, None);
        assert_eq!(statistics.latency.p999_micros, None);
    }

    #[test]
    fn given_buffered_mode_when_successful_then_confirmation_is_unavailable() {
        let recorder = recorder(false);
        recorder.begin(1, 4).finish(true);
        assert_eq!(recorder.snapshot().confirmed_records, None);
    }

    #[test]
    fn given_clones_and_many_calls_when_recorded_then_identity_and_bounded_histogram_are_shared() {
        let recorder = recorder(true);
        let cloned = recorder.clone();
        for _ in 0..1200 {
            cloned.begin(1, 2).finish(true);
        }
        let statistics = recorder.snapshot();
        assert_eq!(statistics.instance_id, cloned.snapshot().instance_id);
        assert_eq!(statistics.latency.samples, 1200);
        assert!(statistics.latency.p999_micros.is_some());
        assert_eq!(recorder.0.lock().unwrap().latencies.len(), 64);
        let encoded = laser_wire::framing::encode_named(&ProducerPresence {
            producer_presence_version: 1,
            observed_at_millis: 10,
            expires_after_millis: 30_000,
            producers: vec![statistics.clone()],
        })
        .unwrap();
        let decoded: ProducerPresence = laser_wire::framing::decode_named(&encoded).unwrap();
        assert_eq!(decoded.producers, vec![statistics]);
    }
}
