use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use laser_sdk::agent::{
    Agent, AgentCtx, AgentHandle, AgentHandler, AgentMessage, CapabilitySelector, Clock, Contract,
    GatherPolicy, InboxRoute, RoutePolicy, SystemClock,
};
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::AgentId;
use laser_wire::agent::CapabilityDescriptor;
use serde::{Deserialize, Serialize};
use strum::{Display, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{
    AgdxArmEvidence, AgdxArmSummary, AgdxCase, MEASUREMENT_RECORD_OFFSET, durations_histogram,
    measured_arm, record_payload, seeded_payload, validate_case, warmup,
};
use crate::engine::Operation;

const SKILL: &str = "laser-bench-work";

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OrchestrationKind {
    FanOut,
    Scatter,
}

impl OrchestrationKind {
    #[must_use]
    pub fn label(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OrchestrationSummary {
    pub orchestration: AgdxArmSummary,
    pub recipients: usize,
    pub recipient_deliveries: u64,
    pub recipient_entry_samples: u64,
    pub recipient_entry_p50_ns: u64,
    pub recipient_entry_p99_ns: u64,
    pub configuration: serde_json::Value,
}

pub struct OrchestrationEvidence {
    pub orchestration: AgdxArmEvidence,
    pub recipients: usize,
    pub recipient_deliveries: u64,
    pub recipient_entry: Histogram<u64>,
    pub configuration: serde_json::Value,
}

impl OrchestrationEvidence {
    #[must_use]
    pub fn summary(&self) -> OrchestrationSummary {
        OrchestrationSummary {
            orchestration: self.orchestration.summary.clone(),
            recipients: self.recipients,
            recipient_deliveries: self.recipient_deliveries,
            recipient_entry_samples: self.recipient_entry.len(),
            recipient_entry_p50_ns: self.recipient_entry.value_at_quantile(0.5),
            recipient_entry_p99_ns: self.recipient_entry.value_at_quantile(0.99),
            configuration: self.configuration.clone(),
        }
    }
}

#[derive(Default)]
struct DeliveryLedger {
    started: tokio::sync::Mutex<HashMap<u64, Instant>>,
    completions: tokio::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>>,
    deliveries: tokio::sync::Mutex<HashMap<u64, HashMap<String, u64>>>,
    entry: tokio::sync::Mutex<Vec<Duration>>,
    checksum_failures: AtomicU64,
}

impl DeliveryLedger {
    async fn register(
        &self,
        id: u64,
        completion: bool,
    ) -> Option<tokio::sync::oneshot::Receiver<()>> {
        self.started.lock().await.insert(id, Instant::now());
        if !completion {
            return None;
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.completions.lock().await.insert(id, sender);
        Some(receiver)
    }

    async fn record(&self, id: u64, worker: &str, valid_body: bool) {
        if !valid_body {
            self.checksum_failures.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(started) = self.started.lock().await.get(&id).copied() {
            self.entry.lock().await.push(started.elapsed());
        }
        let mut deliveries = self.deliveries.lock().await;
        let workers = deliveries.entry(id).or_default();
        *workers.entry(worker.to_owned()).or_default() += 1;
    }

    async fn complete(&self, id: u64) {
        self.started.lock().await.remove(&id);
        if let Some(completion) = self.completions.lock().await.remove(&id) {
            let _ = completion.send(());
        }
    }

    async fn cancel(&self, id: u64) {
        self.started.lock().await.remove(&id);
        self.completions.lock().await.remove(&id);
    }

    async fn reset(&self) {
        self.started.lock().await.clear();
        self.completions.lock().await.clear();
        self.deliveries.lock().await.clear();
        self.entry.lock().await.clear();
        self.checksum_failures.store(0, Ordering::Relaxed);
    }
}

struct WorkerHandler {
    id: String,
    payload: Bytes,
    ledger: Arc<DeliveryLedger>,
}

impl AgentHandler for WorkerHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let body = message.body();
        let id = body_id(body)?;
        let expected = record_payload(&self.payload, id).map_err(LaserError::Handler)?;
        self.ledger.record(id, &self.id, body == expected).await;
        ctx.respond(body.to_vec()).await
    }
}

struct FanOutHandler {
    recipients: usize,
    ledger: Arc<DeliveryLedger>,
    timeout: Duration,
}

impl AgentHandler for FanOutHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let body = message.body();
        let id = body_id(body)?;
        let gather = ctx
            .fan_out(
                CapabilitySelector::new(SKILL, RoutePolicy::Any),
                body.to_vec(),
                GatherPolicy::RequireAll,
                self.timeout,
            )
            .await?;
        if !gather.failures.is_empty() || gather.ok.len() != self.recipients {
            return Err(LaserError::Handler(format!(
                "fan-out completed {} of {} recipients with {} failures",
                gather.ok.len(),
                self.recipients,
                gather.failures.len()
            )));
        }
        let unique = gather
            .ok
            .iter()
            .map(|(agent, reply)| {
                if reply.body() != body {
                    return Err(LaserError::Handler(
                        "fan-out reply body did not match the request".to_owned(),
                    ));
                }
                Ok(agent.as_str())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if unique.len() != self.recipients {
            return Err(LaserError::Handler(
                "fan-out returned duplicate recipients".to_owned(),
            ));
        }
        self.ledger.complete(id).await;
        Ok(())
    }
}

/// Run fan-out or scatter through real agents and capability resolution.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, agent failure, or delivery-accounting failure.
pub async fn run_orchestration_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &AgdxCase,
    recipients: usize,
    kind: OrchestrationKind,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<OrchestrationEvidence, BenchError> {
    validate_case(case)?;
    validate_recipients(recipients)?;
    let scoped = prepare_stream(laser, case, kind, seed).await?;
    let ledger = Arc::new(DeliveryLedger::default());
    let payload = seeded_payload(case.payload_bytes, seed);
    let workers = start_workers(
        connection_string,
        &scoped,
        recipients,
        &payload,
        Arc::clone(&ledger),
    )
    .await?;
    wait_for_recipients(&scoped, recipients).await?;
    let timeout = Duration::from_millis(case.timeout_millis);
    let mut orchestrator = if kind == OrchestrationKind::FanOut {
        Some(start_orchestrator(&scoped, recipients, Arc::clone(&ledger), timeout).await?)
    } else {
        None
    };
    let run = async {
        let warmup_operation = operation(
            scoped.clone(),
            payload.clone(),
            Arc::clone(&ledger),
            recipients,
            kind,
            seed,
            0,
            timeout,
        )?;
        warmup(case, timeout, warmup_operation).await?;
        ledger.reset().await;
        let measured_operation = operation(
            scoped,
            payload,
            Arc::clone(&ledger),
            recipients,
            kind,
            seed,
            MEASUREMENT_RECORD_OFFSET,
            timeout,
        )?;
        let mut orchestration = measured_arm(
            kind.label(),
            1,
            case,
            timeout,
            measured_operation,
            monitored_processes,
        )
        .await?;
        validate_deliveries(recipients, &ledger, &mut orchestration).await;
        let recipient_deliveries = orchestration
            .load
            .outcomes
            .successful
            .saturating_mul(u64::try_from(recipients).unwrap_or(u64::MAX));
        let recipient_entry = durations_histogram(&ledger.entry.lock().await)?;
        Ok::<_, BenchError>((orchestration, recipient_deliveries, recipient_entry))
    }
    .await;
    let orchestrator_shutdown = match orchestrator.take() {
        Some(handle) => handle.shutdown().await.map_err(|error| sdk_error(&error)),
        None => Ok(()),
    };
    let workers_shutdown = shutdown_workers(workers).await;
    let (orchestration, recipient_deliveries, recipient_entry) = run?;
    orchestrator_shutdown?;
    workers_shutdown?;
    Ok(OrchestrationEvidence {
        orchestration,
        recipients,
        recipient_deliveries,
        recipient_entry,
        configuration: serde_json::json!({
            "kind": kind.label(),
            "routing": "all-capable-fixed-inbox",
            "gather_policy": "require-all",
            "handler": "deterministic-echo",
            "recipient_count": recipients,
            "latency_clock": "client-monotonic",
        }),
    })
}

fn validate_recipients(recipients: usize) -> Result<(), BenchError> {
    if recipients == 0 {
        return Err(BenchError::Invalid(
            "fan-out and scatter require at least one recipient".to_owned(),
        ));
    }
    Ok(())
}

async fn prepare_stream(
    laser: &Laser,
    case: &AgdxCase,
    kind: OrchestrationKind,
    seed: u64,
) -> Result<Laser, BenchError> {
    let stream = format!("bench-{}-{seed:016x}", kind.label());
    for topic in [
        AgentTopic::Commands,
        AgentTopic::Responses,
        AgentTopic::Registry,
        AgentTopic::ToolCalls,
    ] {
        laser
            .stream(&stream)
            .topic(topic.topic_string())
            .ensure(case.partitions)
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    Ok(laser.with_default_stream(&stream))
}

async fn wait_for_recipients(laser: &Laser, recipients: usize) -> Result<(), BenchError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let now = SystemClock.now_micros();
        let mut registry = laser.agent_registry().map_err(|error| sdk_error(&error))?;
        registry
            .refresh(now)
            .await
            .map_err(|error| sdk_error(&error))?;
        if registry.resolve(SKILL, now).len() == recipients {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "capability registry did not expose {recipients} recipients before warmup"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_workers(
    connection_string: &str,
    laser: &Laser,
    recipients: usize,
    payload: &Bytes,
    ledger: Arc<DeliveryLedger>,
) -> Result<Vec<AgentHandle>, BenchError> {
    let mut workers = Vec::with_capacity(recipients);
    let stream = laser
        .default_stream()
        .ok_or_else(|| BenchError::Invalid("orchestration worker stream is not set".to_owned()))?
        .to_owned();
    let ops_stream = laser.ops_stream();
    for index in 0..recipients {
        let id = format!("laser-bench-worker-{index:04}");
        let agent: AgentId = id
            .parse()
            .map_err(|error| BenchError::Invalid(format!("invalid worker id: {error}")))?;
        let worker_laser = Laser::connect(connection_string)
            .await
            .map_err(|error| BenchError::Invalid(format!("worker connection failed: {error}")))?
            .with_default_stream(stream.clone())
            .with_ops_stream(ops_stream.clone());
        let mut handle = Agent::builder()
            .id(agent)
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .capabilities(vec![CapabilityDescriptor {
                skill_id: SKILL.to_owned(),
                ..CapabilityDescriptor::default()
            }])
            .handler(WorkerHandler {
                id,
                payload: payload.clone(),
                ledger: Arc::clone(&ledger),
            })
            .poll_interval(Duration::ZERO)
            .build()
            .spawn(worker_laser);
        handle.ready().await.map_err(|error| sdk_error(&error))?;
        workers.push(handle);
    }
    Ok(workers)
}

async fn start_orchestrator(
    laser: &Laser,
    recipients: usize,
    ledger: Arc<DeliveryLedger>,
    timeout: Duration,
) -> Result<AgentHandle, BenchError> {
    let id = "laser-bench-orchestrator"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid orchestrator id: {error}")))?;
    let mut handle = Agent::builder()
        .id(id)
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .handler(FanOutHandler {
            recipients,
            ledger,
            timeout,
        })
        .poll_interval(Duration::ZERO)
        .build()
        .spawn(laser.clone());
    handle.ready().await.map_err(|error| sdk_error(&error))?;
    Ok(handle)
}

#[allow(clippy::too_many_arguments)]
fn operation(
    laser: Laser,
    payload: Bytes,
    ledger: Arc<DeliveryLedger>,
    recipients: usize,
    kind: OrchestrationKind,
    seed: u64,
    id_offset: u64,
    timeout: Duration,
) -> Result<Operation, BenchError> {
    let source: AgentId = "laser-bench-client".parse().map_err(|error| {
        BenchError::Invalid(format!("invalid orchestration source id: {error}"))
    })?;
    Ok(Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let ledger = Arc::clone(&ledger);
        let source = source.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "orchestration ID exceeds u64".to_owned())?;
            let body = record_payload(&payload, id)?;
            match kind {
                OrchestrationKind::FanOut => {
                    let completion = ledger
                        .register(id, true)
                        .await
                        .ok_or_else(|| "fan-out completion was not registered".to_owned())?;
                    let mut provenance = Provenance::builder()
                        .conversation_id(laser_sdk::types::ConversationId::derive(&format!(
                            "laser-bench-{seed}-{id}"
                        )))
                        .build();
                    provenance.agent = Some(source.clone());
                    if let Err(error) = laser
                        .send_agent(AgentTopic::ToolCalls, body, &provenance)
                        .await
                    {
                        ledger.cancel(id).await;
                        return Err(error.to_string());
                    }
                    completion
                        .await
                        .map_err(|_| "fan-out orchestrator stopped".to_owned())
                }
                OrchestrationKind::Scatter => {
                    ledger.register(id, false).await;
                    let report = laser
                        .scatter_report(
                            source,
                            &CapabilitySelector::new(SKILL, RoutePolicy::Any),
                            &body,
                            &InboxRoute::Fixed(AgentTopic::Commands),
                            timeout,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    let mut completed = 0_usize;
                    let mut failed = 0_usize;
                    let mut timed_out = 0_usize;
                    let mut not_consumed = 0_usize;
                    let mut errored = 0_usize;
                    for outcome in &report.outcomes {
                        match &outcome.result {
                            Ok(Contract::Completed(_)) => completed += 1,
                            Ok(Contract::Failed(_)) => failed += 1,
                            Ok(Contract::TimedOut) => timed_out += 1,
                            Ok(Contract::NotConsumed) => not_consumed += 1,
                            Err(_) => errored += 1,
                        }
                    }
                    ledger.complete(id).await;
                    if completed != recipients {
                        return Err(format!(
                            "scatter completed {completed} of {recipients} recipients, failed={failed}, timed_out={timed_out}, not_consumed={not_consumed}, errored={errored}"
                        ));
                    }
                    Ok(())
                }
            }
        })
    }))
}

async fn validate_deliveries(
    recipients: usize,
    ledger: &DeliveryLedger,
    evidence: &mut AgdxArmEvidence,
) {
    let expected_ids = evidence
        .load
        .successful_sequences
        .iter()
        .filter_map(|sequence| MEASUREMENT_RECORD_OFFSET.checked_add(*sequence))
        .collect::<BTreeSet<_>>();
    let explained_ids = evidence
        .load
        .samples
        .iter()
        .filter_map(|sample| MEASUREMENT_RECORD_OFFSET.checked_add(sample.sequence))
        .collect::<BTreeSet<_>>();
    let deliveries = ledger.deliveries.lock().await;
    let mut gaps = 0_u64;
    let mut duplicates = 0_u64;
    for id in &expected_ids {
        let observed = deliveries.get(id);
        let unique = observed.map_or(0, HashMap::len);
        gaps = gaps
            .saturating_add(u64::try_from(recipients.saturating_sub(unique)).unwrap_or(u64::MAX));
        duplicates = duplicates.saturating_add(
            observed
                .into_iter()
                .flat_map(HashMap::values)
                .map(|count| count.saturating_sub(1))
                .sum::<u64>(),
        );
    }
    let mut late_arrivals = 0_u64;
    for (id, observed) in deliveries.iter() {
        if explained_ids.contains(id) {
            late_arrivals =
                late_arrivals.saturating_add(u64::try_from(observed.len()).unwrap_or(u64::MAX));
        } else if !expected_ids.contains(id) {
            gaps = gaps.saturating_add(u64::try_from(observed.len()).unwrap_or(u64::MAX));
        }
    }
    evidence.summary.outcomes.late_arrivals = evidence
        .summary
        .outcomes
        .late_arrivals
        .saturating_add(late_arrivals);
    evidence.summary.outcomes.gaps = evidence.summary.outcomes.gaps.saturating_add(gaps);
    evidence.summary.outcomes.duplicates = evidence
        .summary
        .outcomes
        .duplicates
        .saturating_add(duplicates);
    evidence.summary.outcomes.checksum_failures = evidence
        .summary
        .outcomes
        .checksum_failures
        .saturating_add(ledger.checksum_failures.load(Ordering::Relaxed));
}

async fn shutdown_workers(workers: Vec<AgentHandle>) -> Result<(), BenchError> {
    for worker in workers {
        worker.shutdown().await.map_err(|error| sdk_error(&error))?;
    }
    Ok(())
}

fn body_id(body: &[u8]) -> Result<u64, LaserError> {
    let bytes = body
        .get(..size_of::<u64>())
        .ok_or_else(|| LaserError::Handler("orchestration body has no record ID".to_owned()))?;
    let bytes: [u8; size_of::<u64>()] = bytes
        .try_into()
        .map_err(|_| LaserError::Handler("orchestration record ID is invalid".to_owned()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn sdk_error(error: &LaserError) -> BenchError {
    BenchError::Invalid(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_kind_when_rendering_label_then_should_use_driver_name() {
        assert_eq!(OrchestrationKind::FanOut.label(), "fan_out");
        assert_eq!(OrchestrationKind::Scatter.label(), "scatter");
    }
}
