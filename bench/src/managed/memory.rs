use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use laser_sdk::laser::Laser;
use laser_sdk::memory::{MemoryHandle, MemoryItem};
use laser_sdk::types::ConversationId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup_count,
};
use crate::BenchError;
use crate::agdx::{record_payload, seeded_payload};
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_memory_arm
)]
pub enum MemoryArm {
    BacklogDrain,
    FoldVisibility,
    FoldedRecall,
    RememberAck,
}

fn invalid_memory_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported memory arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MemorySummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub stream: String,
    pub topic: String,
    pub conversation: String,
    pub items_per_operation: usize,
    pub final_visibility_wait_ns: u64,
    pub observation_resolution_ns: Option<u64>,
    pub configuration: serde_json::Value,
}

pub struct MemoryEvidence {
    pub summary: MemorySummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct MemoryOperationContext {
    memory: Arc<MemoryHandle>,
    conversation: ConversationId,
    payload: Bytes,
    case: ManagedCase,
    arm: MemoryArm,
}

/// Run durable memory operations through the public memory facade.
///
/// # Errors
///
/// Returns an error when setup, managed visibility, folded recall, or load execution fails.
pub async fn run_memory_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: MemoryArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<MemoryEvidence, BenchError> {
    validate_case(case)?;
    validate_memory_case(case, arm)?;
    wait_for_memory(laser, Duration::from_secs(30)).await?;
    let names = MemoryNames::new(scenario, seed);
    let stream = laser.stream(&names.stream);
    stream
        .ensure()
        .await
        .map_err(|error| memory_error("create memory stream", &error))?;
    let scoped = laser.with_default_stream(&names.stream);
    let memory = scoped
        .memory_topic(&names.topic)
        .partitions(case.partitions)
        .no_expiry()
        .build()
        .await
        .map_err(|error| memory_error("create memory topic", &error))?;
    let conversation = ConversationId::derive(&format!("{scenario}:{seed}"));
    let payload = seeded_payload(case.payload_bytes, seed);
    let memory = Arc::new(memory);
    if arm == MemoryArm::FoldedRecall {
        let corpus_entries = case.corpus_entries.ok_or_else(|| {
            BenchError::Invalid("folded_recall requires corpus_entries".to_owned())
        })?;
        seed_memory(&memory, conversation, &payload, corpus_entries).await?;
    }
    let context = MemoryOperationContext {
        memory: Arc::clone(&memory),
        conversation,
        payload,
        case: case.clone(),
        arm,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    let warmup_operations = case.warmup_seconds.max(1);
    warmup_count(
        warmup_operations,
        case,
        timeout,
        context.clone().operation(0),
    )
    .await?;
    let before = capture_processes(monitored_processes)?;
    let mut load = run_load(case, timeout, context.clone().operation(warmup_operations)).await?;
    let processes = finish_processes(before, "measurement")?;
    let visibility_started = Instant::now();
    if validates_managed_visibility(arm)
        && await_final_visibility(&context, warmup_operations, &load)
            .await
            .is_err()
    {
        load.outcomes.gaps = load.outcomes.gaps.saturating_add(1);
    }
    let visibility_wait = visibility_started.elapsed();
    let items_per_operation = items_per_operation(arm, case);
    let operation = summarize(arm.into(), &load, case, items_per_operation);
    Ok(MemoryEvidence {
        summary: MemorySummary {
            operation,
            backend_profile: profile,
            stream: names.stream,
            topic: names.topic,
            conversation: conversation.to_string(),
            items_per_operation: usize::try_from(items_per_operation).unwrap_or(usize::MAX),
            final_visibility_wait_ns: duration_ns(visibility_wait),
            observation_resolution_ns: validates_managed_visibility(arm)
                .then_some(duration_ns(VISIBILITY_POLL_INTERVAL)),
            configuration: serde_json::json!({
                "path": "laser_log_memory_through_iggy_and_plane",
                "backend_profile": profile,
                "setup_timed": false,
                "folded_recall_cursor": (arm == MemoryArm::FoldedRecall).then_some("warmed_incremental"),
                "model_calls": false,
            }),
        },
        load,
        processes,
    })
}

#[derive(Clone)]
struct MemoryNames {
    stream: String,
    topic: String,
}

impl MemoryNames {
    fn new(scenario: &str, seed: u64) -> Self {
        let digest = Sha256::digest(scenario.as_bytes());
        let scenario = u64::from_be_bytes(
            digest[..size_of::<u64>()]
                .try_into()
                .expect("SHA-256 prefix has a fixed length"),
        );
        let suffix = format!("{scenario:016x}_{seed:016x}");
        Self {
            stream: format!("bench_memory_stream_{suffix}"),
            topic: format!("bench_memory_topic_{suffix}"),
        }
    }
}

async fn wait_for_memory(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        let capabilities = laser.refresh_capabilities().await;
        if capabilities.managed && capabilities.kv.available {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise managed KV memory within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn seed_memory(
    memory: &MemoryHandle,
    conversation: ConversationId,
    payload: &Bytes,
    entries: u64,
) -> Result<(), BenchError> {
    for id in 0..entries {
        memory
            .remember(record_payload(payload, id).map_err(BenchError::Invalid)?)
            .scope(conversation)
            .send()
            .await
            .map_err(|error| memory_error("seed folded memory", &error))?;
    }
    Ok(())
}

impl MemoryOperationContext {
    fn operation(self, operation_offset: u64) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let operation = operation_offset
                    .checked_add(sequence)
                    .ok_or_else(|| "memory operation ID overflowed".to_owned())?;
                context.execute(operation).await
            })
        })
    }

    async fn execute(&self, operation: u64) -> Result<(), String> {
        match self.arm {
            MemoryArm::FoldedRecall => self.recall(true).await,
            MemoryArm::RememberAck => self.remember(operation).await,
            MemoryArm::FoldVisibility => {
                self.remember(operation).await?;
                self.await_payload(operation, false).await
            }
            MemoryArm::BacklogDrain => {
                let count = items_per_operation(self.arm, &self.case);
                for item in 0..count {
                    let id = operation
                        .checked_mul(count)
                        .and_then(|first| first.checked_add(item))
                        .ok_or_else(|| "memory backlog range overflowed".to_owned())?;
                    self.remember(id).await?;
                }
                self.await_batch(operation, count).await
            }
        }
    }

    async fn remember(&self, id: u64) -> Result<(), String> {
        self.memory
            .remember(record_payload(&self.payload, id)?)
            .scope(self.conversation)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn recall_items(&self, folded: bool) -> Result<Vec<MemoryItem>, String> {
        self.recall_items_with_limit(folded, self.case.batch_size)
            .await
    }

    async fn recall_items_with_limit(
        &self,
        folded: bool,
        limit: usize,
    ) -> Result<Vec<MemoryItem>, String> {
        let recall = self.memory.recall(self.conversation).limit(limit);
        if folded {
            recall.folded().fetch().await
        } else {
            recall.fetch().await
        }
        .map_err(|error| error.to_string())
    }

    async fn recall(&self, folded: bool) -> Result<(), String> {
        let expected = self.case.batch_size;
        let items = self.recall_items(folded).await?;
        if items.len() != expected {
            return Err(format!(
                "memory recall returned {} items, expected {expected}",
                items.len()
            ));
        }
        validate_unique_payloads(&items)
    }

    async fn await_payload(&self, id: u64, folded: bool) -> Result<(), String> {
        let expected = record_payload(&self.payload, id)?;
        let deadline = Instant::now() + Duration::from_millis(self.case.timeout_millis);
        loop {
            if self
                .recall_items(folded)
                .await?
                .iter()
                .any(|item| item.payload == expected)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("memory item visibility timed out".to_owned());
            }
            tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
        }
    }

    async fn await_batch(&self, operation: u64, count: u64) -> Result<(), String> {
        let first = operation
            .checked_mul(count)
            .ok_or_else(|| "memory backlog range overflowed".to_owned())?;
        let expected = (first..first.saturating_add(count))
            .map(|id| record_payload(&self.payload, id))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let deadline = Instant::now() + Duration::from_millis(self.case.timeout_millis);
        loop {
            let actual = self
                .recall_items(false)
                .await?
                .into_iter()
                .map(|item| item.payload)
                .collect::<BTreeSet<_>>();
            if expected.is_subset(&actual) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("memory backlog visibility timed out".to_owned());
            }
            tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
        }
    }
}

async fn await_final_visibility(
    context: &MemoryOperationContext,
    operation_offset: u64,
    load: &LoadResult,
) -> Result<(), String> {
    if load.outcomes.successful == 0 {
        return Ok(());
    }
    if context.arm == MemoryArm::RememberAck {
        return await_range_visible(context, operation_offset, load.outcomes.offered).await;
    }
    let operation = operation_offset.saturating_add(load.outcomes.offered.saturating_sub(1));
    let id = if context.arm == MemoryArm::BacklogDrain {
        operation
            .saturating_add(1)
            .saturating_mul(items_per_operation(context.arm, &context.case))
            .saturating_sub(1)
    } else {
        operation
    };
    context.await_payload(id, false).await
}

async fn await_range_visible(
    context: &MemoryOperationContext,
    first: u64,
    count: u64,
) -> Result<(), String> {
    let expected = (first..first.saturating_add(count))
        .map(|id| record_payload(&context.payload, id))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let limit = usize::try_from(count).map_err(|_| "memory visibility range is too large")?;
    let deadline = Instant::now() + Duration::from_millis(context.case.timeout_millis);
    loop {
        let actual = context
            .recall_items_with_limit(false, limit)
            .await?
            .into_iter()
            .map(|item| item.payload)
            .collect::<BTreeSet<_>>();
        if expected.is_subset(&actual) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("acknowledged memory items did not become visible".to_owned());
        }
        tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
    }
}

fn validate_unique_payloads(items: &[MemoryItem]) -> Result<(), String> {
    let unique = items
        .iter()
        .map(|item| item.payload.as_slice())
        .collect::<BTreeSet<_>>();
    if unique.len() == items.len() {
        Ok(())
    } else {
        Err("memory recall returned duplicate payloads".to_owned())
    }
}

fn items_per_operation(arm: MemoryArm, case: &ManagedCase) -> u64 {
    match arm {
        MemoryArm::BacklogDrain | MemoryArm::FoldedRecall => {
            u64::try_from(case.batch_size).unwrap_or(u64::MAX)
        }
        MemoryArm::FoldVisibility | MemoryArm::RememberAck => 1,
    }
}

fn validates_managed_visibility(arm: MemoryArm) -> bool {
    matches!(
        arm,
        MemoryArm::BacklogDrain | MemoryArm::FoldVisibility | MemoryArm::RememberAck
    )
}

fn validate_memory_case(case: &ManagedCase, arm: MemoryArm) -> Result<(), BenchError> {
    if matches!(arm, MemoryArm::FoldVisibility | MemoryArm::RememberAck) && case.batch_size != 1 {
        return Err(BenchError::Invalid(format!(
            "{arm} requires batch_size = 1"
        )));
    }
    if matches!(arm, MemoryArm::BacklogDrain | MemoryArm::FoldVisibility) && case.concurrency != 1 {
        return Err(BenchError::Invalid(format!(
            "{arm} requires producers = 1 so visibility attribution is unambiguous"
        )));
    }
    if arm == MemoryArm::FoldedRecall
        && case
            .corpus_entries
            .is_none_or(|entries| entries < u64::try_from(case.batch_size).unwrap_or(u64::MAX))
    {
        return Err(BenchError::Invalid(
            "folded_recall requires corpus_entries >= batch_size".to_owned(),
        ));
    }
    Ok(())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn memory_error(operation: &str, error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_memory_arm_names_when_parsed_then_should_use_snake_case() {
        for name in [
            "backlog_drain",
            "fold_visibility",
            "folded_recall",
            "remember_ack",
        ] {
            let arm = name.parse::<MemoryArm>().expect("memory arm should parse");
            let rendered: &'static str = arm.into();
            assert_eq!(rendered, name);
        }
        assert!("remember-ack".parse::<MemoryArm>().is_err());
    }
}
