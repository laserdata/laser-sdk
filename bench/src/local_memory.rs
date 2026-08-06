use std::sync::Arc;
use std::time::Duration;

use laser_sdk::error::LaserError;
use laser_sdk::memory::{Embedder, Memory, MemoryQuery, MemoryScope, VectorMemory};
use laser_sdk::types::ConversationId;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::managed::{
    MEASUREMENT_ID_OFFSET, ManagedArmSummary, ManagedCase, ManagedProcessMeasurement,
    capture_processes, finish_processes, run_load, summarize, validate_case, warmup,
};

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_local_memory_driver
)]
pub enum LocalMemoryDriver {
    VectorMemoryRecall,
    VectorMemoryRemember,
}

fn invalid_local_memory_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported local memory driver `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LocalMemorySummary {
    pub operation: ManagedArmSummary,
    pub corpus_entries: u64,
    pub vector_dimensions: usize,
    pub configuration: serde_json::Value,
}

pub struct LocalMemoryEvidence {
    pub summary: LocalMemorySummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone, Copy)]
struct DeterministicEmbedder {
    dimensions: usize,
}

impl Embedder for DeterministicEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let mut state = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            hash.wrapping_mul(0x100_0000_01b3)
                .wrapping_add(u64::from(byte))
        });
        let mut embedding = Vec::with_capacity(self.dimensions);
        for _ in 0..self.dimensions {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            let component =
                f32::from(u16::from_le_bytes([bytes[0], bytes[1]])) / f32::from(u16::MAX) - 0.5;
            embedding.push(component);
        }
        let norm = embedding
            .iter()
            .map(|component| component * component)
            .sum::<f32>()
            .sqrt();
        if norm > 0.0 {
            for component in &mut embedding {
                *component /= norm;
            }
        }
        Ok(embedding)
    }
}

#[derive(Clone)]
struct LocalMemoryOperation {
    memory: Arc<VectorMemory<DeterministicEmbedder>>,
    scope: MemoryScope,
    driver: LocalMemoryDriver,
    payload_bytes: usize,
    corpus_entries: u64,
    seed: u64,
}

impl LocalMemoryOperation {
    fn operation(self, sequence_offset: u64) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let sequence = sequence.saturating_add(sequence_offset);
                match context.driver {
                    LocalMemoryDriver::VectorMemoryRemember => Memory::remember(
                        context.memory.as_ref(),
                        &context.scope,
                        payload(context.payload_bytes, context.seed, sequence),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
                    LocalMemoryDriver::VectorMemoryRecall => {
                        let expected = payload(
                            context.payload_bytes,
                            context.seed,
                            sequence % context.corpus_entries,
                        );
                        let semantic = String::from_utf8(expected.clone())
                            .map_err(|error| error.to_string())?;
                        let items = Memory::recall(
                            context.memory.as_ref(),
                            &context.scope,
                            &MemoryQuery::builder().limit(1).semantic(semantic).build(),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        if items.first().is_some_and(|item| item.payload == expected) {
                            Ok(())
                        } else {
                            Err("vector recall did not return the exact expected item".to_owned())
                        }
                    }
                }
            })
        })
    }
}

/// Measure in-process vector-memory remember or semantic recall without a model or managed backend.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, process accounting failure, or workload failure.
pub async fn run_local_memory_evidence(
    case: &ManagedCase,
    driver: LocalMemoryDriver,
    vector_dimensions: usize,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<LocalMemoryEvidence, BenchError> {
    validate_case(case)?;
    if vector_dimensions == 0 {
        return Err(BenchError::Invalid(
            "vector_dimensions must be nonzero".to_owned(),
        ));
    }
    let corpus_entries = case.corpus_entries.unwrap_or(1_000);
    if corpus_entries == 0 {
        return Err(BenchError::Invalid(
            "local memory corpus_entries must be nonzero".to_owned(),
        ));
    }
    let memory = Arc::new(VectorMemory::new(DeterministicEmbedder {
        dimensions: vector_dimensions,
    }));
    let scope = MemoryScope::builder()
        .conversation(ConversationId::derive(&format!("{scenario}:{seed}")))
        .build();
    if driver == LocalMemoryDriver::VectorMemoryRecall {
        for sequence in 0..corpus_entries {
            Memory::remember(
                memory.as_ref(),
                &scope,
                payload(case.payload_bytes, seed, sequence),
            )
            .await
            .map_err(|error| BenchError::Invalid(format!("seed vector memory: {error}")))?;
        }
    }
    let operation = LocalMemoryOperation {
        memory: Arc::clone(&memory),
        scope: scope.clone(),
        driver,
        payload_bytes: case.payload_bytes,
        corpus_entries,
        seed,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, operation.clone().operation(0)).await?;
    let before = capture_processes(monitored_processes)?;
    let mut load = run_load(case, timeout, operation.operation(MEASUREMENT_ID_OFFSET)).await?;
    let processes = finish_processes(before, "measurement")?;
    if driver == LocalMemoryDriver::VectorMemoryRemember
        && let Some(sequence) = load.successful_sequences.last().copied()
    {
        let sequence = sequence.saturating_add(MEASUREMENT_ID_OFFSET);
        let expected = payload(case.payload_bytes, seed, sequence);
        let semantic = String::from_utf8(expected.clone()).map_err(|error| {
            BenchError::Invalid(format!("validate vector memory payload: {error}"))
        })?;
        let items = Memory::recall(
            memory.as_ref(),
            &scope,
            &MemoryQuery::builder().limit(1).semantic(semantic).build(),
        )
        .await
        .map_err(|error| BenchError::Invalid(format!("validate vector memory: {error}")))?;
        if items.first().is_none_or(|item| item.payload != expected) {
            load.outcomes.checksum_failures = load.outcomes.checksum_failures.saturating_add(1);
        }
    }
    let operation_summary = summarize(driver.into(), &load, case, 1);
    Ok(LocalMemoryEvidence {
        summary: LocalMemorySummary {
            operation: operation_summary,
            corpus_entries,
            vector_dimensions,
            configuration: serde_json::json!({
                "path": "in_process_vector_memory",
                "model_calls": false,
                "setup_timed": false,
                "embedding": "deterministic_precomputed_cpu",
            }),
        },
        load,
        processes,
    })
}

fn payload(size: usize, seed: u64, sequence: u64) -> Vec<u8> {
    let prefix = format!("vector-memory-{seed:016x}-{sequence:016x}-");
    let mut payload = prefix.into_bytes();
    payload.resize(size.max(payload.len()), b'x');
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_vector_corpus_when_recalled_then_should_return_exact_item() {
        let evidence = run_local_memory_evidence(
            &ManagedCase {
                payload_bytes: 128,
                operations: 8,
                duration_seconds: 1,
                concurrency: 2,
                batch_size: 1,
                partitions: 1,
                corpus_entries: Some(32),
                warmup_seconds: 1,
                timeout_millis: 1_000,
                offered_rate: None,
                spin_dispatch: false,
                max_in_flight: None,
            },
            LocalMemoryDriver::VectorMemoryRecall,
            64,
            "vector-recall-test",
            7,
            &[("client".to_owned(), std::process::id())],
        )
        .await
        .expect("vector recall evidence should complete");

        assert!(evidence.load.outcomes.successful > 0);
        assert_eq!(evidence.load.outcomes.failed, 0);
        assert_eq!(evidence.summary.vector_dimensions, 64);
    }
}
