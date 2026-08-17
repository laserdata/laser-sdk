use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use laser_sdk::laser::Laser;
use laser_sdk::wire::batch::BatchItem;
use laser_sdk::wire::codes::{AGDX_KV_GET_CODE, KV_OP_VERSION};
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::kv::{KvError, KvGet, KvOutcome, KvReply};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup,
};
use crate::BenchError;
use crate::agdx::{record_payload, seeded_payload};
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_batch_arm
)]
pub enum ManagedBatchArm {
    Batched,
    Individual,
    PartialFailure,
}

fn invalid_batch_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported managed batch arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ManagedBatchSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub namespace: String,
    pub commands_per_operation: usize,
    pub configuration: serde_json::Value,
}

pub struct ManagedBatchEvidence {
    pub summary: ManagedBatchSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct BatchOperationContext {
    laser: Laser,
    namespace: String,
    arm: ManagedBatchArm,
    payload: Bytes,
    commands: usize,
}

/// Compare managed batch amortization with individual round trips and verify isolated slot failure.
///
/// # Errors
///
/// Returns an error when managed readiness, setup, request encoding, slot decoding, or load execution fails.
pub async fn run_managed_batch_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: ManagedBatchArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<ManagedBatchEvidence, BenchError> {
    validate_case(case)?;
    validate_batch_case(case, arm)?;
    wait_for_managed(laser, Duration::from_secs(30)).await?;
    let namespace = namespace(scenario, seed);
    let payload = seeded_payload(case.payload_bytes, seed);
    seed_values(laser, &namespace, &payload, case.batch_size).await?;
    let context = BatchOperationContext {
        laser: laser.clone(),
        namespace: namespace.clone(),
        arm,
        payload,
        commands: case.batch_size,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, context.operation()).await?;
    let before = capture_processes(monitored_processes)?;
    let load = run_load(case, timeout, context.operation()).await?;
    let processes = finish_processes(before, "measurement")?;
    let operation = summarize(
        arm.into(),
        &load,
        case,
        u64::try_from(case.batch_size).unwrap_or(u64::MAX),
    );
    Ok(ManagedBatchEvidence {
        summary: ManagedBatchSummary {
            operation,
            backend_profile: profile,
            namespace,
            commands_per_operation: case.batch_size,
            configuration: serde_json::json!({
                "path": "laser_execute_batch_through_iggy_and_plane",
                "backend_profile": profile,
                "setup_timed": false,
                "validation_boundary": "each_reply_slot",
                "transactional": false,
            }),
        },
        load,
        processes,
    })
}

impl BatchOperationContext {
    fn operation(&self) -> Operation {
        let context = self.clone();
        Arc::new(move |_| {
            let context = context.clone();
            Box::pin(async move { context.execute().await })
        })
    }

    async fn execute(&self) -> Result<(), String> {
        match self.arm {
            ManagedBatchArm::Batched => self.execute_batched(false).await,
            ManagedBatchArm::Individual => self.execute_individual().await,
            ManagedBatchArm::PartialFailure => self.execute_batched(true).await,
        }
    }

    async fn execute_individual(&self) -> Result<(), String> {
        let kv = self.laser.kv(&self.namespace);
        for id in 0..self.commands {
            let value = kv
                .get(key(id))
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "individual managed read missed a seeded key".to_owned())?;
            validate_value(&self.payload, id, &value)?;
        }
        Ok(())
    }

    async fn execute_batched(&self, inject_failure: bool) -> Result<(), String> {
        let failed_slot = inject_failure.then_some(self.commands / 2);
        let mut items = Vec::with_capacity(self.commands);
        for id in 0..self.commands {
            if failed_slot == Some(id) {
                let request = KvGet {
                    v: KV_OP_VERSION.saturating_add(1),
                    namespace: self.namespace.clone(),
                    key: key(id),
                    if_none_match: None,
                    min_position: None,
                };
                items.push(BatchItem {
                    code: AGDX_KV_GET_CODE,
                    payload: encode_named(&request).map_err(|error| error.to_string())?,
                });
                continue;
            }
            let request = KvGet {
                v: KV_OP_VERSION,
                namespace: self.namespace.clone(),
                key: key(id),
                if_none_match: None,
                min_position: None,
            };
            items.push(BatchItem {
                code: AGDX_KV_GET_CODE,
                payload: encode_named(&request).map_err(|error| error.to_string())?,
            });
        }
        let slots = self
            .laser
            .execute_batch(items)
            .await
            .map_err(|error| error.to_string())?;
        if slots.len() != self.commands {
            return Err("managed batch reply length did not match the request".to_owned());
        }
        for (id, slot) in slots.iter().enumerate() {
            if failed_slot == Some(id) {
                let reply: KvReply = decode_named(slot).map_err(|error| error.to_string())?;
                if !matches!(
                    reply,
                    KvReply::Err(KvError::Version {
                        expected: KV_OP_VERSION,
                        got,
                    }) if got == KV_OP_VERSION.saturating_add(1)
                ) {
                    return Err("managed batch failure slot had the wrong typed error".to_owned());
                }
                continue;
            }
            let reply: KvReply = decode_named(slot).map_err(|error| error.to_string())?;
            let KvReply::Ok(KvOutcome::Value(Some(entry))) = reply else {
                return Err("managed batch read slot did not contain a value".to_owned());
            };
            validate_value(&self.payload, id, &entry.value)?;
        }
        Ok(())
    }
}

async fn wait_for_managed(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        if laser.refresh_capabilities().await.managed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise managed batching within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn seed_values(
    laser: &Laser,
    namespace: &str,
    payload: &Bytes,
    commands: usize,
) -> Result<(), BenchError> {
    let kv = laser.kv(namespace);
    for id in 0..commands {
        kv.set(key(id))
            .bytes(
                record_payload(
                    payload,
                    u64::try_from(id)
                        .map_err(|_| BenchError::Invalid("batch key ID exceeds u64".to_owned()))?,
                )
                .map_err(BenchError::Invalid)?,
            )
            .send()
            .await
            .map_err(|error| BenchError::Invalid(format!("seed managed batch value: {error}")))?;
    }
    Ok(())
}

fn validate_value(payload: &Bytes, id: usize, actual: &[u8]) -> Result<(), String> {
    let id = u64::try_from(id).map_err(|_| "batch key ID exceeds u64".to_owned())?;
    let expected = record_payload(payload, id)?;
    if actual == expected {
        Ok(())
    } else {
        Err("managed batch returned the wrong value".to_owned())
    }
}

fn key(id: usize) -> Vec<u8> {
    format!("batch_{id:08x}").into_bytes()
}

fn namespace(scenario: &str, seed: u64) -> String {
    let digest = Sha256::digest(scenario.as_bytes());
    let scenario = u64::from_be_bytes(
        digest[..size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix has a fixed length"),
    );
    format!("bench_batch_{scenario:016x}_{seed:08x}")
}

fn validate_batch_case(case: &ManagedCase, arm: ManagedBatchArm) -> Result<(), BenchError> {
    if case.batch_size > laser_sdk::wire::limits::MAX_BATCH_OPS {
        return Err(BenchError::Invalid(format!(
            "managed batch size exceeds {}",
            laser_sdk::wire::limits::MAX_BATCH_OPS
        )));
    }
    if arm == ManagedBatchArm::PartialFailure && case.batch_size < 3 {
        return Err(BenchError::Invalid(
            "partial_failure requires at least three commands".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_managed_batch_arm_names_when_parsed_then_should_use_snake_case() {
        for name in ["batched", "individual", "partial_failure"] {
            let arm = name
                .parse::<ManagedBatchArm>()
                .expect("managed batch arm should parse");
            let rendered: &'static str = arm.into();
            assert_eq!(rendered, name);
        }
    }

    #[test]
    fn given_partial_failure_with_two_commands_when_validated_then_should_reject() {
        let case = ManagedCase {
            payload_bytes: 128,
            operations: 1,
            duration_seconds: 1,
            concurrency: 1,
            batch_size: 2,
            partitions: 1,
            corpus_entries: None,
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        };
        assert!(validate_batch_case(&case, ManagedBatchArm::PartialFailure).is_err());
    }
}
