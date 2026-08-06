use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::observer::ObserverCost;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RunReport {
    pub schema_version: u32,
    pub run_id: String,
    pub suite_digest: String,
    pub scenario: String,
    pub arm: String,
    pub repetition: u32,
    pub seed: u64,
    pub language: BenchmarkLanguage,
    pub source: SourceIdentity,
    pub artifacts: Vec<ArtifactIdentity>,
    pub environment: EnvironmentReport,
    pub workload: WorkloadReport,
    pub outcomes: OutcomeCounts,
    pub histograms: Vec<HistogramRef>,
    pub deterministic_gates: Vec<DeterministicGateEvidence>,
    pub observer_cost: Option<ObserverCost>,
    pub analysis: AnalysisStatus,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BenchmarkLanguage {
    Rust,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SourceIdentity {
    pub sdk_revision: String,
    pub benchmark_revision: String,
    pub dirty: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ArtifactIdentity {
    pub name: String,
    pub version: String,
    pub source: String,
    pub cpu_target: String,
    pub sha256: String,
    pub minisign_verified: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EnvironmentReport {
    pub tier: String,
    pub durability_profile: String,
    pub cache_state: String,
    pub kernel: String,
    pub architecture: String,
    #[serde(default)]
    pub runtime_worker_threads: usize,
}

/// The number of Tokio workers actually serving this benchmark process.
#[must_use]
pub fn runtime_worker_threads() -> usize {
    tokio::runtime::Handle::try_current()
        .map(|handle| handle.metrics().num_workers())
        .unwrap_or_default()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WorkloadReport {
    pub logical_unit: String,
    pub payload_bytes: usize,
    pub batch_size: usize,
    pub partitions: u32,
    pub offered_rate: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OutcomeCounts {
    pub offered: u64,
    pub dispatched: u64,
    pub completed: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub missed: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub ordering_violations: u64,
    pub checksum_failures: u64,
    #[serde(default)]
    pub late_arrivals: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct HistogramRef {
    pub class: String,
    pub path: String,
    pub sha256: String,
    pub samples: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DeterministicGateEvidence {
    pub name: String,
    pub command: Vec<String>,
    pub passed: bool,
    pub observations: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AnalysisStatus {
    pub valid: bool,
    pub publishable: bool,
    pub invalidation_reason: Option<String>,
}
