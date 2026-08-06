use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant};

use laser_bench::agdx::{
    AgdxCase, AgdxDriver, AgdxPublishEvidence, AgdxPublishSummary, AgdxRequestReplyEvidence,
    AgdxRequestReplySummary, AgdxStreamEvidence, AgdxStreamSummary, run_publish_evidence,
    run_request_reply_evidence, run_stream_evidence,
};
use laser_bench::analysis::{PairedObservation, analyze_paired, evaluate_c2};
use laser_bench::binary::{BinaryManifest, SourceSnapshot, sha256_file};
use laser_bench::bundle::{prepare_publication, verify_publication};
use laser_bench::calibration::{SchedulerCalibration, run_scheduler_calibration};
use laser_bench::context_fetch::{
    ContextFetchEvidence, ContextFetchSummary, ContextPolicyKind, run_context_fetch_evidence,
};
use laser_bench::gate::{sdk_allocation_gate, sdk_zero_copy_gate};
use laser_bench::histogram::{read_sidecar, verify_sidecar_at, write_sidecar};
use laser_bench::host::{HostAudit, HostSnapshot, pin_process};
use laser_bench::iggy::{IggyBenchmarkKind, IggyBenchmarkRun};
use laser_bench::local_memory::{LocalMemoryDriver, LocalMemorySummary, run_local_memory_evidence};
use laser_bench::managed::{
    ForkArm, GraphArm, KvArm, ManagedBatchArm, ManagedCase, ManagedDriver, MemoryArm,
    ProjectionArm, QueryArm, UdsArm, run_fork_evidence, run_graph_evidence, run_kv_evidence,
    run_managed_batch_evidence, run_memory_evidence, run_projection_evidence, run_query_evidence,
    run_uds_evidence,
};
use laser_bench::manifest::{BenchmarkLayer, SuiteManifest};
use laser_bench::mcp::{
    McpBridgeEvidence, McpBridgeSummary, McpDriver, McpGuaranteedEvidence,
    McpGuaranteedRecoverySummary, McpGuaranteedSummary, McpMinimalEvidence, McpMinimalSummary,
    McpTriageEvidence, McpTriageRun, McpTriageSummary, run_mcp_bridge_evidence,
    run_mcp_guaranteed_evidence, run_mcp_guaranteed_recovery, run_mcp_minimal_evidence,
    run_mcp_triage_evidence,
};
use laser_bench::observer::ObserverCost;
use laser_bench::orchestration::{
    OrchestrationEvidence, OrchestrationKind, OrchestrationSummary, run_orchestration_evidence,
};
use laser_bench::process::{ComposeServices, NativeIggy, NativePlane};
use laser_bench::provision;
use laser_bench::recovery::{
    ConsumerRecoverySummary, IggyRecoverySummary, RecoveryCase, RecoveryDriver, RecoveryRun,
    RecoverySummary, run_consumer_recovery, run_iggy_recovery, run_recovery_evidence,
};
use laser_bench::reliable::{
    ReliableCase, ReliableEvidence, ReliableSummary, ReliableVariant, run_reliable_evidence,
};
use laser_bench::render::{verify_suite_analysis, write_suite_analysis};
use laser_bench::report::{
    AnalysisStatus, ArtifactIdentity, BenchmarkLanguage, EnvironmentReport, OutcomeCounts,
    RunReport, SourceIdentity, WorkloadReport,
};
use laser_bench::review::{
    McpReviewerBundleRef, verify_mcp_review_signoff, verify_mcp_reviewer_bundle,
    write_mcp_reviewer_bundle,
};
use laser_bench::rust_client::{
    RustClientDriver, RustClientStartupRun, RustClientStartupSummary, run_rust_client_startup,
};
use laser_bench::schema::{REPORT_SCHEMA, SUITE_SCHEMA, validate_json};
use laser_bench::streaming::{
    DirectPairEvidence, DirectPairSummary, DirectStreamingCase, StreamingConsumerPath,
    StreamingPipelinePath, StreamingProducerPath, is_c2_driver, run_consumer_pair_evidence,
    run_pipeline_pair_evidence, run_producer_pair_evidence,
};
use laser_bench::telemetry::TelemetrySampler;
use laser_bench::{BenchError, contract};

use crate::execution_lock::ExecutionLock;
use crate::ui;

#[derive(Clone, Copy)]
pub(crate) struct RunIdentity<'a> {
    suite_digest: &'a str,
    repetition: u32,
    seed: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct DirectExecution<'a> {
    stack: &'a provision::ResolvedStack,
    manifest: &'a SuiteManifest,
    scenario: &'a laser_bench::manifest::Scenario,
    run: RunIdentity<'a>,
    output: &'a Path,
}

#[derive(Clone, Copy)]
pub(crate) struct AgdxExecution<'a> {
    stack: &'a provision::ResolvedStack,
    connection_string: &'a str,
    manifest: &'a SuiteManifest,
    scenario: &'a laser_bench::manifest::Scenario,
    run: RunIdentity<'a>,
    output: &'a Path,
    case: &'a AgdxCase,
    processes: &'a [(String, u32)],
}

#[derive(Clone, Copy)]
pub(crate) struct ReportScope<'a> {
    stack: &'a provision::ResolvedStack,
    manifest: &'a SuiteManifest,
    scenario: &'a laser_bench::manifest::Scenario,
    run: RunIdentity<'a>,
}

#[derive(Clone, Copy)]
pub(crate) struct ManagedMeasurements<'a> {
    operation: &'a laser_bench::managed::ManagedArmSummary,
    load: &'a laser_bench::engine::LoadResult,
    processes: &'a [laser_bench::managed::ManagedProcessMeasurement],
}

#[derive(Clone, Copy)]
pub(crate) struct ManagedExecution<'a> {
    laser: &'a laser_sdk::laser::Laser,
    plane: &'a NativePlane,
    case: &'a ManagedCase,
    scope: ReportScope<'a>,
    output: &'a Path,
    processes: &'a [(String, u32)],
}

pub(crate) struct SuiteScenarioResult {
    index: serde_json::Value,
    invalid_repetitions: usize,
    invalid_analysis: bool,
    reports: Vec<RunReport>,
}

pub(crate) struct PairAnalysisResult {
    value: Option<serde_json::Value>,
    invalid: bool,
}

mod agdx;
mod managed;
mod mcp;
mod recovery;
mod report;
mod runtime;
mod suite;

pub(crate) use agdx::*;
pub(crate) use managed::*;
pub(crate) use mcp::*;
pub(crate) use recovery::*;
pub(crate) use report::*;
pub(crate) use runtime::*;
pub(crate) use suite::*;
