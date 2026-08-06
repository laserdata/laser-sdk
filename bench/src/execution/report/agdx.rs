#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn agdx_report(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    run: RunIdentity<'_>,
    summary: &AgdxPublishSummary,
    evidence: &AgdxPublishEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let source = sdk_source_snapshot()?;
    let arm_outcomes = [
        &summary.bare.outcomes,
        &summary.provenance.outcomes,
        &summary.typed.outcomes,
    ];
    let valid = arm_outcomes
        .iter()
        .all(|outcomes| outcomes_consistent(outcomes) && correctness_valid(outcomes));
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({
            "bare": evidence.bare.processes,
            "provenance": evidence.provenance.processes,
            "typed": evidence.typed.processes,
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "agdx-publish-decomposition".to_owned(),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "publish".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: aggregate_outcomes(arm_outcomes),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid).then(|| {
                "one or more AGDX publish arms failed correctness or accounting".to_owned()
            }),
        },
        extra,
    })
}

pub(crate) fn request_reply_report(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    run: RunIdentity<'_>,
    summary: &AgdxRequestReplySummary,
    evidence: &AgdxRequestReplyEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.request_reply.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::to_value(&evidence.request_reply.processes)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "typed-agdx-request-reply".to_owned(),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "request".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "AGDX request/reply correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn stream_report(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    run: RunIdentity<'_>,
    summary: &AgdxStreamSummary,
    evidence: &AgdxStreamEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.stream.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::to_value(&evidence.stream.processes)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "typed-agdx-stream".to_owned(),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "chunk-stream".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "AGDX stream correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn reliable_report(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    run: RunIdentity<'_>,
    summary: &ReliableSummary,
    evidence: &ReliableEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.consume.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::to_value(&evidence.consume.processes)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: format!("reliable-consume-{}", scenario.arm),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "consumed-command".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "reliable-consume correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn orchestration_report(
    scope: ReportScope<'_>,
    kind: OrchestrationKind,
    summary: &OrchestrationSummary,
    evidence: &OrchestrationEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.orchestration.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::to_value(&evidence.orchestration.processes)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: kind.label().to_owned(),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "orchestration".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| format!("{} correctness or accounting failed", kind.label())),
        },
        extra,
    })
}

pub(crate) fn context_report(
    scope: ReportScope<'_>,
    summary: &ContextFetchSummary,
    evidence: &ContextFetchEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.fetch.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("agdx_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::to_value(&evidence.fetch.processes)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: format!("context-fetch-{}", summary.policy.label()),
        repetition: run.repetition,
        seed: run.seed,
        language: BenchmarkLanguage::Rust,
        source: SourceIdentity {
            sdk_revision: source.revision.clone(),
            benchmark_revision: source.revision,
            dirty: source.dirty,
        },
        artifacts: stack_artifacts(stack, &manifest.provisioning.cpu_target),
        environment: EnvironmentReport {
            tier: manifest.environment.tier.clone(),
            durability_profile: manifest.environment.durability_profile.clone(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "context-fetch".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "context-fetch correctness or accounting failed".to_owned()),
        },
        extra,
    })
}
