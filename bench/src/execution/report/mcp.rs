#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn mcp_bridge_report(
    scope: ReportScope<'_>,
    summary: &McpBridgeSummary,
    evidence: &McpBridgeEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let arm_outcomes = [&summary.native.outcomes, &summary.streamable_http.outcomes];
    let valid = arm_outcomes
        .iter()
        .all(|outcomes| outcomes_consistent(outcomes) && correctness_valid(outcomes));
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("mcp_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({
            "native": evidence.native.processes,
            "streamable_http": evidence.streamable_http.processes,
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "mcp_bridge_native_vs_streamable_http".to_owned(),
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
            logical_unit: "mcp-tools-call".to_owned(),
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
            invalidation_reason: (!valid)
                .then(|| "MCP bridge comparison correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn mcp_minimal_report(
    scope: ReportScope<'_>,
    summary: &McpMinimalSummary,
    evidence: &McpMinimalEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.streamable_http.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("mcp_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({"streamable_http": evidence.streamable_http.processes}),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "minimal_mcp_streamable_http".to_owned(),
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
            logical_unit: "mcp-tools-call".to_owned(),
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
                .then(|| "minimal MCP correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn mcp_guaranteed_report(
    scope: ReportScope<'_>,
    summary: &McpGuaranteedSummary,
    evidence: &McpGuaranteedEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.streamable_http.outcomes;
    let valid = outcomes_consistent(outcomes) && correctness_valid(outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("mcp_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({"streamable_http": evidence.streamable_http.processes}),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "guarantee_matched_mcp".to_owned(),
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
            durability_profile: "postgres_synchronous_commit_on".to_owned(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "durable-mcp-tools-call".to_owned(),
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
                .then(|| "guarantee-matched MCP correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn mcp_recovery_report(
    scope: ReportScope<'_>,
    summary: &McpGuaranteedRecoverySummary,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let valid = summary.result_committed_before_ack
        && summary.replay_attempts == 2
        && summary.retained_results == 1
        && summary.delivered;
    let outcomes = OutcomeCounts {
        offered: 1,
        dispatched: 1,
        completed: 1,
        successful: u64::from(valid),
        failed: u64::from(!valid),
        ..OutcomeCounts::default()
    };
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("mcp_recovery".to_owned(), serde_json::to_value(summary)?);
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "guarantee_matched_mcp_recovery".to_owned(),
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
            durability_profile: "postgres_synchronous_commit_on".to_owned(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "mcp-recovery-window".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: None,
        },
        outcomes,
        histograms: Vec::new(),
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "guarantee-matched MCP did not converge after restart".to_owned()),
        },
        extra,
    })
}

pub(crate) fn mcp_triage_report(
    scope: ReportScope<'_>,
    summary: &McpTriageSummary,
    evidence: &McpTriageEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
    review: &McpReviewerBundleRef,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let arm_outcomes = [
        &summary.agdx.request_reply.outcomes,
        &summary.minimal_mcp.streamable_http.outcomes,
        &summary.guarantee_matched_mcp.streamable_http.outcomes,
    ];
    let outcomes_valid = arm_outcomes.iter().all(|outcomes| {
        outcomes_consistent(outcomes)
            && correctness_valid(outcomes)
            && outcomes.failed == 0
            && outcomes.timed_out == 0
    });
    let valid = outcomes_valid && summary.byte_accounting.m6.measurement_valid;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("mcp_triage".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "mcp_reviewer_bundle".to_owned(),
        serde_json::to_value(review)?,
    );
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({
            "agdx": evidence.agdx.request_reply.processes,
            "minimal_mcp": evidence.minimal_mcp.streamable_http.processes,
            "guarantee_matched_mcp": evidence.guarantee_matched_mcp.streamable_http.processes,
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "agdx_vs_minimal_vs_guarantee_matched_mcp".to_owned(),
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
            durability_profile: "declared_per_arm".to_owned(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "triage-ticket".to_owned(),
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
            publishable: false,
            invalidation_reason: if !outcomes_valid {
                Some("MCP triage arm correctness or outcome accounting failed".to_owned())
            } else if !summary.byte_accounting.m6.measurement_valid {
                Some("MCP triage kernel TCP byte accounting was incomplete".to_owned())
            } else {
                None
            },
        },
        extra,
    })
}
