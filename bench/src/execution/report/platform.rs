#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn rust_client_startup_report(
    scope: ReportScope<'_>,
    summary: &RustClientStartupSummary,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let valid = outcomes_consistent(&summary.outcomes)
        && correctness_valid(&summary.outcomes)
        && summary.outcomes.failed == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert(
        "rust_client_startup".to_owned(),
        serde_json::to_value(summary)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: "rust_client_cold_vs_warmed".to_owned(),
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
            logical_unit: "rust-client-lifecycle".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: None,
        },
        outcomes: summary.outcomes.clone(),
        histograms: Vec::new(),
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "Rust client startup replay validation failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn managed_report(
    scope: ReportScope<'_>,
    plane: &NativePlane,
    operation: &laser_bench::managed::ManagedArmSummary,
    summary: serde_json::Value,
    processes: &[laser_bench::managed::ManagedProcessMeasurement],
    histograms: Vec<laser_bench::report::HistogramRef>,
    invalidation_reason: &str,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &operation.outcomes;
    let valid = outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0
        && outcomes.missed == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("managed_summary".to_owned(), summary);
    extra.insert("processes".to_owned(), serde_json::to_value(processes)?);
    extra.insert(
        "plane".to_owned(),
        serde_json::json!({
            "profile": plane.profile,
            "db_path": plane.db_path,
            "socket_path": plane.socket_path,
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: operation.arm.clone(),
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
            logical_unit: "managed_command".to_owned(),
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
            publishable: report_publishable(manifest, stack, source.dirty, valid)
                && !matches!(
                    scenario.driver.parse::<ManagedDriver>(),
                    Ok(ManagedDriver::Uds)
                ),
            invalidation_reason: (!valid).then(|| invalidation_reason.to_owned()),
        },
        extra,
    })
}

pub(crate) fn local_memory_report(
    scope: ReportScope<'_>,
    summary: &LocalMemorySummary,
    processes: &[laser_bench::managed::ManagedProcessMeasurement],
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.operation.outcomes;
    let valid = outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0
        && outcomes.missed == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("managed_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert("processes".to_owned(), serde_json::to_value(processes)?);
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: summary.operation.arm.clone(),
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
            durability_profile: "in_process".to_owned(),
            cache_state: manifest.environment.cache_state.clone(),
            kernel: kernel_identity()?,
            architecture: std::env::consts::ARCH.to_owned(),
            runtime_worker_threads: laser_bench::report::runtime_worker_threads(),
        },
        workload: WorkloadReport {
            logical_unit: "local_memory_operation".to_owned(),
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
                .then(|| "local vector-memory correctness or outcome accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn recovery_report(
    scope: ReportScope<'_>,
    plane: &NativePlane,
    summary: &RecoverySummary,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.outcomes;
    let valid = outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0
        && outcomes.missed == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert(
        "recovery_summary".to_owned(),
        serde_json::to_value(summary)?,
    );
    extra.insert(
        "plane".to_owned(),
        serde_json::json!({
            "profile": plane.profile,
            "db_path": plane.db_path,
            "socket_path": plane.socket_path,
        }),
    );
    extra.insert(
        "telemetry".to_owned(),
        serde_json::json!({
            "before": "telemetry-before.json",
            "after": "telemetry-after.json",
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: scenario.driver.clone(),
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
            logical_unit: "recovery".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: outcomes.clone(),
        histograms: Vec::new(),
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "recovery correctness or convergence failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn consumer_recovery_report(
    scope: ReportScope<'_>,
    summary: &ConsumerRecoverySummary,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.outcomes;
    let valid = outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert(
        "recovery_summary".to_owned(),
        serde_json::to_value(summary)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: scenario.driver.clone(),
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
            logical_unit: "consumer-recovery".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: None,
        },
        outcomes: outcomes.clone(),
        histograms: Vec::new(),
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "consumer restart did not converge without loss".to_owned()),
        },
        extra,
    })
}

pub(crate) fn iggy_recovery_report(
    scope: ReportScope<'_>,
    summary: &IggyRecoverySummary,
) -> Result<RunReport, BenchError> {
    let ReportScope {
        stack,
        manifest,
        scenario,
        run,
    } = scope;
    let source = sdk_source_snapshot()?;
    let outcomes = &summary.outcomes;
    let valid = outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0;
    let mut extra = std::collections::BTreeMap::new();
    extra.insert(
        "recovery_summary".to_owned(),
        serde_json::to_value(summary)?,
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: scenario.driver.clone(),
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
            logical_unit: "iggy-recovery".to_owned(),
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: None,
        },
        outcomes: outcomes.clone(),
        histograms: Vec::new(),
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "Iggy restart did not preserve and resume the log".to_owned()),
        },
        extra,
    })
}
