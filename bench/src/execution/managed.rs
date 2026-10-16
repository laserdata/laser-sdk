#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn execute_managed(
    laser: &laser_sdk::laser::Laser,
    server: &NativeIggy,
    backend: &NativeBackend,
    execution: DirectExecution<'_>,
) -> Result<(), BenchError> {
    let processes = [
        ("client".to_owned(), std::process::id()),
        (
            "iggy-server".to_owned(),
            server
                .pid()
                .ok_or_else(|| BenchError::Invalid("Iggy PID unavailable".to_owned()))?,
        ),
        (
            "backend".to_owned(),
            backend
                .pid()
                .ok_or_else(|| BenchError::Invalid("backend PID unavailable".to_owned()))?,
        ),
    ];
    let case = ManagedCase {
        payload_bytes: execution.scenario.payload_bytes,
        operations: execution.scenario.operations,
        duration_seconds: execution.scenario.duration_seconds,
        concurrency: usize::try_from(execution.scenario.producers)
            .map_err(|_| BenchError::Invalid("producer count exceeds usize".to_owned()))?,
        batch_size: execution.scenario.batch_size,
        partitions: execution.scenario.partitions,
        corpus_entries: execution.scenario.corpus_entries,
        warmup_seconds: execution.scenario.warmup_seconds.max(1),
        timeout_millis: execution.scenario.timeout_millis.unwrap_or(30_000),
        offered_rate: execution.scenario.offered_rate,
        spin_dispatch: execution.scenario.spin_dispatch,
        max_in_flight: execution.scenario.max_in_flight,
    };
    let scope = ReportScope {
        stack: execution.stack,
        manifest: execution.manifest,
        scenario: execution.scenario,
        run: execution.run,
    };
    let managed = ManagedExecution {
        laser,
        backend,
        case: &case,
        scope,
        output: execution.output,
        processes: &processes,
    };
    match execution.scenario.driver.parse::<ManagedDriver>()? {
        ManagedDriver::Batch => execute_managed_batch(managed).await,
        ManagedDriver::Fork => execute_managed_fork(managed).await,
        ManagedDriver::Graph => execute_managed_graph(managed).await,
        ManagedDriver::Kv => execute_managed_kv(managed).await,
        ManagedDriver::Memory => execute_managed_memory(managed).await,
        ManagedDriver::Projection => execute_managed_projection(managed).await,
        ManagedDriver::Query => execute_managed_query(managed).await,
        ManagedDriver::Uds => execute_managed_uds(managed).await,
    }
}

async fn execute_managed_uds(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<UdsArm>()?;
    let evidence = run_uds_evidence(
        &execution.backend.socket_path,
        execution.case,
        arm,
        execution.backend.profile,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed UDS correctness or accounting failed",
    )
}

async fn execute_managed_batch(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<ManagedBatchArm>()?;
    let evidence = run_managed_batch_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed batch correctness or accounting failed",
    )
}

async fn execute_managed_graph(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<GraphArm>()?;
    let evidence = run_graph_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed graph correctness or accounting failed",
    )
}

async fn execute_managed_fork(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<ForkArm>()?;
    let evidence = run_fork_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed fork correctness or accounting failed",
    )
}

async fn execute_managed_memory(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<MemoryArm>()?;
    let evidence = run_memory_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed memory correctness or accounting failed",
    )
}

async fn execute_managed_kv(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<KvArm>()?;
    let evidence = run_kv_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed KV correctness or accounting failed",
    )
}

async fn execute_managed_query(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<QueryArm>()?;
    let evidence = run_query_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed query correctness or accounting failed",
    )
}

async fn execute_managed_projection(execution: ManagedExecution<'_>) -> Result<(), BenchError> {
    let arm = execution.scope.scenario.arm.parse::<ProjectionArm>()?;
    let evidence = run_projection_evidence(
        execution.laser,
        execution.case,
        arm,
        execution.backend.profile,
        &execution.scope.scenario.name,
        execution.scope.run.seed,
        execution.processes,
    )
    .await?;
    write_managed_evidence(
        execution.scope,
        execution.output,
        execution.backend,
        &evidence.summary,
        ManagedMeasurements {
            operation: &evidence.summary.operation,
            load: &evidence.load,
            processes: &evidence.processes,
        },
        "managed projection correctness or accounting failed",
    )
}

fn write_managed_evidence<T: serde::Serialize>(
    scope: ReportScope<'_>,
    output: &Path,
    backend: &NativeBackend,
    summary: &T,
    measurements: ManagedMeasurements<'_>,
    invalidation_reason: &str,
) -> Result<(), BenchError> {
    let summary = serde_json::to_value(summary)?;
    write_json(&output.join("managed-summary.json"), &summary)?;
    let histograms = write_managed_histograms(output, measurements.load)?;
    let report = managed_report(
        scope,
        backend,
        measurements.operation,
        summary,
        measurements.processes,
        histograms,
        invalidation_reason,
    )?;
    write_validated_report(output, report)
}
