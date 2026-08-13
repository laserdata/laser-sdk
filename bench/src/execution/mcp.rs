#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn execute_mcp(
    laser: &laser_sdk::laser::Laser,
    server: &NativeIggy,
    execution: DirectExecution<'_>,
) -> Result<(), BenchError> {
    let DirectExecution {
        stack,
        manifest,
        scenario,
        run,
        output,
    } = execution;
    if scenario.batch_size != 1 {
        return Err(BenchError::Invalid(
            "MCP bridge comparison requires batch_size = 1".to_owned(),
        ));
    }
    let processes = [
        ("client".to_owned(), std::process::id()),
        (
            "iggy-server".to_owned(),
            server
                .pid()
                .ok_or_else(|| BenchError::Invalid("Iggy process has no PID".to_owned()))?,
        ),
    ];
    let case = AgdxCase {
        payload_bytes: scenario.payload_bytes,
        chunks_per_stream: 1,
        operations: scenario.operations,
        duration_seconds: scenario.duration_seconds,
        concurrency: usize::try_from(scenario.producers)
            .map_err(|_| BenchError::Invalid("producer count exceeds usize".to_owned()))?,
        partitions: scenario.partitions,
        warmup_seconds: scenario.warmup_seconds.max(1),
        timeout_millis: scenario.timeout_millis.unwrap_or(30_000),
        offered_rate: scenario.offered_rate,
        spin_dispatch: scenario.spin_dispatch,
        max_in_flight: scenario.max_in_flight,
    };
    let scope = ReportScope {
        stack,
        manifest,
        scenario,
        run,
    };
    match scenario.driver.parse::<McpDriver>()? {
        McpDriver::McpBridge => {
            execute_mcp_bridge(laser, &case, run.seed, &processes, scope, output).await
        }
        McpDriver::McpGuaranteed => execute_mcp_guaranteed(&case, &processes, scope, output).await,
        McpDriver::McpGuaranteedRecovery => execute_mcp_recovery(scope, output).await,
        McpDriver::McpMinimal => execute_mcp_minimal(&case, &processes, scope, output).await,
        McpDriver::McpTriage => {
            execute_mcp_triage(
                laser,
                &server.connection_string,
                &case,
                &processes,
                server.address.port(),
                scope,
                output,
            )
            .await
        }
    }
}

async fn execute_mcp_bridge(
    laser: &laser_sdk::laser::Laser,
    case: &AgdxCase,
    seed: u64,
    processes: &[(String, u32)],
    scope: ReportScope<'_>,
    output: &Path,
) -> Result<(), BenchError> {
    let evidence = run_mcp_bridge_evidence(laser, case, seed, processes).await?;
    write_json(
        &output.join("mcp-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    let histograms = write_mcp_histograms(output, &evidence)?;
    let report = mcp_bridge_report(scope, &evidence.summary, &evidence, histograms)?;
    write_validated_report(output, report)
}

async fn execute_mcp_guaranteed(
    case: &AgdxCase,
    processes: &[(String, u32)],
    scope: ReportScope<'_>,
    output: &Path,
) -> Result<(), BenchError> {
    let dsn = mcp_postgres_dsn(scope)?;
    let monitored_processes = mcp_monitored_processes(scope, processes)?;
    let evidence = run_mcp_guaranteed_evidence(
        case,
        scope.run.seed,
        scope.scenario.consumers,
        &dsn,
        &monitored_processes,
    )
    .await?;
    write_json(
        &output.join("mcp-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    let histograms = write_mcp_guaranteed_histograms(output, &evidence)?;
    let report = mcp_guaranteed_report(scope, &evidence.summary, &evidence, histograms)?;
    write_validated_report(output, report)
}

async fn execute_mcp_recovery(scope: ReportScope<'_>, output: &Path) -> Result<(), BenchError> {
    let dsn = mcp_postgres_dsn(scope)?;
    let summary = run_mcp_guaranteed_recovery(
        &dsn,
        scope.run.seed,
        scope.scenario.payload_bytes,
        Duration::from_secs(30),
    )
    .await?;
    write_json(
        &output.join("mcp-recovery-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    write_validated_report(output, mcp_recovery_report(scope, &summary)?)
}

fn mcp_postgres_dsn(scope: ReportScope<'_>) -> Result<String, BenchError> {
    let dsn_env = scope
        .manifest
        .environment
        .postgres_dsn_env
        .as_deref()
        .ok_or_else(|| {
            BenchError::Invalid(
                "guarantee-matched MCP requires environment.postgres_dsn_env".to_owned(),
            )
        })?;
    std::env::var(dsn_env).map_err(|_| {
        BenchError::Invalid(format!(
            "guarantee-matched MCP requires environment variable `{dsn_env}`"
        ))
    })
}

fn mcp_monitored_processes(
    scope: ReportScope<'_>,
    base: &[(String, u32)],
) -> Result<Vec<(String, u32)>, BenchError> {
    let mut processes = base.to_vec();
    let Some(pid_env) = scope.manifest.environment.postgres_pid_env.as_deref() else {
        return Ok(processes);
    };
    let value = std::env::var(pid_env).map_err(|_| {
        BenchError::Invalid(format!(
            "configured PostgreSQL PID environment variable `{pid_env}` is not set"
        ))
    })?;
    let pid = value.parse::<u32>().map_err(|error| {
        BenchError::Invalid(format!(
            "configured PostgreSQL PID environment variable `{pid_env}` is not a valid u32: {error}"
        ))
    })?;
    processes.push(("postgres".to_owned(), pid));
    Ok(processes)
}

async fn execute_mcp_minimal(
    case: &AgdxCase,
    processes: &[(String, u32)],
    scope: ReportScope<'_>,
    output: &Path,
) -> Result<(), BenchError> {
    let evidence = run_mcp_minimal_evidence(case, scope.run.seed, processes).await?;
    write_json(
        &output.join("mcp-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    let histograms = write_mcp_minimal_histograms(output, &evidence)?;
    let report = mcp_minimal_report(scope, &evidence.summary, &evidence, histograms)?;
    write_validated_report(output, report)
}

async fn execute_mcp_triage(
    laser: &laser_sdk::laser::Laser,
    connection_string: &str,
    case: &AgdxCase,
    processes: &[(String, u32)],
    iggy_server_port: u16,
    scope: ReportScope<'_>,
    output: &Path,
) -> Result<(), BenchError> {
    let dsn = mcp_postgres_dsn(scope)?;
    let monitored_processes = mcp_monitored_processes(scope, processes)?;
    let evidence = run_mcp_triage_evidence(McpTriageRun {
        laser,
        connection_string,
        case,
        seed: scope.run.seed,
        recipients: scope.scenario.consumers,
        dsn: &dsn,
        monitored_processes: &monitored_processes,
        iggy_server_port,
        output,
    })
    .await?;
    write_json(
        &output.join("mcp-triage-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    let histograms = write_mcp_triage_histograms(output, &evidence)?;
    let review = write_mcp_reviewer_bundle(
        output,
        scope.scenario,
        &scope.manifest.environment,
        &evidence.summary,
        &histograms,
    )?;
    let report = mcp_triage_report(scope, &evidence.summary, &evidence, histograms, &review)?;
    write_validated_report(output, report)
}
