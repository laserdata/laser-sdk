#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn execute_consumer_recovery(
    laser: &laser_sdk::laser::Laser,
    execution: DirectExecution<'_>,
) -> Result<(), BenchError> {
    let driver = execution.scenario.driver.parse::<RecoveryDriver>()?;
    if driver != RecoveryDriver::ConsumerRestart {
        return Err(BenchError::Invalid(format!(
            "recovery driver `{driver}` requires the dedicated process restart path"
        )));
    }
    let summary = run_consumer_recovery(
        laser,
        &RecoveryCase {
            payload_bytes: execution.scenario.payload_bytes,
            backlog_records: usize::try_from(execution.scenario.operations).map_err(|_| {
                BenchError::Invalid("consumer recovery backlog exceeds usize".to_owned())
            })?,
            partitions: execution.scenario.partitions,
            timeout: Duration::from_secs(execution.scenario.duration_seconds),
        },
        &execution.scenario.name,
        execution.run.seed,
    )
    .await?;
    write_json(
        &execution.output.join("consumer-recovery-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    write_validated_report(
        execution.output,
        consumer_recovery_report(
            ReportScope {
                stack: execution.stack,
                manifest: execution.manifest,
                scenario: execution.scenario,
                run: execution.run,
            },
            &summary,
        )?,
    )
}

pub(crate) async fn execute_iggy_recovery(
    laser: laser_sdk::laser::Laser,
    server: NativeIggy,
    server_manifest: &laser_bench::binary::BinaryManifest,
    execution: DirectExecution<'_>,
) -> Result<NativeIggy, BenchError> {
    let evidence = run_iggy_recovery(
        laser,
        server,
        server_manifest,
        &execution.manifest.environment,
        &RecoveryCase {
            payload_bytes: execution.scenario.payload_bytes,
            backlog_records: usize::try_from(execution.scenario.operations).map_err(|_| {
                BenchError::Invalid("Iggy recovery backlog exceeds usize".to_owned())
            })?,
            partitions: execution.scenario.partitions,
            timeout: Duration::from_secs(execution.scenario.duration_seconds),
        },
        &execution.scenario.name,
        execution.run.seed,
    )
    .await?;
    write_json(
        &execution.output.join("iggy-recovery-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    write_json(
        &execution.output.join("telemetry-before.json"),
        &serde_json::to_value(&evidence.telemetry_before)?,
    )?;
    write_json(
        &execution.output.join("telemetry-after.json"),
        &serde_json::to_value(&evidence.telemetry_after)?,
    )?;
    write_json(
        &execution.output.join("telemetry.json"),
        &serde_json::json!({
            "before": evidence.telemetry_before,
            "after": evidence.telemetry_after,
        }),
    )?;
    write_validated_report(
        execution.output,
        iggy_recovery_report(
            ReportScope {
                stack: execution.stack,
                manifest: execution.manifest,
                scenario: execution.scenario,
                run: execution.run,
            },
            &evidence.summary,
        )?,
    )?;
    Ok(evidence.server)
}

pub(crate) async fn execute_recovery(
    laser: &laser_sdk::laser::Laser,
    server: &NativeIggy,
    plane: NativePlane,
    execution: DirectExecution<'_>,
) -> Result<(NativePlane, serde_json::Value, RunReport), BenchError> {
    let plane_manifest = execution
        .stack
        .plane
        .as_ref()
        .ok_or_else(|| BenchError::Invalid("L7 scenario requires a plane binary".to_owned()))?;
    let case = RecoveryCase {
        payload_bytes: execution.scenario.payload_bytes,
        backlog_records: usize::try_from(execution.scenario.operations)
            .map_err(|_| BenchError::Invalid("recovery backlog exceeds usize".to_owned()))?,
        partitions: execution.scenario.partitions,
        timeout: Duration::from_secs(execution.scenario.duration_seconds),
    };
    let driver = execution.scenario.driver.parse::<RecoveryDriver>()?;
    let evidence = run_recovery_evidence(RecoveryRun {
        laser,
        server,
        plane,
        plane_manifest,
        environment: &execution.manifest.environment,
        case: &case,
        driver,
        scenario: &execution.scenario.name,
        seed: execution.run.seed,
    })
    .await?;
    write_json(
        &execution.output.join("recovery-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    write_json(
        &execution.output.join("telemetry-before.json"),
        &serde_json::to_value(&evidence.telemetry_before)?,
    )?;
    write_json(
        &execution.output.join("telemetry-after.json"),
        &serde_json::to_value(&evidence.telemetry_after)?,
    )?;
    let telemetry = serde_json::json!({
        "before": &evidence.telemetry_before,
        "after": &evidence.telemetry_after,
    });
    let report = recovery_report(
        ReportScope {
            stack: execution.stack,
            manifest: execution.manifest,
            scenario: execution.scenario,
            run: execution.run,
        },
        &evidence.plane,
        &evidence.summary,
    )?;
    Ok((evidence.plane, telemetry, report))
}
