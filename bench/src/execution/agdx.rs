#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn execute_agdx(
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
    let processes = [
        ("client".to_owned(), std::process::id()),
        (
            "iggy-server-ng".to_owned(),
            server
                .pid()
                .ok_or_else(|| BenchError::Invalid("Iggy PID unavailable".to_owned()))?,
        ),
    ];
    let case = AgdxCase {
        payload_bytes: scenario.payload_bytes,
        chunks_per_stream: scenario.batch_size,
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
    let agdx = AgdxExecution {
        stack,
        connection_string: &server.connection_string,
        manifest,
        scenario,
        run,
        output,
        case: &case,
        processes: &processes,
    };
    match scenario.driver.parse::<AgdxDriver>()? {
        AgdxDriver::AgdxPublish => execute_agdx_publish(laser, agdx).await,
        AgdxDriver::AgdxStream => execute_agdx_stream(laser, agdx).await,
        AgdxDriver::ContextFetch => execute_context_fetch(laser, agdx).await,
        AgdxDriver::FanOut => execute_orchestration(laser, agdx, OrchestrationKind::FanOut).await,
        AgdxDriver::ReliableConsume => execute_reliable_consume(laser, agdx).await,
        AgdxDriver::RequestReply => execute_request_reply(laser, agdx).await,
        AgdxDriver::Scatter => execute_orchestration(laser, agdx, OrchestrationKind::Scatter).await,
    }
}

async fn execute_agdx_publish(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string: _connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    if scenario.batch_size != 1 {
        return Err(BenchError::Invalid(
            "agdx-publish requires batch_size = 1".to_owned(),
        ));
    }
    let evidence = run_publish_evidence(laser, case, run.seed, processes).await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_agdx_histograms(output, &evidence)?;
    let report = agdx_report(
        stack, manifest, scenario, run, &summary, &evidence, histograms,
    )?;
    write_validated_report(output, report)
}

async fn execute_request_reply(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    if scenario.batch_size != 1 {
        return Err(BenchError::Invalid(
            "request-reply requires batch_size = 1".to_owned(),
        ));
    }
    let evidence =
        run_request_reply_evidence(laser, connection_string, case, run.seed, processes).await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_request_reply_histograms(output, &evidence)?;
    let report = request_reply_report(
        stack, manifest, scenario, run, &summary, &evidence, histograms,
    )?;
    write_validated_report(output, report)
}

async fn execute_agdx_stream(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string: _connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    let evidence = run_stream_evidence(laser, case, run.seed, processes).await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_stream_histograms(output, &evidence)?;
    let report = stream_report(
        stack, manifest, scenario, run, &summary, &evidence, histograms,
    )?;
    write_validated_report(output, report)
}

async fn execute_reliable_consume(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string: _connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    if scenario.batch_size != 1 {
        return Err(BenchError::Invalid(
            "reliable-consume requires batch_size = 1".to_owned(),
        ));
    }
    let reliable_case = ReliableCase {
        payload_bytes: case.payload_bytes,
        operations: case.operations,
        duration_seconds: case.duration_seconds,
        concurrency: case.concurrency,
        partitions: case.partitions,
        warmup_seconds: case.warmup_seconds,
        timeout_millis: case.timeout_millis,
        offered_rate: case.offered_rate,
        spin_dispatch: case.spin_dispatch,
        max_in_flight: case.max_in_flight,
        variant: scenario.arm.parse::<ReliableVariant>()?,
    };
    let evidence = run_reliable_evidence(laser, &reliable_case, run.seed, processes).await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_reliable_histograms(output, &evidence)?;
    let report = reliable_report(
        stack, manifest, scenario, run, &summary, &evidence, histograms,
    )?;
    write_validated_report(output, report)
}

async fn execute_orchestration(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
    kind: OrchestrationKind,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    if scenario.batch_size != 1 {
        return Err(BenchError::Invalid(format!(
            "{} requires batch_size = 1",
            kind.label()
        )));
    }
    let recipients = usize::try_from(scenario.consumers)
        .map_err(|_| BenchError::Invalid("recipient count exceeds usize".to_owned()))?;
    let evidence = run_orchestration_evidence(
        laser,
        connection_string,
        case,
        recipients,
        kind,
        run.seed,
        processes,
    )
    .await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_orchestration_histograms(output, &evidence)?;
    let report = orchestration_report(
        ReportScope {
            stack,
            manifest,
            scenario,
            run,
        },
        kind,
        &summary,
        &evidence,
        histograms,
    )?;
    write_validated_report(output, report)
}

async fn execute_context_fetch(
    laser: &laser_sdk::laser::Laser,
    execution: AgdxExecution<'_>,
) -> Result<(), BenchError> {
    let AgdxExecution {
        stack,
        connection_string: _connection_string,
        manifest,
        scenario,
        run,
        output,
        case,
        processes,
    } = execution;
    let history_messages = scenario
        .history_messages
        .ok_or_else(|| BenchError::Invalid("context-fetch requires history_messages".to_owned()))?;
    let context_limit = scenario
        .context_limit
        .ok_or_else(|| BenchError::Invalid("context-fetch requires context_limit".to_owned()))?;
    let policy = scenario.arm.parse::<ContextPolicyKind>()?;
    let evidence = run_context_fetch_evidence(
        laser,
        case,
        history_messages,
        context_limit,
        policy,
        run.seed,
        processes,
    )
    .await?;
    let summary = evidence.summary();
    write_json(
        &output.join("agdx-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_context_histograms(output, &evidence)?;
    let report = context_report(
        ReportScope {
            stack,
            manifest,
            scenario,
            run,
        },
        &summary,
        &evidence,
        histograms,
    )?;
    write_validated_report(output, report)
}
