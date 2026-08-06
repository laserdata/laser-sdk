#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn resolve_stack(
    manifest: &SuiteManifest,
    benchmark_root: &Path,
) -> Result<provision::ResolvedStack, BenchError> {
    let mut stack = provision::resolve(manifest, benchmark_root)?;
    let source_root = benchmark_root.parent().unwrap_or(benchmark_root);
    let benchmark = BinaryManifest::running_benchmark(source_root)?;
    if manifest.authoritative {
        let expected_cpu_target = if manifest.provisioning.cpu_target == "arm64" {
            "native"
        } else {
            &manifest.provisioning.cpu_target
        };
        let actual_cpu_target = benchmark
            .build
            .as_ref()
            .map_or("generic", |build| build.cpu_target.as_str());
        if actual_cpu_target != expected_cpu_target {
            return Err(BenchError::Invalid(format!(
                "authoritative benchmark binary requires -C target-cpu={expected_cpu_target}, found `{actual_cpu_target}`"
            )));
        }
    }
    stack.benchmark = Some(benchmark);
    Ok(stack)
}

pub(crate) fn announce_stack(stack: &provision::ResolvedStack) {
    for binary in [
        &stack.benchmark,
        &stack.iggy_server,
        &stack.iggy_bench,
        &stack.plane,
    ]
    .into_iter()
    .flatten()
    {
        ui::resolved(binary);
    }
}

pub(crate) fn streaming_latency_boundary(scenario: &laser_bench::manifest::Scenario) -> String {
    let boundary = if scenario.driver.parse::<StreamingPipelinePath>().is_ok() {
        "producer-to-consumer"
    } else {
        match scenario.driver.parse::<StreamingConsumerPath>() {
            Ok(StreamingConsumerPath::StreamCursor) => "full-drain",
            Ok(_) => "record delivery",
            Err(_) => match scenario.driver.parse::<StreamingProducerPath>() {
                Ok(StreamingProducerPath::StreamBackground) => "background enqueue",
                Ok(
                    StreamingProducerPath::StreamBatchingRecord
                    | StreamingProducerPath::StreamBatchingByte
                    | StreamingProducerPath::StreamBatchingLinger,
                ) => "batching enqueue",
                Ok(
                    StreamingProducerPath::StreamDirect
                    | StreamingProducerPath::StreamDirectAa
                    | StreamingProducerPath::StreamFluent,
                )
                | Err(_) => "send acknowledgement",
            },
        }
    };
    let latency = if scenario.offered_rate.is_some() {
        "scheduled-response p99"
    } else {
        "service p99"
    };
    format!("{boundary} {latency}")
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn nanos_as_f64(value: u64) -> f64 {
    value as f64
}

pub(crate) fn streaming_driver_supported(driver: &str) -> bool {
    driver.parse::<StreamingProducerPath>().is_ok()
        || driver.parse::<StreamingConsumerPath>().is_ok()
        || driver.parse::<StreamingPipelinePath>().is_ok()
}

pub(crate) fn agdx_driver_supported(driver: &str) -> bool {
    driver.parse::<AgdxDriver>().is_ok()
}

pub(crate) fn managed_driver_supported(driver: &str) -> bool {
    driver.parse::<ManagedDriver>().is_ok()
}

pub(crate) fn local_memory_driver_supported(driver: &str) -> bool {
    driver.parse::<LocalMemoryDriver>().is_ok()
}

pub(crate) fn mcp_driver_supported(driver: &str) -> bool {
    driver.parse::<McpDriver>().is_ok()
}

pub(crate) fn recovery_driver_supported(driver: &str) -> bool {
    driver.parse::<RecoveryDriver>().is_ok()
}

pub(crate) fn rust_client_driver_supported(driver: &str) -> bool {
    driver.parse::<RustClientDriver>().is_ok()
}

pub(crate) fn recovery_scenario(scenario: &laser_bench::manifest::Scenario) -> bool {
    scenario.layer == BenchmarkLayer::L7
        && scenario
            .driver
            .parse::<RecoveryDriver>()
            .is_ok_and(RecoveryDriver::requires_plane)
}

pub(crate) fn scenario_requires_plane(scenario: &laser_bench::manifest::Scenario) -> bool {
    (scenario.layer == BenchmarkLayer::L4 && !local_memory_driver_supported(&scenario.driver))
        || scenario.name.starts_with("managed-")
        || scenario
            .driver
            .parse::<RecoveryDriver>()
            .is_ok_and(RecoveryDriver::requires_plane)
}

pub(crate) async fn execute_scenario(
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    suite_digest: &str,
    output: &Path,
) -> Result<(), BenchError> {
    let host_before = HostSnapshot::capture(output)?;
    if let Some(requirements) = manifest.environment.host.as_ref() {
        requirements.validate(&host_before, scenario_requires_plane(scenario))?;
        pin_process(std::process::id(), &requirements.client_cpus)?;
    }
    write_json(
        &output.join("host-before.json"),
        &serde_json::to_value(&host_before)?,
    )?;
    let benchmark_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let stack = resolve_stack(manifest, benchmark_root)?;
    stack.verify(manifest.requires_plane())?;
    write_json(
        &output.join("resolved-stack.json"),
        &serde_json::to_value(&stack)?,
    )?;
    execute_native_run(DirectExecution {
        stack: &stack,
        manifest,
        scenario,
        run: RunIdentity {
            suite_digest,
            repetition: 0,
            seed: 1,
        },
        output,
    })
    .await?;
    let host_audit = HostAudit::finish(
        host_before,
        HostSnapshot::capture(output)?,
        manifest.environment.host.as_ref(),
    );
    write_json(
        &output.join("host-audit.json"),
        &serde_json::to_value(&host_audit)?,
    )?;
    if !host_audit.valid {
        return Err(BenchError::Invalid(format!(
            "host audit failed: {}",
            host_audit.invalidations.join(", ")
        )));
    }
    if let Some(report) = validate_run_evidence(output)? {
        ui::report(&report, scenario);
        if !report.analysis.valid {
            return Err(BenchError::Invalid(
                report
                    .analysis
                    .invalidation_reason
                    .unwrap_or_else(|| "run produced invalid evidence".to_owned()),
            ));
        }
    }
    ui::success(&format!("run complete: {}", output.display()));
    Ok(())
}

pub(crate) async fn execute_native_run(
    execution: DirectExecution<'_>,
) -> Result<Option<DirectPairSummary>, BenchError> {
    let services = execution.output.join("services");
    fs::create_dir(&services).map_err(|source| BenchError::Write {
        path: services.clone(),
        source,
    })?;
    if execution.stack.mode == laser_bench::manifest::ProvisionMode::Compose {
        return execute_compose_run(execution, &services).await;
    }
    let server_manifest = execution
        .stack
        .iggy_server
        .as_ref()
        .ok_or_else(|| BenchError::Invalid("native run requires iggy-server-ng".to_owned()))?;
    let plane_socket = if scenario_requires_plane(execution.scenario) {
        Some(plane_socket_path(execution.output)?)
    } else {
        None
    };
    let server = NativeIggy::start(
        server_manifest,
        &services,
        plane_socket.as_deref(),
        &execution.manifest.environment,
    )
    .await?;
    let plane = start_plane(execution, &services, &server, plane_socket).await?;
    execute_started_run(execution, server, plane, Some(server_manifest)).await
}

pub(crate) async fn execute_compose_run(
    execution: DirectExecution<'_>,
    services: &Path,
) -> Result<Option<DirectPairSummary>, BenchError> {
    if execution.scenario.layer == BenchmarkLayer::L1 {
        return Err(BenchError::Invalid(
            "Compose mode does not execute the external iggy-bench binary".to_owned(),
        ));
    }
    if execution.scenario.layer == BenchmarkLayer::L7 {
        return Err(BenchError::Invalid(
            "Compose mode does not own services required by recovery scenarios".to_owned(),
        ));
    }
    if matches!(
        execution.scenario.driver.parse::<ManagedDriver>(),
        Ok(ManagedDriver::Uds)
    ) {
        return Err(BenchError::Invalid(
            "Compose mode cannot expose plane's diagnostic Unix socket to the benchmark client"
                .to_owned(),
        ));
    }
    let compose_file = execution
        .manifest
        .provisioning
        .compose_file
        .as_deref()
        .ok_or_else(|| BenchError::Invalid("Compose mode requires compose_file".to_owned()))?;
    let (compose, server, plane) = ComposeServices::start(
        compose_file,
        services,
        scenario_requires_plane(execution.scenario),
        execution.manifest.environment.plane_profile,
    )?;
    let result = execute_started_run(execution, server, plane, None).await;
    let shutdown = compose.shutdown();
    result.and_then(|result| shutdown.map(|()| result))
}

pub(crate) async fn execute_started_run(
    execution: DirectExecution<'_>,
    server: NativeIggy,
    mut plane: Option<NativePlane>,
    server_manifest: Option<&BinaryManifest>,
) -> Result<Option<DirectPairSummary>, BenchError> {
    let laser = server.probe_vsr().await?;
    if let Some(plane) = plane.as_mut() {
        plane.wait_ready(Duration::from_secs(30)).await?;
    }
    if matches!(
        execution.scenario.driver.parse::<RecoveryDriver>(),
        Ok(RecoveryDriver::IggyRestart)
    ) {
        let server_manifest = server_manifest.ok_or_else(|| {
            BenchError::Invalid("Iggy restart requires a harness-owned native binary".to_owned())
        })?;
        return execute_iggy_restart_path(laser, server, plane, server_manifest, execution).await;
    }
    if recovery_scenario(execution.scenario) {
        let plane = plane
            .take()
            .ok_or_else(|| BenchError::Invalid("L7 scenario requires plane".to_owned()))?;
        let result = execute_recovery(&laser, &server, plane, execution).await;
        drop(laser);
        let server_shutdown = server.shutdown().await;
        let (plane, telemetry, report) = result?;
        let plane_shutdown = plane.shutdown().await;
        write_json(&execution.output.join("telemetry.json"), &telemetry)?;
        write_validated_report(execution.output, report)?;
        plane_shutdown?;
        server_shutdown?;
        return Ok(None);
    }
    let mut telemetry_processes = vec![
        ("laser-bench".to_owned(), std::process::id()),
        (
            "iggy-server-ng".to_owned(),
            server
                .pid()
                .ok_or_else(|| BenchError::Invalid("Iggy PID unavailable".to_owned()))?,
        ),
    ];
    if let Some(plane) = plane.as_ref() {
        telemetry_processes.push((
            "plane".to_owned(),
            plane
                .pid()
                .ok_or_else(|| BenchError::Invalid("plane PID unavailable".to_owned()))?,
        ));
    }
    let telemetry = TelemetrySampler::start(
        &server.connection_string,
        telemetry_processes,
        plane.as_ref().and_then(|plane| plane.health_address),
        Duration::from_secs(1),
        execution
            .manifest
            .environment
            .host
            .as_ref()
            .is_some_and(|host| host.perf_counters),
    )
    .await?;
    let result = execute_layer(&laser, &server, plane.as_ref(), execution).await;
    let telemetry_result = telemetry.stop().await;
    drop(laser);
    let plane_shutdown = match plane {
        Some(plane) => plane.shutdown().await,
        None => Ok(()),
    };
    let server_shutdown = server.shutdown().await;
    let result = result?;
    let telemetry = telemetry_result?;
    write_json(
        &execution.output.join("telemetry.json"),
        &serde_json::to_value(telemetry)?,
    )?;
    plane_shutdown?;
    server_shutdown?;
    Ok(result)
}

pub(crate) fn plane_socket_path(output: &Path) -> Result<PathBuf, BenchError> {
    let mut hasher = DefaultHasher::new();
    output.hash(&mut hasher);
    let runtime = std::env::temp_dir().join(format!(
        "laser-bench-{}-{:016x}",
        std::process::id(),
        hasher.finish()
    ));
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(&runtime)
        .map_err(|source| BenchError::Write {
            path: runtime.clone(),
            source,
        })?;
    Ok(runtime.join("plane.sock"))
}

pub(crate) async fn start_plane(
    execution: DirectExecution<'_>,
    services: &Path,
    server: &NativeIggy,
    plane_socket: Option<PathBuf>,
) -> Result<Option<NativePlane>, BenchError> {
    if !scenario_requires_plane(execution.scenario) {
        return Ok(None);
    }
    let plane_manifest = execution
        .stack
        .plane
        .as_ref()
        .ok_or_else(|| BenchError::Invalid("managed run requires a plane binary".to_owned()))?;
    let socket = plane_socket
        .ok_or_else(|| BenchError::Invalid("managed run requires a plane socket".to_owned()))?;
    NativePlane::start(
        plane_manifest,
        services,
        server,
        socket,
        &execution.manifest.environment,
    )
    .await
    .map(Some)
}

pub(crate) async fn execute_iggy_restart_path(
    laser: laser_sdk::laser::Laser,
    server: NativeIggy,
    plane: Option<NativePlane>,
    server_manifest: &laser_bench::binary::BinaryManifest,
    execution: DirectExecution<'_>,
) -> Result<Option<DirectPairSummary>, BenchError> {
    let result = execute_iggy_recovery(laser, server, server_manifest, execution).await;
    if let Some(plane) = plane {
        plane.shutdown().await?;
    }
    result?.shutdown().await?;
    Ok(None)
}

pub(crate) async fn execute_layer(
    laser: &laser_sdk::laser::Laser,
    server: &NativeIggy,
    plane: Option<&NativePlane>,
    execution: DirectExecution<'_>,
) -> Result<Option<DirectPairSummary>, BenchError> {
    match execution.scenario.layer {
        BenchmarkLayer::L1 => {
            execute_l1(
                execution.stack,
                server,
                execution.scenario,
                execution.output,
            )?;
            Ok(None)
        }
        BenchmarkLayer::L2 if streaming_driver_supported(&execution.scenario.driver) => {
            execute_direct(laser, server, execution).await.map(Some)
        }
        BenchmarkLayer::L3 if agdx_driver_supported(&execution.scenario.driver) => {
            execute_agdx(laser, server, execution).await?;
            Ok(None)
        }
        BenchmarkLayer::L4 if managed_driver_supported(&execution.scenario.driver) => {
            let plane = plane
                .ok_or_else(|| BenchError::Invalid("L4 scenario requires plane".to_owned()))?;
            execute_managed(laser, server, plane, execution).await?;
            Ok(None)
        }
        BenchmarkLayer::L4 if local_memory_driver_supported(&execution.scenario.driver) => {
            execute_local_memory(execution).await?;
            Ok(None)
        }
        BenchmarkLayer::L5 if mcp_driver_supported(&execution.scenario.driver) => {
            execute_mcp(laser, server, execution).await?;
            Ok(None)
        }
        BenchmarkLayer::L6 if rust_client_driver_supported(&execution.scenario.driver) => {
            execute_rust_client(server, execution).await?;
            Ok(None)
        }
        BenchmarkLayer::L7 if recovery_driver_supported(&execution.scenario.driver) => {
            execute_consumer_recovery(laser, execution).await?;
            Ok(None)
        }
        scenario_layer => Err(BenchError::Invalid(format!(
            "scenario driver `{}` is not implemented for layer `{scenario_layer}`",
            execution.scenario.driver
        ))),
    }
}

pub(crate) async fn execute_local_memory(execution: DirectExecution<'_>) -> Result<(), BenchError> {
    let driver = execution.scenario.driver.parse::<LocalMemoryDriver>()?;
    let processes = [("client".to_owned(), std::process::id())];
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
    let dimensions = execution.scenario.vector_dimensions.ok_or_else(|| {
        BenchError::Invalid("local memory scenario requires vector_dimensions".to_owned())
    })?;
    let evidence = run_local_memory_evidence(
        &case,
        driver,
        dimensions,
        &execution.scenario.name,
        execution.run.seed,
        &processes,
    )
    .await?;
    write_json(
        &execution.output.join("local-memory-summary.json"),
        &serde_json::to_value(&evidence.summary)?,
    )?;
    let histograms = write_managed_histograms(execution.output, &evidence.load)?;
    write_validated_report(
        execution.output,
        local_memory_report(
            ReportScope {
                stack: execution.stack,
                manifest: execution.manifest,
                scenario: execution.scenario,
                run: execution.run,
            },
            &evidence.summary,
            &evidence.processes,
            histograms,
        )?,
    )
}

pub(crate) async fn execute_rust_client(
    server: &NativeIggy,
    execution: DirectExecution<'_>,
) -> Result<(), BenchError> {
    let driver = execution.scenario.driver.parse::<RustClientDriver>()?;
    match driver {
        RustClientDriver::RustStartup => {
            let server_pid = server
                .pid()
                .ok_or_else(|| BenchError::Invalid("Iggy process has no PID".to_owned()))?;
            let processes = [
                ("client".to_owned(), std::process::id()),
                ("iggy-server-ng".to_owned(), server_pid),
            ];
            let summary = run_rust_client_startup(RustClientStartupRun {
                connection_string: &server.connection_string,
                seed: execution.run.seed,
                payload_bytes: execution.scenario.payload_bytes,
                partitions: execution.scenario.partitions,
                monitored_processes: &processes,
            })
            .await?;
            write_json(
                &execution.output.join("rust-client-startup-summary.json"),
                &serde_json::to_value(&summary)?,
            )?;
            let scope = ReportScope {
                stack: execution.stack,
                manifest: execution.manifest,
                scenario: execution.scenario,
                run: execution.run,
            };
            write_validated_report(
                execution.output,
                rust_client_startup_report(scope, &summary)?,
            )
        }
    }
}

pub(crate) fn execute_l1(
    stack: &provision::ResolvedStack,
    server: &NativeIggy,
    scenario: &laser_bench::manifest::Scenario,
    output: &Path,
) -> Result<(), BenchError> {
    let binary = stack
        .iggy_bench
        .as_ref()
        .ok_or_else(|| BenchError::Invalid("L1 scenario requires iggy-bench".to_owned()))?;
    let run = IggyBenchmarkRun {
        kind: scenario.driver.parse::<IggyBenchmarkKind>()?,
        message_size: scenario.payload_bytes,
        messages_per_batch: scenario.batch_size,
        message_batches: scenario.operations,
        warmup_seconds: scenario.warmup_seconds,
        streams: scenario.producers.max(1),
        partitions: scenario.partitions,
        producers: scenario.producers,
        consumers: scenario.consumers,
    };
    let imported = run.execute(binary, server.address, &output.join("iggy-bench-output"))?;
    write_json(
        &output.join("iggy-import.json"),
        &serde_json::to_value(imported)?,
    )
}

pub(crate) async fn execute_direct(
    laser: &laser_sdk::laser::Laser,
    server: &NativeIggy,
    execution: DirectExecution<'_>,
) -> Result<DirectPairSummary, BenchError> {
    let DirectExecution {
        stack,
        manifest,
        scenario,
        run,
        output,
    } = execution;
    let server_pid = server
        .pid()
        .ok_or_else(|| BenchError::Invalid("Iggy process has no PID".to_owned()))?;
    let processes = [
        ("client".to_owned(), std::process::id()),
        ("iggy-server-ng".to_owned(), server_pid),
    ];
    let consumer_path = scenario.driver.parse::<StreamingConsumerPath>().ok();
    let pipeline_path = scenario.driver.parse::<StreamingPipelinePath>().ok();
    let actors = if consumer_path.is_some() {
        scenario.consumers
    } else {
        scenario.producers
    };
    let case = DirectStreamingCase {
        payload_bytes: scenario.payload_bytes,
        batch_size: scenario.batch_size,
        batches: scenario.operations,
        duration_seconds: scenario.duration_seconds,
        concurrency: usize::try_from(actors)
            .map_err(|_| BenchError::Invalid("actor count exceeds usize".to_owned()))?,
        partitions: scenario.partitions,
        warmup_seconds: scenario.warmup_seconds.max(1),
        timeout_millis: scenario.timeout_millis.unwrap_or(30_000),
        offered_rate: scenario.offered_rate,
        spin_dispatch: scenario.spin_dispatch,
        max_in_flight: scenario.max_in_flight,
    };
    let connection_string = server.connection_string.as_str();
    let evidence = if let Some(path) = pipeline_path {
        run_pipeline_pair_evidence(laser, connection_string, &case, run.seed, &processes, path)
            .await?
    } else if let Some(path) = consumer_path {
        run_consumer_pair_evidence(laser, connection_string, &case, run.seed, &processes, path)
            .await?
    } else {
        let path = scenario.driver.parse::<StreamingProducerPath>()?;
        run_producer_pair_evidence(laser, connection_string, &case, run.seed, &processes, path)
            .await?
    };
    let summary = evidence.summary();
    write_json(
        &output.join("pair-summary.json"),
        &serde_json::to_value(&summary)?,
    )?;
    let histograms = write_direct_histograms(output, &evidence)?;
    let report = direct_report(
        stack, manifest, scenario, run, &summary, &evidence, histograms,
    )?;
    write_validated_report(output, report)?;
    Ok(summary)
}
