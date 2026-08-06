#[allow(clippy::wildcard_imports)]
use super::*;

mod agdx;
mod mcp;
mod platform;

pub(crate) use agdx::*;
pub(crate) use mcp::*;
pub(crate) use platform::*;

pub(crate) fn write_validated_report(
    output: &Path,
    mut report: RunReport,
) -> Result<(), BenchError> {
    report.deterministic_gates = load_deterministic_gates(output)?;
    if report.observer_cost.is_none() {
        let observer_cost = ObserverCost::process_sampling_pilot()?;
        write_json(
            &output.join("observer-cost.json"),
            &serde_json::to_value(observer_cost)?,
        )?;
        report.observer_cost = Some(observer_cost);
    }
    let value = serde_json::to_value(report)?;
    validate_json(REPORT_SCHEMA, &value)?;
    write_json(&output.join("report.json"), &value)
}

fn load_deterministic_gates(
    output: &Path,
) -> Result<Vec<laser_bench::report::DeterministicGateEvidence>, BenchError> {
    for directory in output.ancestors() {
        let path = directory.join("deterministic-gates.json");
        if path.is_file() {
            return serde_json::from_value(read_json(&path)?).map_err(Into::into);
        }
    }
    Err(BenchError::Invalid(
        "run evidence has no deterministic gate results".to_owned(),
    ))
}

pub(crate) fn write_agdx_histograms(
    output: &Path,
    evidence: &AgdxPublishEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "bare-scheduled-response",
            &evidence.bare.load.scheduled_response,
        ),
        ("bare-service", &evidence.bare.load.service),
        (
            "bare-scheduler-lateness",
            &evidence.bare.load.scheduler_lateness,
        ),
        ("bare-service-failed", &evidence.bare.load.failed_service),
        (
            "provenance-scheduled-response",
            &evidence.provenance.load.scheduled_response,
        ),
        ("provenance-service", &evidence.provenance.load.service),
        (
            "provenance-scheduler-lateness",
            &evidence.provenance.load.scheduler_lateness,
        ),
        (
            "provenance-service-failed",
            &evidence.provenance.load.failed_service,
        ),
        (
            "typed-scheduled-response",
            &evidence.typed.load.scheduled_response,
        ),
        ("typed-service", &evidence.typed.load.service),
        (
            "typed-scheduler-lateness",
            &evidence.typed.load.scheduler_lateness,
        ),
        ("typed-service-failed", &evidence.typed.load.failed_service),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_request_reply_histograms(
    output: &Path,
    evidence: &AgdxRequestReplyEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "request-reply-scheduled-response",
            &evidence.request_reply.load.scheduled_response,
        ),
        (
            "request-reply-service",
            &evidence.request_reply.load.service,
        ),
        (
            "request-reply-scheduler-lateness",
            &evidence.request_reply.load.scheduler_lateness,
        ),
        (
            "request-reply-service-failed",
            &evidence.request_reply.load.failed_service,
        ),
        ("request-handler-entry", &evidence.handler_entry),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_stream_histograms(
    output: &Path,
    evidence: &AgdxStreamEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "stream-scheduled-response",
            &evidence.stream.load.scheduled_response,
        ),
        ("stream-service", &evidence.stream.load.service),
        (
            "stream-scheduler-lateness",
            &evidence.stream.load.scheduler_lateness,
        ),
        (
            "stream-service-failed",
            &evidence.stream.load.failed_service,
        ),
        ("stream-time-to-first-chunk", &evidence.time_to_first_chunk),
        ("stream-inter-chunk-gap", &evidence.inter_chunk_gap),
        ("stream-completion", &evidence.completion),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_reliable_histograms(
    output: &Path,
    evidence: &ReliableEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "reliable-scheduled-response",
            &evidence.consume.load.scheduled_response,
        ),
        ("reliable-service", &evidence.consume.load.service),
        (
            "reliable-scheduler-lateness",
            &evidence.consume.load.scheduler_lateness,
        ),
        (
            "reliable-service-failed",
            &evidence.consume.load.failed_service,
        ),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_orchestration_histograms(
    output: &Path,
    evidence: &OrchestrationEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "orchestration-scheduled-response",
            &evidence.orchestration.load.scheduled_response,
        ),
        (
            "orchestration-service",
            &evidence.orchestration.load.service,
        ),
        (
            "orchestration-scheduler-lateness",
            &evidence.orchestration.load.scheduler_lateness,
        ),
        (
            "orchestration-service-failed",
            &evidence.orchestration.load.failed_service,
        ),
        ("orchestration-recipient-entry", &evidence.recipient_entry),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_context_histograms(
    output: &Path,
    evidence: &ContextFetchEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "context-scheduled-response",
            &evidence.fetch.load.scheduled_response,
        ),
        ("context-service", &evidence.fetch.load.service),
        (
            "context-scheduler-lateness",
            &evidence.fetch.load.scheduler_lateness,
        ),
        (
            "context-service-failed",
            &evidence.fetch.load.failed_service,
        ),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_mcp_histograms(
    output: &Path,
    evidence: &McpBridgeEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "mcp-native-scheduled-response",
            &evidence.native.load.scheduled_response,
        ),
        ("mcp-native-service", &evidence.native.load.service),
        (
            "mcp-native-scheduler-lateness",
            &evidence.native.load.scheduler_lateness,
        ),
        (
            "mcp-native-service-failed",
            &evidence.native.load.failed_service,
        ),
        (
            "mcp-http-scheduled-response",
            &evidence.streamable_http.load.scheduled_response,
        ),
        ("mcp-http-service", &evidence.streamable_http.load.service),
        (
            "mcp-http-scheduler-lateness",
            &evidence.streamable_http.load.scheduler_lateness,
        ),
        (
            "mcp-http-service-failed",
            &evidence.streamable_http.load.failed_service,
        ),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_mcp_minimal_histograms(
    output: &Path,
    evidence: &McpMinimalEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "mcp-minimal-scheduled-response",
            &evidence.streamable_http.load.scheduled_response,
        ),
        (
            "mcp-minimal-service",
            &evidence.streamable_http.load.service,
        ),
        (
            "mcp-minimal-scheduler-lateness",
            &evidence.streamable_http.load.scheduler_lateness,
        ),
        (
            "mcp-minimal-service-failed",
            &evidence.streamable_http.load.failed_service,
        ),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_mcp_guaranteed_histograms(
    output: &Path,
    evidence: &McpGuaranteedEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "mcp-guaranteed-scheduled-response",
            &evidence.streamable_http.load.scheduled_response,
        ),
        (
            "mcp-guaranteed-service",
            &evidence.streamable_http.load.service,
        ),
        (
            "mcp-guaranteed-scheduler-lateness",
            &evidence.streamable_http.load.scheduler_lateness,
        ),
        (
            "mcp-guaranteed-service-failed",
            &evidence.streamable_http.load.failed_service,
        ),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_mcp_triage_histograms(
    output: &Path,
    evidence: &McpTriageEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let arms = [
        ("agdx", &evidence.agdx.request_reply.load),
        ("minimal-mcp", &evidence.minimal_mcp.streamable_http.load),
        (
            "guarantee-matched-mcp",
            &evidence.guarantee_matched_mcp.streamable_http.load,
        ),
    ];
    let mut references = Vec::with_capacity(9);
    for (name, load) in arms {
        references.push(write_sidecar(
            &histogram_directory,
            &format!("{name}-scheduled-response"),
            &load.scheduled_response,
        )?);
        references.push(write_sidecar(
            &histogram_directory,
            &format!("{name}-service"),
            &load.service,
        )?);
        references.push(write_sidecar(
            &histogram_directory,
            &format!("{name}-scheduler-lateness"),
            &load.scheduler_lateness,
        )?);
    }
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn write_managed_histograms(
    output: &Path,
    load: &laser_bench::engine::LoadResult,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        ("managed-scheduled-response", &load.scheduled_response),
        ("managed-service", &load.service),
        ("managed-scheduler-lateness", &load.scheduler_lateness),
        ("managed-service-failed", &load.failed_service),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    relativize_histograms(&mut references)?;
    Ok(references)
}

pub(crate) fn aggregate_outcomes<const N: usize>(
    outcomes: [&laser_bench::report::OutcomeCounts; N],
) -> laser_bench::report::OutcomeCounts {
    outcomes.into_iter().fold(
        laser_bench::report::OutcomeCounts::default(),
        |mut total, item| {
            total.offered = total.offered.saturating_add(item.offered);
            total.dispatched = total.dispatched.saturating_add(item.dispatched);
            total.completed = total.completed.saturating_add(item.completed);
            total.successful = total.successful.saturating_add(item.successful);
            total.failed = total.failed.saturating_add(item.failed);
            total.timed_out = total.timed_out.saturating_add(item.timed_out);
            total.missed = total.missed.saturating_add(item.missed);
            total.duplicates = total.duplicates.saturating_add(item.duplicates);
            total.gaps = total.gaps.saturating_add(item.gaps);
            total.ordering_violations = total
                .ordering_violations
                .saturating_add(item.ordering_violations);
            total.checksum_failures = total
                .checksum_failures
                .saturating_add(item.checksum_failures);
            total.late_arrivals = total.late_arrivals.saturating_add(item.late_arrivals);
            total
        },
    )
}

pub(crate) fn report_publishable(
    manifest: &SuiteManifest,
    stack: &provision::ResolvedStack,
    source_dirty: bool,
    valid: bool,
) -> bool {
    manifest.authoritative && stack.authoritative && !source_dirty && valid
}

pub(crate) fn write_direct_histograms(
    output: &Path,
    evidence: &DirectPairEvidence,
) -> Result<Vec<laser_bench::report::HistogramRef>, BenchError> {
    let histogram_directory = output.join("histograms");
    let mut references = [
        (
            "raw-scheduled-response",
            &evidence.raw.load.scheduled_response,
        ),
        ("raw-service", &evidence.raw.load.service),
        (
            "raw-scheduler-lateness",
            &evidence.raw.load.scheduler_lateness,
        ),
        ("raw-service-failed", &evidence.raw.load.failed_service),
        (
            "laser-scheduled-response",
            &evidence.laser.load.scheduled_response,
        ),
        ("laser-service", &evidence.laser.load.service),
        (
            "laser-scheduler-lateness",
            &evidence.laser.load.scheduler_lateness,
        ),
        ("laser-service-failed", &evidence.laser.load.failed_service),
    ]
    .into_iter()
    .map(|(name, histogram)| write_sidecar(&histogram_directory, name, histogram))
    .collect::<Result<Vec<_>, BenchError>>()?;
    for reference in &mut references {
        let name = Path::new(&reference.path)
            .file_name()
            .ok_or_else(|| BenchError::Invalid("histogram path has no file name".to_owned()))?;
        reference.path = Path::new("histograms")
            .join(name)
            .to_string_lossy()
            .into_owned();
    }
    Ok(references)
}

pub(crate) fn relativize_histograms(
    references: &mut [laser_bench::report::HistogramRef],
) -> Result<(), BenchError> {
    for reference in references {
        let name = Path::new(&reference.path)
            .file_name()
            .ok_or_else(|| BenchError::Invalid("histogram path has no file name".to_owned()))?;
        reference.path = Path::new("histograms")
            .join(name)
            .to_string_lossy()
            .into_owned();
    }
    Ok(())
}

pub(crate) fn direct_report(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    run: RunIdentity<'_>,
    summary: &DirectPairSummary,
    evidence: &DirectPairEvidence,
    histograms: Vec<laser_bench::report::HistogramRef>,
) -> Result<RunReport, BenchError> {
    let source = sdk_source_snapshot()?;
    let valid = outcomes_consistent(&summary.raw.outcomes)
        && outcomes_consistent(&summary.laser.outcomes)
        && correctness_valid(&summary.raw.outcomes)
        && correctness_valid(&summary.laser.outcomes);
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("pair_summary".to_owned(), serde_json::to_value(summary)?);
    extra.insert(
        "processes".to_owned(),
        serde_json::json!({
            "raw": evidence.raw.processes,
            "laser": evidence.laser.processes,
        }),
    );
    Ok(RunReport {
        schema_version: 1,
        run_id: format!("{}-r{}", scenario.name, run.repetition),
        suite_digest: run.suite_digest.to_owned(),
        scenario: scenario.name.clone(),
        arm: format!("paired-raw-iggy-{}", summary.laser.arm),
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
            logical_unit: if scenario.driver.parse::<StreamingPipelinePath>().is_ok() {
                "record".to_owned()
            } else {
                match scenario.driver.parse::<StreamingConsumerPath>() {
                    Ok(StreamingConsumerPath::StreamCursor) => "full-drain".to_owned(),
                    Ok(_) => "record".to_owned(),
                    Err(_)
                        if matches!(
                            scenario.driver.parse::<StreamingProducerPath>(),
                            Ok(StreamingProducerPath::StreamFluent)
                        ) =>
                    {
                        "record".to_owned()
                    }
                    Err(_) => "batch".to_owned(),
                }
            },
            payload_bytes: scenario.payload_bytes,
            batch_size: scenario.batch_size,
            partitions: scenario.partitions,
            offered_rate: scenario.offered_rate,
        },
        outcomes: summary.laser.outcomes.clone(),
        histograms,
        deterministic_gates: Vec::new(),
        observer_cost: None,
        analysis: AnalysisStatus {
            valid,
            publishable: report_publishable(manifest, stack, source.dirty, valid),
            invalidation_reason: (!valid)
                .then(|| "raw or Laser streaming correctness or accounting failed".to_owned()),
        },
        extra,
    })
}

pub(crate) fn sdk_source_snapshot() -> Result<SourceSnapshot, BenchError> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .map_err(|error| BenchError::Invalid(format!("failed to resolve SDK root: {error}")))?;
    SourceSnapshot::capture(&root)
}

pub(crate) fn outcomes_consistent(outcomes: &laser_bench::report::OutcomeCounts) -> bool {
    outcomes.dispatched.saturating_add(outcomes.missed) == outcomes.offered
        && outcomes.completed == outcomes.dispatched
        && outcomes
            .successful
            .saturating_add(outcomes.failed)
            .saturating_add(outcomes.timed_out)
            == outcomes.completed
}

pub(crate) fn correctness_valid(outcomes: &laser_bench::report::OutcomeCounts) -> bool {
    outcomes.duplicates == 0
        && outcomes.gaps == 0
        && outcomes.ordering_violations == 0
        && outcomes.checksum_failures == 0
}

pub(crate) fn c2_outcomes_accepted(outcomes: &laser_bench::report::OutcomeCounts) -> bool {
    outcomes_consistent(outcomes)
        && correctness_valid(outcomes)
        && outcomes.failed == 0
        && outcomes.timed_out == 0
        && outcomes.missed == 0
}

pub(crate) fn stack_artifacts(
    stack: &provision::ResolvedStack,
    cpu_target: &str,
) -> Vec<ArtifactIdentity> {
    [
        stack.benchmark.as_ref(),
        stack.iggy_server.as_ref(),
        stack.iggy_bench.as_ref(),
        stack.plane.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|binary| ArtifactIdentity {
        name: binary.name.clone(),
        version: binary.version.clone(),
        source: binary.source.clone(),
        cpu_target: binary
            .build
            .as_ref()
            .map_or_else(|| cpu_target.to_owned(), |build| build.cpu_target.clone()),
        sha256: binary.sha256.clone(),
        minisign_verified: binary.minisign_verified,
    })
    .collect()
}

pub(crate) fn kernel_identity() -> Result<String, BenchError> {
    let output = ProcessCommand::new("uname")
        .args(["-s", "-r"])
        .output()
        .map_err(|error| BenchError::Invalid(format!("failed to execute uname: {error}")))?;
    if !output.status.success() {
        return Err(BenchError::Invalid("uname failed".to_owned()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub(crate) fn prepare_output(path: &Path) -> Result<(), BenchError> {
    if path.exists() {
        return Err(BenchError::Invalid(format!(
            "output path `{}` already exists",
            path.display()
        )));
    }
    fs::create_dir_all(path).map_err(|source| BenchError::Write {
        path: path.to_path_buf(),
        source,
    })
}

pub(crate) fn read_json(path: &Path) -> Result<serde_json::Value, BenchError> {
    let source = fs::read_to_string(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&source).map_err(Into::into)
}

pub(crate) fn write_json(path: &Path, value: &serde_json::Value) -> Result<(), BenchError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).map_err(|source| BenchError::Write {
        path: path.to_path_buf(),
        source,
    })
}
