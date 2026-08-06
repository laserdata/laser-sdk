#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn inspect_histogram(path: &Path) -> Result<(), BenchError> {
    let histogram = read_sidecar(path)?;
    ui::phase("Histogram", &format!("Decoded {}", path.display()));
    ui::phase(
        "Samples",
        &format!(
            "{} samples, min {} ns, p50 {} ns, p90 {} ns, p99 {} ns, max {} ns",
            histogram.len(),
            histogram.min(),
            histogram.value_at_quantile(0.50),
            histogram.value_at_quantile(0.90),
            histogram.value_at_quantile(0.99),
            histogram.max()
        ),
    );
    ui::success("histogram sidecar verified and decoded");
    Ok(())
}

pub(crate) fn analyze_path(path: &Path) -> Result<(), BenchError> {
    if path.join("suite-index.json").is_file() {
        let (manifest, reports) = load_suite_reports(path)?;
        write_suite_analysis(path, &manifest.name, manifest.authoritative, &reports)?;
        verify_suite_analysis(path)?;
        ui::success(&format!(
            "suite analysis valid: {}",
            path.join("analysis/report.html").display()
        ));
        return Ok(());
    }
    let report_path = path.join("report.json");
    validate_run_evidence(path)?.ok_or_else(|| {
        BenchError::Invalid(format!("report not found at {}", report_path.display()))
    })?;
    ui::success(&format!("report evidence valid: {}", report_path.display()));
    Ok(())
}

pub(crate) fn bundle_suite(suite_dir: &Path, output: &Path) -> Result<(), BenchError> {
    let (manifest, reports) = load_suite_reports(suite_dir)?;
    if !manifest.authoritative {
        return Err(BenchError::Invalid(
            "only an authoritative suite can become a publication bundle".to_owned(),
        ));
    }
    if reports.is_empty() {
        return Err(BenchError::Invalid(
            "publication suite has no reports".to_owned(),
        ));
    }
    validate_publication_reports(suite_dir, &reports)?;
    verify_suite_analysis(suite_dir)?;
    let publication = prepare_publication(suite_dir, output)?;
    ui::success(&format!(
        "unsigned publication bundle prepared: {} file(s) at {}",
        publication.files.len(),
        output.display()
    ));
    ui::phase(
        "Signature",
        &format!(
            "sign {} in the release pipeline, then run verify-bundle",
            output.join("publication-manifest.json").display()
        ),
    );
    Ok(())
}

pub(crate) fn verify_bundle(bundle_dir: &Path) -> Result<(), BenchError> {
    let publication = verify_publication(bundle_dir, true)?;
    ui::success(&format!(
        "signed publication bundle valid: {} file(s) at {}",
        publication.files.len(),
        bundle_dir.display()
    ));
    Ok(())
}

pub(crate) async fn execute_suite(manifest_path: &Path, output: &Path) -> Result<(), BenchError> {
    let _lock = ExecutionLock::acquire()?;
    let suite_digest = sha256_file(manifest_path)?;
    let manifest = SuiteManifest::load(manifest_path)?;
    prepare_output(output)?;
    write_json(
        &output.join("suite-plan.json"),
        &serde_json::to_value(&manifest)?,
    )?;
    execute_deterministic_gates(output)?;
    let host_before = HostSnapshot::capture(output)?;
    if let Some(requirements) = manifest.environment.host.as_ref() {
        requirements.validate(&host_before, manifest.requires_plane())?;
        pin_process(std::process::id(), &requirements.client_cpus)?;
    }
    write_json(
        &output.join("host-before.json"),
        &serde_json::to_value(&host_before)?,
    )?;
    ui::host(&host_before);
    let scheduler_calibration = calibrate_scheduler(&manifest, output).await?;
    let benchmark_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    ui::phase(
        "Resolve",
        &format!(
            "Resolving {} native binaries for CPU target {}...",
            provisioning_label(manifest.provisioning.mode),
            manifest.provisioning.cpu_target
        ),
    );
    let stack = resolve_stack(&manifest, benchmark_root)?;
    stack.verify(manifest.requires_plane())?;
    announce_stack(&stack);
    write_json(
        &output.join("resolved-stack.json"),
        &serde_json::to_value(&stack)?,
    )?;
    Box::pin(execute_suite_scenarios(
        &stack,
        &manifest,
        &suite_digest,
        output,
        host_before,
        scheduler_calibration,
    ))
    .await
}

pub(crate) fn execute_deterministic_gates(output: &Path) -> Result<(), BenchError> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| {
            BenchError::Invalid("benchmark workspace has no repository root".to_owned())
        })?;
    ui::phase(
        "Gates",
        "Checking SDK allocation budget and direct payload pointer identity...",
    );
    let evidence = [sdk_allocation_gate(), sdk_zero_copy_gate()]
        .into_iter()
        .map(|gate| gate.run(repository))
        .collect::<Result<Vec<_>, BenchError>>()?;
    write_json(
        &output.join("deterministic-gates.json"),
        &serde_json::to_value(&evidence)?,
    )?;
    if let Some(failed) = evidence.iter().find(|gate| !gate.passed) {
        return Err(BenchError::Invalid(format!(
            "deterministic gate `{}` failed",
            failed.name
        )));
    }
    ui::phase("Gates", "Allocation and zero-copy checks passed");
    Ok(())
}

pub(crate) async fn calibrate_scheduler(
    manifest: &SuiteManifest,
    output: &Path,
) -> Result<Option<SchedulerCalibration>, BenchError> {
    let highest = manifest
        .scenarios
        .iter()
        .flat_map(|scenario| {
            scenario
                .offered_rate
                .into_iter()
                .chain(scenario.offered_rates.iter().copied())
        })
        .max();
    let Some(highest) = highest else {
        return Ok(None);
    };
    let duration = if manifest.authoritative { 10 } else { 1 };
    let dispatch = if manifest
        .scenarios
        .iter()
        .any(|scenario| scenario.spin_dispatch)
    {
        laser_bench::engine::Dispatch::SpinWindow
    } else {
        laser_bench::engine::Dispatch::Sleep
    };
    ui::phase(
        "Calibrate",
        &format!("Testing no-server scheduler headroom above {highest} operations per second..."),
    );
    let calibration = run_scheduler_calibration(highest, duration, dispatch).await?;
    write_json(
        &output.join("scheduler-calibration.json"),
        &serde_json::to_value(&calibration)?,
    )?;
    ui::phase(
        "Calibrate",
        &format!(
            "{:.0} operations per second achieved, p99 lateness {} ns, {}",
            calibration.achieved_operations_per_second,
            calibration.p99_lateness_ns,
            if calibration.passed { "pass" } else { "fail" }
        ),
    );
    Ok(Some(calibration))
}

pub(crate) async fn execute_suite_scenarios(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    suite_digest: &str,
    output: &Path,
    host_before: HostSnapshot,
    scheduler_calibration: Option<SchedulerCalibration>,
) -> Result<(), BenchError> {
    let scenarios = manifest.expanded_scenarios();
    let scenario_count = scenarios.len();
    let mut index = Vec::with_capacity(scenario_count);
    let mut invalid_repetitions = 0usize;
    let mut invalid_analyses = 0usize;
    let mut reports = Vec::new();
    if manifest.authoritative
        && scheduler_calibration
            .as_ref()
            .is_some_and(|calibration| !calibration.passed)
    {
        invalid_analyses = invalid_analyses.saturating_add(1);
    }
    ui::suite(
        &manifest.name,
        scenario_count,
        manifest.authoritative,
        &manifest.environment.tier,
        output,
    );
    let mut failed_scenarios = 0usize;
    for (scenario_index, scenario) in scenarios.into_iter().enumerate() {
        match execute_suite_scenario(
            stack,
            manifest,
            &scenario,
            suite_digest,
            output,
            scenario_index + 1,
            scenario_count,
        )
        .await
        {
            Ok(result) => {
                invalid_repetitions =
                    invalid_repetitions.saturating_add(result.invalid_repetitions);
                invalid_analyses =
                    invalid_analyses.saturating_add(usize::from(result.invalid_analysis));
                reports.extend(result.reports);
                index.push(result.index);
            }
            // One broken scenario must not orphan the evidence of every other
            // scenario: record the failure, keep going, and fail the suite at
            // the end after the index and analysis exist.
            Err(error) => {
                failed_scenarios = failed_scenarios.saturating_add(1);
                ui::failure(&format!("scenario {} failed: {error}", scenario.name));
                index.push(serde_json::json!({
                    "name": scenario.name,
                    "layer": scenario.layer,
                    "driver": scenario.driver,
                    "repetitions": scenario.repetitions,
                    "invalid_repetitions": scenario.repetitions,
                    "error": error.to_string(),
                    "paired_analysis": serde_json::Value::Null,
                }));
            }
        }
    }
    let host_after = HostSnapshot::capture(output)?;
    let host_audit = HostAudit::finish(host_before, host_after, manifest.environment.host.as_ref());
    write_json(
        &output.join("host-audit.json"),
        &serde_json::to_value(&host_audit)?,
    )?;
    invalid_analyses = invalid_analyses.saturating_add(usize::from(!host_audit.valid));
    write_json(
        &output.join("suite-index.json"),
        &serde_json::json!({
            "schema_version": 1,
            "suite": manifest.name,
            "suite_digest": suite_digest,
            "authoritative": manifest.authoritative,
            "invalid_repetitions": invalid_repetitions,
            "invalid_analyses": invalid_analyses,
            "failed_scenarios": failed_scenarios,
            "host_audit": host_audit,
            "scheduler_calibration": scheduler_calibration,
            "scenarios": index,
        }),
    )?;
    write_suite_analysis(output, &manifest.name, manifest.authoritative, &reports)?;
    ui::complete(output, invalid_repetitions, invalid_analyses);
    if invalid_repetitions != 0 || invalid_analyses != 0 || failed_scenarios != 0 {
        return Err(BenchError::Invalid(format!(
            "suite retained {failed_scenarios} failed scenario(s), {invalid_repetitions} invalid repetition(s), and {invalid_analyses} invalid analysis gate(s)"
        )));
    }
    Ok(())
}

pub(crate) async fn execute_suite_scenario(
    stack: &provision::ResolvedStack,
    manifest: &SuiteManifest,
    scenario: &laser_bench::manifest::Scenario,
    suite_digest: &str,
    output: &Path,
    scenario_index: usize,
    scenario_count: usize,
) -> Result<SuiteScenarioResult, BenchError> {
    let scenario_directory = output.join(&scenario.name);
    fs::create_dir(&scenario_directory).map_err(|source| BenchError::Write {
        path: scenario_directory.clone(),
        source,
    })?;
    ui::scenario(scenario_index, scenario_count, scenario);
    let mut pairs = Vec::new();
    let mut invalid_repetitions = 0usize;
    let mut reports = Vec::new();
    for repetition in 0..scenario.repetitions {
        let run_directory = scenario_directory.join(format!("repetition-{repetition:03}"));
        prepare_output(&run_directory)?;
        let seed = u64::from(repetition) + 1;
        write_json(
            &run_directory.join("run-plan.json"),
            &serde_json::json!({
                "schema_version": 1,
                "suite": manifest.name,
                "scenario": scenario,
                "repetition": repetition,
                "seed": seed,
                "contract": contract::fingerprint(),
            }),
        )?;
        ui::repetition_started(repetition, scenario.repetitions, &run_directory);
        let started = Instant::now();
        let pair = execute_native_run(DirectExecution {
            stack,
            manifest,
            scenario,
            run: RunIdentity {
                suite_digest,
                repetition,
                seed,
            },
            output: &run_directory,
        })
        .await?;
        if let Some(pair) = pair {
            ui::pair(&pair, scenario);
            pairs.push(pair);
        }
        let report = validate_run_evidence(&run_directory)?;
        let valid = report.as_ref().is_none_or(|report| report.analysis.valid);
        if let Some(report) = &report {
            ui::report(report, scenario);
            reports.push(report.clone());
        }
        invalid_repetitions = invalid_repetitions.saturating_add(usize::from(!valid));
        ui::repetition_completed(
            repetition,
            scenario.repetitions,
            started.elapsed(),
            &run_directory,
            valid,
        );
    }
    let analysis = write_pair_analysis(
        &scenario_directory,
        scenario,
        &pairs,
        manifest.authoritative,
        manifest.environment.tier.contains("smoke"),
    )?;
    Ok(SuiteScenarioResult {
        index: serde_json::json!({
            "name": scenario.name,
            "layer": scenario.layer,
            "driver": scenario.driver,
            "repetitions": scenario.repetitions,
            "invalid_repetitions": invalid_repetitions,
            "paired_analysis": analysis.value,
        }),
        invalid_repetitions,
        invalid_analysis: analysis.invalid,
        reports,
    })
}

pub(crate) fn load_suite_reports(
    root: &Path,
) -> Result<(SuiteManifest, Vec<RunReport>), BenchError> {
    let plan = read_json(&root.join("suite-plan.json"))?;
    validate_json(SUITE_SCHEMA, &plan)?;
    let manifest: SuiteManifest = serde_json::from_value(plan)?;
    manifest.validate()?;
    let suite_index = read_json(&root.join("suite-index.json"))?;
    let host_audit_path = root.join("host-audit.json");
    if host_audit_path.is_file() {
        let host_audit = read_json(&host_audit_path)?;
        if suite_index.get("host_audit") != Some(&host_audit) {
            return Err(BenchError::Invalid(
                "suite index host audit does not match host-audit.json".to_owned(),
            ));
        }
        if manifest.authoritative && host_audit.get("valid") != Some(&serde_json::Value::Bool(true))
        {
            return Err(BenchError::Invalid(
                "authoritative suite host audit is invalid".to_owned(),
            ));
        }
    } else if manifest.authoritative {
        return Err(BenchError::Invalid(
            "authoritative suite is missing host-audit.json".to_owned(),
        ));
    }
    let calibration_path = root.join("scheduler-calibration.json");
    if calibration_path.is_file() {
        let calibration = read_json(&calibration_path)?;
        if suite_index.get("scheduler_calibration") != Some(&calibration) {
            return Err(BenchError::Invalid(
                "suite index scheduler calibration does not match scheduler-calibration.json"
                    .to_owned(),
            ));
        }
        if manifest.authoritative
            && calibration.get("passed") != Some(&serde_json::Value::Bool(true))
        {
            return Err(BenchError::Invalid(
                "authoritative suite scheduler calibration failed".to_owned(),
            ));
        }
    } else if manifest.authoritative
        && manifest
            .scenarios
            .iter()
            .any(|scenario| scenario.offered_rate.is_some() || !scenario.offered_rates.is_empty())
    {
        return Err(BenchError::Invalid(
            "authoritative open-loop suite is missing scheduler calibration".to_owned(),
        ));
    }
    let mut reports = Vec::new();
    for scenario in manifest.expanded_scenarios() {
        for repetition in 0..scenario.repetitions {
            let directory = root
                .join(&scenario.name)
                .join(format!("repetition-{repetition:03}"));
            if let Some(report) = validate_run_evidence(&directory)? {
                reports.push(report);
            }
        }
    }
    Ok((manifest, reports))
}

pub(crate) fn validate_run_evidence(directory: &Path) -> Result<Option<RunReport>, BenchError> {
    let report_path = directory.join("report.json");
    if !report_path.is_file() {
        return Ok(None);
    }
    let value = read_json(&report_path)?;
    validate_json(REPORT_SCHEMA, &value)?;
    let report: RunReport = serde_json::from_value(value)?;
    if report.deterministic_gates.is_empty()
        || report.deterministic_gates.iter().any(|gate| !gate.passed)
    {
        return Err(BenchError::Invalid(format!(
            "report `{}` has missing or failed deterministic gates",
            report.run_id
        )));
    }
    if let Some(path) = directory
        .ancestors()
        .map(|ancestor| ancestor.join("deterministic-gates.json"))
        .find(|path| path.is_file())
    {
        let gates = serde_json::from_value::<Vec<laser_bench::report::DeterministicGateEvidence>>(
            read_json(&path)?,
        )?;
        if gates != report.deterministic_gates {
            return Err(BenchError::Invalid(format!(
                "report `{}` does not match the campaign deterministic gates",
                report.run_id
            )));
        }
    }
    for histogram in &report.histograms {
        verify_sidecar_at(directory, histogram)?;
    }
    if let Some(reference) = report.extra.get("mcp_reviewer_bundle") {
        let reference = serde_json::from_value::<McpReviewerBundleRef>(reference.clone())?;
        verify_mcp_reviewer_bundle(directory, &reference)?;
    }
    Ok(Some(report))
}

pub(crate) fn validate_publication_reports(
    root: &Path,
    reports: &[RunReport],
) -> Result<(), BenchError> {
    let stack = read_json(&root.join("resolved-stack.json"))?;
    if stack.get("authoritative") != Some(&serde_json::Value::Bool(true)) {
        return Err(BenchError::Invalid(
            "resolved stack is not authoritative".to_owned(),
        ));
    }
    for report in reports {
        if !report.analysis.valid || report.source.dirty {
            return Err(BenchError::Invalid(format!(
                "report `{}` is invalid or comes from dirty SDK source",
                report.run_id
            )));
        }
        if report.analysis.publishable {
            continue;
        }
        let reference = report
            .extra
            .get("mcp_reviewer_bundle")
            .cloned()
            .ok_or_else(|| {
                BenchError::Invalid(format!("report `{}` is not publishable", report.run_id))
            })?;
        let reference = serde_json::from_value::<McpReviewerBundleRef>(reference)?;
        let directory = root
            .join(&report.scenario)
            .join(format!("repetition-{:03}", report.repetition));
        verify_mcp_review_signoff(&directory, &reference)?;
    }
    Ok(())
}

pub(crate) fn write_pair_analysis(
    directory: &Path,
    scenario: &laser_bench::manifest::Scenario,
    pairs: &[DirectPairSummary],
    authoritative: bool,
    smoke: bool,
) -> Result<PairAnalysisResult, BenchError> {
    let producer_path = scenario.driver.parse::<StreamingProducerPath>().ok();
    let c2 = is_c2_driver(&scenario.driver);
    let calibration = producer_path == Some(StreamingProducerPath::StreamDirectAa);
    if let [pair] = pairs {
        ui::point_estimate(pair);
        return Ok(PairAnalysisResult {
            value: None,
            invalid: authoritative && (c2 || calibration),
        });
    }
    if pairs.len() < 2 {
        return Ok(PairAnalysisResult {
            value: None,
            invalid: false,
        });
    }
    let observations = pairs
        .iter()
        .map(|pair| PairedObservation {
            raw_throughput: pair.raw.records_per_second,
            laser_throughput: pair.laser.records_per_second,
            raw_latency_ns: nanos_as_f64(pair.raw.primary_p99_ns),
            laser_latency_ns: nanos_as_f64(pair.laser.primary_p99_ns),
        })
        .collect::<Vec<_>>();
    let analysis = analyze_paired(&observations, 10_000, 1)?;
    let mut gate = evaluate_c2(&analysis, 0.99, 1.01);
    let load_accepted = pairs.iter().all(|pair| {
        pair.raw.p99_supported
            && pair.laser.p99_supported
            && c2_outcomes_accepted(&pair.raw.outcomes)
            && c2_outcomes_accepted(&pair.laser.outcomes)
    });
    gate.passed &= load_accepted && (c2 || calibration);
    let value = serde_json::json!({
        "latency_boundary": streaming_latency_boundary(scenario),
        "analysis": &analysis,
        "load_accepted": load_accepted,
        "c2": c2.then_some(&gate),
        "aa_calibration": calibration.then_some(&gate),
    });
    write_json(&directory.join("paired-analysis.json"), &value)?;
    if c2 {
        ui::c2(&analysis, gate.passed, authoritative, smoke);
    } else if calibration {
        ui::calibration(&analysis, gate.passed, authoritative);
    } else {
        ui::paired(&analysis);
    }
    Ok(PairAnalysisResult {
        value: Some(value),
        invalid: authoritative && (c2 || calibration) && !gate.passed,
    })
}

pub(crate) fn provisioning_label(mode: laser_bench::manifest::ProvisionMode) -> &'static str {
    match mode {
        laser_bench::manifest::ProvisionMode::Artifact => "signed artifact",
        laser_bench::manifest::ProvisionMode::Compose => "Compose",
        laser_bench::manifest::ProvisionMode::Path => "caller-provided",
        laser_bench::manifest::ProvisionMode::Source => "source-built",
    }
}
