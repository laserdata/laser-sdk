use std::env;
use std::io::{self, IsTerminal, Write as _};
use std::path::Path;
use std::time::Duration;

use colored::Colorize as _;

use laser_bench::analysis::PairedAnalysis;
use laser_bench::binary::BinaryManifest;
use laser_bench::doctor::DoctorReport;
use laser_bench::host::HostSnapshot;
use laser_bench::manifest::{BenchmarkLayer, Scenario};
use laser_bench::report::RunReport;
use laser_bench::streaming::{DirectPairSummary, StreamingConsumerPath, StreamingProducerPath};

pub fn init() {
    let colors = io::stdout().is_terminal()
        && env::var_os("NO_COLOR").is_none()
        && env::var("TERM").is_ok_and(|term| term != "dumb");
    colored::control::set_override(colors);
}

pub fn banner() {
    println!();
    println!("  {}", "LASER BENCH".bright_cyan().bold());
    println!(
        "  {} {}",
        "Native VSR benchmark evidence".bright_white(),
        format!("v{}", env!("CARGO_PKG_VERSION")).bright_black()
    );
    println!();
    flush();
}

pub fn phase(label: &str, detail: &str) {
    println!(
        "{} {}  {}",
        "●".bright_cyan(),
        label.bold().bright_white(),
        detail.bright_black()
    );
    flush();
}

pub fn resolved(binary: &BinaryManifest) {
    let verification = if binary.minisign_verified {
        "Minisign verified"
    } else if binary.reused_local_binary {
        "Local cache reused"
    } else {
        "digest verified"
    };
    let digest = binary.sha256.get(..12).unwrap_or(&binary.sha256);
    println!(
        "  {} {:<16} {:<18} {}  {}",
        "✓".green().bold(),
        binary.name.white().bold(),
        binary.version.cyan(),
        verification.green(),
        format!("sha256:{digest}").bright_black()
    );
    flush();
}

pub fn host(snapshot: &HostSnapshot) {
    println!();
    println!("{}", "Host".yellow().bold());
    row("CPU", &snapshot.cpu_model);
    row("Online CPUs", &format_cpu_set(&snapshot.online_cpus));
    row("NUMA nodes", &snapshot.numa_nodes);
    row("Clocksource", &snapshot.clocksource);
    row(
        "SMT / turbo",
        &format!(
            "{} / {}",
            optional_switch(snapshot.smt_enabled),
            optional_switch(snapshot.turbo_enabled)
        ),
    );
    row(
        "Storage",
        &format!(
            "{} on {} ({})",
            snapshot.filesystem, snapshot.disk_device, snapshot.disk_model
        ),
    );
    row(
        "cgroup v2 / perf paranoid",
        &format!(
            "{} / {}",
            if snapshot.cgroup_v2 {
                "available"
            } else {
                "unavailable"
            },
            snapshot
                .perf_event_paranoid
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string())
        ),
    );
    println!();
    flush();
}

pub fn doctor(report: &DoctorReport) {
    println!("{}", "Doctor".yellow().bold());
    row(
        "Filesystem",
        if report.working_directory.is_available() {
            "writable"
        } else {
            "not writable"
        },
    );
    row(
        "TCP / UDS bind",
        &format!(
            "{} / {}",
            pass_label(report.tcp_bind.is_available()),
            pass_label(report.uds_bind.is_available())
        ),
    );
    row(
        "Disk available / required",
        &format!(
            "{} / {}",
            format_storage_bytes(report.disk_available_bytes),
            format_storage_bytes(report.disk_required_bytes)
        ),
    );
    for tool in &report.required_tools {
        row(&format!("Tool {}", tool.name), pass_label(tool.available));
    }
    println!();
    flush();
}

fn pass_label(passed: bool) -> &'static str {
    if passed { "available" } else { "unavailable" }
}

fn optional_switch(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "on",
        Some(false) => "off",
        None => "unknown",
    }
}

pub fn suite(name: &str, scenarios: usize, authoritative: bool, tier: &str, output: &Path) {
    println!();
    println!("{}", "Suite".yellow().bold());
    row("Name", name);
    row(
        "Evidence",
        if authoritative {
            "authoritative campaign"
        } else if tier.contains("smoke") {
            "smoke only, not publishable"
        } else {
            "local campaign, not publishable"
        },
    );
    row("Scenarios", &scenarios.to_string());
    row("Results", &output.display().to_string());
    println!();
    flush();
}

pub fn scenario(index: usize, total: usize, scenario: &Scenario) {
    println!(
        "{} {} {}",
        format!("[{index}/{total}]").bright_cyan().bold(),
        scenario.name.white().bold(),
        format!(
            "{} / {} / {}",
            scenario.layer, scenario.driver, scenario.arm
        )
        .bright_black()
    );
    if scenario.driver == "rust_startup" {
        println!(
            "    {} payload, {} partition(s), four one-shot client lifecycle boundaries",
            format_bytes(scenario.payload_bytes),
            scenario.partitions
        );
    } else if scenario.layer == BenchmarkLayer::L7 {
        println!(
            "    {} payload, {} backlog record(s), one fault and recovery lifecycle, {}s convergence timeout",
            format_bytes(scenario.payload_bytes),
            scenario.operations,
            scenario.duration_seconds
        );
    } else if scenario.layer == BenchmarkLayer::L1 {
        println!(
            "    {} payload, batch {}, {} partition(s), {} producer(s), {} configured batch target",
            format_bytes(scenario.payload_bytes),
            scenario.batch_size,
            scenario.partitions,
            scenario.producers,
            scenario.operations
        );
    } else if scenario.driver.parse::<StreamingProducerPath>().is_ok() {
        let arms = timed_arm_count(scenario);
        println!(
            "    {} payload, batch {}, {} partition(s), {} producer lane(s), {} timed arm(s), {}s total timed, {}s total warmup",
            format_bytes(scenario.payload_bytes),
            scenario.batch_size,
            scenario.partitions,
            scenario.producers,
            arms,
            scenario.duration_seconds * arms,
            scenario.warmup_seconds * arms
        );
    } else if scenario.driver.parse::<StreamingConsumerPath>().is_ok() {
        println!(
            "    {} payload, poll batch {}, {} partition(s), {} consumer lane(s), {} preloaded records per arm, 2 timed drains",
            format_bytes(scenario.payload_bytes),
            scenario.batch_size,
            scenario.partitions,
            scenario.consumers,
            scenario.operations
        );
    } else {
        let arms = timed_arm_count(scenario);
        let timed_seconds = scenario.duration_seconds * arms;
        let warmup_seconds = scenario.warmup_seconds * arms;
        println!(
            "    {} payload, batch {}, {} partition(s), {} producer(s), {} consumer(s), {} timed arm(s), {}s total timed, {}s total warmup",
            format_bytes(scenario.payload_bytes),
            scenario.batch_size,
            scenario.partitions,
            scenario.producers,
            scenario.consumers,
            arms,
            timed_seconds,
            warmup_seconds
        );
    }
    flush();
}

fn format_cpu_set(cpus: &[u32]) -> String {
    let mut ranges = Vec::new();
    let Some((&first, rest)) = cpus.split_first() else {
        return "0 logical".to_owned();
    };
    let mut start = first;
    let mut end = first;
    for &cpu in rest {
        if cpu == end.saturating_add(1) {
            end = cpu;
            continue;
        }
        ranges.push(format_cpu_range(start, end));
        start = cpu;
        end = cpu;
    }
    ranges.push(format_cpu_range(start, end));
    format!("{} logical ({})", cpus.len(), ranges.join(","))
}

fn format_cpu_range(start: u32, end: u32) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{start}-{end}")
    }
}

pub fn repetition_started(repetition: u32, total: u32, run_directory: &Path) {
    println!(
        "  {} repetition {}/{}  fresh native services",
        "→".bright_cyan(),
        repetition + 1,
        total
    );
    println!(
        "    {} {}",
        "Logs".bright_black(),
        run_directory.join("services").display()
    );
    println!(
        "    {}",
        "Terminal output pauses during the timed path to protect the measurement.".bright_black()
    );
    flush();
}

pub fn pair(pair: &DirectPairSummary, scenario: &Scenario) {
    let boundary = pair
        .raw
        .configuration
        .get("latency_boundary")
        .and_then(serde_json::Value::as_str)
        .map_or("Declared operation boundary", format_boundary);
    let lanes = active_lanes(scenario);
    println!();
    println!(
        "    {}   {boundary} · aggregate across {lanes} lane(s)",
        "Results".bold()
    );
    println!();
    println!(
        "      {:<10} {:>16} {:>16} {:>14} {:>12}",
        "Arm".bright_black(),
        "Total".bright_black(),
        "Average/lane".bright_black(),
        "Primary p99".bright_black(),
        "Correctness".bright_black()
    );
    print_pair_arm("Raw Iggy", &pair.raw, lanes, true);
    print_pair_arm("Laser", &pair.laser, lanes, false);
    println!();
    flush();
}

fn print_pair_arm(
    name: &str,
    arm: &laser_bench::streaming::StreamingArmSummary,
    lanes: u32,
    raw: bool,
) {
    let label = if raw {
        name.green().bold()
    } else {
        name.bright_cyan().bold()
    };
    let correctness = correctness_badge(&arm.outcomes);
    println!(
        "      {:<10} {:>12.0} r/s {:>12.0} r/s {:>14} {:>12}",
        label,
        arm.records_per_second,
        arm.records_per_second / f64::from(lanes),
        format_duration_ns(arm.primary_p99_ns),
        correctness
    );
}

fn correctness_badge(outcomes: &laser_bench::report::OutcomeCounts) -> colored::ColoredString {
    let defects = correctness_defects(outcomes);
    if defects.is_empty() {
        "clean".green()
    } else {
        defects.join(" ").red().bold()
    }
}

fn correctness_defects(outcomes: &laser_bench::report::OutcomeCounts) -> Vec<String> {
    let mut defects = Vec::new();
    for (name, value) in [
        ("dup", outcomes.duplicates),
        ("gap", outcomes.gaps),
        ("order", outcomes.ordering_violations),
        ("checksum", outcomes.checksum_failures),
    ] {
        if value != 0 {
            defects.push(format!("{name}={value}"));
        }
    }
    defects
}

fn format_boundary(boundary: &str) -> &str {
    match boundary {
        "publish_request_to_acknowledgement" => "Publish request to acknowledgement",
        "enqueue_to_background_acknowledgement" => "Enqueue to background acknowledgement",
        "enqueue_to_batch_acknowledgement" => "Enqueue to batch acknowledgement",
        "poll_dispatch_to_record_delivery" => "Poll dispatch to record delivery",
        "full_cursor_drain" => "Full cursor drain",
        "producer_dispatch_to_consumer_receive" => "Producer dispatch to consumer receive",
        _ => "Declared operation boundary",
    }
}

fn timed_arm_count(scenario: &Scenario) -> u64 {
    if scenario.driver.parse::<StreamingConsumerPath>().is_ok() {
        return 2;
    }
    match scenario.driver.as_str() {
        "agdx_publish" | "mcp_triage" => 3,
        "mcp_bridge" | "stream_direct" | "stream_direct_aa" | "stream_end_to_end" => 2,
        _ => 1,
    }
}

fn active_lanes(scenario: &Scenario) -> u32 {
    if scenario.driver.parse::<StreamingConsumerPath>().is_ok() {
        scenario.consumers
    } else {
        scenario.producers
    }
    .max(1)
}

pub fn report(report: &RunReport, scenario: &Scenario) {
    let lanes = active_lanes(scenario);
    if let Some(operation) = report
        .extra
        .get("managed_summary")
        .and_then(|summary| summary.get("operation"))
    {
        let arm = operation
            .get("arm")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&report.arm);
        let throughput = operation
            .get("operations_per_second")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default();
        let p99 = operation
            .get("primary_p99_ns")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        println!(
            "    {:<24} {:>13.0} total op/s  {:>10.0} avg/lane  p99 {:>12}",
            arm.bright_cyan().bold(),
            throughput,
            throughput / f64::from(lanes),
            format_duration_ns(p99)
        );
    }
    if let Some(summary) = report.extra.get("agdx_summary") {
        print_arm_summaries(summary, lanes);
    }
    if let Some(summary) = report.extra.get("mcp_summary") {
        print_arm_summaries(summary, lanes);
    }
    if let Some(summary) = report.extra.get("mcp_triage") {
        print_named_arm_summary("agdx", summary.get("agdx"), lanes);
        print_named_arm_summary("minimal_mcp", summary.get("minimal_mcp"), lanes);
        print_named_arm_summary(
            "guarantee_matched_mcp",
            summary.get("guarantee_matched_mcp"),
            lanes,
        );
        print_mcp_bytes(summary);
    }
    if let Some(review) = report.extra.get("mcp_reviewer_bundle") {
        let path = review
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("mcp-reviewer-bundle.json");
        let status = review
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        println!(
            "    Review    {}  {}",
            status.bright_yellow().bold(),
            path.bright_black()
        );
    }
    if let Some(startup) = report.extra.get("rust_client_startup") {
        print_rust_client_startup(startup);
    }
    if let Some(recovery) = report.extra.get("recovery_summary") {
        print_recovery_summary(recovery);
    }
    println!();
    println!(
        "    {}  {} successful / {} offered · {} failed · {} timed out · {} missed{}",
        "Outcomes".bold(),
        report.outcomes.successful.to_string().green(),
        report.outcomes.offered,
        colored_count(report.outcomes.failed),
        colored_count(report.outcomes.timed_out),
        colored_count(report.outcomes.missed),
        if report.outcomes.late_arrivals != 0 {
            format!(" · {} late (explained)", report.outcomes.late_arrivals)
        } else {
            String::new()
        }
    );
    let defects = correctness_defects(&report.outcomes);
    if report.analysis.valid {
        println!("    {}   correctness clean", "VALID".green().bold());
    } else {
        let detail = if defects.is_empty() {
            String::new()
        } else {
            format!(" · {}", defects.join(" · "))
        };
        println!(
            "    {} {}{detail}",
            "INVALID".red().bold(),
            report
                .analysis
                .invalidation_reason
                .as_deref()
                .unwrap_or("report validation failed"),
        );
    }
    flush();
}

fn colored_count(value: u64) -> colored::ColoredString {
    if value == 0 {
        value.to_string().normal()
    } else {
        value.to_string().red().bold()
    }
}

fn print_rust_client_startup(summary: &serde_json::Value) {
    let duration = |field| {
        summary
            .get(field)
            .and_then(serde_json::Value::as_u64)
            .map_or_else(|| "n/a".to_owned(), format_duration_ns)
    };
    println!(
        "    Rust cold  connect {}  topology {}  first ack {}  warmed ack {}",
        duration("connect_and_negotiate_ns").bright_cyan(),
        duration("topology_setup_ns").bright_cyan(),
        duration("first_publish_ack_ns").bright_cyan(),
        duration("warmed_publish_ack_ns").bright_cyan()
    );
}

fn print_recovery_summary(summary: &serde_json::Value) {
    let driver = summary
        .get("driver")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("recovery");
    let recovered = summary
        .get("recovered_records")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let timeline = summary.get("timeline");
    let fault_ns = timeline
        .and_then(|value| value.get("fault_injected_ns"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let converged_ns = timeline
        .and_then(|value| value.get("converged_ns"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let convergence = format_duration_ns(converged_ns.saturating_sub(fault_ns));
    let catch_up = summary
        .get("catch_up_records_per_second")
        .and_then(serde_json::Value::as_f64)
        .map(|value| format!("  catch-up {value:.0} records/s"))
        .unwrap_or_default();
    println!(
        "    Recovery   {}  {} records  convergence {}{}",
        driver.bright_cyan().bold(),
        recovered,
        convergence.bright_cyan(),
        catch_up
    );
}

fn print_mcp_bytes(summary: &serde_json::Value) {
    let Some(m6) = summary
        .get("byte_accounting")
        .and_then(|accounting| accounting.get("m6"))
    else {
        return;
    };
    let application = m6
        .get("application_ratio_agdx_over_minimal_mcp")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    let network = m6
        .get("network_ratio_agdx_over_minimal_mcp")
        .and_then(serde_json::Value::as_f64);
    let valid = m6
        .get("measurement_valid")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let passed = m6
        .get("passed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let verdict = if !valid {
        "INCOMPLETE".red().bold()
    } else if passed {
        "PASS".green().bold()
    } else {
        "FALSIFIED".bright_yellow().bold()
    };
    println!(
        "    M6 bytes  application {:>7.4}x  TCP payload {:>7}  {}",
        application,
        network.map_or_else(|| "n/a".to_owned(), |ratio| format!("{ratio:.4}x")),
        verdict
    );
}

fn print_arm_summaries(summary: &serde_json::Value, lanes: u32) {
    let Some(arms) = summary.as_object() else {
        return;
    };
    for (name, arm) in arms {
        let Some(throughput) = arm
            .get("operations_per_second")
            .and_then(serde_json::Value::as_f64)
        else {
            continue;
        };
        let p99 = arm
            .get("primary_p99_ns")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        println!(
            "    {:<24} {:>13.0} total op/s  {:>10.0} avg/lane  p99 {:>12}",
            name.bright_cyan().bold(),
            throughput,
            throughput / f64::from(lanes),
            format_duration_ns(p99)
        );
    }
}

fn print_named_arm_summary(name: &str, summary: Option<&serde_json::Value>, lanes: u32) {
    let Some(summary) = summary else {
        return;
    };
    if let Some(arm) = summary
        .get("request_reply")
        .or_else(|| summary.get("streamable_http"))
    {
        print_single_arm_summary(name, arm, lanes);
    }
}

fn print_single_arm_summary(name: &str, arm: &serde_json::Value, lanes: u32) {
    let Some(throughput) = arm
        .get("operations_per_second")
        .and_then(serde_json::Value::as_f64)
    else {
        return;
    };
    let p99 = arm
        .get("primary_p99_ns")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    println!(
        "    {:<24} {:>13.0} total op/s  {:>10.0} avg/lane  p99 {:>12}",
        name.bright_cyan().bold(),
        throughput,
        throughput / f64::from(lanes),
        format_duration_ns(p99)
    );
}

pub fn repetition_completed(
    repetition: u32,
    total: u32,
    elapsed: Duration,
    path: &Path,
    valid: bool,
) {
    let status = if valid {
        "✓".green().bold()
    } else {
        "✗".red().bold()
    };
    println!(
        "  {} repetition {}/{} complete in {:.2}s",
        status,
        repetition + 1,
        total,
        elapsed.as_secs_f64()
    );
    println!("    {} {}", "Evidence".bright_black(), path.display());
    println!();
    flush();
}

pub fn c2(analysis: &PairedAnalysis, passed: bool, authoritative: bool, smoke: bool) {
    let verdict = if authoritative && passed {
        "PASS".green().bold()
    } else if authoritative {
        "FAIL".red().bold()
    } else if smoke && passed {
        "SMOKE PASS".yellow().bold()
    } else if smoke {
        "SMOKE FAIL".yellow().bold()
    } else if passed {
        "LOCAL PASS".bright_cyan().bold()
    } else {
        "LOCAL FAIL".red().bold()
    };
    println!(
        "  {} C2  throughput lower 95% {:.4}  latency upper 95% {:.4}",
        verdict, analysis.throughput_ratio.lower_95, analysis.latency_ratio.upper_95
    );
    if !authoritative {
        println!(
            "    {}",
            "Diagnostic only. Use a registered campaign for a publishable C2 verdict."
                .bright_black()
        );
    }
    flush();
}

pub fn paired(analysis: &PairedAnalysis) {
    println!(
        "  {} paired ratios  throughput {:.4}  latency {:.4}",
        "✓".green().bold(),
        analysis.throughput_ratio.estimate,
        analysis.latency_ratio.estimate
    );
    flush();
}

pub fn calibration(analysis: &PairedAnalysis, passed: bool, authoritative: bool) {
    let verdict = if passed {
        "STABLE".green().bold()
    } else {
        "UNSTABLE".red().bold()
    };
    println!(
        "  {} A/A  throughput lower 95% {:.4}  latency upper 95% {:.4}",
        verdict, analysis.throughput_ratio.lower_95, analysis.latency_ratio.upper_95
    );
    if !authoritative {
        println!(
            "    {}",
            "Diagnostic only. An authoritative C2 campaign requires this calibration to pass."
                .bright_black()
        );
    }
    flush();
}

#[allow(clippy::cast_precision_loss)]
pub fn point_estimate(pair: &DirectPairSummary) {
    let throughput = pair.laser.records_per_second / pair.raw.records_per_second;
    let latency = pair.laser.primary_p99_ns as f64 / pair.raw.primary_p99_ns as f64;
    println!(
        "  {} point estimate  throughput {:.4}  latency {:.4}",
        "1 REP".yellow().bold(),
        throughput,
        latency
    );
    println!(
        "    {}",
        "No confidence interval or performance verdict. Increase repetitions for comparison evidence."
            .bright_black()
    );
    flush();
}

pub fn complete(output: &Path, invalid_repetitions: usize, invalid_analyses: usize) {
    println!();
    if invalid_repetitions == 0 && invalid_analyses == 0 {
        println!("{}", "Benchmark suite complete".green().bold());
    } else {
        println!(
            "{}",
            "Benchmark suite completed with invalid evidence"
                .red()
                .bold()
        );
        row("Invalid", &invalid_repetitions.to_string());
        row("Failed gates", &invalid_analyses.to_string());
    }
    row("Results", &output.display().to_string());
    row(
        "Index",
        &output.join("suite-index.json").display().to_string(),
    );
    row(
        "Report",
        &output.join("analysis/report.html").display().to_string(),
    );
    row(
        "CSV",
        &output.join("analysis/results.csv").display().to_string(),
    );
    println!();
    flush();
}

pub fn success(message: &str) {
    println!("{} {message}", "✓".green().bold());
    flush();
}

pub fn failure(message: &str) {
    eprintln!("{} {message}", "✗".red().bold());
}

fn row(label: &str, value: &str) {
    println!("  {:<12} {}", label.bright_black(), value.white());
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 1_024 {
        format!("{bytes} B")
    } else if bytes < 1_048_576 {
        format_scaled_bytes(bytes, 1_024, "KiB")
    } else {
        format_scaled_bytes(bytes, 1_048_576, "MiB")
    }
}

fn format_scaled_bytes(bytes: usize, divisor: usize, unit: &str) -> String {
    let whole = bytes / divisor;
    let hundredths = (bytes % divisor) * 100 / divisor;
    format!("{whole}.{hundredths:02} {unit}")
}

fn format_storage_bytes(bytes: u64) -> String {
    const MIB: u64 = 1_048_576;
    const GIB: u64 = 1_073_741_824;
    if bytes >= GIB {
        format_scaled_storage(bytes, GIB, "GiB")
    } else {
        format_scaled_storage(bytes, MIB, "MiB")
    }
}

fn format_scaled_storage(bytes: u64, divisor: u64, unit: &str) -> String {
    let whole = bytes / divisor;
    let hundredths = (bytes % divisor) * 100 / divisor;
    format!("{whole}.{hundredths:02} {unit}")
}

fn format_duration_ns(nanos: u64) -> String {
    let duration = Duration::from_nanos(nanos);
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", duration.as_secs_f64() * 1_000_000.0)
    } else {
        format!("{:.2} ms", duration.as_secs_f64() * 1_000.0)
    }
}

fn flush() {
    let _ = io::stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_contiguous_logical_cpus_when_formatted_then_should_use_one_range() {
        assert_eq!(
            format_cpu_set(&(0..32).collect::<Vec<_>>()),
            "32 logical (0-31)"
        );
    }

    #[test]
    fn given_sparse_logical_cpus_when_formatted_then_should_preserve_ranges() {
        assert_eq!(format_cpu_set(&[0, 1, 4, 6, 7]), "5 logical (0-1,4,6-7)");
    }
}
