use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::BenchError;
use crate::analysis::{C2Gate, PairedAnalysis};
use crate::binary::sha256_file;
use crate::engine::LoadTimeSeriesPoint;
use crate::histogram::{read_sidecar, verify_sidecar_at};
use crate::report::{OutcomeCounts, RunReport};

const ANALYSIS_DIRECTORY: &str = "analysis";
const LASERDATA_LOGO: &str = include_str!("../../assets/laserdata.svg");
const REPORT_SCRIPT: &str = include_str!("../../assets/report.js");
const REPORT_STYLE: &str = include_str!("../../assets/report.css");
const REPORT_THEME_INIT: &str = r"
const preferredTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
let savedTheme = null
try { savedTheme = window.localStorage.getItem('laser-bench-theme') } catch (_) {}
document.documentElement.dataset.theme = savedTheme || preferredTheme
";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AnalysisRow {
    pub scenario: String,
    pub arm: String,
    pub repetition: u32,
    pub offered_rate: Option<u64>,
    pub valid: bool,
    pub publishable: bool,
    pub operations_per_second: Option<f64>,
    pub primary_p99_ns: Option<u64>,
    pub outcomes: OutcomeCounts,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PairedAnalysisRow {
    pub scenario: String,
    pub latency_boundary: String,
    pub analysis: PairedAnalysis,
    pub load_accepted: bool,
    pub gate: Option<C2Gate>,
    pub gate_kind: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CurvePoint {
    pub scenario: String,
    pub arm: String,
    pub offered_rate: u64,
    pub achieved_operations_per_second: f64,
    pub primary_p99_ns: u64,
    pub repetitions: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WorkloadSeriesRow {
    pub scenario: String,
    pub arm: String,
    pub repetition: u32,
    pub second: u64,
    pub outcomes: OutcomeCounts,
    pub max_in_flight: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LatencyDistributionRow {
    pub scenario: String,
    pub arm: String,
    pub repetition: u32,
    pub samples: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AnalysisFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SuiteAnalysis {
    pub schema_version: u32,
    pub suite: String,
    pub authoritative: bool,
    pub reports: usize,
    pub valid_reports: usize,
    pub publishable_reports: usize,
    pub rows: Vec<AnalysisRow>,
    pub paired_analyses: Vec<PairedAnalysisRow>,
    pub latency_throughput_curves: Vec<CurvePoint>,
    pub workload_time_series: Vec<WorkloadSeriesRow>,
    pub latency_distributions: Vec<LatencyDistributionRow>,
    pub suite_index: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AnalysisManifest {
    pub schema_version: u32,
    pub files: Vec<AnalysisFile>,
}

/// Render deterministic suite analysis files and record their digests.
///
/// # Errors
///
/// Returns an error when the suite index is invalid, output differs from an existing immutable file, or a file cannot be written or hashed.
pub fn write_suite_analysis(
    root: &Path,
    suite: &str,
    authoritative: bool,
    reports: &[RunReport],
) -> Result<AnalysisManifest, BenchError> {
    let suite_index_path = root.join("suite-index.json");
    let suite_index = read_json(&suite_index_path)?;
    let rows = reports.iter().flat_map(report_rows).collect::<Vec<_>>();
    let paired_analyses = paired_analysis_rows(&suite_index)?;
    let latency_throughput_curves = curve_points(&rows);
    let workload_time_series = reports
        .iter()
        .flat_map(report_workload_series)
        .collect::<Vec<_>>();
    let latency_distributions = histogram_rows(root, reports)?;
    let analysis = SuiteAnalysis {
        schema_version: 1,
        suite: suite.to_owned(),
        authoritative,
        reports: reports.len(),
        valid_reports: reports
            .iter()
            .filter(|report| report.analysis.valid)
            .count(),
        publishable_reports: reports
            .iter()
            .filter(|report| report.analysis.publishable)
            .count(),
        rows,
        paired_analyses,
        latency_throughput_curves,
        workload_time_series,
        latency_distributions,
        suite_index,
    };
    let directory = root.join(ANALYSIS_DIRECTORY);
    fs::create_dir_all(&directory).map_err(|source| BenchError::Write {
        path: directory.clone(),
        source,
    })?;
    let analysis_path = directory.join("analysis.json");
    let csv_path = directory.join("results.csv");
    let paired_csv_path = directory.join("paired-effects.csv");
    let curves_csv_path = directory.join("latency-throughput-curves.csv");
    let workload_csv_path = directory.join("workload-timeseries.csv");
    let distributions_csv_path = directory.join("latency-distributions.csv");
    let html_path = directory.join("report.html");
    write_immutable(&analysis_path, &serde_json::to_vec_pretty(&analysis)?)?;
    write_immutable(&csv_path, render_csv(&analysis.rows).as_bytes())?;
    write_immutable(
        &paired_csv_path,
        render_paired_csv(&analysis.paired_analyses).as_bytes(),
    )?;
    write_immutable(
        &curves_csv_path,
        render_curves_csv(&analysis.latency_throughput_curves).as_bytes(),
    )?;
    write_immutable(
        &workload_csv_path,
        render_workload_csv(&analysis.workload_time_series).as_bytes(),
    )?;
    write_immutable(
        &distributions_csv_path,
        render_distributions_csv(&analysis.latency_distributions).as_bytes(),
    )?;
    write_immutable(&html_path, render_html(&analysis).as_bytes())?;
    let mut files = [
        analysis_path,
        csv_path,
        paired_csv_path,
        curves_csv_path,
        workload_csv_path,
        distributions_csv_path,
        html_path,
    ]
    .into_iter()
    .map(|path| analysis_file(root, &path))
    .collect::<Result<Vec<_>, BenchError>>()?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = AnalysisManifest {
        schema_version: 1,
        files,
    };
    write_immutable(
        &directory.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

/// Verify the deterministic analysis file set without rewriting it.
///
/// # Errors
///
/// Returns an error when the manifest or any referenced file is absent, unsafe, or has a different byte length or digest.
pub fn verify_suite_analysis(root: &Path) -> Result<AnalysisManifest, BenchError> {
    let manifest_path = root.join(ANALYSIS_DIRECTORY).join("manifest.json");
    let manifest: AnalysisManifest = serde_json::from_value(read_json(&manifest_path)?)?;
    if manifest.schema_version != 1 {
        return Err(BenchError::Invalid(format!(
            "unsupported suite analysis schema version {}",
            manifest.schema_version
        )));
    }
    for file in &manifest.files {
        let path = safe_path(root, &file.path)?;
        let metadata = fs::metadata(&path).map_err(|source| BenchError::Read {
            path: path.clone(),
            source,
        })?;
        if metadata.len() != file.bytes || sha256_file(&path)? != file.sha256 {
            return Err(BenchError::Invalid(format!(
                "suite analysis file `{}` failed integrity validation",
                file.path
            )));
        }
    }
    Ok(manifest)
}

fn report_rows(report: &RunReport) -> Vec<AnalysisRow> {
    let mut rows = Vec::new();
    for key in [
        "pair_summary",
        "agdx_summary",
        "mcp_summary",
        "mcp_triage",
        "managed_summary",
        "recovery_summary",
    ] {
        if let Some(value) = report.extra.get(key) {
            collect_rows(report, key, value, &mut rows);
        }
    }
    if let Some(startup) = report.extra.get("rust_client_startup") {
        for (arm, field) in [
            ("connect_and_negotiate", "connect_and_negotiate_ns"),
            ("topology_setup", "topology_setup_ns"),
            ("first_publish_ack", "first_publish_ack_ns"),
            ("warmed_publish_ack", "warmed_publish_ack_ns"),
        ] {
            rows.push(AnalysisRow {
                scenario: report.scenario.clone(),
                arm: arm.to_owned(),
                repetition: report.repetition,
                offered_rate: report.workload.offered_rate,
                valid: report.analysis.valid,
                publishable: report.analysis.publishable,
                operations_per_second: None,
                primary_p99_ns: startup.get(field).and_then(Value::as_u64),
                outcomes: report.outcomes.clone(),
            });
        }
    }
    if rows.is_empty() {
        rows.push(AnalysisRow {
            scenario: report.scenario.clone(),
            arm: report.arm.clone(),
            repetition: report.repetition,
            offered_rate: report.workload.offered_rate,
            valid: report.analysis.valid,
            publishable: report.analysis.publishable,
            operations_per_second: None,
            primary_p99_ns: None,
            outcomes: report.outcomes.clone(),
        });
    }
    rows
}

fn report_workload_series(report: &RunReport) -> Vec<WorkloadSeriesRow> {
    let mut rows = Vec::new();
    for key in [
        "pair_summary",
        "agdx_summary",
        "mcp_summary",
        "mcp_triage",
        "managed_summary",
        "recovery_summary",
    ] {
        if let Some(value) = report.extra.get(key) {
            collect_workload_series(report, key, value, &mut rows);
        }
    }
    rows
}

fn histogram_rows(
    root: &Path,
    reports: &[RunReport],
) -> Result<Vec<LatencyDistributionRow>, BenchError> {
    let mut rows = Vec::new();
    for report in reports {
        let report_directory = root
            .join(&report.scenario)
            .join(format!("repetition-{:03}", report.repetition));
        for reference in &report.histograms {
            verify_sidecar_at(&report_directory, reference)?;
            let path = safe_path(&report_directory, &reference.path)?;
            let histogram = read_sidecar(&path)?;
            if histogram.len() != reference.samples {
                return Err(BenchError::Invalid(format!(
                    "histogram sample count mismatch for `{}`",
                    path.display()
                )));
            }
            rows.push(LatencyDistributionRow {
                scenario: report.scenario.clone(),
                arm: reference.class.clone(),
                repetition: report.repetition,
                samples: histogram.len(),
                p50_ns: histogram.value_at_quantile(0.5),
                p90_ns: histogram.value_at_quantile(0.9),
                p99_ns: histogram.value_at_quantile(0.99),
                p999_ns: (histogram.len() >= 100_000).then(|| histogram.value_at_quantile(0.999)),
            });
        }
    }
    rows.sort_by(|left, right| {
        (&left.scenario, left.repetition, &left.arm).cmp(&(
            &right.scenario,
            right.repetition,
            &right.arm,
        ))
    });
    Ok(rows)
}

fn collect_workload_series(
    report: &RunReport,
    path: &str,
    value: &Value,
    rows: &mut Vec<WorkloadSeriesRow>,
) {
    let Some(object) = value.as_object() else {
        return;
    };
    if let Some(points) = object.get("time_series").and_then(Value::as_array) {
        rows.extend(points.iter().filter_map(|value| {
            let point = serde_json::from_value::<LoadTimeSeriesPoint>(value.clone()).ok()?;
            Some(WorkloadSeriesRow {
                scenario: report.scenario.clone(),
                arm: path.to_owned(),
                repetition: report.repetition,
                second: point.second,
                outcomes: point.outcomes,
                max_in_flight: point.max_in_flight,
            })
        }));
        return;
    }
    for (name, child) in object {
        if matches!(
            name.as_str(),
            "configuration"
                | "outcomes"
                | "processes"
                | "network"
                | "byte_accounting"
                | "shared_trace"
        ) {
            continue;
        }
        collect_workload_series(report, &format!("{path}.{name}"), child, rows);
    }
}

fn collect_rows(report: &RunReport, path: &str, value: &Value, rows: &mut Vec<AnalysisRow>) {
    let Some(object) = value.as_object() else {
        return;
    };
    let throughput = object
        .get("records_per_second")
        .or_else(|| object.get("operations_per_second"))
        .or_else(|| object.get("catch_up_records_per_second"))
        .and_then(Value::as_f64);
    let primary_p99_ns = object.get("primary_p99_ns").and_then(Value::as_u64);
    if throughput.is_some() || primary_p99_ns.is_some() {
        let outcomes = object
            .get("outcomes")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_else(|| report.outcomes.clone());
        rows.push(AnalysisRow {
            scenario: report.scenario.clone(),
            arm: path.to_owned(),
            repetition: report.repetition,
            offered_rate: report.workload.offered_rate,
            valid: report.analysis.valid,
            publishable: report.analysis.publishable,
            operations_per_second: throughput,
            primary_p99_ns,
            outcomes,
        });
        return;
    }
    for (name, child) in object {
        if matches!(
            name.as_str(),
            "configuration"
                | "outcomes"
                | "processes"
                | "network"
                | "byte_accounting"
                | "shared_trace"
                | "time_series"
        ) {
            continue;
        }
        collect_rows(report, &format!("{path}.{name}"), child, rows);
    }
}

fn paired_analysis_rows(index: &Value) -> Result<Vec<PairedAnalysisRow>, BenchError> {
    let scenarios = index
        .get("scenarios")
        .and_then(Value::as_array)
        .ok_or_else(|| BenchError::Invalid("suite index has no scenario array".to_owned()))?;
    let mut rows = Vec::new();
    for scenario in scenarios {
        let Some(paired) = scenario
            .get("paired_analysis")
            .filter(|value| !value.is_null())
        else {
            continue;
        };
        let name = scenario
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BenchError::Invalid("paired analysis has no scenario name".to_owned())
            })?;
        let analysis = paired.get("analysis").cloned().ok_or_else(|| {
            BenchError::Invalid(format!(
                "paired analysis for `{name}` has no effect estimates"
            ))
        })?;
        let analysis = serde_json::from_value::<PairedAnalysis>(analysis)?;
        let latency_boundary = paired
            .get("latency_boundary")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BenchError::Invalid(format!(
                    "paired analysis for `{name}` has no latency boundary"
                ))
            })?;
        let load_accepted = paired
            .get("load_accepted")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                BenchError::Invalid(format!("paired analysis for `{name}` has no load verdict"))
            })?;
        let (gate_kind, gate) =
            if let Some(value) = paired.get("c2").filter(|value| !value.is_null()) {
                (
                    Some("C2".to_owned()),
                    Some(serde_json::from_value::<C2Gate>(value.clone())?),
                )
            } else if let Some(value) = paired
                .get("aa_calibration")
                .filter(|value| !value.is_null())
            {
                (
                    Some("A/A".to_owned()),
                    Some(serde_json::from_value::<C2Gate>(value.clone())?),
                )
            } else {
                (None, None)
            };
        rows.push(PairedAnalysisRow {
            scenario: name.to_owned(),
            latency_boundary: latency_boundary.to_owned(),
            analysis,
            load_accepted,
            gate,
            gate_kind,
        });
    }
    Ok(rows)
}

fn curve_points(rows: &[AnalysisRow]) -> Vec<CurvePoint> {
    let mut samples = BTreeMap::<(String, String, u64), Vec<(f64, u64)>>::new();
    for row in rows.iter().filter(|row| row.valid) {
        let (Some(offered_rate), Some(throughput), Some(p99)) = (
            row.offered_rate,
            row.operations_per_second,
            row.primary_p99_ns,
        ) else {
            continue;
        };
        let scenario = sweep_name(&row.scenario, offered_rate);
        samples
            .entry((scenario, row.arm.clone(), offered_rate))
            .or_default()
            .push((throughput, p99));
    }
    samples
        .into_iter()
        .map(|((scenario, arm, offered_rate), samples)| {
            let mut throughput = samples.iter().map(|sample| sample.0).collect::<Vec<_>>();
            let mut p99 = samples.iter().map(|sample| sample.1).collect::<Vec<_>>();
            CurvePoint {
                scenario,
                arm,
                offered_rate,
                achieved_operations_per_second: median_f64(&mut throughput),
                primary_p99_ns: median_u64(&mut p99),
                repetitions: samples.len(),
            }
        })
        .collect()
}

fn sweep_name(scenario: &str, offered_rate: u64) -> String {
    scenario
        .strip_suffix(&format!("-rate-{offered_rate}"))
        .unwrap_or(scenario)
        .to_owned()
}

fn median_f64(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn median_u64(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        values[middle - 1].midpoint(values[middle])
    } else {
        values[middle]
    }
}

fn render_csv(rows: &[AnalysisRow]) -> String {
    let mut csv = String::from(
        "scenario,arm,repetition,offered_rate,valid,publishable,operations_per_second,primary_p99_ns,offered,successful,failed,timed_out,missed\n",
    );
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&row.scenario),
            csv_field(&row.arm),
            row.repetition,
            optional_u64(row.offered_rate),
            row.valid,
            row.publishable,
            optional_f64(row.operations_per_second),
            optional_u64(row.primary_p99_ns),
            row.outcomes.offered,
            row.outcomes.successful,
            row.outcomes.failed,
            row.outcomes.timed_out,
            row.outcomes.missed,
        )
        .expect("writing to a string should be infallible");
    }
    csv
}

fn render_workload_csv(rows: &[WorkloadSeriesRow]) -> String {
    let mut csv = String::from(
        "scenario,arm,repetition,second,offered,dispatched,completed,successful,failed,timed_out,missed,max_in_flight\n",
    );
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&row.scenario),
            csv_field(&row.arm),
            row.repetition,
            row.second,
            row.outcomes.offered,
            row.outcomes.dispatched,
            row.outcomes.completed,
            row.outcomes.successful,
            row.outcomes.failed,
            row.outcomes.timed_out,
            row.outcomes.missed,
            row.max_in_flight,
        )
        .expect("writing to a string should be infallible");
    }
    csv
}

fn render_distributions_csv(rows: &[LatencyDistributionRow]) -> String {
    let mut csv = String::from("scenario,arm,repetition,samples,p50_ns,p90_ns,p99_ns,p999_ns\n");
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{}",
            csv_field(&row.scenario),
            csv_field(&row.arm),
            row.repetition,
            row.samples,
            row.p50_ns,
            row.p90_ns,
            row.p99_ns,
            optional_u64(row.p999_ns),
        )
        .expect("writing to a string should be infallible");
    }
    csv
}

fn render_paired_csv(rows: &[PairedAnalysisRow]) -> String {
    let mut csv = String::from(
        "scenario,latency_boundary,pairs,throughput_estimate,throughput_lower_95,throughput_upper_95,latency_estimate,latency_lower_95,latency_upper_95,load_accepted,gate_kind,gate_passed\n",
    );
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{}",
            csv_field(&row.scenario),
            csv_field(&row.latency_boundary),
            row.analysis.pairs,
            row.analysis.throughput_ratio.estimate,
            row.analysis.throughput_ratio.lower_95,
            row.analysis.throughput_ratio.upper_95,
            row.analysis.latency_ratio.estimate,
            row.analysis.latency_ratio.lower_95,
            row.analysis.latency_ratio.upper_95,
            row.load_accepted,
            row.gate_kind.as_deref().map_or_else(String::new, csv_field),
            row.gate
                .map(|gate| gate.passed)
                .map_or_else(String::new, |value| value.to_string()),
        )
        .expect("writing to a string should be infallible");
    }
    csv
}

fn render_curves_csv(points: &[CurvePoint]) -> String {
    let mut csv = String::from(
        "scenario,arm,offered_rate,achieved_operations_per_second,primary_p99_ns,repetitions\n",
    );
    for point in points {
        writeln!(
            csv,
            "{},{},{},{:.6},{},{}",
            csv_field(&point.scenario),
            csv_field(&point.arm),
            point.offered_rate,
            point.achieved_operations_per_second,
            point.primary_p99_ns,
            point.repetitions,
        )
        .expect("writing to a string should be infallible");
    }
    csv
}

fn render_html(analysis: &SuiteAnalysis) -> String {
    let rows = analysis.rows.iter().fold(String::new(), |mut html, row| {
        write!(
            html,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td>{}</td></tr>",
            escape_html(&row.scenario),
            escape_html(&row.arm),
            row.repetition,
            row.offered_rate.map_or_else(|| "closed".to_owned(), |value| value.to_string()),
            row.operations_per_second.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.2}")),
            row.primary_p99_ns.map_or_else(|| "n/a".to_owned(), format_duration),
            if row.valid { "valid" } else { "invalid" },
        )
        .expect("writing to a string should be infallible");
        html
    });
    let paired = render_paired_analyses(&analysis.paired_analyses);
    let curves = render_curves(&analysis.latency_throughput_curves);
    let distributions = render_distributions(&analysis.latency_distributions);
    let navigation = render_navigation(
        !paired.is_empty(),
        !curves.is_empty(),
        !distributions.is_empty(),
    );
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{suite} benchmark analysis</title>
<style>{style}</style>
<script>{theme_init}</script>
</head>
<body>
<main>
<header class="report-header">
<div class="header-tools">{logo}<button class="theme-toggle" id="theme-toggle" type="button">Theme</button></div>
<div class="kicker">Benchmark evidence</div>
<h1>{suite}</h1>
<p class="note">Deterministic analysis of immutable suite evidence. Valid means the correctness and evidence checks passed. Publication remains a separate gate.</p>
{navigation}
</header>
<section class="summary" aria-label="Suite summary">
<div class="card"><div>Reports</div><div class="value">{reports}</div></div>
<div class="card"><div>Valid</div><div class="value">{valid}</div></div>
<div class="card"><div>Publishable</div><div class="value">{publishable}</div></div>
<div class="card"><div>Environment</div><div class="value">{authority}</div></div>
</section>
{paired}{curves}{distributions}
<section id="measured-arms">
<h2>All measured arms</h2>
<div class="scroll"><table><thead><tr><th>Scenario</th><th>Arm</th><th>Rep</th><th class="number">Offered rate</th><th class="number">Throughput</th><th class="number">Primary p99</th><th>Status</th></tr></thead><tbody>{rows}</tbody></table></div>
</section>
<footer>Generated from digest-verified reports and HDR histogram sidecars. No benchmark data is loaded from the network.</footer>
</main>
<script>{script}</script>
</body>
</html>"#,
        suite = escape_html(&analysis.suite),
        style = REPORT_STYLE,
        theme_init = REPORT_THEME_INIT,
        logo = LASERDATA_LOGO,
        navigation = navigation,
        reports = analysis.reports,
        valid = analysis.valid_reports,
        publishable = analysis.publishable_reports,
        authority = if analysis.authoritative {
            "Tier B"
        } else {
            "local"
        },
        script = REPORT_SCRIPT,
    )
}

fn render_navigation(has_paired: bool, has_curves: bool, has_distributions: bool) -> String {
    let mut links = String::new();
    if has_paired {
        links.push_str("<a href=\"#paired-effects\">Paired effects</a>");
    }
    if has_curves {
        links.push_str("<a href=\"#latency-curves\">Load curves</a>");
    }
    if has_distributions {
        links.push_str("<a href=\"#latency-distributions\">Latency</a>");
    }
    links.push_str("<a href=\"#measured-arms\">All arms</a>");
    format!("<nav class=\"section-nav\" aria-label=\"Report sections\">{links}</nav>")
}

#[allow(clippy::cast_precision_loss)]
fn render_distributions(rows: &[LatencyDistributionRow]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let body = rows.iter().fold(String::new(), |mut html, row| {
        let maximum = row.p999_ns.unwrap_or(row.p99_ns).max(1);
        let x = |value: u64| 10.0 + (value as f64 / maximum as f64) * 200.0;
        let p50_x = x(row.p50_ns);
        let p90_x = x(row.p90_ns);
        let p99_x = x(row.p99_ns);
        write!(
            html,
            "<tr><td>{}</td><td>{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td><svg class=\"spark\" viewBox=\"0 0 220 28\" role=\"img\" aria-label=\"Latency percentile distribution\"><line class=\"axis\" x1=\"10\" y1=\"14\" x2=\"210\" y2=\"14\"/><circle class=\"point\" cx=\"{p50_x:.1}\" cy=\"14\" r=\"3\"><title>p50 {}</title></circle><circle class=\"point\" cx=\"{p90_x:.1}\" cy=\"14\" r=\"4\"><title>p90 {}</title></circle><circle class=\"point\" cx=\"{p99_x:.1}\" cy=\"14\" r=\"5\"><title>p99 {}</title></circle></svg></td></tr>",
            escape_html(&row.scenario),
            escape_html(&row.arm),
            row.repetition,
            row.samples,
            format_duration(row.p50_ns),
            format_duration(row.p90_ns),
            format_duration(row.p99_ns),
            row.p999_ns.map_or_else(|| "n/a".to_owned(), format_duration),
            format_duration(row.p50_ns),
            format_duration(row.p90_ns),
            format_duration(row.p99_ns),
        )
        .expect("writing to a string should be infallible");
        html
    });
    format!(
        "<section id=\"latency-distributions\"><h2>Latency distributions</h2><p class=\"note\">These percentiles and inline plots are decoded directly from the digest-verified compressed HDR sidecars. Hover the markers for values. p99.9 appears only with at least 100,000 samples.</p><div class=\"scroll\"><table><thead><tr><th>Scenario</th><th>Histogram</th><th class=\"number\">Rep</th><th class=\"number\">Samples</th><th class=\"number\">p50</th><th class=\"number\">p90</th><th class=\"number\">p99</th><th class=\"number\">p99.9</th><th>Distribution</th></tr></thead><tbody>{body}</tbody></table></div></section>"
    )
}

fn render_paired_analyses(rows: &[PairedAnalysisRow]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let body = rows.iter().fold(String::new(), |mut html, row| {
        let gate = match (&row.gate_kind, row.gate) {
            (Some(kind), Some(gate)) => format!(
                "<span class=\"{}\">{} {}</span>",
                if gate.passed { "pass" } else { "fail" },
                escape_html(kind),
                if gate.passed { "pass" } else { "fail" }
            ),
            _ => "descriptive".to_owned(),
        };
        write!(
            html,
            "<tr><td>{}</td><td>{}</td><td class=\"number\">{}</td><td class=\"number\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(&row.scenario),
            escape_html(&row.latency_boundary),
            row.analysis.pairs,
            format_interval(row.analysis.throughput_ratio),
            format_interval(row.analysis.latency_ratio),
            if row.load_accepted { "accepted" } else { "rejected" },
            gate,
        )
        .expect("writing to a string should be infallible");
        html
    });
    format!(
        "<section id=\"paired-effects\"><h2>Paired effects</h2><p class=\"note\">Ratios are Laser divided by raw Iggy. Brackets contain the deterministic bootstrap 95 percent confidence interval.</p><div class=\"scroll\"><table><thead><tr><th>Scenario</th><th>Latency boundary</th><th class=\"number\">Pairs</th><th>Throughput ratio</th><th>Latency ratio</th><th>Load</th><th>Gate</th></tr></thead><tbody>{body}</tbody></table></div></section>"
    )
}

fn render_curves(points: &[CurvePoint]) -> String {
    let mut grouped = BTreeMap::<(&str, &str), Vec<&CurvePoint>>::new();
    for point in points {
        grouped
            .entry((&point.scenario, &point.arm))
            .or_default()
            .push(point);
    }
    let charts = grouped
        .into_iter()
        .filter_map(|((scenario, arm), mut points)| {
            if points.len() < 2 {
                return None;
            }
            points.sort_by_key(|point| point.offered_rate);
            let max_x = points
                .iter()
                .map(|point| point.achieved_operations_per_second)
                .fold(0.0_f64, f64::max)
                .max(1.0);
            let max_y = points
                .iter()
                .map(|point| point.primary_p99_ns)
                .max()
                .unwrap_or(1)
                .max(1);
            let coordinates = points
                .iter()
                .map(|point| curve_coordinate(point, max_x, max_y))
                .collect::<Vec<_>>();
            let polyline = coordinates
                .iter()
                .map(|(x, y)| format!("{x:.1},{y:.1}"))
                .collect::<Vec<_>>()
                .join(" ");
            let marks = points
                .iter()
                .zip(coordinates)
                .fold(String::new(), |mut html, (point, (x, y))| {
                    write!(html, "<circle class=\"point\" cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"4\"><title>offered {}, achieved {:.2}, p99 {}, {} repetition(s)</title></circle>", point.offered_rate, point.achieved_operations_per_second, format_duration(point.primary_p99_ns), point.repetitions).expect("writing to a string should be infallible");
                    html
                });
            Some(format!("<article class=\"curve\"><strong>{}</strong><div>{}</div><svg viewBox=\"0 0 720 260\" role=\"img\" aria-label=\"Latency against achieved throughput\"><line class=\"axis\" x1=\"56\" y1=\"220\" x2=\"700\" y2=\"220\"/><line class=\"axis\" x1=\"56\" y1=\"20\" x2=\"56\" y2=\"220\"/><polyline class=\"series\" points=\"{polyline}\"/>{marks}<text x=\"378\" y=\"250\" text-anchor=\"middle\" font-size=\"12\">achieved operations per second</text><text x=\"14\" y=\"120\" text-anchor=\"middle\" font-size=\"12\" transform=\"rotate(-90 14 120)\">primary p99</text></svg></article>", escape_html(scenario), escape_html(arm)))
        })
        .collect::<String>();
    if charts.is_empty() {
        String::new()
    } else {
        format!(
            "<section id=\"latency-curves\"><h2>Latency and throughput curves</h2><p class=\"note\">Each point is the median repetition at one fixed offered rate. Hover a point for offered load, achieved throughput, tail latency, and repetition count.</p><div class=\"curves\">{charts}</div></section>"
        )
    }
}

#[allow(clippy::cast_precision_loss)]
fn curve_coordinate(point: &CurvePoint, max_x: f64, max_y: u64) -> (f64, f64) {
    let x = 56.0 + point.achieved_operations_per_second / max_x * 644.0;
    let y = 220.0 - (point.primary_p99_ns as f64 / max_y as f64) * 200.0;
    (x, y)
}

fn format_interval(interval: crate::analysis::ConfidenceInterval) -> String {
    format!(
        "{:.4} [{:.4}, {:.4}]",
        interval.estimate, interval.lower_95, interval.upper_95
    )
}

fn analysis_file(root: &Path, path: &Path) -> Result<AnalysisFile, BenchError> {
    let relative = path.strip_prefix(root).map_err(|error| {
        BenchError::Invalid(format!("analysis path is outside suite directory: {error}"))
    })?;
    let metadata = fs::metadata(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(AnalysisFile {
        path: relative.to_string_lossy().into_owned(),
        sha256: sha256_file(path)?,
        bytes: metadata.len(),
    })
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<(), BenchError> {
    if path.exists() {
        let existing = fs::read(path).map_err(|source| BenchError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        if existing == bytes {
            return Ok(());
        }
        return Err(BenchError::Invalid(format!(
            "immutable analysis file `{}` already exists with different contents",
            path.display()
        )));
    }
    fs::write(path, bytes).map_err(|source| BenchError::Write {
        path: path.to_path_buf(),
        source,
    })
}

fn read_json(path: &Path) -> Result<Value, BenchError> {
    let bytes = fs::read(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn safe_path(root: &Path, relative: &str) -> Result<std::path::PathBuf, BenchError> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BenchError::Invalid(format!(
            "unsafe suite analysis path `{relative}`"
        )));
    }
    Ok(root.join(path))
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn optional_f64(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| format!("{value:.6}"))
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn format_duration(nanos: u64) -> String {
    if nanos >= 1_000_000_000 {
        format_scaled_duration(nanos, 1_000_000_000, "s")
    } else if nanos >= 1_000_000 {
        format_scaled_duration(nanos, 1_000_000, "ms")
    } else if nanos >= 1_000 {
        format_scaled_duration(nanos, 1_000, "us")
    } else {
        format!("{nanos} ns")
    }
}

fn format_scaled_duration(nanos: u64, divisor: u64, unit: &str) -> String {
    let whole = nanos / divisor;
    let rounded_hundredths = ((nanos % divisor) * 100 + divisor / 2) / divisor;
    if rounded_hundredths == 100 {
        format!("{}.00 {unit}", whole + 1)
    } else {
        format!("{whole}.{rounded_hundredths:02} {unit}")
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::report::{
        AnalysisStatus, BenchmarkLanguage, EnvironmentReport, SourceIdentity, WorkloadReport,
    };

    #[test]
    fn given_suite_reports_when_rendered_then_should_verify_all_analysis_files() {
        let directory = tempdir().expect("temporary suite should exist");
        fs::write(
            directory.path().join("suite-index.json"),
            br#"{"schema_version":1,"scenarios":[{"name":"stream_direct","paired_analysis":{"latency_boundary":"publish to acknowledgement","analysis":{"pairs":2,"throughput_ratio":{"estimate":1.0,"lower_95":0.99,"upper_95":1.01},"latency_ratio":{"estimate":1.0,"lower_95":0.99,"upper_95":1.01}},"load_accepted":true,"c2":{"throughput_lower_bound":0.99,"latency_upper_bound":1.01,"passed":true},"aa_calibration":null}}]}"#,
        )
        .expect("suite index should write");
        let report = report_at_rate(100, 12_345.0, 50_000, 0);
        assert_eq!(
            report_rows(&report)[0].operations_per_second,
            Some(12_345.0)
        );
        let second = report_at_rate(200, 23_456.0, 75_000, 1);
        let first = write_suite_analysis(directory.path(), "suite", false, &[report, second])
            .expect("analysis should render");
        let verified = verify_suite_analysis(directory.path()).expect("analysis should verify");
        let derived = serde_json::from_value::<SuiteAnalysis>(
            read_json(&directory.path().join("analysis/analysis.json"))
                .expect("analysis JSON should read"),
        )
        .expect("analysis JSON should decode");
        let html = fs::read_to_string(directory.path().join("analysis/report.html"))
            .expect("analysis HTML should read");

        assert_eq!(first, verified);
        assert_eq!(derived.paired_analyses.len(), 1);
        assert_eq!(derived.latency_throughput_curves.len(), 2);
        assert_eq!(derived.workload_time_series.len(), 2);
        assert!(html.contains("Paired effects"));
        assert!(html.contains("Latency and throughput curves"));
        assert!(html.contains("class=\"brand-logo\""));
        assert!(html.contains("id=\"theme-toggle\""));
        assert!(html.contains("prefers-color-scheme: dark"));
        assert!(!html.contains("assets.laserdata.com"));
        assert!(directory.path().join("analysis/report.html").is_file());
        assert!(directory.path().join("analysis/results.csv").is_file());
        assert!(
            directory
                .path()
                .join("analysis/workload-timeseries.csv")
                .is_file()
        );
        assert!(
            directory
                .path()
                .join("analysis/paired-effects.csv")
                .is_file()
        );
        assert!(
            directory
                .path()
                .join("analysis/latency-throughput-curves.csv")
                .is_file()
        );
        assert!(
            directory
                .path()
                .join("analysis/latency-distributions.csv")
                .is_file()
        );
    }

    fn report_at_rate(rate: u64, throughput: f64, p99_ns: u64, repetition: u32) -> RunReport {
        let outcomes = OutcomeCounts {
            offered: 1,
            dispatched: 1,
            completed: 1,
            successful: 1,
            ..OutcomeCounts::default()
        };
        let mut report = RunReport {
            schema_version: 1,
            run_id: format!("run-{repetition}"),
            suite_digest: "a".repeat(64),
            scenario: format!("stream_direct-rate-{rate}"),
            arm: "arm".to_owned(),
            repetition,
            seed: 1,
            language: BenchmarkLanguage::Rust,
            source: SourceIdentity {
                sdk_revision: "revision".to_owned(),
                benchmark_revision: "revision".to_owned(),
                dirty: false,
            },
            artifacts: Vec::new(),
            environment: EnvironmentReport {
                tier: "developer_smoke".to_owned(),
                durability_profile: "release_default".to_owned(),
                cache_state: "warm".to_owned(),
                kernel: "test".to_owned(),
                architecture: "test".to_owned(),
                runtime_worker_threads: 1,
            },
            workload: WorkloadReport {
                logical_unit: "operation".to_owned(),
                payload_bytes: 1,
                batch_size: 1,
                partitions: 1,
                offered_rate: Some(rate),
            },
            outcomes: outcomes.clone(),
            histograms: Vec::new(),
            deterministic_gates: Vec::new(),
            observer_cost: None,
            analysis: AnalysisStatus {
                valid: true,
                publishable: false,
                invalidation_reason: None,
            },
            extra: std::collections::BTreeMap::new(),
        };
        report.extra.insert(
            "recovery_summary".to_owned(),
            json!({
                "catch_up_records_per_second": throughput,
                "primary_p99_ns": p99_ns,
                "outcomes": outcomes,
                "time_series": [{
                    "second": 0,
                    "outcomes": outcomes,
                    "max_in_flight": 1
                }]
            }),
        );
        report
    }
}
