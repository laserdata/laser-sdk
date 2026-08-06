use std::fs;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::json;
use strum::{Display, IntoStaticStr};

use crate::BenchError;
use crate::binary::sha256_file;
use crate::manifest::{Environment, Scenario};
use crate::mcp::McpTriageSummary;
use crate::report::HistogramRef;
use crate::schema::{MCP_REVIEW_SIGNOFF_SCHEMA, validate_json};

const BUNDLE_PATH: &str = "mcp-reviewer-bundle.json";
const REVIEW_DIRECTORY: &str = "reviewer";
const SIGNOFF_PATH: &str = "reviewer/signoff.json";

const SOURCE_FILES: [(&str, &str, &str); 19] = [
    (
        "reviewer/source/Cargo.toml",
        include_str!("../Cargo.toml"),
        "benchmark dependency and feature manifest",
    ),
    (
        "reviewer/source/Cargo.lock",
        include_str!("../Cargo.lock"),
        "exact benchmark dependency resolution",
    ),
    (
        "reviewer/source/src/agdx/mod.rs",
        include_str!("agdx/mod.rs"),
        "shared workload engine and measurement boundary",
    ),
    (
        "reviewer/source/src/engine.rs",
        include_str!("engine.rs"),
        "open-loop and closed-loop scheduler",
    ),
    (
        "reviewer/source/src/network.rs",
        include_str!("network.rs"),
        "kernel TCP byte accounting",
    ),
    (
        "reviewer/source/src/main.rs",
        include_str!("main.rs"),
        "command-line entry point",
    ),
    (
        "reviewer/source/src/execution/mod.rs",
        include_str!("execution/mod.rs"),
        "scenario execution module boundary",
    ),
    (
        "reviewer/source/src/execution/mcp.rs",
        include_str!("execution/mcp.rs"),
        "MCP scenario orchestration",
    ),
    (
        "reviewer/source/src/execution/report/mod.rs",
        include_str!("execution/report/mod.rs"),
        "histogram and shared report construction",
    ),
    (
        "reviewer/source/src/execution/report/mcp.rs",
        include_str!("execution/report/mcp.rs"),
        "MCP report validity and publication status",
    ),
    (
        "reviewer/source/src/execution/runtime.rs",
        include_str!("execution/runtime.rs"),
        "native service and workload dispatch",
    ),
    (
        "reviewer/source/src/review.rs",
        include_str!("review.rs"),
        "review bundle construction and verification",
    ),
    (
        "reviewer/source/src/trace.rs",
        include_str!("trace.rs"),
        "shared deterministic schedule trace",
    ),
    (
        "reviewer/source/src/mcp/mod.rs",
        include_str!("mcp/mod.rs"),
        "MCP benchmark module and bridge diagnostic",
    ),
    (
        "reviewer/source/src/mcp/comparison.rs",
        include_str!("mcp/comparison.rs"),
        "three-arm comparison and M6 verdict",
    ),
    (
        "reviewer/source/src/mcp/guaranteed.rs",
        include_str!("mcp/guaranteed.rs"),
        "PostgreSQL guarantee-matched control and recovery",
    ),
    (
        "reviewer/source/src/mcp/minimal.rs",
        include_str!("mcp/minimal.rs"),
        "minimal MCP control",
    ),
    (
        "reviewer/source/src/mcp/transport.rs",
        include_str!("mcp/transport.rs"),
        "Streamable HTTP transport setup",
    ),
    (
        "reviewer/source/src/mcp/triage.rs",
        include_str!("mcp/triage.rs"),
        "typed AGDX triage arm",
    ),
];

const REVIEW_GUIDE: &str = r"# MCP Benchmark Review

This bundle freezes the exact Rust implementation, dependency resolution, redacted effective configuration, schedule trace, summaries, and histogram references used by the three-arm MCP comparison.

Review the semantic guarantee matrix before comparing performance. Arm A is typed AGDX over the durable log. Arm B is latency-favorable minimal MCP over Streamable HTTP. Arm C adds the declared PostgreSQL durability, idempotency, ordered outbox, retry, and retained-result behavior.

Validate M1 through M6 independently. A benchmark completion is not reviewer acceptance. Record acceptance or requested changes in a new sign-off document that validates against `mcp-review-signoff-v1.schema.json` and names this bundle's SHA-256 digest.
";

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum McpReviewStatus {
    AwaitingExternalReview,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum McpReviewDecision {
    Accepted,
    ChangesRequested,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    IntoStaticStr,
    Serialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum McpHypothesis {
    M1,
    M2,
    M3,
    M4,
    M5,
    M6,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct McpReviewerBundleRef {
    pub path: String,
    pub sha256: String,
    pub status: McpReviewStatus,
    pub signoff_schema: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct McpReviewSignoff {
    pub schema_version: u32,
    pub bundle_sha256: String,
    pub reviewer: String,
    pub reviewed_at: String,
    pub decision: McpReviewDecision,
    pub hypotheses: Vec<McpHypothesis>,
    pub findings: Vec<McpReviewFinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct McpReviewFinding {
    pub severity: String,
    pub summary: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct BundleFile {
    path: String,
    sha256: String,
    bytes: u64,
    purpose: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct McpReviewerBundle {
    schema_version: u32,
    subject: String,
    status: McpReviewStatus,
    hypotheses: [McpHypothesis; 6],
    files: Vec<BundleFile>,
    guarantee_matrix: serde_json::Value,
    review_instructions: String,
    signoff_schema: String,
}

/// Freeze the implementation and evidence needed for external MCP review.
///
/// # Errors
///
/// Returns an error when a source snapshot, configuration, evidence reference, or bundle cannot be written or hashed.
pub fn write_mcp_reviewer_bundle(
    output: &Path,
    scenario: &Scenario,
    environment: &Environment,
    summary: &McpTriageSummary,
    histograms: &[HistogramRef],
) -> Result<McpReviewerBundleRef, BenchError> {
    let reviewer = output.join(REVIEW_DIRECTORY);
    fs::create_dir_all(&reviewer).map_err(|source| BenchError::Write {
        path: reviewer.clone(),
        source,
    })?;
    let mut files = Vec::new();
    write_review_sources(output, &mut files)?;
    write_review_configuration(output, scenario, environment, summary, &mut files)?;
    add_review_evidence(output, histograms, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let bundle = reviewer_bundle(files);
    let bundle_path = output.join(BUNDLE_PATH);
    let bytes = serde_json::to_vec_pretty(&bundle)?;
    fs::write(&bundle_path, bytes).map_err(|source| BenchError::Write {
        path: bundle_path.clone(),
        source,
    })?;
    Ok(McpReviewerBundleRef {
        path: BUNDLE_PATH.to_owned(),
        sha256: sha256_file(&bundle_path)?,
        status: McpReviewStatus::AwaitingExternalReview,
        signoff_schema: "reviewer/mcp-review-signoff-v1.schema.json".to_owned(),
    })
}

fn write_review_sources(output: &Path, files: &mut Vec<BundleFile>) -> Result<(), BenchError> {
    for (path, contents, purpose) in SOURCE_FILES {
        write_bundle_file(output, path, contents.as_bytes(), purpose, files)?;
    }
    write_bundle_file(
        output,
        "reviewer/README.md",
        REVIEW_GUIDE.as_bytes(),
        "review procedure and semantic boundary",
        files,
    )?;
    write_bundle_file(
        output,
        "reviewer/mcp-review-signoff-v1.schema.json",
        include_bytes!("../schemas/mcp-review-signoff-v1.schema.json"),
        "machine-readable external sign-off contract",
        files,
    )?;
    Ok(())
}

fn write_review_configuration(
    output: &Path,
    scenario: &Scenario,
    environment: &Environment,
    summary: &McpTriageSummary,
    files: &mut Vec<BundleFile>,
) -> Result<(), BenchError> {
    let configuration = json!({
        "scenario": scenario,
        "environment": {
            "tier": environment.tier,
            "durability_profile": environment.durability_profile,
            "cache_state": environment.cache_state,
            "postgres_dsn_env": environment.postgres_dsn_env,
            "postgres_pid_env": environment.postgres_pid_env,
        },
        "comparison": summary.configuration,
        "postgres_process_measured": summary.postgres_process_measured,
    });
    let configuration = serde_json::to_vec_pretty(&configuration)?;
    write_bundle_file(
        output,
        "reviewer/effective-config.json",
        &configuration,
        "redacted comparison configuration",
        files,
    )
}

fn add_review_evidence(
    output: &Path,
    histograms: &[HistogramRef],
    files: &mut Vec<BundleFile>,
) -> Result<(), BenchError> {
    add_existing_file(
        output,
        "mcp-triage-summary.json",
        "complete three-arm summary",
        files,
    )?;
    add_existing_file(
        output,
        "shared-trace.bin",
        "fixed-rate binary schedule and payload trace",
        files,
    )?;
    for histogram in histograms {
        add_existing_file(
            output,
            &histogram.path,
            &format!("{} latency distribution", histogram.class),
            files,
        )?;
    }
    Ok(())
}

fn reviewer_bundle(files: Vec<BundleFile>) -> McpReviewerBundle {
    McpReviewerBundle {
        schema_version: 1,
        subject: "agdx_vs_minimal_mcp_vs_guarantee_matched_mcp".to_owned(),
        status: McpReviewStatus::AwaitingExternalReview,
        hypotheses: [
            McpHypothesis::M1,
            McpHypothesis::M2,
            McpHypothesis::M3,
            McpHypothesis::M4,
            McpHypothesis::M5,
            McpHypothesis::M6,
        ],
        files,
        guarantee_matrix: json!({
            "agdx": {
                "transport": "iggy_tcp_vsr",
                "durable_log": true,
                "replay": true,
                "fan_out": "broker_managed",
                "idempotency": "application_key_and_reliable_consumer",
            },
            "minimal_mcp": {
                "transport": "streamable_http",
                "durable_log": false,
                "replay": false,
                "fan_out": "none",
                "idempotency": "none",
            },
            "guarantee_matched_mcp": {
                "transport": "streamable_http",
                "durable_log": false,
                "durable_inbox_outbox": true,
                "replay": "postgres_outbox_reclaim",
                "fan_out": "one_ordered_outbox_row_per_recipient",
                "idempotency": "durable_request_key_and_retained_terminal_result",
            },
        }),
        review_instructions: "reviewer/README.md".to_owned(),
        signoff_schema: "reviewer/mcp-review-signoff-v1.schema.json".to_owned(),
    }
}

/// Verify every digest named by an MCP reviewer bundle reference.
///
/// # Errors
///
/// Returns an error when paths escape the run directory, files are absent, JSON is invalid, or any digest or byte length differs.
pub fn verify_mcp_reviewer_bundle(
    output: &Path,
    reference: &McpReviewerBundleRef,
) -> Result<(), BenchError> {
    let bundle_path = safe_path(output, &reference.path)?;
    if sha256_file(&bundle_path)? != reference.sha256 {
        return Err(BenchError::Invalid(
            "MCP reviewer bundle digest does not match the report".to_owned(),
        ));
    }
    let bytes = fs::read(&bundle_path).map_err(|source| BenchError::Read {
        path: bundle_path,
        source,
    })?;
    let bundle: McpReviewerBundle = serde_json::from_slice(&bytes)?;
    if bundle.schema_version != 1 || bundle.status != reference.status {
        return Err(BenchError::Invalid(
            "MCP reviewer bundle metadata does not match the report".to_owned(),
        ));
    }
    for file in bundle.files {
        let path = safe_path(output, &file.path)?;
        let metadata = fs::metadata(&path).map_err(|source| BenchError::Read {
            path: path.clone(),
            source,
        })?;
        if metadata.len() != file.bytes || sha256_file(&path)? != file.sha256 {
            return Err(BenchError::Invalid(format!(
                "MCP reviewer file `{}` failed integrity validation",
                file.path
            )));
        }
    }
    Ok(())
}

/// Validate the independent review decision attached to one MCP triage repetition.
///
/// # Errors
///
/// Returns an error when the sign-off is absent, violates its schema, names another bundle, omits a hypothesis, requests changes, or contains a blocking finding.
pub fn verify_mcp_review_signoff(
    output: &Path,
    reference: &McpReviewerBundleRef,
) -> Result<McpReviewSignoff, BenchError> {
    verify_mcp_reviewer_bundle(output, reference)?;
    let path = safe_path(output, SIGNOFF_PATH)?;
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).map_err(|source| BenchError::Read {
            path: path.clone(),
            source,
        })?)?;
    validate_json(MCP_REVIEW_SIGNOFF_SCHEMA, &value)?;
    let signoff: McpReviewSignoff = serde_json::from_value(value)?;
    if signoff.bundle_sha256 != reference.sha256 {
        return Err(BenchError::Invalid(
            "MCP review sign-off names another reviewer bundle".to_owned(),
        ));
    }
    if signoff.decision != McpReviewDecision::Accepted {
        return Err(BenchError::Invalid(
            "MCP review requested changes".to_owned(),
        ));
    }
    let hypotheses = signoff
        .hypotheses
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if hypotheses
        != [
            McpHypothesis::M1,
            McpHypothesis::M2,
            McpHypothesis::M3,
            McpHypothesis::M4,
            McpHypothesis::M5,
            McpHypothesis::M6,
        ]
        .into_iter()
        .collect()
    {
        return Err(BenchError::Invalid(
            "MCP review sign-off does not cover M1 through M6".to_owned(),
        ));
    }
    if signoff
        .findings
        .iter()
        .any(|finding| finding.severity == "blocking")
    {
        return Err(BenchError::Invalid(
            "MCP review sign-off contains a blocking finding".to_owned(),
        ));
    }
    Ok(signoff)
}

fn write_bundle_file(
    root: &Path,
    relative: &str,
    bytes: &[u8],
    purpose: &str,
    files: &mut Vec<BundleFile>,
) -> Result<(), BenchError> {
    let path = safe_path(root, relative)?;
    let parent = path
        .parent()
        .ok_or_else(|| BenchError::Invalid(format!("reviewer file `{relative}` has no parent")))?;
    fs::create_dir_all(parent).map_err(|source| BenchError::Write {
        path: parent.to_path_buf(),
        source,
    })?;
    fs::write(&path, bytes).map_err(|source| BenchError::Write {
        path: path.clone(),
        source,
    })?;
    add_existing_file(root, relative, purpose, files)
}

fn add_existing_file(
    root: &Path,
    relative: &str,
    purpose: &str,
    files: &mut Vec<BundleFile>,
) -> Result<(), BenchError> {
    let path = safe_path(root, relative)?;
    let bytes = fs::metadata(&path)
        .map_err(|source| BenchError::Read {
            path: path.clone(),
            source,
        })?
        .len();
    files.push(BundleFile {
        path: relative.to_owned(),
        sha256: sha256_file(&path)?,
        bytes,
        purpose: purpose.to_owned(),
    });
    Ok(())
}

fn safe_path(root: &Path, relative: &str) -> Result<std::path::PathBuf, BenchError> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BenchError::Invalid(format!(
            "unsafe MCP reviewer path `{relative}`"
        )));
    }
    Ok(root.join(path))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn given_parent_component_when_resolved_then_should_reject_path() {
        let error = safe_path(Path::new("evidence"), "../outside")
            .expect_err("parent path should be rejected");

        assert!(error.to_string().contains("unsafe MCP reviewer path"));
    }

    #[test]
    fn given_tampered_reviewer_file_when_verified_then_should_reject_bundle() {
        let directory = tempdir().expect("temporary evidence directory should exist");
        let evidence = directory.path().join("evidence.json");
        fs::write(&evidence, b"original").expect("evidence should write");
        let bundle = McpReviewerBundle {
            schema_version: 1,
            subject: "test".to_owned(),
            status: McpReviewStatus::AwaitingExternalReview,
            hypotheses: [
                McpHypothesis::M1,
                McpHypothesis::M2,
                McpHypothesis::M3,
                McpHypothesis::M4,
                McpHypothesis::M5,
                McpHypothesis::M6,
            ],
            files: vec![BundleFile {
                path: "evidence.json".to_owned(),
                sha256: sha256_file(&evidence).expect("evidence should hash"),
                bytes: 8,
                purpose: "test evidence".to_owned(),
            }],
            guarantee_matrix: json!({}),
            review_instructions: "none".to_owned(),
            signoff_schema: "none".to_owned(),
        };
        let bundle_path = directory.path().join(BUNDLE_PATH);
        fs::write(
            &bundle_path,
            serde_json::to_vec_pretty(&bundle).expect("bundle should encode"),
        )
        .expect("bundle should write");
        let reference = McpReviewerBundleRef {
            path: BUNDLE_PATH.to_owned(),
            sha256: sha256_file(&bundle_path).expect("bundle should hash"),
            status: McpReviewStatus::AwaitingExternalReview,
            signoff_schema: "none".to_owned(),
        };

        verify_mcp_reviewer_bundle(directory.path(), &reference)
            .expect("untouched bundle should verify");
        fs::create_dir(directory.path().join("reviewer")).expect("reviewer directory should exist");
        fs::write(
            directory.path().join(SIGNOFF_PATH),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "bundle_sha256": reference.sha256.clone(),
                "reviewer": "independent reviewer",
                "reviewed_at": "2026-08-05T00:00:00Z",
                "decision": "accepted",
                "hypotheses": ["m1", "m2", "m3", "m4", "m5", "m6"],
                "findings": [],
            }))
            .expect("sign-off should encode"),
        )
        .expect("sign-off should write");
        verify_mcp_review_signoff(directory.path(), &reference)
            .expect("accepted review should verify");
        fs::write(evidence, b"tampered").expect("evidence should be changed");
        let error = verify_mcp_reviewer_bundle(directory.path(), &reference)
            .expect_err("tampered evidence should fail verification");

        assert!(error.to_string().contains("failed integrity validation"));
    }
}
