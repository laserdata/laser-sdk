use std::path::Path;

use laser_sdk::laser::Laser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use strum::{Display, IntoStaticStr};

use super::guaranteed::{McpGuaranteedEvidence, McpGuaranteedSummary, run_mcp_guaranteed_evidence};
use super::minimal::{
    McpMinimalEvidence, McpMinimalSummary, run_mcp_minimal_evidence, text_payload,
};
use super::triage::{AgdxTriageEvidence, AgdxTriageSummary, run_agdx_triage_evidence};
use crate::BenchError;
use crate::agdx::AgdxCase;
use crate::network::NetworkByteMeasurement;
use crate::trace::{SharedTraceRef, write_shared_trace};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpByteArm {
    pub application_request_bytes: usize,
    pub application_response_bytes: usize,
    pub application_total_bytes: usize,
    pub successful_operations: u64,
    pub network: NetworkByteMeasurement,
    pub network_bytes_per_successful_operation: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpByteVerdict {
    pub measurement_valid: bool,
    pub application_ratio_agdx_over_minimal_mcp: f64,
    pub network_ratio_agdx_over_minimal_mcp: Option<f64>,
    pub passed: bool,
    pub invalidation_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum McpApplicationByteBoundary {
    DeclaredSequenceZeroEncodedRecordBeforeTransportFraming,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum McpNetworkByteBoundary {
    KernelObservedTcpPayloadOnEachServerPortDuringTimedArm,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpByteAccounting {
    pub application_boundary: McpApplicationByteBoundary,
    pub network_boundary: McpNetworkByteBoundary,
    pub agdx: McpByteArm,
    pub minimal_mcp: McpByteArm,
    pub guarantee_matched_mcp: McpByteArm,
    pub m6: McpByteVerdict,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpTriageSummary {
    pub agdx: AgdxTriageSummary,
    pub minimal_mcp: McpMinimalSummary,
    pub guarantee_matched_mcp: McpGuaranteedSummary,
    pub shared_trace: SharedTraceRef,
    pub recipients: u32,
    pub postgres_process_measured: bool,
    pub byte_accounting: McpByteAccounting,
    pub configuration: Value,
}

pub struct McpTriageEvidence {
    pub summary: McpTriageSummary,
    pub agdx: AgdxTriageEvidence,
    pub minimal_mcp: McpMinimalEvidence,
    pub guarantee_matched_mcp: McpGuaranteedEvidence,
}

pub struct McpTriageRun<'a> {
    pub laser: &'a Laser,
    pub connection_string: &'a str,
    pub case: &'a AgdxCase,
    pub seed: u64,
    pub recipients: u32,
    pub dsn: &'a str,
    pub monitored_processes: &'a [(String, u32)],
    pub iggy_server_port: u16,
    pub output: &'a Path,
}

#[derive(Clone, Copy, Debug, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum TriageArm {
    Agdx,
    MinimalMcp,
    GuaranteeMatchedMcp,
}

/// Run the three triage arms against one persisted fixed-rate schedule trace.
///
/// # Errors
///
/// Returns an error when the offered rate is absent, any arm fails, trace persistence fails, or payload identity drifts.
pub async fn run_mcp_triage_evidence(
    run: McpTriageRun<'_>,
) -> Result<McpTriageEvidence, BenchError> {
    let McpTriageRun {
        laser,
        connection_string,
        case,
        seed,
        recipients,
        dsn,
        monitored_processes,
        iggy_server_port,
        output,
    } = run;
    let rate = case.offered_rate.ok_or_else(|| {
        BenchError::Invalid("MCP triage comparison requires an offered_rate".to_owned())
    })?;
    let payload = text_payload(case.payload_bytes, seed);
    let shared_trace = write_shared_trace(
        output,
        seed,
        case.duration_seconds,
        rate,
        payload.as_bytes(),
    )?;
    let mut agdx = None;
    let mut minimal_mcp = None;
    let mut guarantee_matched_mcp = None;
    for arm in arm_order(seed) {
        match arm {
            TriageArm::Agdx => {
                agdx = Some(
                    run_agdx_triage_evidence(
                        laser,
                        connection_string,
                        case,
                        seed,
                        monitored_processes,
                        iggy_server_port,
                    )
                    .await?,
                );
            }
            TriageArm::MinimalMcp => {
                minimal_mcp =
                    Some(run_mcp_minimal_evidence(case, seed, monitored_processes).await?);
            }
            TriageArm::GuaranteeMatchedMcp => {
                guarantee_matched_mcp = Some(
                    run_mcp_guaranteed_evidence(case, seed, recipients, dsn, monitored_processes)
                        .await?,
                );
            }
        }
    }
    let agdx = agdx.ok_or_else(|| BenchError::Invalid("AGDX triage arm did not run".to_owned()))?;
    let minimal_mcp = minimal_mcp
        .ok_or_else(|| BenchError::Invalid("minimal MCP triage arm did not run".to_owned()))?;
    let guarantee_matched_mcp = guarantee_matched_mcp.ok_or_else(|| {
        BenchError::Invalid("guarantee-matched MCP triage arm did not run".to_owned())
    })?;
    if agdx.payload != payload {
        return Err(BenchError::Invalid(
            "triage arm payloads are not byte-identical".to_owned(),
        ));
    }
    let byte_accounting = byte_accounting(&agdx, &minimal_mcp, &guarantee_matched_mcp);
    let summary = McpTriageSummary {
        agdx: agdx.summary.clone(),
        minimal_mcp: minimal_mcp.summary.clone(),
        guarantee_matched_mcp: guarantee_matched_mcp.summary.clone(),
        shared_trace,
        recipients,
        postgres_process_measured: guarantee_matched_mcp.summary.postgres_process_measured,
        byte_accounting,
        configuration: json!({
            "workload": "deterministic_triage_echo",
            "schedule": "shared_fixed_rate_binary_trace",
            "arm_order": arm_order(seed).map(<&str>::from),
            "handler_work": "fixed_echo",
            "fixed_model_time": "none",
        }),
    };
    Ok(McpTriageEvidence {
        summary,
        agdx,
        minimal_mcp,
        guarantee_matched_mcp,
    })
}

fn byte_accounting(
    agdx: &AgdxTriageEvidence,
    minimal_mcp: &McpMinimalEvidence,
    guarantee_matched_mcp: &McpGuaranteedEvidence,
) -> McpByteAccounting {
    let agdx = byte_arm(
        agdx.summary.request_bytes,
        agdx.summary.response_bytes,
        agdx.summary.request_reply.outcomes.successful,
        agdx.summary.network.clone(),
    );
    let minimal_mcp = byte_arm(
        minimal_mcp.summary.request_bytes,
        minimal_mcp.summary.response_bytes,
        minimal_mcp.summary.streamable_http.outcomes.successful,
        minimal_mcp.summary.network.clone(),
    );
    let guarantee_matched_mcp = byte_arm(
        guarantee_matched_mcp.summary.request_bytes,
        guarantee_matched_mcp.summary.response_bytes,
        guarantee_matched_mcp
            .summary
            .streamable_http
            .outcomes
            .successful,
        guarantee_matched_mcp.summary.network.clone(),
    );
    let application_ratio = ratio_u64(
        u64::try_from(agdx.application_total_bytes).unwrap_or(u64::MAX),
        u64::try_from(minimal_mcp.application_total_bytes).unwrap_or(u64::MAX),
    )
    .expect("minimal MCP application bytes are non-zero");
    let network_ratio = match (
        agdx.network_bytes_per_successful_operation,
        minimal_mcp.network_bytes_per_successful_operation,
    ) {
        (Some(agdx), Some(minimal)) => ratio_f64(agdx, minimal),
        _ => None,
    };
    let network_complete = agdx.network.complete
        && minimal_mcp.network.complete
        && guarantee_matched_mcp.network.complete;
    let measurement_valid = network_complete && network_ratio.is_some();
    let passed = measurement_valid
        && application_ratio < 1.0
        && network_ratio.is_some_and(|ratio| ratio < 1.0);
    McpByteAccounting {
        application_boundary:
            McpApplicationByteBoundary::DeclaredSequenceZeroEncodedRecordBeforeTransportFraming,
        network_boundary:
            McpNetworkByteBoundary::KernelObservedTcpPayloadOnEachServerPortDuringTimedArm,
        agdx,
        minimal_mcp,
        guarantee_matched_mcp,
        m6: McpByteVerdict {
            measurement_valid,
            application_ratio_agdx_over_minimal_mcp: application_ratio,
            network_ratio_agdx_over_minimal_mcp: network_ratio,
            passed,
            invalidation_reason: (!measurement_valid).then(|| {
                "one or more arm-scoped kernel TCP byte measurements were incomplete".to_owned()
            }),
        },
    }
}

fn byte_arm(
    request_bytes: usize,
    response_bytes: usize,
    successful_operations: u64,
    network: NetworkByteMeasurement,
) -> McpByteArm {
    McpByteArm {
        application_request_bytes: request_bytes,
        application_response_bytes: response_bytes,
        application_total_bytes: request_bytes.saturating_add(response_bytes),
        successful_operations,
        network_bytes_per_successful_operation: ratio_u64(
            network.total_tcp_payload_bytes,
            successful_operations,
        ),
        network,
    }
}

#[allow(clippy::cast_precision_loss)]
fn ratio_u64(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn ratio_f64(numerator: f64, denominator: f64) -> Option<f64> {
    (denominator > 0.0).then_some(numerator / denominator)
}

fn arm_order(seed: u64) -> [TriageArm; 3] {
    const ORDERS: [[TriageArm; 3]; 6] = [
        [
            TriageArm::Agdx,
            TriageArm::MinimalMcp,
            TriageArm::GuaranteeMatchedMcp,
        ],
        [
            TriageArm::Agdx,
            TriageArm::GuaranteeMatchedMcp,
            TriageArm::MinimalMcp,
        ],
        [
            TriageArm::MinimalMcp,
            TriageArm::Agdx,
            TriageArm::GuaranteeMatchedMcp,
        ],
        [
            TriageArm::MinimalMcp,
            TriageArm::GuaranteeMatchedMcp,
            TriageArm::Agdx,
        ],
        [
            TriageArm::GuaranteeMatchedMcp,
            TriageArm::Agdx,
            TriageArm::MinimalMcp,
        ],
        [
            TriageArm::GuaranteeMatchedMcp,
            TriageArm::MinimalMcp,
            TriageArm::Agdx,
        ],
    ];
    ORDERS[usize::try_from(seed % 6).expect("seed remainder fits usize")]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_repetitions_when_ordered_then_should_cover_all_six_permutations() {
        let orders = (0..6).map(arm_order).collect::<Vec<_>>();

        for left in 0..orders.len() {
            for right in left + 1..orders.len() {
                assert_ne!(
                    orders[left].map(<&str>::from),
                    orders[right].map(<&str>::from)
                );
            }
        }
    }
}
