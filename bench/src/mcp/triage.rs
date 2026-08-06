use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use laser_sdk::agent::{Agent, AgentCtx, AgentHandler, AgentMessage, Contract, InboxRoute, Router};
use laser_sdk::error::LaserError;
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::AgentTopic;
use laser_sdk::types::{AgentId, ConversationId};
use laser_wire::agent::{
    AgentEnvelope, ConversationId as WireConversationId, CorrelationId, RecordId,
};
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::framing::encode_named;
use laser_wire::headers::{
    AGENT_VERSION, CONTENT_TYPE, CONVERSATION_ID, HEADER_FRAMING_BYTES, TARGET_AGENT_ID,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::minimal::{text_payload, ticket_arguments};
use crate::BenchError;
use crate::agdx::{AgdxArmEvidence, AgdxArmSummary, AgdxCase, measured_arm_with_network, warmup};
use crate::engine::Operation;
use crate::network::NetworkByteMeasurement;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxTriageSummary {
    pub request_reply: AgdxArmSummary,
    pub request_body_bytes: usize,
    pub request_bytes: usize,
    pub response_body_bytes: usize,
    pub response_bytes: usize,
    pub network: NetworkByteMeasurement,
    pub configuration: Value,
}

pub struct AgdxTriageEvidence {
    pub summary: AgdxTriageSummary,
    pub request_reply: AgdxArmEvidence,
    pub payload: String,
}

struct TriageEcho {
    source: laser_wire::agent::AgentId,
}

impl AgentHandler for TriageEcho {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let envelope = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::Handler("triage command has no AGDX envelope".to_owned()))?;
        let correlation = envelope.correlation.ok_or_else(|| {
            LaserError::Handler("triage command has no AGDX correlation".to_owned())
        })?;
        let producer = ctx.laser().agdx(
            AgentTopic::Responses,
            self.source.clone(),
            envelope.conversation,
        );
        let mut response = producer.respond(correlation, envelope.body.clone());
        response = response.with_target(envelope.source.clone());
        response.send().await.map(|_| ())
    }
}

/// Measure AGDX request and reply over the same logical triage tickets used by MCP controls.
///
/// # Errors
///
/// Returns an error when setup, request execution, response validation, measurement, or shutdown fails.
pub async fn run_agdx_triage_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
    server_port: u16,
) -> Result<AgdxTriageEvidence, BenchError> {
    let stream = format!("bench-mcp-triage-{seed:016x}");
    let scoped = laser.with_default_stream(&stream);
    for topic in [AgentTopic::Commands, AgentTopic::Responses] {
        scoped
            .topic(topic.topic_string())
            .ensure(case.partitions)
            .await
            .map_err(sdk_error)?;
    }
    let worker: AgentId = "laser-bench-triage-worker"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid triage worker id: {error}")))?;
    let worker_laser = Laser::connect(connection_string)
        .await
        .map_err(sdk_error)?
        .with_default_stream(&stream);
    let mut agent = Agent::builder()
        .id(worker.clone())
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .handler(TriageEcho {
            source: worker.wire_id(),
        })
        .poll_interval(Duration::ZERO)
        .build()
        .spawn(worker_laser);
    agent.ready().await.map_err(sdk_error)?;
    let source: AgentId = "laser-bench-triage-client"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid triage client id: {error}")))?;
    let payload = text_payload(case.payload_bytes, seed);
    let operation = triage_operation(
        scoped,
        source.clone(),
        worker.clone(),
        payload.clone(),
        seed,
        case.timeout_millis,
    );
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, Arc::clone(&operation)).await?;
    let request_reply = measured_arm_with_network(
        "agdx_durable_request_reply",
        1,
        case,
        timeout,
        operation,
        monitored_processes,
        server_port,
    )
    .await;
    agent.shutdown().await.map_err(sdk_error)?;
    let request_reply = request_reply?;
    let sample = ticket_bytes(0, &payload)?;
    let (request_bytes, response_bytes) = application_bytes(&sample, &source, &worker)?;
    let network = request_reply.network.clone().ok_or_else(|| {
        BenchError::Invalid("AGDX triage network measurement was not captured".to_owned())
    })?;
    Ok(AgdxTriageEvidence {
        summary: AgdxTriageSummary {
            request_reply: request_reply.summary.clone(),
            request_body_bytes: sample.len(),
            request_bytes,
            response_body_bytes: sample.len(),
            response_bytes,
            network,
            configuration: json!({
                "comparison_role": "agdx_durable_log",
                "request": "typed_agdx_command",
                "response": "typed_agdx_response",
                "handler": "deterministic_echo",
                "ticket_encoding": "json",
                "envelope_encoding": "named_field_cbor",
                "application_byte_boundary": "encoded_envelope_and_iggy_user_headers_before_transport_framing",
                "durability": "iggy_log",
            }),
        },
        request_reply,
        payload,
    })
}

fn triage_operation(
    laser: Laser,
    source: AgentId,
    worker: AgentId,
    payload: String,
    seed: u64,
    timeout_millis: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let laser = laser.clone();
        let source = source.clone();
        let worker = worker.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let body = ticket_bytes(sequence, &payload).map_err(|error| error.to_string())?;
            let response = laser
                .contract(Router::to(worker.clone()))
                .from(source.clone())
                .payload(body.clone())
                .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
                .reply_on(AgentTopic::Responses)
                .conversation(ConversationId::derive(&format!(
                    "laser-bench-triage-{seed}-{sequence}"
                )))
                .deadline(Duration::from_millis(timeout_millis))
                .send()
                .await
                .map_err(|error| error.to_string())?;
            match response {
                Contract::Completed(message) if message.payload == body => Ok(()),
                Contract::Completed(_) => {
                    Err("AGDX triage response did not match the ticket".to_owned())
                }
                Contract::Failed(_) => Err("AGDX triage returned a failed contract".to_owned()),
                Contract::NotConsumed => Err("AGDX triage command was not consumed".to_owned()),
                Contract::TimedOut => Err("AGDX triage contract timed out".to_owned()),
            }
        })
    })
}

fn application_bytes(
    body: &[u8],
    source: &AgentId,
    worker: &AgentId,
) -> Result<(usize, usize), BenchError> {
    let conversation = WireConversationId::from_u128(1);
    let correlation = CorrelationId::from_u128(2);
    let request = AgentEnvelope::command(
        RecordId::from_u128(3),
        conversation,
        source.wire_id(),
        correlation,
        body.to_vec(),
    )
    .with_target(worker.wire_id());
    let response = AgentEnvelope::response(
        RecordId::from_u128(4),
        conversation,
        worker.wire_id(),
        correlation,
        body.to_vec(),
    )
    .with_target(source.wire_id());
    Ok((
        encoded_record_bytes(&request)?,
        encoded_record_bytes(&response)?,
    ))
}

fn encoded_record_bytes(envelope: &AgentEnvelope) -> Result<usize, BenchError> {
    let payload = encode_named(envelope)
        .map_err(|error| BenchError::Invalid(format!("AGDX byte encoding failed: {error}")))?;
    let mut headers = BTreeMap::new();
    headers.insert(
        header_key(AGENT_VERSION)?,
        HeaderValue::from(AGENT_OP_VERSION),
    );
    headers.insert(
        header_key(CONTENT_TYPE)?,
        HeaderValue::from(ContentType::Raw.code()),
    );
    headers.insert(
        header_key(CONVERSATION_ID)?,
        HeaderValue::from(envelope.conversation.as_u128()),
    );
    if let Some(target) = &envelope.target {
        headers.insert(
            header_key(TARGET_AGENT_ID)?,
            HeaderValue::from_str(target.as_str())
                .map_err(|error| BenchError::Invalid(error.to_string()))?,
        );
    }
    let header_bytes = headers
        .iter()
        .map(|(key, value)| key.as_bytes().len() + value.as_bytes().len() + HEADER_FRAMING_BYTES)
        .sum::<usize>();
    Ok(payload.len().saturating_add(header_bytes))
}

fn header_key(value: &str) -> Result<HeaderKey, BenchError> {
    HeaderKey::from_str(value).map_err(|error| BenchError::Invalid(error.to_string()))
}

fn ticket_bytes(sequence: u64, payload: &str) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&ticket_arguments(sequence, payload))
}

fn sdk_error(error: impl std::fmt::Display) -> BenchError {
    BenchError::Invalid(format!("AGDX triage operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_shared_ticket_when_encoded_then_should_match_mcp_arguments() {
        let bytes = ticket_bytes(7, "laser").expect("ticket should encode");
        let value: Value = serde_json::from_slice(&bytes).expect("ticket should decode");

        assert_eq!(value, ticket_arguments(7, "laser"));
    }
}
