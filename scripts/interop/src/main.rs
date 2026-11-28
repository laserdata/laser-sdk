//! The Rust half of the Go/Rust interop check.
//!
//! The Go live suite drives this binary. One client publishes a record and the
//! other reads it back, which is the only way to prove the two ports agree
//! about bytes on a real socket. A fixture cannot prove it: both sides would
//! be checking their own encoder against their own expectation.
//!
//! Usage:
//!   laser-interop publish <stream> <topic>
//!   laser-interop verify  <stream> <topic>
//!
//! `publish` writes a record whose provenance headers carry a full spine, and
//! whose body is a JSON mirror of the very same values. `verify` decodes the
//! headers with this client's own decoder, renders them back into that mirror
//! shape, and compares the two.
//!
//! The mirror is what makes the check honest across languages. Neither side
//! holds a hard-coded copy of the other's fixture, so nothing has to be kept
//! in step by hand. The body says what the publisher meant, the headers say
//! what it actually wrote, and a disagreement between the two is exactly the
//! interop defect worth finding.

use iggy::prelude::{
    Consumer, Identifier, IggyMessage, MessageClient, PollOptions, PollingStrategy,
};
use laser_sdk::laser::Laser;
use laser_sdk::prelude::LaserError;
use laser_sdk::provenance::{LlmUsage, Provenance};
use laser_sdk::types::{AgentId, ConversationId, MessageId};
use serde_json::{Value, json};
use std::process::ExitCode;

/// The partition the check publishes to and reads from.
///
/// Partitions are numbered from zero on this server, and a consumer pointed at
/// a partition that does not exist reads nothing rather than failing, so the
/// number is stated once here instead of being guessed in two places.
const PARTITION: u32 = 0;

#[tokio::main]
async fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let [command, stream, topic] = arguments.as_slice() else {
        eprintln!("usage: laser-interop <publish|verify> <stream> <topic>");
        return ExitCode::FAILURE;
    };
    let Ok(connection) = std::env::var("LASER_CONNECTION_STRING") else {
        eprintln!("LASER_CONNECTION_STRING is required");
        return ExitCode::FAILURE;
    };

    let result = match command.as_str() {
        "publish" => publish(&connection, stream, topic).await,
        "verify" => verify(&connection, stream, topic).await,
        other => {
            eprintln!("unknown command {other}, want publish or verify");
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            eprintln!("{report}");
            ExitCode::FAILURE
        }
    }
}

/// Publishes one record carrying a full provenance spine and a body that
/// mirrors it.
async fn publish(connection: &str, stream: &str, topic: &str) -> Result<(), String> {
    let laser = connect(connection, stream).await?;
    let topic_handle = laser.topic(topic);
    topic_handle
        .ensure(1, None)
        .await
        .map_err(|error| format!("create the topic: {error}"))?;

    let provenance = sample_provenance()?;
    let mirror = mirror_of(&provenance);
    let body = serde_json::to_vec(&mirror).map_err(|error| format!("encode the body: {error}"))?;

    topic_handle
        .publish()
        .payload(body)
        .provenance(&provenance)
        .send()
        .await
        .map_err(|error| format!("publish the record: {error}"))?;

    // The mirror goes to stdout so the Go side can see what Rust claims it
    // wrote, which turns a mismatch into a readable diff rather than a bare
    // failure.
    println!("{mirror}");
    Ok(())
}

/// Reads the topic and checks every record's headers against its own body.
async fn verify(connection: &str, stream: &str, topic: &str) -> Result<(), String> {
    let laser = connect(connection, stream).await?;
    let messages = poll(&laser, stream, topic).await?;
    if messages.is_empty() {
        return Err(format!(
            "no records on {stream}/{topic} partition {PARTITION}: \
             the publisher wrote nothing this client can see"
        ));
    }

    for message in &messages {
        let body: Value = serde_json::from_slice(&message.payload)
            .map_err(|error| format!("read the record body as JSON: {error}"))?;
        let provenance = Provenance::try_from(message).map_err(|error| {
            format!("decode the provenance headers this client just read back: {error}")
        })?;
        let decoded = mirror_of(&provenance);
        if decoded != body {
            return Err(format!(
                "the header block and the body disagree.\n  body says:    {body}\n  headers say:  {decoded}"
            ));
        }
        println!("{decoded}");
    }
    Ok(())
}

/// Opens the connection the check runs over.
async fn connect(connection: &str, stream: &str) -> Result<Laser, String> {
    Laser::connect_with_stream(connection, stream)
        .await
        .map_err(|error: LaserError| format!("connect to {connection}: {error}"))
}

/// Drains the partition from its first offset.
async fn poll(laser: &Laser, stream: &str, topic: &str) -> Result<Vec<IggyMessage>, String> {
    let stream_id = Identifier::named(stream).map_err(|error| format!("stream name: {error}"))?;
    let topic_id = Identifier::named(topic).map_err(|error| format!("topic name: {error}"))?;
    let consumer_id =
        Identifier::named("interop-verifier").map_err(|error| format!("consumer name: {error}"))?;

    let polled = laser
        .client()
        .poll_messages(
            &stream_id,
            &topic_id,
            PARTITION,
            &Consumer::new(consumer_id),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 100,
                ..PollOptions::default()
            },
        )
        .await
        .map_err(|error| format!("read {stream}/{topic}: {error}"))?;
    Ok(polled.messages)
}

/// The provenance this check publishes.
///
/// Every optional field is populated, because a field nobody exercises is
/// where an encoding difference between two ports survives unnoticed.
fn sample_provenance() -> Result<Provenance, String> {
    let agent: AgentId = "rust-author"
        .parse()
        .map_err(|error| format!("author id: {error}"))?;
    let target: AgentId = "go-reader"
        .parse()
        .map_err(|error| format!("target id: {error}"))?;

    Ok(Provenance {
        conversation_id: ConversationId::new(),
        causal_parent: Some(MessageId::new(PARTITION, 42)),
        parent_conversation_id: Some(ConversationId::new()),
        root_conversation_id: Some(ConversationId::new()),
        agent: Some(agent),
        target_agent_id: Some(target),
        usage: Some(LlmUsage {
            input_tokens: Some(1024),
            output_tokens: Some(256),
            // A value that is exact in binary floating point, so a difference
            // in the comparison means a difference on the wire rather than the
            // last bit of a decimal that never round-tripped cleanly anyway.
            cost_usd: Some(0.015625),
        }),
        // Whole seconds, because the header carries microseconds and a value
        // with finer resolution than that would fail on its own truncation
        // rather than on anything the other client did.
        deadline_at: Some(iggy::prelude::IggyTimestamp::from(1_800_000_000_000_000u64)),
        idempotency_key: Some("interop-idempotency-key".to_owned()),
        correlation_id: Some("interop-correlation-id".to_owned()),
        fence_token: Some(99),
    })
}

/// Renders a provenance block into the shared JSON mirror shape.
///
/// Both ports render the same field names, and ids render through each port's
/// own Display, which is the canonical 26-character Crockford base32 form on
/// both sides. That is the point of comparison: if the two disagree about how
/// an id is spelled, the comparison fails, which is the finding.
fn mirror_of(provenance: &Provenance) -> Value {
    let usage = provenance.usage.clone().unwrap_or_default();
    json!({
        "conversation": provenance.conversation_id.to_string(),
        "parent_conversation": provenance.parent_conversation_id.map(|id| id.to_string()),
        "root_conversation": provenance.root_conversation_id.map(|id| id.to_string()),
        "agent": provenance.agent.as_ref().map(|id| id.to_string()),
        "target_agent": provenance.target_agent_id.as_ref().map(|id| id.to_string()),
        "causal_parent": provenance.causal_parent.map(|id| id.to_string()),
        "idempotency_key": provenance.idempotency_key,
        "correlation_id": provenance.correlation_id,
        "fence_token": provenance.fence_token,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cost_usd": usage.cost_usd,
        "deadline_micros": provenance.deadline_at.map(|at| at.as_micros()),
    })
}
