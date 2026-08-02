use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentId, AgentKind, ConversationId as WireConversationId, CorrelationId,
    RecordId, features,
};
use laser_sdk::wire::framing::{decode_named, encode_named};
use std::time::Duration;

// A receiver that implements no must-understand features yet (the open-world
// default): it understands bit set `features::NONE`.
const RECEIVER_UNDERSTOOD: u64 = features::NONE;

fn sample_event(must_understand: u64) -> AgentEnvelope {
    AgentEnvelope::event(
        RecordId::from_u128(1),
        WireConversationId::from_u128(2),
        "source-agent".parse::<AgentId>().expect("valid agent id"),
        b"gated".to_vec(),
    )
    .requiring(must_understand)
}

async fn send_command(world: &mut LaserWorld, payload: &str, agent: &str, idempotency_key: &str) {
    let conversation = world.conversation();
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        .agent(agent.parse().expect("a valid agent id"))
        .idempotency_key(idempotency_key.to_owned())
        .build();
    let result = world
        .laser()
        .send_agent(
            AgentTopic::Commands,
            payload.as_bytes().to_vec(),
            &provenance,
        )
        .await;
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when(
    regex = r#"^I send an agent command "([^"]+)" with agent "([^"]+)" and idempotency key "([^"]+)"$"#
)]
async fn send_one(world: &mut LaserWorld, payload: String, agent: String, idempotency_key: String) {
    send_command(world, &payload, &agent, &idempotency_key).await;
}

#[when(
    regex = r#"^I send an agent command "([^"]+)" with agent "([^"]+)" and correlation id "([^"]+)"$"#
)]
async fn send_with_correlation(
    world: &mut LaserWorld,
    payload: String,
    agent: String,
    correlation_id: String,
) {
    let conversation = world.conversation();
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        .agent(agent.parse().expect("a valid agent id"))
        .correlation_id(correlation_id)
        .build();
    let result = world
        .laser()
        .send_agent(AgentTopic::Commands, payload.into_bytes(), &provenance)
        .await;
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[then(regex = r#"^the assembled message correlation id is "([^"]+)"$"#)]
async fn correlation_id_is(world: &mut LaserWorld, expected: String) {
    assert_eq!(
        world.assembled_correlation_id.as_deref(),
        Some(expected.as_str())
    );
}

#[when(regex = r#"^I send agent commands "([^"]+)", "([^"]+)", "([^"]+)"$"#)]
async fn send_three(world: &mut LaserWorld, first: String, second: String, third: String) {
    for (index, payload) in [first, second, third].iter().enumerate() {
        send_command(world, payload, "planner", &format!("k{index}")).await;
    }
}

#[when(regex = r#"^I publish an AGDX command "([^"]+)" via the typed producer$"#)]
async fn agdx_command(world: &mut LaserWorld, payload: String) {
    let conversation = WireConversationId::from(world.conversation());
    let correlation = CorrelationId::from_u128(1);
    let result = world
        .laser()
        .agdx(
            AgentTopic::Commands,
            "planner".parse().expect("a valid agent id"),
            conversation,
        )
        .command(correlation, payload.into_bytes())
        .send()
        .await;
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

async fn assemble_conversation(world: &LaserWorld, conversation: ConversationId) -> Vec<Vec<u8>> {
    let messages = ContextAssembler::builder()
        .conversation_id(conversation)
        .topics(vec![AgentTopic::Commands])
        .build()
        .assemble(world.laser())
        .await
        .expect("assemble the conversation");
    messages
        .iter()
        .map(|message| message.payload.as_slice().to_vec())
        .collect()
}

#[when("I assemble the conversation")]
async fn assemble(world: &mut LaserWorld) {
    let conversation = world.conversation();

    // Reads off the log are eventually consistent: poll until the first message
    // lands, then settle briefly so a multi-message conversation is complete.
    let mut payloads = assemble_conversation(world, conversation).await;
    let mut tries = 0;
    while payloads.is_empty() && tries < 40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        payloads = assemble_conversation(world, conversation).await;
        tries += 1;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let settled = assemble_conversation(world, conversation).await;
    if settled.len() > payloads.len() {
        payloads = settled;
    }

    // Re-read the typed provenance of the first message for the field asserts.
    let messages = ContextAssembler::builder()
        .conversation_id(conversation)
        .topics(vec![AgentTopic::Commands])
        .build()
        .assemble(world.laser())
        .await
        .expect("assemble the conversation");
    if let Some(first) = messages.first() {
        world.assembled_agent = first
            .provenance
            .agent
            .as_ref()
            .map(|agent| agent.as_str().to_owned());
        world.assembled_idempotency_key = first.provenance.idempotency_key.clone();
        world.assembled_correlation_id = first.provenance.correlation_id.clone();
        world.assembled_conversation_matches = first.provenance.conversation_id == conversation;
    }
    world.assembled_payloads = payloads
        .iter()
        .map(|payload| String::from_utf8_lossy(payload).into_owned())
        .collect();
    world.assembled_raw = payloads;
}

#[then(regex = r#"^the assembled message payload is "([^"]+)"$"#)]
async fn payload_is(world: &mut LaserWorld, expected: String) {
    assert!(
        world.assembled_payloads.iter().any(|p| p == &expected),
        "expected payload {expected:?} among {:?}",
        world.assembled_payloads
    );
}

#[then(regex = r#"^the AGDX command body is "([^"]+)"$"#)]
async fn agdx_command_body_is(world: &mut LaserWorld, expected: String) {
    let raw = world
        .assembled_raw
        .first()
        .expect("an AGDX envelope was assembled");
    let envelope: AgentEnvelope = decode_named(raw).expect("decode the AGDX envelope");
    assert_eq!(envelope.kind, AgentKind::Command, "expected a command kind");
    assert_eq!(
        envelope.body.as_slice(),
        expected.as_bytes(),
        "the AGDX command body should round-trip"
    );
}

#[then(regex = r#"^the assembled message agent is "([^"]+)"$"#)]
async fn agent_is(world: &mut LaserWorld, expected: String) {
    assert_eq!(world.assembled_agent.as_deref(), Some(expected.as_str()));
}

#[then(regex = r#"^the assembled message idempotency key is "([^"]+)"$"#)]
async fn idempotency_key_is(world: &mut LaserWorld, expected: String) {
    assert_eq!(
        world.assembled_idempotency_key.as_deref(),
        Some(expected.as_str())
    );
}

#[then("the assembled message belongs to the conversation")]
async fn belongs_to_conversation(world: &mut LaserWorld) {
    assert!(
        world.assembled_conversation_matches,
        "the assembled message should carry the conversation id"
    );
}

#[then(regex = r#"^the assembled payloads are "([^"]+)", "([^"]+)", "([^"]+)" in order$"#)]
async fn payloads_in_order(world: &mut LaserWorld, first: String, second: String, third: String) {
    assert_eq!(world.assembled_payloads, vec![first, second, third]);
}

#[when("I build an agent event requiring feature bits the receiver lacks")]
async fn build_must_understand_event(world: &mut LaserWorld) {
    // Demand bits 0 and 2, round-trip through the wire, then ask whether a
    // receiver understanding none of them has an unmet requirement.
    let envelope = sample_event(0b101);
    let payload = encode_named(&envelope).expect("the marked envelope encodes");
    let back: AgentEnvelope = decode_named(&payload).expect("the marked envelope decodes");
    world.must_understand_unmet = Some(back.unmet_requirements(RECEIVER_UNDERSTOOD) != 0);
}

#[when("I build a plain agent event")]
async fn build_plain_event(world: &mut LaserWorld) {
    let envelope = sample_event(features::NONE);
    let payload = encode_named(&envelope).expect("the envelope encodes");
    let back: AgentEnvelope = decode_named(&payload).expect("the envelope decodes");
    world.must_understand_unmet = Some(back.unmet_requirements(RECEIVER_UNDERSTOOD) != 0);
}

#[then("the receiver rejects it as not understood")]
async fn receiver_rejects(world: &mut LaserWorld) {
    assert_eq!(
        world.must_understand_unmet,
        Some(true),
        "a receiver lacking a required feature bit must have an unmet requirement"
    );
}

#[then("the receiver understands it")]
async fn receiver_understands(world: &mut LaserWorld) {
    assert_eq!(
        world.must_understand_unmet,
        Some(false),
        "an unmarked envelope must leave no requirement unmet"
    );
}
