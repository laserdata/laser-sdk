use crate::harness;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentKind, ConversationId, CorrelationId, OPERATION_CHAT,
};
use laser_sdk::wire::content::ContentType;
use std::sync::Arc;
use std::sync::Mutex;

struct Capture {
    seen: Arc<Mutex<Vec<AgentEnvelope>>>,
}

impl AgentHandler for Capture {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if let Some(envelope) = &message.envelope {
            self.seen
                .lock()
                .expect("the lock should not be poisoned")
                .push(envelope.clone());
        }
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_agdx_command_when_consumed_then_the_handler_should_see_the_decoded_envelope() {
    let laser = harness::laser().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Capture { seen: seen.clone() })
        .build()
        .spawn(laser.clone());

    // Publish a typed AGDX command (not a `send_agent` message), tunneling the
    // foreign payload byte-identical in the body with `agdx.ct = json`.
    let conversation = ConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0009);
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_000a);
    let params = br#"{"ask":"plan the trip"}"#.to_vec();
    laser
        .agdx(
            AgentTopic::Commands,
            "client".parse().expect("client is a valid agent id"),
            conversation,
        )
        .command(correlation, params.clone())
        .with_operation(OPERATION_CHAT)
        .content_type(ContentType::Json)
        .send()
        .await
        .expect("the AGDX command should publish");

    let envelopes = harness::eventually(|| {
        let seen = seen.clone();
        async move {
            let items = seen
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (!items.is_empty()).then_some(items)
        }
    })
    .await;

    assert_eq!(envelopes.len(), 1);
    let envelope = &envelopes[0];
    assert_eq!(envelope.kind, AgentKind::Command);
    assert_eq!(envelope.conversation, conversation);
    assert_eq!(envelope.correlation, Some(correlation));
    assert_eq!(envelope.source.as_str(), "client");
    // The tunneled remainder reaches the handler byte-identical.
    assert_eq!(envelope.body, params);
}
