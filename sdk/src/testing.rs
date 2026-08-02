use crate::agent::{AgentCtx, AgentMessage, InboxRoute};
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, MessageId};

/// Build an [`AgentMessage`] for a handler unit test: a plain (non-envelope)
/// message carrying `payload` and `provenance`, seated at the start of a
/// synthetic partition. Feed it to an [`AgentHandler`](crate::agent::AgentHandler)
/// to exercise the handler without the reliable consumer or a live server.
pub fn agent_message(payload: impl Into<Vec<u8>>, provenance: Provenance) -> AgentMessage {
    AgentMessage {
        provenance,
        payload: payload.into(),
        id: MessageId::new(0, 0),
        envelope: None,
        content_type: None,
        verified_principal: None,
    }
}

/// Build an [`AgentCtx`] over a caller-owned `laser` and `message`, so a handler
/// test can call `handle(&message, &ctx)` directly. The ctx borrows both, so keep
/// them alive for the duration of the call. The `laser` supplies whatever IO the
/// ctx helpers use (point it at a test server for `respond`/`fan_out`). A handler
/// that only reads its message needs no server at all.
pub fn agent_ctx<'a>(
    laser: &'a Laser,
    message: &'a AgentMessage,
    agent: Option<AgentId>,
    respond_on: Option<AgentTopic<'static>>,
    inbox_route: InboxRoute,
) -> AgentCtx<'a> {
    AgentCtx::new(
        laser,
        message,
        agent,
        respond_on,
        inbox_route,
        #[cfg(feature = "sign")]
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConversationId;

    #[test]
    fn given_a_test_message_when_built_then_should_carry_payload_and_provenance() {
        let conversation = ConversationId::new();
        let provenance = Provenance::builder().conversation_id(conversation).build();
        let message = agent_message(b"hello".to_vec(), provenance);
        assert_eq!(message.body(), b"hello");
        assert_eq!(message.provenance.conversation_id, conversation);
        assert!(message.envelope.is_none());
    }
}
