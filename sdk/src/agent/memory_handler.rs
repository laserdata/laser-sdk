use crate::agent::consumer::{AgentHandler, AgentMessage};
use crate::agent::ctx::AgentCtx;
use crate::error::LaserError;
use crate::memory::{MemoryHandle, MemoryKind};

/// Wraps an [`AgentHandler`] so the runtime turns each handled message into
/// queryable memory: after the inner handler succeeds, the message is remembered
/// under its conversation and attributed to the handling agent. This is the
/// runtime half of agent memory. Recall is the other half, and a handler reads it
/// directly from [`AgentCtx::laser`](crate::agent::AgentCtx::laser):
/// `ctx.laser().memory("agent-mem").recall(conversation).fetch().await`.
///
/// A memory write that fails does not fail the turn (the message was already
/// handled, and re-running it would repeat the handler's effects), so a failed
/// remember is logged as a warning rather than retried.
pub struct MemoryHandler<H> {
    inner: H,
    memory: MemoryHandle,
    remember_kind: Option<MemoryKind>,
}

impl<H> MemoryHandler<H> {
    /// Wrap `inner`, writing to `memory`. Remembering is off until
    /// [`auto_remember`](Self::auto_remember) selects a kind.
    pub fn new(inner: H, memory: MemoryHandle) -> Self {
        Self {
            inner,
            memory,
            remember_kind: None,
        }
    }

    /// Remember each successfully handled message under its conversation, stored
    /// as `kind` (`MemoryKind::Message` for a chat turn).
    #[must_use]
    pub fn auto_remember(mut self, kind: MemoryKind) -> Self {
        self.remember_kind = Some(kind);
        self
    }
}

impl<H: AgentHandler + Sync> AgentHandler for MemoryHandler<H> {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.inner.handle(message, ctx).await?;
        if let Some(kind) = self.remember_kind {
            let conversation = message.provenance.conversation_id;
            let mut remember = self
                .memory
                .remember(message.payload.clone())
                .scope(conversation)
                .kind(kind);
            if let Some(agent) = ctx.agent() {
                remember = remember.agent(agent.clone());
            }
            if let Err(error) = remember.send().await {
                tracing::warn!(
                    target: "laser",
                    conversation_id = %conversation,
                    %error,
                    "Memory handler could not remember the turn of conversation {conversation}: {error}"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::InboxRoute;
    use crate::laser::Laser;
    use crate::memory::{Embedder, Memory, MemoryQuery, MemoryScope};
    use crate::provenance::Provenance;
    use crate::testing::{agent_ctx, agent_message};
    use crate::types::{AgentId, ConversationId};
    use iggy::prelude::IggyClient;

    #[tokio::test]
    async fn given_a_message_from_another_agent_when_auto_remembered_then_should_stamp_the_handling_agent()
     {
        let memory = MemoryHandle::vector(UnitEmbedder);
        let handler = MemoryHandler::new(Handled, memory).auto_remember(MemoryKind::Message);
        let conversation = ConversationId::new();
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent("sender".parse().expect("valid agent"))
            .build();
        let message = agent_message(b"disk pressure on node-7".to_vec(), provenance);
        let laser = Laser::from_client(IggyClient::default());
        let worker: AgentId = "worker".parse().expect("valid agent");
        let ctx = agent_ctx(
            &laser,
            &message,
            Some(worker.clone()),
            None,
            InboxRoute::Advertised,
        );
        handler
            .handle(&message, &ctx)
            .await
            .expect("the turn is handled");
        let scope = |agent: &str| {
            MemoryScope::builder()
                .conversation(conversation)
                .agent(agent.parse().expect("valid agent"))
                .build()
        };
        let query = MemoryQuery::builder().build();
        let items = Memory::recall(&handler.memory, &scope("worker"), &query)
            .await
            .expect("recall as the handling agent");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].provenance.agent, Some(worker));
        let sender = Memory::recall(&handler.memory, &scope("sender"), &query)
            .await
            .expect("recall as the sender");
        assert!(sender.is_empty());
    }

    struct Handled;

    impl AgentHandler for Handled {
        async fn handle(
            &self,
            _message: &AgentMessage,
            _ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            Ok(())
        }
    }

    struct UnitEmbedder;

    impl Embedder for UnitEmbedder {
        async fn embed(&self, _text: &str) -> Result<Vec<f32>, LaserError> {
            Ok(vec![1.0])
        }
    }
}
