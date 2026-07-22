use crate::agent::consumer::{AgentHandler, AgentMessage};
use crate::agent::ctx::AgentCtx;
use crate::error::LaserError;
use crate::memory::{MemoryHandle, MemoryKind};

/// Wraps an [`AgentHandler`] so the runtime turns each handled message into
/// queryable memory: after the inner handler succeeds, the message is remembered
/// under its conversation. This is the runtime half of agent memory. Recall is
/// the other half, and a handler reads it directly from
/// [`AgentCtx::laser`](crate::agent::AgentCtx::laser):
/// `ctx.laser().memory("agent-mem").recall(conversation).fetch().await`.
///
/// A memory write that fails does not fail the turn (the message was already
/// handled, and re-running it would repeat the handler's effects), so a failed
/// remember is dropped rather than retried.
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
            let mut remember = self
                .memory
                .remember(message.payload.clone())
                .scope(message.provenance.conversation_id)
                .kind(kind);
            if let Some(agent) = &message.provenance.agent {
                remember = remember.agent(agent.clone());
            }
            // A handled turn is durable regardless of whether its memory copy
            // lands, so a write failure is dropped, never retried.
            let _ = remember.send().await;
        }
        Ok(())
    }
}
