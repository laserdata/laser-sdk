use crate::context::{ContextAssembler, ContextMessage};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;

/// Rebuilds in-memory state by folding a conversation's logged events (event sourcing).
pub struct ConversationState;

impl ConversationState {
    /// Replay `topics` for `conversation` and fold every message through `fold`, starting from `init`.
    pub async fn load<S, F>(
        laser: &Laser,
        conversation: ConversationId,
        topics: Vec<AgentTopic<'static>>,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &ContextMessage) -> S,
    {
        let history = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(topics)
            .policy(Box::new(crate::context::LastN(usize::MAX)))
            .build()
            .assemble(laser)
            .await?;
        Ok(history.iter().fold(init, fold))
    }
}
