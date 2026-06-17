use crate::provenance::Provenance;
use crate::types::AgentId;

/// Where a message is addressed: one specific agent, or every listener on the topic.
#[derive(Debug, Clone)]
pub enum Router {
    To(AgentId),
    Broadcast,
}

impl Router {
    /// Route to one agent (stamps `agdx.to`).
    pub fn to(agent: AgentId) -> Self {
        Self::To(agent)
    }

    /// Clear any target so every consumer-group member may handle the message.
    pub fn broadcast() -> Self {
        Self::Broadcast
    }

    /// Stamp (or clear) the target agent on `provenance`.
    pub fn apply(&self, provenance: &mut Provenance) {
        match self {
            Self::To(agent) => provenance.target_agent_id = Some(agent.clone()),
            Self::Broadcast => provenance.target_agent_id = None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConversationId;

    #[test]
    fn given_a_route_to_an_agent_when_applied_then_should_set_the_target() {
        let mut provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .build();
        Router::to("executor".parse().expect("executor is a valid agent id"))
            .apply(&mut provenance);
        assert_eq!(
            provenance
                .target_agent_id
                .expect("the target should be set")
                .as_str(),
            "executor"
        );
    }

    #[test]
    fn given_a_broadcast_route_when_applied_then_should_clear_the_target() {
        let mut provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .target_agent_id("executor".parse().expect("executor is a valid agent id"))
            .build();
        Router::broadcast().apply(&mut provenance);
        assert!(provenance.target_agent_id.is_none());
    }
}
