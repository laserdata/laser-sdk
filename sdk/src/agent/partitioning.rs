use crate::agent::SessionLayout;
use iggy::prelude::Partitioning;
use laser_wire::agent::{AgentId, AgentKind, ConversationId};

// Where one agent record lands within its topic, resolved from the stream's
// layout at the send boundary. The provenance partition key stays the
// conversation in every layout.
pub(crate) enum AgentPartitioning {
    // The session's own partition: the conversation as the message key.
    BySession,
    // A partition the layout declares for one agent.
    Explicit(u32),
}

// The topic one agent record lands on under a declared per-agent topic
// layout, or `None` to keep the topic it was sent on. Only `agent.sessions`
// sends move: an envelope command, response, error, or chunk (`kind` set), or
// a plain record (`kind` `None`), addressed to a declared agent goes to that
// agent's topic. Everything else, lifecycle and state included, stays on the
// lane.
pub(crate) fn destination_topic(
    layout: Option<&SessionLayout>,
    topic: &str,
    kind: Option<AgentKind>,
    target: Option<&AgentId>,
) -> Option<String> {
    let Some(SessionLayout::PerAgentTopic(topics)) = layout else {
        return None;
    };
    if topic != laser_wire::topics::AGENT_SESSIONS {
        return None;
    }
    let directed = kind.is_none_or(|kind| {
        matches!(
            kind,
            AgentKind::Command | AgentKind::Response | AgentKind::Error | AgentKind::Chunk
        )
    });
    if !directed {
        return None;
    }
    target.and_then(|agent| topics.get(agent)).cloned()
}

// The topic `agent` reads instead of `agent.sessions` under a declared
// per-agent topic layout, the one its addressed work and replies land on.
pub(crate) fn declared_topic(layout: Option<&SessionLayout>, agent: &AgentId) -> Option<String> {
    match layout {
        Some(SessionLayout::PerAgentTopic(topics)) => topics.get(agent).cloned(),
        _ => None,
    }
}

impl AgentPartitioning {
    // A command is keyed by its addressee and a reply by its requester, which
    // is the reply's addressee. Everything else, lifecycle and state
    // included, rides the session's partition. Only a declared per-agent
    // partition on `agent.sessions` moves a record off the session partition.
    pub(crate) fn resolve(
        layout: Option<&SessionLayout>,
        topic: &str,
        kind: AgentKind,
        target: Option<&AgentId>,
    ) -> Self {
        let Some(SessionLayout::PerAgentPartition(partitions)) = layout else {
            return Self::BySession;
        };
        if topic != laser_wire::topics::AGENT_SESSIONS {
            return Self::BySession;
        }
        let directed = matches!(
            kind,
            AgentKind::Command | AgentKind::Response | AgentKind::Error | AgentKind::Chunk
        );
        match target {
            Some(agent) if directed => partitions
                .get(agent)
                .map_or(Self::BySession, |partition| Self::Explicit(*partition)),
            _ => Self::BySession,
        }
    }

    pub(crate) fn into_partitioning(
        self,
        conversation: ConversationId,
    ) -> Result<Partitioning, crate::error::LaserError> {
        Ok(match self {
            Self::BySession => Partitioning::messages_key_str(&conversation.to_string())?,
            Self::Explicit(partition) => Partitioning::partition_id(partition),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn agent(name: &str) -> AgentId {
        name.parse().expect("valid agent id")
    }

    #[test]
    fn given_a_declared_partition_layout_when_routing_then_should_key_commands_by_addressee_and_replies_by_requester()
     {
        let layout = SessionLayout::PerAgentPartition(BTreeMap::from([
            (agent("planner"), 0),
            (agent("worker"), 2),
        ]));
        let sessions = laser_wire::topics::AGENT_SESSIONS;
        let route = |kind, target: Option<&str>| {
            let target = target.map(agent);
            match AgentPartitioning::resolve(Some(&layout), sessions, kind, target.as_ref()) {
                AgentPartitioning::Explicit(partition) => Some(partition),
                AgentPartitioning::BySession => None,
            }
        };
        assert_eq!(route(AgentKind::Command, Some("worker")), Some(2));
        assert_eq!(route(AgentKind::Response, Some("planner")), Some(0));
        assert_eq!(route(AgentKind::Command, Some("stranger")), None);
        assert_eq!(route(AgentKind::Status, None), None);
        assert_eq!(route(AgentKind::Event, Some("worker")), None);
        assert!(matches!(
            AgentPartitioning::resolve(
                Some(&layout),
                "agent.control",
                AgentKind::Command,
                Some(&agent("worker"))
            ),
            AgentPartitioning::BySession
        ));
        assert!(matches!(
            AgentPartitioning::resolve(None, sessions, AgentKind::Command, Some(&agent("worker"))),
            AgentPartitioning::BySession
        ));
    }

    #[test]
    fn given_a_declared_topic_layout_when_routing_then_should_move_only_addressed_work_off_the_lane()
     {
        let layout = SessionLayout::PerAgentTopic(BTreeMap::from([
            (agent("planner"), "planner.inbox".to_owned()),
            (agent("worker"), "worker.inbox".to_owned()),
        ]));
        let sessions = laser_wire::topics::AGENT_SESSIONS;
        let route = |kind, target: Option<&str>| {
            let target = target.map(agent);
            destination_topic(Some(&layout), sessions, kind, target.as_ref())
        };
        assert_eq!(
            route(Some(AgentKind::Command), Some("worker")).as_deref(),
            Some("worker.inbox")
        );
        assert_eq!(
            route(Some(AgentKind::Response), Some("planner")).as_deref(),
            Some("planner.inbox")
        );
        assert_eq!(
            route(Some(AgentKind::Error), Some("planner")).as_deref(),
            Some("planner.inbox")
        );
        assert_eq!(
            route(Some(AgentKind::Chunk), Some("worker")).as_deref(),
            Some("worker.inbox")
        );
        assert_eq!(route(None, Some("worker")).as_deref(), Some("worker.inbox"));
        assert_eq!(route(Some(AgentKind::Status), Some("worker")), None);
        assert_eq!(route(Some(AgentKind::Event), Some("worker")), None);
        assert_eq!(route(Some(AgentKind::Command), Some("stranger")), None);
        assert_eq!(route(Some(AgentKind::Command), None), None);
        assert_eq!(
            destination_topic(
                Some(&layout),
                "agent.control",
                Some(AgentKind::Command),
                Some(&agent("worker"))
            ),
            None
        );
        assert_eq!(
            declared_topic(Some(&layout), &agent("worker")).as_deref(),
            Some("worker.inbox")
        );
        assert_eq!(declared_topic(Some(&layout), &agent("stranger")), None);
        assert_eq!(declared_topic(None, &agent("worker")), None);
    }
}
