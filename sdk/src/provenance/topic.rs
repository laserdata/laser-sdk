use iggy::prelude::Identifier;

/// A well-known agent topic, or a `Custom` one. Each maps to an Iggy topic name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTopic<'a> {
    /// The session lane: commands, replies, tool and model records, user
    /// turns, lifecycle, and state, keyed by session.
    Sessions,
    /// High-volume chunk streams, collapsed into their response on a timeline.
    Streams,
    /// Process heartbeats listing the sessions a process holds leases on.
    Heartbeats,
    /// Session control requests, sent by operators only.
    Control,
    /// Memory records.
    Memory,
    /// Policy evidence and other audit records.
    Audit,
    /// The agent card registry: agents republish their cards here, and the
    /// registry read model folds it to the latest card per agent.
    Registry,
    /// The workflow journal: the engine records each step's outcome here, keyed by
    /// the workflow run, so a crashed run resumes by replaying it.
    WorkflowJournal,
    /// The dead-letter topic.
    Dlq,
    /// Any other topic, by `Identifier`.
    Custom(&'a Identifier),
}

impl AgentTopic<'_> {
    /// The static topic name, or `None` for `Custom`.
    pub const fn name(&self) -> Option<&'static str> {
        match self {
            Self::Sessions => Some(laser_wire::topics::AGENT_SESSIONS),
            Self::Streams => Some(laser_wire::topics::AGENT_STREAMS),
            Self::Heartbeats => Some(laser_wire::topics::AGENT_HEARTBEATS),
            Self::Control => Some(laser_wire::topics::AGENT_CONTROL),
            Self::Memory => Some(laser_wire::topics::AGENT_MEMORY),
            Self::Audit => Some(laser_wire::topics::AGENT_AUDIT),
            Self::Registry => Some(laser_wire::topics::AGENT_REGISTRY),
            Self::WorkflowJournal => Some(laser_wire::topics::AGENT_JOURNAL),
            Self::Dlq => Some(laser_wire::topics::AGENT_DLQ),
            Self::Custom(_) => None,
        }
    }

    /// The Iggy `Identifier` for this topic.
    pub fn as_identifier(&self) -> Identifier {
        match self {
            Self::Custom(id) => (**id).clone(),
            other => Identifier::named(other.name().expect("non-custom topic has a static name"))
                .expect("static topic name is a valid identifier"),
        }
    }

    /// The topic name as an owned `String`.
    pub fn topic_string(&self) -> String {
        match self {
            Self::Custom(id) => id.to_string(),
            other => other
                .name()
                .expect("non-custom topic has a static name")
                .to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_well_known_topic_when_converted_then_should_map_to_its_identifier() {
        assert_eq!(AgentTopic::Sessions.name(), Some("agent.sessions"));
        assert_eq!(
            AgentTopic::Sessions.as_identifier(),
            Identifier::named("agent.sessions").expect("the topic name is a valid identifier")
        );
    }

    #[test]
    fn given_a_custom_topic_when_converted_then_should_carry_its_identifier() {
        let id = Identifier::named("agent.metrics").expect("the topic name is a valid identifier");
        let topic = AgentTopic::Custom(&id);
        assert_eq!(topic.name(), None);
        assert_eq!(topic.as_identifier(), id);
    }
}
