use iggy::prelude::Identifier;

/// A well-known agent topic, or a `Custom` one. Each maps to an Iggy topic name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTopic<'a> {
    /// Inbound commands to an agent.
    Commands,
    /// Replies from an agent.
    Responses,
    /// Tool-call requests.
    ToolCalls,
    /// Tool-call results.
    ToolResults,
    /// LLM input/output traces.
    LlmIo,
    /// Human-in-the-loop prompts and decisions.
    HumanInput,
    /// Audit / memory records.
    Audit,
    /// The dead-letter queue.
    Dlq,
    /// Any other topic, by `Identifier`.
    Custom(&'a Identifier),
}

impl AgentTopic<'_> {
    /// The static topic name, or `None` for `Custom`.
    pub const fn name(&self) -> Option<&'static str> {
        match self {
            Self::Commands => Some("agent.commands"),
            Self::Responses => Some("agent.responses"),
            Self::ToolCalls => Some("agent.tool_calls"),
            Self::ToolResults => Some("agent.tool_results"),
            Self::LlmIo => Some("agent.llm_io"),
            Self::HumanInput => Some("agent.human_input"),
            Self::Audit => Some("agent.audit"),
            Self::Dlq => Some("agent.dlq"),
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
        assert_eq!(AgentTopic::Commands.name(), Some("agent.commands"));
        assert_eq!(
            AgentTopic::Commands.as_identifier(),
            Identifier::named("agent.commands").expect("the topic name is a valid identifier")
        );
    }

    #[test]
    fn given_a_custom_topic_when_converted_then_should_carry_its_identifier() {
        let id = Identifier::named("agent.billing").expect("the topic name is a valid identifier");
        let topic = AgentTopic::Custom(&id);
        assert_eq!(topic.name(), None);
        assert_eq!(topic.as_identifier(), id);
    }
}
