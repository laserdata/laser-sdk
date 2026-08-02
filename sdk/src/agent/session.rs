use crate::types::ConversationId;

/// How a user key maps to a conversation: a fresh one per call, or a stable one per user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPolicy {
    PerCall,
    PerUser,
}

impl SessionPolicy {
    /// The conversation id for `key` (random for `PerCall`, derived deterministically for `PerUser`).
    pub fn conversation_for(&self, key: &str) -> ConversationId {
        match self {
            Self::PerCall => ConversationId::new(),
            Self::PerUser => ConversationId::derive(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_per_user_policy_when_deriving_for_a_key_then_should_be_stable_and_distinct() {
        let policy = SessionPolicy::PerUser;
        assert_eq!(
            policy.conversation_for("alice"),
            policy.conversation_for("alice")
        );
        assert_ne!(
            policy.conversation_for("alice"),
            policy.conversation_for("bob")
        );
    }

    #[test]
    fn given_per_call_policy_when_deriving_twice_then_should_be_unique() {
        let policy = SessionPolicy::PerCall;
        assert_ne!(policy.conversation_for("x"), policy.conversation_for("x"));
    }
}
