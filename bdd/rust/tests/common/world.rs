use cucumber::World;
use laser_bdd::kv_engine::KvEngine;
use laser_bdd::query_engine::QueryEngine;
use laser_sdk::kv::KvError;
use laser_sdk::prelude::{ConversationId, Laser, QueryResult};
use laser_sdk::query::ResultCode;
use std::fmt;

/// Shared scenario state. Steps connect a `Laser`, act on it, and record the
/// outcome (last result, assembled payloads, negotiated capabilities) for the
/// `Then` steps to assert against.
#[derive(Default, World)]
pub struct LaserWorld {
    pub laser: Option<Laser>,
    pub conversation: Option<ConversationId>,
    /// `Ok(())` or the stringified error of the last fallible action.
    pub last_result: Option<Result<(), String>>,
    /// The unified result code of the last failed managed call, for the
    /// result-space scenarios.
    pub last_code: Option<ResultCode>,
    /// Whether a must-understand marker left a requirement unmet for a receiver
    /// that lacks the demanded feature bits.
    pub must_understand_unmet: Option<bool>,
    /// Negotiated capabilities, set by the capabilities steps.
    pub managed_query: Option<bool>,
    pub managed_kv: Option<bool>,
    pub forks: Option<bool>,
    pub kv_cas: Option<bool>,
    pub read_your_writes: Option<bool>,
    pub strong_consistency: Option<bool>,
    /// Payloads of the assembled conversation, in log order (lossy text view).
    pub assembled_payloads: Vec<String>,
    /// The same payloads as raw bytes, for decoding a binary AGDX envelope.
    pub assembled_raw: Vec<Vec<u8>>,
    /// Provenance of the first assembled message.
    pub assembled_agent: Option<String>,
    pub assembled_idempotency_key: Option<String>,
    pub assembled_conversation_matches: bool,
    /// The in-memory reference query engine and the result of the last query,
    /// for the query-semantics scenarios (no Iggy, no transport).
    pub query_engine: Option<QueryEngine>,
    pub last_query: Option<QueryResult>,
    /// The in-memory reference KV store and the outcome of the last
    /// compare-and-swap, for the CAS-semantics scenarios (no Iggy, no transport).
    pub kv_engine: Option<KvEngine>,
    pub last_cas: Option<Result<u64, KvError>>,
}

impl fmt::Debug for LaserWorld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LaserWorld")
            .field("connected", &self.laser.is_some())
            .field("conversation", &self.conversation)
            .field("last_result", &self.last_result)
            .field("assembled_payloads", &self.assembled_payloads)
            .finish()
    }
}

impl LaserWorld {
    pub fn laser(&self) -> &Laser {
        self.laser.as_ref().expect("a Laser is connected")
    }

    pub fn conversation(&self) -> ConversationId {
        self.conversation.expect("a conversation was started")
    }
}
