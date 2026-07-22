use crate::common::container::TestIggy;
use cucumber::World;
use laser_bdd::graph_engine::GraphEngine;
use laser_bdd::kv_engine::KvEngine;
use laser_bdd::memory_engine::MemoryEngine;
use laser_bdd::query_engine::QueryEngine;
use laser_sdk::kv::KvError;
use laser_sdk::memory::MemoryId;
use laser_sdk::prelude::{ConversationId, Laser, QueryResult};
use laser_sdk::query::ResultCode;
use std::collections::HashMap;
use std::fmt;

/// Shared scenario state. Steps connect a `Laser`, act on it, and record the
/// outcome (last result, assembled payloads, negotiated capabilities) for the
/// `Then` steps to assert against.
#[derive(Default, World)]
pub struct LaserWorld {
    pub laser: Option<Laser>,
    pub platform: Option<TestIggy>,
    pub conversation: Option<ConversationId>,
    /// `Ok(())` or the stringified error of the last fallible action.
    pub last_result: Option<Result<(), String>>,
    /// Number of records accepted by the last streaming batch publish.
    pub last_batch_count: Option<usize>,
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
    pub assembled_correlation_id: Option<String>,
    pub assembled_conversation_matches: bool,
    /// The in-memory reference query engine and the result of the last query,
    /// for the query-semantics scenarios (no Iggy, no transport).
    pub query_engine: Option<QueryEngine>,
    pub last_query: Option<QueryResult>,
    /// The in-memory reference KV store and the outcome of the last
    /// compare-and-swap, for the CAS-semantics scenarios (no Iggy, no transport).
    pub kv_engine: Option<KvEngine>,
    pub last_cas: Option<Result<u64, KvError>>,
    /// The in-memory reference memory engine and the ids of the items remembered
    /// so far (keyed by their text body), for the memory-semantics scenarios (no
    /// Iggy, no transport).
    pub memory_engine: Option<MemoryEngine>,
    pub memory_ids: HashMap<String, MemoryId>,
    /// The in-process semantic memory for the keyword/hybrid recall scenarios
    /// (deterministic token-hash embedder, no server).
    pub semantic_memory: Option<laser_sdk::memory::VectorMemory<TokenEmbedder>>,
    pub semantic_conversation: Option<ConversationId>,
    /// The in-memory reference graph engine, for the graph-semantics scenarios (no
    /// Iggy, no transport).
    pub graph_engine: Option<GraphEngine>,
    /// The governed `Laser` handle of the governance scenarios (the ungoverned
    /// `laser` stays available for reading the audit topic back).
    pub governed: Option<Laser>,
    pub bridge_hops: Vec<String>,
    pub bridge_loop_rejected: bool,
    pub bridge_task_state: Option<String>,
    pub reconstructed_state: Option<serde_json::Value>,
    pub agui_event_types: Vec<String>,
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

/// The deterministic bag-of-tokens embedder the semantic-memory scenarios use:
/// same text, same vector, no model, so the contract stays reproducible in
/// every SDK port.
pub struct TokenEmbedder;

impl laser_sdk::memory::Embedder for TokenEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, laser_sdk::LaserError> {
        const DIMS: usize = 64;
        let mut vector = vec![0.0f32; DIMS];
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|token| !token.is_empty())
        {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in token.to_ascii_lowercase().bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            vector[hash as usize % DIMS] += 1.0;
        }
        Ok(vector)
    }
}
