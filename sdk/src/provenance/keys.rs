// The provenance header dictionary lives in laser-wire (it is on-the-wire
// contract, shared with every port). Re-exported here so the historical
// `provenance::keys::*` paths keep resolving.
pub use laser_wire::headers::{
    AGENT_ID, CAUSAL_PARENT, CONVERSATION_ID, COST_USD, DEADLINE, FENCE, IDEMPOTENCY_KEY,
    PARENT_CONVERSATION_ID, ROOT_CONVERSATION_ID, TARGET_AGENT_ID, USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
};
