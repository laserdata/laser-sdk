// The provenance surface. The header-key dictionary lives in laser-wire and is
// always available via `keys`. The runtime (the `Provenance` struct, header
// encode/decode, `AgentTopic`) stays behind the `provenance` feature.

pub mod keys;

#[cfg(feature = "provenance")]
mod runtime;
#[cfg(feature = "provenance")]
pub mod topic;

#[cfg(feature = "provenance")]
pub use runtime::*;
