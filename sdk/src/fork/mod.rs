// Copy-on-write forks of the materialized read model. The records a fork
// handle returns and its error live in laser-wire and are re-exported here
// unconditionally. The request and reply frames stay at `laser_sdk::wire::fork`.
// The `ForkHandle` and its fluent builders stay in this crate behind the `fork`
// feature.

pub use laser_wire::fork::{ForkError, ForkInfo, ForkKind, ForkStatus};

#[cfg(feature = "fork")]
mod client;
#[cfg(feature = "fork")]
pub use client::{ForkCreateRequest, ForkHandle, ForkPutRequest};
