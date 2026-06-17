// Copy-on-write forks of the materialized read model. Wire types live in
// laser-wire and are re-exported here unconditionally. The `ForkHandle` and
// its fluent builders stay in this crate behind the `query` feature.

pub use laser_wire::fork::{
    ForkCreate, ForkDelete, ForkError, ForkInfo, ForkKind, ForkList, ForkOutcome, ForkPromote,
    ForkPut, ForkReply, ForkStatus,
};

#[cfg(feature = "query")]
mod client;
#[cfg(feature = "query")]
pub use client::{ForkCreateRequest, ForkHandle, ForkPutRequest};
