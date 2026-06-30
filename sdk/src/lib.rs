#![forbid(unsafe_code)]

#[cfg(feature = "a2a-bridge")]
pub mod a2a;
#[cfg(feature = "agent")]
pub mod agent;
#[cfg(feature = "query")]
pub mod agent_tasks;
#[cfg(feature = "agui")]
pub mod agui;
pub mod capabilities;
#[cfg(feature = "agent")]
pub mod context;
#[cfg(any(feature = "agent", feature = "query"))]
pub mod cursor;
pub mod error;
pub mod fork;
pub mod kv;
#[cfg(any(feature = "agent", feature = "query"))]
pub mod laser;
#[cfg(feature = "mcp-bridge")]
pub mod mcp;
#[cfg(feature = "agent")]
pub mod memory;
#[cfg(any(feature = "agent", feature = "query"))]
pub mod message;
#[cfg(any(feature = "agent", feature = "query"))]
mod poll;
pub mod prelude;
pub mod provenance;
pub mod query;
#[cfg(feature = "schema-codecs")]
pub mod schema_codecs;
#[cfg(feature = "sign")]
pub mod sign;
pub mod snapshot;
#[cfg(feature = "agent")]
pub mod state_store;
pub mod types;

pub use error::LaserError;
// The wire contract crate, re-exported whole as `laser_sdk::wire::*` (the
// per-module shims above keep the historical paths working too).
pub use laser_wire as wire;
