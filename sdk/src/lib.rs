#![forbid(unsafe_code)]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod crate_readme {}

#[cfg(doctest)]
#[doc = include_str!("../../README.md")]
mod workspace_readme {}

#[cfg(feature = "a2a-bridge")]
pub mod a2a;
#[cfg(feature = "agent")]
pub mod agent;
#[cfg(feature = "agui")]
pub mod agui;
#[cfg(feature = "agent")]
pub mod batching;
#[cfg(feature = "agent")]
pub mod blob;
// The `bridge_hops` loop guard both protocol bridges stamp.
#[cfg(any(feature = "a2a-bridge", feature = "mcp-bridge"))]
mod bridge_hops;
pub mod capabilities;
#[cfg(feature = "streaming")]
mod connect_options;
#[cfg(feature = "agent")]
pub mod context;
#[cfg(feature = "agent")]
pub mod context_scope;
// The shared control-command publish path, used by the projection registry
// and session source registration.
#[cfg(any(feature = "projections", feature = "agent"))]
mod control;
#[cfg(feature = "agent")]
pub mod crash_context;
#[cfg(feature = "streaming")]
pub mod cursor;
#[cfg(feature = "destinations")]
pub mod destinations;
#[cfg(any(feature = "a2a-bridge", feature = "mcp-bridge"))]
pub mod edge_auth;
pub mod error;
pub mod filters;
pub mod fork;
#[cfg(feature = "agent")]
pub mod govern;
#[cfg(feature = "graph")]
pub mod graph;
#[cfg(feature = "agent")]
pub mod intent;
pub mod kv;
#[cfg(feature = "streaming")]
pub mod laser;
#[cfg(any(
    feature = "filters",
    feature = "fork",
    feature = "destinations",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac"
))]
mod managed;
#[cfg(feature = "mcp-bridge")]
pub mod mcp;
#[cfg(feature = "agent")]
pub mod memory;
#[cfg(feature = "streaming")]
pub mod message;
#[cfg(feature = "streaming")]
mod poll;
pub mod prelude;
#[cfg(feature = "projections")]
pub mod projections;
pub mod provenance;
#[cfg(feature = "streaming")]
mod publish_options;
pub mod query;
#[cfg(feature = "rbac")]
pub mod rbac;
#[cfg(feature = "schema-codecs")]
pub mod schema_codecs;
#[cfg(feature = "sign")]
pub mod sign;
pub mod snapshot;
#[cfg(feature = "agent")]
pub mod state_store;
#[cfg(feature = "streaming")]
pub mod stream;
#[cfg(feature = "agent")]
pub mod swarm;
#[cfg(feature = "agent")]
pub mod testing;
#[cfg(feature = "streaming")]
pub mod typed;
pub mod types;
#[cfg(feature = "watch")]
pub mod watch;

pub use error::LaserError;
// The wire contract crate, re-exported whole as `laser_sdk::wire::*` (the
// per-module shims above keep the historical paths working too).
pub use laser_wire as wire;
// The underlying Apache Iggy client crate, re-exported whole as
// `laser_sdk::iggy::*`. The SDK's own surface hands back iggy types (a
// `Laser::client()` handle, an `iggy_consumer_group()` builder, `Identifier`,
// `PollingStrategy`, header keys), so a consumer that drops to that seam names
// them through here at the exact version the SDK builds against, instead of
// adding a separate `iggy` dependency that must be kept version-matched by hand.
pub use iggy;
