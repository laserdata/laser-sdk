mod harness;
#[path = "../support/test_iggy.rs"]
mod test_iggy;

#[cfg(feature = "a2a-bridge")]
mod a2a;
mod agdx_consume;
mod agdx_stream;
#[cfg(feature = "agui")]
mod agui;
#[cfg(feature = "rbac")]
mod authz_receipts;
mod budget;
mod connection_events;
mod context;
mod contract;
mod deadletter;
mod decomposition;
mod encryption;
mod fanout;
mod filtered_wait;
#[cfg(feature = "filters")]
mod filters;
#[cfg(feature = "filters")]
mod filters_group;
mod governance;
mod handoff;
mod human_input;
mod idempotent_producer;
#[cfg(feature = "kv")]
mod managed_unsupported;
#[cfg(feature = "mcp-bridge")]
mod mcp;
mod memory;
mod pause;
#[cfg(all(feature = "agent", feature = "query"))]
mod presence_pin;
mod provenance;
mod queue_pressure;
mod reconnect;
mod reliable;
mod replay;
mod request;
mod runtime;
mod server_admin;
mod server_system;
mod session;
mod session_lane;
mod shutdown;
#[cfg(feature = "sign")]
mod signing;
mod state;
mod stream_admin;
mod streaming;
mod transports;
#[cfg(feature = "query")]
mod typed_topics;
mod warm_dedup;
mod workflow;
mod workflow_function;
