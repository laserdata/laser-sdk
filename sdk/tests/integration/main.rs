mod harness;
#[path = "../support/test_iggy.rs"]
mod test_iggy;

#[cfg(feature = "a2a-bridge")]
mod a2a;
mod agdx_consume;
mod agdx_stream;
#[cfg(feature = "agui")]
mod agui;
mod context;
mod contract;
mod deadletter;
mod decomposition;
mod fanout;
mod governance;
mod handoff;
mod human_input;
#[cfg(feature = "kv")]
mod managed_unsupported;
#[cfg(feature = "mcp-bridge")]
mod mcp;
mod memory;
mod provenance;
mod queue_pressure;
mod reconnect;
mod reliable;
mod replay;
mod request;
mod runtime;
mod session;
mod shutdown;
#[cfg(feature = "sign")]
mod signing;
mod state;
mod streaming;
#[cfg(feature = "query")]
mod typed_topics;
mod warm_dedup;
mod workflow;
