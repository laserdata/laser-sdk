#![cfg(not(feature = "vsr"))]

mod harness;
mod iggy_container;

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
mod reconnect;
mod reliable;
mod replay;
mod request;
mod runtime;
mod session;
mod shutdown;
mod state;
mod streaming;
#[cfg(feature = "query")]
mod typed_topics;
mod warm_dedup;
mod workflow;
