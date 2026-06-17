#![no_main]

// Decoding hostile CBOR into the wire types must never panic: each call returns
// a typed value or a DecodeError. This covers both directions of every managed
// surface - the request types a server decodes from an untrusted client, and
// the reply/envelope types a client decodes from a server - plus the agent
// envelope, whose hand-written per-kind validity matrix is the most complex
// logic in the crate, so a successful decode is pushed through validate() too.

use laser_wire::agent::{AgentEnvelope, validate};
use laser_wire::browse::{
    BrowseReply, DecodeRecord, GetProjection, GetSchema, ListProjections, ListSchemas,
    RegisterSchema,
};
use laser_wire::control::ControlEnvelope;
use laser_wire::fork::{ForkCreate, ForkPut, ForkReply};
use laser_wire::forward::{ForwardedCommand, ForwardedQuery};
use laser_wire::framing::decode_named;
use laser_wire::hello::{BackendAnnounce, HelloReply};
use laser_wire::kv::{KvCas, KvDelete, KvDeleteMany, KvGet, KvReply, KvScan, KvSet};
use laser_wire::query::{QueryEnvelope, QueryReply};
use laser_wire::result::{CommandError, ResultCode};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Agent envelope: decode + the full validity matrix.
    if let Ok(envelope) = decode_named::<AgentEnvelope>(data) {
        let _ = validate(&envelope);
        // The must-understand check must not panic on any decoded bitset.
        let _ = envelope.unmet_requirements(0);
        let _ = envelope.unmet_requirements(u64::MAX);
    }

    // Client-decoded replies and the capability probe.
    let _ = decode_named::<QueryReply>(data);
    let _ = decode_named::<KvReply>(data);
    let _ = decode_named::<ForkReply>(data);
    let _ = decode_named::<BrowseReply>(data);
    let _ = decode_named::<ControlEnvelope>(data);
    let _ = decode_named::<HelloReply>(data);
    let _ = decode_named::<BackendAnnounce>(data);

    // Server-decoded requests from an untrusted client (the security-critical
    // direction): the query IR (with its consistency level), the KV ops
    // including compare-and-swap, fork writes, registry browse, and the
    // forwarded frames the streaming server stamps.
    let _ = decode_named::<QueryEnvelope>(data);
    let _ = decode_named::<KvGet>(data);
    let _ = decode_named::<KvSet>(data);
    let _ = decode_named::<KvCas>(data);
    let _ = decode_named::<KvDelete>(data);
    let _ = decode_named::<KvDeleteMany>(data);
    let _ = decode_named::<KvScan>(data);
    let _ = decode_named::<ForkCreate>(data);
    let _ = decode_named::<ForkPut>(data);
    let _ = decode_named::<GetProjection>(data);
    let _ = decode_named::<ListProjections>(data);
    let _ = decode_named::<GetSchema>(data);
    let _ = decode_named::<ListSchemas>(data);
    let _ = decode_named::<RegisterSchema>(data);
    let _ = decode_named::<DecodeRecord>(data);
    let _ = decode_named::<ForwardedQuery>(data);
    let _ = decode_named::<ForwardedCommand>(data);
    let _ = decode_named::<CommandError>(data);

    // The unified result code: a decoded code maps to a numeric + HTTP status
    // without panicking on any variant, including an unrecognized one.
    if let Ok(code) = decode_named::<ResultCode>(data) {
        let _ = code.code();
        let _ = code.http_status();
    }
});
