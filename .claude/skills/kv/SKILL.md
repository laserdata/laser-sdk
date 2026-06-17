---
name: kv
description: The key-value store client - `sdk/src/kv/` (feature `kv`, builds on `query`). Use when changing `Laser::kv` or the get/set/delete/scan builders (`sdk/src/kv/client.rs`). The `AGDX_KV_*` codes, the request/reply wire types, and the key/value caps live in laser-wire (`wire/src/kv.rs`, `wire/src/codes.rs`, `wire/src/limits.rs`) - change them there per the wire-contract skill. Client-only. The backend is managed-side. Wire in the AGDX spec
---

# Key-value store (client)

A small managed key-value store next to the query layer, reachable on the one Iggy connection. Like `query`, the SDK ships only the **client**. The store lives in LaserData Cloud's managed point-state backend and the Iggy server forwards the ops. All of it is behind the `kv` feature, which builds on `query` (shared managed bridge + `Laser` plumbing). Wire: the AGDX spec

## What ships here (`sdk/src/kv/mod.rs`)

- `Laser::kv(namespace)` -> a cheap namespace-scoped `Kv<'a>` handle.
- Reads: `get` (`Option<Vec<u8>>`), `get_entry` (`KvEntry` with expiry), `get_typed::<T>` (JSON-decode), `get_as::<C, T>` (decode with any `Decoder`).
- `set(key)` builder: `.bytes(impl AsRef<[u8]>)` / `.json(&v)?` / `.msgpack(&v)?` / `.encode_with::<C, _>(&v)?` (any `Codec` - Avro / Protobuf / Arrow / your own), optional `.ttl(Duration)` / `.expires_at(epoch_micros)`, then `.send()`. Values are opaque bytes, so the codec is the caller's choice on both ends.
- Compare-and-swap on the same `set` builder: `.expect_version(n)` (apply only if the key holds version `n`) or `.expect_absent()` (create-if-absent), then `.commit().await -> u64` (the new version). A precondition miss is `KvError::VersionConflict { current: Option<u64> }` (`LaserError::is_version_conflict()`) carrying the present version (`None` = absent), so the caller re-reads and retries. `KvEntry.version` (from `get_entry`) is the compare token, and `0` means an unversioned store.
- `delete(key)` -> `bool` (existed). `delete_many()` builder (`.prefix` / `.range` / `.key_contains`, same bounds as scan) -> count removed. No bounds clears the namespace. `scan()` builder: `.prefix` / `.range` (byte order) / `.key_contains(str)` (valid-UTF-8 keys only) / `.limit` / `.cursor` -> `.fetch()` (one `KvPage`) or `.entries()` (walk all pages by cursor).
- Keys / values cross the API as `impl AsRef<[u8]>` in, `Vec<u8>` out (no `bytes` crate leak). `KvEntry { key: Vec<u8>, value: Vec<u8>, expires_at_micros }`, `key_str()` gives the UTF-8 view when the key is valid UTF-8, `decode_value`/`decode_value_with::<C, _>` decode the value.
- `execute_kv(code, request)` (crate-internal) gates on `Capabilities::managed_kv` (set by the connect-time `AGDX_HELLO` probe against a managed deployment), encodes the request as CBOR, sends it as the op's managed command via `Laser::send_raw_with_response`, decodes `KvReply`.

## Wire (the managed KV contract)

- One managed command **per op** in the KV block (§13/§14): `AGDX_KV_GET_CODE` 1_000_200, `AGDX_KV_SET_CODE` 1_000_201, `AGDX_KV_SCAN_CODE` 1_000_202, `AGDX_KV_DELETE_CODE` 1_000_203, `AGDX_KV_DELETE_MANY_CODE` 1_000_204, `AGDX_KV_NAMESPACES_CODE` 1_000_205, `AGDX_KV_CAS_CODE` 1_000_206. `KV_OP_VERSION` = 1 (CAS is additive: an incapable backend rejects the code -> `Unsupported`, advertised by the `kv_cas` capability flag, not a version bump).
- Requests: `KvGet` / `KvSet` / `KvCas` / `KvDelete` / `KvScan` / `KvDeleteMany` (CBOR-named, each carries `v` + `namespace`). `KvCas` adds `expect: CasExpect = Match(u64) | Absent`. Reply: `KvReply = Ok(KvOutcome) | Err(KvError)` where `KvOutcome = Value(Option<KvEntry>) | Written | Committed { version } | Deleted(bool) | DeletedMany(usize) | Page(KvPage) | Namespaces(..)` and `KvError` adds `VersionConflict { current: Option<u64> }`. `KvEntry` carries `version: u64` (skip-when-zero).
- `key` and `value` are arbitrary bytes, a CBOR byte string (byte-exact). Scan bounds (`prefix`/`start`/`end`/`cursor`) are bytes, `key_contains` is a string.
- The managed deployment wraps each op in the shared `ForwardedCommand { user_id, client_id, correlation, read_all, command_code, payload }` keyed frame (also used by registry browse, and `AGDX_QUERY` keeps its own `ForwardedQuery`), stamping identity. The forwarded-frame shapes live in laser-wire (`wire/src/forward.rs`). The dispatch and the backend are managed-side behavior, not in this repo.

## Conventions + caps

- **Keys** arbitrary bytes, max 512 B. **Values** arbitrary opaque bytes, max 8 MiB (bounded so the embedded store never bloats). Scan limit 1000 (default 100). The SDK validates key length + value size before sending. Oversized fails fast as `LaserError::Kv`. Byte-prefix/range scans always work. `key_contains` needs LaserData Cloud to index a UTF-8 form of the key, so it matches only keys that are valid UTF-8.
- A **namespace** is a logical bucket (keys unique within it, scans scoped to it, isolated by LaserData Cloud). Expiry is lazy-on-read + swept, managed-side.
- KV is LaserData Cloud only. Raw Apache Iggy (no managed backend) -> `managed_kv` false -> `LaserError::Unsupported`.
- **CAS is backend-gated.** Where the backend supports conditional writes, CAS is served as a single conditional update. A backend that cannot do a conditional write leaves the `kv_cas` capability clear. The flag is advertised on the binary `AGDX_HELLO` reply (`OpVersions.features` bit `feature::KV_CAS`) and surfaced as `laser.capabilities().await.kv_cas`. `commit()` pre-gates on it and returns `LaserError::Unsupported` when it is clear, so a deployment that serves CAS MUST advertise the bit. CAS also rides its own command code, so even an unaware server rejects it cleanly.

## Rules specific to this area

- KV is mutable **point state**, NOT a message log: it does not ride the Iggy log. Do not route set/delete through topics. (Contrast `query`, whose writes ARE log appends.) LaserData Cloud picks the durability and multi-node options.
- The SDK is **client-only**: never add a KV backend/worker here. Storage, expiry, and dispatch are server-side concerns.
- Keep boundaries crisp: log = history/stream, KV = point state, AgentFS (future) = per-agent filesystem/workspace. Do not conflate.
- KV is a **primitive**, not the agent "memory" abstraction (which is the `Memory` trait in `sdk/src/memory.rs`). The two compose: `KvMemory` is a `Memory` backend built on `Laser::kv` (durable + mutable + TTL, recency recall, O(1) forget). Add memory semantics there, behind the `Memory` trait - never rename `kv` to `memory`.
- Tests live in-module (`sdk/src/kv/mod.rs` `#[cfg(test)]`): codes, envelope round-trips, key validation, error mapping. BDD names, `.expect("msg")`.
