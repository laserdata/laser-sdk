---
name: kv
description: The key-value store client - `sdk/src/kv/` (feature `kv`, independently selectable from `query`). Use when changing `Laser::kv` or the get/set/delete/scan builders (`sdk/src/kv/client.rs`). The `AGDX_KV_*` codes, the request/reply wire types, and the key/value caps live in laser-wire (`wire/src/kv.rs`, `wire/src/codes.rs`, `wire/src/limits.rs`) - change them there per the wire-contract skill. Client-only. The backend is managed-side. Wire in the AGDX spec
---

# Key-value store (client)

The TypeScript peer is `foreign/typescript/src/managed/kv.ts` over `src/wire/kv.ts`, including CAS, fenced CAS, scans, expiry, patch, holder-scoped lease renewal, and barriered reads. Python binds the same `Lease` and `MutationPosition` through `foreign/python/src/kv.rs`.

A small managed key-value store next to the query layer, reachable on the one Iggy connection. Like `query`, the SDK ships only the client. The store lives in `laser-plane`, shipped by Laser Stack and LaserData Cloud, and the Iggy server forwards the operations. All of it is behind the independently selectable `kv` feature: private managed-command plumbing is shared without a semantic feature dependency. Wire: the AGDX spec

The server checks KV grants before forwarding commands. Reads use `kv:read`, writes use `kv:write`, and deletes use `kv:delete`, scoped by namespace. Lease lifecycle operations use `kv_lease:admin` on the coordination namespace. Fenced CAS also needs `kv_fence:read` there and `kv:write` on the target. Memory-view reads use `kv:read`, while memory writes require Iggy `send_messages` permission on their topic.

## What ships here (`sdk/src/kv/mod.rs`, the fluent builders in `sdk/src/kv/client.rs`)

- `Laser::kv(namespace)` -> a cheap namespace-scoped `Kv` handle owning a `Laser` clone (`'static`, so it moves into a spawn or stores in a durable `StateStore` seam). `Laser::kv_namespaces()` lists every namespace holding at least one entry for the caller (read-only, user-scoped, for tooling/UI browsing).
- `get` returns optional bytes, and `get_entry` returns `KvEntry`. `get_entry_at_least(key, MutationPosition)` requires the managed view to reach that position or return `KvError::Stale`. `get_typed::<T>` decodes JSON, and `get_as::<C, T>` selects a `Decoder`. `exists(key)` returns optional `KvMetadata` without the value.
- `set(key)` builder: `.bytes(impl AsRef<[u8]>)` / `.json(&v)?` / `.msgpack(&v)?` / `.encode_with::<C, _>(&v)?` (any `Codec` - Avro / Protobuf / Arrow / your own), optional `.ttl(Duration)` / `.expires_at(epoch_micros)`, then `.send()`. Values are opaque bytes, so the codec is the caller's choice on both ends.
- The `set` builder accepts `.expect_version(n)` or `.expect_absent()`, then `.commit().await -> u64`. A conflict returns `KvError::VersionConflict { current: Option<u64> }`. Use `LaserError::is_version_conflict()` to classify it. `KvEntry.version` is the comparison token, and `0` indicates an unversioned store.
- `cas_fenced(key, fence_namespace, fence_key, fence_token)` builder: the same value/precondition/expiry chain as `set`, finished with `.commit().await -> u64`, but the write also requires a live lease in `fence_namespace` and its fence sequence to equal `fence_token`. This is the at-most-one-effective-writer gate for an exclusive workflow step (see `workflow.rs` in [agent-runtime](../agent-runtime/SKILL.md)). An expired, released, or stale lease is `KvError::LeaseLost`. A target precondition miss remains `KvError::VersionConflict`.
- `expire(key, ttl)` changes or clears expiry without replacing the value. `patch(key, patch_bytes)` applies a codec-specific merge patch. Both return the version supplied by the backend.
- `lease(key, holder, ttl) -> Lease { token, granted_ttl, position }` acquires temporary ownership. Rust and Python use a cached dedicated coordination connection with acquisitions serialized per client. Timeout retires the connection. An uncertain acquisition retains the gate through its requested lifetime, even if the caller drops the future.

A live lease conflicts with acquisition. `renew_lease(key, holder, token, ttl)` extends it and returns the same token with a new position. `release(key, holder, token) -> bool` removes it early. Renew before expiry. `WORKFLOW_FENCE_NAMESPACE` exposes the default workflow coordination namespace.
- `delete(key)` returns whether an entry existed. `delete_many()` supports `.prefix`, `.range`, and `.key_contains`. Without bounds it clears the namespace. `scan()` supports those bounds plus `.limit` and `.cursor`. `.fetch()` returns one `KvPage`, while `.entries()` follows pages. `.conversation(id)` filters memory-view rows in scans and deletion. Entries without conversation metadata are excluded.
- `copy_to(key, to_key)` and `move_to(key, to_key)` support `.into_namespace(ns)` and `.send() -> u64`. They execute in one backend transaction. Move also deletes the source. An absent or expired source returns `KvError::NotFound`. The destination is overwritten with the remaining expiry.

`get_many(keys)` returns `Vec<Option<Vec<u8>>>` through `Laser::execute_batch`. `AGDX_BATCH_CODE` is base+20, with `MAX_BATCH_OPS` of 64. Items execute independently, and nested batches are rejected. Python provides `copy_to`, `move_to` with `to_namespace=`, and `get_many`.
- Keys and values enter as `impl AsRef<[u8]>` and return as `Vec<u8>`. `KvEntry` contains key, value, expiry, version, and optional `scope`. `key_str()` reads valid UTF-8 keys, while `decode_value` and `decode_value_with::<C, _>` decode bodies. `scope: Option<Box<MemoryRowScope>>` stores memory metadata and its `SourceRef`. Generic entries use `None`.
- `execute_kv(namespace, code, request)` (crate-internal) gates on `capabilities.kv.available`, set by an initial or refreshed `AGDX_HELLO` announcement only while the managed backend reports ready. Lease, renew, release, fenced CAS, and `get_entry_at_least` additionally require `capabilities.kv.fenced_leases` before encoding or sending. `Laser::execute_batch` inspects raw inner items and applies the same must-not-send gate.
- `FencedLeaseClient` accepts `ManagedKvTransport`. `SharedKvTransport`, or `Arc<dyn DynManagedKvTransport>`, supports a transport selected at runtime. The same framing and retry rules apply. `coordination.rs` prepares transport-bound `PreparedMutation` values with stable operation IDs. `PreparedMutation::ambiguous_recovery()` identifies the required recovery.

Readiness, authentication, and capability failures occur before sending. Timeout shuts down the retired Iggy client. `AmbiguousMutation` is not generically retryable. Repeat prepared renew or release requests with the same identity. Reconcile fenced CAS through its precondition. After an uncertain acquisition, wait its requested maximum lifetime before creating another identity.

## Wire (the managed KV contract)

- KV codes start at `AGDX_KV_BASE = 1_000_300`. Offsets are get (+0), set (+1), scan (+2), delete (+3), delete-many (+4), namespaces (+5), and CAS (+6). Further offsets are exists (+7), expire (+8), patch (+9), lease (+10), release (+11), fenced CAS (+12), copy (+13), move (+14), and renewal (+15). Ordinary requests use `KV_OP_VERSION = 1`. `KvLease`, `KvLeaseRenew`, `KvRelease`, and `KvCasFenced` use `KV_LEASE_OP_VERSION = 1` and require `KV_FENCED_LEASES`.
- Requests are named-field CBOR and carry `v` plus `namespace` where scoped. `KvGet.min_position` is an optional `MutationPosition { topic_generation, partition, offset }`. `KvCasFenced` adds `fence_namespace`, `fence_key`, and `fence_token`. Lease acquire/renew/release carry `holder_id`. Acquire and renew may carry delegated `subject_user_id`. `KvOutcome::Leased` and `Renewed` return token, granted TTL, and position. `Released(bool)` is idempotent for a missing current lease. `KvError` includes `VersionConflict`, `LeaseLost`, and `Stale { required }`. `KvEntry` carries `version: u64` (skip-when-zero).
- `key` and `value` are arbitrary bytes, a CBOR byte string (byte-exact). Scan bounds (`prefix`/`start`/`end`/`cursor`) are bytes, `key_contains` is a string.
- The managed deployment wraps each op in the shared `ForwardedCommand { user_id, client_id, correlation, read_all, command_code, payload }` keyed frame (also used by registry browse, and `AGDX_QUERY` keeps its own `ForwardedQuery`), stamping identity. The forwarded-frame shapes live in laser-wire (`wire/src/forward.rs`). The dispatch and the backend are managed-side behavior, not in this repo.

## Conventions + caps

- Keys must be non-empty and at most 512 B. Values are limited to 8 MiB. `holder_id` must be non-empty and at most `MAX_HOLDER_ID_BYTES = 128` UTF-8 bytes. Fence tokens must be nonzero.

Acquire and renew lifetimes must fall within `MIN_LEASE_TTL_MICROS = 1_000_000` through `MAX_LEASE_TTL_MICROS = 300_000_000`. The store can grant less time, but never more. Reject invalid requests before sending. Scan pages are limited to 1000 entries and default to 100. Clients accept the current lease-family version and reject unsupported versions.
- A namespace is a logical bucket (keys unique within it, scans scoped to it, isolated by managed RBAC). Expiry is lazy-on-read + swept, managed-side.
- KV requires LaserData Cloud or Laser Stack. Apache Iggy without a managed backend reports `capabilities.kv.available` as false and calls return `LaserError::Unsupported`.
- CAS is backend-gated. Plain CAS uses `feature::KV_CAS` / `capabilities.kv.cas`. The full holder-scoped lease contract uses `feature::KV_FENCED_LEASES` / `capabilities.kv.fenced_leases` and subsumes the older `KV_CAS_FENCED` bit, so capability folding must set `cas_fenced` when either bit is present. Binary hello and the HTTP `KvCapsView.fenced_leases` field must agree.

## Rules specific to this area

- SDK KV calls use managed commands rather than publishing mutation-topic records directly. The managed plane records mutations in its own log and maintains the point-state view. Do not add a KV backend or mutation worker to the SDK.
- The SDK is client-only: never add a KV backend/worker here. Storage, expiry, and dispatch are server-side concerns.
- Keep the roles distinct: the log stores history, KV exposes point state, and the proposed AgentFS concerns per-agent files.
- `Laser::kv` provides key-value operations. `Memory` in `sdk/src/memory.rs` provides memory behavior. `MemoryHandle::{set,fetch,update,remove}` records changes on the memory topic, which the deployment materializes into a KV view. Add memory behavior to `Memory` rather than renaming KV operations.
- Tests live in-module (`sdk/src/kv/client.rs` `#[cfg(test)]`): codes, envelope round-trips, key validation, error mapping. BDD names, `.expect("msg")`.
