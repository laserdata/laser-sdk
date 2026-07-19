---
name: provenance
description: The provenance runtime - `sdk/src/provenance/` and `sdk/src/types/ids.rs` (the header-key dictionary itself lives in laser-wire). Use when adding or changing a header key, the `Provenance` struct, the header encode/decode, `AgentTopic`, OTel/`agdx.*` aliasing, header caps/validation, usage/cost attribution, or any id type. Changes here are on-the-wire and affect every message already on the log.
---

# Provenance - the wire contract

`provenance/` defines what every message carries. It is the most dangerous area to change: the headers are serialized into Iggy user-headers and persisted, so a key rename or encode change breaks messages already on the log.

Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

Note for authorization: the RBAC capability layer keys every managed-command check on the **server-stamped `user_id`** (unspoofable, set by the streaming server from the authenticated session, the SDK cannot set it), the same identity that is authorship for provenance. Provenance headers are claims. The authorization subject is not. Signed on-behalf-of delegation rides the envelope metadata key `on_behalf_of`, inside the `Signature` span, so an agent cannot forge whom it acts for.

## STOP and ask the user before

- Renaming/removing a header key constant (the dictionary lives in `wire/src/headers.rs`, and `sdk/src/provenance/keys.rs` re-exports it), or changing the `alias()` mapping (it folds superseded OTel keys onto current ones).
- Changing how `TryFrom<&Provenance>` encodes or `TryFrom<&IggyMessage>` decodes.
- Changing `partition_key()` (currently `conversation_id.to_string()`) - it is the partitioning key, so it defines the per-conversation ordering guarantee.
- Changing `ConversationId::derive` without bumping `DERIVE_VERSION`.
- Lowering `HEADER_VALUE_MAX` (255, the Iggy header-value length limit) or the `HEADER_SOFT_CAP` accounting.

## Key files and symbols

- `keys.rs` - re-exports the header key dictionary from `wire/src/headers.rs` (the wire crate owns it, see [wire-contract](../wire-contract/SKILL.md)). OTel GenAI keys (`gen_ai.*`) for conversation/agent/usage, `agdx.*` short keys (`cause`, `parent_conv`, `root_conv`, `to`, `idem`, `deadline`, `cost`). Current OTel usage keys are `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens`. **No isolation header** - isolation is an Iggy stream boundary, not a per-message field.
- `topic.rs` - `AgentTopic`. Well-known topics have a static `name()` (`agent.commands`, ...). `as_identifier()` is exact (use it on read/poll paths). `topic_string()` is the `&str` name (produce/consumer-group paths, which the Iggy API forces to strings).
- `runtime.rs` - `Provenance` (required `conversation_id`, rest `Option`), `LlmUsage`, `ProvenanceError` (structured), the `put` validator, the cap accounting. `mod.rs` itself is just the feature-gated shell: `keys` is always available, `runtime` (this module, re-exported) and `topic` sit behind the `provenance` feature.
- `types/ids.rs` - `ConversationId` (ULID, `derive` = versioned FNV-1a), `AgentId` (validated string), `MessageId` (`partition:offset`), all via `FromStr`/`Display`/`TryFrom`.

## Rules specific to this area

- New optional field on `Provenance`: add the `keys::` constant, encode in `TryFrom<&Provenance>`, decode in the `match` in `TryFrom<&IggyMessage>`, and extend the round-trip test. Keep it `Option`. Only `conversation_id` is required.
- All header values go through `put`, which rejects empty, `> HEADER_VALUE_MAX`, and ASCII control characters or DEL bytes (`\n`, `\0`, etc.) with a clear `ProvenanceError` (not a raw `IggyError`). Keep that ordering: validate before `HeaderValue::from_str`. Non-finite `f64` (NaN, Inf) goes through `put_finite` and is rejected with `NonFinite`.
- Decode is match-key-first, fail-not-skip: a recognized provenance key reads its value through `str_value`, and a known key carrying a non-string value is a decode error (`ProvenanceError::InvalidValue`), never silently dropped. Only foreign/unknown keys fall through the `_ => {}` arm and are ignored, which is why the string provenance dictionary coexists on one record with AGDX typed headers (`agdx.ct` u8, `agdx.av` u32, the `Uint128` routing duplicates): those keys are foreign to provenance, not skipped because of their type. Do NOT reintroduce a blanket non-string skip - dropping a known key's wrong-typed value is data loss.
- `TryFrom<&IggyMessage>` maps any raw `IggyError` from `user_headers_map()` to `ProvenanceError::MalformedHeaders(String)` so the type-level contract never leaks the iggy crate's error.
- `MessageId::from_str` is strict: rejects leading signs, whitespace, leading zeros, and anything whose `Display` does not reproduce the input string. `AgentId` accepts almost any string (non-empty, ≤255 bytes, no ASCII control characters), including `:`, `@`, `/`, and spaces. Build it with `AgentId::new` or by parsing.
- Ids parse/format only via their trait impls. `FromStr::Err` is `IdError` (structured), never `String`.
- `derive` must stay deterministic across toolchains - that is why it is hand-rolled FNV-1a, not `DefaultHasher`. The golden-value test pins it.

## Review smells

- A raw `IggyError` surfacing from a bad header value (should be `ProvenanceError`).
- `Identifier::named(&topic.topic_string())` on a read path (use `as_identifier()`).
- A new required (non-`Option`) `Provenance` field (breaks decode of old messages).
- Editing the `derive` golden test constant instead of bumping `DERIVE_VERSION`.
- `cost_usd: Some(f64::NAN)` (or `Inf`) ever reaching `put` without going through `put_finite`.
- `MessageId::from_str` admitting a value that does NOT round-trip through `Display` (the strict canonical-digits check is load-bearing).
