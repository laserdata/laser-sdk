---
name: provenance
description: The provenance runtime - `sdk/src/provenance/` and `sdk/src/types/ids.rs` (the header-key dictionary itself lives in laser-wire). Use when adding or changing a header key, the `Provenance` struct, the header encode/decode, `AgentTopic`, OTel/`agdx.*` aliasing, header caps/validation, usage/cost attribution, or any id type. Changes here are on-the-wire and affect every message already on the log.
---

# Provenance - the wire contract

The TypeScript peer lives under `foreign/typescript/src/provenance` and uses the shared dictionary from `src/wire/headers.ts`.

`provenance/` defines recorded metadata. Header renaming or encoding changes affect records already stored on the log. Coordinate intentional changes across every client and consumer.

Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

Managed authorization uses the authenticated `user_id` supplied by the server. The SDK cannot choose this identity. Provenance headers remain writer claims. Signed delegation uses `on_behalf_of` inside the envelope signature, while effective access intersects the permitted identities.

## STOP and ask the user before

- Renaming/removing a header key constant (the dictionary lives in `wire/src/headers.rs`, and `sdk/src/provenance/keys.rs` re-exports it), or changing the `alias()` mapping (it folds superseded OTel keys onto current ones).
- Changing how `TryFrom<&Provenance>` encodes or `TryFrom<&IggyMessage>` decodes.
- Changing `partition_key()` (currently `conversation_id.to_string()`) - it is the partitioning key, so it defines the per-conversation ordering guarantee.
- Changing `ConversationId::derive` without bumping `DERIVE_VERSION`.
- Lowering `HEADER_VALUE_MAX` (255, Iggy header-value length limit) or the `HEADER_SOFT_CAP` accounting.

## Key files and symbols

- `keys.rs` - re-exports the header key dictionary from `wire/src/headers.rs` (the wire crate owns it, see [wire-contract](../wire-contract/SKILL.md)). OTel GenAI keys (`gen_ai.*`) for conversation/agent/usage, `agdx.*` short keys (`cause`, `parent_conv`, `root_conv`, `to`, `idem`, `deadline`, `cost`). Current OTel usage keys are `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens`. No isolation header - isolation is an Iggy stream boundary, not a per-message field.
- `topic.rs` - `AgentTopic`. Well-known topics have a static `name()` (`agent.commands`, ...). `as_identifier()` is exact (use it on read/poll paths). `topic_string()` is the `&str` name (produce/consumer-group paths, which Iggy API forces to strings).
- `runtime.rs` - `Provenance` (required `conversation_id`, rest `Option`), `LlmUsage`, `ProvenanceError` (structured), the `put` validator, the cap accounting. `mod.rs` itself is just the feature-gated shell: `keys` is always available, `runtime` (this module, re-exported) and `topic` sit behind the `provenance` feature.
- `types/ids.rs` - `ConversationId` (ULID, `derive` = versioned FNV-1a), `AgentId` (validated string), `MessageId` (`partition:offset`), all via `FromStr`/`Display`/`TryFrom`.

## Rules specific to this area

- New optional field on `Provenance`: add the `keys::` constant, encode in `TryFrom<&Provenance>`, decode in the `match` in `TryFrom<&IggyMessage>`, and extend the round-trip test. Keep it `Option`. Only `conversation_id` is required.
- Use `put` for header values. It rejects empty values, values above `HEADER_VALUE_MAX`, ASCII controls, and DEL with `ProvenanceError`. Apply these rules before `HeaderValue::from_str`. Use `put_finite` for `f64` values so NaN and infinity return `NonFinite`.
- Match known keys before reading their values. `str_value` returns `ProvenanceError::InvalidValue` for a known key with the wrong type. Ignore only unknown keys through `_ => {}`. Typed AGDX keys such as `agdx.ct`, `agdx.av`, and `Uint128` routing fields are outside this string dictionary. Do not skip all non-string headers, because that hides malformed known values.
- `TryFrom<&IggyMessage>` maps any raw `IggyError` from `user_headers_map()` to `ProvenanceError::MalformedHeaders(String)` so the type-level contract never leaks the iggy crate's error.
- `MessageId::from_str` rejects signs, whitespace, leading zeros, and values that `Display` cannot reproduce exactly. `AgentId` accepts non-empty strings of at most 255 bytes without ASCII controls. It permits `:`, `@`, `/`, and spaces. Use `AgentId::new` or parsing.
- Ids parse/format only via their trait impls. `FromStr::Err` is `IdError` (structured), never `String`.
- `derive` must stay deterministic across toolchains - that is why it is hand-rolled FNV-1a, not `DefaultHasher`. The golden-value test pins it.

## Review smells

- Return `ProvenanceError` for invalid provenance values rather than leaking `IggyError`.
- `Identifier::named(&topic.topic_string())` on a read path (use `as_identifier()`).
- A new required (non-`Option`) `Provenance` field (breaks decode of old messages).
- Editing the `derive` golden test constant instead of bumping `DERIVE_VERSION`.
- `cost_usd: Some(f64::NAN)` (or `Inf`) ever reaching `put` without going through `put_finite`.
- `MessageId::from_str` admitting a value that does NOT round-trip through `Display` (the strict canonical-digits check is load-bearing).
