# CLAUDE.md

Repo-wide agent guidelines live in [AGENTS.md](AGENTS.md). Read it first.

This workspace holds the `wire/` and `sdk/` Rust crates, Python bindings under `foreign/python/`, and the native Node client under `foreign/typescript/`. All three SDKs consume the Rust-owned wire contract and shared BDD scenarios.

The open streaming layer exposes Laser-native direct producers and live async consumers with server-managed offsets, while keeping the exact Apache Iggy builders, client, and types available as an escape hatch. The optional `vsr` Cargo feature forwards Iggy's VSR transport switch through Rust and source-built Python without changing those APIs. Managed custom command codes stay unavailable over VSR until upstream admits them.

Area skills are under `.claude/skills/`. Start with [laser-sdk-overview](.claude/skills/laser-sdk-overview/SKILL.md). TypeScript work also loads [typescript-sdk](.claude/skills/typescript-sdk/SKILL.md).

[the AGDX spec](docs/agdx.md) is the authoritative wire/convention reference (streams, topics, headers, envelopes, query DSL, the agent envelope, caps). The laser-wire crate is its executable form, pinned by the fixture corpus under `wire/fixtures/`.

Docs are part of every change: when code or the wire contract changes, update `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, this file, the relevant `.claude/skills/*`, `docs/*`, and `the AGDX spec` in the same change. Never report a change done while any doc is stale.

Memory governance applies to both log-backed and in-process vector handles created from a `Laser`. Policies see the proposed item body, not a backend encoding.
