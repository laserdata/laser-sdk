# CLAUDE.md

Repo-wide agent guidelines live in [AGENTS.md](AGENTS.md). Read it first.

This workspace holds two published crates: `wire/` (laser-wire, the typed wire contract: codes, envelopes, dictionaries, caps, the golden fixture corpus, the Agent Data Exchange Protocol envelope, runtime-free and wasm-portable) and `sdk/` (laser-sdk, the client and agent runtime on top, re-exporting the wire crate as `laser_sdk::wire` and under its historical module paths).

Area skills are under `.claude/skills/`. Start with [laser-sdk-overview](.claude/skills/laser-sdk-overview/SKILL.md) and load the focused skill for the module you are changing.

[the AGDX spec](docs/agdx.md) is the authoritative wire/convention reference (streams, topics, headers, envelopes, query DSL, the agent envelope, caps). The laser-wire crate is its executable form, pinned by the fixture corpus under `wire/fixtures/`.

Docs are part of every change: when code or the wire contract changes, update `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, this file, the relevant `.claude/skills/*`, `docs/*`, and `the AGDX spec` in the same change. Never report a change done while any doc is stale.
