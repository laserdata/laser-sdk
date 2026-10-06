# CLAUDE.md

Repo-wide agent guidelines live in [AGENTS.md](AGENTS.md). Read it first.

This workspace holds the `wire/` and `sdk/` Rust crates, Python bindings under `foreign/python/`, and the native Node client under `foreign/typescript/`. All three SDKs consume the Rust-owned wire contract and the shared BDD scenarios.

Area skills are under `.claude/skills/`. Start with [laser-sdk-overview](.claude/skills/laser-sdk-overview/SKILL.md). TypeScript work also loads [typescript-sdk](.claude/skills/typescript-sdk/SKILL.md). Consumer filters (`sdk/src/filters/`, `wire/src/filter/`, and their TypeScript and Python peers) load [consumer-filters](.claude/skills/consumer-filters/SKILL.md).

The [AGDX specification](docs/agdx.md) defines streams, topics, headers, envelopes, queries, and limits. `laser-wire` implements these types, and `wire/fixtures/` defines their expected encoding.

Client defaults each have one owning page: [connect timeout](docs/connect-timeout.md), [publish recovery](docs/publish-recovery.md), [producer statistics](docs/producer-statistics.md), and the 0.6.0 [client behavior](docs/client-behavior.md) and upgrade guide. Link to them instead of copying their text.

Keep affected documentation consistent with authorized code or contract changes. This includes `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, this file, relevant `.claude/skills/*`, `docs/*`, and the AGDX specification. Do not report completion while affected documentation is stale.
