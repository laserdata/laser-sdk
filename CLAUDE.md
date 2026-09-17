# CLAUDE.md

Repo-wide agent guidelines live in [AGENTS.md](AGENTS.md). Read it first.

This workspace holds the `wire/` and `sdk/` Rust crates, Python bindings under `foreign/python/`, and the native Node client under `foreign/typescript/`. All three SDKs consume the Rust-owned wire contract and shared BDD scenarios.

The streaming layer provides Laser producers and continuous consumers with server-stored offsets. Apache Iggy builders, client, and types remain available for detailed configuration. All clients use the Iggy transport. Managed reads use the non-replicated extension, and the server classifies the three authorization writes as dedicated replicated operations.

Area skills are under `.claude/skills/`. Start with [laser-sdk-overview](.claude/skills/laser-sdk-overview/SKILL.md). TypeScript work also loads [typescript-sdk](.claude/skills/typescript-sdk/SKILL.md).

The [AGDX specification](docs/agdx.md) defines streams, topics, headers, envelopes, queries, and limits. `laser-wire` implements these types, and `wire/fixtures/` defines their expected encoding.

Keep affected documentation consistent with authorized code or contract changes. This includes `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, this file, relevant `.claude/skills/*`, `docs/*`, and the AGDX specification. Do not report completion while affected documentation is stale.

Memory governance applies to both log-backed and in-process vector handles created from a `Laser`. Policies see the proposed item body, not a backend encoding.

## Publish recovery

Rust, Python, and TypeScript publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds. Explicit builder or connect configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Preserve message identities and confirmed chunks across retries. Return permanent errors immediately. Return exhausted errors without panicking. Rust reconnects the shared client in place so consumers and reply readers stay attached. Recover only the connection that the attempt used. If another publish replaces that connection, skip recovery and use the replacement. See [publish recovery](docs/publish-recovery.md).
