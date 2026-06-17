# Laser SDK examples

Runnable examples of the Laser SDK - an agentic data-platform SDK over Apache Iggy (streaming, projections and query, key-value, forks). They come in two flavors, and most directories are tagged accordingly:

- **generic** - plain low-latency streaming + query/analytics (publish, project, query, resumable readers). No agent concepts.
- **agentic** - the agent runtime on top: conversations, routing, memory, durable approvals, copy-on-write forks, effectively-once effects.

## Layout

```
examples/
  rust/      the Rust examples (one crate, one binary per scenario) + their README
  certs/     public CA certs for LaserData Cloud, shared across languages (base64 PEM)
```

`certs/` lives here, not under a language, because the CA bundle is the same whatever client you use. More language ports (e.g. a future `python/`, `ts/`) will sit alongside `rust/` with the same shape: a per-language README that catalogues its examples and explains how to run them.

## Start here

- **Rust:** [`rust/README.md`](rust/README.md) - the full catalogue (10 examples, each tagged agentic vs generic and whether it needs LaserData Cloud), with a per-example `README.md` under `rust/src/<name>/`.

Examples run on a local Apache Iggy out of the box, or against a LaserData Cloud deployment with no code change (see the per-language README for the environment variables). The handful that exercise LaserData Cloud features (KV, query off the log, forks) print how to point at LaserData Cloud and exit cleanly on raw Apache Iggy.
