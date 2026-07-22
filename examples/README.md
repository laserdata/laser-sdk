# Laser SDK examples

Runnable examples of the Laser SDK, an open data-platform SDK over Apache Iggy. They come in two groups:

- **generic**: low-latency streaming, projections, query, and resumable readers.
- **agentic**: conversations, routing, memory, approvals, forks, and governed effects.

## Layout

```
examples/
  rust/      the Rust examples (one crate, one binary per scenario) + their README
  python/    the Python examples (one script per scenario) + their README
  typescript/ the TypeScript examples (one entry point per scenario) + their README
```

Each SDK owns its connection security. The Rust SDK embeds the LaserData public CA, Python uses that same Rust connection path, and TypeScript embeds the same certificate in its package. Examples do not carry certificates or reimplement TLS selection.

## Start here

- **Rust:** [`rust/README.md`](rust/README.md) - the full catalogue (each tagged agentic vs generic and whether it needs LaserData Cloud), with a per-example `README.md` under `rust/src/<name>/`.
- **Python:** [`python/README.md`](python/README.md) - the Python ports, the same environment conventions, one runnable script per scenario.
- **TypeScript:** [`typescript/README.md`](typescript/README.md) - the native Node ports and package-level smoke tests.

Examples run on a local Apache Iggy out of the box, or against a LaserData Cloud deployment with no code change (see the per-language README for the environment variables). The handful that exercise LaserData Cloud features (KV, query off the log, forks, RBAC, run registry) print how to point at LaserData Cloud and exit cleanly on raw Apache Iggy.
