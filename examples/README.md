# LaserData - Laser SDK examples

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

New to the SDK? Each primitive has its own tiny example (25-50 lines, one primitive, all three languages) before the full scenarios below: `log`, `query`, `watch`, `kv`, `graph`, `recall`, `context`, `agent`. They map one-to-one onto the accessors in the top-level README's grammar table, and the three language ports run the same steps in the same order and print the same lines, so reading one teaches all three. Each links to its [docs.laserdata.cloud/laser-sdk](https://docs.laserdata.cloud/laser-sdk) page and to the full scenario that uses that primitive in anger. `recall` is the Memory primitive's example - named after one of its four verbs, not `memory`, since that name already belongs to the full scenario below.

- **Rust:** [`rust/README.md`](rust/README.md) - the full catalogue (each tagged agentic vs generic and whether it needs a managed deployment), with a per-example `README.md` under `rust/src/<name>/`.
- **Python:** [`python/README.md`](python/README.md) - the Python ports, the same environment conventions, one runnable script per scenario.
- **TypeScript:** [`typescript/README.md`](typescript/README.md) - the native Node ports and package-level smoke tests.

Examples run on the repository's pinned VSR Apache Iggy service, Laser Stack, or LaserData Cloud with no code change. Laser Stack runs the complete managed examples locally. Against Apache Iggy without `laser-plane`, phases that need KV, query, forks, graph, RBAC, or the run registry print the missing capability and exit cleanly.
