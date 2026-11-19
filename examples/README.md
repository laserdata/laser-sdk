# LaserData - Laser SDK examples

Runnable examples of the Laser SDK, an open data-platform SDK over Apache Iggy. They come in two groups:

- generic: low-latency streaming, projections, query, and resumable readers.
- agentic: conversations, routing, memory, approvals, forks, and governed effects.

**Consumer filters: 98.5% less payload transfer in the CDC example.** The reader receives 4 of 240 records from the shared feed, with original bytes and offsets. The example also covers typed records, one-byte numeric headers, previews, and saved group policies. See the [Consumer Filters guide](https://docs.laserdata.com/laser-sdk/consumer-filters).

## Layout

```
examples/
  rust/      the Rust examples (one crate, one binary per scenario) + their README
  python/    the Python examples (one script per scenario) + their README
  typescript/ the TypeScript examples (one entry point per scenario) + their README
```

Each SDK owns its connection security. The Rust SDK embeds the LaserData public CA, Python uses that same Rust connection path, and TypeScript embeds the same certificate in its package. `LASER_TLS_CERT=<path>` enables TLS with an explicit CA for local or privately signed hosts in every example. Examples do not carry certificates or reimplement TLS selection.

## Copy a whole program

Two programs run when copied alone, with no helper from this directory. Each reads `LASER_CONNECTION_STRING`. The workflow runs on the stream named by `LASER_STREAM`, or on a stream named after the workflow, and keeps it after the run so the run can be resumed. The memory correction works in a scratch stream and deletes it at the end.

| program | Rust | Python | TypeScript |
| --- | --- | --- | --- |
| A workflow of three plain functions, one sleeping for a second, that prints its outcome as JSON | [`rust/src/workflow/main.rs`](rust/src/workflow/main.rs) | [`python/workflow.py`](python/workflow.py) | [`typescript/src/workflow/main.ts`](typescript/src/workflow/main.ts) |
| A memory correction for one user with `supersede`, then a recall of the current facts | [`rust/src/memory-correction/main.rs`](rust/src/memory-correction/main.rs) | [`python/memory_correction.py`](python/memory_correction.py) | [`typescript/src/memory-correction/main.ts`](typescript/src/memory-correction/main.ts) |

The three languages print the same fields, and every README shows the programs verbatim. Both run on Apache Iggy, Laser Stack, and LaserData Cloud. The memory correction recalls through the managed memory view where the deployment serves one, and folds the memory topic in process on Apache Iggy.

## Start here

Start with the focused `log`, `query`, `watch`, `kv`, `cdc`, `graph`, `recall`, `context`, and `agent` examples. Each has Rust, Python, and TypeScript versions with matching steps. They link to [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk) and to larger scenarios. `recall` is the focused memory example, while `memory` covers the larger scenario.

- Rust: [`rust/README.md`](rust/README.md): the full catalogue (each tagged agentic vs generic and whether it needs a managed deployment), with a per-example `README.md` under `rust/src/<name>/`.
- Python: [`python/README.md`](python/README.md): the Python ports, the same environment conventions, one runnable script per scenario.
- TypeScript: [`typescript/README.md`](typescript/README.md): the native Node ports and package-level smoke tests.

Examples run on Apache Iggy, Laser Stack, or LaserData Cloud with no code change. Laser Stack runs the complete managed examples locally. Against Apache Iggy without a managed backend, phases that need KV, query, forks, graph, or RBAC print the missing capability and exit cleanly. Managed state under a stream's scoped names, such as key-value entries and memory views, belongs to that stream, so a rerun on a recreated stream starts from empty state.

## Sessions

The sessions example records an incident root, two child tasks, and an independent maintenance root in one stream. It prints the recorded event and managed resource link counts for each session. Child results are collected explicitly, and child state stays separate.
