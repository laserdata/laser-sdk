# firehose - millions of messages, many orgs, gigabytes of data

A load generator, not a narrative. Layer: generic. AGDX surfaces: streaming at volume plus materialized views and the query DSL across many indexes. The other examples are small and reproducible. This one publishes millions of big, richly indexed telemetry events across many org indexes so LaserData Cloud can be driven with gigabytes of data: projections, query, table growth, and read-model behaviour under real storage pressure.

## What it does

- Provisions `LASER_FIREHOSE_ORGS` topics (`org_00`, `org_01`, and so on), each registered as its own projection and binding, so each materializes into its own queryable index. The realistic multi-org shape: one index per org, exercising LaserData Cloud maintaining many materialized indexes at once without inventing fake schemas.
- Publishes `LASER_FIREHOSE_MESSAGES` telemetry events spread across those orgs, with `LASER_FIREHOSE_CONCURRENCY` producers running at once, batching `LASER_FIREHOSE_BATCH` records per send call.
- Each message carries 16 indexed columns (org, service, region, host, env, severity, `message_type`, http_method, status_code, route, user_id, session_id, trace_id, latency_ms, bytes_out, `ts`) plus a JSON body padded to `LASER_FIREHOSE_PAYLOAD_BYTES`, stored inline so LaserData Cloud keeps the bytes.
- Ends with a few best-effort analytics queries: rows per index, count by severity, slowest requests, and the grand total across all orgs.

Reproducible by a per-org xorshift generator (no extra crate), so a run replays identically. Throughput-tuned, so build with `--release`.

## Run it

Every knob shares the SDK `LASER_` namespace under the `LASER_FIREHOSE_` prefix.

| variable | default | meaning |
| --- | --- | --- |
| `LASER_FIREHOSE_ORGS` | `8` | number of org indexes to fan across |
| `LASER_FIREHOSE_MESSAGES` | `2000000` | total messages to publish |
| `LASER_FIREHOSE_PAYLOAD_BYTES` | `4096` | approximate JSON body size per message |
| `LASER_FIREHOSE_BATCH` | `1000` | records per send call |
| `LASER_FIREHOSE_CONCURRENCY` | `12` | orgs published in parallel |
| `LASER_FIREHOSE_PARTITIONS` | `8` | partitions per topic |
| `LASER_FIREHOSE_REGISTER` | `true` | register projections (set `false` for publish only) |
| `LASER_FIREHOSE_QUERY` | `true` | run trailing analytics queries |
| `LASER_FIREHOSE_PROGRESS_EVERY` | `100000` | progress log cadence in messages |

Approximate log volume is `LASER_FIREHOSE_MESSAGES` times `LASER_FIREHOSE_PAYLOAD_BYTES`. The defaults publish 2M messages at 4 KB, roughly 8 GB across 8 org indexes. LaserData Cloud consumes the projection commands and serves the trailing queries. On raw Apache Iggy the publish path still runs at full speed and the managed phases are skipped.

```sh
# defaults: about 2M messages across 8 org indexes, 4 KB payloads, about 8 GB
just up && cargo run --release --example firehose

# bigger: about 10M messages, 32 orgs, 16 producers, LaserData Cloud projects
LASER_FIREHOSE_MESSAGES=10000000 LASER_FIREHOSE_ORGS=32 \
LASER_FIREHOSE_CONCURRENCY=16 \
cargo run --release --example firehose

# publish-only smoke test, no indexes, no queries
LASER_FIREHOSE_MESSAGES=10000 LASER_FIREHOSE_REGISTER=false LASER_FIREHOSE_QUERY=false \
cargo run --release --example firehose
```

## Where to look (LaserData Cloud)

- **Query**: one index per org (`org_00`, `org_01`, ...), each materialized from its own projection, queried for rows per index, count by severity, slowest requests, and the grand total.

## Highlights

- One projection plus binding per org topic, so LaserData Cloud maintains many materialized indexes at once under load.
- `laser.topic(topic).publish_batch()` with `LASER_FIREHOSE_CONCURRENCY` parallel producers and the balanced partitioner, the high-throughput indexed-publish path.
- Inline payloads padded to a configurable size, so the run exercises real storage pressure rather than tiny bodies.
- Body-first indexing: 16 typed columns extracted from the body, plus the `message_type` and `ts` convention fields, no `agdx.idx.*` headers duplicating the payload.
- Trailing aggregate queries (`count` / `group_by` / ordering) across every index, best-effort so the run stays green on an open server.
