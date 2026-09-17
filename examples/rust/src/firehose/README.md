# firehose - millions of messages, many orgs, gigabytes of data

This example generates telemetry records across several organization topics. It tests publication volume, projection, queries, and storage use. Use its message-count and payload-size controls to bound a run.

## What it does

- Create `LASER_FIREHOSE_ORGS` topics, such as `org_00` and `org_01`. Register a projection and binding for each topic.
- Publish `LASER_FIREHOSE_MESSAGES` across the organizations. Use `LASER_FIREHOSE_CONCURRENCY` producers and `LASER_FIREHOSE_BATCH` records per send.
- Include 16 indexed columns in each JSON body. The columns are `org`, `service`, `region`, `host`, `env`, `severity`, `message_type`, and `http_method`. They also include `status_code`, `route`, `user_id`, `session_id`, `trace_id`, `latency_ms`, `bytes_out`, and `ts`. Pad the body to `LASER_FIREHOSE_PAYLOAD_BYTES` and request inline storage.
- Ends with a few best-effort analytics queries: rows per index, count by severity, slowest requests, and the grand total across all orgs.

Each organization uses a deterministic xorshift generator. The same input configuration produces the same generated sequence. Use `--release` for load measurements.

## Run it

Run from `examples/rust`:

All load controls use the `LASER_FIREHOSE_` prefix within the SDK `LASER_` namespace.

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

Approximate payload volume is `LASER_FIREHOSE_MESSAGES` multiplied by `LASER_FIREHOSE_PAYLOAD_BYTES`. Defaults produce 2 million messages of 4 KB across 8 organization indexes, about 8 GB of payload. LaserData Cloud handles projection and queries. Apache Iggy runs publication and skips managed phases.

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

- Query: one index per org (`org_00`, `org_01`, ...), each materialized from its own projection, queried for rows per index, count by severity, slowest requests, and the grand total.

## Highlights

- One projection plus binding per org topic, so LaserData Cloud maintains many materialized indexes at once under load.
- `laser.topic(topic).publish_batch()` with `LASER_FIREHOSE_CONCURRENCY` parallel producers and the balanced partitioner, the high-throughput indexed-publish path.
- Inline payloads padded to a configurable size, so the run exercises real storage pressure rather than tiny bodies.
- Body-first indexing: 16 typed columns extracted from the body, plus the `message_type` and `ts` convention fields, no `agdx.idx.*` headers duplicating the payload.
- Trailing aggregate queries (`count` / `group_by` / ordering) across every index, best-effort so the run stays green on an open server.
