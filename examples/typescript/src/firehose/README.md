# firehose - bounded multi-tenant ingest

> A configurable load generator for streaming throughput, topic fan-out, materialized indexes, and query pressure.

## What it does

1. Creates `org_00`, `org_01`, and additional organization topics with a configurable partition count.
2. Optionally registers one JSON projection and binding per organization so each topic materializes into its own index.
3. Runs a bounded number of publishers in parallel instead of starting one unbounded promise per organization.
4. Generates each batch immediately before sending it, so memory usage stays bounded as the workload grows.
5. Publishes deterministic telemetry with organization, service, region, status, latency, timestamp, configurable payload padding, and inline materialized bodies.
6. Reports elapsed time and records per second, then queries a sample index and decodes one selected payload when the managed query surface is available.

## Run it

```sh
npm run example:firehose
```

Every firehose control is explicit.

| Variable                       | Default | Meaning                                   |
| ------------------------------ | ------- | ----------------------------------------- |
| `LASER_FIREHOSE_ORGS`          | `4`     | Organization topics                       |
| `LASER_FIREHOSE_MESSAGES`      | `10000` | Records per organization                  |
| `LASER_FIREHOSE_CONCURRENCY`   | `4`     | Organization publishers running at once   |
| `LASER_FIREHOSE_PAYLOAD_BYTES` | `128`   | Padding bytes in each JSON body           |
| `LASER_FIREHOSE_BATCH`         | `500`   | Records generated and sent per batch      |
| `LASER_FIREHOSE_PARTITIONS`    | `4`     | Partitions per topic                      |
| `LASER_FIREHOSE_REGISTER`      | `true`  | Register managed projections and bindings |
| `LASER_FIREHOSE_QUERY`         | `true`  | Run the trailing sample query             |

Run a larger managed workload.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
LASER_FIREHOSE_MESSAGES=1000000 \
LASER_FIREHOSE_ORGS=16 \
LASER_FIREHOSE_CONCURRENCY=8 \
LASER_FIREHOSE_BATCH=1000 \
  npm run example:firehose
```

Run a publish-only local smoke test.

```sh
LASER_FIREHOSE_MESSAGES=1000 \
LASER_FIREHOSE_REGISTER=false \
LASER_FIREHOSE_QUERY=false \
  npm run example:firehose
```

## Where to look (LaserData Cloud)

- **Query**: one index per organization, named `org_00`, `org_01`, and so on, with payload selection enabled per record.
- **Bindings**: one source-topic binding per organization.
- **Messages**: deterministic telemetry bodies spread across the configured partitions.

## Highlights

- Bounded concurrency and batch-local allocation keep client memory predictable.
- A runtime codec validates every telemetry body before transport I/O.
- `publishBatch().inlinePayload()` keeps query payload selection available even though each high-volume projection defaults to index-only.
- One projection per organization exercises many managed indexes without inventing unrelated schemas.
- The same command remains useful on Apache Iggy because registration and query are independently capability-gated.
- Throughput output uses the actual sent count and elapsed wall time rather than a generic completion message.
