# firehose - bounded multi-tenant ingest

Publishes deterministic observability records to `org_00`, `org_01`, and
additional organization topics with bounded concurrency and bounded batches.

## What it does

1. Creates one topic per organization.
2. Runs a fixed number of producer workers.
3. Generates each batch immediately before sending it.
4. Reports records per second without retaining the full workload.
5. Optionally registers one projection per organization and queries a sample.

## Run it

```sh
npm run build
node dist/src/firehose/main.js

LASER_FIREHOSE_MESSAGES=1000000 \
LASER_FIREHOSE_ORGS=16 \
LASER_FIREHOSE_CONCURRENCY=8 \
LASER_FIREHOSE_BATCH=1000 \
node dist/src/firehose/main.js
```

`LASER_FIREHOSE_PAYLOAD_BYTES`, `LASER_FIREHOSE_PARTITIONS`,
`LASER_FIREHOSE_REGISTER`, and `LASER_FIREHOSE_QUERY` control the remaining
workload and managed phases.

## Highlights

- Workload memory is bounded by concurrency times batch size.
- Each organization has an independent durable log and managed index.
- Raw Apache Iggy runs the full ingest path and skips only managed work.
