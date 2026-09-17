# event-analytics - one clickstream, every read model

This example publishes clickstream events and reads them through a live consumer and a replay cursor. Managed deployments also project the events for analytics and schema checks.

## What it does

1. Hot path. A consumer-group reader tails `clickstream` from the log's tail while the publisher writes, folding event and checkout counts with server-committed offsets, so a re-run never re-reads old events. A bounded wait prevents a stalled run from hanging.
2. Resumable export. An independent typed cursor persists every partition's next offset in a `StateStore`. A new cursor restores that checkpoint and reads only the remaining records. The first poll plus the resumed tail must cover every record on the topic exactly once.
3. If query support is available, register the `clickstream.v1` projection and its binding. Wait for projected records, then run the analytics queries.
4. Validated ingest. A managed deployment allocates a JSON Schema ID and the example polls the registry until the asynchronous apply lands. The typed topic accepts one valid event and rejects a malformed event locally. A second malformed event rides the raw path past the client, the deployment rejects it server-side, and exactly one row materializes in `clickstream_guarded`.

Indexing is body-first. The projection extracts typed columns from the JSON body, so indexed values cannot disagree with duplicate user headers. The projection stays index-only by default and every clickstream batch explicitly calls `inlinePayload()`, which keeps the original JSON bytes alongside each materialized row. A query with payload selection, therefore returns the body. The example proves this by fetching one row with payload and decoding it through `CLICK_EVENT_CODEC`.

On Apache Iggy, the streaming and checkpoint phases run and the managed phase prints one capability-gated skip message.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
# local Apache Iggy
npm run example:event-analytics

# LaserData Cloud
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:event-analytics

# larger run
LASER_MESSAGES=2000000 npm run example:event-analytics
```

## Where to look (LaserData Cloud)

- Query: the `clickstream` and `clickstream_guarded` indexes.
- Writer schemas: the registered draft 2020-12 click event schema.
- Messages: raw JSON events on the `clickstream` topic, with payloads also available from materialized query rows.
- Bindings: the notifying binding from `clickstream` to `clickstream.v1`.

## Highlights

- Explicit `Codec<ClickEvent>` validation at the untrusted JSON boundary.
- Concurrent publishing and consumer-group folding over Apache Iggy.
- Per-partition cursor offsets persisted through `StateStore`.
- Managed projection registration and bounded materialization polling.
- Client-side and server-side JSON Schema validation with the allocated ID.
- `count`, `groupBy`, `orderDesc`, `messageType`, `timeRange`, `window`, `avg`, and `countDistinct` over the same materialized view.
- `publishBatch().inlinePayload()` paired with `query(...).fetchOne(CLICK_EVENT_CODEC)` proves that payload selection returns decodable event bodies.
