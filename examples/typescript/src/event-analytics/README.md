# event-analytics - one clickstream, every read model

One deterministic clickstream drives the open streaming path, a resumable exporter, and the managed query path. `LASER_MESSAGES` scales the same program from a small local check to a sustained workload. Layer: generic. AGDX surfaces: streaming, materialized views, and the query DSL.

## What it does

1. **Hot path.** A consumer-group reader tails `clickstream` from the log's tail while the publisher writes, folding event and checkout counts with server-committed offsets, so a re-run never re-reads old events. A bounded wait prevents a stalled run from hanging.
2. **Resumable export.** An independent typed cursor persists every partition's next offset in a `StateStore`. A new cursor restores that checkpoint and reads only the remaining records. The first poll plus the resumed tail must cover every record on the topic exactly once.
3. **Analytics.** When the server advertises query support, the example registers the `clickstream.v1` JSON projection and binding before publishing, waits for all rows to materialize, then runs the dashboard queries from the Rust example: counts by event kind, the slowest routes, checkout count, the first five-minute range, per-minute windows, and average latency plus distinct routes by kind.
4. **Validated ingest.** A managed deployment allocates a JSON Schema ID and the example polls the registry until the asynchronous apply lands. The typed topic accepts one valid event and rejects a malformed event locally. A second malformed event rides the raw path past the client, the deployment rejects it server-side, and exactly one row materializes in `clickstream_guarded`.

Indexing is body-first. The projection extracts typed columns from the JSON body, so indexed values cannot disagree with duplicate user headers. The projection stays index-only by default and every clickstream batch explicitly calls `inlinePayload()`, which keeps the original JSON bytes alongside each materialized row. A query with payload selection, including the stream UI DSL `include_payload` flag, therefore returns the body. The example proves this by fetching one row with payload and decoding it through `CLICK_EVENT_CODEC`.

On raw Apache Iggy, the streaming and checkpoint phases run and the managed phase prints one capability-gated skip message.

## Run it

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

- **Query**: the `clickstream` and `clickstream_guarded` indexes.
- **Writer schemas**: the registered draft 2020-12 click event schema.
- **Messages**: raw JSON events on the `clickstream` topic, with payloads also available from materialized query rows.
- **Bindings**: the notifying binding from `clickstream` to `clickstream.v1`.

## Highlights

- Explicit `Codec<ClickEvent>` validation at the untrusted JSON boundary.
- Concurrent publishing and consumer-group folding over raw Apache Iggy.
- Per-partition cursor offsets persisted through `StateStore`.
- Managed projection registration and bounded materialization polling.
- Client-side and server-side JSON Schema validation with the allocated ID.
- `count`, `groupBy`, `orderDesc`, `messageType`, `timeRange`, `window`, `avg`, and `countDistinct` over the same materialized view.
- `publishBatch().inlinePayload()` paired with `query(...).fetchOne(CLICK_EVENT_CODEC)` proves that payload selection returns decodable event bodies.
