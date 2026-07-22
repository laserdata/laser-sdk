# event-analytics - one clickstream, every read model

One deterministic clickstream drives the open streaming path, a resumable
exporter, and the managed query path. `LASER_MESSAGES` scales the same program
from a small local check to a sustained workload. Layer: generic. AGDX
surfaces: streaming, materialized views, and the query DSL.

## What it does

1. **Hot path.** A consumer-group reader tails `clickstream` from the log's
   tail while the publisher writes, folding event and checkout counts with
   server-committed offsets, so a re-run never re-reads old events. A bounded
   wait prevents a stalled run from hanging.
2. **Resumable export.** An independent typed cursor persists every partition's
   next offset in a `StateStore`. A new cursor restores that checkpoint and
   reads only the remaining records. The first poll plus the resumed tail must
   cover every record on the topic exactly once.
3. **Analytics.** When the server advertises query support, the example
   registers the `clickstream.v1` JSON projection and binding before publishing,
   waits for all rows to materialize, and queries checkout rows by the reserved
   `message_type` field.
4. **Validated ingest.** A managed deployment allocates a JSON Schema ID and
   the example polls the registry until the asynchronous apply lands. The
   typed topic accepts one valid event and rejects a malformed event locally.
   A second malformed event rides the raw path past the client, the deployment
   rejects it server-side, and exactly one row materializes in
   `clickstream_guarded`.

The projection extracts fields from the JSON body. The event body is the source
of truth, so indexed values cannot disagree with duplicate user headers. On raw
Apache Iggy, the streaming and checkpoint phases run and the managed phase
prints one capability-gated skip message.

## Run it

```sh
# local Apache Iggy
npm run build
node dist/src/event-analytics/main.js

# LaserData Cloud
LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host \
  node dist/src/event-analytics/main.js

# larger run
LASER_MESSAGES=2000000 node dist/src/event-analytics/main.js
```

## Where to look (LaserData Cloud)

- **Query**: the `clickstream` and `clickstream_guarded` indexes.
- **Writer schemas**: the registered draft 2020-12 click event schema.
- **Messages**: raw JSON events on the `clickstream` topic.
- **Bindings**: the notifying binding from `clickstream` to `clickstream.v1`.

## Highlights

- Explicit `Codec<ClickEvent>` validation at the untrusted JSON boundary.
- Concurrent publishing and consumer-group folding over raw Apache Iggy.
- Per-partition cursor offsets persisted through `StateStore`.
- Managed projection registration and bounded materialization polling.
- Client-side and server-side JSON Schema validation with the allocated ID.
- `messageType("checkout")` over the reserved `message_type` field.
