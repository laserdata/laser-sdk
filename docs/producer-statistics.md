# Producer statistics

Rust, Python, and TypeScript producers report optional statistics through the AGDX client metadata extension. One observer connection per Laser connection carries the reports, so agent presence on the main connection is untouched. A telemetry error never fails a publish. Ordinary publishing against Apache Iggy is unchanged.

## Enable it

Set the variables before creating producers.

| Configuration | Environment variable | Default | Range |
| --- | --- | --- | --- |
| Report interval | `LASER_PRODUCER_TELEMETRY_INTERVAL_MS` | `10000` | `0` disables, otherwise `1000` through `60000` |
| Reported handles per connection | `LASER_PRODUCER_TELEMETRY_MAX_PRODUCERS` | `32` | `0` through `32` |

Values outside a range are clamped to it. Handles past the limit publish normally and are left out of the report. A report fits the 64 KiB metadata limit, expires after three intervals, and disappears when the observer disconnects. A producer id stays stable for the life of its handle, across reconnects. Injected clients that cannot open a second authenticated connection do not report.

## What is counted

| Counter | Meaning |
| --- | --- |
| Submitted records and bytes | Each application send once, before retries. Payload bytes only, no headers or framing. |
| Confirmed records and bytes | Fully successful synchronous send calls. Unavailable for background enqueues. |
| Failed calls | Send calls that returned an error. |
| Last success | The last successful application call. |
| Retries | Unavailable when the native client does not expose its attempts. |

A failed batch can have committed a prefix that the confirmed counters leave out. The counters are SDK reports. Do not use them as durable delivery totals, exactly-once evidence, usage accounting, or authorization input.

## Latency

Publish-call latency includes retry delays. A producer keeps 64 fixed histogram buckets for its lifetime and reports p50, p99, and p99.9 as power-of-two bucket upper bounds with sample counts. p99 needs 100 completed calls, p99.9 needs 1000. Earlier estimates are unavailable, not zero.

## Where to see it

The Producers tab of a topic in Stream UI lists observed handles by their reported stream and topic, with the observer owner, report age, stale state, and whatever statistics are available.

- Coverage is the connected node only. Other nodes are not queried.
- The observer node can differ from the publishing node.
- Uninstrumented connections and handles past the limit are missing, so the list is not a producer inventory.
- Unknown values show as unavailable, never as zero.
