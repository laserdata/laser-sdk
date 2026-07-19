"""event-analytics (generic): one clickstream, every read model.

A clickstream topic with each read model the platform offers layered over it:

  - HOT PATH    a cursor tails the raw log live while the producer streams,
                folding a rolling ops ticker (events seen, checkouts).
  - ANALYTICS   the managed plane materializes a queryable index and answers the
                aggregates a dashboard needs (funnel, slowest routes, windows).
  - EXPORT      an independent cursor tails the same log with a checkpoint,
                resuming exactly where it stopped after a restart.
  - SCHEMAS     a registered JSON Schema guards the index against malformed
                events (managed plane only).

The analytics, export-resume, and schema phases are managed features and skip
cleanly on raw Apache Iggy.

Run it:
    python event_analytics.py
"""

from __future__ import annotations

import asyncio
import json

import _common
import laser_sdk as ls

EXAMPLE = "event-analytics"
TOPIC = "clickstream"

# The validated ingest (managed only): events on this topic stamp a registered
# JSON Schema's id, so a malformed payload never materializes.
GUARDED_TOPIC = "clickstream_guarded"
EVENT_JSON_SCHEMA = """{
    "type":"object",
    "required":["user_id","message_type","route","latency_ms","ts"],
    "properties":{
        "user_id":{"type":"string"},
        "message_type":{"type":"string","enum":["page_view","add_to_cart","checkout"]},
        "route":{"type":"string"},
        "latency_ms":{"type":"integer","minimum":0},
        "ts":{"type":"integer","minimum":0}
    }
}"""

# Indexed columns. message_type and ts are reserved fields backing
# message_type(..) and time_range(..).
USER_ID = "user_id"
MESSAGE_TYPE = "message_type"
ROUTE = "route"
LATENCY_MS = "latency_ms"
TS = "ts"
COLUMNS = [USER_ID, MESSAGE_TYPE, ROUTE, LATENCY_MS, TS]

CHECKPOINT_KEY = "clickstream-export-cursor"

VISITORS = [
    "alice",
    "bob",
    "carol",
    "dave",
    "erin",
    "frank",
    "grace",
    "heidi",
    "ivan",
    "judy",
    "mallory",
    "oscar",
]
ROUTES = [
    "/home",
    "/product/42",
    "/product/7",
    "/search",
    "/cart",
    "/checkout",
    "/pricing",
    "/docs",
]

# A fixed epoch base (micros) so the run is deterministic. Events step forward by
# a random few seconds from here.
BASE_US = 1_900_000_000_000_000
ONE_MINUTE_US = 60_000_000
STEP_MAX_US = 30_000_000

PUBLISH_CHUNK = 30
LIVE_TIMEOUT = 15.0
LIVE_SNAPSHOT_EVERY = 50


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()
    count = _common.messages(default=180)

    await laser.topic(TOPIC).ensure(partitions=_common.PARTITIONS)
    events = clickstream(count)

    # Register the projector before publishing so no event is missed (managed only).
    if caps.query:
        await _common.start_projector(laser, TOPIC, COLUMNS)

    print("hot path: a live reader tails the stream while the producer runs")
    await live_monitor(laser, events)

    if _common.managed_gate(caps.query, "query", EXAMPLE):
        await _common.wait_for_projection(laser, TOPIC, count)
        print("read model 1: ad-hoc analytics over the query layer")
        await run_analytics(laser)
        print("read model 2: a resumable downstream reader")
        await run_resumable_export(laser)

    if caps.managed:
        print("validated ingest: a JSON Schema guards the index")
        await run_guarded_ingest(laser)
    else:
        print("writer schemas live on LaserData Cloud, skipping validated ingest (needs the Cloud)")


def clickstream(count: int) -> list[dict]:
    """A deterministic session: many visitors browsing, page views the common
    case and checkouts the rare one, spaced a few seconds apart from a fixed
    base."""
    rng = _common.Rng(0x123456789ABCDEF0)
    ts = BASE_US
    events = []
    for _ in range(count):
        roll = rng.below(100)
        if roll <= 69:
            kind = "page_view"
        elif roll <= 91:
            kind = "add_to_cart"
        else:
            kind = "checkout"
        events.append(
            {
                USER_ID: rng.pick(VISITORS),
                MESSAGE_TYPE: kind,
                ROUTE: rng.pick(ROUTES),
                LATENCY_MS: 30 + rng.below(600),
                TS: ts,
            }
        )
        ts += 1 + rng.below(STEP_MAX_US)
    return events


async def publish_clickstream(laser: ls.Laser, events: list[dict]) -> None:
    """Publish the clickstream in chunks: each chunk is one send carrying its
    records with inline JSON bodies, so the projection extracts every column out
    of the body. Many chunks rather than one request per event is what matters on
    a rate-limited deployment."""
    published = 0
    for start in range(0, len(events), PUBLISH_CHUNK):
        chunk = events[start : start + PUBLISH_CHUNK]
        batch = laser.topic(TOPIC).publish_batch().inline_payload()
        for event in chunk:
            batch = batch.add_json(event)
        await batch.send()
        published += len(chunk)
        print(f"published {published}/{len(events)} events to '{TOPIC}'")
        await asyncio.sleep(0.02)


async def live_monitor(laser: ls.Laser, events: list[dict]) -> None:
    """The hot path: a cursor folding a rolling ops ticker off the raw log while
    the producer streams. Publisher and reader run concurrently and are not in
    lockstep."""
    publisher = asyncio.create_task(publish_clickstream(laser, events))
    cursor = laser.topic(TOPIC).replay()
    seen = 0
    checkouts = 0
    idle = 0.0
    expected = len(events)
    while seen < expected:
        messages = await cursor.poll()
        if not messages:
            if publisher.done() and idle >= LIVE_TIMEOUT:
                break
            idle += 0.05
            await asyncio.sleep(0.05)
            continue
        idle = 0.0
        for message in messages:
            if message.json().get(MESSAGE_TYPE) == "checkout":
                checkouts += 1
            seen += 1
            if seen % LIVE_SNAPSHOT_EVERY == 0 or seen == expected:
                print(f"live ticker: {seen}/{expected} events, {checkouts} checkouts")
    await publisher


async def run_analytics(laser: ls.Laser) -> None:
    """The analytics read model: the aggregates a dashboard asks of a clickstream."""
    by_kind = await laser.query(TOPIC).count().group_by([MESSAGE_TYPE]).fetch()
    print("events by kind:")
    for row in by_kind.rows:
        print(f"  {row.headers.get(MESSAGE_TYPE, '?'):<12} {row.headers.get('count', '0')}")

    slowest = await laser.query(TOPIC).order_desc(LATENCY_MS).limit(3).fetch()
    print("slowest 3 routes:")
    for row in slowest.rows:
        print(f"  {row.headers.get(LATENCY_MS, '?'):>5}ms  {row.headers.get(ROUTE, '?')}")

    checkouts = await laser.query(TOPIC).message_type("checkout").count().fetch()
    print(f"checkouts: {scalar(checkouts)}")

    first_window = (
        await laser.query(TOPIC).time_range(BASE_US, BASE_US + 5 * ONE_MINUTE_US).count().fetch()
    )
    print(f"events in the first 5 minutes: {scalar(first_window)}")

    per_minute = await laser.query(TOPIC).count().window(TS, ONE_MINUTE_US).fetch()
    print("events per minute:")
    for row in per_minute.rows:
        print(f"  bucket {row.headers.get('window_start', '?')}: {row.headers.get('count', '0')}")

    by_kind_metrics = (
        await laser.query(TOPIC)
        .avg(LATENCY_MS)
        .count_distinct(ROUTE)
        .group_by([MESSAGE_TYPE])
        .fetch()
    )
    print("avg latency and distinct routes by kind:")
    for row in by_kind_metrics.rows:
        kind = row.headers.get(MESSAGE_TYPE, "?")
        avg = row.headers.get("avg", "?")
        routes = row.headers.get("count_distinct", "?")
        print(f"  {kind:<12} avg={avg}ms routes={routes}")


def scalar(result: ls.QueryResult) -> int:
    """Read a single aggregate (count/sum with no group) off its one result row."""
    if not result.rows:
        return 0
    return int(result.rows[0].headers.get("count", "0"))


async def run_resumable_export(laser: ls.Laser) -> None:
    """The resumable read model: a downstream export job tails the same log with
    a cursor, persisting its offsets in a StateStore so a restart resumes from
    the checkpoint and re-reads nothing. An InMemoryStore here. A FileStore or
    the managed kv store survives a real restart through the same get/set API."""
    checkpoint = ls.InMemoryStore()
    reader = laser.topic(TOPIC).replay()
    first = await reader.poll()
    print(f"export job read {len(first)} events, checkpointing offsets")
    await checkpoint.set(CHECKPOINT_KEY, json.dumps(reader.offsets))

    # A brand-new cursor resumes from the saved offsets: a restart re-reads nothing.
    saved = json.loads(bytes(await checkpoint.get(CHECKPOINT_KEY)))
    resumed = laser.topic(TOPIC).replay(from_offsets=saved)
    again = await resumed.poll()
    print(f"after a restart, the export job re-read {len(again)} events (resumed from checkpoint)")


async def run_guarded_ingest(laser: ls.Laser) -> None:
    """Register the Event JSON Schema, publish one well-formed and one malformed
    event both stamping the id, and show only the well-formed one materialized.
    The malformed one is rejected by the schema and never pollutes the index."""
    schema_id = await laser.register_schema(
        {"kind": "json_schema", "schema": EVENT_JSON_SCHEMA}, name="clickstream_event"
    )
    print(f"the managed plane allocated writer-schema id {schema_id} for the Event guard")

    await laser.topic(GUARDED_TOPIC).ensure(partitions=_common.PARTITIONS)
    await _common.start_projector(laser, GUARDED_TOPIC, COLUMNS)

    # Well-formed: passes the schema, materializes.
    valid = {
        USER_ID: "alice",
        MESSAGE_TYPE: "checkout",
        ROUTE: "/checkout",
        LATENCY_MS: 120,
        TS: BASE_US,
    }
    await laser.topic(GUARDED_TOPIC).publish().json(valid).schema_id(schema_id).send()
    # Malformed: latency_ms is a string, violating the schema. It decodes as JSON
    # fine, only the validation catches it.
    malformed = {
        USER_ID: "mallory",
        MESSAGE_TYPE: "checkout",
        ROUTE: "/checkout",
        LATENCY_MS: "fast",
        TS: 1,
    }
    await laser.topic(GUARDED_TOPIC).publish().json(malformed).schema_id(schema_id).send()

    await _common.wait_for_projection(laser, GUARDED_TOPIC, 1)
    # Give the projector a beat to settle the second publish before pinning the count.
    await asyncio.sleep(1.0)
    settled = (await laser.query(GUARDED_TOPIC).with_total().fetch()).total
    if settled == 1:
        print(
            "guarded index holds 1 row: the valid checkout landed, the malformed event "
            "was rejected by the JSON Schema and never materialized"
        )
    else:
        print(
            f"guarded index holds {settled} rows: the valid checkout landed, but this server "
            f"does not enforce JSON-Schema validation, so the malformed event materialized too. "
            f"LaserData Cloud rejects it"
        )


if __name__ == "__main__":
    asyncio.run(main())
