"""firehose (generic): a volume load generator.

A telemetry firehose that publishes many big, richly indexed observability
events across many indexes, so a deployment can be driven with real data. Built
for volume rather than narrative: many concurrent producers run against the log
and the managed plane materializes each topic into its own queryable index.

Every knob reads from the environment, so the same script scales from a quick
smoke run to a heavy soak:

    # defaults: ~20k events across 8 org indexes, 4 KB payloads
    python firehose.py

    # bigger: more events, more orgs, more concurrency
    LASER_FIREHOSE_MESSAGES=200000 LASER_FIREHOSE_ORGS=16 \\
    LASER_FIREHOSE_CONCURRENCY=8 python firehose.py

Indexes only materialize when the connected plane consumes the projections. On
raw Apache Iggy the publish path still runs at full speed and the trailing
analytics are skipped.

Run it:
    python firehose.py
"""

from __future__ import annotations

import asyncio
import time

import _common
import laser_sdk as ls

EXAMPLE = "firehose"

# One index per org, named org_00, org_01, and so on. Query index names accept
# [A-Za-z0-9_] only, so the separator is `_` rather than `.`.
TOPIC_PREFIX = "org_"

# Indexed columns the plane materializes into each index. message_type and ts are
# reserved fields backing message_type(..) and time_range(..).
FIELDS = [
    "org",
    "service",
    "region",
    "host",
    "env",
    "severity",
    "message_type",
    "http_method",
    "status_code",
    "route",
    "user_id",
    "session_id",
    "trace_id",
    "latency_ms",
    "bytes_out",
    "ts",
]

SERVICES = [
    "checkout",
    "catalog",
    "search",
    "auth",
    "payments",
    "shipping",
    "inventory",
    "recommend",
    "notify",
    "gateway",
]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-northeast-1"]
ENVIRONMENTS = ["prod", "staging", "dev"]
HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
ROUTES = [
    "/home",
    "/product/42",
    "/search",
    "/cart",
    "/checkout",
    "/api/v1/orders",
    "/api/v1/users",
    "/healthz",
    "/metrics",
    "/login",
]
STATUS_CODES = [200, 201, 204, 301, 400, 401, 403, 404, 429, 500, 503]

# A fixed epoch base in microseconds keeps timestamps reproducible. Each event
# steps forward by a few seconds from here.
BASE_TIMESTAMP_US = 1_900_000_000_000_000
MAX_STEP_US = 5_000_000
FILLER_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789 "


async def main() -> None:
    config = Config()
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()

    _common.phase("firehose: warming up")
    approx_mb = config.messages * config.payload_bytes / 1e6
    print(
        f"plan: {config.messages} messages across {config.orgs} org indexes, "
        f"~{config.payload_bytes} B payloads (~{approx_mb:.0f} MB on the log), "
        f"batch {config.batch}, {config.concurrency} concurrent producers"
    )

    topics = [f"{TOPIC_PREFIX}{org:02}" for org in range(config.orgs)]
    for topic in topics:
        await laser.topic(topic).ensure(partitions=config.partitions)

    register = config.register and caps.query
    if register:
        _common.phase("provisioning topics and indexes")
        for topic in topics:
            await register_index(laser, topic)
        print(f"registered {len(topics)} projections, waiting for the plane to create indexes")
        # Best effort. Give the managed plane a moment to create the first index.
        # With no managed plane attached this short wait simply elapses and we
        # publish anyway.
        await wait_for_index(laser, topics[0], 15.0)
    elif not config.register:
        print("LASER_FIREHOSE_REGISTER is off, skipping projection registration (publish only)")
    else:
        print("projection registration needs the managed plane, publishing to the raw log only")

    # Spread the total over the orgs, then run `concurrency` shards at a time so a
    # large run does not spawn unbounded work. The first orgs take the remainder
    # so the totals add up exactly.
    per_org = config.messages // config.orgs
    remainder = config.messages % config.orgs
    semaphore = asyncio.Semaphore(config.concurrency)
    counters = {"published": 0, "bytes": 0}
    started = time.monotonic()
    _common.phase("firing the hose")

    async def shard_task(shard: int, topic: str) -> None:
        count = per_org + (1 if shard < remainder else 0)
        async with semaphore:
            await produce_shard(laser, topic, shard, count, config, counters)
        print(f"shard {shard:02} done: {count} messages to '{topic}'")

    await asyncio.gather(*(shard_task(shard, topic) for shard, topic in enumerate(topics)))

    elapsed = max(1e-6, time.monotonic() - started)
    total = counters["published"]
    total_bytes = counters["bytes"]
    print(
        f"done: {total} messages, {total_bytes / 1e9:.2f} GB payload in {elapsed:.1f}s "
        f"({total / elapsed:.0f} msg/s, {(total_bytes / 1e6) / elapsed:.1f} MB/s)"
    )

    if config.query and _common.managed_gate(caps.query, "query", EXAMPLE):
        await _common.wait_for_projection(laser, topics[0], per_org + (1 if remainder else 0))
        _common.phase("sample analytics over the firehose")
        await run_sample_queries(laser, topics)


class Config:
    """Run knobs, all read from the environment with sane defaults. The env
    parsers come from `_common`, never redefined here."""

    def __init__(self) -> None:
        self.orgs = max(1, _common.env_int("LASER_FIREHOSE_ORGS", 8))
        self.messages = max(1, _common.env_int("LASER_FIREHOSE_MESSAGES", 20_000))
        self.payload_bytes = _common.env_int("LASER_FIREHOSE_PAYLOAD_BYTES", 4096)
        self.batch = max(1, _common.env_int("LASER_FIREHOSE_BATCH", 500))
        self.concurrency = max(1, _common.env_int("LASER_FIREHOSE_CONCURRENCY", 4))
        self.partitions = max(1, _common.env_int("LASER_FIREHOSE_PARTITIONS", 8))
        self.register = _common.env_bool("LASER_FIREHOSE_REGISTER", True)
        self.query = _common.env_bool("LASER_FIREHOSE_QUERY", True)
        self.progress_every = max(1, _common.env_int("LASER_FIREHOSE_PROGRESS_EVERY", 5_000))


async def register_index(laser: ls.Laser, topic: str) -> None:
    """Register one projection and binding so `topic` materializes into an index
    of the same name with our indexed columns."""
    await _common.start_projector(laser, topic, FIELDS, content_type="any")


async def wait_for_index(laser: ls.Laser, topic: str, timeout: float) -> None:
    """Poll until `topic`'s index exists (the query stops erroring), or the
    timeout elapses. A short, non-fatal nudge after registration."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            await laser.query(topic).fetch()
            print(f"index '{topic}' is live")
            return
        except ls.LaserError:
            await asyncio.sleep(0.25)
    print(f"index '{topic}' is not live yet (no managed plane attached?), publishing anyway")


async def produce_shard(
    laser: ls.Laser, topic: str, shard: int, count: int, config: Config, counters: dict
) -> None:
    """Publish `count` events for one shard in batches. Each event is a JSON body
    padded to `payload_bytes`, carrying its indexed columns inline so a query can
    return the whole record."""
    rng = _common.Rng(0xD1B54A32 ^ (shard * 0x9E3779B97F4A7C15))
    ts = BASE_TIMESTAMP_US + shard * MAX_STEP_US
    org = f"org-{shard:02}"
    published = 0
    while published < count:
        size = min(config.batch, count - published)
        batch = laser.topic(topic).publish_batch().inline_payload()
        for _ in range(size):
            event = build_event(rng, org, config.payload_bytes, ts)
            ts += 1 + rng.below(MAX_STEP_US)
            batch = batch.add_json(event)
        await batch.send()
        published += size
        counters["published"] += size
        counters["bytes"] += size * config.payload_bytes
        if counters["published"] % config.progress_every < size:
            print(f"published {counters['published']} of {config.messages} messages")


def build_event(rng: _common.Rng, org: str, payload_bytes: int, ts: int) -> dict:
    """One reproducible event for `org`, padded to roughly `payload_bytes`."""
    roll = rng.below(100)
    if roll <= 64:
        severity = "info"
    elif roll <= 84:
        severity = "debug"
    elif roll <= 96:
        severity = "warn"
    else:
        severity = "error"
    roll = rng.below(100)
    if roll <= 59:
        message_type = "http_request"
    elif roll <= 79:
        message_type = "db_query"
    elif roll <= 91:
        message_type = "cache_op"
    elif roll <= 97:
        message_type = "queue_publish"
    else:
        message_type = "job_run"
    service = rng.pick(SERVICES)
    region = rng.pick(REGIONS)
    host = f"{service}-{rng.below(64)}"
    trace_id = f"{rng.next_u64():016x}{rng.next_u64():016x}"

    base_size = 320 + len(service) + len(region) + len(trace_id)
    detail = build_filler(payload_bytes - base_size, rng)
    return {
        "org": org,
        "service": service,
        "region": region,
        "host": host,
        "env": rng.pick(ENVIRONMENTS),
        "severity": severity,
        "message_type": message_type,
        "http_method": rng.pick(HTTP_METHODS),
        "status_code": rng.pick(STATUS_CODES),
        "route": rng.pick(ROUTES),
        "user_id": f"u{rng.below(100_000)}",
        "session_id": f"{rng.next_u64():016x}",
        "trace_id": trace_id,
        "latency_ms": 1 + rng.below(2000),
        "bytes_out": rng.below(1_000_000),
        "ts": ts,
        "attributes": {"sdk": "laser-firehose", "schema": "v1", "shard_host": host},
        "detail": detail,
    }


def build_filler(length: int, rng: _common.Rng) -> str:
    """Printable filler of length `length`, varied enough that payload sizes on
    disk stay honest."""
    return "".join(FILLER_ALPHABET[rng.below(len(FILLER_ALPHABET))] for _ in range(max(0, length)))


async def run_sample_queries(laser: ls.Laser, topics: list[str]) -> None:
    """A few representative analytics the firehose makes possible. Best effort: if
    the indexes are not materialized the queries error and we note it."""
    topic = topics[0]
    try:
        total = (await laser.query(topic).with_total().fetch()).total
    except ls.LaserError as error:
        print(f"query unavailable ({error}). Is the plane materializing the indexes? Skipping")
        return
    print(f"index '{topic}' holds {total} rows")

    by_severity = await laser.query(topic).count().group_by(["severity"]).fetch()
    print(f"'{topic}' events by severity:")
    for row in by_severity.rows:
        print(f"  {row.headers.get('severity', '?'):<6} {row.headers.get('count', '0')}")

    slowest = await laser.query(topic).order_desc("latency_ms").limit(5).fetch()
    print(f"'{topic}' slowest 5 requests:")
    for row in slowest.rows:
        print(f"  {row.headers.get('latency_ms', '?'):>5}ms  {row.headers.get('route', '?')}")

    grand_total = 0
    for index_topic in topics:
        grand_total += (await laser.query(index_topic).with_total().fetch()).total
    print(f"grand total across {len(topics)} indexes: {grand_total} rows")


if __name__ == "__main__":
    asyncio.run(main())
