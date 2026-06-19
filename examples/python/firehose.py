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

Indexes only materialize when the connected plane consumes the projections; on
raw Apache Iggy the publish path still runs at full speed and the trailing
analytics are skipped.

Run it:
    python firehose.py
"""

from __future__ import annotations

import asyncio
import os
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

SERVICES = ["checkout", "catalog", "search", "auth", "payments", "shipping", "inventory", "gateway"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-northeast-1"]
ENVIRONMENTS = ["prod", "staging", "dev"]
HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
ROUTES = ["/home", "/product/42", "/search", "/cart", "/checkout", "/api/v1/orders", "/healthz"]
STATUS_CODES = [200, 201, 204, 301, 400, 401, 403, 404, 429, 500, 503]

# A fixed epoch base in microseconds keeps timestamps reproducible. Each event
# steps forward by a few seconds from here.
BASE_TIMESTAMP_US = 1_900_000_000_000_000
MAX_STEP_US = 5_000_000
FILLER_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789 "


def env_int(key: str, default: int) -> int:
    raw = os.environ.get(key, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, "").strip().lower()
    return raw in ("1", "true", "yes", "on") if raw else default


class Config:
    def __init__(self) -> None:
        self.orgs = max(1, env_int("LASER_FIREHOSE_ORGS", 8))
        self.messages = max(1, env_int("LASER_FIREHOSE_MESSAGES", 20_000))
        self.payload_bytes = env_int("LASER_FIREHOSE_PAYLOAD_BYTES", 4096)
        self.batch = max(1, env_int("LASER_FIREHOSE_BATCH", 500))
        self.concurrency = max(1, env_int("LASER_FIREHOSE_CONCURRENCY", 4))
        self.partitions = max(1, env_int("LASER_FIREHOSE_PARTITIONS", 8))
        self.register = env_bool("LASER_FIREHOSE_REGISTER", True)
        self.query = env_bool("LASER_FIREHOSE_QUERY", True)
        self.progress_every = max(1, env_int("LASER_FIREHOSE_PROGRESS_EVERY", 5_000))


def build_filler(length: int, rng: _common.Rng) -> str:
    """Printable filler of length `length`, varied enough that payload sizes on
    disk stay honest."""
    return "".join(FILLER_ALPHABET[rng.below(len(FILLER_ALPHABET))] for _ in range(max(0, length)))


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


async def produce_shard(
    laser: ls.Laser, topic: str, shard: int, count: int, config: Config
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
        batch = laser.publish_batch(topic).inline_payload()
        for _ in range(size):
            event = build_event(rng, org, config.payload_bytes, ts)
            ts += 1 + rng.below(MAX_STEP_US)
            batch = batch.add_json(event)
        await batch.send()
        published += size


async def register_index(laser: ls.Laser, topic: str) -> None:
    """Register one projection and binding so `topic` materializes into an index
    of the same name with our indexed columns."""
    await _common.start_projector(laser, topic, FIELDS, content_type="any")


async def run_sample_queries(laser: ls.Laser, topics: list[str]) -> None:
    """A few representative analytics the firehose makes possible. Best effort: if
    the indexes are not materialized the queries error and we note it."""
    topic = topics[0]
    try:
        total = (await laser.query(topic).fetch()).total
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
        grand_total += (await laser.query(index_topic).fetch()).total
    print(f"grand total across {len(topics)} indexes: {grand_total} rows")


async def main() -> None:
    config = Config()
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()

    approx_mb = config.messages * config.payload_bytes / 1e6
    print(
        f"plan: {config.messages} messages across {config.orgs} org indexes, "
        f"~{config.payload_bytes} B payloads (~{approx_mb:.0f} MB on the log), "
        f"batch {config.batch}, {config.concurrency} concurrent producers"
    )

    topics = [f"{TOPIC_PREFIX}{org:02}" for org in range(config.orgs)]
    for topic in topics:
        await laser.ensure_topic(topic, partitions=config.partitions)

    register = config.register and caps.managed_query
    if register:
        for topic in topics:
            await register_index(laser, topic)
        print(f"registered {len(topics)} projections")
    else:
        print("skipping projection registration (publish only)")

    # Spread the total over the orgs, then run `concurrency` shards at a time so a
    # large run does not spawn unbounded work. The first orgs take the remainder
    # so the totals add up exactly.
    per_org = config.messages // config.orgs
    remainder = config.messages % config.orgs
    semaphore = asyncio.Semaphore(config.concurrency)
    started = time.monotonic()

    async def shard_task(shard: int, topic: str) -> None:
        count = per_org + (1 if shard < remainder else 0)
        async with semaphore:
            await produce_shard(laser, topic, shard, count, config)
        print(f"shard {shard:02} done: {count} messages to '{topic}'")

    await asyncio.gather(*(shard_task(shard, topic) for shard, topic in enumerate(topics)))

    elapsed = max(1e-6, time.monotonic() - started)
    rate = config.messages / elapsed
    print(f"done: {config.messages} messages in {elapsed:.1f}s ({rate:.0f} msg/s)")

    if config.query and _common.managed_gate(caps.managed_query, "query", EXAMPLE):
        await _common.wait_for_projection(laser, topics[0], per_org + (1 if remainder else 0))
        print("sample analytics over the firehose")
        await run_sample_queries(laser, topics)


if __name__ == "__main__":
    asyncio.run(main())
