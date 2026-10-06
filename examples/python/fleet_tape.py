"""fleet-tape (generic): a fleet telemetry tape with two readers on one connection.

Hosts stream CPU readings (integer percentage, sample count) and two read models
consume them:

  - the HOT path: readings stream to a feed topic and a cursor folds them into a
    live fleet view (last CPU, sample-weighted mean CPU, samples, degraded
    readings per host) straight off the log.
  - the ANALYTICS path: the same readings are indexed to a queryable tape the
    managed plane materializes, and sample and mean CPU aggregates run once the
    feed drains. The query phase needs Laser Stack or LaserData Cloud and skips
    on Apache Iggy without a managed backend.

Run it:
    python3 fleet_tape.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import _common
import laser_sdk as ls

EXAMPLE = "fleet-tape"
FEED_TOPIC = "metrics_feed"  # raw hot path
TAPE_TOPIC = "readings"  # queryable analytics tape
AVRO_TAPE_TOPIC = "readings_avro"  # schema-first tape (managed deployment)
# Index names carry this run's token, so a rerun or another language's example on
# the same deployment never shares their rows.
TAPE_INDEX = _common.index_for(TAPE_TOPIC)
AVRO_TAPE_INDEX = _common.index_for(AVRO_TAPE_TOPIC)
AVRO_PROJECTION = f"{AVRO_TAPE_INDEX}.v1"

# The schema-first tape replays the identical readings as raw Avro datums,
# decoded by a writer schema the managed plane allocated an id for.
READING_AVRO_SCHEMA = """{
    "type":"record","name":"HostReading",
    "fields":[
        {"name":"host","type":"string"},
        {"name":"cpu","type":"long"},
        {"name":"samples","type":"int"},
        {"name":"level","type":"string"},
        {"name":"cpu_total","type":"long"},
        {"name":"message_type","type":"string"},
        {"name":"ts","type":"long"}
    ]
}"""
# Avro phase volume: bounded so the cloud-gated coda stays quick on a heavy run.
AVRO_READINGS_CAP = 500

# Indexed columns on the reading tape (the fields the managed plane materializes).
# message_type and ts are reserved fields backing message_type(..) / time_range(..).
HOST = "host"
CPU = "cpu"
SAMPLES = "samples"
LEVEL = "level"
CPU_TOTAL = "cpu_total"
MESSAGE_TYPE = "message_type"
TS = "ts"
COLUMNS = [HOST, CPU, SAMPLES, LEVEL, CPU_TOTAL, MESSAGE_TYPE, TS]
SUM_RESULT = "sum"

# The starting load: a CPU percentage per host. The feed random-walks each from
# here.
OPENING = [("node-1", 42), ("node-2", 57), ("node-3", 31), ("node-4", 68), ("node-5", 49)]
# A reading at or above this CPU percentage is "degraded".
DEGRADED_CPU = 80

# Paced bursts keep the live feed gentle, well under a free-tier deployment's
# throughput ceiling. The tape indexes in batches so the analytics write is a
# handful of sends rather than one request per reading.
BURST = 40
BURST_GAP = 0.12
TAPE_BATCH = 100
READING_TIMEOUT = 15.0


class Fleet:
    """The fleet's running load: it draws the next reading by random-walking the
    last CPU percentage of a randomly chosen host. Deterministic, so the live
    feed and the tape index replay the identical readings."""

    def __init__(self) -> None:
        self.load = [list(pair) for pair in OPENING]
        self.rng = _common.Rng(0x123456789ABCDEF0)
        self.ts = 1_900_000_000_000_000

    def next_reading(self) -> dict:
        pick = self.rng.below(len(self.load))
        host, cpu = self.load[pick]
        step = self.rng.below(15) - 7
        cpu = min(100, max(0, cpu + step))
        self.load[pick][1] = cpu
        samples = 1 + self.rng.below(500)
        self.ts += 1 + self.rng.below(50_000)
        return {
            HOST: host,
            CPU: cpu,
            SAMPLES: samples,
            LEVEL: "degraded" if cpu >= DEGRADED_CPU else "ok",
            CPU_TOTAL: cpu * samples,
            MESSAGE_TYPE: "reading",
            TS: self.ts,
        }


def generate_readings(count: int) -> list[dict]:
    fleet = Fleet()
    return [fleet.next_reading() for _ in range(count)]


class FleetView:
    """A live fleet view folded from the feed: last CPU, cumulative samples,
    cumulative weighted CPU, and degraded readings per host, updated reading by
    reading."""

    def __init__(self) -> None:
        self.hosts: dict[str, dict] = {}

    def apply(self, reading: dict) -> None:
        load = self.hosts.setdefault(
            reading[HOST], {"last": 0, "samples": 0, "cpu_total": 0, "degraded": 0}
        )
        load["last"] = reading[CPU]
        load["samples"] += reading[SAMPLES]
        load["cpu_total"] += reading[CPU_TOTAL]
        if reading[LEVEL] == "degraded":
            load["degraded"] += 1

    def snapshot(self, readings: int) -> None:
        print(f"fleet @ {readings} readings:")
        for host in sorted(self.hosts):
            load = self.hosts[host]
            mean = load["cpu_total"] // load["samples"] if load["samples"] else 0
            print(
                f"  {host:<7} last cpu {load['last']:>3}%  mean cpu {mean:>3}%  "
                f"samples {load['samples']:>8}  degraded {load['degraded']:>4}"
            )


async def stream_live_view(laser: ls.Laser, readings: list[dict]) -> FleetView:
    """Stream the raw hot feed in paced bursts in a background task while a
    cursor folds arriving readings into the live view. The two sides are
    deliberately not in lockstep, so a delayed delivery cannot deadlock the
    loop."""

    async def feed() -> None:
        for start in range(0, len(readings), BURST):
            batch = laser.topic(FEED_TOPIC).publish_batch()
            for reading in readings[start : start + BURST]:
                batch = batch.add_json(reading)
            await batch.send()
            await asyncio.sleep(BURST_GAP)

    publisher = asyncio.create_task(feed())
    cursor = laser.topic(FEED_TOPIC).replay()
    view = FleetView()
    seen = 0
    idle = 0.0
    while seen < len(readings):
        messages = await cursor.poll()
        if not messages:
            if publisher.done() and idle >= READING_TIMEOUT:
                break
            idle += 0.05
            await asyncio.sleep(0.05)
            continue
        idle = 0.0
        for message in messages:
            view.apply(message.json())
            seen += 1
            if seen % BURST == 0:
                view.snapshot(seen)
    await publisher
    return view


async def index_tape(laser: ls.Laser, readings: list[dict]) -> None:
    """Index every reading to the queryable tape in batches: each batch is one
    send carrying its rows with inline JSON bodies, so the projection's pointers
    extract every column out of the body, typed. No index headers duplicate the
    payload."""
    indexed = 0
    for start in range(0, len(readings), TAPE_BATCH):
        chunk = readings[start : start + TAPE_BATCH]
        batch = laser.topic(TAPE_TOPIC).publish_batch().inline_payload()
        for reading in chunk:
            batch = batch.add_json(reading)
        await batch.send()
        indexed += len(chunk)
        print(f"indexed {indexed}/{len(readings)} readings to '{TAPE_TOPIC}'")


@dataclass
class Reading:
    """One host reading, the typed shape of the tape's JSON bodies."""

    host: str
    cpu: int
    samples: int
    level: str
    cpu_total: int
    message_type: str
    ts: int


async def audit_tape(laser: ls.Laser, readings: list[dict]) -> None:
    """The audit a monitoring stack runs against its own tape: replay the raw log
    through a typed handle (records decode into the Reading dataclass as they
    drain, a record that stopped decoding would raise with its exact log
    position) and the weighted CPU totals recomputed off the log must equal the
    session's own."""
    records = laser.topic(TAPE_TOPIC).json(Reading).records("fleet-tape-audit-py")
    cpu_total_by_host: dict[str, int] = {}
    audited = 0
    while (record := await records.next()) is not None:
        reading: Reading = record.value
        cpu_total_by_host[reading.host] = cpu_total_by_host.get(reading.host, 0) + reading.cpu_total
        audited += 1
    expected: dict[str, int] = {}
    for row in readings:
        expected[row[HOST]] = expected.get(row[HOST], 0) + row[CPU_TOTAL]
    if cpu_total_by_host != expected:
        raise RuntimeError("the typed replay disagrees with the session's own weighted CPU totals")
    print(f"audited {audited} readings off the log, every host's weighted CPU total matches")


def group_totals(result: ls.QueryResult) -> dict[str, int]:
    """Collect a sum(..).group_by([HOST]) result into host -> total. Each row
    carries positional typed values described by the result fields."""
    totals: dict[str, int] = {}
    for row in result.rows:
        host = result.value_text(row, HOST)
        total = result.value_text(row, SUM_RESULT)
        if host is not None and total is not None:
            totals[host] = int(total)
    return totals


async def report_samples_and_mean(laser: ls.Laser) -> None:
    """Query the materialized tape: per-host samples, and the sample-weighted mean
    CPU derived from two grouped sums (mean = cpu_total / samples)."""
    samples = await laser.query(TAPE_INDEX).sum(SAMPLES).group_by([HOST]).fetch()
    cpu_total = await laser.query(TAPE_INDEX).sum(CPU_TOTAL).group_by([HOST]).fetch()
    samples_by_host = group_totals(samples)
    cpu_total_by_host = group_totals(cpu_total)
    print(f"tape analytics over {sum(samples_by_host.values())} samples (Laser query layer):")
    for host in sorted(samples_by_host):
        count = samples_by_host[host]
        mean = cpu_total_by_host.get(host, 0) // count if count else 0
        print(f"  {host:<7} samples {count:>8}  mean cpu {mean:>3}%")


async def avro_tape(laser: ls.Laser, readings: list[dict]) -> None:
    """The schema-first coda (managed deployment): the identical readings ride a
    second tape as raw Avro datums. The managed plane resolves the registered
    writer schema via `agdx.sid`, decodes the binary bodies, and extracts the
    indexed columns, and the weighted CPU totals must come out the same as the
    JSON tape's. The schema is compiled once client-side so a body that stops
    matching fails before publishing, not as a managed-side warning the producer
    cannot see."""
    schema_source = {"kind": "avro", "schema": READING_AVRO_SCHEMA}
    schema_id = await laser.schemas().register(schema_source, name="fleet_reading")
    print(f"the managed plane allocated writer-schema id {schema_id} for the HostReading schema")

    await laser.topic(AVRO_TAPE_TOPIC).ensure(partitions=_common.PARTITIONS)
    await _common.start_projector(
        laser, AVRO_TAPE_TOPIC, COLUMNS, index=AVRO_TAPE_INDEX, content_type="avro"
    )

    compiled = ls.CompiledSchema.compile(schema_source, id=schema_id)
    subset = readings[:AVRO_READINGS_CAP]
    batch = laser.topic(AVRO_TAPE_TOPIC).publish_batch().projection_ref(AVRO_PROJECTION)
    for reading in subset:
        batch = batch.add_avro(compiled, schema_id, reading)
    await batch.send()
    print(f"published {len(subset)} readings as raw Avro datums")

    await _common.wait_for_projection(laser, AVRO_TAPE_INDEX, len(subset))
    per_host = await laser.query(AVRO_TAPE_INDEX).sum(CPU_TOTAL).group_by([HOST]).fetch()
    print("weighted CPU total per host, aggregated over columns decoded out of Avro bodies:")
    for host, total in sorted(group_totals(per_host).items()):
        print(f"  {host:<7} {total:>14}")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        caps = await laser.capabilities()
        count = _common.messages(default=2000)

        await laser.topic(FEED_TOPIC).ensure(partitions=_common.PARTITIONS)
        await laser.topic(TAPE_TOPIC).ensure(partitions=_common.PARTITIONS)

        # Draw the whole session up front so the feed and the tape replay identical readings.
        readings = generate_readings(count)

        # Register the analytics projector before the tape is written so no reading
        # is missed by a projector that starts afterwards (managed-only).
        if caps.query.available:
            await _common.start_projector(laser, TAPE_TOPIC, COLUMNS, index=TAPE_INDEX)

        _common.phase("warming up")
        print(f"{count} readings across {len(OPENING)} hosts")
        _common.phase("streaming a live telemetry feed")
        view = await stream_live_view(laser, readings)
        view.snapshot(count)

        _common.phase("publishing the readings to the durable reading tape")
        await index_tape(laser, readings)

        if _common.managed_gate(caps.query.available, "query", EXAMPLE):
            await _common.wait_for_projection(laser, TAPE_INDEX, count)
            _common.phase("reading-tape analytics")
            await report_samples_and_mean(laser)

        _common.phase("typed tape audit: replaying the log as Reading values")
        await audit_tape(laser, readings)

        # The schema-first coda needs writer schemas from a managed deployment.
        if caps.managed:
            _common.phase("schema-first tape: Avro readings decoded by a registered writer schema")
            await avro_tape(laser, readings)
        elif caps.query.available:
            print("writer schemas need Laser Stack or LaserData Cloud, skipping the Avro tape")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
