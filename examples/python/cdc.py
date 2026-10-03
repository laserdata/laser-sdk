"""cdc (Consumer filters primitive): read only the change-feed records you care about.

A consumer group owns its filter. The server selects the matching records of a
change feed for the group, so its consumers receive only those records, with
their original offsets, and never name a filter themselves. A satellite fleet
streams the change feed of its mission-ops database: every battery reading,
orbit maneuver, and ground-station status flip. The anomaly desk wants the
satellites that enter safe mode or leave the fleet, a handful of records out
of hundreds, and everything else never leaves the broker.

What it shows:
  - publish a busy feed of typed change records, dataclasses keyed by
    satellite (`topic.publish(record).partition_key(key)`)
  - create the anomaly desk group with its filter in one call
  - consume as the group with the normal consumer, decode every delivered
    record back into its dataclass, commit after handling, and see how much
    of the feed stayed on the broker
  - page the matches again with the group reader and its own scan budget
  - test the group's filter on one record and preview every partition
    without storing progress
  - route binary alerts on a `priority` header with a headers-only group
    filter, on their own topic, without decoding a payload
  - filter typed CBOR, Avro, and Protobuf readings, using registered writer
    schemas for Avro and Protobuf
  - draft a stricter revision, run the variant in its own A/B group, pause
    and resume it, see a running policy refuse another one, release and delete both

Consumer group filters need laser-plane, as Laser Stack or LaserData Cloud
runs it, and skip elsewhere.

**The sample saves 98.5% of payload transfer: 424 of 27,953 bytes.**
The desk receives 4 of 240 records, processes each typed change, then
commits it. Binary alerts use a one-byte uint8 priority header.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 cdc.py

Docs: https://docs.laserdata.cloud/laser-sdk/consumer-filters
Related primitive: watch.py (reports view progress instead of filtering the log)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import _common
import laser_sdk as ls
from _cdc_codecs import run_codecs

EXAMPLE = "cdc"
TOPIC = "fleet_changes"
ALERTS = "fleet_alerts"
PARTITIONS = 3
GROUP = "anomaly-desk"
SATELLITES = 8
FEED_SIZE = 240
ROUTINE = 1
CRITICAL = 2
READ_TIMEOUT = 15

Mode = Literal["nominal", "maneuver", "safe"]
Column = Literal["mode", "battery_pct", "status"]


@dataclass(frozen=True)
class Satellite:
    id: str
    name: str
    mode: Mode
    orbit: Literal["leo", "meo"]
    battery_pct: int


@dataclass(frozen=True)
class GroundStation:
    id: str
    status: Literal["online", "maintenance"]


@dataclass(frozen=True)
class SatelliteChange:
    """A captured change of one satellite row."""

    op: Literal["u", "d"]
    changed: list[Column] = field(default_factory=list)
    after: Satellite | None = None
    before_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        record: dict[str, Any] = {"table": "satellites", "op": self.op}
        if self.changed:
            record["changed"] = list(self.changed)
        if self.after is not None:
            record["after"] = asdict(self.after)
        if self.before_id is not None:
            record["before"] = {"id": self.before_id}
        return record

    @property
    def key(self) -> str:
        return self.after.id if self.after is not None else self.before_id or ""

    def describe(self) -> str:
        if self.after is None:
            return f"{self.before_id} left the fleet"
        return (
            f"{self.after.name} entered {self.after.mode} mode at {self.after.battery_pct}% battery"
        )


@dataclass(frozen=True)
class GroundStationChange:
    """A captured change of one ground-station row."""

    after: GroundStation
    changed: list[Column] = field(default_factory=lambda: ["status"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": "ground_stations",
            "op": "u",
            "changed": list(self.changed),
            "after": asdict(self.after),
        }

    @property
    def key(self) -> str:
        return self.after.id

    def describe(self) -> str:
        return f"{self.after.id} is {self.after.status}"


@dataclass(frozen=True)
class TelemetryEvent:
    """A telemetry event a satellite reports."""

    satellite_id: str
    mode: Mode | None = None
    battery_pct: int | None = None

    def to_dict(self) -> dict[str, Any]:
        fields = {"mode": self.mode, "battery_pct": self.battery_pct}
        return {
            "event": "satellite.telemetry_changed",
            "satellite_id": self.satellite_id,
            "fields": {name: value for name, value in fields.items() if value is not None},
        }

    @property
    def key(self) -> str:
        return self.satellite_id

    def describe(self) -> str:
        return f"{self.satellite_id} reported {self.mode or 'no'} mode"


FleetChange = SatelliteChange | GroundStationChange | TelemetryEvent


def fleet_change(record: dict[str, Any]) -> FleetChange:
    """Decode one delivered record into the dataclass it is."""
    if "event" in record:
        fields = record["fields"]
        return TelemetryEvent(record["satellite_id"], fields.get("mode"), fields.get("battery_pct"))
    if record["table"] == "ground_stations":
        return GroundStationChange(GroundStation(**record["after"]), record["changed"])
    after = record.get("after")
    return SatelliteChange(
        op=record["op"],
        changed=record.get("changed", []),
        after=Satellite(**after) if after is not None else None,
        before_id=record.get("before", {}).get("id"),
    )


def satellite(index: int, mode: Mode, battery_pct: int) -> Satellite:
    return Satellite(
        id=f"sat-{index + 1:03}",
        name=f"Kestrel-{index + 1}",
        mode=mode,
        orbit="leo" if index % 2 == 0 else "meo",
        battery_pct=battery_pct,
    )


def satellite_update(index: int, mode: Mode, battery: int, changed: Column) -> SatelliteChange:
    return SatelliteChange(op="u", changed=[changed], after=satellite(index, mode, battery))


def fleet_feed() -> tuple[list[FleetChange], int]:
    """A deterministic feed: mostly battery telemetry, battery updates, orbit
    maneuvers, and ground-station status flips, with the four events the
    anomaly desk cares about spread through it. Returns the feed and how many
    records the strict filter selects."""
    modes: list[Mode] = ["nominal"] * SATELLITES
    records: list[FleetChange] = []
    strict_matches = 0
    for tick in range(FEED_SIZE):
        index = tick % SATELLITES
        battery = 90 - (tick * 7) % 40
        if tick in (FEED_SIZE // 4, 3 * FEED_SIZE // 4):
            entering = 2 if tick == FEED_SIZE // 4 else 6
            modes[entering] = "safe"
            strict_matches += 1
            records.append(satellite_update(entering, "safe", battery, "mode"))
        elif tick == FEED_SIZE // 2:
            strict_matches += 1
            records.append(TelemetryEvent(satellite(4, "nominal", 0).id, mode="safe"))
        elif tick == FEED_SIZE - 1:
            strict_matches += 1
            records.append(SatelliteChange(op="d", before_id=satellite(7, "nominal", 0).id))
        elif tick % 10 <= 4:
            records.append(TelemetryEvent(satellite(index, "nominal", 0).id, battery_pct=battery))
        elif tick % 10 <= 7 or modes[index] == "safe":
            records.append(satellite_update(index, modes[index], battery, "battery_pct"))
        elif tick % 10 == 8:
            status = "maintenance" if tick % 20 == 8 else "online"
            station = ("svalbard", "kiruna", "punta-arenas")[tick % 3]
            records.append(GroundStationChange(GroundStation(station, status)))
        else:
            records.append(satellite_update(index, "maneuver", battery, "mode"))
    return records, strict_matches


def safe_mode_filter() -> ls.ConsumerFilter:
    """Strict: a satellite update that marks its mode as changed to safe, a
    telemetry report of safe mode, or a satellite leaving the fleet."""

    def satellites() -> ls.FilterExpr:
        return ls.FilterExpr.pred("table", "eq", "satellites")

    return ls.ConsumerFilter.json(
        ls.FilterExpr.any(
            [
                safe_mode_transition(),
                ls.FilterExpr.all([satellites(), ls.FilterExpr.pred("op", "eq", "d")]),
                ls.FilterExpr.all(
                    [
                        ls.FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
                        ls.FilterExpr.pred("fields.mode", "eq", "safe"),
                    ]
                ),
            ]
        )
    )


def safe_mode_transition() -> ls.FilterExpr:
    return ls.FilterExpr.all(
        [
            ls.FilterExpr.pred("table", "eq", "satellites"),
            ls.FilterExpr.pred("op", "eq", "u"),
            ls.FilterExpr.pred("changed", "contains", "mode"),
            ls.FilterExpr.pred("after.mode", "eq", "safe"),
        ]
    )


async def read_matches(reader: ls.FilteredReader, expected: int) -> list[ls.MatchedRecord]:
    """Process each typed change before acknowledging it."""
    delivered: list[ls.MatchedRecord] = []
    for _ in range(expected):
        record = await asyncio.wait_for(reader.next_record(), READ_TIMEOUT)
        change = fleet_change(record.message.json())
        print(f"  partition {record.partition_id} offset {record.offset}: {change.describe()}")
        await reader.ack(record)
        delivered.append(record)
    return delivered


async def route_alerts(laser: ls.Laser) -> None:
    """Binary alert frames carry their priority as a header. A pager group
    with a headers-only filter selects the critical ones without decoding a
    payload, so the alert topic can hold any format."""
    alerts = laser.topic(ALERTS)
    await alerts.ensure(1)
    producer = alerts.producer(partitions=1)
    for priority, satellite_id in [
        (ROUTINE, "sat-001"),
        (CRITICAL, "sat-003"),
        (ROUTINE, "sat-004"),
        (CRITICAL, "sat-007"),
    ]:
        frame = b"\x0a\x07" + satellite_id.encode()
        await producer.send(frame, headers={"priority": ("uint8", priority)}, key=satellite_id)
    pager_group = alerts.consumer_group(f"{GROUP}-pager-{_common.run_token()}")
    await pager_group.create(
        filter=ls.ConsumerFilter.headers_only(ls.FilterExpr.header("priority", "eq", CRITICAL))
    )
    pager = await pager_group.reader(start="first")
    try:
        for _ in range(2):
            record = await asyncio.wait_for(pager.next_record(), READ_TIMEOUT)
            print(
                f"  critical alert at offset {record.offset}: "
                f"{len(record.message.payload)} opaque bytes"
            )
            await pager.ack(record)
    finally:
        await pager.close()
        await pager_group.filter().delete()


async def manage_revisions(laser: ls.Laser, desk, binding: dict, expected: int) -> None:
    """Draft a stricter revision on the desk's own filter, run the variant in
    its own group, pause and resume it, then release and delete both
    policies."""
    _common.phase("draft a stricter revision: readers keep running the active one")
    draft = await desk.filter().revise(
        binding["revision"], ls.ConsumerFilter.json(safe_mode_transition())
    )
    revisions = await desk.filter().revisions(page=0, page_size=10)
    print(
        f"  revision {draft['revision']} drafted, the group lists {revisions['total']} "
        f"revisions and still runs revision {binding['revision']}"
    )

    _common.phase("A/B: the transitions-only variant runs in its own group")
    variant_name = f"{GROUP}-transitions-{_common.run_token()}"
    variant = laser.topic(TOPIC).consumer_group(variant_name)
    created = await variant.create(filter=ls.ConsumerFilter.json(safe_mode_transition()))
    variant_binding = created["filter"]
    reader = await variant.reader(count=1, local_guard=True, start="first")
    try:
        first = await asyncio.wait_for(reader.next_record(), READ_TIMEOUT)
        change = fleet_change(first.message.json())
        print(f"  {variant_name}: {change.describe()}")

        _common.phase("pause the variant: new reads stop, in-flight work still acknowledges")
        await variant.filter().set_revision_enabled(variant_binding["revision"], False)
        await reader.ack(first)
        try:
            await reader.try_next_page()
        except ls.FilterError as error:
            if error.reason != "revision_disabled":
                raise
            print("  paused: the server refuses new reads with revision_disabled")
        else:
            raise RuntimeError("a disabled revision kept reading")
        await variant.filter().set_revision_enabled(variant_binding["revision"], True)
        await read_matches(reader, 1)
        print(f"  resumed: the desk handled {expected} broad events, the variant 2 transitions")
    finally:
        await reader.close()

    _common.phase("a group that runs a policy cannot be switched to another one")
    try:
        await desk.filter().configure(ls.ConsumerFilter.json(safe_mode_transition()))
    except ls.FilterError as error:
        if error.reason != "conflict":
            raise
        print("  refused with conflict: create a new group for another policy")
    else:
        raise RuntimeError("a running policy was replaced")

    _common.phase("release both policies")
    released = await desk.filter().release()
    await variant.filter().release()
    print(
        f"  {released['group']['group']} is unbound again and receives every record, "
        f"its filter stays saved as revision {released['revision']}"
    )

    _common.phase("delete both filters: nothing of them stays in the catalog")
    await desk.filter().delete()
    await variant.filter().delete()
    print("  deleted with every revision, the groups keep reading everything")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        caps = await laser.capabilities()
        if not _common.managed_gate(caps.filters_catalog, "consumer group filters", EXAMPLE):
            return

        _common.phase("publish a busy fleet change feed, keyed by satellite")
        feed, strict_matches = fleet_feed()
        topic = laser.topic(TOPIC)
        await topic.ensure(PARTITIONS)
        published_bytes = 0
        for change in feed:
            record = change.to_dict()
            published_bytes += len(json.dumps(record, separators=(",", ":")).encode())
            await topic.publish(record).partition_key(change.key).send()
        print(
            f"  {len(feed)} records, {published_bytes} bytes: battery readings, maneuvers, "
            f"station flips, and {strict_matches} safe-mode or decommission events"
        )

        _common.phase("create the anomaly desk group with its filter")
        desk = topic.consumer_group(f"{GROUP}-{_common.run_token()}")
        created = await desk.create(filter=safe_mode_filter())
        binding = created["filter"]
        print(
            f"  group {created['name']} ({created['id']}) runs revision "
            f"{binding['revision']} of its own filter from now on"
        )

        _common.phase(
            "consume as the group: the application names the group, the server runs its filter"
        )
        consumer = desk.consumer(polling="first", auto_commit="disabled")
        delivered_bytes = 0
        try:
            for _ in range(strict_matches):
                message = await asyncio.wait_for(consumer.next(), READ_TIMEOUT)
                change = fleet_change(message.json())
                print(
                    f"  partition {message.partition_id} offset {message.offset}: "
                    f"{change.describe()}"
                )
                delivered_bytes += len(message.payload)
                await consumer.commit(message)
        finally:
            await consumer.shutdown()
        kept = 100 * (published_bytes - delivered_bytes) / max(published_bytes, 1)
        print(
            f"  delivered {strict_matches} of {len(feed)} records, "
            f"{delivered_bytes} of {published_bytes} payload bytes: "
            f"{kept:.1f}% stayed on the broker"
        )

        _common.phase("page the matches again with the group reader and its own scan budget")
        pager = await desk.reader(start="first", count=10, max_examined=100, local_guard=True)
        try:
            paged = await read_matches(pager, strict_matches)
        finally:
            await pager.close()
        print(
            f"  the reader handed out {len(paged)} matches in pages, "
            f"each acknowledged after handling"
        )

        _common.phase(
            "test the group's filter against a battery update of a satellite already in safe mode"
        )
        still_safe = satellite_update(2, "safe", 58, "battery_pct").to_dict()
        tested = await desk.filter().test(json.dumps(still_safe, separators=(",", ":")))
        print(f"  strict, transitions only: {tested['explanation']['verdict']}")

        _common.phase("preview every partition, nothing is stored")
        for partition_id in range(PARTITIONS):
            preview = await desk.filter().preview(partition_id, max_records=10)
            print(
                f"  partition {partition_id}: examined {preview['examined']}, "
                f"matched {preview['matched']}, stopped at {preview['stop']}"
            )

        _common.phase("route binary alerts on a header, their payload is never decoded")
        await route_alerts(laser)
        await run_codecs(laser)

        await manage_revisions(laser, desk, binding, strict_matches)
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
