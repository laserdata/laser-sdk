"""cdc (Consumer filters primitive): read only the change-feed records you care about.

The server selects the matching records of a change feed, so the reader
receives only those records, with their original offsets. A satellite fleet
streams the change feed of its mission-ops database: every battery reading,
orbit maneuver, and ground-station status flip. The anomaly desk wants the
satellites that enter safe mode or leave the fleet, a handful of records out
of hundreds, and everything else never leaves the broker.

What it shows:
  - publish a busy feed of typed change records, dataclasses keyed by
    satellite (`topic.publish(record).partition_key(key)`)
  - read with an inline filter that needs no catalog, decode every delivered
    record back into its dataclass, and see how much of the feed stayed on
    the broker
  - test a strict filter and a values-only filter on the same record
  - preview every partition without storing progress
  - route binary alerts on a `priority` header with a headers-only filter,
    on their own topic, without decoding a payload
  - with laser-plane: save filters, create and bind independent A/B groups,
    read by numeric group ID, pause and resume revisions, see a bound filter
    refuse deletion, release exact bindings, then archive and delete
  - filter typed CBOR, Avro, and Protobuf readings, using registered writer
    schemas for Avro and Protobuf

Consumer filters need a server that serves them, as the LaserData Iggy fork in
Laser Stack or LaserData Cloud does, and skip elsewhere. Saved filters and
group bindings also need laser-plane.

**The sample saves 98.5% of payload transfer: 424 of 27,953 bytes.**
The reader receives 4 of 240 records, processes each typed change, then
acknowledges it. Binary alerts use a one-byte uint8 priority header.

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
BACKFILL = "safe-mode-backfill"
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


def safe_mode_values_filter() -> ls.ConsumerFilter:
    """Values only: any update of a satellite whose current mode is safe."""
    return ls.ConsumerFilter.json(
        ls.FilterExpr.all(
            [
                ls.FilterExpr.pred("table", "eq", "satellites"),
                ls.FilterExpr.pred("op", "eq", "u"),
                ls.FilterExpr.pred("after.mode", "eq", "safe"),
            ]
        )
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


async def route_alerts(laser: ls.Laser, stream: str) -> None:
    """Binary alert frames carry their priority as a header. A headers-only
    filter selects the critical ones without decoding a payload, so the alert
    topic can hold any format."""
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
    pager = await laser.filters().reader(
        stream,
        ALERTS,
        consumer=f"{BACKFILL}-pager",
        filter=ls.ConsumerFilter.headers_only(ls.FilterExpr.header("priority", "eq", CRITICAL)),
        start="first",
    )

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


async def manage_group(laser: ls.Laser, stream: str, expected: int) -> None:
    """Save both filters, bind the anomaly desk to the strict one, consume as
    the group, then release everything this run created, also when a step
    fails."""
    _common.phase("save both filters in the catalog")
    filters = laser.filters()
    bindings: list = []
    filter_ids: list = []
    try:
        strict = await filters.register(
            f"sats-safe-mode-{_common.run_token()}",
            safe_mode_filter(),
            description="Satellites entering safe mode, reporting it, or leaving the fleet",
        )
        filter_ids.append(strict["filter_id"])
        values = await filters.register(
            f"sats-safe-mode-values-{_common.run_token()}",
            safe_mode_values_filter(),
            description="Every update of a satellite whose current mode is safe",
        )
        filter_ids.append(values["filter_id"])
        print(
            f"  strict is filter {strict['filter_id']} revision {strict['revision']}, "
            f"values only is filter {values['filter_id']} revision {values['revision']}"
        )

        _common.phase("bind the anomaly desk group to the strict filter")
        binding = await filters.create_consumer_group(
            stream, TOPIC, GROUP, strict["filter_id"], strict["revision"]
        )
        bindings.append(binding)
        print(f"  {GROUP} runs revision {binding['revision']} from now on")

        _common.phase("consume as the group and acknowledge")
        desk = await filters.reader(
            stream, TOPIC, group_id=binding["identity"]["group_id"], start="first", local_guard=True
        )
        try:
            handled = await read_matches(desk, expected)
            print(f"  the desk handled {len(handled)} safe-mode or decommission events")
        finally:
            await desk.close()

        _common.phase("A/B: a second revision runs in its own group")
        second = await filters.revise(
            strict["filter_id"], strict["revision"], ls.ConsumerFilter.json(safe_mode_transition())
        )
        variant_binding = await filters.create_consumer_group(
            stream, TOPIC, f"{GROUP}-transitions", second["filter_id"], second["revision"]
        )
        bindings.append(variant_binding)
        variant = await filters.reader(
            stream,
            TOPIC,
            group_id=variant_binding["identity"]["group_id"],
            count=1,
            local_guard=True,
            start="first",
        )
        try:
            first = await asyncio.wait_for(variant.next_record(), READ_TIMEOUT)
            change = fleet_change(first.message.json())
            print(f"  revision {second['revision']}: {change.describe()}")
            await filters.set_revision_enabled(second["filter_id"], second["revision"], False)
            await variant.ack(first)
            try:
                await variant.try_next_page()
            except ls.FilterError as error:
                if error.reason != "revision_disabled":
                    raise
                print("  paused: new reads stop, in-flight work can still be acknowledged")
            else:
                raise RuntimeError("a disabled revision kept reading")
            await filters.set_revision_enabled(second["filter_id"], second["revision"], True)
            await read_matches(variant, 1)
            print(f"  A/B groups handled {expected} broad events and 2 transitions independently")
        finally:
            await variant.close()

        _common.phase("a bound filter cannot be deleted")
        try:
            await filters.delete(strict["filter_id"])
        except ls.FilterError as error:
            if error.reason != "conflict":
                raise
            print(f"  refused with conflict while {GROUP} is bound")
        else:
            raise RuntimeError("a bound filter was deleted")
    except BaseException:
        await release(filters, bindings, filter_ids)
        raise
    error = await release(filters, bindings, filter_ids)
    if error is not None:
        raise error
    print("  both filters are gone, their names are never reused")


async def release(filters, bindings: list, filter_ids: list) -> Exception | None:
    """Release every binding in creation order, then archive and delete each
    filter, also when a step fails. Every step runs, the first error is
    returned, as the Rust and TypeScript examples do."""
    _common.phase("unbind, archive, delete")
    first_error: Exception | None = None
    for binding in bindings:
        try:
            await filters.unbind_binding(binding)
        except Exception as error:
            first_error = first_error or error
    for filter_id in filter_ids:
        for step in (filters.archive, filters.delete):
            try:
                await step(filter_id)
            except Exception as error:
                first_error = first_error or error
    return first_error


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    stream = _common.stream_for(EXAMPLE)
    try:
        caps = await laser.capabilities()
        if not _common.managed_gate(caps.filters, "consumer filters", EXAMPLE):
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

        _common.phase("read only the safe-mode or decommission events, no catalog needed")
        backfill = await laser.filters().reader(
            stream, TOPIC, consumer=BACKFILL, filter=safe_mode_filter(), start="first"
        )
        try:
            delivered = await read_matches(backfill, strict_matches)
        finally:
            await backfill.close()
        delivered_bytes = sum(len(record.message.payload) for record in delivered)
        kept = 100 * (published_bytes - delivered_bytes) / max(published_bytes, 1)
        print(
            f"  delivered {len(delivered)} of {len(feed)} records, "
            f"{delivered_bytes} of {published_bytes} payload bytes: "
            f"{kept:.1f}% stayed on the broker"
        )

        _common.phase(
            "test both filters against a battery update of a satellite already in safe mode"
        )
        still_safe = satellite_update(2, "safe", 58, "battery_pct").to_dict()
        for name, candidate in [
            ("strict, transitions only", safe_mode_filter()),
            ("values only, current state", safe_mode_values_filter()),
        ]:
            tested = await laser.filters().test(
                json.dumps(still_safe, separators=(",", ":")), filter=candidate
            )
            print(f"  {name}: {tested['explanation']['verdict']}")

        _common.phase("preview every partition, nothing is stored")
        for partition_id in range(PARTITIONS):
            preview = await laser.filters().preview(
                stream, TOPIC, partition_id, filter=safe_mode_filter(), max_records=10
            )
            print(
                f"  partition {partition_id}: examined {preview['examined']}, "
                f"matched {preview['matched']}, stopped at {preview['stop']}"
            )

        _common.phase("route binary alerts on a header, their payload is never decoded")
        await route_alerts(laser, stream)
        await run_codecs(laser, stream, caps.filters_catalog)

        if not caps.filters_catalog:
            print("  the saved-filter catalog needs a managed plane, skipping group bindings")
            return
        await manage_group(laser, stream, strict_matches)
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
