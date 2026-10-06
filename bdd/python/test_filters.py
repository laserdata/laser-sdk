import asyncio
import json
import uuid
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "filters.feature"))
scenarios(str(SCENARIOS / "filters_live.feature"))

TOPIC = "fleet_changes"
READ_TIMEOUT = 15

# The record payloads and the feed order, shared with the Rust and TypeScript
# runners so every language evaluates byte-identical records.
_FIXTURES = json.loads((SCENARIOS / "filter_records.json").read_text())
RECORDS = {
    name: payload["text"].encode() if "text" in payload else bytes.fromhex(payload["hex"])
    for name, payload in _FIXTURES["records"].items()
}
FEED = _FIXTURES["feed"]


def safe_mode() -> ls.ConsumerFilter:
    def satellites() -> ls.FilterExpr:
        return ls.FilterExpr.pred("table", "eq", "satellites")

    return ls.ConsumerFilter.json(
        ls.FilterExpr.any(
            [
                ls.FilterExpr.all(
                    [
                        satellites(),
                        ls.FilterExpr.pred("op", "eq", "u"),
                        ls.FilterExpr.pred("changed", "contains", "mode"),
                        ls.FilterExpr.pred("after.mode", "eq", "safe"),
                    ]
                ),
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


FILTERS = {
    "mode present": lambda: ls.ConsumerFilter.json(ls.FilterExpr.present("fields.mode")),
    "safe mode": safe_mode,
    "safe mode values": lambda: ls.ConsumerFilter.json(
        ls.FilterExpr.all(
            [
                ls.FilterExpr.pred("table", "eq", "satellites"),
                ls.FilterExpr.pred("op", "eq", "u"),
                ls.FilterExpr.pred("after.mode", "eq", "safe"),
            ]
        )
    ),
    "mode is not safe": lambda: ls.ConsumerFilter.json(
        ls.FilterExpr.negate(ls.FilterExpr.pred("after.mode", "eq", "safe"))
    ),
    "catalog number 9007199254740993": lambda: ls.ConsumerFilter.json(
        ls.FilterExpr.pred("after.norad_id", "eq", 9007199254740993)
    ),
    "contact after noon UTC": lambda: ls.ConsumerFilter.json(
        ls.FilterExpr.pred_as("contact_at", "gt", "2026-09-21T12:00:00Z", "rfc3339")
    ),
    "critical priority": lambda: ls.ConsumerFilter.headers_only(
        ls.FilterExpr.header("priority", "eq", "critical")
    ),
}
VERDICTS = {"selected": "selected", "rejected": "rejected", "a fault": "fault"}


@given(parsers.parse('the "{name}" filter'))
def given_filter(bench, name):
    bench.filter = FILTERS[name]()


@when(parsers.parse('it evaluates the "{name}" record'))
def evaluate_record(bench, name):
    bench.verdict = ls.CompiledFilter.compile(bench.filter).evaluate(RECORDS[name])


@when(parsers.parse('it evaluates the "{name}" record with header "{key}" set to "{value}"'))
def evaluate_record_with_header(bench, name, key, value):
    bench.verdict = ls.CompiledFilter.compile(bench.filter).evaluate(RECORDS[name], {key: value})


@then(parsers.re(r"the record is (?P<verdict>selected|rejected|a fault)"))
def then_verdict(bench, verdict):
    assert bench.verdict == VERDICTS[verdict]


@then("its digest survives a round trip through its wire form")
def digest_round_trip(bench):
    assert ls.ConsumerFilter.from_dict(bench.filter.to_dict()).digest == bench.filter.digest


@then("a different fault policy gives a different digest")
def fault_policy_digest(bench):
    dropping = ls.ConsumerFilter.json(bench.filter.expr, fault_policy="drop")
    assert dropping.digest != bench.filter.digest


@given("a fresh fleet change feed")
def fresh_feed(world):
    world.connect()

    async def publish():
        producer = world.laser.topic(TOPIC).producer(partition=0, partitions=1)
        await producer.send_batch([RECORDS[name] for name in FEED])

    world.run(publish)


@when(parsers.parse('the anomaly desk reads the feed with the "{name}" filter'))
def read_feed(world, name):
    async def read():
        group = world.laser.topic(TOPIC).consumer_group("anomaly-desk")
        await group.create(filter=FILTERS[name]())
        reader = await group.reader(start="first")
        page = await asyncio.wait_for(reader.next_page(), READ_TIMEOUT)
        await reader.ack_page(page)
        await reader.close()
        return [record.message.payload for record in page.records]

    world.filtered = world.run(read)


@then(parsers.parse('it receives the "{first}", "{second}", and "{third}" records'))
def receives(world, first, second, third):
    assert world.filtered == [RECORDS[first], RECORDS[second], RECORDS[third]]


@when("the anomaly desk lists its saved filters")
def list_filters(world):
    world.capture(
        lambda: world.laser.topic(TOPIC).consumer_group("anomaly-desk").filter().revisions()
    )


@then("the catalog is refused as unsupported")
def catalog_unsupported(world):
    assert isinstance(world.error, ls.UnsupportedError)


@when("the anomaly desk manages a saved policy by numeric group id")
def managed_group(world):
    async def manage():
        topic = world.laser.topic(TOPIC)
        group = topic.consumer_group("managed-desk")
        operation_id = uuid.uuid4().int
        first = await group.create(filter=safe_mode(), operation_id=operation_id)
        assert await group.create(filter=safe_mode(), operation_id=operation_id) == first
        binding = first.filter
        by_id = topic.consumer_group_id(first.id)
        consumer = by_id.consumer(auto_commit="disabled")
        delivered = []
        try:
            for _ in range(3):
                record = await asyncio.wait_for(consumer.next(), READ_TIMEOUT)
                delivered.append(record.payload)
                await consumer.commit(record)
        finally:
            await consumer.shutdown()
        reader = await by_id.reader(start="first")
        try:
            page = await asyncio.wait_for(reader.next_page(), READ_TIMEOUT)
            await group.filter().set_revision_enabled(binding["revision"], False)
            await reader.ack_page(page)
            try:
                await reader.try_next_page()
            except ls.FilterError as error:
                assert "revision_disabled" in str(error)
            else:
                raise AssertionError("paused revision accepted new work")
            await group.filter().set_revision_enabled(binding["revision"], True)
            assert [record.message.payload for record in page.records] == delivered
            return delivered
        finally:
            await reader.close()
            await group.filter().release()

    world.filtered = world.run(manage)


@when("the anomaly desk reads every record through an unbound consumer group")
def read_unbound_consumer(world):
    async def read():
        topic = world.laser.topic(TOPIC)
        info = await topic.consumer_group("unbound-consumers").create()
        consumer = topic.consumer_group_id(info.id).consumer(batch_length=2, auto_commit="disabled")
        delivered = []
        try:
            for _ in FEED:
                record = await asyncio.wait_for(consumer.next(), READ_TIMEOUT)
                delivered.append(record.payload)
                await consumer.commit(record)
            return delivered
        finally:
            await consumer.shutdown()

    world.filtered = world.run(read)


@when("the anomaly desk reads every record through an unbound group reader")
def read_unbound_reader(world):
    async def read():
        group = world.laser.topic(TOPIC).consumer_group("unbound-readers")
        await group.create()
        reader = await group.reader(count=2, max_examined=2)
        delivered = []
        try:
            while len(delivered) < len(FEED):
                page = await asyncio.wait_for(reader.next_page(), READ_TIMEOUT)
                assert page.policy["mode"] == "unfiltered"
                assert page.examined == len(page.records)
                assert len(page.records) <= 2
                assert all(not record.evaluated for record in page.records)
                delivered.extend(record.message.payload for record in page.records)
                await reader.ack_page(page)
            return delivered
        finally:
            await reader.close()

    world.filtered = world.run(read)


@then("it receives every original feed record")
def receives_every_record(world):
    assert world.filtered == [RECORDS[name] for name in FEED]


@when("the anomaly desk scans a hundred non-matches before its first match")
def read_bounded_scan(world):
    async def read():
        topic = world.laser.topic("sparse_changes")
        await topic.ensure(1)
        await topic.producer(partition=0, partitions=1).send_batch(
            [RECORDS["ground station update"]] * 100 + [RECORDS["mode update"]]
        )
        group = topic.consumer_group("bounded-readers")
        await group.create(filter=safe_mode())
        reader = await group.reader(start="first", count=1, max_examined=100)
        try:
            empty, more = await reader.read_round()
            assert empty is None
            assert more is True
            assert reader.examined_in_round() == 100
            page, _ = await reader.read_round()
            assert page is not None
            assert reader.examined_in_round() == 1
            assert [record.offset for record in page.records] == [100]
            await reader.ack_page(page)
            return [record.message.payload for record in page.records]
        finally:
            await reader.close()

    world.filtered = world.run(read)


@then("its first match follows an empty scan of one hundred records")
def first_match_after_empty(world):
    assert world.filtered == [RECORDS["mode update"]]


@when("the anomaly desk consumes a hundred non-matches before its first match")
def consume_bounded_scan(world):
    async def read():
        topic = world.laser.topic("sparse_changes")
        await topic.ensure(1)
        await topic.producer(partition=0, partitions=1).send_batch(
            [RECORDS["ground station update"]] * 100 + [RECORDS["mode update"]]
        )
        group = topic.consumer_group("sparse-consumers")
        await group.create(filter=safe_mode())
        consumer = group.consumer(batch_length=100, auto_commit="disabled", polling="first")
        try:
            record = await asyncio.wait_for(consumer.next(), READ_TIMEOUT)
            assert record.position.offset == 100
            assert await consumer.last_stored_offset(0) == 99
            await consumer.commit(record)
            return [record.payload]
        finally:
            await consumer.shutdown()

    world.filtered = world.run(read)
