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
    bench.verdict = bench.filter.evaluate(RECORDS[name])


@when(parsers.parse('it evaluates the "{name}" record with header "{key}" set to "{value}"'))
def evaluate_record_with_header(bench, name, key, value):
    bench.verdict = bench.filter.evaluate(RECORDS[name], {key: value})


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
        reader = await world.laser.filters().reader(
            world.laser.default_stream,
            TOPIC,
            consumer="anomaly-desk",
            filter=FILTERS[name](),
            start="first",
        )
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
    world.capture(lambda: world.laser.filters().list())


@then("the catalog is refused as unsupported")
def catalog_unsupported(world):
    assert isinstance(world.error, ls.UnsupportedError)


@when("the anomaly desk manages a saved policy by numeric group id")
def managed_group(world):
    async def manage():
        filters = world.laser.filters()
        stream = world.laser.default_stream
        operation_id = uuid.uuid4().int
        mutation = {
            "register": {
                "name": f"bdd-{stream}",
                "description": "",
                "filter": safe_mode().to_dict(),
            }
        }
        first = await filters.apply_as(operation_id, mutation)
        assert await filters.apply_as(operation_id, mutation) == first
        saved = first["registered"]
        binding = await filters.create_consumer_group(
            stream, TOPIC, "managed-desk", saved["filter_id"], saved["revision"]
        )
        reader = await filters.reader(stream, TOPIC, group_id=binding["identity"]["group_id"])
        try:
            page = await asyncio.wait_for(reader.next_page(), READ_TIMEOUT)
            await filters.set_revision_enabled(saved["filter_id"], saved["revision"], False)
            await reader.ack_page(page)
            try:
                await reader.try_next_page()
            except ls.FilterError as error:
                assert "revision_disabled" in str(error)
            else:
                raise AssertionError("paused revision accepted new work")
            await filters.set_revision_enabled(saved["filter_id"], saved["revision"], True)
            return [record.message.payload for record in page.records]
        finally:
            await reader.close()
            await filters.unbind_binding(binding)
            await filters.archive(saved["filter_id"])
            await filters.delete(saved["filter_id"])

    world.filtered = world.run(manage)
