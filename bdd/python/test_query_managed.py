from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "query_managed.feature"))

CATCH_UP_MS = 120_000


@given(parsers.parse('{count:d} api-call records published and materialized into "{topic}"'))
def materialized_api_calls(world, count, topic):
    handle = world.laser.topic(topic)
    world.run(lambda: handle.ensure(1))
    projection = (
        ls.ProjectionBuilder(topic).content_type("json").field("status").field("latency_ms").build()
    )
    materialization = world.run(handle.materialize().typed(projection).apply)
    records = [
        {"status": "500" if latency % 4 == 0 else "200", "latency_ms": latency}
        for latency in range(count)
    ]
    world.run(lambda: handle.publish_batch().extend_json(records).send())
    wait = world.run(lambda: materialization.wait("caught_up", CATCH_UP_MS))
    assert wait["kind"] == "ready", wait


@when(
    parsers.parse(
        'the materialized "{index}" is queried for latency_ms greater than {bound:d} with its total'
    )
)
def filtered(world, index, bound):
    query = world.laser.query(index).filter_gt("latency_ms", bound).with_total()
    world.result = world.run(query.fetch)


@when(
    parsers.parse(
        'the materialized "{index}" is queried ordered by latency_ms descending'
        " with limit {limit:d}"
    )
)
def ordered(world, index, limit):
    query = world.laser.query(index).order_desc("latency_ms").limit(limit)
    world.result = world.run(query.fetch)


@when(parsers.parse('the materialized "{index}" is queried with limit {limit:d} and its total'))
def limited(world, index, limit):
    world.result = world.run(world.laser.query(index).limit(limit).with_total().fetch)


@when(parsers.parse('the materialized "{index}" is counted grouped by status'))
def counted(world, index):
    world.result = world.run(world.laser.query(index).count().group_by(["status"]).fetch)


@when(
    parsers.parse('the materialized "{index}" is walked in pages of {page:d} ordered by latency_ms')
)
def walked(world, index, page):
    latencies = []
    while True:
        result = world.run(
            world.laser.query(index)
            .order_asc("latency_ms")
            .limit(page)
            .offset(len(latencies))
            .fetch
        )
        latencies += [latency(result, row) for row in result.rows]
        if len(result.rows) < page:
            break
    world.walked = latencies


@when(
    parsers.parse(
        'the materialized "{index}" is read whole in pages of {page:d} ordered by latency_ms'
    )
)
def read_whole(world, index, page):
    walked = world.run(world.laser.query(index).order_asc("latency_ms").limit(page).fetch_all)
    world.walk_frontier_moved = walked.frontier_moved
    world.walked = [row.value_i64("latency_ms") for row in walked]


@then("the whole read reports its frontier unmoved")
def frontier_unmoved(world):
    assert world.walk_frontier_moved is False


@then(parsers.parse("the materialized total is {total:d}"))
def total(world, total):
    assert world.result.page.total == total


@then(parsers.parse("every materialized row has latency_ms greater than {bound:d}"))
def rows_above(world, bound):
    assert world.result.rows, "the page holds matching rows"
    assert all(latency(world.result, row) > bound for row in world.result.rows)


@then(parsers.parse('the materialized latency_ms values are "{expected}" in order'))
def values_in_order(world, expected):
    got = [str(latency(world.result, row)) for row in world.result.rows]
    assert got == expected.split(", ")


@then(parsers.parse("the materialized page holds {rows:d} rows"))
def page_rows(world, rows):
    assert len(world.result.rows) == rows


@then(parsers.parse('the materialized group "{status}" has count {count:d}'))
def group_count(world, status, count):
    result = world.result
    row = next(row for row in result.rows if result.value_text(row, "status") == status)
    assert result.value_i64(row, "count") == count


@then(parsers.parse("the walk returns every latency_ms from {first:d} to {last:d} once"))
def walk_complete(world, first, last):
    assert world.walked == list(range(first, last + 1))


def latency(result, row):
    value = result.value_i64(row, "latency_ms")
    assert value is not None, "a numeric latency_ms"
    return value
