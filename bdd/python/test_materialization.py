from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import laser_sdk as ls
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios(str(Path(__file__).parent.parent / "scenarios" / "materialization.feature"))
scenarios(str(Path(__file__).parent.parent / "scenarios" / "operational_sql.feature"))


@given(
    parsers.parse("a realized binding with captured head {head:d} and applied offset {applied:d}")
)
def captured(bench, head, applied):
    bench.materialization_status = {
        "source": {"stream": "shop", "topic": "orders"},
        "index": "orders",
        "realization": "realized",
        "source_incarnation": 7,
        "partitions": [
            {"partition_id": 0, "head_offset": head, "applied_offset": applied, "first_offset": 0}
        ],
        "holds": [],
        "accepted_gaps": [],
        "skipped_records": 0,
    }
    bench.materialization_target = deepcopy(bench.materialization_status)


@when(
    parsers.parse(
        "the binding advances to offset {applied:d} while its source head grows to {head:d}"
    )
)
def advances(bench, applied, head):
    bench.materialization_status["partitions"][0].update(applied_offset=applied, head_offset=head)


@then("the binding has reached the captured head")
def reached(bench):
    assert ls.binding_status_caught_up_to(
        bench.materialization_status, bench.materialization_target
    )


@when("the binding has no known first offset")
def unknown_start(bench):
    bench.materialization_status["partitions"][0].pop("first_offset")


@then("the binding cannot claim complete history")
def incomplete(bench):
    assert not ls.binding_status_history_complete(bench.materialization_status, "block")


@when("another partition lost its records before the binding read them")
def expired_unread(bench):
    assert ls.binding_status_history_complete(bench.materialization_status, "block")
    bench.materialization_status["partitions"].append({"partition_id": 1, "first_offset": 6})


@given(parsers.parse('a pending binding whose table creation failed with class "{failure}"'))
def failed_binding(bench, failure):
    bench.materialization_status = {
        "source": {"stream": "shop", "topic": "orders"},
        "index": "orders",
        "realization": "pending",
        "failure": failure,
    }


@when(parsers.parse('its failure class becomes "{failure}"'))
def failure_becomes(bench, failure):
    bench.materialization_status["failure"] = failure


@then("the binding has failed for good")
def failed_for_good(bench):
    assert ls.binding_status_is_failed(bench.materialization_status)


@then("the binding has not failed for good")
def not_failed_for_good(bench):
    assert not ls.binding_status_is_failed(bench.materialization_status)


@when("I materialize whole records twice under one operation identity")
def materialize(world):
    topic = world.laser.topic("whole_records")
    world.run(lambda: topic.ensure(1))
    request = topic.materialize().operation_id(uuid4().int)
    world.run(request.apply)
    world.materialization = world.run(request.apply)


@when("I publish two arbitrary records to the materialized topic")
def publish(world):
    for body in [b"not-json", bytes([0, 255, 1, 2])]:
        world.run(world.laser.topic("whole_records").publish().payload(body).send)


@then("materialization reaches the captured source head")
def ready(world):
    result = world.run(lambda: world.materialization.wait("caught_up", 20_000))
    assert result["kind"] == "ready", result


@then("the materialized query returns both original bodies exactly once")
def bodies(world):
    result = world.run(world.laser.query("whole_records").with_payload().fetch)
    assert len(result.rows) == 2
    values = [result.value(row, "__laser_original_payload") for row in result.rows]
    assert all(value["kind"] == "binary" for value in values)
    actual = [bytes(value["value"]) for value in values]
    assert sorted(actual) == sorted([b"not-json", bytes([0, 255, 1, 2])])


@when("I materialize keyed records")
def keyed(world):
    materialize_keyed(world, "delete")


@when("I materialize keyed records without a delete header")
def keyed_without_delete(world):
    materialize_keyed(world, None)


def materialize_keyed(world, delete_header):
    topic = world.laser.topic("keyed_records")
    world.run(lambda: topic.ensure(1))
    projection = (
        ls.ProjectionBuilder("keyed_records")
        .record()
        .keyed({"kind": "header", "name": "key"}, delete_header)
        .build()
    )
    world.materialization = world.run(topic.materialize().typed(projection).apply)


@when(parsers.parse('I publish keyed value "{value}"'))
def keyed_value(world, value):
    world.run(
        world.laser.topic("keyed_records")
        .publish()
        .header("key", "one")
        .payload(value.encode())
        .send
    )


@when(parsers.parse('I publish keyed value "{value}" with a "{header}" header'))
def keyed_value_with_header(world, value, header):
    world.run(
        world.laser.topic("keyed_records")
        .publish()
        .header("key", "one")
        .header(header, "true")
        .payload(value.encode())
        .send
    )


@when("I delete the materialized key")
def delete_key(world):
    world.run(
        world.laser.topic("keyed_records")
        .publish()
        .header("key", "one")
        .header("delete", "true")
        .payload(bytes([0]))
        .send
    )


@then(parsers.parse('the keyed view contains only "{value}"'))
def keyed_contains(world, value):
    ready(world)
    result = world.run(world.laser.query("keyed_records").with_payload().fetch)
    assert len(result.rows) == 1
    field = result.value(result.rows[0], "__laser_original_payload")
    assert field["kind"] == "binary"
    assert bytes(field["value"]) == value.encode()


@then("the keyed view is empty")
def keyed_empty(world):
    ready(world)
    assert not world.run(world.laser.query("keyed_records").fetch).rows


@then("operational SQL counts the two materialized records")
def sql_count(world):
    result = world.run(
        world.laser.query("whole_records")
        .raw_sql("sqlite", "SELECT count(*) AS n FROM whole_records")
        .fetch
    )
    assert len(result.rows) == 1
    assert result.value(result.rows[0], "n")["value"] == 2


@then("operational SQL refuses an internal relation and an unserved dialect")
def sql_refusals(world):
    for dialect, statement in [
        ("sqlite", "SELECT * FROM _ld_watermarks"),
        ("my_sql", "SELECT * FROM whole_records"),
    ]:
        with pytest.raises(ls.LaserError):
            world.run(world.laser.query("whole_records").raw_sql(dialect, statement).fetch)


REMOTE_TOPIC = "remote_records"


@given("a declared SQL backend")
def declared_sql_backend(world):
    capabilities = world.run(world.laser.capabilities)
    descriptor = next(
        descriptor
        for descriptor in capabilities.backends
        if descriptor.implementation["kind"] in ("postgres", "velodb")
        and descriptor.mode == "operational"
    )
    world.sql_backend = (
        {
            "resource_id": descriptor.resource_id,
            "generation": descriptor.observed_backend_generation,
        },
        "postgres" if descriptor.implementation["kind"] == "postgres" else "my_sql",
    )


@when("I materialize whole records on the declared SQL backend")
def materialize_remote(world):
    binding, _ = world.sql_backend
    topic = world.laser.topic(REMOTE_TOPIC)
    world.run(lambda: topic.ensure(1))
    world.materialization = world.run(topic.materialize().backend(binding).apply)


@when("I publish two arbitrary records to the remote materialized topic")
def publish_remote(world):
    for body in [b"not-json", bytes([0, 255, 1, 2])]:
        world.run(world.laser.topic(REMOTE_TOPIC).publish().payload(body).send)


@then("the remote materialized query returns both original bodies exactly once")
def remote_bodies(world):
    result = world.run(world.laser.query(REMOTE_TOPIC).with_payload().fetch)
    assert len(result.rows) == 2
    values = [result.value(row, "__laser_original_payload") for row in result.rows]
    assert all(value["kind"] == "binary" for value in values)
    actual = [bytes(value["value"]) for value in values]
    assert sorted(actual) == sorted([b"not-json", bytes([0, 255, 1, 2])])


@then("operational SQL in the backend dialect counts the two remote records")
def remote_sql_count(world):
    _, dialect = world.sql_backend
    result = world.run(
        world.laser.query(REMOTE_TOPIC)
        .raw_sql(dialect, f"SELECT count(*) AS n FROM {REMOTE_TOPIC}")
        .fetch
    )
    assert len(result.rows) == 1
    assert result.value(result.rows[0], "n")["value"] == 2


@then("operational SQL refuses internal and catalog relations of the declared backend")
def remote_sql_refusals(world):
    _, dialect = world.sql_backend
    for statement in [
        "SELECT * FROM _ld_watermarks",
        "SELECT * FROM ld_watermarks",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM whole_records",
    ]:
        with pytest.raises(ls.LaserError):
            world.run(world.laser.query(REMOTE_TOPIC).raw_sql(dialect, statement).fetch)
    with pytest.raises(ls.LaserError):
        world.run(
            world.laser.query(REMOTE_TOPIC)
            .raw_sql("sqlite", f"SELECT count(*) AS n FROM {REMOTE_TOPIC}")
            .fetch
        )
