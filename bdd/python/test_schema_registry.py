import json
from pathlib import Path

from pytest_bdd import parsers, scenarios, then, when

scenarios(str(Path(__file__).parent.parent / "scenarios/schema_registry.feature"))


@when(parsers.parse('the Avro writer schema "{name}" is registered in the stream'))
def register_in_the_stream(world, name):
    register(world, world.laser.default_stream, name)


@when(parsers.parse('the Avro writer schema "{name}" is registered in a second stream'))
def register_in_a_second_stream(world, name):
    stream = f"{world.laser.default_stream}-second"
    world.run(lambda: world.laser.stream(stream).ensure())
    register(world, stream, name)


@then("both registrations answer schema id 1")
def both_registrations_answer_one(world):
    assert [id for _, _, id in world.schema_registrations] == [1, 1]


@then("each stream reads schema 1 as its own writer schema")
def each_stream_reads_its_own(world):
    for stream, name, _ in world.schema_registrations:
        schemas = world.laser.with_default_stream(stream).schemas()
        info = world.run(lambda schemas=schemas: schemas.get(1))
        assert info is not None and info["schema"]["name"] == name, (stream, info)


@then("each stream lists only its own writer schema")
def each_stream_lists_its_own(world):
    for stream, name, _ in world.schema_registrations:
        schemas = world.laser.with_default_stream(stream).schemas()
        page = world.run(lambda schemas=schemas: schemas.list())
        assert [info["schema"]["name"] for info in page["items"]] == [name], (stream, page)


def register(world, stream, name):
    source = {
        "kind": "avro",
        "schema": json.dumps(
            {"type": "record", "name": name, "fields": [{"name": "id", "type": "long"}]}
        ),
    }
    schemas = world.laser.with_default_stream(stream).schemas()
    schema_id = world.run(lambda: schemas.register(source, name=name))
    world.schema_registrations.append((stream, name, schema_id))
