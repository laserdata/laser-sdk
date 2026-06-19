import laser_sdk as ls
import pytest


def test_exception_hierarchy():
    for name in (
        "QueryError",
        "KvError",
        "ForkError",
        "UnsupportedError",
        "InvalidError",
        "CodecError",
        "ProtocolError",
        "TimeoutError",
        "ConfigError",
        "TransportError",
    ):
        assert issubclass(getattr(ls, name), ls.LaserError)


def test_conversation_ids():
    assert ls.new_conversation_id() != ls.new_conversation_id()
    assert ls.derive_conversation_id("seed") == ls.derive_conversation_id("seed")


def test_provenance_round_trips_fields():
    provenance = ls.Provenance(agent="planner", idempotency_key="k1", input_tokens=10, cost_usd=0.5)
    assert provenance.agent == "planner"
    assert provenance.idempotency_key == "k1"
    assert provenance.input_tokens == 10
    assert provenance.cost_usd == 0.5
    assert provenance.conversation_id  # a fresh ULID


def test_invalid_agent_id_raises_invalid_error():
    # An agent id with a control character is rejected client-side, before any
    # round-trip, and carries the classifier attributes.
    with pytest.raises(ls.InvalidError) as caught:
        ls.Provenance(agent="bad\x00id")
    assert isinstance(caught.value, ls.LaserError)
    assert caught.value.unsupported is False
    assert isinstance(caught.value.code, str)


async def test_in_memory_store_get_set_delete():
    # The in-process StateStore needs no connection: get / set / delete over a map.
    store = ls.InMemoryStore()
    assert await store.get("missing") is None
    await store.set("greeting", "hello")
    assert await store.get("greeting") == b"hello"
    await store.set("greeting", b"\x00\x01\x02")
    assert await store.get("greeting") == b"\x00\x01\x02"
    await store.delete("greeting")
    assert await store.get("greeting") is None


async def test_file_store_persists_under_root(tmp_path):
    # The file-backed StateStore hex-encodes keys into file names, so any key is safe.
    store = ls.FileStore(str(tmp_path))
    assert await store.get("a/b") is None
    await store.set("a/b", "durable")
    assert await store.get("a/b") == b"durable"
    await store.delete("a/b")
    assert await store.get("a/b") is None


AVRO_SCHEMA = """{
    "type":"record","name":"Order",
    "fields":[{"name":"customer","type":"string"},{"name":"amount","type":"long"}]
}"""


def test_compiled_avro_round_trips_a_value():
    # Compiling needs no connection: encode a value to an Avro datum client-side,
    # then validate and decode it back.
    schema = ls.CompiledSchema.compile({"kind": "avro", "schema": AVRO_SCHEMA}, id=7)
    datum = schema.encode_avro({"customer": "alice", "amount": 42})
    assert isinstance(datum, bytes)
    assert schema.validate(datum)
    assert schema.decode(datum) == {"customer": "alice", "amount": 42}


def test_compiled_avro_rejects_a_mismatched_value():
    # A body that does not match the schema fails client-side, before publishing.
    schema = ls.CompiledSchema.compile({"kind": "avro", "schema": AVRO_SCHEMA}, id=7)
    with pytest.raises(ls.CodecError):
        schema.encode_avro({"unrelated": True})


def test_compiled_json_schema_validates_a_value():
    # A JSON Schema validates an already-decoded value (the managed-plane check).
    source = {"kind": "json_schema", "schema": '{"type":"object","required":["n"]}'}
    schema = ls.CompiledSchema.compile(source)
    assert schema.validate_value({"n": 1})
    assert not schema.validate_value({})


def test_compiled_schema_rejects_unparseable_definition():
    with pytest.raises(ls.InvalidError):
        ls.CompiledSchema.compile({"kind": "avro", "schema": "{not valid"})
