import inspect
import json
from pathlib import Path

import laser_sdk as ls
import pytest

AVRO_BLOB = json.dumps(
    {
        "type": "record",
        "name": "Blob",
        "fields": [{"name": "blob", "type": "bytes"}, {"name": "n", "type": "long"}],
    }
)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("CONTEXT_READ_WINDOW", 10_000),
        ("OPS_STREAM_DEFAULT", "_agdx"),
        ("DEFAULT_MAX_RECORDS", 512),
        ("DEFAULT_MAX_BYTES", 1024 * 1024),
        ("DEFAULT_LINGER_MS", 5),
        ("MIN_LINGER_MS", 1),
        ("DEFAULT_KEY_NAMESPACE", "agent.keys"),
        ("DEFAULT_ATTEMPT_TIMEOUT_MS", 10_000),
        ("DEFAULT_SNAPSHOT_NAMESPACE", "agent.snapshots"),
        ("DEFAULT_SNAPSHOT_TOPIC", "agent.snapshots"),
        ("DEFAULT_MEMORY_TOPIC_TTL_MS", 30 * 24 * 60 * 60 * 1000),
        ("POLICY_DECISION_OPERATION", "policy_decision"),
        ("A2A_PROTOCOL_VERSION", "1.0"),
        ("A2A_JSONRPC_BINDING", "JSONRPC"),
        ("DEFAULT_OUTCOME_WAIT_MS", 30_000),
        ("FINISH_REASON_ABANDONED", "abandoned"),
        ("FINISH_REASON_GAP", "gap"),
        ("DEFAULT_SESSION_MEMORY_NAMESPACE", "agent.session"),
        ("DEFAULT_SESSION_CONTEXT_TURNS", 50),
        ("DEFAULT_SESSION_CONTEXT_TOKENS", 4000),
        ("WORKFLOW_FENCE_NAMESPACE", "agdx.workflow.fence"),
        ("DEFAULT_CHUNK_FLUSH_BYTES", 512),
        ("DEFAULT_CHUNK_LINGER_MS", 20),
        ("MAX_CHUNK_BODY_BYTES", 64 * 1024),
    ],
)
def test_given_a_rust_default_when_importing_the_module_then_should_export_it(name, value):
    assert getattr(ls, name) == value


def test_given_the_session_defaults_when_importing_then_should_match_the_rust_values():
    assert ls.DEFAULT_SESSION_IDLE_TIMEOUT_MS == 300_000
    assert ls.DEFAULT_SESSION_HEARTBEAT_MS == 60_000
    assert ls.derive_session_id("agents", "ops", "incident") == ls.derive_session_id(
        "agents", "ops", "incident"
    )
    assert ls.derive_session_id("a", "bc", "d") != ls.derive_session_id("ab", "c", "d")


@pytest.mark.parametrize("name", ["cursor_paging", "cancellation", "execution_status"])
def test_given_capabilities_when_reading_query_execution_then_should_expose_the_rust_fields(name):
    assert getattr(ls.Capabilities.OPEN.query, name) is False
    assert inspect.isgetsetdescriptor(ls.Capabilities.hello)


def test_given_a_stream_when_finishing_then_should_take_the_token_usage():
    assert "usage" in inspect.signature(ls.AgdxStream.finish).parameters


@pytest.mark.parametrize("walk", ["QueryRows", "TypedQueryRows"])
def test_given_a_bounded_row_walk_when_iterating_then_should_be_a_lazy_async_iterator(walk):
    cls = getattr(ls, walk)
    for method in ("__aiter__", "__anext__", "next"):
        assert callable(getattr(cls, method))


def test_given_a_bytes_field_when_encoding_avro_then_should_write_avro_bytes():
    schema = ls.CompiledSchema.compile({"kind": "avro", "schema": AVRO_BLOB})
    assert schema.encode_avro({"blob": b"\x00\xff", "n": 7}) == b"\x04\x00\xff\x0e"


def test_given_integer_keys_and_bytes_when_validating_a_value_then_should_lower_like_serde():
    schema = ls.CompiledSchema.compile(
        {
            "kind": "json_schema",
            "schema": json.dumps(
                {
                    "type": "object",
                    "properties": {"7": {"type": "array", "items": {"type": "integer"}}},
                    "required": ["7"],
                }
            ),
        }
    )
    assert schema.validate_value({7: b"\x01\x02"})
    assert not schema.validate_value({8: b"\x01"})


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
def test_given_an_agdx_verb_when_sending_then_should_take_a_per_send_signing_key(verb):
    parameter = inspect.signature(getattr(ls.Agdx, verb)).parameters["signed_by"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None


@pytest.mark.parametrize(
    ("class_name", "method", "parameters"),
    [
        ("PublishRequest", "encode_with", ["body", "codec", "content_type"]),
        ("BatchPublishRequest", "add_encoded", ["body", "codec", "content_type"]),
        (
            "BatchPublishRequest",
            "add_encoded_with_projection",
            ["projection_ref", "body", "codec", "content_type"],
        ),
        ("BatchPublishRequest", "extend_encoded", ["items", "codec", "content_type"]),
        ("Kv", "get_as", ["key", "codec"]),
        ("KvSetRequest", "encode_with", ["value", "codec"]),
        ("KvCasFencedRequest", "encode_with", ["value", "codec"]),
        ("QueryRequest", "fetch_typed_with", ["codec"]),
        ("QueryRequest", "fetch_one_with", ["codec"]),
    ],
)
def test_given_a_user_codec_when_reaching_a_codec_api_then_should_take_it_like_rust(
    class_name, method, parameters
):
    signature = inspect.signature(getattr(getattr(ls, class_name), method))
    assert [name for name in signature.parameters if name not in ("self", "slf")] == parameters


def test_given_a_backend_descriptor_when_reading_then_should_keep_implementation_and_revision():
    fixture = Path(__file__).resolve().parents[3] / "wire" / "fixtures" / "capabilities.json"
    record = json.loads(fixture.read_text())["backends"][0]
    backend = ls.BackendDescriptor.from_dict(record)
    assert backend.implementation == record["implementation"]
    assert backend.runtime_configuration_revision == record["runtime_configuration_revision"]
    assert backend.to_dict() == record


def test_given_readiness_helpers_when_building_a_state_then_should_match_the_wire_readiness():
    ready = ls.backend_readiness_ready(7)
    assert ready == {"ready": True, "reasons": [], "observed_at_micros": 7}
    not_ready = ls.backend_readiness_not_ready("configuration_pending")
    assert not_ready == {
        "ready": False,
        "reasons": [{"code": "configuration_pending"}],
        "observed_at_micros": 0,
    }
    backend = ls.BackendDescriptor(
        "0000000000000000000000000A", "operational", "primary", {"kind": "k", "version": "1"}, 1, 1
    ).with_state("enabled", "ready", ready)
    assert backend.readiness == ready
    with pytest.raises(ls.InvalidError):
        ls.backend_readiness_not_ready("sleepy")
