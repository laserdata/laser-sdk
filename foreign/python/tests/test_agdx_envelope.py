import laser_sdk as ls
import pytest

RECORD = "00000000000000000000000001"
CONVERSATION = "00000000000000000000000002"
CORRELATION = "00000000000000000000000003"
CHANNEL = "00000000000000000000000004"


def chunk(sequence, body, **refinements):
    return ls.chunk_envelope(
        CONVERSATION, "writer", CORRELATION, CHANNEL, sequence, body, **refinements
    )


def test_given_a_wire_code_when_mapped_then_should_round_trip_the_task_state():
    assert ls.TaskState.from_code(ls.TaskState.Working.code()) == ls.TaskState.Working
    assert ls.TaskState.from_code(42) == ls.TaskState.Unrecognized(42)
    assert str(ls.TaskState.InputRequired) == "input-required"
    assert ls.TaskState.Completed.is_terminal()
    assert not ls.TaskState.Unrecognized(42).is_terminal()


def test_given_builder_keywords_when_building_an_event_then_should_refine_the_envelope():
    envelope = ls.event_envelope(
        RECORD,
        CONVERSATION,
        "planner",
        b"payload",
        target="worker",
        correlation=CORRELATION,
        deadline_micros=7,
        terminal="stop",
        task_state=ls.TaskState.Working,
        tool="search",
        metadata={"tenant": "acme"},
        requiring=0b11,
    )
    assert envelope["kind"] == "event"
    assert bytes(envelope["body"]) == b"payload"
    assert envelope["last"] is True
    assert envelope["finish_reason"] == "stop"
    assert envelope["task_state"] == ls.TaskState.Working.code()
    assert envelope["tool"] == "search"
    assert envelope["metadata"] == {"tenant": "acme"}
    assert ls.unmet_requirements(envelope, 0b01) == 0b10


def test_given_a_status_envelope_when_built_then_should_carry_the_operation():
    envelope = ls.status_envelope(
        RECORD,
        CONVERSATION,
        "planner",
        "task",
        correlation=CORRELATION,
        task_state=ls.TaskState.Failed,
    )
    assert envelope["kind"] == "status"
    assert envelope["operation"] == "task"
    assert ls.TaskState.from_code(envelope["task_state"]) == ls.TaskState.Failed


def test_given_a_short_ed25519_signature_when_validated_then_should_raise():
    ls.validate_signature({"scheme": 9, "key_id": b"k", "bytes": b"s"})
    with pytest.raises(ls.ValidateError):
        ls.validate_signature({"scheme": 1, "key_id": b"k", "bytes": b"s"})


def test_given_chunk_envelopes_when_fed_then_should_emit_stream_event_dicts():
    assembler = ls.ChunkAssembler()
    assert assembler.feed(chunk(0, b"a", operation="chat")) == [
        {"kind": "body", "sequence": 0, "payload": b"a"}
    ]
    events = assembler.feed(chunk(1, b"", terminal="stop"))
    assert events == [
        {"kind": "finished", "finish_reason": "stop", "usage": None, "synthetic": False}
    ]
    assert assembler.finished
    assert assembler.abandon() is None


def test_given_a_sequence_gap_when_fed_then_should_synthesize_the_gap_terminal():
    assembler = ls.ChunkAssembler()
    assert assembler.feed(chunk(1, b"b")) == [
        {"kind": "finished", "finish_reason": "gap", "usage": None, "synthetic": True}
    ]


def test_given_a_log_position_when_packed_then_should_round_trip_its_twenty_bytes():
    position = ls.LogPosition(10, 20, 3, 42)
    packed = position.to_bytes()
    assert packed == b"".join(
        value.to_bytes(width, "big") for value, width in [(10, 4), (20, 4), (3, 4), (42, 8)]
    )
    assert ls.LogPosition.from_bytes(packed) == position
    assert (position.stream_id, position.topic_id, position.partition_id, position.offset) == (
        10,
        20,
        3,
        42,
    )
    with pytest.raises(ls.InvalidError):
        ls.LogPosition.from_bytes(b"short")


def test_given_a_presence_when_built_then_should_carry_the_inbox_and_validate():
    presence = ls.agent_presence("triage-1", inbox="triage.inbox")
    assert presence["inbox"] == "triage.inbox"
    assert "inbox" not in ls.agent_presence("triage-1")
    ls.validate_agent_presence(presence)
    with pytest.raises(ls.ValidateError):
        ls.validate_agent_presence(presence | {"inbox": "x" * 10_000})
