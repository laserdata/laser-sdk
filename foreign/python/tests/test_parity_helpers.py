import pathlib
import time

import laser_sdk as ls
import pytest

FIXTURES = pathlib.Path(__file__).resolve().parents[3] / "wire" / "fixtures"
U64_MAX = (1 << 64) - 1
RECORD = "00000000000000000000000001"
CONVERSATION = "00000000000000000000000002"
CORRELATION = "00000000000000000000000003"


def command(payload=b'{"message": "hello", "_meta": {"future": 1}}'):
    return ls.command_from_message_send(RECORD, CONVERSATION, "planner", CORRELATION, payload)


def test_given_a_native_system_clock_when_read_then_should_return_epoch_microseconds():
    before = time.time_ns() // 1_000
    now = ls.SystemClock().now_micros()
    after = time.time_ns() // 1_000
    assert before <= now <= after


def test_given_a_test_clock_when_advanced_then_should_use_native_unsigned_time():
    clock = ls.TestClock(1_000)
    clock.advance(500)
    assert clock.now_micros() == 1_500
    clock.set(U64_MAX)
    clock.advance(1)
    assert clock.now_micros() == 0
    assert ls.TestClock().now_micros() == 0
    with pytest.raises(OverflowError):
        clock.set(-1)
    with pytest.raises(OverflowError):
        clock.advance(U64_MAX + 1)


def test_given_a_signed_card_when_changed_then_should_refuse_the_signature():
    key = ls.SigningKey(bytes(range(32)))
    card = {"name": "planner", "version": "0.5.4", "skills": []}
    signature = ls.sign_card_value(key, card)
    assert ls.verify_card(card, signature, key.verifying_key) is None
    card["signatures"] = [signature]
    assert ls.verify_card(card, signature, key.verifying_key) is None
    card["name"] = "different"
    with pytest.raises(ls.LaserError):
        ls.verify_card(card, signature, key.verifying_key)


def test_given_non_integer_card_numbers_when_signed_then_should_match_native_refusal():
    with pytest.raises(ls.LaserError):
        ls.sign_card_value(ls.SigningKey(bytes(range(32))), {"version": 1.5})


def test_given_signed_delegation_when_changed_then_should_refuse_the_claim():
    key = ls.SigningKey(bytes(range(32)))
    registry = ls.KeyRegistry()
    registry.enroll_record(ls.KeyRecord("enrolled-signer", key.verifying_key))
    envelope = command()
    envelope["metadata"] = {"on_behalf_of": "user-7"}
    envelope["signature"] = key.sign(envelope)
    assert ls.verify_delegation(registry, envelope) == ("enrolled-signer", "user-7")
    envelope["metadata"]["on_behalf_of"] = "user-8"
    with pytest.raises(ls.LaserError):
        ls.verify_delegation(registry, envelope)


def test_given_no_delegation_when_verified_then_should_return_none():
    key = ls.SigningKey(bytes(range(32)))
    registry = ls.KeyRegistry()
    registry.enroll_record(ls.KeyRecord("enrolled-signer", key.verifying_key))
    envelope = command()
    envelope["signature"] = key.sign(envelope)
    assert ls.verify_delegation(registry, envelope) is None


def test_given_the_golden_snapshot_when_reencoded_then_should_keep_exact_bytes():
    payload = FIXTURES.joinpath("fold_snapshot.bin").read_bytes()
    snapshot = ls.decode_snapshot(payload)
    assert ls.encode_snapshot(snapshot) == payload
    assert isinstance(snapshot["state"], bytes)


def test_given_snapshot_offsets_when_resumed_then_should_skip_folded_records():
    snapshot = {
        "conversation": CONVERSATION,
        "as_of": {0: 41, 1: U64_MAX},
        "state": b"\x00\xff",
    }
    assert ls.resume_offsets(snapshot) == {0: 42, 1: U64_MAX}
    assert ls.decode_snapshot(ls.encode_snapshot(snapshot)) == snapshot


def test_given_invalid_snapshot_bytes_when_decoded_then_should_raise_a_typed_error():
    with pytest.raises(ls.LaserError):
        ls.decode_snapshot(b"invalid snapshot")


def test_given_a2a_params_when_converted_then_should_preserve_exact_body_bytes():
    payload = b'{"message":  "hello", "_meta": {"future": 1}}'
    envelope = command(payload)
    assert bytes(envelope["body"]) == payload
    assert envelope["operation"] == "chat"
    task = ls.task_from_envelope("task-7", envelope)
    assert task["id"] == "task-7"
    assert task["status"]["state"] == "completed"
    assert task["artifacts"] == [{"text": payload.decode()}]


def test_given_mcp_params_when_converted_then_should_preserve_tool_and_body():
    payload = b'{"name": "search", "arguments": {"query": "auth"}}'
    envelope = ls.tool_call_from_request(
        RECORD, CONVERSATION, "planner", CORRELATION, "search", payload
    )
    assert bytes(envelope["body"]) == payload
    assert envelope["tool"] == "search"
    assert ls.tool_result_from_envelope(envelope) == {
        "content": [{"type": "text", "text": payload.decode()}]
    }


def test_given_error_and_empty_envelopes_when_rendered_then_should_map_terminal_views():
    error = FIXTURES.joinpath("agent_error.bin").read_bytes()
    assert ls.task_from_envelope("task-7", error)["status"]["state"] == "failed"
    assert ls.tool_result_from_envelope(error)["isError"] is True
    empty = command(b"")
    assert ls.task_from_envelope("task-7", empty).get("artifacts", []) == []
    assert ls.tool_result_from_envelope(empty) == {"content": []}


def test_given_invalid_envelope_ids_when_converted_then_should_refuse_before_publish():
    with pytest.raises(ls.InvalidError):
        ls.command_from_message_send("bad-record", CONVERSATION, "planner", CORRELATION, b"{}")


async def test_given_duplicate_ranked_candidates_when_fused_then_should_preserve_signals():
    memory = ls.Memory.vector(lambda _: [1.0])
    await memory.remember("first")
    await memory.remember("second")
    items = await memory.recall(semantic="query", limit=2)
    original_signals = [list(item.signals) for item in items]
    fused = ls.fuse_reciprocal_rank([items, [items[1]]], 2)
    assert [item.id for item in fused] == [items[1].id, items[0].id]
    assert len(fused[0].signals) == len(items[1].signals) * 2
    assert [list(item.signals) for item in items] == original_signals
    assert len(ls.fuse_reciprocal_rank([items], 1)) == 1
    assert ls.fuse_reciprocal_rank([items], 0) == []


class BlobStore:
    def __init__(self):
        self.values = {}
        self.put_calls = 0

    async def put(self, payload):
        self.put_calls += 1
        reference = f"blob:{self.put_calls}"
        self.values[reference] = bytes(payload)
        return reference

    async def get(self, reference):
        return self.values[reference]


async def test_given_blob_payloads_when_threshold_is_reached_then_should_claim_check():
    store = BlobStore()
    assert await ls.check_in(store, 6, b"small") == (b"small", None)
    assert store.put_calls == 0
    capsule, content_type = await ls.check_in(store, 5, b"small")
    assert content_type == "ref"
    assert store.put_calls == 1
    assert await ls.resolve_body(store, capsule) == b"small"


async def test_given_corrupt_blob_bytes_when_resolved_then_should_refuse_unverified_data():
    store = BlobStore()
    capsule, _ = await ls.check_in(store, 1, b"small")
    store.values["blob:1"] = b"wrong"
    with pytest.raises(ls.LaserError):
        await ls.resolve_body(store, capsule)
