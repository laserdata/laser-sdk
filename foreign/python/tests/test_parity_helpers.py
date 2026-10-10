import json
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
    with pytest.raises(ls.InvalidError):
        clock.set(-1)
    with pytest.raises(ls.InvalidError):
        clock.advance(U64_MAX + 1)
    with pytest.raises(ls.InvalidError):
        ls.TestClock(-5)


def test_given_a_signed_card_when_changed_then_should_refuse_the_signature():
    key = ls.SigningKey(bytes(range(32)))
    card = {"name": "planner", "version": "0.6.0", "skills": []}
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
        "stream": "agents",
        "stream_id": 0,
        "stream_created_at_micros": 100,
        "conversation": CONVERSATION,
        "fold": "planner",
        "as_of": [(2, 20, 0, 41), (2, 20, 1, U64_MAX)],
        "state": b"\x00\xff",
    }
    assert ls.resume_offsets(snapshot) == [(2, 20, 0, 42), (2, 20, 1, U64_MAX)]
    assert ls.decode_snapshot(ls.encode_snapshot(snapshot)) == snapshot


def test_given_invalid_snapshot_bytes_when_decoded_then_should_raise_a_typed_error():
    with pytest.raises(ls.LaserError):
        ls.decode_snapshot(b"invalid snapshot")


@pytest.mark.parametrize(
    ("name", "decode", "encode"),
    [
        ("context_manifest.bin", ls.decode_context_manifest, ls.encode_context_manifest),
        ("context_compaction.bin", ls.decode_context_compaction, ls.encode_context_compaction),
        ("context_retrieval.bin", ls.decode_context_retrieval, ls.encode_context_retrieval),
        ("state_delta.bin", ls.decode_state_delta, ls.encode_state_delta),
        ("state_snapshot.bin", ls.decode_state_snapshot, ls.encode_state_snapshot),
    ],
)
def test_given_context_or_state_fixture_when_reencoded_then_should_keep_exact_bytes(
    name, decode, encode
):
    payload = FIXTURES.joinpath(name).read_bytes()
    assert encode(decode(payload)) == payload


def test_given_oversize_context_or_state_when_encoded_then_should_reject():
    manifest = ls.decode_context_manifest(FIXTURES.joinpath("context_manifest.bin").read_bytes())
    manifest["fragments"] = [manifest["fragments"][0]] * 1_025
    with pytest.raises(ls.InvalidError):
        ls.encode_context_manifest(manifest)

    delta = ls.decode_state_delta(FIXTURES.joinpath("state_delta.bin").read_bytes())
    delta["patch"] = [{"op": "remove", "path": "/x"}] * 257
    with pytest.raises(ls.InvalidError):
        ls.encode_state_delta(delta)

    snapshot = ls.decode_state_snapshot(FIXTURES.joinpath("state_snapshot.bin").read_bytes())
    snapshot["document"] = {"large": "x" * (8 * 1024 * 1024)}
    with pytest.raises(ls.InvalidError):
        ls.encode_state_snapshot(snapshot)


def test_given_invalid_context_digest_or_patch_op_when_encoded_then_should_reject():
    manifest = ls.decode_context_manifest(FIXTURES.joinpath("context_manifest.bin").read_bytes())
    memory = next(fragment["Memory"] for fragment in manifest["fragments"] if "Memory" in fragment)
    memory["digest"] = b"x" * 31
    with pytest.raises(ls.LaserError):
        ls.encode_context_manifest(manifest)
    delta = ls.decode_state_delta(FIXTURES.joinpath("state_delta.bin").read_bytes())
    delta["patch"] = [{"op": "unknown", "path": "/a"}]
    with pytest.raises(ls.LaserError):
        ls.encode_state_delta(delta)


def test_given_shared_json_patch_cases_when_applied_then_should_match_rust_and_typescript():
    cases = json.loads(FIXTURES.joinpath("json_patch/cases.json").read_text())
    for case in cases:
        if case.get("error"):
            with pytest.raises(ls.InvalidError):
                ls.apply_json_patch(case["document"], case["patch"])
        else:
            assert ls.apply_json_patch(case["document"], case["patch"]) == case["result"]


def test_given_state_patch_limits_when_applied_then_should_reject():
    with pytest.raises(ls.InvalidError):
        ls.apply_json_patch({"a": 1}, [{"op": "remove", "path": "/a"}] * 257)
    with pytest.raises(ls.InvalidError):
        ls.apply_json_patch({"large": "x" * (8 * 1024 * 1024)}, [])


def test_given_inexact_state_integer_when_encoded_or_applied_then_should_reject():
    large = 9_007_199_254_740_992
    with pytest.raises(ls.InvalidError):
        ls.encode_state_snapshot({"base_revision": 0, "document": {"large": large}})
    with pytest.raises(ls.InvalidError):
        ls.encode_state_delta(
            {
                "base_revision": 0,
                "patch": [{"op": "add", "path": "/large", "value": large}],
                "op_id": "patch-1",
            }
        )
    with pytest.raises(ls.InvalidError):
        ls.apply_json_patch({"large": large}, [])


@pytest.mark.parametrize(
    ("name", "decode", "encode"),
    [
        ("session_get.bin", ls.decode_session_get, ls.encode_session_get),
        ("session_list.bin", ls.decode_session_list, ls.encode_session_list),
        ("session_events.bin", ls.decode_session_events, ls.encode_session_events),
        ("session_state.bin", ls.decode_session_state, ls.encode_session_state),
        ("session_links.bin", ls.decode_session_links, ls.encode_session_links),
        ("session_sources.bin", ls.decode_session_sources, ls.encode_session_sources),
        ("session_changes.bin", ls.decode_session_changes, ls.encode_session_changes),
    ],
)
def test_given_session_read_request_when_reencoded_then_should_match_rust_and_typescript(
    name, decode, encode
):
    payload = FIXTURES.joinpath(name).read_bytes()
    request = decode(payload)
    assert request["stream"] == "agents"
    assert encode(request) == payload
    del request["stream"]
    with pytest.raises(ls.LaserError):
        encode(request)


@pytest.mark.parametrize(
    "name", ["info", "page", "events", "state", "links", "sources", "changes", "error"]
)
def test_given_session_read_reply_when_reencoded_then_should_match_rust_and_typescript(name):
    payload = FIXTURES.joinpath(f"session_reply_{name}.bin").read_bytes()
    reply = ls.decode_session_reply(payload)
    assert ls.encode_session_reply(reply) == payload
    if name == "error":
        assert reply == {"Err": {"NotRegistered": "agents"}}
    if name == "state":
        assert reply["Ok"]["State"]["document"] == {"tasks": ["triage"]}


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
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
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


def test_given_an_enrolled_signer_when_verifying_at_a_time_then_should_return_the_principal():
    key = ls.SigningKey(bytes(range(32)))
    registry = ls.KeyRegistry()
    registry.enroll_operator("operator-9", key.verifying_key)
    envelope = command()
    envelope["signature"] = key.sign(envelope)
    verified = registry.verify_at(envelope, 1)
    assert isinstance(verified, ls.VerifiedPrincipal)
    assert (verified.principal, verified.kind) == ("operator-9", "operator")


def test_given_the_clock_base_when_subclassed_then_should_share_the_clock_type():
    class FixedClock(ls.Clock):
        def now_micros(self):
            return 42

    assert isinstance(ls.SystemClock(), ls.Clock)
    assert isinstance(ls.TestClock(), ls.Clock)
    assert FixedClock().now_micros() == 42
    with pytest.raises(NotImplementedError):
        ls.Clock().now_micros()


@pytest.mark.parametrize(
    ("name", "decode", "encode"),
    [
        ("agent_session_start.bin", ls.decode_session_start, ls.encode_session_start),
        (
            "agent_session_transition.bin",
            ls.decode_session_transition,
            ls.encode_session_transition,
        ),
        ("agent_session_end.bin", ls.decode_session_end, ls.encode_session_end),
    ],
)
def test_given_a_session_lifecycle_fixture_when_round_tripped_then_should_match_rust(
    name, decode, encode
):
    payload = FIXTURES.joinpath(name).read_bytes()
    assert encode(decode(payload)) == payload
