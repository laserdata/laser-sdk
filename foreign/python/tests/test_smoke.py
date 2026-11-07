import struct
import time

import iggy_test_server
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


def test_mutation_position_preserves_the_barrier_coordinates():
    position = ls.MutationPosition(3, 1, 42)
    assert position.topic_generation == 3
    assert position.partition == 1
    assert position.offset == 42


def test_provenance_round_trips_fields():
    provenance = ls.Provenance(agent="planner", idempotency_key="k1", input_tokens=10, cost_usd=0.5)
    assert provenance.agent == "planner"
    assert provenance.idempotency_key == "k1"
    assert provenance.usage.input_tokens == 10
    assert provenance.usage.output_tokens is None
    assert provenance.usage.cost_usd == 0.5
    assert ls.Provenance().usage is None
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


async def test_standalone_vector_memory_needs_no_connection():
    def embed(text):
        return [1.0, 0.0] if "auth" in text else [0.0, 1.0]

    conversation = ls.new_conversation_id()
    memory = ls.MemoryHandle.vector(embed)
    expected = await memory.remember("auth uses the read replica", conversation=conversation)
    await memory.remember("storage uses idempotency keys", conversation=conversation)

    hits = await memory.recall(
        semantic="auth is slow",
        limit=1,
        conversation=conversation,
    )

    assert hits[0].id == expected


async def test_file_store_persists_under_root(tmp_path):
    # The file-backed StateStore hex-encodes keys into file names, so any key is safe.
    store = ls.FileStore(str(tmp_path))
    assert await store.get("a/b") is None
    await store.set("a/b", "durable")
    assert await store.get("a/b") == b"durable"
    await store.delete("a/b")
    assert await store.get("a/b") is None


AVRO_SCHEMA = """{
    "type":"record","name":"Reading",
    "fields":[{"name":"host","type":"string"},{"name":"cpu","type":"long"}]
}"""


def test_compiled_avro_round_trips_a_value():
    # Compiling needs no connection: encode a value to an Avro datum client-side,
    # then validate and decode it back.
    schema = ls.CompiledSchema.compile({"kind": "avro", "schema": AVRO_SCHEMA}, id=7)
    datum = schema.encode_avro({"host": "node-7", "cpu": 42})
    assert isinstance(datum, bytes)
    assert schema.validate(datum)
    assert schema.decode(datum) == {"host": "node-7", "cpu": 42}


def test_compiled_avro_rejects_a_mismatched_value():
    # A body that does not match the schema fails client-side, before publishing.
    schema = ls.CompiledSchema.compile({"kind": "avro", "schema": AVRO_SCHEMA}, id=7)
    with pytest.raises(ls.CodecError):
        schema.encode_avro({"unrelated": True})


def test_compiled_json_schema_validates_a_value():
    # A JSON Schema validates an already-decoded value (the managed backend's check).
    source = {"kind": "json_schema", "schema": '{"type":"object","required":["n"]}'}
    schema = ls.CompiledSchema.compile(source)
    assert schema.validate_value({"n": 1})
    assert not schema.validate_value({})


def test_compiled_schema_rejects_unparseable_definition():
    with pytest.raises(ls.InvalidError):
        ls.CompiledSchema.compile({"kind": "avro", "schema": "{not valid"})


def make_intent(policy, voters, mandatory=None):
    return ls.Intent(
        conversation=ls.new_conversation_id(),
        proposer="proposer",
        body=b"transfer $100",
        eligible_voters=list(voters),
        mandatory_voters=list(mandatory) if mandatory else None,
        policy=policy,
        policy_version=1,
        deadline_at_micros=time.time_ns() // 1_000 + 1_000_000,
    )


def observed(votes):
    # Offline ballots are observed when they were cast.
    return [ls.ObservedVote(vote, vote.at_micros) for vote in votes]


def test_intent_digest_is_stable_over_the_same_body():
    one = make_intent(ls.IntentPolicy.any(), ["a"])
    two = make_intent(ls.IntentPolicy.any(), ["a"])
    assert one.digest == two.digest
    assert len(one.digest) == 64


def test_intent_all_policy_commits_when_every_voter_allows():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow"), ls.Vote.cast(intent, "b", "allow")]
    decision = ls.decide(intent, observed(votes), time.time_ns() // 1_000)
    assert decision.outcome == "committed"
    assert decision.authorizes(intent) is True


def test_intent_waits_while_the_outcome_is_still_reachable():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow")]
    assert ls.decide(intent, observed(votes), time.time_ns() // 1_000) is None


def test_intent_mandatory_voter_block_aborts_regardless_of_policy():
    intent = make_intent(ls.IntentPolicy.any(), ["safety", "llm"], mandatory=["safety"])
    votes = [
        ls.Vote.cast(intent, "safety", "block"),
        ls.Vote.cast(intent, "llm", "allow"),
    ]
    decision = ls.decide(intent, observed(votes), time.time_ns() // 1_000)
    assert decision.outcome == "aborted"
    assert "safety" in decision.reason


def test_intent_vote_for_a_different_intent_is_discarded():
    # A vote is bound to the exact intent it names: a vote cast against a
    # different intent (even one with the same body, voters, and policy)
    # never counts, so `decide` waits rather than folding it in.
    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    other_intent = make_intent(ls.IntentPolicy.any(), ["a"])
    misdirected_vote = ls.Vote.cast(other_intent, "a", "allow")
    assert ls.decide(intent, observed([misdirected_vote]), time.time_ns() // 1_000) is None
    decision = ls.decide(intent, observed([misdirected_vote]), intent.deadline_at_micros)
    assert decision.outcome == "aborted"


def test_intent_conflicting_repeat_vote_aborts_as_a_protocol_violation():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow"), ls.Vote.cast(intent, "a", "block")]
    decision = ls.decide(intent, observed(votes), time.time_ns() // 1_000)
    assert decision.outcome == "aborted"
    assert "conflicting" in decision.reason


def test_intent_invalid_vote_choice_raises_invalid_error():
    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    with pytest.raises(ls.InvalidError):
        ls.Vote.cast(intent, "a", "maybe")


def test_intent_invalid_configuration_and_outsider_fail_closed():
    with pytest.raises(ls.InvalidError, match="at least one eligible"):
        make_intent(ls.IntentPolicy.any(), [])
    with pytest.raises(ls.InvalidError, match="threshold"):
        make_intent(ls.IntentPolicy.at_least(0), ["a"])

    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    with pytest.raises(ls.InvalidError, match="not eligible"):
        ls.Vote.cast(intent, "outsider", "allow")


def test_intent_mandatory_allow_is_required_before_commit():
    intent = make_intent(ls.IntentPolicy.any(), ["safety", "llm"], mandatory=["safety"])
    vote = ls.Vote.cast(intent, "llm", "allow")
    assert ls.decide(intent, observed([vote]), time.time_ns() // 1_000) is None


# The JSON every client writes for one intent, pinned so Rust, Python, and
# TypeScript rebuild the same record and digest.
RUST_INTENT_JSON = {
    "intent_id": "01JC0000000000000000000000",
    "conversation": "01JC0000000000000000000001",
    "proposer": "proposer",
    "body": list(b"transfer $100"),
    "digest": "93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a",
    "eligible_voters": ["a", "b"],
    "mandatory_voters": ["b"],
    "policy": {"at_least": 1},
    "policy_version": 1,
    "deadline_at_micros": 2_000_000_000_000_000,
    "at_micros": 1_700_000_000_000_000,
}


def test_given_a_known_body_when_digested_then_should_match_the_pinned_digest():
    assert make_intent(ls.IntentPolicy.any(), ["a"]).digest == RUST_INTENT_JSON["digest"]


def test_given_the_shared_intent_json_when_rebuilt_then_should_validate_and_round_trip():
    intent = ls.Intent.__laser_from_json__(RUST_INTENT_JSON)
    assert intent.intent_id == RUST_INTENT_JSON["intent_id"]
    assert intent.__laser_json__() == RUST_INTENT_JSON


def test_given_a_committed_decision_without_votes_when_authorizing_then_should_raise():
    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    forged = ls.Decision.__laser_from_json__(
        {
            "intent_id": intent.intent_id,
            "intent_digest": intent.digest,
            "policy_version": 1,
            "outcome": "committed",
            "reason": "quorum met",
            "votes_considered": [],
            "at_micros": intent.at_micros,
        }
    )
    with pytest.raises(ls.IntentError) as raised:
        forged.authorizes(intent)
    assert raised.value.kind == "decision_quorum_not_met"


def make_dead_letter_capsule(*, attempts=1, reason=1, detail=None, payload=b"poison"):
    # `source` (an AGDX `LogPosition`) is not a dict: it rides as 20
    # big-endian packed bytes (stream_id: u32, topic_id: u32, partition_id:
    # u32, offset: u64), the same layout `Laser.redrive_dead_letter` expects.
    source = struct.pack(">IIIQ", 0, 0, 0, 5)
    return {
        "source": source,
        "reason": reason,
        "attempts": attempts,
        "detail": detail,
        "payload": payload,
    }


def test_crash_context_with_a_dead_letter_capsule_renders_its_detail():
    capsule = make_dead_letter_capsule(attempts=3, detail="handler panicked")
    context = ls.CrashContext(journal=[], dead_letter=capsule, last_decision=None)
    summary = context.summarize()
    assert "dead letter: 3 attempt(s), reason RetryExhausted: handler panicked" in summary


def test_crash_context_dead_letter_without_detail_omits_the_colon_suffix():
    capsule = make_dead_letter_capsule(attempts=1, detail=None)
    context = ls.CrashContext(journal=[], dead_letter=capsule, last_decision=None)
    summary = context.summarize()
    assert "dead letter: 1 attempt(s), reason RetryExhausted\n" in summary


def test_crash_context_rejects_a_malformed_dead_letter_capsule():
    # A `source` that is not exactly 20 packed bytes (here, a dict) fails
    # clearly rather than silently misreading a different field.
    with pytest.raises(ls.CodecError):
        ls.CrashContext(
            journal=[],
            dead_letter={
                "source": {"stream_id": 0, "topic_id": 0, "partition_id": 0, "offset": 5},
                "reason": 1,
                "attempts": 1,
                "payload": b"poison",
            },
            last_decision=None,
        )


@pytest.mark.parametrize(
    "options",
    [
        {"publish_timeout_ms": 0},
        {"publish_retry_backoff_ms": 0},
        {"publish_timeout_ms": 2147483648},
    ],
)
async def test_given_invalid_publish_settings_when_connecting_then_should_reject_before_io(options):
    import laser_sdk

    with pytest.raises(laser_sdk.ConfigError):
        await laser_sdk.Laser.connect("iggy:iggy@127.0.0.1:1", **options)


def test_given_built_binaries_and_no_cargo_when_resolved_then_should_use_the_cargo_target_dir(
    tmp_path, monkeypatch
):
    built = tmp_path / "debug"
    built.mkdir()
    for name in iggy_test_server.TEST_SUPPORT_BINARIES:
        (built / name).write_text("")
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path))
    monkeypatch.setattr(iggy_test_server.shutil, "which", lambda _: None)
    iggy_test_server._test_support_binaries.cache_clear()
    try:
        binaries = iggy_test_server._test_support_binaries()
    finally:
        iggy_test_server._test_support_binaries.cache_clear()
    assert binaries == {name: str(built / name) for name in iggy_test_server.TEST_SUPPORT_BINARIES}


def test_given_no_cargo_and_no_built_binaries_when_resolved_then_should_name_the_build_command(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path))
    monkeypatch.setattr(iggy_test_server.shutil, "which", lambda _: None)
    iggy_test_server._test_support_binaries.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="cargo build -p laser-test-support --bins"):
            iggy_test_server._test_support_binaries()
    finally:
        iggy_test_server._test_support_binaries.cache_clear()


def test_given_a_backdated_ballot_observed_after_the_deadline_when_decided_then_should_not_count():
    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    vote = ls.Vote.cast(intent, "a", "allow")
    late = ls.ObservedVote(vote, intent.deadline_at_micros + 1)
    assert late.vote.at_micros < intent.deadline_at_micros
    decision = ls.decide(intent, [late], intent.deadline_at_micros + 1)
    assert decision.outcome == "aborted"
    assert decision.votes_considered == []
