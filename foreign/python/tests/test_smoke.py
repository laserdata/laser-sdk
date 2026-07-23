import struct
import time

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


async def test_standalone_vector_memory_needs_no_connection():
    def embed(text):
        return [1.0, 0.0] if "checkout" in text else [0.0, 1.0]

    conversation = ls.new_conversation_id()
    memory = ls.Memory.vector(embed)
    expected = await memory.remember("checkout uses the read replica", conversation=conversation)
    await memory.remember("billing uses idempotency keys", conversation=conversation)

    hits = await memory.recall(
        semantic="checkout is slow",
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


def make_intent(policy, voters, mandatory=None):
    return ls.Intent(
        conversation=ls.new_conversation_id(),
        proposer="proposer",
        body=b"transfer $100",
        eligible_voters=list(voters),
        mandatory_voters=list(mandatory) if mandatory else None,
        policy=policy,
        policy_version=1,
        deadline_micros=time.time_ns() // 1_000 + 1_000_000,
    )


def test_intent_digest_is_stable_over_the_same_body():
    one = make_intent(ls.IntentPolicy.any(), ["a"])
    two = make_intent(ls.IntentPolicy.any(), ["a"])
    assert one.digest == two.digest
    assert len(one.digest) == 64


def test_intent_all_policy_commits_when_every_voter_allows():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow"), ls.Vote.cast(intent, "b", "allow")]
    decision = ls.decide(intent, votes, time.time_ns() // 1_000)
    assert decision.outcome == "committed"
    assert decision.authorizes(intent) is True


def test_intent_waits_while_the_outcome_is_still_reachable():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow")]
    assert ls.decide(intent, votes, time.time_ns() // 1_000) is None


def test_intent_mandatory_voter_block_aborts_regardless_of_policy():
    intent = make_intent(ls.IntentPolicy.any(), ["safety", "llm"], mandatory=["safety"])
    votes = [
        ls.Vote.cast(intent, "safety", "block"),
        ls.Vote.cast(intent, "llm", "allow"),
    ]
    decision = ls.decide(intent, votes, time.time_ns() // 1_000)
    assert decision.outcome == "aborted"
    assert "safety" in decision.reason


def test_intent_vote_for_a_different_intent_is_discarded():
    # A vote is bound to the exact intent it names: a vote cast against a
    # different intent (even one with the same body, voters, and policy)
    # never counts, so `decide` waits rather than folding it in.
    intent = make_intent(ls.IntentPolicy.any(), ["a"])
    other_intent = make_intent(ls.IntentPolicy.any(), ["a"])
    misdirected_vote = ls.Vote.cast(other_intent, "a", "allow")
    assert ls.decide(intent, [misdirected_vote], time.time_ns() // 1_000) is None
    decision = ls.decide(intent, [misdirected_vote], intent.deadline_micros)
    assert decision.outcome == "aborted"


def test_intent_conflicting_repeat_vote_aborts_as_a_protocol_violation():
    intent = make_intent(ls.IntentPolicy.all(), ["a", "b"])
    votes = [ls.Vote.cast(intent, "a", "allow"), ls.Vote.cast(intent, "a", "block")]
    decision = ls.decide(intent, votes, time.time_ns() // 1_000)
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
    assert ls.decide(intent, [vote], time.time_ns() // 1_000) is None


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
