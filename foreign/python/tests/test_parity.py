import ast
import asyncio
import pathlib
import time

import laser_sdk as ls
import pytest

FIXTURES = pathlib.Path(__file__).resolve().parents[3] / "wire" / "fixtures"


def bag_of_words(vocabulary):
    def embed(text):
        words = set(text.lower().split())
        return [1.0 if term in words else 0.0 for term in vocabulary]

    return embed


def test_no_stream_errors_are_config_errors():
    assert issubclass(ls.NoStreamError, ls.ConfigError)
    assert issubclass(ls.NoRespondTopicError, ls.ConfigError)


def test_context_read_window_matches_rust():
    assert ls.CONTEXT_READ_WINDOW == 10_000


def test_query_filter_builds_a_predicate_tree():
    degraded = ls.QueryFilter.pred("status", "eq", "degraded")
    hot = ls.QueryFilter.pred("cpu", "gte", 90)
    tree = ls.QueryFilter.any([ls.QueryFilter.all([degraded, hot]), ls.QueryFilter.negate(hot)])
    assert tree.to_dict()


def test_query_filter_rejects_an_unknown_comparison():
    with pytest.raises(ls.InvalidError):
        ls.QueryFilter.pred("cpu", "almost", 90)


def test_context_policies_compose():
    assert ls.Chain([ls.LastN(20), ls.TokenBudget(4_000), ls.RoleFilter(["planner"])])
    assert ls.TokenBudget(100, estimator=lambda message: len(message.payload))
    with pytest.raises(ls.InvalidError):
        ls.Chain([object()])


async def test_connect_env_without_a_connection_string_is_a_config_error(monkeypatch):
    monkeypatch.delenv("LASER_CONNECTION_STRING", raising=False)
    with pytest.raises(ls.ConfigError):
        await ls.Laser.connect_env()


async def test_vector_memory_dedup_stores_one_item_per_body():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage"]))
    first = await memory.remember("auth is slow", agent="planner", dedup=True)
    second = await memory.remember("auth is slow", agent="planner", dedup=True)
    assert first == second


async def test_vector_memory_remembers_with_a_kind():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    await memory.remember("auth runbook", conversation=conversation, kind="procedure")
    items = await memory.recall(conversation=conversation)
    assert [item.kind for item in items] == ["procedure"]
    with pytest.raises(ls.InvalidError):
        await memory.remember("auth runbook", kind="note")


async def test_vector_memory_context_renders_a_prompt_block():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    await memory.remember("auth uses the read replica", conversation=conversation)
    block = await memory.context(conversation, token_budget=1_000)
    assert "auth uses the read replica" in block


async def test_vector_memory_consolidate_reports_the_pass():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    for body in ("auth one", "auth two", "auth three"):
        await memory.remember(body, conversation=conversation)
    report = await memory.consolidate(1, conversation=conversation)
    assert isinstance(report, ls.ConsolidationReport)
    assert report.pruned == 2
    assert len(await memory.recall(conversation=conversation)) == 1


async def test_vector_memory_refuses_the_named_item_altitude():
    memory = ls.Memory.vector(bag_of_words(["auth"]))
    assert memory.backend_name == "vector"
    with pytest.raises(ls.UnsupportedError):
        await memory.set("plan", b"rotate")


async def test_reranker_reorders_only_semantic_recall():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage", "metrics"]))
    conversation = ls.new_conversation_id()
    for body in ("auth is slow", "auth token rotated", "storage is full"):
        await memory.remember(body, conversation=conversation)
    calls = []

    def reverse(query, items):
        calls.append(query)
        return list(reversed(items))

    reranked = memory.reranker(reverse)
    plain = await memory.recall(conversation=conversation, semantic="auth")
    flipped = await reranked.recall(conversation=conversation, semantic="auth")
    assert [item.text for item in flipped] == [item.text for item in reversed(plain)]
    assert calls == ["auth"]
    await reranked.recall(conversation=conversation)
    assert calls == ["auth"]


async def test_async_reranker_may_drop_candidates():
    memory = ls.Memory.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    for body in ("auth one", "auth two"):
        await memory.remember(body, conversation=conversation)

    async def keep_first(query, items):
        return items[:1]

    items = await memory.reranker(keep_first).recall(conversation=conversation, semantic="auth")
    assert len(items) == 1


def test_intent_validate_accepts_a_built_intent():
    intent = ls.Intent(
        ls.new_conversation_id(),
        "proposer",
        b"rotate the storage credentials",
        ["safety"],
        ls.IntentPolicy.all(),
        1,
        time.time_ns() // 1_000 + 60_000_000,
    )
    assert intent.validate() is None


def test_signing_key_signs_an_envelope_with_and_without_context():
    key = ls.SigningKey(bytes(range(32)))
    envelope = FIXTURES.joinpath("agent_command.bin").read_bytes()
    plain = key.sign(envelope)
    bound = key.sign_with_context(envelope, content_type="cbor", agent_version=1)
    assert plain["scheme"] == 1
    assert bytes(plain["key_id"]) == key.key_id
    assert len(bytes(plain["bytes"])) == 64
    assert bytes(bound["bytes"]) != bytes(plain["bytes"])
    with pytest.raises(ls.InvalidError):
        key.sign_with_context(envelope, content_type="nope")


def test_key_registry_refuses_an_unenrolled_signer():
    signer = ls.SigningKey(bytes(range(32)))
    registry = ls.KeyRegistry()
    registry.enroll_record(
        ls.KeyRecord("agent-7", ls.SigningKey(bytes(reversed(range(32)))).verifying_key)
    )
    envelope = FIXTURES.joinpath("agent_command_signed.bin").read_bytes()
    assert signer.sign(envelope)
    with pytest.raises(ls.LaserError):
        registry.verify(envelope)
    with pytest.raises(ls.LaserError):
        registry.verify_at(envelope, 1)
    with pytest.raises(ls.LaserError):
        registry.verify_observed_at(envelope, 1, content_type="cbor")


def test_op_versions_construct_for_injected_capabilities():
    versions = ls.OpVersions(query=2, graph=1, features=3)
    assert (versions.query, versions.control, versions.graph, versions.features) == (2, 1, 1, 3)


STUB = pathlib.Path(__file__).resolve().parents[1] / "laser_sdk.pyi"
SEND_KEYWORDS = {
    "cause",
    "cause_at",
    "deadline_micros",
    "idempotency_key",
    "metadata",
    "tool",
    "usage",
    "claim_check",
}


def stub_keywords(class_name, method):
    tree = ast.parse(STUB.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == method:
                    return {arg.arg for arg in item.args.kwonlyargs}
    raise AssertionError(f"{class_name}.{method} is missing from the stub")


def text(value):
    raw = value.encode()
    return bytes([0x60 + len(raw)]) + raw


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
def test_given_an_agdx_verb_when_reading_the_stub_then_should_list_send_options(verb):
    assert stub_keywords("Agdx", verb) >= SEND_KEYWORDS


@pytest.mark.parametrize("method", ["add_json", "add_msgpack", "add_payload", "add_raw_bytes"])
def test_given_a_batch_add_when_reading_the_stub_then_should_take_a_projection_ref(method):
    assert "projection_ref" in stub_keywords("BatchPublishRequest", method)


def test_given_a_batch_when_reading_add_record_then_should_list_record_options():
    assert stub_keywords("BatchPublishRequest", "add_record") >= {
        "content_type",
        "index",
        "headers",
        "projection_ref",
        "schema_id",
        "inline_payload",
    }


@pytest.mark.parametrize(
    ("class_name", "method", "keyword"),
    [
        ("Laser", "contract", "policy"),
        ("Laser", "contract_report", "policy"),
        ("Laser", "scatter", "policy"),
        ("Laser", "scatter_report", "policy"),
        ("AgentCtx", "fan_out", "route_policy"),
        ("AgentScope", "contract", "policy"),
        ("Workflow", "step", "policy"),
    ],
)
def test_given_a_capability_route_when_reading_the_stub_then_should_take_a_route_policy(
    class_name, method, keyword
):
    assert keyword in stub_keywords(class_name, method)


def test_given_a_checkpoint_from_json_when_reading_topic_offsets_then_should_map_partitions():
    checkpoint = ls.Checkpoint.from_json('{"per_topic": {"readings": {"0": 7, "2": 41}}}')
    assert checkpoint.topic_offsets("readings") == {0: 7, 2: 41}
    assert checkpoint.topic_offsets("incidents") is None


def test_given_a_memory_when_reading_consolidate_then_should_take_a_summarizer():
    for class_name in ("Memory", "ScopedMemory"):
        assert stub_keywords(class_name, "consolidate") >= {"summarizer", "prune_summarized"}


def test_given_an_empty_memory_when_consolidating_with_a_summarizer_then_should_report_nothing():
    async def consolidate():
        memory = ls.Memory.vector(bag_of_words(["cpu"]))
        return await memory.consolidate(
            4, summarizer=lambda bodies: b"".join(bodies), prune_summarized=True
        )

    assert asyncio.run(consolidate()).summarized == 0


def test_given_the_same_body_when_deriving_content_ids_then_should_depend_on_kind_and_owner():
    body = b'{"host": "node-7", "cpu": 82}'
    first = ls.Memory.content_id("fact", body, stream="metrics", agent="monitor")
    assert first == ls.Memory.content_id("fact", body, stream="metrics", agent="monitor")
    assert first != ls.Memory.content_id("message", body, stream="metrics", agent="monitor")
    assert first != ls.Memory.content_id("fact", body, stream="metrics")


def test_given_a_memory_kind_when_reading_its_class_then_should_return_the_class_word():
    assert ls.Memory.kind_class("message") == "episodic"
    assert ls.Memory.kind_class("procedure") == "procedural"
    with pytest.raises(ls.LaserError):
        ls.Memory.kind_class("nope")


def test_given_decoded_evidence_when_encoding_then_should_round_trip():
    fields = {
        "decision_id": "01J0000000000000000000",
        "decision": "allow",
        "mode": "enforce",
        "kind": "command",
        "stream": "metrics",
        "topic": "readings",
        "receipt_digest": "abc123",
        "outcome": "delivered",
    }
    payload = (
        bytes([0xA0 + len(fields) + 1])
        + b"".join(text(key) + text(value) for key, value in fields.items())
        + text("at_micros")
        + bytes([7])
    )
    evidence = ls.PolicyEvidence.decode(payload)
    encoded = evidence.encode()
    assert isinstance(encoded, bytes)
    assert ls.PolicyEvidence.decode(encoded).decision_id == fields["decision_id"]
    assert ls.PolicyEvidence.decode(encoded).encode() == encoded


def test_given_list_projections_when_reading_the_stub_then_should_take_topics_and_search():
    assert stub_keywords("Laser", "list_projections") >= {"topics", "search"}


def test_given_a_card_without_fields_when_checking_it_then_should_raise_invalid():
    with pytest.raises(ls.InvalidError):
        ls.AgentRegistry.card_serves({}, "triage")
    with pytest.raises(ls.InvalidError):
        ls.AgentRegistry.card_available_for({}, "triage")
    with pytest.raises(ls.InvalidError):
        ls.AgentRegistry.card_is_fresh({}, 1)


@pytest.mark.parametrize(
    "kind",
    ["instruction", "response", "model.response", "tool.call", "tool.result", "human.input"],
)
def test_given_a_turn_kind_when_mapping_to_its_topic_then_should_map_back(kind):
    assert ls.Sessions.turn_kind(ls.Sessions.turn_topic(kind)) == kind


def test_given_an_unknown_turn_word_when_mapping_then_should_reject_it():
    assert ls.Sessions.turn_kind("readings") is None
    with pytest.raises(ls.InvalidError):
        ls.Sessions.turn_topic("nope")


def test_given_spawn_agent_when_reading_the_stub_then_should_take_a_dedup_window():
    assert "dedup_window" in stub_keywords("Laser", "spawn_agent")
