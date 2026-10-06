import asyncio
import inspect
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


def test_given_the_error_hierarchy_when_checking_stream_errors_then_should_be_config_errors():
    assert issubclass(ls.NoStreamError, ls.ConfigError)
    assert issubclass(ls.NoRespondTopicError, ls.ConfigError)


def test_given_the_context_read_window_when_read_then_should_match_rust():
    assert ls.CONTEXT_READ_WINDOW == 10_000


def test_given_nested_predicates_when_building_a_query_filter_then_should_produce_the_tree():
    degraded = ls.Filter.pred("status", "eq", "degraded")
    hot = ls.Filter.pred("cpu", "gte", 90)
    tree = ls.Filter.any([ls.Filter.all([degraded, hot]), ls.Filter.negate(hot)])
    hot_leaf = {"pred": {"field": "cpu", "op": "gte", "value": {"kind": "long", "value": 90}}}
    assert tree.to_dict() == {
        "any": [
            {
                "all": [
                    {
                        "pred": {
                            "field": "status",
                            "op": "eq",
                            "value": {"kind": "string", "value": "degraded"},
                        }
                    },
                    hot_leaf,
                ]
            },
            {"not": hot_leaf},
        ]
    }


def test_given_an_unknown_comparison_when_building_a_query_filter_then_should_raise_invalid():
    with pytest.raises(ls.InvalidError):
        ls.Filter.pred("cpu", "almost", 90)


def test_given_context_policies_when_chained_then_should_accept_policies_and_reject_others():
    ls.Chain([ls.LastN(20), ls.TokenBudget(4_000), ls.RoleFilter(["planner"])])
    ls.TokenBudget(100, estimator=lambda message: len(message.payload))
    with pytest.raises(ls.InvalidError):
        ls.Chain([object()])
    with pytest.raises(ls.InvalidError, match="a context policy is"):
        ls.Chain([ls.LastN(1), "newest"])


async def test_given_no_connection_string_when_connecting_from_env_then_should_raise_config_error(
    monkeypatch,
):
    monkeypatch.delenv("LASER_CONNECTION_STRING", raising=False)
    with pytest.raises(ls.ConfigError):
        await ls.Laser.connect_env()


async def test_given_dedup_when_remembering_the_same_body_twice_then_should_store_one_item():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    first = await memory.remember("auth is slow", agent="planner", dedup=True)
    second = await memory.remember("auth is slow", agent="planner", dedup=True)
    assert first == second
    assert [item.id for item in await memory.recall(agent="planner")] == [first]

    plain = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    await plain.remember("auth is slow", agent="planner")
    await plain.remember("auth is slow", agent="planner")
    assert len(await plain.recall(agent="planner")) == 2


async def test_given_a_kind_when_remembering_then_should_recall_it_and_reject_unknown_kinds():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    await memory.remember("auth runbook", conversation=conversation, kind="procedure")
    items = await memory.recall(conversation=conversation)
    assert [item.kind for item in items] == ["procedure"]
    with pytest.raises(ls.InvalidError):
        await memory.remember("auth runbook", kind="note")


async def test_given_a_remembered_item_when_rendering_context_then_should_include_its_text():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    await memory.remember("auth uses the read replica", conversation=conversation)
    block = await memory.context(conversation, token_budget=1_000)
    assert "auth uses the read replica" in block


async def test_given_three_items_when_consolidating_to_one_then_should_prune_the_rest():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    for body in ("auth one", "auth two", "auth three"):
        await memory.remember(body, conversation=conversation)
    report = await memory.consolidate(1, conversation=conversation)
    assert isinstance(report, ls.ConsolidationReport)
    assert report.pruned == 2
    assert len(await memory.recall(conversation=conversation)) == 1


async def test_given_vector_memory_when_using_named_state_then_should_raise_unsupported():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth"]))
    assert memory.backend == "vector"
    with pytest.raises(ls.UnsupportedError):
        await memory.set("plan", b"rotate")


async def test_given_a_reranker_when_recalling_then_should_reorder_only_semantic_recall():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage", "metrics"]))
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
    assert [item.text() for item in flipped] == [item.text() for item in reversed(plain)]
    assert calls == ["auth"]
    await reranked.recall(conversation=conversation)
    assert calls == ["auth"]


async def test_given_an_async_reranker_when_recalling_then_should_allow_dropping_candidates():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth", "storage"]))
    conversation = ls.new_conversation_id()
    for body in ("auth one", "auth two"):
        await memory.remember(body, conversation=conversation)

    async def keep_first(query, items):
        return items[:1]

    items = await memory.reranker(keep_first).recall(conversation=conversation, semantic="auth")
    assert len(items) == 1


def test_given_a_built_intent_when_validated_then_should_accept_it():
    def intent(voters):
        return ls.Intent(
            ls.new_conversation_id(),
            "proposer",
            b"rotate the storage credentials",
            voters,
            ls.IntentPolicy.all(),
            1,
            time.time_ns() // 1_000 + 60_000_000,
        )

    assert intent(["safety"]).validate() is None
    with pytest.raises(ls.InvalidError, match="at least one eligible voter"):
        intent([])


def test_given_a_signing_key_when_signing_with_and_without_context_then_should_bind_the_context():
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


def test_given_an_unenrolled_signer_when_verifying_then_should_refuse_the_envelope():
    signer = ls.SigningKey(bytes(range(32)))
    enrolled = ls.SigningKey(bytes(reversed(range(32))))
    registry = ls.KeyRegistry()
    registry.enroll_record(ls.KeyRecord("agent-7", enrolled.verifying_key))
    envelope = FIXTURES.joinpath("agent_command_signed.bin").read_bytes()
    assert bytes(signer.sign(envelope)["key_id"]) == signer.key_id != enrolled.key_id
    with pytest.raises(ls.SignatureError, match="not enrolled"):
        registry.verify(envelope)
    with pytest.raises(ls.SignatureError, match="not enrolled"):
        registry.verify_at(envelope, 1)
    with pytest.raises(ls.SignatureError):
        registry.verify_observed_at(envelope, 1, content_type="cbor")


def test_given_injected_op_versions_when_constructed_then_should_default_unset_surfaces():
    versions = ls.OpVersions(query=2, graph=1, features=3)
    assert (versions.query, versions.control, versions.graph, versions.features) == (2, 1, 1, 3)


def test_given_open_capabilities_when_read_then_should_nest_every_surface_switched_off():
    caps = ls.Capabilities.OPEN
    assert caps.is_open_only()
    assert (caps.query.available, caps.query.consistency, caps.query.keyword) == (
        False,
        "eventual",
        False,
    )
    assert (caps.destinations.available, caps.destinations.consistency) == (
        False,
        "potentially_stale",
    )
    assert (caps.kv.available, caps.kv.cas, caps.kv.cas_fenced, caps.kv.fenced_leases) == (
        False,
    ) * 4
    assert (caps.filters.native, caps.filters.catalog, caps.filters.evaluation) == (
        False,
        False,
        None,
    )
    assert caps.filters.evaluates(1, "json")
    with pytest.raises(ls.InvalidError):
        caps.filters.evaluates(1, "yaml")


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


# The keyword-only parameters the compiled binding accepts, read from the real
# callable rather than the generated stub, so a stale stub cannot pass. Whether
# a keyword is honored needs a live server and is covered in test_integration.
def keywords(class_name, method):
    parameters = inspect.signature(getattr(getattr(ls, class_name), method)).parameters
    return {name for name, value in parameters.items() if value.kind is value.KEYWORD_ONLY}


def text(value):
    raw = value.encode()
    return bytes([0x60 + len(raw)]) + raw


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
def test_given_an_agdx_verb_when_inspecting_the_binding_then_should_accept_send_options(verb):
    assert keywords("Agdx", verb) >= SEND_KEYWORDS


@pytest.mark.parametrize("method", ["add_json", "add_msgpack", "add_payload", "add_raw_bytes"])
def test_given_a_batch_add_when_inspecting_the_binding_then_should_accept_a_projection_ref(method):
    assert "projection_ref" in keywords("BatchPublishRequest", method)


def test_given_a_batch_when_inspecting_add_record_then_should_accept_record_options():
    assert keywords("BatchPublishRequest", "add_record") >= {
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
        ("Laser", "scatter", "policy"),
        ("Laser", "scatter_report", "policy"),
        ("AgentCtx", "fan_out", "route_policy"),
        ("AgentScope", "contract", "policy"),
        ("Workflow", "step", "policy"),
    ],
)
def test_given_a_capability_route_when_inspecting_the_binding_then_should_accept_a_route_policy(
    class_name, method, keyword
):
    assert keyword in keywords(class_name, method)


def test_given_a_checkpoint_from_json_when_reading_topic_offsets_then_should_map_partitions():
    checkpoint = ls.Checkpoint.from_json('{"per_topic": {"readings": {"0": 7, "2": 41}}}')
    assert checkpoint.topic_offsets("readings") == {0: 7, 2: 41}
    assert checkpoint.topic_offsets("incidents") is None


async def test_given_messages_when_consolidating_with_a_summarizer_then_should_store_its_summary():
    memory = ls.MemoryHandle.vector(bag_of_words(["cpu"]))
    conversation = ls.new_conversation_id()
    for body in ("cpu one", "cpu two", "cpu three"):
        await memory.remember(body, conversation=conversation, kind="message")
    calls = []

    def summarize(bodies):
        calls.append(len(bodies))
        return b"summary: " + b" | ".join(sorted(bytes(body) for body in bodies))

    report = await memory.consolidate(
        1, conversation=conversation, summarizer=summarize, prune_summarized=True
    )
    assert calls == [3]
    assert report.summarized == 3
    items = await memory.recall(conversation=conversation, limit=10)
    assert [(item.kind, item.text()) for item in items] == [
        ("summary", "summary: cpu one | cpu three | cpu two")
    ]


def test_given_scoped_memory_when_inspecting_consolidate_then_should_accept_a_summarizer():
    assert keywords("ScopedMemory", "consolidate") >= {"summarizer", "prune_summarized"}


def test_given_an_empty_memory_when_consolidating_with_a_summarizer_then_should_report_nothing():
    async def consolidate():
        memory = ls.MemoryHandle.vector(bag_of_words(["cpu"]))
        return await memory.consolidate(
            4, summarizer=lambda bodies: b"".join(bodies), prune_summarized=True
        )

    assert asyncio.run(consolidate()).summarized == 0


def test_given_the_same_body_when_deriving_content_ids_then_should_depend_on_kind_and_owner():
    body = b'{"host": "node-7", "cpu": 82}'
    first = ls.memory_id_content("fact", body, stream="metrics", agent="monitor")
    assert first == ls.memory_id_content("fact", body, stream="metrics", agent="monitor")
    assert first != ls.memory_id_content("message", body, stream="metrics", agent="monitor")
    assert first != ls.memory_id_content("fact", body, stream="metrics")


def test_given_a_memory_kind_when_reading_its_class_then_should_return_the_class_word():
    assert ls.memory_kind_class("message") == "episodic"
    assert ls.memory_kind_class("procedure") == "procedural"
    with pytest.raises(ls.LaserError):
        ls.memory_kind_class("nope")


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


def test_given_evidence_with_a_forged_digest_when_verifying_the_chain_then_should_reject_it():
    fields = {
        "decision_id": "01J0000000000000000000",
        "decision": "allow",
        "mode": "enforce",
        "kind": "command",
        "stream": "metrics",
        "topic": "readings",
        "receipt_digest": "abc123",
        "outcome": "effected",
    }
    payload = (
        bytes([0xA0 + len(fields) + 1])
        + b"".join(text(key) + text(value) for key, value in fields.items())
        + text("at_micros")
        + bytes([7])
    )
    assert ls.verify_evidence_chain([])
    assert not ls.verify_evidence_chain([ls.PolicyEvidence.decode(payload)])


def test_given_projections_list_when_inspecting_the_binding_then_should_accept_topics_and_search():
    assert keywords("Projections", "list") >= {"topic", "topics", "search"}


def test_given_a_registered_card_when_checking_it_then_should_answer_freshness_and_skills():
    card = ls.RegisteredCard(
        "triage-1",
        {
            "capabilities": [{"skill_id": "triage"}, {"skill_id": "sleep", "health": 3}],
            "ttl_micros": 10,
        },
        100,
    )
    assert card.agent == "triage-1"
    assert card.observed_at_micros == 100
    assert card.serves("triage")
    assert not card.serves("diagnose")
    assert card.available_for("triage")
    assert not card.available_for("sleep")
    assert card.is_fresh(110)
    assert not card.is_fresh(111)
    with pytest.raises(ls.InvalidError):
        ls.RegisteredCard("", {}, 1)


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


def test_given_spawn_agent_when_inspecting_the_binding_then_should_accept_a_dedup_window():
    assert "dedup_window" in keywords("Laser", "spawn_agent")


def test_given_a_key_record_when_built_by_rust_constructors_then_should_expose_the_verifying_key():
    key = ls.SigningKey.from_bytes(bytes(range(32)))
    agent = ls.KeyRecord.agent("agent-7", key.verifying_key)
    operator = ls.KeyRecord.from_verifying_bytes("operator-9", key.verifying_key, "operator")
    assert (agent.principal, agent.kind, agent.verifying) == ("agent-7", "agent", key.verifying_key)
    assert (operator.kind, operator.key_id) == ("operator", key.key_id)
    with pytest.raises(ls.InvalidError):
        ls.KeyRecord.from_verifying_bytes("agent-7", key.verifying_key, "root")


def parameters(target):
    return list(inspect.signature(target).parameters)


def test_given_a_context_scope_when_inspecting_reads_then_should_take_the_rust_parameter_names():
    assert keywords("ContextScope", "fetch") >= {"topics", "n"}
    assert keywords("ContextScope", "block") >= {"topics", "n"}
    assert parameters(ls.ContextScope.state)[1:3] == ["topics", "init"]
    assert parameters(ls.ContextScope.state_with)[1:4] == ["store", "topics", "init"]
    assert parameters(ls.ContextScope.memory)[1:] == ["namespace"]
    assert parameters(ls.ContextScope.memory_with)[1:3] == ["namespace", "backend"]
    assert "embedder" in keywords("ContextScope", "memory_with")


def test_given_scoped_memory_when_inspecting_forget_and_improve_then_should_take_id_and_target():
    assert parameters(ls.ScopedMemory.forget)[1:] == ["id"]
    assert parameters(ls.ScopedMemory.improve)[1:3] == ["target", "weight"]


def test_given_the_context_module_when_inspecting_then_should_expose_checkpoint_and_messages():
    assert parameters(ls.context_checkpoint) == ["laser", "topics"]
    fields = {"id", "provenance", "payload", "envelope", "topic"}
    assert all(hasattr(ls.ContextMessage, name) for name in fields)


async def test_given_a_semantic_recall_when_reading_signals_then_should_report_recall_signals():
    memory = ls.VectorMemory(bag_of_words(["auth", "storage"]))
    assert isinstance(memory, ls.MemoryHandle)
    assert memory.backend == "vector"
    await memory.remember("auth is slow")
    items = await memory.recall(semantic="auth")
    signal = items[0].signals[0]
    assert isinstance(signal, ls.RecallSignal)
    assert (signal.strategy, signal.rank) == ("semantic", 0)
    assert items[0].signals == items[0].signals


async def test_given_block_when_recalling_then_should_render_the_context_block():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth"]))
    for body in ("auth one", "auth two"):
        await memory.remember(body)
    items = await memory.recall(strategy="recent")
    block = await memory.recall(strategy="recent", block=True, token_budget=1)
    assert block == ls.to_context_block(items, token_budget=1)
    assert block.startswith(items[0].text())


async def test_given_a_reranked_memory_when_recalling_then_should_apply_the_reranker():
    inner = ls.MemoryHandle.vector(bag_of_words(["auth"]))
    for body in ("auth one", "auth two"):
        await inner.remember(body)
    reranked = ls.RerankedMemory(inner, lambda query, items: list(reversed(items)))
    assert isinstance(reranked, ls.MemoryHandle)
    plain = await inner.recall(semantic="auth")
    flipped = await reranked.recall(semantic="auth")
    assert [item.id for item in flipped] == [item.id for item in reversed(plain)]


async def test_given_a_custom_backend_when_wrapped_without_a_connection_then_should_delegate():
    class Remembered:
        async def remember(self, scope, payload):
            return ls.new_conversation_id()

        def recall(self, scope, query):
            return [{"payload": b"\xffkept"}]

        def improve(self, scope, feedback):
            return feedback["target"]

        def forget(self, scope, id):
            return None

    memory = ls.MemoryHandle.custom(Remembered())
    assert memory.backend == "custom"
    assert memory.embedder(lambda _: [1.0]).backend == "custom"
    items = await memory.recall()
    assert items[0].text() == "�kept"
    with pytest.raises(ls.CodecError):
        items[0].json()


async def test_given_a_vector_handle_when_setting_an_embedder_then_should_stay_vector():
    memory = ls.MemoryHandle.vector(bag_of_words(["auth"]))
    swapped = memory.embedder(bag_of_words(["storage"]))
    assert swapped.backend == "vector"
    await swapped.remember('{"note": "storage full"}')
    items = await swapped.recall(semantic="storage")
    assert items[0].json() == {"note": "storage full"}


def test_given_memory_kinds_when_reading_codes_then_should_match_rust():
    codes = ["fact", "message", "summary", "entity", "feedback", "procedure"]
    assert [ls.memory_kind_code(kind) for kind in codes] == [1, 2, 3, 4, 5, 6]
    with pytest.raises(ls.InvalidError):
        ls.memory_kind_code("nope")


def test_given_a_message_id_when_built_then_should_print_partition_and_offset():
    position = ls.MessageId(2, 41)
    assert (position.partition_id, position.offset, str(position)) == (2, 41, "2:41")
    assert position == ls.MessageId(2, 41)
    assert len({position, ls.MessageId(2, 41)}) == 1


def test_given_two_minted_ulids_when_compared_then_should_differ_and_be_ulids():
    first, second = ls.mint_ulid(), ls.mint_ulid()
    assert first != second
    assert len(first) == 26


def test_given_a_provenance_when_reading_the_partition_key_then_should_be_the_conversation():
    provenance = ls.Provenance(agent="planner")
    assert provenance.partition_key() == provenance.conversation_id
