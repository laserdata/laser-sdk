import laser_sdk as ls
import pytest

SPIKE = ls.FilterExpr.pred("cpu", "gte", 90)


def test_given_a_step_up_decision_when_reading_it_then_should_expose_the_verdict_and_evidence():
    policy = ls.PolicyRef("ops-pack", "3", ["rotate-needs-approval"])
    decision = (
        ls.ActionDecision.step_up("storage:rotate")
        .with_reason("needs an operator")
        .with_policy(policy)
        .with_risk_score(0.75)
    )
    assert decision.verdict == ls.Verdict.step_up("storage:rotate")
    assert decision.verdict.as_str() == "step_up"
    assert decision.verdict.scope == "storage:rotate"
    assert decision.verdict.body is None
    assert decision.reason == "needs an operator"
    assert decision.policy == policy
    assert decision.policy.pack_id == "ops-pack"
    assert decision.policy.pack_version == "3"
    assert decision.policy.rule_ids == ["rotate-needs-approval"]
    assert decision.risk_score == 0.75


def test_given_a_modify_decision_when_reading_it_then_should_expose_the_replacement_body():
    decision = ls.ActionDecision.modify(b"redacted")
    assert decision.verdict.as_str() == "modify"
    assert decision.verdict.body == b"redacted"
    assert decision.verdict.scope is None
    assert decision.reason is None
    assert decision.policy is None
    assert decision.risk_score is None
    assert ls.ActionDecision.block("over budget").reason == "over budget"
    assert ls.ActionDecision.block("over budget").verdict == ls.Verdict.block()


def test_given_each_verdict_when_naming_it_then_should_read_its_evidence_name():
    names = [
        ls.Verdict.allow(),
        ls.Verdict.observe(),
        ls.Verdict.block(),
        ls.Verdict.step_up("scope"),
        ls.Verdict.modify(b"body"),
        ls.Verdict.defer(),
    ]
    assert [verdict.as_str() for verdict in names] == [
        "allow",
        "observe",
        "block",
        "step_up",
        "modify",
        "defer",
    ]
    assert str(ls.Verdict.defer()) == "defer"
    assert ls.Verdict.step_up("a") != ls.Verdict.step_up("b")


def test_given_no_arguments_when_building_retention_then_should_keep_the_sdk_defaults():
    defaults = ls.GovernorRetention()
    assert defaults.capacity == 4096
    assert defaults.idle_ttl_ms == 3_600_000.0
    tuned = ls.GovernorRetention(capacity=16, idle_ttl_ms=1_500)
    assert tuned.capacity == 16
    assert tuned.idle_ttl_ms == 1_500
    with pytest.raises(ls.InvalidError):
        ls.GovernorRetention(idle_ttl_ms=-1.0)


def test_given_an_intent_without_voters_when_building_it_then_should_raise_its_variant():
    with pytest.raises(ls.IntentError) as caught:
        ls.Intent(
            conversation=ls.new_conversation_id(),
            proposer="proposer",
            body=b"transfer",
            eligible_voters=[],
            policy=ls.IntentPolicy.any(),
            policy_version=1,
            deadline_micros=2**62,
        )
    assert caught.value.kind == ls.IntentError.NO_ELIGIBLE_VOTERS
    assert ls.IntentError.NO_ELIGIBLE_VOTERS == "no_eligible_voters"
    assert isinstance(caught.value, ls.InvalidError)
    assert ls.ValidateError.TOO_LARGE == "too_large"
    assert issubclass(ls.ValidateError, ls.InvalidError)


def test_given_a_value_without_decide_when_enrolled_as_a_governor_then_should_raise_invalid():
    def allow(action):
        return ls.ActionDecision.allow()

    with pytest.raises(ls.InvalidError):
        ls.SwappableGovernor(initial=object())
    swappable = ls.SwappableGovernor(initial=allow)
    assert swappable.current() is allow
    with pytest.raises(ls.InvalidError):
        swappable.swap(next=object())
    assert swappable.current() is allow
    quorum = ls.QuorumGovernor(ls.QuorumPolicy.any())
    with pytest.raises(ls.InvalidError):
        quorum.voter("safety", object(), True)
    quorum.voter("safety", allow, True)


def test_given_a_deadline_when_building_provenance_then_should_read_it_back():
    assert ls.Provenance(deadline_micros=1_700_000_000_000_000).deadline == (1_700_000_000_000_000)
    assert ls.Provenance().deadline is None


def test_given_a_plain_message_when_reading_its_content_type_then_should_be_none():
    message = ls.agent_message(b"hello", ls.Provenance())
    assert message.content_type is None


def test_given_a_json_filter_when_compiled_then_should_evaluate_like_the_server():
    source = ls.ConsumerFilter.json(SPIKE)
    compiled = ls.CompiledFilter.compile(source)
    assert compiled.digest == source.digest
    assert compiled.filter == source
    assert compiled.fault_policy == "stop"
    assert compiled.reads_payload
    assert not compiled.reads_headers
    assert compiled.header_need == "content_type"
    assert compiled.evaluate(b'{"cpu": 95}') == "selected"
    assert compiled.evaluate(b'{"cpu": 5}') == "rejected"
    assert compiled.evaluate_with_fault(b'{"cpu": 95}') == ("selected", None)
    assert compiled.evaluate_with_fault(b"not json") == ("fault", "malformed")
    assert compiled.explain(b'{"cpu": 95}')["verdict"] == "selected"


def test_given_a_headers_only_filter_when_compiled_then_should_need_every_header():
    compiled = ls.CompiledFilter.compile(
        ls.ConsumerFilter.headers_only(ls.FilterExpr.header("region", "eq", "eu"))
    )
    assert not compiled.reads_payload
    assert compiled.header_need == "all"
    assert compiled.evaluate(b"", {"region": "eu"}) == "selected"
    assert compiled.evaluate(b"", {"region": "us"}) == "rejected"


def test_given_fault_reasons_when_resolving_policies_then_should_follow_the_record_policies():
    compiled = ls.CompiledFilter.compile(
        ls.ConsumerFilter.json(SPIKE, "drop").with_foreign_policy("pass")
    )
    assert compiled.policy_for("malformed") == "drop"
    assert compiled.policy_for("foreign_codec") == "pass"
    assert compiled.policy_for("type_mismatch") == "drop"
    assert compiled.record_policy("malformed") is None
    assert compiled.record_policy("foreign_codec") == "pass"
    assert compiled.record_policy("type_mismatch") == "reject"
    with pytest.raises(ls.InvalidError):
        compiled.policy_for("nope")


def test_given_an_agent_owner_when_deriving_a_content_id_then_should_match_the_pinned_vector():
    assert ls.memory_id_content("fact", b"x", agent="agent") == "63FZWE3WTSCVXAY845QK4TMVWM"


@pytest.mark.integration
async def test_given_session_options_when_reading_the_config_then_should_report_the_layout(laser):
    sessions = laser.sessions(
        stream="support",
        idle_timeout_ms=120_000,
        heartbeat_ms=30_000,
        register_source=False,
        fail_on_dead_letter=True,
        memory_namespace="support.memory",
        context_turns=7,
        context_tokens=900,
    )
    config = sessions.config
    assert config.stream_name == "support"
    assert config.idle_timeout_ms == 120_000
    assert config.heartbeat_ms == 30_000
    assert config.registers_source is False
    assert config.fails_on_dead_letter is True
    assert config.memory_namespace_name == "support.memory"
    assert config.context_turn_bound == 7
    assert config.context_token_bound == 900
    assert sessions.open(ls.new_conversation_id()).config.context_turn_bound == 7
    defaults = laser.sessions().config
    assert defaults.stream_name is None
    assert (defaults.idle_timeout_ms, defaults.heartbeat_ms) == (300_000, 60_000)
    assert defaults.registers_source is True
    assert defaults.memory_namespace_name == "agent.session"
    assert (defaults.context_turn_bound, defaults.context_token_bound) == (50, 4000)


@pytest.mark.integration
async def test_given_a_note_when_improving_through_a_scope_then_should_reach_the_backend(laser):
    calls = []

    class Hooks:
        async def remember(self, scope, payload):
            return ls.new_conversation_id()

        async def recall(self, scope, query):
            return []

        async def forget(self, scope, target):
            calls.append(("forget", scope, target))

        async def improve(self, scope, feedback):
            calls.append(("improve", scope, feedback))
            return ls.new_conversation_id()

    conversation = ls.new_conversation_id()
    target = ls.new_conversation_id()
    scoped = laser.context(conversation).memory(laser.memory_custom(Hooks()))
    await scoped.improve(target, 2.0, note="operator feedback")
    verb, scope, feedback = calls[-1]
    assert verb == "improve"
    assert scope["conversation"] == conversation
    assert feedback == {"target": target, "weight": 2.0, "note": "operator feedback"}


@pytest.mark.integration
async def test_given_upstream_hops_when_a_bridge_submits_then_should_stamp_the_path(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    bridge = ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions")
    assert bridge.with_bridge_hops(["mcp-edge"]) is bridge
    task = await bridge.submit(
        {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}}
    )
    messages = await laser.context(task["id"]).fetch(topics=["agent.sessions"], n=1)
    assert messages[0].envelope["metadata"]["bridge_hops"] == ["mcp-edge", "a2a-edge"]


@pytest.mark.integration
async def test_given_a_path_holding_the_bridge_when_continued_then_should_refuse_the_loop(laser):
    a2a = ls.A2aBridge(laser, "edge", "agent.sessions", "agent.sessions")
    with pytest.raises(ls.InvalidError):
        a2a.with_bridge_hops(["edge"])
    mcp = ls.McpBridge(laser, "edge", "agent.tools", "agent.sessions", "fleet")
    with pytest.raises(ls.InvalidError):
        mcp.with_bridge_hops(["upstream", "edge"])
    assert mcp.with_bridge_hops(["upstream"]) is mcp


def test_given_a_path_holding_a_bridge_when_entered_then_should_refuse_the_loop():
    assert ls.enter_bridge("mcp-edge", ["a2a-edge"]) == ["a2a-edge", "mcp-edge"]
    with pytest.raises(ls.InvalidError):
        ls.enter_bridge("edge", ["upstream", "edge"])


def test_given_a_completed_contract_when_matched_then_should_carry_the_reply():
    reply = ls.agent_message(b"done", ls.Provenance(agent="worker"))
    outcome = ls.Contract.Completed(reply)
    match outcome:
        case ls.Contract.Completed(message):
            assert message.body() == b"done"
        case _:
            pytest.fail("a completed contract must match Completed")
    assert isinstance(ls.Contract.TimedOut(), ls.Contract)
    assert not isinstance(ls.Contract.NotConsumed(), ls.Contract.Completed)


def test_given_session_policies_when_mapping_a_key_then_should_derive_or_mint_conversations():
    per_user = ls.session_policy_conversation_for("per_user", "alice")
    assert per_user == ls.session_policy_conversation_for("per_user", "alice")
    assert per_user == ls.derive_conversation_id("alice")
    assert ls.session_policy_conversation_for("per_call", "alice") != per_user
    with pytest.raises(ls.InvalidError):
        ls.session_policy_conversation_for("per_tenant", "alice")


def test_given_inbox_routes_when_resolving_then_should_pick_fixed_or_advertised_topics():
    assert ls.inbox_route_resolve(None, "worker", "worker.inbox") == "worker.inbox"
    assert ls.inbox_route_resolve("agent.sessions", "worker", "worker.inbox") == "agent.sessions"
    with pytest.raises(ls.LaserError):
        ls.inbox_route_resolve(None, "worker")
