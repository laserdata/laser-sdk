import laser_sdk as ls
import pytest


def test_grant_construction_defaults():
    # feature + action are positional. effect/resource default to allow/all.
    grant = ls.Grant("kv", "read")
    assert grant.feature == "kv"
    assert grant.action == "read"
    assert grant.effect == "allow"
    assert grant.resource == ls.ResourcePattern.all()
    assert grant.resource.kind == "all"
    assert grant.resource.value == ""


def test_grant_full_and_mutable():
    grant = ls.Grant(
        "kv",
        "read",
        effect="deny",
        resource=ls.ResourcePattern.prefix("agent-abc/"),
    )
    assert grant.effect == "deny"
    assert grant.resource.kind == "prefix"
    assert grant.resource.value == "agent-abc/"
    # Fields are settable (the editor builds a grant incrementally).
    grant.action = "write"
    assert grant.action == "write"


def test_laser_exposes_the_rbac_verbs():
    for verb in (
        "whoami",
        "list_roles",
        "get_role",
        "get_bindings",
        "define_role",
        "delete_role",
        "bind_roles",
    ):
        assert hasattr(ls.Laser, verb)


def test_authz_types_are_exposed():
    assert ls.Grant is not None
    assert ls.Role is not None
    assert ls.WhoamiReply is not None
    assert ls.AuthzHistoryReply is not None


def test_grant_decision_helpers_are_exposed():
    user = [
        ls.Grant("kv", "read", resource=ls.ResourcePattern.prefix("support/")),
        ls.Grant("kv", "delete", effect="deny"),
    ]
    agent = [
        ls.Grant("kv", "read", resource=ls.ResourcePattern.prefix("support/tickets/")),
        ls.Grant("kv", "write", resource=ls.ResourcePattern.prefix("support/tickets/")),
    ]
    assert ls.grants_allow(user, "kv", "read", "support/tickets/acme")
    assert not ls.grants_allow(user, "kv", "delete", "support/tickets/acme")
    assert ls.delegated_allow(agent, user, "kv", "read", "support/tickets/acme")
    assert not ls.delegated_allow(agent, user, "kv", "write", "support/tickets/acme")


def test_given_resource_patterns_when_matched_then_should_follow_the_wire_rule():
    assert ls.ResourcePattern.all().matches(None)
    assert ls.ResourcePattern.literal("a/b").matches("a/b")
    assert not ls.ResourcePattern.literal("a/b").matches("a/bc")
    assert ls.ResourcePattern.prefix("a/").matches("a/bc")
    assert not ls.ResourcePattern.prefix("a/").matches(None)
    assert ls.ResourcePattern("literal", "x") == ls.ResourcePattern.literal("x")


def test_given_an_unknown_resource_kind_when_built_then_should_raise_value_error():
    with pytest.raises(ValueError):
        ls.ResourcePattern("everything")


def test_given_role_names_when_validated_then_should_reject_invalid_ones():
    ls.validate_role_name("support-reader.v2")
    with pytest.raises(ls.InvalidError):
        ls.validate_role_name("")
    with pytest.raises(ls.InvalidError):
        ls.validate_role_name("bad name")


def test_given_edge_claims_when_authorized_then_should_return_the_denial():
    assert ls.authorize_edge(["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:read") is None
    step_up = ls.authorize_edge(["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:write")
    assert step_up == ls.EdgeDenial.step_up("tool:write")
    assert step_up.kind == "step_up"
    assert step_up.code == "StepUpRequired"
    assert step_up.challenge == 'Bearer scope="tool:write"'
    foreign = ls.authorize_edge(["other.server"], ["tool:write"], "mcp.laserdata", "tool:write")
    assert foreign == ls.EdgeDenial.wrong_audience("mcp.laserdata")
    assert foreign.code == "Unauthenticated"
    assert foreign.challenge is None
