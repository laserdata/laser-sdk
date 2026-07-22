import laser_sdk as ls


def test_grant_construction_defaults():
    # feature + action are positional. effect/resource default to allow/all.
    grant = ls.Grant("kv", "read")
    assert grant.feature == "kv"
    assert grant.action == "read"
    assert grant.effect == "allow"
    assert grant.resource_kind == "all"
    assert grant.resource_value == ""


def test_grant_full_and_mutable():
    grant = ls.Grant(
        "kv",
        "read",
        effect="deny",
        resource_kind="prefix",
        resource_value="agent-abc/",
    )
    assert grant.effect == "deny"
    assert grant.resource_kind == "prefix"
    assert grant.resource_value == "agent-abc/"
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
    assert ls.Whoami is not None


def test_grant_decision_helpers_are_exposed():
    user = [
        ls.Grant("kv", "read", resource_kind="prefix", resource_value="support/"),
        ls.Grant("kv", "delete", effect="deny"),
    ]
    agent = [
        ls.Grant("kv", "read", resource_kind="prefix", resource_value="support/tickets/"),
        ls.Grant("kv", "write", resource_kind="prefix", resource_value="support/tickets/"),
    ]
    assert ls.grants_allow(user, "kv", "read", "support/tickets/acme")
    assert not ls.grants_allow(user, "kv", "delete", "support/tickets/acme")
    assert ls.delegated_allow(agent, user, "kv", "read", "support/tickets/acme")
    assert not ls.delegated_allow(agent, user, "kv", "write", "support/tickets/acme")
