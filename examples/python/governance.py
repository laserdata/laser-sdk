"""governance: capability RBAC plus agent-governance decisions.

The Python peer of the Rust `governance` example. Live role and binding calls
need server-side `authz`. The pure decision pieces run everywhere.

Set LASER_GOVERNANCE_USER_ID to bind the example roles to a specific Iggy user.
The Rust and TypeScript examples create a dedicated `governance-demo` user when
it is unset. The Python SDK has no Iggy user management, so without the
variable this example defines the roles and skips the binding. It never binds
to the caller: the role set includes deny-wins grants, and binding those to the
session user would poison every later run against the same server.

    python3 governance.py
"""

from __future__ import annotations

import _common
import laser_sdk as ls

EXAMPLE = "governance"
TARGET_USER_ENV = "LASER_GOVERNANCE_USER_ID"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        await laser.bootstrap(
            _common.PARTITIONS, retention=ls.TopicRetention.expire_after(86_400_000)
        )

        _common.phase("Capability RBAC: roles bound to a server-stamped user")
        caps = await laser.capabilities()
        if _common.managed_gate(caps.authz, "capability RBAC", "governance"):
            target_user = _common.env_int(TARGET_USER_ENV, -1)
            await install_roles(laser, target_user if target_user >= 0 else None)

        _common.phase("Permission intersection: agent grants cannot exceed the user")
        demonstrate_intersection()

        _common.phase("External edge: audience validation and step-up")
        demonstrate_edge_auth()

        _common.phase("Session governor: submit a budgeted session")
        await submit_budgeted_session(laser)

        print(
            "\ngovernance: role grants, deny-wins matching, on-behalf-of intersection,\n"
            "external-edge step-up, and budgeted session submission share one governance model."
        )
    finally:
        await laser.close()


async def install_roles(laser, target_user: int | None) -> None:
    for name, grants in roles().items():
        await laser.define_role(name, grants)
        print(f"defined role: {name}")

    bound = ["support-reader", "projection-operator", "agent-runner", "safety-deny"]
    if target_user is not None:
        await laser.bind_roles(target_user, bound)

    who = await laser.whoami()
    print(f"caller roles: {who.roles}, effective grants: {len(who.grants)}")
    support_roles = await laser.list_roles("support")
    print("roles with prefix `support`:", [role.name for role in support_roles])
    if await laser.get_role("support-reader") is None:
        print("support-reader role was not visible after define")
    if target_user is None:
        print(f"set {TARGET_USER_ENV} to bind the roles to a user")
        return
    bindings = await laser.get_bindings(target_user)
    print(f"user {target_user} is bound to: {bindings}")


def roles() -> dict[str, list[ls.Grant]]:
    return {
        "support-reader": [ls.Grant("kv", "read", resource=ls.ResourcePattern.prefix("support/"))],
        "projection-operator": [
            ls.Grant(
                "projection",
                "admin",
                resource=ls.ResourcePattern.prefix("support_"),
            )
        ],
        "agent-runner": [
            ls.Grant("agent", "read"),
            ls.Grant("agent", "write"),
        ],
        "safety-deny": [
            ls.Grant("kv", "delete", effect="deny"),
        ],
    }


def demonstrate_intersection() -> None:
    user = [
        ls.Grant("kv", "read", resource=ls.ResourcePattern.prefix("support/")),
        ls.Grant("kv", "delete", effect="deny"),
    ]
    agent = [
        ls.Grant("kv", "read", resource=ls.ResourcePattern.prefix("support/tickets/")),
        ls.Grant("kv", "write", resource=ls.ResourcePattern.prefix("support/tickets/")),
        ls.Grant("kv", "delete", resource=ls.ResourcePattern.prefix("support/tickets/")),
    ]

    for action in ("read", "write", "delete"):
        allowed = ls.delegated_allow(agent, user, "kv", action, "support/tickets/acme")
        print(f"delegated {action} support/tickets/acme: {allowed}")
    direct = ls.grants_allow(user, "kv", "delete", "support/tickets/acme")
    print(f"direct user delete support/tickets/acme: {direct}")


def demonstrate_edge_auth() -> None:
    denial = ls.authorize_edge(["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:read")
    print(f"edge read authorized: {denial is None}")

    denial = ls.authorize_edge(["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:write")
    print(f"edge write step-up challenge: {denial.challenge}")

    denial = ls.authorize_edge(["other.server"], ["tool:write"], "mcp.laserdata", "tool:write")
    print(f"foreign audience rejected: {denial.kind == 'wrong_audience'}")


async def submit_budgeted_session(laser) -> None:
    submitted = await (
        laser.sessions()
        .submit("governance-auditor", b"audit this incident")
        .from_("governance")
        .budget(ls.Budget(tokens=4_000))
        .send()
    )
    print(f"submitted budgeted session: {submitted.session}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
