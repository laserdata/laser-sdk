"""governance: capability RBAC plus agent-governance decisions.

The Python peer of the Rust `governance` example. Live role and binding calls
need fork-native `authz`. The pure decision pieces run everywhere.

    python governance.py
"""

from __future__ import annotations

import _common
import laser_sdk as ls

EXAMPLE = "governance"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    await laser.bootstrap(_common.PARTITIONS)

    _common.phase("Capability RBAC: roles bound to a server-stamped user")
    caps = await laser.capabilities()
    if _common.managed_gate(caps.authz, "capability RBAC", "governance"):
        await install_roles(laser, _common.env_int("LASER_GOVERNANCE_USER_ID", 1))

    _common.phase("Permission intersection: agent grants cannot exceed the user")
    demonstrate_intersection()

    _common.phase("External edge: audience validation and step-up")
    demonstrate_edge_auth()

    _common.phase("Run governor: submit a budgeted managed run when served")
    if caps.agent_workflow:
        await submit_budgeted_run(laser)
    else:
        print("agent_workflow is not advertised, so the live budgeted-run submit is skipped.")

    print(
        "\ngovernance: role grants, deny-wins matching, on-behalf-of intersection,\n"
        "external-edge step-up, and budgeted run submission share one governance model."
    )


async def install_roles(laser, target_user: int) -> None:
    for name, grants in roles().items():
        await laser.define_role(name, grants)
        print(f"defined role: {name}")

    bound = ["support-reader", "projection-operator", "agent-runner", "safety-deny"]
    await laser.bind_roles(target_user, bound)

    who = await laser.whoami()
    print(f"caller roles: {who.roles}, effective grants: {len(who.grants)}")
    support_roles = await laser.list_roles("support")
    print("roles with prefix `support`:", [role.name for role in support_roles])
    if await laser.get_role("support-reader") is None:
        print("support-reader role was not visible after define")
    bindings = await laser.get_bindings(target_user)
    print(f"user {target_user} is bound to: {bindings}")


def roles() -> dict[str, list[ls.Grant]]:
    return {
        "support-reader": [
            ls.Grant("kv", "read", resource_kind="prefix", resource_value="support/")
        ],
        "projection-operator": [
            ls.Grant(
                "projection",
                "admin",
                resource_kind="prefix",
                resource_value="support_",
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
        ls.Grant("kv", "read", resource_kind="prefix", resource_value="support/"),
        ls.Grant("kv", "delete", effect="deny"),
    ]
    agent = [
        ls.Grant("kv", "read", resource_kind="prefix", resource_value="support/tickets/"),
        ls.Grant("kv", "write", resource_kind="prefix", resource_value="support/tickets/"),
        ls.Grant("kv", "delete", resource_kind="prefix", resource_value="support/tickets/"),
    ]

    for action in ("read", "write", "delete"):
        allowed = ls.delegated_allow(agent, user, "kv", action, "support/tickets/acme")
        print(f"delegated {action} support/tickets/acme: {allowed}")
    direct = ls.grants_allow(user, "kv", "delete", "support/tickets/acme")
    print(f"direct user delete support/tickets/acme: {direct}")


def demonstrate_edge_auth() -> None:
    authorized, challenge = ls.authorize_edge(
        ["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:read"
    )
    print(f"edge read authorized: {authorized}, challenge={challenge}")

    authorized, challenge = ls.authorize_edge(
        ["mcp.laserdata"], ["tool:read"], "mcp.laserdata", "tool:write"
    )
    print(f"edge write authorized: {authorized}, challenge={challenge}")

    authorized, challenge = ls.authorize_edge(
        ["other.server"], ["tool:write"], "mcp.laserdata", "tool:write"
    )
    print(f"foreign audience authorized: {authorized}, challenge={challenge}")


async def submit_budgeted_run(laser) -> None:
    budget = ls.RunBudget(
        max_events=8,
        max_model_calls=1,
        max_tool_calls=2,
        max_wall_clock_micros=30_000_000,
    )
    try:
        run = await laser.runs().submit_budgeted(
            "governance-auditor", input=b"audit this incident", budget=budget
        )
    except ls.UnsupportedError as exc:
        print(f"budgeted run submit returned unsupported: {exc}")
        return
    print(f"submitted budgeted run: {run.run_id}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
