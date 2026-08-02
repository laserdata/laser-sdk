import {
  authorizeEdge,
  delegatedAllow,
  edgeDenialChallenge,
  grantsAllow,
  type Grant,
  type Laser,
  type Role
} from "@laserdata/laser-sdk"

import { envInteger, managedGate, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "governance"

const USER_GRANTS: readonly Grant[] = [
  {
    effect: "allow",
    feature: "kv",
    action: "read",
    resource: { kind: "prefix", value: "support." }
  },
  {
    effect: "deny",
    feature: "kv",
    action: "read",
    resource: { kind: "literal", value: "support.secrets" }
  }
]
const AGENT_GRANTS: readonly Grant[] = [
  {
    effect: "allow",
    feature: "kv",
    action: "read",
    resource: { kind: "prefix", value: "support." }
  }
]

function purePolicy(): void {
  if (!grantsAllow(USER_GRANTS, "kv", "read", "support.tickets")) {
    throw new Error("the support prefix must be allowed")
  }
  if (grantsAllow(USER_GRANTS, "kv", "read", "support.secrets")) {
    throw new Error("a matching deny must override allow")
  }
  if (!delegatedAllow(AGENT_GRANTS, USER_GRANTS, "kv", "read", "support.tickets")) {
    throw new Error("delegated permission must intersect agent and user grants")
  }
  if (delegatedAllow(AGENT_GRANTS, [], "kv", "read", "support.tickets")) {
    throw new Error("delegation must not exceed the invoking user")
  }
  const denial = authorizeEdge({ audience: ["mcp"], scopes: [] }, "mcp", "ticket:write")
  if (denial?.kind !== "stepUp") throw new Error("missing edge scope must require step-up")
  console.log(`edge challenge: ${edgeDenialChallenge(denial) ?? "none"}`)
}

async function managedPolicy(laser: Laser): Promise<void> {
  const userId = envInteger("LASER_GOVERNANCE_USER_ID", 1)
  const role: Role = { name: "support-reader", grants: USER_GRANTS }
  await laser.defineRole(role)
  await laser.bindRoles(userId, [role.name])
  const [who, roles, bound] = await Promise.all([
    laser.whoami(),
    laser.listRoles({ namePrefix: "support-" }),
    laser.getBindings(userId)
  ])
  console.log(
    `roles: effective=${who.roles.join(",")} listed=${String(roles.length)} bound=${bound.join(",")}`
  )
  const capabilities = await laser.capabilities()
  if (capabilities.agentWorkflow) {
    const run = await laser
      .runs()
      .submitBudgeted(
        "governed-agent",
        { maxEvents: 32n, maxModelCalls: 2n, maxWallClockMicros: 5_000_000n },
        utf8("inspect support ticket")
      )
    console.log(`budgeted run: ${run.runId}`)
  }
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("Permission matching: deny wins")
  purePolicy()
  phase("Capability RBAC: roles bound to a server-stamped user")
  const capabilities = await laser.capabilities()
  if (managedGate(capabilities, "authz", EXAMPLE)) await managedPolicy(laser)
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
