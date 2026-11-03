import {
  AgentId,
  authorizeEdge,
  delegatedAllow,
  edgeDenialChallenge,
  grantsAllow,
  resourcePatternAll,
  resourcePatternPrefix,
  type EdgeClaims,
  type Grant,
  type Laser,
  type Role
} from "@laserdata/laser-sdk"

import { envInteger, managedGate, phase, runExample, SESSION_RETENTION, utf8 } from "../common.js"

export const EXAMPLE = "governance"

// The live role and binding calls need server-side `authz`. The pure decision
// pieces run everywhere and mirror the Rust and Python examples.
//
// Set LASER_GOVERNANCE_USER_ID to bind the example roles to a specific Iggy
// user. By default the roles are bound to a dedicated `governance-demo` user
// the example creates on first run. Never the caller: the role set includes
// deny-wins grants, and binding those to the session user would poison every
// later run against the same server.
const TARGET_USER_ENV = "LASER_GOVERNANCE_USER_ID"
const DEMO_USER = "governance-demo"
// Apache Iggy's `UserStatus.Active`.
const ACTIVE_USER = 1

const ROLES: readonly Role[] = [
  {
    name: "support-reader",
    grants: [allow("kv", "read", resourcePatternPrefix("support/"))]
  },
  {
    name: "projection-operator",
    grants: [allow("projection", "admin", resourcePatternPrefix("support_"))]
  },
  {
    name: "agent-runner",
    grants: [
      allow("agent", "read", resourcePatternAll()),
      allow("agent", "write", resourcePatternAll())
    ]
  },
  {
    name: "safety-deny",
    grants: [deny("kv", "delete", resourcePatternAll())]
  }
]

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  await laser.bootstrap(1, SESSION_RETENTION)

  phase("Capability RBAC: roles bound to a server-stamped user")
  const capabilities = await laser.capabilities()
  if (managedGate(capabilities, "authz", EXAMPLE, "capability RBAC")) {
    const configured = envInteger(TARGET_USER_ENV, -1)
    await installRoles(laser, configured >= 0 ? configured : await demoUserId(laser))
  }

  phase("Permission intersection: agent grants cannot exceed the user")
  demonstrateIntersection()

  phase("External edge: audience validation and step-up")
  demonstrateEdgeAuth()

  phase("Session governor: submit a budgeted session")
  await submitBudgetedSession(laser)

  console.log(
    "\ngovernance: role grants, deny-wins matching, on-behalf-of intersection,\n" +
      "external-edge step-up, and budgeted session submission share one governance model."
  )
}

/** The dedicated demo user the roles are bound to, created on first run. */
async function demoUserId(laser: Laser): Promise<number> {
  const existing = await laser.client.user.get({ userId: DEMO_USER })
  if (existing !== null) return existing.id
  const created = await laser.client.user.create({
    username: DEMO_USER,
    password: "governance-demo-secret",
    status: ACTIVE_USER
  })
  return created.id
}

async function installRoles(laser: Laser, targetUser: number): Promise<void> {
  for (const role of ROLES) {
    await laser.defineRole(role)
    console.log(`defined role: ${role.name}`)
  }
  await laser.bindRoles(
    targetUser,
    ROLES.map((role) => role.name)
  )

  const who = await laser.whoami()
  console.log(
    `caller roles: [${who.roles.join(", ")}], effective grants: ${String(who.grants.length)}`
  )
  const supportRoles = await laser.listRoles({ namePrefix: "support" })
  console.log(
    `roles with prefix \`support\`: [${supportRoles.map((role) => role.name).join(", ")}]`
  )
  if ((await laser.getRole("support-reader")) === undefined) {
    console.log("support-reader role was not visible after define")
  }
  const bindings = await laser.getBindings(targetUser)
  console.log(`user ${String(targetUser)} is bound to: [${bindings.join(", ")}]`)
}

function demonstrateIntersection(): void {
  const user: readonly Grant[] = [
    allow("kv", "read", resourcePatternPrefix("support/")),
    deny("kv", "delete", resourcePatternAll())
  ]
  const agent: readonly Grant[] = [
    allow("kv", "read", resourcePatternPrefix("support/tickets/")),
    allow("kv", "write", resourcePatternPrefix("support/tickets/")),
    allow("kv", "delete", resourcePatternPrefix("support/tickets/"))
  ]
  for (const action of ["read", "write", "delete"] as const) {
    const allowed = delegatedAllow(agent, user, "kv", action, "support/tickets/acme")
    console.log(`delegated ${action} support/tickets/acme: ${String(allowed)}`)
  }
  const direct = grantsAllow(user, "kv", "delete", "support/tickets/acme")
  console.log(`direct user delete support/tickets/acme: ${String(direct)}`)
}

function demonstrateEdgeAuth(): void {
  const readOnly: EdgeClaims = { audience: ["mcp.laserdata"], scopes: ["tool:read"] }
  const foreign: EdgeClaims = { audience: ["other.server"], scopes: ["tool:write"] }

  const read = authorizeEdge(readOnly, "mcp.laserdata", "tool:read")
  console.log(`edge read authorized: ${String(read === undefined)}`)

  const write = authorizeEdge(readOnly, "mcp.laserdata", "tool:write")
  const challenge = write === undefined ? "none" : (edgeDenialChallenge(write) ?? "none")
  console.log(`edge write step-up challenge: ${challenge}`)

  const rejected = authorizeEdge(foreign, "mcp.laserdata", "tool:write")
  console.log(`foreign audience rejected: ${String(rejected?.kind === "wrongAudience")}`)
}

async function submitBudgetedSession(laser: Laser): Promise<void> {
  const submitted = await laser
    .sessions()
    .submit(AgentId.new("governance-auditor"), utf8("audit this incident"))
    .from(AgentId.new("governance"))
    .budget({ tokens: 4_000n })
    .send()
  console.log(`submitted budgeted session: ${submitted.session.toString()}`)
}

function allow(
  feature: Grant["feature"],
  action: Grant["action"],
  resource: Grant["resource"]
): Grant {
  return { effect: "allow", feature, action, resource }
}

function deny(
  feature: Grant["feature"],
  action: Grant["action"],
  resource: Grant["resource"]
): Grant {
  return { effect: "deny", feature, action, resource }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
