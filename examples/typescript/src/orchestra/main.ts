import { createInterface } from "node:readline/promises"
import {
  Agent,
  AgentId,
  agentMessageBody,
  AgentTopic,
  capabilitySelector,
  routeAllCapable,
  routeToCapable,
  WorkflowBudget,
  type Contract,
  type Health,
  type Laser
} from "@laserdata/laser-sdk"

import {
  AsyncResourceGroup,
  connectAgain,
  decodeUtf8,
  envBoolean,
  phase,
  runExample,
  SESSION_RETENTION,
  utf8
} from "../common.js"

// One orchestrator coordinating a pool of long-running capability agents,
// entirely over the log. It is interactive and paced: it stops at each phase
// and waits for Enter, so you can watch every transition in the LaserData
// console's Orchestration view. `LASER_NON_INTERACTIVE=1` runs it straight
// through. The phases mirror the Rust and Python `orchestra` examples.
//
// Routing uses a fixed inbox topic so it runs against Apache Iggy.

export const EXAMPLE = "orchestra"
const CLASSIFY = "classify"
const DIAGNOSE = "diagnose"
const REMEDIATE = "remediate"
const SLOW_TASK = "slow-task"
const INCIDENT = "auth API latency spike"
const ORCHESTRATOR = AgentId.new("orchestrator")
const OPERATOR = AgentId.new("operator")
const FIXED_INBOX = { kind: "fixed" as const, topic: AgentTopic.Sessions }
const HEALTHY: Health = { kind: "known", name: "Healthy" }
const UNAVAILABLE: Health = { kind: "known", name: "Unavailable" }

export interface OrchestraSummary {
  readonly classified: string | undefined
  readonly findings: number
  readonly workflowSteps: number
  readonly quarantined: number
  readonly reinstated: number
  readonly recovered: string | undefined
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<OrchestraSummary> {
  await laser.bootstrap(1, SESSION_RETENTION)

  phase("Discovery: a pool of long-running capability agents connects")
  // Kept alive for the whole run so the console stays populated. Health is a
  // property of the card: diag-gamma advertises unavailable to prove routing
  // reads it, and laggard is deliberately slow to drive the expiry phase.
  await using agents = new AsyncResourceGroup()
  await spawnWorker(agents, "triager", CLASSIFY, HEALTHY, 200)
  await spawnWorker(agents, "diag-alpha", DIAGNOSE, HEALTHY, 400)
  await spawnWorker(agents, "diag-beta", DIAGNOSE, HEALTHY, 400)
  await spawnWorker(agents, "diag-gamma", DIAGNOSE, UNAVAILABLE, 400)
  await spawnWorker(agents, "executor", REMEDIATE, HEALTHY, 300)
  await spawnWorker(agents, "laggard", SLOW_TASK, HEALTHY, 6_000)
  console.log("six agents connected and advertised their capability cards")
  await pause("DISCOVERY: six agents are live in the registry (one unavailable)")

  phase("Contract: a directed task to one capable agent, with a deadline")
  // The orchestrator names a capability, not an agent. Routing resolves the one
  // classifier from the registry and waits for the reply or the deadline.
  const classified = completedBody(await contractSkill(laser, CLASSIFY, 10_000))
  console.log(`classifier replied: ${classified ?? "<did not complete>"}`)
  await pause("CONTRACT: a directed task completed (see it in the Contracts panel)")

  phase("Fan-out: a panel scattered to every capable agent")
  // Three agents advertise diagnose, but one is unavailable, so the scatter
  // reaches the two healthy ones without the orchestrator knowing their ids.
  const findings = (await diagnosePanel(laser)).length
  console.log(`panel gathered ${String(findings)} findings (the unavailable agent was skipped)`)
  await pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped")

  phase("Workflow: triage, then a diagnose panel, then remediate (journalled)")
  // The run is a session and every step a child session of it, so the
  // sessions view shows the whole tree on any deployment.
  const workflow = await laser
    .workflow("incident-response")
    .inboxRoute(FIXED_INBOX)
    // Cap the dispatches and wall clock so a runaway fan-out cannot spin.
    .budget(WorkflowBudget.unlimited().invocations(8).wallClock(60_000))
    .step("triage", routeToCapable(CLASSIFY, { kind: "any" }), () => utf8(INCIDENT))
    // Each step reads the prior steps' outputs from the journal, so the
    // dependency edge is data, not a shared variable.
    .step("diagnose", routeAllCapable(DIAGNOSE, { kind: "any" }), ({ outputs }) =>
      utf8(`diagnose: ${decodeUtf8(outputs.get("triage") ?? new Uint8Array())}`)
    )
    .after("triage")
    .verifyWith((folded) => folded.length > 0)
    .step("remediate", routeToCapable(REMEDIATE, { kind: "any" }), ({ outputs }) =>
      utf8(`remediate: ${decodeUtf8(outputs.get("diagnose") ?? new Uint8Array())}`)
    )
    .after("diagnose")
    .run()
  console.log(`workflow completed and journalled: ${String(workflow.outputs.size)} steps`)
  await pause("WORKFLOW: the run journalled triage -> diagnose -> remediate (Workflow panel)")

  phase("Quarantine: an operator pulls a misbehaving agent")
  // Quarantine is a registry fact every fused registry folds, so the next panel
  // routes around diag-alpha with no change to the orchestrator.
  await laser.quarantine(OPERATOR, AgentId.new("diag-alpha"))
  const quarantined = (await diagnosePanel(laser)).length
  console.log(`panel after quarantine: ${String(quarantined)} findings (alpha routed around)`)
  await pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")

  phase("Recovery: the operator reinstates the agent")
  await laser.unquarantine(OPERATOR, AgentId.new("diag-alpha"))
  const reinstated = (await diagnosePanel(laser)).length
  console.log(`panel after un-quarantine: ${String(reinstated)} findings (alpha is back)`)
  await pause("RECOVERY: diag-alpha is reinstated, the panel is whole again")

  phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers")
  // The slow agent acks pickup but cannot finish inside the one-second deadline,
  // so the contract expires. The orchestrator recovers by re-dispatching to a
  // healthy fast agent, the pattern any real coordinator uses for a stuck task.
  const slow = await contractSkill(laser, SLOW_TASK, 1_000)
  let recovered: string | undefined
  if (slow.kind === "completed") {
    console.log(`unexpectedly fast: ${completedBody(slow) ?? ""}`)
  } else {
    console.log("the slow agent missed the deadline, recovering on a healthy agent")
    recovered = completedBody(await contractSkill(laser, REMEDIATE, 10_000))
    console.log(`recovered: ${recovered ?? "<did not complete>"}`)
  }
  await pause("EXPIRY: the slow agent timed out, the task recovered on a healthy agent")

  console.log(
    "\norchestra: discovery, routing, fan-out, a journalled workflow, health,\n" +
      "reversible quarantine, and deadline recovery, all coordinated over the log."
  )
  return {
    classified,
    findings,
    workflowSteps: workflow.outputs.size,
    quarantined,
    reinstated,
    recovered
  }
}

/**
 * Spawns one long-running capability agent on its own connection, so each is
 * a distinct live presence in the console. It advertises its card on start,
 * and the resource group keeps the connection open until the run ends.
 */
async function spawnWorker(
  agents: AsyncResourceGroup,
  name: string,
  skill: string,
  health: Health,
  delayMs: number
): Promise<void> {
  const connection = agents.add(await connectAgain(EXAMPLE))
  const handle = agents.add(
    Agent.builder()
      .id(AgentId.new(name))
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .capabilities([{ skillId: skill, health }])
      // Ack on pickup so the orchestrator can tell a consumed task from an
      // expired one, which is what makes the expiry phase legible.
      .ackOnPickup()
      .handler({
        async handle(message, context): Promise<void> {
          await new Promise((resolve) => setTimeout(resolve, delayMs))
          const task = decodeUtf8(agentMessageBody(message))
          await context.respond(utf8(workerReply(name, skill, task)))
        }
      })
      .build()
      .spawn(connection)
  )
  await handle.ready()
}

function workerReply(name: string, skill: string, task: string): string {
  switch (skill) {
    case CLASSIFY:
      return `severity=high (${task})`
    case DIAGNOSE:
      return `${name}: cache stampede on the hot key [${task}]`
    case REMEDIATE:
      return `${name}: drained the hot key, scaled the cache [${task}]`
    default:
      return `${name}: ${skill} done [${task}]`
  }
}

async function contractSkill(laser: Laser, skill: string, deadlineMs: number): Promise<Contract> {
  return laser
    .contract(routeToCapable(skill, { kind: "any" }))
    .from(ORCHESTRATOR)
    .payload(utf8(INCIDENT))
    .inboxRoute(FIXED_INBOX)
    .deadline(deadlineMs)
    .send()
}

/** Scatters a diagnose panel to every capable agent. Unavailable agents are
 * left out by capability resolution. */
async function diagnosePanel(laser: Laser): Promise<readonly Uint8Array[]> {
  return laser.scatter(
    ORCHESTRATOR,
    capabilitySelector(DIAGNOSE, { kind: "any" }),
    utf8(INCIDENT),
    FIXED_INBOX,
    10_000
  )
}

function completedBody(outcome: Contract): string | undefined {
  return outcome.kind === "completed" ? decodeUtf8(agentMessageBody(outcome.reply)) : undefined
}

/** Prints what to watch, then waits for Enter unless `LASER_NON_INTERACTIVE` is set. */
async function pause(prompt: string): Promise<void> {
  console.log(
    `\n  >>> ${prompt}\n      (watch the console's /orchestration view, then press Enter)`
  )
  if (envBoolean("LASER_NON_INTERACTIVE", false)) return
  // Node 22.14 has no `Symbol.dispose` on a readline interface, so close it by hand.
  const input = createInterface({ input: process.stdin, output: process.stdout })
  try {
    await input.question("")
  } finally {
    input.close()
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
