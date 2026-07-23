import { createInterface } from "node:readline/promises"
import {
  Agent,
  AgentId,
  AgentTopic,
  Budget,
  capabilitySelector,
  routeTo,
  type AgentHandle,
  type Laser
} from "@laserdata/laser-sdk"

import { AsyncResourceGroup, decodeUtf8, envBoolean, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "orchestra"
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Commands }

async function pause(label: string): Promise<void> {
  console.log(label)
  if (envBoolean("LASER_NON_INTERACTIVE", false)) return
  using input = createInterface({
    input: process.stdin,
    output: process.stdout
  })
  await input.question("Press Enter to continue: ")
}

function spawnWorker(laser: Laser, name: string, delayMs = 0): AgentHandle {
  return Agent.builder()
    .id(AgentId.new(name))
    .listenOn(AgentTopic.Commands)
    .respondOn(AgentTopic.Responses)
    .ackOnPickup()
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs))
        await context.respond(
          utf8(`${name}:${decodeUtf8(message.envelope?.body ?? message.payload)}`)
        )
      }
    })
    .spawn(laser)
}

async function advertise(
  laser: Laser,
  name: string,
  skill: string,
  unavailable = false
): Promise<void> {
  await laser.publishCard(AgentId.new(name), {
    name,
    capabilities: [
      {
        skillId: skill,
        health: {
          kind: "known",
          name: unavailable ? "Unavailable" : "Healthy"
        }
      }
    ],
    ttlMicros: 300_000_000n
  })
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("Discovery: a pool of long-running capability agents connects")
  await laser.bootstrap(1)
  const names = ["triage", "diag-alpha", "diag-beta", "remediate", "slow", "backup"] as const
  await using agents = new AsyncResourceGroup()
  const handles = names.map((name) =>
    agents.add(spawnWorker(laser, name, name === "slow" ? 250 : 0))
  )
  await Promise.all(handles.map((handle) => handle.ready()))
  await advertise(laser, "triage", "triage")
  await advertise(laser, "diag-alpha", "diagnose")
  await advertise(laser, "diag-beta", "diagnose")
  await advertise(laser, "diag-offline", "diagnose", true)
  await advertise(laser, "remediate", "remediate")
  await advertise(laser, "slow", "slow")
  await advertise(laser, "backup", "slow")

  phase("Contract: a directed task to one capable agent, with a deadline")
  const contract = await laser
    .contract(routeTo(AgentId.new("triage")))
    .from(AgentId.new("orchestrator"))
    .payload(utf8("classify incident"))
    .inboxRoute(fixedCommands)
    .deadline(2_000)
    .send()
  if (contract.kind !== "completed") throw new Error(`triage contract ended as ${contract.kind}`)
  await pause("contract completed")

  phase("Fan-out: a panel scattered to every capable agent")
  const selector = capabilitySelector("diagnose", { kind: "any" })
  const first = await laser.scatter(
    AgentId.new("orchestrator"),
    selector,
    utf8("inspect incident"),
    fixedCommands,
    2_000
  )
  if (first.length !== 2) throw new Error("healthy scatter must return two findings")
  await pause(`scatter completed with ${String(first.length)} findings`)

  phase("Workflow: triage, diagnose, then remediate")
  const workflow = await laser
    .workflow("orchestrator")
    .inboxRoute(fixedCommands)
    .budget(Budget.unlimited().invocations(3).wallClock(10_000))
    .step("triage", routeTo(AgentId.new("triage")), () => utf8("triage"))
    .step("diagnose", routeTo(AgentId.new("diag-alpha")), ({ outputs }) =>
      utf8(`diagnose:${decodeUtf8(outputs.get("triage") ?? new Uint8Array())}`)
    )
    .after("triage")
    .step("remediate", routeTo(AgentId.new("remediate")), ({ outputs }) =>
      utf8(`remediate:${decodeUtf8(outputs.get("diagnose") ?? new Uint8Array())}`)
    )
    .after("diagnose")
    .run()
  if (workflow.outputs.size !== 3) throw new Error("workflow did not journal all three steps")
  await pause(`workflow ${workflow.runId.toString()} completed`)

  phase("Quarantine: an operator pulls a misbehaving agent")
  await laser.quarantine(AgentId.new("operator"), AgentId.new("diag-alpha"))
  const after = await laser.scatter(
    AgentId.new("orchestrator"),
    selector,
    utf8("inspect after quarantine"),
    fixedCommands,
    2_000
  )
  if (after.length !== 1) throw new Error("quarantine must remove one diagnostic target")
  await pause("quarantine rerouted the panel")

  phase("Recovery: the operator reinstates the agent")
  await laser.unquarantine(AgentId.new("operator"), AgentId.new("diag-alpha"))
  const restored = await laser.scatter(
    AgentId.new("orchestrator"),
    selector,
    utf8("inspect after recovery"),
    fixedCommands,
    2_000
  )
  if (restored.length !== 2) throw new Error("unquarantine must restore the diagnostic target")
  await pause("the full diagnostic panel recovered")

  phase("Expiry + recovery: a tight deadline times out, then reroutes")
  const expired = await laser
    .contract(routeTo(AgentId.new("slow")))
    .from(AgentId.new("orchestrator"))
    .payload(utf8("bounded task"))
    .inboxRoute(fixedCommands)
    .deadline(50)
    .send()
  if (expired.kind !== "timedOut") throw new Error("slow task must time out")
  const recovered = await laser
    .contract(routeTo(AgentId.new("backup")))
    .from(AgentId.new("orchestrator"))
    .payload(utf8("bounded task"))
    .inboxRoute(fixedCommands)
    .deadline(2_000)
    .send()
  if (recovered.kind !== "completed") throw new Error("healthy redispatch must complete")
  await pause("deadline recovery completed")
  console.log("\nall orchestration phases completed with durable journal evidence")
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
