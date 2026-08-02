import {
  ANY_ROUTE_POLICY,
  Agent,
  AgentId,
  AgentTopic,
  agentMessageBody,
  routeToCapable,
  type Laser
} from "@laserdata/laser-sdk"
import { PARTITIONS, decodeUtf8, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "agent"
const CAPABILITY = "resolve-ticket"
const DEADLINE_MS = 60_000
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Commands }

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  // The well-known agent topics (commands, responses, registry, ...) must exist
  // before an agent's consumer group joins one.
  await laser.bootstrap(PARTITIONS)

  phase("spawn a handler, then hand it a deadline-bounded task")
  await using triage = Agent.builder()
    .id(AgentId.new("triage"))
    .listenOn(AgentTopic.Commands)
    .respondOn(AgentTopic.Responses)
    // The advertised capability is what makes this agent addressable by what it
    // can do rather than by the name it happens to run under.
    .capabilities([{ skillId: CAPABILITY }])
    // Acknowledge on pickup, so a crash mid-handler is a retry rather than a
    // silently dropped task.
    .ackOnPickup()
    .handler({
      handle: (message, context) => {
        console.log(`  triage picked up "${decodeUtf8(agentMessageBody(message))}"`)
        return context.respond(utf8("on it"))
      }
    })
    .spawn(laser)
  await triage.ready()

  // A contract is a directed task with a deadline and a real answer: consumed,
  // completed, failed, or timed out. Routed by capability, not by name.
  const contract = await laser
    .contract(routeToCapable(CAPABILITY, ANY_ROUTE_POLICY))
    .from(AgentId.new("orchestrator"))
    .payload(utf8("ticket #42 is stuck"))
    .inboxRoute(fixedCommands)
    .deadline(DEADLINE_MS)
    .send()

  if (contract.kind === "completed") {
    console.log(`  contract completed: ${decodeUtf8(agentMessageBody(contract.reply))}`)
  } else {
    console.log(`  contract ended without a reply: ${contract.kind}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
