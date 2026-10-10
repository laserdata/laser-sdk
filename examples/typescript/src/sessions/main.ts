import {
  AgentId,
  MintUlid,
  ModelRequest,
  LastN,
  type Laser,
  type Session,
  type Sessions
} from "@laserdata/laser-sdk"
import { wire } from "@laserdata/laser-sdk/full"
import { PARTITIONS, phase, runExample, SESSION_RETENTION, utf8 } from "../common.js"

export const EXAMPLE = "sessions"

async function actingOnLast(
  session: Session,
  sessions: Sessions,
  managed: boolean
): Promise<Session> {
  const message = (await session.context()).at(-1)?.message
  if (message === undefined) throw new Error("the recorded source is unavailable")
  const generation = managed ? (await sessions.sources(session.conversation)).lane?.[1] : undefined
  return session.actingOn({
    kind: "message",
    stream: message.streamId,
    topic: message.topicId,
    partition: message.id.partitionId,
    offset: message.id.offset,
    ...(generation === undefined ? {} : { generation }),
    conversation: session.conversation.toString()
  })
}

async function task(
  sessions: Sessions,
  root: Session,
  label: string,
  agent: string,
  result: string
): Promise<Session> {
  const { session: child, lease } = await sessions
    .create(label)
    .namespace(`ops/${root.conversation.toString()}`)
    .agent(AgentId.new(agent))
    .parent(root.conversation, root.conversation)
    .begin()
  try {
    const call = await child.tool("inspect_service", { service: "api", task: label })
    const correlation = call.correlation()
    const receipt = await call.complete(utf8(result))
    await child.state().set("result", result)
    const turns = await child.context()
    const turn = turns.find(
      (turn) => turn.message.envelope?.record?.toString() === receipt.record?.toString()
    )
    if (turn === undefined || receipt.record === undefined)
      throw new Error("the recorded child result is unavailable")
    const message = turn.message
    const at = {
      streamId: message.streamId,
      topicId: message.topicId,
      partitionId: message.id.partitionId,
      offset: message.id.offset
    }
    const reply = wire.withCause(
      wire.withCorrelation(
        wire.eventEnvelope(
          MintUlid.mint(wire.RecordId),
          wire.ConversationId.parse(root.conversation.toString()),
          wire.parseAgentId("triage"),
          utf8(result)
        ),
        correlation
      ),
      receipt.record,
      at
    )
    await root.append(reply)
    await root.state().set(label, result)
    await child.end()
    return child
  } finally {
    lease.release()
  }
}

async function report(
  session: Session,
  label: string,
  sessions: Sessions,
  managed: boolean
): Promise<void> {
  const events = (await session.context()).length
  const links = managed ? (await sessions.links(session.conversation)).links.length : 0
  console.log(`  ${label}: ${events} events, ${links} resource links`)
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const sessions = laser.sessions()
  const registered = (await sessions.bootstrap(PARTITIONS, SESSION_RETENTION)).registered
  const capabilities = await laser.capabilities()
  if (capabilities.sessions && !registered)
    throw new Error("the session source registration is unavailable")
  phase("one root incident, two child tasks, explicit result collection")
  const { session: root, lease: rootLease } = await sessions
    .create("incident-42")
    .namespace("ops")
    .agent(AgentId.new("triage"))
    .begin()
  let maintenanceLease: { release(): void } | undefined
  let diagnosis: Session
  let remediation: Session
  let maintenance: Session
  try {
    const observer = root.asAgent(AgentId.new("specialist"))
    await (
      await observer.tool("read_metrics", { service: "api" })
    ).complete(utf8("latency increased"))
    diagnosis = await task(sessions, root, "diagnosis", "specialist", "cache saturation")
    remediation = await task(sessions, root, "remediation", "resolver", "reduce cache pressure")
    const assembled = await root.assemble(new LastN(20))
    await (
      await root.model(new ModelRequest("mock", utf8("summarize the recorded findings")), assembled)
    ).complete({ body: utf8("reduce cache pressure and observe latency") })
    phase("a separate maintenance session shares resources in the same stream")
    const started = await sessions
      .create("maintenance-7")
      .namespace("ops")
      .agent(AgentId.new("resolver"))
      .begin()
    maintenance = started.session
    maintenanceLease = started.lease
    await maintenance.state().set("task", "verify cache capacity")
    const rootResources = await actingOnLast(root, sessions, capabilities.sessions)
    const maintenanceResources = await actingOnLast(maintenance, sessions, capabilities.sessions)
    if (capabilities.kv.available) {
      await root.kv("infra").set(utf8("service:api")).json({ finding: "cache saturation" }).send()
      await maintenance
        .kv("infra")
        .set(utf8("maintenance:api"))
        .json({ task: "verify cache capacity" })
        .send()
    }
    if (capabilities.graph) {
      await rootResources.linkedGraph("infra").link("service:api", "depends_on", "service:cache")
      await maintenanceResources
        .linkedGraph("infra")
        .link("service:api", "observed_by", "agent:resolver")
    }
    await rootResources
      .linkedMemory()
      .remember(utf8("api latency increased when the cache saturated"))
      .send()
    await root.end()
    await maintenance.end()
  } finally {
    rootLease.release()
    maintenanceLease?.release()
  }
  if (capabilities.sessions) {
    let ready = false
    for (let attempt = 0; attempt < 600; attempt++) {
      const info = await sessions.get(root.conversation)
      const links = await sessions.links(root.conversation)
      if (info.status === "completed" && links.links.some((link) => link.surface === "memory")) {
        ready = true
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    if (!ready) throw new Error("the session example views did not converge")
  }
  for (const [session, label] of [
    [root, "incident-42"],
    [diagnosis, "diagnosis"],
    [remediation, "remediation"],
    [maintenance, "maintenance-7"]
  ] as const) {
    await report(session, label, sessions, capabilities.sessions)
  }
  console.log("  2 independent roots, 2 child sessions, explicit parent results")
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
