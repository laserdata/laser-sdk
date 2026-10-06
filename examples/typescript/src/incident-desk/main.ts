import {
  Agent,
  AgentId,
  AgentTopic,
  ContentType,
  ConversationId,
  ConversationState,
  KvExecutionError,
  MemoryHandle,
  MemoryKind,
  agentMessageBody,
  Json,
  parseProjectionId,
  routeTo,
  type AgentHandle,
  type Deduplicator,
  type Laser,
  type Projection,
  type ProjectionBinding
} from "@laserdata/laser-sdk"

import {
  AsyncResourceGroup,
  PARTITIONS,
  Rng,
  batchSize,
  decodeUtf8,
  envBoolean,
  indexFor,
  managedGate,
  messages,
  phase,
  runExample,
  utf8,
  waitForProjection
} from "../common.js"
import { defaultLlm } from "../llm.js"

export const EXAMPLE = "incident-desk"
const TICKETS = "support_tickets"
// The index carries this run's token, so a rerun or another language's desk on the
// same deployment never shares its rows.
const TICKETS_INDEX = indexFor(TICKETS)
const PLAN = "bulk-resolve-plan"
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Commands }
const fixedTools = { kind: "fixed" as const, topic: AgentTopic.ToolCalls }
const ANGLES = ["most likely root cause", "fastest mitigation", "blast radius"] as const
const NOTES = [
  "auth latency usually traces to database pool exhaustion",
  "config push retries require an idempotency key",
  "critical auth incidents recover by failing over the read replica"
] as const
const GRANTS = [
  { key: "gr-1", cluster: "east-1", units: 150 },
  { key: "gr-2", cluster: "west-2", units: 50 },
  { key: "gr-3", cluster: "eu-1", units: 80 }
] as const

interface Ticket {
  readonly ticket_id: string
  readonly message_type: "ticket"
  readonly cluster: string
  readonly component: string
  readonly severity: string
  readonly status: "open"
  readonly ts: number
}

function ticketValue(value: unknown): Ticket {
  if (value === null || typeof value !== "object") throw new TypeError("ticket must be an object")
  const item = value as Partial<Ticket>
  if (
    typeof item.ticket_id !== "string" ||
    item.message_type !== "ticket" ||
    typeof item.cluster !== "string" ||
    typeof item.component !== "string" ||
    typeof item.severity !== "string" ||
    item.status !== "open" ||
    !Number.isSafeInteger(item.ts)
  ) {
    throw new TypeError("ticket fields are invalid")
  }
  return item as Ticket
}

const TICKET_CODEC = new Json(ticketValue)

class SimpleEmbedder {
  embed(text: string): Promise<readonly number[]> {
    const values = Array.from({ length: 64 }, () => 0)
    for (const byte of utf8(text.toLowerCase())) {
      const index = byte % values.length
      values[index] = (values[index] ?? 0) + 1
    }
    return Promise.resolve(values)
  }
}

class KvDeduplicator implements Deduplicator {
  constructor(
    private readonly laser: Laser,
    private readonly namespace: string
  ) {}

  async observe(key: string): Promise<boolean> {
    try {
      await this.laser
        .kv(this.namespace)
        .set(utf8(key))
        .bytes(Uint8Array.of(1))
        .ttl(3_600_000_000n)
        .expectAbsent()
        .commit()
      return true
    } catch (error) {
      if (
        error instanceof KvExecutionError &&
        typeof error.detail === "object" &&
        error.detail !== null &&
        "kind" in error.detail &&
        error.detail.kind === "versionConflict"
      ) {
        return false
      }
      throw error
    }
  }
}

async function registerTickets(laser: Laser): Promise<void> {
  const id = parseProjectionId(`${TICKETS_INDEX}.v1`)
  const projection: Projection = {
    id,
    name: TICKETS_INDEX,
    version: 1,
    kind: { kind: "row" },
    contentType: ContentType.Json,
    extraction: {
      fields: ["ticket_id", "message_type", "cluster", "component", "severity", "status", "ts"].map(
        (name) => ({ name, pointer: `/${name}` })
      ),
      inlinePayload: false
    },
    inlinePayloadDefault: false
  }
  const binding: ProjectionBinding = {
    source: { stream: laser.defaultStream ?? "", topic: TICKETS },
    allowedProjections: [id],
    defaultProjection: id,
    index: TICKETS_INDEX,
    notify: true
  }
  await laser.projections().register(projection)
  await laser.bindings().apply(binding)
}

function tickets(count: number): readonly Ticket[] {
  const rng = new Rng(0xc0ffee42n)
  const clusters = ["east-1", "west-2", "eu-1", "ap-1", "lab"] as const
  const components = ["auth", "config", "storage", "metrics", "gateway"] as const
  const severities = ["low", "medium", "high", "critical"] as const
  return Array.from({ length: count }, (_, index) => ({
    ticket_id: `ticket-${String(index).padStart(7, "0")}`,
    message_type: "ticket",
    cluster: rng.pick(clusters),
    component: rng.pick(components),
    severity: rng.pick(severities),
    status: "open",
    ts: 1_900_000_000_000_000 + index
  }))
}

async function ingest(laser: Laser, count: number): Promise<void> {
  const values = tickets(count)
  const chunk = batchSize(200)
  for (let start = 0; start < values.length; start += chunk) {
    await laser
      .topic(TICKETS)
      .publishBatch()
      .inlinePayload()
      .extendJson(values.slice(start, start + chunk), TICKET_CODEC)
      .send()
  }
  await waitForProjection(laser, TICKETS_INDEX, count)
  const payload = await laser.query(TICKETS_INDEX).fetchOne(TICKET_CODEC)
  if (payload === undefined) throw new Error("materialized tickets returned no payload")
  console.log(`ticket payload round trip: ${payload.ticket_id}/${payload.component}`)
}

async function spawnDesk(
  laser: Laser,
  memory: MemoryHandle,
  grantNamespace: string,
  dedupNamespace: string
): Promise<readonly AgentHandle[]> {
  const llm = defaultLlm()
  const triage = Agent.builder()
    .id(AgentId.new("triage"))
    .listenOn(AgentTopic.Commands)
    .respondOn(AgentTopic.Responses)
    .inboxRoute(fixedTools)
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        const incident = decodeUtf8(message.envelope?.body ?? message.payload)
        const findings: string[] = []
        for (const angle of ANGLES) {
          const provenance = {
            ...context.spawnSubconversation(),
            targetAgentId: AgentId.new("specialist")
          }
          const reply = await context.request(
            AgentTopic.ToolCalls,
            AgentTopic.ToolResults,
            utf8(`${angle}: ${incident}`),
            provenance,
            15_000
          )
          findings.push(decodeUtf8(agentMessageBody(reply)))
        }
        await context.respond(utf8(await llm.complete(`${incident}\n${findings.join("\n")}`)))
      }
    })
    .build()
    .spawn(laser)
  const specialist = Agent.builder()
    .id(AgentId.new("specialist"))
    .listenOn(AgentTopic.ToolCalls)
    .respondOn(AgentTopic.ToolResults)
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        const prompt = decodeUtf8(message.envelope?.body ?? message.payload)
        const recalled = await memory.recall().semantic(prompt).limit(2).fetch()
        await context.respond(
          utf8(
            await llm.complete(
              `${prompt}\n${recalled.map((item) => decodeUtf8(item.payload)).join("\n")}`
            )
          )
        )
      }
    })
    .build()
    .spawn(laser)
  const approver = Agent.builder()
    .id(AgentId.new("approver"))
    .listenOn(AgentTopic.HumanInput)
    .pollInterval(5)
    .handler({
      handle(_message, context): Promise<void> {
        return context.respondInput(AgentTopic.Responses, utf8("approved"))
      }
    })
    .build()
    .spawn(laser)
  const grants = laser.kv(grantNamespace)
  const resolver = Agent.builder()
    .id(AgentId.new("resolver"))
    .listenOn(AgentTopic.Commands)
    .pollInterval(5)
    .deduplicator(new KvDeduplicator(laser, dedupNamespace))
    .handler({
      async handle(message, context): Promise<void> {
        const grant = JSON.parse(decodeUtf8(message.envelope?.body ?? message.payload)) as {
          cluster: string
          units: number
        }
        if (grant.units >= 100) {
          const decision = await context.approvalGate(
            AgentTopic.Responses,
            utf8(`approve a ${String(grant.units)} unit capacity grant to ${grant.cluster}?`),
            15_000
          )
          if (decodeUtf8(decision) !== "approved") return
        }
        const key = utf8(grant.cluster)
        const current = Number(decodeUtf8((await grants.get(key)) ?? utf8("0")))
        await grants
          .set(key)
          .bytes(utf8(String(current + grant.units)))
          .send()
      }
    })
    .build()
    .spawn(laser)
  const handles = [triage, specialist, resolver, approver]
  await Promise.all(handles.map((handle) => handle.ready()))
  return handles
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("warming up")
  await laser.bootstrap(PARTITIONS)
  await laser.topic(TICKETS).ensure(PARTITIONS)
  const capabilities = await laser.capabilities()
  if (
    !managedGate(capabilities, "query", EXAMPLE) ||
    !managedGate(capabilities, "kvCas", EXAMPLE) ||
    !managedGate(capabilities, "forks", EXAMPLE)
  ) {
    return
  }
  phase("registering the ticket index")
  await registerTickets(laser)
  const count = messages(2_000)
  phase("ingesting the ticket firehose")
  await ingest(laser, count)

  phase("seeding semantic memory with past resolutions")
  const memory = MemoryHandle.vector(new SimpleEmbedder())
  for (const note of NOTES) await memory.remember(utf8(note)).kind(MemoryKind.Fact).send()
  const runId = ConversationId.new()
  const grantNamespace = `desk-grants-${runId.toString()}`
  const dedupNamespace = `desk-dedup-${runId.toString()}`
  phase("spawning the desk: triage, specialist, resolver, approver")
  await using agents = new AsyncResourceGroup()
  const handles = await spawnDesk(laser, memory, grantNamespace, dedupNamespace)
  for (const handle of handles) agents.add(handle)
  phase("triaging the incident through the desk")
  const incident = ConversationId.new()
  const diagnosis = await laser
    .contract(routeTo(AgentId.new("triage")))
    .from(AgentId.new("orchestrator"))
    .conversation(incident)
    .payload(utf8("auth is slow for several clusters"))
    .inboxRoute(fixedCommands)
    .deadline(60_000)
    .send()
  if (diagnosis.kind !== "completed") throw new Error(`diagnosis ended as ${diagnosis.kind}`)
  const text = decodeUtf8(agentMessageBody(diagnosis.reply))
  await memory.remember(utf8(text)).kind(MemoryKind.Summary).durable().send()
  console.log(`diagnosis: ${text}`)

  phase("executing capacity grants effectively once")
  for (const grant of [...GRANTS, ...GRANTS]) {
    await laser
      .agent(AgentId.new("orchestrator"))
      .send(AgentTopic.Commands, utf8(JSON.stringify(grant)), {
        conversationId: incident,
        idempotencyKey: grant.key,
        targetAgentId: AgentId.new("resolver")
      })
  }
  const expected = new Map([
    ["east-1", 150],
    ["west-2", 50],
    ["eu-1", 80]
  ])
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    const actual = await Promise.all(
      [...expected].map(
        async ([cluster, units]) =>
          [
            cluster,
            Number(decodeUtf8((await laser.kv(grantNamespace).get(utf8(cluster))) ?? utf8("0"))),
            units
          ] as const
      )
    )
    if (actual.every(([, value, units]) => value === units)) break
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  for (const [cluster, units] of expected) {
    const actual = Number(
      decodeUtf8((await laser.kv(grantNamespace).get(utf8(cluster))) ?? utf8("0"))
    )
    if (actual !== units) throw new Error(`grant total for ${cluster} is ${String(actual)}`)
  }
  console.log("duplicate deliveries produced exact cluster grant totals")

  phase("speculating a bulk-resolve plan in a fork")
  const fork = laser.fork(PLAN)
  await fork.create().tables([TICKETS_INDEX]).send()
  await fork.putRow(TICKETS_INDEX, 0, 0n).field("status", "resolved").send()
  if (envBoolean("LASER_APPLY_PLAN", false)) await fork.promote()
  console.log(`speculative fork: ${PLAN}`)

  phase("rebuilding the incident from the log alone")
  const rebuilt = await ConversationState.load(
    laser,
    incident,
    [AgentTopic.Commands, AgentTopic.Responses, AgentTopic.ToolCalls, AgentTopic.ToolResults],
    { kind: "full" },
    0,
    (total) => total + 1
  )
  if (rebuilt === 0) throw new Error("conversation state rebuild returned no messages")
  console.log(`audit records rebuilt: ${String(rebuilt)}`)
  phase("done")
  console.log("the desk resolved the incident with a replayable audit trail")
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
