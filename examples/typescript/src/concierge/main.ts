import {
  Agent,
  AgentId,
  AgentTopic,
  ContentType,
  ConversationId,
  ConversationState,
  FULL_REPLAY,
  KvExecutionError,
  MemoryHandle,
  MemoryKind,
  agentMessageBody,
  jsonCodec,
  parseProjectionId,
  routeTo,
  type AgentHandle,
  type Deduplicator,
  type Laser,
  type Projection,
  type ProjectionBinding
} from "@laserdata/laser-sdk"

import {
  PARTITIONS,
  Rng,
  batchSize,
  decodeUtf8,
  envBoolean,
  managedGate,
  messages,
  runExample,
  utf8,
  waitForProjection
} from "../common.js"
import { MockLlm } from "../llm.js"

export const EXAMPLE = "concierge"
const TICKETS = "support_tickets"
const PLAN = "bulk-resolve-plan"
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Commands }
const fixedTools = { kind: "fixed" as const, topic: AgentTopic.ToolCalls }
const ANGLES = ["most likely root cause", "fastest mitigation", "blast radius"] as const
const NOTES = [
  "checkout latency usually traces to database pool exhaustion",
  "billing retries require an idempotency key",
  "critical checkout incidents recover by failing over the read replica"
] as const
const CREDITS = [
  { key: "cr-1", customer: "acme", cents: 150 },
  { key: "cr-2", customer: "globex", cents: 50 },
  { key: "cr-3", customer: "initech", cents: 80 }
] as const

interface Ticket {
  readonly ticket_id: string
  readonly message_type: "ticket"
  readonly customer: string
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
    typeof item.customer !== "string" ||
    typeof item.component !== "string" ||
    typeof item.severity !== "string" ||
    item.status !== "open" ||
    !Number.isSafeInteger(item.ts)
  ) {
    throw new TypeError("ticket fields are invalid")
  }
  return item as Ticket
}

const TICKET_CODEC = jsonCodec(ticketValue)

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
  const id = parseProjectionId(`${TICKETS}.v1`)
  const projection: Projection = {
    id,
    name: TICKETS,
    version: 1,
    kind: { kind: "row" },
    contentType: ContentType.Json,
    extraction: {
      fields: [
        "ticket_id",
        "message_type",
        "customer",
        "component",
        "severity",
        "status",
        "ts"
      ].map((name) => ({ name, pointer: `/${name}` })),
      inlinePayload: false
    },
    inlinePayloadDefault: false
  }
  const binding: ProjectionBinding = {
    source: { stream: laser.defaultStream ?? "", topic: TICKETS },
    allowedProjections: [id],
    defaultProjection: id,
    targets: [
      {
        backend: "embedded",
        table: TICKETS,
        role: "readWrite",
        delivery: "effectivelyOnce",
        required: true
      }
    ],
    notify: false
  }
  await laser.projections().register(projection)
  await laser.bindings().apply(binding)
}

function tickets(count: number): readonly Ticket[] {
  const rng = new Rng(0xc0ffee42n)
  const customers = ["acme", "globex", "initech", "umbrella", "stark"] as const
  const components = ["checkout", "billing", "search", "auth", "uploads"] as const
  const severities = ["low", "medium", "high", "critical"] as const
  return Array.from({ length: count }, (_, index) => ({
    ticket_id: `ticket-${String(index).padStart(7, "0")}`,
    message_type: "ticket",
    customer: rng.pick(customers),
    component: rng.pick(components),
    severity: rng.pick(severities),
    status: "open",
    ts: 1_900_000_000_000_000 + index
  }))
}

async function ingest(laser: Laser, count: number): Promise<void> {
  const values = tickets(count)
  const chunk = batchSize(200)
  const typed = laser.topic(TICKETS).json(TICKET_CODEC)
  for (let start = 0; start < values.length; start += chunk) {
    await typed.publishBatch(values.slice(start, start + chunk))
  }
  await waitForProjection(laser, TICKETS, count)
}

async function spawnDesk(
  laser: Laser,
  memory: MemoryHandle,
  creditNamespace: string,
  dedupNamespace: string
): Promise<readonly AgentHandle[]> {
  const llm = new MockLlm()
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
    .spawn(laser)
  const credits = laser.kv(creditNamespace)
  const resolver = Agent.builder()
    .id(AgentId.new("resolver"))
    .listenOn(AgentTopic.Commands)
    .pollInterval(5)
    .deduplicator(new KvDeduplicator(laser, dedupNamespace))
    .handler({
      async handle(message, context): Promise<void> {
        const credit = JSON.parse(decodeUtf8(message.envelope?.body ?? message.payload)) as {
          customer: string
          cents: number
        }
        if (credit.cents >= 100) {
          const decision = await context.approvalGate(
            AgentTopic.Responses,
            utf8(`approve ${String(credit.cents)} cents for ${credit.customer}?`),
            15_000
          )
          if (decodeUtf8(decision) !== "approved") return
        }
        const key = utf8(credit.customer)
        const current = Number(decodeUtf8((await credits.get(key)) ?? utf8("0")))
        await credits
          .set(key)
          .bytes(utf8(String(current + credit.cents)))
          .send()
      }
    })
    .spawn(laser)
  const handles = [triage, specialist, resolver, approver]
  await Promise.all(handles.map((handle) => handle.ready()))
  return handles
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
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
  await registerTickets(laser)
  const count = messages(2_000)
  await ingest(laser, count)

  const memory = MemoryHandle.vector(new SimpleEmbedder())
  for (const note of NOTES) await memory.remember(utf8(note)).kind(MemoryKind.Fact).send()
  const runId = ConversationId.new()
  const creditNamespace = `concierge-credits-${runId.toString()}`
  const dedupNamespace = `concierge-dedup-${runId.toString()}`
  const handles = await spawnDesk(laser, memory, creditNamespace, dedupNamespace)
  try {
    const incident = ConversationId.new()
    const diagnosis = await laser
      .contract(routeTo(AgentId.new("triage")))
      .from(AgentId.new("orchestrator"))
      .conversation(incident)
      .payload(utf8("checkout is slow for several customers"))
      .inboxRoute(fixedCommands)
      .deadline(60_000)
      .send()
    if (diagnosis.kind !== "completed") throw new Error(`diagnosis ended as ${diagnosis.kind}`)
    const text = decodeUtf8(agentMessageBody(diagnosis.reply))
    await memory.remember(utf8(text)).kind(MemoryKind.Summary).durable().send()
    console.log(`diagnosis: ${text}`)

    for (const credit of [...CREDITS, ...CREDITS]) {
      await laser
        .agent(AgentId.new("orchestrator"))
        .send(AgentTopic.Commands, utf8(JSON.stringify(credit)), {
          conversationId: incident,
          idempotencyKey: credit.key,
          targetAgentId: AgentId.new("resolver")
        })
    }
    const expected = new Map([
      ["acme", 150],
      ["globex", 50],
      ["initech", 80]
    ])
    const deadline = Date.now() + 60_000
    while (Date.now() < deadline) {
      const actual = await Promise.all(
        [...expected].map(
          async ([customer, cents]) =>
            [
              customer,
              Number(
                decodeUtf8((await laser.kv(creditNamespace).get(utf8(customer))) ?? utf8("0"))
              ),
              cents
            ] as const
        )
      )
      if (actual.every(([, value, cents]) => value === cents)) break
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    for (const [customer, cents] of expected) {
      const actual = Number(
        decodeUtf8((await laser.kv(creditNamespace).get(utf8(customer))) ?? utf8("0"))
      )
      if (actual !== cents) throw new Error(`credit total for ${customer} is ${String(actual)}`)
    }

    const fork = laser.fork(PLAN)
    await fork.create().tables([TICKETS]).send()
    await fork.putRow(TICKETS, 0, 0n).field("status", "resolved").send()
    if (envBoolean("LASER_APPLY_PLAN", false)) await fork.promote()
    console.log(`speculative fork: ${PLAN}`)

    const rebuilt = await ConversationState.load(
      laser,
      incident,
      [AgentTopic.Commands, AgentTopic.Responses, AgentTopic.ToolCalls, AgentTopic.ToolResults],
      FULL_REPLAY,
      0,
      (total) => total + 1
    )
    if (rebuilt === 0) throw new Error("conversation state rebuild returned no messages")
    console.log(`audit records rebuilt: ${String(rebuilt)}`)
  } finally {
    for (const handle of [...handles].reverse()) await handle.shutdown()
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
