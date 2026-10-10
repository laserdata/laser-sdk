import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  type AgentEnvelope,
  type AgentKind,
  METADATA_ROLE,
  type TaskState,
  TaskStateName,
  encodeSessionEnd,
  encodeSessionStart,
  encodeSessionTransition,
  estimateTokens,
  parseAgentId
} from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  type HandledOperations,
  type SessionRecord,
  addresseeFilter,
  broadcastFilter,
  classify,
  classifyGeneric,
  displayType
} from "../../src/wire/dispatch.js"
import { consumerFilterDigest } from "../../src/wire/filter.js"
import { ConversationId } from "../../src/wire/ids.js"

interface Case {
  readonly name: string
  readonly topic: string
  readonly record: {
    readonly kind?: string
    readonly operation?: string
    readonly task_state?: string
    readonly body?: string
    readonly target?: string
    readonly role?: string
    readonly memory?: "item" | "forget" | "feedback"
    readonly other?: string
  }
  readonly handled?: "any" | readonly string[]
  readonly display: string
  readonly dispatch?: string
}

interface Table {
  readonly agent: string
  readonly cases: readonly Case[]
  readonly filters: readonly { readonly agent?: string; readonly digest: string }[]
  readonly generic: readonly {
    readonly name: string
    readonly to: string | null
    readonly cause: boolean
    readonly correlation: boolean
    readonly topic: string
    readonly dispatch: string
  }[]
}

const TABLE = path.resolve(process.cwd(), "../../wire/fixtures/dispatch_cases.json")

function taskState(word: string): TaskState {
  const name = (Object.keys(TaskStateName) as (keyof typeof TaskStateName)[]).find(
    (key) => key.toLowerCase() === word
  )
  assert.ok(name !== undefined, word)
  return { kind: "known", name }
}

function envelope(record: Case["record"]): AgentEnvelope {
  const planner = parseAgentId("planner")
  const body =
    record.body === "start"
      ? encodeNamed(
          encodeSessionStart({
            agent: planner,
            sdk: { language: "rust", version: "0.7.0" },
            tags: []
          })
        )
      : record.body === "transition"
        ? encodeNamed(encodeSessionTransition({}))
        : record.body === "end"
          ? encodeNamed(encodeSessionEnd({}))
          : new TextEncoder().encode("{}")
  return {
    kind: record.kind as AgentKind,
    conversation: ConversationId.fromU128(2n),
    source: planner,
    last: false,
    mustUnderstand: 0n,
    body,
    ...(record.operation !== undefined ? { operation: record.operation } : {}),
    ...(record.task_state !== undefined ? { taskState: taskState(record.task_state) } : {}),
    ...(record.target !== undefined ? { target: parseAgentId(record.target) } : {}),
    ...(record.role !== undefined
      ? { metadata: new Map([[METADATA_ROLE, { kind: "str" as const, value: record.role }]]) }
      : {})
  }
}

function sessionRecord(record: Case["record"]): SessionRecord {
  if (record.kind !== undefined) return { kind: "envelope", envelope: envelope(record) }
  if (record.memory === "item")
    return {
      kind: "memory",
      record: {
        kind: "item",
        id: "01KWM3K3XEP3NP5TN850J17YBP",
        memoryKind: "fact",
        body: new Uint8Array()
      }
    }
  if (record.memory === "forget")
    return { kind: "memory", record: { kind: "forget", target: "01KWM3K3XEP3NP5TN850J17YBP" } }
  if (record.memory === "feedback")
    return {
      kind: "memory",
      record: { kind: "feedback", target: "01KWM3K3XEP3NP5TN850J17YBP", weight: 1 }
    }
  if (record.other === "dead_letter") return { kind: "dead_letter" }
  if (record.other === "journal") return { kind: "journal" }
  if (record.other === "kv_mutation") return { kind: "kv_mutation" }
  if (record.other === "graph_mutation") return { kind: "graph_mutation" }
  return { kind: "undecodable" }
}

function hex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex")
}

void test("given_the_shared_table_when_records_are_classified_then_should_match_every_row", async () => {
  const table = JSON.parse(await readFile(TABLE, "utf8")) as Table
  const me = parseAgentId(table.agent)
  assert.equal(table.cases.length, 50)
  for (const row of table.cases) {
    const record = sessionRecord(row.record)
    assert.equal(displayType(record, row.topic), row.display, row.name)
    if (record.kind === "envelope") {
      const handled: HandledOperations = row.handled ?? "any"
      assert.equal(classify(record.envelope, row.topic, me, handled), row.dispatch, row.name)
    }
  }
  for (const row of table.generic) {
    assert.equal(
      classifyGeneric(row.to ?? undefined, row.cause, row.correlation, row.topic, me),
      row.dispatch,
      row.name
    )
  }
  for (const filter of table.filters) {
    const built =
      filter.agent === undefined ? broadcastFilter() : addresseeFilter(parseAgentId(filter.agent))
    assert.equal(hex(consumerFilterDigest(built)), filter.digest)
  }
})

void test("given_the_shared_estimates_when_computed_then_should_match_every_row", async () => {
  const fixture = path.resolve(process.cwd(), "../../wire/fixtures/token_estimates.json")
  const cases = JSON.parse(await readFile(fixture, "utf8")) as readonly {
    readonly bytes: number
    readonly tokens: number
  }[]
  assert.ok(cases.length > 0)
  for (const { bytes, tokens } of cases) {
    assert.equal(estimateTokens(bytes), BigInt(tokens), `${String(bytes)} bytes`)
  }
  assert.throws(() => estimateTokens(-1))
})
