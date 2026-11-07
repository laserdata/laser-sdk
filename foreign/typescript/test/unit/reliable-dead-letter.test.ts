import assert from "node:assert/strict"
import { test } from "node:test"
import { ReliableConsumer, type AgentMiddleware } from "../../src/agent/reliable-consumer.js"
import { CancelledError, ConfigError, TransportError } from "../../src/client/errors.js"
import { INTERNAL_NATIVE_CONSUMER, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import type { Laser } from "../../src/client/laser.js"
import { SessionConfig } from "../../src/session.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import { AgentId, ConsumerGroupName } from "../../src/types/ids.js"
import {
  commandEnvelope,
  decodeAgentErrorBody,
  encodeAgentErrorBody,
  encodeAgentEnvelope,
  parseAgentId,
  withDeadlineMicros
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"

void test("given_a_dead_letter_publish_failing_twice_when_consuming_then_should_retry_it_in_place_and_commit", async () => {
  const harness = deliveries([expiredCommand(1n)])
  let attempts = 0
  harness.sendAgent = () => {
    attempts += 1
    return attempts < 3 ? Promise.reject(new TransportError("down", true)) : Promise.resolve()
  }
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "agent.sessions",
    agent: AgentId.new("worker"),
    shutdownGraceMs: 100
  }).run(harness.laser(), { handle: () => Promise.resolve() }, { signal: harness.stop.signal })
  assert.equal(attempts, 3)
  assert.equal(harness.committed, 1)
})

void test("given_a_dead_letter_publish_that_keeps_failing_when_consuming_then_should_stop_with_the_record_uncommitted", async () => {
  const harness = deliveries([expiredCommand(1n)])
  let attempts = 0
  harness.sendAgent = () => {
    attempts += 1
    return Promise.reject(new TransportError("down", true))
  }
  await assert.rejects(
    new ReliableConsumer({
      group: ConsumerGroupName.forAgent(AgentId.new("worker")),
      topic: "agent.sessions",
      agent: AgentId.new("worker"),
      shutdownGraceMs: 100
    }).run(harness.laser(), { handle: () => Promise.resolve() }, { signal: harness.stop.signal }),
    TransportError
  )
  assert.equal(attempts, 10)
  assert.equal(harness.committed, 0)
  assert.equal(harness.opened, 1, "the consumer stops instead of reopening and reading past it")
})

void test("given_an_expired_correlated_command_when_dead_lettered_then_should_answer_its_source_with_the_reason", async () => {
  const harness = deliveries([expiredCommand(1n)])
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "agent.sessions",
    agent: AgentId.new("worker"),
    respondOn: "agent.sessions",
    shutdownGraceMs: 100
  }).run(harness.laser(), { handle: () => Promise.resolve() }, { signal: harness.stop.signal })
  assert.equal(harness.answers.length, 1)
  const [answer] = harness.answers
  assert.ok(answer !== undefined)
  assert.equal(answer.topic, "agent.sessions")
  assert.equal(answer.target, "caller")
  assert.equal(answer.correlation, CorrelationId.fromU128(1n).toString())
  const context = "AGDX error body"
  const body = decodeAgentErrorBody(
    expectMap(decodeOne(answer.body ?? new Uint8Array(), context), context),
    context
  )
  assert.deepEqual(body.code, { kind: "known", name: "DeadlineExceeded" })
  const reason = body.detail?.get("dead_letter_reason")
  assert.ok(reason !== undefined && (reason.kind === "int" || reason.kind === "uint"))
  assert.equal(reason.value, 4n)
})

void test("given_ack_on_pickup_without_respond_on_when_building_then_should_refuse_it", () => {
  assert.throws(
    () =>
      new ReliableConsumer({
        group: ConsumerGroupName.forAgent(AgentId.new("worker")),
        topic: "agent.sessions",
        agent: AgentId.new("worker"),
        ackOnPickup: true
      }),
    ConfigError
  )
})

void test("given_a_retryable_middleware_failure_when_consuming_then_should_retry_it_instead_of_dead_lettering", async () => {
  const harness = deliveries([liveCommand(1n)])
  let calls = 0
  const middleware: AgentMiddleware = {
    beforeHandle: () => {
      calls += 1
      return calls === 1 ? Promise.reject(new TransportError("down", true)) : Promise.resolve()
    }
  }
  let handled = 0
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "agent.sessions",
    agent: AgentId.new("worker"),
    middleware: [middleware],
    retry: { maxAttempts: 3, baseDelayMs: 1 },
    shutdownGraceMs: 100
  }).run(
    harness.laser(),
    {
      handle: () => {
        handled += 1
        return Promise.resolve()
      }
    },
    { signal: harness.stop.signal }
  )
  assert.equal(handled, 1)
  assert.equal(harness.deadLetters, 0)
})

void test("given_ack_on_pickup_when_a_command_is_picked_up_then_should_address_the_ack_to_its_source", async () => {
  const harness = deliveries([liveCommand(1n)])
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "agent.sessions",
    agent: AgentId.new("worker"),
    respondOn: "agent.sessions",
    ackOnPickup: true,
    shutdownGraceMs: 100
  }).run(harness.laser(), { handle: () => Promise.resolve() }, { signal: harness.stop.signal })
  assert.equal(harness.acks.length, 1)
  assert.equal(harness.acks[0]?.target, "caller")
})

interface Recorded {
  topic: string
  correlation?: string
  target?: string
  body?: Uint8Array
}

interface Harness {
  readonly stop: AbortController
  committed: number
  opened: number
  deadLetters: number
  readonly answers: Recorded[]
  readonly acks: Recorded[]
  sendAgent: () => Promise<void>
  laser(): Laser
}

function expiredCommand(id: bigint): Uint8Array {
  return command(id, 1n)
}

function liveCommand(id: bigint): Uint8Array {
  return command(id, undefined)
}

function command(id: bigint, deadlineMicros: bigint | undefined): Uint8Array {
  const envelope = commandEnvelope(
    RecordId.fromU128(id),
    ConversationId.fromU128(9n),
    parseAgentId("caller"),
    CorrelationId.fromU128(id),
    new TextEncoder().encode("work")
  )
  return encodeNamed(
    encodeAgentEnvelope(
      deadlineMicros === undefined ? envelope : withDeadlineMicros(envelope, deadlineMicros)
    )
  )
}

// A one-partition consumer that yields `payloads` once each, then ends the run.
function deliveries(payloads: readonly Uint8Array[]): Harness {
  const queue = payloads.map(
    (payload, offset) =>
      ({
        partitionId: 0,
        position: { partitionId: 0, offset: BigInt(offset) },
        timestampMicros: BigInt(Date.now()) * 1000n,
        payload,
        headers: new Map<string, HeaderValue>([
          [AGENT_VERSION, { kind: "uint32", value: AGENT_OP_VERSION }],
          [CONTENT_TYPE, { kind: "uint8", value: contentTypeCode(ContentType.Raw) }]
        ])
      }) as unknown as ConsumerMessage
  )
  const harness: Harness = {
    stop: new AbortController(),
    committed: 0,
    opened: 0,
    deadLetters: 0,
    answers: [],
    acks: [],
    sendAgent: () => Promise.resolve(),
    laser: () => laser
  }
  const consumer = {
    nextWithin: (_waitMs: number, options?: { signal?: AbortSignal }) => {
      const next = queue.shift()
      if (next !== undefined) return Promise.resolve(next)
      harness.stop.abort()
      return new Promise<ConsumerMessage>((_resolve, reject) => {
        const abort = (): void => {
          reject(new CancelledError("stopped"))
        }
        options?.signal?.addEventListener("abort", abort, { once: true })
        if (options?.signal?.aborted === true) abort()
      })
    },
    commit: () => {
      harness.committed += 1
      return Promise.resolve()
    },
    shutdown: () => Promise.resolve()
  } as unknown as Consumer
  const recorder = (topic: string, list: Recorded[]) => {
    const entry: Recorded = { topic }
    const send: Record<string, unknown> = {
      withTarget: (target: { asStr: () => string }) => {
        entry.target = target.asStr()
        return send
      },
      withCorrelation: (correlation: CorrelationId) => {
        entry.correlation = correlation.toString()
        return send
      },
      withTaskState: () => send,
      withAncestry: () => send,
      withCause: () => send,
      signedBy: () => send,
      send: () => {
        list.push(entry)
        return Promise.resolve(undefined)
      }
    }
    return { entry, send }
  }
  const laser = {
    defaultStream: "agents",
    topic: () => ({
      consumerGroup: () => ({
        [INTERNAL_NATIVE_CONSUMER]: () => {
          harness.opened += 1
          return Promise.resolve(consumer)
        }
      })
    }),
    capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" }),
    sessions: (config?: SessionConfig) => ({
      config: config ?? new SessionConfig(),
      indexesSessions: () => Promise.resolve(false)
    }),
    sendAgent: () => {
      harness.deadLetters += 1
      return harness.sendAgent()
    },
    agdx: (topic: string) => ({
      fail: (correlation: CorrelationId, body: { readonly code: unknown }) => {
        const { entry, send } = recorder(topic, harness.answers)
        entry.correlation = correlation.toString()
        entry.body = encodeNamedErrorBody(body)
        return send
      },
      status: () => recorder(topic, harness.acks).send
    }),
    [INTERNAL_TRANSPORT]: () => ({
      findTopicPartitionCount: () => Promise.resolve(undefined),
      resolveStreamTopicIds: () => Promise.resolve({ streamId: 1, topicId: 2 })
    })
  } as unknown as Laser
  return harness
}

// The error body as the AGDX producer encodes it.
function encodeNamedErrorBody(body: unknown): Uint8Array {
  return encodeNamed(encodeAgentErrorBody(body as Parameters<typeof encodeAgentErrorBody>[0]))
}
