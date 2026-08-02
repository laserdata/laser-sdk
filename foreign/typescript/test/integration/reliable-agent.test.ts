import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { Agent } from "../../src/agent/builder.js"
import type {
  AgentMessage,
  AgentMiddleware,
  DeadLetterSink,
  HandlerResult
} from "../../src/agent/reliable-consumer.js"
import { INTERNAL_TRANSPORT, Laser } from "../../src/client/laser.js"
import { RejectedError, TimeoutError, TransportError } from "../../src/client/errors.js"
import type { IggyHeaderValue } from "../../src/iggy/apache-iggy.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { KeyRecord, KeyRegistry, SigningKey } from "../../src/signing.js"
import type { ConsumedMessage } from "../../src/stream/consumer.js"
import {
  AgentKind,
  TaskStateName,
  commandEnvelope,
  decodeAgentDeadLetter,
  decodeAgentEnvelope,
  encodeAgentEnvelope,
  parseAgentId,
  type AgentDeadLetter,
  type AgentEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { contentTypeCode, ContentType } from "../../src/wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function sendAgentFixture(laser: Laser, payload: Uint8Array): Promise<void> {
  const headers = new Map<string, IggyHeaderValue>([
    [AGENT_VERSION, { kind: "uint32", value: AGENT_OP_VERSION }],
    [CONTENT_TYPE, { kind: "uint8", value: contentTypeCode(ContentType.Cbor) }]
  ])
  await laser.topic(AgentTopic.Commands).send(payload, { headers })
}

void test("given_a_transient_handler_failure_when_retried_then_should_reply_and_commit_after_success", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    let attempts = 0
    let before = 0
    const after: HandlerResult[] = []
    const middleware: AgentMiddleware = {
      beforeHandle: () => {
        before += 1
        return Promise.resolve()
      },
      afterHandle: (_message, result) => {
        after.push(result)
        return Promise.resolve()
      }
    }
    const handle = Agent.builder()
      .id(AgentId.new("retry-worker"))
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .handler({
        async handle(_message, context): Promise<void> {
          attempts += 1
          if (attempts === 1) throw new TransportError("temporary", true)
          await context.respond(new TextEncoder().encode("complete"))
        }
      })
      .retry({ maxAttempts: 2, baseDelayMs: 1 })
      .middleware(middleware)
      .spawn(laser)
    await handle.ready()

    const reply = await laser
      .agent(AgentId.new("requester"))
      .ask(
        AgentTopic.Commands,
        AgentTopic.Responses,
        new TextEncoder().encode("work"),
        { conversationId: ConversationId.new() },
        2_000
      )
    assert.equal(new TextDecoder().decode(reply.payload), "complete")
    assert.equal(attempts, 2)
    assert.equal(before, 1)
    assert.deepEqual(
      after.map((result) => result.kind),
      ["error", "ok"]
    )
    await handle.shutdown()

    const rejoined = await laser.topic(AgentTopic.Commands).consumerGroup("retry-worker", {
      autoCommit: false,
      startFrom: { kind: "next" }
    })
    try {
      assert.equal(await rejoined.nextWithin(100), null)
    } finally {
      await rejoined.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_invalid_and_unmet_agdx_records_when_consumed_then_should_reject_before_dispatch", async () => {
  const invalidFixtures = [
    "agent_invalid_chunk_late_deadline.bin",
    "agent_invalid_chunk_no_sequence.bin",
    "agent_invalid_chunk_open_no_operation.bin",
    "agent_invalid_command_no_correlation.bin",
    "agent_invalid_error_last.bin",
    "agent_invalid_event_task_state.bin",
    "agent_invalid_response_channel.bin",
    "agent_invalid_status_bad_operation.bin",
    "agent_invalid_status_no_operation.bin"
  ] as const
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const handled: AgentEnvelope[] = []
    const deadLetters: AgentDeadLetter[] = []
    let middlewareCalls = 0
    let publishFailures = 0
    const middleware: AgentMiddleware = {
      beforeHandle: () => {
        middlewareCalls += 1
        return Promise.resolve()
      }
    }
    const sink: DeadLetterSink = {
      onDeadLetter(_message, capsule, publishError): Promise<void> {
        deadLetters.push(capsule)
        if (publishError !== undefined) publishFailures += 1
        return Promise.resolve()
      }
    }
    const handler = {
      handle(message: AgentMessage): Promise<void> {
        if (message.envelope !== undefined) handled.push(message.envelope)
        return Promise.resolve()
      }
    }
    const first = Agent.builder()
      .id(AgentId.new("strict-worker"))
      .listenOn(AgentTopic.Commands)
      .handler(handler)
      .middleware(middleware)
      .deadLetterSink(sink)
      .spawn(laser)
    await first.ready()

    for (const name of invalidFixtures) await sendAgentFixture(laser, await readFixture(name))
    const requiredBytes = await readFixture("agent_must_understand.bin")
    const required = decodeAgentEnvelope(
      expectMap(decodeOne(requiredBytes, "agent_must_understand.bin"), "agent_must_understand.bin"),
      "agent_must_understand.bin"
    )
    assert.notEqual(required.mustUnderstand, 0n)
    await sendAgentFixture(laser, requiredBytes)
    const expected = invalidFixtures.length + 1
    for (let attempt = 0; attempt < 100 && deadLetters.length < expected; attempt += 1) {
      await delay(10)
    }

    assert.equal(deadLetters.length, invalidFixtures.length + 1)
    assert.equal(handled.length, 0)
    assert.equal(middlewareCalls, 0)
    assert.equal(publishFailures, 0)
    await first.shutdown()

    const second = Agent.builder()
      .id(AgentId.new("strict-worker"))
      .listenOn(AgentTopic.Commands)
      .handler(handler)
      .understoodFeatures(required.mustUnderstand)
      .spawn(laser)
    await second.ready()
    await sendAgentFixture(laser, requiredBytes)
    const dispatched = () => handled.length >= 1
    for (let attempt = 0; attempt < 100 && !dispatched(); attempt += 1) await delay(10)
    assert.equal(handled.length, 1)
    await second.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_periodic_memory_consolidation_when_an_agent_runs_then_should_tick_until_shutdown", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    let consolidations = 0
    const handle = Agent.builder()
      .id(AgentId.new("consolidating-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({ handle: () => Promise.resolve() })
      .consolidateEvery(10)
      .consolidator({
        consolidate(scope): Promise<void> {
          assert.deepEqual(scope, {})
          consolidations += 1
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()
    for (let attempt = 0; attempt < 50 && consolidations < 2; attempt += 1) await delay(10)
    assert.ok(consolidations >= 2)
    await handle.shutdown()
    const stoppedAt = consolidations
    await delay(30)
    assert.equal(consolidations, stoppedAt)
  } finally {
    await laser.close()
  }
})

void test("given_a_permanent_handler_rejection_when_consumed_then_should_publish_a_verbatim_dlq_capsule", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const dlq = await laser.topic(AgentTopic.Dlq).replay()
    const observed: {
      capsule?: AgentDeadLetter
      publishError?: Error
      calls: number
    } = { calls: 0 }
    const sink: DeadLetterSink = {
      onDeadLetter(_message, capsule, publishError): Promise<void> {
        observed.calls += 1
        observed.capsule = capsule
        if (publishError !== undefined) observed.publishError = publishError
        return Promise.resolve()
      }
    }
    const handle = Agent.builder()
      .id(AgentId.new("rejecting-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          return Promise.reject(new RejectedError("policy refused"))
        }
      })
      .deadLetterSink(sink)
      .spawn(laser)
    await handle.ready()

    const payload = new TextEncoder().encode("poison-body")
    await laser.agent(AgentId.new("requester")).send(AgentTopic.Commands, payload, {
      conversationId: ConversationId.new(),
      idempotencyKey: "poison-1"
    })
    let record
    for (let attempt = 0; attempt < 80 && record === undefined; attempt += 1) {
      record = (await dlq.poll())[0]
      if (record === undefined) await delay(20)
    }
    assert.ok(record !== undefined)
    const context = "reliable consumer DLQ"
    const capsule = decodeAgentDeadLetter(
      expectMap(decodeOne(record.payload, context), context),
      context
    )
    assert.deepEqual(record.headers.get(CONTENT_TYPE), { kind: "uint8", value: 3 })
    assert.equal(capsule.reason.kind, "known")
    assert.equal(capsule.reason.name, "Rejected")
    assert.equal(capsule.attempts, 1)
    assert.equal(capsule.detail, "policy refused")
    assert.deepEqual(capsule.payload, payload)
    const [streamDetails, topicDetails] = await Promise.all([
      laser.iggyClient.stream.get({ streamId: stream }),
      laser.iggyClient.topic.get({ streamId: stream, topicId: AgentTopic.Commands })
    ])
    assert.ok(streamDetails !== null)
    assert.ok(topicDetails !== null)
    assert.equal(capsule.source.streamId, streamDetails.id)
    assert.equal(capsule.source.topicId, topicDetails.id)
    assert.equal(observed.calls, 1)
    assert.equal(observed.publishError, undefined)
    assert.deepEqual(observed.capsule, capsule)
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_partition_lanes_when_one_blocks_then_should_run_other_partitions_and_drain_in_order", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(2)
    const events: string[] = []
    let releaseSlow = (): void => undefined
    const slow = new Promise<void>((resolve) => {
      releaseSlow = resolve
    })
    const handle = Agent.builder()
      .id(AgentId.new("parallel-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        async handle(message): Promise<void> {
          const body = new TextDecoder().decode(message.payload)
          events.push(`start-${String(message.id.partitionId)}-${body}`)
          if (body === "slow") await slow
          events.push(`end-${String(message.id.partitionId)}-${body}`)
        }
      })
      .concurrency({ kind: "serial-per-partition", maxPartitions: 2 })
      .spawn(laser)
    await handle.ready()

    const transport = laser[INTERNAL_TRANSPORT]()
    const headers0 = encodeProvenanceHeaders({ conversationId: ConversationId.new() })
    const headers1 = encodeProvenanceHeaders({ conversationId: ConversationId.new() })
    await transport.sendMessagesWithHeaders(
      stream,
      AgentTopic.Commands,
      [
        { payload: new TextEncoder().encode("slow"), headers: headers0 },
        { payload: new TextEncoder().encode("after"), headers: headers0 }
      ],
      undefined,
      0
    )
    await transport.sendMessageWithHeaders(
      stream,
      AgentTopic.Commands,
      new TextEncoder().encode("fast"),
      headers1,
      undefined,
      1
    )

    for (let attempt = 0; attempt < 80 && !events.includes("end-1-fast"); attempt += 1) {
      await delay(10)
    }
    assert.ok(events.includes("end-1-fast"))
    assert.ok(!events.includes("start-0-after"))

    await delay(30)
    let drained = false
    const shutdown = handle.shutdown().then(() => {
      drained = true
    })
    await delay(30)
    assert.equal(drained, false)
    releaseSlow()
    await shutdown
    assert.deepEqual(
      events.filter((event) => event.endsWith("slow") || event.endsWith("after")),
      ["start-0-slow", "end-0-slow", "start-0-after", "end-0-after"]
    )
  } finally {
    await laser.close()
  }
})

void test("given_sustained_partition_churn_when_consumed_then_should_bound_concurrency_and_preserve_lane_order", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    const partitions = 32
    const perPartition = 8
    const concurrency = 4
    await laser.bootstrap(partitions)
    let active = 0
    let maxActive = 0
    const received = new Map<number, number[]>()
    const handle = Agent.builder()
      .id(AgentId.new("churn-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        async handle(message): Promise<void> {
          active += 1
          maxActive = Math.max(maxActive, active)
          const sequence = Number(new TextDecoder().decode(message.payload).split(":")[1])
          await delay(1)
          const lane = received.get(message.id.partitionId) ?? []
          lane.push(sequence)
          received.set(message.id.partitionId, lane)
          active -= 1
        }
      })
      .concurrency({ kind: "serial-per-partition", maxPartitions: concurrency })
      .spawn(laser)
    await handle.ready()

    const transport = laser[INTERNAL_TRANSPORT]()
    for (let partition = 0; partition < partitions; partition += 1) {
      const headers = encodeProvenanceHeaders({ conversationId: ConversationId.new() })
      await transport.sendMessagesWithHeaders(
        stream,
        AgentTopic.Commands,
        Array.from({ length: perPartition }, (_, sequence) => ({
          payload: new TextEncoder().encode(`${String(partition)}:${String(sequence)}`),
          headers
        })),
        undefined,
        partition
      )
    }

    const expected = partitions * perPartition
    const allReceived = () =>
      [...received.values()].reduce((sum, lane) => sum + lane.length, 0) >= expected
    for (let attempt = 0; attempt < 1_000 && !allReceived(); attempt += 1) {
      await delay(10)
    }
    await handle.shutdown()
    assert.ok(maxActive <= concurrency, `observed ${String(maxActive)} active lanes`)
    assert.equal(
      [...received.values()].reduce((sum, lane) => sum + lane.length, 0),
      expected
    )
    for (let partition = 0; partition < partitions; partition += 1) {
      assert.deepEqual(
        received.get(partition),
        Array.from({ length: perPartition }, (_, sequence) => sequence)
      )
    }
  } finally {
    await laser.close()
  }
})

void test("given_ack_on_pickup_when_an_agdx_command_arrives_then_should_emit_working_before_handler_completion", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const statuses = await laser.topic(AgentTopic.Responses).replay()
    let releaseHandler = (): void => undefined
    const handlerGate = new Promise<void>((resolve) => {
      releaseHandler = resolve
    })
    const handle = Agent.builder()
      .id(AgentId.new("contract-worker"))
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .ackOnPickup()
      .handler({
        handle(): Promise<void> {
          return handlerGate
        }
      })
      .spawn(laser)
    await handle.ready()

    await laser
      .agdx(AgentTopic.Commands, AgentId.new("requester"), ConversationId.new())
      .command(CorrelationId.fromU128(7n), new TextEncoder().encode("contract"))
      .withTarget(AgentId.new("contract-worker"))
      .send()
    let status
    for (let attempt = 0; attempt < 80 && status === undefined; attempt += 1) {
      status = (await statuses.poll())[0]
      if (status === undefined) await delay(10)
    }
    assert.ok(status !== undefined)
    const context = "pickup status"
    const envelope = decodeAgentEnvelope(
      expectMap(decodeOne(status.payload, context), context),
      context
    )
    assert.equal(envelope.kind, AgentKind.Status)
    assert.equal(envelope.correlation?.asU128(), 7n)
    assert.equal(envelope.taskState?.kind, "known")
    assert.equal(TaskStateName[envelope.taskState.name], TaskStateName.Working)

    releaseHandler()
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_missing_dlq_topic_when_publish_fails_then_should_redeliver_before_commit", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.stream(stream).ensure()
    await laser.topic(AgentTopic.Commands).ensure()
    let published: Error | undefined
    let sinkCalls = 0
    const handle = Agent.builder()
      .id(AgentId.new("dlq-failure-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          return Promise.reject(new RejectedError("reject"))
        }
      })
      .deadLetterSink({
        onDeadLetter(_message, _capsule, publishError): Promise<void> {
          sinkCalls += 1
          published = publishError
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("poison"), {
      conversationId: ConversationId.new()
    })
    for (let attempt = 0; attempt < 80 && sinkCalls === 0; attempt += 1) await delay(10)
    assert.equal(sinkCalls, 1)
    assert.ok(published instanceof TransportError)
    await assert.rejects(handle.join(), TransportError)

    await laser.topic(AgentTopic.Dlq).ensure()
    const dlq = await laser.topic(AgentTopic.Dlq).replay()
    let durableSinkCalls = 0
    const replacement = Agent.builder()
      .id(AgentId.new("dlq-failure-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          return Promise.reject(new RejectedError("reject"))
        }
      })
      .deadLetterSink({
        onDeadLetter(_message, _capsule, publishError): Promise<void> {
          assert.equal(publishError, undefined)
          durableSinkCalls += 1
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await replacement.ready()
    let record
    for (let attempt = 0; attempt < 80 && record === undefined; attempt += 1) {
      record = (await dlq.poll())[0]
      if (record === undefined) await delay(10)
    }
    assert.ok(record !== undefined)
    assert.equal(durableSinkCalls, 1)
    await replacement.shutdown()

    const rejoined = await laser.topic(AgentTopic.Commands).consumerGroup("dlq-failure-worker", {
      autoCommit: false,
      startFrom: { kind: "next" }
    })
    try {
      assert.equal(await rejoined.nextWithin(100), null)
    } finally {
      await rejoined.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_retryable_handler_that_never_succeeds_when_consumed_then_should_exhaust_and_commit_to_dlq", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const dlq = await laser.topic(AgentTopic.Dlq).replay()
    let attempts = 0
    const handle = Agent.builder()
      .id(AgentId.new("exhausted-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          attempts += 1
          return Promise.reject(new TransportError("still unavailable", true))
        }
      })
      .retry({ maxAttempts: 3, baseDelayMs: 1 })
      .spawn(laser)
    await handle.ready()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("retry-me"), {
      conversationId: ConversationId.new()
    })
    let record
    for (let attempt = 0; attempt < 80 && record === undefined; attempt += 1) {
      record = (await dlq.poll())[0]
      if (record === undefined) await delay(10)
    }
    assert.ok(record !== undefined)
    const context = "retry exhausted DLQ"
    const capsule = decodeAgentDeadLetter(
      expectMap(decodeOne(record.payload, context), context),
      context
    )
    assert.equal(capsule.reason.kind, "known")
    assert.equal(capsule.reason.name, "RetryExhausted")
    assert.equal(capsule.attempts, 3)
    assert.equal(attempts, 3)
    await handle.shutdown()

    const rejoined = await laser.topic(AgentTopic.Commands).consumerGroup("exhausted-worker", {
      autoCommit: false
    })
    try {
      assert.equal(await rejoined.nextWithin(100), null)
    } finally {
      await rejoined.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_an_inflight_message_when_hard_aborted_then_should_redeliver_to_the_replacement", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const firstEvents: string[] = []
    let releaseFirst = (): void => undefined
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const first = Agent.builder()
      .id(AgentId.new("abort-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          firstEvents.push("started")
          return firstGate
        }
      })
      .spawn(laser)
    await first.ready()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("uncommitted"), {
      conversationId: ConversationId.new()
    })
    for (let attempt = 0; attempt < 80 && !firstEvents.includes("started"); attempt += 1) {
      await delay(10)
    }
    assert.deepEqual(firstEvents, ["started"])
    first.abort()
    await first.join()

    let replacementBody: string | undefined
    const replacement = Agent.builder()
      .id(AgentId.new("abort-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(message): Promise<void> {
          replacementBody = new TextDecoder().decode(message.payload)
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await replacement.ready()
    for (let attempt = 0; attempt < 80 && replacementBody === undefined; attempt += 1) {
      await delay(10)
    }
    assert.equal(replacementBody, "uncommitted")
    releaseFirst()
    await replacement.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_committed_history_when_a_warmed_agent_restarts_then_should_suppress_a_republished_key", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    let initialCalls = 0
    const first = Agent.builder()
      .id(AgentId.new("warm-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(): Promise<void> {
          initialCalls += 1
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await first.ready()
    const conversationId = ConversationId.new()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("first"), {
      conversationId,
      idempotencyKey: "stable-key"
    })
    for (let attempt = 0; attempt < 80 && initialCalls === 0; attempt += 1) await delay(10)
    assert.equal(initialCalls, 1)
    await first.shutdown()

    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("duplicate"), {
      conversationId,
      idempotencyKey: "stable-key"
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("new"), {
      conversationId,
      idempotencyKey: "new-key"
    })
    const restartedBodies: string[] = []
    const restarted = Agent.builder()
      .id(AgentId.new("warm-worker"))
      .listenOn(AgentTopic.Commands)
      .warmDedup()
      .handler({
        handle(message): Promise<void> {
          restartedBodies.push(new TextDecoder().decode(message.payload))
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await restarted.ready()
    for (let attempt = 0; attempt < 80 && restartedBodies.length === 0; attempt += 1) {
      await delay(10)
    }
    assert.deepEqual(restartedBodies, ["new"])
    await restarted.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_lost_group_membership_when_polling_then_should_rejoin_and_continue", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    let body: string | undefined
    const handle = Agent.builder()
      .id(AgentId.new("rejoin-worker"))
      .listenOn(AgentTopic.Commands)
      .pollInterval(5)
      .handler({
        handle(message): Promise<void> {
          body = new TextDecoder().decode(message.payload)
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()
    await laser.iggyClient.group.leave({
      streamId: stream,
      topicId: AgentTopic.Commands,
      groupId: "rejoin-worker"
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("after-rejoin"), {
      conversationId: ConversationId.new()
    })
    for (let attempt = 0; attempt < 160 && body === undefined; attempt += 1) await delay(10)
    assert.equal(body, "after-rejoin")
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_deadline_fence_and_dedup_records_when_consumed_then_should_apply_each_gate_before_effects", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const handled: string[] = []
    const dlq = await laser.topic(AgentTopic.Dlq).replay()
    const handle = Agent.builder()
      .id(AgentId.new("gated-worker"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(message): Promise<void> {
          handled.push(new TextDecoder().decode(message.payload))
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()
    const fencedConversation = ConversationId.new()
    const duplicateConversation = ConversationId.new()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("expired"), {
      conversationId: ConversationId.new(),
      deadlineMicros: BigInt(Date.now()) * 1_000n - 1n
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("fresh-fence"), {
      conversationId: fencedConversation,
      fenceToken: 2n
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("stale-fence"), {
      conversationId: fencedConversation,
      fenceToken: 1n
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("unique"), {
      conversationId: duplicateConversation,
      idempotencyKey: "same"
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("duplicate"), {
      conversationId: duplicateConversation,
      idempotencyKey: "same"
    })
    let deadlineRecord: ConsumedMessage | undefined
    const deadlinePending = () => handled.length < 2 || deadlineRecord === undefined
    for (let attempt = 0; attempt < 160 && deadlinePending(); attempt += 1) {
      deadlineRecord ??= (await dlq.poll())[0]
      await delay(10)
    }
    assert.deepEqual(handled, ["fresh-fence", "unique"])
    assert.ok(deadlineRecord !== undefined)
    const context = "deadline DLQ"
    const capsule = decodeAgentDeadLetter(
      expectMap(decodeOne(deadlineRecord.payload, context), context),
      context
    )
    assert.equal(capsule.reason.kind, "known")
    assert.equal(capsule.reason.name, "DeadlineExceeded")
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_verified_agent_when_signed_and_unsigned_commands_arrive_then_should_authenticate_reply_and_reject", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const callerKey = SigningKey.fromBytes(new Uint8Array(32).fill(7))
    const workerKey = SigningKey.fromBytes(new Uint8Array(32).fill(8))
    const callers = new KeyRegistry()
    callers.enroll("caller-principal", callerKey.verifyingKey())
    const workers = new KeyRegistry()
    workers.enroll("worker-principal", workerKey.verifyingKey())
    const handledPrincipals: string[] = []
    const handle = Agent.builder()
      .id(AgentId.new("signed-worker"))
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .verifier(callers)
      .signingKey(workerKey)
      .handler({
        async handle(message, context): Promise<void> {
          handledPrincipals.push(message.verifiedPrincipal ?? "missing")
          await context.respond(new TextEncoder().encode("signed-response"))
        }
      })
      .spawn(laser)
    await handle.ready()
    const responses = await laser.topic(AgentTopic.Responses).replay()
    const dlq = await laser.topic(AgentTopic.Dlq).replay()
    const conversation = ConversationId.new()
    await laser
      .agdx(AgentTopic.Commands, AgentId.new("caller"), conversation)
      .command(CorrelationId.fromU128(91n), new TextEncoder().encode("signed-command"))
      .withTarget(AgentId.new("signed-worker"))
      .signedBy(callerKey)
      .send()
    await laser
      .agdx(AgentTopic.Commands, AgentId.new("caller"), conversation)
      .command(CorrelationId.fromU128(92n), new TextEncoder().encode("unsigned-command"))
      .withTarget(AgentId.new("signed-worker"))
      .send()

    let responseEnvelope: AgentEnvelope | undefined
    let rejected: AgentDeadLetter | undefined
    const responsePending = () => responseEnvelope === undefined || rejected === undefined
    for (let attempt = 0; attempt < 160 && responsePending(); attempt += 1) {
      const response = (await responses.poll())[0]
      if (response !== undefined) {
        const context = "signed response"
        responseEnvelope = decodeAgentEnvelope(
          expectMap(decodeOne(response.payload, context), context),
          context
        )
      }
      const deadLetter = (await dlq.poll())[0]
      if (deadLetter !== undefined) {
        const context = "signature DLQ"
        rejected = decodeAgentDeadLetter(
          expectMap(decodeOne(deadLetter.payload, context), context),
          context
        )
      }
      await delay(10)
    }
    assert.deepEqual(handledPrincipals, ["caller-principal"])
    assert.ok(responseEnvelope !== undefined)
    assert.equal(new TextDecoder().decode(responseEnvelope.body), "signed-response")
    assert.equal(workers.verify(responseEnvelope), "worker-principal")
    assert.ok(rejected !== undefined)
    assert.equal(rejected.reason.kind, "known")
    assert.equal(rejected.reason.name, "Rejected")
    assert.equal(rejected.detail, "signature verification failed")
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

// A signed command encoded exactly as the AGDX producer would publish it, so a
// test can vary the broker headers independently of the signed context.
function signedCommand(key: SigningKey, conversation: bigint, correlation: bigint): Uint8Array {
  const envelope = commandEnvelope(
    RecordId.fromU128(correlation | 0x1000n),
    WireConversationId.fromU128(conversation),
    parseAgentId("caller"),
    CorrelationId.fromU128(correlation),
    new TextEncoder().encode("signed-command")
  )
  const signature = key.signWithContext(envelope, {
    contentType: contentTypeCode(ContentType.Cbor),
    agentVersion: AGENT_OP_VERSION
  })
  return encodeNamed(encodeAgentEnvelope({ ...envelope, signature }))
}

async function publishWithHeaders(
  laser: Laser,
  payload: Uint8Array,
  contentType: number | undefined,
  agentVersion: number
): Promise<void> {
  const headers = new Map<string, IggyHeaderValue>([
    [AGENT_VERSION, { kind: "uint32", value: agentVersion }],
    ...(contentType !== undefined
      ? ([[CONTENT_TYPE, { kind: "uint8", value: contentType }]] as const)
      : [])
  ])
  await laser.topic(AgentTopic.Commands).send(payload, { headers })
}

void test("given_a_verified_agent_when_broker_headers_are_mutated_then_should_dead_letter_before_dispatch", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const callerKey = SigningKey.fromBytes(new Uint8Array(32).fill(21))
    const callers = new KeyRegistry()
    callers.enroll("caller", callerKey.verifyingKey())
    const handledPrincipals: (string | undefined)[] = []
    const deadLetters: AgentDeadLetter[] = []
    let middlewareCalls = 0
    const middleware: AgentMiddleware = {
      beforeHandle: () => {
        middlewareCalls += 1
        return Promise.resolve()
      }
    }
    const handle = Agent.builder()
      .id(AgentId.new("header-strict"))
      .listenOn(AgentTopic.Commands)
      .verifier(callers)
      .handler({
        handle(message: AgentMessage): Promise<void> {
          handledPrincipals.push(message.verifiedPrincipal)
          return Promise.resolve()
        }
      })
      .middleware(middleware)
      .deadLetterSink({
        onDeadLetter(_message, capsule): Promise<void> {
          deadLetters.push(capsule)
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()

    // The signature binds `agdx.ct = cbor` and the current `agdx.av`. Republish
    // the same signed bytes under a flipped content type, a stripped content
    // type, and a flipped wire version: each must dead-letter before dispatch.
    const conversation = 0x01903c1faa000000000000000000_0101n
    const payload = signedCommand(callerKey, conversation, 0x0201n)
    await publishWithHeaders(laser, payload, contentTypeCode(ContentType.Json), AGENT_OP_VERSION)
    await publishWithHeaders(laser, payload, undefined, AGENT_OP_VERSION)
    await publishWithHeaders(
      laser,
      payload,
      contentTypeCode(ContentType.Cbor),
      AGENT_OP_VERSION + 1
    )
    for (let attempt = 0; attempt < 160 && deadLetters.length < 3; attempt += 1) await delay(10)

    assert.equal(deadLetters.length, 3)
    assert.equal(handledPrincipals.length, 0)
    assert.equal(middlewareCalls, 0)
    // The two header mutations fail context binding, and the flipped wire
    // version fails decode before verification.
    const rejected = deadLetters.filter(
      (capsule) =>
        capsule.reason.kind === "known" &&
        capsule.reason.name === "Rejected" &&
        capsule.detail === "signature verification failed"
    )
    assert.equal(rejected.length, 2)
    const undecodable = deadLetters.filter(
      (capsule) => capsule.reason.kind === "known" && capsule.reason.name === "DecodeFailed"
    )
    assert.equal(undecodable.length, 1)

    // The untouched record still verifies: the observed headers match the
    // signed context and the handler sees the enrolled principal.
    await publishWithHeaders(
      laser,
      signedCommand(callerKey, conversation, 0x0202n),
      contentTypeCode(ContentType.Cbor),
      AGENT_OP_VERSION
    )
    const dispatched = () => handledPrincipals.length >= 1
    for (let attempt = 0; attempt < 160 && !dispatched(); attempt += 1) {
      await delay(10)
    }
    assert.deepEqual(handledPrincipals, ["caller"])
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_lifecycle_bound_keys_when_verified_at_the_broker_timestamp_then_should_gate_by_validity", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const now = BigInt(Date.now()) * 1000n
    const hour = 3_600_000_000n
    const validKey = SigningKey.fromBytes(new Uint8Array(32).fill(31))
    const futureKey = SigningKey.fromBytes(new Uint8Array(32).fill(32))
    const expiredKey = SigningKey.fromBytes(new Uint8Array(32).fill(33))
    const revokedKey = SigningKey.fromBytes(new Uint8Array(32).fill(34))
    const registry = new KeyRegistry()
    registry.enrollRecord(KeyRecord.agent("valid", validKey.verifyingKey()))
    registry.enrollRecord(
      KeyRecord.agent("future", futureKey.verifyingKey()).validWindow(now + hour)
    )
    registry.enrollRecord(
      KeyRecord.agent("expired", expiredKey.verifyingKey()).validWindow(0n, now - hour)
    )
    registry.enrollRecord(KeyRecord.agent("revoked", revokedKey.verifyingKey()).revoke())

    const handledPrincipals: (string | undefined)[] = []
    const deadLetters: AgentDeadLetter[] = []
    const handle = Agent.builder()
      .id(AgentId.new("window-strict"))
      .listenOn(AgentTopic.Commands)
      .verifier(registry)
      .handler({
        handle(message: AgentMessage): Promise<void> {
          handledPrincipals.push(message.verifiedPrincipal)
          return Promise.resolve()
        }
      })
      .deadLetterSink({
        onDeadLetter(_message, capsule): Promise<void> {
          deadLetters.push(capsule)
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()

    // Each key signs an otherwise identical command. The broker stamps the
    // record timestamp on ingest, so only the key whose window covers that
    // stamp may pass, and the future, expired, and revoked keys dead-letter.
    const conversation = ConversationId.new()
    const senders: readonly (readonly [string, SigningKey])[] = [
      ["future", futureKey],
      ["expired", expiredKey],
      ["revoked", revokedKey],
      ["valid", validKey]
    ]
    for (const [index, [source, key]] of senders.entries()) {
      await laser
        .agdx(AgentTopic.Commands, AgentId.new(source), conversation)
        .command(CorrelationId.fromU128(0x0300n + BigInt(index)), new TextEncoder().encode("gate"))
        .signedBy(key)
        .send()
    }
    const pending = () => deadLetters.length < 3 || handledPrincipals.length < 1
    for (let attempt = 0; attempt < 160 && pending(); attempt += 1) await delay(10)

    assert.deepEqual(handledPrincipals, ["valid"])
    assert.equal(deadLetters.length, 3)
    assert.ok(
      deadLetters.every(
        (capsule) => capsule.reason.kind === "known" && capsule.reason.name === "Rejected"
      )
    )
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_verifier_when_input_replies_are_forged_then_should_resume_only_on_the_signed_response", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const approverKey = SigningKey.fromBytes(new Uint8Array(32).fill(51))
    const registry = new KeyRegistry()
    registry.enroll("approver", approverKey.verifyingKey())
    const caller = laser.withVerifier(registry)

    // An unsigned approver answers every interrupt, but its response cannot
    // verify, so the paused caller must keep waiting and time out.
    const faker = Agent.builder()
      .id(AgentId.new("faker"))
      .listenOn(AgentTopic.HumanInput)
      .handler({
        handle: (_message, context) =>
          context.respondInput(AgentTopic.Responses, new TextEncoder().encode("forged"))
      })
      .spawn(laser)
    await faker.ready()

    const orchestrator = caller.agdx(
      AgentTopic.HumanInput,
      AgentId.new("orchestrator"),
      ConversationId.new()
    )
    await assert.rejects(
      orchestrator.requestInput(AgentTopic.Responses, new TextEncoder().encode("approve?"), 2_000),
      TimeoutError
    )

    // A signing approver resumes the caller: `respondInput` signs with the
    // agent's key, so the verified reader accepts exactly this decision.
    const approver = Agent.builder()
      .id(AgentId.new("approver"))
      .listenOn(AgentTopic.HumanInput)
      .signingKey(approverKey)
      .handler({
        handle: (_message, context) =>
          context.respondInput(AgentTopic.Responses, new TextEncoder().encode("approved-signed"))
      })
      .spawn(laser)
    await approver.ready()

    const decision = await orchestrator.requestInput(
      AgentTopic.Responses,
      new TextEncoder().encode("approve?"),
      10_000
    )
    assert.equal(new TextDecoder().decode(decision), "approved-signed")
    await faker.shutdown()
    await approver.shutdown()
  } finally {
    await laser.close()
  }
})

// Parks on `gate` for the "block" payload (announcing itself via `entered`
// first) and records everything else, so a test can hold one partition lane
// open while watching what the scheduler still lets through.
function gatedHandler(handled: string[], gate: Promise<void>, entered: () => void) {
  return {
    async handle(message: AgentMessage): Promise<void> {
      const payload = new TextDecoder().decode(message.payload)
      if (payload === "block") {
        entered()
        await gate
      }
      handled.push(payload)
    }
  }
}

void test("given_a_blocked_partition_when_the_record_bound_fills_then_should_stall_intake_and_recover", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(4)
    const handled: string[] = []
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    let announceEntered!: () => void
    const entered = new Promise<void>((resolve) => {
      announceEntered = resolve
    })
    const handle = Agent.builder()
      .id(AgentId.new("bounded"))
      .listenOn(AgentTopic.Commands)
      .concurrency({ kind: "serial-per-partition", maxPartitions: 4 })
      .maxQueuedRecords(3)
      .handler(gatedHandler(handled, gate, announceEntered))
      .spawn(laser)
    await handle.ready()

    // A fast conversation drains completely while nothing is blocked.
    const fast = ConversationId.new()
    for (let index = 0; index < 10; index += 1) {
      await laser.sendAgent(
        AgentTopic.Commands,
        new TextEncoder().encode(`fast-${String(index)}`),
        {
          conversationId: fast
        }
      )
    }
    for (let attempt = 0; attempt < 160 && handled.length < 10; attempt += 1) await delay(10)
    assert.equal(handled.length, 10)

    // Block one conversation's lane, then flood it past the three-record
    // bound. The scheduler may buffer at most the bound, so intake stalls and
    // records published afterwards must not reach the handler.
    const blocked = ConversationId.new()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("block"), {
      conversationId: blocked
    })
    await entered
    for (let index = 0; index < 20; index += 1) {
      await laser.sendAgent(
        AgentTopic.Commands,
        new TextEncoder().encode(`queued-${String(index)}`),
        { conversationId: blocked }
      )
    }
    await delay(500)
    const late = ConversationId.new()
    for (let index = 0; index < 5; index += 1) {
      await laser.sendAgent(
        AgentTopic.Commands,
        new TextEncoder().encode(`late-${String(index)}`),
        {
          conversationId: late
        }
      )
    }
    await delay(700)
    assert.equal(
      handled.length,
      10,
      "a full record bound must stall intake instead of buffering the flood"
    )

    // Releasing the lane drains everything exactly once, in partition order.
    releaseGate()
    const drained = () => handled.length >= 36
    for (let attempt = 0; attempt < 300 && !drained(); attempt += 1) await delay(10)
    assert.equal(handled.length, 36)
    assert.deepEqual(
      handled.filter((payload) => payload.startsWith("queued-")),
      Array.from({ length: 20 }, (_, index) => `queued-${String(index)}`)
    )
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_blocked_partition_when_the_byte_bound_fills_then_should_stall_intake_and_recover", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(4)
    const handled: string[] = []
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    let announceEntered!: () => void
    const entered = new Promise<void>((resolve) => {
      announceEntered = resolve
    })
    const handle = Agent.builder()
      .id(AgentId.new("byte-bounded"))
      .listenOn(AgentTopic.Commands)
      .concurrency({ kind: "serial-per-partition", maxPartitions: 4 })
      .maxQueuedBytes(64 * 1024)
      .handler(gatedHandler(handled, gate, announceEntered))
      .spawn(laser)
    await handle.ready()

    // Hold the lane open, then queue payloads that overflow the byte bound:
    // the first large record fits, the second must stall the poll loop.
    const blocked = ConversationId.new()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("block"), {
      conversationId: blocked
    })
    await entered
    for (let index = 0; index < 3; index += 1) {
      await laser.sendAgent(
        AgentTopic.Commands,
        new TextEncoder().encode(`big-${String(index)}-${"x".repeat(40 * 1024)}`),
        { conversationId: blocked }
      )
    }
    await delay(500)
    const probe = ConversationId.new()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("probe"), {
      conversationId: probe
    })
    await delay(700)
    assert.equal(
      handled.length,
      0,
      "a full byte bound must stall intake instead of buffering the flood"
    )

    releaseGate()
    const drained = () => handled.length >= 5
    for (let attempt = 0; attempt < 300 && !drained(); attempt += 1) await delay(10)
    assert.equal(handled.length, 5)
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_failed_lane_when_successors_are_queued_then_should_not_commit_past_the_failure", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const conversation = ConversationId.new()
    const handled: string[] = []
    const recorder = {
      handle(message: AgentMessage): Promise<void> {
        handled.push(new TextDecoder().decode(message.payload))
        return Promise.resolve()
      }
    }
    // The deduplicator rejects once outside the dead-letter funnel, so message
    // A escapes `consume` as a lane failure while B queues behind it. The
    // partition must fence and reconnect without committing B past failed A.
    let failObserve = true
    const handle = Agent.builder()
      .id(AgentId.new("fenced-lane"))
      .listenOn(AgentTopic.Commands)
      .concurrency({ kind: "serial-per-partition", maxPartitions: 2 })
      .deduplicator({
        observe(): Promise<boolean> {
          if (!failObserve) return Promise.resolve(true)
          failObserve = false
          return Promise.reject(new TransportError("dedup store unavailable", true))
        }
      })
      .handler(recorder)
      .spawn(laser)
    await handle.ready()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("a"), {
      conversationId: conversation,
      idempotencyKey: "lane-a"
    })
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("b"), {
      conversationId: conversation,
      idempotencyKey: "lane-b"
    })
    for (let attempt = 0; attempt < 160 && handled.length < 2; attempt += 1) await delay(10)
    assert.deepEqual(handled, ["a", "b"])
    await handle.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_permanent_transport_rejection_when_polling_then_should_stop_with_a_typed_error", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const handled: string[] = []
    const handle = Agent.builder()
      .id(AgentId.new("classified"))
      .listenOn(AgentTopic.Commands)
      .handler({
        handle(message: AgentMessage): Promise<void> {
          handled.push(new TextDecoder().decode(message.payload))
          return Promise.resolve()
        }
      })
      .spawn(laser)
    await handle.ready()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("live"), {
      conversationId: ConversationId.new()
    })
    for (let attempt = 0; attempt < 160 && handled.length < 1; attempt += 1) await delay(10)
    assert.deepEqual(handled, ["live"])

    // Deleting the topic turns every poll into a definitive server rejection.
    // That is permanent, so the consumer must stop with a typed non-retryable
    // error instead of spinning through shutdown-and-reopen forever.
    await laser.iggyClient.topic.delete({
      streamId: stream,
      topicId: AgentTopic.Commands,
      partitionsCount: 1
    })
    await assert.rejects(
      handle.join(),
      (error: unknown) => error instanceof TransportError && !error.retryable
    )
  } finally {
    await laser.close()
  }
})
