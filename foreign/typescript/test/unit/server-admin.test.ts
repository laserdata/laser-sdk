import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError, TransportError, UnsupportedError } from "../../src/client/errors.js"
import { uncertainOperation } from "../../src/client/error-classify.js"
import { OperationUncertainError } from "apache-iggy"
import { PersonalAccessToken, User, serverVerbs } from "../../src/client/server.js"
import { Stream } from "../../src/stream/stream.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"

const NO_ADMIN = {} as LaserTransport

function settled(call: () => Promise<unknown>): Promise<unknown> {
  try {
    return call()
  } catch (error) {
    assert.fail(`threw synchronously: ${String(error)}`)
  }
}

void test("given_a_transport_without_administration_when_a_user_is_read_then_should_reject_unsupported", async () => {
  await assert.rejects(User.create(NO_ADMIN, "operator").get(), UnsupportedError)
})

void test("given_both_expiry_settings_when_a_topic_is_updated_then_should_reject_invalid_error", async () => {
  const topic = Stream.create(NO_ADMIN, "hosts").topic("metrics")
  await assert.rejects(
    settled(() => topic.update({ expireAfterMs: 1_000, neverExpire: true })),
    InvalidError
  )
  await assert.rejects(
    settled(() => topic.update({ maxTopicBytes: 1n, unlimitedTopicSize: true })),
    InvalidError
  )
  await assert.rejects(
    settled(() => topic.update({ expireAfterMs: -1 })),
    InvalidError
  )
})

void test("given_a_zero_segment_count_when_segments_are_deleted_then_should_reject_invalid_error", async () => {
  const topic = Stream.create(NO_ADMIN, "hosts").topic("metrics")
  await assert.rejects(
    settled(() => topic.deleteSegments(0, 0)),
    InvalidError
  )
})

void test("given_a_negative_token_expiry_when_a_token_is_created_then_should_reject_invalid_error", async () => {
  const token = PersonalAccessToken.create(NO_ADMIN, "ci")
  await assert.rejects(
    settled(() => token.create({ expiryMs: -1 })),
    InvalidError
  )
})

void test("given_a_transport_without_administration_when_any_admin_verb_runs_then_should_reject_unsupported", async () => {
  const stream = Stream.create(NO_ADMIN, "hosts")
  const topic = stream.topic("metrics")
  const user = User.create(NO_ADMIN, "operator")
  const calls: (() => Promise<unknown>)[] = [
    () => stream.rename("fleet"),
    () => stream.get(),
    () => stream.topics(),
    () => topic.delete(),
    () => topic.update({ name: "readings" }),
    () => topic.deleteSegments(0, 1),
    () => topic.addPartitions(1),
    () => topic.deletePartitions(1),
    () => topic.partitions(),
    () => topic.consumerGroups(),
    () => user.create("secret"),
    () => user.update({ status: "inactive" }),
    () => user.setPermissions(),
    () => user.changePassword("old", "new"),
    () => user.delete(),
    () => PersonalAccessToken.create(NO_ADMIN, "ci").create(),
    () => PersonalAccessToken.create(NO_ADMIN, "ci").delete(),
    () => serverVerbs.users(NO_ADMIN, {}),
    () => serverVerbs.ping(NO_ADMIN, {}),
    () => serverVerbs.stats(NO_ADMIN, {}),
    () => serverVerbs.describeOptions(NO_ADMIN, "topic", {}),
    () => serverVerbs.metadataChanges(NO_ADMIN, {}, {}),
    () => serverVerbs.resolveOperation(NO_ADMIN, 7n, 1_000, {}),
    () => serverVerbs.abandonOperation(NO_ADMIN, 7n, {}),
    () => serverVerbs.sendCommand(NO_ADMIN, 1, new Uint8Array(), undefined, {})
  ]
  for (const call of calls) await assert.rejects(settled(call), UnsupportedError)
})

// A transport whose administration succeeds and that records each cache it is told to drop.
function forgettingTransport(forgotten: string[]): LaserTransport {
  const raw = {
    sendCommand: () => Promise.resolve({ status: 0, length: 0, data: Buffer.alloc(0) })
  }
  const client = { clientProvider: () => Promise.resolve(raw) }
  return {
    administer: <Value>(operation: (admin: typeof client) => Promise<Value>) => operation(client),
    deleteStream: (name: string) => {
      forgotten.push(`transport ${name}`)
      return Promise.resolve(true)
    },
    deleteTopic: (stream: string, topic: string) => {
      forgotten.push(`transport ${stream}/${topic}`)
      return Promise.resolve(true)
    },
    addPartitions: (stream: string, topic: string) => {
      forgotten.push(`transport ${stream}/${topic}`)
      return Promise.resolve()
    },
    deletePartitions: (stream: string, topic: string) => {
      forgotten.push(`transport ${stream}/${topic}`)
      return Promise.resolve()
    },
    forgetStream: (name: string) => {
      forgotten.push(`transport ${name}`)
    },
    forgetTopic: (stream: string, topic: string) => {
      forgotten.push(`transport ${stream}/${topic}`)
    }
  } as unknown as LaserTransport
}

void test("given_each_stream_and_topic_change_when_it_lands_then_should_drop_the_caches_rust_drops", async () => {
  const forgotten: string[] = []
  const stream = Stream.create(
    forgettingTransport(forgotten),
    "hosts",
    undefined,
    undefined,
    undefined,
    (topic) => forgotten.push(topic === undefined ? "laser hosts" : `laser hosts/${topic}`)
  )
  const topic = stream.topic("metrics")
  const changes: [string, () => Promise<unknown>][] = [
    ["stream delete", () => stream.delete()],
    ["stream rename", () => stream.rename("fleet")],
    ["topic delete", () => topic.delete()],
    ["topic update", () => topic.update({ name: "readings" })],
    ["partition add", () => topic.addPartitions(1)],
    ["partition delete", () => topic.deletePartitions(1)],
    ["segment delete", () => topic.deleteSegments(0, 1)]
  ]
  const dropped = new Map<string, readonly string[]>()
  for (const [change, run] of changes) {
    forgotten.length = 0
    await run()
    dropped.set(change, [...forgotten].sort())
  }
  assert.deepEqual(Object.fromEntries(dropped), {
    "stream delete": ["laser hosts", "transport hosts"],
    "stream rename": ["laser hosts", "transport hosts"],
    "topic delete": ["laser hosts/metrics", "transport hosts/metrics"],
    "topic update": ["laser hosts/metrics", "transport hosts/metrics"],
    "partition add": ["laser hosts/metrics", "transport hosts/metrics"],
    "partition delete": ["laser hosts/metrics", "transport hosts/metrics"],
    "segment delete": []
  })
})

// A transport whose raw client records every operation and command it receives.
function recordingTransport(calls: unknown[][]): LaserTransport {
  const client = {
    resolveOperation: (operation: bigint, options: { deadline: number }) => {
      calls.push(["resolve", operation, options.deadline > Date.now()])
      return Promise.resolve()
    },
    abandonOperation: (operation: bigint) => {
      calls.push(["abandon", operation])
      return Promise.resolve()
    },
    sendBinaryRequest: (code: number, payload: Buffer) => {
      calls.push(["send", code, [...payload]])
      return Promise.resolve(Buffer.from([9]))
    },
    sendBinaryRequestToReplica: (code: number, payload: Buffer, replica: number) => {
      calls.push(["replica", code, [...payload], replica])
      return Promise.resolve(Buffer.from([8]))
    }
  }
  return {
    administer: <Value>(operation: (admin: typeof client) => Promise<Value>) => operation(client)
  } as unknown as LaserTransport
}

void test("given_an_uncertain_operation_when_it_is_resolved_or_abandoned_then_should_reach_the_raw_client_with_its_id", async () => {
  const calls: unknown[][] = []
  const transport = recordingTransport(calls)
  await serverVerbs.resolveOperation(transport, 7n, 1_000, {})
  await serverVerbs.abandonOperation(transport, 8n, {})
  assert.deepEqual(calls, [
    ["resolve", 7n, true],
    ["abandon", 8n]
  ])
  await assert.rejects(
    settled(() => serverVerbs.resolveOperation(transport, 7n, -1, {})),
    InvalidError
  )
})

void test("given_a_raw_command_when_it_is_sent_then_should_return_the_reply_of_the_node_or_replica", async () => {
  const calls: unknown[][] = []
  const transport = recordingTransport(calls)
  assert.deepEqual(
    await serverVerbs.sendCommand(transport, 10, new Uint8Array([1, 2]), undefined, {}),
    new Uint8Array([9])
  )
  assert.deepEqual(
    await serverVerbs.sendCommand(transport, 10, new Uint8Array([3]), 2, {}),
    new Uint8Array([8])
  )
  assert.deepEqual(calls, [
    ["send", 10, [1, 2]],
    ["replica", 10, [3], 2]
  ])
})

void test("given_a_failure_with_an_unknown_outcome_when_read_then_should_name_its_operation", () => {
  const uncertain = new TransportError("send failed", false, {
    cause: new OperationUncertainError(7n, undefined)
  })
  assert.equal(uncertainOperation(uncertain), 7n)
  assert.equal(uncertainOperation(new TransportError("send failed", true)), undefined)
  assert.equal(uncertainOperation(new InvalidError("bad input")), undefined)
})
