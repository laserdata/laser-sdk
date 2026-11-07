import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { A2aBridge, taskFromEnvelope, taskToJson } from "../../src/bridges/a2a.js"
import { BRIDGE_HOPS_META_KEY } from "../../src/bridges/hops.js"
import { McpBridge, type McpRpcResponse } from "../../src/bridges/mcp.js"
import { HandlerConfigError } from "../../src/client/errors.js"
import {
  INTERNAL_REPLY_HUB,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type {
  LaserTransport,
  MessageWithHeaders,
  PolledMessage
} from "../../src/iggy/apache-iggy.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { Sessions } from "../../src/session.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import {
  AgentKind,
  decodeAgentEnvelope,
  encodeAgentEnvelope,
  METADATA_BRIDGE_HOPS,
  parseAgentId,
  responseEnvelope,
  withTaskState,
  type AgentEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

const bridgeKey = SigningKey.fromBytes(new Uint8Array(32).fill(61))
const betaKey = SigningKey.fromBytes(new Uint8Array(32).fill(62))
const otherKey = SigningKey.fromBytes(new Uint8Array(32).fill(63))
const SIGNED_CONTEXT = {
  contentType: contentTypeCode(ContentType.Cbor),
  agentVersion: AGENT_OP_VERSION
}

void test("given_a_signing_key_when_an_a2a_task_is_submitted_then_should_sign_the_command", async () => {
  const fake = logLaser([])
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  ).withSigningKey(bridgeKey)
  await bridge.submit({ message: { text: "hi" } })
  const registry = new KeyRegistry()
  registry.enroll("bridge", bridgeKey.verifyingKey())
  assert.equal(verifiedCommandSigner(fake.log, registry), "bridge")
})

void test("given_a_signing_key_when_an_mcp_tool_is_called_then_should_sign_the_command", async () => {
  const fake = logLaser([])
  const bridge = new McpBridge(
    fake.laser,
    AgentId.new("mcp"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
    "tools"
  ).withSigningKey(bridgeKey)
  await bridge.callTool("search", { q: "x" })
  const registry = new KeyRegistry()
  registry.enroll("mcp", bridgeKey.verifyingKey())
  assert.equal(verifiedCommandSigner(fake.log, registry), "mcp")
})

void test("given_a_target_when_an_mcp_tool_reply_is_awaited_then_should_bind_the_target_as_the_signer", async () => {
  const fake = logLaser([])
  const bridge = new McpBridge(
    fake.laser,
    AgentId.new("mcp"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
    "tools"
  )
    .withTool("search", undefined, { type: "object" })
    .withTarget(AgentId.new("beta"))
  await bridge.callTool("search", { q: "x" }, { target: AgentId.new("alpha") })
  await bridge.callTool("search", { q: "y" })
  const response = await bridge.handleRpc({
    id: 1,
    method: "tools/call",
    params: { name: "search", arguments: {} }
  })
  assert.equal(response?.error, undefined)
  assert.deepEqual(fake.expectedSigners, ["alpha", "beta", "beta"])
  assert.deepEqual(
    commands(fake.log).map((command) => command.target),
    ["alpha", "beta", "beta"]
  )
})

void test("given_mcp_protocol_details_when_dispatched_then_should_follow_the_2025_11_25_schema", async () => {
  const fake = logLaser([])
  const bridge = new McpBridge(
    fake.laser,
    AgentId.new("mcp"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
    "tools"
  )
    .withTool("search", undefined, { type: "object" })
    .withPrompt({ name: "greet" }, [["user", "hello"]])
  assert.deepEqual(await bridge.handleRpc({ jsonrpc: "2.0", id: 1, method: "ping" }), {
    jsonrpc: "2.0",
    id: 1,
    result: {}
  })
  assert.equal(
    await bridge.handleRpc({ jsonrpc: "2.0", method: "notifications/initialized" }),
    undefined,
    "a notification is never answered"
  )
  const init = await answered(
    bridge.handleRpc({ id: 2, method: "initialize", params: { protocolVersion: "1999-01-01" } })
  )
  assert.equal((init.result as { protocolVersion: string }).protocolVersion, "2025-11-25")
  const older = await answered(
    bridge.handleRpc({ id: 3, method: "initialize", params: { protocolVersion: "2025-06-18" } })
  )
  assert.equal((older.result as { protocolVersion: string }).protocolVersion, "2025-06-18")
  const prompt = await answered(
    bridge.handleRpc({ id: 4, method: "prompts/get", params: { name: "greet" } })
  )
  assert.equal("description" in (prompt.result as object), false)
  for (const [request, code] of [
    [null, -32_600],
    [{ id: 5, method: "unknown" }, -32_601],
    [{ id: 6, method: "tools/call", params: { name: "nope" } }, -32_602],
    [{ id: 7, method: "prompts/get", params: { name: "nope" } }, -32_602],
    [{ id: 8, method: "resources/read", params: { uri: "file:///nope" } }, -32_002],
    [{ id: 9, method: "tools/call", params: { name: 7 } }, -32_602]
  ] as const) {
    const response = await bridge.handleRpc(request)
    assert.equal(response?.error?.code, code, JSON.stringify(request))
  }
  assert.equal(commands(fake.log).length, 0, "a refused tool call publishes nothing")
})

void test("given_inbound_hops_when_an_mcp_tool_is_called_then_should_extend_the_path_or_refuse_the_loop", async () => {
  const fake = logLaser([])
  const bridge = new McpBridge(
    fake.laser,
    AgentId.new("mcp"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
    "tools"
  ).withTool("search", undefined, { type: "object" })
  const looped = await bridge.handleRpc({
    id: 1,
    method: "tools/call",
    params: { name: "search", _meta: { [BRIDGE_HOPS_META_KEY]: ["a2a", "mcp"] } }
  })
  assert.equal(looped?.error?.code, -32_602)
  assert.equal(commands(fake.log).length, 0, "a looped call publishes nothing")
  await bridge.handleRpc({
    id: 2,
    method: "tools/call",
    params: { name: "search", _meta: { [BRIDGE_HOPS_META_KEY]: ["a2a"] } }
  })
  await bridge.callToolWithHops(["upstream"], "search", { q: "x" })
  await assert.rejects(bridge.callToolWithHops(["mcp"], "search", {}), /loop/)
  assert.deepEqual(
    commands(fake.log).map((command) => hopsOf(command)),
    [
      ["a2a", "mcp"],
      ["upstream", "mcp"]
    ]
  )
})

void test("given_an_a2a_answer_when_rendered_then_should_carry_the_v1_task_shape", () => {
  const reply = withTaskState(
    responseEnvelope(
      RecordId.fromU128(9n),
      WireConversationId.fromU128(2n),
      parseAgentId("worker"),
      CorrelationId.fromU128(2n),
      new TextEncoder().encode("plan ready")
    ),
    { kind: "known", name: "Completed" }
  )
  const task = taskFromEnvelope("t-1", "c-1", reply)
  assert.deepEqual(taskToJson(task), {
    id: "t-1",
    contextId: "c-1",
    status: { state: "TASK_STATE_COMPLETED" },
    artifacts: [{ artifactId: RecordId.fromU128(9n).toString(), parts: [{ text: "plan ready" }] }]
  })
})

void test("given_a2a_card_details_when_the_card_is_built_then_should_publish_an_absolute_url_and_no_streaming", () => {
  const bridge = new A2aBridge(
    logLaser([]).laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  assert.throws(() => bridge.card(), HandlerConfigError)
  assert.throws(() => bridge.withPublicUrl("/a2a"), /absolute/)
  const card = bridge
    .withPublicUrl("https://agents.example.com/a2a")
    .withDescription("Plans trips")
    .withVersion("2.1.0")
    .card()
  assert.equal(card.capabilities.streaming, false)
  assert.equal(card.description, "Plans trips")
  assert.equal(card.version, "2.1.0")
  assert.equal(card.supportedInterfaces[0]?.url, "https://agents.example.com/a2a")
})

void test("given_a2a_requests_when_dispatched_then_should_answer_v1_results_and_error_codes", async () => {
  const fake = logLaser([])
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  const sent = await bridge.handleRpc({
    id: 1,
    method: "SendMessage",
    params: { message: { messageId: "m-1", role: "ROLE_USER", parts: [{ text: "hi" }] } }
  })
  const created = (sent.result as { task: { id: string; contextId: string; status: unknown } }).task
  assert.deepEqual(created.status, { state: "TASK_STATE_SUBMITTED" })
  assert.equal(created.contextId, created.id)
  for (const [request, code] of [
    [{ id: 2, method: "ListTasks" }, -32_601],
    [{ id: 3, method: "SendStreamingMessage", params: {} }, -32_004],
    [{ id: 4, method: "GetTask", params: { id: "nope" } }, -32_001],
    [{ id: 5, method: "GetTask", params: { id: ConversationId.new().toString() } }, -32_001],
    [{ id: 6, method: "CancelTask", params: { id: "nope" } }, -32_001],
    [[], -32_600]
  ] as const) {
    assert.equal((await bridge.handleRpc(request)).error?.code, code, JSON.stringify(request))
  }
  const working = await bridge.handleRpc({
    id: 7,
    method: "GetTask",
    params: { id: created.id }
  })
  assert.deepEqual((working.result as { status: unknown }).status, { state: "TASK_STATE_WORKING" })
  appendAnswer(fake.log, created.id, "done", undefined)
  const notCancelable = await bridge.handleRpc({
    id: 8,
    method: "CancelTask",
    params: { id: created.id }
  })
  assert.equal(notCancelable.error?.code, -32_002)
  const done = await bridge.handleRpc({
    id: 9,
    method: "GetTask",
    params: { id: created.id }
  })
  assert.deepEqual((done.result as { status: unknown }).status, { state: "TASK_STATE_COMPLETED" })
})

void test("given_a_task_lookup_when_the_log_is_read_then_should_start_at_the_time_the_task_was_minted", async () => {
  const fake = logLaser([])
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  const task = await bridge.submit({ message: { text: "hi" } })
  fake.strategies.length = 0
  await bridge.task(task.id)
  const [first] = fake.strategies
  assert.equal(first?.kind, "timestampMicros")
  const mintedMicros = (ConversationId.parse(task.id).asU128() >> 80n) * 1000n
  assert.ok(first.value <= mintedMicros)
  assert.ok(first.value >= mintedMicros - 300_000_000n)
})

void test("given_a_targeted_task_under_a_verifier_when_another_signer_answers_then_should_ignore_it", async () => {
  const registry = new KeyRegistry()
  registry.enroll("beta", betaKey.verifyingKey())
  registry.enroll("other", otherKey.verifyingKey())
  registry.enroll("bridge", bridgeKey.verifyingKey())
  const fake = logLaser([], registry)
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
    .withSigningKey(bridgeKey)
    .withTarget(AgentId.new("beta"))
  const task = await bridge.submit({ message: { text: "hi" } })
  appendAnswer(fake.log, task.id, "forged", otherKey)
  assert.deepEqual((await bridge.task(task.id)).status.state, { kind: "known", name: "Working" })
  appendAnswer(fake.log, task.id, "honest", betaKey)
  const answered = await bridge.task(task.id)
  assert.deepEqual(answered.status.state, { kind: "known", name: "Completed" })
  assert.equal(answered.artifacts[0]?.parts[0]?.text, "honest")
})

void test("given_a_task_id_in_a_message_when_sent_then_should_continue_the_task_in_its_conversation", async () => {
  const fake = logLaser([])
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  const task = await bridge.submit(
    { message: { text: "hi", contextId: "trip-7" } },
    { target: AgentId.new("beta") }
  )
  assert.equal(task.contextId, "trip-7")
  appendAnswer(fake.log, task.id, "which city", undefined, "InputRequired")
  const resumed = await bridge.handleRpc({
    id: 1,
    method: "SendMessage",
    params: { message: { messageId: "m-2", role: "ROLE_USER", taskId: task.id, parts: [] } }
  })
  const continued = (resumed.result as { task: { id: string; contextId: string } }).task
  assert.equal(continued.id, task.id)
  assert.equal(continued.contextId, "trip-7")
  const [first, second] = commands(fake.log)
  assert.ok(first !== undefined && second !== undefined)
  assert.equal(second.conversation.toString(), first.conversation.toString())
  assert.equal(second.correlation?.toString(), first.correlation?.toString())
  assert.equal(second.target, "beta")
  appendAnswer(fake.log, task.id, "booked", undefined)
  assert.deepEqual((await bridge.task(task.id)).status.state, { kind: "known", name: "Completed" })
  const refused = await bridge.handleRpc({
    id: 2,
    method: "SendMessage",
    params: { message: { messageId: "m-3", role: "ROLE_USER", taskId: task.id, parts: [] } }
  })
  assert.equal(refused.error?.code, -32_004, "a terminal task takes no further message")
})

void test("given_inbound_hops_when_an_a2a_message_is_sent_then_should_refuse_the_loop", async () => {
  const fake = logLaser([])
  const bridge = new A2aBridge(
    fake.laser,
    AgentId.new("bridge"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  const looped = await bridge.handleRpc({
    id: 1,
    method: "SendMessage",
    params: {
      message: { messageId: "m-1", role: "ROLE_USER", parts: [] },
      metadata: { [BRIDGE_HOPS_META_KEY]: ["mcp", "bridge"] }
    }
  })
  assert.equal(looped.error?.code, -32_602)
  await bridge.submitWithHops(["mcp"], { message: { text: "hi" } })
  await assert.rejects(bridge.submitWithHops(["bridge"], {}), /loop/)
  assert.deepEqual(
    commands(fake.log).map((command) => hopsOf(command)),
    [["mcp", "bridge"]]
  )
})

// The response to a request, which an MCP bridge always answers.
async function answered(response: Promise<McpRpcResponse | undefined>): Promise<McpRpcResponse> {
  const resolved = await response
  assert.ok(resolved !== undefined, "a request is answered")
  return resolved
}

// A one-partition topic over an in-memory log: publishes append, polls read
// by offset or by timestamp, and the reply hub records the signer each wait binds.
function logLaser(log: PolledMessage[], verifier?: KeyRegistry) {
  const strategies: ConsumerStart[] = []
  const expectedSigners: (string | undefined)[] = []
  const transport = {
    findTopicPartitionCount: () => Promise.resolve(1),
    findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
    findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 1 }),
    sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) {
      for (const message of messages) {
        log.push({
          payload: message.payload,
          headers: message.headers,
          partitionId: 0,
          offset: BigInt(log.length),
          timestampMicros: BigInt(Date.now()) * 1000n
        })
      }
      return Promise.resolve({ confirmations: [] })
    },
    pollMessages(
      _stream: string,
      _topic: string,
      _target: unknown,
      strategy: ConsumerStart,
      count: number
    ) {
      strategies.push(strategy)
      const from =
        strategy.kind === "offset"
          ? log.filter((message) => message.offset >= strategy.value)
          : strategy.kind === "timestampMicros"
            ? log.filter((message) => (message.timestampMicros ?? 0n) >= strategy.value)
            : log
      return Promise.resolve(from.slice(0, count))
    }
  } as unknown as LaserTransport
  const reply = responseEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseAgentId("worker"),
    CorrelationId.fromU128(3n),
    new TextEncoder().encode("called")
  )
  const hub = {
    subscribeStream: (_correlation: string, expectedSigner?: string) => {
      expectedSigners.push(expectedSigner)
      return { next: () => Promise.resolve({ envelope: reply }), cancel: () => undefined }
    }
  }
  const laser = {
    defaultStream: "agents",
    capabilities: () => Promise.resolve({ sessions: false }),
    [INTERNAL_TRANSPORT]: () => transport,
    [INTERNAL_VERIFIER]: () => verifier,
    [INTERNAL_REPLY_HUB]: () => Promise.resolve(hub),
    agdx: (topic: string, source: AgentId, conversation: ConversationId) =>
      createAgdx(transport, "agents", topic, source, conversation),
    sessions: (): Sessions => Sessions.create(laser)
  } as unknown as Laser
  return { laser, log, strategies, expectedSigners }
}

function decoded(message: PolledMessage): AgentEnvelope {
  return decodeAgentEnvelope(expectMap(decodeOne(message.payload, "log"), "log"), "log")
}

function commands(log: readonly PolledMessage[]): AgentEnvelope[] {
  return log.map(decoded).filter((envelope) => envelope.kind === AgentKind.Command)
}

// The principal the logged command verifies to under the headers it was published with.
function verifiedCommandSigner(log: readonly PolledMessage[], registry: KeyRegistry): string {
  const message = log.find((candidate) => decoded(candidate).kind === AgentKind.Command)
  assert.ok(message !== undefined, "a command was published")
  const contentType = message.headers.get(CONTENT_TYPE)
  const agentVersion = message.headers.get(AGENT_VERSION)
  assert.ok(contentType?.kind === "uint8" && agentVersion?.kind === "uint32")
  return registry.verifyObservedAt(
    decoded(message),
    { contentType: contentType.value, agentVersion: agentVersion.value },
    1n
  ).principal
}

function hopsOf(envelope: AgentEnvelope): readonly string[] | undefined {
  const hops = envelope.metadata?.get(METADATA_BRIDGE_HOPS)
  return hops?.kind === "list"
    ? hops.value.map((hop) => (hop.kind === "str" ? hop.value : "?"))
    : undefined
}

// The worker's answer to the task `id`, signed by `key` when given.
function appendAnswer(
  log: PolledMessage[],
  id: string,
  body: string,
  key: SigningKey | undefined,
  state: "Completed" | "InputRequired" = "Completed"
): void {
  const conversation = ConversationId.parse(id.split(".")[0] ?? id)
  const envelope = withTaskState(
    responseEnvelope(
      RecordId.fromU128(BigInt(log.length) + 0x100n),
      WireConversationId.parse(conversation.toString()),
      parseAgentId("beta"),
      CorrelationId.parse(conversation.toString()),
      new TextEncoder().encode(body)
    ),
    { kind: "known", name: state }
  )
  const sealed =
    key === undefined
      ? envelope
      : { ...envelope, signature: key.signWithContext(envelope, SIGNED_CONTEXT) }
  log.push({
    payload: encodeNamed(encodeAgentEnvelope(sealed)),
    partitionId: 0,
    offset: BigInt(log.length),
    timestampMicros: BigInt(Date.now()) * 1000n,
    headers: new Map([
      [AGENT_VERSION, { kind: "uint32" as const, value: AGENT_OP_VERSION }],
      [CONTENT_TYPE, { kind: "uint8" as const, value: contentTypeCode(ContentType.Cbor) }]
    ])
  })
}
