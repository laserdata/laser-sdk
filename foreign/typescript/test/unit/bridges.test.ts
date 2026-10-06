import assert from "node:assert/strict"
import { test } from "node:test"
import {
  A2A_JSONRPC_BINDING,
  A2A_PROTOCOL_VERSION,
  A2aBridge,
  contentRefMode,
  taskFromEnvelope,
  taskToJson,
  type AgentCard,
  type Artifact,
  type JsonRpcError,
  type JsonRpcRequest,
  type Task,
  type TaskStatus
} from "../../src/bridges/a2a.js"
import { applyJsonPatch, envelopesToAgUi } from "../../src/bridges/agui.js"
import { authorizeEdge, edgeDenialChallenge, edgeDenialCode } from "../../src/bridges/edge-auth.js"
import {
  MCP_APP_ERROR_CODE,
  McpBridge,
  toolResultFromEnvelope,
  type McpContent,
  type McpRpcError,
  type McpRpcRequest
} from "../../src/bridges/mcp.js"
import { bridgeHopMetadata, enterBridge } from "../../src/bridges/hops.js"
import {
  CancelledError,
  ConfigError,
  HandlerConfigError,
  InvalidError,
  NoStreamError,
  RejectedError,
  TimeoutError
} from "../../src/client/errors.js"
import { publicErrorMessage } from "../../src/client/error-classify.js"
import {
  INTERNAL_REPLY_HUB,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { KeyRegistry, SigningKey, verifyCard } from "../../src/signing.js"
import { AgentId, ConversationId as TaskConversationId } from "../../src/types/ids.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import {
  AgentKind,
  encodeAgentEnvelope,
  OPERATION_REASONING,
  OPERATION_TASK,
  errorEnvelope,
  responseEnvelope,
  statusEnvelope,
  withCorrelation,
  withTaskState
} from "../../src/wire/agent.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { ChannelId, ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"
import { parseAgentId } from "../../src/wire/agent.js"

const source = parseAgentId("bridge")
const conversation = ConversationId.fromU128(2n)
const correlation = CorrelationId.fromU128(3n)

void test("given_a_bridge_already_in_the_hop_list_when_entered_then_should_reject_the_loop", () => {
  assert.deepEqual(enterBridge("mcp", ["a2a"]), ["a2a", "mcp"])
  assert.deepEqual(bridgeHopMetadata(["a2a", "mcp"]), {
    kind: "list",
    value: [
      { kind: "str", value: "a2a" },
      { kind: "str", value: "mcp" }
    ]
  })
  assert.throws(() => enterBridge("a2a", ["a2a", "mcp"]), InvalidError)
})

void test("given_edge_claims_when_authorized_then_should_distinguish_rejection_from_step_up", () => {
  assert.equal(
    authorizeEdge({ audience: ["mcp"], scopes: ["tool:read"] }, "mcp", "tool:read"),
    undefined
  )
  const wrongAudience = authorizeEdge(
    { audience: ["other"], scopes: ["tool:read"] },
    "mcp",
    "tool:read"
  )
  assert.deepEqual(wrongAudience, { kind: "wrongAudience", expected: "mcp" })
  assert.deepEqual(edgeDenialCode(wrongAudience), { kind: "known", name: "Unauthenticated" })
  assert.equal(edgeDenialChallenge(wrongAudience), undefined)
  const stepUp = authorizeEdge({ audience: ["mcp"], scopes: [] }, "mcp", "tool:write")
  assert.deepEqual(stepUp, { kind: "stepUp", requiredScope: "tool:write" })
  assert.deepEqual(edgeDenialCode(stepUp), { kind: "known", name: "StepUpRequired" })
  assert.equal(edgeDenialChallenge(stepUp), 'Bearer scope="tool:write"')
})

void test("given_a2a_capabilities_and_replies_when_projected_then_should_use_protocol_spellings", () => {
  assert.equal(contentRefMode({ kind: "contentType", value: ContentType.Json }), "application/json")
  assert.equal(
    contentRefMode({ kind: "schemaId", value: "readings.v1" }),
    "application/x-agdx-schema;id=readings.v1"
  )
  const envelope = responseEnvelope(
    RecordId.fromU128(1n),
    conversation,
    source,
    correlation,
    new TextEncoder().encode("answer")
  )
  const task: Task = taskFromEnvelope("task-1", envelope)
  const status: TaskStatus = { state: { kind: "known", name: "Completed" } }
  const artifact: Artifact = { text: "answer" }
  assert.deepEqual(task, { id: "task-1", status, artifacts: [artifact] })
  assert.deepEqual(taskToJson(task), {
    id: "task-1",
    status: { state: "completed" },
    artifacts: [{ text: "answer" }]
  })
})

void test("given_an_agdx_error_when_rendered_as_mcp_then_should_mark_the_result_as_error", () => {
  const envelope = errorEnvelope(
    RecordId.fromU128(1n),
    conversation,
    source,
    correlation,
    new TextEncoder().encode("failed")
  )
  const content: McpContent = { kind: "text", text: "failed" }
  assert.deepEqual(toolResultFromEnvelope(envelope), { content: [content], isError: true })
})

void test("given_an_rfc6902_patch_when_applied_then_should_support_all_operations", () => {
  const result = applyJsonPatch({ name: "old", items: ["a", "b"], nested: { keep: true } }, [
    { op: "test", path: "/nested/keep", value: true },
    { op: "replace", path: "/name", value: "new" },
    { op: "add", path: "/items/-", value: "c" },
    { op: "copy", from: "/name", path: "/copy" },
    { op: "move", from: "/items/0", path: "/first" },
    { op: "remove", path: "/nested" }
  ])
  assert.deepEqual(result, { name: "new", items: ["b", "c"], copy: "new", first: "a" })
})

void test("given_task_and_reasoning_envelopes_when_rendered_then_should_emit_agui_lifecycle_events", () => {
  const submitted = withTaskState(
    withCorrelation(
      statusEnvelope(RecordId.fromU128(1n), conversation, source, OPERATION_TASK),
      correlation
    ),
    { kind: "known", name: "Submitted" }
  )
  const chunk = {
    kind: AgentKind.Chunk,
    record: RecordId.fromU128(4n),
    conversation,
    source,
    body: new TextEncoder().encode("thinking"),
    operation: OPERATION_REASONING,
    channel: ChannelId.fromU128(5n),
    sequence: 0n,
    last: true,
    mustUnderstand: 0n
  } as const
  assert.deepEqual(
    envelopesToAgUi([submitted, chunk]).map((event) => event.type),
    ["RUN_STARTED", "REASONING_MESSAGE_START", "REASONING_MESSAGE_CONTENT", "REASONING_MESSAGE_END"]
  )
})

void test("given_a_proto_pointer_in_a_patch_when_applied_then_should_reject_and_not_pollute", () => {
  const before = ({} as Record<string, unknown>)["polluted"]
  assert.equal(before, undefined)
  for (const path of ["/__proto__/polluted", "/constructor/polluted", "/prototype/polluted"]) {
    assert.throws(
      () => applyJsonPatch({ name: "state" }, [{ op: "add", path, value: true }]),
      InvalidError,
      `${path} must be refused`
    )
  }
  assert.equal(
    ({} as Record<string, unknown>)["polluted"],
    undefined,
    "Object.prototype must be untouched"
  )
})

void test("given_a_nested_proto_pointer_in_a_patch_when_applied_then_should_reject", () => {
  assert.throws(
    () =>
      applyJsonPatch({ nested: {} }, [
        { op: "add", path: "/nested/__proto__/polluted", value: true }
      ]),
    InvalidError
  )
  assert.equal(({} as Record<string, unknown>)["polluted"], undefined)
})

void test("given_an_inherited_property_when_patched_then_should_not_count_as_existing", () => {
  assert.throws(
    () => applyJsonPatch({ nested: {} }, [{ op: "add", path: "/toString/x", value: 1 }]),
    InvalidError,
    "an inherited property must not satisfy the path-exists check"
  )
})

const replyKey = SigningKey.fromBytes(new Uint8Array(32).fill(51))

function taskReply(task: TaskConversationId, body: string, offset: bigint, signed: boolean) {
  const envelope = responseEnvelope(
    RecordId.fromU128(0x10n + offset),
    conversation,
    parseAgentId("worker"),
    CorrelationId.parse(task.toString()),
    new TextEncoder().encode(body)
  )
  const context = { contentType: contentTypeCode(ContentType.Cbor), agentVersion: AGENT_OP_VERSION }
  const sealed = signed
    ? { ...envelope, signature: replyKey.signWithContext(envelope, context) }
    : envelope
  return {
    payload: encodeNamed(encodeAgentEnvelope(sealed)),
    partitionId: 0,
    offset,
    timestampMicros: BigInt(Date.now()) * 1000n,
    headers: new Map([
      [AGENT_VERSION, { kind: "uint32" as const, value: AGENT_OP_VERSION }],
      [CONTENT_TYPE, { kind: "uint8" as const, value: contentTypeCode(ContentType.Cbor) }]
    ])
  } satisfies PolledMessage
}

function bridgeOver(messages: readonly PolledMessage[], verifier?: KeyRegistry, stream = "s") {
  const transport = {
    findTopicPartitionCount: () => Promise.resolve(1),
    pollMessages: (
      _stream: string,
      _topic: string,
      _target: unknown,
      strategy: { readonly kind: string; readonly value?: bigint }
    ) => Promise.resolve(messages.filter((message) => message.offset >= (strategy.value ?? 0n)))
  } as unknown as LaserTransport
  const laser = {
    defaultStream: stream === "" ? undefined : stream,
    [INTERNAL_TRANSPORT]: () => transport,
    [INTERNAL_VERIFIER]: () => verifier
  } as unknown as Laser
  return new A2aBridge(laser, AgentId.new("bridge"), "requests", "replies")
}

void test("given_a_verifier_when_an_a2a_task_has_only_an_unsigned_reply_then_should_stay_working", async () => {
  const task = TaskConversationId.new()
  const registry = new KeyRegistry()
  registry.enroll("worker", replyKey.verifyingKey())
  const forged = bridgeOver([taskReply(task, "forged", 0n, false)], registry)
  assert.deepEqual((await forged.task(task.toString())).status.state, {
    kind: "known",
    name: "Working"
  })
  const honest = bridgeOver(
    [taskReply(task, "forged", 0n, false), taskReply(task, "honest", 1n, true)],
    registry
  )
  assert.deepEqual((await honest.task(task.toString())).artifacts, [{ text: "honest" }])
})

void test("given_two_replies_when_an_a2a_task_is_read_then_should_report_the_first", async () => {
  const task = TaskConversationId.new()
  const bridge = bridgeOver([
    taskReply(task, "first", 0n, false),
    taskReply(task, "second", 1n, false)
  ])
  assert.deepEqual((await bridge.task(task.toString())).artifacts, [{ text: "first" }])
  await assert.rejects(bridgeOver([], undefined, "").task(task.toString()), NoStreamError)
})

void test("given_bridge_failures_when_rendered_then_should_match_rust_public_result_classification", () => {
  for (const error of [
    new ConfigError("private"),
    new HandlerConfigError("private"),
    new RejectedError("private")
  ]) {
    assert.equal(publicErrorMessage(error), "invalid request")
  }
  for (const error of [new TimeoutError("private"), new CancelledError("private")]) {
    assert.equal(publicErrorMessage(error), "internal error")
  }
  assert.equal(publicErrorMessage(new NoStreamError("private")), "unsupported operation")
})

void test("given_a2a_capabilities_when_the_card_is_built_then_should_carry_the_v1_fields", () => {
  const bridge = bridgeOver([]).withCapabilities([
    {
      skillId: "summarize",
      input: { kind: "contentType", value: ContentType.Json },
      output: { kind: "schemaId", value: "summary.v1" }
    }
  ])
  const card: AgentCard = bridge.card()
  assert.equal(card.name, "bridge")
  assert.equal(card.description, "LaserData AGDX bridge over the durable log")
  assert.deepEqual(card.supportedInterfaces, [
    { url: "/", protocolBinding: A2A_JSONRPC_BINDING, protocolVersion: A2A_PROTOCOL_VERSION }
  ])
  assert.deepEqual(card.defaultInputModes, ["text/plain"])
  assert.deepEqual(card.defaultOutputModes, ["text/plain"])
  assert.deepEqual(card.skills, [
    {
      id: "summarize",
      name: "summarize",
      description: "",
      tags: [],
      inputModes: ["application/json"],
      outputModes: ["application/x-agdx-schema;id=summary.v1"]
    }
  ])
  assert.equal(card.signatures, undefined)
  const signed = bridge.signedCard(replyKey)
  assert.equal(signed.signatures?.length, 1)
  const [signature] = signed.signatures ?? []
  assert.ok(signature !== undefined)
  verifyCard(signed, signature, replyKey.verifyingKey())
})

void test("given_an_unknown_a2a_method_when_handled_then_should_answer_a_json_rpc_error", async () => {
  const request: JsonRpcRequest = { id: 7, method: "ListTasks", params: {} }
  const response = await bridgeOver([]).handleRpc(request)
  const error: JsonRpcError = { code: -32_000, message: "invalid request" }
  assert.deepEqual(response, { jsonrpc: "2.0", id: 7, error })
})

void test("given_raw_json_when_an_a2a_task_is_submitted_then_should_publish_it_byte_identical", async () => {
  const bodies: Uint8Array[] = []
  const send = {
    withOperation: () => send,
    withMetadata: () => send,
    contentType: () => send,
    send: () => Promise.resolve()
  }
  const laser = {
    agdx: () => ({
      command: (_correlation: unknown, body: Uint8Array) => {
        bodies.push(body)
        return send
      }
    })
  } as unknown as Laser
  const bridge = new A2aBridge(laser, AgentId.new("bridge"), "requests", "replies")
  const raw = '{"message" : {"text":"hi"}}'
  await bridge.submit(new TextEncoder().encode(raw))
  await bridge.submit(raw)
  await bridge.submit({ message: { text: "hi" } })
  assert.deepEqual(
    bodies.map((body) => new TextDecoder().decode(body)),
    [raw, raw, '{"message":{"text":"hi"}}']
  )
})

function mcpOver(reply: ReturnType<typeof responseEnvelope>, bodies: Uint8Array[] = []) {
  const send = {
    withTool: () => send,
    withMetadata: () => send,
    contentType: () => send,
    send: () => Promise.resolve()
  }
  const hub = {
    subscribeStream: () => ({
      next: () => Promise.resolve({ envelope: reply }),
      cancel: () => undefined
    })
  }
  const laser = {
    agdx: () => ({
      command: (_correlation: unknown, body: Uint8Array) => {
        bodies.push(body)
        return send
      }
    }),
    [INTERNAL_REPLY_HUB]: () => Promise.resolve(hub)
  } as unknown as Laser
  return new McpBridge(laser, AgentId.new("mcp"), "tool-calls", "tool-results", "tools")
}

void test("given_an_mcp_tool_call_when_handled_then_should_spell_the_content_kind_as_type", async () => {
  const reply = responseEnvelope(
    RecordId.fromU128(1n),
    conversation,
    source,
    correlation,
    new TextEncoder().encode("called")
  )
  const request: McpRpcRequest = { id: "1", method: "tools/call", params: { name: "search" } }
  const response = await mcpOver(reply).handleRpc(request)
  assert.deepEqual(response, {
    jsonrpc: "2.0",
    id: "1",
    result: { content: [{ type: "text", text: "called" }] }
  })
  const failed = await mcpOver(reply).handleRpc({ id: "2", method: "tasks/list" })
  const error: McpRpcError = { code: MCP_APP_ERROR_CODE, message: "invalid request" }
  assert.deepEqual(failed, { jsonrpc: "2.0", id: "2", error })
})

void test("given_raw_json_bytes_or_a_value_when_calling_a_tool_then_should_send_the_same_json", async () => {
  const reply = responseEnvelope(
    RecordId.fromU128(1n),
    conversation,
    source,
    correlation,
    new TextEncoder().encode("called")
  )
  const bodies: Uint8Array[] = []
  const bridge = mcpOver(reply, bodies)
  const raw = '{"q" : "agdx"}'
  await bridge.callTool("search", new TextEncoder().encode(raw))
  const result = await bridge.callTool("search", { q: "agdx" })
  assert.deepEqual(result.content, [{ kind: "text", text: "called" }])
  assert.deepEqual(
    bodies.map((body) => new TextDecoder().decode(body)),
    [raw, '{"q":"agdx"}']
  )
})
