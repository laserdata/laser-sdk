import assert from "node:assert/strict"
import { test } from "node:test"
import { contentRefMode, taskFromEnvelope, taskToJson } from "../../src/bridges/a2a.js"
import { applyJsonPatch, envelopesToAgUi } from "../../src/bridges/agui.js"
import { authorizeEdge, edgeDenialChallenge, edgeDenialCode } from "../../src/bridges/edge-auth.js"
import { toolResultFromEnvelope } from "../../src/bridges/mcp.js"
import { bridgeHopMetadata, enterBridge } from "../../src/bridges/hops.js"
import { InvalidError } from "../../src/client/errors.js"
import {
  AgentKind,
  OPERATION_REASONING,
  OPERATION_TASK,
  errorEnvelope,
  responseEnvelope,
  statusEnvelope,
  withCorrelation,
  withTaskState
} from "../../src/wire/agent.js"
import { ContentType } from "../../src/wire/content.js"
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
      { kind: "string", value: "a2a" },
      { kind: "string", value: "mcp" }
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
    contentRefMode({ kind: "schemaId", value: "orders.v1" }),
    "application/x-agdx-schema;id=orders.v1"
  )
  const envelope = responseEnvelope(
    RecordId.fromU128(1n),
    conversation,
    source,
    correlation,
    new TextEncoder().encode("answer")
  )
  const task = taskFromEnvelope("task-1", envelope)
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
  assert.deepEqual(toolResultFromEnvelope(envelope), {
    content: [{ type: "text", text: "failed" }],
    isError: true
  })
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
