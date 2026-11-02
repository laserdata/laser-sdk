import assert from "node:assert/strict"
import { test } from "node:test"
import { IdError, InvalidError } from "../../src/client/errors.js"
import { AgentId, ConsumerGroupName } from "../../src/types/ids.js"
import { parseIdempotencyKey } from "../../src/wire/agent.js"
import {
  ProjectionBuilder,
  RESERVED_PROJECTION_ID_PREFIX,
  validateControlCommand,
  validateProjection,
  validateSchemaName
} from "../../src/wire/control.js"
import { graphEdgeRelate, graphNodeEntity, validateGraphUpsert } from "../../src/wire/graph.js"
import {
  MAX_AGENT_STRING_BYTES,
  MAX_GRAPH_LABEL_BYTES,
  MAX_INDEX_FIELD_BYTES,
  MAX_MEMORY_ID_BYTES,
  MAX_PROJECTION_ID_BYTES,
  MAX_PROJECTION_NAME_BYTES,
  MAX_SCHEMA_NAME_BYTES,
  MAX_SESSION_CURSOR_BYTES,
  MAX_SESSION_LABEL_BYTES,
  MAX_SESSION_TEXT_QUERY_BYTES
} from "../../src/wire/limits.js"
import { validateMemoryRecord } from "../../src/wire/memory.js"
import { type SessionList, validateSessionList } from "../../src/wire/session.js"
import { encodeKvLease } from "../../src/wire/kv.js"

const projection = (id: string) => new ProjectionBuilder(id).build()

void test("given_projection_names_and_ids_out_of_bounds_when_validated_then_should_refuse_them", () => {
  validateProjection(new ProjectionBuilder("p".repeat(MAX_PROJECTION_ID_BYTES)).build())
  assert.throws(
    () => projection("p".repeat(MAX_PROJECTION_ID_BYTES + 1)),
    new InvalidError("projection id is 256B, exceeds cap 255B")
  )
  assert.throws(
    () => projection("orders\n"),
    new InvalidError("projection id contains a control character")
  )
  assert.throws(() => projection(""), new InvalidError("projection id must not be empty"))
  assert.throws(
    () => projection(`${RESERVED_PROJECTION_ID_PREFIX}orders`),
    new InvalidError('projection id must not start with "index:"')
  )
  assert.throws(() => {
    validateControlCommand({ kind: "dropProjection", id: "index:orders" })
  }, new InvalidError('projection id must not start with "index:"'))
  validateProjection(new ProjectionBuilder("orders:index").build())
  validateProjection(new ProjectionBuilder("orders").build())
  assert.throws(() => {
    validateProjection(
      new ProjectionBuilder("orders").name("n".repeat(MAX_PROJECTION_NAME_BYTES + 1)).build()
    )
  }, new InvalidError("projection name is 256B, exceeds cap 255B"))
  assert.throws(() => {
    validateControlCommand({
      kind: "dropProjection",
      id: "p".repeat(MAX_PROJECTION_ID_BYTES + 1)
    })
  }, InvalidError)
})

void test("given_index_fields_out_of_bounds_when_a_projection_is_validated_then_should_refuse_them", () => {
  // The builder points a field at `/<name>`, so the pointer is one byte longer.
  validateProjection(
    new ProjectionBuilder("orders").field("f".repeat(MAX_INDEX_FIELD_BYTES - 1)).build()
  )
  assert.throws(() => {
    validateProjection(
      new ProjectionBuilder("orders").field("f".repeat(MAX_INDEX_FIELD_BYTES + 1)).build()
    )
  }, /index field (name|pointer) is \d+B, exceeds cap 255B/)
  assert.throws(() => {
    validateProjection(new ProjectionBuilder("orders").vectorField("/v\u0007").build())
  }, new InvalidError("index vector field contains a control character"))
})

void test("given_schema_names_out_of_bounds_when_validated_then_should_refuse_them", () => {
  validateSchemaName(undefined)
  validateSchemaName("s".repeat(MAX_SCHEMA_NAME_BYTES))
  assert.throws(() => {
    validateSchemaName("s".repeat(MAX_SCHEMA_NAME_BYTES + 1))
  }, new InvalidError("schema name is 256B, exceeds cap 255B"))
  assert.throws(() => {
    validateSchemaName("")
  }, new InvalidError("schema name must not be empty"))
})

void test("given_an_invalid_graph_label_or_edge_type_when_upserted_then_should_refuse_it", () => {
  const atCap = "l".repeat(MAX_GRAPH_LABEL_BYTES)
  const alice = graphNodeEntity(atCap, "alice")
  const acme = graphNodeEntity("Company", "acme")
  validateGraphUpsert({
    graph: "knowledge",
    nodes: [alice],
    edges: [graphEdgeRelate(alice, atCap, acme)]
  })
  for (const invalid of ["", "l".repeat(MAX_GRAPH_LABEL_BYTES + 1), "Per\nson"]) {
    assert.throws(() => {
      validateGraphUpsert({
        graph: "knowledge",
        nodes: [graphNodeEntity(invalid, "x")],
        edges: []
      })
    }, InvalidError)
    assert.throws(() => {
      validateGraphUpsert({
        graph: "knowledge",
        nodes: [],
        edges: [graphEdgeRelate(alice, invalid, acme)]
      })
    }, InvalidError)
  }
})

void test("given_an_invalid_memory_id_kind_or_target_when_validated_then_should_refuse_it", () => {
  const item = (id: string, memoryKind: string) => ({
    kind: "item" as const,
    id,
    memoryKind,
    body: new TextEncoder().encode("auth is slow")
  })
  const atCap = "m".repeat(MAX_MEMORY_ID_BYTES)
  validateMemoryRecord(item(atCap, atCap))
  for (const invalid of ["", "m".repeat(MAX_MEMORY_ID_BYTES + 1), "fa\u0000ct"]) {
    assert.throws(() => {
      validateMemoryRecord(item(invalid, "fact"))
    }, /memory item id/)
    assert.throws(() => {
      validateMemoryRecord(item("fact-1", invalid))
    }, /memory item kind/)
    assert.throws(() => {
      validateMemoryRecord({ kind: "forget", target: invalid })
    }, /memory target/)
    assert.throws(() => {
      validateMemoryRecord({ kind: "feedback", target: invalid, weight: 1 })
    }, /memory target/)
  }
})

void test("given_session_list_filters_out_of_bounds_when_validated_then_should_refuse_them", () => {
  const list: SessionList = { stream: "agents", limit: 0, wantTotal: false }
  validateSessionList({
    ...list,
    stream: "s".repeat(255),
    labelPrefix: "l".repeat(MAX_SESSION_LABEL_BYTES),
    text: "t".repeat(MAX_SESSION_TEXT_QUERY_BYTES),
    cursor: "c".repeat(MAX_SESSION_CURSOR_BYTES)
  })
  validateSessionList({ ...list, text: "", cursor: "" })
  const cases: readonly [SessionList, string][] = [
    [{ ...list, stream: "" }, "session list stream must be 1..=255 bytes"],
    [{ ...list, stream: "s".repeat(256) }, "session list stream must be 1..=255 bytes"],
    [
      { ...list, labelPrefix: "l".repeat(MAX_SESSION_LABEL_BYTES + 1) },
      "session list label prefix is 257B, exceeds cap 256B"
    ],
    [
      { ...list, text: "t".repeat(MAX_SESSION_TEXT_QUERY_BYTES + 1) },
      "session list text is 1025B, exceeds cap 1024B"
    ],
    [
      { ...list, cursor: "c".repeat(MAX_SESSION_CURSOR_BYTES + 1) },
      "session list cursor is 513B, exceeds cap 512B"
    ]
  ]
  for (const [request, message] of cases) {
    assert.throws(() => {
      validateSessionList(request)
    }, new InvalidError(message))
  }
})

void test("given_a_control_character_in_an_idempotency_key_or_holder_id_when_validated_then_should_refuse_it", () => {
  parseIdempotencyKey("job-1")
  assert.throws(
    () => parseIdempotencyKey("job\u0000"),
    new InvalidError("idempotency key contains a control character")
  )
  assert.throws(
    () =>
      encodeKvLease({
        v: 1,
        namespace: "jobs",
        key: new TextEncoder().encode("job"),
        holderId: "worker\u0085",
        leaseTtlMicros: 10_000_000n
      }),
    new InvalidError("lease holder id contains a control character")
  )
})

void test("given_agent_ids_at_and_past_the_wire_cap_when_built_then_should_accept_256_bytes_and_refuse_more", () => {
  assert.equal(
    AgentId.new("a".repeat(MAX_AGENT_STRING_BYTES)).asStr().length,
    MAX_AGENT_STRING_BYTES
  )
  assert.throws(() => AgentId.new("a".repeat(MAX_AGENT_STRING_BYTES + 1)), IdError)
  assert.throws(() => ConsumerGroupName.new("g".repeat(256)), IdError)
  assert.throws(
    () => ConsumerGroupName.forAgent(AgentId.new("a".repeat(MAX_AGENT_STRING_BYTES))),
    IdError,
    "the default group of a 256-byte agent id exceeds the group name cap"
  )
})
