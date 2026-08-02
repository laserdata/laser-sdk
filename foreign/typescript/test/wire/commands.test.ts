import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentCancelCommand,
  AgentListCommand,
  AgentStatusCommand,
  AgentSubmitCommand,
  BindRolesCommand,
  DecodeRecordCommand,
  DefineRoleCommand,
  DeleteRoleCommand,
  ForkCreateCommand,
  ForkDeleteCommand,
  ForkListCommand,
  ForkPromoteCommand,
  ForkPutCommand,
  GetBindingsCommand,
  GetProjectionCommand,
  GetRoleCommand,
  GetSchemaCommand,
  KvDeleteCommand,
  KvDeleteManyCommand,
  KvGetCommand,
  KvNamespacesCommand,
  KvScanCommand,
  KvSetCommand,
  ListProjectionsCommand,
  ListRolesCommand,
  ListSchemasCommand,
  MANAGED_COMMANDS,
  QueryCommand,
  RegisterSchemaCommand,
  WhoamiCommand,
  type ManagedCommand
} from "../../src/wire/commands.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
import {
  encodeAgentCancel,
  encodeAgentList,
  encodeAgentStatusReq,
  encodeAgentSubmit
} from "../../src/wire/agent-workflow.js"
import {
  encodeBindRolesReq,
  encodeDefineRoleReq,
  encodeDeleteRoleReq,
  encodeGetBindingsReq,
  encodeGetRoleReq,
  encodeListRolesReq,
  encodeWhoamiReq
} from "../../src/wire/authz.js"
import {
  encodeDecodeRecord,
  encodeGetProjection,
  encodeGetSchema,
  encodeListProjections,
  encodeListSchemas,
  encodeRegisterSchema
} from "../../src/wire/browse.js"
import {
  encodeForkCreate,
  encodeForkDelete,
  encodeForkList,
  encodeForkPromote,
  encodeForkPut
} from "../../src/wire/fork.js"
import {
  encodeKvDelete,
  encodeKvDeleteMany,
  encodeKvGet,
  encodeKvNamespaces,
  encodeKvScan,
  encodeKvSet
} from "../../src/wire/kv.js"
import { encodeQueryEnvelopeFrame } from "../../src/wire/query.js"

function assertFramed<Request>(
  command: ManagedCommand<Request, unknown>,
  request: Request,
  encode: (request: Request) => ReadonlyMap<string, unknown>
): void {
  assert.deepEqual(command.encode(request), encodeNamed(encode(request)))
  assert.ok(decodeOne(command.encode(request), `command ${String(command.code)}`) instanceof Map)
}

void test("given_the_command_registry_when_checked_then_should_pin_every_rust_command_code_once", () => {
  assert.deepEqual(
    MANAGED_COMMANDS.map((command) => command.code),
    [
      1_000_100, 1_000_101, 1_000_102, 1_000_103, 1_000_104, 1_000_105, 1_000_106, 1_000_200,
      1_000_210, 1_000_211, 1_000_220, 1_000_221, 1_000_222, 1_000_223, 1_000_300, 1_000_301,
      1_000_302, 1_000_303, 1_000_304, 1_000_305, 1_000_400, 1_000_401, 1_000_402, 1_000_403,
      1_000_404, 1_000_700, 1_000_701, 1_000_702, 1_000_703
    ]
  )
  assert.equal(new Set(MANAGED_COMMANDS.map((command) => command.code)).size, 29)
})

void test("given_typed_command_requests_when_encoded_then_should_delegate_to_the_exact_wire_codec", () => {
  assertFramed(WhoamiCommand, undefined, encodeWhoamiReq)
  assertFramed(ListRolesCommand, {}, encodeListRolesReq)
  assertFramed(GetRoleCommand, { name: "reader" }, encodeGetRoleReq)
  assertFramed(GetBindingsCommand, { userId: 7 }, encodeGetBindingsReq)
  assertFramed(DefineRoleCommand, { role: { name: "reader", grants: [] } }, encodeDefineRoleReq)
  assertFramed(DeleteRoleCommand, { name: "reader" }, encodeDeleteRoleReq)
  assertFramed(BindRolesCommand, { userId: 7, roles: ["reader"] }, encodeBindRolesReq)

  const query = {
    query: {
      index: "orders",
      byKey: [],
      order: [],
      limit: 10,
      offset: 0,
      distinct: false,
      select: { fields: [], payload: false },
      consistency: "eventual" as const,
      wantTotal: false
    }
  }
  assert.deepEqual(QueryCommand.encode(query), encodeQueryEnvelopeFrame(query))

  assertFramed(GetProjectionCommand, { v: 1, id: "orders" }, encodeGetProjection)
  assertFramed(ListProjectionsCommand, { v: 1, topics: ["orders"] }, encodeListProjections)
  assertFramed(GetSchemaCommand, { v: 1, id: 7 }, encodeGetSchema)
  assertFramed(ListSchemasCommand, { v: 1 }, encodeListSchemas)
  assertFramed(
    RegisterSchemaCommand,
    { v: 1, source: { kind: "jsonSchema", schema: "{}" } },
    encodeRegisterSchema
  )
  assertFramed(DecodeRecordCommand, { v: 1, id: 7, payload: Uint8Array.of(1) }, encodeDecodeRecord)

  assertFramed(KvGetCommand, { namespace: "n", key: Uint8Array.of(1) }, encodeKvGet)
  assertFramed(
    KvSetCommand,
    { namespace: "n", key: Uint8Array.of(1), value: Uint8Array.of(2) },
    encodeKvSet
  )
  assertFramed(KvScanCommand, { namespace: "n", limit: 10 }, encodeKvScan)
  assertFramed(KvDeleteCommand, { namespace: "n", key: Uint8Array.of(1) }, encodeKvDelete)
  assertFramed(KvDeleteManyCommand, { namespace: "n" }, encodeKvDeleteMany)
  assertFramed(KvNamespacesCommand, undefined, encodeKvNamespaces)

  assertFramed(
    ForkCreateCommand,
    { forkId: "draft", kind: "severed", tables: [] },
    encodeForkCreate
  )
  assertFramed(ForkDeleteCommand, { forkId: "draft" }, encodeForkDelete)
  assertFramed(ForkPromoteCommand, { forkId: "draft" }, encodeForkPromote)
  assertFramed(ForkListCommand, undefined, encodeForkList)
  assertFramed(
    ForkPutCommand,
    {
      forkId: "draft",
      table: "orders",
      partitionId: 0,
      offset: 1n,
      projectionId: "orders",
      projectionVersion: 1,
      fields: new Map(),
      metadata: new Map(),
      tombstone: false
    },
    encodeForkPut
  )

  assertFramed(AgentSubmitCommand, { agentId: "planner", params: new Map() }, encodeAgentSubmit)
  assertFramed(AgentCancelCommand, { runId: "run-1" }, encodeAgentCancel)
  assertFramed(AgentStatusCommand, { runId: "run-1" }, encodeAgentStatusReq)
  assertFramed(AgentListCommand, {}, encodeAgentList)
})
