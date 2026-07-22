import type { CapabilitySurface } from "../client/capabilities.js"
import type { OpVersions } from "./hello.js"
import {
  type AgentCancel,
  type AgentList,
  type AgentReply,
  type AgentStatusReq,
  type AgentSubmit,
  decodeAgentReply,
  encodeAgentCancel,
  encodeAgentList,
  encodeAgentStatusReq,
  encodeAgentSubmit
} from "./agent-workflow.js"
import {
  type AuthzHistoryReq,
  type AuthzReply,
  type BindRolesReq,
  type DefineRoleReq,
  type DeleteRoleReq,
  type GetBindingsReq,
  type GetRoleReq,
  type ListRolesReq,
  decodeAuthzReply,
  encodeAuthzHistoryReq,
  encodeBindRolesReq,
  encodeDefineRoleReq,
  encodeDeleteRoleReq,
  encodeGetBindingsReq,
  encodeGetRoleReq,
  encodeListRolesReq,
  encodeWhoamiReq
} from "./authz.js"
import {
  type BatchReply,
  type BatchRequest,
  decodeBatchReply,
  encodeBatchRequest
} from "./batch.js"
import {
  type BrowseReply,
  type DecodeRecord,
  type GetProjection,
  type GetSchema,
  type ListProjections,
  type ListSchemas,
  type RegisterSchema,
  decodeBrowseReply,
  encodeDecodeRecord,
  encodeGetProjection,
  encodeGetSchema,
  encodeListProjections,
  encodeListSchemas,
  encodeRegisterSchema
} from "./browse.js"
import { decodeOne, encodeNamed, expectMap } from "./cbor.js"
import {
  AGDX_AGENT_CANCEL_CODE,
  AGDX_AGENT_LIST_CODE,
  AGDX_AGENT_STATUS_CODE,
  AGDX_AGENT_SUBMIT_CODE,
  AGDX_AUTHZ_BIND_ROLES_CODE,
  AGDX_AUTHZ_DEFINE_ROLE_CODE,
  AGDX_AUTHZ_DELETE_ROLE_CODE,
  AGDX_AUTHZ_GET_BINDINGS_CODE,
  AGDX_AUTHZ_GET_ROLE_CODE,
  AGDX_AUTHZ_HISTORY_CODE,
  AGDX_AUTHZ_LIST_ROLES_CODE,
  AGDX_AUTHZ_WHOAMI_CODE,
  AGDX_BATCH_CODE,
  AGDX_DECODE_RECORD_CODE,
  AGDX_FORK_CREATE_CODE,
  AGDX_FORK_DELETE_CODE,
  AGDX_FORK_LIST_CODE,
  AGDX_FORK_PROMOTE_CODE,
  AGDX_FORK_PUT_CODE,
  AGDX_GET_PROJECTION_CODE,
  AGDX_GET_SCHEMA_CODE,
  AGDX_GRAPH_NEIGHBORS_CODE,
  AGDX_GRAPH_QUERY_CODE,
  AGDX_GRAPH_UPSERT_CODE,
  AGDX_KV_CAS_CODE,
  AGDX_KV_CAS_FENCED_CODE,
  AGDX_KV_COPY_CODE,
  AGDX_KV_DELETE_CODE,
  AGDX_KV_DELETE_MANY_CODE,
  AGDX_KV_EXISTS_CODE,
  AGDX_KV_EXPIRE_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_LEASE_CODE,
  AGDX_KV_MOVE_CODE,
  AGDX_KV_NAMESPACES_CODE,
  AGDX_KV_PATCH_CODE,
  AGDX_KV_RELEASE_CODE,
  AGDX_KV_SCAN_CODE,
  AGDX_KV_SET_CODE,
  AGDX_LIST_PROJECTIONS_CODE,
  AGDX_LIST_SCHEMAS_CODE,
  AGDX_QUERY_CODE,
  AGDX_REGISTER_SCHEMA_CODE,
  FORK_OP_VERSION,
  GRAPH_OP_VERSION,
  KV_OP_VERSION,
  QUERY_OP_VERSION
} from "./codes.js"
import {
  type ForkCreate,
  type ForkDelete,
  type ForkPromote,
  type ForkPut,
  type ForkReply,
  decodeForkReply,
  encodeForkCreate,
  encodeForkDelete,
  encodeForkList,
  encodeForkPromote,
  encodeForkPut
} from "./fork.js"
import {
  type GraphNeighbors,
  type GraphQuery,
  type GraphReply,
  type GraphUpsert,
  decodeGraphReply,
  encodeGraphNeighbors,
  encodeGraphQueryFrame,
  encodeGraphUpsertFrame
} from "./graph.js"
import {
  type KvCas,
  type KvCasFenced,
  type KvCopy,
  type KvDelete,
  type KvDeleteMany,
  type KvExists,
  type KvExpire,
  type KvGet,
  type KvLease,
  type KvMove,
  type KvPatch,
  type KvRelease,
  type KvReply,
  type KvScan,
  type KvSet,
  decodeKvReply,
  encodeKvCas,
  encodeKvCasFenced,
  encodeKvCopy,
  encodeKvDelete,
  encodeKvDeleteMany,
  encodeKvExists,
  encodeKvExpire,
  encodeKvGet,
  encodeKvLease,
  encodeKvMove,
  encodeKvNamespaces,
  encodeKvPatch,
  encodeKvRelease,
  encodeKvScan,
  encodeKvSet
} from "./kv.js"
import {
  type QueryEnvelope,
  type QueryReply,
  decodeQueryReply,
  encodeQueryEnvelopeFrame
} from "./query.js"

export type VersionSurface = keyof Pick<OpVersions, "query" | "control" | "kv" | "fork" | "graph">

export interface ManagedCommand<Request, Reply> {
  readonly code: number
  readonly surface: CapabilitySurface
  readonly version?: { readonly surface: VersionSurface; readonly expected: number }
  encode(request: Request): Uint8Array
  decode(reply: Uint8Array): Reply
  validate?(request: Request): void
}

function framed<Request, Reply>(
  code: number,
  surface: CapabilitySurface,
  encode: (request: Request) => ReadonlyMap<string, unknown>,
  decode: (value: unknown, context: string) => Reply,
  version?: { readonly surface: VersionSurface; readonly expected: number }
): ManagedCommand<Request, Reply> {
  return {
    code,
    surface,
    ...(version !== undefined ? { version } : {}),
    encode: (request) => encodeNamed(encode(request)),
    decode: (reply) =>
      decode(decodeOne(reply, `managed command ${String(code)}`), `managed command ${String(code)}`)
  }
}

const queryVersion = { surface: "query", expected: QUERY_OP_VERSION } as const
const kvVersion = { surface: "kv", expected: KV_OP_VERSION } as const
const forkVersion = { surface: "fork", expected: FORK_OP_VERSION } as const
const graphVersion = { surface: "graph", expected: GRAPH_OP_VERSION } as const

export const WhoamiCommand = framed<undefined, AuthzReply>(
  AGDX_AUTHZ_WHOAMI_CODE,
  "authz",
  encodeWhoamiReq,
  decodeAuthzReply
)
export const ListRolesCommand = framed<ListRolesReq, AuthzReply>(
  AGDX_AUTHZ_LIST_ROLES_CODE,
  "authz",
  encodeListRolesReq,
  decodeAuthzReply
)
export const GetRoleCommand = framed<GetRoleReq, AuthzReply>(
  AGDX_AUTHZ_GET_ROLE_CODE,
  "authz",
  encodeGetRoleReq,
  decodeAuthzReply
)
export const GetBindingsCommand = framed<GetBindingsReq, AuthzReply>(
  AGDX_AUTHZ_GET_BINDINGS_CODE,
  "authz",
  encodeGetBindingsReq,
  decodeAuthzReply
)
export const DefineRoleCommand = framed<DefineRoleReq, AuthzReply>(
  AGDX_AUTHZ_DEFINE_ROLE_CODE,
  "authz",
  encodeDefineRoleReq,
  decodeAuthzReply
)
export const DeleteRoleCommand = framed<DeleteRoleReq, AuthzReply>(
  AGDX_AUTHZ_DELETE_ROLE_CODE,
  "authz",
  encodeDeleteRoleReq,
  decodeAuthzReply
)
export const BindRolesCommand = framed<BindRolesReq, AuthzReply>(
  AGDX_AUTHZ_BIND_ROLES_CODE,
  "authz",
  encodeBindRolesReq,
  decodeAuthzReply
)
export const AuthzHistoryCommand = framed<AuthzHistoryReq, AuthzReply>(
  AGDX_AUTHZ_HISTORY_CODE,
  "authz",
  encodeAuthzHistoryReq,
  decodeAuthzReply
)

export const QueryCommand: ManagedCommand<QueryEnvelope, QueryReply> = {
  code: AGDX_QUERY_CODE,
  surface: "query",
  version: queryVersion,
  encode: encodeQueryEnvelopeFrame,
  decode: (reply) => decodeQueryReply(decodeOne(reply, "query reply"), "query reply")
}

export const GetProjectionCommand = framed<GetProjection, BrowseReply>(
  AGDX_GET_PROJECTION_CODE,
  "managed",
  encodeGetProjection,
  decodeBrowseReply,
  queryVersion
)
export const ListProjectionsCommand = framed<ListProjections, BrowseReply>(
  AGDX_LIST_PROJECTIONS_CODE,
  "managed",
  encodeListProjections,
  decodeBrowseReply,
  queryVersion
)
export const GetSchemaCommand = framed<GetSchema, BrowseReply>(
  AGDX_GET_SCHEMA_CODE,
  "managed",
  encodeGetSchema,
  decodeBrowseReply,
  queryVersion
)
export const ListSchemasCommand = framed<ListSchemas, BrowseReply>(
  AGDX_LIST_SCHEMAS_CODE,
  "managed",
  encodeListSchemas,
  decodeBrowseReply,
  queryVersion
)
export const RegisterSchemaCommand = framed<RegisterSchema, BrowseReply>(
  AGDX_REGISTER_SCHEMA_CODE,
  "managed",
  encodeRegisterSchema,
  decodeBrowseReply,
  queryVersion
)
export const DecodeRecordCommand = framed<DecodeRecord, BrowseReply>(
  AGDX_DECODE_RECORD_CODE,
  "managed",
  encodeDecodeRecord,
  decodeBrowseReply,
  queryVersion
)

export const KvGetCommand = framed<KvGet, KvReply>(
  AGDX_KV_GET_CODE,
  "kv",
  encodeKvGet,
  decodeKvReply,
  kvVersion
)
export const KvSetCommand = framed<KvSet, KvReply>(
  AGDX_KV_SET_CODE,
  "kv",
  encodeKvSet,
  decodeKvReply,
  kvVersion
)
export const KvScanCommand = framed<KvScan, KvReply>(
  AGDX_KV_SCAN_CODE,
  "kv",
  encodeKvScan,
  decodeKvReply,
  kvVersion
)
export const KvDeleteCommand = framed<KvDelete, KvReply>(
  AGDX_KV_DELETE_CODE,
  "kv",
  encodeKvDelete,
  decodeKvReply,
  kvVersion
)
export const KvDeleteManyCommand = framed<KvDeleteMany, KvReply>(
  AGDX_KV_DELETE_MANY_CODE,
  "kv",
  encodeKvDeleteMany,
  decodeKvReply,
  kvVersion
)
export const KvNamespacesCommand = framed<undefined, KvReply>(
  AGDX_KV_NAMESPACES_CODE,
  "kv",
  encodeKvNamespaces,
  decodeKvReply,
  kvVersion
)
export const KvCasCommand = framed<KvCas, KvReply>(
  AGDX_KV_CAS_CODE,
  "kvCas",
  encodeKvCas,
  decodeKvReply,
  kvVersion
)
export const KvCasFencedCommand = framed<KvCasFenced, KvReply>(
  AGDX_KV_CAS_FENCED_CODE,
  "kvCasFenced",
  encodeKvCasFenced,
  decodeKvReply,
  kvVersion
)
export const KvExistsCommand = framed<KvExists, KvReply>(
  AGDX_KV_EXISTS_CODE,
  "kv",
  encodeKvExists,
  decodeKvReply,
  kvVersion
)
export const KvExpireCommand = framed<KvExpire, KvReply>(
  AGDX_KV_EXPIRE_CODE,
  "kv",
  encodeKvExpire,
  decodeKvReply,
  kvVersion
)
export const KvPatchCommand = framed<KvPatch, KvReply>(
  AGDX_KV_PATCH_CODE,
  "kv",
  encodeKvPatch,
  decodeKvReply,
  kvVersion
)
export const KvLeaseCommand = framed<KvLease, KvReply>(
  AGDX_KV_LEASE_CODE,
  "kv",
  encodeKvLease,
  decodeKvReply,
  kvVersion
)
export const KvReleaseCommand = framed<KvRelease, KvReply>(
  AGDX_KV_RELEASE_CODE,
  "kv",
  encodeKvRelease,
  decodeKvReply,
  kvVersion
)
export const KvCopyCommand = framed<KvCopy, KvReply>(
  AGDX_KV_COPY_CODE,
  "kv",
  encodeKvCopy,
  decodeKvReply,
  kvVersion
)
export const KvMoveCommand = framed<KvMove, KvReply>(
  AGDX_KV_MOVE_CODE,
  "kv",
  encodeKvMove,
  decodeKvReply,
  kvVersion
)

export const ForkCreateCommand = framed<ForkCreate, ForkReply>(
  AGDX_FORK_CREATE_CODE,
  "forks",
  encodeForkCreate,
  decodeForkReply,
  forkVersion
)
export const ForkDeleteCommand = framed<ForkDelete, ForkReply>(
  AGDX_FORK_DELETE_CODE,
  "forks",
  encodeForkDelete,
  decodeForkReply,
  forkVersion
)
export const ForkPromoteCommand = framed<ForkPromote, ForkReply>(
  AGDX_FORK_PROMOTE_CODE,
  "forks",
  encodeForkPromote,
  decodeForkReply,
  forkVersion
)
export const ForkListCommand = framed<undefined, ForkReply>(
  AGDX_FORK_LIST_CODE,
  "forks",
  encodeForkList,
  decodeForkReply,
  forkVersion
)
export const ForkPutCommand = framed<ForkPut, ForkReply>(
  AGDX_FORK_PUT_CODE,
  "forks",
  encodeForkPut,
  decodeForkReply,
  forkVersion
)

export const AgentSubmitCommand = framed<AgentSubmit, AgentReply>(
  AGDX_AGENT_SUBMIT_CODE,
  "agentWorkflow",
  encodeAgentSubmit,
  decodeAgentReply
)
export const AgentCancelCommand = framed<AgentCancel, AgentReply>(
  AGDX_AGENT_CANCEL_CODE,
  "agentWorkflow",
  encodeAgentCancel,
  decodeAgentReply
)
export const AgentStatusCommand = framed<AgentStatusReq, AgentReply>(
  AGDX_AGENT_STATUS_CODE,
  "agentWorkflow",
  encodeAgentStatusReq,
  decodeAgentReply
)
export const AgentListCommand = framed<AgentList, AgentReply>(
  AGDX_AGENT_LIST_CODE,
  "agentWorkflow",
  encodeAgentList,
  decodeAgentReply
)

export const BatchCommand: ManagedCommand<BatchRequest, BatchReply> = {
  code: AGDX_BATCH_CODE,
  surface: "managed",
  encode: (request) => encodeNamed(encodeBatchRequest(request)),
  decode: (reply) => {
    const context = `managed command ${String(AGDX_BATCH_CODE)}`
    return decodeBatchReply(expectMap(decodeOne(reply, context), context), context)
  }
}

export const GraphQueryCommand: ManagedCommand<GraphQuery, GraphReply> = {
  code: AGDX_GRAPH_QUERY_CODE,
  surface: "graph",
  version: graphVersion,
  encode: encodeGraphQueryFrame,
  decode: (reply) => decodeGraphReply(decodeOne(reply, "graph query reply"), "graph query reply")
}
export const GraphNeighborsCommand = framed<GraphNeighbors, GraphReply>(
  AGDX_GRAPH_NEIGHBORS_CODE,
  "graph",
  encodeGraphNeighbors,
  decodeGraphReply,
  graphVersion
)
export const GraphUpsertCommand: ManagedCommand<GraphUpsert, GraphReply> = {
  code: AGDX_GRAPH_UPSERT_CODE,
  surface: "graph",
  version: graphVersion,
  encode: encodeGraphUpsertFrame,
  decode: (reply) => decodeGraphReply(decodeOne(reply, "graph upsert reply"), "graph upsert reply")
}

export const MANAGED_COMMANDS = [
  WhoamiCommand,
  ListRolesCommand,
  GetRoleCommand,
  GetBindingsCommand,
  DefineRoleCommand,
  DeleteRoleCommand,
  BindRolesCommand,
  QueryCommand,
  GetProjectionCommand,
  ListProjectionsCommand,
  GetSchemaCommand,
  ListSchemasCommand,
  RegisterSchemaCommand,
  DecodeRecordCommand,
  KvGetCommand,
  KvSetCommand,
  KvScanCommand,
  KvDeleteCommand,
  KvDeleteManyCommand,
  KvNamespacesCommand,
  ForkCreateCommand,
  ForkDeleteCommand,
  ForkPromoteCommand,
  ForkListCommand,
  ForkPutCommand,
  AgentSubmitCommand,
  AgentCancelCommand,
  AgentStatusCommand,
  AgentListCommand
] as const

// Every other managed command this SDK issues that Rust's wire commands.rs
// registry does not also register (its `Command` trait impls only cover the
// codes `sdk/src/kv/client.rs` and friends route through a shared struct;
// these ride a raw code directly in the Rust client, see `wire/src/commands.rs`).
// Kept out of `MANAGED_COMMANDS` so that array stays an exact 1:1 mirror of the
// Rust registry, pinned by `test/wire/commands.test.ts`.
export const EXTRA_MANAGED_COMMANDS = [
  KvCasCommand,
  KvCasFencedCommand,
  KvExistsCommand,
  KvExpireCommand,
  KvPatchCommand,
  KvLeaseCommand,
  KvReleaseCommand,
  KvCopyCommand,
  KvMoveCommand,
  BatchCommand,
  AuthzHistoryCommand,
  GraphQueryCommand,
  GraphNeighborsCommand,
  GraphUpsertCommand
] as const
