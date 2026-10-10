import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"
import {
  AGDX_DECODE_RECORD_CODE,
  AGDX_DESTINATION_GET_CODE,
  AGDX_DESTINATION_LIST_CODE,
  AGDX_FILTER_OPERATION_CODE,
  AGDX_FORK_CREATE_CODE,
  AGDX_FORK_DELETE_CODE,
  AGDX_FORK_LIST_CODE,
  AGDX_FORK_PROMOTE_CODE,
  AGDX_FORK_PUT_CODE,
  AGDX_GET_FILTER_BINDING_CODE,
  AGDX_GET_FILTER_CODE,
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
  AGDX_KV_LEASE_RENEW_CODE,
  AGDX_KV_MOVE_CODE,
  AGDX_KV_NAMESPACES_CODE,
  AGDX_KV_PATCH_CODE,
  AGDX_KV_RELEASE_CODE,
  AGDX_KV_SCAN_CODE,
  AGDX_KV_SET_CODE,
  AGDX_LIST_FILTER_BINDINGS_CODE,
  AGDX_LIST_FILTER_REVISIONS_CODE,
  AGDX_LIST_FILTERS_CODE,
  AGDX_LIST_PROJECTIONS_CODE,
  AGDX_LIST_SCHEMAS_CODE,
  AGDX_QUERY_CODE,
  AGDX_QUERY_ROUTE_LIST_CODE,
  AGDX_SESSION_GET_CODE,
  AGDX_SESSION_LIST_CODE,
  AGDX_SESSION_EVENTS_CODE,
  AGDX_SESSION_STATE_CODE,
  AGDX_SESSION_LINKS_CODE,
  AGDX_SESSION_SOURCES_CODE,
  AGDX_SESSION_CHANGES_CODE,
  AGDX_REGISTER_SCHEMA_CODE,
  AUTHZ_OP_VERSION
} from "./codes.js"
import { MAX_ROLE_NAME_BYTES } from "./limits.js"
import type { ResultCode } from "./result.js"

export type Effect = "allow" | "deny"

function parseEffect(word: string, context: string): Effect {
  if (word !== "allow" && word !== "deny") {
    throw new CodecError(`\`${word}\` is not a recognized authz effect`, context, "effect")
  }
  return word
}

export type Feature =
  | "kv"
  | "memory"
  | "projection"
  | "fork"
  | "graph"
  | "query"
  | "agent"
  | "workflow"
  | "destination"
  | "checkpoint"
  | "authz"
  | "kv_lease"
  | "kv_fence"
  | "filter"
  | "session"
  | "unrecognized"

const KNOWN_FEATURES: ReadonlySet<string> = new Set([
  "kv",
  "memory",
  "projection",
  "fork",
  "graph",
  "query",
  "agent",
  "workflow",
  "destination",
  "checkpoint",
  "authz",
  "kv_lease",
  "kv_fence",
  "filter",
  "session"
])

function parseFeature(word: string): Feature {
  return KNOWN_FEATURES.has(word) ? (word as Feature) : "unrecognized"
}

export type Action = "read" | "write" | "delete" | "admin" | "unrecognized"

const KNOWN_ACTIONS: ReadonlySet<string> = new Set(["read", "write", "delete", "admin"])

function parseAction(word: string): Action {
  return KNOWN_ACTIONS.has(word) ? (word as Action) : "unrecognized"
}

export type ResourceKind = "all" | "literal" | "prefix"

function parseResourceKind(word: string, context: string): ResourceKind {
  if (word !== "all" && word !== "literal" && word !== "prefix") {
    throw new CodecError(`\`${word}\` is not a recognized resource pattern kind`, context, "kind")
  }
  return word
}

/** The prefix of a resource name scoped to one stream, `stream:<name>` or
 * `stream:<name>/<local>`. */
export const STREAM_RESOURCE_PREFIX = "stream:"

/** The resource name of `stream` itself. */
export function streamResource(stream: string): string {
  return `${STREAM_RESOURCE_PREFIX}${stream}`
}

/** The name of the managed resource `local` inside `stream`,
 * `stream:<stream>/<local>`. A `local` that already starts with `stream:` is
 * returned unchanged, so scoping is idempotent and a caller-scoped name is
 * never scoped twice. */
export function scopedResource(stream: string, local: string): string {
  return local.startsWith(STREAM_RESOURCE_PREFIX)
    ? local
    : `${STREAM_RESOURCE_PREFIX}${stream}/${local}`
}

/** The stream and local part of a scoped resource name. `undefined` for a
 * bare name, for the stream resource itself, and for a name with an empty
 * stream or local part. Stream names never contain `/`, so the first `/` ends
 * the stream. */
export function splitScopedResource(
  name: string
): readonly [stream: string, local: string] | undefined {
  if (!name.startsWith(STREAM_RESOURCE_PREFIX)) return undefined
  const rest = name.slice(STREAM_RESOURCE_PREFIX.length)
  const slash = rest.indexOf("/")
  if (slash <= 0 || slash === rest.length - 1) return undefined
  return [rest.slice(0, slash), rest.slice(slash + 1)]
}

export interface ResourcePattern {
  readonly kind: ResourceKind
  readonly value: string
}

export function resourcePatternAll(): ResourcePattern {
  return { kind: "all", value: "" }
}

export function resourcePatternLiteral(value: string): ResourcePattern {
  return { kind: "literal", value }
}

export function resourcePatternPrefix(value: string): ResourcePattern {
  return { kind: "prefix", value }
}

export function resourcePatternMatches(pattern: ResourcePattern, resource?: string): boolean {
  switch (pattern.kind) {
    case "all":
      return true
    case "literal":
      return resource !== undefined && resource === pattern.value
    case "prefix":
      return resource?.startsWith(pattern.value) ?? false
  }
}

export function encodeResourcePattern(pattern: ResourcePattern): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("kind", pattern.kind)
  if (pattern.value.length > 0) map.set("value", pattern.value)
  return map
}

export function decodeResourcePattern(map: CborMap, context: string): ResourcePattern {
  const kind = map.has("kind")
    ? parseResourceKind(field.requiredString(map, "kind", context), context)
    : "all"
  const value = field.optionalString(map, "value", context) ?? ""
  return { kind, value }
}

export interface Grant {
  readonly effect: Effect
  readonly feature: Feature
  readonly action: Action
  readonly resource: ResourcePattern
}

export function encodeGrant(grant: Grant): Map<string, unknown> {
  return new Map<string, unknown>([
    ["effect", grant.effect],
    ["feature", grant.feature],
    ["action", grant.action],
    ["resource", encodeResourcePattern(grant.resource)]
  ])
}

export function decodeGrant(map: CborMap, context: string): Grant {
  const resourceMap = field.optionalMap(map, "resource", context)
  return {
    effect: parseEffect(field.requiredString(map, "effect", context), context),
    feature: parseFeature(field.requiredString(map, "feature", context)),
    action: parseAction(field.requiredString(map, "action", context)),
    resource:
      resourceMap !== undefined
        ? decodeResourcePattern(resourceMap, `${context}.resource`)
        : resourcePatternAll()
  }
}

export interface Role {
  readonly name: string
  readonly grants: readonly Grant[]
}

export function encodeRole(role: Role): Map<string, unknown> {
  return new Map<string, unknown>([
    ["name", role.name],
    ["grants", role.grants.map((grant) => encodeGrant(grant))]
  ])
}

export function decodeRole(map: CborMap, context: string): Role {
  return {
    name: field.requiredString(map, "name", context),
    grants: field.requiredArray(map, "grants", context, (item, index) =>
      decodeGrant(
        expectMap(item, `${context}.grants[${String(index)}]`),
        `${context}.grants[${String(index)}]`
      )
    )
  }
}

/** The roles bound to one user, by the server-stamped user id. */
export interface RoleBinding {
  readonly userId: number
  readonly roles: readonly string[]
}

export function encodeRoleBinding(binding: RoleBinding): Map<string, unknown> {
  return new Map<string, unknown>([
    ["user_id", binding.userId],
    ["roles", [...binding.roles]]
  ])
}

export function decodeRoleBinding(map: CborMap, context: string): RoleBinding {
  return {
    userId: field.requiredU32(map, "user_id", context),
    roles: field.requiredArray(map, "roles", context, (item, index) =>
      expectString(item, `${context}.roles[${String(index)}]`)
    )
  }
}

export function validateRoleName(name: string): void {
  if (name.length === 0) {
    throw new InvalidError("role name must not be empty")
  }
  const bytes = new TextEncoder().encode(name)
  if (bytes.length > MAX_ROLE_NAME_BYTES) {
    throw new InvalidError(
      `role name is ${String(bytes.length)}B, exceeds cap ${String(MAX_ROLE_NAME_BYTES)}B`
    )
  }
  if (!/^[A-Za-z0-9._-]+$/.test(name)) {
    throw new InvalidError(
      "role name has a disallowed byte: allowed are ASCII letters, digits, '-', '_', '.'"
    )
  }
}

export function featureAction(code: number): readonly [Feature, Action] | undefined {
  switch (code) {
    case AGDX_QUERY_CODE:
      return ["query", "read"]
    case AGDX_DESTINATION_GET_CODE:
    case AGDX_DESTINATION_LIST_CODE:
    case AGDX_QUERY_ROUTE_LIST_CODE:
      return ["destination", "read"]
    case AGDX_GET_PROJECTION_CODE:
    case AGDX_LIST_PROJECTIONS_CODE:
    case AGDX_GET_SCHEMA_CODE:
    case AGDX_LIST_SCHEMAS_CODE:
    case AGDX_DECODE_RECORD_CODE:
      return ["projection", "read"]
    case AGDX_REGISTER_SCHEMA_CODE:
      return ["projection", "admin"]
    case AGDX_KV_GET_CODE:
    case AGDX_KV_SCAN_CODE:
    case AGDX_KV_NAMESPACES_CODE:
    case AGDX_KV_EXISTS_CODE:
      return ["kv", "read"]
    // A fenced CAS additionally requires `kv_fence:read` on its coordination
    // namespace. This map carries the primary target-namespace grant and the
    // enforcer checks the fence grant beside it.
    case AGDX_KV_SET_CODE:
    case AGDX_KV_CAS_CODE:
    case AGDX_KV_CAS_FENCED_CODE:
    case AGDX_KV_PATCH_CODE:
    case AGDX_KV_EXPIRE_CODE:
    case AGDX_KV_COPY_CODE:
    case AGDX_KV_MOVE_CODE:
      return ["kv", "write"]
    // Lease lifecycle is its own feature: a `kv:write` grant must never imply
    // acquiring, renewing, or releasing the lease that fences it.
    case AGDX_KV_LEASE_CODE:
    case AGDX_KV_LEASE_RENEW_CODE:
    case AGDX_KV_RELEASE_CODE:
      return ["kv_lease", "admin"]
    case AGDX_KV_DELETE_CODE:
    case AGDX_KV_DELETE_MANY_CODE:
      return ["kv", "delete"]
    case AGDX_FORK_LIST_CODE:
      return ["fork", "read"]
    case AGDX_FORK_CREATE_CODE:
    case AGDX_FORK_PUT_CODE:
      return ["fork", "write"]
    case AGDX_FORK_PROMOTE_CODE:
      return ["fork", "admin"]
    case AGDX_FORK_DELETE_CODE:
      return ["fork", "delete"]
    case AGDX_GRAPH_QUERY_CODE:
    case AGDX_GRAPH_NEIGHBORS_CODE:
      return ["graph", "read"]
    case AGDX_GRAPH_UPSERT_CODE:
      return ["graph", "write"]
    case AGDX_SESSION_GET_CODE:
    case AGDX_SESSION_LIST_CODE:
    case AGDX_SESSION_EVENTS_CODE:
    case AGDX_SESSION_STATE_CODE:
    case AGDX_SESSION_LINKS_CODE:
    case AGDX_SESSION_SOURCES_CODE:
    case AGDX_SESSION_CHANGES_CODE:
      return ["session", "read"]
    case AGDX_GET_FILTER_CODE:
    case AGDX_LIST_FILTERS_CODE:
    case AGDX_LIST_FILTER_REVISIONS_CODE:
    case AGDX_GET_FILTER_BINDING_CODE:
    case AGDX_LIST_FILTER_BINDINGS_CODE:
    case AGDX_FILTER_OPERATION_CODE:
      return ["filter", "read"]
    default:
      return undefined
  }
}

const FEATURE_ORDINALS = {
  kv: 0,
  memory: 1,
  projection: 2,
  fork: 3,
  graph: 4,
  query: 5,
  agent: 6,
  workflow: 7,
  destination: 8,
  checkpoint: 9,
  authz: 10,
  kv_lease: 11,
  kv_fence: 12,
  filter: 13,
  session: 14,
  unrecognized: 15
} as const satisfies Readonly<Record<Feature, number>>
const ACTION_ORDINALS = {
  read: 0,
  write: 1,
  delete: 2,
  admin: 3,
  unrecognized: 4
} as const satisfies Readonly<Record<Action, number>>

export const ACTION_COUNT = 5

export function actionIndex(feature: Feature, action: Action): number {
  return FEATURE_ORDINALS[feature] * ACTION_COUNT + ACTION_ORDINALS[action]
}

export function grantsAllow(
  grants: readonly Grant[],
  feature: Feature,
  action: Action,
  resource?: string
): boolean {
  let allowed = false
  for (const grant of grants) {
    if (
      grant.feature === feature &&
      grant.action === action &&
      resourcePatternMatches(grant.resource, resource)
    ) {
      if (grant.effect === "deny") return false
      allowed = true
    }
  }
  return allowed
}

export function delegatedAllow(
  agent: readonly Grant[],
  user: readonly Grant[],
  feature: Feature,
  action: Action,
  resource?: string
): boolean {
  return (
    grantsAllow(agent, feature, action, resource) && grantsAllow(user, feature, action, resource)
  )
}

export interface WhoamiReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
}

export function encodeWhoamiReq(req: WhoamiReq = {}): Map<string, unknown> {
  return new Map<string, unknown>([["v", req.v ?? AUTHZ_OP_VERSION]])
}

export function decodeWhoamiReq(map: CborMap, context: string): WhoamiReq {
  return { v: field.requiredU32(map, "v", context) }
}

export interface WhoamiReply {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly roles: readonly string[]
  readonly grants: readonly Grant[]
}

export function encodeWhoamiReply(reply: WhoamiReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", reply.v ?? AUTHZ_OP_VERSION],
    ["roles", [...reply.roles]],
    ["grants", reply.grants.map((grant) => encodeGrant(grant))]
  ])
}

export function decodeWhoamiReply(map: CborMap, context: string): WhoamiReply {
  return {
    v: field.requiredU32(map, "v", context),
    roles: field.requiredArray(map, "roles", context, (item) => expectString(item, context)),
    grants: field.requiredArray(map, "grants", context, (item, index) =>
      decodeGrant(
        expectMap(item, `${context}.grants[${String(index)}]`),
        `${context}.grants[${String(index)}]`
      )
    )
  }
}

export interface ListRolesReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly namePrefix?: string
  readonly search?: string
}

export function encodeListRolesReq(req: ListRolesReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", req.v ?? AUTHZ_OP_VERSION)
  if (req.namePrefix !== undefined) map.set("name_prefix", req.namePrefix)
  if (req.search !== undefined) map.set("search", req.search)
  return map
}

export function decodeListRolesReq(map: CborMap, context: string): ListRolesReq {
  const namePrefix = field.optionalString(map, "name_prefix", context)
  const search = field.optionalString(map, "search", context)
  return {
    v: field.requiredU32(map, "v", context),
    ...(namePrefix !== undefined ? { namePrefix } : {}),
    ...(search !== undefined ? { search } : {})
  }
}

export interface ListRolesReply {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly roles: readonly Role[]
}

export function encodeListRolesReply(reply: ListRolesReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", reply.v ?? AUTHZ_OP_VERSION],
    ["roles", reply.roles.map((role) => encodeRole(role))]
  ])
}

export function decodeListRolesReply(map: CborMap, context: string): ListRolesReply {
  return {
    v: field.requiredU32(map, "v", context),
    roles: field.requiredArray(map, "roles", context, (item, index) =>
      decodeRole(
        expectMap(item, `${context}.roles[${String(index)}]`),
        `${context}.roles[${String(index)}]`
      )
    )
  }
}

export interface GetRoleReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly name: string
}

export function encodeGetRoleReq(req: GetRoleReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", req.v ?? AUTHZ_OP_VERSION],
    ["name", req.name]
  ])
}

export function decodeGetRoleReq(map: CborMap, context: string): GetRoleReq {
  return {
    v: field.requiredU32(map, "v", context),
    name: field.requiredString(map, "name", context)
  }
}

export interface GetBindingsReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly userId: number
}

export function encodeGetBindingsReq(req: GetBindingsReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", req.v ?? AUTHZ_OP_VERSION],
    ["user_id", req.userId]
  ])
}

export function decodeGetBindingsReq(map: CborMap, context: string): GetBindingsReq {
  return {
    v: field.requiredU32(map, "v", context),
    userId: field.requiredU32(map, "user_id", context)
  }
}

export interface BindingsReply {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly roles: readonly string[]
}

export function encodeBindingsReply(reply: BindingsReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", reply.v ?? AUTHZ_OP_VERSION],
    ["roles", [...reply.roles]]
  ])
}

export function decodeBindingsReply(map: CborMap, context: string): BindingsReply {
  return {
    v: field.requiredU32(map, "v", context),
    roles: field.requiredArray(map, "roles", context, (item) => expectString(item, context))
  }
}

export interface DefineRoleReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly role: Role
  readonly mutationId?: string
}

export function encodeDefineRoleReq(req: DefineRoleReq): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", req.v ?? AUTHZ_OP_VERSION],
    ["role", encodeRole(req.role)]
  ])
  if (req.mutationId !== undefined) map.set("mutation_id", req.mutationId)
  return map
}

export function decodeDefineRoleReq(map: CborMap, context: string): DefineRoleReq {
  const mutationId = field.optionalString(map, "mutation_id", context)
  return {
    v: field.requiredU32(map, "v", context),
    role: decodeRole(field.requiredMap(map, "role", context), `${context}.role`),
    ...(mutationId !== undefined ? { mutationId } : {})
  }
}

export interface DeleteRoleReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly name: string
  readonly mutationId?: string
}

export function encodeDeleteRoleReq(req: DeleteRoleReq): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", req.v ?? AUTHZ_OP_VERSION],
    ["name", req.name]
  ])
  if (req.mutationId !== undefined) map.set("mutation_id", req.mutationId)
  return map
}

export function decodeDeleteRoleReq(map: CborMap, context: string): DeleteRoleReq {
  const mutationId = field.optionalString(map, "mutation_id", context)
  return {
    v: field.requiredU32(map, "v", context),
    name: field.requiredString(map, "name", context),
    ...(mutationId !== undefined ? { mutationId } : {})
  }
}

export interface BindRolesReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly userId: number
  readonly roles: readonly string[]
  readonly expectRevision?: bigint
  readonly mutationId?: string
}

export function encodeBindRolesReq(req: BindRolesReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", req.v ?? AUTHZ_OP_VERSION)
  map.set("user_id", req.userId)
  map.set("roles", [...req.roles])
  if (req.expectRevision !== undefined) map.set("expect_revision", req.expectRevision)
  if (req.mutationId !== undefined) map.set("mutation_id", req.mutationId)
  return map
}

export function decodeBindRolesReq(map: CborMap, context: string): BindRolesReq {
  const expectRevision = field.optionalU64(map, "expect_revision", context)
  const mutationId = field.optionalString(map, "mutation_id", context)
  return {
    v: field.requiredU32(map, "v", context),
    userId: field.requiredU32(map, "user_id", context),
    roles: field.requiredArray(map, "roles", context, (item) => expectString(item, context)),
    ...(expectRevision !== undefined ? { expectRevision } : {}),
    ...(mutationId !== undefined ? { mutationId } : {})
  }
}

export type AuthzSubject =
  | { readonly kind: "role"; readonly name: string }
  | { readonly kind: "binding"; readonly userId: number }
  | { readonly kind: "all" }

export function encodeAuthzSubject(subject: AuthzSubject): unknown {
  switch (subject.kind) {
    case "role":
      return new Map([["role", subject.name]])
    case "binding":
      return new Map([["binding", new Map<string, unknown>([["user_id", subject.userId]])]])
    case "all":
      return "all"
  }
}

export function decodeAuthzSubject(value: unknown, context: string): AuthzSubject {
  if (typeof value === "string") {
    if (value === "all") return { kind: "all" }
    throw new CodecError(`\`${value}\` is not a recognized authz subject`, context, "subject")
  }
  const map = expectMap(value, context)
  if (map.has("role")) {
    return { kind: "role", name: expectString(map.get("role"), context) }
  }
  if (map.has("binding")) {
    const bindingMap = expectMap(map.get("binding"), context)
    return { kind: "binding", userId: field.requiredU32(bindingMap, "user_id", context) }
  }
  throw new CodecError("authz subject has no recognized tag", context, "subject")
}

export interface AuthzHistoryReq {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly subject: AuthzSubject
  readonly afterRevision?: bigint
  readonly limit: number
}

export function encodeAuthzHistoryReq(req: AuthzHistoryReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", req.v ?? AUTHZ_OP_VERSION)
  map.set("subject", encodeAuthzSubject(req.subject))
  if (req.afterRevision !== undefined) map.set("after_revision", req.afterRevision)
  map.set("limit", req.limit)
  return map
}

export function decodeAuthzHistoryReq(map: CborMap, context: string): AuthzHistoryReq {
  const afterRevision = field.optionalU64(map, "after_revision", context)
  return {
    v: field.requiredU32(map, "v", context),
    subject: decodeAuthzSubject(map.get("subject"), context),
    ...(afterRevision !== undefined ? { afterRevision } : {}),
    limit: field.requiredU32(map, "limit", context)
  }
}

export type AuthzEventKind =
  | { readonly kind: "roleDefined"; readonly name: string }
  | { readonly kind: "roleDeleted"; readonly name: string }
  | { readonly kind: "rolesBound"; readonly userId: number; readonly roles: readonly string[] }

export function encodeAuthzEventKind(op: AuthzEventKind): unknown {
  switch (op.kind) {
    case "roleDefined":
      return new Map([["role_defined", op.name]])
    case "roleDeleted":
      return new Map([["role_deleted", op.name]])
    case "rolesBound":
      return new Map([
        [
          "roles_bound",
          new Map<string, unknown>([
            ["user_id", op.userId],
            ["roles", [...op.roles]]
          ])
        ]
      ])
  }
}

export function decodeAuthzEventKind(value: unknown, context: string): AuthzEventKind {
  const map = expectMap(value, context)
  if (map.has("role_defined")) {
    return { kind: "roleDefined", name: expectString(map.get("role_defined"), context) }
  }
  if (map.has("role_deleted")) {
    return { kind: "roleDeleted", name: expectString(map.get("role_deleted"), context) }
  }
  if (map.has("roles_bound")) {
    const boundMap = expectMap(map.get("roles_bound"), context)
    return {
      kind: "rolesBound",
      userId: field.requiredU32(boundMap, "user_id", context),
      roles: field.requiredArray(boundMap, "roles", context, (item) => expectString(item, context))
    }
  }
  throw new CodecError("authz event has no recognized tag", context, "op")
}

export interface AuthzEvent {
  readonly revision: bigint
  readonly actor: string
  readonly atMicros: bigint
  readonly op: AuthzEventKind
}

export function encodeAuthzEvent(event: AuthzEvent): Map<string, unknown> {
  return new Map<string, unknown>([
    ["revision", event.revision],
    ["actor", event.actor],
    ["at_micros", event.atMicros],
    ["op", encodeAuthzEventKind(event.op)]
  ])
}

export function decodeAuthzEvent(map: CborMap, context: string): AuthzEvent {
  return {
    revision: field.requiredU64(map, "revision", context),
    actor: field.requiredString(map, "actor", context),
    atMicros: field.requiredU64(map, "at_micros", context),
    op: decodeAuthzEventKind(map.get("op"), context)
  }
}

export interface AuthzHistoryReply {
  /** The operation version, the current one when absent. */
  readonly v?: number
  readonly events: readonly AuthzEvent[]
  readonly nextAfterRevision?: bigint
}

export function encodeAuthzHistoryReply(reply: AuthzHistoryReply): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", reply.v ?? AUTHZ_OP_VERSION)
  map.set(
    "events",
    reply.events.map((event) => encodeAuthzEvent(event))
  )
  if (reply.nextAfterRevision !== undefined) map.set("next_after_revision", reply.nextAfterRevision)
  return map
}

export function decodeAuthzHistoryReply(map: CborMap, context: string): AuthzHistoryReply {
  const nextAfterRevision = field.optionalU64(map, "next_after_revision", context)
  return {
    v: field.requiredU32(map, "v", context),
    events: field.requiredArray(map, "events", context, (item, index) =>
      decodeAuthzEvent(
        expectMap(item, `${context}.events[${String(index)}]`),
        `${context}.events[${String(index)}]`
      )
    ),
    ...(nextAfterRevision !== undefined ? { nextAfterRevision } : {})
  }
}

export type AuthzError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "unauthorized" }
  | { readonly kind: "unknownRole"; readonly name: string }
  | { readonly kind: "invalidName"; readonly name: string }
  | { readonly kind: "conflict"; readonly currentRevision: bigint }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  /** A grant refused because it reaches past one stream while the server runs
   * with stream tenancy, for example a wildcard or a prefix spanning streams. */
  | { readonly kind: "tenancyViolation"; readonly message: string }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeAuthzError(error: AuthzError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "unauthorized":
      return "Unauthorized"
    case "unknownRole":
      return new Map([["UnknownRole", error.name]])
    case "invalidName":
      return new Map([["InvalidName", error.name]])
    case "conflict":
      return new Map([
        ["Conflict", new Map<string, unknown>([["current_revision", error.currentRevision]])]
      ])
    case "version":
      return new Map([
        [
          "Version",
          new Map<string, unknown>([
            ["expected", error.expected],
            ["got", error.got]
          ])
        ]
      ])
    case "tenancyViolation":
      return new Map([["TenancyViolation", error.message]])
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

/** The unified result code of an authorization failure. */
export function authzErrorResultCode(error: AuthzError): ResultCode {
  switch (error.kind) {
    case "unsupported":
      return { kind: "known", name: "Unsupported" }
    case "unauthorized":
      return { kind: "known", name: "Forbidden" }
    case "unknownRole":
      return { kind: "known", name: "NotFound" }
    case "invalidName":
    case "tenancyViolation":
      return { kind: "known", name: "InvalidArgument" }
    case "conflict":
      return { kind: "known", name: "Conflict" }
    case "version":
      return { kind: "known", name: "VersionSkew" }
    case "unrecognized":
      return { kind: "known", name: "Backend" }
  }
}

export function decodeAuthzError(value: unknown, context: string): AuthzError {
  if (typeof value === "string") {
    return value === "Unauthorized"
      ? { kind: "unauthorized" }
      : { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "UnknownRole":
      return { kind: "unknownRole", name: expectString(inner, context) }
    case "InvalidName":
      return { kind: "invalidName", name: expectString(inner, context) }
    case "TenancyViolation":
      return { kind: "tenancyViolation", message: expectString(inner, context) }
    case "Conflict": {
      const conflictMap = expectMap(inner, context)
      return {
        kind: "conflict",
        currentRevision: field.requiredU64(conflictMap, "current_revision", context)
      }
    }
    case "Version": {
      const versionMap = expectMap(inner, context)
      return {
        kind: "version",
        expected: field.requiredU32(versionMap, "expected", context),
        got: field.requiredU32(versionMap, "got", context)
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export type AuthzReply =
  | { readonly kind: "ok" }
  | { readonly kind: "whoami"; readonly reply: WhoamiReply }
  | { readonly kind: "roles"; readonly reply: ListRolesReply }
  | { readonly kind: "role"; readonly role?: Role }
  | { readonly kind: "bindings"; readonly reply: BindingsReply }
  | { readonly kind: "history"; readonly reply: AuthzHistoryReply }
  | { readonly kind: "err"; readonly error: AuthzError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeAuthzReply(reply: AuthzReply): unknown {
  switch (reply.kind) {
    case "ok":
      return "Ok"
    case "whoami":
      return new Map([["Whoami", encodeWhoamiReply(reply.reply)]])
    case "roles":
      return new Map([["Roles", encodeListRolesReply(reply.reply)]])
    case "role":
      return new Map([["Role", reply.role !== undefined ? encodeRole(reply.role) : null]])
    case "bindings":
      return new Map([["Bindings", encodeBindingsReply(reply.reply)]])
    case "history":
      return new Map([["History", encodeAuthzHistoryReply(reply.reply)]])
    case "err":
      return new Map([["Err", encodeAuthzError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeAuthzReply(value: unknown, context: string): AuthzReply {
  if (typeof value === "string") {
    return value === "Ok" ? { kind: "ok" } : { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Whoami":
      return { kind: "whoami", reply: decodeWhoamiReply(expectMap(inner, context), context) }
    case "Roles":
      return { kind: "roles", reply: decodeListRolesReply(expectMap(inner, context), context) }
    case "Role":
      return {
        kind: "role",
        ...(inner !== null && inner !== undefined
          ? { role: decodeRole(expectMap(inner, context), context) }
          : {})
      }
    case "Bindings":
      return { kind: "bindings", reply: decodeBindingsReply(expectMap(inner, context), context) }
    case "History":
      return { kind: "history", reply: decodeAuthzHistoryReply(expectMap(inner, context), context) }
    case "Err":
      return { kind: "err", error: decodeAuthzError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}
