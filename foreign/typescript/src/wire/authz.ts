import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"
import {
  AGDX_AGENT_CANCEL_CODE,
  AGDX_AGENT_LIST_CODE,
  AGDX_AGENT_STATUS_CODE,
  AGDX_AGENT_SUBMIT_CODE,
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
  AUTHZ_OP_VERSION
} from "./codes.js"
import { MAX_ROLE_NAME_BYTES } from "./limits.js"

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
  | "authz"
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
  "authz"
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
    case AGDX_KV_SET_CODE:
    case AGDX_KV_CAS_CODE:
    case AGDX_KV_CAS_FENCED_CODE:
    case AGDX_KV_PATCH_CODE:
    case AGDX_KV_EXPIRE_CODE:
    case AGDX_KV_COPY_CODE:
    case AGDX_KV_MOVE_CODE:
    case AGDX_KV_LEASE_CODE:
    case AGDX_KV_RELEASE_CODE:
      return ["kv", "write"]
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
    case AGDX_AGENT_STATUS_CODE:
    case AGDX_AGENT_LIST_CODE:
      return ["agent", "read"]
    case AGDX_AGENT_SUBMIT_CODE:
      return ["agent", "write"]
    case AGDX_AGENT_CANCEL_CODE:
      return ["agent", "delete"]
    default:
      return undefined
  }
}

const FEATURES: readonly Feature[] = [
  "kv",
  "memory",
  "projection",
  "fork",
  "graph",
  "query",
  "agent",
  "workflow",
  "authz",
  "unrecognized"
]
const ACTIONS: readonly Action[] = ["read", "write", "delete", "admin", "unrecognized"]

export const ACTION_COUNT = ACTIONS.length

export function actionIndex(feature: Feature, action: Action): number {
  const featureOrdinal = FEATURES.indexOf(feature)
  const actionOrdinal = ACTIONS.indexOf(action)
  return featureOrdinal * ACTION_COUNT + actionOrdinal
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

export function encodeWhoamiReq(): Map<string, unknown> {
  return new Map<string, unknown>([["v", AUTHZ_OP_VERSION]])
}

export interface WhoamiReply {
  readonly roles: readonly string[]
  readonly grants: readonly Grant[]
}

export function encodeWhoamiReply(reply: WhoamiReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["roles", [...reply.roles]],
    ["grants", reply.grants.map((grant) => encodeGrant(grant))]
  ])
}

export function decodeWhoamiReply(map: CborMap, context: string): WhoamiReply {
  return {
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
  readonly namePrefix?: string
  readonly search?: string
}

export function encodeListRolesReq(req: ListRolesReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AUTHZ_OP_VERSION)
  if (req.namePrefix !== undefined) map.set("name_prefix", req.namePrefix)
  if (req.search !== undefined) map.set("search", req.search)
  return map
}

export function decodeListRolesReq(map: CborMap, context: string): ListRolesReq {
  const namePrefix = field.optionalString(map, "name_prefix", context)
  const search = field.optionalString(map, "search", context)
  return {
    ...(namePrefix !== undefined ? { namePrefix } : {}),
    ...(search !== undefined ? { search } : {})
  }
}

export interface ListRolesReply {
  readonly roles: readonly Role[]
}

export function encodeListRolesReply(reply: ListRolesReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["roles", reply.roles.map((role) => encodeRole(role))]
  ])
}

export function decodeListRolesReply(map: CborMap, context: string): ListRolesReply {
  return {
    roles: field.requiredArray(map, "roles", context, (item, index) =>
      decodeRole(
        expectMap(item, `${context}.roles[${String(index)}]`),
        `${context}.roles[${String(index)}]`
      )
    )
  }
}

export interface GetRoleReq {
  readonly name: string
}

export function encodeGetRoleReq(req: GetRoleReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["name", req.name]
  ])
}

export function decodeGetRoleReq(map: CborMap, context: string): GetRoleReq {
  return { name: field.requiredString(map, "name", context) }
}

export interface GetBindingsReq {
  readonly userId: number
}

export function encodeGetBindingsReq(req: GetBindingsReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["user_id", req.userId]
  ])
}

export function decodeGetBindingsReq(map: CborMap, context: string): GetBindingsReq {
  return { userId: field.requiredU32(map, "user_id", context) }
}

export interface BindingsReply {
  readonly roles: readonly string[]
}

export function encodeBindingsReply(reply: BindingsReply): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["roles", [...reply.roles]]
  ])
}

export function decodeBindingsReply(map: CborMap, context: string): BindingsReply {
  return {
    roles: field.requiredArray(map, "roles", context, (item) => expectString(item, context))
  }
}

export interface DefineRoleReq {
  readonly role: Role
}

export function encodeDefineRoleReq(req: DefineRoleReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["role", encodeRole(req.role)]
  ])
}

export function decodeDefineRoleReq(map: CborMap, context: string): DefineRoleReq {
  return { role: decodeRole(field.requiredMap(map, "role", context), `${context}.role`) }
}

export interface DeleteRoleReq {
  readonly name: string
}

export function encodeDeleteRoleReq(req: DeleteRoleReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AUTHZ_OP_VERSION],
    ["name", req.name]
  ])
}

export function decodeDeleteRoleReq(map: CborMap, context: string): DeleteRoleReq {
  return { name: field.requiredString(map, "name", context) }
}

export interface BindRolesReq {
  readonly userId: number
  readonly roles: readonly string[]
  readonly expectRevision?: bigint
}

export function encodeBindRolesReq(req: BindRolesReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AUTHZ_OP_VERSION)
  map.set("user_id", req.userId)
  map.set("roles", [...req.roles])
  if (req.expectRevision !== undefined) map.set("expect_revision", req.expectRevision)
  return map
}

export function decodeBindRolesReq(map: CborMap, context: string): BindRolesReq {
  const expectRevision = field.optionalU64(map, "expect_revision", context)
  return {
    userId: field.requiredU32(map, "user_id", context),
    roles: field.requiredArray(map, "roles", context, (item) => expectString(item, context)),
    ...(expectRevision !== undefined ? { expectRevision } : {})
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
  readonly subject: AuthzSubject
  readonly afterRevision?: bigint
  readonly limit: number
}

export function encodeAuthzHistoryReq(req: AuthzHistoryReq): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AUTHZ_OP_VERSION)
  map.set("subject", encodeAuthzSubject(req.subject))
  if (req.afterRevision !== undefined) map.set("after_revision", req.afterRevision)
  map.set("limit", req.limit)
  return map
}

export function decodeAuthzHistoryReq(map: CborMap, context: string): AuthzHistoryReq {
  const afterRevision = field.optionalU64(map, "after_revision", context)
  return {
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
  readonly events: readonly AuthzEvent[]
  readonly nextAfterRevision?: bigint
}

export function encodeAuthzHistoryReply(reply: AuthzHistoryReply): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AUTHZ_OP_VERSION)
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
    case "unrecognized":
      return new Map([[error.tag, error.value]])
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
