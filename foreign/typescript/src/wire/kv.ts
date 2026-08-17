import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  expectArray,
  expectBoolean,
  expectMap,
  expectString,
  expectU32,
  expectU64,
  field,
  singleVariantTag
} from "./cbor.js"
import { KV_LEASE_OP_VERSION, KV_OP_VERSION } from "./codes.js"
import { decodeSourceRef, encodeSourceRef, type SourceRef } from "./graph.js"
import {
  MAX_HOLDER_ID_BYTES,
  MAX_KEY_BYTES,
  MAX_LEASE_TTL_MICROS,
  MAX_NAMESPACE_BYTES,
  MAX_VALUE_BYTES,
  MIN_LEASE_TTL_MICROS
} from "./limits.js"
import {
  decodeMutationPosition,
  encodeMutationPosition,
  type MutationPosition
} from "./mutation.js"

export interface MemoryRowScope {
  readonly kind?: string
  readonly agent?: string
  readonly user?: string
  readonly app?: string
  readonly conversation?: string
  readonly source?: SourceRef
}

export function encodeMemoryRowScope(scope: MemoryRowScope): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (scope.kind !== undefined) map.set("kind", scope.kind)
  if (scope.agent !== undefined) map.set("agent", scope.agent)
  if (scope.user !== undefined) map.set("user", scope.user)
  if (scope.app !== undefined) map.set("app", scope.app)
  if (scope.conversation !== undefined) map.set("conversation", scope.conversation)
  if (scope.source !== undefined) map.set("source", encodeSourceRef(scope.source))
  return map
}

export function decodeMemoryRowScope(map: CborMap, context: string): MemoryRowScope {
  const kind = field.optionalString(map, "kind", context)
  const agent = field.optionalString(map, "agent", context)
  const user = field.optionalString(map, "user", context)
  const app = field.optionalString(map, "app", context)
  const conversation = field.optionalString(map, "conversation", context)
  return {
    ...(kind !== undefined ? { kind } : {}),
    ...(agent !== undefined ? { agent } : {}),
    ...(user !== undefined ? { user } : {}),
    ...(app !== undefined ? { app } : {}),
    ...(conversation !== undefined ? { conversation } : {}),
    ...(map.has("source")
      ? { source: decodeSourceRef(map.get("source"), `${context}.source`) }
      : {})
  }
}

export interface KvEntry {
  readonly key: Uint8Array
  readonly value: Uint8Array
  readonly expiresAtMicros?: bigint
  readonly version: bigint
  readonly scope?: MemoryRowScope
  readonly source?: SourceRef
}

export function encodeKvEntry(entry: KvEntry): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("key", entry.key)
  map.set("value", entry.value)
  if (entry.expiresAtMicros !== undefined) map.set("expires_at_micros", entry.expiresAtMicros)
  if (entry.version !== 0n) map.set("version", entry.version)
  if (entry.scope !== undefined) map.set("scope", encodeMemoryRowScope(entry.scope))
  if (entry.source !== undefined) map.set("source", encodeSourceRef(entry.source))
  return map
}

export function decodeKvEntry(map: CborMap, context: string): KvEntry {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  const version = field.optionalU64(map, "version", context)
  const scopeMap = field.optionalMap(map, "scope", context)
  return {
    key: field.requiredBytes(map, "key", context),
    value: field.requiredBytes(map, "value", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
    version: version ?? 0n,
    ...(scopeMap !== undefined
      ? { scope: decodeMemoryRowScope(scopeMap, `${context}.scope`) }
      : {}),
    ...(map.has("source")
      ? { source: decodeSourceRef(map.get("source"), `${context}.source`) }
      : {})
  }
}

export function kvEntryKeyString(entry: KvEntry): string | undefined {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(entry.key)
  } catch {
    return undefined
  }
}

export interface KvPage {
  readonly entries: readonly KvEntry[]
  readonly cursor?: Uint8Array
}

export function encodeKvPage(page: KvPage): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set(
    "entries",
    page.entries.map((entry) => encodeKvEntry(entry))
  )
  if (page.cursor !== undefined) map.set("cursor", page.cursor)
  return map
}

export function decodeKvPage(map: CborMap, context: string): KvPage {
  const cursor = field.optionalBytes(map, "cursor", context)
  return {
    entries: field.requiredArray(map, "entries", context, (item, index) =>
      decodeKvEntry(
        expectMap(item, `${context}.entries[${String(index)}]`),
        `${context}.entries[${String(index)}]`
      )
    ),
    ...(cursor !== undefined ? { cursor } : {})
  }
}

export interface KvGet {
  readonly namespace: string
  readonly key: Uint8Array
  readonly ifNoneMatch?: bigint
  /** Barriered read: the answering fold must have applied at least this
   * position, else the reply is the typed `stale` error, never an absent
   * value. Send only when the server advertises `KV_FENCED_LEASES`. */
  readonly minPosition?: MutationPosition
}

export function encodeKvGet(get: KvGet): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", get.namespace)
  map.set("key", get.key)
  if (get.ifNoneMatch !== undefined) map.set("if_none_match", get.ifNoneMatch)
  if (get.minPosition !== undefined)
    map.set("min_position", encodeMutationPosition(get.minPosition))
  return map
}

export function decodeKvGet(map: CborMap, context: string): KvGet {
  const ifNoneMatch = field.optionalU64(map, "if_none_match", context)
  const minPositionMap = field.optionalMap(map, "min_position", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    ...(ifNoneMatch !== undefined ? { ifNoneMatch } : {}),
    ...(minPositionMap !== undefined
      ? { minPosition: decodeMutationPosition(minPositionMap, `${context}.min_position`) }
      : {})
  }
}

export interface KvSet {
  readonly namespace: string
  readonly key: Uint8Array
  readonly value: Uint8Array
  readonly expiresAtMicros?: bigint
}

export function encodeKvSet(set: KvSet): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", set.namespace)
  map.set("key", set.key)
  map.set("value", set.value)
  if (set.expiresAtMicros !== undefined) map.set("expires_at_micros", set.expiresAtMicros)
  return map
}

export function decodeKvSet(map: CborMap, context: string): KvSet {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    value: field.requiredBytes(map, "value", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {})
  }
}

export type CasExpect =
  { readonly kind: "match"; readonly version: bigint } | { readonly kind: "absent" }

export function encodeCasExpect(expect: CasExpect): unknown {
  return expect.kind === "match" ? new Map([["Match", expect.version]]) : "Absent"
}

export function decodeCasExpect(value: unknown, context: string): CasExpect {
  if (typeof value === "string") {
    if (value === "Absent") return { kind: "absent" }
    throw new CodecError(`\`${value}\` is not a recognized cas expectation`, context, "expect")
  }
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "Match") {
    return { kind: "match", version: expectU64(inner, context) }
  }
  throw new CodecError(`\`${tag}\` is not a recognized cas expectation`, context, "expect")
}

export interface KvCas {
  readonly namespace: string
  readonly key: Uint8Array
  readonly value: Uint8Array
  readonly expiresAtMicros?: bigint
  readonly expect: CasExpect
}

export function encodeKvCas(cas: KvCas): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", cas.namespace)
  map.set("key", cas.key)
  map.set("value", cas.value)
  if (cas.expiresAtMicros !== undefined) map.set("expires_at_micros", cas.expiresAtMicros)
  map.set("expect", encodeCasExpect(cas.expect))
  return map
}

export function decodeKvCas(map: CborMap, context: string): KvCas {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    value: field.requiredBytes(map, "value", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
    expect: decodeCasExpect(map.get("expect"), context)
  }
}

export interface KvCasFenced {
  readonly namespace: string
  readonly key: Uint8Array
  readonly value: Uint8Array
  readonly expiresAtMicros?: bigint
  readonly expect: CasExpect
  /** The coordination namespace holding the lease and fence rows, authorized
   * separately from the target namespace. */
  readonly fenceNamespace: string
  readonly fenceKey: Uint8Array
  readonly fenceToken: bigint
}

function validateKeyShape(key: Uint8Array, label: string): void {
  if (key.byteLength === 0) throw new InvalidError(`${label} must not be empty`)
  if (key.byteLength > MAX_KEY_BYTES) {
    throw new InvalidError(
      `${label} is ${String(key.byteLength)}B, exceeds cap ${String(MAX_KEY_BYTES)}B`
    )
  }
}

function validateHolderId(holderId: string): void {
  const bytes = new TextEncoder().encode(holderId)
  if (bytes.byteLength === 0) throw new InvalidError("lease holder id must not be empty")
  if (bytes.byteLength > MAX_HOLDER_ID_BYTES) {
    throw new InvalidError(
      `lease holder id is ${String(bytes.byteLength)}B, exceeds cap ${String(MAX_HOLDER_ID_BYTES)}B`
    )
  }
}

function validateKvCasFenced(cas: KvCasFenced): void {
  validateNamespace(cas.namespace)
  validateNamespace(cas.fenceNamespace)
  validateKeyShape(cas.key, "key-value key")
  validateKeyShape(cas.fenceKey, "fence key")
  if (cas.value.byteLength > MAX_VALUE_BYTES) {
    throw new InvalidError(
      `value is ${String(cas.value.byteLength)}B, exceeds cap ${String(MAX_VALUE_BYTES)}B`
    )
  }
  if (cas.fenceToken === 0n) throw new InvalidError("fence token must not be zero")
}

function requireLeaseVersion(map: CborMap, context: string): void {
  const version = field.requiredU32(map, "v", context)
  if (version !== KV_LEASE_OP_VERSION) {
    throw new CodecError(
      `unsupported fenced-lease op version (expected ${String(KV_LEASE_OP_VERSION)}, got ${String(version)})`,
      context,
      "v"
    )
  }
}

function decodeValidated<T>(value: T, validate: (value: T) => void, context: string): T {
  try {
    validate(value)
    return value
  } catch (error) {
    if (error instanceof InvalidError) {
      throw new CodecError(error.message, context, "shape", { cause: error })
    }
    throw error
  }
}

export function encodeKvCasFenced(cas: KvCasFenced): Map<string, unknown> {
  validateKvCasFenced(cas)
  const map = new Map<string, unknown>()
  map.set("v", KV_LEASE_OP_VERSION)
  map.set("namespace", cas.namespace)
  map.set("key", cas.key)
  map.set("value", cas.value)
  if (cas.expiresAtMicros !== undefined) map.set("expires_at_micros", cas.expiresAtMicros)
  map.set("expect", encodeCasExpect(cas.expect))
  map.set("fence_namespace", cas.fenceNamespace)
  map.set("fence_key", cas.fenceKey)
  map.set("fence_token", cas.fenceToken)
  return map
}

export function decodeKvCasFenced(map: CborMap, context: string): KvCasFenced {
  requireLeaseVersion(map, context)
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  return decodeValidated(
    {
      namespace: field.requiredString(map, "namespace", context),
      key: field.requiredBytes(map, "key", context),
      value: field.requiredBytes(map, "value", context),
      ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
      expect: decodeCasExpect(map.get("expect"), context),
      fenceNamespace: field.requiredString(map, "fence_namespace", context),
      fenceKey: field.requiredBytes(map, "fence_key", context),
      fenceToken: field.requiredU64(map, "fence_token", context)
    },
    validateKvCasFenced,
    context
  )
}

export interface KvDelete {
  readonly namespace: string
  readonly key: Uint8Array
  readonly ifMatch?: bigint
}

export function encodeKvDelete(del: KvDelete): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", del.namespace)
  map.set("key", del.key)
  if (del.ifMatch !== undefined) map.set("if_match", del.ifMatch)
  return map
}

export function decodeKvDelete(map: CborMap, context: string): KvDelete {
  const ifMatch = field.optionalU64(map, "if_match", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    ...(ifMatch !== undefined ? { ifMatch } : {})
  }
}

export interface KvExists {
  readonly namespace: string
  readonly key: Uint8Array
}

export function encodeKvExists(exists: KvExists): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", KV_OP_VERSION],
    ["namespace", exists.namespace],
    ["key", exists.key]
  ])
}

export function decodeKvExists(map: CborMap, context: string): KvExists {
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context)
  }
}

export interface KvExpire {
  readonly namespace: string
  readonly key: Uint8Array
  readonly expiresAtMicros?: bigint
}

export function encodeKvExpire(expire: KvExpire): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", expire.namespace)
  map.set("key", expire.key)
  if (expire.expiresAtMicros !== undefined) map.set("expires_at_micros", expire.expiresAtMicros)
  return map
}

export function decodeKvExpire(map: CborMap, context: string): KvExpire {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {})
  }
}

export interface KvPatch {
  readonly namespace: string
  readonly key: Uint8Array
  readonly patch: Uint8Array
  readonly ifMatch?: bigint
}

export function encodeKvPatch(patch: KvPatch): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", patch.namespace)
  map.set("key", patch.key)
  map.set("patch", patch.patch)
  if (patch.ifMatch !== undefined) map.set("if_match", patch.ifMatch)
  return map
}

export function decodeKvPatch(map: CborMap, context: string): KvPatch {
  const ifMatch = field.optionalU64(map, "if_match", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    patch: field.requiredBytes(map, "patch", context),
    ...(ifMatch !== undefined ? { ifMatch } : {})
  }
}

export interface KvCopy {
  readonly namespace: string
  readonly key: Uint8Array
  readonly toNamespace?: string
  readonly toKey: Uint8Array
}

export function encodeKvCopy(copy: KvCopy): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", copy.namespace)
  map.set("key", copy.key)
  if (copy.toNamespace !== undefined) map.set("to_namespace", copy.toNamespace)
  map.set("to_key", copy.toKey)
  return map
}

export function decodeKvCopy(map: CborMap, context: string): KvCopy {
  const toNamespace = field.optionalString(map, "to_namespace", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    ...(toNamespace !== undefined ? { toNamespace } : {}),
    toKey: field.requiredBytes(map, "to_key", context)
  }
}

export interface KvMove {
  readonly namespace: string
  readonly key: Uint8Array
  readonly toNamespace?: string
  readonly toKey: Uint8Array
}

export function encodeKvMove(move: KvMove): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", move.namespace)
  map.set("key", move.key)
  if (move.toNamespace !== undefined) map.set("to_namespace", move.toNamespace)
  map.set("to_key", move.toKey)
  return map
}

export function decodeKvMove(map: CborMap, context: string): KvMove {
  const toNamespace = field.optionalString(map, "to_namespace", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    key: field.requiredBytes(map, "key", context),
    ...(toNamespace !== undefined ? { toNamespace } : {}),
    toKey: field.requiredBytes(map, "to_key", context)
  }
}

export interface KvLease {
  readonly namespace: string
  readonly key: Uint8Array
  readonly leaseTtlMicros: bigint
  /** Stable identity of the acquiring holder (a node or worker id). Renewal
   * and release must present the same holder. */
  readonly holderId: string
  /** Delegated acquisition for another user (requires the `kv_lease:admin`
   * grant). Absent leases for the authenticated caller. */
  readonly subjectUserId?: number
}

function validateLeaseShape(namespace: string, key: Uint8Array, holderId: string): void {
  validateNamespace(namespace)
  validateKeyShape(key, "lease key")
  validateHolderId(holderId)
}

/** The requested lifetime is bounded on both sides by the contract, not by each
 * backend: a floor so a grant survives one round trip plus the holder's renewal
 * cadence, a ceiling so a crashed holder's ownership always expires. The store
 * may grant less and never grants more, so the range is checked on the request
 * rather than clamped. */
function validateLeaseTtl(ttlMicros: bigint): void {
  const min = BigInt(MIN_LEASE_TTL_MICROS)
  const max = BigInt(MAX_LEASE_TTL_MICROS)
  if (ttlMicros < min || ttlMicros > max) {
    throw new InvalidError(
      `lease ttl is ${String(ttlMicros)}µs, outside the supported range ` +
        `${String(min)}..=${String(max)}µs`
    )
  }
}

function validateKvLease(lease: KvLease): void {
  validateLeaseShape(lease.namespace, lease.key, lease.holderId)
  validateLeaseTtl(lease.leaseTtlMicros)
}

export function encodeKvLease(lease: KvLease): Map<string, unknown> {
  validateKvLease(lease)
  const map = new Map<string, unknown>([
    ["v", KV_LEASE_OP_VERSION],
    ["namespace", lease.namespace],
    ["key", lease.key],
    ["lease_ttl_micros", lease.leaseTtlMicros],
    ["holder_id", lease.holderId]
  ] as readonly [string, unknown][])
  if (lease.subjectUserId !== undefined) map.set("subject_user_id", BigInt(lease.subjectUserId))
  return map
}

export function decodeKvLease(map: CborMap, context: string): KvLease {
  requireLeaseVersion(map, context)
  const subjectUserId = field.optionalU32(map, "subject_user_id", context)
  return decodeValidated(
    {
      namespace: field.requiredString(map, "namespace", context),
      key: field.requiredBytes(map, "key", context),
      leaseTtlMicros: field.requiredU64(map, "lease_ttl_micros", context),
      holderId: field.requiredString(map, "holder_id", context),
      ...(subjectUserId !== undefined ? { subjectUserId } : {})
    },
    validateKvLease,
    context
  )
}

export interface KvLeaseRenew {
  readonly namespace: string
  readonly key: Uint8Array
  readonly holderId: string
  readonly subjectUserId?: number
  readonly leaseToken: bigint
  readonly leaseTtlMicros: bigint
}

function validateKvLeaseRenew(renew: KvLeaseRenew): void {
  validateLeaseShape(renew.namespace, renew.key, renew.holderId)
  validateLeaseTtl(renew.leaseTtlMicros)
  if (renew.leaseToken === 0n) throw new InvalidError("lease token must not be zero")
}

export function encodeKvLeaseRenew(renew: KvLeaseRenew): Map<string, unknown> {
  validateKvLeaseRenew(renew)
  const map = new Map<string, unknown>([
    ["v", KV_LEASE_OP_VERSION],
    ["namespace", renew.namespace],
    ["key", renew.key],
    ["holder_id", renew.holderId]
  ] as readonly [string, unknown][])
  if (renew.subjectUserId !== undefined) map.set("subject_user_id", BigInt(renew.subjectUserId))
  map.set("lease_token", renew.leaseToken)
  map.set("lease_ttl_micros", renew.leaseTtlMicros)
  return map
}

export function decodeKvLeaseRenew(map: CborMap, context: string): KvLeaseRenew {
  requireLeaseVersion(map, context)
  const subjectUserId = field.optionalU32(map, "subject_user_id", context)
  return decodeValidated(
    {
      namespace: field.requiredString(map, "namespace", context),
      key: field.requiredBytes(map, "key", context),
      holderId: field.requiredString(map, "holder_id", context),
      ...(subjectUserId !== undefined ? { subjectUserId } : {}),
      leaseToken: field.requiredU64(map, "lease_token", context),
      leaseTtlMicros: field.requiredU64(map, "lease_ttl_micros", context)
    },
    validateKvLeaseRenew,
    context
  )
}

export interface KvRelease {
  readonly namespace: string
  readonly key: Uint8Array
  readonly leaseToken: bigint
  /** The same stable holder identity the grant carried. */
  readonly holderId: string
}

function validateKvRelease(release: KvRelease): void {
  validateLeaseShape(release.namespace, release.key, release.holderId)
  if (release.leaseToken === 0n) throw new InvalidError("lease token must not be zero")
}

export function encodeKvRelease(release: KvRelease): Map<string, unknown> {
  validateKvRelease(release)
  return new Map<string, unknown>([
    ["v", KV_LEASE_OP_VERSION],
    ["namespace", release.namespace],
    ["key", release.key],
    ["lease_token", release.leaseToken],
    ["holder_id", release.holderId]
  ] as readonly [string, unknown][])
}

export function decodeKvRelease(map: CborMap, context: string): KvRelease {
  requireLeaseVersion(map, context)
  return decodeValidated(
    {
      namespace: field.requiredString(map, "namespace", context),
      key: field.requiredBytes(map, "key", context),
      leaseToken: field.requiredU64(map, "lease_token", context),
      holderId: field.requiredString(map, "holder_id", context)
    },
    validateKvRelease,
    context
  )
}

export interface KvMetadata {
  readonly version: bigint
  readonly expiresAtMicros?: bigint
  readonly sizeBytes: number
}

export function encodeKvMetadata(meta: KvMetadata): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("version", meta.version)
  if (meta.expiresAtMicros !== undefined) map.set("expires_at_micros", meta.expiresAtMicros)
  map.set("size_bytes", meta.sizeBytes)
  return map
}

export function decodeKvMetadata(map: CborMap, context: string): KvMetadata {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  return {
    version: field.requiredU64(map, "version", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
    sizeBytes: field.requiredU32(map, "size_bytes", context)
  }
}

export function encodeKvNamespaces(): Map<string, unknown> {
  return new Map<string, unknown>([["v", KV_OP_VERSION]])
}

export interface KvNamespaceInfo {
  readonly namespace: string
  readonly entries: number
}

export function encodeKvNamespaceInfo(info: KvNamespaceInfo): Map<string, unknown> {
  return new Map<string, unknown>([
    ["namespace", info.namespace],
    ["entries", info.entries]
  ])
}

export function decodeKvNamespaceInfo(map: CborMap, context: string): KvNamespaceInfo {
  return {
    namespace: field.requiredString(map, "namespace", context),
    entries: field.requiredU32(map, "entries", context)
  }
}

export interface KvScan {
  readonly namespace: string
  readonly prefix?: Uint8Array
  readonly start?: Uint8Array
  readonly end?: Uint8Array
  readonly keyContains?: string
  readonly conversation?: string
  readonly limit: number
  readonly cursor?: Uint8Array
}

export function encodeKvScan(scan: KvScan): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", scan.namespace)
  if (scan.prefix !== undefined) map.set("prefix", scan.prefix)
  if (scan.start !== undefined) map.set("start", scan.start)
  if (scan.end !== undefined) map.set("end", scan.end)
  if (scan.keyContains !== undefined) map.set("key_contains", scan.keyContains)
  if (scan.conversation !== undefined) map.set("conversation", scan.conversation)
  map.set("limit", scan.limit)
  if (scan.cursor !== undefined) map.set("cursor", scan.cursor)
  return map
}

export function decodeKvScan(map: CborMap, context: string): KvScan {
  const prefix = field.optionalBytes(map, "prefix", context)
  const start = field.optionalBytes(map, "start", context)
  const end = field.optionalBytes(map, "end", context)
  const keyContains = field.optionalString(map, "key_contains", context)
  const conversation = field.optionalString(map, "conversation", context)
  const cursor = field.optionalBytes(map, "cursor", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    ...(prefix !== undefined ? { prefix } : {}),
    ...(start !== undefined ? { start } : {}),
    ...(end !== undefined ? { end } : {}),
    ...(keyContains !== undefined ? { keyContains } : {}),
    ...(conversation !== undefined ? { conversation } : {}),
    limit: field.requiredU32(map, "limit", context),
    ...(cursor !== undefined ? { cursor } : {})
  }
}

export interface KvDeleteMany {
  readonly namespace: string
  readonly prefix?: Uint8Array
  readonly start?: Uint8Array
  readonly end?: Uint8Array
  readonly keyContains?: string
  readonly conversation?: string
}

export function encodeKvDeleteMany(deleteMany: KvDeleteMany): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", KV_OP_VERSION)
  map.set("namespace", deleteMany.namespace)
  if (deleteMany.prefix !== undefined) map.set("prefix", deleteMany.prefix)
  if (deleteMany.start !== undefined) map.set("start", deleteMany.start)
  if (deleteMany.end !== undefined) map.set("end", deleteMany.end)
  if (deleteMany.keyContains !== undefined) map.set("key_contains", deleteMany.keyContains)
  if (deleteMany.conversation !== undefined) map.set("conversation", deleteMany.conversation)
  return map
}

export function decodeKvDeleteMany(map: CborMap, context: string): KvDeleteMany {
  const prefix = field.optionalBytes(map, "prefix", context)
  const start = field.optionalBytes(map, "start", context)
  const end = field.optionalBytes(map, "end", context)
  const keyContains = field.optionalString(map, "key_contains", context)
  const conversation = field.optionalString(map, "conversation", context)
  return {
    namespace: field.requiredString(map, "namespace", context),
    ...(prefix !== undefined ? { prefix } : {}),
    ...(start !== undefined ? { start } : {}),
    ...(end !== undefined ? { end } : {}),
    ...(keyContains !== undefined ? { keyContains } : {}),
    ...(conversation !== undefined ? { conversation } : {})
  }
}

export type KvOutcome =
  | { readonly kind: "value"; readonly entry?: KvEntry }
  | { readonly kind: "written" }
  | { readonly kind: "committed"; readonly version: bigint }
  | { readonly kind: "deleted"; readonly removed: boolean }
  | { readonly kind: "deletedMany"; readonly count: number }
  | { readonly kind: "page"; readonly page: KvPage }
  | { readonly kind: "namespaces"; readonly namespaces: readonly KvNamespaceInfo[] }
  | { readonly kind: "notModified" }
  | { readonly kind: "metadata"; readonly metadata?: KvMetadata }
  | { readonly kind: "versioned"; readonly version: bigint }
  | {
      readonly kind: "leased"
      readonly leaseToken: bigint
      readonly grantedTtlMicros: bigint
      readonly position: MutationPosition
    }
  | {
      readonly kind: "renewed"
      readonly leaseToken: bigint
      readonly grantedTtlMicros: bigint
      readonly position: MutationPosition
    }
  | { readonly kind: "released"; readonly wasHeld: boolean }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeKvOutcome(outcome: KvOutcome): unknown {
  switch (outcome.kind) {
    case "value":
      return new Map([["Value", outcome.entry !== undefined ? encodeKvEntry(outcome.entry) : null]])
    case "written":
      return "Written"
    case "committed":
      return new Map([["Committed", new Map<string, unknown>([["version", outcome.version]])]])
    case "deleted":
      return new Map([["Deleted", outcome.removed]])
    case "deletedMany":
      return new Map([["DeletedMany", outcome.count]])
    case "page":
      return new Map([["Page", encodeKvPage(outcome.page)]])
    case "namespaces":
      return new Map([
        ["Namespaces", outcome.namespaces.map((info) => encodeKvNamespaceInfo(info))]
      ])
    case "notModified":
      return "NotModified"
    case "metadata":
      return new Map([
        ["Metadata", outcome.metadata !== undefined ? encodeKvMetadata(outcome.metadata) : null]
      ])
    case "versioned":
      return new Map([["Versioned", new Map<string, unknown>([["version", outcome.version]])]])
    case "leased":
      return new Map([
        [
          "Leased",
          new Map<string, unknown>([
            ["lease_token", outcome.leaseToken],
            ["granted_ttl_micros", outcome.grantedTtlMicros],
            ["position", encodeMutationPosition(outcome.position)]
          ])
        ]
      ])
    case "renewed":
      return new Map([
        [
          "Renewed",
          new Map<string, unknown>([
            ["lease_token", outcome.leaseToken],
            ["granted_ttl_micros", outcome.grantedTtlMicros],
            ["position", encodeMutationPosition(outcome.position)]
          ])
        ]
      ])
    case "released":
      return new Map([["Released", outcome.wasHeld]])
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

export function decodeKvOutcome(value: unknown, context: string): KvOutcome {
  if (typeof value === "string") {
    if (value === "Written") return { kind: "written" }
    if (value === "NotModified") return { kind: "notModified" }
    return { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Value":
      return {
        kind: "value",
        ...(inner !== null && inner !== undefined
          ? { entry: decodeKvEntry(expectMap(inner, context), context) }
          : {})
      }
    case "Committed":
      return {
        kind: "committed",
        version: field.requiredU64(expectMap(inner, context), "version", context)
      }
    case "Deleted":
      return { kind: "deleted", removed: expectBoolean(inner, context) }
    case "DeletedMany":
      return { kind: "deletedMany", count: expectU32(inner, context) }
    case "Page":
      return { kind: "page", page: decodeKvPage(expectMap(inner, context), context) }
    case "Namespaces":
      return {
        kind: "namespaces",
        namespaces: expectArray(inner, context).map((item, index) =>
          decodeKvNamespaceInfo(
            expectMap(item, `${context}[${String(index)}]`),
            `${context}[${String(index)}]`
          )
        )
      }
    case "Metadata": {
      return {
        kind: "metadata",
        ...(inner !== null && inner !== undefined
          ? { metadata: decodeKvMetadata(expectMap(inner, context), context) }
          : {})
      }
    }
    case "Versioned":
      return {
        kind: "versioned",
        version: field.requiredU64(expectMap(inner, context), "version", context)
      }
    case "Leased": {
      const leasedMap = expectMap(inner, context)
      return {
        kind: "leased",
        leaseToken: field.requiredU64(leasedMap, "lease_token", context),
        grantedTtlMicros: field.requiredU64(leasedMap, "granted_ttl_micros", context),
        position: decodeMutationPosition(
          field.requiredMap(leasedMap, "position", context),
          `${context}.position`
        )
      }
    }
    case "Renewed": {
      const renewedMap = expectMap(inner, context)
      return {
        kind: "renewed",
        leaseToken: field.requiredU64(renewedMap, "lease_token", context),
        grantedTtlMicros: field.requiredU64(renewedMap, "granted_ttl_micros", context),
        position: decodeMutationPosition(
          field.requiredMap(renewedMap, "position", context),
          `${context}.position`
        )
      }
    }
    case "Released":
      return { kind: "released", wasHeld: expectBoolean(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export type KvError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "invalidKey"; readonly message: string }
  | { readonly kind: "invalidNamespace"; readonly message: string }
  | {
      readonly kind: "tooLarge"
      readonly what: string
      readonly size: number
      readonly cap: number
    }
  | { readonly kind: "backend"; readonly message: string }
  | { readonly kind: "unavailable"; readonly message: string }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | { readonly kind: "versionConflict"; readonly current?: bigint }
  | { readonly kind: "leaseLost" }
  | { readonly kind: "notFound" }
  | { readonly kind: "notLeader" }
  | { readonly kind: "stale"; readonly required: MutationPosition }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeKvError(error: KvError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "invalidKey":
      return new Map([["InvalidKey", error.message]])
    case "invalidNamespace":
      return new Map([["InvalidNamespace", error.message]])
    case "tooLarge":
      return new Map([
        [
          "TooLarge",
          new Map<string, unknown>([
            ["what", error.what],
            ["size", error.size],
            ["cap", error.cap]
          ])
        ]
      ])
    case "backend":
      return new Map([["Backend", error.message]])
    case "unavailable":
      return new Map([["Unavailable", error.message]])
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
    case "versionConflict":
      return new Map([
        ["VersionConflict", new Map<string, unknown>([["current", error.current ?? null]])]
      ])
    case "leaseLost":
      return "LeaseLost"
    case "notFound":
      return "NotFound"
    case "notLeader":
      return "NotLeader"
    case "stale":
      return new Map([
        ["Stale", new Map<string, unknown>([["required", encodeMutationPosition(error.required)]])]
      ])
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

export function decodeKvError(value: unknown, context: string): KvError {
  if (typeof value === "string") {
    if (value === "LeaseLost") return { kind: "leaseLost" }
    if (value === "NotFound") return { kind: "notFound" }
    if (value === "NotLeader") return { kind: "notLeader" }
    return { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "InvalidKey":
      return { kind: "invalidKey", message: expectString(inner, context) }
    case "InvalidNamespace":
      return { kind: "invalidNamespace", message: expectString(inner, context) }
    case "TooLarge": {
      const tooLargeMap = expectMap(inner, context)
      return {
        kind: "tooLarge",
        what: field.requiredString(tooLargeMap, "what", context),
        size: field.requiredU32(tooLargeMap, "size", context),
        cap: field.requiredU32(tooLargeMap, "cap", context)
      }
    }
    case "Backend":
      return { kind: "backend", message: expectString(inner, context) }
    case "Unavailable":
      return { kind: "unavailable", message: expectString(inner, context) }
    case "Version": {
      const versionMap = expectMap(inner, context)
      return {
        kind: "version",
        expected: field.requiredU32(versionMap, "expected", context),
        got: field.requiredU32(versionMap, "got", context)
      }
    }
    case "VersionConflict": {
      const conflictMap = expectMap(inner, context)
      const current = field.optionalU64(conflictMap, "current", context)
      return { kind: "versionConflict", ...(current !== undefined ? { current } : {}) }
    }
    case "Stale": {
      const staleMap = expectMap(inner, context)
      return {
        kind: "stale",
        required: decodeMutationPosition(
          field.requiredMap(staleMap, "required", context),
          `${context}.required`
        )
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export type KvReply =
  | { readonly kind: "ok"; readonly outcome: KvOutcome }
  | { readonly kind: "err"; readonly error: KvError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeKvReply(reply: KvReply): unknown {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeKvOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeKvError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeKvReply(value: unknown, context: string): KvReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeKvOutcome(inner, context) }
    case "Err":
      return { kind: "err", error: decodeKvError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function validateNamespace(namespace: string): void {
  if (namespace.length === 0) {
    throw new InvalidError("namespace must not be empty")
  }
  const bytes = new TextEncoder().encode(namespace)
  if (bytes.length > MAX_NAMESPACE_BYTES) {
    throw new InvalidError(
      `namespace is ${String(bytes.length)}B, exceeds cap ${String(MAX_NAMESPACE_BYTES)}B`
    )
  }
  for (const byte of bytes) {
    if (byte < 0x20 || byte === 0x7f) {
      throw new InvalidError("namespace must not contain ASCII control characters")
    }
  }
}
