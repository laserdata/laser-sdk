import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"
import { FORK_OP_VERSION } from "./codes.js"
import { MAX_FORK_ID_BYTES } from "./limits.js"

// How a fork relates to the trunk it branched from. Rust carries no serde
// catch-all for this enum, so an unrecognized word is a decode failure,
// not a value.
export type ForkKind = "severed" | "continuous"

function parseForkKind(word: string, context: string): ForkKind {
  if (word !== "severed" && word !== "continuous") {
    throw new CodecError(`\`${word}\` is not a recognized fork kind`, context, "kind")
  }
  return word
}

// Lifecycle of a fork. Same no-catch-all rule as `ForkKind`.
export type ForkStatus = "open" | "promoted" | "squashed"

function parseForkStatus(word: string, context: string): ForkStatus {
  if (word !== "open" && word !== "promoted" && word !== "squashed") {
    throw new CodecError(`\`${word}\` is not a recognized fork status`, context, "status")
  }
  return word
}

// A fork's metadata, returned by `create` and `list`.
export interface ForkInfo {
  readonly forkId: string
  readonly parent?: string
  readonly kind: ForkKind
  readonly userId: number
  readonly status: ForkStatus
  readonly createdAtMicros: bigint
  readonly rowCount: number
}

export function encodeForkInfo(info: ForkInfo): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("fork_id", info.forkId)
  if (info.parent !== undefined) map.set("parent", info.parent)
  map.set("kind", info.kind)
  map.set("user_id", info.userId)
  map.set("status", info.status)
  map.set("created_at_micros", info.createdAtMicros)
  map.set("row_count", info.rowCount)
  return map
}

export function decodeForkInfo(map: CborMap, context: string): ForkInfo {
  const parent = field.optionalString(map, "parent", context)
  return {
    forkId: field.requiredString(map, "fork_id", context),
    ...(parent !== undefined ? { parent } : {}),
    kind: parseForkKind(field.requiredString(map, "kind", context), context),
    userId: field.requiredU32(map, "user_id", context),
    status: parseForkStatus(field.requiredString(map, "status", context), context),
    createdAtMicros: field.requiredU64(map, "created_at_micros", context),
    rowCount: field.requiredU32(map, "row_count", context)
  }
}

// Wire form of the `AGDX_FORK_CREATE` request.
export interface ForkCreate {
  readonly forkId: string
  readonly parent?: string
  readonly kind: ForkKind
  readonly tables: readonly string[]
}

export function encodeForkCreate(create: ForkCreate): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", FORK_OP_VERSION)
  map.set("fork_id", create.forkId)
  if (create.parent !== undefined) map.set("parent", create.parent)
  map.set("kind", create.kind)
  if (create.tables.length > 0) map.set("tables", [...create.tables])
  return map
}

export function decodeForkCreate(map: CborMap, context: string): ForkCreate {
  const parent = field.optionalString(map, "parent", context)
  const kind = map.has("kind")
    ? parseForkKind(field.requiredString(map, "kind", context), context)
    : "continuous"
  return {
    forkId: field.requiredString(map, "fork_id", context),
    ...(parent !== undefined ? { parent } : {}),
    kind,
    tables: field.optionalArray(map, "tables", context, (item, index) =>
      expectString(item, `${context}.tables[${String(index)}]`)
    )
  }
}

// Wire form of the `AGDX_FORK_DELETE` (squash) request.
export interface ForkDelete {
  readonly forkId: string
}

export function encodeForkDelete(del: ForkDelete): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", FORK_OP_VERSION],
    ["fork_id", del.forkId]
  ])
}

export function decodeForkDelete(map: CborMap, context: string): ForkDelete {
  return { forkId: field.requiredString(map, "fork_id", context) }
}

// Wire form of the `AGDX_FORK_PROMOTE` request.
export interface ForkPromote {
  readonly forkId: string
}

export function encodeForkPromote(promote: ForkPromote): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", FORK_OP_VERSION],
    ["fork_id", promote.forkId]
  ])
}

export function decodeForkPromote(map: CborMap, context: string): ForkPromote {
  return { forkId: field.requiredString(map, "fork_id", context) }
}

// Wire form of the `AGDX_FORK_LIST` request. Carries no data beyond the op
// version, so there is no matching interface, just the encoder.
export function encodeForkList(): Map<string, unknown> {
  return new Map<string, unknown>([["v", FORK_OP_VERSION]])
}

// Wire form of the `AGDX_FORK_PUT` request. `fields`/`metadata` are plain
// string maps; `embedding` is an opaque string blob (Rust types it as
// `String`, not a numeric vector), not a codec choice this port makes.
export interface ForkPut {
  readonly forkId: string
  readonly table: string
  readonly partitionId: number
  readonly offset: bigint
  readonly projectionId: string
  readonly projectionVersion: number
  readonly fields: ReadonlyMap<string, string>
  readonly metadata: ReadonlyMap<string, string>
  readonly payload?: Uint8Array
  readonly embedding?: string
  readonly tombstone: boolean
}

function encodeStringMap(entries: ReadonlyMap<string, string>): Map<string, unknown> {
  return new Map(entries)
}

function decodeStringMap(map: CborMap, context: string): ReadonlyMap<string, string> {
  const result = new Map<string, string>()
  for (const [key, value] of map) {
    if (typeof key !== "string" || typeof value !== "string") {
      throw new CodecError(`${context} must map strings to strings`, context, "value")
    }
    result.set(key, value)
  }
  return result
}

export function encodeForkPut(put: ForkPut): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", FORK_OP_VERSION)
  map.set("fork_id", put.forkId)
  map.set("table", put.table)
  map.set("partition_id", put.partitionId)
  map.set("offset", put.offset)
  map.set("projection_id", put.projectionId)
  map.set("projection_version", put.projectionVersion)
  if (put.fields.size > 0) map.set("fields", encodeStringMap(put.fields))
  if (put.metadata.size > 0) map.set("metadata", encodeStringMap(put.metadata))
  if (put.payload !== undefined) map.set("payload", put.payload)
  if (put.embedding !== undefined) map.set("embedding", put.embedding)
  map.set("tombstone", put.tombstone)
  return map
}

export function decodeForkPut(map: CborMap, context: string): ForkPut {
  const fieldsMap = field.optionalMap(map, "fields", context)
  const metadataMap = field.optionalMap(map, "metadata", context)
  const payload = field.optionalBytes(map, "payload", context)
  const embedding = field.optionalString(map, "embedding", context)
  return {
    forkId: field.requiredString(map, "fork_id", context),
    table: field.requiredString(map, "table", context),
    partitionId: field.requiredU32(map, "partition_id", context),
    offset: field.requiredU64(map, "offset", context),
    projectionId: field.optionalString(map, "projection_id", context) ?? "",
    projectionVersion: field.optionalU32(map, "projection_version", context) ?? 0,
    fields: fieldsMap !== undefined ? decodeStringMap(fieldsMap, `${context}.fields`) : new Map(),
    metadata:
      metadataMap !== undefined ? decodeStringMap(metadataMap, `${context}.metadata`) : new Map(),
    ...(payload !== undefined ? { payload } : {}),
    ...(embedding !== undefined ? { embedding } : {}),
    tombstone: map.get("tombstone") === true
  }
}

// The successful outcome of a fork command, shaped per op. Additive: an
// unrecognized variant decodes rather than throws.
export type ForkOutcome =
  | { readonly kind: "created"; readonly info: ForkInfo }
  | { readonly kind: "deleted"; readonly removed: boolean }
  | { readonly kind: "promoted"; readonly rows: number }
  | { readonly kind: "list"; readonly forks: readonly ForkInfo[] }
  | { readonly kind: "written" }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeForkOutcome(outcome: ForkOutcome): unknown {
  switch (outcome.kind) {
    case "created":
      return new Map([["Created", encodeForkInfo(outcome.info)]])
    case "deleted":
      return new Map([["Deleted", outcome.removed]])
    case "promoted":
      return new Map([["Promoted", new Map<string, unknown>([["rows", outcome.rows]])]])
    case "list":
      return new Map([["List", outcome.forks.map((info) => encodeForkInfo(info))]])
    case "written":
      return "Written"
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

export function decodeForkOutcome(value: unknown, context: string): ForkOutcome {
  if (typeof value === "string") {
    if (value === "Written") return { kind: "written" }
    return { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Created":
      return { kind: "created", info: decodeForkInfo(expectMap(inner, context), context) }
    case "Deleted":
      return { kind: "deleted", removed: expectBoolean(inner, context) }
    case "Promoted":
      return {
        kind: "promoted",
        rows: field.requiredU32(expectMap(inner, context), "rows", context)
      }
    case "List": {
      if (!Array.isArray(inner)) {
        throw new CodecError(`expected an array in ${context}`, context, "value")
      }
      return {
        kind: "list",
        forks: inner.map((item, index) =>
          decodeForkInfo(
            expectMap(item, `${context}[${String(index)}]`),
            `${context}[${String(index)}]`
          )
        )
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

function expectBoolean(value: unknown, context: string): boolean {
  if (typeof value !== "boolean") {
    throw new CodecError(`expected a boolean in ${context}`, context, "value")
  }
  return value
}

// Why a fork operation failed. Additive: an unrecognized variant decodes
// rather than throws.
export type ForkError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "notFound"; readonly message: string }
  | { readonly kind: "invalidFork"; readonly message: string }
  | { readonly kind: "conflict"; readonly message: string }
  | { readonly kind: "backend"; readonly message: string }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | { readonly kind: "notLeader" }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeForkError(error: ForkError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "notFound":
      return new Map([["NotFound", error.message]])
    case "invalidFork":
      return new Map([["InvalidFork", error.message]])
    case "conflict":
      return new Map([["Conflict", error.message]])
    case "backend":
      return new Map([["Backend", error.message]])
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
    case "notLeader":
      return "NotLeader"
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

export function decodeForkError(value: unknown, context: string): ForkError {
  if (typeof value === "string") {
    if (value === "NotLeader") return { kind: "notLeader" }
    return { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "NotFound":
      return { kind: "notFound", message: expectString(inner, context) }
    case "InvalidFork":
      return { kind: "invalidFork", message: expectString(inner, context) }
    case "Conflict":
      return { kind: "conflict", message: expectString(inner, context) }
    case "Backend":
      return { kind: "backend", message: expectString(inner, context) }
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

// The result of a fork op: `ok` with the outcome, or `err` with a failure.
export type ForkReply =
  | { readonly kind: "ok"; readonly outcome: ForkOutcome }
  | { readonly kind: "err"; readonly error: ForkError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeForkReply(reply: ForkReply): unknown {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeForkOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeForkError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeForkReply(value: unknown, context: string): ForkReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeForkOutcome(inner, context) }
    case "Err":
      return { kind: "err", error: decodeForkError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

// The canonical fork-id safelist, shared by every fork-serving backend. A
// fork id is caller-chosen and inlined into a copy-on-write query as a
// quoted identifier by some backends, so the charset must be a strict
// safelist, not just a length bound. A valid id is non-empty, at most
// `MAX_FORK_ID_BYTES` bytes, and made only of ASCII letters, digits, `-`,
// `_`, and `.`.
export function validateForkId(forkId: string): void {
  if (forkId.length === 0) {
    throw new InvalidError("fork id must not be empty")
  }
  const bytes = new TextEncoder().encode(forkId)
  if (bytes.length > MAX_FORK_ID_BYTES) {
    throw new InvalidError(
      `fork id is ${String(bytes.length)}B, exceeds cap ${String(MAX_FORK_ID_BYTES)}B`
    )
  }
  if (!/^[A-Za-z0-9._-]+$/.test(forkId)) {
    throw new InvalidError(
      "fork id has a disallowed byte: allowed are ASCII letters, digits, '-', '_', '.'"
    )
  }
}
