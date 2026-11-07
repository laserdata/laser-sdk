import { CodecError } from "../client/errors.js"
import { type CborMap, expectMap, field, singleVariantTag } from "./cbor.js"
import {
  decodeProjectionHistory,
  encodeProjectionHistory,
  type ProjectionHistory,
  decodeGapAcceptance,
  encodeGapAcceptance,
  decodeSourceSelector,
  encodeSourceSelector,
  type GapAcceptance,
  type OversizePolicy,
  type SourceSelector,
  decodeProjection,
  decodeProjectionBinding,
  decodeSchemaDef,
  decodeSchemaSource,
  encodeProjection,
  encodeProjectionBinding,
  encodeSchemaDef,
  encodeSchemaSource,
  projectionKindCode,
  projectionKindFromCode,
  type Projection,
  type ProjectionBinding,
  type ProjectionKind,
  type SchemaDef,
  type SchemaSource
} from "./control.js"
import { decodeQueryError, encodeQueryError, type QueryError } from "./query.js"

export interface ProjectionInfo {
  readonly projection: Projection
  readonly bindings: readonly ProjectionBinding[]
  readonly status: readonly BindingStatus[]
}

export type RealizationState = "pending" | "awaiting_owner" | "realized" | "unknown"
/** Why the destination table could not be created. */
export type FailureClass =
  | "configuration"
  | "schema"
  | "poison"
  | "fatal"
  | "transient"
  | "capacity"
  | "read_only"
  | "unknown"
/** What keeps a binding from visible progress when no record holds it. */
export type StallReason =
  "source_missing" | "source_unreadable" | "destination_unreadable" | "backpressure" | "unknown"
export type HoldCause =
  | "extraction"
  | "schema"
  | "destination"
  | "oversize"
  | "missing_history"
  | "retired_incarnation"
  | "unknown"
export type MaterializationReadiness = "accepted" | "applied" | "realized" | "caught_up"
export interface PartitionProgress {
  readonly partitionId: number
  readonly headOffset?: bigint
  readonly appliedOffset?: bigint
  readonly firstOffset?: bigint
}
export interface Hold {
  readonly partitionId: number
  readonly offset: bigint
  readonly cause: HoldCause
  readonly reason: string
}
export interface BindingStatus {
  readonly history: ProjectionHistory
  readonly source: SourceSelector
  readonly index: string
  readonly realization: RealizationState
  readonly realizationReason?: string
  /** The class of the last failed attempt to create the destination table. */
  readonly failure?: FailureClass
  /** Why the binding makes no visible progress although no record holds it. */
  readonly stall?: StallReason
  readonly sourceIncarnation?: bigint
  readonly partitions: readonly PartitionProgress[]
  readonly holds: readonly Hold[]
  readonly acceptedGaps: readonly GapAcceptance[]
  readonly skippedRecords: bigint
}
export function bindingStatusReadiness(status: BindingStatus): MaterializationReadiness {
  return status.realization === "realized" ? "realized" : "applied"
}
export function bindingStatusIsBlocked(status: BindingStatus): boolean {
  return status.holds.length > 0
}
/** Whether the failure lasts until the binding or its backend changes. */
export function failureClassIsPermanent(failure: FailureClass): boolean {
  return (
    failure === "configuration" ||
    failure === "schema" ||
    failure === "poison" ||
    failure === "fatal"
  )
}
/** Whether the destination table failed in a way no retry recovers. */
export function bindingStatusIsFailed(status: BindingStatus): boolean {
  return status.failure !== undefined && failureClassIsPermanent(status.failure)
}
function sameHistory(left: ProjectionHistory, right: ProjectionHistory): boolean {
  if (left.kind === "append" || right.kind === "append") return left.kind === right.kind
  return (
    left.deleteHeader === right.deleteHeader &&
    (left.key.kind === "header" && right.key.kind === "header"
      ? left.key.name === right.key.name
      : left.key.kind === "pointer" &&
        right.key.kind === "pointer" &&
        left.key.pointer === right.key.pointer)
  )
}

export function bindingStatusCaughtUpTo(status: BindingStatus, target: BindingStatus): boolean {
  return (
    status.realization === "realized" &&
    status.holds.length === 0 &&
    status.sourceIncarnation !== undefined &&
    status.sourceIncarnation === target.sourceIncarnation &&
    status.index === target.index &&
    status.source.stream === target.source.stream &&
    status.source.topic === target.source.topic &&
    sameHistory(status.history, target.history) &&
    target.partitions.every((wanted) => {
      const headOffset = wanted.headOffset
      return (
        headOffset === undefined ||
        status.partitions.some(
          (seen) =>
            seen.partitionId === wanted.partitionId &&
            seen.appliedOffset !== undefined &&
            seen.appliedOffset >= headOffset
        )
      )
    })
  )
}
export function bindingStatusHistoryComplete(
  status: BindingStatus,
  oversize: OversizePolicy
): boolean {
  return (
    status.history.kind === "append" &&
    status.realization === "realized" &&
    status.sourceIncarnation !== undefined &&
    status.holds.length === 0 &&
    oversize !== "reference" &&
    status.acceptedGaps.length === 0 &&
    status.skippedRecords === 0n &&
    status.partitions.every(
      (partition) =>
        partition.firstOffset === 0n ||
        (partition.appliedOffset === undefined && partition.firstOffset === undefined)
    )
  )
}
export function encodeBindingStatus(status: BindingStatus): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ...(status.history.kind !== "append"
      ? [["history", encodeProjectionHistory(status.history)] as [string, unknown]]
      : []),
    ["source", encodeSourceSelector(status.source)],
    ["index", status.index],
    ["realization", status.realization]
  ])
  if (status.realizationReason !== undefined)
    map.set("realization_reason", status.realizationReason)
  if (status.failure !== undefined) map.set("failure", status.failure)
  if (status.stall !== undefined) map.set("stall", status.stall)
  if (status.sourceIncarnation !== undefined)
    map.set("source_incarnation", status.sourceIncarnation)
  if (status.partitions.length > 0)
    map.set(
      "partitions",
      status.partitions.map((partition) => {
        const value = new Map<string, unknown>([["partition_id", BigInt(partition.partitionId)]])
        if (partition.headOffset !== undefined) value.set("head_offset", partition.headOffset)
        if (partition.appliedOffset !== undefined)
          value.set("applied_offset", partition.appliedOffset)
        if (partition.firstOffset !== undefined) value.set("first_offset", partition.firstOffset)
        return value
      })
    )
  if (status.holds.length > 0)
    map.set(
      "holds",
      status.holds.map(
        (hold) =>
          new Map<string, unknown>([
            ["partition_id", BigInt(hold.partitionId)],
            ["offset", hold.offset],
            ["cause", hold.cause],
            ["reason", hold.reason]
          ])
      )
    )
  if (status.acceptedGaps.length > 0)
    map.set("accepted_gaps", status.acceptedGaps.map(encodeGapAcceptance))
  map.set("skipped_records", status.skippedRecords)
  return map
}
export function decodeBindingStatus(map: CborMap, context: string): BindingStatus {
  const realization = field.requiredString(map, "realization", context)
  const reason = field.optionalString(map, "realization_reason", context)
  const failure = field.optionalString(map, "failure", context)
  const stall = field.optionalString(map, "stall", context)
  const incarnation = field.optionalU64(map, "source_incarnation", context)
  return {
    history: map.has("history")
      ? decodeProjectionHistory(field.requiredMap(map, "history", context), context)
      : { kind: "append" },
    source: decodeSourceSelector(field.requiredMap(map, "source", context), context),
    index: field.requiredString(map, "index", context),
    realization:
      realization === "pending" || realization === "awaiting_owner" || realization === "realized"
        ? realization
        : "unknown",
    ...(reason !== undefined ? { realizationReason: reason } : {}),
    ...(failure !== undefined ? { failure: decodeFailureClass(failure) } : {}),
    ...(stall !== undefined ? { stall: decodeStallReason(stall) } : {}),
    ...(incarnation !== undefined ? { sourceIncarnation: incarnation } : {}),
    partitions: field.optionalArray(map, "partitions", context, (item) => {
      const value = expectMap(item, context)
      const headOffset = field.optionalU64(value, "head_offset", context)
      const appliedOffset = field.optionalU64(value, "applied_offset", context)
      const firstOffset = field.optionalU64(value, "first_offset", context)
      return {
        partitionId: field.requiredU32(value, "partition_id", context),
        ...(headOffset !== undefined ? { headOffset } : {}),
        ...(appliedOffset !== undefined ? { appliedOffset } : {}),
        ...(firstOffset !== undefined ? { firstOffset } : {})
      }
    }),
    holds: field.optionalArray(map, "holds", context, (item) => {
      const value = expectMap(item, context)
      const cause = field.requiredString(value, "cause", context)
      return {
        partitionId: field.requiredU32(value, "partition_id", context),
        offset: field.requiredU64(value, "offset", context),
        cause:
          cause === "extraction" ||
          cause === "schema" ||
          cause === "destination" ||
          cause === "oversize" ||
          cause === "missing_history" ||
          cause === "retired_incarnation"
            ? cause
            : "unknown",
        reason: field.requiredString(value, "reason", context)
      }
    }),
    acceptedGaps: field.optionalArray(map, "accepted_gaps", context, (item) =>
      decodeGapAcceptance(expectMap(item, context), context)
    ),
    skippedRecords: field.optionalU64(map, "skipped_records", context) ?? 0n
  }
}

function decodeFailureClass(value: string): FailureClass {
  switch (value) {
    case "configuration":
    case "schema":
    case "poison":
    case "fatal":
    case "transient":
    case "capacity":
    case "read_only":
      return value
    default:
      return "unknown"
  }
}

function decodeStallReason(value: string): StallReason {
  switch (value) {
    case "source_missing":
    case "source_unreadable":
    case "destination_unreadable":
    case "backpressure":
      return value
    default:
      return "unknown"
  }
}

export interface SchemaInfo {
  readonly schema: SchemaDef
  readonly dropped: boolean
}

export interface GetProjection {
  readonly v: number
  readonly id: string
  readonly status?: boolean
}

export interface ListProjections {
  readonly v: number
  readonly topics: readonly string[]
  readonly nameContains?: string
  readonly idPrefix?: string
  readonly search?: string
  /** Keeps projections of the named kinds. */
  readonly kinds?: readonly ProjectionKind[]
  /** The stream whose projections are listed, required in stream tenancy. */
  readonly stream?: string
  /** The 1-based page. Zero or absent reads the first page. */
  readonly page?: number
  /** The projections per page. Zero or absent leaves it to the server. */
  readonly results?: number
}

/** One page of a projection listing. `page` counts from 1. */
export interface ProjectionPage {
  readonly items: readonly ProjectionInfo[]
  readonly page: number
  readonly totalPages: number
  readonly totalResults: number
}

export interface GetSchema {
  readonly v: number
  readonly id: number
  /** The stream whose schema registry the request addresses, the
   * deployment-wide registry when absent. */
  readonly stream?: string
}

export interface ListSchemas {
  readonly v: number
  readonly nameContains?: string
  /** The stream whose schema registry the request addresses, the
   * deployment-wide registry when absent. */
  readonly stream?: string
  /** The 1-based page. Zero or absent reads the first page. */
  readonly page?: number
  /** The schemas per page. Zero or absent leaves it to the server. */
  readonly results?: number
}

/** One page of a writer-schema listing, ordered by id. `page` counts from 1. */
export interface SchemaPage {
  readonly items: readonly SchemaInfo[]
  readonly page: number
  readonly totalPages: number
  readonly totalResults: number
}

export interface RegisterSchema {
  readonly v: number
  readonly source: SchemaSource
  readonly name?: string
  readonly version?: number
  /** The stream whose schema registry the request addresses, the
   * deployment-wide registry when absent. */
  readonly stream?: string
}

export interface DecodeRecord {
  readonly v: number
  readonly id: number
  readonly payload: Uint8Array
  /** The stream the record was read from: `id` resolves in its registry,
   * then in the deployment-wide one. The deployment-wide registry alone
   * when absent. */
  readonly stream?: string
}

export type BrowseOutcome =
  | { readonly kind: "projections"; readonly page: ProjectionPage }
  | { readonly kind: "projection"; readonly projection?: ProjectionInfo }
  | { readonly kind: "schemas"; readonly page: SchemaPage }
  | { readonly kind: "schema"; readonly schema?: SchemaInfo }
  | { readonly kind: "schemaRegistered"; readonly id: number }
  | { readonly kind: "decoded"; readonly value?: unknown }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type BrowseReply =
  | { readonly kind: "ok"; readonly outcome: BrowseOutcome }
  | { readonly kind: "err"; readonly error: QueryError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

function encodeVersioned(
  v: number,
  entries: readonly (readonly [string, unknown])[]
): Map<string, unknown> {
  return new Map<string, unknown>([["v", BigInt(v)], ...entries])
}

export function encodeGetProjection(request: GetProjection): Map<string, unknown> {
  const map = encodeVersioned(request.v, [["id", request.id]])
  if (request.status) map.set("status", true)
  return map
}

export function decodeGetProjection(map: CborMap, context: string): GetProjection {
  return {
    v: field.requiredU32(map, "v", context),
    id: field.requiredString(map, "id", context),
    status: field.optionalBoolean(map, "status", context) ?? false
  }
}

export function encodeListProjections(request: ListProjections): Map<string, unknown> {
  const map = encodeVersioned(request.v, [])
  if (request.topics.length > 0) map.set("topics", [...request.topics])
  if (request.nameContains !== undefined) map.set("name_contains", request.nameContains)
  if (request.idPrefix !== undefined) map.set("id_prefix", request.idPrefix)
  if (request.search !== undefined) map.set("search", request.search)
  if (request.kinds !== undefined && request.kinds.length > 0)
    map.set(
      "kinds",
      request.kinds.map((kind) => BigInt(projectionKindCode(kind)))
    )
  if (request.stream !== undefined) map.set("stream", request.stream)
  if (request.page !== undefined && request.page > 0) map.set("page", BigInt(request.page))
  if (request.results !== undefined && request.results > 0)
    map.set("results", BigInt(request.results))
  return map
}

export function decodeListProjections(map: CborMap, context: string): ListProjections {
  const nameContains = field.optionalString(map, "name_contains", context)
  const idPrefix = field.optionalString(map, "id_prefix", context)
  const search = field.optionalString(map, "search", context)
  const stream = field.optionalString(map, "stream", context)
  const kinds = field.optionalArray(map, "kinds", context, (item, index) =>
    projectionKindFromCode(decodeU8(item, `${context}.kinds[${String(index)}]`))
  )
  const page = field.optionalU32(map, "page", context) ?? 0
  const results = field.optionalU32(map, "results", context) ?? 0
  return {
    v: field.requiredU32(map, "v", context),
    ...(stream !== undefined ? { stream } : {}),
    topics: field.optionalArray(map, "topics", context, (item, index) => {
      if (typeof item !== "string") {
        throw new CodecError(
          `${context}.topics[${String(index)}] must be a string`,
          context,
          "topics"
        )
      }
      return item
    }),
    ...(nameContains !== undefined ? { nameContains } : {}),
    ...(idPrefix !== undefined ? { idPrefix } : {}),
    ...(search !== undefined ? { search } : {}),
    ...(kinds.length > 0 ? { kinds } : {}),
    ...(page > 0 ? { page } : {}),
    ...(results > 0 ? { results } : {})
  }
}

export function encodeProjectionPage(page: ProjectionPage): Map<string, unknown> {
  return new Map<string, unknown>([
    ["items", page.items.map((info) => encodeProjectionInfo(info))],
    ["page", BigInt(page.page)],
    ["total_pages", BigInt(page.totalPages)],
    ["total_results", BigInt(page.totalResults)]
  ])
}

export function decodeProjectionPage(map: CborMap, context: string): ProjectionPage {
  return {
    items: field.optionalArray(map, "items", context, (item, index) =>
      decodeProjectionInfo(
        expectMap(item, `${context}.items[${String(index)}]`),
        `${context}.items[${String(index)}]`
      )
    ),
    page: field.requiredU32(map, "page", context),
    totalPages: field.requiredU32(map, "total_pages", context),
    totalResults: field.requiredU32(map, "total_results", context)
  }
}

export function encodeGetSchema(request: GetSchema): Map<string, unknown> {
  return withStream(encodeVersioned(request.v, [["id", BigInt(request.id)]]), request.stream)
}

export function decodeGetSchema(map: CborMap, context: string): GetSchema {
  return {
    v: field.requiredU32(map, "v", context),
    id: field.requiredU32(map, "id", context),
    ...streamOf(map, context)
  }
}

export function encodeListSchemas(request: ListSchemas): Map<string, unknown> {
  const map = encodeVersioned(request.v, [])
  if (request.nameContains !== undefined) map.set("name_contains", request.nameContains)
  withStream(map, request.stream)
  if (request.page !== undefined && request.page > 0) map.set("page", BigInt(request.page))
  if (request.results !== undefined && request.results > 0)
    map.set("results", BigInt(request.results))
  return map
}

export function decodeListSchemas(map: CborMap, context: string): ListSchemas {
  const nameContains = field.optionalString(map, "name_contains", context)
  const page = field.optionalU32(map, "page", context) ?? 0
  const results = field.optionalU32(map, "results", context) ?? 0
  return {
    v: field.requiredU32(map, "v", context),
    ...(nameContains !== undefined ? { nameContains } : {}),
    ...streamOf(map, context),
    ...(page > 0 ? { page } : {}),
    ...(results > 0 ? { results } : {})
  }
}

export function encodeSchemaPage(page: SchemaPage): Map<string, unknown> {
  return new Map<string, unknown>([
    ["items", page.items.map((info) => encodeSchemaInfo(info))],
    ["page", BigInt(page.page)],
    ["total_pages", BigInt(page.totalPages)],
    ["total_results", BigInt(page.totalResults)]
  ])
}

export function decodeSchemaPage(map: CborMap, context: string): SchemaPage {
  return {
    items: decodeArray(map.get("items") ?? [], `${context}.items`, decodeSchemaInfo),
    page: field.requiredU32(map, "page", context),
    totalPages: field.requiredU32(map, "total_pages", context),
    totalResults: field.requiredU32(map, "total_results", context)
  }
}

export function encodeRegisterSchema(request: RegisterSchema): Map<string, unknown> {
  const map = encodeVersioned(request.v, [["source", encodeSchemaSource(request.source)]])
  if (request.name !== undefined) map.set("name", request.name)
  if (request.version !== undefined) map.set("version", BigInt(request.version))
  return withStream(map, request.stream)
}

export function decodeRegisterSchema(map: CborMap, context: string): RegisterSchema {
  const name = field.optionalString(map, "name", context)
  const version = field.optionalU32(map, "version", context)
  return {
    v: field.requiredU32(map, "v", context),
    source: decodeSchemaSource(field.requiredMap(map, "source", context), `${context}.source`),
    ...(name !== undefined ? { name } : {}),
    ...(version !== undefined ? { version } : {}),
    ...streamOf(map, context)
  }
}

export function encodeDecodeRecord(request: DecodeRecord): Map<string, unknown> {
  return withStream(
    encodeVersioned(request.v, [
      ["id", BigInt(request.id)],
      ["payload", request.payload]
    ]),
    request.stream
  )
}

export function decodeDecodeRecord(map: CborMap, context: string): DecodeRecord {
  return {
    v: field.requiredU32(map, "v", context),
    id: field.requiredU32(map, "id", context),
    payload: field.requiredBytes(map, "payload", context),
    ...streamOf(map, context)
  }
}

function withStream(map: Map<string, unknown>, stream: string | undefined): Map<string, unknown> {
  if (stream !== undefined) map.set("stream", stream)
  return map
}

function streamOf(map: CborMap, context: string): { readonly stream?: string } {
  const stream = field.optionalString(map, "stream", context)
  return stream === undefined ? {} : { stream }
}

export function encodeProjectionInfo(info: ProjectionInfo): Map<string, unknown> {
  const map = new Map<string, unknown>([["projection", encodeProjection(info.projection)]])
  if (info.bindings.length > 0)
    map.set(
      "bindings",
      info.bindings.map((binding) => encodeProjectionBinding(binding))
    )
  if (info.status.length > 0) map.set("status", info.status.map(encodeBindingStatus))
  return map
}

export function decodeProjectionInfo(map: CborMap, context: string): ProjectionInfo {
  return {
    status: field.optionalArray(map, "status", context, (item) =>
      decodeBindingStatus(expectMap(item, context), context)
    ),
    projection: decodeProjection(
      field.requiredMap(map, "projection", context),
      `${context}.projection`
    ),
    bindings: field.optionalArray(map, "bindings", context, (item, index) =>
      decodeProjectionBinding(
        expectMap(item, `${context}.bindings[${String(index)}]`),
        `${context}.bindings[${String(index)}]`
      )
    )
  }
}

export function encodeSchemaInfo(info: SchemaInfo): Map<string, unknown> {
  return new Map<string, unknown>([
    ["schema", encodeSchemaDef(info.schema)],
    ["dropped", info.dropped]
  ])
}

export function decodeSchemaInfo(map: CborMap, context: string): SchemaInfo {
  return {
    schema: decodeSchemaDef(field.requiredMap(map, "schema", context), `${context}.schema`),
    dropped: field.optionalBoolean(map, "dropped", context) ?? false
  }
}

export function encodeBrowseOutcome(outcome: BrowseOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "projections":
      return new Map([["Projections", encodeProjectionPage(outcome.page)]])
    case "projection":
      return new Map([
        [
          "Projection",
          outcome.projection === undefined ? null : encodeProjectionInfo(outcome.projection)
        ]
      ])
    case "schemas":
      return new Map([["Schemas", encodeSchemaPage(outcome.page)]])
    case "schema":
      return new Map([
        ["Schema", outcome.schema === undefined ? null : encodeSchemaInfo(outcome.schema)]
      ])
    case "schemaRegistered":
      return new Map([["SchemaRegistered", BigInt(outcome.id)]])
    case "decoded":
      return new Map([["Decoded", outcome.value ?? null]])
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

export function decodeBrowseOutcome(value: unknown, context: string): BrowseOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Projections":
      return { kind: "projections", page: decodeProjectionPage(expectMap(inner, context), context) }
    case "Projection":
      return inner === null
        ? { kind: "projection" }
        : {
            kind: "projection",
            projection: decodeProjectionInfo(expectMap(inner, context), context)
          }
    case "Schemas":
      return { kind: "schemas", page: decodeSchemaPage(expectMap(inner, context), context) }
    case "Schema":
      return inner === null
        ? { kind: "schema" }
        : { kind: "schema", schema: decodeSchemaInfo(expectMap(inner, context), context) }
    case "SchemaRegistered":
      return { kind: "schemaRegistered", id: decodeU32(inner, context) }
    case "Decoded":
      return inner === null ? { kind: "decoded" } : { kind: "decoded", value: inner }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeBrowseReply(reply: BrowseReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeBrowseOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeQueryError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeBrowseReply(value: unknown, context: string): BrowseReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeBrowseOutcome(inner, context) }
    case "Err":
      return { kind: "err", error: decodeQueryError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

function decodeArray<T>(
  value: unknown,
  context: string,
  decode: (map: CborMap, context: string) => T
): T[] {
  if (!Array.isArray(value)) throw new CodecError(`${context} must be an array`, context, "value")
  return value.map((item, index) =>
    decode(expectMap(item, `${context}[${String(index)}]`), `${context}[${String(index)}]`)
  )
}

function decodeU32(value: unknown, context: string): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff)
    return value
  throw new CodecError(`${context} must fit u32`, context, "value")
}

function decodeU8(value: unknown, context: string): number {
  const decoded = typeof value === "bigint" && value <= 0xffn ? Number(value) : value
  if (typeof decoded === "number" && Number.isInteger(decoded) && decoded >= 0 && decoded <= 0xff)
    return decoded
  throw new CodecError(`${context} must fit u8`, context, "value")
}
