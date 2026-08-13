import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  decodeOne,
  encodeNamed,
  expectMap,
  expectString,
  field,
  singleVariantTag
} from "./cbor.js"
import { BackendResourceId, DestinationId, QueryExecutionId } from "./ids.js"
import {
  type LogicalField,
  type TypedValue,
  decodeLogicalField,
  decodeTypedValue,
  encodeLogicalField,
  encodeTypedValue,
  validateResultFields,
  validateTypedValueAgainst,
  validateTypedValue
} from "./schema.js"
import {
  MAX_PAGE_SIZE,
  MAX_QUERY_CURSOR_BYTES,
  MAX_QUERY_FIELDS,
  MAX_QUERY_NAME_BYTES,
  MAX_QUERY_PARAMETERS,
  MAX_QUERY_PREDICATES,
  MAX_RAW_SQL_BYTES,
  MAX_TEXT_QUERY_BYTES,
  MAX_VECTOR_DIMENSIONS
} from "./limits.js"
import { QUERY_OP_VERSION } from "./codes.js"

export type Consistency = "eventual" | "read_your_writes" | "strong"
export type SqlDialect = "data_fusion" | "postgres" | "my_sql" | "sqlite"
export type QueryTarget =
  | { readonly kind: "operational"; readonly index: string }
  | {
      readonly kind: "lakehouse"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly snapshot?: SnapshotSelector
    }
export type SnapshotSelector =
  | { readonly kind: "snapshot_id"; readonly value: bigint }
  | { readonly kind: "timestamp_micros"; readonly value: bigint }

export interface KeyMatch {
  readonly field: string
  readonly value: TypedValue
}
export type CmpOp = "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "contains" | "prefix"
export interface Predicate {
  readonly field: string
  readonly op: CmpOp
  readonly value: TypedValue
}
export type Filter =
  | { readonly kind: "all"; readonly filters: readonly Filter[] }
  | { readonly kind: "any"; readonly filters: readonly Filter[] }
  | { readonly kind: "not"; readonly filter: Filter }
  | { readonly kind: "pred"; readonly predicate: Predicate }
export type Dir = "asc" | "desc"
export interface Sort {
  readonly field: string
  readonly dir: Dir
}
export interface TextQuery {
  readonly field?: string
  readonly query: string
}
export interface VectorQuery {
  readonly field: string
  readonly embedding: readonly number[]
  readonly topK: number
}
export type AggFunc =
  "count" | "count_distinct" | "sum" | "avg" | "min" | "max" | "percentile" | "std_dev"
export interface AggCall {
  readonly func: AggFunc
  readonly field?: string
  readonly arg?: number
  readonly alias: string
}
export interface Window {
  readonly field: string
  readonly everyMicros: bigint
}
export interface Aggregate {
  readonly groupBy: readonly string[]
  readonly funcs: readonly AggCall[]
  readonly window?: Window
}
export interface Select {
  readonly fields: readonly string[]
  readonly payload: boolean
}
export interface RawSql {
  readonly dialect: SqlDialect
  readonly sql: string
  readonly params: readonly TypedValue[]
}
export interface QueryPageRequest {
  readonly limit: number
  readonly offset?: bigint
  readonly cursor?: string
  readonly wantTotal: boolean
}

export interface Query {
  readonly executionId: QueryExecutionId
  readonly target: QueryTarget
  readonly deadlineMicros: bigint
  readonly byKey: readonly KeyMatch[]
  readonly messageType?: string
  readonly timeRange?: readonly [bigint, bigint]
  readonly filter?: Filter
  readonly vector?: VectorQuery
  readonly text?: TextQuery
  readonly order: readonly Sort[]
  readonly page: QueryPageRequest
  readonly aggregate?: Aggregate
  readonly having?: Filter
  readonly distinct: boolean
  readonly select: Select
  readonly fork?: string
  readonly rawSql?: RawSql
  readonly consistency: Consistency
}

export interface QueryEnvelope {
  readonly v: number
  readonly query: Query
}
export interface QueryPageEnvelope {
  readonly v: number
  readonly executionId: QueryExecutionId
  readonly cursor: string
  readonly deadlineMicros: bigint
}
export interface QueryCancelEnvelope {
  readonly v: number
  readonly executionId: QueryExecutionId
}
export interface QueryStatusEnvelope {
  readonly v: number
  readonly executionId: QueryExecutionId
}

export function encodeQueryPageEnvelope(value: QueryPageEnvelope): Map<string, unknown> {
  return mapOf([
    ["v", value.v],
    ["execution_id", value.executionId.toBytes()],
    ["cursor", value.cursor],
    ["deadline_micros", value.deadlineMicros]
  ])
}
export function encodeQueryCancelEnvelope(value: QueryCancelEnvelope): Map<string, unknown> {
  return mapOf([
    ["v", value.v],
    ["execution_id", value.executionId.toBytes()]
  ])
}
export function encodeQueryStatusEnvelope(value: QueryStatusEnvelope): Map<string, unknown> {
  return mapOf([
    ["v", value.v],
    ["execution_id", value.executionId.toBytes()]
  ])
}
export function decodeQueryPageEnvelope(map: CborMap, context: string): QueryPageEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context)),
    cursor: field.requiredString(map, "cursor", context),
    deadlineMicros: field.requiredU64(map, "deadline_micros", context)
  }
}
export function decodeQueryCancelEnvelope(map: CborMap, context: string): QueryCancelEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context))
  }
}
export function decodeQueryStatusEnvelope(map: CborMap, context: string): QueryStatusEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context))
  }
}
export function encodeQueryPageEnvelopeFrame(value: QueryPageEnvelope): Uint8Array {
  return encodeNamed(encodeQueryPageEnvelope(value))
}
export function encodeQueryCancelEnvelopeFrame(value: QueryCancelEnvelope): Uint8Array {
  return encodeNamed(encodeQueryCancelEnvelope(value))
}
export function encodeQueryStatusEnvelopeFrame(value: QueryStatusEnvelope): Uint8Array {
  return encodeNamed(encodeQueryStatusEnvelope(value))
}
export function decodeQueryPageEnvelopeFrame(bytes: Uint8Array): QueryPageEnvelope {
  const context = "QueryPageEnvelope"
  return decodeQueryPageEnvelope(expectMap(decodeOne(bytes, context), context), context)
}
export function decodeQueryCancelEnvelopeFrame(bytes: Uint8Array): QueryCancelEnvelope {
  const context = "QueryCancelEnvelope"
  return decodeQueryCancelEnvelope(expectMap(decodeOne(bytes, context), context), context)
}
export function decodeQueryStatusEnvelopeFrame(bytes: Uint8Array): QueryStatusEnvelope {
  const context = "QueryStatusEnvelope"
  return decodeQueryStatusEnvelope(expectMap(decodeOne(bytes, context), context), context)
}

export function operationalTarget(index: string): QueryTarget {
  return { kind: "operational", index }
}
export function lakehouseTarget(
  destinationId: DestinationId,
  destinationGeneration: bigint,
  snapshot?: SnapshotSelector
): QueryTarget {
  return {
    kind: "lakehouse",
    destinationId,
    destinationGeneration,
    ...(snapshot === undefined ? {} : { snapshot })
  }
}
export function filterAll(filters: readonly Filter[]): Filter {
  return { kind: "all", filters }
}
export function filterAny(filters: readonly Filter[]): Filter {
  return { kind: "any", filters }
}
export function filterNegate(filter: Filter): Filter {
  return { kind: "not", filter }
}
export function filterPred(fieldName: string, op: CmpOp, value: TypedValue): Filter {
  return { kind: "pred", predicate: { field: fieldName, op, value } }
}

export function newQuery(
  target: QueryTarget,
  executionId: QueryExecutionId,
  deadlineMicros: bigint
): Query {
  return {
    executionId,
    target,
    deadlineMicros,
    byKey: [],
    order: [],
    page: { limit: 50, offset: 0n, wantTotal: false },
    distinct: false,
    select: { fields: [], payload: false },
    consistency: "eventual"
  }
}

function optional<T>(
  map: Map<string, unknown>,
  name: string,
  value: T | undefined,
  encode: (value: T) => unknown = (item) => item
): void {
  if (value !== undefined) map.set(name, encode(value))
}
function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}
function idBytes(id: { toBytes(): Uint8Array }): Uint8Array {
  return id.toBytes()
}

export function encodeQueryTarget(target: QueryTarget): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", target.kind]])
  if (target.kind === "operational") map.set("index", target.index)
  else {
    map.set("destination_id", idBytes(target.destinationId))
    map.set("destination_generation", target.destinationGeneration)
    optional(map, "snapshot", target.snapshot, encodeSnapshotSelector)
  }
  return map
}

export function decodeQueryTarget(map: CborMap, context: string): QueryTarget {
  const kind = field.requiredString(map, "kind", context)
  if (kind === "operational") return { kind, index: field.requiredString(map, "index", context) }
  if (kind === "lakehouse") {
    const snapshot = field.optionalMap(map, "snapshot", context)
    return {
      kind,
      destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
      destinationGeneration: field.requiredU64(map, "destination_generation", context),
      ...(snapshot === undefined
        ? {}
        : { snapshot: decodeSnapshotSelector(snapshot, `${context}.snapshot`) })
    }
  }
  throw new CodecError(`unknown query target \`${kind}\``, context, "kind")
}

function encodeSnapshotSelector(selector: SnapshotSelector): Map<string, unknown> {
  return mapOf([
    ["kind", selector.kind],
    ["value", selector.value]
  ])
}
function decodeSnapshotSelector(map: CborMap, context: string): SnapshotSelector {
  const kind = field.requiredString(map, "kind", context)
  const value = field.requiredI64(map, "value", context)
  if (kind === "snapshot_id" || kind === "timestamp_micros") return { kind, value }
  throw new CodecError(`unknown snapshot selector \`${kind}\``, context, "kind")
}
function encodeKeyMatch(value: KeyMatch): Map<string, unknown> {
  return mapOf([
    ["field", value.field],
    ["value", encodeTypedValue(value.value)]
  ])
}
function decodeKeyMatch(map: CborMap, context: string): KeyMatch {
  return {
    field: field.requiredString(map, "field", context),
    value: decodeTypedValue(map.get("value"), `${context}.value`)
  }
}
function encodePredicate(value: Predicate): Map<string, unknown> {
  return mapOf([
    ["field", value.field],
    ["op", value.op],
    ["value", encodeTypedValue(value.value)]
  ])
}
function decodePredicate(map: CborMap, context: string): Predicate {
  return {
    field: field.requiredString(map, "field", context),
    op: decodeWord(map, "op", context, [
      "eq",
      "ne",
      "lt",
      "lte",
      "gt",
      "gte",
      "in",
      "contains",
      "prefix"
    ]),
    value: decodeTypedValue(map.get("value"), `${context}.value`)
  }
}

export function encodeFilter(value: Filter): Map<string, unknown> {
  switch (value.kind) {
    case "all":
      return mapOf([["all", value.filters.map(encodeFilter)]])
    case "any":
      return mapOf([["any", value.filters.map(encodeFilter)]])
    case "not":
      return mapOf([["not", encodeFilter(value.filter)]])
    case "pred":
      return mapOf([["pred", encodePredicate(value.predicate)]])
  }
}
export function decodeFilter(value: unknown, context: string): Filter {
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "all" || tag === "any") {
    if (!Array.isArray(inner))
      throw new CodecError("filter children must be an array", context, tag)
    return {
      kind: tag,
      filters: inner.map((item, index) => decodeFilter(item, `${context}.${tag}[${String(index)}]`))
    }
  }
  if (tag === "not") return { kind: tag, filter: decodeFilter(inner, `${context}.not`) }
  if (tag === "pred")
    return {
      kind: tag,
      predicate: decodePredicate(expectMap(inner, `${context}.pred`), `${context}.pred`)
    }
  throw new CodecError(`unknown filter \`${tag}\``, context, "filter")
}
function encodeSort(value: Sort): Map<string, unknown> {
  const map = mapOf([["field", value.field]])
  if (value.dir !== "asc") map.set("dir", value.dir)
  return map
}
function decodeSort(map: CborMap, context: string): Sort {
  return {
    field: field.requiredString(map, "field", context),
    dir: decodeOptionalWord(map, "dir", context, ["asc", "desc"]) ?? "asc"
  }
}
function encodeText(value: TextQuery): Map<string, unknown> {
  const map = new Map<string, unknown>()
  optional(map, "field", value.field)
  map.set("query", value.query)
  return map
}
function decodeText(map: CborMap, context: string): TextQuery {
  const textField = field.optionalString(map, "field", context)
  return {
    ...(textField === undefined ? {} : { field: textField }),
    query: field.requiredString(map, "query", context)
  }
}
function encodeVector(value: VectorQuery): Map<string, unknown> {
  return mapOf([
    ["field", value.field],
    ["embedding", [...value.embedding]],
    ["top_k", value.topK]
  ])
}
function decodeVector(map: CborMap, context: string): VectorQuery {
  return {
    field: field.requiredString(map, "field", context),
    embedding: field.requiredArray(map, "embedding", context, (item) => {
      if (typeof item !== "number")
        throw new CodecError("embedding value must be a number", context, "embedding")
      return item
    }),
    topK: field.requiredU32(map, "top_k", context)
  }
}
function encodeAggCall(value: AggCall): Map<string, unknown> {
  const map = mapOf([["func", value.func]])
  optional(map, "field", value.field)
  optional(map, "arg", value.arg)
  map.set("alias", value.alias)
  return map
}
function decodeAggCall(map: CborMap, context: string): AggCall {
  const callField = field.optionalString(map, "field", context)
  const arg = field.optionalF64(map, "arg", context)
  return {
    func: decodeWord(map, "func", context, [
      "count",
      "count_distinct",
      "sum",
      "avg",
      "min",
      "max",
      "percentile",
      "std_dev"
    ]),
    ...(callField === undefined ? {} : { field: callField }),
    ...(arg === undefined ? {} : { arg }),
    alias: field.requiredString(map, "alias", context)
  }
}
function encodeAggregate(value: Aggregate): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (value.groupBy.length > 0) map.set("group_by", [...value.groupBy])
  map.set("funcs", value.funcs.map(encodeAggCall))
  optional(map, "window", value.window, (item) =>
    mapOf([
      ["field", item.field],
      ["every_micros", item.everyMicros]
    ])
  )
  return map
}
function decodeAggregate(map: CborMap, context: string): Aggregate {
  const window = field.optionalMap(map, "window", context)
  return {
    groupBy: field.optionalArray(map, "group_by", context, (item, index) =>
      expectString(item, `${context}.group_by[${String(index)}]`)
    ),
    funcs: field.requiredArray(map, "funcs", context, (item, index) =>
      decodeAggCall(
        expectMap(item, `${context}.funcs[${String(index)}]`),
        `${context}.funcs[${String(index)}]`
      )
    ),
    ...(window === undefined
      ? {}
      : {
          window: {
            field: field.requiredString(window, "field", context),
            everyMicros: field.requiredU64(window, "every_micros", context)
          }
        })
  }
}
function encodeSelect(value: Select): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (value.fields.length > 0) map.set("fields", [...value.fields])
  map.set("payload", value.payload)
  return map
}
function decodeSelect(map: CborMap, context: string): Select {
  return {
    fields: field.optionalArray(map, "fields", context, (item, index) =>
      expectString(item, `${context}.fields[${String(index)}]`)
    ),
    payload: field.optionalBoolean(map, "payload", context) ?? false
  }
}
function encodeRawSql(value: RawSql): Map<string, unknown> {
  const map = mapOf([
    ["dialect", value.dialect],
    ["sql", value.sql]
  ])
  if (value.params.length > 0) map.set("params", value.params.map(encodeTypedValue))
  return map
}
function decodeRawSql(map: CborMap, context: string): RawSql {
  return {
    dialect: decodeWord(map, "dialect", context, ["data_fusion", "postgres", "my_sql", "sqlite"]),
    sql: field.requiredString(map, "sql", context),
    params: field.optionalArray(map, "params", context, (item, index) =>
      decodeTypedValue(item, `${context}.params[${String(index)}]`)
    )
  }
}
function encodePageRequest(value: QueryPageRequest): Map<string, unknown> {
  const map = mapOf([["limit", value.limit]])
  optional(map, "offset", value.offset)
  optional(map, "cursor", value.cursor)
  if (value.wantTotal) map.set("want_total", true)
  return map
}
function decodePageRequest(map: CborMap, context: string): QueryPageRequest {
  const offset = field.optionalU64(map, "offset", context)
  const cursor = field.optionalString(map, "cursor", context)
  return {
    limit: field.requiredU32(map, "limit", context),
    ...(offset === undefined ? {} : { offset }),
    ...(cursor === undefined ? {} : { cursor }),
    wantTotal: field.optionalBoolean(map, "want_total", context) ?? false
  }
}

export function encodeQuery(query: Query): Map<string, unknown> {
  const map = mapOf([
    ["execution_id", idBytes(query.executionId)],
    ["target", encodeQueryTarget(query.target)],
    ["deadline_micros", query.deadlineMicros]
  ])
  if (query.byKey.length > 0) map.set("by_key", query.byKey.map(encodeKeyMatch))
  optional(map, "message_type", query.messageType)
  optional(map, "time_range", query.timeRange, (item) => [...item])
  optional(map, "filter", query.filter, encodeFilter)
  optional(map, "vector", query.vector, encodeVector)
  optional(map, "text", query.text, encodeText)
  if (query.order.length > 0) map.set("order", query.order.map(encodeSort))
  map.set("page", encodePageRequest(query.page))
  optional(map, "aggregate", query.aggregate, encodeAggregate)
  optional(map, "having", query.having, encodeFilter)
  if (query.distinct) map.set("distinct", true)
  map.set("select", encodeSelect(query.select))
  optional(map, "fork", query.fork)
  optional(map, "raw_sql", query.rawSql, encodeRawSql)
  if (query.consistency !== "eventual") map.set("consistency", query.consistency)
  return map
}

export function decodeQuery(map: CborMap, context: string): Query {
  const messageType = field.optionalString(map, "message_type", context)
  const timeRangeValue = map.get("time_range")
  let timeRange: readonly [bigint, bigint] | undefined
  if (timeRangeValue !== undefined) {
    if (!Array.isArray(timeRangeValue) || timeRangeValue.length !== 2)
      throw new CodecError("time range must contain two values", context, "time_range")
    timeRange = [asU64(timeRangeValue[0], context), asU64(timeRangeValue[1], context)]
  }
  const filter = map.get("filter")
  const vector = field.optionalMap(map, "vector", context)
  const text = field.optionalMap(map, "text", context)
  const aggregate = field.optionalMap(map, "aggregate", context)
  const having = map.get("having")
  const fork = field.optionalString(map, "fork", context)
  const rawSql = field.optionalMap(map, "raw_sql", context)
  return {
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context)),
    target: decodeQueryTarget(field.requiredMap(map, "target", context), `${context}.target`),
    deadlineMicros: field.requiredU64(map, "deadline_micros", context),
    byKey: field.optionalArray(map, "by_key", context, (item, index) =>
      decodeKeyMatch(
        expectMap(item, `${context}.by_key[${String(index)}]`),
        `${context}.by_key[${String(index)}]`
      )
    ),
    ...(messageType === undefined ? {} : { messageType }),
    ...(timeRange === undefined ? {} : { timeRange }),
    ...(filter === undefined ? {} : { filter: decodeFilter(filter, `${context}.filter`) }),
    ...(vector === undefined ? {} : { vector: decodeVector(vector, `${context}.vector`) }),
    ...(text === undefined ? {} : { text: decodeText(text, `${context}.text`) }),
    order: field.optionalArray(map, "order", context, (item, index) =>
      decodeSort(
        expectMap(item, `${context}.order[${String(index)}]`),
        `${context}.order[${String(index)}]`
      )
    ),
    page: decodePageRequest(field.requiredMap(map, "page", context), `${context}.page`),
    ...(aggregate === undefined
      ? {}
      : { aggregate: decodeAggregate(aggregate, `${context}.aggregate`) }),
    ...(having === undefined ? {} : { having: decodeFilter(having, `${context}.having`) }),
    distinct: field.optionalBoolean(map, "distinct", context) ?? false,
    select: decodeSelect(field.requiredMap(map, "select", context), `${context}.select`),
    ...(fork === undefined ? {} : { fork }),
    ...(rawSql === undefined ? {} : { rawSql: decodeRawSql(rawSql, `${context}.raw_sql`) }),
    consistency:
      decodeOptionalWord(map, "consistency", context, ["eventual", "read_your_writes", "strong"]) ??
      "eventual"
  }
}

export function validateQuery(query: Query): void {
  if (query.executionId.asU128() === 0n || query.deadlineMicros === 0n)
    throw new InvalidError("query execution identity and deadline must be nonzero")
  if (query.target.kind === "operational") validateName(query.target.index)
  else {
    if (query.target.destinationId.asU128() === 0n || query.target.destinationGeneration === 0n)
      throw new InvalidError("lakehouse target identity must be nonzero")
    if (query.target.snapshot !== undefined && query.target.snapshot.value <= 0n)
      throw new InvalidError("snapshot selector must be positive")
    if (query.fork !== undefined)
      throw new InvalidError("lakehouse queries cannot use an operational fork")
  }
  if (query.page.limit < 1 || query.page.limit > MAX_PAGE_SIZE)
    throw new InvalidError("query page limit is invalid")
  if (query.page.offset !== undefined && query.page.cursor !== undefined)
    throw new InvalidError("query page cannot contain both an offset and a cursor")
  if (query.page.cursor !== undefined) validateCursor(query.page.cursor)
  for (const [label, count] of [
    ["exact matches", query.byKey.length],
    ["sort fields", query.order.length],
    ["selected fields", query.select.fields.length]
  ] as const) {
    if (count > MAX_QUERY_FIELDS)
      throw new InvalidError(`query ${label} count exceeds cap ${String(MAX_QUERY_FIELDS)}`)
  }
  for (const match of query.byKey) {
    validateName(match.field)
    validateTypedValue(match.value)
  }
  if (query.messageType !== undefined) validateName(query.messageType)
  if (query.timeRange !== undefined && query.timeRange[0] >= query.timeRange[1])
    throw new InvalidError("query time range must be a nonempty half-open interval")
  for (const sort of query.order) validateName(sort.field)
  for (const field of query.select.fields) validateName(field)
  if (query.text !== undefined) {
    if (
      query.text.query.trim().length === 0 ||
      byteLength(query.text.query) > MAX_TEXT_QUERY_BYTES ||
      hasControlCharacter(query.text.query)
    )
      throw new InvalidError("text query is invalid")
    if (
      query.text.field !== undefined &&
      (query.text.field.length === 0 ||
        byteLength(query.text.field) > MAX_QUERY_NAME_BYTES ||
        hasControlCharacter(query.text.field))
    )
      throw new InvalidError("text query field is invalid")
  }
  if (query.vector !== undefined) {
    validateName(query.vector.field)
    if (
      query.vector.embedding.length === 0 ||
      query.vector.embedding.length > MAX_VECTOR_DIMENSIONS ||
      query.vector.embedding.some((value) => !Number.isFinite(value) || Object.is(value, -0)) ||
      query.vector.topK < 1 ||
      query.vector.topK > MAX_PAGE_SIZE
    )
      throw new InvalidError("vector query is invalid")
  }
  const visit = (filter: Filter | undefined, depth = 1, count = { value: 0 }): void => {
    if (filter === undefined) return
    count.value += 1
    if (count.value > MAX_QUERY_PREDICATES)
      throw new InvalidError("query filter node count exceeds cap")
    if (depth > 64) throw new InvalidError("query filter depth exceeds cap")
    if (filter.kind === "all" || filter.kind === "any") {
      if (filter.filters.length === 0) throw new InvalidError("filter conjunction cannot be empty")
      for (const child of filter.filters) visit(child, depth + 1, count)
    } else if (filter.kind === "not") visit(filter.filter, depth + 1, count)
    else {
      validateName(filter.predicate.field)
      validateTypedValue(filter.predicate.value)
      if (
        filter.predicate.op === "in" &&
        (filter.predicate.value.kind !== "list" || filter.predicate.value.value.length === 0)
      )
        throw new InvalidError("query IN predicate requires a nonempty typed list")
      if (filter.predicate.op === "prefix" && filter.predicate.value.kind !== "string")
        throw new InvalidError("query prefix predicate requires a string value")
    }
  }
  visit(query.filter)
  visit(query.having)
  if (query.having !== undefined && query.aggregate === undefined)
    throw new InvalidError("having requires an aggregate")
  if (query.aggregate !== undefined) {
    if (
      query.aggregate.funcs.length === 0 ||
      query.aggregate.funcs.length > MAX_QUERY_PARAMETERS ||
      query.aggregate.groupBy.length > MAX_QUERY_FIELDS
    )
      throw new InvalidError("aggregate function or group count is invalid")
    for (const field of query.aggregate.groupBy) validateName(field)
    const aliases = new Set<string>()
    for (const call of query.aggregate.funcs) {
      validateName(call.alias)
      if (aliases.has(call.alias)) throw new InvalidError("aggregate aliases must be unique")
      aliases.add(call.alias)
      if (call.field !== undefined) validateName(call.field)
      if (call.func === "count") {
        if (call.arg !== undefined)
          throw new InvalidError("count aggregate cannot carry an argument")
      } else if (call.func === "percentile") {
        if (
          call.field === undefined ||
          call.arg === undefined ||
          !Number.isFinite(call.arg) ||
          call.arg < 0 ||
          call.arg > 1
        )
          throw new InvalidError("percentile aggregate is invalid")
      } else if (call.field === undefined || call.arg !== undefined) {
        throw new InvalidError("aggregate field and argument do not match its function")
      }
    }
    if (query.aggregate.window !== undefined) {
      validateName(query.aggregate.window.field)
      if (query.aggregate.window.everyMicros === 0n)
        throw new InvalidError("aggregate window duration must be nonzero")
    }
    if (query.select.fields.length > 0 || query.select.payload)
      throw new InvalidError("aggregate query cannot select rows")
  }
  if (query.rawSql !== undefined) {
    if (
      query.rawSql.sql.trim().length === 0 ||
      byteLength(query.rawSql.sql) > MAX_RAW_SQL_BYTES ||
      hasDisallowedSqlControl(query.rawSql.sql) ||
      query.rawSql.params.length > MAX_QUERY_PARAMETERS
    )
      throw new InvalidError("raw SQL is invalid")
    for (const param of query.rawSql.params) validateTypedValue(param)
    if (
      query.byKey.length > 0 ||
      query.messageType !== undefined ||
      query.timeRange !== undefined ||
      query.filter !== undefined ||
      query.vector !== undefined ||
      query.text !== undefined ||
      query.order.length > 0 ||
      query.aggregate !== undefined ||
      query.having !== undefined ||
      query.distinct ||
      query.select.fields.length > 0 ||
      query.select.payload
    )
      throw new InvalidError("raw SQL cannot be combined with the structured query expression")
  }
}

export function validateQueryEnvelope(envelope: QueryEnvelope): void {
  validateQueryControl(envelope.v, envelope.query.executionId)
  validateQuery(envelope.query)
}

export function validateQueryPageEnvelope(envelope: QueryPageEnvelope): void {
  validateQueryControl(envelope.v, envelope.executionId)
  validateCursor(envelope.cursor)
  if (envelope.deadlineMicros === 0n) throw new InvalidError("query page deadline must be nonzero")
}

export function validateQueryCancelEnvelope(envelope: QueryCancelEnvelope): void {
  validateQueryControl(envelope.v, envelope.executionId)
}

export function validateQueryStatusEnvelope(envelope: QueryStatusEnvelope): void {
  validateQueryControl(envelope.v, envelope.executionId)
}

function validateQueryControl(version: number, executionId: QueryExecutionId): void {
  if (version !== QUERY_OP_VERSION)
    throw new InvalidError(`query version must be ${String(QUERY_OP_VERSION)}`)
  if (executionId.asU128() === 0n) throw new InvalidError("query execution id must be nonzero")
}

function validateCursor(cursor: string): void {
  if (
    cursor.length === 0 ||
    byteLength(cursor) > MAX_QUERY_CURSOR_BYTES ||
    hasControlCharacter(cursor)
  )
    throw new InvalidError("query cursor is invalid")
}

export function encodeQueryEnvelope(envelope: QueryEnvelope): Map<string, unknown> {
  return mapOf([
    ["v", envelope.v],
    ["query", encodeQuery(envelope.query)]
  ])
}
export function decodeQueryEnvelope(map: CborMap, context: string): QueryEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    query: decodeQuery(field.requiredMap(map, "query", context), `${context}.query`)
  }
}
export function encodeQueryEnvelopeFrame(envelope: QueryEnvelope): Uint8Array {
  return encodeNamed(encodeQueryEnvelope(envelope), { forceFloatNumbers: true })
}
export function decodeQueryEnvelopeFrame(bytes: Uint8Array): QueryEnvelope {
  return decodeQueryEnvelope(
    expectMap(decodeOne(bytes, "QueryEnvelope"), "QueryEnvelope"),
    "QueryEnvelope"
  )
}

export interface Row {
  readonly values: readonly TypedValue[]
  readonly score?: number
}
export interface Page {
  readonly offset?: bigint
  readonly limit: number
  readonly total?: bigint
  readonly hasMore: boolean
  readonly nextCursor?: string
}
export interface QueryEngine {
  readonly name: string
  readonly version: string
  readonly dialect?: SqlDialect
}
export type ResolvedQueryTarget =
  | {
      readonly kind: "operational"
      readonly index: string
      readonly backendResourceId: BackendResourceId
      readonly backendGeneration: bigint
      readonly runtimeConfigurationRevision: bigint
    }
  | {
      readonly kind: "lakehouse"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly backendResourceId: BackendResourceId
      readonly backendGeneration: bigint
      readonly runtimeConfigurationRevision: bigint
      readonly tableUuid: Uint8Array
      readonly namespace: readonly string[]
      readonly table: string
      readonly snapshotId: bigint
      readonly schemaId: number
      readonly partitionSpecId: number
    }
export type BoundaryRelation = "current" | "historical" | "ahead_of_observed_checkpoint"
export interface MaterializationBoundary {
  readonly digest: Uint8Array
  readonly relationToCurrent: BoundaryRelation
}
export interface QueryContext {
  readonly executionId: QueryExecutionId
  readonly engine: QueryEngine
  readonly resolvedTarget: ResolvedQueryTarget
  readonly requestedConsistency: Consistency
  readonly deliveredConsistency: Consistency
  readonly boundary?: MaterializationBoundary
  readonly checkpointRevision?: bigint
  readonly globalStateRevision?: bigint
  readonly truncated: boolean
  readonly elapsedMicros: bigint
  readonly scannedBytes: bigint
  readonly producedBytes: bigint
  readonly rowCount: bigint
}
export interface QueryResult {
  readonly fields: readonly LogicalField[]
  readonly rows: readonly Row[]
  readonly page: Page
  readonly context: QueryContext
}

function encodeRow(value: Row): Map<string, unknown> {
  const map = mapOf([["values", value.values.map(encodeTypedValue)]])
  optional(map, "score", value.score)
  return map
}
function decodeRow(map: CborMap, context: string): Row {
  const score = field.optionalF64(map, "score", context)
  return {
    values: field.requiredArray(map, "values", context, (item, index) =>
      decodeTypedValue(item, `${context}.values[${String(index)}]`)
    ),
    ...(score === undefined ? {} : { score })
  }
}
function encodePage(value: Page): Map<string, unknown> {
  const map = new Map<string, unknown>()
  optional(map, "offset", value.offset)
  map.set("limit", value.limit)
  optional(map, "total", value.total)
  map.set("has_more", value.hasMore)
  optional(map, "next_cursor", value.nextCursor)
  return map
}
function decodePage(map: CborMap, context: string): Page {
  const offset = field.optionalU64(map, "offset", context)
  const total = field.optionalU64(map, "total", context)
  const nextCursor = field.optionalString(map, "next_cursor", context)
  return {
    ...(offset === undefined ? {} : { offset }),
    limit: field.requiredU32(map, "limit", context),
    ...(total === undefined ? {} : { total }),
    hasMore: field.requiredBoolean(map, "has_more", context),
    ...(nextCursor === undefined ? {} : { nextCursor })
  }
}
function encodeEngine(value: QueryEngine): Map<string, unknown> {
  const map = mapOf([
    ["name", value.name],
    ["version", value.version]
  ])
  optional(map, "dialect", value.dialect)
  return map
}
function decodeEngine(map: CborMap, context: string): QueryEngine {
  const dialect = decodeOptionalWord(map, "dialect", context, [
    "data_fusion",
    "postgres",
    "my_sql",
    "sqlite"
  ])
  return {
    name: field.requiredString(map, "name", context),
    version: field.requiredString(map, "version", context),
    ...(dialect === undefined ? {} : { dialect })
  }
}
function encodeResolvedTarget(value: ResolvedQueryTarget): Map<string, unknown> {
  const map = mapOf([["kind", value.kind]])
  if (value.kind === "operational") map.set("index", value.index)
  else {
    map.set("destination_id", idBytes(value.destinationId))
    map.set("destination_generation", value.destinationGeneration)
    map.set("table_uuid", value.tableUuid)
    map.set("namespace", [...value.namespace])
    map.set("table", value.table)
    map.set("snapshot_id", value.snapshotId)
    map.set("schema_id", value.schemaId)
    map.set("partition_spec_id", value.partitionSpecId)
  }
  map.set("backend_resource_id", idBytes(value.backendResourceId))
  map.set("backend_generation", value.backendGeneration)
  map.set("runtime_configuration_revision", value.runtimeConfigurationRevision)
  return map
}
function decodeResolvedTarget(map: CborMap, context: string): ResolvedQueryTarget {
  const kind = field.requiredString(map, "kind", context)
  const common = {
    backendResourceId: BackendResourceId.fromBytes(
      field.requiredBytes(map, "backend_resource_id", context)
    ),
    backendGeneration: field.requiredU64(map, "backend_generation", context),
    runtimeConfigurationRevision: field.requiredU64(map, "runtime_configuration_revision", context)
  }
  if (kind === "operational")
    return { kind, index: field.requiredString(map, "index", context), ...common }
  if (kind === "lakehouse")
    return {
      kind,
      destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
      destinationGeneration: field.requiredU64(map, "destination_generation", context),
      ...common,
      tableUuid: fixedBytes(field.requiredBytes(map, "table_uuid", context), 16, context),
      namespace: field.requiredArray(map, "namespace", context, (item, index) =>
        expectString(item, `${context}.namespace[${String(index)}]`)
      ),
      table: field.requiredString(map, "table", context),
      snapshotId: field.requiredI64(map, "snapshot_id", context),
      schemaId: field.requiredI32(map, "schema_id", context),
      partitionSpecId: field.requiredI32(map, "partition_spec_id", context)
    }
  throw new CodecError(`unknown resolved target \`${kind}\``, context, "kind")
}
function encodeContext(value: QueryContext): Map<string, unknown> {
  const map = mapOf([
    ["execution_id", idBytes(value.executionId)],
    ["engine", encodeEngine(value.engine)],
    ["resolved_target", encodeResolvedTarget(value.resolvedTarget)],
    ["requested_consistency", value.requestedConsistency],
    ["delivered_consistency", value.deliveredConsistency]
  ])
  optional(map, "boundary", value.boundary, (item) =>
    mapOf([
      ["digest", item.digest],
      ["relation_to_current", item.relationToCurrent]
    ])
  )
  optional(map, "checkpoint_revision", value.checkpointRevision)
  optional(map, "global_state_revision", value.globalStateRevision)
  map.set("truncated", value.truncated)
  map.set("elapsed_micros", value.elapsedMicros)
  map.set("scanned_bytes", value.scannedBytes)
  map.set("produced_bytes", value.producedBytes)
  map.set("row_count", value.rowCount)
  return map
}
function decodeContext(map: CborMap, context: string): QueryContext {
  const boundary = field.optionalMap(map, "boundary", context)
  const checkpointRevision = field.optionalU64(map, "checkpoint_revision", context)
  const globalStateRevision = field.optionalU64(map, "global_state_revision", context)
  return {
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context)),
    engine: decodeEngine(field.requiredMap(map, "engine", context), `${context}.engine`),
    resolvedTarget: decodeResolvedTarget(
      field.requiredMap(map, "resolved_target", context),
      `${context}.resolved_target`
    ),
    requestedConsistency: decodeWord(map, "requested_consistency", context, [
      "eventual",
      "read_your_writes",
      "strong"
    ]),
    deliveredConsistency: decodeWord(map, "delivered_consistency", context, [
      "eventual",
      "read_your_writes",
      "strong"
    ]),
    ...(boundary === undefined
      ? {}
      : {
          boundary: {
            digest: fixedBytes(field.requiredBytes(boundary, "digest", context), 32, context),
            relationToCurrent: decodeWord(boundary, "relation_to_current", context, [
              "current",
              "historical",
              "ahead_of_observed_checkpoint"
            ])
          }
        }),
    ...(checkpointRevision === undefined ? {} : { checkpointRevision }),
    ...(globalStateRevision === undefined ? {} : { globalStateRevision }),
    truncated: field.requiredBoolean(map, "truncated", context),
    elapsedMicros: field.requiredU64(map, "elapsed_micros", context),
    scannedBytes: field.requiredU64(map, "scanned_bytes", context),
    producedBytes: field.requiredU64(map, "produced_bytes", context),
    rowCount: field.requiredU64(map, "row_count", context)
  }
}
export function encodeQueryResult(value: QueryResult): Map<string, unknown> {
  return mapOf([
    ["fields", value.fields.map(encodeLogicalField)],
    ["rows", value.rows.map(encodeRow)],
    ["page", encodePage(value.page)],
    ["context", encodeContext(value.context)]
  ])
}
export function decodeQueryResult(map: CborMap, context: string): QueryResult {
  const result: QueryResult = {
    fields: field.requiredArray(map, "fields", context, (item, index) =>
      decodeLogicalField(
        expectMap(item, `${context}.fields[${String(index)}]`),
        `${context}.fields[${String(index)}]`
      )
    ),
    rows: field.requiredArray(map, "rows", context, (item, index) =>
      decodeRow(
        expectMap(item, `${context}.rows[${String(index)}]`),
        `${context}.rows[${String(index)}]`
      )
    ),
    page: decodePage(field.requiredMap(map, "page", context), `${context}.page`),
    context: decodeContext(field.requiredMap(map, "context", context), `${context}.context`)
  }
  validateQueryResult(result)
  return result
}

export function validateQueryResult(result: QueryResult): void {
  validateResultFields(result.fields)
  for (const row of result.rows) {
    if (row.values.length !== result.fields.length)
      throw new InvalidError("query row value count does not match the result schema")
    for (let index = 0; index < result.fields.length; index += 1) {
      const field = result.fields[index]
      const value = row.values[index]
      if (field === undefined || value === undefined)
        throw new InvalidError("query row value count does not match the result schema")
      validateTypedValueAgainst(value, field.fieldType, field.required)
    }
    if (row.score !== undefined && !Number.isFinite(row.score))
      throw new InvalidError("query row score must be finite")
  }
  if (result.context.executionId.asU128() === 0n)
    throw new InvalidError("query result execution id must be nonzero")
  if (result.context.rowCount !== BigInt(result.rows.length))
    throw new InvalidError("query result row count does not match the page rows")
  validateQueryContext(result.context)
  if (result.page.limit < 1 || result.page.limit > MAX_PAGE_SIZE)
    throw new InvalidError("query result page limit is invalid")
  if (result.page.hasMore !== (result.page.nextCursor !== undefined))
    throw new InvalidError("query result has_more and next_cursor must agree")
  if (result.page.nextCursor !== undefined) validateCursor(result.page.nextCursor)
}

function validateQueryContext(context: QueryContext): void {
  if (context.executionId.asU128() === 0n)
    throw new InvalidError("query context execution id must be nonzero")
  validateName(context.engine.name)
  validateName(context.engine.version)
  const consistencyRank: Record<Consistency, number> = {
    eventual: 0,
    read_your_writes: 1,
    strong: 2
  }
  if (consistencyRank[context.deliveredConsistency] < consistencyRank[context.requestedConsistency])
    throw new InvalidError("delivered consistency is weaker than requested consistency")
  const target = context.resolvedTarget
  if (target.kind === "operational") {
    validateName(target.index)
    if (
      target.backendResourceId.asU128() === 0n ||
      target.backendGeneration === 0n ||
      target.runtimeConfigurationRevision === 0n
    )
      throw new InvalidError("operational query context is missing backend evidence")
    if (
      context.boundary !== undefined ||
      context.checkpointRevision !== undefined ||
      context.globalStateRevision !== undefined
    )
      throw new InvalidError("operational query context cannot carry lakehouse evidence")
  } else {
    if (
      target.destinationId.asU128() === 0n ||
      target.destinationGeneration === 0n ||
      target.backendResourceId.asU128() === 0n ||
      target.backendGeneration === 0n ||
      target.runtimeConfigurationRevision === 0n ||
      target.tableUuid.length !== 16 ||
      target.namespace.length === 0 ||
      target.snapshotId <= 0n ||
      target.schemaId < 0 ||
      target.partitionSpecId < 0 ||
      context.boundary === undefined ||
      context.checkpointRevision === undefined ||
      context.globalStateRevision === undefined
    )
      throw new InvalidError("lakehouse query context is missing resolved target evidence")
    for (const part of target.namespace) validateName(part)
    validateName(target.table)
  }
  if (context.boundary !== undefined && context.boundary.digest.length !== 32)
    throw new InvalidError("query materialization boundary digest must contain 32 bytes")
}
export function queryResultValue(
  result: QueryResult,
  row: Row,
  name: string
): TypedValue | undefined {
  const index = result.fields.findIndex((item) => item.name === name)
  return index < 0 ? undefined : row.values[index]
}

export type QueryError =
  | {
      readonly kind:
        | "unsupported"
        | "unauthorized"
        | "index_not_found"
        | "fork_not_found"
        | "backend"
        | "unavailable"
      readonly message: string
    }
  | {
      readonly kind: "too_large"
      readonly what: string
      readonly size: bigint
      readonly cap: bigint
    }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | {
      readonly kind: "stale"
      readonly what: string
      readonly applied: bigint
      readonly required: bigint
    }
  | { readonly kind: "cancelled" | "deadline_exceeded"; readonly executionId: QueryExecutionId }
  | { readonly kind: "expired_snapshot"; readonly snapshotId: bigint }
  | {
      readonly kind: "stale_generation"
      readonly what: string
      readonly requested: bigint
      readonly observed: bigint
    }
  | { readonly kind: "target_unavailable"; readonly reason: string }
  | {
      readonly kind: "resource_limit"
      readonly resource: string
      readonly observed: bigint
      readonly limit: bigint
    }
export type QueryErrorCode = QueryError["kind"]
export type QueryReply =
  | { readonly kind: "ok"; readonly result: QueryResult }
  | { readonly kind: "err"; readonly error: QueryError }

function snakeToRustVariant(value: string): string {
  return value
    .split("_")
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join("")
}
function rustVariantToSnake(value: string): string {
  return value.replaceAll(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase()
}
export function encodeQueryError(error: QueryError): Map<string, unknown> {
  const tag = snakeToRustVariant(error.kind)
  if ("message" in error) return mapOf([[tag, error.message]])
  const body = new Map<string, unknown>()
  for (const [key, value] of Object.entries(error)) {
    if (key === "kind") continue
    const wireKey = key.replaceAll(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase()
    body.set(wireKey, value instanceof QueryExecutionId ? value.toBytes() : value)
  }
  return mapOf([[tag, body]])
}
export function decodeQueryError(value: unknown, context: string): QueryError {
  const [tag, body] = singleVariantTag(value, context)
  const kind = rustVariantToSnake(tag)
  if (
    [
      "unsupported",
      "unauthorized",
      "index_not_found",
      "fork_not_found",
      "backend",
      "unavailable"
    ].includes(kind)
  )
    return { kind: kind as "unsupported", message: expectString(body, context) }
  const map = expectMap(body, context)
  switch (kind) {
    case "too_large":
      return {
        kind,
        what: field.requiredString(map, "what", context),
        size: field.requiredU64(map, "size", context),
        cap: field.requiredU64(map, "cap", context)
      }
    case "version":
      return {
        kind,
        expected: field.requiredU32(map, "expected", context),
        got: field.requiredU32(map, "got", context)
      }
    case "stale":
      return {
        kind,
        what: field.requiredString(map, "what", context),
        applied: field.requiredU64(map, "applied", context),
        required: field.requiredU64(map, "required", context)
      }
    case "cancelled":
    case "deadline_exceeded":
      return {
        kind,
        executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context))
      }
    case "expired_snapshot":
      return { kind, snapshotId: field.requiredI64(map, "snapshot_id", context) }
    case "stale_generation":
      return {
        kind,
        what: field.requiredString(map, "what", context),
        requested: field.requiredU64(map, "requested", context),
        observed: field.requiredU64(map, "observed", context)
      }
    case "target_unavailable":
      return { kind, reason: field.requiredString(map, "reason", context) }
    case "resource_limit":
      return {
        kind,
        resource: field.requiredString(map, "resource", context),
        observed: field.requiredU64(map, "observed", context),
        limit: field.requiredU64(map, "limit", context)
      }
    default:
      throw new CodecError(`unknown query error \`${tag}\``, context, "error")
  }
}
export function encodeQueryReplyFrame(reply: QueryReply): Uint8Array {
  return encodeNamed(
    reply.kind === "ok"
      ? mapOf([["Ok", encodeQueryResult(reply.result)]])
      : mapOf([["Err", encodeQueryError(reply.error)]]),
    { forceFloatNumbers: true }
  )
}
export function decodeQueryReply(value: unknown, context: string): QueryReply {
  const [tag, body] = singleVariantTag(value, context)
  if (tag === "Ok")
    return { kind: "ok", result: decodeQueryResult(expectMap(body, context), context) }
  if (tag === "Err") return { kind: "err", error: decodeQueryError(body, context) }
  throw new CodecError(`unknown query reply \`${tag}\``, context, "reply")
}
export function decodeQueryReplyFrame(bytes: Uint8Array): QueryReply {
  return decodeQueryReply(decodeOne(bytes, "QueryReply"), "QueryReply")
}

export type QueryExecutionState =
  "queued" | "planning" | "running" | "completed" | "cancelled" | "failed" | "expired"
export interface QueryExecutionStatus {
  readonly executionId: QueryExecutionId
  readonly state: QueryExecutionState
  readonly startedAtMicros: bigint
  readonly finishedAtMicros?: bigint
  readonly scannedBytes: bigint
  readonly producedBytes: bigint
  readonly rowCount: bigint
  readonly error?: QueryError
}
export type QueryStatusReply =
  | { readonly kind: "ok"; readonly status: QueryExecutionStatus }
  | { readonly kind: "err"; readonly error: QueryError }
export type QueryCancelReply = QueryStatusReply

export function encodeQueryExecutionStatus(value: QueryExecutionStatus): Map<string, unknown> {
  const map = mapOf([
    ["execution_id", value.executionId.toBytes()],
    ["state", value.state],
    ["started_at_micros", value.startedAtMicros]
  ])
  optional(map, "finished_at_micros", value.finishedAtMicros)
  map.set("scanned_bytes", value.scannedBytes)
  map.set("produced_bytes", value.producedBytes)
  map.set("row_count", value.rowCount)
  optional(map, "error", value.error, encodeQueryError)
  return map
}
export function decodeQueryExecutionStatus(map: CborMap, context: string): QueryExecutionStatus {
  const finishedAtMicros = field.optionalU64(map, "finished_at_micros", context)
  const error = map.get("error")
  const status: QueryExecutionStatus = {
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context)),
    state: decodeWord(map, "state", context, [
      "queued",
      "planning",
      "running",
      "completed",
      "cancelled",
      "failed",
      "expired"
    ]),
    startedAtMicros: field.requiredU64(map, "started_at_micros", context),
    ...(finishedAtMicros === undefined ? {} : { finishedAtMicros }),
    scannedBytes: field.requiredU64(map, "scanned_bytes", context),
    producedBytes: field.requiredU64(map, "produced_bytes", context),
    rowCount: field.requiredU64(map, "row_count", context),
    ...(error === undefined ? {} : { error: decodeQueryError(error, `${context}.error`) })
  }
  validateQueryExecutionStatus(status)
  return status
}

export function validateQueryExecutionStatus(status: QueryExecutionStatus): void {
  if (status.executionId.asU128() === 0n || status.startedAtMicros === 0n)
    throw new InvalidError("query execution identity and start time must be nonzero")
  const terminal =
    status.state === "completed" ||
    status.state === "cancelled" ||
    status.state === "failed" ||
    status.state === "expired"
  if (
    (terminal &&
      (status.finishedAtMicros === undefined ||
        status.finishedAtMicros < status.startedAtMicros)) ||
    (!terminal && status.finishedAtMicros !== undefined)
  )
    throw new InvalidError("query execution finish time does not match its state")
  if ((status.state === "failed") !== (status.error !== undefined))
    throw new InvalidError("query execution error must be present exactly for failed state")
}
export function encodeQueryStatusReplyFrame(reply: QueryStatusReply): Uint8Array {
  return encodeNamed(
    reply.kind === "ok"
      ? mapOf([["Ok", encodeQueryExecutionStatus(reply.status)]])
      : mapOf([["Err", encodeQueryError(reply.error)]])
  )
}
export function decodeQueryStatusReply(value: unknown, context: string): QueryStatusReply {
  const [tag, body] = singleVariantTag(value, context)
  if (tag === "Ok")
    return { kind: "ok", status: decodeQueryExecutionStatus(expectMap(body, context), context) }
  if (tag === "Err") return { kind: "err", error: decodeQueryError(body, context) }
  throw new CodecError(`unknown query status reply \`${tag}\``, context, "reply")
}
export function decodeQueryStatusReplyFrame(bytes: Uint8Array): QueryStatusReply {
  return decodeQueryStatusReply(decodeOne(bytes, "QueryStatusReply"), "QueryStatusReply")
}

function decodeWord<const T extends string>(
  map: CborMap,
  key: string,
  context: string,
  allowed: readonly T[]
): T {
  const value = field.requiredString(map, key, context)
  if (!allowed.includes(value as T))
    throw new CodecError(`field \`${key}\` has unsupported value \`${value}\``, context, key)
  return value as T
}
function decodeOptionalWord<const T extends string>(
  map: CborMap,
  key: string,
  context: string,
  allowed: readonly T[]
): T | undefined {
  const value = field.optionalString(map, key, context)
  if (value === undefined) return undefined
  if (!allowed.includes(value as T))
    throw new CodecError(`field \`${key}\` has unsupported value \`${value}\``, context, key)
  return value as T
}
function asU64(value: unknown, context: string): bigint {
  if (typeof value === "bigint" && value >= 0n) return value
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value)
  throw new CodecError("value must fit u64", context, "value")
}
function fixedBytes(value: Uint8Array, length: number, context: string): Uint8Array {
  if (value.length !== length)
    throw new CodecError(`expected ${String(length)} bytes`, context, "bytes")
  return value
}
function validateName(value: string): void {
  if (
    value.length === 0 ||
    byteLength(value) > MAX_QUERY_NAME_BYTES ||
    value.trim() !== value ||
    hasControlCharacter(value)
  )
    throw new InvalidError("query name is invalid")
}
function byteLength(value: string): number {
  return new TextEncoder().encode(value).length
}
function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    if (code < 32 || code === 127) return true
  }
  return false
}
function hasDisallowedSqlControl(value: string): boolean {
  for (const character of value) {
    if (
      hasControlCharacter(character) &&
      character !== "\n" &&
      character !== "\r" &&
      character !== "\t"
    )
      return true
  }
  return false
}

export function parseConsistency(value: string, context: string): Consistency {
  if (value === "eventual" || value === "read_your_writes" || value === "strong") return value
  throw new CodecError(`unknown consistency \`${value}\``, context, "consistency")
}
export function consistencyToWord(value: Consistency): string {
  return value
}
export function pageAtLeast(page: Page, rowsOnPage: number): bigint | undefined {
  return page.offset === undefined ? undefined : page.offset + BigInt(rowsOnPage)
}
export function pageTotalPages(page: Page): bigint | undefined {
  return page.total === undefined || page.limit === 0
    ? undefined
    : (page.total + BigInt(page.limit) - 1n) / BigInt(page.limit)
}
export function consistencyGateCheck(
  applied: bigint,
  required: bigint,
  consistency: Consistency,
  what: string
): QueryError | undefined {
  return consistency === "eventual" || applied >= required
    ? undefined
    : { kind: "stale", what, applied, required }
}
