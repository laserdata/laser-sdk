import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  cborFloat,
  decodeOne,
  encodeNamed,
  expectMap,
  expectString,
  expectU32,
  field,
  singleVariantTag
} from "./cbor.js"
import { BackendResourceId, QueryExecutionId } from "./ids.js"
import {
  type LogicalField,
  type TypedValue,
  decodeLogicalField,
  decodeTypedValue,
  encodeLogicalField,
  encodeTypedValue,
  typedValueAsI64,
  typedValueAsU64,
  typedValueDiagnosticText,
  validateResultFields,
  typedValueValidateAgainst,
  typedValueValidateCanonical
} from "./schema.js"
import {
  MAX_PAGE_SIZE,
  MAX_QUERY_FIELDS,
  MAX_QUERY_NAME_BYTES,
  MAX_QUERY_OFFSET,
  MAX_QUERY_PARAMETERS,
  MAX_QUERY_PREDICATES,
  MAX_RAW_SQL_BYTES,
  MAX_TEXT_QUERY_BYTES,
  MAX_VECTOR_DIMENSIONS
} from "./limits.js"
import { QUERY_OP_VERSION } from "./codes.js"

export type Consistency = "eventual" | "read_your_writes"

export function consistencyIsEventual(consistency: Consistency): boolean {
  return consistency === "eventual"
}
export type SqlDialect = "postgres" | "my_sql" | "sqlite"
export interface QueryTarget {
  readonly kind: "operational"
  readonly index: string
}

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
export function operationalTarget(index: string): QueryTarget {
  return { kind: "operational", index }
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

/**
 * Builds a `Query` field for field, like Rust `Query::builder()`. `executionId`,
 * `target`, and `deadlineMicros` are required. Every other field starts at the
 * `newQuery` default, and passing `undefined` to an optional field clears it.
 */
export class QueryBuilder {
  private executionIdValue: QueryExecutionId | undefined
  private targetValue: QueryTarget | undefined
  private deadlineMicrosValue: bigint | undefined
  private fields: QueryBuilderFields = {
    byKey: [],
    order: [],
    page: { limit: 50, offset: 0n, wantTotal: false },
    distinct: false,
    select: { fields: [], payload: false },
    consistency: "eventual"
  }

  executionId(executionId: QueryExecutionId): this {
    this.executionIdValue = executionId
    return this
  }

  target(target: QueryTarget): this {
    this.targetValue = target
    return this
  }

  /** The absolute Unix deadline in microseconds. */
  deadlineMicros(deadlineMicros: bigint): this {
    this.deadlineMicrosValue = deadlineMicros
    return this
  }

  byKey(byKey: readonly KeyMatch[]): this {
    return this.with({ byKey })
  }

  messageType(messageType: string | undefined): this {
    return this.with({ messageType })
  }

  /** The `[start, end]` range in epoch microseconds, both bounds inclusive. */
  timeRange(timeRange: readonly [bigint, bigint] | undefined): this {
    return this.with({ timeRange })
  }

  filter(filter: Filter | undefined): this {
    return this.with({ filter })
  }

  vector(vector: VectorQuery | undefined): this {
    return this.with({ vector })
  }

  text(text: TextQuery | undefined): this {
    return this.with({ text })
  }

  order(order: readonly Sort[]): this {
    return this.with({ order })
  }

  page(page: QueryPageRequest): this {
    return this.with({ page })
  }

  aggregate(aggregate: Aggregate | undefined): this {
    return this.with({ aggregate })
  }

  having(having: Filter | undefined): this {
    return this.with({ having })
  }

  distinct(distinct: boolean): this {
    return this.with({ distinct })
  }

  select(select: Select): this {
    return this.with({ select })
  }

  /** The fork whose view to read, or `undefined` for the trunk. */
  fork(fork: string | undefined): this {
    return this.with({ fork })
  }

  rawSql(rawSql: RawSql | undefined): this {
    return this.with({ rawSql })
  }

  consistency(consistency: Consistency): this {
    return this.with({ consistency })
  }

  /** The query. Throws `InvalidError` when a required field is unset. */
  build(): Query {
    if (this.executionIdValue === undefined) throw missingQueryField("executionId")
    if (this.targetValue === undefined) throw missingQueryField("target")
    if (this.deadlineMicrosValue === undefined) throw missingQueryField("deadlineMicros")
    const query: Record<string, unknown> = {
      executionId: this.executionIdValue,
      target: this.targetValue,
      deadlineMicros: this.deadlineMicrosValue
    }
    for (const [name, value] of Object.entries(this.fields)) {
      if (value !== undefined) query[name] = value
    }
    return query as unknown as Query
  }

  private with(fields: QueryBuilderFields): this {
    this.fields = { ...this.fields, ...fields }
    return this
  }
}

type QueryBuilderFields = {
  readonly [K in Exclude<keyof Query, "executionId" | "target" | "deadlineMicros">]?:
    Query[K] | undefined
}

function missingQueryField(name: string): InvalidError {
  return new InvalidError(`query builder requires ${name}`)
}

export function operationalQuery(
  executionId: QueryExecutionId,
  index: string,
  deadlineMicros: bigint
): Query {
  return newQuery(operationalTarget(index), executionId, deadlineMicros)
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
  return mapOf([
    ["kind", target.kind],
    ["index", target.index]
  ])
}

export function decodeQueryTarget(map: CborMap, context: string): QueryTarget {
  const kind = field.requiredString(map, "kind", context)
  if (kind === "operational") return { kind, index: field.requiredString(map, "index", context) }
  throw new CodecError(`unknown query target \`${kind}\``, context, "kind")
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
    ["embedding", value.embedding.map(cborFloat)],
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
  optional(map, "arg", value.arg, cborFloat)
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
    dialect: decodeWord(map, "dialect", context, ["postgres", "my_sql", "sqlite"]),
    sql: field.requiredString(map, "sql", context),
    params: field.optionalArray(map, "params", context, (item, index) =>
      decodeTypedValue(item, `${context}.params[${String(index)}]`)
    )
  }
}
function encodePageRequest(value: QueryPageRequest): Map<string, unknown> {
  const map = mapOf([["limit", value.limit]])
  optional(map, "offset", value.offset)
  if (value.wantTotal) map.set("want_total", true)
  return map
}
function decodePageRequest(map: CborMap, context: string): QueryPageRequest {
  const offset = field.optionalU64(map, "offset", context)
  return {
    limit: field.requiredU32(map, "limit", context),
    ...(offset === undefined ? {} : { offset }),
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
      decodeOptionalWord(map, "consistency", context, ["eventual", "read_your_writes"]) ??
      "eventual"
  }
}

export function validateQuery(query: Query): void {
  if (query.executionId.asU128() === 0n || query.deadlineMicros === 0n)
    throw new InvalidError("query execution identity and deadline must be nonzero")
  validateName(query.target.index)
  if (
    !Number.isSafeInteger(query.page.limit) ||
    query.page.limit < 1 ||
    query.page.limit > MAX_PAGE_SIZE
  )
    throw new InvalidError("query page limit is invalid")
  if (query.page.offset !== undefined && query.page.offset > BigInt(MAX_QUERY_OFFSET))
    throw new InvalidError(`query offset exceeds cap ${String(MAX_QUERY_OFFSET)}`)
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
    typedValueValidateCanonical(match.value)
  }
  if (query.messageType !== undefined) validateName(query.messageType)
  if (query.timeRange !== undefined && query.timeRange[0] > query.timeRange[1])
    throw new InvalidError(
      "query time range start must not be after its end, and both bounds are inclusive"
    )
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
      !Number.isSafeInteger(query.vector.topK) ||
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
      typedValueValidateCanonical(filter.predicate.value)
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
  const shapes = [
    query.text !== undefined,
    query.vector !== undefined,
    query.aggregate !== undefined,
    query.distinct
  ].filter(Boolean).length
  if (shapes > 1)
    throw new InvalidError("a query takes at most one of text, vector, aggregate and distinct")
  if (query.text !== undefined && (query.order.length > 0 || query.page.wantTotal))
    throw new InvalidError("a text query ranks by relevance and takes no order or total")
  if (
    query.vector !== undefined &&
    (query.order.length > 0 || query.page.wantTotal || (query.page.offset ?? 0n) > 0n)
  )
    throw new InvalidError(
      "a vector query returns its nearest rows and takes no order, offset or total"
    )
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
    for (const param of query.rawSql.params) typedValueValidateCanonical(param)
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

function validateQueryControl(version: number, executionId: QueryExecutionId): void {
  if (version !== QUERY_OP_VERSION)
    throw new InvalidError(`query version must be ${String(QUERY_OP_VERSION)}`)
  if (executionId.asU128() === 0n) throw new InvalidError("query execution id must be nonzero")
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
  return encodeNamed(encodeQueryEnvelope(envelope))
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
}
export interface QueryEngine {
  readonly name: string
  readonly version: string
  readonly dialect?: SqlDialect
}
export interface ResolvedQueryTarget {
  readonly kind: "operational"
  readonly index: string
  readonly backendResourceId: BackendResourceId
  readonly backendGeneration: bigint
  readonly runtimeConfigurationRevision: bigint
}
export interface QueryContext {
  readonly executionId: QueryExecutionId
  readonly engine: QueryEngine
  readonly resolvedTarget: ResolvedQueryTarget
  readonly requestedConsistency: Consistency
  readonly deliveredConsistency: Consistency
  readonly truncated: boolean
  readonly elapsedMicros: bigint
  readonly scannedBytes: bigint
  readonly producedBytes: bigint
  readonly rowCount: bigint
  /** How far the store that served the query had applied each source
   * partition of the index. A reply from another node can sit at another
   * point of the same log, so compare positions, not wall time. Empty when the
   * store reports no positions. */
  readonly frontier: readonly QueryFrontier[]
}
/** One source partition's applied position in the store that served a query. */
export interface QueryFrontier {
  readonly stream: string
  readonly topic: string
  readonly partitionId: number
  /** Creation time of the topic incarnation the rows come from, when the
   * serving node knows it. Offsets of different incarnations do not compare. */
  readonly topicCreatedAtMicros?: bigint
  /** The last offset the store applied. */
  readonly appliedOffset: bigint
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
  return map
}
function decodePage(map: CborMap, context: string): Page {
  const offset = field.optionalU64(map, "offset", context)
  const total = field.optionalU64(map, "total", context)
  return {
    ...(offset === undefined ? {} : { offset }),
    limit: field.requiredU32(map, "limit", context),
    ...(total === undefined ? {} : { total }),
    hasMore: field.requiredBoolean(map, "has_more", context)
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
  const dialect = decodeOptionalWord(map, "dialect", context, ["postgres", "my_sql", "sqlite"])
  return {
    name: field.requiredString(map, "name", context),
    version: field.requiredString(map, "version", context),
    ...(dialect === undefined ? {} : { dialect })
  }
}
function encodeResolvedTarget(value: ResolvedQueryTarget): Map<string, unknown> {
  const map = mapOf([
    ["kind", value.kind],
    ["index", value.index]
  ])
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
  map.set("truncated", value.truncated)
  map.set("elapsed_micros", value.elapsedMicros)
  map.set("scanned_bytes", value.scannedBytes)
  map.set("produced_bytes", value.producedBytes)
  map.set("row_count", value.rowCount)
  if (value.frontier.length > 0)
    map.set(
      "frontier",
      value.frontier.map((entry) => {
        const item = mapOf([
          ["stream", entry.stream],
          ["topic", entry.topic],
          ["partition_id", entry.partitionId]
        ])
        optional(item, "topic_created_at_micros", entry.topicCreatedAtMicros)
        item.set("applied_offset", entry.appliedOffset)
        return item
      })
    )
  return map
}
function decodeContext(map: CborMap, context: string): QueryContext {
  return {
    executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context)),
    engine: decodeEngine(field.requiredMap(map, "engine", context), `${context}.engine`),
    resolvedTarget: decodeResolvedTarget(
      field.requiredMap(map, "resolved_target", context),
      `${context}.resolved_target`
    ),
    requestedConsistency: decodeWord(map, "requested_consistency", context, [
      "eventual",
      "read_your_writes"
    ]),
    deliveredConsistency: decodeWord(map, "delivered_consistency", context, [
      "eventual",
      "read_your_writes"
    ]),
    truncated: field.requiredBoolean(map, "truncated", context),
    elapsedMicros: field.requiredU64(map, "elapsed_micros", context),
    scannedBytes: field.requiredU64(map, "scanned_bytes", context),
    producedBytes: field.requiredU64(map, "produced_bytes", context),
    rowCount: field.requiredU64(map, "row_count", context),
    frontier: field.optionalArray(map, "frontier", context, (item, index) => {
      const entryContext = `${context}.frontier[${String(index)}]`
      const entry = expectMap(item, entryContext)
      const topicCreatedAtMicros = field.optionalU64(entry, "topic_created_at_micros", entryContext)
      return {
        stream: field.requiredString(entry, "stream", entryContext),
        topic: field.requiredString(entry, "topic", entryContext),
        partitionId: field.requiredU32(entry, "partition_id", entryContext),
        ...(topicCreatedAtMicros === undefined ? {} : { topicCreatedAtMicros }),
        appliedOffset: field.requiredU64(entry, "applied_offset", entryContext)
      }
    })
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
      typedValueValidateAgainst(value, field.fieldType, field.required)
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
}

function validateQueryContext(context: QueryContext): void {
  if (context.executionId.asU128() === 0n)
    throw new InvalidError("query context execution id must be nonzero")
  validateName(context.engine.name)
  validateName(context.engine.version)
  const consistencyRank: Record<Consistency, number> = {
    eventual: 0,
    read_your_writes: 1
  }
  if (consistencyRank[context.deliveredConsistency] < consistencyRank[context.requestedConsistency])
    throw new InvalidError("delivered consistency is weaker than requested consistency")
  const target = context.resolvedTarget
  validateName(target.index)
  if (
    target.backendResourceId.asU128() === 0n ||
    target.backendGeneration === 0n ||
    target.runtimeConfigurationRevision === 0n
  )
    throw new InvalidError("operational query context is missing backend evidence")
}
export function queryResultFieldIndex(result: QueryResult, name: string): number | undefined {
  const index = result.fields.findIndex((item) => item.name === name)
  return index < 0 ? undefined : index
}

export function queryResultValue(
  result: QueryResult,
  row: Row,
  name: string
): TypedValue | undefined {
  const index = queryResultFieldIndex(result, name)
  return index === undefined ? undefined : row.values[index]
}

export function queryResultValueText(
  result: QueryResult,
  row: Row,
  name: string
): string | undefined {
  const value = queryResultValue(result, row, name)
  return value === undefined ? undefined : typedValueDiagnosticText(value)
}

export function queryResultValueU64(
  result: QueryResult,
  row: Row,
  name: string
): bigint | undefined {
  const value = queryResultValue(result, row, name)
  return value === undefined ? undefined : typedValueAsU64(value)
}

export function queryResultValueI64(
  result: QueryResult,
  row: Row,
  name: string
): bigint | undefined {
  const value = queryResultValue(result, row, name)
  return value === undefined ? undefined : typedValueAsI64(value)
}

export type QueryError =
  | {
      readonly kind:
        | "unsupported"
        | "invalid"
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
  | { readonly kind: "schema_not_found"; readonly id: number }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | {
      readonly kind: "stale"
      readonly what: string
      readonly applied: bigint
      readonly required: bigint
    }
  | { readonly kind: "deadline_exceeded"; readonly executionId: QueryExecutionId }
  | {
      readonly kind: "stale_generation"
      readonly what: string
      readonly requested: bigint
      readonly observed: bigint
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
/** The error text the Rust client shows for `error`. */
export function describeQueryError(error: QueryError): string {
  switch (error.kind) {
    case "unsupported":
      return `query not supported: ${error.message}`
    case "invalid":
      return `invalid query: ${error.message}`
    case "unauthorized":
      return `unauthorized: ${error.message}`
    case "index_not_found":
      return `index not found: ${error.message}`
    case "fork_not_found":
      return `fork not found: ${error.message}`
    case "schema_not_found":
      return `writer schema ${String(error.id)} is not registered`
    case "backend":
      return `backend error: ${error.message}`
    case "unavailable":
      return `temporarily unavailable: ${error.message}`
    case "too_large":
      return `result too large: ${error.what} ${String(error.size)} exceeds cap ${String(error.cap)}`
    case "version":
      return `unsupported envelope version (expected ${String(error.expected)}, got ${String(error.got)})`
    case "stale":
      return `stale read: ${error.what} applied ${String(error.applied)}, required ${String(error.required)}`
    case "deadline_exceeded":
      return `query ${error.executionId.toString()} exceeded its deadline`
    case "stale_generation":
      return `stale ${error.what} generation: requested ${String(error.requested)}, observed ${String(error.observed)}`
  }
}

export function encodeQueryError(error: QueryError): Map<string, unknown> {
  const tag = snakeToRustVariant(error.kind)
  if ("message" in error) return mapOf([[tag, error.message]])
  if (error.kind === "schema_not_found") return mapOf([[tag, BigInt(error.id)]])
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
      "invalid",
      "unauthorized",
      "index_not_found",
      "fork_not_found",
      "backend",
      "unavailable"
    ].includes(kind)
  )
    return { kind: kind as "unsupported", message: expectString(body, context) }
  if (kind === "schema_not_found") return { kind, id: expectU32(body, context) }
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
    case "deadline_exceeded":
      return {
        kind,
        executionId: QueryExecutionId.fromBytes(field.requiredBytes(map, "execution_id", context))
      }
    case "stale_generation":
      return {
        kind,
        what: field.requiredString(map, "what", context),
        requested: field.requiredU64(map, "requested", context),
        observed: field.requiredU64(map, "observed", context)
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
  if (value === "eventual" || value === "read_your_writes") return value
  throw new CodecError(`unknown consistency \`${value}\``, context, "consistency")
}
export function consistencyToWord(value: Consistency): string {
  return value
}
const U64_MAX = (1n << 64n) - 1n

export function pageAtLeast(page: Page, rowsOnPage: number): bigint | undefined {
  if (page.offset === undefined) return undefined
  const rows = page.offset + BigInt(rowsOnPage)
  return rows > U64_MAX ? undefined : rows
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
