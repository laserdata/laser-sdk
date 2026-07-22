import { CodecError } from "../client/errors.js"
import {
  type CborMap,
  encodeNamed,
  expectMap,
  expectString,
  field,
  singleVariantTag
} from "./cbor.js"
import { QUERY_OP_VERSION } from "./codes.js"
import { decodeValue, encodeValue, type Value } from "./value.js"

// One exact-match constraint: the indexed `field` must equal `value`.
export interface KeyMatch {
  readonly field: string
  readonly value: string
}

export function encodeKeyMatch(match: KeyMatch): Map<string, unknown> {
  return new Map<string, unknown>([
    ["field", match.field],
    ["value", match.value]
  ])
}

export function decodeKeyMatch(map: CborMap, context: string): KeyMatch {
  return {
    field: field.requiredString(map, "field", context),
    value: field.requiredString(map, "value", context)
  }
}

// A comparison operator for a `Predicate`. Rust carries no serde catch-all
// for this enum, so an unrecognized word is a decode failure, not a value.
export type CmpOp = "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "contains" | "prefix"

const CMP_OPS: ReadonlySet<string> = new Set([
  "eq",
  "ne",
  "lt",
  "lte",
  "gt",
  "gte",
  "in",
  "contains",
  "prefix"
])

function parseCmpOp(word: string, context: string): CmpOp {
  if (!CMP_OPS.has(word)) {
    throw new CodecError(`\`${word}\` is not a recognized comparison operator`, context, "op")
  }
  return word as CmpOp
}

// A filter leaf: a field, a comparison op, and a value.
export interface Predicate {
  readonly field: string
  readonly op: CmpOp
  readonly value: Value
}

export function encodePredicate(predicate: Predicate): Map<string, unknown> {
  return new Map<string, unknown>([
    ["field", predicate.field],
    ["op", predicate.op],
    ["value", encodeValue(predicate.value)]
  ])
}

export function decodePredicate(map: CborMap, context: string): Predicate {
  return {
    field: field.requiredString(map, "field", context),
    op: parseCmpOp(field.requiredString(map, "op", context), context),
    value: decodeValue(map.get("value"), `${context}.value`)
  }
}

// A predicate tree. `all`/`any` are n-ary, `not` negates, `pred` is a single
// comparison leaf. Externally tagged on the wire with snake_case tags (this
// enum, unlike the additive request/reply enums elsewhere, carries
// `#[serde(rename_all = "snake_case")]` and no catch-all, so an
// unrecognized tag is a decode failure).
export type Filter =
  | { readonly kind: "all"; readonly filters: readonly Filter[] }
  | { readonly kind: "any"; readonly filters: readonly Filter[] }
  | { readonly kind: "not"; readonly filter: Filter }
  | { readonly kind: "pred"; readonly predicate: Predicate }

export function filterAll(filters: readonly Filter[]): Filter {
  return { kind: "all", filters }
}

export function filterAny(filters: readonly Filter[]): Filter {
  return { kind: "any", filters }
}

export function filterNegate(filter: Filter): Filter {
  return { kind: "not", filter }
}

export function filterPred(fieldName: string, op: CmpOp, value: Value): Filter {
  return { kind: "pred", predicate: { field: fieldName, op, value } }
}

export function encodeFilter(filter: Filter): Map<string, unknown> {
  switch (filter.kind) {
    case "all":
      return new Map([["all", filter.filters.map((child) => encodeFilter(child))]])
    case "any":
      return new Map([["any", filter.filters.map((child) => encodeFilter(child))]])
    case "not":
      return new Map([["not", encodeFilter(filter.filter)]])
    case "pred":
      return new Map([["pred", encodePredicate(filter.predicate)]])
  }
}

export function decodeFilter(value: unknown, context: string): Filter {
  const map = expectMap(value, context)
  if (map.size !== 1) {
    throw new CodecError(`expected exactly one filter tag in ${context}`, context, "filter")
  }
  const [first] = Array.from(map.entries())
  if (first === undefined) {
    throw new CodecError(`expected exactly one filter tag in ${context}`, context, "filter")
  }
  const [tag, inner] = first
  switch (tag) {
    case "all":
      return { kind: "all", filters: expectFilterArray(inner, context) }
    case "any":
      return { kind: "any", filters: expectFilterArray(inner, context) }
    case "not":
      return { kind: "not", filter: decodeFilter(inner, context) }
    case "pred":
      return { kind: "pred", predicate: decodePredicate(expectMap(inner, context), context) }
    default:
      throw new CodecError(`\`${String(tag)}\` is not a recognized filter tag`, context, "filter")
  }
}

function expectFilterArray(value: unknown, context: string): Filter[] {
  if (!Array.isArray(value)) {
    throw new CodecError(`expected an array in ${context}`, context, "filter")
  }
  return value.map((item, index) => decodeFilter(item, `${context}[${String(index)}]`))
}

// Raw-SQL escape hatch. `sql` must be a single read-only SELECT. `params`
// bind positionally. SQL backends only.
export interface RawSql {
  readonly sql: string
  readonly params: readonly Value[]
}

export function encodeRawSql(rawSql: RawSql): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("sql", rawSql.sql)
  if (rawSql.params.length > 0)
    map.set(
      "params",
      rawSql.params.map((value) => encodeValue(value))
    )
  return map
}

export function decodeRawSql(map: CborMap, context: string): RawSql {
  return {
    sql: field.requiredString(map, "sql", context),
    params: field.optionalArray(map, "params", context, (item, index) =>
      decodeValue(item, `${context}.params[${String(index)}]`)
    )
  }
}

// Sort direction. Rust carries no serde catch-all for this enum.
export type Dir = "asc" | "desc"

function parseDir(word: string, context: string): Dir {
  if (word !== "asc" && word !== "desc") {
    throw new CodecError(`\`${word}\` is not a recognized sort direction`, context, "dir")
  }
  return word
}

// An order-by clause: a field and a direction. `dir` is always present on
// the wire (Rust has `#[serde(default)]` but no `skip_serializing_if` on
// this field), unlike most other defaulted fields in this module.
export interface Sort {
  readonly field: string
  readonly dir: Dir
}

export function encodeSort(sort: Sort): Map<string, unknown> {
  return new Map<string, unknown>([
    ["field", sort.field],
    ["dir", sort.dir]
  ])
}

export function decodeSort(map: CborMap, context: string): Sort {
  const dir = field.optionalString(map, "dir", context)
  return {
    field: field.requiredString(map, "field", context),
    dir: dir !== undefined ? parseDir(dir, context) : "asc"
  }
}

// A lexical relevance search: the text to match and, optionally, the one
// indexed field to match it in (absent searches every text-hinted field).
export interface TextQuery {
  readonly field?: string
  readonly query: string
}

export function encodeTextQuery(text: TextQuery): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (text.field !== undefined) map.set("field", text.field)
  map.set("query", text.query)
  return map
}

export function decodeTextQuery(map: CborMap, context: string): TextQuery {
  const textField = field.optionalString(map, "field", context)
  return {
    ...(textField !== undefined ? { field: textField } : {}),
    query: field.requiredString(map, "query", context)
  }
}

// A nearest-neighbour search: the query embedding and how many rows to
// return.
//
// Known gap, latent not failing: `embedding` is `Vec<f32>` in Rust and
// would need the same `forceFloatNumbers` treatment `graph.ts` gives
// `GraphNode.embedding` if a whole-number embedding value and this query's
// other plain-`number` fields (`topK`) ever needed to coexist byte-
// identically in the same encode call. No current fixture's embedding
// holds a whole number, so this is undetected today. Fix it the way
// `graph.ts`'s `encodeGraphNodeFrame` does, the first time it matters.
export interface VectorQuery {
  readonly field: string
  readonly embedding: readonly number[]
  readonly topK: number
}

export function encodeVectorQuery(vector: VectorQuery): Map<string, unknown> {
  return new Map<string, unknown>([
    ["field", vector.field],
    ["embedding", [...vector.embedding]],
    ["top_k", BigInt(vector.topK)]
  ])
}

export function decodeVectorQuery(map: CborMap, context: string): VectorQuery {
  return {
    field: field.requiredString(map, "field", context),
    embedding: field.requiredArray(map, "embedding", context, (item, index) =>
      expectNumber(item, `${context}.embedding[${String(index)}]`)
    ),
    topK: field.requiredU32(map, "top_k", context)
  }
}

function expectNumber(value: unknown, context: string): number {
  if (typeof value !== "number") {
    throw new CodecError(`expected a number in ${context}`, context, "value")
  }
  return value
}

// An aggregate function. `percentile`/`stdDev` are backend-gated. Rust
// carries no serde catch-all for this enum.
export type AggFunc =
  "count" | "countDistinct" | "sum" | "avg" | "min" | "max" | "percentile" | "stdDev"

const AGG_FUNC_WORDS: ReadonlyMap<string, AggFunc> = new Map([
  ["count", "count"],
  ["count_distinct", "countDistinct"],
  ["sum", "sum"],
  ["avg", "avg"],
  ["min", "min"],
  ["max", "max"],
  ["percentile", "percentile"],
  ["std_dev", "stdDev"]
])
const AGG_FUNC_TO_WORD: ReadonlyMap<AggFunc, string> = new Map(
  Array.from(AGG_FUNC_WORDS.entries(), ([word, value]) => [value, word])
)

function parseAggFunc(word: string, context: string): AggFunc {
  const value = AGG_FUNC_WORDS.get(word)
  if (value === undefined) {
    throw new CodecError(`\`${word}\` is not a recognized aggregate function`, context, "func")
  }
  return value
}

function aggFuncToWord(value: AggFunc): string {
  const word = AGG_FUNC_TO_WORD.get(value)
  if (word === undefined) {
    throw new CodecError(`\`${value}\` is not a recognized aggregate function`, "AggFunc", "func")
  }
  return word
}

// One aggregate in an `Aggregate`. `field` is absent only for `count`, and
// `arg` is the fraction for `percentile` (e.g. 0.95).
export interface AggCall {
  readonly func: AggFunc
  readonly field?: string
  readonly arg?: number
  readonly alias: string
}

export function encodeAggCall(call: AggCall): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("func", aggFuncToWord(call.func))
  if (call.field !== undefined) map.set("field", call.field)
  if (call.arg !== undefined) map.set("arg", call.arg)
  map.set("alias", call.alias)
  return map
}

export function decodeAggCall(map: CborMap, context: string): AggCall {
  const callField = field.optionalString(map, "field", context)
  const arg = field.optionalF64(map, "arg", context)
  return {
    func: parseAggFunc(field.requiredString(map, "func", context), context),
    ...(callField !== undefined ? { field: callField } : {}),
    ...(arg !== undefined ? { arg } : {}),
    alias: field.requiredString(map, "alias", context)
  }
}

// A tumbling window of `everyMicros` over the timestamp `field`.
export interface Window {
  readonly field: string
  readonly everyMicros: bigint
}

export function encodeWindow(window: Window): Map<string, unknown> {
  return new Map<string, unknown>([
    ["field", window.field],
    ["every_micros", window.everyMicros]
  ])
}

export function decodeWindow(map: CborMap, context: string): Window {
  return {
    field: field.requiredString(map, "field", context),
    everyMicros: field.requiredU64(map, "every_micros", context)
  }
}

// A grouped aggregation carrying one or more `AggCall`s, so a single query
// can return several aggregates grouped by the same keys. An optional
// `window` adds a time-bucket key.
export interface Aggregate {
  readonly groupBy: readonly string[]
  readonly funcs: readonly AggCall[]
  readonly window?: Window
}

export function encodeAggregate(aggregate: Aggregate): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (aggregate.groupBy.length > 0) map.set("group_by", [...aggregate.groupBy])
  map.set(
    "funcs",
    aggregate.funcs.map((call) => encodeAggCall(call))
  )
  if (aggregate.window !== undefined) map.set("window", encodeWindow(aggregate.window))
  return map
}

export function decodeAggregate(map: CborMap, context: string): Aggregate {
  const windowMap = field.optionalMap(map, "window", context)
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
    ...(windowMap !== undefined ? { window: decodeWindow(windowMap, `${context}.window`) } : {})
  }
}

// Which columns and payload a query returns. `payload` is always present
// on the wire (Rust has `#[serde(default)]` but no `skip_serializing_if`
// on this field).
export interface Select {
  readonly fields: readonly string[]
  readonly payload: boolean
}

export function encodeSelect(select: Select): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (select.fields.length > 0) map.set("fields", [...select.fields])
  map.set("payload", select.payload)
  return map
}

export function decodeSelect(map: CborMap, context: string): Select {
  return {
    fields: field.optionalArray(map, "fields", context, (item, index) =>
      expectString(item, `${context}.fields[${String(index)}]`)
    ),
    payload: field.optionalBoolean(map, "payload", context) ?? false
  }
}

// How fresh a query's view of the materialized index must be. Rust marks
// this `#[non_exhaustive]` but carries no `#[serde(other)]` catch-all, so
// an unrecognized word is a decode failure, not a value (same class as
// `AgentRunState`, not `Feature`/`Action`).
export type Consistency = "eventual" | "readYourWrites" | "strong"

export function parseConsistency(word: string, context: string): Consistency {
  switch (word) {
    case "eventual":
      return "eventual"
    case "read_your_writes":
      return "readYourWrites"
    case "strong":
      return "strong"
    default:
      throw new CodecError(
        `\`${word}\` is not a recognized consistency level`,
        context,
        "consistency"
      )
  }
}

export function consistencyToWord(value: Consistency): string {
  switch (value) {
    case "eventual":
      return "eventual"
    case "readYourWrites":
      return "read_your_writes"
    case "strong":
      return "strong"
  }
}

// A query against a materialized index. `limit`/`offset` are always
// present on the wire; every other field is omitted at its default.
export interface Query {
  readonly index: string
  readonly byKey: readonly KeyMatch[]
  readonly messageType?: string
  readonly timeRange?: readonly [bigint, bigint]
  readonly filter?: Filter
  readonly vector?: VectorQuery
  readonly text?: TextQuery
  readonly order: readonly Sort[]
  readonly limit: number
  readonly offset: number
  readonly aggregate?: Aggregate
  readonly having?: Filter
  readonly distinct: boolean
  readonly select: Select
  readonly fork?: string
  readonly rawSql?: RawSql
  readonly consistency: Consistency
  readonly wantTotal: boolean
}

export function encodeQuery(query: Query): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("index", query.index)
  if (query.byKey.length > 0)
    map.set(
      "by_key",
      query.byKey.map((match) => encodeKeyMatch(match))
    )
  if (query.messageType !== undefined) map.set("message_type", query.messageType)
  if (query.timeRange !== undefined) map.set("time_range", [...query.timeRange])
  if (query.filter !== undefined) map.set("filter", encodeFilter(query.filter))
  if (query.vector !== undefined) map.set("vector", encodeVectorQuery(query.vector))
  if (query.text !== undefined) map.set("text", encodeTextQuery(query.text))
  if (query.order.length > 0)
    map.set(
      "order",
      query.order.map((sort) => encodeSort(sort))
    )
  map.set("limit", BigInt(query.limit))
  map.set("offset", BigInt(query.offset))
  if (query.aggregate !== undefined) map.set("aggregate", encodeAggregate(query.aggregate))
  if (query.having !== undefined) map.set("having", encodeFilter(query.having))
  if (query.distinct) map.set("distinct", true)
  map.set("select", encodeSelect(query.select))
  if (query.fork !== undefined) map.set("fork", query.fork)
  if (query.rawSql !== undefined) map.set("raw_sql", encodeRawSql(query.rawSql))
  if (query.consistency !== "eventual") map.set("consistency", consistencyToWord(query.consistency))
  if (query.wantTotal) map.set("want_total", true)
  return map
}

export function decodeQuery(map: CborMap, context: string): Query {
  const messageType = field.optionalString(map, "message_type", context)
  const timeRangeRaw = map.get("time_range")
  const filterRaw = map.get("filter")
  const vectorMap = field.optionalMap(map, "vector", context)
  const textMap = field.optionalMap(map, "text", context)
  const aggregateMap = field.optionalMap(map, "aggregate", context)
  const havingRaw = map.get("having")
  const fork = field.optionalString(map, "fork", context)
  const rawSqlMap = field.optionalMap(map, "raw_sql", context)
  const consistencyWord = field.optionalString(map, "consistency", context)
  const selectMap = field.optionalMap(map, "select", context)
  return {
    index: field.requiredString(map, "index", context),
    byKey: field.optionalArray(map, "by_key", context, (item, index) =>
      decodeKeyMatch(
        expectMap(item, `${context}.by_key[${String(index)}]`),
        `${context}.by_key[${String(index)}]`
      )
    ),
    ...(messageType !== undefined ? { messageType } : {}),
    ...(timeRangeRaw !== undefined ? { timeRange: expectU64Pair(timeRangeRaw, context) } : {}),
    ...(filterRaw !== undefined ? { filter: decodeFilter(filterRaw, `${context}.filter`) } : {}),
    ...(vectorMap !== undefined
      ? { vector: decodeVectorQuery(vectorMap, `${context}.vector`) }
      : {}),
    ...(textMap !== undefined ? { text: decodeTextQuery(textMap, `${context}.text`) } : {}),
    order: field.optionalArray(map, "order", context, (item, index) =>
      decodeSort(
        expectMap(item, `${context}.order[${String(index)}]`),
        `${context}.order[${String(index)}]`
      )
    ),
    limit: field.requiredU32(map, "limit", context),
    offset: field.optionalU32(map, "offset", context) ?? 0,
    ...(aggregateMap !== undefined
      ? { aggregate: decodeAggregate(aggregateMap, `${context}.aggregate`) }
      : {}),
    ...(havingRaw !== undefined ? { having: decodeFilter(havingRaw, `${context}.having`) } : {}),
    distinct: field.optionalBoolean(map, "distinct", context) ?? false,
    select:
      selectMap !== undefined
        ? decodeSelect(selectMap, `${context}.select`)
        : { fields: [], payload: false },
    ...(fork !== undefined ? { fork } : {}),
    ...(rawSqlMap !== undefined ? { rawSql: decodeRawSql(rawSqlMap, `${context}.raw_sql`) } : {}),
    consistency:
      consistencyWord !== undefined ? parseConsistency(consistencyWord, context) : "eventual",
    wantTotal: field.optionalBoolean(map, "want_total", context) ?? false
  }
}

function expectU64Pair(value: unknown, context: string): readonly [bigint, bigint] {
  if (!Array.isArray(value) || value.length !== 2) {
    throw new CodecError(
      `expected a 2-element array in ${context}.time_range`,
      context,
      "time_range"
    )
  }
  const [start, end] = value as [unknown, unknown]
  return [expectU64Value(start, context), expectU64Value(end, context)]
}

function expectU64Value(value: unknown, context: string): bigint {
  if (typeof value === "bigint") return value
  if (typeof value === "number" && Number.isInteger(value) && value >= 0) return BigInt(value)
  throw new CodecError(
    `expected an unsigned integer in ${context}.time_range`,
    context,
    "time_range"
  )
}

// Internal on-wire envelope: a versioned wrapper around `Query`.
export interface QueryEnvelope {
  readonly query: Query
}

export function encodeQueryEnvelope(envelope: QueryEnvelope): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(QUERY_OP_VERSION)],
    ["query", encodeQuery(envelope.query)]
  ])
}

export function decodeQueryEnvelope(map: CborMap, context: string): QueryEnvelope {
  field.requiredU32(map, "v", context)
  return { query: decodeQuery(field.requiredMap(map, "query", context), `${context}.query`) }
}

export function encodeQueryEnvelopeFrame(envelope: QueryEnvelope): Uint8Array {
  return encodeNamed(encodeQueryEnvelope(envelope), { forceFloatNumbers: true })
}

// Pagination info for a query result. The default reply costs one page:
// the server fetches `limit + 1` rows and answers `hasMore` exactly from
// the probe row. `total` is present only when the request set `wantTotal`.
export interface Page {
  readonly offset: number
  readonly limit: number
  readonly total?: bigint
  readonly hasMore: boolean
}

export function encodePage(page: Page): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("offset", BigInt(page.offset))
  map.set("limit", BigInt(page.limit))
  if (page.total !== undefined) map.set("total", page.total)
  map.set("has_more", page.hasMore)
  return map
}

export function decodePage(map: CborMap, context: string): Page {
  const total = field.optionalU64(map, "total", context)
  return {
    offset: field.requiredU32(map, "offset", context),
    limit: field.requiredU32(map, "limit", context),
    ...(total !== undefined ? { total } : {}),
    hasMore: field.requiredBoolean(map, "has_more", context)
  }
}

export function pageAtLeast(page: Page, rowsOnPage: number): number {
  return page.offset + rowsOnPage
}

export function pageTotalPages(page: Page): bigint | undefined {
  if (page.total === undefined || page.limit === 0) return undefined
  return (page.total + BigInt(page.limit) - 1n) / BigInt(page.limit)
}

// One materialized row: indexed fields, metadata, log position, and
// optional payload/score.
//
// Known gap, latent not failing: `score` is `Option<f32>` in Rust and
// would need force-float treatment if it ever held a whole number
// alongside this row's other plain-`number` fields in the same encode
// call. No current fixture's score is whole. See `VectorQuery`'s note.
export interface Row {
  readonly headers: ReadonlyMap<string, string>
  readonly metadata: ReadonlyMap<string, string>
  readonly partition?: number
  readonly offset?: bigint
  readonly stream?: number
  readonly topic?: number
  readonly payload?: Uint8Array
  readonly score?: number
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

export function encodeRow(row: Row): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("headers", encodeStringMap(row.headers))
  if (row.metadata.size > 0) map.set("metadata", encodeStringMap(row.metadata))
  if (row.partition !== undefined) map.set("partition", BigInt(row.partition))
  if (row.offset !== undefined) map.set("offset", row.offset)
  if (row.stream !== undefined) map.set("stream", BigInt(row.stream))
  if (row.topic !== undefined) map.set("topic", BigInt(row.topic))
  if (row.payload !== undefined) map.set("payload", row.payload)
  if (row.score !== undefined) map.set("score", row.score)
  return map
}

export function decodeRow(map: CborMap, context: string): Row {
  const metadataMap = field.optionalMap(map, "metadata", context)
  const partition = field.optionalU32(map, "partition", context)
  const offset = field.optionalU64(map, "offset", context)
  const stream = field.optionalU32(map, "stream", context)
  const topic = field.optionalU32(map, "topic", context)
  const payload = field.optionalBytes(map, "payload", context)
  const score = field.optionalF64(map, "score", context)
  return {
    headers: decodeStringMap(field.requiredMap(map, "headers", context), `${context}.headers`),
    metadata:
      metadataMap !== undefined ? decodeStringMap(metadataMap, `${context}.metadata`) : new Map(),
    ...(partition !== undefined ? { partition } : {}),
    ...(offset !== undefined ? { offset } : {}),
    ...(stream !== undefined ? { stream } : {}),
    ...(topic !== undefined ? { topic } : {}),
    ...(payload !== undefined ? { payload } : {}),
    ...(score !== undefined ? { score } : {})
  }
}

// A page of result rows plus pagination info.
export interface QueryResult {
  readonly rows: readonly Row[]
  readonly page: Page
}

export function encodeQueryResult(result: QueryResult): Map<string, unknown> {
  return new Map<string, unknown>([
    ["rows", result.rows.map((row) => encodeRow(row))],
    ["page", encodePage(result.page)]
  ])
}

export function decodeQueryResult(map: CborMap, context: string): QueryResult {
  return {
    rows: field.requiredArray(map, "rows", context, (item, index) =>
      decodeRow(
        expectMap(item, `${context}.rows[${String(index)}]`),
        `${context}.rows[${String(index)}]`
      )
    ),
    page: decodePage(field.requiredMap(map, "page", context), `${context}.page`)
  }
}

// Why a query failed. Additive: an unrecognized variant decodes rather
// than throws.
export type QueryError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "unauthorized"; readonly message: string }
  | { readonly kind: "indexNotFound"; readonly message: string }
  | { readonly kind: "forkNotFound"; readonly message: string }
  | { readonly kind: "backend"; readonly message: string }
  | {
      readonly kind: "tooLarge"
      readonly what: string
      readonly size: number
      readonly cap: number
    }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | {
      readonly kind: "stale"
      readonly what: string
      readonly applied: bigint
      readonly required: bigint
    }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeQueryError(error: QueryError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "unauthorized":
      return new Map([["Unauthorized", error.message]])
    case "indexNotFound":
      return new Map([["IndexNotFound", error.message]])
    case "forkNotFound":
      return new Map([["ForkNotFound", error.message]])
    case "backend":
      return new Map([["Backend", error.message]])
    case "tooLarge":
      return new Map([
        [
          "TooLarge",
          new Map<string, unknown>([
            ["what", error.what],
            ["size", BigInt(error.size)],
            ["cap", BigInt(error.cap)]
          ])
        ]
      ])
    case "version":
      return new Map([
        [
          "Version",
          new Map<string, unknown>([
            ["expected", BigInt(error.expected)],
            ["got", BigInt(error.got)]
          ])
        ]
      ])
    case "stale":
      return new Map([
        [
          "Stale",
          new Map<string, unknown>([
            ["what", error.what],
            ["applied", error.applied],
            ["required", error.required]
          ])
        ]
      ])
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

export function decodeQueryError(value: unknown, context: string): QueryError {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "Unauthorized":
      return { kind: "unauthorized", message: expectString(inner, context) }
    case "IndexNotFound":
      return { kind: "indexNotFound", message: expectString(inner, context) }
    case "ForkNotFound":
      return { kind: "forkNotFound", message: expectString(inner, context) }
    case "Backend":
      return { kind: "backend", message: expectString(inner, context) }
    case "TooLarge": {
      const tooLargeMap = expectMap(inner, context)
      return {
        kind: "tooLarge",
        what: field.requiredString(tooLargeMap, "what", context),
        size: field.requiredU32(tooLargeMap, "size", context),
        cap: field.requiredU32(tooLargeMap, "cap", context)
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
    case "Stale": {
      const staleMap = expectMap(inner, context)
      return {
        kind: "stale",
        what: field.requiredString(staleMap, "what", context),
        applied: field.requiredU64(staleMap, "applied", context),
        required: field.requiredU64(staleMap, "required", context)
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

// A query reply: `ok(QueryResult)` or `err(QueryError)`.
export type QueryReply =
  | { readonly kind: "ok"; readonly result: QueryResult }
  | { readonly kind: "err"; readonly error: QueryError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeQueryReply(reply: QueryReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeQueryResult(reply.result)]])
    case "err":
      return new Map([["Err", encodeQueryError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeQueryReply(value: unknown, context: string): QueryReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", result: decodeQueryResult(expectMap(inner, context), context) }
    case "Err":
      return { kind: "err", error: decodeQueryError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeQueryReplyFrame(reply: QueryReply): Uint8Array {
  return encodeNamed(encodeQueryReply(reply), { forceFloatNumbers: true })
}

// The offset obligation a `Consistency` level enforces: whether the
// projector has caught up to the required offset. Fail-not-downgrade: a
// non-`eventual` level that has not caught up is a `stale` error, never a
// silently older read.
export interface ConsistencyGate {
  readonly applied: bigint
  readonly required: bigint
}

export function consistencyGateIsCaughtUp(gate: ConsistencyGate): boolean {
  return gate.applied >= gate.required
}

export function consistencyGateCheck(
  gate: ConsistencyGate,
  level: Consistency,
  what: string
): { readonly ok: true } | { readonly ok: false; readonly error: QueryError } {
  if (level === "eventual" || consistencyGateIsCaughtUp(gate)) {
    return { ok: true }
  }
  return {
    ok: false,
    error: { kind: "stale", what, applied: gate.applied, required: gate.required }
  }
}
