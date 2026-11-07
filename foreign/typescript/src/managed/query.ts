import { QUERY_DEADLINE_MS, QUERY_REPLY_GRACE_MS } from "../retry.js"
import { type CallOptions, abortable, throwIfAborted } from "../client/call-options.js"
import { millis, millisToMicros } from "../client/duration.js"
import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"
import { ConfigError, InvalidError, ProtocolError } from "../client/errors.js"
import { saturatingAdd } from "../runtime/clock.js"
import { mintUlidValue } from "../runtime/ulid.js"
import type { Codec } from "../stream/codecs.js"
import { CONVERSATION_FIELD, VECTOR_FIELD } from "../wire/headers.js"
import { QueryExecutionId } from "../wire/ids.js"
import {
  type AggCall,
  type AggFunc,
  type Aggregate,
  type CmpOp,
  type Consistency,
  type Filter,
  type Query,
  type QueryFrontier,
  type QueryResult,
  type QueryTarget,
  type Row,
  type SqlDialect,
  filterAll,
  filterPred,
  newQuery,
  operationalTarget
} from "../wire/query.js"
import {
  type LogicalField,
  type TypedValue,
  typedValueAsI64,
  typedValueAsU64,
  typedValueDiagnosticText
} from "../wire/schema.js"

const ORIGINAL_PAYLOAD_FIELD = "__laser_original_payload"
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n

export type QueryExecutor = (query: Query, options?: CallOptions) => Promise<QueryResult>

/** A filter or parameter value: a tagged value, or a native one whose tag is
 * inferred like Rust and Python. A safe integer or a bigint is a long, another
 * finite number a double. */
export type QueryValue = TypedValue | string | number | bigint | boolean | null | Uint8Array

/** One query row bound to the fields of the page it came on, read by field name. */
export class QueryRow {
  private constructor(
    readonly fields: readonly LogicalField[],
    readonly row: Row
  ) {}

  /** Every row of one page, each bound to the page's fields. */
  static fromPage(fields: readonly LogicalField[], rows: readonly Row[]): QueryRow[] {
    return rows.map((row) => new QueryRow(fields, row))
  }

  /** Tagged values in field order. */
  get values(): readonly TypedValue[] {
    return this.row.values
  }

  /** Backend-native vector distance or lexical rank. */
  get score(): number | undefined {
    return this.row.score
  }

  fieldIndex(name: string): number | undefined {
    const index = this.fields.findIndex((field) => field.name === name)
    return index < 0 ? undefined : index
  }

  value(name: string): TypedValue | undefined {
    const index = this.fieldIndex(name)
    return index === undefined ? undefined : this.row.values[index]
  }

  valueText(name: string): string | undefined {
    const value = this.value(name)
    return value === undefined ? undefined : typedValueDiagnosticText(value)
  }

  valueU64(name: string): bigint | undefined {
    const value = this.value(name)
    return value === undefined ? undefined : typedValueAsU64(value)
  }

  valueI64(name: string): bigint | undefined {
    const value = this.value(name)
    return value === undefined ? undefined : typedValueAsI64(value)
  }
}

/** Every row a full walk read, and whether a later page was served at another
 * frontier than the first, so rows may repeat or be missing. */
export class QueryRowSet implements Iterable<QueryRow> {
  private constructor(
    readonly rows: readonly QueryRow[],
    readonly frontierMoved: boolean
  ) {}

  /** @internal */
  static create(rows: readonly QueryRow[], frontierMoved: boolean): QueryRowSet {
    return new QueryRowSet(rows, frontierMoved)
  }

  [Symbol.iterator](): Iterator<QueryRow> {
    return this.rows[Symbol.iterator]()
  }
}

/** The bounded row walk of `rows()`. `frontierMoved` turns true once a later
 * page was served at another frontier than the first. */
export class QueryRows implements AsyncIterable<QueryRow> {
  private moved = false
  private readonly walk: AsyncIterator<QueryRow>

  private constructor(start: (onMoved: () => void) => AsyncIterator<QueryRow>) {
    this.walk = start(() => {
      this.moved = true
    })
  }

  /** @internal */
  static create(start: (onMoved: () => void) => AsyncIterator<QueryRow>): QueryRows {
    return new QueryRows(start)
  }

  get frontierMoved(): boolean {
    return this.moved
  }

  /** The next row, or `undefined` at the ceiling or after the last page. An
   * abort drops the row the walk was reading. */
  async next(options: CallOptions = {}): Promise<QueryRow | undefined> {
    throwIfAborted(options.signal, "query rows")
    const step = await abortable(this.walk.next(), options.signal, "query rows")
    return step.done === true ? undefined : step.value
  }

  [Symbol.asyncIterator](): AsyncIterator<QueryRow> {
    return this.walk
  }
}

/** The bounded typed walk of `rowsTyped()`, reporting a moved frontier like `QueryRows`. */
export class TypedQueryRows<T> implements AsyncIterable<T> {
  private moved = false
  private readonly walk: AsyncIterator<T>

  private constructor(start: (onMoved: () => void) => AsyncIterator<T>) {
    this.walk = start(() => {
      this.moved = true
    })
  }

  /** @internal */
  static create<T>(start: (onMoved: () => void) => AsyncIterator<T>): TypedQueryRows<T> {
    return new TypedQueryRows(start)
  }

  get frontierMoved(): boolean {
    return this.moved
  }

  /** The next decoded value, or `undefined` at the ceiling or after the last
   * page. An abort drops the value the walk was reading. */
  async next(options: CallOptions = {}): Promise<T | undefined> {
    throwIfAborted(options.signal, "query rows")
    const step = await abortable(this.walk.next(), options.signal, "query rows")
    return step.done === true ? undefined : step.value
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.walk
  }
}

/** @internal How long to wait for the reply to a query due at
 * `deadlineMicros`. A deadline further out than `QUERY_DEADLINE_MS` fails. */
export function queryReplyWaitMs(deadlineMicros: bigint, nowMicros: bigint): number {
  const remainingMs = deadlineMicros > nowMicros ? Number(deadlineMicros - nowMicros) / 1000 : 0
  if (remainingMs > QUERY_DEADLINE_MS) {
    throw new InvalidError(
      `the query deadline is ${String(Math.floor(remainingMs / 1000))}s away, the longest allowed is ${String(QUERY_DEADLINE_MS / 1000)}s`
    )
  }
  return remainingMs + QUERY_REPLY_GRACE_MS
}

function emptyQuery(target: QueryTarget): Query {
  return newQuery(
    target,
    QueryExecutionId.fromU128(mintUlidValue()),
    BigInt(Date.now()) * 1000n + BigInt(QUERY_DEADLINE_MS) * 1000n
  )
}

export class QueryRequest {
  private queryValue: Query
  private rowCeiling: number | undefined
  private timeoutMs = QUERY_DEADLINE_MS
  private fixedDeadlineMicros: bigint | undefined
  private readonly execute: QueryExecutor

  private constructor(
    indexOrTarget: string | QueryTarget,
    execute: QueryExecutor,
    private readonly scope: ResourceScope = BARE_SCOPE
  ) {
    this.execute = execute
    this.queryValue = emptyQuery(
      typeof indexOrTarget === "string" ? operationalTarget(indexOrTarget) : indexOrTarget
    )
  }

  /** @internal */
  static create(
    indexOrTarget: string | QueryTarget,
    execute: QueryExecutor,
    scope: ResourceScope = BARE_SCOPE
  ): QueryRequest {
    return new QueryRequest(indexOrTarget, execute, scope)
  }

  /** The identity the next call sends and its reply echoes back. Every
   * call and every page of a walk takes a fresh one. */
  executionId(): QueryExecutionId {
    return this.queryValue.executionId
  }

  /** One absolute deadline, in epoch microseconds, for every later call and
   * every page of a walk. It can be at most 25 seconds away when a call
   * starts, and a later one fails the call. */
  deadlineAtMicros(deadlineAtMicros: bigint): this {
    this.fixedDeadlineMicros = deadlineAtMicros
    return this
  }

  /** The budget of each call and each page in milliseconds, counted from when
   * it starts. Defaults to 25 seconds, the longest allowed. */
  deadline(deadlineMs: number): this {
    if (!Number.isFinite(deadlineMs) || deadlineMs < 0) {
      throw new InvalidError("query deadline must be a non-negative finite number")
    }
    this.timeoutMs = Math.min(deadlineMs, Number.MAX_SAFE_INTEGER / 1000)
    this.fixedDeadlineMicros = undefined
    return this
  }

  whereEq(field: string, value: QueryValue): this {
    this.queryValue = {
      ...this.queryValue,
      byKey: [...this.queryValue.byKey, { field, value: typedValue(value) }]
    }
    return this
  }

  conversation(conversationId: string): this {
    return this.whereEq(CONVERSATION_FIELD, conversationId)
  }

  /** Resolve against a fork's copy-on-write view. The id is sent as the
   * connection's `resourceName` names it. */
  fork(forkId: string): this {
    this.queryValue = { ...this.queryValue, fork: this.scope.name(forkId) }
    return this
  }

  filter(filter: Filter): this {
    const current = this.queryValue.filter
    this.queryValue = {
      ...this.queryValue,
      filter:
        current === undefined
          ? filter
          : current.kind === "all"
            ? filterAll([...current.filters, filter])
            : filterAll([current, filter])
    }
    return this
  }

  private predicate(field: string, op: CmpOp, value: TypedValue): this {
    return this.filter(filterPred(field, op, value))
  }

  filterEq(field: string, value: QueryValue): this {
    return this.predicate(field, "eq", typedValue(value))
  }
  filterNe(field: string, value: QueryValue): this {
    return this.predicate(field, "ne", typedValue(value))
  }
  filterGt(field: string, value: QueryValue): this {
    return this.predicate(field, "gt", typedValue(value))
  }
  filterGte(field: string, value: QueryValue): this {
    return this.predicate(field, "gte", typedValue(value))
  }
  filterLt(field: string, value: QueryValue): this {
    return this.predicate(field, "lt", typedValue(value))
  }
  filterLte(field: string, value: QueryValue): this {
    return this.predicate(field, "lte", typedValue(value))
  }
  filterIn(field: string, values: readonly QueryValue[]): this {
    return this.predicate(field, "in", { kind: "list", value: values.map(typedValue) })
  }
  filterContains(field: string, value: string): this {
    return this.predicate(field, "contains", { kind: "string", value })
  }
  filterPrefix(field: string, value: string): this {
    return this.predicate(field, "prefix", { kind: "string", value })
  }

  messageType(value: string): this {
    this.queryValue = { ...this.queryValue, messageType: value }
    return this
  }
  /** Filter rows whose timestamp (epoch micros) falls in `[startMicros, endMicros]`, both bounds inclusive. */
  timeRange(startMicros: bigint, endMicros: bigint): this {
    this.queryValue = { ...this.queryValue, timeRange: [startMicros, endMicros] }
    return this
  }
  orderAsc(field: string): this {
    this.queryValue = {
      ...this.queryValue,
      order: [...this.queryValue.order, { field, dir: "asc" }]
    }
    return this
  }
  orderDesc(field: string): this {
    this.queryValue = {
      ...this.queryValue,
      order: [...this.queryValue.order, { field, dir: "desc" }]
    }
    return this
  }
  limit(value: number): this {
    this.queryValue = { ...this.queryValue, page: { ...this.queryValue.page, limit: value } }
    return this
  }
  offset(value: bigint | number): this {
    this.queryValue = { ...this.queryValue, page: offsetPage(this.queryValue.page, BigInt(value)) }
    return this
  }
  withPayload(): this {
    this.queryValue = { ...this.queryValue, select: { ...this.queryValue.select, payload: true } }
    return this
  }
  withTotal(): this {
    this.queryValue = { ...this.queryValue, page: { ...this.queryValue.page, wantTotal: true } }
    return this
  }
  consistency(level: Consistency): this {
    this.queryValue = { ...this.queryValue, consistency: level }
    return this
  }
  readYourWrites(): this {
    return this.consistency("read_your_writes")
  }
  /** Relevance search. Rows page by offset and take no order or total. */
  text(query: string): this {
    this.queryValue = { ...this.queryValue, text: { query } }
    return this
  }
  textIn(field: string, query: string): this {
    this.queryValue = { ...this.queryValue, text: { field, query } }
    return this
  }
  /** The `topK` nearest rows as one page with no order, offset or total. */
  nearest(embedding: readonly number[], topK: number): this {
    return this.nearestIn(VECTOR_FIELD, embedding, topK)
  }
  nearestIn(field: string, embedding: readonly number[], topK: number): this {
    this.queryValue = { ...this.queryValue, vector: { field, embedding, topK } }
    return this
  }
  selectFields(fields: readonly string[]): this {
    this.queryValue = {
      ...this.queryValue,
      select: { ...this.queryValue.select, fields: [...fields] }
    }
    return this
  }

  private pushAggregate(call: AggCall): this {
    const current = this.queryValue.aggregate ?? { groupBy: [], funcs: [] }
    this.queryValue = {
      ...this.queryValue,
      aggregate: { ...current, funcs: [...current.funcs, call] }
    }
    return this
  }

  aggAs(
    func: AggFunc,
    alias: string,
    options: { readonly field?: string; readonly fraction?: number } = {}
  ): this {
    return this.pushAggregate({
      func,
      alias,
      ...(options.field === undefined ? {} : { field: options.field }),
      ...(options.fraction === undefined ? {} : { arg: options.fraction })
    })
  }

  count(alias = "count"): this {
    return this.aggAs("count", alias)
  }
  sum(field: string, alias = "sum"): this {
    return this.aggAs("sum", alias, { field })
  }
  avg(field: string, alias = "avg"): this {
    return this.aggAs("avg", alias, { field })
  }
  min(field: string, alias = "min"): this {
    return this.aggAs("min", alias, { field })
  }
  max(field: string, alias = "max"): this {
    return this.aggAs("max", alias, { field })
  }
  countDistinct(field: string, alias = "count_distinct"): this {
    return this.aggAs("count_distinct", alias, { field })
  }
  stddev(field: string, alias = "stddev"): this {
    return this.aggAs("std_dev", alias, { field })
  }
  percentile(field: string, fraction: number, alias = "percentile"): this {
    return this.aggAs("percentile", alias, { field, fraction })
  }

  groupBy(fields: readonly string[]): this {
    const current: Aggregate = this.queryValue.aggregate ?? { groupBy: [], funcs: [] }
    this.queryValue = { ...this.queryValue, aggregate: { ...current, groupBy: [...fields] } }
    return this
  }
  /** Buckets the aggregate into tumbling windows `everyMs` milliseconds wide
   * over the timestamp `field`. */
  window(field: string, everyMs: number): this {
    const everyMicros = millisToMicros(millis(everyMs, "everyMs"))
    const current: Aggregate = this.queryValue.aggregate ?? { groupBy: [], funcs: [] }
    this.queryValue = {
      ...this.queryValue,
      aggregate: { ...current, window: { field, everyMicros } }
    }
    return this
  }
  having(filter: Filter): this {
    this.queryValue = { ...this.queryValue, having: filter }
    return this
  }
  distinct(): this {
    this.queryValue = { ...this.queryValue, distinct: true }
    return this
  }
  rawSql(dialect: SqlDialect, sql: string, params: readonly QueryValue[] = []): this {
    this.queryValue = {
      ...this.queryValue,
      rawSql: { dialect, sql, params: params.map(typedValue) }
    }
    return this
  }
  maxRows(value: number): this {
    if (!Number.isSafeInteger(value) || value < 0)
      throw new InvalidError("query maxRows must be a non-negative safe integer")
    this.rowCeiling = value
    return this
  }
  /** The query the next call sends, with its deadline counted from now. */
  intoQuery(): Query {
    return { ...this.queryValue, deadlineMicros: this.nextDeadlineMicros() }
  }
  async fetch(options: CallOptions = {}): Promise<QueryResult> {
    return this.run(this.queryValue, options)
  }

  async fetchTyped<T>(codec: Codec<T>, options: CallOptions = {}): Promise<readonly T[]> {
    const result = await this.run(withPayload(this.queryValue), options)
    return result.rows.map((row) => decodePayload(result, row, codec))
  }
  async fetchOne<T>(codec: Codec<T>, options: CallOptions = {}): Promise<T | undefined> {
    const query = withPayload(this.queryValue)
    const result = await this.run({ ...query, page: { ...query.page, limit: 1 } }, options)
    const row = result.rows[0]
    return row === undefined ? undefined : decodePayload(result, row, codec)
  }
  /** Every matching row across pages, each bound to its fields, and whether
   * the frontier moved between pages. */
  async fetchAll(options: CallOptions = {}): Promise<QueryRowSet> {
    const rows: QueryRow[] = []
    let moved = false
    const walk = this.pageRows(Number.POSITIVE_INFINITY, options, () => {
      moved = true
    })
    for await (const row of walk) rows.push(row)
    return QueryRowSet.create(rows, moved)
  }
  async fetchAllTyped<T>(codec: Codec<T>, options: CallOptions = {}): Promise<readonly T[]> {
    const pages = await this.fetchAllWithSchemas(withPayload(this.queryValue), options)
    return pages.map(({ result, row }) => decodePayload(result, row, codec))
  }
  /** Walks up to the `maxRows` ceiling, each row bound to its fields. */
  rows(): QueryRows {
    const ceiling = this.rowCeiling
    if (ceiling === undefined)
      throw new InvalidError("rows() needs an explicit ceiling: call maxRows(n) first")
    return QueryRows.create((onMoved) => this.pageRows(ceiling, {}, onMoved))
  }

  /** Streams up to the `maxRows` ceiling, decoding each row's original
   * payload with `codec`. Implies `withPayload()`. */
  rowsTyped<T>(codec: Codec<T>): TypedQueryRows<T> {
    const ceiling = this.rowCeiling
    if (ceiling === undefined)
      throw new InvalidError("rowsTyped() needs an explicit ceiling: call maxRows(n) first")
    const query = withPayload(this.queryValue)
    return TypedQueryRows.create((onMoved) => this.typedRows(codec, ceiling, query, onMoved))
  }

  // Send `query` with the current execution id and a deadline counted from
  // now, then move the request to a fresh id for the next call.
  private async run(query: Query, options: CallOptions = {}): Promise<QueryResult> {
    throwIfAborted(options.signal, "query")
    const sent = {
      ...query,
      executionId: this.queryValue.executionId,
      deadlineMicros: this.nextDeadlineMicros()
    }
    this.queryValue = {
      ...this.queryValue,
      executionId: QueryExecutionId.fromU128(mintUlidValue())
    }
    return requireResultExecution(
      await abortable(this.execute(sent, options), options.signal, "query"),
      sent.executionId
    )
  }

  private nextDeadlineMicros(): bigint {
    return (
      this.fixedDeadlineMicros ??
      saturatingAdd(BigInt(Date.now()) * 1000n, millisToMicros(this.timeoutMs))
    )
  }

  private async *typedRows<T>(
    codec: Codec<T>,
    ceiling: number,
    query: Query,
    onMoved: () => void
  ): AsyncGenerator<T> {
    if (ceiling === 0) return
    let emitted = 0
    for await (const result of this.pages(query, {}, onMoved)) {
      for (const row of result.rows) {
        if (emitted >= ceiling) return
        emitted += 1
        yield decodePayload(result, row, codec)
      }
      if (emitted >= ceiling) return
    }
  }

  private async fetchAllWithSchemas(
    query: Query,
    options: CallOptions
  ): Promise<readonly { readonly result: QueryResult; readonly row: Row }[]> {
    const rows: { readonly result: QueryResult; readonly row: Row }[] = []
    for await (const page of this.pages(query, options))
      for (const row of page.rows) rows.push({ result: page, row })
    return rows
  }

  private async *pages(
    first: Query,
    options: CallOptions = {},
    onMoved: () => void = () => undefined
  ): AsyncGenerator<QueryResult> {
    let query = first
    const singlePage = query.aggregate !== undefined || query.vector !== undefined
    let result = await this.run(query, options)
    const frontier = result.context.frontier
    for (;;) {
      if (frontierDiffers(frontier, result.context.frontier)) onMoved()
      yield result
      // An empty page ends the walk even when the server reports more rows,
      // so a server that skews on `has_more` cannot make it repeat forever.
      if (singlePage || result.rows.length === 0 || !result.page.hasMore) return
      query = {
        ...query,
        page: offsetPage(query.page, (query.page.offset ?? 0n) + BigInt(result.rows.length))
      }
      result = await this.run(query, options)
    }
  }

  private async *pageRows(
    ceiling: number,
    options: CallOptions,
    onMoved: () => void
  ): AsyncGenerator<QueryRow> {
    if (ceiling === 0) return
    let emitted = 0
    for await (const result of this.pages(this.queryValue, options, onMoved)) {
      for (const row of QueryRow.fromPage(result.fields, result.rows)) {
        if (emitted >= ceiling) return
        emitted += 1
        yield row
      }
      if (emitted >= ceiling) return
    }
  }
}

function typedValue(value: QueryValue): TypedValue {
  if (value === null) return { kind: "null" }
  if (value instanceof Uint8Array) return { kind: "binary", value }
  if (typeof value === "string") return { kind: "string", value }
  if (typeof value === "boolean") return { kind: "boolean", value }
  if (typeof value === "bigint") {
    if (value < I64_MIN || value > I64_MAX)
      throw new InvalidError(
        "query value integer is out of range: it must fit a signed 64-bit integer"
      )
    return { kind: "long", value }
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new InvalidError("query value number must be finite")
    return Number.isSafeInteger(value)
      ? { kind: "long", value: BigInt(value) }
      : { kind: "double", value }
  }
  return value
}

// Whether two pages were served at different frontiers: another partition set,
// applied offset or topic incarnation.
function frontierDiffers(
  first: readonly QueryFrontier[],
  later: readonly QueryFrontier[]
): boolean {
  if (first.length !== later.length) return true
  const key = (item: QueryFrontier) =>
    `${item.stream}\u0000${item.topic}\u0000${String(item.partitionId)}`
  const position = (item: QueryFrontier) =>
    `${String(item.appliedOffset)}\u0000${String(item.topicCreatedAtMicros ?? "")}`
  const positions = new Map(first.map((item) => [key(item), position(item)] as const))
  return later.some((item) => positions.get(key(item)) !== position(item))
}

function withPayload(query: Query): Query {
  return { ...query, select: { ...query.select, payload: true } }
}

function offsetPage(page: Query["page"], offset: bigint): Query["page"] {
  return { limit: page.limit, offset, wantTotal: page.wantTotal }
}

function decodePayload<T>(result: QueryResult, row: Row, codec: Codec<T>): T {
  const index = result.fields.findIndex((field) => field.name === ORIGINAL_PAYLOAD_FIELD)
  if (index < 0) throw new ConfigError("query result does not contain the original payload field")
  const payload = row.values[index]
  if (payload === undefined)
    throw new ProtocolError("query row is shorter than its declared result schema")
  if (payload.kind === "null") throw new ConfigError("query row has no original payload")
  if (payload.kind !== "binary")
    throw new ProtocolError("query original payload field is not binary")
  return codec.decode(payload.value)
}

function requireResultExecution(result: QueryResult, executionId: QueryExecutionId): QueryResult {
  if (result.context.executionId.asU128() !== executionId.asU128())
    throw new ProtocolError("query reply execution id does not match the request")
  return result
}
