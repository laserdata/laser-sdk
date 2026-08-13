import { InvalidError } from "../client/errors.js"
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
  type QueryExecutionStatus,
  type QueryResult,
  type QueryTarget,
  type Row,
  type SqlDialect,
  filterAll,
  filterPred,
  newQuery,
  operationalTarget,
  queryResultValue
} from "../wire/query.js"
import type { TypedValue } from "../wire/schema.js"

export type QueryExecutor = (query: Query) => Promise<QueryResult>
export type QueryStatusExecutor = (executionId: QueryExecutionId) => Promise<QueryExecutionStatus>

function emptyQuery(target: QueryTarget): Query {
  return newQuery(
    target,
    QueryExecutionId.fromU128(mintUlidValue()),
    BigInt(Date.now()) * 1000n + 30_000_000n
  )
}

export class QueryRequest {
  private queryValue: Query
  private rowCeiling: number | undefined
  private readonly execute: QueryExecutor
  private readonly readStatus: QueryStatusExecutor | undefined
  private readonly cancelExecution: QueryStatusExecutor | undefined

  constructor(
    indexOrTarget: string | QueryTarget,
    execute: QueryExecutor,
    readStatus?: QueryStatusExecutor,
    cancelExecution?: QueryStatusExecutor
  ) {
    this.execute = execute
    this.readStatus = readStatus
    this.cancelExecution = cancelExecution
    this.queryValue = emptyQuery(
      typeof indexOrTarget === "string" ? operationalTarget(indexOrTarget) : indexOrTarget
    )
  }

  executionId(value: QueryExecutionId): this {
    this.queryValue = { ...this.queryValue, executionId: value }
    return this
  }

  deadlineMicros(value: bigint): this {
    this.queryValue = { ...this.queryValue, deadlineMicros: value }
    return this
  }

  whereEq(field: string, value: string | TypedValue): this {
    this.queryValue = {
      ...this.queryValue,
      byKey: [
        ...this.queryValue.byKey,
        { field, value: typeof value === "string" ? { kind: "string", value } : value }
      ]
    }
    return this
  }

  byKey(field: string, value: string | TypedValue): this {
    return this.whereEq(field, value)
  }

  conversation(conversationId: string): this {
    return this.whereEq(CONVERSATION_FIELD, conversationId)
  }

  fork(forkId: string): this {
    this.queryValue = { ...this.queryValue, fork: forkId }
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

  filterEq(field: string, value: TypedValue): this {
    return this.predicate(field, "eq", value)
  }
  filterNe(field: string, value: TypedValue): this {
    return this.predicate(field, "ne", value)
  }
  filterGt(field: string, value: TypedValue): this {
    return this.predicate(field, "gt", value)
  }
  filterGte(field: string, value: TypedValue): this {
    return this.predicate(field, "gte", value)
  }
  filterLt(field: string, value: TypedValue): this {
    return this.predicate(field, "lt", value)
  }
  filterLte(field: string, value: TypedValue): this {
    return this.predicate(field, "lte", value)
  }
  filterIn(field: string, values: readonly TypedValue[]): this {
    return this.predicate(field, "in", { kind: "list", value: values })
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
  cursor(value: string): this {
    this.queryValue = { ...this.queryValue, page: cursorPage(this.queryValue.page, value) }
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
  text(query: string): this {
    this.queryValue = { ...this.queryValue, text: { query } }
    return this
  }
  textIn(field: string, query: string): this {
    this.queryValue = { ...this.queryValue, text: { field, query } }
    return this
  }
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

  aggregateAs(
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
    return this.aggregateAs("count", alias)
  }
  sum(field: string, alias = "sum"): this {
    return this.aggregateAs("sum", alias, { field })
  }
  avg(field: string, alias = "avg"): this {
    return this.aggregateAs("avg", alias, { field })
  }
  min(field: string, alias = "min"): this {
    return this.aggregateAs("min", alias, { field })
  }
  max(field: string, alias = "max"): this {
    return this.aggregateAs("max", alias, { field })
  }
  countDistinct(field: string, alias = "count_distinct"): this {
    return this.aggregateAs("count_distinct", alias, { field })
  }
  stdDev(field: string, alias = "stddev"): this {
    return this.aggregateAs("std_dev", alias, { field })
  }
  percentile(field: string, fraction: number, alias = "percentile"): this {
    return this.aggregateAs("percentile", alias, { field, fraction })
  }

  groupBy(fields: readonly string[]): this {
    const current: Aggregate = this.queryValue.aggregate ?? { groupBy: [], funcs: [] }
    this.queryValue = { ...this.queryValue, aggregate: { ...current, groupBy: [...fields] } }
    return this
  }
  window(field: string, everyMicros: bigint): this {
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
  rawSql(
    sql: string,
    params: readonly TypedValue[] = [],
    dialect: SqlDialect = "data_fusion"
  ): this {
    this.queryValue = { ...this.queryValue, rawSql: { dialect, sql, params } }
    return this
  }
  maxRows(value: number): this {
    this.rowCeiling = value
    return this
  }
  intoQuery(): Query {
    return this.queryValue
  }
  async fetch(): Promise<QueryResult> {
    return requireResultExecution(await this.execute(this.queryValue), this.queryValue.executionId)
  }
  async status(): Promise<QueryExecutionStatus> {
    if (this.readStatus === undefined)
      throw new InvalidError("query status is unavailable on this transport")
    return requireStatusExecution(
      await this.readStatus(this.queryValue.executionId),
      this.queryValue.executionId
    )
  }
  async cancel(): Promise<QueryExecutionStatus> {
    if (this.cancelExecution === undefined)
      throw new InvalidError("query cancellation is unavailable on this transport")
    return requireStatusExecution(
      await this.cancelExecution(this.queryValue.executionId),
      this.queryValue.executionId
    )
  }

  async fetchTyped<T>(codec: Codec<T>): Promise<readonly T[]> {
    this.withPayload()
    const result = await this.fetch()
    return result.rows.map((row) => decodePayload(result, row, codec))
  }
  async fetchOne<T>(codec: Codec<T>): Promise<T | undefined> {
    this.withPayload().limit(1)
    const result = await this.fetch()
    const row = result.rows[0]
    return row === undefined ? undefined : decodePayload(result, row, codec)
  }
  async fetchAll(): Promise<readonly Row[]> {
    const rows: Row[] = []
    for await (const row of this.pageRows()) rows.push(row)
    return rows
  }
  async fetchAllTyped<T>(codec: Codec<T>): Promise<readonly T[]> {
    this.withPayload()
    const pages = await this.fetchAllWithSchemas()
    return pages.map(({ result, row }) => decodePayload(result, row, codec))
  }
  rows(): AsyncIterable<Row> {
    if (this.rowCeiling === undefined)
      throw new InvalidError("rows() needs an explicit ceiling: call maxRows(n) first")
    return this.pageRows(this.rowCeiling)
  }

  private async fetchAllWithSchemas(): Promise<
    readonly { readonly result: QueryResult; readonly row: Row }[]
  > {
    const rows: { readonly result: QueryResult; readonly row: Row }[] = []
    for await (const page of this.pages())
      for (const row of page.rows) rows.push({ result: page, row })
    return rows
  }

  private async *pages(): AsyncGenerator<QueryResult> {
    let query = this.queryValue
    const singlePage = query.aggregate !== undefined || query.vector !== undefined
    let result = requireResultExecution(await this.execute(query), query.executionId)
    for (;;) {
      yield result
      if (singlePage || !result.page.hasMore || result.page.nextCursor === undefined) return
      query = { ...query, page: cursorPage(query.page, result.page.nextCursor) }
      result = requireResultExecution(await this.execute(query), query.executionId)
    }
  }

  private async *pageRows(ceiling = Number.POSITIVE_INFINITY): AsyncGenerator<Row> {
    let emitted = 0
    for await (const result of this.pages()) {
      for (const row of result.rows) {
        if (emitted >= ceiling) return
        emitted += 1
        yield row
      }
      if (emitted >= ceiling) return
    }
  }
}

function offsetPage(page: Query["page"], offset: bigint): Query["page"] {
  return { limit: page.limit, offset, wantTotal: page.wantTotal }
}

function cursorPage(page: Query["page"], cursor: string): Query["page"] {
  return { limit: page.limit, cursor, wantTotal: page.wantTotal }
}

function decodePayload<T>(result: QueryResult, row: Row, codec: Codec<T>): T {
  const payload = queryResultValue(result, row, "__laser_original_payload")
  if (payload?.kind !== "binary") throw new InvalidError("query row has no original payload bytes")
  return codec.decode(payload.value)
}

function requireResultExecution(result: QueryResult, executionId: QueryExecutionId): QueryResult {
  if (result.context.executionId.asU128() !== executionId.asU128())
    throw new InvalidError("query reply execution id does not match the request")
  return result
}

function requireStatusExecution(
  status: QueryExecutionStatus,
  executionId: QueryExecutionId
): QueryExecutionStatus {
  if (status.executionId.asU128() !== executionId.asU128())
    throw new InvalidError("query status execution id does not match the request")
  return status
}
