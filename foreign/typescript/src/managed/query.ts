import { InvalidError } from "../client/errors.js"
import type { Codec } from "../stream/codecs.js"
import { CONVERSATION_FIELD, VECTOR_FIELD } from "../wire/headers.js"
import {
  type AggCall,
  type AggFunc,
  type Aggregate,
  type CmpOp,
  type Consistency,
  type Filter,
  type Query,
  type QueryResult,
  type Row,
  filterAll,
  filterPred
} from "../wire/query.js"
import type { Value } from "../wire/value.js"

export type QueryExecutor = (query: Query) => Promise<QueryResult>

function emptyQuery(index: string): Query {
  return {
    index,
    byKey: [],
    order: [],
    limit: 0,
    offset: 0,
    distinct: false,
    select: { fields: [], payload: false },
    consistency: "eventual",
    wantTotal: false
  }
}

export class QueryRequest {
  private queryValue: Query
  private rowCeiling: number | undefined

  constructor(
    index: string,
    private readonly execute: QueryExecutor
  ) {
    this.queryValue = emptyQuery(index)
  }

  byKey(field: string, value: string): this {
    this.queryValue = {
      ...this.queryValue,
      byKey: [...this.queryValue.byKey, { field, value }]
    }
    return this
  }

  conversation(conversationId: string): this {
    return this.byKey(CONVERSATION_FIELD, conversationId)
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

  private predicate(field: string, op: CmpOp, value: Value): this {
    return this.filter(filterPred(field, op, value))
  }

  filterEq(field: string, value: Value): this {
    return this.predicate(field, "eq", value)
  }

  filterNe(field: string, value: Value): this {
    return this.predicate(field, "ne", value)
  }

  filterGt(field: string, value: Value): this {
    return this.predicate(field, "gt", value)
  }

  filterGte(field: string, value: Value): this {
    return this.predicate(field, "gte", value)
  }

  filterLt(field: string, value: Value): this {
    return this.predicate(field, "lt", value)
  }

  filterLte(field: string, value: Value): this {
    return this.predicate(field, "lte", value)
  }

  filterIn(field: string, values: readonly Value[]): this {
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
    this.queryValue = { ...this.queryValue, limit: value }
    return this
  }

  offset(value: number): this {
    this.queryValue = { ...this.queryValue, offset: value }
    return this
  }

  withPayload(): this {
    this.queryValue = {
      ...this.queryValue,
      select: { ...this.queryValue.select, payload: true }
    }
    return this
  }

  withTotal(): this {
    this.queryValue = { ...this.queryValue, wantTotal: true }
    return this
  }

  consistency(level: Consistency): this {
    this.queryValue = { ...this.queryValue, consistency: level }
    return this
  }

  readYourWrites(): this {
    return this.consistency("readYourWrites")
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
      ...(options.field !== undefined ? { field: options.field } : {}),
      ...(options.fraction !== undefined ? { arg: options.fraction } : {})
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
    return this.aggregateAs("countDistinct", alias, { field })
  }

  stdDev(field: string, alias = "stddev"): this {
    return this.aggregateAs("stdDev", alias, { field })
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

  rawSql(sql: string, params: readonly Value[] = []): this {
    this.queryValue = { ...this.queryValue, rawSql: { sql, params } }
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
    return this.execute(this.queryValue)
  }

  async fetchTyped<T>(codec: Codec<T>): Promise<readonly T[]> {
    this.withPayload()
    const result = await this.fetch()
    return result.rows.map((row) => decodeRow(row, codec))
  }

  async fetchOne<T>(codec: Codec<T>): Promise<T | undefined> {
    this.withPayload().limit(1)
    const result = await this.fetch()
    const row = result.rows[0]
    return row === undefined ? undefined : decodeRow(row, codec)
  }

  async fetchAll(): Promise<readonly Row[]> {
    const rows: Row[] = []
    for await (const row of this.pageRows()) rows.push(row)
    return rows
  }

  async fetchAllTyped<T>(codec: Codec<T>): Promise<readonly T[]> {
    this.withPayload()
    const rows = await this.fetchAll()
    return rows.map((row) => decodeRow(row, codec))
  }

  rows(): AsyncIterable<Row> {
    if (this.rowCeiling === undefined) {
      throw new InvalidError("rows() needs an explicit ceiling: call maxRows(n) first")
    }
    return this.pageRows(this.rowCeiling)
  }

  private async *pageRows(ceiling = Number.POSITIVE_INFINITY): AsyncGenerator<Row> {
    let query = { ...this.queryValue, limit: this.queryValue.limit || 100 }
    const singlePage = query.aggregate !== undefined || query.vector !== undefined
    let emitted = 0
    while (emitted < ceiling) {
      const result = await this.execute(query)
      if (result.rows.length === 0) return
      for (const row of result.rows) {
        if (emitted >= ceiling) return
        emitted += 1
        yield row
      }
      if (singlePage || !result.page.hasMore) return
      query = { ...query, offset: query.offset + result.rows.length }
    }
  }
}

function decodeRow<T>(row: Row, codec: Codec<T>): T {
  if (row.payload === undefined) {
    throw new InvalidError("query row has no payload; the managed backend did not return bytes")
  }
  return codec.decode(row.payload)
}
