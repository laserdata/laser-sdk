use crate::error::LaserError;
use crate::laser::Laser;
use crate::query::{
    AGDX_QUERY_CODE, AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch,
    MAX_PAGE_SIZE, QUERY_OP_VERSION, Query, QueryEnvelope, QueryError, QueryReply, QueryResult,
    RawSql, Row, Sort, TextQuery, VECTOR_FIELD, Value, VectorQuery, Window,
};
use crate::stream::Decoder;
use crate::types::ConversationId;
use laser_wire::framing::encode_named;
use serde::de::DeserializeOwned;

impl Laser {
    /// Start a query over `index`. Returns a fluent builder, finished with
    /// `.fetch().await` (paged `QueryResult`), `.fetch_typed::<T>().await`
    /// (typed `Vec<T>`), or `.fetch_one::<T>().await` (typed `Option<T>`).
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Order { customer: String, amount: i64 }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let alice: Vec<Order> = laser.query("orders")
    ///     .where_eq("customer_id", "alice")
    ///     .filter_gte("total", 100)
    ///     .order_desc("total")
    ///     .limit(10)
    ///     .with_payload()
    ///     .fetch_typed().await?;
    /// # Ok(()) }
    /// ```
    pub fn query<'a>(&'a self, index: &'a str) -> QueryRequest<'a> {
        QueryRequest::new(self, index)
    }

    /// Lower-level: execute a pre-built `Query` and return the raw paged result.
    /// Most callers want `query(index).where_eq(...).fetch().await` instead.
    ///
    /// Query is a LaserData Cloud feature. Against raw Apache Iggy
    /// (`query.available` false) this returns `LaserError::Unsupported`.
    pub async fn execute_query(&self, query: Query) -> Result<QueryResult, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.query.available {
            return Err(LaserError::unsupported(
                "query",
                "the query surface is not served by this deployment",
            ));
        }
        // Fail fast on a consistency level the server has not advertised.
        // The additive `consistency` field is silently ignored by a backend
        // that does not implement it, which then serves an eventual read,
        // so refusing locally is the only way to honor fail-not-downgrade
        // and never serve a silently stale read that looks successful.
        // The Eventual level is always served, so the default path holds.
        if !capabilities.serves_consistency(query.consistency) {
            return Err(LaserError::unsupported_feature(
                "query",
                "consistency",
                format!(
                    "consistency level {:?} is not served by this deployment",
                    query.consistency
                ),
            ));
        }
        // Fail fast on an unadvertised lexical search: the additive `text`
        // field would be silently dropped by an unaware backend, which would
        // then answer the unfiltered query, a wider-than-asked silent wrong
        // answer. Same discipline as an unadvertised consistency level.
        if query.text.is_some() && !capabilities.query.keyword {
            return Err(LaserError::unsupported_feature(
                "query",
                "keyword",
                "lexical text search is not advertised by this deployment",
            ));
        }
        // Fail fast on advertised version skew: the server told us at connect
        // which envelope version it accepts, so spend the typed error locally
        // instead of a decode failure (or a server-side Version error) after a
        // round-trip. Servers that advertise nothing skip this check.
        if let Some(versions) = capabilities.versions
            && versions.query != QUERY_OP_VERSION
        {
            return Err(QueryError::Version {
                expected: versions.query,
                got: QUERY_OP_VERSION,
            }
            .into());
        }
        // Fail fast on an over-cap page before the round trip: LaserData Cloud would
        // reject it with the same `TooLarge`, so spend the error locally. `0`
        // means "a full page" (LaserData Cloud defaults it to `MAX_PAGE_SIZE`), so only
        // an explicit over-cap value is rejected here. `top_k` on a vector query
        // is the same page bound under a different name.
        if query.limit > MAX_PAGE_SIZE {
            return Err(QueryError::TooLarge {
                what: "limit".to_owned(),
                size: query.limit,
                cap: MAX_PAGE_SIZE,
            }
            .into());
        }
        if let Some(vector) = &query.vector
            && vector.top_k > MAX_PAGE_SIZE
        {
            return Err(QueryError::TooLarge {
                what: "top_k".to_owned(),
                size: vector.top_k,
                cap: MAX_PAGE_SIZE,
            }
            .into());
        }
        let request = QueryEnvelope::new(query);
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        // The encoded query envelope rides the `AGDX_QUERY` managed command: the
        // server forwards it to LaserData Cloud over its local socket and returns
        // the `QueryReply` bytes, off the log, no reply topic, no correlation poll.
        let payload = self
            .send_raw_with_response(AGDX_QUERY_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<QueryReply>(&payload)? {
            QueryReply::Ok(result) => Ok(result),
            QueryReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query: unknown reply variant".to_owned(),
            )),
        }
    }
}

/// Fluent builder for `Laser::query`. Accumulates a `Query`, then `.fetch()`
/// returns the paged result, `.fetch_typed::<T>()` deserializes every row's
/// payload into `T`, `.fetch_one::<T>()` is the same but for at most one row.
#[must_use = "call .fetch().await (or a fetch_* variant) to run the query"]
pub struct QueryRequest<'a> {
    laser: &'a Laser,
    query: Query,
    max_rows: Option<usize>,
}

impl<'a> QueryRequest<'a> {
    fn new(laser: &'a Laser, index: &'a str) -> Self {
        Self {
            laser,
            query: Query::builder().index(index).build(),
            max_rows: None,
        }
    }

    /// Exact-match on an indexed field (point lookup).
    pub fn where_eq(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.by_key.push(KeyMatch::new(field, value));
        self
    }

    /// Narrow to the rows a single conversation produced (the conversation
    /// lens): an exact-match on the auto-projected
    /// [`CONVERSATION_FIELD`](laser_wire::headers::CONVERSATION_FIELD), which a
    /// deployment materializes from every message's `gen_ai.conversation.id`
    /// header. No producer-side index header is needed, so this works on any
    /// projection. Sugar over [`where_eq`](Self::where_eq).
    pub fn conversation(self, conversation: ConversationId) -> Self {
        self.where_eq(
            laser_wire::headers::CONVERSATION_FIELD,
            conversation.to_string(),
        )
    }

    /// Resolve this query against a fork's copy-on-write view (the trunk overlaid
    /// with the fork's speculative rows) instead of the trunk. Open the fork with
    /// [`Laser::fork`](crate::laser::Laser::fork).
    pub fn fork(mut self, fork_id: impl Into<String>) -> Self {
        self.query.fork = Some(fork_id.into());
        self
    }

    /// Filter rows where `field == value`.
    pub fn filter_eq(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Eq, value)
    }

    /// Filter rows where `field != value`.
    pub fn filter_ne(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Ne, value)
    }

    /// Filter rows where `field > value` (numeric if both parse as numbers, else lexical).
    pub fn filter_gt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Gt, value)
    }

    /// Filter rows where `field >= value`.
    pub fn filter_gte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Gte, value)
    }

    /// Filter rows where `field < value`.
    pub fn filter_lt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Lt, value)
    }

    /// Filter rows where `field <= value`.
    pub fn filter_lte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Lte, value)
    }

    /// Filter rows where `field` is one of the given values.
    pub fn filter_in<T: Into<Value>>(
        self,
        field: impl Into<String>,
        values: impl IntoIterator<Item = T>,
    ) -> Self {
        let list = Value::List(values.into_iter().map(Into::into).collect());
        self.predicate(field, CmpOp::In, list)
    }

    /// Filter rows where `field` contains `value` (substring).
    pub fn filter_contains(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Contains, Value::Str(value.into()))
    }

    /// Filter rows where `field` starts with `value`.
    pub fn filter_prefix(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Prefix, Value::Str(value.into()))
    }

    /// Filter on `agdx.idx.message_type`.
    pub fn message_type(mut self, value: impl Into<String>) -> Self {
        self.query.message_type = Some(value.into());
        self
    }

    /// Filter rows whose `agdx.idx.ts` (epoch micros) falls in `[start, end]`.
    pub fn time_range(mut self, start: u64, end: u64) -> Self {
        self.query.time_range = Some((start, end));
        self
    }

    /// Sort ascending by `field` (numeric-then-lexical).
    pub fn order_asc(mut self, field: impl Into<String>) -> Self {
        self.query.order.push(Sort {
            field: field.into(),
            dir: Dir::Asc,
        });
        self
    }

    /// Sort descending by `field` (numeric-then-lexical).
    pub fn order_desc(mut self, field: impl Into<String>) -> Self {
        self.query.order.push(Sort {
            field: field.into(),
            dir: Dir::Desc,
        });
        self
    }

    /// Limit the page to `n` rows. Above `MAX_PAGE_SIZE` is rejected with
    /// `QueryError::TooLarge`. `0` means a full page.
    pub fn limit(mut self, n: usize) -> Self {
        self.query.limit = n;
        self
    }

    /// Skip the first `n` matching rows.
    pub fn offset(mut self, n: usize) -> Self {
        self.query.offset = n;
        self
    }

    /// Return the opaque payload bytes on each row (so you can decode them).
    pub fn with_payload(mut self) -> Self {
        self.query.select.payload = true;
        self
    }

    /// Request an exact `page.total` (a `COUNT(*)` on the server). Default
    /// off: without it, `page.total` is `None` and `page.has_more` is still
    /// exact. Ask for this only when the exact count itself matters (e.g.
    /// rendering "page 3 of 12"), it costs a full-table scan on a wide filter.
    pub fn with_total(mut self) -> Self {
        self.query.want_total = true;
        self
    }

    /// Require read-your-writes: wait for the projector to apply the source log
    /// up to its current head before serving, so this query sees writes that
    /// completed before it. Bounded: if the projector cannot catch up in time
    /// the query returns `QueryError::Stale` (`LaserError::is_stale()`) instead
    /// of silently serving older data. Backend-gated: a deployment that cannot
    /// honor it returns `Unsupported`.
    pub fn read_your_writes(mut self) -> Self {
        self.query.consistency = Consistency::ReadYourWrites;
        self
    }

    /// Set the read-consistency level explicitly (`Eventual` is the default,
    /// `ReadYourWrites`, or `Strong`). `Strong` is gated by `strong_consistency`.
    pub fn consistency(mut self, level: Consistency) -> Self {
        self.query.consistency = level;
        self
    }

    /// Lexical relevance search over every text-hinted indexed field. Relevance
    /// lands in each row's `score` exactly as vector distance does. Gated by
    /// the `keyword` capability: refused locally when unadvertised, so an
    /// unaware backend can never silently drop the filter.
    pub fn text(mut self, query: impl Into<String>) -> Self {
        self.query.text = Some(TextQuery {
            field: None,
            query: query.into(),
        });
        self
    }

    /// Lexical relevance search narrowed to one indexed `field`.
    pub fn text_in(mut self, field: impl Into<String>, query: impl Into<String>) -> Self {
        self.query.text = Some(TextQuery {
            field: Some(field.into()),
            query: query.into(),
        });
        self
    }

    /// Project only the named indexed fields into each row's headers.
    pub fn select_fields<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        self.query.select.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Aggregate: row count, output under the header `count`.
    pub fn count(self) -> Self {
        self.push_agg(agg_call(AggFunc::Count, None, None, "count"))
    }

    /// Aggregate: sum of `field`, output under the header `sum`.
    pub fn sum(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Sum, Some(field.into()), None, "sum"))
    }

    /// Aggregate: arithmetic mean of `field`, output under the header `avg`.
    pub fn avg(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Avg, Some(field.into()), None, "avg"))
    }

    /// Aggregate: minimum of `field`, output under the header `min`.
    pub fn min(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Min, Some(field.into()), None, "min"))
    }

    /// Aggregate: maximum of `field`, output under the header `max`.
    pub fn max(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Max, Some(field.into()), None, "max"))
    }

    /// Aggregate: distinct count of `field`, output under the header
    /// `count_distinct`.
    pub fn count_distinct(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::CountDistinct,
            Some(field.into()),
            None,
            "count_distinct",
        ))
    }

    /// Aggregate: population standard deviation of `field`, output under the
    /// header `stddev`. Backend-gated (columnar backends only).
    pub fn stddev(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::StdDev,
            Some(field.into()),
            None,
            "stddev",
        ))
    }

    /// Aggregate: the `fraction` quantile of `field` (e.g. 0.95 for p95), output
    /// under the header `percentile`. Backend-gated (columnar backends only).
    pub fn percentile(self, field: impl Into<String>, fraction: f64) -> Self {
        self.push_agg(agg_call(
            AggFunc::Percentile,
            Some(field.into()),
            Some(fraction),
            "percentile",
        ))
    }

    /// Add an aggregate with an explicit output alias. Use to return several
    /// aggregates of the same kind in one query, or to name the output column.
    pub fn agg_as(
        self,
        func: AggFunc,
        field: Option<String>,
        fraction: Option<f64>,
        alias: impl Into<String>,
    ) -> Self {
        let alias = alias.into();
        self.push_agg(AggCall {
            func,
            field,
            arg: fraction,
            alias,
        })
    }

    /// Group the aggregate by the named fields.
    pub fn group_by<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        let group_by: Vec<String> = fields.into_iter().map(Into::into).collect();
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.group_by = group_by,
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by,
                    funcs: Vec::new(),
                    window: None,
                });
            }
        }
        self
    }

    /// Bucket the aggregate into tumbling windows of `every_micros` over the
    /// timestamp `field`. Each result row carries a `window_start` header (the
    /// bucket's lower edge in epoch micros).
    pub fn window(mut self, field: impl Into<String>, every_micros: u64) -> Self {
        let window = Some(Window {
            field: field.into(),
            every_micros,
        });
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.window = window,
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: Vec::new(),
                    window,
                });
            }
        }
        self
    }

    /// Keep only aggregate groups matching `filter`. Predicate fields reference
    /// an aggregate alias (e.g. `count`) or a group key, not raw row fields.
    pub fn having(mut self, filter: Filter) -> Self {
        self.query.having = Some(filter);
        self
    }

    /// Return only distinct rows over the projected fields. Requires
    /// [`select_fields`](Self::select_fields).
    pub fn distinct(mut self) -> Self {
        self.query.distinct = true;
        self
    }

    /// Opt-in raw-SQL escape hatch: run `sql` (a single read-only SELECT) on the
    /// index's backend. SQL backends only, not portable. Result columns come
    /// back as row headers keyed by column name.
    pub fn raw_sql(mut self, sql: impl Into<String>) -> Self {
        self.query.raw_sql = Some(RawSql {
            sql: sql.into(),
            params: Vec::new(),
        });
        self
    }

    /// Like [`raw_sql`](Self::raw_sql) but with positional bind params.
    pub fn raw_sql_with<V: Into<Value>>(
        mut self,
        sql: impl Into<String>,
        params: impl IntoIterator<Item = V>,
    ) -> Self {
        self.query.raw_sql = Some(RawSql {
            sql: sql.into(),
            params: params.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Approximate nearest-neighbour search against the default vector field
    /// (`VECTOR_FIELD` = `"embedding"`). Use [`nearest_in`](Self::nearest_in) to
    /// point at a different field name.
    pub fn nearest(self, embedding: Vec<f32>, top_k: usize) -> Self {
        self.nearest_in(VECTOR_FIELD, embedding, top_k)
    }

    /// Approximate nearest-neighbour search on an explicit `field`. Use when
    /// your projector stores the vector under a custom payload key.
    pub fn nearest_in(
        mut self,
        field: impl Into<String>,
        embedding: Vec<f32>,
        top_k: usize,
    ) -> Self {
        self.query.vector = Some(VectorQuery {
            field: field.into(),
            embedding,
            top_k,
        });
        self
    }

    /// Run the query and return the paged result + metadata.
    pub async fn fetch(self) -> Result<QueryResult, LaserError> {
        self.laser.execute_query(self.query).await
    }

    /// Run the query, then deserialize every row's payload into `T` (JSON).
    /// `with_payload()` is implied. Single-page only: use `.stream_typed()` or
    /// `.fetch_all_typed()` if there may be more than `MAX_PAGE_SIZE` matches.
    pub async fn fetch_typed<T: DeserializeOwned>(mut self) -> Result<Vec<T>, LaserError> {
        self.query.select.payload = true;
        let result = self.laser.execute_query(self.query).await?;
        result
            .rows
            .iter()
            .map(|row| row.decode_json::<T>().map_err(LaserError::from))
            .collect()
    }

    /// Like `fetch_typed` but caps the result at one row.
    pub async fn fetch_one<T: DeserializeOwned>(mut self) -> Result<Option<T>, LaserError> {
        self.query.select.payload = true;
        self.query.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
        match result.rows.first() {
            Some(row) => row.decode_json::<T>().map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }

    /// Like [`fetch_typed`](Self::fetch_typed) but decode each row's payload
    /// with any [`Decoder`] (`Json`, `Msgpack`, or your own codec) instead of
    /// being locked to JSON. `with_payload()` is implied.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::stream::Msgpack;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Order { id: String }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let orders: Vec<Order> = laser.query("orders").fetch_typed_with::<Msgpack, _>().await?;
    /// # Ok(()) }
    /// ```
    pub async fn fetch_typed_with<C, T>(mut self) -> Result<Vec<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        let result = self.laser.execute_query(self.query).await?;
        result
            .rows
            .iter()
            .map(|row| row.decode_with::<C, T>().map_err(LaserError::from))
            .collect()
    }

    /// Like [`fetch_one`](Self::fetch_one) but decode the row with any
    /// [`Decoder`] instead of JSON.
    pub async fn fetch_one_with<C, T>(mut self) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        self.query.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
        match result.rows.first() {
            Some(row) => row
                .decode_with::<C, T>()
                .map(Some)
                .map_err(LaserError::from),
            None => Ok(None),
        }
    }

    // The auto-paginating page walk behind `rows()` and `fetch_all()`. Private:
    // an unbounded walk is never handed out directly (the bounded-reads law),
    // and the public `stream` verb belongs to the Iggy topology root alone.
    // Auto-paginates with `limit` (or 100 if unset) and stops when the worker
    // reports `has_more = false` (or an empty page, whichever comes first).
    // Aggregate and vector queries are single-page by construction: `offset`
    // is not a meaningful cursor for either shape.
    fn page_stream(mut self) -> QueryStream<'a> {
        if self.query.limit == 0 {
            self.query.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE;
        }
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        QueryStream::new(self.laser, self.query, single_page)
    }

    // Like `page_stream` but each yield is `T` decoded from the row's payload.
    // `with_payload()` is implied, and the publisher must have chained
    // `.inline_payload()` for the bytes to be there.
    fn typed_page_stream<T: DeserializeOwned>(mut self) -> TypedQueryStream<'a, T> {
        self.query.select.payload = true;
        if self.query.limit == 0 {
            self.query.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE;
        }
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        TypedQueryStream::new(self.laser, self.query, single_page)
    }

    /// Cap the total rows a [`rows`](Self::rows) walk may yield. Explicit by
    /// design (the bounded-reads law): a paged walk with no ceiling is an
    /// unbounded read dressed as a stream, so the ceiling is the caller
    /// writing a number, never a silent default.
    pub fn max_rows(mut self, n: usize) -> Self {
        self.max_rows = Some(n);
        self
    }

    /// Walk matching rows across pages, bounded by an explicit
    /// [`max_rows`](Self::max_rows). Each `.next().await` yields the next row
    /// and the walk stops at the cap or the last page, whichever comes first.
    /// The grammar's streaming terminal, mirroring the kv scan's `.entries()`.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let mut rows = laser.query("orders").where_eq("status", "paid").max_rows(1_000).rows()?;
    /// while let Some(row) = rows.next().await? {
    ///     let _ = row;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn rows(self) -> Result<QueryRows<'a>, LaserError> {
        let Some(max_rows) = self.max_rows else {
            return Err(LaserError::Invalid(
                "rows() needs an explicit ceiling: chain .max_rows(n) first".to_owned(),
            ));
        };
        Ok(QueryRows {
            stream: self.page_stream(),
            remaining: max_rows,
        })
    }

    /// Like [`rows`](Self::rows) but each yield is `T` decoded from the row's
    /// payload, under the same explicit [`max_rows`](Self::max_rows) ceiling.
    /// `with_payload()` is implied, and the publisher must have chained
    /// `.inline_payload()` for the bytes to be there.
    pub fn rows_typed<T: DeserializeOwned>(self) -> Result<TypedQueryRows<'a, T>, LaserError> {
        let Some(max_rows) = self.max_rows else {
            return Err(LaserError::Invalid(
                "rows_typed() needs an explicit ceiling: chain .max_rows(n) first".to_owned(),
            ));
        };
        Ok(TypedQueryRows {
            stream: self.typed_page_stream(),
            remaining: max_rows,
        })
    }

    /// Materialize EVERY matching row by walking pages internally: the
    /// explicit full-result opt-in. Convenient when you need them all and the
    /// working set fits comfortably in memory. Prefer the bounded
    /// [`rows`](Self::rows) walk when it might not.
    pub async fn fetch_all(self) -> Result<Vec<Row>, LaserError> {
        let mut stream = self.page_stream();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Materialize EVERY matching row, decoded into `T`: the explicit
    /// full-result opt-in, see [`fetch_all`](Self::fetch_all).
    pub async fn fetch_all_typed<T: DeserializeOwned>(self) -> Result<Vec<T>, LaserError> {
        let mut stream = self.typed_page_stream::<T>();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Inspect the raw `Query` the builder produced (debugging).
    pub fn into_query(self) -> Query {
        self.query
    }

    fn predicate(self, field: impl Into<String>, op: CmpOp, value: impl Into<Value>) -> Self {
        self.and_filter(Filter::pred(field, op, value))
    }

    /// AND `filter` into the query's predicate tree. The fluent `filter_*`
    /// helpers route through here, so chained filters compose as a conjunction.
    /// Build `Any`/`Not` subtrees with [`Filter::any`]/[`Filter::negate`] and pass
    /// them here.
    pub fn filter(self, filter: Filter) -> Self {
        self.and_filter(filter)
    }

    fn and_filter(mut self, filter: Filter) -> Self {
        self.query.filter = Some(match self.query.filter.take() {
            None => filter,
            Some(Filter::All(mut existing)) => {
                existing.push(filter);
                Filter::All(existing)
            }
            Some(other) => Filter::All(vec![other, filter]),
        });
        self
    }

    fn push_agg(mut self, call: AggCall) -> Self {
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.funcs.push(call),
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: vec![call],
                    window: None,
                });
            }
        }
        self
    }
}

/// Build an [`AggCall`] with the given output alias.
fn agg_call(func: AggFunc, field: Option<String>, arg: Option<f64>, alias: &str) -> AggCall {
    AggCall {
        func,
        field,
        arg,
        alias: alias.to_owned(),
    }
}

impl<'a> From<QueryRequest<'a>> for Query {
    fn from(request: QueryRequest<'a>) -> Self {
        request.query
    }
}

/// The auto-paginating row walk behind [`QueryRequest::rows`] and
/// [`QueryRequest::fetch_all`]. Holds the `Query` and refills its buffer by
/// re-issuing it with an advanced `offset` each time the local page drains,
/// until the worker reports `has_more = false` (or returns an empty page,
/// whichever comes first). The empty-page guard rules out an infinite loop if
/// the worker ever skews on the `has_more` flag.
struct QueryStream<'a> {
    laser: &'a Laser,
    query: Query,
    finished: bool,
    // Aggregate / vector queries do not have a meaningful offset cursor, so the
    // stream fetches once and stops regardless of `has_more`.
    single_page: bool,
    buffer: std::vec::IntoIter<Row>,
}

impl<'a> QueryStream<'a> {
    fn new(laser: &'a Laser, query: Query, single_page: bool) -> Self {
        Self {
            laser,
            query,
            finished: false,
            single_page,
            buffer: Vec::new().into_iter(),
        }
    }

    // Yield the next row, fetching the next page if the local buffer is empty.
    // Returns `Ok(None)` after the final row of the last page.
    async fn next(&mut self) -> Result<Option<Row>, LaserError> {
        if let Some(row) = self.buffer.next() {
            return Ok(Some(row));
        }
        if self.finished {
            return Ok(None);
        }
        let page = self.laser.execute_query(self.query.clone()).await?;
        let fetched = page.rows.len();
        // Empty page always terminates: belt-and-braces against a worker that
        // mis-reports `has_more` and would otherwise wedge the loop.
        self.finished = self.single_page || fetched == 0 || !page.page.has_more;
        self.query.offset = self.query.offset.saturating_add(fetched);
        self.buffer = page.rows.into_iter();
        Ok(self.buffer.next())
    }
}

/// The bounded row walk returned by [`QueryRequest::rows`]: the auto-paginating
/// stream under the explicit `max_rows` ceiling. Yields `Ok(None)` at the cap
/// or after the last page, whichever comes first.
pub struct QueryRows<'a> {
    stream: QueryStream<'a>,
    remaining: usize,
}

impl QueryRows<'_> {
    /// Yield the next row, or `Ok(None)` at the ceiling or after the last page.
    pub async fn next(&mut self) -> Result<Option<Row>, LaserError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = self.stream.next().await?;
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }
}

/// The typed sibling of [`QueryStream`], behind [`QueryRequest::rows_typed`]
/// and [`QueryRequest::fetch_all_typed`]. Each `.next().await` decodes the
/// next row's payload into `T`.
struct TypedQueryStream<'a, T> {
    inner: QueryStream<'a>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<'a, T: DeserializeOwned> TypedQueryStream<'a, T> {
    fn new(laser: &'a Laser, query: Query, single_page: bool) -> Self {
        Self {
            inner: QueryStream::new(laser, query, single_page),
            _marker: std::marker::PhantomData,
        }
    }

    // Yield the next decoded value, or `Ok(None)` after the last page.
    async fn next(&mut self) -> Result<Option<T>, LaserError> {
        match self.inner.next().await? {
            Some(row) => row.decode_json::<T>().map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }
}

/// The bounded typed row walk returned by [`QueryRequest::rows_typed`]: the
/// auto-paginating stream under the explicit `max_rows` ceiling, decoding each
/// row's payload into `T`. Yields `Ok(None)` at the cap or after the last
/// page, whichever comes first.
pub struct TypedQueryRows<'a, T> {
    stream: TypedQueryStream<'a, T>,
    remaining: usize,
}

impl<T: DeserializeOwned> TypedQueryRows<'_, T> {
    /// Yield the next decoded value, or `Ok(None)` at the ceiling or after the
    /// last page.
    pub async fn next(&mut self) -> Result<Option<T>, LaserError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = self.stream.next().await?;
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }
}
