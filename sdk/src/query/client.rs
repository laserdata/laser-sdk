use crate::error::LaserError;
use crate::laser::Laser;
use crate::nodes::Target;
use crate::query::{
    AGDX_QUERY_CODE, AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch,
    QUERY_OP_VERSION, Query, QueryError, QueryExecutionId, QueryFrontier, QueryResult, QueryTarget,
    RawSql, Row, Sort, SqlDialect, TextQuery, TypedValue, VECTOR_FIELD, VectorQuery, Window,
};
use crate::stream::Decoder;
use crate::types::{ConversationId, MintUlid};
use laser_wire::framing::encode_named;
use laser_wire::query::{QueryEnvelope, QueryReply};
use laser_wire::schema::{LogicalField, ORIGINAL_PAYLOAD_FIELD_NAME};
use laser_wire::validate::Validate;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::retry::{QUERY_DEADLINE as DEFAULT_QUERY_DEADLINE, QUERY_REPLY_GRACE};

impl Laser {
    /// Start a query over `index`. Returns a fluent builder, finished with
    /// `.fetch().await` (paged `QueryResult`), `.fetch_typed::<T>().await`
    /// (typed `Vec<T>`), or `.fetch_one::<T>().await` (typed `Option<T>`).
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Reading { host: String, cpu: i64 }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let hot: Vec<Reading> = laser.query("readings")
    ///     .where_eq("host_id", "node-7")
    ///     .filter_gte("cpu", 90)
    ///     .order_desc("cpu")
    ///     .limit(10)
    ///     .with_payload()
    ///     .fetch_typed().await?;
    /// # Ok(()) }
    /// ```
    pub fn query(&self, index: &str) -> QueryRequest {
        QueryRequest::new(
            self.clone(),
            QueryTarget::operational(self.resource_name(index)),
        )
    }

    /// Start a query against an explicit target. An operational index is sent
    /// as [`resource_name`](Self::resource_name) names it.
    pub fn query_target(&self, target: QueryTarget) -> QueryRequest {
        let target = match target {
            QueryTarget::Operational { index } => {
                QueryTarget::operational(self.resource_name(&index))
            }
            other => other,
        };
        QueryRequest::new(self.clone(), target)
    }

    /// Lower-level: execute a pre-built `Query` and return the raw paged result.
    /// Most callers want `query(index).where_eq(...).fetch().await` instead.
    ///
    /// Query requires the managed backend in Laser Stack or LaserData Cloud. Against
    /// Apache Iggy without a managed backend (`query.available` false), this
    /// returns `LaserError::Unsupported`.
    pub async fn execute_query(&self, query: Query) -> Result<QueryResult, LaserError> {
        query.validate()?;
        let execution_id = query.execution_id;
        let capabilities = self.capabilities().await;
        if !capabilities.query.available {
            return Err(LaserError::unsupported(
                "query",
                "the query surface is not served by this deployment",
            ));
        }
        if let Some(raw) = &query.raw_sql
            && !capabilities.enabled_backends().any(|backend| {
                backend
                    .query
                    .as_ref()
                    .is_some_and(|query| query.raw_sql && query.dialects.contains(&raw.dialect))
            })
        {
            return Err(LaserError::unsupported(
                "query",
                "raw SQL dialect is not served by an enabled backend",
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
        let deadline_micros = query.deadline_micros;
        let request = QueryEnvelope::new(query);
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        // The encoded query envelope rides the `AGDX_QUERY` managed command: the
        // server forwards it to LaserData Cloud over its local socket and returns
        // the `QueryReply` bytes, off the log, no reply topic, no correlation poll.
        let (payload, _) = self
            .send_query_until(AGDX_QUERY_CODE, payload, deadline_micros, Target::Caller)
            .await?;
        match crate::error::decode_managed_reply::<QueryReply>(&payload)? {
            QueryReply::Ok(result) => validate_query_result(*result, execution_id),
            QueryReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Send one query request and wait for its reply no longer than the
    /// query's deadline allows. A deadline further out than
    /// [`DEFAULT_QUERY_DEADLINE`] is refused, because the reply could not
    /// come back before the transport stops waiting for it. A timed out wait
    /// does not mean the query stopped. The server ends it at its deadline.
    async fn send_query_until(
        &self,
        code: u32,
        payload: Vec<u8>,
        deadline_micros: u64,
        target: Target,
    ) -> Result<(Vec<u8>, Option<u8>), LaserError> {
        tokio::time::timeout(
            query_reply_wait(deadline_micros, now_micros())?,
            self.send_managed_to(code, payload, target),
        )
        .await
        .map_err(|_| LaserError::Timeout("no query reply by the query deadline"))?
    }
}

fn validate_query_result(
    result: QueryResult,
    execution_id: QueryExecutionId,
) -> Result<QueryResult, LaserError> {
    result.validate()?;
    if result.context.execution_id != execution_id {
        return Err(LaserError::Protocol(
            "query reply execution id does not match the request".to_owned(),
        ));
    }
    Ok(result)
}

/// Fluent builder for `Laser::query`. Accumulates a `Query`, then `.fetch()`
/// returns the paged result, `.fetch_typed::<T>()` deserializes every row's
/// payload into `T`, `.fetch_one::<T>()` is the same but for at most one row.
#[must_use = "call .fetch().await (or a fetch_* variant) to run the query"]
pub struct QueryRequest {
    laser: Laser,
    query: Query,
    max_rows: Option<usize>,
    timeout: Option<Duration>,
}

impl QueryRequest {
    fn new(laser: Laser, target: QueryTarget) -> Self {
        Self {
            laser,
            query: Query::builder()
                .execution_id(QueryExecutionId::mint())
                .target(target)
                .deadline_micros(query_deadline(DEFAULT_QUERY_DEADLINE))
                .build(),
            max_rows: None,
            timeout: Some(DEFAULT_QUERY_DEADLINE),
        }
    }

    /// Set the budget of the call and of each page of a walk, counted from
    /// when it starts. The default and longest budget is 25 seconds. A longer
    /// one fails the fetch.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.timeout = Some(deadline);
        self
    }

    /// Set one absolute deadline, in Unix epoch microseconds, for the call and
    /// every page of a walk. A caller that already holds a deadline passes it
    /// through unchanged instead of converting it back into a relative budget.
    pub fn deadline_at_micros(mut self, deadline_at_micros: u64) -> Self {
        self.query.deadline_micros = deadline_at_micros;
        self.timeout = None;
        self
    }

    /// The identity the first request sends and its reply echoes back. Each
    /// later page of a walk takes a fresh one.
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.query.execution_id
    }

    /// Exact-match on an indexed field (point lookup).
    pub fn where_eq(mut self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
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
        self.query.fork = Some(self.laser.resource_name(&fork_id.into()));
        self
    }

    /// Filter rows where `field == value`.
    pub fn filter_eq(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Eq, value)
    }

    /// Filter rows where `field != value`.
    pub fn filter_ne(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Ne, value)
    }

    /// Filter rows where `field > value` (numeric if both parse as numbers, else lexical).
    pub fn filter_gt(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Gt, value)
    }

    /// Filter rows where `field >= value`.
    pub fn filter_gte(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Gte, value)
    }

    /// Filter rows where `field < value`.
    pub fn filter_lt(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Lt, value)
    }

    /// Filter rows where `field <= value`.
    pub fn filter_lte(self, field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.predicate(field, CmpOp::Lte, value)
    }

    /// Filter rows where `field` is one of the given values.
    pub fn filter_in<T: Into<TypedValue>>(
        self,
        field: impl Into<String>,
        values: impl IntoIterator<Item = T>,
    ) -> Self {
        let list = TypedValue::List(values.into_iter().map(Into::into).collect());
        self.predicate(field, CmpOp::In, list)
    }

    /// Filter rows where `field` contains `value` (substring).
    pub fn filter_contains(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Contains, TypedValue::String(value.into()))
    }

    /// Filter rows where `field` starts with `value`.
    pub fn filter_prefix(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Prefix, TypedValue::String(value.into()))
    }

    /// Filter on `agdx.idx.message_type`.
    pub fn message_type(mut self, value: impl Into<String>) -> Self {
        self.query.message_type = Some(value.into());
        self
    }

    /// Filter rows whose `agdx.idx.ts` (epoch micros) falls in `[start, end]`,
    /// both bounds inclusive.
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

    /// Limit the page to `n` rows. Zero or above `MAX_PAGE_SIZE` is rejected
    /// as [`LaserError::Invalid`] before the request is sent.
    pub fn limit(mut self, n: usize) -> Self {
        self.query.page.limit = u32::try_from(n).unwrap_or(u32::MAX);
        self
    }

    /// Skip the first `n` matching rows. An offset above `MAX_QUERY_OFFSET` is
    /// rejected as [`LaserError::Invalid`] before the request is sent.
    pub fn offset(mut self, n: usize) -> Self {
        self.query.page.offset = Some(n as u64);
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
        self.query.page.want_total = true;
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
    /// or `ReadYourWrites`).
    pub fn consistency(mut self, level: Consistency) -> Self {
        self.query.consistency = level;
        self
    }

    /// Lexical relevance search over every text-hinted indexed field. Relevance
    /// lands in each row's `score` exactly as vector distance does. Gated by
    /// the `keyword` capability: refused locally when unadvertised, so an
    /// unaware backend can never silently drop the filter. Rows come in
    /// relevance order and page by offset, so the query takes no `order` and
    /// no total.
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

    /// Project only the named indexed fields into each result row.
    pub fn select_fields<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        self.query.select.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Aggregate: row count, output in the `count` result field.
    pub fn count(self) -> Self {
        self.push_agg(agg_call(AggFunc::Count, None, None, "count"))
    }

    /// Aggregate: sum of `field`, output in the `sum` result field.
    pub fn sum(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Sum, Some(field.into()), None, "sum"))
    }

    /// Aggregate: arithmetic mean of `field`, output in the `avg` result field.
    pub fn avg(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Avg, Some(field.into()), None, "avg"))
    }

    /// Aggregate: minimum of `field`, output in the `min` result field.
    pub fn min(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Min, Some(field.into()), None, "min"))
    }

    /// Aggregate: maximum of `field`, output in the `max` result field.
    pub fn max(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Max, Some(field.into()), None, "max"))
    }

    /// Aggregate: distinct count of `field`, output in the result field
    /// `count_distinct`.
    pub fn count_distinct(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::CountDistinct,
            Some(field.into()),
            None,
            "count_distinct",
        ))
    }

    /// Aggregate: population standard deviation of `field`, output in the
    /// `stddev` result field. Backend-gated (columnar backends only).
    pub fn stddev(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::StdDev,
            Some(field.into()),
            None,
            "stddev",
        ))
    }

    /// Aggregate: the `fraction` quantile of `field` (e.g. 0.95 for p95), output
    /// in the `percentile` result field. Backend-gated (columnar backends only).
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

    /// Bucket the aggregate into tumbling windows `every` wide over the
    /// timestamp `field`. Each result row carries a `window_start` header (the
    /// bucket's lower edge in epoch micros).
    pub fn window(mut self, field: impl Into<String>, every: Duration) -> Self {
        let window = Some(Window {
            field: field.into(),
            every_micros: u64::try_from(every.as_micros()).unwrap_or(u64::MAX),
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
    /// back as typed positional values aligned with the result fields.
    pub fn raw_sql(mut self, dialect: SqlDialect, sql: impl Into<String>) -> Self {
        self.query.raw_sql = Some(RawSql {
            dialect,
            sql: sql.into(),
            params: Vec::new(),
        });
        self
    }

    /// Like [`raw_sql`](Self::raw_sql) but with positional bind params.
    pub fn raw_sql_with<V: Into<TypedValue>>(
        mut self,
        dialect: SqlDialect,
        sql: impl Into<String>,
        params: impl IntoIterator<Item = V>,
    ) -> Self {
        self.query.raw_sql = Some(RawSql {
            dialect,
            sql: sql.into(),
            params: params.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Approximate nearest-neighbour search against the default vector field
    /// (`VECTOR_FIELD` = `"embedding"`). Use [`nearest_in`](Self::nearest_in) to
    /// point at a different field name. The `top_k` nearest rows are the whole
    /// answer, one page with no `order`, offset or total.
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
            top_k: u32::try_from(top_k).unwrap_or(u32::MAX),
        });
        self
    }

    /// Run the query and return the paged result + metadata.
    pub async fn fetch(self) -> Result<QueryResult, LaserError> {
        let (laser, query) = self.stamped();
        laser.execute_query(query).await
    }

    /// Run the query, then deserialize every row's payload into `T` (JSON).
    /// `with_payload()` is implied. Single-page only: use `.stream_typed()` or
    /// `.fetch_all_typed()` if there may be more than `MAX_PAGE_SIZE` matches.
    pub async fn fetch_typed<T: DeserializeOwned>(self) -> Result<Vec<T>, LaserError> {
        let (laser, mut query) = self.stamped();
        query.select.payload = true;
        let result = laser.execute_query(query).await?;
        result
            .rows
            .iter()
            .map(|row| decode_row::<crate::stream::Json, T>(&result.fields, row))
            .collect()
    }

    /// Like `fetch_typed` but caps the result at one row.
    pub async fn fetch_one<T: DeserializeOwned>(self) -> Result<Option<T>, LaserError> {
        let (laser, mut query) = self.stamped();
        query.select.payload = true;
        query.page.limit = 1;
        let result = laser.execute_query(query).await?;
        match result.rows.first() {
            Some(row) => decode_row::<crate::stream::Json, T>(&result.fields, row).map(Some),
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
    /// # #[derive(Deserialize)] struct Reading { host: String }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let readings: Vec<Reading> = laser.query("readings").fetch_typed_with::<Msgpack, _>().await?;
    /// # Ok(()) }
    /// ```
    pub async fn fetch_typed_with<C, T>(self) -> Result<Vec<T>, LaserError>
    where
        C: Decoder<T>,
    {
        let (laser, mut query) = self.stamped();
        query.select.payload = true;
        let result = laser.execute_query(query).await?;
        result
            .rows
            .iter()
            .map(|row| decode_row::<C, T>(&result.fields, row))
            .collect()
    }

    /// Like [`fetch_one`](Self::fetch_one) but decode the row with any
    /// [`Decoder`] instead of JSON.
    pub async fn fetch_one_with<C, T>(self) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        let (laser, mut query) = self.stamped();
        query.select.payload = true;
        query.page.limit = 1;
        let result = laser.execute_query(query).await?;
        match result.rows.first() {
            Some(row) => decode_row::<C, T>(&result.fields, row).map(Some),
            None => Ok(None),
        }
    }

    // The auto-paginating page walk behind `rows()` and `fetch_all()`. Private:
    // an unbounded walk is never handed out directly (the bounded-reads law),
    // and the public `stream` verb belongs to Iggy topology root alone.
    // Pages by offset with the query's `limit` and stops when the worker
    // reports `has_more = false` or an empty page. Aggregate and vector
    // queries are single-page by construction.
    fn page_stream(self) -> QueryStream {
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        QueryStream::new(self.laser, self.query, self.timeout, single_page)
    }

    // Like `page_stream` but each yield is `T` decoded from the row's payload.
    // `with_payload()` is implied, and the publisher must have chained
    // `.inline_payload()` for the bytes to be there.
    fn typed_page_stream<T: DeserializeOwned>(mut self) -> TypedQueryStream<T> {
        self.query.select.payload = true;
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        TypedQueryStream::new(self.laser, self.query, self.timeout, single_page)
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
    /// let mut rows = laser.query("readings").where_eq("status", "degraded").max_rows(1_000).rows()?;
    /// while let Some(row) = rows.next().await? {
    ///     let _ = row;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn rows(self) -> Result<QueryRows, LaserError> {
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
    pub fn rows_typed<T: DeserializeOwned>(self) -> Result<TypedQueryRows<T>, LaserError> {
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
    /// [`rows`](Self::rows) walk when it might not. Each row is bound to its
    /// fields, and the set reports whether the frontier moved between pages.
    pub async fn fetch_all(self) -> Result<QueryRowSet, LaserError> {
        let mut stream = self.page_stream();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(QueryRowSet {
            rows,
            frontier_moved: stream.frontier_moved,
        })
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

    /// Inspect the raw `Query` the builder produced, with its deadline
    /// counted from now (debugging).
    pub fn into_query(self) -> Query {
        self.stamped().1
    }

    // The query a call sends now: a relative budget becomes a deadline counted
    // from this instant.
    fn stamped(mut self) -> (Laser, Query) {
        if let Some(timeout) = self.timeout {
            self.query.deadline_micros = query_deadline(timeout);
        }
        (self.laser, self.query)
    }

    fn predicate(self, field: impl Into<String>, op: CmpOp, value: impl Into<TypedValue>) -> Self {
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

impl From<QueryRequest> for Query {
    fn from(request: QueryRequest) -> Self {
        request.into_query()
    }
}

fn query_deadline(timeout: Duration) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.saturating_add(timeout)
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn now_micros() -> u64 {
    query_deadline(Duration::ZERO)
}

/// How long to wait for the reply to a query due at `deadline_micros`.
fn query_reply_wait(deadline_micros: u64, now_micros: u64) -> Result<Duration, LaserError> {
    let remaining = Duration::from_micros(deadline_micros.saturating_sub(now_micros));
    if remaining > DEFAULT_QUERY_DEADLINE {
        return Err(LaserError::Invalid(format!(
            "the query deadline is {}s away, the longest allowed is {}s",
            remaining.as_secs(),
            DEFAULT_QUERY_DEADLINE.as_secs()
        )));
    }
    Ok(remaining + QUERY_REPLY_GRACE)
}

fn decode_row<C, T>(fields: &[laser_wire::schema::LogicalField], row: &Row) -> Result<T, LaserError>
where
    C: Decoder<T>,
{
    let payload_index = fields
        .iter()
        .position(|field| field.name == ORIGINAL_PAYLOAD_FIELD_NAME)
        .ok_or_else(|| {
            LaserError::from(laser_wire::error::DecodeError::MissingPayload(
                "query result does not contain the original payload field",
            ))
        })?;
    let value = row.values.get(payload_index).ok_or_else(|| {
        LaserError::Protocol("query row is shorter than its declared result schema".to_owned())
    })?;
    match value {
        TypedValue::Binary(value) => C::decode(&value.0).map_err(LaserError::from),
        TypedValue::Null => Err(LaserError::from(
            laser_wire::error::DecodeError::MissingPayload("query row has no original payload"),
        )),
        _ => Err(LaserError::Protocol(
            "query original payload field is not binary".to_owned(),
        )),
    }
}

/// The auto-paginating row walk behind [`QueryRequest::rows`] and
/// [`QueryRequest::fetch_all`]. Holds the `Query` and refills its buffer by
/// moving its offset past the rows already read when a local page drains,
/// until the worker reports `has_more = false` or returns an empty page. The
/// empty-page guard rules out an infinite loop if the worker skews on
/// `has_more`.
struct QueryStream {
    laser: Laser,
    query: Query,
    timeout: Option<Duration>,
    pages_sent: usize,
    finished: bool,
    // Aggregate and vector queries do not page, so the stream fetches once and
    // stops regardless of `has_more`.
    single_page: bool,
    buffer: std::vec::IntoIter<QueryRow>,
    // The first page's frontier, which every later page is compared with.
    frontier: Option<Vec<QueryFrontier>>,
    frontier_moved: bool,
}

impl QueryStream {
    fn new(laser: Laser, query: Query, timeout: Option<Duration>, single_page: bool) -> Self {
        Self {
            laser,
            query,
            timeout,
            pages_sent: 0,
            finished: false,
            single_page,
            buffer: Vec::new().into_iter(),
            frontier: None,
            frontier_moved: false,
        }
    }

    // Yield the next row, fetching the next page if the local buffer is empty.
    // Returns `Ok(None)` after the final row of the last page.
    async fn next(&mut self) -> Result<Option<QueryRow>, LaserError> {
        if let Some(row) = self.buffered() {
            return Ok(Some(row));
        }
        if self.finished {
            return Ok(None);
        }
        let query = self.page_query();
        let page = self.laser.execute_query(query).await?;
        self.absorb(page);
        Ok(self.buffered())
    }

    fn buffered(&mut self) -> Option<QueryRow> {
        self.buffer.next()
    }

    // Take a fetched page: move the offset past its rows, compare its frontier
    // with the first page's, and buffer its rows.
    fn absorb(&mut self, page: QueryResult) {
        self.finished = self.single_page || page_ends_walk(page.rows.len(), &page.page);
        self.query.page.offset = Some(next_offset(self.query.page.offset, page.rows.len()));
        match &self.frontier {
            None => self.frontier = Some(page.context.frontier),
            Some(first) if frontier_differs(first, &page.context.frontier) => {
                self.frontier_moved = true;
            }
            Some(_) => {}
        }
        self.buffer = QueryRow::from_page(page.fields, page.rows).into_iter();
    }

    // The request for the next page: its own deadline counted from now under a
    // relative budget, and a fresh execution id after the first page.
    fn page_query(&mut self) -> Query {
        let mut query = self.query.clone();
        if let Some(timeout) = self.timeout {
            query.deadline_micros = query_deadline(timeout);
        }
        if self.pages_sent > 0 {
            query.execution_id = QueryExecutionId::mint();
        }
        self.pages_sent += 1;
        query
    }
}

// Whether a fetched page is the last one the walk reads. An empty page and a
// page without more rows both end it, so a worker that skews on `has_more`
// cannot make the walk repeat forever.
fn page_ends_walk(fetched: usize, page: &laser_wire::query::Page) -> bool {
    fetched == 0 || !page.has_more
}

// The offset of the page after one that started at `sent` and held `fetched`
// rows.
fn next_offset(sent: Option<u64>, fetched: usize) -> u64 {
    sent.unwrap_or(0)
        .saturating_add(u64::try_from(fetched).unwrap_or(u64::MAX))
}

// Whether two pages were served at different frontiers. The plane names each
// partition once, so equal length and containment is set equality.
fn frontier_differs(first: &[QueryFrontier], later: &[QueryFrontier]) -> bool {
    first.len() != later.len() || later.iter().any(|position| !first.contains(position))
}

/// The bounded row walk returned by [`QueryRequest::rows`]: the auto-paginating
/// stream under the explicit `max_rows` ceiling. Yields `Ok(None)` at the cap
/// or after the last page, whichever comes first.
pub struct QueryRows {
    stream: QueryStream,
    remaining: usize,
}

impl QueryRows {
    /// Yield the next row, or `Ok(None)` at the ceiling or after the last page.
    pub async fn next(&mut self) -> Result<Option<QueryRow>, LaserError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = self.stream.next().await?;
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }

    /// Whether a page after the first was served at another frontier, so rows
    /// may repeat or be missing.
    pub fn frontier_moved(&self) -> bool {
        self.stream.frontier_moved
    }
}

/// One query row bound to the fields of the page it came on, read by field name.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRow {
    fields: Arc<[LogicalField]>,
    row: Row,
}

impl QueryRow {
    /// Every row of one page, each bound to the page's fields.
    pub fn from_page(fields: Vec<LogicalField>, rows: Vec<Row>) -> Vec<Self> {
        let fields: Arc<[LogicalField]> = Arc::from(fields);
        rows.into_iter()
            .map(|row| Self {
                fields: fields.clone(),
                row,
            })
            .collect()
    }

    /// The fields of the page this row came on, in value order.
    pub fn fields(&self) -> &[LogicalField] {
        &self.fields
    }

    /// The tagged values, in field order.
    pub fn values(&self) -> &[TypedValue] {
        &self.row.values
    }

    /// Backend-native vector distance or lexical rank.
    pub fn score(&self) -> Option<f32> {
        self.row.score
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }

    pub fn value(&self, field: &str) -> Option<&TypedValue> {
        self.field_index(field)
            .and_then(|index| self.row.values.get(index))
    }

    pub fn value_text(&self, field: &str) -> Option<String> {
        self.value(field).map(TypedValue::diagnostic_text)
    }

    pub fn value_u64(&self, field: &str) -> Option<u64> {
        self.value(field).and_then(TypedValue::as_u64)
    }

    pub fn value_i64(&self, field: &str) -> Option<i64> {
        self.value(field).and_then(TypedValue::as_i64)
    }

    /// The positional wire row.
    pub fn row(&self) -> &Row {
        &self.row
    }
}

impl From<QueryRow> for Row {
    fn from(row: QueryRow) -> Self {
        row.row
    }
}

/// Every row [`QueryRequest::fetch_all`] read, and whether a page after the
/// first was served at another frontier, so rows may repeat or be missing.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRowSet {
    pub rows: Vec<QueryRow>,
    pub frontier_moved: bool,
}

impl IntoIterator for QueryRowSet {
    type Item = QueryRow;
    type IntoIter = std::vec::IntoIter<QueryRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// The typed sibling of [`QueryStream`], behind [`QueryRequest::rows_typed`]
/// and [`QueryRequest::fetch_all_typed`]. Each `.next().await` decodes the
/// next row's payload into `T`.
struct TypedQueryStream<T> {
    inner: QueryStream,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> TypedQueryStream<T> {
    fn new(laser: Laser, query: Query, timeout: Option<Duration>, single_page: bool) -> Self {
        Self {
            inner: QueryStream::new(laser, query, timeout, single_page),
            _marker: std::marker::PhantomData,
        }
    }

    // Yield the next decoded value, or `Ok(None)` after the last page.
    async fn next(&mut self) -> Result<Option<T>, LaserError> {
        match self.inner.next().await? {
            Some(row) => decode_row::<crate::stream::Json, T>(row.fields(), row.row()).map(Some),
            None => Ok(None),
        }
    }
}

/// The bounded typed row walk returned by [`QueryRequest::rows_typed`]: the
/// auto-paginating stream under the explicit `max_rows` ceiling, decoding each
/// row's payload into `T`. Yields `Ok(None)` at the cap or after the last
/// page, whichever comes first.
pub struct TypedQueryRows<T> {
    stream: TypedQueryStream<T>,
    remaining: usize,
}

impl<T: DeserializeOwned> TypedQueryRows<T> {
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

    /// Whether a page after the first was served at another frontier, so rows
    /// may repeat or be missing.
    pub fn frontier_moved(&self) -> bool {
        self.stream.inner.frontier_moved
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "graph", feature = "fork"))]
    #[tokio::test]
    async fn given_owned_managed_handles_when_spawned_then_should_run_on_their_own_tasks() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default())
            .with_capabilities(crate::capabilities::Capabilities::OPEN);
        let query = tokio::spawn(laser.query("readings").fetch());
        let graph = tokio::spawn(laser.graph("knowledge").fetch());
        let fork = tokio::spawn(laser.fork("experiment").create().send());
        assert!(
            query
                .await
                .expect("the query task joins")
                .is_err_and(|error| error.is_unsupported())
        );
        assert!(
            graph
                .await
                .expect("the graph task joins")
                .is_err_and(|error| error.is_unsupported())
        );
        assert!(
            fork.await
                .expect("the fork task joins")
                .is_err_and(|error| error.is_unsupported())
        );
    }

    #[test]
    fn given_query_deadlines_when_waiting_then_should_cap_them_and_add_the_grace() {
        let now = 1_000_000_000;
        assert_eq!(
            query_reply_wait(now + 5_000_000, now).unwrap(),
            Duration::from_secs(5) + QUERY_REPLY_GRACE
        );
        assert_eq!(query_reply_wait(now - 1, now).unwrap(), QUERY_REPLY_GRACE);
        assert!(query_reply_wait(now + DEFAULT_QUERY_DEADLINE.as_micros() as u64, now).is_ok());
        assert!(matches!(
            query_reply_wait(now + 60_000_000, now),
            Err(LaserError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn given_an_over_cap_limit_when_executed_then_should_be_invalid_before_any_round_trip() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let query = laser
            .query("readings")
            .limit(crate::query::MAX_PAGE_SIZE + 1)
            .into_query();
        assert!(matches!(
            laser.execute_query(query).await,
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_default_stream_when_querying_then_should_scope_the_index_and_the_fork() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default())
            .with_default_stream("acme");
        let query = laser.query("readings").fork("experiment").into_query();
        assert_eq!(
            query.target,
            QueryTarget::operational("stream:acme/readings")
        );
        assert_eq!(query.fork.as_deref(), Some("stream:acme/experiment"));
        let explicit = laser
            .query_target(QueryTarget::operational("readings"))
            .into_query();
        assert_eq!(
            explicit.target,
            QueryTarget::operational("stream:acme/readings")
        );
        let bare = laser
            .with_resource_naming(crate::laser::ResourceNaming::Bare)
            .query("readings")
            .into_query()
            .target;
        assert_eq!(bare, QueryTarget::operational("readings"));
    }

    #[test]
    fn given_a_page_when_paging_then_should_end_only_on_an_empty_or_last_page() {
        let page = |has_more: bool| laser_wire::query::Page {
            offset: Some(0),
            limit: 2,
            total: None,
            has_more,
        };
        assert!(!page_ends_walk(2, &page(true)));
        assert!(page_ends_walk(0, &page(true)));
        assert!(page_ends_walk(2, &page(false)));
    }

    #[test]
    fn given_a_read_page_when_paging_then_should_move_the_offset_past_its_rows() {
        assert_eq!(next_offset(None, 2), 2);
        assert_eq!(next_offset(Some(40), 20), 60);
        assert_eq!(next_offset(Some(u64::MAX), 1), u64::MAX);
    }

    #[test]
    fn given_a_walk_when_paging_then_should_give_every_page_its_own_deadline_and_execution_id() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let request = laser.query("readings").deadline(Duration::from_secs(5));
        let first_id = request.execution_id();
        let mut stream = request.page_stream();
        std::thread::sleep(Duration::from_millis(2));
        let started = now_micros();
        let first = stream.page_query();
        let second = stream.page_query();
        assert_eq!(first.execution_id, first_id);
        assert_ne!(second.execution_id, first.execution_id);
        assert!(first.deadline_micros >= started + 5_000_000);
        assert!(second.deadline_micros >= first.deadline_micros);
    }

    #[test]
    fn given_walked_pages_when_read_then_should_bind_each_row_to_its_fields_and_report_a_moved_frontier()
     {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let mut stream = laser.query("readings").page_stream();
        stream.absorb(walked_page(1, 5, true));
        let first = stream.buffered().expect("the first page holds a row");
        assert_eq!(first.value_text("id").as_deref(), Some("o-1"));
        assert_eq!(first.field_index("id"), Some(0));
        assert_eq!(first.value("absent"), None);
        assert_eq!(first.fields()[0].name, "id");
        assert!(!stream.frontier_moved);
        stream.absorb(walked_page(2, 5, true));
        assert_eq!(
            stream.buffered().and_then(|row| row.value_text("id")),
            Some("o-2".to_owned())
        );
        assert!(!stream.frontier_moved, "the same frontier has not moved");
        stream.absorb(walked_page(3, 6, false));
        assert!(stream.frontier_moved, "a later applied offset moved it");
        assert!(stream.finished);
    }

    #[test]
    fn given_a_relative_deadline_when_the_query_is_taken_later_then_should_count_from_then() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let request = laser.query("readings").deadline(Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(2));
        let started = now_micros();
        assert!(request.into_query().deadline_micros >= started + 5_000_000);
    }

    #[test]
    fn given_an_absolute_deadline_when_set_then_should_carry_it_unchanged() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let query = laser
            .query("readings")
            .deadline_at_micros(1_700_000_000_000_000)
            .into_query();
        assert_eq!(query.deadline_micros, 1_700_000_000_000_000);
    }

    #[test]
    fn given_a_window_duration_when_bucketing_then_should_send_its_width_in_micros() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let query = laser
            .query("readings")
            .count()
            .window("ts", Duration::from_millis(1_500))
            .into_query();
        let window = query
            .aggregate
            .and_then(|aggregate| aggregate.window)
            .expect("the aggregate carries the window");
        assert_eq!(window.every_micros, 1_500_000);
    }

    // One page holding the row `o-<row>` with `has_more`, served at the applied
    // offset `applied` of `shop/orders` partition 0.
    fn walked_page(row: u32, applied: u64, has_more: bool) -> QueryResult {
        QueryResult {
            fields: vec![LogicalField {
                id: 1,
                name: "id".to_owned(),
                required: false,
                field_type: laser_wire::schema::LogicalType::String,
                doc: None,
            }],
            rows: vec![Row {
                values: vec![TypedValue::String(format!("o-{row}"))],
                score: None,
            }],
            page: laser_wire::query::Page {
                offset: Some(u64::from(row) - 1),
                limit: 1,
                total: None,
                has_more,
            },
            context: crate::query::QueryContext {
                execution_id: QueryExecutionId::from_u128(1),
                engine: crate::query::QueryEngine {
                    name: "embedded".to_owned(),
                    version: "1".to_owned(),
                    dialect: None,
                },
                resolved_target: crate::query::ResolvedQueryTarget::Operational {
                    index: "readings".to_owned(),
                    backend_resource_id: laser_wire::destination::BackendResourceId::from_u128(1),
                    backend_generation: 1,
                    runtime_configuration_revision: 1,
                },
                requested_consistency: Consistency::Eventual,
                delivered_consistency: Consistency::Eventual,
                truncated: false,
                elapsed_micros: 1,
                scanned_bytes: 0,
                produced_bytes: 0,
                row_count: 1,
                frontier: vec![QueryFrontier {
                    stream: "shop".to_owned(),
                    topic: "orders".to_owned(),
                    partition_id: 0,
                    topic_created_at_micros: None,
                    applied_offset: applied,
                }],
            },
        }
    }
}
