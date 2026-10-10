use crate::error::LaserError;
use crate::laser::Laser;
use crate::query::{
    AGDX_QUERY_CODE, AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch,
    QUERY_OP_VERSION, Query, QueryError, QueryExecutionId, QueryExecutionStatus, QueryResult,
    QueryTarget, RawSql, Row, SnapshotSelector, Sort, SqlDialect, TextQuery, TypedValue,
    VECTOR_FIELD, VectorQuery, Window,
};
use crate::stream::Decoder;
use crate::types::{ConversationId, MintUlid};
use laser_wire::framing::encode_named;
use laser_wire::query::{
    QueryCancelEnvelope, QueryCancelReply, QueryEnvelope, QueryPageEnvelope, QueryReply,
    QueryStatusEnvelope, QueryStatusReply,
};
use laser_wire::schema::ORIGINAL_PAYLOAD_FIELD_NAME;
use laser_wire::validate::Validate;
use serde::de::DeserializeOwned;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_QUERY_DEADLINE: Duration = Duration::from_secs(30);

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
    pub fn query<'a>(&'a self, index: &'a str) -> QueryRequest<'a> {
        QueryRequest::new(self, QueryTarget::operational(self.resource_name(index)))
    }

    /// Start a query against an explicit operational or lakehouse target. An
    /// operational index is sent as [`resource_name`](Self::resource_name)
    /// names it.
    pub fn query_target(&self, target: QueryTarget) -> QueryRequest<'_> {
        let target = match target {
            QueryTarget::Operational { index } => {
                QueryTarget::operational(self.resource_name(&index))
            }
            other => other,
        };
        QueryRequest::new(self, target)
    }

    /// Start a query against one destination generation.
    pub fn query_lakehouse(
        &self,
        destination_id: laser_wire::destination::DestinationId,
        destination_generation: u64,
    ) -> QueryRequest<'_> {
        self.query_target(QueryTarget::Lakehouse {
            destination_id,
            destination_generation,
            snapshot: None,
        })
    }

    /// Lower-level: execute a pre-built `Query` and return the raw paged result.
    /// Most callers want `query(index).where_eq(...).fetch().await` instead.
    ///
    /// Query requires `laser-plane` in Laser Stack or LaserData Cloud. Against
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
            QueryReply::Ok(result) => validate_query_result(*result, execution_id),
            QueryReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Retrieve the next cursor page for an executing query.
    pub async fn query_page(
        &self,
        execution_id: QueryExecutionId,
        cursor: impl Into<String>,
        deadline_micros: u64,
    ) -> Result<QueryResult, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.query.cursor_paging {
            return Err(LaserError::unsupported_feature(
                "query",
                "cursor_paging",
                "query cursor paging is not advertised by this deployment",
            ));
        }
        let request = QueryPageEnvelope::new(execution_id, cursor, deadline_micros);
        request.validate()?;
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self
            .send_raw_with_response(crate::query::AGDX_QUERY_PAGE_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<QueryReply>(&payload)? {
            QueryReply::Ok(result) => validate_query_result(*result, execution_id),
            QueryReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query page: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Cancel an executing query by its unguessable identity.
    pub async fn cancel_query(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<QueryExecutionStatus, LaserError> {
        if !self.capabilities().await.query.cancellation {
            return Err(LaserError::unsupported_feature(
                "query",
                "cancellation",
                "query cancellation is not advertised by this deployment",
            ));
        }
        let request = QueryCancelEnvelope::new(execution_id);
        request.validate()?;
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self
            .send_raw_with_response(crate::query::AGDX_QUERY_CANCEL_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<QueryCancelReply>(&payload)? {
            QueryCancelReply::Ok(status) => validate_query_status(status, execution_id),
            QueryCancelReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query cancellation: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Read the current state of an executing or recently completed query.
    pub async fn query_status(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<QueryExecutionStatus, LaserError> {
        if !self.capabilities().await.query.execution_status {
            return Err(LaserError::unsupported_feature(
                "query",
                "execution_status",
                "query execution status is not advertised by this deployment",
            ));
        }
        let request = QueryStatusEnvelope::new(execution_id);
        request.validate()?;
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self
            .send_raw_with_response(crate::query::AGDX_QUERY_STATUS_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<QueryStatusReply>(&payload)? {
            QueryStatusReply::Ok(status) => validate_query_status(status, execution_id),
            QueryStatusReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query status: unknown reply variant".to_owned(),
            )),
        }
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

fn validate_query_status(
    status: QueryExecutionStatus,
    execution_id: QueryExecutionId,
) -> Result<QueryExecutionStatus, LaserError> {
    status.validate()?;
    if status.execution_id != execution_id {
        return Err(LaserError::Protocol(
            "query status execution id does not match the request".to_owned(),
        ));
    }
    Ok(status)
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
    fn new(laser: &'a Laser, target: QueryTarget) -> Self {
        Self {
            laser,
            query: Query::builder()
                .execution_id(crate::query::QueryExecutionId::mint())
                .target(target)
                .deadline_micros(query_deadline(DEFAULT_QUERY_DEADLINE))
                .build(),
            max_rows: None,
        }
    }

    /// Set the absolute execution budget from now. The deadline applies to all
    /// pages under this query identity and is never extended by the server.
    pub fn deadline(mut self, timeout: Duration) -> Self {
        self.query.deadline_micros = query_deadline(timeout);
        self
    }

    /// Replace the absolute execution deadline, in Unix epoch microseconds.
    /// A caller that already holds a deadline passes it through unchanged
    /// instead of converting it back into a relative budget.
    pub fn deadline_micros(mut self, deadline_micros: u64) -> Self {
        self.query.deadline_micros = deadline_micros;
        self
    }

    /// The identity shared by execution, cursor pages, status, and cancellation.
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.query.execution_id
    }

    /// Read the current execution state for this query identity.
    pub async fn status(&self) -> Result<QueryExecutionStatus, LaserError> {
        self.laser.query_status(self.query.execution_id).await
    }

    /// Request cancellation for this query identity and return its observed state.
    pub async fn cancel(&self) -> Result<QueryExecutionStatus, LaserError> {
        self.laser.cancel_query(self.query.execution_id).await
    }

    /// Select one exact Iceberg snapshot for a lakehouse query.
    pub fn at_snapshot(mut self, snapshot_id: i64) -> Result<Self, LaserError> {
        let QueryTarget::Lakehouse { snapshot, .. } = &mut self.query.target else {
            return Err(LaserError::Invalid(
                "snapshot selection requires a lakehouse query target".to_owned(),
            ));
        };
        *snapshot = Some(SnapshotSelector::SnapshotId(snapshot_id));
        Ok(self)
    }

    /// Select the retained Iceberg snapshot current at a timestamp.
    pub fn at_timestamp_micros(mut self, timestamp_micros: i64) -> Result<Self, LaserError> {
        let QueryTarget::Lakehouse { snapshot, .. } = &mut self.query.target else {
            return Err(LaserError::Invalid(
                "timestamp selection requires a lakehouse query target".to_owned(),
            ));
        };
        *snapshot = Some(SnapshotSelector::TimestampMicros(timestamp_micros));
        Ok(self)
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

    /// Resume from an opaque cursor that the server returned in a previous
    /// page. Replaces any [`offset`](Self::offset).
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.query.page.cursor = Some(cursor.into());
        self.query.page.offset = None;
        self
    }

    /// Skip the first `n` matching rows.
    pub fn offset(mut self, n: usize) -> Self {
        self.query.page.offset = Some(n as u64);
        self.query.page.cursor = None;
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
            top_k: u32::try_from(top_k).unwrap_or(u32::MAX),
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
            .map(|row| decode_row::<crate::stream::Json, T>(&result.fields, row))
            .collect()
    }

    /// Like `fetch_typed` but caps the result at one row.
    pub async fn fetch_one<T: DeserializeOwned>(mut self) -> Result<Option<T>, LaserError> {
        self.query.select.payload = true;
        self.query.page.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
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
    pub async fn fetch_typed_with<C, T>(mut self) -> Result<Vec<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        let result = self.laser.execute_query(self.query).await?;
        result
            .rows
            .iter()
            .map(|row| decode_row::<C, T>(&result.fields, row))
            .collect()
    }

    /// Like [`fetch_one`](Self::fetch_one) but decode the row with any
    /// [`Decoder`] instead of JSON.
    pub async fn fetch_one_with<C, T>(mut self) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        self.query.page.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
        match result.rows.first() {
            Some(row) => decode_row::<C, T>(&result.fields, row).map(Some),
            None => Ok(None),
        }
    }

    // The auto-paginating page walk behind `rows()` and `fetch_all()`. Private:
    // an unbounded walk is never handed out directly (the bounded-reads law),
    // and the public `stream` verb belongs to Iggy topology root alone.
    // Auto-paginates with `limit` (or 100 if unset) and stops when the worker
    // reports `has_more = false`, an empty page, or no new cursor.
    // Aggregate and vector queries are single-page by construction: `offset`
    // is not a meaningful cursor for either shape.
    fn page_stream(mut self) -> QueryStream<'a> {
        if self.query.page.limit == 0 {
            self.query.page.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE as u32;
        }
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        QueryStream::new(self.laser, self.query, single_page)
    }

    // Like `page_stream` but each yield is `T` decoded from the row's payload.
    // `with_payload()` is implied, and the publisher must have chained
    // `.inline_payload()` for the bytes to be there.
    fn typed_page_stream<T: DeserializeOwned>(mut self) -> TypedQueryStream<'a, T> {
        self.query.select.payload = true;
        if self.query.page.limit == 0 {
            self.query.page.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE as u32;
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
    /// let mut rows = laser.query("readings").where_eq("status", "degraded").max_rows(1_000).rows()?;
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

impl<'a> From<QueryRequest<'a>> for Query {
    fn from(request: QueryRequest<'a>) -> Self {
        request.query
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
/// continuing it with the server-issued opaque cursor when a local page drains,
/// until the worker reports `has_more = false`, returns an empty page, or
/// returns no new cursor, whichever comes first. The empty-page and cursor
/// guards rule out an infinite loop if the worker skews on `has_more`.
struct QueryStream<'a> {
    laser: &'a Laser,
    query: Query,
    finished: bool,
    // Aggregate / vector queries do not support cursor continuation, so the
    // stream fetches once and stops regardless of `has_more`.
    single_page: bool,
    fields: Vec<laser_wire::schema::LogicalField>,
    buffer: std::vec::IntoIter<Row>,
}

impl<'a> QueryStream<'a> {
    fn new(laser: &'a Laser, query: Query, single_page: bool) -> Self {
        Self {
            laser,
            query,
            finished: false,
            single_page,
            fields: Vec::new(),
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
        self.finished = self.single_page
            || page_ends_walk(
                page.rows.len(),
                &page.page,
                self.query.page.cursor.as_deref(),
            );
        self.query.page.cursor = page.page.next_cursor.clone();
        self.query.page.offset = None;
        self.fields = page.fields;
        self.buffer = page.rows.into_iter();
        Ok(self.buffer.next())
    }
}

// Whether a fetched page is the last one the walk reads. An empty page, a
// page without more rows, and a page that reports more rows but gives no new
// cursor all end it. Continuing without a cursor would restart at page one and
// continuing with the same cursor would repeat the page, both forever.
fn page_ends_walk(fetched: usize, page: &laser_wire::query::Page, sent: Option<&str>) -> bool {
    fetched == 0
        || !page.has_more
        || page
            .next_cursor
            .as_deref()
            .is_none_or(|next| Some(next) == sent)
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
            Some(row) => decode_row::<crate::stream::Json, T>(&self.inner.fields, &row).map(Some),
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn given_more_rows_without_a_new_cursor_when_paging_then_should_end_the_walk() {
        let page = |has_more: bool, next_cursor: Option<&str>| laser_wire::query::Page {
            offset: None,
            limit: 2,
            total: None,
            has_more,
            next_cursor: next_cursor.map(str::to_owned),
        };
        assert!(!page_ends_walk(2, &page(true, Some("b")), Some("a")));
        assert!(page_ends_walk(2, &page(true, None), Some("a")));
        assert!(page_ends_walk(2, &page(true, None), None));
        assert!(page_ends_walk(2, &page(true, Some("a")), Some("a")));
        assert!(page_ends_walk(0, &page(true, Some("b")), Some("a")));
        assert!(page_ends_walk(2, &page(false, Some("b")), Some("a")));
    }

    #[test]
    fn given_a_cursor_when_set_then_should_replace_the_offset() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let query = laser
            .query("readings")
            .offset(20)
            .cursor("c-2")
            .into_query();
        assert_eq!(query.page.cursor.as_deref(), Some("c-2"));
        assert_eq!(query.page.offset, None);
        let query = laser.query("readings").cursor("c-2").offset(5).into_query();
        assert_eq!(query.page.cursor, None);
        assert_eq!(query.page.offset, Some(5));
    }

    #[test]
    fn given_an_absolute_deadline_when_set_then_should_carry_it_unchanged() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default());
        let query = laser
            .query("readings")
            .deadline_micros(1_700_000_000_000_000)
            .into_query();
        assert_eq!(query.deadline_micros, 1_700_000_000_000_000);
    }
}
