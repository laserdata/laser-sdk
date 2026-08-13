use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, FIELD_MESSAGE_TYPE, FIELD_TS, Filter,
    Page, Predicate, Query, QueryContext, QueryEngine as QueryEngineEvidence, QueryResult,
    QueryTarget, ResolvedQueryTarget, Row, TypedValue, WINDOW_START, Window,
};
use laser_sdk::wire::destination::BackendResourceId;
use laser_sdk::wire::schema::{LogicalField, LogicalType};
use std::collections::{BTreeMap, BTreeSet, HashMap};

type StoredRow = BTreeMap<String, TypedValue>;

#[derive(Default)]
pub struct QueryEngine {
    indexes: HashMap<String, Vec<StoredRow>>,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, index: &str, row: StoredRow) {
        self.indexes.entry(index.to_owned()).or_default().push(row);
    }

    pub fn row(fields: &[(&str, &str)]) -> StoredRow {
        fields
            .iter()
            .map(|(name, value)| {
                let value = match *name {
                    "latency_ms" | "seq" | "count" => {
                        TypedValue::Long(value.parse().expect("numeric reference query field"))
                    }
                    _ => TypedValue::String((*value).to_owned()),
                };
                ((*name).to_owned(), value)
            })
            .collect()
    }

    pub fn execute(&self, query: &Query) -> QueryResult {
        let index = match &query.target {
            QueryTarget::Operational { index } => index,
            QueryTarget::Lakehouse { .. } => return empty_result(query, "lakehouse"),
            _ => return empty_result(query, "unsupported"),
        };
        let rows = self
            .indexes
            .get(index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut matched: Vec<StoredRow> = rows
            .iter()
            .filter(|row| matches_query(row, query))
            .cloned()
            .collect();

        if let Some(aggregate) = &query.aggregate {
            return render_result(query, index, run_aggregate(&matched, aggregate));
        }

        for sort in query.order.iter().rev() {
            matched.sort_by(|left, right| {
                let ordering = compare_values(left.get(&sort.field), right.get(&sort.field));
                match sort.dir {
                    Dir::Asc => ordering,
                    Dir::Desc => ordering.reverse(),
                }
            });
        }

        render_result(query, index, matched)
    }
}

fn matches_query(row: &StoredRow, query: &Query) -> bool {
    let by_key = query
        .by_key
        .iter()
        .all(|key_match| row.get(&key_match.field) == Some(&key_match.value));
    let predicates = query
        .filter
        .as_ref()
        .is_none_or(|filter| filter_matches(row, filter));
    let message_type = query.message_type.as_ref().is_none_or(|expected| {
        row.get(FIELD_MESSAGE_TYPE) == Some(&TypedValue::String(expected.clone()))
    });
    let time_range = query.time_range.is_none_or(|(start, end)| {
        row.get(FIELD_TS)
            .and_then(as_i64)
            .and_then(|value| u64::try_from(value).ok())
            .is_some_and(|timestamp| timestamp >= start && timestamp <= end)
    });
    by_key && predicates && message_type && time_range
}

fn filter_matches(row: &StoredRow, filter: &Filter) -> bool {
    match filter {
        Filter::All(children) => children.iter().all(|child| filter_matches(row, child)),
        Filter::Any(children) => children.iter().any(|child| filter_matches(row, child)),
        Filter::Not(inner) => !filter_matches(row, inner),
        Filter::Pred(predicate) => predicate_matches(row, predicate),
    }
}

fn predicate_matches(row: &StoredRow, predicate: &Predicate) -> bool {
    let Some(field_value) = row.get(&predicate.field) else {
        return false;
    };
    match (&predicate.op, &predicate.value) {
        (CmpOp::In, TypedValue::List(values)) => values.contains(field_value),
        (CmpOp::Contains, TypedValue::String(value)) => as_string(field_value).contains(value),
        (CmpOp::Prefix, TypedValue::String(value)) => as_string(field_value).starts_with(value),
        (operator, value) => compare_scalar(field_value, operator, value),
    }
}

fn compare_scalar(left: &TypedValue, operator: &CmpOp, right: &TypedValue) -> bool {
    use std::cmp::Ordering;
    let ordering = match (as_f64(left), as_f64(right)) {
        (Some(left), Some(right)) => left.partial_cmp(&right),
        _ => Some(as_string(left).cmp(&as_string(right))),
    };
    match operator {
        CmpOp::Eq => ordering == Some(Ordering::Equal),
        CmpOp::Ne => ordering != Some(Ordering::Equal),
        CmpOp::Lt => ordering == Some(Ordering::Less),
        CmpOp::Lte => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        CmpOp::Gt => ordering == Some(Ordering::Greater),
        CmpOp::Gte => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        CmpOp::In | CmpOp::Contains | CmpOp::Prefix => false,
    }
}

fn compare_values(left: Option<&TypedValue>, right: Option<&TypedValue>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match (as_f64(left), as_f64(right)) {
            (Some(left), Some(right)) => left.total_cmp(&right),
            _ => as_string(left).cmp(&as_string(right)),
        },
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    }
}

fn as_string(value: &TypedValue) -> String {
    value.diagnostic_text()
}

fn as_i64(value: &TypedValue) -> Option<i64> {
    value.as_i64()
}

fn as_f64(value: &TypedValue) -> Option<f64> {
    match value {
        TypedValue::Int(value) => Some(f64::from(*value)),
        TypedValue::Long(value)
        | TypedValue::TimeMicros(value)
        | TypedValue::TimestampMicros(value)
        | TypedValue::TimestampTzMicros(value) => Some(*value as f64),
        TypedValue::Date(value) => Some(f64::from(*value)),
        TypedValue::Float(value) => Some(f64::from(*value)),
        TypedValue::Double(value) => Some(*value),
        _ => None,
    }
}

fn run_aggregate(matched: &[StoredRow], aggregate: &Aggregate) -> Vec<StoredRow> {
    let mut groups: BTreeMap<Vec<String>, Vec<&StoredRow>> = BTreeMap::new();
    for row in matched {
        let mut key: Vec<String> = aggregate
            .group_by
            .iter()
            .map(|name| row.get(name).map(as_string).unwrap_or_default())
            .collect();
        if let Some(window) = &aggregate.window {
            key.push(window_bucket(row, window));
        }
        groups.entry(key).or_default().push(row);
    }

    groups
        .into_iter()
        .map(|(key, members)| {
            let mut row: StoredRow = aggregate
                .group_by
                .iter()
                .cloned()
                .zip(key.iter().cloned().map(TypedValue::String))
                .collect();
            if aggregate.window.is_some()
                && let Some(bucket) = key.last()
            {
                row.insert(WINDOW_START.to_owned(), TypedValue::String(bucket.clone()));
            }
            for call in &aggregate.funcs {
                row.insert(call.alias.clone(), aggregate_value(&members, call));
            }
            row
        })
        .collect()
}

fn window_bucket(row: &StoredRow, window: &Window) -> String {
    let timestamp = row.get(&window.field).and_then(as_i64).unwrap_or(0);
    let every = window.every_micros.max(1) as i64;
    ((timestamp / every) * every).to_string()
}

fn aggregate_value(members: &[&StoredRow], call: &AggCall) -> TypedValue {
    let numbers = || -> Vec<f64> {
        members
            .iter()
            .filter_map(|row| as_f64(row.get(call.field.as_ref()?)?))
            .collect()
    };
    match call.func {
        AggFunc::Count => TypedValue::Long(members.len() as i64),
        AggFunc::CountDistinct => {
            let distinct: BTreeSet<String> = members
                .iter()
                .filter_map(|row| row.get(call.field.as_ref()?).map(as_string))
                .collect();
            TypedValue::Long(distinct.len() as i64)
        }
        AggFunc::Sum => TypedValue::Double(numbers().iter().sum()),
        AggFunc::Avg => {
            let values = numbers();
            TypedValue::Double(values.iter().sum::<f64>() / values.len().max(1) as f64)
        }
        AggFunc::Min => TypedValue::Double(numbers().into_iter().reduce(f64::min).unwrap_or(0.0)),
        AggFunc::Max => TypedValue::Double(numbers().into_iter().reduce(f64::max).unwrap_or(0.0)),
        AggFunc::StdDev => {
            let values = numbers();
            let mean = values.iter().sum::<f64>() / values.len().max(1) as f64;
            let variance = values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / values.len().max(1) as f64;
            TypedValue::Double(variance.sqrt())
        }
        AggFunc::Percentile => {
            let mut values = numbers();
            values.sort_by(f64::total_cmp);
            let fraction = call.arg.unwrap_or(0.5).clamp(0.0, 1.0);
            let rank = (fraction * values.len().saturating_sub(1) as f64).round() as usize;
            TypedValue::Double(values.get(rank).copied().unwrap_or(0.0))
        }
    }
}

fn render_result(query: &Query, index: &str, matched: Vec<StoredRow>) -> QueryResult {
    let total = matched.len();
    let offset = query.page.offset.unwrap_or(0) as usize;
    let limit = query.page.limit as usize;
    let page_rows: Vec<StoredRow> = matched.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(page_rows.len()) < total;
    let fields = result_fields(&page_rows);
    let rows = page_rows
        .iter()
        .map(|row| Row {
            values: fields
                .iter()
                .map(|field| row.get(&field.name).cloned().unwrap_or(TypedValue::Null))
                .collect(),
            score: None,
        })
        .collect::<Vec<_>>();
    QueryResult {
        fields,
        page: Page {
            offset: Some(offset as u64),
            limit: query.page.limit,
            total: query.page.want_total.then_some(total as u64),
            has_more,
            next_cursor: has_more.then(|| format!("offset:{}", offset + rows.len())),
        },
        context: context(query, index, rows.len()),
        rows,
    }
}

fn result_fields(rows: &[StoredRow]) -> Vec<LogicalField> {
    let names: BTreeSet<String> = rows.iter().flat_map(|row| row.keys().cloned()).collect();
    names
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let field_type = rows
                .iter()
                .filter_map(|row| row.get(&name))
                .find_map(logical_type)
                .unwrap_or(LogicalType::String);
            LogicalField {
                id: index as u32 + 1,
                name,
                required: false,
                field_type,
                doc: None,
            }
        })
        .collect()
}

fn logical_type(value: &TypedValue) -> Option<LogicalType> {
    match value {
        TypedValue::String(_) => Some(LogicalType::String),
        TypedValue::Long(_) => Some(LogicalType::Long),
        TypedValue::Double(_) => Some(LogicalType::Double),
        TypedValue::Null => None,
        _ => Some(LogicalType::String),
    }
}

fn context(query: &Query, index: &str, row_count: usize) -> QueryContext {
    QueryContext {
        execution_id: query.execution_id,
        engine: QueryEngineEvidence {
            name: "bdd-reference".to_owned(),
            version: "1".to_owned(),
            dialect: None,
        },
        resolved_target: ResolvedQueryTarget::Operational {
            index: index.to_owned(),
            backend_resource_id: BackendResourceId::from_u128(1),
            backend_generation: 1,
            runtime_configuration_revision: 1,
        },
        requested_consistency: query.consistency,
        delivered_consistency: query.consistency,
        boundary: None,
        checkpoint_revision: None,
        global_state_revision: None,
        truncated: false,
        elapsed_micros: 0,
        scanned_bytes: 0,
        produced_bytes: 0,
        row_count: row_count as u64,
    }
}

fn empty_result(query: &Query, index: &str) -> QueryResult {
    QueryResult {
        fields: vec![LogicalField {
            id: 1,
            name: "value".to_owned(),
            required: false,
            field_type: LogicalType::String,
            doc: None,
        }],
        rows: Vec::new(),
        page: Page {
            offset: Some(0),
            limit: query.page.limit,
            total: query.page.want_total.then_some(0),
            has_more: false,
            next_cursor: None,
        },
        context: context(query, index, 0),
    }
}

pub fn query_on(index: &str) -> Query {
    Query {
        execution_id: laser_sdk::query::QueryExecutionId::from_u128(1),
        target: QueryTarget::operational(index),
        deadline_micros: 1,
        by_key: Vec::new(),
        message_type: None,
        time_range: None,
        filter: None,
        vector: None,
        text: None,
        order: Vec::new(),
        page: laser_sdk::query::QueryPageRequest::default(),
        aggregate: None,
        having: None,
        distinct: false,
        select: laser_sdk::query::Select::default(),
        fork: None,
        raw_sql: None,
        consistency: Consistency::Eventual,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::query::Sort;

    fn seeded() -> QueryEngine {
        let mut engine = QueryEngine::new();
        for (index, status, latency) in [
            (0, "200", "10"),
            (1, "200", "550"),
            (2, "500", "900"),
            (3, "200", "30"),
        ] {
            engine.insert(
                "api_calls",
                QueryEngine::row(&[
                    ("endpoint", "/v1/items"),
                    ("status", status),
                    ("latency_ms", latency),
                    ("seq", &index.to_string()),
                ]),
            );
        }
        engine
    }

    #[test]
    fn typed_query_filter_returns_only_matching_rows() {
        let engine = seeded();
        let mut query = query_on("api_calls");
        query.filter = Some(Filter::pred("latency_ms", CmpOp::Gt, 500_i64));
        let result = engine.execute(&query);
        assert_eq!(result.rows.len(), 2);
        assert!(result.rows.iter().all(|row| {
            result
                .value_i64(row, "latency_ms")
                .is_some_and(|value| value > 500)
        }));
    }

    #[test]
    fn typed_query_ordering_is_numeric() {
        let engine = seeded();
        let mut query = query_on("api_calls");
        query.order = vec![Sort {
            field: "latency_ms".to_owned(),
            dir: Dir::Desc,
        }];
        let result = engine.execute(&query);
        let latencies: Vec<i64> = result
            .rows
            .iter()
            .filter_map(|row| result.value_i64(row, "latency_ms"))
            .collect();
        assert_eq!(latencies, vec![900, 550, 30, 10]);
    }

    #[test]
    fn cursor_page_reports_exact_total_only_when_requested() {
        let engine = seeded();
        let mut query = query_on("api_calls");
        query.page.limit = 2;
        query.page.want_total = true;
        let result = engine.execute(&query);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.page.total, Some(4));
        assert!(result.page.next_cursor.is_some());
    }

    #[test]
    fn typed_aggregate_counts_each_group() {
        let engine = seeded();
        let mut query = query_on("api_calls");
        query.aggregate = Some(Aggregate {
            group_by: vec!["status".to_owned()],
            funcs: vec![AggCall {
                func: AggFunc::Count,
                field: None,
                arg: None,
                alias: "count".to_owned(),
            }],
            window: None,
        });
        let result = engine.execute(&query);
        let counts: BTreeMap<String, i64> = result
            .rows
            .iter()
            .map(|row| {
                (
                    result.value_text(row, "status").expect("status"),
                    result.value_i64(row, "count").expect("count"),
                )
            })
            .collect();
        assert_eq!(counts["200"], 3);
        assert_eq!(counts["500"], 1);
    }
}
