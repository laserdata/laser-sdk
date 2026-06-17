use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Dir, FIELD_MESSAGE_TYPE, FIELD_TS, Filter, MAX_PAGE_SIZE,
    Page, Predicate, Query, QueryResult, Row, Value, WINDOW_START, Window,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Default)]
pub struct QueryEngine {
    indexes: HashMap<String, Vec<Row>>,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, index: &str, row: Row) {
        self.indexes.entry(index.to_owned()).or_default().push(row);
    }

    pub fn row(fields: &[(&str, &str)]) -> Row {
        let headers = fields
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        Row {
            headers,
            metadata: BTreeMap::new(),
            partition: None,
            offset: None,
            payload: None,
            score: None,
        }
    }

    pub fn execute(&self, query: &Query) -> QueryResult {
        let Some(rows) = self.indexes.get(&query.index) else {
            return QueryResult::default();
        };
        let mut matched: Vec<Row> = rows
            .iter()
            .filter(|row| matches_query(row, query))
            .cloned()
            .collect();

        if let Some(aggregate) = &query.aggregate {
            return run_aggregate(&matched, aggregate);
        }

        for sort in query.order.iter().rev() {
            matched.sort_by(|a, b| {
                let left = a.headers.get(&sort.field).map(String::as_str).unwrap_or("");
                let right = b.headers.get(&sort.field).map(String::as_str).unwrap_or("");
                let ordering = match (left.parse::<f64>(), right.parse::<f64>()) {
                    (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                    _ => left.cmp(right),
                };
                match sort.dir {
                    Dir::Asc => ordering,
                    Dir::Desc => ordering.reverse(),
                }
            });
        }

        let total = matched.len();
        // `0` means a full page (capped at MAX_PAGE_SIZE), mirroring the spec.
        let limit = if query.limit == 0 {
            MAX_PAGE_SIZE
        } else {
            query.limit
        };
        let rows: Vec<Row> = matched
            .into_iter()
            .skip(query.offset)
            .take(limit)
            .map(|mut row| {
                if !query.select.payload {
                    row.payload = None;
                }
                row
            })
            .collect();
        let has_more = query.offset.saturating_add(rows.len()) < total;
        QueryResult {
            rows,
            page: Page {
                offset: query.offset,
                limit,
                total,
                has_more,
            },
        }
    }
}

fn matches_query(row: &Row, query: &Query) -> bool {
    let by_key = query.by_key.iter().all(|key_match| {
        row.headers.get(&key_match.field).map(String::as_str) == Some(key_match.value.as_str())
    });
    let predicates = match &query.filter {
        Some(filter) => filter_matches(row, filter),
        None => true,
    };
    let message_type = match &query.message_type {
        Some(expected) => {
            row.headers.get(FIELD_MESSAGE_TYPE).map(String::as_str) == Some(expected.as_str())
        }
        None => true,
    };
    let time_range = match query.time_range {
        Some((start, end)) => row
            .headers
            .get(FIELD_TS)
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|timestamp| timestamp >= start && timestamp <= end),
        None => true,
    };
    by_key && predicates && message_type && time_range
}

fn filter_matches(row: &Row, filter: &Filter) -> bool {
    match filter {
        Filter::All(children) => children.iter().all(|child| filter_matches(row, child)),
        Filter::Any(children) => children.iter().any(|child| filter_matches(row, child)),
        Filter::Not(inner) => !filter_matches(row, inner),
        Filter::Pred(predicate) => predicate_matches(row, predicate),
    }
}

fn predicate_matches(row: &Row, predicate: &Predicate) -> bool {
    let Some(field_value) = row.headers.get(&predicate.field) else {
        return false;
    };
    match (&predicate.op, &predicate.value) {
        (CmpOp::In, Value::List(values)) => values
            .iter()
            .any(|value| scalar_string(value) == *field_value),
        (CmpOp::Contains, value) => field_value.contains(&scalar_string(value)),
        (CmpOp::Prefix, value) => field_value.starts_with(&scalar_string(value)),
        (operator, value) => compare_scalar(field_value, operator, value),
    }
}

// Compare numerically when the bound value is `Int`/`Float`, otherwise as strings.
fn compare_scalar(field_value: &str, operator: &CmpOp, value: &Value) -> bool {
    use std::cmp::Ordering;
    let ordering = match value {
        Value::Int(_) | Value::Float(_) => match (field_value.parse::<f64>(), scalar_f64(value)) {
            (Ok(left), Some(right)) => left.partial_cmp(&right),
            _ => None,
        },
        _ => Some(field_value.cmp(scalar_string(value).as_str())),
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

fn scalar_string(value: &Value) -> String {
    match value {
        Value::Str(text) => text.clone(),
        Value::Int(number) => number.to_string(),
        Value::Uint(number) => number.to_string(),
        Value::Float(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null | Value::List(_) => String::new(),
    }
}

fn scalar_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(number) => Some(*number as f64),
        Value::Float(number) => Some(*number),
        _ => None,
    }
}

fn run_aggregate(matched: &[Row], aggregate: &Aggregate) -> QueryResult {
    let mut groups: BTreeMap<Vec<String>, Vec<&Row>> = BTreeMap::new();
    for row in matched {
        let mut key: Vec<String> = aggregate
            .group_by
            .iter()
            .map(|name| row.headers.get(name).cloned().unwrap_or_default())
            .collect();
        if let Some(window) = &aggregate.window {
            key.push(window_bucket(row, window));
        }
        groups.entry(key).or_default().push(row);
    }
    let mut rows = Vec::new();
    for (key, members) in groups {
        let mut headers: BTreeMap<String, String> = aggregate
            .group_by
            .iter()
            .cloned()
            .zip(key.iter().cloned())
            .collect();
        if aggregate.window.is_some()
            && let Some(bucket) = key.last()
        {
            headers.insert(WINDOW_START.to_owned(), bucket.clone());
        }
        for call in &aggregate.funcs {
            headers.insert(call.alias.clone(), aggregate_value(&members, call));
        }
        rows.push(Row {
            headers,
            metadata: BTreeMap::new(),
            partition: None,
            offset: None,
            payload: None,
            score: None,
        });
    }
    let total = rows.len();
    QueryResult {
        rows,
        page: Page {
            offset: 0,
            limit: total.max(1),
            total,
            has_more: false,
        },
    }
}

fn window_bucket(row: &Row, window: &Window) -> String {
    let timestamp = row
        .headers
        .get(&window.field)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let every = window.every_micros.max(1) as i64;
    ((timestamp / every) * every).to_string()
}

fn aggregate_value(members: &[&Row], call: &AggCall) -> String {
    let numbers = || -> Vec<f64> {
        members
            .iter()
            .filter_map(|row| {
                let field = call.field.as_ref()?;
                row.headers.get(field)?.parse::<f64>().ok()
            })
            .collect()
    };
    match call.func {
        AggFunc::Count => members.len().to_string(),
        AggFunc::CountDistinct => {
            let distinct: BTreeSet<String> = members
                .iter()
                .filter_map(|row| {
                    let field = call.field.as_ref()?;
                    row.headers.get(field).cloned()
                })
                .collect();
            distinct.len().to_string()
        }
        AggFunc::Sum => numbers().iter().sum::<f64>().to_string(),
        AggFunc::Avg => {
            let values = numbers();
            if values.is_empty() {
                String::new()
            } else {
                (values.iter().sum::<f64>() / values.len() as f64).to_string()
            }
        }
        AggFunc::Min => numbers()
            .into_iter()
            .reduce(f64::min)
            .map(|value| value.to_string())
            .unwrap_or_default(),
        AggFunc::Max => numbers()
            .into_iter()
            .reduce(f64::max)
            .map(|value| value.to_string())
            .unwrap_or_default(),
        AggFunc::StdDev => {
            let values = numbers();
            if values.is_empty() {
                return String::new();
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / values.len() as f64;
            variance.sqrt().to_string()
        }
        AggFunc::Percentile => {
            let mut values = numbers();
            if values.is_empty() {
                return String::new();
            }
            values.sort_by(|left, right| left.total_cmp(right));
            let fraction = call.arg.unwrap_or(0.5).clamp(0.0, 1.0);
            let rank = (fraction * (values.len() - 1) as f64).round() as usize;
            values[rank].to_string()
        }
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
            let row = QueryEngine::row(&[
                ("endpoint", "/v1/items"),
                ("status", status),
                ("latency_ms", latency),
                ("seq", &index.to_string()),
            ]);
            engine.insert("api_calls", row);
        }
        engine
    }

    fn query_on(index: &str) -> Query {
        Query {
            index: index.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn given_rows_with_varied_latency_when_filtered_then_should_return_only_matching() {
        let engine = seeded();
        let result = engine.execute(&Query {
            filter: Some(Filter::Pred(Predicate {
                field: "latency_ms".to_owned(),
                op: CmpOp::Gt,
                value: Value::Int(500),
            })),
            ..query_on("api_calls")
        });
        assert_eq!(result.rows.len(), 2, "two rows exceed 500ms");
        for row in &result.rows {
            let latency: i64 = row.headers["latency_ms"].parse().expect("numeric latency");
            assert!(latency > 500, "row latency {latency} should exceed 500");
        }
    }

    #[test]
    fn given_rows_when_ordered_descending_then_should_sort_numerically_not_lexically() {
        let engine = seeded();
        let result = engine.execute(&Query {
            order: vec![Sort {
                field: "latency_ms".to_owned(),
                dir: Dir::Desc,
            }],
            ..query_on("api_calls")
        });
        let latencies: Vec<i64> = result
            .rows
            .iter()
            .map(|row| row.headers["latency_ms"].parse().expect("numeric latency"))
            .collect();
        assert_eq!(
            latencies,
            vec![900, 550, 30, 10],
            "numeric desc, not lexical"
        );
    }

    #[test]
    fn given_more_rows_than_the_limit_when_queried_then_should_cap_the_page_and_report_full_total()
    {
        let engine = seeded();
        let result = engine.execute(&Query {
            limit: 2,
            ..query_on("api_calls")
        });
        assert_eq!(result.rows.len(), 2, "the page is capped at the limit");
        assert_eq!(result.page.total, 4, "the total counts every match");
        assert!(result.page.has_more, "more rows remain past the page");
    }

    #[test]
    fn given_rows_when_count_aggregated_by_status_then_should_count_each_group() {
        let engine = seeded();
        let result = engine.execute(&Query {
            aggregate: Some(Aggregate {
                group_by: vec!["status".to_owned()],
                funcs: vec![AggCall {
                    func: AggFunc::Count,
                    field: None,
                    arg: None,
                    alias: "count".to_owned(),
                }],
                window: None,
            }),
            ..query_on("api_calls")
        });
        let counts: BTreeMap<String, String> = result
            .rows
            .into_iter()
            .map(|row| (row.headers["status"].clone(), row.headers["count"].clone()))
            .collect();
        assert_eq!(counts["200"], "3", "three 200s");
        assert_eq!(counts["500"], "1", "one 500");
    }

    #[test]
    fn given_an_unknown_index_when_queried_then_should_return_empty() {
        let engine = seeded();
        let result = engine.execute(&query_on("nope"));
        assert!(result.rows.is_empty(), "an unknown index has no rows");
        assert_eq!(result.page.total, 0);
    }
}
