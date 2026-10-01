// The named filters and records of the consumer-filter scenarios. Every
// language runner defines the same names with byte-identical payloads, so one
// Gherkin table drives the Rust, Python, and TypeScript evaluators alike.

use laser_sdk::filters::{Coerce, ConsumerFilter, FilterExpr, TimestampFormat};
use laser_sdk::query::CmpOp;
use laser_sdk::wire::schema::TypedValue;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;

// The record payloads and the feed order, shared with the Python and
// TypeScript runners so every language evaluates byte-identical records.
const RECORDS_JSON: &str = include_str!("../../scenarios/filter_records.json");

struct Fixtures {
    records: HashMap<String, Vec<u8>>,
    feed: Vec<String>,
}

pub fn filter(name: &str) -> ConsumerFilter {
    match name {
        "mode present" => ConsumerFilter::json(FilterExpr::present("fields.mode")),
        "safe mode" => safe_mode(),
        "safe mode values" => ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("table", CmpOp::Eq, "satellites"),
            FilterExpr::pred("op", CmpOp::Eq, "u"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ])),
        "mode is not safe" => ConsumerFilter::json(FilterExpr::negate(FilterExpr::pred(
            "after.mode",
            CmpOp::Eq,
            "safe",
        ))),
        "catalog number 9007199254740993" => ConsumerFilter::json(FilterExpr::pred(
            "after.norad_id",
            CmpOp::Eq,
            TypedValue::Long(9_007_199_254_740_993),
        )),
        "contact after noon UTC" => ConsumerFilter::json(FilterExpr::pred_as(
            "contact_at",
            CmpOp::Gt,
            "2026-09-21T12:00:00Z",
            Coerce::Timestamp {
                format: TimestampFormat::Rfc3339,
            },
        )),
        "critical priority" => {
            ConsumerFilter::headers_only(FilterExpr::header("priority", CmpOp::Eq, "critical"))
        }
        other => panic!("no filter named `{other}`"),
    }
}

/// The payload of the record `name`, from the fixture every runner loads.
pub fn record(name: &str) -> &'static [u8] {
    fixtures()
        .records
        .get(name)
        .unwrap_or_else(|| panic!("no record named `{name}`"))
}

/// The records a fresh fleet change feed holds, in publish order.
pub fn feed() -> &'static [String] {
    &fixtures().feed
}

fn fixtures() -> &'static Fixtures {
    static FIXTURES: OnceLock<Fixtures> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        let document: Value =
            serde_json::from_str(RECORDS_JSON).expect("the record fixture is JSON");
        let records = document["records"]
            .as_object()
            .expect("the fixture names its records")
            .iter()
            .map(|(name, payload)| {
                let bytes = match (payload["text"].as_str(), payload["hex"].as_str()) {
                    (Some(text), None) => text.as_bytes().to_vec(),
                    (None, Some(hex)) => (0..hex.len())
                        .step_by(2)
                        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex payload"))
                        .collect(),
                    _ => panic!("record `{name}` needs exactly one of `text` or `hex`"),
                };
                (name.clone(), bytes)
            })
            .collect();
        let feed = document["feed"]
            .as_array()
            .expect("the fixture lists the feed")
            .iter()
            .map(|name| {
                name.as_str()
                    .expect("a feed entry is a record name")
                    .to_owned()
            })
            .collect();
        Fixtures { records, feed }
    })
}

fn safe_mode() -> ConsumerFilter {
    let satellites = || FilterExpr::pred("table", CmpOp::Eq, "satellites");
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::all([
            satellites(),
            FilterExpr::pred("op", CmpOp::Eq, "u"),
            FilterExpr::pred("changed", CmpOp::Contains, "mode"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ]),
        FilterExpr::all([satellites(), FilterExpr::pred("op", CmpOp::Eq, "d")]),
        FilterExpr::all([
            FilterExpr::pred("event", CmpOp::Eq, "satellite.telemetry_changed"),
            FilterExpr::pred("fields.mode", CmpOp::Eq, "safe"),
        ]),
    ]))
}
