use std::fs;
use std::path::Path;

use laser_bench::manifest::SuiteManifest;
use laser_bench::report::RunReport;
use laser_bench::schema::{REPORT_SCHEMA, SUITE_SCHEMA, validate_json};

#[test]
fn given_minimal_report_fixture_when_validated_then_should_roundtrip() {
    let path = Path::new("fixtures/report-minimal.json");
    let source = fs::read_to_string(path).expect("report fixture should be readable");
    let value: serde_json::Value =
        serde_json::from_str(&source).expect("report fixture should be JSON");
    validate_json(REPORT_SCHEMA, &value).expect("report fixture should match its schema");
    let report: RunReport =
        serde_json::from_value(value).expect("report fixture should match Rust types");
    let encoded = serde_json::to_value(report).expect("report should serialize");
    validate_json(REPORT_SCHEMA, &encoded).expect("roundtrip report should match its schema");
}

#[test]
fn given_minimal_suite_fixture_when_validated_then_should_roundtrip() {
    let path = Path::new("fixtures/suite-minimal.toml");
    let manifest = SuiteManifest::load(path).expect("suite fixture should load");
    let value = serde_json::to_value(manifest).expect("suite should serialize as JSON");
    validate_json(SUITE_SCHEMA, &value).expect("suite fixture should match its schema");
}

#[test]
fn given_unknown_report_field_when_deserialized_then_should_preserve_it() {
    let source = fs::read_to_string("fixtures/report-minimal.json")
        .expect("report fixture should be readable");
    let mut value: serde_json::Value =
        serde_json::from_str(&source).expect("report fixture should be JSON");
    value["future_field"] = serde_json::json!({ "value": 7 });
    let report: RunReport =
        serde_json::from_value(value).expect("unknown report field should be accepted");
    assert_eq!(report.extra["future_field"]["value"], 7);
}
