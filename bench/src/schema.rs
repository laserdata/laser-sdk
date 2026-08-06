use serde_json::Value;

use crate::BenchError;

pub const MCP_REVIEW_SIGNOFF_SCHEMA: &str =
    include_str!("../schemas/mcp-review-signoff-v1.schema.json");
pub const REPORT_SCHEMA: &str = include_str!("../schemas/report-v1.schema.json");
pub const SUITE_SCHEMA: &str = include_str!("../schemas/suite-v1.schema.json");

/// Validate one JSON value against a schema document.
///
/// # Errors
///
/// Returns an error when the schema is malformed or the value does not conform.
pub fn validate_json(schema_source: &str, instance: &Value) -> Result<(), BenchError> {
    let schema: Value = serde_json::from_str(schema_source)?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| BenchError::Schema(error.to_string()))?;
    validator
        .validate(instance)
        .map_err(|error| BenchError::Schema(error.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn given_complete_mcp_signoff_when_validated_then_should_pass_schema() {
        let signoff = json!({
            "schema_version": 1,
            "bundle_sha256": "a".repeat(64),
            "reviewer": "independent reviewer",
            "reviewed_at": "2026-08-05T00:00:00Z",
            "decision": "accepted",
            "hypotheses": ["m1", "m2", "m3", "m4", "m5", "m6"],
            "findings": [],
        });

        validate_json(MCP_REVIEW_SIGNOFF_SCHEMA, &signoff)
            .expect("complete MCP sign-off should validate");
    }
}
