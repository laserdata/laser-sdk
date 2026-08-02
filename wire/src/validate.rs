use crate::batch::BatchRequest;
use crate::codes::AGDX_BATCH_CODE;
use crate::error::InvalidError;
use crate::graph::GraphQuery;
use crate::kv::{KvScan, KvSet};
use crate::limits::{
    MAX_BATCH_OPS, MAX_FRAME_BYTES, MAX_GRAPH_RESULT_ELEMENTS, MAX_GRAPH_TRAVERSE_DEPTH,
    MAX_KEY_BYTES, MAX_SCAN_LIMIT, MAX_VALUE_BYTES,
};

/// A capped request type that enforces its own size and shape limits, so the
/// cap logic lives once in the wire crate and every port and both servers get
/// the identical check by construction rather than each remembering to compare
/// against [`crate::limits`]. The SDK calls it before encoding, the servers call
/// it after decode and before execution.
pub trait Validate {
    /// Reject a request that violates a pinned cap or a structural rule.
    fn validate(&self) -> Result<(), InvalidError>;
}

/// The shared rule for caller-chosen names that flow into matching, filtering,
/// or storage identifiers: non-empty, within `cap` bytes, and made only of
/// ASCII letters, digits, `-`, `_`, and `.`. A strict safelist, not just a
/// length bound, because these names get inlined into queries, filters, and
/// rendered views.
pub(crate) fn validate_safelisted_name(
    label: &str,
    value: &str,
    cap: usize,
) -> Result<(), InvalidError> {
    if value.is_empty() {
        return Err(InvalidError::new(format!("{label} must not be empty")));
    }
    if value.len() > cap {
        return Err(InvalidError::new(format!(
            "{label} is {}B, exceeds cap {cap}B",
            value.len()
        )));
    }
    if let Some(bad) = value
        .bytes()
        .find(|byte| !matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'))
    {
        return Err(InvalidError::new(format!(
            "{label} has a disallowed byte {bad:#04x}: allowed are ASCII letters, digits, '-', '_', '.'"
        )));
    }
    Ok(())
}

impl Validate for BatchRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.ops.len() > MAX_BATCH_OPS {
            return Err(InvalidError::new(format!(
                "batch has {} ops, exceeds cap {MAX_BATCH_OPS}",
                self.ops.len()
            )));
        }
        let mut total = 0usize;
        for item in &self.ops {
            // A batch never nests a batch: an op that is itself a batch would let
            // one request fan out without bound past the op cap.
            if item.code == AGDX_BATCH_CODE {
                return Err(InvalidError::new(
                    "a batch may not contain a batch op".to_owned(),
                ));
            }
            if item.payload.len() > MAX_VALUE_BYTES {
                return Err(InvalidError::new(format!(
                    "batch op payload is {}B, exceeds cap {MAX_VALUE_BYTES}B",
                    item.payload.len()
                )));
            }
            total = total.saturating_add(item.payload.len());
        }
        if total > MAX_FRAME_BYTES {
            return Err(InvalidError::new(format!(
                "batch total payload is {total}B, exceeds cap {MAX_FRAME_BYTES}B"
            )));
        }
        Ok(())
    }
}

impl Validate for KvSet {
    fn validate(&self) -> Result<(), InvalidError> {
        crate::kv::validate_namespace(&self.namespace)?;
        if self.key.is_empty() {
            return Err(InvalidError::new("key-value key is empty".to_owned()));
        }
        if self.key.len() > MAX_KEY_BYTES {
            return Err(InvalidError::new(format!(
                "key is {}B, exceeds cap {MAX_KEY_BYTES}B",
                self.key.len()
            )));
        }
        if self.value.len() > MAX_VALUE_BYTES {
            return Err(InvalidError::new(format!(
                "value is {}B, exceeds cap {MAX_VALUE_BYTES}B",
                self.value.len()
            )));
        }
        Ok(())
    }
}

impl Validate for KvScan {
    fn validate(&self) -> Result<(), InvalidError> {
        crate::kv::validate_namespace(&self.namespace)?;
        if self.limit > MAX_SCAN_LIMIT {
            return Err(InvalidError::new(format!(
                "scan limit {} exceeds cap {MAX_SCAN_LIMIT}",
                self.limit
            )));
        }
        Ok(())
    }
}

impl Validate for GraphQuery {
    fn validate(&self) -> Result<(), InvalidError> {
        crate::graph::validate_graph_name(&self.graph)?;
        if self.traverse.len() > MAX_GRAPH_TRAVERSE_DEPTH as usize {
            return Err(InvalidError::new(format!(
                "graph traversal depth {} exceeds cap {MAX_GRAPH_TRAVERSE_DEPTH}",
                self.traverse.len()
            )));
        }
        if self.limit > MAX_GRAPH_RESULT_ELEMENTS {
            return Err(InvalidError::new(format!(
                "graph result limit {} exceeds cap {MAX_GRAPH_RESULT_ELEMENTS}",
                self.limit
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchItem;
    use crate::codes::{AGDX_KV_GET_CODE, BATCH_OP_VERSION, KV_OP_VERSION};

    #[test]
    fn given_a_batch_over_the_op_cap_when_validated_then_should_reject() {
        let request = BatchRequest {
            v: BATCH_OP_VERSION,
            ops: (0..MAX_BATCH_OPS + 1)
                .map(|_| BatchItem {
                    code: AGDX_KV_GET_CODE,
                    payload: Vec::new(),
                })
                .collect(),
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn given_a_batch_nesting_a_batch_when_validated_then_should_reject() {
        let request = BatchRequest {
            v: BATCH_OP_VERSION,
            ops: vec![BatchItem {
                code: AGDX_BATCH_CODE,
                payload: Vec::new(),
            }],
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn given_namespaces_when_validated_then_should_enforce_bounds() {
        use crate::kv::validate_namespace;
        use crate::limits::MAX_NAMESPACE_BYTES;
        assert!(validate_namespace("default").is_ok());
        assert!(validate_namespace("agent-abc/session").is_ok(), "hierarchy");
        assert!(validate_namespace("").is_err(), "empty");
        assert!(validate_namespace("bad\nns").is_err(), "control byte");
        assert!(validate_namespace(&"n".repeat(MAX_NAMESPACE_BYTES)).is_ok());
        assert!(validate_namespace(&"n".repeat(MAX_NAMESPACE_BYTES + 1)).is_err());
    }

    #[test]
    fn given_graph_names_when_validated_then_should_enforce_bounds() {
        use crate::graph::validate_graph_name;
        use crate::limits::MAX_GRAPH_NAME_BYTES;
        assert!(validate_graph_name("knowledge").is_ok());
        assert!(validate_graph_name("").is_err(), "empty");
        assert!(validate_graph_name("bad\tname").is_err(), "control byte");
        assert!(validate_graph_name(&"g".repeat(MAX_GRAPH_NAME_BYTES + 1)).is_err());
    }

    #[test]
    fn given_an_oversized_key_when_validated_then_should_reject_and_a_valid_one_passes() {
        let over = KvSet {
            v: KV_OP_VERSION,
            namespace: "ns".to_owned(),
            key: vec![b'x'; MAX_KEY_BYTES + 1],
            value: vec![1, 2, 3],
            expires_at_micros: None,
        };
        assert!(over.validate().is_err());
        let ok = KvSet {
            v: KV_OP_VERSION,
            namespace: "ns".to_owned(),
            key: vec![b'x'; 8],
            value: vec![1, 2, 3],
            expires_at_micros: None,
        };
        assert!(ok.validate().is_ok());
    }
}
