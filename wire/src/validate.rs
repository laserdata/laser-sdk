use crate::batch::BatchRequest;
use crate::clients::ClientMetadata;
use crate::codes::AGDX_BATCH_CODE;
use crate::error::InvalidError;
use crate::graph::{GraphEdge, GraphNode, GraphQuery, GraphUpsert, SourceRef};
use crate::kv::{KvScan, KvSet};
use crate::limits::{
    MAX_BATCH_OPS, MAX_CLIENT_METADATA, MAX_FRAME_BYTES, MAX_GRAPH_NODE_LABELS,
    MAX_GRAPH_RESULT_ELEMENTS, MAX_GRAPH_TRAVERSE_DEPTH, MAX_KEY_BYTES, MAX_MEMORY_BODY_BYTES,
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_PAGE_SIZE, MAX_QUERY_NAME_BYTES,
    MAX_SCAN_LIMIT, MAX_SOURCE_REF_BYTES, MAX_TEXT_QUERY_BYTES, MAX_VALUE_BYTES,
    MAX_VECTOR_DIMENSIONS,
};
use crate::memory::MemoryRecord;
use crate::query::{TextQuery, Value, VectorQuery};

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

impl Validate for SourceRef {
    fn validate(&self) -> Result<(), InvalidError> {
        let size = match self {
            // A log pointer is fixed-width numerics plus an optional
            // conversation id, so only the id needs a bound.
            SourceRef::Message { conversation, .. } => conversation.as_ref().map_or(0, String::len),
            SourceRef::Kv { namespace, key } => namespace.len() + key.len(),
            SourceRef::Memory { id } => id.len(),
        };
        if size > MAX_SOURCE_REF_BYTES {
            return Err(InvalidError::new(format!(
                "source reference is {size}B, exceeds cap {MAX_SOURCE_REF_BYTES}B"
            )));
        }
        Ok(())
    }
}

// Attribute lists ride on both nodes and edges, so their bound lives once.
fn validate_attrs(label: &str, attrs: &[(String, Value)]) -> Result<(), InvalidError> {
    if attrs.len() > MAX_METADATA_ENTRIES {
        return Err(InvalidError::new(format!(
            "{label} carries {} attributes, exceeds cap {MAX_METADATA_ENTRIES}",
            attrs.len()
        )));
    }
    for (key, _) in attrs {
        if key.len() > MAX_METADATA_KEY_BYTES {
            return Err(InvalidError::new(format!(
                "{label} attribute name is {}B, exceeds cap {MAX_METADATA_KEY_BYTES}B",
                key.len()
            )));
        }
    }
    Ok(())
}

impl Validate for GraphNode {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.labels.len() > MAX_GRAPH_NODE_LABELS {
            return Err(InvalidError::new(format!(
                "graph node carries {} labels, exceeds cap {MAX_GRAPH_NODE_LABELS}",
                self.labels.len()
            )));
        }
        validate_attrs("graph node", &self.attrs)?;
        if let Some(source) = &self.source {
            source.validate()?;
        }
        Ok(())
    }
}

impl Validate for GraphEdge {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_attrs("graph edge", &self.attrs)?;
        if let Some(source) = &self.source {
            source.validate()?;
        }
        Ok(())
    }
}

impl Validate for GraphUpsert {
    fn validate(&self) -> Result<(), InvalidError> {
        crate::graph::validate_graph_name(&self.graph)?;
        let elements = self.nodes.len() + self.edges.len();
        if elements > MAX_GRAPH_RESULT_ELEMENTS {
            return Err(InvalidError::new(format!(
                "graph upsert carries {elements} elements, exceeds cap {MAX_GRAPH_RESULT_ELEMENTS}"
            )));
        }
        for node in &self.nodes {
            node.validate()?;
        }
        for edge in &self.edges {
            edge.validate()?;
        }
        Ok(())
    }
}

impl Validate for TextQuery {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.query.trim().is_empty() || self.query.len() > MAX_TEXT_QUERY_BYTES {
            return Err(InvalidError::new(format!(
                "text query is {}B, expected 1..={MAX_TEXT_QUERY_BYTES}B",
                self.query.len()
            )));
        }
        if self.query.chars().any(char::is_control) {
            return Err(InvalidError::new("text query contains a control character"));
        }
        if let Some(field) = &self.field
            && (field.is_empty()
                || field.len() > MAX_QUERY_NAME_BYTES
                || field.chars().any(char::is_control))
        {
            return Err(InvalidError::new("text query field is invalid"));
        }
        Ok(())
    }
}

impl Validate for VectorQuery {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.field.is_empty()
            || self.field.len() > MAX_QUERY_NAME_BYTES
            || self.field.chars().any(char::is_control)
        {
            return Err(InvalidError::new("vector field is invalid"));
        }
        if self.embedding.is_empty() || self.embedding.len() > MAX_VECTOR_DIMENSIONS {
            return Err(InvalidError::new(format!(
                "vector dimensions must be in 1..={MAX_VECTOR_DIMENSIONS}"
            )));
        }
        if self
            .embedding
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        {
            return Err(InvalidError::new(
                "vector values must be finite canonical floats",
            ));
        }
        if self.top_k == 0 || self.top_k > MAX_PAGE_SIZE as u32 {
            return Err(InvalidError::new(format!(
                "vector top_k {} is outside 1..={MAX_PAGE_SIZE}",
                self.top_k
            )));
        }
        Ok(())
    }
}

impl Validate for MemoryRecord {
    fn validate(&self) -> Result<(), InvalidError> {
        if let MemoryRecord::Item { body, .. } = self
            && body.len() > MAX_MEMORY_BODY_BYTES
        {
            return Err(InvalidError::new(format!(
                "memory body is {}B, exceeds cap {MAX_MEMORY_BODY_BYTES}B",
                body.len()
            )));
        }
        Ok(())
    }
}

impl Validate for ClientMetadata {
    fn validate(&self) -> Result<(), InvalidError> {
        let size = self.metadata.as_ref().map_or(0, Vec::len);
        if size > MAX_CLIENT_METADATA {
            return Err(InvalidError::new(format!(
                "client metadata is {size}B, exceeds cap {MAX_CLIENT_METADATA}B"
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

    fn node_with(labels: Vec<String>, attrs: Vec<(String, Value)>) -> GraphNode {
        GraphNode {
            id: crate::graph::NodeId::from_u128(1),
            labels,
            attrs,
            embedding: None,
            source: None,
        }
    }

    #[test]
    fn given_a_node_over_the_label_cap_when_validated_then_should_reject() {
        let over = node_with(
            (0..=MAX_GRAPH_NODE_LABELS)
                .map(|index| format!("label-{index}"))
                .collect(),
            Vec::new(),
        );
        assert!(over.validate().is_err());
        assert!(
            node_with(vec!["Person".to_owned()], Vec::new())
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn given_a_node_over_the_attribute_cap_when_validated_then_should_reject() {
        let over = node_with(
            Vec::new(),
            (0..=MAX_METADATA_ENTRIES)
                .map(|index| (format!("attr-{index}"), Value::from("x")))
                .collect(),
        );
        assert!(over.validate().is_err());
    }

    #[test]
    fn given_an_oversized_source_reference_when_validated_then_should_reject() {
        let over = SourceRef::Kv {
            namespace: "n".repeat(MAX_SOURCE_REF_BYTES),
            key: "k".repeat(MAX_SOURCE_REF_BYTES),
        };
        assert!(over.validate().is_err());
        assert!(
            SourceRef::Memory {
                id: "01KWM3K3XEP3NP5TN850J17YBP".to_owned()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn given_an_upsert_over_the_element_cap_when_validated_then_should_reject() {
        let over = GraphUpsert {
            v: 1,
            graph: "knowledge".to_owned(),
            nodes: (0..=MAX_GRAPH_RESULT_ELEMENTS)
                .map(|_| node_with(Vec::new(), Vec::new()))
                .collect(),
            edges: Vec::new(),
        };
        assert!(over.validate().is_err());
        let ok = GraphUpsert {
            v: 1,
            graph: "knowledge".to_owned(),
            nodes: vec![node_with(vec!["Person".to_owned()], Vec::new())],
            edges: Vec::new(),
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn given_an_upsert_carrying_an_over_cap_node_when_validated_then_should_reject() {
        let upsert = GraphUpsert {
            v: 1,
            graph: "knowledge".to_owned(),
            nodes: vec![node_with(
                (0..=MAX_GRAPH_NODE_LABELS)
                    .map(|index| format!("label-{index}"))
                    .collect(),
                Vec::new(),
            )],
            edges: Vec::new(),
        };
        assert!(
            upsert.validate().is_err(),
            "an upsert must enforce its elements' caps, not just its own"
        );
    }

    #[test]
    fn given_an_oversized_text_query_when_validated_then_should_reject() {
        let over = TextQuery {
            field: None,
            query: "q".repeat(MAX_TEXT_QUERY_BYTES + 1),
        };
        assert!(over.validate().is_err());
        let ok = TextQuery {
            field: None,
            query: "checkout is slow".to_owned(),
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn given_an_over_cap_vector_top_k_when_validated_then_should_reject() {
        let over = VectorQuery {
            field: "embedding".to_owned(),
            embedding: vec![0.0; 4],
            top_k: MAX_PAGE_SIZE as u32 + 1,
        };
        assert!(over.validate().is_err());
        let ok = VectorQuery {
            field: "embedding".to_owned(),
            embedding: vec![0.0; 4],
            top_k: 10,
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn given_an_oversized_memory_body_when_validated_then_should_reject() {
        let over = MemoryRecord::Item {
            id: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
            kind: "fact".to_owned(),
            body: vec![0u8; MAX_MEMORY_BODY_BYTES + 1],
        };
        assert!(over.validate().is_err());
        let ok = MemoryRecord::Item {
            id: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
            kind: "fact".to_owned(),
            body: b"checkout is slow".to_vec(),
        };
        assert!(ok.validate().is_ok());
        assert!(
            MemoryRecord::Forget {
                target: "01KWM3K3XEP3NP5TN850J17YBP".to_owned()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn given_oversized_client_metadata_when_validated_then_should_reject() {
        let over = ClientMetadata {
            client_id: 1,
            user_id: None,
            transport: 1,
            address: "127.0.0.1:8090".to_owned(),
            consumer_groups_count: 0,
            metadata: Some(vec![0u8; MAX_CLIENT_METADATA + 1]),
        };
        assert!(over.validate().is_err());
        let ok = ClientMetadata {
            client_id: 1,
            user_id: None,
            transport: 1,
            address: "127.0.0.1:8090".to_owned(),
            consumer_groups_count: 0,
            metadata: Some(b"card".to_vec()),
        };
        assert!(ok.validate().is_ok());
    }
}
