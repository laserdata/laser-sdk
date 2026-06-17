use crate::error::LaserError;
use crate::query::{
    CONTENT_TYPE, ContentType, IDX_PREFIX, INLINE_PAYLOAD, MAX_INDEX_ENTRIES_PER_RECORD,
    PROJECTION_REF, SCHEMA_ID,
};
use iggy::prelude::{HeaderKey, HeaderValue};
use laser_wire::headers::{HEADER_FRAMING_BYTES, HEADER_SOFT_CAP, HEADER_VALUE_MAX};
use std::collections::BTreeMap;
use std::str::FromStr;

/// A record to publish: payload plus indexed fields and metadata (built via the publish builder).
#[derive(Clone, Debug, Default, bon::Builder)]
pub struct Record {
    // Indexable scalars stamped under `agdx.idx.<k>`. Accumulated by the builder's
    // `index(k, v)` method rather than set as one vector. At least one entry
    // here is required for a projector to materialize the record - a record
    // with zero indexed fields is treated as "don't index me" and dropped.
    #[builder(field)]
    pub index: Vec<(String, String)>,
    // Ride-along headers stamped verbatim so the projector can store them in a
    // metadata column. Use for trace ids, user tags, content classification,
    // anything callers want next to the row without paying the index cost.
    #[builder(field)]
    pub metadata: Vec<(String, String)>,
    /// Wire content-type. `None` means no `agdx.ct` header is stamped
    /// (consumer treats the payload as opaque bytes). `Some(ContentType::*)`
    /// stamps the tag so downstream readers know how to decode. The codec
    /// helpers (`.json`, `.msgpack`, `.encode_with::<C>`, `.raw_bytes`) set
    /// it automatically. An explicit `.content_type(ContentType::Raw)` also
    /// stamps it (distinct from "unset").
    pub content_type: Option<ContentType>,
    // Projection selector for the materialization LaserData Cloud. Opaque string like
    // `"order.v1"`. When set, the projector looks it up in the binding's
    // `allowed_projections` and applies the matching `Projection` extraction
    // rules. When unset, the binding's default projection (or the explicit
    // `agdx.idx.*` header path) applies.
    #[builder(into)]
    pub projection_ref: Option<String>,
    // Writer-schema id for a schema-first codec (Avro, Protobuf). Stamped on the
    // wire as `agdx.sid` when set. LaserData Cloud resolves it against its
    // schema registry to decode the body. Self-describing codecs (JSON,
    // MessagePack, CBOR, BSON) never need it. NOT used for materialization
    // routing - that is `projection_ref`'s job. A bridge that lives in LaserData Cloud
    // until Iggy gains native schema dispatch.
    pub schema_id: Option<u32>,
    // Whether the projector inlines the payload bytes alongside the row in the
    // query index. Default is `false` - Iggy still keeps the original message
    // in the log, but the materialized row only carries indexed scalars +
    // metadata. Opt in for self-describing bodies callers want back through
    // queries.
    #[builder(default)]
    pub inline_payload: bool,
}

impl<S: record_builder::State> RecordBuilder<S> {
    /// Add an indexed scalar under `agdx.idx.<key>`. Validation (non-empty key,
    /// no reserved `agdx.idx.` prefix, count cap) runs at `.send().await?` time
    /// so the builder stays a simple fluent push. Errors surface as
    /// `LaserError::Invalid` with `record #<idx>: ...` context from the
    /// batch loop.
    pub fn index(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.index.push((key.into(), value.into()));
        self
    }

    /// Add a ride-along metadata header (not indexed).
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push((key.into(), value.into()));
        self
    }
}

impl TryFrom<&Record> for BTreeMap<HeaderKey, HeaderValue> {
    type Error = LaserError;

    fn try_from(record: &Record) -> Result<Self, Self::Error> {
        let mut map: BTreeMap<HeaderKey, HeaderValue> = BTreeMap::new();
        // Reserved keys carry TYPED values (u8 code / u32 id / bool flag), not
        // strings: 1-4 value bytes instead of up to 8 name characters, on
        // every message.
        if let Some(content_type) = record.content_type {
            map.insert(
                HeaderKey::from_str(CONTENT_TYPE)?,
                HeaderValue::from(content_type.code()),
            );
        }
        if let Some(ref projection_ref) = record.projection_ref {
            put_header(&mut map, PROJECTION_REF, projection_ref)?;
        }
        if let Some(schema_id) = record.schema_id {
            map.insert(
                HeaderKey::from_str(SCHEMA_ID)?,
                HeaderValue::from(schema_id),
            );
        }
        if record.inline_payload {
            map.insert(
                HeaderKey::from_str(INLINE_PAYLOAD)?,
                HeaderValue::from(true),
            );
        }
        if record.index.len() > MAX_INDEX_ENTRIES_PER_RECORD {
            return Err(LaserError::Invalid(format!(
                "record has {} indexed scalars, exceeds cap of {MAX_INDEX_ENTRIES_PER_RECORD}",
                record.index.len()
            )));
        }
        for (key, value) in &record.index {
            if key.is_empty() {
                return Err(LaserError::Invalid(
                    "index key must not be empty".to_owned(),
                ));
            }
            if key.starts_with(IDX_PREFIX) {
                return Err(LaserError::Invalid(format!(
                    "index key `{key}` must not start with the reserved `agdx.idx.` prefix"
                )));
            }
            put_header(&mut map, &format!("{IDX_PREFIX}{key}"), value)?;
        }
        for (key, value) in &record.metadata {
            if key.starts_with(IDX_PREFIX) {
                return Err(LaserError::Invalid(format!(
                    "metadata header `{key}` collides with the `agdx.idx.` namespace - use .index() instead"
                )));
            }
            if matches!(
                key.as_str(),
                CONTENT_TYPE | SCHEMA_ID | PROJECTION_REF | INLINE_PAYLOAD
            ) {
                return Err(LaserError::Invalid(format!(
                    "metadata header `{key}` is reserved - set it via the dedicated builder method"
                )));
            }
            put_header(&mut map, key, value)?;
        }
        let size: usize = map
            .iter()
            .map(|(k, v)| k.as_bytes().len() + v.as_bytes().len() + HEADER_FRAMING_BYTES)
            .sum();
        if size > HEADER_SOFT_CAP {
            return Err(LaserError::Invalid(format!(
                "record headers {size}B exceed soft cap {HEADER_SOFT_CAP}B"
            )));
        }
        Ok(map)
    }
}

fn put_header(
    map: &mut BTreeMap<HeaderKey, HeaderValue>,
    key: &str,
    value: &str,
) -> Result<(), LaserError> {
    if value.is_empty() {
        return Err(LaserError::Invalid(format!(
            "header `{key}` value must not be empty"
        )));
    }
    if value.len() > HEADER_VALUE_MAX {
        return Err(LaserError::Invalid(format!(
            "header `{key}` value is {}B, exceeds max {HEADER_VALUE_MAX}B",
            value.len()
        )));
    }
    map.insert(HeaderKey::from_str(key)?, HeaderValue::from_str(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_record_when_lowered_then_should_stamp_compact_typed_headers() {
        let record = Record::builder()
            .content_type(ContentType::Json)
            .schema_id(7)
            .index("order_id", "123")
            .index("customer_id", "abc")
            .build();
        let headers: BTreeMap<HeaderKey, HeaderValue> =
            (&record).try_into().expect("the record lowers to headers");

        let get = |key: &str| {
            headers
                .get(&HeaderKey::from_str(key).expect("the key is valid"))
                .cloned()
        };
        // Reserved keys ride compact + typed: 1-byte content-type code,
        // 4-byte u32 schema id.
        let content_type = get(CONTENT_TYPE).expect("agdx.ct stamped");
        assert_eq!(
            content_type.as_uint8().expect("u8 typed value"),
            ContentType::Json.code(),
        );
        let schema_id = get(SCHEMA_ID).expect("agdx.sid stamped");
        assert_eq!(schema_id.as_uint32().expect("u32 typed value"), 7);
        let as_str =
            |key: &str| get(key).map(|value| value.as_str().expect("utf-8 value").to_owned());
        assert_eq!(as_str("agdx.idx.order_id").as_deref(), Some("123"));
        assert_eq!(as_str("agdx.idx.customer_id").as_deref(), Some("abc"));
        assert_eq!(get(INLINE_PAYLOAD), None, "off by default");
    }

    #[test]
    fn given_a_record_without_content_type_when_lowered_then_should_omit_the_header() {
        let record = Record::builder().index("order_id", "123").build();
        let headers: BTreeMap<HeaderKey, HeaderValue> =
            (&record).try_into().expect("the record lowers to headers");
        let key = HeaderKey::from_str(CONTENT_TYPE).expect("the key is valid");
        assert!(
            !headers.contains_key(&key),
            "agdx.ct must NOT be stamped when Record.content_type is None",
        );
    }

    #[test]
    fn given_inline_payload_and_metadata_when_lowered_then_should_emit_both() {
        let record = Record::builder()
            .index("order_id", "123")
            .metadata("trace_id", "abc")
            .metadata("agdx.actor", "checkout")
            .inline_payload(true)
            .build();
        let headers: BTreeMap<HeaderKey, HeaderValue> =
            (&record).try_into().expect("the record lowers to headers");

        let inline = headers
            .get(&HeaderKey::from_str(INLINE_PAYLOAD).expect("the key is valid"))
            .expect("agdx.inline stamped");
        assert!(inline.as_bool().expect("bool typed value"));
        let get = |key: &str| {
            headers
                .get(&HeaderKey::from_str(key).expect("the key is valid"))
                .map(|value| value.as_str().expect("the value is utf-8").to_owned())
        };
        assert_eq!(get("trace_id").as_deref(), Some("abc"));
        assert_eq!(get("agdx.actor").as_deref(), Some("checkout"));
    }

    #[test]
    fn given_metadata_in_idx_namespace_when_lowered_then_should_error() {
        let record = Record::builder().metadata("agdx.idx.smuggled", "x").build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_metadata_for_a_reserved_key_when_lowered_then_should_error() {
        for reserved in [CONTENT_TYPE, SCHEMA_ID, INLINE_PAYLOAD] {
            let record = Record::builder().metadata(reserved, "smuggled").build();
            let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
            assert!(
                matches!(result, Err(LaserError::Invalid(_))),
                "reserved key `{reserved}` must not be smuggleable through metadata"
            );
        }
    }

    #[test]
    fn given_a_record_with_an_empty_index_value_when_lowered_then_should_error() {
        let record = Record::builder().index("order_id", "").build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_a_record_with_an_oversized_index_value_when_lowered_then_should_error() {
        let record = Record::builder()
            .index("blob", "x".repeat(HEADER_VALUE_MAX + 1))
            .build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_a_record_with_an_empty_index_key_when_lowered_then_should_error() {
        let record = Record::builder().index("", "value").build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_a_record_with_a_reserved_idx_prefix_key_when_lowered_then_should_error() {
        let record = Record::builder().index("agdx.idx.shadow", "value").build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        let message = match result {
            Err(LaserError::Invalid(message)) => message,
            other => panic!("expected an invalid error, got {other:?}"),
        };
        assert!(
            message.contains("agdx.idx."),
            "error mentions the reserved prefix: {message}"
        );
    }

    #[test]
    fn given_a_record_with_more_than_the_index_cap_when_lowered_then_should_error() {
        let mut builder = Record::builder();
        for i in 0..(MAX_INDEX_ENTRIES_PER_RECORD + 1) {
            builder = builder.index(format!("k{i}"), format!("v{i}"));
        }
        let record = builder.build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&record).try_into();
        let message = match result {
            Err(LaserError::Invalid(message)) => message,
            other => panic!("expected an invalid error, got {other:?}"),
        };
        assert!(
            message.contains(&MAX_INDEX_ENTRIES_PER_RECORD.to_string()),
            "error mentions the cap: {message}"
        );
    }
}
