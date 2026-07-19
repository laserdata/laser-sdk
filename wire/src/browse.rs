use crate::control::{Projection, ProjectionBinding, SchemaDef, SchemaSource};
use crate::query::QueryError;
use serde::{Deserialize, Serialize};

/// A registered projection plus the bindings that route topics into it. The full
/// picture of one materialized view: its extraction schema, expected content
/// type, indexed fields, and where it applies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionInfo {
    pub projection: Projection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<ProjectionBinding>,
}

/// A writer schema plus its lifecycle state. A `dropped` schema is hidden
/// from the active set and its id rejects re-registration with a different
/// definition, but records stamped with the id keep decoding (ids are
/// permanent). Wire mirrors stay constructible (wire-stability-bound, not
/// API-stability-bound).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub schema: SchemaDef,
    #[serde(default)]
    pub dropped: bool,
}

/// Request to read one projection's details by id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetProjection {
    pub v: u32,
    pub id: String,
}

/// Request to list registered projections, optionally filtered. Empty filters
/// list every projection. `topics` keeps projections bound to any of the named
/// source topics. `name_contains` keeps those whose name contains the substring.
/// `id_prefix` keeps those whose id starts with it. `search` is the single-box
/// convenience that matches the substring against the name OR the id. The
/// filters compose (AND).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListProjections {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
}

/// Request to read one registered writer schema by id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetSchema {
    pub v: u32,
    pub id: u32,
}

/// Request to list registered writer schemas, optionally filtered.
/// `name_contains` keeps those whose optional name contains the substring, and
/// an absent filter lists every schema. The filter lives here, not only on the
/// HTTP query, so the server pushes it down rather than the client paging the
/// whole set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListSchemas {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
}

/// The synchronous register request: no id, LaserData Cloud validates the
/// definition, allocates the next free id, durably appends the control
/// event, and replies `SchemaRegistered(id)`. Distinct from
/// `ControlCommand::RegisterSchema`, the durable log form that carries the
/// allocated id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterSchema {
    pub v: u32,
    pub source: SchemaSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

/// Request to decode one record body under the schema registered for `id`,
/// the same decode the projector runs on `agdx.sid`-stamped records. Read-only
/// convenience for consoles and tools that hold a schema-first (Avro/Protobuf)
/// payload and want its JSON form without re-implementing the codecs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecodeRecord {
    pub v: u32,
    pub id: u32,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

/// Reply to a registry browse: `Ok` with the result, or `Err`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BrowseReply {
    Ok(BrowseOutcome),
    Err(QueryError),
}

/// The result of a registry browse, shaped per request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BrowseOutcome {
    /// `list_projections`: every registered projection.
    Projections(Vec<ProjectionInfo>),
    /// `get projection`: the projection with the requested id, or `None`.
    Projection(Option<ProjectionInfo>),
    /// `list schemas`: every known writer schema, active and tombstoned.
    Schemas(Vec<SchemaInfo>),
    /// `get schema`: the schema occupying the requested id, or `None`.
    Schema(Option<SchemaInfo>),
    /// `register schema`: the LaserData-Cloud-allocated id, already durably appended
    /// to the control topic (visibility follows within the apply latency).
    SchemaRegistered(u32),
    /// `decode record`: the payload's JSON form under the requested schema,
    /// or `None` when the body does not decode under it. A `serde_json::Value`
    /// because the decoded shape is arbitrary (object, array, scalar) and the
    /// LaserData Cloud encodes exactly that value into the reply.
    Decoded(Option<serde_json::Value>),
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::QUERY_OP_VERSION;
    use crate::content::ContentType;
    use crate::control::ProjectionBinding;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_browse_reply_when_round_tripped_then_should_preserve_projection_details() {
        let info = ProjectionInfo {
            projection: Projection::builder("order.v1")
                .name("order")
                .version(1)
                .content_type(ContentType::Json)
                .fields(["order_id", "amount"])
                .build(),
            bindings: vec![
                ProjectionBinding::builder()
                    .source("shop", "orders")
                    .allow("order.v1")
                    .default_projection("order.v1")
                    .target_table("orders_rows")
                    .build(),
            ],
        };
        let reply = BrowseReply::Ok(BrowseOutcome::Projections(vec![info]));
        let bytes = encode_named(&reply).expect("the reply serializes");
        let back: BrowseReply = decode_named(&bytes).expect("the reply deserializes");
        let BrowseReply::Ok(BrowseOutcome::Projections(list)) = back else {
            panic!("expected an Ok(Projections) browse reply");
        };
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].projection.id.as_str(), "order.v1");
        assert_eq!(list[0].projection.extraction.fields.len(), 2);
        let targets = &list[0].bindings[0].targets;
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].table, "orders_rows");
    }

    #[test]
    fn given_a_decode_record_when_round_tripped_then_should_preserve_payload_bytes() {
        let request = DecodeRecord {
            v: QUERY_OP_VERSION,
            id: 7,
            payload: vec![0xff, 0x00, 0x10],
        };
        let bytes = encode_named(&request).expect("serializes");
        let back: DecodeRecord = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.id, 7);
        assert_eq!(back.payload, vec![0xff, 0x00, 0x10]);
    }
}
