use serde::{Deserialize, Serialize};

/// The wire codec tag stamped on `agdx.ct`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ContentType {
    /// "Best-effort decode": the projector tries JSON first, else
    /// raw. Useful when a projection is content-agnostic. Lands on the wire
    /// as the `any` variant so consumers can distinguish "unknown" from "raw".
    Any,
    #[default]
    Raw,
    Json,
    Avro,
    Protobuf,
    Msgpack,
    Cbor,
    Bson,
    Arrow,
    /// The body is a CBOR [`BodyRef`](crate::agent::BodyRef) capsule
    /// pointing at content stored elsewhere (object storage, KV, another
    /// topic), not the content itself: the claim-check form for payloads too
    /// large or sensitive to inline.
    Ref,
}

impl ContentType {
    /// Whether this is the default `Raw` codec (omitted on the wire when a
    /// field defaults to it).
    pub const fn is_raw(&self) -> bool {
        matches!(self, ContentType::Raw)
    }

    /// The compact `u8` wire code stamped as the `agdx.ct` header value. A fixed
    /// dictionary shared with LaserData Cloud and its display layers (and the
    /// shape planned for Iggy's native reserved content-type field): raw=0,
    /// json=1, msgpack=2, cbor=3, bson=4, avro=5, protobuf=6, arrow=7,
    /// ref=8 (claim-check body reference), any=255 (best-effort sentinel).
    pub const fn code(self) -> u8 {
        match self {
            ContentType::Raw => 0,
            ContentType::Json => 1,
            ContentType::Msgpack => 2,
            ContentType::Cbor => 3,
            ContentType::Bson => 4,
            ContentType::Avro => 5,
            ContentType::Protobuf => 6,
            ContentType::Arrow => 7,
            ContentType::Ref => 8,
            ContentType::Any => 255,
        }
    }

    /// Decode a compact `agdx.ct` code, or `None` for a code this build does
    /// not name. The codes are a growable dictionary, so a server MUST treat an
    /// unknown code as opaque (pass it through, decode the body best-effort)
    /// and never reject the record on it. A newer peer may stamp a code this
    /// build has not learned yet.
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(ContentType::Raw),
            1 => Some(ContentType::Json),
            2 => Some(ContentType::Msgpack),
            3 => Some(ContentType::Cbor),
            4 => Some(ContentType::Bson),
            5 => Some(ContentType::Avro),
            6 => Some(ContentType::Protobuf),
            7 => Some(ContentType::Arrow),
            8 => Some(ContentType::Ref),
            255 => Some(ContentType::Any),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_content_type_codes_when_mapped_then_should_match_the_fixed_dictionary() {
        // The compact `agdx.ct` dictionary is a fixed wire contract: the
        // `ContentType::code` mapping must not drift.
        let expected = [
            (ContentType::Raw, 0u8),
            (ContentType::Json, 1),
            (ContentType::Msgpack, 2),
            (ContentType::Cbor, 3),
            (ContentType::Bson, 4),
            (ContentType::Avro, 5),
            (ContentType::Protobuf, 6),
            (ContentType::Arrow, 7),
            (ContentType::Ref, 8),
            (ContentType::Any, 255),
        ];
        for (content_type, code) in expected {
            assert_eq!(content_type.code(), code);
            assert_eq!(ContentType::from_code(code), Some(content_type));
        }
        assert_eq!(ContentType::from_code(9), None);
    }

    #[test]
    fn given_content_types_when_displayed_then_should_match_the_wire_names() {
        assert_eq!(ContentType::Raw.to_string(), "raw");
        assert_eq!(ContentType::Json.to_string(), "json");
        assert_eq!(ContentType::Msgpack.to_string(), "msgpack");
        assert_eq!(
            "protobuf".parse::<ContentType>().expect("protobuf parses"),
            ContentType::Protobuf
        );
        assert_eq!(ContentType::default(), ContentType::Raw);
    }
}
