// The shared serde helpers every surface encodes byte fields with, in one
// place so no surface can drift from another. `Vec<u8>` rides the wire via
// `serialize_bytes`: a bare `Vec<u8>` would encode as an array of integers,
// which is larger and ambiguous.
//
// These fields are dual-format: the same wire types serialize as CBOR on the
// binary band (where `serialize_bytes` is a byte string) and as JSON on the
// HTTP surface (where serde_json renders bytes as an array of integers, since
// JSON has no byte string). So decode goes through `deserialize_any` and
// accepts both shapes explicitly: a byte string via `visit_bytes` (the binary
// band, with no reliance on a CBOR library's leniency about routing a byte
// string through the sequence path) and an array of `u8` via `visit_seq` (the
// JSON surface). A human-readable format also accepts text as its UTF-8 bytes,
// so a JSON caller can write a key as `"user-1"`. Any other type fails, so
// wrong-typed data is rejected, and a binary format never takes text.

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserializer, Serialize, Serializer};
use std::fmt;

// A hostile array header can declare a length far larger than the bytes that
// follow, so the seq path caps how much it preallocates from the declared
// length. The Vec still grows to fit a genuinely large field. This only stops a
// tiny input from reserving gigabytes up front.
const MAX_SEQ_PREALLOC: usize = 4096;

struct ByteBuf<'a>(&'a [u8]);

impl Serialize for ByteBuf<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

struct ByteVecVisitor;

impl<'de> Visitor<'de> for ByteVecVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string or an array of bytes")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(v)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(MAX_SEQ_PREALLOC));
        while let Some(byte) = seq.next_element::<u8>()? {
            out.push(byte);
        }
        Ok(out)
    }
}

struct ByteTextVisitor;

impl<'de> Visitor<'de> for ByteTextVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UTF-8 text or an array of bytes")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(v.as_bytes().to_vec())
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(v.into_bytes())
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        ByteVecVisitor.visit_bytes(v)
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        ByteVecVisitor.visit_byte_buf(v)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        ByteVecVisitor.visit_seq(seq)
    }
}

/// `Vec<u8>` as a CBOR byte string, or on the JSON view an array of bytes, also read from UTF-8 text.
pub(crate) mod bin_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_any(ByteTextVisitor)
        } else {
            deserializer.deserialize_any(ByteVecVisitor)
        }
    }
}

/// `Vec<Vec<u8>>` as a CBOR array of byte strings, one per element. Built on
/// the same visitor as [`bin_bytes`], no extra dependency.
pub(crate) mod vec_bin_bytes {
    use super::*;
    use serde::Deserialize;
    use serde::ser::SerializeSeq;

    struct BytesElem<'a>(&'a [u8]);

    impl serde::Serialize for BytesElem<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }

    struct ByteElem(Vec<u8>);

    impl<'de> Deserialize<'de> for ByteElem {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            super::bin_bytes::deserialize(deserializer).map(ByteElem)
        }
    }

    pub fn serialize<S: Serializer>(value: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(value.len()))?;
        for payload in value {
            seq.serialize_element(&BytesElem(payload))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        Vec::<ByteElem>::deserialize(deserializer)
            .map(|elems| elems.into_iter().map(|elem| elem.0).collect())
    }
}

/// `Option<Vec<u8>>` as an optional byte string (CBOR) or array of bytes (JSON).
pub(crate) mod opt_bin_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(payload) => serializer.serialize_some(&ByteBuf(payload)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        struct OptVisitor;

        impl<'de> Visitor<'de> for OptVisitor {
            type Value = Option<Vec<u8>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an optional CBOR byte string")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                bin_bytes::deserialize(deserializer).map(Some)
            }
        }

        deserializer.deserialize_option(OptVisitor)
    }
}

/// A `u128` that JSON carries as decimal text, because JSON numbers lose
/// precision above 2^53 in most clients. Binary formats keep the integer.
pub mod u128_text {
    use serde::de::{self, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(value)
        } else {
            serializer.serialize_u128(*value)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        struct Text;
        impl Visitor<'_> for Text {
            type Value = u128;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a u128 as decimal text or as an integer")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<u128, E> {
                v.parse().map_err(|_| E::custom("not a decimal u128"))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<u128, E> {
                Ok(u128::from(v))
            }
            fn visit_u128<E: de::Error>(self, v: u128) -> Result<u128, E> {
                Ok(v)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<u128, E> {
                u128::try_from(v).map_err(|_| E::custom("a negative operation id"))
            }
        }
        if deserializer.is_human_readable() {
            deserializer.deserialize_any(Text)
        } else {
            deserializer.deserialize_u128(Text)
        }
    }
}

pub(crate) mod opt_u128_text {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize, Serialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    #[serde(transparent)]
    struct OperationId(
        #[cfg_attr(
            feature = "schemars",
            schemars(schema_with = "crate::encoding::u128_schema")
        )]
        #[serde(with = "super::u128_text")]
        u128,
    );

    pub fn serialize<S: Serializer>(
        value: &Option<u128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(OperationId).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u128>, D::Error> {
        Option::<OperationId>::deserialize(deserializer)
            .map(|value| value.map(|operation| operation.0))
    }
}

/// JSON Schema of a byte field: UTF-8 text or an array of byte values.
#[cfg(feature = "schemars")]
pub(crate) fn bytes_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "description": "Bytes, written as UTF-8 text or as an array of byte values",
        "anyOf": [
            { "type": "string" },
            { "type": "array", "items": { "type": "integer", "minimum": 0, "maximum": 255 } }
        ]
    })
}

/// JSON Schema of an optional byte field.
#[cfg(feature = "schemars")]
pub(crate) fn opt_bytes_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let bytes = bytes_schema(generator);
    schemars::json_schema!({ "anyOf": [bytes, { "type": "null" }] })
}

/// JSON Schema of a list of byte fields.
#[cfg(feature = "schemars")]
pub(crate) fn vec_bytes_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let bytes = bytes_schema(generator);
    schemars::json_schema!({ "type": "array", "items": bytes })
}

/// JSON Schema of a fixed-length byte field, an array of `length` byte values.
#[cfg(feature = "schemars")]
pub(crate) fn fixed_bytes_schema(
    _: &mut schemars::SchemaGenerator,
    length: usize,
) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": { "type": "integer", "minimum": 0, "maximum": 255 },
        "minItems": length,
        "maxItems": length
    })
}

/// JSON Schema of an RFC 6902 JSON Patch.
#[cfg(feature = "schemars")]
pub(crate) fn json_patch_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "description": "RFC 6902 JSON Patch operations",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "op": { "enum": ["add", "remove", "replace", "move", "copy", "test"] },
                "path": { "type": "string" },
                "from": { "type": "string" },
                "value": {}
            },
            "required": ["op", "path"]
        }
    })
}

/// JSON Schema of a `u128` carried as decimal text.
#[cfg(feature = "schemars")]
pub(crate) fn u128_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "description": "Unsigned 128-bit integer as decimal text",
        "type": "string",
        "pattern": "^[0-9]+$"
    })
}

/// JSON Schema of an optional `u128` carried as decimal text.
#[cfg(feature = "schemars")]
pub(crate) fn opt_u128_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let text = u128_schema(generator);
    schemars::json_schema!({ "anyOf": [text, { "type": "null" }] })
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use crate::framing::{decode_named, encode_named};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    struct WithBytes {
        #[cfg_attr(
            feature = "schemars",
            schemars(schema_with = "crate::encoding::bytes_schema")
        )]
        #[serde(with = "super::bin_bytes")]
        payload: Vec<u8>,
        #[cfg_attr(
            feature = "schemars",
            schemars(schema_with = "crate::encoding::opt_bytes_schema")
        )]
        #[serde(default, with = "super::opt_bin_bytes")]
        opt: Option<Vec<u8>>,
    }

    #[test]
    fn given_byte_fields_when_round_tripped_through_cbor_then_should_preserve_bytes() {
        let value = WithBytes {
            payload: vec![0xff, 0x00, 0x10],
            opt: Some(vec![1, 2, 3]),
        };
        let bytes = encode_named(&value).expect("encodes");
        let back: WithBytes = decode_named(&bytes).expect("decodes");
        assert_eq!(back, value);
    }

    #[test]
    fn given_byte_fields_when_round_tripped_through_json_then_should_preserve_bytes() {
        // The HTTP surface serializes the same types as JSON, where bytes are an
        // array of integers. Decode must accept that shape too.
        let value = WithBytes {
            payload: vec![0xff, 0x00, 0x10],
            opt: Some(vec![1, 2, 3]),
        };
        let json = serde_json::to_string(&value).expect("json encodes");
        assert!(json.contains("[255,0,16]"), "bytes render as a JSON array");
        let back: WithBytes = serde_json::from_str(&json).expect("json decodes");
        assert_eq!(back, value);
    }

    #[test]
    fn given_text_for_a_byte_field_when_decoded_from_json_then_should_take_its_utf8_bytes() {
        let back: WithBytes =
            serde_json::from_str(r#"{"payload":"user-1","opt":"x"}"#).expect("json decodes");
        assert_eq!(back.payload, b"user-1".to_vec());
        assert_eq!(back.opt, Some(b"x".to_vec()));
    }

    #[cfg(feature = "schemars")]
    #[test]
    fn given_a_kv_request_when_its_schema_is_built_then_should_take_text_or_bytes_for_the_key() {
        let schema = serde_json::to_value(schemars::schema_for!(crate::kv::KvGet)).expect("schema");
        let key = &schema["properties"]["key"];
        assert_eq!(key["anyOf"][0]["type"], "string", "{schema}");
        assert_eq!(key["anyOf"][1]["type"], "array", "{schema}");
        let request: crate::kv::KvGet = serde_json::from_value(
            serde_json::json!({"v": 1, "namespace": "state", "key": "user-1"}),
        )
        .expect("a text key decodes");
        assert_eq!(request.key, b"user-1".to_vec());
    }

    #[cfg(feature = "schemars")]
    #[test]
    fn given_a_wire_id_when_its_schema_is_built_then_should_be_26_character_text() {
        let schema = serde_json::to_value(schemars::schema_for!(crate::agent::ConversationId))
            .expect("schema");
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 26);
    }

    #[test]
    fn given_absent_option_when_round_tripped_then_should_stay_none() {
        let value = WithBytes {
            payload: Vec::new(),
            opt: None,
        };
        let bytes = encode_named(&value).expect("encodes");
        let back: WithBytes = decode_named(&bytes).expect("decodes");
        assert_eq!(back.opt, None);
    }

    #[test]
    fn given_a_byte_array_with_a_huge_declared_length_when_decoded_then_should_reject_without_oom()
    {
        // A CBOR array header may declare far more elements than the input
        // holds. Decode must reject on the missing bytes, never preallocate a
        // buffer sized to the declared length.
        let bytes = [
            0xa1, // map(1)
            0x67, b'p', b'a', b'y', b'l', b'o', b'a', b'd', // key "payload"
            0x9a, 0xff, 0xff, 0xff, 0xff, // array(4_294_967_295)
        ];
        assert!(decode_named::<WithBytes>(&bytes).is_err());
    }

    #[test]
    fn given_a_wrong_typed_byte_field_when_decoded_then_should_reject() {
        // A non-bytes, non-array value (here a string) must fail rather than
        // silently coerce.
        #[derive(Serialize)]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        struct AsString {
            payload: &'static str,
            opt: Option<Vec<u8>>,
        }
        let bad = AsString {
            payload: "not bytes",
            opt: None,
        };
        let bytes = encode_named(&bad).expect("encodes");
        assert!(decode_named::<WithBytes>(&bytes).is_err());
    }
}
