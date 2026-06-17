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
// JSON surface). Any other type fails, so wrong-typed data is rejected.

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

/// `Vec<u8>` as a CBOR byte string (or a JSON array of bytes on the HTTP view).
pub(crate) mod bin_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_any(ByteVecVisitor)
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
            Some(bytes) => serializer.serialize_some(&ByteBuf(bytes)),
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

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use crate::framing::{decode_named, encode_named};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct WithBytes {
        #[serde(with = "super::bin_bytes")]
        payload: Vec<u8>,
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
