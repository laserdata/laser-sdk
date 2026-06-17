use crate::content::ContentType;
use crate::error::DecodeError;
use crate::kv::KvEntry;
use crate::query::Row;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Encoding strategy for a typed body. Four first-party codecs ship here, all
/// self-describing so LaserData Cloud's projector can index their fields without a
/// schema: [`Json`] (`serde_json`), [`Msgpack`] (`rmp_serde`), [`Cbor`]
/// (`ciborium`), and [`Bson`] (`bson`, native-only feature). For a
/// schema-first format (Avro or Protobuf), Arrow, or your own framing,
/// implement `Codec` on a marker type. The codec advertises its
/// `ContentType` so consumers can decode downstream.
///
/// The trait is generic on `T` so codecs can constrain the body type they
/// accept. Serde-based codecs constrain `T: Serialize`, a Prost codec
/// constrains `T: prost::Message`.
///
/// ```no_run
/// # use laser_wire::codecs::Codec;
/// # use laser_wire::content::ContentType;
/// # use laser_wire::error::DecodeError;
/// struct AvroCodec<S>(std::marker::PhantomData<S>);
/// // Where `S` is your generated Avro schema:
/// // impl<S: AvroSerialize> Codec<S> for AvroCodec<S> {
/// //     fn content_type() -> ContentType { ContentType::Avro }
/// //     fn encode(value: &S) -> Result<Vec<u8>, DecodeError> {
/// //         avro::to_avro_bytes(value).map_err(|e| DecodeError::Encode(e.to_string()))
/// //     }
/// // }
/// ```
pub trait Codec<T: ?Sized> {
    /// The wire-format tag stamped on `agdx.ct`.
    fn content_type() -> ContentType;
    /// Encode `value` into the bytes that ride the Iggy payload.
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError>;
}

/// The decode half of a codec. Separate from [`Codec`] because encoding is
/// `?Sized` (you can encode a `&str`) while decoding must produce an owned,
/// deserializable value. `Json`, `Msgpack`, `Cbor`, and `Bson` implement both.
/// A custom codec (Avro, Protobuf, Arrow, or your own framing) implements only
/// the half it needs.
///
/// ```no_run
/// # use laser_wire::codecs::Decoder;
/// # use laser_wire::error::DecodeError;
/// struct AvroCodec<S>(std::marker::PhantomData<S>);
/// // impl<S: AvroDeserialize> Decoder<S> for AvroCodec<S> {
/// //     fn decode(bytes: &[u8]) -> Result<S, DecodeError> {
/// //         avro::from_avro_bytes(bytes).map_err(|e| DecodeError::Decode(e.to_string()))
/// //     }
/// // }
/// ```
pub trait Decoder<T> {
    /// Decode bytes previously produced by the matching [`Codec::encode`].
    fn decode(bytes: &[u8]) -> Result<T, DecodeError>;
}

/// Built-in JSON codec (`serde_json`). Constrains the body to `Serialize`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Json;

impl<T: Serialize + ?Sized> Codec<T> for Json {
    fn content_type() -> ContentType {
        ContentType::Json
    }
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError> {
        serde_json::to_vec(value)
            .map_err(|error| DecodeError::Encode(format!("encode JSON payload: {error}")))
    }
}

impl<T: DeserializeOwned> Decoder<T> for Json {
    fn decode(bytes: &[u8]) -> Result<T, DecodeError> {
        serde_json::from_slice(bytes)
            .map_err(|error| DecodeError::Decode(format!("decode JSON payload: {error}")))
    }
}

/// Built-in MessagePack codec (`rmp_serde`, named-map encoding so field
/// names round-trip with JSON-shaped consumers). Constrains the body to
/// `Serialize`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Msgpack;

impl<T: Serialize + ?Sized> Codec<T> for Msgpack {
    fn content_type() -> ContentType {
        ContentType::Msgpack
    }
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError> {
        rmp_serde::to_vec_named(value)
            .map_err(|error| DecodeError::Encode(format!("encode msgpack payload: {error}")))
    }
}

impl<T: DeserializeOwned> Decoder<T> for Msgpack {
    fn decode(bytes: &[u8]) -> Result<T, DecodeError> {
        rmp_serde::from_slice(bytes)
            .map_err(|error| DecodeError::Decode(format!("decode msgpack payload: {error}")))
    }
}

/// Built-in CBOR codec (`ciborium`). Self-describing like JSON, so LaserData Cloud's
/// projector can index its fields without a schema. Constrains the body to
/// `Serialize` / `DeserializeOwned`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cbor;

impl<T: Serialize + ?Sized> Codec<T> for Cbor {
    fn content_type() -> ContentType {
        ContentType::Cbor
    }
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError> {
        let mut buffer = Vec::new();
        ciborium::into_writer(value, &mut buffer)
            .map_err(|error| DecodeError::Encode(format!("encode CBOR payload: {error}")))?;
        Ok(buffer)
    }
}

impl<T: DeserializeOwned> Decoder<T> for Cbor {
    fn decode(bytes: &[u8]) -> Result<T, DecodeError> {
        ciborium::from_reader(bytes)
            .map_err(|error| DecodeError::Decode(format!("decode CBOR payload: {error}")))
    }
}

/// Built-in BSON codec (`bson`, behind the native-only `bson` feature: its
/// dependency tree does not build on `wasm32-unknown-unknown`). Self-describing
/// like JSON. The top-level body must be a document (struct or map).
#[cfg(feature = "bson")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Bson;

#[cfg(feature = "bson")]
impl<T: Serialize> Codec<T> for Bson {
    fn content_type() -> ContentType {
        ContentType::Bson
    }
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError> {
        bson::serialize_to_vec(value)
            .map_err(|error| DecodeError::Encode(format!("encode BSON payload: {error}")))
    }
}

#[cfg(feature = "bson")]
impl<T: DeserializeOwned> Decoder<T> for Bson {
    fn decode(bytes: &[u8]) -> Result<T, DecodeError> {
        bson::deserialize_from_slice(bytes)
            .map_err(|error| DecodeError::Decode(format!("decode BSON payload: {error}")))
    }
}

const NO_ROW_PAYLOAD: &str = "row has no payload, the publisher must call .inline_payload() and the query must call .with_payload()";

impl Row {
    /// Decodes the row's payload as JSON into `T`. Errors if no payload was
    /// returned (publisher did not inline it, or the query did not request
    /// it), or if the bytes fail to deserialize.
    pub fn decode_json<T: DeserializeOwned>(&self) -> Result<T, DecodeError> {
        self.decode_with::<Json, T>()
    }

    /// Decodes the row's payload as MessagePack into `T`. Same prerequisites
    /// as [`decode_json`](Self::decode_json).
    pub fn decode_msgpack<T: DeserializeOwned>(&self) -> Result<T, DecodeError> {
        self.decode_with::<Msgpack, T>()
    }

    /// Decode the row's payload with any [`Decoder`] (`Json`, `Msgpack`, or
    /// your own). `decode_json` and `decode_msgpack` are sugar for the
    /// built-ins. Reach for this with a custom codec. Same prerequisites: the
    /// publisher inlined the payload and the query requested it.
    pub fn decode_with<C, T>(&self) -> Result<T, DecodeError>
    where
        C: Decoder<T>,
    {
        let bytes = self
            .payload
            .as_deref()
            .ok_or(DecodeError::MissingPayload(NO_ROW_PAYLOAD))?;
        C::decode(bytes)
    }
}

impl KvEntry {
    /// Decode the value as JSON into `T`. Sugar for `decode_value_with::<Json, _>`.
    pub fn decode_value<T: DeserializeOwned>(&self) -> Result<T, DecodeError> {
        self.decode_value_with::<Json, T>()
    }

    /// Decode the value with any [`Decoder`] (`Json`, `Msgpack`, or your own).
    pub fn decode_value_with<C, T>(&self) -> Result<T, DecodeError>
    where
        C: Decoder<T>,
    {
        C::decode(&self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Body {
        id: u32,
        name: String,
    }

    fn body() -> Body {
        Body {
            id: 7,
            name: "alice".to_owned(),
        }
    }

    #[test]
    fn given_a_row_payload_when_decoded_with_each_codec_then_should_round_trip() {
        let json_row = Row {
            payload: Some(Json::encode(&body()).expect("json encode")),
            ..Default::default()
        };
        assert_eq!(json_row.decode_with::<Json, Body>().expect("json"), body());
        let msgpack_row = Row {
            payload: Some(Msgpack::encode(&body()).expect("msgpack encode")),
            ..Default::default()
        };
        assert_eq!(
            msgpack_row.decode_with::<Msgpack, Body>().expect("msgpack"),
            body()
        );
        let cbor_row = Row {
            payload: Some(Cbor::encode(&body()).expect("cbor encode")),
            ..Default::default()
        };
        assert_eq!(cbor_row.decode_with::<Cbor, Body>().expect("cbor"), body());
    }

    #[cfg(feature = "bson")]
    #[test]
    fn given_a_bson_payload_when_decoded_then_should_round_trip() {
        let bson_row = Row {
            payload: Some(Bson::encode(&body()).expect("bson encode")),
            ..Default::default()
        };
        assert_eq!(bson_row.decode_with::<Bson, Body>().expect("bson"), body());
        assert_eq!(<Bson as Codec<Body>>::content_type(), ContentType::Bson);
    }

    #[test]
    fn given_each_codec_when_encoding_then_should_advertise_its_content_type() {
        assert_eq!(<Json as Codec<str>>::content_type(), ContentType::Json);
        assert_eq!(
            <Msgpack as Codec<str>>::content_type(),
            ContentType::Msgpack
        );
        assert_eq!(<Cbor as Codec<str>>::content_type(), ContentType::Cbor);
    }

    #[test]
    fn given_a_row_without_payload_when_decoded_then_should_error() {
        let row = Row::default();
        assert!(matches!(
            row.decode_with::<Json, String>(),
            Err(DecodeError::MissingPayload(_))
        ));
    }

    #[test]
    fn given_a_msgpack_value_when_round_tripped_through_the_entry_then_should_decode_back() {
        let encoded = Msgpack::encode(&vec!["a", "b"]).expect("encode");
        let entry = KvEntry {
            key: b"k".to_vec(),
            value: encoded,
            expires_at_micros: None,
            version: 0,
        };
        let decoded: Vec<String> = entry.decode_value_with::<Msgpack, _>().expect("decode");
        assert_eq!(decoded, vec!["a".to_owned(), "b".to_owned()]);
    }
}
