use crate::convert::{BodyValue, payload_bytes};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::LaserError;
use laser_sdk::query::{Bson, Consistency};
use laser_sdk::stream::{Cbor, Codec, ContentType, Decoder, Json, Msgpack};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

// One built-in codec class over the Rust codec of the same name. The class
// itself or an instance is a codec object, so it goes wherever a user codec
// does (`PublishRequest.encode_with`, `Kv.get_as`, `QueryRequest.fetch_typed_with`).
macro_rules! codec_class {
    ($class:ident, $codec:ty, $name:literal, $doc:literal) => {
        #[doc = $doc]
        #[gen_stub_pyclass]
        #[pyclass(name = $name, frozen)]
        pub struct $class;

        #[gen_stub_pymethods]
        #[pymethods]
        impl $class {
            #[new]
            fn new() -> Self {
                Self
            }

            /// The `agdx.ct` word stamped on records this codec encodes.
            #[classattr]
            fn content_type() -> String {
                <$codec as Codec<BodyValue>>::content_type().to_string()
            }

            /// Encode `value` (any JSON-shaped value, bytes kept as byte strings
            /// where the format carries them). Raises `CodecError` when the value
            /// has no form in this format.
            #[staticmethod]
            fn encode<'py>(
                py: Python<'py>,
                value: &Bound<'_, PyAny>,
            ) -> PyResult<Bound<'py, PyBytes>> {
                let body = BodyValue::from_py(value)?;
                let bytes = <$codec as Codec<BodyValue>>::encode(&body)
                    .map_err(|error| to_pyerr(LaserError::from(error)))?;
                Ok(PyBytes::new(py, &bytes))
            }

            /// Decode `payload` (`bytes`, `bytearray`, or `str`) into a Python
            /// value. Raises `CodecError` when the payload does not decode.
            #[staticmethod]
            fn decode(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                let body: BodyValue =
                    <$codec as Decoder<BodyValue>>::decode(&payload_bytes(payload)?)
                        .map_err(|error| to_pyerr(LaserError::from(error)))?;
                body.to_py(py)
            }
        }
    };
}

codec_class!(PyJson, Json, "Json", "The built-in JSON codec.");
codec_class!(
    PyMsgpack,
    Msgpack,
    "Msgpack",
    "The built-in MessagePack codec, named-map encoding so field names round-trip with JSON-shaped consumers."
);
codec_class!(
    PyCbor,
    Cbor,
    "Cbor",
    "The built-in CBOR codec, self-describing like JSON with binary byte strings."
);
codec_class!(
    PyBson,
    Bson,
    "Bson",
    "The built-in BSON codec. The top-level value must be a dict."
);

/// The compact `agdx.ct` header code of `content_type` (`raw` is 0, `json` 1,
/// `any` 255).
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn content_type_code(content_type: &str) -> PyResult<u8> {
    Ok(parse_content_type(content_type)?.code())
}

/// Whether `content_type` is the default `raw` codec.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn content_type_is_raw(content_type: &str) -> PyResult<bool> {
    Ok(parse_content_type(content_type)?.is_raw())
}

/// Whether the read-consistency `level` (`eventual`, `read_your_writes`, or
/// `strong`) is the eventual level.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn consistency_is_eventual(level: &str) -> PyResult<bool> {
    let level: Consistency = serde_json::from_value(serde_json::Value::String(level.to_owned()))
        .map_err(|_| {
            InvalidError::new_err(format!(
                "consistency must be 'eventual', 'read_your_writes', or 'strong', got '{level}'"
            ))
        })?;
    Ok(level.is_eventual())
}

fn parse_content_type(content_type: &str) -> PyResult<ContentType> {
    ContentType::from_str(content_type)
        .map_err(|_| InvalidError::new_err(format!("unknown content type '{content_type}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_each_codec_when_a_value_round_trips_then_should_match_the_rust_codec() {
        Python::initialize();
        Python::attach(|py| {
            let value = pyo3::types::PyDict::new(py);
            value.set_item("cpu", 82).expect("cpu");
            value.set_item("host", "edge-1").expect("host");
            let body = BodyValue::from_py(value.as_any()).expect("body");
            let json = PyJson::encode(py, value.as_any()).expect("json encodes");
            assert_eq!(
                json.as_bytes(),
                <Json as Codec<BodyValue>>::encode(&body).expect("rust json")
            );
            let round_trips = [
                PyJson::decode(py, json.as_any()),
                PyMsgpack::decode(
                    py,
                    PyMsgpack::encode(py, value.as_any())
                        .expect("msgpack")
                        .as_any(),
                ),
                PyCbor::decode(
                    py,
                    PyCbor::encode(py, value.as_any()).expect("cbor").as_any(),
                ),
                PyBson::decode(
                    py,
                    PyBson::encode(py, value.as_any()).expect("bson").as_any(),
                ),
            ];
            for decoded in round_trips {
                let decoded = decoded.expect("each codec decodes its own bytes");
                assert!(decoded.bind(py).eq(&value).expect("compare"));
            }
            assert_eq!(PyJson::content_type(), "json");
            assert_eq!(PyCbor::content_type(), "cbor");
            assert!(PyJson::decode(py, PyBytes::new(py, b"{").as_any()).is_err());
        });
    }

    #[test]
    fn given_wire_words_when_classified_then_should_match_the_rust_dictionary() {
        Python::initialize();
        assert_eq!(content_type_code("json").expect("json"), 1);
        assert_eq!(content_type_code("any").expect("any"), 255);
        assert!(content_type_is_raw("raw").expect("raw"));
        assert!(!content_type_is_raw("cbor").expect("cbor"));
        assert!(content_type_code("yaml").is_err());
        assert!(consistency_is_eventual("eventual").expect("eventual"));
        assert!(!consistency_is_eventual("strong").expect("strong"));
        assert!(consistency_is_eventual("linearizable").is_err());
    }
}
