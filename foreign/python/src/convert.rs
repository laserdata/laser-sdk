use crate::errors::{CodecError, InvalidError};
use laser_sdk::query::{TypedValue, Value};
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::schema::{BinaryValue, DecimalValue, UuidValue};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyString,
    PyTime, PyTuple,
};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

const MAX_DECIMAL_PRECISION: u32 = 38;

pub(crate) fn duration_seconds(value: f64, name: &str) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value).map_err(|_| {
        InvalidError::new_err(format!(
            "{name} must be a finite, non-negative number of seconds"
        ))
    })
}

// Convert a Python scalar (or list/tuple of scalars) into a query `Value`.
// `bool` is checked before `int` because Python's `bool` is an `int` subclass.
pub(crate) fn py_to_typed_value(obj: &Bound<'_, PyAny>) -> PyResult<TypedValue> {
    if obj.is_none() {
        return Ok(TypedValue::Null);
    }
    if let Ok(value) = obj.extract::<bool>() {
        return Ok(TypedValue::Boolean(value));
    }
    // An `int` is matched on its concrete type, so one too large for `i64` or
    // `u64` raises instead of falling through to the float branch, where
    // `__float__` would silently turn `2**64` into an imprecise comparison.
    if obj.is_instance_of::<PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            return Ok(TypedValue::Long(value));
        }
        return Err(InvalidError::new_err(
            "query value integer is out of range: it must fit a signed 64-bit integer",
        ));
    }
    if obj.is_instance_of::<PyFloat>() {
        let value = obj.extract::<f64>()?;
        if !value.is_finite() {
            return Err(InvalidError::new_err("query value float must be finite"));
        }
        return Ok(TypedValue::Double(value));
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(TypedValue::String(text.to_str()?.to_owned()));
    }
    if let Ok(payload) = obj.cast::<PyBytes>() {
        return Ok(TypedValue::Binary(BinaryValue(payload.as_bytes().to_vec())));
    }
    if let Ok(payload) = obj.cast::<PyByteArray>() {
        return Ok(TypedValue::Binary(BinaryValue(payload.to_vec())));
    }
    // `datetime` before `date`: `datetime.datetime` subclasses `datetime.date`.
    if let Ok(value) = obj.cast::<PyDateTime>() {
        return datetime_to_typed_value(value);
    }
    if let Ok(value) = obj.cast::<PyDate>() {
        return date_to_typed_value(value);
    }
    if let Ok(value) = obj.cast::<PyTime>() {
        return time_to_typed_value(value);
    }
    if is_instance_of_named(obj, "uuid", "UUID")? {
        let bytes: [u8; 16] = obj.getattr("bytes")?.extract()?;
        return Ok(TypedValue::Uuid(UuidValue::new(bytes)));
    }
    if is_instance_of_named(obj, "decimal", "Decimal")? {
        return decimal_to_typed_value(obj);
    }
    // Only real sequences become a list. A `dict` would otherwise fold to its
    // keys and `bytes` to a list of integers, both silently.
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        let mut list = Vec::new();
        for item in obj.try_iter()? {
            list.push(py_to_typed_value(&item?)?);
        }
        return Ok(TypedValue::List(list));
    }
    Err(InvalidError::new_err(
        "query value must be str, int, float, bool, None, bytes, uuid.UUID, decimal.Decimal, \
         datetime.date, datetime.time, datetime.datetime, or a list of those",
    ))
}

fn is_instance_of_named(obj: &Bound<'_, PyAny>, module: &str, name: &str) -> PyResult<bool> {
    let type_object = obj.py().import(module)?.getattr(name)?;
    obj.is_instance(&type_object)
}

// A tz-aware `datetime` is a UTC instant (`timestamp_tz_micros`), a naive one a
// zone-less local timestamp (`timestamp_micros`). Both subtract the matching
// epoch so the microsecond count is exact integer arithmetic, never a float
// round trip through `datetime.timestamp()`.
fn datetime_to_typed_value(value: &Bound<'_, PyDateTime>) -> PyResult<TypedValue> {
    let aware = !value.getattr("tzinfo")?.is_none();
    let datetime_module = value.py().import("datetime")?;
    let datetime_type = datetime_module.getattr("datetime")?;
    let epoch = if aware {
        let utc = datetime_module.getattr("timezone")?.getattr("utc")?;
        datetime_type.call1((1970, 1, 1, 0, 0, 0, 0, utc))?
    } else {
        datetime_type.call1((1970, 1, 1))?
    };
    let delta = value.call_method1("__sub__", (epoch,))?;
    let days: i64 = delta.getattr("days")?.extract()?;
    let seconds: i64 = delta.getattr("seconds")?.extract()?;
    let microseconds: i64 = delta.getattr("microseconds")?.extract()?;
    let micros = days
        .checked_mul(86_400_000_000)
        .and_then(|value| value.checked_add(seconds * 1_000_000))
        .and_then(|value| value.checked_add(microseconds))
        .ok_or_else(|| {
            InvalidError::new_err("datetime is outside the microsecond timestamp range")
        })?;
    Ok(if aware {
        TypedValue::TimestampTzMicros(micros)
    } else {
        TypedValue::TimestampMicros(micros)
    })
}

fn date_to_typed_value(value: &Bound<'_, PyDate>) -> PyResult<TypedValue> {
    // 719_163 is `date(1970, 1, 1).toordinal()`.
    let ordinal: i64 = value.call_method0("toordinal")?.extract()?;
    let days = i32::try_from(ordinal - 719_163)
        .map_err(|_| InvalidError::new_err("date is outside the supported day range"))?;
    Ok(TypedValue::Date(days))
}

fn time_to_typed_value(value: &Bound<'_, PyTime>) -> PyResult<TypedValue> {
    if !value.getattr("tzinfo")?.is_none() {
        return Err(InvalidError::new_err(
            "time value must not carry tzinfo: the wire time type is zone-less",
        ));
    }
    let hour: i64 = value.getattr("hour")?.extract()?;
    let minute: i64 = value.getattr("minute")?.extract()?;
    let second: i64 = value.getattr("second")?.extract()?;
    let microsecond: i64 = value.getattr("microsecond")?.extract()?;
    Ok(TypedValue::TimeMicros(
        ((hour * 60 + minute) * 60 + second) * 1_000_000 + microsecond,
    ))
}

// Convert `decimal.Decimal` into the canonical wire form: minimal two's
// complement unscaled bytes with the smallest precision that fits the digits
// and scale. A value whose scale or digit count exceeds 38 has no wire form.
fn decimal_to_typed_value(obj: &Bound<'_, PyAny>) -> PyResult<TypedValue> {
    if !obj.call_method0("is_finite")?.extract::<bool>()? {
        return Err(InvalidError::new_err("decimal value must be finite"));
    }
    let parts = obj.call_method0("as_tuple")?;
    let negative = parts.getattr("sign")?.extract::<u8>()? == 1;
    let digits: Vec<u8> = parts.getattr("digits")?.extract()?;
    let exponent: i64 = parts.getattr("exponent")?.extract()?;

    let mut unscaled: i128 = 0;
    for digit in &digits {
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(*digit)))
            .ok_or_else(|| InvalidError::new_err("decimal value exceeds 38 digits"))?;
    }
    if negative {
        unscaled = -unscaled;
    }
    let scale = if exponent > 0 {
        for _ in 0..exponent {
            unscaled = unscaled
                .checked_mul(10)
                .ok_or_else(|| InvalidError::new_err("decimal value exceeds 38 digits"))?;
        }
        0
    } else {
        u32::try_from(-exponent)
            .map_err(|_| InvalidError::new_err("decimal scale is out of range"))?
    };

    let digit_count = if unscaled == 0 {
        1
    } else {
        unscaled.unsigned_abs().ilog10() + 1
    };
    let precision = digit_count.max(scale).max(1);
    if precision > MAX_DECIMAL_PRECISION {
        return Err(InvalidError::new_err(format!(
            "decimal needs precision {precision}, the wire maximum is {MAX_DECIMAL_PRECISION}"
        )));
    }

    let value = DecimalValue {
        unscaled: minimal_two_complement_bytes(unscaled),
        precision: precision as u8,
        scale: scale as u8,
    };
    value
        .validate_canonical()
        .map_err(|error| InvalidError::new_err(error.to_string()))?;
    Ok(TypedValue::Decimal(value))
}

fn minimal_two_complement_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let first = bytes[start];
        let second = bytes[start + 1];
        if (first == 0 && second & 0x80 == 0) || (first == 0xff && second & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

pub(crate) fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(value) = obj.extract::<bool>() {
        return Ok(Value::Bool(value));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            return Ok(Value::Int(value));
        }
        if let Ok(value) = obj.extract::<u64>() {
            return Ok(Value::Uint(value));
        }
        return Err(InvalidError::new_err(
            "value integer is out of the signed and unsigned 64-bit range",
        ));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::Float(obj.extract::<f64>()?));
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(Value::Str(text.to_str()?.to_owned()));
    }
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        let mut list = Vec::new();
        for item in obj.try_iter()? {
            list.push(py_to_value(&item?)?);
        }
        return Ok(Value::List(list));
    }
    Err(InvalidError::new_err(
        "value must be str, int, float, bool, None, or a list of those",
    ))
}

// A payload argument accepts `str` (UTF-8 encoded), `bytes`, or `bytearray`,
// always producing owned bytes for the wire. Downcast to the concrete Python type
// so the buffer is read and copied exactly once, with no speculative `str`
// decode attempted over binary input.
pub(crate) fn payload_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(payload) = obj.cast::<PyBytes>() {
        return Ok(payload.as_bytes().to_vec());
    }
    if let Ok(payload) = obj.cast::<PyByteArray>() {
        return Ok(payload.to_vec());
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(text.to_str()?.as_bytes().to_vec());
    }
    Err(InvalidError::new_err(
        "payload must be str, bytes, or bytearray",
    ))
}

// Depythonize an arbitrary Python value (dict / list / scalar) into a
// `serde_json::Value` the typed `.json(..)` builders serialize onto the wire.
pub(crate) fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    pythonize::depythonize(obj).map_err(|error| CodecError::new_err(error.to_string()))
}

// Rebuild a Python value from a `serde_json::Value` (query rows, KV reads).
pub(crate) fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    let bound =
        pythonize::pythonize(py, value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(bound.unbind())
}

// Depythonize a Python value directly into any deserializable wire type. Lets a
// Python dict stand in for a structured managed input (a projection, a binding,
// a schema source) without a hand-written class per type.
pub(crate) fn py_to_de<T: serde::de::DeserializeOwned>(obj: &Bound<'_, PyAny>) -> PyResult<T> {
    pythonize::depythonize(obj).map_err(|error| CodecError::new_err(error.to_string()))
}

// Serialize any wire type into a Python value (dicts / lists / scalars). Used
// for structured managed replies (projection info, schema info).
pub(crate) fn ser_to_py<T: serde::Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>> {
    let bound =
        pythonize::pythonize(py, value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(bound.unbind())
}

// A user codec is any object with `encode(value) -> bytes` and
// `decode(data) -> value`, the peer of the Rust `Codec` and `Decoder` traits.
// A failure that is not already an SDK error surfaces as `CodecError` with
// the codec's exception as its cause.
pub(crate) fn codec_encode(
    codec: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<u8>> {
    let encoded = codec
        .call_method1("encode", (value,))
        .map_err(|error| codec_failure(codec.py(), error, "encode"))?;
    if let Ok(bytes) = encoded.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Ok(bytes) = encoded.cast::<PyByteArray>() {
        return Ok(bytes.to_vec());
    }
    Err(CodecError::new_err(
        "codec encode must return bytes or bytearray",
    ))
}

pub(crate) fn codec_decode(codec: &Bound<'_, PyAny>, payload: &[u8]) -> PyResult<Py<PyAny>> {
    codec
        .call_method1("decode", (PyBytes::new(codec.py(), payload),))
        .map(Bound::unbind)
        .map_err(|error| codec_failure(codec.py(), error, "decode"))
}

// The content type a codec publishes under: the explicit argument, else the
// codec's own `content_type` attribute, else none (a raw payload).
pub(crate) fn codec_content_type(
    codec: &Bound<'_, PyAny>,
    explicit: Option<String>,
) -> PyResult<Option<ContentType>> {
    let declared = match explicit {
        Some(value) => Some(value),
        None => match codec.getattr_opt("content_type")? {
            Some(value) if !value.is_none() => Some(value.extract::<String>()?),
            _ => None,
        },
    };
    declared
        .map(|value| {
            ContentType::from_str(&value)
                .map_err(|_| InvalidError::new_err(format!("unknown content type '{value}'")))
        })
        .transpose()
}

fn codec_failure(py: Python<'_>, error: PyErr, operation: &str) -> PyErr {
    if error.is_instance_of::<crate::errors::LaserError>(py) {
        return error;
    }
    let failure = CodecError::new_err(format!("codec {operation} failed: {error}"));
    failure.set_cause(py, Some(error));
    failure
}

/// A typed body in the serde data model, keeping what JSON drops: byte
/// strings, map keys that are not text, and integers wider than 64 bits. CBOR
/// carries all of them, a JSON or Avro lowering applies its own rules, so a
/// Python body round-trips the way the same Rust type does.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BodyValue {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    List(Vec<BodyValue>),
    Map(Vec<(BodyValue, BodyValue)>),
}

impl BodyValue {
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if obj.is_none() {
            return Ok(Self::Null);
        }
        if let Ok(value) = obj.cast::<PyBool>() {
            return Ok(Self::Bool(value.is_true()));
        }
        if obj.is_instance_of::<PyInt>() {
            return obj.extract::<i128>().map(Self::Int).map_err(|_| {
                CodecError::new_err("body integer is out of the signed 128-bit range")
            });
        }
        if obj.is_instance_of::<PyFloat>() {
            return Ok(Self::Float(obj.extract::<f64>()?));
        }
        if let Ok(text) = obj.cast::<PyString>() {
            return Ok(Self::Text(text.to_str()?.to_owned()));
        }
        if let Ok(bytes) = obj.cast::<PyBytes>() {
            return Ok(Self::Bytes(bytes.as_bytes().to_vec()));
        }
        if let Ok(bytes) = obj.cast::<PyByteArray>() {
            return Ok(Self::Bytes(bytes.to_vec()));
        }
        if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
            return obj
                .try_iter()?
                .map(|item| Self::from_py(&item?))
                .collect::<PyResult<Vec<_>>>()
                .map(Self::List);
        }
        if let Ok(map) = obj.cast::<PyDict>() {
            return map
                .iter()
                .map(|(key, value)| Ok((Self::from_py(&key)?, Self::from_py(&value)?)))
                .collect::<PyResult<Vec<_>>>()
                .map(Self::Map);
        }
        Err(CodecError::new_err(format!(
            "body value of type {} has no serde form",
            obj.get_type().name()?
        )))
    }

    pub(crate) fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Null => py.None(),
            Self::Bool(value) => PyBool::new(py, *value).to_owned().into_any().unbind(),
            Self::Int(value) => value.into_pyobject(py)?.into_any().unbind(),
            Self::Float(value) => value.into_pyobject(py)?.into_any().unbind(),
            Self::Text(value) => PyString::new(py, value).into_any().unbind(),
            Self::Bytes(value) => PyBytes::new(py, value).into_any().unbind(),
            Self::List(items) => PyList::new(
                py,
                items
                    .iter()
                    .map(|item| item.to_py(py))
                    .collect::<PyResult<Vec<_>>>()?,
            )?
            .into_any()
            .unbind(),
            Self::Map(entries) => {
                let map = PyDict::new(py);
                for (key, value) in entries {
                    map.set_item(key.to_py_key(py)?, value.to_py(py)?)?;
                }
                map.into_any().unbind()
            }
        })
    }

    // A list key becomes a tuple so it can key a dict.
    fn to_py_key(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::List(items) => Ok(PyTuple::new(
                py,
                items
                    .iter()
                    .map(|item| item.to_py_key(py))
                    .collect::<PyResult<Vec<_>>>()?,
            )?
            .into_any()
            .unbind()),
            Self::Map(_) => Err(CodecError::new_err("a map cannot key a Python dict")),
            other => other.to_py(py),
        }
    }
}

impl Serialize for BodyValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => match (i64::try_from(*value), u64::try_from(*value)) {
                (Ok(value), _) => serializer.serialize_i64(value),
                (_, Ok(value)) => serializer.serialize_u64(value),
                _ => serializer.serialize_i128(*value),
            },
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::Text(value) => serializer.serialize_str(value),
            Self::Bytes(value) => serializer.serialize_bytes(value),
            Self::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Self::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for BodyValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BodyValueVisitor)
    }
}

struct BodyValueVisitor;

impl<'de> Visitor<'de> for BodyValueVisitor {
    type Value = BodyValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any self-describing value")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<BodyValue, E> {
        Ok(BodyValue::Bool(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<BodyValue, E> {
        Ok(BodyValue::Int(value.into()))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<BodyValue, E> {
        Ok(BodyValue::Int(value.into()))
    }

    fn visit_i128<E: serde::de::Error>(self, value: i128) -> Result<BodyValue, E> {
        Ok(BodyValue::Int(value))
    }

    fn visit_u128<E: serde::de::Error>(self, value: u128) -> Result<BodyValue, E> {
        i128::try_from(value)
            .map(BodyValue::Int)
            .map_err(|_| E::custom("integer is out of the signed 128-bit range"))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<BodyValue, E> {
        Ok(BodyValue::Float(value))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<BodyValue, E> {
        Ok(BodyValue::Text(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<BodyValue, E> {
        Ok(BodyValue::Text(value))
    }

    fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<BodyValue, E> {
        Ok(BodyValue::Bytes(value.to_vec()))
    }

    fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<BodyValue, E> {
        Ok(BodyValue::Bytes(value))
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<BodyValue, E> {
        Ok(BodyValue::Null)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<BodyValue, E> {
        Ok(BodyValue::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<BodyValue, D::Error> {
        BodyValue::deserialize(deserializer)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<BodyValue, D::Error> {
        BodyValue::deserialize(deserializer)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<BodyValue, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(BodyValue::List(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<BodyValue, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry()? {
            entries.push(entry);
        }
        Ok(BodyValue::Map(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::stream::{Cbor, Codec, Decoder};

    #[test]
    fn given_bytes_and_integer_keys_when_round_tripped_through_cbor_then_should_keep_them() {
        let body = BodyValue::Map(vec![
            (BodyValue::Int(7), BodyValue::Bytes(vec![0, 255, 1])),
            (
                BodyValue::Text("wide".to_owned()),
                BodyValue::Int(i128::from(u64::MAX) + 1),
            ),
            (
                BodyValue::Text("list".to_owned()),
                BodyValue::List(vec![BodyValue::Null, BodyValue::Float(1.5)]),
            ),
        ]);
        let encoded = <Cbor as Codec<BodyValue>>::encode(&body).expect("the body encodes");
        let decoded = <Cbor as Decoder<BodyValue>>::decode(&encoded).expect("the body decodes");
        assert_eq!(decoded, body);
    }

    fn codecs(py: Python<'_>) -> Bound<'_, PyModule> {
        let module = PyModule::from_code(
            py,
            c"
class Reverse:
    content_type = 'cbor'

    def encode(self, value):
        return value[::-1]

    def decode(self, data):
        return data[::-1]

class Broken:
    def encode(self, value):
        raise ValueError('no encoding')

    def decode(self, data):
        raise ValueError('no decoding')

class Refusing:
    def encode(self, value):
        raise InvalidError('refused')

class Textual:
    def encode(self, value):
        return 'text'
",
            c"codecs.py",
            c"codecs",
        )
        .expect("the codec module compiles");
        module
            .dict()
            .set_item("InvalidError", py.get_type::<InvalidError>())
            .expect("expose InvalidError");
        module
    }

    #[test]
    fn given_a_user_codec_when_encoding_and_decoding_then_should_round_trip_and_wrap_failures() {
        Python::initialize();
        Python::attach(|py| {
            let module = codecs(py);
            let codec = |name: &str| module.getattr(name).and_then(|class| class.call0());
            let reverse = codec("Reverse").expect("Reverse codec");
            let value = PyBytes::new(py, b"abc");
            assert_eq!(
                codec_encode(&reverse, value.as_any()).expect("encodes"),
                b"cba"
            );
            let decoded = codec_decode(&reverse, b"cba").expect("decodes");
            assert_eq!(decoded.extract::<Vec<u8>>(py).expect("bytes"), b"abc");

            let broken = codec("Broken").expect("Broken codec");
            let failure = codec_encode(&broken, value.as_any()).unwrap_err();
            assert!(failure.is_instance_of::<CodecError>(py));
            assert!(
                failure
                    .cause(py)
                    .is_some_and(|cause| cause.is_instance_of::<pyo3::exceptions::PyValueError>(py))
            );
            assert!(
                codec_decode(&broken, b"x")
                    .unwrap_err()
                    .is_instance_of::<CodecError>(py)
            );
            let refusing = codec("Refusing").expect("Refusing codec");
            assert!(
                codec_encode(&refusing, value.as_any())
                    .unwrap_err()
                    .is_instance_of::<InvalidError>(py)
            );
            let textual = codec("Textual").expect("Textual codec");
            assert!(
                codec_encode(&textual, value.as_any())
                    .unwrap_err()
                    .is_instance_of::<CodecError>(py)
            );
        });
    }

    #[test]
    fn given_a_codec_content_type_when_resolved_then_should_prefer_the_explicit_argument() {
        Python::initialize();
        Python::attach(|py| {
            let module = codecs(py);
            let reverse = module
                .getattr("Reverse")
                .and_then(|class| class.call0())
                .expect("codec");
            let broken = module
                .getattr("Broken")
                .and_then(|class| class.call0())
                .expect("codec");
            assert_eq!(
                codec_content_type(&reverse, None).expect("declared"),
                Some(ContentType::Cbor)
            );
            assert_eq!(
                codec_content_type(&reverse, Some("json".to_owned())).expect("explicit"),
                Some(ContentType::Json)
            );
            assert_eq!(codec_content_type(&broken, None).expect("undeclared"), None);
            assert!(
                codec_content_type(&broken, Some("nope".to_owned()))
                    .unwrap_err()
                    .is_instance_of::<InvalidError>(py)
            );
        });
    }

    #[test]
    fn given_a_python_body_with_bytes_and_integer_keys_when_converted_then_should_round_trip() {
        Python::initialize();
        Python::attach(|py| {
            let body = PyDict::new(py);
            body.set_item(3, PyBytes::new(py, b"\x00\xff"))
                .expect("set an integer key");
            body.set_item("flag", true).expect("set a bool");
            let value = BodyValue::from_py(body.as_any()).expect("the body converts");
            assert_eq!(
                value,
                BodyValue::Map(vec![
                    (BodyValue::Int(3), BodyValue::Bytes(vec![0, 255])),
                    (BodyValue::Text("flag".to_owned()), BodyValue::Bool(true)),
                ])
            );
            let back = value.to_py(py).expect("the value converts back");
            assert!(back.bind(py).eq(&body).expect("dicts compare"));
            assert!(BodyValue::from_py(py.get_type::<PyDict>().as_any()).is_err());
        });
    }
}
