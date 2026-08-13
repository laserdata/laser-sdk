use crate::errors::{CodecError, InvalidError};
use laser_sdk::query::{TypedValue, Value};
use laser_sdk::wire::schema::{BinaryValue, DecimalValue, UuidValue};
use pyo3::prelude::*;
use pyo3::types::{
    PyByteArray, PyBytes, PyDate, PyDateTime, PyFloat, PyInt, PyList, PyString, PyTime, PyTuple,
};
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
