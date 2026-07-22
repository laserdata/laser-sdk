use crate::errors::{CodecError, InvalidError};
use laser_sdk::query::Value;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyString};

// Convert a Python scalar (or list of scalars) into a query `Value`. `bool` is
// checked before `int` because Python's `bool` is an `int` subclass.
pub(crate) fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(value) = obj.extract::<bool>() {
        return Ok(Value::Bool(value));
    }
    if let Ok(value) = obj.extract::<i64>() {
        return Ok(Value::Int(value));
    }
    if let Ok(value) = obj.extract::<u64>() {
        return Ok(Value::Uint(value));
    }
    if let Ok(value) = obj.extract::<f64>() {
        return Ok(Value::Float(value));
    }
    if let Ok(value) = obj.extract::<String>() {
        return Ok(Value::Str(value));
    }
    if let Ok(items) = obj.try_iter() {
        let mut list = Vec::new();
        for item in items {
            list.push(py_to_value(&item?)?);
        }
        return Ok(Value::List(list));
    }
    Err(InvalidError::new_err(
        "query value must be str, int, float, bool, None, or a list of those",
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
