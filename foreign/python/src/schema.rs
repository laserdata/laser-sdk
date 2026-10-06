use crate::convert::{BodyValue, json_to_py, payload_bytes, py_to_de, ser_to_py};
use crate::errors::{CodecError, to_pyerr};
use laser_sdk::LaserError;
use laser_sdk::query::{SchemaDef, SchemaSource};
use laser_sdk::schema_codecs::CompiledSchema;
use laser_sdk::wire::schema::{
    DecimalValue, LogicalField, LogicalSchema, LogicalSchemaId, LogicalType, TypedValue,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

/// A compiled writer schema: parse a registered schema definition once
/// client-side, then encode / validate / decode bodies against it. Mirrors the
/// Rust `CompiledSchema`. Avro and Protobuf schemas decode their schema-first
/// bodies. A JSON Schema validates the decoded payload of a self-describing
/// codec.
#[gen_stub_pyclass]
#[pyclass(name = "CompiledSchema", frozen)]
pub struct PyCompiledSchema {
    pub(crate) inner: CompiledSchema,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCompiledSchema {
    /// The `kind` of a compiled Avro writer schema.
    #[classattr]
    const AVRO: &'static str = "avro";
    /// The `kind` of a resolved Protobuf message descriptor.
    #[classattr]
    const PROTOBUF: &'static str = "protobuf";
    /// The `kind` of a compiled JSON Schema validator.
    #[classattr]
    const JSON: &'static str = "json_schema";

    /// Which schema family this compiled to: `CompiledSchema.AVRO`,
    /// `PROTOBUF`, or `JSON`.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner {
            CompiledSchema::Avro(_) => Self::AVRO,
            CompiledSchema::Protobuf(_) => Self::PROTOBUF,
            CompiledSchema::Json(_) => Self::JSON,
        }
    }

    /// Compile a schema from a source dict (the same shape `register_schema`
    /// takes: `{"kind":"avro","schema":...}`, `{"kind":"protobuf",...}`, or
    /// `{"kind":"json_schema","schema":...}`). `id` labels the definition for
    /// error messages and is otherwise unused client-side. Raises
    /// `InvalidError` when the definition does not parse.
    #[staticmethod]
    #[pyo3(signature = (source, *, id=0, name=None, version=None))]
    fn compile(
        source: &Bound<'_, PyAny>,
        id: u32,
        name: Option<String>,
        version: Option<u32>,
    ) -> PyResult<Self> {
        let source: SchemaSource = py_to_de(source)?;
        let def = SchemaDef {
            id,
            source,
            name,
            version,
        };
        Ok(Self {
            inner: CompiledSchema::compile(&def).map_err(to_pyerr)?,
        })
    }

    /// Encode a Python `body` as a raw Avro datum (single-object encoding, no
    /// container header), exactly the bytes a producer stamps alongside
    /// `agdx.sid`. Avro schemas only: a Protobuf or JSON schema raises
    /// `InvalidError`. Encoding fails with `CodecError` when the value does not
    /// match the schema.
    fn encode_avro<'py>(
        &self,
        py: Python<'py>,
        body: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let body = BodyValue::from_py(body)?;
        let payload = self.inner.encode_avro(&body).map_err(to_pyerr)?;
        Ok(PyBytes::new(py, &payload))
    }

    /// Whether `payload` (`str`, `bytes`, or `bytearray`) decodes under this
    /// schema (Avro / Protobuf) or, for a JSON Schema, parses as JSON and
    /// passes validation. `False` means the record would fall back to
    /// header-only extraction.
    fn validate(&self, payload: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.inner.validate(&payload_bytes(payload)?))
    }

    /// Validate an already-decoded Python value against a JSON Schema. Avro and
    /// Protobuf schemas return `False`. The value lowers to JSON like a serde
    /// body: bytes become an array of integers and integer keys become text.
    fn validate_value(&self, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let value = serde_json::to_value(BodyValue::from_py(value)?)
            .map_err(|error| CodecError::new_err(error.to_string()))?;
        Ok(self.inner.validate_value(&value))
    }

    /// Decode `payload` under this schema into a Python value, the model the
    /// managed plane extracts indexed fields from. Raises `CodecError` when the
    /// payload does not decode.
    fn decode(&self, py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let value = self
            .inner
            .decode(&payload_bytes(payload)?)
            .map_err(to_pyerr)?;
        json_to_py(py, &value)
    }
}

/// A logical schema: its versioned identity, its ordered fields, and the
/// fingerprint over their canonical bytes. Construction validates the field
/// shape and computes the fingerprint, so a built schema is always consistent.
#[gen_stub_pyclass]
#[pyclass(name = "LogicalSchema", frozen)]
pub struct PyLogicalSchema {
    inner: LogicalSchema,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLogicalSchema {
    /// Build schema `id` at `version` from logical field dicts. Raises
    /// `InvalidError` when a field is malformed.
    #[new]
    fn new(id: u128, version: u32, fields: &Bound<'_, PyAny>) -> PyResult<Self> {
        let fields: Vec<LogicalField> = py_to_de(fields)?;
        let inner = LogicalSchema::new(LogicalSchemaId::from_u128(id), version, fields)
            .map_err(|error| to_pyerr(LaserError::from(error)))?;
        Ok(Self { inner })
    }

    /// The schema reference dict: `id`, `version`, and `fingerprint`.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.schema)
    }

    /// The ordered logical field dicts.
    #[getter]
    fn fields(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.fields)
    }

    /// The canonical bytes the fingerprint digests.
    fn canonical_fingerprint_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .canonical_fingerprint_bytes()
            .map_err(|error| to_pyerr(LaserError::from(error)))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Recompute the fingerprint from the canonical bytes.
    fn compute_fingerprint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let fingerprint = self
            .inner
            .compute_fingerprint()
            .map_err(|error| to_pyerr(LaserError::from(error)))?;
        Ok(PyBytes::new(py, &fingerprint.0))
    }

    /// The schema as its wire dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }
}

/// The integer in a tagged query value dict as a signed integer, or `None`
/// when it is not an integer.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_as_i64(value: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    Ok(py_to_de::<TypedValue>(value)?.as_i64())
}

/// The integer in a tagged query value dict as an unsigned integer, or `None`
/// when it is negative or not an integer.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_as_u64(value: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    Ok(py_to_de::<TypedValue>(value)?.as_u64())
}

/// The text in a tagged query value dict, or `None` when it is not a string.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_as_str(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    Ok(py_to_de::<TypedValue>(value)?.as_str().map(str::to_owned))
}

/// A tagged query value dict in its stable diagnostic text form.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_diagnostic_text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(py_to_de::<TypedValue>(value)?.diagnostic_text())
}

/// Raise `InvalidError` unless a tagged query value dict is in canonical form.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_validate_canonical(value: &Bound<'_, PyAny>) -> PyResult<()> {
    py_to_de::<TypedValue>(value)?
        .validate_canonical()
        .map_err(|error| to_pyerr(LaserError::from(error)))
}

/// Raise `InvalidError` unless a tagged query value dict fits the logical
/// type dict `logical_type`. A `required` field rejects null.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn typed_value_validate_against(
    value: &Bound<'_, PyAny>,
    logical_type: &Bound<'_, PyAny>,
    required: bool,
) -> PyResult<()> {
    let logical_type: LogicalType = py_to_de(logical_type)?;
    py_to_de::<TypedValue>(value)?
        .validate_against(&logical_type, required)
        .map_err(|error| to_pyerr(LaserError::from(error)))
}

/// The kind name of a logical type dict, such as `long` or `struct`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn logical_type_kind(py: Python<'_>, logical_type: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &py_to_de::<LogicalType>(logical_type)?.kind())
}

/// Whether a logical type dict may key a map.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn logical_type_accepts_map_key(logical_type: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(py_to_de::<LogicalType>(logical_type)?.accepts_map_key())
}

/// Raise `InvalidError` unless a decimal value dict is in canonical form.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decimal_value_validate_canonical(value: &Bound<'_, PyAny>) -> PyResult<()> {
    py_to_de::<DecimalValue>(value)?
        .validate_canonical()
        .map_err(|error| to_pyerr(LaserError::from(error)))
}
