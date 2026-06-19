use crate::convert::{json_to_py, payload_bytes, py_to_de, py_to_json};
use crate::errors::to_pyerr;
use laser_sdk::query::{SchemaDef, SchemaSource};
use laser_sdk::schema_codecs::CompiledSchema;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// A compiled writer schema: parse a registered schema definition once
/// client-side, then encode / validate / decode bodies against it. Mirrors the
/// Rust `CompiledSchema`. Avro and Protobuf schemas decode their schema-first
/// bodies; a JSON Schema validates the decoded payload of a self-describing
/// codec.
#[gen_stub_pyclass]
#[pyclass(name = "CompiledSchema", frozen)]
pub struct PyCompiledSchema {
    pub(crate) inner: CompiledSchema,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCompiledSchema {
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

    /// Encode a Python value as a raw Avro datum (single-object encoding, no
    /// container header), exactly the bytes a producer stamps alongside
    /// `agdx.sid`. Avro schemas only: a Protobuf or JSON schema raises
    /// `InvalidError`. Encoding fails with `CodecError` when the value does not
    /// match the schema.
    fn encode_avro<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let value = py_to_json(value)?;
        let bytes = self.inner.encode_avro(&value).map_err(to_pyerr)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Whether `payload` (`str`, `bytes`, or `bytearray`) decodes under this
    /// schema (Avro / Protobuf) or, for a JSON Schema, parses as JSON and
    /// passes validation. `False` means the record would fall back to
    /// header-only extraction.
    fn validate(&self, payload: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.inner.validate(&payload_bytes(payload)?))
    }

    /// Validate an already-decoded Python value against a JSON Schema. Avro and
    /// Protobuf schemas return `False`.
    fn validate_value(&self, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.inner.validate_value(&py_to_json(value)?))
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
