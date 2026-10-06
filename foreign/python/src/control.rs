use crate::convert::{py_to_de, ser_to_py};
use crate::errors::InvalidError;
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::control::{
    FieldType, IndexField, IndexSchema, IndexSchemaBuilder, Projection, ProjectionBinding,
    ProjectionBindingBuilder, ProjectionBuilder, ProjectionKind, SchemaDef, SourceSelector,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

/// Fluent builder for an index schema dict, the `extraction` of a projection.
/// `build()` returns the dict and spends the builder.
#[gen_stub_pyclass]
#[pyclass(name = "IndexSchemaBuilder")]
pub struct PyIndexSchemaBuilder {
    inner: Option<IndexSchemaBuilder>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyIndexSchemaBuilder {
    /// Start building an index schema.
    #[new]
    fn new() -> Self {
        Self {
            inner: Some(IndexSchema::builder()),
        }
    }

    /// Index the root-level payload field `name` under the same index key.
    fn field(mut slf: PyRefMut<'_, Self>, name: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.field(name))?;
        Ok(slf)
    }

    /// Index the JSON value at `pointer` (RFC-6901) under the index key `name`.
    fn field_at(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        pointer: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.field_at(name, pointer))?;
        Ok(slf)
    }

    /// Point the vector extractor at `pointer` instead of `/embedding`.
    fn vector_field(mut slf: PyRefMut<'_, Self>, pointer: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.vector_field(pointer))?;
        Ok(slf)
    }

    /// Inline the payload bytes alongside every projected row by default.
    fn inline_payload(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(IndexSchemaBuilder::inline_payload)?;
        Ok(slf)
    }

    /// The index schema dict.
    fn build(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &spent(self.inner.take())?.build())
    }
}

impl PyIndexSchemaBuilder {
    fn apply(
        &mut self,
        step: impl FnOnce(IndexSchemaBuilder) -> IndexSchemaBuilder,
    ) -> PyResult<()> {
        self.inner = Some(step(spent(self.inner.take())?));
        Ok(())
    }
}

/// Fluent builder for a projection dict, the shape `Projections.register` and
/// `Projections.register_graph` take. `build()` returns the dict and spends
/// the builder.
#[gen_stub_pyclass]
#[pyclass(name = "ProjectionBuilder")]
pub struct PyProjectionBuilder {
    inner: Option<ProjectionBuilder>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProjectionBuilder {
    /// Start building the projection `id`. It inlines payloads by default.
    #[new]
    fn new(id: String) -> Self {
        Self {
            inner: Some(Projection::builder(id)),
        }
    }

    /// Set the display name.
    fn name(mut slf: PyRefMut<'_, Self>, value: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.name(value))?;
        Ok(slf)
    }

    /// Set the projection version (default 1).
    fn version(mut slf: PyRefMut<'_, Self>, value: u32) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.version(value))?;
        Ok(slf)
    }

    /// Set the expected payload content type, such as `json` or `avro`.
    fn content_type(mut slf: PyRefMut<'_, Self>, value: String) -> PyResult<PyRefMut<'_, Self>> {
        let value = ContentType::from_str(&value)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        slf.apply(|builder| builder.content_type(value))?;
        Ok(slf)
    }

    /// Replace the extraction with an index schema dict.
    fn extraction<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value: IndexSchema = py_to_de(value)?;
        slf.apply(|builder| builder.extraction(value))?;
        Ok(slf)
    }

    /// Make this a graph projection extracting the nodes and edges of the
    /// entity schema dict.
    fn graph<'py>(
        mut slf: PyRefMut<'py, Self>,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let schema = py_to_de(schema)?;
        slf.apply(|builder| builder.graph(schema))?;
        Ok(slf)
    }

    /// Index the root-level payload field `name` under the same index key.
    fn field(mut slf: PyRefMut<'_, Self>, name: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.field(name))?;
        Ok(slf)
    }

    /// Index each root-level payload field in `names`.
    fn fields(mut slf: PyRefMut<'_, Self>, names: Vec<String>) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.fields(names))?;
        Ok(slf)
    }

    /// Index the JSON value at `pointer` (RFC-6901) under the index key `name`.
    fn field_at(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        pointer: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.field_at(name, pointer))?;
        Ok(slf)
    }

    /// Index the root-level field `name` with a storage-type hint: `text`,
    /// `int`, `float`, or `bool`.
    fn field_typed(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        field_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let field_type = parse_field_type(&field_type)?;
        slf.apply(|builder| builder.field_typed(name, field_type))?;
        Ok(slf)
    }

    /// Index the JSON value at `pointer` under `name` with a storage-type hint.
    fn field_at_typed(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        pointer: String,
        field_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let field_type = parse_field_type(&field_type)?;
        slf.apply(|builder| builder.field_at_typed(name, pointer, field_type))?;
        Ok(slf)
    }

    /// Point the vector extractor at `pointer` instead of `/embedding`.
    fn vector_field(mut slf: PyRefMut<'_, Self>, pointer: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.vector_field(pointer))?;
        Ok(slf)
    }

    /// Inline the payload bytes alongside every projected row (the default).
    fn inline_payload(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(ProjectionBuilder::inline_payload)?;
        Ok(slf)
    }

    /// Store only the indexed fields, without the payload bytes.
    fn index_only(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(ProjectionBuilder::index_only)?;
        Ok(slf)
    }

    /// The projection dict.
    fn build(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &spent(self.inner.take())?.build())
    }
}

impl PyProjectionBuilder {
    fn apply(&mut self, step: impl FnOnce(ProjectionBuilder) -> ProjectionBuilder) -> PyResult<()> {
        self.inner = Some(step(spent(self.inner.take())?));
        Ok(())
    }
}

/// Fluent builder for a projection binding dict, the shape `Bindings.apply`
/// takes. `build()` returns the dict and spends the builder.
#[gen_stub_pyclass]
#[pyclass(name = "ProjectionBindingBuilder")]
pub struct PyProjectionBindingBuilder {
    inner: Option<ProjectionBindingBuilder>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProjectionBindingBuilder {
    /// Start building a binding.
    #[new]
    fn new() -> Self {
        Self {
            inner: Some(ProjectionBinding::builder()),
        }
    }

    /// Route the (`stream`, `topic`) source.
    fn source(
        mut slf: PyRefMut<'_, Self>,
        stream: String,
        topic: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.source(stream, topic))?;
        Ok(slf)
    }

    /// Route the source of a `{"stream", "topic"}` dict.
    fn selector<'py>(
        mut slf: PyRefMut<'py, Self>,
        source: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let source: SourceSelector = py_to_de(source)?;
        slf.apply(|builder| builder.selector(source))?;
        Ok(slf)
    }

    /// Allow the source to route into `projection`.
    fn allow(mut slf: PyRefMut<'_, Self>, projection: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.allow(projection))?;
        Ok(slf)
    }

    /// Route records that name no projection into `projection`.
    fn default_projection(
        mut slf: PyRefMut<'_, Self>,
        projection: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.default_projection(projection))?;
        Ok(slf)
    }

    /// Materialize into the backend of a backend binding dict.
    fn backend<'py>(
        mut slf: PyRefMut<'py, Self>,
        backend: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let backend = py_to_de(backend)?;
        slf.apply(|builder| builder.backend(backend))?;
        Ok(slf)
    }

    /// Keep projected rows under a retention policy dict such as
    /// `{"kind": "time_to_live", "ttl_micros": 60000000}`.
    fn retention<'py>(
        mut slf: PyRefMut<'py, Self>,
        retention: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let retention = py_to_de(retention)?;
        slf.apply(|builder| builder.retention(retention))?;
        Ok(slf)
    }

    /// Materialize into the index `index` (default: the source topic).
    fn index(mut slf: PyRefMut<'_, Self>, index: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(|builder| builder.index(index))?;
        Ok(slf)
    }

    /// Announce every applied row to watchers.
    fn notify(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.apply(ProjectionBindingBuilder::notify)?;
        Ok(slf)
    }

    /// The binding dict. Raises `InvalidError` without a source.
    fn build(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.try_build(py)
    }

    /// The binding dict, or `InvalidError` without a source.
    fn try_build(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let binding = spent(self.inner.take())?
            .try_build()
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        ser_to_py(py, &binding)
    }
}

impl PyProjectionBindingBuilder {
    fn apply(
        &mut self,
        step: impl FnOnce(ProjectionBindingBuilder) -> ProjectionBindingBuilder,
    ) -> PyResult<()> {
        self.inner = Some(step(spent(self.inner.take())?));
        Ok(())
    }
}

/// An indexed field dict: the index key `name` extracted from the payload at
/// the JSON `pointer`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn index_field_new(
    py: Python<'_>,
    name: String,
    pointer: String,
) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &IndexField::new(name, pointer))
}

/// An indexed field dict with a storage-type hint: `text`, `int`, `float`, or
/// `bool`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn index_field_typed(
    py: Python<'_>,
    name: String,
    pointer: String,
    field_type: &str,
) -> PyResult<Py<PyAny>> {
    ser_to_py(
        py,
        &IndexField::typed(name, pointer, parse_field_type(field_type)?),
    )
}

/// A `{"stream", "topic"}` source selector dict.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn source_selector_new(
    py: Python<'_>,
    stream: String,
    topic: String,
) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &SourceSelector::new(stream, topic))
}

/// The wire code of the projection `kind` as a projection dict carries it.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn projection_kind_code(kind: u8) -> u8 {
    ProjectionKind::from_code(kind).code()
}

/// Whether the projection `kind` code is a row projection.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn projection_kind_is_row(kind: u8) -> bool {
    ProjectionKind::from_code(kind).is_row()
}

/// The content type the writer schema of a schema dict decodes, such as
/// `avro` or `protobuf`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn schema_def_content_type(schema_def: &Bound<'_, PyAny>) -> PyResult<String> {
    let schema_def: SchemaDef = py_to_de(schema_def)?;
    Ok(schema_def.content_type().to_string())
}

fn spent<T>(builder: Option<T>) -> PyResult<T> {
    builder.ok_or_else(|| InvalidError::new_err("this builder was already built"))
}

fn parse_field_type(value: &str) -> PyResult<FieldType> {
    FieldType::from_str(value).map_err(|_| {
        InvalidError::new_err(format!(
            "field type must be 'text', 'int', 'float', or 'bool', got '{value}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_projection_kind_codes_when_classified_then_should_match_the_rust_kind() {
        assert_eq!(projection_kind_code(1), 1);
        assert!(projection_kind_is_row(0));
        assert!(!projection_kind_is_row(1));
        assert!(!projection_kind_is_row(9));
    }

    #[test]
    fn given_an_unknown_field_type_when_parsed_then_should_raise_invalid() {
        Python::initialize();
        assert!(parse_field_type("int").is_ok());
        Python::attach(|py| {
            assert!(
                parse_field_type("decimal")
                    .unwrap_err()
                    .is_instance_of::<InvalidError>(py)
            );
        });
    }
}
