use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{payload_bytes, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A handle to one fork (copy-on-write branch of the read model) by id.
    /// Forks are a managed feature: against raw Apache Iggy every operation
    /// raises `UnsupportedError`.
    fn fork(&self, fork_id: String) -> PyForkHandle {
        PyForkHandle {
            laser: self.inner.clone(),
            fork_id,
        }
    }

    /// Every open fork for the authenticated user, as a list of dicts.
    fn forks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let forks = laser.forks().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &forks))
        })
    }
}

/// A handle to one fork by id.
#[gen_stub_pyclass]
#[pyclass(name = "ForkHandle", frozen)]
pub struct PyForkHandle {
    laser: Laser,
    #[pyo3(get)]
    fork_id: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyForkHandle {
    /// Open this fork. `severed=True` freezes a snapshot at the trunk's current
    /// offsets. The default (continuous) keeps seeing new trunk appends. Narrow a
    /// severed snapshot with `tables`. Returns the fork's metadata dict.
    #[pyo3(signature = (*, severed=false, parent=None, tables=None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        severed: bool,
        parent: Option<String>,
        tables: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let fork_id = self.fork_id.clone();
        future_into_py(py, async move {
            let handle = laser.fork(fork_id);
            let mut request = handle.create();
            if severed {
                request = request.severed();
            }
            if let Some(parent) = parent {
                request = request.parent(parent);
            }
            if let Some(tables) = tables {
                request = request.tables(tables);
            }
            let info = request.send().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &info))
        })
    }

    /// Promote this fork onto the trunk, then squash it. Returns rows applied.
    fn promote<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let fork_id = self.fork_id.clone();
        future_into_py(py, async move {
            laser.fork(fork_id).promote().await.map_err(to_pyerr)
        })
    }

    /// Squash this fork (discard speculative rows). Returns whether one existed.
    fn squash<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let fork_id = self.fork_id.clone();
        future_into_py(py, async move {
            laser.fork(fork_id).squash().await.map_err(to_pyerr)
        })
    }

    /// Start writing one speculative row at (table, partition_id, offset).
    fn put_row(&self, table: String, partition_id: u32, offset: u64) -> PyForkPut {
        PyForkPut {
            laser: self.laser.clone(),
            fork_id: self.fork_id.clone(),
            table,
            partition_id,
            offset,
            projection: None,
            fields: Vec::new(),
            metadata: Vec::new(),
            payload: None,
            embedding: None,
            tombstone: false,
        }
    }
}

/// Fluent builder for one speculative fork row.
#[gen_stub_pyclass]
#[pyclass(name = "ForkPutRequest")]
pub struct PyForkPut {
    laser: Laser,
    fork_id: String,
    table: String,
    partition_id: u32,
    offset: u64,
    projection: Option<(String, u32)>,
    fields: Vec<(String, String)>,
    metadata: Vec<(String, String)>,
    payload: Option<Vec<u8>>,
    embedding: Option<Vec<f32>>,
    tombstone: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyForkPut {
    /// Set the projection id/version this speculative row belongs to.
    fn projection<'py>(
        mut slf: PyRefMut<'py, Self>,
        id: String,
        version: u32,
    ) -> PyRefMut<'py, Self> {
        slf.projection = Some((id, version));
        slf
    }

    /// Add one indexed field (queries filter and order on these).
    fn field<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.fields.push((name, value));
        slf
    }

    /// Add one non-indexed metadata header.
    fn metadata<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.metadata.push((name, value));
        slf
    }

    /// Attach an opaque payload body (str, bytes, or bytearray).
    fn payload<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.payload = Some(payload_bytes(value)?);
        Ok(slf)
    }

    /// Attach an embedding as a list of floats.
    fn embedding<'py>(mut slf: PyRefMut<'py, Self>, embedding: Vec<f32>) -> PyRefMut<'py, Self> {
        slf.embedding = Some(embedding);
        slf
    }

    /// Mark this as a tombstone: hide the trunk row at this coordinate.
    fn tombstone(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.tombstone = true;
        slf
    }

    /// Write the speculative row.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let fork_id = self.fork_id.clone();
        let table = self.table.clone();
        let partition_id = self.partition_id;
        let offset = self.offset;
        let projection = self.projection.clone();
        let fields = self.fields.clone();
        let metadata = self.metadata.clone();
        let payload = self.payload.clone();
        let embedding = self.embedding.clone();
        let tombstone = self.tombstone;
        future_into_py(py, async move {
            let handle = laser.fork(fork_id);
            let mut request = handle.put_row(table, partition_id, offset);
            if let Some((id, version)) = projection {
                request = request.projection(id, version);
            }
            for (name, value) in fields {
                request = request.field(name, value);
            }
            for (name, value) in metadata {
                request = request.metadata(name, value);
            }
            if let Some(payload) = payload {
                request = request.payload(payload);
            }
            if let Some(embedding) = embedding {
                // The fork put takes the embedding as a JSON array literal.
                let literal = serde_json::to_string(&embedding)
                    .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
                request = request.embedding(literal);
            }
            if tombstone {
                request = request.tombstone();
            }
            request.send().await.map_err(to_pyerr)
        })
    }
}
