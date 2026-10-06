use crate::convert::ser_to_py;
use crate::sign::envelope_of;
use laser_sdk::agent::{ChunkAssembler, StreamEvent};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Reassembles one AGDX chunk stream (a `channel`) back into ordered body bytes,
/// the read-side pairing of the chunk writer. Pure and clock-free: feed each
/// envelope of the channel with `feed`, and drive the deadline yourself by
/// calling `abandon` when it passes. Chunks apply in `sequence` order from zero,
/// each once. A duplicate drops (and counts), a gap ends the stream with a
/// synthetic `gap` terminal, everything after a terminal drops, and a
/// `kind = error` envelope is the failure terminal.
#[gen_stub_pyclass]
#[pyclass(name = "ChunkAssembler")]
pub struct PyChunkAssembler {
    inner: ChunkAssembler,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyChunkAssembler {
    /// A fresh assembler for one channel.
    #[new]
    fn new() -> Self {
        Self {
            inner: ChunkAssembler::new(),
        }
    }

    /// Apply one envelope of this channel (the `AgentMessage.envelope` dict or
    /// its encoded bytes). Returns the events it produced (zero, one, or body
    /// plus terminal for a non-empty terminal chunk), each a dict tagged by
    /// `kind`: `{"kind": "body", "sequence", "payload"}`, `{"kind": "finished",
    /// "finish_reason", "usage", "synthetic"}`, or `{"kind": "failed", "body"}`
    /// (the encoded error body).
    fn feed<'py>(
        &mut self,
        py: Python<'py>,
        envelope: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let envelope = envelope_of(envelope)?;
        events_to_py(py, self.inner.feed(&envelope))
    }

    /// Synthesize the reader-local abandonment terminal (the deadline passed with
    /// no chunk). Returns the terminal event dict, or `None` if the stream
    /// already ended.
    fn abandon(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .abandon()
            .map(|event| ser_to_py(py, &event))
            .transpose()
    }

    /// Whether a terminal (real or synthetic) has been seen.
    #[getter]
    fn finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// Redelivered chunks dropped (consumer at-least-once).
    #[getter]
    fn duplicates_dropped(&self) -> u64 {
        self.inner.duplicates_dropped()
    }

    /// Chunks and terminals dropped after the stream ended.
    #[getter]
    fn late_dropped(&self) -> u64 {
        self.inner.late_dropped()
    }
}

// The event list as a Python list of `StreamEvent` dicts.
pub(crate) fn events_to_py(py: Python<'_>, events: Vec<StreamEvent>) -> PyResult<Bound<'_, PyAny>> {
    let list = PyList::empty(py);
    for event in events {
        list.append(ser_to_py(py, &event)?)?;
    }
    Ok(list.into_any())
}
