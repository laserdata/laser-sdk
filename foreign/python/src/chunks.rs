use crate::agent::PyAgentMessage;
use laser_sdk::agent::{ChunkAssembler, StreamEvent};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Reassembles one AGDX chunk stream (a `channel`) back into ordered body bytes,
/// the read-side pairing of the chunk writer. Pure and clock-free: feed each
/// message of the channel with `feed`, and drive the deadline yourself by calling
/// `abandon` when it passes. Chunks apply in `sequence` order from zero, each
/// once. A duplicate drops (and counts), a gap ends the stream with a synthetic
/// `gap` terminal, everything after a terminal drops, and a `kind = error`
/// message is the failure terminal.
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

    /// Apply one message of this channel. Returns the events it produced, each a
    /// dict: `{"kind": "body", "sequence": int, "bytes": bytes}`, `{"kind":
    /// "finished", "finish_reason": str|None, "synthetic": bool, "input_tokens":
    /// int|None, "output_tokens": int|None}`, or `{"kind": "failed", "bytes":
    /// bytes}` (the encoded error body). A plain (non-AGDX) message produces no
    /// events.
    fn feed<'py>(
        &mut self,
        py: Python<'py>,
        message: &PyAgentMessage,
    ) -> PyResult<Bound<'py, PyAny>> {
        let events = match message.agdx_envelope() {
            Some(envelope) => self.inner.feed(envelope),
            None => Vec::new(),
        };
        events_to_py(py, events)
    }

    /// Synthesize the reader-local abandonment terminal (the deadline passed with
    /// no chunk). Returns the terminal event dict, or `None` if the stream already
    /// ended.
    fn abandon<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.inner.abandon() {
            Some(event) => Ok(Some(event_to_py(py, event)?)),
            None => Ok(None),
        }
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

// The event list as a Python list of dicts.
fn events_to_py(py: Python<'_>, events: Vec<StreamEvent>) -> PyResult<Bound<'_, PyAny>> {
    let list = PyList::empty(py);
    for event in events {
        list.append(event_to_py(py, event)?)?;
    }
    Ok(list.into_any())
}

// One `StreamEvent` as a Python dict.
fn event_to_py(py: Python<'_>, event: StreamEvent) -> PyResult<Bound<'_, PyAny>> {
    let dict = PyDict::new(py);
    match event {
        StreamEvent::Body { sequence, payload } => {
            dict.set_item("kind", "body")?;
            dict.set_item("sequence", sequence)?;
            dict.set_item("payload", payload)?;
        }
        StreamEvent::Finished {
            finish_reason,
            usage,
            synthetic,
        } => {
            dict.set_item("kind", "finished")?;
            dict.set_item("finish_reason", finish_reason)?;
            dict.set_item("synthetic", synthetic)?;
            dict.set_item("input_tokens", usage.as_ref().map(|u| u.input_tokens))?;
            dict.set_item("output_tokens", usage.as_ref().map(|u| u.output_tokens))?;
        }
        StreamEvent::Failed { body } => {
            dict.set_item("kind", "failed")?;
            dict.set_item("bytes", body)?;
        }
    }
    Ok(dict.into_any())
}
