use crate::client::PyLaser;
use crate::convert::json_to_py;
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::message::Message;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A resumable, offset-addressable reader over `topic`. Each `poll()` drains
    /// everything appended since the last poll across every partition, ordered by
    /// timestamp. You own the offsets: read them with `offsets`, persist them, and
    /// pass them back as `from_offsets=` to resume. Reads the default stream unless
    /// `stream=` names another.
    #[pyo3(signature = (topic, *, stream=None, batch=None, from_offsets=None))]
    fn reader(
        &self,
        topic: String,
        stream: Option<String>,
        batch: Option<u32>,
        from_offsets: Option<Vec<u64>>,
    ) -> PyCursor {
        PyCursor {
            laser: self.inner.clone(),
            stream,
            topic,
            batch,
            offsets: Arc::new(Mutex::new(from_offsets.unwrap_or_default())),
        }
    }
}

/// A resumable reader over one topic.
#[gen_stub_pyclass]
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    batch: Option<u32>,
    // Shared so the 'static poll future can read the saved offsets and write back
    // the advanced ones, keeping resumption correct across polls.
    offsets: Arc<Mutex<Vec<u64>>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCursor {
    /// The next offset to read on each partition. Persist this to resume later.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.offsets.lock().expect("offsets lock").clone()
    }

    /// Drain everything appended since the last poll, advancing the cursor. Returns
    /// the new messages ordered by timestamp, or an empty list when caught up.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let saved = offsets.lock().expect("offsets lock").clone();
        future_into_py(py, async move {
            let mut cursor = match &stream {
                Some(stream) => laser.reader_on(stream, &topic).map_err(to_pyerr)?,
                None => laser.reader(&topic).map_err(to_pyerr)?,
            }
            .from_offsets(saved);
            if let Some(batch) = batch {
                cursor = cursor.batch(batch);
            }
            let messages = cursor.poll().await.map_err(to_pyerr)?;
            *offsets.lock().expect("offsets lock") = cursor.offsets().to_vec();
            Ok(messages
                .into_iter()
                .map(PyMessage::from)
                .collect::<Vec<_>>())
        })
    }
}

/// One log record: raw payload, log position, and string user headers.
#[gen_stub_pyclass]
#[pyclass(name = "Message", frozen)]
pub struct PyMessage {
    #[pyo3(get)]
    pub payload: Vec<u8>,
    #[pyo3(get)]
    pub message_id: String,
    #[pyo3(get)]
    pub headers: BTreeMap<String, String>,
}

impl From<Message> for PyMessage {
    fn from(message: Message) -> Self {
        Self {
            payload: message.payload,
            message_id: message.id.to_string(),
            headers: message.headers,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMessage {
    /// Decode the payload as JSON into a Python value.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.payload)
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
        json_to_py(py, &value)
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(id={}, headers={}, bytes={})",
            self.message_id,
            self.headers.len(),
            self.payload.len()
        )
    }
}
