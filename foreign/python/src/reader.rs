use crate::async_bridge::future_into_py;
use crate::convert::json_to_py;
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::message::Message;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// A resumable reader over one topic. Build with `Topic.replay`.
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
    // Holds one poll's messages so `async for` yields them one at a time.
    buffered: Arc<Mutex<VecDeque<Message>>>,
}

impl PyCursor {
    pub(crate) fn new(
        laser: Laser,
        stream: Option<String>,
        topic: String,
        batch: Option<u32>,
        offsets: Arc<Mutex<Vec<u64>>>,
    ) -> Self {
        Self {
            laser,
            stream,
            topic,
            batch,
            offsets,
            buffered: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
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
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            let mut cursor = handle.replay().map_err(to_pyerr)?.from_offsets(saved);
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

    /// `async for message in cursor` drains what is currently appended, one
    /// message per step, and stops (raises `StopAsyncIteration`) when caught up.
    /// A fresh `async for` later resumes from the same offsets and reads what has
    /// landed since. Drive one cursor from one task.
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let buffered = self.buffered.clone();
        future_into_py(py, async move {
            if let Some(message) = buffered.lock().expect("buffer lock").pop_front() {
                return Ok(PyMessage::from(message));
            }
            let saved = offsets.lock().expect("offsets lock").clone();
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            let mut cursor = handle.replay().map_err(to_pyerr)?.from_offsets(saved);
            if let Some(batch) = batch {
                cursor = cursor.batch(batch);
            }
            let messages = cursor.poll().await.map_err(to_pyerr)?;
            *offsets.lock().expect("offsets lock") = cursor.offsets().to_vec();
            let mut queue: VecDeque<Message> = messages.into_iter().collect();
            match queue.pop_front() {
                Some(message) => {
                    buffered.lock().expect("buffer lock").extend(queue);
                    Ok(PyMessage::from(message))
                }
                None => Err(PyStopAsyncIteration::new_err(())),
            }
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
