use crate::async_bridge::{Awaitable, awaitable};
use crate::convert::json_to_py;
use crate::errors::to_pyerr;
use crate::transport::{PyHeader, header_dict, header_kinds, typed_headers};
use laser_sdk::laser::Laser;
use laser_sdk::message::Message;
use laser_sdk::types::MessageId;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

/// A resumable reader over one topic. Build with `Topic.replay`.
#[gen_stub_pyclass]
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    batch: Option<u32>,
    // Shared so the 'static poll future can read the saved offsets and write back
    // the advanced ones, keeping resumption correct across polls. They move
    // past each message `async for` hands out.
    offsets: Arc<Mutex<Vec<u64>>>,
    // Holds one poll's messages so `async for` yields them one at a time.
    buffered: Arc<Mutex<Buffered>>,
}

// The messages of one poll not yet handed out, and the offsets after the poll.
#[derive(Default)]
struct Buffered {
    messages: VecDeque<Message>,
    watermark: Vec<u64>,
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
            buffered: Arc::new(Mutex::new(Buffered::default())),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCursor {
    /// The next offset to read on each partition, past every message handed
    /// out so far. Persist this after handling a message to resume right
    /// after it.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.offsets.lock().expect("offsets lock").clone()
    }

    /// Drain everything appended since the last poll, advancing the cursor. Returns
    /// the new messages ordered by timestamp, or an empty list when caught up.
    /// Messages an `async for` left unread come first.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyMessage>>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let saved = offsets.lock().expect("offsets lock").clone();
        let unread = {
            let mut buffered = self.buffered.lock().unwrap_or_else(PoisonError::into_inner);
            let messages: Vec<PyMessage> =
                buffered.messages.drain(..).map(PyMessage::from).collect();
            if !messages.is_empty() {
                *offsets.lock().unwrap_or_else(PoisonError::into_inner) =
                    std::mem::take(&mut buffered.watermark);
            }
            messages
        };
        awaitable(py, async move {
            if !unread.is_empty() {
                return Ok(unread);
            }
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

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyMessage>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let buffered = self.buffered.clone();
        awaitable(py, async move {
            if let Some(message) = hand_out(&buffered, &offsets) {
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
            if messages.is_empty() {
                *offsets.lock().unwrap_or_else(PoisonError::into_inner) = cursor.offsets().to_vec();
                return Err(PyStopAsyncIteration::new_err(()));
            }
            *buffered.lock().unwrap_or_else(PoisonError::into_inner) = Buffered {
                messages: messages.into(),
                watermark: cursor.offsets().to_vec(),
            };
            hand_out(&buffered, &offsets)
                .map(PyMessage::from)
                .ok_or_else(|| PyStopAsyncIteration::new_err(()))
        })
    }
}

/// One log record: raw payload, log position, and typed user headers.
#[gen_stub_pyclass]
#[pyclass(name = "Message", frozen)]
pub struct PyMessage {
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition and offset).
    #[pyo3(get)]
    pub id: crate::ids::PyMessageId,
    headers: BTreeMap<String, PyHeader>,
}

impl From<Message> for PyMessage {
    fn from(message: Message) -> Self {
        Self {
            payload: message.payload,
            id: message.id.into(),
            headers: typed_headers(message.headers),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMessage {
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports = ("builtins",)))]
    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    /// Typed Iggy user headers as Python `bytes`, `str`, `bool`, `int`, or
    /// `float` values, like `ConsumerMessage.headers`.
    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        header_dict(py, &self.headers)
    }

    /// Exact Iggy value kind for each user header (`uint16`, `string`, etc.).
    #[getter]
    fn header_kinds(&self) -> BTreeMap<String, String> {
        header_kinds(&self.headers)
    }

    /// Decode the payload as JSON into a Python value.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.payload)
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
        json_to_py(py, &value)
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(id={}, headers={}, bytes={})",
            self.id.inner,
            self.headers.len(),
            self.payload.len()
        )
    }
}

// Take the next buffered message and move the offsets past it, or to the
// poll's watermark once the buffer is empty.
fn hand_out(buffered: &Mutex<Buffered>, offsets: &Mutex<Vec<u64>>) -> Option<Message> {
    let mut buffered = buffered.lock().unwrap_or_else(PoisonError::into_inner);
    let message = buffered.messages.pop_front()?;
    let mut offsets = offsets.lock().unwrap_or_else(PoisonError::into_inner);
    if buffered.messages.is_empty() {
        *offsets = std::mem::take(&mut buffered.watermark);
    } else {
        advance_past(&mut offsets, message.id);
    }
    Some(message)
}

/// Moves `offsets` past `position`, growing it for a partition it does not name.
pub(crate) fn advance_past(offsets: &mut Vec<u64>, position: MessageId) {
    let partition = position.partition_id as usize;
    if offsets.len() <= partition {
        offsets.resize(partition + 1, 0);
    }
    offsets[partition] = offsets[partition].max(position.offset + 1);
}
