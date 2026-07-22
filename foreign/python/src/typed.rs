use crate::convert::{json_to_py, py_to_json};
use crate::errors::{TypedDecodeError, to_pyerr};
use laser_sdk::laser::Laser;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The typed reader over one topic: each `await .next()` yields the next
/// record decoded into the topic's `cls`, `None` when caught up. A record that
/// does not decode raises `TypedDecodeError` naming its log position and the
/// reader moves past it. Build with `Topic.records(reader_name)` on a topic
/// opened with `cls=`.
#[gen_stub_pyclass]
#[pyclass(name = "TypedRecords")]
pub struct PyTypedRecords {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    cls: Py<PyAny>,
    reader_name: String,
    batch: Option<u32>,
    // Shared with the 'static poll futures: the advanced offsets write back so
    // resumption stays correct, the buffer holds one poll's decoded records.
    offsets: Arc<Mutex<Vec<u64>>>,
    buffered: Arc<Mutex<VecDeque<Result<Entry, PyErr>>>>,
}

struct Entry {
    value: Py<PyAny>,
    position: String,
    headers: BTreeMap<String, String>,
}

impl PyTypedRecords {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        laser: Laser,
        stream: Option<String>,
        topic: String,
        cls: Py<PyAny>,
        reader_name: String,
        batch: Option<u32>,
        from_offsets: Vec<u64>,
    ) -> Self {
        Self {
            laser,
            stream,
            topic,
            cls,
            reader_name,
            batch,
            offsets: Arc::new(Mutex::new(from_offsets)),
            buffered: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedRecords {
    /// The next offset to read on each partition. Persist this to resume later
    /// with `from_offsets=`.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.offsets.lock().expect("offsets lock").clone()
    }

    /// The next record decoded as the topic's `cls`, or `None` when caught up
    /// (call again later to see new records). Raises `TypedDecodeError` for a
    /// record that does not decode, naming its exact log position, and the
    /// next call continues past it. Drive one reader from one task: awaiting
    /// two `next()` on the same reader concurrently would re-poll the same
    /// offset window and surface records twice.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let cls = self.cls.clone_ref(py);
        let reader_name = self.reader_name.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let buffered = self.buffered.clone();
        future_into_py(py, async move {
            let empty = buffered.lock().expect("buffer lock").is_empty();
            if empty {
                let saved = offsets.lock().expect("offsets lock").clone();
                let handle = match &stream {
                    Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                    None => laser.topic(&*topic),
                };
                let typed = handle.json::<serde_json::Value>();
                let mut records = typed
                    .records(&reader_name)
                    .map_err(to_pyerr)?
                    .from_offsets(saved);
                if let Some(batch) = batch {
                    records = records.batch(batch);
                }
                let polled = records.poll().await.map_err(to_pyerr)?;
                *offsets.lock().expect("offsets lock") = records.offsets().to_vec();
                Python::attach(|py| {
                    let mut buffer = buffered.lock().expect("buffer lock");
                    for item in polled {
                        buffer.push_back(match item {
                            Ok(record) => decode_entry(py, &cls, record),
                            Err(error) => Err(TypedDecodeError::new_err(error.to_string())),
                        });
                    }
                });
            }
            let entry = buffered.lock().expect("buffer lock").pop_front();
            match entry {
                None => Ok(None),
                Some(Ok(entry)) => Ok(Some(PyTypedRecord {
                    value: entry.value,
                    position: entry.position,
                    headers: entry.headers,
                })),
                Some(Err(error)) => Err(error),
            }
        })
    }

    /// `async for record in reader` yields decoded records one at a time and stops
    /// (raises `StopAsyncIteration`) when caught up. A record that does not decode
    /// raises `TypedDecodeError` and ends the loop. Use `next()` in a loop instead
    /// to skip a poison record and continue. Drive one reader from one task.
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let cls = self.cls.clone_ref(py);
        let reader_name = self.reader_name.clone();
        let batch = self.batch;
        let offsets = self.offsets.clone();
        let buffered = self.buffered.clone();
        future_into_py(py, async move {
            let empty = buffered.lock().expect("buffer lock").is_empty();
            if empty {
                let saved = offsets.lock().expect("offsets lock").clone();
                let handle = match &stream {
                    Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                    None => laser.topic(&*topic),
                };
                let typed = handle.json::<serde_json::Value>();
                let mut records = typed
                    .records(&reader_name)
                    .map_err(to_pyerr)?
                    .from_offsets(saved);
                if let Some(batch) = batch {
                    records = records.batch(batch);
                }
                let polled = records.poll().await.map_err(to_pyerr)?;
                *offsets.lock().expect("offsets lock") = records.offsets().to_vec();
                Python::attach(|py| {
                    let mut buffer = buffered.lock().expect("buffer lock");
                    for item in polled {
                        buffer.push_back(match item {
                            Ok(record) => decode_entry(py, &cls, record),
                            Err(error) => Err(TypedDecodeError::new_err(error.to_string())),
                        });
                    }
                });
            }
            let entry = buffered.lock().expect("buffer lock").pop_front();
            match entry {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(Ok(entry)) => Ok(PyTypedRecord {
                    value: entry.value,
                    position: entry.position,
                    headers: entry.headers,
                }),
                Some(Err(error)) => Err(error),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "TypedRecords(topic={}, reader_name={})",
            self.topic, self.reader_name
        )
    }
}

/// One record decoded off the log: the `cls` instance, the `partition:offset`
/// position, and the string user headers.
#[gen_stub_pyclass]
#[pyclass(name = "TypedRecord", frozen)]
pub struct PyTypedRecord {
    /// The payload decoded into the topic's `cls`.
    #[pyo3(get)]
    pub value: Py<PyAny>,
    /// The record's log position, from its own message header.
    #[pyo3(get)]
    pub position: String,
    /// The record's user headers decoded to strings.
    #[pyo3(get)]
    pub headers: BTreeMap<String, String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedRecord {
    fn __repr__(&self) -> String {
        format!("TypedRecord(position={})", self.position)
    }
}

/// Encode a typed body for publishing: an SDK native record
/// (`__laser_json__`), a pydantic model (`model_dump`), a dataclass instance
/// (`dataclasses.asdict`), or any JSON-shaped value as-is.
pub(crate) fn body_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.hasattr("__laser_json__")? {
        return py_to_json(&obj.call_method0("__laser_json__")?);
    }
    if obj.hasattr("model_dump")? {
        let kwargs = PyDict::new(obj.py());
        kwargs.set_item("mode", "json")?;
        return py_to_json(&obj.call_method("model_dump", (), Some(&kwargs))?);
    }
    let dataclasses = obj.py().import("dataclasses")?;
    let is_dataclass = dataclasses
        .call_method1("is_dataclass", (obj,))?
        .is_truthy()?;
    if is_dataclass && !obj.is_instance_of::<pyo3::types::PyType>() {
        return py_to_json(&dataclasses.call_method1("asdict", (obj,))?);
    }
    py_to_json(obj)
}

// A polled record into a buffer entry: JSON value to Python object to a `cls`
// instance (an SDK native record's `__laser_from_json__`, pydantic
// `model_validate`, or `cls(**fields)` for a dataclass or plain class). A body
// the class refuses becomes the
// position-carrying typed error, exactly like a payload that was never JSON.
fn decode_entry(
    py: Python<'_>,
    cls: &Py<PyAny>,
    record: laser_sdk::typed::TypedRecord<serde_json::Value>,
) -> Result<Entry, PyErr> {
    let position = record.position.to_string();
    let value = json_to_py(py, &record.value).and_then(|obj| {
        let cls = cls.bind(py);
        if cls.hasattr("__laser_from_json__")? {
            return Ok(cls.call_method1("__laser_from_json__", (obj,))?.unbind());
        }
        if cls.hasattr("model_validate")? {
            return Ok(cls.call_method1("model_validate", (obj,))?.unbind());
        }
        let fields = obj.bind(py).cast::<PyDict>().map_err(PyErr::from)?;
        Ok(cls.call((), Some(fields))?.unbind())
    });
    match value {
        Ok(value) => Ok(Entry {
            value,
            position,
            headers: record.headers,
        }),
        Err(error) => Err(TypedDecodeError::new_err(format!(
            "record at {position} does not decode as the topic's cls: {error}"
        ))),
    }
}
