use crate::async_bridge::future_into_py;
use crate::convert::{BodyValue, py_to_json};
use crate::errors::{TypedDecodeError, to_pyerr};
use crate::ids::PyMessageId;
use crate::publish::{PyBatchPublish, PyPublish};
use crate::stream::PyTopic;
use laser_sdk::laser::Laser;
use laser_sdk::typed::TypedTopic;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// One topic seen through one body type: `publish(body)` encodes, validates,
/// and stamps the body, and `records(reader_name)` decodes every record back
/// into `cls` with its log position. A handle without `cls` yields plain
/// Python values. Build with `Topic.json(cls)`, `Topic.cbor(cls)`, or
/// `await Topic.schema(schema_id, cls)`.
#[gen_stub_pyclass]
#[pyclass(name = "TypedTopic")]
pub struct PyTypedTopic {
    laser: Laser,
    stream: Option<String>,
    name: String,
    cls: Option<Py<PyAny>>,
    form: TypedForm,
}

// How the typed handle encodes bodies and decodes records: the JSON or CBOR
// serde form, or a registered writer schema resolved and compiled once when
// the handle is built.
#[derive(Clone)]
pub(crate) enum TypedForm {
    Json,
    Cbor,
    Schema(Box<TypedTopic<BodyValue>>),
}

impl PyTypedTopic {
    pub(crate) fn new(
        laser: Laser,
        stream: Option<String>,
        name: String,
        cls: Option<Py<PyAny>>,
        form: TypedForm,
    ) -> Self {
        Self {
            laser,
            stream,
            name,
            cls,
            form,
        }
    }

    fn typed(&self) -> TypedTopic<BodyValue> {
        let topic = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        match &self.form {
            TypedForm::Json => topic.json(),
            TypedForm::Cbor => topic.cbor(),
            TypedForm::Schema(typed) => (**typed).clone(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedTopic {
    /// Encode `body` (a `cls` instance, a dataclass, a pydantic model, or any
    /// JSON-shaped value) under the handle's form and open the publish builder
    /// with the payload and `agdx.ct`, plus `agdx.sid` for a schema-bound
    /// handle, already stamped. Chain record options, then `await .send()`.
    /// Encoding and schema validation run here, so a body the schema rejects
    /// raises `CodecError` before any byte reaches the wire.
    fn publish(&self, body: &Bound<'_, PyAny>) -> PyResult<PyPublish> {
        let request = PyPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        match self.form {
            TypedForm::Json => Ok(request.with_json_body(body_to_json(body)?)),
            _ => request.with_typed_body(self.typed(), body_to_value(body)?),
        }
    }

    /// Encode every body in `bodies` under the handle's form and open the
    /// batch publish builder with the payloads and `agdx.ct`, plus `agdx.sid`
    /// for a schema-bound handle, already stamped. Chain batch options, then
    /// `await .send()`. A body that does not encode raises `CodecError` here,
    /// so a partly valid batch never reaches the wire.
    fn publish_batch(&self, bodies: Vec<Bound<'_, PyAny>>) -> PyResult<PyBatchPublish> {
        let request =
            PyBatchPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        match self.form {
            TypedForm::Json => Ok(request.with_json_bodies(
                bodies
                    .iter()
                    .map(|body| body_to_json(body))
                    .collect::<PyResult<Vec<_>>>()?,
            )),
            _ => request.with_typed_bodies(
                self.typed(),
                bodies
                    .iter()
                    .map(|body| body_to_value(body))
                    .collect::<PyResult<Vec<_>>>()?,
            ),
        }
    }

    /// The typed reader over this topic under the consumer identity
    /// `reader_name`, decoding every record into `cls`. Own the offsets
    /// exactly like `replay()`: persist `offsets` and resume with
    /// `from_offsets=`.
    #[pyo3(signature = (reader_name, *, batch=None, from_offsets=None))]
    fn records(
        &self,
        py: Python<'_>,
        reader_name: String,
        batch: Option<u32>,
        from_offsets: Option<Vec<u64>>,
    ) -> PyTypedRecords {
        PyTypedRecords::new(
            self.typed(),
            self.name.clone(),
            self.cls.as_ref().map(|cls| cls.clone_ref(py)),
            reader_name,
            batch,
            from_offsets.unwrap_or_default(),
        )
    }

    /// The untyped handle underneath, for the verbs the typed form does not
    /// wrap: raw sends, batches, replay, and the producer and consumer builders.
    fn topic(&self) -> PyTopic {
        PyTopic::new(self.laser.clone(), self.stream.clone(), self.name.clone())
    }

    fn __repr__(&self) -> String {
        format!("TypedTopic(name={})", self.name)
    }
}

/// The typed reader over one topic: each `await .next()` yields the next
/// record decoded into the topic's `cls`, `None` when caught up, and
/// `await .poll()` returns one bounded poll at once. A record that does not
/// decode raises `TypedDecodeError` naming its log position and the reader
/// moves past it. Build with `TypedTopic.records(reader_name)`.
#[gen_stub_pyclass]
#[pyclass(name = "TypedRecords")]
pub struct PyTypedRecords {
    topic: String,
    reader: Reader,
}

// The reader state the 'static poll futures share: the advanced offsets write
// back so resumption stays correct, the buffer holds one poll's decoded
// records.
struct Reader {
    typed: TypedTopic<BodyValue>,
    cls: Option<Py<PyAny>>,
    reader_name: String,
    batch: Option<u32>,
    offsets: Arc<Mutex<Vec<u64>>>,
    buffered: Arc<Mutex<VecDeque<Result<Entry, PyErr>>>>,
}

struct Entry {
    value: Py<PyAny>,
    position: PyMessageId,
    headers: BTreeMap<String, String>,
}

impl PyTypedRecords {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        typed: TypedTopic<BodyValue>,
        topic: String,
        cls: Option<Py<PyAny>>,
        reader_name: String,
        batch: Option<u32>,
        from_offsets: Vec<u64>,
    ) -> Self {
        Self {
            topic,
            reader: Reader {
                typed,
                cls,
                reader_name,
                batch,
                offsets: Arc::new(Mutex::new(from_offsets)),
                buffered: Arc::new(Mutex::new(VecDeque::new())),
            },
        }
    }
}

impl Reader {
    fn share(&self, py: Python<'_>) -> Self {
        Self {
            typed: self.typed.clone(),
            cls: self.cls.as_ref().map(|cls| cls.clone_ref(py)),
            reader_name: self.reader_name.clone(),
            batch: self.batch,
            offsets: self.offsets.clone(),
            buffered: self.buffered.clone(),
        }
    }

    // Poll the log once into the buffer when it holds nothing, decoding each
    // record as it lands. A failed poll raises the transport error itself.
    async fn fill(&self) -> PyResult<()> {
        if !self.buffered.lock().expect("buffer lock").is_empty() {
            return Ok(());
        }
        let saved = self.offsets.lock().expect("offsets lock").clone();
        let mut records = self
            .typed
            .records(&self.reader_name)
            .map_err(to_pyerr)?
            .from_offsets(saved);
        if let Some(batch) = self.batch {
            records = records.batch(batch);
        }
        let polled = records.poll().await.map_err(to_pyerr)?;
        *self.offsets.lock().expect("offsets lock") = records.offsets().to_vec();
        Python::attach(|py| {
            let mut buffer = self.buffered.lock().expect("buffer lock");
            for item in polled {
                buffer.push_back(match item {
                    Ok(record) => decode_entry(py, self.cls.as_ref(), record),
                    Err(error) => {
                        let message = error.to_string();
                        let position = error.position.map(PyMessageId::from);
                        Err(typed_decode_error(
                            py,
                            message,
                            position,
                            to_pyerr(*error.source),
                        ))
                    }
                });
            }
        });
        Ok(())
    }

    fn pop(&self) -> Option<Result<Entry, PyErr>> {
        self.buffered.lock().expect("buffer lock").pop_front()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedRecords {
    /// The next offset to read on each partition. Persist this to resume later
    /// with `from_offsets=`.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.reader.offsets.lock().expect("offsets lock").clone()
    }

    /// The next record decoded as the topic's `cls`, or `None` when caught up
    /// (call again later to see new records). Raises `TypedDecodeError` for a
    /// record that does not decode, naming its exact log position, and the
    /// next call continues past it. Drive one reader from one task: awaiting
    /// two `next()` on the same reader concurrently would re-poll the same
    /// offset window and surface records twice.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader.share(py);
        future_into_py(py, async move {
            reader.fill().await?;
            match reader.pop() {
                None => Ok(None),
                Some(entry) => entry.map(|entry| Some(PyTypedRecord::from(entry))),
            }
        })
    }

    /// One bounded poll, decoded: every record appended since the last read
    /// (at most `batch` per partition), each a `TypedRecord` or the
    /// `TypedDecodeError` of a record that does not decode, returned rather
    /// than raised so one poison record does not hide the rest. An empty list
    /// means caught up. A failed poll raises.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader.share(py);
        future_into_py(py, async move {
            reader.fill().await?;
            let drained: Vec<_> = reader
                .buffered
                .lock()
                .expect("buffer lock")
                .drain(..)
                .collect();
            Python::attach(|py| {
                drained
                    .into_iter()
                    .map(|entry| match entry {
                        Ok(entry) => Ok(Py::new(py, PyTypedRecord::from(entry))?.into_any()),
                        Err(error) => Ok(error.into_value(py).into_any()),
                    })
                    .collect::<PyResult<Vec<Py<PyAny>>>>()
            })
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
        let reader = self.reader.share(py);
        future_into_py(py, async move {
            reader.fill().await?;
            match reader.pop() {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(entry) => entry.map(PyTypedRecord::from),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "TypedRecords(topic={}, reader_name={})",
            self.topic, self.reader.reader_name
        )
    }
}

/// One record decoded off the log: the `cls` instance, the `partition:offset`
/// position, and the string user headers.
#[gen_stub_pyclass]
#[pyclass(name = "TypedRecord", frozen)]
pub struct PyTypedRecord {
    /// The payload decoded into the topic's `cls`, or the plain decoded value
    /// when the topic has none.
    #[pyo3(get)]
    pub value: Py<PyAny>,
    /// The record's log position (partition and offset), from its own message
    /// header.
    #[pyo3(get)]
    pub position: PyMessageId,
    /// The record's user headers decoded to strings.
    #[pyo3(get)]
    pub headers: BTreeMap<String, String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedRecord {
    fn __repr__(&self) -> String {
        format!("TypedRecord(position={})", self.position.inner)
    }
}

impl From<Entry> for PyTypedRecord {
    fn from(entry: Entry) -> Self {
        Self {
            value: entry.value,
            position: entry.position,
            headers: entry.headers,
        }
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

/// Lower a typed body for the CBOR and schema forms, keeping byte strings and
/// non-text map keys that the JSON lowering cannot carry. An SDK native record
/// and a dataclass convert as for JSON. A pydantic model dumps in Python mode
/// so bytes stay bytes, and falls back to the JSON-mode dump when a field has
/// no plain Python form (a datetime, a UUID).
pub(crate) fn body_to_value(obj: &Bound<'_, PyAny>) -> PyResult<BodyValue> {
    if obj.hasattr("__laser_json__")? {
        return BodyValue::from_py(&obj.call_method0("__laser_json__")?);
    }
    if obj.hasattr("model_dump")? {
        if let Ok(value) = BodyValue::from_py(&obj.call_method0("model_dump")?) {
            return Ok(value);
        }
        let kwargs = PyDict::new(obj.py());
        kwargs.set_item("mode", "json")?;
        return BodyValue::from_py(&obj.call_method("model_dump", (), Some(&kwargs))?);
    }
    let dataclasses = obj.py().import("dataclasses")?;
    let is_dataclass = dataclasses
        .call_method1("is_dataclass", (obj,))?
        .is_truthy()?;
    if is_dataclass && !obj.is_instance_of::<pyo3::types::PyType>() {
        return BodyValue::from_py(&dataclasses.call_method1("asdict", (obj,))?);
    }
    BodyValue::from_py(obj)
}

// A polled record into a buffer entry: JSON value to Python object to a `cls`
// instance (an SDK native record's `__laser_from_json__`, pydantic
// `model_validate`, or `cls(**fields)` for a dataclass or plain class). With
// no class the Python object is the value. A body the class refuses becomes
// the position-carrying typed error, exactly like a payload that was never
// JSON.
fn decode_entry(
    py: Python<'_>,
    cls: Option<&Py<PyAny>>,
    record: laser_sdk::typed::TypedRecord<BodyValue>,
) -> Result<Entry, PyErr> {
    let position = PyMessageId::from(record.position);
    let value = record.value.to_py(py).and_then(|obj| {
        let Some(cls) = cls else {
            return Ok(obj);
        };
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
        Err(error) => Err(typed_decode_error(
            py,
            format!(
                "record at {} does not decode as the topic's cls: {error}",
                position.inner
            ),
            Some(position),
            error,
        )),
    }
}

// The typed read failure: the message names the record, `position` carries
// its log position (`None` when no record was reached), and `source` the
// underlying failure, which is also the exception's cause.
fn typed_decode_error(
    py: Python<'_>,
    message: String,
    position: Option<PyMessageId>,
    source: PyErr,
) -> PyErr {
    let error = TypedDecodeError::new_err(message);
    let value = error.value(py);
    let position = position.and_then(|position| Py::new(py, position).ok());
    let _ = value.setattr("position", position);
    let _ = value.setattr("source", source.value(py));
    error.set_cause(py, Some(source));
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(form: TypedForm) -> PyTypedTopic {
        let laser = Laser::from_client(laser_sdk::iggy::prelude::IggyClient::default());
        PyTypedTopic::new(laser, None, "readings".to_owned(), None, form)
    }

    #[test]
    fn given_typed_bodies_when_batched_then_should_queue_each_body_in_the_handle_form() {
        Python::initialize();
        Python::attach(|py| {
            let bodies = vec![PyDict::new(py).into_any(), PyDict::new(py).into_any()];
            for form in [TypedForm::Json, TypedForm::Cbor] {
                let batch = topic(form)
                    .publish_batch(bodies.clone())
                    .expect("dict bodies encode");
                let batch = Py::new(py, batch).expect("batch object");
                assert_eq!(batch.bind(py).len().expect("batch length"), 2);
            }
        });
    }
}
