use crate::agent::PyProvenance;
use crate::async_bridge::future_into_py;
use crate::convert::{payload_bytes, py_to_json};
use crate::errors::{InvalidError, to_pyerr};
use crate::schema::PyCompiledSchema;
use laser_sdk::laser::Laser;
use laser_sdk::stream::Record;
use laser_sdk::wire::content::ContentType;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

fn parse_content_type(value: &str) -> PyResult<ContentType> {
    ContentType::from_str(value)
        .map_err(|_| InvalidError::new_err(format!("unknown content type '{value}'")))
}

/// How a record's body is encoded. JSON / MessagePack carry a depythonized
/// value encoded at send time, reusing the SDK's codec + content-type stamping.
/// `Raw` is already-encoded bytes plus an explicit content type and optional
/// writer-schema id (the Avro / Protobuf / own-framing path).
#[derive(Clone)]
enum Body {
    Empty,
    Bytes(Vec<u8>),
    Raw {
        payload: Vec<u8>,
        content_type: ContentType,
        schema_id: Option<u32>,
    },
    Json(serde_json::Value),
    Msgpack(serde_json::Value),
}

/// Fluent builder for a single-record publish, finished with `await .send()`.
#[gen_stub_pyclass]
#[pyclass(name = "PublishRequest")]
pub struct PyPublish {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    indexes: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    inline: bool,
    projection_ref: Option<String>,
    schema_id: Option<u32>,
    partition_key: Option<String>,
    provenance: Option<laser_sdk::provenance::Provenance>,
    body: Body,
}

impl PyPublish {
    pub(crate) fn new(laser: Laser, stream: Option<String>, topic: String) -> Self {
        Self {
            laser,
            stream,
            topic,
            indexes: Vec::new(),
            headers: Vec::new(),
            inline: false,
            projection_ref: None,
            schema_id: None,
            partition_key: None,
            provenance: None,
            body: Body::Empty,
        }
    }

    // The typed-topic entry: the body arrives already lowered to a JSON value
    // (dataclass / pydantic / plain), the builder is otherwise untouched.
    pub(crate) fn with_json_body(mut self, value: serde_json::Value) -> Self {
        self.body = Body::Json(value);
        self
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPublish {
    /// Index `value` under `agdx.idx.<key>` so queries can match / sort on it.
    /// A record with zero indexed fields is dropped by the projector.
    fn index<'py>(mut slf: PyRefMut<'py, Self>, key: String, value: String) -> PyRefMut<'py, Self> {
        slf.indexes.push((key, value));
        slf
    }

    /// Attach a non-indexed metadata header, surfaced as `Row.metadata`.
    fn header<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.headers.push((key, value));
        slf
    }

    /// Inline the payload bytes alongside the indexed row so readers can fetch
    /// the body back through the query layer. Off by default.
    fn inline_payload(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.inline = true;
        slf
    }

    /// Route the record through a projection by ref (e.g. "order.v1").
    fn projection_ref<'py>(mut slf: PyRefMut<'py, Self>, value: String) -> PyRefMut<'py, Self> {
        slf.projection_ref = Some(value);
        slf
    }

    /// Stamp a writer-schema id on `agdx.sid`.
    fn schema_id(mut slf: PyRefMut<'_, Self>, value: u32) -> PyRefMut<'_, Self> {
        slf.schema_id = Some(value);
        slf
    }

    /// Pin the record to a partition by key (preserves per-key order).
    fn partition_key<'py>(mut slf: PyRefMut<'py, Self>, key: String) -> PyRefMut<'py, Self> {
        slf.partition_key = Some(key);
        slf
    }

    /// Stamp AGDX provenance headers on this publish.
    fn provenance<'py>(
        mut slf: PyRefMut<'py, Self>,
        provenance: &PyProvenance,
    ) -> PyRefMut<'py, Self> {
        slf.provenance = Some(provenance.inner.clone());
        slf
    }

    /// Encode the body as JSON and stamp `agdx.ct=json`. Accepts any
    /// JSON-serializable Python value (dict, list, scalar).
    fn json<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Json(py_to_json(value)?);
        Ok(slf)
    }

    /// Encode the body as MessagePack and stamp `agdx.ct=msgpack`.
    fn msgpack<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Msgpack(py_to_json(value)?);
        Ok(slf)
    }

    /// Use raw payload bytes (str, bytes, or bytearray) with no content-type tag.
    fn payload<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(payload_bytes(value)?);
        Ok(slf)
    }

    /// Use already-encoded raw bytes plus a content type (e.g. "avro",
    /// "protobuf", "cbor"). Encode the body with whichever library you prefer,
    /// hand the bytes and the codec tag in one call.
    fn raw_bytes<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
        content_type: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let content_type = parse_content_type(&content_type)?;
        slf.body = Body::Raw {
            payload: payload_bytes(value)?,
            content_type,
            schema_id: None,
        };
        Ok(slf)
    }

    /// Encode `value` as a raw Avro datum under a compiled writer schema and
    /// stamp `agdx.ct=avro` + `agdx.sid`. Compile the schema once with
    /// `CompiledSchema.compile(..)` and reuse it. Encoding fails client-side
    /// (`CodecError`) when the value does not match the schema.
    fn avro<'py>(
        mut slf: PyRefMut<'py, Self>,
        schema: &PyCompiledSchema,
        schema_id: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let payload = schema
            .inner
            .encode_avro(&py_to_json(value)?)
            .map_err(to_pyerr)?;
        slf.body = Body::Raw {
            payload,
            content_type: ContentType::Avro,
            schema_id: Some(schema_id),
        };
        Ok(slf)
    }

    /// Publish the record.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let indexes = self.indexes.clone();
        let headers = self.headers.clone();
        let inline = self.inline;
        let projection_ref = self.projection_ref.clone();
        let schema_id = self.schema_id;
        let partition_key = self.partition_key.clone();
        let provenance = self.provenance.clone();
        let body = self.body.clone();
        future_into_py(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            let mut request = handle.publish();
            for (key, value) in indexes {
                request = request.index(key, value);
            }
            for (key, value) in headers {
                request = request.header(key, value);
            }
            if inline {
                request = request.inline_payload();
            }
            if let Some(projection_ref) = projection_ref {
                request = request.projection_ref(projection_ref);
            }
            if let Some(schema_id) = schema_id {
                request = request.schema_id(schema_id);
            }
            if let Some(partition_key) = &partition_key {
                request = request.partition_key(partition_key.clone());
            }
            if let Some(provenance) = &provenance {
                request = request.provenance(provenance);
            }
            request = match body {
                Body::Empty => request,
                Body::Bytes(payload) => request.payload(payload),
                Body::Raw {
                    payload,
                    content_type,
                    schema_id,
                } => {
                    let request = request.raw_bytes(payload, content_type);
                    match schema_id {
                        Some(schema_id) => request.schema_id(schema_id),
                        None => request,
                    }
                }
                Body::Json(value) => request.json(&value).map_err(to_pyerr)?,
                Body::Msgpack(value) => request.msgpack(&value).map_err(to_pyerr)?,
            };
            request.send().await.map_err(to_pyerr)
        })
    }
}

/// Fluent builder for a batch publish, finished with `await .send()` returning
/// the number of records sent.
#[gen_stub_pyclass]
#[pyclass(name = "BatchPublishRequest")]
pub struct PyBatchPublish {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    inline: bool,
    projection_ref: Option<String>,
    schema_id: Option<u32>,
    indexes: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    partition_key: Option<String>,
    bodies: Vec<Body>,
}

impl PyBatchPublish {
    pub(crate) fn new(laser: Laser, stream: Option<String>, topic: String) -> Self {
        Self {
            laser,
            stream,
            topic,
            inline: false,
            projection_ref: None,
            schema_id: None,
            indexes: Vec::new(),
            headers: Vec::new(),
            partition_key: None,
            bodies: Vec::new(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBatchPublish {
    /// Inline the payload bytes for every record in the batch.
    fn inline_payload(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.inline = true;
        slf
    }

    /// Stamp the projection ref on every record.
    fn projection_ref<'py>(mut slf: PyRefMut<'py, Self>, value: String) -> PyRefMut<'py, Self> {
        slf.projection_ref = Some(value);
        slf
    }

    /// Stamp a writer-schema id on every record.
    fn schema_id(mut slf: PyRefMut<'_, Self>, value: u32) -> PyRefMut<'_, Self> {
        slf.schema_id = Some(value);
        slf
    }

    /// Add an indexed field to every record in the batch.
    fn index<'py>(mut slf: PyRefMut<'py, Self>, key: String, value: String) -> PyRefMut<'py, Self> {
        slf.indexes.push((key, value));
        slf
    }

    /// Add a metadata header to every record in the batch.
    fn header<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.headers.push((key, value));
        slf
    }

    /// Pin every record in the batch to one partition by key.
    fn partition_key<'py>(mut slf: PyRefMut<'py, Self>, key: String) -> PyRefMut<'py, Self> {
        slf.partition_key = Some(key);
        slf
    }

    /// Append one JSON-encoded record.
    fn add_json<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.bodies.push(Body::Json(py_to_json(value)?));
        Ok(slf)
    }

    /// Append one MessagePack-encoded record.
    fn add_msgpack<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.bodies.push(Body::Msgpack(py_to_json(value)?));
        Ok(slf)
    }

    /// Append one raw-bytes record.
    fn add_payload<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.bodies.push(Body::Bytes(payload_bytes(value)?));
        Ok(slf)
    }

    /// Append one already-encoded record plus its content type (e.g. "avro",
    /// "protobuf", "cbor"). Use it for bodies you encoded with another library.
    fn add_raw_bytes<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
        content_type: String,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let content_type = parse_content_type(&content_type)?;
        slf.bodies.push(Body::Raw {
            payload: payload_bytes(value)?,
            content_type,
            schema_id: None,
        });
        Ok(slf)
    }

    /// Append one record encoded as a raw Avro datum under a compiled writer
    /// schema, stamped with `agdx.ct=avro` + `agdx.sid`. The batch counterpart
    /// of `PublishRequest.avro`. Each record carries its own schema id, so a
    /// batch may mix writer schemas.
    fn add_avro<'py>(
        mut slf: PyRefMut<'py, Self>,
        schema: &PyCompiledSchema,
        schema_id: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let payload = schema
            .inner
            .encode_avro(&py_to_json(value)?)
            .map_err(to_pyerr)?;
        slf.bodies.push(Body::Raw {
            payload,
            content_type: ContentType::Avro,
            schema_id: Some(schema_id),
        });
        Ok(slf)
    }

    /// Append every value in `items` as a JSON-encoded record.
    fn extend_json<'py>(
        mut slf: PyRefMut<'py, Self>,
        items: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        for item in items.try_iter()? {
            slf.bodies.push(Body::Json(py_to_json(&item?)?));
        }
        Ok(slf)
    }

    /// Append every value in `items` as a MessagePack-encoded record.
    fn extend_msgpack<'py>(
        mut slf: PyRefMut<'py, Self>,
        items: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        for item in items.try_iter()? {
            slf.bodies.push(Body::Msgpack(py_to_json(&item?)?));
        }
        Ok(slf)
    }

    /// Number of records queued so far.
    fn __len__(&self) -> usize {
        self.bodies.len()
    }

    /// Flush every queued record in a single Iggy send. Returns the count sent.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let inline = self.inline;
        let projection_ref = self.projection_ref.clone();
        let schema_id = self.schema_id;
        let indexes = self.indexes.clone();
        let headers = self.headers.clone();
        let partition_key = self.partition_key.clone();
        let bodies = self.bodies.clone();
        future_into_py(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            let mut request = handle.publish_batch();
            if inline {
                request = request.inline_payload();
            }
            if let Some(projection_ref) = projection_ref {
                request = request.projection_ref(projection_ref);
            }
            if let Some(schema_id) = schema_id {
                request = request.schema_id(schema_id);
            }
            for (key, value) in indexes {
                request = request.index(key, value);
            }
            for (key, value) in headers {
                request = request.header(key, value);
            }
            if let Some(partition_key) = &partition_key {
                request = request.partition_key(partition_key.clone());
            }
            for body in bodies {
                request = match body {
                    Body::Empty => request,
                    Body::Bytes(payload) => request.add_payload(payload),
                    Body::Raw {
                        payload,
                        content_type,
                        schema_id: None,
                    } => request.add_raw_bytes(payload, content_type),
                    Body::Raw {
                        payload,
                        content_type,
                        schema_id: Some(schema_id),
                    } => request.add_record(
                        payload,
                        Record::builder()
                            .content_type(content_type)
                            .schema_id(schema_id)
                            .build(),
                    ),
                    Body::Json(value) => request.add_json(&value).map_err(to_pyerr)?,
                    Body::Msgpack(value) => request.add_msgpack(&value).map_err(to_pyerr)?,
                };
            }
            request.send().await.map_err(to_pyerr)
        })
    }
}
