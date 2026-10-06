use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::consumer_group::PyConsumerGroup;
use crate::convert::BodyValue;
use crate::errors::{InvalidError, to_pyerr};
use crate::publish::{PyBatchPublish, PyPublish};
use crate::reader::PyCursor;
use crate::transport::{
    ConsumerConfig, ProducerSettings, PyConsumer, PyProducer, configure_consumer,
    positive_duration_ms, routing,
};
use crate::typed::{PyTypedTopic, TypedForm, body_to_json};
use iggy::prelude::IggyExpiry;
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The stream accessor: a real Apache Iggy stream grouping topics, first
    /// class and dynamic. Free and synchronous, IO happens at the verbs.
    fn stream(&self, name: String) -> PyStream {
        PyStream {
            laser: self.inner.clone(),
            name,
        }
    }

    /// The topic accessor against the default stream, the one-word shortcut
    /// (`laser.stream(name).topic(name)` addresses any topic on any stream).
    /// Raises the typed no-stream error at the verbs when the client was
    /// connected without a default stream. `json(cls)`, `cbor(cls)`, and
    /// `schema(schema_id, cls)` open the typed handle over it.
    fn topic(&self, name: String) -> PyTopic {
        PyTopic::new(self.inner.clone(), None, name)
    }
}

/// One Apache Iggy stream: the layer grouping topics. Build with `Laser.stream`.
#[gen_stub_pyclass]
#[pyclass(name = "Stream")]
pub struct PyStream {
    laser: Laser,
    name: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyStream {
    /// This stream's name.
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    /// A topic on this stream.
    fn topic(&self, name: String) -> PyTopic {
        PyTopic::new(self.laser.clone(), Some(self.name.clone()), name)
    }

    /// Idempotently create this stream.
    fn ensure<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser.stream(&name).ensure().await.map_err(to_pyerr)
        })
    }

    /// Delete this stream with every topic and message in it. Returns `False` when the stream did not exist.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser.stream(&name).delete().await.map_err(to_pyerr)
        })
    }

    fn __repr__(&self) -> String {
        format!("Stream(name={})", self.name)
    }
}

/// One topic: where records live. Publish to it, replay it, ensure it. Build
/// with `Laser.topic` (default stream) or `Stream.topic`.
#[gen_stub_pyclass]
#[pyclass(name = "Topic", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTopic {
    laser: Laser,
    stream: Option<String>,
    name: String,
}

impl PyTopic {
    pub(crate) fn new(laser: Laser, stream: Option<String>, name: String) -> Self {
        Self {
            laser,
            stream,
            name,
        }
    }

    fn typed(&self, cls: Option<Py<PyAny>>, form: TypedForm) -> PyTypedTopic {
        PyTypedTopic::new(
            self.laser.clone(),
            self.stream.clone(),
            self.name.clone(),
            cls,
            form,
        )
    }

    fn handle(&self) -> laser_sdk::stream::Topic {
        match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTopic {
    /// This topic's name.
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Start publishing a single record. Chain `.index(..)`, `.json(..)` /
    /// `.msgpack(..)` / `.payload(..)`, then `await .send()`. With `body`
    /// given, a dataclass instance, a pydantic model, or any JSON-shaped value
    /// is encoded as JSON with `agdx.ct` stamped, and the builder is ready to
    /// `.send()`.
    #[pyo3(signature = (body=None))]
    fn publish(&self, body: Option<&Bound<'_, PyAny>>) -> PyResult<PyPublish> {
        let request = PyPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        match body {
            Some(body) => Ok(request.with_json_body(body_to_json(body)?)),
            None => Ok(request),
        }
    }

    /// The typed handle in the JSON serde form: `publish(body)` encodes the
    /// body (a dataclass, a pydantic model, or any JSON-shaped value) with
    /// `agdx.ct=json`, and `records(reader_name)` decodes every record back
    /// into `cls`, or into a plain Python object when no class is set.
    #[pyo3(signature = (cls=None))]
    fn json(&self, cls: Option<Py<PyAny>>) -> PyTypedTopic {
        self.typed(cls, TypedForm::Json)
    }

    /// The typed handle in the CBOR serde form: the same contract as `json`
    /// with a binary self-describing body and `agdx.ct=cbor`.
    #[pyo3(signature = (cls=None))]
    fn cbor(&self, cls: Option<Py<PyAny>>) -> PyTypedTopic {
        self.typed(cls, TypedForm::Cbor)
    }

    /// The typed handle bound to registered writer schema `schema_id`: resolves
    /// the definition from the managed registry and compiles it once, so every
    /// `publish(body)` validates and encodes against it in the client and
    /// stamps `agdx.ct` and `agdx.sid`. A body the schema rejects raises
    /// `CodecError` before any byte leaves the process. The registry is
    /// managed, so this needs Laser Stack or LaserData Cloud. Raises
    /// `InvalidError` when the schema is not registered.
    #[pyo3(signature = (schema_id, cls=None))]
    fn schema<'py>(
        &self,
        py: Python<'py>,
        schema_id: u32,
        cls: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let topic = PyTopic::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        let handle = self.handle();
        future_into_py(py, async move {
            let typed = handle
                .schema::<BodyValue>(schema_id)
                .await
                .map_err(to_pyerr)?;
            Ok(topic.typed(cls, TypedForm::Schema(Box::new(typed))))
        })
    }

    /// One raw message with explicit user `headers` (a dict of header name to
    /// value), the zero-overhead path. `partition_key` keeps per-key order,
    /// `None` lets the producer balance across partitions. Returns the commit
    /// confirmations.
    #[pyo3(signature = (payload, *, headers=None, partition_key=None))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        partition_key: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle();
        let payload = crate::convert::payload_bytes(payload)?;
        let headers = crate::transport::headers(headers)?;
        future_into_py(py, async move {
            let response = handle
                .send(payload, headers, partition_key.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(crate::transport::PySendMessagesResponse::from(response))
        })
    }

    /// Many raw payloads in one Iggy send, all sharing `partition_key` (or
    /// balanced when `None`). An empty list is a cheap no-op.
    #[pyo3(signature = (messages, *, partition_key=None))]
    fn batch<'py>(
        &self,
        py: Python<'py>,
        messages: Vec<Bound<'_, PyAny>>,
        partition_key: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle();
        let messages = messages
            .iter()
            .map(|payload| {
                let bytes = crate::convert::payload_bytes(payload)?;
                iggy::prelude::IggyMessage::builder()
                    .payload(bytes.into())
                    .build()
                    .map_err(|error| to_pyerr(error.into()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        future_into_py(py, async move {
            let response = handle
                .batch(messages, partition_key.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(crate::transport::PySendMessagesResponse::from(response))
        })
    }

    /// A size-and-time batching publisher over this topic: queued payloads
    /// flush as one append when `max_records` or `max_bytes` trips, or after
    /// `linger_ms`. `partition_key` keys every batch.
    #[pyo3(signature = (*, max_records=None, max_bytes=None, linger_ms=None, partition_key=None))]
    fn batching(
        &self,
        max_records: Option<usize>,
        max_bytes: Option<usize>,
        linger_ms: Option<u64>,
        partition_key: Option<String>,
    ) -> PyResult<crate::batching::PyBatchingProducer> {
        let mut builder = self.handle().batching().map_err(to_pyerr)?;
        if let Some(n) = max_records {
            builder = builder.max_records(n);
        }
        if let Some(n) = max_bytes {
            builder = builder.max_bytes(n);
        }
        if let Some(ms) = linger_ms {
            builder = builder.linger(Duration::from_millis(ms));
        }
        if let Some(key) = partition_key {
            builder = builder.partition_key(key);
        }
        Ok(crate::batching::PyBatchingProducer::new(builder))
    }

    /// Start a batch publish that flushes 1..N records in a single Iggy send.
    fn publish_batch(&self) -> PyBatchPublish {
        PyBatchPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone())
    }

    /// Build a Laser direct producer. This is the full streaming hot path
    /// below the typed publish API: tune batching/linger/retries,
    /// topology creation, and default key or partition, then `await send(...)`.
    /// `background=True` switches to Apache Iggy's buffered, sharded send mode
    /// with its defaults, and `background=BackgroundConfig(...)` configures it
    /// (shards, sharding, flush limits, byte budget, in-flight writes,
    /// backpressure, error callback). `batch_length` and `linger_ms` apply to
    /// direct mode only. A send then returns once the record is queued, so call
    /// `await producer.shutdown()` before exit.
    #[pyo3(signature = (*, batch_length=1000, linger_ms=0, retries=None, retry_interval_ms=None, key=None, partition=None, create_stream=true, create_topic=true, partitions=1, message_expiry="server_default", max_topic_size=0, background=None))]
    #[allow(clippy::too_many_arguments)]
    fn producer(
        &self,
        batch_length: u32,
        linger_ms: u64,
        retries: Option<u32>,
        retry_interval_ms: Option<u64>,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
        create_stream: bool,
        create_topic: bool,
        partitions: u32,
        message_expiry: &str,
        max_topic_size: u64,
        background: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyProducer> {
        if batch_length == 0 {
            return Err(InvalidError::new_err(
                "producer batch_length must be greater than zero",
            ));
        }
        if create_topic && partitions == 0 {
            return Err(InvalidError::new_err(
                "topic partitions must be greater than zero",
            ));
        }
        if let Some(interval) = retry_interval_ms {
            positive_duration_ms(interval, "retry_interval_ms")?;
        }
        let stream = self
            .stream
            .clone()
            .or_else(|| self.laser.default_stream().map(str::to_owned))
            .ok_or_else(|| to_pyerr(LaserError::NoStream))?;
        let settings = ProducerSettings {
            batch_length,
            linger: Duration::from_millis(linger_ms),
            retries,
            retry_interval: retry_interval_ms.map(Duration::from_millis),
            routing: routing(key, partition)?,
            create_stream,
            create_topic,
            partitions,
            expiry: message_expiry
                .parse::<IggyExpiry>()
                .map_err(InvalidError::new_err)?,
            max_topic_size,
            background: match background.filter(|value| !value.is_none()) {
                None => None,
                Some(value) => match value.cast::<crate::transport::PyBackgroundConfig>() {
                    Ok(config) => Some(crate::transport::BackgroundSettings::from(config.get())),
                    Err(_) => value
                        .extract::<bool>()
                        .map_err(|_| {
                            InvalidError::new_err("background must be a bool or a BackgroundConfig")
                        })?
                        .then(crate::transport::BackgroundSettings::default),
                },
            },
        };
        Ok(PyProducer::new(
            self.laser.stream(stream.clone()).topic(&*self.name),
            stream,
            settings,
        ))
    }

    /// Build a Laser reader for one partition. It is an async iterator
    /// and supports the same polling, batching, replay, retry, and commit modes
    /// as the Rust builder. Use `auto_commit="disabled"` plus
    /// `commit(message)` for commit-after-handle delivery.
    #[pyo3(signature = (name, *, partition=0, batch_length=1000, poll_interval_ms=None, polling="next", offset=None, timestamp_micros=None, auto_commit="polling", commit_interval_ms=0, commit_every=None, polling_retry_interval_ms=1000, init_retries=None, init_retry_interval_ms=1000, allow_replay=false))]
    #[allow(clippy::too_many_arguments)]
    fn consumer(
        &self,
        name: &str,
        partition: u32,
        batch_length: u32,
        poll_interval_ms: Option<u64>,
        polling: &str,
        offset: Option<u64>,
        timestamp_micros: Option<u64>,
        auto_commit: &str,
        commit_interval_ms: u64,
        commit_every: Option<u32>,
        polling_retry_interval_ms: u64,
        init_retries: Option<u32>,
        init_retry_interval_ms: u64,
        allow_replay: bool,
    ) -> PyResult<PyConsumer> {
        let handle = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        configure_consumer(
            handle.consumer(name, partition),
            name.to_owned(),
            ConsumerConfig {
                batch_length,
                poll_interval_ms,
                polling,
                offset,
                timestamp_micros,
                auto_commit,
                commit_interval_ms,
                commit_every,
                auto_join_group: false,
                create_group: false,
                polling_retry_interval_ms,
                init_retries,
                init_retry_interval_ms,
                allow_replay,
            },
            false,
        )
    }

    /// The consumer group `group` of this topic: the handle that owns the
    /// group's filter policy and builds its consumers and readers. Free and
    /// synchronous, IO happens at the verbs.
    fn consumer_group(&self, group: String) -> PyConsumerGroup {
        let handle = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        PyConsumerGroup::new(handle.consumer_group(group), self.clone())
    }

    /// `consumer_group` by the group's native numeric id. The id names a
    /// group inside this topic incarnation only.
    fn consumer_group_id(&self, id: u64) -> PyConsumerGroup {
        let handle = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        PyConsumerGroup::new(handle.consumer_group_id(id), self.clone())
    }

    /// A resumable, offset-addressable reader over this topic. Each `poll()`
    /// drains everything appended since the last poll across every partition,
    /// ordered by timestamp. Persist `offsets` and pass them back as
    /// `from_offsets=` to resume.
    #[pyo3(signature = (*, batch=None, from_offsets=None))]
    fn replay(&self, batch: Option<u32>, from_offsets: Option<Vec<u64>>) -> PyCursor {
        PyCursor::new(
            self.laser.clone(),
            self.stream.clone(),
            self.name.clone(),
            batch,
            Arc::new(Mutex::new(from_offsets.unwrap_or_default())),
        )
    }

    /// Idempotently create this topic with `partitions`, creating the stream
    /// first when needed.
    fn ensure<'py>(&self, py: Python<'py>, partitions: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*name),
                None => laser.topic(&*name),
            };
            handle.ensure(partitions).await.map_err(to_pyerr)
        })
    }

    /// Idempotently create the consumer group `name` on this topic without
    /// joining it.
    fn ensure_consumer_group<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.name.clone();
        future_into_py(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            handle.ensure_consumer_group(&name).await.map_err(to_pyerr)
        })
    }

    fn __repr__(&self) -> String {
        match &self.stream {
            Some(stream) => format!("Topic(stream={stream}, name={})", self.name),
            None => format!("Topic(name={})", self.name),
        }
    }
}
