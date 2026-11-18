use crate::admin::{PyConsumerGroupSummary, PyPartitionInfo, PyStreamInfo, PyTopicInfo};
use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::consumer_group::PyConsumerGroup;
use crate::convert::BodyValue;
use crate::errors::{InvalidError, to_pyerr};
use crate::materialize::{PyMaterialization, PyMaterializeRequest};
use crate::publish::{PyBatchPublish, PyPublish};
use crate::reader::PyCursor;
use crate::transport::{
    ConsumerConfig, ProducerSettings, PyConsumer, PyProducer, PySendMessagesResponse,
    configure_consumer, positive_duration_ms, routing,
};
use crate::typed::{PyTypedTopic, TypedForm, body_to_json};
use iggy::prelude::IggyExpiry;
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::stream::ScratchStream;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::{Arc, Mutex};

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

    /// A uniquely named stream for one program run, entered without `await`:
    /// `async with laser.scratch_stream(prefix) as scratch:` creates `{prefix}-`
    /// and eight lowercase hex characters, `scratch.laser` uses it as the
    /// default stream, and leaving the block deletes it, also when the block
    /// raised. Use a named stream (`LASER_STREAM` or `with_default_stream`)
    /// to keep what the program writes.
    fn scratch_stream(&self, prefix: String) -> PyScratchStream {
        PyScratchStream {
            laser: self.inner.clone(),
            prefix,
            opened: Arc::default(),
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

    /// Every stream this connection's user can read.
    fn streams<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyStreamInfo>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let streams = laser.streams().await.map_err(to_pyerr)?;
            Ok(streams.into_iter().map(PyStreamInfo::from).collect())
        })
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
    fn ensure<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        awaitable(py, async move {
            laser.stream(&name).ensure().await.map_err(to_pyerr)
        })
    }

    /// Delete this stream with every topic and message in it. Returns `False` when the stream did not exist.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        awaitable(py, async move {
            laser.stream(&name).delete().await.map_err(to_pyerr)
        })
    }

    /// This stream as the server reports it, `None` when it does not exist.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Option<PyStreamInfo>>> {
        let handle = self.laser.stream(&*self.name);
        awaitable(py, async move {
            Ok(handle
                .get()
                .await
                .map_err(to_pyerr)?
                .map(PyStreamInfo::from))
        })
    }

    /// Whether this stream exists.
    fn exists<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let handle = self.laser.stream(&*self.name);
        awaitable(py, async move { handle.exists().await.map_err(to_pyerr) })
    }

    /// Every topic of this stream. A missing stream raises an error
    /// with `stream_or_topic_not_found` set.
    fn topics<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyTopicInfo>>> {
        let handle = self.laser.stream(&*self.name);
        awaitable(py, async move {
            let topics = handle.topics().await.map_err(to_pyerr)?;
            Ok(topics.into_iter().map(PyTopicInfo::from).collect())
        })
    }

    /// Rename this stream. After the rename, address the stream by its new
    /// name.
    fn rename<'py>(&self, py: Python<'py>, name: String) -> PyResult<Awaitable<'py, ()>> {
        let handle = self.laser.stream(&*self.name);
        awaitable(
            py,
            async move { handle.rename(name).await.map_err(to_pyerr) },
        )
    }

    fn __repr__(&self) -> String {
        format!("Stream(name={})", self.name)
    }
}

/// One topic: where records live. Publish to it, replay it, ensure it. Build
/// with `Laser.topic` (default stream) or `Stream.topic`.
#[gen_stub_pyclass]
#[pyclass(name = "Topic")]
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
    fn materialize(&self) -> PyMaterializeRequest {
        PyMaterializeRequest {
            inner: self.handle().materialize(),
        }
    }

    /// The `Materialization` that `materialize().apply()` returned for the
    /// setup whose projection is `projection_id`, read under this topic's
    /// stream. A setup without an explicit projection id or typed projection
    /// uses the index name, which defaults to the topic name.
    fn materialization(&self, projection_id: String) -> PyResult<PyMaterialization> {
        Ok(PyMaterialization {
            inner: self
                .handle()
                .materialization(projection_id)
                .map_err(to_pyerr)?,
        })
    }

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
    ) -> PyResult<Awaitable<'py, PyTypedTopic>> {
        let topic = PyTopic::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        let handle = self.handle();
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        partition_key: Option<String>,
    ) -> PyResult<Awaitable<'py, PySendMessagesResponse>> {
        let handle = self.handle();
        let payload = crate::convert::payload_bytes(payload)?;
        let headers = crate::transport::headers(headers)?;
        awaitable(py, async move {
            let response = handle
                .send(payload, headers, partition_key.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(crate::transport::PySendMessagesResponse::from(response))
        })
    }

    /// Many raw payloads in one Iggy send, all sharing `partition_key` (or
    /// balanced when `None`). An empty list is a cheap no-op. An item can also
    /// be an `IggyMessage` from `PublishFailedError.unconfirmed`, which is
    /// resent with its original message id and headers so the server
    /// deduplicates it.
    #[pyo3(signature = (messages, *, partition_key=None))]
    fn batch<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "typing.Sequence[typing.Union[builtins.bytes, builtins.bytearray, builtins.str, IggyMessage]]", imports = ("typing", "builtins")))]
        messages: Vec<Bound<'_, PyAny>>,
        partition_key: Option<String>,
    ) -> PyResult<Awaitable<'py, PySendMessagesResponse>> {
        let handle = self.handle();
        let messages = messages
            .iter()
            .map(|payload| {
                if let Ok(message) = payload.cast::<crate::errors::PyUnconfirmedMessage>() {
                    return message.get().to_iggy_message();
                }
                let bytes = crate::convert::payload_bytes(payload)?;
                iggy::prelude::IggyMessage::builder()
                    .payload(bytes.into())
                    .build()
                    .map_err(|error| to_pyerr(error.into()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        awaitable(py, async move {
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
        linger_ms: Option<f64>,
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
            builder = builder.linger(crate::convert::duration_ms(ms, "linger_ms")?);
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
    /// `await producer.shutdown()` before exit. A topic this producer creates
    /// takes `expire_after_ms` (milliseconds) or `never_expire=True`, else the
    /// server default, and `max_topic_bytes` or `unlimited_topic_size=True`,
    /// else the server default. Setting both of a pair raises `InvalidError`.
    /// `replication_factor` stores each partition of a topic this producer
    /// creates on that many nodes, 1 to `MAX_REPLICATION_FACTOR` and at most
    /// the cluster's node count, else the server default.
    /// `sharding(request: ShardRequest) -> int` picks the background shard of
    /// each send in place of the `BackgroundConfig` sharding, and needs
    /// background mode. It returns an index below `request.shards`. An
    /// exception, or an index out of range, fails the send before anything is
    /// queued. `encryption=Encryption.aes256_gcm(key)` encrypts the payload and
    /// the user-header block of every record. A publish failure lists its
    /// records as sent, before encryption. `idempotent=True` writes each batch
    /// once per partition, also when a lost reply makes the producer send it
    /// again: the producer takes a producer id, and a resend keeps the id, the
    /// sequence, and the message ids of the first attempt.
    #[pyo3(signature = (*, batch_length=1000, linger_ms=0.0, retries=None, retry_interval_ms=None, key=None, partition=None, create_stream=false, create_topic=true, partitions=1, replication_factor=None, expire_after_ms=None, never_expire=false, max_topic_bytes=None, unlimited_topic_size=false, background=None, sharding=None, encryption=None, idempotent=false))]
    #[allow(clippy::too_many_arguments)]
    fn producer(
        &self,
        batch_length: u32,
        linger_ms: f64,
        retries: Option<u32>,
        retry_interval_ms: Option<f64>,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
        create_stream: bool,
        create_topic: bool,
        partitions: u32,
        replication_factor: Option<u32>,
        expire_after_ms: Option<f64>,
        never_expire: bool,
        max_topic_bytes: Option<u64>,
        unlimited_topic_size: bool,
        background: Option<&Bound<'_, PyAny>>,
        #[gen_stub(override_type(
            type_repr = "typing.Callable[[ShardRequest], int] | None",
            imports = ("typing",)
        ))]
        sharding: Option<&Bound<'_, PyAny>>,
        encryption: Option<PyRef<'_, crate::encryption::PyEncryption>>,
        idempotent: bool,
    ) -> PyResult<PyProducer> {
        let max_topic_size = match (max_topic_bytes, unlimited_topic_size) {
            (Some(_), true) => {
                return Err(InvalidError::new_err(
                    "set max_topic_bytes or unlimited_topic_size, not both",
                ));
            }
            (Some(0), false) => {
                return Err(InvalidError::new_err(
                    "max_topic_bytes must be greater than zero",
                ));
            }
            (Some(bytes), false) => iggy::prelude::MaxTopicSize::from(bytes),
            (None, true) => iggy::prelude::MaxTopicSize::Unlimited,
            (None, false) => iggy::prelude::MaxTopicSize::ServerDefault,
        };
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
        let retry_interval = retry_interval_ms
            .map(|interval| positive_duration_ms(interval, "retry_interval_ms"))
            .transpose()?;
        let stream = self
            .stream
            .clone()
            .or_else(|| self.laser.default_stream().map(str::to_owned))
            .ok_or_else(|| to_pyerr(LaserError::NoStream))?;
        let settings = ProducerSettings {
            batch_length,
            linger: crate::convert::duration_ms(linger_ms, "linger_ms")?,
            retries,
            retry_interval,
            routing: routing(key, partition)?,
            create_stream,
            create_topic,
            partitions,
            replication_factor,
            expiry: match (expire_after_ms, never_expire) {
                (Some(_), true) => {
                    return Err(InvalidError::new_err(
                        "set expire_after_ms or never_expire, not both",
                    ));
                }
                (Some(ms), false) => {
                    let age = crate::convert::duration_ms(ms, "expire_after_ms")?;
                    if age.is_zero() {
                        return Err(InvalidError::new_err(
                            "expire_after_ms must be greater than zero",
                        ));
                    }
                    IggyExpiry::ExpireDuration(age.into())
                }
                (None, true) => IggyExpiry::NeverExpire,
                (None, false) => IggyExpiry::ServerDefault,
            },
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
            sharding: sharding
                .filter(|value| !value.is_none())
                .map(crate::sharding::PySharding::new)
                .transpose()?,
            encryption: encryption.map(|encryption| encryption.inner.clone()),
            idempotent,
        };
        Ok(PyProducer::new(
            self.laser.stream(stream.clone()).topic(&*self.name),
            stream,
            settings,
        ))
    }

    /// Build a Laser reader for one partition. It is an async iterator
    /// and supports the same polling, batching, replay, retry, and commit modes
    /// as the Rust builder. The default `auto_commit="all"` stores once the
    /// last message of a poll is handled, a message counting as handled when
    /// the next one is asked for or the consumer shuts down. Use
    /// `auto_commit="disabled"` plus `commit(message)` for
    /// commit-after-handle delivery. The server holds a
    /// poll that finds fewer than `min_count` new records for up to
    /// `max_wait_ms` (0 to 600000, 0 answers at once), and `max_bytes` caps
    /// the record bytes of one reply. `encryption=Encryption.aes256_gcm(key)`
    /// decrypts the payload and the user-header block of every record. A
    /// record that does not decrypt raises `TransportError` once, with Apache
    /// Iggy error code 72, and is read past. It refuses `auto_commit="polling"`,
    /// because the server would store a poll before it is decrypted.
    #[pyo3(signature = (name, *, partition, batch_length=1000, max_wait_ms=5000.0, min_count=1, max_bytes=1048576, polling="next", offset=None, timestamp_micros=None, auto_commit="all", commit_interval_ms=0.0, commit_every=None, polling_retry_interval_ms=1000.0, init_retries=None, init_retry_interval_ms=1000.0, allow_replay=false, encryption=None))]
    #[allow(clippy::too_many_arguments)]
    fn consumer(
        &self,
        name: &str,
        partition: u32,
        batch_length: u32,
        max_wait_ms: f64,
        min_count: u32,
        max_bytes: u32,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['first', 'last', 'next', 'offset', 'timestamp']",
            imports = ("typing",)
        ))]
        polling: &str,
        offset: Option<u64>,
        timestamp_micros: Option<u64>,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['disabled', 'interval', 'polling', 'all', 'each', 'every']",
            imports = ("typing",)
        ))]
        auto_commit: &str,
        commit_interval_ms: f64,
        commit_every: Option<u32>,
        polling_retry_interval_ms: f64,
        init_retries: Option<u32>,
        init_retry_interval_ms: f64,
        allow_replay: bool,
        encryption: Option<PyRef<'_, crate::encryption::PyEncryption>>,
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
                max_wait_ms,
                min_count,
                max_bytes,
                polling,
                offset,
                timestamp_micros,
                auto_commit,
                commit_interval_ms,
                commit_every,
                create_group: false,
                polling_retry_interval_ms,
                init_retries,
                init_retry_interval_ms,
                allow_replay,
                encryption: encryption.map(|encryption| encryption.inner.clone()),
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
    /// first when needed. `replication_factor` is how many nodes store each
    /// partition, 1 to `MAX_REPLICATION_FACTOR` and at most the cluster's node
    /// count, the server default when `None`. An existing topic is left as it is.
    #[pyo3(signature = (partitions, replication_factor=None))]
    fn ensure<'py>(
        &self,
        py: Python<'py>,
        partitions: u32,
        replication_factor: Option<u32>,
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let name = self.name.clone();
        awaitable(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*name),
                None => laser.topic(&*name),
            };
            handle
                .ensure(partitions, replication_factor)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Idempotently create the consumer group `name` on this topic without
    /// joining it.
    fn ensure_consumer_group<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let stream = self.stream.clone();
        let topic = self.name.clone();
        awaitable(py, async move {
            let handle = match &stream {
                Some(stream) => laser.stream(stream.clone()).topic(&*topic),
                None => laser.topic(&*topic),
            };
            handle.ensure_consumer_group(&name).await.map_err(to_pyerr)
        })
    }

    /// This topic as the server reports it, `None` when the topic or its
    /// stream does not exist.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Option<PyTopicInfo>>> {
        let handle = self.handle();
        awaitable(py, async move {
            Ok(handle.get().await.map_err(to_pyerr)?.map(PyTopicInfo::from))
        })
    }

    /// Whether this topic exists.
    fn exists<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let handle = self.handle();
        awaitable(py, async move { handle.exists().await.map_err(to_pyerr) })
    }

    /// Delete this topic with every message in it. Returns `False` when the
    /// topic or its stream did not exist.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let handle = self.handle();
        awaitable(py, async move { handle.delete().await.map_err(to_pyerr) })
    }

    /// Every partition of this topic. A missing topic raises an error
    /// with `stream_or_topic_not_found` set.
    fn partitions<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyPartitionInfo>>> {
        let handle = self.handle();
        awaitable(py, async move {
            let partitions = handle.partitions().await.map_err(to_pyerr)?;
            Ok(partitions.into_iter().map(PyPartitionInfo::from).collect())
        })
    }

    /// Add `count` partitions after the existing ones. A topic created with a
    /// fixed partition layout refuses the change.
    fn add_partitions<'py>(&self, py: Python<'py>, count: u32) -> PyResult<Awaitable<'py, ()>> {
        let handle = self.handle();
        awaitable(py, async move {
            handle.add_partitions(count).await.map_err(to_pyerr)
        })
    }

    /// Delete the last `count` partitions with their messages. A topic created
    /// with a fixed partition layout refuses the change.
    fn delete_partitions<'py>(&self, py: Python<'py>, count: u32) -> PyResult<Awaitable<'py, ()>> {
        let handle = self.handle();
        awaitable(py, async move {
            handle.delete_partitions(count).await.map_err(to_pyerr)
        })
    }

    /// Change the name, retention, or size cap of this topic. A keyword left
    /// unset keeps its value. Pass `expire_after_ms` or `never_expire`, and
    /// `max_topic_bytes` or `unlimited_topic_size`. After a rename, address
    /// the topic by its new name.
    #[pyo3(signature = (*, name=None, expire_after_ms=None, never_expire=false, max_topic_bytes=None, unlimited_topic_size=false))]
    fn update<'py>(
        &self,
        py: Python<'py>,
        name: Option<String>,
        expire_after_ms: Option<f64>,
        never_expire: bool,
        max_topic_bytes: Option<u64>,
        unlimited_topic_size: bool,
    ) -> PyResult<Awaitable<'py, ()>> {
        let options = laser_sdk::stream::UpdateTopicOptions {
            name,
            expire_after: expire_after_ms
                .map(|expiry| crate::convert::duration_ms(expiry, "expire_after_ms"))
                .transpose()?,
            never_expire,
            max_topic_bytes,
            unlimited_topic_size,
        };
        let handle = self.handle();
        awaitable(
            py,
            async move { handle.update(options).await.map_err(to_pyerr) },
        )
    }

    /// Delete the oldest `count` segments of one partition with their
    /// messages. The open segment stays.
    fn delete_segments<'py>(
        &self,
        py: Python<'py>,
        partition_id: u32,
        count: u32,
    ) -> PyResult<Awaitable<'py, ()>> {
        let handle = self.handle();
        awaitable(py, async move {
            handle
                .delete_segments(partition_id, count)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Every consumer group of this topic. A missing topic raises an error
    /// with `stream_or_topic_not_found` set.
    fn consumer_groups<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Vec<PyConsumerGroupSummary>>> {
        let handle = self.handle();
        awaitable(py, async move {
            let groups = handle.consumer_groups().await.map_err(to_pyerr)?;
            Ok(groups
                .into_iter()
                .map(PyConsumerGroupSummary::from)
                .collect())
        })
    }

    fn __repr__(&self) -> String {
        match &self.stream {
            Some(stream) => format!("Topic(stream={stream}, name={})", self.name),
            None => format!("Topic(name={})", self.name),
        }
    }
}

/// A stream that lives for one program run, from `Laser.scratch_stream`.
/// Entering `async with` creates it, leaving deletes it. `name` and `laser`
/// read it once entered, and `delete()` removes it earlier.
#[gen_stub_pyclass]
#[pyclass(name = "ScratchStream")]
pub struct PyScratchStream {
    laser: Laser,
    prefix: String,
    opened: Arc<Mutex<Option<Arc<ScratchStream>>>>,
}

impl PyScratchStream {
    fn opened(&self) -> PyResult<Arc<ScratchStream>> {
        self.opened
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| {
                InvalidError::new_err(
                    "the scratch stream is not open: enter it with `async with laser.scratch_stream(prefix) as scratch`",
                )
            })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScratchStream {
    /// The stream's unique name.
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.opened()?.name().to_owned())
    }

    /// A handle on the same connection whose default stream is this one.
    #[getter]
    fn laser(&self) -> PyResult<PyLaser> {
        Ok(PyLaser::from_inner(self.opened()?.laser().clone()))
    }

    /// Delete the stream with every topic and message in it. Idempotent.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let scratch = self.opened()?;
        awaitable(py, async move { scratch.delete().await.map_err(to_pyerr) })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Awaitable<'py, Py<Self>>> {
        let (laser, prefix, opened) = {
            let this = slf.borrow(py);
            (
                this.laser.clone(),
                this.prefix.clone(),
                Arc::clone(&this.opened),
            )
        };
        awaitable(py, async move {
            let scratch = laser.scratch_stream(prefix).await.map_err(to_pyerr)?;
            *opened
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(scratch));
            Ok(slf)
        })
    }

    /// Delete the stream and let an exception in the block propagate.
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let scratch = self
            .opened
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        awaitable(py, async move {
            if let Some(scratch) = scratch {
                scratch.delete().await.map_err(to_pyerr)?;
            }
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        match self.opened() {
            Ok(scratch) => format!("ScratchStream(name={})", scratch.name()),
            Err(_) => format!("ScratchStream(prefix={}, not open)", self.prefix),
        }
    }
}
