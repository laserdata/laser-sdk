use crate::client::PyLaser;
use crate::errors::{InvalidError, to_pyerr};
use crate::publish::{PyBatchPublish, PyPublish};
use crate::reader::PyCursor;
use crate::transport::{
    ConsumerConfig, PyConsumer, PyConsumerGroupTarget, PyProducer, configure_consumer, duration_ms,
    partitioning,
};
use crate::typed::{PyTypedRecords, body_to_json};
use iggy::prelude::{DirectConfig, IggyExpiry, MaxTopicSize};
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
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

    /// The topic accessor against the default stream, the one-word shortcut
    /// (`laser.stream(name).topic(name)` addresses any topic on any stream).
    /// Raises the typed no-stream error at the verbs when the client was
    /// connected without a default stream. Pass `cls=` (a dataclass or
    /// pydantic model) for the typed handle: `publish(body)` encodes it and
    /// `records(reader_name)` decodes every record back into the class.
    #[pyo3(signature = (name, *, cls=None))]
    fn topic(&self, name: String, cls: Option<Py<PyAny>>) -> PyTopic {
        PyTopic {
            laser: self.inner.clone(),
            stream: None,
            name,
            cls,
        }
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

    /// A topic on this stream. Pass `cls=` (a dataclass or pydantic model)
    /// for the typed handle.
    #[pyo3(signature = (name, *, cls=None))]
    fn topic(&self, name: String, cls: Option<Py<PyAny>>) -> PyTopic {
        PyTopic {
            laser: self.laser.clone(),
            stream: Some(self.name.clone()),
            name,
            cls,
        }
    }

    /// Idempotently create this stream.
    fn ensure<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser.stream(&name).ensure().await.map_err(to_pyerr)
        })
    }

    fn __repr__(&self) -> String {
        format!("Stream(name={})", self.name)
    }
}

/// One topic: where records live. Publish to it, replay it, ensure it. Build
/// with `Laser.topic` (default stream) or `Stream.topic`.
#[gen_stub_pyclass]
#[pyclass(name = "Topic")]
pub struct PyTopic {
    laser: Laser,
    stream: Option<String>,
    name: String,
    cls: Option<Py<PyAny>>,
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
    /// given, the record is already typed: a dataclass instance, a pydantic
    /// model, or any JSON-shaped value is encoded as JSON with `agdx.ct`
    /// stamped, and the builder is ready to `.send()`.
    #[pyo3(signature = (body=None))]
    fn publish(&self, body: Option<&Bound<'_, PyAny>>) -> PyResult<PyPublish> {
        let request = PyPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone());
        match body {
            Some(body) => Ok(request.with_json_body(body_to_json(body)?)),
            None => Ok(request),
        }
    }

    /// The typed reader over this topic under the consumer identity
    /// `reader_name`,
    /// decoding every record into the topic's `cls` (pass `cls=` at
    /// `laser.topic(..)`). Own the offsets exactly like `replay()`: persist
    /// `offsets` and resume with `from_offsets=`.
    #[pyo3(signature = (reader_name, *, batch=None, from_offsets=None))]
    fn records(
        &self,
        reader_name: String,
        batch: Option<u32>,
        from_offsets: Option<Vec<u64>>,
    ) -> PyResult<PyTypedRecords> {
        let Some(cls) = &self.cls else {
            return Err(InvalidError::new_err(
                "records() needs a typed topic: open it with laser.topic(name, cls=YourClass)",
            ));
        };
        Ok(PyTypedRecords::new(
            self.laser.clone(),
            self.stream.clone(),
            self.name.clone(),
            Python::attach(|py| cls.clone_ref(py)),
            reader_name,
            batch,
            from_offsets.unwrap_or_default(),
        ))
    }

    /// Start a batch publish that flushes 1..N records in a single Iggy send.
    fn publish_batch(&self) -> PyBatchPublish {
        PyBatchPublish::new(self.laser.clone(), self.stream.clone(), self.name.clone())
    }

    /// Build a Laser direct producer. This is the full streaming hot path
    /// below the typed publish API: tune batching/linger/retries,
    /// topology creation, and default key or partition, then `await send(...)`.
    #[pyo3(signature = (*, batch_length=1000, linger_ms=0, retries=Some(3), retry_interval_ms=1000, key=None, partition=None, create_stream=true, create_topic=true, partitions=1, replication_factor=None, message_expiry="server_default", max_topic_size=0))]
    #[allow(clippy::too_many_arguments)]
    fn producer(
        &self,
        batch_length: u32,
        linger_ms: u64,
        retries: Option<u32>,
        retry_interval_ms: u64,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
        create_stream: bool,
        create_topic: bool,
        partitions: u32,
        replication_factor: Option<u8>,
        message_expiry: &str,
        max_topic_size: u64,
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
        let handle = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        let mut builder = handle
            .iggy_producer()
            .map_err(to_pyerr)?
            .direct(
                DirectConfig::builder()
                    .batch_length(batch_length)
                    .linger_time(duration_ms(linger_ms))
                    .build(),
            )
            .partitioning(partitioning(key, partition)?)
            .send_retries(retries, Some(duration_ms(retry_interval_ms)));
        builder = if create_stream {
            builder.create_stream_if_not_exists()
        } else {
            builder.do_not_create_stream_if_not_exists()
        };
        builder = if create_topic {
            let expiry = message_expiry
                .parse::<IggyExpiry>()
                .map_err(InvalidError::new_err)?;
            builder.create_topic_if_not_exists(
                partitions,
                replication_factor,
                expiry,
                MaxTopicSize::from(max_topic_size),
            )
        } else {
            builder.do_not_create_topic_if_not_exists()
        };
        Ok(PyProducer::new(builder.build()))
    }

    /// Build a Laser reader for one partition. It is an async iterator
    /// and supports the same polling, batching, replay, retry, and commit modes
    /// as the Rust builder. Use `auto_commit="disabled"` plus
    /// `commit(message)` for commit-after-handle delivery.
    #[pyo3(signature = (name, *, partition=0, batch_length=1000, poll_interval_ms=None, polling="next", offset=None, timestamp_micros=None, auto_commit="polling", commit_interval_ms=1000, commit_every=None, polling_retry_interval_ms=1000, init_retries=None, init_retry_interval_ms=1000, allow_replay=false))]
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
        let builder = handle.iggy_consumer(name, partition).map_err(to_pyerr)?;
        configure_consumer(
            builder,
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
            None,
        )
    }

    /// Build a Laser consumer-group reader. Group creation/joining is
    /// configurable, and offsets are stored under `group` on the server.
    #[pyo3(signature = (group, *, batch_length=1000, poll_interval_ms=None, polling="next", offset=None, timestamp_micros=None, auto_commit="polling", commit_interval_ms=1000, commit_every=None, auto_join_group=true, create_group=true, polling_retry_interval_ms=1000, init_retries=None, init_retry_interval_ms=1000, allow_replay=false))]
    #[allow(clippy::too_many_arguments)]
    fn consumer_group(
        &self,
        group: &str,
        batch_length: u32,
        poll_interval_ms: Option<u64>,
        polling: &str,
        offset: Option<u64>,
        timestamp_micros: Option<u64>,
        auto_commit: &str,
        commit_interval_ms: u64,
        commit_every: Option<u32>,
        auto_join_group: bool,
        create_group: bool,
        polling_retry_interval_ms: u64,
        init_retries: Option<u32>,
        init_retry_interval_ms: u64,
        allow_replay: bool,
    ) -> PyResult<PyConsumer> {
        let stream = self
            .stream
            .as_deref()
            .or_else(|| self.laser.default_stream())
            .ok_or_else(|| to_pyerr(laser_sdk::error::LaserError::NoStream))?
            .to_owned();
        let shutdown_target = PyConsumerGroupTarget {
            laser: self.laser.clone(),
            stream,
            topic: self.name.clone(),
            group: group.to_owned(),
        };
        let handle = match &self.stream {
            Some(stream) => self.laser.stream(stream.clone()).topic(&*self.name),
            None => self.laser.topic(&*self.name),
        };
        let builder = handle.iggy_consumer_group(group).map_err(to_pyerr)?;
        configure_consumer(
            builder,
            ConsumerConfig {
                batch_length,
                poll_interval_ms,
                polling,
                offset,
                timestamp_micros,
                auto_commit,
                commit_interval_ms,
                commit_every,
                auto_join_group,
                create_group,
                polling_retry_interval_ms,
                init_retries,
                init_retry_interval_ms,
                allow_replay,
            },
            true,
            Some(shutdown_target),
        )
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

    fn __repr__(&self) -> String {
        match &self.stream {
            Some(stream) => format!("Topic(stream={stream}, name={})", self.name),
            None => format!("Topic(name={})", self.name),
        }
    }
}
