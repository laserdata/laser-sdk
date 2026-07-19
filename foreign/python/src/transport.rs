use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, ConsumerGroupClient, HeaderKey, HeaderKind, HeaderValue,
    Identifier, IggyConsumer, IggyConsumerBuilder, IggyDuration, IggyMessage, IggyProducer,
    IggyTimestamp, Partitioning, PollingStrategy, ReceivedMessage,
};
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

pub(crate) fn transport_error(error: impl Into<LaserError>) -> PyErr {
    to_pyerr(error.into())
}

pub(crate) fn duration_ms(value: u64) -> IggyDuration {
    IggyDuration::from(Duration::from_millis(value))
}

enum PollingMode {
    First,
    Last,
    Next,
    Offset,
    Timestamp,
}

impl FromStr for PollingMode {
    type Err = PyErr;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            "next" => Ok(Self::Next),
            "offset" => Ok(Self::Offset),
            "timestamp" => Ok(Self::Timestamp),
            _ => Err(InvalidError::new_err(
                "polling must be first, last, next, offset, or timestamp",
            )),
        }
    }
}

impl Display for PollingMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::First => "first",
            Self::Last => "last",
            Self::Next => "next",
            Self::Offset => "offset",
            Self::Timestamp => "timestamp",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Copy)]
enum CommitMode {
    Disabled,
    Interval,
    Polling,
    All,
    Each,
    Every,
}

impl FromStr for CommitMode {
    type Err = PyErr;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "interval" => Ok(Self::Interval),
            "polling" => Ok(Self::Polling),
            "all" => Ok(Self::All),
            "each" => Ok(Self::Each),
            "every" => Ok(Self::Every),
            _ => Err(InvalidError::new_err(
                "auto_commit must be disabled, interval, polling, all, each, or every",
            )),
        }
    }
}

impl Display for CommitMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Disabled => "disabled",
            Self::Interval => "interval",
            Self::Polling => "polling",
            Self::All => "all",
            Self::Each => "each",
            Self::Every => "every",
        };
        formatter.write_str(value)
    }
}

pub(crate) struct ConsumerConfig<'a> {
    pub batch_length: u32,
    pub poll_interval_ms: Option<u64>,
    pub polling: &'a str,
    pub offset: Option<u64>,
    pub timestamp_micros: Option<u64>,
    pub auto_commit: &'a str,
    pub commit_interval_ms: u64,
    pub commit_every: Option<u32>,
    pub auto_join_group: bool,
    pub create_group: bool,
    pub polling_retry_interval_ms: u64,
    pub init_retries: Option<u32>,
    pub init_retry_interval_ms: u64,
    pub allow_replay: bool,
}

pub(crate) fn configure_consumer(
    mut builder: IggyConsumerBuilder,
    config: ConsumerConfig<'_>,
    group: bool,
    shutdown_target: Option<PyConsumerGroupTarget>,
) -> PyResult<PyConsumer> {
    if config.batch_length == 0 {
        return Err(InvalidError::new_err(
            "consumer batch_length must be greater than zero",
        ));
    }
    let polling = match (config.offset, config.timestamp_micros) {
        (Some(_), Some(_)) => {
            return Err(InvalidError::new_err(
                "offset and timestamp_micros are mutually exclusive",
            ));
        }
        (Some(offset), None) => PollingStrategy::offset(offset),
        (None, Some(timestamp)) => PollingStrategy::timestamp(IggyTimestamp::from(timestamp)),
        (None, None) => match config.polling.parse::<PollingMode>()? {
            PollingMode::First => PollingStrategy::first(),
            PollingMode::Last => PollingStrategy::last(),
            PollingMode::Next => PollingStrategy::next(),
            PollingMode::Offset => {
                return Err(InvalidError::new_err("polling='offset' requires offset"));
            }
            PollingMode::Timestamp => {
                return Err(InvalidError::new_err(
                    "polling='timestamp' requires timestamp_micros",
                ));
            }
        },
    };
    let mode = config.auto_commit.parse::<CommitMode>()?;
    let commit_when = match mode {
        CommitMode::Polling => Some(AutoCommitWhen::PollingMessages),
        CommitMode::All => Some(AutoCommitWhen::ConsumingAllMessages),
        CommitMode::Each => Some(AutoCommitWhen::ConsumingEachMessage),
        CommitMode::Every => {
            let every = config
                .commit_every
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    InvalidError::new_err("auto_commit='every' requires commit_every > 0")
                })?;
            Some(AutoCommitWhen::ConsumingEveryNthMessage(every))
        }
        CommitMode::Disabled | CommitMode::Interval => None,
    };
    let auto_commit = match (mode, config.commit_interval_ms, commit_when) {
        (CommitMode::Disabled, _, _) => AutoCommit::Disabled,
        (CommitMode::Interval, 0, _) => {
            return Err(InvalidError::new_err(
                "auto_commit='interval' requires commit_interval_ms > 0",
            ));
        }
        (CommitMode::Interval, interval, _) => AutoCommit::Interval(duration_ms(interval)),
        (_, 0, Some(mode)) => AutoCommit::When(mode),
        (_, interval, Some(mode)) => AutoCommit::IntervalOrWhen(duration_ms(interval), mode),
        _ => unreachable!("commit modes are exhaustively mapped"),
    };
    builder = builder
        .batch_length(config.batch_length)
        .polling_strategy(polling)
        .auto_commit(auto_commit)
        .polling_retry_interval(duration_ms(config.polling_retry_interval_ms));
    builder = match config.poll_interval_ms {
        Some(interval) => builder.poll_interval(duration_ms(interval)),
        None => builder.without_poll_interval(),
    };
    if group {
        builder = if config.auto_join_group {
            builder.auto_join_consumer_group()
        } else {
            builder.do_not_auto_join_consumer_group()
        };
        builder = if config.create_group {
            builder.create_consumer_group_if_not_exists()
        } else {
            builder.do_not_create_consumer_group_if_not_exists()
        };
    }
    if let Some(retries) = config.init_retries {
        builder = builder.init_retries(retries, duration_ms(config.init_retry_interval_ms));
    }
    if config.allow_replay {
        builder = builder.allow_replay();
    }
    let manual_commit = matches!(mode, CommitMode::Disabled);
    let shutdown_target = (manual_commit && config.auto_join_group)
        .then_some(shutdown_target)
        .flatten();
    Ok(PyConsumer::new(
        builder.build(),
        manual_commit,
        shutdown_target,
    ))
}

pub(crate) fn partitioning(
    key: Option<&Bound<'_, PyAny>>,
    partition: Option<u32>,
) -> PyResult<Partitioning> {
    match (key, partition) {
        (Some(_), Some(_)) => Err(InvalidError::new_err(
            "key and partition are mutually exclusive",
        )),
        (Some(key), None) => {
            Partitioning::messages_key(&payload_bytes(key)?).map_err(transport_error)
        }
        (None, Some(partition)) => Ok(Partitioning::partition_id(partition)),
        (None, None) => Ok(Partitioning::balanced()),
    }
}

fn header_value(value: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
    if let Ok(pair) = value.cast::<PyTuple>() {
        if pair.len() != 2 {
            return Err(InvalidError::new_err(
                "a typed header must be (kind, value)",
            ));
        }
        let kind = pair.get_item(0)?.extract::<String>()?;
        let kind = kind.parse::<HeaderKind>().map_err(transport_error)?;
        let value = pair.get_item(1)?;
        let invalid = || InvalidError::new_err(format!("header value does not fit {kind}"));
        return match kind {
            HeaderKind::Raw => {
                HeaderValue::try_from(payload_bytes(&value)?).map_err(transport_error)
            }
            HeaderKind::String => {
                HeaderValue::try_from(value.extract::<String>()?).map_err(transport_error)
            }
            HeaderKind::Bool => value
                .extract::<bool>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Int8 => value
                .extract::<i8>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Int16 => value
                .extract::<i16>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Int32 => value
                .extract::<i32>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Int64 => value
                .extract::<i64>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Int128 => value
                .extract::<i128>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Uint8 => value
                .extract::<u8>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Uint16 => value
                .extract::<u16>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Uint32 => value
                .extract::<u32>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Uint64 => value
                .extract::<u64>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Uint128 => value
                .extract::<u128>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Float32 => value
                .extract::<f32>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
            HeaderKind::Float64 => value
                .extract::<f64>()
                .map(HeaderValue::from)
                .map_err(|_| invalid()),
        };
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(value.into());
    }
    if let Ok(value) = value.extract::<u64>() {
        return Ok(value.into());
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(value.into());
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(value.into());
    }
    if let Ok(value) = value.extract::<String>() {
        return HeaderValue::try_from(value).map_err(transport_error);
    }
    if let Ok(value) = payload_bytes(value) {
        return HeaderValue::try_from(value).map_err(transport_error);
    }
    Err(InvalidError::new_err(
        "header values must be str, bytes, bool, int, or float",
    ))
}

fn headers(values: Option<&Bound<'_, PyDict>>) -> PyResult<BTreeMap<HeaderKey, HeaderValue>> {
    let Some(values) = values else {
        return Ok(BTreeMap::new());
    };
    values
        .iter()
        .map(|(key, value)| {
            let key = key.extract::<String>()?;
            let key = HeaderKey::try_from(key).map_err(transport_error)?;
            Ok((key, header_value(&value)?))
        })
        .collect()
}

fn message(
    payload: &Bound<'_, PyAny>,
    headers_value: Option<&Bound<'_, PyDict>>,
) -> PyResult<IggyMessage> {
    IggyMessage::builder()
        .payload(payload_bytes(payload)?.into())
        .user_headers(headers(headers_value)?)
        .build()
        .map_err(transport_error)
}

fn messages(values: &Bound<'_, PyAny>) -> PyResult<Vec<IggyMessage>> {
    values
        .try_iter()?
        .map(|value| {
            let value = value?;
            let Ok(pair) = value.cast::<PyTuple>() else {
                return message(&value, None);
            };
            if pair.len() != 2 {
                return Err(InvalidError::new_err(
                    "a batch tuple must be (payload, headers)",
                ));
            }
            let payload = pair.get_item(0)?;
            let headers_value = pair.get_item(1)?;
            let headers_value = if headers_value.is_none() {
                None
            } else {
                Some(headers_value.cast::<PyDict>().map_err(PyErr::from)?)
            };
            message(&payload, headers_value)
        })
        .collect()
}

/// A configurable Laser streaming producer. Build it with `Topic.producer`.
/// Sends use Apache Iggy's direct producer path and accept per-send key or
/// partition overrides without passing through the typed publish layer.
#[gen_stub_pyclass]
#[pyclass(name = "Producer")]
pub struct PyProducer {
    inner: Arc<IggyProducer>,
}

impl PyProducer {
    pub(crate) fn new(inner: IggyProducer) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProducer {
    /// Initialize the producer, including configured stream/topic creation.
    /// `send` and `send_batch` also initialize lazily, so calling this is useful
    /// when startup should fail before accepting work.
    fn init<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let producer = self.inner.clone();
        future_into_py(
            py,
            async move { producer.init().await.map_err(transport_error) },
        )
    }

    /// Send one raw message. `headers` preserves Python scalar types as Iggy
    /// typed headers. `key` and `partition` are mutually exclusive.
    #[pyo3(signature = (payload, *, headers=None, key=None, partition=None))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let message = message(payload, headers)?;
        let partitioning = match (key, partition) {
            (None, None) => None,
            _ => Some(Arc::new(partitioning(key, partition)?)),
        };
        let producer = self.inner.clone();
        future_into_py(py, async move {
            producer.init().await.map_err(transport_error)?;
            producer
                .send_with_partitioning(vec![message], partitioning)
                .await
                .map_err(transport_error)
        })
    }

    /// Send one Iggy batch. Each item is either a raw payload or
    /// `(payload, headers)`. All records share the optional key or partition,
    /// matching Iggy's `send_with_partitioning` contract.
    #[pyo3(signature = (values, *, key=None, partition=None))]
    fn send_batch<'py>(
        &self,
        py: Python<'py>,
        values: &Bound<'_, PyAny>,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = messages(values)?;
        let count = messages.len();
        let partitioning = match (key, partition) {
            (None, None) => None,
            _ => Some(Arc::new(partitioning(key, partition)?)),
        };
        let producer = self.inner.clone();
        future_into_py(py, async move {
            producer.init().await.map_err(transport_error)?;
            producer
                .send_with_partitioning(messages, partitioning)
                .await
                .map_err(transport_error)?;
            Ok(count)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Producer(stream={}, topic={})",
            self.inner.stream(),
            self.inner.topic()
        )
    }
}

#[derive(Clone)]
enum PyHeaderValue {
    Raw(Vec<u8>),
    String(String),
    Bool(bool),
    Int(i128),
    Uint(u128),
    Float(f64),
}

#[derive(Clone)]
struct PyHeader {
    kind: String,
    value: PyHeaderValue,
}

impl TryFrom<HeaderValue> for PyHeader {
    type Error = laser_sdk::iggy::prelude::IggyError;

    fn try_from(value: HeaderValue) -> Result<Self, Self::Error> {
        let kind = value.kind();
        let converted = match kind {
            HeaderKind::Raw => PyHeaderValue::Raw(value.as_raw()?.to_vec()),
            HeaderKind::String => PyHeaderValue::String(value.as_str()?.to_owned()),
            HeaderKind::Bool => PyHeaderValue::Bool(value.as_bool()?),
            HeaderKind::Int8 => PyHeaderValue::Int(value.as_int8()?.into()),
            HeaderKind::Int16 => PyHeaderValue::Int(value.as_int16()?.into()),
            HeaderKind::Int32 => PyHeaderValue::Int(value.as_int32()?.into()),
            HeaderKind::Int64 => PyHeaderValue::Int(value.as_int64()?.into()),
            HeaderKind::Int128 => PyHeaderValue::Int(value.as_int128()?),
            HeaderKind::Uint8 => PyHeaderValue::Uint(value.as_uint8()?.into()),
            HeaderKind::Uint16 => PyHeaderValue::Uint(value.as_uint16()?.into()),
            HeaderKind::Uint32 => PyHeaderValue::Uint(value.as_uint32()?.into()),
            HeaderKind::Uint64 => PyHeaderValue::Uint(value.as_uint64()?.into()),
            HeaderKind::Uint128 => PyHeaderValue::Uint(value.as_uint128()?),
            HeaderKind::Float32 => PyHeaderValue::Float(value.as_float32()?.into()),
            HeaderKind::Float64 => PyHeaderValue::Float(value.as_float64()?),
        };
        Ok(Self {
            kind: kind.to_string(),
            value: converted,
        })
    }
}

/// One message yielded by a Laser consumer, including its exact log
/// position and the server-side consumer offset used for manual commits.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerMessage", frozen)]
pub struct PyConsumerMessage {
    payload: Vec<u8>,
    message_id: String,
    headers: BTreeMap<String, PyHeader>,
    #[pyo3(get)]
    pub checksum: u64,
    #[pyo3(get)]
    pub offset: u64,
    #[pyo3(get)]
    pub current_offset: u64,
    #[pyo3(get)]
    pub partition_id: u32,
    #[pyo3(get)]
    pub timestamp_micros: u64,
    #[pyo3(get)]
    pub origin_timestamp_micros: u64,
}

impl TryFrom<ReceivedMessage> for PyConsumerMessage {
    type Error = laser_sdk::iggy::prelude::IggyError;

    fn try_from(received: ReceivedMessage) -> Result<Self, Self::Error> {
        let headers = received
            .message
            .user_headers_map()?
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| Ok((key.to_string_value(), value.try_into()?)))
            .collect::<Result<_, Self::Error>>()?;
        Ok(Self {
            payload: received.message.payload.to_vec(),
            message_id: received.message.header.id.to_string(),
            headers,
            checksum: received.message.header.checksum,
            offset: received.message.header.offset,
            current_offset: received.current_offset,
            partition_id: received.partition_id,
            timestamp_micros: received.message.header.timestamp,
            origin_timestamp_micros: received.message.header.origin_timestamp,
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerMessage {
    /// Raw payload bytes.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    /// Iggy's message identifier.
    #[getter]
    fn message_id(&self) -> String {
        self.message_id.clone()
    }

    /// Typed Iggy user headers as Python `bytes`, `str`, `bool`, `int`, or
    /// `float` values. Send `(kind, value)`, for example `("uint16", 7)`,
    /// when the receiver requires an exact numeric width.
    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let values = PyDict::new(py);
        for (key, header) in &self.headers {
            match &header.value {
                PyHeaderValue::Raw(value) => values.set_item(key, PyBytes::new(py, value))?,
                PyHeaderValue::String(value) => values.set_item(key, value)?,
                PyHeaderValue::Bool(value) => values.set_item(key, value)?,
                PyHeaderValue::Int(value) => values.set_item(key, value)?,
                PyHeaderValue::Uint(value) => values.set_item(key, value)?,
                PyHeaderValue::Float(value) => values.set_item(key, value)?,
            }
        }
        Ok(values.unbind())
    }

    /// Exact Iggy value kind for each user header (`uint16`, `string`, etc.).
    #[getter]
    fn header_kinds(&self) -> BTreeMap<String, String> {
        self.headers
            .iter()
            .map(|(key, value)| (key.clone(), value.kind.clone()))
            .collect()
    }

    /// Decode the payload as JSON.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.payload)
            .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))?;
        crate::convert::json_to_py(py, &value)
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsumerMessage(partition={}, offset={}, bytes={})",
            self.partition_id,
            self.offset,
            self.payload.len()
        )
    }
}

/// A Laser partition or consumer-group reader. It is an async iterator and
/// exposes manual offset storage for commit-after-handle delivery.
#[gen_stub_pyclass]
#[pyclass(name = "Consumer")]
pub struct PyConsumer {
    name: String,
    inner: Arc<Mutex<Option<IggyConsumer>>>,
    shutdown: watch::Sender<bool>,
    manual_commit: bool,
    shutdown_target: Option<PyConsumerGroupTarget>,
}

impl PyConsumer {
    pub(crate) fn new(
        inner: IggyConsumer,
        manual_commit: bool,
        shutdown_target: Option<PyConsumerGroupTarget>,
    ) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            name: inner.name().to_owned(),
            inner: Arc::new(Mutex::new(Some(inner))),
            shutdown,
            manual_commit,
            shutdown_target,
        }
    }

    async fn receive(
        inner: Arc<Mutex<Option<IggyConsumer>>>,
        shutdown: watch::Sender<bool>,
    ) -> PyResult<Option<PyConsumerMessage>> {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let mut inner = inner.lock().await;
        let Some(consumer) = inner.as_mut() else {
            return Ok(None);
        };
        consumer.init().await.map_err(transport_error)?;
        let mut shutdown_rx = shutdown.subscribe();
        tokio::select! {
            received = consumer.next() => match received {
                Some(Ok(received)) => Ok(Some(received.try_into().map_err(transport_error)?)),
                Some(Err(error)) => Err(transport_error(error)),
                None => Ok(None),
            },
            result = shutdown_rx.changed() => {
                let _ = result;
                Ok(None)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct PyConsumerGroupTarget {
    pub laser: Laser,
    pub stream: String,
    pub topic: String,
    pub group: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumer {
    /// Consumer or group name.
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Initialize and join/create the configured group. Reads initialize lazily
    /// too, so call this when startup should fail before accepting work.
    fn init<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut inner = inner.lock().await;
            let consumer = inner
                .as_mut()
                .ok_or_else(|| InvalidError::new_err("consumer has been shut down"))?;
            consumer.init().await.map_err(transport_error)
        })
    }

    /// Wait for the next message. Returns `None` after shutdown.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let shutdown = self.shutdown.clone();
        future_into_py(py, async move { Self::receive(inner, shutdown).await })
    }

    /// Store `offset` on the server for the message partition. With no
    /// `partition`, Iggy uses the consumer's current partition.
    #[pyo3(signature = (offset, *, partition=None))]
    fn store_offset<'py>(
        &self,
        py: Python<'py>,
        offset: u64,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let inner = inner.lock().await;
            inner
                .as_ref()
                .ok_or_else(|| InvalidError::new_err("consumer has been shut down"))?
                .store_offset(offset, partition)
                .await
                .map_err(transport_error)
        })
    }

    /// Store a successfully handled message's offset on the server.
    fn commit<'py>(
        &self,
        py: Python<'py>,
        message: &PyConsumerMessage,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let offset = message.offset;
        let partition = message.partition_id;
        future_into_py(py, async move {
            let inner = inner.lock().await;
            inner
                .as_ref()
                .ok_or_else(|| InvalidError::new_err("consumer has been shut down"))?
                .store_offset(offset, Some(partition))
                .await
                .map_err(transport_error)
        })
    }

    /// Delete the stored server offset for one partition or the consumer's
    /// current partition.
    #[pyo3(signature = (*, partition=None))]
    fn delete_offset<'py>(
        &self,
        py: Python<'py>,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let inner = inner.lock().await;
            inner
                .as_ref()
                .ok_or_else(|| InvalidError::new_err("consumer has been shut down"))?
                .delete_offset(partition)
                .await
                .map_err(transport_error)
        })
    }

    /// Last message offset yielded locally for `partition`.
    fn last_consumed_offset<'py>(
        &self,
        py: Python<'py>,
        partition: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(inner
                .lock()
                .await
                .as_ref()
                .and_then(|consumer| consumer.get_last_consumed_offset(partition)))
        })
    }

    /// Last offset this consumer stored on the server for `partition`.
    fn last_stored_offset<'py>(
        &self,
        py: Python<'py>,
        partition: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(inner
                .lock()
                .await
                .as_ref()
                .and_then(|consumer| consumer.get_last_stored_offset(partition)))
        })
    }

    /// Stop polling and leave the group. Automatic policies flush final offset
    /// state. Disabled auto-commit preserves the last explicit commit.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let already_shutdown = self.shutdown.send_replace(true);
        let inner = self.inner.clone();
        let manual_commit = self.manual_commit;
        let shutdown_target = self.shutdown_target.clone();
        future_into_py(py, async move {
            if already_shutdown {
                return Ok(());
            }
            if manual_commit {
                drop(inner.lock().await.take());
                if let Some(target) = shutdown_target {
                    target
                        .laser
                        .client()
                        .leave_consumer_group(
                            &Identifier::try_from(target.stream).map_err(transport_error)?,
                            &Identifier::try_from(target.topic).map_err(transport_error)?,
                            &Identifier::try_from(target.group).map_err(transport_error)?,
                        )
                        .await
                        .map_err(transport_error)?;
                }
                return Ok(());
            }
            let mut inner = inner.lock().await;
            if let Some(consumer) = inner.as_mut() {
                consumer.shutdown().await.map_err(transport_error)?;
            }
            inner.take();
            Ok(())
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let shutdown = self.shutdown.clone();
        future_into_py(py, async move {
            match Self::receive(inner, shutdown).await? {
                Some(message) => Ok(message),
                None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("Consumer(name={})", self.name)
    }
}
