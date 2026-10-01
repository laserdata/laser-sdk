use crate::async_bridge::future_into_py;
use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, ConsumerGroupClient, HeaderKey, HeaderKind, HeaderValue,
    Identifier, IggyConsumer, IggyConsumerBuilder, IggyDuration, IggyMessage, IggyProducer,
    IggyTimestamp, NonZeroIggyDuration, Partitioning, PollingStrategy, ReceivedMessage,
    SendMessagesConfirmationResponse, SendMessagesResponse,
};
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
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

pub(crate) fn positive_duration_ms(
    value: u64,
    field: &'static str,
) -> PyResult<NonZeroIggyDuration> {
    NonZeroIggyDuration::try_from(Duration::from_millis(value))
        .map_err(|_| InvalidError::new_err(format!("{field} must be greater than zero")))
}

/// One committed partition range reported by Apache Iggy after a send.
#[gen_stub_pyclass]
#[pyclass(
    name = "SendMessagesConfirmation",
    frozen,
    get_all,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PySendMessagesConfirmation {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub base_offset: u64,
}

impl From<SendMessagesConfirmationResponse> for PySendMessagesConfirmation {
    fn from(confirmation: SendMessagesConfirmationResponse) -> Self {
        Self {
            stream_id: confirmation.stream_id,
            topic_id: confirmation.topic_id,
            partition_id: confirmation.partition_id,
            base_offset: confirmation.base_offset,
        }
    }
}

/// Commit confirmations returned by a successful Apache Iggy send.
#[gen_stub_pyclass]
#[pyclass(name = "SendMessagesResponse", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PySendMessagesResponse {
    #[pyo3(get)]
    pub confirmations: Vec<PySendMessagesConfirmation>,
}

impl From<SendMessagesResponse> for PySendMessagesResponse {
    fn from(response: SendMessagesResponse) -> Self {
        Self {
            confirmations: response
                .confirmations
                .into_iter()
                .map(PySendMessagesConfirmation::from)
                .collect(),
        }
    }
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
        (CommitMode::Interval, interval, _) => {
            AutoCommit::Interval(positive_duration_ms(interval, "commit_interval_ms")?)
        }
        (_, 0, Some(mode)) => AutoCommit::When(mode),
        (_, interval, Some(mode)) => {
            AutoCommit::IntervalOrWhen(positive_duration_ms(interval, "commit_interval_ms")?, mode)
        }
        _ => unreachable!("commit modes are exhaustively mapped"),
    };
    builder = builder
        .batch_length(config.batch_length)
        .polling_strategy(polling)
        .auto_commit(auto_commit)
        .polling_retry_interval(positive_duration_ms(
            config.polling_retry_interval_ms,
            "polling_retry_interval_ms",
        )?);
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
        builder = builder.init_retries(
            retries,
            positive_duration_ms(config.init_retry_interval_ms, "init_retry_interval_ms")?,
        );
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

pub(crate) fn header_value(value: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
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
    // Matched on the concrete type so an `int` too large for 64 bits raises
    // rather than falling through to the float branch and being stamped as a
    // lossy `Float64` header.
    if value.is_instance_of::<pyo3::types::PyInt>() {
        if let Ok(unsigned) = value.extract::<u64>() {
            return Ok(unsigned.into());
        }
        if let Ok(signed) = value.extract::<i64>() {
            return Ok(signed.into());
        }
        return Err(InvalidError::new_err(
            "header integer is out of range: must fit a signed or unsigned 64-bit integer",
        ));
    }
    if value.is_instance_of::<pyo3::types::PyFloat>() {
        return Ok(value.extract::<f64>()?.into());
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
                .map(PySendMessagesResponse::from)
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
                .map(PySendMessagesResponse::from)
                .map_err(transport_error)
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
        let converted = (|| -> Result<PyHeaderValue, Self::Error> {
            Ok(match kind {
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
            })
        })();
        let (kind, converted) = match converted {
            Ok(converted) => (kind.to_string(), converted),
            Err(_) => (
                "raw".to_owned(),
                PyHeaderValue::Raw(value.as_bytes().to_vec()),
            ),
        };
        Ok(Self {
            kind,
            value: converted,
        })
    }
}

fn received_headers(mut bytes: &[u8]) -> Option<(BTreeMap<String, PyHeader>, bool)> {
    let mut headers = BTreeMap::new();
    let mut malformed = false;
    while !bytes.is_empty() {
        let (key_kind, key_bytes) = header_field(&mut bytes)?;
        let (value_kind, value_bytes) = header_field(&mut bytes)?;
        let Ok(key_kind) = HeaderKind::from_code(key_kind) else {
            continue;
        };
        let key = HeaderKey::from_raw(key_kind, key_bytes).ok()?;
        if key_kind == HeaderKind::String && std::str::from_utf8(key_bytes).is_err() {
            malformed = true;
            continue;
        }
        let raw = || PyHeader {
            kind: "raw".to_owned(),
            value: PyHeaderValue::Raw(value_bytes.to_vec()),
        };
        let value = match HeaderKind::from_code(value_kind) {
            Ok(kind) => {
                let converted = if kind == HeaderKind::Bool && !matches!(value_bytes, [0] | [1]) {
                    None
                } else {
                    HeaderValue::from_raw(kind, value_bytes)
                        .ok()
                        .and_then(|value| PyHeader::try_from(value).ok())
                };
                match converted {
                    Some(value) => {
                        malformed |= value.kind == "raw" && kind != HeaderKind::Raw;
                        value
                    }
                    None => {
                        malformed = true;
                        raw()
                    }
                }
            }
            Err(_) => raw(),
        };
        headers.insert(key.to_string_value(), value);
    }
    Some((headers, malformed))
}

fn header_field<'a>(bytes: &mut &'a [u8]) -> Option<(u8, &'a [u8])> {
    let kind = *bytes.first()?;
    let length = u32::from_le_bytes(bytes.get(1..5)?.try_into().ok()?) as usize;
    if kind == 0 || !(1..=255).contains(&length) {
        return None;
    }
    let value = bytes.get(5..5 + length)?;
    *bytes = &bytes[5 + length..];
    Some((kind, value))
}

/// One message yielded by a Laser consumer, including its exact log
/// position and the server-side consumer offset used for manual commits.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerMessage", frozen)]
pub struct PyConsumerMessage {
    payload: bytes::Bytes,
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
    /// True when a header entry or the block structure is malformed.
    /// Valid entries remain readable. Unknown value kinds are raw bytes.
    /// A structurally truncated block has no decoded headers.
    #[pyo3(get)]
    pub headers_malformed: bool,
}

impl TryFrom<ReceivedMessage> for PyConsumerMessage {
    type Error = laser_sdk::iggy::prelude::IggyError;

    fn try_from(received: ReceivedMessage) -> Result<Self, Self::Error> {
        Self::of(
            &received.message,
            received.partition_id,
            received.current_offset,
        )
    }
}

impl PyConsumerMessage {
    /// A view of one stored record, read through any path that keeps the
    /// record's bytes intact.
    pub(crate) fn of(
        message: &laser_sdk::iggy::prelude::IggyMessage,
        partition_id: u32,
        current_offset: u64,
    ) -> Result<Self, laser_sdk::iggy::prelude::IggyError> {
        let (headers, headers_malformed) =
            received_headers(message.user_headers.as_deref().unwrap_or_default())
                .unwrap_or_else(|| (BTreeMap::new(), true));
        Ok(Self {
            payload: message.payload.clone(),
            message_id: message.header.id.to_string(),
            headers,
            headers_malformed,
            checksum: message.header.checksum,
            offset: message.header.offset,
            current_offset,
            partition_id,
            timestamp_micros: message.header.timestamp,
            origin_timestamp_micros: message.header.origin_timestamp,
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
/// exposes manual offset storage for commit-after-handle delivery. A purge
/// restarts the partition at offset 0 without telling an open reader, which
/// can keep its old position and skip the replacement records, so rebuild it
/// after a purge.
#[gen_stub_pyclass]
#[pyclass(name = "Consumer")]
pub struct PyConsumer {
    name: String,
    inner: Arc<Mutex<Option<IggyConsumer>>>,
    shutdown: watch::Sender<bool>,
    manual_commit: bool,
    yielded_zero: Arc<Mutex<std::collections::BTreeSet<u32>>>,
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
            yielded_zero: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
            shutdown_target,
        }
    }

    async fn receive(
        inner: Arc<Mutex<Option<IggyConsumer>>>,
        shutdown: watch::Sender<bool>,
        yielded_zero: Arc<Mutex<std::collections::BTreeSet<u32>>>,
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
                Some(Ok(received)) => {
                    let partition = received.partition_id;
                    let offset = received.message.header.offset;
                    let message = received.try_into().map_err(transport_error)?;
                    let mut zeros = yielded_zero.lock().await;
                    if offset == 0 { zeros.insert(partition); } else { zeros.remove(&partition); }
                    Ok(Some(message))
                },
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

#[cfg(test)]
mod tests {
    use super::{PyConsumerMessage, PyHeaderValue, positive_duration_ms};
    use iggy::prelude::IggyMessage;

    #[test]
    fn given_unknown_header_values_when_a_record_is_exposed_then_should_preserve_raw_and_known_entries()
     {
        let mut bytes = entry("future", 99, b"opaque");
        bytes.extend(entry("priority", 10, &7_u16.to_le_bytes()));
        let mut message = IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"payload"))
            .build()
            .expect("message");
        message.user_headers = Some(bytes.into());
        let decoded = PyConsumerMessage::of(&message, 0, 0).expect("readable message");
        assert!(!decoded.headers_malformed);
        assert!(
            matches!(&decoded.headers["future"].value, PyHeaderValue::Raw(value) if value == b"opaque")
        );
        assert!(matches!(
            decoded.headers["priority"].value,
            PyHeaderValue::Uint(7)
        ));
    }

    #[test]
    fn given_an_invalid_header_width_when_a_record_is_exposed_then_should_keep_payload_and_other_headers()
     {
        let mut bytes = entry("broken", 10, &[7]);
        bytes.extend(entry("agdx.ct", 9, &[1]));
        let mut message = IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"payload"))
            .build()
            .expect("message");
        message.user_headers = Some(bytes.into());
        let decoded = PyConsumerMessage::of(&message, 0, 0).expect("readable message");
        assert!(decoded.headers_malformed);
        assert_eq!(decoded.payload.as_ref(), b"payload");
        assert!(
            matches!(&decoded.headers["broken"].value, PyHeaderValue::Raw(value) if value == &[7])
        );
        assert!(matches!(
            decoded.headers["agdx.ct"].value,
            PyHeaderValue::Uint(1)
        ));
    }

    fn entry(key: &str, kind: u8, value: &[u8]) -> Vec<u8> {
        let mut bytes = vec![2];
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(kind);
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes
    }

    #[test]
    fn given_retry_intervals_when_converted_then_zero_should_be_rejected() {
        assert!(positive_duration_ms(1, "retry_interval_ms").is_ok());
        assert!(positive_duration_ms(0, "retry_interval_ms").is_err());
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
        let yielded_zero = self.yielded_zero.clone();
        future_into_py(py, async move {
            Self::receive(inner, shutdown, yielded_zero).await
        })
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

    /// Local Iggy SDK offset bookkeeping. An initial zero does not prove a
    /// durable checkpoint exists. Use `next` polling for server-side resume.
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

    /// Stop polling and leave the group. Automatic policies delegate final
    /// offset handling to the Iggy SDK. Polling commits before delivery, so
    /// shutdown is not a processing checkpoint. Disabled auto-commit preserves
    /// the last explicit commit.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let already_shutdown = self.shutdown.send_replace(true);
        let inner = self.inner.clone();
        let manual_commit = self.manual_commit;
        let yielded_zero = self.yielded_zero.clone();
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
            let outcome = if let Some(consumer) = inner.as_mut() {
                let mut stored = Ok(());
                for partition in yielded_zero.lock().await.iter() {
                    if consumer
                        .get_last_stored_offset(*partition)
                        .is_none_or(|offset| offset == 0)
                    {
                        stored = stored.and(consumer.store_offset(0, Some(*partition)).await);
                    }
                }
                let stopped = consumer.shutdown().await;
                stored.and(stopped)
            } else {
                Ok(())
            };
            inner.take();
            outcome.map_err(transport_error)
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let shutdown = self.shutdown.clone();
        let yielded_zero = self.yielded_zero.clone();
        future_into_py(py, async move {
            match Self::receive(inner, shutdown, yielded_zero).await? {
                Some(message) => Ok(message),
                None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("Consumer(name={})", self.name)
    }
}
