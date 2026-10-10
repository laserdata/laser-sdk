use crate::async_bridge::{
    PyHook, Undelivered, future_into_py, future_into_py_returning, hook_callable,
};
use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use iggy::clients::producer_config::BackpressureMode;
use iggy::clients::producer_error_callback::{ErrorCallback, ErrorCtx};
use iggy::prelude::{
    BackgroundConfig, BalancedSharding, HeaderKey, HeaderKind, HeaderValue, IggyByteSize,
    IggyDuration, IggyError, IggyExpiry, NonZeroIggyDuration, OrderedSharding,
    SendMessagesConfirmationResponse, SendMessagesResponse, Sharding,
};
use laser_sdk::error::LaserError;
use laser_sdk::stream::{
    CommitPolicy, Consumer, ConsumerBuilder, ConsumerMessage, ConsumerStart, Producer,
    ProducerMessage, Routing, StoredOffset, Topic,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Poll, ready};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock, watch};

pub(crate) fn transport_error(error: impl Into<LaserError>) -> PyErr {
    to_pyerr(error.into())
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
    name = "SendMessagesConfirmationResponse",
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

/// Apply the Python keyword options to a Laser consumer builder. The consumer
/// itself is built on first use, so construction stays synchronous.
pub(crate) fn configure_consumer(
    mut builder: ConsumerBuilder,
    name: String,
    config: ConsumerConfig<'_>,
    group: bool,
) -> PyResult<PyConsumer> {
    if config.batch_length == 0 {
        return Err(InvalidError::new_err(
            "consumer batch_length must be greater than zero",
        ));
    }
    let start = match (config.offset, config.timestamp_micros) {
        (Some(_), Some(_)) => {
            return Err(InvalidError::new_err(
                "offset and timestamp_micros are mutually exclusive",
            ));
        }
        (Some(offset), None) => ConsumerStart::Offset(offset),
        (None, Some(timestamp)) => ConsumerStart::TimestampMicros(timestamp),
        (None, None) => match config.polling.parse::<PollingMode>()? {
            PollingMode::First => ConsumerStart::First,
            PollingMode::Last => ConsumerStart::Last,
            PollingMode::Next => ConsumerStart::Next,
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
    let interval = Duration::from_millis(config.commit_interval_ms);
    let commit = match (
        config.auto_commit.parse::<CommitMode>()?,
        interval.is_zero(),
    ) {
        (CommitMode::Disabled, _) => CommitPolicy::Disabled,
        (CommitMode::Interval, true) => {
            return Err(InvalidError::new_err(
                "auto_commit='interval' requires commit_interval_ms > 0",
            ));
        }
        (CommitMode::Interval, false) => CommitPolicy::Interval(interval),
        (CommitMode::Polling, true) => CommitPolicy::Polling,
        (CommitMode::Polling, false) => CommitPolicy::IntervalOrPolling(interval),
        (CommitMode::All, true) => CommitPolicy::All,
        (CommitMode::All, false) => CommitPolicy::IntervalOrAll(interval),
        (CommitMode::Each, true) => CommitPolicy::Each,
        (CommitMode::Each, false) => CommitPolicy::IntervalOrEach(interval),
        (CommitMode::Every, zero) => {
            let every = config
                .commit_every
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    InvalidError::new_err("auto_commit='every' requires commit_every > 0")
                })?;
            if zero {
                CommitPolicy::Every(every)
            } else {
                CommitPolicy::IntervalOrEvery(interval, every)
            }
        }
    };
    positive_duration_ms(
        config.polling_retry_interval_ms,
        "polling_retry_interval_ms",
    )?;
    builder = builder
        .batch_length(config.batch_length)
        .start_at(start)
        .commit_policy(commit)
        .polling_retry_interval(Duration::from_millis(config.polling_retry_interval_ms));
    builder = match config.poll_interval_ms {
        Some(interval) => builder.poll_interval(Duration::from_millis(interval)),
        None => builder.without_poll_interval(),
    };
    if group {
        builder = builder
            .auto_join_group(config.auto_join_group)
            .create_group(config.create_group);
    }
    if let Some(retries) = config.init_retries {
        positive_duration_ms(config.init_retry_interval_ms, "init_retry_interval_ms")?;
        builder = builder.init_retries(
            retries,
            Duration::from_millis(config.init_retry_interval_ms),
        );
    }
    if config.allow_replay {
        builder = builder.allow_replay();
    }
    Ok(PyConsumer::new(name, builder))
}

pub(crate) fn routing(key: Option<&Bound<'_, PyAny>>, partition: Option<u32>) -> PyResult<Routing> {
    match (key, partition) {
        (Some(_), Some(_)) => Err(InvalidError::new_err(
            "key and partition are mutually exclusive",
        )),
        (Some(key), None) => Ok(Routing::key(payload_bytes(key)?)),
        (None, Some(partition)) => Ok(Routing::Partition(partition)),
        (None, None) => Ok(Routing::Balanced),
    }
}

fn routing_override(
    key: Option<&Bound<'_, PyAny>>,
    partition: Option<u32>,
) -> PyResult<Option<Routing>> {
    match (key, partition) {
        (None, None) => Ok(None),
        _ => routing(key, partition).map(Some),
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

pub(crate) fn headers(
    values: Option<&Bound<'_, PyDict>>,
) -> PyResult<BTreeMap<HeaderKey, HeaderValue>> {
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
) -> PyResult<ProducerMessage> {
    Ok(ProducerMessage::new(payload_bytes(payload)?).with_headers(headers(headers_value)?))
}

fn messages(values: &Bound<'_, PyAny>) -> PyResult<Vec<ProducerMessage>> {
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

/// The settings a Python `Producer` builds its Laser producer from on first use.
#[derive(Clone)]
pub(crate) struct ProducerSettings {
    pub batch_length: u32,
    pub linger: Duration,
    pub retries: Option<u32>,
    pub retry_interval: Option<Duration>,
    pub routing: Routing,
    pub create_stream: bool,
    pub create_topic: bool,
    pub partitions: u32,
    pub expiry: IggyExpiry,
    pub max_topic_size: iggy::prelude::MaxTopicSize,
    pub background: Option<BackgroundSettings>,
}

impl ProducerSettings {
    async fn build(self, topic: Topic) -> Result<Producer, LaserError> {
        let mut builder = topic
            .producer()
            .batch_length(self.batch_length)
            .linger(self.linger)
            .routing(self.routing)
            .create_stream(self.create_stream)
            .create_topic(self.create_topic)
            .partitions(self.partitions);
        builder = match self.max_topic_size {
            iggy::prelude::MaxTopicSize::Unlimited => builder.unlimited_topic_size(),
            iggy::prelude::MaxTopicSize::Custom(bytes) => {
                builder.max_topic_bytes(bytes.as_bytes_u64())
            }
            iggy::prelude::MaxTopicSize::ServerDefault => builder,
        };
        if let Some(retries) = self.retries {
            builder = builder.retries(Some(retries), self.retry_interval);
        } else if let Some(interval) = self.retry_interval {
            builder = builder.retry_backoff(interval);
        }
        let builder = match self.expiry {
            IggyExpiry::ServerDefault => builder,
            IggyExpiry::NeverExpire => builder.never_expire(),
            IggyExpiry::ExpireDuration(expiry) => builder.expire_after(expiry.get_duration()),
        };
        let builder = match self.background {
            Some(background) => builder.background(background.config()),
            None => builder,
        };
        builder.build().await
    }
}

/// Apache Iggy's buffered background send mode, passed as
/// `Topic.producer(background=BackgroundConfig(...))`. A send returns once the
/// records are queued. Unset fields take Apache Iggy's defaults: one ordered
/// shard, a flush at `batch_length` sends, `batch_size` bytes, or `linger_ms`,
/// a `max_buffer_size` byte budget, and `max_in_flight` writes at once.
/// `sharding="balanced"` spreads one topic over every shard and gives up
/// ordering. `failure_mode` is what a send does when the budget is full:
/// `"block"` waits, `"block_with_timeout"` waits up to `block_timeout_ms`, and
/// `"fail_immediately"` raises. `error_callback(error)`, a callable or an
/// object with `call(error)`, sync or async, receives each write that failed
/// after its retries as a `PublishFailedError` with `stream`, `topic`,
/// `committed`, `unconfirmed`, and the cause as `__cause__`. Without one, the
/// failure is logged on the `laser_sdk`
/// logger and its records are dropped.
#[gen_stub_pyclass]
#[pyclass(name = "BackgroundConfig", frozen)]
pub struct PyBackgroundConfig {
    settings: BackgroundSettings,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBackgroundConfig {
    #[new]
    #[pyo3(signature = (*, num_shards=None, sharding="ordered", batch_length=None, batch_size=None, linger_ms=None, max_buffer_size=None, max_in_flight=None, failure_mode="block", block_timeout_ms=None, error_callback=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        num_shards: Option<usize>,
        sharding: &str,
        batch_length: Option<usize>,
        batch_size: Option<usize>,
        linger_ms: Option<f64>,
        max_buffer_size: Option<u64>,
        max_in_flight: Option<usize>,
        failure_mode: &str,
        block_timeout_ms: Option<u64>,
        error_callback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let defaults = BackgroundSettings::default();
        let balanced = match sharding {
            "ordered" => false,
            "balanced" => true,
            _ => {
                return Err(InvalidError::new_err(
                    "sharding must be ordered or balanced",
                ));
            }
        };
        let failure_mode = match (failure_mode, block_timeout_ms) {
            ("block", None) => BackpressureMode::Block,
            ("block_with_timeout", Some(timeout)) => BackpressureMode::BlockWithTimeout(
                IggyDuration::from(Duration::from_millis(timeout)),
            ),
            ("fail_immediately", None) => BackpressureMode::FailImmediately,
            ("block_with_timeout", None) => {
                return Err(InvalidError::new_err(
                    "failure_mode='block_with_timeout' requires block_timeout_ms",
                ));
            }
            ("block" | "fail_immediately", Some(_)) => {
                return Err(InvalidError::new_err(
                    "block_timeout_ms applies to failure_mode='block_with_timeout' only",
                ));
            }
            _ => {
                return Err(InvalidError::new_err(
                    "failure_mode must be block, block_with_timeout, or fail_immediately",
                ));
            }
        };
        let error_callback = error_callback
            .map(|callback| {
                hook_callable(callback, "call", "an error callback")?;
                Ok::<_, PyErr>(Arc::new(callback.clone().unbind()))
            })
            .transpose()?;
        Ok(Self {
            settings: BackgroundSettings {
                num_shards: num_shards.unwrap_or(defaults.num_shards),
                balanced,
                batch_length: batch_length.unwrap_or(defaults.batch_length),
                batch_size: batch_size.unwrap_or(defaults.batch_size),
                linger: linger_ms
                    .map(|linger| {
                        crate::convert::duration_ms(linger, "linger_ms").map(IggyDuration::from)
                    })
                    .transpose()?
                    .unwrap_or(defaults.linger),
                max_buffer_size: max_buffer_size.unwrap_or(defaults.max_buffer_size),
                max_in_flight: max_in_flight.unwrap_or(defaults.max_in_flight),
                failure_mode,
                error_callback,
            },
        })
    }

    #[getter]
    fn num_shards(&self) -> usize {
        self.settings.num_shards
    }

    /// `ordered` or `balanced`.
    #[getter]
    fn sharding(&self) -> &'static str {
        if self.settings.balanced {
            "balanced"
        } else {
            "ordered"
        }
    }

    #[getter]
    fn batch_length(&self) -> usize {
        self.settings.batch_length
    }

    #[getter]
    fn batch_size(&self) -> usize {
        self.settings.batch_size
    }

    /// The flush delay in milliseconds, fractional below one.
    #[getter]
    fn linger_ms(&self) -> f64 {
        self.settings.linger.get_duration().as_secs_f64() * 1000.0
    }

    #[getter]
    fn max_buffer_size(&self) -> u64 {
        self.settings.max_buffer_size
    }

    #[getter]
    fn max_in_flight(&self) -> usize {
        self.settings.max_in_flight
    }

    /// `block`, `block_with_timeout`, or `fail_immediately`.
    #[getter]
    fn failure_mode(&self) -> &'static str {
        match self.settings.failure_mode {
            BackpressureMode::Block => "block",
            BackpressureMode::BlockWithTimeout(_) => "block_with_timeout",
            BackpressureMode::FailImmediately => "fail_immediately",
        }
    }

    #[getter]
    fn block_timeout_ms(&self) -> Option<u128> {
        match &self.settings.failure_mode {
            BackpressureMode::BlockWithTimeout(timeout) => Some(timeout.get_duration().as_millis()),
            _ => None,
        }
    }
}

/// The background mode a producer builds from: Apache Iggy's defaults, or a
/// `BackgroundConfig`.
#[derive(Clone)]
pub(crate) struct BackgroundSettings {
    num_shards: usize,
    balanced: bool,
    batch_length: usize,
    batch_size: usize,
    linger: IggyDuration,
    max_buffer_size: u64,
    max_in_flight: usize,
    failure_mode: BackpressureMode,
    error_callback: Option<Arc<Py<PyAny>>>,
}

impl Default for BackgroundSettings {
    fn default() -> Self {
        let defaults = BackgroundConfig::builder().build();
        Self {
            num_shards: defaults.num_shards,
            balanced: false,
            batch_length: defaults.batch_length,
            batch_size: defaults.batch_size,
            linger: defaults.linger_time,
            max_buffer_size: defaults.max_buffer_size.as_bytes_u64(),
            max_in_flight: defaults.max_in_flight,
            failure_mode: defaults.failure_mode,
            error_callback: None,
        }
    }
}

impl From<&PyBackgroundConfig> for BackgroundSettings {
    fn from(config: &PyBackgroundConfig) -> Self {
        config.settings.clone()
    }
}

impl BackgroundSettings {
    // Runs inside the awaited call that builds the producer, so the callback
    // falls back to that caller's loop on Iggy's error task.
    fn config(self) -> BackgroundConfig {
        let sharding: Box<dyn Sharding + Send + Sync> = if self.balanced {
            Box::new(BalancedSharding::default())
        } else {
            Box::new(OrderedSharding)
        };
        let callback = self.error_callback.and_then(|callback| {
            Python::attach(|py| PyHook::new(callback.bind(py), "call", "an error callback"))
                .inspect_err(|error| log::warn!("background error callback unusable: {error}"))
                .ok()
        });
        BackgroundConfig::builder()
            .num_shards(self.num_shards)
            .sharding(sharding)
            .batch_length(self.batch_length)
            .batch_size(self.batch_size)
            .linger_time(self.linger)
            .max_buffer_size(IggyByteSize::from(self.max_buffer_size))
            .max_in_flight(self.max_in_flight)
            .failure_mode(self.failure_mode)
            .error_callback(Arc::new(Box::new(PyErrorCallback(callback))))
            .build()
    }
}

// Hands a failed background write to the Python callback as the same typed
// exception a direct send raises, or logs it when there is no callback.
struct PyErrorCallback(Option<PyHook>);

impl std::fmt::Debug for PyErrorCallback {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PyErrorCallback")
    }
}

impl ErrorCallback for PyErrorCallback {
    fn call(&self, ctx: ErrorCtx) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let hook = self.0.clone();
        Box::pin(async move {
            let error = LaserError::Iggy(IggyError::ProducerSendFailed {
                cause: ctx.cause,
                failed: ctx.messages,
                committed: ctx.committed,
                stream_name: ctx.stream_name,
                topic_name: ctx.topic_name,
            });
            let Some(hook) = hook else {
                log::error!("background send failed, its records are dropped: {error}");
                return;
            };
            let raised = hook
                .call(|py| (to_pyerr(error).value(py).clone(),).into_pyobject(py))
                .await;
            if let Err(error) = raised {
                log::warn!("background error callback raised: {error}");
            }
        })
    }
}

/// A configurable Laser streaming producer. Build it with `Topic.producer`.
/// Every send runs the Laser publish recovery: each attempt is bounded by the
/// connection's publish timeout, a failed attempt reconnects and retries
/// `retries` times with the same message ids, and the final error names the
/// cause. Sends accept per-send key or partition overrides.
#[gen_stub_pyclass]
#[pyclass(name = "Producer")]
pub struct PyProducer {
    topic: Topic,
    settings: ProducerSettings,
    producer: Arc<RwLock<ProducerState>>,
    stream: String,
    name: String,
}

enum ProducerState {
    Pending,
    Ready(Box<Producer>),
    Closed,
}

impl PyProducer {
    pub(crate) fn new(topic: Topic, stream: String, settings: ProducerSettings) -> Self {
        Self {
            name: topic.name().to_owned(),
            topic,
            settings,
            producer: Arc::new(RwLock::new(ProducerState::Pending)),
            stream,
        }
    }

    // A send holds this read guard for its whole call, so `shutdown`, which
    // takes the write side, waits for every in-flight send and then owns the
    // only handle to the producer.
    fn producer(
        &self,
    ) -> impl Future<Output = PyResult<OwnedRwLockReadGuard<ProducerState, Producer>>> + Send + 'static
    {
        let cell = self.producer.clone();
        let topic = self.topic.clone();
        let settings = self.settings.clone();
        async move {
            loop {
                let state = cell.clone().read_owned().await;
                let state = match OwnedRwLockReadGuard::try_map(state, |state| match state {
                    ProducerState::Ready(producer) => Some(producer.as_ref()),
                    ProducerState::Pending | ProducerState::Closed => None,
                }) {
                    Ok(producer) => return Ok(producer),
                    Err(state) => state,
                };
                if matches!(*state, ProducerState::Closed) {
                    return Err(InvalidError::new_err(
                        "the producer is shut down, build a new one with Topic.producer",
                    ));
                }
                drop(state);
                let mut state = cell.write().await;
                if matches!(*state, ProducerState::Pending) {
                    let producer = settings
                        .clone()
                        .build(topic.clone())
                        .await
                        .map_err(to_pyerr)?;
                    *state = ProducerState::Ready(Box::new(producer));
                }
            }
        }
    }
}

// Waits for in-flight sends, then flushes and stops the producer on a task of
// its own, so cancelling the caller never drops buffered records. Later calls
// find it closed and return at once.
async fn close_producer(cell: Arc<RwLock<ProducerState>>) -> Result<(), LaserError> {
    let flush = tokio::spawn(async move {
        let mut state = cell.write_owned().await;
        match std::mem::replace(&mut *state, ProducerState::Closed) {
            ProducerState::Ready(producer) => producer.shutdown().await,
            ProducerState::Pending | ProducerState::Closed => Ok(()),
        }
    });
    flush
        .await
        .map_err(|error| LaserError::HandlerConfig(format!("producer shutdown: {error}")))?
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProducer {
    /// Initialize the producer, including configured stream/topic creation.
    /// `send` and `send_batch` also initialize lazily, so calling this is useful
    /// when startup should fail before accepting work.
    fn init<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let producer = self.producer();
        future_into_py(py, async move {
            producer.await?;
            Ok(())
        })
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
        let routing = routing_override(key, partition)?;
        let producer = self.producer();
        future_into_py(py, async move {
            let producer = producer.await?;
            let response = match routing {
                Some(routing) => producer.send_with_routing(message, routing).await,
                None => producer.send_message(message).await,
            };
            response.map(PySendMessagesResponse::from).map_err(to_pyerr)
        })
    }

    /// Send one Iggy batch. Each item is either a raw payload or
    /// `(payload, headers)`. All records share the optional key or partition,
    /// matching Iggy's `send_with_partitioning` contract.
    #[pyo3(signature = (messages, *, key=None, partition=None))]
    fn send_batch<'py>(
        &self,
        py: Python<'py>,
        messages: &Bound<'_, PyAny>,
        key: Option<&Bound<'_, PyAny>>,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = self::messages(messages)?;
        let routing = routing_override(key, partition)?;
        let producer = self.producer();
        future_into_py(py, async move {
            producer
                .await?
                .send_batch_with_routing(messages, routing)
                .await
                .map(PySendMessagesResponse::from)
                .map_err(to_pyerr)
        })
    }

    /// Flush buffered `background`-mode messages and stop the worker. It waits
    /// for sends already in flight, so every record a send accepted is
    /// flushed, and cancelling the call does not stop the flush. A direct
    /// producer has nothing to flush, so this only closes it. Later sends
    /// raise `InvalidError`, and a second shutdown returns at once.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.producer.clone();
        future_into_py(
            py,
            async move { close_producer(cell).await.map_err(to_pyerr) },
        )
    }

    fn __repr__(&self) -> String {
        format!("Producer(stream={}, topic={})", self.stream, self.name)
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
    message_id: u128,
    headers: BTreeMap<String, PyHeader>,
    user_headers: Option<bytes::Bytes>,
    #[pyo3(get)]
    pub checksum: u64,
    offset: u64,
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
    // The record as the Laser consumer delivered it, which a commit names.
    // Absent on a record read through another path.
    delivered: Option<ConsumerMessage>,
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
            message_id: message.header.id,
            headers,
            user_headers: message.user_headers.clone(),
            headers_malformed,
            checksum: message.header.checksum,
            offset: message.header.offset,
            current_offset,
            partition_id,
            timestamp_micros: message.header.timestamp,
            origin_timestamp_micros: message.header.origin_timestamp,
            delivered: None,
        })
    }

    /// A record a Laser consumer delivered, kept so a commit can name it.
    pub(crate) fn from_delivered(message: ConsumerMessage) -> Self {
        let (headers, headers_malformed) =
            received_headers(message.user_headers.as_deref().unwrap_or_default())
                .unwrap_or_else(|| (BTreeMap::new(), true));
        Self {
            payload: message.payload.clone(),
            message_id: message.message_id,
            headers,
            user_headers: message.user_headers.clone(),
            headers_malformed,
            checksum: message.checksum,
            offset: message.position.offset,
            current_offset: message.current_offset,
            partition_id: message.partition_id,
            timestamp_micros: message.timestamp_micros,
            origin_timestamp_micros: message.origin_timestamp_micros,
            delivered: Some(message),
        }
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

    /// Iggy's 128-bit message identifier.
    #[getter]
    fn message_id(&self) -> u128 {
        self.message_id
    }

    /// The record's log position (partition and offset).
    #[getter]
    fn position(&self) -> crate::ids::PyMessageId {
        laser_sdk::types::MessageId::new(self.partition_id, self.offset).into()
    }

    /// The user header block exactly as stored, for a caller that decodes
    /// its entries itself, or `None` when the record has none.
    #[getter]
    fn user_headers<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.user_headers
            .as_ref()
            .map(|block| PyBytes::new(py, block))
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
/// exposes manual offset storage for commit-after-handle delivery. A group
/// consumer on a server that resolves group policies runs the group's
/// filter, or none, without naming one, and commits through the group's
/// fenced acknowledgments. A purge restarts the partition at offset 0
/// without telling an open reader, which can keep its old position and skip
/// the replacement records, so rebuild it after a purge.
#[gen_stub_pyclass]
#[pyclass(name = "Consumer")]
pub struct PyConsumer {
    name: String,
    state: Arc<Mutex<ConsumerState>>,
    receiving: Arc<Mutex<()>>,
    changed: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    returned: Arc<std::sync::Mutex<VecDeque<ConsumerMessage>>>,
}

// The consumer is built on first use: joining a group and resolving its
// policy are round trips, and construction is synchronous.
enum ConsumerState {
    Unbuilt(Box<ConsumerBuilder>),
    Built(Box<Consumer>),
    Closed,
}

impl ConsumerState {
    async fn built(&mut self) -> PyResult<&mut Consumer> {
        if let Self::Unbuilt(builder) = self {
            let consumer = builder.as_ref().clone().build().await.map_err(to_pyerr)?;
            *self = Self::Built(Box::new(consumer));
        }
        match self {
            Self::Built(consumer) => Ok(consumer),
            Self::Unbuilt(_) | Self::Closed => {
                Err(InvalidError::new_err("consumer has been shut down"))
            }
        }
    }
}

impl PyConsumer {
    pub(crate) fn new(name: String, builder: ConsumerBuilder) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            name,
            state: Arc::new(Mutex::new(ConsumerState::Unbuilt(Box::new(builder)))),
            receiving: Arc::default(),
            changed: Arc::default(),
            shutdown,
            returned: Arc::default(),
        }
    }

    async fn receive(
        state: Arc<Mutex<ConsumerState>>,
        receiving: Arc<Mutex<()>>,
        changed: Arc<Notify>,
        shutdown: watch::Sender<bool>,
        returned: Arc<std::sync::Mutex<VecDeque<ConsumerMessage>>>,
    ) -> PyResult<Option<PyConsumerMessage>> {
        let mut shutdown_rx = shutdown.subscribe();
        if *shutdown_rx.borrow() {
            return Ok(None);
        }
        tokio::select! {
            received = async {
                let _receiving = receiving.lock().await;
                {
                    let mut state = state.lock().await;
                    if matches!(*state, ConsumerState::Closed) {
                        return Ok(None);
                    }
                    return_deliveries(state.built().await?, &returned).await?;
                }
                let mut locked = pin!(Arc::clone(&state).lock_owned());
                let message = loop {
                    let notified = changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    tokio::select! {
                        message = poll_fn(|context| {
                            let mut guard = ready!(locked.as_mut().poll(context));
                            locked.set(Arc::clone(&state).lock_owned());
                            match &mut *guard {
                                ConsumerState::Built(consumer) => pin!(consumer.next()).poll(context),
                                ConsumerState::Unbuilt(_) | ConsumerState::Closed => Poll::Ready(None),
                            }
                        }) => break message,
                        () = notified => {}
                    }
                };
                match message {
                    Some(Ok(message)) => Ok(Some(PyConsumerMessage::from_delivered(message))),
                    Some(Err(error)) => Err(to_pyerr(error)),
                    None => Ok(None),
                }
            } => received,
            result = shutdown_rx.changed() => {
                let _ = result;
                Ok(None)
            }
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumer {
    /// Initialize and join/create the configured group. Reads initialize lazily
    /// too, so call this when startup should fail before accepting work.
    fn init<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            state.lock().await.built().await?;
            Ok(())
        })
    }

    /// Wait for the next message. Returns `None` after shutdown.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let shutdown = self.shutdown.clone();
        future_into_py_returning(
            py,
            Self::receive(
                state,
                Arc::clone(&self.receiving),
                Arc::clone(&self.changed),
                shutdown,
                Arc::clone(&self.returned),
            ),
            give_back_delivery(Arc::clone(&self.returned)),
        )
    }

    /// Wait at most `wait_ms` milliseconds for the next message. Raises
    /// `TimeoutError` when none arrives in time and `InvalidError` when the
    /// consumer has shut down, matching the Rust `next_within`.
    fn next_within<'py>(&self, py: Python<'py>, wait_ms: f64) -> PyResult<Bound<'py, PyAny>> {
        let wait = crate::convert::duration_ms(wait_ms, "wait_ms")?;
        let receive = Self::receive(
            self.state.clone(),
            Arc::clone(&self.receiving),
            Arc::clone(&self.changed),
            self.shutdown.clone(),
            Arc::clone(&self.returned),
        );
        future_into_py_returning(
            py,
            async move {
                match tokio::time::timeout(wait, receive).await {
                    Ok(Ok(Some(message))) => Ok(message),
                    Ok(Ok(None)) => Err(to_pyerr(LaserError::Invalid(
                        "the live consumer stream ended".to_owned(),
                    ))),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(to_pyerr(LaserError::Timeout(
                        "the live consumer to yield a record",
                    ))),
                }
            },
            give_back_delivery(Arc::clone(&self.returned)),
        )
    }

    /// Store `offset` on the server for the message partition. With no
    /// `partition`, Iggy uses the consumer's current partition. A group
    /// consumer on a server that resolves group policies refuses it, because
    /// an arbitrary offset bypasses the group's acknowledgment contract.
    /// Commit a delivered message instead.
    #[pyo3(signature = (offset, *, partition=None))]
    fn store_offset<'py>(
        &self,
        py: Python<'py>,
        offset: u64,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            state
                .lock()
                .await
                .built()
                .await?
                .store_offset(offset, partition)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Store a successfully handled message's offset on the server. A group
    /// consumer on a server that resolves group policies stores the
    /// contiguous prefix of the partition through this message.
    fn commit<'py>(
        &self,
        py: Python<'py>,
        message: &PyConsumerMessage,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let changed = Arc::clone(&self.changed);
        let delivered = message.delivered.clone();
        let offset = message.offset;
        let partition = message.partition_id;
        future_into_py(py, async move {
            let mut state = state.lock().await;
            let consumer = state.built().await?;
            let result = match delivered {
                Some(message) => consumer.commit(&message).await,
                None => consumer.store_offset(offset, Some(partition)).await,
            }
            .map_err(to_pyerr);
            drop(state);
            changed.notify_one();
            result
        })
    }

    /// Delete the stored server offset for one partition or the consumer's
    /// current partition. Refused like `store_offset` by a policy-aware group
    /// consumer.
    #[pyo3(signature = (*, partition=None))]
    fn delete_offset<'py>(
        &self,
        py: Python<'py>,
        partition: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            state
                .lock()
                .await
                .built()
                .await?
                .delete_offset(partition)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Last message offset yielded locally for `partition`.
    fn last_consumed_offset<'py>(
        &self,
        py: Python<'py>,
        partition: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            Ok(match &*state.lock().await {
                ConsumerState::Built(consumer) => consumer.last_consumed_offset(partition),
                ConsumerState::Unbuilt(_) | ConsumerState::Closed => None,
            })
        })
    }

    /// Read the offset the server stores for this consumer, or for its group,
    /// on `partition`, with the partition's current head. `None` when the
    /// server stores nothing for it yet. A server read, unlike the local
    /// bookkeeping of `last_stored_offset`.
    fn stored_offset<'py>(&self, py: Python<'py>, partition: u32) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            let stored = state
                .lock()
                .await
                .built()
                .await?
                .stored_offset(partition)
                .await
                .map_err(to_pyerr)?;
            Ok(stored.map(PyStoredOffset::from))
        })
    }

    /// Local offset bookkeeping: the native SDK's stored offset, whose initial
    /// zero does not prove a durable checkpoint exists, or the last offset a
    /// policy-aware group consumer acknowledged. Use `next` polling for
    /// server-side resume.
    fn last_stored_offset<'py>(
        &self,
        py: Python<'py>,
        partition: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            Ok(match &*state.lock().await {
                ConsumerState::Built(consumer) => consumer.last_stored_offset(partition),
                ConsumerState::Unbuilt(_) | ConsumerState::Closed => None,
            })
        })
    }

    /// Stop polling and leave the group. Automatic policies store the handled
    /// prefix first. On the native path, Polling commits before delivery.
    /// On the group-aware path it commits the delivered prefix. Disabled
    /// auto-commit preserves the last explicit commit. Use explicit commits
    /// when shutdown must preserve only successfully processed records.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let already_shutdown = self.shutdown.send_replace(true);
        let state = self.state.clone();
        let returned = Arc::clone(&self.returned);
        future_into_py(py, async move {
            if already_shutdown {
                return Ok(());
            }
            let taken = std::mem::replace(&mut *state.lock().await, ConsumerState::Closed);
            match taken {
                ConsumerState::Built(mut consumer) => {
                    return_deliveries(&mut consumer, &returned).await?;
                    consumer.shutdown().await.map_err(to_pyerr)
                }
                ConsumerState::Unbuilt(_) | ConsumerState::Closed => Ok(()),
            }
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let shutdown = self.shutdown.clone();
        let returned = Arc::clone(&self.returned);
        let receiving = Arc::clone(&self.receiving);
        let changed = Arc::clone(&self.changed);
        future_into_py_returning(
            py,
            async move {
                match Self::receive(state, receiving, changed, shutdown, returned).await? {
                    Some(message) => Ok(message),
                    None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                }
            },
            give_back_delivery(Arc::clone(&self.returned)),
        )
    }

    fn __repr__(&self) -> String {
        format!("Consumer(name={})", self.name)
    }
}

/// A consumer's offset as the server stores it, read by
/// `Consumer.stored_offset`.
#[gen_stub_pyclass]
#[pyclass(name = "StoredOffset", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyStoredOffset {
    /// The offset the server stored for the consumer.
    pub stored_offset: u64,
    /// The partition's current head offset.
    pub current_offset: u64,
}

impl From<StoredOffset> for PyStoredOffset {
    fn from(offset: StoredOffset) -> Self {
        Self {
            stored_offset: offset.stored_offset,
            current_offset: offset.current_offset,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyStoredOffset {
    fn __repr__(&self) -> String {
        format!(
            "StoredOffset(stored_offset={}, current_offset={})",
            self.stored_offset, self.current_offset
        )
    }
}

fn give_back_delivery(returned: Arc<std::sync::Mutex<VecDeque<ConsumerMessage>>>) -> Undelivered {
    Box::new(move |py, value| {
        let Ok(message) = value.extract::<PyRef<'_, PyConsumerMessage>>(py) else {
            return;
        };
        if let Some(message) = &message.delivered {
            returned
                .lock()
                .expect("consumer returned-delivery lock")
                .push_back(message.clone());
        }
    })
}

async fn return_deliveries(
    consumer: &mut Consumer,
    returned: &std::sync::Mutex<VecDeque<ConsumerMessage>>,
) -> PyResult<()> {
    let messages = returned
        .lock()
        .expect("consumer returned-delivery lock")
        .drain(..)
        .collect::<Vec<_>>();
    for message in messages.into_iter().rev() {
        consumer.return_delivery(message).await.map_err(to_pyerr)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConsumerState, ProducerState, PyConsumer, PyConsumerMessage, PyHeaderValue, close_producer,
        positive_duration_ms,
    };
    use iggy::prelude::IggyMessage;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Mutex, RwLock, watch};

    #[tokio::test]
    async fn given_an_in_flight_send_when_shutting_down_then_should_wait_for_it_and_close_once() {
        let state = Arc::new(RwLock::new(ProducerState::Pending));
        let in_flight = Arc::clone(&state).read_owned().await;
        let closing = tokio::spawn(close_producer(Arc::clone(&state)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !closing.is_finished(),
            "shutdown ran past an in-flight send"
        );
        drop(in_flight);
        closing
            .await
            .expect("shutdown task")
            .expect("shutdown succeeds");
        assert!(matches!(*state.read().await, ProducerState::Closed));
        close_producer(state)
            .await
            .expect("a second shutdown returns at once");
    }

    #[tokio::test]
    async fn given_shutdown_while_waiting_for_consumer_state_when_signalled_then_should_cancel_the_wait()
     {
        let state = Arc::new(Mutex::new(ConsumerState::Closed));
        let guard = state.lock().await;
        let (shutdown, _) = watch::channel(false);
        let received = PyConsumer::receive(
            Arc::clone(&state),
            Arc::default(),
            Arc::default(),
            shutdown.clone(),
            Arc::default(),
        );
        tokio::pin!(received);
        tokio::select! {
            _ = &mut received => panic!("the state lock is still held"),
            () = tokio::task::yield_now() => {},
        }
        shutdown.send_replace(true);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut received)
            .await
            .expect("shutdown cancels the state wait")
            .expect("shutdown succeeds");
        assert!(result.is_none());
        drop(guard);
    }

    #[test]
    fn given_unknown_header_values_when_a_record_is_exposed_then_should_preserve_raw_and_known_entries()
     {
        let mut bytes = entry("future", 99, b"opaque");
        bytes.extend(entry("priority", 10, &7_u16.to_le_bytes()));
        let mut message = IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"payload"))
            .build()
            .expect("message");
        message.user_headers = Some(bytes.clone().into());
        let decoded = PyConsumerMessage::of(&message, 0, 0).expect("readable message");
        assert!(!decoded.headers_malformed);
        assert_eq!(decoded.user_headers.as_deref(), Some(bytes.as_slice()));
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
