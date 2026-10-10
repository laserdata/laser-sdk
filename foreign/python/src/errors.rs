use crate::convert::{py_to_de, ser_to_py};
use crate::transport::PySendMessagesConfirmation;
use iggy::prelude::{HeaderKind, HeaderValue, IggyError, IggyMessage};
use laser_sdk::LaserError as SdkError;
use pyo3::IntoPyObjectExt;
use pyo3::conversion::FromPyObjectOwned;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyPermissionError, PyTimeoutError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::OnceLockExt;
use pyo3::types::{PyBytes, PyDict, PyTuple, PyType};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};

// The classifiers every instance carries next to `code` and `retryable`. Each
// mirrors the Rust `LaserError::is_<name>` method of the same name.
const FLAGS: [&str; 15] = [
    "unavailable",
    "unsupported",
    "not_found",
    "version_skew",
    "version_conflict",
    "ambiguous_mutation",
    "stale",
    "permission_denied",
    "stream_or_topic_not_found",
    "no_capable_agent",
    "lease_lost",
    "fence_violation",
    "budget_exceeded",
    "quarantined",
    "not_leader",
];
const DETAIL: (&str, &str) = ("detail", "typing.Any");
// A class constant naming one variant of a Rust error enum. Its value is the
// lowercase constant name, the word a raised instance carries in `kind`.
const VARIANT: &str = "typing.ClassVar[builtins.str]";
const INTENT_FIELDS: [(&str, &str); 10] = [
    ("kind", "builtins.str"),
    ("NO_ELIGIBLE_VOTERS", VARIANT),
    ("DUPLICATE_ELIGIBLE_VOTER", VARIANT),
    ("DUPLICATE_MANDATORY_VOTER", VARIANT),
    ("MANDATORY_VOTER_NOT_ELIGIBLE", VARIANT),
    ("INVALID_THRESHOLD", VARIANT),
    ("INVALID_DEADLINE", VARIANT),
    ("DIGEST_MISMATCH", VARIANT),
    ("INELIGIBLE_VOTER", VARIANT),
    ("DECISION_INTENT_MISMATCH", VARIANT),
];
const VALIDATE_FIELDS: [(&str, &str); 6] = [
    ("kind", "builtins.str"),
    ("field", "builtins.str"),
    ("MISSING", VARIANT),
    ("FORBIDDEN", VARIANT),
    ("TOO_LARGE", VARIANT),
    ("INVALID", VARIANT),
];
const FILTER_FIELDS: [(&str, &str); 8] = [
    ("reason", "builtins.str"),
    ("fault_reason", "builtins.str | None"),
    ("partition_id", "builtins.int | None"),
    ("offset", "builtins.int | None"),
    ("group_id", "builtins.int | None"),
    ("group_name", "builtins.str | None"),
    (
        "identity",
        "builtins.dict[builtins.str, builtins.int] | None",
    ),
    DETAIL,
];

// The variant words an `IdError` and a `ProvenanceError` carry as `kind`,
// each also a class constant so a handler can compare without a literal.
const ID_KINDS: [(&str, &str); 5] = [
    ("EMPTY", "empty"),
    ("TOO_LONG", "too_long"),
    ("INVALID_CHAR", "invalid_char"),
    ("INVALID_ULID", "invalid_ulid"),
    ("INVALID_MESSAGE_ID", "invalid_message_id"),
];
const ID_FIELDS: [(&str, &str); 6] = [
    ("kind", "builtins.str | None"),
    ("EMPTY", "typing.ClassVar[builtins.str]"),
    ("TOO_LONG", "typing.ClassVar[builtins.str]"),
    ("INVALID_CHAR", "typing.ClassVar[builtins.str]"),
    ("INVALID_ULID", "typing.ClassVar[builtins.str]"),
    ("INVALID_MESSAGE_ID", "typing.ClassVar[builtins.str]"),
];
const PROVENANCE_KINDS: [(&str, &str); 10] = [
    ("MISSING_REQUIRED", "missing_required"),
    ("TOO_LARGE", "too_large"),
    ("INVALID_VALUE", "invalid_value"),
    ("INVALID_VALUE_BYTES", "invalid_value_bytes"),
    ("NON_FINITE", "non_finite"),
    ("EMPTY_VALUE", "empty_value"),
    ("VALUE_TOO_LONG", "value_too_long"),
    ("MALFORMED_HEADERS", "malformed_headers"),
    ("HEADER", "header"),
    ("ID", "id"),
];
const PROVENANCE_FIELDS: [(&str, &str); 11] = [
    ("kind", "builtins.str | None"),
    ("MISSING_REQUIRED", "typing.ClassVar[builtins.str]"),
    ("TOO_LARGE", "typing.ClassVar[builtins.str]"),
    ("INVALID_VALUE", "typing.ClassVar[builtins.str]"),
    ("INVALID_VALUE_BYTES", "typing.ClassVar[builtins.str]"),
    ("NON_FINITE", "typing.ClassVar[builtins.str]"),
    ("EMPTY_VALUE", "typing.ClassVar[builtins.str]"),
    ("VALUE_TOO_LONG", "typing.ClassVar[builtins.str]"),
    ("MALFORMED_HEADERS", "typing.ClassVar[builtins.str]"),
    ("HEADER", "typing.ClassVar[builtins.str]"),
    ("ID", "typing.ClassVar[builtins.str]"),
];

create_exception!(
    laser_sdk,
    LaserError,
    PyException,
    "Base class for every laser-sdk error. Catch it to handle any SDK failure. `code` is \
     the failure's result code and `retryable` says whether the same call can succeed \
     later. The boolean classifiers answer the common questions without matching the \
     class. Each class carries defaults for all of them, so an exception raised directly \
     by application code is classified too."
);
create_exception!(
    laser_sdk,
    ConfigError,
    LaserError,
    "Invalid client configuration, or a convenience call needed a default stream and none was set."
);
create_exception!(
    laser_sdk,
    NoStreamError,
    ConfigError,
    "A convenience call needed a default stream and none was set. Connect with \
     `stream=` or call `with_default_stream`."
);
create_exception!(
    laser_sdk,
    NoRespondTopicError,
    ConfigError,
    "An agent reply was attempted without a configured respond topic."
);
create_exception!(
    laser_sdk,
    HandlerConfigError,
    ConfigError,
    "A deterministic handler wiring, startup, or configuration failure. Retrying cannot \
     change the outcome, so it is never retryable."
);
create_exception!(
    laser_sdk,
    HandlerError,
    LaserError,
    "A transient handler failure. The reliable consumer retries it under its retry policy."
);
create_exception!(
    laser_sdk,
    RejectedError,
    LaserError,
    "A handler rejected the message permanently. The reliable consumer dead-letters it \
     without retrying."
);
create_exception!(
    laser_sdk,
    AmbiguousMutationError,
    LaserError,
    "A managed mutation's outcome is unknown: the server may or may not have applied it. \
     Not generically retryable, follow the operation's own recovery."
);
create_exception!(
    laser_sdk,
    StateStoreError,
    LaserError,
    "The state store failed to read or write."
);
create_exception!(
    laser_sdk,
    QueryError,
    LaserError,
    "The managed query / projection surface returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    KvError,
    LaserError,
    "The managed key-value store returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    ForkError,
    LaserError,
    "A fork operation returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    GraphError,
    LaserError,
    "The managed knowledge-graph surface returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    SessionError,
    LaserError,
    "A session read returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    AuthzError,
    LaserError,
    "The authorization surface returned a typed failure (unknown or invalid role, \
     unauthorized caller, or a lost bind revision race). `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    CheckpointError,
    LaserError,
    "A destination or checkpoint operation returned a typed failure. `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    FilterError,
    LaserError,
    "A consumer-filter operation returned a typed failure. `reason` names the cause \
     (`conflict`, `source_changed`, `not_found`, ...) and `detail` is the typed cause."
);
create_exception!(
    laser_sdk,
    FilterFaultError,
    FilterError,
    "A filtered read stopped in front of a record the filter could not evaluate. `reason` \
     is `fault`, `fault_reason` names the decode fault, and `partition_id` and `offset` \
     locate the record. The record and everything after it stay unacknowledged."
);
create_exception!(
    laser_sdk,
    FilterOversizedRecordError,
    FilterError,
    "A matching record alone exceeds the filtered reply cap. `reason` is \
     `oversized_record`, and `partition_id` and `offset` locate the record."
);
create_exception!(
    laser_sdk,
    ConsumerGroupSetupError,
    FilterError,
    "The consumer group exists but its filter was not configured. `group_id`, \
     `group_name`, and `identity` name the group, `reason` is the catalog's refusal or \
     `setup_failed`, and `__cause__` is the original failure."
);
create_exception!(
    laser_sdk,
    SignatureError,
    LaserError,
    "Envelope signing or verification failed, or a signer was not enrolled."
);
create_exception!(
    laser_sdk,
    UnsupportedError,
    LaserError,
    "The connected infrastructure does not provide the requested feature (Apache Iggy \
     without LaserData Cloud). `surface` names the refused accessor and `feature` the \
     sub-capability, when there is one."
);
create_exception!(
    laser_sdk,
    InvalidError,
    LaserError,
    "Client-side validation rejected the input before any round-trip. Also a \
     `ValueError`: a rejected argument is Python's value error, so stdlib-style \
     `except ValueError` catches it too."
);
create_exception!(
    laser_sdk,
    IntentError,
    InvalidError,
    "An invalid durable-intent configuration or operation. `kind` names the \
     variant, one of the class constants such as `IntentError.DIGEST_MISMATCH`."
);
create_exception!(
    laser_sdk,
    ValidateError,
    InvalidError,
    "An AGDX envelope broke the per-kind validity matrix or a size cap. `kind` names \
     the variant, one of the class constants such as `ValidateError.MISSING`, and \
     `field` the offending envelope field."
);
create_exception!(
    laser_sdk,
    IdError,
    InvalidError,
    "An identifier failed to parse or validate."
);
create_exception!(
    laser_sdk,
    CodecError,
    LaserError,
    "A payload or wire envelope encode/decode failed."
);
create_exception!(
    laser_sdk,
    TypedDecodeError,
    CodecError,
    "A typed read could not produce a value. `position` names the record's log position and `source` the underlying failure, also the cause."
);
create_exception!(
    laser_sdk,
    ProtocolError,
    LaserError,
    "The reply decoded but carried an unexpected shape (client/server skew)."
);
create_exception!(
    laser_sdk,
    TransportError,
    LaserError,
    "The underlying Apache Iggy transport failed."
);
create_exception!(
    laser_sdk,
    PublishFailedError,
    LaserError,
    "A publish that gave up. `committed` lists the ranges confirmed before the failure, \
     never replay them. `unconfirmed` lists the records left without a confirmation, \
     with the message ids the attempts used, and some may already be on the server. \
     `__cause__` is the original failure, and every classifier answers for it."
);
create_exception!(
    laser_sdk,
    IntegrityError,
    LaserError,
    "A claim-checked body failed its digest check. `reference` names the capsule."
);
create_exception!(
    laser_sdk,
    PolicyBlockedError,
    LaserError,
    "The enrolled action governor rejected the action before the effect ran. Not retryable."
);
create_exception!(
    laser_sdk,
    StepUpRequiredError,
    LaserError,
    "The enrolled action governor paused the action on a stronger approval. \
     `scope` names the scope the approval must grant."
);
create_exception!(
    laser_sdk,
    PolicyDeferredError,
    LaserError,
    "The enrolled action governor held the action for later execution. Retryable."
);
create_exception!(
    laser_sdk,
    RoutingError,
    LaserError,
    "Capability routing could not address an agent."
);
create_exception!(
    laser_sdk,
    NoCapableAgentError,
    RoutingError,
    "No live agent advertises `skill`. Retry after the registry refreshes, or route elsewhere."
);
create_exception!(
    laser_sdk,
    NoInboxError,
    RoutingError,
    "The capable `agent` advertises no inbox topic in its live presence."
);
create_exception!(
    laser_sdk,
    RoutePrincipalMismatchError,
    RoutingError,
    "The selected `agent` is not bound to the `expected` principal. `actual` is the bound \
     principal, or None."
);
create_exception!(
    laser_sdk,
    PresenceConflictError,
    LaserError,
    "The connection already advertises the `advertised` agent and cannot advertise `requested`."
);
create_exception!(
    laser_sdk,
    FenceViolationError,
    LaserError,
    "A fenced write lost the fence: the `held` token is below the `current` sequence, so a \
     newer holder owns the task. Never retryable by the loser."
);
create_exception!(
    laser_sdk,
    BudgetExceededError,
    LaserError,
    "A spend ceiling was reached: `spent` of `ceiling`. Not retryable until the ceiling is raised."
);
create_exception!(
    laser_sdk,
    QuarantinedError,
    LaserError,
    "The `agent` was quarantined and may not act. Not retryable."
);
create_exception!(
    laser_sdk,
    ProvenanceError,
    LaserError,
    "Provenance headers failed to encode or decode."
);
// `TimeoutError` and `CancelledError` are synthesized with two bases each so the
// SDK hierarchy stays catchable (`except LaserError`) while `except TimeoutError`
// (the builtin) and `except asyncio.CancelledError` also catch them. The macro
// takes a single base, so these are built with the `type(...)` builtin at
// registration and cached for `to_pyerr`.
static TIMEOUT_ERROR: OnceLock<Py<PyType>> = OnceLock::new();
static CANCELLED_ERROR: OnceLock<Py<PyType>> = OnceLock::new();

/// One record a failed publish left without a confirmation. It keeps the
/// message id the attempts used, and it may already be on the server.
#[gen_stub_pyclass]
#[pyclass(name = "IggyMessage", frozen, skip_from_py_object)]
pub struct PyUnconfirmedMessage {
    message_id: u128,
    payload: bytes::Bytes,
    headers: Vec<(String, HeaderValue)>,
}

impl From<&IggyMessage> for PyUnconfirmedMessage {
    fn from(message: &IggyMessage) -> Self {
        let headers = message
            .user_headers_map()
            .ok()
            .flatten()
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key.to_string_value(), value))
            .collect();
        Self {
            message_id: message.header.id,
            payload: message.payload.clone(),
            headers,
        }
    }
}

impl PyUnconfirmedMessage {
    /// The same record with the message id the failed attempts used, so a
    /// resend through `Topic.batch` stays deduplicated on the server.
    pub(crate) fn to_iggy_message(&self) -> PyResult<IggyMessage> {
        let headers = self
            .headers
            .iter()
            .map(|(key, value)| {
                let key = iggy::prelude::HeaderKey::try_from(key.as_str())
                    .map_err(|error| to_pyerr(error.into()))?;
                Ok((key, value.clone()))
            })
            .collect::<PyResult<std::collections::BTreeMap<_, _>>>()?;
        IggyMessage::builder()
            .id(self.message_id)
            .payload(self.payload.clone())
            .user_headers(headers)
            .build()
            .map_err(|error| to_pyerr(error.into()))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyUnconfirmedMessage {
    /// The 128-bit message id the publish attempts used.
    #[getter]
    fn message_id(&self) -> u128 {
        self.message_id
    }

    /// Raw payload bytes.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    /// Typed Iggy user headers as Python `bytes`, `str`, `bool`, `int`, or `float` values.
    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let values = PyDict::new(py);
        for (key, value) in &self.headers {
            values.set_item(key, header_object(py, value)?.1)?;
        }
        Ok(values.unbind())
    }

    /// Exact Iggy value kind for each user header (`uint16`, `string`, etc.).
    #[getter]
    fn header_kinds(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let kinds = PyDict::new(py);
        for (key, value) in &self.headers {
            kinds.set_item(key, header_object(py, value)?.0)?;
        }
        Ok(kinds.unbind())
    }

    fn __repr__(&self) -> String {
        format!(
            "IggyMessage(message_id={}, bytes={})",
            self.message_id,
            self.payload.len()
        )
    }
}

#[pyclass]
struct NativeError {
    inner: Mutex<Option<SdkError>>,
}

// One exception class: the classification it defaults to when application
// code raises it directly, and the attributes its SDK-raised instances carry.
struct ExceptionClass<'py> {
    class: Bound<'py, PyType>,
    code: Option<&'static str>,
    retryable: Option<bool>,
    flags: &'static [&'static str],
    fields: &'static [(&'static str, &'static str)],
}

// Map a Python exception a callback raised back onto the SDK error, keeping
// an SDK-raised error intact and the class and fields of one raised by hand.
pub(crate) fn from_callback_error(error: PyErr) -> SdkError {
    Python::attach(|py| {
        let value = error.value(py);
        if let Ok(native) = value.getattr("_native_error").and_then(|value| {
            value
                .extract::<PyRef<'_, NativeError>>()
                .map_err(Into::into)
        }) && let Some(error) = native.inner.lock().expect("native error lock").take()
        {
            return error;
        }
        let is = |class: Bound<'_, PyType>| value.is_instance(&class).unwrap_or(false);
        let text = |name: &str| attribute::<String>(value, name);
        let number = |name: &str| attribute::<u64>(value, name).unwrap_or_default();
        let message = error.to_string();
        if let Some(error) = typed_detail(py, value) {
            return error;
        }
        if is(py.get_type::<AmbiguousMutationError>())
            || attribute::<bool>(value, "ambiguous_mutation") == Some(true)
        {
            SdkError::AmbiguousMutation(message)
        } else if is(py.get_type::<RoutePrincipalMismatchError>()) {
            SdkError::RoutePrincipalMismatch {
                agent: text("agent").unwrap_or(message),
                expected: attribute::<u32>(value, "expected").unwrap_or_default(),
                actual: attribute::<Option<u32>>(value, "actual").flatten(),
            }
        } else if is(py.get_type::<NoCapableAgentError>()) {
            SdkError::NoCapableAgent {
                skill: text("skill").unwrap_or(message),
            }
        } else if is(py.get_type::<NoInboxError>()) {
            SdkError::NoInbox {
                agent: text("agent").unwrap_or(message),
            }
        } else if is(py.get_type::<PresenceConflictError>()) {
            SdkError::PresenceConflict {
                advertised: text("advertised").unwrap_or_default(),
                requested: text("requested").unwrap_or(message),
            }
        } else if is(py.get_type::<FenceViolationError>()) {
            SdkError::FenceViolation {
                stale: number("held"),
                current: number("current"),
            }
        } else if is(py.get_type::<BudgetExceededError>()) {
            SdkError::BudgetExceeded {
                ceiling: number("ceiling"),
                spent: number("spent"),
            }
        } else if is(py.get_type::<QuarantinedError>()) {
            SdkError::Quarantined {
                agent: text("agent").unwrap_or(message),
            }
        } else if error.is_instance_of::<pyo3::exceptions::asyncio::CancelledError>(py) {
            // Cancellation is never a transient failure, whether the SDK
            // observed a run's cancel intent or the callback's task was cancelled.
            SdkError::Cancelled {
                run: text("run").unwrap_or(message),
            }
        } else if is(py.get_type::<IntegrityError>()) {
            SdkError::Integrity {
                reference: text("reference").unwrap_or(message),
            }
        } else if is(py.get_type::<StepUpRequiredError>()) {
            SdkError::StepUpRequired {
                scope: text("scope").unwrap_or(message),
            }
        } else if is(py.get_type::<PolicyBlockedError>()) {
            SdkError::PolicyBlocked(message)
        } else if is(py.get_type::<PolicyDeferredError>()) {
            SdkError::PolicyDeferred(message)
        } else if is(py.get_type::<SignatureError>()) {
            SdkError::Signature(message)
        } else if is(py.get_type::<StateStoreError>()) {
            SdkError::StateStore(message)
        } else if is(py.get_type::<RejectedError>()) {
            SdkError::Rejected(message)
        } else if is(py.get_type::<HandlerError>()) {
            SdkError::Handler(message)
        } else if attribute::<bool>(value, "permission_denied") == Some(true)
            || error.is_instance_of::<PyPermissionError>(py)
        {
            SdkError::Iggy(IggyError::Unauthorized)
        } else if is(py.get_type::<NoStreamError>()) {
            SdkError::NoStream
        } else if is(py.get_type::<NoRespondTopicError>()) {
            SdkError::NoRespondTopic
        } else if is(py.get_type::<ConfigError>()) || error.is_instance_of::<PyTypeError>(py) {
            SdkError::HandlerConfig(message)
        } else if is(py.get_type::<UnsupportedError>())
            || attribute::<bool>(value, "unsupported") == Some(true)
        {
            SdkError::unsupported("handler", message)
        } else if is(py.get_type::<InvalidError>()) {
            SdkError::Invalid(message)
        } else if is(py.get_type::<CodecError>()) {
            SdkError::Codec(message)
        } else if is(py.get_type::<ProtocolError>()) {
            SdkError::Protocol(message)
        } else if error.is_instance_of::<PyTimeoutError>(py) {
            SdkError::Timeout("a callback")
        } else if attribute::<bool>(value, "retryable") == Some(false) {
            SdkError::Rejected(message)
        } else {
            SdkError::Handler(message)
        }
    })
}

pub(crate) fn to_pyerr(err: SdkError) -> PyErr {
    let pyerr = to_pyerr_ref(&err);
    Python::attach(|py| {
        if let Ok(native) = Py::new(
            py,
            NativeError {
                inner: Mutex::new(Some(err)),
            },
        ) {
            let _ = pyerr.value(py).setattr("_native_error", native);
        }
    });
    pyerr
}

// Observer hooks receive the same exception class and fields without taking the native result.
pub(crate) fn to_pyerr_ref(err: &SdkError) -> PyErr {
    Python::attach(|py| {
        let pyerr = PyErr::from_type(class_of(py, err), message_of(err));
        let value = pyerr.value(py);
        let flags = [
            err.is_unavailable(),
            err.is_unsupported(),
            err.is_not_found(),
            err.is_version_skew(),
            err.is_version_conflict(),
            err.is_ambiguous_mutation(),
            err.is_stale(),
            err.is_permission_denied(),
            err.is_stream_or_topic_not_found(),
            err.is_no_capable_agent(),
            err.is_lease_lost(),
            err.is_fence_violation(),
            err.is_budget_exceeded(),
            err.is_quarantined(),
            err.is_not_leader(),
        ];
        let _ = value.setattr("code", format!("{:?}", err.code()));
        let _ = value.setattr("retryable", err.is_retryable());
        let _ = value.setattr("iggy_error_code", err.iggy_error_code());
        let _ = value.setattr(
            "filter_reason",
            err.filter_reason().map(|reason| reason.to_string()),
        );
        for (name, flag) in FLAGS.iter().zip(flags) {
            let _ = value.setattr(*name, flag);
        }
        let _ = set_fields(py, &pyerr, err);
        pyerr
    })
}

// `IntentError` for one Rust intent error, its variant word in `kind`.
pub(crate) fn intent_error(error: &laser_sdk::intent::IntentError) -> PyErr {
    use laser_sdk::intent::IntentError as Variant;
    let kind = match error {
        Variant::NoEligibleVoters => "no_eligible_voters",
        Variant::DuplicateEligibleVoter(_) => "duplicate_eligible_voter",
        Variant::DuplicateMandatoryVoter(_) => "duplicate_mandatory_voter",
        Variant::MandatoryVoterNotEligible(_) => "mandatory_voter_not_eligible",
        Variant::InvalidThreshold { .. } => "invalid_threshold",
        Variant::InvalidDeadline { .. } => "invalid_deadline",
        Variant::DigestMismatch => "digest_mismatch",
        Variant::IneligibleVoter(_) => "ineligible_voter",
        Variant::DecisionIntentMismatch => "decision_intent_mismatch",
    };
    Python::attach(|py| {
        let pyerr = IntentError::new_err(error.to_string());
        let _ = pyerr.value(py).setattr("kind", kind);
        pyerr
    })
}

// `ValidateError` for one envelope validity violation, its variant word in
// `kind` and the offending envelope field in `field`.
pub(crate) fn validate_error(error: &laser_sdk::wire::agent::ValidateError) -> PyErr {
    use laser_sdk::wire::agent::ValidateError as Variant;
    let (kind, field) = match error {
        Variant::Missing { field, .. } => ("missing", *field),
        Variant::Forbidden { field, .. } => ("forbidden", *field),
        Variant::TooLarge { field, .. } => ("too_large", *field),
        Variant::Invalid { field, .. } => ("invalid", *field),
        _ => ("invalid", ""),
    };
    Python::attach(|py| {
        let pyerr = ValidateError::new_err(error.to_string());
        let value = pyerr.value(py);
        let _ = value.setattr("kind", kind);
        let _ = value.setattr("field", field);
        pyerr
    })
}

/// The exception block of the generated stub, rendered from the registered
/// classes: their names, bases, and docs, and the attributes they carry.
pub fn exception_stub(py: Python<'_>) -> PyResult<String> {
    let module = PyModule::new(py, "laser_sdk")?;
    register(py, &module)?;
    let mut stub = String::from("\nimport asyncio\n");
    for entry in exception_classes(py) {
        let class = &entry.class;
        let mut bases = Vec::new();
        for base in class.getattr("__bases__")?.cast_into::<PyTuple>()?.iter() {
            let module: String = base.getattr("__module__")?.extract()?;
            let name: String = base.getattr("__name__")?.extract()?;
            bases.push(match module.as_str() {
                "laser_sdk" => name,
                module if module.starts_with("asyncio") => format!("asyncio.{name}"),
                module => format!("{module}.{name}"),
            });
        }
        let name: String = class.getattr("__name__")?.extract()?;
        let _ = write!(stub, "\nclass {name}({}):\n", bases.join(", "));
        if let Ok(doc) = class.getattr("__doc__")?.extract::<String>() {
            let _ = write!(stub, "    r\"\"\"\n    {doc}\n    \"\"\"\n");
        }
        for (field, kind) in entry.fields {
            let _ = writeln!(stub, "    {field}: {kind}");
        }
        if class.is(py.get_type::<FilterError>()) {
            stub.push_str(FILTER_ERROR_METHODS);
        }
    }
    Ok(stub)
}

// The Rust `FilterError` constructors, attached to the exception class that
// stands for it in `register`.
const FILTER_ERROR_METHODS: &str = r#"    @staticmethod
    def invalid(error: InvalidError) -> FilterError:
        r"""
        The `FilterError` an `InvalidError` becomes, with reason `invalid_request`. Returns the exception without raising it.
        """
    @staticmethod
    def check_version(v: builtins.int) -> None:
        r"""
        Raise `FilterError` with reason `version_skew` when `v` is not the filter op version this build speaks.
        """
"#;

// Register the exception types on the module so `from laser_sdk import LaserError`
// resolves and isinstance checks against the hierarchy work.
pub(crate) fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Graft `ValueError` in as a second base (the macro takes one base only):
    // a rejected argument is Python's value error, and the SDK hierarchy must
    // keep catching it as a `LaserError`. Same dual-base intent as the
    // synthesized `TimeoutError`/`CancelledError`, applied to the macro class.
    let invalid = py.get_type::<InvalidError>();
    if !invalid.is_subclass_of::<pyo3::exceptions::PyValueError>()? {
        let bases = PyTuple::new(
            py,
            [
                py.get_type::<LaserError>().as_any(),
                py.get_type::<pyo3::exceptions::PyValueError>().as_any(),
            ],
        )?;
        invalid.setattr("__bases__", bases)?;
    }
    for entry in exception_classes(py) {
        let class = &entry.class;
        if let Some(code) = entry.code {
            class.setattr("code", code)?;
        }
        if let Some(retryable) = entry.retryable {
            class.setattr("retryable", retryable)?;
        }
        if class.is(py.get_type::<LaserError>()) {
            for flag in FLAGS {
                class.setattr(flag, false)?;
            }
            class.setattr("iggy_error_code", py.None())?;
            class.setattr("filter_reason", py.None())?;
        }
        if class.is(py.get_type::<IdError>()) {
            class.setattr("kind", py.None())?;
            for (name, word) in ID_KINDS {
                class.setattr(name, word)?;
            }
        }
        if class.is(py.get_type::<ProvenanceError>()) {
            class.setattr("kind", py.None())?;
            for (name, word) in PROVENANCE_KINDS {
                class.setattr(name, word)?;
            }
        }
        // A builtin function is not a descriptor, so it reads as a static
        // method through the class and its instances.
        if class.is(py.get_type::<FilterError>()) {
            class.setattr(
                "invalid",
                wrap_pyfunction!(crate::filters::filter_error_invalid, module)?,
            )?;
            class.setattr(
                "check_version",
                wrap_pyfunction!(crate::filters::filter_error_check_version, module)?,
            )?;
        }
        for flag in entry.flags {
            class.setattr(*flag, true)?;
        }
        for (field, kind) in entry.fields {
            if *kind == VARIANT {
                class.setattr(*field, field.to_lowercase())?;
            }
        }
        module.add(class.getattr("__name__")?.extract::<String>()?, class)?;
    }
    module.add_class::<PyUnconfirmedMessage>()?;
    Ok(())
}

// Every class in registration order, parents before children. `code` and
// `retryable` follow what Rust `LaserError::code` and `is_retryable` answer
// for the variant the class stands for.
fn exception_classes(py: Python<'_>) -> Vec<ExceptionClass<'_>> {
    fn class<'py>(
        class: Bound<'py, PyType>,
        code: Option<&'static str>,
        retryable: Option<bool>,
        flags: &'static [&'static str],
        fields: &'static [(&'static str, &'static str)],
    ) -> ExceptionClass<'py> {
        ExceptionClass {
            class,
            code,
            retryable,
            flags,
            fields,
        }
    }
    const BASE_FIELDS: [(&str, &str); 19] = [
        ("code", "builtins.str"),
        ("retryable", "builtins.bool"),
        ("iggy_error_code", "builtins.int | None"),
        ("filter_reason", "builtins.str | None"),
        ("unavailable", "builtins.bool"),
        ("unsupported", "builtins.bool"),
        ("not_found", "builtins.bool"),
        ("version_skew", "builtins.bool"),
        ("version_conflict", "builtins.bool"),
        ("ambiguous_mutation", "builtins.bool"),
        ("stale", "builtins.bool"),
        ("permission_denied", "builtins.bool"),
        ("stream_or_topic_not_found", "builtins.bool"),
        ("no_capable_agent", "builtins.bool"),
        ("lease_lost", "builtins.bool"),
        ("fence_violation", "builtins.bool"),
        ("budget_exceeded", "builtins.bool"),
        ("quarantined", "builtins.bool"),
        ("not_leader", "builtins.bool"),
    ];
    let invalid = Some("InvalidArgument");
    vec![
        class(
            py.get_type::<LaserError>(),
            Some("Backend"),
            Some(false),
            &[],
            &BASE_FIELDS,
        ),
        class(py.get_type::<ConfigError>(), invalid, None, &[], &[]),
        class(
            py.get_type::<NoStreamError>(),
            Some("Unsupported"),
            None,
            &[],
            &[],
        ),
        class(
            py.get_type::<NoRespondTopicError>(),
            Some("Unsupported"),
            None,
            &[],
            &[],
        ),
        class(py.get_type::<HandlerConfigError>(), None, None, &[], &[]),
        class(py.get_type::<HandlerError>(), None, Some(true), &[], &[]),
        class(py.get_type::<RejectedError>(), invalid, None, &[], &[]),
        class(timeout_error(py).clone(), None, Some(true), &[], &[]),
        class(
            py.get_type::<AmbiguousMutationError>(),
            None,
            None,
            &["ambiguous_mutation"],
            &[],
        ),
        class(py.get_type::<StateStoreError>(), None, None, &[], &[]),
        class(py.get_type::<QueryError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<KvError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<ForkError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<GraphError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<SessionError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<AuthzError>(), None, None, &[], &[DETAIL]),
        class(py.get_type::<CheckpointError>(), None, None, &[], &[DETAIL]),
        class(
            py.get_type::<FilterError>(),
            None,
            None,
            &[],
            &FILTER_FIELDS,
        ),
        class(py.get_type::<FilterFaultError>(), invalid, None, &[], &[]),
        class(
            py.get_type::<FilterOversizedRecordError>(),
            Some("TooLarge"),
            None,
            &[],
            &[],
        ),
        class(
            py.get_type::<ConsumerGroupSetupError>(),
            None,
            None,
            &[],
            &[],
        ),
        class(
            py.get_type::<SignatureError>(),
            Some("Unauthenticated"),
            None,
            &[],
            &[],
        ),
        class(
            py.get_type::<UnsupportedError>(),
            Some("Unsupported"),
            None,
            &["unsupported"],
            &[
                ("surface", "builtins.str"),
                ("feature", "builtins.str | None"),
            ],
        ),
        class(py.get_type::<InvalidError>(), invalid, None, &[], &[]),
        class(
            py.get_type::<IntentError>(),
            invalid,
            None,
            &[],
            &INTENT_FIELDS,
        ),
        class(
            py.get_type::<ValidateError>(),
            invalid,
            None,
            &[],
            &VALIDATE_FIELDS,
        ),
        class(
            py.get_type::<IdError>(),
            Some("Backend"),
            None,
            &[],
            &ID_FIELDS,
        ),
        class(py.get_type::<CodecError>(), None, None, &[], &[]),
        class(
            py.get_type::<TypedDecodeError>(),
            None,
            None,
            &[],
            &[
                ("position", "MessageId | None"),
                ("source", "builtins.BaseException"),
            ],
        ),
        class(py.get_type::<ProtocolError>(), None, None, &[], &[]),
        class(py.get_type::<TransportError>(), None, None, &[], &[]),
        class(
            py.get_type::<PublishFailedError>(),
            None,
            None,
            &[],
            &[
                ("stream", "builtins.str"),
                ("topic", "builtins.str"),
                (
                    "committed",
                    "builtins.list[SendMessagesConfirmationResponse]",
                ),
                ("unconfirmed", "builtins.list[IggyMessage]"),
            ],
        ),
        class(
            py.get_type::<IntegrityError>(),
            None,
            None,
            &[],
            &[("reference", "builtins.str")],
        ),
        class(
            py.get_type::<PolicyBlockedError>(),
            Some("Forbidden"),
            None,
            &[],
            &[],
        ),
        class(
            py.get_type::<StepUpRequiredError>(),
            Some("StepUpRequired"),
            None,
            &[],
            &[("scope", "builtins.str")],
        ),
        class(
            py.get_type::<PolicyDeferredError>(),
            None,
            Some(true),
            &[],
            &[],
        ),
        class(
            py.get_type::<RoutingError>(),
            Some("NotFound"),
            Some(true),
            &[],
            &[],
        ),
        class(
            py.get_type::<NoCapableAgentError>(),
            None,
            None,
            &["no_capable_agent"],
            &[("skill", "builtins.str")],
        ),
        class(
            py.get_type::<NoInboxError>(),
            None,
            None,
            &[],
            &[("agent", "builtins.str")],
        ),
        class(
            py.get_type::<RoutePrincipalMismatchError>(),
            Some("Forbidden"),
            Some(false),
            &["permission_denied"],
            &[
                ("agent", "builtins.str"),
                ("expected", "builtins.int"),
                ("actual", "builtins.int | None"),
            ],
        ),
        class(
            py.get_type::<PresenceConflictError>(),
            Some("Conflict"),
            None,
            &[],
            &[
                ("advertised", "builtins.str"),
                ("requested", "builtins.str"),
            ],
        ),
        class(
            py.get_type::<FenceViolationError>(),
            Some("Conflict"),
            None,
            &["fence_violation"],
            &[("held", "builtins.int"), ("current", "builtins.int")],
        ),
        class(
            py.get_type::<BudgetExceededError>(),
            None,
            None,
            &["budget_exceeded"],
            &[("ceiling", "builtins.int"), ("spent", "builtins.int")],
        ),
        class(
            cancelled_error(py).clone(),
            None,
            None,
            &[],
            &[("run", "builtins.str")],
        ),
        class(
            py.get_type::<QuarantinedError>(),
            Some("Forbidden"),
            None,
            &["quarantined"],
            &[("agent", "builtins.str")],
        ),
        class(
            py.get_type::<ProvenanceError>(),
            None,
            None,
            &[],
            &PROVENANCE_FIELDS,
        ),
    ]
}

// The class standing for `err`'s variant. A send Apache Iggy's producer gave
// up on is a failed publish too, the same as the SDK reports for its own.
fn class_of<'py>(py: Python<'py>, err: &SdkError) -> Bound<'py, PyType> {
    match err {
        SdkError::PublishFailed(_) | SdkError::Iggy(IggyError::ProducerSendFailed { .. }) => {
            py.get_type::<PublishFailedError>()
        }
        SdkError::Handler(_) => py.get_type::<HandlerError>(),
        SdkError::HandlerConfig(_) => py.get_type::<HandlerConfigError>(),
        SdkError::Rejected(_) => py.get_type::<RejectedError>(),
        SdkError::Timeout(_) => timeout_error(py).clone(),
        SdkError::AmbiguousMutation(_) => py.get_type::<AmbiguousMutationError>(),
        SdkError::NoRespondTopic => py.get_type::<NoRespondTopicError>(),
        SdkError::StateStore(_) => py.get_type::<StateStoreError>(),
        SdkError::Config(_) => py.get_type::<ConfigError>(),
        SdkError::NoStream => py.get_type::<NoStreamError>(),
        SdkError::Query(_) => py.get_type::<QueryError>(),
        SdkError::Kv(_) => py.get_type::<KvError>(),
        SdkError::Fork(_) => py.get_type::<ForkError>(),
        SdkError::Graph(_) => py.get_type::<GraphError>(),
        SdkError::Session(_) => py.get_type::<SessionError>(),
        SdkError::Authz(_) => py.get_type::<AuthzError>(),
        SdkError::Filter(_) => py.get_type::<FilterError>(),
        SdkError::FilterFault { .. } => py.get_type::<FilterFaultError>(),
        SdkError::FilterOversizedRecord { .. } => py.get_type::<FilterOversizedRecordError>(),
        SdkError::ConsumerGroupSetup { .. } => py.get_type::<ConsumerGroupSetupError>(),
        SdkError::Checkpoint(_) => py.get_type::<CheckpointError>(),
        SdkError::Codec(_) => py.get_type::<CodecError>(),
        SdkError::Invalid(_) => py.get_type::<InvalidError>(),
        SdkError::Protocol(_) => py.get_type::<ProtocolError>(),
        SdkError::Unsupported { .. } => py.get_type::<UnsupportedError>(),
        SdkError::Integrity { .. } => py.get_type::<IntegrityError>(),
        SdkError::Signature(_) => py.get_type::<SignatureError>(),
        SdkError::PolicyBlocked(_) => py.get_type::<PolicyBlockedError>(),
        SdkError::StepUpRequired { .. } => py.get_type::<StepUpRequiredError>(),
        SdkError::PolicyDeferred(_) => py.get_type::<PolicyDeferredError>(),
        SdkError::NoCapableAgent { .. } => py.get_type::<NoCapableAgentError>(),
        SdkError::NoInbox { .. } => py.get_type::<NoInboxError>(),
        SdkError::PresenceConflict { .. } => py.get_type::<PresenceConflictError>(),
        SdkError::RoutePrincipalMismatch { .. } => py.get_type::<RoutePrincipalMismatchError>(),
        SdkError::FenceViolation { .. } => py.get_type::<FenceViolationError>(),
        SdkError::BudgetExceeded { .. } => py.get_type::<BudgetExceededError>(),
        SdkError::Cancelled { .. } => cancelled_error(py).clone(),
        SdkError::Quarantined { .. } => py.get_type::<QuarantinedError>(),
        SdkError::Iggy(_) => py.get_type::<TransportError>(),
        SdkError::Id(_) => py.get_type::<IdError>(),
        SdkError::Provenance(_) => py.get_type::<ProvenanceError>(),
        _ => py.get_type::<LaserError>(),
    }
}

// Apache Iggy prints a failed producer send as "Producer send failed" alone,
// so the message names its target and cause like the SDK's own failed publish.
fn message_of(err: &SdkError) -> String {
    match err {
        SdkError::Iggy(IggyError::ProducerSendFailed {
            cause,
            stream_name,
            topic_name,
            ..
        }) => format!("publish failed to {stream_name}/{topic_name}: {cause}"),
        _ => err.to_string(),
    }
}

// The variant's own fields, and the original failure as `__cause__` for a
// failure that wraps one.
fn set_fields(py: Python<'_>, pyerr: &PyErr, err: &SdkError) -> PyResult<()> {
    let value = pyerr.value(py);
    match err {
        SdkError::PublishFailed(failure) => {
            set_publish_failure(
                value,
                &failure.stream,
                &failure.topic,
                &failure.committed,
                &failure.unconfirmed,
            )?;
            pyerr.set_cause(py, Some(to_pyerr_ref(&failure.source)));
        }
        SdkError::Iggy(IggyError::ProducerSendFailed {
            cause,
            failed,
            committed,
            stream_name,
            topic_name,
        }) => {
            set_publish_failure(value, stream_name, topic_name, committed, failed)?;
            pyerr.set_cause(py, Some(to_pyerr_ref(&SdkError::Iggy((**cause).clone()))));
        }
        SdkError::Query(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Kv(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Fork(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Graph(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Session(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Authz(error) => value.setattr("detail", ser_to_py(py, error)?)?,
        SdkError::Checkpoint(error) => value.setattr("detail", ser_to_py(py, error.as_ref())?)?,
        SdkError::Id(error) => value.setattr("kind", id_kind(error))?,
        SdkError::Provenance(error) => value.setattr("kind", provenance_kind(error))?,
        SdkError::Filter(error) => {
            set_filter_stop(value, &error.reason.to_string(), None, None, None)?;
            no_group(value)?;
            value.setattr("detail", ser_to_py(py, error)?)?;
        }
        SdkError::FilterFault {
            partition_id,
            offset,
            reason,
        } => {
            set_filter_stop(
                value,
                "fault",
                Some(reason.to_string()),
                Some(*partition_id),
                Some(*offset),
            )?;
            no_group(value)?;
            value.setattr("detail", py.None())?;
        }
        SdkError::FilterOversizedRecord {
            partition_id,
            offset,
        } => {
            set_filter_stop(
                value,
                "oversized_record",
                None,
                Some(*partition_id),
                Some(*offset),
            )?;
            no_group(value)?;
            value.setattr("detail", py.None())?;
        }
        // The group exists, its filter was not configured. `reason` is the
        // catalog's refusal when there is one.
        SdkError::ConsumerGroupSetup {
            group_id,
            name,
            identity,
            source,
        } => {
            let reason = source
                .filter_reason()
                .map_or_else(|| "setup_failed".to_owned(), |reason| reason.to_string());
            set_filter_stop(value, &reason, None, None, None)?;
            value.setattr("group_id", group_id)?;
            value.setattr("group_name", name)?;
            value.setattr("identity", ser_to_py(py, identity)?)?;
            value.setattr("detail", py.None())?;
            pyerr.set_cause(py, Some(to_pyerr_ref(source)));
        }
        SdkError::Unsupported {
            surface, feature, ..
        } => {
            value.setattr("surface", surface)?;
            value.setattr("feature", feature)?;
        }
        SdkError::Integrity { reference } => value.setattr("reference", reference)?,
        SdkError::StepUpRequired { scope } => value.setattr("scope", scope)?,
        SdkError::NoCapableAgent { skill } => value.setattr("skill", skill)?,
        SdkError::NoInbox { agent } | SdkError::Quarantined { agent } => {
            value.setattr("agent", agent)?;
        }
        SdkError::PresenceConflict {
            advertised,
            requested,
        } => {
            value.setattr("advertised", advertised)?;
            value.setattr("requested", requested)?;
        }
        SdkError::RoutePrincipalMismatch {
            agent,
            expected,
            actual,
        } => {
            value.setattr("agent", agent)?;
            value.setattr("expected", expected)?;
            value.setattr("actual", actual)?;
        }
        // `stale` is already the read-model classifier, so the held token is `held`.
        SdkError::FenceViolation { stale, current } => {
            value.setattr("held", stale)?;
            value.setattr("current", current)?;
        }
        SdkError::BudgetExceeded { ceiling, spent } => {
            value.setattr("ceiling", ceiling)?;
            value.setattr("spent", spent)?;
        }
        SdkError::Cancelled { run } => value.setattr("run", run)?,
        _ => {}
    }
    Ok(())
}

fn set_publish_failure(
    value: &Bound<'_, pyo3::exceptions::PyBaseException>,
    stream: &str,
    topic: &str,
    committed: &[iggy::prelude::SendMessagesConfirmationResponse],
    unconfirmed: &[IggyMessage],
) -> PyResult<()> {
    let committed = committed
        .iter()
        .cloned()
        .map(PySendMessagesConfirmation::from)
        .collect::<Vec<_>>();
    let unconfirmed = unconfirmed
        .iter()
        .map(PyUnconfirmedMessage::from)
        .collect::<Vec<_>>();
    value.setattr("stream", stream)?;
    value.setattr("topic", topic)?;
    value.setattr("committed", committed)?;
    value.setattr("unconfirmed", unconfirmed)
}

fn set_filter_stop(
    value: &Bound<'_, pyo3::exceptions::PyBaseException>,
    reason: &str,
    fault_reason: Option<String>,
    partition_id: Option<u32>,
    offset: Option<u64>,
) -> PyResult<()> {
    value.setattr("reason", reason)?;
    value.setattr("fault_reason", fault_reason)?;
    value.setattr("partition_id", partition_id)?;
    value.setattr("offset", offset)
}

// A filter failure that is not a group setup failure names no group.
fn no_group(value: &Bound<'_, pyo3::exceptions::PyBaseException>) -> PyResult<()> {
    let none = value.py().None();
    value.setattr("group_id", &none)?;
    value.setattr("group_name", &none)?;
    value.setattr("identity", &none)
}

// A surface failure raised by hand with the typed `detail` an SDK-raised one
// carries decodes back to that typed failure.
fn typed_detail(
    py: Python<'_>,
    value: &Bound<'_, pyo3::exceptions::PyBaseException>,
) -> Option<SdkError> {
    let detail = value
        .getattr("detail")
        .ok()
        .filter(|detail| !detail.is_none())?;
    let is = |class: Bound<'_, PyType>| value.is_instance(&class).unwrap_or(false);
    if is(py.get_type::<QueryError>()) {
        py_to_de(&detail).ok().map(SdkError::Query)
    } else if is(py.get_type::<KvError>()) {
        py_to_de(&detail).ok().map(SdkError::Kv)
    } else if is(py.get_type::<ForkError>()) {
        py_to_de(&detail).ok().map(SdkError::Fork)
    } else if is(py.get_type::<GraphError>()) {
        py_to_de(&detail).ok().map(SdkError::Graph)
    } else if is(py.get_type::<SessionError>()) {
        py_to_de(&detail).ok().map(SdkError::Session)
    } else if is(py.get_type::<AuthzError>()) {
        py_to_de(&detail).ok().map(SdkError::Authz)
    } else if is(py.get_type::<CheckpointError>()) {
        py_to_de::<laser_sdk::wire::checkpoint::CheckpointError>(&detail)
            .ok()
            .map(SdkError::from)
    } else if is(py.get_type::<FilterError>()) {
        py_to_de(&detail).ok().map(SdkError::Filter)
    } else {
        None
    }
}

fn attribute<'py, T: FromPyObjectOwned<'py>>(
    value: &Bound<'py, pyo3::exceptions::PyBaseException>,
    name: &str,
) -> Option<T> {
    value.getattr(name).ok()?.extract().ok()
}

// A header value as its Iggy kind word and the Python value. A value that
// does not decode as its declared kind is raw bytes.
fn header_object(py: Python<'_>, value: &HeaderValue) -> PyResult<(String, Py<PyAny>)> {
    let kind = value.kind();
    let converted = match kind {
        HeaderKind::Raw => value.as_raw().map(|raw| object(py, PyBytes::new(py, raw))),
        HeaderKind::String => value.as_str().map(|text| object(py, text)),
        HeaderKind::Bool => value.as_bool().map(|flag| object(py, flag)),
        HeaderKind::Int8 => value.as_int8().map(|number| object(py, number)),
        HeaderKind::Int16 => value.as_int16().map(|number| object(py, number)),
        HeaderKind::Int32 => value.as_int32().map(|number| object(py, number)),
        HeaderKind::Int64 => value.as_int64().map(|number| object(py, number)),
        HeaderKind::Int128 => value.as_int128().map(|number| object(py, number)),
        HeaderKind::Uint8 => value.as_uint8().map(|number| object(py, number)),
        HeaderKind::Uint16 => value.as_uint16().map(|number| object(py, number)),
        HeaderKind::Uint32 => value.as_uint32().map(|number| object(py, number)),
        HeaderKind::Uint64 => value.as_uint64().map(|number| object(py, number)),
        HeaderKind::Uint128 => value.as_uint128().map(|number| object(py, number)),
        HeaderKind::Float32 => value.as_float32().map(|number| object(py, number)),
        HeaderKind::Float64 => value.as_float64().map(|number| object(py, number)),
    };
    Ok(match converted {
        Ok(converted) => (kind.to_string(), converted),
        Err(_) => (
            "raw".to_owned(),
            object(py, PyBytes::new(py, value.as_bytes())),
        ),
    })
}

fn object<'py>(py: Python<'py>, value: impl IntoPyObjectExt<'py>) -> Py<PyAny> {
    value
        .into_bound_py_any(py)
        .map(Bound::unbind)
        .unwrap_or_else(|_| py.None())
}

// Build a `name` exception class inheriting `LaserError` and `extra_base`, tagged
// as living in the `laser_sdk` module and carrying `doc`. Panics only if the core
// interpreter builtins are unavailable, which is unreachable at runtime.
fn synth_exception<'py>(
    py: Python<'py>,
    name: &str,
    extra_base: &Bound<'py, PyType>,
    doc: &str,
) -> Py<PyType> {
    let laser = py.get_type::<LaserError>();
    let bases = PyTuple::new(py, [laser.as_any(), extra_base.as_any()]).expect("bases tuple");
    let namespace = PyDict::new(py);
    namespace
        .set_item("__module__", "laser_sdk")
        .expect("set __module__");
    namespace.set_item("__doc__", doc).expect("set __doc__");
    let class = py
        .import("builtins")
        .and_then(|builtins| builtins.getattr("type"))
        .and_then(|type_fn| type_fn.call1((name, bases, namespace)))
        .and_then(|class| Ok(class.cast_into::<PyType>()?))
        .unwrap_or_else(|_| panic!("synthesize the {name} exception class"));
    class.unbind()
}

// The synthesized `TimeoutError` class (also a builtins.TimeoutError). Cached on
// first use, seeded at registration so it always resolves during `to_pyerr`.
fn timeout_error(py: Python<'_>) -> &Bound<'_, PyType> {
    TIMEOUT_ERROR
        .get_or_init_py_attached(py, || {
            synth_exception(
                py,
                "TimeoutError",
                &py.get_type::<PyTimeoutError>(),
                "A request timed out. Retryable.",
            )
        })
        .bind(py)
}

// The synthesized `CancelledError` class (also an asyncio.CancelledError).
fn cancelled_error(py: Python<'_>) -> &Bound<'_, PyType> {
    CANCELLED_ERROR
        .get_or_init_py_attached(py, || {
            let asyncio_cancelled = py
                .import("asyncio")
                .and_then(|asyncio| asyncio.getattr("CancelledError"))
                .and_then(|class| Ok(class.cast_into::<PyType>()?))
                .expect("import asyncio.CancelledError");
            synth_exception(
                py,
                "CancelledError",
                &asyncio_cancelled,
                "A registered run's recorded cancel intent was observed at a step boundary. \
                 `run` names the run. Not retryable on the same run.",
            )
        })
        .bind(py)
}

// The variant word of an id failure, one of the `IdError` class constants.
fn id_kind(error: &laser_sdk::types::IdError) -> &'static str {
    use laser_sdk::types::IdError as Id;
    match error {
        Id::Empty => "empty",
        Id::TooLong { .. } => "too_long",
        Id::InvalidChar(_) => "invalid_char",
        Id::InvalidUlid(_) => "invalid_ulid",
        Id::InvalidMessageId(_) => "invalid_message_id",
    }
}

// The variant word of a provenance failure, one of the `ProvenanceError`
// class constants.
fn provenance_kind(error: &laser_sdk::provenance::ProvenanceError) -> &'static str {
    use laser_sdk::provenance::ProvenanceError as Provenance;
    match error {
        Provenance::MissingRequired(_) => "missing_required",
        Provenance::TooLarge { .. } => "too_large",
        Provenance::InvalidValue(_) => "invalid_value",
        Provenance::InvalidValueBytes { .. } => "invalid_value_bytes",
        Provenance::NonFinite(_) => "non_finite",
        Provenance::EmptyValue(_) => "empty_value",
        Provenance::ValueTooLong { .. } => "value_too_long",
        Provenance::MalformedHeaders(_) => "malformed_headers",
        Provenance::Header(_) => "header",
        Provenance::Id(_) => "id",
    }
}
