use laser_sdk::LaserError as SdkError;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTimeoutError};
use pyo3::prelude::*;
use pyo3::sync::OnceLockExt;
use pyo3::types::{PyDict, PyTuple, PyType};
use std::sync::OnceLock;

create_exception!(
    laser_sdk,
    LaserError,
    PyException,
    "Base class for every laser-sdk error. Catch it to handle any SDK failure. \
     Every instance carries `code`, `retryable`, `unsupported`, `not_found`, \
     `version_skew`, `version_conflict`, `stale`, `permission_denied`, \
     `stream_or_topic_not_found`, `no_capable_agent`, `lease_lost`, \
     `fence_violation`, `budget_exceeded`, `quarantined`, and `not_leader` attributes."
);
create_exception!(
    laser_sdk,
    ConfigError,
    LaserError,
    "Invalid client configuration, or a convenience call needed a default stream and none was set."
);
create_exception!(
    laser_sdk,
    QueryError,
    LaserError,
    "The managed query / projection surface returned a typed failure."
);
create_exception!(
    laser_sdk,
    KvError,
    LaserError,
    "The managed key-value store returned a typed failure."
);
create_exception!(
    laser_sdk,
    ForkError,
    LaserError,
    "A fork operation returned a typed failure."
);
create_exception!(
    laser_sdk,
    GraphError,
    LaserError,
    "The managed knowledge-graph surface returned a typed failure."
);
create_exception!(
    laser_sdk,
    AuthzError,
    LaserError,
    "The authorization surface returned a typed failure (unknown or invalid role, \
     unauthorized caller, or a lost bind revision race)."
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
    "The connected infrastructure does not provide the requested feature (raw Apache Iggy without LaserData Cloud)."
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
    CodecError,
    LaserError,
    "A payload or wire envelope encode/decode failed."
);
create_exception!(
    laser_sdk,
    TypedDecodeError,
    CodecError,
    "A typed read could not produce a value: the message names the record's log position."
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
    PolicyBlockedError,
    LaserError,
    "The enrolled action governor rejected the action before the effect ran. Not retryable."
);
create_exception!(
    laser_sdk,
    StepUpRequiredError,
    LaserError,
    "The enrolled action governor paused the action on a stronger approval. \
     The message names the scope the approval must grant."
);
create_exception!(
    laser_sdk,
    PolicyDeferredError,
    LaserError,
    "The enrolled action governor held the action for later execution. Retryable."
);
create_exception!(
    laser_sdk,
    BudgetExceededError,
    LaserError,
    "A spend ceiling was reached. Not retryable until the ceiling is raised."
);
// `TimeoutError` and `CancelledError` are synthesized with two bases each so the
// SDK hierarchy stays catchable (`except LaserError`) while `except TimeoutError`
// (the builtin) and `except asyncio.CancelledError` also catch them. The macro
// takes a single base, so these are built with the `type(...)` builtin at
// registration and cached for `to_pyerr`.
static TIMEOUT_ERROR: OnceLock<Py<PyType>> = OnceLock::new();
static CANCELLED_ERROR: OnceLock<Py<PyType>> = OnceLock::new();

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
                "A request timed out.",
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
                 Not retryable on the same run.",
            )
        })
        .bind(py)
}

// Map the SDK's one error type onto the typed Python exception hierarchy, then
// attach the classifier results as instance attributes so Python callers branch
// on `err.retryable` / `err.unsupported` without re-deriving them.
pub(crate) fn to_pyerr(err: SdkError) -> PyErr {
    let message = err.to_string();
    let code = format!("{:?}", err.code());
    let retryable = err.is_retryable();
    let unsupported = err.is_unsupported();
    let not_found = err.is_not_found();
    let version_skew = err.is_version_skew();
    let version_conflict = err.is_version_conflict();
    let stale = err.is_stale();
    let permission_denied = err.is_permission_denied();
    let stream_or_topic_not_found = err.is_stream_or_topic_not_found();
    let no_capable_agent = err.is_no_capable_agent();
    let lease_lost = err.is_lease_lost();
    let fence_violation = err.is_fence_violation();
    let budget_exceeded = err.is_budget_exceeded();
    let quarantined = err.is_quarantined();
    let not_leader = err.is_not_leader();

    let pyerr = match &err {
        SdkError::Query(_) => QueryError::new_err(message),
        SdkError::Kv(_) => KvError::new_err(message),
        SdkError::Fork(_) => ForkError::new_err(message),
        SdkError::Graph(_) => GraphError::new_err(message),
        SdkError::Authz(_) => AuthzError::new_err(message),
        SdkError::Signature(_) => SignatureError::new_err(message),
        SdkError::Timeout(_) => {
            Python::attach(|py| PyErr::from_type(timeout_error(py).clone(), message))
        }
        SdkError::Unsupported { .. } => UnsupportedError::new_err(message),
        SdkError::Invalid(_) | SdkError::Rejected(_) | SdkError::Id(_) => {
            InvalidError::new_err(message)
        }
        SdkError::Codec(_) => CodecError::new_err(message),
        SdkError::Protocol(_) => ProtocolError::new_err(message),
        SdkError::Config(_)
        | SdkError::HandlerConfig(_)
        | SdkError::NoStream
        | SdkError::NoRespondTopic => ConfigError::new_err(message),
        SdkError::Iggy(_) => TransportError::new_err(message),
        SdkError::BudgetExceeded { .. } => BudgetExceededError::new_err(message),
        SdkError::PolicyBlocked(_) => PolicyBlockedError::new_err(message),
        SdkError::StepUpRequired(_) => StepUpRequiredError::new_err(message),
        SdkError::PolicyDeferred(_) => PolicyDeferredError::new_err(message),
        SdkError::Cancelled { .. } => {
            Python::attach(|py| PyErr::from_type(cancelled_error(py).clone(), message))
        }
        _ => LaserError::new_err(message),
    };

    Python::attach(|py| {
        let value = pyerr.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("retryable", retryable);
        let _ = value.setattr("unsupported", unsupported);
        let _ = value.setattr("not_found", not_found);
        let _ = value.setattr("version_skew", version_skew);
        let _ = value.setattr("version_conflict", version_conflict);
        let _ = value.setattr("stale", stale);
        let _ = value.setattr("permission_denied", permission_denied);
        let _ = value.setattr("stream_or_topic_not_found", stream_or_topic_not_found);
        let _ = value.setattr("no_capable_agent", no_capable_agent);
        let _ = value.setattr("lease_lost", lease_lost);
        let _ = value.setattr("fence_violation", fence_violation);
        let _ = value.setattr("budget_exceeded", budget_exceeded);
        let _ = value.setattr("quarantined", quarantined);
        let _ = value.setattr("not_leader", not_leader);
    });
    pyerr
}

// Register the exception types on the module so `from laser_sdk import LaserError`
// resolves and isinstance checks against the hierarchy work.
pub(crate) fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("LaserError", py.get_type::<LaserError>())?;
    module.add("ConfigError", py.get_type::<ConfigError>())?;
    module.add("TimeoutError", timeout_error(py).clone())?;
    module.add("QueryError", py.get_type::<QueryError>())?;
    module.add("KvError", py.get_type::<KvError>())?;
    module.add("ForkError", py.get_type::<ForkError>())?;
    module.add("GraphError", py.get_type::<GraphError>())?;
    module.add("AuthzError", py.get_type::<AuthzError>())?;
    module.add("SignatureError", py.get_type::<SignatureError>())?;
    module.add("UnsupportedError", py.get_type::<UnsupportedError>())?;
    // Graft `ValueError` in as a second base (the macro takes one base only):
    // a rejected argument is Python's value error, and the SDK hierarchy must
    // keep catching it as a `LaserError`. Same dual-base intent as the
    // synthesized `TimeoutError`/`CancelledError`, applied to the macro class.
    let invalid = py.get_type::<InvalidError>();
    let bases = PyTuple::new(
        py,
        [
            py.get_type::<LaserError>().as_any(),
            py.get_type::<pyo3::exceptions::PyValueError>().as_any(),
        ],
    )?;
    invalid.setattr("__bases__", bases)?;
    module.add("InvalidError", invalid)?;
    module.add("CodecError", py.get_type::<CodecError>())?;
    module.add("TypedDecodeError", py.get_type::<TypedDecodeError>())?;
    module.add("ProtocolError", py.get_type::<ProtocolError>())?;
    module.add("TransportError", py.get_type::<TransportError>())?;
    module.add("BudgetExceededError", py.get_type::<BudgetExceededError>())?;
    module.add("PolicyBlockedError", py.get_type::<PolicyBlockedError>())?;
    module.add("StepUpRequiredError", py.get_type::<StepUpRequiredError>())?;
    module.add("PolicyDeferredError", py.get_type::<PolicyDeferredError>())?;
    module.add("CancelledError", cancelled_error(py).clone())?;
    Ok(())
}
