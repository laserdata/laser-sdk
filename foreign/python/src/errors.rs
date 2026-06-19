use laser_sdk::LaserError as SdkError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    laser_sdk,
    LaserError,
    PyException,
    "Base class for every laser-sdk error. Catch it to handle any SDK failure. \
     Every instance carries `code`, `retryable`, `unsupported`, `not_found`, \
     `version_skew`, `version_conflict`, and `stale` attributes."
);
create_exception!(
    laser_sdk,
    ConfigError,
    LaserError,
    "Invalid client configuration, or a convenience call needed a default stream and none was set."
);
create_exception!(laser_sdk, TimeoutError, LaserError, "A request timed out.");
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
    UnsupportedError,
    LaserError,
    "The connected infrastructure does not provide the requested feature (raw Apache Iggy without LaserData Cloud)."
);
create_exception!(
    laser_sdk,
    InvalidError,
    LaserError,
    "Client-side validation rejected the input before any round-trip."
);
create_exception!(
    laser_sdk,
    CodecError,
    LaserError,
    "A payload or wire envelope encode/decode failed."
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

    let pyerr = match &err {
        SdkError::Query(_) => QueryError::new_err(message),
        SdkError::Kv(_) => KvError::new_err(message),
        SdkError::Fork(_) => ForkError::new_err(message),
        SdkError::Timeout(_) => TimeoutError::new_err(message),
        SdkError::Unsupported(_) => UnsupportedError::new_err(message),
        SdkError::Invalid(_) | SdkError::Rejected(_) | SdkError::Id(_) => {
            InvalidError::new_err(message)
        }
        SdkError::Codec(_) => CodecError::new_err(message),
        SdkError::Protocol(_) => ProtocolError::new_err(message),
        SdkError::Config(_) | SdkError::NoStream | SdkError::NoRespondTopic => {
            ConfigError::new_err(message)
        }
        SdkError::Iggy(_) => TransportError::new_err(message),
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
    });
    pyerr
}

// Register the exception types on the module so `from laser_sdk import LaserError`
// resolves and isinstance checks against the hierarchy work.
pub(crate) fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("LaserError", py.get_type::<LaserError>())?;
    module.add("ConfigError", py.get_type::<ConfigError>())?;
    module.add("TimeoutError", py.get_type::<TimeoutError>())?;
    module.add("QueryError", py.get_type::<QueryError>())?;
    module.add("KvError", py.get_type::<KvError>())?;
    module.add("ForkError", py.get_type::<ForkError>())?;
    module.add("UnsupportedError", py.get_type::<UnsupportedError>())?;
    module.add("InvalidError", py.get_type::<InvalidError>())?;
    module.add("CodecError", py.get_type::<CodecError>())?;
    module.add("ProtocolError", py.get_type::<ProtocolError>())?;
    module.add("TransportError", py.get_type::<TransportError>())?;
    Ok(())
}
