use pyo3_stub_gen::Result;
use std::fs;
use std::path::Path;

// The exception hierarchy is declared with `create_exception!`, which the stub
// gatherer does not see, so append it to the generated stub. Keeping it here (not
// hand-edited into the .pyi) means the file regenerates reproducibly.
// `TimeoutError` and `CancelledError` carry a second base each (the runtime
// synthesizes them with `type(...)`): `builtins.TimeoutError` and
// `asyncio.CancelledError`, so `except TimeoutError` / `except
// asyncio.CancelledError` catch them while `except LaserError` still does too.
// `InvalidError` also grafts `builtins.ValueError` on as a second base at
// registration, so `except ValueError` catches it too.
const EXCEPTIONS: &str = "
import asyncio

class LaserError(Exception):
    code: builtins.str
    retryable: builtins.bool
    unsupported: builtins.bool
    not_found: builtins.bool
    version_skew: builtins.bool
    version_conflict: builtins.bool
    stale: builtins.bool
    permission_denied: builtins.bool
    stream_or_topic_not_found: builtins.bool
    no_capable_agent: builtins.bool
    lease_lost: builtins.bool
    fence_violation: builtins.bool
    budget_exceeded: builtins.bool
    quarantined: builtins.bool
    not_leader: builtins.bool

class ConfigError(LaserError): ...
class TimeoutError(LaserError, builtins.TimeoutError): ...
class QueryError(LaserError): ...
class KvError(LaserError): ...
class ForkError(LaserError): ...
class GraphError(LaserError): ...
class AuthzError(LaserError): ...
class SignatureError(LaserError): ...
class UnsupportedError(LaserError): ...
class InvalidError(LaserError, builtins.ValueError): ...
class CodecError(LaserError): ...
class TypedDecodeError(CodecError): ...
class ProtocolError(LaserError): ...
class TransportError(LaserError): ...
class BudgetExceededError(LaserError): ...
class PolicyBlockedError(LaserError): ...
class StepUpRequiredError(LaserError): ...
class PolicyDeferredError(LaserError): ...
class CancelledError(LaserError, asyncio.CancelledError): ...
";

fn main() -> Result<()> {
    let stub = laser_sdk_py::stub_info()?;
    stub.generate()?;
    let path = Path::new("laser_sdk.pyi");
    let mut content = fs::read_to_string(path)?;
    if !content.contains("class LaserError(Exception):") {
        content.push_str(EXCEPTIONS);
        fs::write(path, content)?;
    }
    Ok(())
}
