use pyo3_stub_gen::Result;
use std::fs;
use std::path::Path;

// The exception hierarchy is declared with `create_exception!`, which the stub
// gatherer does not see, so append it to the generated stub. Keeping it here (not
// hand-edited into the .pyi) means the file regenerates reproducibly.
const EXCEPTIONS: &str = "
class LaserError(Exception):
    code: builtins.str
    retryable: builtins.bool
    unsupported: builtins.bool
    not_found: builtins.bool
    version_skew: builtins.bool
    version_conflict: builtins.bool
    stale: builtins.bool

class ConfigError(LaserError): ...
class TimeoutError(LaserError): ...
class QueryError(LaserError): ...
class KvError(LaserError): ...
class ForkError(LaserError): ...
class UnsupportedError(LaserError): ...
class InvalidError(LaserError): ...
class CodecError(LaserError): ...
class ProtocolError(LaserError): ...
class TransportError(LaserError): ...
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
