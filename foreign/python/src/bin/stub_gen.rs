use pyo3::Python;
use pyo3_stub_gen::Result;
use std::fs;
use std::path::Path;

// `create_exception!` classes are invisible to the stub gatherer, so their
// block is rendered from the registered classes themselves and appended.
fn main() -> Result<()> {
    let stub = laser_sdk_py::stub_info()?;
    stub.generate()?;
    Python::initialize();
    let exceptions = Python::attach(laser_sdk_py::exception_stub)?;
    let path = Path::new("laser_sdk.pyi");
    let mut content = fs::read_to_string(path)?;
    content.push_str(&exceptions);
    let mut normalized = String::with_capacity(content.len());
    for line in content.lines() {
        normalized.push_str(line.trim_end());
        normalized.push('\n');
    }
    fs::write(path, normalized)?;
    Ok(())
}
