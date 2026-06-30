mod agdx;
mod agent;
mod agent_runtime;
mod client;
mod convert;
mod errors;
mod fork;
mod graph;
mod interop;
mod kv;
mod memory;
mod publish;
mod query;
mod reader;
mod schema;
mod state_store;
mod workflow;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;

#[pymodule]
fn laser_sdk(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(py, module)?;
    module.add_class::<client::PyLaser>()?;
    module.add_class::<client::PyCapabilities>()?;
    module.add_class::<client::PyBackendDescriptor>()?;
    module.add_class::<publish::PyPublish>()?;
    module.add_class::<publish::PyBatchPublish>()?;
    module.add_class::<schema::PyCompiledSchema>()?;
    module.add_class::<query::PyQuery>()?;
    module.add_class::<query::PyRow>()?;
    module.add_class::<query::PyQueryResult>()?;
    module.add_class::<kv::PyKv>()?;
    module.add_class::<kv::PyKvEntry>()?;
    module.add_class::<kv::PyKvPage>()?;
    module.add_class::<kv::PyKvSet>()?;
    module.add_class::<kv::PyKvScan>()?;
    module.add_class::<kv::PyKvDeleteMany>()?;
    module.add_class::<fork::PyForkHandle>()?;
    module.add_class::<fork::PyForkPut>()?;
    module.add_class::<agent::PyProvenance>()?;
    module.add_class::<agent::PyAgentMessage>()?;
    module.add_class::<agent::PyTopics>()?;
    module.add_class::<agdx::PyAgdx>()?;
    module.add_class::<agdx::PyAgdxStream>()?;
    module.add_class::<agent_runtime::PyAgentCtx>()?;
    module.add_class::<agent_runtime::PyAgentHandle>()?;
    module.add_class::<workflow::PyWorkflow>()?;
    module.add_class::<reader::PyCursor>()?;
    module.add_class::<reader::PyMessage>()?;
    module.add_class::<memory::PyMemory>()?;
    module.add_class::<memory::PyMemoryItem>()?;
    module.add_class::<graph::PyGraph>()?;
    module.add_class::<state_store::PyInMemoryStore>()?;
    module.add_class::<state_store::PyFileStore>()?;
    module.add_class::<interop::PyA2aBridge>()?;
    module.add_class::<interop::PyMcpBridge>()?;
    module.add_function(wrap_pyfunction!(agent::new_conversation_id, module)?)?;
    module.add_function(wrap_pyfunction!(agent::new_correlation_id, module)?)?;
    module.add_function(wrap_pyfunction!(agent::derive_conversation_id, module)?)?;
    module.add_function(wrap_pyfunction!(graph::node_id, module)?)?;
    module.add_function(wrap_pyfunction!(graph::edge_id, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_node, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_edge, module)?)?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
