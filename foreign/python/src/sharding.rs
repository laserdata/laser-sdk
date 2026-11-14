use crate::errors::{InvalidError, from_callback_error};
use laser_sdk::error::LaserError;
use laser_sdk::stream::ShardRequest;
use pyo3::prelude::*;
use pyo3::types::PyBool;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;

/// What a producer's own `sharding` callable sees for one background send:
/// the shard count, the target, and how many records the send carries.
#[gen_stub_pyclass]
#[pyclass(name = "ShardRequest", frozen, get_all)]
pub struct PyShardRequest {
    /// The background shard count, at least one.
    shards: usize,
    stream: String,
    topic: String,
    /// The records of this send.
    records: usize,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyShardRequest {
    fn __repr__(&self) -> String {
        format!(
            "ShardRequest(shards={}, stream={:?}, topic={:?}, records={})",
            self.shards, self.stream, self.topic, self.records
        )
    }
}

impl From<&ShardRequest<'_>> for PyShardRequest {
    fn from(request: &ShardRequest<'_>) -> Self {
        Self {
            shards: request.shards,
            stream: request.stream.to_owned(),
            topic: request.topic.to_owned(),
            records: request.records,
        }
    }
}

/// A Python `sharding` callable: `sharding(request) -> int`.
#[derive(Clone)]
pub(crate) struct PySharding(Arc<Py<PyAny>>);

impl PySharding {
    pub(crate) fn new(callable: &Bound<'_, PyAny>) -> PyResult<Self> {
        if !callable.is_callable() {
            return Err(InvalidError::new_err(
                "sharding must be a callable that takes a ShardRequest and returns a shard index",
            ));
        }
        Ok(Self(Arc::new(callable.clone().unbind())))
    }

    // The producer asks for a shard inline in each background send. An
    // exception, or a value that is not a shard index, fails that send. A bool
    // is an int to Python but never a shard index.
    pub(crate) fn picker(
        self,
    ) -> impl Fn(&ShardRequest<'_>) -> Result<usize, LaserError> + Send + Sync + 'static {
        move |request| {
            Python::attach(|py| {
                let shard = self
                    .0
                    .call1(py, (PyShardRequest::from(request),))
                    .map_err(from_callback_error)?
                    .into_bound(py);
                if shard.is_instance_of::<PyBool>() {
                    return Err(LaserError::Invalid(format!(
                        "sharding must return a shard index, not {shard}"
                    )));
                }
                shard.extract::<usize>().map_err(|error| {
                    LaserError::Invalid(format!("sharding must return a shard index: {error}"))
                })
            })
        }
    }
}
