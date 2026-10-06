use laser_sdk::types::MessageId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

/// A message's position on the log: its partition and offset. Prints as
/// `<partition_id>:<offset>`.
#[gen_stub_pyclass]
#[pyclass(name = "MessageId", frozen, eq, hash, skip_from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyMessageId {
    pub(crate) inner: MessageId,
}

impl From<MessageId> for PyMessageId {
    fn from(inner: MessageId) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMessageId {
    /// A message id at `(partition_id, offset)`.
    #[new]
    fn new(partition_id: u32, offset: u64) -> Self {
        Self::from(MessageId::new(partition_id, offset))
    }

    /// The partition the message lives on.
    #[getter]
    fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// The message's offset within that partition.
    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("MessageId({})", self.inner)
    }
}

/// A fresh time-ordered ULID, the value every SDK-minted wire id carries
/// (correlation, record, channel, execution, and destination ids).
#[gen_stub_pyfunction]
#[pyfunction]
pub fn mint_ulid() -> String {
    use laser_sdk::types::MintUlid;
    laser_sdk::wire::agent::CorrelationId::mint().to_string()
}
