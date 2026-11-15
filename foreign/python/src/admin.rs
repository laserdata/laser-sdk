use laser_sdk::stream::{ConsumerGroupSummary, PartitionInfo, StreamInfo, TopicInfo};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// One stream as the server reports it. `Laser.streams` and `Stream.get`
/// return it.
#[gen_stub_pyclass]
#[pyclass(name = "StreamInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyStreamInfo {
    info: StreamInfo,
}

/// One topic as the server reports it. `Stream.topics` and `Topic.get`
/// return it.
#[gen_stub_pyclass]
#[pyclass(name = "TopicInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyTopicInfo {
    info: TopicInfo,
}

/// One partition of a topic as the server reports it. `Topic.partitions`
/// returns it.
#[gen_stub_pyclass]
#[pyclass(name = "PartitionInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyPartitionInfo {
    info: PartitionInfo,
}

/// One consumer group of a topic as the topic listing reports it.
/// `Topic.consumer_groups` returns it.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerGroupSummary", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyConsumerGroupSummary {
    info: ConsumerGroupSummary,
}

impl From<StreamInfo> for PyStreamInfo {
    fn from(info: StreamInfo) -> Self {
        Self { info }
    }
}

impl From<TopicInfo> for PyTopicInfo {
    fn from(info: TopicInfo) -> Self {
        Self { info }
    }
}

impl From<PartitionInfo> for PyPartitionInfo {
    fn from(info: PartitionInfo) -> Self {
        Self { info }
    }
}

impl From<ConsumerGroupSummary> for PyConsumerGroupSummary {
    fn from(info: ConsumerGroupSummary) -> Self {
        Self { info }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyStreamInfo {
    /// The native numeric stream id.
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    fn name(&self) -> String {
        self.info.name.clone()
    }

    #[getter]
    const fn topics_count(&self) -> u32 {
        self.info.topics_count
    }

    #[getter]
    const fn size_bytes(&self) -> u64 {
        self.info.size_bytes
    }

    #[getter]
    const fn messages_count(&self) -> u64 {
        self.info.messages_count
    }

    #[getter]
    const fn created_at_micros(&self) -> u64 {
        self.info.created_at_micros
    }

    fn __repr__(&self) -> String {
        format!(
            "StreamInfo(id={}, name={}, topics_count={})",
            self.info.id, self.info.name, self.info.topics_count
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTopicInfo {
    /// The native numeric topic id.
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    fn name(&self) -> String {
        self.info.name.clone()
    }

    #[getter]
    const fn partitions_count(&self) -> u32 {
        self.info.partitions_count
    }

    /// How many nodes store each partition, `None` when the server reports no factor.
    #[getter]
    const fn replication_factor(&self) -> Option<u32> {
        self.info.replication_factor
    }

    /// How long a message is kept in milliseconds, `None` when messages never expire.
    #[getter]
    fn message_expiry_ms(&self) -> Option<f64> {
        self.info
            .message_expiry
            .map(|age| age.as_secs_f64() * 1000.0)
    }

    /// The retained size cap in bytes, `None` when the topic is unlimited.
    #[getter]
    const fn max_size_bytes(&self) -> Option<u64> {
        self.info.max_size_bytes
    }

    #[getter]
    const fn size_bytes(&self) -> u64 {
        self.info.size_bytes
    }

    #[getter]
    const fn messages_count(&self) -> u64 {
        self.info.messages_count
    }

    #[getter]
    const fn created_at_micros(&self) -> u64 {
        self.info.created_at_micros
    }

    fn __repr__(&self) -> String {
        format!(
            "TopicInfo(id={}, name={}, partitions_count={})",
            self.info.id, self.info.name, self.info.partitions_count
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPartitionInfo {
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    const fn segments_count(&self) -> u32 {
        self.info.segments_count
    }

    /// The offset of the newest message, 0 for an empty partition.
    #[getter]
    const fn current_offset(&self) -> u64 {
        self.info.current_offset
    }

    #[getter]
    const fn size_bytes(&self) -> u64 {
        self.info.size_bytes
    }

    #[getter]
    const fn messages_count(&self) -> u64 {
        self.info.messages_count
    }

    #[getter]
    const fn created_at_micros(&self) -> u64 {
        self.info.created_at_micros
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionInfo(id={}, messages_count={})",
            self.info.id, self.info.messages_count
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerGroupSummary {
    /// The native numeric group id.
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    fn name(&self) -> String {
        self.info.name.clone()
    }

    #[getter]
    const fn partitions_count(&self) -> u32 {
        self.info.partitions_count
    }

    #[getter]
    const fn members_count(&self) -> u32 {
        self.info.members_count
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsumerGroupSummary(id={}, name={}, members_count={})",
            self.info.id, self.info.name, self.info.members_count
        )
    }
}
