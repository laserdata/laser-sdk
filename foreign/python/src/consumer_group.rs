use crate::async_bridge::{Awaitable, awaitable};
use crate::convert::{duration_ms, payload_bytes, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::filters::{PyConsumerFilter, PyFilteredReader, filter_headers, filtered_start};
use crate::stream::PyTopic;
use crate::transport::{ConsumerConfig, PyConsumer, configure_consumer};
use laser_sdk::filters::{FilteredStart, GroupFilterSpec};
use laser_sdk::stream::{ConsumerGroup, ConsumerGroupInfo};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// One consumer group of a topic. The group owns its filter policy: consumers
/// and readers built from this handle run whatever the group is configured
/// with, a filter or none, and never name a filter themselves. Build it with
/// `Topic.consumer_group` or `Topic.consumer_group_id`.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerGroup", frozen)]
pub struct PyConsumerGroup {
    group: ConsumerGroup,
    topic: PyTopic,
}

/// A consumer group as the server knows it, returned by `ConsumerGroup.create`
/// and `ConsumerGroup.info`.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerGroupInfo", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyConsumerGroupInfo {
    info: ConsumerGroupInfo,
}

/// A consumer group's filter policy. Build it with `ConsumerGroup.filter`.
/// Every verb needs a managed backend that serves the filter catalog. Catalog
/// replies are dicts in the wire shape.
#[gen_stub_pyclass]
#[pyclass(name = "GroupFilter", frozen)]
pub struct PyGroupFilter {
    group: ConsumerGroup,
}

impl PyConsumerGroup {
    pub(crate) const fn new(group: ConsumerGroup, topic: PyTopic) -> Self {
        Self { group, topic }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerGroup {
    /// The group name, `None` for a handle addressed by numeric id.
    #[getter]
    fn name(&self) -> Option<String> {
        self.group.name().map(str::to_owned)
    }

    /// The native numeric id, `None` for a handle addressed by name.
    #[getter]
    fn id(&self) -> Option<u64> {
        self.group.id()
    }

    /// The topic this group consumes.
    #[getter]
    fn topic(&self) -> PyTopic {
        self.topic.clone()
    }

    /// Create the group, with an optional filter policy configured in the
    /// same call: a `filter` definition saved as the group's own filter under
    /// `filter_name`. `filter_name` is required with `filter`, follows the
    /// registered filter name rule and need not be unique. Idempotent: an
    /// existing group is kept and the same policy keeps its binding. A group
    /// that runs another policy raises `FilterError` with reason `conflict`.
    /// Pass `operation_id` to resume the same configuration after a crash.
    /// Returns the `ConsumerGroupInfo`.
    #[pyo3(signature = (*, filter=None, filter_name=None, operation_id=None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyConsumerFilter>,
        filter_name: Option<String>,
        operation_id: Option<u128>,
    ) -> PyResult<Awaitable<'py, PyConsumerGroupInfo>> {
        let policy = group_policy(filter, filter_name)?;
        let group = self.group.clone();
        awaitable(py, async move {
            let mut create = group.create();
            if let Some(policy) = policy {
                create = create.policy(policy);
            }
            if let Some(operation_id) = operation_id {
                create = create.operation_id(operation_id);
            }
            let info = create.build().await.map_err(to_pyerr)?;
            Ok(PyConsumerGroupInfo { info })
        })
    }

    /// The group as the server knows it.
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyConsumerGroupInfo>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let info = group.info().await.map_err(to_pyerr)?;
            Ok(PyConsumerGroupInfo { info })
        })
    }

    /// The group's filter policy: configure it, inspect it, pause and resume
    /// it, release it, preview and sample-test it.
    fn filter(&self) -> PyGroupFilter {
        PyGroupFilter {
            group: self.group.clone(),
        }
    }

    /// Build a live, load-balanced consumer of this group with server-backed
    /// offsets. On a server that resolves group policies the consumer runs
    /// the group's filter, or none, and commits through the group's fenced
    /// acknowledgments. On Apache Iggy it is the native group consumer, which
    /// always joins the group. It supports the same polling, batching,
    /// replay, retry, and commit modes as the Rust builder. The default
    /// `auto_commit="all"` stores once the last message of a poll is handled,
    /// a message counting as handled when the next one is asked for or the
    /// consumer shuts down. Use `auto_commit="disabled"` plus
    /// `commit(message)` for commit-after-handle delivery. `batch_length` also bounds the source
    /// records one partition poll examines. The server holds a poll that
    /// finds fewer than `min_count` new records for up to `max_wait_ms`
    /// (0 to 600000, 0 answers at once), and `max_bytes` caps the record
    /// bytes of one reply. The consumer always resumes after the group's
    /// stored offset. Its `polling`, `offset` or `timestamp_micros` start
    /// only seeds a partition the group never stored, before its first read
    /// (the native consumer only those it holds when it joins), and a refused
    /// seed, such as an offset past a partition's end, fails `init` or the
    /// first `next` with the Iggy error.
    #[pyo3(signature = (*, batch_length=1000, max_wait_ms=5000.0, min_count=1, max_bytes=1048576, polling="next", offset=None, timestamp_micros=None, auto_commit="all", commit_interval_ms=0.0, commit_every=None, create_group=true, polling_retry_interval_ms=1000.0, init_retries=None, init_retry_interval_ms=1000.0, allow_replay=false))]
    #[allow(clippy::too_many_arguments)]
    fn consumer(
        &self,
        batch_length: u32,
        max_wait_ms: f64,
        min_count: u32,
        max_bytes: u32,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['first', 'last', 'next', 'offset', 'timestamp']",
            imports = ("typing",)
        ))]
        polling: &str,
        offset: Option<u64>,
        timestamp_micros: Option<u64>,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['disabled', 'interval', 'polling', 'all', 'each', 'every']",
            imports = ("typing",)
        ))]
        auto_commit: &str,
        commit_interval_ms: f64,
        commit_every: Option<u32>,
        create_group: bool,
        polling_retry_interval_ms: f64,
        init_retries: Option<u32>,
        init_retry_interval_ms: f64,
        allow_replay: bool,
    ) -> PyResult<PyConsumer> {
        configure_consumer(
            self.group.consumer(),
            label(&self.group),
            ConsumerConfig {
                batch_length,
                max_wait_ms,
                min_count,
                max_bytes,
                polling,
                offset,
                timestamp_micros,
                auto_commit,
                commit_interval_ms,
                commit_every,
                create_group,
                polling_retry_interval_ms,
                init_retries,
                init_retry_interval_ms,
                allow_replay,
            },
            true,
        )
    }

    /// Pages selected by the group's policy with original offsets, an independent
    /// scan budget, and explicit acknowledgments. An unbound group returns every
    /// record without evaluating its payload.
    /// `start` is `next` (the default), `first`, `last`, `{"offset": n}`, or
    /// `{"timestamp": micros}`.
    /// `count` bounds records per page, default 100. `max_examined` bounds
    /// the source records one page examines, independent of `count`.
    /// `max_reply_bytes` bounds record bytes per page.
    /// `max_unacked_pages` bounds outstanding pages per partition, default 1024.
    /// `local_guard` checks delivered records against the filter locally.
    /// `max_wait_ms` is how long the server may hold one filtered wait after
    /// a round that found nothing new, at most what it announces, default
    /// 5000. A record ends the wait at once. `idle_interval_ms` paces idle
    /// rounds on a server that holds no filtered wait. Both are milliseconds,
    /// finite and non-negative.
    #[pyo3(signature = (
        *,
        start=None,
        count=None,
        max_examined=None,
        max_reply_bytes=None,
        max_unacked_pages=None,
        local_guard=false,
        idle_interval_ms=None,
        max_wait_ms=None,
        partitions=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn reader<'py>(
        &self,
        py: Python<'py>,
        start: Option<&Bound<'_, PyAny>>,
        count: Option<u32>,
        max_examined: Option<u32>,
        max_reply_bytes: Option<u32>,
        max_unacked_pages: Option<usize>,
        local_guard: bool,
        idle_interval_ms: Option<f64>,
        max_wait_ms: Option<f64>,
        partitions: Option<Vec<u32>>,
    ) -> PyResult<Awaitable<'py, PyFilteredReader>> {
        let start = start
            .map(filtered_start)
            .transpose()?
            .unwrap_or(FilteredStart::Next);
        let idle_interval = idle_interval_ms
            .map(|ms| duration_ms(ms, "idle_interval_ms"))
            .transpose()?;
        let max_wait = max_wait_ms
            .map(|ms| duration_ms(ms, "max_wait_ms"))
            .transpose()?;
        let group = self.group.clone();
        awaitable(py, async move {
            let mut builder = group
                .reader()
                .map_err(to_pyerr)?
                .start(start)
                .local_guard(local_guard);
            for partition in partitions.unwrap_or_default() {
                builder = builder.partition(partition);
            }
            if let Some(count) = count {
                builder = builder.count(count);
            }
            if let Some(max_examined) = max_examined {
                builder = builder.max_examined(max_examined);
            }
            if let Some(max_reply_bytes) = max_reply_bytes {
                builder = builder.max_reply_bytes(max_reply_bytes);
            }
            if let Some(pages) = max_unacked_pages {
                builder = builder.max_unacked_pages(pages);
            }
            if let Some(idle_interval) = idle_interval {
                builder = builder.idle_interval(idle_interval);
            }
            if let Some(max_wait) = max_wait {
                builder = builder.max_wait(max_wait);
            }
            let reader = builder.build().await.map_err(to_pyerr)?;
            Ok(PyFilteredReader::new(reader))
        })
    }

    fn __repr__(&self) -> String {
        format!("ConsumerGroup({})", label(&self.group))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerGroupInfo {
    /// The native numeric group id.
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    fn name(&self) -> String {
        self.info.name.clone()
    }

    /// The exact group incarnation inside its stream and topic incarnations,
    /// as a dict. A recreated group is another identity and inherits no
    /// policy.
    #[getter]
    fn identity(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.info.identity)
    }

    /// The active policy binding as a dict, `None` for an unbound group whose
    /// readers receive every record.
    #[getter]
    fn filter(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.info
            .filter
            .as_ref()
            .map(|binding| ser_to_py(py, binding))
            .transpose()
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsumerGroupInfo(id={}, name={})",
            self.info.id, self.info.name
        )
    }
}

impl PyGroupFilter {
    fn configure_policy<'py>(
        &self,
        py: Python<'py>,
        operation_id: Option<u128>,
        policy: GroupFilterSpec,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let binding = group
                .filter()
                .configure_as(operation_id, policy)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }
}

// A named definition, required.
fn required_policy(
    filter: Option<PyConsumerFilter>,
    filter_name: Option<String>,
) -> PyResult<GroupFilterSpec> {
    group_policy(filter, filter_name)?
        .ok_or_else(|| InvalidError::new_err("pass `filter` with `filter_name`"))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGroupFilter {
    /// Give the group `filter` as its policy. The definition is saved as the
    /// group's own filter under `name` and the group is bound to it in one
    /// catalog transaction. The name follows the registered filter name rule
    /// and need not be unique. The same digest again keeps the binding and
    /// renames the own filter when `name` differs. Another digest on a group
    /// that runs a policy raises `FilterError` with reason `conflict`: create
    /// a new group for another policy. Returns the binding dict.
    fn configure<'py>(
        &self,
        py: Python<'py>,
        name: String,
        filter: PyConsumerFilter,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        self.configure_policy(
            py,
            None,
            GroupFilterSpec::Definition {
                name,
                filter: filter.inner,
            },
        )
    }

    /// `configure` with a `filter` definition saved under `filter_name`.
    #[pyo3(signature = (*, filter=None, filter_name=None))]
    fn configure_with<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyConsumerFilter>,
        filter_name: Option<String>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let policy = required_policy(filter, filter_name)?;
        self.configure_policy(py, None, policy)
    }

    /// `configure_with` under a caller-chosen `operation_id`, so a caller that
    /// records the id first resumes the same configuration after a crash and
    /// reads its first outcome. `None` mints a fresh id.
    #[pyo3(signature = (operation_id, *, filter=None, filter_name=None))]
    fn configure_as<'py>(
        &self,
        py: Python<'py>,
        operation_id: Option<u128>,
        filter: Option<PyConsumerFilter>,
        filter_name: Option<String>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let policy = required_policy(filter, filter_name)?;
        self.configure_policy(py, operation_id, policy)
    }

    /// The active binding dict, `None` for an unbound group.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let binding = group.filter().get().await.map_err(to_pyerr)?;
            Python::attach(|py| match binding {
                Some(binding) => ser_to_py(py, &binding),
                None => Ok(py.None()),
            })
        })
    }

    /// One page of the group's own filter revisions, newest first. `page`
    /// counts from 1 and `results` is the page length.
    #[pyo3(signature = (*, page=1, results=50))]
    fn revisions<'py>(
        &self,
        py: Python<'py>,
        page: u32,
        results: u32,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let revisions = group
                .filter()
                .revisions(page, results)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revisions))
        })
    }

    /// Pause or resume a revision of the group's own filter. A paused active
    /// revision refuses new reads, while records already delivered can still
    /// be acknowledged.
    fn set_revision_enabled<'py>(
        &self,
        py: Python<'py>,
        revision: u32,
        enabled: bool,
    ) -> PyResult<Awaitable<'py, ()>> {
        let group = self.group.clone();
        awaitable(py, async move {
            group
                .filter()
                .set_revision_enabled(revision, enabled)
                .await
                .map_err(to_pyerr)
        })
    }

    /// The group's own filter as a catalog row dict, bound or released: the
    /// caller's `name`, state, latest revision and the owning group under
    /// `owner`. `None` when the group has no filter of its own.
    fn summary<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let summary = group.filter().summary().await.map_err(to_pyerr)?;
            Python::attach(|py| match summary {
                Some(summary) => ser_to_py(py, &summary),
                None => Ok(py.None()),
            })
        })
    }

    /// Delete the group's own filter with every revision. A bound group is
    /// released first, so its consumers receive every record from their next
    /// poll. Nothing of the filter stays in the catalog. Returns `False` when
    /// the group has no filter of its own.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let group = self.group.clone();
        awaitable(py, async move {
            group.filter().delete().await.map_err(to_pyerr)
        })
    }

    /// Release the group's policy. Its readers then receive every record. A
    /// released group may only be configured with the digest it ran. Returns
    /// the released binding dict.
    fn release<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let released = group.filter().release().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &released))
        })
    }

    /// Preview the active policy over the stored records of one partition. A
    /// preview joins no group and stores no offset.
    #[pyo3(signature = (partition_id, *, from_offset=0, max_examined=None, max_records=None, explain=false))]
    fn preview<'py>(
        &self,
        py: Python<'py>,
        partition_id: u32,
        from_offset: u64,
        max_examined: Option<u32>,
        max_records: Option<u32>,
        explain: bool,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let group = self.group.clone();
        awaitable(py, async move {
            let filter = group.filter();
            let mut request = filter
                .preview(partition_id)
                .await
                .map_err(to_pyerr)?
                .from_offset(from_offset)
                .explain(explain);
            if let Some(max_examined) = max_examined {
                request = request.max_examined(max_examined);
            }
            if let Some(max_records) = max_records {
                request = request.max_records(max_records);
            }
            let preview = request.send().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &preview))
        })
    }

    /// Evaluate the active policy against one supplied `payload` and optional
    /// typed `headers`, and explain the verdict. Nothing is read or stored.
    #[pyo3(signature = (payload, *, headers=None))]
    fn test<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let payload = payload_bytes(payload)?;
        let headers = filter_headers(headers)?;
        let group = self.group.clone();
        awaitable(py, async move {
            let result = group
                .filter()
                .test(payload, headers)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }
}

fn label(group: &ConsumerGroup) -> String {
    match (group.name(), group.id()) {
        (Some(name), _) => name.to_owned(),
        (None, Some(id)) => id.to_string(),
        (None, None) => String::new(),
    }
}

fn group_policy(
    filter: Option<PyConsumerFilter>,
    filter_name: Option<String>,
) -> PyResult<Option<GroupFilterSpec>> {
    match (filter, filter_name) {
        (Some(filter), Some(name)) => Ok(Some(GroupFilterSpec::Definition {
            name,
            filter: filter.inner,
        })),
        (Some(_), None) => Err(InvalidError::new_err(
            "`filter_name` is required with `filter`",
        )),
        (None, Some(_)) => Err(InvalidError::new_err(
            "`filter` is required with `filter_name`",
        )),
        (None, None) => Ok(None),
    }
}
