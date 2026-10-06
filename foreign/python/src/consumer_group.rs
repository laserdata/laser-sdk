use crate::async_bridge::future_into_py;
use crate::convert::{duration_seconds, payload_bytes, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::filters::{PyConsumerFilter, PyFilteredReader, filter_headers, filtered_start};
use crate::transport::{ConsumerConfig, PyConsumer, configure_consumer};
use laser_sdk::filters::{GroupFilterSpec, ReadMode};
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
}

/// A consumer group's filter policy. Build it with `ConsumerGroup.filter`.
/// Every verb needs a managed plane that serves the filter catalog. Catalog
/// replies are dicts in the wire shape.
#[gen_stub_pyclass]
#[pyclass(name = "GroupFilter", frozen)]
pub struct PyGroupFilter {
    group: ConsumerGroup,
}

impl PyConsumerGroup {
    pub(crate) const fn new(group: ConsumerGroup) -> Self {
        Self { group }
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

    /// Create the group, with an optional filter policy configured in the
    /// same call: a `filter` definition saved as the group's own filter, or
    /// one of its own revisions as `filter_id` and `revision`. Idempotent: an
    /// existing group is kept and the same policy keeps its binding. A group
    /// that runs another policy raises `FilterError` with reason `conflict`.
    /// Pass `operation_id` to resume the same configuration after a crash.
    /// Returns a dict with `id`, `name`, `identity`, and `filter` (the
    /// binding, or `None` for an unbound group).
    #[pyo3(signature = (*, filter=None, filter_id=None, revision=None, operation_id=None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyConsumerFilter>,
        filter_id: Option<u32>,
        revision: Option<u32>,
        operation_id: Option<u128>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let policy = group_policy(filter, filter_id, revision)?;
        let group = self.group.clone();
        future_into_py(py, async move {
            let mut create = group.create();
            if let Some(policy) = policy {
                create = create.policy(policy);
            }
            if let Some(operation_id) = operation_id {
                create = create.operation_id(operation_id);
            }
            let info = create.build().await.map_err(to_pyerr)?;
            Python::attach(|py| group_info(py, &info))
        })
    }

    /// The group as the server knows it: `id`, `name`, `identity`, and
    /// `filter` (the active binding, or `None`).
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            let info = group.info().await.map_err(to_pyerr)?;
            Python::attach(|py| group_info(py, &info))
        })
    }

    /// The group's filter policy: configure it, inspect it, draft and pause
    /// revisions, release it, preview and sample-test it.
    fn filter(&self) -> PyGroupFilter {
        PyGroupFilter {
            group: self.group.clone(),
        }
    }

    /// Build a live, load-balanced consumer of this group with server-backed
    /// offsets. On a server that resolves group policies the consumer runs
    /// the group's filter, or none, and commits through the group's fenced
    /// acknowledgments. On Apache Iggy it is the native group consumer. It
    /// supports the same polling, batching, replay, retry, and commit modes
    /// as the Rust builder. Use `auto_commit="disabled"` plus
    /// `commit(message)` for commit-after-handle delivery. `batch_length`
    /// also bounds the source records one partition poll examines.
    #[pyo3(signature = (*, batch_length=1000, poll_interval_ms=None, polling="next", offset=None, timestamp_micros=None, auto_commit="polling", commit_interval_ms=0, commit_every=None, auto_join_group=true, create_group=true, polling_retry_interval_ms=1000, init_retries=None, init_retry_interval_ms=1000, allow_replay=false))]
    #[allow(clippy::too_many_arguments)]
    fn consumer(
        &self,
        batch_length: u32,
        poll_interval_ms: Option<u64>,
        polling: &str,
        offset: Option<u64>,
        timestamp_micros: Option<u64>,
        auto_commit: &str,
        commit_interval_ms: u64,
        commit_every: Option<u32>,
        auto_join_group: bool,
        create_group: bool,
        polling_retry_interval_ms: u64,
        init_retries: Option<u32>,
        init_retry_interval_ms: u64,
        allow_replay: bool,
    ) -> PyResult<PyConsumer> {
        configure_consumer(
            self.group.consumer(),
            label(&self.group),
            ConsumerConfig {
                batch_length,
                poll_interval_ms,
                polling,
                offset,
                timestamp_micros,
                auto_commit,
                commit_interval_ms,
                commit_every,
                auto_join_group,
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
    /// `start` is `next` (the default), `first`, or `last`, or use
    /// `start_offset` / `start_timestamp_micros`.
    /// `count` bounds records per page, default 100. `max_examined` bounds
    /// the source records one page examines, independent of `count`.
    /// `max_reply_bytes` bounds record bytes per page.
    /// `max_unacked_pages` bounds outstanding pages per partition, default 1024.
    /// `read_mode` is primary or local. Local reads cannot acknowledge.
    /// `local_guard` checks delivered records against the filter locally.
    /// `idle_interval` is seconds, finite and non-negative.
    #[pyo3(signature = (
        *,
        start="next",
        start_offset=None,
        start_timestamp_micros=None,
        count=None,
        max_examined=None,
        max_reply_bytes=None,
        max_unacked_pages=None,
        read_mode="primary",
        local_guard=false,
        idle_interval=None,
        partitions=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn reader<'py>(
        &self,
        py: Python<'py>,
        start: &str,
        start_offset: Option<u64>,
        start_timestamp_micros: Option<u64>,
        count: Option<u32>,
        max_examined: Option<u32>,
        max_reply_bytes: Option<u32>,
        max_unacked_pages: Option<usize>,
        read_mode: &str,
        local_guard: bool,
        idle_interval: Option<f64>,
        partitions: Option<Vec<u32>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = filtered_start(start, start_offset, start_timestamp_micros)?;
        let read_mode = match read_mode {
            "primary" => ReadMode::Primary,
            "local" => ReadMode::Local,
            other => {
                return Err(InvalidError::new_err(format!(
                    "read_mode must be `primary` or `local`, got `{other}`"
                )));
            }
        };
        let idle_interval = idle_interval
            .map(|seconds| duration_seconds(seconds, "idle_interval"))
            .transpose()?;
        let group = self.group.clone();
        future_into_py(py, async move {
            let mut builder = group
                .reader()
                .map_err(to_pyerr)?
                .start(start)
                .read_mode(read_mode)
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
impl PyGroupFilter {
    /// Give the group its policy: a `filter` definition saved as the group's
    /// own filter, or one of its own revisions as `filter_id` and `revision`.
    /// The group is bound in one catalog transaction. The same digest again
    /// keeps the binding. Another digest on a group that runs a policy raises
    /// `FilterError` with reason `conflict`: create a new group for another
    /// policy. Pass `operation_id` to resume the same configuration after a
    /// crash. Returns the binding dict.
    #[pyo3(signature = (filter=None, *, filter_id=None, revision=None, operation_id=None))]
    fn configure<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyConsumerFilter>,
        filter_id: Option<u32>,
        revision: Option<u32>,
        operation_id: Option<u128>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let policy = group_policy(filter, filter_id, revision)?.ok_or_else(|| {
            InvalidError::new_err("pass `filter`, or both `filter_id` and `revision`")
        })?;
        let group = self.group.clone();
        future_into_py(py, async move {
            let binding = group
                .filter()
                .configure_as(operation_id, policy)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }

    /// The active binding dict, `None` for an unbound group.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            let binding = group.filter().get().await.map_err(to_pyerr)?;
            Python::attach(|py| match binding {
                Some(binding) => ser_to_py(py, &binding),
                None => Ok(py.None()),
            })
        })
    }

    /// One page of the group's own filter revisions, newest first.
    #[pyo3(signature = (*, page=0, page_size=50))]
    fn revisions<'py>(
        &self,
        py: Python<'py>,
        page: u32,
        page_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            let revisions = group
                .filter()
                .revisions(page, page_size)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revisions))
        })
    }

    /// Draft a revision of the group's own filter. Readers keep running the
    /// active revision. `expected_revision` must still be the latest.
    fn revise<'py>(
        &self,
        py: Python<'py>,
        expected_revision: u32,
        filter: PyConsumerFilter,
    ) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            let revised = group
                .filter()
                .revise(expected_revision, filter.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revised))
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            group
                .filter()
                .set_revision_enabled(revision, enabled)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Delete the group's own filter with every revision. A bound group is
    /// released first, so its consumers receive every record from their next
    /// poll. Nothing of the filter stays in the catalog. Returns `False` when
    /// the group has no filter of its own.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
            group.filter().delete().await.map_err(to_pyerr)
        })
    }

    /// Release the group's policy. Its readers then receive every record. A
    /// released group may only be configured with the digest it ran. Returns
    /// the released binding dict.
    fn release<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let group = self.group.clone();
        future_into_py(py, async move {
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
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = payload_bytes(payload)?;
        let headers = filter_headers(headers)?;
        let group = self.group.clone();
        future_into_py(py, async move {
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
    filter_id: Option<u32>,
    revision: Option<u32>,
) -> PyResult<Option<GroupFilterSpec>> {
    match (filter, filter_id, revision) {
        (Some(filter), None, None) => Ok(Some(GroupFilterSpec::Definition(filter.inner))),
        (None, Some(filter_id), Some(revision)) => Ok(Some(GroupFilterSpec::Revision {
            filter_id,
            revision,
        })),
        (None, None, None) => Ok(None),
        _ => Err(InvalidError::new_err(
            "pass either `filter`, or both `filter_id` and `revision`",
        )),
    }
}

fn group_info(py: Python<'_>, info: &ConsumerGroupInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("id", info.id)?;
    dict.set_item("name", &info.name)?;
    dict.set_item("identity", ser_to_py(py, &info.identity)?)?;
    dict.set_item(
        "filter",
        match &info.filter {
            Some(binding) => ser_to_py(py, binding)?,
            None => py.None(),
        },
    )?;
    Ok(dict.into_any().unbind())
}
