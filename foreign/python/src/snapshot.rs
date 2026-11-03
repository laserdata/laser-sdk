use crate::async_bridge::{HookLoop, call_hook, future_into_py};
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::snapshot::{FoldSnapshot, KvSnapshotStore, SnapshotStore, TopicSnapshotStore};
use laser_sdk::wire::agent::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
enum Kind {
    Kv(Arc<KvSnapshotStore>),
    Topic(Arc<TopicSnapshotStore>),
    Custom(Arc<Py<PyAny>>, HookLoop),
}

/// A fold snapshot store. `KvSnapshotStore` and `TopicSnapshotStore` are the native stores. `SnapshotStore(backend)` wraps a custom backend whose `latest(conversation)` and `save(snapshot)` callbacks can return directly or through an awaitable.
#[gen_stub_pyclass]
#[pyclass(name = "SnapshotStore", subclass)]
pub struct PySnapshotStore {
    kind: Kind,
}

/// Fold snapshots in the managed key-value store, one key per conversation in a dedicated namespace (default `agent.snapshots`). Apache Iggy raises `UnsupportedError` on its verbs, so pick `TopicSnapshotStore` there.
#[gen_stub_pyclass]
#[pyclass(name = "KvSnapshotStore", extends = PySnapshotStore)]
pub struct PyKvSnapshotStore;

#[gen_stub_pymethods]
#[pymethods]
impl PyKvSnapshotStore {
    /// A store over the default `agent.snapshots` namespace.
    #[new]
    fn new(laser: PyRef<'_, PyLaser>, fold: String) -> PyClassInitializer<PyKvSnapshotStore> {
        let store = KvSnapshotStore::new(laser.inner.clone(), fold);
        PyClassInitializer::from(PySnapshotStore::native(Kind::Kv(Arc::new(store))))
            .add_subclass(PyKvSnapshotStore)
    }

    /// A store over `namespace`, for keeping several folds' snapshots apart.
    #[staticmethod]
    fn in_namespace(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        namespace: String,
        fold: String,
    ) -> PyResult<Py<PyKvSnapshotStore>> {
        let store = KvSnapshotStore::in_namespace(laser.inner.clone(), namespace, fold);
        Py::new(
            py,
            PyClassInitializer::from(PySnapshotStore::native(Kind::Kv(Arc::new(store))))
                .add_subclass(PyKvSnapshotStore),
        )
    }
}

/// Fold snapshots as records on a dedicated topic (default `agent.snapshots`), partitioned by conversation. Works on Apache Iggy. `latest` scans backward from the tail, so keep the topic on retention.
#[gen_stub_pyclass]
#[pyclass(name = "TopicSnapshotStore", extends = PySnapshotStore)]
pub struct PyTopicSnapshotStore;

#[gen_stub_pymethods]
#[pymethods]
impl PyTopicSnapshotStore {
    /// A store over the default `agent.snapshots` topic.
    #[new]
    fn new(laser: PyRef<'_, PyLaser>, fold: String) -> PyClassInitializer<PyTopicSnapshotStore> {
        let store = TopicSnapshotStore::new(laser.inner.clone(), fold);
        PyClassInitializer::from(PySnapshotStore::native(Kind::Topic(Arc::new(store))))
            .add_subclass(PyTopicSnapshotStore)
    }

    /// A store over `topic`, for keeping several folds' snapshots apart.
    #[staticmethod]
    fn on_topic(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        topic: String,
        fold: String,
    ) -> PyResult<Py<PyTopicSnapshotStore>> {
        let store = TopicSnapshotStore::on_topic(laser.inner.clone(), topic, fold);
        Py::new(
            py,
            PyClassInitializer::from(PySnapshotStore::native(Kind::Topic(Arc::new(store))))
                .add_subclass(PyTopicSnapshotStore),
        )
    }
}

/// The offset a partition's resume reads from: one past the last offset
/// `snapshot` folded, or `0` for a partition it did not cover.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn fold_snapshot_resume_offset(
    snapshot: &Bound<'_, PyAny>,
    topic_id: u32,
    topic_created_at_micros: u64,
    partition_id: u32,
) -> PyResult<u64> {
    Ok(snapshot_from_py(snapshot)?.resume_offset(topic_id, topic_created_at_micros, partition_id))
}

#[derive(Clone)]
pub(crate) struct SnapshotHandle {
    kind: Kind,
}

impl SnapshotHandle {
    pub(crate) fn from_py(store: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(store) = store.extract::<PyRef<'_, PySnapshotStore>>() {
            return Ok(store.handle());
        }
        Ok(PySnapshotStore::new(store)?.handle())
    }

    pub(crate) async fn latest(
        &self,
        conversation: ConversationId,
    ) -> Result<Option<FoldSnapshot>, laser_sdk::LaserError> {
        match &self.kind {
            Kind::Kv(store) => store.latest(conversation).await,
            Kind::Topic(store) => store.latest(conversation).await,
            Kind::Custom(store, fallback) => {
                let value = call_hook(fallback, |call| {
                    call.call_method(store.bind(call.py()), "latest", (conversation.to_string(),))
                })
                .await
                .map_err(crate::errors::from_callback_error)?;
                Python::attach(|py| {
                    if value.bind(py).is_none() {
                        Ok(None)
                    } else {
                        snapshot_from_py(value.bind(py)).map(Some)
                    }
                })
                .map_err(|error| {
                    laser_sdk::LaserError::HandlerConfig(format!(
                        "snapshot latest must return None or a snapshot dict: {error}"
                    ))
                })
            }
        }
    }

    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), laser_sdk::LaserError> {
        match &self.kind {
            Kind::Kv(store) => store.save(snapshot).await,
            Kind::Topic(store) => store.save(snapshot).await,
            Kind::Custom(store, fallback) => {
                call_hook(fallback, |call| {
                    let snapshot = snapshot_to_py(call.py(), snapshot)?;
                    call.call_method(store.bind(call.py()), "save", (snapshot,))
                })
                .await
                .map_err(crate::errors::from_callback_error)?;
                Ok(())
            }
        }
    }
}

impl PySnapshotStore {
    fn native(kind: Kind) -> Self {
        Self { kind }
    }

    pub(crate) fn handle(&self) -> SnapshotHandle {
        SnapshotHandle {
            kind: self.kind.clone(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySnapshotStore {
    #[new]
    fn new(backend: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(store) = backend.extract::<PyRef<'_, PySnapshotStore>>() {
            return Ok(Self {
                kind: store.kind.clone(),
            });
        }
        for name in ["latest", "save"] {
            let callback = backend.getattr(name).map_err(|error| {
                if error.is_instance_of::<pyo3::exceptions::PyAttributeError>(backend.py()) {
                    crate::errors::ConfigError::new_err(format!(
                        "snapshot backend must define {name}"
                    ))
                } else {
                    error
                }
            })?;
            if !callback.is_callable() {
                return Err(crate::errors::ConfigError::new_err(format!(
                    "snapshot backend {name} must be callable"
                )));
            }
        }
        Ok(Self {
            kind: Kind::Custom(
                Arc::new(backend.clone().unbind()),
                HookLoop::capture(backend.py()),
            ),
        })
    }

    /// The newest snapshot as the dict `decode_snapshot` returns (stream, stream id and creation time, conversation, fold, `as_of` entries of `(topic_id, topic_created_at_micros, partition_id, offset)`, and state), or `None`. The offsets are inclusive.
    fn latest<'py>(&self, py: Python<'py>, conversation: String) -> PyResult<Bound<'py, PyAny>> {
        let conversation = ConversationId::from_str(&conversation)
            .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))?;
        let store = self.handle();
        future_into_py(py, async move {
            let snapshot = store.latest(conversation).await.map_err(to_pyerr)?;
            Python::attach(|py| match snapshot {
                Some(snapshot) => Ok(Some(snapshot_to_py(py, &snapshot)?.unbind())),
                None => Ok(None),
            })
        })
    }

    /// Save a snapshot with a stream, fold, and source generation.
    fn save<'py>(
        &self,
        py: Python<'py>,
        snapshot: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let snapshot = snapshot_from_py(snapshot)?;
        let store = self.handle();
        future_into_py(
            py,
            async move { store.save(&snapshot).await.map_err(to_pyerr) },
        )
    }
}

// A fold snapshot crosses as its serde dict: `conversation` (str), `as_of`
// (partition to last folded offset, inclusive), and `state` (bytes).
pub(crate) fn snapshot_to_py<'py>(
    py: Python<'py>,
    snapshot: &FoldSnapshot,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(ser_to_py(py, snapshot)?.into_bound(py))
}

pub(crate) fn snapshot_from_py(value: &Bound<'_, PyAny>) -> PyResult<FoldSnapshot> {
    py_to_de(value)
}
