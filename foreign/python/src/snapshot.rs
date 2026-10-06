use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::errors::to_pyerr;
use laser_sdk::snapshot::{FoldSnapshot, KvSnapshotStore, SnapshotStore, TopicSnapshotStore};
use laser_sdk::wire::agent::ConversationId;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
enum Kind {
    Kv(Arc<KvSnapshotStore>),
    Topic(Arc<TopicSnapshotStore>),
    Custom(Arc<Py<PyAny>>),
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A snapshot store in the managed key-value view. The namespace defaults to `agent.snapshots`. Apache Iggy raises `UnsupportedError` on its verbs.
    #[pyo3(signature = (namespace=None))]
    fn kv_snapshot_store(&self, namespace: Option<String>) -> PySnapshotStore {
        PySnapshotStore {
            kind: Kind::Kv(Arc::new(KvSnapshotStore::in_namespace(
                self.inner.clone(),
                namespace
                    .unwrap_or_else(|| laser_sdk::snapshot::DEFAULT_SNAPSHOT_NAMESPACE.to_owned()),
            ))),
        }
    }

    /// A snapshot store on a dedicated topic, default `agent.snapshots`. Works on Apache Iggy. `latest` scans backward from the tail, so keep the topic on retention.
    #[pyo3(signature = (topic=None))]
    fn topic_snapshot_store(&self, topic: Option<String>) -> PySnapshotStore {
        PySnapshotStore {
            kind: Kind::Topic(Arc::new(TopicSnapshotStore::on_topic(
                self.inner.clone(),
                topic.unwrap_or_else(|| laser_sdk::snapshot::DEFAULT_SNAPSHOT_TOPIC.to_owned()),
            ))),
        }
    }
}

/// A fold snapshot store. Build a native store from `Laser`, or wrap a custom backend with `SnapshotStore(backend)`. Its `latest(conversation)` and `save(snapshot)` callbacks can return directly or through an awaitable.
#[gen_stub_pyclass]
#[pyclass(name = "SnapshotStore")]
pub struct PySnapshotStore {
    kind: Kind,
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
            Kind::Custom(store) => {
                let value = crate::memory::call_hook_cancellable(|py| {
                    store
                        .bind(py)
                        .call_method1("latest", (conversation.to_string(),))
                        .map(Bound::unbind)
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
            Kind::Custom(store) => {
                crate::memory::call_hook_cancellable(|py| {
                    let snapshot = snapshot_to_py(py, snapshot)?;
                    store
                        .bind(py)
                        .call_method1("save", (snapshot,))
                        .map(Bound::unbind)
                })
                .await
                .map_err(crate::errors::from_callback_error)?;
                Ok(())
            }
        }
    }
}

impl PySnapshotStore {
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
            kind: Kind::Custom(Arc::new(backend.clone().unbind())),
        })
    }

    /// The newest snapshot as `{"conversation": str, "as_of": {partition: offset}, "state": bytes}`, or `None`. The offsets are inclusive.
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

    /// Save opaque folded bytes for `conversation`. `as_of` maps each partition to the last folded offset, inclusive. Custom callbacks receive the complete snapshot dict.
    fn save<'py>(
        &self,
        py: Python<'py>,
        conversation: String,
        as_of: BTreeMap<u32, u64>,
        state: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conversation = ConversationId::from_str(&conversation)
            .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))?;
        let store = self.handle();
        future_into_py(py, async move {
            store
                .save(&FoldSnapshot {
                    conversation,
                    as_of,
                    state,
                })
                .await
                .map_err(to_pyerr)
        })
    }
}

pub(crate) fn snapshot_to_py<'py>(
    py: Python<'py>,
    snapshot: &FoldSnapshot,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("conversation", snapshot.conversation.to_string())?;
    let as_of = PyDict::new(py);
    for (partition, offset) in &snapshot.as_of {
        as_of.set_item(partition, offset)?;
    }
    dict.set_item("as_of", as_of)?;
    dict.set_item("state", pyo3::types::PyBytes::new(py, &snapshot.state))?;
    Ok(dict.into_any())
}

pub(crate) fn snapshot_from_py(value: &Bound<'_, PyAny>) -> PyResult<FoldSnapshot> {
    let dict = value.cast::<PyDict>()?;
    let field = |name: &str| {
        dict.get_item(name)?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("snapshot is missing {name}"))
        })
    };
    let conversation = field("conversation")?.extract::<String>()?;
    Ok(FoldSnapshot {
        conversation: ConversationId::from_str(&conversation)
            .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))?,
        as_of: field("as_of")?.extract::<BTreeMap<u32, u64>>()?,
        state: field("state")?.extract::<Vec<u8>>()?,
    })
}
