use crate::client::PyLaser;
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::snapshot::{FoldSnapshot, KvSnapshotStore, SnapshotStore, TopicSnapshotStore};
use laser_sdk::wire::agent::ConversationId;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::str::FromStr;

// Which backend a `PySnapshotStore` builds per call. `SnapshotStore` is an RPITIT
// trait (not object-safe), so the concrete store is rebuilt from the `Laser` (a
// cheap Arc clone) and the name each call, rather than boxed.
#[derive(Clone)]
enum Kind {
    Kv(String),
    Topic(String),
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A fold-snapshot store in the managed key-value store (one key per
    /// conversation in `namespace`, default `agent.snapshots`), so a long
    /// conversation resumes from its last checkpoint instead of replaying every
    /// record. Managed: against raw Apache Iggy the calls raise `UnsupportedError`
    /// - use `topic_snapshot_store` there.
    #[pyo3(signature = (namespace=None))]
    fn kv_snapshot_store(&self, namespace: Option<String>) -> PySnapshotStore {
        PySnapshotStore {
            laser: self.inner.clone(),
            kind: Kind::Kv(
                namespace
                    .unwrap_or_else(|| laser_sdk::snapshot::DEFAULT_SNAPSHOT_NAMESPACE.to_owned()),
            ),
        }
    }

    /// A fold-snapshot store as records on a dedicated snapshots `topic` (default
    /// `agent.snapshots`), partitioned by conversation. Log-native: works on raw
    /// Apache Iggy. `latest` walks the topic backward to the newest checkpoint, so
    /// keep the topic on retention.
    #[pyo3(signature = (topic=None))]
    fn topic_snapshot_store(&self, topic: Option<String>) -> PySnapshotStore {
        PySnapshotStore {
            laser: self.inner.clone(),
            kind: Kind::Topic(
                topic.unwrap_or_else(|| laser_sdk::snapshot::DEFAULT_SNAPSHOT_TOPIC.to_owned()),
            ),
        }
    }
}

/// A fold-snapshot store: `save` a checkpoint, `latest` the newest for a
/// conversation. Build with `Laser.kv_snapshot_store` / `topic_snapshot_store`.
#[gen_stub_pyclass]
#[pyclass(name = "SnapshotStore")]
pub struct PySnapshotStore {
    laser: Laser,
    kind: Kind,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySnapshotStore {
    /// The newest snapshot for `conversation` as a dict `{"conversation": str,
    /// "as_of": {partition: offset}, "state": bytes}`, or `None` when it has never
    /// been snapshotted.
    fn latest<'py>(&self, py: Python<'py>, conversation: String) -> PyResult<Bound<'py, PyAny>> {
        let conversation = ConversationId::from_str(&conversation)
            .map_err(|e| crate::errors::InvalidError::new_err(e.to_string()))?;
        let laser = self.laser.clone();
        let kind = self.kind.clone();
        future_into_py(py, async move {
            let snapshot = match kind {
                Kind::Kv(namespace) => {
                    KvSnapshotStore::in_namespace(laser, namespace)
                        .latest(conversation)
                        .await
                }
                Kind::Topic(topic) => {
                    TopicSnapshotStore::on_topic(laser, topic)
                        .latest(conversation)
                        .await
                }
            }
            .map_err(to_pyerr)?;
            Python::attach(|py| match snapshot {
                Some(snapshot) => Ok(Some(snapshot_to_py(py, &snapshot)?.unbind())),
                None => Ok(None),
            })
        })
    }

    /// Persist a checkpoint for `conversation`: `as_of` is the per-partition last
    /// folded offset (inclusive), `state` the opaque folded bytes (any codec).
    fn save<'py>(
        &self,
        py: Python<'py>,
        conversation: String,
        as_of: BTreeMap<u32, u64>,
        state: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conversation = ConversationId::from_str(&conversation)
            .map_err(|e| crate::errors::InvalidError::new_err(e.to_string()))?;
        let laser = self.laser.clone();
        let kind = self.kind.clone();
        future_into_py(py, async move {
            let snapshot = FoldSnapshot {
                conversation,
                as_of,
                state,
            };
            match kind {
                Kind::Kv(namespace) => {
                    KvSnapshotStore::in_namespace(laser, namespace)
                        .save(&snapshot)
                        .await
                }
                Kind::Topic(topic) => {
                    TopicSnapshotStore::on_topic(laser, topic)
                        .save(&snapshot)
                        .await
                }
            }
            .map_err(to_pyerr)
        })
    }
}

// A `FoldSnapshot` as a Python dict.
fn snapshot_to_py<'py>(py: Python<'py>, snapshot: &FoldSnapshot) -> PyResult<Bound<'py, PyAny>> {
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
