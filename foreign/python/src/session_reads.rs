use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::context::PyContextMessage;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::session::{PySession, PySessions};
use laser_sdk::agent::{SessionChange, SessionWatch};
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

fn session_id(value: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(value).map_err(|error| to_pyerr(error.into()))
}

fn word<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: Option<String>,
) -> PyResult<Option<T>> {
    value
        .map(|value| py_to_de(&value.into_pyobject(py)?.into_any()))
        .transpose()
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessions {
    /// One session's summary from the deployment's session index, as a
    /// `SessionInfo` dict.
    fn get<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        let id = session_id(id)?;
        future_into_py(py, async move {
            let info = sessions.get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &info))
        })
    }

    /// A page of this stream's sessions, newest first, as a `SessionPage`
    /// dict. `status` is a status word such as `active`, `root` a session id
    /// whose tree to list, `label_prefix` the start of the label, `agent` an
    /// agent id, `text` a label or id substring, and a `limit` of zero leaves
    /// the page size to the server. `total=True` also counts every match.
    #[pyo3(signature = (*, status=None, root=None, label_prefix=None, agent=None, text=None, cursor=None, limit=0, total=false))]
    #[allow(clippy::too_many_arguments)]
    fn list<'py>(
        &self,
        py: Python<'py>,
        status: Option<String>,
        root: Option<String>,
        label_prefix: Option<String>,
        agent: Option<String>,
        text: Option<String>,
        cursor: Option<String>,
        limit: u32,
        total: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut request = self.inner.list().limit(limit);
        if let Some(root) = root {
            request = request.root(session_id(&root)?);
        }
        if let Some(prefix) = label_prefix {
            request = request.label_prefix(prefix);
        }
        if let Some(status) = word(py, status)? {
            request = request.status(status);
        }
        if let Some(agent) = agent {
            request = request.agent(
                agent
                    .parse::<laser_sdk::wire::agent::AgentId>()
                    .map_err(|_| InvalidError::new_err(format!("invalid agent id `{agent}`")))?,
            );
        }
        if let Some(text) = text {
            request = request.text(text);
        }
        if let Some(cursor) = cursor {
            request = request.cursor(cursor);
        }
        if total {
            request = request.total();
        }
        future_into_py(py, async move {
            let page = request.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &page))
        })
    }

    /// A page of one session's events in broker time order, as a
    /// `SessionEventsPage` dict. `fixed_frontier=True` pins the fold frontier
    /// of the first page for a historical walk.
    #[pyo3(signature = (id, *, cursor=None, limit=0, fixed_frontier=false))]
    fn events<'py>(
        &self,
        py: Python<'py>,
        id: &str,
        cursor: Option<String>,
        limit: u32,
        fixed_frontier: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut request = self.inner.events(session_id(id)?).limit(limit);
        if let Some(cursor) = cursor {
            request = request.cursor(cursor);
        }
        if fixed_frontier {
            request = request.fixed_frontier();
        }
        future_into_py(py, async move {
            let page = request.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &page))
        })
    }

    /// One session's folded state document with up to `history_limit`
    /// history rows, as a `SessionStateView` dict. Zero leaves the history
    /// size to the server.
    #[pyo3(signature = (id, history_limit=0))]
    fn state<'py>(
        &self,
        py: Python<'py>,
        id: &str,
        history_limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        let id = session_id(id)?;
        future_into_py(py, async move {
            let view = sessions.state(id, history_limit).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &view))
        })
    }

    /// The resources one session wrote, recalled, or touched, as a
    /// `SessionLinksView` dict. `surface` narrows them, for example `memory`
    /// or `kv`.
    #[pyo3(signature = (id, surface=None))]
    fn links<'py>(
        &self,
        py: Python<'py>,
        id: &str,
        surface: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        let id = session_id(id)?;
        let surface = word(py, surface)?;
        future_into_py(py, async move {
            let view = sessions.links(id, surface).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &view))
        })
    }

    /// The source partitions that hold one session's records, as a
    /// `SessionSources` dict.
    fn sources<'py>(&self, py: Python<'py>, id: &str) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        let id = session_id(id)?;
        future_into_py(py, async move {
            let sources = sessions.sources(id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &sources))
        })
    }

    /// This stream's change rows after `after`, as a `SessionChanges` dict.
    /// A `limit` of zero leaves the page size to the server.
    #[pyo3(signature = (after=0, limit=0))]
    fn changes<'py>(&self, py: Python<'py>, after: u64, limit: u32) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        future_into_py(py, async move {
            let changes = sessions.changes(after, limit).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &changes))
        })
    }

    /// Follow this stream's session changes from now, polling every
    /// `poll_every_ms` milliseconds. Returns a `SessionWatch` that starts at
    /// the change rows retained when this call returns. Await `next()` on it.
    fn watch<'py>(&self, py: Python<'py>, poll_every_ms: f64) -> PyResult<Bound<'py, PyAny>> {
        let poll_every = crate::convert::duration_ms(poll_every_ms, "poll_every_ms")?;
        let sessions = self.inner.clone();
        future_into_py(py, async move {
            let watch = sessions.watch(poll_every).await.map_err(to_pyerr)?;
            Ok(PySessionWatch {
                inner: Arc::new(Mutex::new(watch)),
            })
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySession {
    /// This session's summary from the deployment's session index, as a
    /// `SessionInfo` dict.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            let info = session.status().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &info))
        })
    }

    /// Whether this session's usage has passed its budget: the summed input
    /// and output tokens of its records over the token ceiling, or their
    /// summed cost over the cost ceiling. A deployment that indexes sessions
    /// answers from its index. On open Apache Iggy, or for a session the
    /// index does not know yet, the retained session lane is folded. A
    /// session without a budget is never over it.
    fn over_budget<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(
            py,
            async move { session.over_budget().await.map_err(to_pyerr) },
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Read the one record `at` names, a message source reference dict.
    /// Returns a `ContextMessage`, or None when the record is gone or the
    /// topic was recreated.
    fn read_at<'py>(&self, py: Python<'py>, at: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let at: laser_sdk::wire::graph::SourceRef = py_to_de(at)?;
        future_into_py(py, async move {
            let record = laser.read_at(&at).await.map_err(to_pyerr)?;
            Ok(record.map(PyContextMessage::new))
        })
    }
}

/// What a session watch reports.
#[gen_stub_pyclass_complex_enum]
#[pyclass(name = "SessionChange", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub enum PySessionChange {
    /// These sessions changed, each named once.
    Changed { sessions: Vec<String> },
    /// The watch fell below the retained change floor. List the sessions
    /// again to catch up.
    Resync(),
}

/// A follower of one stream's session changes. Build it with
/// `Sessions.watch`.
#[gen_stub_pyclass]
#[pyclass(name = "SessionWatch")]
pub struct PySessionWatch {
    inner: Arc<Mutex<SessionWatch>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionWatch {
    /// The next change, as a `SessionChange`, waiting until one lands.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let watch = self.inner.clone();
        future_into_py(py, async move {
            let change = watch.lock().await.next().await.map_err(to_pyerr)?;
            Ok(match change {
                SessionChange::Changed(sessions) => PySessionChange::Changed {
                    sessions: sessions.iter().map(ToString::to_string).collect(),
                },
                SessionChange::Resync => PySessionChange::Resync(),
            })
        })
    }
}
