use crate::client::PyLaser;
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::wire::agent_workflow::{AgentRunInfo, AgentRunState, RunBudget, RunPage};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::str::FromStr;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A handle to the managed run registry: submit a run to an agent or
    /// workflow, cancel it, read its state, or list runs. The registry is a
    /// managed feature: against raw Apache Iggy every operation raises
    /// `UnsupportedError`.
    fn runs(&self) -> PyRuns {
        PyRuns {
            laser: self.inner.clone(),
        }
    }
}

/// A handle to the managed run registry.
#[gen_stub_pyclass]
#[pyclass(name = "Runs", frozen)]
pub struct PyRuns {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRuns {
    /// Submit `input` (str, bytes, or bytearray) to the agent `agent_id`,
    /// returning the run's metadata. The backend mints the run id by
    /// content-addressing the submit identity, so a retried submit converges
    /// on the same run.
    fn submit<'py>(
        &self,
        py: Python<'py>,
        agent_id: String,
        input: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let input = payload_bytes(input)?;
        future_into_py(py, async move {
            let info = laser
                .runs()
                .submit(agent_id, input)
                .await
                .map_err(to_pyerr)?;
            Ok(PyRunInfo::from(info))
        })
    }

    /// Submit with a caller-assigned `run_id`, explicit `params`, and optional
    /// `input`, for full control over the run request. An absent `run_id` lets
    /// the backend mint one from the submit identity.
    #[pyo3(signature = (agent_id, *, run_id=None, input=None, params=None))]
    fn submit_with<'py>(
        &self,
        py: Python<'py>,
        agent_id: String,
        run_id: Option<String>,
        input: Option<&Bound<'_, PyAny>>,
        params: Option<BTreeMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let input = input.map(payload_bytes).transpose()?;
        future_into_py(py, async move {
            let info = laser
                .runs()
                .submit_with(agent_id, run_id, input, params.unwrap_or_default())
                .await
                .map_err(to_pyerr)?;
            Ok(PyRunInfo::from(info))
        })
    }

    /// Submit with a multi-dimensional per-run [`RunBudget`]. A run that crosses
    /// any cap is failed by the run governor, surfaced as `BudgetExceededError`.
    /// Each cap is a keyword and absent means unbounded on that dimension.
    #[pyo3(signature = (agent_id, *, input=None, budget))]
    fn submit_budgeted<'py>(
        &self,
        py: Python<'py>,
        agent_id: String,
        input: Option<&Bound<'_, PyAny>>,
        budget: PyRunBudget,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let input = input.map(payload_bytes).transpose()?;
        let budget = budget.into();
        future_into_py(py, async move {
            let info = laser
                .runs()
                .submit_budgeted(agent_id, input, budget)
                .await
                .map_err(to_pyerr)?;
            Ok(PyRunInfo::from(info))
        })
    }

    /// Record the cancel intent on `run_id` and return the run. The engine
    /// observes the intent at its next step boundary, so the state moves only
    /// when the engine reports it.
    fn cancel<'py>(&self, py: Python<'py>, run_id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let info = laser.runs().cancel(run_id).await.map_err(to_pyerr)?;
            Ok(PyRunInfo::from(info))
        })
    }

    /// Read the current state of `run_id`.
    fn status<'py>(&self, py: Python<'py>, run_id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let info = laser.runs().status(run_id).await.map_err(to_pyerr)?;
            Ok(PyRunInfo::from(info))
        })
    }

    /// Register `stream/topic` as a run-status source: LaserData Cloud folds
    /// the run-tagged agent records published there into the run registry.
    /// A control command with 202-accepted semantics, idempotent by source.
    fn register_source<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser
                .runs()
                .register_source(stream, topic)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Stop folding run-status records from `stream/topic`. Idempotent.
    fn remove_source<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser
                .runs()
                .remove_source(stream, topic)
                .await
                .map_err(to_pyerr)
        })
    }

    /// List runs, newest first, one page per call. Optional filters narrow the
    /// page: `agent_id`, `state` (a pinned snake-case word, an unknown word
    /// raises `ValueError`), `limit` (clamped server-side to the wire page
    /// cap), and `cursor` (the opaque continuation from the previous page).
    #[pyo3(signature = (*, agent_id=None, state=None, limit=None, cursor=None))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        agent_id: Option<String>,
        state: Option<String>,
        limit: Option<u32>,
        cursor: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let state = state.as_deref().map(parse_state).transpose()?;
        let cursor = cursor.map(payload_bytes).transpose()?;
        future_into_py(py, async move {
            let runs = laser.runs();
            let mut request = runs.list();
            if let Some(agent_id) = agent_id {
                request = request.agent(agent_id);
            }
            if let Some(state) = state {
                request = request.state(state);
            }
            if let Some(limit) = limit {
                request = request.limit(limit);
            }
            if let Some(cursor) = cursor {
                request = request.cursor(cursor);
            }
            let page = request.fetch().await.map_err(to_pyerr)?;
            Ok(PyRunPage::from(page))
        })
    }
}

/// One run's metadata: identity, lifecycle state (as its pinned snake-case
/// word), timestamps, the terminal `detail` summary, and the recorded cancel
/// intent (`cancel_requested` is the intent, not a state: the state moves only
/// when the engine reports it).
#[gen_stub_pyclass]
#[pyclass(name = "RunInfo", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyRunInfo {
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub agent_id: String,
    #[pyo3(get)]
    pub user_id: u32,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub created_at_micros: u64,
    #[pyo3(get)]
    pub updated_at_micros: u64,
    #[pyo3(get)]
    pub detail: Option<String>,
    #[pyo3(get)]
    pub cancel_requested: bool,
}

impl From<AgentRunInfo> for PyRunInfo {
    fn from(info: AgentRunInfo) -> Self {
        Self {
            run_id: info.run_id,
            agent_id: info.agent_id,
            user_id: info.user_id,
            state: info.state.as_str().to_owned(),
            created_at_micros: info.created_at_micros,
            updated_at_micros: info.updated_at_micros,
            detail: info.detail,
            cancel_requested: info.cancel_requested,
        }
    }
}

/// A page of runs plus the cursor to resume after the last one.
#[gen_stub_pyclass]
#[pyclass(name = "RunPage", frozen)]
pub struct PyRunPage {
    #[pyo3(get)]
    pub runs: Vec<PyRunInfo>,
    #[pyo3(get)]
    pub cursor: Option<Vec<u8>>,
}

impl From<RunPage> for PyRunPage {
    fn from(page: RunPage) -> Self {
        Self {
            runs: page.runs.into_iter().map(PyRunInfo::from).collect(),
            cursor: page.cursor,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRunPage {
    fn __len__(&self) -> usize {
        self.runs.len()
    }
}

/// A multi-dimensional per-run resource ceiling for `Runs.submit_budgeted`.
/// Each cap is an optional keyword. An absent cap is unbounded on that
/// dimension. A run that crosses any cap is failed by the run governor.
#[gen_stub_pyclass]
#[pyclass(name = "RunBudget", from_py_object)]
#[derive(Clone, Default)]
pub struct PyRunBudget {
    #[pyo3(get, set)]
    pub max_events: Option<u64>,
    #[pyo3(get, set)]
    pub max_model_calls: Option<u64>,
    #[pyo3(get, set)]
    pub max_tool_calls: Option<u64>,
    #[pyo3(get, set)]
    pub max_patches: Option<u64>,
    #[pyo3(get, set)]
    pub max_depth: Option<u32>,
    #[pyo3(get, set)]
    pub max_wall_clock_micros: Option<u64>,
    #[pyo3(get, set)]
    pub max_cost_usd: Option<f64>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRunBudget {
    #[new]
    #[pyo3(signature = (*, max_events=None, max_model_calls=None, max_tool_calls=None, max_patches=None, max_depth=None, max_wall_clock_micros=None, max_cost_usd=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_events: Option<u64>,
        max_model_calls: Option<u64>,
        max_tool_calls: Option<u64>,
        max_patches: Option<u64>,
        max_depth: Option<u32>,
        max_wall_clock_micros: Option<u64>,
        max_cost_usd: Option<f64>,
    ) -> Self {
        Self {
            max_events,
            max_model_calls,
            max_tool_calls,
            max_patches,
            max_depth,
            max_wall_clock_micros,
            max_cost_usd,
        }
    }
}

impl From<PyRunBudget> for RunBudget {
    fn from(budget: PyRunBudget) -> Self {
        Self {
            max_events: budget.max_events,
            max_model_calls: budget.max_model_calls,
            max_tool_calls: budget.max_tool_calls,
            max_patches: budget.max_patches,
            max_depth: budget.max_depth,
            max_wall_clock_micros: budget.max_wall_clock_micros,
            max_cost_usd: budget.max_cost_usd,
        }
    }
}

fn parse_state(word: &str) -> PyResult<AgentRunState> {
    AgentRunState::from_str(word).map_err(|error| PyValueError::new_err(error.to_string()))
}
