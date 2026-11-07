use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::convert::{duration_ms, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::materialize::{
    BindingStatus, FailureClass, Materialization, MaterializationReadiness, MaterializeRequest,
};
use laser_sdk::wire::control::{InvalidRecordPolicy, OversizePolicy};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

#[gen_stub_pyclass]
#[pyclass(name = "MaterializeRequest")]
pub struct PyMaterializeRequest {
    pub(crate) inner: MaterializeRequest,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMaterializeRequest {
    fn index(mut slf: PyRefMut<'_, Self>, index: String) -> PyRefMut<'_, Self> {
        slf.inner = slf.inner.clone().index(index);
        slf
    }
    fn projection_id(mut slf: PyRefMut<'_, Self>, id: String) -> PyRefMut<'_, Self> {
        slf.inner = slf.inner.clone().projection_id(id);
        slf
    }
    fn operation_id(mut slf: PyRefMut<'_, Self>, id: u128) -> PyRefMut<'_, Self> {
        slf.inner = slf.inner.clone().operation_id(id);
        slf
    }
    /// The setup identity `apply` sends. Every content change mints a fresh
    /// one unless `operation_id` pinned it.
    #[getter]
    fn operation(&self) -> u128 {
        self.inner.operation()
    }
    fn typed<'py>(
        mut slf: PyRefMut<'py, Self>,
        projection: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.inner = slf.inner.clone().typed(py_to_de(projection)?);
        Ok(slf)
    }
    fn backend<'py>(
        mut slf: PyRefMut<'py, Self>,
        backend: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.inner = slf.inner.clone().backend(setting(backend, "backend")?);
        Ok(slf)
    }
    fn retention<'py>(
        mut slf: PyRefMut<'py, Self>,
        retention: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.inner = slf
            .inner
            .clone()
            .retention(setting(retention, "retention")?);
        Ok(slf)
    }
    fn invalid_records<'py>(
        mut slf: PyRefMut<'py, Self>,
        policy: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let policy = InvalidRecordPolicy::from_str(policy)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        slf.inner = slf.inner.clone().invalid_records(policy);
        Ok(slf)
    }
    fn oversize<'py>(mut slf: PyRefMut<'py, Self>, policy: &str) -> PyResult<PyRefMut<'py, Self>> {
        let policy = OversizePolicy::from_str(policy)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        slf.inner = slf.inner.clone().oversize(policy);
        Ok(slf)
    }
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyMaterialization>>> {
        let request = self.inner.clone();
        awaitable(py, async move {
            let inner = request.apply().await.map_err(to_pyerr)?;
            Python::attach(|py| Py::new(py, PyMaterialization { inner }))
        })
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "Materialization", frozen)]
pub struct PyMaterialization {
    pub(crate) inner: Materialization,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    fn materialization(&self, projection_id: String) -> PyMaterialization {
        PyMaterialization {
            inner: self.inner.materialization(projection_id),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMaterialization {
    /// The status dict of each binding this handle follows, or None when no
    /// projection has the id or the followed source is no longer bound to it.
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[typing.Optional[list[dict[str, typing.Any]]]]",
        imports = ("typing",)
    ))]
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let materialization = self.inner.clone();
        awaitable(py, async move {
            let status = materialization.status().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &status))
        })
    }
    /// Wait until every followed binding reaches `level`. The dict's `kind`
    /// is `ready`, `blocked`, `failed`, `not_found` or `timed_out`, with the
    /// statuses under `status` for every kind but `not_found`.
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[dict[str, typing.Any]]",
        imports = ("typing",)
    ))]
    fn wait<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['accepted', 'applied', 'realized', 'caught_up']",
            imports = ("typing",)
        ))]
        level: &str,
        timeout_ms: f64,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let level = match level {
            "accepted" => MaterializationReadiness::Accepted,
            "applied" => MaterializationReadiness::Applied,
            "realized" => MaterializationReadiness::Realized,
            "caught_up" => MaterializationReadiness::CaughtUp,
            _ => return Err(InvalidError::new_err("unknown materialization readiness")),
        };
        let timeout = duration_ms(timeout_ms, "materialization timeout")?;
        let materialization = self.inner.clone();
        awaitable(py, async move {
            let result = materialization
                .wait(level, timeout)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn binding_status_readiness(
    py: Python<'_>,
    status: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let status: BindingStatus = py_to_de(status)?;
    ser_to_py(py, &status.readiness())
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn binding_status_caught_up_to(
    status: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let status: BindingStatus = py_to_de(status)?;
    let target: BindingStatus = py_to_de(target)?;
    Ok(status.caught_up_to(&target))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn binding_status_is_blocked(status: &Bound<'_, PyAny>) -> PyResult<bool> {
    let status: BindingStatus = py_to_de(status)?;
    Ok(status.is_blocked())
}

/// Whether the destination table failed in a way no retry recovers, so the
/// binding or its backend must change.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn binding_status_is_failed(status: &Bound<'_, PyAny>) -> PyResult<bool> {
    let status: BindingStatus = py_to_de(status)?;
    Ok(status.is_failed())
}

/// Whether a realization failure class lasts until the binding or its backend
/// changes.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn is_permanent_failure_class(
    #[gen_stub(override_type(
        type_repr = "typing.Literal['configuration', 'schema', 'poison', 'fatal', 'transient', 'capacity', 'read_only', 'unknown']",
        imports = ("typing",)
    ))]
    failure: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let failure: FailureClass = py_to_de(failure)?;
    Ok(failure.is_permanent())
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn binding_status_history_complete(
    status: &Bound<'_, PyAny>,
    oversize: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let status: BindingStatus = py_to_de(status)?;
    Ok(status.history_complete(py_to_de(oversize)?))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn is_block_invalid_record_policy(policy: &Bound<'_, PyAny>) -> PyResult<bool> {
    let policy: InvalidRecordPolicy = py_to_de(policy)?;
    Ok(policy.is_block())
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn is_block_oversize_policy(policy: &Bound<'_, PyAny>) -> PyResult<bool> {
    let policy: OversizePolicy = py_to_de(policy)?;
    Ok(policy.is_block())
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn materialization_operation_id(setup: u128, step: u8) -> u128 {
    laser_sdk::wire::hashing::materialization_operation_id(setup, step)
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn covers_gap_acceptance(
    acceptance: &Bound<'_, PyAny>,
    generation: u64,
    cursor: u64,
    first_available: u64,
) -> PyResult<bool> {
    let acceptance: laser_sdk::wire::control::GapAcceptance = py_to_de(acceptance)?;
    Ok(acceptance.covers(generation, cursor, first_available))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn is_append_projection_history(history: &Bound<'_, PyAny>) -> PyResult<bool> {
    let history: laser_sdk::wire::control::ProjectionHistory = py_to_de(history)?;
    Ok(history.is_append())
}

// A setup dict that does not describe a policy is a caller mistake, the same
// error a misspelled policy word raises.
fn setting<T: serde::de::DeserializeOwned>(value: &Bound<'_, PyAny>, name: &str) -> PyResult<T> {
    pythonize::depythonize(value).map_err(|error| InvalidError::new_err(format!("{name}: {error}")))
}
