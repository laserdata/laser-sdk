use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::wire::checkpoint::{
    CheckpointReadConsistency, DestinationListFilter, PublicCheckpointMutation,
};
use laser_sdk::wire::destination::{
    DestinationDesiredState, DestinationId, MaterializationDestination, QueryRoute, QueryRouteId,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Open the managed materialization destination and query-route surface.
    fn destinations(&self) -> PyDestinations {
        PyDestinations {
            laser: self.inner.clone(),
        }
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "Destinations")]
pub struct PyDestinations {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyDestinations {
    /// Submit one bounded public checkpoint mutation dict.
    fn mutate<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        mutation: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mutation: PublicCheckpointMutation = py_to_de(mutation)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .mutate(expected_global_state_revision, mutation)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Register one complete materialization destination declaration dict.
    fn register<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let destination: MaterializationDestination = py_to_de(destination)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .register(expected_global_state_revision, destination)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Enable or disable a destination with definition-revision compare-and-set.
    fn set_desired_state<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_definition_revision: u64,
        desired_state: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let destination_id = parse_destination_id(&destination_id)?;
        let desired_state = match desired_state {
            "disabled" => DestinationDesiredState::Disabled,
            "enabled" => DestinationDesiredState::Enabled,
            other => {
                return Err(InvalidError::new_err(format!(
                    "desired state must be 'disabled' or 'enabled', got '{other}'"
                )));
            }
        };
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .set_desired_state(
                    expected_global_state_revision,
                    destination_id,
                    destination_generation,
                    expected_definition_revision,
                    desired_state,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Read one destination declaration and checkpoint status.
    #[pyo3(signature = (destination_id, *, consistency="potentially_stale"))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        destination_id: String,
        consistency: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let destination_id = parse_destination_id(&destination_id)?;
        let consistency = parse_consistency(consistency)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .get(destination_id, consistency)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// List destination declarations and checkpoint status using a bounded page.
    #[pyo3(signature = (*, filter=None, after=None, limit=100, consistency="potentially_stale"))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        filter: Option<&Bound<'_, PyAny>>,
        after: Option<String>,
        limit: usize,
        consistency: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let filter = match filter {
            Some(filter) => py_to_de(filter)?,
            None => DestinationListFilter::default(),
        };
        let after = after.as_deref().map(parse_destination_id).transpose()?;
        let consistency = parse_consistency(consistency)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .list(filter, after, limit, consistency)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Register one explicit logical query route declaration dict.
    fn register_query_route<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        route: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let route: QueryRoute = py_to_de(route)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .register_query_route(expected_global_state_revision, route)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Remove one query route generation with definition-revision compare-and-set.
    fn remove_query_route<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        route_id: String,
        route_generation: u64,
        expected_definition_revision: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let route_id = QueryRouteId::from_str(&route_id)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .remove_query_route(
                    expected_global_state_revision,
                    route_id,
                    route_generation,
                    expected_definition_revision,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// List explicit query routes using a bounded page.
    #[pyo3(signature = (*, name_contains=None, after=None, limit=100, consistency="potentially_stale"))]
    fn query_routes<'py>(
        &self,
        py: Python<'py>,
        name_contains: Option<String>,
        after: Option<String>,
        limit: usize,
        consistency: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let after = after
            .as_deref()
            .map(QueryRouteId::from_str)
            .transpose()
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        let consistency = parse_consistency(consistency)?;
        future_into_py(py, async move {
            let result = laser
                .destinations()
                .query_routes(name_contains, after, limit, consistency)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }
}

fn parse_destination_id(value: &str) -> PyResult<DestinationId> {
    DestinationId::from_str(value).map_err(|error| InvalidError::new_err(error.to_string()))
}

fn parse_consistency(value: &str) -> PyResult<CheckpointReadConsistency> {
    match value {
        "linearizable" => Ok(CheckpointReadConsistency::Linearizable),
        "potentially_stale" => Ok(CheckpointReadConsistency::PotentiallyStale),
        other => Err(InvalidError::new_err(format!(
            "checkpoint consistency must be 'linearizable' or 'potentially_stale', got '{other}'"
        ))),
    }
}
