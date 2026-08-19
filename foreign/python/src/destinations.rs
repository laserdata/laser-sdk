use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::wire::authz::SupervisorActorAssertion;
use laser_sdk::wire::checkpoint::{
    CheckpointOwnerId, CheckpointReadConsistency, CompletedAttempt, DestinationBlock,
    DestinationBlockCode, DestinationListFilter, PreparedAttempt, PublicCheckpointMutation,
    RepairRecord, RetentionGap,
};
use laser_sdk::wire::destination::{
    DestinationDesiredState, DestinationId, MaterializationDestination, QueryRoute, QueryRouteId,
};
use laser_sdk::wire::schema::UuidValue;
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
    #[pyo3(signature = (expected_global_state_revision, mutation, *, supervisor_assertion=None))]
    fn mutate<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        mutation: &Bound<'_, PyAny>,
        supervisor_assertion: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mutation: PublicCheckpointMutation = py_to_de(mutation)?;
        let supervisor_assertion = supervisor_assertion
            .map(py_to_de::<SupervisorActorAssertion>)
            .transpose()?;
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            mutation,
            supervisor_assertion,
        )
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

    fn bind_table<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_definition_revision: u64,
        table_uuid: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::BindTable {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_definition_revision,
                table_uuid: py_to_de::<UuidValue>(table_uuid)?,
            },
            None,
        )
    }

    fn add_partition<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::AddPartition {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                partition_id,
            },
            None,
        )
    }

    fn observe_partition_lifecycle<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::ObservePartitionLifecycle {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                partition_id,
            },
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lease<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        owner: String,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::AcquireLease {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                owner: parse_owner_id(&owner)?,
                expected_lease_sequence,
                lease_duration_micros,
            },
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn renew_lease<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        owner: String,
        epoch: u64,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::RenewLease {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                owner: parse_owner_id(&owner)?,
                epoch,
                expected_lease_sequence,
                lease_duration_micros,
            },
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn take_over_lease<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        owner: String,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::TakeoverLease {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                owner: parse_owner_id(&owner)?,
                expected_lease_sequence,
                lease_duration_micros,
            },
            None,
        )
    }

    fn prepare<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        expected_checkpoint_revision: u64,
        attempt: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::Prepare {
                expected_checkpoint_revision,
                attempt: py_to_de::<PreparedAttempt>(attempt)?,
            },
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn complete<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        owner: String,
        epoch: u64,
        expected_checkpoint_revision: u64,
        completion: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::Complete {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                owner: parse_owner_id(&owner)?,
                epoch,
                expected_checkpoint_revision,
                completion: py_to_de::<CompletedAttempt>(completion)?,
            },
            None,
        )
    }

    fn record_block<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        block: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::RecordBlock {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                block: py_to_de::<DestinationBlock>(block)?,
            },
            None,
        )
    }

    fn clear_block<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        expected_code: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::ClearBlock {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                expected_code: py_to_de::<DestinationBlockCode>(expected_code)?,
            },
            None,
        )
    }

    fn record_retention_gap<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        gap: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::RecordRetentionGap {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                gap: py_to_de::<RetentionGap>(gap)?,
            },
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn accept_retention_gap<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        next_offset: u64,
        supervisor_assertion: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::AcceptRetentionGap {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                next_offset,
            },
            Some(py_to_de::<SupervisorActorAssertion>(supervisor_assertion)?),
        )
    }

    fn supersede_generation<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        expected_definition_revision: u64,
        replacement: &Bound<'_, PyAny>,
        supervisor_assertion: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::SupersedeGeneration {
                expected_definition_revision,
                replacement: py_to_de::<MaterializationDestination>(replacement)?,
            },
            Some(py_to_de::<SupervisorActorAssertion>(supervisor_assertion)?),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_repair<'py>(
        &self,
        py: Python<'py>,
        expected_global_state_revision: u64,
        destination_id: String,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        repair: &Bound<'_, PyAny>,
        supervisor_assertion: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        submit_mutation(
            py,
            self.laser.clone(),
            expected_global_state_revision,
            PublicCheckpointMutation::RecordRepair {
                destination_id: parse_destination_id(&destination_id)?,
                destination_generation,
                expected_checkpoint_revision,
                repair: py_to_de::<RepairRecord>(repair)?,
            },
            Some(py_to_de::<SupervisorActorAssertion>(supervisor_assertion)?),
        )
    }

    /// Read one destination declaration and checkpoint status.
    #[pyo3(signature = (destination_id, *, consistency))]
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
    #[pyo3(signature = (*, consistency, filter=None, after=None, limit=100))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        consistency: &str,
        filter: Option<&Bound<'_, PyAny>>,
        after: Option<String>,
        limit: usize,
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
    #[pyo3(signature = (*, consistency, name_contains=None, after=None, limit=100))]
    fn query_routes<'py>(
        &self,
        py: Python<'py>,
        consistency: &str,
        name_contains: Option<String>,
        after: Option<String>,
        limit: usize,
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

fn parse_owner_id(value: &str) -> PyResult<CheckpointOwnerId> {
    CheckpointOwnerId::from_str(value).map_err(|error| InvalidError::new_err(error.to_string()))
}

fn submit_mutation<'py>(
    py: Python<'py>,
    laser: Laser,
    expected_global_state_revision: u64,
    mutation: PublicCheckpointMutation,
    supervisor_assertion: Option<SupervisorActorAssertion>,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let destinations = laser.destinations();
        let result = match supervisor_assertion {
            Some(assertion) => {
                destinations
                    .mutate_with_supervisor_assertion(
                        expected_global_state_revision,
                        mutation,
                        assertion,
                    )
                    .await
            }
            None => {
                destinations
                    .mutate(expected_global_state_revision, mutation)
                    .await
            }
        }
        .map_err(to_pyerr)?;
        Python::attach(|py| ser_to_py(py, &result))
    })
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
