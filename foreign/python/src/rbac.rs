use crate::client::PyLaser;
use crate::errors::to_pyerr;
use laser_sdk::rbac::{
    Action, AuthzEvent, AuthzEventKind, AuthzHistoryReply, AuthzSubject, Effect, Feature, Grant,
    ResourceKind, ResourcePattern, Role,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

/// Apply the wire RBAC decision rule: deny-wins, empty grants deny, and
/// `resource=None` means the operation has no keyed selector.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (grants, feature, action, resource=None))]
pub fn grants_allow(
    grants: Vec<PyGrant>,
    feature: String,
    action: String,
    resource: Option<String>,
) -> PyResult<bool> {
    let grants = grants
        .into_iter()
        .map(Grant::try_from)
        .collect::<PyResult<Vec<_>>>()?;
    let (feature, action) = parse_feature_action(&feature, &action)?;
    Ok(laser_sdk::rbac::grants_allow(
        &grants,
        feature,
        action,
        resource.as_deref(),
    ))
}

/// Apply the on-behalf-of rule: the agent and user grant sets must both allow
/// the operation, so an agent can never exceed the user it acts for.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (agent_grants, user_grants, feature, action, resource=None))]
pub fn delegated_allow(
    agent_grants: Vec<PyGrant>,
    user_grants: Vec<PyGrant>,
    feature: String,
    action: String,
    resource: Option<String>,
) -> PyResult<bool> {
    let agent_grants = agent_grants
        .into_iter()
        .map(Grant::try_from)
        .collect::<PyResult<Vec<_>>>()?;
    let user_grants = user_grants
        .into_iter()
        .map(Grant::try_from)
        .collect::<PyResult<Vec<_>>>()?;
    let (feature, action) = parse_feature_action(&feature, &action)?;
    Ok(laser_sdk::rbac::delegated_allow(
        &agent_grants,
        &user_grants,
        feature,
        action,
        resource.as_deref(),
    ))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The caller's own effective capabilities: the bound role names and their
    /// flattened grants. Always answered to the authenticated caller. The
    /// authorization surface is fork-native: against raw Apache Iggy it raises
    /// `UnsupportedError`.
    fn whoami<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser.whoami().await.map_err(to_pyerr)?;
            Ok(PyWhoami {
                roles: reply.roles,
                grants: reply.grants.into_iter().map(PyGrant::from).collect(),
            })
        })
    }

    /// List defined roles, optionally filtered by name prefix.
    #[pyo3(signature = (name_prefix=None))]
    fn list_roles<'py>(
        &self,
        py: Python<'py>,
        name_prefix: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let roles = laser
                .list_roles(name_prefix.as_deref())
                .await
                .map_err(to_pyerr)?;
            Ok(roles.into_iter().map(PyRole::from).collect::<Vec<_>>())
        })
    }

    /// One role by name, or `None`.
    fn get_role<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let role = laser.get_role(name).await.map_err(to_pyerr)?;
            Ok(role.map(PyRole::from))
        })
    }

    /// One user's bound role names.
    fn get_bindings<'py>(&self, py: Python<'py>, user_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser
                .get_bindings(laser_sdk::types::PrincipalId::new(user_id))
                .await
                .map_err(to_pyerr)
        })
    }

    /// Define or replace a role (upsert). Requires `authz:admin`.
    fn define_role<'py>(
        &self,
        py: Python<'py>,
        name: String,
        grants: Vec<PyGrant>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let grants = grants
            .into_iter()
            .map(Grant::try_from)
            .collect::<PyResult<Vec<_>>>()?;
        future_into_py(py, async move {
            laser
                .define_role(Role { name, grants })
                .await
                .map_err(to_pyerr)
        })
    }

    /// Delete a role by name. Requires `authz:admin`.
    fn delete_role<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser.delete_role(name).await.map_err(to_pyerr)
        })
    }

    /// Bind a user's whole role set (replace). Requires `authz:admin`.
    #[pyo3(signature = (user_id, roles, *, expect_revision=None))]
    fn bind_roles<'py>(
        &self,
        py: Python<'py>,
        user_id: u32,
        roles: Vec<String>,
        expect_revision: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            match expect_revision {
                Some(revision) => {
                    laser
                        .bind_roles_expect_revision(
                            laser_sdk::types::PrincipalId::new(user_id),
                            roles,
                            revision,
                        )
                        .await
                }
                None => {
                    laser
                        .bind_roles(laser_sdk::types::PrincipalId::new(user_id), roles)
                        .await
                }
            }
            .map_err(to_pyerr)
        })
    }

    /// Read authorization history for all role and binding changes.
    #[pyo3(signature = (*, after_revision=None, limit=100))]
    fn authz_history_all<'py>(
        &self,
        py: Python<'py>,
        after_revision: Option<u64>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser
                .authz_history(AuthzSubject::All, after_revision, limit)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAuthzHistoryPage::from(reply))
        })
    }

    /// Read authorization history for one role.
    #[pyo3(signature = (name, *, after_revision=None, limit=100))]
    fn authz_history_role<'py>(
        &self,
        py: Python<'py>,
        name: String,
        after_revision: Option<u64>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser
                .authz_history(AuthzSubject::Role(name), after_revision, limit)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAuthzHistoryPage::from(reply))
        })
    }

    /// Read authorization history for one user's role bindings.
    #[pyo3(signature = (user_id, *, after_revision=None, limit=100))]
    fn authz_history_binding<'py>(
        &self,
        py: Python<'py>,
        user_id: u32,
        after_revision: Option<u64>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser
                .authz_history(AuthzSubject::Binding { user_id }, after_revision, limit)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAuthzHistoryPage::from(reply))
        })
    }
}

/// One capability grant: `effect feature:action [on resource]`. `effect` is
/// `allow` or `deny`. `resource_kind` is `all`, `literal`, or `prefix` (with
/// `resource_value` empty for `all`). The words are the pinned snake-case
/// vocabulary. An unknown word raises `ValueError` when the grant is used.
#[gen_stub_pyclass]
#[pyclass(name = "Grant", from_py_object)]
#[derive(Clone)]
pub struct PyGrant {
    #[pyo3(get, set)]
    pub effect: String,
    #[pyo3(get, set)]
    pub feature: String,
    #[pyo3(get, set)]
    pub action: String,
    #[pyo3(get, set)]
    pub resource_kind: String,
    #[pyo3(get, set)]
    pub resource_value: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGrant {
    #[new]
    #[pyo3(signature = (feature, action, *, effect="allow".to_owned(), resource_kind="all".to_owned(), resource_value=String::new()))]
    fn new(
        feature: String,
        action: String,
        effect: String,
        resource_kind: String,
        resource_value: String,
    ) -> Self {
        Self {
            effect,
            feature,
            action,
            resource_kind,
            resource_value,
        }
    }
}

impl From<Grant> for PyGrant {
    fn from(grant: Grant) -> Self {
        Self {
            effect: grant.effect.to_string(),
            feature: grant.feature.to_string(),
            action: grant.action.to_string(),
            resource_kind: grant.resource.kind.to_string(),
            resource_value: grant.resource.value,
        }
    }
}

impl TryFrom<PyGrant> for Grant {
    type Error = PyErr;

    fn try_from(grant: PyGrant) -> PyResult<Self> {
        let parse =
            |kind: &str, word: &str| PyValueError::new_err(format!("unknown authz {kind}: {word}"));
        Ok(Self {
            effect: Effect::from_str(&grant.effect).map_err(|_| parse("effect", &grant.effect))?,
            feature: Feature::from_str(&grant.feature)
                .map_err(|_| parse("feature", &grant.feature))?,
            action: Action::from_str(&grant.action).map_err(|_| parse("action", &grant.action))?,
            resource: ResourcePattern {
                kind: ResourceKind::from_str(&grant.resource_kind)
                    .map_err(|_| parse("resource kind", &grant.resource_kind))?,
                value: grant.resource_value,
            },
        })
    }
}

fn parse_feature_action(feature: &str, action: &str) -> PyResult<(Feature, Action)> {
    let parse =
        |kind: &str, word: &str| PyValueError::new_err(format!("unknown authz {kind}: {word}"));
    Ok((
        Feature::from_str(feature).map_err(|_| parse("feature", feature))?,
        Action::from_str(action).map_err(|_| parse("action", action))?,
    ))
}

/// A named set of grants.
#[gen_stub_pyclass]
#[pyclass(name = "Role", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyRole {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub grants: Vec<PyGrant>,
}

impl From<Role> for PyRole {
    fn from(role: Role) -> Self {
        Self {
            name: role.name,
            grants: role.grants.into_iter().map(PyGrant::from).collect(),
        }
    }
}

/// The caller's effective capabilities: bound role names and flattened grants.
#[gen_stub_pyclass]
#[pyclass(name = "Whoami", frozen)]
pub struct PyWhoami {
    #[pyo3(get)]
    pub roles: Vec<String>,
    #[pyo3(get)]
    pub grants: Vec<PyGrant>,
}

/// One authorization history event.
#[gen_stub_pyclass]
#[pyclass(name = "AuthzEvent", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyAuthzEvent {
    #[pyo3(get)]
    pub revision: u64,
    #[pyo3(get)]
    pub actor: String,
    #[pyo3(get)]
    pub at_micros: u64,
    #[pyo3(get)]
    pub op: String,
    #[pyo3(get)]
    pub role: Option<String>,
    #[pyo3(get)]
    pub user_id: Option<u32>,
    #[pyo3(get)]
    pub roles: Vec<String>,
}

impl From<AuthzEvent> for PyAuthzEvent {
    fn from(event: AuthzEvent) -> Self {
        let (op, role, user_id, roles) = match event.op {
            AuthzEventKind::RoleDefined(name) => {
                ("role_defined".to_owned(), Some(name), None, Vec::new())
            }
            AuthzEventKind::RoleDeleted(name) => {
                ("role_deleted".to_owned(), Some(name), None, Vec::new())
            }
            AuthzEventKind::RolesBound { user_id, roles } => {
                ("roles_bound".to_owned(), None, Some(user_id), roles)
            }
        };
        Self {
            revision: event.revision,
            actor: event.actor,
            at_micros: event.at_micros,
            op,
            role,
            user_id,
            roles,
        }
    }
}

/// A page of authorization history events.
#[gen_stub_pyclass]
#[pyclass(name = "AuthzHistoryPage", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyAuthzHistoryPage {
    #[pyo3(get)]
    pub events: Vec<PyAuthzEvent>,
    #[pyo3(get)]
    pub next_after_revision: Option<u64>,
}

impl From<AuthzHistoryReply> for PyAuthzHistoryPage {
    fn from(reply: AuthzHistoryReply) -> Self {
        Self {
            events: reply.events.into_iter().map(PyAuthzEvent::from).collect(),
            next_after_revision: reply.next_after_revision,
        }
    }
}
