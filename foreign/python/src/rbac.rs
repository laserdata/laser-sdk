use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::LaserError;
use laser_sdk::edge_auth::{EdgeClaims, EdgeDenial};
use laser_sdk::rbac::{
    Action, AuthzEvent, AuthzEventKind, AuthzHistoryReply, AuthzSubject, Effect, Feature, Grant,
    ResourceKind, ResourcePattern, Role, WhoamiReply,
};
use laser_sdk::types::PrincipalId;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
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

/// Apply the on-behalf-of rule: the `agent` and `user` grant sets must both
/// allow the operation, so an agent can never exceed the user it acts for.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (agent, user, feature, action, resource=None))]
pub fn delegated_allow(
    agent: Vec<PyGrant>,
    user: Vec<PyGrant>,
    feature: String,
    action: String,
    resource: Option<String>,
) -> PyResult<bool> {
    let agent = agent
        .into_iter()
        .map(Grant::try_from)
        .collect::<PyResult<Vec<_>>>()?;
    let user = user
        .into_iter()
        .map(Grant::try_from)
        .collect::<PyResult<Vec<_>>>()?;
    let (feature, action) = parse_feature_action(&feature, &action)?;
    Ok(laser_sdk::rbac::delegated_allow(
        &agent,
        &user,
        feature,
        action,
        resource.as_deref(),
    ))
}

/// Check a role name against the rule every tier enforces: non-empty, at most
/// `MAX_ROLE_NAME_BYTES` bytes, and only ASCII letters, digits, `-`, `_`, and
/// `.`. An invalid name raises `InvalidError`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn validate_role_name(name: &str) -> PyResult<()> {
    laser_sdk::rbac::validate_role_name(name).map_err(|error| to_pyerr(LaserError::from(error)))
}

/// Authorize an external-edge (MCP / A2A) request against the decoded token
/// claims `audience` and `scopes`: strict audience validation, then a scope
/// check. Returns `None` when the token is minted for `expected_audience` and
/// carries `required_scope`, otherwise the `EdgeDenial`. The transport decodes
/// the token, this decides.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn authorize_edge(
    audience: Vec<String>,
    scopes: Vec<String>,
    expected_audience: String,
    required_scope: String,
) -> Option<PyEdgeDenial> {
    let claims = EdgeClaims { audience, scopes };
    laser_sdk::edge_auth::authorize_edge(&claims, &expected_audience, &required_scope)
        .err()
        .map(|inner| PyEdgeDenial { inner })
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The caller's own effective capabilities: the bound role names and their
    /// flattened grants. Always answered to the authenticated caller. The
    /// authorization surface is server-side: against Apache Iggy it raises
    /// `UnsupportedError`.
    fn whoami<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser.whoami().await.map_err(to_pyerr)?;
            Ok(PyWhoamiReply::from(reply))
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

    /// The role names bound to the user `principal`.
    fn get_bindings<'py>(&self, py: Python<'py>, principal: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser
                .get_bindings(PrincipalId::new(principal))
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

    /// Bind the user `principal`'s whole role set (replace). Requires
    /// `authz:admin`. With `expect_revision`, bind only if the binding's
    /// current revision matches, otherwise raise `AuthzError`.
    #[pyo3(signature = (principal, roles, *, expect_revision=None))]
    fn bind_roles<'py>(
        &self,
        py: Python<'py>,
        principal: u32,
        roles: Vec<String>,
        expect_revision: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let principal = PrincipalId::new(principal);
        future_into_py(py, async move {
            match expect_revision {
                Some(revision) => {
                    laser
                        .bind_roles_expect_revision(principal, roles, revision)
                        .await
                }
                None => laser.bind_roles(principal, roles).await,
            }
            .map_err(to_pyerr)
        })
    }

    /// Read a page of the authorization change history. Requires
    /// `authz:read`. `subject` is `"all"`, `{"role": name}`, or
    /// `{"binding": {"user_id": id}}`.
    #[pyo3(signature = (subject, *, after_revision=None, limit=100))]
    fn authz_history<'py>(
        &self,
        py: Python<'py>,
        subject: &Bound<'_, PyAny>,
        after_revision: Option<u64>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subject: AuthzSubject = py_to_de(subject)?;
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let reply = laser
                .authz_history(subject, after_revision, limit)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAuthzHistoryReply::from(reply))
        })
    }
}

/// A resource selector on a grant: `all` (the whole feature), `literal` (one
/// exact name), or `prefix` (every name under `value`). An unknown `kind`
/// raises `ValueError`.
#[gen_stub_pyclass]
#[pyclass(name = "ResourcePattern", frozen, eq, from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyResourcePattern {
    inner: ResourcePattern,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyResourcePattern {
    #[new]
    #[pyo3(signature = (kind="all", value=String::new()))]
    fn new(kind: &str, value: String) -> PyResult<Self> {
        let kind = ResourceKind::from_str(kind)
            .map_err(|_| PyValueError::new_err(format!("unknown authz resource kind: {kind}")))?;
        Ok(Self {
            inner: ResourcePattern { kind, value },
        })
    }

    /// The whole-feature pattern.
    #[staticmethod]
    fn all() -> Self {
        Self {
            inner: ResourcePattern::all(),
        }
    }

    /// An exact-name pattern.
    #[staticmethod]
    fn literal(value: String) -> Self {
        Self {
            inner: ResourcePattern::literal(value),
        }
    }

    /// A prefix pattern: every name under `value`.
    #[staticmethod]
    fn prefix(value: String) -> Self {
        Self {
            inner: ResourcePattern::prefix(value),
        }
    }

    /// `all`, `literal`, or `prefix`.
    #[getter]
    fn kind(&self) -> String {
        self.inner.kind.to_string()
    }

    /// The name or prefix, empty for `all`.
    #[getter]
    fn value(&self) -> &str {
        &self.inner.value
    }

    /// Whether `resource`, the selector of a request, matches. An unkeyed
    /// request (`None`) matches only the whole-feature pattern.
    #[pyo3(signature = (resource=None))]
    fn matches(&self, resource: Option<&str>) -> bool {
        self.inner.matches(resource)
    }

    fn __repr__(&self) -> String {
        format!(
            "ResourcePattern(kind={:?}, value={:?})",
            self.inner.kind.to_string(),
            self.inner.value
        )
    }
}

/// One capability grant: `effect feature:action [on resource]`. `effect` is
/// `allow` or `deny`, and `resource` defaults to the whole feature. The words
/// are the pinned snake-case vocabulary. An unknown word raises `ValueError`
/// when the grant is used.
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
    pub resource: PyResourcePattern,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGrant {
    #[new]
    #[pyo3(signature = (feature, action, *, effect="allow".to_owned(), resource=None))]
    fn new(
        feature: String,
        action: String,
        effect: String,
        resource: Option<PyResourcePattern>,
    ) -> Self {
        Self {
            effect,
            feature,
            action,
            resource: resource.unwrap_or_else(PyResourcePattern::all),
        }
    }
}

impl From<Grant> for PyGrant {
    fn from(grant: Grant) -> Self {
        Self {
            effect: grant.effect.to_string(),
            feature: grant.feature.to_string(),
            action: grant.action.to_string(),
            resource: PyResourcePattern {
                inner: grant.resource,
            },
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
            resource: grant.resource.inner,
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
#[pyclass(name = "WhoamiReply", frozen)]
pub struct PyWhoamiReply {
    /// The authorization operation version of the reply.
    #[pyo3(get)]
    pub v: u32,
    #[pyo3(get)]
    pub roles: Vec<String>,
    #[pyo3(get)]
    pub grants: Vec<PyGrant>,
}

impl From<WhoamiReply> for PyWhoamiReply {
    fn from(reply: WhoamiReply) -> Self {
        Self {
            v: reply.v,
            roles: reply.roles,
            grants: reply.grants.into_iter().map(PyGrant::from).collect(),
        }
    }
}

/// One recorded authorization change: its revision, who made it, when, and
/// what. `op` is `{"role_defined": name}`, `{"role_deleted": name}`, or
/// `{"roles_bound": {"user_id": id, "roles": [...]}}`.
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
    op: AuthzEventKind,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAuthzEvent {
    /// What the change recorded, as a dict keyed by its kind.
    #[getter]
    fn op(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.op)
    }
}

impl From<AuthzEvent> for PyAuthzEvent {
    fn from(event: AuthzEvent) -> Self {
        Self {
            revision: event.revision,
            actor: event.actor,
            at_micros: event.at_micros,
            op: event.op,
        }
    }
}

/// A page of authorization change history. Pass `next_after_revision` as
/// `after_revision` to read the next page.
#[gen_stub_pyclass]
#[pyclass(name = "AuthzHistoryReply", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyAuthzHistoryReply {
    /// The authorization operation version of the reply.
    #[pyo3(get)]
    pub v: u32,
    #[pyo3(get)]
    pub events: Vec<PyAuthzEvent>,
    #[pyo3(get)]
    pub next_after_revision: Option<u64>,
}

impl From<AuthzHistoryReply> for PyAuthzHistoryReply {
    fn from(reply: AuthzHistoryReply) -> Self {
        Self {
            v: reply.v,
            events: reply.events.into_iter().map(PyAuthzEvent::from).collect(),
            next_after_revision: reply.next_after_revision,
        }
    }
}

/// Why an external-edge request was refused. `kind` is `wrong_audience`, a
/// hard reject of a token minted for another server, or `step_up`, a token
/// that lacks the required scope.
#[gen_stub_pyclass]
#[pyclass(name = "EdgeDenial", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyEdgeDenial {
    inner: EdgeDenial,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEdgeDenial {
    /// The token was not minted for `expected`.
    #[staticmethod]
    fn wrong_audience(expected: String) -> Self {
        Self {
            inner: EdgeDenial::WrongAudience { expected },
        }
    }

    /// The token lacks `required_scope`.
    #[staticmethod]
    fn step_up(required_scope: String) -> Self {
        Self {
            inner: EdgeDenial::StepUp { required_scope },
        }
    }

    /// `wrong_audience` or `step_up`.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner {
            EdgeDenial::WrongAudience { .. } => "wrong_audience",
            EdgeDenial::StepUp { .. } => "step_up",
        }
    }

    /// The result code: `Unauthenticated` for a wrong audience,
    /// `StepUpRequired` for a missing scope.
    #[getter]
    fn code(&self) -> String {
        format!("{:?}", self.inner.code())
    }

    /// The `WWW-Authenticate` challenge naming the scope to acquire, `None`
    /// for a wrong audience.
    #[getter]
    fn challenge(&self) -> Option<String> {
        self.inner.challenge()
    }

    fn __repr__(&self) -> String {
        format!("EdgeDenial({:?})", self.inner)
    }
}
