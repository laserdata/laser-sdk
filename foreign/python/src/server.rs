use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::convert::{duration_ms, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::server::{
    CacheMetrics, ClientConsumerGroup, ClientInfo, ClusterInfo, CreateUserOptions,
    MetadataChangeGroup, MetadataChangeRecord, MetadataChangeStart, MetadataChanges,
    MetadataChangesQuery, MetadataItem, MetadataSubject, OptionInfo, OptionsScope, Permissions,
    PersonalAccessToken, PersonalAccessTokenInfo, ServerStats, UpdateUserOptions, User, UserInfo,
    UserStatus,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Every user of the server, without permissions. Needs the read users
    /// permission.
    fn users<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyUserInfo>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let users = laser.users().await.map_err(to_pyerr)?;
            Ok(users.into_iter().map(PyUserInfo::from).collect())
        })
    }

    /// The user accessor. Free and synchronous, IO happens at the verbs.
    fn user(&self, name: String) -> PyUser {
        PyUser {
            user: self.inner.user(name),
        }
    }

    /// Every personal access token of the signed-in user.
    fn personal_access_tokens<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Vec<PyPersonalAccessTokenInfo>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let tokens = laser.personal_access_tokens().await.map_err(to_pyerr)?;
            Ok(tokens
                .into_iter()
                .map(PyPersonalAccessTokenInfo::from)
                .collect())
        })
    }

    /// The personal access token accessor for the signed-in user. Free and
    /// synchronous, IO happens at the verbs.
    fn personal_access_token(&self, name: String) -> PyPersonalAccessToken {
        PyPersonalAccessToken {
            token: self.inner.personal_access_token(name),
        }
    }

    /// Ping the server.
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.inner.clone();
        awaitable(py, async move { laser.ping().await.map_err(to_pyerr) })
    }

    /// The server statistics. Needs the read servers permission.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyServerStats>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            Ok(laser.stats().await.map_err(to_pyerr)?.into())
        })
    }

    /// This connection as the server sees it.
    fn me<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyClientInfo>> {
        let laser = self.inner.clone();
        awaitable(
            py,
            async move { Ok(laser.me().await.map_err(to_pyerr)?.into()) },
        )
    }

    /// Every client connected to the server. Needs the read servers
    /// permission.
    fn clients<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyClientInfo>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let clients = laser.clients().await.map_err(to_pyerr)?;
            Ok(clients.into_iter().map(PyClientInfo::from).collect())
        })
    }

    /// The cluster name and its nodes with their roles and states.
    fn cluster<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyClusterInfo>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            Ok(laser.cluster().await.map_err(to_pyerr)?.into())
        })
    }

    /// A server diagnostics snapshot as the bytes of a ZIP archive, with every
    /// section the server collects: file system overview, process list,
    /// resource use, server logs, and server configuration. Needs the manage
    /// servers permission.
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[builtins.bytes]", imports = ("typing", "builtins")))]
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<u8>>> {
        let laser = self.inner.clone();
        awaitable(py, async move { laser.snapshot().await.map_err(to_pyerr) })
    }

    /// One connected client with its consumer groups, `None` when no client
    /// has `client_id`. Needs the read servers permission.
    fn client_info<'py>(
        &self,
        py: Python<'py>,
        client_id: u32,
    ) -> PyResult<Awaitable<'py, Option<PyClientInfo>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let client = laser.client_info(client_id).await.map_err(to_pyerr)?;
            Ok(client.map(PyClientInfo::from))
        })
    }

    /// Prove the outcome of the request `operation` that a failure's
    /// `uncertain_operation` names, waiting at most `timeout_ms`. Resolves
    /// when the request committed and raises a committed refusal's own error.
    /// Either outcome frees the requests the operation blocked.
    fn resolve_operation<'py>(
        &self,
        py: Python<'py>,
        operation: u64,
        timeout_ms: f64,
    ) -> PyResult<Awaitable<'py, ()>> {
        let timeout = duration_ms(timeout_ms, "timeout_ms")?;
        let laser = self.inner.clone();
        awaitable(py, async move {
            laser
                .resolve_operation(operation, timeout)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Give up the request `operation` that a failure's
    /// `uncertain_operation` names, and free the requests it blocked. Its
    /// outcome stays unknown.
    fn abandon_operation<'py>(
        &self,
        py: Python<'py>,
        operation: u64,
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            laser.abandon_operation(operation).await.map_err(to_pyerr)
        })
    }

    /// Send the command `code` with a raw payload and return the raw reply.
    /// The server checks the permissions of the command. The sign-in,
    /// sign-out, and session commands are refused before anything is sent.
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[builtins.bytes]", imports = ("typing", "builtins")))]
    fn send_command<'py>(
        &self,
        py: Python<'py>,
        code: u32,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports = ("builtins",)))]
        payload: Vec<u8>,
    ) -> PyResult<Awaitable<'py, Vec<u8>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            laser.send_command(code, payload).await.map_err(to_pyerr)
        })
    }

    /// Send the command `code` with a raw payload to the replica `replica`,
    /// as `cluster()` numbers it, and return that replica's own reply. Only a
    /// command that a node answers from its own state goes to a replica.
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[builtins.bytes]", imports = ("typing", "builtins")))]
    fn send_command_to_replica<'py>(
        &self,
        py: Python<'py>,
        code: u32,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports = ("builtins",)))]
        payload: Vec<u8>,
        replica: u8,
    ) -> PyResult<Awaitable<'py, Vec<u8>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            laser
                .send_command_to_replica(code, payload, replica)
                .await
                .map_err(to_pyerr)
        })
    }
}

/// One user of the server, addressed by name. Build it with `Laser.user`.
/// Each verb needs the matching user permission on this connection.
/// Permissions are dicts with a `global` dict of grant flags and a `streams`
/// dict keyed by native stream id, each with its own `topics` dict keyed by
/// native topic id. A grant flag left out is `False`.
#[gen_stub_pyclass]
#[pyclass(name = "User", frozen)]
pub struct PyUser {
    user: User,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyUser {
    /// This user's name.
    #[getter]
    fn name(&self) -> String {
        self.user.name().to_owned()
    }

    /// This user with its permissions, `None` when it does not exist.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Option<PyUserInfo>>> {
        let user = self.user.clone();
        awaitable(py, async move {
            Ok(user.get().await.map_err(to_pyerr)?.map(PyUserInfo::from))
        })
    }

    /// Whether this user exists.
    fn exists<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let user = self.user.clone();
        awaitable(py, async move { user.exists().await.map_err(to_pyerr) })
    }

    /// Create this user with `password`. A user starts `active` without
    /// grants. An existing user of the same name fails the call.
    #[pyo3(signature = (password, *, status="active", permissions=None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        password: String,
        #[gen_stub(override_type(type_repr = "typing.Literal['active', 'inactive']", imports = ("typing",)))]
        status: &str,
        #[gen_stub(override_type(type_repr = "dict[str, typing.Any] | None", imports = ("typing",)))]
        permissions: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Awaitable<'py, PyUserInfo>> {
        let options = CreateUserOptions {
            status: user_status(status)?,
            permissions: permissions_arg(permissions.as_ref())?,
        };
        let user = self.user.clone();
        awaitable(py, async move {
            Ok(user
                .create(&password, options)
                .await
                .map_err(to_pyerr)?
                .into())
        })
    }

    /// Rename this user or change its status. A keyword left `None` keeps its
    /// value. A deactivated user can no longer sign in. After a rename,
    /// address the user by its new name.
    #[pyo3(signature = (*, username=None, status=None))]
    fn update<'py>(
        &self,
        py: Python<'py>,
        username: Option<String>,
        #[gen_stub(override_type(type_repr = "typing.Literal['active', 'inactive'] | None", imports = ("typing",)))]
        status: Option<&str>,
    ) -> PyResult<Awaitable<'py, ()>> {
        let options = UpdateUserOptions {
            username,
            status: status.map(user_status).transpose()?,
        };
        let user = self.user.clone();
        awaitable(
            py,
            async move { user.update(options).await.map_err(to_pyerr) },
        )
    }

    /// Replace every permission of this user. `None` removes every grant.
    fn set_permissions<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "dict[str, typing.Any] | None", imports = ("typing",)))]
        permissions: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Awaitable<'py, ()>> {
        let permissions = permissions_arg(permissions.as_ref())?;
        let user = self.user.clone();
        awaitable(py, async move {
            user.set_permissions(permissions).await.map_err(to_pyerr)
        })
    }

    /// Change this user's password. The server checks `current_password`
    /// first.
    fn change_password<'py>(
        &self,
        py: Python<'py>,
        current_password: String,
        new_password: String,
    ) -> PyResult<Awaitable<'py, ()>> {
        let user = self.user.clone();
        awaitable(py, async move {
            user.change_password(&current_password, &new_password)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Delete this user. Returns `False` when the user did not exist.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let user = self.user.clone();
        awaitable(py, async move { user.delete().await.map_err(to_pyerr) })
    }

    fn __repr__(&self) -> String {
        format!("User(name={})", self.user.name())
    }
}

fn user_status(word: &str) -> PyResult<UserStatus> {
    word.parse()
        .map_err(|error: laser_sdk::server::UnknownUserStatus| {
            InvalidError::new_err(error.to_string())
        })
}

fn permissions_arg(permissions: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Permissions>> {
    permissions.map(py_to_de::<Permissions>).transpose()
}

/// One user as the server reports it. `Laser.users`, `User.get`, and
/// `User.create` return it. `permissions` is `None` in a listing and for a
/// user without grants.
#[gen_stub_pyclass]
#[pyclass(name = "UserInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyUserInfo {
    info: UserInfo,
}

impl From<UserInfo> for PyUserInfo {
    fn from(info: UserInfo) -> Self {
        Self { info }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyUserInfo {
    /// The native numeric user id.
    #[getter]
    const fn id(&self) -> u32 {
        self.info.id
    }

    #[getter]
    fn username(&self) -> String {
        self.info.username.clone()
    }

    /// `active` or `inactive`.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "typing.Literal['active', 'inactive']", imports = ("typing",)))]
    fn status(&self) -> String {
        self.info.status.to_string()
    }

    #[getter]
    const fn created_at_micros(&self) -> u64 {
        self.info.created_at_micros
    }

    /// The grants as a dict, `None` in a listing and for a user without
    /// grants.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any] | None", imports = ("typing",)))]
    fn permissions(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.info
            .permissions
            .as_ref()
            .map(|permissions| ser_to_py(py, permissions))
            .transpose()
    }

    fn __repr__(&self) -> String {
        format!(
            "UserInfo(id={}, username={}, status={})",
            self.info.id, self.info.username, self.info.status
        )
    }
}

/// One personal access token of the signed-in user, addressed by name. Build
/// it with `Laser.personal_access_token`. A connection string that carries the
/// token value in place of a username and password signs in with it.
#[gen_stub_pyclass]
#[pyclass(name = "PersonalAccessToken", frozen)]
pub struct PyPersonalAccessToken {
    token: PersonalAccessToken,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPersonalAccessToken {
    /// This token's name.
    #[getter]
    fn name(&self) -> String {
        self.token.name().to_owned()
    }

    /// Create this token and return its value. The server shows the value
    /// only once. `expiry_ms` sets how long the token works, and `None` means
    /// it never expires.
    #[pyo3(signature = (expiry_ms=None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        expiry_ms: Option<f64>,
    ) -> PyResult<Awaitable<'py, String>> {
        let expiry = expiry_ms
            .map(|expiry| duration_ms(expiry, "expiry_ms"))
            .transpose()?;
        let token = self.token.clone();
        awaitable(
            py,
            async move { token.create(expiry).await.map_err(to_pyerr) },
        )
    }

    /// Delete this token. Returns `False` when the token did not exist.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let token = self.token.clone();
        awaitable(py, async move { token.delete().await.map_err(to_pyerr) })
    }

    fn __repr__(&self) -> String {
        format!("PersonalAccessToken(name={})", self.token.name())
    }
}

/// One personal access token as `Laser.personal_access_tokens` lists it.
#[gen_stub_pyclass]
#[pyclass(name = "PersonalAccessTokenInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyPersonalAccessTokenInfo {
    #[pyo3(get)]
    name: String,
    /// When the token stops working, `None` for a token that never expires.
    #[pyo3(get)]
    expires_at_micros: Option<u64>,
}

impl From<PersonalAccessTokenInfo> for PyPersonalAccessTokenInfo {
    fn from(token: PersonalAccessTokenInfo) -> Self {
        Self {
            name: token.name,
            expires_at_micros: token.expires_at_micros,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPersonalAccessTokenInfo {
    fn __repr__(&self) -> String {
        format!("PersonalAccessTokenInfo(name={})", self.name)
    }
}

/// One consumer group a connected client is a member of, by native ids.
#[gen_stub_pyclass]
#[pyclass(name = "ClientConsumerGroup", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyClientConsumerGroup {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    group_id: u32,
}

impl From<ClientConsumerGroup> for PyClientConsumerGroup {
    fn from(group: ClientConsumerGroup) -> Self {
        Self {
            stream_id: group.stream_id,
            topic_id: group.topic_id,
            group_id: group.group_id,
        }
    }
}

/// One connected client as the server reports it. `Laser.me`, `Laser.clients`,
/// and `Laser.client_info` return it. `consumer_groups` is empty in a listing.
#[gen_stub_pyclass]
#[pyclass(name = "ClientInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyClientInfo {
    #[pyo3(get)]
    client_id: u32,
    /// The signed-in user, `None` before the client signs in.
    #[pyo3(get)]
    user_id: Option<u32>,
    #[pyo3(get)]
    address: String,
    #[pyo3(get)]
    transport: String,
    #[pyo3(get)]
    consumer_groups_count: u32,
    #[pyo3(get)]
    consumer_groups: Vec<PyClientConsumerGroup>,
}

impl From<ClientInfo> for PyClientInfo {
    fn from(client: ClientInfo) -> Self {
        Self {
            client_id: client.client_id,
            user_id: client.user_id,
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups: client
                .consumer_groups
                .into_iter()
                .map(PyClientConsumerGroup::from)
                .collect(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyClientInfo {
    fn __repr__(&self) -> String {
        format!(
            "ClientInfo(client_id={}, address={}, transport={})",
            self.client_id, self.address, self.transport
        )
    }
}

/// The cache use of one partition, by native ids.
#[gen_stub_pyclass]
#[pyclass(name = "CacheMetrics", frozen)]
#[derive(Clone)]
pub struct PyCacheMetrics {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    partition_id: u32,
    #[pyo3(get)]
    hits: u64,
    #[pyo3(get)]
    misses: u64,
    #[pyo3(get)]
    hit_ratio: f32,
}

impl From<CacheMetrics> for PyCacheMetrics {
    fn from(metrics: CacheMetrics) -> Self {
        Self {
            stream_id: metrics.stream_id,
            topic_id: metrics.topic_id,
            partition_id: metrics.partition_id,
            hits: metrics.hits,
            misses: metrics.misses,
            hit_ratio: metrics.hit_ratio,
        }
    }
}

/// The server statistics that `Laser.stats` returns. Sizes are in bytes and
/// times in microseconds.
#[gen_stub_pyclass]
#[pyclass(name = "ServerStats", frozen)]
#[derive(Clone)]
pub struct PyServerStats {
    #[pyo3(get)]
    process_id: u32,
    #[pyo3(get)]
    cpu_usage: f32,
    #[pyo3(get)]
    total_cpu_usage: f32,
    #[pyo3(get)]
    memory_usage_bytes: u64,
    #[pyo3(get)]
    total_memory_bytes: u64,
    #[pyo3(get)]
    available_memory_bytes: u64,
    #[pyo3(get)]
    run_time_micros: u64,
    #[pyo3(get)]
    start_time_micros: u64,
    #[pyo3(get)]
    read_bytes: u64,
    #[pyo3(get)]
    written_bytes: u64,
    #[pyo3(get)]
    messages_size_bytes: u64,
    #[pyo3(get)]
    streams_count: u32,
    #[pyo3(get)]
    topics_count: u32,
    #[pyo3(get)]
    partitions_count: u32,
    #[pyo3(get)]
    segments_count: u32,
    #[pyo3(get)]
    messages_count: u64,
    #[pyo3(get)]
    clients_count: u32,
    #[pyo3(get)]
    consumer_groups_count: u32,
    #[pyo3(get)]
    hostname: String,
    #[pyo3(get)]
    os_name: String,
    #[pyo3(get)]
    os_version: String,
    #[pyo3(get)]
    kernel_version: String,
    #[pyo3(get)]
    server_version: String,
    #[pyo3(get)]
    threads_count: u32,
    #[pyo3(get)]
    free_disk_space_bytes: u64,
    #[pyo3(get)]
    total_disk_space_bytes: u64,
    #[pyo3(get)]
    open_files_count: u64,
    #[pyo3(get)]
    open_files_limit: u64,
    #[pyo3(get)]
    cache_metrics: Vec<PyCacheMetrics>,
}

impl From<ServerStats> for PyServerStats {
    fn from(stats: ServerStats) -> Self {
        Self {
            process_id: stats.process_id,
            cpu_usage: stats.cpu_usage,
            total_cpu_usage: stats.total_cpu_usage,
            memory_usage_bytes: stats.memory_usage_bytes,
            total_memory_bytes: stats.total_memory_bytes,
            available_memory_bytes: stats.available_memory_bytes,
            run_time_micros: stats.run_time_micros,
            start_time_micros: stats.start_time_micros,
            read_bytes: stats.read_bytes,
            written_bytes: stats.written_bytes,
            messages_size_bytes: stats.messages_size_bytes,
            streams_count: stats.streams_count,
            topics_count: stats.topics_count,
            partitions_count: stats.partitions_count,
            segments_count: stats.segments_count,
            messages_count: stats.messages_count,
            clients_count: stats.clients_count,
            consumer_groups_count: stats.consumer_groups_count,
            hostname: stats.hostname,
            os_name: stats.os_name,
            os_version: stats.os_version,
            kernel_version: stats.kernel_version,
            server_version: stats.server_version,
            threads_count: stats.threads_count,
            free_disk_space_bytes: stats.free_disk_space_bytes,
            total_disk_space_bytes: stats.total_disk_space_bytes,
            open_files_count: stats.open_files_count,
            open_files_limit: stats.open_files_limit,
            cache_metrics: stats
                .cache_metrics
                .into_iter()
                .map(PyCacheMetrics::from)
                .collect(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyServerStats {
    fn __repr__(&self) -> String {
        format!(
            "ServerStats(server_version={}, streams_count={}, clients_count={})",
            self.server_version, self.streams_count, self.clients_count
        )
    }
}

/// One node of the cluster. A port is 0 when the node does not serve that
/// transport. `role` is `leader` or `follower`, and `status` is `healthy`,
/// `starting`, `stopping`, `unreachable`, `maintenance`, or `unknown`.
#[gen_stub_pyclass]
#[pyclass(name = "ClusterNode", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyClusterNode {
    #[pyo3(get)]
    replica_id: u8,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    ip: String,
    #[pyo3(get)]
    tcp_port: u16,
    #[pyo3(get)]
    quic_port: u16,
    #[pyo3(get)]
    http_port: u16,
    #[pyo3(get)]
    websocket_port: u16,
    #[pyo3(get)]
    role: String,
    #[pyo3(get)]
    status: String,
}

/// The cluster as `Laser.cluster` reports it. A single server reports one
/// node.
#[gen_stub_pyclass]
#[pyclass(name = "ClusterInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyClusterInfo {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    nodes: Vec<PyClusterNode>,
}

impl From<ClusterInfo> for PyClusterInfo {
    fn from(cluster: ClusterInfo) -> Self {
        Self {
            name: cluster.name,
            nodes: cluster
                .nodes
                .into_iter()
                .map(|node| PyClusterNode {
                    replica_id: node.replica_id,
                    name: node.name,
                    ip: node.ip,
                    tcp_port: node.tcp_port,
                    quic_port: node.quic_port,
                    http_port: node.http_port,
                    websocket_port: node.websocket_port,
                    role: node.role.to_string(),
                    status: node.status.to_string(),
                })
                .collect(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyClusterInfo {
    fn __repr__(&self) -> String {
        format!(
            "ClusterInfo(name={}, nodes={})",
            self.name,
            self.nodes.len()
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Every option the server accepts for `scope`, `topic`, `stream`, or
    /// `user`, with its type, rules, default, and description.
    fn describe_options<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "typing.Literal['topic', 'stream', 'user']", imports = ("typing",)))]
        scope: String,
    ) -> PyResult<Awaitable<'py, Vec<PyOptionInfo>>> {
        let scope: OptionsScope = scope.parse().map_err(to_pyerr)?;
        let laser = self.inner.clone();
        awaitable(py, async move {
            let options = laser.describe_options(scope).await.map_err(to_pyerr)?;
            Ok(options.into_iter().map(PyOptionInfo::from).collect())
        })
    }

    /// One page of the server's metadata changes, or of its current state.
    /// `start` is a `MetadataChangeStart`, the earliest record by default.
    /// `groups` names the record families to read, every group when empty.
    /// `since_micros` and `until_micros` keep records stamped inside the
    /// inclusive range with the earliest and after starts. `limit` is 1 to
    /// 1000. Needs the manage servers permission.
    #[pyo3(signature = (start=None, *, groups=None, since_micros=None, until_micros=None, limit=100))]
    fn metadata_changes<'py>(
        &self,
        py: Python<'py>,
        start: Option<PyRef<'_, PyMetadataChangeStart>>,
        #[gen_stub(override_type(type_repr = "typing.Optional[typing.Sequence[typing.Literal['streams', 'topics', 'partitions', 'consumer_groups', 'consumer_group_members', 'users', 'personal_access_tokens', 'sessions']]]", imports = ("typing",)))]
        groups: Option<Vec<String>>,
        since_micros: Option<u64>,
        until_micros: Option<u64>,
        limit: u32,
    ) -> PyResult<Awaitable<'py, PyMetadataChanges>> {
        let query = MetadataChangesQuery {
            start: start.map(|start| start.start.clone()).unwrap_or_default(),
            groups: groups
                .unwrap_or_default()
                .iter()
                .map(|group| group.parse::<MetadataChangeGroup>())
                .collect::<Result<_, _>>()
                .map_err(to_pyerr)?,
            since_micros,
            until_micros,
            limit,
        };
        let laser = self.inner.clone();
        awaitable(py, async move {
            Ok(laser
                .metadata_changes(query)
                .await
                .map_err(to_pyerr)?
                .into())
        })
    }
}

/// One option the server accepts for a scope. `kind` names the value type,
/// such as `uint64`, `bool`, or `string`. `default_value` is the default in
/// its text form, `None` when the option has no default. Integers are decimal,
/// floats are the `0x` hex of their bits, and raw bytes are base64.
/// `updatable` and `resettable` say what an update may do, `secret` options
/// are never returned, `read_only` options are never accepted, and a node
/// that does not know a `critical` option must not serve the resource.
#[gen_stub_pyclass]
#[pyclass(name = "OptionInfo", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyOptionInfo {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    updatable: bool,
    #[pyo3(get)]
    resettable: bool,
    #[pyo3(get)]
    secret: bool,
    #[pyo3(get)]
    read_only: bool,
    #[pyo3(get)]
    critical: bool,
    #[pyo3(get)]
    default_value: Option<String>,
    #[pyo3(get)]
    description: String,
}

impl From<OptionInfo> for PyOptionInfo {
    fn from(option: OptionInfo) -> Self {
        Self {
            key: option.key,
            kind: option.kind,
            updatable: option.updatable,
            resettable: option.resettable,
            secret: option.secret,
            read_only: option.read_only,
            critical: option.critical,
            default_value: option.default_value,
            description: option.description,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyOptionInfo {
    fn __repr__(&self) -> String {
        format!("OptionInfo(key={}, kind={})", self.key, self.kind)
    }
}

/// Where `Laser.metadata_changes` starts. `after` takes the `next` cursor of
/// an earlier page. `current` pages through the current state, starting a
/// scan without a token and continuing it with the `page_token` of the page
/// before.
#[gen_stub_pyclass]
#[pyclass(name = "MetadataChangeStart", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyMetadataChangeStart {
    start: MetadataChangeStart,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMetadataChangeStart {
    /// The oldest record the requested groups still hold completely.
    #[staticmethod]
    const fn earliest() -> Self {
        Self {
            start: MetadataChangeStart::Earliest,
        }
    }

    /// No records, only a cursor at the newest record.
    #[staticmethod]
    const fn latest() -> Self {
        Self {
            start: MetadataChangeStart::Latest,
        }
    }

    /// The records after `cursor`.
    #[staticmethod]
    const fn after(cursor: String) -> Self {
        Self {
            start: MetadataChangeStart::After(cursor),
        }
    }

    /// A page of the current state, the first without `page_token`.
    #[staticmethod]
    #[pyo3(signature = (page_token=None))]
    const fn current(page_token: Option<String>) -> Self {
        Self {
            start: MetadataChangeStart::Current(page_token),
        }
    }

    fn __repr__(&self) -> String {
        format!("MetadataChangeStart({:?})", self.start)
    }
}

/// What one change record or current state item is about. Each kind fills
/// the fields that name its resource and what changed, and leaves the rest
/// `None` or empty. Ids are native ids. A `*_created_op` or
/// `user_created_at_micros` tells a recreated resource apart from an older one
/// with the same id. Resource options are not carried.
#[gen_stub_pyclass]
#[pyclass(name = "MetadataSubject", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyMetadataSubject {
    subject: MetadataSubject,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMetadataSubject {
    #[getter]
    const fn stream_id(&self) -> Option<u32> {
        self.subject.stream_id
    }

    #[getter]
    const fn stream_created_op(&self) -> Option<u64> {
        self.subject.stream_created_op
    }

    #[getter]
    const fn topic_id(&self) -> Option<u32> {
        self.subject.topic_id
    }

    #[getter]
    const fn topic_created_op(&self) -> Option<u64> {
        self.subject.topic_created_op
    }

    #[getter]
    const fn group_id(&self) -> Option<u32> {
        self.subject.group_id
    }

    #[getter]
    const fn member_id(&self) -> Option<u32> {
        self.subject.member_id
    }

    #[getter]
    const fn partition_id(&self) -> Option<u32> {
        self.subject.partition_id
    }

    #[getter]
    const fn user_id(&self) -> Option<u32> {
        self.subject.user_id
    }

    #[getter]
    const fn user_created_at_micros(&self) -> Option<u64> {
        self.subject.user_created_at_micros
    }

    #[getter]
    const fn client_id(&self) -> Option<u128> {
        self.subject.client_id
    }

    #[getter]
    const fn session(&self) -> Option<u64> {
        self.subject.session
    }

    /// The stream, topic, consumer group, or personal access token name.
    #[getter]
    fn name(&self) -> Option<String> {
        self.subject.name.clone()
    }

    #[getter]
    fn username(&self) -> Option<String> {
        self.subject.username.clone()
    }

    #[getter]
    const fn created_at_micros(&self) -> Option<u64> {
        self.subject.created_at_micros
    }

    #[getter]
    const fn partitions_count(&self) -> Option<u32> {
        self.subject.partitions_count
    }

    #[getter]
    fn partition_ids(&self) -> Vec<u32> {
        self.subject.partition_ids.clone()
    }

    #[getter]
    fn removed_partition_ids(&self) -> Vec<u32> {
        self.subject.removed_partition_ids.clone()
    }

    #[getter]
    const fn consumer_groups_count(&self) -> Option<u32> {
        self.subject.consumer_groups_count
    }

    #[getter]
    const fn members_count(&self) -> Option<u32> {
        self.subject.members_count
    }

    /// The op of the request a deferred delete answers.
    #[getter]
    const fn requested_op(&self) -> Option<u64> {
        self.subject.requested_op
    }

    /// `active` or `inactive` for a user.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "typing.Optional[typing.Literal['active', 'inactive']]", imports = ("typing",)))]
    fn status(&self) -> Option<String> {
        self.subject.status.map(|status| status.to_string())
    }

    /// The user's grants as a dict, shaped as `User.set_permissions` takes
    /// them.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any] | None", imports = ("typing",)))]
    fn permissions(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.subject
            .permissions
            .as_ref()
            .map(|permissions| ser_to_py(py, permissions))
            .transpose()
    }

    #[getter]
    const fn expires_at_micros(&self) -> Option<u64> {
        self.subject.expires_at_micros
    }

    /// Why a member left, a token was deleted, a session closed, or a user's
    /// permissions changed.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "typing.Optional[typing.Literal['left', 'session_ended', 'deleted', 'expired', 'logout', 'revoked', 'updated', 'resource_deleted']]", imports = ("typing",)))]
    fn reason(&self) -> Option<String> {
        self.subject.reason.map(|reason| reason.to_string())
    }

    fn __repr__(&self) -> String {
        format!("MetadataSubject({:?})", self.subject)
    }
}

/// One committed metadata change. `kind` is a change kind word such as
/// `stream_created`, `None` for a kind this client does not know, and its
/// subject is then empty. `actor_user_id` is `None` for a change the server
/// made itself.
#[gen_stub_pyclass]
#[pyclass(name = "MetadataChangeRecord", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyMetadataChangeRecord {
    #[pyo3(get)]
    op: u64,
    #[pyo3(get)]
    ordinal: u32,
    #[pyo3(get)]
    timestamp_micros: u64,
    #[pyo3(get)]
    actor_user_id: Option<u32>,
    #[pyo3(get)]
    kind: Option<String>,
    #[pyo3(get)]
    subject: PyMetadataSubject,
}

impl From<MetadataChangeRecord> for PyMetadataChangeRecord {
    fn from(record: MetadataChangeRecord) -> Self {
        Self {
            op: record.op,
            ordinal: record.ordinal,
            timestamp_micros: record.timestamp_micros,
            actor_user_id: record.actor_user_id,
            kind: record.kind.map(|kind| kind.to_string()),
            subject: PyMetadataSubject {
                subject: record.subject,
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMetadataChangeRecord {
    fn __repr__(&self) -> String {
        format!(
            "MetadataChangeRecord(op={}, ordinal={}, kind={:?})",
            self.op, self.ordinal, self.kind
        )
    }
}

/// One resource of the current state. `group` is a group word such as
/// `streams`, `None` for a group this client does not know, and its subject
/// is then empty.
#[gen_stub_pyclass]
#[pyclass(name = "MetadataItem", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyMetadataItem {
    #[pyo3(get)]
    group: Option<String>,
    #[pyo3(get)]
    subject: PyMetadataSubject,
}

impl From<MetadataItem> for PyMetadataItem {
    fn from(item: MetadataItem) -> Self {
        Self {
            group: item.group.map(|group| group.to_string()),
            subject: PyMetadataSubject {
                subject: item.subject,
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMetadataItem {
    fn __repr__(&self) -> String {
        format!("MetadataItem(group={:?})", self.group)
    }
}

/// One page of metadata changes, or of the current state. `status` is `ok`,
/// `behind` (the node has not applied the start yet, ask again), `expired`
/// (the node no longer holds every record after the start), or `reset` (the
/// start belongs to another history). After `expired` or `reset`, read the
/// current state again. `phase` is `changes` or `current`. Pass `next` to
/// `MetadataChangeStart.after` to read on, and `page_token` to
/// `MetadataChangeStart.current` to continue a current state scan.
#[gen_stub_pyclass]
#[pyclass(name = "MetadataChanges", frozen, eq)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyMetadataChanges {
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    phase: String,
    #[pyo3(get)]
    head_op: u64,
    #[pyo3(get)]
    oldest_op: u64,
    #[pyo3(get)]
    retained_since_micros: u64,
    #[pyo3(get)]
    next: Option<String>,
    #[pyo3(get)]
    has_more: bool,
    #[pyo3(get)]
    records: Vec<PyMetadataChangeRecord>,
    #[pyo3(get)]
    items: Vec<PyMetadataItem>,
    #[pyo3(get)]
    page_token: Option<String>,
}

impl From<MetadataChanges> for PyMetadataChanges {
    fn from(page: MetadataChanges) -> Self {
        Self {
            status: page.status.to_string(),
            phase: page.phase.to_string(),
            head_op: page.head_op,
            oldest_op: page.oldest_op,
            retained_since_micros: page.retained_since_micros,
            next: page.next,
            has_more: page.has_more,
            records: page
                .records
                .into_iter()
                .map(PyMetadataChangeRecord::from)
                .collect(),
            items: page.items.into_iter().map(PyMetadataItem::from).collect(),
            page_token: page.page_token,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMetadataChanges {
    fn __repr__(&self) -> String {
        format!(
            "MetadataChanges(status={}, phase={}, records={}, items={}, has_more={})",
            self.status,
            self.phase,
            self.records.len(),
            self.items.len(),
            self.has_more
        )
    }
}
