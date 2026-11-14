use crate::error::LaserError;
use crate::laser::Laser;
use bytes::Bytes;
use iggy::prelude::{
    Client, ClusterClient, Identifier, IggyDuration, IggyError, IggyExpiry,
    PersonalAccessTokenClient, SnapshotCompression, SystemClient, SystemSnapshotType, UserClient,
    UserCreateOptions, UserUpdateOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::time::{Duration, Instant};

/// Whether a user can sign in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    #[default]
    Active,
    Inactive,
}

/// A user status word other than `active` or `inactive`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown user status `{0}`, expected `active` or `inactive`")]
pub struct UnknownUserStatus(pub String);

impl From<UnknownUserStatus> for LaserError {
    fn from(error: UnknownUserStatus) -> Self {
        Self::Invalid(error.to_string())
    }
}

impl Display for UserStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Active => "active",
            Self::Inactive => "inactive",
        })
    }
}

impl FromStr for UserStatus {
    type Err = UnknownUserStatus;

    fn from_str(word: &str) -> Result<Self, Self::Err> {
        match word {
            "active" => Ok(Self::Active),
            "inactive" => Ok(Self::Inactive),
            other => Err(UnknownUserStatus(other.to_owned())),
        }
    }
}

impl From<iggy::prelude::UserStatus> for UserStatus {
    fn from(status: iggy::prelude::UserStatus) -> Self {
        match status {
            iggy::prelude::UserStatus::Active => Self::Active,
            iggy::prelude::UserStatus::Inactive => Self::Inactive,
        }
    }
}

impl From<UserStatus> for iggy::prelude::UserStatus {
    fn from(status: UserStatus) -> Self {
        match status {
            UserStatus::Active => Self::Active,
            UserStatus::Inactive => Self::Inactive,
        }
    }
}

/// What a user may do. `global` applies to the whole server, and `streams`
/// adds grants on single streams, keyed by the native stream id.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub global: GlobalPermissions,
    #[serde(default)]
    pub streams: BTreeMap<u32, StreamPermissions>,
}

/// Grants that apply to the whole server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GlobalPermissions {
    pub manage_servers: bool,
    pub read_servers: bool,
    pub manage_users: bool,
    pub read_users: bool,
    pub manage_streams: bool,
    pub read_streams: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

/// Grants on one stream. `topics` adds grants on single topics of the
/// stream, keyed by the native topic id.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamPermissions {
    pub manage_stream: bool,
    pub read_stream: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
    pub topics: BTreeMap<u32, TopicPermissions>,
}

/// Grants on one topic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicPermissions {
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

impl From<iggy::prelude::Permissions> for Permissions {
    fn from(permissions: iggy::prelude::Permissions) -> Self {
        let global = permissions.global;
        Self {
            global: GlobalPermissions {
                manage_servers: global.manage_servers,
                read_servers: global.read_servers,
                manage_users: global.manage_users,
                read_users: global.read_users,
                manage_streams: global.manage_streams,
                read_streams: global.read_streams,
                manage_topics: global.manage_topics,
                read_topics: global.read_topics,
                poll_messages: global.poll_messages,
                send_messages: global.send_messages,
            },
            streams: permissions
                .streams
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(id, stream)| Some((u32::try_from(id).ok()?, stream.into())))
                .collect(),
        }
    }
}

impl From<iggy::prelude::StreamPermissions> for StreamPermissions {
    fn from(stream: iggy::prelude::StreamPermissions) -> Self {
        Self {
            manage_stream: stream.manage_stream,
            read_stream: stream.read_stream,
            manage_topics: stream.manage_topics,
            read_topics: stream.read_topics,
            poll_messages: stream.poll_messages,
            send_messages: stream.send_messages,
            topics: stream
                .topics
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(id, topic)| {
                    Some((
                        u32::try_from(id).ok()?,
                        TopicPermissions {
                            manage_topic: topic.manage_topic,
                            read_topic: topic.read_topic,
                            poll_messages: topic.poll_messages,
                            send_messages: topic.send_messages,
                        },
                    ))
                })
                .collect(),
        }
    }
}

impl From<Permissions> for iggy::prelude::Permissions {
    fn from(permissions: Permissions) -> Self {
        let global = permissions.global;
        Self {
            global: iggy::prelude::GlobalPermissions {
                manage_servers: global.manage_servers,
                read_servers: global.read_servers,
                manage_users: global.manage_users,
                read_users: global.read_users,
                manage_streams: global.manage_streams,
                read_streams: global.read_streams,
                manage_topics: global.manage_topics,
                read_topics: global.read_topics,
                poll_messages: global.poll_messages,
                send_messages: global.send_messages,
            },
            streams: (!permissions.streams.is_empty()).then(|| {
                permissions
                    .streams
                    .into_iter()
                    .map(|(id, stream)| (id as usize, stream.into()))
                    .collect()
            }),
        }
    }
}

impl From<StreamPermissions> for iggy::prelude::StreamPermissions {
    fn from(stream: StreamPermissions) -> Self {
        Self {
            manage_stream: stream.manage_stream,
            read_stream: stream.read_stream,
            manage_topics: stream.manage_topics,
            read_topics: stream.read_topics,
            poll_messages: stream.poll_messages,
            send_messages: stream.send_messages,
            topics: (!stream.topics.is_empty()).then(|| {
                stream
                    .topics
                    .into_iter()
                    .map(|(id, topic)| {
                        (
                            id as usize,
                            iggy::prelude::TopicPermissions {
                                manage_topic: topic.manage_topic,
                                read_topic: topic.read_topic,
                                poll_messages: topic.poll_messages,
                                send_messages: topic.send_messages,
                            },
                        )
                    })
                    .collect()
            }),
        }
    }
}

/// One user as the server reports it. `permissions` is `None` in a listing
/// and for a user without grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserInfo {
    /// The native numeric user id.
    pub id: u32,
    pub username: String,
    pub status: UserStatus,
    pub created_at_micros: u64,
    pub permissions: Option<Permissions>,
}

impl From<iggy::prelude::UserInfo> for UserInfo {
    fn from(user: iggy::prelude::UserInfo) -> Self {
        Self {
            id: user.id,
            username: user.username,
            status: user.status.into(),
            created_at_micros: user.created_at.as_micros(),
            permissions: None,
        }
    }
}

impl From<iggy::prelude::UserInfoDetails> for UserInfo {
    fn from(user: iggy::prelude::UserInfoDetails) -> Self {
        Self {
            id: user.id,
            username: user.username,
            status: user.status.into(),
            created_at_micros: user.created_at.as_micros(),
            permissions: user.permissions.map(Permissions::from),
        }
    }
}

/// The settings of a new user. A user starts active without grants.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CreateUserOptions {
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
}

/// The changes [`User::update`] applies. A field left `None` keeps its value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateUserOptions {
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

/// One user of the server, addressed by name. Build it with [`Laser::user`].
/// Each verb needs the matching user permission on this connection.
#[derive(Clone)]
pub struct User {
    laser: Laser,
    name: String,
}

impl User {
    /// This user's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This user with its permissions, `None` when it does not exist.
    pub async fn get(&self) -> Result<Option<UserInfo>, LaserError> {
        match self.laser.client().get_user(&self.identifier()?).await {
            Ok(found) => Ok(found.map(UserInfo::from)),
            Err(error) if is_missing_user(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this user exists.
    pub async fn exists(&self) -> Result<bool, LaserError> {
        Ok(self.get().await?.is_some())
    }

    /// Create this user with `password`. An existing user of the same name
    /// fails the call.
    pub async fn create(
        &self,
        password: &str,
        options: CreateUserOptions,
    ) -> Result<UserInfo, LaserError> {
        let user = self
            .laser
            .client()
            .create_user(
                &self.name,
                password,
                options.status.into(),
                options.permissions.map(Into::into),
                &UserCreateOptions::default(),
            )
            .await?;
        Ok(user.into())
    }

    /// Rename this user or change its status. A deactivated user can no
    /// longer sign in. After a rename, address the user by its new name.
    pub async fn update(&self, options: UpdateUserOptions) -> Result<(), LaserError> {
        let update = UserUpdateOptions {
            username: options.username,
            status: options.status.map(Into::into),
            ..UserUpdateOptions::default()
        };
        self.laser
            .client()
            .update_user(&self.identifier()?, &update)
            .await?;
        Ok(())
    }

    /// Replace every permission of this user. `None` removes every grant.
    pub async fn set_permissions(
        &self,
        permissions: Option<Permissions>,
    ) -> Result<(), LaserError> {
        self.laser
            .client()
            .update_permissions(&self.identifier()?, permissions.map(Into::into))
            .await?;
        Ok(())
    }

    /// Change this user's password. The server checks `current_password`
    /// first.
    pub async fn change_password(
        &self,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), LaserError> {
        self.laser
            .client()
            .change_password(&self.identifier()?, current_password, new_password)
            .await?;
        Ok(())
    }

    /// Delete this user. Returns `false` when the user did not exist.
    pub async fn delete(&self) -> Result<bool, LaserError> {
        let identifier = self.identifier()?;
        let client = self.laser.client();
        match client.delete_user(&identifier).await {
            Ok(()) => Ok(true),
            Err(error) if is_missing_user(&error) => Ok(false),
            Err(error) => match client.get_user(&identifier).await {
                Ok(None) => Ok(false),
                _ => Err(error.into()),
            },
        }
    }

    fn identifier(&self) -> Result<Identifier, LaserError> {
        Ok(Identifier::named(&self.name)?)
    }
}

fn is_missing_user(error: &IggyError) -> bool {
    matches!(error, IggyError::ResourceNotFound(_))
}

/// One personal access token of the signed-in user. The server never returns
/// the token value again after [`PersonalAccessToken::create`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersonalAccessTokenInfo {
    pub name: String,
    /// When the token stops working, `None` for a token that never expires.
    pub expires_at_micros: Option<u64>,
}

impl From<iggy_common::PersonalAccessTokenInfo> for PersonalAccessTokenInfo {
    fn from(token: iggy_common::PersonalAccessTokenInfo) -> Self {
        Self {
            name: token.name,
            expires_at_micros: token.expiry_at.map(|at| at.as_micros()),
        }
    }
}

/// One personal access token of the signed-in user, addressed by name. Build
/// it with [`Laser::personal_access_token`]. A connection string that carries
/// the token value in place of a username and password signs in with it.
#[derive(Clone)]
pub struct PersonalAccessToken {
    laser: Laser,
    name: String,
}

impl PersonalAccessToken {
    /// This token's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create this token and return its value. The server shows the value
    /// only once. `expiry` sets how long the token works, and `None` means it
    /// never expires.
    pub async fn create(&self, expiry: Option<Duration>) -> Result<String, LaserError> {
        let expiry = expiry.map_or(IggyExpiry::NeverExpire, |expiry| {
            IggyExpiry::ExpireDuration(IggyDuration::new(expiry))
        });
        let token = self
            .laser
            .client()
            .create_personal_access_token(&self.name, expiry)
            .await?;
        Ok(token.token)
    }

    /// Delete this token. Returns `false` when the token did not exist.
    pub async fn delete(&self) -> Result<bool, LaserError> {
        let client = self.laser.client();
        let exists = client
            .get_personal_access_tokens()
            .await?
            .iter()
            .any(|token| token.name == self.name);
        if !exists {
            return Ok(false);
        }
        client.delete_personal_access_token(&self.name).await?;
        Ok(true)
    }
}

/// One consumer group a connected client is a member of, by native ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConsumerGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

/// One connected client as the server reports it. `consumer_groups` is empty
/// in a listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientInfo {
    pub client_id: u32,
    /// The signed-in user, `None` before the client signs in.
    pub user_id: Option<u32>,
    pub address: String,
    pub transport: String,
    pub consumer_groups_count: u32,
    pub consumer_groups: Vec<ClientConsumerGroup>,
}

impl From<iggy_common::ClientInfo> for ClientInfo {
    fn from(client: iggy_common::ClientInfo) -> Self {
        Self {
            client_id: client.client_id,
            user_id: client.user_id,
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups: Vec::new(),
        }
    }
}

impl From<iggy::prelude::ClientInfoDetails> for ClientInfo {
    fn from(client: iggy::prelude::ClientInfoDetails) -> Self {
        Self {
            client_id: client.client_id,
            user_id: client.user_id,
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups: client
                .consumer_groups
                .into_iter()
                .map(|group| ClientConsumerGroup {
                    stream_id: group.stream_id,
                    topic_id: group.topic_id,
                    group_id: group.group_id,
                })
                .collect(),
        }
    }
}

/// The cache use of one partition, by native ids.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CacheMetrics {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub hits: u64,
    pub misses: u64,
    pub hit_ratio: f32,
}

/// The server statistics. Sizes are in bytes and times in microseconds.
#[derive(Clone, Debug, PartialEq)]
pub struct ServerStats {
    pub process_id: u32,
    pub cpu_usage: f32,
    pub total_cpu_usage: f32,
    pub memory_usage_bytes: u64,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub run_time_micros: u64,
    pub start_time_micros: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub messages_size_bytes: u64,
    pub streams_count: u32,
    pub topics_count: u32,
    pub partitions_count: u32,
    pub segments_count: u32,
    pub messages_count: u64,
    pub clients_count: u32,
    pub consumer_groups_count: u32,
    pub hostname: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub server_version: String,
    pub threads_count: u32,
    pub free_disk_space_bytes: u64,
    pub total_disk_space_bytes: u64,
    pub open_files_count: u64,
    pub open_files_limit: u64,
    pub cache_metrics: Vec<CacheMetrics>,
}

impl From<iggy::prelude::Stats> for ServerStats {
    fn from(stats: iggy::prelude::Stats) -> Self {
        let mut cache_metrics: Vec<CacheMetrics> = stats
            .cache_metrics
            .into_iter()
            .map(|(key, metrics)| CacheMetrics {
                stream_id: key.stream_id,
                topic_id: key.topic_id,
                partition_id: key.partition_id,
                hits: metrics.hits,
                misses: metrics.misses,
                hit_ratio: metrics.hit_ratio,
            })
            .collect();
        cache_metrics
            .sort_by_key(|metrics| (metrics.stream_id, metrics.topic_id, metrics.partition_id));
        Self {
            process_id: stats.process_id,
            cpu_usage: stats.cpu_usage,
            total_cpu_usage: stats.total_cpu_usage,
            memory_usage_bytes: stats.memory_usage.as_bytes_u64(),
            total_memory_bytes: stats.total_memory.as_bytes_u64(),
            available_memory_bytes: stats.available_memory.as_bytes_u64(),
            run_time_micros: stats.run_time.as_micros(),
            start_time_micros: stats.start_time.as_micros(),
            read_bytes: stats.read_bytes.as_bytes_u64(),
            written_bytes: stats.written_bytes.as_bytes_u64(),
            messages_size_bytes: stats.messages_size_bytes.as_bytes_u64(),
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
            server_version: stats.iggy_server_version,
            threads_count: stats.threads_count,
            free_disk_space_bytes: stats.free_disk_space.as_bytes_u64(),
            total_disk_space_bytes: stats.total_disk_space.as_bytes_u64(),
            open_files_count: stats.open_files_count,
            open_files_limit: stats.open_files_limit,
            cache_metrics,
        }
    }
}

/// The role of one cluster node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClusterNodeRole {
    Leader,
    Follower,
}

/// The state of one cluster node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClusterNodeStatus {
    Healthy,
    Starting,
    Stopping,
    Unreachable,
    Maintenance,
    Unknown,
}

impl Display for ClusterNodeRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
        })
    }
}

impl Display for ClusterNodeStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Healthy => "healthy",
            Self::Starting => "starting",
            Self::Stopping => "stopping",
            Self::Unreachable => "unreachable",
            Self::Maintenance => "maintenance",
            Self::Unknown => "unknown",
        })
    }
}

impl From<iggy::prelude::ClusterNodeRole> for ClusterNodeRole {
    fn from(role: iggy::prelude::ClusterNodeRole) -> Self {
        match role {
            iggy::prelude::ClusterNodeRole::Leader => Self::Leader,
            iggy::prelude::ClusterNodeRole::Follower => Self::Follower,
        }
    }
}

impl From<iggy::prelude::ClusterNodeStatus> for ClusterNodeStatus {
    fn from(status: iggy::prelude::ClusterNodeStatus) -> Self {
        match status {
            iggy::prelude::ClusterNodeStatus::Healthy => Self::Healthy,
            iggy::prelude::ClusterNodeStatus::Starting => Self::Starting,
            iggy::prelude::ClusterNodeStatus::Stopping => Self::Stopping,
            iggy::prelude::ClusterNodeStatus::Unreachable => Self::Unreachable,
            iggy::prelude::ClusterNodeStatus::Maintenance => Self::Maintenance,
            iggy::prelude::ClusterNodeStatus::Unknown => Self::Unknown,
        }
    }
}

/// One node of the cluster. A port is 0 when the node does not serve that
/// transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterNode {
    pub replica_id: u8,
    pub name: String,
    pub ip: String,
    pub tcp_port: u16,
    pub quic_port: u16,
    pub http_port: u16,
    pub websocket_port: u16,
    pub role: ClusterNodeRole,
    pub status: ClusterNodeStatus,
}

/// The cluster as the server reports it. A single server reports one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterInfo {
    pub name: String,
    pub nodes: Vec<ClusterNode>,
}

impl From<iggy::prelude::ClusterMetadata> for ClusterInfo {
    fn from(cluster: iggy::prelude::ClusterMetadata) -> Self {
        Self {
            name: cluster.name,
            nodes: cluster
                .nodes
                .into_iter()
                .map(|node| ClusterNode {
                    replica_id: node.replica_id,
                    name: node.name,
                    ip: node.ip,
                    tcp_port: node.endpoints.tcp,
                    quic_port: node.endpoints.quic,
                    http_port: node.endpoints.http,
                    websocket_port: node.endpoints.websocket,
                    role: node.role.into(),
                    status: node.status.into(),
                })
                .collect(),
        }
    }
}

impl Laser {
    /// Every user of the server, without permissions. Needs the read users
    /// permission.
    pub async fn users(&self) -> Result<Vec<UserInfo>, LaserError> {
        let users = self.client().get_users().await?;
        Ok(users.into_iter().map(UserInfo::from).collect())
    }

    /// The user accessor. Free and synchronous, IO happens at the verbs.
    pub fn user(&self, name: impl Into<String>) -> User {
        User {
            laser: self.clone(),
            name: name.into(),
        }
    }

    /// Every personal access token of the signed-in user.
    pub async fn personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, LaserError> {
        let tokens = self.client().get_personal_access_tokens().await?;
        Ok(tokens
            .into_iter()
            .map(PersonalAccessTokenInfo::from)
            .collect())
    }

    /// The personal access token accessor for the signed-in user. Free and
    /// synchronous, IO happens at the verbs.
    pub fn personal_access_token(&self, name: impl Into<String>) -> PersonalAccessToken {
        PersonalAccessToken {
            laser: self.clone(),
            name: name.into(),
        }
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<(), LaserError> {
        self.client().ping().await?;
        Ok(())
    }

    /// The server statistics. Needs the read servers permission.
    pub async fn stats(&self) -> Result<ServerStats, LaserError> {
        Ok(self.client().get_stats().await?.into())
    }

    /// This connection as the server sees it.
    pub async fn me(&self) -> Result<ClientInfo, LaserError> {
        Ok(self.client().get_me().await?.into())
    }

    /// Every client connected to the server. Needs the read servers
    /// permission.
    pub async fn clients(&self) -> Result<Vec<ClientInfo>, LaserError> {
        let clients = self.client().get_clients().await?;
        Ok(clients.into_iter().map(ClientInfo::from).collect())
    }

    /// The cluster name and its nodes with their roles and states.
    pub async fn cluster(&self) -> Result<ClusterInfo, LaserError> {
        Ok(self.client().get_cluster_metadata().await?.into())
    }

    /// A server diagnostics snapshot as the bytes of a ZIP archive, with every
    /// section the server collects: file system overview, process list,
    /// resource use, server logs, and server configuration. Needs the manage
    /// servers permission.
    pub async fn snapshot(&self) -> Result<Vec<u8>, LaserError> {
        let snapshot = self
            .client()
            .snapshot(SnapshotCompression::Deflated, vec![SystemSnapshotType::All])
            .await?;
        Ok(snapshot.0)
    }

    /// One connected client with its consumer groups, `None` when no client
    /// has `client_id`. Needs the read servers permission.
    pub async fn client_info(&self, client_id: u32) -> Result<Option<ClientInfo>, LaserError> {
        Ok(self
            .client()
            .get_client(client_id)
            .await?
            .map(ClientInfo::from))
    }

    /// Prove the outcome of the request `operation` that
    /// [`LaserError::uncertain_operation`] names, waiting at most `timeout`.
    /// `Ok` means the request committed. A committed refusal returns its own
    /// error. Either outcome frees the requests the operation blocked. A
    /// failure that names the operation again leaves it unknown.
    pub async fn resolve_operation(
        &self,
        operation: u64,
        timeout: Duration,
    ) -> Result<(), LaserError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| LaserError::Invalid("the operation timeout is too long".to_owned()))?;
        self.client().resolve_operation(operation, deadline).await?;
        Ok(())
    }

    /// Give up the request `operation` that
    /// [`LaserError::uncertain_operation`] names, and free the requests it
    /// blocked. Its outcome stays unknown.
    pub async fn abandon_operation(&self, operation: u64) -> Result<(), LaserError> {
        self.client().abandon_operation(operation).await?;
        Ok(())
    }

    /// Send the command `code` with a raw payload and return the raw reply.
    /// The client picks the node, as it does for the same typed call. The
    /// server checks the permissions of the command. The sign-in, sign-out,
    /// and session commands are refused before anything is sent, because only
    /// the connection sends them.
    pub async fn send_command(
        &self,
        code: u32,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Vec<u8>, LaserError> {
        let reply = self
            .client()
            .send_binary_request(code, Bytes::from(payload.into()))
            .await?;
        Ok(reply.to_vec())
    }

    /// Send the command `code` with a raw payload to the replica `replica`,
    /// as [`cluster`](Self::cluster) numbers it, and return that replica's own
    /// reply, for example the statistics each node keeps. Only a command that
    /// a node answers from its own state goes to a replica. Another command,
    /// or a replica the cluster does not name, is refused before anything is
    /// sent. Nothing is retried on another replica.
    pub async fn send_command_to_replica(
        &self,
        code: u32,
        payload: impl Into<Vec<u8>>,
        replica: u8,
    ) -> Result<Vec<u8>, LaserError> {
        let reply = self
            .client()
            .send_binary_request_to_replica(code, Bytes::from(payload.into()), replica)
            .await?;
        Ok(reply.to_vec())
    }
}

// The conversions and words of an enum that mirrors an Apache Iggy enum
// variant for variant.
macro_rules! mirror_words {
    ($name:ident, $native:ident, [$($variant:ident),+ $(,)?]) => {
        impl From<iggy::prelude::$native> for $name {
            fn from(value: iggy::prelude::$native) -> Self {
                match value {
                    $(iggy::prelude::$native::$variant => Self::$variant),+
                }
            }
        }

        impl From<$name> for iggy::prelude::$native {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => Self::$variant),+
                }
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                Display::fmt(&iggy::prelude::$native::from(*self), formatter)
            }
        }

        impl FromStr for $name {
            type Err = LaserError;

            fn from_str(word: &str) -> Result<Self, Self::Err> {
                word.parse::<iggy::prelude::$native>()
                    .map(Self::from)
                    .map_err(|_| {
                        LaserError::Invalid(format!(
                            concat!("unknown ", stringify!($name), " `{}`"),
                            word
                        ))
                    })
            }
        }
    };
}

/// The resource an option catalog describes: `topic`, `stream`, or `user`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OptionsScope {
    Topic,
    Stream,
    User,
}

mirror_words!(OptionsScope, OptionsScope, [Topic, Stream, User]);

/// One option the server accepts for a scope. `kind` names the value type,
/// such as `uint64`, `bool`, or `string`. `default_value` is the default in
/// its text form, `None` when the option has no default. Integers are
/// decimal, floats are the `0x` hex of their bits, and raw bytes are base64.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionInfo {
    pub key: String,
    pub kind: String,
    /// An update may set the option.
    pub updatable: bool,
    /// An update may reset the option to its default.
    pub resettable: bool,
    /// The server accepts the option and never returns it.
    pub secret: bool,
    /// The server computes the option and never accepts it.
    pub read_only: bool,
    /// A node that does not know the option must not serve the resource.
    pub critical: bool,
    pub default_value: Option<String>,
    pub description: String,
}

impl From<iggy::prelude::OptionSpec> for OptionInfo {
    fn from(spec: iggy::prelude::OptionSpec) -> Self {
        let default_value = (!spec.default_value.is_empty())
            .then(|| iggy::prelude::HeaderValue::from_raw(spec.kind, &spec.default_value).ok())
            .flatten()
            .and_then(|value| iggy_common::resource_options_json::encode_value(&value).ok());
        Self {
            key: spec.key,
            kind: spec.kind.to_string(),
            updatable: spec.updatable,
            resettable: spec.resettable,
            secret: spec.secret,
            read_only: spec.read_only,
            critical: spec.critical,
            default_value,
            description: spec.description,
        }
    }
}

/// A family of metadata change records, and the unit a query filters on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataChangeGroup {
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions,
}

mirror_words!(
    MetadataChangeGroup,
    MetadataChangeGroup,
    [
        Streams,
        Topics,
        Partitions,
        ConsumerGroups,
        ConsumerGroupMembers,
        Users,
        PersonalAccessTokens,
        Sessions
    ]
);

/// What one metadata change record says happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataChangeKind {
    StreamCreated,
    StreamUpdated,
    StreamDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    PartitionsChanged,
    ConsumerGroupCreated,
    ConsumerGroupDeleted,
    ConsumerGroupMemberJoined,
    ConsumerGroupMemberLeft,
    ConsumerGroupPartitionHandedOver,
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserPasswordChanged,
    UserPermissionsUpdated,
    PersonalAccessTokenCreated,
    PersonalAccessTokenDeleted,
    SessionRegistered,
    SessionClosed,
}

mirror_words!(
    MetadataChangeKind,
    MetadataChangeKind,
    [
        StreamCreated,
        StreamUpdated,
        StreamDeleted,
        TopicCreated,
        TopicUpdated,
        TopicDeleted,
        PartitionsChanged,
        ConsumerGroupCreated,
        ConsumerGroupDeleted,
        ConsumerGroupMemberJoined,
        ConsumerGroupMemberLeft,
        ConsumerGroupPartitionHandedOver,
        UserCreated,
        UserUpdated,
        UserDeleted,
        UserPasswordChanged,
        UserPermissionsUpdated,
        PersonalAccessTokenCreated,
        PersonalAccessTokenDeleted,
        SessionRegistered,
        SessionClosed
    ]
);

/// The outcome of a metadata change query. `Ok` returns the records or
/// items after the start. `Behind` means the answering node has not
/// applied the start yet, so ask again. `Expired` means the node no longer
/// holds every record after the start, and `Reset` means the start
/// belongs to another history. After `Expired` or `Reset`, read the
/// current state again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataChangeStatus {
    Ok,
    Behind,
    Expired,
    Reset,
}

mirror_words!(
    MetadataChangeStatus,
    MetadataChangeStatus,
    [Ok, Behind, Expired, Reset]
);

/// Whether a page holds change records or current state items.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataChangePhase {
    Changes,
    Current,
}

mirror_words!(MetadataChangePhase, MetadataChangePhase, [Changes, Current]);

/// Why a member left, a token was deleted, a session closed, or a user's
/// permissions changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataChangeReason {
    Left,
    SessionEnded,
    Deleted,
    Expired,
    Logout,
    Revoked,
    Updated,
    ResourceDeleted,
}

impl Display for MetadataChangeReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Left => "left",
            Self::SessionEnded => "session_ended",
            Self::Deleted => "deleted",
            Self::Expired => "expired",
            Self::Logout => "logout",
            Self::Revoked => "revoked",
            Self::Updated => "updated",
            Self::ResourceDeleted => "resource_deleted",
        })
    }
}

impl From<iggy::prelude::MetadataMemberLeftReason> for MetadataChangeReason {
    fn from(reason: iggy::prelude::MetadataMemberLeftReason) -> Self {
        match reason {
            iggy::prelude::MetadataMemberLeftReason::Left => Self::Left,
            iggy::prelude::MetadataMemberLeftReason::SessionEnded => Self::SessionEnded,
        }
    }
}

impl From<iggy::prelude::MetadataTokenDeletedReason> for MetadataChangeReason {
    fn from(reason: iggy::prelude::MetadataTokenDeletedReason) -> Self {
        match reason {
            iggy::prelude::MetadataTokenDeletedReason::Deleted => Self::Deleted,
            iggy::prelude::MetadataTokenDeletedReason::Expired => Self::Expired,
        }
    }
}

impl From<iggy::prelude::MetadataSessionClosedReason> for MetadataChangeReason {
    fn from(reason: iggy::prelude::MetadataSessionClosedReason) -> Self {
        match reason {
            iggy::prelude::MetadataSessionClosedReason::Logout => Self::Logout,
            iggy::prelude::MetadataSessionClosedReason::Expired => Self::Expired,
            iggy::prelude::MetadataSessionClosedReason::Revoked => Self::Revoked,
        }
    }
}

impl From<iggy::prelude::MetadataPermissionsUpdatedReason> for MetadataChangeReason {
    fn from(reason: iggy::prelude::MetadataPermissionsUpdatedReason) -> Self {
        match reason {
            iggy::prelude::MetadataPermissionsUpdatedReason::Updated => Self::Updated,
            iggy::prelude::MetadataPermissionsUpdatedReason::ResourceDeleted => {
                Self::ResourceDeleted
            }
        }
    }
}

/// Where a metadata change query starts. `After` takes the `next` cursor of
/// an earlier page. `Current` pages through the current state, starting a
/// scan with `None` and continuing it with the `page_token` of the page
/// before.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MetadataChangeStart {
    /// The oldest record the requested groups still hold completely.
    #[default]
    Earliest,
    /// No records, only a cursor at the newest record.
    Latest,
    After(String),
    Current(Option<String>),
}

/// One request for a page of metadata changes or of the current state.
/// `groups` empty means every group. `since_micros` and `until_micros` keep
/// records stamped inside the inclusive range and apply only to the
/// `Earliest` and `After` starts. `limit` is 1 to 1000 records or items. A
/// current scan of a group must also name its parent groups, such as
/// `Streams` with `Topics`. A query that breaks these rules fails with
/// [`LaserError::Invalid`] before anything is sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataChangesQuery {
    pub start: MetadataChangeStart,
    pub groups: Vec<MetadataChangeGroup>,
    pub since_micros: Option<u64>,
    pub until_micros: Option<u64>,
    pub limit: u32,
}

impl Default for MetadataChangesQuery {
    fn default() -> Self {
        Self {
            start: MetadataChangeStart::Earliest,
            groups: Vec::new(),
            since_micros: None,
            until_micros: None,
            limit: iggy_common::DEFAULT_METADATA_CHANGES_LIMIT,
        }
    }
}

const INVALID_METADATA_QUERY: &str = "a metadata change query needs a limit of 1 to 1000, since no later than until, no time bound with the latest or current start, and every parent group in a current scan";

impl TryFrom<MetadataChangesQuery> for iggy::prelude::MetadataChangesQuery {
    type Error = LaserError;

    fn try_from(query: MetadataChangesQuery) -> Result<Self, Self::Error> {
        let start = match query.start {
            MetadataChangeStart::Earliest => iggy::prelude::MetadataChangeStart::Earliest,
            MetadataChangeStart::Latest => iggy::prelude::MetadataChangeStart::Latest,
            MetadataChangeStart::After(cursor) => {
                iggy::prelude::MetadataChangeStart::After(cursor.parse().map_err(|_| {
                    LaserError::Invalid(format!("malformed metadata change cursor `{cursor}`"))
                })?)
            }
            MetadataChangeStart::Current(token) => iggy::prelude::MetadataChangeStart::Current(
                token
                    .map(|token| {
                        token.parse().map_err(|_| {
                            LaserError::Invalid(format!("malformed metadata page token `{token}`"))
                        })
                    })
                    .transpose()?,
            ),
        };
        let query = Self {
            start,
            groups: query.groups.into_iter().map(Into::into).collect(),
            since: query.since_micros.map(iggy::prelude::IggyTimestamp::from),
            until: query.until_micros.map(iggy::prelude::IggyTimestamp::from),
            limit: query.limit,
        };
        query
            .validate()
            .map_err(|_| LaserError::Invalid(INVALID_METADATA_QUERY.to_owned()))?;
        Ok(query)
    }
}

/// What one change record or current state item is about. Each kind fills
/// the fields that name its resource and what changed, and leaves the rest
/// `None` or empty. Ids are native ids. A `*_created_op` or
/// `user_created_at_micros` tells a recreated resource apart from an older
/// one with the same id. Resource options are not carried.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataSubject {
    pub stream_id: Option<u32>,
    pub stream_created_op: Option<u64>,
    pub topic_id: Option<u32>,
    pub topic_created_op: Option<u64>,
    pub group_id: Option<u32>,
    pub member_id: Option<u32>,
    pub partition_id: Option<u32>,
    pub user_id: Option<u32>,
    pub user_created_at_micros: Option<u64>,
    pub client_id: Option<u128>,
    pub session: Option<u64>,
    /// The stream, topic, consumer group, or personal access token name.
    pub name: Option<String>,
    pub username: Option<String>,
    pub created_at_micros: Option<u64>,
    pub partitions_count: Option<u32>,
    pub partition_ids: Vec<u32>,
    pub removed_partition_ids: Vec<u32>,
    pub consumer_groups_count: Option<u32>,
    pub members_count: Option<u32>,
    /// The op of the request a deferred delete answers.
    pub requested_op: Option<u64>,
    pub status: Option<UserStatus>,
    pub permissions: Option<Permissions>,
    pub expires_at_micros: Option<u64>,
    pub reason: Option<MetadataChangeReason>,
}

impl MetadataSubject {
    fn stream(stream_id: u32, stream_created_op: u64) -> Self {
        Self {
            stream_id: Some(stream_id),
            stream_created_op: Some(stream_created_op),
            ..Self::default()
        }
    }

    fn topic(stream_id: u32, stream_created_op: u64, topic_id: u32, topic_created_op: u64) -> Self {
        Self {
            topic_id: Some(topic_id),
            topic_created_op: Some(topic_created_op),
            ..Self::stream(stream_id, stream_created_op)
        }
    }

    fn user(user_id: u32, user_created_at: u64) -> Self {
        Self {
            user_id: Some(user_id),
            user_created_at_micros: Some(user_created_at),
            ..Self::default()
        }
    }
}

impl From<iggy::prelude::MetadataStreamChange> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataStreamChange) -> Self {
        Self {
            created_at_micros: Some(change.created_at),
            name: Some(change.name),
            ..Self::stream(change.stream_id, change.stream_created_op)
        }
    }
}

impl From<iggy::prelude::MetadataStreamDeleted> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataStreamDeleted) -> Self {
        Self {
            created_at_micros: Some(change.created_at),
            name: Some(change.name),
            requested_op: change.requested_op,
            ..Self::stream(change.stream_id, change.stream_created_op)
        }
    }
}

impl From<iggy::prelude::MetadataTopicChange> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataTopicChange) -> Self {
        Self {
            created_at_micros: Some(change.created_at),
            name: Some(change.name),
            partitions_count: Some(change.partitions_count),
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataTopicItem> for MetadataSubject {
    fn from(item: iggy::prelude::MetadataTopicItem) -> Self {
        Self {
            created_at_micros: Some(item.created_at),
            name: Some(item.name),
            ..Self::topic(
                item.stream_id,
                item.stream_created_op,
                item.topic_id,
                item.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataTopicDeleted> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataTopicDeleted) -> Self {
        Self {
            created_at_micros: Some(change.created_at),
            name: Some(change.name),
            requested_op: change.requested_op,
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataPartitionsChanged> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataPartitionsChanged) -> Self {
        Self {
            partitions_count: Some(change.partitions_count),
            removed_partition_ids: change.removed_partition_ids,
            requested_op: change.requested_op,
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataPartitionsItem> for MetadataSubject {
    fn from(item: iggy::prelude::MetadataPartitionsItem) -> Self {
        Self {
            partitions_count: Some(item.partitions_count),
            ..Self::topic(
                item.stream_id,
                item.stream_created_op,
                item.topic_id,
                item.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataConsumerGroupCreated> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataConsumerGroupCreated) -> Self {
        Self {
            group_id: Some(change.group_id),
            name: Some(change.name),
            consumer_groups_count: Some(change.consumer_groups_count),
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataConsumerGroupDeleted> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataConsumerGroupDeleted) -> Self {
        Self {
            group_id: Some(change.group_id),
            name: Some(change.name),
            consumer_groups_count: Some(change.consumer_groups_count),
            requested_op: change.requested_op,
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataConsumerGroupItem> for MetadataSubject {
    fn from(item: iggy::prelude::MetadataConsumerGroupItem) -> Self {
        Self {
            group_id: Some(item.group_id),
            name: Some(item.name),
            ..Self::topic(
                item.stream_id,
                item.stream_created_op,
                item.topic_id,
                item.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataMemberJoined> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataMemberJoined) -> Self {
        Self {
            group_id: Some(change.group_id),
            member_id: Some(change.member_id),
            client_id: Some(change.client_id),
            members_count: Some(change.members_count),
            partition_ids: change.partition_ids,
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataMemberLeft> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataMemberLeft) -> Self {
        Self {
            group_id: Some(change.group_id),
            member_id: Some(change.member_id),
            client_id: Some(change.client_id),
            members_count: Some(change.members_count),
            reason: Some(change.reason.into()),
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataPartitionHandedOver> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataPartitionHandedOver) -> Self {
        Self {
            group_id: Some(change.group_id),
            partition_id: Some(change.partition_id),
            member_id: change.member_id,
            ..Self::topic(
                change.stream_id,
                change.stream_created_op,
                change.topic_id,
                change.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataMemberItem> for MetadataSubject {
    fn from(item: iggy::prelude::MetadataMemberItem) -> Self {
        Self {
            group_id: Some(item.group_id),
            member_id: Some(item.member_id),
            client_id: Some(item.client_id),
            partition_ids: item.partition_ids,
            ..Self::topic(
                item.stream_id,
                item.stream_created_op,
                item.topic_id,
                item.topic_created_op,
            )
        }
    }
}

impl From<iggy::prelude::MetadataUserChange> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataUserChange) -> Self {
        Self {
            username: Some(change.username),
            status: Some(change.status.into()),
            permissions: change.permissions.map(Permissions::from),
            ..Self::user(change.user_id, change.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataUserIdentity> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataUserIdentity) -> Self {
        Self {
            username: Some(change.username),
            ..Self::user(change.user_id, change.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataUserPermissionsUpdated> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataUserPermissionsUpdated) -> Self {
        Self {
            username: Some(change.username),
            permissions: change.permissions.map(Permissions::from),
            reason: Some(change.reason.into()),
            ..Self::user(change.user_id, change.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataTokenChange> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataTokenChange) -> Self {
        Self {
            name: Some(change.name),
            expires_at_micros: change.expiry,
            ..Self::user(change.user_id, change.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataTokenDeleted> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataTokenDeleted) -> Self {
        Self {
            name: Some(change.name),
            reason: Some(change.reason.into()),
            ..Self::user(change.user_id, change.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataSession> for MetadataSubject {
    fn from(session: iggy::prelude::MetadataSession) -> Self {
        Self {
            client_id: Some(session.client_id),
            session: Some(session.session),
            ..Self::user(session.user_id, session.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataSessionClosed> for MetadataSubject {
    fn from(session: iggy::prelude::MetadataSessionClosed) -> Self {
        Self {
            client_id: Some(session.client_id),
            session: Some(session.session),
            reason: Some(session.reason.into()),
            ..Self::user(session.user_id, session.user_created_at)
        }
    }
}

impl From<iggy::prelude::MetadataChange> for MetadataSubject {
    fn from(change: iggy::prelude::MetadataChange) -> Self {
        use iggy::prelude::MetadataChange as Change;
        match change {
            Change::StreamCreated(change) | Change::StreamUpdated(change) => change.into(),
            Change::StreamDeleted(change) => change.into(),
            Change::TopicCreated(change) | Change::TopicUpdated(change) => change.into(),
            Change::TopicDeleted(change) => change.into(),
            Change::PartitionsChanged(change) => change.into(),
            Change::ConsumerGroupCreated(change) => change.into(),
            Change::ConsumerGroupDeleted(change) => change.into(),
            Change::ConsumerGroupMemberJoined(change) => change.into(),
            Change::ConsumerGroupMemberLeft(change) => change.into(),
            Change::ConsumerGroupPartitionHandedOver(change) => change.into(),
            Change::UserCreated(change) | Change::UserUpdated(change) => change.into(),
            Change::UserDeleted(change) | Change::UserPasswordChanged(change) => change.into(),
            Change::UserPermissionsUpdated(change) => change.into(),
            Change::PersonalAccessTokenCreated(change) => change.into(),
            Change::PersonalAccessTokenDeleted(change) => change.into(),
            Change::SessionRegistered(session) => session.into(),
            Change::SessionClosed(session) => session.into(),
            Change::Unknown { .. } | Change::UnknownJson { .. } => Self::default(),
        }
    }
}

impl From<iggy::prelude::MetadataCurrentItem> for MetadataSubject {
    fn from(item: iggy::prelude::MetadataCurrentItem) -> Self {
        use iggy::prelude::MetadataCurrentItem as Item;
        match item {
            Item::Stream(item) => item.into(),
            Item::Topic(item) => item.into(),
            Item::Partitions(item) => item.into(),
            Item::ConsumerGroup(item) => item.into(),
            Item::ConsumerGroupMember(item) => item.into(),
            Item::User(item) => item.into(),
            Item::PersonalAccessToken(item) => item.into(),
            Item::Session(item) => item.into(),
            Item::Unknown { .. } | Item::UnknownJson { .. } => Self::default(),
        }
    }
}

/// One committed metadata change. `kind` is `None` for a kind this client
/// does not know, and its subject is then empty. `actor_user_id` is `None`
/// for a change the server made itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataChangeRecord {
    pub op: u64,
    /// The position of this record among the records of one op.
    pub ordinal: u32,
    pub timestamp_micros: u64,
    pub actor_user_id: Option<u32>,
    pub kind: Option<MetadataChangeKind>,
    pub subject: MetadataSubject,
}

impl From<iggy::prelude::MetadataChangeRecord> for MetadataChangeRecord {
    fn from(record: iggy::prelude::MetadataChangeRecord) -> Self {
        Self {
            op: record.op,
            ordinal: record.ordinal,
            timestamp_micros: record.timestamp,
            actor_user_id: record.actor_user_id,
            kind: record.change.kind().map(Into::into),
            subject: record.change.into(),
        }
    }
}

/// One resource of the current state. `group` is `None` for a group this
/// client does not know, and its subject is then empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataItem {
    pub group: Option<MetadataChangeGroup>,
    pub subject: MetadataSubject,
}

impl From<iggy::prelude::MetadataCurrentItem> for MetadataItem {
    fn from(item: iggy::prelude::MetadataCurrentItem) -> Self {
        use iggy::prelude::{MetadataChangeGroup as Group, MetadataCurrentItem as Item};
        let group = match &item {
            Item::Stream(_) => Some(Group::Streams),
            Item::Topic(_) => Some(Group::Topics),
            Item::Partitions(_) => Some(Group::Partitions),
            Item::ConsumerGroup(_) => Some(Group::ConsumerGroups),
            Item::ConsumerGroupMember(_) => Some(Group::ConsumerGroupMembers),
            Item::User(_) => Some(Group::Users),
            Item::PersonalAccessToken(_) => Some(Group::PersonalAccessTokens),
            Item::Session(_) => Some(Group::Sessions),
            Item::Unknown { .. } | Item::UnknownJson { .. } => None,
        };
        Self {
            group: group.map(Into::into),
            subject: item.into(),
        }
    }
}

/// One page of metadata changes, or of the current state. Send `next` back
/// as [`MetadataChangeStart::After`] to read on, and `page_token` as
/// [`MetadataChangeStart::Current`] to continue a current state scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataChanges {
    pub status: MetadataChangeStatus,
    pub phase: MetadataChangePhase,
    /// The newest op the answering node applied.
    pub head_op: u64,
    /// The oldest op from which the requested groups are complete.
    pub oldest_op: u64,
    /// The requested groups are complete after this time, 0 when the node
    /// dropped nothing.
    pub retained_since_micros: u64,
    /// The cursor after this page, `None` on `Expired` and `Reset`.
    pub next: Option<String>,
    /// More records follow `next`, or another current state page exists.
    pub has_more: bool,
    pub records: Vec<MetadataChangeRecord>,
    pub items: Vec<MetadataItem>,
    /// The token of the next current state page, `None` once the scan ends.
    pub page_token: Option<String>,
}

impl From<iggy::prelude::MetadataChanges> for MetadataChanges {
    fn from(page: iggy::prelude::MetadataChanges) -> Self {
        Self {
            status: page.status.into(),
            phase: page.phase.into(),
            head_op: page.head_op,
            oldest_op: page.oldest_op,
            retained_since_micros: page.retained_since,
            next: page.next.map(|cursor| cursor.to_string()),
            has_more: page.has_more,
            records: page
                .records
                .into_iter()
                .map(MetadataChangeRecord::from)
                .collect(),
            items: page.items.into_iter().map(MetadataItem::from).collect(),
            page_token: page.current_page_token.map(|token| token.to_string()),
        }
    }
}

impl Laser {
    /// Every option the server accepts for `scope`, with its type, rules,
    /// default, and description.
    pub async fn describe_options(
        &self,
        scope: OptionsScope,
    ) -> Result<Vec<OptionInfo>, LaserError> {
        let options = self.client().describe_options(scope.into()).await?;
        Ok(options.into_iter().map(OptionInfo::from).collect())
    }

    /// One page of the server's metadata changes, or of its current state.
    /// Needs the manage servers permission.
    pub async fn metadata_changes(
        &self,
        query: MetadataChangesQuery,
    ) -> Result<MetadataChanges, LaserError> {
        let query = query.try_into()?;
        Ok(self.client().get_metadata_changes(&query).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_user_status_words_when_parsed_then_should_round_trip_through_display() {
        for status in [UserStatus::Active, UserStatus::Inactive] {
            let parsed: UserStatus = status.to_string().parse().expect("status word parses");
            assert_eq!(parsed, status);
        }
        assert_eq!(
            "paused".parse::<UserStatus>(),
            Err(UnknownUserStatus("paused".to_owned()))
        );
    }

    #[test]
    fn given_stream_and_topic_grants_when_converted_to_iggy_and_back_then_should_keep_every_grant()
    {
        let permissions = Permissions {
            global: GlobalPermissions {
                read_streams: true,
                poll_messages: true,
                ..GlobalPermissions::default()
            },
            streams: BTreeMap::from([(
                7,
                StreamPermissions {
                    send_messages: true,
                    topics: BTreeMap::from([(
                        3,
                        TopicPermissions {
                            read_topic: true,
                            ..TopicPermissions::default()
                        },
                    )]),
                    ..StreamPermissions::default()
                },
            )]),
        };
        let native: iggy::prelude::Permissions = permissions.clone().into();
        assert_eq!(Permissions::from(native), permissions);
    }

    #[test]
    fn given_no_stream_grants_when_converted_to_iggy_then_should_send_no_stream_map() {
        let native: iggy::prelude::Permissions = Permissions::default().into();
        assert!(
            native.streams.is_none(),
            "an empty grant map is sent as none"
        );
    }

    #[test]
    fn given_a_permissions_dict_when_deserialized_then_should_default_missing_grants() {
        let permissions: Permissions = serde_json::from_str(
            r#"{"global":{"read_users":true},"streams":{"2":{"read_stream":true}}}"#,
        )
        .expect("permissions decode");
        assert!(permissions.global.read_users);
        assert!(!permissions.global.manage_users);
        assert!(permissions.streams[&2].read_stream);
    }

    #[test]
    fn given_metadata_words_when_parsed_then_should_round_trip_through_display() {
        let group: MetadataChangeGroup =
            "consumer_group_members".parse().expect("group word parses");
        assert_eq!(group, MetadataChangeGroup::ConsumerGroupMembers);
        assert_eq!(group.to_string(), "consumer_group_members");
        let scope: OptionsScope = "topic".parse().expect("scope word parses");
        assert_eq!(scope.to_string(), "topic");
        assert_eq!(
            MetadataChangeKind::ConsumerGroupPartitionHandedOver.to_string(),
            "consumer_group_partition_handed_over"
        );
        assert!(matches!(
            "nodes".parse::<MetadataChangeGroup>(),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_an_option_spec_when_converted_then_should_render_its_default_as_text() {
        let spec = |kind, default_value: Vec<u8>| iggy::prelude::OptionSpec {
            key: "segment_size".to_owned(),
            kind,
            updatable: true,
            resettable: true,
            secret: false,
            read_only: false,
            critical: false,
            default_value,
            description: "segment size".to_owned(),
        };
        let option = OptionInfo::from(spec(
            iggy::prelude::HeaderKind::Uint64,
            1_073_741_824u64.to_le_bytes().to_vec(),
        ));
        assert_eq!(option.kind, "uint64");
        assert_eq!(option.default_value.as_deref(), Some("1073741824"));
        assert!(option.updatable);
        let option = OptionInfo::from(spec(iggy::prelude::HeaderKind::Bool, Vec::new()));
        assert_eq!(option.default_value, None);
    }

    #[test]
    fn given_a_member_left_record_when_converted_then_should_name_the_member_and_reason() {
        let record = iggy::prelude::MetadataChangeRecord {
            op: 9,
            ordinal: 1,
            checksum: 0,
            timestamp: 1_700_000_000_000_000,
            actor_user_id: None,
            change: iggy::prelude::MetadataChange::ConsumerGroupMemberLeft(
                iggy::prelude::MetadataMemberLeft {
                    stream_id: 1,
                    stream_created_op: 2,
                    topic_id: 3,
                    topic_created_op: 4,
                    group_id: 5,
                    member_id: 6,
                    client_id: 7,
                    members_count: 0,
                    reason: iggy::prelude::MetadataMemberLeftReason::SessionEnded,
                },
            ),
        };
        let record = MetadataChangeRecord::from(record);
        assert_eq!(
            record.kind,
            Some(MetadataChangeKind::ConsumerGroupMemberLeft)
        );
        assert_eq!(record.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(record.subject.stream_id, Some(1));
        assert_eq!(record.subject.topic_id, Some(3));
        assert_eq!(record.subject.group_id, Some(5));
        assert_eq!(record.subject.member_id, Some(6));
        assert_eq!(record.subject.client_id, Some(7));
        assert_eq!(record.subject.members_count, Some(0));
        assert_eq!(
            record.subject.reason,
            Some(MetadataChangeReason::SessionEnded)
        );
        assert_eq!(record.subject.name, None);
    }

    #[test]
    fn given_a_malformed_cursor_when_the_query_is_converted_then_should_refuse_it() {
        let query = MetadataChangesQuery {
            start: MetadataChangeStart::After("mc1_nope".to_owned()),
            ..MetadataChangesQuery::default()
        };
        assert!(matches!(
            iggy::prelude::MetadataChangesQuery::try_from(query),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_query_breaking_the_server_rules_when_converted_then_should_refuse_it() {
        for query in [
            MetadataChangesQuery {
                limit: 0,
                ..MetadataChangesQuery::default()
            },
            MetadataChangesQuery {
                start: MetadataChangeStart::Latest,
                since_micros: Some(1),
                ..MetadataChangesQuery::default()
            },
            MetadataChangesQuery {
                start: MetadataChangeStart::Current(None),
                groups: vec![MetadataChangeGroup::Topics],
                ..MetadataChangesQuery::default()
            },
        ] {
            assert!(matches!(
                iggy::prelude::MetadataChangesQuery::try_from(query),
                Err(LaserError::Invalid(_))
            ));
        }
    }

    #[test]
    fn given_a_default_query_when_converted_then_should_start_at_the_earliest_record() {
        let query = iggy::prelude::MetadataChangesQuery::try_from(MetadataChangesQuery {
            groups: vec![MetadataChangeGroup::Streams],
            since_micros: Some(5),
            ..MetadataChangesQuery::default()
        })
        .expect("the query converts");
        assert_eq!(query.start, iggy::prelude::MetadataChangeStart::Earliest);
        assert_eq!(
            query.groups,
            vec![iggy::prelude::MetadataChangeGroup::Streams]
        );
        assert_eq!(query.since.map(|since| since.as_micros()), Some(5));
        assert_eq!(query.limit, 100);
    }
}
