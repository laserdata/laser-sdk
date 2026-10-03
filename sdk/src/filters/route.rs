use crate::error::LaserError;
use crate::iggy::prelude::{
    AutoLogin, Client, ClientWrapper, ClusterClient, Credentials, IggyClient,
    PersonalAccessTokenClient, QuicClient, QuicClientConfig, TcpClient, TcpClientConfig,
    UserClient, WebSocketClient, WebSocketClientConfig,
};
use crate::laser::{endpoint_of, host_of, with_endpoint};
use iggy_binary_protocol::codes::{
    ATTACH_CONSUMER_SESSION_CODE, GET_CONSUMER_OFFSET_ROUTING_CODE, GET_POLL_ROUTING_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::GetConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::system::AttachConsumerSessionRequest;
use iggy_binary_protocol::responses::messages::PollRoutingResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::wire_conversions::{
    consumer_to_wire, identifier_to_wire, polling_strategy_to_wire,
};
use iggy_common::{
    ClusterNode, ConnectionString, ConnectionStringUtils, Consumer, Identifier, PollingStrategy,
    QuicConnectionStringOptions, TcpConnectionStringOptions, TransportProtocol,
    WebSocketConnectionStringOptions,
};
use laser_wire::filter::{FilterConsumer, FilterSource};
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PRIMARY_CONNECTIONS: usize = 16;

/// Data connections to partition primaries, one per node endpoint, each
/// attached to the coordinator's consumer session. The coordinator keeps the
/// session, the user, and any group membership. A data connection only carries
/// reads and acknowledgments on its behalf.
#[derive(Default)]
pub(crate) struct Routes {
    connections: HashMap<String, PrimaryConnection>,
    partitions: HashMap<u32, String>,
    opened: u64,
    // Whether the deployment is one node, learned on the first route. A single
    // node is every partition's primary, and the address it advertises can be
    // one the caller cannot reach (a port mapped by a container runtime, a
    // slot behind a proxy), so its data connection dials what the caller
    // dialed.
    single_node: Option<bool>,
}

struct PrimaryConnection {
    client: Arc<IggyClient>,
    session: AttachConsumerSessionRequest,
    opened: u64,
}

/// What a fresh route means for the data connection already open to its node.
///
/// A connection stays open for the life of its session. The coordinator's
/// metadata watermark grows with every write it commits, and that alone never
/// justifies a new connection: the attachment is sent again on the open one,
/// which only raises its read-your-writes floor.
#[derive(Debug, PartialEq, Eq)]
enum Reuse {
    /// The same session at the same or a lower floor.
    Keep,
    /// The same session at a higher floor.
    Raise,
    /// Another session, after the coordinator rejoined or reconnected.
    Replace,
}

impl Reuse {
    fn of(open: &AttachConsumerSessionRequest, routed: &AttachConsumerSessionRequest) -> Self {
        if open.client_id != routed.client_id || open.session != routed.session {
            Reuse::Replace
        } else if routed.metadata_watermark > open.metadata_watermark {
            Reuse::Raise
        } else {
            Reuse::Keep
        }
    }
}

impl Routes {
    /// The attached data connection serving `partition_id`, routed through the
    /// `coordinator` on first use and after [`invalidate`](Self::invalidate).
    pub(crate) async fn connection(
        &mut self,
        coordinator: &IggyClient,
        connection_string: Option<&str>,
        route_to: &RouteTo<'_>,
        partition_id: u32,
        acknowledgment: bool,
    ) -> Result<Arc<IggyClient>, LaserError> {
        if let Some(connection) = self
            .partitions
            .get(&partition_id)
            .and_then(|endpoint| self.connections.get(endpoint))
        {
            return Ok(Arc::clone(&connection.client));
        }
        let connection_string = connection_string.ok_or(LaserError::Config(
            "primary filtered reads open data connections, so they need a Laser built from a connection string. Read in ReadMode::Local for a bring-your-own client",
        ))?;
        let route = resolve(coordinator, route_to, partition_id, acknowledgment).await?;
        let endpoint = self
            .endpoint(coordinator, connection_string, &route)
            .await?;
        let session = route.consumer_session;
        let reuse = self
            .connections
            .get(&endpoint)
            .map(|connection| Reuse::of(&connection.session, &session));
        match reuse {
            Some(Reuse::Keep) | None => {}
            Some(Reuse::Raise) => {
                let connection = &self.connections[&endpoint];
                let raised = connection
                    .client
                    .send_binary_request(ATTACH_CONSUMER_SESSION_CODE, session.to_bytes())
                    .await;
                match raised {
                    Ok(_) => {
                        if let Some(connection) = self.connections.get_mut(&endpoint) {
                            connection.session = session;
                        }
                    }
                    Err(_) => self.drop_endpoint(&endpoint).await,
                }
            }
            Some(Reuse::Replace) => self.drop_endpoint(&endpoint).await,
        }
        if !self.connections.contains_key(&endpoint) {
            if self.connections.len() >= MAX_PRIMARY_CONNECTIONS {
                let idle = self.connections.iter().filter(|(endpoint, _)| {
                    !self.partitions.values().any(|routed| routed == *endpoint)
                });
                let oldest = idle
                    .min_by_key(|(_, connection)| connection.opened)
                    .or_else(|| {
                        self.connections
                            .iter()
                            .min_by_key(|(_, connection)| connection.opened)
                    })
                    .map(|(endpoint, _)| endpoint.clone());
                if let Some(oldest) = oldest {
                    self.drop_endpoint(&oldest).await;
                }
            }
            let client = attach(connection_string, &endpoint, session).await?;
            self.opened += 1;
            self.connections.insert(
                endpoint.clone(),
                PrimaryConnection {
                    client: Arc::new(client),
                    session,
                    opened: self.opened,
                },
            );
        }
        self.partitions.insert(partition_id, endpoint.clone());
        Ok(Arc::clone(&self.connections[&endpoint].client))
    }

    async fn endpoint(
        &mut self,
        coordinator: &(impl ClusterClient + Sync),
        connection_string: &str,
        route: &PollRoutingResponse,
    ) -> Result<String, LaserError> {
        let single_node = match self.single_node {
            Some(single_node) => single_node,
            None => {
                let single_node = coordinator.get_cluster_metadata().await?.nodes.len() <= 1;
                self.single_node = Some(single_node);
                single_node
            }
        };
        if single_node {
            Ok(endpoint_of(connection_string).to_owned())
        } else {
            endpoint(connection_string, route)
        }
    }

    /// Data connections these routes opened, which stays at one per node while every session lives.
    pub(crate) fn opened(&self) -> u64 {
        self.opened
    }

    /// Stop routing `partition_id`, for a partition this reader no longer
    /// reads. The node connection stays open for the other partitions it
    /// serves and closes only when none is left.
    pub(crate) async fn release(&mut self, partition_id: u32) {
        if let Some(client) = self.forget(partition_id) {
            let _ = client.shutdown().await;
        }
    }

    pub(crate) fn forget(&mut self, partition_id: u32) -> Option<Arc<IggyClient>> {
        let endpoint = self.partitions.remove(&partition_id)?;
        if self.partitions.values().any(|routed| *routed == endpoint) {
            return None;
        }
        self.connections
            .remove(&endpoint)
            .map(|connection| connection.client)
    }

    /// Route `partition_id` again on its next use and drop the connection it
    /// used, because that node no longer serves it as primary or the session
    /// behind the attachment has ended.
    pub(crate) async fn invalidate(&mut self, partition_id: u32) {
        if let Some(endpoint) = self.partitions.remove(&partition_id) {
            self.drop_endpoint(&endpoint).await;
        }
    }

    /// Close every data connection. Takes the routes by value, so a caller
    /// that is cancelled mid-close has already forgotten them.
    pub(crate) async fn close(self) {
        for (_, connection) in self.connections {
            let _ = connection.client.shutdown().await;
        }
    }

    // The routing table forgets the endpoint before the shutdown awaits, so a
    // cancelled caller never keeps a half-closed connection routed.
    async fn drop_endpoint(&mut self, endpoint: &str) {
        self.partitions.retain(|_, routed| routed != endpoint);
        if let Some(connection) = self.connections.remove(endpoint) {
            let _ = connection.client.shutdown().await;
        }
    }
}

/// The consumer and source a partition route is resolved for.
pub(crate) struct RouteTo<'a> {
    pub(crate) source: &'a FilterSource,
    pub(crate) consumer: &'a FilterConsumer,
}

// Ask the coordinator which node is the partition primary and which consumer
// session a data connection must attach. The coordinator answers only for a
// committing poll shape, so the request says so. It routes and reads nothing.
async fn resolve(
    coordinator: &IggyClient,
    route_to: &RouteTo<'_>,
    partition_id: u32,
    acknowledgment: bool,
) -> Result<PollRoutingResponse, LaserError> {
    let source = route_to.source;
    let consumer = match route_to.consumer {
        FilterConsumer::Consumer(name) => Consumer::new(Identifier::named(name)?),
        FilterConsumer::Group(name) => Consumer::group(Identifier::named(name)?),
        FilterConsumer::GroupId(id) => {
            Consumer::group(Identifier::numeric(u32::try_from(*id).map_err(|_| {
                LaserError::Config("consumer group id exceeds 32 bits")
            })?)?)
        }
    };
    let request = PollMessagesRequest {
        consumer: consumer_to_wire(&consumer)?,
        stream_id: identifier_to_wire(&Identifier::named(&source.stream)?)?,
        topic_id: identifier_to_wire(&Identifier::named(&source.topic)?)?,
        partition_id: Some(partition_id),
        strategy: polling_strategy_to_wire(&PollingStrategy::next()),
        count: 1,
        auto_commit: true,
    };
    let (code, payload) = if acknowledgment {
        (
            GET_CONSUMER_OFFSET_ROUTING_CODE,
            GetConsumerOffsetRequest {
                consumer: request.consumer,
                stream_id: request.stream_id,
                topic_id: request.topic_id,
                partition_id: request.partition_id,
            }
            .to_bytes(),
        )
    } else {
        (GET_POLL_ROUTING_CODE, request.to_bytes())
    };
    let reply = coordinator.send_binary_request(code, payload).await?;
    PollRoutingResponse::decode_from(&reply)
        .map_err(|error| LaserError::Protocol(format!("decode the partition route: {error}")))
}

// The primary's address on this connection's transport. A node that reports an
// unspecified address is reached through the host the caller connected to.
fn endpoint(connection_string: &str, route: &PollRoutingResponse) -> Result<String, LaserError> {
    let node = ClusterNode::try_from(route.primary.clone())?;
    let port = match ConnectionStringUtils::parse_protocol(connection_string)? {
        TransportProtocol::Tcp => node.endpoints.tcp,
        TransportProtocol::Quic => node.endpoints.quic,
        TransportProtocol::WebSocket => node.endpoints.websocket,
        TransportProtocol::Http => 0,
    };
    if port == 0 {
        return Err(LaserError::unsupported_feature(
            "filters",
            "primary_routing",
            format!(
                "partition primary {} serves no binary endpoint for this transport",
                node.name
            ),
        ));
    }
    let host = match node.ip.as_str() {
        "" | "0.0.0.0" | "::" | "[::]" => host_of(connection_string),
        ip => ip,
    };
    Ok(if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

// Dial the primary itself and stay there. An ordinary connect with
// credentials signs in and then moves to the metadata leader, so the data
// connection is built without them and signs in on the transport, which never
// redirects. It does not reconnect: a lost data connection is routed again.
async fn attach(
    connection_string: &str,
    endpoint: &str,
    session: AttachConsumerSessionRequest,
) -> Result<IggyClient, LaserError> {
    let address = with_endpoint(connection_string, endpoint);
    let (transport, credentials) = match ConnectionStringUtils::parse_protocol(&address)? {
        TransportProtocol::Tcp => {
            let parsed = ConnectionString::<TcpConnectionStringOptions>::from_str(&address)?;
            let credentials = parsed.auto_login().clone();
            let mut config = TcpClientConfig::from(parsed);
            config.auto_login = AutoLogin::Disabled;
            config.reconnection.enabled = false;
            (
                ClientWrapper::Tcp(TcpClient::create(Arc::new(config))?),
                credentials,
            )
        }
        TransportProtocol::Quic => {
            let parsed = ConnectionString::<QuicConnectionStringOptions>::from_str(&address)?;
            let credentials = parsed.auto_login().clone();
            let mut config = QuicClientConfig::from(parsed);
            config.auto_login = AutoLogin::Disabled;
            config.reconnection.enabled = false;
            (
                ClientWrapper::Quic(QuicClient::create(Arc::new(config))?),
                credentials,
            )
        }
        TransportProtocol::WebSocket => {
            let parsed = ConnectionString::<WebSocketConnectionStringOptions>::from_str(&address)?;
            let credentials = parsed.auto_login().clone();
            let mut config = WebSocketClientConfig::from(parsed);
            config.auto_login = AutoLogin::Disabled;
            config.reconnection.enabled = false;
            (
                ClientWrapper::WebSocket(WebSocketClient::create(Arc::new(config))?),
                credentials,
            )
        }
        TransportProtocol::Http => {
            return Err(LaserError::unsupported_feature(
                "filters",
                "primary_routing",
                "primary filtered reads need a binary transport",
            ));
        }
    };
    let client = IggyClient::new(transport);
    tokio::time::timeout(CONNECT_TIMEOUT, client.connect())
        .await
        .map_err(|_| LaserError::Timeout("the partition primary to accept a data connection"))??;
    {
        let transport = client.client();
        let transport = transport.read().await;
        match credentials {
            AutoLogin::Enabled(Credentials::UsernamePassword(username, password)) => {
                transport
                    .login_user(&username, password.expose_secret())
                    .await?;
            }
            AutoLogin::Enabled(Credentials::PersonalAccessToken(token)) => {
                transport
                    .login_with_personal_access_token(token.expose_secret())
                    .await?;
            }
            AutoLogin::Disabled => {}
        }
    }
    client
        .send_binary_request(ATTACH_CONSUMER_SESSION_CODE, session.to_bytes())
        .await?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::responses::system::get_cluster_metadata::ClusterNodeResponse;
    use iggy_common::{ClusterMetadata, IggyError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecoveringCluster {
        calls: AtomicUsize,
        metadata: ClusterMetadata,
    }

    #[async_trait::async_trait]
    impl ClusterClient for RecoveringCluster {
        async fn get_cluster_metadata(&self) -> Result<ClusterMetadata, IggyError> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(IggyError::TransientNotAccepted);
            }
            Ok(self.metadata.clone())
        }
    }

    #[tokio::test]
    async fn given_a_failed_topology_probe_when_routing_again_then_should_recover_and_cache_the_endpoint()
     {
        let route = PollRoutingResponse {
            consumer_session: session(7, 3, 10),
            primary: ClusterNodeResponse {
                name: "node-1".to_owned(),
                ip: "127.0.0.1".to_owned(),
                tcp_port: 8090,
                quic_port: 8091,
                http_port: 0,
                websocket_port: 3000,
                role: 1,
                status: 1,
            },
        };
        let connection_string = "iggy+tcp://iggy:iggy@127.0.0.1:18090";
        for (nodes, expected) in [(1, "127.0.0.1:18090"), (3, "127.0.0.1:8090")] {
            let coordinator = RecoveringCluster {
                calls: AtomicUsize::new(0),
                metadata: ClusterMetadata {
                    name: "cluster".to_owned(),
                    nodes: vec![
                        ClusterNode::try_from(route.primary.clone()).expect("valid node");
                        nodes
                    ],
                },
            };
            let mut routes = Routes::default();
            assert!(matches!(
                routes
                    .endpoint(&coordinator, connection_string, &route)
                    .await,
                Err(LaserError::Iggy(IggyError::TransientNotAccepted))
            ));
            assert_eq!(routes.single_node, None);
            for _ in 0..2 {
                assert_eq!(
                    routes
                        .endpoint(&coordinator, connection_string, &route)
                        .await
                        .expect("topology discovery recovers"),
                    expected
                );
            }
            assert_eq!(coordinator.calls.load(Ordering::Relaxed), 2);
        }
    }

    fn session(
        client_id: u128,
        session: u64,
        metadata_watermark: u64,
    ) -> AttachConsumerSessionRequest {
        AttachConsumerSessionRequest {
            client_id,
            session,
            metadata_watermark,
        }
    }

    #[test]
    fn given_the_same_session_at_a_higher_floor_when_routed_then_should_raise_it_on_the_open_connection()
     {
        assert_eq!(
            Reuse::of(&session(7, 3, 10), &session(7, 3, 12)),
            Reuse::Raise
        );
    }

    #[test]
    fn given_the_same_session_at_the_same_or_a_lower_floor_when_routed_then_should_keep_the_connection()
     {
        assert_eq!(
            Reuse::of(&session(7, 3, 10), &session(7, 3, 10)),
            Reuse::Keep
        );
        assert_eq!(
            Reuse::of(&session(7, 3, 10), &session(7, 3, 9)),
            Reuse::Keep
        );
    }

    #[test]
    fn given_another_session_when_routed_then_should_replace_the_connection() {
        assert_eq!(
            Reuse::of(&session(7, 3, 10), &session(7, 4, 10)),
            Reuse::Replace
        );
        assert_eq!(
            Reuse::of(&session(7, 3, 10), &session(8, 3, 10)),
            Reuse::Replace
        );
    }
}
