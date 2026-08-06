use axum::serve::ListenerExt;
use rmcp::model::ClientInfo;
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{Peer, RoleClient, ServiceExt};
use tokio_util::sync::CancellationToken;

use crate::BenchError;

pub(super) const HTTP_VERSION: &str = "http_1_1";

pub(super) struct McpTransport {
    pub peer: Peer<RoleClient>,
    server_port: u16,
    client: RunningService<RoleClient, ClientInfo>,
    cancellation: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl McpTransport {
    pub async fn start(
        router: axum::Router,
        cancellation: CancellationToken,
        concurrency: usize,
    ) -> Result<Self, BenchError> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to bind MCP server: {error}")))?;
        let address = listener.local_addr().map_err(|error| {
            BenchError::Invalid(format!("failed to read MCP server address: {error}"))
        })?;
        let listener = listener.tap_io(|stream| {
            stream
                .set_nodelay(true)
                .expect("benchmark MCP server must enable TCP_NODELAY");
        });
        let server_cancellation = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(server_cancellation.cancelled_owned())
                .await
        });
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(concurrency)
            .http1_only()
            .tcp_nodelay(true)
            .build()
            .map_err(|error| {
                BenchError::Invalid(format!("failed to build pooled MCP client: {error}"))
            })?;
        let transport = StreamableHttpClientTransport::with_client(
            http_client,
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        );
        let client = ClientInfo::default()
            .serve(transport)
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to initialize MCP: {error}")))?;
        let peer = client.peer().clone();
        Ok(Self {
            peer,
            server_port: address.port(),
            client,
            cancellation,
            server,
        })
    }

    pub fn server_port(&self) -> u16 {
        self.server_port
    }

    pub async fn stop(self) -> Result<(), BenchError> {
        self.client
            .cancel()
            .await
            .map_err(|error| BenchError::Invalid(format!("MCP client shutdown failed: {error}")))?;
        self.cancellation.cancel();
        self.server
            .await
            .map_err(|error| BenchError::Invalid(format!("MCP server task failed: {error}")))?
            .map_err(|error| BenchError::Invalid(format!("MCP server shutdown failed: {error}")))
    }
}
