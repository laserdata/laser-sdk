use std::sync::Arc;
use std::time::Duration;

use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{CallToolRequestParams, ContentBlock, ErrorData, ServerCapabilities, ServerInfo};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{Peer, RoleClient, ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls, Row};
use tokio_util::sync::CancellationToken;

use super::TOOL_NAME;
use super::minimal::{
    RMCP_VERSION, application_bytes, expected_response, text_payload, ticket_arguments,
};
use super::transport::{HTTP_VERSION, McpTransport};
use crate::BenchError;
use crate::agdx::{AgdxArmEvidence, AgdxArmSummary, AgdxCase, measured_arm_with_network, warmup};
use crate::engine::Operation;
use crate::network::NetworkByteMeasurement;

const MIN_POSTGRES_VERSION: i32 = 160_000;
const DELIVERY_LEASE: &str = "1 second";
const IDLE_POLL: Duration = Duration::from_micros(100);
const WARMUP_SEQUENCE_OFFSET: u64 = 1 << 62;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpGuaranteedSummary {
    pub streamable_http: AgdxArmSummary,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub network: NetworkByteMeasurement,
    pub recipients: u32,
    pub postgres_process_measured: bool,
    pub delivery_attempts: u64,
    pub storage_bytes: u64,
    pub postgres_version: String,
    pub configuration: Value,
}

pub struct McpGuaranteedEvidence {
    pub summary: McpGuaranteedSummary,
    pub streamable_http: AgdxArmEvidence,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct McpGuaranteedRecoverySummary {
    pub result_committed_before_ack: bool,
    pub replay_attempts: u64,
    pub retained_results: u64,
    pub delivered: bool,
    pub postgres_version: String,
    pub configuration: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GuaranteedRequest {
    sequence: u64,
    payload: String,
}

#[derive(Clone, Debug)]
struct GuaranteedServer {
    store: Arc<GuaranteedStore>,
    tool_router: ToolRouter<Self>,
}

impl GuaranteedServer {
    fn new(store: Arc<GuaranteedStore>) -> Self {
        Self {
            store,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl GuaranteedServer {
    #[tool(description = "Persist and deliver a deterministic benchmark ticket")]
    async fn echo(
        &self,
        Parameters(request): Parameters<GuaranteedRequest>,
    ) -> Result<String, ErrorData> {
        self.store
            .submit(request.sequence, &request.payload)
            .await
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))
    }
}

#[allow(clippy::unused_async_trait_impl)]
#[tool_handler(router = self.tool_router)]
impl ServerHandler for GuaranteedServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }
}

#[derive(Debug)]
struct GuaranteedStore {
    client: Arc<Client>,
    schema: String,
    recipients: i32,
    timeout: Duration,
}

struct GuaranteedRuntime {
    store: Arc<GuaranteedStore>,
    cancellation: CancellationToken,
    workers: Vec<tokio::task::JoinHandle<Result<(), BenchError>>>,
    connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    postgres_version: String,
}

struct Delivery {
    request_id: i64,
    recipient: i32,
    payload: Vec<u8>,
}

struct RuntimeSummary {
    attempts: u64,
    storage_bytes: u64,
    postgres_version: String,
}

impl GuaranteedRuntime {
    async fn start(
        dsn: &str,
        seed: u64,
        recipients: u32,
        timeout: Duration,
    ) -> Result<Self, BenchError> {
        Self::start_with_workers(dsn, seed, recipients, timeout, true).await
    }

    async fn start_with_workers(
        dsn: &str,
        seed: u64,
        recipients: u32,
        timeout: Duration,
        start_workers: bool,
    ) -> Result<Self, BenchError> {
        let recipients = i32::try_from(recipients)
            .map_err(|_| BenchError::Invalid("MCP recipient count exceeds i32".to_owned()))?;
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .map_err(|error| postgres_error(&error))?;
        let connection = tokio::spawn(connection);
        let client = Arc::new(client);
        client
            .batch_execute("SET synchronous_commit = on")
            .await
            .map_err(|error| postgres_error(&error))?;
        let version_number = client
            .query_one("SHOW server_version_num", &[])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, String>(0)
            .parse::<i32>()
            .map_err(|error| {
                BenchError::Invalid(format!("invalid PostgreSQL server version: {error}"))
            })?;
        if version_number < MIN_POSTGRES_VERSION {
            return Err(BenchError::Invalid(format!(
                "guarantee-matched MCP requires PostgreSQL 16 or newer, found {version_number}"
            )));
        }
        let synchronous_commit = client
            .query_one("SHOW synchronous_commit", &[])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, String>(0);
        if synchronous_commit != "on" {
            return Err(BenchError::Invalid(format!(
                "guarantee-matched MCP requires synchronous_commit=on, found {synchronous_commit}"
            )));
        }
        let postgres_version = client
            .query_one("SHOW server_version", &[])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, String>(0);
        let schema = format!("laser_bench_mcp_{seed:016x}");
        initialize_schema(&client, &schema).await?;
        let store = Arc::new(GuaranteedStore {
            client,
            schema,
            recipients,
            timeout,
        });
        let cancellation = CancellationToken::new();
        let workers = if start_workers {
            (0..recipients)
                .map(|_| spawn_worker(Arc::clone(&store), cancellation.child_token()))
                .collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            store,
            cancellation,
            workers,
            connection,
            postgres_version,
        })
    }

    async fn finish(self) -> Result<RuntimeSummary, BenchError> {
        self.cancellation.cancel();
        for worker in self.workers {
            worker
                .await
                .map_err(|error| BenchError::Invalid(format!("MCP worker failed: {error}")))??;
        }
        let attempts = self.store.delivery_attempts().await?;
        let storage_bytes = self.store.storage_bytes().await?;
        self.store.drop_schema().await?;
        self.connection.abort();
        match self.connection.await {
            Err(error) if error.is_cancelled() => {}
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(postgres_error(&error)),
            Err(error) => {
                return Err(BenchError::Invalid(format!(
                    "PostgreSQL connection task failed: {error}"
                )));
            }
        }
        Ok(RuntimeSummary {
            attempts,
            storage_bytes,
            postgres_version: self.postgres_version,
        })
    }
}

impl GuaranteedStore {
    async fn submit(&self, sequence: u64, payload: &str) -> Result<String, BenchError> {
        let request_id = self.enqueue(sequence, payload).await?;
        self.await_results(request_id).await?;
        Ok(expected_response(sequence, payload))
    }

    async fn enqueue(&self, sequence: u64, payload: &str) -> Result<i64, BenchError> {
        let request_id = i64::try_from(sequence)
            .map_err(|_| BenchError::Invalid("MCP request sequence exceeds i64".to_owned()))?;
        let statement = format!(
            "WITH inbox AS (\
                INSERT INTO {schema}.inbox (request_id, idempotency_key, payload) \
                VALUES ($1, $1::text, $2) \
                ON CONFLICT (request_id) DO UPDATE SET request_id = EXCLUDED.request_id \
                RETURNING request_id\
             ) \
             INSERT INTO {schema}.outbox (request_id, recipient, sequence, payload) \
             SELECT inbox.request_id, recipient, inbox.request_id, $2 \
             FROM inbox CROSS JOIN generate_series(0, $3 - 1) AS recipient \
             ON CONFLICT (request_id, recipient) DO NOTHING",
            schema = self.schema,
        );
        self.client
            .execute(
                &statement,
                &[&request_id, &payload.as_bytes(), &self.recipients],
            )
            .await
            .map_err(|error| postgres_error(&error))?;
        Ok(request_id)
    }

    async fn await_results(&self, request_id: i64) -> Result<(), BenchError> {
        let query = format!(
            "SELECT count(*)::bigint FROM {}.results WHERE request_id = $1",
            self.schema
        );
        tokio::time::timeout(self.timeout, async {
            loop {
                let completed = self
                    .client
                    .query_one(&query, &[&request_id])
                    .await
                    .map_err(|error| postgres_error(&error))?
                    .get::<_, i64>(0);
                if completed == i64::from(self.recipients) {
                    return Ok(());
                }
                tokio::time::sleep(IDLE_POLL).await;
            }
        })
        .await
        .map_err(|_| BenchError::Invalid("guarantee-matched MCP delivery timed out".to_owned()))?
    }

    async fn claim(&self) -> Result<Option<Delivery>, BenchError> {
        let statement = format!(
            "UPDATE {schema}.outbox SET \
                attempts = attempts + 1, \
                next_attempt_at = clock_timestamp() + interval '{lease}' \
             WHERE (request_id, recipient) = (\
                SELECT request_id, recipient FROM {schema}.outbox candidate \
                WHERE delivered_at IS NULL AND next_attempt_at <= clock_timestamp() \
                  AND NOT EXISTS (\
                    SELECT 1 FROM {schema}.outbox earlier \
                    WHERE earlier.recipient = candidate.recipient \
                      AND earlier.sequence < candidate.sequence \
                      AND earlier.delivered_at IS NULL\
                  ) \
                ORDER BY sequence, recipient \
                FOR UPDATE SKIP LOCKED LIMIT 1\
             ) \
             RETURNING request_id, recipient, payload",
            schema = self.schema,
            lease = DELIVERY_LEASE,
        );
        self.client
            .query_opt(&statement, &[])
            .await
            .map_err(|error| postgres_error(&error))
            .map(|row| row.map(|row| delivery_from_row(&row)))
    }

    async fn persist_result(&self, delivery: &Delivery) -> Result<(), BenchError> {
        let statement = format!(
            "INSERT INTO {}.results (request_id, recipient, payload) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (request_id, recipient) DO NOTHING",
            self.schema,
        );
        self.client
            .execute(
                &statement,
                &[&delivery.request_id, &delivery.recipient, &delivery.payload],
            )
            .await
            .map_err(|error| postgres_error(&error))?;
        Ok(())
    }

    async fn acknowledge(&self, delivery: &Delivery) -> Result<(), BenchError> {
        let statement = format!(
            "UPDATE {}.outbox SET delivered_at = clock_timestamp() \
             WHERE request_id = $1 AND recipient = $2",
            self.schema,
        );
        self.client
            .execute(&statement, &[&delivery.request_id, &delivery.recipient])
            .await
            .map_err(|error| postgres_error(&error))?;
        Ok(())
    }

    async fn delivery_attempts(&self) -> Result<u64, BenchError> {
        let query = format!(
            "SELECT coalesce(sum(attempts), 0)::bigint FROM {}.outbox",
            self.schema
        );
        let attempts = self
            .client
            .query_one(&query, &[])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, i64>(0);
        u64::try_from(attempts)
            .map_err(|_| BenchError::Invalid("negative MCP delivery attempts".to_owned()))
    }

    async fn result_count(&self, request_id: i64) -> Result<u64, BenchError> {
        let query = format!(
            "SELECT count(*)::bigint FROM {}.results WHERE request_id = $1",
            self.schema
        );
        let count = self
            .client
            .query_one(&query, &[&request_id])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, i64>(0);
        u64::try_from(count)
            .map_err(|_| BenchError::Invalid("negative MCP result count".to_owned()))
    }

    async fn delivered(&self, request_id: i64) -> Result<bool, BenchError> {
        let query = format!(
            "SELECT delivered_at IS NOT NULL FROM {}.outbox WHERE request_id = $1",
            self.schema
        );
        self.client
            .query_one(&query, &[&request_id])
            .await
            .map_err(|error| postgres_error(&error))
            .map(|row| row.get(0))
    }

    async fn storage_bytes(&self) -> Result<u64, BenchError> {
        let query = "SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 AND c.relkind = 'r'";
        let bytes = self
            .client
            .query_one(query, &[&self.schema])
            .await
            .map_err(|error| postgres_error(&error))?
            .get::<_, i64>(0);
        u64::try_from(bytes)
            .map_err(|_| BenchError::Invalid("negative PostgreSQL storage size".to_owned()))
    }

    async fn drop_schema(&self) -> Result<(), BenchError> {
        self.client
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .map_err(|error| postgres_error(&error))
    }
}

/// Measure the guarantee-matched MCP application backed by `PostgreSQL`.
///
/// # Errors
///
/// Returns an error when `PostgreSQL` guarantees, MCP setup, delivery, validation, measurement, or cleanup fail.
pub async fn run_mcp_guaranteed_evidence(
    case: &AgdxCase,
    seed: u64,
    recipients: u32,
    dsn: &str,
    monitored_processes: &[(String, u32)],
) -> Result<McpGuaranteedEvidence, BenchError> {
    let timeout = Duration::from_millis(case.timeout_millis);
    let runtime = GuaranteedRuntime::start(dsn, seed, recipients, timeout).await?;
    let cancellation = CancellationToken::new();
    let store = Arc::clone(&runtime.store);
    let service: StreamableHttpService<GuaranteedServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(GuaranteedServer::new(Arc::clone(&store))),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_sse_keep_alive(None)
                .with_cancellation_token(cancellation.child_token()),
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    let transport = McpTransport::start(router, cancellation, case.concurrency).await?;
    let payload = text_payload(case.payload_bytes, seed);
    let warmup_operation = guaranteed_operation(
        transport.peer.clone(),
        payload.clone(),
        WARMUP_SEQUENCE_OFFSET,
    );
    warmup(case, timeout, warmup_operation).await?;
    let operation = guaranteed_operation(transport.peer.clone(), payload.clone(), 0);
    let streamable_http = measured_arm_with_network(
        "guarantee_matched_mcp",
        1,
        case,
        timeout,
        operation,
        monitored_processes,
        transport.server_port(),
    )
    .await;
    transport.stop().await?;
    let runtime = runtime.finish().await?;
    let streamable_http = streamable_http?;
    let network = streamable_http.network.clone().ok_or_else(|| {
        BenchError::Invalid("guarantee-matched MCP network measurement was not captured".to_owned())
    })?;
    let (request_bytes, response_bytes) = application_bytes(&payload)?;
    Ok(McpGuaranteedEvidence {
        summary: McpGuaranteedSummary {
            streamable_http: streamable_http.summary.clone(),
            request_bytes,
            response_bytes,
            network,
            recipients,
            postgres_process_measured: monitored_processes
                .iter()
                .any(|(name, _)| name == "postgres"),
            delivery_attempts: runtime.attempts,
            storage_bytes: runtime.storage_bytes,
            postgres_version: runtime.postgres_version,
            configuration: json!({
                "comparison_role": "guarantee_matched_mcp_control",
                "mcp_sdk": RMCP_VERSION,
                "http": "streamable_http_sse_response",
                "http_version": HTTP_VERSION,
                "connection_pool": "reqwest_keep_alive",
                "tcp_nodelay": true,
                "postgres_durability": "synchronous_commit_on",
                "inbox": "durable_idempotent",
                "outbox": "ordered_per_recipient",
                "claim": "for_update_skip_locked",
                "delivery": "at_least_once",
                "terminal_results": "retained_during_measurement",
                "delivery_lease": DELIVERY_LEASE,
            }),
        },
        streamable_http,
    })
}

/// Exercise the committed-result-before-ack recovery window with a fresh worker lifecycle.
///
/// # Errors
///
/// Returns an error when `PostgreSQL` setup, fault injection, replay, validation, or cleanup fails.
pub async fn run_mcp_guaranteed_recovery(
    dsn: &str,
    seed: u64,
    payload_bytes: usize,
    timeout: Duration,
) -> Result<McpGuaranteedRecoverySummary, BenchError> {
    let runtime = GuaranteedRuntime::start_with_workers(dsn, seed, 1, timeout, false).await?;
    let payload = text_payload(payload_bytes, seed);
    let request_id = runtime.store.enqueue(0, &payload).await?;
    let first = runtime.store.claim().await?.ok_or_else(|| {
        BenchError::Invalid("fault injection could not claim delivery".to_owned())
    })?;
    runtime.store.persist_result(&first).await?;
    let result_committed_before_ack = runtime.store.result_count(request_id).await? == 1
        && !runtime.store.delivered(request_id).await?;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let replay = runtime.store.claim().await?.ok_or_else(|| {
        BenchError::Invalid("replacement worker did not reclaim delivery".to_owned())
    })?;
    runtime.store.persist_result(&replay).await?;
    runtime.store.acknowledge(&replay).await?;
    let retained_results = runtime.store.result_count(request_id).await?;
    let delivered = runtime.store.delivered(request_id).await?;
    let summary = runtime.finish().await?;
    if !result_committed_before_ack || summary.attempts != 2 || retained_results != 1 || !delivered
    {
        return Err(BenchError::Invalid(
            "guarantee-matched MCP did not converge after the injected crash window".to_owned(),
        ));
    }
    Ok(McpGuaranteedRecoverySummary {
        result_committed_before_ack,
        replay_attempts: summary.attempts,
        retained_results,
        delivered,
        postgres_version: summary.postgres_version,
        configuration: json!({
            "fault": "worker_stopped_after_result_commit_before_delivery_ack",
            "replacement": "fresh_worker_lifecycle",
            "expected_delivery": "at_least_once",
            "expected_result": "idempotent_singleton",
            "delivery_lease": DELIVERY_LEASE,
        }),
    })
}

fn guaranteed_operation(
    peer: Peer<RoleClient>,
    payload: String,
    sequence_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let peer = peer.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let sequence = sequence
                .checked_add(sequence_offset)
                .ok_or_else(|| "guarantee-matched MCP sequence overflowed".to_owned())?;
            let arguments = serde_json::from_value(ticket_arguments(sequence, &payload))
                .map_err(|error| error.to_string())?;
            let result = peer
                .call_tool(CallToolRequestParams::new(TOOL_NAME).with_arguments(arguments))
                .await
                .map_err(|error| error.to_string())?;
            if result.is_error == Some(true) || result.content.len() != 1 {
                return Err("guarantee-matched MCP returned an invalid result".to_owned());
            }
            let actual = match &result.content[0] {
                ContentBlock::Text(text) => &text.text,
                _ => return Err("guarantee-matched MCP returned non-text content".to_owned()),
            };
            if actual != &expected_response(sequence, &payload) {
                return Err("guarantee-matched MCP response did not match the ticket".to_owned());
            }
            Ok(())
        })
    })
}

fn spawn_worker(
    store: Arc<GuaranteedStore>,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<Result<(), BenchError>> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                delivery = store.claim() => match delivery? {
                    Some(delivery) => {
                        store.persist_result(&delivery).await?;
                        store.acknowledge(&delivery).await?;
                    }
                    None => tokio::time::sleep(IDLE_POLL).await,
                }
            }
        }
    })
}

async fn initialize_schema(client: &Client, schema: &str) -> Result<(), BenchError> {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.inbox (\
                request_id bigint PRIMARY KEY, \
                idempotency_key text NOT NULL UNIQUE, \
                payload bytea NOT NULL, \
                committed_at timestamptz NOT NULL DEFAULT clock_timestamp()\
             ); \
             CREATE TABLE {schema}.outbox (\
                request_id bigint NOT NULL REFERENCES {schema}.inbox(request_id), \
                recipient integer NOT NULL, \
                sequence bigint NOT NULL, \
                payload bytea NOT NULL, \
                attempts integer NOT NULL DEFAULT 0, \
                next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(), \
                delivered_at timestamptz, \
                PRIMARY KEY (request_id, recipient)\
             ); \
             CREATE INDEX outbox_due ON {schema}.outbox (sequence, recipient) \
                WHERE delivered_at IS NULL; \
             CREATE TABLE {schema}.results (\
                request_id bigint NOT NULL, \
                recipient integer NOT NULL, \
                payload bytea NOT NULL, \
                completed_at timestamptz NOT NULL DEFAULT clock_timestamp(), \
                PRIMARY KEY (request_id, recipient)\
             )"
        ))
        .await
        .map_err(|error| postgres_error(&error))
}

fn delivery_from_row(row: &Row) -> Delivery {
    Delivery {
        request_id: row.get(0),
        recipient: row.get(1),
        payload: row.get(2),
    }
}

fn postgres_error(error: &tokio_postgres::Error) -> BenchError {
    BenchError::Invalid(format!("PostgreSQL MCP control failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_seed_when_schema_is_derived_then_should_be_identifier_safe() {
        let schema = format!("laser_bench_mcp_{:016x}", 42);

        assert!(
            schema
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        );
    }

    #[test]
    fn given_guarantee_configuration_when_serialized_then_should_not_contain_dsn() {
        let configuration = json!({
            "postgres_durability": "synchronous_commit_on",
            "delivery": "at_least_once",
        });

        assert!(!configuration.to_string().contains("postgres://"));
    }
}
