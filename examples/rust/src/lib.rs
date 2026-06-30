use async_trait::async_trait;
use iggy::prelude::*;
use laser_sdk::laser::Laser;
use laser_sdk::prelude::{Capabilities, LaserError};
use laser_sdk::query::{ContentType, Projection, ProjectionBinding};
use std::path::PathBuf;
use std::time::{Duration, Instant};

// Stream name prefix when `LASER_STREAM` is unset. Each example gets its own
// stream (`laser-<example>`, see `stream_for`), never one shared stream.
// AGDX isolates workloads by stream, never by partition: unrelated apps sharing
// one stream would also share the well-known
// agent topics (`agent.commands`, `agent.tool_calls`, ...), so each app's
// freshly joined consumer group would replay the other app's traffic from
// offset 0 and dead-letter every message it cannot decode. Per-example streams
// let all examples run against one local server without colliding.
pub const DEFAULT_STREAM: &str = "laser";
pub const PARTITIONS: u32 = 4;

/// The data stream an example uses: `LASER_STREAM` if set (managed: your
/// provisioned stream, so the SDK auto-creates nothing), else a per-example
/// stream `laser-<example>`. The `_agdx` ops stream is owned by LaserData Cloud
/// (created at boot in the cloud), not by the SDK.
pub fn stream_for(example: &str) -> String {
    std::env::var("LASER_STREAM")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{DEFAULT_STREAM}-{example}"))
}

/// Shared volume knobs every data-publishing example honors, so one run scales
/// from ten records to millions without editing code. Each example passes its
/// own default. The env var wins when set:
/// `LASER_MESSAGES` (total records), `LASER_BATCH` (records per send call),
/// `LASER_CONCURRENCY` (parallel publishers), `LASER_PAYLOAD_BYTES`
/// (approximate body size in bytes).
pub fn messages(default: u64) -> u64 {
    env_u64("LASER_MESSAGES", default).max(1)
}

pub fn batch(default: usize) -> usize {
    env_usize("LASER_BATCH", default).max(1)
}

pub fn concurrency(default: usize) -> usize {
    env_usize("LASER_CONCURRENCY", default).max(1)
}

pub fn payload_bytes(default: usize) -> usize {
    env_usize("LASER_PAYLOAD_BYTES", default)
}

pub fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

pub fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

// Local single-node Iggy (root creds), the default when no cloud target is set.
const LOCAL_CONNECTION_STRING: &str = "iggy://iggy:iggy@127.0.0.1:8090";
const DEFAULT_TCP_PORT: u16 = 8090;

// Public CA certificates for LaserData Cloud, vendored under `examples/certs/`
// (examples-level, language-agnostic). `connect()` picks one by the host
// subdomain: a `.sandbox`/`.dev` host uses the dev CA, everything else the prod CA.
static PROD_CERT: &[u8] = include_bytes!("../../certs/laserdata.crt");
static DEV_CERT: &[u8] = include_bytes!("../../certs/laserdata_dev.crt");

/// The example-side LLM seam. The SDK never calls an LLM, so examples plug a
/// model in here. `MockLlm` keeps every example deterministic and key-free in
/// CI. `AnthropicLlm` (feature `llm-anthropic`) is the same example code, real model.
#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, prompt: &str) -> String;
}

pub struct MockLlm;

#[async_trait]
impl LlmClient for MockLlm {
    async fn complete(&self, prompt: &str) -> String {
        format!("[mock-llm] {prompt}")
    }
}

/// Picks the LLM backend for an example: real Claude (`llm-anthropic` +
/// `ANTHROPIC_API_KEY`) or OpenAI (`llm-openai` + `OPENAI_API_KEY`) when built
/// and keyed, otherwise the deterministic `MockLlm`. Same example code runs free
/// in CI and "for real" with a key.
pub fn default_llm() -> std::sync::Arc<dyn LlmClient> {
    #[cfg(feature = "llm-anthropic")]
    if let Some(client) = anthropic::AnthropicLlm::from_env() {
        return std::sync::Arc::new(client);
    }
    #[cfg(feature = "llm-openai")]
    if let Some(client) = openai::OpenAiLlm::from_env() {
        return std::sync::Arc::new(client);
    }
    std::sync::Arc::new(MockLlm)
}

#[cfg(feature = "llm-anthropic")]
mod anthropic {
    use super::LlmClient;
    use async_trait::async_trait;
    use serde::Deserialize;

    const ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
    const API_VERSION: &str = "2023-06-01";
    const DEFAULT_MODEL: &str = "claude-sonnet-4-6";
    const MAX_TOKENS: u32 = 1024;

    // Real Claude backend. Reads ANTHROPIC_API_KEY (required) and ANTHROPIC_MODEL
    // (optional) from the environment. The SDK itself never does this.
    pub struct AnthropicLlm {
        client: reqwest::Client,
        api_key: String,
        model: String,
    }

    impl AnthropicLlm {
        pub fn from_env() -> Option<Self> {
            let api_key = std::env::var("ANTHROPIC_API_KEY").ok()?;
            let model =
                std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_owned());
            Some(Self {
                client: reqwest::Client::new(),
                api_key,
                model,
            })
        }
    }

    #[derive(Deserialize)]
    struct MessageResponse {
        content: Vec<ContentBlock>,
    }

    #[derive(Deserialize)]
    struct ContentBlock {
        #[serde(default)]
        text: String,
    }

    #[async_trait]
    impl LlmClient for AnthropicLlm {
        async fn complete(&self, prompt: &str) -> String {
            let body = serde_json::json!({
                "model": self.model,
                "max_tokens": MAX_TOKENS,
                "messages": [{ "role": "user", "content": prompt }],
            });
            let response = match self
                .client
                .post(ENDPOINT)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", API_VERSION)
                .json(&body)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
            {
                Ok(response) => response,
                Err(error) => return format!("[anthropic-request-error] {error}"),
            };
            match response.json::<MessageResponse>().await {
                Ok(parsed) => parsed.content.into_iter().map(|b| b.text).collect(),
                Err(error) => format!("[anthropic-decode-error] {error}"),
            }
        }
    }
}

#[cfg(feature = "llm-openai")]
mod openai {
    use super::LlmClient;
    use async_trait::async_trait;
    use serde::Deserialize;

    const ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
    const DEFAULT_MODEL: &str = "gpt-4o";

    // Real OpenAI backend. Reads OPENAI_API_KEY (required) and OPENAI_MODEL
    // (optional) from the environment. The SDK itself never does this.
    pub struct OpenAiLlm {
        client: reqwest::Client,
        api_key: String,
        model: String,
    }

    impl OpenAiLlm {
        pub fn from_env() -> Option<Self> {
            let api_key = std::env::var("OPENAI_API_KEY").ok()?;
            let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_owned());
            Some(Self {
                client: reqwest::Client::new(),
                api_key,
                model,
            })
        }
    }

    #[derive(Deserialize)]
    struct ChatResponse {
        choices: Vec<Choice>,
    }

    #[derive(Deserialize)]
    struct Choice {
        message: ChoiceMessage,
    }

    #[derive(Deserialize)]
    struct ChoiceMessage {
        #[serde(default)]
        content: String,
    }

    #[async_trait]
    impl LlmClient for OpenAiLlm {
        async fn complete(&self, prompt: &str) -> String {
            let body = serde_json::json!({
                "model": self.model,
                "messages": [{ "role": "user", "content": prompt }],
            });
            let response = match self
                .client
                .post(ENDPOINT)
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
            {
                Ok(response) => response,
                Err(error) => return format!("[openai-request-error] {error}"),
            };
            match response.json::<ChatResponse>().await {
                Ok(parsed) => parsed
                    .choices
                    .into_iter()
                    .next()
                    .map(|c| c.message.content)
                    .unwrap_or_default(),
                Err(error) => format!("[openai-decode-error] {error}"),
            }
        }
    }
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

/// A titled phase header printed to stdout, fencing an example into clear acts so
/// the output reads as a story rather than an undifferentiated log stream. Bold
/// cyan title over a matching rule, so it stands apart from the tracing lines.
pub fn phase(title: &str) {
    let rule = "─".repeat(title.chars().count() + 3);
    println!("\n\x1b[1;36m▸ {title}\x1b[0m\n\x1b[36m{rule}\x1b[0m");
}

/// Connect the way every example does. Resolves a target from the environment,
/// so the same binary runs against a local Iggy or a LaserData Cloud deployment
/// with no code change:
///
/// - `LASER_CONNECTION_STRING`: a full iggy connection string with embedded
///   credentials, the easiest cloud path: `iggy+tcp://user:pwd@host` or
///   `iggy+tcp://<token>@host` (a bare `user:pwd@host` works too, the
///   `iggy+tcp://` scheme is added). The port defaults to 8090 when omitted.
/// - `LASER_SERVER`: a bootstrap host (e.g.
///   `starter-123.us-west-1.aws.sandbox.laserdata.cloud`) plus auth from
///   `LASER_TOKEN` (PAT) or `LASER_USERNAME` and `LASER_PASSWORD`.
/// - neither set: local default (`iggy://iggy:iggy@127.0.0.1:8090`).
///
/// TLS resolution: when the host is a LaserData Cloud host (`*.laserdata.cloud`)
/// and the string did not already carry a `tls_ca_file=`, the matching CA cert is
/// attached automatically (and `tls=true` added if missing), so passing just the
/// connection string is enough. The CA is chosen by subdomain: a `.sandbox`/`.dev`
/// host uses the dev CA, any other LaserData host the prod CA. Non-LaserData
/// hosts are left untouched (manage their own TLS in the string). Override the
/// cert with `LASER_TLS_CERT=<path>`, disable with `LASER_NO_TLS=1`.
pub async fn connect() -> Result<IggyClient, LaserError> {
    let connection_string = resolve_connection_string()?;
    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}

/// Build a `Laser` on `stream` over the resolved connection. This is the path
/// every example should use: unlike `Laser::from_client`, the builder runs the
/// connect-time `AGDX_HELLO` capability probe, so against LaserData Cloud the query
/// path negotiates the `AGDX_QUERY` managed command. Pass the capabilities the example
/// needs (e.g. `managed_query` because the example spawns its own in-process query
/// worker for offline demos). Raw Apache Iggy on its own has no query LaserData Cloud and
/// would return `Unsupported`.
/// The probe only ever adds the fork-implied capability on top.
pub async fn laser(stream: &str, capabilities: Capabilities) -> Result<Laser, LaserError> {
    Laser::builder()
        .client(connect().await?)
        .stream(stream)
        .capabilities(capabilities)
        .build()
        .await
}

/// Guard for the LaserData-Cloud-only examples. Returns `true` when `enabled` (the
/// connected streaming infrastructure negotiated the capability, so run the
/// demo). When it is `false`, prints what is missing and how to point the
/// example at LaserData Cloud, then returns `false` so the caller can exit
/// cleanly. The example stays green in CI on raw Apache Iggy instead of
/// erroring.
pub fn fork_feature_ready(enabled: bool, feature: &str, example: &str) -> bool {
    if enabled {
        return true;
    }
    println!(
        "The connected Apache Iggy is not LaserData Cloud, so {feature} is unavailable. Point the \
         example at LaserData Cloud to run it live:\n\n    \
         LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host cargo run --example {example}\n"
    );
    false
}

fn resolve_connection_string() -> Result<String, LaserError> {
    // 1. A full connection string with credentials embedded.
    if let Ok(provided) = std::env::var("LASER_CONNECTION_STRING")
        && !provided.trim().is_empty()
    {
        return with_resolved_tls(provided.trim().to_owned());
    }
    // 2. A bootstrap host plus separate credentials.
    let server = std::env::var("LASER_SERVER").unwrap_or_default();
    let server = server.trim();
    if server.is_empty() {
        // 3. Local default.
        return Ok(LOCAL_CONNECTION_STRING.to_owned());
    }
    let credentials = resolve_credentials()?;
    with_resolved_tls(format!("iggy+tcp://{credentials}{server}"))
}

fn resolve_credentials() -> Result<String, LaserError> {
    if let Ok(token) = std::env::var("LASER_TOKEN")
        && !token.is_empty()
    {
        return Ok(format!("{token}@"));
    }
    match (
        std::env::var("LASER_USERNAME"),
        std::env::var("LASER_PASSWORD"),
    ) {
        (Ok(username), Ok(password)) if !username.is_empty() && !password.is_empty() => {
            Ok(format!("{username}:{password}@"))
        }
        _ => Err(LaserError::Invalid(
            "LaserData Cloud needs credentials: set LASER_TOKEN, or LASER_USERNAME + LASER_PASSWORD"
                .to_owned(),
        )),
    }
}

// Normalize the scheme and the port, then for a LaserData Cloud host that does
// not already name a CA file, attach `tls=true` plus the matching CA cert.
// Non-LaserData hosts and strings that opt out (`LASER_NO_TLS`) pass through.
fn with_resolved_tls(connection_string: String) -> Result<String, LaserError> {
    let mut connection_string = connection_string;
    if !connection_string.contains("://") {
        connection_string = format!("iggy+tcp://{connection_string}");
    }
    connection_string = ensure_default_port(connection_string);

    let host = host_of(&connection_string).to_owned();
    if std::env::var("LASER_NO_TLS").is_ok()
        || !is_laserdata_host(&host)
        || connection_string.contains("tls_ca_file=")
    {
        return Ok(connection_string);
    }
    let cert_path = resolve_cert_path(&host)?;
    Ok(attach_tls(
        &connection_string,
        &cert_path.display().to_string(),
    ))
}

// Append `tls=true` (when absent) and `tls_ca_file=<cert>` to the connection
// string. The caller has already checked that no `tls_ca_file=` is present.
fn attach_tls(connection_string: &str, cert_path: &str) -> String {
    let mut with_tls = connection_string.to_owned();
    if !with_tls.contains("tls=") {
        let separator = if with_tls.contains('?') { '&' } else { '?' };
        with_tls = format!("{with_tls}{separator}tls=true");
    }
    format!("{with_tls}&tls_ca_file={cert_path}")
}

// Append the default TCP port when the authority has none, so callers can pass a
// bare host. An explicit port, path, and query are left intact.
fn ensure_default_port(connection_string: String) -> String {
    let Some((scheme, remainder)) = connection_string.split_once("://") else {
        return connection_string;
    };
    let (authority, path_and_query) = match remainder.find(['/', '?']) {
        Some(index) => (&remainder[..index], &remainder[index..]),
        None => (remainder, ""),
    };
    let (user_info, host_and_port) = match authority.rsplit_once('@') {
        Some((user_info, host)) => (format!("{user_info}@"), host),
        None => (String::new(), authority),
    };
    if host_and_port.contains(':') {
        return connection_string;
    }
    format!("{scheme}://{user_info}{host_and_port}:{DEFAULT_TCP_PORT}{path_and_query}")
}

// Extract the bare host from a connection string: drop the `scheme://`, the
// `/path` and `?query`, the `user:pwd@` / `token@` credentials, and the `:port`.
fn host_of(connection_string: &str) -> &str {
    let after_scheme = connection_string
        .split_once("://")
        .map_or(connection_string, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?'])
        .next()
        .unwrap_or(after_scheme);
    let host_and_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    host_and_port.split(':').next().unwrap_or(host_and_port)
}

// True for LaserData Cloud hosts (`*.laserdata.cloud`). The trailing-dot match
// rejects look-alikes like `laserdata.cloud.attacker.com`.
fn is_laserdata_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "laserdata.cloud" || host.ends_with(".laserdata.cloud")
}

/// A projector handle for the query examples. Query is a managed (LaserData
/// Cloud) feature, so the projection is registered on the Cloud, which runs the
/// projector. Returned by [`start_projector`].
pub struct Projector;

impl Projector {
    pub async fn shutdown(self) {}
}

/// Register `topic`'s projection over `fields` on LaserData Cloud so
/// `query(topic)` returns rows. Query is managed-only, so this requires a
/// LaserData Cloud deployment (on raw Apache Iggy the registration and the
/// query both return `LaserError::Unsupported`).
pub async fn start_projector(
    laser: &Laser,
    topic: &str,
    content_type: ContentType,
    fields: &[&str],
) -> Result<Projector, LaserError> {
    register_cloud_projection(laser, topic, content_type, fields).await?;
    Ok(Projector)
}

async fn register_cloud_projection(
    laser: &Laser,
    topic: &str,
    content_type: ContentType,
    fields: &[&str],
) -> Result<(), LaserError> {
    let projection_id = format!("{topic}.v1");
    // `index_only` so per-record `.inline_payload()` decides inlining, matching
    // the local worker (raw payloads stay on the log, opted-in bodies are kept).
    let mut projection = Projection::builder(projection_id.clone())
        .name(topic)
        .version(1)
        .content_type(content_type)
        .index_only();
    for field in fields {
        projection = projection.field(*field);
    }
    laser.projections().register(projection.build()).await?;

    let binding = ProjectionBinding::builder()
        .source(
            laser
                .stream()
                .expect("the projector needs a default stream"),
            topic,
        )
        .allow(projection_id.clone())
        .default_projection(projection_id)
        .target_table(topic)
        .build();
    laser.bindings().apply(binding).await?;

    // Wait until LaserData Cloud has applied the registration and created the index
    // (its table) before we return, so records published next flow into a live
    // projector rather than being missed by one that spawns afterwards. A query
    // errors until the table exists, then returns an empty page.
    let deadline = Instant::now() + Duration::from_secs(30);
    while laser.query(topic).fetch().await.is_err() {
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "LaserData Cloud did not create index `{topic}` in time (is LaserData Cloud running and consuming `control.commands`?)"
            )));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

// Dev environments live under a `.sandbox` or `.dev` subdomain
// (`starter-123.us-west-1.aws.sandbox.laserdata.cloud`). Everything else is prod.
fn is_dev_host(host: &str) -> bool {
    host.split('.')
        .any(|label| label.eq_ignore_ascii_case("sandbox") || label.eq_ignore_ascii_case("dev"))
}

fn resolve_cert_path(host: &str) -> Result<PathBuf, LaserError> {
    if let Ok(custom) = std::env::var("LASER_TLS_CERT")
        && !custom.is_empty()
    {
        return Ok(PathBuf::from(custom));
    }
    let (bytes, name) = if is_dev_host(host) {
        (DEV_CERT, "laserdata_dev.crt")
    } else {
        (PROD_CERT, "laserdata.crt")
    };
    let path = std::env::temp_dir().join(name);
    if !path.exists() {
        std::fs::write(&path, bytes)
            .map_err(|error| LaserError::Invalid(format!("write CA cert: {error}")))?;
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::{attach_tls, ensure_default_port, host_of, is_dev_host, is_laserdata_host};

    #[test]
    fn given_a_host_without_a_port_when_normalized_then_should_add_the_default() {
        assert_eq!(
            ensure_default_port("iggy+tcp://u:p@h.laserdata.cloud".to_owned()),
            "iggy+tcp://u:p@h.laserdata.cloud:8090"
        );
        assert_eq!(
            ensure_default_port("iggy+tcp://token@h.laserdata.cloud?x=1".to_owned()),
            "iggy+tcp://token@h.laserdata.cloud:8090?x=1"
        );
    }

    #[test]
    fn given_a_host_with_a_port_when_normalized_then_should_be_unchanged() {
        assert_eq!(
            ensure_default_port("iggy+tcp://u:p@h.laserdata.cloud:9000".to_owned()),
            "iggy+tcp://u:p@h.laserdata.cloud:9000"
        );
    }

    #[test]
    fn given_connection_strings_when_parsed_then_should_extract_the_bare_host() {
        assert_eq!(
            host_of("iggy+tcp://user:pwd@starter-123.aws.sandbox.laserdata.cloud:8090"),
            "starter-123.aws.sandbox.laserdata.cloud"
        );
        assert_eq!(
            host_of("iggy+tcp://token@host.laserdata.cloud:8090?tls=true"),
            "host.laserdata.cloud"
        );
        assert_eq!(
            host_of("user:pwd@host.laserdata.cloud:8090"),
            "host.laserdata.cloud"
        );
        assert_eq!(host_of("iggy://iggy:iggy@127.0.0.1:8090"), "127.0.0.1");
    }

    #[test]
    fn given_hosts_when_checked_then_should_detect_laserdata_cloud() {
        assert!(is_laserdata_host(
            "starter-123.us-west-1.aws.sandbox.laserdata.cloud"
        ));
        assert!(is_laserdata_host("host.laserdata.cloud"));
        assert!(is_laserdata_host("laserdata.cloud"));
        // not LaserData: third-party hosts + look-alikes get no auto cert.
        assert!(!is_laserdata_host("127.0.0.1"));
        assert!(!is_laserdata_host("iggy.example.com"));
        assert!(!is_laserdata_host("laserdata.cloud.attacker.com"));
    }

    #[test]
    fn given_a_sandbox_or_dev_host_when_checked_then_should_select_the_dev_cert() {
        assert!(is_dev_host(
            "starter-123.us-west-1.aws.sandbox.laserdata.cloud"
        ));
        assert!(is_dev_host("starter-123.eu-west-1.dev.laserdata.cloud"));
    }

    #[test]
    fn given_a_prod_host_when_checked_then_should_select_the_prod_cert() {
        assert!(!is_dev_host("starter-123.us-west-1.aws.laserdata.cloud"));
        assert!(!is_dev_host("localhost"));
    }

    #[test]
    fn given_a_string_without_tls_when_attaching_then_should_add_tls_and_ca() {
        assert_eq!(
            attach_tls("iggy+tcp://u:p@h.laserdata.cloud:8090", "/tmp/ca.crt"),
            "iggy+tcp://u:p@h.laserdata.cloud:8090?tls=true&tls_ca_file=/tmp/ca.crt"
        );
    }

    #[test]
    fn given_tls_true_without_ca_when_attaching_then_should_add_only_the_ca() {
        // `tls=true` passed but no cert: we still inject the CA file.
        assert_eq!(
            attach_tls(
                "iggy+tcp://t@h.laserdata.cloud:8090?tls=true",
                "/tmp/ca.crt"
            ),
            "iggy+tcp://t@h.laserdata.cloud:8090?tls=true&tls_ca_file=/tmp/ca.crt"
        );
    }

    #[test]
    fn given_an_existing_query_when_attaching_then_should_use_ampersand() {
        assert_eq!(
            attach_tls("iggy+tcp://h.laserdata.cloud:8090?foo=bar", "/tmp/ca.crt"),
            "iggy+tcp://h.laserdata.cloud:8090?foo=bar&tls=true&tls_ca_file=/tmp/ca.crt"
        );
    }
}
