use bytes::Bytes;
use iggy::prelude::*;
use laser_sdk::prelude::Laser;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IGGY_IMAGE: &str = "apache/iggy";
// `edge` is a moving tag. Pin a release via LASER_TEST_IGGY_TAG for reproducible runs.
const IGGY_DEFAULT_TAG: &str = "edge";
const IGGY_TAG_ENV: &str = "LASER_TEST_IGGY_TAG";
const IGGY_TCP_PORT: u16 = 3000;
const IGGY_HTTP_PORT: u16 = 80;

pub struct TestIggy {
    container: ContainerAsync<GenericImage>,
    tcp_port: u16,
}

impl TestIggy {
    /// A container with an ephemeral host port, the default for the shared
    /// harness (many run in parallel, so the port must not collide).
    pub async fn start() -> Self {
        Self::start_inner(None).await
    }

    /// A container whose TCP host port is pinned. `docker restart` reassigns an
    /// ephemeral host port, which would leave a reconnecting client pointed at a
    /// dead address, so the reconnect test pins the port to a free one picked up
    /// front and it survives the restart. Returns the same fixed address after.
    pub async fn start_pinned() -> Self {
        Self::start_inner(Some(free_host_port())).await
    }

    async fn start_inner(pinned_tcp_port: Option<u16>) -> Self {
        let tag = std::env::var(IGGY_TAG_ENV).unwrap_or_else(|_| IGGY_DEFAULT_TAG.to_owned());
        let mut image = GenericImage::new(IGGY_IMAGE, tag.as_str())
            .with_exposed_port(ContainerPort::Tcp(IGGY_TCP_PORT))
            .with_exposed_port(ContainerPort::Tcp(IGGY_HTTP_PORT))
            .with_wait_for(
                HttpWaitStrategy::new("/")
                    .with_port(ContainerPort::Tcp(IGGY_HTTP_PORT))
                    .with_expected_status_code(200u16)
                    .into(),
            )
            .with_cap_add("SYS_NICE")
            .with_security_opt("seccomp=unconfined")
            .with_ulimit("memlock", -1, Some(-1))
            .with_ulimit("nofile", 65_536, Some(65_536))
            .with_env_var("IGGY_ROOT_USERNAME", DEFAULT_ROOT_USERNAME)
            .with_env_var("IGGY_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD)
            .with_env_var("IGGY_HTTP_ENABLED", "true")
            .with_env_var("IGGY_HTTP_ADDRESS", "0.0.0.0:80")
            .with_env_var("IGGY_TCP_ENABLED", "true")
            .with_env_var("IGGY_TCP_ADDRESS", "0.0.0.0:3000");
        if let Some(port) = pinned_tcp_port {
            // Pin the HTTP port too (to another free port), or the readiness
            // wait cannot resolve its host mapping once any port is pinned.
            image = image
                .with_mapped_port(port, ContainerPort::Tcp(IGGY_TCP_PORT))
                .with_mapped_port(free_host_port(), ContainerPort::Tcp(IGGY_HTTP_PORT));
        }

        let container = image.start().await.expect("failed to start iggy container");
        let tcp_port = match pinned_tcp_port {
            Some(port) => port,
            None => container
                .get_host_port_ipv4(IGGY_TCP_PORT)
                .await
                .expect("failed to get iggy tcp port"),
        };
        wait_for_tcp_writes(tcp_port).await;
        Self {
            container,
            tcp_port,
        }
    }

    pub fn container_id(&self) -> &str {
        self.container.id()
    }

    /// Restart the server in place, keeping the same mapped ports so every
    /// connected client sees its connection die and must reconnect. A hard
    /// stop: without fsync-on-write the server can lose unflushed messages,
    /// so this is for reconnect testing, never data-survival assertions.
    /// Returns once the restarted server answers a fresh login again.
    pub async fn restart(&self) {
        // `-t 1` bounds the graceful-stop wait to a second before the kill, so
        // the restart does not sit on docker's default 10s SIGTERM grace.
        let status = tokio::process::Command::new("docker")
            .args(["restart", "-t", "1", self.container.id()])
            .status()
            .await
            .expect("docker restart runs");
        assert!(status.success(), "docker restart failed");
        // Each probe is bounded: mid-restart the port can accept a TCP connect
        // yet never complete the handshake, which would hang the loop forever
        // and starve the deadline check. A timed-out probe is just "not ready".
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let ready = tokio::time::timeout(std::time::Duration::from_secs(2), self.client())
                .await
                .map(|result| result.is_ok())
                .unwrap_or(false);
            if ready {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "iggy did not come back within 20s of the restart"
            );
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }

    pub async fn client(&self) -> Result<IggyClient, IggyError> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(format!("127.0.0.1:{}", self.tcp_port))
            .build()?;
        client.connect().await?;
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await?;
        Ok(client)
    }

    pub async fn laser(&self, stream: impl Into<String>) -> Result<Laser, IggyError> {
        Ok(Laser::from_client(self.client().await?).with_default_stream(stream))
    }

    /// A laser whose client carries auto-login credentials, so iggy-rs
    /// re-authenticates on every reconnect. The reconnect test needs this to
    /// resume a session after a server restart (the plain `client` above logs
    /// in once and stays unauthenticated after the socket drops). Built from a
    /// connection string, exactly the real `Laser::connect` path.
    pub async fn laser_reconnecting(&self, stream: impl Into<String>) -> Result<Laser, IggyError> {
        let client = IggyClientBuilder::from_connection_string(&format!(
            "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@127.0.0.1:{}",
            self.tcp_port
        ))?
        .build()?;
        client.connect().await?;
        Ok(Laser::from_client(client).with_default_stream(stream))
    }
}

async fn wait_for_tcp_writes(tcp_port: u16) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    let stream = format!("ready_{}", std::process::id());
    let topic = "probe";
    loop {
        let ready = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let client = IggyClientBuilder::new()
                .with_tcp()
                .with_server_address(format!("127.0.0.1:{tcp_port}"))
                .build()?;
            client.connect().await?;
            client
                .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
                .await?;
            if client
                .get_stream(&Identifier::named(&stream)?)
                .await?
                .is_none()
            {
                client.create_stream(&stream).await?;
            }
            let stream_id = Identifier::named(&stream)?;
            let topic_id = Identifier::named(topic)?;
            if client.get_topic(&stream_id, &topic_id).await?.is_none() {
                client
                    .create_topic(
                        &stream_id,
                        topic,
                        1,
                        CompressionAlgorithm::default(),
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::ServerDefault,
                    )
                    .await?;
            }
            let producer = client.producer(&stream, topic)?.build();
            producer.init().await?;
            let message = IggyMessage::builder()
                .payload(Bytes::from_static(b"ready"))
                .build()?;
            producer.send(vec![message]).await
        })
        .await
        .map(|result: Result<_, IggyError>| result.is_ok())
        .unwrap_or(false);
        if ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "iggy did not accept tcp writes within 20s of container startup"
        );
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
}

// A currently-free host port, grabbed by letting the OS assign one and dropping
// the listener. A pinned container maps it, so its address survives a restart
// without the collision risk of a hardcoded port.
fn free_host_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the bound port")
        .port()
}
