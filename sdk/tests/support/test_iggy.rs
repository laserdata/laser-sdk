use bytes::Bytes;
use iggy::prelude::*;
use laser_sdk::prelude::Laser;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;
use tempfile::TempDir;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const IGGY_SERVER_ENV: &str = "LASER_TEST_IGGY_SERVER";

#[allow(dead_code)]
pub struct TestIggy {
    binary: PathBuf,
    child: Mutex<Child>,
    data_dir: TempDir,
    tcp_port: u16,
}

#[allow(dead_code)]
impl TestIggy {
    pub async fn start() -> Self {
        Self::start_inner(free_host_port()).await
    }

    pub async fn start_pinned() -> Self {
        Self::start_inner(free_host_port()).await
    }

    async fn start_inner(tcp_port: u16) -> Self {
        let binary = resolve_server_binary();
        let data_dir = tempfile::tempdir().expect("create Iggy test data directory");
        let child = spawn_server(&binary, data_dir.path(), tcp_port);
        let server = Self {
            binary,
            child: Mutex::new(child),
            data_dir,
            tcp_port,
        };
        server.wait_until_ready().await;
        server
    }

    pub async fn restart(&self) {
        {
            let mut child = self.child.lock().expect("lock Iggy test process");
            child.kill().expect("kill Iggy test process");
            child.wait().expect("reap Iggy test process");
            *child = spawn_server(&self.binary, self.data_dir.path(), self.tcp_port);
        }
        self.wait_until_ready().await;
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

    pub async fn laser_reconnecting(&self, stream: impl Into<String>) -> Result<Laser, IggyError> {
        let client = IggyClientBuilder::from_connection_string(&format!(
            "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@127.0.0.1:{}",
            self.tcp_port
        ))?
        .build()?;
        client.connect().await?;
        Ok(Laser::from_client(client).with_default_stream(stream))
    }

    async fn wait_until_ready(&self) {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            if self
                .child
                .lock()
                .expect("lock Iggy test process")
                .try_wait()
                .expect("inspect Iggy test process")
                .is_some()
            {
                let log = std::fs::read_to_string(self.log_path())
                    .unwrap_or_else(|error| format!("failed to read server log: {error}"));
                panic!(
                    "Iggy test server exited during startup, log {}:\n{log}",
                    self.log_path().display(),
                );
            }
            let ready = tokio::time::timeout(Duration::from_secs(2), self.probe_writes())
                .await
                .map(|result| result.is_ok())
                .unwrap_or(false);
            if ready {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                let log = std::fs::read_to_string(self.log_path())
                    .unwrap_or_else(|error| format!("failed to read server log: {error}"));
                panic!(
                    "Iggy did not accept VSR writes within 30s, log {}:\n{log}",
                    self.log_path().display(),
                );
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    async fn probe_writes(&self) -> Result<(), IggyError> {
        let client = self.client().await?;
        let stream = format!("ready_{}", std::process::id());
        let topic = "probe";
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
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.path().join("test-server.log")
    }
}

impl Drop for TestIggy {
    fn drop(&mut self) {
        let child = self.child.get_mut().expect("lock Iggy test process");
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn spawn_server(binary: &Path, data_dir: &Path, tcp_port: u16) -> Child {
    let log_path = data_dir.join("test-server.log");
    let stdout = File::create(&log_path).expect("create Iggy test server log");
    let stderr = stdout.try_clone().expect("clone Iggy test server log");
    Command::new(binary)
        .env("IGGY_SYSTEM_PATH", data_dir)
        .env("IGGY_TCP_ADDRESS", format!("127.0.0.1:{tcp_port}"))
        .env("IGGY_HTTP_ENABLED", "false")
        .env("IGGY_QUIC_ENABLED", "false")
        .env("IGGY_WEBSOCKET_ENABLED", "false")
        .env("IGGY_ROOT_USERNAME", DEFAULT_ROOT_USERNAME)
        .env("IGGY_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD)
        .env("IGGY_SHARD_RUNTIME_CAPACITY", "256")
        .env("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "all")
        .env("IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL", "200 ms")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn the VSR Iggy test server")
}

fn resolve_server_binary() -> PathBuf {
    if let Some(path) = std::env::var_os(IGGY_SERVER_ENV) {
        let path = PathBuf::from(path);
        assert!(
            path.is_file(),
            "{IGGY_SERVER_ENV} is not a file: {}",
            path.display()
        );
        return path;
    }

    let resolver = repository_root()
        .join("scripts")
        .join("resolve-test-iggy-server.sh");
    let output = Command::new(&resolver)
        .output()
        .expect("run the Iggy test server resolver");
    assert!(
        output.status.success(),
        "Iggy test server resolver failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    PathBuf::from(
        String::from_utf8(output.stdout)
            .expect("resolver path is UTF-8")
            .trim(),
    )
}

fn repository_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    if manifest.ends_with("sdk") {
        return manifest
            .parent()
            .expect("sdk has a repository parent")
            .to_owned();
    }
    manifest
        .parent()
        .and_then(Path::parent)
        .expect("bdd/rust has a repository parent")
        .to_owned()
}

fn free_host_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the bound port")
        .port()
}
