use bytes::Bytes;
use iggy::prelude::*;
use laser_sdk::prelude::Laser;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tempfile::TempDir;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const IGGY_SERVER_ENV: &str = "LASER_TEST_IGGY_SERVER";
const CLUSTER_SIZE: usize = 3;

struct ClusterNodeProcess {
    child: Child,
    data_dir: TempDir,
    log_path: PathBuf,
}

pub struct TestIggyCluster {
    binary: PathBuf,
    nodes: Vec<ClusterNodeProcess>,
    tcp_ports: [u16; CLUSTER_SIZE],
    replica_ports: [u16; CLUSTER_SIZE],
    proxy_port: u16,
    proxy_target: Arc<AtomicU16>,
    proxy_task: tokio::task::JoinHandle<()>,
}

impl TestIggyCluster {
    pub async fn start() -> Self {
        let binary = resolve_server_binary();
        let ports = free_host_ports(CLUSTER_SIZE * 2 + 1);
        let tcp_ports: [u16; CLUSTER_SIZE] = ports[..CLUSTER_SIZE].try_into().expect("TCP ports");
        let replica_ports: [u16; CLUSTER_SIZE] = ports[CLUSTER_SIZE..CLUSTER_SIZE * 2]
            .try_into()
            .expect("replica ports");
        let proxy_port = ports[CLUSTER_SIZE * 2];
        let proxy_target = Arc::new(AtomicU16::new(tcp_ports[0]));
        let proxy_task = spawn_stable_proxy(proxy_port, proxy_target.clone()).await;
        let mut nodes = Vec::with_capacity(CLUSTER_SIZE);
        for replica_id in 0..CLUSTER_SIZE {
            let data_dir = tempfile::tempdir().expect("cluster node data directory");
            let log_path = data_dir.path().join("test-server.log");
            let child = spawn_cluster_node(
                &binary,
                data_dir.path(),
                &log_path,
                replica_id,
                &tcp_ports,
                &replica_ports,
            );
            nodes.push(ClusterNodeProcess {
                child,
                data_dir,
                log_path,
            });
        }
        let cluster = Self {
            binary,
            nodes,
            tcp_ports,
            replica_ports,
            proxy_port,
            proxy_target,
            proxy_task,
        };
        cluster.wait_for_mesh(None).await;
        cluster
    }

    pub fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.proxy_port)
    }

    pub fn node_endpoint(&self, replica_id: usize) -> String {
        format!("127.0.0.1:{}", self.tcp_ports[replica_id])
    }

    pub fn route_endpoint_to(&self, replica_id: usize) {
        self.proxy_target
            .store(self.tcp_ports[replica_id], Ordering::Release);
    }

    pub fn stop_node(&mut self, replica_id: usize) {
        let node = self.nodes.get_mut(replica_id).expect("cluster node");
        graceful_stop(&mut node.child);
    }

    pub async fn start_node(&mut self, replica_id: usize) {
        let node = self.nodes.get_mut(replica_id).expect("cluster node");
        node.child = spawn_cluster_node(
            &self.binary,
            node.data_dir.path(),
            &node.log_path,
            replica_id,
            &self.tcp_ports,
            &self.replica_ports,
        );
        self.wait_for_mesh(Some(replica_id)).await;
    }

    pub async fn restart_node(&mut self, replica_id: usize) {
        self.stop_node(replica_id);
        self.start_node(replica_id).await;
    }

    async fn wait_for_mesh(&self, node: Option<usize>) {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            let ready = match node {
                Some(replica_id) => {
                    log_contains(&self.nodes[replica_id].log_path, "replica mesh complete")
                }
                None => self
                    .nodes
                    .iter()
                    .all(|process| log_contains(&process.log_path, "replica mesh complete")),
            };
            if ready {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Iggy cluster did not form its replica mesh before the deadline"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TestIggyCluster {
    fn drop(&mut self) {
        self.proxy_task.abort();
        for node in &mut self.nodes {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

async fn spawn_stable_proxy(
    proxy_port: u16,
    target: Arc<AtomicU16>,
) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", proxy_port))
        .await
        .expect("bind stable cluster endpoint");
    tokio::spawn(async move {
        while let Ok((mut incoming, _)) = listener.accept().await {
            let target_port = target.load(Ordering::Acquire);
            tokio::spawn(async move {
                if let Ok(mut upstream) =
                    tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await
                {
                    let _ = tokio::io::copy_bidirectional(&mut incoming, &mut upstream).await;
                }
            });
        }
    })
}

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
            let options = TopicCreateOptions {
                partitions_count: Some(1),
                compression_algorithm: Some(CompressionAlgorithm::default()),
                message_expiry: Some(IggyExpiry::NeverExpire),
                max_topic_size: Some(MaxTopicSize::ServerDefault),
                ..TopicCreateOptions::default()
            };
            client.create_topic(&stream_id, topic, &options).await?;
        }
        let producer = client.producer(&stream, topic)?.build();
        producer.init().await?;
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"ready"))
            .build()?;
        producer.send(vec![message]).await.map(|_| ())
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

fn spawn_cluster_node(
    binary: &Path,
    data_dir: &Path,
    log_path: &Path,
    replica_id: usize,
    tcp_ports: &[u16; CLUSTER_SIZE],
    replica_ports: &[u16; CLUSTER_SIZE],
) -> Child {
    let stdout = File::create(log_path).expect("create cluster node log");
    let stderr = stdout.try_clone().expect("clone cluster node log");
    let mut command = Command::new(binary);
    command
        .arg("--replica-id")
        .arg(replica_id.to_string())
        .env("IGGY_SYSTEM_PATH", data_dir)
        .env("IGGY_CLUSTER_ENABLED", "true")
        .env("IGGY_CLUSTER_NAME", "laser-sdk-rolling-restart")
        .env("IGGY_MESSAGE_BUS_RECONNECT_PERIOD", "100ms")
        .env("IGGY_HTTP_ENABLED", "false")
        .env("IGGY_QUIC_ENABLED", "false")
        .env("IGGY_WEBSOCKET_ENABLED", "false")
        .env("IGGY_ROOT_USERNAME", DEFAULT_ROOT_USERNAME)
        .env("IGGY_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD)
        .env("IGGY_SHARD_RUNTIME_CAPACITY", "256")
        .env("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "0..1")
        .env("IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL", "200 ms")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for node in 0..CLUSTER_SIZE {
        command
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_NAME"),
                format!("node-{node}"),
            )
            .env(format!("IGGY_CLUSTER_NODES_{node}_IP"), "127.0.0.1")
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_REPLICA_ID"),
                node.to_string(),
            )
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_PORTS_TCP"),
                tcp_ports[node].to_string(),
            )
            .env(
                format!("IGGY_CLUSTER_NODES_{node}_PORTS_TCP_REPLICA"),
                replica_ports[node].to_string(),
            );
    }
    command.spawn().expect("spawn Iggy cluster node")
}

fn log_contains(path: &Path, marker: &str) -> bool {
    std::fs::read_to_string(path).is_ok_and(|content| content.contains(marker))
}

fn graceful_stop(child: &mut Child) {
    // SAFETY: `child.id()` is the live process spawned and owned by this harness.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("inspect cluster node").is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.kill().expect("kill cluster node after grace period");
    child.wait().expect("reap cluster node");
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

fn free_host_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("reserve host port"))
        .collect::<Vec<_>>();
    listeners
        .iter()
        .map(|listener| listener.local_addr().expect("reserved address").port())
        .collect()
}
