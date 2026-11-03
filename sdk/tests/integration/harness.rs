use crate::test_iggy::{TestIggy, TestIggyCluster};
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
use laser_sdk::iggy::prelude::{
    Client, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, HttpMethod, Identifier, IggyClient,
    IggyClientBuilder, IggyError, TopicClient, UserClient,
};
use laser_sdk::prelude::full::*;
#[cfg(feature = "sign")]
use laser_sdk::sign::KeyRegistry;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tokio::time::{Instant, sleep};

const CLUSTER_ENV: &str = "LASER_TEST_IGGY_CLUSTER";
// The server gauge of parked requests: deferred polls, deferred fetches and
// filtered waits, one series per shard.
const HELD_REQUESTS_METRIC: &str = "deferred_poll_pending";
// One JWT key on every node, so a token one node issues reads the metrics of
// any node.
const SHARED_JWT_SECRET: &str = "laser-sdk-integration-shared-jwt-secret-0123456789";

static IGGY: OnceCell<SharedIggy> = OnceCell::const_new();
static BOOTSTRAP: AsyncMutex<()> = AsyncMutex::const_new(());
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The server a suite shares: one node, or a three-node cluster reached
/// through its metadata leader when `LASER_TEST_IGGY_CLUSTER` is set.
pub enum SharedIggy {
    Node(TestIggy),
    Cluster {
        cluster: TestIggyCluster,
        leader: usize,
    },
}

impl SharedIggy {
    /// Start the server, node `replica_id` with the `IGGY_*` settings
    /// `env(replica_id)` returns. A single node takes those of node 0.
    pub async fn start(env: impl Fn(usize) -> Vec<(String, String)>) -> Self {
        if std::env::var_os(CLUSTER_ENV).is_none() {
            return Self::Node(TestIggy::start_with(env(0)).await);
        }
        let cluster = TestIggyCluster::start_with(env).await;
        let leader = cluster.leader(0).await;
        Self::Cluster { cluster, leader }
    }

    pub fn connection_string(&self) -> String {
        match self {
            Self::Node(node) => node.connection_string(),
            Self::Cluster { cluster, leader } => format!(
                "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{}",
                cluster.node_endpoint(*leader)
            ),
        }
    }

    pub async fn client(&self) -> Result<IggyClient, IggyError> {
        let client =
            IggyClientBuilder::from_connection_string(&self.connection_string())?.build()?;
        client.connect().await?;
        Ok(client)
    }
}

async fn iggy() -> &'static SharedIggy {
    IGGY.get_or_init(|| SharedIggy::start(|_| Vec::new())).await
}

pub async fn client() -> IggyClient {
    iggy().await.client().await.expect("connect test client")
}

/// A freshly bootstrapped `Laser` on a data stream + ops stream unique to this
/// test, so the one shared Iggy instance stays isolated across the whole suite.
pub async fn laser() -> Laser {
    let _bootstrap = BOOTSTRAP.lock().await;
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stream = format!("it_{}_{id}", std::process::id());
    let ops_stream = format!("ld_{}_{id}", std::process::id());
    let laser = Laser::from_client(iggy().await.client().await.expect("connect"))
        .with_default_stream(stream)
        .with_ops_stream(ops_stream);
    laser
        .bootstrap(
            4,
            laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(86_400)),
        )
        .await
        .expect("bootstrap");
    laser
}

/// A `Laser` connected through a connection string, on a data stream unique to
/// this test. The connect-time probe runs, and dedicated data connections can
/// open with the same credentials.
pub async fn connected_laser() -> Laser {
    connected_laser_at(&iggy().await.connection_string()).await
}

pub async fn connected_laser_on(server: &TestIggy) -> Laser {
    connected_laser_at(&server.connection_string()).await
}

pub async fn connected_laser_at(connection_string: &str) -> Laser {
    let _bootstrap = BOOTSTRAP.lock().await;
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stream = format!("it_{}_{id}", std::process::id());
    let laser = Laser::connect(connection_string)
        .await
        .expect("connect")
        .with_default_stream(stream);
    laser
        .bootstrap(
            1,
            laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(86_400)),
        )
        .await
        .expect("bootstrap");
    laser
}

/// A `Laser` on a three-node cluster through its stable endpoint. A short
/// publish attempt timeout keeps every stall inside the SDK's own recovery.
pub async fn cluster_laser(cluster: &TestIggyCluster, stream: &str) -> Laser {
    Laser::builder()
        .connection_string(cluster.connection_string())
        .stream(stream)
        .publish_timeout(Duration::from_secs(5))
        .build()
        .await
        .expect("connect to the cluster")
}

/// The replica that leads each partition of `topic` on the default stream,
/// by partition id, read again while a view change leaves a route unknown.
pub async fn partition_primaries(laser: &Laser, topic: &str, partitions: u32) -> Vec<u8> {
    let stream_id = identifier_to_wire(
        &Identifier::named(
            laser
                .default_stream()
                .expect("the test laser names its stream"),
        )
        .expect("stream id"),
    )
    .expect("wire stream id");
    let topic_id =
        identifier_to_wire(&Identifier::named(topic).expect("topic id")).expect("wire topic id");
    let mut primaries = Vec::new();
    for partition_id in 0..partitions {
        let request = GetPartitionRouteRequest {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
        };
        let primary = eventually(|| async {
            let reply = laser
                .client()
                .send_binary_request(GET_PARTITION_ROUTE_CODE, request.to_bytes())
                .await
                .ok()?;
            PartitionRouteResponse::decode_from(&reply)
                .ok()
                .map(|route| route.primary_replica)
        })
        .await;
        primaries.push(primary);
    }
    primaries
}

/// The replica that leads each partition of `topic`, by partition id, once more
/// than one node leads one. Creation can lead every partition on the one node
/// counted ready, and the cluster moves primaries one at a time afterwards.
pub async fn spread_primaries(laser: &Laser, topic: &str, partitions: u32) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let primaries = partition_primaries(laser, topic, partitions).await;
        if primaries.iter().collect::<BTreeSet<_>>().len() > 1 {
            return primaries;
        }
        assert!(
            Instant::now() < deadline,
            "the primaries never spread: {primaries:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
}

/// The replica that leads each partition of `topic`, by partition id, once no
/// node leads two more partitions than another. The cluster balances the
/// partitions of every topic together, so only the one topic of its cluster
/// settles this way.
pub async fn settled_primaries(
    laser: &Laser,
    topic: &str,
    partitions: u32,
    nodes: usize,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let primaries = partition_primaries(laser, topic, partitions).await;
        let led: Vec<usize> = (0..nodes)
            .map(|node| {
                primaries
                    .iter()
                    .filter(|primary| usize::from(**primary) == node)
                    .count()
            })
            .collect();
        let most = led.iter().copied().max().unwrap_or(0);
        let fewest = led.iter().copied().min().unwrap_or(0);
        if most - fewest <= 1 {
            return primaries;
        }
        assert!(
            Instant::now() < deadline,
            "the primaries never settled: {primaries:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
}

/// The replicas that store each partition of `topic` on the default stream,
/// by partition id, as the server reports them.
pub async fn partition_members(laser: &Laser, topic: &str) -> BTreeMap<u32, Vec<u8>> {
    let stream = Identifier::named(
        laser
            .default_stream()
            .expect("the test laser names its stream"),
    )
    .expect("stream id");
    let topic = Identifier::named(topic).expect("topic id");
    eventually(|| async {
        let details = laser.client().get_topic(&stream, &topic).await.ok()??;
        Some(
            details
                .partitions
                .into_iter()
                .map(|partition| (partition.id, partition.members))
                .collect(),
        )
    })
    .await
}

pub async fn reconnect(existing: &Laser) -> Laser {
    let stream = existing
        .default_stream()
        .expect("the test laser names its stream");
    let ops_stream = existing.ops_stream().to_owned();
    Laser::from_client(iggy().await.client().await.expect("reconnect"))
        .with_default_stream(stream.to_owned())
        .with_ops_stream(ops_stream)
}

#[cfg(feature = "sign")]
pub async fn verified(existing: &Laser, verifier: std::sync::Arc<KeyRegistry>) -> Laser {
    let stream = existing
        .default_stream()
        .expect("the test laser names its stream")
        .to_owned();
    let client = iggy().await.client().await.expect("verified client");
    Laser::builder()
        .client(client)
        .stream(stream)
        .capabilities(Capabilities::OPEN)
        .verifier(verifier)
        .build()
        .await
        .expect("verified laser builds")
        .with_ops_stream(existing.ops_stream().to_owned())
}

/// A free local address for a server's HTTP API.
pub fn free_http_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve an HTTP port");
    listener
        .local_addr()
        .expect("reserved HTTP address")
        .to_string()
}

/// `IGGY_*` settings that serve the HTTP API, and with it the metrics, on
/// `address`.
pub fn http_settings(address: &str) -> Vec<(String, String)> {
    [
        ("IGGY_HTTP_ENABLED", "true"),
        ("IGGY_HTTP_ADDRESS", address),
        ("IGGY_HTTP_JWT_ENCODING_SECRET", SHARED_JWT_SECRET),
        ("IGGY_HTTP_JWT_DECODING_SECRET", SHARED_JWT_SECRET),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

/// [`http_settings`] for node `node` of a cluster whose nodes serve HTTP on
/// `addresses`, with every node's HTTP port in the roster as cluster mode
/// requires.
pub fn cluster_http_settings(addresses: &[String], node: usize) -> Vec<(String, String)> {
    let mut settings = http_settings(&addresses[node]);
    for (replica_id, address) in addresses.iter().enumerate() {
        let port = address
            .rsplit_once(':')
            .map(|(_, port)| port.to_owned())
            .expect("an address with a port");
        settings.push((format!("IGGY_CLUSTER_NODES_{replica_id}_PORTS_HTTP"), port));
    }
    settings
}

/// The requests the server whose HTTP API listens on `http_address` holds
/// parked now, summed over its shards. `None` while it cannot be read.
pub async fn held_requests(http_address: &str) -> Option<u64> {
    let client = IggyClientBuilder::from_connection_string(&format!(
        "iggy+http://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{http_address}"
    ))
    .ok()?
    .build()
    .ok()?;
    client.connect().await.ok()?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .ok()?;
    let exposition = client
        .send_http_request(HttpMethod::Get, "/metrics", None)
        .await
        .ok()?;
    String::from_utf8_lossy(&exposition)
        .lines()
        .filter(|line| line.starts_with(HELD_REQUESTS_METRIC))
        .map(|line| {
            line.rsplit_once(' ')
                .and_then(|(_, value)| value.parse::<f64>().ok())
        })
        .sum::<Option<f64>>()
        .map(|held| held as u64)
}

/// Poll the held requests at `http_address` until `done` accepts them or
/// `within` passes, and return the last count with the time it took.
pub async fn held_requests_until(
    http_address: &str,
    within: Duration,
    done: impl Fn(u64) -> bool,
) -> (Option<u64>, Duration) {
    let started = Instant::now();
    loop {
        let held = held_requests(http_address).await;
        if held.is_some_and(&done) || started.elapsed() >= within {
            return (held, started.elapsed());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn eventually<F, Fut, T>(mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(Instant::now() < deadline, "condition not met within 15s");
        sleep(Duration::from_millis(200)).await;
    }
}
