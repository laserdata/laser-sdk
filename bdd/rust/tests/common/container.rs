use iggy::prelude::*;
use laser_sdk::prelude::Laser;
use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IGGY_IMAGE: &str = "apache/iggy";
const IGGY_DEFAULT_TAG: &str = "edge";
const IGGY_TAG_ENV: &str = "LASER_TEST_IGGY_TAG";
const IGGY_TCP_PORT: u16 = 3000;
const IGGY_HTTP_PORT: u16 = 80;

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct TestIggy {
    _container: ContainerAsync<GenericImage>,
    tcp_port: u16,
}

impl TestIggy {
    async fn start() -> Self {
        let tag = std::env::var(IGGY_TAG_ENV).unwrap_or_else(|_| IGGY_DEFAULT_TAG.to_owned());
        let image = GenericImage::new(IGGY_IMAGE, tag.as_str())
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

        let container = image.start().await.expect("failed to start iggy container");
        let tcp_port = container
            .get_host_port_ipv4(IGGY_TCP_PORT)
            .await
            .expect("failed to get iggy tcp port");
        Self {
            _container: container,
            tcp_port,
        }
    }

    async fn client(&self) -> Result<IggyClient, IggyError> {
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
}

pub struct FreshLaser {
    pub laser: Laser,
    pub iggy: Option<TestIggy>,
}

const ADDR_ENV: &str = "LASER_BDD_ADDR";

/// A fresh `Laser` on a stream unique to the calling scenario. By default each
/// scenario owns one self-managed Apache Iggy testcontainer. When
/// `LASER_BDD_ADDR` is set (the docker-compose path other language runners
/// share), it connects to that already-running server instead.
pub async fn fresh_laser() -> FreshLaser {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stream = format!("bdd_{}_{id}", std::process::id());
    let ops_stream = format!("agdx_{}_{id}", std::process::id());
    let (client, iggy) = connect_client().await;
    let laser = Laser::from_client(client)
        .with_default_stream(stream)
        .with_ops_stream(ops_stream);
    FreshLaser { laser, iggy }
}

async fn connect_client() -> (IggyClient, Option<TestIggy>) {
    let Ok(address) = std::env::var(ADDR_ENV) else {
        let iggy = TestIggy::start().await;
        let client = iggy.client().await.expect("connect to iggy");
        return (client, Some(iggy));
    };
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(address)
        .build()
        .expect("build iggy client");
    client.connect().await.expect("connect to iggy");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login to iggy");
    (client, None)
}
