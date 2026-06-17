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
    pub async fn start() -> Self {
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
            container,
            tcp_port,
        }
    }

    pub fn container_id(&self) -> &str {
        self.container.id()
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
        Ok(Laser::from_client(self.client().await?).with_stream(stream))
    }
}
