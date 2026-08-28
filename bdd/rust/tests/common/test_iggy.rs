use iggy::prelude::*;
use laser_sdk::prelude::Laser;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::OnceCell;

#[path = "../../../../sdk/tests/support/test_iggy.rs"]
#[allow(dead_code)]
mod server;
pub use server::TestIggy;

static COUNTER: AtomicU64 = AtomicU64::new(0);
static IGGY: OnceCell<Arc<TestIggy>> = OnceCell::const_new();

pub struct FreshLaser {
    pub laser: Laser,
    pub iggy: Option<Arc<TestIggy>>,
}

const ADDR_ENV: &str = "LASER_BDD_ADDR";

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

async fn connect_client() -> (IggyClient, Option<Arc<TestIggy>>) {
    let Ok(address) = std::env::var(ADDR_ENV) else {
        let iggy = Arc::clone(
            IGGY.get_or_init(|| async { Arc::new(TestIggy::start().await) })
                .await,
        );
        let client = iggy.client().await.expect("connect to Iggy");
        return (client, Some(iggy));
    };
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(address)
        .build()
        .expect("build Iggy client");
    client.connect().await.expect("connect to Iggy");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login to Iggy");
    (client, None)
}
