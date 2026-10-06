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
const URL_ENV: &str = "LASER_BDD_URL";

// An already-running server: `LASER_BDD_URL` (a connection string with its own
// credentials, as the Python and TypeScript runners accept) wins over
// `LASER_BDD_ADDR` (host:port with the default root credentials). The scheme is
// optional, as in every SDK.
fn external() -> Option<String> {
    if let Ok(url) = std::env::var(URL_ENV) {
        let url = url.trim();
        if url.starts_with("iggy://") || url.starts_with("iggy+") {
            return Some(url.to_owned());
        }
        return Some(format!("iggy://{url}"));
    }
    let address = std::env::var(ADDR_ENV).ok()?;
    Some(format!(
        "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{address}"
    ))
}

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

/// A `Laser` connected through a connection string on a fresh stream, so the
/// capability probe runs and partition-primary data connections can open.
pub async fn fresh_connected_laser() -> FreshLaser {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let stream = format!("bdd_{}_{id}", std::process::id());
    let (connection_string, iggy) = match external() {
        Some(connection_string) => (connection_string, None),
        None => {
            let iggy = Arc::clone(
                IGGY.get_or_init(|| async { Arc::new(TestIggy::start().await) })
                    .await,
            );
            (iggy.connection_string(), Some(iggy))
        }
    };
    let laser = Laser::connect(&connection_string)
        .await
        .expect("connect to Iggy")
        .with_default_stream(stream);
    FreshLaser { laser, iggy }
}

async fn connect_client() -> (IggyClient, Option<Arc<TestIggy>>) {
    let Some(connection_string) = external() else {
        let iggy = Arc::clone(
            IGGY.get_or_init(|| async { Arc::new(TestIggy::start().await) })
                .await,
        );
        let client = iggy.client().await.expect("connect to Iggy");
        return (client, Some(iggy));
    };
    // The connection string carries the transport, its options, and the
    // credentials, which the client logs in with on connect.
    let client = IggyClientBuilder::from_connection_string(&connection_string)
        .expect("parse the Iggy connection string")
        .build()
        .expect("build Iggy client");
    client.connect().await.expect("connect to Iggy");
    (client, None)
}
