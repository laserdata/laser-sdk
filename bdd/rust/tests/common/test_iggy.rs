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

struct External {
    connection_string: String,
    address: String,
    username: String,
    password: String,
}

// An already-running server: `LASER_BDD_URL` (a connection string with its own
// credentials, as the Python and TypeScript runners accept) wins over
// `LASER_BDD_ADDR` (host:port with the default root credentials).
fn external() -> Option<External> {
    if let Ok(url) = std::env::var(URL_ENV) {
        let rest = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
        let (credentials, authority) = rest
            .rsplit_once('@')
            .map_or((None, rest), |(credentials, authority)| {
                (Some(credentials), authority)
            });
        // The address ends where a path or the connection options begin.
        let address = authority.split(['/', '?']).next().unwrap_or(authority);
        let (username, password) = credentials
            .and_then(|credentials| credentials.split_once(':'))
            .unwrap_or((DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD));
        return Some(External {
            connection_string: url.clone(),
            address: address.to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
        });
    }
    let address = std::env::var(ADDR_ENV).ok()?;
    Some(External {
        connection_string: format!(
            "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{address}"
        ),
        address,
        username: DEFAULT_ROOT_USERNAME.to_owned(),
        password: DEFAULT_ROOT_PASSWORD.to_owned(),
    })
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
        Some(external) => (external.connection_string, None),
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
    let Some(external) = external() else {
        let iggy = Arc::clone(
            IGGY.get_or_init(|| async { Arc::new(TestIggy::start().await) })
                .await,
        );
        let client = iggy.client().await.expect("connect to Iggy");
        return (client, Some(iggy));
    };
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(external.address)
        .build()
        .expect("build Iggy client");
    client.connect().await.expect("connect to Iggy");
    client
        .login_user(&external.username, &external.password)
        .await
        .expect("login to Iggy");
    (client, None)
}
