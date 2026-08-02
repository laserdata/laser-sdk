use crate::test_iggy::TestIggy;
use laser_sdk::iggy::prelude::IggyClient;
use laser_sdk::prelude::full::*;
#[cfg(feature = "sign")]
use laser_sdk::sign::KeyRegistry;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tokio::time::{Instant, sleep};

static IGGY: OnceCell<TestIggy> = OnceCell::const_new();
static BOOTSTRAP: AsyncMutex<()> = AsyncMutex::const_new(());
static COUNTER: AtomicU64 = AtomicU64::new(0);

async fn iggy() -> &'static TestIggy {
    IGGY.get_or_init(|| async { TestIggy::start().await }).await
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
    let laser = iggy()
        .await
        .laser(stream)
        .await
        .expect("connect")
        .with_ops_stream(ops_stream);
    laser.bootstrap(4).await.expect("bootstrap");
    laser
}

pub async fn reconnect(existing: &Laser) -> Laser {
    let stream = existing
        .default_stream()
        .expect("the test laser names its stream");
    let ops_stream = existing.ops_stream().to_owned();
    iggy()
        .await
        .laser(stream.to_owned())
        .await
        .expect("reconnect")
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
