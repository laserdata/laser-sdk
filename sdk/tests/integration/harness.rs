use crate::iggy_container::TestIggy;
use laser_sdk::prelude::*;
use std::future::Future;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::{Instant, sleep};

static IGGY: OnceCell<TestIggy> = OnceCell::const_new();
static CONTAINER_IDS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static COUNTER: AtomicU64 = AtomicU64::new(0);

#[dtor::dtor]
unsafe fn cleanup_containers() {
    let ids = CONTAINER_IDS
        .lock()
        .expect("container-id registry lock is not poisoned");
    for id in ids.iter() {
        let _ = Command::new("docker").args(["rm", "-f", "-v", id]).output();
    }
}

async fn iggy() -> &'static TestIggy {
    IGGY.get_or_init(|| async {
        let iggy = TestIggy::start().await;
        CONTAINER_IDS
            .lock()
            .expect("container-id registry lock is not poisoned")
            .push(iggy.container_id().to_string());
        iggy
    })
    .await
}

/// A freshly bootstrapped `Laser` on a data stream + ops stream unique to this
/// test, so the one shared Apache Iggy instance stays isolated across the whole
/// suite. The ops stream override mirrors production's separate `_agdx` stream
/// while keeping each test's query/control surface from colliding with every
/// other test's worker on the shared Apache Iggy instance.
pub async fn laser() -> Laser {
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

/// Polls `f` until it yields `Some`, or panics after 15s. Integration timing is
/// inherently eventual, and this keeps assertions reliable without fixed sleeps.
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
