use crate::test_iggy::{TestIggy, TestIggyCluster};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_three_node_cluster_when_follower_and_leader_restart_then_same_sdk_handle_should_continue_streaming()
 {
    use iggy::prelude::{
        Client, ClusterClient, ClusterNodeRole, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME,
        IggyClientBuilder, UserClient,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let mut cluster = TestIggyCluster::start().await;
    let discovery = IggyClientBuilder::from_connection_string(&format!(
        "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{}",
        cluster.node_endpoint(0),
    ))
    .expect("connection string")
    .build()
    .expect("discovery client");
    discovery.connect().await.expect("discovery connect");
    discovery
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login");
    let metadata = discovery
        .get_cluster_metadata()
        .await
        .expect("cluster metadata");
    let leader = metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .and_then(|node| node.name.strip_prefix("node-"))
        .and_then(|value| value.parse::<usize>().ok())
        .expect("configured leader");
    let follower = (0..3).find(|node| *node != leader).expect("follower");
    discovery.disconnect().await.expect("discovery disconnect");
    cluster.route_endpoint_to(leader);
    let connection = format!(
        "iggy+tcp://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{}?reconnection_retries=unlimited&reconnection_interval=100ms",
        cluster.endpoint(),
    );
    let laser = Arc::new(
        laser_sdk::prelude::Laser::connect_with_stream(&connection, "rolling_restart")
            .await
            .expect("connect"),
    );
    laser
        .stream("rolling_restart")
        .ensure()
        .await
        .expect("stream");
    laser.topic("pulse").ensure(1).await.expect("topic");

    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let observed = Arc::new(AtomicU64::new(0));
    let worker = {
        let laser = laser.clone();
        let stop = stop.clone();
        let sent = sent.clone();
        let observed = observed.clone();
        tokio::spawn(async move {
            let topic = laser.topic("pulse");
            while !stop.load(Ordering::Acquire) {
                let sequence = sent.load(Ordering::Acquire) + 1;
                let payload = sequence.to_le_bytes();
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    topic.send(&payload[..], BTreeMap::new(), None),
                )
                .await
                {
                    Ok(Ok(_)) => sent.store(sequence, Ordering::Release),
                    Ok(Err(error)) => eprintln!("rolling publish failed: {error:?}"),
                    Err(_) => eprintln!("rolling publish timed out"),
                }
                if let Ok(mut cursor) = topic.replay()
                    && let Ok(Ok(messages)) =
                        tokio::time::timeout(Duration::from_secs(2), cursor.poll()).await
                {
                    observed.fetch_add(messages.len() as u64, Ordering::AcqRel);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    };

    wait_for_progress(&sent, 1, "initial streaming").await;
    cluster.restart_node(follower).await;
    let after_follower = sent.load(Ordering::Acquire);
    wait_for_progress(&sent, after_follower + 1, "follower restart").await;
    cluster.restart_node(leader).await;
    let after_leader = sent.load(Ordering::Acquire);
    wait_for_progress(&sent, after_leader + 1, "leader restart").await;

    stop.store(true, Ordering::Release);
    worker.await.expect("streaming worker");
    assert!(
        observed.load(Ordering::Acquire) > 0,
        "the same handle consumed records"
    );
}

async fn wait_for_progress(counter: &std::sync::atomic::AtomicU64, expected: u64, phase: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while counter.load(std::sync::atomic::Ordering::Acquire) < expected {
        assert!(
            Instant::now() < deadline,
            "streaming did not resume after {phase}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// The SDK claims resilience through Iggy SDK auto-reconnect. This is the
// demonstration: a publish warms the producer cache, the server restarts under
// the same mapped port (every connection dies), and the SAME `Laser` publishes
// and consumes again. What it pins: a cached producer whose connection died
// must never permanently poison its cell. Recovery may take retries while the
// client reconnects, permanent failure is the bug. A dedicated process, not
// the shared harness one, keeps the restart from disturbing concurrent tests.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_server_restart_when_reusing_the_same_client_then_should_publish_and_consume_again()
{
    let iggy = TestIggy::start_pinned().await;
    let laser = iggy
        .laser_reconnecting("reconnect_it")
        .await
        .expect("connect");
    laser
        .stream("reconnect_it")
        .ensure()
        .await
        .expect("stream exists");
    let topic = laser.topic("pulse");
    topic.ensure(1).await.expect("topic exists");
    topic
        .send(&b"before-restart"[..], BTreeMap::new(), None)
        .await
        .expect("the warm-up publish succeeds");

    iggy.restart().await;

    // The restart is a hard stop with no fsync, so the server comes back empty:
    // the pre-restart topology and message are gone. Recovery re-creates the
    // topology and publishes through the SAME client. What this pins is that the
    // cached producer whose connection died is not permanently poisoned: once
    // the client reconnects, ensure and publish succeed again. Each attempt is
    // bounded so a producer that blocks inside a reconnect cannot hang the loop.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let recovered = tokio::time::timeout(Duration::from_secs(3), async {
            laser.stream("reconnect_it").ensure().await?;
            topic.ensure(1).await?;
            topic
                .send(&b"after-restart"[..], BTreeMap::new(), None)
                .await
        })
        .await;
        match recovered {
            Ok(Ok(_)) => break,
            Ok(Err(error)) => {
                assert!(
                    Instant::now() < deadline,
                    "the cached producer never recovered from the restart: {error}"
                );
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(_) => assert!(
                Instant::now() < deadline,
                "the cached producer never recovered from the restart (send hung)"
            ),
        }
    }

    let mut cursor = topic.replay().expect("reader opens");
    let payloads: Vec<Vec<u8>> = cursor
        .poll()
        .await
        .expect("replay after the restart succeeds")
        .into_iter()
        .map(|message| message.payload)
        .collect();
    assert!(
        payloads.contains(&b"after-restart".to_vec()),
        "the post-restart publish reads back through the same client"
    );
}
