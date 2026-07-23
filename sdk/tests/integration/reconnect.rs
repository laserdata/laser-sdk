use crate::iggy_container::TestIggy;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

// The SDK claims resilience through iggy-rs auto-reconnect. This is the
// demonstration: a publish warms the producer cache, the server restarts under
// the same mapped port (every connection dies), and the SAME `Laser` publishes
// and consumes again. What it pins: a cached producer whose connection died
// must never permanently poison its cell. Recovery may take retries while the
// client reconnects, permanent failure is the bug. A dedicated container, not
// the shared harness one, so the restart cannot disturb concurrent tests.
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
            Ok(Ok(())) => break,
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
