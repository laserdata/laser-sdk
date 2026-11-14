use crate::harness;
use laser_sdk::prelude::ConnectionEvent;
use std::time::Duration;

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_watched_connection_when_closed_then_should_end_with_shutdown() {
    let laser = harness::laser().await;
    let mut events = laser.connection_events().await;

    laser.close().await.expect("close the connection");

    let mut last = None;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .expect("the events end after the close")
    {
        last = Some(event);
    }
    assert_eq!(last, Some(ConnectionEvent::Shutdown));
}
