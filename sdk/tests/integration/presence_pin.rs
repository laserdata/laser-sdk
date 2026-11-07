// Presence stays on the replica that received it. A client whose managed
// requests move elsewhere clears and re-advertises on that replica, and
// reports the replica unavailable while it does not answer.

use crate::harness;
use crate::test_iggy::TestIggyCluster;
use laser_sdk::prelude::full::Laser;
use laser_sdk::wire::agent::AgentPresence;

fn presence() -> AgentPresence {
    AgentPresence {
        v: 1,
        agent: "risk".parse().expect("risk is a valid agent id"),
        inbox: Some("risk.inbox".to_owned()),
    }
}

#[tokio::test]
async fn given_a_presence_pinned_to_a_node_when_that_node_is_down_then_should_report_unavailable_keep_the_pin_and_clear_once_it_returns()
 {
    let mut cluster = TestIggyCluster::start().await;
    let laser = Laser::connect(&cluster.connection_string())
        .await
        .expect("the client connects");
    laser
        .advertise_presence(&presence())
        .await
        .expect("the first advertise pins the node that holds it");

    // The pinned node is whichever node a clear cannot reach once it is down.
    let mut pinned = None;
    for node in 0..3 {
        cluster.kill_node(node);
        match laser.clear_presence().await {
            Err(error) if error.is_unavailable() => {
                pinned = Some(node);
                break;
            }
            Ok(()) => {
                cluster.start_node(node).await;
                laser
                    .advertise_presence(&presence())
                    .await
                    .expect("the presence is advertised again once the node returns");
            }
            Err(error) => panic!("a clear with the pinned node up succeeds: {error}"),
        }
    }
    let pinned = pinned.expect("one of the nodes holds the presence");

    let again = laser.advertise_presence(&presence()).await;
    assert!(
        again
            .as_ref()
            .is_err_and(laser_sdk::LaserError::is_unavailable),
        "the presence is never advertised on another node while its node is down: {again:?}"
    );

    cluster.start_node(pinned).await;
    harness::eventually(|| async { laser.clear_presence().await.ok() }).await;
    laser
        .advertise_presence(&presence())
        .await
        .expect("a fresh advertise pins a node again");
    laser
        .clear_presence()
        .await
        .expect("the fresh presence clears");
    laser.close().await.expect("the client closes");
}
