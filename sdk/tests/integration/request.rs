use crate::harness;
use crate::test_iggy::{TestIggy, TestIggyCluster};
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::topics::AGENT_SESSIONS;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

const CLUSTER_PARTITIONS: u32 = 3;
const CLUSTER_REQUESTS: usize = 12;
// The reply hub holds its read this long, so a reply slower than one hold
// waited out a hold on another node.
const REPLY_HOLD: Duration = Duration::from_secs(5);
const ANSWER_DELAY: Duration = Duration::from_millis(50);
const RECOVERY: Duration = Duration::from_secs(90);
const FIVE_NODES: usize = 5;

struct ToolRunner;

struct DelayedRunner;

struct SameTopicRunner;

impl AgentHandler for ToolRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let output = Bytes::from(format!(
            "result: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.reply_on(AgentTopic::Sessions, output).await
    }
}

impl AgentHandler for DelayedRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        tokio::time::sleep(ANSWER_DELAY).await;
        let output = Bytes::from(format!(
            "result: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.reply_on(AgentTopic::Sessions, output).await
    }
}

impl AgentHandler for SameTopicRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.payload == b"search" {
            ctx.reply_on(
                AgentTopic::Sessions,
                Bytes::from_static(b"same-topic-result"),
            )
            .await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_tool_runner_when_requesting_then_should_await_the_correlated_reply() {
    let laser = harness::laser().await;
    let _agent_lifetime_1 = Agent::builder()
        .id("tool".parse().expect("tool is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(ToolRunner)
        .build()
        .spawn(laser.clone());

    let correlation = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"search"),
            &correlation,
            Duration::from_secs(10),
        )
        .await
        .expect("the tool result should arrive before the timeout");

    assert_eq!(reply.payload.as_slice(), b"result: search");
    assert_eq!(
        reply
            .provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str(),
        "tool"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_one_topic_for_request_and_reply_then_should_skip_the_request_address() {
    let laser = harness::laser().await;
    let _agent_lifetime = Agent::builder()
        .id("same-topic".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(SameTopicRunner)
        .build()
        .spawn(laser.clone());
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"search"),
            &provenance,
            Duration::from_secs(10),
        )
        .await
        .expect("same-topic reply arrives");
    assert_eq!(reply.payload.as_slice(), b"same-topic-result");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_no_responder_when_requesting_then_should_time_out() {
    let laser = harness::laser().await;
    let correlation = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let result = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"unanswered"),
            &correlation,
            Duration::from_millis(300),
        )
        .await;
    assert!(matches!(result, Err(LaserError::Timeout("reply"))));
}

struct MisaddressedFirst;

impl AgentHandler for MisaddressedFirst {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.payload != b"search" {
            return Ok(());
        }
        let misaddressed = Provenance::builder()
            .conversation_id(message.provenance.conversation_id)
            .maybe_correlation_id(message.provenance.correlation_id.clone())
            .agent("tool".parse().expect("valid agent id"))
            .target_agent_id("bystander".parse().expect("valid agent id"))
            .build();
        ctx.send(
            AgentTopic::Sessions,
            Bytes::from_static(b"not-yours"),
            &misaddressed,
        )
        .await?;
        ctx.reply_on(AgentTopic::Sessions, Bytes::from_static(b"yours"))
            .await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_correlated_reply_addressed_to_another_agent_when_requesting_then_should_wait_for_the_requester_reply()
 {
    let laser = harness::laser().await;
    let _agent_lifetime = Agent::builder()
        .id("tool".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(MisaddressedFirst)
        .build()
        .spawn(laser.clone());
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .agent("planner".parse().expect("valid agent id"))
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"search"),
            &provenance,
            Duration::from_secs(10),
        )
        .await
        .expect("the requester's reply arrives");
    assert_eq!(reply.payload.as_slice(), b"yours");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_reply_partitions_led_by_different_nodes_on_a_cluster_when_answers_land_then_the_reply_hub_should_complete_each_request_without_waiting_out_its_hold_also_after_a_node_restart()
 {
    let mut cluster = TestIggyCluster::start().await;
    replies_across_nodes(&mut cluster, "cluster_replies").await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_reply_partitions_stored_on_three_of_five_nodes_on_a_cluster_when_answers_land_then_the_reply_hub_should_read_them_from_their_members_also_after_a_member_restart()
 {
    let mut cluster = TestIggyCluster::start_sized(FIVE_NODES, |_| {
        vec![(
            "IGGY_CLUSTER_DEFAULT_REPLICATION_FACTOR".to_owned(),
            "3".to_owned(),
        )]
    })
    .await;
    replies_across_nodes(&mut cluster, "cluster_replies_rf3").await;
}

#[tokio::test]
async fn given_a_reply_hub_whose_last_waiter_leaves_during_a_held_read_when_idle_then_should_hold_no_request_on_the_server()
 {
    let http_address = harness::free_http_address();
    let server = TestIggy::start_with(harness::http_settings(&http_address)).await;
    let laser = harness::connected_laser_on(&server).await;
    let (held, _) =
        harness::held_requests_until(&http_address, Duration::from_secs(15), |_| true).await;
    assert_eq!(held, Some(0), "nothing is held before the first request");

    let requester = laser.clone();
    let unanswered = tokio::spawn(async move {
        let provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .build();
        requester
            .request(
                AgentTopic::Sessions,
                AgentTopic::Sessions,
                Bytes::from_static(b"search"),
                &provenance,
                Duration::from_secs(60),
            )
            .await
    });
    let (held, _) =
        harness::held_requests_until(&http_address, Duration::from_secs(15), |held| held >= 1)
            .await;
    assert_eq!(held, Some(1), "the waiting hub holds one read on the node");
    let held_since = Instant::now();

    unanswered.abort();
    let (held, released_after) =
        harness::held_requests_until(&http_address, Duration::from_secs(10), |held| held == 0)
            .await;
    let held_for = held_since.elapsed();
    eprintln!("reply hub: held read released {released_after:?} after the last waiter left");
    assert_eq!(held, Some(0), "an idle hub holds nothing");
    // The release must come well before the hold seen on the server would end.
    assert!(
        held_for < REPLY_HOLD / 2,
        "the held read ended {held_for:?} after the server reported it, it waited out its hold"
    );
}

// Requests whose replies land on partitions of different primaries complete
// promptly, also while and after a node that leads one of them restarts.
async fn replies_across_nodes(cluster: &mut TestIggyCluster, stream: &str) {
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let laser = harness::cluster_laser(cluster, stream).await;
    laser
        .bootstrap(
            CLUSTER_PARTITIONS,
            laser_sdk::agent::TopicRetention::expire_after(Duration::from_secs(86_400)),
        )
        .await
        .expect("bootstrap");
    let primaries = harness::partition_primaries(&laser, AGENT_SESSIONS, CLUSTER_PARTITIONS).await;
    assert!(
        primaries.iter().collect::<BTreeSet<_>>().len() > 1,
        "the reply partitions are led by different nodes: {primaries:?}"
    );
    let mut tool = Agent::builder()
        .id("tool".parse().expect("tool is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .max_wait(Duration::from_secs(20))
        .concurrency(ConcurrencyPolicy::SerialPerPartition {
            max_partitions: CLUSTER_PARTITIONS as usize,
        })
        .handler(DelayedRunner)
        .build()
        .spawn(laser.clone());
    tool.ready().await.expect("the tool is ready");

    let answered = request_round(&laser, REPLY_HOLD).await;
    let nodes: BTreeSet<u8> = answered
        .iter()
        .map(|partition| primaries[*partition as usize])
        .collect();
    assert!(
        nodes.len() > 1,
        "the replies came from partitions of different primaries: {answered:?} over {primaries:?}"
    );

    // Requests stay in flight across the restart, so the hub's held reads do too.
    let restarted = (0..cluster.size())
        .filter(|node| *node != leader)
        .find(|node| {
            primaries
                .iter()
                .any(|primary| usize::from(*primary) == *node)
        })
        .unwrap_or_else(|| (leader + 1) % cluster.size());
    let in_flight = {
        let laser = laser.clone();
        tokio::spawn(async move {
            futures::future::join_all((0..CLUSTER_REQUESTS).map(|index| {
                let laser = laser.clone();
                async move { request(&laser, index, RECOVERY).await.is_ok() }
            }))
            .await
        })
    };
    cluster.restart_node(restarted).await;
    let survived = in_flight.await.expect("the in-flight requests end");
    eprintln!(
        "cluster reply hub: {} of {} requests in flight across the restart were answered",
        survived.iter().filter(|answered| **answered).count(),
        survived.len()
    );
    // The cluster serves again within its recovery bound, then every reply is
    // prompt again.
    request(&laser, 0, RECOVERY)
        .await
        .expect("a request after the restart is answered");
    request_round(&laser, REPLY_HOLD).await;
    tool.shutdown().await.expect("the tool stops");
}

// Send `CLUSTER_REQUESTS` requests at once, each in its own session so the
// replies spread over the partitions, and return the partition of each reply.
// Every reply arrives within `bound` of its request.
async fn request_round(laser: &Laser, bound: Duration) -> Vec<u32> {
    let replies = futures::future::join_all(
        (0..CLUSTER_REQUESTS).map(|index| request(laser, index, Duration::from_secs(30))),
    )
    .await;
    replies
        .into_iter()
        .map(|reply| {
            let (partition, elapsed) = reply.expect("every request is answered");
            assert!(
                elapsed < bound,
                "a reply on partition {partition} took {elapsed:?}, the hub waited out a hold"
            );
            partition
        })
        .collect()
}

async fn request(
    laser: &Laser,
    index: usize,
    timeout: Duration,
) -> Result<(u32, Duration), LaserError> {
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let started = Instant::now();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from(format!("search-{index}")),
            &provenance,
            timeout,
        )
        .await?;
    assert_eq!(
        reply.payload,
        format!("result: search-{index}").into_bytes(),
        "the reply answers its own request"
    );
    Ok((reply.id.partition_id, started.elapsed()))
}
