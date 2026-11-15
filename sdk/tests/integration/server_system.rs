use crate::harness;
use iggy::prelude::IggyError;
use laser_sdk::LaserError;
use laser_sdk::server::{
    CreateUserOptions, MetadataChangeGroup, MetadataChangeKind, MetadataChangeStart,
    MetadataChangeStatus, MetadataChangesQuery, OptionsScope,
};

#[tokio::test]
async fn given_a_server_when_topic_options_are_described_then_should_list_each_option_with_its_rules()
 {
    let laser = harness::connected_laser().await;
    let options = laser
        .describe_options(OptionsScope::Topic)
        .await
        .expect("topic options are described");
    let segment_size = options
        .iter()
        .find(|option| option.key == "segment_size")
        .expect("topics take a segment size");
    assert_eq!(segment_size.kind, "uint64");
    assert!(segment_size.default_value.is_some());
    assert!(!segment_size.description.is_empty());
}

#[tokio::test]
async fn given_a_created_stream_when_metadata_changes_are_read_then_should_report_its_creation() {
    let laser = harness::connected_laser().await;
    let latest = laser
        .metadata_changes(MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("the latest cursor reads");
    assert_eq!(latest.status, MetadataChangeStatus::Ok);
    let cursor = latest.next.expect("the latest page carries a cursor");

    let name = format!("{}_changes", laser.default_stream().expect("a stream"));
    laser
        .stream(&name)
        .ensure()
        .await
        .expect("the stream is created");
    let page = laser
        .metadata_changes(MetadataChangesQuery {
            start: MetadataChangeStart::After(cursor),
            groups: vec![MetadataChangeGroup::Streams],
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("the changes read");
    let created = page
        .records
        .iter()
        .find(|record| record.subject.name.as_deref() == Some(name.as_str()))
        .expect("the stream creation is recorded");
    assert_eq!(created.kind, Some(MetadataChangeKind::StreamCreated));
    assert!(created.subject.stream_id.is_some());
    assert!(created.timestamp_micros > 0);
    laser
        .stream(&name)
        .delete()
        .await
        .expect("the stream is deleted");
}

#[tokio::test]
async fn given_a_user_without_server_grants_when_metadata_changes_are_read_then_should_be_refused()
{
    let laser = harness::connected_laser().await;
    let name = format!("{}_reader", laser.default_stream().expect("a stream"));
    laser
        .user(&name)
        .create("secret-1", CreateUserOptions::default())
        .await
        .expect("the user is created");
    let connection_string = harness::connection_string().await;
    let (scheme, rest) = connection_string
        .split_once("://")
        .expect("the connection string has a scheme");
    let (_, address) = rest
        .split_once('@')
        .expect("the connection string has credentials");
    let reader =
        laser_sdk::prelude::full::Laser::connect(&format!("{scheme}://{name}:secret-1@{address}"))
            .await
            .expect("the user signs in");
    let refused = reader
        .metadata_changes(MetadataChangesQuery::default())
        .await
        .expect_err("reading changes needs the manage servers permission");
    assert!(
        matches!(&refused, LaserError::Transport(transport)
            if transport.iggy_error_code() == IggyError::Unauthorized.as_code()),
        "unexpected error {refused:?}"
    );
    laser
        .user(&name)
        .delete()
        .await
        .expect("the user is deleted");
}

const PING_CODE: u32 = 1;
const GET_STATS_CODE: u32 = 10;
const LOGIN_USER_CODE: u32 = 38;

#[tokio::test]
async fn given_raw_commands_when_sent_then_should_return_the_reply_of_the_node_or_the_replica() {
    let laser = harness::connected_laser().await;
    let pong = laser
        .send_command(PING_CODE, Vec::new())
        .await
        .expect("the ping command runs");
    assert!(pong.is_empty());
    let cluster = laser.cluster().await.expect("the cluster reads");
    let replica = cluster.nodes.first().expect("a node").replica_id;
    let stats = laser
        .send_command_to_replica(GET_STATS_CODE, Vec::new(), replica)
        .await
        .expect("the replica answers with its own statistics");
    assert!(!stats.is_empty());
    let refused = laser
        .send_command(LOGIN_USER_CODE, Vec::new())
        .await
        .expect_err("only the connection signs in");
    assert!(
        matches!(&refused, LaserError::Transport(transport)
            if transport.iggy_error_code() == IggyError::InvalidCommand.as_code()),
        "unexpected error {refused:?}"
    );
}

#[tokio::test]
async fn given_an_operation_the_client_does_not_retain_when_settled_then_should_name_it_as_not_retained()
 {
    let laser = harness::connected_laser().await;
    let operation = u64::MAX - 7;
    for refused in [
        laser
            .resolve_operation(operation, std::time::Duration::from_secs(5))
            .await
            .expect_err("the client never sent the operation"),
        laser
            .abandon_operation(operation)
            .await
            .expect_err("the client never sent the operation"),
    ] {
        assert_eq!(
            refused.iggy_error_code(),
            Some(IggyError::OperationNotRetained(operation).as_code()),
            "unexpected error {refused:?}"
        );
        assert_eq!(refused.uncertain_operation(), Some(operation));
    }
}
