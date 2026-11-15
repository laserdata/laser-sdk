use crate::harness;
use laser_sdk::prelude::full::Laser;
use laser_sdk::server::{
    CreateUserOptions, GlobalPermissions, Permissions, UpdateUserOptions, UserStatus,
};
use laser_sdk::stream::UpdateTopicOptions;
use std::time::Duration;

fn with_credentials(connection_string: &str, credentials: &str) -> String {
    let (scheme, rest) = connection_string
        .split_once("://")
        .expect("the connection string has a scheme");
    let (_, address) = rest
        .split_once('@')
        .expect("the connection string has credentials");
    format!("{scheme}://{credentials}@{address}")
}

fn unique(laser: &Laser, suffix: &str) -> String {
    format!(
        "{}_{suffix}",
        laser
            .default_stream()
            .expect("the test laser names its stream")
    )
}

#[tokio::test]
async fn given_a_new_user_when_created_updated_and_deleted_then_should_report_each_change() {
    let laser = harness::connected_laser().await;
    let name = unique(&laser, "user");
    let user = laser.user(&name);
    assert!(!user.exists().await.expect("a missing user reads"));

    let read_streams = Permissions {
        global: GlobalPermissions {
            read_streams: true,
            ..GlobalPermissions::default()
        },
        ..Permissions::default()
    };
    let created = user
        .create(
            "secret-1",
            CreateUserOptions {
                permissions: Some(read_streams.clone()),
                ..CreateUserOptions::default()
            },
        )
        .await
        .expect("the user is created");
    assert_eq!(created.username, name);
    assert_eq!(created.status, UserStatus::Active);
    assert!(created.created_at_micros > 0);

    let listed = laser.users().await.expect("users are listed");
    assert!(listed.iter().any(|info| info.username == name));
    let read = user
        .get()
        .await
        .expect("the user reads")
        .expect("the user exists");
    assert_eq!(read.permissions, Some(read_streams));

    let as_user = Laser::connect(&with_credentials(
        &harness::connection_string().await,
        &format!("{name}:secret-1"),
    ))
    .await
    .expect("the new user signs in");
    assert_eq!(
        as_user.me().await.expect("the connection reads").user_id,
        Some(read.id)
    );
    as_user.close().await.expect("the user connection closes");

    user.change_password("secret-1", "secret-2")
        .await
        .expect("the password changes");
    user.set_permissions(None)
        .await
        .expect("the permissions are removed");
    user.update(UpdateUserOptions {
        status: Some(UserStatus::Inactive),
        ..UpdateUserOptions::default()
    })
    .await
    .expect("the user is deactivated");
    let read = user
        .get()
        .await
        .expect("the user reads")
        .expect("the user exists");
    assert_eq!(read.status, UserStatus::Inactive);
    assert_eq!(read.permissions, None);

    let renamed = format!("{name}_renamed");
    user.update(UpdateUserOptions {
        username: Some(renamed.clone()),
        ..UpdateUserOptions::default()
    })
    .await
    .expect("the user is renamed");
    assert!(!user.exists().await.expect("the old name reads"));
    let user = laser.user(&renamed);
    assert!(user.delete().await.expect("the user is deleted"));
    assert!(!user.delete().await.expect("a missing user deletes"));
}

#[tokio::test]
async fn given_a_personal_access_token_when_created_then_should_sign_in_until_deleted() {
    let laser = harness::connected_laser().await;
    let name = unique(&laser, "token");
    let token = laser.personal_access_token(&name);
    let value = token
        .create(Some(Duration::from_secs(3_600)))
        .await
        .expect("the token is created");

    let listed = laser
        .personal_access_tokens()
        .await
        .expect("tokens are listed");
    let info = listed
        .iter()
        .find(|info| info.name == name)
        .expect("the new token is listed");
    assert!(info.expires_at_micros.is_some());

    let with_token = Laser::connect(&with_credentials(
        &harness::connection_string().await,
        &value,
    ))
    .await
    .expect("the token signs in");
    with_token.ping().await.expect("the token connection pings");
    with_token
        .close()
        .await
        .expect("the token connection closes");

    assert!(token.delete().await.expect("the token is deleted"));
    assert!(!token.delete().await.expect("a missing token deletes"));
}

#[tokio::test]
async fn given_a_connected_client_when_the_server_is_inspected_then_should_report_it() {
    let laser = harness::connected_laser().await;
    laser.ping().await.expect("the server answers a ping");

    let stats = laser.stats().await.expect("the statistics read");
    assert!(!stats.server_version.is_empty());
    assert!(stats.streams_count >= 1);

    let me = laser.me().await.expect("the connection reads");
    assert!(me.user_id.is_some());
    let clients = laser.clients().await.expect("the clients are listed");
    assert!(
        clients
            .iter()
            .any(|client| client.client_id == me.client_id)
    );
    let found = laser
        .client_info(me.client_id)
        .await
        .expect("the client reads")
        .expect("this client is connected");
    assert_eq!(found.address, me.address);
    assert!(
        laser
            .client_info(u32::MAX)
            .await
            .expect("a missing client reads")
            .is_none()
    );
}

#[tokio::test]
async fn given_a_stream_and_topic_when_renamed_and_updated_then_should_report_the_new_settings() {
    let laser = harness::connected_laser().await;
    let name = unique(&laser, "rename");
    let stream = laser.stream(&name);
    stream.ensure().await.expect("the stream is created");
    stream
        .topic("metrics")
        .ensure(1, None)
        .await
        .expect("the topic is created");

    let renamed = format!("{name}_new");
    stream
        .rename(&renamed)
        .await
        .expect("the stream is renamed");
    assert!(!stream.exists().await.expect("the old name reads"));
    let stream = laser.stream(&renamed);
    assert!(stream.exists().await.expect("the new name reads"));

    let topic = stream.topic("metrics");
    topic
        .update(UpdateTopicOptions {
            name: Some("hosts".to_owned()),
            expire_after: Some(Duration::from_secs(3_600)),
            max_topic_bytes: Some(1 << 30),
            ..UpdateTopicOptions::default()
        })
        .await
        .expect("the topic is updated");
    let info = stream
        .topic("hosts")
        .get()
        .await
        .expect("the topic reads")
        .expect("the renamed topic exists");
    assert_eq!(info.message_expiry, Some(Duration::from_secs(3_600)));
    assert_eq!(info.max_size_bytes, Some(1 << 30));

    stream
        .topic("hosts")
        .delete_segments(0, 1)
        .await
        .expect("the oldest segment is deleted");
    assert!(stream.delete().await.expect("the stream is deleted"));
}

#[tokio::test]
async fn given_a_server_when_the_cluster_and_a_snapshot_are_read_then_should_report_them() {
    let laser = harness::connected_laser().await;
    let cluster = laser.cluster().await.expect("the cluster reads");
    assert!(!cluster.nodes.is_empty());
    assert!(cluster.nodes.iter().any(|node| node.tcp_port > 0));

    let archive = laser.snapshot().await.expect("the snapshot downloads");
    assert_eq!(&archive[..2], b"PK", "the snapshot is a ZIP archive");
}
