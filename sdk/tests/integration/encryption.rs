use crate::harness;
use laser_sdk::iggy::prelude::BackgroundConfig;
use laser_sdk::prelude::full::LaserError;
use laser_sdk::prelude::{CommitPolicy, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::stream::{Encryption, HeaderKey, HeaderValue, ShardRequest};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const PLAINTEXT: &[u8] = br#"{"host":"node-7","cpu":82}"#;

fn encryption() -> Encryption {
    Encryption::aes256_gcm(*b"laser-encryption-test-key-32byte").expect("a 32-byte key")
}

fn header(name: &str) -> HeaderKey {
    HeaderKey::from_str(name).expect("a header key")
}

#[tokio::test]
async fn given_an_encrypted_producer_when_read_with_and_without_the_key_then_should_store_only_ciphertext()
 {
    let laser = harness::laser().await;
    let topic = laser.topic("sealed");
    let producer = topic
        .producer()
        .partitions(1)
        .encryption(encryption())
        .build()
        .await
        .expect("producer");
    producer
        .send_message(ProducerMessage::new(PLAINTEXT).header(
            header("source"),
            HeaderValue::from_str("node-7").expect("a header value"),
        ))
        .await
        .expect("publish");

    let mut plain = topic
        .consumer("without-key", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("consumer without the key");
    let stored = plain
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("stored record");
    assert_ne!(stored.payload.as_ref(), PLAINTEXT);
    assert_eq!(
        encryption().decrypt(&stored.payload).expect("decrypt"),
        PLAINTEXT
    );
    assert!(stored.headers.is_empty(), "the header block is sealed");
    plain.shutdown().await.expect("shutdown");

    let mut opened = topic
        .consumer("with-key", 0)
        .start_at(ConsumerStart::First)
        .encryption(encryption())
        .build()
        .await
        .expect("consumer with the key");
    let record = opened
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("decrypted record");
    assert_eq!(record.payload.as_ref(), PLAINTEXT);
    assert_eq!(
        record.headers.get(&header("source")),
        Some(&HeaderValue::from_str("node-7").expect("a header value"))
    );
    opened.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_an_encrypted_group_consumer_when_reading_then_should_see_the_plaintext() {
    let laser = harness::laser().await;
    let topic = laser.topic("sealed-group");
    let producer = topic
        .producer()
        .partitions(1)
        .encryption(encryption())
        .build()
        .await
        .expect("producer");
    producer.send(PLAINTEXT).await.expect("publish");
    let mut consumer = topic
        .consumer_group("sealed-readers")
        .consumer()
        .start_at(ConsumerStart::First)
        .encryption(encryption())
        .build()
        .await
        .expect("group consumer");
    let record = consumer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("decrypted record");
    assert_eq!(record.payload.as_ref(), PLAINTEXT);
    consumer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_a_record_that_does_not_decrypt_when_read_then_should_report_it_once_and_read_past() {
    let laser = harness::laser().await;
    let topic = laser.topic("sealed-mixed");
    let plain = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("plain producer");
    plain
        .send_to_partition(ProducerMessage::new(b"not sealed".as_slice()), 0)
        .await
        .expect("plain publish");
    let sealed = topic
        .producer()
        .partitions(1)
        .encryption(encryption())
        .build()
        .await
        .expect("sealed producer");
    sealed
        .send_to_partition(ProducerMessage::new(PLAINTEXT), 0)
        .await
        .expect("sealed publish");
    let mut consumer = topic
        .consumer("mixed", 0)
        .start_at(ConsumerStart::First)
        .encryption(encryption())
        .build()
        .await
        .expect("consumer");
    let error = consumer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect_err("the plain record does not decrypt");
    assert_eq!(error.iggy_error_code(), Some(72));
    let record = consumer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the sealed record after it");
    assert_eq!(record.payload.as_ref(), PLAINTEXT);
    assert_eq!(record.position.offset, 1);
    consumer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_an_encrypted_consumer_when_it_commits_on_polling_then_should_refuse_to_build() {
    let laser = harness::laser().await;
    let result = laser
        .topic("sealed-polling")
        .consumer("polling", 0)
        .commit_policy(CommitPolicy::Polling)
        .encryption(encryption())
        .build()
        .await;
    assert!(matches!(result, Err(LaserError::Invalid(_))));
}

#[tokio::test]
async fn given_a_sharding_picker_when_sending_in_background_then_should_route_each_send_through_it()
{
    let laser = harness::laser().await;
    let topic = laser.topic("sharded");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let producer = topic
        .producer()
        .partitions(1)
        .background(BackgroundConfig::builder().num_shards(4).build())
        .sharding(move |request: &ShardRequest<'_>| {
            recorded.lock().expect("seen").push((
                request.shards,
                request.stream.to_owned(),
                request.topic.to_owned(),
                request.records,
            ));
            Ok(request.records % request.shards)
        })
        .build()
        .await
        .expect("producer");
    producer
        .send_batch_with_routing(
            [
                ProducerMessage::new(b"one".as_slice()),
                ProducerMessage::new(b"two".as_slice()),
                ProducerMessage::new(b"three".as_slice()),
            ],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("queue");
    producer.shutdown().await.expect("drain");
    let stream = laser.default_stream().expect("a default stream").to_owned();
    assert_eq!(
        *seen.lock().expect("seen"),
        vec![(4, stream, "sharded".to_owned(), 3)]
    );
    let mut consumer = topic
        .consumer("sharded-reader", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("consumer");
    for expected in [b"one".as_slice(), b"two", b"three"] {
        let record = consumer.next_within(RECEIVE_TIMEOUT).await.expect("record");
        assert_eq!(record.payload.as_ref(), expected);
    }
    consumer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_a_sharding_picker_when_it_picks_past_the_last_shard_or_fails_then_should_fail_the_send_and_queue_nothing()
 {
    let laser = harness::laser().await;
    let topic = laser.topic("sharded-refused");
    let picks = Arc::new(Mutex::new(
        vec![
            Ok(2),
            Err(LaserError::Invalid("no shard for this send".to_owned())),
            Ok(1),
        ]
        .into_iter(),
    ));
    let producer = topic
        .producer()
        .partitions(1)
        .background(BackgroundConfig::builder().num_shards(2).build())
        .sharding(move |_: &ShardRequest<'_>| {
            picks
                .lock()
                .expect("picks")
                .next()
                .expect("a pick for each send")
        })
        .build()
        .await
        .expect("producer");
    let send =
        |payload: &'static [u8]| producer.send_to_partition(ProducerMessage::new(payload), 0);
    assert!(matches!(
        send(b"past-the-last-shard").await,
        Err(LaserError::Invalid(_))
    ));
    assert!(matches!(
        send(b"failing-picker").await,
        Err(LaserError::Invalid(message)) if message == "no shard for this send"
    ));
    send(b"accepted").await.expect("queue");
    producer.shutdown().await.expect("drain");
    let mut consumer = topic
        .consumer("sharded-refused-reader", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("consumer");
    let record = consumer.next_within(RECEIVE_TIMEOUT).await.expect("record");
    assert_eq!(record.payload.as_ref(), b"accepted");
    consumer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_a_sharding_picker_without_background_mode_when_building_then_should_refuse_it() {
    let laser = harness::laser().await;
    let result = laser
        .topic("sharded-direct")
        .producer()
        .sharding(|_: &ShardRequest<'_>| Ok(0))
        .build()
        .await;
    assert!(matches!(result, Err(LaserError::Invalid(_))));
}
