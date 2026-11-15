use crate::harness;
use laser_sdk::iggy::prelude::{BatchProducer, Identifier, IggyMessage, MessageClient};
use laser_sdk::prelude::{ConsumerStart, ProducerMessage, Routing};
use std::time::Duration;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const PAYLOADS: [&str; 3] = [
    r#"{"host":"node-1","cpu":41}"#,
    r#"{"host":"node-2","cpu":57}"#,
    r#"{"host":"node-3","cpu":82}"#,
];

#[tokio::test]
async fn given_a_batch_whose_reply_was_lost_when_the_idempotent_producer_sends_it_then_should_write_it_once()
 {
    let laser = harness::laser().await;
    let topic = laser.topic("idempotent");
    topic.ensure(1, None).await.expect("topic");
    let client = laser.client();
    // Ids of one session count up, so the producer takes the next one.
    let taken = client
        .next_producer_id()
        .await
        .expect("a producer id of the session");
    let first_attempt = BatchProducer::new(taken + 1, 0, 0).expect("a producer section");
    let mut landed: Vec<IggyMessage> = PAYLOADS
        .iter()
        .map(|payload| {
            IggyMessage::builder()
                .payload(payload.as_bytes().to_vec().into())
                .build()
                .expect("a message")
        })
        .collect();
    client
        .send_producer_messages(
            &Identifier::named(laser.default_stream().expect("a default stream"))
                .expect("stream id"),
            &Identifier::named("idempotent").expect("topic id"),
            0,
            first_attempt,
            &mut landed,
        )
        .await
        .expect("the first attempt lands");

    let producer = topic
        .producer()
        .idempotent()
        .routing(Routing::Partition(0))
        .build()
        .await
        .expect("idempotent producer");
    let resent = producer
        .send_batch(PAYLOADS.map(ProducerMessage::new))
        .await
        .expect("the resend is confirmed");
    assert_eq!(
        resent
            .confirmations
            .iter()
            .map(|confirmation| confirmation.base_offset)
            .collect::<Vec<_>>(),
        vec![0],
        "the resend answers the offset of the first write"
    );
    let next = producer
        .send(r#"{"host":"node-4","cpu":12}"#)
        .await
        .expect("the next batch");
    assert_eq!(next.confirmations[0].base_offset, 3);

    let mut consumer = topic
        .consumer("idempotent-reader", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("consumer");
    let mut read = Vec::new();
    for _ in 0..4 {
        let record = consumer
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("stored record");
        read.push((record.position.offset, record.payload.to_vec()));
    }
    assert_eq!(
        read,
        vec![
            (0, PAYLOADS[0].as_bytes().to_vec()),
            (1, PAYLOADS[1].as_bytes().to_vec()),
            (2, PAYLOADS[2].as_bytes().to_vec()),
            (3, br#"{"host":"node-4","cpu":12}"#.to_vec()),
        ]
    );
    consumer.shutdown().await.expect("shutdown");
    producer.shutdown().await.expect("shutdown");
}
