use laser_examples::{init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::{
    Capabilities, CommitPolicy, Consumer, ConsumerStart, LaserError, Producer, ProducerMessage,
    Routing,
};
use laser_sdk::stream::{HeaderKey, HeaderValue};
use std::time::Duration;
use tracing::info;

const TOPIC: &str = "events";
const MESSAGE_COUNT: usize = 1000;
const BATCH: usize = 100;
const PROGRESS_EVERY: usize = 100;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("native-streaming"), Capabilities::OPEN).await?;
    let topic = laser.topic(TOPIC);
    let producer = topic
        .producer()
        .batch_length(BATCH as u32)
        .linger(Duration::from_millis(5))
        .retries(Some(3), Some(Duration::from_secs(1)))
        .routing(Routing::Balanced)
        .partitions(1)
        .build()
        .await?;

    phase("producer: exact-width header, keyed routing, and 1000 batched messages");
    publish_messages(&producer).await?;

    phase("consumer: production interval-or-each auto commit");
    let auto = topic
        .consumer_group("auto-workers")
        .batch_length(BATCH as u32)
        .poll_interval(Duration::from_millis(5))
        .start_at(ConsumerStart::First)
        .allow_replay()
        .commit_policy(CommitPolicy::IntervalOrEach(Duration::from_secs(1)))
        .build()
        .await?;
    receive(auto, false).await?;

    phase("consumer: commit after successful handling (one round-trip per message)");
    let manual = topic
        .consumer_group("manual-workers")
        .batch_length(BATCH as u32)
        .poll_interval(Duration::from_millis(5))
        .start_at(ConsumerStart::First)
        .allow_replay()
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await?;
    receive(manual, true).await
}

// One keyed send with an exact-width header, then the rest of `MESSAGE_COUNT`
// in batches of `BATCH`, matching the producer's own `batch_length`.
async fn publish_messages(producer: &Producer) -> Result<(), LaserError> {
    let event_type = HeaderKey::try_from("type")?;
    producer
        .send_keyed(
            ProducerMessage::new(b"message-0".as_slice())
                .header(event_type, HeaderValue::from(7_u16)),
            b"account-42".to_vec(),
        )
        .await?;

    let mut sent = 1;
    while sent < MESSAGE_COUNT {
        // The lone keyed send above already used up one of this batch's slots,
        // so trim it by one here to keep every later boundary (and the progress
        // print) on a round multiple of BATCH instead of off by one forever.
        let batch_size = if sent == 1 { BATCH - 1 } else { BATCH }.min(MESSAGE_COUNT - sent);
        let batch = (sent..sent + batch_size)
            .map(|index| ProducerMessage::new(format!("message-{index}").into_bytes()))
            .collect::<Vec<_>>();
        producer.send_batch(batch).await?;
        sent += batch_size;
        info!("published {sent}/{MESSAGE_COUNT} messages");
    }
    Ok(())
}

async fn receive(mut consumer: Consumer, manual_commit: bool) -> Result<(), LaserError> {
    if manual_commit {
        info!(
            "committing after every message: one store_offset round-trip each, {MESSAGE_COUNT} \
             total - much slower than the batched auto-commit above on any real network, by design"
        );
    }
    let mut seen = 0usize;
    while let Some(message) = consumer.next().await {
        let message = message?;
        if manual_commit {
            consumer.commit(&message).await?;
        }
        seen += 1;
        if seen.is_multiple_of(PROGRESS_EVERY) || seen == MESSAGE_COUNT {
            info!(
                "received {seen}/{MESSAGE_COUNT}: partition={} offset={} headers={:?} payload={:?}",
                message.partition_id, message.position.offset, message.headers, message.payload
            );
        }
        if seen == MESSAGE_COUNT {
            break;
        }
    }
    consumer.shutdown().await
}
