use crate::error::LaserError;
use iggy::prelude::*;

pub(crate) struct PartitionBatch {
    pub messages: Vec<IggyMessage>,
    pub next_offset: u64,
}

// Drains a partition from `from_offset` to its current tail in `batch`-sized
// polls, returning every message read plus the offset to resume from. Callers
// that poll repeatedly pass back `next_offset` so each pass reads only what is
// new instead of rescanning from zero.
pub(crate) async fn drain_partition(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    partition: u32,
    from_offset: u64,
    batch: u32,
) -> Result<PartitionBatch, LaserError> {
    let mut offset = from_offset;
    let mut messages = Vec::new();
    loop {
        let polled = client
            .poll_messages(
                stream,
                topic,
                Some(partition),
                consumer,
                &PollingStrategy::offset(offset),
                batch,
                false,
            )
            .await?;
        let Some(last) = polled.messages.last() else {
            break;
        };
        offset = last.header.offset + 1;
        let count = polled.messages.len();
        messages.extend(polled.messages);
        if (count as u32) < batch {
            break;
        }
    }
    Ok(PartitionBatch {
        messages,
        next_offset: offset,
    })
}
