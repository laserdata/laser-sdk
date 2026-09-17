use crate::error::LaserError;
use iggy::prelude::*;
use tokio::time::{Duration, sleep};

// Ceiling on the messages one drain accumulates in memory before returning.
// A partition holds an unbounded history, so a pass that reads to the tail
// would otherwise materialize all of it, payloads included. A caller that has
// not caught up resumes from `next_offset`.
pub(crate) const MAX_DRAIN_MESSAGES: usize = 10_000;

// Defensive ceiling on a server-reported partition count. It sizes client-side
// allocations and loop counts, so it is treated as untrusted input.
pub(crate) const MAX_TOPIC_PARTITIONS: u32 = 1024;

// Clamp a server-reported partition count before it sizes an allocation or a
// loop. A hostile or corrupt reply cannot turn a topic lookup into a multi-
// gigabyte reservation.
pub(crate) fn bounded_partitions(reported: u32) -> u32 {
    reported.min(MAX_TOPIC_PARTITIONS)
}

pub(crate) struct PartitionBatch {
    pub messages: Vec<IggyMessage>,
    pub next_offset: u64,
}

// The offset a bounded read should start from so its window ends at the
// partition tail instead of being cut off at the head. A reader that wants the
// most recent records (context assembly, a `LastN` policy) must not be handed
// the oldest `MAX_DRAIN_MESSAGES` of a long partition and then filter those,
// which would return the wrong records entirely.
#[cfg(feature = "agent")]
pub(crate) async fn tail_anchored_offset(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    partition: u32,
    from_offset: u64,
) -> Result<u64, LaserError> {
    let polled = client
        .poll_messages(
            stream,
            topic,
            Some(partition),
            consumer,
            &PollingStrategy::last(),
            1,
            false,
        )
        .await?;
    let Some(last) = polled.messages.last() else {
        return Ok(from_offset);
    };
    let window_start = last
        .header
        .offset
        .saturating_sub(MAX_DRAIN_MESSAGES as u64 - 1);
    Ok(from_offset.max(window_start))
}

// One partition read: from `from` toward the tail, or up to and including
// `end` for a point-in-time read behind a `Checkpoint`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainRange {
    pub(crate) partition: u32,
    pub(crate) from: u64,
    pub(crate) end: Option<u64>,
}

impl DrainRange {
    pub(crate) const fn open(partition: u32, from: u64) -> Self {
        Self {
            partition,
            from,
            end: None,
        }
    }

    #[cfg(feature = "agent")]
    pub(crate) const fn until(partition: u32, from: u64, end: u64) -> Self {
        Self {
            partition,
            from,
            end: Some(end),
        }
    }
}

// Drains `range` in `batch`-sized polls, returning the messages read (at most
// `MAX_DRAIN_MESSAGES`) plus the offset to resume from. Callers that poll
// repeatedly pass back `next_offset` so each pass reads only what is new. A
// bounded range drops anything past `end` and resumes at `end + 1`.
pub(crate) async fn drain_partition(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    range: DrainRange,
    batch: u32,
) -> Result<PartitionBatch, LaserError> {
    let DrainRange {
        partition,
        from,
        end,
    } = range;
    if end.is_some_and(|end| from > end) {
        return Ok(PartitionBatch {
            messages: Vec::new(),
            next_offset: from,
        });
    }
    let mut offset = from;
    let mut messages = Vec::new();
    loop {
        let count = batch
            .max(1)
            .min((MAX_DRAIN_MESSAGES - messages.len()) as u32);
        let mut last_error = None;
        let mut polled = None;
        for attempt in 0..5 {
            match client
                .poll_messages(
                    stream,
                    topic,
                    Some(partition),
                    consumer,
                    &PollingStrategy::offset(offset),
                    count,
                    false,
                )
                .await
            {
                Ok(batch) => {
                    polled = Some(batch);
                    break;
                }
                Err(error) if crate::laser::is_transient_iggy_io_error(&error) && attempt < 4 => {
                    last_error = Some(error);
                    sleep(Duration::from_millis(50 * (attempt + 1))).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let polled = polled
            .ok_or_else(|| LaserError::from(last_error.expect("retry loop stores the error")))?;
        let Some(last) = polled.messages.last() else {
            break;
        };
        offset = last.header.offset.saturating_add(1);
        let received_count = polled.messages.len();
        let mut received = polled.messages;
        if let Some(end) = end
            && let Some(cut) = received
                .iter()
                .position(|message| message.header.offset > end)
        {
            received.truncate(cut);
            messages.extend(received);
            offset = end.saturating_add(1);
            break;
        }
        messages.extend(received);
        if (received_count as u32) < count {
            break;
        }
        if messages.len() >= MAX_DRAIN_MESSAGES {
            break;
        }
        if end.is_some_and(|end| offset > end) {
            break;
        }
    }
    Ok(PartitionBatch {
        messages,
        next_offset: offset,
    })
}

// The next offset one partition will write: one past its last message, or 0
// when it is empty. This is what a `Checkpoint` records.
#[cfg(feature = "agent")]
pub(crate) async fn current_tail_offset(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    partition: u32,
) -> Result<u64, LaserError> {
    let polled = client
        .poll_messages(
            stream,
            topic,
            Some(partition),
            consumer,
            &PollingStrategy::last(),
            1,
            false,
        )
        .await?;
    Ok(polled
        .messages
        .last()
        .map_or(0, |message| message.header.offset.saturating_add(1)))
}
