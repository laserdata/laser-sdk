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

// Drains a partition from `from_offset` toward its current tail in `batch`-sized
// polls, returning the messages read (at most `MAX_DRAIN_MESSAGES`) plus the
// offset to resume from. Callers that poll repeatedly pass back `next_offset` so
// each pass reads only what is new instead of rescanning from zero.
//
// `end_offset` is the optional upper bound (inclusive) a point-in-time read
// stops at instead of the tail (`ContextAssembler`'s `to_checkpoint`, behind
// `Session::state_at`): a message past it is dropped and `next_offset` is
// clamped to `end_offset + 1`, so a caller resuming from it never reads past
// the checkpoint. `None` preserves the original tail-following behavior
// every existing caller relies on.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_partition(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    partition: u32,
    from_offset: u64,
    batch: u32,
    end_offset: Option<u64>,
) -> Result<PartitionBatch, LaserError> {
    if end_offset.is_some_and(|end| from_offset > end) {
        return Ok(PartitionBatch {
            messages: Vec::new(),
            next_offset: from_offset,
        });
    }
    let mut offset = from_offset;
    let mut messages = Vec::new();
    loop {
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
                    batch,
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
        let count = polled.messages.len();
        let mut received = polled.messages;
        if let Some(end) = end_offset
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
        if (count as u32) < batch {
            break;
        }
        if messages.len() >= MAX_DRAIN_MESSAGES {
            break;
        }
        if end_offset.is_some_and(|end| offset > end) {
            break;
        }
    }
    Ok(PartitionBatch {
        messages,
        next_offset: offset,
    })
}

// The current tail of one partition: the offset one past its last message,
// or 0 for an empty (or not-yet-created) partition. The single-message
// sibling of `tail_anchored_offset`'s own tail probe, pulled out so
// `Checkpoint` capture (a point in the log to fold up to or resume from
// later) can read it without pulling in the windowing logic that primitive
// exists for.
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
