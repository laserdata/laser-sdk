use crate::error::LaserError;
use iggy::prelude::{Identifier, IggyMessage, Sharding};
use std::future::Future;
use std::sync::Arc;

/// What a producer's own sharding sees for one background send: the shard
/// count, the target, and how many records the send carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRequest<'a> {
    /// The background shard count, at least one.
    pub shards: usize,
    pub stream: &'a str,
    pub topic: &'a str,
    /// The records of this send.
    pub records: usize,
}

pub(crate) type ShardPicker =
    Arc<dyn Fn(&ShardRequest<'_>) -> Result<usize, LaserError> + Send + Sync>;

tokio::task_local! {
    static PICKED: usize;
}

// A shard index `pick` checked against the background shard count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PickedShard(usize);

// The caller's shard for one send. An error or an index past the last shard
// fails the send before anything is queued.
pub(crate) fn pick(
    picker: &ShardPicker,
    request: &ShardRequest<'_>,
) -> Result<PickedShard, LaserError> {
    let shard = picker(request)?;
    if shard >= request.shards {
        return Err(LaserError::Invalid(format!(
            "sharding picked shard {shard}, but the producer has {} shards",
            request.shards
        )));
    }
    Ok(PickedShard(shard))
}

// Runs one background send with the shard the caller picked for it. Apache
// Iggy picks the shard inline in the send, on the same task.
pub(crate) async fn on_shard<F: Future>(shard: PickedShard, send: F) -> F::Output {
    PICKED.scope(shard.0, send).await
}

// The Apache Iggy sharding behind a caller's picker. Every send of such a
// producer runs inside `on_shard` with a shard `pick` checked against this
// same shard count, so the shard of the send in progress is always in scope.
#[derive(Debug)]
pub(crate) struct PickedSharding;

impl Sharding for PickedSharding {
    fn pick_shard(
        &self,
        _num_shards: usize,
        _messages: &[IggyMessage],
        _stream: &Identifier,
        _topic: &Identifier,
    ) -> usize {
        PICKED.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(shards: usize) -> ShardRequest<'static> {
        ShardRequest {
            shards,
            stream: "metrics",
            topic: "readings",
            records: 3,
        }
    }

    #[test]
    fn given_a_picker_when_it_picks_a_shard_in_range_then_should_use_it() {
        let picker: ShardPicker = Arc::new(|request| Ok(request.records % request.shards));
        assert_eq!(
            pick(&picker, &request(2)).expect("shard in range"),
            PickedShard(1)
        );
    }

    #[test]
    fn given_a_picker_when_it_picks_past_the_last_shard_then_should_refuse_the_send() {
        let picker: ShardPicker = Arc::new(|request| Ok(request.shards));
        assert!(matches!(
            pick(&picker, &request(4)),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_failing_picker_when_picking_then_should_return_its_error() {
        let picker: ShardPicker =
            Arc::new(|_| Err(LaserError::Invalid("no shard for this send".to_owned())));
        assert!(matches!(
            pick(&picker, &request(4)),
            Err(LaserError::Invalid(message)) if message == "no shard for this send"
        ));
    }

    #[tokio::test]
    async fn given_a_picked_shard_when_iggy_picks_inside_the_send_then_should_read_it() {
        let stream = Identifier::named("metrics").expect("stream identifier");
        let topic = Identifier::named("readings").expect("topic identifier");
        let picked = on_shard(PickedShard(2), async {
            PickedSharding.pick_shard(4, &[], &stream, &topic)
        })
        .await;
        assert_eq!(picked, 2);
    }
}
