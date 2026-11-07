use laser_sdk::LaserError;
use std::time::Duration;
use tokio::time::{Instant, sleep};

pub mod filter_fixtures;
pub mod graph_engine;
pub mod kv_engine;
pub mod memory_engine;
pub mod query_engine;

pub async fn wait_for_partition_reads(
    mut read: impl AsyncFnMut() -> Result<(), LaserError>,
) -> Result<(), LaserError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match read().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_unavailable() && Instant::now() < deadline => {
                sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::IggyError;

    #[tokio::test]
    async fn given_a_partition_not_yet_admitted_when_waiting_for_reads_then_should_probe_until_readable()
     {
        let mut calls = 0;
        let result = wait_for_partition_reads(async || {
            calls += 1;
            if calls == 1 {
                Err(IggyError::TransientNotAccepted.into())
            } else {
                Ok(())
            }
        })
        .await;
        assert!(result.is_ok(), "partition readiness: {result:?}");
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn given_a_permanent_read_error_when_waiting_for_reads_then_should_return_it_without_retry()
     {
        let mut calls = 0;
        let result = wait_for_partition_reads(async || {
            calls += 1;
            Err(LaserError::Invalid("invalid fixture read".to_owned()))
        })
        .await;
        assert!(matches!(result, Err(LaserError::Invalid(_))));
        assert_eq!(calls, 1);
    }
}
