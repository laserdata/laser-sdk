use crate::error::LaserError;
use std::future::Future;
use std::time::Duration;

/// Publish attempts are at-least-once. A lost reply can cause duplicate delivery.
#[derive(Clone, Copy, Debug)]
pub struct PublishOptions {
    pub timeout: Duration,
    /// Additional attempts after the initial send. Zero disables retries.
    pub max_retries: u32,
    pub retry_backoff: Duration,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            max_retries: 3,
            retry_backoff: Duration::from_millis(250),
        }
    }
}

impl PublishOptions {
    pub(crate) fn from_env(
        timeout: Option<Duration>,
        max_retries: Option<u32>,
        retry_backoff: Option<Duration>,
    ) -> Result<Self, LaserError> {
        let defaults = Self::default();
        let options = Self {
            timeout: match timeout {
                Some(value) => value,
                None => {
                    Duration::from_millis(env_number("LASER_PUBLISH_TIMEOUT_MS")?.unwrap_or(60_000))
                }
            },
            max_retries: match max_retries {
                Some(value) => value,
                None => env_number("LASER_PUBLISH_MAX_RETRIES")?
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| LaserError::Config("LASER_PUBLISH_MAX_RETRIES exceeds u32"))?
                    .unwrap_or(defaults.max_retries),
            },
            retry_backoff: match retry_backoff {
                Some(value) => value,
                None => Duration::from_millis(
                    env_number("LASER_PUBLISH_RETRY_BACKOFF_MS")?.unwrap_or(250),
                ),
            },
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<(), LaserError> {
        if self.timeout < Duration::from_millis(1)
            || self.timeout > Duration::from_millis(i32::MAX as u64)
        {
            return Err(LaserError::Config(
                "publish timeout must be between 1 and 2147483647 milliseconds",
            ));
        }
        if self.retry_backoff < Duration::from_millis(1)
            || self.retry_backoff > Duration::from_millis(i32::MAX as u64)
        {
            return Err(LaserError::Config(
                "publish retry backoff must be between 1 and 2147483647 milliseconds",
            ));
        }
        Ok(())
    }

    pub(crate) async fn run<T, F, Fut, R, Recover>(
        self,
        mut send: F,
        mut recover: R,
    ) -> Result<T, LaserError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, LaserError>>,
        R: FnMut() -> Recover,
        Recover: Future<Output = Result<(), LaserError>>,
    {
        let mut needs_recovery = false;
        let mut attempt = 0;
        loop {
            let result = tokio::time::timeout(self.timeout, async {
                if needs_recovery {
                    recover().await?;
                }
                send().await
            })
            .await
            .unwrap_or(Err(LaserError::Timeout("Iggy publish response")));
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let retryable = match &error {
                        LaserError::Timeout(_) => true,
                        LaserError::Iggy(cause) => crate::laser::is_transient_iggy_io_error(cause),
                        _ => false,
                    };
                    if !retryable || attempt == self.max_retries {
                        // A cancelled read cannot leave its late frame on the shared connection.
                        if matches!(error, LaserError::Timeout(_)) {
                            let _ = tokio::time::timeout(self.timeout, recover()).await;
                        }
                        return Err(error);
                    }
                    needs_recovery = true;
                    if matches!(error, LaserError::Timeout(_)) {
                        needs_recovery = !matches!(
                            tokio::time::timeout(self.timeout, recover()).await,
                            Ok(Ok(()))
                        );
                    }
                    let delay = self
                        .retry_backoff
                        .saturating_mul(1u32 << attempt.min(16))
                        .min(Duration::from_secs(30));
                    tracing::warn!(attempt = attempt + 1, max_retries = self.max_retries, %error, "publish failed, reconnecting before retry");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

fn env_number(name: &'static str) -> Result<Option<u64>, LaserError> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|_| LaserError::Config(name)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(LaserError::Config(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::IggyError;
    use std::cell::Cell;

    fn options() -> PublishOptions {
        PublishOptions {
            timeout: Duration::from_millis(10),
            max_retries: 2,
            retry_backoff: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn given_a_stalled_publish_when_retried_then_should_recover_and_return_confirmation() {
        let sends = Cell::new(0);
        let recoveries = Cell::new(0);
        let result = options()
            .run(
                || {
                    sends.set(sends.get() + 1);
                    let attempt = sends.get();
                    async move {
                        if attempt == 1 {
                            std::future::pending::<()>().await;
                        }
                        Ok(42)
                    }
                },
                || {
                    recoveries.set(recoveries.get() + 1);
                    async { Ok(()) }
                },
            )
            .await
            .expect("retry succeeds");
        assert_eq!(result, 42);
        assert_eq!(sends.get(), 2);
        assert_eq!(recoveries.get(), 1);
    }

    #[tokio::test]
    async fn given_a_disconnected_server_when_retries_exhaust_then_should_stop_at_the_limit() {
        let sends = Cell::new(0);
        let result = options()
            .run(
                || {
                    sends.set(sends.get() + 1);
                    async { Err::<(), _>(LaserError::Iggy(IggyError::Disconnected)) }
                },
                || async { Ok(()) },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(sends.get(), 3);
    }

    #[tokio::test]
    async fn given_a_permanent_error_when_publishing_then_should_not_retry() {
        let sends = Cell::new(0);
        let result = options()
            .run(
                || {
                    sends.set(sends.get() + 1);
                    async { Err::<(), _>(LaserError::Iggy(IggyError::Unauthorized)) }
                },
                || async { panic!("permanent failure must not reconnect") },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(sends.get(), 1);
    }

    #[tokio::test]
    async fn given_disabled_retries_when_publish_times_out_then_should_clean_up_and_stop() {
        let sends = Cell::new(0);
        let recoveries = Cell::new(0);
        let config = PublishOptions {
            max_retries: 0,
            ..options()
        };
        let result = config
            .run(
                || {
                    sends.set(sends.get() + 1);
                    std::future::pending::<Result<(), LaserError>>()
                },
                || {
                    recoveries.set(recoveries.get() + 1);
                    async { Ok(()) }
                },
            )
            .await;
        assert!(matches!(result, Err(LaserError::Timeout(_))));
        assert_eq!(sends.get(), 1);
        assert_eq!(recoveries.get(), 1);
    }

    #[tokio::test]
    async fn given_an_exhausted_outage_when_a_later_call_succeeds_then_should_remain_usable() {
        let offline = Cell::new(true);
        let send = || async {
            if offline.get() {
                Err(LaserError::Iggy(IggyError::Disconnected))
            } else {
                Ok(7)
            }
        };
        assert!(options().run(send, || async { Ok(()) }).await.is_err());
        offline.set(false);
        assert_eq!(
            options()
                .run(send, || async { Ok(()) })
                .await
                .expect("later publish succeeds"),
            7
        );
    }

    #[tokio::test]
    async fn given_a_stalled_reconnect_when_retrying_then_should_return_a_bounded_error() {
        let config = PublishOptions {
            max_retries: 1,
            ..options()
        };
        let result = config
            .run(
                || async { Err::<(), _>(LaserError::Iggy(IggyError::Disconnected)) },
                std::future::pending::<Result<(), LaserError>>,
            )
            .await;
        assert!(matches!(result, Err(LaserError::Timeout(_))));
    }

    #[tokio::test]
    async fn given_an_applied_send_with_an_unreadable_reply_when_publishing_then_should_not_resend()
    {
        for failure in [
            iggy::prelude::IggyError::InvalidBytesResponse,
            iggy::prelude::IggyError::InvalidJsonResponse,
            iggy::prelude::IggyError::RequestAlreadyApplied,
        ] {
            let mut failure = Some(failure);
            let result = options()
                .run(
                    || {
                        std::future::ready(Err::<(), _>(LaserError::Iggy(
                            failure.take().expect("must only send once"),
                        )))
                    },
                    || async { panic!("an applied send must not reconnect and replay") },
                )
                .await;
            assert!(result.is_err());
        }
    }

    #[test]
    fn given_invalid_publish_durations_when_configured_then_should_reject_them() {
        assert!(
            PublishOptions {
                timeout: Duration::ZERO,
                ..options()
            }
            .validate()
            .is_err()
        );
        assert!(
            PublishOptions {
                retry_backoff: Duration::ZERO,
                ..options()
            }
            .validate()
            .is_err()
        );
        assert!(
            PublishOptions {
                timeout: Duration::MAX,
                ..options()
            }
            .validate()
            .is_err()
        );
    }
}
