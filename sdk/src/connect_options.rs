use crate::error::LaserError;
use crate::publish_options::env_number;
use iggy::prelude::{Client, IggyClient};
use iggy_common::DiagnosticEvent;
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_STAGE: &str = "the Iggy server to accept the connection";
const LOGIN_STAGE: &str = "the Iggy login reply";

/// Budget for the initial connect: TCP dial, TLS handshake, login, and the managed capability probe.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectOptions {
    pub(crate) timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }
}

impl ConnectOptions {
    pub(crate) fn from_env(timeout: Option<Duration>) -> Result<Self, LaserError> {
        let options = Self {
            timeout: match timeout {
                Some(value) => value,
                None => env_number("LASER_CONNECT_TIMEOUT_MS")?
                    .map_or(DEFAULT_CONNECT_TIMEOUT, Duration::from_millis),
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
                "connect timeout must be between 1 and 2147483647 milliseconds",
            ));
        }
        Ok(())
    }
}

/// Connect `client` before `deadline`. The Iggy client retries a failed dial by itself, and the deadline stops those retries.
pub(crate) async fn connect_before(
    client: &IggyClient,
    deadline: Instant,
) -> Result<(), LaserError> {
    let mut events = client.subscribe_events().await;
    match tokio::time::timeout_at(deadline, client.connect()).await {
        Ok(result) => result.map_err(LaserError::from),
        Err(_elapsed) => {
            // `Connected` is published only once the transport, TLS included, is up, so it
            // separates a server that never accepts the socket from one that never answers login.
            let mut accepted = false;
            while let Ok(event) = events.try_recv() {
                accepted |= event == DiagnosticEvent::Connected;
            }
            Err(LaserError::Timeout(if accepted {
                LOGIN_STAGE
            } else {
                ACCEPT_STAGE
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::IggyClientBuilder;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn given_no_explicit_timeout_when_resolving_then_should_default_to_thirty_seconds() {
        assert_eq!(ConnectOptions::default().timeout, Duration::from_secs(30));
        assert_eq!(
            ConnectOptions::from_env(Some(Duration::from_secs(5)))
                .expect("an explicit timeout is valid")
                .timeout,
            Duration::from_secs(5)
        );
    }

    #[test]
    fn given_invalid_connect_timeouts_when_configured_then_should_reject_them() {
        for timeout in [Duration::ZERO, Duration::MAX] {
            assert!(ConnectOptions { timeout }.validate().is_err());
        }
    }

    #[tokio::test]
    async fn given_a_listener_that_never_answers_when_connecting_then_should_time_out_at_the_deadline()
     {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local listener");
        let address = listener.local_addr().expect("read the bound address");
        // Accept and hold the socket without replying, the shape of a server that
        // completed the TCP handshake but never answers the login request.
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept the client");
            let mut buffer = [0_u8; 1024];
            while socket.read(&mut buffer).await.is_ok_and(|read| read > 0) {}
        });
        let client =
            IggyClientBuilder::from_connection_string(&format!("iggy+tcp://iggy:iggy@{address}"))
                .expect("parse the connection string")
                .build()
                .expect("build the client");
        let started = Instant::now();
        let result = connect_before(&client, started + Duration::from_millis(300)).await;
        assert!(matches!(result, Err(LaserError::Timeout(LOGIN_STAGE))));
        assert!(started.elapsed() < Duration::from_secs(5));
        server.abort();
    }

    #[tokio::test]
    async fn given_an_unreachable_server_when_connecting_then_should_stop_retrying_at_the_deadline()
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local listener");
        let address = listener.local_addr().expect("read the bound address");
        drop(listener);
        let client =
            IggyClientBuilder::from_connection_string(&format!("iggy+tcp://iggy:iggy@{address}"))
                .expect("parse the connection string")
                .build()
                .expect("build the client");
        let started = Instant::now();
        let result = connect_before(&client, started + Duration::from_millis(300)).await;
        assert!(matches!(result, Err(LaserError::Timeout(ACCEPT_STAGE))));
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
