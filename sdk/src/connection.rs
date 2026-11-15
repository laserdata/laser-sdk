use futures::Stream;
use iggy_common::DiagnosticEvent;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A change in the state of the connection a [`crate::laser::Laser`] holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionEvent {
    /// The client reached a server.
    Connected,
    /// The client lost its last link to the servers. It reconnects by the policy of the connection string.
    Disconnected,
    /// The client signed in, or resumed its session after a reconnect.
    SignedIn,
    /// The client signed out.
    SignedOut,
    /// The connection was closed for good. The stream ends after this event.
    Shutdown,
}

impl From<DiagnosticEvent> for ConnectionEvent {
    fn from(event: DiagnosticEvent) -> Self {
        match event {
            DiagnosticEvent::Connected => Self::Connected,
            DiagnosticEvent::Disconnected => Self::Disconnected,
            DiagnosticEvent::SignedIn => Self::SignedIn,
            DiagnosticEvent::SignedOut => Self::SignedOut,
            DiagnosticEvent::Shutdown => Self::Shutdown,
        }
    }
}

impl std::fmt::Display for ConnectionEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::SignedIn => "signed_in",
            Self::SignedOut => "signed_out",
            Self::Shutdown => "shutdown",
        })
    }
}

/// The [`ConnectionEvent`]s of one connection from the moment of the subscription, from
/// [`crate::laser::Laser::connection_events`]. A subscriber that falls behind loses its
/// oldest events. The stream ends after [`ConnectionEvent::Shutdown`] or when the client is dropped.
pub struct ConnectionEvents {
    events: Pin<Box<dyn Stream<Item = DiagnosticEvent> + Send>>,
    ended: bool,
}

impl ConnectionEvents {
    pub(crate) fn new(events: impl Stream<Item = DiagnosticEvent> + Send + 'static) -> Self {
        Self {
            events: Box::pin(events),
            ended: false,
        }
    }

    /// Wait for the next event. `None` once the stream ended.
    pub async fn next(&mut self) -> Option<ConnectionEvent> {
        futures::StreamExt::next(self).await
    }
}

impl Stream for ConnectionEvents {
    type Item = ConnectionEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let event =
            std::task::ready!(self.events.as_mut().poll_next(cx)).map(ConnectionEvent::from);
        self.ended = matches!(event, None | Some(ConnectionEvent::Shutdown));
        Poll::Ready(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn given_diagnostic_events_when_streamed_then_should_map_each_and_end_after_shutdown() {
        let events = ConnectionEvents::new(futures::stream::iter([
            DiagnosticEvent::Connected,
            DiagnosticEvent::SignedIn,
            DiagnosticEvent::Disconnected,
            DiagnosticEvent::SignedOut,
            DiagnosticEvent::Shutdown,
            DiagnosticEvent::Connected,
        ]));

        assert_eq!(
            events.collect::<Vec<_>>().await,
            [
                ConnectionEvent::Connected,
                ConnectionEvent::SignedIn,
                ConnectionEvent::Disconnected,
                ConnectionEvent::SignedOut,
                ConnectionEvent::Shutdown,
            ]
        );
    }

    #[tokio::test]
    async fn given_a_closed_channel_when_streamed_then_should_end() {
        let mut events = ConnectionEvents::new(futures::stream::empty());

        assert_eq!(events.next().await, None);
        assert_eq!(events.next().await, None);
    }

    #[test]
    fn given_each_event_when_displayed_then_should_spell_the_shared_name() {
        assert_eq!(
            [
                ConnectionEvent::Connected,
                ConnectionEvent::Disconnected,
                ConnectionEvent::SignedIn,
                ConnectionEvent::SignedOut,
                ConnectionEvent::Shutdown,
            ]
            .map(|event| event.to_string()),
            [
                "connected",
                "disconnected",
                "signed_in",
                "signed_out",
                "shutdown"
            ]
        );
    }
}
