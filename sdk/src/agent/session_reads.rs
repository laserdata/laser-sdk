use crate::agent::{Session, Sessions};
use crate::context::ContextMessage;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::ConversationId;
use iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy, TopicClient};
use laser_wire::codes::{
    AGDX_SESSION_CHANGES_CODE, AGDX_SESSION_EVENTS_CODE, AGDX_SESSION_GET_CODE,
    AGDX_SESSION_LINKS_CODE, AGDX_SESSION_LIST_CODE, AGDX_SESSION_SOURCES_CODE,
    AGDX_SESSION_STATE_CODE,
};
use laser_wire::graph::SourceRef;
use laser_wire::session::request;
use laser_wire::session::{
    LinkSurface, SessionChanges, SessionEventsPage, SessionInfo, SessionLinksView, SessionOutcome,
    SessionPage, SessionReply, SessionSources, SessionStateView,
};
use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

impl Sessions {
    /// One session's summary from the deployment's session index.
    pub async fn get(&self, id: ConversationId) -> Result<SessionInfo, LaserError> {
        let request = request::SessionGet {
            stream: self.stream()?.to_owned(),
            id: id.into(),
        };
        match self.read(AGDX_SESSION_GET_CODE, &request).await? {
            SessionOutcome::Info(info) => Ok(*info),
            _ => Err(unexpected("get")),
        }
    }

    /// A page of this stream's sessions, newest first. Chain the filters and
    /// finish with [`fetch`](SessionListRequest::fetch).
    pub fn list(&self) -> SessionListRequest {
        SessionListRequest {
            sessions: self.clone(),
            status: None,
            root: None,
            label_prefix: None,
            agent: None,
            text: None,
            cursor: None,
            limit: 0,
            total: false,
        }
    }

    /// A page of one session's events in broker time order. Chain the paging
    /// and finish with [`fetch`](SessionEventsRequest::fetch).
    pub fn events(&self, id: ConversationId) -> SessionEventsRequest {
        SessionEventsRequest {
            sessions: self.clone(),
            id,
            cursor: None,
            limit: 0,
            fixed_frontier: false,
        }
    }

    /// One session's folded state document with up to `history_limit` history
    /// rows. Zero leaves the history size to the server.
    pub async fn state(
        &self,
        id: ConversationId,
        history_limit: u32,
    ) -> Result<SessionStateView, LaserError> {
        let request = request::SessionState {
            stream: self.stream()?.to_owned(),
            id: id.into(),
            history_limit,
        };
        match self.read(AGDX_SESSION_STATE_CODE, &request).await? {
            SessionOutcome::State(view) => Ok(view),
            _ => Err(unexpected("state")),
        }
    }

    /// The resources one session wrote, recalled, or touched, narrowed to
    /// `surface` when given.
    pub async fn links(
        &self,
        id: ConversationId,
        surface: Option<LinkSurface>,
    ) -> Result<SessionLinksView, LaserError> {
        let request = request::SessionLinks {
            stream: self.stream()?.to_owned(),
            id: id.into(),
            surface,
        };
        match self.read(AGDX_SESSION_LINKS_CODE, &request).await? {
            SessionOutcome::Links(view) => Ok(view),
            _ => Err(unexpected("links")),
        }
    }

    /// The source partitions that hold one session's records.
    pub async fn sources(&self, id: ConversationId) -> Result<SessionSources, LaserError> {
        let request = request::SessionSources {
            stream: self.stream()?.to_owned(),
            id: id.into(),
            lane_only: false,
        };
        match self.read(AGDX_SESSION_SOURCES_CODE, &request).await? {
            SessionOutcome::Sources(sources) => Ok(sources),
            _ => Err(unexpected("sources")),
        }
    }

    pub(crate) async fn registered_lane(
        &self,
        id: ConversationId,
    ) -> Result<Option<(u32, u64, u32)>, LaserError> {
        let request = request::SessionSources {
            stream: self.stream()?.to_owned(),
            id: id.into(),
            lane_only: true,
        };
        match self.read(AGDX_SESSION_SOURCES_CODE, &request).await? {
            SessionOutcome::Sources(view) => Ok(view.lane),
            _ => Err(unexpected("sources")),
        }
    }

    /// The change rows of this stream after `after`, up to `limit`. Zero
    /// leaves the page size to the server.
    pub async fn changes(&self, after: u64, limit: u32) -> Result<SessionChanges, LaserError> {
        let request = request::SessionChanges {
            stream: self.stream()?.to_owned(),
            after,
            limit,
        };
        match self.read(AGDX_SESSION_CHANGES_CODE, &request).await? {
            SessionOutcome::Changes(changes) => Ok(changes),
            _ => Err(unexpected("changes")),
        }
    }

    /// Follow this stream's session changes from now, polling every
    /// `poll_every` and coalescing the changed session ids of each poll. The
    /// watch starts at the change rows retained when this call returns, so a
    /// change made after it is never missed.
    pub async fn watch(&self, poll_every: Duration) -> Result<SessionWatch, LaserError> {
        let mut after = 0;
        loop {
            let page = self.changes(after, 0).await?;
            match page.rows.iter().map(|row| row.seq).max() {
                Some(seq) => after = seq,
                None => break,
            }
        }
        Ok(SessionWatch {
            sessions: self.clone(),
            poll_every,
            after,
            ready: VecDeque::new(),
        })
    }

    async fn read<T: serde::Serialize>(
        &self,
        code: u32,
        request: &T,
    ) -> Result<SessionOutcome, LaserError> {
        let laser = self.laser();
        if !laser.capabilities().await.sessions {
            return Err(LaserError::unsupported_feature(
                "sessions",
                "sessions",
                "session reads need a deployment that indexes sessions",
            ));
        }
        let payload = laser_wire::framing::encode_named(request)?;
        let reply = laser.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<SessionReply>(&reply)? {
            SessionReply::Ok(outcome) => Ok(outcome),
            SessionReply::Err(error) => Err(error.into()),
            _ => Err(unexpected("reply")),
        }
    }
}

impl Session {
    /// This session's summary from the deployment's session index: status,
    /// participants, counters, and the derived idle and over-budget flags.
    pub async fn status(&self) -> Result<SessionInfo, LaserError> {
        self.laser.sessions().get(self.conversation()).await
    }
}

/// A session list read. Build it with [`Sessions::list`].
#[must_use = "a session list does nothing until fetch().await"]
pub struct SessionListRequest {
    sessions: Sessions,
    status: Option<laser_wire::agent::SessionStatus>,
    root: Option<ConversationId>,
    label_prefix: Option<String>,
    agent: Option<laser_wire::agent::AgentId>,
    text: Option<String>,
    cursor: Option<String>,
    limit: u32,
    total: bool,
}

impl SessionListRequest {
    /// Only sessions in `status`.
    pub fn status(mut self, status: laser_wire::agent::SessionStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Only sessions in the tree rooted at `root`.
    pub fn root(mut self, root: ConversationId) -> Self {
        self.root = Some(root);
        self
    }

    /// Only sessions whose label starts with `prefix`.
    pub fn label_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.label_prefix = Some(prefix.into());
        self
    }

    /// Only sessions `agent` took part in.
    pub fn agent(mut self, agent: impl Into<laser_wire::agent::AgentId>) -> Self {
        let agent = agent.into();
        self.agent = Some(agent);
        self
    }

    /// Only sessions whose label or id contains `text`.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// Continue after the page that returned `cursor`.
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// The page size. Zero leaves it to the server.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Also count every match.
    pub fn total(mut self) -> Self {
        self.total = true;
        self
    }

    /// Read the page.
    pub async fn fetch(self) -> Result<SessionPage, LaserError> {
        let request = request::SessionList {
            stream: self.sessions.stream()?.to_owned(),
            status: self.status,
            root: self.root.map(Into::into),
            label_prefix: self.label_prefix,
            agent: self.agent,
            text: self.text,
            cursor: self.cursor,
            limit: self.limit,
            want_total: self.total,
        };
        match self.sessions.read(AGDX_SESSION_LIST_CODE, &request).await? {
            SessionOutcome::Page(page) => Ok(page),
            _ => Err(unexpected("list")),
        }
    }
}

/// A session events read. Build it with [`Sessions::events`].
#[must_use = "a session events read does nothing until fetch().await"]
pub struct SessionEventsRequest {
    sessions: Sessions,
    id: ConversationId,
    cursor: Option<String>,
    limit: u32,
    fixed_frontier: bool,
}

impl SessionEventsRequest {
    /// Continue after the page that returned `cursor`.
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// The page size. Zero leaves it to the server.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Pin the fold frontier of the first page, for a historical walk.
    pub fn fixed_frontier(mut self) -> Self {
        self.fixed_frontier = true;
        self
    }

    /// Read the page.
    pub async fn fetch(self) -> Result<SessionEventsPage, LaserError> {
        let request = request::SessionEvents {
            stream: self.sessions.stream()?.to_owned(),
            id: self.id.into(),
            cursor: self.cursor,
            limit: self.limit,
            fixed_frontier: self.fixed_frontier,
        };
        match self
            .sessions
            .read(AGDX_SESSION_EVENTS_CODE, &request)
            .await?
        {
            SessionOutcome::Events(page) => Ok(page),
            _ => Err(unexpected("events")),
        }
    }
}

/// What a [`SessionWatch`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionChange {
    /// These sessions changed, each named once.
    Changed(Vec<laser_wire::agent::ConversationId>),
    /// The watch fell below the retained change floor. List the sessions
    /// again to catch up.
    Resync,
}

/// A follower of one stream's session changes. Build it with
/// [`Sessions::watch`].
pub struct SessionWatch {
    sessions: Sessions,
    poll_every: Duration,
    after: u64,
    ready: VecDeque<SessionChange>,
}

impl SessionWatch {
    /// The next change, waiting until one lands.
    pub async fn next(&mut self) -> Result<SessionChange, LaserError> {
        loop {
            if let Some(change) = self.ready.pop_front() {
                return Ok(change);
            }
            let after = self.after;
            let page = self.sessions.changes(after, 0).await?;
            if page.resync {
                self.after = page.floor;
                return Ok(SessionChange::Resync);
            }
            if page.rows.is_empty() {
                tokio::time::sleep(self.poll_every).await;
                continue;
            }
            let mut seen = BTreeSet::new();
            let mut changed = Vec::new();
            for row in &page.rows {
                self.after = self.after.max(row.seq);
                for session in &row.sessions {
                    if seen.insert(*session) {
                        changed.push(*session);
                    }
                }
            }
            self.ready.push_back(SessionChange::Changed(changed));
        }
    }
}

impl Laser {
    /// Read the one record `at` names, checking that the topic still has the
    /// generation the reference recorded and that the returned record sits at
    /// the named offset. `None` when the record is gone or the topic was
    /// recreated. Only a message reference names a log record.
    pub async fn read_at(&self, at: &SourceRef) -> Result<Option<ContextMessage>, LaserError> {
        let SourceRef::Message {
            stream,
            topic,
            partition,
            offset,
            generation,
            ..
        } = at
        else {
            return Err(LaserError::Invalid(
                "only a message reference names a log record".to_owned(),
            ));
        };
        let client = self.client();
        let stream_id = Identifier::numeric(*stream)?;
        let topic_id = Identifier::numeric(*topic)?;
        let consumer = Consumer::new(Identifier::named("laser-read-at")?);
        let Some((source, message)) = read_at_checked(
            *topic,
            *generation,
            || async {
                Ok(client
                    .get_topic(&stream_id, &topic_id)
                    .await?
                    .map(|details| ReadAtSource {
                        id: details.id,
                        generation: details.created_at.as_micros(),
                        name: details.name,
                    }))
            },
            || async {
                let polled = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(*partition),
                        &consumer,
                        &PollingStrategy::offset(*offset),
                        1,
                        false,
                    )
                    .await?;
                Ok(polled
                    .messages
                    .into_iter()
                    .find(|message| message.header.offset == *offset))
            },
        )
        .await?
        else {
            return Ok(None);
        };
        let (provenance, envelope) = crate::agent::provenance_and_envelope(&message)?;
        Ok(Some(ContextMessage {
            id: crate::types::MessageId::new(*partition, *offset),
            provenance,
            payload: message.payload.to_vec(),
            envelope,
            topic: source.name,
            timestamp_micros: message.header.timestamp,
            stream_id: *stream,
            topic_id: *topic,
        }))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReadAtSource {
    id: u32,
    generation: u64,
    name: String,
}

async fn read_at_checked<T, M, MF, P, PF>(
    topic: u32,
    generation: Option<u64>,
    mut metadata: M,
    poll: P,
) -> Result<Option<(ReadAtSource, T)>, LaserError>
where
    M: FnMut() -> MF,
    MF: std::future::Future<Output = Result<Option<ReadAtSource>, LaserError>>,
    P: FnOnce() -> PF,
    PF: std::future::Future<Output = Result<Option<T>, LaserError>>,
{
    let Some(source) = metadata().await? else {
        return Ok(None);
    };
    if source.id != topic || generation.is_some_and(|generation| generation != source.generation) {
        return Ok(None);
    }
    let Some(message) = poll().await? else {
        return Ok(None);
    };
    if metadata().await?.as_ref() != Some(&source) {
        return Ok(None);
    }
    Ok(Some((source, message)))
}

fn unexpected(read: &str) -> LaserError {
    LaserError::Protocol(format!("session {read}: unexpected reply shape"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn given_a_source_recreated_during_read_at_when_polled_then_should_discard_replacement_bytes()
     {
        for expected in [None, Some(10)] {
            let generation = Arc::new(AtomicU64::new(10));
            let result = read_at_checked(
                2,
                expected,
                || {
                    let generation = generation.load(Ordering::SeqCst);
                    async move {
                        Ok(Some(ReadAtSource {
                            id: 2,
                            generation,
                            name: "agent.sessions".to_owned(),
                        }))
                    }
                },
                || async {
                    generation.store(11, Ordering::SeqCst);
                    Ok(Some(b"replacement"))
                },
            )
            .await
            .expect("read finishes");
            assert!(
                result.is_none(),
                "a reused offset is not the original record"
            );
        }
    }

    #[tokio::test]
    async fn given_a_source_deleted_during_read_at_when_polled_then_should_discard_the_record() {
        let generation = AtomicU64::new(10);
        let result = read_at_checked(
            2,
            Some(10),
            || {
                let exists = generation.load(Ordering::SeqCst) != 0;
                async move {
                    Ok(exists.then(|| ReadAtSource {
                        id: 2,
                        generation: 10,
                        name: "agent.sessions".to_owned(),
                    }))
                }
            },
            || async {
                generation.store(0, Ordering::SeqCst);
                Ok(Some(b"removed"))
            },
        )
        .await
        .expect("read finishes");
        assert!(result.is_none(), "a removed source has no retained record");
    }

    #[tokio::test]
    async fn given_a_live_source_when_read_at_then_should_return_the_original_record() {
        for expected in [None, Some(10)] {
            let result = read_at_checked(
                2,
                expected,
                || async {
                    Ok(Some(ReadAtSource {
                        id: 2,
                        generation: 10,
                        name: "agent.sessions".to_owned(),
                    }))
                },
                || async { Ok(Some(b"original")) },
            )
            .await
            .expect("read finishes")
            .expect("the source still exists");
            assert_eq!(result.1, b"original");
        }
    }

    #[tokio::test]
    async fn given_a_stale_source_when_read_at_then_should_refuse_before_polling() {
        let result = read_at_checked(
            2,
            Some(9),
            || async {
                Ok(Some(ReadAtSource {
                    id: 2,
                    generation: 10,
                    name: "agent.sessions".to_owned(),
                }))
            },
            || async { Err::<Option<()>, _>(LaserError::Invalid("unexpected poll".to_owned())) },
        )
        .await
        .expect("read finishes");
        assert!(result.is_none());
    }
}
