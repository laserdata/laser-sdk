use crate::agent::{Session, Sessions};
use crate::context::ContextMessage;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::ConversationId;
use iggy::prelude::{
    Consumer, Identifier, MessageClient, PollOptions, PollingStrategy, TopicClient,
};
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
use laser_wire::validate::Validate;
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

    /// A page of this stream's sessions, newest created first. Chain the
    /// filters and the page and finish with
    /// [`fetch`](SessionListRequest::fetch).
    pub fn list(&self) -> SessionListRequest {
        SessionListRequest {
            sessions: self.clone(),
            status: None,
            root: None,
            label_prefix: None,
            agent: None,
            text: None,
            page: 0,
            results: 0,
            bound: None,
        }
    }

    /// A page of one session's events in server append time order. Chain the
    /// page and finish with [`fetch`](SessionEventsRequest::fetch).
    pub fn events(&self, id: ConversationId) -> SessionEventsRequest {
        SessionEventsRequest {
            sessions: self.clone(),
            id,
            page: 0,
            results: 0,
            bound: None,
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

    /// One page of this stream's change feed in sequence order. Page `page`
    /// of size `results` holds the changes with sequence
    /// `(page - 1) * results + 1 ..= page * results`, so appends and pruning
    /// never shift it. Zero `results` leaves the page size to the server, and
    /// the reply names the size it used.
    pub async fn changes(&self, page: u32, results: u32) -> Result<SessionChanges, LaserError> {
        let request = request::SessionChanges {
            stream: self.stream()?.to_owned(),
            page,
            results,
        };
        match self.read(AGDX_SESSION_CHANGES_CODE, &request).await? {
            SessionOutcome::Changes(changes) => Ok(changes),
            _ => Err(unexpected("changes")),
        }
    }

    /// Follow this stream's session changes from now, polling every
    /// `poll_every`, which must be above zero, and coalescing the changed
    /// session ids of each poll. The watch starts in one read at the newest
    /// change when this call returns, so a change made after it is never
    /// missed and no retained change is replayed.
    pub async fn watch(&self, poll_every: Duration) -> Result<SessionWatch, LaserError> {
        if poll_every.is_zero() {
            return Err(LaserError::Invalid(
                "session watch poll interval must be above zero".to_owned(),
            ));
        }
        let start = watch_start(|page, results| self.changes(page, results)).await?;
        Ok(SessionWatch {
            sessions: self.clone(),
            poll_every,
            next: start.total_results.saturating_add(1),
            results: start.results,
            series: start.series,
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
    page: u32,
    results: u32,
    bound: Option<(u64, u64)>,
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

    /// The 1-based page to read. Zero reads the first page.
    pub fn page(mut self, page: u32) -> Self {
        self.page = page;
        self
    }

    /// The sessions per page. Zero leaves it to the server.
    pub fn results(mut self, results: u32) -> Self {
        self.results = results;
        self
    }

    /// Read within the bound of a walk: the `as_of` and `series` its first
    /// page returned, so sessions folded after it stay out of every page.
    pub fn as_of(mut self, as_of: u64, series: u64) -> Self {
        self.bound = Some((as_of, series));
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
            page: self.page,
            results: self.results,
            as_of: self.bound.map(|(as_of, _)| as_of),
            series: self.bound.map(|(_, series)| series),
        };
        request.validate()?;
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
    page: u32,
    results: u32,
    bound: Option<(u64, u64)>,
}

impl SessionEventsRequest {
    /// The 1-based page to read. Zero reads the first page.
    pub fn page(mut self, page: u32) -> Self {
        self.page = page;
        self
    }

    /// The events per page. Zero leaves it to the server.
    pub fn results(mut self, results: u32) -> Self {
        self.results = results;
        self
    }

    /// Read within the bound of a walk: the `as_of` and `series` its first
    /// page returned, so events folded after it stay out of every page.
    pub fn as_of(mut self, as_of: u64, series: u64) -> Self {
        self.bound = Some((as_of, series));
        self
    }

    /// Read the page.
    pub async fn fetch(self) -> Result<SessionEventsPage, LaserError> {
        let request = request::SessionEvents {
            stream: self.sessions.stream()?.to_owned(),
            id: self.id.into(),
            page: self.page,
            results: self.results,
            as_of: self.bound.map(|(as_of, _)| as_of),
            series: self.bound.map(|(_, series)| series),
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
    /// The retained feed cannot identify every changed session. List the
    /// sessions again to catch up.
    Resync,
}

/// A follower of one stream's session changes. Build it with
/// [`Sessions::watch`].
pub struct SessionWatch {
    sessions: Sessions,
    poll_every: Duration,
    /// The next change sequence to report. The page to read follows from it.
    next: u64,
    results: u32,
    // Sequence numbers of another node's or a restored index never continue
    // this watch.
    series: u64,
    ready: VecDeque<SessionChange>,
}

impl SessionWatch {
    /// The next change, waiting until one lands.
    pub async fn next(&mut self) -> Result<SessionChange, LaserError> {
        loop {
            if let Some(change) = self.ready.pop_front() {
                return Ok(change);
            }
            let page =
                (self.next.saturating_sub(1) / u64::from(self.results.max(1))).saturating_add(1);
            let page = match self
                .sessions
                .changes(u32::try_from(page).unwrap_or(u32::MAX), self.results)
                .await
            {
                Ok(page) => page,
                // A node that has not folded this stream far enough yet,
                // typically the one a move just reached, answers Stale. The
                // watch waits for it as it waits for a new change.
                Err(error) if error.code() == crate::query::ResultCode::Stale => {
                    tokio::time::sleep(self.poll_every).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            self.results = page.results;
            // Another series numbers another index's changes, and a floor past
            // the next sequence means retention pruned changes not reported.
            if page.series != self.series || page.floor > self.next {
                self.series = page.series;
                self.next = page.total_results.saturating_add(1);
                return Ok(SessionChange::Resync);
            }
            let rows: Vec<_> = page
                .items
                .into_iter()
                .filter(|row| row.seq >= self.next)
                .collect();
            let Some(last) = rows.last().map(|row| row.seq) else {
                tokio::time::sleep(self.poll_every).await;
                continue;
            };
            self.next = last.saturating_add(1);
            // A truncated row does not name every changed session. The
            // caller lists again after the resync, which covers these rows.
            if rows.iter().any(|row| row.truncated) {
                return Ok(SessionChange::Resync);
            }
            let mut seen = BTreeSet::new();
            let mut changed = Vec::new();
            for row in &rows {
                for session in &row.sessions {
                    if seen.insert(*session) {
                        changed.push(*session);
                    }
                }
            }
            self.ready.push_back(SessionChange::Changed(changed));
        }
    }

    /// This watch as a [`Stream`](futures::Stream) of changes. It follows
    /// changes until the caller drops it and ends after the first failed read.
    /// Drive it with `futures::StreamExt`.
    pub fn stream(self) -> impl futures::Stream<Item = Result<SessionChange, LaserError>> {
        futures::stream::unfold(Some(self), |watch| async move {
            let mut watch = watch?;
            match watch.next().await {
                Ok(change) => Some((Ok(change), Some(watch))),
                Err(error) => Some((Err(error), None)),
            }
        })
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
                        *partition,
                        &consumer,
                        &PollingStrategy::offset(*offset),
                        PollOptions {
                            count: 1,
                            ..PollOptions::default()
                        },
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

/// The one-call start of a change follower: any page names the series, the
/// page size and the newest sequence, and the follower starts after it.
async fn watch_start<F, Fut>(changes: F) -> Result<SessionChanges, LaserError>
where
    F: FnOnce(u32, u32) -> Fut,
    Fut: std::future::Future<Output = Result<SessionChanges, LaserError>>,
{
    changes(1, 0).await
}

fn unexpected(read: &str) -> LaserError {
    LaserError::Protocol(format!("session {read}: unexpected reply shape"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iggy::prelude::IggyClient;
    use futures::StreamExt;
    use laser_wire::agent::ConversationId as WireConversationId;
    use laser_wire::limits::MAX_SESSION_TEXT_QUERY_BYTES;
    use laser_wire::session::{SessionChangeOrigin, SessionChangeRow};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn given_a_session_list_over_its_caps_when_fetched_then_should_refuse_it_before_sending()
    {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("agents");
        let error = laser
            .sessions()
            .list()
            .text("t".repeat(MAX_SESSION_TEXT_QUERY_BYTES + 1))
            .fetch()
            .await
            .expect_err("an over-long text filter is refused");
        assert!(matches!(error, LaserError::Invalid(_)), "{error:?}");
        let error = laser
            .sessions()
            .list()
            .fetch()
            .await
            .expect_err("an offline Laser does not index sessions");
        assert!(matches!(error, LaserError::Unsupported { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn given_a_zero_poll_interval_when_watching_then_should_refuse_it_before_reading() {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("agents");
        let error = laser
            .sessions()
            .watch(Duration::ZERO)
            .await
            .err()
            .expect("a zero poll interval is refused");
        assert!(matches!(error, LaserError::Invalid(_)), "{error:?}");
    }

    #[tokio::test]
    async fn given_retained_change_rows_when_a_watch_starts_then_should_start_in_one_read_without_draining_them()
     {
        let id = WireConversationId::from_u128(7);
        let row = |seq| SessionChangeRow {
            seq,
            sessions: vec![id],
            positions: Vec::new(),
            truncated: false,
            origin: SessionChangeOrigin::Records,
        };
        let replies = Mutex::new(VecDeque::from([SessionChanges {
            items: vec![row(1), row(2)],
            page: 1,
            total_pages: 441,
            total_results: 4405,
            results: 10,
            floor: 1,
            series: 77,
        }]));
        let calls = Mutex::new(Vec::new());
        let start = watch_start(|page, results| {
            calls.lock().expect("calls lock").push((page, results));
            let reply = replies.lock().expect("replies lock").pop_front();
            async move { reply.ok_or_else(|| LaserError::Invalid("no scripted reply".to_owned())) }
        })
        .await
        .expect("the watch starts");
        assert_eq!(
            (start.total_results, start.results, start.series),
            (4405, 10, 77)
        );
        assert_eq!(*calls.lock().expect("calls lock"), vec![(1, 0)]);
    }

    #[tokio::test]
    async fn given_a_watch_when_iterated_as_a_stream_then_should_yield_changes_and_end_after_a_failed_read()
     {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("agents");
        let id = WireConversationId::from_u128(7);
        let watch = SessionWatch {
            sessions: laser.sessions(),
            poll_every: Duration::from_millis(1),
            next: 1,
            results: 10,
            series: 77,
            ready: VecDeque::from([SessionChange::Changed(vec![id])]),
        };
        let mut stream = std::pin::pin!(watch.stream());
        let first = stream
            .next()
            .await
            .expect("a buffered change")
            .expect("the buffered change reads");
        assert_eq!(first, SessionChange::Changed(vec![id]));
        let failed = stream.next().await.expect("the failed read is yielded");
        assert!(
            matches!(failed, Err(LaserError::Unsupported { .. })),
            "{failed:?}"
        );
        assert!(stream.next().await.is_none());
    }

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
