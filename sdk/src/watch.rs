use crate::cursor::Cursor;
use crate::error::LaserError;
use crate::iggy::prelude::{Consumer, Identifier, TopicClient};
use crate::laser::Laser;
use crate::types::MessageId;
use laser_wire::change::ChangeRecord;
use laser_wire::codes::CHANGE_OP_VERSION;
use laser_wire::framing::decode_named;
use laser_wire::result::{CommandError, ResultCode};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::{Instant, sleep};

// How often a wait reads the feed again while nothing new landed.
const WAIT_INTERVAL: Duration = Duration::from_millis(100);

impl Laser {
    /// The change-feed accessor: [`ChangeRecord`]s the managed backend publishes after
    /// each committed projector batch for a binding that opted into `notify`.
    /// "Query after my data landed" becomes await-then-query instead of
    /// sleep-and-retry, and it composes with read-your-writes rather than
    /// replacing it. Gated on the `WATCH` capability: refused typed when the
    /// deployment does not publish the feed, so a consumer can never wait on
    /// a topic nothing writes to.
    pub fn watch(&self) -> Watch {
        Watch {
            laser: self.clone(),
            index: None,
            from_end: false,
        }
    }
}

/// A fluent change-feed read. Narrow with [`index`](Self::index), choose the
/// start with [`from_end`](Self::from_end), then open the reader with
/// [`records`](Self::records). Build it with [`Laser::watch`].
pub struct Watch {
    laser: Laser,
    index: Option<String>,
    from_end: bool,
}

impl Watch {
    /// Keep only advancements of this materialized index, named as
    /// [`Laser::resource_name`] names it.
    #[must_use]
    pub fn index(mut self, index: impl Into<String>) -> Self {
        self.index = Some(self.laser.resource_name(&index.into()));
        self
    }

    /// Start at the feed's current end, so the reader receives only changes
    /// published after it opens. Without it a reader starts at the first
    /// retained change.
    #[must_use]
    pub fn from_end(mut self) -> Self {
        self.from_end = true;
        self
    }

    /// Open the feed reader over the changes topic on the ops stream. The
    /// capabilities are read again, so a deployment that began publishing the
    /// feed after this client connected is seen.
    ///
    /// # Errors
    ///
    /// [`LaserError::Unsupported`] when the deployment does not publish the
    /// change feed.
    pub async fn records(self) -> Result<WatchReader, LaserError> {
        let capabilities = self.laser.capabilities().await;
        if !capabilities.watch {
            return Err(LaserError::unsupported_feature(
                "watch",
                "watch",
                "the change feed is not published by this deployment",
            ));
        }
        let ops_stream = self.laser.ops_stream();
        // Under stream tenancy each stream's change feed rides its own ops
        // topic, so a scoped handle reads its stream's topic.
        let changes_topic = match self.laser.resource_stream() {
            Some(stream) if capabilities.stream_tenancy => {
                laser_wire::topics::stream_ops_topic(stream, laser_wire::topics::CHANGES_TOPIC)
            }
            _ => self.laser.changes_topic(),
        };
        let cursor = self.laser.reader_on(&ops_stream, &changes_topic)?;
        let mut reader = WatchReader {
            laser: self.laser,
            ops_stream,
            changes_topic,
            cursor,
            index: self.index,
            unanchored: BTreeSet::new(),
            missed: 0,
            skipped: 0,
        };
        if self.from_end {
            reader.start_at_end().await?;
        }
        Ok(reader)
    }
}

/// One change-feed record with its place on the feed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WatchedChange {
    pub change: ChangeRecord,
    /// The feed record's message id. A retried batch publishes a change again
    /// under the same id, so a reader drops a copy it has seen.
    pub message_id: u128,
    /// Where the record sits on the change feed.
    pub position: MessageId,
}

/// A resumable change-feed reader. Each [`poll`](Self::poll) drains the
/// changes that landed since the last one. Persist [`offsets`](Self::offsets)
/// and seed a fresh reader to resume across restarts, exactly like any
/// [`Cursor`].
pub struct WatchReader {
    laser: Laser,
    ops_stream: String,
    changes_topic: String,
    cursor: Cursor,
    index: Option<String>,
    // Partitions the reader started at the end of while they held nothing,
    // so the first record they get says nothing about records it missed.
    unanchored: BTreeSet<u32>,
    missed: u64,
    skipped: u64,
}

impl WatchReader {
    /// Resume from a previous reader's offsets.
    #[must_use]
    pub fn from_offsets(mut self, offsets: Vec<u64>) -> Self {
        self.cursor = self.cursor.from_offsets(offsets);
        self.unanchored.clear();
        self
    }

    /// The per-partition offsets consumed so far, the resume state.
    pub fn offsets(&self) -> &[u64] {
        self.cursor.offsets()
    }

    /// Feed records retention removed before this reader reached them,
    /// counted from the offsets it started or resumed at. Non-zero means
    /// changes were missed: read the view again instead of trusting the feed.
    pub fn missed(&self) -> u64 {
        self.missed
    }

    /// Feed records that were not change records and were skipped: the
    /// changes topic is overridable ([`Laser::with_changes_topic`]), and other
    /// traffic on it must not wedge the reader.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// Drain the changes that landed since the last poll, filtered to the
    /// watched index when one was set. A change record of another version
    /// stops its partition there: the changes before it are returned, and a
    /// poll with nothing else to return fails, so this build never misreads
    /// a newer record.
    ///
    /// # Errors
    ///
    /// A [`LaserError::Command`] with code
    /// [`VersionSkew`](laser_wire::result::ResultCode::VersionSkew) for a
    /// record of another version, or the read failure.
    pub async fn poll(&mut self) -> Result<Vec<WatchedChange>, LaserError> {
        let resumed = self.cursor.offsets().to_vec();
        let messages = self.cursor.poll_iggy().await?;
        let mut offsets = self.cursor.offsets().to_vec();
        let mut seen = BTreeSet::new();
        let mut stopped = BTreeMap::new();
        let mut changes = Vec::new();
        for (partition_id, message) in messages {
            if stopped.contains_key(&partition_id) {
                continue;
            }
            let offset = message.header.offset;
            if seen.insert(partition_id) && !self.unanchored.remove(&partition_id) {
                let expected = resumed.get(partition_id as usize).copied().unwrap_or(0);
                self.missed += offset.saturating_sub(expected);
            }
            let Ok(FeedVersion { v }) = decode_named::<FeedVersion>(&message.payload) else {
                self.skipped += 1;
                continue;
            };
            if v != CHANGE_OP_VERSION {
                stopped.insert(partition_id, v);
                if let Some(next) = offsets.get_mut(partition_id as usize) {
                    *next = offset;
                }
                continue;
            }
            let Ok(change) = decode_named::<ChangeRecord>(&message.payload) else {
                self.skipped += 1;
                continue;
            };
            if self
                .index
                .as_deref()
                .is_some_and(|index| change.index != index)
            {
                continue;
            }
            changes.push(WatchedChange {
                change,
                message_id: message.header.id,
                position: MessageId::new(partition_id, offset),
            });
        }
        if !stopped.is_empty() {
            self.cursor = self
                .laser
                .reader_on(&self.ops_stream, &self.changes_topic)?
                .from_offsets(offsets);
        }
        match stopped.values().next() {
            Some(version) if changes.is_empty() => Err(CommandError::new(
                ResultCode::VersionSkew,
                format!(
                    "change record version {version} is not the version {CHANGE_OP_VERSION} this build reads"
                ),
            )
            .into()),
            _ => Ok(changes),
        }
    }

    /// [`poll`](Self::poll), waiting up to `timeout` for the first change to
    /// land. Returns an empty batch when none landed in time.
    ///
    /// # Errors
    ///
    /// The same as [`poll`](Self::poll).
    pub async fn poll_within(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<WatchedChange>, LaserError> {
        let deadline = Instant::now() + timeout;
        loop {
            let changes = self.poll().await?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !changes.is_empty() || remaining.is_zero() {
                return Ok(changes);
            }
            sleep(WAIT_INTERVAL.min(remaining)).await;
        }
    }

    /// This reader as a [`Stream`](futures::Stream) of changes, one at a time,
    /// draining what landed since the last read and ending once caught up
    /// (the same shape as the async reader in the Python binding). Drive it
    /// with `futures::StreamExt`.
    pub fn stream(self) -> impl futures::Stream<Item = Result<WatchedChange, LaserError>> {
        futures::stream::unfold(
            (self, std::collections::VecDeque::new(), false),
            |(mut reader, mut buffered, stop)| async move {
                if stop {
                    return None;
                }
                loop {
                    if let Some(change) = buffered.pop_front() {
                        return Some((Ok(change), (reader, buffered, false)));
                    }
                    match reader.poll().await {
                        Ok(batch) if batch.is_empty() => return None,
                        Ok(batch) => buffered.extend(batch),
                        Err(error) => return Some((Err(error), (reader, buffered, true))),
                    }
                }
            },
        )
    }

    // Seed every partition at the offset it writes next.
    async fn start_at_end(&mut self) -> Result<(), LaserError> {
        let client = self.laser.client();
        let stream = Identifier::named(&self.ops_stream)?;
        let topic = Identifier::named(&self.changes_topic)?;
        let Some(details) = client.get_topic(&stream, &topic).await? else {
            return Ok(());
        };
        let consumer = Consumer::new(Identifier::named("laser-watch")?);
        let partitions = crate::poll::bounded_partitions(details.partitions_count);
        let mut offsets = Vec::with_capacity(partitions as usize);
        for partition in 0..partitions {
            let tail =
                crate::poll::current_tail_offset(&client, &stream, &topic, &consumer, partition)
                    .await?;
            if tail == 0 {
                self.unanchored.insert(partition);
            }
            offsets.push(tail);
        }
        self.cursor = self
            .laser
            .reader_on(&self.ops_stream, &self.changes_topic)?
            .from_offsets(offsets);
        Ok(())
    }
}

// The version a change-feed record names. A payload without one is no
// change record.
#[derive(Deserialize)]
struct FeedVersion {
    v: u32,
}

#[cfg(test)]
mod tests {
    use crate::capabilities::Capabilities;
    use crate::iggy::prelude::*;
    use crate::laser::Laser;
    use crate::types::MessageId;
    use axum::extract::{Query, State};
    use axum::routing::get;
    use axum::{Json, Router};
    use iggy::http::http_client::HttpClient;
    use laser_wire::change::ChangeRecord;
    use laser_wire::codes::CHANGE_OP_VERSION;
    use laser_wire::framing::encode_named;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[test]
    fn given_a_default_stream_when_watching_an_index_then_should_match_the_scoped_name() {
        let laser = Laser::from_client(crate::iggy::prelude::IggyClient::default())
            .with_default_stream("acme");
        let watch = laser.watch().index("readings");
        assert_eq!(watch.index.as_deref(), Some("stream:acme/readings"));
    }

    #[tokio::test]
    async fn given_a_change_record_of_another_version_when_polled_then_should_refuse_it_as_version_skew()
     {
        let feed = Feed::start(1).await;
        feed.append(0, 7, &change("readings", 0, CHANGE_OP_VERSION + 1));
        let mut reader = feed.laser().watch().records().await.expect("reader");

        let error = reader.poll().await.expect_err("a newer record is refused");

        assert!(error.is_version_skew(), "{error}");
        assert_eq!(
            reader.offsets(),
            &[0],
            "the reader stays at the newer record"
        );
    }

    #[tokio::test]
    async fn given_changes_before_a_newer_record_when_polled_then_should_return_them_and_stop_there()
     {
        let feed = Feed::start(1).await;
        feed.append(0, 7, &change("readings", 0, CHANGE_OP_VERSION));
        feed.append(0, 8, &change("readings", 1, CHANGE_OP_VERSION + 1));
        let mut reader = feed.laser().watch().records().await.expect("reader");

        let first = reader
            .poll()
            .await
            .expect("the change before the newer one");

        assert_eq!(first.len(), 1);
        assert_eq!(reader.offsets(), &[1]);
        assert!(
            reader
                .poll()
                .await
                .expect_err("the newer record")
                .is_version_skew()
        );
    }

    #[tokio::test]
    async fn given_a_change_when_polled_then_should_carry_its_message_id_and_feed_position() {
        let feed = Feed::start(1).await;
        feed.append(0, 42, &change("readings", 9, CHANGE_OP_VERSION));
        let mut reader = feed.laser().watch().records().await.expect("reader");

        let changes = reader.poll().await.expect("poll");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].message_id, 42);
        assert_eq!(changes[0].position, MessageId::new(0, 0));
        assert_eq!(changes[0].change.to_offset, 9);
    }

    #[tokio::test]
    async fn given_other_traffic_and_other_indexes_when_polled_then_should_count_only_the_traffic_as_skipped()
     {
        let feed = Feed::start(1).await;
        feed.append(0, 1, b"not a change record");
        feed.append(0, 2, &change("hosts", 0, CHANGE_OP_VERSION));
        feed.append(0, 3, &change("readings", 0, CHANGE_OP_VERSION));
        let mut reader = feed
            .laser()
            .watch()
            .index("readings")
            .records()
            .await
            .expect("reader");

        let changes = reader.poll().await.expect("poll");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].message_id, 3);
        assert_eq!(reader.skipped(), 1);
    }

    #[tokio::test]
    async fn given_records_retention_removed_when_a_resumed_reader_polls_then_should_count_them_as_missed()
     {
        let feed = Feed::start(1).await;
        for to_offset in 0..4 {
            feed.append(0, u128::from(to_offset), &change("readings", to_offset, 1));
        }
        feed.expire_below(0, 3);
        let mut reader = feed
            .laser()
            .watch()
            .records()
            .await
            .expect("reader")
            .from_offsets(vec![1]);

        let changes = reader.poll().await.expect("poll");

        assert_eq!(changes.len(), 1);
        assert_eq!(reader.missed(), 2);
    }

    #[tokio::test]
    async fn given_a_reader_from_the_end_when_opened_then_should_read_only_later_changes() {
        let feed = Feed::start(2).await;
        feed.append(0, 1, &change("readings", 0, CHANGE_OP_VERSION));
        feed.append(0, 2, &change("readings", 1, CHANGE_OP_VERSION));
        for to_offset in 0..5 {
            feed.append(
                1,
                10 + u128::from(to_offset),
                &change("readings", to_offset, 1),
            );
        }
        feed.expire_below(1, 5);
        let mut reader = feed
            .laser()
            .watch()
            .from_end()
            .records()
            .await
            .expect("reader");
        assert_eq!(reader.offsets(), &[2, 0]);
        assert!(reader.poll().await.expect("poll").is_empty());

        feed.append(0, 3, &change("readings", 2, CHANGE_OP_VERSION));
        feed.append(1, 20, &change("readings", 5, CHANGE_OP_VERSION));
        let changes = reader.poll().await.expect("poll");

        assert_eq!(
            changes
                .iter()
                .map(|watched| watched.message_id)
                .collect::<Vec<_>>(),
            vec![3, 20]
        );
        assert_eq!(
            reader.missed(),
            0,
            "a partition that held nothing at open has no history to miss"
        );
    }

    #[tokio::test]
    async fn given_nothing_new_when_polling_within_a_timeout_then_should_wait_for_the_first_change()
    {
        let feed = Feed::start(1).await;
        let mut reader = feed.laser().watch().records().await.expect("reader");
        assert!(
            reader
                .poll_within(Duration::ZERO)
                .await
                .expect("an empty wait")
                .is_empty()
        );
        let records = Arc::clone(&feed.records);
        let publisher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            records.lock().expect("feed lock").push(FeedRecord {
                partition_id: 0,
                offset: 0,
                id: 5,
                payload: change("readings", 0, CHANGE_OP_VERSION),
            });
        });

        let changes = reader
            .poll_within(Duration::from_secs(5))
            .await
            .expect("the wait ends at the change");

        publisher.await.expect("publisher");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].message_id, 5);
    }

    fn change(index: &str, to_offset: u64, v: u32) -> Vec<u8> {
        encode_named(&ChangeRecord {
            v,
            index: index.to_owned(),
            partition_id: 0,
            from_offset: to_offset,
            to_offset,
            rows: 1,
            stream: Some("orbit".to_owned()),
        })
        .expect("change record encodes")
    }

    // A fake change feed served over the Apache Iggy HTTP API: one topic of
    // `partitions` partitions, each retaining its records from an offset.
    struct Feed {
        url: String,
        records: Arc<Mutex<Vec<FeedRecord>>>,
        retained_from: Arc<Mutex<HashMap<u32, u64>>>,
        partitions: u32,
        task: JoinHandle<()>,
    }

    #[derive(Clone)]
    struct FeedRecord {
        partition_id: u32,
        offset: u64,
        id: u128,
        payload: Vec<u8>,
    }

    #[derive(Clone)]
    struct FeedState {
        records: Arc<Mutex<Vec<FeedRecord>>>,
        retained_from: Arc<Mutex<HashMap<u32, u64>>>,
        partitions: u32,
    }

    impl Feed {
        async fn start(partitions: u32) -> Self {
            let records = Arc::new(Mutex::new(Vec::new()));
            let retained_from = Arc::new(Mutex::new(HashMap::new()));
            let app = Router::new()
                .route("/streams/ops/topics/changes", get(topic))
                .route("/streams/ops/topics/changes/messages/deferred", get(poll))
                .with_state(FeedState {
                    records: Arc::clone(&records),
                    retained_from: Arc::clone(&retained_from),
                    partitions,
                });
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("feed listener");
            let url = format!(
                "http://{}",
                listener.local_addr().expect("listener address")
            );
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("feed server");
            });
            Self {
                url,
                records,
                retained_from,
                partitions,
                task,
            }
        }

        // Append a record at the partition's next offset, `id` its message id.
        fn append(&self, partition_id: u32, id: u128, payload: &[u8]) {
            assert!(partition_id < self.partitions, "a partition of the feed");
            let mut records = self.records.lock().expect("feed lock");
            let offset = records
                .iter()
                .filter(|record| record.partition_id == partition_id)
                .map(|record| record.offset + 1)
                .max()
                .unwrap_or(0);
            records.push(FeedRecord {
                partition_id,
                offset,
                id,
                payload: payload.to_vec(),
            });
        }

        // Drop every record of the partition below `offset`, as retention does.
        fn expire_below(&self, partition_id: u32, offset: u64) {
            self.retained_from
                .lock()
                .expect("retention lock")
                .insert(partition_id, offset);
        }

        fn laser(&self) -> Laser {
            let client = HttpClient::create(Arc::new(HttpClientConfig {
                api_url: self.url.clone(),
                jwt: Some("test-token".to_owned()),
                retries: 0,
                ..Default::default()
            }))
            .expect("HTTP client");
            let mut capabilities = Capabilities::OPEN;
            capabilities.watch = true;
            Laser::from_client(IggyClient::new(ClientWrapper::Http(client)))
                .with_capabilities(capabilities)
                .with_ops_stream("ops")
                .with_changes_topic("changes")
        }
    }

    impl Drop for Feed {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn topic(State(state): State<FeedState>) -> Json<TopicDetails> {
        Json(TopicDetails {
            id: 1,
            created_at: IggyTimestamp::from(0),
            name: "changes".to_owned(),
            size: IggyByteSize::from(0),
            messages_count: 0,
            partitions_count: state.partitions,
            partitions: Vec::new(),
            options: Default::default(),
        })
    }

    async fn poll(
        State(state): State<FeedState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<PolledMessages> {
        let partition_id: u32 = query["partition_id"].parse().expect("partition id");
        let count: usize = query["count"].parse().expect("poll count");
        let retained_from = state
            .retained_from
            .lock()
            .expect("retention lock")
            .get(&partition_id)
            .copied()
            .unwrap_or(0);
        let stored: Vec<FeedRecord> = state
            .records
            .lock()
            .expect("feed lock")
            .iter()
            .filter(|record| record.partition_id == partition_id)
            .filter(|record| record.offset >= retained_from)
            .cloned()
            .collect();
        let head = stored.last().map_or(0, |record| record.offset);
        let selected: Vec<FeedRecord> = match query.get("kind").map(String::as_str) {
            Some("last") => stored.last().cloned().into_iter().collect(),
            _ => {
                let from: u64 = query["value"].parse().expect("poll offset");
                stored
                    .into_iter()
                    .filter(|record| record.offset >= from)
                    .take(count)
                    .collect()
            }
        };
        let messages: Vec<IggyMessage> = selected
            .into_iter()
            .map(|record| {
                let mut message = IggyMessage::builder()
                    .id(record.id)
                    .payload(record.payload.into())
                    .build()
                    .expect("feed message");
                message.header.offset = record.offset;
                message.header.timestamp = record.offset;
                message
            })
            .collect();
        Json(PolledMessages::new(
            PartitionContext::default(),
            partition_id,
            head,
            messages.len() as u32,
            messages,
        ))
    }
}
