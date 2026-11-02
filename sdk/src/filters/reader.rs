use crate::error::{LaserError, decode_managed_reply};
use crate::filters::group::Membership;
#[cfg(feature = "filters")]
use crate::filters::guard::LocalGuard;
use crate::filters::progress::{Blocked, PartitionProgress};
#[cfg(test)]
use crate::iggy::prelude::PartitionContext;
use crate::iggy::prelude::{
    Client, IggyClient, IggyClientBuilder, IggyError, IggyMessage, PolledMessages, PollingStrategy,
    StreamClient, TopicClient,
};
use crate::laser::Laser;
use crate::nodes::{Answered, Target, is_route_stale, partition_primary};
use crate::retry::{MANAGED_MUTATION, MANAGED_MUTATION_DEADLINE};
use crate::stream::ConsumerStart;
use crate::stream::transport::seed_group_partition;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use iggy_binary_protocol::batch::{BatchIntegrity, decode_batch_slice_with};
use iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE;
use iggy_common::Identifier;
use laser_wire::codes::{
    AGDX_FILTERED_ACK_CODE, AGDX_FILTERED_POLL_CODE, AGDX_FILTERED_WAIT_CANCEL_CODE,
    AGDX_FILTERED_WAIT_CODE, FILTER_OP_VERSION,
};
#[cfg(feature = "filters")]
use laser_wire::filter::ConsumerFilter;
use laser_wire::filter::{
    AppliedPolicy, CatalogPosition, ExecutionMode, FilterConsumer, FilterError, FilterErrorReason,
    FilterOutcome, FilterRef, FilterReply, FilterSource, FilteredAck, FilteredPage,
    FilteredPollRequest, FilteredStart, FilteredWaitCancel, FilteredWaitPartition,
    FilteredWaitRequest, SourceGeneration, StopReason,
};
use laser_wire::framing::encode_named;
#[cfg(feature = "filters")]
use laser_wire::limits::MAX_FILTER_CATALOG_PAGE;
use laser_wire::result::ResultCode;
use laser_wire::validate::Validate;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::{Instant, sleep};

const DEFAULT_COUNT: u32 = 100;
const DEFAULT_MAX_REPLY_BYTES: u32 = 1024 * 1024;
pub(crate) const DEFAULT_IDLE_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(5);
// How much longer than its wait a held filtered wait has to answer.
const WAIT_REPLY_MARGIN: Duration = Duration::from_secs(5);
// How long a read waits on a partition's known primary before it asks the
// cluster whether another node leads the partition now.
const ROUTE_RECHECK: Duration = Duration::from_secs(1);
const DEFAULT_MAX_UNACKED_PAGES: usize = 1024;

// A filtered wait is named on its connection, so ids stay unique across every
// reader of the process, including readers sharing one client.
static NEXT_WAIT_ID: AtomicU64 = AtomicU64::new(1);

// A filtered wait in flight: its node, when it was sent, and its answer.
type HeldWait =
    Pin<Box<dyn Future<Output = (Target, Instant, Result<Vec<u32>, LaserError>)> + Send>>;

/// Builds a [`FilteredReader`] over one consumer group. Start from
/// [`ConsumerGroup::reader`](crate::stream::ConsumerGroup::reader).
pub struct FilteredReaderBuilder<'a> {
    laser: &'a Laser,
    source: FilterSource,
    consumer: FilterConsumer,
    filter: FilterRef,
    min_catalog_position: Option<CatalogPosition>,
    start: FilteredStart,
    count: u32,
    max_examined: Option<u32>,
    max_reply_bytes: u32,
    #[cfg(feature = "filters")]
    local_guard: bool,
    idle_interval: Duration,
    max_wait: Duration,
    retry_interval: Option<Duration>,
    max_unacked_pages: usize,
    selected_partitions: BTreeSet<u32>,
    seed: Option<ConsumerStart>,
}

/// Reads the records a consumer group's policy selects, with their original
/// offsets, and stores progress through fenced acknowledgments.
///
/// Pages are read ahead of acknowledgments. An acknowledgment stores a page's
/// safe offset only after that page and every earlier page of the partition
/// are fully handled, so progress never skips an unhandled record. Pages that
/// matched nothing are stored by the reader itself.
///
/// A partition that fails, faults, or blocks waits one idle interval before it
/// is read again, while the other partitions keep reading. A recreated
/// partition or a new group binding restarts the partition from its stored
/// offset once, and the error that reports it reaches the caller.
///
/// Every read goes to the partition's primary when its route is known, any
/// other node serves it from its own copy otherwise, and every acknowledgment
/// is a consumer offset store fenced to the partition incarnation and the
/// group owner the page was read under.
///
/// Every method is cancel-safe: a cancelled call leaves the position and the
/// unstored progress as they were, and the next call repeats the work.
pub struct FilteredReader {
    owner: Arc<()>,
    initial_assignment: bool,
    laser: Laser,
    // The client that holds the group membership and sends every read and
    // acknowledgment. A group reader owns its own, so two readers are two
    // members.
    coordinator: Arc<IggyClient>,
    consumer: FilterConsumer,
    // The request every read sends, with the partition and the start set per
    // read.
    request: FilteredPollRequest,
    stream: Identifier,
    topic: Identifier,
    // The replica id of each partition's primary, looked up before the first
    // read and again after a read that found it moved or gone.
    leaders: BTreeMap<u32, u8>,
    idle_interval: Duration,
    max_wait: Duration,
    // The longest filtered wait the server announced, zero once it refused one.
    announced_wait_ms: u32,
    // Waits still held when another node answered first, at most one per node,
    // so a busy node never piles waits up on a quiet one, each node with the
    // id its wait is cancelled by.
    held_waits: Mutex<FuturesUnordered<HeldWait>>,
    held_nodes: BTreeMap<Target, u64>,
    // Where a dropped reader sends the cancel of its held waits.
    runtime: Option<Handle>,
    // The partitions the last wait found ready, the only ones the next round
    // reads.
    ready: BTreeSet<u32>,
    // A wait answered ready, and the round after it has not shown yet that
    // the ready partitions held something new.
    ready_unconfirmed: bool,
    retry_interval: Duration,
    max_unacked_pages: usize,
    #[cfg(feature = "filters")]
    guard: Option<LocalGuard>,
    #[cfg(feature = "filters")]
    guard_enabled: bool,
    membership: Option<Membership>,
    partitions: BTreeMap<u32, PartitionProgress>,
    revoked: BTreeSet<u32>,
    selected_partitions: BTreeSet<u32>,
    cursor: usize,
    sequence: u64,
    examined_in_round: u64,
    // Partitions whose last read found the group assignment gone. The next
    // such read reaches the caller instead of syncing the group again.
    lost_assignments: BTreeSet<u32>,
    // The last wait was refused and sent again at once.
    wait_refused: bool,
    // The start a group consumer stores on a partition it gains that the
    // group never stored, and the gained partitions not checked yet.
    seed: Option<ConsumerStart>,
    unseeded: BTreeSet<u32>,
    buffered: VecDeque<MatchedRecord>,
    // An error a partition raised in a round that another partition's page
    // ended, returned by the next round before any read, so a stopped
    // partition is reported even while others keep matching.
    deferred_error: Option<LaserError>,
    // The stream and topic incarnation a numeric group id was first read
    // under. Group ids restart at zero on a recreated topic, so a later page
    // from another incarnation belongs to an unrelated group.
    pinned_source: Option<SourceIncarnation>,
}

/// The stream and topic a numeric group id is scoped to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceIncarnation {
    stream_id: u32,
    stream_created_at_micros: u64,
    topic_id: u32,
    topic_created_at_micros: u64,
}

impl SourceIncarnation {
    pub(crate) async fn read(
        client: &IggyClient,
        source: &FilterSource,
    ) -> Result<Self, LaserError> {
        let stream = client
            .get_stream(&Identifier::named(&source.stream)?)
            .await?
            .ok_or_else(|| {
                FilterError::new(FilterErrorReason::NotFound, "source stream does not exist")
            })?;
        let topic = client
            .get_topic(
                &Identifier::numeric(stream.id)?,
                &Identifier::named(&source.topic)?,
            )
            .await?
            .ok_or_else(|| {
                FilterError::new(FilterErrorReason::NotFound, "source topic does not exist")
            })?;
        Ok(Self {
            stream_id: stream.id,
            stream_created_at_micros: stream.created_at.as_micros(),
            topic_id: topic.id,
            topic_created_at_micros: topic.created_at.as_micros(),
        })
    }
}

impl SourceIncarnation {
    /// The exact identity of group `group_id` inside this incarnation.
    pub(crate) const fn identity(self, group_id: u64) -> laser_wire::filter::FilterGroupIdentity {
        laser_wire::filter::FilterGroupIdentity {
            stream_id: self.stream_id,
            stream_created_at_micros: self.stream_created_at_micros,
            topic_id: self.topic_id,
            topic_created_at_micros: self.topic_created_at_micros,
            group_id,
        }
    }
}

impl From<SourceGeneration> for SourceIncarnation {
    fn from(generation: SourceGeneration) -> Self {
        Self {
            stream_id: generation.stream_id,
            stream_created_at_micros: generation.stream_created_at_micros,
            topic_id: generation.topic_id,
            topic_created_at_micros: generation.topic_created_at_micros,
        }
    }
}

/// One page of matching records from one partition.
#[derive(Debug)]
pub struct MatchedPage {
    pub partition_id: u32,
    pub records: Vec<MatchedRecord>,
    /// The filter the server executed.
    pub policy: AppliedPolicy,
    /// The source history the page was read from.
    pub generation: SourceGeneration,
    pub stop: StopReason,
    pub examined: u32,
    /// The partition head when the page was read. Reported, not progress.
    pub frontier: u64,
    /// The offset acknowledging this page stores, once every earlier page is
    /// handled too. Absent when the page examined nothing.
    pub safe_ack_offset: Option<u64>,
    sequence: u64,
    owner: Arc<()>,
}

/// One matching record, unchanged from the stream.
#[derive(Debug)]
pub struct MatchedRecord {
    pub partition_id: u32,
    pub offset: u64,
    /// The partition head when the record was read.
    pub frontier: u64,
    /// `false` when a pass fault, foreign-record, or mismatch policy delivered
    /// the record without evaluating it.
    pub evaluated: bool,
    pub message: IggyMessage,
    sequence: u64,
    owner: Arc<()>,
}

/// The handle a delivered record is acknowledged by: the partition read it
/// came from and the reader membership that read it.
#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    pub(crate) partition_id: u32,
    pub(crate) offset: u64,
    sequence: u64,
    owner: Arc<()>,
}

impl MatchedRecord {
    pub(crate) fn delivery(&self) -> Delivery {
        Delivery {
            partition_id: self.partition_id,
            offset: self.offset,
            sequence: self.sequence,
            owner: Arc::clone(&self.owner),
        }
    }

    /// Decode the record's payload as JSON into `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, LaserError> {
        serde_json::from_slice(&self.message.payload)
            .map_err(|error| LaserError::Codec(error.to_string()))
    }

    /// Whether the record's header block fails to decode. The payload stays
    /// readable, and `message.user_headers_map()` returns the decode error.
    pub fn headers_malformed(&self) -> bool {
        self.message.user_headers_map().is_err()
    }
}

impl Delivery {
    pub(crate) fn same(&self, other: &Self) -> bool {
        self.partition_id == other.partition_id
            && self.offset == other.offset
            && self.sequence == other.sequence
            && Arc::ptr_eq(&self.owner, &other.owner)
    }
}

enum Read {
    Page(MatchedPage),
    /// Nothing matched. `more` when the page stopped before the visible end.
    Empty {
        more: bool,
    },
    Saturated,
}

enum Round {
    Page(MatchedPage),
    Busy,
    Idle,
}

/// What one answered filtered wait means for the next round.
#[derive(Debug, PartialEq, Eq)]
enum WaitAnswer {
    /// These partitions hold a record past what was examined.
    Ready(Vec<u32>),
    /// The wait held to its end without a record.
    Elapsed,
    /// Pace by one idle interval: the wait failed or answered empty long
    /// before its time.
    Sleep,
    /// The server holds no filtered wait.
    Unsupported,
}

impl<'a> FilteredReaderBuilder<'a> {
    pub(crate) fn new(
        laser: &'a Laser,
        source: FilterSource,
        consumer: FilterConsumer,
        filter: FilterRef,
        min_catalog_position: Option<CatalogPosition>,
    ) -> Self {
        Self {
            laser,
            source,
            consumer,
            filter,
            min_catalog_position,
            start: FilteredStart::Next,
            count: DEFAULT_COUNT,
            max_examined: None,
            max_reply_bytes: DEFAULT_MAX_REPLY_BYTES,
            #[cfg(feature = "filters")]
            local_guard: false,
            idle_interval: DEFAULT_IDLE_INTERVAL,
            max_wait: DEFAULT_MAX_WAIT,
            retry_interval: None,
            max_unacked_pages: DEFAULT_MAX_UNACKED_PAGES,
            selected_partitions: BTreeSet::new(),
            seed: None,
        }
    }

    /// Read every partition after the group's stored offset, and store
    /// `start` first on each partition this member gains that the group
    /// never stored.
    #[must_use]
    pub(crate) fn seeded(mut self, start: ConsumerStart) -> Self {
        self.seed = Some(start);
        self.start = FilteredStart::Next;
        self
    }

    /// Where each partition read at the build starts. Defaults to after the
    /// stored offset. A partition the group hands this reader later always
    /// resumes after the group's stored offset, so the backlog the previous
    /// owner left unacknowledged is read, not skipped.
    #[must_use]
    pub fn start(mut self, start: FilteredStart) -> Self {
        self.start = start;
        self
    }

    /// Restrict reads to this partition when the group assigns it to this
    /// member. Repeat for several partitions. Native ownership stays unchanged.
    #[must_use]
    pub fn partition(mut self, partition_id: u32) -> Self {
        self.selected_partitions.insert(partition_id);
        self
    }

    /// Most matching records per page.
    #[must_use]
    pub fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    /// Most source records one page examines, independent of
    /// [`count`](Self::count). Without it the server's own budget bounds the
    /// scan. A page that examines its budget without a match returns empty
    /// and the reader continues from where it stopped.
    #[must_use]
    pub fn max_examined(mut self, records: u32) -> Self {
        self.max_examined = Some(records);
        self
    }

    /// Most record bytes per page. The server may lower it.
    #[must_use]
    pub fn max_reply_bytes(mut self, max_reply_bytes: u32) -> Self {
        self.max_reply_bytes = max_reply_bytes;
        self
    }

    /// Re-evaluate every returned record locally with the shared evaluator
    /// and fail the page on any disagreement, before the caller sees it.
    #[cfg(feature = "filters")]
    #[must_use]
    pub fn local_guard(mut self, enabled: bool) -> Self {
        self.local_guard = enabled;
        self
    }

    /// How long [`FilteredReader::next_page`] waits after a round that found
    /// nothing new on a server that holds no filtered wait, and how long a
    /// blocked partition waits before it is read again.
    #[must_use]
    pub fn idle_interval(mut self, interval: Duration) -> Self {
        self.idle_interval = interval;
        self
    }

    /// How long the server may hold one filtered wait after a round that found
    /// nothing new, at most what it announces. A record ends the wait at once.
    /// Default 5 s. Zero paces idle rounds by the idle interval.
    #[must_use]
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// How long a partition whose read failed waits before it is read again.
    /// Defaults to the idle interval.
    #[must_use]
    pub(crate) fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = Some(interval);
        self
    }

    /// Most outstanding record-bearing pages per partition. Defaults to 1024.
    /// A larger value permits more concurrent work and retains more offset metadata.
    /// Acknowledge processed pages to release slots. Zero is not allowed.
    #[must_use]
    pub fn max_unacked_pages(mut self, pages: usize) -> Self {
        self.max_unacked_pages = pages;
        self
    }

    pub async fn build(self) -> Result<FilteredReader, LaserError> {
        if self.max_unacked_pages == 0 {
            return Err(LaserError::Config("max_unacked_pages must be positive"));
        }
        let capabilities = self.laser.capabilities().await;
        if !matches!(self.filter, FilterRef::Group) && !capabilities.filters.native {
            return Err(LaserError::unsupported(
                "filters",
                "consumer filters are not served by this server",
            ));
        }
        if matches!(self.filter, FilterRef::Group) && !capabilities.filters.group_policy_reads {
            return Err(LaserError::unsupported_feature(
                "filters",
                "group_policy_reads",
                "this server serves consumer filters but not group-aware reads, upgrade it before reading groups through the Laser SDK",
            ));
        }
        let consumer = self.consumer;
        let request = FilteredPollRequest {
            v: FILTER_OP_VERSION,
            source: self.source,
            partition_id: 0,
            consumer: consumer.clone(),
            filter: self.filter,
            start: self.start.clone(),
            count: self.count,
            max_reply_bytes: self.max_reply_bytes,
            max_examined: self.max_examined,
            min_catalog_position: self.min_catalog_position,
        };
        request.validate()?;
        let stream = Identifier::named(&request.source.stream)?;
        let topic = Identifier::named(&request.source.topic)?;
        let pinned_source = if matches!(consumer, FilterConsumer::GroupId(_)) {
            Some(SourceIncarnation::read(&self.laser.client(), &request.source).await?)
        } else {
            None
        };
        // A group reader owns the connection that holds its membership, so
        // two readers are two members. Without a connection string the
        // shared client is the only one there is.
        let coordinator = match self.laser.connection_string() {
            Some(connection_string) => {
                let client =
                    IggyClientBuilder::from_connection_string(connection_string)?.build()?;
                client.connect().await?;
                Arc::new(client)
            }
            None => self.laser.client(),
        };
        let membership =
            Membership::join(&coordinator, &request.source, &consumer, pinned_source).await?;
        let partitions: BTreeMap<u32, PartitionProgress> = membership
            .partitions()
            .iter()
            .filter(|partition_id| {
                self.selected_partitions.is_empty()
                    || self.selected_partitions.contains(partition_id)
            })
            .map(|partition_id| (*partition_id, PartitionProgress::new(self.start.clone())))
            .collect();
        let mut unseeded = BTreeSet::new();
        if let Some(start) = self.seed {
            let group = iggy::prelude::Consumer::group(membership.group().clone());
            for partition_id in partitions.keys() {
                match seed_group_partition(
                    &coordinator,
                    &group,
                    (&stream, &topic, *partition_id),
                    start,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) if seed_not_held(&error) => {
                        unseeded.insert(*partition_id);
                    }
                    Err(error) => {
                        let _ = membership.leave(&coordinator).await;
                        return Err(error);
                    }
                }
            }
        }
        Ok(FilteredReader {
            owner: Arc::new(()),
            laser: self.laser.clone(),
            coordinator,
            consumer,
            request,
            stream,
            topic,
            leaders: BTreeMap::new(),
            idle_interval: self.idle_interval,
            max_wait: self.max_wait,
            announced_wait_ms: capabilities.filters.max_wait_ms,
            held_waits: Mutex::new(FuturesUnordered::new()),
            held_nodes: BTreeMap::new(),
            runtime: Handle::try_current().ok(),
            ready: BTreeSet::new(),
            ready_unconfirmed: false,
            retry_interval: self.retry_interval.unwrap_or(self.idle_interval),
            max_unacked_pages: self.max_unacked_pages,
            #[cfg(feature = "filters")]
            guard: None,
            #[cfg(feature = "filters")]
            guard_enabled: self.local_guard,
            initial_assignment: membership.partitions().is_empty(),
            membership: Some(membership),
            partitions,
            seed: self.seed,
            unseeded,
            wait_refused: false,
            revoked: BTreeSet::new(),
            selected_partitions: self.selected_partitions,
            cursor: 0,
            sequence: 0,
            examined_in_round: 0,
            lost_assignments: BTreeSet::new(),
            buffered: VecDeque::new(),
            deferred_error: None,
            pinned_source,
        })
    }
}

impl FilteredReader {
    /// The next page with at least one matching record. Waits while every
    /// partition has nothing new. Wrap it in a timeout to bound the wait.
    pub async fn next_page(&mut self) -> Result<MatchedPage, LaserError> {
        loop {
            match self.round().await? {
                Round::Page(page) => return Ok(page),
                Round::Busy => {}
                Round::Idle => self.wait_for_records().await,
            }
        }
    }

    /// Read each partition in turn until a page matches or every partition
    /// has nothing new, without the idle wait. `None` when nothing is new, so
    /// a caller that drives its own loop calls
    /// [`wait_for_records`](Self::wait_for_records) before it asks again.
    pub async fn try_next_page(&mut self) -> Result<Option<MatchedPage>, LaserError> {
        loop {
            match self.round().await? {
                Round::Page(page) => return Ok(Some(page)),
                Round::Busy => {}
                Round::Idle => return Ok(None),
            }
        }
    }

    /// Perform one bounded round without an idle wait. The flag means unmatched
    /// work remains. Read [`examined_in_round`](Self::examined_in_round) for its
    /// source-record count, including pages that matched nothing.
    pub async fn read_round(&mut self) -> Result<(Option<MatchedPage>, bool), LaserError> {
        Ok(match self.round().await? {
            Round::Page(page) => (Some(page), false),
            Round::Busy => (None, true),
            Round::Idle => (None, false),
        })
    }

    /// Source records examined by the last bounded round, including empty
    /// pages that advance progress.
    pub const fn examined_in_round(&self) -> u64 {
        self.examined_in_round
    }

    /// How long this reader waits when nothing is new on a server that holds
    /// no filtered wait.
    pub const fn idle_interval(&self) -> Duration {
        self.idle_interval
    }

    /// Wait after a round that found nothing new. The server holds one
    /// filtered wait per node that serves this reader's partitions until one
    /// of them has a record past what was examined, at most
    /// [`max_wait`](FilteredReaderBuilder::max_wait) and what the server
    /// announces, and the next round reads only the ready partitions. On a
    /// server without filtered waits, or after a wait that failed or answered
    /// empty long before its time, it sleeps one idle interval instead.
    pub async fn wait_for_records(&mut self) {
        self.wait_for_records_within(None).await;
    }

    // `limit` bounds the whole wait, for a caller with work due sooner.
    pub(crate) async fn wait_for_records_within(&mut self, limit: Option<Duration>) {
        let pacing = limit.map_or(self.idle_interval, |limit| limit.min(self.idle_interval));
        let mut wait = self
            .max_wait
            .min(Duration::from_millis(u64::from(self.announced_wait_ms)));
        if let Some(limit) = limit {
            wait = wait.min(limit);
        }
        // A ready answer the round after it could not confirm would repeat
        // at once, so that round is paced instead.
        let unconfirmed =
            std::mem::take(&mut self.ready_unconfirmed) && self.examined_in_round == 0;
        if wait.as_millis() == 0 || unconfirmed {
            sleep(pacing).await;
            return;
        }
        match self.held_wait(wait).await {
            WaitAnswer::Ready(ready) => {
                self.ready.extend(
                    ready
                        .into_iter()
                        .filter(|partition_id| self.partitions.contains_key(partition_id)),
                );
                self.ready_unconfirmed = true;
            }
            WaitAnswer::Elapsed => {}
            WaitAnswer::Sleep => sleep(pacing).await,
            WaitAnswer::Unsupported => {
                tracing::debug!(target: "laser", "The server holds no filtered wait, idle rounds are paced by the idle interval.");
                self.announced_wait_ms = 0;
                sleep(pacing).await;
            }
        }
    }

    // Send one filtered wait to each node that serves a readable partition
    // and holds no wait yet, and take the first answer.
    async fn held_wait(&mut self, wait: Duration) -> WaitAnswer {
        let now = Instant::now();
        let mut partitions = Vec::new();
        for partition_id in self.partitions() {
            let Some(progress) = self.partitions.get(&partition_id) else {
                continue;
            };
            if progress.waiting(now) {
                continue;
            }
            let offset = progress.wait_offset();
            let target = self.read_target(partition_id).await;
            if !self.held_nodes.contains_key(&target) {
                partitions.push((
                    target,
                    FilteredWaitPartition {
                        partition_id,
                        offset,
                    },
                ));
            }
        }
        let deadline = std::time::Instant::now() + wait + WAIT_REPLY_MARGIN;
        let next_wait_id = || NEXT_WAIT_ID.fetch_add(1, Ordering::Relaxed);
        for (target, request) in wait_requests(&self.request, partitions, wait, next_wait_id) {
            let payload = match request.validate().map_err(LaserError::from).and_then(|()| {
                encode_named(&request)
                    .map_err(|error| LaserError::Codec(format!("encode filtered wait: {error}")))
            }) {
                Ok(payload) => Bytes::from(payload),
                Err(error) => {
                    tracing::debug!(target: "laser", %error, "A filtered wait could not be built.");
                    return WaitAnswer::Sleep;
                }
            };
            let laser = self.laser.clone();
            let coordinator = Arc::clone(&self.coordinator);
            self.held_nodes.insert(target, request.wait_id);
            self.held_waits
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Box::pin(async move {
                    let sent = Instant::now();
                    let answer = async {
                        let answered = laser
                            .nodes()
                            .send_held(
                                &coordinator,
                                AGDX_FILTERED_WAIT_CODE,
                                payload,
                                target,
                                deadline,
                            )
                            .await?;
                        match decode_managed_reply::<FilterReply>(&answered.reply)? {
                            FilterReply::Ok(FilterOutcome::Ready(result)) => Ok(result.ready),
                            FilterReply::Ok(_) => Err(unexpected("filtered wait")),
                            FilterReply::Err(error) => Err(error.into()),
                        }
                    }
                    .await;
                    (target, sent, answer)
                }));
        }
        // The assignment is read again when it is due while every wait
        // holds, so partitions gained or lost meanwhile are read at once.
        let first = loop {
            let refresh_in = self.membership.as_ref().map(Membership::refresh_in);
            let next = self
                .held_waits
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .next();
            let Some(refresh_in) = refresh_in else {
                break next.await;
            };
            match tokio::time::timeout(refresh_in, next).await {
                Ok(first) => break first,
                Err(_) => match self.refresh_membership().await {
                    Ok(false) => {}
                    Ok(true) => return WaitAnswer::Elapsed,
                    Err(_) => {
                        self.cancel_held_waits();
                        return WaitAnswer::Elapsed;
                    }
                },
            }
        };
        let Some((target, sent, answer)) = first else {
            return WaitAnswer::Sleep;
        };
        self.held_nodes.remove(&target);
        if let Err(error) = &answer {
            tracing::debug!(target: "laser", %error, "A filtered wait failed.");
            // A node that no longer serves the partitions of its wait sends
            // their next round to a route looked up again.
            if is_route_moved(error)
                && let Target::Replica(replica) = target
            {
                self.leaders.retain(|_, leader| *leader != replica);
            }
        }
        if let Some(again) = pace_refusal(&mut self.wait_refused, &answer, target) {
            return again;
        }
        let mut answer = classify_wait(answer, sent.elapsed(), wait);
        if let WaitAnswer::Ready(ready) = &mut answer {
            // Answers already in keep their ready partitions instead of
            // costing another round.
            let held_waits = self
                .held_waits
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner);
            while let Some(Some((target, _, answered))) = held_waits.next().now_or_never() {
                self.held_nodes.remove(&target);
                if let Ok(more) = answered {
                    ready.extend(more);
                }
            }
            self.cancel_held_waits();
        }
        answer
    }

    // End every wait still held on another node, so its slot frees at once
    // instead of at its deadline. The cancel is sent in the background and a
    // failed one is only logged, the server still ends the wait at its
    // deadline.
    fn cancel_held_waits(&mut self) {
        let held = self.take_held_waits();
        if held.is_empty() {
            return;
        }
        let laser = self.laser.clone();
        let coordinator = Arc::clone(&self.coordinator);
        let cancel = async move { cancel_waits(&laser, &coordinator, held).await };
        match &self.runtime {
            Some(runtime) => drop(runtime.spawn(cancel)),
            None => {
                tracing::debug!(target: "laser", "No runtime takes the cancel of the held filtered waits, the server ends them at their deadline.");
            }
        }
    }

    // Forget the waits still held, dropping their pending answers, and return
    // the id each node holds.
    fn take_held_waits(&mut self) -> BTreeMap<Target, u64> {
        self.held_waits
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        std::mem::take(&mut self.held_nodes)
    }

    /// The connection that holds this reader's group membership, for tests
    /// that simulate the server dropping it.
    #[doc(hidden)]
    pub fn coordinator(&self) -> Arc<IggyClient> {
        Arc::clone(&self.coordinator)
    }

    /// Whether `page` came from this reader in its current membership, so its
    /// records can still be acknowledged. A rejoin retires every earlier page.
    pub fn owns(&self, page: &MatchedPage) -> bool {
        Arc::ptr_eq(&self.owner, &page.owner)
            && self
                .partitions
                .get(&page.partition_id)
                .is_some_and(|progress| progress.knows(page.sequence))
    }

    pub(crate) fn owns_record(&self, record: &MatchedRecord) -> bool {
        Arc::ptr_eq(&self.owner, &record.owner)
            && self
                .partitions
                .get(&record.partition_id)
                .is_some_and(|progress| progress.knows(record.sequence))
    }

    /// The next matching record, one at a time over [`next_page`](Self::next_page).
    pub async fn next_record(&mut self) -> Result<MatchedRecord, LaserError> {
        loop {
            if let Some(record) = self.buffered.pop_front() {
                return Ok(record);
            }
            let page = self.next_page().await?;
            self.buffered.extend(page.records);
        }
    }

    /// Mark one record handled, and store the progress this completes.
    pub async fn ack(&mut self, record: &MatchedRecord) -> Result<(), LaserError> {
        self.require_owner(&record.owner)?;
        self.require_sequence(record.partition_id, record.sequence)?;
        self.progress(record.partition_id)?
            .complete_record(record.sequence, record.offset);
        self.store(record.partition_id).await
    }

    /// Mark every record on this partition through `record` handled, and
    /// store that completed prefix. Process all preceding records first.
    /// Records after it remain pending, even inside the same page.
    pub async fn ack_through(&mut self, record: &MatchedRecord) -> Result<(), LaserError> {
        self.require_owner(&record.owner)?;
        self.require_sequence(record.partition_id, record.sequence)?;
        if !self
            .progress(record.partition_id)?
            .complete_through(record.sequence, record.offset)
        {
            return Err(LaserError::Invalid(
                "the record is past the page acknowledgment boundary".to_owned(),
            ));
        }
        self.store(record.partition_id).await
    }

    /// Mark every record of `page` handled, and store the progress this
    /// completes.
    pub async fn ack_page(&mut self, page: &MatchedPage) -> Result<(), LaserError> {
        self.require_owner(&page.owner)?;
        self.require_sequence(page.partition_id, page.sequence)?;
        self.progress(page.partition_id)?
            .complete_page(page.sequence);
        self.store(page.partition_id).await
    }

    /// Mark the partition's records through `delivery` handled without
    /// storing yet. The normal consumer yields in order, so every yield
    /// completes the prefix through it, also inside a page, and its automatic
    /// commit policies store that prefix on their own cadence through
    /// [`flush_completed`](Self::flush_completed).
    pub(crate) fn handled(&mut self, delivery: &Delivery) {
        if !Arc::ptr_eq(&self.owner, &delivery.owner) {
            return;
        }
        if let Some(progress) = self.partitions.get_mut(&delivery.partition_id) {
            progress.complete_through(delivery.sequence, delivery.offset);
        }
    }

    /// Acknowledge every record on the partition through `delivery`, the
    /// normal consumer's manual commit.
    pub(crate) async fn ack_through_delivery(
        &mut self,
        delivery: &Delivery,
    ) -> Result<(), LaserError> {
        self.require_owner(&delivery.owner)?;
        self.require_sequence(delivery.partition_id, delivery.sequence)?;
        if !self
            .progress(delivery.partition_id)?
            .complete_through(delivery.sequence, delivery.offset)
        {
            return Err(LaserError::Invalid(
                "the record is past the page acknowledgment boundary".to_owned(),
            ));
        }
        self.store(delivery.partition_id).await
    }

    /// The newest offset this reader stored per partition.
    pub(crate) fn stored_offsets(&self) -> BTreeMap<u32, u64> {
        self.partitions
            .iter()
            .filter_map(|(partition_id, progress)| {
                progress
                    .stored_offset()
                    .map(|offset| (*partition_id, offset))
            })
            .collect()
    }

    /// The partitions this reader reads now.
    pub fn partitions(&self) -> Vec<u32> {
        self.partitions
            .keys()
            .filter(|partition_id| !self.revoked.contains(partition_id))
            .copied()
            .collect()
    }

    /// Flush completed targets while retaining the reader if a binding cancels the await.
    #[doc(hidden)]
    pub async fn flush_completed(&mut self) -> Result<(), LaserError> {
        let mut outcome = Ok(());
        for partition_id in self.partitions.keys().copied().collect::<Vec<_>>() {
            let flushed = self.store(partition_id).await;
            outcome = outcome.and(flushed);
        }
        outcome
    }

    /// Store completed progress and leave the group. Handled records that
    /// were not acknowledged are read again by the next reader. A reader
    /// dropped without `close` leaves its group when its own coordinator
    /// connection closes.
    pub async fn close(mut self) -> Result<(), LaserError> {
        let held = self.take_held_waits();
        cancel_waits(&self.laser, &self.coordinator, held).await;
        let mut outcome = self.flush_completed().await;
        if let Some(membership) = &self.membership {
            outcome = outcome.and(membership.leave(&self.coordinator).await);
        }
        // A group reader owns the coordinator it joined over. Every other
        // reader borrows the Laser's shared client.
        if self.membership.is_some() && self.laser.connection_string().is_some() {
            let _ = self.coordinator.shutdown().await;
        }
        outcome
    }

    // One read of every partition that is not waiting, in turn from where the
    // last round stopped. A failing partition is deferred and the rest still
    // read, so one partition never starves the others, and its error reaches
    // the caller at most once per idle interval: at once when the round ends
    // without a page, or first thing in the next round when a later
    // partition's page ended this one.
    async fn round(&mut self) -> Result<Round, LaserError> {
        self.examined_in_round = 0;
        if let Some(error) = self.deferred_error.take() {
            return Err(error);
        }
        if self.membership.as_ref().is_some_and(Membership::is_due) {
            self.refresh_membership().await?;
        }
        let mut readable = self.partitions();
        let ready = std::mem::take(&mut self.ready);
        if readable
            .iter()
            .any(|partition_id| ready.contains(partition_id))
        {
            readable.retain(|partition_id| ready.contains(partition_id));
        }
        if readable.is_empty() {
            return Ok(Round::Idle);
        }
        let now = Instant::now();
        let mut busy = false;
        let mut failed = None;
        let mut saturated = Vec::new();
        for step in 0..readable.len() {
            let index = (self.cursor + step) % readable.len();
            let partition_id = readable[index];
            if self.progress(partition_id)?.waiting(now) {
                continue;
            }
            match self.read(partition_id).await {
                Ok(Read::Page(page)) => {
                    self.cursor = index + 1;
                    self.deferred_error = failed;
                    return Ok(Round::Page(page));
                }
                Ok(Read::Empty { more }) => busy |= more,
                Ok(Read::Saturated) => saturated.push(partition_id),
                Err(error) => {
                    let retry_at = now + self.retry_interval;
                    if let Ok(progress) = self.progress(partition_id) {
                        progress.defer(retry_at);
                    }
                    failed.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failed {
            return Err(error);
        }
        if !saturated.is_empty() && !busy {
            return Err(LaserError::Invalid(format!(
                "partitions {saturated:?} each have {} unacknowledged pages. Acknowledge before reading more",
                self.max_unacked_pages
            )));
        }
        Ok(if busy { Round::Busy } else { Round::Idle })
    }

    async fn read(&mut self, partition_id: u32) -> Result<Read, LaserError> {
        if let (Some(start), Some(membership)) = (self.seed, self.membership.as_mut())
            && self.unseeded.contains(&partition_id)
        {
            match seed_group_partition(
                &self.coordinator,
                &iggy::prelude::Consumer::group(membership.group().clone()),
                (&self.stream, &self.topic, partition_id),
                start,
            )
            .await
            {
                Ok(()) => {
                    self.unseeded.remove(&partition_id);
                }
                // Not installed for this member yet: it stays unread until a
                // later sync shows it held.
                Err(error) if seed_not_held(&error) => {
                    membership.expire();
                    return Ok(Read::Empty { more: false });
                }
                Err(error) => return Err(error),
            }
        }
        let maximum = self.max_unacked_pages;
        let progress = self.progress(partition_id)?;
        if progress.in_flight() >= maximum {
            return Ok(Read::Saturated);
        }
        let start = progress.start().clone();
        self.request.partition_id = partition_id;
        self.request.start = start;
        let payload = encode_named(&self.request)
            .map_err(|error| LaserError::Codec(format!("encode filter request: {error}")))?;
        let outcome = self
            .exchange(partition_id, AGDX_FILTERED_POLL_CODE, payload)
            .await;
        let mut page = match outcome {
            Ok(FilterOutcome::Page(page)) => {
                self.lost_assignments.remove(&partition_id);
                page
            }
            Ok(_) => return Err(unexpected("filtered poll")),
            Err(error) if is_assignment_lost(&error) => {
                self.leaders.remove(&partition_id);
                if !self.lost_assignments.insert(partition_id) {
                    return Err(error);
                }
                if let Some(membership) = self.membership.as_mut() {
                    membership.expire();
                }
                return Ok(Read::Empty { more: false });
            }
            Err(error) => {
                let error = self.restart_after(partition_id, error);
                // The group's policy changed under this read. The partition
                // restarted from its stored offset and is read again at
                // once, now under the policy the group holds. Delivery goes
                // on, only an explicit acknowledgment of a record read under
                // the old policy reports the change.
                if error.filter_reason() == Some(FilterErrorReason::Conflict) {
                    tracing::warn!(target: "laser", partition_id, %error, "the group's policy changed, the partition continues from its stored offset (partition: {partition_id})");
                    return Ok(Read::Empty { more: true });
                }
                return Err(error);
            }
        };
        if page.records.len() > self.request.max_reply_bytes as usize + POLL_RESPONSE_HEADER_SIZE {
            return Err(LaserError::Protocol(
                "filtered page exceeds the requested record byte limit".to_owned(),
            ));
        }
        validate_record_body(&self.request, &page)?;
        // The strategy seeds only an empty batch's continuation, which this reader keeps itself.
        let polled = PolledMessages::from_bytes(
            Bytes::from(std::mem::take(&mut page.records)),
            &PollingStrategy::next(),
        )?;
        validate_page(&self.request, &page, &polled)?;
        self.pin_source(&page)?;
        let messages = polled.messages;
        #[cfg(feature = "filters")]
        if page.policy.mode.is_filtered() {
            if self.guard_enabled && self.guard.is_none() {
                self.guard = Some(
                    LocalGuard::load(
                        &self.laser,
                        &bound_definition(&self.laser, &page, &self.request.source).await?,
                    )
                    .await?,
                );
            }
            if let Some(guard) = &self.guard
                && let Err(error) = guard.check(&page, &messages)
            {
                return Err(self.restart_after(partition_id, error));
            }
        }
        self.sequence += 1;
        let sequence = self.sequence;
        let retry_at = Instant::now() + self.idle_interval;
        let progress = self.progress(partition_id)?;
        progress.record(
            sequence,
            &page,
            messages.iter().map(|message| message.header.offset),
        );
        let blocked = progress.blocked();
        if blocked.is_some() {
            progress.defer(retry_at);
        }
        self.examined_in_round += u64::from(page.examined);
        if page.examined > 0 {
            self.ready_unconfirmed = false;
        }
        if messages.is_empty() {
            if let Err(error) = self.flush(partition_id).await {
                tracing::debug!(target: "laser", partition_id, %error, "storing filtered progress failed, retried with the next acknowledgment");
            }
            if let Some(blocked) = blocked {
                return Err(blocked_error(partition_id, blocked));
            }
            return Ok(Read::Empty {
                more: matches!(page.stop, StopReason::Filled | StopReason::Budget),
            });
        }
        let unevaluated: BTreeSet<u64> = page.unevaluated.iter().copied().collect();
        let records = messages
            .into_iter()
            .map(|message| MatchedRecord {
                partition_id,
                offset: message.header.offset,
                frontier: page.frontier,
                evaluated: page.policy.mode.is_filtered()
                    && !unevaluated.contains(&message.header.offset),
                message,
                sequence,
                owner: Arc::clone(&self.owner),
            })
            .collect();
        Ok(Read::Page(MatchedPage {
            partition_id,
            records,
            policy: page.policy,
            generation: page.generation,
            stop: page.stop,
            examined: page.examined,
            frontier: page.frontier,
            safe_ack_offset: page.safe_ack_offset,
            sequence,
            owner: Arc::clone(&self.owner),
        }))
    }

    // A recreated partition or group, or a new group binding, ends the
    // history or the policy the partition's continuation was issued for. The
    // partition starts over from its stored offset, the guard reloads a
    // rebound filter, and the error reaches the caller once.
    fn restart_after(&mut self, partition_id: u32, error: LaserError) -> LaserError {
        let reason = match &error {
            LaserError::Iggy(IggyError::HistoryUnavailable) => {
                Some(FilterErrorReason::SourceChanged)
            }
            error => error.filter_reason(),
        };
        if !matches!(
            reason,
            Some(FilterErrorReason::SourceChanged | FilterErrorReason::Conflict)
        ) {
            return error;
        }
        self.leaders.remove(&partition_id);
        if let Ok(progress) = self.progress(partition_id) {
            *progress = PartitionProgress::new(FilteredStart::Next);
        }
        // Buffered records of the partition belong to the dropped pages, so
        // they are read again from the stored offset instead.
        self.buffered
            .retain(|record| record.partition_id != partition_id);
        if reason == Some(FilterErrorReason::SourceChanged)
            && let Some(membership) = self.membership.as_mut()
        {
            membership.expire();
        }
        #[cfg(feature = "filters")]
        {
            self.guard = None;
        }
        error
    }

    // Store the completed prefix like `flush`, again on the managed mutation
    // backoff while a failover leaves the partition without a primary that
    // takes it. Each repeat sends the same target, never past what was
    // delivered.
    async fn store(&mut self, partition_id: u32) -> Result<(), LaserError> {
        let deadline = Instant::now() + MANAGED_MUTATION_DEADLINE;
        let mut attempt = 0;
        loop {
            match self.flush(partition_id).await {
                Err(error) if is_route_moved(&error) && Instant::now() < deadline => {
                    attempt += 1;
                    tracing::debug!(target: "laser", partition_id, attempt, %error, "Storing the progress of partition {partition_id} found no primary, it is sent again.");
                    sleep(MANAGED_MUTATION.delay(attempt)).await;
                }
                stored => return stored,
            }
        }
    }

    // Store the completed prefix of one partition. A lost reply keeps the
    // target unstored, and the next store sends it again. Stores are
    // monotone, so a repeat never moves progress back.
    async fn flush(&mut self, partition_id: u32) -> Result<(), LaserError> {
        let Some(target) = self.progress(partition_id)?.unstored().cloned() else {
            self.retire(partition_id);
            return Ok(());
        };
        let ack = FilteredAck {
            v: FILTER_OP_VERSION,
            source: self.request.source.clone(),
            partition_id,
            consumer: self.consumer.clone(),
            group_id: target.group_id,
            generation: target.generation,
            ack_context: target.ack_context,
            digest: target.policy.digest.clone(),
            mode: target.policy.mode,
            policy_generation: target.policy.policy_generation,
            offset: target.offset,
        };
        let stored = self
            .exchange(partition_id, AGDX_FILTERED_ACK_CODE, encode_named(&ack)?)
            .await;
        match stored {
            Ok(FilterOutcome::Acknowledged(receipt))
                if receipt.partition_id == partition_id
                    && receipt.offset == target.offset
                    && receipt.generation == target.generation =>
            {
                self.progress(partition_id)?.stored(&target);
                self.retire(partition_id);
                Ok(())
            }
            Ok(_) => Err(LaserError::Protocol(
                "the acknowledgment receipt does not match its page".to_owned(),
            )),
            Err(error) => {
                if is_assignment_lost(&error)
                    && let Some(membership) = self.membership.as_mut()
                {
                    membership.expire();
                }
                Err(self.restart_after(partition_id, error))
            }
        }
    }

    async fn exchange(
        &mut self,
        partition_id: u32,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<FilterOutcome, LaserError> {
        let route = ClusterRoute {
            laser: &self.laser,
            coordinator: &self.coordinator,
            stream: &self.stream,
            topic: &self.topic,
        };
        exchange_on(
            &route,
            &mut self.leaders,
            partition_id,
            code,
            Bytes::from(payload),
        )
        .await
    }

    async fn read_target(&mut self, partition_id: u32) -> Target {
        let route = ClusterRoute {
            laser: &self.laser,
            coordinator: &self.coordinator,
            stream: &self.stream,
            topic: &self.topic,
        };
        route_target(&route, &mut self.leaders, partition_id).await
    }

    // Follow the group's current assignment, `true` when it changed. The
    // whole change is applied to the reader before anything awaits, so a
    // cancelled call never loses it.
    // A partition that left the assignment stops being read but keeps its
    // in-flight pages, so records already handed out can still be
    // acknowledged while the server's offset fence allows it.
    async fn refresh_membership(&mut self) -> Result<bool, LaserError> {
        let Some(membership) = self.membership.as_mut() else {
            return Ok(false);
        };
        if !membership.sync(&self.coordinator).await? {
            return Ok(false);
        }
        let assigned: BTreeSet<u32> = membership
            .partitions()
            .iter()
            .copied()
            .filter(|partition| {
                self.selected_partitions.is_empty() || self.selected_partitions.contains(partition)
            })
            .collect();
        let rejoined = membership.take_rejoined();
        // Held waits name the old assignment's partitions.
        self.cancel_held_waits();
        if rejoined {
            self.initial_assignment = false;
            self.owner = Arc::new(());
            self.partitions.clear();
            self.revoked.clear();
            self.buffered.clear();
            self.unseeded.clear();
        }
        let left: Vec<u32> = self
            .partitions
            .keys()
            .filter(|partition_id| !assigned.contains(partition_id))
            .copied()
            .collect();
        for partition_id in &left {
            self.revoked.insert(*partition_id);
            self.retire(*partition_id);
        }
        // A partition gained on a rebalance resumes after the group's stored
        // offset, which holds what the previous owner acknowledged. One that
        // comes back before its revoked entry retired starts over the same
        // way, so it never replays another member's work from an old
        // continuation.
        let initial_assignment = self.initial_assignment;
        if !assigned.is_empty() {
            self.initial_assignment = false;
        }
        self.unseeded
            .retain(|partition_id| assigned.contains(partition_id));
        for partition_id in assigned {
            if self.revoked.remove(&partition_id) || !self.partitions.contains_key(&partition_id) {
                if self.seed.is_some() {
                    self.unseeded.insert(partition_id);
                }
                self.buffered
                    .retain(|record| record.partition_id != partition_id);
                self.partitions.insert(
                    partition_id,
                    PartitionProgress::new(if initial_assignment {
                        self.request.start.clone()
                    } else {
                        FilteredStart::Next
                    }),
                );
            }
        }
        // A rejoin can land in a recreated group with another binding.
        #[cfg(feature = "filters")]
        if rejoined {
            self.guard = None;
        }
        Ok(true)
    }

    // A numeric group id names a group inside one topic incarnation. The
    // first page pins it, and a page from a recreated stream or topic is
    // refused instead of reading an unrelated group under the same id.
    fn pin_source(&mut self, page: &FilteredPage) -> Result<(), LaserError> {
        let FilterConsumer::GroupId(id) = self.consumer else {
            return Ok(());
        };
        let incarnation = SourceIncarnation::from(page.generation);
        match self.pinned_source {
            None => {
                self.pinned_source = Some(incarnation);
                Ok(())
            }
            Some(pinned) if pinned == incarnation => Ok(()),
            Some(_) => Err(FilterError::new(
                FilterErrorReason::NotFound,
                format!(
                    "the numeric consumer group {id} belongs to a replaced stream or topic, build a new reader"
                ),
            )
            .into()),
        }
    }

    // Forget a revoked partition once nothing of it is in flight.
    fn retire(&mut self, partition_id: u32) {
        if self.revoked.contains(&partition_id)
            && self
                .partitions
                .get(&partition_id)
                .is_some_and(|progress| progress.in_flight() == 0 && progress.unstored().is_none())
        {
            self.partitions.remove(&partition_id);
            self.revoked.remove(&partition_id);
        }
    }

    fn progress(&mut self, partition_id: u32) -> Result<&mut PartitionProgress, LaserError> {
        self.partitions.get_mut(&partition_id).ok_or_else(|| {
            FilterError::new(
                FilterErrorReason::MembershipStale,
                format!(
                    "partition {partition_id} is no longer read by this reader. Its unacknowledged records are read again"
                ),
            )
            .into()
        })
    }

    fn require_sequence(&self, partition: u32, sequence: u64) -> Result<(), LaserError> {
        if self
            .partitions
            .get(&partition)
            .is_some_and(|progress| progress.knows(sequence))
        {
            return Ok(());
        }
        Err(FilterError::new(
            FilterErrorReason::MembershipStale,
            "the acknowledgment belongs to a retired partition read",
        )
        .into())
    }

    fn require_owner(&self, owner: &Arc<()>) -> Result<(), LaserError> {
        if !Arc::ptr_eq(&self.owner, owner) {
            return Err(LaserError::Invalid(
                "the page or record belongs to another filtered reader, or was read before this reader rejoined its group"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl Drop for FilteredReader {
    fn drop(&mut self) {
        self.cancel_held_waits();
    }
}

// The full definition a reader executes, for the local guard.
#[cfg(feature = "filters")]
async fn bound_definition(
    laser: &Laser,
    page: &FilteredPage,
    names: &FilterSource,
) -> Result<ConsumerFilter, LaserError> {
    let source = page.generation;
    for index in 0_u32.. {
        let bindings = laser
            .filters()
            .bindings(
                None,
                Some(&names.stream),
                Some(&names.topic),
                index,
                MAX_FILTER_CATALOG_PAGE,
            )
            .await?;
        if let Some(binding) = bindings.items.into_iter().find(|binding| {
            let id = binding.identity;
            Some(id.group_id) == page.policy.group_id
                && id.stream_id == source.stream_id
                && id.stream_created_at_micros == source.stream_created_at_micros
                && id.topic_id == source.topic_id
                && id.topic_created_at_micros == source.topic_created_at_micros
        }) {
            if Some(&binding.digest) != page.policy.digest.as_ref() {
                return Err(LaserError::Protocol(
                    "served policy differs from the group's catalog binding".to_owned(),
                ));
            }
            return definition(
                laser,
                &FilterRef::Revision {
                    filter_id: binding.filter_id,
                    revision: binding.revision,
                },
            )
            .await;
        }
        if (index + 1).saturating_mul(MAX_FILTER_CATALOG_PAGE) >= bindings.total {
            break;
        }
    }
    Err(FilterError::new(
        FilterErrorReason::NotFound,
        "no readable catalog binding matches this group identity",
    )
    .into())
}

#[cfg(feature = "filters")]
async fn definition(laser: &Laser, filter: &FilterRef) -> Result<ConsumerFilter, LaserError> {
    let (filter_id, revision) = match filter {
        FilterRef::Inline(filter) => return Ok(filter.clone()),
        FilterRef::Revision {
            filter_id,
            revision,
        } => (*filter_id, *revision),
        FilterRef::Bound | FilterRef::Group => {
            return Err(LaserError::Config(
                "resolve a group guard from the served page",
            ));
        }
    };
    let mut page = 0;
    loop {
        let revisions = laser
            .filters()
            .revisions(filter_id, page, MAX_FILTER_CATALOG_PAGE)
            .await?;
        if let Some(found) = revisions
            .items
            .into_iter()
            .find(|info| info.revision == revision)
        {
            return Ok(found.filter);
        }
        page += 1;
        if page.saturating_mul(MAX_FILTER_CATALOG_PAGE) >= revisions.total {
            return Err(FilterError::new(
                FilterErrorReason::NotFound,
                format!("filter {filter_id} has no revision {revision}"),
            )
            .into());
        }
    }
}

// The group no longer assigns the partition to this member, or its session
// ended. Reads are side-effect free, so the next one follows the synced
// assignment.
// A request that reached no primary of its partition: the node is gone, or
// another node leads the partition now.
// A node that no longer serves the partitions of a wait, or refuses for now
// as a backup does, refused the whole wait: the next round waits again at
// once, as a read is resent once, and only a second refusal in a row is
// paced. The caller's node moves on by itself after a lost link.
fn pace_refusal(
    refused_before: &mut bool,
    answer: &Result<Vec<u32>, LaserError>,
    target: Target,
) -> Option<WaitAnswer> {
    let Err(error) = answer else {
        *refused_before = false;
        return None;
    };
    let refused = match error {
        LaserError::Iggy(error) => matches!(target, Target::Replica(_)) && is_route_stale(error),
        error => error.filter_reason() == Some(FilterErrorReason::Unavailable),
    };
    (refused && !std::mem::replace(refused_before, true)).then_some(WaitAnswer::Elapsed)
}

fn is_route_moved(error: &LaserError) -> bool {
    match error {
        LaserError::Iggy(error) => is_route_stale(error),
        error => error.filter_reason() == Some(FilterErrorReason::Unavailable),
    }
}

// The two cluster calls a partition read makes.
#[trait_variant::make(Send)]
trait PartitionRoute {
    async fn send(&self, code: u32, payload: Bytes, target: Target) -> Result<Answered, IggyError>;
    async fn primary(&self, partition_id: u32) -> Result<u8, IggyError>;
}

struct ClusterRoute<'a> {
    laser: &'a Laser,
    coordinator: &'a IggyClient,
    stream: &'a Identifier,
    topic: &'a Identifier,
}

impl PartitionRoute for ClusterRoute<'_> {
    async fn send(&self, code: u32, payload: Bytes, target: Target) -> Result<Answered, IggyError> {
        self.laser
            .nodes()
            .send(self.coordinator, code, payload, target)
            .await
    }

    async fn primary(&self, partition_id: u32) -> Result<u8, IggyError> {
        partition_primary(self.coordinator, self.stream, self.topic, partition_id).await
    }
}

// One read on the partition's primary. A read the primary no longer takes
// looks the route up again and goes to the node it names, once.
async fn exchange_on(
    route: &impl PartitionRoute,
    leaders: &mut BTreeMap<u32, u8>,
    partition_id: u32,
    code: u32,
    payload: Bytes,
) -> Result<FilterOutcome, LaserError> {
    let mut rerouted = false;
    loop {
        let target = route_target(route, leaders, partition_id).await;
        // A resend rechecks the route too: right after a crash the lookup can
        // still name the dead primary, whose link holds the resend until the
        // call deadline.
        let Some(sent) =
            send_unless_moved(route, leaders, partition_id, code, payload.clone(), target).await
        else {
            rerouted = true;
            continue;
        };
        let reply = match sent {
            Ok(answered) => answered.reply,
            Err(error) => {
                let moved = is_route_stale(&error);
                if moved {
                    leaders.remove(&partition_id);
                }
                if !moved || rerouted {
                    return Err(error.into());
                }
                rerouted = true;
                continue;
            }
        };
        match decode_managed_reply::<FilterReply>(&reply)? {
            FilterReply::Ok(outcome) => return Ok(outcome),
            // A backup refuses a read of a partition it no longer leads as
            // unavailable, not as a transport error.
            FilterReply::Err(error) if error.reason == FilterErrorReason::Unavailable => {
                leaders.remove(&partition_id);
                if rerouted {
                    return Err(error.into());
                }
                rerouted = true;
            }
            FilterReply::Err(error) => return Err(error.into()),
        }
    }
}

// Send to the partition's known primary. A stopped primary holds the request
// until the client's operation deadline, while the cluster names its
// successor within moments, so a slow reply looks the route up again each
// `ROUTE_RECHECK` until it answers. `None` when another node leads the
// partition now: the request is dropped and every route through the old node
// is read again.
async fn send_unless_moved(
    route: &impl PartitionRoute,
    leaders: &mut BTreeMap<u32, u8>,
    partition_id: u32,
    code: u32,
    payload: Bytes,
    target: Target,
) -> Option<Result<Answered, IggyError>> {
    let send = route.send(code, payload, target);
    let Target::Replica(replica) = target else {
        return Some(send.await);
    };
    tokio::pin!(send);
    loop {
        if let Ok(sent) = tokio::time::timeout(ROUTE_RECHECK, &mut send).await {
            return Some(sent);
        }
        if let Ok(primary) = route.primary(partition_id).await
            && primary != replica
        {
            tracing::debug!(target: "laser", partition_id, replica, primary, "Partition {partition_id} moved from replica {replica} to {primary}, the read follows it.");
            leaders.retain(|_, leader| *leader != replica);
            leaders.insert(partition_id, primary);
            return None;
        }
    }
}

// The partition's primary, or the caller's node when the route cannot be read.
// A node that does not store the partition refuses with `NotPrimary`, and the
// request then looks the route up again.
async fn route_target(
    route: &impl PartitionRoute,
    leaders: &mut BTreeMap<u32, u8>,
    partition_id: u32,
) -> Target {
    if let Some(replica) = leaders.get(&partition_id) {
        return Target::Replica(*replica);
    }
    match route.primary(partition_id).await {
        Ok(replica) => {
            leaders.insert(partition_id, replica);
            Target::Replica(replica)
        }
        Err(error) => {
            tracing::debug!(target: "laser", partition_id, %error, "The route of partition {partition_id} is unknown, reading it on the client's own node");
            Target::Caller
        }
    }
}

// The fork refuses a group offset store from a member that does not hold the
// partition yet with `TransientNotAccepted`, a poll with `NotOwned`.
fn seed_not_held(error: &LaserError) -> bool {
    is_assignment_lost(error) || matches!(error, LaserError::Iggy(IggyError::TransientNotAccepted))
}

fn is_assignment_lost(error: &LaserError) -> bool {
    error.filter_reason() == Some(FilterErrorReason::MembershipStale)
        || matches!(
            error,
            LaserError::Iggy(IggyError::ConsumerGroupPartitionNotOwned(..))
        )
}

const fn blocked_error(partition_id: u32, blocked: Blocked) -> LaserError {
    match blocked {
        Blocked::Fault { offset, reason } => LaserError::FilterFault {
            partition_id,
            offset,
            reason,
        },
        Blocked::Oversized { offset } => LaserError::FilterOversizedRecord {
            partition_id,
            offset,
        },
    }
}

// One filtered wait per node, each naming that node's partitions.
fn wait_requests(
    template: &FilteredPollRequest,
    partitions: Vec<(Target, FilteredWaitPartition)>,
    wait: Duration,
    mut next_wait_id: impl FnMut() -> u64,
) -> Vec<(Target, FilteredWaitRequest)> {
    let max_wait_ms = u32::try_from(wait.as_millis()).unwrap_or(u32::MAX);
    let mut by_node: BTreeMap<Target, Vec<FilteredWaitPartition>> = BTreeMap::new();
    for (target, partition) in partitions {
        by_node.entry(target).or_default().push(partition);
    }
    by_node
        .into_iter()
        .map(|(target, partitions)| {
            (
                target,
                FilteredWaitRequest {
                    v: FILTER_OP_VERSION,
                    source: template.source.clone(),
                    consumer: template.consumer.clone(),
                    partitions,
                    max_wait_ms,
                    wait_id: next_wait_id(),
                },
            )
        })
        .collect()
}

// Send one cancel per node that holds a wait of this reader. Each runs at
// once, so a node that is gone never delays the others.
async fn cancel_waits(laser: &Laser, coordinator: &IggyClient, held: BTreeMap<Target, u64>) {
    let cancels = held.into_iter().map(|(target, wait_id)| async move {
        let request = FilteredWaitCancel {
            v: FILTER_OP_VERSION,
            wait_ids: vec![wait_id],
        };
        let cancelled = async {
            let payload = Bytes::from(encode_named(&request)?);
            let answered = laser
                .nodes()
                .send(coordinator, AGDX_FILTERED_WAIT_CANCEL_CODE, payload, target)
                .await?;
            match decode_managed_reply::<FilterReply>(&answered.reply)? {
                FilterReply::Ok(FilterOutcome::Cancelled(cancelled)) => Ok(cancelled.cancelled),
                FilterReply::Ok(_) => Err(unexpected("filtered wait cancel")),
                FilterReply::Err(error) => Err(LaserError::from(error)),
            }
        }
        .await;
        match cancelled {
            Ok(cancelled) => {
                tracing::debug!(target: "laser", wait_id, ?cancelled, "The filtered wait {wait_id} was cancelled.");
            }
            Err(error) => {
                tracing::debug!(target: "laser", wait_id, %error, "Cancelling the filtered wait {wait_id} failed, the server ends it at its deadline.");
            }
        }
    });
    futures::future::join_all(cancels).await;
}

fn classify_wait(
    answer: Result<Vec<u32>, LaserError>,
    held_for: Duration,
    wait: Duration,
) -> WaitAnswer {
    match answer {
        Ok(ready) if !ready.is_empty() => WaitAnswer::Ready(ready),
        Ok(_) if held_for >= wait / 2 => WaitAnswer::Elapsed,
        Ok(_) => WaitAnswer::Sleep,
        Err(error) if error.code() == ResultCode::Unsupported => WaitAnswer::Unsupported,
        Err(_) => WaitAnswer::Sleep,
    }
}

fn unexpected(verb: &str) -> LaserError {
    LaserError::Protocol(format!("{verb}: unexpected reply variant"))
}

fn validate_record_body(
    request: &FilteredPollRequest,
    page: &FilteredPage,
) -> Result<(), LaserError> {
    let invalid =
        || LaserError::Protocol("filtered record body has invalid framing or counts".to_owned());
    let head = page
        .records
        .get(..POLL_RESPONSE_HEADER_SIZE)
        .ok_or_else(invalid)?;
    let count = u32::from_le_bytes(head[12..16].try_into().map_err(|_| invalid())?);
    if count != page.matched || count > request.count {
        return Err(invalid());
    }
    let mut rest = &page.records[POLL_RESPONSE_HEADER_SIZE..];
    let mut decoded = 0u32;
    while !rest.is_empty() {
        let batch =
            decode_batch_slice_with(rest, BatchIntegrity::LayoutOnly).map_err(|_| invalid())?;
        decoded = decoded
            .checked_add(batch.header.message_count)
            .ok_or_else(invalid)?;
        if decoded > count {
            return Err(invalid());
        }
        for frame in batch.iter() {
            batch
                .header
                .base_offset
                .checked_add(u64::from(frame.header.offset_delta))
                .ok_or_else(invalid)?;
            batch
                .header
                .origin_timestamp
                .checked_add(u64::from(frame.header.timestamp_delta))
                .ok_or_else(invalid)?;
        }
        rest = rest.get(batch.header.total_size()..).ok_or_else(invalid)?;
    }
    if decoded != count {
        return Err(invalid());
    }
    Ok(())
}

fn validate_page(
    request: &FilteredPollRequest,
    page: &FilteredPage,
    polled: &PolledMessages,
) -> Result<(), LaserError> {
    let invalid = || {
        LaserError::Protocol("filtered page does not match its request or scan boundary".to_owned())
    };
    if page.v != FILTER_OP_VERSION
        || page.partition_id != request.partition_id
        || page.generation.partition_id != request.partition_id
        || polled.partition_id != request.partition_id
        || polled.current_offset != page.frontier
        || polled.context().incarnation != page.generation.partition_incarnation
        || polled.context().owner_generation != page.ack_context.owner_generation
        || polled.context().metadata_op != page.ack_context.metadata_op
        || page.matched as usize != polled.messages.len()
        || page.matched > page.examined
        || request
            .max_examined
            .is_some_and(|maximum| page.examined > maximum)
        || page.policy.validate().is_err()
    {
        return Err(invalid());
    }
    // A strict group read runs a binding. An unfiltered page is the whole
    // examined range, so nothing was selected out of it.
    match page.policy.mode {
        ExecutionMode::Filtered => {}
        ExecutionMode::Unfiltered
            if matches!(request.filter, FilterRef::Bound)
                || page.matched != page.examined
                || page.policy.filter_id.is_some()
                || page.policy.revision.is_some() =>
        {
            return Err(invalid());
        }
        ExecutionMode::Unfiltered => {}
    }
    if let FilterConsumer::GroupId(id) = request.consumer
        && page.policy.group_id != Some(id)
    {
        return Err(LaserError::Protocol(
            "filtered page identifies another consumer group".to_owned(),
        ));
    }
    if request.consumer.is_group() != page.policy.group_id.is_some() {
        return Err(invalid());
    }
    let start = match &request.start {
        FilteredStart::Continue(cursor) => {
            if cursor.generation != page.generation
                || cursor.digest != page.policy.digest
                || cursor.group_id != page.policy.group_id
                || cursor.mode != page.policy.mode
                || cursor.policy_generation != page.policy.policy_generation
            {
                return Err(invalid());
            }
            Some(cursor.next_scan_offset)
        }
        FilteredStart::Offset(offset) => Some(*offset),
        FilteredStart::First => Some(0),
        _ => None,
    };
    // The last offset the page examined, which the page offers as its safe
    // acknowledgment offset.
    let scanned_to = page
        .next_scan_offset
        .and_then(|offset| offset.checked_sub(1));
    if page.examined == 0 {
        if page.safe_ack_offset.is_some() || page.next_scan_offset != start {
            return Err(invalid());
        }
    } else if scanned_to.is_none() || page.safe_ack_offset != scanned_to {
        return Err(invalid());
    }
    if scanned_to.is_some_and(|scanned| scanned > page.frontier) {
        return Err(invalid());
    }
    if start
        .zip(page.next_scan_offset)
        .is_some_and(|(start, next)| next < start)
    {
        return Err(invalid());
    }
    let mut previous = None;
    for message in &polled.messages {
        let offset = message.header.offset;
        if previous.is_some_and(|last| offset <= last)
            || start.is_some_and(|start| offset < start)
            || scanned_to.is_none_or(|scanned| offset > scanned)
            || offset > page.frontier
        {
            return Err(invalid());
        }
        previous = Some(offset);
    }
    // The returned offsets are strictly ascending, so a binary search proves
    // each unevaluated offset is one of them.
    let unevaluated: BTreeSet<_> = page.unevaluated.iter().copied().collect();
    if unevaluated.len() != page.unevaluated.len()
        || unevaluated.iter().any(|offset| {
            polled
                .messages
                .binary_search_by_key(offset, |message| message.header.offset)
                .is_err()
        })
    {
        return Err(invalid());
    }
    if matches!(page.stop, StopReason::Fault | StopReason::OversizedRecord) != page.fault.is_some()
    {
        return Err(invalid());
    }
    if let Some(fault) = page.fault
        && scanned_to.is_some_and(|scanned| scanned >= fault.offset)
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::filter::{AckContext, FilterExpr, FilteredPage, FilteredWaitCancelled};

    #[tokio::test]
    async fn given_a_rerouted_read_when_the_route_still_names_the_silent_primary_then_should_recheck_it_and_follow_the_successor()
     {
        let outcome = FilterOutcome::Cancelled(FilteredWaitCancelled { cancelled: vec![7] });
        let route = ScriptedRoute {
            answers: Mutex::new(VecDeque::from([
                Answer::Refuse(IggyError::NotPrimary),
                Answer::Silent,
                Answer::Reply(Bytes::from(
                    encode_named(&FilterReply::Ok(outcome.clone())).expect("encoded reply"),
                )),
            ])),
            primaries: Mutex::new(VecDeque::from([1, 2])),
            sent: Mutex::new(Vec::new()),
        };
        let mut leaders = BTreeMap::from([(0, 1)]);

        let answered = tokio::time::timeout(
            Duration::from_secs(5),
            exchange_on(
                &route,
                &mut leaders,
                0,
                AGDX_FILTERED_POLL_CODE,
                Bytes::new(),
            ),
        )
        .await
        .expect("the resend follows the successor long before the call deadline")
        .expect("the successor answers");

        assert_eq!(answered, outcome);
        assert_eq!(
            *route.sent.lock().expect("sent lock"),
            vec![Target::Replica(1), Target::Replica(1), Target::Replica(2)]
        );
        assert_eq!(leaders, BTreeMap::from([(0, 2)]));
    }

    fn sample() -> (FilteredPollRequest, FilteredPage, PolledMessages) {
        let filter = laser_wire::filter::ConsumerFilter::json(FilterExpr::present("mode"));
        let request = FilteredPollRequest {
            v: FILTER_OP_VERSION,
            source: FilterSource {
                stream: "orbit".to_owned(),
                topic: "fleet_changes".to_owned(),
            },
            partition_id: 0,
            consumer: FilterConsumer::Group("anomaly-desk".to_owned()),
            filter: FilterRef::Bound,
            start: FilteredStart::First,
            count: 10,
            max_reply_bytes: 1024,
            max_examined: None,
            min_catalog_position: None,
        };
        let page = FilteredPage {
            v: FILTER_OP_VERSION,
            partition_id: 0,
            policy: AppliedPolicy {
                group_id: Some(9),
                digest: Some(filter.digest()),
                filter_id: Some(1),
                revision: Some(1),
                mode: ExecutionMode::Filtered,
                policy_generation: 1,
            },
            generation: SourceGeneration {
                stream_id: 1,
                stream_created_at_micros: 1,
                topic_id: 1,
                topic_created_at_micros: 1,
                partition_id: 0,
                partition_incarnation: 1,
            },
            ack_context: AckContext {
                owner_generation: 2,
                metadata_op: 3,
            },
            next_scan_offset: Some(10),
            safe_ack_offset: Some(9),
            frontier: 9,
            examined: 10,
            matched: 0,
            stop: StopReason::EndOfVisible,
            fault: None,
            unevaluated: Vec::new(),
            evaluation_limits: None,
            records: Vec::new(),
        };
        let polled = PolledMessages::new(
            PartitionContext {
                incarnation: 1,
                owner_generation: 2,
                metadata_op: 3,
            },
            0,
            9,
            0,
            Vec::new(),
        );
        (request, page, polled)
    }

    #[test]
    fn given_empty_scanned_pages_when_validated_then_should_accept_only_consistent_progress() {
        let (request, page, polled) = sample();
        validate_page(&request, &page, &polled).expect("valid scanned range");
        let mut malformed = page.clone();
        malformed.safe_ack_offset = Some(10);
        assert!(validate_page(&request, &malformed, &polled).is_err());
        malformed = page.clone();
        malformed.policy.digest = None;
        assert!(
            validate_page(&request, &malformed, &polled).is_err(),
            "a filtered page names the digest it ran"
        );
        malformed = page.clone();
        malformed.partition_id = 1;
        assert!(validate_page(&request, &malformed, &polled).is_err());
        malformed = page;
        malformed.examined = 0;
        assert!(validate_page(&request, &malformed, &polled).is_err());
    }

    #[test]
    fn given_an_unfiltered_page_when_validated_then_should_need_a_group_read_without_a_digest() {
        let (mut request, mut page, polled) = sample();
        page.policy = AppliedPolicy::unfiltered(9, 0);
        page.matched = 0;
        page.examined = 0;
        page.safe_ack_offset = None;
        page.next_scan_offset = Some(0);
        assert!(
            validate_page(&request, &page, &polled).is_err(),
            "a strict read never accepts an unfiltered page"
        );
        request.filter = FilterRef::Group;
        validate_page(&request, &page, &polled).expect("an automatic read may run unfiltered");
        page.examined = 3;
        page.next_scan_offset = Some(3);
        page.safe_ack_offset = Some(2);
        assert!(
            validate_page(&request, &page, &polled).is_err(),
            "an unfiltered page delivers everything it examined"
        );
    }

    #[test]
    fn given_a_continuation_when_the_reply_changes_history_or_policy_then_should_reject() {
        let (mut request, mut page, polled) = sample();
        let cursor = laser_wire::filter::Continuation {
            group_id: Some(9),
            next_scan_offset: 0,
            generation: page.generation,
            digest: page.policy.digest.clone(),
            mode: ExecutionMode::Filtered,
            policy_generation: 1,
        };
        request.start = FilteredStart::Continue(cursor.clone());
        validate_page(&request, &page, &polled).expect("the same history and policy continue");
        page.generation.partition_incarnation = 2;
        assert!(validate_page(&request, &page, &polled).is_err());
        page.generation.partition_incarnation = 1;
        page.policy.policy_generation = 2;
        assert!(
            validate_page(&request, &page, &polled).is_err(),
            "a page under another policy generation does not continue this cursor"
        );
    }

    #[test]
    fn given_an_examined_limit_when_a_reply_exceeds_it_then_should_refuse_the_page() {
        let (mut request, page, polled) = sample();
        request.max_examined = Some(page.examined);
        validate_page(&request, &page, &polled).expect("the scan reaches its limit");
        request.max_examined = Some(page.examined - 1);
        assert!(validate_page(&request, &page, &polled).is_err());
    }

    #[test]
    fn given_a_record_head_from_another_owner_when_validated_then_should_refuse_the_page() {
        let (request, page, polled) = sample();
        validate_page(&request, &page, &polled).expect("the head names the page's owner");
        let context = polled.context();
        let other_owner = PolledMessages::new(
            PartitionContext {
                owner_generation: context.owner_generation + 1,
                ..context
            },
            polled.partition_id,
            polled.current_offset,
            polled.count,
            Vec::new(),
        );
        assert!(
            validate_page(&request, &page, &other_owner).is_err(),
            "the records were served under another owner than the page acknowledges with"
        );
        let other_incarnation = PolledMessages::new(
            PartitionContext {
                incarnation: context.incarnation + 1,
                ..context
            },
            polled.partition_id,
            polled.current_offset,
            polled.count,
            Vec::new(),
        );
        assert!(validate_page(&request, &page, &other_incarnation).is_err());
    }

    #[test]
    fn given_a_forged_inner_count_when_decoding_then_should_reject_before_allocation() {
        let (request, mut page, _) = sample();
        page.records = vec![0; POLL_RESPONSE_HEADER_SIZE];
        page.records[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(validate_record_body(&request, &page).is_err());
    }

    #[test]
    fn given_a_scanned_page_without_an_acknowledgment_offset_when_validated_then_should_reject() {
        let (request, mut page, polled) = sample();
        page.safe_ack_offset = None;
        assert!(
            validate_page(&request, &page, &polled).is_err(),
            "a page that examined records offers the last one for acknowledgment"
        );
    }

    #[test]
    fn given_a_page_that_examined_nothing_when_its_scan_offset_moved_then_should_reject() {
        let (request, mut page, polled) = sample();
        page.examined = 0;
        page.safe_ack_offset = None;
        page.next_scan_offset = Some(0);
        validate_page(&request, &page, &polled).expect("an empty scan repeats its start");
        page.next_scan_offset = Some(5);
        assert!(validate_page(&request, &page, &polled).is_err());
    }

    #[test]
    fn given_a_wait_that_found_records_when_classified_then_should_read_the_ready_partitions() {
        assert_eq!(
            classify_wait(
                Ok(vec![2, 0]),
                Duration::from_millis(10),
                Duration::from_secs(5)
            ),
            WaitAnswer::Ready(vec![2, 0])
        );
    }

    #[test]
    fn given_an_empty_answer_at_its_deadline_when_classified_then_should_start_the_next_round() {
        assert_eq!(
            classify_wait(
                Ok(Vec::new()),
                Duration::from_secs(5),
                Duration::from_secs(5)
            ),
            WaitAnswer::Elapsed
        );
    }

    #[test]
    fn given_an_empty_answer_well_before_its_deadline_when_classified_then_should_pace_by_the_idle_interval()
     {
        assert_eq!(
            classify_wait(
                Ok(Vec::new()),
                Duration::from_millis(5),
                Duration::from_secs(5)
            ),
            WaitAnswer::Sleep
        );
    }

    #[test]
    fn given_a_server_without_filtered_waits_when_classified_then_should_stop_waiting() {
        let refused = LaserError::from(FilterError::new(
            FilterErrorReason::Unsupported,
            "this server holds no filtered wait",
        ));
        let unknown = LaserError::from(laser_wire::result::CommandError::new(
            laser_wire::result::ResultCode::Unsupported,
            "unknown command",
        ));
        for error in [refused, unknown] {
            assert_eq!(
                classify_wait(Err(error), Duration::from_millis(1), Duration::from_secs(5)),
                WaitAnswer::Unsupported
            );
        }
    }

    #[test]
    fn given_a_wait_refused_by_a_node_that_moved_when_answered_then_should_wait_again_at_once_and_pace_only_a_second_refusal()
     {
        let moved = || Err(LaserError::from(IggyError::NotPrimary));
        let unavailable = || {
            Err(LaserError::from(FilterError::new(
                FilterErrorReason::Unavailable,
                "a backup refuses for now",
            )))
        };
        let mut refused_before = false;

        assert_eq!(
            pace_refusal(&mut refused_before, &moved(), Target::Replica(1)),
            Some(WaitAnswer::Elapsed),
            "the first refusal waits again at once"
        );
        assert_eq!(
            pace_refusal(&mut refused_before, &unavailable(), Target::Replica(2)),
            None,
            "a second refusal in a row is paced"
        );
        assert_eq!(
            pace_refusal(&mut refused_before, &Ok(Vec::new()), Target::Replica(2)),
            None
        );
        assert_eq!(
            pace_refusal(&mut refused_before, &unavailable(), Target::Caller),
            Some(WaitAnswer::Elapsed),
            "an answer ends the run of refusals"
        );
        assert_eq!(
            pace_refusal(&mut false, &moved(), Target::Caller),
            None,
            "the caller's node moves on by itself after a lost link"
        );
    }

    #[test]
    fn given_a_failed_wait_when_classified_then_should_pace_by_the_idle_interval() {
        let error = LaserError::from(FilterError::new(
            FilterErrorReason::Unavailable,
            "too many waiting reads",
        ));
        assert_eq!(
            classify_wait(Err(error), Duration::from_millis(1), Duration::from_secs(5)),
            WaitAnswer::Sleep
        );
    }

    #[test]
    fn given_partitions_on_several_nodes_when_waiting_then_should_send_one_wait_per_node_each_with_its_own_id()
     {
        let (template, _, _) = sample();
        let partitions = vec![
            (
                Target::Replica(2),
                FilteredWaitPartition {
                    partition_id: 0,
                    offset: 10,
                },
            ),
            (
                Target::Replica(1),
                FilteredWaitPartition {
                    partition_id: 1,
                    offset: 4,
                },
            ),
            (
                Target::Replica(2),
                FilteredWaitPartition {
                    partition_id: 2,
                    offset: 0,
                },
            ),
            (
                Target::Caller,
                FilteredWaitPartition {
                    partition_id: 3,
                    offset: 7,
                },
            ),
        ];

        let mut wait_ids = 40..;
        let requests = wait_requests(&template, partitions, Duration::from_millis(1500), || {
            wait_ids.next().expect("wait ids never run out")
        });

        let wait = |partitions: Vec<FilteredWaitPartition>, wait_id: u64| FilteredWaitRequest {
            v: FILTER_OP_VERSION,
            source: template.source.clone(),
            consumer: template.consumer.clone(),
            partitions,
            max_wait_ms: 1500,
            wait_id,
        };
        assert_eq!(
            requests,
            vec![
                (
                    Target::Caller,
                    wait(
                        vec![FilteredWaitPartition {
                            partition_id: 3,
                            offset: 7
                        }],
                        40
                    )
                ),
                (
                    Target::Replica(1),
                    wait(
                        vec![FilteredWaitPartition {
                            partition_id: 1,
                            offset: 4
                        }],
                        41
                    )
                ),
                (
                    Target::Replica(2),
                    wait(
                        vec![
                            FilteredWaitPartition {
                                partition_id: 0,
                                offset: 10
                            },
                            FilteredWaitPartition {
                                partition_id: 2,
                                offset: 0
                            },
                        ],
                        42
                    )
                ),
            ]
        );
    }

    enum Answer {
        Refuse(IggyError),
        Silent,
        Reply(Bytes),
    }

    // Answers each send with the next scripted answer, and each route lookup
    // with the next primary, repeating the last one.
    struct ScriptedRoute {
        answers: Mutex<VecDeque<Answer>>,
        primaries: Mutex<VecDeque<u8>>,
        sent: Mutex<Vec<Target>>,
    }

    impl PartitionRoute for ScriptedRoute {
        async fn send(
            &self,
            _code: u32,
            _payload: Bytes,
            target: Target,
        ) -> Result<Answered, IggyError> {
            self.sent.lock().expect("sent lock").push(target);
            let answer = self.answers.lock().expect("answers lock").pop_front();
            match answer {
                Some(Answer::Refuse(error)) => Err(error),
                Some(Answer::Reply(reply)) => Ok(Answered {
                    reply,
                    replica: None,
                }),
                Some(Answer::Silent) | None => std::future::pending().await,
            }
        }

        async fn primary(&self, _partition_id: u32) -> Result<u8, IggyError> {
            let mut primaries = self.primaries.lock().expect("primaries lock");
            let primary = if primaries.len() > 1 {
                primaries.pop_front()
            } else {
                primaries.front().copied()
            };
            primary.ok_or(IggyError::NotPrimary)
        }
    }
}
