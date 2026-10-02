use crate::error::{LaserError, decode_managed_reply};
use crate::filters::group::Membership;
#[cfg(feature = "filters")]
use crate::filters::guard::LocalGuard;
use crate::filters::progress::{Blocked, PartitionProgress};
use crate::filters::route::{RouteTo, Routes};
use crate::iggy::prelude::{
    Client, IggyClient, IggyClientBuilder, IggyError, IggyMessage, PolledMessages, StreamClient,
    TopicClient,
};
use crate::laser::Laser;
use bytes::Bytes;
use iggy_binary_protocol::batch::{BatchIntegrity, decode_batch_slice_with};
use iggy_common::Identifier;
use laser_wire::codes::{AGDX_FILTERED_ACK_CODE, AGDX_FILTERED_POLL_CODE, FILTER_OP_VERSION};
#[cfg(feature = "filters")]
use laser_wire::filter::ConsumerFilter;
use laser_wire::filter::{
    AppliedPolicy, CatalogPosition, ExecutionMode, FilterConsumer, FilterError, FilterErrorReason,
    FilterOutcome, FilterRef, FilterReply, FilterSource, FilteredAck, FilteredPage,
    FilteredPollRequest, FilteredStart, ReadMode, SourceGeneration, StopReason,
};
use laser_wire::framing::encode_named;
#[cfg(feature = "filters")]
use laser_wire::limits::MAX_FILTER_CATALOG_PAGE;
use laser_wire::validate::Validate;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_COUNT: u32 = 100;
const DEFAULT_MAX_REPLY_BYTES: u32 = 1024 * 1024;
pub(crate) const DEFAULT_IDLE_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_MAX_UNACKED_PAGES: usize = 1024;

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
    read_mode: ReadMode,
    #[cfg(feature = "filters")]
    local_guard: bool,
    idle_interval: Duration,
    retry_interval: Option<Duration>,
    max_unacked_pages: usize,
    selected_partitions: BTreeSet<u32>,
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
/// is read again, while the other partitions keep reading. A purge or a new
/// group binding restarts the partition from its stored offset once, and the
/// error that reports it reaches the caller.
///
/// Every method is cancel-safe: a cancelled call leaves the position and the
/// unstored progress as they were, and the next call repeats the work.
pub struct FilteredReader {
    owner: Arc<()>,
    laser: Laser,
    // The connection that holds the group membership and whose consumer
    // session the data connections attach to. A group reader owns its own, so
    // two readers are two members.
    coordinator: Arc<IggyClient>,
    consumer: FilterConsumer,
    // The request every read sends, with the partition and the start set per
    // read.
    request: FilteredPollRequest,
    idle_interval: Duration,
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
    routes: Routes,
    // Data connections opened by routes this reader already retired on a rejoin.
    retired_connections: u64,
    cursor: usize,
    sequence: u64,
    examined_in_round: u64,
    lost_routes: BTreeSet<u32>,
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
    /// handled too. Absent on a local page, which cannot be acknowledged.
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
            read_mode: ReadMode::Primary,
            #[cfg(feature = "filters")]
            local_guard: false,
            idle_interval: DEFAULT_IDLE_INTERVAL,
            retry_interval: None,
            max_unacked_pages: DEFAULT_MAX_UNACKED_PAGES,
            selected_partitions: BTreeSet::new(),
        }
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

    /// `Primary` (the default) reads from each partition primary and can
    /// acknowledge, and needs a Laser built from a connection string. `Local`
    /// reads whatever replica this connection reaches, cannot acknowledge, and
    /// keeps no pending pages.
    #[must_use]
    pub fn read_mode(mut self, read_mode: ReadMode) -> Self {
        self.read_mode = read_mode;
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
    /// nothing new, and how long a blocked partition waits before it is read
    /// again.
    #[must_use]
    pub fn idle_interval(mut self, interval: Duration) -> Self {
        self.idle_interval = interval;
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
        if self.read_mode == ReadMode::Primary && self.laser.connection_string().is_none() {
            return Err(LaserError::Config(
                "primary group reads open data connections, so they need a Laser built from a connection string. Read in ReadMode::Local for a bring-your-own client",
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
            read_mode: self.read_mode,
            max_examined: self.max_examined,
            min_catalog_position: self.min_catalog_position,
        };
        request.validate()?;
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
        let partitions = membership
            .partitions()
            .iter()
            .filter(|partition_id| {
                self.selected_partitions.is_empty()
                    || self.selected_partitions.contains(partition_id)
            })
            .map(|partition_id| (*partition_id, PartitionProgress::new(self.start.clone())))
            .collect();
        Ok(FilteredReader {
            owner: Arc::new(()),
            laser: self.laser.clone(),
            coordinator,
            consumer,
            request,
            idle_interval: self.idle_interval,
            retry_interval: self.retry_interval.unwrap_or(self.idle_interval),
            max_unacked_pages: self.max_unacked_pages,
            #[cfg(feature = "filters")]
            guard: None,
            #[cfg(feature = "filters")]
            guard_enabled: self.local_guard,
            membership: Some(membership),
            partitions,
            revoked: BTreeSet::new(),
            selected_partitions: self.selected_partitions,
            routes: Routes::default(),
            retired_connections: 0,
            cursor: 0,
            sequence: 0,
            examined_in_round: 0,
            lost_routes: BTreeSet::new(),
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
                Round::Idle => sleep(self.idle_interval).await,
            }
        }
    }

    /// Read each partition in turn until a page matches or every partition
    /// has nothing new, without the idle wait. `None` when nothing is new, so
    /// a caller that drives its own loop waits [`idle_interval`](Self::idle_interval)
    /// before it asks again.
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

    /// How long this reader waits when nothing is new.
    pub const fn idle_interval(&self) -> Duration {
        self.idle_interval
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
        self.require_acknowledgments()?;
        self.require_owner(&record.owner)?;
        self.require_sequence(record.partition_id, record.sequence)?;
        self.progress(record.partition_id)?
            .complete_record(record.sequence, record.offset);
        self.flush(record.partition_id).await
    }

    /// Mark every record on this partition through `record` handled, and
    /// store that completed prefix. Process all preceding records first.
    /// Records after it remain pending, even inside the same page.
    pub async fn ack_through(&mut self, record: &MatchedRecord) -> Result<(), LaserError> {
        self.require_acknowledgments()?;
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
        self.flush(record.partition_id).await
    }

    /// Mark every record of `page` handled, and store the progress this
    /// completes.
    pub async fn ack_page(&mut self, page: &MatchedPage) -> Result<(), LaserError> {
        self.require_acknowledgments()?;
        self.require_owner(&page.owner)?;
        self.require_sequence(page.partition_id, page.sequence)?;
        self.progress(page.partition_id)?
            .complete_page(page.sequence);
        self.flush(page.partition_id).await
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
        self.require_acknowledgments()?;
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
        self.flush(delivery.partition_id).await
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

    /// Data connections this reader opened to partition primaries. A healthy
    /// reader opens one per node and keeps it: the count grows only when a
    /// node stops being primary, a connection fails, or the group session
    /// ends. A local reader opens none.
    pub fn data_connections_opened(&self) -> u64 {
        self.retired_connections + self.routes.opened()
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
        if self.request.read_mode == ReadMode::Primary {
            for partition_id in self.partitions.keys().copied().collect::<Vec<_>>() {
                let flushed = self.flush(partition_id).await;
                outcome = outcome.and(flushed);
            }
        }
        outcome
    }

    /// Store completed progress, leave the group, and close the data
    /// connections. Handled records that were not acknowledged are read again
    /// by the next reader. A reader dropped without `close` leaves its group
    /// when its own coordinator connection closes.
    pub async fn close(mut self) -> Result<(), LaserError> {
        let mut outcome = self.flush_completed().await;
        if let Some(membership) = &self.membership {
            outcome = outcome.and(membership.leave(&self.coordinator).await);
        }
        std::mem::take(&mut self.routes).close().await;
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
        let readable = self.partitions();
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
                self.lost_routes.remove(&partition_id);
                page
            }
            Ok(_) => return Err(unexpected("filtered poll")),
            Err(error) if is_route_lost(&error) => {
                if !self.lost_routes.insert(partition_id) {
                    return Err(error);
                }
                if let Some(membership) = self.membership.as_mut() {
                    membership.expire();
                }
                return Ok(Read::Empty { more: false });
            }
            Err(error) => return Err(self.restart_after(partition_id, error)),
        };
        if page.records.len() > self.request.max_reply_bytes as usize + 16 {
            return Err(LaserError::Protocol(
                "filtered page exceeds the requested record byte limit".to_owned(),
            ));
        }
        validate_record_body(&self.request, &page)?;
        let polled = PolledMessages::from_bytes(Bytes::from(std::mem::take(&mut page.records)))?;
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
        let primary = self.request.read_mode == ReadMode::Primary;
        let retry_at = Instant::now() + self.idle_interval;
        let progress = self.progress(partition_id)?;
        // A local page cannot be acknowledged, so its records are not pending
        // work and never saturate the partition.
        progress.record(
            sequence,
            &page,
            messages
                .iter()
                .filter(|_| primary)
                .map(|message| message.header.offset),
        );
        let blocked = progress.blocked();
        if blocked.is_some() {
            progress.defer(retry_at);
        }
        self.examined_in_round += u64::from(page.examined);
        if messages.is_empty() {
            if primary && let Err(error) = self.flush(partition_id).await {
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

    // A purge, a recreated group, or a new group binding ends the history or
    // the policy the partition's continuation was issued for. The partition
    // starts over from its stored offset, the guard reloads a rebound
    // filter, and the error reaches the caller once.
    fn restart_after(&mut self, partition_id: u32, error: LaserError) -> LaserError {
        let reason = error.filter_reason();
        if !matches!(
            reason,
            Some(FilterErrorReason::SourceChanged | FilterErrorReason::Conflict)
        ) {
            return error;
        }
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

    // Store the completed prefix of one partition. A lost reply keeps the
    // target unstored, and the next store sends it again. Stores are
    // monotone, so a repeat never moves progress back.
    async fn flush(&mut self, partition_id: u32) -> Result<(), LaserError> {
        let Some(target) = self.progress(partition_id)?.unstored().cloned() else {
            self.retire(partition_id);
            return Ok(());
        };
        let ack = FilteredAck {
            group_id: target.group_id,
            v: FILTER_OP_VERSION,
            source: self.request.source.clone(),
            partition_id,
            consumer: self.consumer.clone(),
            generation: target.generation,
            digest: target.digest.clone(),
            offset: target.offset,
            mode: target.mode,
            policy_generation: target.policy_generation,
        };
        let payload = encode_named(&ack)
            .map_err(|error| LaserError::Codec(format!("encode filter request: {error}")))?;
        let mut rerouted = false;
        loop {
            match self
                .exchange(partition_id, AGDX_FILTERED_ACK_CODE, payload.clone())
                .await
            {
                Ok(FilterOutcome::Acknowledged(receipt)) => {
                    if receipt.partition_id != partition_id
                        || receipt.offset != target.offset
                        || receipt.generation != target.generation
                    {
                        return Err(LaserError::Protocol(
                            "filtered acknowledgment does not match its request".to_owned(),
                        ));
                    }
                    self.progress(partition_id)?.stored(&target);
                    let revoked = self.revoked.contains(&partition_id);
                    self.retire(partition_id);
                    if revoked {
                        self.routes.release(partition_id).await;
                    }
                    return Ok(());
                }
                Ok(_) => return Err(unexpected("filtered acknowledgment")),
                Err(error) if is_route_lost(&error) && !rerouted => rerouted = true,
                Err(error) => return Err(self.restart_after(partition_id, error)),
            }
        }
    }

    async fn exchange(
        &mut self,
        partition_id: u32,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<FilterOutcome, LaserError> {
        let payload = Bytes::from(payload);
        let reply = match self.request.read_mode {
            ReadMode::Local => self.coordinator.send_binary_request(code, payload).await?,
            ReadMode::Primary => {
                let route_to = RouteTo {
                    source: &self.request.source,
                    consumer: &self.consumer,
                };
                let connection = self
                    .routes
                    .connection(
                        &self.coordinator,
                        self.laser.connection_string(),
                        &route_to,
                        partition_id,
                        code == AGDX_FILTERED_ACK_CODE,
                    )
                    .await?;
                match connection.send_binary_request(code, payload).await {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.routes.invalidate(partition_id).await;
                        return Err(error.into());
                    }
                }
            }
        };
        match decode_managed_reply::<FilterReply>(&reply)? {
            FilterReply::Ok(outcome) => Ok(outcome),
            FilterReply::Err(error) => {
                if matches!(
                    error.reason,
                    FilterErrorReason::NotPrimary | FilterErrorReason::MembershipStale
                ) {
                    self.routes.invalidate(partition_id).await;
                }
                Err(error.into())
            }
        }
    }

    // Follow the group's current assignment. The whole change is applied to
    // the reader before anything awaits, so a cancelled call never loses it.
    // A partition that left the assignment stops being read but keeps its
    // in-flight pages, so records already handed out can still be
    // acknowledged while the server's offset fence allows it.
    async fn refresh_membership(&mut self) -> Result<(), LaserError> {
        let Some(membership) = self.membership.as_mut() else {
            return Ok(());
        };
        if !membership.sync(&self.coordinator).await? {
            return Ok(());
        }
        let assigned: BTreeSet<u32> = membership
            .partitions()
            .iter()
            .copied()
            .filter(|partition| {
                self.selected_partitions.is_empty() || self.selected_partitions.contains(partition)
            })
            .collect();
        let stale_routes = if membership.take_rejoined() {
            self.owner = Arc::new(());
            self.partitions.clear();
            self.revoked.clear();
            self.buffered.clear();
            Some(std::mem::take(&mut self.routes))
        } else {
            None
        };
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
        for partition_id in assigned {
            if self.revoked.remove(&partition_id) || !self.partitions.contains_key(&partition_id) {
                self.buffered
                    .retain(|record| record.partition_id != partition_id);
                self.partitions
                    .insert(partition_id, PartitionProgress::new(FilteredStart::Next));
            }
        }
        // A rejoin can land in a recreated group with another binding.
        #[cfg(feature = "filters")]
        if stale_routes.is_some() {
            self.guard = None;
        }
        let retired: Vec<_> = left
            .into_iter()
            .filter_map(|partition| self.routes.forget(partition))
            .collect();
        if let Some(routes) = stale_routes {
            self.retired_connections += routes.opened();
            routes.close().await;
        }
        for client in retired {
            let _ = client.shutdown().await;
        }
        Ok(())
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

    fn require_acknowledgments(&self) -> Result<(), LaserError> {
        if self.request.read_mode == ReadMode::Local {
            return Err(LaserError::Invalid(
                "a ReadMode::Local reader cannot acknowledge: only the partition primary proves the history a page came from".to_owned(),
            ));
        }
        Ok(())
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

// The data connection no longer reaches the partition primary for this
// consumer: the primary moved, the attached session ended, or the group
// assignment changed. Reads are side-effect free, so the next one routes again.
fn is_route_lost(error: &LaserError) -> bool {
    matches!(
        error.filter_reason(),
        Some(FilterErrorReason::NotPrimary | FilterErrorReason::MembershipStale)
    ) || matches!(
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

fn unexpected(verb: &str) -> LaserError {
    LaserError::Protocol(format!("{verb}: unexpected reply variant"))
}

fn validate_record_body(
    request: &FilteredPollRequest,
    page: &FilteredPage,
) -> Result<(), LaserError> {
    let invalid =
        || LaserError::Protocol("filtered record body has invalid framing or counts".to_owned());
    let head = page.records.get(..16).ok_or_else(invalid)?;
    let count = u32::from_le_bytes(head[12..16].try_into().map_err(|_| invalid())?);
    if count != page.matched || count > request.count {
        return Err(invalid());
    }
    let mut rest = &page.records[16..];
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
        || page.read_mode != request.read_mode
        || polled.current_offset != page.frontier
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
                || cursor.read_mode != request.read_mode
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
    // The last offset the page examined. A primary page offers it as its safe
    // acknowledgment offset, a local page offers none.
    let scanned_to = page
        .next_scan_offset
        .and_then(|offset| offset.checked_sub(1));
    if page.examined == 0 {
        if page.safe_ack_offset.is_some() || page.next_scan_offset != start {
            return Err(invalid());
        }
    } else if scanned_to.is_none()
        || match request.read_mode {
            ReadMode::Primary => page.safe_ack_offset != scanned_to,
            ReadMode::Local => page.safe_ack_offset.is_some(),
        }
    {
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
    use laser_wire::filter::{FilterExpr, FilteredPage};

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
            read_mode: ReadMode::Primary,
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
                partition_created_revision: 1,
                purge_generation: 0,
            },
            read_mode: ReadMode::Primary,
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
        let polled = PolledMessages {
            partition_id: 0,
            current_offset: 9,
            count: 0,
            messages: Vec::new(),
        };
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
        malformed.read_mode = ReadMode::Local;
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
            read_mode: ReadMode::Primary,
            mode: ExecutionMode::Filtered,
            policy_generation: 1,
        };
        request.start = FilteredStart::Continue(cursor.clone());
        validate_page(&request, &page, &polled).expect("the same history and policy continue");
        page.generation.purge_generation = 1;
        assert!(validate_page(&request, &page, &polled).is_err());
        page.generation.purge_generation = 0;
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
    fn given_a_forged_inner_count_when_decoding_then_should_reject_before_allocation() {
        let (request, mut page, _) = sample();
        page.records = vec![0; 16];
        page.records[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(validate_record_body(&request, &page).is_err());
    }

    #[test]
    fn given_a_local_page_when_validated_then_should_carry_no_acknowledgment_offset() {
        let (mut request, mut page, polled) = sample();
        request.read_mode = ReadMode::Local;
        page.read_mode = ReadMode::Local;
        page.safe_ack_offset = None;
        validate_page(&request, &page, &polled).expect("a scanned local page");
        page.safe_ack_offset = Some(9);
        assert!(
            validate_page(&request, &page, &polled).is_err(),
            "a local page never offers an acknowledgment"
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
}
