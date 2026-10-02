use crate::codes::FILTER_OP_VERSION;
use crate::error::InvalidError;
use crate::filter::catalog::CatalogPosition;
use crate::filter::expr::ConsumerFilter;
use crate::limits::{
    MAX_FILTER_PREVIEW_EXAMINED, MAX_FILTER_PREVIEW_RECORDS, MAX_FILTER_SAMPLE_BYTES,
    MAX_FILTER_SAMPLE_HEADERS, MAX_FILTER_SOURCE_NAME_BYTES, MAX_FILTERED_PAGE_BYTES,
    MAX_FILTERED_PAGE_EXAMINED, MAX_FILTERED_PAGE_RECORDS,
};
use crate::result::ResultCode;
use crate::schema::Digest32;
use crate::validate::Validate;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// The stream and topic a filtered read or preview addresses, by name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterSource {
    pub stream: String,
    pub topic: String,
}

/// Whose progress a filtered read follows: an independent consumer or a
/// consumer group. Groups accept either a name or a numeric ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterConsumer {
    Consumer(String),
    Group(String),
    GroupId(u64),
}

impl fmt::Display for FilterConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Consumer(name) | Self::Group(name) => formatter.write_str(name),
            Self::GroupId(id) => write!(formatter, "{id}"),
        }
    }
}

impl FilterConsumer {
    pub const fn is_group(&self) -> bool {
        matches!(self, Self::Group(_) | Self::GroupId(_))
    }
}

impl Serialize for FilterConsumer {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("FilterConsumer", 2)?;
        match self {
            Self::Consumer(name) => {
                state.serialize_field("kind", "consumer")?;
                state.serialize_field("name", name)?;
            }
            Self::Group(name) => {
                state.serialize_field("kind", "group")?;
                state.serialize_field("name", name)?;
            }
            Self::GroupId(id) => {
                state.serialize_field("kind", "group_id")?;
                state.serialize_field("id", id)?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for FilterConsumer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Selector {
            Consumer { name: String },
            Group { name: String },
            GroupId { id: u64 },
        }
        Ok(match Selector::deserialize(deserializer)? {
            Selector::Consumer { name } => Self::Consumer(name),
            Selector::Group { name } => Self::Group(name),
            Selector::GroupId { id } => Self::GroupId(id),
        })
    }
}

/// Which filter a read executes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterRef {
    /// The full filter in the request. Needs no catalog. A consumer group
    /// still needs a binding with the same digest.
    Inline(ConsumerFilter),
    /// A saved revision from the catalog.
    Revision { filter_id: u32, revision: u32 },
    /// Whatever revision the consumer group is bound to. Groups only. An
    /// unbound group is refused.
    Bound,
    /// Whatever policy the consumer group has. Groups only. A bound group is
    /// filtered by its revision, an unbound group receives every record. Needs
    /// a server that advertises `GROUP_POLICY_READS`.
    Group,
}

/// Where a filtered read starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilteredStart {
    /// After the consumer's stored offset.
    Next,
    First,
    Last,
    Offset(u64),
    /// Records at or after this broker timestamp, in microseconds.
    Timestamp(u64),
    /// Continue exactly where the previous page stopped.
    Continue(Continuation),
}

/// Which replica serves the read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ReadMode {
    /// The partition primary, through an attached consumer session. The owner
    /// proves the page came from the generation the reply names. Required for
    /// acknowledgments.
    #[default]
    Primary,
    /// Whatever replica the connection reaches. It may lag without bound, so it
    /// serves diagnostics and previews, never acknowledgments.
    Local,
}

impl ReadMode {
    pub const fn is_primary(&self) -> bool {
        matches!(self, Self::Primary)
    }
}

/// How the server selected the records of a page. `Unfiltered` is the explicit
/// policy of a group without a binding, never a fallback: it carries no digest
/// and the page holds every record the owner read returned.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ExecutionMode {
    #[default]
    Filtered,
    Unfiltered,
}

impl ExecutionMode {
    pub const fn is_filtered(&self) -> bool {
        matches!(self, Self::Filtered)
    }
}

/// A filtered policy names its digest and an unfiltered one has none.
fn validate_policy_identity(
    mode: ExecutionMode,
    digest: Option<&Digest32>,
) -> Result<(), InvalidError> {
    match (mode, digest) {
        (ExecutionMode::Filtered, Some(digest)) => digest.validate(),
        (ExecutionMode::Filtered, None) => {
            Err(InvalidError::new("a filtered policy names its digest"))
        }
        (ExecutionMode::Unfiltered, None) => Ok(()),
        (ExecutionMode::Unfiltered, Some(_)) => {
            Err(InvalidError::new("an unfiltered policy carries no digest"))
        }
    }
}

/// The exact source history a page was read from. A purge or a recreated
/// stream, topic, or partition changes it, so a continuation or acknowledgment
/// from an older history is rejected rather than applied to new records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGeneration {
    pub stream_id: u32,
    pub stream_created_at_micros: u64,
    pub topic_id: u32,
    pub topic_created_at_micros: u64,
    pub partition_id: u32,
    pub partition_created_revision: u64,
    pub purge_generation: u64,
}

/// The resume point a page hands to the next request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Continuation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u64>,
    pub next_scan_offset: u64,
    pub generation: SourceGeneration,
    /// The digest of the filter the page ran, absent for an unfiltered page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<Digest32>,
    /// The route the page was read on. A continuation from a local page never
    /// resumes a primary read, because a local replica can lag a purge.
    #[serde(default, skip_serializing_if = "ReadMode::is_primary")]
    pub read_mode: ReadMode,
    #[serde(default)]
    pub mode: ExecutionMode,
    /// The group's policy generation the page ran under, zero for an
    /// independent consumer.
    #[serde(default)]
    pub policy_generation: u64,
}

/// Read one bounded page of matching records from one partition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilteredPollRequest {
    pub v: u32,
    pub source: FilterSource,
    pub partition_id: u32,
    pub consumer: FilterConsumer,
    pub filter: FilterRef,
    pub start: FilteredStart,
    /// Most matching records to return.
    pub count: u32,
    /// Most record bytes to return. The server may lower it.
    pub max_reply_bytes: u32,
    #[serde(default)]
    pub read_mode: ReadMode,
    /// Most source records to examine. The server lowers it to its own
    /// budget. Absent means the server budget alone. A normal consumer sets
    /// it to `count`, so a batch of 100 examines at most 100 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_examined: Option<u32>,
    /// Serve only once the catalog has been examined through this position,
    /// so a reader sees the configuration its application just made.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_catalog_position: Option<CatalogPosition>,
}

/// Why a page stopped scanning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum StopReason {
    /// The requested number of matches was reached.
    Filled,
    /// A record, byte, or round budget was spent. More records may follow.
    Budget,
    /// No more records are visible on the serving replica right now.
    EndOfVisible,
    /// A record failed to decode under the `stop` fault policy.
    Fault,
    /// One matching record alone exceeds the reply cap.
    OversizedRecord,
}

/// Why a record could not be evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FaultReason {
    /// No valid uint32 agdx.sid header identifies the writer schema.
    MissingSchema,
    /// The record schema is not permitted by this filter.
    SchemaNotAllowed,
    /// The registered schema family differs from the payload codec.
    SchemaMismatch,
    /// The payload is not valid for the filter's codec.
    Malformed,
    /// The payload exceeds the decoder's size limit.
    TooLarge,
    /// The payload nests deeper than the decoder's depth limit.
    TooDeep,
    /// The record's agdx.ct header names another codec than the filter's.
    ForeignCodec,
    /// A predicate's value exists but has a type the predicate cannot compare.
    TypeMismatch,
}

impl FaultReason {
    /// Whether the record is in another format than the filter reads, which
    /// follows the filter's foreign policy instead of its fault policy.
    pub const fn is_foreign(self) -> bool {
        matches!(
            self,
            FaultReason::ForeignCodec
                | FaultReason::MissingSchema
                | FaultReason::SchemaNotAllowed
                | FaultReason::SchemaMismatch
        )
    }
}

/// One record the filter could not evaluate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordFault {
    pub offset: u64,
    pub reason: FaultReason,
}

/// Decoder limits applied by the server to unevaluated records in a page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterDecodeLimits {
    pub max_payload_bytes: u32,
    pub max_depth: u32,
}

/// The filter a page actually executed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u64>,
    /// The digest of the filter that ran, absent for an unfiltered page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<Digest32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u32>,
    #[serde(default)]
    pub mode: ExecutionMode,
    /// The group's policy generation, zero for an independent consumer.
    #[serde(default)]
    pub policy_generation: u64,
}

impl AppliedPolicy {
    /// The policy an independent consumer or a bound group ran: one filter.
    #[must_use]
    pub fn filtered(group_id: Option<u64>, digest: Digest32) -> Self {
        Self {
            group_id,
            digest: Some(digest),
            filter_id: None,
            revision: None,
            mode: ExecutionMode::Filtered,
            policy_generation: 0,
        }
    }

    /// The policy of a group without a binding: every record.
    #[must_use]
    pub const fn unfiltered(group_id: u64, policy_generation: u64) -> Self {
        Self {
            group_id: Some(group_id),
            digest: None,
            filter_id: None,
            revision: None,
            mode: ExecutionMode::Unfiltered,
            policy_generation,
        }
    }
}

impl Validate for AppliedPolicy {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_policy_identity(self.mode, self.digest.as_ref())?;
        if self.mode == ExecutionMode::Unfiltered {
            if self.group_id.is_none() {
                return Err(InvalidError::new("only a consumer group runs unfiltered"));
            }
            if self.filter_id.is_some() || self.revision.is_some() {
                return Err(InvalidError::new(
                    "an unfiltered policy has no filter reference",
                ));
            }
        }
        Ok(())
    }
}

/// One page of a filtered read.
///
/// `records` is a standard polled-messages body: the matching records keep
/// their original offsets, ids, timestamps, headers, and payload bytes.
/// `next_scan_offset` is the first record the server has not examined, `None`
/// when nothing was examined and the start selector must be sent again.
/// `safe_ack_offset` is the last examined offset. The reader may acknowledge it
/// only after every returned record at or below it is fully handled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilteredPage {
    pub v: u32,
    pub partition_id: u32,
    pub policy: AppliedPolicy,
    pub generation: SourceGeneration,
    pub read_mode: ReadMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_scan_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_ack_offset: Option<u64>,
    /// The serving replica's partition head when the page was read. Reported,
    /// never used as scan progress.
    pub frontier: u64,
    pub examined: u32,
    pub matched: u32,
    pub stop: StopReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<RecordFault>,
    /// Offsets of returned records the filter could not evaluate (the `pass`
    /// fault policy).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unevaluated: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_limits: Option<Box<FilterDecodeLimits>>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub records: Vec<u8>,
}

impl FilteredPage {
    /// The start of the next request: continue after this page, or repeat the
    /// original selector when nothing was examined.
    pub fn next_start(&self, original: &FilteredStart) -> FilteredStart {
        match self.next_scan_offset {
            Some(next_scan_offset) => FilteredStart::Continue(Continuation {
                group_id: self.policy.group_id,
                next_scan_offset,
                generation: self.generation,
                digest: self.policy.digest.clone(),
                read_mode: self.read_mode,
                mode: self.policy.mode,
                policy_generation: self.policy.policy_generation,
            }),
            None => original.clone(),
        }
    }
}

/// Acknowledge a filtered page through its safe offset. The server stores the
/// offset only while the source still has the generation the page was read
/// from, and only while the consumer still owns the partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilteredAck {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u64>,
    pub v: u32,
    pub source: FilterSource,
    pub partition_id: u32,
    pub consumer: FilterConsumer,
    pub generation: SourceGeneration,
    /// The digest of the filter the page ran, absent for an unfiltered page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<Digest32>,
    pub offset: u64,
    #[serde(default)]
    pub mode: ExecutionMode,
    /// The group's policy generation the page ran under. The server refuses
    /// the acknowledgment when the group's generation moved on.
    #[serde(default)]
    pub policy_generation: u64,
}

/// A stored acknowledgment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckReceipt {
    pub partition_id: u32,
    pub offset: u64,
    pub generation: SourceGeneration,
}

/// Preview a filter over stored records of one partition. Observational: it
/// joins no group, stores no offset, and changes no consumer state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterPreviewRequest {
    pub v: u32,
    pub source: FilterSource,
    pub partition_id: u32,
    /// `inline` or `revision`. `bound` is rejected.
    pub filter: FilterRef,
    pub from_offset: u64,
    pub max_examined: u32,
    pub max_records: u32,
    /// Include a per-predicate explanation for every returned record.
    #[serde(default)]
    pub explain: bool,
}

/// How the filter judged one previewed record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Verdict {
    Selected,
    Rejected,
    Fault,
}

/// One previewed record: identity, verdict, and a bounded text view.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreviewRecord {
    pub offset: u64,
    pub timestamp_micros: u64,
    pub verdict: Verdict,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<FaultReason>,
    /// The payload as lossy UTF-8, cut at the preview cap.
    pub payload_text: String,
    #[serde(default)]
    pub payload_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explanation: Option<FilterExplanation>,
}

/// A preview of one partition range.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterPreview {
    pub v: u32,
    pub partition_id: u32,
    pub policy: AppliedPolicy,
    pub read_mode: ReadMode,
    pub examined: u32,
    pub matched: u32,
    pub faults: u32,
    pub stop: StopReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
    pub frontier: u64,
    /// Selected records first, capped at `max_records`. Rejected records are
    /// listed only when `explain` was requested.
    pub records: Vec<PreviewRecord>,
}

/// A typed user header value, as the evaluator reads it. Iggy integer kinds up
/// to 64 bits map to `int` or `uint`. 128-bit integers arrive as `raw` bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HeaderScalar {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    String(String),
    Raw(#[serde(with = "crate::encoding::bin_bytes")] Vec<u8>),
}

/// One user header of a record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterHeader {
    pub key: String,
    pub value: HeaderScalar,
}

/// Evaluate a filter against one supplied record. Nothing is read from a
/// stream and nothing is stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterTestRequest {
    pub v: u32,
    /// `inline` or `revision`. `bound` is rejected.
    pub filter: FilterRef,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<FilterHeader>,
}

/// Three-valued comparison truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Truth {
    Match,
    NoMatch,
    Unknown,
}

/// One node of an explanation tree: what it tested and how it resolved.
/// `truth` is absent when the payload could not be decoded for this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainNode {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truth: Option<Truth>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ExplainNode>,
}

/// Why a record was or was not selected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterExplanation {
    pub verdict: Verdict,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<FaultReason>,
    pub root: ExplainNode,
}

/// The result of a sample test.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterTestResult {
    pub v: u32,
    pub policy: AppliedPolicy,
    pub explanation: FilterExplanation,
}

/// A filter that validated and compiled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterValidation {
    pub v: u32,
    pub digest: Digest32,
    /// Whether evaluating the filter can decode the payload.
    pub reads_payload: bool,
    /// Whether evaluating the filter can read user headers.
    pub reads_headers: bool,
}

/// The typed cause of a consumer-filter failure. Each maps to one shared
/// [`ResultCode`], so a generic client can still classify it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum FilterErrorReason {
    /// The filter or request is malformed or out of range.
    InvalidRequest,
    /// The server or its backend does not serve this operation.
    Unsupported,
    /// The wire version is not accepted.
    VersionSkew,
    /// The source, filter, revision, binding, or operation does not exist.
    NotFound,
    /// A policy or mutation precondition does not hold (a different digest is
    /// bound, a name is taken, an expected revision moved).
    Conflict,
    /// The source history changed (purge or recreation) since the page or
    /// continuation was issued.
    SourceChanged,
    /// The consumer no longer owns the partition, or its session ended.
    MembershipStale,
    /// This node is not the partition primary. Resolve the route again.
    NotPrimary,
    /// The catalog cannot confirm the policy right now.
    CatalogUnavailable,
    /// The saved revision is paused. Pending records may still be acknowledged.
    RevisionDisabled,
    /// The request is too large.
    TooLarge,
    Unauthenticated,
    Forbidden,
    /// A transient failure. The same request may succeed later.
    Unavailable,
    /// A configured catalog resource limit is reached.
    CapacityExhausted,
    /// An unexpected failure the caller cannot retry away.
    Backend,
    /// A reason this build does not know. The error's `code` still classifies
    /// it.
    #[serde(other)]
    Unknown,
}

impl FilterErrorReason {
    pub const fn code(self) -> ResultCode {
        match self {
            Self::InvalidRequest => ResultCode::InvalidArgument,
            Self::Unsupported => ResultCode::Unsupported,
            Self::VersionSkew => ResultCode::VersionSkew,
            Self::NotFound => ResultCode::NotFound,
            Self::Conflict => ResultCode::Conflict,
            Self::SourceChanged => ResultCode::StaleGeneration,
            Self::MembershipStale | Self::NotPrimary => ResultCode::Stale,
            Self::CatalogUnavailable | Self::Unavailable | Self::RevisionDisabled => {
                ResultCode::Unavailable
            }
            Self::TooLarge => ResultCode::TooLarge,
            Self::Unauthenticated => ResultCode::Unauthenticated,
            Self::Forbidden => ResultCode::Forbidden,
            Self::CapacityExhausted => ResultCode::ResourceLimit,
            Self::Backend | Self::Unknown => ResultCode::Backend,
        }
    }
}

/// A consumer-filter failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{reason}: {message}")]
pub struct FilterError {
    pub code: ResultCode,
    pub reason: FilterErrorReason,
    pub message: String,
}

impl FilterError {
    pub fn new(reason: FilterErrorReason, message: impl Into<String>) -> Self {
        Self {
            code: reason.code(),
            reason,
            message: message.into(),
        }
    }

    pub fn invalid(error: InvalidError) -> Self {
        Self::new(FilterErrorReason::InvalidRequest, error.0)
    }

    /// Refuse a request of another filter op version as `version_skew`, before
    /// its shape is validated, so a client of another build can tell a version
    /// gap from a malformed request.
    ///
    /// # Errors
    ///
    /// `version_skew` when `v` is not [`FILTER_OP_VERSION`].
    pub fn check_version(v: u32) -> Result<(), Self> {
        validate_version(v).map_err(|error| Self::new(FilterErrorReason::VersionSkew, error.0))
    }
}

/// A successful reply of a server-native consumer-filter command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOutcome {
    Page(FilteredPage),
    Acknowledged(AckReceipt),
    Preview(FilterPreview),
    Tested(FilterTestResult),
    Validated(FilterValidation),
}

/// The reply to every server-native consumer-filter command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
// Keep successful pages inline without another allocation on every read.
#[allow(clippy::large_enum_variant)]
pub enum FilterReply {
    Ok(FilterOutcome),
    Err(FilterError),
}

fn validate_version(v: u32) -> Result<(), InvalidError> {
    if v == FILTER_OP_VERSION {
        Ok(())
    } else {
        Err(InvalidError::new(format!(
            "filter request version {v} is not supported, expected {FILTER_OP_VERSION}"
        )))
    }
}

pub(crate) fn validate_name(label: &str, name: &str) -> Result<(), InvalidError> {
    if name.is_empty() || name.len() > MAX_FILTER_SOURCE_NAME_BYTES {
        return Err(InvalidError::new(format!(
            "{label} must be 1..={MAX_FILTER_SOURCE_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

impl Validate for FilterSource {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_name("stream", &self.stream)?;
        validate_name("topic", &self.topic)
    }
}

impl Validate for FilterConsumer {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::Consumer(name) => validate_name("consumer", name),
            Self::Group(name) => validate_name("consumer group", name),
            Self::GroupId(id) => u32::try_from(*id).map(|_| ()).map_err(|_| {
                InvalidError::new("consumer group id exceeds the native offset-key range")
            }),
        }
    }
}

impl Validate for FilterRef {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::Inline(filter) => filter.validate(),
            Self::Revision { .. } | Self::Bound | Self::Group => Ok(()),
        }
    }
}

impl Validate for FilteredPollRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        self.source.validate()?;
        self.consumer.validate()?;
        self.filter.validate()?;
        if matches!(self.filter, FilterRef::Bound | FilterRef::Group) && !self.consumer.is_group() {
            return Err(InvalidError::new(
                "a `bound` or `group` filter needs a consumer group",
            ));
        }
        if self.count == 0 || self.count > MAX_FILTERED_PAGE_RECORDS {
            return Err(InvalidError::new(format!(
                "count must be 1..={MAX_FILTERED_PAGE_RECORDS}"
            )));
        }
        if let Some(max_examined) = self.max_examined
            && (max_examined == 0 || max_examined > MAX_FILTERED_PAGE_EXAMINED)
        {
            return Err(InvalidError::new(format!(
                "max_examined must be 1..={MAX_FILTERED_PAGE_EXAMINED}"
            )));
        }
        if self.max_reply_bytes == 0 || self.max_reply_bytes > MAX_FILTERED_PAGE_BYTES {
            return Err(InvalidError::new(format!(
                "max_reply_bytes must be 1..={MAX_FILTERED_PAGE_BYTES}"
            )));
        }
        if let FilteredStart::Continue(continuation) = &self.start {
            validate_policy_identity(continuation.mode, continuation.digest.as_ref())?;
            if continuation.mode == ExecutionMode::Unfiltered && !self.consumer.is_group() {
                return Err(InvalidError::new("only a consumer group runs unfiltered"));
            }
            if self.consumer.is_group() != continuation.group_id.is_some() {
                return Err(InvalidError::new(
                    "a group continuation must retain its group identity",
                ));
            }
            if continuation.generation.partition_id != self.partition_id {
                return Err(InvalidError::new(
                    "a continuation belongs to another partition",
                ));
            }
        }
        Ok(())
    }
}

impl Validate for FilteredAck {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        self.source.validate()?;
        self.consumer.validate()?;
        validate_policy_identity(self.mode, self.digest.as_ref())?;
        if self.mode == ExecutionMode::Unfiltered && !self.consumer.is_group() {
            return Err(InvalidError::new("only a consumer group runs unfiltered"));
        }
        if self.consumer.is_group() != self.group_id.is_some() {
            return Err(InvalidError::new(
                "a group acknowledgment must retain its group identity",
            ));
        }
        if self.generation.partition_id != self.partition_id {
            return Err(InvalidError::new(
                "an acknowledgment names another partition's generation",
            ));
        }
        Ok(())
    }
}

impl Validate for FilterPreviewRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        self.source.validate()?;
        self.filter.validate()?;
        if matches!(self.filter, FilterRef::Bound | FilterRef::Group) {
            return Err(InvalidError::new(
                "a preview executes an inline filter or a saved revision",
            ));
        }
        if self.max_examined == 0 || self.max_examined > MAX_FILTER_PREVIEW_EXAMINED {
            return Err(InvalidError::new(format!(
                "max_examined must be 1..={MAX_FILTER_PREVIEW_EXAMINED}"
            )));
        }
        if self.max_records == 0 || self.max_records > MAX_FILTER_PREVIEW_RECORDS {
            return Err(InvalidError::new(format!(
                "max_records must be 1..={MAX_FILTER_PREVIEW_RECORDS}"
            )));
        }
        Ok(())
    }
}

impl Validate for FilterTestRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        self.filter.validate()?;
        if matches!(self.filter, FilterRef::Bound | FilterRef::Group) {
            return Err(InvalidError::new(
                "a sample test executes an inline filter or a saved revision",
            ));
        }
        if self.payload.len() > MAX_FILTER_SAMPLE_BYTES {
            return Err(InvalidError::new(format!(
                "a sample payload exceeds {MAX_FILTER_SAMPLE_BYTES}B"
            )));
        }
        if self.headers.len() > MAX_FILTER_SAMPLE_HEADERS {
            return Err(InvalidError::new(format!(
                "a sample carries more than {MAX_FILTER_SAMPLE_HEADERS} headers"
            )));
        }
        for header in &self.headers {
            validate_name("sample header key", &header.key)?;
            let bytes = match &header.value {
                HeaderScalar::String(value) => value.len(),
                HeaderScalar::Raw(value) => value.len(),
                _ => 0,
            };
            if bytes > crate::limits::MAX_FILTER_STRING_BYTES {
                return Err(InvalidError::new(
                    "a sample header value exceeds its byte limit",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::expr::FilterExpr;
    use crate::query::CmpOp;

    fn generation() -> SourceGeneration {
        SourceGeneration {
            stream_id: 1,
            stream_created_at_micros: 10,
            topic_id: 2,
            topic_created_at_micros: 20,
            partition_id: 0,
            partition_created_revision: 30,
            purge_generation: 0,
        }
    }

    fn request() -> FilteredPollRequest {
        FilteredPollRequest {
            v: FILTER_OP_VERSION,
            source: FilterSource {
                stream: "orbit".to_owned(),
                topic: "fleet_changes".to_owned(),
            },
            partition_id: 0,
            consumer: FilterConsumer::Group("anomaly-desk".to_owned()),
            filter: FilterRef::Bound,
            start: FilteredStart::Next,
            count: 100,
            max_reply_bytes: 1024 * 1024,
            read_mode: ReadMode::Primary,
            max_examined: None,
            min_catalog_position: None,
        }
    }

    #[test]
    fn given_a_group_read_when_validated_then_should_pass() {
        request().validate().expect("valid");
    }

    #[test]
    fn given_out_of_range_reads_when_validated_then_should_be_rejected() {
        let mut bound_consumer = request();
        bound_consumer.consumer = FilterConsumer::Consumer("one".to_owned());
        let mut zero = request();
        zero.count = 0;
        let mut huge = request();
        huge.max_reply_bytes = MAX_FILTERED_PAGE_BYTES + 1;
        let mut other_partition = request();
        other_partition.start = FilteredStart::Continue(Continuation {
            group_id: None,
            next_scan_offset: 7,
            generation: SourceGeneration {
                partition_id: 5,
                ..generation()
            },
            digest: Some(Digest32::new([1; 32])),
            read_mode: ReadMode::Primary,
            mode: ExecutionMode::Filtered,
            policy_generation: 0,
        });
        let mut group_consumer = request();
        group_consumer.filter = FilterRef::Group;
        group_consumer.consumer = FilterConsumer::Consumer("one".to_owned());
        let mut over_examined = request();
        over_examined.max_examined = Some(MAX_FILTERED_PAGE_EXAMINED + 1);
        let mut zero_examined = request();
        zero_examined.max_examined = Some(0);
        let mut unfiltered_with_digest = request();
        unfiltered_with_digest.start = FilteredStart::Continue(Continuation {
            group_id: Some(3),
            next_scan_offset: 7,
            generation: generation(),
            digest: Some(Digest32::new([1; 32])),
            read_mode: ReadMode::Primary,
            mode: ExecutionMode::Unfiltered,
            policy_generation: 1,
        });
        for request in [
            bound_consumer,
            zero,
            huge,
            other_partition,
            group_consumer,
            over_examined,
            zero_examined,
            unfiltered_with_digest,
        ] {
            assert!(request.validate().is_err(), "{request:?} must be rejected");
        }
    }

    #[test]
    fn given_an_automatic_group_read_with_an_examined_bound_when_validated_then_should_pass() {
        let mut automatic = request();
        automatic.filter = FilterRef::Group;
        automatic.max_examined = Some(100);
        automatic.min_catalog_position = Some(CatalogPosition {
            partition_id: 0,
            offset: 41,
            operation_id: Some(11),
        });
        automatic.validate().expect("valid");
    }

    #[test]
    fn given_an_unfiltered_policy_when_validated_then_should_need_a_group_and_no_digest() {
        AppliedPolicy::unfiltered(3, 1).validate().expect("valid");
        assert!(
            AppliedPolicy::filtered(Some(3), Digest32::new([1; 32]))
                .validate()
                .is_ok()
        );
        let mut no_group = AppliedPolicy::unfiltered(3, 1);
        no_group.group_id = None;
        assert!(no_group.validate().is_err());
        let mut with_digest = AppliedPolicy::unfiltered(3, 1);
        with_digest.digest = Some(Digest32::new([1; 32]));
        assert!(with_digest.validate().is_err());
        let mut with_reference = AppliedPolicy::unfiltered(3, 1);
        with_reference.filter_id = Some(1);
        assert!(with_reference.validate().is_err());
        with_reference.filter_id = None;
        with_reference.revision = Some(1);
        assert!(with_reference.validate().is_err());
        let mut without_digest = AppliedPolicy::filtered(None, Digest32::new([1; 32]));
        without_digest.digest = None;
        assert!(without_digest.validate().is_err());
    }

    #[test]
    fn given_a_page_when_asked_for_the_next_start_then_should_continue_or_repeat() {
        let page = FilteredPage {
            v: FILTER_OP_VERSION,
            partition_id: 0,
            policy: AppliedPolicy {
                group_id: None,
                digest: Some(Digest32::new([9; 32])),
                filter_id: Some(3),
                revision: Some(1),
                mode: ExecutionMode::Filtered,
                policy_generation: 0,
            },
            generation: generation(),
            read_mode: ReadMode::Primary,
            next_scan_offset: Some(1397),
            safe_ack_offset: Some(1396),
            frontier: 2000,
            examined: 397,
            matched: 3,
            stop: StopReason::Filled,
            fault: None,
            unevaluated: Vec::new(),
            evaluation_limits: None,
            records: Vec::new(),
        };
        assert_eq!(
            page.next_start(&FilteredStart::Next),
            FilteredStart::Continue(Continuation {
                group_id: None,
                next_scan_offset: 1397,
                generation: generation(),
                digest: Some(Digest32::new([9; 32])),
                read_mode: ReadMode::Primary,
                mode: ExecutionMode::Filtered,
                policy_generation: 0,
            })
        );
        let empty = FilteredPage {
            next_scan_offset: None,
            safe_ack_offset: None,
            ..page
        };
        assert_eq!(empty.next_start(&FilteredStart::Last), FilteredStart::Last);
    }

    #[test]
    fn given_error_reasons_when_classified_then_should_map_to_shared_codes() {
        assert_eq!(
            FilterError::new(FilterErrorReason::SourceChanged, "purged").code,
            ResultCode::StaleGeneration
        );
        assert_eq!(FilterErrorReason::NotPrimary.code(), ResultCode::Stale);
        assert_eq!(FilterErrorReason::Conflict.code(), ResultCode::Conflict);
        assert_eq!(
            FilterErrorReason::CatalogUnavailable.code(),
            ResultCode::Unavailable
        );
    }

    #[test]
    fn given_samples_and_previews_when_validated_then_should_reject_group_selectors() {
        let inline = FilterRef::Inline(ConsumerFilter::json(FilterExpr::pred(
            "kind",
            CmpOp::Eq,
            "satellites",
        )));
        let test = FilterTestRequest {
            v: FILTER_OP_VERSION,
            filter: inline.clone(),
            payload: br#"{"kind":"satellites"}"#.to_vec(),
            headers: Vec::new(),
        };
        test.validate().expect("valid sample");
        let mut preview = FilterPreviewRequest {
            v: FILTER_OP_VERSION,
            source: request().source,
            partition_id: 0,
            filter: inline,
            from_offset: 0,
            max_examined: 100,
            max_records: 10,
            explain: false,
        };
        preview.validate().expect("valid preview");
        for selector in [FilterRef::Bound, FilterRef::Group] {
            let sample = FilterTestRequest {
                filter: selector.clone(),
                ..test.clone()
            };
            assert!(sample.validate().is_err());
            preview.filter = selector;
            assert!(preview.validate().is_err());
        }
    }

    #[test]
    fn given_oversized_sample_headers_when_validated_then_should_reject_before_evaluation() {
        let mut request = FilterTestRequest {
            v: FILTER_OP_VERSION,
            filter: FilterRef::Inline(ConsumerFilter::headers_only(FilterExpr::header(
                "priority",
                CmpOp::Eq,
                "critical",
            ))),
            payload: Vec::new(),
            headers: vec![FilterHeader {
                key: "priority".to_owned(),
                value: HeaderScalar::Raw(vec![0; crate::limits::MAX_FILTER_STRING_BYTES + 1]),
            }],
        };
        assert!(request.validate().is_err());
        request.headers[0].value = HeaderScalar::String("critical".to_owned());
        request.validate().expect("bounded header");
        request.headers[0].key.clear();
        assert!(request.validate().is_err());
    }
}
