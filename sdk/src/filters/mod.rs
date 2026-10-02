// Server-side consumer filters. Wire types, codes, and caps live in laser-wire
// and are re-exported here unconditionally. A consumer group owns its filter
// policy: `ConsumerGroup::filter()` administers it and `ConsumerGroup::reader()`
// reads the matches. The group transport, routing, and progress tracking are
// part of `streaming`. The local guard's evaluator stays behind `filters`.

pub use laser_wire::codes::{
    AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE, AGDX_FILTER_PREVIEW_CODE,
    AGDX_FILTER_TEST_CODE, AGDX_FILTER_VALIDATE_CODE, AGDX_FILTERED_ACK_CODE,
    AGDX_FILTERED_POLL_CODE, AGDX_GET_FILTER_BINDING_CODE, AGDX_GET_FILTER_CODE,
    AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE, AGDX_LIST_FILTERS_CODE,
    FILTER_OP_VERSION,
};
pub use laser_wire::filter::{
    AckReceipt, AppliedPolicy, CatalogPosition, Coerce, CoercedPredicate, ConsumerFilter,
    Continuation, ExactDecimal, ExecutionMode, ExplainNode, FILTER_EVALUATOR_VERSION, FaultPolicy,
    FaultReason, FieldPath, FilterBinding, FilterBindingPage, FilterCodec, FilterConsumer,
    FilterDetail, FilterError, FilterErrorReason, FilterExplanation, FilterExpr,
    FilterGroupIdentity, FilterGroupRef, FilterHeader, FilterMutation, FilterMutationOutcome,
    FilterMutationResult, FilterMutationStatus, FilterPage, FilterPreview, FilterPreviewRequest,
    FilterRef, FilterRevisionInfo, FilterRevisionPage, FilterRevisionRef, FilterSource,
    FilterState, FilterSummary, FilterTestResult, FilterValidation, FilteredPage, FilteredStart,
    GroupFilterSpec, GroupPolicyUnbound, HeaderPredicate, HeaderScalar, PathSegment, PreviewRecord,
    ReadMode, RecordFault, RecordPolicy, SourceGeneration, StopReason, TextMatch, TextPredicate,
    TimestampFormat, Truth, Verdict,
};
pub use laser_wire::limits::{
    MAX_FILTER_BYTES, MAX_FILTER_CATALOG_PAGE, MAX_FILTER_NAME_BYTES, MAX_FILTER_PREVIEW_EXAMINED,
    MAX_FILTER_PREVIEW_RECORDS, MAX_FILTERED_PAGE_BYTES, MAX_FILTERED_PAGE_EXAMINED,
    MAX_FILTERED_PAGE_RECORDS,
};

#[cfg(feature = "streaming")]
pub(crate) mod client;
#[cfg(feature = "streaming")]
mod group;
#[cfg(feature = "filters")]
mod guard;
#[cfg(feature = "streaming")]
mod progress;
#[cfg(feature = "streaming")]
pub(crate) mod reader;
#[cfg(feature = "streaming")]
mod route;
#[cfg(feature = "streaming")]
pub use client::{DEFAULT_OUTCOME_WAIT, FilterPreviewBuilder};
#[cfg(feature = "filters")]
pub use laser_wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
#[cfg(feature = "streaming")]
pub use reader::{FilteredReader, FilteredReaderBuilder, MatchedPage, MatchedRecord};
