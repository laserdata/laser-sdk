// Server-side consumer filters. Wire types, codes, and caps live in laser-wire
// and are re-exported here unconditionally. The `Filters` handle, the filtered
// reader, and its progress tracking stay in this crate behind the `filters`
// feature.

pub use laser_wire::codes::{
    AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE, AGDX_FILTER_PREVIEW_CODE,
    AGDX_FILTER_TEST_CODE, AGDX_FILTER_VALIDATE_CODE, AGDX_FILTERED_ACK_CODE,
    AGDX_FILTERED_POLL_CODE, AGDX_GET_FILTER_BINDING_CODE, AGDX_GET_FILTER_CODE,
    AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE, AGDX_LIST_FILTERS_CODE,
    FILTER_OP_VERSION,
};
pub use laser_wire::filter::{
    AckReceipt, AppliedPolicy, Coerce, CoercedPredicate, ConsumerFilter, Continuation,
    ExactDecimal, ExplainNode, FILTER_EVALUATOR_VERSION, FaultPolicy, FaultReason, FieldPath,
    FilterBinding, FilterBindingPage, FilterCodec, FilterConsumer, FilterDetail, FilterError,
    FilterErrorReason, FilterExplanation, FilterExpr, FilterGroupIdentity, FilterGroupRef,
    FilterHeader, FilterMutation, FilterMutationOutcome, FilterMutationResult,
    FilterMutationStatus, FilterPage, FilterPreview, FilterPreviewRequest, FilterRef,
    FilterRevisionInfo, FilterRevisionPage, FilterRevisionRef, FilterSource, FilterState,
    FilterSummary, FilterTestResult, FilterValidation, FilteredPage, FilteredStart,
    HeaderPredicate, HeaderScalar, PathSegment, PreviewRecord, ReadMode, RecordFault, RecordPolicy,
    SourceGeneration, StopReason, TextMatch, TextPredicate, TimestampFormat, Truth, Verdict,
};
pub use laser_wire::limits::{
    MAX_FILTER_BYTES, MAX_FILTER_CATALOG_PAGE, MAX_FILTER_NAME_BYTES, MAX_FILTER_PREVIEW_EXAMINED,
    MAX_FILTER_PREVIEW_RECORDS, MAX_FILTERED_PAGE_BYTES, MAX_FILTERED_PAGE_RECORDS,
};

#[cfg(feature = "filters")]
mod client;
#[cfg(feature = "filters")]
mod group;
#[cfg(feature = "filters")]
mod guard;
#[cfg(feature = "filters")]
mod progress;
#[cfg(feature = "filters")]
mod reader;
#[cfg(feature = "filters")]
mod route;
#[cfg(feature = "filters")]
pub use client::{DEFAULT_OUTCOME_WAIT, FilterList, FilterPreviewBuilder, Filters};
#[cfg(feature = "filters")]
pub use laser_wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
#[cfg(feature = "filters")]
pub use reader::{FilteredReader, FilteredReaderBuilder, MatchedPage, MatchedRecord};
