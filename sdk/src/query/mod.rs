// The query surface. The wire contract (types, codes, headers, topics, caps)
// lives in the laser-wire crate and is re-exported here unconditionally so
// every historical `laser_sdk::query::*` import keeps resolving. The runtime
// half (publish/query/browse/control builders, `Record` lowering onto iggy
// headers) stays in this crate behind the `query` feature.

pub use laser_wire::browse::{
    BrowseOutcome, BrowseReply, DecodeRecord, GetProjection, GetSchema, ListProjections,
    ListSchemas, ProjectionInfo, RegisterSchema, SchemaInfo,
};
pub use laser_wire::codes::{
    AGDX_COMMAND_BASE, AGDX_DECODE_RECORD_CODE, AGDX_FORK_BASE, AGDX_FORK_CREATE_CODE,
    AGDX_FORK_DELETE_CODE, AGDX_FORK_LIST_CODE, AGDX_FORK_PROMOTE_CODE, AGDX_FORK_PUT_CODE,
    AGDX_GET_PROJECTION_CODE, AGDX_GET_SCHEMA_CODE, AGDX_HELLO_CODE, AGDX_LIST_PROJECTIONS_CODE,
    AGDX_LIST_SCHEMAS_CODE, AGDX_QUERY_CODE, AGDX_REGISTER_SCHEMA_CODE, CONTROL_OP_VERSION,
    FORK_OP_VERSION, QUERY_OP_VERSION,
};
pub use laser_wire::content::ContentType;
pub use laser_wire::control::{
    ControlCommand, ControlEnvelope, Delivery, FieldType, IndexField, IndexSchema,
    IndexSchemaBuilder, Projection, ProjectionBinding, ProjectionBindingBuilder, ProjectionBuilder,
    ProjectionId, RetentionPolicy, SchemaDef, SchemaSource, SourceSelector, Target, TargetRole,
};
pub use laser_wire::headers::{
    CONTENT_TYPE, FIELD_MESSAGE_TYPE, FIELD_TS, IDX_PREFIX, INLINE_PAYLOAD, PROJECTION_REF,
    SCHEMA_ID, VECTOR_FIELD, WINDOW_START,
};
pub use laser_wire::hello::HelloReply;
pub use laser_wire::limits::{
    DEFAULT_STREAM_PAGE_SIZE, MAX_INDEX_ENTRIES_PER_RECORD, MAX_PAGE_SIZE,
};
pub use laser_wire::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Page, Predicate, Query,
    QueryBuilder, QueryEnvelope, QueryError, QueryReply, QueryResult, RawSql, Row, Select, Sort,
    Value, VectorQuery, Window,
};
pub use laser_wire::result::ResultCode;
pub use laser_wire::topics::{CONTROL_TOPIC, DLQ_TOPIC, OPS_STREAM};

#[cfg(feature = "query")]
pub use laser_wire::codecs::{Bson, Cbor, Codec, Decoder, Json, Msgpack};

#[cfg(feature = "query")]
mod client;
#[cfg(feature = "query")]
mod record;

#[cfg(feature = "query")]
pub use client::{
    BatchPublishRequest, Bindings, Projections, ProjectionsRequest, PublishRequest, QueryRequest,
    RegisterSchemaRequest, Schemas,
};
#[cfg(feature = "query")]
pub use record::{Record, RecordBuilder};
