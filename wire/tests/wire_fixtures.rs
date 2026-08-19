// Golden wire-fixture assertions. The corpus lives in this crate (the one
// typed source of truth). This suite re-encodes canonical values and asserts
// byte-for-byte equality, so an encoding change (a serde attribute edit, an
// rmp-serde upgrade, a field reorder) fails here instead of shipping a silent
// wire break. Consumer repos assert against the same bytes through the
// `fixtures` feature instead of copying files.

use laser_wire::agent::ConversationId;
use laser_wire::agent_workflow::{
    AgentError, AgentList, AgentOutcome, AgentReply, AgentRunInfo, AgentRunState, AgentSubmit,
    RunPage,
};
use laser_wire::arrow::{
    ArrowIpcMessageMetadata, ArrowIpcPolicy, ArrowIpcRejectionCode, ArrowTimestampUnit,
};
use laser_wire::authz::{
    Action, AuthzEvent, AuthzEventKind, AuthzHistoryReply, AuthzHistoryReq, AuthzReply,
    AuthzSubject, BindRolesReq, Effect, Feature, Grant, ResourcePattern, Role, WhoamiReply,
};
use laser_wire::batch::{BatchItem, BatchReply, BatchRequest};
use laser_wire::browse::{
    BrowseOutcome, BrowseReply, DecodeRecord, ProjectionInfo, RegisterSchema, SchemaInfo,
};
use laser_wire::change::ChangeRecord;
use laser_wire::checkpoint::{
    AttemptColumnMetrics, AttemptObject, CheckpointMutationResult, CheckpointMutationStamp,
    CheckpointOwnerId, CheckpointReadConsistency, CheckpointReply, CheckpointRequestEnvelope,
    CheckpointRequestId, CompletedAttempt, CredentialGeneration, DestinationBlock,
    DestinationBlockCode, DestinationCheckpointStatus, DestinationEffectiveState,
    IcebergCommitRequirement, PartitionCheckpoint, PartitionLifecycleChange,
    PartitionLifecycleState, PreparedAttempt, PreparedAttemptId, PreparedTableRequirements,
    PublicCheckpointMutation, RepairAction, RepairRecord, ReplicatedCheckpointMutation,
    ReplicatedCheckpointMutationBody, RetentionGap, SourceOffsetRange,
};
use laser_wire::clients::{ClientMetadata, ClientMetadataList, ClientMetadataQuery};
use laser_wire::codes::{
    AGDX_KV_SET_CODE, AGENT_OP_VERSION, AGENT_WORKFLOW_OP_VERSION, AUTHZ_OP_VERSION,
    BATCH_OP_VERSION, CHANGE_OP_VERSION, CHECKPOINT_OP_VERSION, CLIENT_METADATA_OP_VERSION,
    CONTROL_OP_VERSION, FORK_OP_VERSION, GRAPH_OP_VERSION, KV_LEASE_OP_VERSION, KV_OP_VERSION,
    QUERY_OP_VERSION,
};
use laser_wire::content::ContentType;
use laser_wire::control::{
    ControlCommand, ControlEnvelope, FieldType, IndexField, IndexSchema, Projection,
    ProjectionBinding, ProjectionId, ProjectionKind, RetentionPolicy, SchemaDef, SchemaSource,
    SourceSelector,
};
use laser_wire::destination::{
    BackendBinding, BackendResourceId, DestinationDesiredState, DestinationErrorPolicy,
    DestinationId, DestinationOperationId, FileFormat, MaterializationDestination,
    NewPartitionPolicy, PartitionStart, PhysicalTable, ProjectionRef, QueryRoute, QueryRouteId,
    QueryRouteTarget, RecreatedPartitionPolicy, StartPolicy, TableFormat,
};
use laser_wire::fork::{
    ForkCreate, ForkInfo, ForkKind, ForkOutcome, ForkPut, ForkReply, ForkStatus,
};
use laser_wire::forward::{ForwardedCommand, ForwardedQuery};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::graph::{
    EdgeDir, GraphEdge, GraphNeighbors, GraphNode, GraphQuery, GraphReply, GraphResult,
    GraphReturn, GraphStart, GraphUpsert, Hop, Path, SourceRef,
};
use laser_wire::hello::{
    BackendAnnounce, BackendDescriptor, BackendDesiredState, BackendImplementation, BackendLimits,
    BackendMode, BackendObservedState, BackendReadiness, BackendReadinessCode, HelloReply,
    MaterializationCapability, OpVersions, QueryCapabilities, QueryPagingCapability,
    SchemaCapabilities, TimeTravelCapability, feature,
};
use laser_wire::http::{
    AcceptedOperationView, Capabilities, DestinationCapsView, DestinationIssueView,
    DestinationPageView, DestinationView, ErrorBody, KvEntryView, KvPageView, OperationState,
    QueryExecutionView, QueryRoutePageView, SnapshotPageView, TableFilePageView, TableFileView,
    TableMetricsView, TableSchemaView, TableSnapshotView, TableView,
};
use laser_wire::keys::{KEY_ID_BYTES, KeyKind, KeyRecord, VERIFYING_KEY_BYTES};
use laser_wire::kv::{
    CasExpect, KvCas, KvCasFenced, KvCopy, KvEntry, KvError, KvGet, KvLease, KvLeaseRenew, KvMove,
    KvNamespaceInfo, KvNamespaces, KvOutcome, KvPage, KvRelease, KvReply, KvScan, KvSet,
};
use laser_wire::mutation::{
    MANAGED_REQUEST_VERSION, ManagedRequestEnvelope, MutationCommandEnvelope, MutationPosition,
};
use laser_wire::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Page, Query,
    QueryCancelEnvelope, QueryContext, QueryEngine, QueryEnvelope, QueryError, QueryErrorCode,
    QueryExecutionId, QueryExecutionState, QueryExecutionStatus, QueryPageEnvelope,
    QueryPageRequest, QueryReply, QueryResult, QueryStatusEnvelope, QueryStatusReply, QueryTarget,
    RawSql, ResolvedQueryTarget, Row, Select, Sort, SqlDialect, TextQuery, VectorQuery, Window,
};
use laser_wire::result::ResultCode;
use laser_wire::schema::{
    BinaryValue, DecimalValue, Digest32, FieldValue, LogicalField, LogicalSchema, LogicalSchemaId,
    LogicalType, LogicalTypeKind, MapEntry, SchemaFingerprint, TypedValue, UuidValue,
};
use laser_wire::snapshot::FoldSnapshot;
use laser_wire::source::{PhysicalClusterIncarnation, SourceIncarnation, SourceScope};
use laser_wire::topology::WireTopology;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

const REGEN_ENV: &str = "AGDX_WIRE_FIXTURES_REGEN";
const TIMESTAMP_MICROS: u64 = 1_717_171_717_000_000;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DataStackStringDiscriminants {
    arrow_timestamp_units: Vec<ArrowTimestampUnit>,
    arrow_rejections: Vec<ArrowIpcRejectionCode>,
    file_formats: Vec<FileFormat>,
    table_formats: Vec<TableFormat>,
    new_partition_policies: Vec<NewPartitionPolicy>,
    recreated_partition_policies: Vec<RecreatedPartitionPolicy>,
    destination_error_policies: Vec<DestinationErrorPolicy>,
    destination_desired_states: Vec<DestinationDesiredState>,
    partition_lifecycle_states: Vec<PartitionLifecycleState>,
    destination_block_codes: Vec<DestinationBlockCode>,
    checkpoint_read_consistency: Vec<CheckpointReadConsistency>,
    destination_effective_states: Vec<DestinationEffectiveState>,
    partition_lifecycle_changes: Vec<PartitionLifecycleChange>,
    repair_actions: Vec<RepairAction>,
    sql_dialects: Vec<SqlDialect>,
    query_consistency: Vec<Consistency>,
    comparison_operators: Vec<CmpOp>,
    sort_directions: Vec<Dir>,
    aggregate_functions: Vec<AggFunc>,
    boundary_relations: Vec<laser_wire::query::BoundaryRelation>,
    query_execution_states: Vec<QueryExecutionState>,
    query_error_codes: Vec<QueryErrorCode>,
    logical_type_kinds: Vec<LogicalTypeKind>,
    backend_modes: Vec<BackendMode>,
    backend_desired_states: Vec<BackendDesiredState>,
    backend_observed_states: Vec<BackendObservedState>,
    backend_readiness_codes: Vec<BackendReadinessCode>,
    time_travel_capabilities: Vec<TimeTravelCapability>,
    query_paging_capabilities: Vec<QueryPagingCapability>,
    operation_states: Vec<OperationState>,
}

fn data_stack_string_discriminants() -> DataStackStringDiscriminants {
    DataStackStringDiscriminants {
        arrow_timestamp_units: vec![ArrowTimestampUnit::Microsecond],
        arrow_rejections: vec![
            ArrowIpcRejectionCode::FileFormat,
            ArrowIpcRejectionCode::MissingSchema,
            ArrowIpcRejectionCode::MissingDictionary,
            ArrowIpcRejectionCode::DictionaryDelta,
            ArrowIpcRejectionCode::DictionaryReplacement,
            ArrowIpcRejectionCode::Union,
            ArrowIpcRejectionCode::ExtensionType,
            ArrowIpcRejectionCode::TimestampUnit,
            ArrowIpcRejectionCode::DecimalWidth,
            ArrowIpcRejectionCode::SchemaFingerprint,
            ArrowIpcRejectionCode::FieldLimit,
            ArrowIpcRejectionCode::BatchLimit,
            ArrowIpcRejectionCode::RowLimit,
            ArrowIpcRejectionCode::ByteLimit,
            ArrowIpcRejectionCode::MalformedStream,
        ],
        file_formats: vec![FileFormat::Parquet],
        table_formats: vec![TableFormat::IcebergV2],
        new_partition_policies: vec![
            NewPartitionPolicy::Beginning,
            NewPartitionPolicy::CapturedLatest,
            NewPartitionPolicy::Reject,
        ],
        recreated_partition_policies: vec![RecreatedPartitionPolicy::Reject],
        destination_error_policies: vec![DestinationErrorPolicy::Block],
        destination_desired_states: vec![
            DestinationDesiredState::Disabled,
            DestinationDesiredState::Enabled,
        ],
        partition_lifecycle_states: vec![
            PartitionLifecycleState::Active,
            PartitionLifecycleState::Removed,
            PartitionLifecycleState::Recreated,
        ],
        destination_block_codes: vec![
            DestinationBlockCode::Decode,
            DestinationBlockCode::Schema,
            DestinationBlockCode::Projection,
            DestinationBlockCode::Value,
            DestinationBlockCode::Size,
            DestinationBlockCode::RetentionGap,
            DestinationBlockCode::PreparedAttempt,
            DestinationBlockCode::BackendGeneration,
            DestinationBlockCode::BackendUnavailable,
            DestinationBlockCode::TableIdentity,
            DestinationBlockCode::CatalogOutcomeUnknown,
            DestinationBlockCode::SourceIncarnation,
            DestinationBlockCode::Authorization,
        ],
        checkpoint_read_consistency: vec![
            CheckpointReadConsistency::Linearizable,
            CheckpointReadConsistency::PotentiallyStale,
        ],
        destination_effective_states: vec![
            DestinationEffectiveState::Disabled,
            DestinationEffectiveState::WaitingForBackend,
            DestinationEffectiveState::Ready,
            DestinationEffectiveState::Running,
            DestinationEffectiveState::Blocked,
        ],
        partition_lifecycle_changes: vec![
            PartitionLifecycleChange::Removed,
            PartitionLifecycleChange::Recreated,
        ],
        repair_actions: vec![
            RepairAction::ReconciledPreparedAttempt,
            RepairAction::AcceptedRetentionGap,
            RepairAction::ClearedRetryableBlock,
            RepairAction::SupersededGeneration,
        ],
        sql_dialects: vec![
            SqlDialect::DataFusion,
            SqlDialect::Postgres,
            SqlDialect::MySql,
            SqlDialect::Sqlite,
        ],
        query_consistency: vec![
            Consistency::Eventual,
            Consistency::ReadYourWrites,
            Consistency::Strong,
        ],
        comparison_operators: vec![
            CmpOp::Eq,
            CmpOp::Ne,
            CmpOp::Lt,
            CmpOp::Lte,
            CmpOp::Gt,
            CmpOp::Gte,
            CmpOp::In,
            CmpOp::Contains,
            CmpOp::Prefix,
        ],
        sort_directions: vec![Dir::Asc, Dir::Desc],
        aggregate_functions: vec![
            AggFunc::Count,
            AggFunc::CountDistinct,
            AggFunc::Sum,
            AggFunc::Avg,
            AggFunc::Min,
            AggFunc::Max,
            AggFunc::Percentile,
            AggFunc::StdDev,
        ],
        boundary_relations: vec![
            laser_wire::query::BoundaryRelation::Current,
            laser_wire::query::BoundaryRelation::Historical,
            laser_wire::query::BoundaryRelation::AheadOfObservedCheckpoint,
        ],
        query_execution_states: vec![
            QueryExecutionState::Queued,
            QueryExecutionState::Planning,
            QueryExecutionState::Running,
            QueryExecutionState::Completed,
            QueryExecutionState::Cancelled,
            QueryExecutionState::Failed,
            QueryExecutionState::Expired,
        ],
        query_error_codes: vec![
            QueryErrorCode::Unsupported,
            QueryErrorCode::Unauthorized,
            QueryErrorCode::IndexNotFound,
            QueryErrorCode::ForkNotFound,
            QueryErrorCode::Backend,
            QueryErrorCode::Unavailable,
            QueryErrorCode::TooLarge,
            QueryErrorCode::Version,
            QueryErrorCode::Stale,
            QueryErrorCode::Cancelled,
            QueryErrorCode::DeadlineExceeded,
            QueryErrorCode::ExpiredSnapshot,
            QueryErrorCode::StaleGeneration,
            QueryErrorCode::TargetUnavailable,
            QueryErrorCode::ResourceLimit,
        ],
        logical_type_kinds: vec![
            LogicalTypeKind::Boolean,
            LogicalTypeKind::Int,
            LogicalTypeKind::Long,
            LogicalTypeKind::Float,
            LogicalTypeKind::Double,
            LogicalTypeKind::Decimal,
            LogicalTypeKind::Date,
            LogicalTypeKind::TimeMicros,
            LogicalTypeKind::TimestampMicros,
            LogicalTypeKind::TimestampTzMicros,
            LogicalTypeKind::String,
            LogicalTypeKind::Uuid,
            LogicalTypeKind::Fixed,
            LogicalTypeKind::Binary,
            LogicalTypeKind::Struct,
            LogicalTypeKind::List,
            LogicalTypeKind::Map,
        ],
        backend_modes: vec![BackendMode::Operational, BackendMode::Lakehouse],
        backend_desired_states: vec![BackendDesiredState::Disabled, BackendDesiredState::Enabled],
        backend_observed_states: vec![
            BackendObservedState::Disabled,
            BackendObservedState::Starting,
            BackendObservedState::Ready,
            BackendObservedState::Degraded,
            BackendObservedState::Unavailable,
        ],
        backend_readiness_codes: vec![
            BackendReadinessCode::Disabled,
            BackendReadinessCode::ConfigurationPending,
            BackendReadinessCode::ConfigurationRejected,
            BackendReadinessCode::CredentialUnavailable,
            BackendReadinessCode::ObjectStoreUnavailable,
            BackendReadinessCode::CatalogUnavailable,
            BackendReadinessCode::QueryRuntimeUnavailable,
            BackendReadinessCode::GenerationMismatch,
            BackendReadinessCode::ProbeFailed,
        ],
        time_travel_capabilities: vec![
            TimeTravelCapability::SnapshotId,
            TimeTravelCapability::TimestampMicros,
        ],
        query_paging_capabilities: vec![
            QueryPagingCapability::Offset,
            QueryPagingCapability::Cursor,
        ],
        operation_states: vec![
            OperationState::Accepted,
            OperationState::Running,
            OperationState::Succeeded,
            OperationState::Failed,
            OperationState::Cancelled,
        ],
    }
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn assert_frame<T>(name: &str, value: &T)
where
    T: Serialize + DeserializeOwned,
{
    let encoded = encode_named(value).expect("fixture value serializes");
    let path = fixture_path(name);
    if std::env::var(REGEN_ENV).is_ok() {
        std::fs::write(&path, &encoded).expect("write fixture");
    }
    let golden = std::fs::read(&path)
        .unwrap_or_else(|error| panic!("read fixture {name}: {error} (regen with {REGEN_ENV}=1)"));
    assert_eq!(
        encoded, golden,
        "fixture `{name}` drifted from the canonical frame"
    );
    let decoded: T = decode_named(&golden).expect("fixture frame decodes");
    let reencoded = encode_named(&decoded).expect("decoded value re-serializes");
    assert_eq!(reencoded, golden, "fixture `{name}` decode round-trip");
}

// Same for a JSON fixture (the HTTP path's encoding).
fn assert_json<T>(name: &str, value: &T)
where
    T: Serialize + DeserializeOwned,
{
    let encoded = serde_json::to_string_pretty(value).expect("fixture value serializes");
    let path = fixture_path(name);
    if std::env::var(REGEN_ENV).is_ok() {
        std::fs::write(&path, &encoded).expect("write fixture");
    }
    let golden = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read fixture {name}: {error} (regen with {REGEN_ENV}=1)"));
    assert_eq!(
        encoded, golden,
        "fixture `{name}` drifted from the canonical frame"
    );
    let decoded: T = serde_json::from_str(&golden).expect("fixture frame decodes");
    let reencoded = serde_json::to_string_pretty(&decoded).expect("decoded value re-serializes");
    assert_eq!(reencoded, golden, "fixture `{name}` decode round-trip");
}

// The embedded corpus and the on-disk directory must be the same set, so a new
// fixture cannot land on disk without a registration in `fixtures::ALL` (or the
// reverse). This kills the drift class where a consumer's `fixtures` feature
// pins fewer frames than the suite writes.
#[test]
fn given_fixture_directory_when_compared_to_embedded_corpus_then_should_match_exactly() {
    let embedded: std::collections::BTreeSet<String> = laser_wire::fixtures::ALL
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect();
    let on_disk: std::collections::BTreeSet<String> =
        std::fs::read_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures"))
            .expect("read fixtures dir")
            .map(|entry| {
                entry
                    .expect("fixtures dir entry")
                    .file_name()
                    .into_string()
                    .expect("fixture name is utf8")
            })
            .filter(|name| name.ends_with(".bin") || name.ends_with(".json"))
            .collect();
    assert_eq!(
        embedded, on_disk,
        "fixtures::ALL must list every file in wire/fixtures/ exactly"
    );
}

fn canonical_projection() -> Projection {
    Projection {
        id: ProjectionId::new("order.v1"),
        name: "order".to_owned(),
        version: 1,
        kind: ProjectionKind::Row,
        content_type: ContentType::Json,
        extraction: IndexSchema {
            fields: vec![
                IndexField::new("order_id", "/order_id"),
                IndexField::new("customer", "/customer/id"),
                IndexField::typed("amount", "/amount", FieldType::Int),
            ],
            vector_field: Some("/embedding".to_owned()),
            inline_payload: true,
        },
        entity_schema: None,
        inline_payload_default: true,
    }
}

fn canonical_binding() -> ProjectionBinding {
    ProjectionBinding {
        source: SourceSelector::new("shop", "orders"),
        allowed_projections: vec![ProjectionId::new("order.v1")],
        default_projection: Some(ProjectionId::new("order.v1")),
        backend: Some(BackendBinding {
            resource_id: BackendResourceId::from_u128(4),
            generation: 2,
        }),
        index: "orders_rows".to_owned(),
        notify: false,
        retention: Some(RetentionPolicy::TimeToLive {
            ttl_micros: 3_600_000_000,
        }),
    }
}

fn canonical_avro_schema() -> SchemaDef {
    SchemaDef {
        id: 7,
        source: SchemaSource::Avro {
            schema: r#"{"type":"record","name":"Order","fields":[]}"#.to_owned(),
        },
        name: None,
        version: None,
    }
}

fn canonical_protobuf_schema() -> SchemaDef {
    SchemaDef {
        id: 3,
        source: SchemaSource::Protobuf {
            descriptor_set: vec![0, 1, 2, 255],
            message_type: "shop.Order".to_owned(),
        },
        name: None,
        version: None,
    }
}

fn canonical_json_schema() -> SchemaDef {
    SchemaDef {
        id: 9,
        source: SchemaSource::JsonSchema {
            schema: r#"{"type":"object","required":["customer"]}"#.to_owned(),
        },
        name: Some("order-events".to_owned()),
        version: Some(2),
    }
}

fn canonical_logical_schema() -> LogicalSchema {
    LogicalSchema::new(
        LogicalSchemaId::from_u128(100),
        2,
        vec![
            LogicalField {
                id: 1,
                name: "order_id".to_owned(),
                required: true,
                field_type: LogicalType::Uuid,
                doc: Some("Stable order identity".to_owned()),
            },
            LogicalField {
                id: 2,
                name: "line_items".to_owned(),
                required: true,
                field_type: LogicalType::List {
                    element_id: 3,
                    element_required: true,
                    element: Box::new(LogicalType::Struct {
                        fields: vec![LogicalField {
                            id: 4,
                            name: "amount".to_owned(),
                            required: true,
                            field_type: LogicalType::Decimal {
                                precision: 18,
                                scale: 2,
                            },
                            doc: None,
                        }],
                    }),
                },
                doc: None,
            },
        ],
    )
    .expect("canonical logical schema is valid")
}

fn canonical_incarnation(partition_id: u32) -> SourceIncarnation {
    SourceIncarnation {
        cluster: PhysicalClusterIncarnation::from_u128(200),
        stream_id: 2,
        topic_id: 3,
        partition_id,
        partition_created_revision: 4,
    }
}

fn canonical_destination() -> MaterializationDestination {
    MaterializationDestination {
        id: DestinationId::from_u128(300),
        generation: 2,
        definition_revision: 7,
        name: "orders-lakehouse".to_owned(),
        source: SourceScope::new("shop", "orders"),
        recreated_partition_policy: RecreatedPartitionPolicy::Reject,
        projection: ProjectionRef {
            id: ProjectionId::new("order.v1"),
            version: 3,
        },
        schema: canonical_logical_schema().schema,
        backend: BackendBinding {
            resource_id: BackendResourceId::from_u128(301),
            generation: 5,
        },
        table: PhysicalTable {
            namespace: vec!["shop".to_owned(), "analytics".to_owned()],
            table: "orders".to_owned(),
            expected_table_uuid: Some(UuidValue::new([9; 16])),
        },
        file_format: FileFormat::Parquet,
        table_format: TableFormat::IcebergV2,
        start_policy: StartPolicy::CapturedLatest,
        new_partition_policy: NewPartitionPolicy::CapturedLatest,
        error_policy: DestinationErrorPolicy::Block,
        desired_state: DestinationDesiredState::Enabled,
    }
}

fn canonical_checkpoint_status() -> DestinationCheckpointStatus {
    DestinationCheckpointStatus {
        destination_id: DestinationId::from_u128(300),
        destination_generation: 2,
        backend: canonical_destination().backend,
        schema: canonical_logical_schema().schema,
        projection: canonical_destination().projection,
        global_state_revision: 11,
        definition_revision: 7,
        checkpoint_revision: 3,
        desired_state: DestinationDesiredState::Enabled,
        effective_state: DestinationEffectiveState::Running,
        table_uuid: Some(UuidValue::new([9; 16])),
        owner: Some(laser_wire::checkpoint::CheckpointOwnerLease {
            owner: CheckpointOwnerId::from_u128(401),
            epoch: 2,
            sequence: 4,
            deadline_micros: TIMESTAMP_MICROS + 30_000_000,
        }),
        partitions: vec![PartitionCheckpoint {
            incarnation: canonical_incarnation(0),
            started_at_offset: 100,
            next_offset: 150,
            lifecycle: PartitionLifecycleState::Active,
        }],
        prepared_attempt: None,
        last_completion: None,
        retention_gap: None,
        block: None,
        last_repair: None,
        consistency: CheckpointReadConsistency::Linearizable,
    }
}

fn canonical_table_requirements() -> PreparedTableRequirements {
    let table_uuid = UuidValue::new([9; 16]);
    let base_metadata_identity = "metadata/00001.json".to_owned();
    PreparedTableRequirements {
        table_uuid: table_uuid.clone(),
        base_metadata_identity: base_metadata_identity.clone(),
        base_snapshot_id: Some(40),
        schema_id: 3,
        partition_spec_id: 1,
        commit_requirements: vec![
            IcebergCommitRequirement::AssertTableUuid { table_uuid },
            IcebergCommitRequirement::AssertMetadataIdentity {
                identity: base_metadata_identity,
            },
            IcebergCommitRequirement::AssertCurrentSnapshot {
                snapshot_id: Some(40),
            },
            IcebergCommitRequirement::AssertCurrentSchema { schema_id: 3 },
            IcebergCommitRequirement::AssertDefaultPartitionSpec {
                partition_spec_id: 1,
            },
        ],
    }
}

fn canonical_source_range() -> SourceOffsetRange {
    SourceOffsetRange {
        incarnation: canonical_incarnation(0),
        start: 100,
        end_exclusive: 150,
    }
}

fn canonical_prepared_attempt() -> PreparedAttempt {
    let range = canonical_source_range();
    PreparedAttempt {
        id: PreparedAttemptId::from_u128(402),
        destination_id: DestinationId::from_u128(300),
        destination_generation: 2,
        backend: canonical_destination().backend,
        owner: CheckpointOwnerId::from_u128(401),
        epoch: 2,
        created_at_checkpoint_revision: 3,
        table: canonical_table_requirements(),
        schema_fingerprint: SchemaFingerprint::new([6; 32]),
        projection: canonical_destination().projection,
        ranges: vec![range.clone()],
        resulting_boundary: vec![PartitionCheckpoint {
            incarnation: range.incarnation,
            started_at_offset: 100,
            next_offset: 150,
            lifecycle: PartitionLifecycleState::Active,
        }],
        resulting_boundary_digest: Digest32::new([7; 32]),
        manifest_identity: "manifests/attempt-402.avro".to_owned(),
        manifest_digest: Digest32::new([8; 32]),
        objects: vec![AttemptObject {
            identity: "data/attempt-402-0.parquet".to_owned(),
            size_bytes: 4_096,
            row_count: 50,
            sha256: Digest32::new([10; 32]),
            columns: vec![AttemptColumnMetrics {
                field_id: 1,
                value_count: 50,
                null_count: 0,
                nan_count: 0,
                lower_bound: Some(TypedValue::Long(1)),
                upper_bound: Some(TypedValue::Long(50)),
            }],
        }],
        credential_generations: vec![CredentialGeneration {
            role: "writer".to_owned(),
            generation: 4,
        }],
    }
}

fn canonical_completion() -> CompletedAttempt {
    CompletedAttempt {
        id: PreparedAttemptId::from_u128(402),
        table_uuid: UuidValue::new([9; 16]),
        snapshot_id: 41,
        manifest_digest: Digest32::new([8; 32]),
        resulting_boundary_digest: Digest32::new([7; 32]),
        ranges: vec![canonical_source_range()],
        completion_revision: 4,
    }
}

fn canonical_query_route() -> QueryRoute {
    QueryRoute {
        id: QueryRouteId::from_u128(302),
        generation: 2,
        definition_revision: 8,
        name: "orders-history".to_owned(),
        target: QueryRouteTarget::Lakehouse {
            destination_id: DestinationId::from_u128(300),
            destination_generation: 2,
        },
        desired_state: DestinationDesiredState::Enabled,
    }
}

fn base_query(index: &str, execution_id: u128) -> Query {
    Query {
        execution_id: QueryExecutionId::from_u128(execution_id),
        target: QueryTarget::operational(index),
        deadline_micros: TIMESTAMP_MICROS + 30_000_000,
        by_key: Vec::new(),
        message_type: None,
        time_range: None,
        filter: None,
        vector: None,
        text: None,
        order: Vec::new(),
        page: QueryPageRequest::default(),
        aggregate: None,
        having: None,
        distinct: false,
        select: Select::default(),
        fork: None,
        raw_sql: None,
        consistency: Consistency::Eventual,
    }
}

fn canonical_query() -> Query {
    Query {
        by_key: vec![KeyMatch::new("customer_id", "alice")],
        message_type: Some("order_created".to_owned()),
        time_range: Some((1_000, 2_000)),
        filter: Some(Filter::all([
            Filter::pred("status", CmpOp::Eq, "paid"),
            Filter::any([
                Filter::pred("amount", CmpOp::Gte, 100i64),
                Filter::negate(Filter::pred("region", CmpOp::Eq, "eu")),
            ]),
        ])),
        vector: Some(VectorQuery {
            field: "embedding".to_owned(),
            embedding: vec![0.25, -0.5, 0.125],
            top_k: 5,
        }),
        text: None,
        order: vec![Sort {
            field: "ts".to_owned(),
            dir: Dir::Desc,
        }],
        page: QueryPageRequest {
            limit: 20,
            offset: Some(40),
            cursor: None,
            want_total: false,
        },
        aggregate: None,
        having: None,
        distinct: false,
        select: Select {
            fields: Vec::new(),
            payload: true,
        },
        fork: Some("agent-run-7".to_owned()),
        raw_sql: None,
        consistency: Consistency::Eventual,
        ..base_query("orders", 1)
    }
}

fn canonical_aggregate_query() -> Query {
    Query {
        aggregate: Some(Aggregate {
            group_by: vec!["route".to_owned()],
            funcs: vec![
                AggCall {
                    func: AggFunc::Count,
                    field: None,
                    arg: None,
                    alias: "n".to_owned(),
                },
                AggCall {
                    func: AggFunc::Percentile,
                    field: Some("latency_ms".to_owned()),
                    arg: Some(0.95),
                    alias: "p95".to_owned(),
                },
            ],
            window: Some(Window {
                field: "ts".to_owned(),
                every_micros: 60_000_000,
            }),
        }),
        having: Some(Filter::pred("n", CmpOp::Gt, 10i64)),
        distinct: true,
        page: QueryPageRequest {
            limit: 100,
            ..QueryPageRequest::default()
        },
        ..base_query("metrics", 2)
    }
}

fn canonical_raw_sql_query() -> Query {
    Query {
        page: QueryPageRequest {
            limit: 10,
            ..QueryPageRequest::default()
        },
        raw_sql: Some(RawSql {
            dialect: SqlDialect::DataFusion,
            sql: "SELECT customer, amount FROM orders_rows WHERE amount > ? LIMIT ?".to_owned(),
            params: vec![
                TypedValue::Long(100),
                TypedValue::Long(i64::MAX),
                TypedValue::Double(0.5),
                TypedValue::Boolean(true),
                TypedValue::String("x".to_owned()),
                TypedValue::Null,
                TypedValue::List(vec![TypedValue::Long(1), TypedValue::Long(2)]),
            ],
        }),
        ..base_query("orders", 3)
    }
}

fn canonical_query_result() -> QueryResult {
    QueryResult {
        fields: vec![
            LogicalField {
                id: 1,
                name: "amount".to_owned(),
                required: true,
                field_type: LogicalType::Long,
                doc: None,
            },
            LogicalField {
                id: 2,
                name: "customer".to_owned(),
                required: true,
                field_type: LogicalType::String,
                doc: None,
            },
        ],
        rows: vec![Row {
            values: vec![TypedValue::Long(42), TypedValue::String("alice".to_owned())],
            score: Some(0.25),
        }],
        page: Page {
            offset: Some(0),
            limit: 50,
            total: Some(1),
            has_more: false,
            next_cursor: None,
        },
        context: QueryContext {
            execution_id: QueryExecutionId::from_u128(1),
            engine: QueryEngine {
                name: "datafusion".to_owned(),
                version: "50.0.0".to_owned(),
                dialect: Some(SqlDialect::DataFusion),
            },
            resolved_target: ResolvedQueryTarget::Operational {
                index: "orders".to_owned(),
                backend_resource_id: BackendResourceId::from_u128(10),
                backend_generation: 4,
                runtime_configuration_revision: 9,
            },
            requested_consistency: Consistency::Eventual,
            delivered_consistency: Consistency::Eventual,
            boundary: None,
            checkpoint_revision: None,
            global_state_revision: None,
            truncated: false,
            elapsed_micros: 12_000,
            scanned_bytes: 4_096,
            produced_bytes: 128,
            row_count: 1,
        },
    }
}

fn canonical_backend(
    resource_id: u128,
    mode: BackendMode,
    label: &str,
    kind: &str,
) -> BackendDescriptor {
    let materialization = if mode == BackendMode::Lakehouse {
        vec![MaterializationCapability {
            file_format: FileFormat::Parquet,
            table_format: TableFormat::IcebergV2,
            create_table: true,
            append: true,
        }]
    } else {
        Vec::new()
    };
    BackendDescriptor::new(
        BackendResourceId::from_u128(resource_id),
        mode,
        label,
        BackendImplementation {
            kind: kind.to_owned(),
            version: "2.1.0".to_owned(),
        },
        4,
        9,
    )
    .with_state(
        BackendDesiredState::Enabled,
        BackendObservedState::Ready,
        BackendReadiness::ready(TIMESTAMP_MICROS),
    )
    .with_materialization(materialization)
    .with_query(QueryCapabilities {
        dialects: vec![SqlDialect::DataFusion],
        time_travel: vec![
            TimeTravelCapability::SnapshotId,
            TimeTravelCapability::TimestampMicros,
        ],
        consistency: vec![Consistency::Eventual, Consistency::ReadYourWrites],
        logical_types: vec![LogicalTypeKind::Long, LogicalTypeKind::String],
        paging: vec![QueryPagingCapability::Offset, QueryPagingCapability::Cursor],
        cancellation: true,
        execution_status: true,
        raw_sql: true,
    })
    .with_schema(SchemaCapabilities {
        logical_schema: true,
        arrow_ipc_stream: true,
        schema_evolution: true,
    })
    .with_limits(BackendLimits {
        max_query_rows: 100_000,
        max_query_bytes: 8_388_608,
        max_scan_bytes: 1_073_741_824,
        max_query_micros: 30_000_000,
        max_concurrent_queries: 16,
        max_schema_fields: 4_096,
        max_materialization_file_bytes: 536_870_912,
    })
}

fn canonical_fork_info() -> ForkInfo {
    ForkInfo {
        fork_id: "agent-run-7".to_owned(),
        parent: Some("trunk".to_owned()),
        kind: ForkKind::Severed,
        user_id: 5,
        status: ForkStatus::Open,
        created_at_micros: TIMESTAMP_MICROS,
        row_count: 0,
    }
}

#[test]
fn given_query_frames_when_encoded_then_should_match_golden_fixtures() {
    let destination_id = DestinationId::from_u128(300);
    assert_frame(
        "query_target_discriminants.bin",
        &vec![
            QueryTarget::operational("orders"),
            QueryTarget::Lakehouse {
                destination_id,
                destination_generation: 2,
                snapshot: None,
            },
            QueryTarget::Lakehouse {
                destination_id,
                destination_generation: 2,
                snapshot: Some(laser_wire::query::SnapshotSelector::SnapshotId(41)),
            },
            QueryTarget::Lakehouse {
                destination_id,
                destination_generation: 2,
                snapshot: Some(laser_wire::query::SnapshotSelector::TimestampMicros(
                    TIMESTAMP_MICROS as i64,
                )),
            },
        ],
    );
    let execution_id = QueryExecutionId::from_u128(500);
    assert_frame(
        "query_error_discriminants.bin",
        &vec![
            QueryError::Unsupported("feature".to_owned()),
            QueryError::Unauthorized("query".to_owned()),
            QueryError::IndexNotFound("orders".to_owned()),
            QueryError::ForkNotFound("run-7".to_owned()),
            QueryError::Backend("planning failed".to_owned()),
            QueryError::Unavailable("runtime restarting".to_owned()),
            QueryError::TooLarge {
                what: "rows".to_owned(),
                size: 101,
                cap: 100,
            },
            QueryError::Version {
                expected: QUERY_OP_VERSION,
                got: QUERY_OP_VERSION + 1,
            },
            QueryError::Stale {
                what: "orders".to_owned(),
                applied: 40,
                required: 41,
            },
            QueryError::Cancelled { execution_id },
            QueryError::DeadlineExceeded { execution_id },
            QueryError::ExpiredSnapshot { snapshot_id: 41 },
            QueryError::StaleGeneration {
                what: "destination".to_owned(),
                requested: 2,
                observed: 3,
            },
            QueryError::TargetUnavailable {
                reason: "backend not ready".to_owned(),
            },
            QueryError::ResourceLimit {
                resource: "scan_bytes".to_owned(),
                observed: 101,
                limit: 100,
            },
        ],
    );
    assert_frame("query_envelope.bin", &QueryEnvelope::new(canonical_query()));
    assert_frame(
        "query_envelope_aggregate.bin",
        &QueryEnvelope::new(canonical_aggregate_query()),
    );
    assert_frame(
        "query_envelope_raw_sql.bin",
        &QueryEnvelope::new(canonical_raw_sql_query()),
    );
    assert_frame(
        "query_reply_ok.bin",
        &QueryReply::Ok(Box::new(canonical_query_result())),
    );
    assert_frame(
        "query_reply_err_too_large.bin",
        &QueryReply::Err(QueryError::TooLarge {
            what: "limit".to_owned(),
            size: 2_000,
            cap: 1_000,
        }),
    );
    assert_frame(
        "query_envelope_read_your_writes.bin",
        &QueryEnvelope::new(Query {
            consistency: Consistency::ReadYourWrites,
            ..base_query("orders", 4)
        }),
    );
    assert_frame(
        "query_envelope_text.bin",
        &QueryEnvelope::new(Query {
            text: Some(TextQuery {
                field: Some("summary".to_owned()),
                query: "refund dispute".to_owned(),
            }),
            ..base_query("orders", 5)
        }),
    );
    assert_frame(
        "query_reply_err_stale.bin",
        &QueryReply::Err(QueryError::Stale {
            what: "orders".to_owned(),
            applied: 41,
            required: 57,
        }),
    );
}

#[test]
fn given_data_contract_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "data_stack_string_discriminants.bin",
        &data_stack_string_discriminants(),
    );
    assert_frame(
        "start_policy_discriminants.bin",
        &vec![
            StartPolicy::Beginning,
            StartPolicy::CapturedLatest,
            StartPolicy::Explicit {
                partitions: vec![PartitionStart {
                    incarnation: canonical_incarnation(0),
                    next_offset: 100,
                }],
            },
        ],
    );
    assert_frame("logical_schema.bin", &canonical_logical_schema());
    assert_frame(
        "logical_type_discriminants.bin",
        &vec![
            LogicalType::Boolean,
            LogicalType::Int,
            LogicalType::Long,
            LogicalType::Float,
            LogicalType::Double,
            LogicalType::Decimal {
                precision: 18,
                scale: 2,
            },
            LogicalType::Date,
            LogicalType::TimeMicros,
            LogicalType::TimestampMicros,
            LogicalType::TimestampTzMicros,
            LogicalType::String,
            LogicalType::Uuid,
            LogicalType::Fixed { length: 8 },
            LogicalType::Binary,
            LogicalType::Struct {
                fields: vec![LogicalField {
                    id: 1,
                    name: "nested".to_owned(),
                    required: false,
                    field_type: LogicalType::String,
                    doc: None,
                }],
            },
            LogicalType::List {
                element_id: 2,
                element_required: false,
                element: Box::new(LogicalType::String),
            },
            LogicalType::Map {
                key_id: 3,
                key: Box::new(LogicalType::String),
                value_id: 4,
                value_required: false,
                value: Box::new(LogicalType::Long),
            },
        ],
    );
    assert_frame(
        "typed_value_discriminants.bin",
        &vec![
            TypedValue::Null,
            TypedValue::Boolean(true),
            TypedValue::Int(-7),
            TypedValue::Long(9),
            TypedValue::Float(0.5),
            TypedValue::Double(-0.25),
            TypedValue::Decimal(DecimalValue {
                unscaled: vec![0x04, 0xd2],
                precision: 6,
                scale: 2,
            }),
            TypedValue::Date(20_000),
            TypedValue::TimeMicros(1_000),
            TypedValue::TimestampMicros(-1),
            TypedValue::TimestampTzMicros(2),
            TypedValue::String("typed".to_owned()),
            TypedValue::Uuid(UuidValue::new([1; 16])),
            TypedValue::Fixed(BinaryValue(vec![2; 8])),
            TypedValue::Binary(BinaryValue(vec![0, 255])),
            TypedValue::Struct(vec![FieldValue {
                field_id: 1,
                value: TypedValue::String("nested".to_owned()),
            }]),
            TypedValue::List(vec![TypedValue::Long(1)]),
            TypedValue::Map(vec![MapEntry {
                key: TypedValue::String("key".to_owned()),
                value: TypedValue::Long(2),
            }]),
        ],
    );
    assert_frame("materialization_destination.bin", &canonical_destination());
    assert_frame("query_route.bin", &canonical_query_route());
    assert_frame(
        "arrow_ipc_metadata.bin",
        &ArrowIpcMessageMetadata {
            contract_version: 1,
            schema_fingerprint: canonical_logical_schema().schema.fingerprint,
            encoded_bytes: 4_096,
            field_count: 4,
            record_batch_count: 1,
            row_count: 100,
            dictionary_count: 1,
        },
    );
    assert_frame("arrow_ipc_policy.bin", &ArrowIpcPolicy::default());
}

#[test]
fn given_checkpoint_frames_when_encoded_then_should_match_golden_fixtures() {
    let public = CheckpointRequestEnvelope::new(
        CheckpointRequestId::from_u128(400),
        10,
        PublicCheckpointMutation::RegisterDestination {
            destination: canonical_destination(),
        },
    );
    assert_frame("checkpoint_request_public.bin", &public);
    let destination_id = DestinationId::from_u128(300);
    let destination_generation = 2;
    let owner = CheckpointOwnerId::from_u128(401);
    let block = DestinationBlock {
        code: DestinationBlockCode::PreparedAttempt,
        message: "catalog outcome requires reconciliation".to_owned(),
        incarnation: Some(canonical_incarnation(0)),
        offset: Some(149),
        row_ordinal: Some(49),
    };
    let gap = RetentionGap {
        incarnation: canonical_incarnation(0),
        required_next_offset: 50,
        retained_start: 100,
    };
    let repair = RepairRecord {
        action: RepairAction::ReconciledPreparedAttempt,
        detail: "catalog snapshot 41 contains attempt 402".to_owned(),
    };
    assert_frame(
        "checkpoint_public_mutation_discriminants.bin",
        &vec![
            PublicCheckpointMutation::RegisterDestination {
                destination: canonical_destination(),
            },
            PublicCheckpointMutation::RegisterQueryRoute {
                route: canonical_query_route(),
            },
            PublicCheckpointMutation::RemoveQueryRoute {
                route_id: QueryRouteId::from_u128(302),
                route_generation: 2,
                expected_definition_revision: 8,
            },
            PublicCheckpointMutation::BindTable {
                destination_id,
                destination_generation,
                expected_definition_revision: 7,
                table_uuid: UuidValue::new([9; 16]),
            },
            PublicCheckpointMutation::SetDesiredState {
                destination_id,
                destination_generation,
                expected_definition_revision: 7,
                desired_state: DestinationDesiredState::Disabled,
            },
            PublicCheckpointMutation::AddPartition {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                partition_id: 1,
            },
            PublicCheckpointMutation::ObservePartitionLifecycle {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                partition_id: 1,
            },
            PublicCheckpointMutation::AcquireLease {
                destination_id,
                destination_generation,
                owner,
                expected_lease_sequence: 0,
                lease_duration_micros: 30_000_000,
            },
            PublicCheckpointMutation::RenewLease {
                destination_id,
                destination_generation,
                owner,
                epoch: 2,
                expected_lease_sequence: 4,
                lease_duration_micros: 30_000_000,
            },
            PublicCheckpointMutation::TakeoverLease {
                destination_id,
                destination_generation,
                owner,
                expected_lease_sequence: 4,
                lease_duration_micros: 30_000_000,
            },
            PublicCheckpointMutation::Prepare {
                expected_checkpoint_revision: 3,
                attempt: canonical_prepared_attempt(),
            },
            PublicCheckpointMutation::Complete {
                destination_id,
                destination_generation,
                owner,
                epoch: 2,
                expected_checkpoint_revision: 3,
                completion: canonical_completion(),
            },
            PublicCheckpointMutation::RecordBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                block: block.clone(),
            },
            PublicCheckpointMutation::ClearBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                expected_code: DestinationBlockCode::PreparedAttempt,
            },
            PublicCheckpointMutation::RecordRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                gap,
            },
            PublicCheckpointMutation::AcceptRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                next_offset: 100,
            },
            PublicCheckpointMutation::SupersedeGeneration {
                expected_definition_revision: 7,
                replacement: MaterializationDestination {
                    generation: 3,
                    definition_revision: 8,
                    desired_state: DestinationDesiredState::Disabled,
                    ..canonical_destination()
                },
            },
            PublicCheckpointMutation::RecordRepair {
                destination_id,
                destination_generation,
                expected_checkpoint_revision: 3,
                repair,
            },
        ],
    );
    let replicated = ReplicatedCheckpointMutation {
        request_id: CheckpointRequestId::from_u128(400),
        expected_global_state_revision: 10,
        stamp: CheckpointMutationStamp {
            committed_at_micros: TIMESTAMP_MICROS,
            iggy_actor_id: 0,
            supervisor_actor: None,
        },
        mutation: ReplicatedCheckpointMutationBody::RegisterDestination {
            destination: canonical_destination(),
        },
    };
    assert_frame("checkpoint_mutation_replicated.bin", &replicated);
    assert_frame(
        "checkpoint_reply_destination.bin",
        &CheckpointReply::Ok(CheckpointMutationResult::Destination {
            request_id: CheckpointRequestId::from_u128(400),
            destination_id,
            destination_generation,
            global_state_revision: 11,
            definition_revision: 7,
            checkpoint_revision: 3,
            lease: None,
        }),
    );
    assert_frame(
        "checkpoint_reply_query_route.bin",
        &CheckpointReply::Ok(CheckpointMutationResult::QueryRoute {
            request_id: CheckpointRequestId::from_u128(403),
            route_id: QueryRouteId::from_u128(302),
            route_generation: 2,
            global_state_revision: 12,
            definition_revision: 8,
        }),
    );
    assert!(
        decode_named::<ReplicatedCheckpointMutation>(
            &encode_named(&public).expect("public request encodes")
        )
        .is_err()
    );
    assert!(
        decode_named::<CheckpointRequestEnvelope>(
            &encode_named(&replicated).expect("replicated mutation encodes")
        )
        .is_err()
    );
    assert_frame(
        "destination_checkpoint_status.bin",
        &canonical_checkpoint_status(),
    );
}

#[test]
fn given_query_control_frames_when_encoded_then_should_match_golden_fixtures() {
    let execution_id = QueryExecutionId::from_u128(500);
    assert_frame(
        "query_page.bin",
        &QueryPageEnvelope::new(execution_id, "cursor-2", TIMESTAMP_MICROS + 30_000_000),
    );
    assert_frame("query_cancel.bin", &QueryCancelEnvelope::new(execution_id));
    assert_frame("query_status.bin", &QueryStatusEnvelope::new(execution_id));
    assert_frame(
        "query_status_reply.bin",
        &QueryStatusReply::Ok(QueryExecutionStatus {
            execution_id,
            state: QueryExecutionState::Completed,
            started_at_micros: TIMESTAMP_MICROS,
            finished_at_micros: Some(TIMESTAMP_MICROS + 1_000),
            scanned_bytes: 4_096,
            produced_bytes: 256,
            row_count: 2,
            error: None,
        }),
    );
}

#[test]
fn given_control_frames_when_encoded_then_should_match_golden_fixtures() {
    let envelope = |command| ControlEnvelope {
        v: CONTROL_OP_VERSION,
        timestamp_micros: TIMESTAMP_MICROS,
        command,
    };
    assert_frame(
        "control_register_projection.bin",
        &envelope(ControlCommand::RegisterProjection(canonical_projection())),
    );
    assert_frame(
        "control_apply_binding.bin",
        &envelope(ControlCommand::ApplyBinding(canonical_binding())),
    );
    assert_frame(
        "control_remove_binding.bin",
        &envelope(ControlCommand::RemoveBinding {
            source: SourceSelector::new("shop", "orders"),
            projection_ref: Some("order.v1".to_owned()),
        }),
    );
    assert_frame(
        "control_register_run_source.bin",
        &envelope(ControlCommand::RegisterRunSource(SourceSelector::new(
            "laser-orchestra",
            "agents",
        ))),
    );
    assert_frame(
        "control_remove_run_source.bin",
        &envelope(ControlCommand::RemoveRunSource(SourceSelector::new(
            "laser-orchestra",
            "agents",
        ))),
    );
    assert_frame(
        "control_register_schema_avro.bin",
        &envelope(ControlCommand::RegisterSchema(canonical_avro_schema())),
    );
    assert_frame(
        "control_register_schema_protobuf.bin",
        &envelope(ControlCommand::RegisterSchema(canonical_protobuf_schema())),
    );
    assert_frame(
        "control_drop_schema.bin",
        &envelope(ControlCommand::DropSchema(7)),
    );
    assert_frame(
        "control_register_schema_json.bin",
        &envelope(ControlCommand::RegisterSchema(canonical_json_schema())),
    );
    assert_frame(
        "register_schema_managed.bin",
        &RegisterSchema {
            v: QUERY_OP_VERSION,
            source: SchemaSource::Avro {
                schema: r#"{"type":"record","name":"Order","fields":[]}"#.to_owned(),
            },
            name: Some("fills".to_owned()),
            version: Some(1),
        },
    );
    assert_frame(
        "browse_reply_schema_registered.bin",
        &BrowseReply::Ok(BrowseOutcome::SchemaRegistered(7)),
    );
}

#[test]
fn given_browse_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "browse_reply_projections.bin",
        &BrowseReply::Ok(BrowseOutcome::Projections(vec![ProjectionInfo {
            projection: canonical_projection(),
            bindings: vec![canonical_binding()],
        }])),
    );
    assert_frame(
        "browse_reply_schemas.bin",
        &BrowseReply::Ok(BrowseOutcome::Schemas(vec![
            SchemaInfo {
                schema: canonical_avro_schema(),
                dropped: false,
            },
            SchemaInfo {
                schema: canonical_protobuf_schema(),
                dropped: true,
            },
            SchemaInfo {
                schema: canonical_json_schema(),
                dropped: false,
            },
        ])),
    );
    assert_frame(
        "decode_record.bin",
        &DecodeRecord {
            v: QUERY_OP_VERSION,
            id: 7,
            payload: vec![0xff, 0x00, 0x10],
        },
    );
    assert_frame(
        "browse_reply_decoded.bin",
        &BrowseReply::Ok(BrowseOutcome::Decoded(Some(serde_json::json!({
            "customer": "alice",
            "total": 42
        })))),
    );
}

fn canonical_role() -> Role {
    Role {
        name: "kv-reader".to_owned(),
        grants: vec![
            Grant {
                effect: Effect::Allow,
                feature: Feature::Kv,
                action: Action::Read,
                resource: ResourcePattern::prefix("agent-abc/"),
            },
            Grant {
                effect: Effect::Deny,
                feature: Feature::Kv,
                action: Action::Read,
                resource: ResourcePattern::literal("agent-abc/secret"),
            },
        ],
    }
}

#[test]
fn given_authz_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame("authz_role.bin", &canonical_role());
    assert_frame(
        "authz_whoami_reply.bin",
        &AuthzReply::Whoami(WhoamiReply {
            v: AUTHZ_OP_VERSION,
            roles: vec!["admin".to_owned()],
            grants: vec![Grant {
                effect: Effect::Allow,
                feature: Feature::Kv,
                action: Action::Write,
                resource: ResourcePattern::all(),
            }],
        }),
    );
    assert_frame(
        "authz_get_role_reply.bin",
        &AuthzReply::Role(Some(canonical_role())),
    );
    assert_frame(
        "authz_bind_roles.bin",
        &BindRolesReq {
            v: AUTHZ_OP_VERSION,
            user_id: 7,
            roles: vec!["kv-reader".to_owned(), "admin".to_owned()],
            expect_revision: Some(3),
            // None keeps the frame byte-identical to the recorded fixture:
            // the field is skip-none on the wire.
            mutation_id: None,
        },
    );
    assert_frame(
        "authz_history_request.bin",
        &AuthzHistoryReq {
            v: AUTHZ_OP_VERSION,
            subject: AuthzSubject::Binding { user_id: 7 },
            after_revision: Some(2),
            limit: 50,
        },
    );
    assert_frame(
        "authz_history_reply.bin",
        &AuthzReply::History(AuthzHistoryReply {
            v: AUTHZ_OP_VERSION,
            events: vec![AuthzEvent {
                revision: 3,
                actor: "root".to_owned(),
                at_micros: 1_717_171_717_000_000,
                op: AuthzEventKind::RolesBound {
                    user_id: 7,
                    roles: vec!["kv-reader".to_owned()],
                },
            }],
            next_after_revision: None,
        }),
    );
}

#[test]
fn given_kv_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "kv_set.bin",
        &KvSet {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            key: vec![0xff, 0x00, b'k'],
            value: b"online".to_vec(),
            expires_at_micros: Some(1_700_000_000_000_000),
        },
    );
    assert_frame(
        "kv_cas.bin",
        &KvCas {
            v: KV_OP_VERSION,
            namespace: "counters".to_owned(),
            key: b"hits".to_vec(),
            value: b"42".to_vec(),
            expires_at_micros: None,
            expect: CasExpect::Match(7),
        },
    );
    assert_frame(
        "kv_cas_fenced.bin",
        &KvCasFenced {
            v: KV_LEASE_OP_VERSION,
            namespace: "effects".to_owned(),
            key: b"apply-credit:order-7".to_vec(),
            value: b"done".to_vec(),
            expires_at_micros: None,
            expect: CasExpect::Absent,
            fence_namespace: "coordination".to_owned(),
            fence_key: b"task:order-7".to_vec(),
            fence_token: 3,
        },
    );
    assert_frame(
        "kv_lease.bin",
        &KvLease {
            v: KV_LEASE_OP_VERSION,
            namespace: "coordination".to_owned(),
            key: b"task:order-7".to_vec(),
            lease_ttl_micros: 30_000_000,
            holder_id: "worker-1".to_owned(),
            subject_user_id: Some(42),
        },
    );
    assert_frame(
        "kv_lease_renew.bin",
        &KvLeaseRenew {
            v: KV_LEASE_OP_VERSION,
            namespace: "coordination".to_owned(),
            key: b"task:order-7".to_vec(),
            holder_id: "worker-1".to_owned(),
            subject_user_id: None,
            lease_token: 3,
            lease_ttl_micros: 30_000_000,
        },
    );
    assert_frame(
        "kv_release.bin",
        &KvRelease {
            v: KV_LEASE_OP_VERSION,
            namespace: "coordination".to_owned(),
            key: b"task:order-7".to_vec(),
            lease_token: 3,
            holder_id: "worker-1".to_owned(),
        },
    );
    assert_frame(
        "kv_get_barriered.bin",
        &KvGet {
            v: KV_OP_VERSION,
            namespace: "state".to_owned(),
            key: b"source_state/pg".to_vec(),
            if_none_match: None,
            min_position: Some(MutationPosition {
                topic_generation: 1,
                partition: 0,
                offset: 512,
            }),
        },
    );
    assert_frame(
        "kv_reply_leased.bin",
        &KvReply::Ok(KvOutcome::Leased {
            lease_token: 3,
            granted_ttl_micros: 30_000_000,
            position: MutationPosition {
                topic_generation: 1,
                partition: 0,
                offset: 512,
            },
        }),
    );
    assert_frame(
        "kv_reply_renewed.bin",
        &KvReply::Ok(KvOutcome::Renewed {
            lease_token: 3,
            granted_ttl_micros: 30_000_000,
            position: MutationPosition {
                topic_generation: 1,
                partition: 0,
                offset: 513,
            },
        }),
    );
    assert_frame(
        "kv_reply_stale.bin",
        &KvReply::Err(KvError::Stale {
            required: MutationPosition {
                topic_generation: 1,
                partition: 0,
                offset: 512,
            },
        }),
    );
    assert_frame(
        "kv_copy.bin",
        &KvCopy {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            key: b"user:42".to_vec(),
            to_namespace: Some("archive".to_owned()),
            to_key: b"user:42:2026".to_vec(),
        },
    );
    assert_frame(
        "kv_move.bin",
        &KvMove {
            v: KV_OP_VERSION,
            namespace: "staging".to_owned(),
            key: b"plan:draft".to_vec(),
            to_namespace: None,
            to_key: b"plan:current".to_vec(),
        },
    );
    assert_frame(
        "kv_reply_committed.bin",
        &KvReply::Ok(KvOutcome::Committed { version: 8 }),
    );
    assert_frame(
        "kv_reply_version_conflict.bin",
        &KvReply::Err(KvError::VersionConflict { current: Some(7) }),
    );
    assert_frame("kv_namespaces.bin", &KvNamespaces { v: KV_OP_VERSION });
    assert_frame(
        "kv_reply_namespaces.bin",
        &KvReply::Ok(KvOutcome::Namespaces(vec![
            KvNamespaceInfo {
                namespace: "concierge_sessions".to_owned(),
                entries: 12,
            },
            KvNamespaceInfo {
                namespace: "sessions".to_owned(),
                entries: 3,
            },
        ])),
    );
    assert_frame(
        "kv_scan.bin",
        &KvScan {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            prefix: Some(b"user:".to_vec()),
            start: None,
            end: None,
            key_contains: Some("admin".to_owned()),
            conversation: None,
            limit: 50,
            cursor: Some(b"user:9".to_vec()),
        },
    );
    assert_frame(
        "kv_reply_page.bin",
        &KvReply::Ok(KvOutcome::Page(KvPage {
            entries: vec![KvEntry {
                key: b"user:1".to_vec(),
                value: vec![0, 1, 2],
                expires_at_micros: None,
                version: 0,
                scope: None,
                source: None,
            }],
            cursor: Some(b"user:1".to_vec()),
        })),
    );
}

#[test]
fn given_fork_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "fork_create.bin",
        &ForkCreate {
            v: FORK_OP_VERSION,
            fork_id: "agent-run-7".to_owned(),
            parent: Some("trunk".to_owned()),
            kind: ForkKind::Severed,
            tables: vec!["orders_rows".to_owned()],
        },
    );
    assert_frame(
        "fork_put.bin",
        &ForkPut {
            v: FORK_OP_VERSION,
            fork_id: "agent-run-7".to_owned(),
            table: "orders_rows".to_owned(),
            partition_id: 2,
            offset: 1_000,
            projection_id: "order.v1".to_owned(),
            projection_version: 1,
            fields: BTreeMap::from([("amount".to_owned(), "999".to_owned())]),
            metadata: BTreeMap::from([("note".to_owned(), "speculative".to_owned())]),
            payload: Some(b"body".to_vec()),
            embedding: Some("[0.1,0.2]".to_owned()),
            tombstone: false,
        },
    );
    assert_frame(
        "fork_reply_created.bin",
        &ForkReply::Ok(ForkOutcome::Created(canonical_fork_info())),
    );
}

#[test]
fn given_graph_frames_when_encoded_then_should_match_golden_fixtures() {
    let alice = GraphNode::entity("Person", "Alice");
    let acme = GraphNode::entity("Company", "Acme");
    let mut doc = GraphNode::entity("Doc", "spec");
    doc.embedding = Some(vec![0.1, 0.2, 0.3]);
    assert_frame("graph_node.bin", &doc);

    let edge = GraphEdge::relate(&alice, "works_at", &acme).valid(Some(1_000), Some(2_000));
    assert_frame("graph_edge.bin", &edge);

    assert_frame(
        "graph_upsert.bin",
        &GraphUpsert {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            nodes: vec![alice.clone(), acme.clone()],
            edges: vec![edge.clone()],
        },
    );

    assert_frame(
        "graph_query.bin",
        &GraphQuery {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            start: GraphStart::Match(Filter::pred("label", CmpOp::Eq, "Person")),
            traverse: vec![Hop {
                edge_type: Some("works_at".to_owned()),
                dir: EdgeDir::Out,
                max: 2,
            }],
            node_filter: None,
            edge_filter: None,
            return_: GraphReturn::Paths,
            limit: 100,
            fork: None,
            consistency: Consistency::Eventual,
            as_of: Some(1_500),
            conversation: None,
        },
    );

    assert_frame(
        "graph_neighbors.bin",
        &GraphNeighbors {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            node: alice.id,
            dir: EdgeDir::Out,
            edge_type: Some("works_at".to_owned()),
            depth: 1,
            limit: 50,
            as_of: Some(1_500),
            conversation: None,
        },
    );

    assert_frame(
        "graph_reply.bin",
        &GraphReply::Ok(GraphResult {
            nodes: vec![alice.clone(), acme.clone()],
            edges: vec![edge.clone()],
            paths: vec![Path {
                nodes: vec![alice.id, acme.id],
                edges: vec![edge.id],
            }],
        }),
    );

    // Provenance is pinned cross-SDK on a dedicated node and edge, so the
    // canonical source-less frames above stay byte-identical. An unset
    // conversation keeps these frames identical to the pre-conversation contract.
    let source = SourceRef::Message {
        stream: 7,
        topic: 2,
        partition: 3,
        offset: 4096,
        conversation: None,
    };
    let mut sourced_node = GraphNode::entity("Component", "cache");
    sourced_node.source = Some(source.clone());
    assert_frame("graph_node_sourced.bin", &sourced_node);
    assert_frame(
        "graph_edge_sourced.bin",
        &GraphEdge::relate(&alice, "works_at", &acme).with_source(source),
    );

    // The conversation lens, pinned cross-SDK: a source that carries the
    // conversation that asserted the element, and the read filters that narrow a
    // traversal to one conversation.
    let conv_source = SourceRef::Message {
        stream: 7,
        topic: 2,
        partition: 3,
        offset: 4096,
        conversation: Some("7ZZZZZZZZZZZZZZZZZZZZZZZZZ".to_owned()),
    };
    let mut conv_node = GraphNode::entity("Component", "cache");
    conv_node.source = Some(conv_source.clone());
    assert_frame("graph_node_conversation.bin", &conv_node);
    assert_frame(
        "graph_edge_conversation.bin",
        &GraphEdge::relate(&alice, "works_at", &acme).with_source(conv_source),
    );
    assert_frame(
        "graph_query_conversation.bin",
        &GraphQuery {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            start: GraphStart::Match(Filter::pred("label", CmpOp::Eq, "Person")),
            traverse: Vec::new(),
            node_filter: None,
            edge_filter: None,
            return_: GraphReturn::Nodes,
            limit: 100,
            fork: None,
            consistency: Consistency::Eventual,
            as_of: None,
            conversation: Some("7ZZZZZZZZZZZZZZZZZZZZZZZZZ".to_owned()),
        },
    );
    assert_frame(
        "graph_neighbors_conversation.bin",
        &GraphNeighbors {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            node: alice.id,
            dir: EdgeDir::Out,
            edge_type: None,
            depth: 1,
            limit: 50,
            as_of: None,
            conversation: Some("7ZZZZZZZZZZZZZZZZZZZZZZZZZ".to_owned()),
        },
    );
}

#[test]
fn given_agent_workflow_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "agent_submit.bin",
        &AgentSubmit {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: "diagnoser".to_owned(),
            run_id: Some("run-7".to_owned()),
            params: BTreeMap::from([("priority".to_owned(), "high".to_owned())]),
            input: Some(br#"{"incident":"INC-7"}"#.to_vec()),
            budget: None,
        },
    );
    let run = AgentRunInfo {
        run_id: "run-7".to_owned(),
        agent_id: "diagnoser".to_owned(),
        user_id: 42,
        state: AgentRunState::Running,
        created_at_micros: TIMESTAMP_MICROS,
        updated_at_micros: TIMESTAMP_MICROS + 1_000_000,
        detail: None,
        cancel_requested: false,
    };
    assert_frame(
        "agent_reply_status.bin",
        &AgentReply::Ok(AgentOutcome::Status(run.clone())),
    );
    assert_frame(
        "agent_list_page.bin",
        &AgentList {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: Some("diagnoser".to_owned()),
            state: Some(AgentRunState::Running),
            limit: Some(25),
            cursor: Some(vec![0x0a, 0x0b]),
        },
    );
    assert_frame(
        "agent_reply_list_page.bin",
        &AgentReply::Ok(AgentOutcome::List(RunPage {
            runs: vec![AgentRunInfo {
                state: AgentRunState::Failed,
                detail: Some("budget exhausted".to_owned()),
                ..run
            }],
            cursor: Some(vec![0x0c, 0x0d]),
        })),
    );
    assert_frame(
        "agent_reply_error.bin",
        &AgentReply::Err(AgentError::Version {
            expected: AGENT_WORKFLOW_OP_VERSION,
            got: 99,
        }),
    );
}

#[test]
fn given_a_mixed_batch_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "batch_request.bin",
        &BatchRequest {
            v: BATCH_OP_VERSION,
            ops: vec![
                BatchItem {
                    code: laser_wire::codes::AGDX_KV_GET_CODE,
                    payload: b"\xa1av\x01".to_vec(),
                },
                BatchItem {
                    code: laser_wire::codes::AGDX_KV_SET_CODE,
                    payload: b"\xa2av\x01akbven".to_vec(),
                },
            ],
        },
    );
    assert_frame(
        "batch_reply.bin",
        &BatchReply {
            results: vec![b"\xa1bok\xf6".to_vec(), Vec::new()],
        },
    );
}

#[test]
fn given_a_change_record_when_encoded_then_should_match_the_golden_fixture() {
    assert_frame(
        "change_record.bin",
        &ChangeRecord {
            v: CHANGE_OP_VERSION,
            index: "orders_v1".to_owned(),
            partition_id: 3,
            from_offset: 100,
            to_offset: 141,
            rows: 42,
        },
    );
}

#[test]
fn given_client_metadata_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "client_metadata_query.bin",
        &ClientMetadataQuery {
            v: CLIENT_METADATA_OP_VERSION,
            with_metadata_only: true,
            user_id: Some(42),
            after_client_id: Some(100),
            limit: 50,
        },
    );
    assert_frame(
        "client_metadata_list.bin",
        &ClientMetadataList {
            clients: vec![
                ClientMetadata {
                    client_id: 7,
                    user_id: Some(42),
                    transport: 1,
                    address: "127.0.0.1:8090".to_owned(),
                    consumer_groups_count: 2,
                    metadata: Some(br#"{"role":"planner"}"#.to_vec()),
                },
                ClientMetadata {
                    client_id: 9,
                    user_id: None,
                    transport: 2,
                    address: "10.0.0.2:7000".to_owned(),
                    consumer_groups_count: 0,
                    metadata: None,
                },
            ],
            next_cursor: Some(9),
        },
    );
}

#[test]
fn given_a_fold_snapshot_when_encoded_then_should_match_golden_fixture() {
    assert_frame(
        "fold_snapshot.bin",
        &FoldSnapshot {
            conversation: ConversationId::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            as_of: BTreeMap::from([(0, 41), (1, 9)]),
            state: br#"{"folded":true}"#.to_vec(),
        },
    );
}

#[test]
fn given_forwarded_frames_when_encoded_then_should_match_golden_fixtures() {
    // Forwarded managed-request frames: the SDK never sends them, but the
    // server and LaserData Cloud both consume these exact bytes.
    assert_frame(
        "forwarded_query.bin",
        &ForwardedQuery {
            user_id: 7,
            client_id: 42,
            correlation: Some("conv-1".to_owned()),
            query_envelope: vec![1, 2, 3, 4],
            grants: Vec::new(),
        },
    );
    assert_frame(
        "forwarded_command.bin",
        &ForwardedCommand {
            user_id: 7,
            client_id: 42,
            correlation: None,
            operation_id: None,
            read_all: true,
            command_code: AGDX_KV_SET_CODE,
            payload: vec![9, 9, 9],
            grants: Vec::new(),
        },
    );
}

#[test]
fn given_hello_reply_frame_when_encoded_then_should_match_golden_fixture() {
    assert_frame(
        "hello_reply.bin",
        &HelloReply::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        )),
    );
    // The additive agent advertisement: pre-AGDX frames stay byte-identical
    // (agent = 0 is skipped), an advertising server encodes the extra field.
    assert_frame(
        "hello_reply_agent.bin",
        &HelloReply::new(
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_agent(AGENT_OP_VERSION),
        ),
    );
    // The additive capability feature bitset: a server advertising
    // compare-and-swap + read-your-writes encodes the `features` field, pinned
    // so every port reads the same bits.
    assert_frame(
        "hello_reply_features.bin",
        &HelloReply::new(
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_checkpoint(CHECKPOINT_OP_VERSION)
            .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES | feature::DESTINATIONS),
        ),
    );
    // The managed backend announces its served capabilities to the streaming server over
    // their private socket, so the streaming server relays them instead of hardcoding bits.
    // The announce also lists secret-free structured backend descriptors with
    // stable identity, revisions, readiness, query, schema, and maintenance
    // capabilities. The embedded engine serves everything. A second backend
    // advertises a narrower capability set, so a consumer can gate precisely.
    assert_frame(
        "backend_announce.bin",
        &BackendAnnounce::new(
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_checkpoint(CHECKPOINT_OP_VERSION)
            .with_features(feature::KV_CAS | feature::DESTINATIONS),
        )
        .with_backends(vec![
            canonical_backend(10, BackendMode::Operational, "Embedded", "embedded"),
            canonical_backend(
                11,
                BackendMode::Lakehouse,
                "Analytics warehouse",
                "columnar",
            ),
        ]),
    );
    assert_frame(
        "backend_announce_unavailable.bin",
        &BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ))
        .unavailable(),
    );
    // A topology-bearing announce: the deployment's resolved stream/topic
    // names ride the same private-socket hello, so a custom-topology plane
    // and every consumer decode the identical shape.
    assert_frame(
        "backend_announce_topology.bin",
        &BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ))
        .with_topology(WireTopology {
            ops_stream: "acme-ops".to_owned(),
            control_topic: "acme.control".to_owned(),
            dlq_topic: "acme.dlq".to_owned(),
            changes_topic: "acme.changes".to_owned(),
            kv_mutations_topic: "acme.kv.mutations".to_owned(),
            fork_mutations_topic: "acme.fork.mutations".to_owned(),
            run_mutations_topic: "acme.run.mutations".to_owned(),
            graph_mutations_topic: "acme.graph.mutations".to_owned(),
            checkpoint_mutations_topic: "acme.checkpoint.mutations".to_owned(),
        }),
    );
    // The durable managed-mutation log record: the plane replays these on
    // every fold catch-up, the one place a silent serde drift would be
    // unrecoverable, so the canonical bytes are pinned here.
    assert_frame(
        "mutation_command.bin",
        &MutationCommandEnvelope {
            v: KV_OP_VERSION,
            operation_id: 42,
            timestamp_micros: 1_700_000_000_000_000,
            command_code: AGDX_KV_SET_CODE,
            payload: encode_named(&KvSet {
                v: KV_OP_VERSION,
                namespace: "sessions".to_owned(),
                key: vec![0xff, 0x00, b'k'],
                value: b"online".to_vec(),
                expires_at_micros: Some(1_700_000_000_000_000),
            })
            .expect("kv set encodes"),
        },
    );
}

#[test]
fn given_managed_identity_frames_when_encoded_then_should_match_golden_fixtures() {
    assert_frame(
        "managed_request.bin",
        &ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: 42,
            payload: vec![9, 8, 7],
        },
    );
    assert_frame(
        "key_record.bin",
        &KeyRecord {
            v: laser_wire::keys::KEY_RECORD_VERSION,
            principal: "operator-1".to_owned(),
            key_id: vec![3; KEY_ID_BYTES],
            verifying_key: vec![7; VERIFYING_KEY_BYTES],
            kind: KeyKind::Operator,
            valid_from_micros: 100,
            valid_to_micros: Some(200),
            revoked: false,
        },
    );
}

#[test]
fn given_http_json_shapes_when_encoded_then_should_match_golden_fixtures() {
    assert_json("schema_def.json", &canonical_avro_schema());
    // The HTTP browse routes serve the BARE Ok payload (a JSON array), not the
    // binary band's `BrowseReply::Ok(BrowseOutcome::...)` wrapper. The wrapper is
    // a CBOR-socket artifact (one reply enum multiplexes every browse op). An
    // HTTP route is already specific (`GET /agdx/schemas` is unambiguously a
    // schema list), so the tag is dead weight and every sibling route already
    // serves bare. These fixtures pin the bare shape the typed client decodes.
    assert_json(
        "browse_schemas.json",
        &vec![
            SchemaInfo {
                schema: canonical_avro_schema(),
                dropped: false,
            },
            SchemaInfo {
                schema: canonical_protobuf_schema(),
                dropped: true,
            },
            SchemaInfo {
                schema: canonical_json_schema(),
                dropped: false,
            },
        ],
    );
    assert_json(
        "browse_projections.json",
        &vec![ProjectionInfo {
            projection: canonical_projection(),
            bindings: vec![canonical_binding()],
        }],
    );
    assert_json("query_result.json", &canonical_query_result());
    assert_json("fork_info.json", &canonical_fork_info());
    assert_json(
        "capabilities.json",
        // The embedded transactional backend serves CAS and read-your-writes,
        // so it opts both on. Strong consistency stays off (per deployment).
        // The reply also lists the materialization backends the server exposes,
        // mirroring the binary announce, so the HTTP client sees the same set.
        &Capabilities::from_versions(
            true,
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_checkpoint(CHECKPOINT_OP_VERSION)
            .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES | feature::DESTINATIONS),
        )
        .with_query_execution(true, true, true)
        .with_destinations(DestinationCapsView {
            available: true,
            lifecycle: true,
            checkpoint_status: true,
            query_routes: true,
            table_schema: true,
            snapshots: true,
            files: true,
            metrics: true,
            strongest_consistency: CheckpointReadConsistency::Linearizable,
        })
        .with_backends(vec![
            canonical_backend(10, BackendMode::Operational, "Embedded", "embedded"),
            canonical_backend(
                11,
                BackendMode::Lakehouse,
                "Analytics warehouse",
                "columnar",
            ),
        ]),
    );
    assert_json(
        "kv_page_view.json",
        &KvPageView {
            entries: vec![KvEntryView {
                key: "dXNlcjox".to_owned(),
                value: "AAEC".to_owned(),
                expires_at_micros: Some(1_700_000_000_000_000),
                scope: None,
                source: None,
            }],
            cursor: Some("dXNlcjox".to_owned()),
        },
    );
    assert_json(
        "error_body.json",
        // The canonical non-2xx body: a classified code, a human message, and
        // optional structured detail (here the version a CAS write lost to).
        &ErrorBody::new(
            ResultCode::Conflict,
            "key-value version conflict: current version 3",
        )
        .with_detail(serde_json::json!({ "current": 3 })),
    );
    assert_json(
        "destination_page.json",
        &DestinationPageView {
            destinations: vec![DestinationView {
                destination: canonical_destination(),
                status: canonical_checkpoint_status(),
            }],
            next_cursor: Some("cursor-2".to_owned()),
            global_state_revision: 11,
            consistency: CheckpointReadConsistency::Linearizable,
        },
    );
    assert_json(
        "query_route_page.json",
        &QueryRoutePageView {
            routes: vec![canonical_query_route()],
            next_cursor: Some("route-2".to_owned()),
            definition_revision: 8,
            global_state_revision: 11,
            consistency: CheckpointReadConsistency::Linearizable,
        },
    );
    assert_json(
        "table_view.json",
        &TableView {
            table_uuid: UuidValue::new([9; 16]),
            destination_id: DestinationId::from_u128(300),
            destination_generation: 2,
            namespace: vec!["shop".to_owned(), "analytics".to_owned()],
            table: "orders".to_owned(),
            current_snapshot_id: 42,
            current_schema_id: 3,
            current_partition_spec_id: 1,
            metadata_identity: "metadata/v2.json".to_owned(),
            properties: BTreeMap::from([
                ("format-version".to_owned(), "2".to_owned()),
                ("write.format.default".to_owned(), "parquet".to_owned()),
            ]),
        },
    );
    assert_json(
        "table_schema_view.json",
        &TableSchemaView {
            table_uuid: UuidValue::new([9; 16]),
            iceberg_schema_id: 3,
            logical_schema: canonical_logical_schema(),
        },
    );
    assert_json(
        "snapshot_page.json",
        &SnapshotPageView {
            snapshots: vec![TableSnapshotView {
                snapshot_id: 42,
                parent_snapshot_id: Some(41),
                sequence_number: 5,
                committed_at_micros: TIMESTAMP_MICROS,
                schema_id: 3,
                partition_spec_id: 1,
                materialization_boundary_digest: laser_wire::schema::Digest32::new([7; 32]),
                checkpoint_revision: 3,
                summary: BTreeMap::from([
                    ("added-data-files".to_owned(), "1".to_owned()),
                    ("added-records".to_owned(), "100".to_owned()),
                ]),
            }],
            next_before_snapshot_id: Some(41),
        },
    );
    assert_json(
        "table_file_page.json",
        &TableFilePageView {
            files: vec![TableFileView {
                object_identity: "data/0001.parquet".to_owned(),
                file_size_bytes: 4_096,
                row_count: 100,
                partition: BTreeMap::from([("day".to_owned(), TypedValue::Date(20_000))]),
                lower_bounds: BTreeMap::from([(1, TypedValue::Long(1))]),
                upper_bounds: BTreeMap::from([(1, TypedValue::Long(100))]),
                null_value_counts: BTreeMap::from([(1, 0)]),
            }],
            next_cursor: Some("file-2".to_owned()),
        },
    );
    assert_json(
        "table_metrics.json",
        &TableMetricsView {
            snapshot_id: 42,
            data_file_count: 1,
            delete_file_count: 0,
            total_rows: 100,
            total_bytes: 4_096,
            partition_count: 1,
        },
    );
    assert_json(
        "accepted_operation.json",
        &AcceptedOperationView {
            operation_id: DestinationOperationId::from_u128(600),
            request_id: CheckpointRequestId::from_u128(400),
            state: OperationState::Succeeded,
            submitted_at_micros: TIMESTAMP_MICROS,
            completed_at_micros: Some(TIMESTAMP_MICROS + 1_000),
            error: None,
            result: Some(CheckpointMutationResult::Destination {
                request_id: CheckpointRequestId::from_u128(400),
                destination_id: DestinationId::from_u128(300),
                destination_generation: 2,
                global_state_revision: 11,
                definition_revision: 7,
                checkpoint_revision: 3,
                lease: None,
            }),
        },
    );
    assert_json(
        "query_execution.json",
        &QueryExecutionView {
            status: QueryExecutionStatus {
                execution_id: QueryExecutionId::from_u128(500),
                state: QueryExecutionState::Completed,
                started_at_micros: TIMESTAMP_MICROS,
                finished_at_micros: Some(TIMESTAMP_MICROS + 1_000),
                scanned_bytes: 4_096,
                produced_bytes: 256,
                row_count: 2,
                error: None,
            },
            result: Some(canonical_query_result()),
        },
    );
    assert_json(
        "destination_issue.json",
        &DestinationIssueView {
            retention_gap: Some(RetentionGap {
                incarnation: canonical_incarnation(0),
                required_next_offset: 10,
                retained_start: 20,
            }),
            prepared_attempt: None,
            block: None,
            global_state_revision: 11,
            checkpoint_revision: 4,
            consistency: CheckpointReadConsistency::Linearizable,
        },
    );
}

mod agent_fixtures {
    use super::{REGEN_ENV, assert_frame, decode_named, encode_named, fixture_path};
    use laser_wire::agent::{
        AgentCard, AgentDeadLetter, AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId,
        AgentKind, AgentPresence, BodyRef, CapabilityDescriptor, ChannelId, ContentRef,
        ConversationId, CorrelationId, DeadLetterReason, Health, LogPosition, METADATA_RUN,
        OPERATION_CARD, OPERATION_CHAT, OPERATION_REASONING, OPERATION_TASK, RecordId,
        SIGNATURE_SCHEME_ED25519, Signature, TaskState, TokenUsage, validate,
    };
    use laser_wire::content::ContentType;
    use laser_wire::query::Value;
    use std::collections::BTreeMap;

    // DRAFT-grade fixtures by design: a real multi-agent application gets
    // built on this envelope before the corpus hardens, so v1 pins a shape
    // usage has already bent.
    #[test]
    fn given_agent_frames_when_encoded_then_should_match_golden_fixtures() {
        let command = AgentEnvelope::command(
            record(),
            conversation(),
            source(),
            correlation(),
            br#"{"ask":"plan the trip"}"#.to_vec(),
        )
        .with_target(target())
        .with_idempotency_key("order-123-attempt-2".parse().expect("valid key"))
        .with_deadline_micros(1_717_171_777_000_000)
        .with_operation(OPERATION_CHAT)
        .with_metadata("priority", "high");
        validate(&command).expect("canonical command validates");
        assert_frame("agent_command.bin", &command);

        // A signed command: the optional `signature` field present. The corpus
        // keeps a fixed deterministic pattern (the wire crate is crypto-free), the
        // real sign-and-verify round trip is an SDK unit test.
        let signed = command.clone().with_signature(Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            bytes: (0u8..64).collect(),
            context: None,
        });
        validate(&signed).expect("signed command validates");
        assert_frame("agent_command_signed.bin", &signed);

        let response = AgentEnvelope::response(
            record(),
            conversation(),
            source(),
            correlation(),
            br#"{"plan":["fly","drive"]}"#.to_vec(),
        )
        .with_cause(record(), Some(LogPosition::new(1, 2, 3, 41)))
        .with_task_state(TaskState::Completed)
        .with_usage(TokenUsage {
            input_tokens: 1200,
            output_tokens: 256,
            reasoning_output_tokens: Some(64),
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
        });
        validate(&response).expect("canonical response validates");
        assert_frame("agent_response.bin", &response);

        let event = AgentEnvelope::event(
            record(),
            conversation(),
            source(),
            br#"{"observed":"user paid"}"#.to_vec(),
        );
        validate(&event).expect("canonical event validates");
        assert_frame("agent_event.bin", &event);

        // A must-understand marker: an event a receiver must reject unless it
        // understands feature bits 0 and 2. Pins the on-wire shape of the
        // must-understand marker for every port.
        let must_understand = AgentEnvelope::event(
            record(),
            conversation(),
            source(),
            br#"{"feature":"gated"}"#.to_vec(),
        )
        .requiring(0b101);
        validate(&must_understand).expect("must-understand event validates");
        assert_frame("agent_must_understand.bin", &must_understand);

        let chunk = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            7,
            b"tok".to_vec(),
        );
        validate(&chunk).expect("canonical chunk validates");
        assert_frame("agent_chunk.bin", &chunk);

        // The stream-opening chunk: sequence 0 declares the channel's purpose
        // (`operation`, the pinned chunk-stream vocabulary) and the
        // abandonment bound (`deadline_micros`), so multi-channel reassembly
        // is self-describing without decoding bodies.
        let opening = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            0,
            b"thinking".to_vec(),
        )
        .with_operation(OPERATION_REASONING)
        .with_deadline_micros(1_717_171_777_000_000);
        validate(&opening).expect("canonical stream-opening chunk validates");
        assert_frame("agent_chunk_open.bin", &opening);

        let terminal = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            8,
            Vec::new(),
        )
        .terminal("stop")
        .with_usage(TokenUsage {
            input_tokens: 1200,
            output_tokens: 88,
            reasoning_output_tokens: None,
            cache_read_input_tokens: Some(900),
            cache_creation_input_tokens: None,
        });
        validate(&terminal).expect("canonical terminal chunk validates");
        assert_frame("agent_chunk_terminal.bin", &terminal);

        let task = AgentEnvelope::status(record(), conversation(), source(), OPERATION_TASK)
            .with_correlation(correlation())
            .with_task_state(TaskState::Working);
        validate(&task).expect("canonical task update validates");
        assert_frame("agent_status_task.bin", &task);

        // A registered run's status record: identical to the task update above
        // plus the pinned `run` metadata key the run-registry fold selects on.
        let registered = AgentEnvelope::status(record(), conversation(), source(), OPERATION_TASK)
            .with_correlation(correlation())
            .with_task_state(TaskState::Working)
            .with_metadata(METADATA_RUN, "run-7");
        validate(&registered).expect("canonical registered-run status validates");
        assert_frame("agent_status_run_metadata.bin", &registered);

        let card = AgentEnvelope::status(record(), conversation(), source(), OPERATION_CARD);
        validate(&card).expect("canonical card validates");
        assert_frame("agent_status_card.bin", &card);

        let error_body = AgentErrorBody {
            code: AgentErrorCode::ToolFailure,
            message: Some("search timed out".to_owned()),
            retryable: true,
            detail: Some(BTreeMap::from([("attempt".to_owned(), Value::Int(3))])),
        };
        let error_bytes = encode_named(&error_body).expect("error body encodes");
        let error = AgentEnvelope::error(
            record(),
            conversation(),
            source(),
            correlation(),
            error_bytes,
        );
        validate(&error).expect("canonical error validates");
        assert_frame("agent_error.bin", &error);
        assert_frame("agent_error_body.bin", &error_body);

        let poison = encode_named(&AgentEnvelope::command(
            record(),
            conversation(),
            source(),
            correlation(),
            b"poison".to_vec(),
        ))
        .expect("poison encodes");
        let capsule = AgentDeadLetter {
            source: LogPosition::new(1, 2, 3, 99),
            reason: DeadLetterReason::RetryExhausted,
            attempts: 5,
            detail: Some("handler kept failing".to_owned()),
            payload: poison,
        };
        assert_frame("agent_dead_letter.bin", &capsule);

        // The claim-check capsule a `agdx.ct = ref` body carries.
        let body_ref = BodyRef::new("s3://transcripts/conv-2/msg-9", 4_194_304, [7u8; 32]);
        body_ref.validate().expect("canonical body ref validates");
        assert_frame("agent_body_ref.bin", &body_ref);

        // The pinned minimal card body (status, operation = card).
        let agent_card = AgentCard {
            name: Some("trip-planner".to_owned()),
            version: Some("1.4.2".to_owned()),
            capabilities: vec![
                CapabilityDescriptor {
                    skill_id: "chat".to_owned(),
                    input: Some(ContentRef::ContentType(ContentType::Json)),
                    output: Some(ContentRef::ContentType(ContentType::Json)),
                    cost_class: Some(2),
                    latency_class: Some(1),
                    max_concurrency: Some(8),
                    health: Some(Health::Healthy),
                    load: Some(250),
                },
                CapabilityDescriptor {
                    skill_id: "search_flights".to_owned(),
                    input: Some(ContentRef::SchemaId("order.v1".to_owned())),
                    output: None,
                    cost_class: None,
                    latency_class: None,
                    max_concurrency: None,
                    health: Some(Health::Degraded),
                    load: None,
                },
            ],
            ttl_micros: Some(30_000_000),
        };
        agent_card.validate().expect("canonical card validates");
        assert_frame("agent_card.bin", &agent_card);

        // The live presence body an agent advertises in its connection metadata:
        // the link from a connection to its card plus the inbox topic routing
        // resolves to. Pinned so the discovery convention cannot drift.
        let agent_presence = AgentPresence::new(source()).with_inbox("trip-planner.work");
        agent_presence
            .validate()
            .expect("canonical presence validates");
        assert_frame("agent_presence.bin", &agent_presence);

        // The dormant signature capsule: pinned so the wire shape cannot
        // drift before the opt-in activates.
        let signature = Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: vec![0xAB; 8],
            bytes: vec![0xCD; 64],
            context: None,
        };
        signature.validate().expect("canonical signature validates");
        assert_frame("agent_signature.bin", &signature);
    }

    // Negative fixtures: frames that DECODE but violate the validity matrix,
    // so every port's validator rejects identically.
    #[test]
    fn given_invalid_agent_frames_when_validated_then_every_port_should_reject() {
        let mut command = AgentEnvelope::command(
            record(),
            conversation(),
            source(),
            correlation(),
            b"x".to_vec(),
        );
        command.correlation = None;
        assert_invalid("agent_invalid_command_no_correlation.bin", &command);

        let mut response = AgentEnvelope::response(
            record(),
            conversation(),
            source(),
            correlation(),
            b"x".to_vec(),
        );
        response.channel = Some(channel());
        assert_invalid("agent_invalid_response_channel.bin", &response);

        let mut event = AgentEnvelope::event(record(), conversation(), source(), b"x".to_vec());
        event.task_state = Some(TaskState::Working);
        assert_invalid("agent_invalid_event_task_state.bin", &event);

        let mut chunk = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            0,
            b"x".to_vec(),
        );
        chunk.sequence = None;
        assert_invalid("agent_invalid_chunk_no_sequence.bin", &chunk);

        // The abandonment bound rides only the opening chunk (sequence 0).
        let late_deadline = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            5,
            b"x".to_vec(),
        )
        .with_deadline_micros(1_717_171_777_000_000);
        assert_invalid("agent_invalid_chunk_late_deadline.bin", &late_deadline);

        let mut status = AgentEnvelope::status(record(), conversation(), source(), OPERATION_CARD);
        status.operation = None;
        assert_invalid("agent_invalid_status_no_operation.bin", &status);

        // The status discriminator is a closed vocabulary (task|card|progress).
        let off_vocabulary = AgentEnvelope::status(record(), conversation(), source(), "telemetry");
        assert_invalid("agent_invalid_status_bad_operation.bin", &off_vocabulary);

        // The opening chunk must declare its purpose (chat|reasoning|tool_args).
        let undeclared_opening = AgentEnvelope::chunk(
            conversation(),
            source(),
            correlation(),
            channel(),
            0,
            b"x".to_vec(),
        );
        assert_invalid(
            "agent_invalid_chunk_open_no_operation.bin",
            &undeclared_opening,
        );

        let mut error = AgentEnvelope::error(
            record(),
            conversation(),
            source(),
            correlation(),
            b"x".to_vec(),
        );
        error.last = true;
        assert_invalid("agent_invalid_error_last.bin", &error);
    }

    #[test]
    fn given_kind_names_when_displayed_then_should_be_snake_case() {
        assert_eq!(AgentKind::Command.to_string(), "command");
        assert_eq!(AgentKind::Chunk.to_string(), "chunk");
    }

    fn assert_invalid(name: &str, envelope: &AgentEnvelope) {
        assert_frame(name, envelope);
        let golden = std::fs::read(fixture_path(name)).expect("fixture exists");
        let decoded: AgentEnvelope = decode_named(&golden).expect("frame decodes");
        assert!(
            validate(&decoded).is_err(),
            "negative fixture `{name}` must fail validation"
        );
        let _ = REGEN_ENV;
    }

    // Deterministic canonical ids (ULID-shaped values, fixed for the corpus).
    fn record() -> RecordId {
        RecordId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0001)
    }

    fn conversation() -> ConversationId {
        ConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0002)
    }

    fn source() -> AgentId {
        "source-agent".parse().expect("valid agent id")
    }

    fn target() -> AgentId {
        "target-agent".parse().expect("valid agent id")
    }

    fn correlation() -> CorrelationId {
        CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0005)
    }

    fn channel() -> ChannelId {
        ChannelId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0006)
    }
}
