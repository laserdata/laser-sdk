// Golden wire-fixture assertions. The corpus lives in this crate (the one
// typed source of truth). This suite re-encodes canonical values and asserts
// byte-for-byte equality, so an encoding change (a serde attribute edit, an
// rmp-serde upgrade, a field reorder) fails here instead of shipping a silent
// wire break. Consumer repos assert against the same bytes through the
// `fixtures` feature instead of copying files.

use laser_wire::browse::{
    BrowseOutcome, BrowseReply, DecodeRecord, ProjectionInfo, RegisterSchema, SchemaInfo,
};
use laser_wire::codes::{
    AGDX_KV_SET_CODE, AGENT_OP_VERSION, CONTROL_OP_VERSION, FORK_OP_VERSION, KV_OP_VERSION,
    QUERY_OP_VERSION,
};
use laser_wire::content::ContentType;
use laser_wire::control::{
    ControlCommand, ControlEnvelope, Delivery, FieldType, IndexField, IndexSchema, Projection,
    ProjectionBinding, ProjectionId, ProjectionKind, RetentionPolicy, SchemaDef, SchemaSource,
    SourceSelector, Target, TargetRole,
};
use laser_wire::fork::{
    ForkCreate, ForkInfo, ForkKind, ForkOutcome, ForkPut, ForkReply, ForkStatus,
};
use laser_wire::forward::{ForwardedCommand, ForwardedQuery};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::hello::{BackendAnnounce, BackendDescriptor, HelloReply, OpVersions, feature};
use laser_wire::http::{Capabilities, ErrorBody, KvEntryView, KvPageView};
use laser_wire::kv::{
    CasExpect, KvCas, KvEntry, KvError, KvNamespaceInfo, KvNamespaces, KvOutcome, KvPage, KvReply,
    KvScan, KvSet,
};
use laser_wire::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Page, Query,
    QueryEnvelope, QueryError, QueryReply, QueryResult, RawSql, Row, Select, Sort, Value,
    VectorQuery, Window,
};
use laser_wire::result::ResultCode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::path::PathBuf;

const REGEN_ENV: &str = "AGDX_WIRE_FIXTURES_REGEN";
const TIMESTAMP_MICROS: u64 = 1_717_171_717_000_000;

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
        targets: vec![
            Target {
                backend: "embedded".to_owned(),
                table: "orders_rows".to_owned(),
                role: TargetRole::ReadWrite,
                delivery: Delivery::EffectivelyOnce,
                required: true,
            },
            Target {
                backend: "warehouse".to_owned(),
                table: "orders_mirror".to_owned(),
                role: TargetRole::WriteOnly,
                delivery: Delivery::AtMostOnce,
                required: false,
            },
        ],
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

fn canonical_query() -> Query {
    Query {
        index: "orders".to_owned(),
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
        order: vec![Sort {
            field: "ts".to_owned(),
            dir: Dir::Desc,
        }],
        limit: 20,
        offset: 40,
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
    }
}

fn canonical_aggregate_query() -> Query {
    Query {
        index: "metrics".to_owned(),
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
        limit: 100,
        ..Default::default()
    }
}

fn canonical_raw_sql_query() -> Query {
    Query {
        index: "orders".to_owned(),
        limit: 10,
        raw_sql: Some(RawSql {
            sql: "SELECT customer, amount FROM orders_rows WHERE amount > ? LIMIT ?".to_owned(),
            params: vec![
                Value::Int(100),
                Value::Uint(u64::MAX),
                Value::Float(0.5),
                Value::Bool(true),
                Value::Str("x".to_owned()),
                Value::Null,
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            ],
        }),
        ..Default::default()
    }
}

fn canonical_query_result() -> QueryResult {
    QueryResult {
        rows: vec![Row {
            headers: BTreeMap::from([
                ("amount".to_owned(), "42".to_owned()),
                ("customer".to_owned(), "alice".to_owned()),
            ]),
            metadata: BTreeMap::from([("agdx.ct".to_owned(), "1".to_owned())]),
            partition: Some(2),
            offset: Some(17),
            payload: Some(b"{\"total\":42}".to_vec()),
            score: Some(0.5),
        }],
        page: Page {
            offset: 0,
            limit: 50,
            total: 1,
            has_more: false,
        },
    }
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
        &QueryReply::Ok(canonical_query_result()),
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
            index: "orders".to_owned(),
            consistency: Consistency::ReadYourWrites,
            // Match the builder's `limit` default (50) so this fixture's bytes
            // are unchanged from before `builders` was feature-gated.
            limit: 50,
            ..Default::default()
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
fn given_forwarded_frames_when_encoded_then_should_match_golden_fixtures() {
    // Forwarded managed-request frames - the SDK never sends them, but the
    // server and LaserData Cloud both consume these exact bytes.
    assert_frame(
        "forwarded_query.bin",
        &ForwardedQuery {
            user_id: 7,
            client_id: 42,
            correlation: Some("conv-1".to_owned()),
            query_envelope: vec![1, 2, 3, 4],
        },
    );
    assert_frame(
        "forwarded_command.bin",
        &ForwardedCommand {
            user_id: 7,
            client_id: 42,
            correlation: None,
            read_all: true,
            command_code: AGDX_KV_SET_CODE,
            payload: vec![9, 9, 9],
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
            .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES),
        ),
    );
    // The managed backend announces its served capabilities to the streaming server over
    // their private socket, so the streaming server relays them instead of hardcoding bits.
    // The announce also lists the materialization backends the server exposes:
    // each an identity-only descriptor (stable id + opaque engine kind) with an
    // optional advisory label/version and a set of opaque capability tags the
    // backend declares about itself. The embedded engine serves everything; a
    // second backend advertises a narrower tag set, so a consumer can gate.
    assert_frame(
        "backend_announce.bin",
        &BackendAnnounce::new(
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_features(feature::KV_CAS),
        )
        .with_backends(vec![
            BackendDescriptor::new("embedded", "embedded").with_capabilities([
                "ingest",
                "query",
                "vector_search",
            ]),
            BackendDescriptor::new("warehouse", "columnar")
                .with_label("Analytics warehouse")
                .with_version("2.1.0")
                .with_capabilities(["ingest", "query", "percentile"]),
        ]),
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
        &Capabilities::new(true, OpVersions::new(1, 1, 1, 1))
            .with_kv_cas(true)
            .with_query_consistency(laser_wire::query::Consistency::ReadYourWrites)
            .with_backends(vec![
                BackendDescriptor::new("embedded", "embedded").with_capabilities([
                    "ingest",
                    "query",
                    "vector_search",
                ]),
                BackendDescriptor::new("warehouse", "columnar")
                    .with_label("Analytics warehouse")
                    .with_version("2.1.0")
                    .with_capabilities(["ingest", "query", "percentile"]),
            ]),
    );
    assert_json(
        "kv_page_view.json",
        &KvPageView {
            entries: vec![KvEntryView {
                key: "dXNlcjox".to_owned(),
                value: "AAEC".to_owned(),
                expires_at_micros: Some(1_700_000_000_000_000),
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
}

mod agent_fixtures {
    use super::{REGEN_ENV, assert_frame, decode_named, encode_named, fixture_path};
    use laser_wire::agent::{
        AgentCard, AgentDeadLetter, AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId,
        AgentKind, BodyRef, ChannelId, ConversationId, CorrelationId, DeadLetterReason,
        LogPosition, OPERATION_CARD, OPERATION_CHAT, OPERATION_REASONING, OPERATION_TASK, RecordId,
        SIGNATURE_SCHEME_ED25519, Signature, TaskState, TokenUsage, validate,
    };
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
            capabilities: vec!["chat".to_owned(), "search_flights".to_owned()],
            ttl_micros: Some(30_000_000),
        };
        agent_card.validate().expect("canonical card validates");
        assert_frame("agent_card.bin", &agent_card);

        // The dormant signature capsule: pinned so the wire shape cannot
        // drift before the opt-in activates.
        let signature = Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: vec![0xAB; 8],
            bytes: vec![0xCD; 64],
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
