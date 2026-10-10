// Pinned-constants suite: every command code, header key, topic name, op
// version, content-type code, and cap as a LITERAL, so a refactor cannot
// silently renumber the cross-repo contract. These values are byte-identical
// in LaserData Cloud, the Iggy server, and every SDK port.

use laser_wire::agent;
use laser_wire::codes::*;
use laser_wire::content::ContentType;
use laser_wire::headers;
use laser_wire::limits;
use laser_wire::result::ResultCode;
use laser_wire::topics;

#[test]
fn given_managed_command_codes_when_compared_then_should_match_the_dictionary() {
    assert_eq!(AGDX_COMMAND_BASE, 1_000_000);
    assert_eq!(AGDX_HELLO_CODE, 1_000_000);
    assert_eq!(AGDX_BACKEND_HELLO_CODE, 1_000_001);
    assert_eq!(AGDX_SET_CLIENT_METADATA_CODE, 1_000_002);
    assert_eq!(AGDX_GET_CLIENTS_METADATA_CODE, 1_000_003);
    assert_eq!(AGDX_BATCH_CODE, 1_000_020);
    assert_eq!(AGDX_CHECKPOINT_CODE, 1_000_021);
    assert_eq!(AGDX_DESTINATION_GET_CODE, 1_000_022);
    assert_eq!(AGDX_DESTINATION_LIST_CODE, 1_000_023);
    assert_eq!(AGDX_QUERY_ROUTE_LIST_CODE, 1_000_024);
    assert_eq!(AGDX_AUTHZ_BASE, 1_000_100);
    assert_eq!(AGDX_AUTHZ_WHOAMI_CODE, 1_000_100);
    assert_eq!(AGDX_AUTHZ_LIST_ROLES_CODE, 1_000_101);
    assert_eq!(AGDX_AUTHZ_GET_ROLE_CODE, 1_000_102);
    assert_eq!(AGDX_AUTHZ_GET_BINDINGS_CODE, 1_000_103);
    assert_eq!(AGDX_AUTHZ_DEFINE_ROLE_CODE, 1_000_104);
    assert_eq!(AGDX_AUTHZ_DELETE_ROLE_CODE, 1_000_105);
    assert_eq!(AGDX_AUTHZ_BIND_ROLES_CODE, 1_000_106);
    assert_eq!(AGDX_AUTHZ_HISTORY_CODE, 1_000_107);
    assert_eq!(AGDX_CONTROL_PUBLISH_CODE, 1_000_108);
    assert_eq!(AGDX_DESTINATION_HTTP_CODE, 1_000_025);
    assert_eq!(AGDX_QUERY_BASE, 1_000_200);
    assert_eq!(AGDX_QUERY_CODE, 1_000_200);
    assert_eq!(AGDX_QUERY_PAGE_CODE, 1_000_201);
    assert_eq!(AGDX_QUERY_CANCEL_CODE, 1_000_202);
    assert_eq!(AGDX_QUERY_STATUS_CODE, 1_000_203);
    assert_eq!(AGDX_GET_PROJECTION_CODE, 1_000_210);
    assert_eq!(AGDX_LIST_PROJECTIONS_CODE, 1_000_211);
    assert_eq!(AGDX_GET_SCHEMA_CODE, 1_000_220);
    assert_eq!(AGDX_LIST_SCHEMAS_CODE, 1_000_221);
    assert_eq!(AGDX_REGISTER_SCHEMA_CODE, 1_000_222);
    assert_eq!(AGDX_DECODE_RECORD_CODE, 1_000_223);
    assert_eq!(AGDX_KV_BASE, 1_000_300);
    assert_eq!(AGDX_KV_GET_CODE, 1_000_300);
    assert_eq!(AGDX_KV_SET_CODE, 1_000_301);
    assert_eq!(AGDX_KV_SCAN_CODE, 1_000_302);
    assert_eq!(AGDX_KV_DELETE_CODE, 1_000_303);
    assert_eq!(AGDX_KV_DELETE_MANY_CODE, 1_000_304);
    assert_eq!(AGDX_KV_NAMESPACES_CODE, 1_000_305);
    assert_eq!(AGDX_KV_CAS_CODE, 1_000_306);
    assert_eq!(AGDX_KV_EXISTS_CODE, 1_000_307);
    assert_eq!(AGDX_KV_EXPIRE_CODE, 1_000_308);
    assert_eq!(AGDX_KV_PATCH_CODE, 1_000_309);
    assert_eq!(AGDX_KV_LEASE_CODE, 1_000_310);
    assert_eq!(AGDX_KV_RELEASE_CODE, 1_000_311);
    assert_eq!(AGDX_KV_CAS_FENCED_CODE, 1_000_312);
    assert_eq!(AGDX_KV_COPY_CODE, 1_000_313);
    assert_eq!(AGDX_KV_MOVE_CODE, 1_000_314);
    assert_eq!(AGDX_KV_LEASE_RENEW_CODE, 1_000_315);
    assert_eq!(AGDX_FORK_BASE, 1_000_400);
    assert_eq!(AGDX_FORK_CREATE_CODE, 1_000_400);
    assert_eq!(AGDX_FORK_DELETE_CODE, 1_000_401);
    assert_eq!(AGDX_FORK_PROMOTE_CODE, 1_000_402);
    assert_eq!(AGDX_FORK_LIST_CODE, 1_000_403);
    assert_eq!(AGDX_FORK_PUT_CODE, 1_000_404);
    assert_eq!(AGDX_GRAPH_BASE, 1_000_600);
    assert_eq!(AGDX_GRAPH_QUERY_CODE, 1_000_600);
    assert_eq!(AGDX_GRAPH_UPSERT_CODE, 1_000_601);
    assert_eq!(AGDX_GRAPH_NEIGHBORS_CODE, 1_000_602);
    assert_eq!(AGDX_AGENT_BASE, 1_000_700);
    assert_eq!(AGDX_SESSION_GET_CODE, 1_000_710);
    assert_eq!(AGDX_SESSION_LIST_CODE, 1_000_711);
    assert_eq!(AGDX_SESSION_EVENTS_CODE, 1_000_712);
    assert_eq!(AGDX_SESSION_STATE_CODE, 1_000_713);
    assert_eq!(AGDX_SESSION_LINKS_CODE, 1_000_714);
    assert_eq!(AGDX_SESSION_SOURCES_CODE, 1_000_715);
    assert_eq!(AGDX_SESSION_CHANGES_CODE, 1_000_716);
    assert_eq!(AGDX_DELIVERY_BASE, 1_000_800);
    assert_eq!(AGDX_FILTERED_POLL_CODE, 1_000_800);
    assert_eq!(AGDX_FILTERED_ACK_CODE, 1_000_801);
    assert_eq!(AGDX_FILTER_PREVIEW_CODE, 1_000_802);
    assert_eq!(AGDX_FILTER_TEST_CODE, 1_000_803);
    assert_eq!(AGDX_FILTER_VALIDATE_CODE, 1_000_804);
    assert_eq!(AGDX_FILTER_MUTATE_CODE, 1_000_810);
    assert_eq!(AGDX_GET_FILTER_CODE, 1_000_811);
    assert_eq!(AGDX_LIST_FILTERS_CODE, 1_000_812);
    assert_eq!(AGDX_LIST_FILTER_REVISIONS_CODE, 1_000_813);
    assert_eq!(AGDX_GET_FILTER_BINDING_CODE, 1_000_814);
    assert_eq!(AGDX_LIST_FILTER_BINDINGS_CODE, 1_000_815);
    assert_eq!(AGDX_FILTER_OPERATION_CODE, 1_000_816);
    assert_eq!(AGDX_RESOLVE_FILTER_POLICY_CODE, 1_000_817);
    assert_eq!(AGDX_WATCH_FILTER_CATALOG_CODE, 1_000_818);
}

#[test]
fn given_op_versions_when_compared_then_should_match_the_pinned_values() {
    assert_eq!(AUTHZ_OP_VERSION, 1);
    assert_eq!(QUERY_OP_VERSION, 1);
    assert_eq!(CONTROL_OP_VERSION, 1);
    assert_eq!(KV_OP_VERSION, 1);
    assert_eq!(KV_LEASE_OP_VERSION, 1);
    assert_eq!(FORK_OP_VERSION, 1);
    assert_eq!(GRAPH_OP_VERSION, 1);
    assert_eq!(AGENT_OP_VERSION, 1);
    assert_eq!(BATCH_OP_VERSION, 1);
    assert_eq!(CHANGE_OP_VERSION, 1);
    assert_eq!(CLIENT_METADATA_OP_VERSION, 1);
    assert_eq!(CHECKPOINT_OP_VERSION, 1);
    assert_eq!(DESTINATION_HTTP_OP_VERSION, 1);
    assert_eq!(PRESENCE_OP_VERSION, 1);
    assert_eq!(FILTER_OP_VERSION, 1);
    assert_eq!(laser_wire::filter::FILTER_EVALUATOR_VERSION, 1);
}

#[test]
fn given_capability_feature_bits_when_compared_then_should_match_the_dictionary() {
    // The hello-reply feature bitset is a pinned cross-repo / cross-language
    // contract: each managed sub-feature owns one bit, byte-identical in
    // LaserData Cloud and every SDK port.
    use laser_wire::hello::feature;
    assert_eq!(feature::KV_CAS, 1 << 0);
    assert_eq!(feature::READ_YOUR_WRITES, 1 << 1);
    assert_eq!(feature::STRONG_CONSISTENCY, 1 << 2);
    assert_eq!(feature::KV_CAS_FENCED, 1 << 3);
    assert_eq!(feature::AGENT_WORKFLOW, 1 << 4);
    assert_eq!(feature::KEYWORD_SEARCH, 1 << 5);
    assert_eq!(feature::WATCH, 1 << 6);
    assert_eq!(feature::AUTHZ, 1 << 7);
    assert_eq!(feature::DESTINATIONS, 1 << 8);
    assert_eq!(feature::KV_FENCED_LEASES, 1 << 9);
    assert_eq!(feature::CONSUMER_FILTERS, 1 << 10);
    assert_eq!(feature::GROUP_POLICY_READS, 1 << 11);
    assert_eq!(feature::SESSIONS, 1 << 12);
}

#[test]
fn given_header_keys_when_compared_then_should_match_the_dictionary() {
    assert_eq!(headers::CONTENT_TYPE, "agdx.ct");
    assert_eq!(headers::SCHEMA_ID, "agdx.sid");
    assert_eq!(headers::IDX_PREFIX, "agdx.idx.");
    assert_eq!(headers::INLINE_PAYLOAD, "agdx.inline");
    assert_eq!(headers::PROJECTION_REF, "agdx.ref");
    assert_eq!(headers::LOGICAL_SCHEMA_FINGERPRINT, "agdx.sfp");
    assert_eq!(headers::CORRELATION_ID, "agdx.corr");
    assert_eq!(headers::FIELD_MESSAGE_TYPE, "message_type");
    assert_eq!(headers::FIELD_TS, "ts");
    assert_eq!(headers::VECTOR_FIELD, "embedding");
    assert_eq!(headers::WINDOW_START, "window_start");
}

#[test]
fn given_provenance_keys_when_compared_then_should_match_the_dictionary() {
    assert_eq!(headers::CONVERSATION_ID, "gen_ai.conversation.id");
    assert_eq!(headers::AGENT_ID, "gen_ai.agent.id");
    assert_eq!(headers::REQUEST_MODEL, "gen_ai.request.model");
    assert_eq!(headers::RESPONSE_MODEL, "gen_ai.response.model");
    assert_eq!(headers::PROVIDER_NAME, "gen_ai.provider.name");
    assert_eq!(headers::USAGE_INPUT_TOKENS, "gen_ai.usage.input_tokens");
    assert_eq!(headers::USAGE_OUTPUT_TOKENS, "gen_ai.usage.output_tokens");
    assert_eq!(headers::CAUSAL_PARENT, "agdx.cause");
    assert_eq!(headers::PARENT_CONVERSATION_ID, "agdx.parent_conv");
    assert_eq!(headers::ROOT_CONVERSATION_ID, "agdx.root_conv");
    assert_eq!(headers::TARGET_AGENT_ID, "agdx.to");
    assert_eq!(headers::DELEGATED_BY, "agdx.on_behalf_of");
    assert_eq!(headers::IDEMPOTENCY_KEY, "agdx.idem");
    assert_eq!(headers::DEADLINE, "agdx.deadline");
    assert_eq!(headers::COST_USD, "agdx.cost");
    assert_eq!(headers::FENCE, "agdx.fence");
    assert_eq!(headers::AGENT_VERSION, "agdx.av");
    assert_eq!(headers::MEMORY_NAMESPACE, "agdx.mem.ns");
    assert_eq!(headers::MEMORY_USER, "agdx.mem.user");
    assert_eq!(headers::MEMORY_APP, "agdx.mem.app");
}

#[test]
fn given_agent_vocabulary_when_compared_then_should_match_the_dictionary() {
    assert_eq!(agent::OPERATION_TASK, "task");
    assert_eq!(agent::OPERATION_SESSION, "session");
    assert_eq!(agent::OPERATION_CARD, "card");
    assert_eq!(agent::OPERATION_PROGRESS, "progress");
    assert_eq!(agent::OPERATION_QUARANTINE, "quarantine");
    assert_eq!(agent::OPERATION_UNQUARANTINE, "unquarantine");
    assert_eq!(agent::OPERATION_CHAT, "chat");
    assert_eq!(agent::OPERATION_REASONING, "reasoning");
    assert_eq!(agent::OPERATION_TOOL_ARGS, "tool_args");
    assert_eq!(agent::OPERATION_STATE_SNAPSHOT, "state_snapshot");
    assert_eq!(agent::OPERATION_STATE_DELTA, "state_delta");
    assert_eq!(agent::OPERATION_SESSION_PARKED, "session_parked");
    assert_eq!(agent::OPERATION_SESSION_UNPARKED, "session_unparked");
    assert_eq!(agent::METADATA_ROLE, "role");
    assert_eq!(agent::METADATA_BRIDGE_HOPS, "bridge_hops");
    assert_eq!(agent::SIGNATURE_SCHEME_ED25519, 1);
    assert_eq!(agent::SIGNATURE_DOMAIN, b"agdx.signature.v1");
}

#[test]
fn given_topic_names_when_compared_then_should_match_the_dictionary() {
    assert_eq!(topics::OPS_STREAM, "_agdx");
    assert_eq!(topics::CONTROL_TOPIC, "control.commands");
    assert_eq!(topics::DLQ_TOPIC, "dlq");
    assert_eq!(topics::CHANGES_TOPIC, "changes");
    assert_eq!(topics::AGENT_SESSIONS, "agent.sessions");
    assert_eq!(topics::AGENT_STREAMS, "agent.streams");
    assert_eq!(topics::AGENT_HEARTBEATS, "agent.heartbeats");
    assert_eq!(topics::AGENT_CONTROL, "agent.control");
    assert_eq!(topics::AGENT_MEMORY, "agent.memory");
    assert_eq!(topics::AGENT_DLQ, "agent.dlq");
    assert_eq!(topics::AGENT_AUDIT, "agent.audit");
    assert_eq!(topics::AGENT_JOURNAL, "agent.workflow_journal");
    assert_eq!(topics::AGENT_REGISTRY, "agent.registry");
}

#[test]
fn given_limits_when_compared_then_should_match_the_pinned_values() {
    assert_eq!(limits::MAX_FILTER_BYTES, 8192);
    assert_eq!(limits::MAX_FILTER_NODES, 128);
    assert_eq!(limits::MAX_FILTER_DEPTH, 8);
    assert_eq!(limits::MAX_FILTER_PATH_SEGMENTS, 16);
    assert_eq!(limits::MAX_FILTER_PATH_BYTES, 256);
    assert_eq!(limits::MAX_FILTER_LIST_ITEMS, 64);
    assert_eq!(limits::MAX_FILTER_STRING_BYTES, 1024);
    assert_eq!(limits::MAX_FILTER_NAME_BYTES, 128);
    assert_eq!(limits::MAX_FILTER_DESCRIPTION_BYTES, 1024);
    assert_eq!(limits::MAX_FILTER_SOURCE_NAME_BYTES, 255);
    assert_eq!(limits::MAX_FILTERED_PAGE_RECORDS, 1000);
    assert_eq!(limits::MAX_FILTERED_PAGE_BYTES, 8_388_608);
    assert_eq!(limits::MAX_FILTER_PREVIEW_EXAMINED, 10_000);
    assert_eq!(limits::MAX_FILTER_PREVIEW_RECORDS, 100);
    assert_eq!(limits::MAX_FILTER_PREVIEW_PAYLOAD_BYTES, 4096);
    assert_eq!(limits::MAX_FILTER_SAMPLE_BYTES, 1_048_576);
    assert_eq!(limits::MAX_FILTER_SAMPLE_HEADERS, 64);
    assert_eq!(limits::MAX_FILTER_CATALOG_PAGE, 200);
    assert_eq!(limits::MAX_PAGE_SIZE, 1000);
    assert_eq!(limits::DEFAULT_STREAM_PAGE_SIZE, 100);
    assert_eq!(limits::MAX_INDEX_ENTRIES_PER_RECORD, 32);
    assert_eq!(limits::MAX_PROJECTOR_PAYLOAD_BYTES, 8_388_608);
    assert_eq!(limits::MAX_KEY_BYTES, 512);
    assert_eq!(limits::MAX_VALUE_BYTES, 8_388_608);
    assert_eq!(limits::MAX_SCAN_LIMIT, 1000);
    assert_eq!(limits::DEFAULT_SCAN_LIMIT, 100);
    assert_eq!(limits::DEFAULT_NAMESPACE, "default");
    assert_eq!(limits::MAX_NAMESPACE_BYTES, 128);
    assert_eq!(limits::MAX_GRAPH_NAME_BYTES, 128);
    assert_eq!(limits::MAX_FORK_ID_BYTES, 128);
    assert_eq!(limits::MAX_ROLE_NAME_BYTES, 64);
    assert_eq!(limits::MAX_FRAME_BYTES, 67_108_864);
    assert_eq!(limits::MAX_BATCH_OPS, 64);
    assert_eq!(limits::MAX_QUERY_REPLY_BYTES, 67_108_864);
    assert_eq!(headers::HEADER_SOFT_CAP, 1024);
    assert_eq!(headers::HEADER_FRAMING_BYTES, 9);
    assert_eq!(headers::HEADER_VALUE_MAX, 255);
    assert_eq!(limits::MAX_AGENT_STRING_BYTES, 256);
    assert_eq!(limits::MAX_IDEMPOTENCY_KEY_BYTES, 64);
    assert_eq!(limits::MAX_METADATA_ENTRIES, 32);
    assert_eq!(limits::MAX_METADATA_KEY_BYTES, 256);
    assert_eq!(limits::MAX_METADATA_VALUE_BYTES, 1024);
    assert_eq!(limits::MAX_METADATA_TOTAL_BYTES, 8192);
    assert_eq!(limits::MAX_BODY_REFERENCE_BYTES, 1024);
    assert_eq!(limits::MAX_CARD_CAPABILITIES, 64);
    assert_eq!(limits::MAX_CLIENT_METADATA, 64 * 1024);
}

#[test]
fn given_content_type_codes_when_compared_then_should_match_the_dictionary() {
    assert_eq!(ContentType::Raw.code(), 0);
    assert_eq!(ContentType::Json.code(), 1);
    assert_eq!(ContentType::Msgpack.code(), 2);
    assert_eq!(ContentType::Cbor.code(), 3);
    assert_eq!(ContentType::Bson.code(), 4);
    assert_eq!(ContentType::Avro.code(), 5);
    assert_eq!(ContentType::Protobuf.code(), 6);
    assert_eq!(ContentType::Arrow.code(), 7);
    assert_eq!(ContentType::Ref.code(), 8);
    assert_eq!(ContentType::Any.code(), 255);
}

#[test]
fn given_result_codes_when_compared_then_should_match_the_dictionary() {
    // The unified result-code space is a pinned cross-repo / cross-language
    // contract: the numeric value and the HTTP status are byte-identical in
    // LaserData Cloud and every SDK port.
    assert_eq!(ResultCode::Ok.code(), 0);
    assert_eq!(ResultCode::Unsupported.code(), 1);
    assert_eq!(ResultCode::NotFound.code(), 2);
    assert_eq!(ResultCode::InvalidArgument.code(), 3);
    assert_eq!(ResultCode::TooLarge.code(), 4);
    assert_eq!(ResultCode::Conflict.code(), 5);
    assert_eq!(ResultCode::Stale.code(), 6);
    assert_eq!(ResultCode::VersionSkew.code(), 7);
    assert_eq!(ResultCode::Unauthenticated.code(), 8);
    assert_eq!(ResultCode::Backend.code(), 9);
    assert_eq!(ResultCode::Forbidden.code(), 10);
    assert_eq!(ResultCode::StepUpRequired.code(), 11);
    assert_eq!(ResultCode::Unavailable.code(), 12);
    assert_eq!(ResultCode::from_code(2), ResultCode::NotFound);
    assert_eq!(ResultCode::from_code(404), ResultCode::Unrecognized(404));
    assert_eq!(ResultCode::NotFound.http_status(), 404);
    assert_eq!(ResultCode::Conflict.http_status(), 409);
    assert_eq!(ResultCode::Stale.http_status(), 503);
}

#[test]
fn given_http_routes_when_compared_then_should_match_the_router() {
    assert_eq!(laser_wire::http::CAPABILITIES_PATH, "/agdx/capabilities");
    assert_eq!(laser_wire::http::QUERY_PATH, "/agdx/query");
    assert_eq!(laser_wire::http::PROJECTIONS_PATH, "/agdx/projections");
    assert_eq!(laser_wire::http::BINDINGS_PATH, "/agdx/bindings");
    assert_eq!(laser_wire::http::SCHEMAS_PATH, "/agdx/schemas");
    assert_eq!(
        laser_wire::http::GROUP_POLICIES_PATH,
        "/agdx/group-policies"
    );
    assert_eq!(
        laser_wire::http::group_policy_path("fleet", "changes/telemetry", "safety?check"),
        "/agdx/group-policies/fleet/changes%2Ftelemetry/safety%3Fcheck"
    );
    assert_eq!(laser_wire::http::KV_PATH, "/agdx/kv");
    assert_eq!(laser_wire::http::FORKS_PATH, "/agdx/forks");
    assert_eq!(laser_wire::http::CLIENTS_PATH, "/agdx/clients");
    assert_eq!(laser_wire::http::SESSIONS_PATH, "/agdx/sessions");
    let session = laser_wire::agent::ConversationId::from_u128(3);
    assert_eq!(
        laser_wire::http::sessions_path("a/b?!"),
        "/agdx/sessions/a%2Fb%3F%21"
    );
    assert_eq!(
        laser_wire::http::session_path("a/b?!", session),
        "/agdx/sessions/a%2Fb%3F%21/00000000000000000000000003"
    );
    assert_eq!(
        laser_wire::http::session_changes_path("a/b?!"),
        "/agdx/sessions/a%2Fb%3F%21/changes"
    );
    assert_eq!(
        laser_wire::http::session_events_path("a/b?!", session),
        "/agdx/sessions/a%2Fb%3F%21/00000000000000000000000003/events"
    );
    assert_eq!(laser_wire::http::PARAM_STATUS, "status");
    assert_eq!(laser_wire::http::PARAM_AGENT, "agent");
    assert_eq!(laser_wire::http::PARAM_SURFACE, "surface");
    assert_eq!(laser_wire::http::PARAM_AFTER, "after");
    assert_eq!(laser_wire::http::PARAM_TOTAL, "total");
    assert_eq!(laser_wire::http::AUTHZ_WHOAMI_PATH, "/agdx/authz/whoami");
    assert_eq!(laser_wire::http::AUTHZ_ROLES_PATH, "/agdx/authz/roles");
    assert_eq!(
        laser_wire::http::authz_role_path("admin"),
        "/agdx/authz/roles/admin"
    );
    assert_eq!(
        laser_wire::http::authz_user_roles_path(42),
        "/agdx/authz/users/42/roles"
    );
}

#[test]
fn given_session_queries_when_decoded_then_should_take_the_stream_from_the_path() {
    let query: laser_wire::http::SessionsQuery =
        serde_json::from_str(r#"{"status":"active","total":true}"#).expect("session query decodes");
    assert_eq!(query.status.as_deref(), Some("active"));
    assert!(query.want_total);
    let events: laser_wire::http::SessionEventsQuery =
        serde_json::from_str(r#"{"fixed_frontier":true}"#).expect("events query decodes");
    assert!(events.fixed_frontier);
    assert_eq!(events.limit, None);
    let changes: laser_wire::http::SessionChangesQuery =
        serde_json::from_str("{}").expect("changes query decodes");
    assert_eq!(changes.after, 0);
    assert_eq!(laser_wire::http::PARAM_FIXED_FRONTIER, "fixed_frontier");
    assert_eq!(laser_wire::http::PARAM_HISTORY_LIMIT, "history_limit");
    assert_eq!(laser_wire::http::PARAM_TEXT, "text");
    assert_eq!(laser_wire::http::PARAM_ROOT, "root");
    assert_eq!(
        laser_wire::topics::stream_ops_topic("orders", laser_wire::topics::CHANGES_TOPIC),
        "stream:orders/_agdx/changes"
    );
    assert_eq!(laser_wire::hello::feature::STREAM_TENANCY, 1 << 13);
    assert_eq!(
        laser_wire::http::fork_rows_path("stream:acme/try"),
        "/agdx/forks/stream%3Aacme%2Ftry/rows"
    );
    assert_eq!(laser_wire::http::fork_path("exp-1"), "/agdx/forks/exp-1");
    assert_eq!(laser_wire::http::PARAM_LABEL_PREFIX, "label_prefix");
    assert_eq!(
        laser_wire::authz::stream_resource("agents"),
        "stream:agents"
    );
}
