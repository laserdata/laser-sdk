mod agdx;
mod agent;
mod agent_runtime;
mod async_bridge;
mod batching;
mod blob;
mod chunks;
mod client;
mod codecs;
mod consumer_group;
mod context;
mod control;
mod convert;
mod coordination;
mod crash_context;
mod destinations;
mod errors;
mod filters;
mod fork;
mod govern;
mod graph;
mod ids;
mod intent;
mod interop;
mod kv;
mod memory;
mod memory_handler;
mod parity_helpers;
mod projections;
mod publish;
mod query;
mod rbac;
mod reader;
mod registry;
mod runs;
mod schema;
mod session;
mod sign;
mod snapshot;
mod state_store;
mod stream;
mod swarm;
mod transport;
mod typed;
mod watch;
mod workflow;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;

pub use errors::exception_stub;

#[pymodule]
fn laser_sdk(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bridge Rust `log` records to Python's `logging`, so the runtime's
    // diagnostics land on the `laser_sdk` logger a host app already configures
    // instead of on stderr. Best effort: a second import must not fail the module.
    let _ = pyo3_log::try_init();
    errors::register(py, module)?;
    client::register_constants(module)?;
    module.add_class::<client::PyLaser>()?;
    module.add_class::<client::PyCapabilities>()?;
    module.add_class::<ids::PyMessageId>()?;
    module.add_class::<client::PyQueryCaps>()?;
    module.add_class::<client::PyDestinationCaps>()?;
    module.add_class::<client::PyKvCaps>()?;
    module.add_class::<client::PyFilterCaps>()?;
    module.add_class::<client::PyFilterAnnounce>()?;
    module.add_class::<client::PyOpVersions>()?;
    module.add_class::<client::PyBackendDescriptor>()?;
    module.add_function(wrap_pyfunction!(
        client::backend_readiness_not_ready,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(client::backend_readiness_ready, module)?)?;
    module.add_class::<publish::PyPublish>()?;
    module.add_class::<publish::PyBatchPublish>()?;
    module.add_class::<schema::PyCompiledSchema>()?;
    module.add_class::<schema::PyLogicalSchema>()?;
    module.add_function(wrap_pyfunction!(schema::typed_value_as_i64, module)?)?;
    module.add_function(wrap_pyfunction!(schema::typed_value_as_u64, module)?)?;
    module.add_function(wrap_pyfunction!(schema::typed_value_as_str, module)?)?;
    module.add_function(wrap_pyfunction!(
        schema::typed_value_diagnostic_text,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        schema::typed_value_validate_canonical,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        schema::typed_value_validate_against,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(schema::logical_type_kind, module)?)?;
    module.add_function(wrap_pyfunction!(
        schema::logical_type_accepts_map_key,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        schema::decimal_value_validate_canonical,
        module
    )?)?;
    module.add_class::<sign::PySigningKey>()?;
    module.add_class::<sign::PyKeyRegistry>()?;
    module.add_class::<sign::PyKeyRecord>()?;
    module.add_class::<sign::PyKvKeyRegistry>()?;
    module.add_class::<sign::PyVerifiedPrincipal>()?;
    module.add_class::<query::PyQuery>()?;
    module.add_class::<query::PyRow>()?;
    module.add_class::<query::PyQueryResult>()?;
    module.add_class::<query::PyPage>()?;
    module.add_class::<query::PyQueryBuilder>()?;
    module.add_function(wrap_pyfunction!(query::query_new, module)?)?;
    module.add_function(wrap_pyfunction!(query::query_operational, module)?)?;
    module.add_function(wrap_pyfunction!(query::query_target_operational, module)?)?;
    module.add_function(wrap_pyfunction!(query::key_match_new, module)?)?;
    module.add_function(wrap_pyfunction!(query::result_code_code, module)?)?;
    module.add_function(wrap_pyfunction!(query::result_code_from_code, module)?)?;
    module.add_function(wrap_pyfunction!(query::result_code_http_status, module)?)?;
    module.add_function(wrap_pyfunction!(query::result_code_is_retryable, module)?)?;
    module.add_class::<query::PyQueryFilter>()?;
    module.add_class::<query::PyQueryRows>()?;
    module.add_class::<query::PyTypedQueryRows>()?;
    module.add_class::<memory::PyConsolidationReport>()?;
    module.add_class::<context::PyLastN>()?;
    module.add_class::<context::PyTokenBudget>()?;
    module.add_class::<context::PyRoleFilter>()?;
    module.add_class::<context::PyChain>()?;
    module.add_class::<destinations::PyDestinations>()?;
    module.add_class::<projections::PyProjections>()?;
    module.add_class::<projections::PyBindings>()?;
    module.add_class::<projections::PySchemas>()?;
    module.add_class::<kv::PyKv>()?;
    module.add_class::<kv::PyLease>()?;
    module.add_class::<coordination::PyDedicatedKvTransport>()?;
    module.add_class::<coordination::PyFencedLeaseClient>()?;
    module.add_class::<coordination::PyPreparedMutation>()?;
    module.add_class::<coordination::PyAmbiguousMutationRecovery>()?;
    module.add_class::<kv::PyMutationPosition>()?;
    module.add_class::<kv::PyKvEntry>()?;
    module.add_class::<kv::PyKvMetadata>()?;
    module.add_class::<kv::PyKvPage>()?;
    module.add_class::<stream::PyStream>()?;
    module.add_class::<stream::PyTopic>()?;
    module.add_class::<watch::PyWatchReader>()?;
    module.add_class::<watch::PyChangeRecord>()?;
    module.add_class::<kv::PyKvSet>()?;
    module.add_class::<kv::PyKvCopy>()?;
    module.add_class::<kv::PyKvCasFenced>()?;
    module.add_class::<kv::PyKvScan>()?;
    module.add_class::<kv::PyKvDeleteMany>()?;
    module.add_class::<consumer_group::PyConsumerGroup>()?;
    module.add_class::<consumer_group::PyConsumerGroupInfo>()?;
    module.add_class::<consumer_group::PyGroupFilter>()?;
    module.add_class::<filters::PyFilterExpr>()?;
    module.add_class::<filters::PyFieldPath>()?;
    module.add_class::<filters::PyConsumerFilter>()?;
    module.add_class::<filters::PyCompiledFilter>()?;
    module.add_class::<filters::PyFilteredReader>()?;
    module.add_class::<filters::PyMatchedPage>()?;
    module.add_class::<filters::PyMatchedRecord>()?;
    module.add_function(wrap_pyfunction!(
        filters::execution_mode_is_filtered,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(filters::fault_reason_is_foreign, module)?)?;
    module.add_function(wrap_pyfunction!(filters::read_mode_is_primary, module)?)?;
    module.add_function(wrap_pyfunction!(filters::record_policy_is_reject, module)?)?;
    module.add_function(wrap_pyfunction!(filters::filter_error_reason_code, module)?)?;
    module.add_function(wrap_pyfunction!(
        filters::timestamp_format_micros_from_integer,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        filters::timestamp_format_micros_from_text,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(filters::text_predicate_validate, module)?)?;
    module.add_function(wrap_pyfunction!(filters::applied_policy_filtered, module)?)?;
    module.add_function(wrap_pyfunction!(
        filters::applied_policy_unfiltered,
        module
    )?)?;
    module.add_class::<fork::PyForkHandle>()?;
    module.add_class::<fork::PyForkPut>()?;
    module.add_class::<agent::PyProvenance>()?;
    module.add_class::<agent::PyAgentMessage>()?;
    module.add_class::<agent::PyLlmUsage>()?;
    module.add_class::<agent::PyAgentTopic>()?;
    module.add_class::<agdx::PyAgdx>()?;
    module.add_class::<agdx::PyAgdxStream>()?;
    module.add_class::<agdx::PyTaskState>()?;
    module.add_function(wrap_pyfunction!(agdx::command_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::response_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::error_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::event_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::chunk_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::status_envelope, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::unmet_requirements, module)?)?;
    module.add_function(wrap_pyfunction!(agdx::validate_signature, module)?)?;
    module.add_class::<agent_runtime::PyAgentCtx>()?;
    module.add_class::<agent_runtime::PyAgentHandle>()?;
    module.add_class::<agent_runtime::PyContract>()?;
    module.add_class::<agent_runtime::PyScatterOutcome>()?;
    module.add_class::<agent_runtime::PyScatterReport>()?;
    module.add_class::<agent_runtime::PyGather>()?;
    module.add_class::<registry::PyConsumerRef>()?;
    module.add_class::<registry::PyRegisteredCard>()?;
    module.add_class::<registry::PyClientMetadataPage>()?;
    module.add_class::<agent_runtime::PyRouteCandidate>()?;
    module.add_class::<agent_runtime::PyConversationState>()?;
    module.add_class::<agdx::PyLogPosition>()?;
    module.add_class::<registry::PyConsumptionStatus>()?;
    module.add_class::<workflow::PyWorkflowOutcome>()?;
    module.add_class::<workflow::PyWorkflow>()?;
    module.add_class::<runs::PyRuns>()?;
    module.add_class::<runs::PyAgentRunInfo>()?;
    module.add_class::<runs::PyRunPage>()?;
    module.add_class::<runs::PyRunBudget>()?;
    module.add_class::<rbac::PyGrant>()?;
    module.add_class::<rbac::PyRole>()?;
    module.add_class::<rbac::PyResourcePattern>()?;
    module.add_class::<rbac::PyWhoamiReply>()?;
    module.add_class::<rbac::PyAuthzEvent>()?;
    module.add_class::<rbac::PyAuthzHistoryReply>()?;
    module.add_class::<rbac::PyEdgeDenial>()?;
    module.add_class::<reader::PyCursor>()?;
    module.add_class::<typed::PyTypedTopic>()?;
    module.add_class::<typed::PyTypedRecords>()?;
    module.add_class::<typed::PyTypedRecord>()?;
    module.add_class::<reader::PyMessage>()?;
    module.add_class::<transport::PyProducer>()?;
    module.add_class::<transport::PySendMessagesConfirmation>()?;
    module.add_class::<transport::PySendMessagesResponse>()?;
    module.add_class::<transport::PyConsumer>()?;
    module.add_class::<transport::PyConsumerMessage>()?;
    module.add_class::<transport::PyStoredOffset>()?;
    module.add_class::<transport::PyBackgroundConfig>()?;
    module.add_class::<chunks::PyChunkAssembler>()?;
    module.add_class::<snapshot::PySnapshotStore>()?;
    module.add_class::<snapshot::PyKvSnapshotStore>()?;
    module.add_class::<snapshot::PyTopicSnapshotStore>()?;
    module.add_class::<parity_helpers::PyClock>()?;
    module.add_class::<parity_helpers::PySystemClock>()?;
    module.add_class::<parity_helpers::PyTestClock>()?;
    module.add_class::<govern::PyGovernedAction>()?;
    module.add_class::<govern::PyActionDecision>()?;
    module.add_class::<govern::PyPolicyEvidence>()?;
    module.add_function(wrap_pyfunction!(govern::verify_evidence_chain, module)?)?;
    module.add_class::<govern::PyQuorumPolicy>()?;
    module.add_class::<govern::PyQuorumGovernor>()?;
    module.add_class::<govern::PySwappableGovernor>()?;
    module.add_class::<govern::PyActionCounters>()?;
    module.add_class::<govern::PyVerdict>()?;
    module.add_class::<govern::PyPolicyRef>()?;
    module.add_class::<govern::PyGovernorRetention>()?;
    module.add_class::<swarm::PyAgentActivity>()?;
    module.add_class::<swarm::PySwarmActivity>()?;
    module.add_class::<crash_context::PyCrashContext>()?;
    module.add_class::<intent::PyIntentPolicy>()?;
    module.add_class::<intent::PyIntent>()?;
    module.add_class::<intent::PyVote>()?;
    module.add_class::<intent::PyDecision>()?;
    module.add_class::<context::PyContextScope>()?;
    module.add_class::<context::PyScopedMemory>()?;
    module.add_class::<context::PyContextMessage>()?;
    module.add_function(wrap_pyfunction!(context::context_checkpoint, module)?)?;
    module.add_class::<session::PySessions>()?;
    module.add_class::<session::PySessionConfig>()?;
    module.add_class::<session::PySession>()?;
    module.add_class::<session::PySessionTurn>()?;
    module.add_class::<session::PyCheckpoint>()?;
    module.add_function(wrap_pyfunction!(
        session::session_policy_conversation_for,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(registry::inbox_route_resolve, module)?)?;
    module.add_function(wrap_pyfunction!(registry::agent_presence, module)?)?;
    module.add_function(wrap_pyfunction!(registry::validate_agent_presence, module)?)?;
    module.add_class::<memory::PyMemory>()?;
    module.add_class::<memory_handler::PyMemoryHandler>()?;
    module.add_class::<memory::PyMemoryItem>()?;
    module.add_class::<memory::PyLogMemory>()?;
    module.add_class::<memory::PyVectorMemory>()?;
    module.add_class::<memory::PyRerankedMemory>()?;
    module.add_class::<memory::PyRecallSignal>()?;
    module.add_function(wrap_pyfunction!(memory::to_context_block, module)?)?;
    module.add_function(wrap_pyfunction!(memory::memory_id_content, module)?)?;
    module.add_function(wrap_pyfunction!(memory::memory_kind_class, module)?)?;
    module.add_function(wrap_pyfunction!(memory::memory_kind_code, module)?)?;
    module.add_class::<graph::PyGraph>()?;
    module.add_class::<state_store::PyStateStore>()?;
    module.add_class::<state_store::PyInMemoryStore>()?;
    module.add_class::<state_store::PyFileStore>()?;
    module.add_class::<interop::PyA2aBridge>()?;
    module.add_class::<interop::PyMcpBridge>()?;
    module.add_class::<registry::PyAgentRegistry>()?;
    module.add_class::<batching::PyBatchingProducer>()?;
    module.add_class::<registry::PyAgentScope>()?;
    module.add_class::<codecs::PyJson>()?;
    module.add_class::<codecs::PyMsgpack>()?;
    module.add_class::<codecs::PyCbor>()?;
    module.add_class::<codecs::PyBson>()?;
    module.add_function(wrap_pyfunction!(codecs::content_type_code, module)?)?;
    module.add_function(wrap_pyfunction!(codecs::content_type_is_raw, module)?)?;
    module.add_function(wrap_pyfunction!(codecs::consistency_is_eventual, module)?)?;
    module.add_class::<control::PyIndexSchemaBuilder>()?;
    module.add_class::<control::PyProjectionBuilder>()?;
    module.add_class::<control::PyProjectionBindingBuilder>()?;
    module.add_function(wrap_pyfunction!(control::index_field_new, module)?)?;
    module.add_function(wrap_pyfunction!(control::index_field_typed, module)?)?;
    module.add_function(wrap_pyfunction!(control::source_selector_new, module)?)?;
    module.add_function(wrap_pyfunction!(control::projection_kind_code, module)?)?;
    module.add_function(wrap_pyfunction!(control::projection_kind_is_row, module)?)?;
    module.add_function(wrap_pyfunction!(control::schema_def_content_type, module)?)?;
    module.add_function(wrap_pyfunction!(agent::new_conversation_id, module)?)?;
    module.add_function(wrap_pyfunction!(ids::mint_ulid, module)?)?;
    module.add_function(wrap_pyfunction!(agent::derive_conversation_id, module)?)?;
    module.add_function(wrap_pyfunction!(rbac::authorize_edge, module)?)?;
    module.add_function(wrap_pyfunction!(interop::enter_bridge, module)?)?;
    module.add_function(wrap_pyfunction!(rbac::grants_allow, module)?)?;
    module.add_function(wrap_pyfunction!(rbac::delegated_allow, module)?)?;
    module.add_function(wrap_pyfunction!(rbac::validate_role_name, module)?)?;
    module.add_function(wrap_pyfunction!(graph::node_id_content, module)?)?;
    module.add_function(wrap_pyfunction!(graph::edge_id_content, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_node_entity, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_edge_relate, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_edge_with_source, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_edge_valid, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_edge_valid_at, module)?)?;
    module.add_function(wrap_pyfunction!(graph::edge_dir_is_out, module)?)?;
    module.add_function(wrap_pyfunction!(graph::graph_return_is_nodes, module)?)?;
    module.add_function(wrap_pyfunction!(runs::agent_run_state_is_terminal, module)?)?;
    module.add_function(wrap_pyfunction!(
        snapshot::fold_snapshot_resume_offset,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        destinations::new_checkpoint_request_envelope,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        destinations::public_checkpoint_mutation_required_capability,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(destinations::new_source_scope, module)?)?;
    module.add_function(wrap_pyfunction!(
        destinations::validate_supervisor_actor_assertion,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(intent::decide, module)?)?;
    module.add_function(wrap_pyfunction!(agent_runtime::agent_message, module)?)?;
    module.add_function(wrap_pyfunction!(agent_runtime::agent_ctx, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::sign_card_value, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::verify_card, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::verify_delegation, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::encode_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::decode_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::resume_offsets, module)?)?;
    module.add_function(wrap_pyfunction!(
        parity_helpers::fuse_reciprocal_rank,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parity_helpers::command_from_message_send,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parity_helpers::task_from_envelope,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parity_helpers::tool_call_from_request,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parity_helpers::tool_result_from_envelope,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::check_in, module)?)?;
    module.add_function(wrap_pyfunction!(parity_helpers::resolve_body, module)?)?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
