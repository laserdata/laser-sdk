// The golden corpus, embedded so a consumer (LaserData Cloud, the Iggy server, a port's
// conformance suite) can assert against the exact canonical bytes in its own
// CI without copying files.

macro_rules! corpus {
    ($($name:literal),+ $(,)?) => {
        /// Every fixture in the corpus as `(file name, canonical bytes)`.
        pub const ALL: &[(&str, &[u8])] = &[
            $(($name, include_bytes!(concat!("../fixtures/", $name)))),+
        ];
    };
}

corpus!(
    // The agent surface (DRAFT-grade until a real multi-agent application
    // has bent the envelope - the `agent_invalid_*` negatives must fail
    // `validate()` identically in every port).
    "agent_body_ref.bin",
    "agent_card.bin",
    "agent_chunk.bin",
    "agent_chunk_open.bin",
    "agent_chunk_terminal.bin",
    "agent_command.bin",
    "agent_dead_letter.bin",
    "agent_error.bin",
    "agent_error_body.bin",
    "agent_event.bin",
    "agent_invalid_chunk_late_deadline.bin",
    "agent_invalid_chunk_no_sequence.bin",
    "agent_invalid_chunk_open_no_operation.bin",
    "agent_invalid_command_no_correlation.bin",
    "agent_invalid_error_last.bin",
    "agent_invalid_event_task_state.bin",
    "agent_invalid_response_channel.bin",
    "agent_invalid_status_bad_operation.bin",
    "agent_invalid_status_no_operation.bin",
    "agent_must_understand.bin",
    "agent_record.bin",
    "agent_response.bin",
    "agent_signature.bin",
    "agent_status_card.bin",
    "agent_status_task.bin",
    "backend_announce.bin",
    "browse_projections.json",
    "browse_reply_decoded.bin",
    "browse_reply_projections.bin",
    "browse_reply_schema_registered.bin",
    "browse_reply_schemas.bin",
    "browse_schemas.json",
    "capabilities.json",
    "control_apply_binding.bin",
    "control_drop_schema.bin",
    "control_register_projection.bin",
    "control_register_schema_avro.bin",
    "control_register_schema_json.bin",
    "control_register_schema_protobuf.bin",
    "control_remove_binding.bin",
    "decode_record.bin",
    "error_body.json",
    "fork_create.bin",
    "fork_info.json",
    "fork_put.bin",
    "fork_reply_created.bin",
    "forwarded_command.bin",
    "forwarded_query.bin",
    "graph_edge.bin",
    "graph_neighbors.bin",
    "graph_node.bin",
    "graph_query.bin",
    "graph_reply.bin",
    "graph_upsert.bin",
    "hello_reply.bin",
    "hello_reply_agent.bin",
    "hello_reply_features.bin",
    "kv_cas.bin",
    "kv_namespaces.bin",
    "kv_page_view.json",
    "kv_reply_committed.bin",
    "kv_reply_namespaces.bin",
    "kv_reply_page.bin",
    "kv_reply_version_conflict.bin",
    "kv_scan.bin",
    "kv_set.bin",
    "query_envelope.bin",
    "query_envelope_aggregate.bin",
    "query_envelope_raw_sql.bin",
    "query_envelope_read_your_writes.bin",
    "query_reply_err_stale.bin",
    "query_reply_err_too_large.bin",
    "query_reply_ok.bin",
    "query_result.json",
    "register_schema_managed.bin",
    "schema_def.json",
);

/// Canonical bytes of one fixture by name, or `None` if the name is unknown.
pub fn bytes(name: &str) -> Option<&'static [u8]> {
    ALL.iter()
        .find(|(fixture, _)| *fixture == name)
        .map(|(_, bytes)| *bytes)
}

/// Assert that `encoded` matches the canonical fixture byte-for-byte.
/// Panics with a diff-friendly message on drift.
pub fn assert_matches(name: &str, encoded: &[u8]) {
    let golden = bytes(name).unwrap_or_else(|| panic!("unknown fixture `{name}`"));
    assert_eq!(
        encoded, golden,
        "fixture `{name}` drifted from the canonical frame"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn given_the_corpus_when_looked_up_then_should_resolve_known_names() {
        assert!(super::bytes("hello_reply.bin").is_some());
        assert!(super::bytes("nope.bin").is_none());
    }
}
