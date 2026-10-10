#![cfg(feature = "cbor")]

// The shared display and dispatch table. TypeScript asserts the same file, so
// both ports agree on how a session timeline shows a record and what a
// reliable consumer does with it.

use laser_wire::agent::{
    AgentEnvelope, AgentId, ConversationId, CorrelationId, METADATA_ROLE, RecordId, SdkInfo,
    SessionEnd, SessionStart, SessionTransition, TaskState,
};
use laser_wire::dispatch::{
    HandledOperations, SessionRecord, addressee_filter, broadcast_filter, classify,
    classify_generic, display_type,
};
use laser_wire::framing::encode_named;
use laser_wire::memory::MemoryRecord;
use laser_wire::query::Value;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::PathBuf;

const REGEN_ENV: &str = "AGDX_WIRE_FIXTURES_REGEN";

#[test]
fn given_the_shared_table_when_records_are_classified_then_should_match_every_row() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/dispatch_cases.json");
    let mut table: Json =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read table")).expect("json");
    let me: AgentId = table["agent"]
        .as_str()
        .expect("agent")
        .parse()
        .expect("agent id");
    for case in table["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().expect("name");
        let topic = case["topic"].as_str().expect("topic");
        let record = &case["record"];
        if let Some(kind) = record["kind"].as_str() {
            let envelope = envelope(kind, record);
            assert_eq!(
                display_type(SessionRecord::Envelope(&envelope), topic).as_str(),
                case["display"],
                "{name}"
            );
            let operations: Vec<&str> = case["handled"]
                .as_array()
                .map(|list| list.iter().filter_map(Json::as_str).collect())
                .unwrap_or_default();
            let handled = if case["handled"] == "any" {
                HandledOperations::Any
            } else {
                HandledOperations::Only(&operations)
            };
            assert_eq!(
                classify(&envelope, topic, &me, handled).as_str(),
                case["dispatch"],
                "{name}"
            );
        } else {
            let memory = |target: &str| match target {
                "item" => MemoryRecord::Item {
                    id: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
                    kind: "fact".to_owned(),
                    body: Vec::new(),
                    origin: None,
                    producer: None,
                },
                "forget" => MemoryRecord::Forget {
                    target: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
                    conversation: None,
                },
                _ => MemoryRecord::Feedback {
                    target: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
                    weight: 1.0,
                    conversation: None,
                },
            };
            let held;
            let session_record = match (record["memory"].as_str(), record["other"].as_str()) {
                (Some(variant), _) => {
                    held = memory(variant);
                    SessionRecord::Memory(&held)
                }
                (_, Some("dead_letter")) => SessionRecord::DeadLetter,
                (_, Some("journal")) => SessionRecord::Journal,
                (_, Some("kv_mutation")) => SessionRecord::KvMutation,
                (_, Some("graph_mutation")) => SessionRecord::GraphMutation,
                _ => SessionRecord::Undecodable(None),
            };
            assert_eq!(
                display_type(session_record, topic).as_str(),
                case["display"],
                "{name}"
            );
        }
    }
    for case in table["generic"].as_array().expect("generic cases") {
        assert_eq!(
            classify_generic(
                case["to"].as_str(),
                case["cause"] == true,
                case["correlation"] == true,
                case["topic"].as_str().expect("topic"),
                &me,
            )
            .as_str(),
            case["dispatch"],
            "{}",
            case["name"]
        );
    }
    let regen = std::env::var(REGEN_ENV).is_ok();
    for filter in table["filters"].as_array_mut().expect("filters") {
        let built = match filter["agent"].as_str() {
            Some(agent) => addressee_filter(&agent.parse().expect("agent id")),
            None => broadcast_filter(),
        };
        let digest = hex(built.digest().as_bytes());
        if regen {
            filter["digest"] = Json::String(digest);
        } else {
            assert_eq!(filter["digest"], digest, "{filter}");
        }
    }
    if regen {
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&table).expect("table encodes") + "\n",
        )
        .expect("write table");
    }
}

fn envelope(kind: &str, record: &Json) -> AgentEnvelope {
    let conversation = ConversationId::from_u128(2);
    let source: AgentId = "planner".parse().expect("agent id");
    let record_id = RecordId::from_u128(3);
    let correlation = CorrelationId::from_u128(4);
    let body = match record["body"].as_str() {
        Some("start") => encode_named(&SessionStart {
            label: None,
            namespace: None,
            agent: source.clone(),
            sdk: SdkInfo {
                language: "rust".to_owned(),
                version: "0.7.0".to_owned(),
            },
            parent: None,
            root: None,
            idle_timeout_micros: None,
            budget: None,
            tags: Vec::new(),
        })
        .expect("start encodes"),
        Some("transition") => {
            encode_named(&SessionTransition::default()).expect("transition encodes")
        }
        Some("end") => encode_named(&SessionEnd::default()).expect("end encodes"),
        _ => b"{}".to_vec(),
    };
    let operation = record["operation"].as_str().unwrap_or_default();
    let mut envelope = match kind {
        "command" => AgentEnvelope::command(record_id, conversation, source, correlation, body),
        "response" => AgentEnvelope::response(record_id, conversation, source, correlation, body),
        "event" => AgentEnvelope::event(record_id, conversation, source, body),
        "error" => AgentEnvelope::error(record_id, conversation, source, correlation, body),
        "chunk" => AgentEnvelope::chunk(
            conversation,
            source,
            correlation,
            laser_wire::agent::ChannelId::from_u128(5),
            0,
            body,
        ),
        _ => {
            let mut status = AgentEnvelope::status(record_id, conversation, source, operation);
            status.body = body;
            status
        }
    };
    envelope.operation = Some(operation.to_owned());
    envelope.task_state = record["task_state"]
        .as_str()
        .map(|state| state.parse::<TaskState>().expect("task state"));
    envelope.target = record["target"]
        .as_str()
        .map(|target| target.parse().expect("target"));
    if let Some(role) = record["role"].as_str() {
        envelope.metadata = Some(BTreeMap::from([(
            METADATA_ROLE.to_owned(),
            Value::Str(role.to_owned()),
        )]));
    }
    envelope
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn given_the_shared_estimates_when_computed_then_should_match_every_row() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/token_estimates.json");
    let cases: Json =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read")).expect("json");
    for case in cases.as_array().expect("cases") {
        let bytes = case["bytes"].as_u64().expect("bytes") as usize;
        assert_eq!(
            laser_wire::agent::estimate_tokens(bytes),
            case["tokens"].as_u64().expect("tokens"),
            "{bytes} bytes"
        );
    }
}
