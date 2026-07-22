use crate::topics::{CHANGES_TOPIC, CONTROL_TOPIC, DLQ_TOPIC, OPS_STREAM};
use serde::{Deserialize, Serialize};

/// Default names for the log-first managed mutation topics. Declared now so
/// `WireTopology`'s shape is frozen ahead of the code that uses them.
pub const DEFAULT_KV_MUTATIONS_TOPIC: &str = "kv.mutations";
pub const DEFAULT_FORK_MUTATIONS_TOPIC: &str = "fork.mutations";
pub const DEFAULT_RUN_MUTATIONS_TOPIC: &str = "run.mutations";
pub const DEFAULT_GRAPH_MUTATIONS_TOPIC: &str = "graph.mutations";

fn default_ops_stream() -> String {
    OPS_STREAM.to_owned()
}

fn default_control_topic() -> String {
    CONTROL_TOPIC.to_owned()
}

fn default_dlq_topic() -> String {
    DLQ_TOPIC.to_owned()
}

fn default_changes_topic() -> String {
    CHANGES_TOPIC.to_owned()
}

fn default_kv_mutations_topic() -> String {
    DEFAULT_KV_MUTATIONS_TOPIC.to_owned()
}

fn default_fork_mutations_topic() -> String {
    DEFAULT_FORK_MUTATIONS_TOPIC.to_owned()
}

fn default_run_mutations_topic() -> String {
    DEFAULT_RUN_MUTATIONS_TOPIC.to_owned()
}

fn default_graph_mutations_topic() -> String {
    DEFAULT_GRAPH_MUTATIONS_TOPIC.to_owned()
}

/// The stream/topic names a deployment's plane and streaming server use,
/// provisioned or discovered rather than hardcoded. `Default` returns today's
/// constants from [`crate::topics`], so a deployment that never configures
/// this sees exactly the pre-topology names, the one place those names live.
/// Every field has its own `#[serde(default = ...)]` function (not the
/// derived per-type default, which would be an empty string), so a partial
/// announcement from an older peer missing a newer field still decodes to
/// that field's real default name, not an empty one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTopology {
    #[serde(default = "default_ops_stream")]
    pub ops_stream: String,
    #[serde(default = "default_control_topic")]
    pub control_topic: String,
    #[serde(default = "default_dlq_topic")]
    pub dlq_topic: String,
    #[serde(default = "default_changes_topic")]
    pub changes_topic: String,
    /// Log-first KV mutation topic.
    #[serde(default = "default_kv_mutations_topic")]
    pub kv_mutations_topic: String,
    /// Log-first fork mutation topic.
    #[serde(default = "default_fork_mutations_topic")]
    pub fork_mutations_topic: String,
    /// Log-first run mutation topic.
    #[serde(default = "default_run_mutations_topic")]
    pub run_mutations_topic: String,
    /// Log-first graph mutation topic.
    #[serde(default = "default_graph_mutations_topic")]
    pub graph_mutations_topic: String,
}

impl Default for WireTopology {
    fn default() -> Self {
        Self {
            ops_stream: default_ops_stream(),
            control_topic: default_control_topic(),
            dlq_topic: default_dlq_topic(),
            changes_topic: default_changes_topic(),
            kv_mutations_topic: default_kv_mutations_topic(),
            fork_mutations_topic: default_fork_mutations_topic(),
            run_mutations_topic: default_run_mutations_topic(),
            graph_mutations_topic: default_graph_mutations_topic(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_config_when_defaulting_then_should_match_todays_topic_constants() {
        let topology = WireTopology::default();
        assert_eq!(topology.ops_stream, OPS_STREAM);
        assert_eq!(topology.control_topic, CONTROL_TOPIC);
        assert_eq!(topology.dlq_topic, DLQ_TOPIC);
        assert_eq!(topology.changes_topic, CHANGES_TOPIC);
        assert_eq!(topology.kv_mutations_topic, DEFAULT_KV_MUTATIONS_TOPIC);
        assert_eq!(topology.fork_mutations_topic, DEFAULT_FORK_MUTATIONS_TOPIC);
        assert_eq!(topology.run_mutations_topic, DEFAULT_RUN_MUTATIONS_TOPIC);
        assert_eq!(
            topology.graph_mutations_topic,
            DEFAULT_GRAPH_MUTATIONS_TOPIC
        );
    }

    #[test]
    fn given_a_partial_json_object_when_decoded_then_missing_fields_should_default_to_real_names() {
        let partial = serde_json::json!({ "ops_stream": "custom-ops" });
        let topology: WireTopology = serde_json::from_value(partial).expect("decodes");
        assert_eq!(topology.ops_stream, "custom-ops");
        // Every field missing from the partial object defaults to the real
        // topic name, not an empty string.
        assert_eq!(topology.control_topic, CONTROL_TOPIC);
        assert_eq!(topology.dlq_topic, DLQ_TOPIC);
        assert_eq!(topology.changes_topic, CHANGES_TOPIC);
        assert_eq!(topology.kv_mutations_topic, DEFAULT_KV_MUTATIONS_TOPIC);
        assert_eq!(
            topology.graph_mutations_topic,
            DEFAULT_GRAPH_MUTATIONS_TOPIC
        );
    }
}
