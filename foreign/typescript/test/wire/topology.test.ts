import assert from "node:assert/strict"
import { test } from "node:test"
import { CHANGES_TOPIC, CONTROL_TOPIC, DLQ_TOPIC, OPS_STREAM } from "../../src/wire/topics.js"
import {
  DEFAULT_FORK_MUTATIONS_TOPIC,
  DEFAULT_GRAPH_MUTATIONS_TOPIC,
  DEFAULT_KV_MUTATIONS_TOPIC,
  DEFAULT_RUN_MUTATIONS_TOPIC,
  defaultWireTopology,
  wireTopologyFromPartial
} from "../../src/wire/topology.js"

void test("given_no_config_when_defaulting_then_should_match_todays_topic_constants", () => {
  const topology = defaultWireTopology()
  assert.equal(topology.opsStream, OPS_STREAM)
  assert.equal(topology.controlTopic, CONTROL_TOPIC)
  assert.equal(topology.dlqTopic, DLQ_TOPIC)
  assert.equal(topology.changesTopic, CHANGES_TOPIC)
  assert.equal(topology.kvMutationsTopic, DEFAULT_KV_MUTATIONS_TOPIC)
  assert.equal(topology.forkMutationsTopic, DEFAULT_FORK_MUTATIONS_TOPIC)
  assert.equal(topology.runMutationsTopic, DEFAULT_RUN_MUTATIONS_TOPIC)
  assert.equal(topology.graphMutationsTopic, DEFAULT_GRAPH_MUTATIONS_TOPIC)
})

void test("given_a_partial_object_when_defaulted_then_missing_fields_should_use_real_names", () => {
  const topology = wireTopologyFromPartial({ opsStream: "custom-ops" })
  assert.equal(topology.opsStream, "custom-ops")
  assert.equal(topology.controlTopic, CONTROL_TOPIC)
  assert.equal(topology.dlqTopic, DLQ_TOPIC)
  assert.equal(topology.changesTopic, CHANGES_TOPIC)
  assert.equal(topology.kvMutationsTopic, DEFAULT_KV_MUTATIONS_TOPIC)
  assert.equal(topology.graphMutationsTopic, DEFAULT_GRAPH_MUTATIONS_TOPIC)
})
