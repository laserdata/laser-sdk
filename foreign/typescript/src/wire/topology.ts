import { type CborMap, field } from "./cbor.js"
import { CHANGES_TOPIC, CONTROL_TOPIC, DLQ_TOPIC, OPS_STREAM } from "./topics.js"

export const DEFAULT_KV_MUTATIONS_TOPIC = "kv.mutations"
export const DEFAULT_FORK_MUTATIONS_TOPIC = "fork.mutations"
export const DEFAULT_RUN_MUTATIONS_TOPIC = "run.mutations"
export const DEFAULT_GRAPH_MUTATIONS_TOPIC = "graph.mutations"

export interface WireTopology {
  readonly opsStream: string
  readonly controlTopic: string
  readonly dlqTopic: string
  readonly changesTopic: string
  readonly kvMutationsTopic: string
  readonly forkMutationsTopic: string
  readonly runMutationsTopic: string
  readonly graphMutationsTopic: string
}

export function defaultWireTopology(): WireTopology {
  return {
    opsStream: OPS_STREAM,
    controlTopic: CONTROL_TOPIC,
    dlqTopic: DLQ_TOPIC,
    changesTopic: CHANGES_TOPIC,
    kvMutationsTopic: DEFAULT_KV_MUTATIONS_TOPIC,
    forkMutationsTopic: DEFAULT_FORK_MUTATIONS_TOPIC,
    runMutationsTopic: DEFAULT_RUN_MUTATIONS_TOPIC,
    graphMutationsTopic: DEFAULT_GRAPH_MUTATIONS_TOPIC
  }
}

type PartialWireTopology = { readonly [K in keyof WireTopology]?: string | undefined }

export function wireTopologyFromPartial(partial: PartialWireTopology): WireTopology {
  const defaults = defaultWireTopology()
  return {
    opsStream: partial.opsStream ?? defaults.opsStream,
    controlTopic: partial.controlTopic ?? defaults.controlTopic,
    dlqTopic: partial.dlqTopic ?? defaults.dlqTopic,
    changesTopic: partial.changesTopic ?? defaults.changesTopic,
    kvMutationsTopic: partial.kvMutationsTopic ?? defaults.kvMutationsTopic,
    forkMutationsTopic: partial.forkMutationsTopic ?? defaults.forkMutationsTopic,
    runMutationsTopic: partial.runMutationsTopic ?? defaults.runMutationsTopic,
    graphMutationsTopic: partial.graphMutationsTopic ?? defaults.graphMutationsTopic
  }
}

export function encodeWireTopology(topology: WireTopology): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("ops_stream", topology.opsStream)
  map.set("control_topic", topology.controlTopic)
  map.set("dlq_topic", topology.dlqTopic)
  map.set("changes_topic", topology.changesTopic)
  map.set("kv_mutations_topic", topology.kvMutationsTopic)
  map.set("fork_mutations_topic", topology.forkMutationsTopic)
  map.set("run_mutations_topic", topology.runMutationsTopic)
  map.set("graph_mutations_topic", topology.graphMutationsTopic)
  return map
}

export function decodeWireTopology(map: CborMap, context: string): WireTopology {
  return wireTopologyFromPartial({
    opsStream: field.optionalString(map, "ops_stream", context),
    controlTopic: field.optionalString(map, "control_topic", context),
    dlqTopic: field.optionalString(map, "dlq_topic", context),
    changesTopic: field.optionalString(map, "changes_topic", context),
    kvMutationsTopic: field.optionalString(map, "kv_mutations_topic", context),
    forkMutationsTopic: field.optionalString(map, "fork_mutations_topic", context),
    runMutationsTopic: field.optionalString(map, "run_mutations_topic", context),
    graphMutationsTopic: field.optionalString(map, "graph_mutations_topic", context)
  })
}
