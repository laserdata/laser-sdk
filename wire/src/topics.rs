// Ops stream + topics. The control surface rides a dedicated `_agdx` ops
// stream, separate from the customer data stream. Topic names drop the `agdx.`
// prefix because the `_agdx` stream already namespaces them. A pinned wire
// contract: drift breaks the managed control surface silently. `_agdx/dlq`
// collects dead-letter capsules, each JSON capsule tagged with a `kind`
// discriminator.

/// The ops stream name (`_agdx`).
pub const OPS_STREAM: &str = "_agdx";
/// Ops topic: projection control commands.
pub const CONTROL_TOPIC: &str = "control.commands";
/// Ops topic: the universal dead-letter queue (capsules carry a `kind`).
pub const DLQ_TOPIC: &str = "dlq";
