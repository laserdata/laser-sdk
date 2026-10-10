#!/usr/bin/env python3
"""Rust, Python, and TypeScript public-surface parity check.

Reads the public Rust surface of `sdk/src` (own items and every item it
re-exports by name from `wire/src`), the Python stub (`foreign/python/laser_sdk.pyi`),
and the TypeScript API reports (`foreign/typescript/api/*.api.md`), renders
`docs/parity.md`, and fails when the rendered matrix differs from the
committed one, when a row is MISSING, or when a peer API has no Rust row.

    python3 scripts/check-parity.py           # check
    python3 scripts/check-parity.py --write   # regenerate docs/parity.md
"""

import ast
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "parity.md"
STUB = ("foreign", "python", "laser_sdk.pyi")
REPORTS = ("laser-sdk.api.md", "laser-sdk-full.api.md", "laser-sdk-testing.api.md", "laser-sdk-opentelemetry.api.md")
EXTERN_CRATES = ("iggy", "std", "core", "alloc", "serde", "serde_json", "bytes", "tokio", "futures", "axum")

MISSING = "MISSING"
TYPE_KINDS = ("struct", "enum", "union", "trait", "type")

# The only accepted reasons for a peer to differ from the Rust spelling. Each
# one is a construct the peer language lacks or spells natively. A reason that
# allows omission (`True`) lets a cell read `omitted`. Every other cell must
# resolve to a real Python or TypeScript symbol, so a renamed or reshaped API
# is a gap.
REASONS = {
    "native-binding": ("Python exposes a native Rust value through its binding name and delegates the operation to that same Rust type", False),
    "keywords": ("Python takes keyword arguments and TypeScript an options object or builder where Rust fills a struct or chains a builder", False),
    "one-call": ("The peer configures and runs the operation in one call where Rust finishes a builder", False),
    "constructor": ("A Rust constructor function or struct literal is the peer class constructor", False),
    "protocol": ("The peer spells it through a language protocol: iteration, length, or truthiness", False),
    "overload": ("Rust has no optional parameters or overloads, so it spells a variant of a call as a second method. The peer takes an optional or alternative parameter", False),
    "free-function": ("The peer type is a plain value, object literal, or union, so its methods live in functions", False),
    "async-runtime": ("Rust passes tokio channels or futures where the peer uses its own async primitives", False),
    "callback": ("The peer passes a callable or duck-typed object where Rust implements a trait", False),
    "trait-variant": ("Rust spells one trait as Send, local, and boxed variants. The peer has one interface", False),
    "error-function": ("The peer exposes the Rust error classification method as a function over its exception classes", False),
    "error-class": ("The peer raises one error class per LaserError variant, carrying the payload fields", False),
    "property": ("The peer reads a property where Rust calls a getter", False),
    "plain-value": ("The peer uses a plain string, number, or literal union where Rust wraps a newtype or word enum", True),
    "shared-ownership": ("Rust wraps a value to share it across tasks. Peer references are already shared", True),
    "name-collision": ("The peer keeps fields and methods in one namespace, so a method that shares a field's name is renamed", False),
    "lazy-init": ("The peer builds a client synchronously and connects in an explicit `init()` where Rust awaits the builder", False),
    "telemetry": ("Each client reports through its language's telemetry seam: Rust tracing spans, Python logging, a TypeScript observer", False),
    "flat-namespace": ("The peer exports one flat namespace, so a Rust module function carries its module name", False),
    "rust-crate": ("The item exposes a Rust-only crate (the Apache Iggy SDK, axum, tokio) that has no binding outside Rust", True),
    "converted-error": ("A Rust error type the SDK converts into a LaserError variant before any caller sees it. The peer raises that variant's error class", True),
    "native-value": ("Python passes the native scalar, None, or list that serde reads and writes for this untagged Rust enum", True),
    "std-trait": ("The peer spells a Rust standard trait impl (FromStr, Display, From) as a function or static member", False),
    "serde-dict": ("Python passes the record as a dict that serde builds from the same Rust type, keyed by its serde field names", False),
}

# A serde derive on the Rust type, which fixes the dict keys a `serde-dict` peer carries.
SERDE_DERIVE = re.compile(r"derive\s*\(.*\b(?:serde::)?(?:Serialize|Deserialize)\b")
SERDE_SKIP = re.compile(r"serde\s*\(.*\bskip(?:_serializing|_deserializing)?\b(?!_)")

# Rust conversions a plain-value peer needs no spelling for.
CONVERSIONS = re.compile(r"^(new|get|parse|as_\w+|to_\w+|into_\w+|from_\w+|try_from_\w+)$")

# Python and TypeScript members that spell a Rust method through a language protocol.
PROTOCOL = {
    "len": (("__len__",), ("length", "size")),
    "is_empty": (("__len__", "__bool__"), ("length", "size")),
    "next": (("__anext__", "__next__"), ("[Symbol.asyncIterator]", "[Symbol.iterator]")),
    "stream": (("__aiter__", "__iter__"), ("[Symbol.asyncIterator]",)),
    "iter": (("__iter__",), ("[Symbol.iterator]",)),
}
PY_RESERVED = {"from", "in", "is", "as", "def", "class", "import", "global", "lambda", "pass", "with", "yield", "async", "await", "type"}
UNITS = ("ms", "secs", "micros", "millis")
# Field types of a Rust newtype that a peer spells as a plain value.
PLAIN_FIELD = re.compile(r"^(?:pub\s+)?(?:String|Box<str>|Arc<str>|Cow<'static,\s*str>|[ui](?:8|16|32|64|128|size)|f32|f64|bool|Ulid|Uuid|\[u8;\s*\d+\]|Vec<u8>|NonZero\w+)$")

# Peer parameters that carry at run time what a Rust generic type parameter
# carries at compile time: a codec, a decoder, or a Python class to decode into.
TYPE_STANDINS = {"codec", "decoder", "codecOrDecoder", "decodeValue", "cls"}
# Methods that finish a hand-written Rust builder. A peer that takes the
# builder as keywords runs the operation in the same call.
FINISHERS = {"build", "send", "fetch", "call", "commit", "run", "connect"}

# TypeScript members that implement a language protocol and so need no Rust
# row of their own. Every Python dunder is a protocol member too.
# Equality, parsing, ordering, and serialization members implement what Rust
# gets from PartialEq, FromStr, Ord, and serde derives.
TS_PROTOCOL = {"[Symbol.asyncIterator]", "[Symbol.iterator]", "[Symbol.asyncDispose]", "[Symbol.dispose]", "toString", "toJSON", "fromJSON", "valueOf", "equals", "parse", "tryParse", "compare"}
PY_PROTOCOL = {"to_dict", "from_dict", "to_json", "from_json"}

# Per-row peers, keyed by (Rust owner, member). A member of None is the type
# row itself. Each value is (spelling, reason). The spelling grammar:
#   `Class` or `Class.member`         a class, type, or member
#   `Class.member(kw=, ...)`          a member that takes these keyword or option names
#   `fn:name(kw=)`                    a module function
#   `new:Class(kw=)`                  a public constructor of Class
#   `const:NAME`                      a module constant
#   `-`                               omitted, only for a reason that allows it
#   `~spelling`                       where the capability lives today under another
#                                     name or shape. Still a gap.
# A type row spelled `Class.member`, `fn:name`, or `new:Class` makes the
# members of that Rust type keywords of that call. A type row spelled `Class`
# or `~Class` makes `Class` host the members.
PY_PEERS = {
    ("SessionLayout", "per_agent_topic"): ("SessionLayout.PerAgentTopic", "constructor"),
    ("SessionLayout", "per_agent_partition"): ("SessionLayout.PerAgentPartition", "constructor"),
    ("LaserError", "ConsumerGroupSetup.name"): ("ConsumerGroupSetupError.group_name", "name-collision"),
    ("Agent", None): ("Laser.spawn_agent", "keywords"),
    ("Agent", "builder"): ("Laser.spawn_agent", "keywords"),
    ("Agent", "spawn"): ("Laser.spawn_agent", "one-call"),
    ("AgentBuilder", None): ("Laser.spawn_agent", "keywords"),
    ("AgentBuilder", "id"): ("Laser.spawn_agent(agent_id=)", "keywords"),
    ("AgentBuilder", "inbox_route"): ("Laser.spawn_agent(fixed_inbox=)", "keywords"),
    ("AgentBuilder", "concurrency"): ("Laser.spawn_agent(max_partitions=)", "keywords"),
    ("AgentBuilder", "deduplicator"): ("Laser.spawn_agent(dedup=)", "keywords"),
    ("AgentBuilder", "on_dead_letter"): ("Laser.spawn_agent(dead_letter=)", "keywords"),
    ("AgentBuilder", "retry"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "keywords"),
    ("AgentHandler", None): ("Laser.spawn_agent(handler=)", "callback"),
    ("LocalAgentHandler", None): ("Laser.spawn_agent(handler=)", "callback"),
    ("AgentMiddleware", None): ("Laser.spawn_agent(middleware=)", "callback"),
    ("DeadLetterSink", None): ("Laser.spawn_agent(dead_letter=)", "callback"),
    ("Deduplicator", None): ("Laser.spawn_agent(dedup=)", "callback"),
    ("Consolidator", None): ("Laser.spawn_agent(consolidator=)", "callback"),
    ("LocalConsolidator", None): ("Laser.spawn_agent(consolidator=)", "callback"),
    ("ContextPolicy", None): ("Laser.assemble_context(policy=)", "callback"),
    ("RouteScorer", None): ("Laser.contract(policy=)", "callback"),
    ("StepFn", None): ("Workflow.step(build=)", "callback"),
    ("Verifier", None): ("Workflow.step(verify=)", "callback"),
    ("BlobStore", None): ("PublishRequest.claim_check(store=)", "callback"),
    ("Embedder", None): ("MemoryHandle.vector(embedder=)", "callback"),
    ("LocalEmbedder", None): ("MemoryHandle.vector(embedder=)", "callback"),
    ("Reranker", None): ("MemoryHandle.reranker(reranker=)", "callback"),
    ("LocalReranker", None): ("MemoryHandle.reranker(reranker=)", "callback"),
    ("Summarizer", None): ("MemoryHandle.consolidate(summarizer=)", "callback"),
    ("LocalSummarizer", None): ("MemoryHandle.consolidate(summarizer=)", "callback"),
    ("ActionGovernor", None): ("Laser.with_governor(governor=)", "callback"),
    ("ManagedKvTransport", None): ("new:FencedLeaseClient(transport=)", "callback"),
    ("LocalManagedKvTransport", None): ("new:FencedLeaseClient(transport=)", "callback"),
    ("DynManagedKvTransport", None): ("new:FencedLeaseClient(transport=)", "callback"),
    ("DynMemory", None): ("Laser.memory_custom(backend=)", "callback"),
    ("AgdxSend", None): ("Agdx.command", "keywords"),
    ("AgdxSend", "body"): ("Agdx.status(body=)", "keywords"),
    ("AgdxSend", "last"): ("Agdx.status(last=)", "keywords"),
    ("AgdxSend", "with_correlation"): ("Agdx.status(correlation=)", "keywords"),
    ("AgdxSend", "with_task_state"): ("Agdx.status(task_state=)", "keywords"),
    ("AgdxSend", "send"): ("Agdx.command", "one-call"),
    ("AgdxSend", "signed_by"): ("Laser.agdx(signing_key=)", "keywords"),
    ("WorkflowBudget", "tokens"): ("WorkflowBudget.tokens", "constructor"),
    ("SessionListRequest", None): ("Sessions.list", "keywords"),
    ("SessionListRequest", "fetch"): ("Sessions.list", "one-call"),
    ("SessionEventsRequest", None): ("Sessions.events", "keywords"),
    ("SessionEventsRequest", "fetch"): ("Sessions.events", "one-call"),
    ("SessionInfo", None): ("dict:Sessions.get", "serde-dict"),
    ("SessionFlags", None): ("dict:Sessions.get", "serde-dict"),
    ("SessionPage", None): ("dict:Sessions.list", "serde-dict"),
    ("SessionEventsPage", None): ("dict:Sessions.events", "serde-dict"),
    ("SessionEvent", None): ("dict:Sessions.events", "serde-dict"),
    ("PayloadRange", None): ("dict:Sessions.events", "serde-dict"),
    ("SourceGap", None): ("dict:Sessions.events", "serde-dict"),
    ("SessionLinksView", None): ("dict:Sessions.links", "serde-dict"),
    ("SessionLink", None): ("dict:Sessions.links", "serde-dict"),
    ("ContractBuilder", None): ("Laser.contract", "keywords"),
    ("ContractBuilder", "inbox_route"): ("Laser.contract(fixed_inbox=)", "keywords"),
    ("ContractBuilder", "from"): ("Laser.contract(source=)", "keywords"),
    ("ContractBuilder", "send"): ("Laser.contract", "one-call"),
    ("ContractBuilder", "parent"): ("Laser.contract(parent=, root=)", "keywords"),
    ("ClientMetadataRequest", None): ("Laser.client_metadata", "keywords"),
    ("ClientMetadataRequest", "page"): ("Laser.client_metadata", "one-call"),
    ("ClientMetadataRequest", "with_metadata_only"): ("Laser.client_metadata(metadata_only=)", "keywords"),
    ("LaserBuilder", None): ("Laser.connect", "keywords"),
    ("LaserBuilder", "client"): ("-", "rust-crate"),
    ("Laser", "client"): ("-", "rust-crate"),
    ("Laser", "from_client"): ("-", "rust-crate"),
    ("ProducerBuilder", None): ("Topic.producer", "keywords"),
    ("ConsumerBuilder", None): ("Topic.consumer", "keywords"),
    ("BatchingProducerBuilder", None): ("Topic.batching", "keywords"),
    ("FilteredReaderBuilder", None): ("ConsumerGroup.reader", "keywords"),
    ("FilterPreviewBuilder", None): ("GroupFilter.preview", "keywords"),
    ("FilterPreviewBuilder", "send"): ("GroupFilter.preview", "one-call"),
    ("CreateConsumerGroup", None): ("ConsumerGroup.create", "keywords"),
    ("ContextAssembler", None): ("Laser.assemble_context", "keywords"),
    ("ContextAssembler", "assemble"): ("Laser.assemble_context", "one-call"),
    ("ContextAssemblerBuilder", None): ("Laser.assemble_context", "keywords"),
    ("ProjectionsRequest", None): ("Projections.list", "keywords"),
    ("ProjectionsRequest", "fetch"): ("Projections.list", "one-call"),
    ("RegisterSchemaRequest", None): ("Schemas.register", "keywords"),
    ("RegisterSchemaRequest", "send"): ("Schemas.register", "one-call"),
    ("ForkCreateRequest", None): ("ForkHandle.create", "keywords"),
    ("ForkCreateRequest", "send"): ("ForkHandle.create", "one-call"),
    ("MemoryTopicBuilder", None): ("Laser.memory_topic", "keywords"),
    ("RecallBuilder", None): ("MemoryHandle.recall", "keywords"),
    ("RememberBuilder", None): ("MemoryHandle.remember", "keywords"),
    ("MemoryQuery", None): ("MemoryHandle.recall", "keywords"),
    ("MemoryQueryBuilder", None): ("MemoryHandle.recall", "keywords"),
    ("MemoryScope", None): ("MemoryHandle.remember", "keywords"),
    ("MemoryScopeBuilder", None): ("MemoryHandle.remember", "keywords"),
    ("LlmUsageBuilder", None): ("new:Provenance", "keywords"),
    ("ProvenanceBuilder", None): ("new:Provenance", "keywords"),
    ("ProducerMessage", None): ("Producer.send", "keywords"),
    ("ProducerMessageBuilder", None): ("Producer.send", "keywords"),
    ("Record", None): ("BatchPublishRequest.add_record", "keywords"),
    ("RecordBuilder", None): ("BatchPublishRequest.add_record", "keywords"),
    ("IntentBuilder", None): ("new:Intent", "keywords"),
    ("Watch", None): ("Laser.watch", "keywords"),
    ("CapabilitySelector", None): ("Laser.contract", "keywords"),
    ("Router", None): ("Workflow.step", "keywords"),
    ("Router", "to_principal"): ("Workflow.step(to=, principal=)", "keywords"),
    ("Router", "apply"): ("new:Provenance(target_agent_id=)", "keywords"),
    ("Router", "resolve_targets"): ("AgentRegistry.resolve_targets", "keywords"),
    ("StepHandle", None): ("Workflow.step", "keywords"),
    ("StepHandle", "compensate_with"): ("Workflow.step(compensate=)", "keywords"),
    ("StepHandle", "exclusive_in"): ("Workflow.step(fence_namespace=)", "keywords"),
    ("StepHandle", "verify_with"): ("Workflow.step(verify=)", "keywords"),
    ("StepHandle", "budget"): ("Workflow.budget", "native-binding"),
    ("StepHandle", "run"): ("~Workflow.run", None),
    ("StepHandle", "run_id"): ("Workflow.run_id", "native-binding"),
    ("StepHandle", "step"): ("Workflow.step", "keywords"),
    ("StepHandle", "inbox_route"): ("Laser.workflow(fixed_inbox=)", "keywords"),
    ("Workflow", "inbox_route"): ("Laser.workflow(fixed_inbox=)", "keywords"),
    ("InboxRoute", "resolve"): ("fn:inbox_route_resolve", "free-function"),
    ("SessionConfig", "new"): ("Laser.sessions", "keywords"),
    ("SessionConfig", "context_tokens"): ("Laser.sessions(context_tokens=)", "keywords"),
    ("SessionConfig", "context_turns"): ("Laser.sessions(context_turns=)", "keywords"),
    ("SessionConfig", "memory_namespace"): ("Laser.sessions(memory_namespace=)", "keywords"),
    ("SessionConfig", "stream"): ("Laser.sessions(stream=)", "keywords"),
    ("SessionConfig", "layout"): ("Laser.sessions(layout=)", "keywords"),
    ("SessionConfig", "idle_timeout"): ("Laser.sessions(idle_timeout_ms=)", "keywords"),
    ("SessionConfig", "heartbeat"): ("Laser.sessions(heartbeat_ms=)", "keywords"),
    ("SessionConfig", "register_source"): ("Laser.sessions(register_source=)", "keywords"),
    ("SessionConfig", "fail_on_dead_letter"): ("Laser.sessions(fail_on_dead_letter=)", "keywords"),
    ("SessionConfig", "sdk"): ("Laser.sessions(sdk=)", "keywords"),
    ("SessionConfig", "layout_kind"): ("SessionConfig.layout", "property"),
    ("SessionConfig", "sdk_info"): ("SessionConfig.sdk", "property"),
    ("SessionConfig", "idle_timeout_value"): ("SessionConfig.idle_timeout_ms", "property"),
    ("SessionConfig", "heartbeat_value"): ("SessionConfig.heartbeat_ms", "property"),
    ("TopicRetention", "expiry"): ("TopicRetention.expiry_ms", "property"),
    ("TopicRetention", "new"): ("new:TopicRetention(expiry_ms=, max_size=)", "keywords"),
    ("ScopedMemory", "consolidate_with"): ("ScopedMemory.consolidate(summarizer=, prune_summarized=)", "overload"),
    ("MemoryHandle", "consolidate_with"): ("MemoryHandle.consolidate(summarizer=, prune_summarized=)", "overload"),
    ("AgdxSend", "send_receipt"): ("Agdx.command(receipt=)", "keywords"),
    ("AgdxSend", "with_ancestry"): ("Agdx.command(parent=, root=)", "keywords"),
    ("SessionRef", None): ("dict:Session.reference", "serde-dict"),
    ("SessionStateView", None): ("dict:SessionState.get", "serde-dict"),
    ("StateChange", None): ("dict:SessionState.get", "serde-dict"),
    ("SourceFrontier", None): ("dict:SessionState.get", "serde-dict"),
    ("ContextManifest", None): ("dict:AssembledContext.manifest", "serde-dict"),
    ("Fragment", None): ("dict:AssembledContext.manifest", "serde-dict"),
    ("ContextCompaction", None): ("dict:Session.record_compaction", "serde-dict"),
    ("SessionParking", None): ("dict:ParkedRecords.records", "serde-dict"),
    ("SessionPolicy", "conversation_for"): ("fn:session_policy_conversation_for", "free-function"),
    ("ChunkAssembler", "is_finished"): ("ChunkAssembler.finished", "property"),
    ("Laser", "bind_roles_expect_revision"): ("Laser.bind_roles(expect_revision=)", "overload"),
    ("A2aBridge", "submit_to"): ("A2aBridge.submit(target=)", "overload"),
    ("A2aBridge", "submit_in_to"): ("A2aBridge.submit_in(target=)", "overload"),
    ("McpBridge", "call_tool_to"): ("McpBridge.call_tool(target=)", "overload"),
    ("McpBridge", "call_tool_in_to"): ("McpBridge.call_tool_in(target=)", "overload"),
    ("Agdx", "request_input_from"): ("Agdx.request_input(target=)", "overload"),
    ("Laser", "builder"): ("Laser.connect", "keywords"),
    ("Laser", "sessions_with"): ("Laser.sessions(stream=)", "overload"),
    ("Laser", "with_governor_retention"): ("Laser.with_governor_retention", None),
    ("Gather", "replies"): ("~AgentCtx.fan_out", None),
    ("ScatterReport", "completed"): ("~Laser.scatter_report", None),
    ("ScatterReport", "failures"): ("~Laser.scatter_report", None),
    ("ClientMetadataRequest", "all"): ("Laser.client_metadata_all", "one-call"),
    ("LaserError", "publish_cause"): ("PublishFailedError", "protocol"),
    ("LaserError", "rejected"): ("new:RejectedError", "constructor"),
    ("LaserError", "unsupported"): ("new:UnsupportedError", "constructor"),
    ("LaserError", "unsupported_feature"): ("new:UnsupportedError", "constructor"),
    ("LaserError", "is_ambiguous_mutation"): ("LaserError.ambiguous_mutation", "property"),
    ("LaserError", "is_budget_exceeded"): ("LaserError.budget_exceeded", "property"),
    ("LaserError", "is_fence_violation"): ("LaserError.fence_violation", "property"),
    ("LaserError", "is_lease_lost"): ("LaserError.lease_lost", "property"),
    ("LaserError", "is_no_capable_agent"): ("LaserError.no_capable_agent", "property"),
    ("LaserError", "is_not_found"): ("LaserError.not_found", "property"),
    ("LaserError", "is_not_leader"): ("LaserError.not_leader", "property"),
    ("LaserError", "is_permission_denied"): ("LaserError.permission_denied", "property"),
    ("LaserError", "is_quarantined"): ("LaserError.quarantined", "property"),
    ("LaserError", "is_retryable"): ("LaserError.retryable", "property"),
    ("LaserError", "is_stale"): ("LaserError.stale", "property"),
    ("LaserError", "is_stream_or_topic_not_found"): ("LaserError.stream_or_topic_not_found", "property"),
    ("LaserError", "is_unavailable"): ("LaserError.unavailable", "property"),
    ("LaserError", "is_unsupported"): ("LaserError.unsupported", "property"),
    ("LaserError", "is_version_conflict"): ("LaserError.version_conflict", "property"),
    ("LaserError", "is_version_skew"): ("LaserError.version_skew", "property"),
    ("PublishFailure", None): ("PublishFailedError", "error-class"),
    ("PublishFailure", "source"): ("PublishFailedError", "protocol"),
    ("ConsumerBuilder", "auto_join_group"): ("ConsumerGroup.consumer(auto_join_group=)", "keywords"),
    ("ConsumerBuilder", "create_group"): ("ConsumerGroup.consumer(create_group=)", "keywords"),
    ("ConsumerBuilder", "commit_policy"): ("Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)", "keywords"),
    ("ConsumerBuilder", "start_at"): ("Topic.consumer(polling=, offset=, timestamp_micros=)", "keywords"),
    ("ConsumerBuilder", "without_poll_interval"): ("Topic.consumer(poll_interval_ms=)", "overload"),
    ("ProducerBuilder", "expire_after"): ("Topic.producer(message_expiry=)", "keywords"),
    ("ProducerBuilder", "never_expire"): ("Topic.producer(message_expiry=)", "keywords"),
    ("ProducerBuilder", "max_topic_bytes"): ("Topic.producer(max_topic_size=)", "keywords"),
    ("ProducerBuilder", "unlimited_topic_size"): ("Topic.producer(max_topic_size=)", "keywords"),
    ("ProducerBuilder", "retry_backoff"): ("Topic.producer(retry_interval_ms=)", "keywords"),
    ("ProducerBuilder", "routing"): ("Topic.producer(key=, partition=)", "keywords"),
    ("Producer", "send_keyed"): ("Producer.send(key=)", "overload"),
    ("Producer", "send_to_partition"): ("Producer.send(partition=)", "overload"),
    ("Producer", "send_with_routing"): ("Producer.send(key=, partition=)", "overload"),
    ("Producer", "send_batch_with_routing"): ("Producer.send_batch(key=, partition=)", "overload"),
    ("Producer", "send_message"): ("Producer.send(headers=)", "overload"),
    ("Capabilities", "with_sessions"): ("Laser.with_capabilities(sessions=)", "keywords"),
    ("Capabilities", "with_stream_tenancy"): ("Laser.with_capabilities(stream_tenancy=)", "keywords"),
    ("ProducerMessage", "header"): ("Producer.send(headers=)", "keywords"),
    ("Record", "metadata"): ("BatchPublishRequest.add_record(headers=)", "keywords"),
    ("RecordBuilder", "metadata"): ("BatchPublishRequest.add_record(headers=)", "keywords"),
    ("CreateConsumerGroup", "policy"): ("ConsumerGroup.create(filter_id=, revision=)", "keywords"),
    ("Topic", "iggy_consumer"): ("-", "rust-crate"),
    ("Topic", "iggy_consumer_group"): ("-", "rust-crate"),
    ("Topic", "iggy_producer"): ("-", "rust-crate"),
    ("BatchPublishRequest", "add_json_with_projection"): ("BatchPublishRequest.add_json(projection_ref=)", "overload"),
    ("BatchPublishRequest", "add_msgpack_with_projection"): ("BatchPublishRequest.add_msgpack(projection_ref=)", "overload"),
    ("BatchPublishRequest", "add_payload_with_projection"): ("BatchPublishRequest.add_payload(projection_ref=)", "overload"),
    ("BatchPublishRequest", "add_raw_bytes_with_projection"): ("BatchPublishRequest.add_raw_bytes(projection_ref=)", "overload"),
    ("BatchPublishRequest", "content_type"): ("BatchPublishRequest.add_raw_bytes(content_type=)", "keywords"),
    ("PublishRequest", "content_type"): ("PublishRequest.raw_bytes(content_type=)", "keywords"),

    ("SharedConsolidator", None): ("-", "shared-ownership"),
    ("SharedEmbedder", None): ("-", "shared-ownership"),
    ("SharedReranker", None): ("-", "shared-ownership"),
    ("MaybeEmbedder", None): ("-", "shared-ownership"),
    ("SharedKvTransport", None): ("-", "shared-ownership"),
    ("DefaultConsolidator", None): ("MemoryHandle.consolidate", "keywords"),
    ("NoSummarizer", None): ("MemoryHandle.consolidate(summarizer=)", "overload"),
    ("TokenBudget", "with_estimator"): ("new:TokenBudget(estimator=)", "overload"),
    ("MemoryHandle", "recall_folded"): ("MemoryHandle.recall(folded=)", "overload"),
    ("LogMemory", "recall_folded"): ("MemoryHandle.recall(folded=)", "overload"),
    ("LogMemory", "on_topic_named"): ("LogMemory.on_topic", "overload"),
    ("LogMemory", "on_stream_topic_named"): ("LogMemory.on_stream_topic", "overload"),
    ("RecallBuilder", "hybrid"): ("MemoryHandle.recall(strategy=)", "overload"),
    ("RecallBuilder", "keyword"): ("MemoryHandle.recall(strategy=)", "overload"),
    ("RecallBuilder", "recent"): ("MemoryHandle.recall(strategy=)", "overload"),
    ("RecallBuilder", "block"): ("MemoryHandle.recall(block=)", "overload"),
    ("RememberBuilder", "scope"): ("MemoryHandle.remember(conversation=)", "keywords"),
    ("MemoryScopeBuilder", "app"): ("MemoryHandle.remember(application=)", "keywords"),
    ("MemoryScopeBuilder", "lifetime"): ("MemoryHandle.remember(durable=)", "keywords"),
    ("MemoryTopicBuilder", "no_expiry"): ("Laser.memory_topic(ttl_ms=)", "overload"),
    ("MemoryKind", "class"): ("fn:memory_kind_class", "free-function"),
    ("MemoryId", "content"): ("fn:memory_id_content", "free-function"),
    ("Capabilities", "with_managed"): ("Laser.with_capabilities(managed=)", "keywords"),
    ("Capabilities", "with_query"): ("Laser.with_capabilities(query=)", "keywords"),
    ("Capabilities", "with_query_consistency"): ("Laser.with_capabilities(query_consistency=)", "keywords"),
    ("Capabilities", "with_query_keyword"): ("Laser.with_capabilities(query_keyword=)", "keywords"),
    ("Capabilities", "with_query_execution"): ("Laser.with_capabilities(query_execution=)", "keywords"),
    ("Capabilities", "with_destinations"): ("Laser.with_capabilities(destinations=)", "keywords"),
    ("Capabilities", "with_destination_consistency"): ("Laser.with_capabilities(destinations_consistency=)", "keywords"),
    ("Capabilities", "with_kv"): ("Laser.with_capabilities(kv=)", "keywords"),
    ("Capabilities", "with_kv_cas"): ("Laser.with_capabilities(kv_cas=)", "keywords"),
    ("Capabilities", "with_kv_fenced_leases"): ("Laser.with_capabilities(kv_fenced_leases=)", "keywords"),
    ("Capabilities", "with_kv_cas_fenced"): ("Laser.with_capabilities(kv_cas_fenced=)", "keywords"),
    ("Capabilities", "with_graph"): ("Laser.with_capabilities(graph=)", "keywords"),
    ("Capabilities", "with_forks"): ("Laser.with_capabilities(forks=)", "keywords"),
    ("Capabilities", "with_a2a_gateway"): ("Laser.with_capabilities(a2a_gateway=)", "keywords"),
    ("Capabilities", "with_filters"): ("Laser.with_capabilities(filters=, filters_catalog=)", "keywords"),
    ("Capabilities", "with_versions"): ("Laser.with_capabilities(versions=)", "keywords"),
    ("Capabilities", "with_backends"): ("Laser.with_capabilities(backends=)", "keywords"),
    ("QueryRequest", "nearest_in"): ("QueryRequest.nearest(field=)", "overload"),
    ("QueryRequest", "raw_sql_with"): ("QueryRequest.raw_sql(dialect=)", "overload"),
    ("FilteredReaderBuilder", "partition"): ("ConsumerGroup.reader(partitions=)", "keywords"),
    ("ProjectionsRequest", "for_topic"): ("Projections.list(topic=)", "keywords"),
    ("ProjectionsRequest", "for_topics"): ("Projections.list(topics=)", "keywords"),
    ("AgentTopic", "as_identifier"): ("-", "rust-crate"),
    ("ProvenanceBuilder", "deadline"): ("new:Provenance(deadline_micros=)", "keywords"),
    ("KeyRecord", "operator"): ("new:KeyRecord(kind=)", "keywords"),
    ("KeyRecord", "valid_window"): ("new:KeyRecord(valid_from_micros=, valid_to_micros=)", "keywords"),
    ("KvKeyRegistry", "enroll_record"): ("KvKeyRegistry.enroll_record", None),
    ("KvKeyRegistry", "in_namespace"): ("new:KvKeyRegistry(namespace=)", "overload"),
    ("A2aBridge", "with_capabilities"): ("new:A2aBridge(capabilities=)", "keywords"),
    ("A2aBridge", "with_signing_key"): ("new:A2aBridge(signing_key=)", "keywords"),
    ("A2aBridge", "router"): ("-", "rust-crate"),
    ("McpBridge", "router"): ("-", "rust-crate"),
    ("McpBridge", "with_memory_tools"): ("new:McpBridge(memory_tools=)", "keywords"),
    ("McpBridge", "with_timeout"): ("new:McpBridge(timeout_ms=)", "keywords"),
    ("McpBridge", "with_prompt"): ("new:McpBridge(prompts=)", "keywords"),
    ("McpBridge", "with_resource"): ("new:McpBridge(resources=)", "keywords"),
    ("McpBridge", "with_tool"): ("new:McpBridge(tools=)", "keywords"),
    ("CrashContext", "assemble"): ("new:CrashContext", "constructor"),
    ("Cursor", "batch"): ("Topic.replay(batch=)", "keywords"),
    ("Cursor", "from_offsets"): ("Topic.replay(from_offsets=)", "keywords"),
    ("Destinations", "mutate_with_supervisor_assertion"): ("Destinations.mutate(supervisor_assertion=)", "overload"),
    ("KeyRecord", "revoked"): ("KeyRecord.revoke", "name-collision"),

    ("PartitionCheckpoint", None): ("dict:Destinations.get", "serde-dict"),
    ("SourceOffsetRange", None): ("dict:Destinations.prepare", "serde-dict"),
    ("CheckpointOwnerLease", None): ("dict:Destinations.get", "serde-dict"),
    ("CredentialGeneration", None): ("dict:Destinations.prepare", "serde-dict"),
    ("AttemptObject", None): ("dict:Destinations.prepare", "serde-dict"),
    ("AttemptColumnMetrics", None): ("dict:Destinations.prepare", "serde-dict"),
    ("PreparedTableRequirements", None): ("dict:Destinations.prepare", "serde-dict"),
    ("IcebergCommitRequirement", None): ("dict:Destinations.prepare", "serde-dict"),
    ("PreparedAttempt", None): ("dict:Destinations.prepare", "serde-dict"),
    ("PreparedAttemptSummary", None): ("dict:Destinations.get", "serde-dict"),
    ("CompletedAttempt", None): ("dict:Destinations.complete", "serde-dict"),
    ("RetentionGap", None): ("dict:Destinations.record_retention_gap", "serde-dict"),
    ("DestinationBlock", None): ("dict:Destinations.record_block", "serde-dict"),
    ("DestinationCheckpointStatus", None): ("dict:Destinations.get", "serde-dict"),
    ("DestinationCheckpointView", None): ("dict:Destinations.get", "serde-dict"),
    ("DestinationCheckpointPage", None): ("dict:Destinations.list", "serde-dict"),
    ("DestinationListFilter", None): ("dict:Destinations.list", "serde-dict"),
    ("QueryRoutePage", None): ("dict:Destinations.query_routes", "serde-dict"),
    ("CheckpointRequestEnvelope", None): ("dict:Laser.execute_checkpoint", "serde-dict"),
    ("PublicCheckpointMutation", None): ("dict:Destinations.mutate", "serde-dict"),
    ("RepairRecord", None): ("dict:Destinations.record_repair", "serde-dict"),
    ("CheckpointMutationResult", None): ("dict:Destinations.mutate", "serde-dict"),
    ("CheckpointError", None): ("dict:CheckpointError.detail", "serde-dict"),
    ("BackendBinding", None): ("dict:Destinations.register", "serde-dict"),
    ("ProjectionRef", None): ("dict:Destinations.register", "serde-dict"),
    ("PhysicalTable", None): ("dict:Destinations.register", "serde-dict"),
    ("PartitionStart", None): ("dict:Destinations.register", "serde-dict"),
    ("StartPolicy", None): ("dict:Destinations.register", "serde-dict"),
    ("MaterializationDestination", None): ("dict:Destinations.register", "serde-dict"),
    ("QueryRoute", None): ("dict:Destinations.register_query_route", "serde-dict"),
    ("QueryRouteTarget", None): ("dict:Destinations.register_query_route", "serde-dict"),
    ("SourceIncarnation", None): ("dict:Destinations.get", "serde-dict"),
    ("SourceScope", None): ("dict:Destinations.register", "serde-dict"),
    ("EdgeClaims", None): ("fn:authorize_edge", "keywords"),
    ("AuthzSubject", None): ("dict:Laser.authz_history", "serde-dict"),
    ("AuthzEventKind", None): ("dict:AuthzEvent.op", "serde-dict"),
    ("AuthzError", "Unauthorized"): ("key", "serde-dict"),
    ("AuthzError", "UnknownRole"): ("key", "serde-dict"),
    ("AuthzError", "InvalidName"): ("key", "serde-dict"),
    ("AuthzError", "Conflict"): ("key", "serde-dict"),
    ("AuthzError", "Version"): ("key", "serde-dict"),
    ("AuthzError", "TenancyViolation"): ("key", "serde-dict"),
    ("KvError", "InvalidKey"): ("key", "serde-dict"),
    ("KvError", "InvalidNamespace"): ("key", "serde-dict"),
    ("KvError", "TooLarge"): ("key", "serde-dict"),
    ("KvError", "Backend"): ("key", "serde-dict"),
    ("KvError", "Version"): ("key", "serde-dict"),
    ("FilterError", "message"): ("key", "serde-dict"),
    ("ReliableConsumer", None): ("Laser.spawn_agent", "keywords"),
    ("ReliableConsumer", "builder"): ("Laser.spawn_agent", "keywords"),
    ("ReliableConsumer", "run"): ("Laser.spawn_agent", "one-call"),
    ("ReliableConsumerBuilder", None): ("Laser.spawn_agent", "keywords"),
    ("ReliableConsumerBuilder", "agent"): ("Laser.spawn_agent(agent_id=)", "keywords"),
    ("ReliableConsumerBuilder", "group"): ("Laser.spawn_agent(consumer_group=)", "keywords"),
    ("ReliableConsumerBuilder", "topic"): ("Laser.spawn_agent(listen_on=)", "keywords"),
    ("ReliableConsumerBuilder", "inbox_route"): ("Laser.spawn_agent(fixed_inbox=)", "keywords"),
    ("ReliableConsumerBuilder", "concurrency"): ("Laser.spawn_agent(max_partitions=)", "keywords"),
    ("ReliableConsumerBuilder", "deduplicator"): ("Laser.spawn_agent(dedup=)", "keywords"),
    ("ReliableConsumerBuilder", "on_dead_letter"): ("Laser.spawn_agent(dead_letter=)", "keywords"),
    ("ReliableConsumerBuilder", "retry"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "keywords"),
    ("RetryPolicy", None): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "keywords"),
    ("RetryPolicy", "max_attempts"): ("Laser.spawn_agent(retry_max_attempts=)", "keywords"),
    ("RetryPolicy", "base_delay"): ("Laser.spawn_agent(retry_base_delay_ms=)", "keywords"),
    ("RetryPolicy", "backoff"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "keywords"),
    ("ConcurrencyPolicy", None): ("Laser.spawn_agent(max_partitions=)", "keywords"),
    ("ConcurrencyPolicy", "Serial"): ("Laser.spawn_agent(max_partitions=)", "keywords"),
    ("ConcurrencyPolicy", "SerialPerPartition"): ("Laser.spawn_agent(max_partitions=)", "keywords"),
    ("SlidingWindow", None): ("Laser.spawn_agent(dedup_window=)", "keywords"),
    ("GatherPolicy", None): ("AgentCtx.fan_out(policy=, quorum=)", "keywords"),
    ("GatherPolicy", "RequireAll"): ("AgentCtx.fan_out(policy=)", "keywords"),
    ("GatherPolicy", "Quorum"): ("AgentCtx.fan_out(policy=, quorum=)", "keywords"),
    ("GatherPolicy", "BestEffort"): ("AgentCtx.fan_out(policy=)", "keywords"),
    ("RoutePolicy", None): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "Cheapest"): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "Fastest"): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "LeastLoaded"): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "Sticky"): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "Any"): ("Laser.contract(policy=)", "keywords"),
    ("RoutePolicy", "Custom"): ("Laser.contract(policy=)", "keywords"),
    ("ReplayBound", None): ("ContextScope.state", "keywords"),
    ("ReplayBound", "Last"): ("ContextScope.state(last_n=)", "keywords"),
    ("Router", "ToPrincipal"): ("Workflow.step(to=, principal=)", "keywords"),
    ("Router", "Broadcast"): ("new:Provenance(target_agent_id=)", "keywords"),
    ("Router", "broadcast"): ("new:Provenance(target_agent_id=)", "keywords"),
    ("Workflow", "step"): ("Workflow.step", "keywords"),
    ("Laser", "scatter"): ("Laser.scatter", "keywords"),
    ("Laser", "scatter_report"): ("Laser.scatter_report", "keywords"),
    ("StepContext", None): ("Workflow.step(build=)", "callback"),
    ("InboxRoute", None): ("Laser.spawn_agent(fixed_inbox=)", "keywords"),
    ("InboxRoute", "Advertised"): ("Laser.spawn_agent(fixed_inbox=)", "keywords"),
    ("InboxRoute", "Fixed"): ("Laser.spawn_agent(fixed_inbox=)", "keywords"),
    ("AgentCard", None): ("dict:A2aBridge.card", "serde-dict"),
    ("AgentCardCapabilities", None): ("dict:A2aBridge.card", "serde-dict"),
    ("AgentCardSignature", None): ("dict:A2aBridge.card", "serde-dict"),
    ("AgentInterface", None): ("dict:A2aBridge.card", "serde-dict"),
    ("AgentSkill", None): ("dict:A2aBridge.card", "serde-dict"),
    ("Task", None): ("dict:A2aBridge.submit", "serde-dict"),
    ("TaskStatus", None): ("dict:A2aBridge.submit", "serde-dict"),
    ("Artifact", None): ("dict:A2aBridge.submit", "serde-dict"),
    ("JsonRpcResponse", None): ("dict:A2aBridge.handle_rpc", "serde-dict"),
    ("JsonRpcError", None): ("dict:A2aBridge.handle_rpc", "serde-dict"),
    ("McpRpcResponse", None): ("dict:McpBridge.handle_rpc", "serde-dict"),
    ("McpRpcError", None): ("dict:McpBridge.handle_rpc", "serde-dict"),
    ("McpTool", None): ("dict:McpBridge.list_tools", "serde-dict"),
    ("McpResource", None): ("dict:McpBridge.list_resources", "serde-dict"),
    ("McpPrompt", None): ("dict:McpBridge.list_prompts", "serde-dict"),
    ("McpPromptArgument", None): ("dict:McpBridge.list_prompts", "serde-dict"),
    ("McpContent", None): ("dict:McpBridge.call_tool", "serde-dict"),
    ("McpToolResult", None): ("dict:McpBridge.call_tool", "serde-dict"),
    ("AgUiEvent", None): ("dict:Laser.agui_events", "serde-dict"),
    ("SigningKey", "sign_with_context"): ("SigningKey.sign_with_context(content_type=, agent_version=)", "keywords"),
    ("KeyRegistry", "verify_observed_at"): ("KeyRegistry.verify_observed_at(content_type=, agent_version=)", "keywords"),
    ("JsonRpcRequest", None): ("dict:A2aBridge.handle_rpc", "serde-dict"),
    ("McpRpcRequest", None): ("dict:McpBridge.handle_rpc", "serde-dict"),
    ("KvNamespaceInfo", None): ("dict:Laser.kv_namespaces", "serde-dict"),
    ("CasExpect", None): ("dict:FencedLeaseClient.prepare_cas_fenced", "serde-dict"),
    ("KvCasFenced", None): ("dict:FencedLeaseClient.prepare_cas_fenced", "serde-dict"),
    ("KvGet", None): ("dict:FencedLeaseClient.get", "serde-dict"),
    ("KvLease", None): ("dict:FencedLeaseClient.prepare_acquire", "serde-dict"),
    ("KvLeaseRenew", None): ("dict:FencedLeaseClient.prepare_renew", "serde-dict"),
    ("KvRelease", None): ("dict:FencedLeaseClient.prepare_release", "serde-dict"),
    ("CheckpointRequestEnvelope", "with_supervisor_assertion"): ("fn:new_checkpoint_request_envelope(supervisor_assertion=)", "keywords"),
    ("ForkInfo", None): ("dict:ForkHandle.create", "serde-dict"),
    ("ForkError", None): ("dict:ForkError.detail", "serde-dict"),
    ("FoldSnapshot", None): ("dict:SnapshotStore.latest", "serde-dict"),
    ("SnapshotOffset", None): ("-", "native-value"),
    ("SnapshotOffset", "topic_id"): ("-", "native-value"),
    ("SnapshotOffset", "topic_created_at_micros"): ("-", "native-value"),
    ("SnapshotOffset", "partition_id"): ("-", "native-value"),
    ("SnapshotOffset", "offset"): ("-", "native-value"),
    ("ProducerInfo", None): ("dict:GraphHandle.upsert", "serde-dict"),
    ("GraphNode", None): ("dict:GraphHandle.upsert", "serde-dict"),
    ("GraphEdge", None): ("dict:GraphHandle.upsert", "serde-dict"),
    ("GraphResult", None): ("dict:GraphHandle.fetch", "serde-dict"),
    ("SourceRef", None): ("dict:GraphHandle.fetch", "serde-dict"),
    ("QueryError", "Unauthorized"): ("key", "serde-dict"),
    ("QueryError", "IndexNotFound"): ("key", "serde-dict"),
    ("QueryError", "ForkNotFound"): ("key", "serde-dict"),
    ("QueryError", "Backend"): ("key", "serde-dict"),
    ("QueryError", "TooLarge"): ("key", "serde-dict"),
    ("QueryError", "Version"): ("key", "serde-dict"),
    ("QueryError", "Cancelled"): ("key", "serde-dict"),
    ("QueryError", "DeadlineExceeded"): ("key", "serde-dict"),
    ("QueryError", "ExpiredSnapshot"): ("key", "serde-dict"),
    ("QueryError", "StaleGeneration"): ("key", "serde-dict"),
    ("QueryError", "TargetUnavailable"): ("key", "serde-dict"),
    ("QueryError", "ResourceLimit"): ("key", "serde-dict"),
    ("Filter", "Not"): ("Filter.negate", "constructor"),
    ("TypedRecords", "batch"): ("TypedTopic.records(batch=)", "keywords"),
    ("TypedRecords", "from_offsets"): ("TypedTopic.records(from_offsets=)", "keywords"),
    ("ClientMetadata", None): ("dict:Laser.client_metadata_all", "serde-dict"),
    ("BackendImplementation", None): ("dict:BackendDescriptor.implementation", "serde-dict"),
    ("BackendLimits", None): ("dict:BackendDescriptor.limits", "serde-dict"),
    ("BackendReadiness", None): ("dict:BackendDescriptor.readiness", "serde-dict"),
    ("BackendReadinessReason", None): ("dict:BackendDescriptor.readiness", "serde-dict"),
    ("MaintenanceCapabilities", None): ("dict:BackendDescriptor.maintenance", "serde-dict"),
    ("MaterializationCapability", None): ("dict:BackendDescriptor.materialization", "serde-dict"),
    ("QueryCapabilities", None): ("dict:BackendDescriptor.query", "serde-dict"),
    ("SchemaCapabilities", None): ("dict:BackendDescriptor.schema", "serde-dict"),
    ("Codec", None): ("PublishRequest.encode_with(codec=)", "callback"),
    ("Decoder", None): ("KvEntry.decode_value_with(codec=)", "callback"),
    ("context::checkpoint", None): ("fn:context_checkpoint", "flat-namespace"),
    ("Projection", None): ("dict:Projections.register", "serde-dict"),
    ("IndexSchema", None): ("dict:Projections.register", "serde-dict"),
    ("IndexField", None): ("dict:Projections.register", "serde-dict"),
    ("EntitySchema", None): ("dict:Projections.register_graph", "serde-dict"),
    ("NodeExtract", None): ("dict:Projections.register_graph", "serde-dict"),
    ("EdgeExtract", None): ("dict:Projections.register_graph", "serde-dict"),
    ("ProjectionKind", None): ("-", "plain-value"),
    ("ProjectionBinding", None): ("dict:Bindings.apply", "serde-dict"),
    ("RetentionPolicy", None): ("dict:Bindings.apply", "serde-dict"),
    ("SourceSelector", None): ("dict:Bindings.remove", "serde-dict"),
    ("SchemaSource", None): ("dict:Schemas.register", "serde-dict"),
    ("SchemaDef", None): ("dict:Schemas.get", "serde-dict"),
    ("ProjectionInfo", None): ("dict:Projections.get", "serde-dict"),
    ("SchemaInfo", None): ("dict:Schemas.get", "serde-dict"),
    ("IndexSchema", "builder"): ("new:IndexSchemaBuilder", "constructor"),
    ("Projection", "builder"): ("new:ProjectionBuilder", "constructor"),
    ("ProjectionBinding", "builder"): ("new:ProjectionBindingBuilder", "constructor"),
    ("Query", None): ("dict:Laser.execute_query", "serde-dict"),
    ("AggCall", None): ("dict:Laser.execute_query", "serde-dict"),
    ("Aggregate", None): ("dict:Laser.execute_query", "serde-dict"),
    ("KeyMatch", None): ("dict:Laser.execute_query", "serde-dict"),
    ("Predicate", None): ("dict:Laser.execute_query", "serde-dict"),
    ("QueryPageRequest", None): ("dict:Laser.execute_query", "serde-dict"),
    ("QueryTarget", None): ("dict:Laser.execute_query", "serde-dict"),
    ("RawSql", None): ("dict:Laser.execute_query", "serde-dict"),
    ("Select", None): ("dict:Laser.execute_query", "serde-dict"),
    ("SnapshotSelector", None): ("dict:Laser.execute_query", "serde-dict"),
    ("Sort", None): ("dict:Laser.execute_query", "serde-dict"),
    ("TextQuery", None): ("dict:Laser.execute_query", "serde-dict"),
    ("VectorQuery", None): ("dict:Laser.execute_query", "serde-dict"),
    ("Window", None): ("dict:Laser.execute_query", "serde-dict"),
    ("QueryContext", None): ("dict:QueryResult.context", "serde-dict"),
    ("QueryEngine", None): ("dict:QueryResult.context", "serde-dict"),
    ("MaterializationBoundary", None): ("dict:QueryResult.context", "serde-dict"),
    ("ResolvedQueryTarget", None): ("dict:QueryResult.context", "serde-dict"),
    ("QueryExecutionStatus", None): ("dict:Laser.query_status", "serde-dict"),
    ("LogicalField", None): ("dict:QueryResult.fields", "serde-dict"),
    ("LogicalType", None): ("dict:QueryResult.fields", "serde-dict"),
    ("TypedValue", None): ("dict:Row.values", "serde-dict"),
    ("DecimalValue", None): ("dict:Row.values", "serde-dict"),
    ("FieldValue", None): ("dict:Row.values", "serde-dict"),
    ("MapEntry", None): ("dict:Row.values", "serde-dict"),
    ("LogicalSchemaRef", None): ("dict:LogicalSchema.schema", "serde-dict"),
    ("Query", "builder"): ("new:QueryBuilder", "constructor"),
    ("ResultCode", None): ("-", "plain-value"),
    ("AgentTopic", "name"): ("-", "plain-value"),
    ("AgentTopic", "topic_string"): ("-", "plain-value"),
    ("ConsumerGroupName", "for_agent"): ("-", "plain-value"),
    ("MintUlid", None): ("fn:mint_ulid", "free-function"),
    ("MintUlid", "mint"): ("fn:mint_ulid", "free-function"),
    ("Memory", None): ("Laser.memory_custom(backend=)", "callback"),
    ("LocalMemory", None): ("Laser.memory_custom(backend=)", "callback"),
    ("MemoryKind", "code"): ("fn:memory_kind_code", "free-function"),
    ("MemoryHandle", "Log"): ("new:LogMemory", "constructor"),
    ("MemoryHandle", "Reranked"): ("new:RerankedMemory", "constructor"),
    ("Feedback", None): ("MemoryHandle.improve", "keywords"),
    ("AgentEnvelope", None): ("dict:AgentMessage.envelope", "serde-dict"),
    ("Signature", None): ("dict:AgentMessage.envelope", "serde-dict"),
    ("AgentEnvelope", "command"): ("fn:command_envelope", "free-function"),
    ("AgentEnvelope", "response"): ("fn:response_envelope", "free-function"),
    ("AgentEnvelope", "error"): ("fn:error_envelope", "free-function"),
    ("AgentEnvelope", "event"): ("fn:event_envelope", "free-function"),
    ("AgentEnvelope", "chunk"): ("fn:chunk_envelope", "free-function"),
    ("AgentEnvelope", "status"): ("fn:status_envelope", "free-function"),
    ("AgentEnvelope", "with_target"): ("fn:event_envelope(target=)", "keywords"),
    ("AgentEnvelope", "with_cause"): ("fn:event_envelope(cause=, cause_at=)", "keywords"),
    ("AgentEnvelope", "with_correlation"): ("fn:event_envelope(correlation=)", "keywords"),
    ("AgentEnvelope", "with_idempotency_key"): ("fn:event_envelope(idempotency_key=)", "keywords"),
    ("AgentEnvelope", "with_deadline_micros"): ("fn:event_envelope(deadline_micros=)", "keywords"),
    ("AgentEnvelope", "terminal"): ("fn:event_envelope(terminal=)", "keywords"),
    ("AgentEnvelope", "with_task_state"): ("fn:event_envelope(task_state=)", "keywords"),
    ("AgentEnvelope", "with_operation"): ("fn:event_envelope(operation=)", "keywords"),
    ("AgentEnvelope", "with_tool"): ("fn:event_envelope(tool=)", "keywords"),
    ("AgentEnvelope", "with_usage"): ("fn:event_envelope(usage=)", "keywords"),
    ("AgentEnvelope", "with_metadata"): ("fn:event_envelope(metadata=)", "keywords"),
    ("AgentEnvelope", "requiring"): ("fn:event_envelope(requiring=)", "keywords"),
    ("AgentEnvelope", "with_signature"): ("fn:event_envelope(signature=)", "keywords"),
    ("AgentEnvelope", "unmet_requirements"): ("fn:unmet_requirements", "free-function"),
    ("Signature", "validate"): ("fn:validate_signature", "free-function"),
    ("filters::Verdict", None): ("-", "plain-value"),
    ("filters::Verdict", "Selected"): ("-", "plain-value"),
    ("filters::Verdict", "Rejected"): ("-", "plain-value"),
    ("filters::Verdict", "Fault"): ("-", "plain-value"),
    ("AppliedPolicy", None): ("dict:MatchedPage.policy", "serde-dict"),
    ("SourceGeneration", None): ("dict:MatchedPage.generation", "serde-dict"),
    ("FilterBinding", None): ("dict:GroupFilter.get", "serde-dict"),
    ("FilterGroupRef", None): ("dict:GroupFilter.get", "serde-dict"),
    ("FilterGroupIdentity", None): ("dict:ConsumerGroupInfo.identity", "serde-dict"),
    ("FilterRevisionPage", None): ("dict:GroupFilter.revisions", "serde-dict"),
    ("FilterRevisionInfo", None): ("dict:GroupFilter.revisions", "serde-dict"),
    ("FilterRevisionRef", None): ("dict:GroupFilter.revise", "serde-dict"),
    ("FilterPreview", None): ("dict:GroupFilter.preview", "serde-dict"),
    ("PreviewRecord", None): ("dict:GroupFilter.preview", "serde-dict"),
    ("FilterTestResult", None): ("dict:GroupFilter.test", "serde-dict"),
    ("FilterExplanation", None): ("dict:CompiledFilter.explain", "serde-dict"),
    ("ExplainNode", None): ("dict:CompiledFilter.explain", "serde-dict"),
    ("FilteredStart", None): ("dict:ConsumerGroup.reader", "serde-dict"),
    ("Continuation", None): ("dict:ConsumerGroup.reader", "serde-dict"),
    ("Coerce", None): ("dict:FilterExpr.to_dict", "serde-dict"),
    ("CoercedPredicate", None): ("dict:FilterExpr.to_dict", "serde-dict"),
    ("TextPredicate", None): ("dict:FilterExpr.to_dict", "serde-dict"),
    ("HeaderPredicate", None): ("dict:FilterExpr.to_dict", "serde-dict"),
    ("DecodeLimits", None): ("CompiledFilter.evaluate", "keywords"),
    ("FilterRecord", None): ("CompiledFilter.evaluate", "keywords"),
    ("HeaderRef", None): ("CompiledFilter.evaluate(headers=)", "keywords"),
    ("HeaderRef", "key"): ("CompiledFilter.evaluate(headers=)", "keywords"),
    ("HeaderRef", "value"): ("CompiledFilter.evaluate(headers=)", "keywords"),
    ("FilterHeader", None): ("GroupFilter.test(headers=)", "keywords"),
    ("FilterHeader", "key"): ("GroupFilter.test(headers=)", "keywords"),
    ("FilterHeader", "value"): ("GroupFilter.test(headers=)", "keywords"),
    ("HeaderScalar", None): ("-", "plain-value"),
    ("GroupFilterSpec", None): ("ConsumerGroup.create(filter=, filter_id=, revision=)", "keywords"),
    ("GroupFilterSpec", "Definition"): ("ConsumerGroup.create(filter=)", "keywords"),
    ("GroupFilterSpec", "Revision"): ("ConsumerGroup.create(filter_id=, revision=)", "keywords"),
    ("FilterExpr", "Not"): ("FilterExpr.negate", "constructor"),
    ("CompiledFilter", "compile_with_schemas"): ("CompiledFilter.compile(schemas=)", "overload"),
    ("PathSegment", None): ("-", "plain-value"),
    ("StreamEvent", None): ("dict:ChunkAssembler.feed", "serde-dict"),
    ("LaserBuilder", "governor_with_retention"): ("Laser.connect(governor=, governor_mode=, governor_retention=)", "keywords"),
    ("PublishOptions", None): ("Laser.connect", "keywords"),
    ("PublishOptions", "timeout"): ("Laser.connect(publish_timeout_ms=)", "keywords"),
    ("PublishOptions", "max_retries"): ("Laser.connect(publish_max_retries=)", "keywords"),
    ("PublishOptions", "retry_backoff"): ("Laser.connect(publish_retry_backoff_ms=)", "keywords"),
    ("CommitPolicy", None): ("Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)", "keywords"),
    ("CommitPolicy", "Disabled"): ("Topic.consumer(auto_commit=)", "keywords"),
    ("CommitPolicy", "Polling"): ("Topic.consumer(auto_commit=)", "keywords"),
    ("CommitPolicy", "All"): ("Topic.consumer(auto_commit=)", "keywords"),
    ("CommitPolicy", "Each"): ("Topic.consumer(auto_commit=)", "keywords"),
    ("CommitPolicy", "Interval"): ("Topic.consumer(auto_commit=, commit_interval_ms=)", "keywords"),
    ("CommitPolicy", "IntervalOrPolling"): ("Topic.consumer(auto_commit=, commit_interval_ms=)", "keywords"),
    ("CommitPolicy", "IntervalOrAll"): ("Topic.consumer(auto_commit=, commit_interval_ms=)", "keywords"),
    ("CommitPolicy", "IntervalOrEach"): ("Topic.consumer(auto_commit=, commit_interval_ms=)", "keywords"),
    ("CommitPolicy", "Every"): ("Topic.consumer(auto_commit=, commit_every=)", "keywords"),
    ("CommitPolicy", "IntervalOrEvery"): ("Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)", "keywords"),
    ("ConsumerStart", None): ("Topic.consumer(polling=, offset=, timestamp_micros=)", "keywords"),
    ("ConsumerStart", "First"): ("Topic.consumer(polling=)", "keywords"),
    ("ConsumerStart", "Last"): ("Topic.consumer(polling=)", "keywords"),
    ("ConsumerStart", "Next"): ("Topic.consumer(polling=)", "keywords"),
    ("Routing", None): ("Topic.producer(key=, partition=)", "keywords"),
    ("Routing", "Balanced"): ("Topic.producer(key=, partition=)", "keywords"),
    ("Headers", None): ("-", "rust-crate"),
    ("testing::agent_ctx", None): ("fn:agent_ctx(fixed_inbox=)", "keywords"),
    ("DecodeError", None): ("-", "converted-error"),
    ("Value", None): ("-", "native-value"),
    ("AgentTopic", "Custom"): ("-", "plain-value"),
    ("types::ConversationId", "derive"): ("fn:derive_conversation_id", "free-function"),
    ("types::AgentId", "wire_id"): ("-", "plain-value"),
    ("AgentErrorBody", None): ("dict:Agdx.fail", "serde-dict"),
    ("AgentErrorCode", None): ("dict:Agdx.fail", "serde-dict"),
    ("AgentPresence", None): ("dict:Laser.advertise_presence", "serde-dict"),
    ("CapabilityDescriptor", None): ("dict:RouteCandidate.capability", "serde-dict"),
    ("ContentRef", None): ("dict:RouteCandidate.capability", "serde-dict"),
    ("Health", None): ("dict:RouteCandidate.capability", "serde-dict"),
    ("AgentDeadLetter", None): ("dict:Laser.redrive_dead_letter", "serde-dict"),
    ("DeadLetterReason", None): ("dict:Laser.redrive_dead_letter", "serde-dict"),
    ("SignatureContext", None): ("dict:AgentMessage.envelope", "serde-dict"),
    ("TokenUsage", None): ("dict:AgentMessage.envelope", "serde-dict"),
    ("AgentPresence", "new"): ("fn:agent_presence", "free-function"),
    ("AgentPresence", "with_inbox"): ("fn:agent_presence(inbox=)", "keywords"),
    ("AgentPresence", "validate"): ("fn:validate_agent_presence", "free-function"),
    ("AgentErrorCode", "code"): ("-", "plain-value"),
    ("AgentErrorCode", "from_code"): ("-", "plain-value"),
    ("Health", "code"): ("-", "plain-value"),
    ("Health", "from_code"): ("-", "plain-value"),
    ("DeadLetterReason", "code"): ("-", "plain-value"),
    ("DeadLetterReason", "from_code"): ("-", "plain-value"),
    ("Path", None): ("dict:GraphHandle.fetch", "serde-dict"),
    ("BatchItem", None): ("dict:Laser.execute_batch", "serde-dict"),
    ("ArrowIpcMessageMetadata", None): ("dict:PublishRequest.arrow_ipc", "serde-dict"),
    ("MemoryRowScope", None): ("dict:KvEntry.scope", "serde-dict"),
    ("SupervisorActorAssertion", None): ("dict:Destinations.mutate", "serde-dict"),
    ("SupervisorActorClaims", None): ("dict:Destinations.mutate", "serde-dict"),
    ("HeaderValueRef", None): ("-", "plain-value"),
}
TS_PEERS = {
    ("SessionLayout", "per_agent_topic"): ("-", "plain-value"),
    ("SessionLayout", "per_agent_partition"): ("-", "plain-value"),
    ("DisplayType", "as_str"): ("-", "plain-value"),
    ("DynMemory", None): ("Memory", "trait-variant"),
    ("DynManagedKvTransport", None): ("ManagedKvTransport", "trait-variant"),
    ("LaserBuilder", "build"): ("LaserBuilder.connect", "one-call"),
    ("LaserBuilder", "governor_with_retention"): ("LaserBuilder.governor(retention=)", "overload"),
    ("Laser", "with_governor_retention"): ("Laser.withGovernor(retention=)", "overload"),
    ("ProducerBuilder", None): ("ProducerOptions", "keywords"),
    ("ProducerBuilder", "build"): ("Topic.producer", "one-call"),
    ("ConsumerBuilder", None): ("ConsumerOptions", "keywords"),
    ("ConsumerBuilder", "build"): ("Topic.consumer", "one-call"),
    ("FilterPreviewBuilder", None): ("FilterPreviewOptions", "keywords"),
    ("FilterPreviewBuilder", "send"): ("GroupFilter.preview", "one-call"),
    ("CreateConsumerGroup", None): ("CreateConsumerGroupOptions", "keywords"),
    ("CreateConsumerGroup", "build"): ("ConsumerGroup.create", "one-call"),
    ("MemoryQueryBuilder", None): ("MemoryQuery", "keywords"),
    ("MemoryScopeBuilder", None): ("MemoryScope", "keywords"),
    ("LlmUsageBuilder", None): ("LlmUsage", "keywords"),
    ("ProvenanceBuilder", None): ("Provenance", "keywords"),
    ("ProducerMessageBuilder", None): ("ProducerMessage", "keywords"),
    ("RecordBuilder", None): ("Record", "keywords"),
    ("IntentBuilder", None): ("IntentOptions", "keywords"),
    ("ReliableConsumer", "builder"): ("new:ReliableConsumer", "constructor"),
    ("ReliableConsumer", "run"): ("ReliableConsumer.run", "async-runtime"),
    ("ReliableConsumerBuilder", None): ("ReliableConsumerOptions", "keywords"),
    ("ReliableConsumerBuilder", "build"): ("new:ReliableConsumer", "one-call"),
    ("Router", "to"): ("fn:routeTo", "free-function"),
    ("Router", "to_capable"): ("fn:routeToCapable", "free-function"),
    ("Router", "all_capable"): ("fn:routeAllCapable", "free-function"),
    ("Router", "to_principal"): ("fn:routeToPrincipal", "free-function"),
    ("Router", "broadcast"): ("fn:routeBroadcast", "free-function"),
    ("Router", "apply"): ("fn:applyRoute", "free-function"),
    ("Router", "resolve_targets"): ("fn:resolveTargets", "free-function"),
    ("InboxRoute", "resolve"): ("fn:resolveInboxRoute", "free-function"),
    ("RegisteredCard", "available_for"): ("fn:cardAvailableFor", "free-function"),
    ("RegisteredCard", "is_fresh"): ("fn:cardIsFresh", "free-function"),
    ("RegisteredCard", "serves"): ("fn:cardServes", "free-function"),
    ("RetryPolicy", "backoff"): ("fn:retryBackoff", "free-function"),
    ("Gather", "replies"): ("fn:gatherReplies", "free-function"),
    ("CapabilitySelector", "new"): ("fn:capabilitySelector", "free-function"),
    ("SessionPolicy", "conversation_for"): ("fn:conversationFor", "free-function"),
    ("Session", "memory_in"): ("Session.memory(namespace=)", "overload"),
    ("StepFn", "build"): ("StepFn", "callback"),
    ("Verifier", "verify"): ("Verifier", "callback"),
    ("Laser", "bind_roles_expect_revision"): ("Laser.bindRoles(expectRevision=)", "overload"),
    ("A2aBridge", "submit_to"): ("A2aBridge.submit(target=)", "overload"),
    ("A2aBridge", "submit_in_to"): ("A2aBridge.submitIn(target=)", "overload"),
    ("McpBridge", "call_tool_to"): ("McpBridge.callTool(target=)", "overload"),
    ("McpBridge", "call_tool_in_to"): ("McpBridge.callToolIn(target=)", "overload"),
    ("Agdx", "request_input_from"): ("Agdx.requestInput(target=)", "overload"),
    ("LaserError", "code"): ("fn:code", "error-function"),
    ("LaserError", "filter_reason"): ("fn:filterReason", "error-function"),
    ("LaserError", "publish_cause"): ("fn:publishCause", "error-function"),
    ("LaserError", "rejected"): ("new:RejectedError", "constructor"),
    ("LaserError", "unsupported"): ("new:UnsupportedError", "constructor"),
    ("LaserError", "unsupported_feature"): ("new:UnsupportedError", "constructor"),
    ("LaserError", "is_ambiguous_mutation"): ("fn:isAmbiguousMutation", "error-function"),
    ("LaserError", "is_budget_exceeded"): ("fn:isBudgetExceeded", "error-function"),
    ("LaserError", "is_fence_violation"): ("fn:isFenceViolation", "error-function"),
    ("LaserError", "is_lease_lost"): ("fn:isLeaseLost", "error-function"),
    ("LaserError", "is_no_capable_agent"): ("fn:isNoCapableAgent", "error-function"),
    ("LaserError", "is_not_found"): ("fn:isNotFound", "error-function"),
    ("LaserError", "is_not_leader"): ("fn:isNotLeader", "error-function"),
    ("LaserError", "is_permission_denied"): ("fn:isPermissionDenied", "error-function"),
    ("LaserError", "is_quarantined"): ("fn:isQuarantined", "error-function"),
    ("LaserError", "is_retryable"): ("fn:isRetryable", "error-function"),
    ("LaserError", "is_stale"): ("fn:isStale", "error-function"),
    ("LaserError", "is_stream_or_topic_not_found"): ("fn:isStreamOrTopicNotFound", "error-function"),
    ("LaserError", "is_unavailable"): ("fn:isUnavailable", "error-function"),
    ("LaserError", "is_unsupported"): ("fn:isUnsupported", "error-function"),
    ("LaserError", "is_version_conflict"): ("fn:isVersionConflict", "error-function"),
    ("LaserError", "is_version_skew"): ("fn:isVersionSkew", "error-function"),
    ("LaserError", "ConsumerGroupSetup.name"): ("ConsumerGroupSetupError.groupName", "name-collision"),
    ("PublishFailure", None): ("PublishFailedError", "error-class"),
    ("PublishFailure", "source"): ("PublishFailedError.publishCause", "error-class"),
    ("ConsumerBuilder", "without_poll_interval"): ("ConsumerOptions.pollIntervalMs", "overload"),
    ("RecordBuilder", "build"): ("new:Record", "one-call"),
    ("Topic", "iggy_consumer"): ("-", "rust-crate"),
    ("Topic", "iggy_consumer_group"): ("-", "rust-crate"),
    ("Topic", "iggy_producer"): ("-", "rust-crate"),
    ("Consumer", "next"): ("Consumer.stream", "protocol"),
    ("SharedConsolidator", None): ("-", "shared-ownership"),
    ("SharedEmbedder", None): ("-", "shared-ownership"),
    ("SharedReranker", None): ("-", "shared-ownership"),
    ("MaybeEmbedder", None): ("-", "shared-ownership"),
    ("SharedKvTransport", None): ("-", "shared-ownership"),
    ("DefaultConsolidator", None): ("ConsolidateOptions", "keywords"),
    ("NoSummarizer", None): ("ConsolidateOptions.summarizer", "overload"),
    ("TokenBudget", "with_estimator"): ("new:TokenBudget(estimate=)", "overload"),
    ("LogMemory", "in_namespace"): ("new:LogMemory", "overload"),
    ("LogMemory", "on_topic"): ("new:LogMemory(topic=)", "overload"),
    ("LogMemory", "on_topic_named"): ("new:LogMemory(namespace=, topic=)", "overload"),
    ("LogMemory", "on_stream_topic"): ("new:LogMemory(topic=, stream=)", "overload"),
    ("LogMemory", "on_stream_topic_named"): ("new:LogMemory(namespace=, topic=, stream=)", "overload"),
    ("MemoryKind", "class"): ("fn:memoryClass", "free-function"),
    ("Capabilities", "with_query"): ("{QueryCaps}.available", "keywords"),
    ("Capabilities", "with_query_consistency"): ("{QueryCaps}.consistency", "keywords"),
    ("Capabilities", "with_query_keyword"): ("{QueryCaps}.keyword", "keywords"),
    ("Capabilities", "with_query_execution"): ("{QueryCaps}.cursorPaging", "keywords"),
    ("Capabilities", "with_destinations"): ("{DestinationCaps}.available", "keywords"),
    ("Capabilities", "with_destination_consistency"): ("{DestinationCaps}.consistency", "keywords"),
    ("Capabilities", "with_kv"): ("{KvCaps}.available", "keywords"),
    ("Capabilities", "with_kv_cas"): ("{KvCaps}.cas", "keywords"),
    ("Capabilities", "with_kv_fenced_leases"): ("{KvCaps}.fencedLeases", "keywords"),
    ("Capabilities", "with_kv_cas_fenced"): ("{KvCaps}.casFenced", "keywords"),
    ("Capabilities", "with_filters"): ("{FilterCaps}.native", "keywords"),
    ("Capabilities", "OPEN"): ("const:OPEN_CAPABILITIES", "free-function"),
    ("FilterCaps", "evaluates"): ("fn:filterCapsEvaluates", "free-function"),
    ("QueryRequest", "agg_as"): ("~QueryRequest.aggregateAs", None),
    ("QueryRequest", "stddev"): ("~QueryRequest.stdDev", None),
    ("QueryRequest", "raw_sql_with"): ("QueryRequest.rawSql", "overload"),
    ("QueryRequest", "fetch_one_with"): ("QueryRequest.fetchOne", "overload"),
    ("QueryRequest", "fetch_typed_with"): ("QueryRequest.fetchTyped", "overload"),
    ("QueryRows", None): ("QueryRequest.rows", "protocol"),
    ("QueryRows", "next"): ("QueryRequest.rows", "protocol"),
    ("TypedQueryRows", None): ("QueryRequest.rowsTyped", "protocol"),
    ("TypedQueryRows", "next"): ("QueryRequest.rowsTyped", "protocol"),
    ("Filter", "all"): ("fn:filterAll", "free-function"),
    ("Filter", "any"): ("fn:filterAny", "free-function"),
    ("Filter", "negate"): ("fn:filterNegate", "free-function"),
    ("Filter", "pred"): ("fn:filterPred", "free-function"),
    ("ForkHandle", "id"): ("~Fork.forkId", None),
    ("PolicyEvidence", "decode"): ("fn:decodePolicyEvidence", "free-function"),
    ("PolicyEvidence", "encode"): ("fn:encodePolicyEvidence", "free-function"),
    ("AgentTopic", "as_identifier"): ("-", "rust-crate"),
    ("AgentTopic", "name"): ("-", "plain-value"),
    ("AgentTopic", "topic_string"): ("-", "plain-value"),
    ("Provenance", "partition_key"): ("fn:provenancePartitionKey", "free-function"),
    ("KvKeyRegistry", "in_namespace"): ("new:KvKeyRegistry", "overload"),
    ("KvSnapshotStore", "in_namespace"): ("new:KvSnapshotStore", "overload"),
    ("TopicSnapshotStore", "on_topic"): ("new:TopicSnapshotStore", "overload"),
    ("A2aBridge", "router"): ("-", "rust-crate"),
    ("McpBridge", "router"): ("-", "rust-crate"),
    ("CrashContext", "assemble"): ("new:CrashContext", "constructor"),
    ("AgentEnvelope", "event"): ("fn:eventEnvelope", "free-function"),
    ("TaskState", "from_code"): ("fn:taskStateFromCode", "free-function"),
    ("KeyRecord", "revoked"): ("KeyRecord.revoke", "name-collision"),
    ("CompiledSchema", "Avro"): ("CompiledSchemaKind", "plain-value"),
    ("CompiledSchema", "Protobuf"): ("CompiledSchemaKind", "plain-value"),
    ("CompiledSchema", "Json"): ("CompiledSchemaKind", "plain-value"),
    ("MemoryHandle", "Log"): ("MemoryBackendKind", "plain-value"),
    ("MemoryHandle", "Vector"): ("MemoryBackendKind", "plain-value"),
    ("MemoryHandle", "Reranked"): ("MemoryBackendKind", "plain-value"),
    ("MemoryHandle", "Custom"): ("MemoryBackendKind", "plain-value"),
    ("MemoryKind", "code"): ("fn:memoryKindCode", "free-function"),
    ("ProducerMessageBuilder", "build"): ("new:ProducerMessage", "one-call"),
    ("LaserError", "iggy_error_code"): ("fn:iggyErrorCode", "error-function"),
    ("GraphNode", "entity"): ("fn:graphNodeEntity", "free-function"),
    ("GraphEdge", "relate"): ("fn:graphEdgeRelate", "free-function"),
    ("AgentEnvelope", "requiring"): ("fn:requiring", "free-function"),
    ("ProjectionId", "new"): ("fn:parseProjectionId", "free-function"),
    ("ResourcePattern", "all"): ("fn:resourcePatternAll", "free-function"),
    ("ResourcePattern", "literal"): ("fn:resourcePatternLiteral", "free-function"),
    ("ResourcePattern", "prefix"): ("fn:resourcePatternPrefix", "free-function"),
    ("ProvenanceBuilder", "deadline"): ("Provenance.deadlineMicros", "keywords"),
    ("IntentId", "new"): ("IntentId.new", None),
    ("MemoryId", "new"): ("MemoryId.new", None),
    ("filters::Verdict", None): ("FilterVerdict", "flat-namespace"),
    ("IntentBuilder", "build"): ("new:Intent", "one-call"),
    ("GraphEdge", "valid"): ("fn:graphEdgeValid", "free-function"),
    ("GraphEdge", "with_source"): ("fn:graphEdgeWithSource", "free-function"),
    ("LogPosition", "new"): ("fn:newLogPosition", "free-function"),
    ("ScopedMemory", "consolidate_with"): ("ScopedMemory.consolidate", "overload"),
    ("MemoryHandle", "consolidate_with"): ("MemoryHandle.consolidate", "overload"),
    ("IndexField", "typed"): ("IndexField", "keywords"),
    ("ProjectionKind", "from_code"): ("fn:projectionKindFromCode", "free-function"),
    ("Query", "operational"): ("fn:operationalQuery", "free-function"),
    ("Query", "builder"): ("new:QueryBuilder", "constructor"),
    ("QueryTarget", "operational"): ("fn:operationalTarget", "free-function"),
    ("ResultCode", "from_code"): ("fn:resultCodeFromCode", "free-function"),
    ("ContentType", "code"): ("fn:contentTypeCode", "free-function"),
    ("ContentType", "is_raw"): ("fn:isRawContentType", "free-function"),
    ("Value", "from_input"): ("fn:valueFromInput", "free-function"),
    ("AgentEnvelope", "command"): ("fn:commandEnvelope", "free-function"),
    ("AgentEnvelope", "response"): ("fn:responseEnvelope", "free-function"),
    ("AgentEnvelope", "chunk"): ("fn:chunkEnvelope", "free-function"),
    ("AgentEnvelope", "status"): ("fn:statusEnvelope", "free-function"),
    ("AgentEnvelope", "error"): ("fn:errorEnvelope", "free-function"),
    ("AgentEnvelope", "terminal"): ("fn:terminal", "free-function"),
    ("Signature", "validate"): ("fn:validateSignature", "free-function"),
    ("BackendReadiness", "not_ready"): ("fn:backendReadinessNotReady", "free-function"),
    ("BackendDescriptor", "with_state"): ("BackendDescriptor.desiredState", "keywords"),
    ("MemoryItem", "text"): ("fn:memoryItemText", "free-function"),
    ("MemoryItem", "json"): ("fn:memoryItemJson", "free-function"),
    ("AppliedPolicy", "filtered"): ("fn:appliedPolicyFiltered", "free-function"),
    ("AppliedPolicy", "unfiltered"): ("fn:appliedPolicyUnfiltered", "free-function"),
    ("FilterError", "invalid"): ("fn:filterErrorInvalid", "free-function"),
    ("FilterExpr", "case_insensitive"): ("fn:filterExprCaseInsensitive", "free-function"),
    ("FilterExpr", "reads_payload"): ("fn:filterExprReadsPayload", "free-function"),
    ("FilterExpr", "reads_headers"): ("fn:filterExprReadsHeaders", "free-function"),
    ("CompiledFilter", "compile_with_schemas"): ("CompiledFilter.compile(schemas=)", "overload"),
    ("IndexSchema", "builder"): ("new:IndexSchemaBuilder", "constructor"),
    ("Projection", "builder"): ("new:ProjectionBuilder", "constructor"),
    ("ProjectionBinding", "builder"): ("new:ProjectionBindingBuilder", "constructor"),
    ("BackgroundConfig", None): ("ProducerBackgroundOptions", "keywords"),
    ("FilterAnnounce", "served"): ("fn:filterAnnounceServed", "free-function"),
    ("Laser", "sessions_with"): ("Laser.sessions(config=)", "overload"),
    ("DecodeError", None): ("-", "converted-error"),
    ("types::ConversationId", "new"): ("ConversationId.new", None),
    ("AgentErrorCode", "from_code"): ("fn:agentErrorCodeFromCode", "free-function"),
    ("AgentErrorCode", "code"): ("fn:agentErrorCode", "free-function"),
    ("Health", "from_code"): ("fn:healthFromCode", "free-function"),
    ("Health", "code"): ("fn:healthCode", "free-function"),
    ("DeadLetterReason", "from_code"): ("fn:deadLetterReasonFromCode", "free-function"),
    ("DeadLetterReason", "code"): ("fn:deadLetterReasonCode", "free-function"),
    ("AgentPresence", "new"): ("fn:newAgentPresence", "free-function"),
    ("AgentPresence", "with_inbox"): ("fn:newAgentPresence(inbox=)", "keywords"),
    ("AgentPresence", "validate"): ("fn:validateAgentPresence", "free-function"),
    ("LogPosition", "from_bytes"): ("fn:logPositionFromBytes", "free-function"),
    ("LogPosition", "to_bytes"): ("fn:logPositionToBytes", "free-function"),
    ("SupervisorActorAssertion", "validate"): ("fn:validateSupervisorAssertion", "free-function"),
    ("snapshot::ConversationId", None): ("wire.ConversationId", None),
}

# Peer APIs with no Rust row of their own, each kept for a language reason.
PY_ONLY = {
    "AmbiguousMutationRecovery.ttl_ms": "property",
    "KvStore": "native-binding",
    "Consumer.init": "lazy-init",
    "Producer.init": "lazy-init",
    "ConsumerMessage.header_kinds": "rust-crate",
    "encode_context_manifest": "native-binding",
    "decode_context_manifest": "native-binding",
    "encode_context_compaction": "native-binding",
    "decode_context_compaction": "native-binding",
    "encode_context_retrieval": "native-binding",
    "decode_context_retrieval": "native-binding",
    "encode_state_delta": "native-binding",
    "decode_state_delta": "native-binding",
    "encode_state_snapshot": "native-binding",
    "decode_state_snapshot": "native-binding",
    "apply_json_patch": "native-binding",
    "encode_session_get": "native-binding",
    "decode_session_get": "native-binding",
    "encode_session_list": "native-binding",
    "decode_session_list": "native-binding",
    "encode_session_events": "native-binding",
    "decode_session_events": "native-binding",
    "encode_session_state": "native-binding",
    "decode_session_state": "native-binding",
    "encode_session_start": "native-binding",
    "decode_session_start": "native-binding",
    "encode_session_transition": "native-binding",
    "decode_session_transition": "native-binding",
    "encode_session_end": "native-binding",
    "decode_session_end": "native-binding",
    "encode_session_links": "native-binding",
    "decode_session_links": "native-binding",
    "encode_session_sources": "native-binding",
    "decode_session_sources": "native-binding",
    "encode_session_changes": "native-binding",
    "decode_session_changes": "native-binding",
    "encode_session_reply": "native-binding",
    "decode_session_reply": "native-binding",
}
TS_ONLY = {
    "Laser.withObserver": "telemetry",
    "LaserBuilder.observer": "telemetry",
    "LaserObserver": "telemetry",
    "NOOP_OBSERVER": "telemetry",
    "ObservationLevel": "telemetry",
    "ObserveEffect": "telemetry",
    "SpanScope": "telemetry",
    "OpenTelemetryObserver": "telemetry",
    "OpenTelemetrySpan": "telemetry",
    "OpenTelemetryTracer": "telemetry",
    "parseMessageId": "std-trait",
    "messageIdToString": "std-trait",
    "MintUlid.fromU128": "std-trait",
    "parseIdempotencyKey": "std-trait",
    "KvStore": "callback",
}

# The error class each LaserError variant raises, when it is not `<Variant>Error`.
# Rust keeps a variant and its payload type in separate namespaces, so
# `LaserError::Query(QueryError)` needs no second name. TypeScript exports the
# payload type and the error class side by side, so the class takes an
# `ExecutionError` suffix. No peer binds the Apache Iggy client error, so its
# variant raises the transport error. A `~` class is a gap that names where
# the variant is raised today.
PY_ERROR_CLASSES = {"Iggy": "TransportError"}
TS_ERROR_CLASSES = {
    "Iggy": "TransportError",
    "Query": "QueryExecutionError",
    "Kv": "KvExecutionError",
    "Fork": "ForkExecutionError",
    "Graph": "GraphExecutionError",
    "Authz": "AuthzExecutionError",
    "Filter": "FilterExecutionError",
    "Checkpoint": "CheckpointExecutionError",
    "FilterFault": "FilterFaultError",
    "FilterOversizedRecord": "FilterOversizedRecordError",
    "NoCapableAgent": "NoCapableAgentError",
    "NoInbox": "NoInboxError",
    "RoutePrincipalMismatch": "RoutePrincipalMismatchError",
}


def main():
    text, problems = check()
    if "--write" in sys.argv:
        DOC.write_text(text)
    elif not DOC.exists() or DOC.read_text() != text:
        problems.insert(0, "docs/parity.md is stale, run `python3 scripts/check-parity.py --write`")
    for problem in problems:
        print(problem)
    if problems:
        print(f"{len(problems)} parity problem(s).")
        return 1
    print("parity ok")
    return 0


def check():
    """The rendered matrix and every parity problem."""
    rows = rust_surface()
    matrix = compare(rows, python_surface(), typescript_surface())
    return render(matrix), matrix.problems


def rust_surface():
    """Public rows of `laser_sdk`, in module order."""
    crates = {"laser_sdk": load_crate("laser_sdk", ROOT / "sdk" / "src"), "laser_wire": load_crate("laser_wire", ROOT / "wire" / "src")}
    resolver = Resolver(crates)
    reached = resolver.public_items("laser_sdk")
    counts = {}
    for _, name, _ in reached:
        counts[name] = counts.get(name, 0) + 1
    displays = {id(item): (f"{section}::{name}" if counts[name] > 1 else name) for section, name, item in reached}
    rows = []
    for section, name, item in reached:
        rows += item_rows(section, displays[id(item)], item, resolver, displays)
    return rows


def python_surface():
    """Classes, functions, and constants of the Python stub."""
    tree = ast.parse((ROOT.joinpath(*STUB)).read_text())
    surface = PySurface()
    exported = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            exported = set(ast.literal_eval(node.value))
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            surface.classes[node.name] = py_class(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            merge_callable(surface.functions, node.name, py_params(node))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            surface.constants.add(node.target.id)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id != "__all__":
                    surface.constants.add(target.id)
    if exported is None:
        exported = set(surface.classes) | set(surface.functions) | surface.constants
    surface.exported = {name for name in exported if not name.startswith("_")}
    surface.native_types, surface.native_methods = native_python_bindings(surface)
    return surface


def native_python_bindings(surface):
    """Native holder types and direct delegations in the binding source."""
    root = ROOT / "foreign" / "python" / "src"
    if not (root / "lib.rs").exists():
        return {}, {}
    types, holders, methods = {}, {}, {}
    modules = load_crate("python", root)
    containers = {"Arc", "Box", "Mutex", "RwLock", "Option", "Held", "dyn"}
    for module in modules.values():
        for item in module.items:
            if item.kind != "struct":
                continue
            declared = next((re.search(r'pyclass\s*\(.*?name\s*=\s*"(\w+)"', attr) for attr in item.attrs if "pyclass" in attr), None)
            if declared is None or declared.group(1) not in surface.classes:
                continue
            inner = next((field for field in item.fields if field.name == "inner"), None)
            if inner is None:
                continue
            names = [name for name in re.findall(r"\b([A-Z]\w*)", inner.type) if name not in containers]
            if not names:
                continue
            native = names[0]
            peer = declared.group(1)
            holders[item.name] = (native, peer)
            types.setdefault(native, peer)
    for module in modules.values():
        text = (ROOT / module.file).read_text()
        code = strip_rust(text)
        for block in module.items:
            if block.kind != "impl" or block.owner not in holders:
                continue
            native, peer = holders[block.owner]
            for method in block.items:
                if method.kind != "fn" or method.name not in surface.classes[peer].members:
                    continue
                body = code[method.span[0]:method.span[1]]
                references = {"self.inner"}
                references.update(re.findall(r"let\s+(?:mut\s+)?(\w+)\s*=\s*self\.inner\s*\.(?:clone|get)\s*\(", body))
                for reference in references:
                    for call in re.finditer(re.escape(reference) + r"\s*\.\s*(\w+)\s*(?:\(\)|)?\s*\(", body):
                        name = call.group(1)
                        if name not in ("clone", "get"):
                            methods.setdefault((native, name), f"{peer}.{method.name}")
    return types, methods


def typescript_surface():
    """Exported declarations of every TypeScript API report."""
    surface = TsSurface()
    for report in REPORTS:
        path = ROOT / "foreign" / "typescript" / "api" / report
        if path.exists():
            parse_report(path.read_text(), surface)
    return surface


def compare(rows, python, typescript):
    """Resolve every Rust row in both peers and collect the problems."""
    matrix = Matrix()
    by_key = {(row.owner, row.member): row for row in rows}
    shapes = {}
    for row in rows:
        if row.member is not None and (row.kind in ("field", "setter", "variant") or row.returns in ("Self", row.owner)):
            shapes.setdefault(row.owner, set()).update([snake(row.member)] + getattr(row, "variant_fields", []))
    py = Lookup("Python", python, PY_PEERS, PY_ERROR_CLASSES, matrix.py_claims, shapes)
    ts = Lookup("TypeScript", typescript, TS_PEERS, TS_ERROR_CLASSES, matrix.ts_claims, shapes)
    for row in sorted(rows, key=lambda row: (row.member is not None, row.builder is not None, row.setter_of is not None)):
        row.python = resolve_row(row, py, by_key)
        row.typescript = resolve_row(row, ts, by_key)
        row.note = ", ".join(dict.fromkeys(reason for reason in (row.python.reason, row.typescript.reason) if reason))
    matrix.rows = rows
    for row in rows:
        if row.setter_of:
            continue
        for label, attribute in (("Python", "python"), ("TypeScript", "typescript")):
            cell = getattr(row, attribute)
            parent = by_key.get((row.owner, None))
            parent_cell = getattr(parent, attribute) if parent is not None and row.member is not None else None
            if cell.mirror or (cell.gap and parent_cell is not None and parent_cell.gap and not parent_cell.hint):
                continue
            if cell.gap:
                folded = 0
                if row.member is None:
                    folded = sum(1 for other in rows if other.owner == row.owner and other.member is not None and getattr(other, attribute).gap)
                suffix = f" and {folded} member(s)" if folded else ""
                matrix.problems.append(f"{row.display}: {label} {cell.problem}{suffix} ({row.source})")
            elif cell.param_problem:
                matrix.problems.append(f"{row.display}: {label} `{cell.spelling}` {cell.param_problem} ({row.source})")
    for label, table in (("PY_PEERS", PY_PEERS), ("TS_PEERS", TS_PEERS)):
        for key in table:
            if key not in by_key:
                matrix.problems.append(f"{label} entry {key} names no public Rust item")
    for label, table in (("PY_ERROR_CLASSES", PY_ERROR_CLASSES), ("TS_ERROR_CLASSES", TS_ERROR_CLASSES)):
        for variant in table:
            if ("LaserError", variant) not in by_key:
                matrix.problems.append(f"{label} entry {variant} names no LaserError variant")
    for row in rows:
        for trait in row.traits:
            for member in (other for other in rows if other.owner == trait and other.member is not None):
                for lookup in (py, ts):
                    host = member_host(row.owner, lookup, by_key)
                    if host.cls and lookup.has_type(host.cls):
                        found = lookup.members(host.cls).get(lookup.name(member.member))
                        if found:
                            lookup.claims.add(f"{found[0]}.{lookup.name(member.member)}")
    for row in rows:
        if row.kind != "variant" or not getattr(row, "variant_fields", None):
            continue
        for lookup, cell in ((py, row.python), (ts, row.typescript)):
            host = member_host(row.owner, lookup, by_key)
            if cell.gap or not host.cls or not lookup.has_type(host.cls):
                continue
            # A variant's payload fields read as getters of the class that hosts the enum.
            for field in row.variant_fields:
                found = lookup.members(host.cls).get(lookup.name(field))
                if found:
                    lookup.claims.add(f"{found[0]}.{lookup.name(field)}")
    matrix.python_only = python_only(python, matrix.py_claims)
    matrix.typescript_only = typescript_only(typescript, matrix.ts_claims)
    for label, table, found in (("PY_ONLY", PY_ONLY, matrix.python_only), ("TS_ONLY", TS_ONLY, matrix.typescript_only)):
        for name, reason in table.items():
            if reason not in REASONS:
                matrix.problems.append(f"{label} entry {name} has unknown reason `{reason}`")
            elif name not in found and f"{name}()" not in found:
                matrix.problems.append(f"{label} entry {name} names no unclaimed peer API")
        matrix.accepted += [(label[:2], name, table[name.rstrip("()")]) for name in found if name.rstrip("()") in table]
    matrix.python_only = [name for name in matrix.python_only if name.rstrip("()") not in PY_ONLY]
    matrix.typescript_only = [name for name in matrix.typescript_only if name.rstrip("()") not in TS_ONLY]
    matrix.problems += [f"Python only: {name} has no Rust row" for name in matrix.python_only]
    matrix.problems += [f"TypeScript only: {name} has no Rust row" for name in matrix.typescript_only]
    return matrix


def render(matrix):
    """The docs/parity.md text."""
    lines = [
        "# Rust, Python, and TypeScript parity",
        "",
        "This matrix lists every public item of the Rust `laser_sdk` crate, including the items it re-exports from `laser_wire` and the types its public methods hand out, with its Python and TypeScript spelling. Types, struct fields, enum variants, LaserError variants and their payload fields, methods, generated builders and their setters, associated constants, free functions, and module constants each get a row. `scripts/check-parity.py --write` generates it from the Rust sources, the Python stub, and the TypeScript API reports. `just parity-check` fails when the committed file is stale, when a row says MISSING, when parameters differ, or when a Python or TypeScript API has no Rust row. Python uses snake_case and TypeScript uses camelCase for the same name.",
        "",
        "A peer may differ from the Rust spelling only for one of the reasons below. Any other difference is a gap. A MISSING cell names where the capability lives today when the peer has it under another name or shape.",
        "",
        "| Note | Reason |",
        "| --- | --- |",
    ]
    lines += [f"| {key} | {text} |" for key, (text, _) in REASONS.items()]
    section = None
    for row in matrix.rows:
        if row.section != section:
            section = row.section
            lines += ["", f"## {section or 'laser_sdk'}", "", "| Rust | Python | TypeScript | Notes |", "| --- | --- | --- | --- |"]
        lines.append(f"| `{row.display}` | {row.python.show()} | {row.typescript.show()} | {row.note} |".rstrip())
    if matrix.accepted:
        lines += ["", "## Peer-only APIs", "", "These Python and TypeScript APIs have no Rust row for a language reason.", "", "| Client | API | Note |", "| --- | --- | --- |"]
        lines += [f"| {'Python' if client == 'PY' else 'TypeScript'} | `{name}` | {reason} |" for client, name, reason in matrix.accepted]
    for title, names in (("Python only", matrix.python_only), ("TypeScript only", matrix.typescript_only)):
        if names:
            lines += ["", f"## {title}", "", f"These {title.split()[0]} APIs have no Rust row.", ""]
            lines += [f"- `{name}`" for name in names]
    return "\n".join(lines).rstrip() + "\n"


class Row:
    """One public Rust item or member and its resolved peers."""

    def __init__(self, section, owner, member, kind, source, **extra):
        self.section = section
        self.owner = owner
        self.member = member
        self.kind = kind
        self.source = source
        self.name = member
        self.params = []
        self.returns = ""
        self.receiver = False
        self.setter_of = None
        self.finish = False
        self.start_of = None
        self.value_type = ""
        self.literal = False
        self.tuple = False
        self.positional = False
        self.traits = []
        self.plain = False
        self.builder = None
        self.variant_of = None
        self.serde = False
        self.skipped = False
        self.python = self.typescript = None
        self.note = ""
        self.__dict__.update(extra)

    @property
    def display(self):
        if self.member is None:
            return self.owner
        separator = "." if self.kind in ("field", "variant-field") else "::"
        return f"{self.owner}{separator}{self.member}"


class Cell:
    """One resolved peer spelling."""

    def __init__(self, spelling, reason=None, gap=False, problem="", omitted=False, hint=None):
        self.spelling = spelling
        self.reason = reason
        self.gap = gap
        self.problem = problem
        self.omitted = omitted
        self.hint = hint
        self.param_problem = None
        self.mirror = False

    def show(self):
        if self.omitted:
            return "omitted"
        if self.gap:
            return f"{MISSING} (today {show_spelling(self.hint)})" if self.hint else MISSING
        return show_spelling(self.spelling)


class Matrix:
    def __init__(self):
        self.rows = []
        self.problems = []
        self.py_claims = set()
        self.ts_claims = set()
        self.python_only = []
        self.typescript_only = []
        self.accepted = []


class PySurface:
    def __init__(self):
        self.classes = {}
        self.functions = {}
        self.constants = set()
        self.exported = set()
        self.native_types = {}
        self.native_methods = {}


class PyClass:
    def __init__(self, name, bases):
        self.name = name
        self.bases = bases
        self.members = {}
        self.constructor = None


class TsSurface:
    def __init__(self):
        self.decls = {}
        self.functions = {}
        self.wire = set()
        self.wire_aliases = {}
        self.hidden = {}


class TsDecl:
    def __init__(self, kind, name, text):
        self.kind = kind
        self.name = name
        self.text = text
        self.members = {}
        self.constructor = None
        self.abstract = False
        self.base = None
        self.bases = []


class Host:
    """Where the members of one Rust type live in a peer."""

    def __init__(self, cls=None, call=None, omitted=None, callback=None, keywords=False, literal=False, words=False, serde=False):
        self.serde = serde
        self.literal = literal
        self.words = words
        self.keywords_reason = False
        self.cls = cls
        self.call = call
        self.omitted = omitted
        self.callback = callback
        self.keywords = keywords


class Lookup:
    """Spelling resolution against one peer surface."""

    def __init__(self, label, surface, peers, errors, claims, shapes):
        self.label = label
        self.surface = surface
        self.peers = peers
        self.errors = errors
        self.claims = claims
        self.shapes = shapes
        self.python = isinstance(surface, PySurface)

    def name(self, rust_name):
        return rust_name if self.python else camel(rust_name)

    def has_type(self, name):
        if self.python:
            return name in self.surface.classes or name in self.surface.constants
        return name in self.surface.decls

    def type_kind(self, name):
        if self.python:
            return "class" if name in self.surface.classes else None
        decl = self.surface.decls.get(name)
        return decl.kind if decl else None

    def type_name(self, name):
        """The peer type for a Rust type name. TypeScript falls back to its wire namespace."""
        if not self.python and name not in self.surface.decls and f"wire.{name}" in self.surface.decls:
            return f"wire.{name}"
        return name

    def members(self, cls):
        """Members of a class including inherited ones, by name."""
        out = {}
        if self.python:
            chain, seen = [cls], set()
            while chain:
                current = chain.pop(0)
                if current in seen or current not in self.surface.classes:
                    continue
                seen.add(current)
                py_class = self.surface.classes[current]
                for name, member in py_class.members.items():
                    out.setdefault(name, (current, member))
                chain += py_class.bases
            return out
        chain, seen = [cls], set()
        while chain:
            current = chain.pop(0)
            decl = self.surface.decls.get(current) or (self.surface.hidden.get(current) if current != cls else None)
            if current in seen or decl is None:
                continue
            seen.add(current)
            for name, member in decl.members.items():
                out.setdefault(name, (current, member))
            chain += decl.bases
        return out

    def constructor(self, cls):
        """Constructor parameters of a class, or None when it has no public one."""
        if self.python:
            chain, seen = [cls], set()
            while chain:
                current = chain.pop(0)
                if current in seen:
                    continue
                seen.add(current)
                if current in ("Exception", "builtins.Exception") or current.startswith(("builtins.", "asyncio.")):
                    return [{"name": "*", "rest": True}]
                py_class = self.surface.classes.get(current)
                if py_class is None:
                    continue
                if py_class.constructor is not None:
                    return py_class.constructor
                chain += py_class.bases
            return None
        decl = self.surface.decls.get(cls)
        if decl is None or decl.kind != "class" or decl.abstract:
            return None
        current, seen = decl, set()
        while current is not None and current.name not in seen:
            seen.add(current.name)
            if current.constructor is not None:
                return current.constructor if current.constructor != "hidden" else None
            current = (self.surface.decls.get(current.base) or self.surface.hidden.get(current.base)) if current.base else None
        return []

    def resolve(self, spelling):
        """(found, keywords accepted, params, member info) for a spelling. Claims what it finds."""
        if spelling.startswith("new:"):
            cls, kws = split_call(spelling[4:])
            params = self.constructor(cls) if self.has_type(cls) else None
            if params is None:
                return False, False, None, None
            self.claims.update({cls, f"{cls}.constructor"})
            return True, self.accepts(params, kws), params, None
        if spelling.startswith("fn:"):
            name, kws = split_call(spelling[3:])
            params = self.surface.functions.get(name)
            if params is None:
                return False, False, None, None
            self.claims.add(f"fn:{name}")
            return True, self.accepts(params, kws), params, None
        if spelling.startswith("const:"):
            name = spelling[6:]
            if not self.python:
                name = self.type_name(name)
            found = name in self.surface.constants if self.python else name in self.surface.decls and self.surface.decls[name].kind == "const"
            if found:
                self.claims.add(name)
            return found, found, None, None
        target, kws = split_call(spelling)
        if "." not in target or (target.startswith("wire.") and target.count(".") == 1):
            if self.has_type(target):
                self.claims.add(target)
                return True, not kws, None, None
            return False, False, None, None
        cls, member = target.rsplit(".", 1)
        if not self.has_type(cls):
            return False, False, None, None
        found = self.members(cls).get(member)
        if found is None:
            return False, False, None, None
        owner, info = found
        self.claims.update({cls, f"{owner}.{member}"})
        params = info.get("params")
        return True, self.accepts(params, kws), params, info

    def accepts(self, params, kws):
        if not kws:
            return True
        if params is None:
            return False
        if any(param.get("rest") or param.get("options") for param in params) and not self.python:
            return True
        names = {param["name"] for param in params}
        return all(kw in names for kw in kws)


def resolve_row(row, lookup, by_key):
    """The peer cell for one row.

    A PEERS entry comes first and the conventional spellings after it, so a
    stale entry falls back to the Rust name and a peer that adopts it passes
    without a table edit. An omission entry comes last, so it never hides a
    peer API. A spelling that resolves with matching parameters wins over one
    that resolves with different ones. A `~` entry only names where a missing
    capability lives today.
    """
    entry = lookup.peers.get((row.owner, row.member))
    if lookup.python:
        native = row.owner.rsplit("::", 1)[-1]
        if row.member is None and row.kind in TYPE_KINDS and native in lookup.surface.native_types:
            if not entry or entry[0].startswith("~"):
                entry = (lookup.surface.native_types[native], "native-binding")
        elif row.kind == "method" and (native, row.member) in lookup.surface.native_methods:
            if not entry or entry[0].startswith("~"):
                entry = (lookup.surface.native_methods[(native, row.member)], "native-binding")
    if entry is not None and entry[1] == "name-collision" and row.kind == "field":
        # The entry renames the method that shares this field's name. The field keeps its own.
        entry = None
    hint = entry if entry is not None and entry[0] and entry[0].startswith("~") else None
    if hint:
        entry = None
    if row.setter_of and entry is None:
        base = by_key.get((row.owner, row.setter_of))
        if base is not None:
            cell = base.python if lookup.python else base.typescript
            return Cell(cell.spelling, cell.reason, cell.gap, cell.problem, cell.omitted, cell.hint)
    defaults = default_spellings(row, lookup, by_key)
    if row.kind == "field" and row.builder and entry is None and hint is None and defaults:
        setter = by_key.get((row.builder, row.member))
        cell = None if setter is None else setter.python if lookup.python else setter.typescript
        if cell is not None and resolve_cell(row, lookup, substitute(defaults[0][0], lookup, by_key), defaults[0][1]).gap:
            mirror = Cell(cell.spelling, cell.reason or "keywords", cell.gap, cell.problem, cell.omitted, cell.hint)
            mirror.mirror = True
            return mirror
    first = fallback = None
    ordered = defaults + [entry] if entry and entry[0] == "-" else ([entry] if entry else []) + defaults
    for spelling, reason in ordered:
        if spelling is None:
            continue
        cell = resolve_cell(row, lookup, substitute(spelling, lookup, by_key), reason)
        if not cell.gap and not cell.param_problem:
            claim_constructor(row, lookup, cell)
            return cell
        if not cell.gap:
            fallback = fallback or cell
        elif first is None or (entry and spelling == entry[0]):
            first = cell
    if fallback:
        claim_constructor(row, lookup, fallback)
        return fallback
    if hint:
        return resolve_cell(row, lookup, substitute(hint[0], lookup, by_key), hint[1])
    return first or Cell("", gap=True, problem=MISSING)


def claim_constructor(row, lookup, cell):
    """A struct literal or a LaserError variant is the peer constructor of the
    class it resolves to. A peer class for a Rust enum reads the variant from
    its `kind` discriminator where Rust matches, and an error class carries a
    tuple variant's payload as `detail`. The members of a peer class that an
    Apache Iggy re-export resolves to have no Rust rows to answer for."""
    if (row.member is None and row.literal) or (row.owner == "LaserError" and row.kind == "variant"):
        lookup.claims.add(f"{cell.spelling}.constructor")
    if row.owner == "LaserError" and row.kind == "variant" and row.tuple:
        lookup.claims.add(f"{cell.spelling}.detail")
    if row.kind == "extern" or (row.member is None and row.positional):
        lookup.claims.update(f"{owner}.{member}" for member, (owner, info) in lookup.members(cell.spelling).items() if row.kind == "extern" or info.get("kind") != "method")
    if row.member is None and row.kind == "enum":
        lookup.claims.add(f"{cell.spelling}.kind")


def resolve_cell(row, lookup, spelling, reason):
    if reason is not None and reason not in REASONS:
        return Cell(spelling, gap=True, problem=f"unknown reason `{reason}`")
    if spelling.startswith("~"):
        lookup.resolve(spelling[1:])
        return Cell(spelling[1:], reason, gap=True, problem=f"{MISSING}, today {show_spelling(spelling[1:])}", hint=spelling[1:])
    if reason == "serde-dict" or spelling == "key" or spelling.startswith("dict:"):
        return serde_cell(row, lookup, spelling, reason)
    if spelling == "-":
        if reason is None or not REASONS[reason][1]:
            return Cell("-", reason, gap=True, problem=f"omitted without an omission reason ({reason})")
        return Cell("-", reason, omitted=True)
    found, accepted, params, info = lookup.resolve(spelling)
    if not found:
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}")
    if not accepted:
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, a keyword or option is absent")
    if reason == "free-function" and lookup.type_kind(row.owner.rsplit("::", 1)[-1]) == "class":
        return Cell(spelling, reason, gap=True, problem=f"{MISSING}, free function {show_spelling(spelling)} where the peer type is a class")
    cell = Cell(spelling, reason)
    if row.kind == "method" and not row.params and reason is None and info and info.get("kind") != "method":
        cell.reason = "property"
    if row.kind in ("method", "fn", "setter") and not row.start_of and params is not None and not split_call(spelling)[1] and reason not in ("callback", "keywords", "one-call", "async-runtime", "overload", "native-binding"):
        cell.param_problem = param_mismatch(row, params, lookup, reason)
    return cell


def serde_cell(row, lookup, spelling, reason):
    """A Python dict peer. The type row names the Python API that hands out or
    takes the dict, the Rust type must derive serde, and its fields and
    variants are the dict keys and tags unless serde skips them."""
    if not lookup.python or reason != "serde-dict":
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, a serde dict is a Python representation with the `serde-dict` reason")
    if not row.serde:
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, the Rust type derives no serde Serialize or Deserialize")
    if spelling == "key":
        if row.member is None or row.kind not in ("field", "variant") or row.skipped:
            return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, serde writes no key for it")
        return Cell(spelling, reason)
    if row.member is not None or not spelling.startswith("dict:"):
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, only a type row names a serde dict API")
    api = spelling[5:]
    found, _, _, _ = lookup.resolve(api)
    if not found:
        return Cell(spelling, reason, gap=True, problem=f"{MISSING} {show_spelling(spelling)}, the Python API is absent")
    return Cell(spelling, reason)


def default_spellings(row, lookup, by_key):
    """Conventional peer spellings for a row, in order, each with the reason that permits it."""
    name = lookup.name
    if row.member is None:
        if row.kind == "fn":
            module = row.section.rsplit("::", 1)[-1]
            return [(f"fn:{name(row.name)}", None), (f"fn:{name(row.name + '_' + module)}", "flat-namespace"), (f"fn:{name(module + '_' + row.name)}", "flat-namespace")]
        if row.kind in ("const", "static"):
            out = [(f"const:{row.name}", None)]
            if re.search(r"\bDuration\b", row.value_type):
                out += [(f"const:{row.name}_{unit.upper()}", None) for unit in UNITS]
            return out
        short = row.owner.rsplit("::", 1)[-1]
        out = [(lookup.type_name(short), None)]
        if row.variant_of:
            out.append((lookup.type_name(row.variant_of), "trait-variant"))
        if row.kind == "extern":
            out.append(("-", "rust-crate"))
        elif row.plain and lookup.python:
            out.append(("-", "plain-value"))
        return out
    host = member_host(row.owner, lookup, by_key)
    if host.omitted:
        owner = snake(row.owner.rsplit("::", 1)[-1])
        if row.kind == "assoc-const":
            return [(f"const:{owner.upper()}_{row.member}", "free-function")]
        out = [(f"fn:{name(row.member + '_' + owner)}", "free-function"), (f"fn:{name(owner + '_' + row.member)}", "free-function")]
        if row.kind in ("variant", "variant-field"):
            return [("-", host.omitted)]
        if CONVERSIONS.match(row.member):
            out.append(("-", host.omitted))
        return out
    if host.words and row.kind == "method" and CONVERSIONS.match(row.member):
        return [(f"{host.cls}.{name(row.member)}", None), ("-", "plain-value")]
    if host.callback:
        return [(host.callback, "callback")]
    if host.serde:
        if row.kind in ("field", "variant"):
            return [("key", "serde-dict")]
        owner = snake(row.owner.rsplit("::", 1)[-1])
        return [(f"fn:{name(row.member + '_' + owner)}", "free-function"), (f"fn:{name(owner + '_' + row.member)}", "free-function")]
    if row.kind == "variant-field":
        variant, field = row.member.split(".", 1)
        error = error_class(variant, lookup)
        if error.startswith("~"):
            return [(error, None)]
        out = [(f"{error}.{name(field)}", "error-class")]
        if field in ("source", "message"):
            out.append((error, "protocol"))
        return out
    if row.kind == "variant":
        if row.owner == "LaserError":
            return [(error_class(row.member, lookup), "error-class")]
        if host.call:
            return [(f"{host.call}({name(snake(row.member))}=)", "keywords")]
        return [variant_spelling(row, lookup, host.cls)] if host.cls else []
    names = member_names(row, lookup, host.keywords or bool(host.call))
    if row.start_of:
        builder = member_host(row.start_of, lookup, by_key)
        out = [(f"{host.cls}.{names[0]}", None)] if host.cls else []
        if builder.call:
            out.append((builder.call, "keywords"))
        elif builder.cls:
            out += [(f"{builder.cls}.{names[0]}", None), (f"new:{builder.cls}", "constructor")]
            if builder.literal:
                out.append((builder.cls, "keywords"))
        return out
    if host.call:
        if row.finish or row.member in FINISHERS:
            return [(host.call, "one-call")]
        if row.member == "new":
            return [(host.call, "keywords")]
        out = [(f"{host.call}({candidate}=)", "keywords") for candidate in names]
        fields = sorted(lookup.shapes.get(type_base(row.value_type), ()))
        if fields and row.kind in ("setter", "method", "field"):
            out.append((f"{host.call}({', '.join(name(field) + '=' for field in fields)})", "keywords"))
        return out
    cls = host.cls
    reason = "keywords" if host.keywords else None
    out = []
    if row.member == "new" and row.kind == "method":
        out += [(f"new:{cls}", "constructor"), (f"{cls}.new", None)]
    elif row.kind == "assoc-const":
        out.append((f"{cls}.{row.member}", None))
    else:
        out += [(f"{cls}.{candidate}", reason) for candidate in names]
    if row.kind == "field" and row.builder:
        builder = member_host(row.builder, lookup, by_key)
        if builder.call:
            out += [(f"{builder.call}({candidate}=)", "keywords") for candidate in names]
        elif builder.cls:
            out += [(f"{builder.cls}.{candidate}", "keywords") for candidate in names]
    if host.literal and (row.finish or row.member in FINISHERS or row.member == "new"):
        out.append((cls, "keywords"))
    protocol = PROTOCOL.get(row.member) if row.kind == "method" else None
    if protocol:
        out += [(f"{cls}.{member}", "protocol") for member in protocol[0 if lookup.python else 1]]
    if row.kind == "method" and not lookup.python and not host.keywords_reason and row.returns not in ("Self", row.owner) and lookup.type_kind(cls) in ("interface", "type"):
        out += [(f"fn:{camel(snake(cls) + '_' + row.member)}", "free-function"), (f"fn:{camel(row.member)}", "free-function")]
    return out


def member_names(row, lookup, keywords):
    """Peer spellings of one member name: as is, without a builder `with_` prefix
    for keywords, with a Python reserved word escaped, and with a unit suffix
    when it carries a Duration."""
    names = [row.member]
    if keywords and row.member.startswith("with_"):
        names.append(row.member[5:])
    if lookup.python:
        names += [f"{name}_" for name in names if name in PY_RESERVED]
    if keywords and type_base(row.value_type).endswith("Id") and not row.member.endswith("id"):
        names.append(f"{row.member}_id")
    if re.search(r"\b(?:Duration|Instant|SystemTime)\b", row.value_type):
        names += [f"{name}_{unit}" for name in list(names) for unit in UNITS]
    return [lookup.name(name) for name in names]


def member_host(owner, lookup, by_key):
    """The peer class or call that hosts the members of one Rust type."""
    parent = by_key.get((owner, None))
    cell = None if parent is None else parent.python if lookup.python else parent.typescript
    short = lookup.type_name(owner.rsplit("::", 1)[-1])
    if cell is None:
        return Host(cls=short)
    if cell.omitted:
        return Host(omitted=cell.reason)
    if cell.reason == "callback" and not cell.gap:
        return Host(callback=cell.spelling)
    if cell.reason == "serde-dict" and not cell.gap:
        return Host(serde=True)
    spelling = cell.hint or ("" if cell.gap else cell.spelling)
    if not spelling:
        return Host(cls=short)
    target = split_call(spelling)[0]
    if spelling.startswith(("fn:", "new:")) or "." in target.replace("wire.", "", 1):
        return Host(call=target)
    literal = not lookup.python and lookup.type_kind(target) in ("interface", "type")
    kind = None if lookup.python else lookup.type_kind(target)
    words = (literal and is_plain_alias(lookup.surface.decls[target].text)) or kind == "const"
    host = Host(cls=target, keywords=cell.reason == "keywords" or literal, literal=literal, words=words)
    host.keywords_reason = cell.reason == "keywords"
    return host


def substitute(spelling, lookup, by_key):
    """Replace each `{RustType}` with the peer class or call that hosts it."""
    def host(match):
        found = member_host(match.group(1), lookup, by_key)
        return found.cls or found.call or match.group(1)
    return re.sub(r"\{([\w:]+)\}", host, spelling)


def variant_spelling(row, lookup, owner):
    if lookup.has_type(owner):
        members = lookup.members(owner)
        for candidate in (row.member, upper_snake(row.member), snake(row.member), camel(snake(row.member))):
            if candidate in members:
                return f"{owner}.{candidate}", None
        if not lookup.python and lookup.surface.decls[owner].kind in ("type", "const"):
            text = lookup.surface.decls[owner].text
            keyed = re.search(r"keyof typeof (\w+)", text)
            if keyed and lookup.type_name(keyed.group(1)) in lookup.surface.decls:
                text += " " + " ".join(f'"{key}"' for key in lookup.members(lookup.type_name(keyed.group(1))))
            indexed = re.search(r"=\s*(\w+)\[\"(\w+)\"\]", text)
            if indexed and lookup.type_name(indexed.group(1)) in lookup.surface.decls:
                text = " ".join(re.findall(indexed.group(2) + r"\??\s*:\s*([^;]*)", lookup.surface.decls[lookup.type_name(indexed.group(1))].text))
            wanted = normalize(row.member)
            if any(normalize(literal) == wanted for literal in re.findall(r"\"([^\"]*)\"", text)):
                return owner, "plain-value"
    return f"{owner}.{row.member}", None


def error_class(variant, lookup):
    return lookup.errors.get(variant, f"{variant}Error")


def param_mismatch(row, params, lookup, reason):
    """How the peer parameters differ from the Rust ones, or None.

    Python keyword names are API, so each Rust parameter must reach a Python
    parameter of the same name, a Duration may carry a unit suffix, and a
    struct may arrive unpacked into its field names. TypeScript names are not
    API, so parameters that do not match by name pair up by position.
    """
    peer = [param for param in params if param["name"] not in ("self", "cls")]
    if reason in ("free-function", "error-function") and peer and row.receiver:
        peer = peer[1:]
    if any(param.get("rest") for param in peer):
        return None
    unmatched = list(peer)
    pending = []
    for rust_name, rust_type in row.params:
        direct = param_names(rust_name, rust_type, lookup)
        hits = [param for param in unmatched if param["name"] in direct]
        if hits:
            unmatched = [param for param in unmatched if param not in hits]
        else:
            pending.append((rust_name, rust_type))
    absent = []
    for rust_name, rust_type in pending:
        unpacked = {lookup.name(field) for field in lookup.shapes.get(type_base(rust_type), ())}
        hits = [param for param in unmatched if param["name"] in unpacked]
        if hits:
            unmatched = [param for param in unmatched if param not in hits]
        else:
            absent.append((rust_name, rust_type))
    if not lookup.python:
        for _ in list(absent):
            if not unmatched:
                break
            absent.pop(0)
            unmatched.pop(0)
        if absent and any(param.get("options") for param in peer):
            absent = []
    builder_return = bool(lookup.shapes.get(row.returns)) or reason in ("keywords", "one-call")
    extra = [] if builder_return else [param for param in unmatched if param.get("required") and param["name"] not in TYPE_STANDINS]
    if absent or extra:
        rust = ", ".join(lookup.name(name) for name, _ in row.params)
        return f"takes ({', '.join(param['name'] for param in peer)}), Rust takes ({rust})"
    return None


def param_names(rust_name, rust_type, lookup):
    """Peer parameter names that carry one Rust parameter as it is: the same
    name, a Duration with a unit suffix, or a plain-value id with an `_id` suffix."""
    out = {lookup.name(rust_name)}
    if lookup.python and rust_name in PY_RESERVED:
        out.add(f"{rust_name}_")
    if re.search(r"\b(?:Duration|Instant|SystemTime)\b", rust_type):
        out |= {lookup.name(f"{rust_name}_{unit}") for unit in UNITS}
    if type_base(rust_type).endswith("Id") and not rust_name.endswith("id"):
        out.add(lookup.name(f"{rust_name}_id"))
    if type_base(rust_type) == "Duration":
        out |= {"seconds", "milliseconds"}
    return out


def type_base(rust_type):
    """The named type inside references, smart pointers, Option, and impl Into."""
    text = re.sub(r"&\s*(?:'\w+\s+)?(?:mut\s+)?", "", rust_type)
    while True:
        wrapped = re.match(r"^(?:impl\s+(?:Into|AsRef|IntoIterator<Item\s*=)\s*<?|(?:Option|Arc|Box|Vec|Rc|Result)\s*<)\s*(.*?)>*$", text.strip())
        if not wrapped:
            break
        text = wrapped.group(1)
    named = re.match(r"^(?:\w+::)*(\w+)", text.strip())
    return named.group(1) if named else ""


def python_only(python, claims):
    out = []
    for name in sorted(python.exported):
        if name in python.classes:
            if name not in claims:
                out.append(name)
                continue
            for member in sorted(python.classes[name].members):
                if not is_dunder(member) and member not in PY_PROTOCOL and f"{name}.{member}" not in claims:
                    out.append(f"{name}.{member}")
        elif name in python.functions:
            if f"fn:{name}" not in claims:
                out.append(f"{name}()")
        elif name not in claims:
            out.append(name)
    return out


def typescript_only(typescript, claims):
    out = []
    for name in sorted(typescript.decls):
        decl = typescript.decls[name]
        if name.startswith("wire."):
            continue
        if name not in claims:
            out.append(name)
            continue
        for member in sorted(decl.members):
            if member in TS_PROTOCOL or f"{name}.{member}" in claims:
                continue
            symbol = re.fullmatch(r"\[(\w+)\]", member)
            if symbol and symbol.group(1) not in typescript.decls:
                continue
            out.append(f"{name}.{member}")
        if decl.constructor not in (None, "hidden") and f"{name}.constructor" not in claims:
            out.append(f"new {name}()")
    for name in sorted(typescript.functions):
        if f"fn:{name}" not in claims:
            out.append(f"{name}()")
    return out


def item_rows(section, display, item, resolver, displays):
    """Rows for one public Rust item and its public members."""
    source = f"{item.file}:{item.line}"
    if item.kind in ("fn", "const", "static"):
        name = display.rsplit("::", 1)[-1]
        owner = f"{section}::{name}" if section else name
        row = Row(section, owner, None, item.kind, source, name=name, returns=type_base(return_type(item.header)))
        if item.kind in ("const", "static"):
            row.value_type = item.header.split(":", 1)[-1].split("=", 1)[0].strip()
        row.params = [(param.name, getattr(param, "type", "")) for param in item.params if not getattr(param, "receiver", False)]
        return [row]
    if item.kind == "extern":
        return [Row(section, display, None, "extern", source)]
    builders = getattr(item, "builders", [])
    builder = displays.get(id(builders[0])) if builders else None
    fields = [field for field in item.fields if not field.hidden]
    literal = item.kind == "struct" and all(field.public for field in fields) and not any(attr == "non_exhaustive" for attr in item.attrs)
    newtype = item.kind == "struct" and item.tuple and len(item.fields) == 1 and PLAIN_FIELD.match(item.fields[0].type)
    plain = (item.kind == "enum" and item.variants and not any(variant.fields for variant in item.variants)) or newtype
    positional = item.kind == "struct" and item.tuple and bool(fields) and all(field.public for field in fields)
    serde = any(SERDE_DERIVE.search(attr) for attr in item.attrs)
    rows = [Row(section, display, None, item.kind, source, literal=literal, plain=bool(plain), positional=positional, variant_of=getattr(item, "variant_of", None), traits=resolver.traits(item), serde=serde)]
    if item.kind in ("struct", "union") and not item.tuple:
        for field in fields:
            if field.public:
                rows.append(Row(section, display, field.name, "field", f"{item.file}:{field.line}", builder=builder, value_type=field.type, serde=serde, skipped=any(SERDE_SKIP.search(attr) for attr in field.attrs)))
    if item.kind == "enum":
        for variant in item.variants:
            if variant.hidden:
                continue
            rows.append(Row(section, display, variant.name, "variant", f"{item.file}:{variant.line}", tuple=variant.tuple, variant_fields=[field.name for field in variant.fields if not variant.tuple], serde=serde, skipped=any(SERDE_SKIP.search(attr) for attr in variant.attrs)))
            if display == "LaserError" and not variant.tuple:
                for field in variant.fields:
                    rows.append(Row(section, display, f"{variant.name}.{field.name}", "variant-field", f"{item.file}:{field.line}"))
    for member in resolver.members(item):
        row = Row(section, display, member.name, member.kind, f"{member.file}:{member.line}", returns=type_base(return_type(member.header)))
        row.params = [(param.name, getattr(param, "type", "")) for param in member.params if not getattr(param, "receiver", False)]
        row.receiver = member.kind == "setter" or any(getattr(param, "receiver", False) for param in member.params)
        row.setter_of = getattr(member, "setter_of", None)
        row.finish = getattr(member, "finish", False)
        row.start_of = displays.get(id(getattr(member, "start_of", None)))
        row.value_type = row.params[0][1] if len(row.params) == 1 else return_type(member.header) if not row.params else ""
        rows.append(row)
    return rows


def is_plain_alias(text):
    """Whether a TypeScript type alias is a plain value: a string literal union, a primitive, or a branded primitive."""
    body = " ".join(text.split()).split("=", 1)[-1].strip().rstrip(";").strip()
    return bool(re.fullmatch(r"(?:\|?\s*\"[^\"]*\"\s*)+", body) or re.match(r"^(?:string|number|bigint|boolean|Uint8Array)\b(?:\s*&.*)?$", body))


def show_spelling(spelling):
    if spelling == "key":
        return "dict key"
    if spelling.startswith("dict:"):
        return f"dict via {show_spelling(spelling[5:])}"
    if spelling.startswith("new:"):
        target, kws = split_call(spelling[4:])
        return f"`new {target}({', '.join(kw + '=' for kw in kws)})`"
    return f"`{spelling}`"


def is_dunder(name):
    return name.startswith("__") and name.endswith("__")


class Item:
    """One parsed Rust declaration."""

    def __init__(self, kind, name, **extra):
        self.kind = kind
        self.name = name
        self.public = False
        self.hidden = False
        self.attrs = []
        self.line = 0
        self.file = None
        self.module = ()
        self.crate = None
        self.fields = []
        self.variants = []
        self.items = []
        self.params = []
        self.tuple = False
        self.header = ""
        self.owner = None
        self.trait = None
        self.span = None
        self.methods = []
        self.__dict__.update(extra)


class Module:
    def __init__(self, path, file):
        self.path = path
        self.file = file
        self.items = []
        self.children = {}


class Resolver:
    """Path resolution and public reachability across the parsed crates."""

    def __init__(self, crates):
        self.crates = crates
        self.impls = {}
        self.named = {}
        for crate, modules in crates.items():
            for module in modules.values():
                for item in module.items:
                    if item.kind == "impl" and item.owner and not item.hidden:
                        self.impls.setdefault((crate, item.owner), []).append(item)
                    elif item.kind in ("struct", "enum", "union", "trait") and item.public and not item.hidden:
                        self.named.setdefault(item.name, []).append(item)

    def public_items(self, crate):
        """(section, name, item) for every public item, each item once, at its home module.

        A caller can also hold a public type it cannot name: a generated builder
        or the return type of a public method. Those join the section of the
        item that hands them out.
        """
        found = {}
        self.walk(crate, (), found)
        pending = list(found.values())
        while pending:
            rank, section, _, item = pending.pop()
            for reached in self.handed_out(item):
                key = (reached.crate, reached.module, reached.name, reached.kind)
                if key not in found:
                    found[key] = (rank, section, reached.name, reached)
                    pending.append(found[key])
        return [found[key][1:] for key in sorted(found, key=lambda key: (found[key][1], found[key][2]))]

    def handed_out(self, item):
        """Public types a caller receives from or passes to `item` without naming them."""
        if item.kind == "extern":
            return []
        out = list(getattr(item, "builders", []))
        functions = [item] if item.kind == "fn" else self.members(item) if item.kind in TYPE_KINDS else []
        for function in functions:
            taken = " ".join(getattr(param, "type", "") for param in function.params if not getattr(param, "receiver", False))
            for name in re.findall(r"\b([A-Z]\w*)", return_type(function.header) + " " + taken):
                candidates = self.named.get(name, [])
                local = [found for found in candidates if found.crate == item.crate]
                if len(local or candidates) == 1:
                    out += local or candidates
        for field in getattr(item, "fields", []) if item.kind in ("struct", "union") else []:
            if field.public and not field.hidden:
                for name in re.findall(r"\b([A-Z]\w*)", field.type):
                    candidates = self.named.get(name, [])
                    local = [found for found in candidates if found.crate == item.crate]
                    if len(local or candidates) == 1:
                        out += local or candidates
        return out

    def walk(self, crate, path, found):
        rank = ("prelude" in path, len(path))
        for name, hits in self.exports(crate, path).items():
            for hit in hits:
                if hit[0] == "mod":
                    continue
                if hit[0] == "extern":
                    item = Item("extern", name, public=True, file="(re-export)", line=0, crate=None)
                    item.file = f"{self.crates[crate][path].file}"
                    key = ("extern",) + tuple(hit[1])
                else:
                    owner_crate, module, item = hit
                    key = (owner_crate, module, item.name, item.kind)
                section = "::".join(path)
                if key not in found or rank < found[key][0]:
                    found[key] = (rank, section, name, item)
        for child, visible in self.crates[crate][path].children.items():
            if visible and path + (child,) in self.crates[crate]:
                self.walk(crate, path + (child,), found)

    def exports(self, crate, path):
        """Public names of one module mapped to their resolved targets."""
        out = {}
        for item in self.crates[crate][path].items:
            if not item.public or item.hidden:
                continue
            if item.kind == "use":
                for target, alias in item.tree:
                    if len(target) == 1 and (target[0] in self.crates or target[0] in EXTERN_CRATES):
                        continue
                    if target[0] in EXTERN_CRATES:
                        out.setdefault(alias, []).append(("extern", target))
                        continue
                    if alias == "*":
                        inner = self.module_of(crate, path, target)
                        if inner:
                            for name, hits in self.exports(*inner).items():
                                out.setdefault(name, []).extend(hits)
                        continue
                    out.setdefault(alias, []).extend(self.resolve(crate, path, target, frozenset()))
            elif item.kind not in ("impl", "macro_rules!", "macro_call"):
                out.setdefault(item.name, []).append((crate, path, item))
        return out

    def members(self, item):
        """Public methods, setters, and associated consts of a type."""
        out = list(item.methods)
        if item.kind == "trait":
            for member in item.items:
                if member.kind in ("fn", "const") and not member.hidden:
                    out.append(member_of(member, "method" if member.kind == "fn" else "assoc-const"))
            return out
        for block in self.impls.get((item.crate, item.name), []):
            if block.trait:
                continue
            for member in block.items:
                if member.kind not in ("fn", "const") or not member.public or member.hidden:
                    continue
                if getattr(member, "replaced", False):
                    continue
                out.append(member_of(member, "method" if member.kind == "fn" else "assoc-const"))
        seen, unique = set(), []
        for member in out:
            if member.name not in seen:
                seen.add(member.name)
                unique.append(member)
        return sorted(unique, key=lambda member: member.name)

    def traits(self, item):
        """Names of the traits an impl block implements for a type."""
        out = []
        for block in self.impls.get((item.crate, item.name), []):
            if block.trait:
                named = re.match(r"^(?:\w+::)*(\w+)", block.trait.lstrip("!"))
                if named:
                    out.append(named.group(1))
        return out

    def module_of(self, crate, path, target):
        for hit in self.resolve(crate, path, target, frozenset()):
            if hit[0] == "mod":
                return hit[1], hit[2]
        return None

    def resolve(self, crate, path, target, seen):
        """Targets a use path names, following re-exports."""
        head = self.start(crate, path, target[0])
        rest = target[1:]
        if head is None:
            if target[0] in EXTERN_CRATES:
                return [("extern", target)]
            hits = self.lookup(crate, path, target[0], seen)
            if not rest:
                return hits
            modules = [hit for hit in hits if hit[0] == "mod"]
            if not modules:
                return []
            head = (modules[0][1], modules[0][2])
        crate, path = head
        if not rest:
            return [("mod", crate, path)]
        for segment in rest[:-1]:
            step = self.start(crate, path, segment) if segment in ("super", "self") else None
            if step is None:
                modules = [hit for hit in self.lookup(crate, path, segment, seen) if hit[0] == "mod"]
                if not modules:
                    return []
                step = (modules[0][1], modules[0][2])
            crate, path = step
        return self.lookup(crate, path, rest[-1], seen)

    def start(self, crate, path, first):
        if first == "crate":
            return crate, ()
        if first == "self":
            return crate, path
        if first == "super":
            return crate, path[:-1]
        if first in self.crates:
            return first, ()
        if first in self.crates[crate].get(path, Module(path, None)).children:
            return crate, path + (first,)
        return None

    def lookup(self, crate, path, name, seen):
        """Items called `name` in one module, following its use declarations."""
        module = self.crates[crate].get(path)
        if module is None:
            return []
        found = [(crate, path, item) for item in module.items if item.name == name and item.kind not in ("use", "impl", "macro_call")]
        if found:
            return found
        if name in module.children:
            return [("mod", crate, path + (name,))]
        for item in module.items:
            if item.kind != "use":
                continue
            for target, alias in item.tree:
                key = (crate, path, target)
                if key in seen:
                    continue
                if alias == name:
                    return self.resolve(crate, path, target, seen | {key})
                if alias == "*":
                    inner = self.module_of(crate, path, target)
                    if inner:
                        hits = self.lookup(inner[0], inner[1], name, seen | {key})
                        if hits:
                            return hits
        return []


def member_of(member, kind):
    out = Item(kind, member.name, file=member.file, line=member.line, params=member.params, header=member.header)
    out.setter_of = getattr(member, "setter_of", None)
    return out


def load_crate(name, root):
    """Every module of one crate, inline modules, expanded macros, and bon builders included."""
    modules = {}
    macros = {}

    def add(path, file, items):
        module = modules.setdefault(path, Module(path, file))
        for item in items:
            item.file, item.module, item.crate = file, path, name
            for nested in item.items:
                nested.file = nested.file or file
            if item.kind == "mod":
                if any(attr == "cfg(test)" for attr in item.attrs):
                    continue
                module.children[item.name] = item.public and not item.hidden
                if item.inline:
                    add(path + (item.name,), file, item.items)
                continue
            if item.kind == "macro_rules!":
                macros[item.name] = item
            module.items.append(item)
            module.items += variant_traits(item)

    for path in sorted(root.rglob("*.rs")):
        parts = path.relative_to(root).with_suffix("").parts
        if parts[-1] in ("mod", "lib"):
            parts = parts[:-1]
        text = path.read_text()
        code = strip_rust(text)
        add(tuple(parts), path.relative_to(ROOT).as_posix(), parse_items(code, text, 0, len(code)))
    for module in list(modules.values()):
        for item in list(module.items):
            if item.kind == "macro_call" and item.name in macros:
                expanded = expand_macro(macros[item.name], item.args)
                for generated in expanded:
                    generated.line = item.line
                add(module.path, module.file, expanded)
    for module in modules.values():
        generate_builders(module)
    return modules


def generate_builders(module):
    """Add the start functions and builder types bon generates."""
    types = {item.name: item for item in module.items if item.kind in ("struct", "enum")}
    for item in list(module.items):
        if item.kind == "struct" and any(re.search(r"derive\s*\(.*\b(?:bon::)?Builder\b", attr) for attr in item.attrs):
            config = builder_config(item.attrs)
            members = []
            for field in item.fields:
                if builder_attr(field.attrs, "field") or builder_attr(field.attrs, "skip"):
                    continue
                members.append((field.name, field.type, field.attrs))
            add_builder(module, item, config.get("builder_type", f"{item.name}Builder"), config.get("start_fn", "builder"), config.get("finish_fn", "build"), members)
        if item.kind == "impl" and any(re.fullmatch(r"(?:bon::)?bon", attr) for attr in item.attrs) and item.owner in types:
            owner = types[item.owner]
            for function in item.items:
                if function.kind != "fn" or not any(re.fullmatch(r"builder(\s*\(.*\))?", attr) for attr in function.attrs):
                    continue
                function.replaced = True
                config = builder_config(function.attrs)
                if function.name == "new":
                    names = (f"{owner.name}Builder", "builder", "build")
                else:
                    names = (f"{owner.name}{pascal(function.name)}Builder", function.name, "call")
                members = [(param.name, param.type, param.attrs) for param in function.params]
                add_builder(module, owner, config.get("builder_type", names[0]), config.get("start_fn", names[1]), config.get("finish_fn", names[2]), members, function)


def add_builder(module, owner, builder_name, start_fn, finish_fn, members, source=None):
    source = source or owner
    builder = Item("struct", builder_name, public=owner.public, hidden=owner.hidden, line=source.line, file=owner.file, module=owner.module, crate=owner.crate)
    start = Item("method", start_fn, public=owner.public, line=source.line, file=owner.file, start_of=builder)
    owner.methods = [method for method in owner.methods if method.name != start_fn] + [start]
    for field_name, field_type, attrs in members:
        builder.methods.append(Item("setter", field_name, line=source.line, file=owner.file, params=[Item("param", field_name, type=field_type)]))
        optional = field_type.startswith("Option<") or builder_attr(attrs, "default")
        if optional and not builder_attr(attrs, "required"):
            maybe = Item("setter", f"maybe_{field_name}", line=source.line, file=owner.file, params=[Item("param", field_name, type=field_type)])
            maybe.setter_of = field_name
            builder.methods.append(maybe)
    builder.methods.append(Item("method", finish_fn, line=source.line, file=owner.file, finish=True))
    module.items.append(builder)
    owner.builders = getattr(owner, "builders", []) + [builder]


def builder_config(attrs):
    out = {}
    for attr in attrs:
        if not attr.startswith("builder"):
            continue
        for key in ("start_fn", "finish_fn", "builder_type"):
            named = re.search(key + r"\s*(?:=\s*|\(\s*name\s*=\s*)(\w+)", attr)
            if named:
                out[key] = named.group(1)
    return out


def builder_attr(attrs, word):
    return any(re.match(r"builder\s*\(.*\b" + word + r"\b", attr) for attr in attrs)


def expand_macro(definition, args):
    """Items one call of a single-arm `macro_rules!` produces."""
    arm = re.match(r"\s*\((.*?)\)\s*=>\s*\{(.*)\}\s*;?\s*$", definition.body, re.S)
    if not arm:
        return []
    matcher, transcriber = arm.groups()
    matcher = re.sub(r"\$\((?:[^()]|\([^()]*\))*\)\s*[,;]?\s*[*+?]", "", matcher)
    transcriber = re.sub(r"\$\((?:[^()]|\([^()]*\))*\)\s*[*+?]", "", transcriber)
    fragments = re.findall(r"\$(\w+)\s*:\s*\w+", matcher)
    values = [args[start:end].strip() for start, end in split_top(args, 0, len(args), True)]
    values = [re.sub(r"^(?:#\s*\[[^\]]*\]\s*)*", "", value).strip() for value in values]
    if len(values) != len(fragments):
        return []
    for fragment, value in zip(fragments, values):
        transcriber = re.sub(r"\$" + fragment + r"\b", value, transcriber)
    code = transcriber.replace("$crate", "crate")
    return parse_items(code, code, 0, len(code))


def variant_traits(item):
    """Traits generated by `trait_variant::make` next to their source trait."""
    out = []
    if item.kind != "trait":
        return out
    for attr in item.attrs:
        made = re.match(r"trait_variant::make\s*\(\s*(\w+)", attr)
        if made:
            out.append(Item("trait", made.group(1), public=item.public, hidden=item.hidden, line=item.line, file=item.file, module=item.module, crate=item.crate, items=item.items, variant_of=item.name))
            item.variant_of = made.group(1)
    return out


def strip_rust(text):
    """Blank comments and literal contents. Offsets and newlines stay put."""
    out = list(text)
    size = len(text)

    def blank(start, end):
        for index in range(start, min(end, size)):
            if out[index] != "\n":
                out[index] = " "

    def ident_before(index):
        return index > 0 and (text[index - 1].isalnum() or text[index - 1] == "_")

    index = 0
    while index < size:
        char = text[index]
        if text.startswith("//", index):
            end = text.find("\n", index)
            end = size if end < 0 else end
            blank(index, end)
            index = end
            continue
        if text.startswith("/*", index):
            depth, end = 1, index + 2
            while end < size and depth:
                if text.startswith("/*", end):
                    depth, end = depth + 1, end + 2
                elif text.startswith("*/", end):
                    depth, end = depth - 1, end + 2
                else:
                    end += 1
            blank(index, end)
            index = end
            continue
        raw = re.match(r'[bc]?r(#*)"', text[index:index + 12])
        if raw and not ident_before(index):
            start = index + raw.end()
            end = text.find('"' + raw.group(1), start)
            end = size if end < 0 else end
            blank(start, end)
            index = end + 1 + len(raw.group(1))
            continue
        quoted = re.match(r'[bc]?"', text[index:index + 2])
        if quoted and not ident_before(index):
            start = index + quoted.end()
            end = start
            while end < size and text[end] != '"':
                end += 2 if text[end] == "\\" else 1
            blank(start, end)
            index = end + 1
            continue
        if char == "'" or (char == "b" and text.startswith("b'", index) and not ident_before(index)):
            offset = 1 if char == "b" else 0
            literal = re.match(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'", text[index + offset:index + offset + 14])
            if literal:
                blank(index + offset + 1, index + offset + literal.end() - 1)
                index += offset + literal.end()
                continue
        index += 1
    return "".join(out)


def close(code, index):
    """Index of the bracket that closes the one at `index`."""
    depth = 0
    for position in range(index, len(code)):
        char = code[position]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                return position
    return len(code) - 1


def close_angle(code, index):
    depth = 0
    for position in range(index, len(code)):
        char = code[position]
        if char in "<([{":
            depth += 1
        elif char in ")]}" or (char == ">" and code[position - 1] not in "-="):
            depth -= 1
            if depth == 0:
                return position
    return len(code) - 1


def split_top(code, start, end, angles):
    """Comma separated spans of `code[start:end]` outside any bracket."""
    spans, depth, begin = [], 0, start
    for position in range(start, end):
        char = code[position]
        if char in "([{" or (angles and char == "<"):
            depth += 1
        elif char in ")]}" or (angles and char == ">" and code[position - 1] not in "-="):
            depth -= 1
        elif char == "," and depth == 0:
            spans.append((begin, position))
            begin = position + 1
    if code[begin:end].strip():
        spans.append((begin, end))
    return spans


def leading_attrs(code, text, position, end):
    attrs = []
    while True:
        while position < end and code[position].isspace():
            position += 1
        if code.startswith("#[", position) or code.startswith("#![", position):
            opening = code.index("[", position)
            shut = close(code, opening)
            attrs.append(" ".join(text[opening + 1:shut].split()))
            position = shut + 1
        else:
            return attrs, position


HEAD = re.compile(
    r"(?P<vis>pub(?:\s*\([^)]*\))?\s+)?"
    r"(?P<quals>(?:(?:const|async|unsafe|default|extern\s*(?:\"[^\"]*\")?)\s+)*)"
    r"(?P<kind>fn|struct|enum|union|trait|type|const|static|mod|use|impl|macro_rules!)(?!\w)"
)
MACRO_CALL = re.compile(r"(\w+)\s*!\s*([(\[{])")


def parse_items(code, text, start, end):
    """Declarations between `start` and `end`, attributes attached."""
    items = []
    position = start
    while position < end:
        attrs, position = leading_attrs(code, text, position, end)
        if position >= end:
            break
        line = text.count("\n", 0, position) + 1
        head = HEAD.match(code, position)
        call = None if head else MACRO_CALL.match(code, position)
        if call:
            shut = close(code, call.end() - 1)
            stop = shut + 1
            while stop < end and code[stop] in " \t\n":
                stop += 1
            if stop < end and code[stop] == ";":
                stop += 1
            if "cfg(test)" not in attrs:
                items.append(Item("macro_call", call.group(1), args=code[call.end():shut], line=line, attrs=attrs))
            position = stop
            continue
        kind = head.group("kind") if head else None
        scan, depth = position, 0
        while scan < end:
            char = code[scan]
            if char in "([":
                depth += 1
            elif char in ")]":
                depth -= 1
            elif depth == 0 and char in "{;":
                break
            scan += 1
        header = code[position:scan]
        body = None
        if scan >= end:
            stop = end
        elif code[scan] == ";":
            stop = scan + 1
        elif kind in ("use", "const", "static", "type"):
            depth = 0
            while scan < end and not (depth == 0 and code[scan] == ";"):
                if code[scan] in "([{":
                    depth += 1
                elif code[scan] in ")]}":
                    depth -= 1
                scan += 1
            header = code[position:scan]
            stop = scan + 1
        else:
            body = (scan, close(code, scan))
            stop = body[1] + 1
            while stop < end and code[stop] in " \t":
                stop += 1
            if stop < end and code[stop] == ";":
                stop += 1
        if "cfg(test)" not in attrs and kind:
            item = interpret(code, text, head, header, body, attrs)
            if item:
                item.line = line
                item.span = (position, stop)
                items.append(item)
        position = stop
    return items


def interpret(code, text, head, header, body, attrs):
    kind = head.group("kind")
    vis = (head.group("vis") or "").split()
    rest = header[head.end() - head.start():]
    if kind == "impl":
        item = Item("impl", None)
        item.owner, item.trait = impl_target(header)
        item.items = parse_items(code, text, body[0] + 1, body[1]) if body else []
    elif kind == "use":
        item = Item("use", None)
        item.tree = use_tree(" ".join(rest.split()))
    else:
        named = re.match(r"\s*(\w+)", rest)
        if not named:
            return None
        item = Item(kind, named.group(1))
        if kind in ("struct", "union"):
            after = named.end()
            generics = re.compile(r"\s*<").match(rest, after)
            if generics:
                after = close_angle(rest, generics.end() - 1) + 1
            tuple_open = re.compile(r"\s*\(").match(rest, after)
            if tuple_open:
                opening = head.end() + tuple_open.end() - 1
                item.tuple = True
                item.fields = parse_fields(code, text, opening, close(code, opening), True)
            elif body:
                item.fields = parse_fields(code, text, body[0], body[1], False)
        elif kind == "enum" and body:
            item.variants = parse_variants(code, text, body[0], body[1])
        elif kind == "trait" and body:
            item.items = parse_items(code, text, body[0] + 1, body[1])
        elif kind == "mod":
            item.inline = body is not None
            item.items = parse_items(code, text, body[0] + 1, body[1]) if body else []
        elif kind == "fn":
            item.params = parse_params(code, text, head.end() + named.end())
        elif kind == "macro_rules!" and body:
            item.body = code[body[0] + 1:body[1]]
    item.public = vis == ["pub"]
    item.attrs = attrs
    item.hidden = any(re.fullmatch(r"doc\s*\(\s*hidden\s*\)", attr) for attr in attrs)
    item.header = " ".join(header.split())
    return item


def return_type(header):
    """The text after the top-level `->` of a fn header, or an empty string."""
    depth = 0
    for index, char in enumerate(header):
        if char in "<([":
            depth += 1
        elif char in ">)]" and header[index - 1] != "-":
            depth -= 1
        elif depth == 0 and header.startswith("->", index):
            return re.split(r"\bwhere\b", header[index + 2:], maxsplit=1)[0]
    return ""


def impl_target(header):
    """The implementing type and trait of an impl header."""
    head = header.strip()[4:].lstrip()
    if head.startswith("<"):
        head = head[close_angle(head, 0) + 1:].lstrip()
    main = re.split(r"\bwhere\b", head, maxsplit=1)[0].strip()
    trait = None
    depth = 0
    for index in range(len(main)):
        char = main[index]
        if char in "<([":
            depth += 1
        elif char in ">)]" and main[index - 1] != "-":
            depth -= 1
        elif depth == 0 and main.startswith(" for ", index):
            trait, main = main[:index].strip(), main[index + 5:].strip()
            break
    owner = re.match(r"(?:dyn\s+)?(?:[\w]+::)*([A-Za-z_]\w*)", main)
    return (owner.group(1) if owner else None), trait


def use_tree(tree, prefix=()):
    """Expand a use tree into (path, alias) pairs. A glob has alias `*`."""
    tree = re.sub(r"\s*(::|\{|\}|,)\s*", r"\1", tree.strip())
    brace = tree.find("{")
    if brace >= 0 and tree.endswith("}"):
        base = tuple(part for part in tree[:brace].split("::") if part)
        inner = tree[brace + 1:-1]
        out = []
        for start, end in split_top(inner, 0, len(inner), False):
            out += use_tree(inner[start:end], prefix + base)
        return out
    named = re.match(r"^(.*?)(?: as (\w+))?$", tree)
    path = prefix + tuple(part for part in named.group(1).split("::") if part)
    if not path:
        return []
    if path[-1] == "*":
        return [(path[:-1], "*")]
    if path[-1] == "self":
        return [(path[:-1], named.group(2) or path[-2])]
    return [(path, named.group(2) or path[-1])]


def parse_fields(code, text, opening, shut, positional):
    fields = []
    for index, (start, end) in enumerate(split_top(code, opening + 1, shut, True)):
        attrs, position = leading_attrs(code, text, start, end)
        chunk = code[position:end]
        if positional:
            field = re.match(r"\s*(pub(?:\s*\([^)]*\))?\s+)?(.*)", chunk, re.S)
            name, kind = str(index), field.group(2)
        else:
            field = re.match(r"\s*(pub(?:\s*\([^)]*\))?\s+)?(\w+)\s*:(.*)", chunk, re.S)
            if not field:
                continue
            name, kind = field.group(2), field.group(3)
        hidden = any(re.fullmatch(r"doc\s*\(\s*hidden\s*\)", attr) for attr in attrs)
        fields.append(Item("field", name, public=(field.group(1) or "").split() == ["pub"], hidden=hidden, attrs=attrs, line=text.count("\n", 0, position) + 1, type=" ".join(kind.split())))
    return fields


def parse_variants(code, text, opening, shut):
    variants = []
    for start, end in split_top(code, opening + 1, shut, False):
        attrs, position = leading_attrs(code, text, start, end)
        named = re.compile(r"\s*(\w+)\s*").match(code, position, end)
        if not named:
            continue
        variant = Item("variant", named.group(1), attrs=attrs, line=text.count("\n", 0, position) + 1, public=True)
        variant.hidden = any(re.fullmatch(r"doc\s*\(\s*hidden\s*\)", attr) for attr in attrs)
        after = named.end()
        if after < end and code[after] in "({":
            variant.tuple = code[after] == "("
            variant.fields = parse_fields(code, text, after, close(code, after), variant.tuple)
            for field in variant.fields:
                field.public = True
        variants.append(variant)
    return variants


def parse_params(code, text, position):
    """Parameters of a fn whose name ends at `position`, receiver excluded."""
    generics = re.compile(r"\s*<").match(code, position)
    if generics:
        position = close_angle(code, generics.end() - 1) + 1
    opening = re.compile(r"\s*\(").match(code, position)
    if not opening:
        return []
    opening = opening.end() - 1
    params = []
    for start, end in split_top(code, opening + 1, close(code, opening), True):
        attrs, at = leading_attrs(code, text, start, end)
        chunk = code[at:end].strip()
        if re.match(r"^&?\s*(?:'\w+\s+)?(?:mut\s+)?self\b", chunk):
            params.append(Item("param", "self", receiver=True))
            continue
        named = re.match(r"^(?:mut\s+)?(\w+)\s*:(.*)$", chunk, re.S)
        if named:
            params.append(Item("param", named.group(1).lstrip("_") or named.group(1), attrs=attrs, type=" ".join(named.group(2).split())))
    return params


def py_class(node):
    """Members of one stub class, constructor included."""
    out = PyClass(node.name, [ast.unparse(base) for base in node.bases])
    for child in node.body:
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorators = {ast.unparse(decorator) for decorator in child.decorator_list}
            if child.name in ("__new__", "__init__"):
                out.constructor = py_params(child)
                continue
            if "property" in decorators or any(decorator.endswith(".setter") for decorator in decorators):
                out.members.setdefault(child.name, {"kind": "property"})
                continue
            existing = out.members.get(child.name)
            params = py_params(child)
            if existing and existing.get("params") is not None:
                known = {param["name"] for param in existing["params"]}
                existing["params"] += [param for param in params if param["name"] not in known]
            else:
                out.members[child.name] = {"kind": "method", "params": params}
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            out.members[child.target.id] = {"kind": "attribute"}
        elif isinstance(child, ast.ClassDef):
            # A complex enum variant is a nested class of the enum class.
            out.members[child.name] = {"kind": "attribute"}
        elif isinstance(child, ast.Assign):
            for target in child.targets:
                if isinstance(target, ast.Name):
                    out.members[target.id] = {"kind": "attribute"}
    return out


def py_params(node):
    args = node.args
    positional = args.posonlyargs + args.args
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    out = []
    for arg, default in zip(positional, defaults):
        if arg.arg not in ("self", "cls"):
            out.append({"name": arg.arg, "required": default is None})
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        out.append({"name": arg.arg, "required": default is None, "keyword": True})
    if args.vararg or args.kwarg:
        out.append({"name": "*", "rest": True})
    return out


def merge_callable(table, name, params):
    if name in table:
        known = {param["name"] for param in table[name]}
        table[name] += [param for param in params if param["name"] not in known]
    else:
        table[name] = params


def parse_report(source, surface):
    """Add the declarations of one API Extractor report to the surface."""
    block = re.search(r"```ts\n(.*?)\n```", source, re.S)
    code = block.group(1) if block else source
    aliases = {}
    for statement in ts_statements(code):
        exported = statement.startswith("export ")
        body = statement[7:] if exported else statement
        body = re.sub(r"^declare\s+", "", body)
        reexport = re.match(r"^\{\s*(.*?)\s*\}$", body.rstrip(";"), re.S)
        if exported and reexport:
            for part in reexport.group(1).split(","):
                named = re.match(r"\s*(\w+)(?:\s+as\s+(\w+))?\s*$", part)
                if named:
                    aliases[named.group(1)] = named.group(2) or named.group(1)
            continue
        namespace = re.match(r"^namespace\s+(\w+)\s*\{\s*export\s*\{(.*?)\}", body, re.S)
        if namespace:
            if namespace.group(1) == "wire":
                for part in namespace.group(2).split(","):
                    named = re.match(r"\s*(\w+)(?:\s+as\s+(\w+))?\s*$", part)
                    if named:
                        source_name = named.group(1)
                        public_name = named.group(2) or source_name
                        surface.wire.add(public_name)
                        surface.wire_aliases[public_name] = source_name
            continue
        decl = ts_decl(body)
        if decl is None:
            continue
        if decl.kind == "function":
            if exported:
                merge_callable(surface.functions, decl.name, decl.params)
            else:
                surface.hidden.setdefault(decl.name, decl)
            continue
        if exported:
            previous = surface.decls.get(decl.name)
            if previous is not None and {previous.kind, decl.kind} in ({"const", "type"}, {"const", "interface"}):
                # A companion const and its shape type share one name. Both carry members.
                merged, shape = (previous, decl) if previous.kind == "const" else (decl, previous)
                merged.text += "\n" + shape.text
                for name, member in shape.members.items():
                    merged.members.setdefault(name, member)
                surface.decls[decl.name] = merged
                continue
            if previous is not None and previous.kind == decl.kind == "const" and previous.text.startswith(decl.text):
                # Another report repeats a companion const already merged with its shape.
                continue
            surface.decls[decl.name] = decl
        else:
            surface.hidden.setdefault(decl.name, decl)
    for hidden, alias in aliases.items():
        if hidden in surface.hidden and alias not in surface.decls:
            decl = surface.hidden[hidden]
            decl.name = alias
            surface.decls[alias] = decl
    for name in surface.wire:
        source_name = surface.wire_aliases.get(name, name)
        decl = surface.decls.get(source_name) or surface.hidden.get(source_name)
        if decl is not None and f"wire.{name}" not in surface.decls:
            surface.decls[f"wire.{name}"] = decl
    for decl in surface.decls.values():
        decl.bases = [aliases.get(base, base) for base in decl.bases]
        decl.base = decl.bases[0] if decl.bases else None


def ts_statements(code):
    """Top-level statements. Each one starts at column 0 with a declaration keyword."""
    statements, current = [], []
    for line in code.split("\n"):
        if line.lstrip().startswith("//") or not line.strip():
            continue
        if re.match(r"^(export|declare|class|interface|function|const|let|type|enum|abstract|namespace|import)\b", line):
            if current:
                statements.append("\n".join(current))
            current = [line]
        elif current:
            current.append(line)
    if current:
        statements.append("\n".join(current))
    return [statement for statement in statements if not statement.startswith("import ")]


def ts_decl(body):
    head = re.match(r"^(abstract\s+)?(class|interface|function|const|let|type|enum)\s+(\w+)", body)
    if not head:
        return None
    kind, name = head.group(2), head.group(3)
    kind = "const" if kind == "let" else kind
    decl = TsDecl(kind, name, body)
    decl.abstract = bool(head.group(1))
    if kind == "function":
        opening = body.index("(", head.end())
        decl.params = ts_params(body[opening + 1:matching(body, opening)])
        return decl
    if kind in ("class", "interface"):
        header = body[head.end():body.index("{", head.end())]
        base = re.search(r"\bextends\s+(.+?)(?:\bimplements\b|$)", header)
        decl.bases = []
        if base:
            bases = base.group(1).strip()
            for start, end in split_top(bases, 0, len(bases), True):
                named = re.match(r"([\w.]+)", bases[start:end].strip())
                if named:
                    decl.bases.append(named.group(1))
        decl.base = decl.bases[0] if decl.bases else None
        opening = body.index("{", head.end())
        ts_members(body[opening + 1:matching(body, opening)], decl)
    elif kind == "const":
        literal = re.match(r"^const\s+\w+\s*:\s*\{", body)
        if literal:
            opening = body.index("{", head.end())
            ts_members(body[opening + 1:matching(body, opening)], decl)
    elif kind == "enum":
        opening = body.index("{", head.end())
        for part in body[opening + 1:matching(body, opening)].split(","):
            named = re.match(r"\s*(\w+)", part)
            if named:
                decl.members[named.group(1)] = {"kind": "property"}
    return decl


def ts_members(body, decl):
    for member in ts_member_statements(body):
        modifiers = re.match(r"^((?:(?:public|protected|private|static|readonly|abstract|override|declare|async|get|set)\s+)*)", member)
        words = modifiers.group(1).split()
        rest = member[modifiers.end():]
        if "private" in words or "protected" in words or rest.startswith("#"):
            if rest.startswith("constructor"):
                decl.constructor = "hidden"
            continue
        named = re.match(r"^(\[[^\]]+\]|\"[^\"]+\"|[\w$]+)(\?)?\s*([(<:])?", rest)
        if not named:
            continue
        name = named.group(1).strip('"')
        if name == "constructor" and named.group(3) == "(":
            decl.constructor = ts_params(rest[rest.index("(") + 1:matching(rest, rest.index("("))])
            continue
        callable_ = named.group(3) in ("(", "<") and "get" not in words
        info = {"kind": "method" if callable_ else "property", "static": "static" in words}
        if callable_:
            opening = rest.index("(", named.end(1))
            info["params"] = ts_params(rest[opening + 1:matching(rest, opening)])
        existing = decl.members.get(name)
        if existing and existing.get("params") is not None and info.get("params") is not None:
            known = {param["name"] for param in existing["params"]}
            existing["params"] += [param for param in info["params"] if param["name"] not in known]
        else:
            decl.members[name] = info


def ts_member_statements(body):
    """Members of a class or interface body. Each one starts at a four-space indent."""
    statements, current = [], []
    for line in body.split("\n"):
        if line.lstrip().startswith("//") or not line.strip():
            continue
        if re.match(r"^ {4}[^\s})\]|&]", line):
            if current:
                statements.append(" ".join(current))
            current = [line.strip()]
        elif current:
            current.append(line.strip())
    if current:
        statements.append(" ".join(current))
    return statements


def ts_params(text):
    out = []
    for start, end in split_top(text, 0, len(text), True):
        chunk = text[start:end].strip()
        rest = chunk.startswith("...")
        named = re.match(r"^(?:\.\.\.)?([\w$]+)(\?)?\s*(?::(.*))?$", chunk, re.S)
        if named:
            kind = (named.group(3) or "").strip()
            options = bool(re.match(r"^(?:Readonly<)?\w*Options\b|^\{", kind))
            out.append({"name": named.group(1), "required": not named.group(2) and not rest, "rest": rest, "options": options})
        elif chunk.startswith("{"):
            out.append({"name": "{}", "required": True, "options": True})
    return out


def matching(text, index):
    """Index of the bracket that closes the one at `index`, angle brackets ignored."""
    depth = 0
    for position in range(index, len(text)):
        char = text[position]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                return position
    return len(text)


def split_call(spelling):
    """(target, keyword names) of `target(kw=, kw2=)`."""
    call = re.match(r"^([^()]*)(?:\((.*)\))?$", spelling)
    target, args = call.group(1), call.group(2)
    keywords = [part.strip().rstrip("=").strip() for part in (args or "").split(",") if part.strip()]
    return target, keywords


def camel(name):
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


def pascal(name):
    return "".join(part[:1].upper() + part[1:] for part in name.split("_"))


def snake(name):
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def upper_snake(name):
    return snake(name).upper()


def normalize(word):
    return re.sub(r"[^a-z0-9]", "", word.lower())


if __name__ == "__main__":
    sys.exit(main())
