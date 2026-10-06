#!/usr/bin/env python3
"""Rust, Python, and TypeScript public-surface parity check.

Extracts the public methods of the covered Rust types (`sdk/src`), the Python
stub (`foreign/python/laser_sdk.pyi`), and the TypeScript API report
(`foreign/typescript/api/laser-sdk-full.api.md`), renders `docs/parity.md`,
and fails when the rendered matrix differs from the committed one or when any
row is MISSING.

    python3 scripts/check-parity.py           # check
    python3 scripts/check-parity.py --write   # regenerate docs/parity.md
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "parity.md"

# (section, Rust types, Python classes, TypeScript classes). A Rust method is
# looked up by its snake_case name in the Python classes and by its camelCase
# name in the TypeScript classes, in order.
SECTIONS = [
    ("Laser", ["Laser"], ["Laser"], ["Laser"]),
    ("LaserBuilder", ["LaserBuilder"], ["Laser"], ["LaserBuilder"]),
    ("Stream", ["Stream"], ["Stream"], ["Stream"]),
    ("Topic", ["Topic"], ["Topic"], ["Topic"]),
    ("TypedTopic", ["TypedTopic"], ["Topic", "TypedRecords"], ["TypedTopic"]),
    ("PublishRequest", ["PublishRequest"], ["PublishRequest"], ["PublishRequest"]),
    ("Producer", ["ProducerBuilder", "Producer"], ["Producer", "Topic"], ["Producer", "ProducerOptions", "Topic"]),
    ("BatchingProducer", ["BatchingProducerBuilder", "BatchingProducer"], ["BatchingProducer", "Topic"], ["BatchingProducerBuilder", "BatchingProducer"]),
    ("Consumer", ["ConsumerBuilder", "Consumer"], ["Consumer", "Topic"], ["Consumer", "ConsumerOptions"]),
    ("ConsumerGroup", ["ConsumerGroup"], ["ConsumerGroup"], ["ConsumerGroup"]),
    ("Cursor", ["Cursor"], ["Cursor"], ["Cursor"]),
    ("Kv", ["Kv"], ["Kv"], ["Kv"]),
    ("KvSetRequest", ["KvSetRequest"], ["KvSetRequest"], ["KvSetRequest"]),
    ("KvCasFencedRequest", ["KvCasFencedRequest"], ["KvCasFencedRequest"], ["KvCasFencedRequest"]),
    ("KvScanRequest", ["KvScanRequest"], ["KvScanRequest"], ["KvScanRequest"]),
    ("KvDeleteManyRequest", ["KvDeleteManyRequest"], ["KvDeleteManyRequest"], ["KvDeleteManyRequest"]),
    ("KvCopyRequest", ["KvCopyRequest"], ["KvCopyRequest"], ["KvCopyRequest"]),
    ("Fork", ["ForkHandle", "ForkCreateRequest", "ForkPutRequest"], ["ForkHandle", "ForkPutRequest"], ["Fork", "ForkCreateRequest", "ForkPutRequest"]),
    ("QueryRequest", ["QueryRequest", "QueryRows"], ["QueryRequest"], ["QueryRequest"]),
    ("Graph", ["GraphHandle"], ["Graph"], ["GraphHandle"]),
    ("Memory", ["Memory", "MemoryHandle", "RememberBuilder", "RecallBuilder"], ["Memory"], ["Memory", "MemoryHandle", "RememberBuilder", "RecallBuilder"]),
    ("ScopedMemory", ["ScopedMemory"], ["ScopedMemory"], ["ScopedMemory"]),
    ("ContextScope", ["ContextScope", "TokenBudget"], ["ContextScope", "TokenBudget"], ["ContextScope", "TokenBudget"]),
    ("Sessions", ["Sessions", "SessionConfig"], ["Sessions"], ["Sessions", "SessionConfig"]),
    ("Session", ["Session", "SessionTurn"], ["Session", "SessionTurn"], ["Session", "SessionTurn"]),
    ("AgentScope", ["AgentScope"], ["AgentScope"], ["AgentScope"]),
    ("AgentHandle", ["AgentHandle"], ["AgentHandle"], ["AgentHandle"]),
    ("Contract", ["ContractBuilder"], [], ["ContractBuilder"]),
    ("Workflow", ["Workflow", "StepHandle"], ["Workflow"], ["Workflow", "StepBuilder"]),
    ("Runs", ["Runs", "RunListRequest"], ["Runs"], ["Runs", "RunListRequest"]),
    ("Destinations", ["Destinations"], ["Destinations"], ["Destinations"]),
    ("Projections", ["Projections", "Bindings", "Schemas"], [], ["Projections", "Bindings", "Schemas"]),
    ("Watch", ["Watch", "WatchReader"], ["WatchReader"], ["Watch", "WatchReader"]),
    ("Capabilities", ["Capabilities"], ["Capabilities"], ["Capabilities"]),
    ("AgentRegistry", ["AgentRegistry"], ["AgentRegistry"], ["AgentRegistry"]),
    ("Agdx", ["Agdx", "AgdxStream"], ["Agdx", "AgdxStream"], ["Agdx", "AgdxStream"]),
    ("Governance", ["QuorumGovernor", "SwappableGovernor", "Intent", "Vote", "ActionDecision", "SwarmActivity"], ["QuorumGovernor", "SwappableGovernor", "Intent", "Vote", "ActionDecision", "SwarmActivity"], ["QuorumGovernor", "SwappableGovernor", "Intent", "Vote", "ActionDecision", "SwarmActivity"]),
    ("Signing", ["SigningKey", "KeyRegistry"], ["SigningKey", "KeyRegistry"], ["SigningKey", "KeyRegistry"]),
    ("A2aBridge", ["A2aBridge"], ["A2aBridge"], ["A2aBridge"]),
    ("McpBridge", ["McpBridge"], ["McpBridge"], ["McpBridge"]),
    ("LaserError", ["LaserError"], ["LaserError"], ["LaserError", "PublishFailedError"]),
]

# OVERRIDES below map (section, Rust method) to (python, typescript, note).
# A spelling `Class.member`, `member`, or `Class.member(kw=, ..)` is checked
# against the extracted surface, keyword names included for Python. `fn:name`
# is a module-level function, `new:Class` a default constructor. A spelling
# that starts with `-` is a deliberate omission and needs a note.

# TS_OVERRIDES map (section, Rust method) to (typescript, note) for rows whose
# Python spelling resolves on its own. A full OVERRIDES entry wins over it.
TS_FREE = "TypeScript uses the free function"
TS_OVERRIDES = {
    ("Capabilities", "backend"): ("fn:backend", TS_FREE),
    ("Capabilities", "enabled_backends"): ("fn:enabledBackends", TS_FREE),
    ("Capabilities", "is_open_only"): ("fn:isOpenOnly", TS_FREE),
    ("Capabilities", "is_ready"): ("fn:isReady", TS_FREE),
    ("Capabilities", "readiness_reasons"): ("fn:readinessReasons", TS_FREE),
    ("Capabilities", "serves_consistency"): ("fn:servesConsistency", TS_FREE),
    ("Capabilities", "unready_backends"): ("fn:unreadyBackends", TS_FREE),
    ("Producer", "background"): ("ProducerOptions.background", "TypeScript passes background mode in ProducerOptions"),
}

MISSING = "MISSING"
PYTHON = None


def builder_fields():
    """The setters bon generates, including setters for private fields."""
    out = {}
    for path in sorted((ROOT / "sdk" / "src").rglob("*.rs")):
        lines = path.read_text().splitlines()
        deriving = False
        owner = None
        attributes = []
        for line in lines:
            if line.startswith("#[derive("):
                deriving = "bon::Builder" in line
            head = re.match(r"^pub struct (\w+)", line)
            if head:
                owner = head.group(1) if deriving else None
                deriving = False
                attributes = []
                if owner:
                    out.setdefault(owner, set())
                continue
            if owner is None:
                continue
            if line.startswith("}"):
                owner = None
                continue
            if line.startswith("    #["):
                attributes.append(line)
                continue
            field = re.match(r"^    (?:pub )?(\w+)\s*:", line)
            if field:
                if not any("builder(field)" in attr or "builder(skip)" in attr for attr in attributes):
                    out[owner].add(field.group(1))
                attributes = []
    return out


def impl_owner(header):
    head = header.strip()[4:].lstrip()
    if head.startswith("<"):
        depth = 0
        for index, char in enumerate(head):
            if char == "<":
                depth += 1
            elif char == ">":
                depth -= 1
                if depth == 0:
                    head = head[index + 1:].lstrip()
                    break
    main = re.split(r"\bwhere\b", head, maxsplit=1)[0]
    if " for " in main:
        return None
    owner = re.match(r"([A-Z]\w*)", head)
    return owner.group(1) if owner else None


def rust_surface():
    out = {}
    for path in sorted((ROOT / "sdk" / "src").rglob("*.rs")):
        lines = path.read_text().splitlines()
        module = "::".join(path.relative_to(ROOT / "sdk" / "src").with_suffix("").parts)
        current = ()
        in_trait = False
        for index, line in enumerate(lines):
            trait = re.match(r"^pub trait (\w+)", line)
            if trait:
                current = (trait.group(1),)
                back = index - 1
                while back >= 0 and lines[back].startswith(("#", "///")):
                    variant = re.match(r"#\[trait_variant::make\((\w+)", lines[back])
                    if variant:
                        current += (variant.group(1),)
                    back -= 1
                in_trait = True
                continue
            if line.startswith("impl"):
                header = line
                next_line = index + 1
                while "{" not in header and next_line < len(lines):
                    header += " " + lines[next_line]
                    next_line += 1
                owner = impl_owner(header)
                current = (owner,) if owner else ()
                in_trait = False
                continue
            if line.startswith("}"):
                current = ()
                in_trait = False
                continue
            free = re.match(r"^pub (?:async )?(?:const )?fn (\w+)", line)
            if not current and not free:
                continue
            visibility = "" if in_trait else "pub "
            method = free or re.match(r"^    " + visibility + r"(?:async )?(?:const )?fn (\w+)", line)
            if not method:
                continue
            back = index - 1
            hidden = False
            indent = "" if free else "    "
            while back >= 0 and lines[back].startswith((indent + "#", indent + "///")):
                if "doc(hidden)" in lines[back]:
                    hidden = True
                back -= 1
            if not hidden:
                for owner in ((module,) if free else current):
                    out.setdefault(owner, set()).add(method.group(1))
    for owner, fields in builder_fields().items():
        out.setdefault(owner, set()).add("builder")
        out.setdefault(owner + "Builder", set()).update(fields | {"build"})
    return out


def public_rust_types():
    """Public declarations, trait variants, and generated bon builders."""
    names = set()
    for path in sorted((ROOT / "sdk" / "src").rglob("*.rs")):
        source = path.read_text()
        names.update(re.findall(r"^pub (?:struct|enum|trait) (\w+)", source, re.M))
        names.update(re.findall(r"^#\[trait_variant::make\((\w+)", source, re.M))
        if re.search(r"^pub (?:async )?(?:const )?fn ", source, re.M):
            names.add("::".join(path.relative_to(ROOT / "sdk" / "src").with_suffix("").parts))
    names.update(owner + "Builder" for owner in builder_fields())
    return names


def python_surface():
    out = {"fn": set()}
    current = None
    in_doc = False
    for line in (ROOT / "foreign" / "python" / "laser_sdk.pyi").read_text().splitlines():
        quotes = line.count('"""')
        if in_doc:
            in_doc = quotes % 2 == 0
            continue
        if quotes % 2 == 1:
            in_doc = True
            continue
        head = re.match(r"^class (\w+)", line)
        if head:
            current = head.group(1)
            out.setdefault(current, set())
            continue
        function = re.match(r"^def (\w+)\((.*)", line)
        if function:
            out["fn"].add(function.group(1))
            KWARGS[("fn", function.group(1))] = params(function.group(2))
            current = None
            continue
        if line and not line.startswith((" ", "\t", "@", "#")):
            current = None
        if current is None:
            continue
        method = re.match(r"^    (?:async )?def (\w+)\((.*)", line)
        if method:
            out[current].add(method.group(1))
            KWARGS[(current, method.group(1))] = params(method.group(2))
            continue
        field = re.match(r"^    (\w+)\s*:", line)
        if field:
            out[current].add(field.group(1))
    return out


KWARGS = {}


def params(signature):
    return set(re.findall(r"(?<![\w.\[])(\w+)\s*(?=[:=])", signature))


def typescript_surface():
    out = {}
    current = None
    report = ROOT / "foreign" / "typescript" / "api" / "laser-sdk-full.api.md"
    source = report.read_text()
    aliases = dict(re.findall(r"^export \{ (\w+) as (\w+) \}", source, re.M))
    out["fn"] = set(re.findall(r"^export (?:declare )?function (\w+)", source, re.M))
    for line in source.splitlines():
        head = re.match(r"^(export )?(?:declare )?(?:abstract )?(?:class|interface) (\w+)", line)
        constant = re.match(r"^export const (\w+): \{$", line)
        if head:
            declared = head.group(2)
            if head.group(1) or declared in aliases:
                current = aliases.get(declared, declared)
                out.setdefault(current, set())
            else:
                current = None
            continue
        if constant:
            current = constant.group(1)
            out.setdefault(current, set())
            continue
        if line.startswith("}"):
            current = None
            continue
        if current is None:
            continue
        member = re.match(r"^    (?:static )?(?:readonly )?(?:get |set )?(\w+)\??[(<:]", line)
        if member:
            out[current].add(member.group(1))
    return out


def camel(name):
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


M = MISSING
KW = "Python takes keyword arguments where Rust chains a builder"
OPT = "TypeScript passes an options object"
CODEC = "Rust is generic over a codec. Python uses json, msgpack, or payload"
IGGY = "Escape hatch to the Apache Iggy client. Python has no Iggy client object"
IGGY_RUST = "Rust-only escape hatch to the Apache Iggy SDK builders"
HTTP = "Rust-only axum router behind the HTTP feature"
CTOR = "constructor"
OVERRIDES = {
    ("Laser", "authz_history"): ("Laser.authz_history_all", "authzHistory", "Python splits it into authz_history_all, authz_history_role, and authz_history_binding"),
    ("Laser", "bind_roles_expect_revision"): ("Laser.bind_roles(expect_revision=)", "Laser.bindRoles", "Python and TypeScript fold the revision into bind_roles"),
    ("Laser", "bindings"): ("Laser.apply_binding", "bindings", "Python flattens the handle into apply_binding and remove_binding on Laser"),
    ("Laser", "projections"): ("Laser.register_projection", "projections", "Python flattens the handle into register_, drop_, get_, and list_projection(s) on Laser"),
    ("Laser", "schemas"): ("Laser.register_schema", "schemas", "Python flattens the handle into register_, drop_, get_, and list_schema(s) on Laser"),
    ("Laser", "builder"): ("Laser.connect", "builder", "Python configures the connection with connect keyword arguments"),
    ("Laser", "client"): ("-", "Laser.iggyClient", IGGY),
    ("Laser", "dlq_topic"): ("dlq_topic", "Laser.deadLetterTopic", "TypeScript name"),
    ("Laser", "from_client"): ("-", "Laser.fromIggyClient", IGGY),
    ("Laser", "sessions_with"): ("Laser.sessions(stream=)", "Laser.sessions", "Python and TypeScript pass the layout to sessions"),
    ("Laser", "with_default_stream"): ("Laser.with_stream", "withDefaultStream", "Python name"),
    ("Laser", "with_dlq_topic"): ("with_dlq_topic", "Laser.withDeadLetterTopic", "TypeScript name"),
    ("Laser", "with_governor_retention"): ("with_governor_retention", "Laser.withGovernor", "TypeScript passes retention as the third withGovernor argument"),
    ("LaserBuilder", "address"): ("Laser.connect", "address", "Python takes the address in the connection string"),
    ("LaserBuilder", "build"): ("Laser.connect", "LaserBuilder.connect", "Python and TypeScript connect in one call"),
    ("LaserBuilder", "client"): ("-", "LaserBuilder.iggyClient", IGGY),
    ("LaserBuilder", "connect_timeout"): ("Laser.connect(connect_timeout_ms=)", "connectTimeout", KW),
    ("LaserBuilder", "connection_string"): ("Laser.connect", "connectionString", KW),
    ("LaserBuilder", "credentials"): ("Laser.connect", "credentials", "Python takes the credentials in the connection string"),
    ("LaserBuilder", "dlq_topic"): ("Laser.connect(dlq_topic=)", "LaserBuilder.deadLetterTopic", "TypeScript name"),
    ("LaserBuilder", "governor"): ("Laser.with_governor", "governor", "Python sets the governor on the connected Laser"),
    ("LaserBuilder", "governor_with_retention"): ("Laser.with_governor_retention", "LaserBuilder.governor", "TypeScript passes retention as the third governor argument"),
    ("LaserBuilder", "publish_max_retries"): ("Laser.connect(publish_max_retries=)", "publishMaxRetries", KW),
    ("LaserBuilder", "publish_retry_backoff"): ("Laser.connect(publish_retry_backoff_ms=)", "publishRetryBackoff", KW),
    ("LaserBuilder", "publish_timeout"): ("Laser.connect(publish_timeout_ms=)", "publishTimeout", KW),
    ("LaserBuilder", "stream"): ("Laser.connect(stream=)", "LaserBuilder.defaultStream", "TypeScript name"),
    ("LaserBuilder", "verifier"): ("Laser.connect(verifier=)", "verifier", KW),
    ("LaserBuilder", "capabilities"): ("Laser.with_capabilities", "capabilities", "Python injects capabilities on the connected Laser"),
    ("LaserBuilder", "changes_topic"): ("Laser.connect(changes_topic=)", "changesTopic", KW),
    ("LaserBuilder", "control_topic"): ("Laser.connect(control_topic=)", "controlTopic", KW),
    ("LaserBuilder", "ops_stream"): ("Laser.connect(ops_stream=)", "opsStream", KW),
    ("Topic", "iggy_consumer"): ("-", "-", IGGY_RUST),
    ("Topic", "iggy_consumer_group"): ("-", "-", IGGY_RUST),
    ("Topic", "iggy_producer"): ("-", "-", IGGY_RUST),
    ("Topic", "json"): ("Stream.topic(cls=)", "json", "Python types a topic with the cls argument and decodes JSON"),
    ("TypedTopic", "topic"): ("-", "-", "Accessor to the untyped topic. Python and TypeScript keep the Topic that created the typed view"),
    ("PublishRequest", "content_type"): ("PublishRequest.raw_bytes(content_type=)", "contentType", "Python sets the content type with raw_bytes"),
    ("PublishRequest", "encode_with"): ("-", "encodeWith", CODEC),
    ("Producer", "batch_length"): ("Topic.producer(batch_length=)", "ProducerOptions.batchLength", KW),
    ("Producer", "background"): ("Topic.producer(background=, background_shards=)", "ProducerOptions.background", "Python passes background mode as producer keywords. TypeScript passes it in ProducerOptions"),
    ("Producer", "build"): ("Topic.producer", "Topic.producer", "Python and TypeScript build the producer in one call"),
    ("Producer", "create_stream"): ("Topic.producer(create_stream=)", "ProducerOptions.createStream", KW),
    ("Producer", "create_topic"): ("Topic.producer(create_topic=)", "ProducerOptions.createTopic", KW),
    ("Producer", "expire_after"): ("Topic.producer(message_expiry=)", "ProducerOptions.messageExpiryMicros", KW),
    ("Producer", "never_expire"): ("Topic.producer(message_expiry=)", "ProducerOptions.messageExpiryMicros", KW),
    ("Producer", "linger"): ("Topic.producer(linger_ms=)", "ProducerOptions.lingerMs", KW),
    ("Producer", "max_topic_bytes"): ("Topic.producer(max_topic_size=)", "ProducerOptions.maxTopicBytes", KW),
    ("Producer", "unlimited_topic_size"): ("Topic.producer(max_topic_size=)", "ProducerOptions.unlimitedTopicSize", KW),
    ("Producer", "partitions"): ("Topic.producer(partitions=)", "ProducerOptions.partitions", KW),
    ("Producer", "retries"): ("Topic.producer(retries=)", "ProducerOptions.retries", KW),
    ("Producer", "retry_backoff"): ("Topic.producer(retry_interval_ms=)", "ProducerOptions.retryIntervalMs", KW),
    ("Producer", "routing"): ("Topic.producer(key=, partition=)", "ProducerOptions.routing", KW),
    ("Producer", "send_batch_with_routing"): ("Producer.send_batch(key=, partition=)", "Producer.sendBatchWithRouting", "Python routes with key and partition arguments"),
    ("Producer", "send_keyed"): ("Producer.send(key=)", "Producer.sendKeyed", "Python routes with key and partition arguments"),
    ("Producer", "send_message"): ("Producer.send(headers=)", "Producer.sendMessage", "Python sends headers with send"),
    ("Producer", "send_to_partition"): ("Producer.send(partition=)", "Producer.sendToPartition", "Python routes with key and partition arguments"),
    ("Producer", "send_with_routing"): ("Producer.send(key=, partition=)", "Producer.sendWithRouting", "Python routes with key and partition arguments"),
    ("BatchingProducer", "build"): ("Topic.batching", "BatchingProducerBuilder.build", KW),
    ("BatchingProducer", "linger"): ("Topic.batching(linger_ms=)", "BatchingProducerBuilder.linger", KW),
    ("BatchingProducer", "max_bytes"): ("Topic.batching(max_bytes=)", "BatchingProducerBuilder.maxBytes", KW),
    ("BatchingProducer", "max_records"): ("Topic.batching(max_records=)", "BatchingProducerBuilder.maxRecords", KW),
    ("BatchingProducer", "partition_key"): ("Topic.batching(partition_key=)", "BatchingProducerBuilder.partitionKey", KW),
    ("Consumer", "allow_replay"): ("Topic.consumer(allow_replay=)", "ConsumerOptions.allowReplay", KW),
    ("Consumer", "auto_join_group"): ("ConsumerGroup.consumer(auto_join_group=)", "ConsumerOptions.autoJoinGroup", KW),
    ("Consumer", "batch_length"): ("Topic.consumer(batch_length=)", "ConsumerOptions.batchLength", KW),
    ("Consumer", "build"): ("Topic.consumer", "Topic.consumer", "Python and TypeScript build the consumer in one call"),
    ("Consumer", "commit_policy"): ("Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)", "ConsumerOptions.commitPolicy", KW),
    ("Consumer", "create_group"): ("ConsumerGroup.consumer(create_group=)", "ConsumerOptions.createGroup", KW),
    ("Consumer", "init_retries"): ("Topic.consumer(init_retries=, init_retry_interval_ms=)", "ConsumerOptions.initRetries", KW),
    ("Consumer", "next"): ("Consumer.next", "Consumer.stream", "TypeScript iterates with stream or for await"),
    ("Consumer", "poll_interval"): ("Topic.consumer(poll_interval_ms=)", "ConsumerOptions.pollIntervalMs", KW),
    ("Consumer", "polling_retry_interval"): ("Topic.consumer(polling_retry_interval_ms=)", "ConsumerOptions.pollingRetryIntervalMs", KW),
    ("Consumer", "start_at"): ("Topic.consumer(polling=, offset=, timestamp_micros=)", "ConsumerOptions.startFrom", KW),
    ("Consumer", "without_poll_interval"): ("Topic.consumer(poll_interval_ms=)", "ConsumerOptions.pollIntervalMs", "Pass zero to poll without a pause"),
    ("ConsumerGroup", "topic"): ("-", "-", "Accessor to the owning topic. Python and TypeScript keep the Topic that created the group"),
    ("Cursor", "batch"): ("Topic.replay(batch=)", "batch", KW),
    ("Cursor", "from_offsets"): ("Topic.replay(from_offsets=)", "fromOffsets", KW),
    ("Cursor", "stream"): ("Cursor.__aiter__", "stream", "Python iterates with async for"),
    ("Kv", "get_as"): ("-", "getAs", CODEC),
    ("KvSetRequest", "encode_with"): ("-", "encodeWith", CODEC),
    ("KvCasFencedRequest", "encode_with"): ("-", "encodeWith", CODEC),
    ("Fork", "id"): ("ForkHandle.fork_id", "Fork.forkId", "Python and TypeScript name"),
    ("Fork", "parent"): ("ForkHandle.create(parent=)", "ForkCreateRequest.parent", KW),
    ("Fork", "severed"): ("ForkHandle.create(severed=)", "ForkCreateRequest.severed", KW),
    ("Fork", "continuous"): ("ForkHandle.create(continuous=)", "ForkCreateRequest.continuous", KW),
    ("Fork", "tables"): ("ForkHandle.create(tables=)", "ForkCreateRequest.tables", KW),
    ("QueryRequest", "agg_as"): ("agg_as", "QueryRequest.aggregateAs", "TypeScript name"),
    ("QueryRequest", "fetch_all_typed"): ("QueryRequest.fetch_typed", "fetchAllTyped", "Python fetch_typed returns every page"),
    ("QueryRequest", "fetch_one_with"): ("-", "QueryRequest.fetchOne", CODEC),
    ("QueryRequest", "fetch_typed_with"): ("-", "QueryRequest.fetchTyped", CODEC),
    ("QueryRequest", "into_query"): ("QueryRequest.to_dict", "intoQuery", "Python returns the wire query as a dict"),
    ("QueryRequest", "nearest_in"): ("QueryRequest.nearest(field=)", "nearestIn", KW),
    ("QueryRequest", "next"): ("QueryRequest.rows", "QueryRequest.rows", "QueryRows::next. Python and TypeScript iterate the rows"),
    ("QueryRequest", "raw_sql_with"): ("QueryRequest.raw_sql(dialect=)", "QueryRequest.rawSql", "Python and TypeScript pass the dialect to raw_sql"),
    ("QueryRequest", "stddev"): ("stddev", "QueryRequest.stdDev", "TypeScript name"),
    ("Graph", "as_of"): ("Graph.query(as_of=)", "asOf", KW),
    ("Graph", "both"): ("Graph.query(hops=)", "both", KW),
    ("Graph", "conversation"): ("Graph.query(conversation=)", "conversation", KW),
    ("Graph", "fetch"): ("Graph.query", "fetch", KW),
    ("Graph", "incoming"): ("Graph.query(hops=)", "incoming", KW),
    ("Graph", "limit"): ("Graph.query(limit=)", "limit", KW),
    ("Graph", "out"): ("Graph.query(hops=)", "out", KW),
    ("Graph", "return_edges"): ("Graph.query(returns=)", "returnEdges", KW),
    ("Graph", "return_paths"): ("Graph.query(returns=)", "returnPaths", KW),
    ("Graph", "return_triplets"): ("Graph.query(returns=)", "returnTriplets", KW),
    ("Graph", "start_ids"): ("Graph.query(start_ids=)", "startIds", KW),
    ("Graph", "start_match"): ("Graph.query(start_match=)", "startMatch", KW),
    ("Graph", "start_nearest"): ("Graph.query(nearest=)", "startNearest", KW),
    ("Memory", "agent"): ("Memory.remember(agent=)", "RememberBuilder.agent", KW),
    ("Memory", "application"): ("Memory.remember(application=)", "RememberBuilder.application", KW),
    ("Memory", "backend"): ("Memory.backend_name", "MemoryHandle.backend", "Python returns the backend name"),
    ("Memory", "block"): ("Memory.context", "RecallBuilder.block", "Python renders the block with context(conversation, token_budget)"),
    ("Memory", "dedup"): ("Memory.remember(dedup=)", "RememberBuilder.dedup", KW),
    ("Memory", "durable"): ("Memory.remember(durable=)", "RememberBuilder.durable", KW),
    ("Memory", "embedder"): ("Laser.memory_with(embedder=)", "MemoryHandle.vector", "Python and TypeScript pass the embedder when they open a vector handle"),
    ("Memory", "folded"): ("Memory.recall(folded=)", "RecallBuilder.folded", KW),
    ("Memory", "hybrid"): ("Memory.recall(strategy=)", "RecallBuilder.hybrid", "Python selects the strategy by name"),
    ("Memory", "keyword"): ("Memory.recall(strategy=)", "RecallBuilder.keyword", "Python selects the strategy by name"),
    ("Memory", "kind"): ("Memory.remember(kind=)", "RememberBuilder.kind", KW),
    ("Memory", "limit"): ("Memory.recall(limit=)", "RecallBuilder.limit", KW),
    ("Memory", "recall_folded"): ("Memory.recall(folded=)", "MemoryHandle.recallFolded", KW),
    ("Memory", "recent"): ("Memory.recall(strategy=)", "RecallBuilder.recent", "Python selects the strategy by name"),
    ("Memory", "scope"): ("Memory.remember(conversation=)", "RememberBuilder.conversation", "Python and TypeScript scope by conversation"),
    ("Memory", "semantic"): ("Memory.recall(semantic=)", "RecallBuilder.semantic", KW),
    ("Memory", "send"): ("Memory.remember", "RememberBuilder.send", "Python remembers in one call"),
    ("Memory", "strategy"): ("Memory.recall(strategy=)", "RecallBuilder.strategy", KW),
    ("Memory", "stream"): ("Memory.remember(stream=)", "RememberBuilder.stream", KW),
    ("Memory", "user"): ("Memory.remember(user=)", "RememberBuilder.user", KW),
    ("ContextScope", "laser"): ("-", "-", "Accessor to the owning connection. Python and TypeScript keep the Laser that created the scope"),
    ("ContextScope", "memory_with"): ("ContextScope.memory", "ContextScope.memory", "Python and TypeScript pass a memory handle to memory"),
    ("ContextScope", "new"): ("TokenBudget.__new__", "TokenBudget.constructor", CTOR),
    ("ContextScope", "with_estimator"): ("TokenBudget.__new__(estimator=)", "TokenBudget.constructor", "Python and TypeScript take the estimator as the second constructor argument"),
    ("Sessions", "config"): ("-", "config", "Python configures sessions with keyword arguments and exposes no config object"),
    ("Sessions", "context_token_bound"): ("-", "SessionConfig.contextTokens", "Python exposes no config object"),
    ("Sessions", "context_tokens"): ("Laser.sessions(context_tokens=)", "SessionConfig.contextTokens", OPT),
    ("Sessions", "context_turn_bound"): ("-", "SessionConfig.contextTurns", "Python exposes no config object"),
    ("Sessions", "context_turns"): ("Laser.sessions(context_turns=)", "SessionConfig.contextTurns", OPT),
    ("Sessions", "kind_for"): ("-", "SessionConfig.kindFor", "Python exposes no config object"),
    ("Sessions", "memory_namespace"): ("Laser.sessions(memory_namespace=)", "SessionConfig.memoryNamespace", OPT),
    ("Sessions", "memory_namespace_name"): ("-", "SessionConfig.memoryNamespace", "Python exposes no config object"),
    ("Sessions", "new"): ("Laser.sessions", "SessionConfig.constructor", CTOR),
    ("Sessions", "stream"): ("Laser.sessions(stream=)", "SessionConfig.stream", OPT),
    ("Sessions", "stream_name"): ("-", "SessionConfig.stream", "Python exposes no config object"),
    ("Sessions", "topic"): ("Laser.sessions(topics=)", "SessionConfig.topics", OPT),
    ("Sessions", "topic_for"): ("-", "SessionConfig.topicFor", "Python exposes no config object"),
    ("Sessions", "topics"): ("-", "SessionConfig.topicList", "Python exposes no config object"),
    ("Session", "config"): ("-", "Session.config", "Python exposes no config object"),
    ("Session", "memory_in"): ("Session.memory_in", "Session.memory", "TypeScript passes the namespace to memory"),
    ("Session", "text"): ("SessionTurn.text", "fn:sessionTurnText", "TypeScript uses the free function"),
    ("Contract", "conversation"): ("Laser.contract(conversation=)", "conversation", KW),
    ("Contract", "deadline"): ("Laser.contract(deadline_ms=)", "deadline", KW),
    ("Contract", "expire_if_not_consumed"): ("Laser.contract(expire_if_not_consumed_ms=)", "expireIfNotConsumed", KW),
    ("Contract", "fence"): ("Laser.contract(fence=)", "fence", KW),
    ("Contract", "from"): ("Laser.contract(source=)", "from", KW),
    ("Contract", "inbox_route"): ("Laser.contract(fixed_inbox=)", "inboxRoute", KW),
    ("Contract", "payload"): ("Laser.contract(payload=)", "payload", KW),
    ("Contract", "registered"): ("Laser.contract(registered=)", "registered", KW),
    ("Contract", "reply_on"): ("Laser.contract(reply_on=)", "replyOn", KW),
    ("Contract", "send"): ("Laser.contract_report", "send", "Python sends with contract or contract_report"),
    ("Workflow", "after"): ("Workflow.step(after=)", "StepBuilder.after", KW),
    ("Workflow", "compensate_with"): ("Workflow.step(compensate=)", "StepBuilder.compensateWith", KW),
    ("Workflow", "exclusive"): ("Workflow.step(exclusive=)", "StepBuilder.exclusive", KW),
    ("Workflow", "exclusive_in"): ("Workflow.step(fence_namespace=)", "StepBuilder.exclusiveIn", KW),
    ("Workflow", "inbox_route"): ("Laser.workflow(fixed_inbox=)", "Workflow.inboxRoute", KW),
    ("Workflow", "on_timeout"): ("Workflow.step(on_timeout=)", "StepBuilder.onTimeout", KW),
    ("Workflow", "verify_with"): ("Workflow.step(verify=)", "StepBuilder.verifyWith", KW),
    ("Runs", "agent"): ("Runs.list(agent_id=)", "RunListRequest.agent", KW),
    ("Runs", "cursor"): ("Runs.list(cursor=)", "RunListRequest.cursor", KW),
    ("Runs", "fetch"): ("Runs.list", "RunListRequest.fetch", KW),
    ("Runs", "limit"): ("Runs.list(limit=)", "RunListRequest.limit", KW),
    ("Runs", "state"): ("Runs.list(state=)", "RunListRequest.state", KW),
    ("Destinations", "mutate_with_supervisor_assertion"): ("Destinations.mutate(supervisor_assertion=)", "mutateWithSupervisorAssertion", KW),
    ("Projections", "apply"): ("Laser.apply_binding", "Bindings.apply", "Python flattens the handles onto Laser"),
    ("Projections", "drop"): ("Laser.drop_projection", "Projections.drop", "Python flattens the handles onto Laser (drop_projection, drop_schema)"),
    ("Projections", "drop_graph"): ("Laser.drop_graph", "Projections.dropGraph", "Python flattens the handles onto Laser"),
    ("Projections", "get"): ("Laser.get_projection", "Projections.get", "Python flattens the handles onto Laser (get_projection, get_schema)"),
    ("Projections", "list"): ("Laser.list_projections", "Projections.list", "Python flattens the handles onto Laser (list_projections, list_schemas)"),
    ("Projections", "register"): ("Laser.register_projection", "Projections.register", "Python flattens the handles onto Laser (register_projection, register_schema)"),
    ("Projections", "register_graph"): ("Laser.register_graph", "Projections.registerGraph", "Python flattens the handles onto Laser"),
    ("Projections", "remove"): ("Laser.remove_binding", "Bindings.remove", "Python flattens the handles onto Laser"),
    ("Watch", "index"): ("Laser.watch(index=)", "Watch.index", KW),
    ("Watch", "records"): ("Laser.watch", "Watch.records", "Python opens the reader with watch"),
    ("Watch", "stream"): ("WatchReader.__aiter__", "WatchReader.stream", "Python iterates with async for"),
    ("Governance", "new"): ("QuorumGovernor.__new__", "QuorumGovernor.constructor", CTOR),
    ("Signing", "from_bytes"): ("SigningKey.__new__", "SigningKey.fromBytes", "Python constructs the key from its secret bytes"),
    ("Signing", "new"): ("KeyRegistry.__new__", "new:KeyRegistry", CTOR),
    ("A2aBridge", "new"): ("Laser.a2a_bridge", "A2aBridge.constructor", CTOR),
    ("A2aBridge", "router"): ("-", "A2aBridge.handleRpc", HTTP + ". TypeScript serves JSON-RPC through handleRpc"),
    ("A2aBridge", "with_capabilities"): ("Laser.a2a_bridge(capabilities=)", "withCapabilities", KW),
    ("A2aBridge", "with_signing_key"): ("Laser.a2a_bridge(signing_key=)", "withSigningKey", KW),
    ("McpBridge", "new"): ("Laser.mcp_bridge", "McpBridge.constructor", CTOR),
    ("McpBridge", "router"): ("-", "McpBridge.handleRpc", HTTP + ". TypeScript serves JSON-RPC through handleRpc"),
    ("McpBridge", "with_memory_tools"): ("Laser.mcp_bridge(memory_tools=)", "withMemoryTools", KW),
    ("McpBridge", "with_prompt"): ("Laser.mcp_bridge(prompts=)", "withPrompt", KW),
    ("McpBridge", "with_resource"): ("Laser.mcp_bridge(resources=)", "withResource", KW),
    ("McpBridge", "with_timeout"): ("Laser.mcp_bridge(timeout_secs=)", "withTimeout", KW),
    ("McpBridge", "with_tool"): ("Laser.mcp_bridge(tools=)", "withTool", KW),
    ("LaserError", "code"): ("code", "fn:code", "TypeScript uses the free function"),
    ("LaserError", "filter_reason"): ("FilterError.reason", "fn:filterReason", "Python reads reason on FilterError. TypeScript uses the free function"),
    ("LaserError", "publish_cause"): ("-", "PublishFailedError.publishCause", "Python raises the cause class itself with committed and unconfirmed_count attributes"),
    ("LaserError", "rejected"): ("-", "-", "Rust constructor for SDK internals"),
    ("LaserError", "unsupported"): ("-", "-", "Rust constructor for SDK internals"),
    ("LaserError", "unsupported_feature"): ("-", "-", "Rust constructor for SDK internals"),
}
for _name in ["ambiguous_mutation", "budget_exceeded", "fence_violation", "lease_lost", "no_capable_agent", "not_found", "not_leader", "permission_denied", "quarantined", "retryable", "stale", "stream_or_topic_not_found", "unavailable", "unsupported", "version_conflict", "version_skew"]:
    OVERRIDES[("LaserError", f"is_{_name}")] = (f"LaserError.{_name}", "fn:" + camel(f"is_{_name}"), "Python reads a boolean attribute. TypeScript uses the free function")
for _flag, _kw in [("a2a_gateway", "a2a_gateway"), ("agent_workflow", "agent_workflow"), ("destinations", "destinations"), ("destination_consistency", "destinations_consistency"), ("filters", "filters"), ("forks", "forks"), ("graph", "graph"), ("kv", "kv"), ("kv_cas", "kv_cas"), ("kv_cas_fenced", "kv_cas_fenced"), ("kv_fenced_leases", "kv_fenced_leases"), ("managed", "managed"), ("query", "query"), ("query_consistency", "query_consistency"), ("query_keyword", "query_keyword")]:
    OVERRIDES[("Capabilities", f"with_{_flag}")] = (f"Laser.with_capabilities({_kw}=)", "Laser.withCapabilities", "Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object")
OVERRIDES[("Capabilities", "with_backends")] = ("Laser.with_capabilities(backends=)", "Laser.withCapabilities", "TypeScript passes a Capabilities object")
OVERRIDES[("Capabilities", "with_versions")] = ("Laser.with_capabilities(versions=)", "Laser.withCapabilities", "TypeScript passes a Capabilities object")
OVERRIDES[("Capabilities", "with_query_execution")] = ("Laser.with_capabilities(query_execution=)", "Laser.withCapabilities", "TypeScript passes a Capabilities object")


# Per-language peers for the Rust types outside SECTIONS, keyed by (Rust type,
# method). Each entry is (spelling, note) in the OVERRIDES grammar. A row with
# no entry resolves by name on the same-named class.
INTERNAL = "Send instrumentation the SDK producers drive. A caller records nothing by hand"
PY_PEERS = {
    ("SharedConsolidator", "new"): ("Laser.spawn_agent(consolidator=)", "Python takes the runtime consolidator callback"),
    ("AgdxSend", "claim_check"): ("Agdx.command(claim_check=)", KW),
    ("AgdxSend", "with_cause"): ("Agdx.command(cause=)", KW),
    ("AgdxSend", "with_deadline_micros"): ("Agdx.command(deadline_micros=)", KW),
    ("AgdxSend", "with_idempotency_key"): ("Agdx.command(idempotency_key=)", KW),
    ("AgdxSend", "with_metadata"): ("Agdx.command(metadata=)", KW),
    ("AgdxSend", "with_tool"): ("Agdx.command(tool=)", KW),
    ("AgdxSend", "with_usage"): ("Agdx.command(usage=)", KW),
    ("BatchPublishRequest", "add_encoded_with_projection"): ("BatchPublishRequest.add_raw_bytes(projection_ref=)", KW),
    ("BatchPublishRequest", "add_json_with_projection"): ("BatchPublishRequest.add_json(projection_ref=)", KW),
    ("BatchPublishRequest", "add_msgpack_with_projection"): ("BatchPublishRequest.add_msgpack(projection_ref=)", KW),
    ("BatchPublishRequest", "add_payload_with_projection"): ("BatchPublishRequest.add_payload(projection_ref=)", KW),
    ("BatchPublishRequest", "add_raw_bytes_with_projection"): ("BatchPublishRequest.add_raw_bytes(projection_ref=)", KW),
    ("CapabilitySelector", "new"): ("Laser.contract(policy=)", KW),
    ("DefaultConsolidator", "prune_summarized"): ("Memory.consolidate(prune_summarized=)", KW),
    ("DefaultConsolidator", "with_summarizer"): ("Memory.consolidate(summarizer=)", KW),
    ("MemoryHandler", "auto_remember"): ("MemoryHandler.auto_remember", ""),
    ("MemoryHandler", "new"): ("new:MemoryHandler", ""),
    ("MemoryId", "content"): ("Memory.content_id", ""),
    ("MemoryKind", "class"): ("Memory.kind_class", ""),
    ("ProjectionsRequest", "for_topics"): ("Laser.list_projections(topics=)", KW),
    ("ProjectionsRequest", "search"): ("Laser.list_projections(search=)", KW),
    ("RegisteredCard", "available_for"): ("AgentRegistry.card_available_for", ""),
    ("RegisteredCard", "is_fresh"): ("AgentRegistry.card_is_fresh", ""),
    ("RegisteredCard", "serves"): ("AgentRegistry.card_serves", ""),
    ("SessionTurnKind", "for_topic"): ("Sessions.turn_kind", ""),
    ("SessionTurnKind", "topic"): ("Sessions.turn_topic", ""),
    ("SlidingWindow", "new"): ("Laser.spawn_agent(dedup_window=)", KW),
    ("ActionKind", "as_str"): ("GovernedAction.kind", "Kinds are plain str in Python"),
    ("AgdxSend", "body"): ("Agdx.status(body=)", KW),
    ("AgdxSend", "content_type"): ("Agdx.command(content_type=)", KW),
    ("AgdxSend", "last"): ("Agdx.status(last=)", KW),
    ("AgdxSend", "send"): ("Agdx.command", "Each Python verb publishes when awaited"),
    ("AgdxSend", "signed_by"): ("Laser.agdx(signing_key=)", "Python sets the signing key once per producer"),
    ("AgdxSend", "with_correlation"): ("Agdx.status(correlation=)", KW),
    ("AgdxSend", "with_operation"): ("Agdx.command(operation=)", KW),
    ("AgdxSend", "with_target"): ("Agdx.command(target=)", KW),
    ("AgdxSend", "with_task_state"): ("Agdx.status(task_state=)", KW),
    ("AgentId", "as_str"): ("-", "Agent ids are plain str in Python"),
    ("AgentId", "new"): ("-", "Agent ids are plain str in Python"),
    ("AgentId", "wire_id"): ("-", "Wire-encoding conversion for SDK internals"),
    ("AgentTopic", "as_identifier"): ("-", IGGY_RUST),
    ("AgentTopic", "name"): ("-", "Topics are plain str in Python"),
    ("AgentTopic", "topic_string"): ("-", "Topics are plain str in Python"),
    ("BatchPublishRequest", "add_encoded"): ("BatchPublishRequest.add_raw_bytes", CODEC),
    ("BatchPublishRequest", "content_type"): ("BatchPublishRequest.add_raw_bytes", "Python gives the content type per record"),
    ("BatchPublishRequest", "extend_encoded"): ("BatchPublishRequest.extend_json", CODEC),
    ("BatchPublishRequest", "is_empty"): ("BatchPublishRequest.__len__", "Python uses len(batch)"),
    ("BatchPublishRequest", "len"): ("BatchPublishRequest.__len__", "Python uses len(batch)"),
    ("Budget", "invocations"): ("Workflow.budget(invocations=)", KW),
    ("Budget", "tokens"): ("Workflow.budget(tokens=)", KW),
    ("Budget", "unlimited"): ("Workflow.budget", "The default when no keyword is passed"),
    ("Budget", "wall_clock"): ("Workflow.budget(wall_clock_ms=)", KW),
    ("CapabilitySelector", "principal"): ("Laser.contract(principal=)", KW),
    ("ChunkAssembler", "is_finished"): ("ChunkAssembler.finished", "Python reads a property"),
    ("ChunkAssembler", "new"): ("new:ChunkAssembler", CTOR),
    ("ClientMetadataRequest", "after"): ("Laser.client_metadata(after=)", KW),
    ("ClientMetadataRequest", "all"): ("Laser.client_metadata(after=)", "Python walks pages with the returned next_cursor"),
    ("ClientMetadataRequest", "limit"): ("Laser.client_metadata(limit=)", KW),
    ("ClientMetadataRequest", "page"): ("Laser.client_metadata", KW),
    ("ClientMetadataRequest", "principal"): ("Laser.client_metadata(principal=)", KW),
    ("ClientMetadataRequest", "with_metadata_only"): ("Laser.client_metadata(metadata_only=)", KW),
    ("ConsumerGroupName", "as_str"): ("ConsumerGroup.name", "Group names are plain str in Python"),
    ("ConsumerGroupName", "for_agent"): ("Laser.spawn_agent(consumer_group=)", "Leaving consumer_group unset uses the agent id"),
    ("ConsumerGroupName", "new"): ("Topic.consumer_group", "Group names are plain str in Python"),
    ("ContextAssembler", "assemble"): ("Laser.assemble_context(topics=, last_n=, roles=, token_budget=)", KW),
    ("ConversationId", "as_u128"): ("-", "Conversation ids are plain str in Python"),
    ("ConversationId", "derive"): ("fn:derive_conversation_id", ""),
    ("ConversationId", "new"): ("fn:new_conversation_id", ""),
    ("ConversationState", "load"): ("ContextScope.state", ""),
    ("ConversationState", "load_with"): ("ContextScope.state_with", ""),
    ("CrashContext", "assemble"): ("new:CrashContext", "The Python constructor assembles"),
    ("CreateConsumerGroup", "build"): ("ConsumerGroup.create", KW),
    ("CreateConsumerGroup", "filter"): ("ConsumerGroup.create(filter=)", KW),
    ("CreateConsumerGroup", "operation_id"): ("ConsumerGroup.create(operation_id=)", KW),
    ("CreateConsumerGroup", "policy"): ("ConsumerGroup.create(filter_id=, revision=)", KW),
    ("DedicatedKvTransport", "new"): ("new:DedicatedKvTransport", ""),
    ("DefaultConsolidator", "new"): ("Memory.consolidate", KW),
    ("EdgeDenial", "challenge"): ("fn:authorize_edge", "Python returns the challenge in a tuple"),
    ("EdgeDenial", "code"): ("fn:authorize_edge", "A denial without a challenge is unauthenticated, with one it is a step-up"),
    ("Feedback", "new"): ("Memory.improve", "Python passes the id and weight to improve"),
    ("FencedLeaseClient", "acquire"): ("FencedLeaseClient.acquire", ""),
    ("FencedLeaseClient", "cas_fenced"): ("FencedLeaseClient.cas_fenced", ""),
    ("FencedLeaseClient", "connect_dedicated"): ("FencedLeaseClient.connect_dedicated", ""),
    ("FencedLeaseClient", "get"): ("FencedLeaseClient.get", ""),
    ("FencedLeaseClient", "new"): ("new:FencedLeaseClient", ""),
    ("FencedLeaseClient", "prepare_acquire"): ("FencedLeaseClient.prepare_acquire", ""),
    ("FencedLeaseClient", "prepare_cas_fenced"): ("FencedLeaseClient.prepare_cas_fenced", ""),
    ("FencedLeaseClient", "prepare_release"): ("FencedLeaseClient.prepare_release", ""),
    ("FencedLeaseClient", "prepare_renew"): ("FencedLeaseClient.prepare_renew", ""),
    ("FencedLeaseClient", "release"): ("FencedLeaseClient.release", ""),
    ("FencedLeaseClient", "renew"): ("FencedLeaseClient.renew", ""),
    ("FencedLeaseClient", "with_attempt_timeout"): ("FencedLeaseClient.with_attempt_timeout", ""),
    ("FileStore", "new"): ("new:FileStore", CTOR),
    ("FilterCaps", "evaluates"): ("Capabilities.evaluation", "Python compares the announced evaluator and codecs"),
    ("FilterPreviewBuilder", "explain"): ("GroupFilter.preview(explain=)", KW),
    ("FilterPreviewBuilder", "from_offset"): ("GroupFilter.preview(from_offset=)", KW),
    ("FilterPreviewBuilder", "max_examined"): ("GroupFilter.preview(max_examined=)", KW),
    ("FilterPreviewBuilder", "max_records"): ("GroupFilter.preview(max_records=)", KW),
    ("FilterPreviewBuilder", "send"): ("GroupFilter.preview", KW),
    ("FilteredReaderBuilder", "build"): ("ConsumerGroup.reader", KW),
    ("FilteredReaderBuilder", "count"): ("ConsumerGroup.reader(count=)", KW),
    ("FilteredReaderBuilder", "idle_interval"): ("ConsumerGroup.reader(idle_interval=)", KW),
    ("FilteredReaderBuilder", "local_guard"): ("ConsumerGroup.reader(local_guard=)", KW),
    ("FilteredReaderBuilder", "max_examined"): ("ConsumerGroup.reader(max_examined=)", KW),
    ("FilteredReaderBuilder", "max_reply_bytes"): ("ConsumerGroup.reader(max_reply_bytes=)", KW),
    ("FilteredReaderBuilder", "max_unacked_pages"): ("ConsumerGroup.reader(max_unacked_pages=)", KW),
    ("FilteredReaderBuilder", "partition"): ("ConsumerGroup.reader(partitions=)", KW),
    ("FilteredReaderBuilder", "read_mode"): ("ConsumerGroup.reader(read_mode=)", KW),
    ("FilteredReaderBuilder", "start"): ("ConsumerGroup.reader(start=, start_offset=, start_timestamp_micros=)", KW),
    ("Gather", "replies"): ("AgentCtx.fan_out", "Python returns the replies in the fan_out result"),
    ("GovernorMode", "as_str"): ("PolicyEvidence.mode", "Modes are plain str in Python"),
    ("GroupFilter", "configure_as"): ("GroupFilter.configure(operation_id=)", KW),
    ("GroupFilter", "configure_with"): ("GroupFilter.configure(filter_id=, revision=)", KW),
    ("InMemoryStore", "new"): ("new:InMemoryStore", CTOR),
    ("InboxRoute", "resolve"): ("AgentRegistry.inbox_for", "Python resolves an advertised inbox through the registry"),
    ("IntentId", "new"): ("new:Intent", "Intent mints its own id"),
    ("KeyRecord", "agent"): ("new:KeyRecord", "The Python constructor defaults to the agent kind"),
    ("KeyRecord", "from_verifying_bytes"): ("KeyRecord.__new__(kind=)", CTOR),
    ("KeyRecord", "operator"): ("KeyRecord.__new__(kind=)", CTOR),
    ("KeyRecord", "valid_window"): ("KeyRecord.__new__(valid_from_micros=, valid_to_micros=)", CTOR),
    ("KvKeyRegistry", "enroll_record"): ("KvKeyRegistry.enroll", ""),
    ("KvKeyRegistry", "in_namespace"): ("KvKeyRegistry.__new__(namespace=)", CTOR),
    ("KvKeyRegistry", "new"): ("new:KvKeyRegistry", CTOR),
    ("KvSnapshotStore", "in_namespace"): ("Laser.kv_snapshot_store(namespace=)", KW),
    ("KvSnapshotStore", "new"): ("Laser.kv_snapshot_store", ""),
    ("LogMemory", "fetch_named"): ("Memory.fetch", ""),
    ("LogMemory", "fetch_named_folded"): ("Memory.fetch_folded", ""),
    ("LogMemory", "forget_named"): ("Memory.remove", ""),
    ("LogMemory", "in_namespace"): ("Laser.memory", ""),
    ("LogMemory", "new"): ("Laser.memory", "Python always names the namespace"),
    ("LogMemory", "on_stream_topic"): ("Laser.memory_on_topic(stream=)", KW),
    ("LogMemory", "on_stream_topic_named"): ("Laser.memory_on_topic(stream=)", KW),
    ("LogMemory", "on_topic"): ("Laser.memory_on_topic", ""),
    ("LogMemory", "on_topic_named"): ("Laser.memory_on_topic", ""),
    ("LogMemory", "recall_folded"): ("Memory.recall(folded=)", KW),
    ("LogMemory", "set_named"): ("Memory.set", ""),
    ("LogMemory", "update_named"): ("Memory.update", ""),
    ("MatchedRecord", "json"): ("ConsumerMessage.json", "Python reads it through MatchedRecord.message"),
    ("MemoryId", "as_u128"): ("MemoryItem.id", "Memory ids are plain str in Python"),
    ("MemoryId", "from_u128"): ("Memory.forget", "Memory ids are plain str in Python"),
    ("MemoryId", "new"): ("-", "Memory.remember returns the minted id"),
    ("MemoryKind", "code"): ("-", "The byte only feeds the content id hash"),
    ("MemoryKind", "from_word"): ("Memory.remember(kind=)", "Kinds are plain str in Python"),
    ("MemoryTopicBuilder", "build"): ("Laser.memory_topic", KW),
    ("MemoryTopicBuilder", "no_expiry"): ("Laser.memory_topic(ttl_secs=)", "A ttl_secs of zero or less disables expiry"),
    ("MemoryTopicBuilder", "partitions"): ("Laser.memory_topic(partitions=)", KW),
    ("MemoryTopicBuilder", "stream"): ("Laser.memory_topic(stream=)", KW),
    ("MemoryTopicBuilder", "ttl"): ("Laser.memory_topic(ttl_secs=)", KW),
    ("MessageId", "new"): ("new:Provenance", "Python passes the partition:offset str as causal_parent"),
    ("PreparedMutation", "ambiguous_recovery"): ("PreparedMutation.ambiguous_recovery", ""),
    ("PreparedMutation", "operation_id"): ("PreparedMutation.operation_id", ""),
    ("PrincipalId", "get"): ("AgentRegistry.principal_for", "Principals are plain int in Python"),
    ("PrincipalId", "new"): ("Laser.contract(principal=)", "Principals are plain int in Python"),
    ("ProducerMessage", "header"): ("Producer.send(headers=)", KW),
    ("ProducerMessage", "new"): ("Producer.send", "Python passes the payload to send"),
    ("ProducerMessage", "with_headers"): ("Producer.send(headers=)", KW),
    ("ProducerObservation", "finish"): ("-", INTERNAL),
    ("ProducerRecorder", "begin"): ("-", INTERNAL),
    ("ProducerRecorder", "new"): ("-", INTERNAL),
    ("ProducerRecorder", "snapshot"): ("-", INTERNAL),
    ("ProjectionsRequest", "fetch"): ("Laser.list_projections", KW),
    ("ProjectionsRequest", "for_topic"): ("Laser.list_projections(topic=)", KW),
    ("ProjectionsRequest", "id_prefix"): ("Laser.list_projections(id_prefix=)", KW),
    ("ProjectionsRequest", "name_contains"): ("Laser.list_projections(name_contains=)", KW),
    ("Provenance", "partition_key"): ("Provenance.conversation_id", "The partition key is the conversation id"),
    ("RegisterSchemaRequest", "name"): ("Laser.register_schema(name=)", KW),
    ("RegisterSchemaRequest", "send"): ("Laser.register_schema", KW),
    ("RegisterSchemaRequest", "version"): ("Laser.register_schema(version=)", KW),
    ("ReliableConsumer", "run"): ("Laser.spawn_agent", "spawn_agent runs the reliable consumer"),
    ("RerankedMemory", "new"): ("Memory.reranker", ""),
    ("RetryPolicy", "backoff"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", KW),
    ("Router", "all_capable"): ("Workflow.step(all_capable=)", KW),
    ("Router", "apply"): ("new:Provenance", "Python stamps the target with target_agent_id"),
    ("Router", "broadcast"): ("new:Provenance", "Leave target_agent_id unset"),
    ("Router", "resolve_targets"): ("AgentRegistry.resolve(now_micros=)", ""),
    ("Router", "to"): ("Workflow.step(to=)", KW),
    ("Router", "to_capable"): ("Workflow.step(to_capable=)", KW),
    ("Router", "to_principal"): ("Workflow.step(to=, principal=)", KW),
    ("Routing", "key"): ("Topic.producer(key=)", KW),
    ("ScatterReport", "completed"): ("Laser.scatter_report", "Python returns dicts with a state field"),
    ("ScatterReport", "failures"): ("Laser.scatter_report", "Python returns dicts with a state field"),
    ("SessionPolicy", "conversation_for"): ("fn:derive_conversation_id", "PerUser derives the id. PerCall uses new_conversation_id"),
    ("TestClock", "advance"): ("TestClock.advance", ""),
    ("TestClock", "new"): ("new:TestClock", CTOR),
    ("TestClock", "set"): ("TestClock.set", ""),
    ("TopicSnapshotStore", "new"): ("Laser.topic_snapshot_store", ""),
    ("TopicSnapshotStore", "on_topic"): ("Laser.topic_snapshot_store(topic=)", KW),
    ("TypedQueryRows", "next"): ("QueryRequest.rows_typed", "Python returns the bounded list"),
    ("TypedRecords", "batch"): ("Topic.records(batch=)", KW),
    ("TypedRecords", "from_offsets"): ("Topic.records(from_offsets=)", KW),
    ("TypedRecords", "poll"): ("TypedRecords.next", "Python yields one record per call"),
    ("TypedRecords", "stream"): ("TypedRecords.__aiter__", "Python iterates with async for"),
    ("VectorMemory", "governed"): ("Laser.vector_memory", ""),
    ("VectorMemory", "new"): ("Memory.vector", ""),
    ("Verdict", "as_str"): ("PolicyEvidence.decision", "Verdicts are plain str in Python"),
}
TS_PEERS = {
    ("ActionKind", "as_str"): ("-", "ActionKind is a string literal union in TypeScript"),
    ("AgentCtx", "approval_gate"): ("AgentContext.approvalGate", ""),
    ("AgentCtx", "fan_out"): ("AgentContext.fanOut", ""),
    ("AgentCtx", "laser"): ("AgentContext.laser", ""),
    ("AgentCtx", "message"): ("AgentContext.message", ""),
    ("AgentCtx", "reply_on"): ("AgentContext.replyOn", ""),
    ("AgentCtx", "request"): ("AgentContext.request", ""),
    ("AgentCtx", "respond"): ("AgentContext.respond", ""),
    ("AgentCtx", "respond_input"): ("AgentContext.respondInput", ""),
    ("AgentCtx", "send"): ("AgentContext.send", ""),
    ("AgentCtx", "spawn_subconversation"): ("AgentContext.spawnSubconversation", ""),
    ("AgentId", "as_str"): ("AgentId.asString", ""),
    ("AgentId", "wire_id"): ("fn:parseWireAgentId", TS_FREE),
    ("AgentMessage", "body"): ("fn:agentMessageBody", TS_FREE),
    ("AgentTopic", "as_identifier"): ("-", IGGY_RUST),
    ("AgentTopic", "name"): ("-", "AgentTopic is a string literal union in TypeScript"),
    ("AgentTopic", "topic_string"): ("-", "AgentTopic is a string literal union in TypeScript"),
    ("BatchPublishRequest", "add_msgpack_with_projection"): ("BatchPublishRequest.addMessagePackWithProjection", ""),
    ("BatchPublishRequest", "extend_msgpack"): ("BatchPublishRequest.extendMessagePack", ""),
    ("BatchPublishRequest", "len"): ("BatchPublishRequest.length", ""),
    ("CapabilitySelector", "new"): ("fn:capabilitySelector", TS_FREE),
    ("ChunkAssembler", "new"): ("new:ChunkAssembler", CTOR),
    ("CompiledSchema", "encode_avro"): ("CompiledSchema.encode", "TypeScript encodes every schema kind with encode"),
    ("ConsumerGroupName", "as_str"): ("ConsumerGroupName.asString", ""),
    ("ConversationState", "load"): ("ConversationState.load", ""),
    ("ConversationState", "load_with"): ("ConversationState.loadWith", ""),
    ("CrashContext", "assemble"): ("CrashContext.constructor", CTOR),
    ("CreateConsumerGroup", "build"): ("ConsumerGroup.create", OPT),
    ("CreateConsumerGroup", "filter"): ("CreateConsumerGroupOptions.filter", OPT),
    ("CreateConsumerGroup", "operation_id"): ("CreateConsumerGroupOptions.operationId", OPT),
    ("CreateConsumerGroup", "policy"): ("CreateConsumerGroupOptions.policy", OPT),
    ("DedicatedKvTransport", "new"): ("DedicatedKvTransport.constructor", ""),
    ("DefaultConsolidator", "new"): ("MemoryHandle.consolidate", ""),
    ("DefaultConsolidator", "prune_summarized"): ("ConsolidateOptions.pruneSummarized", OPT),
    ("DefaultConsolidator", "with_summarizer"): ("ConsolidateOptions.summarizer", OPT),
    ("EdgeDenial", "challenge"): ("fn:edgeDenialChallenge", TS_FREE),
    ("EdgeDenial", "code"): ("fn:edgeDenialCode", TS_FREE),
    ("Feedback", "new"): ("Feedback.target", "TypeScript writes the Feedback object literal"),
    ("FencedLeaseClient", "acquire"): ("FencedLeaseClient.acquire", ""),
    ("FencedLeaseClient", "cas_fenced"): ("FencedLeaseClient.casFenced", ""),
    ("FencedLeaseClient", "connect_dedicated"): ("FencedLeaseClient.connectDedicated", ""),
    ("FencedLeaseClient", "get"): ("FencedLeaseClient.get", ""),
    ("FencedLeaseClient", "new"): ("FencedLeaseClient.constructor", ""),
    ("FencedLeaseClient", "prepare_acquire"): ("FencedLeaseClient.prepareAcquire", ""),
    ("FencedLeaseClient", "prepare_cas_fenced"): ("FencedLeaseClient.prepareCasFenced", ""),
    ("FencedLeaseClient", "prepare_release"): ("FencedLeaseClient.prepareRelease", ""),
    ("FencedLeaseClient", "prepare_renew"): ("FencedLeaseClient.prepareRenew", ""),
    ("FencedLeaseClient", "release"): ("FencedLeaseClient.release", ""),
    ("FencedLeaseClient", "renew"): ("FencedLeaseClient.renew", ""),
    ("FencedLeaseClient", "with_attempt_timeout"): ("FencedLeaseClient.withAttemptTimeout", ""),
    ("FileStore", "new"): ("FileStore.constructor", CTOR),
    ("FilterCaps", "evaluates"): ("fn:filterCapsEvaluates", TS_FREE),
    ("FilterPreviewBuilder", "explain"): ("FilterPreviewOptions.explain", OPT),
    ("FilterPreviewBuilder", "from_offset"): ("FilterPreviewOptions.fromOffset", OPT),
    ("FilterPreviewBuilder", "max_examined"): ("FilterPreviewOptions.maxExamined", OPT),
    ("FilterPreviewBuilder", "max_records"): ("FilterPreviewOptions.maxRecords", OPT),
    ("FilterPreviewBuilder", "send"): ("GroupFilter.preview", OPT),
    ("FilteredReader", "idle_interval"): ("FilteredReader.idleIntervalMs", "TypeScript name"),
    ("Gather", "replies"): ("fn:gatherReplies", TS_FREE),
    ("GovernorMode", "as_str"): ("-", "GovernorMode is a string literal union in TypeScript"),
    ("InMemoryStore", "new"): ("new:InMemoryStore", CTOR),
    ("InboxRoute", "resolve"): ("fn:resolveInboxRoute", TS_FREE),
    ("KeyRecord", "from_verifying_bytes"): ("KeyRecord.constructor", CTOR),
    ("KvKeyRegistry", "in_namespace"): ("KvKeyRegistry.constructor", CTOR),
    ("KvKeyRegistry", "new"): ("KvKeyRegistry.constructor", CTOR),
    ("KvSnapshotStore", "in_namespace"): ("KvSnapshotStore.constructor", CTOR),
    ("KvSnapshotStore", "new"): ("KvSnapshotStore.constructor", CTOR),
    ("LogMemory", "fetch_named"): ("LogMemory.fetch", ""),
    ("LogMemory", "fetch_named_folded"): ("LogMemory.fetchFolded", ""),
    ("LogMemory", "forget_named"): ("LogMemory.remove", ""),
    ("LogMemory", "in_namespace"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "new"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "on_stream_topic"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "on_stream_topic_named"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "on_topic"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "on_topic_named"): ("LogMemory.constructor", CTOR),
    ("LogMemory", "set_named"): ("LogMemory.set", ""),
    ("LogMemory", "update_named"): ("LogMemory.update", ""),
    ("MemoryHandler", "new"): ("MemoryHandler.constructor", CTOR),
    ("MemoryKind", "class"): ("fn:memoryClass", TS_FREE),
    ("MemoryKind", "code"): ("-", "The byte only feeds the content id hash"),
    ("MemoryKind", "from_word"): ("-", "MemoryKind is a string literal union in TypeScript"),
    ("MessageId", "new"): ("MessageId.offset", "TypeScript writes the MessageId object literal"),
    ("PolicyEvidence", "decode"): ("fn:decodePolicyEvidence", TS_FREE),
    ("PolicyEvidence", "encode"): ("fn:encodePolicyEvidence", TS_FREE),
    ("PreparedMutation", "ambiguous_recovery"): ("PreparedMutation.ambiguousRecovery", ""),
    ("PreparedMutation", "operation_id"): ("PreparedMutation.operationId", ""),
    ("ProducerMessage", "header"): ("ProducerMessage.headers", "TypeScript writes the ProducerMessage object literal"),
    ("ProducerMessage", "new"): ("ProducerMessage.payload", "TypeScript writes the ProducerMessage object literal"),
    ("ProducerMessage", "with_headers"): ("ProducerMessage.headers", "TypeScript writes the ProducerMessage object literal"),
    ("ProducerObservation", "finish"): ("-", INTERNAL),
    ("ProducerRecorder", "begin"): ("-", INTERNAL),
    ("ProducerRecorder", "new"): ("-", INTERNAL),
    ("ProducerRecorder", "snapshot"): ("-", INTERNAL),
    ("Provenance", "partition_key"): ("fn:provenancePartitionKey", TS_FREE),
    ("RegisteredCard", "available_for"): ("fn:cardAvailableFor", TS_FREE),
    ("RegisteredCard", "is_fresh"): ("fn:cardIsFresh", TS_FREE),
    ("RegisteredCard", "serves"): ("fn:cardServes", TS_FREE),
    ("RerankedMemory", "new"): ("MemoryHandle.reranker", ""),
    ("RetryPolicy", "backoff"): ("fn:retryBackoff", TS_FREE),
    ("Router", "all_capable"): ("fn:routeAllCapable", TS_FREE),
    ("Router", "apply"): ("fn:applyRoute", TS_FREE),
    ("Router", "broadcast"): ("fn:routeBroadcast", TS_FREE),
    ("Router", "resolve_targets"): ("fn:resolveTargets", TS_FREE),
    ("Router", "to"): ("fn:routeTo", TS_FREE),
    ("Router", "to_capable"): ("fn:routeToCapable", TS_FREE),
    ("Router", "to_principal"): ("fn:routeToPrincipal", TS_FREE),
    ("Routing", "key"): ("-", "Routing is a tagged union object in TypeScript"),
    ("SessionPolicy", "conversation_for"): ("fn:conversationFor", TS_FREE),
    ("SessionTurnKind", "for_topic"): ("fn:sessionTurnKind", TS_FREE),
    ("SessionTurnKind", "topic"): ("fn:sessionTurnTopic", TS_FREE),
    ("SharedConsolidator", "new"): ("-", "Rust wrapper for shared ownership. TypeScript shares the object"),
    ("SlidingWindow", "new"): ("SlidingWindow.constructor", CTOR),
    ("TestClock", "new"): ("TestClock.constructor", CTOR),
    ("TopicSnapshotStore", "new"): ("TopicSnapshotStore.constructor", CTOR),
    ("TopicSnapshotStore", "on_topic"): ("TopicSnapshotStore.constructor", CTOR),
    ("TypedQueryRows", "next"): ("QueryRequest.rowsTyped", "TypeScript iterates the rows"),
    ("TypedRecords", "next"): ("TypedRecords.stream", "TypeScript iterates with stream"),
    ("VectorMemory", "new"): ("VectorMemory.constructor", CTOR),
    ("Verdict", "as_str"): ("-", "Verdict is a tagged union in TypeScript and its kind holds the word"),
}

# TypeScript classes that carry a different name than their Rust type.
TS_RENAMED = {"AgentCtx": "AgentContext"}


# Keep rows owned by their Rust type. Shared method names must not collapse.
OWNER_OVERRIDES = {
    ("RecallBuilder", "agent"): ("Memory.recall(agent=)", "RecallBuilder.agent", KW),
    ("RecallBuilder", "user"): ("Memory.recall(user=)", "RecallBuilder.user", KW),
    ("RecallBuilder", "application"): ("Memory.recall(application=)", "RecallBuilder.application", KW),
    ("RecallBuilder", "fetch"): ("Memory.recall", "RecallBuilder.fetch", "Python recalls in one call"),
    ("ForkCreateRequest", "send"): ("ForkHandle.create", "ForkCreateRequest.send", "Python creates the fork in one call"),
    ("ForkPutRequest", "send"): ("ForkPutRequest.send", "ForkPutRequest.send", ""),
    ("Schemas", "register"): ("Laser.register_schema", "Schemas.register", "Python flattens schema operations onto Laser"),
    ("Schemas", "get"): ("Laser.get_schema", "Schemas.get", "Python flattens schema operations onto Laser"),
    ("Schemas", "list"): ("Laser.list_schemas", "Schemas.list", "Python flattens schema operations onto Laser"),
    ("Schemas", "drop"): ("Laser.drop_schema", "Schemas.drop", "Python flattens schema operations onto Laser"),
    ("AgdxStream", "fail"): ("AgdxStream.fail", "AgdxStream.fail", ""),
    ("SwappableGovernor", "new"): ("SwappableGovernor.__new__", "SwappableGovernor.constructor", CTOR),
    ("Intent", "new"): ("Intent.__new__", "Intent.constructor", CTOR),
    ("SwarmActivity", "new"): ("SwarmActivity.__new__", "new:SwarmActivity", CTOR),
    ("SwarmActivity", "observe"): ("SwarmActivity.observe", "SwarmActivity.observe", ""),
    ("ActionDecision", "observe"): ("ActionDecision.observe", "ActionDecision.observe", ""),
}
for _method in ("budget", "registered", "run", "run_id", "step"):
    OWNER_OVERRIDES[("StepHandle", _method)] = (
        f"Workflow.{_method}", f"Workflow.{camel(_method)}",
        "Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle",
    )
OWNER_OVERRIDES[("StepHandle", "inbox_route")] = (
    "Laser.workflow(fixed_inbox=)", "Workflow.inboxRoute",
    "Rust forwards from the step handle to its workflow",
)


# Generated bon builders are public API even when their source fields are private.
GENERATED_BUILDER_PEERS = {
    ("AgentBuilder", "id"): ("Laser.spawn_agent(agent_id=)", "AgentBuilder.id", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "consumer_group"): ("Laser.spawn_agent(consumer_group=)", "AgentBuilder.consumerGroup", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "listen_on"): ("Laser.spawn_agent(listen_on=)", "AgentBuilder.listenOn", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "handler"): ("Laser.spawn_agent(handler=)", "AgentBuilder.handler", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "respond_on"): ("Laser.spawn_agent(respond_on=)", "AgentBuilder.respondOn", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "inbox_route"): ("Laser.spawn_agent(fixed_inbox=)", "AgentBuilder.inboxRoute", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "poll_interval"): ("Laser.spawn_agent(poll_interval_ms=)", "AgentBuilder.pollInterval", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "shutdown_grace"): ("Laser.spawn_agent(shutdown_grace_ms=)", "AgentBuilder.shutdownGrace", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "concurrency"): ("Laser.spawn_agent(max_partitions=)", "AgentBuilder.concurrency", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "max_queued_records"): ("Laser.spawn_agent(max_queued_records=)", "AgentBuilder.maxQueuedRecords", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "max_queued_bytes"): ("Laser.spawn_agent(max_queued_bytes=)", "AgentBuilder.maxQueuedBytes", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "warm_dedup"): ("Laser.spawn_agent(warm_dedup=)", "AgentBuilder.warmDedup", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "middleware"): ("Laser.spawn_agent(middleware=)", "AgentBuilder.middleware", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "on_dead_letter"): ("Laser.spawn_agent(dead_letter=)", "AgentBuilder.deadLetterSink", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "dedup_window"): ("Laser.spawn_agent(dedup_window=)", "AgentBuilder.dedupWindow", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "retry"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "AgentBuilder.retry", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "understood_features"): ("Laser.spawn_agent(understood_features=)", "AgentBuilder.understoodFeatures", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "deduplicator"): ("Laser.spawn_agent(dedup=)", "AgentBuilder.deduplicator", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "verifier"): ("Laser.spawn_agent(verifier=)", "AgentBuilder.verifier", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "signing_key"): ("Laser.spawn_agent(signing_key=)", "AgentBuilder.signingKey", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "capabilities"): ("Laser.spawn_agent(capabilities=)", "AgentBuilder.capabilities", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "ack_on_pickup"): ("Laser.spawn_agent(ack_on_pickup=)", "AgentBuilder.ackOnPickup", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "consolidate_every"): ("Laser.spawn_agent(consolidate_every_ms=)", "AgentBuilder.consolidateEvery", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "consolidator"): ("Laser.spawn_agent(consolidator=)", "AgentBuilder.consolidator", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "governor"): ("Laser.spawn_agent(governor=)", "AgentBuilder.governor", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "governor_retention"): ("Laser.spawn_agent(governor_retention=)", "AgentBuilder.governor", "Python uses keywords. TypeScript uses a builder or object field"),
    ("AgentBuilder", "build"): ("Laser.spawn_agent", "AgentBuilder.spawn", "Python and TypeScript build and spawn in one call"),
    ("Agent", "builder"): ("Laser.spawn_agent", "Agent.builder", "Python configures the runtime through spawn_agent"),
    ("Agent", "spawn"): ("Laser.spawn_agent", "AgentBuilder.spawn", "Python and TypeScript build and spawn in one call"),
    ("ReliableConsumerBuilder", "handler"): ("Laser.spawn_agent(handler=)", "ReliableConsumerOptions.handler", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "respond_on"): ("Laser.spawn_agent(respond_on=)", "ReliableConsumerOptions.respondOn", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "inbox_route"): ("Laser.spawn_agent(fixed_inbox=)", "ReliableConsumerOptions.inboxRoute", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "poll_interval"): ("Laser.spawn_agent(poll_interval_ms=)", "ReliableConsumerOptions.pollIntervalMs", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "shutdown_grace"): ("Laser.spawn_agent(shutdown_grace_ms=)", "ReliableConsumerOptions.shutdownGraceMs", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "concurrency"): ("Laser.spawn_agent(max_partitions=)", "ReliableConsumerOptions.concurrency", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "max_queued_records"): ("Laser.spawn_agent(max_queued_records=)", "ReliableConsumerOptions.maxQueuedRecords", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "max_queued_bytes"): ("Laser.spawn_agent(max_queued_bytes=)", "ReliableConsumerOptions.maxQueuedBytes", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "warm_dedup"): ("Laser.spawn_agent(warm_dedup=)", "ReliableConsumerOptions.warmDedup", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "middleware"): ("Laser.spawn_agent(middleware=)", "ReliableConsumerOptions.middleware", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "on_dead_letter"): ("Laser.spawn_agent(dead_letter=)", "ReliableConsumerOptions.deadLetterSink", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "dedup_window"): ("Laser.spawn_agent(dedup_window=)", "ReliableConsumerOptions.dedupWindow", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "retry"): ("Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)", "ReliableConsumerOptions.retry", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "understood_features"): ("Laser.spawn_agent(understood_features=)", "ReliableConsumerOptions.understoodFeatures", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "deduplicator"): ("Laser.spawn_agent(dedup=)", "ReliableConsumerOptions.deduplicator", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "verifier"): ("Laser.spawn_agent(verifier=)", "ReliableConsumerOptions.verifier", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "signing_key"): ("Laser.spawn_agent(signing_key=)", "ReliableConsumerOptions.signingKey", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "ack_on_pickup"): ("Laser.spawn_agent(ack_on_pickup=)", "ReliableConsumerOptions.ackOnPickup", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "agent"): ("Laser.spawn_agent(agent_id=)", "ReliableConsumerOptions.agent", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "group"): ("Laser.spawn_agent(consumer_group=)", "ReliableConsumerOptions.group", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "topic"): ("Laser.spawn_agent(listen_on=)", "ReliableConsumerOptions.topic", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ReliableConsumerBuilder", "build"): ("Laser.spawn_agent", "ReliableConsumer.constructor", "Python starts the configured consumer. TypeScript constructs the consumer"),
    ("ReliableConsumer", "builder"): ("Laser.spawn_agent", "ReliableConsumer.constructor", "Python starts the configured consumer. TypeScript passes an options object"),
    ("ContextAssemblerBuilder", "conversation_id"): ("Laser.assemble_context(conversation_id=)", "ContextAssemblerBuilder.constructor", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "across_subconversations"): ("Laser.assemble_context(across_subconversations=)", "ContextAssemblerBuilder.acrossSubconversations", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "topics"): ("Laser.assemble_context(topics=)", "ContextAssemblerBuilder.topics", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "policy"): ("Laser.assemble_context(policy=)", "ContextAssemblerBuilder.policy", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "from_offsets"): ("Laser.assemble_context(from_offsets=)", "ContextAssemblerBuilder.fromOffsets", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "from_checkpoint"): ("Laser.assemble_context(from_checkpoint=)", "ContextAssemblerBuilder.fromCheckpoint", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "to_checkpoint"): ("Laser.assemble_context(to_checkpoint=)", "ContextAssemblerBuilder.toCheckpoint", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ContextAssemblerBuilder", "build"): ("Laser.assemble_context", "ContextAssemblerBuilder.build", "Python assembles the configured context in one call"),
    ("ContextAssembler", "builder"): ("Laser.assemble_context", "ContextAssembler.builder", "Python assembles the configured context in one call"),
    ("LlmUsageBuilder", "input_tokens"): ("Provenance.__new__(input_tokens=)", "LlmUsage.inputTokens", "Python uses keywords. TypeScript uses a builder or object field"),
    ("LlmUsageBuilder", "output_tokens"): ("Provenance.__new__(output_tokens=)", "LlmUsage.outputTokens", "Python uses keywords. TypeScript uses a builder or object field"),
    ("LlmUsageBuilder", "cost_usd"): ("Provenance.__new__(cost_usd=)", "LlmUsage.costUsd", "Python uses keywords. TypeScript uses a builder or object field"),
    ("LlmUsage", "builder"): ("new:Provenance", "Provenance.usage", "Usage is part of provenance. TypeScript uses the LlmUsage object literal"),
    ("LlmUsageBuilder", "build"): ("new:Provenance", "Provenance.usage", "Usage is part of provenance. TypeScript uses the LlmUsage object literal"),
    ("MemoryQueryBuilder", "limit"): ("Memory.recall(limit=)", "MemoryQuery.limit", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryQueryBuilder", "token_budget"): ("Memory.recall(token_budget=)", "MemoryQuery.tokenBudget", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryQueryBuilder", "agent"): ("Memory.recall(agent=)", "MemoryQuery.agent", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryQueryBuilder", "semantic"): ("Memory.recall(semantic=)", "MemoryQuery.semantic", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryQueryBuilder", "strategy"): ("Memory.recall(strategy=)", "MemoryQuery.strategy", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryQuery", "builder"): ("Memory.recall", "MemoryQuery.limit", "Python passes query keywords. TypeScript uses the MemoryQuery object literal"),
    ("MemoryQueryBuilder", "build"): ("Memory.recall", "MemoryQuery.limit", "Python passes query keywords. TypeScript uses the MemoryQuery object literal"),
    ("MemoryScopeBuilder", "stream"): ("Memory.remember(stream=)", "MemoryScope.stream", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScopeBuilder", "user"): ("Memory.remember(user=)", "MemoryScope.user", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScopeBuilder", "agent"): ("Memory.remember(agent=)", "MemoryScope.agent", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScopeBuilder", "conversation"): ("Memory.remember(conversation=)", "MemoryScope.conversation", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScopeBuilder", "app"): ("Memory.remember(application=)", "MemoryScope.application", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScopeBuilder", "lifetime"): ("Memory.remember(durable=)", "MemoryScope.lifetime", "Python uses keywords. TypeScript uses a builder or object field"),
    ("MemoryScope", "builder"): ("Memory.remember", "MemoryScope.conversation", "Python passes scope keywords or a dictionary. TypeScript uses the MemoryScope object literal"),
    ("MemoryScopeBuilder", "build"): ("Memory.remember", "MemoryScope.conversation", "Python passes scope keywords or a dictionary. TypeScript uses the MemoryScope object literal"),
    ("ProducerMessageBuilder", "payload"): ("Producer.send(payload=)", "ProducerMessage.payload", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProducerMessageBuilder", "headers"): ("Producer.send(headers=)", "ProducerMessage.headers", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProducerMessage", "builder"): ("Producer.send", "ProducerMessage.payload", "Python passes payload and headers to send. TypeScript uses the ProducerMessage object literal"),
    ("ProducerMessageBuilder", "build"): ("Producer.send", "ProducerMessage.payload", "Python passes payload and headers to send. TypeScript uses the ProducerMessage object literal"),
    ("ProvenanceBuilder", "conversation_id"): ("Provenance.__new__(conversation_id=)", "Provenance.conversationId", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "causal_parent"): ("Provenance.__new__(causal_parent=)", "Provenance.causalParent", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "parent_conversation_id"): ("Provenance.__new__(parent_conversation_id=)", "Provenance.parentConversationId", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "root_conversation_id"): ("Provenance.__new__(root_conversation_id=)", "Provenance.rootConversationId", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "agent"): ("Provenance.__new__(agent=)", "Provenance.agent", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "target_agent_id"): ("Provenance.__new__(target_agent_id=)", "Provenance.targetAgentId", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "deadline"): ("Provenance.__new__(deadline_micros=)", "Provenance.deadlineMicros", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "idempotency_key"): ("Provenance.__new__(idempotency_key=)", "Provenance.idempotencyKey", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "correlation_id"): ("Provenance.__new__(correlation_id=)", "Provenance.correlationId", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "fence_token"): ("Provenance.__new__(fence_token=)", "Provenance.fenceToken", "Python uses keywords. TypeScript uses a builder or object field"),
    ("ProvenanceBuilder", "usage"): ("Provenance.__new__(input_tokens=, output_tokens=, cost_usd=)", "Provenance.usage", "Python uses keywords. TypeScript uses a builder or object field"),
    ("Provenance", "builder"): ("new:Provenance", "Provenance.conversationId", "Python constructs provenance. TypeScript uses the Provenance object literal"),
    ("ProvenanceBuilder", "build"): ("new:Provenance", "Provenance.conversationId", "Python constructs provenance. TypeScript uses the Provenance object literal"),
    ("RecordBuilder", "content_type"): ("BatchPublishRequest.add_record(content_type=)", "Record.contentType", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "projection_ref"): ("BatchPublishRequest.add_record(projection_ref=)", "Record.projectionRef", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "schema_id"): ("BatchPublishRequest.add_record(schema_id=)", "Record.schemaId", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "logical_schema_fingerprint"): ("BatchPublishRequest.add_record(logical_schema_fingerprint=)", "Record.logicalSchemaFingerprint", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "index"): ("BatchPublishRequest.add_record(index=)", "Record.index", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "metadata"): ("BatchPublishRequest.add_record(headers=)", "Record.header", "Python supplies per-record metadata. TypeScript uses Record"),
    ("RecordBuilder", "inline_payload"): ("BatchPublishRequest.add_record(inline_payload=)", "Record.inlinePayload", "Python supplies per-record metadata. TypeScript uses Record"),
    ("Record", "builder"): ("BatchPublishRequest.add_record", "new:Record", "Python supplies per-record metadata. TypeScript constructs Record"),
    ("RecordBuilder", "build"): ("BatchPublishRequest.add_record", "new:Record", "Python supplies per-record metadata. TypeScript constructs Record"),
}
OWNER_OVERRIDES.update(GENERATED_BUILDER_PEERS)

# Public traits include required methods, default hooks, and generated variants.
PUBLIC_TRAIT_PEERS = {
    ("Memory", "append"): ("Memory.append", "MemoryHandle.append", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("LocalMemory", "append"): ("Memory.append", "MemoryHandle.append", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("DynMemory", "append"): ("Memory.append", "MemoryHandle.append", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("Memory", "forget"): ("Memory.forget", "MemoryHandle.forget", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("LocalMemory", "forget"): ("Memory.forget", "MemoryHandle.forget", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("DynMemory", "forget"): ("Memory.forget", "MemoryHandle.forget", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("Memory", "improve"): ("Memory.improve", "MemoryHandle.improve", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("LocalMemory", "improve"): ("Memory.improve", "MemoryHandle.improve", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("DynMemory", "improve"): ("Memory.improve", "MemoryHandle.improve", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("Memory", "recall"): ("Memory.recall", "MemoryHandle.recall", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("LocalMemory", "recall"): ("Memory.recall", "MemoryHandle.recall", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("DynMemory", "recall"): ("Memory.recall", "MemoryHandle.recall", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("Memory", "remember"): ("Memory.remember", "MemoryHandle.remember", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("LocalMemory", "remember"): ("Memory.remember", "MemoryHandle.remember", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("DynMemory", "remember"): ("Memory.remember", "MemoryHandle.remember", "Python uses scope keywords. Local and boxed Rust traits share the same binding"),
    ("ManagedKvTransport", "ready"): ("DedicatedKvTransport.ready", "ManagedKvTransport.ready", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("LocalManagedKvTransport", "ready"): ("DedicatedKvTransport.ready", "ManagedKvTransport.ready", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("DynManagedKvTransport", "ready"): ("DedicatedKvTransport.ready", "ManagedKvTransport.ready", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("ManagedKvTransport", "send"): ("DedicatedKvTransport.send", "ManagedKvTransport.send", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("LocalManagedKvTransport", "send"): ("DedicatedKvTransport.send", "ManagedKvTransport.send", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("DynManagedKvTransport", "send"): ("DedicatedKvTransport.send", "ManagedKvTransport.send", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("ManagedKvTransport", "reset"): ("DedicatedKvTransport.reset", "ManagedKvTransport.reset", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("LocalManagedKvTransport", "reset"): ("DedicatedKvTransport.reset", "ManagedKvTransport.reset", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("DynManagedKvTransport", "reset"): ("DedicatedKvTransport.reset", "ManagedKvTransport.reset", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("ManagedKvTransport", "close"): ("DedicatedKvTransport.close", "ManagedKvTransport.close", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("LocalManagedKvTransport", "close"): ("DedicatedKvTransport.close", "ManagedKvTransport.close", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("DynManagedKvTransport", "close"): ("DedicatedKvTransport.close", "ManagedKvTransport.close", "Custom Python transports also supply these methods to FencedLeaseClient"),
    ("StateStore", "get"): ("InMemoryStore.get", "StateStore.get", "Python built-in stores share the same methods"),
    ("LocalStateStore", "get"): ("InMemoryStore.get", "StateStore.get", "Python built-in stores share the same methods"),
    ("StateStore", "set"): ("InMemoryStore.set", "StateStore.set", "Python built-in stores share the same methods"),
    ("LocalStateStore", "set"): ("InMemoryStore.set", "StateStore.set", "Python built-in stores share the same methods"),
    ("StateStore", "delete"): ("InMemoryStore.delete", "StateStore.delete", "Python built-in stores share the same methods"),
    ("LocalStateStore", "delete"): ("InMemoryStore.delete", "StateStore.delete", "Python built-in stores share the same methods"),
    ("SnapshotStore", "latest"): ("SnapshotStore.latest", "SnapshotStore.latest", "Python state_with also accepts a custom latest/save object"),
    ("LocalSnapshotStore", "latest"): ("SnapshotStore.latest", "SnapshotStore.latest", "Python state_with also accepts a custom latest/save object"),
    ("SnapshotStore", "save"): ("SnapshotStore.save", "SnapshotStore.save", "Python state_with also accepts a custom latest/save object"),
    ("LocalSnapshotStore", "save"): ("SnapshotStore.save", "SnapshotStore.save", "Python state_with also accepts a custom latest/save object"),
    ("Clock", "now_micros"): ("SystemClock.now_micros", "Clock.nowMicros", ""),
    ("ContextPolicy", "select"): ("Laser.assemble_context(policy=)", "ContextPolicy.select", "Python accepts a synchronous callable or select object"),
    ("ActionGovernor", "decide"): ("Laser.with_governor(governor=)", "ActionGovernor.decide", "Python accepts a governor object with decide"),
    ("BlobStore", "put"): ("PublishRequest.claim_check(store=)", "BlobStore.put", "Python accepts a store object with put/get"),
    ("BlobStore", "get"): ("AgentMessage.resolve_body(store=)", "BlobStore.get", "Python accepts a store object with put/get"),
    ("Embedder", "embed"): ("Memory.vector(embedder=)", "Embedder.embed", "Python accepts a callable"),
    ("LocalEmbedder", "embed"): ("Memory.vector(embedder=)", "Embedder.embed", "Python accepts a callable"),
    ("Reranker", "rerank"): ("Memory.reranker(reranker=)", "Reranker.rerank", "Python accepts a callable"),
    ("LocalReranker", "rerank"): ("Memory.reranker(reranker=)", "Reranker.rerank", "Python accepts a callable"),
    ("Summarizer", "summarize"): ("Memory.consolidate(summarizer=)", "Summarizer.summarize", "Python accepts a callable"),
    ("LocalSummarizer", "summarize"): ("Memory.consolidate(summarizer=)", "Summarizer.summarize", "Python accepts a callable"),
    ("Consolidator", "consolidate"): ("Laser.spawn_agent(consolidator=)", "Consolidator.consolidate", "Python accepts a callable or consolidate object"),
    ("LocalConsolidator", "consolidate"): ("Laser.spawn_agent(consolidator=)", "Consolidator.consolidate", "Python accepts a callable or consolidate object"),
    ("AgentHandler", "handle"): ("Laser.spawn_agent(handler=)", "AgentHandler.handle", "Python accepts a callable or handle object"),
    ("LocalAgentHandler", "handle"): ("Laser.spawn_agent(handler=)", "AgentHandler.handle", "Python accepts a callable or handle object"),
    ("AgentMiddleware", "before_handle"): ("Laser.spawn_agent(middleware=)", "AgentMiddleware.beforeHandle", "Python middleware objects receive full attempt results"),
    ("AgentMiddleware", "after_handle"): ("Laser.spawn_agent(middleware=)", "AgentMiddleware.afterHandle", "Python middleware objects receive full attempt results"),
    ("DeadLetterSink", "on_dead_letter"): ("Laser.spawn_agent(dead_letter=)", "DeadLetterSink.onDeadLetter", "Python receives the full capsule and typed publish error"),
    ("Deduplicator", "observe"): ("Laser.spawn_agent(dedup=)", "Deduplicator.observe", "Python accepts an observe object"),
    ("RouteScorer", "select"): ("Laser.contract(policy=)", "RouteScorer.select", "Python accepts a synchronous callable or select object"),
    ("StepFn", "build"): ("Workflow.step(build=)", "Workflow.step", "Python and TypeScript take synchronous or async callbacks"),
    ("Verifier", "verify"): ("Workflow.step(verify=)", "StepBuilder.verifyWith", "Python and TypeScript take synchronous or async callbacks"),
    ("MintUlid", "mint"): ("fn:new_conversation_id", "ConversationId.new", "Python mints plain ULID strings. TypeScript uses branded IDs"),
}
OWNER_OVERRIDES.update(PUBLIC_TRAIT_PEERS)

# These helpers remain callable without a live connection.
PUBLIC_FUNCTION_PEERS = {
    ("a2a", "command_from_message_send"): ("fn:command_from_message_send", "fn:commandFromMessageSend", ""),
    ("a2a", "task_from_envelope"): ("fn:task_from_envelope", "fn:taskFromEnvelope", ""),
    ("agent::state", "resume_offsets"): ("fn:resume_offsets", "fn:resumeOffsets", ""),
    ("blob", "check_in"): ("fn:check_in", "fn:checkIn", ""),
    ("blob", "resolve_body"): ("fn:resolve_body", "fn:resolveBody", ""),
    ("context", "checkpoint"): ("ContextScope.checkpoint", "ContextScope.checkpoint", "The scope retains the connection and conversation"),
    ("mcp", "tool_call_from_request"): ("fn:tool_call_from_request", "fn:toolCallFromRequest", ""),
    ("mcp", "tool_result_from_envelope"): ("fn:tool_result_from_envelope", "fn:toolResultFromEnvelope", ""),
    ("memory", "fuse_reciprocal_rank"): ("fn:fuse_reciprocal_rank", "fn:fuseReciprocalRank", ""),
    ("memory", "to_context_block"): ("Memory.to_context_block", "fn:toContextBlock", "Python exposes a static helper"),
    ("sign", "sign_card_value"): ("fn:sign_card_value", "fn:signCardValue", ""),
    ("sign", "verify_card"): ("fn:verify_card", "fn:verifyCard", ""),
    ("sign", "verify_delegation"): ("fn:verify_delegation", "fn:verifyDelegation", ""),
    ("snapshot", "decode"): ("fn:decode_snapshot", "fn:decodeSnapshot", ""),
    ("snapshot", "encode"): ("fn:encode_snapshot", "fn:encodeSnapshot", ""),
    ("testing", "agent_ctx"): ("fn:agent_ctx", "fn:agentContext", "Python and TypeScript take callback context options"),
    ("testing", "agent_message"): ("fn:agent_message", "fn:agentMessage", ""),
}
OWNER_OVERRIDES.update(PUBLIC_FUNCTION_PEERS)

def find(surface, classes, name):
    for cls in classes:
        if name in surface.get(cls, set()):
            return cls if len(classes) > 1 else None
    return False


def resolve(surface, classes, spelling):
    if spelling.startswith("-"):
        return True
    if spelling.startswith("new:"):
        return spelling[4:] in surface
    call = re.match(r"^(?:(\w+)[.:])?(\w+)(?:\(([\w, =]*)\))?$", spelling)
    if not call:
        return False
    cls, member, args = call.groups()
    pool = [cls] if cls else classes
    owners = [c for c in pool if member in surface.get(c, set())]
    if not owners:
        return False
    wanted = [a.strip().rstrip("=") for a in (args or "").split(",") if a.strip()]
    if surface is not PYTHON or not wanted:
        return True
    return any(set(wanted) <= KWARGS.get((c, member), set()) for c in owners)


def render():
    global PYTHON
    rust, python, typescript = rust_surface(), python_surface(), typescript_surface()
    PYTHON = python
    problems = []
    lines = [
        "# Rust, Python, and TypeScript parity",
        "",
        "This matrix lists public inherent and trait methods, generated builders, and standalone SDK functions with their Python and TypeScript spellings. `scripts/check-parity.py --write` generates it from the Rust sources, the Python stub, and the TypeScript API report. `just parity-check` fails when the committed file is stale or when a row says MISSING. A new public Rust type gets its own section without a registration step, so nothing stays outside the check. Python uses snake_case and TypeScript uses camelCase for the same name. Each row retains its Rust owner so identical names on different types are checked separately. A note explains every deliberate difference. This source listing checks API spellings, not runtime behavior. Behavioral parity also requires the shared scenarios and client tests.",
        "",
    ]
    covered = {owner for _, rust_types, _, _ in SECTIONS for owner in rust_types}
    # Every other public Rust type with public methods gets its own section, so
    # a new type or method can never stay outside the check.
    extra = [
        (owner, [owner], ["fn" if owner[0].islower() else owner], ["fn" if owner[0].islower() else TS_RENAMED.get(owner, owner)])
        for owner in sorted(public_rust_types() - covered)
        if rust.get(owner)
    ]
    for section, rust_types, py_classes, ts_classes in SECTIONS + extra:
        for owner in rust_types:
            if not rust.get(owner):
                problems.append(f"{section}: no Rust methods found for {owner}")
        methods = [(owner, method) for owner in rust_types for method in sorted(rust.get(owner, set()))]
        if not methods:
            continue
        lines += [f"## {section}", "", "| Rust | Python | TypeScript | Notes |", "| --- | --- | --- | --- |"]
        for owner, method in methods:
            override = OWNER_OVERRIDES.get((owner, method), OVERRIDES.get((section, method)))
            py_owners = [owner] if owner in py_classes else py_classes
            ts_owners = [owner] if owner in ts_classes else ts_classes
            note = ""
            if override:
                py_cell, ts_cell, note = override
                for label, cell, surface, classes in (("Python", py_cell, python, py_classes), ("TypeScript", ts_cell, typescript, ts_classes)):
                    if cell != MISSING and not resolve(surface, classes, cell):
                        problems.append(f"{owner}::{method}: {label} override `{cell}` not found")
                    if cell.startswith("-") and not note:
                        problems.append(f"{owner}::{method}: omission without a note")
            else:
                notes = []
                py_peer = PY_PEERS.get((owner, method))
                if py_peer:
                    py_cell, py_note = py_peer
                    if not resolve(python, py_classes, py_cell):
                        problems.append(f"{owner}::{method}: Python peer `{py_cell}` not found")
                    if py_cell.startswith("-") and not py_note:
                        problems.append(f"{owner}::{method}: Python omission without a note")
                    notes.append(py_note)
                else:
                    found = find(python, py_owners, method)
                    py_cell = MISSING if found is False else (f"{found}.{method}" if found else method)
                ts_peer = TS_OVERRIDES.get((section, method)) or TS_PEERS.get((owner, method))
                if ts_peer:
                    ts_cell, ts_note = ts_peer
                    if not resolve(typescript, ts_classes, ts_cell):
                        problems.append(f"{owner}::{method}: TypeScript peer `{ts_cell}` not found")
                    if ts_cell.startswith("-") and not ts_note:
                        problems.append(f"{owner}::{method}: TypeScript omission without a note")
                    notes.append(ts_note)
                else:
                    found = find(typescript, ts_owners, camel(method))
                    ts_cell = MISSING if found is False else (f"{found}.{camel(method)}" if found else camel(method))
                note = ". ".join(dict.fromkeys(part.rstrip(".") for part in notes if part))
            def show(cell):
                if cell == MISSING:
                    return MISSING
                if cell.startswith("-"):
                    return "omitted"
                if cell.startswith("new:"):
                    return f"`new {cell[4:]}()`"
                return f"`{cell}`"
            if MISSING in (py_cell, ts_cell):
                problems.append(f"{owner}::{method}: Python {py_cell}, TypeScript {ts_cell}")
            lines.append(f"| `{owner}::{method}` | {show(py_cell)} | {show(ts_cell)} | {note} |".rstrip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n", problems


def main():
    text, problems = render()
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


if __name__ == "__main__":
    sys.exit(main())
