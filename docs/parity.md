# Rust, Python, and TypeScript parity

This matrix lists public inherent and trait methods, generated builders, and standalone SDK functions with their Python and TypeScript spellings. `scripts/check-parity.py --write` generates it from the Rust sources, the Python stub, and the TypeScript API report. `just parity-check` fails when the committed file is stale or when a row says MISSING. A new public Rust type gets its own section without a registration step, so nothing stays outside the check. Python uses snake_case and TypeScript uses camelCase for the same name. Each row retains its Rust owner so identical names on different types are checked separately. A note explains every deliberate difference. This source listing checks API spellings, not runtime behavior. Behavioral parity also requires the shared scenarios and client tests.

## Laser

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Laser::advertise_presence` | `advertise_presence` | `advertisePresence` |  |
| `Laser::agdx` | `agdx` | `agdx` |  |
| `Laser::agent` | `agent` | `agent` |  |
| `Laser::agent_registry` | `agent_registry` | `agentRegistry` |  |
| `Laser::agui_events` | `agui_events` | `aguiEvents` |  |
| `Laser::authz_history` | `Laser.authz_history_all` | `authzHistory` | Python splits it into authz_history_all, authz_history_role, and authz_history_binding |
| `Laser::bind_roles` | `bind_roles` | `bindRoles` |  |
| `Laser::bind_roles_expect_revision` | `Laser.bind_roles(expect_revision=)` | `Laser.bindRoles` | Python and TypeScript fold the revision into bind_roles |
| `Laser::bindings` | `Laser.apply_binding` | `bindings` | Python flattens the handle into apply_binding and remove_binding on Laser |
| `Laser::bootstrap` | `bootstrap` | `bootstrap` |  |
| `Laser::builder` | `Laser.connect` | `builder` | Python configures the connection with connect keyword arguments |
| `Laser::cancel_query` | `cancel_query` | `cancelQuery` |  |
| `Laser::capabilities` | `capabilities` | `capabilities` |  |
| `Laser::changes_topic` | `changes_topic` | `changesTopic` |  |
| `Laser::clear_presence` | `clear_presence` | `clearPresence` |  |
| `Laser::client` | omitted | `Laser.iggyClient` | Escape hatch to the Apache Iggy client. Python has no Iggy client object |
| `Laser::client_metadata` | `client_metadata` | `clientMetadata` |  |
| `Laser::close` | `close` | `close` |  |
| `Laser::connect` | `connect` | `connect` |  |
| `Laser::connect_env` | `connect_env` | `connectEnv` |  |
| `Laser::connect_with_stream` | `connect_with_stream` | `connectWithStream` |  |
| `Laser::consumed` | `consumed` | `consumed` |  |
| `Laser::context` | `context` | `context` |  |
| `Laser::contract` | `contract` | `contract` |  |
| `Laser::control_topic` | `control_topic` | `controlTopic` |  |
| `Laser::default_stream` | `default_stream` | `defaultStream` |  |
| `Laser::define_role` | `define_role` | `defineRole` |  |
| `Laser::delete_role` | `delete_role` | `deleteRole` |  |
| `Laser::destinations` | `destinations` | `destinations` |  |
| `Laser::dlq_topic` | `dlq_topic` | `Laser.deadLetterTopic` | TypeScript name |
| `Laser::execute_batch` | `execute_batch` | `executeBatch` |  |
| `Laser::execute_checkpoint` | `execute_checkpoint` | `executeCheckpoint` |  |
| `Laser::execute_query` | `execute_query` | `executeQuery` |  |
| `Laser::fork` | `fork` | `fork` |  |
| `Laser::forks` | `forks` | `forks` |  |
| `Laser::from_client` | omitted | `Laser.fromIggyClient` | Escape hatch to the Apache Iggy client. Python has no Iggy client object |
| `Laser::get_bindings` | `get_bindings` | `getBindings` |  |
| `Laser::get_role` | `get_role` | `getRole` |  |
| `Laser::graph` | `graph` | `graph` |  |
| `Laser::kv` | `kv` | `kv` |  |
| `Laser::kv_namespaces` | `kv_namespaces` | `kvNamespaces` |  |
| `Laser::list_roles` | `list_roles` | `listRoles` |  |
| `Laser::local` | `local` | `local` |  |
| `Laser::memory` | `memory` | `memory` |  |
| `Laser::memory_custom` | `memory_custom` | `memoryCustom` |  |
| `Laser::memory_on_topic` | `memory_on_topic` | `memoryOnTopic` |  |
| `Laser::memory_topic` | `memory_topic` | `memoryTopic` |  |
| `Laser::memory_with` | `memory_with` | `memoryWith` |  |
| `Laser::ops_stream` | `ops_stream` | `opsStream` |  |
| `Laser::projections` | `Laser.register_projection` | `projections` | Python flattens the handle into register_, drop_, get_, and list_projection(s) on Laser |
| `Laser::publish_card` | `publish_card` | `publishCard` |  |
| `Laser::publish_state_delta` | `publish_state_delta` | `publishStateDelta` |  |
| `Laser::publish_state_snapshot` | `publish_state_snapshot` | `publishStateSnapshot` |  |
| `Laser::quarantine` | `quarantine` | `quarantine` |  |
| `Laser::quarantine_signed` | `quarantine_signed` | `quarantineSigned` |  |
| `Laser::query` | `query` | `query` |  |
| `Laser::query_lakehouse` | `query_lakehouse` | `queryLakehouse` |  |
| `Laser::query_page` | `query_page` | `queryPage` |  |
| `Laser::query_status` | `query_status` | `queryStatus` |  |
| `Laser::query_target` | `query_target` | `queryTarget` |  |
| `Laser::reassemble_channel` | `reassemble_channel` | `reassembleChannel` |  |
| `Laser::reconstruct_state` | `reconstruct_state` | `reconstructState` |  |
| `Laser::redrive_dead_letter` | `redrive_dead_letter` | `redriveDeadLetter` |  |
| `Laser::refresh_capabilities` | `refresh_capabilities` | `refreshCapabilities` |  |
| `Laser::request` | `request` | `request` |  |
| `Laser::runs` | `runs` | `runs` |  |
| `Laser::scatter` | `scatter` | `scatter` |  |
| `Laser::scatter_report` | `scatter_report` | `scatterReport` |  |
| `Laser::schemas` | `Laser.register_schema` | `schemas` | Python flattens the handle into register_, drop_, get_, and list_schema(s) on Laser |
| `Laser::send_agent` | `send_agent` | `sendAgent` |  |
| `Laser::sessions` | `sessions` | `sessions` |  |
| `Laser::sessions_with` | `Laser.sessions(stream=)` | `Laser.sessions` | Python and TypeScript pass the layout to sessions |
| `Laser::spawn_subconversation` | `spawn_subconversation` | `spawnSubconversation` |  |
| `Laser::stream` | `stream` | `stream` |  |
| `Laser::topic` | `topic` | `topic` |  |
| `Laser::unquarantine` | `unquarantine` | `unquarantine` |  |
| `Laser::unquarantine_signed` | `unquarantine_signed` | `unquarantineSigned` |  |
| `Laser::wait_until_ready` | `wait_until_ready` | `waitUntilReady` |  |
| `Laser::watch` | `watch` | `watch` |  |
| `Laser::whoami` | `whoami` | `whoami` |  |
| `Laser::with_capabilities` | `with_capabilities` | `withCapabilities` |  |
| `Laser::with_changes_topic` | `with_changes_topic` | `withChangesTopic` |  |
| `Laser::with_control_topic` | `with_control_topic` | `withControlTopic` |  |
| `Laser::with_default_stream` | `Laser.with_stream` | `withDefaultStream` | Python name |
| `Laser::with_dlq_topic` | `with_dlq_topic` | `Laser.withDeadLetterTopic` | TypeScript name |
| `Laser::with_governor` | `with_governor` | `withGovernor` |  |
| `Laser::with_governor_retention` | `with_governor_retention` | `Laser.withGovernor` | TypeScript passes retention as the third withGovernor argument |
| `Laser::with_ops_stream` | `with_ops_stream` | `withOpsStream` |  |
| `Laser::workflow` | `workflow` | `workflow` |  |

## LaserBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LaserBuilder::address` | `Laser.connect` | `address` | Python takes the address in the connection string |
| `LaserBuilder::build` | `Laser.connect` | `LaserBuilder.connect` | Python and TypeScript connect in one call |
| `LaserBuilder::capabilities` | `Laser.with_capabilities` | `capabilities` | Python injects capabilities on the connected Laser |
| `LaserBuilder::changes_topic` | `Laser.connect(changes_topic=)` | `changesTopic` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::client` | omitted | `LaserBuilder.iggyClient` | Escape hatch to the Apache Iggy client. Python has no Iggy client object |
| `LaserBuilder::connect_timeout` | `Laser.connect(connect_timeout_ms=)` | `connectTimeout` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::connection_string` | `Laser.connect` | `connectionString` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::control_topic` | `Laser.connect(control_topic=)` | `controlTopic` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::credentials` | `Laser.connect` | `credentials` | Python takes the credentials in the connection string |
| `LaserBuilder::dlq_topic` | `Laser.connect(dlq_topic=)` | `LaserBuilder.deadLetterTopic` | TypeScript name |
| `LaserBuilder::governor` | `Laser.with_governor` | `governor` | Python sets the governor on the connected Laser |
| `LaserBuilder::governor_with_retention` | `Laser.with_governor_retention` | `LaserBuilder.governor` | TypeScript passes retention as the third governor argument |
| `LaserBuilder::ops_stream` | `Laser.connect(ops_stream=)` | `opsStream` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::publish_max_retries` | `Laser.connect(publish_max_retries=)` | `publishMaxRetries` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::publish_retry_backoff` | `Laser.connect(publish_retry_backoff_ms=)` | `publishRetryBackoff` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::publish_timeout` | `Laser.connect(publish_timeout_ms=)` | `publishTimeout` | Python takes keyword arguments where Rust chains a builder |
| `LaserBuilder::stream` | `Laser.connect(stream=)` | `LaserBuilder.defaultStream` | TypeScript name |
| `LaserBuilder::verifier` | `Laser.connect(verifier=)` | `verifier` | Python takes keyword arguments where Rust chains a builder |

## Stream

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Stream::delete` | `delete` | `delete` |  |
| `Stream::ensure` | `ensure` | `ensure` |  |
| `Stream::name` | `name` | `name` |  |
| `Stream::topic` | `topic` | `topic` |  |

## Topic

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Topic::batch` | `batch` | `batch` |  |
| `Topic::batching` | `batching` | `batching` |  |
| `Topic::cbor` | `cbor` | `cbor` |  |
| `Topic::consumer` | `consumer` | `consumer` |  |
| `Topic::consumer_group` | `consumer_group` | `consumerGroup` |  |
| `Topic::consumer_group_id` | `consumer_group_id` | `consumerGroupId` |  |
| `Topic::ensure` | `ensure` | `ensure` |  |
| `Topic::ensure_consumer_group` | `ensure_consumer_group` | `ensureConsumerGroup` |  |
| `Topic::iggy_consumer` | omitted | omitted | Rust-only escape hatch to the Apache Iggy SDK builders |
| `Topic::iggy_consumer_group` | omitted | omitted | Rust-only escape hatch to the Apache Iggy SDK builders |
| `Topic::iggy_producer` | omitted | omitted | Rust-only escape hatch to the Apache Iggy SDK builders |
| `Topic::json` | `Stream.topic(cls=)` | `json` | Python types a topic with the cls argument and decodes JSON |
| `Topic::name` | `name` | `name` |  |
| `Topic::producer` | `producer` | `producer` |  |
| `Topic::publish` | `publish` | `publish` |  |
| `Topic::publish_batch` | `publish_batch` | `publishBatch` |  |
| `Topic::replay` | `replay` | `replay` |  |
| `Topic::schema` | `schema` | `schema` |  |
| `Topic::send` | `send` | `send` |  |

## TypedTopic

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TypedTopic::publish` | `Topic.publish` | `publish` |  |
| `TypedTopic::records` | `Topic.records` | `records` |  |
| `TypedTopic::topic` | omitted | omitted | Accessor to the untyped topic. Python and TypeScript keep the Topic that created the typed view |

## PublishRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `PublishRequest::arrow_ipc` | `arrow_ipc` | `arrowIpc` |  |
| `PublishRequest::avro` | `avro` | `avro` |  |
| `PublishRequest::claim_check` | `claim_check` | `claimCheck` |  |
| `PublishRequest::content_type` | `PublishRequest.raw_bytes(content_type=)` | `contentType` | Python sets the content type with raw_bytes |
| `PublishRequest::encode_with` | omitted | `encodeWith` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `PublishRequest::header` | `header` | `header` |  |
| `PublishRequest::index` | `index` | `index` |  |
| `PublishRequest::inline_payload` | `inline_payload` | `inlinePayload` |  |
| `PublishRequest::json` | `json` | `json` |  |
| `PublishRequest::msgpack` | `msgpack` | `msgpack` |  |
| `PublishRequest::partition_key` | `partition_key` | `partitionKey` |  |
| `PublishRequest::payload` | `payload` | `payload` |  |
| `PublishRequest::projection_ref` | `projection_ref` | `projectionRef` |  |
| `PublishRequest::provenance` | `provenance` | `provenance` |  |
| `PublishRequest::raw_bytes` | `raw_bytes` | `rawBytes` |  |
| `PublishRequest::schema_id` | `schema_id` | `schemaId` |  |
| `PublishRequest::send` | `send` | `send` |  |

## Producer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProducerBuilder::background` | `Topic.producer(background=, background_shards=)` | `ProducerOptions.background` | Python passes background mode as producer keywords. TypeScript passes it in ProducerOptions |
| `ProducerBuilder::batch_length` | `Topic.producer(batch_length=)` | `ProducerOptions.batchLength` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::build` | `Topic.producer` | `Topic.producer` | Python and TypeScript build the producer in one call |
| `ProducerBuilder::create_stream` | `Topic.producer(create_stream=)` | `ProducerOptions.createStream` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::create_topic` | `Topic.producer(create_topic=)` | `ProducerOptions.createTopic` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::expire_after` | `Topic.producer(message_expiry=)` | `ProducerOptions.messageExpiryMicros` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::linger` | `Topic.producer(linger_ms=)` | `ProducerOptions.lingerMs` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::max_topic_bytes` | `Topic.producer(max_topic_size=)` | `ProducerOptions.maxTopicBytes` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::never_expire` | `Topic.producer(message_expiry=)` | `ProducerOptions.messageExpiryMicros` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::partitions` | `Topic.producer(partitions=)` | `ProducerOptions.partitions` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::retries` | `Topic.producer(retries=)` | `ProducerOptions.retries` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::retry_backoff` | `Topic.producer(retry_interval_ms=)` | `ProducerOptions.retryIntervalMs` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::routing` | `Topic.producer(key=, partition=)` | `ProducerOptions.routing` | Python takes keyword arguments where Rust chains a builder |
| `ProducerBuilder::unlimited_topic_size` | `Topic.producer(max_topic_size=)` | `ProducerOptions.unlimitedTopicSize` | Python takes keyword arguments where Rust chains a builder |
| `Producer::send` | `send` | `send` |  |
| `Producer::send_batch` | `send_batch` | `sendBatch` |  |
| `Producer::send_batch_with_routing` | `Producer.send_batch(key=, partition=)` | `Producer.sendBatchWithRouting` | Python routes with key and partition arguments |
| `Producer::send_keyed` | `Producer.send(key=)` | `Producer.sendKeyed` | Python routes with key and partition arguments |
| `Producer::send_message` | `Producer.send(headers=)` | `Producer.sendMessage` | Python sends headers with send |
| `Producer::send_to_partition` | `Producer.send(partition=)` | `Producer.sendToPartition` | Python routes with key and partition arguments |
| `Producer::send_with_routing` | `Producer.send(key=, partition=)` | `Producer.sendWithRouting` | Python routes with key and partition arguments |
| `Producer::shutdown` | `shutdown` | `shutdown` |  |

## BatchingProducer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BatchingProducerBuilder::build` | `Topic.batching` | `BatchingProducerBuilder.build` | Python takes keyword arguments where Rust chains a builder |
| `BatchingProducerBuilder::linger` | `Topic.batching(linger_ms=)` | `BatchingProducerBuilder.linger` | Python takes keyword arguments where Rust chains a builder |
| `BatchingProducerBuilder::max_bytes` | `Topic.batching(max_bytes=)` | `BatchingProducerBuilder.maxBytes` | Python takes keyword arguments where Rust chains a builder |
| `BatchingProducerBuilder::max_records` | `Topic.batching(max_records=)` | `BatchingProducerBuilder.maxRecords` | Python takes keyword arguments where Rust chains a builder |
| `BatchingProducerBuilder::partition_key` | `Topic.batching(partition_key=)` | `BatchingProducerBuilder.partitionKey` | Python takes keyword arguments where Rust chains a builder |
| `BatchingProducer::close` | `close` | `close` |  |
| `BatchingProducer::flush` | `flush` | `flush` |  |
| `BatchingProducer::send` | `send` | `send` |  |

## Consumer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConsumerBuilder::allow_replay` | `Topic.consumer(allow_replay=)` | `ConsumerOptions.allowReplay` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::auto_join_group` | `ConsumerGroup.consumer(auto_join_group=)` | `ConsumerOptions.autoJoinGroup` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::batch_length` | `Topic.consumer(batch_length=)` | `ConsumerOptions.batchLength` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::build` | `Topic.consumer` | `Topic.consumer` | Python and TypeScript build the consumer in one call |
| `ConsumerBuilder::commit_policy` | `Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)` | `ConsumerOptions.commitPolicy` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::create_group` | `ConsumerGroup.consumer(create_group=)` | `ConsumerOptions.createGroup` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::init_retries` | `Topic.consumer(init_retries=, init_retry_interval_ms=)` | `ConsumerOptions.initRetries` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::poll_interval` | `Topic.consumer(poll_interval_ms=)` | `ConsumerOptions.pollIntervalMs` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::polling_retry_interval` | `Topic.consumer(polling_retry_interval_ms=)` | `ConsumerOptions.pollingRetryIntervalMs` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::start_at` | `Topic.consumer(polling=, offset=, timestamp_micros=)` | `ConsumerOptions.startFrom` | Python takes keyword arguments where Rust chains a builder |
| `ConsumerBuilder::without_poll_interval` | `Topic.consumer(poll_interval_ms=)` | `ConsumerOptions.pollIntervalMs` | Pass zero to poll without a pause |
| `Consumer::commit` | `commit` | `commit` |  |
| `Consumer::delete_offset` | `delete_offset` | `deleteOffset` |  |
| `Consumer::last_consumed_offset` | `last_consumed_offset` | `lastConsumedOffset` |  |
| `Consumer::last_stored_offset` | `last_stored_offset` | `lastStoredOffset` |  |
| `Consumer::next` | `Consumer.next` | `Consumer.stream` | TypeScript iterates with stream or for await |
| `Consumer::next_within` | `next_within` | `nextWithin` |  |
| `Consumer::shutdown` | `shutdown` | `shutdown` |  |
| `Consumer::store_offset` | `store_offset` | `storeOffset` |  |

## ConsumerGroup

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConsumerGroup::consumer` | `consumer` | `consumer` |  |
| `ConsumerGroup::create` | `create` | `create` |  |
| `ConsumerGroup::filter` | `filter` | `filter` |  |
| `ConsumerGroup::id` | `id` | `id` |  |
| `ConsumerGroup::info` | `info` | `info` |  |
| `ConsumerGroup::name` | `name` | `name` |  |
| `ConsumerGroup::reader` | `reader` | `reader` |  |
| `ConsumerGroup::topic` | omitted | omitted | Accessor to the owning topic. Python and TypeScript keep the Topic that created the group |

## Cursor

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Cursor::batch` | `Topic.replay(batch=)` | `batch` | Python takes keyword arguments where Rust chains a builder |
| `Cursor::from_offsets` | `Topic.replay(from_offsets=)` | `fromOffsets` | Python takes keyword arguments where Rust chains a builder |
| `Cursor::offsets` | `offsets` | `offsets` |  |
| `Cursor::poll` | `poll` | `poll` |  |
| `Cursor::stream` | `Cursor.__aiter__` | `stream` | Python iterates with async for |

## Kv

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Kv::cas_fenced` | `cas_fenced` | `casFenced` |  |
| `Kv::copy_to` | `copy_to` | `copyTo` |  |
| `Kv::delete` | `delete` | `delete` |  |
| `Kv::delete_many` | `delete_many` | `deleteMany` |  |
| `Kv::exists` | `exists` | `exists` |  |
| `Kv::expire` | `expire` | `expire` |  |
| `Kv::get` | `get` | `get` |  |
| `Kv::get_as` | omitted | `getAs` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `Kv::get_entry` | `get_entry` | `getEntry` |  |
| `Kv::get_entry_at_least` | `get_entry_at_least` | `getEntryAtLeast` |  |
| `Kv::get_many` | `get_many` | `getMany` |  |
| `Kv::get_typed` | `get_typed` | `getTyped` |  |
| `Kv::lease` | `lease` | `lease` |  |
| `Kv::move_to` | `move_to` | `moveTo` |  |
| `Kv::namespace` | `namespace` | `namespace` |  |
| `Kv::patch` | `patch` | `patch` |  |
| `Kv::release` | `release` | `release` |  |
| `Kv::renew_lease` | `renew_lease` | `renewLease` |  |
| `Kv::scan` | `scan` | `scan` |  |
| `Kv::set` | `set` | `set` |  |

## KvSetRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvSetRequest::bytes` | `bytes` | `bytes` |  |
| `KvSetRequest::commit` | `commit` | `commit` |  |
| `KvSetRequest::encode_with` | omitted | `encodeWith` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `KvSetRequest::expect_absent` | `expect_absent` | `expectAbsent` |  |
| `KvSetRequest::expect_version` | `expect_version` | `expectVersion` |  |
| `KvSetRequest::expires_at` | `expires_at` | `expiresAt` |  |
| `KvSetRequest::json` | `json` | `json` |  |
| `KvSetRequest::msgpack` | `msgpack` | `msgpack` |  |
| `KvSetRequest::send` | `send` | `send` |  |
| `KvSetRequest::ttl` | `ttl` | `ttl` |  |

## KvCasFencedRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvCasFencedRequest::bytes` | `bytes` | `bytes` |  |
| `KvCasFencedRequest::commit` | `commit` | `commit` |  |
| `KvCasFencedRequest::encode_with` | omitted | `encodeWith` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `KvCasFencedRequest::expect_absent` | `expect_absent` | `expectAbsent` |  |
| `KvCasFencedRequest::expect_version` | `expect_version` | `expectVersion` |  |
| `KvCasFencedRequest::expires_at` | `expires_at` | `expiresAt` |  |
| `KvCasFencedRequest::json` | `json` | `json` |  |
| `KvCasFencedRequest::msgpack` | `msgpack` | `msgpack` |  |
| `KvCasFencedRequest::ttl` | `ttl` | `ttl` |  |

## KvScanRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvScanRequest::conversation` | `conversation` | `conversation` |  |
| `KvScanRequest::cursor` | `cursor` | `cursor` |  |
| `KvScanRequest::entries` | `entries` | `entries` |  |
| `KvScanRequest::fetch` | `fetch` | `fetch` |  |
| `KvScanRequest::key_contains` | `key_contains` | `keyContains` |  |
| `KvScanRequest::limit` | `limit` | `limit` |  |
| `KvScanRequest::prefix` | `prefix` | `prefix` |  |
| `KvScanRequest::range` | `range` | `range` |  |

## KvDeleteManyRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvDeleteManyRequest::conversation` | `conversation` | `conversation` |  |
| `KvDeleteManyRequest::key_contains` | `key_contains` | `keyContains` |  |
| `KvDeleteManyRequest::prefix` | `prefix` | `prefix` |  |
| `KvDeleteManyRequest::range` | `range` | `range` |  |
| `KvDeleteManyRequest::send` | `send` | `send` |  |

## KvCopyRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvCopyRequest::into_namespace` | `into_namespace` | `intoNamespace` |  |
| `KvCopyRequest::send` | `send` | `send` |  |

## Fork

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ForkHandle::create` | `create` | `Fork.create` |  |
| `ForkHandle::id` | `ForkHandle.fork_id` | `Fork.forkId` | Python and TypeScript name |
| `ForkHandle::promote` | `promote` | `Fork.promote` |  |
| `ForkHandle::put_row` | `put_row` | `Fork.putRow` |  |
| `ForkHandle::squash` | `squash` | `Fork.squash` |  |
| `ForkCreateRequest::continuous` | `ForkHandle.create(continuous=)` | `ForkCreateRequest.continuous` | Python takes keyword arguments where Rust chains a builder |
| `ForkCreateRequest::parent` | `ForkHandle.create(parent=)` | `ForkCreateRequest.parent` | Python takes keyword arguments where Rust chains a builder |
| `ForkCreateRequest::send` | `ForkHandle.create` | `ForkCreateRequest.send` | Python creates the fork in one call |
| `ForkCreateRequest::severed` | `ForkHandle.create(severed=)` | `ForkCreateRequest.severed` | Python takes keyword arguments where Rust chains a builder |
| `ForkCreateRequest::tables` | `ForkHandle.create(tables=)` | `ForkCreateRequest.tables` | Python takes keyword arguments where Rust chains a builder |
| `ForkPutRequest::embedding` | `embedding` | `embedding` |  |
| `ForkPutRequest::field` | `field` | `field` |  |
| `ForkPutRequest::metadata` | `metadata` | `metadata` |  |
| `ForkPutRequest::payload` | `payload` | `payload` |  |
| `ForkPutRequest::projection` | `projection` | `projection` |  |
| `ForkPutRequest::send` | `ForkPutRequest.send` | `ForkPutRequest.send` |  |
| `ForkPutRequest::tombstone` | `tombstone` | `tombstone` |  |

## QueryRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `QueryRequest::agg_as` | `agg_as` | `QueryRequest.aggregateAs` | TypeScript name |
| `QueryRequest::at_snapshot` | `at_snapshot` | `atSnapshot` |  |
| `QueryRequest::at_timestamp_micros` | `at_timestamp_micros` | `atTimestampMicros` |  |
| `QueryRequest::avg` | `avg` | `avg` |  |
| `QueryRequest::cancel` | `cancel` | `cancel` |  |
| `QueryRequest::consistency` | `consistency` | `consistency` |  |
| `QueryRequest::conversation` | `conversation` | `conversation` |  |
| `QueryRequest::count` | `count` | `count` |  |
| `QueryRequest::count_distinct` | `count_distinct` | `countDistinct` |  |
| `QueryRequest::cursor` | `cursor` | `cursor` |  |
| `QueryRequest::deadline` | `deadline` | `deadline` |  |
| `QueryRequest::distinct` | `distinct` | `distinct` |  |
| `QueryRequest::execution_id` | `execution_id` | `executionId` |  |
| `QueryRequest::fetch` | `fetch` | `fetch` |  |
| `QueryRequest::fetch_all` | `fetch_all` | `fetchAll` |  |
| `QueryRequest::fetch_all_typed` | `QueryRequest.fetch_typed` | `fetchAllTyped` | Python fetch_typed returns every page |
| `QueryRequest::fetch_one` | `fetch_one` | `fetchOne` |  |
| `QueryRequest::fetch_one_with` | omitted | `QueryRequest.fetchOne` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `QueryRequest::fetch_typed` | `fetch_typed` | `fetchTyped` |  |
| `QueryRequest::fetch_typed_with` | omitted | `QueryRequest.fetchTyped` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `QueryRequest::filter` | `filter` | `filter` |  |
| `QueryRequest::filter_contains` | `filter_contains` | `filterContains` |  |
| `QueryRequest::filter_eq` | `filter_eq` | `filterEq` |  |
| `QueryRequest::filter_gt` | `filter_gt` | `filterGt` |  |
| `QueryRequest::filter_gte` | `filter_gte` | `filterGte` |  |
| `QueryRequest::filter_in` | `filter_in` | `filterIn` |  |
| `QueryRequest::filter_lt` | `filter_lt` | `filterLt` |  |
| `QueryRequest::filter_lte` | `filter_lte` | `filterLte` |  |
| `QueryRequest::filter_ne` | `filter_ne` | `filterNe` |  |
| `QueryRequest::filter_prefix` | `filter_prefix` | `filterPrefix` |  |
| `QueryRequest::fork` | `fork` | `fork` |  |
| `QueryRequest::group_by` | `group_by` | `groupBy` |  |
| `QueryRequest::having` | `having` | `having` |  |
| `QueryRequest::into_query` | `QueryRequest.to_dict` | `intoQuery` | Python returns the wire query as a dict |
| `QueryRequest::limit` | `limit` | `limit` |  |
| `QueryRequest::max` | `max` | `max` |  |
| `QueryRequest::max_rows` | `max_rows` | `maxRows` |  |
| `QueryRequest::message_type` | `message_type` | `messageType` |  |
| `QueryRequest::min` | `min` | `min` |  |
| `QueryRequest::nearest` | `nearest` | `nearest` |  |
| `QueryRequest::nearest_in` | `QueryRequest.nearest(field=)` | `nearestIn` | Python takes keyword arguments where Rust chains a builder |
| `QueryRequest::offset` | `offset` | `offset` |  |
| `QueryRequest::order_asc` | `order_asc` | `orderAsc` |  |
| `QueryRequest::order_desc` | `order_desc` | `orderDesc` |  |
| `QueryRequest::percentile` | `percentile` | `percentile` |  |
| `QueryRequest::raw_sql` | `raw_sql` | `rawSql` |  |
| `QueryRequest::raw_sql_with` | `QueryRequest.raw_sql(dialect=)` | `QueryRequest.rawSql` | Python and TypeScript pass the dialect to raw_sql |
| `QueryRequest::read_your_writes` | `read_your_writes` | `readYourWrites` |  |
| `QueryRequest::rows` | `rows` | `rows` |  |
| `QueryRequest::rows_typed` | `rows_typed` | `rowsTyped` |  |
| `QueryRequest::select_fields` | `select_fields` | `selectFields` |  |
| `QueryRequest::status` | `status` | `status` |  |
| `QueryRequest::stddev` | `stddev` | `QueryRequest.stdDev` | TypeScript name |
| `QueryRequest::sum` | `sum` | `sum` |  |
| `QueryRequest::text` | `text` | `text` |  |
| `QueryRequest::text_in` | `text_in` | `textIn` |  |
| `QueryRequest::time_range` | `time_range` | `timeRange` |  |
| `QueryRequest::where_eq` | `where_eq` | `whereEq` |  |
| `QueryRequest::window` | `window` | `window` |  |
| `QueryRequest::with_payload` | `with_payload` | `withPayload` |  |
| `QueryRequest::with_total` | `with_total` | `withTotal` |  |
| `QueryRows::next` | `QueryRequest.rows` | `QueryRequest.rows` | QueryRows::next. Python and TypeScript iterate the rows |

## Graph

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `GraphHandle::as_of` | `Graph.query(as_of=)` | `asOf` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::both` | `Graph.query(hops=)` | `both` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::conversation` | `Graph.query(conversation=)` | `conversation` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::fetch` | `Graph.query` | `fetch` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::incoming` | `Graph.query(hops=)` | `incoming` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::limit` | `Graph.query(limit=)` | `limit` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::link` | `link` | `link` |  |
| `GraphHandle::neighbors` | `neighbors` | `neighbors` |  |
| `GraphHandle::out` | `Graph.query(hops=)` | `out` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::relink` | `relink` | `relink` |  |
| `GraphHandle::return_edges` | `Graph.query(returns=)` | `returnEdges` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::return_paths` | `Graph.query(returns=)` | `returnPaths` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::return_triplets` | `Graph.query(returns=)` | `returnTriplets` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::start_ids` | `Graph.query(start_ids=)` | `startIds` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::start_match` | `Graph.query(start_match=)` | `startMatch` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::start_nearest` | `Graph.query(nearest=)` | `startNearest` | Python takes keyword arguments where Rust chains a builder |
| `GraphHandle::unlink` | `unlink` | `unlink` |  |
| `GraphHandle::upsert` | `upsert` | `upsert` |  |

## Memory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Memory::append` | `Memory.append` | `MemoryHandle.append` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `Memory::forget` | `Memory.forget` | `MemoryHandle.forget` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `Memory::improve` | `Memory.improve` | `MemoryHandle.improve` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `Memory::recall` | `Memory.recall` | `MemoryHandle.recall` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `Memory::remember` | `Memory.remember` | `MemoryHandle.remember` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `MemoryHandle::backend` | `Memory.backend_name` | `MemoryHandle.backend` | Python returns the backend name |
| `MemoryHandle::consolidate` | `consolidate` | `consolidate` |  |
| `MemoryHandle::context` | `context` | `context` |  |
| `MemoryHandle::embedder` | `Laser.memory_with(embedder=)` | `MemoryHandle.vector` | Python and TypeScript pass the embedder when they open a vector handle |
| `MemoryHandle::fetch` | `fetch` | `fetch` |  |
| `MemoryHandle::fetch_folded` | `fetch_folded` | `fetchFolded` |  |
| `MemoryHandle::forget` | `forget` | `forget` |  |
| `MemoryHandle::improve` | `improve` | `improve` |  |
| `MemoryHandle::recall` | `recall` | `recall` |  |
| `MemoryHandle::recall_folded` | `Memory.recall(folded=)` | `MemoryHandle.recallFolded` | Python takes keyword arguments where Rust chains a builder |
| `MemoryHandle::remember` | `remember` | `remember` |  |
| `MemoryHandle::remove` | `remove` | `remove` |  |
| `MemoryHandle::reranker` | `reranker` | `reranker` |  |
| `MemoryHandle::set` | `set` | `set` |  |
| `MemoryHandle::update` | `update` | `update` |  |
| `MemoryHandle::vector` | `vector` | `vector` |  |
| `RememberBuilder::agent` | `Memory.remember(agent=)` | `RememberBuilder.agent` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::application` | `Memory.remember(application=)` | `RememberBuilder.application` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::dedup` | `Memory.remember(dedup=)` | `RememberBuilder.dedup` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::durable` | `Memory.remember(durable=)` | `RememberBuilder.durable` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::kind` | `Memory.remember(kind=)` | `RememberBuilder.kind` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::scope` | `Memory.remember(conversation=)` | `RememberBuilder.conversation` | Python and TypeScript scope by conversation |
| `RememberBuilder::send` | `Memory.remember` | `RememberBuilder.send` | Python remembers in one call |
| `RememberBuilder::stream` | `Memory.remember(stream=)` | `RememberBuilder.stream` | Python takes keyword arguments where Rust chains a builder |
| `RememberBuilder::user` | `Memory.remember(user=)` | `RememberBuilder.user` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::agent` | `Memory.recall(agent=)` | `RecallBuilder.agent` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::application` | `Memory.recall(application=)` | `RecallBuilder.application` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::block` | `Memory.context` | `RecallBuilder.block` | Python renders the block with context(conversation, token_budget) |
| `RecallBuilder::fetch` | `Memory.recall` | `RecallBuilder.fetch` | Python recalls in one call |
| `RecallBuilder::folded` | `Memory.recall(folded=)` | `RecallBuilder.folded` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::hybrid` | `Memory.recall(strategy=)` | `RecallBuilder.hybrid` | Python selects the strategy by name |
| `RecallBuilder::keyword` | `Memory.recall(strategy=)` | `RecallBuilder.keyword` | Python selects the strategy by name |
| `RecallBuilder::limit` | `Memory.recall(limit=)` | `RecallBuilder.limit` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::recent` | `Memory.recall(strategy=)` | `RecallBuilder.recent` | Python selects the strategy by name |
| `RecallBuilder::semantic` | `Memory.recall(semantic=)` | `RecallBuilder.semantic` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::strategy` | `Memory.recall(strategy=)` | `RecallBuilder.strategy` | Python takes keyword arguments where Rust chains a builder |
| `RecallBuilder::user` | `Memory.recall(user=)` | `RecallBuilder.user` | Python takes keyword arguments where Rust chains a builder |

## ScopedMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ScopedMemory::block` | `block` | `block` |  |
| `ScopedMemory::consolidate` | `consolidate` | `consolidate` |  |
| `ScopedMemory::conversation` | `conversation` | `conversation` |  |
| `ScopedMemory::forget` | `forget` | `forget` |  |
| `ScopedMemory::handle` | `handle` | `handle` |  |
| `ScopedMemory::improve` | `improve` | `improve` |  |
| `ScopedMemory::recall` | `recall` | `recall` |  |
| `ScopedMemory::remember` | `remember` | `remember` |  |
| `ScopedMemory::search` | `search` | `search` |  |

## ContextScope

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContextScope::append` | `append` | `append` |  |
| `ContextScope::block` | `block` | `block` |  |
| `ContextScope::checkpoint` | `checkpoint` | `checkpoint` |  |
| `ContextScope::conversation` | `conversation` | `conversation` |  |
| `ContextScope::fetch` | `fetch` | `fetch` |  |
| `ContextScope::fetch_with` | `fetch_with` | `fetchWith` |  |
| `ContextScope::graph` | `graph` | `graph` |  |
| `ContextScope::laser` | omitted | omitted | Accessor to the owning connection. Python and TypeScript keep the Laser that created the scope |
| `ContextScope::memory` | `memory` | `memory` |  |
| `ContextScope::memory_with` | `ContextScope.memory` | `ContextScope.memory` | Python and TypeScript pass a memory handle to memory |
| `ContextScope::state` | `state` | `state` |  |
| `ContextScope::state_with` | `state_with` | `stateWith` |  |
| `TokenBudget::new` | `TokenBudget.__new__` | `TokenBudget.constructor` | constructor |
| `TokenBudget::with_estimator` | `TokenBudget.__new__(estimator=)` | `TokenBudget.constructor` | Python and TypeScript take the estimator as the second constructor argument |

## Sessions

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Sessions::config` | omitted | `config` | Python configures sessions with keyword arguments and exposes no config object |
| `Sessions::create` | `create` | `create` |  |
| `Sessions::open` | `open` | `open` |  |
| `Sessions::start` | `start` | `start` |  |
| `SessionConfig::context_token_bound` | omitted | `SessionConfig.contextTokens` | Python exposes no config object |
| `SessionConfig::context_tokens` | `Laser.sessions(context_tokens=)` | `SessionConfig.contextTokens` | TypeScript passes an options object |
| `SessionConfig::context_turn_bound` | omitted | `SessionConfig.contextTurns` | Python exposes no config object |
| `SessionConfig::context_turns` | `Laser.sessions(context_turns=)` | `SessionConfig.contextTurns` | TypeScript passes an options object |
| `SessionConfig::kind_for` | omitted | `SessionConfig.kindFor` | Python exposes no config object |
| `SessionConfig::memory_namespace` | `Laser.sessions(memory_namespace=)` | `SessionConfig.memoryNamespace` | TypeScript passes an options object |
| `SessionConfig::memory_namespace_name` | omitted | `SessionConfig.memoryNamespace` | Python exposes no config object |
| `SessionConfig::new` | `Laser.sessions` | `SessionConfig.constructor` | constructor |
| `SessionConfig::stream` | `Laser.sessions(stream=)` | `SessionConfig.stream` | TypeScript passes an options object |
| `SessionConfig::stream_name` | omitted | `SessionConfig.stream` | Python exposes no config object |
| `SessionConfig::topic` | `Laser.sessions(topics=)` | `SessionConfig.topics` | TypeScript passes an options object |
| `SessionConfig::topic_for` | omitted | `SessionConfig.topicFor` | Python exposes no config object |
| `SessionConfig::topics` | omitted | `SessionConfig.topicList` | Python exposes no config object |

## Session

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Session::append` | `append` | `append` |  |
| `Session::checkpoint` | `checkpoint` | `checkpoint` |  |
| `Session::config` | omitted | `Session.config` | Python exposes no config object |
| `Session::context` | `context` | `context` |  |
| `Session::context_with` | `context_with` | `contextWith` |  |
| `Session::conversation` | `conversation` | `conversation` |  |
| `Session::graph` | `graph` | `graph` |  |
| `Session::memory` | `memory` | `memory` |  |
| `Session::memory_in` | `Session.memory_in` | `Session.memory` | TypeScript passes the namespace to memory |
| `Session::replay` | `replay` | `replay` |  |
| `Session::scope` | `scope` | `scope` |  |
| `Session::state_at` | `state_at` | `stateAt` |  |
| `Session::turns_at` | `turns_at` | `turnsAt` |  |
| `Session::turns_since` | `turns_since` | `turnsSince` |  |
| `SessionTurn::text` | `SessionTurn.text` | `fn:sessionTurnText` | TypeScript uses the free function |

## AgentScope

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentScope::advertise` | `advertise` | `advertise` |  |
| `AgentScope::ask` | `ask` | `ask` |  |
| `AgentScope::contract` | `contract` | `contract` |  |
| `AgentScope::id` | `id` | `id` |  |
| `AgentScope::publish_card` | `publish_card` | `publishCard` |  |
| `AgentScope::send` | `send` | `send` |  |

## AgentHandle

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentHandle::abort` | `abort` | `abort` |  |
| `AgentHandle::join` | `join` | `join` |  |
| `AgentHandle::ready` | `ready` | `ready` |  |
| `AgentHandle::shutdown` | `shutdown` | `shutdown` |  |

## Contract

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContractBuilder::conversation` | `Laser.contract(conversation=)` | `conversation` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::deadline` | `Laser.contract(deadline_ms=)` | `deadline` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::expire_if_not_consumed` | `Laser.contract(expire_if_not_consumed_ms=)` | `expireIfNotConsumed` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::fence` | `Laser.contract(fence=)` | `fence` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::from` | `Laser.contract(source=)` | `from` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::inbox_route` | `Laser.contract(fixed_inbox=)` | `inboxRoute` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::payload` | `Laser.contract(payload=)` | `payload` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::registered` | `Laser.contract(registered=)` | `registered` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::reply_on` | `Laser.contract(reply_on=)` | `replyOn` | Python takes keyword arguments where Rust chains a builder |
| `ContractBuilder::send` | `Laser.contract_report` | `send` | Python sends with contract or contract_report |

## Workflow

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Workflow::budget` | `budget` | `budget` |  |
| `Workflow::inbox_route` | `Laser.workflow(fixed_inbox=)` | `Workflow.inboxRoute` | Python takes keyword arguments where Rust chains a builder |
| `Workflow::registered` | `registered` | `registered` |  |
| `Workflow::run` | `run` | `run` |  |
| `Workflow::run_id` | `run_id` | `runId` |  |
| `Workflow::step` | `step` | `step` |  |
| `StepHandle::after` | `Workflow.step(after=)` | `StepBuilder.after` | Python takes keyword arguments where Rust chains a builder |
| `StepHandle::budget` | `Workflow.budget` | `Workflow.budget` | Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle |
| `StepHandle::compensate_with` | `Workflow.step(compensate=)` | `StepBuilder.compensateWith` | Python takes keyword arguments where Rust chains a builder |
| `StepHandle::exclusive` | `Workflow.step(exclusive=)` | `StepBuilder.exclusive` | Python takes keyword arguments where Rust chains a builder |
| `StepHandle::exclusive_in` | `Workflow.step(fence_namespace=)` | `StepBuilder.exclusiveIn` | Python takes keyword arguments where Rust chains a builder |
| `StepHandle::inbox_route` | `Laser.workflow(fixed_inbox=)` | `Workflow.inboxRoute` | Rust forwards from the step handle to its workflow |
| `StepHandle::on_timeout` | `Workflow.step(on_timeout=)` | `StepBuilder.onTimeout` | Python takes keyword arguments where Rust chains a builder |
| `StepHandle::registered` | `Workflow.registered` | `Workflow.registered` | Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle |
| `StepHandle::run` | `Workflow.run` | `Workflow.run` | Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle |
| `StepHandle::run_id` | `Workflow.run_id` | `Workflow.runId` | Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle |
| `StepHandle::step` | `Workflow.step` | `Workflow.step` | Rust forwards from the step handle to its workflow. Python and TypeScript keep the workflow handle |
| `StepHandle::verify_with` | `Workflow.step(verify=)` | `StepBuilder.verifyWith` | Python takes keyword arguments where Rust chains a builder |

## Runs

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Runs::cancel` | `cancel` | `cancel` |  |
| `Runs::list` | `list` | `list` |  |
| `Runs::register_source` | `register_source` | `registerSource` |  |
| `Runs::remove_source` | `remove_source` | `removeSource` |  |
| `Runs::status` | `status` | `status` |  |
| `Runs::submit` | `submit` | `submit` |  |
| `Runs::submit_budgeted` | `submit_budgeted` | `submitBudgeted` |  |
| `Runs::submit_with` | `submit_with` | `submitWith` |  |
| `RunListRequest::agent` | `Runs.list(agent_id=)` | `RunListRequest.agent` | Python takes keyword arguments where Rust chains a builder |
| `RunListRequest::cursor` | `Runs.list(cursor=)` | `RunListRequest.cursor` | Python takes keyword arguments where Rust chains a builder |
| `RunListRequest::fetch` | `Runs.list` | `RunListRequest.fetch` | Python takes keyword arguments where Rust chains a builder |
| `RunListRequest::limit` | `Runs.list(limit=)` | `RunListRequest.limit` | Python takes keyword arguments where Rust chains a builder |
| `RunListRequest::state` | `Runs.list(state=)` | `RunListRequest.state` | Python takes keyword arguments where Rust chains a builder |

## Destinations

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Destinations::accept_retention_gap` | `accept_retention_gap` | `acceptRetentionGap` |  |
| `Destinations::acquire_lease` | `acquire_lease` | `acquireLease` |  |
| `Destinations::add_partition` | `add_partition` | `addPartition` |  |
| `Destinations::bind_table` | `bind_table` | `bindTable` |  |
| `Destinations::clear_block` | `clear_block` | `clearBlock` |  |
| `Destinations::complete` | `complete` | `complete` |  |
| `Destinations::get` | `get` | `get` |  |
| `Destinations::list` | `list` | `list` |  |
| `Destinations::mutate` | `mutate` | `mutate` |  |
| `Destinations::mutate_with_supervisor_assertion` | `Destinations.mutate(supervisor_assertion=)` | `mutateWithSupervisorAssertion` | Python takes keyword arguments where Rust chains a builder |
| `Destinations::observe_partition_lifecycle` | `observe_partition_lifecycle` | `observePartitionLifecycle` |  |
| `Destinations::prepare` | `prepare` | `prepare` |  |
| `Destinations::query_routes` | `query_routes` | `queryRoutes` |  |
| `Destinations::record_block` | `record_block` | `recordBlock` |  |
| `Destinations::record_repair` | `record_repair` | `recordRepair` |  |
| `Destinations::record_retention_gap` | `record_retention_gap` | `recordRetentionGap` |  |
| `Destinations::register` | `register` | `register` |  |
| `Destinations::register_query_route` | `register_query_route` | `registerQueryRoute` |  |
| `Destinations::remove_query_route` | `remove_query_route` | `removeQueryRoute` |  |
| `Destinations::renew_lease` | `renew_lease` | `renewLease` |  |
| `Destinations::set_desired_state` | `set_desired_state` | `setDesiredState` |  |
| `Destinations::supersede_generation` | `supersede_generation` | `supersedeGeneration` |  |
| `Destinations::take_over_lease` | `take_over_lease` | `takeOverLease` |  |

## Projections

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Projections::drop` | `Laser.drop_projection` | `Projections.drop` | Python flattens the handles onto Laser (drop_projection, drop_schema) |
| `Projections::drop_graph` | `Laser.drop_graph` | `Projections.dropGraph` | Python flattens the handles onto Laser |
| `Projections::get` | `Laser.get_projection` | `Projections.get` | Python flattens the handles onto Laser (get_projection, get_schema) |
| `Projections::list` | `Laser.list_projections` | `Projections.list` | Python flattens the handles onto Laser (list_projections, list_schemas) |
| `Projections::register` | `Laser.register_projection` | `Projections.register` | Python flattens the handles onto Laser (register_projection, register_schema) |
| `Projections::register_graph` | `Laser.register_graph` | `Projections.registerGraph` | Python flattens the handles onto Laser |
| `Bindings::apply` | `Laser.apply_binding` | `Bindings.apply` | Python flattens the handles onto Laser |
| `Bindings::remove` | `Laser.remove_binding` | `Bindings.remove` | Python flattens the handles onto Laser |
| `Schemas::drop` | `Laser.drop_schema` | `Schemas.drop` | Python flattens schema operations onto Laser |
| `Schemas::get` | `Laser.get_schema` | `Schemas.get` | Python flattens schema operations onto Laser |
| `Schemas::list` | `Laser.list_schemas` | `Schemas.list` | Python flattens schema operations onto Laser |
| `Schemas::register` | `Laser.register_schema` | `Schemas.register` | Python flattens schema operations onto Laser |

## Watch

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Watch::index` | `Laser.watch(index=)` | `Watch.index` | Python takes keyword arguments where Rust chains a builder |
| `Watch::records` | `Laser.watch` | `Watch.records` | Python opens the reader with watch |
| `WatchReader::from_offsets` | `from_offsets` | `fromOffsets` |  |
| `WatchReader::offsets` | `offsets` | `offsets` |  |
| `WatchReader::poll` | `poll` | `poll` |  |
| `WatchReader::stream` | `WatchReader.__aiter__` | `WatchReader.stream` | Python iterates with async for |

## Capabilities

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Capabilities::backend` | `backend` | `fn:backend` | TypeScript uses the free function |
| `Capabilities::enabled_backends` | `enabled_backends` | `fn:enabledBackends` | TypeScript uses the free function |
| `Capabilities::is_open_only` | `is_open_only` | `fn:isOpenOnly` | TypeScript uses the free function |
| `Capabilities::is_ready` | `is_ready` | `fn:isReady` | TypeScript uses the free function |
| `Capabilities::readiness_reasons` | `readiness_reasons` | `fn:readinessReasons` | TypeScript uses the free function |
| `Capabilities::serves_consistency` | `serves_consistency` | `fn:servesConsistency` | TypeScript uses the free function |
| `Capabilities::unready_backends` | `unready_backends` | `fn:unreadyBackends` | TypeScript uses the free function |
| `Capabilities::with_a2a_gateway` | `Laser.with_capabilities(a2a_gateway=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_agent_workflow` | `Laser.with_capabilities(agent_workflow=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_backends` | `Laser.with_capabilities(backends=)` | `Laser.withCapabilities` | TypeScript passes a Capabilities object |
| `Capabilities::with_destination_consistency` | `Laser.with_capabilities(destinations_consistency=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_destinations` | `Laser.with_capabilities(destinations=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_filters` | `Laser.with_capabilities(filters=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_forks` | `Laser.with_capabilities(forks=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_graph` | `Laser.with_capabilities(graph=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_kv` | `Laser.with_capabilities(kv=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_kv_cas` | `Laser.with_capabilities(kv_cas=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_kv_cas_fenced` | `Laser.with_capabilities(kv_cas_fenced=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_kv_fenced_leases` | `Laser.with_capabilities(kv_fenced_leases=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_managed` | `Laser.with_capabilities(managed=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_query` | `Laser.with_capabilities(query=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_query_consistency` | `Laser.with_capabilities(query_consistency=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_query_execution` | `Laser.with_capabilities(query_execution=)` | `Laser.withCapabilities` | TypeScript passes a Capabilities object |
| `Capabilities::with_query_keyword` | `Laser.with_capabilities(query_keyword=)` | `Laser.withCapabilities` | Python injects flags with with_capabilities keywords. TypeScript passes a Capabilities object |
| `Capabilities::with_versions` | `Laser.with_capabilities(versions=)` | `Laser.withCapabilities` | TypeScript passes a Capabilities object |

## AgentRegistry

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentRegistry::agents` | `agents` | `agents` |  |
| `AgentRegistry::inbox_for` | `inbox_for` | `inboxFor` |  |
| `AgentRegistry::inbox_for_principal` | `inbox_for_principal` | `inboxForPrincipal` |  |
| `AgentRegistry::is_quarantined` | `is_quarantined` | `isQuarantined` |  |
| `AgentRegistry::lookup` | `lookup` | `lookup` |  |
| `AgentRegistry::principal_for` | `principal_for` | `principalFor` |  |
| `AgentRegistry::refresh` | `refresh` | `refresh` |  |
| `AgentRegistry::refresh_presence` | `refresh_presence` | `refreshPresence` |  |
| `AgentRegistry::resolve` | `resolve` | `resolve` |  |

## Agdx

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Agdx::command` | `command` | `command` |  |
| `Agdx::emit` | `emit` | `emit` |  |
| `Agdx::fail` | `fail` | `fail` |  |
| `Agdx::request_input` | `request_input` | `requestInput` |  |
| `Agdx::respond` | `respond` | `respond` |  |
| `Agdx::status` | `status` | `status` |  |
| `Agdx::stream` | `stream` | `stream` |  |
| `AgdxStream::buffered` | `buffered` | `buffered` |  |
| `AgdxStream::channel` | `channel` | `channel` |  |
| `AgdxStream::content_type` | `content_type` | `contentType` |  |
| `AgdxStream::fail` | `AgdxStream.fail` | `AgdxStream.fail` |  |
| `AgdxStream::finish` | `finish` | `finish` |  |
| `AgdxStream::flush` | `flush` | `flush` |  |
| `AgdxStream::with_deadline_micros` | `with_deadline_micros` | `withDeadlineMicros` |  |
| `AgdxStream::with_target` | `with_target` | `withTarget` |  |
| `AgdxStream::write` | `write` | `write` |  |

## Governance

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `QuorumGovernor::new` | `QuorumGovernor.__new__` | `QuorumGovernor.constructor` | constructor |
| `QuorumGovernor::voter` | `voter` | `voter` |  |
| `SwappableGovernor::current` | `current` | `current` |  |
| `SwappableGovernor::new` | `SwappableGovernor.__new__` | `SwappableGovernor.constructor` | constructor |
| `SwappableGovernor::swap` | `swap` | `swap` |  |
| `Intent::new` | `Intent.__new__` | `Intent.constructor` | constructor |
| `Intent::validate` | `validate` | `validate` |  |
| `Vote::cast` | `cast` | `cast` |  |
| `ActionDecision::allow` | `allow` | `allow` |  |
| `ActionDecision::block` | `block` | `block` |  |
| `ActionDecision::defer` | `defer` | `defer` |  |
| `ActionDecision::modify` | `modify` | `modify` |  |
| `ActionDecision::observe` | `ActionDecision.observe` | `ActionDecision.observe` |  |
| `ActionDecision::step_up` | `step_up` | `stepUp` |  |
| `ActionDecision::with_policy` | `with_policy` | `withPolicy` |  |
| `ActionDecision::with_reason` | `with_reason` | `withReason` |  |
| `ActionDecision::with_risk_score` | `with_risk_score` | `withRiskScore` |  |
| `SwarmActivity::agent` | `agent` | `agent` |  |
| `SwarmActivity::agents` | `agents` | `agents` |  |
| `SwarmActivity::new` | `SwarmActivity.__new__` | `new SwarmActivity()` | constructor |
| `SwarmActivity::observe` | `SwarmActivity.observe` | `SwarmActivity.observe` |  |

## Signing

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SigningKey::from_bytes` | `SigningKey.__new__` | `SigningKey.fromBytes` | Python constructs the key from its secret bytes |
| `SigningKey::key_id` | `key_id` | `keyId` |  |
| `SigningKey::sign` | `sign` | `sign` |  |
| `SigningKey::sign_with_context` | `sign_with_context` | `signWithContext` |  |
| `SigningKey::verifying_key` | `verifying_key` | `verifyingKey` |  |
| `KeyRegistry::enroll` | `enroll` | `enroll` |  |
| `KeyRegistry::enroll_operator` | `enroll_operator` | `enrollOperator` |  |
| `KeyRegistry::enroll_record` | `enroll_record` | `enrollRecord` |  |
| `KeyRegistry::new` | `KeyRegistry.__new__` | `new KeyRegistry()` | constructor |
| `KeyRegistry::verify` | `verify` | `verify` |  |
| `KeyRegistry::verify_at` | `verify_at` | `verifyAt` |  |
| `KeyRegistry::verify_observed_at` | `verify_observed_at` | `verifyObservedAt` |  |

## A2aBridge

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `A2aBridge::cancel` | `cancel` | `cancel` |  |
| `A2aBridge::card` | `card` | `card` |  |
| `A2aBridge::new` | `Laser.a2a_bridge` | `A2aBridge.constructor` | constructor |
| `A2aBridge::router` | omitted | `A2aBridge.handleRpc` | Rust-only axum router behind the HTTP feature. TypeScript serves JSON-RPC through handleRpc |
| `A2aBridge::signed_card` | `signed_card` | `signedCard` |  |
| `A2aBridge::submit` | `submit` | `submit` |  |
| `A2aBridge::task` | `task` | `task` |  |
| `A2aBridge::with_capabilities` | `Laser.a2a_bridge(capabilities=)` | `withCapabilities` | Python takes keyword arguments where Rust chains a builder |
| `A2aBridge::with_signing_key` | `Laser.a2a_bridge(signing_key=)` | `withSigningKey` | Python takes keyword arguments where Rust chains a builder |

## McpBridge

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `McpBridge::call_tool` | `call_tool` | `callTool` |  |
| `McpBridge::get_prompt` | `get_prompt` | `getPrompt` |  |
| `McpBridge::initialize` | `initialize` | `initialize` |  |
| `McpBridge::list_prompts` | `list_prompts` | `listPrompts` |  |
| `McpBridge::list_resources` | `list_resources` | `listResources` |  |
| `McpBridge::list_tools` | `list_tools` | `listTools` |  |
| `McpBridge::new` | `Laser.mcp_bridge` | `McpBridge.constructor` | constructor |
| `McpBridge::read_resource` | `read_resource` | `readResource` |  |
| `McpBridge::router` | omitted | `McpBridge.handleRpc` | Rust-only axum router behind the HTTP feature. TypeScript serves JSON-RPC through handleRpc |
| `McpBridge::with_memory_tools` | `Laser.mcp_bridge(memory_tools=)` | `withMemoryTools` | Python takes keyword arguments where Rust chains a builder |
| `McpBridge::with_prompt` | `Laser.mcp_bridge(prompts=)` | `withPrompt` | Python takes keyword arguments where Rust chains a builder |
| `McpBridge::with_resource` | `Laser.mcp_bridge(resources=)` | `withResource` | Python takes keyword arguments where Rust chains a builder |
| `McpBridge::with_timeout` | `Laser.mcp_bridge(timeout_secs=)` | `withTimeout` | Python takes keyword arguments where Rust chains a builder |
| `McpBridge::with_tool` | `Laser.mcp_bridge(tools=)` | `withTool` | Python takes keyword arguments where Rust chains a builder |

## LaserError

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LaserError::code` | `code` | `fn:code` | TypeScript uses the free function |
| `LaserError::filter_reason` | `FilterError.reason` | `fn:filterReason` | Python reads reason on FilterError. TypeScript uses the free function |
| `LaserError::is_ambiguous_mutation` | `LaserError.ambiguous_mutation` | `fn:isAmbiguousMutation` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_budget_exceeded` | `LaserError.budget_exceeded` | `fn:isBudgetExceeded` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_fence_violation` | `LaserError.fence_violation` | `fn:isFenceViolation` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_lease_lost` | `LaserError.lease_lost` | `fn:isLeaseLost` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_no_capable_agent` | `LaserError.no_capable_agent` | `fn:isNoCapableAgent` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_not_found` | `LaserError.not_found` | `fn:isNotFound` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_not_leader` | `LaserError.not_leader` | `fn:isNotLeader` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_permission_denied` | `LaserError.permission_denied` | `fn:isPermissionDenied` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_quarantined` | `LaserError.quarantined` | `fn:isQuarantined` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_retryable` | `LaserError.retryable` | `fn:isRetryable` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_stale` | `LaserError.stale` | `fn:isStale` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_stream_or_topic_not_found` | `LaserError.stream_or_topic_not_found` | `fn:isStreamOrTopicNotFound` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_unavailable` | `LaserError.unavailable` | `fn:isUnavailable` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_unsupported` | `LaserError.unsupported` | `fn:isUnsupported` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_version_conflict` | `LaserError.version_conflict` | `fn:isVersionConflict` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::is_version_skew` | `LaserError.version_skew` | `fn:isVersionSkew` | Python reads a boolean attribute. TypeScript uses the free function |
| `LaserError::publish_cause` | omitted | `PublishFailedError.publishCause` | Python raises the cause class itself with committed and unconfirmed_count attributes |
| `LaserError::rejected` | omitted | omitted | Rust constructor for SDK internals |
| `LaserError::unsupported` | omitted | omitted | Rust constructor for SDK internals |
| `LaserError::unsupported_feature` | omitted | omitted | Rust constructor for SDK internals |

## ActionGovernor

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ActionGovernor::decide` | `Laser.with_governor(governor=)` | `ActionGovernor.decide` | Python accepts a governor object with decide |

## ActionKind

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ActionKind::as_str` | `GovernedAction.kind` | omitted | Kinds are plain str in Python. ActionKind is a string literal union in TypeScript |

## AgdxSend

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgdxSend::body` | `Agdx.status(body=)` | `body` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::claim_check` | `Agdx.command(claim_check=)` | `claimCheck` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::content_type` | `Agdx.command(content_type=)` | `contentType` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::last` | `Agdx.status(last=)` | `last` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::send` | `Agdx.command` | `send` | Each Python verb publishes when awaited |
| `AgdxSend::signed_by` | `Laser.agdx(signing_key=)` | `signedBy` | Python sets the signing key once per producer |
| `AgdxSend::with_cause` | `Agdx.command(cause=)` | `withCause` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_correlation` | `Agdx.status(correlation=)` | `withCorrelation` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_deadline_micros` | `Agdx.command(deadline_micros=)` | `withDeadlineMicros` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_idempotency_key` | `Agdx.command(idempotency_key=)` | `withIdempotencyKey` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_metadata` | `Agdx.command(metadata=)` | `withMetadata` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_operation` | `Agdx.command(operation=)` | `withOperation` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_target` | `Agdx.command(target=)` | `withTarget` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_task_state` | `Agdx.status(task_state=)` | `withTaskState` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_tool` | `Agdx.command(tool=)` | `withTool` | Python takes keyword arguments where Rust chains a builder |
| `AgdxSend::with_usage` | `Agdx.command(usage=)` | `withUsage` | Python takes keyword arguments where Rust chains a builder |

## Agent

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Agent::builder` | `Laser.spawn_agent` | `Agent.builder` | Python configures the runtime through spawn_agent |
| `Agent::spawn` | `Laser.spawn_agent` | `AgentBuilder.spawn` | Python and TypeScript build and spawn in one call |

## AgentActivity

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentActivity::count` | `count` | `count` |  |

## AgentBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentBuilder::ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `AgentBuilder.ackOnPickup` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::build` | `Laser.spawn_agent` | `AgentBuilder.spawn` | Python and TypeScript build and spawn in one call |
| `AgentBuilder::capabilities` | `Laser.spawn_agent(capabilities=)` | `AgentBuilder.capabilities` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::concurrency` | `Laser.spawn_agent(max_partitions=)` | `AgentBuilder.concurrency` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::consolidate_every` | `Laser.spawn_agent(consolidate_every_ms=)` | `AgentBuilder.consolidateEvery` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::consolidator` | `Laser.spawn_agent(consolidator=)` | `AgentBuilder.consolidator` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::consumer_group` | `Laser.spawn_agent(consumer_group=)` | `AgentBuilder.consumerGroup` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::dedup_window` | `Laser.spawn_agent(dedup_window=)` | `AgentBuilder.dedupWindow` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::deduplicator` | `Laser.spawn_agent(dedup=)` | `AgentBuilder.deduplicator` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::governor` | `Laser.spawn_agent(governor=)` | `AgentBuilder.governor` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::governor_retention` | `Laser.spawn_agent(governor_retention=)` | `AgentBuilder.governor` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::handler` | `Laser.spawn_agent(handler=)` | `AgentBuilder.handler` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::id` | `Laser.spawn_agent(agent_id=)` | `AgentBuilder.id` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `AgentBuilder.inboxRoute` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::listen_on` | `Laser.spawn_agent(listen_on=)` | `AgentBuilder.listenOn` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `AgentBuilder.maxQueuedBytes` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `AgentBuilder.maxQueuedRecords` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::middleware` | `Laser.spawn_agent(middleware=)` | `AgentBuilder.middleware` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `AgentBuilder.deadLetterSink` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `AgentBuilder.pollInterval` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::respond_on` | `Laser.spawn_agent(respond_on=)` | `AgentBuilder.respondOn` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `AgentBuilder.retry` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `AgentBuilder.shutdownGrace` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::signing_key` | `Laser.spawn_agent(signing_key=)` | `AgentBuilder.signingKey` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::understood_features` | `Laser.spawn_agent(understood_features=)` | `AgentBuilder.understoodFeatures` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::verifier` | `Laser.spawn_agent(verifier=)` | `AgentBuilder.verifier` | Python uses keywords. TypeScript uses a builder or object field |
| `AgentBuilder::warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `AgentBuilder.warmDedup` | Python uses keywords. TypeScript uses a builder or object field |

## AgentCtx

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentCtx::approval_gate` | `approval_gate` | `AgentContext.approvalGate` |  |
| `AgentCtx::fan_out` | `fan_out` | `AgentContext.fanOut` |  |
| `AgentCtx::laser` | `laser` | `AgentContext.laser` |  |
| `AgentCtx::message` | `message` | `AgentContext.message` |  |
| `AgentCtx::reply_on` | `reply_on` | `AgentContext.replyOn` |  |
| `AgentCtx::request` | `request` | `AgentContext.request` |  |
| `AgentCtx::respond` | `respond` | `AgentContext.respond` |  |
| `AgentCtx::respond_input` | `respond_input` | `AgentContext.respondInput` |  |
| `AgentCtx::send` | `send` | `AgentContext.send` |  |
| `AgentCtx::spawn_subconversation` | `spawn_subconversation` | `AgentContext.spawnSubconversation` |  |

## AgentHandler

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentHandler::handle` | `Laser.spawn_agent(handler=)` | `AgentHandler.handle` | Python accepts a callable or handle object |

## AgentId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentId::as_str` | omitted | `AgentId.asString` | Agent ids are plain str in Python |
| `AgentId::new` | omitted | `new` | Agent ids are plain str in Python |
| `AgentId::wire_id` | omitted | `fn:parseWireAgentId` | Wire-encoding conversion for SDK internals. TypeScript uses the free function |

## AgentMessage

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentMessage::body` | `body` | `fn:agentMessageBody` | TypeScript uses the free function |

## AgentMiddleware

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentMiddleware::after_handle` | `Laser.spawn_agent(middleware=)` | `AgentMiddleware.afterHandle` | Python middleware objects receive full attempt results |
| `AgentMiddleware::before_handle` | `Laser.spawn_agent(middleware=)` | `AgentMiddleware.beforeHandle` | Python middleware objects receive full attempt results |

## AgentTopic

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentTopic::as_identifier` | omitted | omitted | Rust-only escape hatch to the Apache Iggy SDK builders |
| `AgentTopic::name` | omitted | omitted | Topics are plain str in Python. AgentTopic is a string literal union in TypeScript |
| `AgentTopic::topic_string` | omitted | omitted | Topics are plain str in Python. AgentTopic is a string literal union in TypeScript |

## BatchPublishRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BatchPublishRequest::add_arrow_ipc` | `add_arrow_ipc` | `addArrowIpc` |  |
| `BatchPublishRequest::add_avro` | `add_avro` | `addAvro` |  |
| `BatchPublishRequest::add_encoded` | `BatchPublishRequest.add_raw_bytes` | `addEncoded` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `BatchPublishRequest::add_encoded_with_projection` | `BatchPublishRequest.add_raw_bytes(projection_ref=)` | `addEncodedWithProjection` | Python takes keyword arguments where Rust chains a builder |
| `BatchPublishRequest::add_json` | `add_json` | `addJson` |  |
| `BatchPublishRequest::add_json_with_projection` | `BatchPublishRequest.add_json(projection_ref=)` | `addJsonWithProjection` | Python takes keyword arguments where Rust chains a builder |
| `BatchPublishRequest::add_msgpack` | `add_msgpack` | `addMsgpack` |  |
| `BatchPublishRequest::add_msgpack_with_projection` | `BatchPublishRequest.add_msgpack(projection_ref=)` | `BatchPublishRequest.addMessagePackWithProjection` | Python takes keyword arguments where Rust chains a builder |
| `BatchPublishRequest::add_payload` | `add_payload` | `addPayload` |  |
| `BatchPublishRequest::add_payload_with_projection` | `BatchPublishRequest.add_payload(projection_ref=)` | `addPayloadWithProjection` | Python takes keyword arguments where Rust chains a builder |
| `BatchPublishRequest::add_raw_bytes` | `add_raw_bytes` | `addRawBytes` |  |
| `BatchPublishRequest::add_raw_bytes_with_projection` | `BatchPublishRequest.add_raw_bytes(projection_ref=)` | `addRawBytesWithProjection` | Python takes keyword arguments where Rust chains a builder |
| `BatchPublishRequest::add_record` | `add_record` | `addRecord` |  |
| `BatchPublishRequest::content_type` | `BatchPublishRequest.add_raw_bytes` | `contentType` | Python gives the content type per record |
| `BatchPublishRequest::extend_encoded` | `BatchPublishRequest.extend_json` | `extendEncoded` | Rust is generic over a codec. Python uses json, msgpack, or payload |
| `BatchPublishRequest::extend_json` | `extend_json` | `extendJson` |  |
| `BatchPublishRequest::extend_msgpack` | `extend_msgpack` | `BatchPublishRequest.extendMessagePack` |  |
| `BatchPublishRequest::header` | `header` | `header` |  |
| `BatchPublishRequest::index` | `index` | `index` |  |
| `BatchPublishRequest::inline_payload` | `inline_payload` | `inlinePayload` |  |
| `BatchPublishRequest::is_empty` | `BatchPublishRequest.__len__` | `isEmpty` | Python uses len(batch) |
| `BatchPublishRequest::len` | `BatchPublishRequest.__len__` | `BatchPublishRequest.length` | Python uses len(batch) |
| `BatchPublishRequest::partition_key` | `partition_key` | `partitionKey` |  |
| `BatchPublishRequest::projection_ref` | `projection_ref` | `projectionRef` |  |
| `BatchPublishRequest::schema_id` | `schema_id` | `schemaId` |  |
| `BatchPublishRequest::send` | `send` | `send` |  |

## BlobStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BlobStore::get` | `AgentMessage.resolve_body(store=)` | `BlobStore.get` | Python accepts a store object with put/get |
| `BlobStore::put` | `PublishRequest.claim_check(store=)` | `BlobStore.put` | Python accepts a store object with put/get |

## Budget

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Budget::invocations` | `Workflow.budget(invocations=)` | `invocations` | Python takes keyword arguments where Rust chains a builder |
| `Budget::tokens` | `Workflow.budget(tokens=)` | `tokens` | Python takes keyword arguments where Rust chains a builder |
| `Budget::unlimited` | `Workflow.budget` | `unlimited` | The default when no keyword is passed |
| `Budget::wall_clock` | `Workflow.budget(wall_clock_ms=)` | `wallClock` | Python takes keyword arguments where Rust chains a builder |

## CapabilitySelector

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `CapabilitySelector::new` | `Laser.contract(policy=)` | `fn:capabilitySelector` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |
| `CapabilitySelector::principal` | `Laser.contract(principal=)` | `principal` | Python takes keyword arguments where Rust chains a builder |

## Checkpoint

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Checkpoint::is_empty` | `is_empty` | `isEmpty` |  |
| `Checkpoint::topic_offsets` | `topic_offsets` | `topicOffsets` |  |

## ChunkAssembler

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ChunkAssembler::abandon` | `abandon` | `abandon` |  |
| `ChunkAssembler::duplicates_dropped` | `duplicates_dropped` | `duplicatesDropped` |  |
| `ChunkAssembler::feed` | `feed` | `feed` |  |
| `ChunkAssembler::is_finished` | `ChunkAssembler.finished` | `isFinished` | Python reads a property |
| `ChunkAssembler::late_dropped` | `late_dropped` | `lateDropped` |  |
| `ChunkAssembler::new` | `new ChunkAssembler()` | `new ChunkAssembler()` | constructor |

## ClientMetadataRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ClientMetadataRequest::after` | `Laser.client_metadata(after=)` | `after` | Python takes keyword arguments where Rust chains a builder |
| `ClientMetadataRequest::all` | `Laser.client_metadata(after=)` | `all` | Python walks pages with the returned next_cursor |
| `ClientMetadataRequest::limit` | `Laser.client_metadata(limit=)` | `limit` | Python takes keyword arguments where Rust chains a builder |
| `ClientMetadataRequest::page` | `Laser.client_metadata` | `page` | Python takes keyword arguments where Rust chains a builder |
| `ClientMetadataRequest::principal` | `Laser.client_metadata(principal=)` | `principal` | Python takes keyword arguments where Rust chains a builder |
| `ClientMetadataRequest::with_metadata_only` | `Laser.client_metadata(metadata_only=)` | `withMetadataOnly` | Python takes keyword arguments where Rust chains a builder |

## Clock

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Clock::now_micros` | `SystemClock.now_micros` | `Clock.nowMicros` |  |

## CompiledSchema

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `CompiledSchema::compile` | `compile` | `compile` |  |
| `CompiledSchema::decode` | `decode` | `decode` |  |
| `CompiledSchema::encode_avro` | `encode_avro` | `CompiledSchema.encode` | TypeScript encodes every schema kind with encode |
| `CompiledSchema::validate` | `validate` | `validate` |  |
| `CompiledSchema::validate_value` | `validate_value` | `validateValue` |  |

## Consolidator

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Consolidator::consolidate` | `Laser.spawn_agent(consolidator=)` | `Consolidator.consolidate` | Python accepts a callable or consolidate object |

## ConsumerGroupName

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConsumerGroupName::as_str` | `ConsumerGroup.name` | `ConsumerGroupName.asString` | Group names are plain str in Python |
| `ConsumerGroupName::for_agent` | `Laser.spawn_agent(consumer_group=)` | `forAgent` | Leaving consumer_group unset uses the agent id |
| `ConsumerGroupName::new` | `Topic.consumer_group` | `new` | Group names are plain str in Python |

## ConsumerMessage

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConsumerMessage::json` | `json` | `json` |  |

## ContextAssembler

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContextAssembler::assemble` | `Laser.assemble_context(topics=, last_n=, roles=, token_budget=)` | `assemble` | Python takes keyword arguments where Rust chains a builder |
| `ContextAssembler::builder` | `Laser.assemble_context` | `ContextAssembler.builder` | Python assembles the configured context in one call |

## ContextAssemblerBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContextAssemblerBuilder::across_subconversations` | `Laser.assemble_context(across_subconversations=)` | `ContextAssemblerBuilder.acrossSubconversations` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::build` | `Laser.assemble_context` | `ContextAssemblerBuilder.build` | Python assembles the configured context in one call |
| `ContextAssemblerBuilder::conversation_id` | `Laser.assemble_context(conversation_id=)` | `ContextAssemblerBuilder.constructor` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::from_checkpoint` | `Laser.assemble_context(from_checkpoint=)` | `ContextAssemblerBuilder.fromCheckpoint` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::from_offsets` | `Laser.assemble_context(from_offsets=)` | `ContextAssemblerBuilder.fromOffsets` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::policy` | `Laser.assemble_context(policy=)` | `ContextAssemblerBuilder.policy` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::to_checkpoint` | `Laser.assemble_context(to_checkpoint=)` | `ContextAssemblerBuilder.toCheckpoint` | Python uses keywords. TypeScript uses a builder or object field |
| `ContextAssemblerBuilder::topics` | `Laser.assemble_context(topics=)` | `ContextAssemblerBuilder.topics` | Python uses keywords. TypeScript uses a builder or object field |

## ContextPolicy

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContextPolicy::select` | `Laser.assemble_context(policy=)` | `ContextPolicy.select` | Python accepts a synchronous callable or select object |

## ConversationId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConversationId::as_u128` | omitted | `asU128` | Conversation ids are plain str in Python |
| `ConversationId::derive` | `fn:derive_conversation_id` | `derive` |  |
| `ConversationId::new` | `fn:new_conversation_id` | `new` |  |

## ConversationState

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConversationState::load` | `ContextScope.state` | `ConversationState.load` |  |
| `ConversationState::load_with` | `ContextScope.state_with` | `ConversationState.loadWith` |  |

## CrashContext

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `CrashContext::assemble` | `new CrashContext()` | `CrashContext.constructor` | The Python constructor assembles. constructor |
| `CrashContext::summarize` | `summarize` | `summarize` |  |

## CreateConsumerGroup

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `CreateConsumerGroup::build` | `ConsumerGroup.create` | `ConsumerGroup.create` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `CreateConsumerGroup::filter` | `ConsumerGroup.create(filter=)` | `CreateConsumerGroupOptions.filter` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `CreateConsumerGroup::operation_id` | `ConsumerGroup.create(operation_id=)` | `CreateConsumerGroupOptions.operationId` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `CreateConsumerGroup::policy` | `ConsumerGroup.create(filter_id=, revision=)` | `CreateConsumerGroupOptions.policy` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |

## DeadLetterSink

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `DeadLetterSink::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `DeadLetterSink.onDeadLetter` | Python receives the full capsule and typed publish error |

## Decision

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Decision::authorizes` | `authorizes` | `authorizes` |  |

## DedicatedKvTransport

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `DedicatedKvTransport::close` | `close` | `close` |  |
| `DedicatedKvTransport::new` | `new DedicatedKvTransport()` | `DedicatedKvTransport.constructor` |  |

## Deduplicator

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Deduplicator::observe` | `Laser.spawn_agent(dedup=)` | `Deduplicator.observe` | Python accepts an observe object |

## DefaultConsolidator

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `DefaultConsolidator::new` | `Memory.consolidate` | `MemoryHandle.consolidate` | Python takes keyword arguments where Rust chains a builder |
| `DefaultConsolidator::prune_summarized` | `Memory.consolidate(prune_summarized=)` | `ConsolidateOptions.pruneSummarized` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `DefaultConsolidator::with_summarizer` | `Memory.consolidate(summarizer=)` | `ConsolidateOptions.summarizer` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |

## DynManagedKvTransport

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `DynManagedKvTransport::close` | `DedicatedKvTransport.close` | `ManagedKvTransport.close` | Custom Python transports also supply these methods to FencedLeaseClient |
| `DynManagedKvTransport::ready` | `DedicatedKvTransport.ready` | `ManagedKvTransport.ready` | Custom Python transports also supply these methods to FencedLeaseClient |
| `DynManagedKvTransport::reset` | `DedicatedKvTransport.reset` | `ManagedKvTransport.reset` | Custom Python transports also supply these methods to FencedLeaseClient |
| `DynManagedKvTransport::send` | `DedicatedKvTransport.send` | `ManagedKvTransport.send` | Custom Python transports also supply these methods to FencedLeaseClient |

## DynMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `DynMemory::append` | `Memory.append` | `MemoryHandle.append` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `DynMemory::forget` | `Memory.forget` | `MemoryHandle.forget` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `DynMemory::improve` | `Memory.improve` | `MemoryHandle.improve` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `DynMemory::recall` | `Memory.recall` | `MemoryHandle.recall` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `DynMemory::remember` | `Memory.remember` | `MemoryHandle.remember` | Python uses scope keywords. Local and boxed Rust traits share the same binding |

## EdgeDenial

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `EdgeDenial::challenge` | `fn:authorize_edge` | `fn:edgeDenialChallenge` | Python returns the challenge in a tuple. TypeScript uses the free function |
| `EdgeDenial::code` | `fn:authorize_edge` | `fn:edgeDenialCode` | A denial without a challenge is unauthenticated, with one it is a step-up. TypeScript uses the free function |

## Embedder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Embedder::embed` | `Memory.vector(embedder=)` | `Embedder.embed` | Python accepts a callable |

## Feedback

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Feedback::new` | `Memory.improve` | `Feedback.target` | Python passes the id and weight to improve. TypeScript writes the Feedback object literal |

## FencedLeaseClient

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FencedLeaseClient::acquire` | `FencedLeaseClient.acquire` | `FencedLeaseClient.acquire` |  |
| `FencedLeaseClient::cas_fenced` | `FencedLeaseClient.cas_fenced` | `FencedLeaseClient.casFenced` |  |
| `FencedLeaseClient::close` | `close` | `close` |  |
| `FencedLeaseClient::connect_dedicated` | `FencedLeaseClient.connect_dedicated` | `FencedLeaseClient.connectDedicated` |  |
| `FencedLeaseClient::get` | `FencedLeaseClient.get` | `FencedLeaseClient.get` |  |
| `FencedLeaseClient::new` | `new FencedLeaseClient()` | `FencedLeaseClient.constructor` |  |
| `FencedLeaseClient::prepare_acquire` | `FencedLeaseClient.prepare_acquire` | `FencedLeaseClient.prepareAcquire` |  |
| `FencedLeaseClient::prepare_cas_fenced` | `FencedLeaseClient.prepare_cas_fenced` | `FencedLeaseClient.prepareCasFenced` |  |
| `FencedLeaseClient::prepare_release` | `FencedLeaseClient.prepare_release` | `FencedLeaseClient.prepareRelease` |  |
| `FencedLeaseClient::prepare_renew` | `FencedLeaseClient.prepare_renew` | `FencedLeaseClient.prepareRenew` |  |
| `FencedLeaseClient::release` | `FencedLeaseClient.release` | `FencedLeaseClient.release` |  |
| `FencedLeaseClient::renew` | `FencedLeaseClient.renew` | `FencedLeaseClient.renew` |  |
| `FencedLeaseClient::with_attempt_timeout` | `FencedLeaseClient.with_attempt_timeout` | `FencedLeaseClient.withAttemptTimeout` |  |

## FileStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FileStore::new` | `new FileStore()` | `FileStore.constructor` | constructor |

## FilterCaps

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FilterCaps::evaluates` | `Capabilities.evaluation` | `fn:filterCapsEvaluates` | Python compares the announced evaluator and codecs. TypeScript uses the free function |

## FilterPreviewBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FilterPreviewBuilder::explain` | `GroupFilter.preview(explain=)` | `FilterPreviewOptions.explain` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `FilterPreviewBuilder::from_offset` | `GroupFilter.preview(from_offset=)` | `FilterPreviewOptions.fromOffset` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `FilterPreviewBuilder::max_examined` | `GroupFilter.preview(max_examined=)` | `FilterPreviewOptions.maxExamined` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `FilterPreviewBuilder::max_records` | `GroupFilter.preview(max_records=)` | `FilterPreviewOptions.maxRecords` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |
| `FilterPreviewBuilder::send` | `GroupFilter.preview` | `GroupFilter.preview` | Python takes keyword arguments where Rust chains a builder. TypeScript passes an options object |

## FilteredReader

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FilteredReader::ack` | `ack` | `ack` |  |
| `FilteredReader::ack_page` | `ack_page` | `ackPage` |  |
| `FilteredReader::ack_through` | `ack_through` | `ackThrough` |  |
| `FilteredReader::close` | `close` | `close` |  |
| `FilteredReader::data_connections_opened` | `data_connections_opened` | `dataConnectionsOpened` |  |
| `FilteredReader::examined_in_round` | `examined_in_round` | `examinedInRound` |  |
| `FilteredReader::idle_interval` | `idle_interval` | `FilteredReader.idleIntervalMs` | TypeScript name |
| `FilteredReader::next_page` | `next_page` | `nextPage` |  |
| `FilteredReader::next_record` | `next_record` | `nextRecord` |  |
| `FilteredReader::owns` | `owns` | `owns` |  |
| `FilteredReader::partitions` | `partitions` | `partitions` |  |
| `FilteredReader::read_round` | `read_round` | `readRound` |  |
| `FilteredReader::try_next_page` | `try_next_page` | `tryNextPage` |  |

## FilteredReaderBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FilteredReaderBuilder::build` | `ConsumerGroup.reader` | `build` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::count` | `ConsumerGroup.reader(count=)` | `count` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::idle_interval` | `ConsumerGroup.reader(idle_interval=)` | `idleInterval` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::local_guard` | `ConsumerGroup.reader(local_guard=)` | `localGuard` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::max_examined` | `ConsumerGroup.reader(max_examined=)` | `maxExamined` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::max_reply_bytes` | `ConsumerGroup.reader(max_reply_bytes=)` | `maxReplyBytes` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::max_unacked_pages` | `ConsumerGroup.reader(max_unacked_pages=)` | `maxUnackedPages` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::partition` | `ConsumerGroup.reader(partitions=)` | `partition` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::read_mode` | `ConsumerGroup.reader(read_mode=)` | `readMode` | Python takes keyword arguments where Rust chains a builder |
| `FilteredReaderBuilder::start` | `ConsumerGroup.reader(start=, start_offset=, start_timestamp_micros=)` | `start` | Python takes keyword arguments where Rust chains a builder |

## Gather

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Gather::replies` | `AgentCtx.fan_out` | `fn:gatherReplies` | Python returns the replies in the fan_out result. TypeScript uses the free function |

## GovernorMode

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `GovernorMode::as_str` | `PolicyEvidence.mode` | omitted | Modes are plain str in Python. GovernorMode is a string literal union in TypeScript |

## GroupFilter

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `GroupFilter::configure` | `configure` | `configure` |  |
| `GroupFilter::configure_as` | `GroupFilter.configure(operation_id=)` | `configureAs` | Python takes keyword arguments where Rust chains a builder |
| `GroupFilter::configure_with` | `GroupFilter.configure(filter_id=, revision=)` | `configureWith` | Python takes keyword arguments where Rust chains a builder |
| `GroupFilter::delete` | `delete` | `delete` |  |
| `GroupFilter::get` | `get` | `get` |  |
| `GroupFilter::preview` | `preview` | `preview` |  |
| `GroupFilter::release` | `release` | `release` |  |
| `GroupFilter::revise` | `revise` | `revise` |  |
| `GroupFilter::revisions` | `revisions` | `revisions` |  |
| `GroupFilter::set_revision_enabled` | `set_revision_enabled` | `setRevisionEnabled` |  |
| `GroupFilter::test` | `test` | `test` |  |

## InMemoryStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `InMemoryStore::new` | `new InMemoryStore()` | `new InMemoryStore()` | constructor |

## InboxRoute

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `InboxRoute::resolve` | `AgentRegistry.inbox_for` | `fn:resolveInboxRoute` | Python resolves an advertised inbox through the registry. TypeScript uses the free function |

## IntentId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `IntentId::new` | `new Intent()` | `new` | Intent mints its own id |

## KeyRecord

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KeyRecord::agent` | `new KeyRecord()` | `agent` | The Python constructor defaults to the agent kind |
| `KeyRecord::from_verifying_bytes` | `KeyRecord.__new__(kind=)` | `KeyRecord.constructor` | constructor |
| `KeyRecord::key_id` | `key_id` | `keyId` |  |
| `KeyRecord::operator` | `KeyRecord.__new__(kind=)` | `operator` | constructor |
| `KeyRecord::revoked` | `revoked` | `revoked` |  |
| `KeyRecord::valid_window` | `KeyRecord.__new__(valid_from_micros=, valid_to_micros=)` | `validWindow` | constructor |

## KvKeyRegistry

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvKeyRegistry::enroll` | `enroll` | `enroll` |  |
| `KvKeyRegistry::enroll_record` | `KvKeyRegistry.enroll` | `enrollRecord` |  |
| `KvKeyRegistry::in_namespace` | `KvKeyRegistry.__new__(namespace=)` | `KvKeyRegistry.constructor` | constructor |
| `KvKeyRegistry::new` | `new KvKeyRegistry()` | `KvKeyRegistry.constructor` | constructor |
| `KvKeyRegistry::registry` | `registry` | `registry` |  |
| `KvKeyRegistry::revoke` | `revoke` | `revoke` |  |

## KvSnapshotStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `KvSnapshotStore::in_namespace` | `Laser.kv_snapshot_store(namespace=)` | `KvSnapshotStore.constructor` | Python takes keyword arguments where Rust chains a builder. constructor |
| `KvSnapshotStore::new` | `Laser.kv_snapshot_store` | `KvSnapshotStore.constructor` | constructor |

## LlmUsage

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LlmUsage::builder` | `new Provenance()` | `Provenance.usage` | Usage is part of provenance. TypeScript uses the LlmUsage object literal |

## LlmUsageBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LlmUsageBuilder::build` | `new Provenance()` | `Provenance.usage` | Usage is part of provenance. TypeScript uses the LlmUsage object literal |
| `LlmUsageBuilder::cost_usd` | `Provenance.__new__(cost_usd=)` | `LlmUsage.costUsd` | Python uses keywords. TypeScript uses a builder or object field |
| `LlmUsageBuilder::input_tokens` | `Provenance.__new__(input_tokens=)` | `LlmUsage.inputTokens` | Python uses keywords. TypeScript uses a builder or object field |
| `LlmUsageBuilder::output_tokens` | `Provenance.__new__(output_tokens=)` | `LlmUsage.outputTokens` | Python uses keywords. TypeScript uses a builder or object field |

## LocalAgentHandler

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalAgentHandler::handle` | `Laser.spawn_agent(handler=)` | `AgentHandler.handle` | Python accepts a callable or handle object |

## LocalConsolidator

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalConsolidator::consolidate` | `Laser.spawn_agent(consolidator=)` | `Consolidator.consolidate` | Python accepts a callable or consolidate object |

## LocalEmbedder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalEmbedder::embed` | `Memory.vector(embedder=)` | `Embedder.embed` | Python accepts a callable |

## LocalManagedKvTransport

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalManagedKvTransport::close` | `DedicatedKvTransport.close` | `ManagedKvTransport.close` | Custom Python transports also supply these methods to FencedLeaseClient |
| `LocalManagedKvTransport::ready` | `DedicatedKvTransport.ready` | `ManagedKvTransport.ready` | Custom Python transports also supply these methods to FencedLeaseClient |
| `LocalManagedKvTransport::reset` | `DedicatedKvTransport.reset` | `ManagedKvTransport.reset` | Custom Python transports also supply these methods to FencedLeaseClient |
| `LocalManagedKvTransport::send` | `DedicatedKvTransport.send` | `ManagedKvTransport.send` | Custom Python transports also supply these methods to FencedLeaseClient |

## LocalMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalMemory::append` | `Memory.append` | `MemoryHandle.append` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `LocalMemory::forget` | `Memory.forget` | `MemoryHandle.forget` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `LocalMemory::improve` | `Memory.improve` | `MemoryHandle.improve` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `LocalMemory::recall` | `Memory.recall` | `MemoryHandle.recall` | Python uses scope keywords. Local and boxed Rust traits share the same binding |
| `LocalMemory::remember` | `Memory.remember` | `MemoryHandle.remember` | Python uses scope keywords. Local and boxed Rust traits share the same binding |

## LocalReranker

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalReranker::rerank` | `Memory.reranker(reranker=)` | `Reranker.rerank` | Python accepts a callable |

## LocalSnapshotStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalSnapshotStore::latest` | `SnapshotStore.latest` | `SnapshotStore.latest` | Python state_with also accepts a custom latest/save object |
| `LocalSnapshotStore::save` | `SnapshotStore.save` | `SnapshotStore.save` | Python state_with also accepts a custom latest/save object |

## LocalStateStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalStateStore::delete` | `InMemoryStore.delete` | `StateStore.delete` | Python built-in stores share the same methods |
| `LocalStateStore::get` | `InMemoryStore.get` | `StateStore.get` | Python built-in stores share the same methods |
| `LocalStateStore::set` | `InMemoryStore.set` | `StateStore.set` | Python built-in stores share the same methods |

## LocalSummarizer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LocalSummarizer::summarize` | `Memory.consolidate(summarizer=)` | `Summarizer.summarize` | Python accepts a callable |

## LogMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LogMemory::fetch_named` | `Memory.fetch` | `LogMemory.fetch` |  |
| `LogMemory::fetch_named_folded` | `Memory.fetch_folded` | `LogMemory.fetchFolded` |  |
| `LogMemory::forget_named` | `Memory.remove` | `LogMemory.remove` |  |
| `LogMemory::in_namespace` | `Laser.memory` | `LogMemory.constructor` | constructor |
| `LogMemory::new` | `Laser.memory` | `LogMemory.constructor` | Python always names the namespace. constructor |
| `LogMemory::on_stream_topic` | `Laser.memory_on_topic(stream=)` | `LogMemory.constructor` | Python takes keyword arguments where Rust chains a builder. constructor |
| `LogMemory::on_stream_topic_named` | `Laser.memory_on_topic(stream=)` | `LogMemory.constructor` | Python takes keyword arguments where Rust chains a builder. constructor |
| `LogMemory::on_topic` | `Laser.memory_on_topic` | `LogMemory.constructor` | constructor |
| `LogMemory::on_topic_named` | `Laser.memory_on_topic` | `LogMemory.constructor` | constructor |
| `LogMemory::recall_folded` | `Memory.recall(folded=)` | `recallFolded` | Python takes keyword arguments where Rust chains a builder |
| `LogMemory::set_named` | `Memory.set` | `LogMemory.set` |  |
| `LogMemory::update_named` | `Memory.update` | `LogMemory.update` |  |

## ManagedKvTransport

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ManagedKvTransport::close` | `DedicatedKvTransport.close` | `ManagedKvTransport.close` | Custom Python transports also supply these methods to FencedLeaseClient |
| `ManagedKvTransport::ready` | `DedicatedKvTransport.ready` | `ManagedKvTransport.ready` | Custom Python transports also supply these methods to FencedLeaseClient |
| `ManagedKvTransport::reset` | `DedicatedKvTransport.reset` | `ManagedKvTransport.reset` | Custom Python transports also supply these methods to FencedLeaseClient |
| `ManagedKvTransport::send` | `DedicatedKvTransport.send` | `ManagedKvTransport.send` | Custom Python transports also supply these methods to FencedLeaseClient |

## MatchedRecord

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MatchedRecord::headers_malformed` | `headers_malformed` | `headersMalformed` |  |
| `MatchedRecord::json` | `ConsumerMessage.json` | `json` | Python reads it through MatchedRecord.message |

## MemoryHandler

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryHandler::auto_remember` | `MemoryHandler.auto_remember` | `autoRemember` |  |
| `MemoryHandler::new` | `new MemoryHandler()` | `MemoryHandler.constructor` | constructor |

## MemoryId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryId::as_u128` | `MemoryItem.id` | `asU128` | Memory ids are plain str in Python |
| `MemoryId::content` | `Memory.content_id` | `content` |  |
| `MemoryId::from_u128` | `Memory.forget` | `fromU128` | Memory ids are plain str in Python |
| `MemoryId::new` | omitted | `new` | Memory.remember returns the minted id |

## MemoryKind

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryKind::class` | `Memory.kind_class` | `fn:memoryClass` | TypeScript uses the free function |
| `MemoryKind::code` | omitted | omitted | The byte only feeds the content id hash |
| `MemoryKind::from_word` | `Memory.remember(kind=)` | omitted | Kinds are plain str in Python. MemoryKind is a string literal union in TypeScript |

## MemoryQuery

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryQuery::builder` | `Memory.recall` | `MemoryQuery.limit` | Python passes query keywords. TypeScript uses the MemoryQuery object literal |

## MemoryQueryBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryQueryBuilder::agent` | `Memory.recall(agent=)` | `MemoryQuery.agent` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryQueryBuilder::build` | `Memory.recall` | `MemoryQuery.limit` | Python passes query keywords. TypeScript uses the MemoryQuery object literal |
| `MemoryQueryBuilder::limit` | `Memory.recall(limit=)` | `MemoryQuery.limit` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryQueryBuilder::semantic` | `Memory.recall(semantic=)` | `MemoryQuery.semantic` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryQueryBuilder::strategy` | `Memory.recall(strategy=)` | `MemoryQuery.strategy` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryQueryBuilder::token_budget` | `Memory.recall(token_budget=)` | `MemoryQuery.tokenBudget` | Python uses keywords. TypeScript uses a builder or object field |

## MemoryScope

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryScope::builder` | `Memory.remember` | `MemoryScope.conversation` | Python passes scope keywords or a dictionary. TypeScript uses the MemoryScope object literal |

## MemoryScopeBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryScopeBuilder::agent` | `Memory.remember(agent=)` | `MemoryScope.agent` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryScopeBuilder::app` | `Memory.remember(application=)` | `MemoryScope.application` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryScopeBuilder::build` | `Memory.remember` | `MemoryScope.conversation` | Python passes scope keywords or a dictionary. TypeScript uses the MemoryScope object literal |
| `MemoryScopeBuilder::conversation` | `Memory.remember(conversation=)` | `MemoryScope.conversation` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryScopeBuilder::lifetime` | `Memory.remember(durable=)` | `MemoryScope.lifetime` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryScopeBuilder::stream` | `Memory.remember(stream=)` | `MemoryScope.stream` | Python uses keywords. TypeScript uses a builder or object field |
| `MemoryScopeBuilder::user` | `Memory.remember(user=)` | `MemoryScope.user` | Python uses keywords. TypeScript uses a builder or object field |

## MemoryTopicBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MemoryTopicBuilder::build` | `Laser.memory_topic` | `build` | Python takes keyword arguments where Rust chains a builder |
| `MemoryTopicBuilder::no_expiry` | `Laser.memory_topic(ttl_secs=)` | `noExpiry` | A ttl_secs of zero or less disables expiry |
| `MemoryTopicBuilder::partitions` | `Laser.memory_topic(partitions=)` | `partitions` | Python takes keyword arguments where Rust chains a builder |
| `MemoryTopicBuilder::stream` | `Laser.memory_topic(stream=)` | `stream` | Python takes keyword arguments where Rust chains a builder |
| `MemoryTopicBuilder::ttl` | `Laser.memory_topic(ttl_secs=)` | `ttl` | Python takes keyword arguments where Rust chains a builder |

## MessageId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MessageId::new` | `new Provenance()` | `MessageId.offset` | Python passes the partition:offset str as causal_parent. TypeScript writes the MessageId object literal |

## MintUlid

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `MintUlid::mint` | `fn:new_conversation_id` | `ConversationId.new` | Python mints plain ULID strings. TypeScript uses branded IDs |

## PolicyEvidence

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `PolicyEvidence::decode` | `decode` | `fn:decodePolicyEvidence` | TypeScript uses the free function |
| `PolicyEvidence::encode` | `encode` | `fn:encodePolicyEvidence` | TypeScript uses the free function |

## PreparedMutation

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `PreparedMutation::ambiguous_recovery` | `PreparedMutation.ambiguous_recovery` | `PreparedMutation.ambiguousRecovery` |  |
| `PreparedMutation::operation_id` | `PreparedMutation.operation_id` | `PreparedMutation.operationId` |  |

## PrincipalId

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `PrincipalId::get` | `AgentRegistry.principal_for` | `get` | Principals are plain int in Python |
| `PrincipalId::new` | `Laser.contract(principal=)` | `new` | Principals are plain int in Python |

## ProducerMessage

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProducerMessage::builder` | `Producer.send` | `ProducerMessage.payload` | Python passes payload and headers to send. TypeScript uses the ProducerMessage object literal |
| `ProducerMessage::header` | `Producer.send(headers=)` | `ProducerMessage.headers` | Python takes keyword arguments where Rust chains a builder. TypeScript writes the ProducerMessage object literal |
| `ProducerMessage::new` | `Producer.send` | `ProducerMessage.payload` | Python passes the payload to send. TypeScript writes the ProducerMessage object literal |
| `ProducerMessage::with_headers` | `Producer.send(headers=)` | `ProducerMessage.headers` | Python takes keyword arguments where Rust chains a builder. TypeScript writes the ProducerMessage object literal |

## ProducerMessageBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProducerMessageBuilder::build` | `Producer.send` | `ProducerMessage.payload` | Python passes payload and headers to send. TypeScript uses the ProducerMessage object literal |
| `ProducerMessageBuilder::headers` | `Producer.send(headers=)` | `ProducerMessage.headers` | Python uses keywords. TypeScript uses a builder or object field |
| `ProducerMessageBuilder::payload` | `Producer.send(payload=)` | `ProducerMessage.payload` | Python uses keywords. TypeScript uses a builder or object field |

## ProducerObservation

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProducerObservation::finish` | omitted | omitted | Send instrumentation the SDK producers drive. A caller records nothing by hand |

## ProducerRecorder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProducerRecorder::begin` | omitted | omitted | Send instrumentation the SDK producers drive. A caller records nothing by hand |
| `ProducerRecorder::new` | omitted | omitted | Send instrumentation the SDK producers drive. A caller records nothing by hand |
| `ProducerRecorder::snapshot` | omitted | omitted | Send instrumentation the SDK producers drive. A caller records nothing by hand |

## ProjectionsRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProjectionsRequest::fetch` | `Laser.list_projections` | `fetch` | Python takes keyword arguments where Rust chains a builder |
| `ProjectionsRequest::for_topic` | `Laser.list_projections(topic=)` | `forTopic` | Python takes keyword arguments where Rust chains a builder |
| `ProjectionsRequest::for_topics` | `Laser.list_projections(topics=)` | `forTopics` | Python takes keyword arguments where Rust chains a builder |
| `ProjectionsRequest::id_prefix` | `Laser.list_projections(id_prefix=)` | `idPrefix` | Python takes keyword arguments where Rust chains a builder |
| `ProjectionsRequest::name_contains` | `Laser.list_projections(name_contains=)` | `nameContains` | Python takes keyword arguments where Rust chains a builder |
| `ProjectionsRequest::search` | `Laser.list_projections(search=)` | `search` | Python takes keyword arguments where Rust chains a builder |

## Provenance

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Provenance::builder` | `new Provenance()` | `Provenance.conversationId` | Python constructs provenance. TypeScript uses the Provenance object literal |
| `Provenance::partition_key` | `Provenance.conversation_id` | `fn:provenancePartitionKey` | The partition key is the conversation id. TypeScript uses the free function |

## ProvenanceBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ProvenanceBuilder::agent` | `Provenance.__new__(agent=)` | `Provenance.agent` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::build` | `new Provenance()` | `Provenance.conversationId` | Python constructs provenance. TypeScript uses the Provenance object literal |
| `ProvenanceBuilder::causal_parent` | `Provenance.__new__(causal_parent=)` | `Provenance.causalParent` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::conversation_id` | `Provenance.__new__(conversation_id=)` | `Provenance.conversationId` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::correlation_id` | `Provenance.__new__(correlation_id=)` | `Provenance.correlationId` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::deadline` | `Provenance.__new__(deadline_micros=)` | `Provenance.deadlineMicros` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::fence_token` | `Provenance.__new__(fence_token=)` | `Provenance.fenceToken` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::idempotency_key` | `Provenance.__new__(idempotency_key=)` | `Provenance.idempotencyKey` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::parent_conversation_id` | `Provenance.__new__(parent_conversation_id=)` | `Provenance.parentConversationId` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::root_conversation_id` | `Provenance.__new__(root_conversation_id=)` | `Provenance.rootConversationId` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::target_agent_id` | `Provenance.__new__(target_agent_id=)` | `Provenance.targetAgentId` | Python uses keywords. TypeScript uses a builder or object field |
| `ProvenanceBuilder::usage` | `Provenance.__new__(input_tokens=, output_tokens=, cost_usd=)` | `Provenance.usage` | Python uses keywords. TypeScript uses a builder or object field |

## Record

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Record::builder` | `BatchPublishRequest.add_record` | `new Record()` | Python supplies per-record metadata. TypeScript constructs Record |

## RecordBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RecordBuilder::build` | `BatchPublishRequest.add_record` | `new Record()` | Python supplies per-record metadata. TypeScript constructs Record |
| `RecordBuilder::content_type` | `BatchPublishRequest.add_record(content_type=)` | `Record.contentType` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::index` | `BatchPublishRequest.add_record(index=)` | `Record.index` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::inline_payload` | `BatchPublishRequest.add_record(inline_payload=)` | `Record.inlinePayload` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::metadata` | `BatchPublishRequest.add_record(headers=)` | `Record.header` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::projection_ref` | `BatchPublishRequest.add_record(projection_ref=)` | `Record.projectionRef` | Python supplies per-record metadata. TypeScript uses Record |
| `RecordBuilder::schema_id` | `BatchPublishRequest.add_record(schema_id=)` | `Record.schemaId` | Python supplies per-record metadata. TypeScript uses Record |

## RegisterSchemaRequest

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RegisterSchemaRequest::name` | `Laser.register_schema(name=)` | `name` | Python takes keyword arguments where Rust chains a builder |
| `RegisterSchemaRequest::send` | `Laser.register_schema` | `send` | Python takes keyword arguments where Rust chains a builder |
| `RegisterSchemaRequest::version` | `Laser.register_schema(version=)` | `version` | Python takes keyword arguments where Rust chains a builder |

## RegisteredCard

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RegisteredCard::available_for` | `AgentRegistry.card_available_for` | `fn:cardAvailableFor` | TypeScript uses the free function |
| `RegisteredCard::is_fresh` | `AgentRegistry.card_is_fresh` | `fn:cardIsFresh` | TypeScript uses the free function |
| `RegisteredCard::serves` | `AgentRegistry.card_serves` | `fn:cardServes` | TypeScript uses the free function |

## ReliableConsumer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ReliableConsumer::builder` | `Laser.spawn_agent` | `ReliableConsumer.constructor` | Python starts the configured consumer. TypeScript passes an options object |
| `ReliableConsumer::run` | `Laser.spawn_agent` | `run` | spawn_agent runs the reliable consumer |

## ReliableConsumerBuilder

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ReliableConsumerBuilder::ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `ReliableConsumerOptions.ackOnPickup` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::agent` | `Laser.spawn_agent(agent_id=)` | `ReliableConsumerOptions.agent` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::build` | `Laser.spawn_agent` | `ReliableConsumer.constructor` | Python starts the configured consumer. TypeScript constructs the consumer |
| `ReliableConsumerBuilder::concurrency` | `Laser.spawn_agent(max_partitions=)` | `ReliableConsumerOptions.concurrency` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::dedup_window` | `Laser.spawn_agent(dedup_window=)` | `ReliableConsumerOptions.dedupWindow` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::deduplicator` | `Laser.spawn_agent(dedup=)` | `ReliableConsumerOptions.deduplicator` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::group` | `Laser.spawn_agent(consumer_group=)` | `ReliableConsumerOptions.group` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `ReliableConsumerOptions.inboxRoute` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `ReliableConsumerOptions.maxQueuedBytes` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `ReliableConsumerOptions.maxQueuedRecords` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::middleware` | `Laser.spawn_agent(middleware=)` | `ReliableConsumerOptions.middleware` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `ReliableConsumerOptions.deadLetterSink` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `ReliableConsumerOptions.pollIntervalMs` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::respond_on` | `Laser.spawn_agent(respond_on=)` | `ReliableConsumerOptions.respondOn` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `ReliableConsumerOptions.retry` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `ReliableConsumerOptions.shutdownGraceMs` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::signing_key` | `Laser.spawn_agent(signing_key=)` | `ReliableConsumerOptions.signingKey` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::topic` | `Laser.spawn_agent(listen_on=)` | `ReliableConsumerOptions.topic` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::understood_features` | `Laser.spawn_agent(understood_features=)` | `ReliableConsumerOptions.understoodFeatures` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::verifier` | `Laser.spawn_agent(verifier=)` | `ReliableConsumerOptions.verifier` | Python uses keywords. TypeScript uses a builder or object field |
| `ReliableConsumerBuilder::warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `ReliableConsumerOptions.warmDedup` | Python uses keywords. TypeScript uses a builder or object field |

## RerankedMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RerankedMemory::new` | `Memory.reranker` | `MemoryHandle.reranker` |  |

## Reranker

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Reranker::rerank` | `Memory.reranker(reranker=)` | `Reranker.rerank` | Python accepts a callable |

## RetryPolicy

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RetryPolicy::backoff` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `fn:retryBackoff` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |

## RouteScorer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RouteScorer::select` | `Laser.contract(policy=)` | `RouteScorer.select` | Python accepts a synchronous callable or select object |

## Router

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Router::all_capable` | `Workflow.step(all_capable=)` | `fn:routeAllCapable` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |
| `Router::apply` | `new Provenance()` | `fn:applyRoute` | Python stamps the target with target_agent_id. TypeScript uses the free function |
| `Router::broadcast` | `new Provenance()` | `fn:routeBroadcast` | Leave target_agent_id unset. TypeScript uses the free function |
| `Router::resolve_targets` | `AgentRegistry.resolve(now_micros=)` | `fn:resolveTargets` | TypeScript uses the free function |
| `Router::to` | `Workflow.step(to=)` | `fn:routeTo` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |
| `Router::to_capable` | `Workflow.step(to_capable=)` | `fn:routeToCapable` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |
| `Router::to_principal` | `Workflow.step(to=, principal=)` | `fn:routeToPrincipal` | Python takes keyword arguments where Rust chains a builder. TypeScript uses the free function |

## Routing

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Routing::key` | `Topic.producer(key=)` | omitted | Python takes keyword arguments where Rust chains a builder. Routing is a tagged union object in TypeScript |

## ScatterReport

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ScatterReport::completed` | `Laser.scatter_report` | `completed` | Python returns dicts with a state field |
| `ScatterReport::failures` | `Laser.scatter_report` | `failures` | Python returns dicts with a state field |

## SessionPolicy

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SessionPolicy::conversation_for` | `fn:derive_conversation_id` | `fn:conversationFor` | PerUser derives the id. PerCall uses new_conversation_id. TypeScript uses the free function |

## SessionTurnKind

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SessionTurnKind::for_topic` | `Sessions.turn_kind` | `fn:sessionTurnKind` | TypeScript uses the free function |
| `SessionTurnKind::topic` | `Sessions.turn_topic` | `fn:sessionTurnTopic` | TypeScript uses the free function |

## SharedConsolidator

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SharedConsolidator::new` | `Laser.spawn_agent(consolidator=)` | omitted | Python takes the runtime consolidator callback. Rust wrapper for shared ownership. TypeScript shares the object |

## SlidingWindow

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SlidingWindow::new` | `Laser.spawn_agent(dedup_window=)` | `SlidingWindow.constructor` | Python takes keyword arguments where Rust chains a builder. constructor |

## SnapshotStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `SnapshotStore::latest` | `SnapshotStore.latest` | `SnapshotStore.latest` | Python state_with also accepts a custom latest/save object |
| `SnapshotStore::save` | `SnapshotStore.save` | `SnapshotStore.save` | Python state_with also accepts a custom latest/save object |

## StateStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `StateStore::delete` | `InMemoryStore.delete` | `StateStore.delete` | Python built-in stores share the same methods |
| `StateStore::get` | `InMemoryStore.get` | `StateStore.get` | Python built-in stores share the same methods |
| `StateStore::set` | `InMemoryStore.set` | `StateStore.set` | Python built-in stores share the same methods |

## StepFn

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `StepFn::build` | `Workflow.step(build=)` | `Workflow.step` | Python and TypeScript take synchronous or async callbacks |

## Summarizer

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Summarizer::summarize` | `Memory.consolidate(summarizer=)` | `Summarizer.summarize` | Python accepts a callable |

## TestClock

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TestClock::advance` | `TestClock.advance` | `advance` |  |
| `TestClock::new` | `new TestClock()` | `TestClock.constructor` | constructor |
| `TestClock::set` | `TestClock.set` | `set` |  |

## TopicSnapshotStore

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TopicSnapshotStore::new` | `Laser.topic_snapshot_store` | `TopicSnapshotStore.constructor` | constructor |
| `TopicSnapshotStore::on_topic` | `Laser.topic_snapshot_store(topic=)` | `TopicSnapshotStore.constructor` | Python takes keyword arguments where Rust chains a builder. constructor |

## TypedQueryRows

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TypedQueryRows::next` | `QueryRequest.rows_typed` | `QueryRequest.rowsTyped` | Python returns the bounded list. TypeScript iterates the rows |

## TypedRecords

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TypedRecords::batch` | `Topic.records(batch=)` | `batch` | Python takes keyword arguments where Rust chains a builder |
| `TypedRecords::from_offsets` | `Topic.records(from_offsets=)` | `fromOffsets` | Python takes keyword arguments where Rust chains a builder |
| `TypedRecords::next` | `next` | `TypedRecords.stream` | TypeScript iterates with stream |
| `TypedRecords::offsets` | `offsets` | `offsets` |  |
| `TypedRecords::poll` | `TypedRecords.next` | `poll` | Python yields one record per call |
| `TypedRecords::stream` | `TypedRecords.__aiter__` | `stream` | Python iterates with async for |

## VectorMemory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `VectorMemory::governed` | `Laser.vector_memory` | `governed` |  |
| `VectorMemory::new` | `Memory.vector` | `VectorMemory.constructor` | constructor |

## Verdict

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Verdict::as_str` | `PolicyEvidence.decision` | omitted | Verdicts are plain str in Python. Verdict is a tagged union in TypeScript and its kind holds the word |

## Verifier

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Verifier::verify` | `Workflow.step(verify=)` | `StepBuilder.verifyWith` | Python and TypeScript take synchronous or async callbacks |

## a2a

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `a2a::command_from_message_send` | `fn:command_from_message_send` | `fn:commandFromMessageSend` |  |
| `a2a::enter_bridge` | `enter_bridge` | `enterBridge` |  |
| `a2a::task_from_envelope` | `fn:task_from_envelope` | `fn:taskFromEnvelope` |  |

## agent::state

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `agent::state::resume_offsets` | `fn:resume_offsets` | `fn:resumeOffsets` |  |

## blob

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `blob::check_in` | `fn:check_in` | `fn:checkIn` |  |
| `blob::resolve_body` | `fn:resolve_body` | `fn:resolveBody` |  |

## context

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `context::checkpoint` | `ContextScope.checkpoint` | `ContextScope.checkpoint` | The scope retains the connection and conversation |

## edge_auth

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `edge_auth::authorize_edge` | `authorize_edge` | `authorizeEdge` |  |

## intent

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `intent::decide` | `decide` | `decide` |  |

## mcp

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `mcp::tool_call_from_request` | `fn:tool_call_from_request` | `fn:toolCallFromRequest` |  |
| `mcp::tool_result_from_envelope` | `fn:tool_result_from_envelope` | `fn:toolResultFromEnvelope` |  |

## memory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `memory::fuse_reciprocal_rank` | `fn:fuse_reciprocal_rank` | `fn:fuseReciprocalRank` |  |
| `memory::to_context_block` | `Memory.to_context_block` | `fn:toContextBlock` | Python exposes a static helper |

## sign

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `sign::sign_card_value` | `fn:sign_card_value` | `fn:signCardValue` |  |
| `sign::verify_card` | `fn:verify_card` | `fn:verifyCard` |  |
| `sign::verify_delegation` | `fn:verify_delegation` | `fn:verifyDelegation` |  |

## snapshot

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `snapshot::decode` | `fn:decode_snapshot` | `fn:decodeSnapshot` |  |
| `snapshot::encode` | `fn:encode_snapshot` | `fn:encodeSnapshot` |  |

## testing

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `testing::agent_ctx` | `fn:agent_ctx` | `fn:agentContext` | Python and TypeScript take callback context options |
| `testing::agent_message` | `fn:agent_message` | `fn:agentMessage` |  |
