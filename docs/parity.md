# Rust, Python, and TypeScript parity

This matrix lists every public item of the Rust `laser_sdk` crate, including the items it re-exports from `laser_wire` and the types its public methods hand out, with its Python and TypeScript spelling. Types, struct fields, enum variants, LaserError variants and their payload fields, methods, generated builders and their setters, associated constants, free functions, and module constants each get a row. `scripts/check-parity.py --write` generates it from the Rust sources, the Python stub, and the TypeScript API reports. `just parity-check` fails when the committed file is stale, when a row says MISSING, when parameters differ, or when a Python or TypeScript API has no Rust row. Python uses snake_case and TypeScript uses camelCase for the same name.

A peer may differ from the Rust spelling only for one of the reasons below. Any other difference is a gap. A MISSING cell names where the capability lives today when the peer has it under another name or shape.

| Note | Reason |
| --- | --- |
| native-binding | Python exposes a native Rust value through its binding name and delegates the operation to that same Rust type |
| keywords | Python takes keyword arguments and TypeScript an options object or builder where Rust fills a struct or chains a builder |
| one-call | The peer configures and runs the operation in one call where Rust finishes a builder |
| constructor | A Rust constructor function or struct literal is the peer class constructor |
| protocol | The peer spells it through a language protocol: iteration, length, or truthiness |
| overload | Rust has no optional parameters or overloads, so it spells a variant of a call as a second method. The peer takes an optional or alternative parameter |
| free-function | The peer type is a plain value, object literal, or union, so its methods live in functions |
| async-runtime | Rust passes tokio channels or futures where the peer uses its own async primitives |
| callback | The peer passes a callable or duck-typed object where Rust implements a trait |
| trait-variant | Rust spells one trait as Send, local, and boxed variants. The peer has one interface |
| error-function | The peer exposes the Rust error classification method as a function over its exception classes |
| error-class | The peer raises one error class per LaserError variant, carrying the payload fields |
| property | The peer reads a property where Rust calls a getter |
| plain-value | The peer uses a plain string, number, or literal union where Rust wraps a newtype or word enum |
| shared-ownership | Rust wraps a value to share it across tasks. Peer references are already shared |
| name-collision | The peer keeps fields and methods in one namespace, so a method that shares a field's name is renamed |
| lazy-init | The peer builds a client synchronously and connects in an explicit `init()` where Rust awaits the builder |
| telemetry | Each client reports through its language's telemetry seam: Rust tracing spans, Python logging, a TypeScript observer |
| flat-namespace | The peer exports one flat namespace, so a Rust module function carries its module name |
| rust-crate | The item exposes a Rust-only crate (the Apache Iggy SDK, axum, tokio) that has no binding outside Rust |
| converted-error | A Rust error type the SDK converts into a LaserError variant before any caller sees it. The peer raises that variant's error class |
| native-value | Python passes the native scalar, None, or list that serde reads and writes for this untagged Rust enum |
| std-trait | The peer spells a Rust standard trait impl (FromStr, Display, From) as a function or static member |
| serde-dict | Python passes the record as a dict that serde builds from the same Rust type, keyed by its serde field names |

## laser_sdk

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `LaserError` | `LaserError` | `LaserError` |  |
| `LaserError::PublishFailed` | `PublishFailedError` | `PublishFailedError` | error-class |
| `LaserError::Handler` | `HandlerError` | `HandlerError` | error-class |
| `LaserError::HandlerConfig` | `HandlerConfigError` | `HandlerConfigError` | error-class |
| `LaserError::Rejected` | `RejectedError` | `RejectedError` | error-class |
| `LaserError::Timeout` | `TimeoutError` | `TimeoutError` | error-class |
| `LaserError::AmbiguousMutation` | `AmbiguousMutationError` | `AmbiguousMutationError` | error-class |
| `LaserError::NoRespondTopic` | `NoRespondTopicError` | `NoRespondTopicError` | error-class |
| `LaserError::StateStore` | `StateStoreError` | `StateStoreError` | error-class |
| `LaserError::Config` | `ConfigError` | `ConfigError` | error-class |
| `LaserError::NoStream` | `NoStreamError` | `NoStreamError` | error-class |
| `LaserError::Query` | `QueryError` | `QueryExecutionError` | error-class |
| `LaserError::Kv` | `KvError` | `KvExecutionError` | error-class |
| `LaserError::Fork` | `ForkError` | `ForkExecutionError` | error-class |
| `LaserError::Graph` | `GraphError` | `GraphExecutionError` | error-class |
| `LaserError::Agent` | `AgentError` | `AgentWorkflowExecutionError` | error-class |
| `LaserError::Authz` | `AuthzError` | `AuthzExecutionError` | error-class |
| `LaserError::Filter` | `FilterError` | `FilterExecutionError` | error-class |
| `LaserError::FilterFault` | `FilterFaultError` | `FilterFaultError` | error-class |
| `LaserError.FilterFault.partition_id` | `FilterFaultError.partition_id` | `FilterFaultError.partitionId` | error-class |
| `LaserError.FilterFault.offset` | `FilterFaultError.offset` | `FilterFaultError.offset` | error-class |
| `LaserError.FilterFault.reason` | `FilterFaultError.reason` | `FilterFaultError.reason` | error-class |
| `LaserError::FilterOversizedRecord` | `FilterOversizedRecordError` | `FilterOversizedRecordError` | error-class |
| `LaserError.FilterOversizedRecord.partition_id` | `FilterOversizedRecordError.partition_id` | `FilterOversizedRecordError.partitionId` | error-class |
| `LaserError.FilterOversizedRecord.offset` | `FilterOversizedRecordError.offset` | `FilterOversizedRecordError.offset` | error-class |
| `LaserError::ConsumerGroupSetup` | `ConsumerGroupSetupError` | `ConsumerGroupSetupError` | error-class |
| `LaserError.ConsumerGroupSetup.group_id` | `ConsumerGroupSetupError.group_id` | `ConsumerGroupSetupError.groupId` | error-class |
| `LaserError.ConsumerGroupSetup.name` | `ConsumerGroupSetupError.group_name` | `ConsumerGroupSetupError.groupName` | name-collision |
| `LaserError.ConsumerGroupSetup.identity` | `ConsumerGroupSetupError.identity` | `ConsumerGroupSetupError.identity` | error-class |
| `LaserError.ConsumerGroupSetup.source` | `ConsumerGroupSetupError` | `ConsumerGroupSetupError` | protocol |
| `LaserError::Checkpoint` | `CheckpointError` | `CheckpointExecutionError` | error-class |
| `LaserError::Codec` | `CodecError` | `CodecError` | error-class |
| `LaserError::Invalid` | `InvalidError` | `InvalidError` | error-class |
| `LaserError::Protocol` | `ProtocolError` | `ProtocolError` | error-class |
| `LaserError::Unsupported` | `UnsupportedError` | `UnsupportedError` | error-class |
| `LaserError.Unsupported.surface` | `UnsupportedError.surface` | `UnsupportedError.surface` | error-class |
| `LaserError.Unsupported.feature` | `UnsupportedError.feature` | `UnsupportedError.feature` | error-class |
| `LaserError.Unsupported.message` | `UnsupportedError` | `UnsupportedError` | protocol |
| `LaserError::Integrity` | `IntegrityError` | `IntegrityError` | error-class |
| `LaserError.Integrity.reference` | `IntegrityError.reference` | `IntegrityError.reference` | error-class |
| `LaserError::Signature` | `SignatureError` | `SignatureError` | error-class |
| `LaserError::PolicyBlocked` | `PolicyBlockedError` | `PolicyBlockedError` | error-class |
| `LaserError::StepUpRequired` | `StepUpRequiredError` | `StepUpRequiredError` | error-class |
| `LaserError.StepUpRequired.scope` | `StepUpRequiredError.scope` | `StepUpRequiredError.scope` | error-class |
| `LaserError::PolicyDeferred` | `PolicyDeferredError` | `PolicyDeferredError` | error-class |
| `LaserError::NoCapableAgent` | `NoCapableAgentError` | `NoCapableAgentError` | error-class |
| `LaserError.NoCapableAgent.skill` | `NoCapableAgentError.skill` | `NoCapableAgentError.skill` | error-class |
| `LaserError::NoInbox` | `NoInboxError` | `NoInboxError` | error-class |
| `LaserError.NoInbox.agent` | `NoInboxError.agent` | `NoInboxError.agent` | error-class |
| `LaserError::PresenceConflict` | `PresenceConflictError` | `PresenceConflictError` | error-class |
| `LaserError.PresenceConflict.advertised` | `PresenceConflictError.advertised` | `PresenceConflictError.advertised` | error-class |
| `LaserError.PresenceConflict.requested` | `PresenceConflictError.requested` | `PresenceConflictError.requested` | error-class |
| `LaserError::RoutePrincipalMismatch` | `RoutePrincipalMismatchError` | `RoutePrincipalMismatchError` | error-class |
| `LaserError.RoutePrincipalMismatch.agent` | `RoutePrincipalMismatchError.agent` | `RoutePrincipalMismatchError.agent` | error-class |
| `LaserError.RoutePrincipalMismatch.expected` | `RoutePrincipalMismatchError.expected` | `RoutePrincipalMismatchError.expected` | error-class |
| `LaserError.RoutePrincipalMismatch.actual` | `RoutePrincipalMismatchError.actual` | `RoutePrincipalMismatchError.actual` | error-class |
| `LaserError::FenceViolation` | `FenceViolationError` | `FenceViolationError` | error-class |
| `LaserError.FenceViolation.stale` | `FenceViolationError.stale` | `FenceViolationError.stale` | error-class |
| `LaserError.FenceViolation.current` | `FenceViolationError.current` | `FenceViolationError.current` | error-class |
| `LaserError::BudgetExceeded` | `BudgetExceededError` | `BudgetExceededError` | error-class |
| `LaserError.BudgetExceeded.ceiling` | `BudgetExceededError.ceiling` | `BudgetExceededError.ceiling` | error-class |
| `LaserError.BudgetExceeded.spent` | `BudgetExceededError.spent` | `BudgetExceededError.spent` | error-class |
| `LaserError::Cancelled` | `CancelledError` | `CancelledError` | error-class |
| `LaserError.Cancelled.run` | `CancelledError.run` | `CancelledError.run` | error-class |
| `LaserError::Quarantined` | `QuarantinedError` | `QuarantinedError` | error-class |
| `LaserError.Quarantined.agent` | `QuarantinedError.agent` | `QuarantinedError.agent` | error-class |
| `LaserError::Iggy` | `TransportError` | `TransportError` | error-class |
| `LaserError::Id` | `IdError` | `IdError` | error-class |
| `LaserError::Provenance` | `ProvenanceError` | `ProvenanceError` | error-class |
| `LaserError::code` | `LaserError.code` | `fn:code` | property, error-function |
| `LaserError::filter_reason` | `LaserError.filter_reason` | `fn:filterReason` | property, error-function |
| `LaserError::iggy_error_code` | `LaserError.iggy_error_code` | `fn:iggyErrorCode` | property, error-function |
| `LaserError::is_ambiguous_mutation` | `LaserError.ambiguous_mutation` | `fn:isAmbiguousMutation` | property, error-function |
| `LaserError::is_budget_exceeded` | `LaserError.budget_exceeded` | `fn:isBudgetExceeded` | property, error-function |
| `LaserError::is_fence_violation` | `LaserError.fence_violation` | `fn:isFenceViolation` | property, error-function |
| `LaserError::is_lease_lost` | `LaserError.lease_lost` | `fn:isLeaseLost` | property, error-function |
| `LaserError::is_no_capable_agent` | `LaserError.no_capable_agent` | `fn:isNoCapableAgent` | property, error-function |
| `LaserError::is_not_found` | `LaserError.not_found` | `fn:isNotFound` | property, error-function |
| `LaserError::is_not_leader` | `LaserError.not_leader` | `fn:isNotLeader` | property, error-function |
| `LaserError::is_permission_denied` | `LaserError.permission_denied` | `fn:isPermissionDenied` | property, error-function |
| `LaserError::is_quarantined` | `LaserError.quarantined` | `fn:isQuarantined` | property, error-function |
| `LaserError::is_retryable` | `LaserError.retryable` | `fn:isRetryable` | property, error-function |
| `LaserError::is_stale` | `LaserError.stale` | `fn:isStale` | property, error-function |
| `LaserError::is_stream_or_topic_not_found` | `LaserError.stream_or_topic_not_found` | `fn:isStreamOrTopicNotFound` | property, error-function |
| `LaserError::is_unavailable` | `LaserError.unavailable` | `fn:isUnavailable` | property, error-function |
| `LaserError::is_unsupported` | `LaserError.unsupported` | `fn:isUnsupported` | property, error-function |
| `LaserError::is_version_conflict` | `LaserError.version_conflict` | `fn:isVersionConflict` | property, error-function |
| `LaserError::is_version_skew` | `LaserError.version_skew` | `fn:isVersionSkew` | property, error-function |
| `LaserError::publish_cause` | `PublishFailedError` | `fn:publishCause` | protocol, error-function |
| `LaserError::rejected` | `new RejectedError()` | `new RejectedError()` | constructor |
| `LaserError::unsupported` | `new UnsupportedError()` | `new UnsupportedError()` | constructor |
| `LaserError::unsupported_feature` | `new UnsupportedError()` | `new UnsupportedError()` | constructor |

## a2a

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `a2a::A2A_JSONRPC_BINDING` | `const:A2A_JSONRPC_BINDING` | `const:A2A_JSONRPC_BINDING` |  |
| `a2a::A2A_PROTOCOL_VERSION` | `const:A2A_PROTOCOL_VERSION` | `const:A2A_PROTOCOL_VERSION` |  |
| `A2aBridge` | `A2aBridge` | `A2aBridge` |  |
| `A2aBridge::cancel` | `A2aBridge.cancel` | `A2aBridge.cancel` |  |
| `A2aBridge::card` | `A2aBridge.card` | `A2aBridge.card` |  |
| `A2aBridge::handle_rpc` | `A2aBridge.handle_rpc` | `A2aBridge.handleRpc` |  |
| `A2aBridge::new` | `new A2aBridge()` | `new A2aBridge()` | constructor |
| `A2aBridge::router` | omitted | omitted | rust-crate |
| `A2aBridge::signed_card` | `A2aBridge.signed_card` | `A2aBridge.signedCard` |  |
| `A2aBridge::submit` | `A2aBridge.submit` | `A2aBridge.submit` |  |
| `A2aBridge::task` | `A2aBridge.task` | `A2aBridge.task` |  |
| `A2aBridge::with_bridge_hops` | `A2aBridge.with_bridge_hops` | `A2aBridge.withBridgeHops` |  |
| `A2aBridge::with_capabilities` | `new A2aBridge(capabilities=)` | `A2aBridge.withCapabilities` | keywords |
| `A2aBridge::with_signing_key` | `new A2aBridge(signing_key=)` | `A2aBridge.withSigningKey` | keywords |
| `A2aMethod` | omitted | `A2aMethod` | plain-value |
| `A2aMethod::MessageSend` | omitted | `A2aMethod.MessageSend` | plain-value |
| `A2aMethod::MessageStream` | omitted | `A2aMethod.MessageStream` | plain-value |
| `A2aMethod::TasksGet` | omitted | `A2aMethod.TasksGet` | plain-value |
| `A2aMethod::TasksCancel` | omitted | `A2aMethod.TasksCancel` | plain-value |
| `AgentCard` | dict via `A2aBridge.card` | `AgentCard` | serde-dict |
| `AgentCard.name` | dict key | `AgentCard.name` | serde-dict, keywords |
| `AgentCard.description` | dict key | `AgentCard.description` | serde-dict, keywords |
| `AgentCard.version` | dict key | `AgentCard.version` | serde-dict, keywords |
| `AgentCard.supported_interfaces` | dict key | `AgentCard.supportedInterfaces` | serde-dict, keywords |
| `AgentCard.capabilities` | dict key | `AgentCard.capabilities` | serde-dict, keywords |
| `AgentCard.default_input_modes` | dict key | `AgentCard.defaultInputModes` | serde-dict, keywords |
| `AgentCard.default_output_modes` | dict key | `AgentCard.defaultOutputModes` | serde-dict, keywords |
| `AgentCard.skills` | dict key | `AgentCard.skills` | serde-dict, keywords |
| `AgentCard.signatures` | dict key | `AgentCard.signatures` | serde-dict, keywords |
| `AgentCardCapabilities` | dict via `A2aBridge.card` | `AgentCardCapabilities` | serde-dict |
| `AgentCardCapabilities.streaming` | dict key | `AgentCardCapabilities.streaming` | serde-dict, keywords |
| `AgentCardCapabilities.push_notifications` | dict key | `AgentCardCapabilities.pushNotifications` | serde-dict, keywords |
| `AgentCardCapabilities.state_transition_history` | dict key | `AgentCardCapabilities.stateTransitionHistory` | serde-dict, keywords |
| `AgentCardCapabilities.extended_agent_card` | dict key | `AgentCardCapabilities.extendedAgentCard` | serde-dict, keywords |
| `AgentCardSignature` | dict via `A2aBridge.card` | `AgentCardSignature` | serde-dict |
| `AgentCardSignature.protected` | dict key | `AgentCardSignature.protected` | serde-dict, keywords |
| `AgentCardSignature.signature` | dict key | `AgentCardSignature.signature` | serde-dict, keywords |
| `AgentInterface` | dict via `A2aBridge.card` | `AgentInterface` | serde-dict |
| `AgentInterface.url` | dict key | `AgentInterface.url` | serde-dict, keywords |
| `AgentInterface.protocol_binding` | dict key | `AgentInterface.protocolBinding` | serde-dict, keywords |
| `AgentInterface.protocol_version` | dict key | `AgentInterface.protocolVersion` | serde-dict, keywords |
| `AgentSkill` | dict via `A2aBridge.card` | `AgentSkill` | serde-dict |
| `AgentSkill.id` | dict key | `AgentSkill.id` | serde-dict, keywords |
| `AgentSkill.name` | dict key | `AgentSkill.name` | serde-dict, keywords |
| `AgentSkill.description` | dict key | `AgentSkill.description` | serde-dict, keywords |
| `AgentSkill.tags` | dict key | `AgentSkill.tags` | serde-dict, keywords |
| `AgentSkill.input_modes` | dict key | `AgentSkill.inputModes` | serde-dict, keywords |
| `AgentSkill.output_modes` | dict key | `AgentSkill.outputModes` | serde-dict, keywords |
| `Artifact` | dict via `A2aBridge.submit` | `Artifact` | serde-dict |
| `Artifact.text` | dict key | `Artifact.text` | serde-dict, keywords |
| `JsonRpcError` | dict via `A2aBridge.handle_rpc` | `JsonRpcError` | serde-dict |
| `JsonRpcError.code` | dict key | `JsonRpcError.code` | serde-dict, keywords |
| `JsonRpcError.message` | dict key | `JsonRpcError.message` | serde-dict, keywords |
| `JsonRpcRequest` | dict via `A2aBridge.handle_rpc` | `JsonRpcRequest` | serde-dict |
| `JsonRpcRequest.id` | dict key | `JsonRpcRequest.id` | serde-dict, keywords |
| `JsonRpcRequest.method` | dict key | `JsonRpcRequest.method` | serde-dict, keywords |
| `JsonRpcRequest.params` | dict key | `JsonRpcRequest.params` | serde-dict, keywords |
| `JsonRpcResponse` | dict via `A2aBridge.handle_rpc` | `JsonRpcResponse` | serde-dict |
| `JsonRpcResponse.jsonrpc` | dict key | `JsonRpcResponse.jsonrpc` | serde-dict, keywords |
| `JsonRpcResponse.id` | dict key | `JsonRpcResponse.id` | serde-dict, keywords |
| `JsonRpcResponse.result` | dict key | `JsonRpcResponse.result` | serde-dict, keywords |
| `JsonRpcResponse.error` | dict key | `JsonRpcResponse.error` | serde-dict, keywords |
| `Task` | dict via `A2aBridge.submit` | `Task` | serde-dict |
| `Task.id` | dict key | `Task.id` | serde-dict, keywords |
| `Task.status` | dict key | `Task.status` | serde-dict, keywords |
| `Task.artifacts` | dict key | `Task.artifacts` | serde-dict, keywords |
| `TaskState` | `TaskState` | `TaskState` |  |
| `TaskState::Submitted` | `TaskState.Submitted` | `TaskState` | plain-value |
| `TaskState::Working` | `TaskState.Working` | `TaskState` | plain-value |
| `TaskState::InputRequired` | `TaskState.InputRequired` | `TaskState` | plain-value |
| `TaskState::Completed` | `TaskState.Completed` | `TaskState` | plain-value |
| `TaskState::Canceled` | `TaskState.Canceled` | `TaskState` | plain-value |
| `TaskState::Failed` | `TaskState.Failed` | `TaskState` | plain-value |
| `TaskState::Rejected` | `TaskState.Rejected` | `TaskState` | plain-value |
| `TaskState::AuthRequired` | `TaskState.AuthRequired` | `TaskState` | plain-value |
| `TaskState::Unknown` | `TaskState.Unknown` | `TaskState` | plain-value |
| `TaskState::Unrecognized` | `TaskState.Unrecognized` | `TaskState` | plain-value |
| `TaskState::code` | `TaskState.code` | `fn:code` | free-function |
| `TaskState::from_code` | `TaskState.from_code` | `fn:taskStateFromCode` | free-function |
| `TaskState::is_terminal` | `TaskState.is_terminal` | `fn:taskStateIsTerminal` | free-function |
| `TaskStatus` | dict via `A2aBridge.submit` | `TaskStatus` | serde-dict |
| `TaskStatus.state` | dict key | `TaskStatus.state` | serde-dict, keywords |
| `a2a::command_from_message_send` | `fn:command_from_message_send` | `fn:commandFromMessageSend` |  |
| `a2a::enter_bridge` | `fn:enter_bridge` | `fn:enterBridge` |  |
| `a2a::task_from_envelope` | `fn:task_from_envelope` | `fn:taskFromEnvelope` |  |

## agent

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Agdx` | `Agdx` | `Agdx` |  |
| `Agdx::command` | `Agdx.command` | `Agdx.command` | keywords |
| `Agdx::emit` | `Agdx.emit` | `Agdx.emit` | keywords |
| `Agdx::fail` | `Agdx.fail` | `Agdx.fail` | keywords |
| `Agdx::request_input` | `Agdx.request_input` | `Agdx.requestInput` | keywords |
| `Agdx::respond` | `Agdx.respond` | `Agdx.respond` | keywords |
| `Agdx::status` | `Agdx.status` | `Agdx.status` | keywords |
| `Agdx::stream` | `Agdx.stream` | `Agdx.stream` | keywords |
| `AgdxSend` | `Agdx.command` | `AgdxSend` | keywords |
| `AgdxSend::body` | `Agdx.status(body=)` | `AgdxSend.body` | keywords |
| `AgdxSend::claim_check` | `Agdx.command(claim_check=)` | `AgdxSend.claimCheck` | keywords |
| `AgdxSend::content_type` | `Agdx.command(content_type=)` | `AgdxSend.contentType` | keywords |
| `AgdxSend::last` | `Agdx.status(last=)` | `AgdxSend.last` | keywords |
| `AgdxSend::send` | `Agdx.command` | `AgdxSend.send` | one-call, keywords |
| `AgdxSend::signed_by` | `Laser.agdx(signing_key=)` | `AgdxSend.signedBy` | keywords |
| `AgdxSend::with_cause` | `Agdx.command(cause=)` | `AgdxSend.withCause` | keywords |
| `AgdxSend::with_correlation` | `Agdx.status(correlation=)` | `AgdxSend.withCorrelation` | keywords |
| `AgdxSend::with_deadline_micros` | `Agdx.command(deadline_micros=)` | `AgdxSend.withDeadlineMicros` | keywords |
| `AgdxSend::with_idempotency_key` | `Agdx.command(idempotency_key=)` | `AgdxSend.withIdempotencyKey` | keywords |
| `AgdxSend::with_metadata` | `Agdx.command(metadata=)` | `AgdxSend.withMetadata` | keywords |
| `AgdxSend::with_operation` | `Agdx.command(operation=)` | `AgdxSend.withOperation` | keywords |
| `AgdxSend::with_target` | `Agdx.command(target=)` | `AgdxSend.withTarget` | keywords |
| `AgdxSend::with_task_state` | `Agdx.status(task_state=)` | `AgdxSend.withTaskState` | keywords |
| `AgdxSend::with_tool` | `Agdx.command(tool=)` | `AgdxSend.withTool` | keywords |
| `AgdxSend::with_usage` | `Agdx.command(usage=)` | `AgdxSend.withUsage` | keywords |
| `AgdxStream` | `AgdxStream` | `AgdxStream` |  |
| `AgdxStream::buffered` | `AgdxStream.buffered` | `AgdxStream.buffered` | keywords |
| `AgdxStream::channel` | `AgdxStream.channel` | `AgdxStream.channel` | property, keywords |
| `AgdxStream::content_type` | `AgdxStream.content_type` | `AgdxStream.contentType` | keywords |
| `AgdxStream::fail` | `AgdxStream.fail` | `AgdxStream.fail` | keywords |
| `AgdxStream::finish` | `AgdxStream.finish` | `AgdxStream.finish` | keywords |
| `AgdxStream::flush` | `AgdxStream.flush` | `AgdxStream.flush` | keywords |
| `AgdxStream::with_deadline_micros` | `AgdxStream.with_deadline_micros` | `AgdxStream.withDeadlineMicros` | keywords |
| `AgdxStream::with_target` | `AgdxStream.with_target` | `AgdxStream.withTarget` | keywords |
| `AgdxStream::write` | `AgdxStream.write` | `AgdxStream.write` | keywords |
| `Agent` | `Laser.spawn_agent` | `Agent` | keywords |
| `Agent.id` | `Laser.spawn_agent(agent_id=)` | `AgentBuilder.id` | keywords |
| `Agent.consumer_group` | `Laser.spawn_agent(consumer_group=)` | `AgentBuilder.consumerGroup` | keywords |
| `Agent.listen_on` | `Laser.spawn_agent(listen_on=)` | `AgentBuilder.listenOn` | keywords |
| `Agent.handler` | `Laser.spawn_agent(handler=)` | `AgentBuilder.handler` | keywords |
| `Agent.respond_on` | `Laser.spawn_agent(respond_on=)` | `AgentBuilder.respondOn` | keywords |
| `Agent.inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `AgentBuilder.inboxRoute` | keywords |
| `Agent.poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `AgentBuilder.pollInterval` | keywords |
| `Agent.shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `AgentBuilder.shutdownGrace` | keywords |
| `Agent.concurrency` | `Laser.spawn_agent(max_partitions=)` | `AgentBuilder.concurrency` | keywords |
| `Agent.max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `AgentBuilder.maxQueuedRecords` | keywords |
| `Agent.max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `AgentBuilder.maxQueuedBytes` | keywords |
| `Agent.warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `AgentBuilder.warmDedup` | keywords |
| `Agent.middleware` | `Laser.spawn_agent(middleware=)` | `AgentBuilder.middleware` | keywords |
| `Agent.on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `AgentBuilder.onDeadLetter` | keywords |
| `Agent.dedup_window` | `Laser.spawn_agent(dedup_window=)` | `AgentBuilder.dedupWindow` | keywords |
| `Agent.retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `AgentBuilder.retry` | keywords |
| `Agent.understood_features` | `Laser.spawn_agent(understood_features=)` | `AgentBuilder.understoodFeatures` | keywords |
| `Agent.deduplicator` | `Laser.spawn_agent(dedup=)` | `AgentBuilder.deduplicator` | keywords |
| `Agent.verifier` | `Laser.spawn_agent(verifier=)` | `AgentBuilder.verifier` | keywords |
| `Agent.signing_key` | `Laser.spawn_agent(signing_key=)` | `AgentBuilder.signingKey` | keywords |
| `Agent.capabilities` | `Laser.spawn_agent(capabilities=)` | `AgentBuilder.capabilities` | keywords |
| `Agent.ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `AgentBuilder.ackOnPickup` | keywords |
| `Agent.consolidate_every` | `Laser.spawn_agent(consolidate_every_ms=)` | `AgentBuilder.consolidateEvery` | keywords |
| `Agent.consolidator` | `Laser.spawn_agent(consolidator=)` | `AgentBuilder.consolidator` | keywords |
| `Agent.governor` | `Laser.spawn_agent(governor=)` | `AgentBuilder.governor` | keywords |
| `Agent.governor_retention` | `Laser.spawn_agent(governor_retention=)` | `AgentBuilder.governorRetention` | keywords |
| `Agent::builder` | `Laser.spawn_agent` | `Agent.builder` | keywords |
| `Agent::spawn` | `Laser.spawn_agent` | `Agent.spawn` | one-call |
| `AgentBuilder` | `Laser.spawn_agent` | `AgentBuilder` | keywords |
| `AgentBuilder::ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `AgentBuilder.ackOnPickup` | keywords |
| `AgentBuilder::build` | `Laser.spawn_agent` | `AgentBuilder.build` | one-call |
| `AgentBuilder::capabilities` | `Laser.spawn_agent(capabilities=)` | `AgentBuilder.capabilities` | keywords |
| `AgentBuilder::concurrency` | `Laser.spawn_agent(max_partitions=)` | `AgentBuilder.concurrency` | keywords |
| `AgentBuilder::consolidate_every` | `Laser.spawn_agent(consolidate_every_ms=)` | `AgentBuilder.consolidateEvery` | keywords |
| `AgentBuilder::consolidator` | `Laser.spawn_agent(consolidator=)` | `AgentBuilder.consolidator` | keywords |
| `AgentBuilder::consumer_group` | `Laser.spawn_agent(consumer_group=)` | `AgentBuilder.consumerGroup` | keywords |
| `AgentBuilder::dedup_window` | `Laser.spawn_agent(dedup_window=)` | `AgentBuilder.dedupWindow` | keywords |
| `AgentBuilder::deduplicator` | `Laser.spawn_agent(dedup=)` | `AgentBuilder.deduplicator` | keywords |
| `AgentBuilder::governor` | `Laser.spawn_agent(governor=)` | `AgentBuilder.governor` | keywords |
| `AgentBuilder::governor_retention` | `Laser.spawn_agent(governor_retention=)` | `AgentBuilder.governorRetention` | keywords |
| `AgentBuilder::handler` | `Laser.spawn_agent(handler=)` | `AgentBuilder.handler` | keywords |
| `AgentBuilder::id` | `Laser.spawn_agent(agent_id=)` | `AgentBuilder.id` | keywords |
| `AgentBuilder::inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `AgentBuilder.inboxRoute` | keywords |
| `AgentBuilder::listen_on` | `Laser.spawn_agent(listen_on=)` | `AgentBuilder.listenOn` | keywords |
| `AgentBuilder::max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `AgentBuilder.maxQueuedBytes` | keywords |
| `AgentBuilder::max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `AgentBuilder.maxQueuedRecords` | keywords |
| `AgentBuilder::maybe_ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `AgentBuilder.ackOnPickup` | keywords |
| `AgentBuilder::maybe_capabilities` | `Laser.spawn_agent(capabilities=)` | `AgentBuilder.capabilities` | keywords |
| `AgentBuilder::maybe_concurrency` | `Laser.spawn_agent(max_partitions=)` | `AgentBuilder.concurrency` | keywords |
| `AgentBuilder::maybe_consolidate_every` | `Laser.spawn_agent(consolidate_every_ms=)` | `AgentBuilder.consolidateEvery` | keywords |
| `AgentBuilder::maybe_consolidator` | `Laser.spawn_agent(consolidator=)` | `AgentBuilder.consolidator` | keywords |
| `AgentBuilder::maybe_consumer_group` | `Laser.spawn_agent(consumer_group=)` | `AgentBuilder.consumerGroup` | keywords |
| `AgentBuilder::maybe_dedup_window` | `Laser.spawn_agent(dedup_window=)` | `AgentBuilder.dedupWindow` | keywords |
| `AgentBuilder::maybe_deduplicator` | `Laser.spawn_agent(dedup=)` | `AgentBuilder.deduplicator` | keywords |
| `AgentBuilder::maybe_governor` | `Laser.spawn_agent(governor=)` | `AgentBuilder.governor` | keywords |
| `AgentBuilder::maybe_governor_retention` | `Laser.spawn_agent(governor_retention=)` | `AgentBuilder.governorRetention` | keywords |
| `AgentBuilder::maybe_inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `AgentBuilder.inboxRoute` | keywords |
| `AgentBuilder::maybe_max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `AgentBuilder.maxQueuedBytes` | keywords |
| `AgentBuilder::maybe_max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `AgentBuilder.maxQueuedRecords` | keywords |
| `AgentBuilder::maybe_middleware` | `Laser.spawn_agent(middleware=)` | `AgentBuilder.middleware` | keywords |
| `AgentBuilder::maybe_on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `AgentBuilder.onDeadLetter` | keywords |
| `AgentBuilder::maybe_poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `AgentBuilder.pollInterval` | keywords |
| `AgentBuilder::maybe_respond_on` | `Laser.spawn_agent(respond_on=)` | `AgentBuilder.respondOn` | keywords |
| `AgentBuilder::maybe_retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `AgentBuilder.retry` | keywords |
| `AgentBuilder::maybe_shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `AgentBuilder.shutdownGrace` | keywords |
| `AgentBuilder::maybe_signing_key` | `Laser.spawn_agent(signing_key=)` | `AgentBuilder.signingKey` | keywords |
| `AgentBuilder::maybe_understood_features` | `Laser.spawn_agent(understood_features=)` | `AgentBuilder.understoodFeatures` | keywords |
| `AgentBuilder::maybe_verifier` | `Laser.spawn_agent(verifier=)` | `AgentBuilder.verifier` | keywords |
| `AgentBuilder::maybe_warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `AgentBuilder.warmDedup` | keywords |
| `AgentBuilder::middleware` | `Laser.spawn_agent(middleware=)` | `AgentBuilder.middleware` | keywords |
| `AgentBuilder::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `AgentBuilder.onDeadLetter` | keywords |
| `AgentBuilder::poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `AgentBuilder.pollInterval` | keywords |
| `AgentBuilder::respond_on` | `Laser.spawn_agent(respond_on=)` | `AgentBuilder.respondOn` | keywords |
| `AgentBuilder::retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `AgentBuilder.retry` | keywords |
| `AgentBuilder::shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `AgentBuilder.shutdownGrace` | keywords |
| `AgentBuilder::signing_key` | `Laser.spawn_agent(signing_key=)` | `AgentBuilder.signingKey` | keywords |
| `AgentBuilder::understood_features` | `Laser.spawn_agent(understood_features=)` | `AgentBuilder.understoodFeatures` | keywords |
| `AgentBuilder::verifier` | `Laser.spawn_agent(verifier=)` | `AgentBuilder.verifier` | keywords |
| `AgentBuilder::warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `AgentBuilder.warmDedup` | keywords |
| `AgentCtx` | `AgentCtx` | `AgentCtx` |  |
| `AgentCtx::approval_gate` | `AgentCtx.approval_gate` | `AgentCtx.approvalGate` |  |
| `AgentCtx::fan_out` | `AgentCtx.fan_out` | `AgentCtx.fanOut` |  |
| `AgentCtx::laser` | `AgentCtx.laser` | `AgentCtx.laser` | property |
| `AgentCtx::message` | `AgentCtx.message` | `AgentCtx.message` | property |
| `AgentCtx::reply_on` | `AgentCtx.reply_on` | `AgentCtx.replyOn` |  |
| `AgentCtx::request` | `AgentCtx.request` | `AgentCtx.request` |  |
| `AgentCtx::respond` | `AgentCtx.respond` | `AgentCtx.respond` |  |
| `AgentCtx::respond_input` | `AgentCtx.respond_input` | `AgentCtx.respondInput` |  |
| `AgentCtx::send` | `AgentCtx.send` | `AgentCtx.send` |  |
| `AgentCtx::spawn_subconversation` | `AgentCtx.spawn_subconversation` | `AgentCtx.spawnSubconversation` |  |
| `AgentErrorBody` | dict via `Agdx.fail` | `AgentErrorBody` | serde-dict |
| `AgentErrorBody.code` | dict key | `AgentErrorBody.code` | serde-dict, keywords |
| `AgentErrorBody.message` | dict key | `AgentErrorBody.message` | serde-dict, keywords |
| `AgentErrorBody.retryable` | dict key | `AgentErrorBody.retryable` | serde-dict, keywords |
| `AgentErrorBody.detail` | dict key | `AgentErrorBody.detail` | serde-dict, keywords |
| `AgentErrorCode` | dict via `Agdx.fail` | `AgentErrorCode` | serde-dict |
| `AgentErrorCode::InvalidRequest` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::Unauthorized` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::Unsupported` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::DeadlineExceeded` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::Cancelled` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::ToolFailure` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::Internal` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::Unrecognized` | dict key | `AgentErrorCode` | serde-dict, plain-value |
| `AgentErrorCode::code` | omitted | `fn:agentErrorCode` | plain-value, free-function |
| `AgentErrorCode::from_code` | omitted | `fn:agentErrorCodeFromCode` | plain-value, free-function |
| `AgentHandle` | `AgentHandle` | `AgentHandle` |  |
| `AgentHandle::abort` | `AgentHandle.abort` | `AgentHandle.abort` |  |
| `AgentHandle::join` | `AgentHandle.join` | `AgentHandle.join` |  |
| `AgentHandle::ready` | `AgentHandle.ready` | `AgentHandle.ready` |  |
| `AgentHandle::shutdown` | `AgentHandle.shutdown` | `AgentHandle.shutdown` |  |
| `AgentHandler` | `Laser.spawn_agent(handler=)` | `AgentHandler` | callback |
| `AgentHandler::handle` | `Laser.spawn_agent(handler=)` | `AgentHandler.handle` | callback, keywords |
| `AgentMessage` | `AgentMessage` | `AgentMessage` |  |
| `AgentMessage.provenance` | `AgentMessage.provenance` | `AgentMessage.provenance` | keywords |
| `AgentMessage.payload` | `AgentMessage.payload` | `AgentMessage.payload` | keywords |
| `AgentMessage.id` | `AgentMessage.id` | `AgentMessage.id` | keywords |
| `AgentMessage.envelope` | `AgentMessage.envelope` | `AgentMessage.envelope` | keywords |
| `AgentMessage.content_type` | `AgentMessage.content_type` | `AgentMessage.contentType` | keywords |
| `AgentMessage.verified_principal` | `AgentMessage.verified_principal` | `AgentMessage.verifiedPrincipal` | keywords |
| `AgentMessage::body` | `AgentMessage.body` | `fn:agentMessageBody` | free-function |
| `AgentMessage::resolve_body` | `AgentMessage.resolve_body` | `fn:resolveBody` | free-function |
| `AgentMiddleware` | `Laser.spawn_agent(middleware=)` | `AgentMiddleware` | callback |
| `AgentMiddleware::before_handle` | `Laser.spawn_agent(middleware=)` | `AgentMiddleware.beforeHandle` | callback, keywords |
| `AgentMiddleware::after_handle` | `Laser.spawn_agent(middleware=)` | `AgentMiddleware.afterHandle` | callback, keywords |
| `AgentPresence` | dict via `Laser.advertise_presence` | `AgentPresence` | serde-dict |
| `AgentPresence.v` | dict key | `AgentPresence.v` | serde-dict, keywords |
| `AgentPresence.agent` | dict key | `AgentPresence.agent` | serde-dict, keywords |
| `AgentPresence.inbox` | dict key | `AgentPresence.inbox` | serde-dict, keywords |
| `AgentPresence::new` | `fn:agent_presence` | `fn:newAgentPresence` | free-function |
| `AgentPresence::validate` | `fn:validate_agent_presence` | `fn:validateAgentPresence` | free-function |
| `AgentPresence::with_inbox` | `fn:agent_presence(inbox=)` | `fn:newAgentPresence(inbox=)` | keywords |
| `AgentRegistry` | `AgentRegistry` | `AgentRegistry` |  |
| `AgentRegistry::agents` | `AgentRegistry.agents` | `AgentRegistry.agents` |  |
| `AgentRegistry::inbox_for` | `AgentRegistry.inbox_for` | `AgentRegistry.inboxFor` |  |
| `AgentRegistry::inbox_for_principal` | `AgentRegistry.inbox_for_principal` | `AgentRegistry.inboxForPrincipal` |  |
| `AgentRegistry::is_quarantined` | `AgentRegistry.is_quarantined` | `AgentRegistry.isQuarantined` |  |
| `AgentRegistry::lookup` | `AgentRegistry.lookup` | `AgentRegistry.lookup` |  |
| `AgentRegistry::principal_for` | `AgentRegistry.principal_for` | `AgentRegistry.principalFor` |  |
| `AgentRegistry::refresh` | `AgentRegistry.refresh` | `AgentRegistry.refresh` |  |
| `AgentRegistry::refresh_presence` | `AgentRegistry.refresh_presence` | `AgentRegistry.refreshPresence` |  |
| `AgentRegistry::resolve` | `AgentRegistry.resolve` | `AgentRegistry.resolve` |  |
| `AgentScope` | `AgentScope` | `AgentScope` |  |
| `AgentScope::advertise` | `AgentScope.advertise` | `AgentScope.advertise` |  |
| `AgentScope::ask` | `AgentScope.ask` | `AgentScope.ask` |  |
| `AgentScope::contract` | `AgentScope.contract` | `AgentScope.contract` |  |
| `AgentScope::id` | `AgentScope.id` | `AgentScope.id` | property |
| `AgentScope::publish_card` | `AgentScope.publish_card` | `AgentScope.publishCard` |  |
| `AgentScope::send` | `AgentScope.send` | `AgentScope.send` |  |
| `BatchItem` | dict via `Laser.execute_batch` | `wire.BatchItem` | serde-dict |
| `BatchItem.code` | dict key | `wire.BatchItem.code` | serde-dict, keywords |
| `BatchItem.payload` | dict key | `wire.BatchItem.payload` | serde-dict, keywords |
| `Budget` | `Workflow.budget` | `Budget` | keywords |
| `Budget::invocations` | `Workflow.budget(invocations=)` | `Budget.invocations` | keywords |
| `Budget::tokens` | `Workflow.budget(tokens=)` | `Budget.tokens` | keywords |
| `Budget::unlimited` | `Workflow.budget` | `Budget.unlimited` | keywords |
| `Budget::wall_clock` | `Workflow.budget(wall_clock_ms=)` | `Budget.wallClock` | keywords |
| `CapabilityDescriptor` | dict via `RouteCandidate.capability` | `CapabilityDescriptor` | serde-dict |
| `CapabilityDescriptor.skill_id` | dict key | `CapabilityDescriptor.skillId` | serde-dict, keywords |
| `CapabilityDescriptor.input` | dict key | `CapabilityDescriptor.input` | serde-dict, keywords |
| `CapabilityDescriptor.output` | dict key | `CapabilityDescriptor.output` | serde-dict, keywords |
| `CapabilityDescriptor.cost_class` | dict key | `CapabilityDescriptor.costClass` | serde-dict, keywords |
| `CapabilityDescriptor.latency_class` | dict key | `CapabilityDescriptor.latencyClass` | serde-dict, keywords |
| `CapabilityDescriptor.max_concurrency` | dict key | `CapabilityDescriptor.maxConcurrency` | serde-dict, keywords |
| `CapabilityDescriptor.health` | dict key | `CapabilityDescriptor.health` | serde-dict, keywords |
| `CapabilityDescriptor.load` | dict key | `CapabilityDescriptor.load` | serde-dict, keywords |
| `CapabilitySelector` | `Laser.contract` | `CapabilitySelector` | keywords |
| `CapabilitySelector.skill` | `Laser.contract(skill=)` | `CapabilitySelector.skill` | keywords |
| `CapabilitySelector.policy` | `Laser.contract(policy=)` | `CapabilitySelector.policy` | keywords |
| `CapabilitySelector.principal` | `Laser.contract(principal=)` | `CapabilitySelector.principal` | keywords |
| `CapabilitySelector::new` | `Laser.contract` | `fn:capabilitySelector` | keywords, free-function |
| `CapabilitySelector::principal` | `Laser.contract(principal=)` | `CapabilitySelector.principal` | keywords |
| `ChunkAssembler` | `ChunkAssembler` | `ChunkAssembler` |  |
| `ChunkAssembler::abandon` | `ChunkAssembler.abandon` | `ChunkAssembler.abandon` |  |
| `ChunkAssembler::duplicates_dropped` | `ChunkAssembler.duplicates_dropped` | `ChunkAssembler.duplicatesDropped` | property |
| `ChunkAssembler::feed` | `ChunkAssembler.feed` | `ChunkAssembler.feed` |  |
| `ChunkAssembler::is_finished` | `ChunkAssembler.finished` | `ChunkAssembler.isFinished` | property |
| `ChunkAssembler::late_dropped` | `ChunkAssembler.late_dropped` | `ChunkAssembler.lateDropped` | property |
| `ChunkAssembler::new` | `new ChunkAssembler()` | `new ChunkAssembler()` | constructor |
| `ClientMetadata` | dict via `Laser.client_metadata_all` | `ClientMetadata` | serde-dict |
| `ClientMetadata.client_id` | dict key | `ClientMetadata.clientId` | serde-dict, keywords |
| `ClientMetadata.user_id` | dict key | `ClientMetadata.userId` | serde-dict, keywords |
| `ClientMetadata.transport` | dict key | `ClientMetadata.transport` | serde-dict, keywords |
| `ClientMetadata.address` | dict key | `ClientMetadata.address` | serde-dict, keywords |
| `ClientMetadata.consumer_groups_count` | dict key | `ClientMetadata.consumerGroupsCount` | serde-dict, keywords |
| `ClientMetadata.metadata` | dict key | `ClientMetadata.metadata` | serde-dict, keywords |
| `ClientMetadataPage` | `ClientMetadataPage` | `ClientMetadataPage` |  |
| `ClientMetadataPage.clients` | `ClientMetadataPage.clients` | `ClientMetadataPage.clients` | keywords |
| `ClientMetadataPage.next_cursor` | `ClientMetadataPage.next_cursor` | `ClientMetadataPage.nextCursor` | keywords |
| `ClientMetadataRequest` | `Laser.client_metadata` | `ClientMetadataRequest` | keywords |
| `ClientMetadataRequest::after` | `Laser.client_metadata(after=)` | `ClientMetadataRequest.after` | keywords |
| `ClientMetadataRequest::all` | `Laser.client_metadata_all` | `ClientMetadataRequest.all` | one-call |
| `ClientMetadataRequest::limit` | `Laser.client_metadata(limit=)` | `ClientMetadataRequest.limit` | keywords |
| `ClientMetadataRequest::page` | `Laser.client_metadata` | `ClientMetadataRequest.page` | one-call |
| `ClientMetadataRequest::principal` | `Laser.client_metadata(principal=)` | `ClientMetadataRequest.principal` | keywords |
| `ClientMetadataRequest::with_metadata_only` | `Laser.client_metadata(metadata_only=)` | `ClientMetadataRequest.withMetadataOnly` | keywords |
| `Clock` | `Clock` | `Clock` |  |
| `Clock::now_micros` | `Clock.now_micros` | `Clock.nowMicros` | keywords |
| `ConcurrencyPolicy` | `Laser.spawn_agent(max_partitions=)` | `ConcurrencyPolicy` | keywords |
| `ConcurrencyPolicy::Serial` | `Laser.spawn_agent(max_partitions=)` | `ConcurrencyPolicy` | keywords, plain-value |
| `ConcurrencyPolicy::SerialPerPartition` | `Laser.spawn_agent(max_partitions=)` | `ConcurrencyPolicy` | keywords, plain-value |
| `ConsumerRef` | `ConsumerRef` | `ConsumerRef` |  |
| `ConsumerRef::Group` | `ConsumerRef.Group` | `ConsumerRef` | plain-value |
| `ConsumerRef::Consumer` | `ConsumerRef.Consumer` | `ConsumerRef` | plain-value |
| `ConsumptionStatus` | `ConsumptionStatus` | `ConsumptionStatus` |  |
| `ConsumptionStatus::NotYetConsumed` | `ConsumptionStatus.NotYetConsumed` | `ConsumptionStatus` | plain-value |
| `ConsumptionStatus::Consumed` | `ConsumptionStatus.Consumed` | `ConsumptionStatus` | plain-value |
| `ContentRef` | dict via `RouteCandidate.capability` | `ContentRef` | serde-dict |
| `ContentRef::ContentType` | dict key | `ContentRef` | serde-dict, plain-value |
| `ContentRef::SchemaId` | dict key | `ContentRef` | serde-dict, plain-value |
| `Contract` | `Contract` | `Contract` |  |
| `Contract::Completed` | `Contract.Completed` | `Contract` | plain-value |
| `Contract::Failed` | `Contract.Failed` | `Contract` | plain-value |
| `Contract::NotConsumed` | `Contract.NotConsumed` | `Contract` | plain-value |
| `Contract::TimedOut` | `Contract.TimedOut` | `Contract` | plain-value |
| `ContractBuilder` | `Laser.contract` | `ContractBuilder` | keywords |
| `ContractBuilder::conversation` | `Laser.contract(conversation=)` | `ContractBuilder.conversation` | keywords |
| `ContractBuilder::deadline` | `Laser.contract(deadline_ms=)` | `ContractBuilder.deadline` | keywords |
| `ContractBuilder::expire_if_not_consumed` | `Laser.contract(expire_if_not_consumed_ms=)` | `ContractBuilder.expireIfNotConsumed` | keywords |
| `ContractBuilder::fence` | `Laser.contract(fence=)` | `ContractBuilder.fence` | keywords |
| `ContractBuilder::from` | `Laser.contract(source=)` | `ContractBuilder.from` | keywords |
| `ContractBuilder::inbox_route` | `Laser.contract(fixed_inbox=)` | `ContractBuilder.inboxRoute` | keywords |
| `ContractBuilder::payload` | `Laser.contract(payload=)` | `ContractBuilder.payload` | keywords |
| `ContractBuilder::registered` | `Laser.contract(registered=)` | `ContractBuilder.registered` | keywords |
| `ContractBuilder::reply_on` | `Laser.contract(reply_on=)` | `ContractBuilder.replyOn` | keywords |
| `ContractBuilder::send` | `Laser.contract` | `ContractBuilder.send` | one-call |
| `ConversationState` | `ConversationState` | `ConversationState` |  |
| `ConversationState::load` | `ConversationState.load` | `ConversationState.load` |  |
| `ConversationState::load_with` | `ConversationState.load_with` | `ConversationState.loadWith` |  |
| `agent::DEFAULT_CHUNK_FLUSH_BYTES` | `const:DEFAULT_CHUNK_FLUSH_BYTES` | `const:DEFAULT_CHUNK_FLUSH_BYTES` |  |
| `agent::DEFAULT_CHUNK_LINGER_MS` | `const:DEFAULT_CHUNK_LINGER_MS` | `const:DEFAULT_CHUNK_LINGER_MS` |  |
| `agent::DEFAULT_SESSION_CONTEXT_TOKENS` | `const:DEFAULT_SESSION_CONTEXT_TOKENS` | `const:DEFAULT_SESSION_CONTEXT_TOKENS` |  |
| `agent::DEFAULT_SESSION_CONTEXT_TURNS` | `const:DEFAULT_SESSION_CONTEXT_TURNS` | `const:DEFAULT_SESSION_CONTEXT_TURNS` |  |
| `agent::DEFAULT_SESSION_MEMORY_NAMESPACE` | `const:DEFAULT_SESSION_MEMORY_NAMESPACE` | `const:DEFAULT_SESSION_MEMORY_NAMESPACE` |  |
| `agent::DEFAULT_SESSION_TOPICS` | `const:DEFAULT_SESSION_TOPICS` | `const:DEFAULT_SESSION_TOPICS` |  |
| `DeadLetterSink` | `Laser.spawn_agent(dead_letter=)` | `DeadLetterSink` | callback |
| `DeadLetterSink::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `DeadLetterSink.onDeadLetter` | callback, keywords |
| `Deduplicator` | `Laser.spawn_agent(dedup=)` | `Deduplicator` | callback |
| `Deduplicator::observe` | `Laser.spawn_agent(dedup=)` | `Deduplicator.observe` | callback, keywords |
| `agent::FINISH_REASON_ABANDONED` | `const:FINISH_REASON_ABANDONED` | `const:FINISH_REASON_ABANDONED` |  |
| `agent::FINISH_REASON_GAP` | `const:FINISH_REASON_GAP` | `const:FINISH_REASON_GAP` |  |
| `Gather` | `Gather` | `Gather` |  |
| `Gather.ok` | `Gather.ok` | `Gather.ok` | keywords |
| `Gather.failures` | `Gather.failures` | `Gather.failures` | keywords |
| `Gather::replies` | `Gather.replies` | `fn:gatherReplies` | free-function |
| `GatherPolicy` | `AgentCtx.fan_out(policy=, quorum=)` | `GatherPolicy` | keywords |
| `GatherPolicy::RequireAll` | `AgentCtx.fan_out(policy=)` | `GatherPolicy` | keywords, plain-value |
| `GatherPolicy::Quorum` | `AgentCtx.fan_out(policy=, quorum=)` | `GatherPolicy` | keywords, plain-value |
| `GatherPolicy::BestEffort` | `AgentCtx.fan_out(policy=)` | `GatherPolicy` | keywords, plain-value |
| `Health` | dict via `RouteCandidate.capability` | `Health` | serde-dict |
| `Health::Healthy` | dict key | `Health` | serde-dict, plain-value |
| `Health::Degraded` | dict key | `Health` | serde-dict, plain-value |
| `Health::Unavailable` | dict key | `Health` | serde-dict, plain-value |
| `Health::Unrecognized` | dict key | `Health` | serde-dict, plain-value |
| `Health::code` | omitted | `fn:healthCode` | plain-value, free-function |
| `Health::from_code` | omitted | `fn:healthFromCode` | plain-value, free-function |
| `InboxRoute` | `Laser.spawn_agent(fixed_inbox=)` | `InboxRoute` | keywords |
| `InboxRoute::Advertised` | `Laser.spawn_agent(fixed_inbox=)` | `InboxRoute` | keywords, plain-value |
| `InboxRoute::Fixed` | `Laser.spawn_agent(fixed_inbox=)` | `InboxRoute` | keywords, plain-value |
| `InboxRoute::resolve` | `fn:inbox_route_resolve` | `fn:resolveInboxRoute` | free-function |
| `Laser` | `Laser` | `Laser` |  |
| `Laser::advertise_presence` | `Laser.advertise_presence` | `Laser.advertisePresence` |  |
| `Laser::agdx` | `Laser.agdx` | `Laser.agdx` |  |
| `Laser::agent` | `Laser.agent` | `Laser.agent` |  |
| `Laser::agent_registry` | `Laser.agent_registry` | `Laser.agentRegistry` |  |
| `Laser::agui_events` | `Laser.agui_events` | `Laser.aguiEvents` |  |
| `Laser::authz_history` | `Laser.authz_history` | `Laser.authzHistory` |  |
| `Laser::bind_roles` | `Laser.bind_roles` | `Laser.bindRoles` |  |
| `Laser::bind_roles_expect_revision` | `Laser.bind_roles(expect_revision=)` | `Laser.bindRoles(expectRevision=)` | overload |
| `Laser::bindings` | `Laser.bindings` | `Laser.bindings` |  |
| `Laser::bootstrap` | `Laser.bootstrap` | `Laser.bootstrap` |  |
| `Laser::builder` | `Laser.connect` | `Laser.builder` | keywords |
| `Laser::cancel_query` | `Laser.cancel_query` | `Laser.cancelQuery` |  |
| `Laser::capabilities` | `Laser.capabilities` | `Laser.capabilities` |  |
| `Laser::changes_topic` | `Laser.changes_topic` | `Laser.changesTopic` | property |
| `Laser::clear_presence` | `Laser.clear_presence` | `Laser.clearPresence` |  |
| `Laser::client` | omitted | `Laser.client` | rust-crate, property |
| `Laser::client_metadata` | `Laser.client_metadata` | `Laser.clientMetadata` |  |
| `Laser::close` | `Laser.close` | `Laser.close` |  |
| `Laser::connect` | `Laser.connect` | `Laser.connect` |  |
| `Laser::connect_env` | `Laser.connect_env` | `Laser.connectEnv` |  |
| `Laser::connect_with_stream` | `Laser.connect_with_stream` | `Laser.connectWithStream` |  |
| `Laser::consumed` | `Laser.consumed` | `Laser.consumed` |  |
| `Laser::context` | `Laser.context` | `Laser.context` |  |
| `Laser::contract` | `Laser.contract` | `Laser.contract` |  |
| `Laser::control_topic` | `Laser.control_topic` | `Laser.controlTopic` | property |
| `Laser::default_stream` | `Laser.default_stream` | `Laser.defaultStream` | property |
| `Laser::define_role` | `Laser.define_role` | `Laser.defineRole` |  |
| `Laser::delete_role` | `Laser.delete_role` | `Laser.deleteRole` |  |
| `Laser::destinations` | `Laser.destinations` | `Laser.destinations` |  |
| `Laser::dlq_topic` | `Laser.dlq_topic` | `Laser.dlqTopic` | property |
| `Laser::execute_batch` | `Laser.execute_batch` | `Laser.executeBatch` |  |
| `Laser::execute_checkpoint` | `Laser.execute_checkpoint` | `Laser.executeCheckpoint` |  |
| `Laser::execute_query` | `Laser.execute_query` | `Laser.executeQuery` |  |
| `Laser::fork` | `Laser.fork` | `Laser.fork` |  |
| `Laser::forks` | `Laser.forks` | `Laser.forks` |  |
| `Laser::from_client` | omitted | `Laser.fromClient` | rust-crate |
| `Laser::get_bindings` | `Laser.get_bindings` | `Laser.getBindings` |  |
| `Laser::get_role` | `Laser.get_role` | `Laser.getRole` |  |
| `Laser::graph` | `Laser.graph` | `Laser.graph` |  |
| `Laser::kv` | `Laser.kv` | `Laser.kv` |  |
| `Laser::kv_namespaces` | `Laser.kv_namespaces` | `Laser.kvNamespaces` |  |
| `Laser::list_roles` | `Laser.list_roles` | `Laser.listRoles` |  |
| `Laser::local` | `Laser.local` | `Laser.local` |  |
| `Laser::memory` | `Laser.memory` | `Laser.memory` |  |
| `Laser::memory_custom` | `Laser.memory_custom` | `Laser.memoryCustom` |  |
| `Laser::memory_on_topic` | `Laser.memory_on_topic` | `Laser.memoryOnTopic` |  |
| `Laser::memory_topic` | `Laser.memory_topic` | `Laser.memoryTopic` |  |
| `Laser::memory_with` | `Laser.memory_with` | `Laser.memoryWith` |  |
| `Laser::ops_stream` | `Laser.ops_stream` | `Laser.opsStream` | property |
| `Laser::projections` | `Laser.projections` | `Laser.projections` |  |
| `Laser::publish_card` | `Laser.publish_card` | `Laser.publishCard` |  |
| `Laser::publish_state_delta` | `Laser.publish_state_delta` | `Laser.publishStateDelta` |  |
| `Laser::publish_state_snapshot` | `Laser.publish_state_snapshot` | `Laser.publishStateSnapshot` |  |
| `Laser::quarantine` | `Laser.quarantine` | `Laser.quarantine` |  |
| `Laser::quarantine_signed` | `Laser.quarantine_signed` | `Laser.quarantineSigned` |  |
| `Laser::query` | `Laser.query` | `Laser.query` |  |
| `Laser::query_lakehouse` | `Laser.query_lakehouse` | `Laser.queryLakehouse` |  |
| `Laser::query_page` | `Laser.query_page` | `Laser.queryPage` |  |
| `Laser::query_status` | `Laser.query_status` | `Laser.queryStatus` |  |
| `Laser::query_target` | `Laser.query_target` | `Laser.queryTarget` |  |
| `Laser::reassemble_channel` | `Laser.reassemble_channel` | `Laser.reassembleChannel` |  |
| `Laser::reconstruct_state` | `Laser.reconstruct_state` | `Laser.reconstructState` |  |
| `Laser::redrive_dead_letter` | `Laser.redrive_dead_letter` | `Laser.redriveDeadLetter` |  |
| `Laser::refresh_capabilities` | `Laser.refresh_capabilities` | `Laser.refreshCapabilities` |  |
| `Laser::request` | `Laser.request` | `Laser.request` |  |
| `Laser::runs` | `Laser.runs` | `Laser.runs` |  |
| `Laser::scatter` | `Laser.scatter` | `Laser.scatter` | keywords |
| `Laser::scatter_report` | `Laser.scatter_report` | `Laser.scatterReport` | keywords |
| `Laser::schemas` | `Laser.schemas` | `Laser.schemas` |  |
| `Laser::send_agent` | `Laser.send_agent` | `Laser.sendAgent` |  |
| `Laser::sessions` | `Laser.sessions` | `Laser.sessions` |  |
| `Laser::sessions_with` | `Laser.sessions(stream=)` | `Laser.sessions(config=)` | overload |
| `Laser::spawn_subconversation` | `Laser.spawn_subconversation` | `Laser.spawnSubconversation` |  |
| `Laser::stream` | `Laser.stream` | `Laser.stream` |  |
| `Laser::topic` | `Laser.topic` | `Laser.topic` |  |
| `Laser::unquarantine` | `Laser.unquarantine` | `Laser.unquarantine` |  |
| `Laser::unquarantine_signed` | `Laser.unquarantine_signed` | `Laser.unquarantineSigned` |  |
| `Laser::wait_until_ready` | `Laser.wait_until_ready` | `Laser.waitUntilReady` |  |
| `Laser::watch` | `Laser.watch` | `Laser.watch` |  |
| `Laser::whoami` | `Laser.whoami` | `Laser.whoami` |  |
| `Laser::with_capabilities` | `Laser.with_capabilities` | `Laser.withCapabilities` |  |
| `Laser::with_changes_topic` | `Laser.with_changes_topic` | `Laser.withChangesTopic` |  |
| `Laser::with_control_topic` | `Laser.with_control_topic` | `Laser.withControlTopic` |  |
| `Laser::with_default_stream` | `Laser.with_default_stream` | `Laser.withDefaultStream` |  |
| `Laser::with_dlq_topic` | `Laser.with_dlq_topic` | `Laser.withDlqTopic` |  |
| `Laser::with_governor` | `Laser.with_governor` | `Laser.withGovernor` |  |
| `Laser::with_governor_retention` | `Laser.with_governor_retention` | `Laser.withGovernor(retention=)` | overload |
| `Laser::with_ops_stream` | `Laser.with_ops_stream` | `Laser.withOpsStream` |  |
| `Laser::workflow` | `Laser.workflow` | `Laser.workflow` |  |
| `LocalAgentHandler` | `Laser.spawn_agent(handler=)` | `AgentHandler` | callback, trait-variant |
| `LocalAgentHandler::handle` | `Laser.spawn_agent(handler=)` | `AgentHandler.handle` | callback, keywords |
| `agent::MAX_CHUNK_BODY_BYTES` | `const:MAX_CHUNK_BODY_BYTES` | `const:MAX_CHUNK_BODY_BYTES` |  |
| `MemoryHandler` | `MemoryHandler` | `MemoryHandler` |  |
| `MemoryHandler::auto_remember` | `MemoryHandler.auto_remember` | `MemoryHandler.autoRemember` |  |
| `MemoryHandler::new` | `new MemoryHandler()` | `new MemoryHandler()` | constructor |
| `OnTimeout` | omitted | `OnTimeout` | plain-value |
| `OnTimeout::Fail` | omitted | `OnTimeout` | plain-value |
| `OnTimeout::Reassign` | omitted | `OnTimeout` | plain-value |
| `RegisteredCard` | `RegisteredCard` | `RegisteredCard` |  |
| `RegisteredCard.agent` | `RegisteredCard.agent` | `RegisteredCard.agent` | keywords |
| `RegisteredCard.card` | `RegisteredCard.card` | `RegisteredCard.card` | keywords |
| `RegisteredCard.observed_at_micros` | `RegisteredCard.observed_at_micros` | `RegisteredCard.observedAtMicros` | keywords |
| `RegisteredCard::available_for` | `RegisteredCard.available_for` | `fn:cardAvailableFor` | free-function |
| `RegisteredCard::is_fresh` | `RegisteredCard.is_fresh` | `fn:cardIsFresh` | free-function |
| `RegisteredCard::serves` | `RegisteredCard.serves` | `fn:cardServes` | free-function |
| `ReliableConsumer` | `Laser.spawn_agent` | `ReliableConsumer` | keywords |
| `ReliableConsumer.group` | `Laser.spawn_agent(consumer_group=)` | `ReliableConsumerOptions.group` | keywords |
| `ReliableConsumer.agent` | `Laser.spawn_agent(agent_id=)` | `ReliableConsumerOptions.agent` | keywords |
| `ReliableConsumer.topic` | `Laser.spawn_agent(listen_on=)` | `ReliableConsumerOptions.topic` | keywords |
| `ReliableConsumer.dedup_window` | `Laser.spawn_agent(dedup_window=)` | `ReliableConsumerOptions.dedupWindow` | keywords |
| `ReliableConsumer.retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `ReliableConsumerOptions.retry` | keywords |
| `ReliableConsumer.understood_features` | `Laser.spawn_agent(understood_features=)` | `ReliableConsumerOptions.understoodFeatures` | keywords |
| `ReliableConsumer.poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `ReliableConsumerOptions.pollIntervalMs` | keywords |
| `ReliableConsumer.shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `ReliableConsumerOptions.shutdownGraceMs` | keywords |
| `ReliableConsumer.concurrency` | `Laser.spawn_agent(max_partitions=)` | `ReliableConsumerOptions.concurrency` | keywords |
| `ReliableConsumer.max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `ReliableConsumerOptions.maxQueuedRecords` | keywords |
| `ReliableConsumer.max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `ReliableConsumerOptions.maxQueuedBytes` | keywords |
| `ReliableConsumer.respond_on` | `Laser.spawn_agent(respond_on=)` | `ReliableConsumerOptions.respondOn` | keywords |
| `ReliableConsumer.inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `ReliableConsumerOptions.inboxRoute` | keywords |
| `ReliableConsumer.ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `ReliableConsumerOptions.ackOnPickup` | keywords |
| `ReliableConsumer.deduplicator` | `Laser.spawn_agent(dedup=)` | `ReliableConsumerOptions.deduplicator` | keywords |
| `ReliableConsumer.warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `ReliableConsumerOptions.warmDedup` | keywords |
| `ReliableConsumer.middleware` | `Laser.spawn_agent(middleware=)` | `ReliableConsumerOptions.middleware` | keywords |
| `ReliableConsumer.on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `ReliableConsumerOptions.onDeadLetter` | keywords |
| `ReliableConsumer.verifier` | `Laser.spawn_agent(verifier=)` | `ReliableConsumerOptions.verifier` | keywords |
| `ReliableConsumer.signing_key` | `Laser.spawn_agent(signing_key=)` | `ReliableConsumerOptions.signingKey` | keywords |
| `ReliableConsumer::builder` | `Laser.spawn_agent` | `new ReliableConsumer()` | keywords, constructor |
| `ReliableConsumer::run` | `Laser.spawn_agent` | `ReliableConsumer.run` | one-call, async-runtime |
| `ReliableConsumerBuilder` | `Laser.spawn_agent` | `ReliableConsumerOptions` | keywords |
| `ReliableConsumerBuilder::ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `ReliableConsumerOptions.ackOnPickup` | keywords |
| `ReliableConsumerBuilder::agent` | `Laser.spawn_agent(agent_id=)` | `ReliableConsumerOptions.agent` | keywords |
| `ReliableConsumerBuilder::build` | `Laser.spawn_agent` | `new ReliableConsumer()` | one-call |
| `ReliableConsumerBuilder::concurrency` | `Laser.spawn_agent(max_partitions=)` | `ReliableConsumerOptions.concurrency` | keywords |
| `ReliableConsumerBuilder::dedup_window` | `Laser.spawn_agent(dedup_window=)` | `ReliableConsumerOptions.dedupWindow` | keywords |
| `ReliableConsumerBuilder::deduplicator` | `Laser.spawn_agent(dedup=)` | `ReliableConsumerOptions.deduplicator` | keywords |
| `ReliableConsumerBuilder::group` | `Laser.spawn_agent(consumer_group=)` | `ReliableConsumerOptions.group` | keywords |
| `ReliableConsumerBuilder::inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `ReliableConsumerOptions.inboxRoute` | keywords |
| `ReliableConsumerBuilder::max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `ReliableConsumerOptions.maxQueuedBytes` | keywords |
| `ReliableConsumerBuilder::max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `ReliableConsumerOptions.maxQueuedRecords` | keywords |
| `ReliableConsumerBuilder::maybe_ack_on_pickup` | `Laser.spawn_agent(ack_on_pickup=)` | `ReliableConsumerOptions.ackOnPickup` | keywords |
| `ReliableConsumerBuilder::maybe_agent` | `Laser.spawn_agent(agent_id=)` | `ReliableConsumerOptions.agent` | keywords |
| `ReliableConsumerBuilder::maybe_concurrency` | `Laser.spawn_agent(max_partitions=)` | `ReliableConsumerOptions.concurrency` | keywords |
| `ReliableConsumerBuilder::maybe_dedup_window` | `Laser.spawn_agent(dedup_window=)` | `ReliableConsumerOptions.dedupWindow` | keywords |
| `ReliableConsumerBuilder::maybe_deduplicator` | `Laser.spawn_agent(dedup=)` | `ReliableConsumerOptions.deduplicator` | keywords |
| `ReliableConsumerBuilder::maybe_inbox_route` | `Laser.spawn_agent(fixed_inbox=)` | `ReliableConsumerOptions.inboxRoute` | keywords |
| `ReliableConsumerBuilder::maybe_max_queued_bytes` | `Laser.spawn_agent(max_queued_bytes=)` | `ReliableConsumerOptions.maxQueuedBytes` | keywords |
| `ReliableConsumerBuilder::maybe_max_queued_records` | `Laser.spawn_agent(max_queued_records=)` | `ReliableConsumerOptions.maxQueuedRecords` | keywords |
| `ReliableConsumerBuilder::maybe_middleware` | `Laser.spawn_agent(middleware=)` | `ReliableConsumerOptions.middleware` | keywords |
| `ReliableConsumerBuilder::maybe_on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `ReliableConsumerOptions.onDeadLetter` | keywords |
| `ReliableConsumerBuilder::maybe_poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `ReliableConsumerOptions.pollIntervalMs` | keywords |
| `ReliableConsumerBuilder::maybe_respond_on` | `Laser.spawn_agent(respond_on=)` | `ReliableConsumerOptions.respondOn` | keywords |
| `ReliableConsumerBuilder::maybe_retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `ReliableConsumerOptions.retry` | keywords |
| `ReliableConsumerBuilder::maybe_shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `ReliableConsumerOptions.shutdownGraceMs` | keywords |
| `ReliableConsumerBuilder::maybe_signing_key` | `Laser.spawn_agent(signing_key=)` | `ReliableConsumerOptions.signingKey` | keywords |
| `ReliableConsumerBuilder::maybe_understood_features` | `Laser.spawn_agent(understood_features=)` | `ReliableConsumerOptions.understoodFeatures` | keywords |
| `ReliableConsumerBuilder::maybe_verifier` | `Laser.spawn_agent(verifier=)` | `ReliableConsumerOptions.verifier` | keywords |
| `ReliableConsumerBuilder::maybe_warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `ReliableConsumerOptions.warmDedup` | keywords |
| `ReliableConsumerBuilder::middleware` | `Laser.spawn_agent(middleware=)` | `ReliableConsumerOptions.middleware` | keywords |
| `ReliableConsumerBuilder::on_dead_letter` | `Laser.spawn_agent(dead_letter=)` | `ReliableConsumerOptions.onDeadLetter` | keywords |
| `ReliableConsumerBuilder::poll_interval` | `Laser.spawn_agent(poll_interval_ms=)` | `ReliableConsumerOptions.pollIntervalMs` | keywords |
| `ReliableConsumerBuilder::respond_on` | `Laser.spawn_agent(respond_on=)` | `ReliableConsumerOptions.respondOn` | keywords |
| `ReliableConsumerBuilder::retry` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `ReliableConsumerOptions.retry` | keywords |
| `ReliableConsumerBuilder::shutdown_grace` | `Laser.spawn_agent(shutdown_grace_ms=)` | `ReliableConsumerOptions.shutdownGraceMs` | keywords |
| `ReliableConsumerBuilder::signing_key` | `Laser.spawn_agent(signing_key=)` | `ReliableConsumerOptions.signingKey` | keywords |
| `ReliableConsumerBuilder::topic` | `Laser.spawn_agent(listen_on=)` | `ReliableConsumerOptions.topic` | keywords |
| `ReliableConsumerBuilder::understood_features` | `Laser.spawn_agent(understood_features=)` | `ReliableConsumerOptions.understoodFeatures` | keywords |
| `ReliableConsumerBuilder::verifier` | `Laser.spawn_agent(verifier=)` | `ReliableConsumerOptions.verifier` | keywords |
| `ReliableConsumerBuilder::warm_dedup` | `Laser.spawn_agent(warm_dedup=)` | `ReliableConsumerOptions.warmDedup` | keywords |
| `ReplayBound` | `ContextScope.state` | `ReplayBound` | keywords |
| `ReplayBound::FromOffsets` | `ContextScope.state(from_offsets=)` | `ReplayBound` | keywords, plain-value |
| `ReplayBound::Last` | `ContextScope.state(last_n=)` | `ReplayBound` | keywords, plain-value |
| `ReplayBound::Full` | `ContextScope.state(full=)` | `ReplayBound` | keywords, plain-value |
| `ReplayBound::FromCheckpoint` | `ContextScope.state(from_checkpoint=)` | `ReplayBound` | keywords, plain-value |
| `ReplayBound::At` | `ContextScope.state(at=)` | `ReplayBound` | keywords, plain-value |
| `RetryPolicy` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `RetryPolicy` | keywords |
| `RetryPolicy.max_attempts` | `Laser.spawn_agent(retry_max_attempts=)` | `RetryPolicy.maxAttempts` | keywords |
| `RetryPolicy.base_delay` | `Laser.spawn_agent(retry_base_delay_ms=)` | `RetryPolicy.baseDelayMs` | keywords |
| `RetryPolicy::backoff` | `Laser.spawn_agent(retry_max_attempts=, retry_base_delay_ms=)` | `fn:retryBackoff` | keywords, free-function |
| `RouteCandidate` | `RouteCandidate` | `RouteCandidate` |  |
| `RouteCandidate.agent` | `RouteCandidate.agent` | `RouteCandidate.agent` | keywords |
| `RouteCandidate.card` | `RouteCandidate.card` | `RouteCandidate.card` | keywords |
| `RouteCandidate.capability` | `RouteCandidate.capability` | `RouteCandidate.capability` | keywords |
| `RoutePolicy` | `Laser.contract(policy=)` | `RoutePolicy` | keywords |
| `RoutePolicy::Cheapest` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RoutePolicy::Fastest` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RoutePolicy::LeastLoaded` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RoutePolicy::Sticky` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RoutePolicy::Any` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RoutePolicy::Custom` | `Laser.contract(policy=)` | `RoutePolicy` | keywords, plain-value |
| `RouteScorer` | `Laser.contract(policy=)` | `RouteScorer` | callback |
| `RouteScorer::select` | `Laser.contract(policy=)` | `RouteScorer.select` | callback, keywords |
| `Router` | `Workflow.step` | `Router` | keywords |
| `Router::To` | `Workflow.step(to=)` | `Router` | keywords, plain-value |
| `Router::ToPrincipal` | `Workflow.step(to=, principal=)` | `Router` | keywords, plain-value |
| `Router::Broadcast` | `new Provenance(target_agent_id=)` | `Router` | keywords, plain-value |
| `Router::ToCapable` | `Workflow.step(to_capable=)` | `Router` | keywords, plain-value |
| `Router::AllCapable` | `Workflow.step(all_capable=)` | `Router` | keywords, plain-value |
| `Router::all_capable` | `Workflow.step(all_capable=)` | `fn:routeAllCapable` | keywords, free-function |
| `Router::apply` | `new Provenance(target_agent_id=)` | `fn:applyRoute` | keywords, free-function |
| `Router::broadcast` | `new Provenance(target_agent_id=)` | `fn:routeBroadcast` | keywords, free-function |
| `Router::resolve_targets` | `AgentRegistry.resolve_targets` | `fn:resolveTargets` | keywords, free-function |
| `Router::to` | `Workflow.step(to=)` | `fn:routeTo` | keywords, free-function |
| `Router::to_capable` | `Workflow.step(to_capable=)` | `fn:routeToCapable` | keywords, free-function |
| `Router::to_principal` | `Workflow.step(to=, principal=)` | `fn:routeToPrincipal` | keywords, free-function |
| `ScatterOutcome` | `ScatterOutcome` | `ScatterOutcome` |  |
| `ScatterOutcome.agent` | `ScatterOutcome.agent` | `ScatterOutcome.agent` | keywords |
| `ScatterOutcome.result` | `ScatterOutcome.result` | `ScatterOutcome.result` | keywords |
| `ScatterReport` | `ScatterReport` | `ScatterReport` |  |
| `ScatterReport.outcomes` | `ScatterReport.outcomes` | `ScatterReport.outcomes` |  |
| `ScatterReport::completed` | `ScatterReport.completed` | `ScatterReport.completed` |  |
| `ScatterReport::failures` | `ScatterReport.failures` | `ScatterReport.failures` |  |
| `Session` | `Session` | `Session` |  |
| `Session::append` | `Session.append` | `Session.append` |  |
| `Session::checkpoint` | `Session.checkpoint` | `Session.checkpoint` |  |
| `Session::config` | `Session.config` | `Session.config` | property |
| `Session::context` | `Session.context` | `Session.context` |  |
| `Session::context_with` | `Session.context_with` | `Session.contextWith` |  |
| `Session::conversation` | `Session.conversation` | `Session.conversation` | property |
| `Session::graph` | `Session.graph` | `Session.graph` |  |
| `Session::memory` | `Session.memory` | `Session.memory` |  |
| `Session::memory_in` | `Session.memory_in` | `Session.memory(namespace=)` | overload |
| `Session::replay` | `Session.replay` | `Session.replay` |  |
| `Session::scope` | `Session.scope` | `Session.scope` | property |
| `Session::state_at` | `Session.state_at` | `Session.stateAt` |  |
| `Session::turns_at` | `Session.turns_at` | `Session.turnsAt` |  |
| `Session::turns_since` | `Session.turns_since` | `Session.turnsSince` |  |
| `SessionConfig` | `SessionConfig` | `SessionConfig` |  |
| `SessionConfig::context_token_bound` | `SessionConfig.context_token_bound` | `SessionConfig.contextTokenBound` | property |
| `SessionConfig::context_tokens` | `Laser.sessions(context_tokens=)` | `SessionConfig.contextTokens` | keywords |
| `SessionConfig::context_turn_bound` | `SessionConfig.context_turn_bound` | `SessionConfig.contextTurnBound` | property |
| `SessionConfig::context_turns` | `Laser.sessions(context_turns=)` | `SessionConfig.contextTurns` | keywords |
| `SessionConfig::kind_for` | `SessionConfig.kind_for` | `SessionConfig.kindFor` |  |
| `SessionConfig::memory_namespace` | `Laser.sessions(memory_namespace=)` | `SessionConfig.memoryNamespace` | keywords |
| `SessionConfig::memory_namespace_name` | `SessionConfig.memory_namespace_name` | `SessionConfig.memoryNamespaceName` | property |
| `SessionConfig::new` | `Laser.sessions` | `new SessionConfig()` | keywords, constructor |
| `SessionConfig::stream` | `Laser.sessions(stream=)` | `SessionConfig.stream` | keywords |
| `SessionConfig::stream_name` | `SessionConfig.stream_name` | `SessionConfig.streamName` | property |
| `SessionConfig::topic` | `Laser.sessions(topics=)` | `SessionConfig.topic` | keywords |
| `SessionConfig::topic_for` | `SessionConfig.topic_for` | `SessionConfig.topicFor` |  |
| `SessionConfig::topics` | `SessionConfig.topics` | `SessionConfig.topics` | property |
| `SessionPolicy` | omitted | `SessionPolicy` | plain-value |
| `SessionPolicy::PerCall` | omitted | `SessionPolicy` | plain-value |
| `SessionPolicy::PerUser` | omitted | `SessionPolicy` | plain-value |
| `SessionPolicy::conversation_for` | `fn:session_policy_conversation_for` | `fn:conversationFor` | free-function |
| `SessionTurn` | `SessionTurn` | `SessionTurn` |  |
| `SessionTurn.kind` | `SessionTurn.kind` | `SessionTurn.kind` | keywords |
| `SessionTurn.message` | `SessionTurn.message` | `SessionTurn.message` | keywords |
| `SessionTurn::text` | `SessionTurn.text` | `fn:sessionTurnText` | free-function |
| `SessionTurnKind` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::Instruction` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::Response` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::ModelResponse` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::ToolCall` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::ToolResult` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::HumanInput` | omitted | `SessionTurnKind` | plain-value |
| `SessionTurnKind::for_topic` | `Sessions.turn_kind` | `fn:sessionTurnKind` | free-function |
| `SessionTurnKind::topic` | `Sessions.turn_topic` | `fn:sessionTurnTopic` | free-function |
| `Sessions` | `Sessions` | `Sessions` |  |
| `Sessions::config` | `Sessions.config` | `Sessions.config` | property |
| `Sessions::create` | `Sessions.create` | `Sessions.create` |  |
| `Sessions::open` | `Sessions.open` | `Sessions.open` |  |
| `Sessions::start` | `Sessions.start` | `Sessions.start` |  |
| `SlidingWindow` | `Laser.spawn_agent(dedup_window=)` | `SlidingWindow` | keywords |
| `SlidingWindow::new` | `Laser.spawn_agent` | `new SlidingWindow()` | keywords, constructor |
| `StepContext` | `Workflow.step(build=)` | `StepContext` | callback |
| `StepContext.outputs` | `Workflow.step(build=)` | `StepContext.outputs` | callback, keywords |
| `StepFn` | `Workflow.step(build=)` | `StepFn` | callback |
| `StepFn::build` | `Workflow.step(build=)` | `StepFn` | callback |
| `StepHandle` | `Workflow.step` | `StepHandle` | keywords |
| `StepHandle::after` | `Workflow.step(after=)` | `StepHandle.after` | keywords |
| `StepHandle::budget` | `Workflow.budget` | `StepHandle.budget` | native-binding |
| `StepHandle::compensate_with` | `Workflow.step(compensate=)` | `StepHandle.compensateWith` | keywords |
| `StepHandle::exclusive` | `Workflow.step(exclusive=)` | `StepHandle.exclusive` | keywords |
| `StepHandle::exclusive_in` | `Workflow.step(fence_namespace=)` | `StepHandle.exclusiveIn` | keywords |
| `StepHandle::inbox_route` | `Laser.workflow(fixed_inbox=)` | `StepHandle.inboxRoute` | keywords |
| `StepHandle::on_timeout` | `Workflow.step(on_timeout=)` | `StepHandle.onTimeout` | keywords |
| `StepHandle::registered` | `Workflow.registered` | `StepHandle.registered` | native-binding |
| `StepHandle::run` | `Workflow.step` | `StepHandle.run` | one-call |
| `StepHandle::run_id` | `Workflow.run_id` | `StepHandle.runId` | native-binding |
| `StepHandle::step` | `Workflow.step` | `StepHandle.step` | keywords |
| `StepHandle::verify_with` | `Workflow.step(verify=)` | `StepHandle.verifyWith` | keywords |
| `StreamEvent` | dict via `ChunkAssembler.feed` | `StreamEvent` | serde-dict |
| `StreamEvent::Body` | dict key | `StreamEvent` | serde-dict, plain-value |
| `StreamEvent::Finished` | dict key | `StreamEvent` | serde-dict, plain-value |
| `StreamEvent::Failed` | dict key | `StreamEvent` | serde-dict, plain-value |
| `SystemClock` | `SystemClock` | `SystemClock` |  |
| `TestClock` | `TestClock` | `TestClock` |  |
| `TestClock::advance` | `TestClock.advance` | `TestClock.advance` |  |
| `TestClock::new` | `new TestClock()` | `new TestClock()` | constructor |
| `TestClock::set` | `TestClock.set` | `TestClock.set` |  |
| `Verifier` | `Workflow.step(verify=)` | `Verifier` | callback |
| `Verifier::verify` | `Workflow.step(verify=)` | `Verifier` | callback |
| `agent::WORKFLOW_FENCE_NAMESPACE` | `const:WORKFLOW_FENCE_NAMESPACE` | `const:WORKFLOW_FENCE_NAMESPACE` |  |
| `Workflow` | `Workflow` | `Workflow` |  |
| `Workflow::budget` | `Workflow.budget` | `Workflow.budget` |  |
| `Workflow::inbox_route` | `Laser.workflow(fixed_inbox=)` | `Workflow.inboxRoute` | keywords |
| `Workflow::registered` | `Workflow.registered` | `Workflow.registered` |  |
| `Workflow::run` | `Workflow.run` | `Workflow.run` |  |
| `Workflow::run_id` | `Workflow.run_id` | `Workflow.runId` |  |
| `Workflow::step` | `Workflow.step` | `Workflow.step` | keywords |
| `WorkflowOutcome` | `WorkflowOutcome` | `WorkflowOutcome` |  |
| `WorkflowOutcome.outputs` | `WorkflowOutcome.outputs` | `WorkflowOutcome.outputs` | keywords |
| `WorkflowOutcome.run_id` | `WorkflowOutcome.run_id` | `WorkflowOutcome.runId` | keywords |
| `agent::resume_offsets` | `fn:resume_offsets` | `fn:resumeOffsets` |  |

## agui

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgUiEvent` | dict via `Laser.agui_events` | `AgUiEvent` | serde-dict |
| `AgUiEvent::RunStarted` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::RunFinished` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::TextMessageStart` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::TextMessageContent` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::TextMessageEnd` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ReasoningMessageStart` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ReasoningMessageContent` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ReasoningMessageEnd` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ToolCallStart` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ToolCallArgs` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ToolCallEnd` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::ToolCallResult` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::StateSnapshot` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::StateDelta` | dict key | `AgUiEvent` | serde-dict, plain-value |
| `AgUiEvent::RunError` | dict key | `AgUiEvent` | serde-dict, plain-value |

## batching

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BatchingProducer` | `BatchingProducer` | `BatchingProducer` |  |
| `BatchingProducer::close` | `BatchingProducer.close` | `BatchingProducer.close` |  |
| `BatchingProducer::flush` | `BatchingProducer.flush` | `BatchingProducer.flush` |  |
| `BatchingProducer::send` | `BatchingProducer.send` | `BatchingProducer.send` |  |
| `BatchingProducerBuilder` | `Topic.batching` | `BatchingProducerBuilder` | keywords |
| `BatchingProducerBuilder::build` | `Topic.batching` | `BatchingProducerBuilder.build` | one-call |
| `BatchingProducerBuilder::linger` | `Topic.batching(linger_ms=)` | `BatchingProducerBuilder.linger` | keywords |
| `BatchingProducerBuilder::max_bytes` | `Topic.batching(max_bytes=)` | `BatchingProducerBuilder.maxBytes` | keywords |
| `BatchingProducerBuilder::max_records` | `Topic.batching(max_records=)` | `BatchingProducerBuilder.maxRecords` | keywords |
| `BatchingProducerBuilder::partition_key` | `Topic.batching(partition_key=)` | `BatchingProducerBuilder.partitionKey` | keywords |
| `batching::DEFAULT_LINGER` | `const:DEFAULT_LINGER_MS` | `const:DEFAULT_LINGER_MS` |  |
| `batching::DEFAULT_MAX_BYTES` | `const:DEFAULT_MAX_BYTES` | `const:DEFAULT_MAX_BYTES` |  |
| `batching::DEFAULT_MAX_RECORDS` | `const:DEFAULT_MAX_RECORDS` | `const:DEFAULT_MAX_RECORDS` |  |
| `batching::MIN_LINGER` | `const:MIN_LINGER_MS` | `const:MIN_LINGER_MS` |  |

## blob

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BlobStore` | `PublishRequest.claim_check(store=)` | `BlobStore` | callback |
| `BlobStore::put` | `PublishRequest.claim_check(store=)` | `BlobStore.put` | callback, keywords |
| `BlobStore::get` | `PublishRequest.claim_check(store=)` | `BlobStore.get` | callback, keywords |
| `blob::check_in` | `fn:check_in` | `fn:checkIn` |  |
| `blob::resolve_body` | `fn:resolve_body` | `fn:resolveBody` |  |

## capabilities

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `BackendDescriptor` | `BackendDescriptor` | `BackendDescriptor` |  |
| `BackendDescriptor.descriptor_version` | `BackendDescriptor.descriptor_version` | `BackendDescriptor.descriptorVersion` | keywords |
| `BackendDescriptor.resource_id` | `BackendDescriptor.resource_id` | `BackendDescriptor.resourceId` | keywords |
| `BackendDescriptor.mode` | `BackendDescriptor.mode` | `BackendDescriptor.mode` | keywords |
| `BackendDescriptor.label` | `BackendDescriptor.label` | `BackendDescriptor.label` | keywords |
| `BackendDescriptor.implementation` | `BackendDescriptor.implementation` | `BackendDescriptor.implementation` | keywords |
| `BackendDescriptor.observed_backend_generation` | `BackendDescriptor.observed_backend_generation` | `BackendDescriptor.observedBackendGeneration` | keywords |
| `BackendDescriptor.runtime_configuration_revision` | `BackendDescriptor.runtime_configuration_revision` | `BackendDescriptor.runtimeConfigurationRevision` | keywords |
| `BackendDescriptor.desired_state` | `BackendDescriptor.desired_state` | `BackendDescriptor.desiredState` | keywords |
| `BackendDescriptor.observed_state` | `BackendDescriptor.observed_state` | `BackendDescriptor.observedState` | keywords |
| `BackendDescriptor.readiness` | `BackendDescriptor.readiness` | `BackendDescriptor.readiness` | keywords |
| `BackendDescriptor.materialization` | `BackendDescriptor.materialization` | `BackendDescriptor.materialization` | keywords |
| `BackendDescriptor.query` | `BackendDescriptor.query` | `BackendDescriptor.query` | keywords |
| `BackendDescriptor.schema` | `BackendDescriptor.schema` | `BackendDescriptor.schema` | keywords |
| `BackendDescriptor.maintenance` | `BackendDescriptor.maintenance` | `BackendDescriptor.maintenance` | keywords |
| `BackendDescriptor.limits` | `BackendDescriptor.limits` | `BackendDescriptor.limits` | keywords |
| `BackendDescriptor::new` | `new BackendDescriptor()` | `BackendDescriptor` | constructor, keywords |
| `BackendDescriptor::with_limits` | `BackendDescriptor.with_limits` | `BackendDescriptor.limits` | keywords |
| `BackendDescriptor::with_maintenance` | `BackendDescriptor.with_maintenance` | `BackendDescriptor.maintenance` | keywords |
| `BackendDescriptor::with_materialization` | `BackendDescriptor.with_materialization` | `BackendDescriptor.materialization` | keywords |
| `BackendDescriptor::with_query` | `BackendDescriptor.with_query` | `BackendDescriptor.query` | keywords |
| `BackendDescriptor::with_schema` | `BackendDescriptor.with_schema` | `BackendDescriptor.schema` | keywords |
| `BackendDescriptor::with_state` | `BackendDescriptor.with_state` | `BackendDescriptor.desiredState` | keywords |
| `BackendDesiredState` | omitted | `wire.BackendDesiredState` | plain-value |
| `BackendDesiredState::Disabled` | omitted | `wire.BackendDesiredState` | plain-value |
| `BackendDesiredState::Enabled` | omitted | `wire.BackendDesiredState` | plain-value |
| `BackendImplementation` | dict via `BackendDescriptor.implementation` | `wire.BackendImplementation` | serde-dict |
| `BackendImplementation.kind` | dict key | `wire.BackendImplementation.kind` | serde-dict, keywords |
| `BackendImplementation.version` | dict key | `wire.BackendImplementation.version` | serde-dict, keywords |
| `BackendLimits` | dict via `BackendDescriptor.limits` | `wire.BackendLimits` | serde-dict |
| `BackendLimits.max_query_rows` | dict key | `wire.BackendLimits.maxQueryRows` | serde-dict, keywords |
| `BackendLimits.max_query_bytes` | dict key | `wire.BackendLimits.maxQueryBytes` | serde-dict, keywords |
| `BackendLimits.max_scan_bytes` | dict key | `wire.BackendLimits.maxScanBytes` | serde-dict, keywords |
| `BackendLimits.max_query_micros` | dict key | `wire.BackendLimits.maxQueryMicros` | serde-dict, keywords |
| `BackendLimits.max_concurrent_queries` | dict key | `wire.BackendLimits.maxConcurrentQueries` | serde-dict, keywords |
| `BackendLimits.max_schema_fields` | dict key | `wire.BackendLimits.maxSchemaFields` | serde-dict, keywords |
| `BackendLimits.max_materialization_file_bytes` | dict key | `wire.BackendLimits.maxMaterializationFileBytes` | serde-dict, keywords |
| `BackendMode` | omitted | `wire.BackendMode` | plain-value |
| `BackendMode::Operational` | omitted | `wire.BackendMode` | plain-value |
| `BackendMode::Lakehouse` | omitted | `wire.BackendMode` | plain-value |
| `BackendObservedState` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendObservedState::Disabled` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendObservedState::Starting` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendObservedState::Ready` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendObservedState::Degraded` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendObservedState::Unavailable` | omitted | `wire.BackendObservedState` | plain-value |
| `BackendReadiness` | dict via `BackendDescriptor.readiness` | `BackendReadiness` | serde-dict |
| `BackendReadiness.ready` | dict key | `BackendReadiness.ready` | serde-dict, keywords |
| `BackendReadiness.reasons` | dict key | `BackendReadiness.reasons` | serde-dict, keywords |
| `BackendReadiness.observed_at_micros` | dict key | `BackendReadiness.observedAtMicros` | serde-dict, keywords |
| `BackendReadiness::not_ready` | `fn:backend_readiness_not_ready` | `fn:backendReadinessNotReady` | free-function |
| `BackendReadiness::ready` | `fn:backend_readiness_ready` | `BackendReadiness.ready` | free-function, keywords |
| `BackendReadinessCode` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::Disabled` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::ConfigurationPending` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::ConfigurationRejected` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::CredentialUnavailable` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::ObjectStoreUnavailable` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::CatalogUnavailable` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::QueryRuntimeUnavailable` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::GenerationMismatch` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessCode::ProbeFailed` | omitted | `wire.BackendReadinessCode` | plain-value |
| `BackendReadinessReason` | dict via `BackendDescriptor.readiness` | `BackendReadinessReason` | serde-dict |
| `BackendReadinessReason.code` | dict key | `BackendReadinessReason.code` | serde-dict, keywords |
| `BackendReadinessReason.detail` | dict key | `BackendReadinessReason.detail` | serde-dict, keywords |
| `BackendResourceId` | omitted | `wire.BackendResourceId` | plain-value |
| `BackendResourceId::as_u128` | omitted | `wire.BackendResourceId.asU128` | plain-value |
| `BackendResourceId::from_bytes` | omitted | `wire.BackendResourceId.fromBytes` | plain-value |
| `BackendResourceId::from_u128` | omitted | `wire.BackendResourceId.fromU128` | plain-value |
| `BackendResourceId::to_bytes` | omitted | `wire.BackendResourceId.toBytes` | plain-value |
| `Capabilities` | `Capabilities` | `Capabilities` |  |
| `Capabilities.managed` | `Capabilities.managed` | `Capabilities.managed` | keywords |
| `Capabilities.query` | `Capabilities.query` | `Capabilities.query` | keywords |
| `Capabilities.destinations` | `Capabilities.destinations` | `Capabilities.destinations` | keywords |
| `Capabilities.kv` | `Capabilities.kv` | `Capabilities.kv` | keywords |
| `Capabilities.graph` | `Capabilities.graph` | `Capabilities.graph` | keywords |
| `Capabilities.forks` | `Capabilities.forks` | `Capabilities.forks` | keywords |
| `Capabilities.a2a_gateway` | `Capabilities.a2a_gateway` | `Capabilities.a2aGateway` | keywords |
| `Capabilities.agent_workflow` | `Capabilities.agent_workflow` | `Capabilities.agentWorkflow` | keywords |
| `Capabilities.watch` | `Capabilities.watch` | `Capabilities.watch` | keywords |
| `Capabilities.authz` | `Capabilities.authz` | `Capabilities.authz` | keywords |
| `Capabilities.filters` | `Capabilities.filters` | `Capabilities.filters` | keywords |
| `Capabilities.versions` | `Capabilities.versions` | `Capabilities.versions` | keywords |
| `Capabilities.backends` | `Capabilities.backends` | `Capabilities.backends` | keywords |
| `Capabilities.hello` | `Capabilities.hello` | `Capabilities.hello` | keywords |
| `Capabilities::OPEN` | `Capabilities.OPEN` | `const:OPEN_CAPABILITIES` | free-function |
| `Capabilities::backend` | `Capabilities.backend` | `fn:backend` | free-function |
| `Capabilities::enabled_backends` | `Capabilities.enabled_backends` | `fn:enabledBackends` | free-function |
| `Capabilities::is_open_only` | `Capabilities.is_open_only` | `fn:isOpenOnly` | free-function |
| `Capabilities::is_ready` | `Capabilities.is_ready` | `fn:isReady` | free-function |
| `Capabilities::readiness_reasons` | `Capabilities.readiness_reasons` | `fn:readinessReasons` | free-function |
| `Capabilities::serves_consistency` | `Capabilities.serves_consistency` | `fn:servesConsistency` | free-function |
| `Capabilities::unready_backends` | `Capabilities.unready_backends` | `fn:unreadyBackends` | free-function |
| `Capabilities::with_a2a_gateway` | `Laser.with_capabilities(a2a_gateway=)` | `Capabilities.a2aGateway` | keywords |
| `Capabilities::with_agent_workflow` | `Laser.with_capabilities(agent_workflow=)` | `Capabilities.agentWorkflow` | keywords |
| `Capabilities::with_backends` | `Laser.with_capabilities(backends=)` | `Capabilities.backends` | keywords |
| `Capabilities::with_destination_consistency` | `Laser.with_capabilities(destinations_consistency=)` | `DestinationCaps.consistency` | keywords |
| `Capabilities::with_destinations` | `Laser.with_capabilities(destinations=)` | `DestinationCaps.available` | keywords |
| `Capabilities::with_filters` | `Laser.with_capabilities(filters=, filters_catalog=)` | `FilterCaps.native` | keywords |
| `Capabilities::with_forks` | `Laser.with_capabilities(forks=)` | `Capabilities.forks` | keywords |
| `Capabilities::with_graph` | `Laser.with_capabilities(graph=)` | `Capabilities.graph` | keywords |
| `Capabilities::with_kv` | `Laser.with_capabilities(kv=)` | `KvCaps.available` | keywords |
| `Capabilities::with_kv_cas` | `Laser.with_capabilities(kv_cas=)` | `KvCaps.cas` | keywords |
| `Capabilities::with_kv_cas_fenced` | `Laser.with_capabilities(kv_cas_fenced=)` | `KvCaps.casFenced` | keywords |
| `Capabilities::with_kv_fenced_leases` | `Laser.with_capabilities(kv_fenced_leases=)` | `KvCaps.fencedLeases` | keywords |
| `Capabilities::with_managed` | `Laser.with_capabilities(managed=)` | `Capabilities.managed` | keywords |
| `Capabilities::with_query` | `Laser.with_capabilities(query=)` | `QueryCaps.available` | keywords |
| `Capabilities::with_query_consistency` | `Laser.with_capabilities(query_consistency=)` | `QueryCaps.consistency` | keywords |
| `Capabilities::with_query_execution` | `Laser.with_capabilities(query_execution=)` | `QueryCaps.cursorPaging` | keywords |
| `Capabilities::with_query_keyword` | `Laser.with_capabilities(query_keyword=)` | `QueryCaps.keyword` | keywords |
| `Capabilities::with_versions` | `Laser.with_capabilities(versions=)` | `Capabilities.versions` | keywords |
| `DestinationCaps` | `DestinationCaps` | `DestinationCaps` |  |
| `DestinationCaps.available` | `DestinationCaps.available` | `DestinationCaps.available` | keywords |
| `DestinationCaps.consistency` | `DestinationCaps.consistency` | `DestinationCaps.consistency` | keywords |
| `FilterAnnounce` | `FilterAnnounce` | `FilterAnnounce` |  |
| `FilterAnnounce.evaluator_version` | `FilterAnnounce.evaluator_version` | `FilterAnnounce.evaluatorVersion` | keywords |
| `FilterAnnounce.codecs` | `FilterAnnounce.codecs` | `FilterAnnounce.codecs` | keywords |
| `FilterAnnounce::evaluates` | `FilterAnnounce.evaluates` | `fn:filterAnnounceEvaluates` | free-function |
| `FilterAnnounce::served` | `FilterAnnounce.served` | `fn:filterAnnounceServed` | free-function |
| `FilterCaps` | `FilterCaps` | `FilterCaps` |  |
| `FilterCaps.native` | `FilterCaps.native` | `FilterCaps.native` | keywords |
| `FilterCaps.catalog` | `FilterCaps.catalog` | `FilterCaps.catalog` | keywords |
| `FilterCaps.group_policy_reads` | `FilterCaps.group_policy_reads` | `FilterCaps.groupPolicyReads` | keywords |
| `FilterCaps.evaluation` | `FilterCaps.evaluation` | `FilterCaps.evaluation` | keywords |
| `FilterCaps::evaluates` | `FilterCaps.evaluates` | `fn:filterCapsEvaluates` | free-function |
| `HelloOutcome` | omitted | `HelloOutcome` | plain-value |
| `HelloOutcome::Unknown` | omitted | `HelloOutcome` | plain-value |
| `HelloOutcome::Answered` | omitted | `HelloOutcome` | plain-value |
| `HelloOutcome::Rejected` | omitted | `HelloOutcome` | plain-value |
| `HelloOutcome::Failed` | omitted | `HelloOutcome` | plain-value |
| `KvCaps` | `KvCaps` | `KvCaps` |  |
| `KvCaps.available` | `KvCaps.available` | `KvCaps.available` | keywords |
| `KvCaps.cas` | `KvCaps.cas` | `KvCaps.cas` | keywords |
| `KvCaps.cas_fenced` | `KvCaps.cas_fenced` | `KvCaps.casFenced` | keywords |
| `KvCaps.fenced_leases` | `KvCaps.fenced_leases` | `KvCaps.fencedLeases` | keywords |
| `MaintenanceCapabilities` | dict via `BackendDescriptor.maintenance` | `wire.MaintenanceCapabilities` | serde-dict |
| `MaintenanceCapabilities.expire_snapshots` | dict key | `wire.MaintenanceCapabilities.expireSnapshots` | serde-dict, keywords |
| `MaintenanceCapabilities.remove_orphan_files` | dict key | `wire.MaintenanceCapabilities.removeOrphanFiles` | serde-dict, keywords |
| `MaintenanceCapabilities.compact_data_files` | dict key | `wire.MaintenanceCapabilities.compactDataFiles` | serde-dict, keywords |
| `MaterializationCapability` | dict via `BackendDescriptor.materialization` | `wire.MaterializationCapability` | serde-dict |
| `MaterializationCapability.file_format` | dict key | `wire.MaterializationCapability.fileFormat` | serde-dict, keywords |
| `MaterializationCapability.table_format` | dict key | `wire.MaterializationCapability.tableFormat` | serde-dict, keywords |
| `MaterializationCapability.create_table` | dict key | `wire.MaterializationCapability.createTable` | serde-dict, keywords |
| `MaterializationCapability.append` | dict key | `wire.MaterializationCapability.append` | serde-dict, keywords |
| `OpVersions` | `OpVersions` | `OpVersions` |  |
| `OpVersions.query` | `OpVersions.query` | `OpVersions.query` | keywords |
| `OpVersions.control` | `OpVersions.control` | `OpVersions.control` | keywords |
| `OpVersions.kv` | `OpVersions.kv` | `OpVersions.kv` | keywords |
| `OpVersions.fork` | `OpVersions.fork` | `OpVersions.fork` | keywords |
| `OpVersions.agent` | `OpVersions.agent` | `OpVersions.agent` | keywords |
| `OpVersions.graph` | `OpVersions.graph` | `OpVersions.graph` | keywords |
| `OpVersions.checkpoint` | `OpVersions.checkpoint` | `OpVersions.checkpoint` | keywords |
| `OpVersions.filter` | `OpVersions.filter` | `OpVersions.filter` | keywords |
| `OpVersions.features` | `OpVersions.features` | `OpVersions.features` | keywords |
| `OpVersions::has_feature` | `OpVersions.has_feature` | `fn:opVersionsHasFeature` | free-function |
| `OpVersions::new` | `new OpVersions()` | `OpVersions` | constructor, keywords |
| `OpVersions::with_agent` | `OpVersions.with_agent` | `OpVersions.agent` | keywords |
| `OpVersions::with_checkpoint` | `OpVersions.with_checkpoint` | `OpVersions.checkpoint` | keywords |
| `OpVersions::with_features` | `OpVersions.with_features` | `OpVersions.features` | keywords |
| `OpVersions::with_filter` | `OpVersions.with_filter` | `OpVersions.filter` | keywords |
| `OpVersions::with_graph` | `OpVersions.with_graph` | `OpVersions.graph` | keywords |
| `QueryCapabilities` | dict via `BackendDescriptor.query` | `wire.QueryCapabilities` | serde-dict |
| `QueryCapabilities.dialects` | dict key | `wire.QueryCapabilities.dialects` | serde-dict, keywords |
| `QueryCapabilities.time_travel` | dict key | `wire.QueryCapabilities.timeTravel` | serde-dict, keywords |
| `QueryCapabilities.consistency` | dict key | `wire.QueryCapabilities.consistency` | serde-dict, keywords |
| `QueryCapabilities.logical_types` | dict key | `wire.QueryCapabilities.logicalTypes` | serde-dict, keywords |
| `QueryCapabilities.paging` | dict key | `wire.QueryCapabilities.paging` | serde-dict, keywords |
| `QueryCapabilities.cancellation` | dict key | `wire.QueryCapabilities.cancellation` | serde-dict, keywords |
| `QueryCapabilities.execution_status` | dict key | `wire.QueryCapabilities.executionStatus` | serde-dict, keywords |
| `QueryCapabilities.raw_sql` | dict key | `wire.QueryCapabilities.rawSql` | serde-dict, keywords |
| `QueryCaps` | `QueryCaps` | `QueryCaps` |  |
| `QueryCaps.available` | `QueryCaps.available` | `QueryCaps.available` | keywords |
| `QueryCaps.consistency` | `QueryCaps.consistency` | `QueryCaps.consistency` | keywords |
| `QueryCaps.keyword` | `QueryCaps.keyword` | `QueryCaps.keyword` | keywords |
| `QueryCaps.cursor_paging` | `QueryCaps.cursor_paging` | `QueryCaps.cursorPaging` | keywords |
| `QueryCaps.cancellation` | `QueryCaps.cancellation` | `QueryCaps.cancellation` | keywords |
| `QueryCaps.execution_status` | `QueryCaps.execution_status` | `QueryCaps.executionStatus` | keywords |
| `QueryPagingCapability` | omitted | `wire.QueryPagingCapability` | plain-value |
| `QueryPagingCapability::Offset` | omitted | `wire.QueryPagingCapability` | plain-value |
| `QueryPagingCapability::Cursor` | omitted | `wire.QueryPagingCapability` | plain-value |
| `SchemaCapabilities` | dict via `BackendDescriptor.schema` | `wire.SchemaCapabilities` | serde-dict |
| `SchemaCapabilities.logical_schema` | dict key | `wire.SchemaCapabilities.logicalSchema` | serde-dict, keywords |
| `SchemaCapabilities.arrow_ipc_stream` | dict key | `wire.SchemaCapabilities.arrowIpcStream` | serde-dict, keywords |
| `SchemaCapabilities.schema_evolution` | dict key | `wire.SchemaCapabilities.schemaEvolution` | serde-dict, keywords |
| `TimeTravelCapability` | omitted | `wire.TimeTravelCapability` | plain-value |
| `TimeTravelCapability::SnapshotId` | omitted | `wire.TimeTravelCapability` | plain-value |
| `TimeTravelCapability::TimestampMicros` | omitted | `wire.TimeTravelCapability` | plain-value |

## context

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `context::CONTEXT_READ_WINDOW` | `const:CONTEXT_READ_WINDOW` | `const:CONTEXT_READ_WINDOW` |  |
| `Chain` | `Chain` | `Chain` |  |
| `Checkpoint` | `Checkpoint` | `Checkpoint` |  |
| `Checkpoint::is_empty` | `Checkpoint.is_empty` | `Checkpoint.isEmpty` |  |
| `Checkpoint::topic_offsets` | `Checkpoint.topic_offsets` | `Checkpoint.topicOffsets` |  |
| `ContextAssembler` | `Laser.assemble_context` | `ContextAssembler` | keywords |
| `ContextAssembler::assemble` | `Laser.assemble_context` | `ContextAssembler.assemble` | one-call |
| `ContextAssembler::builder` | `Laser.assemble_context` | `ContextAssembler.builder` | keywords |
| `ContextAssemblerBuilder` | `Laser.assemble_context` | `ContextAssemblerBuilder` | keywords |
| `ContextAssemblerBuilder::across_subconversations` | `Laser.assemble_context(across_subconversations=)` | `ContextAssemblerBuilder.acrossSubconversations` | keywords |
| `ContextAssemblerBuilder::build` | `Laser.assemble_context` | `ContextAssemblerBuilder.build` | one-call |
| `ContextAssemblerBuilder::conversation_id` | `Laser.assemble_context(conversation_id=)` | `ContextAssemblerBuilder.conversationId` | keywords |
| `ContextAssemblerBuilder::from_checkpoint` | `Laser.assemble_context(from_checkpoint=)` | `ContextAssemblerBuilder.fromCheckpoint` | keywords |
| `ContextAssemblerBuilder::from_offsets` | `Laser.assemble_context(from_offsets=)` | `ContextAssemblerBuilder.fromOffsets` | keywords |
| `ContextAssemblerBuilder::maybe_across_subconversations` | `Laser.assemble_context(across_subconversations=)` | `ContextAssemblerBuilder.acrossSubconversations` | keywords |
| `ContextAssemblerBuilder::maybe_from_checkpoint` | `Laser.assemble_context(from_checkpoint=)` | `ContextAssemblerBuilder.fromCheckpoint` | keywords |
| `ContextAssemblerBuilder::maybe_from_offsets` | `Laser.assemble_context(from_offsets=)` | `ContextAssemblerBuilder.fromOffsets` | keywords |
| `ContextAssemblerBuilder::maybe_policy` | `Laser.assemble_context(policy=)` | `ContextAssemblerBuilder.policy` | keywords |
| `ContextAssemblerBuilder::maybe_to_checkpoint` | `Laser.assemble_context(to_checkpoint=)` | `ContextAssemblerBuilder.toCheckpoint` | keywords |
| `ContextAssemblerBuilder::maybe_topics` | `Laser.assemble_context(topics=)` | `ContextAssemblerBuilder.topics` | keywords |
| `ContextAssemblerBuilder::policy` | `Laser.assemble_context(policy=)` | `ContextAssemblerBuilder.policy` | keywords |
| `ContextAssemblerBuilder::to_checkpoint` | `Laser.assemble_context(to_checkpoint=)` | `ContextAssemblerBuilder.toCheckpoint` | keywords |
| `ContextAssemblerBuilder::topics` | `Laser.assemble_context(topics=)` | `ContextAssemblerBuilder.topics` | keywords |
| `ContextMessage` | `ContextMessage` | `ContextMessage` |  |
| `ContextMessage.id` | `ContextMessage.id` | `ContextMessage.id` | keywords |
| `ContextMessage.provenance` | `ContextMessage.provenance` | `ContextMessage.provenance` | keywords |
| `ContextMessage.payload` | `ContextMessage.payload` | `ContextMessage.payload` | keywords |
| `ContextMessage.envelope` | `ContextMessage.envelope` | `ContextMessage.envelope` | keywords |
| `ContextMessage.topic` | `ContextMessage.topic` | `ContextMessage.topic` | keywords |
| `ContextPolicy` | `Laser.assemble_context(policy=)` | `ContextPolicy` | callback |
| `ContextPolicy::select` | `Laser.assemble_context(policy=)` | `ContextPolicy.select` | callback, keywords |
| `LastN` | `LastN` | `LastN` |  |
| `RoleFilter` | `RoleFilter` | `RoleFilter` |  |
| `TokenBudget` | `TokenBudget` | `TokenBudget` |  |
| `TokenBudget::new` | `new TokenBudget()` | `new TokenBudget()` | constructor |
| `TokenBudget::with_estimator` | `new TokenBudget(estimator=)` | `new TokenBudget(estimate=)` | overload |
| `context::checkpoint` | `fn:context_checkpoint` | `fn:contextCheckpoint` | flat-namespace |

## context_scope

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ContextScope` | `ContextScope` | `ContextScope` |  |
| `ContextScope::append` | `ContextScope.append` | `ContextScope.append` |  |
| `ContextScope::block` | `ContextScope.block` | `ContextScope.block` |  |
| `ContextScope::checkpoint` | `ContextScope.checkpoint` | `ContextScope.checkpoint` |  |
| `ContextScope::conversation` | `ContextScope.conversation` | `ContextScope.conversation` | property |
| `ContextScope::fetch` | `ContextScope.fetch` | `ContextScope.fetch` |  |
| `ContextScope::fetch_with` | `ContextScope.fetch_with` | `ContextScope.fetchWith` |  |
| `ContextScope::graph` | `ContextScope.graph` | `ContextScope.graph` |  |
| `ContextScope::laser` | `ContextScope.laser` | `ContextScope.laser` | property |
| `ContextScope::memory` | `ContextScope.memory` | `ContextScope.memory` |  |
| `ContextScope::memory_with` | `ContextScope.memory_with` | `ContextScope.memory` | overload |
| `ContextScope::state` | `ContextScope.state` | `ContextScope.state` |  |
| `ContextScope::state_with` | `ContextScope.state_with` | `ContextScope.stateWith` |  |
| `ScopedMemory` | `ScopedMemory` | `ScopedMemory` |  |
| `ScopedMemory::block` | `ScopedMemory.block` | `ScopedMemory.block` |  |
| `ScopedMemory::consolidate` | `ScopedMemory.consolidate` | `ScopedMemory.consolidate` |  |
| `ScopedMemory::conversation` | `ScopedMemory.conversation` | `ScopedMemory.conversation` | property |
| `ScopedMemory::forget` | `ScopedMemory.forget` | `ScopedMemory.forget` |  |
| `ScopedMemory::handle` | `ScopedMemory.handle` | `ScopedMemory.handle` | property |
| `ScopedMemory::improve` | `ScopedMemory.improve` | `ScopedMemory.improve` |  |
| `ScopedMemory::recall` | `ScopedMemory.recall` | `ScopedMemory.recall` |  |
| `ScopedMemory::remember` | `ScopedMemory.remember` | `ScopedMemory.remember` |  |
| `ScopedMemory::search` | `ScopedMemory.search` | `ScopedMemory.search` |  |

## crash_context

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentDeadLetter` | dict via `Laser.redrive_dead_letter` | `AgentDeadLetter` | serde-dict |
| `AgentDeadLetter.source` | dict key | `AgentDeadLetter.source` | serde-dict, keywords |
| `AgentDeadLetter.reason` | dict key | `AgentDeadLetter.reason` | serde-dict, keywords |
| `AgentDeadLetter.attempts` | dict key | `AgentDeadLetter.attempts` | serde-dict, keywords |
| `AgentDeadLetter.detail` | dict key | `AgentDeadLetter.detail` | serde-dict, keywords |
| `AgentDeadLetter.payload` | dict key | `AgentDeadLetter.payload` | serde-dict, keywords |
| `CrashContext` | `CrashContext` | `CrashContext` |  |
| `CrashContext.journal` | `CrashContext.journal` | `CrashContext.journal` |  |
| `CrashContext.dead_letter` | `CrashContext.dead_letter` | `CrashContext.deadLetter` |  |
| `CrashContext.last_decision` | `CrashContext.last_decision` | `CrashContext.lastDecision` |  |
| `CrashContext::assemble` | `new CrashContext()` | `new CrashContext()` | constructor |
| `CrashContext::summarize` | `CrashContext.summarize` | `CrashContext.summarize` |  |
| `DeadLetterReason` | dict via `Laser.redrive_dead_letter` | `DeadLetterReason` | serde-dict |
| `DeadLetterReason::RetryExhausted` | dict key | `DeadLetterReason` | serde-dict, plain-value |
| `DeadLetterReason::Rejected` | dict key | `DeadLetterReason` | serde-dict, plain-value |
| `DeadLetterReason::DecodeFailed` | dict key | `DeadLetterReason` | serde-dict, plain-value |
| `DeadLetterReason::DeadlineExceeded` | dict key | `DeadLetterReason` | serde-dict, plain-value |
| `DeadLetterReason::Unrecognized` | dict key | `DeadLetterReason` | serde-dict, plain-value |
| `DeadLetterReason::code` | omitted | `fn:deadLetterReasonCode` | plain-value, free-function |
| `DeadLetterReason::from_code` | omitted | `fn:deadLetterReasonFromCode` | plain-value, free-function |

## cursor

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Cursor` | `Cursor` | `Cursor` |  |
| `Cursor::batch` | `Topic.replay(batch=)` | `Cursor.batch` | keywords |
| `Cursor::from_offsets` | `Topic.replay(from_offsets=)` | `Cursor.fromOffsets` | keywords |
| `Cursor::offsets` | `Cursor.offsets` | `Cursor.offsets` | property |
| `Cursor::poll` | `Cursor.poll` | `Cursor.poll` |  |
| `Cursor::stream` | `Cursor.__aiter__` | `Cursor.stream` | protocol |

## destinations

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Destinations` | `Destinations` | `Destinations` |  |
| `Destinations::accept_retention_gap` | `Destinations.accept_retention_gap` | `Destinations.acceptRetentionGap` |  |
| `Destinations::acquire_lease` | `Destinations.acquire_lease` | `Destinations.acquireLease` |  |
| `Destinations::add_partition` | `Destinations.add_partition` | `Destinations.addPartition` |  |
| `Destinations::bind_table` | `Destinations.bind_table` | `Destinations.bindTable` |  |
| `Destinations::clear_block` | `Destinations.clear_block` | `Destinations.clearBlock` |  |
| `Destinations::complete` | `Destinations.complete` | `Destinations.complete` |  |
| `Destinations::get` | `Destinations.get` | `Destinations.get` |  |
| `Destinations::list` | `Destinations.list` | `Destinations.list` |  |
| `Destinations::mutate` | `Destinations.mutate` | `Destinations.mutate` |  |
| `Destinations::mutate_with_supervisor_assertion` | `Destinations.mutate(supervisor_assertion=)` | `Destinations.mutateWithSupervisorAssertion` | overload |
| `Destinations::observe_partition_lifecycle` | `Destinations.observe_partition_lifecycle` | `Destinations.observePartitionLifecycle` |  |
| `Destinations::prepare` | `Destinations.prepare` | `Destinations.prepare` |  |
| `Destinations::query_routes` | `Destinations.query_routes` | `Destinations.queryRoutes` |  |
| `Destinations::record_block` | `Destinations.record_block` | `Destinations.recordBlock` |  |
| `Destinations::record_repair` | `Destinations.record_repair` | `Destinations.recordRepair` |  |
| `Destinations::record_retention_gap` | `Destinations.record_retention_gap` | `Destinations.recordRetentionGap` |  |
| `Destinations::register` | `Destinations.register` | `Destinations.register` |  |
| `Destinations::register_query_route` | `Destinations.register_query_route` | `Destinations.registerQueryRoute` |  |
| `Destinations::remove_query_route` | `Destinations.remove_query_route` | `Destinations.removeQueryRoute` |  |
| `Destinations::renew_lease` | `Destinations.renew_lease` | `Destinations.renewLease` |  |
| `Destinations::set_desired_state` | `Destinations.set_desired_state` | `Destinations.setDesiredState` |  |
| `Destinations::supersede_generation` | `Destinations.supersede_generation` | `Destinations.supersedeGeneration` |  |
| `Destinations::take_over_lease` | `Destinations.take_over_lease` | `Destinations.takeOverLease` |  |

## edge_auth

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `EdgeClaims` | `fn:authorize_edge` | `EdgeClaims` | keywords |
| `EdgeClaims.audience` | `fn:authorize_edge(audience=)` | `EdgeClaims.audience` | keywords |
| `EdgeClaims.scopes` | `fn:authorize_edge(scopes=)` | `EdgeClaims.scopes` | keywords |
| `EdgeDenial` | `EdgeDenial` | `EdgeDenial` |  |
| `EdgeDenial::WrongAudience` | `EdgeDenial.wrong_audience` | `EdgeDenial` | plain-value |
| `EdgeDenial::StepUp` | `EdgeDenial.step_up` | `EdgeDenial` | plain-value |
| `EdgeDenial::challenge` | `EdgeDenial.challenge` | `fn:edgeDenialChallenge` | property, free-function |
| `EdgeDenial::code` | `EdgeDenial.code` | `fn:edgeDenialCode` | property, free-function |
| `edge_auth::authorize_edge` | `fn:authorize_edge` | `fn:authorizeEdge` |  |

## error

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `PublishFailure` | `PublishFailedError` | `PublishFailedError` | error-class |
| `PublishFailure.source` | `PublishFailedError` | `PublishFailedError.publishCause` | protocol, error-class |
| `PublishFailure.stream` | `PublishFailedError.stream` | `PublishFailedError.stream` |  |
| `PublishFailure.topic` | `PublishFailedError.topic` | `PublishFailedError.topic` |  |
| `PublishFailure.committed` | `PublishFailedError.committed` | `PublishFailedError.committed` |  |
| `PublishFailure.unconfirmed` | `PublishFailedError.unconfirmed` | `PublishFailedError.unconfirmed` |  |

## filters

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `filters::AGDX_FILTERED_ACK_CODE` | `const:AGDX_FILTERED_ACK_CODE` | `const:AGDX_FILTERED_ACK_CODE` |  |
| `filters::AGDX_FILTERED_POLL_CODE` | `const:AGDX_FILTERED_POLL_CODE` | `const:AGDX_FILTERED_POLL_CODE` |  |
| `filters::AGDX_FILTER_MUTATE_CODE` | `const:AGDX_FILTER_MUTATE_CODE` | `const:AGDX_FILTER_MUTATE_CODE` |  |
| `filters::AGDX_FILTER_OPERATION_CODE` | `const:AGDX_FILTER_OPERATION_CODE` | `const:AGDX_FILTER_OPERATION_CODE` |  |
| `filters::AGDX_FILTER_PREVIEW_CODE` | `const:AGDX_FILTER_PREVIEW_CODE` | `const:AGDX_FILTER_PREVIEW_CODE` |  |
| `filters::AGDX_FILTER_TEST_CODE` | `const:AGDX_FILTER_TEST_CODE` | `const:AGDX_FILTER_TEST_CODE` |  |
| `filters::AGDX_FILTER_VALIDATE_CODE` | `const:AGDX_FILTER_VALIDATE_CODE` | `const:AGDX_FILTER_VALIDATE_CODE` |  |
| `filters::AGDX_GET_FILTER_BINDING_CODE` | `const:AGDX_GET_FILTER_BINDING_CODE` | `const:AGDX_GET_FILTER_BINDING_CODE` |  |
| `filters::AGDX_GET_FILTER_CODE` | `const:AGDX_GET_FILTER_CODE` | `const:AGDX_GET_FILTER_CODE` |  |
| `filters::AGDX_LIST_FILTERS_CODE` | `const:AGDX_LIST_FILTERS_CODE` | `const:AGDX_LIST_FILTERS_CODE` |  |
| `filters::AGDX_LIST_FILTER_BINDINGS_CODE` | `const:AGDX_LIST_FILTER_BINDINGS_CODE` | `const:AGDX_LIST_FILTER_BINDINGS_CODE` |  |
| `filters::AGDX_LIST_FILTER_REVISIONS_CODE` | `const:AGDX_LIST_FILTER_REVISIONS_CODE` | `const:AGDX_LIST_FILTER_REVISIONS_CODE` |  |
| `AppliedPolicy` | dict via `MatchedPage.policy` | `AppliedPolicy` | serde-dict |
| `AppliedPolicy.group_id` | dict key | `AppliedPolicy.groupId` | serde-dict, keywords |
| `AppliedPolicy.digest` | dict key | `AppliedPolicy.digest` | serde-dict, keywords |
| `AppliedPolicy.filter_id` | dict key | `AppliedPolicy.filterId` | serde-dict, keywords |
| `AppliedPolicy.revision` | dict key | `AppliedPolicy.revision` | serde-dict, keywords |
| `AppliedPolicy.mode` | dict key | `AppliedPolicy.mode` | serde-dict, keywords |
| `AppliedPolicy.policy_generation` | dict key | `AppliedPolicy.policyGeneration` | serde-dict, keywords |
| `AppliedPolicy::filtered` | `fn:applied_policy_filtered` | `fn:appliedPolicyFiltered` | free-function |
| `AppliedPolicy::unfiltered` | `fn:applied_policy_unfiltered` | `fn:appliedPolicyUnfiltered` | free-function |
| `Coerce` | dict via `FilterExpr.to_dict` | `Coerce` | serde-dict |
| `Coerce::Timestamp` | dict key | `Coerce` | serde-dict, plain-value |
| `Coerce::Number` | dict key | `Coerce` | serde-dict, plain-value |
| `CoercedPredicate` | dict via `FilterExpr.to_dict` | `CoercedPredicate` | serde-dict |
| `CoercedPredicate.pred` | dict key | `CoercedPredicate.pred` | serde-dict, keywords |
| `CoercedPredicate.coerce` | dict key | `CoercedPredicate.coerce` | serde-dict, keywords |
| `CompiledFilter` | `CompiledFilter` | `CompiledFilter` |  |
| `CompiledFilter::compile` | `CompiledFilter.compile` | `CompiledFilter.compile` |  |
| `CompiledFilter::compile_with_schemas` | `CompiledFilter.compile(schemas=)` | `CompiledFilter.compile(schemas=)` | overload |
| `CompiledFilter::digest` | `CompiledFilter.digest` | `CompiledFilter.digest` | property |
| `CompiledFilter::evaluate` | `CompiledFilter.evaluate` | `CompiledFilter.evaluate` |  |
| `CompiledFilter::evaluate_with_fault` | `CompiledFilter.evaluate_with_fault` | `CompiledFilter.evaluateWithFault` |  |
| `CompiledFilter::explain` | `CompiledFilter.explain` | `CompiledFilter.explain` |  |
| `CompiledFilter::fault_policy` | `CompiledFilter.fault_policy` | `CompiledFilter.faultPolicy` | property |
| `CompiledFilter::filter` | `CompiledFilter.filter` | `CompiledFilter.filter` | property |
| `CompiledFilter::header_need` | `CompiledFilter.header_need` | `CompiledFilter.headerNeed` | property |
| `CompiledFilter::policy_for` | `CompiledFilter.policy_for` | `CompiledFilter.policyFor` |  |
| `CompiledFilter::reads_headers` | `CompiledFilter.reads_headers` | `CompiledFilter.readsHeaders` | property |
| `CompiledFilter::reads_payload` | `CompiledFilter.reads_payload` | `CompiledFilter.readsPayload` | property |
| `CompiledFilter::record_policy` | `CompiledFilter.record_policy` | `CompiledFilter.recordPolicy` |  |
| `ConsumerFilter` | `ConsumerFilter` | `ConsumerFilter` |  |
| `ConsumerFilter.v` | `ConsumerFilter.v` | `ConsumerFilter.v` |  |
| `ConsumerFilter.evaluator_version` | `ConsumerFilter.evaluator_version` | `ConsumerFilter.evaluatorVersion` |  |
| `ConsumerFilter.expr` | `ConsumerFilter.expr` | `ConsumerFilter.expr` |  |
| `ConsumerFilter.codec` | `ConsumerFilter.codec` | `ConsumerFilter.codec` |  |
| `ConsumerFilter.fault_policy` | `ConsumerFilter.fault_policy` | `ConsumerFilter.faultPolicy` |  |
| `ConsumerFilter.foreign_policy` | `ConsumerFilter.foreign_policy` | `ConsumerFilter.foreignPolicy` |  |
| `ConsumerFilter.mismatch_policy` | `ConsumerFilter.mismatch_policy` | `ConsumerFilter.mismatchPolicy` |  |
| `ConsumerFilter.schema_refs` | `ConsumerFilter.schema_refs` | `ConsumerFilter.schemaRefs` |  |
| `ConsumerFilter::avro` | `ConsumerFilter.avro` | `ConsumerFilter.avro` |  |
| `ConsumerFilter::cbor` | `ConsumerFilter.cbor` | `ConsumerFilter.cbor` |  |
| `ConsumerFilter::digest` | `ConsumerFilter.digest` | `ConsumerFilter.digest` | property |
| `ConsumerFilter::headers_only` | `ConsumerFilter.headers_only` | `ConsumerFilter.headersOnly` |  |
| `ConsumerFilter::json` | `ConsumerFilter.json` | `ConsumerFilter.json` |  |
| `ConsumerFilter::protobuf` | `ConsumerFilter.protobuf` | `ConsumerFilter.protobuf` |  |
| `ConsumerFilter::with_fault_policy` | `ConsumerFilter.with_fault_policy` | `ConsumerFilter.withFaultPolicy` |  |
| `ConsumerFilter::with_foreign_policy` | `ConsumerFilter.with_foreign_policy` | `ConsumerFilter.withForeignPolicy` |  |
| `ConsumerFilter::with_mismatch_policy` | `ConsumerFilter.with_mismatch_policy` | `ConsumerFilter.withMismatchPolicy` |  |
| `Continuation` | dict via `ConsumerGroup.reader` | `Continuation` | serde-dict |
| `Continuation.group_id` | dict key | `Continuation.groupId` | serde-dict, keywords |
| `Continuation.next_scan_offset` | dict key | `Continuation.nextScanOffset` | serde-dict, keywords |
| `Continuation.generation` | dict key | `Continuation.generation` | serde-dict, keywords |
| `Continuation.digest` | dict key | `Continuation.digest` | serde-dict, keywords |
| `Continuation.read_mode` | dict key | `Continuation.readMode` | serde-dict, keywords |
| `Continuation.mode` | dict key | `Continuation.mode` | serde-dict, keywords |
| `Continuation.policy_generation` | dict key | `Continuation.policyGeneration` | serde-dict, keywords |
| `filters::DEFAULT_OUTCOME_WAIT` | `const:DEFAULT_OUTCOME_WAIT_SECS` | `const:DEFAULT_OUTCOME_WAIT_MS` |  |
| `DecodeLimits` | `CompiledFilter.evaluate` | `DecodeLimits` | keywords |
| `DecodeLimits.max_payload_bytes` | `CompiledFilter.evaluate(max_payload_bytes=)` | `DecodeLimits.maxPayloadBytes` | keywords |
| `DecodeLimits.max_depth` | `CompiledFilter.evaluate(max_depth=)` | `DecodeLimits.maxDepth` | keywords |
| `ExecutionMode` | omitted | `ExecutionMode` | plain-value |
| `ExecutionMode::Filtered` | omitted | `ExecutionMode` | plain-value |
| `ExecutionMode::Unfiltered` | omitted | `ExecutionMode` | plain-value |
| `ExecutionMode::is_filtered` | `fn:execution_mode_is_filtered` | `fn:executionModeIsFiltered` | free-function |
| `ExplainNode` | dict via `CompiledFilter.explain` | `ExplainNode` | serde-dict |
| `ExplainNode.label` | dict key | `ExplainNode.label` | serde-dict, keywords |
| `ExplainNode.truth` | dict key | `ExplainNode.truth` | serde-dict, keywords |
| `ExplainNode.children` | dict key | `ExplainNode.children` | serde-dict, keywords |
| `filters::FILTER_EVALUATOR_VERSION` | `const:FILTER_EVALUATOR_VERSION` | `const:FILTER_EVALUATOR_VERSION` |  |
| `filters::FILTER_OP_VERSION` | `const:FILTER_OP_VERSION` | `const:FILTER_OP_VERSION` |  |
| `FaultPolicy` | omitted | `FaultPolicy` | plain-value |
| `FaultPolicy::Stop` | omitted | `FaultPolicy` | plain-value |
| `FaultPolicy::Pass` | omitted | `FaultPolicy` | plain-value |
| `FaultPolicy::Drop` | omitted | `FaultPolicy` | plain-value |
| `FaultReason` | omitted | `FaultReason` | plain-value |
| `FaultReason::MissingSchema` | omitted | `FaultReason` | plain-value |
| `FaultReason::SchemaNotAllowed` | omitted | `FaultReason` | plain-value |
| `FaultReason::SchemaMismatch` | omitted | `FaultReason` | plain-value |
| `FaultReason::Malformed` | omitted | `FaultReason` | plain-value |
| `FaultReason::TooLarge` | omitted | `FaultReason` | plain-value |
| `FaultReason::TooDeep` | omitted | `FaultReason` | plain-value |
| `FaultReason::ForeignCodec` | omitted | `FaultReason` | plain-value |
| `FaultReason::TypeMismatch` | omitted | `FaultReason` | plain-value |
| `FaultReason::is_foreign` | `fn:fault_reason_is_foreign` | `fn:faultReasonIsForeign` | free-function |
| `FieldPath` | `FieldPath` | `FieldPath` |  |
| `FieldPath::parse` | `FieldPath.parse` | `FieldPath.parse` |  |
| `FieldPath::segments` | `FieldPath.segments` | `FieldPath.segments` | property |
| `FilterBinding` | dict via `GroupFilter.get` | `FilterBinding` | serde-dict |
| `FilterBinding.group` | dict key | `FilterBinding.group` | serde-dict, keywords |
| `FilterBinding.identity` | dict key | `FilterBinding.identity` | serde-dict, keywords |
| `FilterBinding.filter_id` | dict key | `FilterBinding.filterId` | serde-dict, keywords |
| `FilterBinding.revision` | dict key | `FilterBinding.revision` | serde-dict, keywords |
| `FilterBinding.digest` | dict key | `FilterBinding.digest` | serde-dict, keywords |
| `FilterBinding.bound_at_micros` | dict key | `FilterBinding.boundAtMicros` | serde-dict, keywords |
| `FilterBinding.policy_generation` | dict key | `FilterBinding.policyGeneration` | serde-dict, keywords |
| `FilterCodec` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::Json` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::Cbor` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::Avro` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::Protobuf` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::HeadersOnly` | omitted | `FilterCodec` | plain-value |
| `FilterCodec::Unknown` | omitted | `FilterCodec` | plain-value |
| `FilterError` | `FilterError` | `FilterError` |  |
| `FilterError.code` | `FilterError.code` | `FilterError.code` | keywords |
| `FilterError.reason` | `FilterError.reason` | `FilterError.reason` | keywords |
| `FilterError.message` | dict key | `FilterError.message` | serde-dict, keywords |
| `FilterError::check_version` | `FilterError.check_version` | `fn:filterErrorCheckVersion` | free-function |
| `FilterError::invalid` | `FilterError.invalid` | `fn:filterErrorInvalid` | free-function |
| `FilterError::new` | `new FilterError()` | `FilterError` | constructor, keywords |
| `FilterErrorReason` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::InvalidRequest` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Unsupported` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::VersionSkew` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::NotFound` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Conflict` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::SourceChanged` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::MembershipStale` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::NotPrimary` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::CatalogUnavailable` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::RevisionDisabled` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::TooLarge` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Unauthenticated` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Forbidden` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Unavailable` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::CapacityExhausted` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Backend` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::Unknown` | omitted | `FilterErrorReason` | plain-value |
| `FilterErrorReason::code` | `fn:filter_error_reason_code` | `fn:code` | free-function |
| `FilterExplanation` | dict via `CompiledFilter.explain` | `FilterExplanation` | serde-dict |
| `FilterExplanation.verdict` | dict key | `FilterExplanation.verdict` | serde-dict, keywords |
| `FilterExplanation.fault` | dict key | `FilterExplanation.fault` | serde-dict, keywords |
| `FilterExplanation.root` | dict key | `FilterExplanation.root` | serde-dict, keywords |
| `FilterExpr` | `FilterExpr` | `FilterExpr` |  |
| `FilterExpr::All` | `FilterExpr.all` | `FilterExpr.all` |  |
| `FilterExpr::Any` | `FilterExpr.any` | `FilterExpr.any` |  |
| `FilterExpr::Not` | `FilterExpr.negate` | `FilterExpr` | constructor, plain-value |
| `FilterExpr::Pred` | `FilterExpr.pred` | `FilterExpr.pred` |  |
| `FilterExpr::PredAs` | `FilterExpr.pred_as` | `FilterExpr.predAs` |  |
| `FilterExpr::Present` | `FilterExpr.present` | `FilterExpr.present` |  |
| `FilterExpr::Absent` | `FilterExpr.absent` | `FilterExpr.absent` |  |
| `FilterExpr::Header` | `FilterExpr.header` | `FilterExpr.header` |  |
| `FilterExpr::Text` | `FilterExpr.text` | `FilterExpr.text` |  |
| `FilterExpr::HeaderText` | `FilterExpr.header_text` | `FilterExpr.headerText` |  |
| `FilterExpr::absent` | `FilterExpr.absent` | `FilterExpr.absent` |  |
| `FilterExpr::all` | `FilterExpr.all` | `FilterExpr.all` |  |
| `FilterExpr::any` | `FilterExpr.any` | `FilterExpr.any` |  |
| `FilterExpr::case_insensitive` | `FilterExpr.case_insensitive` | `fn:filterExprCaseInsensitive` | free-function |
| `FilterExpr::header` | `FilterExpr.header` | `FilterExpr.header` |  |
| `FilterExpr::header_text` | `FilterExpr.header_text` | `FilterExpr.headerText` |  |
| `FilterExpr::negate` | `FilterExpr.negate` | `FilterExpr.negate` |  |
| `FilterExpr::pred` | `FilterExpr.pred` | `FilterExpr.pred` |  |
| `FilterExpr::pred_as` | `FilterExpr.pred_as` | `FilterExpr.predAs` |  |
| `FilterExpr::present` | `FilterExpr.present` | `FilterExpr.present` |  |
| `FilterExpr::reads_headers` | `FilterExpr.reads_headers` | `fn:filterExprReadsHeaders` | property, free-function |
| `FilterExpr::reads_payload` | `FilterExpr.reads_payload` | `fn:filterExprReadsPayload` | property, free-function |
| `FilterExpr::text` | `FilterExpr.text` | `FilterExpr.text` |  |
| `FilterExpr::try_absent` | `FilterExpr.try_absent` | `FilterExpr.tryAbsent` |  |
| `FilterExpr::try_present` | `FilterExpr.try_present` | `FilterExpr.tryPresent` |  |
| `FilterGroupIdentity` | dict via `ConsumerGroupInfo.identity` | `FilterGroupIdentity` | serde-dict |
| `FilterGroupIdentity.stream_id` | dict key | `FilterGroupIdentity.streamId` | serde-dict, keywords |
| `FilterGroupIdentity.stream_created_at_micros` | dict key | `FilterGroupIdentity.streamCreatedAtMicros` | serde-dict, keywords |
| `FilterGroupIdentity.topic_id` | dict key | `FilterGroupIdentity.topicId` | serde-dict, keywords |
| `FilterGroupIdentity.topic_created_at_micros` | dict key | `FilterGroupIdentity.topicCreatedAtMicros` | serde-dict, keywords |
| `FilterGroupIdentity.group_id` | dict key | `FilterGroupIdentity.groupId` | serde-dict, keywords |
| `FilterGroupRef` | dict via `GroupFilter.get` | `FilterGroupRef` | serde-dict |
| `FilterGroupRef.stream` | dict key | `FilterGroupRef.stream` | serde-dict, keywords |
| `FilterGroupRef.topic` | dict key | `FilterGroupRef.topic` | serde-dict, keywords |
| `FilterGroupRef.group` | dict key | `FilterGroupRef.group` | serde-dict, keywords |
| `FilterHeader` | `GroupFilter.test(headers=)` | `FilterHeader` | keywords |
| `FilterHeader.key` | `GroupFilter.test(headers=)` | `FilterHeader.key` | keywords |
| `FilterHeader.value` | `GroupFilter.test(headers=)` | `FilterHeader.value` | keywords |
| `FilterPreview` | dict via `GroupFilter.preview` | `FilterPreview` | serde-dict |
| `FilterPreview.v` | dict key | `FilterPreview.v` | serde-dict, keywords |
| `FilterPreview.partition_id` | dict key | `FilterPreview.partitionId` | serde-dict, keywords |
| `FilterPreview.policy` | dict key | `FilterPreview.policy` | serde-dict, keywords |
| `FilterPreview.read_mode` | dict key | `FilterPreview.readMode` | serde-dict, keywords |
| `FilterPreview.examined` | dict key | `FilterPreview.examined` | serde-dict, keywords |
| `FilterPreview.matched` | dict key | `FilterPreview.matched` | serde-dict, keywords |
| `FilterPreview.faults` | dict key | `FilterPreview.faults` | serde-dict, keywords |
| `FilterPreview.stop` | dict key | `FilterPreview.stop` | serde-dict, keywords |
| `FilterPreview.next_offset` | dict key | `FilterPreview.nextOffset` | serde-dict, keywords |
| `FilterPreview.frontier` | dict key | `FilterPreview.frontier` | serde-dict, keywords |
| `FilterPreview.records` | dict key | `FilterPreview.records` | serde-dict, keywords |
| `FilterPreviewBuilder` | `GroupFilter.preview` | `FilterPreviewOptions` | keywords |
| `FilterPreviewBuilder::explain` | `GroupFilter.preview(explain=)` | `FilterPreviewOptions.explain` | keywords |
| `FilterPreviewBuilder::from_offset` | `GroupFilter.preview(from_offset=)` | `FilterPreviewOptions.fromOffset` | keywords |
| `FilterPreviewBuilder::max_examined` | `GroupFilter.preview(max_examined=)` | `FilterPreviewOptions.maxExamined` | keywords |
| `FilterPreviewBuilder::max_records` | `GroupFilter.preview(max_records=)` | `FilterPreviewOptions.maxRecords` | keywords |
| `FilterPreviewBuilder::send` | `GroupFilter.preview` | `GroupFilter.preview` | one-call |
| `FilterRecord` | `CompiledFilter.evaluate` | `FilterRecord` | keywords |
| `FilterRecord.payload` | `CompiledFilter.evaluate(payload=)` | `FilterRecord.payload` | keywords |
| `FilterRecord.headers` | `CompiledFilter.evaluate(headers=)` | `FilterRecord.headers` | keywords |
| `FilterRevisionInfo` | dict via `GroupFilter.revisions` | `FilterRevisionInfo` | serde-dict |
| `FilterRevisionInfo.enabled` | dict key | `FilterRevisionInfo.enabled` | serde-dict, keywords |
| `FilterRevisionInfo.revision` | dict key | `FilterRevisionInfo.revision` | serde-dict, keywords |
| `FilterRevisionInfo.digest` | dict key | `FilterRevisionInfo.digest` | serde-dict, keywords |
| `FilterRevisionInfo.filter` | dict key | `FilterRevisionInfo.filter` | serde-dict, keywords |
| `FilterRevisionInfo.created_at_micros` | dict key | `FilterRevisionInfo.createdAtMicros` | serde-dict, keywords |
| `FilterRevisionPage` | dict via `GroupFilter.revisions` | `FilterRevisionPage` | serde-dict |
| `FilterRevisionPage.filter_id` | dict key | `FilterRevisionPage.filterId` | serde-dict, keywords |
| `FilterRevisionPage.items` | dict key | `FilterRevisionPage.items` | serde-dict, keywords |
| `FilterRevisionPage.page` | dict key | `FilterRevisionPage.page` | serde-dict, keywords |
| `FilterRevisionPage.page_size` | dict key | `FilterRevisionPage.pageSize` | serde-dict, keywords |
| `FilterRevisionPage.total` | dict key | `FilterRevisionPage.total` | serde-dict, keywords |
| `FilterRevisionRef` | dict via `GroupFilter.revise` | `FilterRevisionRef` | serde-dict |
| `FilterRevisionRef.filter_id` | dict key | `FilterRevisionRef.filterId` | serde-dict, keywords |
| `FilterRevisionRef.revision` | dict key | `FilterRevisionRef.revision` | serde-dict, keywords |
| `FilterRevisionRef.digest` | dict key | `FilterRevisionRef.digest` | serde-dict, keywords |
| `FilterState` | omitted | `FilterState` | plain-value |
| `FilterState::Active` | omitted | `FilterState` | plain-value |
| `FilterState::Archived` | omitted | `FilterState` | plain-value |
| `FilterState::Dropped` | omitted | `FilterState` | plain-value |
| `FilterTestResult` | dict via `GroupFilter.test` | `FilterTestResult` | serde-dict |
| `FilterTestResult.v` | dict key | `FilterTestResult.v` | serde-dict, keywords |
| `FilterTestResult.policy` | dict key | `FilterTestResult.policy` | serde-dict, keywords |
| `FilterTestResult.explanation` | dict key | `FilterTestResult.explanation` | serde-dict, keywords |
| `FilteredReader` | `FilteredReader` | `FilteredReader` |  |
| `FilteredReader::ack` | `FilteredReader.ack` | `FilteredReader.ack` |  |
| `FilteredReader::ack_page` | `FilteredReader.ack_page` | `FilteredReader.ackPage` |  |
| `FilteredReader::ack_through` | `FilteredReader.ack_through` | `FilteredReader.ackThrough` |  |
| `FilteredReader::close` | `FilteredReader.close` | `FilteredReader.close` |  |
| `FilteredReader::data_connections_opened` | `FilteredReader.data_connections_opened` | `FilteredReader.dataConnectionsOpened` |  |
| `FilteredReader::examined_in_round` | `FilteredReader.examined_in_round` | `FilteredReader.examinedInRound` |  |
| `FilteredReader::idle_interval` | `FilteredReader.idle_interval` | `FilteredReader.idleIntervalMs` |  |
| `FilteredReader::next_page` | `FilteredReader.next_page` | `FilteredReader.nextPage` |  |
| `FilteredReader::next_record` | `FilteredReader.next_record` | `FilteredReader.nextRecord` |  |
| `FilteredReader::owns` | `FilteredReader.owns` | `FilteredReader.owns` |  |
| `FilteredReader::partitions` | `FilteredReader.partitions` | `FilteredReader.partitions` |  |
| `FilteredReader::read_round` | `FilteredReader.read_round` | `FilteredReader.readRound` |  |
| `FilteredReader::try_next_page` | `FilteredReader.try_next_page` | `FilteredReader.tryNextPage` |  |
| `FilteredReaderBuilder` | `ConsumerGroup.reader` | `FilteredReaderBuilder` | keywords |
| `FilteredReaderBuilder::build` | `ConsumerGroup.reader` | `FilteredReaderBuilder.build` | one-call |
| `FilteredReaderBuilder::count` | `ConsumerGroup.reader(count=)` | `FilteredReaderBuilder.count` | keywords |
| `FilteredReaderBuilder::idle_interval` | `ConsumerGroup.reader(idle_interval=)` | `FilteredReaderBuilder.idleInterval` | keywords |
| `FilteredReaderBuilder::local_guard` | `ConsumerGroup.reader(local_guard=)` | `FilteredReaderBuilder.localGuard` | keywords |
| `FilteredReaderBuilder::max_examined` | `ConsumerGroup.reader(max_examined=)` | `FilteredReaderBuilder.maxExamined` | keywords |
| `FilteredReaderBuilder::max_reply_bytes` | `ConsumerGroup.reader(max_reply_bytes=)` | `FilteredReaderBuilder.maxReplyBytes` | keywords |
| `FilteredReaderBuilder::max_unacked_pages` | `ConsumerGroup.reader(max_unacked_pages=)` | `FilteredReaderBuilder.maxUnackedPages` | keywords |
| `FilteredReaderBuilder::partition` | `ConsumerGroup.reader(partitions=)` | `FilteredReaderBuilder.partition` | keywords |
| `FilteredReaderBuilder::read_mode` | `ConsumerGroup.reader(read_mode=)` | `FilteredReaderBuilder.readMode` | keywords |
| `FilteredReaderBuilder::start` | `ConsumerGroup.reader(start=)` | `FilteredReaderBuilder.start` | keywords |
| `FilteredStart` | dict via `ConsumerGroup.reader` | `FilteredStart` | serde-dict |
| `FilteredStart::Next` | dict key | `FilteredStart` | serde-dict, plain-value |
| `FilteredStart::First` | dict key | `FilteredStart` | serde-dict, plain-value |
| `FilteredStart::Last` | dict key | `FilteredStart` | serde-dict, plain-value |
| `FilteredStart::Offset` | dict key | `FilteredStart` | serde-dict, plain-value |
| `FilteredStart::Timestamp` | dict key | `FilteredStart` | serde-dict, plain-value |
| `FilteredStart::Continue` | dict key | `FilteredStart` | serde-dict, plain-value |
| `GroupFilterSpec` | `ConsumerGroup.create(filter=, filter_id=, revision=)` | `GroupFilterSpec` | keywords |
| `GroupFilterSpec::Definition` | `ConsumerGroup.create(filter=)` | `GroupFilterSpec` | keywords, plain-value |
| `GroupFilterSpec::Revision` | `ConsumerGroup.create(filter_id=, revision=)` | `GroupFilterSpec` | keywords, plain-value |
| `HeaderNeed` | omitted | `HeaderNeed` | plain-value |
| `HeaderNeed::None` | omitted | `HeaderNeed` | plain-value |
| `HeaderNeed::ContentType` | omitted | `HeaderNeed` | plain-value |
| `HeaderNeed::All` | omitted | `HeaderNeed` | plain-value |
| `HeaderPredicate` | dict via `FilterExpr.to_dict` | `HeaderPredicate` | serde-dict |
| `HeaderPredicate.key` | dict key | `HeaderPredicate.key` | serde-dict, keywords |
| `HeaderPredicate.op` | dict key | `HeaderPredicate.op` | serde-dict, keywords |
| `HeaderPredicate.value` | dict key | `HeaderPredicate.value` | serde-dict, keywords |
| `HeaderRef` | `CompiledFilter.evaluate(headers=)` | `HeaderRef` | keywords |
| `HeaderRef.key` | `CompiledFilter.evaluate(headers=)` | `HeaderRef.key` | keywords |
| `HeaderRef.value` | `CompiledFilter.evaluate(headers=)` | `HeaderRef.value` | keywords |
| `HeaderScalar` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::Bool` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::Int` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::Uint` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::Float` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::String` | omitted | `HeaderScalar` | plain-value |
| `HeaderScalar::Raw` | omitted | `HeaderScalar` | plain-value |
| `HeaderValueRef` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::Bool` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::Int` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::Uint` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::Float` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::String` | omitted | `HeaderValueRef` | plain-value |
| `HeaderValueRef::Raw` | omitted | `HeaderValueRef` | plain-value |
| `filters::MAX_FILTERED_PAGE_BYTES` | `const:MAX_FILTERED_PAGE_BYTES` | `const:MAX_FILTERED_PAGE_BYTES` |  |
| `filters::MAX_FILTERED_PAGE_EXAMINED` | `const:MAX_FILTERED_PAGE_EXAMINED` | `const:MAX_FILTERED_PAGE_EXAMINED` |  |
| `filters::MAX_FILTERED_PAGE_RECORDS` | `const:MAX_FILTERED_PAGE_RECORDS` | `const:MAX_FILTERED_PAGE_RECORDS` |  |
| `filters::MAX_FILTER_BYTES` | `const:MAX_FILTER_BYTES` | `const:MAX_FILTER_BYTES` |  |
| `filters::MAX_FILTER_CATALOG_PAGE` | `const:MAX_FILTER_CATALOG_PAGE` | `const:MAX_FILTER_CATALOG_PAGE` |  |
| `filters::MAX_FILTER_NAME_BYTES` | `const:MAX_FILTER_NAME_BYTES` | `const:MAX_FILTER_NAME_BYTES` |  |
| `filters::MAX_FILTER_PREVIEW_EXAMINED` | `const:MAX_FILTER_PREVIEW_EXAMINED` | `const:MAX_FILTER_PREVIEW_EXAMINED` |  |
| `filters::MAX_FILTER_PREVIEW_RECORDS` | `const:MAX_FILTER_PREVIEW_RECORDS` | `const:MAX_FILTER_PREVIEW_RECORDS` |  |
| `MatchedPage` | `MatchedPage` | `MatchedPage` |  |
| `MatchedPage.partition_id` | `MatchedPage.partition_id` | `MatchedPage.partitionId` | keywords |
| `MatchedPage.records` | `MatchedPage.records` | `MatchedPage.records` | keywords |
| `MatchedPage.policy` | `MatchedPage.policy` | `MatchedPage.policy` | keywords |
| `MatchedPage.generation` | `MatchedPage.generation` | `MatchedPage.generation` | keywords |
| `MatchedPage.stop` | `MatchedPage.stop` | `MatchedPage.stop` | keywords |
| `MatchedPage.examined` | `MatchedPage.examined` | `MatchedPage.examined` | keywords |
| `MatchedPage.frontier` | `MatchedPage.frontier` | `MatchedPage.frontier` | keywords |
| `MatchedPage.safe_ack_offset` | `MatchedPage.safe_ack_offset` | `MatchedPage.safeAckOffset` | keywords |
| `MatchedRecord` | `MatchedRecord` | `MatchedRecord` |  |
| `MatchedRecord.partition_id` | `MatchedRecord.partition_id` | `MatchedRecord.partitionId` | keywords |
| `MatchedRecord.offset` | `MatchedRecord.offset` | `MatchedRecord.offset` | keywords |
| `MatchedRecord.frontier` | `MatchedRecord.frontier` | `MatchedRecord.frontier` | keywords |
| `MatchedRecord.evaluated` | `MatchedRecord.evaluated` | `MatchedRecord.evaluated` | keywords |
| `MatchedRecord.message` | `MatchedRecord.message` | `MatchedRecord.message` | keywords |
| `MatchedRecord::headers_malformed` | `MatchedRecord.headers_malformed` | `MatchedRecord.headersMalformed` | property, keywords |
| `MatchedRecord::json` | `MatchedRecord.json` | `MatchedRecord.json` | keywords |
| `PathSegment` | omitted | `PathSegment` | plain-value |
| `PathSegment::Key` | omitted | `PathSegment` | plain-value |
| `PathSegment::Index` | omitted | `PathSegment` | plain-value |
| `PreviewRecord` | dict via `GroupFilter.preview` | `PreviewRecord` | serde-dict |
| `PreviewRecord.offset` | dict key | `PreviewRecord.offset` | serde-dict, keywords |
| `PreviewRecord.timestamp_micros` | dict key | `PreviewRecord.timestampMicros` | serde-dict, keywords |
| `PreviewRecord.verdict` | dict key | `PreviewRecord.verdict` | serde-dict, keywords |
| `PreviewRecord.fault` | dict key | `PreviewRecord.fault` | serde-dict, keywords |
| `PreviewRecord.payload_text` | dict key | `PreviewRecord.payloadText` | serde-dict, keywords |
| `PreviewRecord.payload_truncated` | dict key | `PreviewRecord.payloadTruncated` | serde-dict, keywords |
| `PreviewRecord.explanation` | dict key | `PreviewRecord.explanation` | serde-dict, keywords |
| `ReadMode` | omitted | `ReadMode` | plain-value |
| `ReadMode::Primary` | omitted | `ReadMode` | plain-value |
| `ReadMode::Local` | omitted | `ReadMode` | plain-value |
| `ReadMode::is_primary` | `fn:read_mode_is_primary` | `fn:readModeIsPrimary` | free-function |
| `RecordPolicy` | omitted | `RecordPolicy` | plain-value |
| `RecordPolicy::Reject` | omitted | `RecordPolicy` | plain-value |
| `RecordPolicy::Pass` | omitted | `RecordPolicy` | plain-value |
| `RecordPolicy::is_reject` | `fn:record_policy_is_reject` | `fn:recordPolicyIsReject` | free-function |
| `SourceGeneration` | dict via `MatchedPage.generation` | `SourceGeneration` | serde-dict |
| `SourceGeneration.stream_id` | dict key | `SourceGeneration.streamId` | serde-dict, keywords |
| `SourceGeneration.stream_created_at_micros` | dict key | `SourceGeneration.streamCreatedAtMicros` | serde-dict, keywords |
| `SourceGeneration.topic_id` | dict key | `SourceGeneration.topicId` | serde-dict, keywords |
| `SourceGeneration.topic_created_at_micros` | dict key | `SourceGeneration.topicCreatedAtMicros` | serde-dict, keywords |
| `SourceGeneration.partition_id` | dict key | `SourceGeneration.partitionId` | serde-dict, keywords |
| `SourceGeneration.partition_created_revision` | dict key | `SourceGeneration.partitionCreatedRevision` | serde-dict, keywords |
| `SourceGeneration.purge_generation` | dict key | `SourceGeneration.purgeGeneration` | serde-dict, keywords |
| `StopReason` | omitted | `StopReason` | plain-value |
| `StopReason::Filled` | omitted | `StopReason` | plain-value |
| `StopReason::Budget` | omitted | `StopReason` | plain-value |
| `StopReason::EndOfVisible` | omitted | `StopReason` | plain-value |
| `StopReason::Fault` | omitted | `StopReason` | plain-value |
| `StopReason::OversizedRecord` | omitted | `StopReason` | plain-value |
| `TextMatch` | omitted | `TextMatch` | plain-value |
| `TextMatch::Equals` | omitted | `TextMatch` | plain-value |
| `TextMatch::Prefix` | omitted | `TextMatch` | plain-value |
| `TextMatch::Suffix` | omitted | `TextMatch` | plain-value |
| `TextMatch::Contains` | omitted | `TextMatch` | plain-value |
| `TextMatch::Glob` | omitted | `TextMatch` | plain-value |
| `TextMatch::Regex` | omitted | `TextMatch` | plain-value |
| `TextPredicate` | dict via `FilterExpr.to_dict` | `TextPredicate` | serde-dict |
| `TextPredicate.field` | dict key | `TextPredicate.field` | serde-dict, keywords |
| `TextPredicate.kind` | dict key | `TextPredicate.kind` | serde-dict, keywords |
| `TextPredicate.pattern` | dict key | `TextPredicate.pattern` | serde-dict, keywords |
| `TextPredicate.case_insensitive` | dict key | `TextPredicate.caseInsensitive` | serde-dict, keywords |
| `TextPredicate::validate` | `fn:text_predicate_validate` | `fn:textPredicateValidate` | free-function |
| `TimestampFormat` | omitted | `TimestampFormat` | plain-value |
| `TimestampFormat::Rfc3339` | omitted | `TimestampFormat` | plain-value |
| `TimestampFormat::EpochSeconds` | omitted | `TimestampFormat` | plain-value |
| `TimestampFormat::EpochMillis` | omitted | `TimestampFormat` | plain-value |
| `TimestampFormat::EpochMicros` | omitted | `TimestampFormat` | plain-value |
| `TimestampFormat::micros_from_integer` | `fn:timestamp_format_micros_from_integer` | `fn:timestampFormatMicrosFromInteger` | free-function |
| `TimestampFormat::micros_from_text` | `fn:timestamp_format_micros_from_text` | `fn:timestampFormatMicrosFromText` | free-function |
| `Truth` | omitted | `Truth` | plain-value |
| `Truth::Match` | omitted | `Truth` | plain-value |
| `Truth::NoMatch` | omitted | `Truth` | plain-value |
| `Truth::Unknown` | omitted | `Truth` | plain-value |
| `filters::Verdict` | `Verdict` | `FilterVerdict` | flat-namespace |
| `filters::Verdict::Selected` | omitted | `FilterVerdict` | plain-value |
| `filters::Verdict::Rejected` | omitted | `FilterVerdict` | plain-value |
| `filters::Verdict::Fault` | omitted | `FilterVerdict` | plain-value |

## fork

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ForkCreateRequest` | `ForkHandle.create` | `ForkCreateRequest` | keywords |
| `ForkCreateRequest::continuous` | `ForkHandle.create(continuous=)` | `ForkCreateRequest.continuous` | keywords |
| `ForkCreateRequest::parent` | `ForkHandle.create(parent=)` | `ForkCreateRequest.parent` | keywords |
| `ForkCreateRequest::send` | `ForkHandle.create` | `ForkCreateRequest.send` | one-call |
| `ForkCreateRequest::severed` | `ForkHandle.create(severed=)` | `ForkCreateRequest.severed` | keywords |
| `ForkCreateRequest::tables` | `ForkHandle.create(tables=)` | `ForkCreateRequest.tables` | keywords |
| `ForkError` | dict via `ForkError.detail` | `ForkError` | serde-dict |
| `ForkError::Unsupported` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::NotFound` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::InvalidFork` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::Conflict` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::Backend` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::Unavailable` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::Version` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkError::NotLeader` | dict key | `ForkError` | serde-dict, plain-value |
| `ForkHandle` | `ForkHandle` | `ForkHandle` |  |
| `ForkHandle::create` | `ForkHandle.create` | `ForkHandle.create` |  |
| `ForkHandle::id` | `ForkHandle.id` | `ForkHandle.id` | property |
| `ForkHandle::promote` | `ForkHandle.promote` | `ForkHandle.promote` |  |
| `ForkHandle::put_row` | `ForkHandle.put_row` | `ForkHandle.putRow` |  |
| `ForkHandle::squash` | `ForkHandle.squash` | `ForkHandle.squash` |  |
| `ForkInfo` | dict via `ForkHandle.create` | `ForkInfo` | serde-dict |
| `ForkInfo.fork_id` | dict key | `ForkInfo.forkId` | serde-dict, keywords |
| `ForkInfo.parent` | dict key | `ForkInfo.parent` | serde-dict, keywords |
| `ForkInfo.kind` | dict key | `ForkInfo.kind` | serde-dict, keywords |
| `ForkInfo.user_id` | dict key | `ForkInfo.userId` | serde-dict, keywords |
| `ForkInfo.status` | dict key | `ForkInfo.status` | serde-dict, keywords |
| `ForkInfo.created_at_micros` | dict key | `ForkInfo.createdAtMicros` | serde-dict, keywords |
| `ForkInfo.row_count` | dict key | `ForkInfo.rowCount` | serde-dict, keywords |
| `ForkKind` | omitted | `ForkKind` | plain-value |
| `ForkKind::Severed` | omitted | `ForkKind` | plain-value |
| `ForkKind::Continuous` | omitted | `ForkKind` | plain-value |
| `ForkPutRequest` | `ForkPutRequest` | `ForkPutRequest` |  |
| `ForkPutRequest::embedding` | `ForkPutRequest.embedding` | `ForkPutRequest.embedding` |  |
| `ForkPutRequest::field` | `ForkPutRequest.field` | `ForkPutRequest.field` |  |
| `ForkPutRequest::metadata` | `ForkPutRequest.metadata` | `ForkPutRequest.metadata` |  |
| `ForkPutRequest::payload` | `ForkPutRequest.payload` | `ForkPutRequest.payload` |  |
| `ForkPutRequest::projection` | `ForkPutRequest.projection` | `ForkPutRequest.projection` |  |
| `ForkPutRequest::send` | `ForkPutRequest.send` | `ForkPutRequest.send` |  |
| `ForkPutRequest::tombstone` | `ForkPutRequest.tombstone` | `ForkPutRequest.tombstone` |  |
| `ForkStatus` | omitted | `ForkStatus` | plain-value |
| `ForkStatus::Open` | omitted | `ForkStatus` | plain-value |
| `ForkStatus::Promoted` | omitted | `ForkStatus` | plain-value |
| `ForkStatus::Squashed` | omitted | `ForkStatus` | plain-value |

## govern

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ActionCounters` | `ActionCounters` | `ActionCounters` |  |
| `ActionCounters.sends` | `ActionCounters.sends` | `ActionCounters.sends` | keywords |
| `ActionCounters.requests` | `ActionCounters.requests` | `ActionCounters.requests` | keywords |
| `ActionCounters.bytes_sent` | `ActionCounters.bytes_sent` | `ActionCounters.bytesSent` | keywords |
| `ActionDecision` | `ActionDecision` | `ActionDecision` |  |
| `ActionDecision.verdict` | `ActionDecision.verdict` | `ActionDecision.verdict` |  |
| `ActionDecision.reason` | `ActionDecision.reason` | `ActionDecision.reason` |  |
| `ActionDecision.policy` | `ActionDecision.policy` | `ActionDecision.policy` |  |
| `ActionDecision.risk_score` | `ActionDecision.risk_score` | `ActionDecision.riskScore` |  |
| `ActionDecision::allow` | `ActionDecision.allow` | `ActionDecision.allow` |  |
| `ActionDecision::block` | `ActionDecision.block` | `ActionDecision.block` |  |
| `ActionDecision::defer` | `ActionDecision.defer` | `ActionDecision.defer` |  |
| `ActionDecision::modify` | `ActionDecision.modify` | `ActionDecision.modify` |  |
| `ActionDecision::observe` | `ActionDecision.observe` | `ActionDecision.observe` |  |
| `ActionDecision::step_up` | `ActionDecision.step_up` | `ActionDecision.stepUp` |  |
| `ActionDecision::with_policy` | `ActionDecision.with_policy` | `ActionDecision.withPolicy` |  |
| `ActionDecision::with_reason` | `ActionDecision.with_reason` | `ActionDecision.withReason` |  |
| `ActionDecision::with_risk_score` | `ActionDecision.with_risk_score` | `ActionDecision.withRiskScore` |  |
| `ActionGovernor` | `Laser.with_governor(governor=)` | `ActionGovernor` | callback |
| `ActionGovernor::decide` | `Laser.with_governor(governor=)` | `ActionGovernor.decide` | callback, keywords |
| `ActionKind` | omitted | `ActionKind` | plain-value |
| `ActionKind::Send` | omitted | `ActionKind.Send` | plain-value |
| `ActionKind::Publish` | omitted | `ActionKind.Publish` | plain-value |
| `ActionKind::Request` | omitted | `ActionKind.Request` | plain-value |
| `ActionKind::Command` | omitted | `ActionKind.Command` | plain-value |
| `ActionKind::Response` | omitted | `ActionKind.Response` | plain-value |
| `ActionKind::Event` | omitted | `ActionKind.Event` | plain-value |
| `ActionKind::Status` | omitted | `ActionKind.Status` | plain-value |
| `ActionKind::Error` | omitted | `ActionKind.Error` | plain-value |
| `ActionKind::MemoryWrite` | omitted | `ActionKind.MemoryWrite` | plain-value |
| `ActionKind::as_str` | omitted | omitted | plain-value |
| `GovernedAction` | `GovernedAction` | `GovernedAction` |  |
| `GovernedAction.kind` | `GovernedAction.kind` | `GovernedAction.kind` | keywords |
| `GovernedAction.stream` | `GovernedAction.stream` | `GovernedAction.stream` | keywords |
| `GovernedAction.topic` | `GovernedAction.topic` | `GovernedAction.topic` | keywords |
| `GovernedAction.source` | `GovernedAction.source` | `GovernedAction.source` | keywords |
| `GovernedAction.target` | `GovernedAction.target` | `GovernedAction.target` | keywords |
| `GovernedAction.conversation` | `GovernedAction.conversation` | `GovernedAction.conversation` | keywords |
| `GovernedAction.correlation` | `GovernedAction.correlation` | `GovernedAction.correlation` | keywords |
| `GovernedAction.operation` | `GovernedAction.operation` | `GovernedAction.operation` | keywords |
| `GovernedAction.tool` | `GovernedAction.tool` | `GovernedAction.tool` | keywords |
| `GovernedAction.on_behalf_of` | `GovernedAction.on_behalf_of` | `GovernedAction.onBehalfOf` | keywords |
| `GovernedAction.purpose` | `GovernedAction.purpose` | `GovernedAction.purpose` | keywords |
| `GovernedAction.data_classification` | `GovernedAction.data_classification` | `GovernedAction.dataClassification` | keywords |
| `GovernedAction.payload` | `GovernedAction.payload` | `GovernedAction.payload` | keywords |
| `GovernedAction.signed` | `GovernedAction.signed` | `GovernedAction.signed` | keywords |
| `GovernedAction.counters` | `GovernedAction.counters` | `GovernedAction.counters` | keywords |
| `GovernorMode` | omitted | `GovernorMode` | plain-value |
| `GovernorMode::Observe` | omitted | `GovernorMode.Observe` | plain-value |
| `GovernorMode::Enforce` | omitted | `GovernorMode.Enforce` | plain-value |
| `GovernorMode::as_str` | omitted | omitted | plain-value |
| `GovernorRetention` | `GovernorRetention` | `GovernorRetention` |  |
| `GovernorRetention.capacity` | `GovernorRetention.capacity` | `GovernorRetention.capacity` | keywords |
| `GovernorRetention.idle_ttl` | `GovernorRetention.idle_ttl_secs` | `GovernorRetention.idleTtlMs` | keywords |
| `govern::POLICY_DECISION_OPERATION` | `const:POLICY_DECISION_OPERATION` | `const:POLICY_DECISION_OPERATION` |  |
| `PolicyEvidence` | `PolicyEvidence` | `PolicyEvidence` |  |
| `PolicyEvidence.decision_id` | `PolicyEvidence.decision_id` | `PolicyEvidence.decisionId` | keywords |
| `PolicyEvidence.decision` | `PolicyEvidence.decision` | `PolicyEvidence.decision` | keywords |
| `PolicyEvidence.mode` | `PolicyEvidence.mode` | `PolicyEvidence.mode` | keywords |
| `PolicyEvidence.kind` | `PolicyEvidence.kind` | `PolicyEvidence.kind` | keywords |
| `PolicyEvidence.stream` | `PolicyEvidence.stream` | `PolicyEvidence.stream` | keywords |
| `PolicyEvidence.topic` | `PolicyEvidence.topic` | `PolicyEvidence.topic` | keywords |
| `PolicyEvidence.source` | `PolicyEvidence.source` | `PolicyEvidence.source` | keywords |
| `PolicyEvidence.target` | `PolicyEvidence.target` | `PolicyEvidence.target` | keywords |
| `PolicyEvidence.conversation` | `PolicyEvidence.conversation` | `PolicyEvidence.conversation` | keywords |
| `PolicyEvidence.correlation` | `PolicyEvidence.correlation` | `PolicyEvidence.correlation` | keywords |
| `PolicyEvidence.operation` | `PolicyEvidence.operation` | `PolicyEvidence.operation` | keywords |
| `PolicyEvidence.tool` | `PolicyEvidence.tool` | `PolicyEvidence.tool` | keywords |
| `PolicyEvidence.on_behalf_of` | `PolicyEvidence.on_behalf_of` | `PolicyEvidence.onBehalfOf` | keywords |
| `PolicyEvidence.reason` | `PolicyEvidence.reason` | `PolicyEvidence.reason` | keywords |
| `PolicyEvidence.approved_scope` | `PolicyEvidence.approved_scope` | `PolicyEvidence.approvedScope` | keywords |
| `PolicyEvidence.policy` | `PolicyEvidence.policy` | `PolicyEvidence.policy` | keywords |
| `PolicyEvidence.risk_score` | `PolicyEvidence.risk_score` | `PolicyEvidence.riskScore` | keywords |
| `PolicyEvidence.receipt_digest` | `PolicyEvidence.receipt_digest` | `PolicyEvidence.receiptDigest` | keywords |
| `PolicyEvidence.previous_digest` | `PolicyEvidence.previous_digest` | `PolicyEvidence.previousDigest` | keywords |
| `PolicyEvidence.outcome` | `PolicyEvidence.outcome` | `PolicyEvidence.outcome` | keywords |
| `PolicyEvidence.at_micros` | `PolicyEvidence.at_micros` | `PolicyEvidence.atMicros` | keywords |
| `PolicyEvidence::decode` | `PolicyEvidence.decode` | `fn:decodePolicyEvidence` | free-function |
| `PolicyEvidence::encode` | `PolicyEvidence.encode` | `fn:encodePolicyEvidence` | free-function |
| `PolicyRef` | `PolicyRef` | `PolicyRef` |  |
| `PolicyRef.pack_id` | `PolicyRef.pack_id` | `PolicyRef.packId` | keywords |
| `PolicyRef.pack_version` | `PolicyRef.pack_version` | `PolicyRef.packVersion` | keywords |
| `PolicyRef.rule_ids` | `PolicyRef.rule_ids` | `PolicyRef.ruleIds` | keywords |
| `QuorumGovernor` | `QuorumGovernor` | `QuorumGovernor` |  |
| `QuorumGovernor::new` | `new QuorumGovernor()` | `new QuorumGovernor()` | constructor |
| `QuorumGovernor::voter` | `QuorumGovernor.voter` | `QuorumGovernor.voter` |  |
| `QuorumPolicy` | `QuorumPolicy` | `QuorumPolicy` |  |
| `QuorumPolicy::All` | `QuorumPolicy.all` | `QuorumPolicy` | plain-value |
| `QuorumPolicy::Any` | `QuorumPolicy.any` | `QuorumPolicy` | plain-value |
| `QuorumPolicy::AtLeast` | `QuorumPolicy.at_least` | `QuorumPolicy` | plain-value |
| `SwappableGovernor` | `SwappableGovernor` | `SwappableGovernor` |  |
| `SwappableGovernor::current` | `SwappableGovernor.current` | `SwappableGovernor.current` |  |
| `SwappableGovernor::new` | `new SwappableGovernor()` | `new SwappableGovernor()` | constructor |
| `SwappableGovernor::swap` | `SwappableGovernor.swap` | `SwappableGovernor.swap` |  |
| `govern::Verdict` | `Verdict` | `Verdict` |  |
| `govern::Verdict::Allow` | `Verdict.allow` | `Verdict` | plain-value |
| `govern::Verdict::Observe` | `Verdict.observe` | `Verdict` | plain-value |
| `govern::Verdict::Block` | `Verdict.block` | `Verdict` | plain-value |
| `govern::Verdict::StepUp` | `Verdict.step_up` | `Verdict` | plain-value |
| `govern::Verdict::Modify` | `Verdict.modify` | `Verdict` | plain-value |
| `govern::Verdict::Defer` | `Verdict.defer` | `Verdict` | plain-value |
| `govern::Verdict::as_str` | `Verdict.as_str` | `fn:verdictAsStr` | free-function |
| `govern::verify_evidence_chain` | `fn:verify_evidence_chain` | `fn:verifyEvidenceChain` |  |

## graph

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `GraphHandle` | `GraphHandle` | `GraphHandle` |  |
| `GraphHandle::as_of` | `GraphHandle.as_of` | `GraphHandle.asOf` |  |
| `GraphHandle::both` | `GraphHandle.both` | `GraphHandle.both` |  |
| `GraphHandle::conversation` | `GraphHandle.conversation` | `GraphHandle.conversation` |  |
| `GraphHandle::fetch` | `GraphHandle.fetch` | `GraphHandle.fetch` |  |
| `GraphHandle::incoming` | `GraphHandle.incoming` | `GraphHandle.incoming` |  |
| `GraphHandle::limit` | `GraphHandle.limit` | `GraphHandle.limit` |  |
| `GraphHandle::link` | `GraphHandle.link` | `GraphHandle.link` |  |
| `GraphHandle::neighbors` | `GraphHandle.neighbors` | `GraphHandle.neighbors` |  |
| `GraphHandle::out` | `GraphHandle.out` | `GraphHandle.out` |  |
| `GraphHandle::relink` | `GraphHandle.relink` | `GraphHandle.relink` |  |
| `GraphHandle::return_edges` | `GraphHandle.return_edges` | `GraphHandle.returnEdges` |  |
| `GraphHandle::return_paths` | `GraphHandle.return_paths` | `GraphHandle.returnPaths` |  |
| `GraphHandle::return_triplets` | `GraphHandle.return_triplets` | `GraphHandle.returnTriplets` |  |
| `GraphHandle::start_ids` | `GraphHandle.start_ids` | `GraphHandle.startIds` |  |
| `GraphHandle::start_match` | `GraphHandle.start_match` | `GraphHandle.startMatch` |  |
| `GraphHandle::start_nearest` | `GraphHandle.start_nearest` | `GraphHandle.startNearest` |  |
| `GraphHandle::unlink` | `GraphHandle.unlink` | `GraphHandle.unlink` |  |
| `GraphHandle::upsert` | `GraphHandle.upsert` | `GraphHandle.upsert` |  |

## intent

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Decision` | `Decision` | `Decision` |  |
| `Decision.intent_id` | `Decision.intent_id` | `Decision.intentId` |  |
| `Decision.intent_digest` | `Decision.intent_digest` | `Decision.intentDigest` |  |
| `Decision.policy_version` | `Decision.policy_version` | `Decision.policyVersion` |  |
| `Decision.outcome` | `Decision.outcome` | `Decision.outcome` |  |
| `Decision.reason` | `Decision.reason` | `Decision.reason` |  |
| `Decision.votes_considered` | `Decision.votes_considered` | `Decision.votesConsidered` |  |
| `Decision.at_micros` | `Decision.at_micros` | `Decision.atMicros` |  |
| `Decision::authorizes` | `Decision.authorizes` | `Decision.authorizes` |  |
| `Intent` | `Intent` | `Intent` |  |
| `Intent.intent_id` | `Intent.intent_id` | `Intent.intentId` |  |
| `Intent.conversation` | `Intent.conversation` | `Intent.conversation` |  |
| `Intent.proposer` | `Intent.proposer` | `Intent.proposer` |  |
| `Intent.body` | `Intent.body` | `Intent.body` |  |
| `Intent.digest` | `Intent.digest` | `Intent.digest` |  |
| `Intent.eligible_voters` | `Intent.eligible_voters` | `Intent.eligibleVoters` |  |
| `Intent.mandatory_voters` | `Intent.mandatory_voters` | `Intent.mandatoryVoters` |  |
| `Intent.policy` | `Intent.policy` | `Intent.policy` |  |
| `Intent.policy_version` | `Intent.policy_version` | `Intent.policyVersion` |  |
| `Intent.deadline_micros` | `Intent.deadline_micros` | `Intent.deadlineMicros` |  |
| `Intent.at_micros` | `Intent.at_micros` | `Intent.atMicros` |  |
| `Intent::builder` | `new Intent()` | `IntentOptions` | keywords |
| `Intent::validate` | `Intent.validate` | `Intent.validate` |  |
| `IntentBuilder` | `new Intent()` | `IntentOptions` | keywords |
| `IntentBuilder::body` | `new Intent(body=)` | `IntentOptions.body` | keywords |
| `IntentBuilder::build` | `new Intent()` | `new Intent()` | one-call |
| `IntentBuilder::conversation` | `new Intent(conversation=)` | `IntentOptions.conversation` | keywords |
| `IntentBuilder::deadline_micros` | `new Intent(deadline_micros=)` | `IntentOptions.deadlineMicros` | keywords |
| `IntentBuilder::eligible_voters` | `new Intent(eligible_voters=)` | `IntentOptions.eligibleVoters` | keywords |
| `IntentBuilder::mandatory_voters` | `new Intent(mandatory_voters=)` | `IntentOptions.mandatoryVoters` | keywords |
| `IntentBuilder::maybe_mandatory_voters` | `new Intent(mandatory_voters=)` | `IntentOptions.mandatoryVoters` | keywords |
| `IntentBuilder::policy` | `new Intent(policy=)` | `IntentOptions.policy` | keywords |
| `IntentBuilder::policy_version` | `new Intent(policy_version=)` | `IntentOptions.policyVersion` | keywords |
| `IntentBuilder::proposer` | `new Intent(proposer=)` | `IntentOptions.proposer` | keywords |
| `IntentError` | `IntentError` | `IntentError` |  |
| `IntentError::NoEligibleVoters` | `IntentError.NO_ELIGIBLE_VOTERS` | `IntentError.noEligibleVoters` |  |
| `IntentError::DuplicateEligibleVoter` | `IntentError.DUPLICATE_ELIGIBLE_VOTER` | `IntentError.duplicateEligibleVoter` |  |
| `IntentError::DuplicateMandatoryVoter` | `IntentError.DUPLICATE_MANDATORY_VOTER` | `IntentError.duplicateMandatoryVoter` |  |
| `IntentError::MandatoryVoterNotEligible` | `IntentError.MANDATORY_VOTER_NOT_ELIGIBLE` | `IntentError.mandatoryVoterNotEligible` |  |
| `IntentError::InvalidThreshold` | `IntentError.INVALID_THRESHOLD` | `IntentError.invalidThreshold` |  |
| `IntentError::InvalidDeadline` | `IntentError.INVALID_DEADLINE` | `IntentError.invalidDeadline` |  |
| `IntentError::DigestMismatch` | `IntentError.DIGEST_MISMATCH` | `IntentError.digestMismatch` |  |
| `IntentError::IneligibleVoter` | `IntentError.INELIGIBLE_VOTER` | `IntentError.ineligibleVoter` |  |
| `IntentError::DecisionIntentMismatch` | `IntentError.DECISION_INTENT_MISMATCH` | `IntentError.decisionIntentMismatch` |  |
| `IntentOutcome` | omitted | `IntentOutcome` | plain-value |
| `IntentOutcome::Committed` | omitted | `IntentOutcome.Committed` | plain-value |
| `IntentOutcome::Aborted` | omitted | `IntentOutcome.Aborted` | plain-value |
| `IntentPolicy` | `IntentPolicy` | `IntentPolicy` |  |
| `IntentPolicy::All` | `IntentPolicy.all` | `IntentPolicy` | plain-value |
| `IntentPolicy::Any` | `IntentPolicy.any` | `IntentPolicy` | plain-value |
| `IntentPolicy::AtLeast` | `IntentPolicy.at_least` | `IntentPolicy` | plain-value |
| `Vote` | `Vote` | `Vote` |  |
| `Vote.intent_id` | `Vote.intent_id` | `Vote.intentId` |  |
| `Vote.intent_digest` | `Vote.intent_digest` | `Vote.intentDigest` |  |
| `Vote.policy_version` | `Vote.policy_version` | `Vote.policyVersion` |  |
| `Vote.voter` | `Vote.voter` | `Vote.voter` |  |
| `Vote.choice` | `Vote.choice` | `Vote.choice` |  |
| `Vote.at_micros` | `Vote.at_micros` | `Vote.atMicros` |  |
| `Vote::cast` | `Vote.cast` | `Vote.cast` |  |
| `VoteChoice` | omitted | `VoteChoice` | plain-value |
| `VoteChoice::Allow` | omitted | `VoteChoice.Allow` | plain-value |
| `VoteChoice::Block` | omitted | `VoteChoice.Block` | plain-value |
| `VoteChoice::Abstain` | omitted | `VoteChoice.Abstain` | plain-value |
| `intent::decide` | `fn:decide` | `fn:decide` |  |

## kv

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `kv::AGDX_KV_BASE` | `const:AGDX_KV_BASE` | `const:AGDX_KV_BASE` |  |
| `kv::AGDX_KV_CAS_CODE` | `const:AGDX_KV_CAS_CODE` | `const:AGDX_KV_CAS_CODE` |  |
| `kv::AGDX_KV_CAS_FENCED_CODE` | `const:AGDX_KV_CAS_FENCED_CODE` | `const:AGDX_KV_CAS_FENCED_CODE` |  |
| `kv::AGDX_KV_COPY_CODE` | `const:AGDX_KV_COPY_CODE` | `const:AGDX_KV_COPY_CODE` |  |
| `kv::AGDX_KV_DELETE_CODE` | `const:AGDX_KV_DELETE_CODE` | `const:AGDX_KV_DELETE_CODE` |  |
| `kv::AGDX_KV_DELETE_MANY_CODE` | `const:AGDX_KV_DELETE_MANY_CODE` | `const:AGDX_KV_DELETE_MANY_CODE` |  |
| `kv::AGDX_KV_EXISTS_CODE` | `const:AGDX_KV_EXISTS_CODE` | `const:AGDX_KV_EXISTS_CODE` |  |
| `kv::AGDX_KV_EXPIRE_CODE` | `const:AGDX_KV_EXPIRE_CODE` | `const:AGDX_KV_EXPIRE_CODE` |  |
| `kv::AGDX_KV_GET_CODE` | `const:AGDX_KV_GET_CODE` | `const:AGDX_KV_GET_CODE` |  |
| `kv::AGDX_KV_LEASE_CODE` | `const:AGDX_KV_LEASE_CODE` | `const:AGDX_KV_LEASE_CODE` |  |
| `kv::AGDX_KV_LEASE_RENEW_CODE` | `const:AGDX_KV_LEASE_RENEW_CODE` | `const:AGDX_KV_LEASE_RENEW_CODE` |  |
| `kv::AGDX_KV_MOVE_CODE` | `const:AGDX_KV_MOVE_CODE` | `const:AGDX_KV_MOVE_CODE` |  |
| `kv::AGDX_KV_NAMESPACES_CODE` | `const:AGDX_KV_NAMESPACES_CODE` | `const:AGDX_KV_NAMESPACES_CODE` |  |
| `kv::AGDX_KV_PATCH_CODE` | `const:AGDX_KV_PATCH_CODE` | `const:AGDX_KV_PATCH_CODE` |  |
| `kv::AGDX_KV_RELEASE_CODE` | `const:AGDX_KV_RELEASE_CODE` | `const:AGDX_KV_RELEASE_CODE` |  |
| `kv::AGDX_KV_SCAN_CODE` | `const:AGDX_KV_SCAN_CODE` | `const:AGDX_KV_SCAN_CODE` |  |
| `kv::AGDX_KV_SET_CODE` | `const:AGDX_KV_SET_CODE` | `const:AGDX_KV_SET_CODE` |  |
| `AmbiguousMutationRecovery` | `AmbiguousMutationRecovery` | `AmbiguousMutationRecovery` |  |
| `AmbiguousMutationRecovery::WaitForLeaseExpiry` | `AmbiguousMutationRecovery.wait_for_lease_expiry` | `AmbiguousMutationRecovery` | plain-value |
| `AmbiguousMutationRecovery::RepeatPrepared` | `AmbiguousMutationRecovery.repeat_prepared` | `AmbiguousMutationRecovery` | plain-value |
| `AmbiguousMutationRecovery::ReconcileTargetPrecondition` | `AmbiguousMutationRecovery.reconcile_target_precondition` | `AmbiguousMutationRecovery` | plain-value |
| `CasExpect` | dict via `FencedLeaseClient.prepare_cas_fenced` | `CasExpect` | serde-dict |
| `CasExpect::Match` | dict key | `CasExpect` | serde-dict, plain-value |
| `CasExpect::Absent` | dict key | `CasExpect` | serde-dict, plain-value |
| `kv::DEFAULT_ATTEMPT_TIMEOUT` | `const:DEFAULT_ATTEMPT_TIMEOUT_SECS` | `const:DEFAULT_ATTEMPT_TIMEOUT_MS` |  |
| `kv::DEFAULT_NAMESPACE` | `const:DEFAULT_NAMESPACE` | `const:DEFAULT_NAMESPACE` |  |
| `kv::DEFAULT_SCAN_LIMIT` | `const:DEFAULT_SCAN_LIMIT` | `const:DEFAULT_SCAN_LIMIT` |  |
| `DedicatedKvTransport` | `DedicatedKvTransport` | `DedicatedKvTransport` |  |
| `DedicatedKvTransport::close` | `DedicatedKvTransport.close` | `DedicatedKvTransport.close` |  |
| `DedicatedKvTransport::new` | `new DedicatedKvTransport()` | `new DedicatedKvTransport()` | constructor |
| `DynManagedKvTransport` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport` | callback, trait-variant |
| `DynManagedKvTransport::ready` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.ready` | callback, keywords |
| `DynManagedKvTransport::send` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.send` | callback, keywords |
| `DynManagedKvTransport::reset` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.reset` | callback, keywords |
| `DynManagedKvTransport::close` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.close` | callback, keywords |
| `FencedLeaseClient` | `FencedLeaseClient` | `FencedLeaseClient` |  |
| `FencedLeaseClient::acquire` | `FencedLeaseClient.acquire` | `FencedLeaseClient.acquire` |  |
| `FencedLeaseClient::cas_fenced` | `FencedLeaseClient.cas_fenced` | `FencedLeaseClient.casFenced` |  |
| `FencedLeaseClient::close` | `FencedLeaseClient.close` | `FencedLeaseClient.close` |  |
| `FencedLeaseClient::connect_dedicated` | `FencedLeaseClient.connect_dedicated` | `FencedLeaseClient.connectDedicated` |  |
| `FencedLeaseClient::get` | `FencedLeaseClient.get` | `FencedLeaseClient.get` |  |
| `FencedLeaseClient::new` | `new FencedLeaseClient()` | `new FencedLeaseClient()` | constructor |
| `FencedLeaseClient::prepare_acquire` | `FencedLeaseClient.prepare_acquire` | `FencedLeaseClient.prepareAcquire` |  |
| `FencedLeaseClient::prepare_cas_fenced` | `FencedLeaseClient.prepare_cas_fenced` | `FencedLeaseClient.prepareCasFenced` |  |
| `FencedLeaseClient::prepare_release` | `FencedLeaseClient.prepare_release` | `FencedLeaseClient.prepareRelease` |  |
| `FencedLeaseClient::prepare_renew` | `FencedLeaseClient.prepare_renew` | `FencedLeaseClient.prepareRenew` |  |
| `FencedLeaseClient::release` | `FencedLeaseClient.release` | `FencedLeaseClient.release` |  |
| `FencedLeaseClient::renew` | `FencedLeaseClient.renew` | `FencedLeaseClient.renew` |  |
| `FencedLeaseClient::with_attempt_timeout` | `FencedLeaseClient.with_attempt_timeout` | `FencedLeaseClient.withAttemptTimeout` |  |
| `kv::KV_LEASE_OP_VERSION` | `const:KV_LEASE_OP_VERSION` | `const:KV_LEASE_OP_VERSION` |  |
| `kv::KV_OP_VERSION` | `const:KV_OP_VERSION` | `const:KV_OP_VERSION` |  |
| `Kv` | `Kv` | `Kv` |  |
| `Kv::cas_fenced` | `Kv.cas_fenced` | `Kv.casFenced` |  |
| `Kv::copy_to` | `Kv.copy_to` | `Kv.copyTo` |  |
| `Kv::delete` | `Kv.delete` | `Kv.delete` |  |
| `Kv::delete_many` | `Kv.delete_many` | `Kv.deleteMany` |  |
| `Kv::exists` | `Kv.exists` | `Kv.exists` |  |
| `Kv::expire` | `Kv.expire` | `Kv.expire` |  |
| `Kv::expire_at` | `Kv.expire_at` | `Kv.expireAt` |  |
| `Kv::get` | `Kv.get` | `Kv.get` |  |
| `Kv::get_as` | `Kv.get_as` | `Kv.getAs` |  |
| `Kv::get_entry` | `Kv.get_entry` | `Kv.getEntry` |  |
| `Kv::get_entry_at_least` | `Kv.get_entry_at_least` | `Kv.getEntryAtLeast` |  |
| `Kv::get_many` | `Kv.get_many` | `Kv.getMany` |  |
| `Kv::get_typed` | `Kv.get_typed` | `Kv.getTyped` |  |
| `Kv::lease` | `Kv.lease` | `Kv.lease` |  |
| `Kv::move_to` | `Kv.move_to` | `Kv.moveTo` |  |
| `Kv::namespace` | `Kv.namespace` | `Kv.namespace` | property |
| `Kv::patch` | `Kv.patch` | `Kv.patch` |  |
| `Kv::release` | `Kv.release` | `Kv.release` |  |
| `Kv::renew_lease` | `Kv.renew_lease` | `Kv.renewLease` |  |
| `Kv::scan` | `Kv.scan` | `Kv.scan` |  |
| `Kv::set` | `Kv.set` | `Kv.set` |  |
| `KvCasFenced` | dict via `FencedLeaseClient.prepare_cas_fenced` | `KvCasFenced` | serde-dict |
| `KvCasFenced.v` | dict key | `KvCasFenced.v` | serde-dict, keywords |
| `KvCasFenced.namespace` | dict key | `KvCasFenced.namespace` | serde-dict, keywords |
| `KvCasFenced.key` | dict key | `KvCasFenced.key` | serde-dict, keywords |
| `KvCasFenced.value` | dict key | `KvCasFenced.value` | serde-dict, keywords |
| `KvCasFenced.expires_at_micros` | dict key | `KvCasFenced.expiresAtMicros` | serde-dict, keywords |
| `KvCasFenced.expect` | dict key | `KvCasFenced.expect` | serde-dict, keywords |
| `KvCasFenced.fence_namespace` | dict key | `KvCasFenced.fenceNamespace` | serde-dict, keywords |
| `KvCasFenced.fence_key` | dict key | `KvCasFenced.fenceKey` | serde-dict, keywords |
| `KvCasFenced.fence_token` | dict key | `KvCasFenced.fenceToken` | serde-dict, keywords |
| `KvCasFencedRequest` | `KvCasFencedRequest` | `KvCasFencedRequest` |  |
| `KvCasFencedRequest::bytes` | `KvCasFencedRequest.bytes` | `KvCasFencedRequest.bytes` |  |
| `KvCasFencedRequest::commit` | `KvCasFencedRequest.commit` | `KvCasFencedRequest.commit` |  |
| `KvCasFencedRequest::encode_with` | `KvCasFencedRequest.encode_with` | `KvCasFencedRequest.encodeWith` |  |
| `KvCasFencedRequest::expect_absent` | `KvCasFencedRequest.expect_absent` | `KvCasFencedRequest.expectAbsent` |  |
| `KvCasFencedRequest::expect_version` | `KvCasFencedRequest.expect_version` | `KvCasFencedRequest.expectVersion` |  |
| `KvCasFencedRequest::expires_at` | `KvCasFencedRequest.expires_at` | `KvCasFencedRequest.expiresAt` |  |
| `KvCasFencedRequest::json` | `KvCasFencedRequest.json` | `KvCasFencedRequest.json` |  |
| `KvCasFencedRequest::msgpack` | `KvCasFencedRequest.msgpack` | `KvCasFencedRequest.msgpack` |  |
| `KvCasFencedRequest::ttl` | `KvCasFencedRequest.ttl` | `KvCasFencedRequest.ttl` |  |
| `KvCopyRequest` | `KvCopyRequest` | `KvCopyRequest` |  |
| `KvCopyRequest::into_namespace` | `KvCopyRequest.into_namespace` | `KvCopyRequest.intoNamespace` |  |
| `KvCopyRequest::send` | `KvCopyRequest.send` | `KvCopyRequest.send` |  |
| `KvDeleteManyRequest` | `KvDeleteManyRequest` | `KvDeleteManyRequest` |  |
| `KvDeleteManyRequest::conversation` | `KvDeleteManyRequest.conversation` | `KvDeleteManyRequest.conversation` |  |
| `KvDeleteManyRequest::key_contains` | `KvDeleteManyRequest.key_contains` | `KvDeleteManyRequest.keyContains` |  |
| `KvDeleteManyRequest::prefix` | `KvDeleteManyRequest.prefix` | `KvDeleteManyRequest.prefix` |  |
| `KvDeleteManyRequest::range` | `KvDeleteManyRequest.range` | `KvDeleteManyRequest.range` |  |
| `KvDeleteManyRequest::send` | `KvDeleteManyRequest.send` | `KvDeleteManyRequest.send` |  |
| `KvEntry` | `KvEntry` | `KvEntry` |  |
| `KvEntry.key` | `KvEntry.key` | `KvEntry.key` | keywords |
| `KvEntry.value` | `KvEntry.value` | `KvEntry.value` | keywords |
| `KvEntry.expires_at_micros` | `KvEntry.expires_at_micros` | `KvEntry.expiresAtMicros` | keywords |
| `KvEntry.version` | `KvEntry.version` | `KvEntry.version` | keywords |
| `KvEntry.scope` | `KvEntry.scope` | `KvEntry.scope` | keywords |
| `KvEntry.source` | `KvEntry.source` | `KvEntry.source` | keywords |
| `KvEntry::decode_value` | `KvEntry.decode_value` | `fn:kvEntryDecodeValue` | free-function |
| `KvEntry::decode_value_with` | `KvEntry.decode_value_with` | `fn:kvEntryDecodeValueWith` | free-function |
| `KvEntry::key_str` | `KvEntry.key_str` | `fn:kvEntryKeyStr` | free-function |
| `KvError` | `KvError` | `KvError` |  |
| `KvError::Unsupported` | `KvError.unsupported` | `KvError` | plain-value |
| `KvError::InvalidKey` | dict key | `KvError` | serde-dict, plain-value |
| `KvError::InvalidNamespace` | dict key | `KvError` | serde-dict, plain-value |
| `KvError::TooLarge` | dict key | `KvError` | serde-dict, plain-value |
| `KvError::Backend` | dict key | `KvError` | serde-dict, plain-value |
| `KvError::Unavailable` | `KvError.unavailable` | `KvError` | plain-value |
| `KvError::Version` | dict key | `KvError` | serde-dict, plain-value |
| `KvError::VersionConflict` | `KvError.version_conflict` | `KvError` | plain-value |
| `KvError::LeaseLost` | `KvError.lease_lost` | `KvError` | plain-value |
| `KvError::Stale` | `KvError.stale` | `KvError` | plain-value |
| `KvError::NotFound` | `KvError.not_found` | `KvError` | plain-value |
| `KvError::NotLeader` | `KvError.not_leader` | `KvError` | plain-value |
| `KvGet` | dict via `FencedLeaseClient.get` | `KvGet` | serde-dict |
| `KvGet.v` | dict key | `KvGet.v` | serde-dict, keywords |
| `KvGet.namespace` | dict key | `KvGet.namespace` | serde-dict, keywords |
| `KvGet.key` | dict key | `KvGet.key` | serde-dict, keywords |
| `KvGet.if_none_match` | dict key | `KvGet.ifNoneMatch` | serde-dict, keywords |
| `KvGet.min_position` | dict key | `KvGet.minPosition` | serde-dict, keywords |
| `KvLease` | dict via `FencedLeaseClient.prepare_acquire` | `KvLease` | serde-dict |
| `KvLease.v` | dict key | `KvLease.v` | serde-dict, keywords |
| `KvLease.namespace` | dict key | `KvLease.namespace` | serde-dict, keywords |
| `KvLease.key` | dict key | `KvLease.key` | serde-dict, keywords |
| `KvLease.lease_ttl_micros` | dict key | `KvLease.leaseTtlMicros` | serde-dict, keywords |
| `KvLease.holder_id` | dict key | `KvLease.holderId` | serde-dict, keywords |
| `KvLease.subject_user_id` | dict key | `KvLease.subjectUserId` | serde-dict, keywords |
| `KvLeaseRenew` | dict via `FencedLeaseClient.prepare_renew` | `KvLeaseRenew` | serde-dict |
| `KvLeaseRenew.v` | dict key | `KvLeaseRenew.v` | serde-dict, keywords |
| `KvLeaseRenew.namespace` | dict key | `KvLeaseRenew.namespace` | serde-dict, keywords |
| `KvLeaseRenew.key` | dict key | `KvLeaseRenew.key` | serde-dict, keywords |
| `KvLeaseRenew.holder_id` | dict key | `KvLeaseRenew.holderId` | serde-dict, keywords |
| `KvLeaseRenew.subject_user_id` | dict key | `KvLeaseRenew.subjectUserId` | serde-dict, keywords |
| `KvLeaseRenew.lease_token` | dict key | `KvLeaseRenew.leaseToken` | serde-dict, keywords |
| `KvLeaseRenew.lease_ttl_micros` | dict key | `KvLeaseRenew.leaseTtlMicros` | serde-dict, keywords |
| `KvMetadata` | `KvMetadata` | `KvMetadata` |  |
| `KvMetadata.version` | `KvMetadata.version` | `KvMetadata.version` | keywords |
| `KvMetadata.expires_at_micros` | `KvMetadata.expires_at_micros` | `KvMetadata.expiresAtMicros` | keywords |
| `KvMetadata.size_bytes` | `KvMetadata.size_bytes` | `KvMetadata.sizeBytes` | keywords |
| `KvNamespaceInfo` | dict via `Laser.kv_namespaces` | `KvNamespaceInfo` | serde-dict |
| `KvNamespaceInfo.namespace` | dict key | `KvNamespaceInfo.namespace` | serde-dict, keywords |
| `KvNamespaceInfo.entries` | dict key | `KvNamespaceInfo.entries` | serde-dict, keywords |
| `KvPage` | `KvPage` | `KvPage` |  |
| `KvPage.entries` | `KvPage.entries` | `KvPage.entries` | keywords |
| `KvPage.cursor` | `KvPage.cursor` | `KvPage.cursor` | keywords |
| `KvRelease` | dict via `FencedLeaseClient.prepare_release` | `KvRelease` | serde-dict |
| `KvRelease.v` | dict key | `KvRelease.v` | serde-dict, keywords |
| `KvRelease.namespace` | dict key | `KvRelease.namespace` | serde-dict, keywords |
| `KvRelease.key` | dict key | `KvRelease.key` | serde-dict, keywords |
| `KvRelease.lease_token` | dict key | `KvRelease.leaseToken` | serde-dict, keywords |
| `KvRelease.holder_id` | dict key | `KvRelease.holderId` | serde-dict, keywords |
| `KvScanRequest` | `KvScanRequest` | `KvScanRequest` |  |
| `KvScanRequest::conversation` | `KvScanRequest.conversation` | `KvScanRequest.conversation` |  |
| `KvScanRequest::cursor` | `KvScanRequest.cursor` | `KvScanRequest.cursor` |  |
| `KvScanRequest::entries` | `KvScanRequest.entries` | `KvScanRequest.entries` |  |
| `KvScanRequest::fetch` | `KvScanRequest.fetch` | `KvScanRequest.fetch` |  |
| `KvScanRequest::key_contains` | `KvScanRequest.key_contains` | `KvScanRequest.keyContains` |  |
| `KvScanRequest::limit` | `KvScanRequest.limit` | `KvScanRequest.limit` |  |
| `KvScanRequest::prefix` | `KvScanRequest.prefix` | `KvScanRequest.prefix` |  |
| `KvScanRequest::range` | `KvScanRequest.range` | `KvScanRequest.range` |  |
| `KvSetRequest` | `KvSetRequest` | `KvSetRequest` |  |
| `KvSetRequest::bytes` | `KvSetRequest.bytes` | `KvSetRequest.bytes` |  |
| `KvSetRequest::commit` | `KvSetRequest.commit` | `KvSetRequest.commit` |  |
| `KvSetRequest::encode_with` | `KvSetRequest.encode_with` | `KvSetRequest.encodeWith` |  |
| `KvSetRequest::expect_absent` | `KvSetRequest.expect_absent` | `KvSetRequest.expectAbsent` |  |
| `KvSetRequest::expect_version` | `KvSetRequest.expect_version` | `KvSetRequest.expectVersion` |  |
| `KvSetRequest::expires_at` | `KvSetRequest.expires_at` | `KvSetRequest.expiresAt` |  |
| `KvSetRequest::json` | `KvSetRequest.json` | `KvSetRequest.json` |  |
| `KvSetRequest::msgpack` | `KvSetRequest.msgpack` | `KvSetRequest.msgpack` |  |
| `KvSetRequest::send` | `KvSetRequest.send` | `KvSetRequest.send` |  |
| `KvSetRequest::ttl` | `KvSetRequest.ttl` | `KvSetRequest.ttl` |  |
| `Lease` | `Lease` | `Lease` |  |
| `Lease.token` | `Lease.token` | `Lease.token` | keywords |
| `Lease.granted_ttl` | `Lease.granted_ttl_secs` | `Lease.grantedTtlMicros` | keywords |
| `Lease.position` | `Lease.position` | `Lease.position` | keywords |
| `LocalManagedKvTransport` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport` | callback, trait-variant |
| `LocalManagedKvTransport::ready` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.ready` | callback, keywords |
| `LocalManagedKvTransport::send` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.send` | callback, keywords |
| `LocalManagedKvTransport::reset` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.reset` | callback, keywords |
| `LocalManagedKvTransport::close` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.close` | callback, keywords |
| `kv::MAX_HOLDER_ID_BYTES` | `const:MAX_HOLDER_ID_BYTES` | `const:MAX_HOLDER_ID_BYTES` |  |
| `kv::MAX_KEY_BYTES` | `const:MAX_KEY_BYTES` | `const:MAX_KEY_BYTES` |  |
| `kv::MAX_LEASE_TTL_MICROS` | `const:MAX_LEASE_TTL_MICROS` | `const:MAX_LEASE_TTL_MICROS` |  |
| `kv::MAX_SCAN_LIMIT` | `const:MAX_SCAN_LIMIT` | `const:MAX_SCAN_LIMIT` |  |
| `kv::MAX_VALUE_BYTES` | `const:MAX_VALUE_BYTES` | `const:MAX_VALUE_BYTES` |  |
| `kv::MIN_LEASE_TTL_MICROS` | `const:MIN_LEASE_TTL_MICROS` | `const:MIN_LEASE_TTL_MICROS` |  |
| `ManagedKvTransport` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport` | callback |
| `ManagedKvTransport::ready` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.ready` | callback, keywords |
| `ManagedKvTransport::send` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.send` | callback, keywords |
| `ManagedKvTransport::reset` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.reset` | callback, keywords |
| `ManagedKvTransport::close` | `new FencedLeaseClient(transport=)` | `ManagedKvTransport.close` | callback, keywords |
| `MemoryRowScope` | dict via `KvEntry.scope` | `wire.MemoryRowScope` | serde-dict |
| `MemoryRowScope.kind` | dict key | `wire.MemoryRowScope.kind` | serde-dict, keywords |
| `MemoryRowScope.agent` | dict key | `wire.MemoryRowScope.agent` | serde-dict, keywords |
| `MemoryRowScope.user` | dict key | `wire.MemoryRowScope.user` | serde-dict, keywords |
| `MemoryRowScope.app` | dict key | `wire.MemoryRowScope.app` | serde-dict, keywords |
| `MemoryRowScope.conversation` | dict key | `wire.MemoryRowScope.conversation` | serde-dict, keywords |
| `MemoryRowScope.source` | dict key | `wire.MemoryRowScope.source` | serde-dict, keywords |
| `MutationPosition` | `MutationPosition` | `MutationPosition` |  |
| `MutationPosition.topic_generation` | `MutationPosition.topic_generation` | `MutationPosition.topicGeneration` | keywords |
| `MutationPosition.partition` | `MutationPosition.partition` | `MutationPosition.partition` | keywords |
| `MutationPosition.offset` | `MutationPosition.offset` | `MutationPosition.offset` | keywords |
| `PreparedMutation` | `PreparedMutation` | `PreparedMutation` |  |
| `PreparedMutation::ambiguous_recovery` | `PreparedMutation.ambiguous_recovery` | `PreparedMutation.ambiguousRecovery` | property, keywords |
| `PreparedMutation::operation_id` | `PreparedMutation.operation_id` | `PreparedMutation.operationId` | property, keywords |
| `SharedKvTransport` | omitted | omitted | shared-ownership |

## laser

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `laser::CORRELATION_ID` | `const:CORRELATION_ID` | `const:CORRELATION_ID` |  |
| `laser::HEADER_FRAMING_BYTES` | `const:HEADER_FRAMING_BYTES` | `const:HEADER_FRAMING_BYTES` |  |
| `laser::HEADER_SOFT_CAP` | `const:HEADER_SOFT_CAP` | `const:HEADER_SOFT_CAP` |  |
| `laser::HEADER_VALUE_MAX` | `const:HEADER_VALUE_MAX` | `const:HEADER_VALUE_MAX` |  |
| `LaserBuilder` | `Laser.connect` | `LaserBuilder` | keywords |
| `LaserBuilder::address` | `Laser.connect(address=)` | `LaserBuilder.address` | keywords |
| `LaserBuilder::build` | `Laser.connect` | `LaserBuilder.connect` | one-call |
| `LaserBuilder::capabilities` | `Laser.connect(capabilities=)` | `LaserBuilder.capabilities` | keywords |
| `LaserBuilder::changes_topic` | `Laser.connect(changes_topic=)` | `LaserBuilder.changesTopic` | keywords |
| `LaserBuilder::client` | omitted | `LaserBuilder.client` | rust-crate |
| `LaserBuilder::connect_timeout` | `Laser.connect(connect_timeout_ms=)` | `LaserBuilder.connectTimeout` | keywords |
| `LaserBuilder::connection_string` | `Laser.connect(connection_string=)` | `LaserBuilder.connectionString` | keywords |
| `LaserBuilder::control_topic` | `Laser.connect(control_topic=)` | `LaserBuilder.controlTopic` | keywords |
| `LaserBuilder::credentials` | `Laser.connect(credentials=)` | `LaserBuilder.credentials` | keywords |
| `LaserBuilder::dlq_topic` | `Laser.connect(dlq_topic=)` | `LaserBuilder.dlqTopic` | keywords |
| `LaserBuilder::governor` | `Laser.connect(governor=)` | `LaserBuilder.governor` | keywords |
| `LaserBuilder::governor_with_retention` | `Laser.connect(governor=, governor_mode=, governor_retention=)` | `LaserBuilder.governor(retention=)` | keywords, overload |
| `LaserBuilder::ops_stream` | `Laser.connect(ops_stream=)` | `LaserBuilder.opsStream` | keywords |
| `LaserBuilder::publish_max_retries` | `Laser.connect(publish_max_retries=)` | `LaserBuilder.publishMaxRetries` | keywords |
| `LaserBuilder::publish_retry_backoff` | `Laser.connect(publish_retry_backoff_ms=)` | `LaserBuilder.publishRetryBackoff` | keywords |
| `LaserBuilder::publish_timeout` | `Laser.connect(publish_timeout_ms=)` | `LaserBuilder.publishTimeout` | keywords |
| `LaserBuilder::stream` | `Laser.connect(stream=)` | `LaserBuilder.stream` | keywords |
| `LaserBuilder::verifier` | `Laser.connect(verifier=)` | `LaserBuilder.verifier` | keywords |
| `laser::OPS_STREAM_DEFAULT` | `const:OPS_STREAM_DEFAULT` | `const:OPS_STREAM_DEFAULT` |  |
| `PublishOptions` | `Laser.connect` | `PublishOptions` | keywords |
| `PublishOptions.timeout` | `Laser.connect(publish_timeout_ms=)` | `PublishOptions.timeoutMs` | keywords |
| `PublishOptions.max_retries` | `Laser.connect(publish_max_retries=)` | `PublishOptions.maxRetries` | keywords |
| `PublishOptions.retry_backoff` | `Laser.connect(publish_retry_backoff_ms=)` | `PublishOptions.retryBackoffMs` | keywords |

## mcp

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `McpBridge` | `McpBridge` | `McpBridge` |  |
| `McpBridge::call_tool` | `McpBridge.call_tool` | `McpBridge.callTool` |  |
| `McpBridge::get_prompt` | `McpBridge.get_prompt` | `McpBridge.getPrompt` |  |
| `McpBridge::handle_rpc` | `McpBridge.handle_rpc` | `McpBridge.handleRpc` |  |
| `McpBridge::initialize` | `McpBridge.initialize` | `McpBridge.initialize` |  |
| `McpBridge::list_prompts` | `McpBridge.list_prompts` | `McpBridge.listPrompts` |  |
| `McpBridge::list_resources` | `McpBridge.list_resources` | `McpBridge.listResources` |  |
| `McpBridge::list_tools` | `McpBridge.list_tools` | `McpBridge.listTools` |  |
| `McpBridge::new` | `new McpBridge()` | `new McpBridge()` | constructor |
| `McpBridge::read_resource` | `McpBridge.read_resource` | `McpBridge.readResource` |  |
| `McpBridge::router` | omitted | omitted | rust-crate |
| `McpBridge::with_bridge_hops` | `McpBridge.with_bridge_hops` | `McpBridge.withBridgeHops` |  |
| `McpBridge::with_memory_tools` | `new McpBridge(memory_tools=)` | `McpBridge.withMemoryTools` | keywords |
| `McpBridge::with_prompt` | `new McpBridge(prompts=)` | `McpBridge.withPrompt` | keywords |
| `McpBridge::with_resource` | `new McpBridge(resources=)` | `McpBridge.withResource` | keywords |
| `McpBridge::with_timeout` | `new McpBridge(timeout_secs=)` | `McpBridge.withTimeout` | keywords |
| `McpBridge::with_tool` | `new McpBridge(tools=)` | `McpBridge.withTool` | keywords |
| `McpContent` | dict via `McpBridge.call_tool` | `McpContent` | serde-dict |
| `McpContent.kind` | dict key | `McpContent.kind` | serde-dict, keywords |
| `McpContent.text` | dict key | `McpContent.text` | serde-dict, keywords |
| `McpMethod` | omitted | `McpMethod` | plain-value |
| `McpMethod::Initialize` | omitted | `McpMethod.Initialize` | plain-value |
| `McpMethod::ToolsList` | omitted | `McpMethod.ToolsList` | plain-value |
| `McpMethod::ToolsCall` | omitted | `McpMethod.ToolsCall` | plain-value |
| `McpMethod::ResourcesList` | omitted | `McpMethod.ResourcesList` | plain-value |
| `McpMethod::ResourcesRead` | omitted | `McpMethod.ResourcesRead` | plain-value |
| `McpMethod::PromptsList` | omitted | `McpMethod.PromptsList` | plain-value |
| `McpMethod::PromptsGet` | omitted | `McpMethod.PromptsGet` | plain-value |
| `McpPrompt` | dict via `McpBridge.list_prompts` | `McpPrompt` | serde-dict |
| `McpPrompt.name` | dict key | `McpPrompt.name` | serde-dict, keywords |
| `McpPrompt.title` | dict key | `McpPrompt.title` | serde-dict, keywords |
| `McpPrompt.description` | dict key | `McpPrompt.description` | serde-dict, keywords |
| `McpPrompt.arguments` | dict key | `McpPrompt.arguments` | serde-dict, keywords |
| `McpPromptArgument` | dict via `McpBridge.list_prompts` | `McpPromptArgument` | serde-dict |
| `McpPromptArgument.name` | dict key | `McpPromptArgument.name` | serde-dict, keywords |
| `McpPromptArgument.description` | dict key | `McpPromptArgument.description` | serde-dict, keywords |
| `McpPromptArgument.required` | dict key | `McpPromptArgument.required` | serde-dict, keywords |
| `McpResource` | dict via `McpBridge.list_resources` | `McpResource` | serde-dict |
| `McpResource.uri` | dict key | `McpResource.uri` | serde-dict, keywords |
| `McpResource.name` | dict key | `McpResource.name` | serde-dict, keywords |
| `McpResource.title` | dict key | `McpResource.title` | serde-dict, keywords |
| `McpResource.description` | dict key | `McpResource.description` | serde-dict, keywords |
| `McpResource.mime_type` | dict key | `McpResource.mimeType` | serde-dict, keywords |
| `McpRpcError` | dict via `McpBridge.handle_rpc` | `McpRpcError` | serde-dict |
| `McpRpcError.code` | dict key | `McpRpcError.code` | serde-dict, keywords |
| `McpRpcError.message` | dict key | `McpRpcError.message` | serde-dict, keywords |
| `McpRpcRequest` | dict via `McpBridge.handle_rpc` | `McpRpcRequest` | serde-dict |
| `McpRpcRequest.id` | dict key | `McpRpcRequest.id` | serde-dict, keywords |
| `McpRpcRequest.method` | dict key | `McpRpcRequest.method` | serde-dict, keywords |
| `McpRpcRequest.params` | dict key | `McpRpcRequest.params` | serde-dict, keywords |
| `McpRpcResponse` | dict via `McpBridge.handle_rpc` | `McpRpcResponse` | serde-dict |
| `McpRpcResponse.jsonrpc` | dict key | `McpRpcResponse.jsonrpc` | serde-dict, keywords |
| `McpRpcResponse.id` | dict key | `McpRpcResponse.id` | serde-dict, keywords |
| `McpRpcResponse.result` | dict key | `McpRpcResponse.result` | serde-dict, keywords |
| `McpRpcResponse.error` | dict key | `McpRpcResponse.error` | serde-dict, keywords |
| `McpTool` | dict via `McpBridge.list_tools` | `McpTool` | serde-dict |
| `McpTool.name` | dict key | `McpTool.name` | serde-dict, keywords |
| `McpTool.title` | dict key | `McpTool.title` | serde-dict, keywords |
| `McpTool.description` | dict key | `McpTool.description` | serde-dict, keywords |
| `McpTool.input_schema` | dict key | `McpTool.inputSchema` | serde-dict, keywords |
| `McpToolResult` | dict via `McpBridge.call_tool` | `McpToolResult` | serde-dict |
| `McpToolResult.content` | dict key | `McpToolResult.content` | serde-dict, keywords |
| `McpToolResult.is_error` | dict key | `McpToolResult.isError` | serde-dict, keywords |
| `mcp::tool_call_from_request` | `fn:tool_call_from_request` | `fn:toolCallFromRequest` |  |
| `mcp::tool_result_from_envelope` | `fn:tool_result_from_envelope` | `fn:toolResultFromEnvelope` |  |

## memory

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ConsolidationReport` | `ConsolidationReport` | `ConsolidationReport` |  |
| `ConsolidationReport.summarized` | `ConsolidationReport.summarized` | `ConsolidationReport.summarized` | keywords |
| `ConsolidationReport.reweighted` | `ConsolidationReport.reweighted` | `ConsolidationReport.reweighted` | keywords |
| `ConsolidationReport.pruned` | `ConsolidationReport.pruned` | `ConsolidationReport.pruned` | keywords |
| `ConsolidationReport.derived` | `ConsolidationReport.derived` | `ConsolidationReport.derived` | keywords |
| `Consolidator` | `Laser.spawn_agent(consolidator=)` | `Consolidator` | callback |
| `Consolidator::consolidate` | `Laser.spawn_agent(consolidator=)` | `Consolidator.consolidate` | callback, keywords |
| `memory::DEFAULT_MEMORY_TOPIC_TTL` | `const:DEFAULT_MEMORY_TOPIC_TTL_SECS` | `const:DEFAULT_MEMORY_TOPIC_TTL_MS` |  |
| `DefaultConsolidator` | `MemoryHandle.consolidate` | `ConsolidateOptions` | keywords |
| `DefaultConsolidator::new` | `MemoryHandle.consolidate` | `ConsolidateOptions` | keywords |
| `DefaultConsolidator::prune_summarized` | `MemoryHandle.consolidate(prune_summarized=)` | `ConsolidateOptions.pruneSummarized` | keywords |
| `DefaultConsolidator::with_summarizer` | `MemoryHandle.consolidate(summarizer=)` | `ConsolidateOptions.summarizer` | keywords |
| `DynMemory` | `Laser.memory_custom(backend=)` | `Memory` | callback, trait-variant |
| `DynMemory::remember` | `Laser.memory_custom(backend=)` | `Memory.remember` | callback, keywords |
| `DynMemory::append` | `Laser.memory_custom(backend=)` | `Memory.append` | callback, keywords |
| `DynMemory::recall` | `Laser.memory_custom(backend=)` | `Memory.recall` | callback, keywords |
| `DynMemory::improve` | `Laser.memory_custom(backend=)` | `Memory.improve` | callback, keywords |
| `DynMemory::forget` | `Laser.memory_custom(backend=)` | `Memory.forget` | callback, keywords |
| `Embedder` | `MemoryHandle.vector(embedder=)` | `Embedder` | callback |
| `Embedder::embed` | `MemoryHandle.vector(embedder=)` | `Embedder.embed` | callback, keywords |
| `Feedback` | `MemoryHandle.improve` | `Feedback` | keywords |
| `Feedback.target` | `MemoryHandle.improve(target=)` | `Feedback.target` | keywords |
| `Feedback.weight` | `MemoryHandle.improve(weight=)` | `Feedback.weight` | keywords |
| `Feedback.note` | `MemoryHandle.improve(note=)` | `Feedback.note` | keywords |
| `Feedback::new` | `MemoryHandle.improve` | `Feedback` | keywords |
| `Lifetime` | omitted | `Lifetime` | plain-value |
| `Lifetime::Session` | omitted | `Lifetime.Session` | plain-value |
| `Lifetime::Durable` | omitted | `Lifetime.Durable` | plain-value |
| `LocalConsolidator` | `Laser.spawn_agent(consolidator=)` | `Consolidator` | callback, trait-variant |
| `LocalConsolidator::consolidate` | `Laser.spawn_agent(consolidator=)` | `Consolidator.consolidate` | callback, keywords |
| `LocalEmbedder` | `MemoryHandle.vector(embedder=)` | `Embedder` | callback, trait-variant |
| `LocalEmbedder::embed` | `MemoryHandle.vector(embedder=)` | `Embedder.embed` | callback, keywords |
| `LocalMemory` | `Laser.memory_custom(backend=)` | `Memory` | callback, trait-variant |
| `LocalMemory::remember` | `Laser.memory_custom(backend=)` | `Memory.remember` | callback, keywords |
| `LocalMemory::append` | `Laser.memory_custom(backend=)` | `Memory.append` | callback, keywords |
| `LocalMemory::recall` | `Laser.memory_custom(backend=)` | `Memory.recall` | callback, keywords |
| `LocalMemory::improve` | `Laser.memory_custom(backend=)` | `Memory.improve` | callback, keywords |
| `LocalMemory::forget` | `Laser.memory_custom(backend=)` | `Memory.forget` | callback, keywords |
| `LocalReranker` | `MemoryHandle.reranker(reranker=)` | `Reranker` | callback, trait-variant |
| `LocalReranker::rerank` | `MemoryHandle.reranker(reranker=)` | `Reranker.rerank` | callback, keywords |
| `LocalSummarizer` | `MemoryHandle.consolidate(summarizer=)` | `Summarizer` | callback, trait-variant |
| `LocalSummarizer::summarize` | `MemoryHandle.consolidate(summarizer=)` | `Summarizer.summarize` | callback, keywords |
| `LogMemory` | `LogMemory` | `LogMemory` |  |
| `LogMemory::fetch_named` | `LogMemory.fetch_named` | `LogMemory.fetchNamed` |  |
| `LogMemory::fetch_named_folded` | `LogMemory.fetch_named_folded` | `LogMemory.fetchNamedFolded` |  |
| `LogMemory::forget_named` | `LogMemory.forget_named` | `LogMemory.forgetNamed` |  |
| `LogMemory::in_namespace` | `LogMemory.in_namespace` | `new LogMemory()` | overload |
| `LogMemory::new` | `new LogMemory()` | `new LogMemory()` | constructor |
| `LogMemory::on_stream_topic` | `LogMemory.on_stream_topic` | `new LogMemory(topic=, stream=)` | overload |
| `LogMemory::on_stream_topic_named` | `LogMemory.on_stream_topic` | `new LogMemory(namespace=, topic=, stream=)` | overload |
| `LogMemory::on_topic` | `LogMemory.on_topic` | `new LogMemory(topic=)` | overload |
| `LogMemory::on_topic_named` | `LogMemory.on_topic` | `new LogMemory(namespace=, topic=)` | overload |
| `LogMemory::recall_folded` | `MemoryHandle.recall(folded=)` | `LogMemory.recallFolded` | overload |
| `LogMemory::set_named` | `LogMemory.set_named` | `LogMemory.setNamed` |  |
| `LogMemory::update_named` | `LogMemory.update_named` | `LogMemory.updateNamed` |  |
| `MaybeEmbedder` | omitted | omitted | shared-ownership |
| `Memory` | `Laser.memory_custom(backend=)` | `Memory` | callback |
| `Memory::remember` | `Laser.memory_custom(backend=)` | `Memory.remember` | callback, keywords |
| `Memory::append` | `Laser.memory_custom(backend=)` | `Memory.append` | callback, keywords |
| `Memory::recall` | `Laser.memory_custom(backend=)` | `Memory.recall` | callback, keywords |
| `Memory::improve` | `Laser.memory_custom(backend=)` | `Memory.improve` | callback, keywords |
| `Memory::forget` | `Laser.memory_custom(backend=)` | `Memory.forget` | callback, keywords |
| `MemoryBackend` | omitted | `MemoryBackend` | plain-value |
| `MemoryBackend::Auto` | omitted | `MemoryBackend.Auto` | plain-value |
| `MemoryBackend::Log` | omitted | `MemoryBackend.Log` | plain-value |
| `MemoryBackend::Vector` | omitted | `MemoryBackend.Vector` | plain-value |
| `MemoryClass` | omitted | `MemoryClass` | plain-value |
| `MemoryClass::Episodic` | omitted | `MemoryClass.Episodic` | plain-value |
| `MemoryClass::Semantic` | omitted | `MemoryClass.Semantic` | plain-value |
| `MemoryClass::Procedural` | omitted | `MemoryClass.Procedural` | plain-value |
| `MemoryHandle` | `MemoryHandle` | `MemoryHandle` |  |
| `MemoryHandle::Log` | `new LogMemory()` | `MemoryBackendKind` | constructor, plain-value |
| `MemoryHandle::Vector` | `MemoryHandle.vector` | `MemoryBackendKind` | plain-value |
| `MemoryHandle::Reranked` | `new RerankedMemory()` | `MemoryBackendKind` | constructor, plain-value |
| `MemoryHandle::Custom` | `MemoryHandle.custom` | `MemoryBackendKind` | plain-value |
| `MemoryHandle::backend` | `MemoryHandle.backend` | `MemoryHandle.backend` | property |
| `MemoryHandle::consolidate` | `MemoryHandle.consolidate` | `MemoryHandle.consolidate` |  |
| `MemoryHandle::context` | `MemoryHandle.context` | `MemoryHandle.context` |  |
| `MemoryHandle::embedder` | `MemoryHandle.embedder` | `MemoryHandle.embedder` |  |
| `MemoryHandle::fetch` | `MemoryHandle.fetch` | `MemoryHandle.fetch` |  |
| `MemoryHandle::fetch_folded` | `MemoryHandle.fetch_folded` | `MemoryHandle.fetchFolded` |  |
| `MemoryHandle::forget` | `MemoryHandle.forget` | `MemoryHandle.forget` |  |
| `MemoryHandle::improve` | `MemoryHandle.improve` | `MemoryHandle.improve` |  |
| `MemoryHandle::recall` | `MemoryHandle.recall` | `MemoryHandle.recall` |  |
| `MemoryHandle::recall_folded` | `MemoryHandle.recall(folded=)` | `MemoryHandle.recallFolded` | overload |
| `MemoryHandle::remember` | `MemoryHandle.remember` | `MemoryHandle.remember` |  |
| `MemoryHandle::remove` | `MemoryHandle.remove` | `MemoryHandle.remove` |  |
| `MemoryHandle::reranker` | `MemoryHandle.reranker` | `MemoryHandle.reranker` |  |
| `MemoryHandle::set` | `MemoryHandle.set` | `MemoryHandle.set` |  |
| `MemoryHandle::update` | `MemoryHandle.update` | `MemoryHandle.update` |  |
| `MemoryHandle::vector` | `MemoryHandle.vector` | `MemoryHandle.vector` |  |
| `MemoryId` | omitted | `MemoryId` | plain-value |
| `MemoryId::as_u128` | omitted | `MemoryId.asU128` | plain-value |
| `MemoryId::content` | `fn:memory_id_content` | `MemoryId.content` | free-function |
| `MemoryId::from_u128` | omitted | `MemoryId.fromU128` | plain-value |
| `MemoryId::new` | omitted | `MemoryId.new` | plain-value |
| `MemoryItem` | `MemoryItem` | `MemoryItem` |  |
| `MemoryItem.id` | `MemoryItem.id` | `MemoryItem.id` | keywords |
| `MemoryItem.payload` | `MemoryItem.payload` | `MemoryItem.payload` | keywords |
| `MemoryItem.provenance` | `MemoryItem.provenance` | `MemoryItem.provenance` | keywords |
| `MemoryItem.kind` | `MemoryItem.kind` | `MemoryItem.kind` | keywords |
| `MemoryItem.score` | `MemoryItem.score` | `MemoryItem.score` | keywords |
| `MemoryItem.signals` | `MemoryItem.signals` | `MemoryItem.signals` | keywords |
| `MemoryItem.source` | `MemoryItem.source` | `MemoryItem.source` | keywords |
| `MemoryItem::json` | `MemoryItem.json` | `fn:memoryItemJson` | free-function |
| `MemoryItem::text` | `MemoryItem.text` | `fn:memoryItemText` | free-function |
| `MemoryKind` | omitted | `MemoryKind` | plain-value |
| `MemoryKind::Fact` | omitted | `MemoryKind.Fact` | plain-value |
| `MemoryKind::Message` | omitted | `MemoryKind.Message` | plain-value |
| `MemoryKind::Summary` | omitted | `MemoryKind.Summary` | plain-value |
| `MemoryKind::Entity` | omitted | `MemoryKind.Entity` | plain-value |
| `MemoryKind::Feedback` | omitted | `MemoryKind.Feedback` | plain-value |
| `MemoryKind::Procedure` | omitted | `MemoryKind.Procedure` | plain-value |
| `MemoryKind::class` | `fn:memory_kind_class` | `fn:memoryClass` | free-function |
| `MemoryKind::code` | `fn:memory_kind_code` | `fn:memoryKindCode` | free-function |
| `MemoryKind::from_word` | omitted | omitted | plain-value |
| `MemoryQuery` | `MemoryHandle.recall` | `MemoryQuery` | keywords |
| `MemoryQuery.limit` | `MemoryHandle.recall(limit=)` | `MemoryQuery.limit` | keywords |
| `MemoryQuery.token_budget` | `MemoryHandle.recall(token_budget=)` | `MemoryQuery.tokenBudget` | keywords |
| `MemoryQuery.agent` | `MemoryHandle.recall(agent=)` | `MemoryQuery.agent` | keywords |
| `MemoryQuery.semantic` | `MemoryHandle.recall(semantic=)` | `MemoryQuery.semantic` | keywords |
| `MemoryQuery.strategy` | `MemoryHandle.recall(strategy=)` | `MemoryQuery.strategy` | keywords |
| `MemoryQuery::builder` | `MemoryHandle.recall` | `MemoryQuery` | keywords |
| `MemoryQueryBuilder` | `MemoryHandle.recall` | `MemoryQuery` | keywords |
| `MemoryQueryBuilder::agent` | `MemoryHandle.recall(agent=)` | `MemoryQuery.agent` | keywords |
| `MemoryQueryBuilder::build` | `MemoryHandle.recall` | `MemoryQuery` | one-call, keywords |
| `MemoryQueryBuilder::limit` | `MemoryHandle.recall(limit=)` | `MemoryQuery.limit` | keywords |
| `MemoryQueryBuilder::maybe_agent` | `MemoryHandle.recall(agent=)` | `MemoryQuery.agent` | keywords |
| `MemoryQueryBuilder::maybe_limit` | `MemoryHandle.recall(limit=)` | `MemoryQuery.limit` | keywords |
| `MemoryQueryBuilder::maybe_semantic` | `MemoryHandle.recall(semantic=)` | `MemoryQuery.semantic` | keywords |
| `MemoryQueryBuilder::maybe_strategy` | `MemoryHandle.recall(strategy=)` | `MemoryQuery.strategy` | keywords |
| `MemoryQueryBuilder::maybe_token_budget` | `MemoryHandle.recall(token_budget=)` | `MemoryQuery.tokenBudget` | keywords |
| `MemoryQueryBuilder::semantic` | `MemoryHandle.recall(semantic=)` | `MemoryQuery.semantic` | keywords |
| `MemoryQueryBuilder::strategy` | `MemoryHandle.recall(strategy=)` | `MemoryQuery.strategy` | keywords |
| `MemoryQueryBuilder::token_budget` | `MemoryHandle.recall(token_budget=)` | `MemoryQuery.tokenBudget` | keywords |
| `MemoryScope` | `MemoryHandle.remember` | `MemoryScope` | keywords |
| `MemoryScope.stream` | `MemoryHandle.remember(stream=)` | `MemoryScope.stream` | keywords |
| `MemoryScope.user` | `MemoryHandle.remember(user=)` | `MemoryScope.user` | keywords |
| `MemoryScope.agent` | `MemoryHandle.remember(agent=)` | `MemoryScope.agent` | keywords |
| `MemoryScope.conversation` | `MemoryHandle.remember(conversation=)` | `MemoryScope.conversation` | keywords |
| `MemoryScope.app` | `MemoryHandle.remember(application=)` | `MemoryScope.app` | keywords |
| `MemoryScope.lifetime` | `MemoryHandle.remember(durable=)` | `MemoryScope.lifetime` | keywords |
| `MemoryScope::builder` | `MemoryHandle.remember` | `MemoryScope` | keywords |
| `MemoryScopeBuilder` | `MemoryHandle.remember` | `MemoryScope` | keywords |
| `MemoryScopeBuilder::agent` | `MemoryHandle.remember(agent=)` | `MemoryScope.agent` | keywords |
| `MemoryScopeBuilder::app` | `MemoryHandle.remember(application=)` | `MemoryScope.app` | keywords |
| `MemoryScopeBuilder::build` | `MemoryHandle.remember` | `MemoryScope` | one-call, keywords |
| `MemoryScopeBuilder::conversation` | `MemoryHandle.remember(conversation=)` | `MemoryScope.conversation` | keywords |
| `MemoryScopeBuilder::lifetime` | `MemoryHandle.remember(durable=)` | `MemoryScope.lifetime` | keywords |
| `MemoryScopeBuilder::maybe_agent` | `MemoryHandle.remember(agent=)` | `MemoryScope.agent` | keywords |
| `MemoryScopeBuilder::maybe_app` | `MemoryHandle.remember(application=)` | `MemoryScope.app` | keywords |
| `MemoryScopeBuilder::maybe_conversation` | `MemoryHandle.remember(conversation=)` | `MemoryScope.conversation` | keywords |
| `MemoryScopeBuilder::maybe_lifetime` | `MemoryHandle.remember(durable=)` | `MemoryScope.lifetime` | keywords |
| `MemoryScopeBuilder::maybe_stream` | `MemoryHandle.remember(stream=)` | `MemoryScope.stream` | keywords |
| `MemoryScopeBuilder::maybe_user` | `MemoryHandle.remember(user=)` | `MemoryScope.user` | keywords |
| `MemoryScopeBuilder::stream` | `MemoryHandle.remember(stream=)` | `MemoryScope.stream` | keywords |
| `MemoryScopeBuilder::user` | `MemoryHandle.remember(user=)` | `MemoryScope.user` | keywords |
| `MemoryTopicBuilder` | `Laser.memory_topic` | `MemoryTopicBuilder` | keywords |
| `MemoryTopicBuilder::build` | `Laser.memory_topic` | `MemoryTopicBuilder.build` | one-call |
| `MemoryTopicBuilder::no_expiry` | `Laser.memory_topic(ttl_secs=)` | `MemoryTopicBuilder.noExpiry` | overload |
| `MemoryTopicBuilder::partitions` | `Laser.memory_topic(partitions=)` | `MemoryTopicBuilder.partitions` | keywords |
| `MemoryTopicBuilder::stream` | `Laser.memory_topic(stream=)` | `MemoryTopicBuilder.stream` | keywords |
| `MemoryTopicBuilder::ttl` | `Laser.memory_topic(ttl_secs=)` | `MemoryTopicBuilder.ttl` | keywords |
| `NoSummarizer` | `MemoryHandle.consolidate(summarizer=)` | `ConsolidateOptions.summarizer` | overload |
| `RecallBuilder` | `MemoryHandle.recall` | `RecallBuilder` | keywords |
| `RecallBuilder::agent` | `MemoryHandle.recall(agent=)` | `RecallBuilder.agent` | keywords |
| `RecallBuilder::application` | `MemoryHandle.recall(application=)` | `RecallBuilder.application` | keywords |
| `RecallBuilder::block` | `MemoryHandle.recall(block=)` | `RecallBuilder.block` | overload |
| `RecallBuilder::fetch` | `MemoryHandle.recall` | `RecallBuilder.fetch` | one-call |
| `RecallBuilder::folded` | `MemoryHandle.recall(folded=)` | `RecallBuilder.folded` | keywords |
| `RecallBuilder::hybrid` | `MemoryHandle.recall(strategy=)` | `RecallBuilder.hybrid` | overload |
| `RecallBuilder::keyword` | `MemoryHandle.recall(strategy=)` | `RecallBuilder.keyword` | overload |
| `RecallBuilder::limit` | `MemoryHandle.recall(limit=)` | `RecallBuilder.limit` | keywords |
| `RecallBuilder::recent` | `MemoryHandle.recall(strategy=)` | `RecallBuilder.recent` | overload |
| `RecallBuilder::semantic` | `MemoryHandle.recall(semantic=)` | `RecallBuilder.semantic` | keywords |
| `RecallBuilder::strategy` | `MemoryHandle.recall(strategy=)` | `RecallBuilder.strategy` | keywords |
| `RecallBuilder::stream` | `MemoryHandle.recall(stream=)` | `RecallBuilder.stream` | keywords |
| `RecallBuilder::user` | `MemoryHandle.recall(user=)` | `RecallBuilder.user` | keywords |
| `RecallSignal` | `RecallSignal` | `RecallSignal` |  |
| `RecallSignal.strategy` | `RecallSignal.strategy` | `RecallSignal.strategy` | keywords |
| `RecallSignal.rank` | `RecallSignal.rank` | `RecallSignal.rank` | keywords |
| `RecallSignal.score` | `RecallSignal.score` | `RecallSignal.score` | keywords |
| `RecallStrategy` | omitted | `RecallStrategy` | plain-value |
| `RecallStrategy::Auto` | omitted | `RecallStrategy.Auto` | plain-value |
| `RecallStrategy::Recent` | omitted | `RecallStrategy.Recent` | plain-value |
| `RecallStrategy::Semantic` | omitted | `RecallStrategy.Semantic` | plain-value |
| `RecallStrategy::Keyword` | omitted | `RecallStrategy.Keyword` | plain-value |
| `RecallStrategy::Graph` | omitted | `RecallStrategy.Graph` | plain-value |
| `RecallStrategy::Temporal` | omitted | `RecallStrategy.Temporal` | plain-value |
| `RecallStrategy::Hybrid` | omitted | `RecallStrategy.Hybrid` | plain-value |
| `RememberBuilder` | `MemoryHandle.remember` | `RememberBuilder` | keywords |
| `RememberBuilder::agent` | `MemoryHandle.remember(agent=)` | `RememberBuilder.agent` | keywords |
| `RememberBuilder::application` | `MemoryHandle.remember(application=)` | `RememberBuilder.application` | keywords |
| `RememberBuilder::dedup` | `MemoryHandle.remember(dedup=)` | `RememberBuilder.dedup` | keywords |
| `RememberBuilder::durable` | `MemoryHandle.remember(durable=)` | `RememberBuilder.durable` | keywords |
| `RememberBuilder::kind` | `MemoryHandle.remember(kind=)` | `RememberBuilder.kind` | keywords |
| `RememberBuilder::scope` | `MemoryHandle.remember(conversation=)` | `RememberBuilder.scope` | keywords |
| `RememberBuilder::send` | `MemoryHandle.remember` | `RememberBuilder.send` | one-call |
| `RememberBuilder::stream` | `MemoryHandle.remember(stream=)` | `RememberBuilder.stream` | keywords |
| `RememberBuilder::user` | `MemoryHandle.remember(user=)` | `RememberBuilder.user` | keywords |
| `RerankedMemory` | `RerankedMemory` | `RerankedMemory` |  |
| `RerankedMemory::new` | `new RerankedMemory()` | `new RerankedMemory()` | constructor |
| `Reranker` | `MemoryHandle.reranker(reranker=)` | `Reranker` | callback |
| `Reranker::rerank` | `MemoryHandle.reranker(reranker=)` | `Reranker.rerank` | callback, keywords |
| `SharedConsolidator` | omitted | omitted | shared-ownership |
| `SharedConsolidator::new` | omitted | omitted | shared-ownership |
| `SharedEmbedder` | omitted | omitted | shared-ownership |
| `SharedReranker` | omitted | omitted | shared-ownership |
| `Summarizer` | `MemoryHandle.consolidate(summarizer=)` | `Summarizer` | callback |
| `Summarizer::summarize` | `MemoryHandle.consolidate(summarizer=)` | `Summarizer.summarize` | callback, keywords |
| `VectorMemory` | `VectorMemory` | `VectorMemory` |  |
| `VectorMemory::governed` | `VectorMemory.governed` | `VectorMemory.governed` |  |
| `VectorMemory::new` | `new VectorMemory()` | `new VectorMemory()` | constructor |
| `memory::fuse_reciprocal_rank` | `fn:fuse_reciprocal_rank` | `fn:fuseReciprocalRank` |  |
| `memory::to_context_block` | `fn:to_context_block` | `fn:toContextBlock` |  |

## message

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Message` | `Message` | `Message` |  |
| `Message.payload` | `Message.payload` | `Message.payload` | keywords |
| `Message.id` | `Message.id` | `Message.id` | keywords |
| `Message.headers` | `Message.headers` | `Message.headers` | keywords |
| `Message::json` | `Message.json` | `Message.json` | keywords |

## prelude

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentRunState` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::Submitted` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::Running` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::Completed` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::Cancelled` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::Failed` | omitted | `AgentRunState` | plain-value |
| `AgentRunState::as_str` | omitted | omitted | plain-value |
| `AgentRunState::is_terminal` | `fn:agent_run_state_is_terminal` | `fn:agentRunStateIsTerminal` | free-function |
| `EdgeDir` | omitted | `EdgeDir` | plain-value |
| `EdgeDir::Out` | omitted | `EdgeDir` | plain-value |
| `EdgeDir::In` | omitted | `EdgeDir` | plain-value |
| `EdgeDir::Both` | omitted | `EdgeDir` | plain-value |
| `EdgeDir::is_out` | `fn:edge_dir_is_out` | `fn:edgeDirIsOut` | free-function |

## prelude::full

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentRunInfo` | `AgentRunInfo` | `AgentRunInfo` |  |
| `AgentRunInfo.run_id` | `AgentRunInfo.run_id` | `AgentRunInfo.runId` | keywords |
| `AgentRunInfo.agent_id` | `AgentRunInfo.agent_id` | `AgentRunInfo.agentId` | keywords |
| `AgentRunInfo.user_id` | `AgentRunInfo.user_id` | `AgentRunInfo.userId` | keywords |
| `AgentRunInfo.state` | `AgentRunInfo.state` | `AgentRunInfo.state` | keywords |
| `AgentRunInfo.created_at_micros` | `AgentRunInfo.created_at_micros` | `AgentRunInfo.createdAtMicros` | keywords |
| `AgentRunInfo.updated_at_micros` | `AgentRunInfo.updated_at_micros` | `AgentRunInfo.updatedAtMicros` | keywords |
| `AgentRunInfo.detail` | `AgentRunInfo.detail` | `AgentRunInfo.detail` | keywords |
| `AgentRunInfo.cancel_requested` | `AgentRunInfo.cancel_requested` | `AgentRunInfo.cancelRequested` | keywords |
| `AttemptColumnMetrics` | dict via `Destinations.prepare` | `wire.AttemptColumnMetrics` | serde-dict |
| `AttemptColumnMetrics.field_id` | dict key | `wire.AttemptColumnMetrics.fieldId` | serde-dict, keywords |
| `AttemptColumnMetrics.value_count` | dict key | `wire.AttemptColumnMetrics.valueCount` | serde-dict, keywords |
| `AttemptColumnMetrics.null_count` | dict key | `wire.AttemptColumnMetrics.nullCount` | serde-dict, keywords |
| `AttemptColumnMetrics.nan_count` | dict key | `wire.AttemptColumnMetrics.nanCount` | serde-dict, keywords |
| `AttemptColumnMetrics.lower_bound` | dict key | `wire.AttemptColumnMetrics.lowerBound` | serde-dict, keywords |
| `AttemptColumnMetrics.upper_bound` | dict key | `wire.AttemptColumnMetrics.upperBound` | serde-dict, keywords |
| `AttemptObject` | dict via `Destinations.prepare` | `wire.AttemptObject` | serde-dict |
| `AttemptObject.identity` | dict key | `wire.AttemptObject.identity` | serde-dict, keywords |
| `AttemptObject.size_bytes` | dict key | `wire.AttemptObject.sizeBytes` | serde-dict, keywords |
| `AttemptObject.row_count` | dict key | `wire.AttemptObject.rowCount` | serde-dict, keywords |
| `AttemptObject.sha256` | dict key | `wire.AttemptObject.sha256` | serde-dict, keywords |
| `AttemptObject.columns` | dict key | `wire.AttemptObject.columns` | serde-dict, keywords |
| `BackendBinding` | dict via `Destinations.register` | `wire.BackendBinding` | serde-dict |
| `BackendBinding.resource_id` | dict key | `wire.BackendBinding.resourceId` | serde-dict, keywords |
| `BackendBinding.generation` | dict key | `wire.BackendBinding.generation` | serde-dict, keywords |
| `BinaryValue` | omitted | `BinaryValue` | plain-value |
| `CheckpointError` | dict via `CheckpointError.detail` | `CheckpointError` | serde-dict |
| `CheckpointError::Invalid` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::NotFound` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::Conflict` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::LeaseLost` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::Unauthorized` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::Unavailable` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointError::Version` | dict key | `CheckpointError` | serde-dict, plain-value |
| `CheckpointMutationResult` | dict via `Destinations.mutate` | `CheckpointMutationResult` | serde-dict |
| `CheckpointMutationResult::Destination` | dict key | `CheckpointMutationResult` | serde-dict, plain-value |
| `CheckpointMutationResult::QueryRoute` | dict key | `CheckpointMutationResult` | serde-dict, plain-value |
| `CheckpointOwnerId` | omitted | `wire.CheckpointOwnerId` | plain-value |
| `CheckpointOwnerId::as_u128` | omitted | `wire.CheckpointOwnerId.asU128` | plain-value |
| `CheckpointOwnerId::from_bytes` | omitted | `wire.CheckpointOwnerId.fromBytes` | plain-value |
| `CheckpointOwnerId::from_u128` | omitted | `wire.CheckpointOwnerId.fromU128` | plain-value |
| `CheckpointOwnerId::to_bytes` | omitted | `wire.CheckpointOwnerId.toBytes` | plain-value |
| `CheckpointOwnerLease` | dict via `Destinations.get` | `wire.CheckpointOwnerLease` | serde-dict |
| `CheckpointOwnerLease.owner` | dict key | `wire.CheckpointOwnerLease.owner` | serde-dict, keywords |
| `CheckpointOwnerLease.epoch` | dict key | `wire.CheckpointOwnerLease.epoch` | serde-dict, keywords |
| `CheckpointOwnerLease.sequence` | dict key | `wire.CheckpointOwnerLease.sequence` | serde-dict, keywords |
| `CheckpointOwnerLease.deadline_micros` | dict key | `wire.CheckpointOwnerLease.deadlineMicros` | serde-dict, keywords |
| `CheckpointReadConsistency` | omitted | `CheckpointReadConsistency` | plain-value |
| `CheckpointReadConsistency::Linearizable` | omitted | `CheckpointReadConsistency` | plain-value |
| `CheckpointReadConsistency::PotentiallyStale` | omitted | `CheckpointReadConsistency` | plain-value |
| `CheckpointRequestEnvelope` | dict via `Laser.execute_checkpoint` | `CheckpointRequestEnvelope` | serde-dict |
| `CheckpointRequestEnvelope.v` | dict key | `CheckpointRequestEnvelope.v` | serde-dict, keywords |
| `CheckpointRequestEnvelope.request_id` | dict key | `CheckpointRequestEnvelope.requestId` | serde-dict, keywords |
| `CheckpointRequestEnvelope.expected_global_state_revision` | dict key | `CheckpointRequestEnvelope.expectedGlobalStateRevision` | serde-dict, keywords |
| `CheckpointRequestEnvelope.mutation` | dict key | `CheckpointRequestEnvelope.mutation` | serde-dict, keywords |
| `CheckpointRequestEnvelope.supervisor_assertion` | dict key | `CheckpointRequestEnvelope.supervisorAssertion` | serde-dict, keywords |
| `CheckpointRequestEnvelope::new` | `fn:new_checkpoint_request_envelope` | `CheckpointRequestEnvelope` | free-function, keywords |
| `CheckpointRequestEnvelope::with_supervisor_assertion` | `fn:new_checkpoint_request_envelope(supervisor_assertion=)` | `CheckpointRequestEnvelope.supervisorAssertion` | keywords |
| `CheckpointRequestId` | omitted | `wire.CheckpointRequestId` | plain-value |
| `CheckpointRequestId::as_u128` | omitted | `wire.CheckpointRequestId.asU128` | plain-value |
| `CheckpointRequestId::from_bytes` | omitted | `wire.CheckpointRequestId.fromBytes` | plain-value |
| `CheckpointRequestId::from_u128` | omitted | `wire.CheckpointRequestId.fromU128` | plain-value |
| `CheckpointRequestId::to_bytes` | omitted | `wire.CheckpointRequestId.toBytes` | plain-value |
| `CompletedAttempt` | dict via `Destinations.complete` | `wire.CompletedAttempt` | serde-dict |
| `CompletedAttempt.id` | dict key | `wire.CompletedAttempt.id` | serde-dict, keywords |
| `CompletedAttempt.table_uuid` | dict key | `wire.CompletedAttempt.tableUuid` | serde-dict, keywords |
| `CompletedAttempt.snapshot_id` | dict key | `wire.CompletedAttempt.snapshotId` | serde-dict, keywords |
| `CompletedAttempt.manifest_digest` | dict key | `wire.CompletedAttempt.manifestDigest` | serde-dict, keywords |
| `CompletedAttempt.resulting_boundary_digest` | dict key | `wire.CompletedAttempt.resultingBoundaryDigest` | serde-dict, keywords |
| `CompletedAttempt.ranges` | dict key | `wire.CompletedAttempt.ranges` | serde-dict, keywords |
| `CompletedAttempt.completion_revision` | dict key | `wire.CompletedAttempt.completionRevision` | serde-dict, keywords |
| `CredentialGeneration` | dict via `Destinations.prepare` | `wire.CredentialGeneration` | serde-dict |
| `CredentialGeneration.role` | dict key | `wire.CredentialGeneration.role` | serde-dict, keywords |
| `CredentialGeneration.generation` | dict key | `wire.CredentialGeneration.generation` | serde-dict, keywords |
| `DecimalValue` | dict via `Row.values` | `DecimalValue` | serde-dict |
| `DecimalValue.unscaled` | dict key | `DecimalValue.unscaled` | serde-dict, keywords |
| `DecimalValue.precision` | dict key | `DecimalValue.precision` | serde-dict, keywords |
| `DecimalValue.scale` | dict key | `DecimalValue.scale` | serde-dict, keywords |
| `DecimalValue::validate_canonical` | `fn:decimal_value_validate_canonical` | `fn:decimalValueValidateCanonical` | free-function |
| `DestinationBlock` | dict via `Destinations.record_block` | `wire.DestinationBlock` | serde-dict |
| `DestinationBlock.code` | dict key | `wire.DestinationBlock.code` | serde-dict, keywords |
| `DestinationBlock.message` | dict key | `wire.DestinationBlock.message` | serde-dict, keywords |
| `DestinationBlock.incarnation` | dict key | `wire.DestinationBlock.incarnation` | serde-dict, keywords |
| `DestinationBlock.offset` | dict key | `wire.DestinationBlock.offset` | serde-dict, keywords |
| `DestinationBlock.row_ordinal` | dict key | `wire.DestinationBlock.rowOrdinal` | serde-dict, keywords |
| `DestinationBlockCode` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Decode` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Schema` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Projection` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Value` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Size` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::RetentionGap` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::PreparedAttempt` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::BackendGeneration` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::BackendUnavailable` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::TableIdentity` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::CatalogOutcomeUnknown` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::SourceIncarnation` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationBlockCode::Authorization` | omitted | `wire.DestinationBlockCode` | plain-value |
| `DestinationCheckpointPage` | dict via `Destinations.list` | `DestinationCheckpointPage` | serde-dict |
| `DestinationCheckpointPage.destinations` | dict key | `DestinationCheckpointPage.destinations` | serde-dict, keywords |
| `DestinationCheckpointPage.next_after` | dict key | `DestinationCheckpointPage.nextAfter` | serde-dict, keywords |
| `DestinationCheckpointPage.global_state_revision` | dict key | `DestinationCheckpointPage.globalStateRevision` | serde-dict, keywords |
| `DestinationCheckpointPage.consistency` | dict key | `DestinationCheckpointPage.consistency` | serde-dict, keywords |
| `DestinationCheckpointStatus` | dict via `Destinations.get` | `DestinationCheckpointStatus` | serde-dict |
| `DestinationCheckpointStatus.destination_id` | dict key | `DestinationCheckpointStatus.destinationId` | serde-dict, keywords |
| `DestinationCheckpointStatus.destination_generation` | dict key | `DestinationCheckpointStatus.destinationGeneration` | serde-dict, keywords |
| `DestinationCheckpointStatus.backend` | dict key | `DestinationCheckpointStatus.backend` | serde-dict, keywords |
| `DestinationCheckpointStatus.schema` | dict key | `DestinationCheckpointStatus.schema` | serde-dict, keywords |
| `DestinationCheckpointStatus.projection` | dict key | `DestinationCheckpointStatus.projection` | serde-dict, keywords |
| `DestinationCheckpointStatus.global_state_revision` | dict key | `DestinationCheckpointStatus.globalStateRevision` | serde-dict, keywords |
| `DestinationCheckpointStatus.definition_revision` | dict key | `DestinationCheckpointStatus.definitionRevision` | serde-dict, keywords |
| `DestinationCheckpointStatus.checkpoint_revision` | dict key | `DestinationCheckpointStatus.checkpointRevision` | serde-dict, keywords |
| `DestinationCheckpointStatus.desired_state` | dict key | `DestinationCheckpointStatus.desiredState` | serde-dict, keywords |
| `DestinationCheckpointStatus.effective_state` | dict key | `DestinationCheckpointStatus.effectiveState` | serde-dict, keywords |
| `DestinationCheckpointStatus.table_uuid` | dict key | `DestinationCheckpointStatus.tableUuid` | serde-dict, keywords |
| `DestinationCheckpointStatus.owner` | dict key | `DestinationCheckpointStatus.owner` | serde-dict, keywords |
| `DestinationCheckpointStatus.partitions` | dict key | `DestinationCheckpointStatus.partitions` | serde-dict, keywords |
| `DestinationCheckpointStatus.prepared_attempt` | dict key | `DestinationCheckpointStatus.preparedAttempt` | serde-dict, keywords |
| `DestinationCheckpointStatus.last_completion` | dict key | `DestinationCheckpointStatus.lastCompletion` | serde-dict, keywords |
| `DestinationCheckpointStatus.retention_gap` | dict key | `DestinationCheckpointStatus.retentionGap` | serde-dict, keywords |
| `DestinationCheckpointStatus.block` | dict key | `DestinationCheckpointStatus.block` | serde-dict, keywords |
| `DestinationCheckpointStatus.last_repair` | dict key | `DestinationCheckpointStatus.lastRepair` | serde-dict, keywords |
| `DestinationCheckpointStatus.consistency` | dict key | `DestinationCheckpointStatus.consistency` | serde-dict, keywords |
| `DestinationCheckpointView` | dict via `Destinations.get` | `DestinationCheckpointView` | serde-dict |
| `DestinationCheckpointView.destination` | dict key | `DestinationCheckpointView.destination` | serde-dict, keywords |
| `DestinationCheckpointView.status` | dict key | `DestinationCheckpointView.status` | serde-dict, keywords |
| `DestinationDesiredState` | omitted | `wire.DestinationDesiredState` | plain-value |
| `DestinationDesiredState::Disabled` | omitted | `wire.DestinationDesiredState` | plain-value |
| `DestinationDesiredState::Enabled` | omitted | `wire.DestinationDesiredState` | plain-value |
| `DestinationEffectiveState` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationEffectiveState::Disabled` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationEffectiveState::WaitingForBackend` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationEffectiveState::Ready` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationEffectiveState::Running` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationEffectiveState::Blocked` | omitted | `wire.DestinationEffectiveState` | plain-value |
| `DestinationErrorPolicy` | omitted | `wire.DestinationErrorPolicy` | plain-value |
| `DestinationErrorPolicy::Block` | omitted | `wire.DestinationErrorPolicy` | plain-value |
| `DestinationId` | omitted | `wire.DestinationId` | plain-value |
| `DestinationId::as_u128` | omitted | `wire.DestinationId.asU128` | plain-value |
| `DestinationId::from_bytes` | omitted | `wire.DestinationId.fromBytes` | plain-value |
| `DestinationId::from_u128` | omitted | `wire.DestinationId.fromU128` | plain-value |
| `DestinationId::to_bytes` | omitted | `wire.DestinationId.toBytes` | plain-value |
| `DestinationListFilter` | dict via `Destinations.list` | `wire.DestinationListFilter` | serde-dict |
| `DestinationListFilter.source_stream` | dict key | `wire.DestinationListFilter.sourceStream` | serde-dict, keywords |
| `DestinationListFilter.source_topic` | dict key | `wire.DestinationListFilter.sourceTopic` | serde-dict, keywords |
| `DestinationListFilter.name_contains` | dict key | `wire.DestinationListFilter.nameContains` | serde-dict, keywords |
| `DestinationOperationId` | omitted | `wire.DestinationOperationId` | plain-value |
| `DestinationOperationId::as_u128` | omitted | `wire.DestinationOperationId.asU128` | plain-value |
| `DestinationOperationId::from_bytes` | omitted | `wire.DestinationOperationId.fromBytes` | plain-value |
| `DestinationOperationId::from_u128` | omitted | `wire.DestinationOperationId.fromU128` | plain-value |
| `DestinationOperationId::to_bytes` | omitted | `wire.DestinationOperationId.toBytes` | plain-value |
| `Digest32` | omitted | `Digest32` | plain-value |
| `Digest32::BYTES` | `const:DIGEST32_BYTES` | `Digest32.BYTES` | free-function |
| `Digest32::as_bytes` | omitted | omitted | plain-value |
| `Digest32::new` | omitted | omitted | plain-value |
| `EdgeId` | omitted | `EdgeId` | plain-value |
| `EdgeId::as_u128` | omitted | `EdgeId.asU128` | plain-value |
| `EdgeId::content` | `fn:edge_id_content` | `EdgeId.content` | free-function |
| `EdgeId::from_bytes` | omitted | `EdgeId.fromBytes` | plain-value |
| `EdgeId::from_u128` | omitted | `EdgeId.fromU128` | plain-value |
| `EdgeId::to_bytes` | omitted | `EdgeId.toBytes` | plain-value |
| `FieldValue` | dict via `Row.values` | `FieldValue` | serde-dict |
| `FieldValue.field_id` | dict key | `FieldValue.fieldId` | serde-dict, keywords |
| `FieldValue.value` | dict key | `FieldValue.value` | serde-dict, keywords |
| `FileFormat` | omitted | `wire.FileFormat` | plain-value |
| `FileFormat::Parquet` | omitted | `wire.FileFormat` | plain-value |
| `GraphEdge` | dict via `GraphHandle.upsert` | `GraphEdge` | serde-dict |
| `GraphEdge.id` | dict key | `GraphEdge.id` | serde-dict, keywords |
| `GraphEdge.from` | dict key | `GraphEdge.from` | serde-dict, keywords |
| `GraphEdge.to` | dict key | `GraphEdge.to` | serde-dict, keywords |
| `GraphEdge.edge_type` | dict key | `GraphEdge.edgeType` | serde-dict, keywords |
| `GraphEdge.weight` | dict key | `GraphEdge.weight` | serde-dict, keywords |
| `GraphEdge.attrs` | dict key | `GraphEdge.attrs` | serde-dict, keywords |
| `GraphEdge.valid_from` | dict key | `GraphEdge.validFrom` | serde-dict, keywords |
| `GraphEdge.valid_to` | dict key | `GraphEdge.validTo` | serde-dict, keywords |
| `GraphEdge.source` | dict key | `GraphEdge.source` | serde-dict, keywords |
| `GraphEdge::relate` | `fn:graph_edge_relate` | `fn:graphEdgeRelate` | free-function |
| `GraphEdge::valid` | `fn:graph_edge_valid` | `fn:graphEdgeValid` | free-function |
| `GraphEdge::valid_at` | `fn:graph_edge_valid_at` | `fn:graphEdgeValidAt` | free-function |
| `GraphEdge::with_source` | `fn:graph_edge_with_source` | `GraphEdge.source` | free-function, keywords |
| `GraphNode` | dict via `GraphHandle.upsert` | `GraphNode` | serde-dict |
| `GraphNode.id` | dict key | `GraphNode.id` | serde-dict, keywords |
| `GraphNode.labels` | dict key | `GraphNode.labels` | serde-dict, keywords |
| `GraphNode.attrs` | dict key | `GraphNode.attrs` | serde-dict, keywords |
| `GraphNode.embedding` | dict key | `GraphNode.embedding` | serde-dict, keywords |
| `GraphNode.source` | dict key | `GraphNode.source` | serde-dict, keywords |
| `GraphNode::entity` | `fn:graph_node_entity` | `fn:graphNodeEntity` | free-function |
| `GraphResult` | dict via `GraphHandle.fetch` | `GraphResult` | serde-dict |
| `GraphResult.nodes` | dict key | `GraphResult.nodes` | serde-dict, keywords |
| `GraphResult.edges` | dict key | `GraphResult.edges` | serde-dict, keywords |
| `GraphResult.paths` | dict key | `GraphResult.paths` | serde-dict, keywords |
| `GraphReturn` | omitted | `GraphReturn` | plain-value |
| `GraphReturn::Nodes` | omitted | `GraphReturn` | plain-value |
| `GraphReturn::Edges` | omitted | `GraphReturn` | plain-value |
| `GraphReturn::Paths` | omitted | `GraphReturn` | plain-value |
| `GraphReturn::Triplets` | omitted | `GraphReturn` | plain-value |
| `GraphReturn::is_nodes` | `fn:graph_return_is_nodes` | `fn:graphReturnIsNodes` | free-function |
| `IcebergCommitRequirement` | dict via `Destinations.prepare` | `wire.IcebergCommitRequirement` | serde-dict |
| `IcebergCommitRequirement::AssertTableUuid` | dict key | `wire.IcebergCommitRequirement` | serde-dict, plain-value |
| `IcebergCommitRequirement::AssertMetadataIdentity` | dict key | `wire.IcebergCommitRequirement` | serde-dict, plain-value |
| `IcebergCommitRequirement::AssertCurrentSnapshot` | dict key | `wire.IcebergCommitRequirement` | serde-dict, plain-value |
| `IcebergCommitRequirement::AssertCurrentSchema` | dict key | `wire.IcebergCommitRequirement` | serde-dict, plain-value |
| `IcebergCommitRequirement::AssertDefaultPartitionSpec` | dict key | `wire.IcebergCommitRequirement` | serde-dict, plain-value |
| `LogicalField` | dict via `QueryResult.fields` | `LogicalField` | serde-dict |
| `LogicalField.id` | dict key | `LogicalField.id` | serde-dict, keywords |
| `LogicalField.name` | dict key | `LogicalField.name` | serde-dict, keywords |
| `LogicalField.required` | dict key | `LogicalField.required` | serde-dict, keywords |
| `LogicalField.field_type` | dict key | `LogicalField.fieldType` | serde-dict, keywords |
| `LogicalField.doc` | dict key | `LogicalField.doc` | serde-dict, keywords |
| `LogicalSchema` | `LogicalSchema` | `LogicalSchema` |  |
| `LogicalSchema.schema` | `LogicalSchema.schema` | `LogicalSchema.schema` | keywords |
| `LogicalSchema.fields` | `LogicalSchema.fields` | `LogicalSchema.fields` | keywords |
| `LogicalSchema::canonical_fingerprint_bytes` | `LogicalSchema.canonical_fingerprint_bytes` | `fn:logicalSchemaCanonicalFingerprintBytes` | free-function |
| `LogicalSchema::compute_fingerprint` | `LogicalSchema.compute_fingerprint` | `fn:logicalSchemaComputeFingerprint` | free-function |
| `LogicalSchema::new` | `new LogicalSchema()` | `LogicalSchema` | constructor, keywords |
| `LogicalSchemaId` | omitted | `wire.LogicalSchemaId` | plain-value |
| `LogicalSchemaId::as_u128` | omitted | `wire.LogicalSchemaId.asU128` | plain-value |
| `LogicalSchemaId::from_bytes` | omitted | `wire.LogicalSchemaId.fromBytes` | plain-value |
| `LogicalSchemaId::from_u128` | omitted | `wire.LogicalSchemaId.fromU128` | plain-value |
| `LogicalSchemaId::to_bytes` | omitted | `wire.LogicalSchemaId.toBytes` | plain-value |
| `LogicalSchemaRef` | dict via `LogicalSchema.schema` | `LogicalSchemaRef` | serde-dict |
| `LogicalSchemaRef.id` | dict key | `LogicalSchemaRef.id` | serde-dict, keywords |
| `LogicalSchemaRef.version` | dict key | `LogicalSchemaRef.version` | serde-dict, keywords |
| `LogicalSchemaRef.fingerprint` | dict key | `LogicalSchemaRef.fingerprint` | serde-dict, keywords |
| `LogicalType` | dict via `QueryResult.fields` | `LogicalType` | serde-dict |
| `LogicalType::Boolean` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Int` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Long` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Float` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Double` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Decimal` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Date` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::TimeMicros` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::TimestampMicros` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::TimestampTzMicros` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::String` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Uuid` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Fixed` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Binary` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Struct` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::List` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::Map` | dict key | `LogicalType` | serde-dict, plain-value |
| `LogicalType::accepts_map_key` | `fn:logical_type_accepts_map_key` | `fn:logicalTypeAcceptsMapKey` | free-function |
| `LogicalType::kind` | `fn:logical_type_kind` | `fn:logicalTypeKind` | free-function |
| `LogicalTypeKind` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Boolean` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Int` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Long` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Float` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Double` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Decimal` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Date` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::TimeMicros` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::TimestampMicros` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::TimestampTzMicros` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::String` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Uuid` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Fixed` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Binary` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Struct` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::List` | omitted | `wire.LogicalTypeKind` | plain-value |
| `LogicalTypeKind::Map` | omitted | `wire.LogicalTypeKind` | plain-value |
| `MapEntry` | dict via `Row.values` | `MapEntry` | serde-dict |
| `MapEntry.key` | dict key | `MapEntry.key` | serde-dict, keywords |
| `MapEntry.value` | dict key | `MapEntry.value` | serde-dict, keywords |
| `MaterializationDestination` | dict via `Destinations.register` | `MaterializationDestination` | serde-dict |
| `MaterializationDestination.id` | dict key | `MaterializationDestination.id` | serde-dict, keywords |
| `MaterializationDestination.generation` | dict key | `MaterializationDestination.generation` | serde-dict, keywords |
| `MaterializationDestination.definition_revision` | dict key | `MaterializationDestination.definitionRevision` | serde-dict, keywords |
| `MaterializationDestination.name` | dict key | `MaterializationDestination.name` | serde-dict, keywords |
| `MaterializationDestination.source` | dict key | `MaterializationDestination.source` | serde-dict, keywords |
| `MaterializationDestination.recreated_partition_policy` | dict key | `MaterializationDestination.recreatedPartitionPolicy` | serde-dict, keywords |
| `MaterializationDestination.projection` | dict key | `MaterializationDestination.projection` | serde-dict, keywords |
| `MaterializationDestination.schema` | dict key | `MaterializationDestination.schema` | serde-dict, keywords |
| `MaterializationDestination.backend` | dict key | `MaterializationDestination.backend` | serde-dict, keywords |
| `MaterializationDestination.table` | dict key | `MaterializationDestination.table` | serde-dict, keywords |
| `MaterializationDestination.file_format` | dict key | `MaterializationDestination.fileFormat` | serde-dict, keywords |
| `MaterializationDestination.table_format` | dict key | `MaterializationDestination.tableFormat` | serde-dict, keywords |
| `MaterializationDestination.start_policy` | dict key | `MaterializationDestination.startPolicy` | serde-dict, keywords |
| `MaterializationDestination.new_partition_policy` | dict key | `MaterializationDestination.newPartitionPolicy` | serde-dict, keywords |
| `MaterializationDestination.error_policy` | dict key | `MaterializationDestination.errorPolicy` | serde-dict, keywords |
| `MaterializationDestination.desired_state` | dict key | `MaterializationDestination.desiredState` | serde-dict, keywords |
| `NewPartitionPolicy` | omitted | `wire.NewPartitionPolicy` | plain-value |
| `NewPartitionPolicy::Beginning` | omitted | `wire.NewPartitionPolicy` | plain-value |
| `NewPartitionPolicy::CapturedLatest` | omitted | `wire.NewPartitionPolicy` | plain-value |
| `NewPartitionPolicy::Reject` | omitted | `wire.NewPartitionPolicy` | plain-value |
| `NodeId` | omitted | `wire.NodeId` | plain-value |
| `NodeId::as_u128` | omitted | `wire.NodeId.asU128` | plain-value |
| `NodeId::content` | `fn:node_id_content` | `wire.NodeId.content` | free-function |
| `NodeId::from_bytes` | omitted | `wire.NodeId.fromBytes` | plain-value |
| `NodeId::from_u128` | omitted | `wire.NodeId.fromU128` | plain-value |
| `NodeId::to_bytes` | omitted | `wire.NodeId.toBytes` | plain-value |
| `PartitionCheckpoint` | dict via `Destinations.get` | `wire.PartitionCheckpoint` | serde-dict |
| `PartitionCheckpoint.incarnation` | dict key | `wire.PartitionCheckpoint.incarnation` | serde-dict, keywords |
| `PartitionCheckpoint.started_at_offset` | dict key | `wire.PartitionCheckpoint.startedAtOffset` | serde-dict, keywords |
| `PartitionCheckpoint.next_offset` | dict key | `wire.PartitionCheckpoint.nextOffset` | serde-dict, keywords |
| `PartitionCheckpoint.lifecycle` | dict key | `wire.PartitionCheckpoint.lifecycle` | serde-dict, keywords |
| `PartitionLifecycleChange` | omitted | `PartitionLifecycleChange` | plain-value |
| `PartitionLifecycleChange::Removed` | omitted | `PartitionLifecycleChange` | plain-value |
| `PartitionLifecycleChange::Recreated` | omitted | `PartitionLifecycleChange` | plain-value |
| `PartitionLifecycleState` | omitted | `wire.PartitionLifecycleState` | plain-value |
| `PartitionLifecycleState::Active` | omitted | `wire.PartitionLifecycleState` | plain-value |
| `PartitionLifecycleState::Removed` | omitted | `wire.PartitionLifecycleState` | plain-value |
| `PartitionLifecycleState::Recreated` | omitted | `wire.PartitionLifecycleState` | plain-value |
| `PartitionStart` | dict via `Destinations.register` | `wire.PartitionStart` | serde-dict |
| `PartitionStart.incarnation` | dict key | `wire.PartitionStart.incarnation` | serde-dict, keywords |
| `PartitionStart.next_offset` | dict key | `wire.PartitionStart.nextOffset` | serde-dict, keywords |
| `Path` | dict via `GraphHandle.fetch` | `wire.Path` | serde-dict |
| `Path.nodes` | dict key | `wire.Path.nodes` | serde-dict, keywords |
| `Path.edges` | dict key | `wire.Path.edges` | serde-dict, keywords |
| `PhysicalClusterIncarnation` | omitted | `wire.PhysicalClusterIncarnation` | plain-value |
| `PhysicalClusterIncarnation::as_u128` | omitted | `wire.PhysicalClusterIncarnation.asU128` | plain-value |
| `PhysicalClusterIncarnation::from_bytes` | omitted | `wire.PhysicalClusterIncarnation.fromBytes` | plain-value |
| `PhysicalClusterIncarnation::from_u128` | omitted | `wire.PhysicalClusterIncarnation.fromU128` | plain-value |
| `PhysicalClusterIncarnation::to_bytes` | omitted | `wire.PhysicalClusterIncarnation.toBytes` | plain-value |
| `PhysicalTable` | dict via `Destinations.register` | `wire.PhysicalTable` | serde-dict |
| `PhysicalTable.namespace` | dict key | `wire.PhysicalTable.namespace` | serde-dict, keywords |
| `PhysicalTable.table` | dict key | `wire.PhysicalTable.table` | serde-dict, keywords |
| `PhysicalTable.expected_table_uuid` | dict key | `wire.PhysicalTable.expectedTableUuid` | serde-dict, keywords |
| `PreparedAttempt` | dict via `Destinations.prepare` | `wire.PreparedAttempt` | serde-dict |
| `PreparedAttempt.id` | dict key | `wire.PreparedAttempt.id` | serde-dict, keywords |
| `PreparedAttempt.destination_id` | dict key | `wire.PreparedAttempt.destinationId` | serde-dict, keywords |
| `PreparedAttempt.destination_generation` | dict key | `wire.PreparedAttempt.destinationGeneration` | serde-dict, keywords |
| `PreparedAttempt.backend` | dict key | `wire.PreparedAttempt.backend` | serde-dict, keywords |
| `PreparedAttempt.owner` | dict key | `wire.PreparedAttempt.owner` | serde-dict, keywords |
| `PreparedAttempt.epoch` | dict key | `wire.PreparedAttempt.epoch` | serde-dict, keywords |
| `PreparedAttempt.created_at_checkpoint_revision` | dict key | `wire.PreparedAttempt.createdAtCheckpointRevision` | serde-dict, keywords |
| `PreparedAttempt.table` | dict key | `wire.PreparedAttempt.table` | serde-dict, keywords |
| `PreparedAttempt.schema_fingerprint` | dict key | `wire.PreparedAttempt.schemaFingerprint` | serde-dict, keywords |
| `PreparedAttempt.projection` | dict key | `wire.PreparedAttempt.projection` | serde-dict, keywords |
| `PreparedAttempt.ranges` | dict key | `wire.PreparedAttempt.ranges` | serde-dict, keywords |
| `PreparedAttempt.resulting_boundary` | dict key | `wire.PreparedAttempt.resultingBoundary` | serde-dict, keywords |
| `PreparedAttempt.resulting_boundary_digest` | dict key | `wire.PreparedAttempt.resultingBoundaryDigest` | serde-dict, keywords |
| `PreparedAttempt.manifest_identity` | dict key | `wire.PreparedAttempt.manifestIdentity` | serde-dict, keywords |
| `PreparedAttempt.manifest_digest` | dict key | `wire.PreparedAttempt.manifestDigest` | serde-dict, keywords |
| `PreparedAttempt.objects` | dict key | `wire.PreparedAttempt.objects` | serde-dict, keywords |
| `PreparedAttempt.credential_generations` | dict key | `wire.PreparedAttempt.credentialGenerations` | serde-dict, keywords |
| `PreparedAttemptId` | omitted | `wire.PreparedAttemptId` | plain-value |
| `PreparedAttemptId::as_u128` | omitted | `wire.PreparedAttemptId.asU128` | plain-value |
| `PreparedAttemptId::from_bytes` | omitted | `wire.PreparedAttemptId.fromBytes` | plain-value |
| `PreparedAttemptId::from_u128` | omitted | `wire.PreparedAttemptId.fromU128` | plain-value |
| `PreparedAttemptId::to_bytes` | omitted | `wire.PreparedAttemptId.toBytes` | plain-value |
| `PreparedAttemptSummary` | dict via `Destinations.get` | `wire.PreparedAttemptSummary` | serde-dict |
| `PreparedAttemptSummary.id` | dict key | `wire.PreparedAttemptSummary.id` | serde-dict, keywords |
| `PreparedAttemptSummary.owner` | dict key | `wire.PreparedAttemptSummary.owner` | serde-dict, keywords |
| `PreparedAttemptSummary.epoch` | dict key | `wire.PreparedAttemptSummary.epoch` | serde-dict, keywords |
| `PreparedAttemptSummary.table` | dict key | `wire.PreparedAttemptSummary.table` | serde-dict, keywords |
| `PreparedAttemptSummary.schema_fingerprint` | dict key | `wire.PreparedAttemptSummary.schemaFingerprint` | serde-dict, keywords |
| `PreparedAttemptSummary.projection` | dict key | `wire.PreparedAttemptSummary.projection` | serde-dict, keywords |
| `PreparedAttemptSummary.manifest_identity` | dict key | `wire.PreparedAttemptSummary.manifestIdentity` | serde-dict, keywords |
| `PreparedAttemptSummary.manifest_digest` | dict key | `wire.PreparedAttemptSummary.manifestDigest` | serde-dict, keywords |
| `PreparedAttemptSummary.resulting_boundary_digest` | dict key | `wire.PreparedAttemptSummary.resultingBoundaryDigest` | serde-dict, keywords |
| `PreparedAttemptSummary.ranges` | dict key | `wire.PreparedAttemptSummary.ranges` | serde-dict, keywords |
| `PreparedAttemptSummary.object_count` | dict key | `wire.PreparedAttemptSummary.objectCount` | serde-dict, keywords |
| `PreparedAttemptSummary.credential_generations` | dict key | `wire.PreparedAttemptSummary.credentialGenerations` | serde-dict, keywords |
| `PreparedTableRequirements` | dict via `Destinations.prepare` | `wire.PreparedTableRequirements` | serde-dict |
| `PreparedTableRequirements.table_uuid` | dict key | `wire.PreparedTableRequirements.tableUuid` | serde-dict, keywords |
| `PreparedTableRequirements.base_metadata_identity` | dict key | `wire.PreparedTableRequirements.baseMetadataIdentity` | serde-dict, keywords |
| `PreparedTableRequirements.base_snapshot_id` | dict key | `wire.PreparedTableRequirements.baseSnapshotId` | serde-dict, keywords |
| `PreparedTableRequirements.schema_id` | dict key | `wire.PreparedTableRequirements.schemaId` | serde-dict, keywords |
| `PreparedTableRequirements.partition_spec_id` | dict key | `wire.PreparedTableRequirements.partitionSpecId` | serde-dict, keywords |
| `PreparedTableRequirements.commit_requirements` | dict key | `wire.PreparedTableRequirements.commitRequirements` | serde-dict, keywords |
| `ProjectionRef` | dict via `Destinations.register` | `wire.ProjectionRef` | serde-dict |
| `ProjectionRef.id` | dict key | `wire.ProjectionRef.id` | serde-dict, keywords |
| `ProjectionRef.version` | dict key | `wire.ProjectionRef.version` | serde-dict, keywords |
| `PublicCheckpointMutation` | dict via `Destinations.mutate` | `PublicCheckpointMutation` | serde-dict |
| `PublicCheckpointMutation::RegisterDestination` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RegisterQueryRoute` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RemoveQueryRoute` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::BindTable` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::SetDesiredState` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::AddPartition` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::ObservePartitionLifecycle` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::AcquireLease` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RenewLease` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::TakeoverLease` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::Prepare` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::Complete` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RecordBlock` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::ClearBlock` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RecordRetentionGap` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::AcceptRetentionGap` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::SupersedeGeneration` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::RecordRepair` | dict key | `PublicCheckpointMutation` | serde-dict, plain-value |
| `PublicCheckpointMutation::required_capability` | `fn:public_checkpoint_mutation_required_capability` | `fn:publicCheckpointMutationRequiredCapability` | free-function |
| `QueryRoute` | dict via `Destinations.register_query_route` | `QueryRoute` | serde-dict |
| `QueryRoute.id` | dict key | `QueryRoute.id` | serde-dict, keywords |
| `QueryRoute.generation` | dict key | `QueryRoute.generation` | serde-dict, keywords |
| `QueryRoute.definition_revision` | dict key | `QueryRoute.definitionRevision` | serde-dict, keywords |
| `QueryRoute.name` | dict key | `QueryRoute.name` | serde-dict, keywords |
| `QueryRoute.target` | dict key | `QueryRoute.target` | serde-dict, keywords |
| `QueryRoute.desired_state` | dict key | `QueryRoute.desiredState` | serde-dict, keywords |
| `QueryRouteId` | omitted | `wire.QueryRouteId` | plain-value |
| `QueryRouteId::as_u128` | omitted | `wire.QueryRouteId.asU128` | plain-value |
| `QueryRouteId::from_bytes` | omitted | `wire.QueryRouteId.fromBytes` | plain-value |
| `QueryRouteId::from_u128` | omitted | `wire.QueryRouteId.fromU128` | plain-value |
| `QueryRouteId::to_bytes` | omitted | `wire.QueryRouteId.toBytes` | plain-value |
| `QueryRoutePage` | dict via `Destinations.query_routes` | `wire.QueryRoutePage` | serde-dict |
| `QueryRoutePage.routes` | dict key | `wire.QueryRoutePage.routes` | serde-dict, keywords |
| `QueryRoutePage.next_after` | dict key | `wire.QueryRoutePage.nextAfter` | serde-dict, keywords |
| `QueryRoutePage.global_state_revision` | dict key | `wire.QueryRoutePage.globalStateRevision` | serde-dict, keywords |
| `QueryRoutePage.consistency` | dict key | `wire.QueryRoutePage.consistency` | serde-dict, keywords |
| `QueryRouteTarget` | dict via `Destinations.register_query_route` | `QueryRouteTarget` | serde-dict |
| `QueryRouteTarget::Operational` | dict key | `QueryRouteTarget` | serde-dict, plain-value |
| `QueryRouteTarget::Lakehouse` | dict key | `QueryRouteTarget` | serde-dict, plain-value |
| `RecreatedPartitionPolicy` | omitted | `wire.RecreatedPartitionPolicy` | plain-value |
| `RecreatedPartitionPolicy::Reject` | omitted | `wire.RecreatedPartitionPolicy` | plain-value |
| `RepairAction` | omitted | `wire.RepairAction` | plain-value |
| `RepairAction::ReconciledPreparedAttempt` | omitted | `wire.RepairAction` | plain-value |
| `RepairAction::AcceptedRetentionGap` | omitted | `wire.RepairAction` | plain-value |
| `RepairAction::ClearedRetryableBlock` | omitted | `wire.RepairAction` | plain-value |
| `RepairAction::SupersededGeneration` | omitted | `wire.RepairAction` | plain-value |
| `RepairRecord` | dict via `Destinations.record_repair` | `wire.RepairRecord` | serde-dict |
| `RepairRecord.action` | dict key | `wire.RepairRecord.action` | serde-dict, keywords |
| `RepairRecord.detail` | dict key | `wire.RepairRecord.detail` | serde-dict, keywords |
| `RetentionGap` | dict via `Destinations.record_retention_gap` | `wire.RetentionGap` | serde-dict |
| `RetentionGap.incarnation` | dict key | `wire.RetentionGap.incarnation` | serde-dict, keywords |
| `RetentionGap.required_next_offset` | dict key | `wire.RetentionGap.requiredNextOffset` | serde-dict, keywords |
| `RetentionGap.retained_start` | dict key | `wire.RetentionGap.retainedStart` | serde-dict, keywords |
| `RunBudget` | `RunBudget` | `wire.RunBudget` |  |
| `RunBudget.max_events` | `RunBudget.max_events` | `wire.RunBudget.maxEvents` | keywords |
| `RunBudget.max_model_calls` | `RunBudget.max_model_calls` | `wire.RunBudget.maxModelCalls` | keywords |
| `RunBudget.max_tool_calls` | `RunBudget.max_tool_calls` | `wire.RunBudget.maxToolCalls` | keywords |
| `RunBudget.max_patches` | `RunBudget.max_patches` | `wire.RunBudget.maxPatches` | keywords |
| `RunBudget.max_depth` | `RunBudget.max_depth` | `wire.RunBudget.maxDepth` | keywords |
| `RunBudget.max_wall_clock_micros` | `RunBudget.max_wall_clock_micros` | `wire.RunBudget.maxWallClockMicros` | keywords |
| `RunBudget.max_cost_usd` | `RunBudget.max_cost_usd` | `wire.RunBudget.maxCostUsd` | keywords |
| `RunPage` | `RunPage` | `RunPage` |  |
| `RunPage.runs` | `RunPage.runs` | `RunPage.runs` | keywords |
| `RunPage.cursor` | `RunPage.cursor` | `RunPage.cursor` | keywords |
| `SchemaFingerprint` | omitted | `SchemaFingerprint` | plain-value |
| `SchemaFingerprint::BYTES` | `const:SCHEMA_FINGERPRINT_BYTES` | `SchemaFingerprint.BYTES` | free-function |
| `SchemaFingerprint::as_bytes` | omitted | omitted | plain-value |
| `SchemaFingerprint::new` | omitted | omitted | plain-value |
| `SourceIncarnation` | dict via `Destinations.get` | `wire.SourceIncarnation` | serde-dict |
| `SourceIncarnation.cluster` | dict key | `wire.SourceIncarnation.cluster` | serde-dict, keywords |
| `SourceIncarnation.stream_id` | dict key | `wire.SourceIncarnation.streamId` | serde-dict, keywords |
| `SourceIncarnation.topic_id` | dict key | `wire.SourceIncarnation.topicId` | serde-dict, keywords |
| `SourceIncarnation.partition_id` | dict key | `wire.SourceIncarnation.partitionId` | serde-dict, keywords |
| `SourceIncarnation.partition_created_revision` | dict key | `wire.SourceIncarnation.partitionCreatedRevision` | serde-dict, keywords |
| `SourceOffsetRange` | dict via `Destinations.prepare` | `wire.SourceOffsetRange` | serde-dict |
| `SourceOffsetRange.incarnation` | dict key | `wire.SourceOffsetRange.incarnation` | serde-dict, keywords |
| `SourceOffsetRange.start` | dict key | `wire.SourceOffsetRange.start` | serde-dict, keywords |
| `SourceOffsetRange.end_exclusive` | dict key | `wire.SourceOffsetRange.endExclusive` | serde-dict, keywords |
| `SourceRef` | dict via `GraphHandle.fetch` | `SourceRef` | serde-dict |
| `SourceRef::Message` | dict key | `SourceRef` | serde-dict, plain-value |
| `SourceRef::Kv` | dict key | `SourceRef` | serde-dict, plain-value |
| `SourceRef::Memory` | dict key | `SourceRef` | serde-dict, plain-value |
| `SourceScope` | dict via `Destinations.register` | `wire.SourceScope` | serde-dict |
| `SourceScope.stream` | dict key | `wire.SourceScope.stream` | serde-dict, keywords |
| `SourceScope.topic` | dict key | `wire.SourceScope.topic` | serde-dict, keywords |
| `SourceScope::new` | `fn:new_source_scope` | `wire.SourceScope` | free-function, keywords |
| `StartPolicy` | dict via `Destinations.register` | `wire.StartPolicy` | serde-dict |
| `StartPolicy::Beginning` | dict key | `wire.StartPolicy` | serde-dict, plain-value |
| `StartPolicy::CapturedLatest` | dict key | `wire.StartPolicy` | serde-dict, plain-value |
| `StartPolicy::Explicit` | dict key | `wire.StartPolicy` | serde-dict, plain-value |
| `SupervisorActorAssertion` | dict via `Destinations.mutate` | `wire.SupervisorActorAssertion` | serde-dict |
| `SupervisorActorAssertion.claims` | dict key | `wire.SupervisorActorAssertion.claims` | serde-dict, keywords |
| `SupervisorActorAssertion.key_id` | dict key | `wire.SupervisorActorAssertion.keyId` | serde-dict, keywords |
| `SupervisorActorAssertion.signature` | dict key | `wire.SupervisorActorAssertion.signature` | serde-dict, keywords |
| `SupervisorActorAssertion::validate` | `fn:validate_supervisor_actor_assertion` | `fn:validateSupervisorAssertion` | free-function |
| `SupervisorActorClaims` | dict via `Destinations.mutate` | `wire.SupervisorActorClaims` | serde-dict |
| `SupervisorActorClaims.v` | dict key | `wire.SupervisorActorClaims.v` | serde-dict, keywords |
| `SupervisorActorClaims.request_id` | dict key | `wire.SupervisorActorClaims.requestId` | serde-dict, keywords |
| `SupervisorActorClaims.deployment_id` | dict key | `wire.SupervisorActorClaims.deploymentId` | serde-dict, keywords |
| `SupervisorActorClaims.cloud_user_id` | dict key | `wire.SupervisorActorClaims.cloudUserId` | serde-dict, keywords |
| `SupervisorActorClaims.action` | dict key | `wire.SupervisorActorClaims.action` | serde-dict, keywords |
| `SupervisorActorClaims.destination_id` | dict key | `wire.SupervisorActorClaims.destinationId` | serde-dict, keywords |
| `SupervisorActorClaims.destination_generation` | dict key | `wire.SupervisorActorClaims.destinationGeneration` | serde-dict, keywords |
| `SupervisorActorClaims.expected_revision` | dict key | `wire.SupervisorActorClaims.expectedRevision` | serde-dict, keywords |
| `SupervisorActorClaims.issued_at_micros` | dict key | `wire.SupervisorActorClaims.issuedAtMicros` | serde-dict, keywords |
| `SupervisorActorClaims.expires_at_micros` | dict key | `wire.SupervisorActorClaims.expiresAtMicros` | serde-dict, keywords |
| `SupervisorAssertionAction` | omitted | `wire.SupervisorAssertionAction` | plain-value |
| `SupervisorAssertionAction::AcceptRetentionGap` | omitted | `wire.SupervisorAssertionAction` | plain-value |
| `SupervisorAssertionAction::SupersedeGeneration` | omitted | `wire.SupervisorAssertionAction` | plain-value |
| `SupervisorAssertionAction::RecordRepair` | omitted | `wire.SupervisorAssertionAction` | plain-value |
| `TableFormat` | omitted | `wire.TableFormat` | plain-value |
| `TableFormat::IcebergV2` | omitted | `wire.TableFormat` | plain-value |
| `UuidValue` | omitted | `UuidValue` | plain-value |
| `UuidValue::BYTES` | `const:UUID_VALUE_BYTES` | `UuidValue.BYTES` | free-function |
| `UuidValue::as_bytes` | omitted | omitted | plain-value |
| `UuidValue::new` | omitted | omitted | plain-value |

## projections

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `Bindings` | `Bindings` | `Bindings` |  |
| `Bindings::apply` | `Bindings.apply` | `Bindings.apply` |  |
| `Bindings::remove` | `Bindings.remove` | `Bindings.remove` |  |
| `Projections` | `Projections` | `Projections` |  |
| `Projections::drop` | `Projections.drop` | `Projections.drop` |  |
| `Projections::drop_graph` | `Projections.drop_graph` | `Projections.dropGraph` |  |
| `Projections::get` | `Projections.get` | `Projections.get` |  |
| `Projections::list` | `Projections.list` | `Projections.list` |  |
| `Projections::register` | `Projections.register` | `Projections.register` |  |
| `Projections::register_graph` | `Projections.register_graph` | `Projections.registerGraph` |  |
| `ProjectionsRequest` | `Projections.list` | `ProjectionsRequest` | keywords |
| `ProjectionsRequest::fetch` | `Projections.list` | `ProjectionsRequest.fetch` | one-call |
| `ProjectionsRequest::for_topic` | `Projections.list(topic=)` | `ProjectionsRequest.forTopic` | keywords |
| `ProjectionsRequest::for_topics` | `Projections.list(topics=)` | `ProjectionsRequest.forTopics` | keywords |
| `ProjectionsRequest::id_prefix` | `Projections.list(id_prefix=)` | `ProjectionsRequest.idPrefix` | keywords |
| `ProjectionsRequest::name_contains` | `Projections.list(name_contains=)` | `ProjectionsRequest.nameContains` | keywords |
| `ProjectionsRequest::search` | `Projections.list(search=)` | `ProjectionsRequest.search` | keywords |
| `RegisterSchemaRequest` | `Schemas.register` | `RegisterSchemaRequest` | keywords |
| `RegisterSchemaRequest::name` | `Schemas.register(name=)` | `RegisterSchemaRequest.name` | keywords |
| `RegisterSchemaRequest::send` | `Schemas.register` | `RegisterSchemaRequest.send` | one-call |
| `RegisterSchemaRequest::version` | `Schemas.register(version=)` | `RegisterSchemaRequest.version` | keywords |
| `Schemas` | `Schemas` | `Schemas` |  |
| `Schemas::drop` | `Schemas.drop` | `Schemas.drop` |  |
| `Schemas::get` | `Schemas.get` | `Schemas.get` |  |
| `Schemas::list` | `Schemas.list` | `Schemas.list` |  |
| `Schemas::register` | `Schemas.register` | `Schemas.register` |  |

## provenance

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentTopic` | `AgentTopic` | `AgentTopic` |  |
| `AgentTopic::Commands` | `AgentTopic.Commands` | `AgentTopic.Commands` |  |
| `AgentTopic::Responses` | `AgentTopic.Responses` | `AgentTopic.Responses` |  |
| `AgentTopic::ToolCalls` | `AgentTopic.ToolCalls` | `AgentTopic.ToolCalls` |  |
| `AgentTopic::ToolResults` | `AgentTopic.ToolResults` | `AgentTopic.ToolResults` |  |
| `AgentTopic::LlmIo` | `AgentTopic.LlmIo` | `AgentTopic.LlmIo` |  |
| `AgentTopic::HumanInput` | `AgentTopic.HumanInput` | `AgentTopic.HumanInput` |  |
| `AgentTopic::Audit` | `AgentTopic.Audit` | `AgentTopic.Audit` |  |
| `AgentTopic::Registry` | `AgentTopic.Registry` | `AgentTopic.Registry` |  |
| `AgentTopic::WorkflowJournal` | `AgentTopic.WorkflowJournal` | `AgentTopic.WorkflowJournal` |  |
| `AgentTopic::Dlq` | `AgentTopic.Dlq` | `AgentTopic.Dlq` |  |
| `AgentTopic::Custom` | omitted | `AgentTopic.Custom` | plain-value |
| `AgentTopic::as_identifier` | omitted | omitted | rust-crate, plain-value |
| `AgentTopic::name` | omitted | omitted | plain-value |
| `AgentTopic::topic_string` | omitted | omitted | plain-value |
| `LlmUsage` | `LlmUsage` | `LlmUsage` |  |
| `LlmUsage.input_tokens` | `LlmUsage.input_tokens` | `LlmUsage.inputTokens` | keywords |
| `LlmUsage.output_tokens` | `LlmUsage.output_tokens` | `LlmUsage.outputTokens` | keywords |
| `LlmUsage.cost_usd` | `LlmUsage.cost_usd` | `LlmUsage.costUsd` | keywords |
| `LlmUsage::builder` | `new Provenance()` | `LlmUsage` | keywords |
| `LlmUsageBuilder` | `new Provenance()` | `LlmUsage` | keywords |
| `LlmUsageBuilder::build` | `new Provenance()` | `LlmUsage` | one-call, keywords |
| `LlmUsageBuilder::cost_usd` | `new Provenance(cost_usd=)` | `LlmUsage.costUsd` | keywords |
| `LlmUsageBuilder::input_tokens` | `new Provenance(input_tokens=)` | `LlmUsage.inputTokens` | keywords |
| `LlmUsageBuilder::maybe_cost_usd` | `new Provenance(cost_usd=)` | `LlmUsage.costUsd` | keywords |
| `LlmUsageBuilder::maybe_input_tokens` | `new Provenance(input_tokens=)` | `LlmUsage.inputTokens` | keywords |
| `LlmUsageBuilder::maybe_output_tokens` | `new Provenance(output_tokens=)` | `LlmUsage.outputTokens` | keywords |
| `LlmUsageBuilder::output_tokens` | `new Provenance(output_tokens=)` | `LlmUsage.outputTokens` | keywords |
| `Provenance` | `Provenance` | `Provenance` |  |
| `Provenance.conversation_id` | `Provenance.conversation_id` | `Provenance.conversationId` | keywords |
| `Provenance.causal_parent` | `Provenance.causal_parent` | `Provenance.causalParent` | keywords |
| `Provenance.parent_conversation_id` | `Provenance.parent_conversation_id` | `Provenance.parentConversationId` | keywords |
| `Provenance.root_conversation_id` | `Provenance.root_conversation_id` | `Provenance.rootConversationId` | keywords |
| `Provenance.agent` | `Provenance.agent` | `Provenance.agent` | keywords |
| `Provenance.target_agent_id` | `Provenance.target_agent_id` | `Provenance.targetAgentId` | keywords |
| `Provenance.usage` | `Provenance.usage` | `Provenance.usage` | keywords |
| `Provenance.deadline` | `Provenance.deadline` | `Provenance.deadlineMicros` | keywords |
| `Provenance.idempotency_key` | `Provenance.idempotency_key` | `Provenance.idempotencyKey` | keywords |
| `Provenance.correlation_id` | `Provenance.correlation_id` | `Provenance.correlationId` | keywords |
| `Provenance.fence_token` | `Provenance.fence_token` | `Provenance.fenceToken` | keywords |
| `Provenance::builder` | `new Provenance()` | `Provenance` | keywords |
| `Provenance::partition_key` | `Provenance.partition_key` | `fn:provenancePartitionKey` | free-function |
| `ProvenanceBuilder` | `new Provenance()` | `Provenance` | keywords |
| `ProvenanceBuilder::agent` | `new Provenance(agent=)` | `Provenance.agent` | keywords |
| `ProvenanceBuilder::build` | `new Provenance()` | `Provenance` | one-call, keywords |
| `ProvenanceBuilder::causal_parent` | `new Provenance(causal_parent=)` | `Provenance.causalParent` | keywords |
| `ProvenanceBuilder::conversation_id` | `new Provenance(conversation_id=)` | `Provenance.conversationId` | keywords |
| `ProvenanceBuilder::correlation_id` | `new Provenance(correlation_id=)` | `Provenance.correlationId` | keywords |
| `ProvenanceBuilder::deadline` | `new Provenance(deadline_micros=)` | `Provenance.deadlineMicros` | keywords |
| `ProvenanceBuilder::fence_token` | `new Provenance(fence_token=)` | `Provenance.fenceToken` | keywords |
| `ProvenanceBuilder::idempotency_key` | `new Provenance(idempotency_key=)` | `Provenance.idempotencyKey` | keywords |
| `ProvenanceBuilder::maybe_agent` | `new Provenance(agent=)` | `Provenance.agent` | keywords |
| `ProvenanceBuilder::maybe_causal_parent` | `new Provenance(causal_parent=)` | `Provenance.causalParent` | keywords |
| `ProvenanceBuilder::maybe_correlation_id` | `new Provenance(correlation_id=)` | `Provenance.correlationId` | keywords |
| `ProvenanceBuilder::maybe_deadline` | `new Provenance(deadline_micros=)` | `Provenance.deadlineMicros` | keywords |
| `ProvenanceBuilder::maybe_fence_token` | `new Provenance(fence_token=)` | `Provenance.fenceToken` | keywords |
| `ProvenanceBuilder::maybe_idempotency_key` | `new Provenance(idempotency_key=)` | `Provenance.idempotencyKey` | keywords |
| `ProvenanceBuilder::maybe_parent_conversation_id` | `new Provenance(parent_conversation_id=)` | `Provenance.parentConversationId` | keywords |
| `ProvenanceBuilder::maybe_root_conversation_id` | `new Provenance(root_conversation_id=)` | `Provenance.rootConversationId` | keywords |
| `ProvenanceBuilder::maybe_target_agent_id` | `new Provenance(target_agent_id=)` | `Provenance.targetAgentId` | keywords |
| `ProvenanceBuilder::maybe_usage` | `new Provenance(cost_usd=, input_tokens=, output_tokens=)` | `Provenance.usage` | keywords |
| `ProvenanceBuilder::parent_conversation_id` | `new Provenance(parent_conversation_id=)` | `Provenance.parentConversationId` | keywords |
| `ProvenanceBuilder::root_conversation_id` | `new Provenance(root_conversation_id=)` | `Provenance.rootConversationId` | keywords |
| `ProvenanceBuilder::target_agent_id` | `new Provenance(target_agent_id=)` | `Provenance.targetAgentId` | keywords |
| `ProvenanceBuilder::usage` | `new Provenance(cost_usd=, input_tokens=, output_tokens=)` | `Provenance.usage` | keywords |
| `ProvenanceError` | `ProvenanceError` | `ProvenanceError` |  |
| `ProvenanceError::MissingRequired` | `ProvenanceError.MISSING_REQUIRED` | `ProvenanceError.missingRequired` |  |
| `ProvenanceError::TooLarge` | `ProvenanceError.TOO_LARGE` | `ProvenanceError.tooLarge` |  |
| `ProvenanceError::InvalidValue` | `ProvenanceError.INVALID_VALUE` | `ProvenanceError.invalidValue` |  |
| `ProvenanceError::InvalidValueBytes` | `ProvenanceError.INVALID_VALUE_BYTES` | `ProvenanceError.invalidValueBytes` |  |
| `ProvenanceError::NonFinite` | `ProvenanceError.NON_FINITE` | `ProvenanceError.nonFinite` |  |
| `ProvenanceError::EmptyValue` | `ProvenanceError.EMPTY_VALUE` | `ProvenanceError.emptyValue` |  |
| `ProvenanceError::ValueTooLong` | `ProvenanceError.VALUE_TOO_LONG` | `ProvenanceError.valueTooLong` |  |
| `ProvenanceError::MalformedHeaders` | `ProvenanceError.MALFORMED_HEADERS` | `ProvenanceError.malformedHeaders` |  |
| `ProvenanceError::Header` | `ProvenanceError.HEADER` | `ProvenanceError.header` |  |
| `ProvenanceError::Id` | `ProvenanceError.ID` | `ProvenanceError.id` |  |

## provenance::keys

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `provenance::keys::AGENT_ID` | `const:AGENT_ID` | `const:AGENT_ID` |  |
| `provenance::keys::CAUSAL_PARENT` | `const:CAUSAL_PARENT` | `const:CAUSAL_PARENT` |  |
| `provenance::keys::CONVERSATION_ID` | `const:CONVERSATION_ID` | `const:CONVERSATION_ID` |  |
| `provenance::keys::COST_USD` | `const:COST_USD` | `const:COST_USD` |  |
| `provenance::keys::DEADLINE` | `const:DEADLINE` | `const:DEADLINE` |  |
| `provenance::keys::FENCE` | `const:FENCE` | `const:FENCE` |  |
| `provenance::keys::IDEMPOTENCY_KEY` | `const:IDEMPOTENCY_KEY` | `const:IDEMPOTENCY_KEY` |  |
| `provenance::keys::PARENT_CONVERSATION_ID` | `const:PARENT_CONVERSATION_ID` | `const:PARENT_CONVERSATION_ID` |  |
| `provenance::keys::ROOT_CONVERSATION_ID` | `const:ROOT_CONVERSATION_ID` | `const:ROOT_CONVERSATION_ID` |  |
| `provenance::keys::TARGET_AGENT_ID` | `const:TARGET_AGENT_ID` | `const:TARGET_AGENT_ID` |  |
| `provenance::keys::USAGE_INPUT_TOKENS` | `const:USAGE_INPUT_TOKENS` | `const:USAGE_INPUT_TOKENS` |  |
| `provenance::keys::USAGE_OUTPUT_TOKENS` | `const:USAGE_OUTPUT_TOKENS` | `const:USAGE_OUTPUT_TOKENS` |  |

## query

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `query::AGDX_COMMAND_BASE` | `const:AGDX_COMMAND_BASE` | `const:AGDX_COMMAND_BASE` |  |
| `query::AGDX_DECODE_RECORD_CODE` | `const:AGDX_DECODE_RECORD_CODE` | `const:AGDX_DECODE_RECORD_CODE` |  |
| `query::AGDX_FORK_BASE` | `const:AGDX_FORK_BASE` | `const:AGDX_FORK_BASE` |  |
| `query::AGDX_FORK_CREATE_CODE` | `const:AGDX_FORK_CREATE_CODE` | `const:AGDX_FORK_CREATE_CODE` |  |
| `query::AGDX_FORK_DELETE_CODE` | `const:AGDX_FORK_DELETE_CODE` | `const:AGDX_FORK_DELETE_CODE` |  |
| `query::AGDX_FORK_LIST_CODE` | `const:AGDX_FORK_LIST_CODE` | `const:AGDX_FORK_LIST_CODE` |  |
| `query::AGDX_FORK_PROMOTE_CODE` | `const:AGDX_FORK_PROMOTE_CODE` | `const:AGDX_FORK_PROMOTE_CODE` |  |
| `query::AGDX_FORK_PUT_CODE` | `const:AGDX_FORK_PUT_CODE` | `const:AGDX_FORK_PUT_CODE` |  |
| `query::AGDX_GET_PROJECTION_CODE` | `const:AGDX_GET_PROJECTION_CODE` | `const:AGDX_GET_PROJECTION_CODE` |  |
| `query::AGDX_GET_SCHEMA_CODE` | `const:AGDX_GET_SCHEMA_CODE` | `const:AGDX_GET_SCHEMA_CODE` |  |
| `query::AGDX_HELLO_CODE` | `const:AGDX_HELLO_CODE` | `const:AGDX_HELLO_CODE` |  |
| `query::AGDX_LIST_PROJECTIONS_CODE` | `const:AGDX_LIST_PROJECTIONS_CODE` | `const:AGDX_LIST_PROJECTIONS_CODE` |  |
| `query::AGDX_LIST_SCHEMAS_CODE` | `const:AGDX_LIST_SCHEMAS_CODE` | `const:AGDX_LIST_SCHEMAS_CODE` |  |
| `query::AGDX_QUERY_CANCEL_CODE` | `const:AGDX_QUERY_CANCEL_CODE` | `const:AGDX_QUERY_CANCEL_CODE` |  |
| `query::AGDX_QUERY_CODE` | `const:AGDX_QUERY_CODE` | `const:AGDX_QUERY_CODE` |  |
| `query::AGDX_QUERY_PAGE_CODE` | `const:AGDX_QUERY_PAGE_CODE` | `const:AGDX_QUERY_PAGE_CODE` |  |
| `query::AGDX_QUERY_STATUS_CODE` | `const:AGDX_QUERY_STATUS_CODE` | `const:AGDX_QUERY_STATUS_CODE` |  |
| `query::AGDX_REGISTER_SCHEMA_CODE` | `const:AGDX_REGISTER_SCHEMA_CODE` | `const:AGDX_REGISTER_SCHEMA_CODE` |  |
| `AggCall` | dict via `Laser.execute_query` | `wire.AggCall` | serde-dict |
| `AggCall.func` | dict key | `wire.AggCall.func` | serde-dict, keywords |
| `AggCall.field` | dict key | `wire.AggCall.field` | serde-dict, keywords |
| `AggCall.arg` | dict key | `wire.AggCall.arg` | serde-dict, keywords |
| `AggCall.alias` | dict key | `wire.AggCall.alias` | serde-dict, keywords |
| `AggFunc` | omitted | `AggFunc` | plain-value |
| `AggFunc::Count` | omitted | `AggFunc` | plain-value |
| `AggFunc::CountDistinct` | omitted | `AggFunc` | plain-value |
| `AggFunc::Sum` | omitted | `AggFunc` | plain-value |
| `AggFunc::Avg` | omitted | `AggFunc` | plain-value |
| `AggFunc::Min` | omitted | `AggFunc` | plain-value |
| `AggFunc::Max` | omitted | `AggFunc` | plain-value |
| `AggFunc::Percentile` | omitted | `AggFunc` | plain-value |
| `AggFunc::StdDev` | omitted | `AggFunc` | plain-value |
| `Aggregate` | dict via `Laser.execute_query` | `wire.Aggregate` | serde-dict |
| `Aggregate.group_by` | dict key | `wire.Aggregate.groupBy` | serde-dict, keywords |
| `Aggregate.funcs` | dict key | `wire.Aggregate.funcs` | serde-dict, keywords |
| `Aggregate.window` | dict key | `wire.Aggregate.window` | serde-dict, keywords |
| `BoundaryRelation` | omitted | `wire.BoundaryRelation` | plain-value |
| `BoundaryRelation::Current` | omitted | `wire.BoundaryRelation` | plain-value |
| `BoundaryRelation::Historical` | omitted | `wire.BoundaryRelation` | plain-value |
| `BoundaryRelation::AheadOfObservedCheckpoint` | omitted | `wire.BoundaryRelation` | plain-value |
| `Bson` | `Bson` | `Bson` |  |
| `query::CONTENT_TYPE` | `const:CONTENT_TYPE` | `const:CONTENT_TYPE` |  |
| `query::CONTROL_OP_VERSION` | `const:CONTROL_OP_VERSION` | `const:CONTROL_OP_VERSION` |  |
| `query::CONTROL_TOPIC` | `const:CONTROL_TOPIC` | `const:CONTROL_TOPIC` |  |
| `CmpOp` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Eq` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Ne` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Lt` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Lte` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Gt` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Gte` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::In` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Contains` | omitted | `wire.CmpOp` | plain-value |
| `CmpOp::Prefix` | omitted | `wire.CmpOp` | plain-value |
| `Consistency` | omitted | `Consistency` | plain-value |
| `Consistency::Eventual` | omitted | `Consistency` | plain-value |
| `Consistency::ReadYourWrites` | omitted | `Consistency` | plain-value |
| `Consistency::Strong` | omitted | `Consistency` | plain-value |
| `Consistency::is_eventual` | `fn:consistency_is_eventual` | `fn:consistencyIsEventual` | free-function |
| `query::DEFAULT_STREAM_PAGE_SIZE` | `const:DEFAULT_STREAM_PAGE_SIZE` | `const:DEFAULT_STREAM_PAGE_SIZE` |  |
| `query::DLQ_TOPIC` | `const:DLQ_TOPIC` | `const:DLQ_TOPIC` |  |
| `Dir` | omitted | `wire.Dir` | plain-value |
| `Dir::Asc` | omitted | `wire.Dir` | plain-value |
| `Dir::Desc` | omitted | `wire.Dir` | plain-value |
| `EdgeExtract` | dict via `Projections.register_graph` | `EdgeExtract` | serde-dict |
| `EdgeExtract.edge_type` | dict key | `EdgeExtract.edgeType` | serde-dict, keywords |
| `EdgeExtract.from_pointer` | dict key | `EdgeExtract.fromPointer` | serde-dict, keywords |
| `EdgeExtract.to_pointer` | dict key | `EdgeExtract.toPointer` | serde-dict, keywords |
| `EdgeExtract.valid_from_pointer` | dict key | `EdgeExtract.validFromPointer` | serde-dict, keywords |
| `EdgeExtract.valid_to_pointer` | dict key | `EdgeExtract.validToPointer` | serde-dict, keywords |
| `EntitySchema` | dict via `Projections.register_graph` | `EntitySchema` | serde-dict |
| `EntitySchema.nodes` | dict key | `EntitySchema.nodes` | serde-dict, keywords |
| `EntitySchema.edges` | dict key | `EntitySchema.edges` | serde-dict, keywords |
| `query::FIELD_MESSAGE_TYPE` | `const:FIELD_MESSAGE_TYPE` | `const:FIELD_MESSAGE_TYPE` |  |
| `query::FIELD_TS` | `const:FIELD_TS` | `const:FIELD_TS` |  |
| `query::FORK_OP_VERSION` | `const:FORK_OP_VERSION` | `const:FORK_OP_VERSION` |  |
| `FieldType` | omitted | `FieldType` | plain-value |
| `FieldType::Text` | omitted | `FieldType` | plain-value |
| `FieldType::Int` | omitted | `FieldType` | plain-value |
| `FieldType::Float` | omitted | `FieldType` | plain-value |
| `FieldType::Bool` | omitted | `FieldType` | plain-value |
| `Filter` | `Filter` | `Filter` |  |
| `Filter::All` | `Filter.all` | `Filter` | plain-value |
| `Filter::Any` | `Filter.any` | `Filter` | plain-value |
| `Filter::Not` | `Filter.negate` | `Filter` | constructor, plain-value |
| `Filter::Pred` | `Filter.pred` | `Filter` | plain-value |
| `Filter::all` | `Filter.all` | `fn:filterAll` | free-function |
| `Filter::any` | `Filter.any` | `fn:filterAny` | free-function |
| `Filter::negate` | `Filter.negate` | `fn:filterNegate` | free-function |
| `Filter::pred` | `Filter.pred` | `fn:filterPred` | free-function |
| `query::IDX_PREFIX` | `const:IDX_PREFIX` | `const:IDX_PREFIX` |  |
| `query::INLINE_PAYLOAD` | `const:INLINE_PAYLOAD` | `const:INLINE_PAYLOAD` |  |
| `IndexField` | dict via `Projections.register` | `IndexField` | serde-dict |
| `IndexField.name` | dict key | `IndexField.name` | serde-dict, keywords |
| `IndexField.pointer` | dict key | `IndexField.pointer` | serde-dict, keywords |
| `IndexField.field_type` | dict key | `IndexField.fieldType` | serde-dict, keywords |
| `IndexField::new` | `fn:index_field_new` | `IndexField` | free-function, keywords |
| `IndexField::typed` | `fn:index_field_typed` | `IndexField` | free-function, keywords |
| `IndexSchema` | dict via `Projections.register` | `IndexSchema` | serde-dict |
| `IndexSchema.fields` | dict key | `IndexSchema.fields` | serde-dict, keywords |
| `IndexSchema.vector_field` | dict key | `IndexSchema.vectorField` | serde-dict, keywords |
| `IndexSchema.inline_payload` | dict key | `IndexSchema.inlinePayload` | serde-dict, keywords |
| `IndexSchema::builder` | `new IndexSchemaBuilder()` | `new IndexSchemaBuilder()` | constructor |
| `IndexSchemaBuilder` | `IndexSchemaBuilder` | `IndexSchemaBuilder` |  |
| `IndexSchemaBuilder::build` | `IndexSchemaBuilder.build` | `IndexSchemaBuilder.build` |  |
| `IndexSchemaBuilder::field` | `IndexSchemaBuilder.field` | `IndexSchemaBuilder.field` |  |
| `IndexSchemaBuilder::field_at` | `IndexSchemaBuilder.field_at` | `IndexSchemaBuilder.fieldAt` |  |
| `IndexSchemaBuilder::inline_payload` | `IndexSchemaBuilder.inline_payload` | `IndexSchemaBuilder.inlinePayload` |  |
| `IndexSchemaBuilder::vector_field` | `IndexSchemaBuilder.vector_field` | `IndexSchemaBuilder.vectorField` |  |
| `KeyMatch` | dict via `Laser.execute_query` | `wire.KeyMatch` | serde-dict |
| `KeyMatch.field` | dict key | `wire.KeyMatch.field` | serde-dict, keywords |
| `KeyMatch.value` | dict key | `wire.KeyMatch.value` | serde-dict, keywords |
| `KeyMatch::new` | `fn:key_match_new` | `wire.KeyMatch` | free-function, keywords |
| `query::MAX_INDEX_ENTRIES_PER_RECORD` | `const:MAX_INDEX_ENTRIES_PER_RECORD` | `const:MAX_INDEX_ENTRIES_PER_RECORD` |  |
| `query::MAX_PAGE_SIZE` | `const:MAX_PAGE_SIZE` | `const:MAX_PAGE_SIZE` |  |
| `MaterializationBoundary` | dict via `QueryResult.context` | `wire.MaterializationBoundary` | serde-dict |
| `MaterializationBoundary.digest` | dict key | `wire.MaterializationBoundary.digest` | serde-dict, keywords |
| `MaterializationBoundary.relation_to_current` | dict key | `wire.MaterializationBoundary.relationToCurrent` | serde-dict, keywords |
| `NodeExtract` | dict via `Projections.register_graph` | `NodeExtract` | serde-dict |
| `NodeExtract.label` | dict key | `NodeExtract.label` | serde-dict, keywords |
| `NodeExtract.value_pointer` | dict key | `NodeExtract.valuePointer` | serde-dict, keywords |
| `NodeExtract.embedding_pointer` | dict key | `NodeExtract.embeddingPointer` | serde-dict, keywords |
| `query::OPS_STREAM` | `const:OPS_STREAM` | `const:OPS_STREAM` |  |
| `query::PROJECTION_REF` | `const:PROJECTION_REF` | `const:PROJECTION_REF` |  |
| `Page` | `Page` | `Page` |  |
| `Page.offset` | `Page.offset` | `Page.offset` | keywords |
| `Page.limit` | `Page.limit` | `Page.limit` | keywords |
| `Page.total` | `Page.total` | `Page.total` | keywords |
| `Page.has_more` | `Page.has_more` | `Page.hasMore` | keywords |
| `Page.next_cursor` | `Page.next_cursor` | `Page.nextCursor` | keywords |
| `Page::at_least` | `Page.at_least` | `fn:pageAtLeast` | free-function |
| `Page::total_pages` | `Page.total_pages` | `fn:pageTotalPages` | free-function |
| `Predicate` | dict via `Laser.execute_query` | `Predicate` | serde-dict |
| `Predicate.field` | dict key | `Predicate.field` | serde-dict, keywords |
| `Predicate.op` | dict key | `Predicate.op` | serde-dict, keywords |
| `Predicate.value` | dict key | `Predicate.value` | serde-dict, keywords |
| `Projection` | dict via `Projections.register` | `Projection` | serde-dict |
| `Projection.id` | dict key | `Projection.id` | serde-dict, keywords |
| `Projection.name` | dict key | `Projection.name` | serde-dict, keywords |
| `Projection.version` | dict key | `Projection.version` | serde-dict, keywords |
| `Projection.kind` | dict key | `Projection.kind` | serde-dict, keywords |
| `Projection.content_type` | dict key | `Projection.contentType` | serde-dict, keywords |
| `Projection.extraction` | dict key | `Projection.extraction` | serde-dict, keywords |
| `Projection.entity_schema` | dict key | `Projection.entitySchema` | serde-dict, keywords |
| `Projection.inline_payload_default` | dict key | `Projection.inlinePayloadDefault` | serde-dict, keywords |
| `Projection::builder` | `new ProjectionBuilder()` | `new ProjectionBuilder()` | constructor |
| `ProjectionBinding` | dict via `Bindings.apply` | `ProjectionBinding` | serde-dict |
| `ProjectionBinding.source` | dict key | `ProjectionBinding.source` | serde-dict, keywords |
| `ProjectionBinding.allowed_projections` | dict key | `ProjectionBinding.allowedProjections` | serde-dict, keywords |
| `ProjectionBinding.default_projection` | dict key | `ProjectionBinding.defaultProjection` | serde-dict, keywords |
| `ProjectionBinding.backend` | dict key | `ProjectionBinding.backend` | serde-dict, keywords |
| `ProjectionBinding.index` | dict key | `ProjectionBinding.index` | serde-dict, keywords |
| `ProjectionBinding.notify` | dict key | `ProjectionBinding.notify` | serde-dict, keywords |
| `ProjectionBinding.retention` | dict key | `ProjectionBinding.retention` | serde-dict, keywords |
| `ProjectionBinding::builder` | `new ProjectionBindingBuilder()` | `new ProjectionBindingBuilder()` | constructor |
| `ProjectionBindingBuilder` | `ProjectionBindingBuilder` | `ProjectionBindingBuilder` |  |
| `ProjectionBindingBuilder::allow` | `ProjectionBindingBuilder.allow` | `ProjectionBindingBuilder.allow` |  |
| `ProjectionBindingBuilder::backend` | `ProjectionBindingBuilder.backend` | `ProjectionBindingBuilder.backend` |  |
| `ProjectionBindingBuilder::build` | `ProjectionBindingBuilder.build` | `ProjectionBindingBuilder.build` |  |
| `ProjectionBindingBuilder::default_projection` | `ProjectionBindingBuilder.default_projection` | `ProjectionBindingBuilder.defaultProjection` |  |
| `ProjectionBindingBuilder::index` | `ProjectionBindingBuilder.index` | `ProjectionBindingBuilder.index` |  |
| `ProjectionBindingBuilder::notify` | `ProjectionBindingBuilder.notify` | `ProjectionBindingBuilder.notify` |  |
| `ProjectionBindingBuilder::retention` | `ProjectionBindingBuilder.retention` | `ProjectionBindingBuilder.retention` |  |
| `ProjectionBindingBuilder::selector` | `ProjectionBindingBuilder.selector` | `ProjectionBindingBuilder.selector` |  |
| `ProjectionBindingBuilder::source` | `ProjectionBindingBuilder.source` | `ProjectionBindingBuilder.source` |  |
| `ProjectionBindingBuilder::try_build` | `ProjectionBindingBuilder.try_build` | `ProjectionBindingBuilder.tryBuild` |  |
| `ProjectionBuilder` | `ProjectionBuilder` | `ProjectionBuilder` |  |
| `ProjectionBuilder::build` | `ProjectionBuilder.build` | `ProjectionBuilder.build` |  |
| `ProjectionBuilder::content_type` | `ProjectionBuilder.content_type` | `ProjectionBuilder.contentType` |  |
| `ProjectionBuilder::extraction` | `ProjectionBuilder.extraction` | `ProjectionBuilder.extraction` |  |
| `ProjectionBuilder::field` | `ProjectionBuilder.field` | `ProjectionBuilder.field` |  |
| `ProjectionBuilder::field_at` | `ProjectionBuilder.field_at` | `ProjectionBuilder.fieldAt` |  |
| `ProjectionBuilder::field_at_typed` | `ProjectionBuilder.field_at_typed` | `ProjectionBuilder.fieldAtTyped` |  |
| `ProjectionBuilder::field_typed` | `ProjectionBuilder.field_typed` | `ProjectionBuilder.fieldTyped` |  |
| `ProjectionBuilder::fields` | `ProjectionBuilder.fields` | `ProjectionBuilder.fields` |  |
| `ProjectionBuilder::graph` | `ProjectionBuilder.graph` | `ProjectionBuilder.graph` |  |
| `ProjectionBuilder::index_only` | `ProjectionBuilder.index_only` | `ProjectionBuilder.indexOnly` |  |
| `ProjectionBuilder::inline_payload` | `ProjectionBuilder.inline_payload` | `ProjectionBuilder.inlinePayload` |  |
| `ProjectionBuilder::name` | `ProjectionBuilder.name` | `ProjectionBuilder.name` |  |
| `ProjectionBuilder::vector_field` | `ProjectionBuilder.vector_field` | `ProjectionBuilder.vectorField` |  |
| `ProjectionBuilder::version` | `ProjectionBuilder.version` | `ProjectionBuilder.version` |  |
| `ProjectionId` | omitted | `wire.ProjectionId` | plain-value |
| `ProjectionId::as_str` | omitted | omitted | plain-value |
| `ProjectionId::new` | omitted | `fn:parseProjectionId` | plain-value, free-function |
| `ProjectionInfo` | dict via `Projections.get` | `ProjectionInfo` | serde-dict |
| `ProjectionInfo.projection` | dict key | `ProjectionInfo.projection` | serde-dict, keywords |
| `ProjectionInfo.bindings` | dict key | `ProjectionInfo.bindings` | serde-dict, keywords |
| `ProjectionKind` | omitted | `ProjectionKind` | plain-value |
| `ProjectionKind::Row` | omitted | `ProjectionKind` | plain-value |
| `ProjectionKind::Graph` | omitted | `ProjectionKind` | plain-value |
| `ProjectionKind::Unrecognized` | omitted | `ProjectionKind` | plain-value |
| `ProjectionKind::code` | `fn:projection_kind_code` | `fn:code` | free-function |
| `ProjectionKind::from_code` | omitted | `fn:projectionKindFromCode` | plain-value, free-function |
| `ProjectionKind::is_row` | `fn:projection_kind_is_row` | `fn:projectionKindIsRow` | free-function |
| `query::QUERY_OP_VERSION` | `const:QUERY_OP_VERSION` | `const:QUERY_OP_VERSION` |  |
| `Query` | dict via `Laser.execute_query` | `Query` | serde-dict |
| `Query.execution_id` | dict key | `Query.executionId` | serde-dict, keywords |
| `Query.target` | dict key | `Query.target` | serde-dict, keywords |
| `Query.deadline_micros` | dict key | `Query.deadlineMicros` | serde-dict, keywords |
| `Query.by_key` | dict key | `Query.byKey` | serde-dict, keywords |
| `Query.message_type` | dict key | `Query.messageType` | serde-dict, keywords |
| `Query.time_range` | dict key | `Query.timeRange` | serde-dict, keywords |
| `Query.filter` | dict key | `Query.filter` | serde-dict, keywords |
| `Query.vector` | dict key | `Query.vector` | serde-dict, keywords |
| `Query.text` | dict key | `Query.text` | serde-dict, keywords |
| `Query.order` | dict key | `Query.order` | serde-dict, keywords |
| `Query.page` | dict key | `Query.page` | serde-dict, keywords |
| `Query.aggregate` | dict key | `Query.aggregate` | serde-dict, keywords |
| `Query.having` | dict key | `Query.having` | serde-dict, keywords |
| `Query.distinct` | dict key | `Query.distinct` | serde-dict, keywords |
| `Query.select` | dict key | `Query.select` | serde-dict, keywords |
| `Query.fork` | dict key | `Query.fork` | serde-dict, keywords |
| `Query.raw_sql` | dict key | `Query.rawSql` | serde-dict, keywords |
| `Query.consistency` | dict key | `Query.consistency` | serde-dict, keywords |
| `Query::builder` | `new QueryBuilder()` | `Query` | constructor, keywords |
| `Query::new` | `fn:query_new` | `Query` | free-function, keywords |
| `Query::operational` | `fn:query_operational` | `fn:operationalQuery` | free-function |
| `QueryBuilder` | `QueryBuilder` | `Query` | keywords |
| `QueryBuilder::aggregate` | `QueryBuilder.aggregate` | `Query.aggregate` | keywords |
| `QueryBuilder::build` | `QueryBuilder.build` | `Query` | keywords |
| `QueryBuilder::by_key` | `QueryBuilder.by_key` | `Query.byKey` | keywords |
| `QueryBuilder::consistency` | `QueryBuilder.consistency` | `Query.consistency` | keywords |
| `QueryBuilder::deadline_micros` | `QueryBuilder.deadline_micros` | `Query.deadlineMicros` | keywords |
| `QueryBuilder::distinct` | `QueryBuilder.distinct` | `Query.distinct` | keywords |
| `QueryBuilder::execution_id` | `QueryBuilder.execution_id` | `Query.executionId` | keywords |
| `QueryBuilder::filter` | `QueryBuilder.filter` | `Query.filter` | keywords |
| `QueryBuilder::fork` | `QueryBuilder.fork` | `Query.fork` | keywords |
| `QueryBuilder::having` | `QueryBuilder.having` | `Query.having` | keywords |
| `QueryBuilder::maybe_aggregate` | `QueryBuilder.aggregate` | `Query.aggregate` | keywords |
| `QueryBuilder::maybe_filter` | `QueryBuilder.filter` | `Query.filter` | keywords |
| `QueryBuilder::maybe_fork` | `QueryBuilder.fork` | `Query.fork` | keywords |
| `QueryBuilder::maybe_having` | `QueryBuilder.having` | `Query.having` | keywords |
| `QueryBuilder::maybe_message_type` | `QueryBuilder.message_type` | `Query.messageType` | keywords |
| `QueryBuilder::maybe_raw_sql` | `QueryBuilder.raw_sql` | `Query.rawSql` | keywords |
| `QueryBuilder::maybe_text` | `QueryBuilder.text` | `Query.text` | keywords |
| `QueryBuilder::maybe_time_range` | `QueryBuilder.time_range` | `Query.timeRange` | keywords |
| `QueryBuilder::maybe_vector` | `QueryBuilder.vector` | `Query.vector` | keywords |
| `QueryBuilder::message_type` | `QueryBuilder.message_type` | `Query.messageType` | keywords |
| `QueryBuilder::order` | `QueryBuilder.order` | `Query.order` | keywords |
| `QueryBuilder::page` | `QueryBuilder.page` | `Query.page` | keywords |
| `QueryBuilder::raw_sql` | `QueryBuilder.raw_sql` | `Query.rawSql` | keywords |
| `QueryBuilder::select` | `QueryBuilder.select` | `Query.select` | keywords |
| `QueryBuilder::target` | `QueryBuilder.target` | `Query.target` | keywords |
| `QueryBuilder::text` | `QueryBuilder.text` | `Query.text` | keywords |
| `QueryBuilder::time_range` | `QueryBuilder.time_range` | `Query.timeRange` | keywords |
| `QueryBuilder::vector` | `QueryBuilder.vector` | `Query.vector` | keywords |
| `QueryContext` | dict via `QueryResult.context` | `QueryContext` | serde-dict |
| `QueryContext.execution_id` | dict key | `QueryContext.executionId` | serde-dict, keywords |
| `QueryContext.engine` | dict key | `QueryContext.engine` | serde-dict, keywords |
| `QueryContext.resolved_target` | dict key | `QueryContext.resolvedTarget` | serde-dict, keywords |
| `QueryContext.requested_consistency` | dict key | `QueryContext.requestedConsistency` | serde-dict, keywords |
| `QueryContext.delivered_consistency` | dict key | `QueryContext.deliveredConsistency` | serde-dict, keywords |
| `QueryContext.boundary` | dict key | `QueryContext.boundary` | serde-dict, keywords |
| `QueryContext.checkpoint_revision` | dict key | `QueryContext.checkpointRevision` | serde-dict, keywords |
| `QueryContext.global_state_revision` | dict key | `QueryContext.globalStateRevision` | serde-dict, keywords |
| `QueryContext.truncated` | dict key | `QueryContext.truncated` | serde-dict, keywords |
| `QueryContext.elapsed_micros` | dict key | `QueryContext.elapsedMicros` | serde-dict, keywords |
| `QueryContext.scanned_bytes` | dict key | `QueryContext.scannedBytes` | serde-dict, keywords |
| `QueryContext.produced_bytes` | dict key | `QueryContext.producedBytes` | serde-dict, keywords |
| `QueryContext.row_count` | dict key | `QueryContext.rowCount` | serde-dict, keywords |
| `QueryEngine` | dict via `QueryResult.context` | `wire.QueryEngine` | serde-dict |
| `QueryEngine.name` | dict key | `wire.QueryEngine.name` | serde-dict, keywords |
| `QueryEngine.version` | dict key | `wire.QueryEngine.version` | serde-dict, keywords |
| `QueryEngine.dialect` | dict key | `wire.QueryEngine.dialect` | serde-dict, keywords |
| `QueryError` | `QueryError` | `wire.QueryError` |  |
| `QueryError::Unsupported` | `QueryError.unsupported` | `wire.QueryError` | plain-value |
| `QueryError::Unauthorized` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::IndexNotFound` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::ForkNotFound` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::Backend` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::Unavailable` | `QueryError.unavailable` | `wire.QueryError` | plain-value |
| `QueryError::TooLarge` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::Version` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::Stale` | `QueryError.stale` | `wire.QueryError` | plain-value |
| `QueryError::Cancelled` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::DeadlineExceeded` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::ExpiredSnapshot` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::StaleGeneration` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::TargetUnavailable` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::ResourceLimit` | dict key | `wire.QueryError` | serde-dict, plain-value |
| `QueryError::code` | `QueryError.code` | `fn:code` | property, free-function |
| `QueryErrorCode` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Unsupported` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Unauthorized` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::IndexNotFound` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::ForkNotFound` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Backend` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Unavailable` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::TooLarge` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Version` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Stale` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::Cancelled` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::DeadlineExceeded` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::ExpiredSnapshot` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::StaleGeneration` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::TargetUnavailable` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryErrorCode::ResourceLimit` | omitted | `wire.QueryErrorCode` | plain-value |
| `QueryExecutionId` | omitted | `wire.QueryExecutionId` | plain-value |
| `QueryExecutionId::as_u128` | omitted | `wire.QueryExecutionId.asU128` | plain-value |
| `QueryExecutionId::from_bytes` | omitted | `wire.QueryExecutionId.fromBytes` | plain-value |
| `QueryExecutionId::from_u128` | omitted | `wire.QueryExecutionId.fromU128` | plain-value |
| `QueryExecutionId::to_bytes` | omitted | `wire.QueryExecutionId.toBytes` | plain-value |
| `QueryExecutionState` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Queued` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Planning` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Running` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Completed` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Cancelled` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Failed` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionState::Expired` | omitted | `wire.QueryExecutionState` | plain-value |
| `QueryExecutionStatus` | dict via `Laser.query_status` | `QueryExecutionStatus` | serde-dict |
| `QueryExecutionStatus.execution_id` | dict key | `QueryExecutionStatus.executionId` | serde-dict, keywords |
| `QueryExecutionStatus.state` | dict key | `QueryExecutionStatus.state` | serde-dict, keywords |
| `QueryExecutionStatus.started_at_micros` | dict key | `QueryExecutionStatus.startedAtMicros` | serde-dict, keywords |
| `QueryExecutionStatus.finished_at_micros` | dict key | `QueryExecutionStatus.finishedAtMicros` | serde-dict, keywords |
| `QueryExecutionStatus.scanned_bytes` | dict key | `QueryExecutionStatus.scannedBytes` | serde-dict, keywords |
| `QueryExecutionStatus.produced_bytes` | dict key | `QueryExecutionStatus.producedBytes` | serde-dict, keywords |
| `QueryExecutionStatus.row_count` | dict key | `QueryExecutionStatus.rowCount` | serde-dict, keywords |
| `QueryExecutionStatus.error` | dict key | `QueryExecutionStatus.error` | serde-dict, keywords |
| `QueryPageRequest` | dict via `Laser.execute_query` | `wire.QueryPageRequest` | serde-dict |
| `QueryPageRequest.limit` | dict key | `wire.QueryPageRequest.limit` | serde-dict, keywords |
| `QueryPageRequest.offset` | dict key | `wire.QueryPageRequest.offset` | serde-dict, keywords |
| `QueryPageRequest.cursor` | dict key | `wire.QueryPageRequest.cursor` | serde-dict, keywords |
| `QueryPageRequest.want_total` | dict key | `wire.QueryPageRequest.wantTotal` | serde-dict, keywords |
| `QueryRequest` | `QueryRequest` | `QueryRequest` |  |
| `QueryRequest::agg_as` | `QueryRequest.agg_as` | `QueryRequest.aggAs` |  |
| `QueryRequest::at_snapshot` | `QueryRequest.at_snapshot` | `QueryRequest.atSnapshot` |  |
| `QueryRequest::at_timestamp_micros` | `QueryRequest.at_timestamp_micros` | `QueryRequest.atTimestampMicros` |  |
| `QueryRequest::avg` | `QueryRequest.avg` | `QueryRequest.avg` |  |
| `QueryRequest::cancel` | `QueryRequest.cancel` | `QueryRequest.cancel` |  |
| `QueryRequest::consistency` | `QueryRequest.consistency` | `QueryRequest.consistency` |  |
| `QueryRequest::conversation` | `QueryRequest.conversation` | `QueryRequest.conversation` |  |
| `QueryRequest::count` | `QueryRequest.count` | `QueryRequest.count` |  |
| `QueryRequest::count_distinct` | `QueryRequest.count_distinct` | `QueryRequest.countDistinct` |  |
| `QueryRequest::cursor` | `QueryRequest.cursor` | `QueryRequest.cursor` |  |
| `QueryRequest::deadline` | `QueryRequest.deadline` | `QueryRequest.deadline` |  |
| `QueryRequest::deadline_micros` | `QueryRequest.deadline_micros` | `QueryRequest.deadlineMicros` |  |
| `QueryRequest::distinct` | `QueryRequest.distinct` | `QueryRequest.distinct` |  |
| `QueryRequest::execution_id` | `QueryRequest.execution_id` | `QueryRequest.executionId` | property |
| `QueryRequest::fetch` | `QueryRequest.fetch` | `QueryRequest.fetch` |  |
| `QueryRequest::fetch_all` | `QueryRequest.fetch_all` | `QueryRequest.fetchAll` |  |
| `QueryRequest::fetch_all_typed` | `QueryRequest.fetch_all_typed` | `QueryRequest.fetchAllTyped` |  |
| `QueryRequest::fetch_one` | `QueryRequest.fetch_one` | `QueryRequest.fetchOne` |  |
| `QueryRequest::fetch_one_with` | `QueryRequest.fetch_one_with` | `QueryRequest.fetchOne` | overload |
| `QueryRequest::fetch_typed` | `QueryRequest.fetch_typed` | `QueryRequest.fetchTyped` |  |
| `QueryRequest::fetch_typed_with` | `QueryRequest.fetch_typed_with` | `QueryRequest.fetchTyped` | overload |
| `QueryRequest::filter` | `QueryRequest.filter` | `QueryRequest.filter` |  |
| `QueryRequest::filter_contains` | `QueryRequest.filter_contains` | `QueryRequest.filterContains` |  |
| `QueryRequest::filter_eq` | `QueryRequest.filter_eq` | `QueryRequest.filterEq` |  |
| `QueryRequest::filter_gt` | `QueryRequest.filter_gt` | `QueryRequest.filterGt` |  |
| `QueryRequest::filter_gte` | `QueryRequest.filter_gte` | `QueryRequest.filterGte` |  |
| `QueryRequest::filter_in` | `QueryRequest.filter_in` | `QueryRequest.filterIn` |  |
| `QueryRequest::filter_lt` | `QueryRequest.filter_lt` | `QueryRequest.filterLt` |  |
| `QueryRequest::filter_lte` | `QueryRequest.filter_lte` | `QueryRequest.filterLte` |  |
| `QueryRequest::filter_ne` | `QueryRequest.filter_ne` | `QueryRequest.filterNe` |  |
| `QueryRequest::filter_prefix` | `QueryRequest.filter_prefix` | `QueryRequest.filterPrefix` |  |
| `QueryRequest::fork` | `QueryRequest.fork` | `QueryRequest.fork` |  |
| `QueryRequest::group_by` | `QueryRequest.group_by` | `QueryRequest.groupBy` |  |
| `QueryRequest::having` | `QueryRequest.having` | `QueryRequest.having` |  |
| `QueryRequest::into_query` | `QueryRequest.into_query` | `QueryRequest.intoQuery` |  |
| `QueryRequest::limit` | `QueryRequest.limit` | `QueryRequest.limit` |  |
| `QueryRequest::max` | `QueryRequest.max` | `QueryRequest.max` |  |
| `QueryRequest::max_rows` | `QueryRequest.max_rows` | `QueryRequest.maxRows` |  |
| `QueryRequest::message_type` | `QueryRequest.message_type` | `QueryRequest.messageType` |  |
| `QueryRequest::min` | `QueryRequest.min` | `QueryRequest.min` |  |
| `QueryRequest::nearest` | `QueryRequest.nearest` | `QueryRequest.nearest` |  |
| `QueryRequest::nearest_in` | `QueryRequest.nearest(field=)` | `QueryRequest.nearestIn` | overload |
| `QueryRequest::offset` | `QueryRequest.offset` | `QueryRequest.offset` |  |
| `QueryRequest::order_asc` | `QueryRequest.order_asc` | `QueryRequest.orderAsc` |  |
| `QueryRequest::order_desc` | `QueryRequest.order_desc` | `QueryRequest.orderDesc` |  |
| `QueryRequest::percentile` | `QueryRequest.percentile` | `QueryRequest.percentile` |  |
| `QueryRequest::raw_sql` | `QueryRequest.raw_sql` | `QueryRequest.rawSql` |  |
| `QueryRequest::raw_sql_with` | `QueryRequest.raw_sql(dialect=)` | `QueryRequest.rawSql` | overload |
| `QueryRequest::read_your_writes` | `QueryRequest.read_your_writes` | `QueryRequest.readYourWrites` |  |
| `QueryRequest::rows` | `QueryRequest.rows` | `QueryRequest.rows` |  |
| `QueryRequest::rows_typed` | `QueryRequest.rows_typed` | `QueryRequest.rowsTyped` |  |
| `QueryRequest::select_fields` | `QueryRequest.select_fields` | `QueryRequest.selectFields` |  |
| `QueryRequest::status` | `QueryRequest.status` | `QueryRequest.status` |  |
| `QueryRequest::stddev` | `QueryRequest.stddev` | `QueryRequest.stddev` |  |
| `QueryRequest::sum` | `QueryRequest.sum` | `QueryRequest.sum` |  |
| `QueryRequest::text` | `QueryRequest.text` | `QueryRequest.text` |  |
| `QueryRequest::text_in` | `QueryRequest.text_in` | `QueryRequest.textIn` |  |
| `QueryRequest::time_range` | `QueryRequest.time_range` | `QueryRequest.timeRange` |  |
| `QueryRequest::where_eq` | `QueryRequest.where_eq` | `QueryRequest.whereEq` |  |
| `QueryRequest::window` | `QueryRequest.window` | `QueryRequest.window` |  |
| `QueryRequest::with_payload` | `QueryRequest.with_payload` | `QueryRequest.withPayload` |  |
| `QueryRequest::with_total` | `QueryRequest.with_total` | `QueryRequest.withTotal` |  |
| `QueryResult` | `QueryResult` | `QueryResult` |  |
| `QueryResult.fields` | `QueryResult.fields` | `QueryResult.fields` | keywords |
| `QueryResult.rows` | `QueryResult.rows` | `QueryResult.rows` | keywords |
| `QueryResult.page` | `QueryResult.page` | `QueryResult.page` | keywords |
| `QueryResult.context` | `QueryResult.context` | `QueryResult.context` | keywords |
| `QueryResult::field_index` | `QueryResult.field_index` | `fn:queryResultFieldIndex` | free-function |
| `QueryResult::value` | `QueryResult.value` | `fn:queryResultValue` | free-function |
| `QueryResult::value_i64` | `QueryResult.value_i64` | `fn:queryResultValueI64` | free-function |
| `QueryResult::value_text` | `QueryResult.value_text` | `fn:queryResultValueText` | free-function |
| `QueryResult::value_u64` | `QueryResult.value_u64` | `fn:queryResultValueU64` | free-function |
| `QueryRows` | `QueryRows` | `QueryRequest.rows` | protocol |
| `QueryRows::next` | `QueryRows.next` | `QueryRequest.rows` | protocol |
| `QueryTarget` | dict via `Laser.execute_query` | `QueryTarget` | serde-dict |
| `QueryTarget::Operational` | dict key | `QueryTarget` | serde-dict, plain-value |
| `QueryTarget::Lakehouse` | dict key | `QueryTarget` | serde-dict, plain-value |
| `QueryTarget::operational` | `fn:query_target_operational` | `fn:operationalTarget` | free-function |
| `RawSql` | dict via `Laser.execute_query` | `wire.RawSql` | serde-dict |
| `RawSql.dialect` | dict key | `wire.RawSql.dialect` | serde-dict, keywords |
| `RawSql.sql` | dict key | `wire.RawSql.sql` | serde-dict, keywords |
| `RawSql.params` | dict key | `wire.RawSql.params` | serde-dict, keywords |
| `ResolvedQueryTarget` | dict via `QueryResult.context` | `wire.ResolvedQueryTarget` | serde-dict |
| `ResolvedQueryTarget::Operational` | dict key | `wire.ResolvedQueryTarget` | serde-dict, plain-value |
| `ResolvedQueryTarget::Lakehouse` | dict key | `wire.ResolvedQueryTarget` | serde-dict, plain-value |
| `ResultCode` | omitted | `ResultCode` | plain-value |
| `ResultCode::Ok` | omitted | `ResultCode` | plain-value |
| `ResultCode::Unsupported` | omitted | `ResultCode` | plain-value |
| `ResultCode::NotFound` | omitted | `ResultCode` | plain-value |
| `ResultCode::InvalidArgument` | omitted | `ResultCode` | plain-value |
| `ResultCode::TooLarge` | omitted | `ResultCode` | plain-value |
| `ResultCode::Conflict` | omitted | `ResultCode` | plain-value |
| `ResultCode::Stale` | omitted | `ResultCode` | plain-value |
| `ResultCode::VersionSkew` | omitted | `ResultCode` | plain-value |
| `ResultCode::Unauthenticated` | omitted | `ResultCode` | plain-value |
| `ResultCode::Backend` | omitted | `ResultCode` | plain-value |
| `ResultCode::Forbidden` | omitted | `ResultCode` | plain-value |
| `ResultCode::StepUpRequired` | omitted | `ResultCode` | plain-value |
| `ResultCode::Unavailable` | omitted | `ResultCode` | plain-value |
| `ResultCode::ResourceLimit` | omitted | `ResultCode` | plain-value |
| `ResultCode::Cancelled` | omitted | `ResultCode` | plain-value |
| `ResultCode::DeadlineExceeded` | omitted | `ResultCode` | plain-value |
| `ResultCode::ExpiredSnapshot` | omitted | `ResultCode` | plain-value |
| `ResultCode::StaleGeneration` | omitted | `ResultCode` | plain-value |
| `ResultCode::TargetUnavailable` | omitted | `ResultCode` | plain-value |
| `ResultCode::Unrecognized` | omitted | `ResultCode` | plain-value |
| `ResultCode::code` | `fn:result_code_code` | `fn:code` | free-function |
| `ResultCode::from_code` | `fn:result_code_from_code` | `fn:resultCodeFromCode` | free-function |
| `ResultCode::http_status` | `fn:result_code_http_status` | `fn:resultCodeHttpStatus` | free-function |
| `ResultCode::is_retryable` | `fn:result_code_is_retryable` | `fn:isRetryable` | free-function |
| `RetentionPolicy` | dict via `Bindings.apply` | `RetentionPolicy` | serde-dict |
| `RetentionPolicy::MirrorLog` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `RetentionPolicy::Keep` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `RetentionPolicy::KeepUntilSourceDeleted` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `RetentionPolicy::TimeToLive` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `RetentionPolicy::MaxRows` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `RetentionPolicy::Unknown` | dict key | `RetentionPolicy` | serde-dict, plain-value |
| `Row` | `Row` | `Row` |  |
| `Row.values` | `Row.values` | `Row.values` | keywords |
| `Row.score` | `Row.score` | `Row.score` | keywords |
| `query::SCHEMA_ID` | `const:SCHEMA_ID` | `const:SCHEMA_ID` |  |
| `SchemaDef` | dict via `Schemas.get` | `SchemaDef` | serde-dict |
| `SchemaDef.id` | dict key | `SchemaDef.id` | serde-dict, keywords |
| `SchemaDef.source` | dict key | `SchemaDef.source` | serde-dict, keywords |
| `SchemaDef.name` | dict key | `SchemaDef.name` | serde-dict, keywords |
| `SchemaDef.version` | dict key | `SchemaDef.version` | serde-dict, keywords |
| `SchemaDef::content_type` | `fn:schema_def_content_type` | `fn:schemaDefContentType` | free-function |
| `SchemaInfo` | dict via `Schemas.get` | `SchemaInfo` | serde-dict |
| `SchemaInfo.schema` | dict key | `SchemaInfo.schema` | serde-dict, keywords |
| `SchemaInfo.dropped` | dict key | `SchemaInfo.dropped` | serde-dict, keywords |
| `SchemaSource` | dict via `Schemas.register` | `SchemaSource` | serde-dict |
| `SchemaSource::Avro` | dict key | `SchemaSource` | serde-dict, plain-value |
| `SchemaSource::Protobuf` | dict key | `SchemaSource` | serde-dict, plain-value |
| `SchemaSource::JsonSchema` | dict key | `SchemaSource` | serde-dict, plain-value |
| `SchemaSource::Unknown` | dict key | `SchemaSource` | serde-dict, plain-value |
| `Select` | dict via `Laser.execute_query` | `wire.Select` | serde-dict |
| `Select.fields` | dict key | `wire.Select.fields` | serde-dict, keywords |
| `Select.payload` | dict key | `wire.Select.payload` | serde-dict, keywords |
| `SnapshotSelector` | dict via `Laser.execute_query` | `SnapshotSelector` | serde-dict |
| `SnapshotSelector::SnapshotId` | dict key | `SnapshotSelector` | serde-dict, plain-value |
| `SnapshotSelector::TimestampMicros` | dict key | `SnapshotSelector` | serde-dict, plain-value |
| `Sort` | dict via `Laser.execute_query` | `wire.Sort` | serde-dict |
| `Sort.field` | dict key | `wire.Sort.field` | serde-dict, keywords |
| `Sort.dir` | dict key | `wire.Sort.dir` | serde-dict, keywords |
| `SourceSelector` | dict via `Bindings.remove` | `SourceSelector` | serde-dict |
| `SourceSelector.stream` | dict key | `SourceSelector.stream` | serde-dict, keywords |
| `SourceSelector.topic` | dict key | `SourceSelector.topic` | serde-dict, keywords |
| `SourceSelector::new` | `fn:source_selector_new` | `SourceSelector` | free-function, keywords |
| `SqlDialect` | omitted | `SqlDialect` | plain-value |
| `SqlDialect::DataFusion` | omitted | `SqlDialect` | plain-value |
| `SqlDialect::Postgres` | omitted | `SqlDialect` | plain-value |
| `SqlDialect::MySql` | omitted | `SqlDialect` | plain-value |
| `SqlDialect::Sqlite` | omitted | `SqlDialect` | plain-value |
| `TextQuery` | dict via `Laser.execute_query` | `wire.TextQuery` | serde-dict |
| `TextQuery.field` | dict key | `wire.TextQuery.field` | serde-dict, keywords |
| `TextQuery.query` | dict key | `wire.TextQuery.query` | serde-dict, keywords |
| `TypedQueryRows` | `TypedQueryRows` | `QueryRequest.rowsTyped` | protocol |
| `TypedQueryRows::next` | `TypedQueryRows.next` | `QueryRequest.rowsTyped` | protocol |
| `TypedValue` | dict via `Row.values` | `TypedValue` | serde-dict |
| `TypedValue::Null` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Boolean` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Int` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Long` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Float` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Double` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Decimal` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Date` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::TimeMicros` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::TimestampMicros` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::TimestampTzMicros` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::String` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Uuid` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Fixed` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Binary` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Struct` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::List` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::Map` | dict key | `TypedValue` | serde-dict, plain-value |
| `TypedValue::as_i64` | `fn:typed_value_as_i64` | `fn:typedValueAsI64` | free-function |
| `TypedValue::as_str` | `fn:typed_value_as_str` | `fn:typedValueAsStr` | free-function |
| `TypedValue::as_u64` | `fn:typed_value_as_u64` | `fn:typedValueAsU64` | free-function |
| `TypedValue::diagnostic_text` | `fn:typed_value_diagnostic_text` | `fn:typedValueDiagnosticText` | free-function |
| `TypedValue::validate_against` | `fn:typed_value_validate_against` | `fn:typedValueValidateAgainst` | free-function |
| `TypedValue::validate_canonical` | `fn:typed_value_validate_canonical` | `fn:typedValueValidateCanonical` | free-function |
| `query::VECTOR_FIELD` | `const:VECTOR_FIELD` | `const:VECTOR_FIELD` |  |
| `Value` | omitted | `Value` | native-value |
| `Value::Str` | omitted | `Value` | native-value, plain-value |
| `Value::Int` | omitted | `Value` | native-value, plain-value |
| `Value::Uint` | omitted | `Value` | native-value, plain-value |
| `Value::Float` | omitted | `Value` | native-value, plain-value |
| `Value::Bool` | omitted | `Value` | native-value, plain-value |
| `Value::Null` | omitted | `Value` | native-value, plain-value |
| `Value::List` | omitted | `Value` | native-value, plain-value |
| `Value::from_input` | omitted | `fn:valueFromInput` | native-value, free-function |
| `VectorQuery` | dict via `Laser.execute_query` | `wire.VectorQuery` | serde-dict |
| `VectorQuery.field` | dict key | `wire.VectorQuery.field` | serde-dict, keywords |
| `VectorQuery.embedding` | dict key | `wire.VectorQuery.embedding` | serde-dict, keywords |
| `VectorQuery.top_k` | dict key | `wire.VectorQuery.topK` | serde-dict, keywords |
| `query::WINDOW_START` | `const:WINDOW_START` | `const:WINDOW_START` |  |
| `Window` | dict via `Laser.execute_query` | `wire.Window` | serde-dict |
| `Window.field` | dict key | `wire.Window.field` | serde-dict, keywords |
| `Window.every_micros` | dict key | `wire.Window.everyMicros` | serde-dict, keywords |

## rbac

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `rbac::AGDX_AUTHZ_BIND_ROLES_CODE` | `const:AGDX_AUTHZ_BIND_ROLES_CODE` | `const:AGDX_AUTHZ_BIND_ROLES_CODE` |  |
| `rbac::AGDX_AUTHZ_DEFINE_ROLE_CODE` | `const:AGDX_AUTHZ_DEFINE_ROLE_CODE` | `const:AGDX_AUTHZ_DEFINE_ROLE_CODE` |  |
| `rbac::AGDX_AUTHZ_DELETE_ROLE_CODE` | `const:AGDX_AUTHZ_DELETE_ROLE_CODE` | `const:AGDX_AUTHZ_DELETE_ROLE_CODE` |  |
| `rbac::AGDX_AUTHZ_GET_BINDINGS_CODE` | `const:AGDX_AUTHZ_GET_BINDINGS_CODE` | `const:AGDX_AUTHZ_GET_BINDINGS_CODE` |  |
| `rbac::AGDX_AUTHZ_GET_ROLE_CODE` | `const:AGDX_AUTHZ_GET_ROLE_CODE` | `const:AGDX_AUTHZ_GET_ROLE_CODE` |  |
| `rbac::AGDX_AUTHZ_HISTORY_CODE` | `const:AGDX_AUTHZ_HISTORY_CODE` | `const:AGDX_AUTHZ_HISTORY_CODE` |  |
| `rbac::AGDX_AUTHZ_LIST_ROLES_CODE` | `const:AGDX_AUTHZ_LIST_ROLES_CODE` | `const:AGDX_AUTHZ_LIST_ROLES_CODE` |  |
| `rbac::AGDX_AUTHZ_WHOAMI_CODE` | `const:AGDX_AUTHZ_WHOAMI_CODE` | `const:AGDX_AUTHZ_WHOAMI_CODE` |  |
| `rbac::AUTHZ_OP_VERSION` | `const:AUTHZ_OP_VERSION` | `const:AUTHZ_OP_VERSION` |  |
| `Action` | omitted | `Action` | plain-value |
| `Action::Read` | omitted | `Action` | plain-value |
| `Action::Write` | omitted | `Action` | plain-value |
| `Action::Delete` | omitted | `Action` | plain-value |
| `Action::Admin` | omitted | `Action` | plain-value |
| `Action::Unrecognized` | omitted | `Action` | plain-value |
| `AuthzError` | `AuthzError` | `AuthzError` |  |
| `AuthzError::Unsupported` | `AuthzError.unsupported` | `AuthzError` | plain-value |
| `AuthzError::Unauthorized` | dict key | `AuthzError` | serde-dict, plain-value |
| `AuthzError::UnknownRole` | dict key | `AuthzError` | serde-dict, plain-value |
| `AuthzError::InvalidName` | dict key | `AuthzError` | serde-dict, plain-value |
| `AuthzError::Conflict` | dict key | `AuthzError` | serde-dict, plain-value |
| `AuthzError::Version` | dict key | `AuthzError` | serde-dict, plain-value |
| `AuthzEvent` | `AuthzEvent` | `AuthzEvent` |  |
| `AuthzEvent.revision` | `AuthzEvent.revision` | `AuthzEvent.revision` | keywords |
| `AuthzEvent.actor` | `AuthzEvent.actor` | `AuthzEvent.actor` | keywords |
| `AuthzEvent.at_micros` | `AuthzEvent.at_micros` | `AuthzEvent.atMicros` | keywords |
| `AuthzEvent.op` | `AuthzEvent.op` | `AuthzEvent.op` | keywords |
| `AuthzEventKind` | dict via `AuthzEvent.op` | `AuthzEventKind` | serde-dict |
| `AuthzEventKind::RoleDefined` | dict key | `AuthzEventKind` | serde-dict, plain-value |
| `AuthzEventKind::RoleDeleted` | dict key | `AuthzEventKind` | serde-dict, plain-value |
| `AuthzEventKind::RolesBound` | dict key | `AuthzEventKind` | serde-dict, plain-value |
| `AuthzHistoryReply` | `AuthzHistoryReply` | `AuthzHistoryReply` |  |
| `AuthzHistoryReply.v` | `AuthzHistoryReply.v` | `AuthzHistoryReply.v` | keywords |
| `AuthzHistoryReply.events` | `AuthzHistoryReply.events` | `AuthzHistoryReply.events` | keywords |
| `AuthzHistoryReply.next_after_revision` | `AuthzHistoryReply.next_after_revision` | `AuthzHistoryReply.nextAfterRevision` | keywords |
| `AuthzSubject` | dict via `Laser.authz_history` | `AuthzSubject` | serde-dict |
| `AuthzSubject::Role` | dict key | `AuthzSubject` | serde-dict, plain-value |
| `AuthzSubject::Binding` | dict key | `AuthzSubject` | serde-dict, plain-value |
| `AuthzSubject::All` | dict key | `AuthzSubject` | serde-dict, plain-value |
| `Effect` | omitted | `Effect` | plain-value |
| `Effect::Allow` | omitted | `Effect` | plain-value |
| `Effect::Deny` | omitted | `Effect` | plain-value |
| `Feature` | omitted | `wire.Feature` | plain-value |
| `Feature::Kv` | omitted | `wire.Feature` | plain-value |
| `Feature::Memory` | omitted | `wire.Feature` | plain-value |
| `Feature::Projection` | omitted | `wire.Feature` | plain-value |
| `Feature::Fork` | omitted | `wire.Feature` | plain-value |
| `Feature::Graph` | omitted | `wire.Feature` | plain-value |
| `Feature::Query` | omitted | `wire.Feature` | plain-value |
| `Feature::Agent` | omitted | `wire.Feature` | plain-value |
| `Feature::Workflow` | omitted | `wire.Feature` | plain-value |
| `Feature::Destination` | omitted | `wire.Feature` | plain-value |
| `Feature::Checkpoint` | omitted | `wire.Feature` | plain-value |
| `Feature::Authz` | omitted | `wire.Feature` | plain-value |
| `Feature::KvLease` | omitted | `wire.Feature` | plain-value |
| `Feature::KvFence` | omitted | `wire.Feature` | plain-value |
| `Feature::Filter` | omitted | `wire.Feature` | plain-value |
| `Feature::Unrecognized` | omitted | `wire.Feature` | plain-value |
| `Grant` | `Grant` | `Grant` |  |
| `Grant.effect` | `Grant.effect` | `Grant.effect` | keywords |
| `Grant.feature` | `Grant.feature` | `Grant.feature` | keywords |
| `Grant.action` | `Grant.action` | `Grant.action` | keywords |
| `Grant.resource` | `Grant.resource` | `Grant.resource` | keywords |
| `InvalidError` | `InvalidError` | `InvalidError` |  |
| `InvalidError::new` | `new InvalidError()` | `new InvalidError()` | constructor |
| `rbac::MAX_ROLE_NAME_BYTES` | `const:MAX_ROLE_NAME_BYTES` | `const:MAX_ROLE_NAME_BYTES` |  |
| `ResourceKind` | omitted | `ResourceKind` | plain-value |
| `ResourceKind::All` | omitted | `ResourceKind` | plain-value |
| `ResourceKind::Literal` | omitted | `ResourceKind` | plain-value |
| `ResourceKind::Prefix` | omitted | `ResourceKind` | plain-value |
| `ResourcePattern` | `ResourcePattern` | `ResourcePattern` |  |
| `ResourcePattern.kind` | `ResourcePattern.kind` | `ResourcePattern.kind` | keywords |
| `ResourcePattern.value` | `ResourcePattern.value` | `ResourcePattern.value` | keywords |
| `ResourcePattern::all` | `ResourcePattern.all` | `fn:resourcePatternAll` | free-function |
| `ResourcePattern::literal` | `ResourcePattern.literal` | `fn:resourcePatternLiteral` | free-function |
| `ResourcePattern::matches` | `ResourcePattern.matches` | `fn:resourcePatternMatches` | free-function |
| `ResourcePattern::prefix` | `ResourcePattern.prefix` | `fn:resourcePatternPrefix` | free-function |
| `Role` | `Role` | `Role` |  |
| `Role.name` | `Role.name` | `Role.name` | keywords |
| `Role.grants` | `Role.grants` | `Role.grants` | keywords |
| `WhoamiReply` | `WhoamiReply` | `WhoamiReply` |  |
| `WhoamiReply.v` | `WhoamiReply.v` | `WhoamiReply.v` | keywords |
| `WhoamiReply.roles` | `WhoamiReply.roles` | `WhoamiReply.roles` | keywords |
| `WhoamiReply.grants` | `WhoamiReply.grants` | `WhoamiReply.grants` | keywords |
| `rbac::delegated_allow` | `fn:delegated_allow` | `fn:delegatedAllow` |  |
| `rbac::grants_allow` | `fn:grants_allow` | `fn:grantsAllow` |  |
| `rbac::validate_role_name` | `fn:validate_role_name` | `fn:validateRoleName` |  |

## runs

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `RunListRequest` | `Runs.list` | `RunListRequest` | keywords |
| `RunListRequest::agent` | `Runs.list(agent=)` | `RunListRequest.agent` | keywords |
| `RunListRequest::cursor` | `Runs.list(cursor=)` | `RunListRequest.cursor` | keywords |
| `RunListRequest::fetch` | `Runs.list` | `RunListRequest.fetch` | one-call |
| `RunListRequest::limit` | `Runs.list(limit=)` | `RunListRequest.limit` | keywords |
| `RunListRequest::state` | `Runs.list(state=)` | `RunListRequest.state` | keywords |
| `Runs` | `Runs` | `Runs` |  |
| `Runs::cancel` | `Runs.cancel` | `Runs.cancel` |  |
| `Runs::list` | `Runs.list` | `Runs.list` |  |
| `Runs::register_source` | `Runs.register_source` | `Runs.registerSource` |  |
| `Runs::remove_source` | `Runs.remove_source` | `Runs.removeSource` |  |
| `Runs::status` | `Runs.status` | `Runs.status` |  |
| `Runs::submit` | `Runs.submit` | `Runs.submit` |  |
| `Runs::submit_budgeted` | `Runs.submit_budgeted` | `Runs.submitBudgeted` |  |
| `Runs::submit_with` | `Runs.submit_with` | `Runs.submitWith` |  |

## schema_codecs

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `CompiledSchema` | `CompiledSchema` | `CompiledSchema` |  |
| `CompiledSchema::Avro` | `CompiledSchema.AVRO` | `CompiledSchemaKind` | plain-value |
| `CompiledSchema::Protobuf` | `CompiledSchema.PROTOBUF` | `CompiledSchemaKind` | plain-value |
| `CompiledSchema::Json` | `CompiledSchema.JSON` | `CompiledSchemaKind` | plain-value |
| `CompiledSchema::compile` | `CompiledSchema.compile` | `CompiledSchema.compile` |  |
| `CompiledSchema::decode` | `CompiledSchema.decode` | `CompiledSchema.decode` |  |
| `CompiledSchema::encode_avro` | `CompiledSchema.encode_avro` | `CompiledSchema.encodeAvro` |  |
| `CompiledSchema::validate` | `CompiledSchema.validate` | `CompiledSchema.validate` |  |
| `CompiledSchema::validate_value` | `CompiledSchema.validate_value` | `CompiledSchema.validateValue` |  |

## sign

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentEnvelope` | dict via `AgentMessage.envelope` | `AgentEnvelope` | serde-dict |
| `AgentEnvelope.kind` | dict key | `AgentEnvelope.kind` | serde-dict, keywords |
| `AgentEnvelope.record` | dict key | `AgentEnvelope.record` | serde-dict, keywords |
| `AgentEnvelope.conversation` | dict key | `AgentEnvelope.conversation` | serde-dict, keywords |
| `AgentEnvelope.source` | dict key | `AgentEnvelope.source` | serde-dict, keywords |
| `AgentEnvelope.target` | dict key | `AgentEnvelope.target` | serde-dict, keywords |
| `AgentEnvelope.cause` | dict key | `AgentEnvelope.cause` | serde-dict, keywords |
| `AgentEnvelope.cause_at` | dict key | `AgentEnvelope.causeAt` | serde-dict, keywords |
| `AgentEnvelope.correlation` | dict key | `AgentEnvelope.correlation` | serde-dict, keywords |
| `AgentEnvelope.channel` | dict key | `AgentEnvelope.channel` | serde-dict, keywords |
| `AgentEnvelope.idempotency_key` | dict key | `AgentEnvelope.idempotencyKey` | serde-dict, keywords |
| `AgentEnvelope.deadline_micros` | dict key | `AgentEnvelope.deadlineMicros` | serde-dict, keywords |
| `AgentEnvelope.sequence` | dict key | `AgentEnvelope.sequence` | serde-dict, keywords |
| `AgentEnvelope.last` | dict key | `AgentEnvelope.last` | serde-dict, keywords |
| `AgentEnvelope.finish_reason` | dict key | `AgentEnvelope.finishReason` | serde-dict, keywords |
| `AgentEnvelope.task_state` | dict key | `AgentEnvelope.taskState` | serde-dict, keywords |
| `AgentEnvelope.operation` | dict key | `AgentEnvelope.operation` | serde-dict, keywords |
| `AgentEnvelope.tool` | dict key | `AgentEnvelope.tool` | serde-dict, keywords |
| `AgentEnvelope.usage` | dict key | `AgentEnvelope.usage` | serde-dict, keywords |
| `AgentEnvelope.metadata` | dict key | `AgentEnvelope.metadata` | serde-dict, keywords |
| `AgentEnvelope.must_understand` | dict key | `AgentEnvelope.mustUnderstand` | serde-dict, keywords |
| `AgentEnvelope.body` | dict key | `AgentEnvelope.body` | serde-dict, keywords |
| `AgentEnvelope.signature` | dict key | `AgentEnvelope.signature` | serde-dict, keywords |
| `AgentEnvelope::chunk` | `fn:chunk_envelope` | `fn:chunkEnvelope` | free-function |
| `AgentEnvelope::command` | `fn:command_envelope` | `fn:commandEnvelope` | free-function |
| `AgentEnvelope::error` | `fn:error_envelope` | `fn:errorEnvelope` | free-function |
| `AgentEnvelope::event` | `fn:event_envelope` | `fn:eventEnvelope` | free-function |
| `AgentEnvelope::requiring` | `fn:event_envelope(requiring=)` | `fn:requiring` | keywords, free-function |
| `AgentEnvelope::response` | `fn:response_envelope` | `fn:responseEnvelope` | free-function |
| `AgentEnvelope::status` | `fn:status_envelope` | `fn:statusEnvelope` | free-function |
| `AgentEnvelope::terminal` | `fn:event_envelope(terminal=)` | `fn:terminal` | keywords, free-function |
| `AgentEnvelope::unmet_requirements` | `fn:unmet_requirements` | `fn:unmetRequirements` | free-function |
| `AgentEnvelope::with_cause` | `fn:event_envelope(cause=, cause_at=)` | `AgentEnvelope.cause` | keywords |
| `AgentEnvelope::with_correlation` | `fn:event_envelope(correlation=)` | `AgentEnvelope.correlation` | keywords |
| `AgentEnvelope::with_deadline_micros` | `fn:event_envelope(deadline_micros=)` | `AgentEnvelope.deadlineMicros` | keywords |
| `AgentEnvelope::with_idempotency_key` | `fn:event_envelope(idempotency_key=)` | `AgentEnvelope.idempotencyKey` | keywords |
| `AgentEnvelope::with_metadata` | `fn:event_envelope(metadata=)` | `AgentEnvelope.metadata` | keywords |
| `AgentEnvelope::with_operation` | `fn:event_envelope(operation=)` | `AgentEnvelope.operation` | keywords |
| `AgentEnvelope::with_signature` | `fn:event_envelope(signature=)` | `AgentEnvelope.signature` | keywords |
| `AgentEnvelope::with_target` | `fn:event_envelope(target=)` | `AgentEnvelope.target` | keywords |
| `AgentEnvelope::with_task_state` | `fn:event_envelope(task_state=)` | `AgentEnvelope.taskState` | keywords |
| `AgentEnvelope::with_tool` | `fn:event_envelope(tool=)` | `AgentEnvelope.tool` | keywords |
| `AgentEnvelope::with_usage` | `fn:event_envelope(usage=)` | `AgentEnvelope.usage` | keywords |
| `sign::AgentId` | omitted | `AgentId` | plain-value |
| `sign::AgentId::as_str` | omitted | `AgentId.asStr` | plain-value |
| `AgentKind` | omitted | `wire.AgentKind` | plain-value |
| `AgentKind::Command` | omitted | `wire.AgentKind.Command` | plain-value |
| `AgentKind::Response` | omitted | `wire.AgentKind.Response` | plain-value |
| `AgentKind::Event` | omitted | `wire.AgentKind.Event` | plain-value |
| `AgentKind::Chunk` | omitted | `wire.AgentKind.Chunk` | plain-value |
| `AgentKind::Status` | omitted | `wire.AgentKind.Status` | plain-value |
| `AgentKind::Error` | omitted | `wire.AgentKind.Error` | plain-value |
| `ChannelId` | omitted | `ChannelId` | plain-value |
| `ChannelId::as_u128` | omitted | `ChannelId.asU128` | plain-value |
| `ChannelId::from_bytes` | omitted | `ChannelId.fromBytes` | plain-value |
| `ChannelId::from_u128` | omitted | `ChannelId.fromU128` | plain-value |
| `ChannelId::to_bytes` | omitted | `ChannelId.toBytes` | plain-value |
| `CorrelationId` | omitted | `CorrelationId` | plain-value |
| `CorrelationId::as_u128` | omitted | `CorrelationId.asU128` | plain-value |
| `CorrelationId::from_bytes` | omitted | `CorrelationId.fromBytes` | plain-value |
| `CorrelationId::from_u128` | omitted | `CorrelationId.fromU128` | plain-value |
| `CorrelationId::to_bytes` | omitted | `CorrelationId.toBytes` | plain-value |
| `sign::DEFAULT_KEY_NAMESPACE` | `const:DEFAULT_KEY_NAMESPACE` | `const:DEFAULT_KEY_NAMESPACE` |  |
| `IdempotencyKey` | omitted | `IdempotencyKey` | plain-value |
| `IdempotencyKey::as_str` | omitted | omitted | plain-value |
| `KeyKind` | omitted | `KeyKind` | plain-value |
| `KeyKind::Agent` | omitted | `KeyKind.Agent` | plain-value |
| `KeyKind::Operator` | omitted | `KeyKind.Operator` | plain-value |
| `KeyRecord` | `KeyRecord` | `KeyRecord` |  |
| `KeyRecord.principal` | `KeyRecord.principal` | `KeyRecord.principal` |  |
| `KeyRecord.verifying` | `KeyRecord.verifying` | `KeyRecord.verifying` |  |
| `KeyRecord.kind` | `KeyRecord.kind` | `KeyRecord.kind` |  |
| `KeyRecord.valid_from_micros` | `KeyRecord.valid_from_micros` | `KeyRecord.validFromMicros` |  |
| `KeyRecord.valid_to_micros` | `KeyRecord.valid_to_micros` | `KeyRecord.validToMicros` |  |
| `KeyRecord.revoked` | `KeyRecord.revoked` | `KeyRecord.revoked` |  |
| `KeyRecord::agent` | `KeyRecord.agent` | `KeyRecord.agent` |  |
| `KeyRecord::from_verifying_bytes` | `KeyRecord.from_verifying_bytes` | `KeyRecord.fromVerifyingBytes` |  |
| `KeyRecord::key_id` | `KeyRecord.key_id` | `KeyRecord.keyId` | property |
| `KeyRecord::operator` | `new KeyRecord(kind=)` | `KeyRecord.operator` | keywords |
| `KeyRecord::revoked` | `KeyRecord.revoke` | `KeyRecord.revoke` | name-collision |
| `KeyRecord::valid_window` | `new KeyRecord(valid_from_micros=, valid_to_micros=)` | `KeyRecord.validWindow` | keywords |
| `KeyRegistry` | `KeyRegistry` | `KeyRegistry` |  |
| `KeyRegistry::enroll` | `KeyRegistry.enroll` | `KeyRegistry.enroll` |  |
| `KeyRegistry::enroll_operator` | `KeyRegistry.enroll_operator` | `KeyRegistry.enrollOperator` |  |
| `KeyRegistry::enroll_record` | `KeyRegistry.enroll_record` | `KeyRegistry.enrollRecord` |  |
| `KeyRegistry::new` | `new KeyRegistry()` | `new KeyRegistry()` | constructor |
| `KeyRegistry::verify` | `KeyRegistry.verify` | `KeyRegistry.verify` |  |
| `KeyRegistry::verify_at` | `KeyRegistry.verify_at` | `KeyRegistry.verifyAt` |  |
| `KeyRegistry::verify_observed_at` | `KeyRegistry.verify_observed_at(content_type=, agent_version=)` | `KeyRegistry.verifyObservedAt` | keywords |
| `KvKeyRegistry` | `KvKeyRegistry` | `KvKeyRegistry` |  |
| `KvKeyRegistry::enroll` | `KvKeyRegistry.enroll` | `KvKeyRegistry.enroll` |  |
| `KvKeyRegistry::enroll_record` | `KvKeyRegistry.enroll_record` | `KvKeyRegistry.enrollRecord` |  |
| `KvKeyRegistry::in_namespace` | `new KvKeyRegistry(namespace=)` | `new KvKeyRegistry()` | overload |
| `KvKeyRegistry::new` | `new KvKeyRegistry()` | `new KvKeyRegistry()` | constructor |
| `KvKeyRegistry::registry` | `KvKeyRegistry.registry` | `KvKeyRegistry.registry` |  |
| `KvKeyRegistry::revoke` | `KvKeyRegistry.revoke` | `KvKeyRegistry.revoke` |  |
| `LogPosition` | `LogPosition` | `LogPosition` |  |
| `LogPosition.stream_id` | `LogPosition.stream_id` | `LogPosition.streamId` | keywords |
| `LogPosition.topic_id` | `LogPosition.topic_id` | `LogPosition.topicId` | keywords |
| `LogPosition.partition_id` | `LogPosition.partition_id` | `LogPosition.partitionId` | keywords |
| `LogPosition.offset` | `LogPosition.offset` | `LogPosition.offset` | keywords |
| `LogPosition::from_bytes` | `LogPosition.from_bytes` | `fn:logPositionFromBytes` | free-function |
| `LogPosition::new` | `new LogPosition()` | `LogPosition` | constructor, keywords |
| `LogPosition::to_bytes` | `LogPosition.to_bytes` | `fn:logPositionToBytes` | free-function |
| `RecordId` | omitted | `RecordId` | plain-value |
| `RecordId::as_u128` | omitted | `RecordId.asU128` | plain-value |
| `RecordId::from_bytes` | omitted | `RecordId.fromBytes` | plain-value |
| `RecordId::from_u128` | omitted | `RecordId.fromU128` | plain-value |
| `RecordId::to_bytes` | omitted | `RecordId.toBytes` | plain-value |
| `Signature` | dict via `AgentMessage.envelope` | `Signature` | serde-dict |
| `Signature.scheme` | dict key | `Signature.scheme` | serde-dict, keywords |
| `Signature.key_id` | dict key | `Signature.keyId` | serde-dict, keywords |
| `Signature.bytes` | dict key | `Signature.bytes` | serde-dict, keywords |
| `Signature.context` | dict key | `Signature.context` | serde-dict, keywords |
| `Signature::validate` | `fn:validate_signature` | `fn:validateSignature` | free-function |
| `SignatureContext` | dict via `AgentMessage.envelope` | `wire.SignatureContext` | serde-dict |
| `SignatureContext.content_type` | dict key | `wire.SignatureContext.contentType` | serde-dict, keywords |
| `SignatureContext.agent_version` | dict key | `wire.SignatureContext.agentVersion` | serde-dict, keywords |
| `SigningKey` | `SigningKey` | `SigningKey` |  |
| `SigningKey::from_bytes` | `SigningKey.from_bytes` | `SigningKey.fromBytes` |  |
| `SigningKey::key_id` | `SigningKey.key_id` | `SigningKey.keyId` | property |
| `SigningKey::sign` | `SigningKey.sign` | `SigningKey.sign` |  |
| `SigningKey::sign_with_context` | `SigningKey.sign_with_context(content_type=, agent_version=)` | `SigningKey.signWithContext` | keywords |
| `SigningKey::verifying_key` | `SigningKey.verifying_key` | `SigningKey.verifyingKey` | property |
| `TokenUsage` | dict via `AgentMessage.envelope` | `wire.TokenUsage` | serde-dict |
| `TokenUsage.input_tokens` | dict key | `wire.TokenUsage.inputTokens` | serde-dict, keywords |
| `TokenUsage.output_tokens` | dict key | `wire.TokenUsage.outputTokens` | serde-dict, keywords |
| `TokenUsage.reasoning_output_tokens` | dict key | `wire.TokenUsage.reasoningOutputTokens` | serde-dict, keywords |
| `TokenUsage.cache_read_input_tokens` | dict key | `wire.TokenUsage.cacheReadInputTokens` | serde-dict, keywords |
| `TokenUsage.cache_creation_input_tokens` | dict key | `wire.TokenUsage.cacheCreationInputTokens` | serde-dict, keywords |
| `ValidateError` | `ValidateError` | `wire.ValidateError` |  |
| `ValidateError::Missing` | `ValidateError.MISSING` | `wire.ValidateError` | plain-value |
| `ValidateError::Forbidden` | `ValidateError.FORBIDDEN` | `wire.ValidateError` | plain-value |
| `ValidateError::TooLarge` | `ValidateError.TOO_LARGE` | `wire.ValidateError` | plain-value |
| `ValidateError::Invalid` | `ValidateError.INVALID` | `wire.ValidateError` | plain-value |
| `VerifiedPrincipal` | `VerifiedPrincipal` | `VerifiedPrincipal` |  |
| `VerifiedPrincipal.principal` | `VerifiedPrincipal.principal` | `VerifiedPrincipal.principal` | keywords |
| `VerifiedPrincipal.kind` | `VerifiedPrincipal.kind` | `VerifiedPrincipal.kind` | keywords |
| `sign::sign_card_value` | `fn:sign_card_value` | `fn:signCardValue` |  |
| `sign::verify_card` | `fn:verify_card` | `fn:verifyCard` |  |
| `sign::verify_delegation` | `fn:verify_delegation` | `fn:verifyDelegation` |  |

## snapshot

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `snapshot::ConversationId` | omitted | `wire.ConversationId` | plain-value |
| `snapshot::ConversationId::as_u128` | omitted | `wire.ConversationId.asU128` | plain-value |
| `snapshot::ConversationId::from_bytes` | omitted | `wire.ConversationId.fromBytes` | plain-value |
| `snapshot::ConversationId::from_u128` | omitted | `wire.ConversationId.fromU128` | plain-value |
| `snapshot::ConversationId::to_bytes` | omitted | `wire.ConversationId.toBytes` | plain-value |
| `snapshot::DEFAULT_SNAPSHOT_NAMESPACE` | `const:DEFAULT_SNAPSHOT_NAMESPACE` | `const:DEFAULT_SNAPSHOT_NAMESPACE` |  |
| `snapshot::DEFAULT_SNAPSHOT_TOPIC` | `const:DEFAULT_SNAPSHOT_TOPIC` | `const:DEFAULT_SNAPSHOT_TOPIC` |  |
| `FoldSnapshot` | dict via `SnapshotStore.latest` | `FoldSnapshot` | serde-dict |
| `FoldSnapshot.conversation` | dict key | `FoldSnapshot.conversation` | serde-dict, keywords |
| `FoldSnapshot.as_of` | dict key | `FoldSnapshot.asOf` | serde-dict, keywords |
| `FoldSnapshot.state` | dict key | `FoldSnapshot.state` | serde-dict, keywords |
| `FoldSnapshot::resume_offset` | `fn:fold_snapshot_resume_offset` | `fn:foldSnapshotResumeOffset` | free-function |
| `KvSnapshotStore` | `KvSnapshotStore` | `KvSnapshotStore` |  |
| `KvSnapshotStore::in_namespace` | `KvSnapshotStore.in_namespace` | `new KvSnapshotStore()` | overload |
| `KvSnapshotStore::new` | `new KvSnapshotStore()` | `new KvSnapshotStore()` | constructor |
| `LocalSnapshotStore` | `SnapshotStore` | `SnapshotStore` | trait-variant |
| `LocalSnapshotStore::latest` | `SnapshotStore.latest` | `SnapshotStore.latest` | keywords |
| `LocalSnapshotStore::save` | `SnapshotStore.save` | `SnapshotStore.save` | keywords |
| `SnapshotStore` | `SnapshotStore` | `SnapshotStore` |  |
| `SnapshotStore::latest` | `SnapshotStore.latest` | `SnapshotStore.latest` | keywords |
| `SnapshotStore::save` | `SnapshotStore.save` | `SnapshotStore.save` | keywords |
| `TopicSnapshotStore` | `TopicSnapshotStore` | `TopicSnapshotStore` |  |
| `TopicSnapshotStore::new` | `new TopicSnapshotStore()` | `new TopicSnapshotStore()` | constructor |
| `TopicSnapshotStore::on_topic` | `TopicSnapshotStore.on_topic` | `new TopicSnapshotStore()` | overload |
| `snapshot::decode` | `fn:decode_snapshot` | `fn:decodeSnapshot` | flat-namespace |
| `snapshot::encode` | `fn:encode_snapshot` | `fn:encodeSnapshot` | flat-namespace |

## state_store

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `FileStore` | `FileStore` | `FileStore` |  |
| `FileStore::new` | `new FileStore()` | `new FileStore()` | constructor |
| `InMemoryStore` | `InMemoryStore` | `InMemoryStore` |  |
| `InMemoryStore::new` | `new InMemoryStore()` | `new InMemoryStore()` | constructor |
| `LocalStateStore` | `StateStore` | `StateStore` | trait-variant |
| `LocalStateStore::get` | `StateStore.get` | `StateStore.get` | keywords |
| `LocalStateStore::set` | `StateStore.set` | `StateStore.set` | keywords |
| `LocalStateStore::delete` | `StateStore.delete` | `StateStore.delete` | keywords |
| `StateStore` | `StateStore` | `StateStore` |  |
| `StateStore::get` | `StateStore.get` | `StateStore.get` | keywords |
| `StateStore::set` | `StateStore.set` | `StateStore.set` | keywords |
| `StateStore::delete` | `StateStore.delete` | `StateStore.delete` | keywords |

## stream

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ArrowIpcMessageMetadata` | dict via `PublishRequest.arrow_ipc` | `ArrowIpcMessageMetadata` | serde-dict |
| `ArrowIpcMessageMetadata.contract_version` | dict key | `ArrowIpcMessageMetadata.contractVersion` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.schema_fingerprint` | dict key | `ArrowIpcMessageMetadata.schemaFingerprint` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.encoded_bytes` | dict key | `ArrowIpcMessageMetadata.encodedBytes` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.field_count` | dict key | `ArrowIpcMessageMetadata.fieldCount` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.record_batch_count` | dict key | `ArrowIpcMessageMetadata.recordBatchCount` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.row_count` | dict key | `ArrowIpcMessageMetadata.rowCount` | serde-dict, keywords |
| `ArrowIpcMessageMetadata.dictionary_count` | dict key | `ArrowIpcMessageMetadata.dictionaryCount` | serde-dict, keywords |
| `BackgroundConfig` | `BackgroundConfig` | `ProducerBackgroundOptions` | keywords |
| `BalancedSharding` | omitted | omitted | rust-crate |
| `BatchPublishRequest` | `BatchPublishRequest` | `BatchPublishRequest` |  |
| `BatchPublishRequest::add_arrow_ipc` | `BatchPublishRequest.add_arrow_ipc` | `BatchPublishRequest.addArrowIpc` |  |
| `BatchPublishRequest::add_avro` | `BatchPublishRequest.add_avro` | `BatchPublishRequest.addAvro` |  |
| `BatchPublishRequest::add_encoded` | `BatchPublishRequest.add_encoded` | `BatchPublishRequest.addEncoded` |  |
| `BatchPublishRequest::add_encoded_with_projection` | `BatchPublishRequest.add_encoded_with_projection` | `BatchPublishRequest.addEncodedWithProjection` |  |
| `BatchPublishRequest::add_json` | `BatchPublishRequest.add_json` | `BatchPublishRequest.addJson` |  |
| `BatchPublishRequest::add_json_with_projection` | `BatchPublishRequest.add_json(projection_ref=)` | `BatchPublishRequest.addJsonWithProjection` | overload |
| `BatchPublishRequest::add_msgpack` | `BatchPublishRequest.add_msgpack` | `BatchPublishRequest.addMsgpack` |  |
| `BatchPublishRequest::add_msgpack_with_projection` | `BatchPublishRequest.add_msgpack(projection_ref=)` | `BatchPublishRequest.addMsgpackWithProjection` | overload |
| `BatchPublishRequest::add_payload` | `BatchPublishRequest.add_payload` | `BatchPublishRequest.addPayload` |  |
| `BatchPublishRequest::add_payload_with_projection` | `BatchPublishRequest.add_payload(projection_ref=)` | `BatchPublishRequest.addPayloadWithProjection` | overload |
| `BatchPublishRequest::add_raw_bytes` | `BatchPublishRequest.add_raw_bytes` | `BatchPublishRequest.addRawBytes` |  |
| `BatchPublishRequest::add_raw_bytes_with_projection` | `BatchPublishRequest.add_raw_bytes(projection_ref=)` | `BatchPublishRequest.addRawBytesWithProjection` | overload |
| `BatchPublishRequest::add_record` | `BatchPublishRequest.add_record` | `BatchPublishRequest.addRecord` |  |
| `BatchPublishRequest::content_type` | `BatchPublishRequest.add_raw_bytes(content_type=)` | `BatchPublishRequest.contentType` | keywords |
| `BatchPublishRequest::extend_encoded` | `BatchPublishRequest.extend_encoded` | `BatchPublishRequest.extendEncoded` |  |
| `BatchPublishRequest::extend_json` | `BatchPublishRequest.extend_json` | `BatchPublishRequest.extendJson` |  |
| `BatchPublishRequest::extend_msgpack` | `BatchPublishRequest.extend_msgpack` | `BatchPublishRequest.extendMsgpack` |  |
| `BatchPublishRequest::header` | `BatchPublishRequest.header` | `BatchPublishRequest.header` |  |
| `BatchPublishRequest::index` | `BatchPublishRequest.index` | `BatchPublishRequest.index` |  |
| `BatchPublishRequest::inline_payload` | `BatchPublishRequest.inline_payload` | `BatchPublishRequest.inlinePayload` |  |
| `BatchPublishRequest::is_empty` | `BatchPublishRequest.__len__` | `BatchPublishRequest.isEmpty` | protocol, property |
| `BatchPublishRequest::len` | `BatchPublishRequest.__len__` | `BatchPublishRequest.length` | protocol |
| `BatchPublishRequest::partition_key` | `BatchPublishRequest.partition_key` | `BatchPublishRequest.partitionKey` |  |
| `BatchPublishRequest::projection_ref` | `BatchPublishRequest.projection_ref` | `BatchPublishRequest.projectionRef` |  |
| `BatchPublishRequest::schema_id` | `BatchPublishRequest.schema_id` | `BatchPublishRequest.schemaId` |  |
| `BatchPublishRequest::send` | `BatchPublishRequest.send` | `BatchPublishRequest.send` |  |
| `Cbor` | `Cbor` | `Cbor` |  |
| `Codec` | `PublishRequest.encode_with(codec=)` | `Codec` | callback |
| `Codec::content_type` | `PublishRequest.encode_with(codec=)` | `Codec.contentType` | callback, keywords |
| `Codec::encode` | `PublishRequest.encode_with(codec=)` | `Codec.encode` | callback, keywords |
| `CommitPolicy` | `Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)` | `CommitPolicy` | keywords |
| `CommitPolicy::Disabled` | `Topic.consumer(auto_commit=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::Interval` | `Topic.consumer(auto_commit=, commit_interval_ms=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::Polling` | `Topic.consumer(auto_commit=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::IntervalOrPolling` | `Topic.consumer(auto_commit=, commit_interval_ms=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::All` | `Topic.consumer(auto_commit=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::IntervalOrAll` | `Topic.consumer(auto_commit=, commit_interval_ms=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::Each` | `Topic.consumer(auto_commit=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::IntervalOrEach` | `Topic.consumer(auto_commit=, commit_interval_ms=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::Every` | `Topic.consumer(auto_commit=, commit_every=)` | `CommitPolicy` | keywords, plain-value |
| `CommitPolicy::IntervalOrEvery` | `Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)` | `CommitPolicy` | keywords, plain-value |
| `Consumer` | `Consumer` | `Consumer` |  |
| `Consumer::commit` | `Consumer.commit` | `Consumer.commit` |  |
| `Consumer::delete_offset` | `Consumer.delete_offset` | `Consumer.deleteOffset` |  |
| `Consumer::last_consumed_offset` | `Consumer.last_consumed_offset` | `Consumer.lastConsumedOffset` |  |
| `Consumer::last_stored_offset` | `Consumer.last_stored_offset` | `Consumer.lastStoredOffset` |  |
| `Consumer::next` | `Consumer.next` | `Consumer.stream` | protocol |
| `Consumer::next_within` | `Consumer.next_within` | `Consumer.nextWithin` |  |
| `Consumer::shutdown` | `Consumer.shutdown` | `Consumer.shutdown` |  |
| `Consumer::store_offset` | `Consumer.store_offset` | `Consumer.storeOffset` |  |
| `Consumer::stored_offset` | `Consumer.stored_offset` | `Consumer.storedOffset` |  |
| `ConsumerBuilder` | `Topic.consumer` | `ConsumerOptions` | keywords |
| `ConsumerBuilder::allow_replay` | `Topic.consumer(allow_replay=)` | `ConsumerOptions.allowReplay` | keywords |
| `ConsumerBuilder::auto_join_group` | `ConsumerGroup.consumer(auto_join_group=)` | `ConsumerOptions.autoJoinGroup` | keywords |
| `ConsumerBuilder::batch_length` | `Topic.consumer(batch_length=)` | `ConsumerOptions.batchLength` | keywords |
| `ConsumerBuilder::build` | `Topic.consumer` | `Topic.consumer` | one-call |
| `ConsumerBuilder::commit_policy` | `Topic.consumer(auto_commit=, commit_interval_ms=, commit_every=)` | `ConsumerOptions.commitPolicy` | keywords |
| `ConsumerBuilder::create_group` | `ConsumerGroup.consumer(create_group=)` | `ConsumerOptions.createGroup` | keywords |
| `ConsumerBuilder::init_retries` | `Topic.consumer(init_retries=)` | `ConsumerOptions.initRetries` | keywords |
| `ConsumerBuilder::poll_interval` | `Topic.consumer(poll_interval_ms=)` | `ConsumerOptions.pollIntervalMs` | keywords |
| `ConsumerBuilder::polling_retry_interval` | `Topic.consumer(polling_retry_interval_ms=)` | `ConsumerOptions.pollingRetryIntervalMs` | keywords |
| `ConsumerBuilder::start_at` | `Topic.consumer(polling=, offset=, timestamp_micros=)` | `ConsumerOptions.startAt` | keywords |
| `ConsumerBuilder::without_poll_interval` | `Topic.consumer(poll_interval_ms=)` | `ConsumerOptions.pollIntervalMs` | overload |
| `ConsumerGroup` | `ConsumerGroup` | `ConsumerGroup` |  |
| `ConsumerGroup::consumer` | `ConsumerGroup.consumer` | `ConsumerGroup.consumer` |  |
| `ConsumerGroup::create` | `ConsumerGroup.create` | `ConsumerGroup.create` |  |
| `ConsumerGroup::filter` | `ConsumerGroup.filter` | `ConsumerGroup.filter` |  |
| `ConsumerGroup::id` | `ConsumerGroup.id` | `ConsumerGroup.id` | property |
| `ConsumerGroup::info` | `ConsumerGroup.info` | `ConsumerGroup.info` |  |
| `ConsumerGroup::name` | `ConsumerGroup.name` | `ConsumerGroup.name` | property |
| `ConsumerGroup::reader` | `ConsumerGroup.reader` | `ConsumerGroup.reader` |  |
| `ConsumerGroup::topic` | `ConsumerGroup.topic` | `ConsumerGroup.topic` | property |
| `ConsumerGroupInfo` | `ConsumerGroupInfo` | `ConsumerGroupInfo` |  |
| `ConsumerGroupInfo.id` | `ConsumerGroupInfo.id` | `ConsumerGroupInfo.id` | keywords |
| `ConsumerGroupInfo.name` | `ConsumerGroupInfo.name` | `ConsumerGroupInfo.name` | keywords |
| `ConsumerGroupInfo.identity` | `ConsumerGroupInfo.identity` | `ConsumerGroupInfo.identity` | keywords |
| `ConsumerGroupInfo.filter` | `ConsumerGroupInfo.filter` | `ConsumerGroupInfo.filter` | keywords |
| `ConsumerMessage` | `ConsumerMessage` | `ConsumerMessage` |  |
| `ConsumerMessage.payload` | `ConsumerMessage.payload` | `ConsumerMessage.payload` | keywords |
| `ConsumerMessage.headers` | `ConsumerMessage.headers` | `ConsumerMessage.headers` | keywords |
| `ConsumerMessage.message_id` | `ConsumerMessage.message_id` | `ConsumerMessage.messageId` | keywords |
| `ConsumerMessage.checksum` | `ConsumerMessage.checksum` | `ConsumerMessage.checksum` | keywords |
| `ConsumerMessage.position` | `ConsumerMessage.position` | `ConsumerMessage.position` | keywords |
| `ConsumerMessage.current_offset` | `ConsumerMessage.current_offset` | `ConsumerMessage.currentOffset` | keywords |
| `ConsumerMessage.partition_id` | `ConsumerMessage.partition_id` | `ConsumerMessage.partitionId` | keywords |
| `ConsumerMessage.timestamp_micros` | `ConsumerMessage.timestamp_micros` | `ConsumerMessage.timestampMicros` | keywords |
| `ConsumerMessage.origin_timestamp_micros` | `ConsumerMessage.origin_timestamp_micros` | `ConsumerMessage.originTimestampMicros` | keywords |
| `ConsumerMessage.user_headers` | `ConsumerMessage.user_headers` | `ConsumerMessage.userHeaders` | keywords |
| `ConsumerMessage.headers_malformed` | `ConsumerMessage.headers_malformed` | `ConsumerMessage.headersMalformed` | keywords |
| `ConsumerMessage::json` | `ConsumerMessage.json` | `ConsumerMessage.json` | keywords |
| `ConsumerStart` | `Topic.consumer(polling=, offset=, timestamp_micros=)` | `ConsumerStart` | keywords |
| `ConsumerStart::First` | `Topic.consumer(polling=)` | `ConsumerStart` | keywords, plain-value |
| `ConsumerStart::Last` | `Topic.consumer(polling=)` | `ConsumerStart` | keywords, plain-value |
| `ConsumerStart::Next` | `Topic.consumer(polling=)` | `ConsumerStart` | keywords, plain-value |
| `ConsumerStart::Offset` | `Topic.consumer(offset=)` | `ConsumerStart` | keywords, plain-value |
| `ConsumerStart::TimestampMicros` | `Topic.consumer(timestamp_micros=)` | `ConsumerStart` | keywords, plain-value |
| `ContentType` | omitted | `ContentType` | plain-value |
| `ContentType::Any` | omitted | `ContentType.Any` | plain-value |
| `ContentType::Raw` | omitted | `ContentType.Raw` | plain-value |
| `ContentType::Json` | omitted | `ContentType.Json` | plain-value |
| `ContentType::Avro` | omitted | `ContentType.Avro` | plain-value |
| `ContentType::Protobuf` | omitted | `ContentType.Protobuf` | plain-value |
| `ContentType::Msgpack` | omitted | `ContentType.Msgpack` | plain-value |
| `ContentType::Cbor` | omitted | `ContentType.Cbor` | plain-value |
| `ContentType::Bson` | omitted | `ContentType.Bson` | plain-value |
| `ContentType::Arrow` | omitted | `ContentType.Arrow` | plain-value |
| `ContentType::Ref` | omitted | `ContentType.Ref` | plain-value |
| `ContentType::code` | `fn:content_type_code` | `fn:contentTypeCode` | free-function |
| `ContentType::from_code` | omitted | omitted | plain-value |
| `ContentType::is_raw` | `fn:content_type_is_raw` | `fn:isRawContentType` | free-function |
| `CreateConsumerGroup` | `ConsumerGroup.create` | `CreateConsumerGroupOptions` | keywords |
| `CreateConsumerGroup::build` | `ConsumerGroup.create` | `ConsumerGroup.create` | one-call |
| `CreateConsumerGroup::filter` | `ConsumerGroup.create(filter=)` | `CreateConsumerGroupOptions.filter` | keywords |
| `CreateConsumerGroup::operation_id` | `ConsumerGroup.create(operation_id=)` | `CreateConsumerGroupOptions.operationId` | keywords |
| `CreateConsumerGroup::policy` | `ConsumerGroup.create(filter_id=, revision=)` | `CreateConsumerGroupOptions.policy` | keywords |
| `DecodeError` | omitted | omitted | converted-error |
| `DecodeError::Encode` | omitted | omitted | converted-error |
| `DecodeError::Decode` | omitted | omitted | converted-error |
| `DecodeError::MissingPayload` | omitted | omitted | converted-error |
| `DecodeError::Frame` | omitted | omitted | converted-error |
| `Decoder` | `KvEntry.decode_value_with(codec=)` | `Decoder` | callback |
| `Decoder::decode` | `KvEntry.decode_value_with(codec=)` | `Decoder.decode` | callback, keywords |
| `DirectConfig` | omitted | omitted | rust-crate |
| `GroupFilter` | `GroupFilter` | `GroupFilter` |  |
| `GroupFilter::configure` | `GroupFilter.configure` | `GroupFilter.configure` |  |
| `GroupFilter::configure_as` | `GroupFilter.configure(operation_id=)` | `GroupFilter.configureAs` | overload |
| `GroupFilter::configure_with` | `GroupFilter.configure(filter_id=, revision=)` | `GroupFilter.configureWith` | overload |
| `GroupFilter::delete` | `GroupFilter.delete` | `GroupFilter.delete` |  |
| `GroupFilter::get` | `GroupFilter.get` | `GroupFilter.get` |  |
| `GroupFilter::preview` | `GroupFilter.preview` | `GroupFilter.preview` |  |
| `GroupFilter::release` | `GroupFilter.release` | `GroupFilter.release` |  |
| `GroupFilter::revise` | `GroupFilter.revise` | `GroupFilter.revise` |  |
| `GroupFilter::revisions` | `GroupFilter.revisions` | `GroupFilter.revisions` |  |
| `GroupFilter::set_revision_enabled` | `GroupFilter.set_revision_enabled` | `GroupFilter.setRevisionEnabled` |  |
| `GroupFilter::test` | `GroupFilter.test` | `GroupFilter.test` |  |
| `HeaderKey` | omitted | omitted | rust-crate |
| `HeaderValue` | omitted | `HeaderValue` | rust-crate |
| `Headers` | omitted | `Headers` | rust-crate |
| `IggyConsumer` | omitted | omitted | rust-crate |
| `IggyConsumerBuilder` | omitted | omitted | rust-crate |
| `IggyMessage` | `IggyMessage` | omitted | rust-crate |
| `IggyProducer` | omitted | omitted | rust-crate |
| `IggyProducerBuilder` | omitted | omitted | rust-crate |
| `Json` | `Json` | `Json` |  |
| `stream::LOGICAL_SCHEMA_FINGERPRINT` | `const:LOGICAL_SCHEMA_FINGERPRINT` | `const:LOGICAL_SCHEMA_FINGERPRINT` |  |
| `Msgpack` | `Msgpack` | `Msgpack` |  |
| `OrderedSharding` | omitted | omitted | rust-crate |
| `Producer` | `Producer` | `Producer` |  |
| `Producer::send` | `Producer.send` | `Producer.send` |  |
| `Producer::send_batch` | `Producer.send_batch` | `Producer.sendBatch` |  |
| `Producer::send_batch_with_routing` | `Producer.send_batch(key=, partition=)` | `Producer.sendBatchWithRouting` | overload |
| `Producer::send_keyed` | `Producer.send(key=)` | `Producer.sendKeyed` | overload |
| `Producer::send_message` | `Producer.send(headers=)` | `Producer.sendMessage` | overload |
| `Producer::send_to_partition` | `Producer.send(partition=)` | `Producer.sendToPartition` | overload |
| `Producer::send_with_routing` | `Producer.send(key=, partition=)` | `Producer.sendWithRouting` | overload |
| `Producer::shutdown` | `Producer.shutdown` | `Producer.shutdown` |  |
| `ProducerBuilder` | `Topic.producer` | `ProducerOptions` | keywords |
| `ProducerBuilder::background` | `Topic.producer(background=)` | `ProducerOptions.background` | keywords |
| `ProducerBuilder::batch_length` | `Topic.producer(batch_length=)` | `ProducerOptions.batchLength` | keywords |
| `ProducerBuilder::build` | `Topic.producer` | `Topic.producer` | one-call |
| `ProducerBuilder::create_stream` | `Topic.producer(create_stream=)` | `ProducerOptions.createStream` | keywords |
| `ProducerBuilder::create_topic` | `Topic.producer(create_topic=)` | `ProducerOptions.createTopic` | keywords |
| `ProducerBuilder::expire_after` | `Topic.producer(message_expiry=)` | `ProducerOptions.expireAfterMicros` | keywords |
| `ProducerBuilder::linger` | `Topic.producer(linger_ms=)` | `ProducerOptions.lingerMs` | keywords |
| `ProducerBuilder::max_topic_bytes` | `Topic.producer(max_topic_size=)` | `ProducerOptions.maxTopicBytes` | keywords |
| `ProducerBuilder::never_expire` | `Topic.producer(message_expiry=)` | `ProducerOptions.neverExpire` | keywords |
| `ProducerBuilder::partitions` | `Topic.producer(partitions=)` | `ProducerOptions.partitions` | keywords |
| `ProducerBuilder::retries` | `Topic.producer(retries=)` | `ProducerOptions.retries` | keywords |
| `ProducerBuilder::retry_backoff` | `Topic.producer(retry_interval_ms=)` | `ProducerOptions.retryBackoffMs` | keywords |
| `ProducerBuilder::routing` | `Topic.producer(key=, partition=)` | `ProducerOptions.routing` | keywords |
| `ProducerBuilder::unlimited_topic_size` | `Topic.producer(max_topic_size=)` | `ProducerOptions.unlimitedTopicSize` | keywords |
| `ProducerMessage` | `Producer.send` | `ProducerMessage` | keywords |
| `ProducerMessage::builder` | `Producer.send` | `new ProducerMessage()` | keywords, constructor |
| `ProducerMessage::header` | `Producer.send(headers=)` | `ProducerMessage.header` | keywords |
| `ProducerMessage::new` | `Producer.send` | `new ProducerMessage()` | keywords, constructor |
| `ProducerMessage::with_headers` | `Producer.send(headers=)` | `ProducerMessage.withHeaders` | keywords |
| `ProducerMessageBuilder` | `Producer.send` | `ProducerMessage` | keywords |
| `ProducerMessageBuilder::build` | `Producer.send` | `new ProducerMessage()` | one-call |
| `ProducerMessageBuilder::headers` | `Producer.send(headers=)` | `ProducerMessage.headers` | keywords |
| `ProducerMessageBuilder::maybe_headers` | `Producer.send(headers=)` | `ProducerMessage.headers` | keywords |
| `ProducerMessageBuilder::payload` | `Producer.send(payload=)` | `ProducerMessage.payload` | keywords |
| `PublishRequest` | `PublishRequest` | `PublishRequest` |  |
| `PublishRequest::arrow_ipc` | `PublishRequest.arrow_ipc` | `PublishRequest.arrowIpc` |  |
| `PublishRequest::avro` | `PublishRequest.avro` | `PublishRequest.avro` |  |
| `PublishRequest::claim_check` | `PublishRequest.claim_check` | `PublishRequest.claimCheck` |  |
| `PublishRequest::content_type` | `PublishRequest.raw_bytes(content_type=)` | `PublishRequest.contentType` | keywords |
| `PublishRequest::encode_with` | `PublishRequest.encode_with` | `PublishRequest.encodeWith` |  |
| `PublishRequest::header` | `PublishRequest.header` | `PublishRequest.header` |  |
| `PublishRequest::index` | `PublishRequest.index` | `PublishRequest.index` |  |
| `PublishRequest::inline_payload` | `PublishRequest.inline_payload` | `PublishRequest.inlinePayload` |  |
| `PublishRequest::json` | `PublishRequest.json` | `PublishRequest.json` |  |
| `PublishRequest::msgpack` | `PublishRequest.msgpack` | `PublishRequest.msgpack` |  |
| `PublishRequest::partition_key` | `PublishRequest.partition_key` | `PublishRequest.partitionKey` |  |
| `PublishRequest::payload` | `PublishRequest.payload` | `PublishRequest.payload` |  |
| `PublishRequest::projection_ref` | `PublishRequest.projection_ref` | `PublishRequest.projectionRef` |  |
| `PublishRequest::provenance` | `PublishRequest.provenance` | `PublishRequest.provenance` |  |
| `PublishRequest::raw_bytes` | `PublishRequest.raw_bytes` | `PublishRequest.rawBytes` |  |
| `PublishRequest::schema_id` | `PublishRequest.schema_id` | `PublishRequest.schemaId` |  |
| `PublishRequest::send` | `PublishRequest.send` | `PublishRequest.send` |  |
| `Record` | `BatchPublishRequest.add_record` | `Record` | keywords |
| `Record.index` | `BatchPublishRequest.add_record(index=)` | `Record.index` | keywords |
| `Record.metadata` | `BatchPublishRequest.add_record(headers=)` | `Record.metadata` | keywords |
| `Record.inline_payload` | `BatchPublishRequest.add_record(inline_payload=)` | `Record.inlinePayload` | keywords |
| `Record.content_type` | `BatchPublishRequest.add_record(content_type=)` | `Record.contentType` | keywords |
| `Record.projection_ref` | `BatchPublishRequest.add_record(projection_ref=)` | `Record.projectionRef` | keywords |
| `Record.schema_id` | `BatchPublishRequest.add_record(schema_id=)` | `Record.schemaId` | keywords |
| `Record.logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` | keywords |
| `Record::builder` | `BatchPublishRequest.add_record` | `new Record()` | keywords, constructor |
| `RecordBuilder` | `BatchPublishRequest.add_record` | `Record` | keywords |
| `RecordBuilder::build` | `BatchPublishRequest.add_record` | `new Record()` | one-call |
| `RecordBuilder::content_type` | `BatchPublishRequest.add_record(content_type=)` | `Record.contentType` | keywords |
| `RecordBuilder::index` | `BatchPublishRequest.add_record(index=)` | `Record.index` | keywords |
| `RecordBuilder::inline_payload` | `BatchPublishRequest.add_record(inline_payload=)` | `Record.inlinePayload` | keywords |
| `RecordBuilder::logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` | keywords |
| `RecordBuilder::maybe_content_type` | `BatchPublishRequest.add_record(content_type=)` | `Record.contentType` | keywords |
| `RecordBuilder::maybe_logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` | keywords |
| `RecordBuilder::maybe_projection_ref` | `BatchPublishRequest.add_record(projection_ref=)` | `Record.projectionRef` | keywords |
| `RecordBuilder::maybe_schema_id` | `BatchPublishRequest.add_record(schema_id=)` | `Record.schemaId` | keywords |
| `RecordBuilder::metadata` | `BatchPublishRequest.add_record(headers=)` | `Record.metadata` | keywords |
| `RecordBuilder::projection_ref` | `BatchPublishRequest.add_record(projection_ref=)` | `Record.projectionRef` | keywords |
| `RecordBuilder::schema_id` | `BatchPublishRequest.add_record(schema_id=)` | `Record.schemaId` | keywords |
| `Routing` | `Topic.producer(key=, partition=)` | `Routing` | keywords |
| `Routing::Balanced` | `Topic.producer(key=, partition=)` | `Routing.balanced` | keywords |
| `Routing::Key` | `Topic.producer(key=)` | `Routing.key` | keywords |
| `Routing::Partition` | `Topic.producer(partition=)` | `Routing.partition` | keywords |
| `Routing::key` | `Topic.producer(key=)` | `Routing.key` | keywords |
| `SendMessagesConfirmationResponse` | `SendMessagesConfirmationResponse` | omitted | rust-crate |
| `SendMessagesResponse` | `SendMessagesResponse` | omitted | rust-crate |
| `Sharding` | omitted | omitted | rust-crate |
| `StoredOffset` | `StoredOffset` | `StoredOffset` |  |
| `StoredOffset.stored_offset` | `StoredOffset.stored_offset` | `StoredOffset.storedOffset` | keywords |
| `StoredOffset.current_offset` | `StoredOffset.current_offset` | `StoredOffset.currentOffset` | keywords |
| `Stream` | `Stream` | `Stream` |  |
| `Stream::delete` | `Stream.delete` | `Stream.delete` |  |
| `Stream::ensure` | `Stream.ensure` | `Stream.ensure` |  |
| `Stream::name` | `Stream.name` | `Stream.name` | property |
| `Stream::topic` | `Stream.topic` | `Stream.topic` |  |
| `Topic` | `Topic` | `Topic` |  |
| `Topic::batch` | `Topic.batch` | `Topic.batch` |  |
| `Topic::batching` | `Topic.batching` | `Topic.batching` |  |
| `Topic::cbor` | `Topic.cbor` | `Topic.cbor` |  |
| `Topic::consumer` | `Topic.consumer` | `Topic.consumer` |  |
| `Topic::consumer_group` | `Topic.consumer_group` | `Topic.consumerGroup` |  |
| `Topic::consumer_group_id` | `Topic.consumer_group_id` | `Topic.consumerGroupId` |  |
| `Topic::ensure` | `Topic.ensure` | `Topic.ensure` |  |
| `Topic::ensure_consumer_group` | `Topic.ensure_consumer_group` | `Topic.ensureConsumerGroup` |  |
| `Topic::iggy_consumer` | omitted | omitted | rust-crate |
| `Topic::iggy_consumer_group` | omitted | omitted | rust-crate |
| `Topic::iggy_producer` | omitted | omitted | rust-crate |
| `Topic::json` | `Topic.json` | `Topic.json` |  |
| `Topic::name` | `Topic.name` | `Topic.name` | property |
| `Topic::producer` | `Topic.producer` | `Topic.producer` |  |
| `Topic::publish` | `Topic.publish` | `Topic.publish` |  |
| `Topic::publish_batch` | `Topic.publish_batch` | `Topic.publishBatch` |  |
| `Topic::replay` | `Topic.replay` | `Topic.replay` |  |
| `Topic::schema` | `Topic.schema` | `Topic.schema` |  |
| `Topic::send` | `Topic.send` | `Topic.send` |  |

## swarm

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `AgentActivity` | `AgentActivity` | `AgentActivity` |  |
| `AgentActivity.decisions` | `AgentActivity.decisions` | `AgentActivity.decisions` |  |
| `AgentActivity.last_decision` | `AgentActivity.last_decision` | `AgentActivity.lastDecision` |  |
| `AgentActivity::count` | `AgentActivity.count` | `AgentActivity.count` |  |
| `SwarmActivity` | `SwarmActivity` | `SwarmActivity` |  |
| `SwarmActivity::agent` | `SwarmActivity.agent` | `SwarmActivity.agent` |  |
| `SwarmActivity::agents` | `SwarmActivity.agents` | `SwarmActivity.agents` |  |
| `SwarmActivity::new` | `new SwarmActivity()` | `new SwarmActivity()` | constructor |
| `SwarmActivity::observe` | `SwarmActivity.observe` | `SwarmActivity.observe` |  |

## testing

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `testing::agent_ctx` | `fn:agent_ctx(fixed_inbox=)` | `fn:agentCtx` | keywords |
| `testing::agent_message` | `fn:agent_message` | `fn:agentMessage` |  |

## typed

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `TypedDecodeError` | `TypedDecodeError` | `TypedDecodeError` |  |
| `TypedDecodeError.position` | `TypedDecodeError.position` | `TypedDecodeError.position` |  |
| `TypedDecodeError.source` | `TypedDecodeError.source` | `TypedDecodeError.source` |  |
| `TypedRecord` | `TypedRecord` | `TypedRecord` |  |
| `TypedRecord.value` | `TypedRecord.value` | `TypedRecord.value` | keywords |
| `TypedRecord.position` | `TypedRecord.position` | `TypedRecord.position` | keywords |
| `TypedRecord.headers` | `TypedRecord.headers` | `TypedRecord.headers` | keywords |
| `TypedRecords` | `TypedRecords` | `TypedRecords` |  |
| `TypedRecords::batch` | `TypedTopic.records(batch=)` | `TypedRecords.batch` | keywords |
| `TypedRecords::from_offsets` | `TypedTopic.records(from_offsets=)` | `TypedRecords.fromOffsets` | keywords |
| `TypedRecords::next` | `TypedRecords.next` | `TypedRecords.stream` | protocol |
| `TypedRecords::offsets` | `TypedRecords.offsets` | `TypedRecords.offsets` | property |
| `TypedRecords::poll` | `TypedRecords.poll` | `TypedRecords.poll` |  |
| `TypedRecords::stream` | `TypedRecords.__aiter__` | `TypedRecords.stream` | protocol |
| `TypedTopic` | `TypedTopic` | `TypedTopic` |  |
| `TypedTopic::publish` | `TypedTopic.publish` | `TypedTopic.publish` |  |
| `TypedTopic::publish_batch` | `TypedTopic.publish_batch` | `TypedTopic.publishBatch` |  |
| `TypedTopic::records` | `TypedTopic.records` | `TypedTopic.records` |  |
| `TypedTopic::topic` | `TypedTopic.topic` | `TypedTopic.topic` | property |

## types

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `types::AgentId` | omitted | `AgentId` | plain-value |
| `types::AgentId::as_str` | omitted | `AgentId.asStr` | plain-value |
| `types::AgentId::new` | omitted | `AgentId.new` | plain-value |
| `types::AgentId::wire_id` | omitted | `AgentId.wireId` | plain-value |
| `ConsumerGroupName` | omitted | `ConsumerGroupName` | plain-value |
| `ConsumerGroupName::as_str` | omitted | `ConsumerGroupName.asStr` | plain-value |
| `ConsumerGroupName::for_agent` | omitted | `ConsumerGroupName.forAgent` | plain-value |
| `ConsumerGroupName::new` | omitted | `ConsumerGroupName.new` | plain-value |
| `types::ConversationId` | omitted | `ConversationId` | plain-value |
| `types::ConversationId::as_u128` | omitted | `ConversationId.asU128` | plain-value |
| `types::ConversationId::derive` | `fn:derive_conversation_id` | `ConversationId.derive` | free-function |
| `types::ConversationId::new` | `fn:new_conversation_id` | `ConversationId.new` | free-function |
| `IdError` | `IdError` | `IdError` |  |
| `IdError::Empty` | `IdError.EMPTY` | `IdError.empty` |  |
| `IdError::TooLong` | `IdError.TOO_LONG` | `IdError.tooLong` |  |
| `IdError::InvalidChar` | `IdError.INVALID_CHAR` | `IdError.invalidChar` |  |
| `IdError::InvalidUlid` | `IdError.INVALID_ULID` | `IdError.invalidUlid` |  |
| `IdError::InvalidMessageId` | `IdError.INVALID_MESSAGE_ID` | `IdError.invalidMessageId` |  |
| `IntentId` | omitted | `IntentId` | plain-value |
| `IntentId::new` | omitted | `IntentId.new` | plain-value |
| `MessageId` | `MessageId` | `MessageId` |  |
| `MessageId.partition_id` | `MessageId.partition_id` | `MessageId.partitionId` | keywords |
| `MessageId.offset` | `MessageId.offset` | `MessageId.offset` | keywords |
| `MessageId::new` | `new MessageId()` | `MessageId` | constructor, keywords |
| `MintUlid` | `fn:mint_ulid` | `MintUlid` | free-function |
| `MintUlid::mint` | `fn:mint_ulid` | `MintUlid.mint` | free-function, property |
| `PrincipalId` | omitted | `PrincipalId` | plain-value |
| `PrincipalId::get` | omitted | `PrincipalId.get` | plain-value |
| `PrincipalId::new` | omitted | `PrincipalId.new` | plain-value |

## watch

| Rust | Python | TypeScript | Notes |
| --- | --- | --- | --- |
| `ChangeRecord` | `ChangeRecord` | `ChangeRecord` |  |
| `ChangeRecord.v` | `ChangeRecord.v` | `ChangeRecord.v` | keywords |
| `ChangeRecord.index` | `ChangeRecord.index` | `ChangeRecord.index` | keywords |
| `ChangeRecord.partition_id` | `ChangeRecord.partition_id` | `ChangeRecord.partitionId` | keywords |
| `ChangeRecord.from_offset` | `ChangeRecord.from_offset` | `ChangeRecord.fromOffset` | keywords |
| `ChangeRecord.to_offset` | `ChangeRecord.to_offset` | `ChangeRecord.toOffset` | keywords |
| `ChangeRecord.rows` | `ChangeRecord.rows` | `ChangeRecord.rows` | keywords |
| `Watch` | `Laser.watch` | `Watch` | keywords |
| `Watch::index` | `Laser.watch(index=)` | `Watch.index` | keywords |
| `Watch::records` | `Laser.watch(from_offsets=)` | `Watch.records` | keywords |
| `WatchReader` | `WatchReader` | `WatchReader` |  |
| `WatchReader::from_offsets` | `WatchReader.from_offsets` | `WatchReader.fromOffsets` |  |
| `WatchReader::offsets` | `WatchReader.offsets` | `WatchReader.offsets` | property |
| `WatchReader::poll` | `WatchReader.poll` | `WatchReader.poll` |  |
| `WatchReader::stream` | `WatchReader.__aiter__` | `WatchReader.stream` | protocol |

## Peer-only APIs

These Python and TypeScript APIs have no Rust row for a language reason.

| Client | API | Note |
| --- | --- | --- |
| Python | `Consumer.init` | lazy-init |
| Python | `ConsumerMessage.header_kinds` | rust-crate |
| Python | `Producer.init` | lazy-init |
| TypeScript | `Laser.withObserver` | telemetry |
| TypeScript | `LaserBuilder.observer` | telemetry |
| TypeScript | `LaserObserver` | telemetry |
| TypeScript | `MintUlid.fromU128` | std-trait |
| TypeScript | `NOOP_OBSERVER` | telemetry |
| TypeScript | `ObservationLevel` | telemetry |
| TypeScript | `ObserveEffect` | telemetry |
| TypeScript | `OpenTelemetryObserver` | telemetry |
| TypeScript | `OpenTelemetrySpan` | telemetry |
| TypeScript | `OpenTelemetryTracer` | telemetry |
| TypeScript | `SpanScope` | telemetry |
| TypeScript | `messageIdToString()` | std-trait |
| TypeScript | `parseIdempotencyKey()` | std-trait |
| TypeScript | `parseMessageId()` | std-trait |
