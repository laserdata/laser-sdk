# LaserData - Go client

The Go client for [LaserData](https://laserdata.com) implements the `laser-wire` contract.

The Go client uses the Apache Iggy Go SDK over TCP. It includes streaming, managed queries and state, sessions, memory, workflows, and the A2A and MCP bridges. The [implementation status](IMPLEMENTATION_STATUS.md) tracks coverage and the [parity matrix](../../docs/parity.md#the-go-client) lists supported capabilities.

## Quick start

Set `LASER_CONNECTION_STRING` to a development deployment and run these commands from the repository root using Go 1.25 or later. The examples module uses the local client. The log example records the current end of each partition, publishes two typed readings, and replays the new records. It uses `LASER_STREAM` or the default stream `laser-log-go`.

```sh
cd examples/go
go run ./log
```

The [repository quick start](../../README.md#quick-start) includes a complete Go program. The [Go examples](../../examples/go/README.md) cover the individual primitives and larger applications. Managed operations need Laser Stack or LaserData Cloud. The pinned Apache Iggy Go SDK supports TCP. Producer idempotence and background sharding require upstream support.

## Module versions

`laser.ClientVersion` in `laser/version.go` gives the version recorded in session and bridge metadata. It matches the workspace package version. The `go` directive in `go.mod` specifies the minimum Go toolchain.

For a release, the maintainer pushes `go-vX.Y.Z` on the tested revision. Go CI verifies the version, runs the gates, and creates `foreign/go/vX.Y.Z`. Go uses this second tag because the module lives in `foreign/go`. Once the release is published, consumers can run `go get github.com/laserdata/laser-sdk/foreign/go@vX.Y.Z` with the released version.

## Managed commands

The managed surfaces (query, key-value, sessions, authorization, filters, forks, graph) ride one command band off the log, which this client reaches through the Apache Iggy Go SDK's `SendBinaryRequest`. `Laser.SendManaged` is that path.

A command that changes state is wrapped in a managed envelope carrying an operation identity, so a transport retry of the same call is applied once. `SendManagedWithOperation` lets a caller choose the identity, so a retry that outlives the process still applies once. Reads are sent as given.

That identity is also what lets this client repeat the mutation itself. A reply that never came back is not a mutation that did not happen: the request may have reached a node and applied there, so reporting the lost link as a failure would name an outcome nobody knows. The frame goes again under the same identity instead, for up to `ManagedDeadline`, and the backend answers the outcome it recorded rather than applying a second time. A refusal that says the command may yet apply, pending or an unavailable backend, counts the same way.

Each attempt is bounded on its own, the first by `ManagedAttemptFirst` and each later one by twice that, never past the whole deadline, so one dead node costs one bound rather than the entire deadline. Between attempts the managed backoff spreads the repeats.

What a caller is told at the end is either the outcome or an `AmbiguousMutationError` naming the identity to recover under, and the ambiguity is reported in two places: an outcome still unknown when the deadline passes, and a refusal that arrives after an attempt whose outcome was never known. The second is the one worth stating plainly. A refusal on the first attempt is the whole answer. The same refusal after a lost attempt only says this attempt was not admitted, and the earlier one may have applied already, so it is reported as ambiguous rather than as a clean failure.

A batch frame settles item by item. One item whose outcome is unknown leaves the whole frame unresolved and the same frame is sent again, where each item answers under its own derived identity: a mutation with its recorded outcome, a read with a fresh result. An item that was unknown and is then refused by admission is ambiguous exactly like a single mutation, and names the item.

Managed names are scoped to the default stream (`stream:<stream>/<name>`), which is how two streams in one deployment keep separate namespaces. `Options.ResourceNaming: laser.ResourceNamingBare`, or `WithResourceNaming(laser.ResourceNamingBare)` on a handle, opts out and sends every name as written. Listings are mapped back to the caller's own names.

Every managed reply raises the client's applied frontier, and a later read of that surface sends it, so whichever node answers has applied at least what this client already wrote or read.

Each surface is gated twice before a call leaves: once on the capability the deployment announced, and once on the op version it serves, because a payload sent across a version gap would be read under a contract neither side agreed to.

## Typed topics and registered schemas

Use `laser.TypedJSON[T](topic)` or `laser.TypedCBOR[T](topic)` when the payload carries its own encoding. For a registered writer schema, `laser.TypedSchema[T](ctx, topic, id)` fetches and compiles the definition once. Its Avro and JSON Schema writes encode and validate before sending. To publish a generated Protobuf message, marshal it with Protobuf and call `topic.Publish().RawBytes(payload, wire.ContentProtobuf).SchemaID(id).Send(ctx)`. Use the typed handle to decode the stored bytes and include the record position in a decode error. The [fleet tape example](../../examples/go/fleet-tape/main.go) registers, publishes, and replays an Avro reading.

Go validates JSON Schema `pattern` and `patternProperties` using ECMAScript Unicode regular expressions. Each match has a 100 ms timeout and a bounded backtracking stack. Exceeding either limit fails the entire validation, including under a negated condition. The validator resolves references inside the registered document.

A registered Protobuf schema encodes too. `CompiledSchema.EncodeProtobuf(value)` writes one message from a value whose JSON form names the message's fields, and a topic typed with `TypedSchema` or `TypedWith(topic, compiled)` publishes through it and stamps the content type and the schema id. Bytes fields take a byte array or base64 text, an enum takes its number or its value name, and a key the schema does not declare is left out. A body that does not fit is a `CodecError` naming the field, before anything is sent.

## Key-value

`Laser.Kv(namespace)` opens a namespace. Reads are `Get`, `GetEntry`, `GetEntryIfChanged` (a conditional read that skips the body transfer) and `Exists`. Writes are the `Set` builder with `TTL` or `ExpiresAt`, turning into a compare-and-swap through `ExpectVersion` or `ExpectAbsent`. `Scan` lists a namespace by prefix, range or substring, with `Each` for a bounded walk and `All` to collect. `DeleteMany` applies the same bounds in bulk.

A refusal arrives as a `KvError` carrying the contract's own classification, so `Retryable` and `Conflict` tell a momentary outage from a precondition that will never hold. Keys are arbitrary bytes, so every read hands back a `[]byte` rather than a string.

`Kv.Lease(ctx, key, holder, ttl)` acquires a bounded lease. The grant carries its fencing token, validity deadline and mutation position. The acquisition runs on a coordination connection of its own, opened on first use and closed with the handle, so a handle from `FromClient` refuses it with a `ConfigError`. An unknown outcome repeats under one operation identity until the requested TTL since the first send has passed, and only then is an `AmbiguousMutationError`. `TryLease` returns nil for an already held lease. Use `RenewLease` to extend it and `ReleaseLease` to release it. A `LeaseGuard` can be released with a deferred call. `PrepareRelease` captures the key, token and operation identity so the same release can be retried.

`Kv.CasFenced(key, fenceNamespace, fenceKey, fenceToken)` requires a live lease and an explicit `ExpectAbsent` or `ExpectVersion`. The deployment must advertise fenced leases. Managed writes repeat under their original identity when their outcome is uncertain. A grant that arrives after its local deadline reports lease loss.

`Laser.ExecuteBatch(ctx, ops)` sends up to `wire.MaxBatchOps` independent managed commands in one round trip. It returns one encoded reply per command in input order. `Kv.GetMany(ctx, keys)` uses a batch of scoped reads and returns nil for a missing key and an empty slice for a stored empty value.

`Kv.Patch(ctx, key, patch)` applies a JSON merge patch and returns the new version, and `PatchIfVersion` applies it only at the version given. `CopyTo(key, toKey)` and `MoveTo(key, toKey)` build a copy or a move that `Send` runs. `Expire` and `ExpireAt` set a lifetime on a stored key and return its version. `ExpireAt` refuses a time before the epoch. A conditional write built from `Set` takes `TTL` or `ExpiresAt` like an unconditional one. `Set(key).EncodeWith(codec, value)` stores what any `wire.Codec` encodes, and `Msgpack(value)` is the MessagePack form. `CasFenced` takes the same two.

`Kv.InSession(session)` returns a handle that links its writes to that session. A batch refuses an item above the value limit and a batch above the frame limit before sending. A refusal of the whole batch is reported as the refusal and repeats under the batch's identity like a single write.

## Fenced lease coordination

`laser.NewDedicatedFencedLeaseClient(connectionString)` opens a coordination client on its own connection, dialed on first use, so a slow lease call never holds the connection that publishes and reads. `PrepareAcquire`, `PrepareRenew`, `PrepareRelease` and `PrepareCasFenced` validate a `wire` request and mint its operation identity once. `Acquire`, `Renew`, `Release` and `CasFenced` send that exact frame under a per-attempt timeout, 15 seconds unless `WithAttemptTimeout` sets another. `Get` is the read behind `wire.KvGet.MinPosition`, and a barrier the store cannot reach is a `KvError` of kind `wire.KvStale`, never an absent value. A granted `Lease` is valid from the first send plus the granted TTL.

A timeout, a reply that names no outcome yet, or a link that failed after the frame may have left is an `AmbiguousMutationError` carrying the operation identity. `PreparedMutation.AmbiguousRecovery()` names what to do: repeat the same prepared renewal or release, repeat a prepared acquisition or wait out its TTL before acquiring again, or read the target and reconcile a fenced write through its precondition. A failure before the frame was written keeps its own error. A deployment without the fenced-lease contract answers `UnsupportedError` and keeps the connection. `NewFencedLeaseClient(transport)` takes any `ManagedKvTransport`. Call `Close` when done.

## Authorization

`Whoami` reads the caller's own effective capabilities. `ListRoles`, `GetRole`, `DefineRole`, `DeleteRole`, `GetBindings`, `BindRoles` and `AuthzHistory` administer the rest. Deny wins over allow, and the default is deny.

This surface is served by the streaming server itself rather than by a managed backend, so a deployment announcing the authorization feature serves it either way. The gate is that feature, not the managed root.

Every write carries a receipt identity, so an exact repeat after a lost reply returns the original outcome instead of applying twice. `DefineRoleAs`, `DeleteRoleAs` and `BindRolesAs` let a caller choose it and keep it across a restart. `BindRolesExpectRevision` makes a bind conditional, so two administrators editing one user cannot both win.

## Query

`Laser.Query(index)` builds a query against a materialized index. Predicates chain and compose with AND, `AnyOf` adds a disjunction, and the aggregate, grouping, ordering, paging and projection steps read as one expression. `Fetch` sends it.

Three fields are additive on the wire, so an unaware backend would drop them and answer anyway: a consistency level, a lexical search, and raw SQL. Each is refused before sending unless the deployment announced it, because an answer that is wider or staler than was asked for still looks like success. Raw SQL additionally needs a ready backend that announced that dialect.

The deadline is absolute on the wire and resolved at send time, so a request built and held does not arrive already expired. Every execution carries its own identity. A result whose rows do not match its schema is refused rather than read, since misaligned values look like data rather than a fault.

The deadline defaults to 25 seconds, which is also the longest a query may ask for. A later deadline is refused before sending, and a call stops waiting two seconds after its deadline. Each `Fetch` on one builder is a new execution, and a result that names another execution is refused.

`Rows()` reads pages on demand. `Next` returns one row at a time and nil at the end, `All` ranges over them, and `MaxRows(n)` caps the read. `FrontierMoved` reports that a later page was served at a different position than the first. `FetchAll` collects every page into one set. A `QueryRow` reads a column with `Value`, `ValueText`, `ValueI64` or `ValueU64`, finds it with `FieldIndex`, and decodes the original payload with `Decode`.

`TypedQuery[T](request, codec)` reads results as values of `T` from their original payloads, with `Fetch`, `FetchOne`, `FetchAll` and `Rows`. `WhereEq` and `Predicate` add filters, `Nearest(embedding, topK)` and `NearestIn(field, embedding, topK)` run a vector search, and `Window(field, every)` groups an aggregate by intervals of a timestamp field. The standard deviation aggregate is returned in the `stddev` column.

`laser.QueryRowsFromPage(fields, rows)` binds the rows of one page to its fields. A row's `Score` is the vector distance or lexical rank when the backend returned one. `wire.TypedValue` reads with `AsStr`, `AsI64` and `AsU64`, and `ValidateAgainst(logicalType, required)` checks a value against a logical type. A result is refused when a value does not fit its field, a score is not finite, or the row count differs from the rows.

## Forks

`Laser.Fork(id)` branches the read model. A fork overlays speculative rows on the trunk without writing to it, so a what-if can be explored and then promoted or discarded. `Create` makes one (continuous by default, `Severed` freezes it at the trunk's current offsets), `PutRow` writes a speculative row or a tombstone, and `Promote` or `Squash` ends it. `Laser.Forks` lists them.

An embedding travels as a JSON array literal, so a component that is not a finite number is refused: JSON has no spelling for one, and it would silently change which rows a nearest-neighbour search returns.

## Graph

`Laser.Graph(name)` builds a traversal: a start (`StartIDs`, `StartMatch`, `StartNearest`), hops (`Out`, `In`, `Both`, `Hop`), filters, and what to return. `Neighbors` is the cheap one-hop read. `AsOf` reads the graph as it stood at an instant, keeping only edges whose valid-time window contains it.

`Upsert` and `Link` write facts. Ids are content-addressed, so re-applying the same extraction converges on the same elements instead of duplicating them, which is what lets a projector replay without the graph growing.

`Unlink(ctx, from, relation, to)` closes an edge by setting its valid-to instant. `Relink` closes the live edges of that relation to other targets and links the new one at the same instant, and returns how many it closed. `Graph.Conversation(id)` reads the graph through one conversation.

## Session reads

`Laser.Sessions()` reads the deployment's session index for the default stream, and `SessionsIn(stream)` for another. `Get`, `List`, `Events`, `State`, `Links`, `Sources` and `Changes` cover it.

`Sessions.Watch(ctx, pollEvery)` follows changes to the index, starting after the newest change already visible. `Next` waits for a change and names the sessions that changed. Its `Resync` flag asks the caller to list sessions again after a retention gap or an index replacement.

`Laser.ReadAt(ctx, wire.SourceRef)` reads the one log record a reference names, for a caller holding a `Sources` entry and wanting the record behind it. It is a `Laser` method rather than a `Session` one, matching Rust, since a reference carries its own stream and topic and needs no session to resolve against. The topic's id, generation and name are snapshotted before the poll and again after, so a topic recreated mid-read reads as gone rather than returning a record presented as something it is not. A stale reference answers `nil, nil` rather than an error: holding one from earlier is an ordinary thing, not a fault. Only a message reference names a log record, and a kv or memory reference is refused.

Every request names its stream first, because the index is per stream: a session id alone does not say which index to look in. The stream is sent as given rather than scoped, since it is a stream name and not a managed resource name.

A list or timeline walked page by page would shift under appends, so the first page returns a boundary and later pages pass it back through `Bound`. The boundary carries its change series too, because a rebuilt index restarts the sequence and the offset alone would then mean something else.

## Streams, topics and publishing

`Laser.Stream(name).Topic(name)` addresses a topic, and `Laser.Topic(name)` uses the stream pinned at connect. `Ensure` creates what is absent and leaves what exists alone, so it is safe on every start: two callers racing to create one stream both succeed, because the loser re-reads and finds what it wanted.

`Publish` builds a record (`Payload`, `JSON`, `CBOR`, `Header`, `Key`) and `Send` appends it. `Topic.Send` is the same path with explicit headers, and `Topic.Batch` appends many records in one call under one partitioning.

A partition key is the only ordering Iggy gives: records sharing a key land on one partition. Omitting the key balances across partitions and promises no order between records. An empty key is refused, because it would hash every record onto one partition while reading as "no key given".

## Stream and topic administration

`Laser.Streams`, `Stream.Get`, `Stream.Exists`, `Stream.Rename` and `Stream.Topics` read and rename the catalogue. `Topic.Get`, `Topic.Exists`, `Topic.Update`, `Topic.Partitions`, `Topic.AddPartitions`, `Topic.DeletePartitions`, `Topic.DeleteSegments` and `Topic.ConsumerGroups` do the same for a topic.

A stream or topic that does not exist reads as absent from `Get` and `Exists`, because the caller asked whether it is there and the answer is no. A listing is different: the server lists a missing stream exactly the way it lists one with no topics, so the listing checks and fails rather than handing back an empty slice that means something else.

`UpdateTopicOptions` leaves every field it does not set. An expiry together with never-expire, or a size cap together with an unlimited size, is refused rather than resolved: the caller asked for two different things and only it knows which it meant. A count of zero partitions or segments is refused before it is sent, since the server would either do nothing or refuse it and neither tells the caller it asked for nothing.

Never-expire and unlimited are not a duration and not a cap, so each reads as none rather than as the largest value this build can hold. A caller planning around a limit that is not there is worse off than one told there is none.

## Provenance

`Provenance` is the agentic spine: conversation, causality, routing and usage. `Publish().Provenance(p)` stamps it onto a record and partitions by the conversation, which is what keeps one conversation ordered. `ProvenanceFrom(message)` reads it back.

Numbers ride typed rather than as decimal text, using the contract's own encoder, so there is one implementation of the byte layout rather than two that can drift. A header outside the dictionary is foreign and ignored, so an application's keys and the AGDX envelope stamps pass through. A key the dictionary does know, carrying the wrong kind, is corruption and is reported: silently dropping a fence would skip the gate and let a stale holder's replay through as if it were unfenced.

A record with no conversation is refused, since it has no partition key and no conversation to belong to. A broadcast addressee reads back as no target rather than an agent named `*`. A non-finite cost is refused in both directions, because it would be rolled up into a total as a number that is not one.

## Causality

A record names the one it answers with `laser.MessageID`: the partition and offset of that record, spelled `partition:offset` on the `agdx.cause` header. A record id would not do, because a consumer that wants to read what a reply answers needs to know where it is.

An AGDX envelope carries both: a record id in `cause`, which is portable, and the log position in `cause_at`, which is what the header is derived from. A reply whose caller knew no position stamps no cause header rather than one a reader cannot resolve.

A padded number is refused on the way in. It parses the same as its canonical spelling, so accepting it would let two clients disagree about whether `01:42` and `1:42` name the same record.

One difference from the other clients remains here. They synthesize a record's spine from the decoded envelope, so an AGDX record carries only the envelope header block that `wire/fixtures/header_block_envelope.json` pins. This client builds the spine from headers, so it also stamps `agdx.corr` on a correlated AGDX record, which that block does not carry. Both peers ignore it, because for an AGDX record they read the envelope and not the headers, so this costs bytes and not correctness. The gates here read the spine from the headers.

## Settling an uncertain request

A send whose reply never arrived leaves a request the client cannot account for. The transport retains it and reports its id, which `TransportError.UncertainOperation` reads, and the lane it blocks stays blocked until it is settled.

`Laser.ResolveOperation` replays the retained request until the deployment gives its final result, which is the only honest way to learn whether it applied. `Laser.AbandonOperation` gives it up and frees the lane, and cannot undo anything the server did: an abandoned request may well have applied, and nothing afterwards will say whether it did. That is why it is the second choice rather than the quick way out.

The transport keeps the outcomes its replays proved, so asking twice answers from that record. An operation it never proved, because a session reset or an abandon came first or the record aged out, is reported as not retained rather than as a success nobody checked.

`TransportError.IggyErrorCode` reads the Apache Iggy code underneath a transport failure, so a caller branches on the server's own vocabulary rather than on message text.

## Classifying failures

A failure is asked what it is with a function over the error, whichever surface raised it: `laser.IsNotFound`, `IsStreamOrTopicNotFound`, `IsPermissionDenied`, `IsUnavailable`, `IsNotLeader`, `IsStale`, `IsVersionConflict`, `IsVersionSkew`, `IsLeaseLost`, `IsFenceViolation` and `IsQuarantined`. A failed publish and a consumer group setup failure answer for their cause, and so does an error wrapped with `%w`. `IsUnavailable` is the transient class a retry loop keys on. `IsStale` is a read model that has not caught up. `IsPermissionDenied` covers a missing credential and a missing grant, where the fix is a grant and not a retry. `IsStreamOrTopicNotFound` is the server saying the stream or topic does not exist for this principal, which is also what a stream the principal cannot see answers.

## Consuming

`Topic.Consumer(name, options)` opens a standalone consumer and `Topic.ConsumerGroup(group, options)` joins a group. A standalone consumer names the partitions it reads, because nothing assigns them to it. A group member names none, because the server assigns and rebalances them.

`Next` hands over one record and `Each` runs a handler over them. A record arrives with its provenance already read: a record with no spine carries none, which is the normal case on a plain data topic, while a record whose spine is present but broken is refused rather than handed over with its fence silently missing.

`ConsumerOptions.AutoCommit` selects when offsets are stored. The default follows successful processing. Set `ManualCommit` and call `Commit` after your own side effects when you need explicit acknowledgment. Spawned agents use explicit acknowledgment by default.

On a server that resolves group policies, a group consumer runs the group's saved filter on the server and stores its progress through fenced acknowledgments. A handler error leaves that record uncounted until a later record of its partition counts. After a rebalance or a policy change the consumer restarts the affected partition and reads on. While idle it holds a filtered wait on the server when the deployment announces one, and polls on a 250 ms interval otherwise.

`Consumer.LastConsumedOffset(partition)` is the last offset this consumer handed over, and `LastStoredOffset(partition)` the last one it stored by `Commit`, `StoreOffset` or the acknowledgments of a group that runs a policy. Both are local bookkeeping. `StoredOffset` reads what the server holds. `ConsumerOptions.PollRetryInterval` is the wait before a failed read is sent again, one second by default. `InitRetries` with `InitRetryInterval` tries a failed open again when the failure is retryable. `StoreOffset` does not send an offset at or below the last one this consumer stored unless `AllowReplay` is set.

## Stored offsets

`Consumer.StoredOffset` reads what the server holds for this consumer, or for its group, on one partition, with the partition's head beside it so a caller can see how far behind it is. It is a server read rather than this client's own bookkeeping, so it answers what a restart would resume from, including a position another member of the group stored. A server holding nothing reports absence rather than zero, because offset zero is a stored position like any other.

`Consumer.StoreOffset` writes a position directly and `Consumer.DeleteOffset` removes one. Storing is the escape hatch rather than the usual path: it names a position nothing delivered, so a caller that stores past what it handled skips records silently. Commit a delivered record instead, and keep this for a deliberate reposition. Deleting what is not there reads as done, which lets a cleanup run twice.

## Replay and typed records

`Topic.Replay()` opens a positional cursor. It owns its offsets rather than storing them server-side, so persist `Offsets()` and resume with `FromOffsets()`. `Poll` drains what is there and stops, which is the bounded read below a consumer. Records come back ordered by the server's own clock, then partition and offset, so two readers of one topic see one order.

`TypedJSON[T]`, `TypedCBOR[T]` and `TypedWith[T]` view a topic as carrying one body type through one codec. `Publish` stamps the codec's content type and `Records` walks a cursor handing over decoded bodies.

One unreadable record is usually one bad writer, not a broken topic, so a decode failure names the partition and offset it sat at and the read carries on. Passing no error handler skips those records quietly.

`Cursor.Poll` returns the records ahead of an unreadable one first. The next call reports that record alone as an `*UndecodableRecord` with its partition and message, and the cursor moves past it.

`TypedTopic.PublishBatch(bodies)` encodes every body and sends them as one batch. A body that does not encode fails the send before any record reaches the server.

## Agent messaging

`Laser.Agdx(source, conversation)` sends AGDX messages on one conversation. `Command`, `Respond`, `Emit`, `Fail` and `Chunk` cover the kinds, and `OpenAgdx` reads one back off a record.

Each record carries the whole envelope as its body, because a reader needs the routing and causality to decide anything. The envelope stamps and the provenance spine ride one header block, each key once, so a reader can route and audit without decoding the body. The conversation is the partition key.

Where the contract ties fields together, the constructor does too rather than leaving a chain a caller can half build. `TaskStatus` takes the correlation and the state it requires. `SessionStarted`, `SessionProgressed` and `SessionEnded` each take the typed lifecycle record their state calls for, and the terminal flag is derived from the state rather than taken from the caller, which is the only way the two cannot drift apart.

Cost rides the envelope in integer micro-units and the headers in whole currency, so it is converted between them rather than copied.

`Agdx.RequestInput(ctx, replyTopic, prompt, timeout)` marks the task input-required, sends the prompt to every agent as an `input_request` command and returns the answer. `RequestInputFrom` addresses one agent and accepts only its answer. An answer from another agent is ignored and the wait goes on. A rejection is an `InputRejectedError`.

## Chunk streams

`agdx.Stream(correlation, wire.OperationChat)` opens a chunk stream writer on a fresh channel. `Write` publishes the next chunk and assigns its sequence. The opening chunk carries the purpose and the bound after which readers abandon the stream, ten minutes unless `WithDeadline` sets another. A failed `Write` keeps its sequence, so writing the body again continues without a gap. `Finish(ctx, reason, usage)` ends the stream, and `Fail(ctx, failure)` ends it with an error record on the same channel. A chunk body is at most 64 KiB.

`Buffered(maxChunks, linger)` holds chunks and appends them as one batch when the count fills or a `Write` finds the linger passed. `Flush` sends what is held, and the terminal always does. Chunk bodies are raw unless `ContentType` says otherwise, and `WithTarget` addresses one agent. The writer's records are not offered to the governor. `Laser.ReassembleChannel` and `ChunkAssembler` read a stream back in order.

## Agent gates

`Laser.Agent(identity, options)` holds the gates a running agent applies. `Decide` returns a verdict for a record and `DecideMessage` for an AGDX message. `Run` reads a consumer through them.

The order is the point. Expiry and the must-understand set come first, because a record failing either can never be handled, and spending a dedup key on it would suppress a later record that could be. The fence comes next, so a stale holder's replay advances nothing. Dedup comes last, so a key is spent only on a record that would otherwise have been handled.

The fence is the real at-most-once gate: it refuses a record whose fence is behind the highest accepted for its task, accepts an equal one as the same holder retrying, and lets unfenced work through. Dedup is a best-effort window scoped to the producing agent, so one producer cannot suppress or replay another's key. Both are bounded by the recently active working set rather than by every record ever seen.

## Registry and routing

`NewAgentRegistry()` holds what the deployment knows about its agents: the cards they published and the ones an operator took out of rotation. A card with a lifetime stops describing a live agent once it runs out, which is what makes a card liveness and not only discovery.

`To`, `Broadcast`, `ToCapable`, `AllCapable`, `Sticky` and `WithScorer` build a route, and `ResolveTargets` names the agents it reaches. `RouteCheapest`, `RouteFastest` and `RouteLeastLoaded` rank on the advisory classes a card advertises.

A capability route that matches no live agent is an error, not a silent broadcast: the caller asked for a skill, and handing the work to whoever is listening is not that. An agent that declared itself unavailable is skipped, while a degraded one still routes, since a struggling agent beats no agent. A sticky route falls back when its agent is gone, because pinning a conversation is a preference rather than a requirement. Candidates come back in a stable order, so two resolutions pick the same agent and a retry lands where the first attempt did.

`Laser.Quarantine(ctx, operator, agent)` appends a quarantine fact to the registry topic and `Unquarantine` lifts it. `QuarantineSigned` and `UnquarantineSigned` sign the fact with the operator's key. A fact names its agent in its body, is valid for five minutes, and a registry folds it once. A route that names a quarantined agent is refused with a `*QuarantinedError`. `AgentRegistry.Verifier(keys)` makes a registry accept a quarantine fact only when an operator key in `keys` signed it.

## Request and reply

`NewReplyHub()` matches incoming replies to the requests waiting for them. One hub reads a reply topic once for every waiter, so many concurrent requests share one read rather than each scanning the topic.

`Subscribe` registers a wait **before** the request is sent, because the reply cannot exist until it is: a hub that started listening afterwards could miss an answer that arrived in between. `Await` blocks for it.

A correlation alone does not match. Two conversations can reuse one, and a shared reply topic carries every agent's answers, so the session and the addressee are checked too. A waiter that knows who it is will not take an unaddressed reply. One that does not care takes whatever arrives, which is what a sole consumer of a private reply topic wants.

An acknowledgment (`task`/`working`) extends the wait rather than ending it, because the deadline is for a reply that never comes rather than for a handler that is slow. `Acknowledged` reports it, so a slow handler is waited out rather than retried into a duplicate.

`Laser.Requester(source, topic)` ties the two halves together: `Request` sends a command and waits for its answer, narrowing the wait to the command it sent once the send returns. Nothing drives the hub on its own, so a caller feeds `Hub().Deliver` from whatever reads the reply topic. `Deliver` returns false for anything that answers no waiting request, so one reader serves both the requests and the rest of the traffic.

`Laser.DeadLetter` publishes a record the gates turned away, keeping the payload so the work can be replayed and naming where the original sat.

`Requester.Verifier(*KeyRegistry)` arms a hub against forged replies: with one set, `Deliver` drops (silently, the same as a message nobody is waiting for) a reply that is unsigned, does not verify, or verifies as a principal other than the addressed target, and stamps the verified principal on `AgdxMessage.VerifiedPrincipal` on one that holds up. A broadcast request (no `Target`) accepts any principal the registry verifies, since no particular agent was ever the expected answerer. An agent's own internal hub (`AgentCtx.Request`, `ApprovalGate`) is armed automatically from `RuntimeOptions.Verifier`, so the registry gating that agent's incoming work also gates the replies it waits on.

`RequestOptions.Provenance` sends a request under another conversation and ancestry, such as the subconversation `AgentCtx.SpawnSubconversation` derives. `AgentCtx.Request` may await its answer on any reply topic, and a request sent from inside a handler is answered while the handler waits. `wire.DynValueOf(value)` turns a Go value into the dynamic value a session state or a tool argument carries.

## Contract

`Laser.Contract(router, topic)` opens a `ContractBuilder`: a directed request to one agent, bundling resolution, a deadline, and an optional consumption expiry into one call. `From`, `Payload`, `Deadline`, and `Send` are the usual path. `Registry` supplies the `AgentRegistry` a capability route (`ToCapable`, `Sticky`, `WithScorer`) resolves against, the same registry `AgentCtx.FanOut` takes. Without one, the contract reads the registry topic through the registry kept for its connection and stream. `router` must resolve to exactly one target: `Broadcast` and `AllCapable` are refused at `Send`.

Unlike `Requester`, `Send` drives its own wait. It reads the reply topic through a shared dispatcher and needs no caller that already reads it and feeds a `ReplyHub`, so a contract is usable standalone by an orchestrator with no running consumer of its own, the same shape the MCP bridge's tool-call wait uses. The command goes to the inbox its route resolves and the reply is awaited on `topic`, as [Where a command is addressed](#where-a-command-is-addressed) describes.

`Send` settles in one of four `ContractState`s. `ContractCompleted` is a non-error reply and `ContractFailed` a terminal error reply. `ContractNotConsumed` means the target's reliable consumer read the command past its own `ExpireIfNotConsumed` expiry and dropped it before any handler ran. It is reported only on that answer, which a target whose `RuntimeOptions` name a `RespondOn` topic sends. `ContractTimedOut` means nothing landed within `Deadline`. `Fence` stamps a fence token, and `Conversation` pins the conversation a fenced, re-dispatchable task must reuse across attempts.

`Parent(parent, root)` runs the contract as a child session: `Send` writes the child's submitted start on `agent.sessions` with the ancestry before the command, stamps the ancestry on the command, and ends the child by the outcome. The child completes on a reply and fails otherwise, including when the command failed to publish.

`Verifier(*KeyRegistry)` checks each candidate reply against the expected signer. A principal-bound route uses the resolved principal. Other routes use the target agent. The contract waits for a verified reply until its deadline.

## Scatter

`Laser.ScatterReport(ctx, ScatterRequest)` dispatches a `Contract` to every agent advertising `Skill`, resolved through `AllCapable` over the `Registry` the request names, as `ContractBuilder.Registry` and `FanOutRequest.Registry` do, or over the one kept for the connection. The branches run concurrently and are gathered under `Policy` within `Deadline` the way `AgentCtx.FanOut` gathers: one goroutine per target, a quorum stops the rest, best-effort takes whatever landed by the wall clock, and the branches still running when the gather stops come back as `Cancelled`. Nothing advertising the skill at all is a `NoCapableAgentError`, and `GatherQuorum(0)` is refused before any read.

`ScatterReport` keeps every branch's own outcome, attributed to its agent, so an all-failed scatter reads as a report of failures rather than an empty success: `ScatterOutcome.Contract` is set (`ContractCompleted`, `ContractFailed`, `ContractNotConsumed` or `ContractTimedOut`, exactly as `ContractBuilder.Send` classifies it) for a branch that finished, and `.Err` instead only for one that could not run at all, which is an infrastructure or send failure and never a non-completing terminal. `Completed()` and `Failures()` filter each. `Satisfied` reports whether the policy was met: `GatherAll` needs every branch to have *completed*, not merely finished, so one failed branch among otherwise-successful ones reads as unsatisfied even though nothing timed out or was cancelled.

## One reader per reply topic

A contract waits for its answer on a shared dispatcher, opened on first use and keyed by stream and reply topic. Ten concurrent contracts on one topic read it once between them, where a poll per waiter would scan the same records ten times. A scatter over ten agents is the ordinary case.

The wait is registered **before** the command is sent. The reply cannot exist until the command does, but it can land before the caller starts listening, and a dispatcher that only subscribed afterwards would miss it.

Two consequences worth knowing:

- `Laser.Close()` stops the dispatchers. Go has no destructor to end them the way Rust's last-clone drop does, so a handle that is never closed leaves one poller per reply topic running for the life of the process. That is the same bargain `AgentHandle` makes, and the same fix.
- A contract that arms its own `Verifier` keeps a private poll instead of joining the shared dispatcher. A hub's verifier covers every waiter on that topic, so one caller's key registry must not silently become every caller's. Rust puts the verifier on the connection and so has one answer for the whole topic. This client takes it per contract, and this is what that costs.

A waiter's deadline is also extended whenever the target acknowledges that it is working, so the deadline bounds an answer that never comes rather than a handler that is slow.

## Where a command is addressed

`Laser.Contract(router, topic)` sends the command to the inbox the resolved target advertises and awaits the answer on `topic`. Two builders change that.

`InboxRoute(AdvertisedInbox())` is the default route. It addresses the command to the inbox the target advertises in its live presence, and to the inbox on its registry card when the server keeps no presence, as on Apache Iggy. The substrate has no well-known inbox on purpose: a deployment scopes each user to its own stream and each workflow to its own topic, and a topic may be created for one process and dropped when it is done. An agent whose presence and card both name no inbox is a `NoInboxError`, because an invented destination delivers work where nobody consumes. Under an advertised route, a capability pick considers agents with an advertised inbox. `FixedInbox(topic)` sends to a shared topic instead.

Without a `Registry`, `Contract`, `Scatter` and `Workflow` read the registry topic through one registry kept per connection and stream, so a send reads only the cards published since the last one.

`ReplyOn(topic)` awaits the answer on another topic than the one `Contract` was given. A requester that declares its own topic is answered there.

`Workflow.InboxRoute` and `ScatterRequest.Inbox` carry the same route into every step and branch. Both pin their replies to the topic the caller named rather than following the command: a workflow's journal records that topic's tail as each step's dispatch point, so a resumed run would scan the wrong offsets if an advertised inbox moved the answers elsewhere.

`ScatterRequest.Verifier(*KeyRegistry)` arms each branch's own contract, and the expected signer is the branch's own resolved target rather than one principal for the whole scatter: a reply signed by an enrolled key that is not the agent that branch was sent to does not complete that branch. A skill served by several agents is exactly where a global check would be too loose.

`Laser.Scatter(ctx, ScatterRequest)` is a convenience over `ScatterReport` for a caller who only wants the completed reply bodies, dropping agent attribution and every other outcome.

## Workflow

`Laser.Workflow(source, topic)` opens a journalled directed-acyclic workflow over `Contract` and `Scatter`. `.Step(label, router, run)` adds a step that dispatches to one agent or, under `AllCapable`, to every agent advertising a skill, joining their completed replies with `\n`. `.StepLocal(label, run)` adds one whose body runs in this process and needs no agent. Both return a `*StepHandle` that refines the step with `.After(label)` for a dependency, `.VerifyWith(verifier)` and `.Deadline(d)` before the next `.Step`, `.StepLocal` or `.Run(ctx)`. `run` and `verifier` are plain `func` values, `StepFunc` and `StepVerifier`, where Rust needs a trait. `.Budget(WorkflowBudget)` caps tokens, wall-clock time, and invocations. `.Registry(*AgentRegistry)` names the registry an `AllCapable` step's agents are resolved from, the same registry `Contract` and `AgentCtx.FanOut` take.

`Run` runs the steps in dependency order, by Kahn's algorithm, to a `*WorkflowOutcome`: `Outputs` by label, `RunID`, `CompletedSteps` in completion order, and `.Text(label)`. A duplicate label, a broadcast step, or a dependency cycle is refused before anything is written. A run on a stream without `agent.sessions` or `agent.workflow_journal` bootstraps them with the defaults, a day's retention, before retrying, so a workflow of only local steps needs no agent and no caller-made `Bootstrap` call. The run is a session on `agent.sessions` whose id is the run id, and every directed or all-capable step is a child session of it, since it dispatches through `ContractBuilder.Parent` or `Scatter`. A step that does not complete fails the run with a `StepFailedError` naming why: `StepTimedOut`, `StepFailed`, `StepNotConsumed`, `StepUnverified` or `StepNoCompletion`. A budget breach is a `BudgetExceededError`, ended the way a session budget breach is, through `Session.FailOverBudget`. A token budget is also written on the run session's start record. At every step boundary the engine reads `agent.control` for a cancel request on the run. A requested cancel compensates the completed steps, ends the run session as canceled and returns a `CancelledError` naming the run. The engine then checks whether the run session is over its budget, and refuses the step when less than 100 ms of a wall-clock budget is left. `WorkflowRun.Checkpoint` makes the same checks in the plain code form, and `RunStep` calls it before a step body.

`.RunID(runID)` resumes an earlier run, from `WorkflowOutcome.RunID` or one derived for a task. `Run` replays the run's whole journal on `agent.workflow_journal`, folded start to finish and never a window, and a non-empty journal missing its start is a `JournalTruncatedError`. It skips every step already recorded complete. For a step dispatched but not answered, it looks for the reply at the positions journaled before that dispatch and does not resend blind. It resends only when nothing is there, under the same derived idempotency key, so a duplicate is harmless.

`StepHandle.CompensateWith(run)` gives a step the saga rollback: if a later step fails, every earlier step with a compensation and an effect is undone in reverse completion order. An effect is a directed or all-capable step that reached a reply, even one a verifier then rejected, or a local step, whose build always counts as its own effect. A directed or all-capable step's compensation is sent as a fresh `Contract` to the exact agents that ran the original, with no fresh capability resolution. A local step's compensation runs in process when it builds: the build itself is the undo and nothing is dispatched anywhere. A completed compensation is journaled as `StepCompensated`, so a resume reruns the undone step and does not read it as still done. One that does not complete is left unjournaled and the rollback continues, never overriding the original failure.

`StepHandle.Exclusive()`, or `.ExclusiveIn(namespace)` to share a fence sequence with the handler's own `Kv.CasFenced` commit, opts a directed step into a fenced, at-most-once effect: the engine claims a lease with `Kv.LeaseGuard` before dispatching, stamps its token on the command's `agdx.fence` header, renews the lease at half its TTL while the contract is in flight, and releases it on every exit. `.OnTimeout(OnTimeoutReassign)`, which needs `Exclusive`, hands a timed-out attempt to a fresh holder. It prefers a capable agent other than the ones that already timed out, through `ContractBuilder.Excluding`, and reuses one of them only when no other is capable. The fresh holder's lease acquisition bumps the fence and gates the stale holder out. A local step cannot be exclusive, and an all-capable step cannot either, since it has no single target to fence. Both are refused by `Run` before anything is written, as is `OnTimeout(OnTimeoutReassign)` on a non-exclusive step and an exclusive step against a deployment without the managed fenced-lease capability. The lease is acquired and renewed on the handle's coordination connection, so an exclusive step needs a handle opened from a connection string.

`Workflow.Start(ctx)` opens a `*WorkflowRun` for the plain code form: the caller's own code is the workflow, calling the free generic function `laser.RunStep[T](ctx, run, label, body)` inline and declaring no steps up front. Go has no generic methods, so `RunStep` and `RunWith` cannot be methods of `WorkflowRun` or `Workflow`: a type parameter can only live on the type itself, and one run serves steps of many different `T`s. `laser.RunWith[T]` wraps `Start`, the body and `Finish` into one call, and `WorkflowOutcome.Value`, read back with `WorkflowValue[T]`, holds the body's own JSON-encoded return value. A step run already recorded returns its recorded value without running again. The second call of one label in a run is journaled as `label#2`, the third as `label#3`, and a call whose body failed gives its label back to a retry, so concurrent branches whose order can change between runs should use a label of their own. The lower-level primitives `BeginStep`, `Checkpoint`, `CompleteStep`, `AbandonStep`, `RefuseNewSteps`, `Finish` and `Release` are exported too, for a step body that is not a single Go closure. This form writes the same journal entry kinds as the declared-steps form, with one additive field, the call's own argument digest, so everything below about the journal's encoding applies to it unchanged.

A run holds a session lease for exactly as long as it is working. The lease is taken when the run's root session begins and released after its ending record, so a deployment can tell a run that is still going from one that stopped: without the heartbeat a live run would read as abandoned the moment its lease expired.

`Release` gives the lease up and leaves the run open and resumable by its id, for a caller that was cancelled or interrupted. It ends the run as far as this process is concerned, so a `Finish` afterwards is refused rather than writing an ending record for a run nobody is driving. Whichever of the two comes first takes the lease, and the second is a no-op, so the two can never both end one run.

The workflow journal's encoding is the same across clients: an externally tagged enum, one CBOR map from the variant's name to its fields, pinned in `agent_workflow_journal_codec_test.go` against bytes the Rust client itself produced. A run this client starts resumes on another client, and one another client started resumes here. A variant this client does not know is refused rather than skipped, because folding a history only partly understood would resume a run from a state nobody wrote.

One detail worth knowing if you read the bytes: a step's output rides as a CBOR **array of integers**, not a byte string, because that is how the other clients' `Vec<u8>` serialises.

## Semantic memory

`laser.NewVectorMemory(embedder)` keeps facts in this process and ranks recall by how close their embeddings are to the query's, so a recall finds what a question means rather than which words it used. It loses everything when the program exits. The durable memory is `Laser.Memory(namespace)`, which writes to the log.

`Embedder`, `Reranker` and `Summarizer` are seams, not implementations. The client ships no model and never will: an embedding API, a local model or a hosted service lives in application code, and these three interfaces are the boundary. `NoSummarizer` is the do-nothing summarizer, so "no summarizer" is a value rather than a nil check at every call site.

On the vector index every strategy except `RecallKeyword` and `RecallRecent` scores an item by the cosine similarity of its embedding to the query's, and `RecallKeyword` and `RecallHybrid` add the share of the query's words the item holds. Feedback adds its weight under every strategy except `RecallRecent`, as a signal labelled `RecallAuto`. An item's score is the sum of its signals, and each signal stays on the item as a `RecallSignal` with its own rank, so a caller can see why something was recalled. With nothing to rank on, the newest items come first. `FuseReciprocalRank` is a separate helper that fuses ranked lists by the sum of `1/(60 + rank)`.

`VectorMemory.WithReranker(reranker)` puts a second, more expensive pass over the candidates a cheap signal proposed. A recall with no query is handed back untouched: there is nothing to rank against, and asking anyway would invent an ordering.

## Rebuilding state from the log

`laser.LoadConversationState(ctx, laser, conversation, topics, bound, initial, fold)` folds a conversation's logged messages back into state, which is event sourcing over the log. A free function rather than a method, because Go has no generic methods and the state type is the caller's.

The bound is explicit, because the three readings of "the history" are different jobs: `FullReplay()` rebuilds everything, `LastReplay(n)` keeps the newest turns, `ReplayAfter(checkpoint)` catches up on what is new, and `ReplayUntil(checkpoint)` reproduces a moment exactly however much has been appended since. A bound that needs a checkpoint and was given none is refused rather than quietly becoming a full replay.

`laser.WithMemory(handler, memory).AutoRemember(kind)` wraps a handler so each turn it handles is remembered under its own conversation. Wrapping alone changes nothing until a kind names what to store the turn as. A failed memory write does not fail the turn: the message was already handled, and a retry would repeat the handler's own effects, so a memory that could not be written is dropped rather than turned into a redelivery.

## Memory

`Laser.Memory(namespace)` opens the agentic memory surface. `Remember`, `Forget` and `Improve` write to the memory topic, so memory is replayable: the log is the audit, and a deployment folds it into a read view.

`Memory.Recall` reads the scoped KV view when the deployment serves it. The connection tracks up to 4,096 recent writes. A recall covering one of those writes waits up to ten seconds for the view to reflect it. On Apache Iggy, recall folds the memory topic. `RecallFolded` selects that path explicitly. The first write creates a missing stream and memory topic, with thirty days of topic retention.

`Memory.Consolidate(ctx, maxItems)` keeps the newest items in the handle's scope. `ConsolidateWith` accepts an application summarizer and an option to prune the covered messages. Each conversation receives its own durable summary, identified by the sorted IDs of the messages it covers. A later pass over the same messages reuses that summary. Pass a `NewDefaultConsolidator(memory, maxItems)` to a spawned agent for periodic consolidation.

An item's id is derived from its owner, kind and body. The owner is part of the hash, so two users never share an item and the same fact in two conversations is two items. Remembering the same thing twice converges on one item rather than growing the store. Forgetting writes a tombstone: it changes what is recalled rather than erasing what happened.

`Rank` orders items against a query, which is the ranking a client can do with no model: overlap on words. It counts query words covered rather than item words matched, so a long item does not outrank a precise one. `ToContextBlock` renders them for a prompt, filling in rank order until the token budget is reached. The budget is advisory, since tokens are estimated rather than tokenized: a real count needs the model's tokenizer, and carrying one would tie the client to a provider.

`Memory.Set(ctx, key, body)` writes named point state under your own key, as one record on the memory topic. `Fetch` reads it from the managed view where the deployment serves one and folds the topic elsewhere, and `FetchFolded` always folds. `Update` applies a JSON merge patch (RFC 7386) to the current value and writes the result as one record. `Remove` deletes the key, and removing an absent key succeeds. Keys are prefixed with the namespace, so two namespaces on one topic never collide. A named item is never returned by recall.

`Laser.MemoryOnTopic(topic, stream)` opens memory in an existing topic of your own, on the default stream when the stream is empty. `Laser.MemoryTopic(topic)` builds one: chain `Stream`, `Partitions`, `TTL` or `NoExpiry`, then `Build(ctx)` creates the topic when it is missing and returns the handle. It defaults to one partition and thirty days of retention, and an existing topic keeps its settings.

`Laser.MemoryWith(namespace, backend)` picks the store behind the handle. `MemoryBackendAuto` and `MemoryBackendLog` are the durable memory on the stream. `MemoryBackendVector` is an in-process similarity index whose writes still pass the governor. Register its embedder with `Memory.Embedder`. `Laser.MemoryCustom(backend)` routes every verb to your own `CustomMemory`. `Memory.Backend()` reports `log`, `vector` or `custom`. The named verbs need the log backend. `Memory.Reranker(reranker)` reorders what a recall with a query returns, on any backend.

`Memory.Search(ctx, query, limit)` is keyword recall within the handle's scope, 50 items when the limit is zero.

## Context assembly

`Assemble(conversation)` builds a conversation's history into the messages a prompt carries. `Add` offers records and keeps what belongs to the conversation, since a topic carries many.

Ordering is by the server's append time, which is the one clock every topic shares. It is best effort rather than strictly chronological: Apache Iggy orders per partition, so no total order exists across them to read. Ties are broken on topic then offset, because a prompt that reorders between runs is not reproducible.

`LastN`, `RoleFilter`, `TokenBudget` and `Chain` narrow the history. A budget walks newest first, since a prompt wants what just happened, and always keeps at least one message so an oversized one reaches the model to be truncated there rather than vanishing before it arrives. `TokenBudget.Estimate` takes a real tokenizer when a caller has one. The default is the contract's own estimate, which memory spends too, so history and recalled memory share one notion of a budget.

## Sessions

`Laser.Sessions()` opens and writes agent sessions, alongside the index reads above. `Create(label)` derives a session id from its stream, namespace and label, so a job that runs nightly resumes its session rather than starting a new one each time. `Start()` opens a fresh one.

`Begin`, `Progress`, `Complete` and `Fail` write the lifecycle. A session writes as an agent, so one that names none with `As` is refused before it sends. `Session.Memory()` is scoped to the conversation and `Session.Context()` carries the configured turn and token bounds.

`Session.MemoryIn(namespace)` is the session's memory in an explicit namespace and `Session.Graph(name)` the named knowledge graph. `LinkedMemory()` and `LinkedGraph(name)` stamp what they write with the session, its producer and the source record being handled, and `Session.Kv(namespace)` links key-value writes to the session. `ActingOn(source)` names the source record those linked writes identify. `RecordRetrieval(ctx, query, items)` records the recalled items that entered the session's context. `WithBudget(budget)` records token and cost limits on the start record, and `WithIdleTimeout(timeout)` sets how long the session can go without a heartbeat before it is idle. `Stream`, `Agent`, `Parent`, `Root`, `Config` and `Scope` read the handle back.

A derived id is wire-visible identity: the same seed reaches the same conversation in every client. The algorithm is a versioned FNV-1a, pinned by the same golden vector the Rust client pins, so a drift in either is caught rather than silently addressing different sessions. Bumping the version remaps every derived conversation, which is why it is a constant to change on purpose rather than an implementation detail.

`Session.Checkpoint(ctx)` records where the session lane and the declared agent topics end. `TurnsAt(ctx, checkpoint)` reads the records up to it and `TurnsSince(ctx, checkpoint)` the records after it, each as a `SessionTurn` with the display type a timeline shows and `Text()` for its body. `laser.SessionStateAt` folds the records up to a checkpoint into state as it stood then, and `laser.SessionReplay` brings state saved at a checkpoint up to date.

`Session.Run(ctx, lease, work)` runs work inside the session and ends it by the outcome: completed on success, failed on an error or a panic, which is raised again after the failure is written. `Session.Append(ctx, envelope)` appends one envelope that names this session. `Session.Status(ctx)` reads the session's summary from the session index. `Tag` adds a searchable tag to the start record and `SignedBy(key)` signs the terminal record.

`Session.PendingControl(ctx)` and `CancelRequested(ctx)` read the pause and cancel requests an operator sent on `agent.control`. Inside a handler they follow the runtime's control subscription, and anywhere else they fold the retained control records, so they answer on Apache Iggy too.

`SessionConfig.ContextTurns` bounds a session's context to its newest records, 50 by default, before the token bound applies. `Session.Context(ctx)` reads those turns from the session's lane, and `ContextWith(ctx, policy)` reads them under another policy. `FailOnDeadLetter` fails a session when a runtime dead-letters one of its records. `SkipSourceRegistration` keeps `Bootstrap` from registering the stream as a session source.

## A session's lifecycle rules

Three rules decide what a session will and will not write, and all three exist because a session is addressed by a derived id, so the same label reaches the same session on every run. A fourth is about the lane rather than the id, and is described under "A handle is bound to one lane" below.

**A session that ended refuses to begin again.** Beginning one would append a second start under an id a reader has already seen close. `Resume` opens it anyway, with one exception: a canceled session never resumes, because cancellation is a decision somebody made about the work and reopening it under the same label would quietly overturn that decision. A session started with `Start` holds a conversation minted on the spot, so it cannot have ended and is not read for.

**The first terminal verb latches.** Ending a session a second time the same way repeats the same record rather than appending another, which is what makes ending idempotent for a caller that cannot tell whether its first attempt landed. Ending it a different way is refused: a session that completed did not then fail. The latch is shared with every derived handle, or a caller could contradict an ending simply by taking another one.

**Completing checkpoints the document first.** A reader of a completed session then starts from its final state rather than folding every delta. Only a completion does this. A failed or canceled session stopped partway, so its last consistent document is whatever it managed to write, and presenting a half-finished document as a settled one would mislead. If another writer changed the document since this handle's last write, the session still ends and the checkpoint is skipped, because ending is what the caller asked for and overwriting somebody's later change to save a reader some folding would be a poor trade.

## Consumer filters

`topic.Filters(consumer)` reads a topic through a server-side filter. The server evaluates it and sends back only what matched, so a reader pays for the records it wanted rather than for the whole topic: that saving is in what never leaves the server, and it is why the feature exists.

`Read(partition, filter)` builds one filtered poll. `Count` bounds the matches a page returns and `MaxExamined` bounds how many source records the server looks at: a normal consumer sets it to the count, so a sparse filter returns quickly rather than scanning a whole partition, while a sweep over a cold topic leaves it unset and takes the server's own budget. `Each` walks pages until the partition is exhausted or the caller stops.

Every page is held to the request before a record is read from it. The record body's framing is walked first, because the count in its head is what a decoder sizes an allocation against, and then the page itself: the partition, the incarnation, the policy, the budget, the scan boundary, the record order, and the faults it reports. A page that misstates its boundary is worse than a lost one, since the records it skipped past are never read again.

`Ack` stores progress through the page's safe offset, which is the last record examined rather than the last one matched: a reader that stored only its matches would re-examine everything the filter rejected on the next read. A page that examined nothing offers no progress, so nothing is sent. The server refuses an acknowledgment once the source generation moved on or the consumer no longer owns the partition.

`Preview` says what a filter would select over real records without joining a group or storing progress, and `Explain` turns that from testable into debuggable by saying why each record did or did not match. `Test` judges one record a caller holds. `ValidateFilter` compiles a filter locally with the contract's own evaluator, which costs no round trip and works against a deployment that serves no filters at all.

Filtered delivery is served by the streaming server, so it is gated on the native capability. The saved catalog needs a managed backend and is gated separately: a deployment can filter without saving anything.

## A group and its own filter

A filter belongs to the consumer group that runs it, and `topic.Group(name)` is the handle on that group. `Selector` addresses it the way a filtered read does, by name for a named handle and by id for a numeric one, because the server resolves either and resolving a number here would cost a round trip on every read. `Filters` hands back the match-oriented reader, and `Consumer` joins the group as one load-balanced member.

Apache Iggy identifies a member by the client connection. On a server that resolves group policies, each consumer opened from a connection string joins on its own connection as its own member. On Apache Iggy, or with `FromClient`, consumers share the client's connection and join as one member, so a second member needs its own client. A numeric handle joins an existing group and never creates one.

`group.OpenReader(ctx)` joins the group for filtered reads. `Partitions(ctx)` returns its current assignment. Read an assigned partition with `Read(partition, wire.GroupPolicy()).Fetch(ctx)`, then acknowledge completed pages with `Ack(ctx, page)`, a processed prefix with `AckThrough(ctx, page, offset)`, or one record with `AckRecord(ctx, page, offset)`. After a read that found nothing new, `WaitForRecords(ctx)` returns the partitions that hold a new record. The server holds that wait for up to five seconds when it announces filtered waits. A partition blocked by a record the policy stops on is deferred until `Skip` passes that record. Close the reader to release the membership.

`group.OpenReaderWith(ctx, laser.GroupReaderOptions{...})` opens the reader under options. `Partitions` restricts it to the named partitions when the group assigns them, `Start` is where the partitions held at the open begin, and `Count`, `MaxExamined` and `MaxReplyBytes` bound each page. `MaxUnackedPages` is the most pages with records one partition holds unacknowledged, 1024 by default. `LocalGuard` evaluates every returned record again with the shared evaluator and fails the page on a disagreement, before the caller sees it.

`NextPage(ctx)` returns the next page with a match across every partition the reader holds, and waits while nothing is new. `TryNextPage` does the same without the wait and returns nil when nothing is new. `ReadRound` performs one bounded round and reports whether unmatched work remains, and `ExaminedInRound` is how many source records that round examined. `NextRecord` hands records over one at a time, and `record.Page()` is the page its acknowledgment names. `Owns(page)` reports whether a page can still be acknowledged in the current membership. A record the policy stops on is reported as a `*FilterFaultError` whose `Page` is passed to `Skip`. `MatchedRecord.JSON(into)` decodes a payload and `HeadersMalformed` reports a header block that does not decode.

`Filter` reaches the group's own policy. `Configure` sets it, and `Get`, `Summary`, `Revisions`, `SetRevisionEnabled`, `Preview`, `Test`, `Release` and `Delete` act on it. Each one resolves the binding first and refuses an unbound group by saying so, rather than acting on whichever filter happens to come first in the catalog: the group's filter is found by its owner identity, never by its name, so the same policy saved under a new name is a rename rather than a second filter. An unbound group receives every record, which is a supported way to run one rather than a misconfiguration.

## The saved-filter catalog

`Laser.FilterCatalog()` administers the saved filters and the groups bound to them. `Register` saves the first revision, `Revise` appends one conditional on which is currently latest, and `Describe`, `SetRevisionEnabled`, `Archive` and `Drop` change a filter without changing what a bound group reads mid-page. `Bind`, `Unbind`, `ReleaseBinding` and `ConfigureGroup` move groups between revisions.

Every change carries an operation identity, so a change repeated under the same one returns the recorded outcome rather than applying twice. A retryable refusal is repeated under that identity rather than under a fresh one. A change the catalog has accepted but not yet folded is read back by its identity rather than sent again, and a change whose outcome never arrives is reported as `AmbiguousMutationError` naming the identity to recover under, because an attempt whose reply was lost may still have applied.

Each verb returns what its change produced and where the fold applied it. That position is what a group read can wait for, which is what gives an application that just configured a group read-your-writes through any node.

## Fold snapshots

A long conversation should resume from its last checkpoint rather than replaying every record, so `SnapshotStore` keeps fold snapshots. There are two honest backends and no behaviour difference a caller can see: the managed store is a point read and a point write, and the log-native one works where there is no managed backend at all.

The log-native store partitions by conversation, so one conversation's checkpoints stay ordered, and a read walks that one partition backward from its tail and stops at the first record for the conversation. A hit costs the distance back to the last checkpoint. A conversation that was never snapshotted walks the partition fully before answering, so keep the snapshots topic on retention: its history is checkpoints rather than truth.

Saving one fold's state under another's key would resume a fold from state it never produced, which is worse than resuming from the beginning, so both the save and the read hold a snapshot to the store's own identity.

The key bytes matter beyond this client. The storage key decides where a checkpoint is filed and the partition key decides which partition it routes to, so a client spelling either differently would write a checkpoint where another client never looks. `wire.XXHash32` is the routing hash the streaming server itself uses, written here rather than taken as a dependency because the Go ecosystem's xxHash packages are the 64-bit and XXH3 variants. It is pinned against vectors the Rust client produced.

`StateWith(ctx, scope, store, topics, initial, fold)` and `LoadConversationStateWith` seed a fold from the newest snapshot in a `SnapshotStore`, decoded as JSON, and read only the records past it. A conversation with no snapshot folds fully. `SnapshotFromCheckpoint` builds the snapshot of a state folded up to a checkpoint, `CheckpointFromSnapshot` turns one back into the point to resume from and refuses a stream or topic recreated since, and `ResumeOffsets` lists one past each folded offset.

## The context scope

`Laser.Context(conversation)` is one conversation's working context: append to it, read it back bounded, or render the prompt-ready block. It is never a second store. Appending rides an ordinary publish keyed by the conversation and reading rides the assembler, so what a handler sees is the log rather than a copy that can drift from it.

A read is bounded by turns and, when a budget is set, by prompt size at once, because either alone leaves the other unbounded. The bound is required rather than optional: an unbounded read of a long-lived conversation is a replay nobody asked for, so the full walk has its own deliberate spelling.

`Checkpoint` says where the topics stood, and `FetchSince` reads only what arrived after it, so a caller that already folded up to a moment does not fold the same records twice. Offsets are per topic partition rather than per conversation, because a conversation is a key within a partition and the log has no per-key position to resume from.

`laser.State` folds the history into whatever a caller's state is. The fold is theirs: this knows how to read the log in order and nothing about what it says.

The scope's memory carries the conversation already, so remembering and recalling never repeat it. Durable facts and the knowledge graph stay deliberately cross-conversation, because a fact learned in one session is worth recalling in the next and a dependency edge holds whichever task asked, so the graph comes back unnarrowed.

`ContextScope.AcrossSubconversations()` returns a scope whose reads also keep the subconversations whose root or parent is this conversation. `PolicyVersion(policy)` is the version a context manifest records, `1` unless the policy implements `Version`. `SelectionOf(policy, history)` reports what a policy kept and dropped and the policy that decided. `ContextScope.MemoryWith(namespace, backend)` is the scoped memory on an explicit backend.

A `ContextMessage` read through a scope or a session carries `StreamID` and `TopicID`, the numeric ids of the stream and topic it was read from.

## Claim-checked bodies

A body too large for the log is externalized and replaced by a capsule naming where it went. `AgdxSend.ClaimCheck` does it at send, after the governor and before the signature, so a policy decides on the body the producer meant and the signature covers the capsule the log actually carries. Under the threshold the payload is untouched, byte-identical to no claim check at all.

No store ships with this client. The deployment chooses one, because a body externalized to the wrong place is a body nobody can read back.

`ResolveBody` fetches and verifies. The capsule's digest is the reader's proof that the store returned what the producer externalized, so a mismatch is refused rather than handed over: a body nobody can vouch for is worse than no body at all. The capsule comes off the log, where any writer can forge one, so its reference is held to the contract's bounds before it reaches the store.

## Local state

`InMemoryStore` and `FileStore` are the same contract, so a caller swaps one for the other without changing anything else. What they hold is a process's own state beside the log: dedup keys, cursors, whatever has to survive a restart.

The file store encodes keys into file names, so any key is safe and none can traverse out of the directory however it was built. A key that names nothing is refused by both stores, since an empty name is the directory itself. The directory is narrowed to its owner even when it already existed, because state written into one anybody can read is state anybody can read.

A write stages into a file of its own and renames into place, because a crash partway through a plain write leaves a partial file the next read returns as though it were valid state. The bytes are flushed before the rename, since a rename can otherwise reach disk first, and the directory is flushed after so the rename itself survives. The staging name carries an identity, so two writers on one key cannot clobber each other and whichever renames last wins whole.

## Swarm activity and crash context

`SwarmActivity` folds policy evidence a caller has already read into per-agent counts, so a coordinator answers "what has the swarm been doing" without re-deriving them. It is a pure fold rather than a reader of its own, so it composes with whatever offset-resume convention the caller already uses. Evidence is replayable, so a decision already folded is dropped rather than counted twice, and one naming no acting agent is dropped rather than counted against a name nobody chose. The latest decision is the one that happened last, not the one that arrived last.

`CrashContext` combines three pieces a recovery tool has already read: the recent journal, the dead-letter capsule for the record that crashed, and the last governance decision. `Summarize` renders them in one fixed order every time, because two identical crashes should read identically. A payload is previewed by characters rather than bytes, so a multi-byte character is never cut in half, and control characters are escaped so a payload carrying a newline cannot forge a field of its own.

## Edge authorization

`AuthorizeEdge` decides an external request against a decoded token: the audience first, then the scope. This client parses no tokens. The transport decodes one and this decides on what it decoded, which keeps the token format the deployment's business.

The order is the point. A token minted for another audience is never inspected further, because reading its scopes would be trusting a credential that was not issued to this server. A wrong audience is a hard refusal, where a missing scope is a step-up naming what to acquire, and the challenge escapes it so a scope carrying a quote cannot smuggle in another parameter.

A bridge never forwards the caller's token upstream. It acts under its own enrolled identity and mints a fresh credential, so a compromised bridge cannot replay the caller's.

## Bootstrap and readiness

`Laser.Bootstrap` creates the whole agent topology on the default stream, so an application does not have to know which topics the runtime needs. The session lane takes the retention the caller chose, the heartbeats expire after an hour because they only prove liveness, and the satellites take the server's defaults. It is idempotent, and a topic somebody already provisioned keeps the retention they chose rather than having this one's imposed on it.

The control topic is not created: only operators may write to it, so a client creating it would be claiming a privilege it does not have. The session lane's retention must bound something, because a lane with neither an expiry nor a size bound grows until the deployment runs out of disk and the first anyone hears of it is a server that stops accepting writes.

`RefreshCapabilities` probes again, so a deployment that began serving a surface after this client connected is visible without a restart. `Capabilities.IsOpenOnly()` reports a deployment that advertised nothing beyond the open SDK surface. Re-negotiation starts from what the caller pinned rather than from the last probe's answer, so one failed probe cannot narrow a handle permanently.

`WaitUntilReady` waits for every enabled backend to report itself serving. A disabled backend is not waited on, since nobody asked it to serve, and a deployment that announces none at all is refused at once rather than waited out: there is nothing there that will ever become ready. A backend the deployment observes as degraded is not ready however it describes itself, because the observation is the more reliable half.

## A handle is bound to one lane

A session handle pins the lane it first wrote to: the stream and topic it rides, each by creation time rather than by name or numeric id, together with the topic's partition count. It refuses every later write if any of that has changed. The refusal is a `*laser.StaleLaneError`, and the record never reaches the log.

This is about a stream or topic that was deleted and created again. The replacement takes the same name and can take the same numeric id, so a handle retained across the gap would append to it and look successful, while a reader of the recovered lane would never see those records as part of the session they belong to. The partition count is pinned for the same reason in a different disguise: a record's partition is chosen by hashing over the count, so changing it scatters one session's history across partitions.

Recover by registering the replacement as the session source, where the deployment folds sessions, and opening a fresh handle. The pin is shared by a `Sessions` and every session opened from it, so a new `Sessions` re-reads the lane and writes against whatever is there now, while any handle retained from before keeps refusing. Nothing clears a pin in place, because a handle that quietly re-pinned would be the bug this prevents.

Where a deployment folds sessions, the handle also checks once that the lane it writes to is the session source the deployment has registered, and refuses if they disagree: a lane that is healthy in itself but is not the registered one would take writes that no aggregate read ever returns.

Addressed work that a per-agent topic layout moves off the lane is not checked. It is no more the session lane than any other topic.

## Session layout

`SessionConfig.Layout` decides where a stream's agents put their work, and `Laser.SessionsWith` declares it for the connection, so an agent and a lens opened from different handles route one agent's work to the same place.

The default is one shared lane. `PerAgentTopicLayout` gives each declared agent its own topic and `PerAgentPartitionLayout` keeps work on the lane but on a declared partition per agent, both so one agent's work stays out of another's read path. `SinglePartitionLayout` gives up parallelism for total order across the whole stream.

Only addressed work moves: a command to a declared agent, and the response, error or chunk that answers it. Lifecycle, state, broadcasts, and anything addressed to an agent the layout never declared stay on the lane, so a reader always finds a session's spine in one place. Moving a record off the lane never changes its key, so an agent's own topic is still ordered by session. `Laser.DeclaredTopic` is how a consumer asks where to read, because an agent that kept reading `agent.sessions` under a per-agent topic layout would see its own lifecycle records and none of its work.

`Sessions.Bootstrap` creates the agent topology under the layout: a single-partition layout creates every topic with one partition whatever count was asked for, and a per-agent topic layout also creates each declared topic with the lane's partition count and retention, since each one carries a share of the same work. A layout that declared `agent.sessions` or `agent.control` is refused rather than routed around, because it would send an agent's work to the very lane the layout exists to keep it off.

On a deployment that indexes sessions, bootstrap then registers the stream as a session source and waits, bounded by the publish timeout, for the index to answer. A refused or not-yet-visible registration is reported as `Registered: false` and never raised: the topics are there either way, and an account without session administration rights relies on provisioning. `Sessions.Unregister` stops the indexing again.

## Session leases and heartbeats

`Laser.LeaseSession` takes a lease on one session's liveness. While any lease on a session is held, the process lists it in a heartbeat on `agent.heartbeats`, which is how a deployment tells a session somebody is still working on from one whose process died.

`Session.Begin`, and `Begin` on a resumed session, take one of these leases automatically and return it alongside the start record's receipt: `(*AgdxReceipt, *SessionLease, error)`. A lease failure after a successful start is reported as the error, but the receipt is still returned, because the session did begin. `defer lease.Release()` right after `Begin` is the shape that gets it right, the same as taking one directly. The lease beats at the smaller of `SessionConfig.Heartbeat` and a fifth of `SessionConfig.IdleTimeout`, as in the Rust client.

Go has no destructor, so `Release` is the whole protocol and `defer lease.Release()` is the shape that gets it right. A lease never released keeps its session alive for as long as the process runs. Releasing twice is a no-op rather than a dropped holder somebody else still has, and a terminal verb on the session releases nothing: one part of a process ending its work says nothing about another part's.

One registry per connection serves every lease, so a process holding a hundred sessions publishes one heartbeat per stream per tick rather than a hundred. Several leases beat at the shortest interval among them, because the holder that asked for the tightest liveness window is the one that needs it. The loop ends when the last lease is released, so a process holding none publishes nothing. It notices at its next tick rather than at once, as it does in Rust: a newly taken lease with a tighter interval applies from the next tick, and releasing a long-interval lease leaves one sleeping goroutine until that interval elapses. `LeaseRegistry.Loops` reports whether the loop is still running.

A lease is scoped to the stream incarnation it was taken on, and the stream's creation time is re-read every tick. A stream recreated since then holds none of these sessions, so nothing is published for it: claiming them alive would keep a deployment waiting on work nobody is doing. A failed read or publish is retried on the next tick and never fails a session, because a missed heartbeat is a liveness signal the deployment did not get rather than work that went wrong.

The Rust client additionally re-checks the stream generation at the send boundary, inside the lane-identity guard described under [A handle is bound to one lane](#a-handle-is-bound-to-one-lane). Here the check is the one in the heartbeat loop.

## What a session fold reads

Lifecycle and state records stay on the shared lane in every layout, but addressed work and its replies move under a per-agent topic layout. So a fold reads the lane *and* every topic the layout declares: reading the lane alone would miss the usage a model call spent and the agents a session handed work to.

Records from several topics are ordered by append time, with the topic and then the offset breaking a tie, which is the order the other clients assemble in. A fold that replayed them in the order the topics happened to be read would not be replayable, and two readers of one log could reach different documents.

## Session budgets

A budget is a token and cost ceiling a session's start record asks for. `Session.OverBudget` and `Session.BudgetBreach` report whether its usage has passed one.

It is a cooperative limit, not a hard spending cap. The answer is folded from records that are already written, so it is eventually consistent and a session always passes its ceiling by a little before anybody notices. A deployment that indexes sessions answers from the index, whose own flag decides: when its counters do not line up with that flag the breach is reported by its numbers rather than denied, because the index said the session is over. On open Apache Iggy, or for a session the index has not caught up to, the retained lane is folded instead: an index that cannot answer yet is not a session with no budget. A read refused for any other reason surfaces rather than quietly falling back.

A lane whose start record retention has removed reads as within budget, since the ceiling it was given is no longer on the log and inventing one would end sessions nobody limited. Tokens are checked before cost, the order the index applies, so both answers name the same dimension. Usage sums input and output, and saturates rather than wrapping: a total that wrapped would read as under budget, which is the one answer a runaway session must not get.

`Session.FailOverBudget` ends a session with the pinned reason `budget`, so a reader can tell a budget stop from any other failure without parsing a message. The failure is not retryable: the same work against the same ceiling spends the same again.

`BudgetGate` checks whether a session's work can reach a handler. The agent runtime shares the verdict within a poll batch. When a session exceeds its budget, the runtime ends it once and skips its work. A failed budget read leaves the verdict uncached.

## Submitting work

`Sessions.Submit` hands a new session to an agent with a first command. The submitted status is written before the command, so a session never has work on it before it exists on the log, and the command is marked submitted so the receiving agent knows to mark the session working when it picks it up.

`SubmitBuilder.ID` names the session the submission will write, before it writes it, which is what lets a caller watch the session it is about to create. A label derives that id from the stream, the namespace and the label, so the same label reaches the same session and a nightly job resumes rather than starting a new one.

A labeled session that ended completed or failed resumes. One an agent or an operator canceled is refused: a cancel is a decision somebody made, and resuming under the same label would undo it quietly. A forced cancel leaves no lane record, so the control route is read as well.

When the command cannot be published after the status landed, the session is ended failed and the publish error is returned. A session that still reads as submitted with no work in it is worse than one that visibly failed. Ending it is best effort: the publish failure is what the caller asked about, and reporting a second one in its place would hide it.

`FoldLaneLifecycle` folds a session's lane into whether it started, the terminal status it reached and the budget its start asked for. A start after a completed or failed end resumes the session and takes that start's budget, since a resumed session runs under the ceiling its resumer chose. A start after a cancel does not resume.

A session with no idle timeout can only be reclaimed by an operator noticing it, so `SessionConfig.IdleTimeout` has a default of five minutes rather than being left absent.

## Model and tool calls

This client never calls a model. The application calls its provider and records the call through the session, so the log holds what was asked and what came back without the SDK sitting in the path.

`Session.Model` records the request and hands back a call to finish when the provider answers, and `Session.Tool` does the same for a tool. Both records are marked recorded, so the agent's own runtime never picks its own timeline entry up as new work addressed to it. `Session.RecordModelCall` records a call that already happened in one go.

A response that names no duration is timed from when the call was opened, because the application asked this handle to hold it and the elapsed time is the only measurement there is. A call recorded after the fact was not timed here at all, so it records zero rather than a number this client invented. The model that answered is recorded beside the one that was asked for, since they differ often enough to matter.

Tool arguments and JSON prompts are redacted before they are recorded: `authorization`, `api_key`, `token`, `password`, `secret` and `cookie`, at any depth, whatever their case. Only the values go, so a reader still sees what was passed. It is a convenience and not a guarantee, and `Session.Redact` replaces it: a nil redactor records arguments unchanged, which is what a caller that redacts upstream wants. A prompt that is not JSON cannot be read key by key, so it is recorded as it stands.

A failure carries the structured error body rather than a message a reader would have to parse, and a failed tool call names its tool so a reader can tell which one failed.

## Session state

`Session.State` is the session's state document: RFC 6902 patches on the session lane, applied in lane order, with snapshots as checkpoints. `Set`, `Patch` and `Replace` write, and `Get` folds the lane back into the document.

A successful append does not prove a patch applied. Another writer may have reached the same revision first, and a reader learns the outcome from the folded state rather than from the receipt. Each delta names both the revision it continues from and the delta that reached it, which is what tells a writer that lost a race from one that did not: two writers can reach the same revision, only one reached it with this parent.

The cursor is shared by a session handle and every handle derived from it, and held across each publish, so one process's writes land in revision order. A failed publish clears it, because a record that reached the log with a lost acknowledgment looks exactly like one that never landed, and naming the old revision again would write a delta the fold skips. The next write reseeds from the lane instead.

`Snapshot` writes the whole document, and refuses with a stale session error when the session's document is no longer this handle's, so a handle whose earlier write did not apply never overwrites a later change. One is written automatically every 64 deltas, because without a checkpoint a long-lived session's state costs more to read every time it is written to. A failed automatic snapshot is not a failed write: the delta is on the log and the next write retries the checkpoint.

`FoldSessionState` is the fold itself. The first snapshot is the baseline. A later one counts only at the revision the fold has reached and with the document it holds, so a second writer's checkpoint of a document the session never reached changes nothing. A record that does not decode, or a snapshot carrying a number no JSON reader holds exactly, is skipped the way the session index skips it. `Complete` is false when retention removed the records the document starts from, which is what separates a document that is empty from one whose beginning is gone.

A writer continues from the fold of the retained lane. When the lane no longer holds the document's baseline and the deployment indexes sessions, it continues from the managed state view instead, read with one walk of the roster, and keeps the fold when the view does not answer, does not know the session, or is behind the fold. `Laser.ReconstructState(ctx, conversation)` reads the document the same way and returns nil until a state record exists.

## Operator control

`ControlRequestOf` reads what a record on `agent.control` asks of a session: a pause, a resume or a cancel. A forced cancel is a canceled session status on that topic. A request addressed to another agent is not this agent's to read, and a reader that is nobody in particular, an operator console say, reads them all.

A pause applies to every agent it reaches. Its participant list only decides whose acknowledgment it waits for, so an agent the list omits still pauses but is not named by the request.

`ControlBook` folds those requests per session. A resume lifts the pause it follows. A cancel is terminal and nothing lifts it. Records are ordered by offset within a partition, so a replayed earlier record cannot undo a later one, and a record on another partition is taken as newer rather than compared against a sequence it is not part of.

Every session's control records have one home: the `agent.control` partition its conversation key hashes to, which is the rule the managed backend applies too. A request that arrives anywhere else is flagged and never applied, here or there, because it cannot be ordered against the records that belong to the session.

A book nobody follows does not answer at all rather than answering stale, and a session whose history has not been read is reported as unknown rather than as having no requests. Live requests that arrive while that history is being read are held and replayed on top of the fold, since applying a resume to nothing would report it as though no pause preceded it.

A spawned agent follows `agent.control` and parks a paused session's work on the session lane before committing its offset. On resume, it handles the held records in source order. Enable `RuntimeOptions.EnforceControl` when supplying your own record source to `AgentRuntime`.

The gate reads live state, so the follower loads the topic's history before reporting the book live. Until then the book cannot tell "not paused" from "not read yet", and a gate acting on that difference is exactly how a paused session's work gets through.

Parking is keyed by the source position, not the record id, so a record redelivered after a crash between its parking and its commit is already held rather than held twice. A held record whose source expired, or whose topic was recreated, cannot be replayed. It stays listed as held rather than being quietly forgotten.

`Session.Parked(ctx)` folds the lane into what is still being held and by whom: a parked record adds held work and an unparked one takes it away. Two roles holding one record are two pieces of held work, so one role's resume does not clear the other's, and a parking record that will not decode makes the account incomplete rather than silently short.

Held work is rebuilt from the lane when the runtime starts, so a process that restarts finds what an earlier one parked and hands it back on resume. The scan runs only once some session has a pause history at all, so a deployment that never pauses pays a book read rather than a lane read. Only this role's own parkings are taken: another role's held work is that role's to replay, and a record already written off the lane is done whichever order the two records come back in.

A role acknowledges the control request it is acting on, on the session lane, carrying the position of the request it answers. That is what lets an operator see which agents actually stopped rather than only that a pause was asked for, and tell this pause's acknowledgment from the next one's. One request is acknowledged once however many records arrive. A resume is acknowledged only by a role that acknowledged the pause: an agent that never stopped has nothing to say it has started again.

The runtime refreshes held work when the consumer's assignment changes. Custom record sources can expose `Partitions() []uint32` to participate in assignment tracking. The runtime rechecks routing, deadlines, fences and any configured signature verifier before delivering a parked record. A correlated decision can resume a parked approval after restart.

The folding rules are the same in every client, which is what lets a Go process and a Rust one agree about whether a session is paused.

`Sessions.Control` is the write side. An operator names itself with `AsOperator`, which is required: a control record nobody signed for is a record nobody can be asked about. `SignedBy` signs it, so a verifying reader can prove which operator sent it. Only accounts with send permission on `agent.control` can use any of this, which is what separates an operator from the agents doing the work, since a handler cannot pause its own session by writing a record.

`Pause` names the agents whose acknowledgments complete it: the set given to `Participants`, or else the agents the session lane shows working on the session. A caller that named nobody meant nobody, so an empty set stays empty rather than having the lane read behind its back. The lane set is sorted, so two operators reading one lane write the same request. Model and tool calls are the session talking to something that is not an agent, and the control operations are the operator's own, so neither makes anyone a participant.

`Cancel` asks and `ForceCancel` tells. The forced one ends the session as canceled without its agents, for a session whose agent is gone: nothing acknowledges it, so a session whose agent is in fact alive would find its lifecycle ended underneath it.

## Agent sends, redrives and replays

`SendAgent` appends to an agent topic with a provenance the caller built. It is the low-altitude form: `Agdx` reads better because it derives the provenance from the envelope, so the two cannot disagree about who sent what to whom. A record on a shared lane that addresses nobody is addressed to every agent, so a role group bound to the addressee filter still receives it.

`SpawnSubconversation` derives a child conversation carrying both its parent and the root. Without the root, a reconstruction three levels down would have to walk every parent to find out what the work was for.

`RedriveDeadLetter` replays a dead-lettered record so a fixed handler reprocesses it. The payload goes back verbatim, but the idempotency key is re-keyed by the position the original sat at: a consumer that still remembers the original would otherwise drop the copy as a duplicate, while a key derived from the position means two redrives of one capsule still agree. A signed record is refused, because its signature covers both the record id and the idempotency key a redrive has to change, and one past its deadline is refused because it would only dead-letter again.

A capsule names its own source, and any writer that can append to the dead-letter topic can forge one, so a redrive is confined to this connection's own stream rather than copying into a stream the caller did not choose.

`ReassembleChannel` rebuilds a chunk stream from the log. This is what offset replay does that a server-sent event stream cannot: a finished stream reconstructs deterministically after the fact, with no transcript store anywhere. Chunks are ordered by sequence, so it reassembles the same way whichever partitions the stream was keyed onto, and a stream whose producer never ended it gets a synthetic ending once the bound its opening chunk declared has passed.

`ChunkAssembler` is that reassembly on its own, pure and clock-free so the same code serves a live stream and a replayed one. A redelivered chunk is dropped and counted because a consumer is at-least-once, a missing sequence ends the stream because the log is in order within a channel, and the first terminal wins.

`Consumed` asks whether a consumer has committed past a record. A consumer that never committed reads as not yet consumed, and so does a read the server could not authorize, so a caller deciding a reassignment on this needs verified read access: otherwise a misconfiguration reads as "not consumed" and drives a reassignment nobody needed.

## The change feed

`Laser.Watch()` reads one record per committed projector batch, for a binding that opted into notifications. It turns "query after my data landed" from sleep-and-retry into await-then-query, and it composes with read-your-writes rather than replacing it: the feed says a batch landed, and the applied frontier says a read will see it.

`Index` narrows to one materialized index and `FromEnd` starts at the feed's current end, for a caller awaiting its own write rather than catching up on a backlog. A deployment that does not publish the feed is refused rather than left polling a topic nothing writes to, because waiting on silence looks identical to waiting on work that never comes. Under stream tenancy each stream's feed rides its own ops topic, so a scoped handle reads its own.

`Missed` counts what retention removed before the reader reached it, measured from where the reader started or resumed. Above zero means a caller should read the view itself rather than trust the feed to have told it everything. A partition the reader opened at the end of while it held nothing is anchored, so its first record is not mistaken for the end of a range of lost ones.

`Skipped` counts feed records that were not change records. The changes topic is overridable, so other traffic on it is stepped over rather than allowed to wedge the reader. The version is read as an optional field precisely because absent and zero are different: a payload that is not a change record is skipped, while one claiming version zero is a change record this build cannot read.

A change record of another version stops its partition where it sits. The changes before it come back, the partition stays parked at it, and a poll with nothing else to return says why. This build never guesses at a newer record and never steps over one, and one partition stopping does not stop the others.

`PollWithin` waits for the first change to land, and `Each` walks until the reader is caught up. A caller that stops a walk early is left positioned at the first change it did not take, so the offsets it persists mean what they say: without that, stopping mid-batch would silently skip the rest of it.

## Publish recovery

A publish is retried under the schedule every Laser client shares, documented in [publish recovery](../../docs/publish-recovery.md): a 60 second attempt timeout, three attempts after the first, and a delay starting at 250 milliseconds that doubles per attempt, stops at 30 seconds, and is spread over its upper half so clients that failed together do not come back together.

Only a lost or stalled link and an expired session are retried. A refusal the server reasoned about, a malformed request or a missing topic will fail the same way however many times it is sent, and retrying it only delays the report.

Publishing is at-least-once: a reply lost after the server applied the append cannot be told from one it never received, so a resend can duplicate a record. A caller that cannot tolerate that sets the retries to zero.

One deadline covers the reconnect and the attempt together, so a reconnect that hangs cannot eat the budget and leave no time to send. An attempt that timed out is followed by a cleanup whatever happens next, because a cancelled read must not leave its late reply on a shared connection for the next caller to decode as its own. A caller's own deadline is reported as the caller's rather than as a stage this client timed out: retrying is not the answer to a caller that has stopped waiting.

A schedule the client could not honour is refused before anything is dialed, since a client that connects and then fails on every send is harder to diagnose than one that refuses to start. `Options.Publish` pins it, `Laser.WithPublishOptions` derives a handle with its own, and the `LASER_PUBLISH_*` variables are read when neither does. Configuration a caller wrote is never overridden by what is ambient.

## Direct producer

`topic.Producer()` builds a synchronous producer. `Build(ctx)` creates a missing topic by default, and `CreateStream`, `Partitions`, `ReplicationFactor`, `ExpireAfter`, `NeverExpire`, `MaxTopicBytes` and `UnlimitedTopicSize` shape what it creates. Each send returns the confirmations the server supplied.

`Send(ctx, payload)` publishes one record with the producer's routing. `Routing` picks the partition for a whole send: `BalancedRouting()`, `KeyRouting(key)` or `PartitionRouting(id)`. `SendKeyed`, `SendToPartition` and `SendWithRouting` route one message, and `SendBatch` and `SendBatchWithRouting` route a slice. `laser.ProducerHeader(key, value)` builds a typed header for a producer message from a string, bytes, a boolean or a sized integer. A batch longer than `BatchLength`, 1000 by default, goes out in several appends. When one of them fails the error is a `*PublishFailure` holding the confirmations already received and the records not confirmed.

`Flush(ctx)` waits for the sends in flight. `Close(ctx)` waits for them and refuses later sends and flushes. `Statistics()` returns the counters described in [producer statistics](../../docs/producer-statistics.md).

`topic.PublishBatch()` collects records for one send. It validates every record before sending, so a record that cannot be published fails the batch with nothing written. `ExtendJSON`, `ExtendMsgpack` and `ExtendEncoded` add one record per value, and `IsEmpty` reports a batch without records.

## Batching producer

The Apache Iggy Go SDK has no high-level producer, so `topic.Batching()` builds one here: the batching, the linger timer and the failure accounting are this client's own. The unbatched publish path is untouched, so batching is opted into rather than imposed.

Many records become one append, which is the point: the per-call overhead is paid once rather than per record. The batch flushes on whichever of the record count, the payload bytes or the linger trips first, so a trickle of records never waits for a batch that never fills.

One partition key per handle by construction. A batch is flushed whole under one partitioning, so ordering within a key is never silently interleaved by records that went out under another. Without a key the server's balanced partitioner spreads each flushed batch.

A failed append says what landed and what did not. A caller that replayed everything would double what is already on the log, and one that replayed nothing would lose what never arrived, so `PublishFailure` names the confirmations that arrived and the records without one. Those records keep the ids their attempt used, so a resend stays deduplicated on a server that deduplicates.

A failed linger flush has nobody to tell, so it keeps its failure and the next `Send`, `Flush` or `Close` hears it exactly once. The timer keeps running: one failed batch does not stop the producer. A `Send` that finds a kept failure refuses its own record rather than queueing behind a batch the caller has not heard about yet, and the report lists both the lost batch and the refused record.

`Flush` is the guaranteed path and `Close` is the graceful one: the timer is signalled and waited for rather than cancelled, because a cancelled flush would drop a batch already taken out of the queue. Each record is governed before it is queued, so a policy that refuses one refuses it before it is batched with records that were allowed.

## Materialization

`topic.Materialize()` is the short path through those registries: a topic goes from a log to a queryable table in one call, rather than through a projection, a binding and an index the caller keeps consistent by hand. With nothing else set it materializes every record whole into an index named after the topic.

`Typed` materializes the fields a projection extracts instead of whole records, `Index` and `ProjectionID` name what it writes, `Backend` says where the index lives, and `Retention` says how long rows stay.

`InvalidRecords` and `Oversize` both default to holding the partition, because skipping silently is how a table quietly stops matching its log. A keyed view keeps its rows unless told otherwise, since the row is the current value rather than one event among many, and a retention that expired one would answer as though the key had been deleted.

`Apply` registers and binds under derived identities of one setup, so a retry of either step applies once and the pair cannot half-apply under two unrelated identities. Changing the setup mints a fresh identity, so a corrected setup is never refused as a replay of the one it corrects, and `OperationID` pins one for a caller driving its own recovery across a restart. The binding is checked locally first, because the register goes out first: a binding the backend would refuse must not leave a registered projection behind with nothing routing into it.

`Status` reports each followed binding's progress, and a handle tied to one source reports that source alone. A binding that was removed reads as gone, the same as a projection that never existed, which is what a caller waiting on a setup needs to be told.

`Wait` reads until every followed binding reaches a readiness level, a record or a gap holds one, realization fails for good, the binding is gone, or the bound passes. Each pause doubles the one before it, so a setup that lands quickly is noticed quickly and a slow one is not polled to death.

A level past realized is measured against the heads the first complete read captured rather than against whatever the source holds now, because a source that keeps growing would otherwise never be caught up with. A source recreated during the wait drops that target, since the heads it captured no longer exist, and a binding that was in the target and vanishes holds the wait rather than letting the remaining ones declare it finished.

## Projections, bindings and schemas

`Laser.Projections()` administers what a deployment materializes from a topic and into what shape. `Register` declares a projection and the deployment creates its backing table. `RegisterGraph` declares a graph projection, which materializes nodes and edges rather than a row table, so the two are not interchangeable: each refuses the other's kind rather than registering something that creates the wrong thing. `Drop` stops a projection being applied and leaves the rows it already wrote alone, because dropping a plan is not deleting data.

`Laser.Bindings()` routes sources into projections. `Apply` starts a projector for a `(stream, topic)` source, and `Remove` stops it. Naming one projection scopes the removal to it rather than to the whole binding.

`Laser.Schemas()` administers the writer-schema registry. `Register` is synchronous and answers the allocated id, because the id is what a producer stamps on every record and a caller cannot proceed without it. `Drop` tombstones rather than deletes: records already stamped with the id keep decoding, and the id stays reserved against re-registration under a different definition.

Writes append control commands, which the deployment applies asynchronously, so a caller observes the apply by reading the registry back rather than by the append succeeding. Each carries an operation identity, so a transport retry is applied once, and `PublishControlAs` lets a caller choose the identity so a retry that outlives the process still applies once. Reads are registry browses, served off the snapshot by a managed backend. Browse has no topic transport, so a deployment without a backend is refused rather than left waiting for a reply nothing will send.

Every resource name is scoped to the handle's stream, which is how two streams in one deployment keep separate registries, and listings come back under the names the caller wrote so a caller never sees scoping it did not ask for. `Options.ResourceNaming` opts out.

## Signing and keys

A record on a shared topic says who sent it. `AgdxSend.Signed(key)` makes that something a reader can check: the signature covers the envelope with its own signature cleared, prefixed by the contract's domain separator so an AGDX signature can never be replayed into another protocol.

The interpretation attributes the record is stamped with, its content type and wire version, are bound into the preimage too. Without that, an intermediary could flip the codec on a signed record and the same bytes would read as something else entirely while the signature still verified. A signature that bound nothing reproduces the preimage a signature written before contexts existed covered, so an old record still verifies.

`KeyRegistry` holds enrolled verifying keys, each bound to the authenticated principal that enrolled it. Verifying returns that principal, and a caller compares it against the server-stamped identity. That comparison is the authorship binding: a key proves the principal it was enrolled under, never the source an envelope claims for itself. A key carries a kind, so a privileged control fact can require an operator key and an enrolled agent cannot quarantine another agent.

A key also carries a window, so a rotation overlaps rather than cuts over: the old key stays valid long enough to cover the records already in flight. `Verify` reads no clock and checks the bytes alone, `VerifyAt` holds the key to its window, and `VerifyObservedAt` additionally refuses a signature whose bound context differs from what the record carried, before any cryptography runs. Revocation disables a key whatever its window says.

`VerifyDelegation` reads whom a signer acted for. The claim rides inside the signed span rather than on a header, so a signer cannot forge it and an intermediary cannot rewrite it. The server authorizes only the authenticated caller, so a caller intersects the two principals' grants itself: an agent must never exceed the user it acts for.

`Laser.KeyRegistry()` is the managed sibling, storing lifecycle-aware records in a reserved key-value namespace keyed by canonical key id. Enrollments are conditional on what is already there, so two administrators cannot both believe they won. `Snapshot` reads them into the in-memory registry a verifier folds with, skipping any entry that does not hold up: one corrupt enrollment must not stop every other principal's records from verifying.

The preimage is pinned against the Rust client: `sign_test.go` carries signature vectors Rust produced for the same envelope and seed, so a change to any part of the preimage shows up as a signature no other client can verify.

## Action governance

`Laser.WithGovernor(governor, mode)` derives a handle whose publishes, AGDX verbs, requests and memory writes run a policy hook before the effect. It is defense in depth at the effect boundary: authorization on the managed surfaces stays server-owned, and this hook cannot widen it. It can only stop or narrow what the client was already allowed to do.

The governor sees who is acting, where the effect lands, the body about to be published, and the session's running counters. Every agent-written field it reads is a claim rather than a fact: a declared purpose is advisory unless the envelope is signed.

The vocabulary is broader than allow and deny, because a policy that can only refuse forces every nuance into a refusal. `Allow` runs the effect and records nothing. `Observe` runs it and records evidence. `Modify` replaces the body, applied before signing so a signature always covers what the log carries. `Block` and `Defer` reject, the second retryable. `StepUp` rejects naming the scope an approval must grant, so a handler knows what to ask for.

`GovernorObserve` is a shadow rollout: every decision is recorded with what enforcement would have done, the effect always runs unmodified, and a failed evidence write only warns. Observing never impacts production, a failing governor included. `GovernorEnforce` applies the verdict, and a governor that fails or answers nothing fails the effect rather than failing open. Under enforcement a proceeding decision whose evidence did not land fails the call, because a governed effect nobody recorded is an ungoverned effect. A denial stands whether or not its evidence landed.

Each decision is appended to the audit topic as an AGDX event, sealed with BLAKE3 over its own canonical encoding and chained to the previous decision in the same conversation. `VerifyEvidenceChain` checks a chain read back: a reordered, dropped or edited record breaks it, which is the point, since evidence nobody can check is not evidence. Chain heads are process-local, so a restart or a retention eviction starts a new chain rather than breaking the old one, and chains of several processes interleave on one topic. Evidence goes straight to the log rather than back through the governed verbs, so a blocking policy cannot stop its own decision being recorded.

`laser.NewQuorumGovernor(policy)` composes independent voters and is itself a governor, so it enrolls exactly like a single one. Voters run concurrently and each is bounded, so one hung voter never stalls every governed effect: a mandatory one that does not answer blocks and any other abstains. When the quorum is met the composite is the strongest affirmative, in order modify, observe, allow. When it is not, it is the most actionable denial, in order block, step-up, defer. Every voter's verdict is recorded, so the audit trail names who said what. Nothing here reaches a durable log: it is an in-process combinator, not the crash-safe propose, vote and commit lifecycle a durable version would need.

`laser.NewSwappableGovernor(initial)` is itself a governor whose policy can be replaced while the client runs: `Swap` installs the next one and returns the one it replaced, `Current` reads it. A swap binds from the next `Decide` onward. A decision already in flight finishes under the policy it read, and nothing already evaluated is reinterpreted. That is the same non-retroactive rule a durable intent's policy version gives, so an audit trail stays readable against the policy that was actually in force when each entry was written. Go has no `RwLock<T>` type as such. A `sync.RWMutex` over a stored interface value is the idiomatic equivalent of what Rust wraps.

## Approvals

`Requester.ApprovalGate` asks a named approver and waits for the answer. The reply mints an `ApprovalGrant`, which is the only way to hold one: a grant cannot be constructed from a message the approver never sent, and a reply from anyone else decides nothing.

`AgdxSend.WithGrant` carries a grant with an effect. A governor stepping up in the grant's own conversation is answered by it rather than raising the step-up again, which is what lets a handler gate once and then act. A grant from another conversation approves nothing, because an approval given for one piece of work is not an approval for another. The step-up is still recorded with its scope: the approval answered it, it did not make it disappear.

A spawned agent can park an approval when the decision returns to its consumed topic. The prompt is published, the work is held on the session lane, and the runtime commits its source offset. A decision replays the original work. With a verifier configured, the decision must carry a signature from the named approver.

Three things follow from that, and a handler has to be written for them. It runs **again** from the top, so whatever it did before the gate happens twice and has to be idempotent. The session's **later work is held behind** the approval and runs after it, so the decision resumes the session as it left it. And an approval nobody answers **expires**: when the deadline passes the handler is replayed anyway, and the gate returns a `*laser.ApprovalExpiredError` for it to decide what an unanswered approval means for its own work.

Everywhere else the gate waits in place, and a gate held open holds its caller open. That is what `Requester.ApprovalGate` does, and what `AgentCtx`'s does under a runtime with no pause gate.

## Durable intents

`laser.NewIntent` proposes an effect that several principals have to agree to. The intent, the `laser.Vote` records that name it, and the `laser.IntentDecision` the fold seals are ordinary typed records: publish them like any others. This surface defines record shapes and a pure fold, not a new wire protocol and not an implicit log integration, which is why nothing here reaches a topic on its own.

`Proposer` and `Voter` are record claims. A deployment that needs trusted authorship enforces a signed-principal or topology-isolated profile, because the fold cannot authenticate who appended a record. This composes with action governance rather than replacing it: a governor can require a committed decision before it releases an effect.

`laser.Decide` folds ballots into a decision, or reports none while the outcome is still reachable before the deadline. Each ballot is windowed and ordered by the time the reader observed it, the record's server timestamp, never by the voter's own stamp, so a late ballot cannot claim an earlier time. Ballots for another intent, body or policy version are ignored, as are ballots from voters the intent never enrolled. Identical repeats are idempotent, and one voter contradicting itself aborts. A mandatory voter must allow before any quorum commits. The valid set is sorted before folding, so input and replay order change neither the decision nor its recorded evidence.

`IntentDecision.Authorizes` is the check before an effect. An aborted decision is valid evidence but authorizes nothing, and a committed one authorizes only when the votes it carries actually meet the intent's quorum, so a decision record stripped of its evidence is refused rather than trusted.

The body hashes as one canonical CBOR byte string under the name `body`, then BLAKE3, then lower-case hex, which is what every other client hashes. On the log the body itself rides as serde's array of integers instead, and the SDK-level ids ride as their text. All three records are pinned byte for byte against vectors the Rust client printed, in both the CBOR and the JSON form.

## Bridges

`bridges` maps external agent interfaces onto AGDX. A bridge is a translation and nothing more: the log stays the source of truth, so a task survives a bridge restart and remains replayable.

`NewA2ABridge(laser, source, requestTopic, replyTopic)` is the A2A task adapter. `Submit` publishes the params as a typed AGDX command on a fresh conversation, carrying the foreign request byte-identical so an agent reads exactly what the A2A client sent. The task's identity is that conversation and its correlation is derived from it, which is what makes `Task` and `Cancel` stateless: a bridge holding no table answers for a task it is handed the id of, including one another process submitted.

`Task` replays the log forward from the time the id was minted, less five minutes of clock slack, over at most 32 passes per partition. A task nothing answered yet is working. One whose scan could not reach the end of the topic is unspecified rather than working, because the answer may already be there and "working" would be a claim the scan did not check. `Cancel` writes the canceled terminal for a task still in progress and refuses one that already ended: a terminal state is a fact on the log, and a second one after it would make the task read differently depending on which record a reader saw last.

With `WithVerifier` only a signature the registry accepts settles a task, and only from the agent the command addressed or from the bridge's own cancel. An unaddressed command is a broadcast, so any enrolled signer counts for it. Without a verifier every answer counts, which is right for a trusted lane and wrong for a shared one.

`HandleRPC` answers a JSON-RPC call without any HTTP around it, so the shell that serves it is a shell. A message naming a live task continues it in its own conversation addressed to the agent it already went to, rather than starting a second task nobody connected to the first. `SendStreamingMessage` answers unsupported rather than unknown, because the card advertises no streaming: AGDX chunk channels ride the log, not SSE. `Card` renders the v1.0 discovery document and is refused until `WithPublicURL` names the endpoint.

`WithTaskBinding(key, ttl)` scopes every task id to the caller that created it. Without it a task id is a bare conversation, and anyone who learns or guesses one can read and cancel that task: the log is the authority on what happened and has no opinion on who may ask. A bound id is `<task>.<expiry>.<mac>`, keyed BLAKE3 over the caller, the stream's creation time and the task, so an id that is not this caller's, has expired, or was minted on an earlier incarnation of the stream reads as a task that does not exist rather than as a refusal that confirms it. `bridge.Caller(principal)` names who is asking, and a bound bridge called without one refuses rather than minting an id nobody could open.

`Handler()` serves the bridge over HTTP: the JSON-RPC endpoint at `POST /` and the card at `/.well-known/agent-card.json`. It is a shell over `HandleRPC` and `Card`, so serving the bridge another way answers identically. The endpoint is guarded by `WithAllowedOrigins` and names its caller through `WithCallerFrom`, whose error is a 401 rather than an unauthenticated call. A non-JSON content type is 415, a body over 2 MiB is 413, a body that is not JSON is 400, and a wrong method is 405 with what is allowed. The card path is public discovery and unguarded: a client has to read it before it can be an allowed origin.

`SubmitIn`/`SubmitInTo` submit a task as a child session of a parent, in the tree rooted at root: the child's submitted start lands on `agent.sessions` with the ancestry before the command, and the command carries it too. The handling agent ends the child. `MCPBridge.CallToolIn`/`CallToolInTo` are the MCP peer, and end the child themselves: completed on a tool result, failed on a tool error.

`A2ABridge.SignedCard(key)` returns a discovery card with a detached Ed25519 signature. `WithSigningKey` also signs cards served at the well-known HTTP path. `laser.VerifyCard` accepts the original JSON so the signature covers extension fields. `MCPBridge.Handler()` serves the MCP endpoint.

A2A task states map both ways, and a word outside the vocabulary is refused rather than guessed, since an edge sending one is speaking a protocol this bridge does not implement. A paused task reads as working, because A2A has no paused state and reporting anything terminal would say the work ended when it has not.

MCP marks a failure as one rather than returning it as ordinary content, since an edge that cannot tell them apart shows an error to a user as though it were the answer.

AG-UI renders envelopes as the events a front end consumes. A chunk stream's purpose is declared only on its opening chunk, so `AgUiStream` remembers it per channel and forgets a channel once it closes.

A bridge that forwards a call to another bridge's HTTP endpoint carries an ordered hop path under `BridgeHopsMetaKey`, so the next bridge can refuse a loop. `EnterBridge` appends this bridge and refuses a path that already names it: the call has been here before, so handling it again is the loop. A hop the configured path already names is not repeated, because a path listing one bridge twice would read as a loop that is not there. Anything but a list of strings under that key is refused rather than read past, a null included: a guard the sender blanked is not no guard.

`OriginAllowed` guards a bridge HTTP endpoint a caller serves. A request with no `Origin` header comes from a non-browser client and passes, because the header is a browser's statement about itself and its absence is not a claim to check. A browser request passes when its origin is allowlisted, or when it is same-origin with a host no DNS answer can point a foreign page at: an IP literal or localhost. That host restriction is the point, since a rebound page sends its own name as both origin and host and a same-origin check on any other host would wave it through.

Like the TypeScript client, this is a native port rather than a binding over the Rust crate. The Rust crates expose no C ABI, cgo would cost consumers cross-compilation, and the Apache Iggy Go client is itself pure Go.

`bridges.CommandFromMessageSend` and `bridges.ToolCallFromRequest` are the mappings the bridges apply, for an edge that builds the envelope itself.

## Layout

| Package | Owns |
|---|---|
| `wire` | the contract: codes, headers, topics, envelopes, queries, projections and bindings, logical and writer schemas, filters, sessions, the HTTP surface, and a typed client over a caller-supplied transport |
| `laser` | the client: connect, the connection string grammar and TLS resolution, the capability negotiation, the managed command band and the typed surfaces on it, and the stream, topic and publish facade. The grammar is parsed here rather than by the Apache Iggy Go SDK, because the TLS decision is Laser's and because the accepted grammar has to match the Rust and TypeScript clients |
| `bridges` | the A2A task adapter and its HTTP handler, the MCP tool-call adapter, and the AG-UI mappings. |
| `internal/cborform` | an order-preserving CBOR value tree and a shortest-form encoder. The CBOR and JSON encoders both walk it, so field order and the skip rules cannot drift between the two encodings |

## Transport

The client reaches a deployment over TCP, plain or TLS, because the [Apache Iggy Go SDK](https://pkg.go.dev/github.com/apache/iggy/foreign/go) implements that transport alone. QUIC and WebSocket are served by the Rust and Python clients, and a connection string naming one is refused with that hint rather than silently downgraded.

A `*.laserdata.cloud` or `*.laserdata.com` host is encrypted automatically against the root CA this package carries, a copy of `sdk/certs/laserdata.crt` that a test checks byte for byte. That SDK reads a CA from a path, so the bundled root is cached in an owner-only per-user directory and reused. `LASER_TLS_CERT=<path>` names a CA for any host, and `LASER_NO_TLS=1` opts out of the automatic choice. See [client behavior](../../docs/client-behavior.md).

The reconnection options (`reconnection_retries`, `reconnection_interval`, `reestablish_after`) are parsed and validated for compatibility and then not forwarded: that SDK keeps its own reconnect pacing and exposes no setting for it. A value the other clients reject is still rejected here, so one connection string means the same thing wherever it is read.

`ConnectWith(ctx, "", laser.Options{Address: "fleet.example:8090", Username: "agent", Password: secret})` connects with the host and the credentials given apart from a connection string. The port defaults to 8090, the credentials may hold any character, and the host follows the same TLS rules as a connection string. An address without a username, credentials without an address, or either beside a connection string is a `ConfigError` before anything is dialed.

The ops stream and its topics follow the deployment: `OpsStream()`, `ControlTopic()`, `DLQTopic()` and `ChangesTopic()` return the names the deployment announced, or `_agdx`, `control.commands`, `dlq` and `changes` when it announced none. `Options.OpsStream`, `ControlTopic`, `DLQTopic` and `ChangesTopic` pin a name at connect, and `WithOpsStream`, `WithControlTopic`, `WithDLQTopic` and `WithChangesTopic` derive a handle on another name over the same connection. An empty name restores the announced one. `Watch` reads the change feed on the ops stream and changes topic of its handle.

Formatting `Credentials`, a `ConnectionString`, `Options`, a `FencedLeaseClient` or a `SigningKey` with any `fmt` verb never prints the password, the token, the connection string or the private key.

A filtered read, an acknowledgment and a preview outside a group go to the partition's primary when the route lookup answers, and to the client's own node otherwise. A group reader looks the route up again every second while its primary does not answer, follows a moved partition once per request, and sends once more after a backup answers unavailable. An acknowledgment that still finds no primary is sent again with the same target on the managed retry backoff, for up to 30 seconds, and then returns the refusal. Deleting or renaming a stream, and deleting, updating or repartitioning a topic, drops the shared registry and the reply readers cached for it.

## How the port is verified

The shared fixture corpus pins the bytes of the shapes it carries, and the behaviour corpora pin the decisions every client must agree on. Neither covers a shape the corpus never carried: in those, a round trip shows this port's encoder and decoder agree, not that either agrees with the contract.

`just go-differential` closes that. This port writes one populated value per unfixtured shape, and the Rust crate decodes each into its own typed peer and re-encodes it. A shape that returns the same bytes is one both implementations agree on, which is what a fixture proves, for a shape no fixture exists for.

Eight structural audits run in `just go-check`, each verified to fail when the thing it checks is removed: every contract type, constant, struct field, enum variant, validation rule, non-zero default, deliberate rename and wire shape has a peer here or is declared.

Interoperation has its own proof. `go test -tags integration ./laser` publishes from Go and reads from Rust, and the reverse, against a live server, and `bdd/go` runs the shared scenarios against one.

## Cluster routing

Every node of a Laser cluster answers every managed request from shared state, so there is no one node a client has to reach. The client keeps to one, picked by a random key of its own, which spreads clients over the cluster rather than piling them onto whichever node they dialled.

A roster of one is a deployment with one node, and the request goes over the band as a plain call rather than naming the only replica there is. That is what a single-node Apache Iggy, and every deployment without clustering, understands.

On a cluster the request names its node. A node whose link drops, or that answers but could not reach its own managed backend, is a node to move off: the caller walks to the next one, at most once per node, and the move sticks so the next request starts where the last one landed. A refusal the server reasoned about is returned rather than walked for, because every node would give the same answer and walking is only delay. When every node refuses with the same backend failure, the last refusal is handed back rather than swallowed.

A read every node refused is not answered with that refusal at once. It waits on the managed backoff and walks the roster again until it is served or its deadline passes, the query's own or `ManagedDeadline`, and only then returns the same refusal. A mutation repeats under its operation identity, and a read has no identity to repeat under, so this walk is what carries a read past a single node, or a cluster that restarted all at once, refusing a moment before it serves again. A node still replaying its logs after a restart refuses the same way. The deadline is checked before each wait and again when the wait ends, so a timer that fired late never starts another request. A filter catalog read counts `catalog_unavailable` as that refusal too, which is the form the streaming server gives it there.

The roster is read once and kept. A replica id the cluster no longer knows means the roster is stale, so it is read again, once per request rather than once per attempt: a second stale id inside one request says the roster is not the problem.

The three replicated authorization writes go to the metadata primary, over the band, because it is the only node that submits them and it is the node the connection already has.

An attempt deadline ends a walk rather than being spent inside it, so the caller's next attempt starts on the next node with a fresh deadline.

## Client encryption

`laser.AES256GCM` seals and opens records in Apache Iggy's own AES-256-GCM format: a random 12-byte nonce, the ciphertext, then the 16-byte tag. The server stores only ciphertext, so a deployment that holds records never holds the key that reads them.

`topic.WithEncryption(key)` derives a publishing handle that seals every record it sends, and `ConsumerOptions.Encryption` opens them again. Both the payload and the user-header block are sealed, the block whole and as raw bytes, because ciphertext is not a header block any reader could decode. The sealing happens at the one place a record reaches the log rather than in each publish verb, so a path that was added later cannot forget it, and a record is opened before its agentic spine is read, or a sealed provenance would look undecodable and be dead-lettered.

What the key buys costs something: a server that cannot read a record cannot filter, index or materialize it either, so server-side filters and the managed read views see ciphertext. A consumer opened without the key is handed what the log holds rather than an error, because a reader that is not meant to see those records should not be stopped by them, while a consumer that holds the wrong key is told so rather than passed bytes it believes it can read.

A key that is not 32 bytes is refused rather than stretched, because a stretched key produces records no other client can open. The nonce is drawn from the system source on every send: reusing one under a single key breaks GCM outright, and a counter would have to survive restarts to be safe.

A ciphertext that is short, altered, or sealed with another key reports Apache Iggy error 72, which is what the Rust, Python and TypeScript clients raise, so one error matches across every client. Which of those three went wrong is deliberately not reported, because telling a bad tag from a bad key is a probe an attacker can run.

The suite pins the ciphertexts that Apache Iggy's Rust encryptor and Node's AES-256-GCM sealed under a shared key, so this is one of the few places in the port where Go reads bytes another client produced rather than bytes it produced itself.

## Scratch streams

`Laser.ScratchStream` creates a stream for one program run, named for a prefix and eight random hex characters, and hands back a handle whose default stream it is. A name already taken is refused rather than reused, because reuse would quietly hand a caller another run's records as its own.

Unlike the Rust and Python clients, nothing removes the stream when the handle goes out of scope. Go has no destructor, and a background delete would need a context the caller never supplied, so the cleanup is explicit: pair the call with a deferred `Delete`. An abandoned stream is then a visible omission in the code rather than a cleanup that silently did not run.

`Delete` is idempotent, so a deferred call after an explicit one costs nothing and a stream something else already removed is not a failure. A delete the server refused is reported and stays retryable: the handle records the removal only once the server confirmed it, rather than remembering an attempt as a success.

## A publish creates the topic it names

Publishing to a topic that does not exist creates it with one partition and the server's own retention, so a first publish needs no bootstrap before it. This is what the reference client's producer does, and a Go client that refused instead would make the same program work in Rust and fail here.

The topic is created only after a send comes back saying it was missing, not checked for beforehand. Checking first would double the cost of every publish to buy nothing on the path that matters.

The stream is never created this way. A governed identity has no stream-wide create right and a stream is provisioned for its owner, so publishing into a stream that does not exist is a mistake to report rather than paper over. When the stream is what was missing, the create fails too and the original publish error is returned, because that error already names what was not found.

Two publishers racing to create the same topic both succeed. The one that loses the race reads the topic back, finds it there, and sends: both wanted the topic to exist, and it does.

## Partitions are numbered from zero

A topic's partitions start at 0, so a topic created with one partition delivers every record on partition 0, and a topic with four uses 0 to 3.

This matters most for a standalone consumer, which names the partitions it reads:

```go
consumer, err := topic.Consumer(ctx, "worker", laser.ConsumerOptions{
    Partitions: []uint32{0},
})
```

Naming a partition the topic does not have is not reported as an error. The consumer simply never reads anything and the call blocks until the context deadline, which looks exactly like a publish that never arrived. If a read times out with nothing on the topic, check the partition number before suspecting the producer.

Rather than assume the numbering, a consumer can read it back:

```go
partitions, err := topic.Partitions(ctx)
```

A consumer group names no partitions at all, because the server assigns them.

## Server administration

`Laser.Users` administers the accounts a deployment signs in with: read one with its permissions or check that it exists, create, rename or change status, replace permissions, change a password, and delete. `Laser.PersonalAccessTokens` lists, creates and deletes the tokens of the signed-in user.

The system reads are `Ping`, `Stats`, `Me`, `Clients`, `ClientInfo`, `Cluster`, `Snapshot` and `DescribeOptions`. `Me` is this connection as the server sees it, which is not always what the client believes it asked for.

`MetadataChanges` reads the server's metadata change feed, which completes this group: every user, token and system capability the reference client serves is served here.

`Laser.SendCommand(ctx, code, payload)` sends a raw server command and returns the raw reply, and `SendCommandToReplica` sends it to one replica as `Cluster` numbers it. The server checks the command's permissions. The sign-in, sign-out and session commands are refused before anything is sent, and only a command a node answers from its own state goes to a replica.

## The agent runtime

`laser.AgentRuntime` puts an agent's records through one reliable loop: decode, gate, dispatch, commit, and dead-letter whatever cannot be handled.

The gate order matters and follows the Rust client rather than reading naturally. A fence is checked BEFORE a deadline, and a record behind a stale fence is skipped rather than dead-lettered, because there is nothing to redrive: a newer fence already settled what should happen. A record whose spine will not decode is a different case, and those bytes do go to the dead-letter topic, because something produced them and a human needs to see what.

`laser.AgentCtx` is what a handler is given: `Respond`, `ReplyOn`, `Send`, `Request`, `Session`, `SpawnSubconversation`, `FanOut` and the pending control book. A reply echoes its request's correlation, and a request that carries none becomes its own correlation, so a conversation can always be followed back to the record that started it.

`FanOut` asks every capable agent at once, each branch in its own conversation, and gathers what answers under a `GatherPolicy`. The policy is a type rather than a count because the three answers are different questions: `GatherAll` waits for every branch, `GatherQuorum` stops and cancels the rest as soon as enough have answered, and `GatherBestEffort` takes whatever landed by the deadline. A quorum of none is refused before anything is asked, since it would be met before a single branch ran. A branch that never answers comes back as a `GatheredFailure` with its cause rather than going missing, and each `GatheredReply` keeps the agent that gave it, so a caller can tell which result came from where.

`AgentOptions.Dedup` takes any `DedupStore`, whose `Observe(ctx, key)` records a key and reports whether it is new. `NewDeduplicator(limit)` is the in-memory window and the default. A store of your own, such as one kept in KV, makes the window survive a restart. A record without an idempotency key is never offered to it.

## Testing a handler

Package `laser/lasertest` builds what a handler receives, so a handler is unit tested without a consumer or a server. `lasertest.AgentMessage(payload, provenance)` is a plain message at the start of partition 0. `lasertest.AgentCtx(client, message, lasertest.AgentCtxOptions{Agent: id, RespondOn: topic})` is its handler context over a client the test owns, which supplies whatever IO the context helpers use. `lasertest.NewTestClock(start)` is a clock the test moves with `Advance` and `Set`. Pass `clock.Now` as `AgentOptions.Now` to drive deadlines and fences without sleeping.

## Who an agent really is

An agent id is a claim. Any connection can advertise any id, so routing by name alone trusts whoever got there first. A principal is the identity the deployment authenticated at login, and binding the two is what stops a connection advertising somebody else's agent id from being handed that agent's work.

`Laser.ClientMetadata()` is the discovery read behind that binding: the connections the deployment is holding and the opaque metadata each advertised, filtered by `WithMetadataOnly`, `Principal`, `Limit` and `After`. `Page` fetches one page and `All` walks every page on every node. Every node, because a connection lives on the one node it dialled, so a read answered by a single node would see only its share and report an agent connected elsewhere as absent. A cursor that does not advance is refused as a `ProtocolError` rather than paged forever.

`AgentRegistry.RefreshPresence(ctx, laser)` folds that read into the registry, keyed by the agent id each connection claims and bound to the principal that connection authenticated as. `PrincipalFor`, `InboxFor` and `InboxForPrincipal` read it back. Presence is deliberately separate from the card fold: a card is durable, replayable capability that outlives a brief disconnect, where presence is live transport truth that vanishes with the connection and answers where to send work right now.

Within `PresenceTTL` a refresh reuses what it holds instead of paging again, because paging the whole connection table is the dominant per-send cost and an inbox changes rarely. Apache Iggy keeps no connection metadata, so against it this folds nothing and routes fall through to the card's inbox. A deployment that never answered the hello and then refuses the read is read the same way, rather than failing a send over a surface nothing promised. A deployment that *did* answer the hello and then refuses is a real error, since that surface was promised.

`laser.ToPrincipal(agent, principal)` routes to one agent only while its live presence is bound to that principal, and `Router.WithPrincipal` binds a capability route the same way, so discovery by skill cannot hand work to an agent that merely claims it. On a plain `To` route `WithPrincipal` becomes `ToPrincipal`, rather than leaving a requirement sitting in a field nothing reads. A broadcast names nobody to bind, so it ignores one.

A bound route that resolves to an agent under the wrong principal reports a `RoutePrincipalMismatchError` naming both the expected and the actual, with the actual absent when the agent has no live presence at all, because an agent that is not there and an agent that is there as somebody else are different problems. A capability route whose skill is served, but by nobody bound to the required principal, reports the same mismatch rather than `NoCapableAgentError`: one is fixed by provisioning an agent, the other by fixing an enrollment. A quarantine still outranks the check, since that is an operator's decision about the agent itself.

`Router.RequiresPresence` says whether a route needs presence refreshed before it will resolve, and `RequiredPrincipal` names the identity it insisted on. `Contract` uses the second: under a verifier, a route that required a principal expects the reply to be signed by *that* principal rather than by the resolved target's name, which is what joins discovery to authorship. A route that insisted on who the agent is should not then settle for an answer signed by whoever holds the name.

## Spawning an agent

`Laser.SpawnAgent(ctx, SpawnOptions)` is the one call that joins a consumer group and starts the reliable consumer on its own goroutine, instead of a caller wiring `Laser.Agent`, `Laser.AgentRuntime` and `Topic.ConsumerGroup` together by hand. `SpawnOptions` embeds `AgentOptions` and `RuntimeOptions` rather than restating them, because a plain struct literal is already how this package configures everything else.

It returns an `AgentHandle`: `Ready` waits until the loop is running, `Shutdown` stops it and reports how it stopped, `Join` waits without stopping, `Abort` drops it. The group join happens before a handle is returned at all, so a publish after `SpawnAgent` returns is never racing the join. A handle that is simply dropped leaks its goroutine for the life of the process: Go has no `Drop` to catch that the way Rust does, which is why stopping is the caller's job and is documented as such rather than left to a finalizer that would not run.

`Capabilities` makes the agent discoverable. When it is non-empty, `SpawnAgent` publishes a capability card to `agent.registry` and advertises the agent's topic as its live presence, refreshing both every `CardRefresh` (`DefaultCardRefresh`, 60s) and keeping the card fresh for three refreshes, so one missed beat does not read a live agent as dead. On any stop, clean, cancelled or errored, it marks every capability unavailable, republishes once more, and clears the presence, and it does all of that before `Join` or `Shutdown` returns, so the withdrawal a caller was promised has actually landed by the time it observes the stop. Empty capabilities is a private agent that advertises nothing.

Card and presence writes are best-effort and never surface as an agent error, matching Rust. Rust logs the failures. This client has no logging facility of its own, so they are dropped, the same treatment `LeaseRegistry`'s own heartbeats already get.

`Laser.AdvertisePresence`/`ClearPresence`/`PublishCard` are the same surface for an agent not spawned through `SpawnAgent`. The first advertise pins the replica that answered it, and every later advertise or clear on that connection goes to the same one. One connection carries one agent identity: a second id is refused with `PresenceConflictError`, though re-advertising the same id is idempotent rather than a conflict.

`Governor` re-scopes the agent's `Laser` before anything else in `SpawnAgent` runs, before the runtime is built, the group is joined and the card is published, so a governed agent's own card and presence writes are governed too, and not only its handler's effects. It is `Laser.WithGovernor` scoped to one agent rather than to the handle the caller keeps. `GovernorMode` and `GovernorRetention` are read only when a governor is set.

`ConsolidateEvery` and `Consolidator` run memory consolidation off the handler loop, on its own goroutine, scoped to this agent's own memory and bounded by the period itself. Both must be given: either one alone means no tick, ever. The first pass runs immediately rather than waiting out the first period, matching Rust, since Go's `time.Ticker` has no immediate first tick. `ConsolidateEvery` is a `*time.Duration` and not a plain one because Rust's `Option<Duration>` draws three states and the zero case is a caller error rather than a default: a zero or negative period fails `SpawnAgent` with an `InvalidError` instead of reaching `time.NewTicker` and panicking. Stopping the agent cancels the context each pass runs under, so a consolidator that respects cancellation returns at once mid-pass. Go has no forcible task abort to stand in for Rust's.

`RuntimeOptions.Verifier` and `SigningKey` are the agent-side signature gate. With a verifier configured, an unsigned or unverifiable record is dead-lettered as rejected before any other gate runs, and a record whose id already verified once is dropped silently rather than handled twice, because anyone can replay captured bytes and they verify again. The signature is checked against a context rebuilt from the record's own headers rather than anything the envelope claims about itself. With a signing key set, the agent's own answers are signed: `AgentCtx.Respond`, `RespondVia` and `RespondInput` all go through one function, as do pickup acknowledgments and dead-letter answers.

A spawned agent that names no topic reads the session lane. Under a per-agent topic layout a declared agent reads its declared topic in place of the lane. `RuntimeOptions.MaxQueuedRecords` and `MaxQueuedBytes` bound what a per-partition runtime holds between the poll and the handlers, 4,096 records and 64 MiB by default, so one slow partition stops the poll only when a bound fills. A spawned agent closes its consumer when it stops, so its group hands its partitions to the next member.

## How a spawned agent schedules work

`RuntimeOptions.Concurrency` decides how the loop handles what it is assigned. `SerialConcurrency()` is the zero value: one message at a time across every partition, fully ordered, where a slow or retrying message holds up everything. `SerialPerPartitionConcurrency(maxPartitions)` runs one lane per partition instead, concurrent across partitions and still strictly in order within one, so per-partition ordering and the committed prefix both hold, and retry backoff stays lane-local, where a poison message on one partition never stalls another. `maxPartitions` bounds the lanes: a record for an unassigned partition arriving at the cap retires an idle lane, or waits for one to go idle if every lane is busy.

`RuntimeOptions.WarmDedup` replays each assigned partition's recent history into the dedup window at startup, so a restart does not re-handle what the group already committed. It needs `Group`, since the refill reads the group's stored offset to know what counts as handled. It reads that offset and never writes it. `WarmDedupPolicy` decides which committed records refill. `WarmDedupDeadLetters`, the zero value, leaves out a copy a deadline-exceeded dead letter names, reading the dead-letter topic to find them and falling back to `WarmDedupUnexpired` if they cannot all be read. `WarmDedupUnexpired` leaves out every copy whose deadline has passed without reading dead letters at all. `WarmDedupAll` refills everything, so a resend after an expiry is still recognized as a repeat. A record carrying no deadline refills its key under every policy. With a verifier configured, a committed record that would not verify is left out of the refill, the same way the live gate would dead-letter it.

## Dependencies

The client uses the Apache Iggy Go SDK for transport, `google.golang.org/protobuf` for descriptor-based Protobuf handling, `github.com/zeebo/blake3` for policy evidence digests, `github.com/santhosh-tekuri/jsonschema/v6` for JSON Schema validation, and `github.com/dlclark/regexp2/v2` for its ECMAScript patterns. The [module manifest](go.mod) pins the dependency versions.

## Why there is a regular expression engine here

Go's `regexp` cannot stand in for the one the server compiles filter patterns with. It reads `\d`, `\w`, `\s` and `\b` as ASCII where the contract reads them as Unicode, and it has no character class set operations. A filter that compiled on the server would then select different records through this client. `wire/regex.go` is a port of the reference syntax, run as a Thompson NFA so matching stays linear in the input, with character classes resolved to explicit rune sets from Go's own Unicode tables.

A property query Go's Unicode tables cannot answer is refused rather than approximated, because an approximation would quietly match a different set of records than the server. A visible compile failure is the safer answer. The refused queries are `\p{Script_Extensions=...}`, `\p{Age=...}`, the break properties `Grapheme_Cluster_Break`, `Sentence_Break` and `Word_Break`, and these binary properties: `Bidi_Mirrored`, `Changes_When_Casefolded`, `Changes_When_Casemapped`, `Changes_When_Lowercased`, `Changes_When_Titlecased`, `Changes_When_Uppercased`, `Default_Ignorable_Code_Point`, `Emoji`, `Emoji_Component`, `Emoji_Modifier`, `Emoji_Modifier_Base`, `Emoji_Presentation`, `Extended_Pictographic`, `Grapheme_Base`, `Grapheme_Extend`, `Grapheme_Link`, `IDS_Unary_Operator`, `ID_Compat_Math_Continue`, `ID_Compat_Math_Start`, `InCB` and `Modifier_Combining_Mark`. General categories, scripts and the other binary properties resolve.

## Verification

```sh
just go-check
```

That runs formatting, `go vet`, a build, the unit suite, and the suite again under the race detector, across the client, the examples and the conformance runner. `ci-go` runs the unit suite on the minimum Go the module declares, the previous stable and the current stable release. It also runs the parity check, module tidiness, the race suite, decoder fuzzing, the vulnerability scan, the wire differential, live interoperation with Rust, and the shared scenarios against Apache Iggy, Laser Stack and the recovering-catalog fixture.

What the suite pins:

- Every fixture in `wire/fixtures/` re-encodes byte for byte. The manifest is parsed out of the `corpus!()` macro in `wire/src/fixtures.rs`, so a fixture added upstream fails this suite rather than being silently skipped.
- The shared behavior corpora: filter evaluation (`filter_eval_cases.json`), the payload codecs (`filter_codec_cases.json`), and dispatch classification (`dispatch_cases.json`). These are verdicts, not encodings, and a divergence means a Go consumer would deliver a record another client skips.
- Every public type of `wire/src` has a Go peer, asserted by parsing both the Rust sources and this package's syntax tree, with an explicit list of the deliberate renames and a second test proving each claimed rename exists.

## Status against the other clients

The fixture manifest test requires a typed assertion for each shared fixture. The [implementation status](IMPLEMENTATION_STATUS.md) identifies the transport constraints, and the [parity matrix](../../docs/parity.md#the-go-client) lists the Go capability entry points.
