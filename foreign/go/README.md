# LaserData - Go client

The Go client for [LaserData](https://laserdata.com), a native port of the
`laser-wire` contract.

**This client is in progress.** The wire contract is complete and pinned
against the shared corpus, and the client is built on it: `Connect`, the shared
connection string grammar, TLS resolution, the connect-time capability
negotiation, the managed command band and its typed surfaces (key-value, query,
forks, graph, session reads and writes, authorization, consumer filters and the
saved-filter catalog), streams, topics and publishing, consuming, replay, typed
records, provenance headers, AGDX agent messaging and gates, the registry and
router, request and reply, memory, context assembly, the projection, binding
and writer-schema registries, record signing with its key registry, action governance with its approval gate, and the bridge value
mappings. All of it rides the Apache Iggy Go SDK's TCP transport.

It is not published yet. [docs/parity.md](../../docs/parity.md) lists its
standing on each capability in its own section, and `bdd/go/` runs the shared
behavior scenarios it covers. The handle hands out the underlying Iggy client
for anything beyond what it serves. Use the [Rust](../../sdk/README.md),
[Python](../python/README.md), or [TypeScript](../typescript/README.md)
clients for anything this one does not cover yet.

## Managed commands

The managed surfaces (query, key-value, sessions, authorization, filters,
forks, graph) ride one command band off the log, which this client reaches
through the Apache Iggy Go SDK's `SendBinaryRequest`. `Laser.SendManaged` is
that path.

A command that changes state is wrapped in a managed envelope carrying an
operation identity, so a transport retry of the same call is applied once.
`SendManagedWithOperation` lets a caller choose the identity, so a retry that
outlives the process still applies once. Reads are sent as given.

Managed names are scoped to the default stream (`stream:<stream>/<name>`),
which is how two streams in one deployment keep separate namespaces.
`Options.ResourceNaming` opts out, and listings are mapped back to the
caller's own names.

Every managed reply raises the client's applied frontier, and a later read of
that surface sends it, so whichever node answers has applied at least what this
client already wrote or read.

Each surface is gated twice before a call leaves: once on the capability the
deployment announced, and once on the op version it serves, because a payload
sent across a version gap would be read under a contract neither side agreed
to.

## Key-value

`Laser.Kv(namespace)` opens a namespace. Reads are `Get`, `GetEntry`,
`GetEntryIfChanged` (a conditional read that skips the body transfer) and
`Exists`. Writes are the `Set` builder with `TTL` or `ExpiresAt`, turning into
a compare-and-swap through `ExpectVersion` or `ExpectAbsent`. `Scan` lists a
namespace by prefix, range or substring, with `Each` for a bounded walk and
`All` to collect. `DeleteMany` applies the same bounds in bulk.

A refusal arrives as a `KvError` carrying the contract's own classification, so
`Retryable` and `Conflict` tell a momentary outage from a precondition that
will never hold. Keys are arbitrary bytes, so every read hands back a `[]byte`
rather than a string.

## Authorization

`Whoami` reads the caller's own effective capabilities. `ListRoles`, `GetRole`,
`DefineRole`, `DeleteRole`, `GetBindings`, `BindRoles` and `AuthzHistory`
administer the rest. Deny wins over allow, and the default is deny.

This surface is served by the streaming server itself rather than by a managed
backend, so a deployment announcing the authorization feature serves it either
way. The gate is that feature, not the managed root.

Every write carries a receipt identity, so an exact repeat after a lost reply
returns the original outcome instead of applying twice. `DefineRoleAs`,
`DeleteRoleAs` and `BindRolesAs` let a caller choose it and keep it across a
restart. `BindRolesExpectRevision` makes a bind conditional, so two
administrators editing one user cannot both win.

## Query

`Laser.Query(index)` builds a query against a materialized index. Predicates
chain and compose with AND, `AnyOf` adds a disjunction, and the aggregate,
grouping, ordering, paging and projection steps read as one expression.
`Fetch` sends it.

Three fields are additive on the wire, so an unaware backend would drop them
and answer anyway: a consistency level, a lexical search, and raw SQL. Each is
refused before sending unless the deployment announced it, because an answer
that is wider or staler than was asked for still looks like success. Raw SQL
additionally needs a ready backend that announced that dialect.

The deadline is absolute on the wire and resolved at send time, so a request
built and held does not arrive already expired. Every execution carries its own
identity. A result whose rows do not match its schema is refused rather than
read, since misaligned values look like data rather than a fault.

## Forks

`Laser.Fork(id)` branches the read model. A fork overlays speculative rows on
the trunk without writing to it, so a what-if can be explored and then promoted
or discarded. `Create` makes one (continuous by default, `Severed` freezes it
at the trunk's current offsets), `PutRow` writes a speculative row or a
tombstone, and `Promote` or `Squash` ends it. `Laser.Forks` lists them.

An embedding travels as a JSON array literal, so a component that is not a
finite number is refused: JSON has no spelling for one, and it would silently
change which rows a nearest-neighbour search returns.

## Graph

`Laser.Graph(name)` builds a traversal: a start (`StartIDs`, `StartMatch`,
`StartNearest`), hops (`Out`, `In`, `Both`, `Hop`), filters, and what to return.
`Neighbors` is the cheap one-hop read. `AsOf` reads the graph as it stood at an
instant, keeping only edges whose valid-time window contains it.

`Upsert` and `Link` write facts. Ids are content-addressed, so re-applying the
same extraction converges on the same elements instead of duplicating them,
which is what lets a projector replay without the graph growing.

## Session reads

`Laser.Sessions()` reads the deployment's session index for the default
stream, and `SessionsIn(stream)` for another. `Get`, `List`, `Events`, `State`,
`Links`, `Sources` and `Changes` cover it.

Every request names its stream first, because the index is per stream: a
session id alone does not say which index to look in. The stream is sent as
given rather than scoped, since it is a stream name and not a managed resource
name.

A list or timeline walked page by page would shift under appends, so the first
page returns a boundary and later pages pass it back through `Bound`. The
boundary carries its change series too, because a rebuilt index restarts the
sequence and the offset alone would then mean something else.

## Streams, topics and publishing

`Laser.Stream(name).Topic(name)` addresses a topic, and `Laser.Topic(name)`
uses the stream pinned at connect. `Ensure` creates what is absent and leaves
what exists alone, so it is safe on every start: two callers racing to create
one stream both succeed, because the loser re-reads and finds what it wanted.

`Publish` builds a record (`Payload`, `JSON`, `CBOR`, `Header`, `Key`) and
`Send` appends it. `Topic.Send` is the same path with explicit headers, and
`Topic.Batch` appends many records in one call under one partitioning.

A partition key is the only ordering Iggy gives: records sharing a key land on
one partition. Omitting the key balances across partitions and promises no
order between records. An empty key is refused, because it would hash every
record onto one partition while reading as "no key given".

## Provenance

`Provenance` is the agentic spine: conversation, causality, routing and usage.
`Publish().Provenance(p)` stamps it onto a record and partitions by the
conversation, which is what keeps one conversation ordered.
`ProvenanceFrom(message)` reads it back.

Numbers ride typed rather than as decimal text, using the contract's own
encoder, so there is one implementation of the byte layout rather than two that
can drift. A header outside the dictionary is foreign and ignored, so an
application's keys and the AGDX envelope stamps pass through. A key the
dictionary does know, carrying the wrong kind, is corruption and is reported:
silently dropping a fence would skip the gate and let a stale holder's replay
through as if it were unfenced.

A record with no conversation is refused, since it has no partition key and no
conversation to belong to. A broadcast addressee reads back as no target rather
than an agent named `*`. A non-finite cost is refused in both directions,
because it would be rolled up into a total as a number that is not one.

## Consuming

`Topic.Consumer(name, options)` opens a standalone consumer and
`Topic.ConsumerGroup(group, options)` joins a group. A standalone consumer
names the partitions it reads, because nothing assigns them to it. A group
member names none, because the server assigns and rebalances them.

`Next` hands over one record and `Each` runs a handler over them. A record
arrives with its provenance already read: a record with no spine carries none,
which is the normal case on a plain data topic, while a record whose spine is
present but broken is refused rather than handed over with its fence silently
missing.

Nothing is committed unless the caller asks. `ConsumerOptions.AutoCommit`
turns on client-side commits, and `Commit` and `Mark` do it explicitly. The
default suits a handler with side effects: a record is re-delivered after a
crash rather than quietly skipped.

## Replay and typed records

`Topic.Replay()` opens a positional cursor. It owns its offsets rather than
storing them server-side, so persist `Offsets()` and resume with
`FromOffsets()`. `Poll` drains what is there and stops, which is the bounded
read below a consumer. Records come back ordered by the server's own clock,
then partition and offset, so two readers of one topic see one order.

`TypedJSON[T]`, `TypedCBOR[T]` and `TypedWith[T]` view a topic as carrying one
body type through one codec. `Publish` stamps the codec's content type and
`Records` walks a cursor handing over decoded bodies.

One unreadable record is usually one bad writer, not a broken topic, so a
decode failure names the partition and offset it sat at and the read carries
on. Passing no error handler skips those records quietly.

## Agent messaging

`Laser.Agdx(source, conversation)` sends AGDX messages on one conversation.
`Command`, `Respond`, `Emit`, `Fail` and `Chunk` cover the kinds, and
`OpenAgdx` reads one back off a record.

Each record carries the whole envelope as its body, because a reader needs the
routing and causality to decide anything. The envelope stamps and the
provenance spine ride one header block, each key once, so a reader can route
and audit without decoding the body. The conversation is the partition key.

Where the contract ties fields together, the constructor does too rather than
leaving a chain a caller can half build. `TaskStatus` takes the correlation and
the state it requires. `SessionStarted`, `SessionProgressed` and `SessionEnded`
each take the typed lifecycle record their state calls for, and the terminal
flag is derived from the state rather than taken from the caller, which is the
only way the two cannot drift apart.

Cost rides the envelope in integer micro-units and the headers in whole
currency, so it is converted between them rather than copied.

## Agent gates

`Laser.Agent(identity, options)` holds the gates a running agent applies.
`Decide` returns a verdict for a record and `DecideMessage` for an AGDX
message. `Run` reads a consumer through them.

The order is the point. Expiry and the must-understand set come first, because
a record failing either can never be handled, and spending a dedup key on it
would suppress a later record that could be. The fence comes next, so a stale
holder's replay advances nothing. Dedup comes last, so a key is spent only on a
record that would otherwise have been handled.

The fence is the real at-most-once gate: it refuses a record whose fence is
behind the highest accepted for its task, accepts an equal one as the same
holder retrying, and lets unfenced work through. Dedup is a best-effort window
scoped to the producing agent, so one producer cannot suppress or replay
another's key. Both are bounded by the recently active working set rather than
by every record ever seen.

## Registry and routing

`NewAgentRegistry()` holds what the deployment knows about its agents: the
cards they published and the ones an operator took out of rotation. A card with
a lifetime stops describing a live agent once it runs out, which is what makes
a card liveness and not only discovery.

`To`, `Broadcast`, `ToCapable`, `AllCapable`, `Sticky` and `WithScorer` build a
route, and `ResolveTargets` names the agents it reaches. `RouteCheapest`,
`RouteFastest` and `RouteLeastLoaded` rank on the advisory classes a card
advertises.

A capability route that matches no live agent is an error, not a silent
broadcast: the caller asked for a skill, and handing the work to whoever is
listening is not that. An agent that declared itself unavailable is skipped,
while a degraded one still routes, since a struggling agent beats no agent. A
sticky route falls back when its agent is gone, because pinning a conversation
is a preference rather than a requirement. Candidates come back in a stable
order, so two resolutions pick the same agent and a retry lands where the
first attempt did.

## Request and reply

`NewReplyHub()` matches incoming replies to the requests waiting for them. One
hub reads a reply topic once for every waiter, so many concurrent requests
share one read rather than each scanning the topic.

`Subscribe` registers a wait **before** the request is sent, because the reply
cannot exist until it is: a hub that started listening afterwards could miss an
answer that arrived in between. `Await` blocks for it.

A correlation alone does not match. Two conversations can reuse one, and a
shared reply topic carries every agent's answers, so the session and the
addressee are checked too. A waiter that knows who it is will not take an
unaddressed reply; one that does not care takes whatever arrives, which is what
a sole consumer of a private reply topic wants.

An acknowledgment (`task`/`working`) extends the wait rather than ending it,
because the deadline is for a reply that never comes rather than for a handler
that is slow. `Acknowledged` reports it, so a slow handler is waited out rather
than retried into a duplicate.

`Laser.Requester(source, topic)` ties the two halves together: `Request` sends
a command and waits for its answer, narrowing the wait to the command it sent
once the send returns. Nothing drives the hub on its own, so a caller feeds
`Hub().Deliver` from whatever reads the reply topic. `Deliver` returns false
for anything that answers no waiting request, so one reader serves both the
requests and the rest of the traffic.

`Laser.DeadLetter` publishes a record the gates turned away, keeping the
payload so the work can be replayed and naming where the original sat.

## Memory

`Laser.Memory(namespace)` opens the agentic memory surface. `Remember`,
`Forget` and `Improve` write to the memory topic, so memory is replayable: the
log is the audit, and a deployment folds it into a read view.

An item's id is derived from its owner, kind and body. The owner is part of the
hash, so two users never share an item and the same fact in two conversations
is two items. Remembering the same thing twice converges on one item rather
than growing the store. Forgetting writes a tombstone: it changes what is
recalled rather than erasing what happened.

`Rank` orders items against a query, which is the ranking a client can do with
no model: overlap on words. It counts query words covered rather than item
words matched, so a long item does not outrank a precise one. `ToContextBlock`
renders them for a prompt, filling in rank order until the token budget is
reached. The budget is advisory, since tokens are estimated rather than
tokenized: a real count needs the model's tokenizer, and carrying one would tie
the client to a provider.

## Context assembly

`Assemble(conversation)` builds a conversation's history into the messages a
prompt carries. `Add` offers records and keeps what belongs to the
conversation, since a topic carries many.

Ordering is by the server's append time, which is the one clock every topic
shares. It is best effort rather than strictly chronological: Apache Iggy
orders per partition, so no total order exists across them to read. Ties are
broken on topic then offset, because a prompt that reorders between runs is not
reproducible.

`LastN`, `RoleFilter`, `TokenBudget` and `Chain` narrow the history. A budget
walks newest first, since a prompt wants what just happened, and always keeps
at least one message so an oversized one reaches the model to be truncated
there rather than vanishing before it arrives. `TokenBudget.Estimate` takes a
real tokenizer when a caller has one; the default is the contract's own
estimate, which memory spends too, so history and recalled memory share one
notion of a budget.

## Sessions

`Laser.Sessions()` opens and writes agent sessions, alongside the index reads
above. `Create(label)` derives a session id from its stream, namespace and
label, so a job that runs nightly resumes its session rather than starting a
new one each time. `Start()` opens a fresh one.

`Begin`, `Progress`, `Complete` and `Fail` write the lifecycle. A session
writes as an agent, so one that names none with `As` is refused before it
sends. `Session.Memory()` is scoped to the conversation and `Session.Context()`
carries the configured token bound.

A derived id is wire-visible identity: the same seed reaches the same
conversation in every client. The algorithm is a versioned FNV-1a, pinned by
the same golden vector the Rust client pins, so a drift in either is caught
rather than silently addressing different sessions. Bumping the version remaps
every derived conversation, which is why it is a constant to change on purpose
rather than an implementation detail.

## Consumer filters

`topic.Filters(consumer)` reads a topic through a server-side filter. The
server evaluates it and sends back only what matched, so a reader pays for the
records it wanted rather than for the whole topic: that saving is in what never
leaves the server, and it is why the feature exists.

`Read(partition, filter)` builds one filtered poll. `Count` bounds the matches
a page returns and `MaxExamined` bounds how many source records the server
looks at: a normal consumer sets it to the count, so a sparse filter returns
quickly rather than scanning a whole partition, while a sweep over a cold topic
leaves it unset and takes the server's own budget. `Each` walks pages until the
partition is exhausted or the caller stops.

Every page is held to the request before a record is read from it. The record
body's framing is walked first, because the count in its head is what a decoder
sizes an allocation against, and then the page itself: the partition, the
incarnation, the policy, the budget, the scan boundary, the record order, and
the faults it reports. A page that misstates its boundary is worse than a lost
one, since the records it skipped past are never read again.

`Ack` stores progress through the page's safe offset, which is the last record
examined rather than the last one matched: a reader that stored only its
matches would re-examine everything the filter rejected on the next read. A
page that examined nothing offers no progress, so nothing is sent. The server
refuses an acknowledgment once the source generation moved on or the consumer
no longer owns the partition.

`Preview` says what a filter would select over real records without joining a
group or storing progress, and `Explain` turns that from testable into
debuggable by saying why each record did or did not match. `Test` judges one
record a caller holds. `ValidateFilter` compiles a filter locally with the
contract's own evaluator, which costs no round trip and works against a
deployment that serves no filters at all.

Filtered delivery is served by the streaming server, so it is gated on the
native capability. The saved catalog needs a managed backend and is gated
separately: a deployment can filter without saving anything.

## The saved-filter catalog

`Laser.FilterCatalog()` administers the saved filters and the groups bound to
them. `Register` saves the first revision, `Revise` appends one conditional on
which is currently latest, and `Describe`, `SetRevisionEnabled`, `Archive` and
`Drop` change a filter without changing what a bound group reads mid-page.
`Bind`, `Unbind`, `ReleaseBinding` and `ConfigureGroup` move groups between
revisions.

Every change carries an operation identity, so a change repeated under the same
one returns the recorded outcome rather than applying twice. A retryable
refusal is repeated under that identity rather than under a fresh one. A change
the catalog has accepted but not yet folded is read back by its identity rather
than sent again, and a change whose outcome never arrives is reported as
`AmbiguousMutationError` naming the identity to recover under, because an
attempt whose reply was lost may still have applied.

Each verb returns what its change produced and where the fold applied it. That
position is what a group read can wait for, which is what gives an application
that just configured a group read-your-writes through any node.

## Projections, bindings and schemas

`Laser.Projections()` administers what a deployment materializes from a topic
and into what shape. `Register` declares a projection and the deployment
creates its backing table. `RegisterGraph` declares a graph projection, which
materializes nodes and edges rather than a row table, so the two are not
interchangeable: each refuses the other's kind rather than registering
something that creates the wrong thing. `Drop` stops a projection being
applied and leaves the rows it already wrote alone, because dropping a plan is
not deleting data.

`Laser.Bindings()` routes sources into projections. `Apply` starts a projector
for a `(stream, topic)` source, and `Remove` stops it. Naming one projection
scopes the removal to it rather than to the whole binding.

`Laser.Schemas()` administers the writer-schema registry. `Register` is
synchronous and answers the allocated id, because the id is what a producer
stamps on every record and a caller cannot proceed without it. `Drop`
tombstones rather than deletes: records already stamped with the id keep
decoding, and the id stays reserved against re-registration under a different
definition.

Writes append control commands, which the deployment applies asynchronously,
so a caller observes the apply by reading the registry back rather than by the
append succeeding. Each carries an operation identity, so a transport retry is
applied once, and `PublishControlAs` lets a caller choose the identity so a
retry that outlives the process still applies once. Reads are registry
browses, served off the snapshot by a managed backend. Browse has no topic
transport, so a deployment without a backend is refused rather than left
waiting for a reply nothing will send.

Every resource name is scoped to the handle's stream, which is how two streams
in one deployment keep separate registries, and listings come back under the
names the caller wrote so a caller never sees scoping it did not ask for.
`Options.ResourceNaming` opts out.

## Signing and keys

A record on a shared topic says who sent it. `AgdxSend.Signed(key)` makes that
something a reader can check: the signature covers the envelope with its own
signature cleared, prefixed by the contract's domain separator so an AGDX
signature can never be replayed into another protocol.

The interpretation attributes the record is stamped with, its content type and
wire version, are bound into the preimage too. Without that, an intermediary
could flip the codec on a signed record and the same bytes would read as
something else entirely while the signature still verified. A signature that
bound nothing reproduces the preimage a signature written before contexts
existed covered, so an old record still verifies.

`KeyRegistry` holds enrolled verifying keys, each bound to the authenticated
principal that enrolled it. Verifying returns that principal, and a caller
compares it against the server-stamped identity. That comparison is the
authorship binding: a key proves the principal it was enrolled under, never
the source an envelope claims for itself. A key carries a kind, so a
privileged control fact can require an operator key and an enrolled agent
cannot quarantine another agent.

A key also carries a window, so a rotation overlaps rather than cuts over: the
old key stays valid long enough to cover the records already in flight.
`Verify` reads no clock and checks the bytes alone, `VerifyAt` holds the key to
its window, and `VerifyObservedAt` additionally refuses a signature whose bound
context differs from what the record carried, before any cryptography runs.
Revocation disables a key whatever its window says.

`VerifyDelegation` reads whom a signer acted for. The claim rides inside the
signed span rather than on a header, so a signer cannot forge it and an
intermediary cannot rewrite it. The server authorizes only the authenticated
caller, so a caller intersects the two principals' grants itself: an agent
must never exceed the user it acts for.

`Laser.KeyRegistry()` is the managed sibling, storing lifecycle-aware records
in a reserved key-value namespace keyed by canonical key id. Enrollments are
conditional on what is already there, so two administrators cannot both
believe they won. `Snapshot` reads them into the in-memory registry a verifier
folds with, skipping any entry that does not hold up: one corrupt enrollment
must not stop every other principal's records from verifying.

The preimage is pinned against the Rust client: `sign_test.go` carries
signature vectors Rust produced for the same envelope and seed, so a change to
any part of the preimage shows up as a signature no other client can verify.

## Action governance

`laser.WithGovernor(governor, mode)` derives a handle whose publishes, AGDX
verbs, requests and memory writes run a policy hook before the effect. It is
defense in depth at the effect boundary: authorization on the managed surfaces
stays server-owned, and this hook cannot widen it. It can only stop or narrow
what the client was already allowed to do.

The governor sees who is acting, where the effect lands, the body about to be
published, and the session's running counters. Every agent-written field it
reads is a claim rather than a fact: a declared purpose is advisory unless the
envelope is signed.

The vocabulary is broader than allow and deny, because a policy that can only
refuse forces every nuance into a refusal. `Allow` runs the effect and records
nothing. `Observe` runs it and records evidence. `Modify` replaces the body,
applied before signing so a signature always covers what the log carries.
`Block` and `Defer` reject, the second retryable. `StepUp` rejects naming the
scope an approval must grant, so a handler knows what to ask for.

`GovernorObserve` is a shadow rollout: every decision is recorded with what
enforcement would have done, the effect always runs unmodified, and a failed
evidence write only warns. Observing never impacts production, a failing
governor included. `GovernorEnforce` applies the verdict, and a governor that
fails or answers nothing fails the effect rather than failing open. Under
enforcement a proceeding decision whose evidence did not land fails the call,
because a governed effect nobody recorded is an ungoverned effect. A denial
stands whether or not its evidence landed.

Each decision is appended to the audit topic as an AGDX event, sealed with
BLAKE3 over its own canonical encoding and chained to the previous decision in
the same conversation. `VerifyEvidenceChain` checks a chain read back: a
reordered, dropped or edited record breaks it, which is the point, since
evidence nobody can check is not evidence. Chain heads are process-local, so a
restart or a retention eviction starts a new chain rather than breaking the
old one, and chains of several processes interleave on one topic. Evidence
goes straight to the log rather than back through the governed verbs, so a
blocking policy cannot stop its own decision being recorded.

`laser.NewQuorumGovernor(policy)` composes independent voters and is itself a
governor, so it enrolls exactly like a single one. Voters run concurrently and
each is bounded, so one hung voter never stalls every governed effect: a
mandatory one that does not answer blocks and any other abstains. When the
quorum is met the composite is the strongest affirmative, in order modify,
observe, allow. When it is not, it is the most actionable denial, in order
block, step-up, defer. Every voter's verdict is recorded, so the audit trail
names who said what. Nothing here reaches a durable log: it is an in-process
combinator, not the crash-safe propose, vote and commit lifecycle a durable
version would need.

## Approvals

`Requester.ApprovalGate` asks a named approver and waits for the answer. The
reply mints an `ApprovalGrant`, which is the only way to hold one: a grant
cannot be constructed from a message the approver never sent, and a reply from
anyone else decides nothing.

`AgdxSend.WithGrant` carries a grant with an effect. A governor stepping up in
the grant's own conversation is answered by it rather than raising the step-up
again, which is what lets a handler gate once and then act. A grant from
another conversation approves nothing, because an approval given for one piece
of work is not an approval for another. The step-up is still recorded with its
scope: the approval answered it, it did not make it disappear.

Waiting in place is the whole of this gate. The Rust client can instead park
the work and replay the handler once the decision lands, which frees the
consumer for the session's other work. That runtime is not ported here, so a
gate held open holds its caller open.

## Bridges

`bridges` maps external agent interfaces onto AGDX. A bridge is a translation
and nothing more: the log stays the source of truth, so a task survives a
bridge restart and remains replayable.

A2A task states map both ways, and a word outside the vocabulary is refused
rather than guessed, since an edge sending one is speaking a protocol this
bridge does not implement. A paused task reads as working, because A2A has no
paused state and reporting anything terminal would say the work ended when it
has not.

MCP marks a failure as one rather than returning it as ordinary content, since
an edge that cannot tell them apart shows an error to a user as though it were
the answer.

AG-UI renders envelopes as the events a front end consumes. A chunk stream's
purpose is declared only on its opening chunk, so `AgUiStream` remembers it per
channel and forgets a channel once it closes.

Like the TypeScript client, this is a native port rather than a binding over
the Rust crate. The Rust crates expose no C ABI, cgo would cost consumers
cross-compilation, and the Apache Iggy Go client is itself pure Go.

## Layout

| Package | Owns |
|---|---|
| `wire` | the contract: codes, headers, topics, envelopes, queries, projections and bindings, logical and writer schemas, filters, sessions, the HTTP surface, and a typed client over a caller-supplied transport |
| `laser` | the client: connect, the connection string grammar and TLS resolution, the capability negotiation, the managed command band and the typed surfaces on it, and the stream, topic and publish facade. The grammar is parsed here rather than by the Apache Iggy Go SDK, because the TLS decision is Laser's and because the accepted grammar has to match the Rust and TypeScript clients |
| `internal/cborform` | an order-preserving CBOR value tree and a shortest-form encoder. The CBOR and JSON encoders both walk it, so field order and the skip rules cannot drift between the two encodings |

## Transport

The client reaches a deployment over TCP, plain or TLS, because the [Apache Iggy Go
SDK](https://pkg.go.dev/github.com/apache/iggy/foreign/go) implements that transport
alone. QUIC and WebSocket are served by the Rust and Python clients, and a connection
string naming one is refused with that hint rather than silently downgraded.

A `*.laserdata.cloud` or `*.laserdata.com` host is encrypted automatically against the
root CA this package carries, a copy of `sdk/certs/laserdata.crt` that a test checks
byte for byte. That SDK reads a CA from a path, so the bundled root is cached in an
owner-only per-user directory and reused. `LASER_TLS_CERT=<path>` names a CA for any
host, and `LASER_NO_TLS=1` opts out of the automatic choice. See
[client behavior](../../docs/client-behavior.md).

The reconnection options (`reconnection_retries`, `reconnection_interval`,
`reestablish_after`) are parsed and validated for compatibility and then not forwarded:
that SDK keeps its own reconnect pacing and exposes no setting for it. A value the
other clients reject is still rejected here, so one connection string means the same
thing wherever it is read.

## How the port is verified

The shared fixture corpus pins the bytes of the shapes it carries, and the
behaviour corpora pin the decisions every client must agree on. Neither covers
a shape the corpus never carried: in those, a round trip shows this port's
encoder and decoder agree, not that either agrees with the contract.

`just go-differential` closes that. This port writes one populated value per
unfixtured shape, and the Rust crate decodes each into its own typed peer and
re-encodes it. A shape that returns the same bytes is one both implementations
agree on, which is what a fixture proves, for a shape no fixture exists for.

Eight structural audits run in `just go-check`, each verified to fail when the
thing it checks is removed: every contract type, constant, struct field, enum
variant, validation rule, non-zero default, deliberate rename and wire shape
has a peer here or is declared.

What none of this proves is interoperation. Every byte this port has produced
was compared against a file or another implementation in the same process.
None has crossed a socket, because there is no transport yet.

## Dependencies

Two beyond the Apache Iggy client.

`google.golang.org/protobuf`, for descriptor resolution in the Protobuf filter
codec. Hand-rolling descriptor semantics means reimplementing oneofs, proto3
synthetic optionals, map entries and packed repeated fields, which is where a
canonical implementation earns its place. Rust uses `prost-reflect` and
TypeScript uses `protobufjs` for the same reason.

`github.com/zeebo/blake3`, for the policy-evidence receipt digest. The hash is
part of the contract rather than an implementation choice: evidence sealed
with anything else reads as a broken chain to every other client. Go's
standard library has no BLAKE3, Rust uses the `blake3` crate and TypeScript
uses `@noble/hashes`.

Everything else is written directly: Avro, MessagePack, BSON, the JSON codec,
and the regular expression engine.

## Why there is a regular expression engine here

Go's `regexp` cannot stand in for the one the server compiles filter patterns
with. It reads `\d`, `\w`, `\s` and `\b` as ASCII where the contract reads
them as Unicode, and it has no character class set operations. A filter that
compiled on the server would then select different records through this
client. `wire/regex.go` is a port of the reference syntax, run as a Thompson
NFA so matching stays linear in the input, with character classes resolved to
explicit rune sets from Go's own Unicode tables.

One construct is refused rather than approximated: `\p{Script_Extensions=...}`.
Go ships script memberships but not script extensions, and approximating one
with the other would quietly match a different set of records than the server.
A visible compile failure is the safer answer.

## Verification

```sh
just go-check
```

That runs formatting, `go vet`, a build, the unit suite, and the suite again
under the race detector. `ci-go` runs the same set on the minimum Go the module
declares and on the current release.

What the suite pins:

- Every fixture in `wire/fixtures/` re-encodes byte for byte. The manifest is
  parsed out of the `corpus!()` macro in `wire/src/fixtures.rs`, so a fixture
  added upstream fails this suite rather than being silently skipped.
- The shared behavior corpora: filter evaluation (`filter_eval_cases.json`),
  the payload codecs (`filter_codec_cases.json`), and dispatch classification
  (`dispatch_cases.json`). These are verdicts, not encodings, and a divergence
  means a Go consumer would deliver a record another client skips.
- Every public type of `wire/src` has a Go peer, asserted by parsing both the
  Rust sources and this package's syntax tree, with an explicit list of the
  deliberate renames and a second test proving each claimed rename exists.

## Status against the other clients

The contract is type-complete and every one of the 252 fixtures carries a
typed assertion, which is what the TypeScript port carries. A fixture added
upstream without one fails the suite rather than being quietly covered by the
structural pass alone.
