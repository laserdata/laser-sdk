# LaserData - Go client

The Go client for [LaserData](https://laserdata.com), a native port of the
`laser-wire` contract.

**This client is in progress.** The wire contract is complete and pinned
against the shared corpus, and the client is being built on it: `Connect`, the
shared connection string grammar, TLS resolution, the connect-time capability
negotiation, the managed command band, and the key-value, query, fork, graph,
session-read and authorization surfaces, plus addressing streams and topics and
publishing to them. All of it rides the Apache Iggy Go SDK's TCP transport.

There is no consumer, no agent runtime, and no bridge yet, so the handle hands
out the underlying Iggy client for anything beyond that. It is not published,
it is absent from the [cross-SDK parity matrix](../../docs/parity.md), and it
does not run the shared behavior scenarios. Use the [Rust](../../sdk/README.md),
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

One, `google.golang.org/protobuf`, for descriptor resolution in the Protobuf
filter codec. Hand-rolling descriptor semantics means reimplementing oneofs,
proto3 synthetic optionals, map entries and packed repeated fields, which is
where a canonical implementation earns its place. Rust uses `prost-reflect`
and TypeScript uses `protobufjs` for the same reason.

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
