# LaserData - Go client

The Go client for [LaserData](https://laserdata.com), a native port of the
`laser-wire` contract.

**This client is in progress.** The wire contract is complete and pinned
against the shared corpus. There is no SDK surface yet: no transport, no
streaming client, no managed clients, no agent runtime, and no bridges. It is
not published, it is absent from the [cross-SDK parity
matrix](../../docs/parity.md), and it does not run the shared behavior
scenarios. Use the [Rust](../../sdk/README.md),
[Python](../python/README.md), or [TypeScript](../typescript/README.md)
clients for anything beyond encoding and decoding the contract.

Like the TypeScript client, this is a native port rather than a binding over
the Rust crate. The Rust crates expose no C ABI, cgo would cost consumers
cross-compilation, and the Apache Iggy Go client is itself pure Go.

## Layout

| Package | Owns |
|---|---|
| `wire` | the contract: codes, headers, topics, envelopes, queries, projections and bindings, logical and writer schemas, filters, sessions, the HTTP surface, and a typed client over a caller-supplied transport |
| `internal/cborform` | an order-preserving CBOR value tree and a shortest-form encoder. The CBOR and JSON encoders both walk it, so field order and the skip rules cannot drift between the two encodings |

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
