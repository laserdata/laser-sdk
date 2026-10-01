---
name: consumer-filters
description: Server-side consumer filters - `sdk/src/filters/` (feature `filters`) over the `wire/src/filter/` contract, with the TypeScript peer in `foreign/typescript/src/managed/filters.ts` and the Python peer in `foreign/python/src/filters.rs`. Use when changing `Laser::filters()`, the filtered reader and its acknowledgments, previews and tests, the saved-filter catalog and group bindings, the evaluator, or the `cdc` examples. Wire in the AGDX spec section A14
---

# Consumer filters

A consumer filter selects records on the LaserData Iggy fork before they cross the network. The reader receives only the matching records, with their original offsets, headers, and payload bytes. The fork evaluates the filter next to the data. The saved-filter catalog and consumer group bindings live in `laser-plane`.

## Where the code is

- `wire/src/filter/`: the contract. `expr.rs` holds `ConsumerFilter`, `FilterExpr`, coercions, and the fault, foreign, and mismatch record policies. `text.rs` holds the six text match kinds. `read.rs` holds the filtered poll, acknowledgment, preview, and test types and the reason codes. `catalog.rs` holds saved filters, revisions, bindings, mutations, and outcomes. `headers.rs` holds the typed header dictionary. `codecs.rs` decodes JSON, CBOR, Avro, and Protobuf. `eval.rs` is the compiled evaluator (feature `filter-eval`).
- `sdk/src/filters/`: `client.rs` (`Filters`: validate, test, preview, the catalog verbs, `apply`, `apply_as`, `wait_for_outcome`), `reader.rs` (`FilteredReaderBuilder`, `FilteredReader`, `MatchedPage`, `MatchedRecord`, `ack`, `ack_through`, `ack_page`, `read_round`, `examined_in_round`, `owns`, the outstanding-page bound `max_unacked_pages`, default 1024), `group.rs` (membership and the rejoin refusal on a recreated source), `guard.rs` (the local re-check), `progress.rs`, `route.rs`.
- `foreign/typescript/src/wire/filter.ts`, `filter-eval.ts`, `filter-codecs.ts`, and `foreign/typescript/src/managed/filters.ts`. The TypeScript guard refuses regex filters at build.
- `foreign/python/src/filters.rs` plus the stubs in `foreign/python/laser_sdk.pyi`. Complex shapes cross through serde.

## Rules

- Rust defines the behavior. TypeScript and Python expose the same builders, reader methods, catalog verbs, record fields, and error reasons. A change to one language changes all three, the stubs, the TypeScript API reports (`npm run api:report`), and the docs listed below, in one change.
- Verdicts are identical across languages. Add a case to `wire/fixtures/filter_eval_cases.json` or `filter_codec_cases.json` for every evaluator change and keep `wire/tests/filter_eval_corpus.rs`, the TypeScript wire tests, and the Python corpus test green.
- Progress is stored only through acknowledgments of completed work. A page from another reader, or from this reader before a rejoin, is refused. Never store an offset the application has not handled.
- Group bindings are immutable per group incarnation. A numeric group id names one incarnation of a stream and topic, and a recreated source refuses the rejoin.
- Reason codes, operation versions, and limits are literals pinned by tests. Never bump an op version for an unreleased shape change.

## Docs that must move with the code

`README.md`, `sdk/README.md`, `wire/README.md`, `foreign/typescript/README.md`, `foreign/python/README.md`, `docs/agdx.md` section A14, `docs/tutorial.md` chapter 11, `AGENTS.md`, the `cdc` example READMEs under `examples/*/`, and the published consumer-filters guide.

## Examples

The `cdc` example exists in Rust, Python, and TypeScript with the same phases in the same order: publish, inline read, sample test, preview, header routing, CBOR, Avro, and Protobuf, catalog, A/B revisions, delete refusal, cleanup. Its figures (4 of 240 records, 424 of 27,953 payload bytes, 98.5%) come from the shared feed generator and are stated identically everywhere.
