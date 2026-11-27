# Go SDK Implementation Status

`foreign/go` ports the wire contract and agent-orchestration layer to Go. This

file is the current-state inventory only — no session narrative. For the

independently-verified review behind these numbers, read

`foreign/go/FINAL_REVIEW.md`; for per-scenario BDD detail, `foreign/go/TEST_RESULTS.md`.

`AGENTS.md`, `CLAUDE.md`, `docs/parity.md` and `foreign/go/README.md` link

here rather than restating it.

- **Last verified**: 2026-11-27, branch `feat/go-wire-contract` — all of the

  below is uncommitted working-tree state, never through CI or review.
- **Gates**: `gofmt -l .`, `go vet ./...`, `go build ./...`,

  `go test ./... -count=1` and `-race -count=1` are green across

  `foreign/go`, `bdd/go`, `examples/go`.
- **BDD conformance**: `bdd/go` has a runner for all 34 shared

  `bdd/scenarios/*.feature` files. Live against open Apache Iggy:

  **197 pass / 77 skip (all `@managed`-gated) / 0 fail**. Against a managed

  deployment: 190 pass / 48 skip / 36 fail (see `TEST_RESULTS.md` for

  per-failure triage).
- **Examples**: 3 of ~21 per-scenario examples (`agent`, `kv`, `memory`).

Status legend: **Done**, **Partial** (behavior exists, gap named below),

**Missing** (nothing exists), **Blocked** (not implementable against the

current Apache Iggy Go SDK).

## Capability matrix

### Wire, streaming, managed surfaces


| Capability                                                                                      | Status                                               |
| ----------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Wire contract (codes, headers, envelopes, queries, filters, sessions, HTTP)                     | Done                                                 |
| Connect, TLS, capability negotiation                                                            | Done                                                 |
| Managed command band, KV, Query, Fork, Graph, RBAC, Projections, Schemas, Filters+catalog       | Done                                                 |
| KV lease/fence client (`Lease`/`TryLease`/`RenewLease`/`ReleaseLease`/`LeaseGuard`/`CasFenced`) | Partial — no `PrepareRelease` (see gaps)             |
| Materialization, Watch/change feed                                                              | Done                                                 |
| Memory (log-backed, keyword/term-overlap recall)                                                | Done                                                 |
| Memory (semantic/vector: embedder/reranker seam, fusion ranking, durable/graph lifetime)        | Missing                                              |
| Managed batch command builder (`laser`-level)                                                   | Missing (wire-level `BatchRequest` exists; see gaps) |
| Producer idempotence / sharding                                                                 | Blocked (Apache Iggy Go SDK has no equivalent)       |


### Agent gates, reliable consumer, lifecycle


| Capability                                                                                                  | Status |
| ----------------------------------------------------------------------------------------------------------- | ------ |
| `Agent` gates (dedup/fence/deadline/classify), `AgentRuntime.Run`                                           | Done   |
| `ConcurrencyPolicy`, `WarmDedupPolicy`, submitted-session pickup                                            | Done   |
| `SpawnAgent`/`AgentHandle`: capability card/presence, governor, consolidation tick, signing/verifying gates | Done   |


### Session management


| Capability                                                                                                  | Status                                                                                                    |
| ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Lifecycle, terminal latch, checkpoints, state, model/tool call recording, `ReadAt`                          | Done                                                                                                      |
| Declared layouts, leases/heartbeats (bound to the pinned lane identity)                                     | Done                                                                                                      |
| `Session.Assemble()`                                                                                        | Done                                                                                                      |
| Operator control: mandatory pause/resume dispatch gate, durable parking, resume replay, `Session.Parked`    | Done — opt-in via `RuntimeOptions.EnforceControl`, not unconditional (see gaps)                           |
| Operator control: recovery after restart/rebalance, approval parking, lane acks, epoch/assignment machinery | Done — approval parking landed; recovery of held work after restart/rebalance remains the only open slice |
| Session budgets / `BudgetGate`                                                                              | Done                                                                                                      |


### Multi-agent orchestration (caller-side)


| Capability                                                                                                                                 | Status                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| Registry, presence fold, principal-bound routing, `Router`, quarantine                                                                     | Done                                                                              |
| `ReplyHub`/`Requester`, shared reply dispatcher                                                                                            | Done                                                                              |
| `Contract`/`ContractBuilder`, `Scatter`/`ScatterReport` (incl. reply-side signature verification)                                          | Done                                                                              |
| `ApprovalGate`/`ApprovalGrant`, `SwappableGovernor`                                                                                        | Done                                                                              |
| Workflow engine: declared-steps DAG, saga compensation, exclusive/fenced steps with `OnTimeout(Reassign)`, plain-code form (`WorkflowRun`) | Done — journal CBOR encoding is Go-native, so a Go-started run resumes only on Go |


### Bridges (A2A / MCP / AG-UI)


| Capability                                                                                                   | Status  |
| ------------------------------------------------------------------------------------------------------------ | ------- |
| A2A task adapter (submit/task/cancel/card), HTTP router, `SubmitTo`/`SubmitWithHops`/`SubmitIn`/`SubmitInTo` | Done    |
| A2A agent-card signing/verification (`SignCard`/`VerifyCard`)                                                | Missing |
| MCP tool-call adapter, resources/prompts, HTTP router                                                        | Done    |
| `AgUiEvents`                                                                                                 | Done    |
| `submit_in`/`call_tool_in` (plain `Laser` surface)                                                           | Done    |
| `A2ACaller.SubmitIn`/`SubmitInTo` (task-bound caller)                                                        | Missing |
| Bridge hop-loop guard                                                                                        | Done    |


## Known open gaps


| Gap                                                | Impact                                                                                                                                                                                 |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No `laser`-level managed batch command builder     | Can't batch several managed mutations into one atomic-outcome frame                                                                                                                    |
| KV fenced-lease `PrepareRelease`                   | Can't prepare a fenced lease's release ahead of the release call                                                                                                                       |
| A2A agent-card signing/verification                | A served card can't be cryptographically verified (needs an RFC 8785 JSON canonicalizer not yet in the repo)                                                                           |
| `A2ACaller.SubmitIn`/`SubmitInTo`                  | Child-session task submission unavailable through the task-bound A2A caller specifically                                                                                               |
| Operator pause/resume is opt-in, not unconditional | A handler not written for `EnforceControl` keeps processing a "paused" session's work                                                                                                  |
| Semantic/vector memory                             | Recall is keyword/term-overlap only; no embedder/reranker seam, fusion ranking, consolidator defaults, `ConversationState`/`ReplayBound`, `MemoryHandler`, or durable/graph `Lifetime` |
| Workflow journal's CBOR encoding is Go-native      | A Go-started workflow run resumes only on Go, not cross-SDK                                                                                                                            |
| `Drop`-based `LeaseGuard` auto-release             | Architectural — Go has no destructors; caller must call `Release`/`defer` explicitly                                                                                                   |
| Operator-pause rebalance re-read                   | Architectural — a resumed session is only re-noticed on its next incoming record, not proactively on rebalance                                                                         |
| `examples/go` has 3 of ~21 per-scenario examples   | No working reference for the workflow engine, scatter/contract, bridges, materialization, query                                                                                        |
| No quickstart snippet in `README.md`               | No "connect, publish, consume" minimal example in prose                                                                                                                                |


## Explicitly out of scope (by direction, not oversight)

- `producer.idempotent`/`producer.sharding` — blocked on the Apache Iggy Go SDK.
- QUIC and WebSocket transports — Apache Iggy Go SDK implements TCP only.
- Joining `docs/parity.md`'s three-way table as a fourth column — deferred

  until the gaps above read as Go gaps rather than parity-table artifacts.

## Next priorities

1. Add a minimal quickstart snippet to `README.md`.
2. Port the managed batch command builder and KV `PrepareRelease` — both

  bounded, wire-level pieces already exist.
3. Grow `examples/go` past 3 — at minimum one for the workflow engine and

  one for contract/scatter.
4. Decide, explicitly, whether operator pause should become unconditional by

  default or stay opt-in.
5. Operator-pause recovery of held work after a restart or rebalance.
6. A2A card signing (needs new RFC 8785 canonicalization machinery).
7. Bundle session convenience wrappers (`checkpoint`/`replay`/etc.) as sugar

  over existing primitives, for ergonomics parity.

