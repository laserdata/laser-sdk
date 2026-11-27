package laser

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
	"github.com/zeebo/blake3"
)

// governorAgent is who evidence is attributed to when the governed action
// names no acting agent.
const governorAgent = "governor"

// PolicyDecisionOperation is the operation stamped on every policy-evidence
// event, so an audit topic filters governance decisions without decoding
// bodies.
const PolicyDecisionOperation = "policy_decision"

// GovernorErrorDecision is the decision recorded when the governor itself
// fails in observe mode. The effect proceeds and the reason carries the
// failure.
const GovernorErrorDecision = "error"

// DefaultVoterTimeout bounds how long one quorum voter may take.
const DefaultVoterTimeout = 10 * time.Second

// ActionGovernor is the pre-effect policy hook. Decide runs before the client
// publishes an agent send, a topic publish, an AGDX verb, or a memory write,
// and its decision is applied under the configured mode.
//
// This is defense in depth at the effect boundary. Authorization on the
// managed surfaces stays server-owned, and this hook cannot widen it: it can
// only stop or narrow what this client was already allowed to do.
//
// Under GovernorEnforce an error fails the governed action, so a broken
// governor never fails open. Under GovernorObserve the effect proceeds and
// the failure is recorded under GovernorErrorDecision.
type ActionGovernor interface {
	Decide(ctx context.Context, action *GovernedAction) (*ActionDecision, error)
}

// GovernedAction is one side effect about to run, as the governor sees it.
//
// Every agent-written field here is a claim rather than a fact: Purpose and
// DataClassification are advisory unless the envelope is signed, and Signed
// says only that this client will sign the record, not that a peer verified
// it.
type GovernedAction struct {
	// Kind is what sort of effect this is.
	Kind ActionKind
	// Stream and Topic are where the effect publishes.
	Stream string
	Topic  string
	// Source is the acting agent, when the effect carries one.
	Source string
	// Target is the addressed agent, when the effect targets one.
	Target string
	// Conversation is the conversation the effect belongs to.
	Conversation *wire.ConversationID
	// Correlation is the reply-correlation key, when the effect carries one.
	Correlation string
	// Operation is the envelope operation name on the AGDX path.
	Operation string
	// Tool is the tool name on the AGDX path.
	Tool string
	// OnBehalfOf is the delegation subject from the envelope metadata.
	OnBehalfOf string
	// Purpose is the declared purpose from the envelope metadata, advisory.
	Purpose string
	// DataClassification is the declared classification, advisory.
	DataClassification string
	// Payload is the body about to be published.
	Payload []byte
	// Signed says whether this client will sign the record at send.
	Signed bool
	// Counters are the session counters at decision time, for rate and budget
	// policies.
	Counters ActionCounters
	// Approval is the approval the effect carries. A step-up in the grant's
	// own conversation proceeds on it instead of stepping up again.
	Approval *ApprovalGrant
}

// ActionKind is the kind of side effect a governed action describes. It is
// recorded in evidence by this name.
type ActionKind string

const (
	// ActionSend is a plain agent-topic send.
	ActionSend ActionKind = "send"
	// ActionPublish is a typed or raw publish to a data topic.
	ActionPublish ActionKind = "publish"
	// ActionRequest is a request awaiting a correlated reply.
	ActionRequest ActionKind = "request"
	// ActionCommand is an AGDX command.
	ActionCommand ActionKind = "command"
	// ActionResponse is an AGDX response.
	ActionResponse ActionKind = "response"
	// ActionEvent is an AGDX event.
	ActionEvent ActionKind = "event"
	// ActionStatus is an AGDX status.
	ActionStatus ActionKind = "status"
	// ActionError is an AGDX error terminal.
	ActionError ActionKind = "error"
	// ActionMemoryWrite is a memory write.
	ActionMemoryWrite ActionKind = "memory_write"
)

// String is the pinned evidence name of this kind.
func (kind ActionKind) String() string { return string(kind) }

// IsNamed reports a kind this client knows, so a kind read back off the audit
// topic from a newer peer is visible as unrecognized rather than silently
// treated as one of these.
func (kind ActionKind) IsNamed() bool {
	switch kind {
	case ActionSend, ActionPublish, ActionRequest, ActionCommand, ActionResponse,
		ActionEvent, ActionStatus, ActionError, ActionMemoryWrite:
		return true
	default:
		return false
	}
}

// ActionKindOf is the governed kind an AGDX message kind is decided as.
func ActionKindOf(kind wire.AgentKind) ActionKind {
	switch kind {
	case wire.KindCommand:
		return ActionCommand
	case wire.KindResponse:
		return ActionResponse
	case wire.KindEvent, wire.KindChunk:
		return ActionEvent
	case wire.KindStatus:
		return ActionStatus
	case wire.KindError:
		return ActionError
	default:
		return ActionSend
	}
}

// ActionCounters are the session counters at decision time. They are shared
// by every clone of a governed handle, so a policy can bound a whole session
// rather than one handle.
type ActionCounters struct {
	// Sends is how many governed non-request effects have run.
	Sends uint64
	// Requests is how many governed requests have run.
	Requests uint64
	// BytesSent is how many payload bytes governed effects have published.
	BytesSent uint64
}

// ActionDecision is what the governor decided, with the evidence detail it
// wants recorded.
type ActionDecision struct {
	// Verdict is what to apply.
	Verdict PolicyVerdict
	// Reason is why, recorded in evidence.
	Reason string
	// Policy is the pack and rules that decided, recorded in evidence.
	Policy *PolicyRef
	// RiskScore is the governor's estimate, recorded in evidence.
	RiskScore *float64
}

// Allow runs the effect and records no evidence.
func Allow() *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictAllow}}
}

// Observe runs the effect and records evidence.
func Observe() *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictObserve}}
}

// Block rejects the effect before it runs.
func Block(reason string) *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictBlock}, Reason: reason}
}

// StepUp rejects the effect naming the scope an approval must grant. A
// handler catches it, runs an approval gate, and sends again.
func StepUp(scope string) *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictStepUp, Scope: scope}}
}

// Modify replaces the body before the effect.
//
// It is applied before signing, so a signature always covers the body the log
// actually carries rather than the one the caller offered.
func Modify(body []byte) *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictModify, Body: body}}
}

// Defer holds the work for later. The denial is retryable.
func Defer(reason string) *ActionDecision {
	return &ActionDecision{Verdict: PolicyVerdict{Kind: VerdictDefer}, Reason: reason}
}

// WithReason records why, in evidence.
func (decision *ActionDecision) WithReason(reason string) *ActionDecision {
	decision.Reason = reason
	return decision
}

// WithPolicy records the deciding policy pack and rules, in evidence.
func (decision *ActionDecision) WithPolicy(policy PolicyRef) *ActionDecision {
	decision.Policy = &policy
	return decision
}

// WithRiskScore records the governor's risk estimate, in evidence.
func (decision *ActionDecision) WithRiskScore(score float64) *ActionDecision {
	decision.RiskScore = &score
	return decision
}

// VerdictKind discriminates a decision. The vocabulary is broader than allow
// and deny, because a policy that can only refuse forces every nuance into a
// refusal.
type VerdictKind string

const (
	// VerdictAllow runs the effect.
	VerdictAllow VerdictKind = "allow"
	// VerdictObserve runs the effect and records evidence.
	VerdictObserve VerdictKind = "observe"
	// VerdictBlock rejects before the effect.
	VerdictBlock VerdictKind = "block"
	// VerdictStepUp pauses on an approval granting a scope.
	VerdictStepUp VerdictKind = "step_up"
	// VerdictModify replaces the body, then runs the effect.
	VerdictModify VerdictKind = "modify"
	// VerdictDefer records that the work is held for later.
	VerdictDefer VerdictKind = "defer"
)

// PolicyVerdict is one decision, with whatever the kind carries.
type PolicyVerdict struct {
	Kind VerdictKind
	// Scope is what a step-up approval must grant.
	Scope string
	// Body is the replacement a modify carries.
	Body []byte
}

// String is the pinned evidence name of this verdict.
func (verdict PolicyVerdict) String() string { return string(verdict.Kind) }

// PolicyRef is the versioned policy artifact a decision came from, recorded
// verbatim in evidence.
//
// This client parses no policy language. A governor maps whatever engine it
// fronts onto this, which is what keeps the evidence readable without the
// engine that produced it.
type PolicyRef struct {
	PackID      string   `cbor:"pack_id"`
	PackVersion string   `cbor:"pack_version"`
	RuleIDs     []string `cbor:"rule_ids"`
}

// GovernorMode is how a decision is applied.
//
// It is configuration, never an envelope claim: an agent must not be able to
// self-authorize weaker enforcement.
type GovernorMode string

const (
	// GovernorObserve is a shadow rollout: every decision is recorded with
	// what enforcement would have done, the effect always runs unmodified,
	// and a failed evidence write only warns. Observing never impacts
	// production.
	GovernorObserve GovernorMode = "observe"
	// GovernorEnforce applies the verdict. A failed evidence write on a
	// proceeding decision fails the call, so a governed effect is never
	// unrecorded.
	GovernorEnforce GovernorMode = "enforce"
)

// String is the pinned evidence name of this mode.
func (mode GovernorMode) String() string { return string(mode) }

// PolicyEvidence is one governance decision, appended as an AGDX event on the
// audit topic.
//
// It is append-only evidence, never a grant. ReceiptDigest is the BLAKE3 hash
// of this record's canonical encoding with the digest field empty, and
// PreviousDigest chains it to the prior decision in the same conversation, so
// reordering or dropping local evidence is detectable. Signed evidence
// additionally proves the producer, and the log position proves order on the
// substrate.
type PolicyEvidence struct {
	// DecisionID is this decision's own identity.
	DecisionID string `cbor:"decision_id"`
	// Decision is the verdict name.
	Decision string `cbor:"decision"`
	// Mode is the enforcement mode the decision ran under.
	Mode string `cbor:"mode"`
	// Kind is the governed action's kind.
	Kind string `cbor:"kind"`
	// Stream and Topic are what the action targeted.
	Stream        string     `cbor:"stream"`
	Topic         string     `cbor:"topic"`
	Source        *string    `cbor:"source,omitempty"`
	Target        *string    `cbor:"target,omitempty"`
	Conversation  *string    `cbor:"conversation,omitempty"`
	Correlation   *string    `cbor:"correlation,omitempty"`
	Operation     *string    `cbor:"operation,omitempty"`
	Tool          *string    `cbor:"tool,omitempty"`
	OnBehalfOf    *string    `cbor:"on_behalf_of,omitempty"`
	Reason        *string    `cbor:"reason,omitempty"`
	ApprovedScope *string    `cbor:"approved_scope,omitempty"`
	Policy        *PolicyRef `cbor:"policy,omitempty"`
	RiskScore     *float64   `cbor:"risk_score,omitempty"`
	// ReceiptDigest is BLAKE3 of this record's canonical encoding with the
	// digest field empty.
	ReceiptDigest string `cbor:"receipt_digest"`
	// PreviousDigest is the prior decision's receipt digest in this chain.
	PreviousDigest *string `cbor:"previous_digest,omitempty"`
	// ChainID is the chain this record extends. Each governed handle starts
	// its own chain per conversation, and a restart or a retention eviction
	// starts a new one, so chains of several processes interleave on one
	// topic.
	ChainID string `cbor:"chain_id"`
	// Outcome is what the decision did to the effect: effected, blocked,
	// step_up or deferred. Effected means the decision let it proceed, which
	// the publish itself can still fail after.
	Outcome string `cbor:"outcome"`
	// AtMicros is when the decision was made.
	AtMicros uint64 `cbor:"at_micros"`
}

// The outcomes an evidence record carries.
const (
	OutcomeEffected = "effected"
	OutcomeBlocked  = "blocked"
	OutcomeStepUp   = "step_up"
	OutcomeDeferred = "deferred"
)

// Encode is the canonical encoding this evidence rides the log as.
func (evidence PolicyEvidence) Encode() ([]byte, error) {
	encoded, err := wire.Marshal(evidence)
	if err != nil {
		return nil, &CodecError{Reason: "encode the policy evidence", Cause: err}
	}
	return encoded, nil
}

// DecodePolicyEvidence reads an evidence body back off the audit topic.
func DecodePolicyEvidence(payload []byte) (*PolicyEvidence, error) {
	var evidence PolicyEvidence
	if err := wire.Unmarshal(payload, &evidence); err != nil {
		return nil, &CodecError{Reason: "decode the policy evidence", Cause: err}
	}
	return &evidence, nil
}

// Seal computes the receipt digest over the canonical encoding with the
// digest field empty, which is what makes the digest reproducible by anyone
// holding the record.
//
// A caller writing evidence through a path of its own seals with this, so its
// records chain and verify alongside the ones this client writes. The hash is
// BLAKE3, the same one every Laser client seals with: a record sealed with
// any other reads as a broken chain everywhere else.
func (evidence PolicyEvidence) Seal() (PolicyEvidence, error) {
	evidence.ReceiptDigest = ""
	canonical, err := evidence.Encode()
	if err != nil {
		return PolicyEvidence{}, err
	}
	digest := blake3.Sum256(canonical)
	evidence.ReceiptDigest = hex.EncodeToString(digest[:])
	return evidence, nil
}

// VerifyEvidenceChain reports whether evidence, in log order for one
// conversation, is a set of unbroken chains.
//
// Within each chain every record must reproduce its own receipt digest and
// name the previous record's digest, the first naming none. Chains of several
// processes may interleave on one topic, so they are followed separately. A
// reordered, dropped or edited record breaks its chain, which is the whole
// point: evidence nobody can check is not evidence.
func VerifyEvidenceChain(evidence []PolicyEvidence) bool {
	heads := map[string]string{}
	for _, item := range evidence {
		head, chained := heads[item.ChainID]
		switch {
		case item.PreviousDigest == nil && chained:
			return false
		case item.PreviousDigest != nil && !chained:
			return false
		case item.PreviousDigest != nil && *item.PreviousDigest != head:
			return false
		}
		sealed, err := item.Seal()
		if err != nil || sealed.ReceiptDigest != item.ReceiptDigest {
			return false
		}
		heads[item.ChainID] = item.ReceiptDigest
	}
	return true
}

// PolicyBlockedError reports an effect the governor rejected.
type PolicyBlockedError struct{ Reason string }

func (e *PolicyBlockedError) Error() string {
	return "the governor blocked this action: " + e.Reason
}

// StepUpRequiredError reports an effect that needs an approval granting Scope
// before it can run.
type StepUpRequiredError struct{ Scope string }

func (e *StepUpRequiredError) Error() string {
	return "the governor requires an approval granting " + e.Scope
}

// PolicyDeferredError reports work held for later. The identical effect may
// succeed once the condition that deferred it has passed.
//
// Raised by the governor, and by a parked approval gate: there the record is
// parked rather than lost, and the runtime hands it back to the handler once
// the approver decides or the deadline passes. The handler then runs again
// from the top, so whatever it did before the gate happens twice and has to
// be idempotent.
type PolicyDeferredError struct {
	Reason string
	// Deferrer names what held the work, so a caller can tell a governor's
	// decision from an approval the runtime is waiting on.
	Deferrer DeferralSource
}

// DeferralSource is what held a deferred action.
type DeferralSource uint8

const (
	// DeferredByGovernor is an action a policy held for later. The default,
	// so an error built without naming a source reads as it always did.
	DeferredByGovernor DeferralSource = iota
	// DeferredByApproval is a handler parked on an approval gate.
	DeferredByApproval
)

func (e *PolicyDeferredError) Error() string {
	if e.Deferrer == DeferredByApproval {
		return "parked awaiting an approval: " + e.Reason
	}
	return "the governor deferred this action: " + e.Reason
}

// Retryable reports a condition where the identical effect may run later.
func (e *PolicyDeferredError) Retryable() bool { return true }

// GovernorRetention bounds the evidence-chain heads a governed handle keeps.
//
// Heads are process-local, so an eviction or a restart starts a new local
// chain for that conversation rather than breaking the old one.
type GovernorRetention struct {
	// Capacity is the most conversation heads retained.
	Capacity int
	// IdleTTL is how long an unused head is kept before it may be evicted.
	IdleTTL time.Duration
}

// DefaultGovernorRetention is what a governed handle keeps unless told
// otherwise.
func DefaultGovernorRetention() GovernorRetention {
	return GovernorRetention{Capacity: 4096, IdleTTL: time.Hour}
}

// WithGovernor is a copy of this handle whose agent sends, topic publishes,
// AGDX verbs and memory writes run the governor before the effect.
//
// The connection and everything else is shared with the original, while the
// governor's session counters and evidence chain are fresh and shared by
// every copy of the returned handle. Agents built from the governed handle
// inherit it, so a handler's own effects are governed too.
func (laser *Laser) WithGovernor(governor ActionGovernor, mode GovernorMode) *Laser {
	return laser.WithGovernorRetention(governor, mode, DefaultGovernorRetention())
}

// WithGovernorRetention is WithGovernor with an explicit chain-head budget.
func (laser *Laser) WithGovernorRetention(
	governor ActionGovernor, mode GovernorMode, retention GovernorRetention,
) *Laser {
	governed := laser.copy()
	governed.governor = newGovernorState(governor, mode, retention)
	return governed
}

// Governor reports whether this handle governs its effects, and under which
// mode.
func (laser *Laser) Governor() (GovernorMode, bool) {
	if laser.governor == nil {
		return "", false
	}
	return laser.governor.mode, true
}

// GovernedCounters are the session counters this handle's governor has
// accumulated, nil when it governs nothing.
func (laser *Laser) GovernedCounters() *ActionCounters {
	if laser.governor == nil {
		return nil
	}
	counters := laser.governor.snapshot()
	return &counters
}

// govern runs the governor over one action when a handle has one.
//
// A nil body proceeds with the caller's own, a non-nil body proceeds with the
// replacement, and an error is the applied denial or the fail-closed governor
// or evidence failure.
func (laser *Laser) govern(
	ctx context.Context, action *GovernedAction,
) ([]byte, error) {
	state := laser.governor
	if state == nil {
		return nil, nil
	}
	action.Counters = state.snapshot()
	decision, decisionName, err := state.decide(ctx, action)
	if err != nil {
		return nil, err
	}
	if action.Kind == ActionRequest {
		atomic.AddUint64(&state.requests, 1)
	} else {
		atomic.AddUint64(&state.sends, 1)
	}
	applied := applyVerdict(state.mode, granted(action, decision.Verdict))
	if applied.recorded {
		emitted := state.emitChainedEvidence(
			ctx, laser, action, decision, decisionName, applied.outcome)
		if state.mode == GovernorEnforce && applied.denial == nil && emitted != nil {
			// A proceeding decision with no recorded evidence must not
			// proceed: fail closed rather than run an unrecorded effect.
			return nil, emitted
		}
		// In observe mode nothing may impact production, and a denial stands
		// whether or not its evidence landed.
	}
	if applied.denial != nil {
		return nil, explained(applied.denial, decision.Reason)
	}
	// Count what actually goes out, which is the replacement when modified.
	sent := len(action.Payload)
	if applied.body != nil {
		sent = len(applied.body)
	}
	atomic.AddUint64(&state.bytesSent, uint64(sent))
	return applied.body, nil
}

// governPublish governs one raw publish, with the acting agent and
// conversation read from the provenance its headers carry.
func (laser *Laser) governPublish(
	ctx context.Context, stream, topic string, payload []byte,
	headers []iggcon.HeaderEntry,
) ([]byte, error) {
	if laser.governor == nil {
		return nil, nil
	}
	action := &GovernedAction{
		Kind: ActionPublish, Stream: stream, Topic: topic, Payload: payload,
	}
	if provenance, err := ProvenanceFromHeaders(headers); err == nil && provenance != nil {
		conversation := provenance.Conversation
		action.Conversation = &conversation
		if provenance.Agent != nil {
			action.Source = provenance.Agent.String()
		}
		if provenance.TargetAgent != nil {
			action.Target = provenance.TargetAgent.String()
		}
		if provenance.CorrelationID != nil {
			action.Correlation = *provenance.CorrelationID
		}
	}
	return laser.govern(ctx, action)
}

// governMessages governs every record of a batch in order, replacing a
// modified body in place.
//
// The first denial fails the whole batch before anything is sent, because a
// batch half-published is a batch whose caller cannot tell what landed.
func (laser *Laser) governMessages(
	ctx context.Context, stream, topic string, messages []iggcon.IggyMessage,
) error {
	if laser.governor == nil {
		return nil
	}
	for index := range messages {
		headers, err := iggcon.DeserializeHeaders(messages[index].UserHeaders)
		if err != nil {
			return &CodecError{Reason: "read the record headers", Cause: err}
		}
		body, err := laser.governPublish(
			ctx, stream, topic, messages[index].Payload, headers)
		if err != nil {
			return err
		}
		if body == nil {
			continue
		}
		replaced, err := iggcon.NewIggyMessage(body, iggcon.WithUserHeaders(headers),
			iggcon.WithID(messages[index].Header.Id))
		if err != nil {
			return &InvalidError{Reason: "rebuild the governed record: " + err.Error()}
		}
		messages[index] = replaced
	}
	return nil
}

// emitEvidence publishes one evidence record as an AGDX event on the audit
// topic of the action's stream, keyed by the conversation.
//
// It goes straight to the ungoverned send path rather than back through the
// governed verbs, so evidence cannot recurse into governance and a blocking
// policy cannot stop its own decision being recorded.
func (laser *Laser) emitEvidence(
	ctx context.Context, action *GovernedAction, evidence PolicyEvidence,
) error {
	body, err := evidence.Encode()
	if err != nil {
		return err
	}
	// Evidence written by an action that names no agent is still evidence, so
	// it is attributed to the governor rather than dropped.
	source, err := wire.NewAgentID(governorAgent)
	if err != nil {
		return err
	}
	if action.Source != "" {
		if named, err := wire.NewAgentID(action.Source); err == nil {
			source = named
		}
	}
	conversation := wire.ConversationID{}
	if action.Conversation != nil {
		conversation = *action.Conversation
	}
	record := wire.RecordID{ID: wire.IDFromBytes(NewOperationID())}
	operation := PolicyDecisionOperation
	envelope := wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Record:       &record,
		Conversation: conversation,
		Source:       source,
		Operation:    &operation,
		Body:         body,
	}
	if err := envelope.Validate(); err != nil {
		return &InvalidError{Reason: "the policy evidence event: " + err.Error()}
	}
	payload, err := wire.Marshal(envelope)
	if err != nil {
		return &CodecError{Reason: "encode the policy evidence event", Cause: err}
	}
	headers, err := EncodeHeaderBlock(
		wire.HeadersForEnvelope(&envelope, wire.ContentCBOR, false))
	if err != nil {
		return err
	}
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return &InvalidError{Reason: "build the evidence record: " + err.Error()}
	}
	topic := laser.Stream(action.Stream).Topic(wire.AgentAudit)
	_, err = topic.sendMessages(ctx, []iggcon.IggyMessage{message},
		conversation.String(), true)
	return err
}

// governorState is the shared state of one governed handle: the hook, the
// mode, the session counters, and the per-conversation evidence chain.
type governorState struct {
	governor  ActionGovernor
	mode      GovernorMode
	sends     uint64
	requests  uint64
	bytesSent uint64
	retention GovernorRetention

	guard sync.Mutex
	// released wakes a caller waiting for a chain head to free up, so a
	// handle at its head budget does not spin.
	released *sync.Cond
	chain    map[string]*chainEntry
}

// chainEntry is one conversation's evidence chain head.
type chainEntry struct {
	id string
	// digest serializes evidence writes for this conversation, so two
	// concurrent effects cannot both claim the same predecessor.
	digest   sync.Mutex
	previous string
	chained  bool
	// leases counts the callers holding this entry, so one in use is never
	// evicted out from under them.
	leases       int
	touchedMicro uint64
}

func newGovernorState(
	governor ActionGovernor, mode GovernorMode, retention GovernorRetention,
) *governorState {
	if retention.Capacity < 1 {
		retention.Capacity = 1
	}
	state := &governorState{
		governor:  governor,
		mode:      mode,
		retention: retention,
		chain:     map[string]*chainEntry{},
	}
	state.released = sync.NewCond(&state.guard)
	return state
}

func (state *governorState) snapshot() ActionCounters {
	return ActionCounters{
		Sends:     atomic.LoadUint64(&state.sends),
		Requests:  atomic.LoadUint64(&state.requests),
		BytesSent: atomic.LoadUint64(&state.bytesSent),
	}
}

// decide runs the hook, turning a failure into what the mode says it is.
func (state *governorState) decide(
	ctx context.Context, action *GovernedAction,
) (*ActionDecision, string, error) {
	decision, err := state.governor.Decide(ctx, action)
	switch {
	case err == nil && decision == nil:
		// A governor that answers nothing has not decided anything, and
		// treating that as allow would be failing open.
		err = &InvalidError{Reason: "the governor returned no decision"}
	case err == nil:
		return decision, decision.Verdict.String(), nil
	}
	if state.mode == GovernorObserve {
		// Observing never impacts the effect, a failing governor included.
		return Observe().WithReason(err.Error()), GovernorErrorDecision, nil
	}
	return nil, "", err
}

// acquireChain leases one conversation's chain head, making one when the
// handle has room and waiting when every head it holds is in use.
func (state *governorState) acquireChain(key string) *chainEntry {
	state.guard.Lock()
	defer state.guard.Unlock()
	for {
		state.pruneChain(nowMicros())
		if entry, held := state.chain[key]; held {
			entry.leases++
			return entry
		}
		for len(state.chain) >= state.retention.Capacity {
			if !state.evictOldest() {
				break
			}
		}
		if len(state.chain) < state.retention.Capacity {
			entry := &chainEntry{
				id:           newChainID(),
				leases:       1,
				touchedMicro: nowMicros(),
			}
			state.chain[key] = entry
			return entry
		}
		state.released.Wait()
	}
}

// releaseChain gives one head back and wakes whoever is waiting for room.
func (state *governorState) releaseChain(entry *chainEntry) {
	state.guard.Lock()
	entry.leases--
	entry.touchedMicro = nowMicros()
	state.guard.Unlock()
	state.released.Broadcast()
}

// pruneChain drops the unleased heads that have been idle past the budget.
func (state *governorState) pruneChain(now uint64) {
	ttl := uint64(state.retention.IdleTTL / time.Microsecond)
	for key, entry := range state.chain {
		if entry.leases == 0 && now-entry.touchedMicro >= ttl {
			delete(state.chain, key)
		}
	}
}

// evictOldest drops the least recently used unleased head, reporting whether
// it found one.
func (state *governorState) evictOldest() bool {
	oldest, found := "", false
	for key, entry := range state.chain {
		if entry.leases > 0 {
			continue
		}
		if !found || entry.touchedMicro < state.chain[oldest].touchedMicro {
			oldest, found = key, true
		}
	}
	if found {
		delete(state.chain, oldest)
	}
	return found
}

// emitChainedEvidence serializes audit publication per conversation, and
// advances the chain only after the record lands, so a failed write cannot
// become a predecessor.
func (state *governorState) emitChainedEvidence(
	ctx context.Context, laser *Laser, action *GovernedAction,
	decision *ActionDecision, decisionName, outcome string,
) error {
	key := ""
	if action.Conversation != nil {
		key = action.Conversation.String()
	}
	entry := state.acquireChain(key)
	defer state.releaseChain(entry)
	entry.digest.Lock()
	defer entry.digest.Unlock()

	var previous *string
	if entry.chained {
		head := entry.previous
		previous = &head
	}
	evidence, err := state.sealEvidence(
		action, decision, decisionName, outcome, entry.id, previous)
	if err != nil {
		return err
	}
	if err := laser.emitEvidence(ctx, action, evidence); err != nil {
		return err
	}
	entry.previous, entry.chained = evidence.ReceiptDigest, true
	return nil
}

// sealEvidence builds and seals one evidence record against its predecessor.
func (state *governorState) sealEvidence(
	action *GovernedAction, decision *ActionDecision,
	decisionName, outcome, chainID string, previous *string,
) (PolicyEvidence, error) {
	evidence := PolicyEvidence{
		DecisionID:     newChainID(),
		Decision:       decisionName,
		Mode:           state.mode.String(),
		Kind:           action.Kind.String(),
		Stream:         action.Stream,
		Topic:          action.Topic,
		Source:         optional(action.Source),
		Target:         optional(action.Target),
		Correlation:    optional(action.Correlation),
		Operation:      optional(action.Operation),
		Tool:           optional(action.Tool),
		OnBehalfOf:     optional(action.OnBehalfOf),
		Reason:         optional(decision.Reason),
		Policy:         decision.Policy,
		RiskScore:      decision.RiskScore,
		PreviousDigest: previous,
		ChainID:        chainID,
		Outcome:        outcome,
		AtMicros:       nowMicros(),
	}
	if action.Conversation != nil {
		evidence.Conversation = optional(action.Conversation.String())
	}
	if decision.Verdict.Kind == VerdictStepUp {
		evidence.ApprovedScope = optional(decision.Verdict.Scope)
	}
	return evidence.Seal()
}

// appliedVerdict is one verdict applied under one mode: whether evidence is
// recorded, whether and with what body the effect proceeds, and the denial
// when it does not.
type appliedVerdict struct {
	recorded bool
	outcome  string
	body     []byte
	denial   error
}

// granted turns a step-up the action's own approval already answers into an
// observed effect, so a handler that gated once is not asked to gate again
// for the same conversation.
func granted(action *GovernedAction, verdict PolicyVerdict) PolicyVerdict {
	if verdict.Kind != VerdictStepUp || action.Approval == nil {
		return verdict
	}
	if action.Conversation == nil ||
		*action.Conversation != action.Approval.Conversation() {
		return verdict
	}
	return PolicyVerdict{Kind: VerdictObserve}
}

// applyVerdict is the verdict-by-mode table. Observe mode records what
// enforcement would have done and always proceeds unmodified.
func applyVerdict(mode GovernorMode, verdict PolicyVerdict) appliedVerdict {
	enforced := mode == GovernorEnforce
	denied := func(outcome string, denial error) appliedVerdict {
		if !enforced {
			return appliedVerdict{recorded: true, outcome: OutcomeEffected}
		}
		return appliedVerdict{recorded: true, outcome: outcome, denial: denial}
	}
	switch verdict.Kind {
	case VerdictAllow:
		return appliedVerdict{outcome: OutcomeEffected}
	case VerdictObserve:
		return appliedVerdict{recorded: true, outcome: OutcomeEffected}
	case VerdictModify:
		applied := appliedVerdict{recorded: true, outcome: OutcomeEffected}
		if enforced {
			applied.body = verdict.Body
		}
		return applied
	case VerdictBlock:
		return denied(OutcomeBlocked, &PolicyBlockedError{})
	case VerdictStepUp:
		return denied(OutcomeStepUp, &StepUpRequiredError{Scope: verdict.Scope})
	case VerdictDefer:
		return denied(OutcomeDeferred, &PolicyDeferredError{})
	default:
		// A verdict this client does not know cannot be applied, and treating
		// it as allow would run an effect the governor may have refused.
		return denied(OutcomeBlocked, &PolicyBlockedError{
			Reason: "the governor returned the unknown verdict " + string(verdict.Kind)})
	}
}

// explained carries the governor's reason into the denial, so a caller sees
// why rather than only that something refused.
func explained(denial error, reason string) error {
	if reason == "" {
		return denial
	}
	switch failure := denial.(type) {
	case *PolicyBlockedError:
		if failure.Reason == "" {
			return &PolicyBlockedError{Reason: reason}
		}
		return &PolicyBlockedError{Reason: failure.Reason + ": " + reason}
	case *PolicyDeferredError:
		if failure.Reason == "" {
			return &PolicyDeferredError{Reason: reason}
		}
		return &PolicyDeferredError{Reason: failure.Reason + ": " + reason}
	default:
		return denial
	}
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func nowMicros() uint64 { return uint64(time.Now().UnixMicro()) }

// newChainID mints an identity for a chain or a decision.
func newChainID() string {
	return wire.IDFromBytes(NewOperationID()).String()
}

// QuorumPolicy is how a QuorumGovernor combines its voters into one decision.
//
// Only allow, observe and modify count as affirmative, which is to say the
// action would proceed under that voter alone. Block, step-up and defer never
// do, whatever the policy.
type QuorumPolicy struct {
	// Kind is which rule applies.
	Kind QuorumKind
	// Required is how many distinct voters must be affirmative under
	// QuorumAtLeast.
	Required int
}

// QuorumKind discriminates a quorum rule.
type QuorumKind string

const (
	// QuorumAll requires every voter to be affirmative.
	QuorumAll QuorumKind = "all"
	// QuorumAny requires at least one.
	QuorumAny QuorumKind = "any"
	// QuorumAtLeast requires a named number.
	QuorumAtLeast QuorumKind = "at_least"
)

// All requires every voter.
func All() QuorumPolicy { return QuorumPolicy{Kind: QuorumAll} }

// Any requires one voter.
func Any() QuorumPolicy { return QuorumPolicy{Kind: QuorumAny} }

// AtLeast requires a named number of voters.
func AtLeast(required int) QuorumPolicy {
	return QuorumPolicy{Kind: QuorumAtLeast, Required: required}
}

// String names the policy for the ballot an evidence record carries.
func (policy QuorumPolicy) String() string {
	if policy.Kind == QuorumAtLeast {
		return "at_least(" + itoa(policy.Required) + ")"
	}
	return string(policy.Kind)
}

func (policy QuorumPolicy) met(affirmative, total int) bool {
	switch policy.Kind {
	case QuorumAll:
		return affirmative == total
	case QuorumAny:
		return affirmative >= 1
	case QuorumAtLeast:
		return affirmative >= policy.Required
	default:
		return false
	}
}

// voter is one named governor in a quorum.
type voter struct {
	name string
	// governor is the hook this voter fronts.
	governor ActionGovernor
	// mandatory makes this voter a required affirmative, so an unavailable
	// safety voter cannot be bypassed by a loose policy.
	mandatory bool
}

// QuorumGovernor composes independent voters under a policy, and is itself a
// governor so it enrolls exactly like a single one.
//
// Every voter runs concurrently over the same action. Nothing here reaches a
// durable log: this is an in-process combinator, not the crash-safe,
// forgery-resistant propose, vote and commit lifecycle a durable version
// would need.
//
// When the quorum is met the composite verdict is the strongest affirmative
// found, in order modify, then observe, then allow: a body replacement must
// survive, and a voter wanting evidence recorded outranks one that does not.
// When it is not met the composite is the most actionable denial, in order
// block, then step-up, then defer. Every voter's own verdict is recorded in
// the reason, so the audit trail names who said what.
type QuorumGovernor struct {
	voters  []voter
	policy  QuorumPolicy
	timeout time.Duration
}

// NewQuorumGovernor opens a quorum under one policy. An empty or otherwise
// invalid configuration blocks when it is evaluated, rather than passing
// everything.
func NewQuorumGovernor(policy QuorumPolicy) *QuorumGovernor {
	return &QuorumGovernor{policy: policy, timeout: DefaultVoterTimeout}
}

// VoterTimeout bounds how long each voter may take.
//
// A voter that does not answer in time counts as an error: a mandatory one
// blocks and any other abstains, so one hung voter never stalls every
// governed effect.
func (quorum *QuorumGovernor) VoterTimeout(timeout time.Duration) *QuorumGovernor {
	quorum.timeout = timeout
	return quorum
}

// Voter enrolls one named governor. A mandatory voter must be affirmative
// before the action can proceed, whatever the policy says.
func (quorum *QuorumGovernor) Voter(
	name string, governor ActionGovernor, mandatory bool,
) *QuorumGovernor {
	quorum.voters = append(quorum.voters,
		voter{name: name, governor: governor, mandatory: mandatory})
	return quorum
}

// Decide runs every voter concurrently and combines their verdicts.
func (quorum *QuorumGovernor) Decide(
	ctx context.Context, action *GovernedAction,
) (*ActionDecision, error) {
	if refusal := quorum.misconfigured(); refusal != nil {
		return refusal, nil
	}
	ballots := quorum.poll(ctx, action)
	return quorum.combine(ballots), nil
}

// misconfigured is the refusal a configuration that cannot decide anything
// answers with, rather than letting the action through.
func (quorum *QuorumGovernor) misconfigured() *ActionDecision {
	if len(quorum.voters) == 0 {
		return Block("the quorum governor has no configured voters")
	}
	if quorum.policy.Kind == QuorumAtLeast &&
		(quorum.policy.Required == 0 || quorum.policy.Required > len(quorum.voters)) {
		return Block(fmt.Sprintf("the quorum threshold %d is invalid for %d voters",
			quorum.policy.Required, len(quorum.voters)))
	}
	seen := map[string]struct{}{}
	for _, enrolled := range quorum.voters {
		if _, twice := seen[enrolled.name]; twice {
			return Block("the quorum voter " + enrolled.name +
				" is configured more than once")
		}
		seen[enrolled.name] = struct{}{}
	}
	if quorum.timeout <= 0 {
		return Block("the quorum voter timeout must be greater than zero")
	}
	return nil
}

// ballot is one voter's answer.
type ballot struct {
	voter    voter
	decision *ActionDecision
	err      error
}

// poll runs every voter concurrently, bounding each by the voter timeout.
func (quorum *QuorumGovernor) poll(
	ctx context.Context, action *GovernedAction,
) []ballot {
	ballots := make([]ballot, len(quorum.voters))
	var waiting sync.WaitGroup
	for index, enrolled := range quorum.voters {
		waiting.Add(1)
		go func(index int, enrolled voter) {
			defer waiting.Done()
			bounded, cancel := context.WithTimeout(ctx, quorum.timeout)
			defer cancel()
			decision, err := enrolled.governor.Decide(bounded, action)
			if err == nil && decision == nil {
				err = &InvalidError{Reason: "the voter returned no decision"}
			}
			ballots[index] = ballot{voter: enrolled, decision: decision, err: err}
		}(index, enrolled)
	}
	waiting.Wait()
	return ballots
}

// combine folds the ballots into one decision.
func (quorum *QuorumGovernor) combine(ballots []ballot) *ActionDecision {
	names := make([]string, 0, len(ballots))
	affirmative := 0
	var mandatoryDenial *ActionDecision
	mandatoryError := ""
	var bestAffirmative, bestDenial *ActionDecision
	var replacement []byte
	replacing, conflicting := false, false

	for _, cast := range ballots {
		if cast.err != nil {
			names = append(names, cast.voter.name+"=error("+cast.err.Error()+")")
			// A non-mandatory voter's failure is an abstention. A mandatory
			// one's blocks, because a safety voter that did not answer has
			// not approved anything.
			if cast.voter.mandatory && mandatoryError == "" {
				mandatoryError = cast.voter.name
			}
			continue
		}
		names = append(names, cast.voter.name+"="+cast.decision.Verdict.String())
		if !isAffirmative(cast.decision.Verdict) {
			if cast.voter.mandatory && mandatoryDenial == nil {
				mandatoryDenial = cast.decision
			}
			if bestDenial == nil ||
				denialRank(cast.decision.Verdict) > denialRank(bestDenial.Verdict) {
				bestDenial = cast.decision
			}
			continue
		}
		affirmative++
		if cast.decision.Verdict.Kind == VerdictModify {
			switch {
			case !replacing:
				replacement, replacing = cast.decision.Verdict.Body, true
			case string(replacement) != string(cast.decision.Verdict.Body):
				conflicting = true
			}
		}
		if bestAffirmative == nil ||
			affirmativeRank(cast.decision.Verdict) >
				affirmativeRank(bestAffirmative.Verdict) {
			bestAffirmative = cast.decision
		}
	}

	sort.Strings(names)
	reason := "quorum(" + quorum.policy.String() + "): " + strings.Join(names, ", ")

	switch {
	case mandatoryError != "":
		return annotate(
			Block("the mandatory voter "+mandatoryError+" failed"), reason)
	case mandatoryDenial != nil:
		return annotate(mandatoryDenial, reason)
	case conflicting:
		return annotate(
			Block("the quorum voters proposed conflicting body replacements"), reason)
	case quorum.policy.met(affirmative, len(quorum.voters)):
		if bestAffirmative == nil {
			bestAffirmative = Allow()
		}
		return annotate(bestAffirmative, reason)
	case bestDenial != nil:
		return annotate(bestDenial, reason)
	default:
		return annotate(Block("no voter reached the required quorum"), reason)
	}
}

// annotate records the whole ballot in the decision's reason, so the audit
// trail names who said what rather than only what the quorum concluded.
func annotate(decision *ActionDecision, ballot string) *ActionDecision {
	combined := *decision
	if combined.Reason == "" {
		combined.Reason = ballot
	} else {
		combined.Reason = combined.Reason + "; " + ballot
	}
	return &combined
}

func isAffirmative(verdict PolicyVerdict) bool {
	switch verdict.Kind {
	case VerdictAllow, VerdictObserve, VerdictModify:
		return true
	default:
		return false
	}
}

// affirmativeRank orders modify over observe over allow: a body replacement
// must survive, evidence-wanting is the next strongest signal, and a plain
// allow is the default.
func affirmativeRank(verdict PolicyVerdict) int {
	switch verdict.Kind {
	case VerdictModify:
		return 2
	case VerdictObserve:
		return 1
	default:
		return 0
	}
}

// denialRank orders block over step-up over defer: the most actionable denial
// is the one a caller can do something about soonest.
func denialRank(verdict PolicyVerdict) int {
	switch verdict.Kind {
	case VerdictBlock:
		return 2
	case VerdictStepUp:
		return 1
	default:
		return 0
	}
}
