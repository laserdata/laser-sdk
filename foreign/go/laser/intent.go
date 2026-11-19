package laser

import (
	"encoding/hex"
	"sort"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
	"github.com/zeebo/blake3"
)

// A durable proposal for an effect. Publish an Intent like any other typed
// record, fold the Vote records that name it, and call Decide for a
// replayable outcome. This file defines record shapes and a pure fold, not a
// new wire protocol or an implicit log integration.
//
// Proposer and Voter are record claims. A deployment that needs trusted
// authorship must enforce a signed-principal or topology-isolated profile.
// This pure fold does not authenticate who appended a record.
//
// This composes with, and does not replace, ActionGovernor. A governor can
// require a committed decision before releasing an effect.

// IntentID names one proposed effect across its Intent, Vote and
// IntentDecision records.
//
// It is a time-ordered ULID like the other identities this client mints, but
// it rides as its 26-character text in every encoding, which is what the Rust
// and TypeScript clients write. It does not embed the wire id type, so it
// cannot pick up the binary form the envelope ids use.
type IntentID struct{ value wire.ID }

// NewIntentID mints a fresh, time-ordered intent id.
func NewIntentID() IntentID { return IntentID{value: wire.IDFromBytes(NewOperationID())} }

// ParseIntentID reads the 26-character Crockford base32 form.
func ParseIntentID(text string) (IntentID, error) {
	parsed, err := wire.ParseID(text)
	if err != nil {
		return IntentID{}, err
	}
	return IntentID{value: parsed}, nil
}

// String is the 26-character text form.
func (id IntentID) String() string { return id.value.String() }

// IsZero reports an unset intent id.
func (id IntentID) IsZero() bool { return id.value.IsZero() }

// VoteChoice is a durable ballot choice.
type VoteChoice string

// The ballot choices. A voter who abstains has responded, which is what
// separates abstaining from staying silent until the deadline.
const (
	VoteAllow   VoteChoice = "allow"
	VoteBlock   VoteChoice = "block"
	VoteAbstain VoteChoice = "abstain"
)

// IsRecognized reports a choice this client names.
func (choice VoteChoice) IsRecognized() bool {
	switch choice {
	case VoteAllow, VoteBlock, VoteAbstain:
		return true
	default:
		return false
	}
}

// IntentOutcome is the terminal outcome of one intent.
type IntentOutcome string

// The terminal outcomes.
const (
	IntentCommitted IntentOutcome = "committed"
	IntentAborted   IntentOutcome = "aborted"
)

// IsRecognized reports an outcome this client names.
func (outcome IntentOutcome) IsRecognized() bool {
	return outcome == IntentCommitted || outcome == IntentAborted
}

// The policy kinds, as they are spelled on the log.
const (
	policyAll     = "all"
	policyAny     = "any"
	policyAtLeast = "at_least"
)

// IntentPolicy is how Votes combine into an IntentDecision.
//
// Its fields are unexported and reached through the three constructors, so a
// policy cannot carry a threshold that its kind does not use, and the only
// malformed value a caller can build is the zero one, which Validate refuses.
type IntentPolicy struct {
	kind     string
	required int
}

// IntentAll requires every eligible voter to allow.
func IntentAll() IntentPolicy { return IntentPolicy{kind: policyAll} }

// IntentAny requires at least one eligible voter to allow.
func IntentAny() IntentPolicy { return IntentPolicy{kind: policyAny} }

// IntentAtLeast requires n distinct eligible voters to allow.
func IntentAtLeast(required int) IntentPolicy {
	return IntentPolicy{kind: policyAtLeast, required: required}
}

// Kind is the policy name: "all", "any" or "at_least".
func (policy IntentPolicy) Kind() string { return policy.kind }

// Required is the threshold an at_least policy carries, and zero otherwise.
func (policy IntentPolicy) Required() int {
	if policy.kind == policyAtLeast {
		return policy.required
	}
	return 0
}

// String renders the policy the way an operator would read it.
func (policy IntentPolicy) String() string {
	if policy.kind == policyAtLeast {
		return policyAtLeast + " " + strconv.Itoa(policy.required)
	}
	return policy.kind
}

// MarshalCBOR writes the policy externally tagged, the serde default: a
// payload-free kind is its bare name and the threshold rides as a one-entry
// map.
func (policy IntentPolicy) MarshalCBOR() (cborform.Value, error) {
	switch policy.kind {
	case policyAll, policyAny:
		return cborform.Text(policy.kind), nil
	case policyAtLeast:
		if policy.required < 0 {
			return nil, unknownPolicyError()
		}
		return cborform.Map{{
			Key:   cborform.Text(policyAtLeast),
			Value: cborform.Uint(uint64(policy.required)),
		}}, nil
	default:
		return nil, unknownPolicyError()
	}
}

// UnmarshalCBOR reads a policy another client wrote, refusing any kind this
// client does not name rather than folding votes under a rule it guessed.
func (policy *IntentPolicy) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Text:
		if string(typed) != policyAll && string(typed) != policyAny {
			return unknownPolicyError()
		}
		*policy = IntentPolicy{kind: string(typed)}
		return nil
	case cborform.Map:
		if len(typed) != 1 {
			return unknownPolicyError()
		}
		name, ok := typed[0].Key.(cborform.Text)
		if !ok || string(name) != policyAtLeast {
			return unknownPolicyError()
		}
		required, ok := typed[0].Value.(cborform.Uint)
		if !ok || uint64(required) > uint64(maxPolicyThreshold) {
			return unknownPolicyError()
		}
		*policy = IntentAtLeast(int(required))
		return nil
	default:
		return unknownPolicyError()
	}
}

// maxPolicyThreshold is the largest threshold this client will read back.
//
// A threshold is bounded by the eligible voters a record can carry, so a
// value past this is a corrupt or hostile record rather than a large quorum.
const maxPolicyThreshold = 1 << 31

// Intent is a durable proposal for an effect.
type Intent struct {
	// IntentID is a fresh ULID stamped at build time.
	IntentID IntentID
	// Conversation is the conversation and usual partition key.
	Conversation wire.ConversationID
	// Proposer is the proposing agent.
	Proposer wire.AgentID
	// Body is the proposed effect, opaque to this file.
	Body []byte
	// Digest is BLAKE3 over the canonical named-field CBOR encoding of Body.
	Digest string
	// EligibleVoters are the only principals whose votes count.
	EligibleVoters []wire.AgentID
	// MandatoryVoters are eligible principals that must allow before commit.
	MandatoryVoters []wire.AgentID
	// Policy is how affirmative votes combine.
	Policy IntentPolicy
	// PolicyVersion is the policy version frozen when this intent was
	// proposed.
	PolicyVersion uint64
	// DeadlineAtMicros is the epoch micros after which an incomplete intent
	// aborts.
	DeadlineAtMicros uint64
	// AtMicros is the proposal time, in epoch micros.
	AtMicros uint64
}

// IntentOptions is what an intent is proposed with. The id, digest and
// proposal time are stamped by NewIntent rather than supplied.
type IntentOptions struct {
	Conversation     wire.ConversationID
	Proposer         wire.AgentID
	Body             []byte
	EligibleVoters   []wire.AgentID
	MandatoryVoters  []wire.AgentID
	Policy           IntentPolicy
	PolicyVersion    uint64
	DeadlineAtMicros uint64
}

// NewIntent mints the intent id, hashes the body, stamps the proposal time
// and validates the result, so an intent that exists is one that can be
// published.
func NewIntent(options IntentOptions) (Intent, error) {
	digest, err := digestOf(options.Body)
	if err != nil {
		return Intent{}, err
	}
	intent := Intent{
		IntentID:         NewIntentID(),
		Conversation:     options.Conversation,
		Proposer:         options.Proposer,
		Body:             cloneBody(options.Body),
		Digest:           digest,
		EligibleVoters:   cloneAgents(options.EligibleVoters),
		MandatoryVoters:  cloneAgents(options.MandatoryVoters),
		Policy:           options.Policy,
		PolicyVersion:    options.PolicyVersion,
		DeadlineAtMicros: options.DeadlineAtMicros,
		AtMicros:         nowMicros(),
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

// Validate checks a constructed or decoded intent.
func (intent Intent) Validate() error {
	if len(intent.EligibleVoters) == 0 {
		return &IntentError{Reason: "an intent requires at least one eligible voter"}
	}
	if intent.Proposer.IsZero() {
		return &IntentError{Reason: "an intent requires a proposer"}
	}

	eligible := make(map[wire.AgentID]struct{}, len(intent.EligibleVoters))
	for _, voter := range intent.EligibleVoters {
		if voter.IsZero() {
			return &IntentError{Reason: "an eligible voter must be named"}
		}
		if _, repeated := eligible[voter]; repeated {
			return &IntentError{
				Reason: "eligible voter '" + voter.String() + "' appears more than once",
				Voter:  voter.String(),
			}
		}
		eligible[voter] = struct{}{}
	}

	mandatory := make(map[wire.AgentID]struct{}, len(intent.MandatoryVoters))
	for _, voter := range intent.MandatoryVoters {
		if _, repeated := mandatory[voter]; repeated {
			return &IntentError{
				Reason: "mandatory voter '" + voter.String() + "' appears more than once",
				Voter:  voter.String(),
			}
		}
		mandatory[voter] = struct{}{}
		if _, allowed := eligible[voter]; !allowed {
			return &IntentError{
				Reason: "mandatory voter '" + voter.String() + "' is not eligible",
				Voter:  voter.String(),
			}
		}
	}

	switch intent.Policy.kind {
	case policyAll, policyAny:
	case policyAtLeast:
		if intent.Policy.required <= 0 || intent.Policy.required > len(intent.EligibleVoters) {
			return &IntentError{
				Reason: "threshold " + strconv.Itoa(intent.Policy.required) +
					" is invalid for " + strconv.Itoa(len(intent.EligibleVoters)) +
					" eligible voters",
				Required: intent.Policy.required,
				Eligible: len(intent.EligibleVoters),
			}
		}
	default:
		return unknownPolicyError()
	}

	if intent.DeadlineAtMicros <= intent.AtMicros {
		return &IntentError{
			Reason: "deadline " + strconv.FormatUint(intent.DeadlineAtMicros, 10) +
				" must be after proposal time " +
				strconv.FormatUint(intent.AtMicros, 10),
			Proposed: intent.AtMicros,
			Deadline: intent.DeadlineAtMicros,
		}
	}
	digest, err := digestOf(intent.Body)
	if err != nil {
		return err
	}
	if intent.Digest != digest {
		return &IntentError{Reason: "intent digest does not match its body"}
	}
	return nil
}

// CastVote is a ballot bound to this intent's digest and policy version.
func (intent Intent) CastVote(voter wire.AgentID, choice VoteChoice) (Vote, error) {
	if err := intent.Validate(); err != nil {
		return Vote{}, err
	}
	if !containsAgent(intent.EligibleVoters, voter) {
		return Vote{}, &IntentError{
			Reason: "voter '" + voter.String() + "' is not eligible for this intent",
			Voter:  voter.String(),
		}
	}
	if !choice.IsRecognized() {
		return Vote{}, &IntentError{Reason: "a ballot must be allow, block or abstain"}
	}
	return Vote{
		IntentID:      intent.IntentID,
		IntentDigest:  intent.Digest,
		PolicyVersion: intent.PolicyVersion,
		Voter:         voter,
		Choice:        choice,
		AtMicros:      nowMicros(),
	}, nil
}

// Vote is one principal's claimed ballot on one intent. Trusted authorship
// comes from the deployment's signing or topic-write policy, not this record
// shape.
type Vote struct {
	IntentID      IntentID
	IntentDigest  string
	PolicyVersion uint64
	Voter         wire.AgentID
	Choice        VoteChoice
	AtMicros      uint64
}

// ObservedVote is one ballot as a reader saw it: the Vote record and the time
// the log assigned it, the record's server timestamp.
//
// Decide windows and orders by ObservedAtMicros and keeps the voter's own
// AtMicros only as recorded evidence, so a late ballot cannot claim an
// earlier time.
type ObservedVote struct {
	Vote             Vote
	ObservedAtMicros uint64
}

// Ballot is one considered (voter, choice) pair. It rides as the two-element
// array the Rust tuple encodes to.
type Ballot struct {
	Voter  wire.AgentID
	Choice VoteChoice
}

// MarshalCBOR writes the pair as a two-element array.
func (ballot Ballot) MarshalCBOR() (cborform.Value, error) {
	if !ballot.Choice.IsRecognized() {
		return nil, &IntentError{Reason: "a ballot must be allow, block or abstain"}
	}
	return cborform.Array{
		cborform.Text(ballot.Voter.String()),
		cborform.Text(string(ballot.Choice)),
	}, nil
}

// UnmarshalCBOR reads the pair back, refusing a choice this client does not
// name.
func (ballot *Ballot) UnmarshalCBOR(item cborform.Value) error {
	pair, ok := item.(cborform.Array)
	if !ok || len(pair) != 2 {
		return &IntentError{Reason: "a considered vote is a voter and a choice"}
	}
	name, nameOK := pair[0].(cborform.Text)
	choice, choiceOK := pair[1].(cborform.Text)
	if !nameOK || !choiceOK {
		return &IntentError{Reason: "a considered vote is a voter and a choice"}
	}
	voter, err := wire.NewAgentID(string(name))
	if err != nil {
		return err
	}
	if !VoteChoice(choice).IsRecognized() {
		return &IntentError{Reason: "a ballot must be allow, block or abstain"}
	}
	*ballot = Ballot{Voter: voter, Choice: VoteChoice(choice)}
	return nil
}

// IntentDecision is a terminal outcome bound to one exact intent body and
// policy version.
//
// It is named apart from the delivery Decision an agent handler reaches,
// because one package holds both and a reader must never have to ask which
// kind of decision a value is.
type IntentDecision struct {
	IntentID      IntentID
	IntentDigest  string
	PolicyVersion uint64
	Outcome       IntentOutcome
	Reason        string
	// VotesConsidered are the canonically ordered pairs the fold used.
	VotesConsidered []Ballot
	// AtMicros is the fact time that made the decision terminal, not replay
	// wall time.
	AtMicros uint64
}

// Authorizes reports whether this decision releases intent's effect.
//
// An aborted decision is valid evidence but does not authorize. A committed
// one authorizes only when its considered votes meet the intent's quorum, so
// a decision record without them is refused rather than trusted.
func (decision IntentDecision) Authorizes(intent Intent) (bool, error) {
	if err := intent.Validate(); err != nil {
		return false, err
	}
	if decision.IntentID != intent.IntentID ||
		decision.IntentDigest != intent.Digest ||
		decision.PolicyVersion != intent.PolicyVersion {
		return false, &IntentError{
			Reason: "decision is not bound to this intent body and policy version"}
	}
	if decision.Outcome != IntentCommitted {
		return false, nil
	}
	if !commitSupported(intent, decision.VotesConsidered) {
		return false, &IntentError{
			Reason: "decision votes do not meet this intent's quorum"}
	}
	return true, nil
}

// Decide folds ballots into a decision, or returns no decision while the
// outcome remains reachable before the deadline.
//
// Each ballot is windowed and ordered by the time the reader observed it, the
// record's server timestamp, never by the voter's own AtMicros. Invalid
// ballots are ignored. Ballots observed outside the proposal-to-deadline
// window or after nowMicros are invalid. Identical repeats are idempotent.
// Conflicting choices by one voter abort. Mandatory voters must allow before
// a quorum can commit. The valid ballot set is sorted before folding, so
// input and replay order do not affect the decision or its evidence.
func Decide(intent Intent, votes []ObservedVote, nowMicros uint64) (*IntentDecision, error) {
	if err := intent.Validate(); err != nil {
		return nil, err
	}
	eligible := make(map[wire.AgentID]struct{}, len(intent.EligibleVoters))
	for _, voter := range intent.EligibleVoters {
		eligible[voter] = struct{}{}
	}
	valid := make([]ObservedVote, 0, len(votes))
	for _, observed := range votes {
		_, allowed := eligible[observed.Vote.Voter]
		if observed.Vote.IntentID == intent.IntentID &&
			observed.Vote.IntentDigest == intent.Digest &&
			observed.Vote.PolicyVersion == intent.PolicyVersion &&
			allowed &&
			observed.ObservedAtMicros >= intent.AtMicros &&
			observed.ObservedAtMicros <= intent.DeadlineAtMicros &&
			observed.ObservedAtMicros <= nowMicros {
			valid = append(valid, observed)
		}
	}
	sort.Slice(valid, func(left, right int) bool {
		if valid[left].Vote.Voter.String() != valid[right].Vote.Voter.String() {
			return valid[left].Vote.Voter.String() < valid[right].Vote.Voter.String()
		}
		if valid[left].ObservedAtMicros != valid[right].ObservedAtMicros {
			return valid[left].ObservedAtMicros < valid[right].ObservedAtMicros
		}
		return choiceRank(valid[left].Vote.Choice) < choiceRank(valid[right].Vote.Choice)
	})

	ballots := make(map[wire.AgentID]VoteChoice, len(valid))
	var considered []Ballot
	terminalAt := intent.AtMicros
	for _, observed := range valid {
		if observed.ObservedAtMicros > terminalAt {
			terminalAt = observed.ObservedAtMicros
		}
		existing, cast := ballots[observed.Vote.Voter]
		switch {
		case !cast:
			ballots[observed.Vote.Voter] = observed.Vote.Choice
			considered = append(considered,
				Ballot{Voter: observed.Vote.Voter, Choice: observed.Vote.Choice})
		case existing == observed.Vote.Choice:
		default:
			considered = append(considered,
				Ballot{Voter: observed.Vote.Voter, Choice: observed.Vote.Choice})
			return sealed(intent, IntentAborted,
				"voter '"+observed.Vote.Voter.String()+"' cast conflicting votes",
				considered, terminalAt), nil
		}
	}

	for _, voter := range intent.MandatoryVoters {
		if choice, cast := ballots[voter]; cast && choice != VoteAllow {
			return sealed(intent, IntentAborted,
				"mandatory voter '"+voter.String()+"' did not allow",
				considered, terminalAt), nil
		}
	}

	allow := 0
	for _, choice := range ballots {
		if choice == VoteAllow {
			allow++
		}
	}
	responded, total := len(ballots), len(intent.EligibleVoters)
	mandatoryMet := true
	for _, voter := range intent.MandatoryVoters {
		if ballots[voter] != VoteAllow {
			mandatoryMet = false
		}
	}
	if quorumMet(intent.Policy, allow, total) && mandatoryMet {
		return sealed(intent, IntentCommitted, "quorum met", considered, terminalAt), nil
	}

	impossible := false
	switch intent.Policy.kind {
	case policyAll:
		impossible = responded > allow
	case policyAny:
		impossible = responded == total && allow == 0
	case policyAtLeast:
		impossible = allow+(total-responded) < intent.Policy.required
	}
	deadlinePassed := nowMicros >= intent.DeadlineAtMicros
	if !impossible && !deadlinePassed {
		return nil, nil
	}
	reason := "quorum not reached by deadline"
	switch {
	case deadlinePassed && !mandatoryMet:
		reason = "mandatory approval not reached by deadline"
	case impossible:
		reason = "quorum became impossible"
	}
	at := terminalAt
	if deadlinePassed {
		at = intent.DeadlineAtMicros
	}
	return sealed(intent, IntentAborted, reason, considered, at), nil
}

func quorumMet(policy IntentPolicy, allow, total int) bool {
	switch policy.kind {
	case policyAll:
		return allow == total
	case policyAny:
		return allow >= 1
	default:
		return allow >= policy.required
	}
}

// commitSupported reports whether votes alone justify committing intent:
// eligible voters, one choice each, every mandatory voter allowing, and the
// quorum met.
func commitSupported(intent Intent, votes []Ballot) bool {
	ballots := make(map[wire.AgentID]VoteChoice, len(votes))
	for _, ballot := range votes {
		if !containsAgent(intent.EligibleVoters, ballot.Voter) {
			return false
		}
		if existing, cast := ballots[ballot.Voter]; cast && existing != ballot.Choice {
			return false
		}
		ballots[ballot.Voter] = ballot.Choice
	}
	allow := 0
	for _, choice := range ballots {
		if choice == VoteAllow {
			allow++
		}
	}
	for _, voter := range intent.MandatoryVoters {
		if ballots[voter] != VoteAllow {
			return false
		}
	}
	return quorumMet(intent.Policy, allow, len(intent.EligibleVoters))
}

// choiceRank orders the choices within one voter and observation time, so two
// readers fold the same evidence in the same order.
func choiceRank(choice VoteChoice) int {
	switch choice {
	case VoteAllow:
		return 0
	case VoteBlock:
		return 1
	default:
		return 2
	}
}

func sealed(
	intent Intent, outcome IntentOutcome, reason string, votes []Ballot, atMicros uint64,
) *IntentDecision {
	considered := make([]Ballot, len(votes))
	copy(considered, votes)
	return &IntentDecision{
		IntentID:        intent.IntentID,
		IntentDigest:    intent.Digest,
		PolicyVersion:   intent.PolicyVersion,
		Outcome:         outcome,
		Reason:          reason,
		VotesConsidered: considered,
		AtMicros:        atMicros,
	}
}

// digestOf hashes the body the way every client does: one canonical CBOR byte
// string under the name "body", then BLAKE3, then lower-case hex. A client
// that hashed the raw bytes instead would disagree with every other.
func digestOf(body []byte) (string, error) {
	canonical, err := wire.Marshal(struct {
		Body []byte `cbor:"body"`
	}{Body: cloneBody(body)})
	if err != nil {
		return "", &IntentError{
			Reason: "intent body does not encode for its digest: " + err.Error(),
			Cause:  err,
		}
	}
	sum := blake3.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// cloneBody keeps a body the caller can no longer reach, and makes an absent
// one the empty byte string rather than null, which is what the other clients
// encode.
func cloneBody(body []byte) []byte {
	cloned := make([]byte, len(body))
	copy(cloned, body)
	return cloned
}

func cloneAgents(agents []wire.AgentID) []wire.AgentID {
	if len(agents) == 0 {
		return nil
	}
	cloned := make([]wire.AgentID, len(agents))
	copy(cloned, agents)
	return cloned
}

func containsAgent(agents []wire.AgentID, wanted wire.AgentID) bool {
	for _, agent := range agents {
		if agent == wanted {
			return true
		}
	}
	return false
}

func unknownPolicyError() error {
	return &IntentError{
		Reason: `intent policy must be "all", "any", or {"at_least": n}`}
}

// IntentError reports an invalid durable-intent configuration or operation.
//
// Each Reason is one Rust IntentError variant, word for word, with its
// payload in the fields beside it. Rust raises these as LaserError::Invalid
// and the other clients as their invalid-argument class, so this unwraps to
// InvalidError and a caller matching the broad class sees an intent failure
// as one.
type IntentError struct {
	Reason string
	// Voter names the voter a duplicate, eligibility or mandatory rule
	// rejected, and is empty for the rules that name none.
	Voter string
	// Required and Eligible carry a rejected threshold.
	Required, Eligible int
	// Proposed and Deadline carry a rejected window.
	Proposed, Deadline uint64
	// Cause is the encoding failure behind a digest error.
	Cause error
}

func (e *IntentError) Error() string { return e.Reason }

func (e *IntentError) Unwrap() []error {
	invalid := error(&InvalidError{Reason: e.Reason})
	if e.Cause == nil {
		return []error{invalid}
	}
	return []error{invalid, e.Cause}
}
