package laser

import (
	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The on-log shapes of the three intent records.
//
// The SDK-level ids ride as their text in every encoding, which is what Rust's
// `into = "String"` and the TypeScript `toJSON` write, and is not the binary
// form the envelope ids use. The mirrors below say so once, so the CBOR a
// record is published as and the JSON an operator reads carry the same fields
// in the same order as the other clients.

// bodyBytes is a record's body as the other clients encode it.
//
// Serde writes a Vec<u8> as an array of integers, so the body that rides the
// log is an array and not the CBOR byte string the digest input uses. The two
// shapes are deliberately different: the digest hashes one byte string so the
// hash is cheap and stable, while the record keeps serde's default so a
// Rust or TypeScript reader decodes it without a custom visitor.
type bodyBytes []byte

// MarshalCBOR writes the body as an array of integers.
func (body bodyBytes) MarshalCBOR() (cborform.Value, error) {
	items := make(cborform.Array, len(body))
	for index, octet := range body {
		items[index] = cborform.Uint(octet)
	}
	return items, nil
}

// UnmarshalCBOR reads the array back, and accepts the byte string a client
// that encoded it compactly would have written.
func (body *bodyBytes) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Bytes:
		*body = bodyBytes(typed)
		return nil
	case cborform.Array:
		decoded := make(bodyBytes, len(typed))
		for index, element := range typed {
			octet, ok := element.(cborform.Uint)
			if !ok || octet > 0xff {
				return &IntentError{Reason: "an intent body must be an array of bytes"}
			}
			decoded[index] = byte(octet)
		}
		*body = decoded
		return nil
	default:
		return &IntentError{Reason: "an intent body must be an array of bytes"}
	}
}

type intentRecord struct {
	IntentID         string       `cbor:"intent_id"`
	Conversation     string       `cbor:"conversation"`
	Proposer         string       `cbor:"proposer"`
	Body             bodyBytes    `cbor:"body"`
	Digest           string       `cbor:"digest"`
	EligibleVoters   []string     `cbor:"eligible_voters"`
	MandatoryVoters  []string     `cbor:"mandatory_voters"`
	Policy           IntentPolicy `cbor:"policy"`
	PolicyVersion    uint64       `cbor:"policy_version"`
	DeadlineAtMicros uint64       `cbor:"deadline_at_micros"`
	AtMicros         uint64       `cbor:"at_micros"`
}

type voteRecord struct {
	IntentID      string `cbor:"intent_id"`
	IntentDigest  string `cbor:"intent_digest"`
	PolicyVersion uint64 `cbor:"policy_version"`
	Voter         string `cbor:"voter"`
	Choice        string `cbor:"choice"`
	AtMicros      uint64 `cbor:"at_micros"`
}

type decisionRecord struct {
	IntentID        string   `cbor:"intent_id"`
	IntentDigest    string   `cbor:"intent_digest"`
	PolicyVersion   uint64   `cbor:"policy_version"`
	Outcome         string   `cbor:"outcome"`
	Reason          string   `cbor:"reason"`
	VotesConsidered []Ballot `cbor:"votes_considered"`
	AtMicros        uint64   `cbor:"at_micros"`
}

func (intent Intent) record() intentRecord {
	return intentRecord{
		IntentID:         intent.IntentID.String(),
		Conversation:     intent.Conversation.String(),
		Proposer:         intent.Proposer.String(),
		Body:             bodyBytes(cloneBody(intent.Body)),
		Digest:           intent.Digest,
		EligibleVoters:   agentNames(intent.EligibleVoters),
		MandatoryVoters:  agentNames(intent.MandatoryVoters),
		Policy:           intent.Policy,
		PolicyVersion:    intent.PolicyVersion,
		DeadlineAtMicros: intent.DeadlineAtMicros,
		AtMicros:         intent.AtMicros,
	}
}

func (record intentRecord) intent() (Intent, error) {
	intentID, err := ParseIntentID(record.IntentID)
	if err != nil {
		return Intent{}, &IntentError{Reason: "intent intent_id must be a ULID", Cause: err}
	}
	conversation, err := wire.ParseID(record.Conversation)
	if err != nil {
		return Intent{}, &IntentError{Reason: "intent conversation must be a ULID", Cause: err}
	}
	proposer, err := wire.NewAgentID(record.Proposer)
	if err != nil {
		return Intent{}, &IntentError{Reason: "intent proposer must be an agent id", Cause: err}
	}
	eligible, err := agentIDs(record.EligibleVoters, "eligible_voters")
	if err != nil {
		return Intent{}, err
	}
	mandatory, err := agentIDs(record.MandatoryVoters, "mandatory_voters")
	if err != nil {
		return Intent{}, err
	}
	intent := Intent{
		IntentID:         intentID,
		Conversation:     wire.ConversationID{ID: conversation},
		Proposer:         proposer,
		Body:             cloneBody(record.Body),
		Digest:           record.Digest,
		EligibleVoters:   eligible,
		MandatoryVoters:  mandatory,
		Policy:           record.Policy,
		PolicyVersion:    record.PolicyVersion,
		DeadlineAtMicros: record.DeadlineAtMicros,
		AtMicros:         record.AtMicros,
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

// Encode is the canonical encoding this intent rides the log as.
func (intent Intent) Encode() ([]byte, error) {
	encoded, err := wire.Marshal(intent.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the intent", Cause: err}
	}
	return encoded, nil
}

// DecodeIntent reads an intent another client published, validating it before
// handing it back: an intent whose digest or voter set does not hold is not
// one to fold votes against.
func DecodeIntent(payload []byte) (Intent, error) {
	var record intentRecord
	if err := wire.Unmarshal(payload, &record); err != nil {
		return Intent{}, &CodecError{Reason: "decode the intent", Cause: err}
	}
	return record.intent()
}

// MarshalJSON writes the intent as the snake_case JSON the Rust, Python and
// TypeScript clients write, with the body as an array of bytes.
func (intent Intent) MarshalJSON() ([]byte, error) {
	encoded, err := wire.MarshalJSON(intent.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the intent", Cause: err}
	}
	return encoded, nil
}

// UnmarshalJSON rebuilds an intent another client wrote. The id, digest and
// proposal time are kept as written, then validated.
func (intent *Intent) UnmarshalJSON(data []byte) error {
	var record intentRecord
	if err := wire.UnmarshalJSON(data, &record); err != nil {
		return &CodecError{Reason: "decode the intent", Cause: err}
	}
	decoded, err := record.intent()
	if err != nil {
		return err
	}
	*intent = decoded
	return nil
}

func (vote Vote) record() voteRecord {
	return voteRecord{
		IntentID:      vote.IntentID.String(),
		IntentDigest:  vote.IntentDigest,
		PolicyVersion: vote.PolicyVersion,
		Voter:         vote.Voter.String(),
		Choice:        string(vote.Choice),
		AtMicros:      vote.AtMicros,
	}
}

func (record voteRecord) vote() (Vote, error) {
	intentID, err := ParseIntentID(record.IntentID)
	if err != nil {
		return Vote{}, &IntentError{Reason: "vote intent_id must be a ULID", Cause: err}
	}
	voter, err := wire.NewAgentID(record.Voter)
	if err != nil {
		return Vote{}, &IntentError{Reason: "vote voter must be an agent id", Cause: err}
	}
	choice := VoteChoice(record.Choice)
	if !choice.IsRecognized() {
		return Vote{}, &IntentError{Reason: "a ballot must be allow, block or abstain"}
	}
	return Vote{
		IntentID:      intentID,
		IntentDigest:  record.IntentDigest,
		PolicyVersion: record.PolicyVersion,
		Voter:         voter,
		Choice:        choice,
		AtMicros:      record.AtMicros,
	}, nil
}

// Encode is the canonical encoding this ballot rides the log as.
func (vote Vote) Encode() ([]byte, error) {
	encoded, err := wire.Marshal(vote.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the vote", Cause: err}
	}
	return encoded, nil
}

// DecodeVote reads a ballot another client cast.
func DecodeVote(payload []byte) (Vote, error) {
	var record voteRecord
	if err := wire.Unmarshal(payload, &record); err != nil {
		return Vote{}, &CodecError{Reason: "decode the vote", Cause: err}
	}
	return record.vote()
}

// MarshalJSON writes the ballot as the snake_case JSON the other clients
// write.
func (vote Vote) MarshalJSON() ([]byte, error) {
	encoded, err := wire.MarshalJSON(vote.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the vote", Cause: err}
	}
	return encoded, nil
}

// UnmarshalJSON reads a ballot another client wrote.
func (vote *Vote) UnmarshalJSON(data []byte) error {
	var record voteRecord
	if err := wire.UnmarshalJSON(data, &record); err != nil {
		return &CodecError{Reason: "decode the vote", Cause: err}
	}
	decoded, err := record.vote()
	if err != nil {
		return err
	}
	*vote = decoded
	return nil
}

func (decision IntentDecision) record() decisionRecord {
	considered := make([]Ballot, len(decision.VotesConsidered))
	copy(considered, decision.VotesConsidered)
	return decisionRecord{
		IntentID:        decision.IntentID.String(),
		IntentDigest:    decision.IntentDigest,
		PolicyVersion:   decision.PolicyVersion,
		Outcome:         string(decision.Outcome),
		Reason:          decision.Reason,
		VotesConsidered: considered,
		AtMicros:        decision.AtMicros,
	}
}

func (record decisionRecord) decision() (IntentDecision, error) {
	intentID, err := ParseIntentID(record.IntentID)
	if err != nil {
		return IntentDecision{}, &IntentError{Reason: "decision intent_id must be a ULID", Cause: err}
	}
	outcome := IntentOutcome(record.Outcome)
	if !outcome.IsRecognized() {
		return IntentDecision{}, &IntentError{
			Reason: "a decision outcome must be committed or aborted"}
	}
	return IntentDecision{
		IntentID:        intentID,
		IntentDigest:    record.IntentDigest,
		PolicyVersion:   record.PolicyVersion,
		Outcome:         outcome,
		Reason:          record.Reason,
		VotesConsidered: record.VotesConsidered,
		AtMicros:        record.AtMicros,
	}, nil
}

// Encode is the canonical encoding this decision rides the log as.
func (decision IntentDecision) Encode() ([]byte, error) {
	encoded, err := wire.Marshal(decision.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the decision", Cause: err}
	}
	return encoded, nil
}

// DecodeIntentDecision reads a decision another client sealed.
func DecodeIntentDecision(payload []byte) (IntentDecision, error) {
	var record decisionRecord
	if err := wire.Unmarshal(payload, &record); err != nil {
		return IntentDecision{}, &CodecError{Reason: "decode the decision", Cause: err}
	}
	return record.decision()
}

// MarshalJSON writes the decision as the snake_case JSON the other clients
// write, with each considered vote as a voter and choice pair.
func (decision IntentDecision) MarshalJSON() ([]byte, error) {
	encoded, err := wire.MarshalJSON(decision.record())
	if err != nil {
		return nil, &CodecError{Reason: "encode the decision", Cause: err}
	}
	return encoded, nil
}

// UnmarshalJSON reads a decision another client wrote.
func (decision *IntentDecision) UnmarshalJSON(data []byte) error {
	var record decisionRecord
	if err := wire.UnmarshalJSON(data, &record); err != nil {
		return &CodecError{Reason: "decode the decision", Cause: err}
	}
	decoded, err := record.decision()
	if err != nil {
		return err
	}
	*decision = decoded
	return nil
}

func agentNames(agents []wire.AgentID) []string {
	names := make([]string, len(agents))
	for index, agent := range agents {
		names[index] = agent.String()
	}
	return names
}

func agentIDs(names []string, field string) ([]wire.AgentID, error) {
	if len(names) == 0 {
		return nil, nil
	}
	agents := make([]wire.AgentID, len(names))
	for index, name := range names {
		agent, err := wire.NewAgentID(name)
		if err != nil {
			return nil, &IntentError{
				Reason: "intent " + field + " must be agent ids", Cause: err}
		}
		agents[index] = agent
	}
	return agents, nil
}
