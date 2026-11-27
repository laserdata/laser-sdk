package laser

import (
	"sort"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ContextMessage is one record as context assembly sees it.
//
// It is what a prompt is built from, so it carries the decoded envelope
// alongside the raw payload: a reader needs the routing to decide whether a
// record belongs in the context at all.
type ContextMessage struct {
	// Record is where the message came from.
	Record *Record
	// Envelope is the decoded AGDX envelope when the record carries one, nil
	// when it does not.
	Envelope *wire.AgentEnvelope
	// Topic is the name of the topic it was read from, so a reader can tell
	// session traffic from memory.
	Topic string
	// TimestampMicros is the server's append time, which is the one clock
	// every topic shares.
	TimestampMicros uint64
}

// Body is the envelope body of an AGDX message, or the raw payload otherwise:
// the text a prompt carries and the bytes a token estimate counts.
func (message ContextMessage) Body() []byte {
	if message.Envelope != nil {
		return message.Envelope.Body
	}
	if message.Record == nil {
		return nil
	}
	return message.Record.Payload()
}

// Agent is who wrote the message, nil when it says.
func (message ContextMessage) Agent() *wire.AgentID {
	if message.Envelope != nil {
		return &message.Envelope.Source
	}
	if message.Record == nil || message.Record.Provenance == nil {
		return nil
	}
	return message.Record.Provenance.Agent
}

// NewContextMessage reads a record into the shape context assembly works on.
func NewContextMessage(record *Record, topic string) ContextMessage {
	message := ContextMessage{
		Record:          record,
		Topic:           topic,
		TimestampMicros: record.Message.Header.Timestamp,
	}
	// A record that is not an agent message is still context: a plain data
	// topic carries plenty worth putting in a prompt. Only the envelope is
	// missing, not the message.
	if agdx, err := OpenAgdx(record); err == nil {
		envelope := agdx.Envelope
		message.Envelope = &envelope
	}
	return message
}

// ContextPolicy narrows a history down to what belongs in a prompt.
//
// A policy returns the messages it keeps rather than mutating the history, so
// policies compose and a caller can see what each one did.
type ContextPolicy interface {
	// Name says what the policy is, for a log or a trace.
	Name() string
	// Select keeps what belongs. The history is in order, oldest first.
	Select(history []ContextMessage) []ContextMessage
}

// LastN keeps the most recent n messages.
type LastN int

func (policy LastN) Name() string { return "last_n(" + itoa(int(policy)) + ")" }

func (policy LastN) Select(history []ContextMessage) []ContextMessage {
	if int(policy) >= len(history) {
		return append([]ContextMessage(nil), history...)
	}
	if policy <= 0 {
		return nil
	}
	return append([]ContextMessage(nil), history[len(history)-int(policy):]...)
}

// RoleFilter keeps only what the named agents wrote.
type RoleFilter struct{ Agents []wire.AgentID }

func (policy RoleFilter) Name() string { return "role_filter" }

func (policy RoleFilter) Select(history []ContextMessage) []ContextMessage {
	wanted := make(map[wire.AgentID]struct{}, len(policy.Agents))
	for _, agent := range policy.Agents {
		wanted[agent] = struct{}{}
	}
	kept := make([]ContextMessage, 0, len(history))
	for _, message := range history {
		agent := message.Agent()
		if agent == nil {
			// A message that names no author cannot be attributed, so a filter
			// by author does not keep it.
			continue
		}
		if _, held := wanted[*agent]; held {
			kept = append(kept, message)
		}
	}
	return kept
}

// Chain applies policies in order, each narrowing what the last kept.
//
// It is what lets a caller pipeline a role filter, then a recency cut, then a
// budget into one slot rather than choosing exactly one policy.
type Chain []ContextPolicy

func (policy Chain) Name() string {
	names := make([]string, 0, len(policy))
	for _, step := range policy {
		names = append(names, step.Name())
	}
	return "chain(" + strings.Join(names, ",") + ")"
}

func (policy Chain) Select(history []ContextMessage) []ContextMessage {
	current := append([]ContextMessage(nil), history...)
	for _, step := range policy {
		current = step.Select(current)
	}
	return current
}

// TokenEstimator sizes one message against a budget.
type TokenEstimator func(ContextMessage) int

// TokenBudget keeps the most recent messages that fit a budget.
//
// It meets memory's own budget in the middle, so a prompt built from history
// and recalled memory spends one notion of a token rather than two.
type TokenBudget struct {
	MaxTokens int
	// Estimate sizes a message. Nil uses the contract's own estimate, which is
	// coarse on purpose: a real count needs the model's tokenizer, and
	// carrying one would tie this client to a provider.
	Estimate TokenEstimator
}

func (policy TokenBudget) Name() string {
	return "token_budget(" + itoa(policy.MaxTokens) + ")"
}

func (policy TokenBudget) Select(history []ContextMessage) []ContextMessage {
	estimate := policy.Estimate
	if estimate == nil {
		estimate = func(message ContextMessage) int {
			return int(wire.EstimateTokens(len(message.Body())))
		}
	}
	// Walked newest first, so what survives a budget is the most recent tail
	// rather than the oldest head: a prompt wants what just happened.
	var kept []ContextMessage
	total := 0
	for index := len(history) - 1; index >= 0; index-- {
		cost := estimate(history[index])
		// At least one message is always kept, so a single message larger than
		// the whole budget is truncated by the model rather than silently
		// vanishing before it gets there.
		if len(kept) > 0 && total+cost > policy.MaxTokens {
			break
		}
		total += cost
		kept = append(kept, history[index])
	}
	reverse(kept)
	return kept
}

func reverse(messages []ContextMessage) {
	for first, last := 0, len(messages)-1; first < last; first, last = first+1, last-1 {
		messages[first], messages[last] = messages[last], messages[first]
	}
}

// ContextAssembler builds a conversation's history into the messages a prompt
// carries.
//
// Ordering is best effort by the server's append time, deterministic on ties,
// and not strictly chronological across topics: Apache Iggy orders per
// partition, so no total order exists across them to read.
type ContextAssembler struct {
	conversation wire.ConversationID
	policy       ContextPolicy
	history      []ContextMessage
}

// Assemble opens an assembler for one conversation.
func Assemble(conversation wire.ConversationID) *ContextAssembler {
	return &ContextAssembler{conversation: conversation}
}

// WithPolicy sets what belongs in the context. Without one, everything read
// belongs.
func (assembler *ContextAssembler) WithPolicy(policy ContextPolicy) *ContextAssembler {
	assembler.policy = policy
	return assembler
}

// Add offers records to the assembler, keeping those belonging to its
// conversation.
//
// Records from another conversation are left out rather than reported: a
// topic carries many conversations, so reading one and filtering is the normal
// path, not an error.
func (assembler *ContextAssembler) Add(topic string, records ...*Record) *ContextAssembler {
	for _, record := range records {
		if record == nil || record.Provenance == nil {
			continue
		}
		if record.Provenance.Conversation != assembler.conversation {
			continue
		}
		assembler.history = append(assembler.history, NewContextMessage(record, topic))
	}
	return assembler
}

// Messages is the assembled context, ordered and narrowed by the policy.
func (assembler *ContextAssembler) Messages() []ContextMessage {
	ordered := append([]ContextMessage(nil), assembler.history...)
	sort.SliceStable(ordered, func(first, second int) bool {
		left, right := ordered[first], ordered[second]
		if left.TimestampMicros != right.TimestampMicros {
			return left.TimestampMicros < right.TimestampMicros
		}
		// Two records can share a timestamp across topics, and a prompt that
		// reorders between runs is not reproducible, so the tie is broken on
		// something stable rather than left to the sort.
		if left.Topic != right.Topic {
			return left.Topic < right.Topic
		}
		return left.Record.Offset() < right.Record.Offset()
	})
	if assembler.policy == nil {
		return ordered
	}
	return assembler.policy.Select(ordered)
}

// Conversation is the conversation being assembled.
func (assembler *ContextAssembler) Conversation() wire.ConversationID {
	return assembler.conversation
}
