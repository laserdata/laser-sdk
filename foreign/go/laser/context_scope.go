package laser

import (
	"context"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ContextScope is one conversation's working context: append to it, read it
// back bounded, or render the prompt-ready block.
//
// It is never a second store. Appending rides an ordinary publish keyed by
// the conversation, and reading rides the context assembler, so what a
// handler sees is the log rather than a copy of it that can drift.
type ContextScope struct {
	laser        *Laser
	conversation wire.ConversationID
}

// Context opens the working context of one conversation. It is free: the
// reads and writes happen at the verbs.
func (laser *Laser) Context(conversation wire.ConversationID) *ContextScope {
	return &ContextScope{laser: laser, conversation: conversation}
}

// Conversation is the conversation this scope covers.
func (scope *ContextScope) Conversation() wire.ConversationID {
	return scope.conversation
}

// Laser is the handle this scope reads and writes through.
func (scope *ContextScope) Laser() *Laser { return scope.laser }

// Append adds a payload to a topic within this conversation.
//
// The provenance is pinned to the conversation, so a later read finds it in
// order with everything else that belongs to the same session.
func (scope *ContextScope) Append(
	ctx context.Context, topic string, payload []byte,
) error {
	_, err := scope.laser.SendAgent(ctx, topic, payload, &Provenance{
		Conversation: scope.conversation,
	})
	return err
}

// Fetch reads this conversation's history from some topics, bounded to the
// last n messages and optionally trimmed to a token budget.
//
// The bound is required rather than optional. An unbounded read of a
// long-lived conversation is a replay nobody asked for, so the full walk has
// its own deliberate spelling in FetchWith.
func (scope *ContextScope) Fetch(
	ctx context.Context, topics []string, n int, tokenBudget int,
) ([]ContextMessage, error) {
	return scope.FetchWith(ctx, topics, boundedPolicy(n, tokenBudget))
}

// FetchWith reads this conversation's history under a policy of the caller's
// own, the deep form behind Fetch.
func (scope *ContextScope) FetchWith(
	ctx context.Context, topics []string, policy ContextPolicy,
) ([]ContextMessage, error) {
	assembler := Assemble(scope.conversation).WithPolicy(policy)
	for _, topic := range topics {
		records, err := scope.read(ctx, topic)
		if err != nil {
			return nil, err
		}
		assembler.Add(topic, records...)
	}
	return assembler.Messages(), nil
}

// read drains one topic. A topic nobody has written to yet is empty rather
// than an error: a reader started before its writer is not a failure.
func (scope *ContextScope) read(
	ctx context.Context, topic string,
) ([]*Record, error) {
	if scope.laser.defaultStream == "" {
		return nil, errNoStream()
	}
	cursor, err := scope.laser.Stream(scope.laser.defaultStream).
		Topic(topic).ReplayAs(contextReaderName)
	if err != nil {
		return nil, err
	}
	return cursor.Poll(ctx)
}

// contextReaderName is how a context read names itself to the server. The
// offsets stay client-owned, so the name only identifies the reader.
const contextReaderName = "laser-context"

// Block renders the last n messages as one newline-joined text block, the
// prompt-ready form.
func (scope *ContextScope) Block(
	ctx context.Context, topics []string, n int, tokenBudget int,
) (string, error) {
	messages, err := scope.Fetch(ctx, topics, n, tokenBudget)
	if err != nil {
		return "", err
	}
	bodies := make([]string, 0, len(messages))
	for _, message := range messages {
		bodies = append(bodies, string(message.Body()))
	}
	return strings.Join(bodies, "\n"), nil
}

// State rebuilds in-memory state by folding this conversation's history.
//
// The fold is the caller's, because what the state means is theirs: this
// knows how to read the log in order and nothing about what it says.
func State[S any](
	ctx context.Context, scope *ContextScope, topics []string,
	n int, tokenBudget int, initial S, fold func(S, ContextMessage) S,
) (S, error) {
	messages, err := scope.Fetch(ctx, topics, n, tokenBudget)
	if err != nil {
		return initial, err
	}
	state := initial
	for _, message := range messages {
		state = fold(state, message)
	}
	return state, nil
}

// Memory is this conversation's memory in a namespace, pre-scoped so
// remembering and recalling never repeat the conversation id.
//
// Durable facts and the knowledge graph stay deliberately
// cross-conversation, because a fact learned in one session is worth
// recalling in the next, so they are reached through the handle rather than
// narrowed here.
func (scope *ContextScope) Memory(namespace string) *Memory {
	conversation := scope.conversation
	return scope.laser.Memory(namespace).For(MemoryScope{
		Stream:       scope.laser.defaultStream,
		Conversation: &conversation,
	})
}

// Graph is a knowledge graph, reached from this scope for the common flow
// where a task streams messages, keeps session memory, and resolves the
// dependencies between them.
//
// It is returned unnarrowed on purpose. A dependency graph is shared across
// conversations, because a service-to-component edge holds whichever task
// asked, and scoping it to one conversation would hide the very
// relationships a caller wants.
func (scope *ContextScope) Graph(name string) *Graph {
	return scope.laser.Graph(name)
}

// ContextCheckpoint is where a conversation's topics stood at one moment:
// the next offset of each partition, per topic.
//
// Offsets are per topic partition rather than per conversation, because a
// conversation is a key within a partition and the log has no per-key
// position to resume from.
type ContextCheckpoint struct {
	// Offsets is the next offset per partition, keyed by topic.
	Offsets map[string][]uint64
}

// Checkpoint reads the current tail of some topics.
//
// Fold up to it to get a reproducible view of a moment, or resume after it to
// read only what arrived since.
func (scope *ContextScope) Checkpoint(
	ctx context.Context, topics []string,
) (*ContextCheckpoint, error) {
	checkpoint := &ContextCheckpoint{Offsets: map[string][]uint64{}}
	for _, topic := range topics {
		if scope.laser.defaultStream == "" {
			return nil, errNoStream()
		}
		cursor, err := scope.laser.Stream(scope.laser.defaultStream).
			Topic(topic).ReplayAs(contextReaderName)
		if err != nil {
			return nil, err
		}
		if _, err := cursor.Poll(ctx); err != nil {
			return nil, err
		}
		checkpoint.Offsets[topic] = cursor.Offsets()
	}
	return checkpoint, nil
}

// FetchSince reads only what arrived on these topics after a checkpoint, so a
// caller that already folded up to it does not fold the same records twice.
func (scope *ContextScope) FetchSince(
	ctx context.Context, checkpoint *ContextCheckpoint,
	topics []string, policy ContextPolicy,
) ([]ContextMessage, error) {
	assembler := Assemble(scope.conversation).WithPolicy(policy)
	for _, topic := range topics {
		if scope.laser.defaultStream == "" {
			return nil, errNoStream()
		}
		cursor, err := scope.laser.Stream(scope.laser.defaultStream).
			Topic(topic).ReplayAs(contextReaderName)
		if err != nil {
			return nil, err
		}
		if checkpoint != nil {
			cursor = cursor.FromOffsets(checkpoint.Offsets[topic])
		}
		records, err := cursor.Poll(ctx)
		if err != nil {
			return nil, err
		}
		assembler.Add(topic, records...)
	}
	return assembler.Messages(), nil
}

// boundedPolicy is the last n messages, trimmed to a token budget when one is
// set.
//
// The two compose rather than replacing each other: a read is bounded by
// turns and by prompt size at once, because either alone leaves the other
// unbounded.
func boundedPolicy(n int, tokenBudget int) ContextPolicy {
	if tokenBudget <= 0 {
		return LastN(n)
	}
	return Chain{LastN(n), TokenBudget{MaxTokens: tokenBudget}}
}
