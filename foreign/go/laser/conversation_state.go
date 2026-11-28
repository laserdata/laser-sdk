package laser

import (
	"context"
	"encoding/json"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ReplayBoundKind is how much of a conversation's history a replay reads.
type ReplayBoundKind int

const (
	// ReplayFull folds the whole history.
	ReplayFull ReplayBoundKind = iota
	// ReplayLast folds the newest n messages.
	ReplayLast
	// ReplayFrom folds only what was appended after a checkpoint.
	ReplayFrom
	// ReplayUpTo folds the history up to a checkpoint and stops there.
	ReplayUpTo
)

// ReplayBound bounds a conversation replay.
//
// The bound is explicit rather than inferred, because the three readings of
// "the history" are genuinely different jobs: rebuilding everything,
// catching up on what is new, and reproducing a moment exactly.
type ReplayBound struct {
	Kind ReplayBoundKind
	// N is how many messages ReplayLast keeps.
	N int
	// Checkpoint is the point ReplayFrom resumes after, or ReplayUpTo stops
	// at.
	Checkpoint *ContextCheckpoint
}

// FullReplay folds a conversation's whole history.
func FullReplay() ReplayBound { return ReplayBound{Kind: ReplayFull} }

// LastReplay folds the newest n messages.
func LastReplay(n int) ReplayBound { return ReplayBound{Kind: ReplayLast, N: n} }

// ReplayAfter folds only what arrived after a checkpoint.
func ReplayAfter(checkpoint *ContextCheckpoint) ReplayBound {
	return ReplayBound{Kind: ReplayFrom, Checkpoint: checkpoint}
}

// ReplayUntil folds the history up to a checkpoint and stops there.
func ReplayUntil(checkpoint *ContextCheckpoint) ReplayBound {
	return ReplayBound{Kind: ReplayUpTo, Checkpoint: checkpoint}
}

// FoldFunc folds one message into the state being rebuilt.
type FoldFunc[S any] func(state S, message ContextMessage) S

// LoadConversationState rebuilds in-memory state by folding a conversation's
// logged messages, which is event sourcing over the log.
//
// A free function rather than a method, because Go has no generic methods and
// the state type belongs to the caller. Rust spells this
// `ConversationState::load`.
//
// The fold is the caller's, so what the state means stays theirs: this reads
// the history under the bound, in order, and hands each message over.
func LoadConversationState[S any](
	ctx context.Context, laser *Laser, conversation wire.ConversationID,
	topics []string, bound ReplayBound, initial S, fold FoldFunc[S],
) (S, error) {
	messages, err := replayMessages(ctx, laser, conversation, topics, bound)
	if err != nil {
		return initial, err
	}
	state := initial
	for _, message := range messages {
		state = fold(state, message)
	}
	return state, nil
}

// LoadConversationStateWith replays through a snapshot store. The newest snapshot's
// state, decoded as JSON, seeds the fold and only the records past it are read.
// A conversation with no snapshot folds fully from initial.
func LoadConversationStateWith[S any](
	ctx context.Context, laser *Laser, store SnapshotStore, conversation wire.ConversationID,
	topics []string, initial S, fold FoldFunc[S],
) (S, error) {
	if laser == nil || store == nil {
		return initial, &InvalidError{Reason: "replaying through a snapshot needs a client and a store"}
	}
	snapshot, err := store.Latest(ctx, conversation)
	if err != nil {
		return initial, err
	}
	if snapshot == nil {
		return LoadConversationState(ctx, laser, conversation, topics, FullReplay(), initial, fold)
	}
	if snapshot.Conversation != conversation {
		return initial, &InvalidError{Reason: "snapshot conversation does not match the fold"}
	}
	checkpoint, err := CheckpointFromSnapshot(ctx, laser, *snapshot, topics)
	if err != nil {
		return initial, err
	}
	var seed S
	if err := json.Unmarshal(snapshot.State, &seed); err != nil {
		return initial, &CodecError{Reason: "decode snapshot state", Cause: err}
	}
	return LoadConversationState(ctx, laser, conversation, topics, ReplayAfter(checkpoint), seed, fold)
}

// replayMessages reads a conversation's history under the bound.
func replayMessages(
	ctx context.Context, laser *Laser, conversation wire.ConversationID,
	topics []string, bound ReplayBound,
) ([]ContextMessage, error) {
	if laser == nil {
		return nil, &InvalidError{Reason: "replaying a conversation needs a client"}
	}
	scope := laser.Context(conversation)
	switch bound.Kind {
	case ReplayLast:
		return scope.FetchWith(ctx, topics, LastN(bound.N))
	case ReplayFrom:
		if bound.Checkpoint == nil {
			return nil, &InvalidError{
				Reason: "a replay resuming after a checkpoint must name one"}
		}
		return scope.FetchSince(ctx, bound.Checkpoint, topics, everyMessage())
	case ReplayUpTo:
		if bound.Checkpoint == nil {
			return nil, &InvalidError{
				Reason: "a replay stopping at a checkpoint must name one"}
		}
		return scope.FetchUntil(ctx, bound.Checkpoint, topics, everyMessage())
	default:
		return scope.FetchWith(ctx, topics, everyMessage())
	}
}

// everyMessage is the policy that keeps the whole history, for the bounds
// that are bounded by position rather than by count.
func everyMessage() ContextPolicy { return LastN(maxReplayMessages) }

// maxReplayMessages is the ceiling a full replay reads. It is large enough
// not to bound a real conversation and finite so a fold cannot be handed an
// unbounded slice by a topic nobody pruned.
const maxReplayMessages = 1 << 30
