package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionTurn is one record read back from a session: the message off the log and how a timeline shows it.
type SessionTurn struct {
	Display wire.DisplayType
	Message ContextMessage
}

// Text is the record's body as text: the envelope body of a typed record, else the raw payload.
func (turn SessionTurn) Text() string { return string(turn.Message.Body()) }

// Checkpoint is where the session lane and the declared agent topics end right now.
func (session *Session) Checkpoint(ctx context.Context) (*ContextCheckpoint, error) {
	return session.Scope().Checkpoint(ctx, session.turnTopics())
}

// TurnsAt reads the records up to checkpoint.
func (session *Session) TurnsAt(ctx context.Context, checkpoint *ContextCheckpoint) ([]SessionTurn, error) {
	if checkpoint == nil {
		return nil, &InvalidError{Reason: "session turns up to a checkpoint must name one"}
	}
	messages, err := session.Scope().FetchUntil(ctx, checkpoint, session.turnTopics(), everyMessage())
	if err != nil {
		return nil, err
	}
	return turnsOf(messages), nil
}

// TurnsSince reads the records appended after checkpoint.
func (session *Session) TurnsSince(ctx context.Context, checkpoint *ContextCheckpoint) ([]SessionTurn, error) {
	if checkpoint == nil {
		return nil, &InvalidError{Reason: "session turns after a checkpoint must name one"}
	}
	messages, err := session.Scope().FetchSince(ctx, checkpoint, session.turnTopics(), everyMessage())
	if err != nil {
		return nil, err
	}
	return turnsOf(messages), nil
}

// SessionStateAt folds the records up to checkpoint into state as it stood then.
func SessionStateAt[S any](
	ctx context.Context, session *Session, checkpoint *ContextCheckpoint,
	initial S, fold func(state S, turn SessionTurn) S,
) (S, error) {
	turns, err := session.TurnsAt(ctx, checkpoint)
	return foldTurns(turns, err, initial, fold)
}

// SessionReplay folds the records appended after checkpoint into state saved at that checkpoint.
func SessionReplay[S any](
	ctx context.Context, session *Session, checkpoint *ContextCheckpoint,
	initial S, fold func(state S, turn SessionTurn) S,
) (S, error) {
	turns, err := session.TurnsSince(ctx, checkpoint)
	return foldTurns(turns, err, initial, fold)
}

func foldTurns[S any](
	turns []SessionTurn, err error, initial S, fold func(state S, turn SessionTurn) S,
) (S, error) {
	if err != nil {
		return initial, err
	}
	state := initial
	for _, turn := range turns {
		state = fold(state, turn)
	}
	return state, nil
}

// turnTopics are the topics this session's records ride.
func (session *Session) turnTopics() []string {
	return laneTopics(session.laser, session.config.Stream)
}

func turnsOf(messages []ContextMessage) []SessionTurn {
	turns := make([]SessionTurn, len(messages))
	for index, message := range messages {
		display := wire.DisplayAgentMessage
		if message.Envelope != nil {
			display = wire.DisplayTypeOf(
				wire.SessionRecord{Kind: wire.RecordEnvelope, Envelope: message.Envelope}, wire.AgentSessions)
		}
		turns[index] = SessionTurn{Display: display, Message: message}
	}
	return turns
}
