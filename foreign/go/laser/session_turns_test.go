package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACheckpointWhenTurnsAreReadThenShouldSplitTheLaneAtIt(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 21})}
	server.appendRecord(t, conversation, "planner", "first")
	server.appendRecord(t, conversation, "planner", "second")
	session := scopeHandle(server).Sessions().Open(conversation, "")
	ctx := context.Background()

	checkpoint, err := session.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	server.appendRecord(t, conversation, "planner", "third")

	before, err := session.TurnsAt(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns at: %v", err)
	}
	after, err := session.TurnsSince(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns since: %v", err)
	}
	if got := turnTexts(before); got != "first,second" {
		t.Errorf("want the records up to the checkpoint, got %q", got)
	}
	if got := turnTexts(after); got != "third" {
		t.Errorf("want the records after the checkpoint, got %q", got)
	}
	if after[0].Display != wire.DisplayAgentMessage {
		t.Errorf("want an event shown as an agent message, got %q", after[0].Display)
	}
}

func TestGivenACheckpointWhenStateIsFoldedThenShouldReproduceThatMomentAndCatchUp(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 22})}
	server.appendRecord(t, conversation, "planner", "one")
	session := scopeHandle(server).Sessions().Open(conversation, "")
	ctx := context.Background()
	checkpoint, err := session.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	server.appendRecord(t, conversation, "planner", "two")
	server.appendRecord(t, conversation, "planner", "three")
	count := func(total int, _ laser.SessionTurn) int { return total + 1 }

	then, err := laser.SessionStateAt(ctx, session, checkpoint, 0, count)
	if err != nil {
		t.Fatalf("state at: %v", err)
	}
	now, err := laser.SessionReplay(ctx, session, checkpoint, then, count)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if then != 1 || now != 3 {
		t.Errorf("want 1 record at the checkpoint and 3 after the replay, got %d and %d", then, now)
	}
}

func TestGivenALaneRecordWhenReadAsATurnThenShouldNameTheStreamAndTopicItCameFrom(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 24})}
	server.appendRecord(t, conversation, "planner", "first")
	session := scopeHandle(server).Sessions().Open(conversation, "")
	ctx := context.Background()
	checkpoint, err := session.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	server.appendRecord(t, conversation, "planner", "second")

	before, err := session.TurnsAt(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns at: %v", err)
	}
	after, err := session.TurnsSince(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns since: %v", err)
	}

	stream, err := server.GetStream(ctx, iggcon.Identifier{})
	if err != nil || stream.Id == 0 {
		t.Fatalf("the fake must number its stream: %v", err)
	}
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("want one turn on each side of the checkpoint, got %d and %d", len(before), len(after))
	}
	for _, turn := range append(before, after...) {
		if turn.Message.StreamID != stream.Id || turn.Message.TopicID != feedTopicID {
			t.Fatalf("want stream %d topic %d on %q, got %d and %d",
				stream.Id, feedTopicID, turn.Text(), turn.Message.StreamID, turn.Message.TopicID)
		}
	}
}

func TestGivenAnotherSessionsRecordsWhenTurnsAreReadThenShouldLeaveThemOut(t *testing.T) {
	server := newScope()
	mine := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 23})}
	other := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 24})}
	server.appendRecord(t, mine, "planner", "mine")
	server.appendRecord(t, other, "planner", "theirs")
	session := scopeHandle(server).Sessions().Open(mine, "")
	ctx := context.Background()
	checkpoint, err := session.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	turns, err := session.TurnsAt(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns at: %v", err)
	}

	if got := turnTexts(turns); got != "mine" {
		t.Errorf("want only this session's records, got %q", got)
	}
}

func TestGivenNoCheckpointWhenTurnsAreReadThenShouldRefuse(t *testing.T) {
	session := scopeHandle(newScope()).Sessions().Start()

	_, atErr := session.TurnsAt(context.Background(), nil)
	_, sinceErr := session.TurnsSince(context.Background(), nil)

	var invalid *laser.InvalidError
	if !errors.As(atErr, &invalid) || !errors.As(sinceErr, &invalid) {
		t.Errorf("want both reads refused without a checkpoint, got %v and %v", atErr, sinceErr)
	}
}

func TestGivenARecordWithoutAnEnvelopeWhenShownThenShouldReadItsRawPayload(t *testing.T) {
	message, err := iggcon.NewIggyMessage([]byte("plain"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	turn := laser.SessionTurn{
		Display: wire.DisplayAgentMessage,
		Message: laser.ContextMessage{Record: &laser.Record{Message: message}},
	}

	if turn.Text() != "plain" {
		t.Errorf("want the raw payload, got %q", turn.Text())
	}
}

func TestGivenAConfiguredTurnBoundWhenContextIsReadThenShouldKeepTheNewestTurnsOfTheLane(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 23})}
	for _, body := range []string{"a", "b", "c"} {
		server.appendRecord(t, conversation, "planner", body)
	}
	session := scopeHandle(server).SessionsWith(laser.SessionConfig{ContextTurns: 2}).Open(conversation, "")

	turns, err := session.Context(context.Background())
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	if got := turnTexts(turns); got != "b,c" {
		t.Errorf("want the newest two turns of the session's lane, got %q", got)
	}
	if got := session.Config().ContextTurns; got != 2 {
		t.Errorf("want the configured bound reported, got %d", got)
	}
	defaults := scopeHandle(server).Sessions().Config()
	if defaults.ContextTurns != laser.DefaultSessionContextTurns {
		t.Errorf("want the default turn bound, got %d", defaults.ContextTurns)
	}
}

func TestGivenAnExplicitPolicyWhenContextIsReadThenShouldApplyItToTheLane(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 24})}
	for _, body := range []string{"a", "b", "c"} {
		server.appendRecord(t, conversation, "planner", body)
	}
	session := scopeHandle(server).Sessions().Open(conversation, "")

	turns, err := session.ContextWith(context.Background(), laser.LastN(1))
	if err != nil {
		t.Fatalf("context with: %v", err)
	}

	if got := turnTexts(turns); got != "c" {
		t.Errorf("want the policy applied to the lane, got %q", got)
	}
}

func turnTexts(turns []laser.SessionTurn) string {
	texts := make([]string, len(turns))
	for index, turn := range turns {
		texts[index] = turn.Text()
	}
	return strings.Join(texts, ",")
}
