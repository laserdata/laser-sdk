package bridges_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func streamChatChunks(t *testing.T, laserClient *laser.Laser, chunks []string) wire.ConversationID {
	t.Helper()
	ctx := context.Background()
	conversation := laser.NewConversationID()
	source := agent(t, "assistant")
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	channel := wire.ChannelID{ID: wire.IDFromBytes(laser.NewOperationID())}
	agdx := laserClient.Agdx(source, conversation)
	for sequence, body := range chunks {
		send := agdx.Chunk(correlation, channel, uint64(sequence), []byte(body))
		if sequence == 0 {
			// The stream purpose rides only the opening chunk.
			send = send.WithOperation(wire.OperationChat)
		}
		if _, err := send.Send(ctx); err != nil {
			t.Fatalf("send chunk %d: %v", sequence, err)
		}
	}
	finisher := agdx.Chunk(correlation, channel, uint64(len(chunks)), nil).
		Last().WithFinishReason("stop")
	if _, err := finisher.Send(ctx); err != nil {
		t.Fatalf("finish the stream: %v", err)
	}
	return conversation
}

func aguiHandle(t *testing.T) *laser.Laser {
	t.Helper()
	return laser.FromClient(&bridgeLog{}, laser.Options{DefaultStream: "agents"})
}

func eventKinds(events []bridges.AgUiEvent) []bridges.AgUiEventKind {
	kinds := make([]bridges.AgUiEventKind, len(events))
	for index, event := range events {
		kinds[index] = event.Type
	}
	return kinds
}

func TestGivenAChatChunkStreamWhenReadBackThenShouldRenderTheFullLifecycle(t *testing.T) {
	handle := aguiHandle(t)
	conversation := streamChatChunks(t, handle, []string{"hello ", "world"})

	events, err := bridges.AgUiEvents(context.Background(), handle, conversation, wire.AgentSessions)
	if err != nil {
		t.Fatalf("agui events: %v", err)
	}

	want := []bridges.AgUiEventKind{
		bridges.RunStarted, bridges.TextMessageStart, bridges.TextMessageContent,
		bridges.TextMessageContent, bridges.TextMessageEnd, bridges.RunFinished,
	}
	if got := eventKinds(events); !equalKinds(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestGivenALongChatChunkStreamWhenReadBackThenShouldOpenFromItsFirstChunk(t *testing.T) {
	handle := aguiHandle(t)
	chunks := make([]string, 60)
	for index := range chunks {
		chunks[index] = "chunk "
	}
	conversation := streamChatChunks(t, handle, chunks)

	events, err := bridges.AgUiEvents(context.Background(), handle, conversation, wire.AgentSessions)
	if err != nil {
		t.Fatalf("agui events: %v", err)
	}

	kinds := eventKinds(events)
	if len(kinds) < 2 || kinds[0] != bridges.RunStarted || kinds[1] != bridges.TextMessageStart {
		t.Fatalf("want the run opened from its first chunk, got %v", kinds)
	}
	contents := 0
	for _, kind := range kinds {
		if kind == bridges.TextMessageContent {
			contents++
		}
	}
	if contents != len(chunks) {
		t.Errorf("want %d text contents, got %d", len(chunks), contents)
	}
	if n := len(kinds); n < 2 || kinds[n-2] != bridges.TextMessageEnd || kinds[n-1] != bridges.RunFinished {
		t.Errorf("want the run closed at the end, got %v", kinds)
	}
}

// TestGivenARunThatNeverOpenedWhenClosedThenShouldEmitNothing proves a read
// with no AGDX records on it renders no run at all, rather than an empty one
// nobody asked for.
func TestGivenARunThatNeverOpenedWhenClosedThenShouldEmitNothing(t *testing.T) {
	handle := aguiHandle(t)
	events, err := bridges.AgUiEvents(
		context.Background(), handle, laser.NewConversationID(), wire.AgentSessions)
	if err != nil {
		t.Fatalf("agui events: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("want no events, got %+v", events)
	}
}

func equalKinds(got, want []bridges.AgUiEventKind) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
