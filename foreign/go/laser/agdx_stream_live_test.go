//go:build integration

package laser_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveChunkStreamWhenReassembledFromTheLogThenShouldReplayInOrder(t *testing.T) {
	ctx := liveContext(t)
	client := liveScratch(t, dialLive(t), ctx, "laser-live-chunks").Laser()
	conversation := laser.NewConversationID()
	model, err := wire.NewAgentID("model")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	stream := client.Agdx(model, conversation).Stream(correlationID(17), wire.OperationChat)

	for _, body := range []string{"Hel", "lo"} {
		if err := stream.Write(ctx, []byte(body)); err != nil {
			t.Fatalf("write %q: %v", body, err)
		}
	}
	if err := stream.Finish(ctx, "stop", nil); err != nil {
		t.Fatalf("finish: %v", err)
	}

	stop := "stop"
	want := []laser.StreamEvent{
		{Kind: laser.StreamBody, Sequence: 0, Payload: []byte("Hel")},
		{Kind: laser.StreamBody, Sequence: 1, Payload: []byte("lo")},
		{Kind: laser.StreamFinished, FinishReason: &stop},
	}
	var events []laser.StreamEvent
	for len(events) < len(want) {
		if ctx.Err() != nil {
			t.Fatalf("the stream never reassembled whole, got %+v", events)
		}
		events, err = client.ReassembleChannel(ctx, conversation, wire.AgentSessions, stream.Channel())
		if err != nil {
			t.Fatalf("reassemble: %v", err)
		}
	}
	if !reflect.DeepEqual(events, want) {
		t.Errorf("want %+v, got %+v", want, events)
	}
}

func TestGivenAnUnfinishedLiveStreamLongerThanFiftyChunksWhenReassembledPastItsBoundThenShouldEndAbandoned(t *testing.T) {
	const chunks = 60
	ctx := liveContext(t)
	client := liveScratch(t, dialLive(t), ctx, "laser-live-chunks").Laser()
	conversation := laser.NewConversationID()
	model, err := wire.NewAgentID("model")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	stream := client.Agdx(model, conversation).Stream(correlationID(18), wire.OperationChat).
		WithDeadline(time.Now()).Buffered(chunks, time.Minute)

	for index := range chunks {
		if err := stream.Write(ctx, []byte{byte(index)}); err != nil {
			t.Fatalf("buffer chunk %d: %v", index, err)
		}
	}
	if err := stream.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var events []laser.StreamEvent
	for len(events) < chunks+1 {
		if ctx.Err() != nil {
			t.Fatalf("the stream never reassembled whole, got %d events", len(events))
		}
		events, err = client.ReassembleChannel(ctx, conversation, wire.AgentSessions, stream.Channel())
		if err != nil {
			t.Fatalf("reassemble: %v", err)
		}
	}
	for index, event := range events[:chunks] {
		if event.Kind != laser.StreamBody || event.Sequence != uint64(index) {
			t.Fatalf("event %d is %+v", index, event)
		}
	}
	abandoned := laser.FinishReasonAbandoned
	last := laser.StreamEvent{Kind: laser.StreamFinished, FinishReason: &abandoned, Synthetic: true}
	if got := events[chunks]; !reflect.DeepEqual(got, last) {
		t.Errorf("want a synthetic abandonment, got %+v", got)
	}
}
