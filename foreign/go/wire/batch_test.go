package wire_test

import (
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACommandErrorWhenDecodedAsABatchReplyThenShouldBeRefused(t *testing.T) {
	refusal := mustMarshal(t, wire.CommandError{Code: wire.ResultUnavailable, Message: "busy"})

	var reply wire.BatchReply
	if err := wire.Unmarshal(refusal, &reply); err == nil {
		t.Fatalf("a frame-level refusal decoded as a batch of %d results", len(reply.Results))
	}
}

func TestGivenAnEmptyBatchReplyWhenDecodedThenShouldKeepNoResults(t *testing.T) {
	var reply wire.BatchReply
	if err := wire.Unmarshal(mustMarshal(t, wire.BatchReply{Results: [][]byte{}}), &reply); err != nil {
		t.Fatalf("an empty batch reply must decode: %v", err)
	}
	if len(reply.Results) != 0 {
		t.Fatalf("results = %d, want none", len(reply.Results))
	}
}

func TestGivenABatchWhenValidatedThenShouldCapEachItemAndTheWholeFrame(t *testing.T) {
	body := make([]byte, wire.MaxValueBytes+1)
	item := func(size int) wire.BatchItem {
		return wire.BatchItem{Code: wire.AgdxKvGetCode, Payload: body[:size]}
	}
	atCap := wire.BatchRequest{V: wire.BatchOpVersion, Ops: []wire.BatchItem{item(wire.MaxValueBytes)}}
	if err := atCap.Validate(); err != nil {
		t.Fatalf("an item at the value cap must pass: %v", err)
	}

	oversized := wire.BatchRequest{V: wire.BatchOpVersion, Ops: []wire.BatchItem{item(wire.MaxValueBytes + 1)}}
	if err := oversized.Validate(); err == nil || !strings.Contains(err.Error(), "batch op payload") {
		t.Fatalf("an item past the value cap must be refused, got %v", err)
	}

	whole := wire.BatchRequest{V: wire.BatchOpVersion}
	for range wire.MaxFrameBytes/wire.MaxValueBytes + 1 {
		whole.Ops = append(whole.Ops, item(wire.MaxValueBytes))
	}
	if err := whole.Validate(); err == nil || !strings.Contains(err.Error(), "batch total payload") {
		t.Fatalf("a batch past the frame cap must be refused, got %v", err)
	}
}
