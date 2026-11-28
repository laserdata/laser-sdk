//go:build integration

package laser_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenLiveConsolidationWhenPrunedThenShouldReadOnlyTheDurableSummary(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	scratch := liveScratch(t, client, ctx, "consolidation")
	session := conversation(1)
	handle := scratch.Laser().Context(session).Memory("notes")
	for _, text := range []string{"first", "second"} {
		if _, err := handle.Remember(ctx, laser.MemoryMessage, []byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	report, err := handle.ConsolidateWith(ctx, 10, &joiningSummarizer{}, true)
	if err != nil || report.Summarized != 2 || report.Pruned != 2 {
		t.Fatalf("consolidation returned %+v, %v", report, err)
	}
	other := scratch.Laser().Context(session).Memory("notes")
	items, err := other.Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(items) != 1 || items[0].Kind != laser.MemorySummary || items[0].Text() != "second first" {
		t.Fatalf("recall after consolidation returned %+v, %v", items, err)
	}
}
