//go:build integration

package laser_test

import (
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenLiveGroupWhenReadingFilteredPagesThenShouldOwnAndAcknowledgePartition(t *testing.T) {
	client := dialLive(t)
	if !client.Capabilities().Filters.GroupPolicyReads {
		t.Skip("deployment does not serve group-aware reads")
	}
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)
	if _, err := topic.Publish().Payload([]byte("first")).Send(ctx); err != nil {
		t.Fatal(err)
	}
	group := topic.Group("readers")
	if err := group.EnsureNative(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := group.OpenReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	for {
		assigned, err := reader.Partitions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(assigned) > 0 {
			page, err := reader.Read(assigned[0], wire.GroupPolicy()).Count(1).Fetch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Records) != 1 || string(page.Records[0].Payload()) != "first" {
				t.Fatalf("unexpected filtered records: %+v", page.Records)
			}
			if err := reader.Ack(ctx, page); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
