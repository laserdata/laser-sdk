//go:build integration

package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenALiveMemoryWhenANamedItemIsSetUpdatedAndRemovedThenShouldReadEachStateBack(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	scratch := liveScratch(t, client, ctx, "named-memory").Laser()
	memory := scratch.Memory("notes")

	if err := memory.Set(ctx, "plan", []byte(`{"owner":"a","step":1}`)); err != nil {
		t.Fatalf("set: %v", err)
	}
	awaitNamed(t, ctx, "the set value", `{"owner":"a","step":1}`, func() ([]byte, bool, error) { return memory.Fetch(ctx, "plan") })

	if err := memory.Update(ctx, "plan", []byte(`{"owner":null,"step":2}`)); err != nil {
		t.Fatalf("update: %v", err)
	}
	awaitNamed(t, ctx, "the merged value", `{"step":2}`, func() ([]byte, bool, error) { return memory.Fetch(ctx, "plan") })
	// A fresh handle holds no fold, so this read comes off the topic.
	fresh := scratch.Memory("notes")
	awaitNamed(t, ctx, "the folded value", `{"step":2}`, func() ([]byte, bool, error) { return fresh.FetchFolded(ctx, "plan") })
	if _, found, err := scratch.Memory("other").FetchFolded(ctx, "plan"); err != nil || found {
		t.Fatalf("another namespace read the key: found=%v err=%v", found, err)
	}

	if err := memory.Remove(ctx, "plan"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	awaitNamed(t, ctx, "the removal", "", func() ([]byte, bool, error) { return memory.Fetch(ctx, "plan") })
	awaitNamed(t, ctx, "the folded removal", "", func() ([]byte, bool, error) { return fresh.FetchFolded(ctx, "plan") })
	t.Logf("named memory served by the managed view: %v", client.Capabilities().Kv.Available)
}

func TestGivenALiveMemoryTopicWhenBuiltThenShouldHoldItsOwnMemory(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	scratch := liveScratch(t, client, ctx, "memory-topic").Laser()

	memory, err := scratch.MemoryTopic("team.memory").Partitions(2).TTL(time.Hour).Build(ctx)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := memory.Remember(ctx, laser.MemoryFact, []byte("the team ships on friday")); err != nil {
		t.Fatalf("remember: %v", err)
	}

	details, err := scratch.Topic("team.memory").Get(ctx)
	if err != nil || details == nil || details.PartitionsCount != 2 || details.MessageExpiry != time.Hour {
		t.Fatalf("want two partitions and an hour of retention, got %+v err=%v", details, err)
	}
	reopened, err := scratch.MemoryOnTopic("team.memory", "")
	if err != nil {
		t.Fatal(err)
	}
	items, err := reopened.RecallFolded(ctx, laser.MemoryQuery{})
	if err != nil || len(items) != 1 || items[0].Text() != "the team ships on friday" {
		t.Fatalf("items=%v err=%v", texts(items), err)
	}
	if elsewhere, err := scratch.Memory("team.memory").RecallFolded(ctx, laser.MemoryQuery{}); err != nil || len(elsewhere) != 0 {
		t.Fatalf("the default memory topic must not hold it, got %v err=%v", texts(elsewhere), err)
	}
}

// awaitNamed polls read until it returns want, an empty want meaning absent.
func awaitNamed(t *testing.T, ctx context.Context, what, want string, read func() ([]byte, bool, error)) {
	t.Helper()
	for {
		value, found, err := read()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if found == (want != "") && string(value) == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s never showed: last read %q found=%v", what, value, found)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
