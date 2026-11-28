//go:build integration

// Live coverage for the scratch stream.
//
// A scratch stream is only meaningful against a server: its whole purpose is
// that the name is free on a real deployment and that deleting it takes the
// topics and records with it. A hermetic test can check the name's shape and
// nothing else that matters.
package laser_test

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// TestGivenScratchStreamWhenCreatedThenShouldOwnAUniqueStreamForOneRun covers
// the lifecycle: a free name, a usable default stream, and a delete that
// removes the stream with everything in it.
func TestGivenScratchStreamWhenCreatedThenShouldOwnAUniqueStreamForOneRun(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)

	const prefix = "laser-live-scratch"
	scratch, err := client.ScratchStream(ctx, prefix)
	if err != nil {
		t.Fatalf("create a scratch stream: %v", err)
	}
	// Deferred rather than only at the end, because a failure below must not
	// leave the stream behind on a long lived container.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := scratch.Delete(cleanup); err != nil {
			t.Errorf("delete the scratch stream: %v", err)
		}
	}()

	// The name is prefix, a hyphen, then eight lowercase hex characters. The
	// shape is checked rather than assumed because every client appends the
	// same suffix, and a client that drifted would collide with another run.
	suffix, found := strings.CutPrefix(scratch.Name(), prefix+"-")
	if !found {
		t.Fatalf("a scratch stream must be named after its prefix, got %q", scratch.Name())
	}
	if len(suffix) != 8 {
		t.Errorf("a scratch suffix is eight hex characters, got %q", suffix)
	}
	if strings.ToLower(suffix) != suffix {
		t.Errorf("a scratch suffix is lowercase, got %q", suffix)
	}
	if _, err := hex.DecodeString(suffix); err != nil {
		t.Errorf("a scratch suffix is hex, got %q: %v", suffix, err)
	}

	// The handle it hands back addresses the scratch stream as its default, so
	// a one-word topic resolves there without naming the stream again.
	derived := scratch.Laser()
	if derived.DefaultStream() != scratch.Name() {
		t.Fatalf("the scratch handle must default to its own stream, got %q",
			derived.DefaultStream())
	}

	topic := derived.Topic("records")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create a topic on the scratch stream: %v", err)
	}
	if topic.StreamName() != scratch.Name() {
		t.Errorf("the topic resolved onto %q rather than the scratch stream",
			topic.StreamName())
	}

	streams, err := client.Streams(ctx)
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if !containsStream(streams, scratch.Name()) {
		t.Fatalf("the scratch stream %s is missing from the server's listing",
			scratch.Name())
	}

	// A record proves the stream is a real one and not just a name, and gives
	// the delete below something to take with it.
	if _, err := topic.Publish().Payload([]byte("scratch")).Key("one").Send(ctx); err != nil {
		t.Fatalf("publish to the scratch stream: %v", err)
	}

	if err := scratch.Delete(ctx); err != nil {
		t.Fatalf("delete the scratch stream: %v", err)
	}
	exists, err := client.Stream(scratch.Name()).Exists(ctx)
	if err != nil {
		t.Fatalf("read whether the deleted scratch stream exists: %v", err)
	}
	if exists {
		t.Fatal("the deleted scratch stream is still there")
	}

	// Delete is idempotent, which is what lets a deferred call sit behind an
	// explicit one without turning a clean run into a failure.
	if err := scratch.Delete(ctx); err != nil {
		t.Errorf("deleting an already deleted scratch stream must be a no-op: %v", err)
	}
}

// TestGivenTwoScratchStreamsWhenCreatedThenShouldNotCollide proves the
// isolation the surface exists for: two runs side by side get their own
// streams rather than sharing one and reading each other's records.
func TestGivenTwoScratchStreamsWhenCreatedThenShouldNotCollide(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)

	const prefix = "laser-live-scratch-pair"
	first, err := client.ScratchStream(ctx, prefix)
	if err != nil {
		t.Fatalf("create the first scratch stream: %v", err)
	}
	t.Cleanup(func() { closeScratch(t, first) })

	second, err := client.ScratchStream(ctx, prefix)
	if err != nil {
		t.Fatalf("create the second scratch stream: %v", err)
	}
	t.Cleanup(func() { closeScratch(t, second) })

	if first.Name() == second.Name() {
		t.Fatalf("two scratch streams took the same name %q, so one run would "+
			"read the other's records", first.Name())
	}

	// Each one publishes to a topic of the same name, and each reads only its
	// own record. Same name, different stream, no crosstalk.
	for _, scratch := range []scratchStream{{first, "first"}, {second, "second"}} {
		topic := scratch.handle.Laser().Topic("records")
		if err := topic.Ensure(ctx, 1, nil); err != nil {
			t.Fatalf("create a topic on %s: %v", scratch.handle.Name(), err)
		}
		if _, err := topic.Publish().
			Payload([]byte(scratch.body)).Key("one").Send(ctx); err != nil {
			t.Fatalf("publish to %s: %v", scratch.handle.Name(), err)
		}
	}

	for _, scratch := range []scratchStream{{first, "first"}, {second, "second"}} {
		topic := scratch.handle.Laser().Topic("records")
		consumer := liveConsumer(t, topic, ctx, livePartitions(t, topic, ctx))
		record, err := consumer.Next(ctx)
		if err != nil {
			t.Fatalf("read back from %s: %v", scratch.handle.Name(), err)
		}
		if got := string(record.Payload()); got != scratch.body {
			t.Errorf("%s returned %q, so the two scratch streams are not isolated",
				scratch.handle.Name(), got)
		}
	}
}

// scratchStream pairs a scratch stream with the body published to it.
type scratchStream struct {
	handle *laser.ScratchStream
	body   string
}

// closeScratch removes a scratch stream at the end of a test.
func closeScratch(t *testing.T, scratch *laser.ScratchStream) {
	t.Helper()
	cleanup, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	defer cancel()
	if err := scratch.Delete(cleanup); err != nil {
		t.Errorf("delete the scratch stream %s: %v", scratch.Name(), err)
	}
}
