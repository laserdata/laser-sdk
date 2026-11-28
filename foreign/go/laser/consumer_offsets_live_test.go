//go:build integration

package laser_test

import (
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenALiveConsumerWhenReadingAndStoringThenShouldKeepItsLocalOffsetsAndRefuseToMoveBackWithoutReplay(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)
	for _, body := range []string{"first", "second", "third"} {
		if _, err := topic.Publish().Payload([]byte(body)).Send(ctx); err != nil {
			t.Fatal(err)
		}
	}
	partition := uint32(0)
	start := iggcon.FirstPollingStrategy()
	options := laser.ConsumerOptions{
		Partitions: []uint32{partition}, ManualCommit: true, Start: &start,
		PollRetryInterval: 100 * time.Millisecond, InitRetries: 2, InitRetryInterval: 10 * time.Millisecond,
	}
	consumer, err := topic.Consumer(ctx, liveName("offsets"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(ctx)

	if _, err := consumer.Next(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if consumed, known := consumer.LastConsumedOffset(partition); !known || consumed != 1 {
		t.Fatalf("want the last handed over offset 1, got %d (%v)", consumed, known)
	}
	if err := consumer.Commit(ctx, second.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if stored, known := consumer.LastStoredOffset(partition); !known || stored != 1 {
		t.Fatalf("want the committed offset 1 remembered, got %d (%v)", stored, known)
	}

	if err := consumer.StoreOffset(ctx, 0, &partition); err != nil {
		t.Fatal(err)
	}
	held, err := consumer.StoredOffset(ctx, partition)
	if err != nil || held == nil || held.Offset != 1 {
		t.Fatalf("an earlier offset must not be stored without replay, the server holds %+v (%v)", held, err)
	}

	options.AllowReplay = true
	replaying, err := topic.Consumer(ctx, liveName("replay"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer replaying.Close(ctx)
	for _, offset := range []uint64{2, 0} {
		if err := replaying.StoreOffset(ctx, offset, &partition); err != nil {
			t.Fatal(err)
		}
	}
	held, err = replaying.StoredOffset(ctx, partition)
	if err != nil || held == nil || held.Offset != 0 {
		t.Fatalf("replay moves the stored offset back, the server holds %+v (%v)", held, err)
	}
}
