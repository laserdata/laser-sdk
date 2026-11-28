package laser

import (
	"context"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestGivenAStoredOffsetWhenAnEarlierRecordIsCommittedThenShouldKeepItUnlessReplayIsAllowed(t *testing.T) {
	for _, replay := range []bool{false, true} {
		inner := &countingReader{}
		consumer := &Consumer{inner: inner, allowReplay: replay}
		consumer.offsets.store(0, 40)

		if err := consumer.Commit(context.Background(), iggcon.Checkpoint{}); err != nil {
			t.Fatalf("replay %v: commit: %v", replay, err)
		}

		want := 0
		if replay {
			want = 1
		}
		if inner.commits != want {
			t.Errorf("replay %v: want %d commits sent, got %d", replay, want, inner.commits)
		}
	}
}

func TestGivenNoStoredOffsetWhenARecordIsCommittedThenShouldSendIt(t *testing.T) {
	inner := &countingReader{}
	consumer := &Consumer{inner: inner}

	if err := consumer.Commit(context.Background(), iggcon.Checkpoint{}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if inner.commits != 1 {
		t.Errorf("want the first commit sent, got %d", inner.commits)
	}
}

// countingReader is a reader that counts the commits it is handed.
type countingReader struct {
	consumerReader
	commits int
}

func (reader *countingReader) Commit(context.Context, iggcon.Checkpoint) error {
	reader.commits++
	return nil
}
