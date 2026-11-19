package wire_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The streaming server routes a message key to a partition by this exact
// hash, so a client that computed a different number would write a key to one
// partition and look for it on another. These vectors come from the Rust
// client computing the same hash, so a drift here shows up as a failing test
// rather than as data that silently vanished.
func TestGivenTheSharedHashVectorsWhenComputedThenShouldMatchTheRustClient(t *testing.T) {
	for input, want := range map[string]uint32{
		"":                 46947589,
		"a":                1426945110,
		"abc":              852579327,
		"hello world":      3468387874,
		"0123456789abcdef": 3267648361,
		"0123456789abcdef0123456789abcdef0123456789": 946835922,
	} {
		if got := wire.XXHash32([]byte(input), 0); got != want {
			t.Errorf("%q: want %d, got %d", input, want, got)
		}
	}
}

// The seed changes the hash, which is what lets one algorithm serve several
// independent routings.
func TestGivenASeedWhenHashedThenShouldChangeTheResult(t *testing.T) {
	unseeded := wire.XXHash32([]byte("abc"), 0)

	if wire.XXHash32([]byte("abc"), 1) == unseeded {
		t.Error("a seed must change the hash")
	}
	// The same seed gives the same answer, every time.
	if wire.XXHash32([]byte("abc"), 7) != wire.XXHash32([]byte("abc"), 7) {
		t.Error("one input and one seed give one hash")
	}
}

// The length is mixed in, so two inputs that differ only in length cannot
// collide through their shared bytes alone.
func TestGivenInputsOfEveryLengthWhenHashedThenShouldDifferFromEachOther(t *testing.T) {
	seen := map[uint32]string{}

	// Every length across the block, tail and byte paths of the algorithm.
	for length := range 40 {
		input := strings.Repeat("a", length)
		hash := wire.XXHash32([]byte(input), 0)
		if previous, collided := seen[hash]; collided {
			t.Errorf("%q and %q hash alike", previous, input)
		}
		seen[hash] = input
	}
}

// A key routes to one partition, inside the range the topic has, and the same
// key routes the same way every time.
func TestGivenAKeyWhenRoutedThenShouldLandInRangeAndStayThere(t *testing.T) {
	const partitions = 7

	for _, key := range []string{"", "a", "tenant-acme", strings.Repeat("x", 300)} {
		partition := wire.PartitionForKey([]byte(key), partitions)
		if partition >= partitions {
			t.Errorf("%q: want a partition under %d, got %d",
				key, partitions, partition)
		}
		if wire.PartitionForKey([]byte(key), partitions) != partition {
			t.Errorf("%q: want one key to route one way", key)
		}
	}

	// A topic with no partitions has nowhere to route to, which is not a
	// division by zero.
	if wire.PartitionForKey([]byte("k"), 0) != 0 {
		t.Error("want a topic with no partitions to route to zero")
	}
}

// Different keys spread across the partitions rather than piling onto one,
// which is the whole reason a hash is used instead of a counter.
func TestGivenManyKeysWhenRoutedThenShouldSpreadAcrossThePartitions(t *testing.T) {
	const partitions = 8
	used := map[uint32]int{}

	for index := range 200 {
		key := "conversation-" + strconv.Itoa(index)
		used[wire.PartitionForKey([]byte(key), partitions)]++
	}

	if len(used) != partitions {
		t.Errorf("want every partition used, got %d of %d", len(used), partitions)
	}
}
