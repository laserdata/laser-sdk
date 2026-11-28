package laser

import (
	"testing"
	"time"
)

// SetOwnWritesWaitForTest shortens how long a recall waits for its own writes.
func SetOwnWritesWaitForTest(t *testing.T, wait time.Duration) {
	previous := ownWritesWait
	ownWritesWait = wait
	t.Cleanup(func() { ownWritesWait = previous })
}
