package laser

import (
	"testing"
	"time"
)

// SetControlTopicCheckForTest shortens how often an idle control follower
// checks that agent.control is still the topic it reads.
func SetControlTopicCheckForTest(t *testing.T, interval time.Duration) {
	previous := controlTopicCheck
	controlTopicCheck = interval
	t.Cleanup(func() { controlTopicCheck = previous })
}
