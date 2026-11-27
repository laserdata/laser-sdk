package examples

import "testing"

// Regression test for a bug where an example run with only
// LASER_CONNECTION_STRING set (no LASER_STREAM) failed with "no stream
// selected" instead of defaulting a stream the way the Rust and TypeScript
// examples do.
func TestGivenNoLaserStreamWhenResolvingAnExampleStreamThenShouldDefaultPerExample(t *testing.T) {
	t.Setenv("LASER_STREAM", "")
	if got, want := StreamFor("agent"), "laser-agent-go"; got != want {
		t.Fatalf("StreamFor(%q) = %q, want %q", "agent", got, want)
	}
	if got, want := StreamFor("memory"), "laser-memory-go"; got != want {
		t.Fatalf("StreamFor(%q) = %q, want %q", "memory", got, want)
	}
}

func TestGivenLaserStreamSetWhenResolvingAnExampleStreamThenShouldUseIt(t *testing.T) {
	t.Setenv("LASER_STREAM", "my-named-stream")
	if got, want := StreamFor("agent"), "my-named-stream"; got != want {
		t.Fatalf("StreamFor(%q) = %q, want %q", "agent", got, want)
	}
}

func TestGivenLaserStreamBlankWhenResolvingAnExampleStreamThenShouldStillDefault(t *testing.T) {
	// A blank LASER_STREAM (set but empty) must not be treated as a pinned
	// stream name -- that would resolve to an empty DefaultStream and
	// reproduce the original "no stream selected" failure.
	t.Setenv("LASER_STREAM", "   ")
	if got, want := StreamFor("agent"), "laser-agent-go"; got != want {
		t.Fatalf("StreamFor(%q) = %q, want %q", "agent", got, want)
	}
}
