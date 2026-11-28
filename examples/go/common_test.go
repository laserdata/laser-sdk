package examples

import (
	"fmt"
	"strings"
	"testing"
)

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
	// A blank LASER_STREAM is unset, not a pinned empty stream name.
	t.Setenv("LASER_STREAM", "   ")
	if got, want := StreamFor("agent"), "laser-agent-go"; got != want {
		t.Fatalf("StreamFor(%q) = %q, want %q", "agent", got, want)
	}
}

func TestGivenAProviderKeyWhenTheModelClientIsFormattedThenShouldNeverPrintTheKey(t *testing.T) {
	const key = "sk-example-secret-key"
	for _, provider := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY"} {
		t.Setenv("ANTHROPIC_API_KEY", "")
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv(provider, key)
		llm := DefaultLLM()
		if _, mock := llm.(MockLLM); mock {
			t.Fatalf("%s: want a provider client, got the mock", provider)
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if text := fmt.Sprintf(verb, llm); strings.Contains(text, key) {
				t.Errorf("%s with %s prints the key: %s", provider, verb, text)
			}
		}
	}
}
