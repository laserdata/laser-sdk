package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// roundTripJSONFixture decodes a JSON fixture and re-encodes it, asserting the
// bytes are identical.
//
// The HTTP surface's identity is its JSON, so a field that reordered, a
// default that stopped being skipped, or an id that rendered as bytes instead
// of text would all be a contract break even though the value is unchanged.
func roundTripJSONFixture[T any](t *testing.T, name string) T {
	t.Helper()
	golden := readFixture(t, name)

	var decoded T
	if err := wire.UnmarshalJSON(golden, &decoded); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	encoded, err := wire.MarshalJSONIndent(decoded)
	if err != nil {
		t.Fatalf("re-encode %s: %v", name, err)
	}
	if string(encoded) != string(golden) {
		t.Errorf("%s re-encoded differently:\n got %s\nwant %s", name, encoded, golden)
	}
	return decoded
}

// The HTTP routes. A path that encoded a name differently would address the
// wrong resource, or none.
func TestGivenNamesWithReservedCharactersWhenRoutedThenShouldEncodeOneSegment(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"a slash in a stream name",
			wire.SessionsPathFor("fleet/eu"), "/agdx/sessions/fleet%2Feu"},
		{"a question mark in a group name",
			wire.GroupPolicyPath("a", "b", "c?d"),
			"/agdx/group-policies/a/b/c%3Fd"},
		{"a hash in a topic name",
			wire.FilterBindingPath("s", "t#1", "g"),
			"/agdx/filter-bindings/s/t%231/g"},
		{"an unreserved name is untouched",
			wire.ProjectionPath("reading.v1-x_y~z"),
			"/agdx/projections/reading.v1-x_y~z"},
		{"a scoped graph name",
			wire.GraphQueryPath("stream:fleet/graph"),
			"/agdx/graph/stream%3Afleet%2Fgraph/query"},
		{"a multi-byte name encodes each byte",
			wire.ForkPath("λ"), "/agdx/forks/%CE%BB"},
	}
	for _, testCase := range cases {
		if testCase.got != testCase.want {
			t.Errorf("%s: got %q, want %q", testCase.name, testCase.got, testCase.want)
		}
	}
}
