package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The hello reply is the first frame a client reads, and every managed
// capability is gated on it, so its bytes must match exactly.
func TestGivenHelloFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	for _, name := range []string{
		"hello_reply.bin",
		"hello_reply_features.bin",
		"hello_reply_agent.bin",
	} {
		t.Run(name, func(t *testing.T) {
			raw := readFixture(t, name)
			var reply wire.HelloReply
			if err := wire.Unmarshal(raw, &reply); err != nil {
				t.Fatalf("decode: %v", err)
			}
			assertSameBytes(t, raw, mustMarshal(t, reply))
		})
	}
}

// A pre-versioned server answers the probe with an empty body. A client must
// read that as "nothing advertised", never as an error.
func TestGivenAnEmptyHelloBodyWhenDecodedThenShouldAdvertiseNothing(t *testing.T) {
	empty, err := wire.Marshal(struct{}{})
	if err != nil {
		t.Fatalf("encode an empty body: %v", err)
	}

	var reply wire.HelloReply
	if err := wire.Unmarshal(empty, &reply); err != nil {
		t.Fatalf("an empty hello body must decode, got %v", err)
	}
	if reply.Versions.Query != 0 || reply.Versions.Features != 0 {
		t.Fatalf("an empty body advertises nothing, got %+v", reply.Versions)
	}
	if reply.Versions.HasFeature(wire.CapabilityKvCas) {
		t.Error("no feature may read as advertised from an empty body")
	}
}

// The feature fixture carries 3, which is two specific bits. Decoding it must
// light up exactly those capabilities and no others, since a client gates real
// behaviour on them.
func TestGivenTheFeatureFixtureWhenReadThenShouldLightExactlyItsBits(t *testing.T) {
	var reply wire.HelloReply
	if err := wire.Unmarshal(readFixture(t, "hello_reply_features.bin"), &reply); err != nil {
		t.Fatalf("decode: %v", err)
	}

	const want = wire.CapabilityKvCas | wire.CapabilityReadYourWrites
	if reply.Versions.Features != want {
		t.Fatalf("want features %d, got %d", want, reply.Versions.Features)
	}
	if reply.Versions.Features != 3 {
		t.Fatalf("the fixture pins 3, got %d", reply.Versions.Features)
	}

	for name, bit := range map[string]uint64{
		"kv cas":           wire.CapabilityKvCas,
		"read your writes": wire.CapabilityReadYourWrites,
	} {
		if !reply.Versions.HasFeature(bit) {
			t.Errorf("%s must be advertised", name)
		}
	}
	// The retired bits are never reused, so a build that read one as served
	// would gate behaviour on a surface no server offers.
	for name, bit := range map[string]uint64{
		"strong consistency": wire.CapabilityStrongConsistency,
		"destinations":       wire.CapabilityDestinations,
		"consumer filters":   wire.CapabilityConsumerFilters,
		"sessions":           wire.CapabilitySessions,
		"fenced leases":      wire.CapabilityKvFencedLeases,
	} {
		if reply.Versions.HasFeature(bit) {
			t.Errorf("%s must not be advertised", name)
		}
	}
}

// HasFeature takes a set, so asking for two bits must require both.
func TestGivenAFeatureSetWhenCheckedThenShouldRequireEveryBit(t *testing.T) {
	versions := wire.NewOpVersions(1, 1, 1, 1).
		WithFeatures(wire.CapabilityKvCas | wire.CapabilityWatch)

	if !versions.HasFeature(wire.CapabilityKvCas | wire.CapabilityWatch) {
		t.Error("both advertised bits must satisfy the set")
	}
	if versions.HasFeature(wire.CapabilityKvCas | wire.CapabilitySessions) {
		t.Error("a set with one missing bit must not be satisfied")
	}
	if !versions.HasFeature(0) {
		t.Error("the empty set is always satisfied")
	}
}

// A surface at version zero is not served and must stay off the wire, so a
// hello frame from before that surface existed is unchanged.
func TestGivenUnservedSurfacesWhenEncodedThenShouldStayOffTheWire(t *testing.T) {
	base := wire.HelloReply{Versions: wire.NewOpVersions(1, 1, 1, 1)}
	encoded := mustMarshal(t, base)

	if string(encoded) != string(readFixture(t, "hello_reply.bin")) {
		t.Fatalf("the base hello must match the fixture byte for byte")
	}

	withAgent := wire.HelloReply{Versions: base.Versions.WithAgent(wire.AgentOpVersion)}
	if string(mustMarshal(t, withAgent)) != string(readFixture(t, "hello_reply_agent.bin")) {
		t.Fatal("advertising the agent surface must match the agent fixture")
	}
}
