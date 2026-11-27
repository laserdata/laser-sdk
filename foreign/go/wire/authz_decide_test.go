package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func allow(feature wire.Feature, action wire.Action, resource wire.ResourcePattern) wire.Grant {
	return wire.Grant{
		Effect: wire.EffectAllow, Feature: feature, Action: action, Resource: resource,
	}
}

func deny(feature wire.Feature, action wire.Action, resource wire.ResourcePattern) wire.Grant {
	return wire.Grant{
		Effect: wire.EffectDeny, Feature: feature, Action: action, Resource: resource,
	}
}

// Deny wins over any allow, and an empty grant set permits nothing: there is
// no allow to match. A client that decided otherwise would let a request
// through that the server refuses.
func TestGivenGrantsWhenEvaluatedThenShouldLetDenyWin(t *testing.T) {
	readAll := allow(wire.FeatureKv, wire.ActionRead, wire.AllResources())
	denyOne := deny(wire.FeatureKv, wire.ActionRead, wire.LiteralResource("secrets"))

	if !wire.GrantsAllow([]wire.Grant{readAll}, wire.FeatureKv, wire.ActionRead,
		"anything", true) {
		t.Error("a whole-feature allow covers any resource")
	}
	if wire.GrantsAllow([]wire.Grant{readAll, denyOne}, wire.FeatureKv,
		wire.ActionRead, "secrets", true) {
		t.Error("a matching deny must win over an allow")
	}
	if !wire.GrantsAllow([]wire.Grant{readAll, denyOne}, wire.FeatureKv,
		wire.ActionRead, "public", true) {
		t.Error("a deny narrows only what it names")
	}
	if wire.GrantsAllow(nil, wire.FeatureKv, wire.ActionRead, "x", true) {
		t.Error("an empty grant set permits nothing")
	}
	// Another action or feature is a different capability entirely.
	if wire.GrantsAllow([]wire.Grant{readAll}, wire.FeatureKv, wire.ActionWrite,
		"x", true) {
		t.Error("a read grant does not imply write")
	}

	// A whole-feature pattern covers an unkeyed op. Every other kind needs a
	// selector to compare against.
	if !wire.GrantsAllow([]wire.Grant{readAll}, wire.FeatureKv, wire.ActionRead,
		"", false) {
		t.Error("a whole-feature allow covers an unkeyed op")
	}
	prefixed := allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("a/"))
	if wire.GrantsAllow([]wire.Grant{prefixed}, wire.FeatureKv, wire.ActionRead,
		"", false) {
		t.Error("a prefix grant cannot cover an unkeyed op")
	}
	if !wire.GrantsAllow([]wire.Grant{prefixed}, wire.FeatureKv, wire.ActionRead,
		"a/b", true) {
		t.Error("a prefix grant covers names under it")
	}
}

// An agent acting for a user can never exceed the user who invoked it.
func TestGivenDelegatedGrantsWhenEvaluatedThenShouldIntersectThem(t *testing.T) {
	agent := []wire.Grant{allow(wire.FeatureKv, wire.ActionWrite, wire.AllResources())}
	user := []wire.Grant{
		allow(wire.FeatureKv, wire.ActionWrite, wire.PrefixResource("stream:fleet/")),
	}

	if !wire.DelegatedAllow(agent, user, wire.FeatureKv, wire.ActionWrite,
		"stream:fleet/state", true) {
		t.Error("both sides permit this one")
	}
	if wire.DelegatedAllow(agent, user, wire.FeatureKv, wire.ActionWrite,
		"stream:other/state", true) {
		t.Error("the agent must not exceed the user")
	}
	if wire.DelegatedAllow(nil, user, wire.FeatureKv, wire.ActionWrite,
		"stream:fleet/state", true) {
		t.Error("an agent with no grants is permitted nothing")
	}
}

// Each capability occupies its own bit, so one feature's last action can never
// collide with the next feature's first.
func TestGivenEveryCapabilityWhenIndexedThenShouldOccupyItsOwnBit(t *testing.T) {
	features := []wire.Feature{
		wire.FeatureKv, wire.FeatureMemory, wire.FeatureProjection, wire.FeatureFork,
		wire.FeatureGraph, wire.FeatureQuery, wire.FeatureAgent, wire.FeatureWorkflow,
		wire.FeatureDestination, wire.FeatureCheckpoint, wire.FeatureAuthz,
		wire.FeatureKvLease, wire.FeatureKvFence, wire.FeatureFilter,
		wire.FeatureSession, wire.FeatureUnrecognized,
	}
	actions := []wire.Action{
		wire.ActionRead, wire.ActionWrite, wire.ActionDelete, wire.ActionAdmin,
		wire.ActionUnrecognized,
	}
	seen := map[int]bool{}
	for _, feature := range features {
		for _, action := range actions {
			index, placed := wire.ActionIndex(feature, action)
			if !placed {
				t.Fatalf("%s:%s has no bit", feature, action)
			}
			if seen[index] {
				t.Fatalf("%s:%s collides at bit %d", feature, action, index)
			}
			// The shared mask is 128 bits wide.
			if index >= 128 {
				t.Fatalf("%s:%s is at bit %d, past the mask", feature, action, index)
			}
			seen[index] = true
		}
	}
	if len(seen) != len(features)*wire.ActionCount {
		t.Errorf("want %d bits, got %d", len(features)*wire.ActionCount, len(seen))
	}
}

// A managed command code maps to the capability it is gated by. A code that
// mapped to the wrong one would let a client send a request the server
// refuses, or hold back one it would accept.
func TestGivenAManagedCommandCodeWhenMappedThenShouldNameItsCapability(t *testing.T) {
	cases := []struct {
		code    uint32
		feature wire.Feature
		action  wire.Action
	}{
		{wire.AgdxQueryCode, wire.FeatureQuery, wire.ActionRead},
		{wire.AgdxRegisterSchemaCode, wire.FeatureProjection, wire.ActionAdmin},
		{wire.AgdxKvGetCode, wire.FeatureKv, wire.ActionRead},
		{wire.AgdxKvSetCode, wire.FeatureKv, wire.ActionWrite},
		{wire.AgdxKvDeleteCode, wire.FeatureKv, wire.ActionDelete},
		// Lease control is its own feature: a write grant on a namespace must
		// never imply acquiring the lease that fences it.
		{wire.AgdxKvLeaseCode, wire.FeatureKvLease, wire.ActionAdmin},
		{wire.AgdxKvReleaseCode, wire.FeatureKvLease, wire.ActionAdmin},
		{wire.AgdxForkPromoteCode, wire.FeatureFork, wire.ActionAdmin},
		{wire.AgdxGraphUpsertCode, wire.FeatureGraph, wire.ActionWrite},
		{wire.AgdxSessionGetCode, wire.FeatureSession, wire.ActionRead},
		{wire.AgdxGetFilterCode, wire.FeatureFilter, wire.ActionRead},
	}
	for _, testCase := range cases {
		feature, action, gated := wire.FeatureAction(testCase.code)
		if !gated {
			t.Errorf("code %d must name a capability", testCase.code)
			continue
		}
		if feature != testCase.feature || action != testCase.action {
			t.Errorf("code %d is %s:%s, want %s:%s", testCase.code,
				feature, action, testCase.feature, testCase.action)
		}
	}

	// A code with no capability semantics is gated another way.
	if _, _, gated := wire.FeatureAction(wire.AgdxBatchCode); gated {
		t.Error("a batch is gated by the commands inside it")
	}
}

// A role's audit events are attributed to the one stream its allows stay
// inside, and to nothing when they reach further.
func TestGivenRoleGrantsWhenScopedThenShouldNameTheOneStreamTheyStayInside(t *testing.T) {
	inFleet := []wire.Grant{
		allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("stream:fleet/")),
		allow(wire.FeatureKv, wire.ActionWrite, wire.LiteralResource("stream:fleet/state")),
	}
	stream, scoped := wire.GrantsStream(inFleet)
	if !scoped || stream != "fleet" {
		t.Errorf("want fleet, got %q %v", stream, scoped)
	}

	// The stream resource itself counts as that stream.
	itself := []wire.Grant{
		allow(wire.FeatureKv, wire.ActionRead, wire.LiteralResource("stream:fleet")),
	}
	if stream, scoped := wire.GrantsStream(itself); !scoped || stream != "fleet" {
		t.Errorf("the stream resource names its own stream, got %q %v", stream, scoped)
	}

	// Two streams, a whole-feature grant, and a bare name all reach further.
	for name, grants := range map[string][]wire.Grant{
		"two streams": {
			allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("stream:a/")),
			allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("stream:b/")),
		},
		"a whole feature": {allow(wire.FeatureKv, wire.ActionRead, wire.AllResources())},
		"a bare name":     {allow(wire.FeatureKv, wire.ActionRead, wire.LiteralResource("x"))},
		"nothing allowed": {deny(wire.FeatureKv, wire.ActionRead, wire.AllResources())},
	} {
		if _, scoped := wire.GrantsStream(grants); scoped {
			t.Errorf("%s must not resolve to one stream", name)
		}
	}
}

// A deny only narrows access, so it never spans streams however broad it is.
func TestGivenAGrantWhenCheckedForSpanThenShouldIgnoreDenies(t *testing.T) {
	if !wire.GrantSpansStreams(allow(wire.FeatureKv, wire.ActionRead,
		wire.AllResources())) {
		t.Error("a whole-feature allow spans streams")
	}
	if wire.GrantSpansStreams(deny(wire.FeatureKv, wire.ActionRead,
		wire.AllResources())) {
		t.Error("a deny never spans")
	}
	if wire.GrantSpansStreams(allow(wire.FeatureKv, wire.ActionRead,
		wire.LiteralResource("anything"))) {
		t.Error("a literal names one resource")
	}
	if wire.GrantSpansStreams(allow(wire.FeatureKv, wire.ActionRead,
		wire.PrefixResource("stream:fleet/"))) {
		t.Error("a prefix inside one stream does not span")
	}
	if !wire.GrantSpansStreams(allow(wire.FeatureKv, wire.ActionRead,
		wire.PrefixResource("stream:"))) {
		t.Error("a prefix across streams spans")
	}
}
