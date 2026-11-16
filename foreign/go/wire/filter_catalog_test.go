package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The catalog mutations and their replies. A mutation that decoded as the
// wrong variant would reconfigure the wrong filter or group.
func TestGivenCatalogFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("filter_mutation_register.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilterMutationRequest](t, "filter_mutation_register.bin")
		if request.Mutation.Kind != wire.MutateRegister {
			t.Fatalf("want a registration, got kind %d", request.Mutation.Kind)
		}
		if request.Mutation.Filter == nil || request.Mutation.Name == "" {
			t.Error("a registration names the filter and its name")
		}
		// The operation id of this fixture exceeds 64 bits, so it rides as a
		// bignum rather than a plain integer.
		if _, fits := request.OperationID.Uint64(); fits {
			t.Error("the register fixture carries a 128-bit operation id")
		}
	})
	t.Run("filter_mutation_configure_group.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilterMutationRequest](t,
			"filter_mutation_configure_group.bin")
		if request.Mutation.Kind != wire.MutateConfigureGroup {
			t.Fatalf("want a group configuration, got kind %d", request.Mutation.Kind)
		}
		if request.Mutation.Group == nil || request.Mutation.Policy == nil {
			t.Error("a group configuration names the group and its policy")
		}
	})
	t.Run("filter_mutation_revision_state.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilterMutationRequest](t,
			"filter_mutation_revision_state.bin")
		if request.Mutation.Kind != wire.MutateSetRevisionEnabled {
			t.Fatalf("want a revision-state change, got kind %d", request.Mutation.Kind)
		}
	})
	t.Run("filter_mutation_unbind_exact.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilterMutationRequest](t,
			"filter_mutation_unbind_exact.bin")
		if request.Mutation.Kind != wire.MutateUnbind {
			t.Fatalf("want an unbind, got kind %d", request.Mutation.Kind)
		}
		// An exact unbind pins both the digest and the group identity, so a
		// recreated group never inherits the release.
		if request.Mutation.ExpectedDigest == nil {
			t.Error("an unbind is conditional on the bound digest")
		}
		if request.Mutation.ExpectedIdentity == nil {
			t.Error("the exact fixture pins the group identity")
		}
	})
}

func TestGivenCatalogReplyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("filter_catalog_reply_registered.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterCatalogReply](t,
			"filter_catalog_reply_registered.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.CatalogMutation {
			t.Fatalf("want a mutation outcome, got %+v", reply)
		}
		status := reply.Outcome.Mutation.Status
		if status.Kind != wire.StatusApplied ||
			status.Applied.Kind != wire.ResultRegistered {
			t.Fatalf("want an applied registration, got %+v", status)
		}
		if reply.RegisteredDigest() == nil {
			t.Error("a registration reports its digest")
		}
	})
	t.Run("filter_catalog_reply_configured.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterCatalogReply](t,
			"filter_catalog_reply_configured.bin")
		if !reply.IsOk() {
			t.Fatalf("want a successful reply, got %+v", reply.Err)
		}
	})
	t.Run("filter_catalog_reply_bound.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterCatalogReply](t, "filter_catalog_reply_bound.bin")
		if !reply.IsOk() {
			t.Fatalf("want a successful reply, got %+v", reply.Err)
		}
	})
	t.Run("filter_catalog_reply_unbound.bin", func(t *testing.T) {
		roundTripFixture[wire.FilterCatalogReply](t, "filter_catalog_reply_unbound.bin")
	})
	t.Run("filter_catalog_reply_revisions.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterCatalogReply](t,
			"filter_catalog_reply_revisions.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.CatalogRevisions {
			t.Fatalf("want a revisions page, got %+v", reply)
		}
	})
	t.Run("filter_catalog_reply_revision_state.bin", func(t *testing.T) {
		roundTripFixture[wire.FilterCatalogReply](t, "filter_catalog_reply_revision_state.bin")
	})
}

// A revision with no enabled field must read as live, not paused. Reading it
// as paused would silently stop a reader that was running fine.
func TestGivenARevisionWithoutAnEnabledFieldWhenDecodedThenShouldReadAsLive(t *testing.T) {
	partial, err := wire.Marshal(struct {
		Revision uint32 `cbor:"revision"`
	}{Revision: 3})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var info wire.FilterRevisionInfo
	if err := wire.Unmarshal(partial, &info); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !info.Enabled {
		t.Fatal("a revision with no enabled field is live")
	}

	// An explicit false must still win.
	paused, err := wire.Marshal(struct {
		Enabled  bool   `cbor:"enabled"`
		Revision uint32 `cbor:"revision"`
	}{Enabled: false, Revision: 3})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var pausedInfo wire.FilterRevisionInfo
	if err := wire.Unmarshal(paused, &pausedInfo); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pausedInfo.Enabled {
		t.Fatal("an explicit false must pause the revision")
	}
}

// The catalog byte cap defaults on decode, since zero disables a limit and a
// command written before the cap existed must keep a real bound.
func TestGivenCatalogLimitsWithoutAByteCapWhenDecodedThenShouldKeepABound(t *testing.T) {
	partial, err := wire.Marshal(struct {
		MaxDefinitions uint32 `cbor:"max_definitions"`
	}{MaxDefinitions: 10})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var limits wire.FilterCatalogLimits
	if err := wire.Unmarshal(partial, &limits); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if limits.MaxDefinitions != 10 {
		t.Errorf("the named field must win, got %d", limits.MaxDefinitions)
	}
	if limits.MaxCatalogBytes != wire.DefaultFilterCatalogLimits().MaxCatalogBytes {
		t.Fatalf("want the default byte cap, got %d", limits.MaxCatalogBytes)
	}
}

// Every catalog mutation must survive the wire as its own variant.
func TestGivenEveryCatalogMutationWhenRoundTrippedThenShouldKeepItsVariant(t *testing.T) {
	filter := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")
	digest := wire.NewDigest32([32]byte{3})
	group := wire.FilterGroupRef{Stream: "orders", Topic: "changes", Group: "cdc"}
	identity := wire.FilterGroupIdentity{StreamID: 1, TopicID: 2, GroupID: 3}
	spec := wire.DefinitionSpec("group-policy", filter)

	mutations := map[string]wire.FilterMutation{
		"set revision enabled": {Kind: wire.MutateSetRevisionEnabled, FilterID: 1, Revision: 2},
		"register":             {Kind: wire.MutateRegister, Name: "n", Filter: &filter},
		"revise":               {Kind: wire.MutateRevise, FilterID: 1, ExpectedRevision: 1, Filter: &filter},
		"describe":             {Kind: wire.MutateDescribe, FilterID: 1, Description: "d"},
		"archive":              {Kind: wire.MutateArchive, FilterID: 1},
		"drop":                 {Kind: wire.MutateDrop, FilterID: 1},
		"bind":                 {Kind: wire.MutateBind, Group: &group, FilterID: 1, Revision: 1},
		"unbind":               {Kind: wire.MutateUnbind, Group: &group, ExpectedDigest: &digest},
		"configure group":      {Kind: wire.MutateConfigureGroup, Group: &group, Policy: &spec, ExpectedIdentity: &identity},
	}

	for name, mutation := range mutations {
		encoded, err := wire.Marshal(mutation)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.FilterMutation
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back.Kind != mutation.Kind {
			t.Errorf("%s: variant changed from %d to %d", name, mutation.Kind, back.Kind)
			continue
		}
		again, err := wire.Marshal(back)
		if err != nil {
			t.Errorf("%s: re-marshal: %v", name, err)
			continue
		}
		if string(again) != string(encoded) {
			t.Errorf("%s: re-encode differs", name)
		}
	}
}

// An unrecognized feature or action is inert: it decodes rather than failing
// the grant set, and matches no request, so it is default-deny never a silent
// allow.
func TestGivenAnUnknownGrantVocabularyWhenDecodedThenShouldBeInert(t *testing.T) {
	encoded, err := wire.Marshal(wire.Feature("timetravel"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var feature wire.Feature
	if err := wire.Unmarshal(encoded, &feature); err != nil {
		t.Fatalf("an unknown feature must decode: %v", err)
	}
	if feature != wire.FeatureUnrecognized || feature.IsNamed() {
		t.Fatalf("want the inert feature, got %q", feature)
	}

	actionEncoded, err := wire.Marshal(wire.Action("incinerate"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var action wire.Action
	if err := wire.Unmarshal(actionEncoded, &action); err != nil {
		t.Fatalf("an unknown action must decode: %v", err)
	}
	if action != wire.ActionUnrecognized || action.IsNamed() {
		t.Fatalf("want the inert action, got %q", action)
	}

	// A whole grant carrying both still decodes, so one foreign grant cannot
	// fail a set.
	grant := wire.Grant{
		Effect: wire.EffectAllow, Feature: "timetravel",
		Action: "incinerate", Resource: wire.AllResources(),
	}
	grantEncoded, err := wire.Marshal(grant)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.Grant
	if err := wire.Unmarshal(grantEncoded, &back); err != nil {
		t.Fatalf("a grant with unknown vocabulary must decode: %v", err)
	}
	if back.Feature.IsNamed() || back.Action.IsNamed() {
		t.Fatal("the unknown vocabulary must stay inert")
	}
}
