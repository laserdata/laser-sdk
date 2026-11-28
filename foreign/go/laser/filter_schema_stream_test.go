package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAFilterWithSchemaRefsWhenAGroupIsConfiguredThenShouldResolveThemInTheHandlesStream(t *testing.T) {
	elsewhere, own := "shared-schemas", "agents"
	for name, expect := range map[string]struct {
		options laser.Options
		named   *string
		refs    []uint32
		want    *string
	}{
		"the handle's stream when none is named": {refs: []uint32{1}, want: &own},
		"the stream the caller named":            {refs: []uint32{1}, named: &elsewhere, want: &elsewhere},
		"none for a filter without schema refs":  {},
		"none for a handle that does not scope names": {
			options: laser.Options{ResourceNaming: laser.ResourceNamingBare}, refs: []uint32{1},
		},
	} {
		group := wire.FilterGroupRef{Stream: "agents", Topic: "events", Group: "readers"}
		binding := wire.FilterBinding{Group: group, Digest: wire.NewDigest32([32]byte{4})}
		server := &filterServer{catalog: appliedResult(
			wire.FilterMutationResult{Kind: wire.ResultBound, Binding: &binding})}
		definition := inlineDefinition()
		definition.SchemaRefs, definition.SchemaStream = expect.refs, expect.named
		catalog := filterHandle(server, expect.options).FilterCatalog()

		if _, _, err := catalog.ConfigureGroup(
			context.Background(), group, wire.DefinitionSpec("safe-mode", definition)); err != nil {
			t.Fatalf("%s: configure: %v", name, err)
		}
		if _, _, err := catalog.ConfigureGroupAs(context.Background(), laser.NewOperationID(), group,
			wire.DefinitionSpec("safe-mode", definition), wire.FilterGroupIdentity{}); err != nil {
			t.Fatalf("%s: configure as: %v", name, err)
		}

		for index := range server.requests {
			var request wire.FilterMutationRequest
			decodeFilterRequest(t, server, index, &request)
			got := request.Mutation.Policy.Filter.SchemaStream
			if (got == nil) != (expect.want == nil) || got != nil && *got != *expect.want {
				t.Errorf("%s, request %d: want schema stream %v, got %v", name, index, schemaStreamText(expect.want), schemaStreamText(got))
			}
		}
	}
}

func schemaStreamText(value *string) string {
	if value == nil {
		return "<none>"
	}
	return *value
}
