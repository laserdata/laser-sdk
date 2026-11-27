package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The registry browse surface. Its three optional results answer a routine
// miss with an explicit null, so decoding one as an error would turn "no such
// id" into a failure.
func TestGivenBrowseFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("browse_reply_projections.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BrowseReply](t, "browse_reply_projections.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.BrowseProjections {
			t.Fatalf("want a projections outcome, got %+v", reply)
		}
	})
	t.Run("browse_reply_schemas.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BrowseReply](t, "browse_reply_schemas.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.BrowseSchemas {
			t.Fatalf("want a schemas outcome, got %+v", reply)
		}
	})
	t.Run("browse_reply_schema_registered.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BrowseReply](t, "browse_reply_schema_registered.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.BrowseSchemaRegistered {
			t.Fatalf("want an allocated id, got %+v", reply)
		}
		if reply.Outcome.SchemaID == 0 {
			t.Error("a registration reports the id it allocated")
		}
	})
	t.Run("browse_reply_decoded.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BrowseReply](t, "browse_reply_decoded.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.BrowseDecoded {
			t.Fatalf("want a decoded body, got %+v", reply)
		}
		if reply.Outcome.Decoded == nil {
			t.Error("the decoded fixture carries a body")
		}
	})
	t.Run("browse_list_projections_page.bin", func(t *testing.T) {
		request := roundTripFixture[wire.ListProjections](t,
			"browse_list_projections_page.bin")
		// Paging is explicit on the request, so the server pages rather than
		// the client reading the whole registry.
		if request.Page == 0 || request.Results == 0 {
			t.Errorf("the paged fixture names a page and a size, got %+v", request)
		}
	})
	t.Run("browse_get_projection_status.bin", func(t *testing.T) {
		request := roundTripFixture[wire.GetProjection](t,
			"browse_get_projection_status.bin")
		if !request.Status {
			t.Error("the status fixture asks for each binding's progress")
		}
	})
	t.Run("browse_reply_projection_status.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BrowseReply](t,
			"browse_reply_projection_status.bin")
		if !reply.IsOk() || reply.Outcome.Projection == nil {
			t.Fatalf("want one projection, got %+v", reply)
		}
		status := reply.Outcome.Projection.Status
		if len(status) == 0 {
			t.Fatal("the status fixture reports each binding's progress")
		}
		// Readiness is a ladder, and a status proves on its own only as far
		// as realization: catching up needs a captured target to compare to.
		if status[0].Realization == wire.RealizationRealized &&
			status[0].Readiness() != wire.ReadinessRealized {
			t.Error("a realized binding proves the realized level")
		}
	})
	t.Run("browse_binding_status_stalled.bin", func(t *testing.T) {
		status := roundTripFixture[wire.BindingStatus](t,
			"browse_binding_status_stalled.bin")
		// A stall is why a binding makes no progress although no record holds
		// it, so it must not read as a hold.
		if status.Stall == nil {
			t.Fatal("the stalled fixture names why it stalls")
		}
		if *status.Stall == wire.StallUnknown {
			t.Errorf("this build does not name the stall reason the contract uses")
		}
	})
	t.Run("register_schema_managed.bin", func(t *testing.T) {
		request := roundTripFixture[wire.RegisterSchema](t, "register_schema_managed.bin")
		if request.V != wire.QueryOpVersion {
			t.Errorf("want version %d, got %d", wire.QueryOpVersion, request.V)
		}
	})
}

// A lookup that finds nothing answers an explicit null, not an error.
func TestGivenAnEmptyBrowseLookupWhenEncodedThenShouldRideAsAnExplicitNull(t *testing.T) {
	for name, outcome := range map[string]wire.BrowseOutcome{
		"projection": {Kind: wire.BrowseProjection},
		"schema":     {Kind: wire.BrowseSchema},
		"decoded":    {Kind: wire.BrowseDecoded},
	} {
		encoded, err := wire.Marshal(outcome)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.BrowseOutcome
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back.Kind != outcome.Kind {
			t.Errorf("%s: variant changed", name)
			continue
		}
		if back.Projection != nil || back.Schema != nil || back.Decoded != nil {
			t.Errorf("%s: an empty lookup must decode as absent", name)
		}
	}
}

// Each managed request must ride the code the contract pins for it. A wrong
// pairing would send a request to the wrong server handler.
func TestGivenEveryManagedRequestWhenAskedForItsCodeThenShouldMatchTheContract(t *testing.T) {
	// A slice rather than a map: several request types hold slices, which Go
	// cannot use as map keys.
	pairs := []struct {
		request wire.Command
		want    uint32
	}{
		{wire.WhoamiReq{}, wire.AgdxAuthzWhoamiCode},
		{wire.ListRolesReq{}, wire.AgdxAuthzListRolesCode},
		{wire.GetRoleReq{}, wire.AgdxAuthzGetRoleCode},
		{wire.GetBindingsReq{}, wire.AgdxAuthzGetBindingsCode},
		{wire.DefineRoleReq{}, wire.AgdxAuthzDefineRoleCode},
		{wire.DeleteRoleReq{}, wire.AgdxAuthzDeleteRoleCode},
		{wire.BindRolesReq{}, wire.AgdxAuthzBindRolesCode},
		{wire.QueryEnvelope{}, wire.AgdxQueryCode},
		{wire.GetProjection{}, wire.AgdxGetProjectionCode},
		{wire.ListProjections{}, wire.AgdxListProjectionsCode},
		{wire.GetSchema{}, wire.AgdxGetSchemaCode},
		{wire.ListSchemas{}, wire.AgdxListSchemasCode},
		{wire.RegisterSchema{}, wire.AgdxRegisterSchemaCode},
		{wire.DecodeRecord{}, wire.AgdxDecodeRecordCode},
		{wire.KvGet{}, wire.AgdxKvGetCode},
		{wire.KvSet{}, wire.AgdxKvSetCode},
		{wire.KvScan{}, wire.AgdxKvScanCode},
		{wire.KvDelete{}, wire.AgdxKvDeleteCode},
		{wire.KvDeleteMany{}, wire.AgdxKvDeleteManyCode},
		{wire.KvNamespaces{}, wire.AgdxKvNamespacesCode},
		{wire.ForkCreate{}, wire.AgdxForkCreateCode},
		{wire.ForkDelete{}, wire.AgdxForkDeleteCode},
		{wire.ForkPromote{}, wire.AgdxForkPromoteCode},
		{wire.ForkList{}, wire.AgdxForkListCode},
		{wire.ForkPut{}, wire.AgdxForkPutCode},
		{wire.SessionGetRequest{}, wire.AgdxSessionGetCode},
		{wire.SessionListRequest{}, wire.AgdxSessionListCode},
		{wire.SessionEventsRequest{}, wire.AgdxSessionEventsCode},
		{wire.SessionStateRequest{}, wire.AgdxSessionStateCode},
		{wire.SessionLinksRequest{}, wire.AgdxSessionLinksCode},
		{wire.SessionSourcesRequest{}, wire.AgdxSessionSourcesCode},
		{wire.SessionChangesRequest{}, wire.AgdxSessionChangesCode},
		{wire.BatchRequest{}, wire.AgdxBatchCode},
	}

	seen := make(map[uint32]bool, len(pairs))
	for _, pair := range pairs {
		if got := pair.request.CommandCode(); got != pair.want {
			t.Errorf("%T: want code %d, got %d", pair.request, pair.want, got)
			continue
		}
		if seen[pair.want] {
			t.Errorf("code %d is claimed by more than one request", pair.want)
		}
		seen[pair.want] = true
	}
	if len(seen) != len(pairs) {
		t.Errorf("want %d distinct codes, got %d", len(pairs), len(seen))
	}

	// Every mutating request must be one the managed envelope gives a stable
	// operation identity, or a transport retry could apply it twice.
	for _, mutating := range []wire.Command{
		wire.KvSet{}, wire.KvDelete{}, wire.KvDeleteMany{},
		wire.ForkCreate{}, wire.ForkDelete{}, wire.ForkPromote{}, wire.ForkPut{},
		wire.BatchRequest{},
	} {
		if !wire.IsIdempotentManagedRequest(mutating.CommandCode()) {
			t.Errorf("%T mutates, so it needs a stable operation identity", mutating)
		}
	}
	// And every barriered read must be recognised as one.
	for _, barriered := range []wire.Command{
		wire.KvGet{}, wire.KvScan{}, wire.KvNamespaces{}, wire.ForkList{},
		wire.SessionGetRequest{}, wire.SessionListRequest{}, wire.SessionEventsRequest{},
		wire.SessionStateRequest{}, wire.SessionLinksRequest{},
		wire.SessionSourcesRequest{}, wire.SessionChangesRequest{},
	} {
		if !wire.IsBarrierRead(barriered.CommandCode()) {
			t.Errorf("%T reads folded state, so it may carry a barrier", barriered)
		}
	}
}
