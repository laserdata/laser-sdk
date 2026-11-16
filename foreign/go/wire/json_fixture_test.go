package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The JSON bodies the managed surface serves. Their identity is their JSON,
// so a field that reordered or a default that stopped being skipped is a
// contract break even when the value is unchanged.

func TestGivenManagedJSONFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("capabilities.json", func(t *testing.T) {
		capabilities := roundTripJSONFixture[wire.Capabilities](t, "capabilities.json")
		if !capabilities.Managed {
			t.Error("the fixture reports a managed deployment")
		}
		// The consistency ladder is ordered, so naming one level implies the
		// weaker ones.
		if capabilities.Query.Consistency != wire.ConsistencyReadYourWrites {
			t.Errorf("want read-your-writes, got %q", capabilities.Query.Consistency)
		}
		if !capabilities.Query.Projections || !capabilities.Query.Schemas {
			t.Error("the fixture query surface serves both registry browses")
		}
		if len(capabilities.Backends) != 2 {
			t.Errorf("want two announced backends, got %d", len(capabilities.Backends))
		}
	})

	t.Run("error_body.json", func(t *testing.T) {
		body := roundTripJSONFixture[wire.ErrorBody](t, "error_body.json")
		// A client branches on the code, never on the message.
		if body.Code != wire.ResultConflict {
			t.Errorf("want a conflict, got %q", body.Code)
		}
		if body.Error() != body.Message {
			t.Error("a decoded failure is usable as an error")
		}
		// The detail carries the structured context a caller acts on, here
		// the version the entry currently holds.
		if body.Detail == nil {
			t.Fatal("the fixture carries a detail")
		}
		current, present := body.Detail.Field("current")
		if !present {
			t.Fatal("the detail names the current version")
		}
		if _, isText := current.Text(); isText {
			t.Error("the current version is a number, not text")
		}
	})

	t.Run("fork_info.json", func(t *testing.T) {
		info := roundTripJSONFixture[wire.ForkInfo](t, "fork_info.json")
		if info.ForkID == "" {
			t.Error("a fork names itself")
		}
		if info.Parent == nil || *info.Parent == "" {
			t.Error("the fixture fork has a parent")
		}
	})

	t.Run("schema_def.json", func(t *testing.T) {
		def := roundTripJSONFixture[wire.SchemaDef](t, "schema_def.json")
		if def.ID == 0 || def.Source.Kind != wire.SchemaAvro {
			t.Errorf("want an Avro schema, got %+v", def.Source.Kind)
		}
		if def.Source.Schema == "" {
			t.Error("an Avro source carries its schema text")
		}
	})

	t.Run("kv_page_view.json", func(t *testing.T) {
		page := roundTripJSONFixture[wire.KvPageView](t, "kv_page_view.json")
		if len(page.Items) != 1 {
			t.Fatalf("want one entry, got %d", len(page.Items))
		}
		// A scan reports where it is in the whole result, so a caller can
		// page without a cursor it has to carry.
		if page.Page != 1 || page.TotalPages != 1 || page.TotalResults != 1 {
			t.Errorf("want a single full page, got %+v", page)
		}
		// Keys and values are base64 here, because a JSON string cannot carry
		// arbitrary bytes.
		key, err := wire.Base64URLDecode(page.Items[0].Key)
		if err != nil {
			t.Fatalf("the key must be base64url: %v", err)
		}
		if string(key) != "user:1" {
			t.Errorf("want the decoded key, got %q", key)
		}
		if page.Items[0].ExpiresAtMicros == nil {
			t.Error("the fixture entry expires")
		}
	})

	t.Run("query_result.json", func(t *testing.T) {
		result := roundTripJSONFixture[wire.QueryResult](t, "query_result.json")
		if len(result.Fields) == 0 {
			t.Error("a result names its fields")
		}
	})

	t.Run("browse_projections.json", func(t *testing.T) {
		page := roundTripJSONFixture[wire.ProjectionPage](t,
			"browse_projections.json")
		if len(page.Items) == 0 {
			t.Fatal("the fixture lists projections")
		}
		if page.Items[0].Projection.ID == "" {
			t.Error("a browsed projection names itself")
		}
		if page.Page == 0 || page.TotalPages == 0 {
			t.Errorf("a listing reports where it sits, got %+v", page)
		}
	})

	t.Run("browse_schemas.json", func(t *testing.T) {
		entries := roundTripJSONFixture[[]wire.SchemaInfo](t, "browse_schemas.json")
		if len(entries) == 0 {
			t.Fatal("the fixture lists schemas")
		}
		// A dropped schema stays listed so a reader of an old record can still
		// resolve the id it names.
		dropped := false
		for _, entry := range entries {
			dropped = dropped || entry.Dropped
		}
		if !dropped {
			t.Error("the fixture includes a dropped schema")
		}
	})

	t.Run("filter_validation.json", func(t *testing.T) {
		validation := roundTripJSONFixture[wire.FilterValidation](t,
			"filter_validation.json")
		// The digest is the filter's identity, so a validation that lost it
		// could not be matched to what was validated.
		if len(validation.Digest.Bytes) != wire.DigestBytes {
			t.Errorf("a validation carries a %d byte digest",
				len(validation.Digest.Bytes))
		}
	})

}

// The saved-filter catalog mutations in their JSON form. The binary forms are
// pinned elsewhere, and these prove the same mutation survives the other
// encoding.
func TestGivenCatalogMutationJSONFixturesWhenDecodedThenShouldKeepTheirVariant(t *testing.T) {
	cases := map[string]wire.FilterMutationKind{
		"filter_mutation_register.json":        wire.MutateRegister,
		"filter_mutation_configure_group.json": wire.MutateConfigureGroup,
		"filter_mutation_revision_state.json":  wire.MutateSetRevisionEnabled,
		"filter_mutation_unbind_exact.json":    wire.MutateUnbind,
	}
	for name, kind := range cases {
		t.Run(name, func(t *testing.T) {
			request := roundTripJSONFixture[wire.FilterMutationRequest](t, name)
			if request.Mutation.Kind != kind {
				t.Errorf("mutation is %d, want %d", request.Mutation.Kind, kind)
			}
		})
	}
}

// Every SDK and the session fold share one token estimate, so a context
// assembled in Go fills the same window it would in any other client.
func TestGivenTheTokenEstimateCorpusWhenComputedThenShouldMatchEveryEntry(t *testing.T) {
	type estimate struct {
		Bytes  int    `cbor:"bytes"`
		Tokens uint64 `cbor:"tokens"`
	}
	var corpus []estimate
	if err := wire.UnmarshalJSON(readFixture(t, "token_estimates.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus) == 0 {
		t.Fatal("the corpus lists estimates")
	}
	for _, entry := range corpus {
		if got := wire.EstimateTokens(entry.Bytes); got != entry.Tokens {
			t.Errorf("%d bytes estimates %d tokens, the contract says %d",
				entry.Bytes, got, entry.Tokens)
		}
	}
}
