package wire_test

import (
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The fixture corpus proves the bytes of the shapes it covers. It does not
// cover every shape: a request a fixture never carries can still have an
// encoder and a decoder that disagree, and nothing would say so.
//
// A round trip says so. Populating every field, encoding, decoding and
// encoding again catches the asymmetries a fixture would have caught: a field
// that encodes but never decodes, a tag that differs between the two halves,
// a codec that drops a value it cannot read back.
//
// It proves less than a fixture does, because the bytes are this port's own
// rather than the contract's. It is what can be proven without one.

// roundTripSamples is one value of every wire-encoded type the fixture corpus
// does not reach. An audit below fails when a type is added without one.
var roundTripSamples = []any{
	// The registry browse and listing requests.
	wire.GetFilter{}, wire.GetFilterBinding{}, wire.GetFilterOperation{},
	wire.GetSchema{}, wire.ListFilters{}, wire.ListFilterRevisions{},
	wire.ListFilterBindings{}, wire.ListSchemas{}, wire.ResolveFilterPolicy{},
	wire.WatchFilterCatalog{}, wire.FilterCatalogVersion{},

	// The fork and key-value requests.
	wire.ForkCreate{}, wire.ForkDelete{}, wire.ForkList{}, wire.ForkPromote{},
	wire.KvDeleteMany{}, wire.KvExists{}, wire.KvExpire{},

	// The authorization requests.
	wire.WhoamiReq{}, wire.ListRolesReq{}, wire.GetRoleReq{},
	wire.DefineRoleReq{}, wire.DeleteRoleReq{}, wire.GetBindingsReq{},
	wire.RoleBinding{},

	// The filter read and preview requests.
	wire.FilterPreviewRequest{}, wire.FilterTestRequest{}, wire.FilterHeader{},
	wire.FilterRecord{},

	// The HTTP query strings and bodies.
	wire.ClientsQuery{}, wire.ProjectionListQuery{}, wire.SchemaListQuery{},
	wire.SchemaQuery{}, wire.StreamQuery{}, wire.OperationQuery{},
	wire.FilterListQuery{}, wire.FilterRevisionListQuery{},
	wire.FilterBindingListQuery{}, wire.KvScanQuery{}, wire.KvPutQuery{},
	wire.KvCasQuery{}, wire.GraphNeighborsQuery{}, wire.SessionsQuery{},
	wire.SessionEventsQuery{}, wire.SessionStateQuery{}, wire.SessionLinksQuery{},
	wire.SessionChangesQuery{},
	wire.RegisterSchemaBody{}, wire.DecodeRecordBody{}, wire.ForkCreateBody{},
	wire.ForkPutBody{}, wire.RemoveBindingBody{}, wire.FilterTestBody{},
	wire.CasCommittedView{}, wire.DeletedManyView{}, wire.PromotedView{},
	wire.ClientMetadataView{}, wire.ClientMetadataListView{},
	wire.GraphNodeView{}, wire.GraphEdgeView{}, wire.PathView{},
	wire.GraphResultView{},

	// The remaining record and session shapes.
	wire.SessionPauseRequest{},
	wire.BinaryValue{}, wire.UuidValue{},

	// The values whose own codec is the whole shape: a header scalar, a
	// readiness level, a policy reference, and the rendered attribute pair.
	wire.StringHeaderScalar("x"), wire.ReadinessRealized,
	wire.FilterPolicyRef{Kind: wire.PolicyByRevision, FilterID: 1, Revision: 2},
	wire.NamedText{Name: "host", Value: "node-7"},
}

// A populated value must survive a round trip unchanged.
func TestGivenEveryUnfixturedShapeWhenRoundTrippedThenShouldKeepItsBytes(t *testing.T) {
	for _, sample := range roundTripSamples {
		typeOf := reflect.TypeOf(sample)
		t.Run(typeOf.Name(), func(t *testing.T) {
			populated := reflect.New(typeOf).Elem()
			populateForRoundTrip(populated, 0)
			value := populated.Interface()

			encoded, err := wire.Marshal(value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			decoded := reflect.New(typeOf)
			if err := wire.Unmarshal(encoded, decoded.Interface()); err != nil {
				t.Fatalf("decode: %v", err)
			}
			again, err := wire.Marshal(decoded.Elem().Interface())
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if string(again) != string(encoded) {
				t.Errorf("a round trip changed the bytes (%d in, %d out)%s",
					len(encoded), len(again), describeDiff(encoded, again))
			}
		})
	}
}

// sampleValues are the types whose values are drawn from a closed set, where
// an arbitrary one would decode to the unrecognized passthrough and so could
// not round-trip.
var sampleValues = map[string]any{
	"ContentType":         wire.ContentJSON,
	"Consistency":         wire.ConsistencyReadYourWrites,
	"FilterCodec":         wire.CodecJSON,
	"ResultCode":          wire.ResultConflict,
	"QueryErrorCode":      wire.QueryCodeStale,
	"RealizationState":    wire.RealizationRealized,
	"FailureClass":        wire.FailureSchema,
	"StallReason":         wire.StallBackpressure,
	"HoldCause":           wire.HoldSchema,
	"InvalidRecordPolicy": wire.InvalidRecordDeadLetter,
	"OversizePolicy":      wire.OversizeReference,
	"FilterState":         wire.FilterActive,
	"LinkSurface":         wire.LinkKv,
	"SchemaSourceKind":    wire.SchemaAvro,
	"LogicalTypeKind":     wire.KindLong,
	"SqlDialect":          wire.DialectPostgres,
	"AggFunc":             wire.AggCount,
	"Dir":                 wire.DirAsc,
	"CmpOp":               wire.CmpEq,
	"Feature":             wire.FeatureKv,
	"Action":              wire.ActionRead,
	"Effect":              wire.EffectAllow,
	"ForkKind":            wire.ForkSevered,
	"ResourceKind":        wire.ResourceLiteral,
	// A header scalar and a source reference are tagged unions, so an
	// arbitrary value names no variant and could not encode at all.
	"HeaderScalar": wire.StringHeaderScalar("x"),
	"SourceRef": wire.NewMessageSource(wire.MessageSource{
		Stream: 5, Topic: 3, Partition: 1, Offset: 40,
	}),
	// An agent id is validated on decode, so it must be a real one.
	"AgentID": mustSampleAgent(),
}

// mustSampleAgent is one valid agent id, for the shapes that carry one.
func mustSampleAgent() wire.AgentID {
	agent, err := wire.NewAgentID("planner")
	if err != nil {
		panic("the sample agent id must parse: " + err.Error())
	}
	return agent
}

// populateForRoundTrip fills every field with a value that is not the zero
// one, so a field skipped on encode is actually exercised rather than
// silently omitted.
func populateForRoundTrip(value reflect.Value, depth int) {
	if depth > 8 {
		return
	}
	if sample, closed := sampleValues[value.Type().Name()]; closed {
		sampleOf := reflect.ValueOf(sample)
		if sampleOf.Type().AssignableTo(value.Type()) {
			value.Set(sampleOf)
			return
		}
	}

	switch value.Kind() {
	case reflect.Bool:
		value.SetBool(true)
	case reflect.String:
		value.SetString("x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(1)
	case reflect.Float32, reflect.Float64:
		value.SetFloat(1.5)
	case reflect.Pointer:
		created := reflect.New(value.Type().Elem())
		populateForRoundTrip(created.Elem(), depth+1)
		value.Set(created)
	case reflect.Slice:
		// A byte slice is one value, not a collection of them.
		if value.Type().Elem().Kind() == reflect.Uint8 &&
			value.Type().Elem().PkgPath() == "" {
			value.SetBytes([]byte{1, 2, 3})
			return
		}
		item := reflect.New(value.Type().Elem()).Elem()
		populateForRoundTrip(item, depth+1)
		value.Set(reflect.Append(value, item))
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			populateForRoundTrip(value.Index(i), depth+1)
		}
	case reflect.Map:
		created := reflect.MakeMap(value.Type())
		key := reflect.New(value.Type().Key()).Elem()
		populateForRoundTrip(key, depth+1)
		item := reflect.New(value.Type().Elem()).Elem()
		populateForRoundTrip(item, depth+1)
		created.SetMapIndex(key, item)
		value.Set(created)
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				populateForRoundTrip(value.Field(i), depth+1)
			}
		}
	}
}

// Every wire-encoded shape must be proven by a fixture or by a round trip.
//
// A shape the corpus never carries and this registry never names is a shape
// nothing checks: it compiles, its field names look right, and whether its
// two halves agree is unknown. This fails when such a shape appears, so the
// gap is closed when it is introduced rather than found later.
func TestGivenEveryWireShapeWhenCheckedThenShouldBeFixturedOrRoundTripped(t *testing.T) {
	fixtured := goTypesReachedByFixtures(t)

	roundTripped := map[string]bool{}
	for _, sample := range roundTripSamples {
		roundTripped[reflect.TypeOf(sample).Name()] = true
	}

	// The two classification verdicts are proven by the dispatch corpus,
	// which compares the exact string every client must reach for the same
	// record. That is stronger than a round trip of this port's own bytes.
	provenByCorpus := map[string]bool{"Dispatch": true, "DisplayType": true}

	var unproven []string
	for _, name := range sortedKeys(wireEncodedGoTypes(t)) {
		// An unexported shape is a variant's payload body, which rides inside
		// its variant and is proven whenever that variant is.
		if !token.IsExported(name) || fixtured[name] || roundTripped[name] ||
			provenByCorpus[name] {
			continue
		}
		unproven = append(unproven, name)
	}
	if len(unproven) != 0 {
		t.Errorf("%d wire shapes are neither fixtured nor round-tripped:\n  %s",
			len(unproven), strings.Join(unproven, "\n  "))
	}
}
