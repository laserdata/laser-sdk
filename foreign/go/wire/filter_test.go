package wire_test

import (
	"encoding/json"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenTheConsumerFilterFixtureWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	filter := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")

	if filter.Codec != wire.CodecJSON {
		t.Errorf("want the JSON codec, got %q", filter.Codec)
	}
	if filter.FaultPolicy != wire.FaultStop {
		t.Errorf("want the stop fault policy, got %q", filter.FaultPolicy)
	}
	if filter.Expr.Kind != wire.ExprAny {
		t.Errorf("want a disjunction at the root, got kind %d", filter.Expr.Kind)
	}
	if err := filter.Validate(); err != nil {
		t.Fatalf("the corpus filter must validate: %v", err)
	}
}

// The digest is the filter's identity in the catalog, taken over its canonical
// JSON rather than its CBOR. If Go produced different JSON, two filters with
// identical semantics would get different identities in different clients, and
// the catalog's dedup and revision logic would split.
//
// The registered-catalog fixture carries the digest the Rust encoder produced
// for this filter, so matching it proves the whole canonical JSON encoding.
func TestGivenTheCorpusFilterWhenDigestedThenShouldMatchTheCatalogDigest(t *testing.T) {
	filter := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")

	digest, err := filter.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if len(digest.Bytes) != wire.DigestBytes {
		t.Fatalf("want a %d-byte digest, got %d", wire.DigestBytes, len(digest.Bytes))
	}

	// The digest the Rust side recorded when it registered this filter.
	reply := roundTripFixture[wire.FilterCatalogReply](t, "filter_catalog_reply_registered.bin")
	registered := reply.RegisteredDigest()
	if registered == nil {
		t.Fatal("the registered fixture carries a digest")
	}
	recorded := registered.Bytes
	if string(digest.Bytes) != string(recorded) {
		t.Fatalf("digest differs from the catalog's\n computed: %x\n recorded: %x",
			digest.Bytes, recorded)
	}
}

// The canonical JSON must match serde's structure exactly, so it is compared
// against the readable JSON fixture after both are normalised. A field order
// or skip-rule difference would change the digest.
func TestGivenTheCorpusFilterWhenEncodedAsJSONThenShouldMatchTheJSONFixture(t *testing.T) {
	filter := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")

	encoded, err := wire.MarshalCanonicalJSON(filter)
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}

	var fromGo, fromRust any
	if err := json.Unmarshal(encoded, &fromGo); err != nil {
		t.Fatalf("the canonical JSON must parse: %v\n%s", err, encoded)
	}
	if err := json.Unmarshal(readFixture(t, "filter_consumer_filter.json"), &fromRust); err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}

	goShape, err := json.Marshal(fromGo)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	rustShape, err := json.Marshal(fromRust)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if string(goShape) != string(rustShape) {
		t.Fatalf("the canonical JSON differs in shape\n go:   %s\n rust: %s", goShape, rustShape)
	}
}

// The canonical writer must differ from Go's encoding/json where serde does:
// bytes as integer arrays, and no HTML escaping.
func TestGivenCanonicalJSONWhenWrittenThenShouldFollowSerdeNotEncodingJSON(t *testing.T) {
	// A binary literal rides as an array of integers, not base64.
	binary := wire.BinaryTyped([]byte{1, 2, 255})
	encoded, err := wire.MarshalCanonicalJSON(binary)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const wantBinary = `{"kind":"binary","value":[1,2,255]}`
	if string(encoded) != wantBinary {
		t.Errorf("want %s, got %s", wantBinary, encoded)
	}

	// HTML characters stay literal, where encoding/json would escape them.
	text := wire.StringTyped("a<b>c&d")
	encodedText, err := wire.MarshalCanonicalJSON(text)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const wantText = `{"kind":"string","value":"a<b>c&d"}`
	if string(encodedText) != wantText {
		t.Errorf("want %s, got %s", wantText, encodedText)
	}

	// A unit variant carries no value entry at all.
	encodedNull, err := wire.MarshalCanonicalJSON(wire.NullTyped())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encodedNull) != `{"kind":"null"}` {
		t.Errorf("want a bare kind, got %s", encodedNull)
	}
}

// A changed filter must get a changed identity, or a revised filter would
// silently reuse the previous one's catalog entry.
func TestGivenFilterChangesWhenDigestedThenShouldChangeTheIdentity(t *testing.T) {
	base := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")
	baseline := mustDigest(t, base)

	variants := map[string]func(*wire.ConsumerFilter){
		"a different codec":        func(f *wire.ConsumerFilter) { f.Codec = wire.CodecCBOR },
		"a different fault policy": func(f *wire.ConsumerFilter) { f.FaultPolicy = wire.FaultDrop },
		"a passing foreign policy": func(f *wire.ConsumerFilter) { f.ForeignPolicy = wire.PolicyPass },
		"a passing mismatch policy": func(f *wire.ConsumerFilter) {
			f.MismatchPolicy = wire.PolicyPass
		},
		"a different evaluator version": func(f *wire.ConsumerFilter) { f.EvaluatorVersion = 2 },
		"a negated expression": func(f *wire.ConsumerFilter) {
			f.Expr = wire.ExprNegate(f.Expr)
		},
	}
	for name, mutate := range variants {
		altered := base
		mutate(&altered)
		if mustDigest(t, altered).Bytes == nil {
			t.Fatalf("%s: no digest", name)
		}
		if string(mustDigest(t, altered).Bytes) == string(baseline.Bytes) {
			t.Errorf("%s must change the digest", name)
		}
	}

	// The schema stream is documented as leaving the digest unchanged only
	// because it is absent. Setting it does change the encoding.
	withStream := base
	stream := "orders"
	withStream.SchemaStream = &stream
	if string(mustDigest(t, withStream).Bytes) == string(baseline.Bytes) {
		t.Error("naming a schema stream changes the encoding and so the digest")
	}
}

// The field path text form is the canonical encoding the digest covers, so one
// path must have exactly one spelling.
func TestGivenFieldPathsWhenParsedThenShouldRoundTripTheirCanonicalForm(t *testing.T) {
	for _, text := range []string{
		"mode",
		"after.mode",
		"after.ground_stations[0]",
		"codes.200",
		"[0]",
		"[0].name",
		"a.b.c[12].d",
		`weird\.key`,
		`weird\[key\]`,
		`back\\slash`,
	} {
		path, err := wire.ParseFieldPath(text)
		if err != nil {
			t.Errorf("%q: parse: %v", text, err)
			continue
		}
		if got := path.String(); got != text {
			t.Errorf("%q: round trip rendered %q", text, got)
		}
	}
}

func TestGivenAnInvalidFieldPathWhenParsedThenShouldBeRefused(t *testing.T) {
	for name, text := range map[string]string{
		"empty":                "",
		"empty key":            "a..b",
		"trailing dot":         "a.",
		"unterminated bracket": "a[0",
		"unterminated escape":  `a\`,
		"bad escape":           `a\q`,
		"unescaped bracket":    "a]b",
		"non-canonical index":  "a[01]",
		"empty index":          "a[]",
		"index past 32 bits":   "a[4294967296]",
		"junk after a segment": "a[0]x",
	} {
		if _, err := wire.ParseFieldPath(text); err == nil {
			t.Errorf("%s: want a parse error for %q", name, text)
		}
	}
}

// A digit-only key stays a key: only brackets select an index. Getting this
// wrong would read codes.200 as an array element.
func TestGivenADigitKeyWhenParsedThenShouldStayAKey(t *testing.T) {
	path, err := wire.ParseFieldPath("codes.200")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	segments := path.Segments()
	if len(segments) != 2 {
		t.Fatalf("want two segments, got %d", len(segments))
	}
	if segments[1].Kind != wire.SegmentKey || segments[1].Key != "200" {
		t.Fatalf("want a key segment \"200\", got %+v", segments[1])
	}

	indexed, err := wire.ParseFieldPath("codes[200]")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if indexed.Segments()[1].Kind != wire.SegmentIndex {
		t.Fatal("brackets must select an index")
	}
}

// An unknown codec decodes rather than failing, so a filter a newer catalog
// saved still lists, but it must not validate or compile.
func TestGivenAnUnknownCodecWhenDecodedThenShouldListButNotValidate(t *testing.T) {
	filter := roundTripFixture[wire.ConsumerFilter](t, "filter_consumer_filter.bin")
	filter.Codec = "quantum"

	encoded, err := wire.Marshal(filter)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.ConsumerFilter
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("an unknown codec must still decode: %v", err)
	}
	if back.Codec != wire.CodecUnknown {
		t.Fatalf("want the unknown codec, got %q", back.Codec)
	}
	if back.Codec.IsNamed() {
		t.Error("the unknown codec is not a named one")
	}
}

// A schema codec needs writer-schema references and a non-schema codec must
// carry none, or a record could not be decoded against a known schema.
func TestGivenAFilterCodecWhenValidatedThenShouldAgreeWithItsSchemaRefs(t *testing.T) {
	expr := wire.ExprPredicate("a", wire.CmpEq, wire.StringTyped("b"))

	jsonFilter := wire.NewConsumerFilter(expr, wire.CodecJSON)
	if err := jsonFilter.Validate(); err != nil {
		t.Errorf("a JSON filter with no refs must validate: %v", err)
	}
	jsonFilter.SchemaRefs = []uint32{1}
	if err := jsonFilter.Validate(); err == nil {
		t.Error("a JSON filter must not carry schema references")
	}

	avro := wire.NewConsumerFilter(expr, wire.CodecAvro)
	if err := avro.Validate(); err == nil {
		t.Error("an Avro filter must name its writer schemas")
	}
	avro.SchemaRefs = []uint32{7}
	if err := avro.Validate(); err != nil {
		t.Errorf("an Avro filter with refs must validate: %v", err)
	}
}

func mustDigest(t *testing.T, filter wire.ConsumerFilter) wire.Digest32 {
	t.Helper()
	digest, err := filter.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return digest
}

// The server-native filtered read path: poll requests, pages, acknowledgments
// and the failure reply.
func TestGivenFilterReadFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("filter_poll_request.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilteredPollRequest](t, "filter_poll_request.bin")
		if request.Source.Topic == "" {
			t.Error("a poll names its source topic")
		}
	})
	t.Run("filter_poll_request_group_id.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilteredPollRequest](t, "filter_poll_request_group_id.bin")
		if request.Consumer.Kind != wire.ConsumerGroupID || !request.Consumer.IsGroup() {
			t.Errorf("want a numeric group selector, got %+v", request.Consumer)
		}
	})
	t.Run("filter_poll_request_group_auto.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilteredPollRequest](t, "filter_poll_request_group_auto.bin")
		// The automatic selector needs a server advertising group policy
		// reads, so a client must not send it otherwise.
		if request.Filter.Kind != wire.RefGroup {
			t.Errorf("want the group policy reference, got kind %d", request.Filter.Kind)
		}
	})
	t.Run("filter_reply_page.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_page.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.FilterPageOutcome {
			t.Fatalf("want a page outcome, got %+v", reply)
		}
		page := reply.Outcome.Page
		if err := page.Policy.Validate(); err != nil {
			t.Fatalf("the page policy must validate: %v", err)
		}
		if !page.Policy.Mode.IsFiltered() || page.Policy.Digest == nil {
			t.Error("a filtered page names the filter that ran")
		}
	})
	t.Run("filter_reply_page_unfiltered.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_page_unfiltered.bin")
		page := reply.Outcome.Page
		// Unfiltered is an explicit policy, never a fallback: no digest, and
		// only a group may run it.
		if page.Policy.Mode.IsFiltered() {
			t.Error("the unfiltered fixture runs unfiltered")
		}
		if page.Policy.Digest != nil {
			t.Error("an unfiltered policy carries no digest")
		}
		if page.Policy.GroupID == nil {
			t.Error("only a consumer group runs unfiltered")
		}
		if err := page.Policy.Validate(); err != nil {
			t.Fatalf("the unfiltered policy must validate: %v", err)
		}
	})
	t.Run("filter_ack.bin", func(t *testing.T) {
		roundTripFixture[wire.FilteredAck](t, "filter_ack.bin")
	})
	t.Run("filter_ack_unfiltered.bin", func(t *testing.T) {
		ack := roundTripFixture[wire.FilteredAck](t, "filter_ack_unfiltered.bin")
		if ack.Digest != nil {
			t.Error("an unfiltered acknowledgment carries no digest")
		}
	})
	t.Run("filter_reply_page_unevaluated.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_page_unevaluated.bin")
		page := reply.Outcome.Page
		// Under the pass fault policy a record the filter could not evaluate
		// is still returned, so the page must say why: a reader that saw a
		// bare offset could not tell a malformed body from one that outran
		// the decode bounds.
		if len(page.Unevaluated) == 0 {
			t.Fatal("the unevaluated fixture names the records it could not evaluate")
		}
		for index, fault := range page.Unevaluated {
			if fault.Reason == "" {
				t.Errorf("unevaluated record %d names no reason", index)
			}
		}
	})
	t.Run("filter_reply_acknowledged.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_acknowledged.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.FilterAcknowledged {
			t.Fatalf("want an acknowledgment, got %+v", reply)
		}
	})
	t.Run("filter_reply_error.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_error.bin")
		if reply.IsOk() {
			t.Fatalf("want a failure, got %+v", reply.Outcome)
		}
		// The stored code must agree with the reason's own projection.
		if reply.Err.Code != reply.Err.Reason.Code() {
			t.Errorf("the code %s disagrees with the reason %s (%s)",
				reply.Err.Code, reply.Err.Reason, reply.Err.Reason.Code())
		}
	})
	t.Run("filter_reply_revision_disabled.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_revision_disabled.bin")
		if reply.IsOk() || reply.Err.Reason != wire.ReasonRevisionDisabled {
			t.Fatalf("want a disabled revision, got %+v", reply)
		}
		// A paused revision is transient: pending records may still be
		// acknowledged, so the caller retries rather than giving up.
		if !reply.Err.Code.IsRetryable() {
			t.Error("a paused revision is retryable")
		}
	})
}

// A policy must name a filter exactly when it claims to be filtered, or a page
// could assert it filtered records while naming no filter.
func TestGivenAnAppliedPolicyWhenValidatedThenShouldMatchItsMode(t *testing.T) {
	digest := wire.NewDigest32([32]byte{1})
	group := uint64(7)

	if err := wire.FilteredPolicy(nil, digest).Validate(); err != nil {
		t.Errorf("a filtered policy with a digest must validate: %v", err)
	}
	if err := wire.UnfilteredPolicy(group, 3).Validate(); err != nil {
		t.Errorf("an unfiltered group policy must validate: %v", err)
	}

	noDigest := wire.AppliedPolicy{Mode: wire.ModeFiltered}
	if err := noDigest.Validate(); err == nil {
		t.Error("a filtered policy without a digest must be refused")
	}
	strayDigest := wire.AppliedPolicy{Mode: wire.ModeUnfiltered, GroupID: &group, Digest: &digest}
	if err := strayDigest.Validate(); err == nil {
		t.Error("an unfiltered policy with a digest must be refused")
	}
	loneConsumer := wire.AppliedPolicy{Mode: wire.ModeUnfiltered}
	if err := loneConsumer.Validate(); err == nil {
		t.Error("an independent consumer must not run unfiltered")
	}
	filterID := uint32(1)
	strayRef := wire.AppliedPolicy{Mode: wire.ModeUnfiltered, GroupID: &group, FilterID: &filterID}
	if err := strayRef.Validate(); err == nil {
		t.Error("an unfiltered policy must carry no filter reference")
	}
}

// A page hands the reader its resume point. When nothing was examined the
// original selector must be repeated, or the reader would silently skip the
// range it never scanned.
func TestGivenAPageWhenContinuedThenShouldResumeOrRepeatTheSelector(t *testing.T) {
	reply := roundTripFixture[wire.FilterReply](t, "filter_reply_page.bin")
	page := *reply.Outcome.Page
	original := wire.StartFromOffset(10)

	if page.NextScanOffset == nil {
		t.Skip("the corpus page examined nothing")
	}
	next := page.NextStart(original)
	if next.Kind != wire.StartContinue || next.Continuation == nil {
		t.Fatalf("want a continuation, got kind %d", next.Kind)
	}
	if next.Continuation.NextScanOffset != *page.NextScanOffset {
		t.Errorf("the continuation must carry the next scan offset")
	}
	// The continuation carries the generation, so a recreated source is
	// refused rather than applied to new records.
	if next.Continuation.Generation != page.Generation {
		t.Error("the continuation must carry the source generation")
	}

	examinedNothing := page
	examinedNothing.NextScanOffset = nil
	if repeated := examinedNothing.NextStart(original); repeated.Kind != original.Kind ||
		repeated.Offset != original.Offset {
		t.Errorf("an unexamined page must repeat the original selector, got %+v", repeated)
	}
}

// Every filter failure reason must classify, including one a newer server
// sends, since a client dispatches retry behaviour on the shared code.
func TestGivenFilterErrorReasonsWhenClassifiedThenShouldMapToSharedCodes(t *testing.T) {
	for reason, want := range map[wire.FilterErrorReason]wire.ResultCode{
		wire.ReasonInvalidRequest:     wire.ResultInvalidArgument,
		wire.ReasonUnsupported:        wire.ResultUnsupported,
		wire.ReasonVersionSkew:        wire.ResultVersionSkew,
		wire.ReasonNotFound:           wire.ResultNotFound,
		wire.ReasonConflict:           wire.ResultConflict,
		wire.ReasonSourceChanged:      wire.ResultStaleGeneration,
		wire.ReasonMembershipStale:    wire.ResultStale,
		wire.ReasonCatalogUnavailable: wire.ResultUnavailable,
		wire.ReasonRevisionDisabled:   wire.ResultUnavailable,
		wire.ReasonTooLarge:           wire.ResultTooLarge,
		wire.ReasonUnauthenticated:    wire.ResultUnauthenticated,
		wire.ReasonForbidden:          wire.ResultForbidden,
		wire.ReasonUnavailable:        wire.ResultUnavailable,
		wire.ReasonCapacityExhausted:  wire.ResultResourceLimit,
		wire.ReasonBackend:            wire.ResultBackend,
	} {
		if got := reason.Code(); got != want {
			t.Errorf("%s: want %s, got %s", reason, want, got)
		}
	}

	// A reason from a newer server decodes to the passthrough and still
	// classifies through its code.
	encoded, err := wire.Marshal(wire.FilterErrorReason("sunspots"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var reason wire.FilterErrorReason
	if err := wire.Unmarshal(encoded, &reason); err != nil {
		t.Fatalf("an unknown reason must decode: %v", err)
	}
	if reason != wire.ReasonUnknown || reason.Code() != wire.ResultBackend {
		t.Fatalf("want the passthrough reason classifying as backend, got %q/%s",
			reason, reason.Code())
	}

	if err := wire.CheckFilterVersion(wire.FilterOpVersion); err != nil {
		t.Errorf("the current version must pass: %v", err)
	}
	if err := wire.CheckFilterVersion(99); err == nil {
		t.Error("another version must be refused as a skew")
	}
}

// The consumer selector and the start selector both ride tagged forms, and a
// wrong reading would address the wrong consumer or the wrong offset.
func TestGivenFilterSelectorsWhenRoundTrippedThenShouldKeepTheirForm(t *testing.T) {
	for name, consumer := range map[string]wire.FilterConsumer{
		"independent": wire.IndependentConsumer("reader-1"),
		"named group": wire.NamedGroup("cdc"),
		"group id":    wire.GroupByID(42),
	} {
		encoded, err := wire.Marshal(consumer)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.FilterConsumer
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back != consumer {
			t.Errorf("%s: round trip changed the selector: %+v", name, back)
		}
	}
	if wire.IndependentConsumer("r").IsGroup() {
		t.Error("an independent consumer is not a group")
	}

	for name, start := range map[string]wire.FilteredStart{
		"next":      wire.StartAtNext(),
		"first":     wire.StartAtFirst(),
		"last":      wire.StartAtLast(),
		"offset":    wire.StartFromOffset(99),
		"timestamp": wire.StartFromTimestamp(1700),
	} {
		encoded, err := wire.Marshal(start)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.FilteredStart
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back.Kind != start.Kind || back.Offset != start.Offset ||
			back.Timestamp != start.Timestamp {
			t.Errorf("%s: round trip changed the start: %+v", name, back)
		}
	}
}

// A foreign record follows the filter's foreign policy rather than its fault
// policy, so the classification must be exact.
func TestGivenFaultReasonsWhenClassifiedThenShouldSeparateForeignFromFault(t *testing.T) {
	for _, foreign := range []wire.FaultReason{
		wire.FaultForeignCodec, wire.FaultMissingSchema,
		wire.FaultSchemaNotAllowed, wire.FaultSchemaMismatch,
	} {
		if !foreign.IsForeign() {
			t.Errorf("%s names another format", foreign)
		}
	}
	for _, fault := range []wire.FaultReason{
		wire.FaultMalformed, wire.FaultTooLarge,
		wire.FaultTooDeep, wire.FaultTypeMismatch,
	} {
		if fault.IsForeign() {
			t.Errorf("%s is a decode fault, not a foreign record", fault)
		}
	}
}

// A filtered wait holds until a partition has a record to read, so a reader
// waits on the server rather than polling it. The wait reads nothing itself:
// the reader then polls the partitions that came back ready.
func TestGivenFilteredWaitFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("filter_wait_request.bin", func(t *testing.T) {
		request := roundTripFixture[wire.FilteredWaitRequest](t, "filter_wait_request.bin")
		if err := request.Validate(); err != nil {
			t.Fatalf("the corpus wait must validate: %v", err)
		}
		if len(request.Partitions) == 0 {
			t.Error("a wait names the partitions it waits on")
		}
		// The wait id names the wait on its connection, which is what lets a
		// cancel end it early.
		if request.WaitID == 0 {
			t.Error("a wait names itself so a cancel can end it")
		}
		if request.MaxWaitMs > wire.MaxFilteredWaitMs {
			t.Errorf("the wait exceeds the cap of %d", wire.MaxFilteredWaitMs)
		}
	})
	t.Run("filter_reply_ready.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_ready.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.FilterReady {
			t.Fatalf("want a ready outcome, got %+v", reply)
		}
		if len(reply.Outcome.WaitResult.Ready) == 0 {
			t.Error("this fixture names a partition that became ready")
		}
	})
	t.Run("filter_wait_cancel.bin", func(t *testing.T) {
		cancel := roundTripFixture[wire.FilteredWaitCancel](t, "filter_wait_cancel.bin")
		if err := cancel.Validate(); err != nil {
			t.Fatalf("the corpus cancel must validate: %v", err)
		}
		if len(cancel.WaitIDs) == 0 {
			t.Error("a cancel names the waits it ends")
		}
	})
	t.Run("filter_reply_cancelled.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.FilterReply](t, "filter_reply_cancelled.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.FilterCancelled {
			t.Fatalf("want a cancelled outcome, got %+v", reply)
		}
		// A wait that had already answered, or was never held, is left out,
		// so the reply is what the cancel actually ended.
		if len(reply.Outcome.Cancelled.Cancelled) == 0 {
			t.Error("this fixture ended a wait")
		}
	})
}

// A wait must name each partition once and stay inside the announced bound, or
// a reader could hold the server on a request it never meant to make.
func TestGivenAMalformedFilteredWaitWhenValidatedThenShouldBeRefused(t *testing.T) {
	sound := roundTripFixture[wire.FilteredWaitRequest](t, "filter_wait_request.bin")

	repeated := sound
	repeated.Partitions = []wire.FilteredWaitPartition{
		{PartitionID: 3, Offset: 0}, {PartitionID: 3, Offset: 1},
	}
	if err := repeated.Validate(); err == nil {
		t.Error("a wait naming one partition twice must be refused")
	}

	empty := sound
	empty.Partitions = nil
	if err := empty.Validate(); err == nil {
		t.Error("a wait naming no partition must be refused")
	}

	tooLong := sound
	tooLong.MaxWaitMs = wire.MaxFilteredWaitMs + 1
	if err := tooLong.Validate(); err == nil {
		t.Error("a wait past the cap must be refused")
	}

	repeatedCancel := wire.FilteredWaitCancel{V: wire.FilterOpVersion, WaitIDs: []uint64{4, 4}}
	if err := repeatedCancel.Validate(); err == nil {
		t.Error("a cancel naming one wait twice must be refused")
	}
}
