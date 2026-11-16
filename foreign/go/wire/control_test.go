package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Every control fixture in the corpus. A control record is ordered and
// replayed, so a command that decoded as the wrong variant would reconfigure
// the deployment differently on every replica.
func TestGivenControlFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	cases := map[string]wire.ControlCommandKind{
		"control_register_projection.bin":                      wire.CommandRegisterProjection,
		"control_apply_binding.bin":                            wire.CommandApplyBinding,
		"control_remove_binding.bin":                           wire.CommandRemoveBinding,
		"control_stamped_remove_binding.bin":                   wire.CommandRemoveBinding,
		"control_register_schema_avro.bin":                     wire.CommandRegisterSchema,
		"control_register_schema_json.bin":                     wire.CommandRegisterSchema,
		"control_register_schema_protobuf.bin":                 wire.CommandRegisterSchema,
		"control_drop_schema.bin":                              wire.CommandDropSchema,
		"control_register_session_source_all.bin":              wire.CommandRegisterSessionSource,
		"control_register_session_source_named.bin":            wire.CommandRegisterSessionSource,
		"control_remove_session_source.bin":                    wire.CommandRemoveSessionSource,
		"control_filter_catalog.bin":                           wire.CommandFilterCatalog,
		"control_accept_gap.bin":                               wire.CommandAcceptGap,
		"control_skip_record.bin":                              wire.CommandSkipRecord,
		"control_source_retained.bin":                          wire.CommandSourceRetained,
		"control_allocate_schema.bin":                          wire.CommandAllocateSchema,
		"control_apply_binding_policies.bin":                   wire.CommandApplyBinding,
		"control_generation_remove_binding.bin":                wire.CommandRemoveBinding,
		"control_register_projection_keyed.bin":                wire.CommandRegisterProjection,
		"control_register_projection_keyed_without_delete.bin": wire.CommandRegisterProjection,
		"control_register_projection_record.bin":               wire.CommandRegisterProjection,
		"control_stamped_caller.bin":                           wire.CommandDropSchema,
	}

	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			envelope := roundTripFixture[wire.ControlEnvelope](t, name)
			if envelope.Command.Kind != want {
				t.Fatalf("want command kind %d, got %d", want, envelope.Command.Kind)
			}
			if envelope.V != wire.ControlOpVersion {
				t.Errorf("want version %d, got %d", wire.ControlOpVersion, envelope.V)
			}
		})
	}
}

// The backend seals the whole envelope including its stamp, so a stamped
// record carries the identity that binds it to one deployment, actor and
// stream generation.
func TestGivenAStampedControlRecordWhenDecodedThenShouldCarryItsIdentity(t *testing.T) {
	envelope := roundTripFixture[wire.ControlEnvelope](t, "control_stamped_remove_binding.bin")

	if envelope.Stamp == nil {
		t.Fatal("the stamped fixture carries a stamp")
	}
	if envelope.Stamp.OperationID.IsZero() {
		t.Error("a stamp names the logical control operation, so a retry applies once")
	}

	// An unstamped record must leave the field absent, since a client never
	// sets it.
	plain := roundTripFixture[wire.ControlEnvelope](t, "control_remove_binding.bin")
	if plain.Stamp != nil {
		t.Error("a client-written record carries no stamp")
	}
}

// The three schema sources decode to their own kinds, and each reports the
// codec it applies to.
func TestGivenSchemaRegistrationsWhenDecodedThenShouldNameTheirCodec(t *testing.T) {
	for name, want := range map[string]struct {
		kind  wire.SchemaSourceKind
		codec wire.ContentType
	}{
		"control_register_schema_avro.bin":     {wire.SchemaAvro, wire.ContentAvro},
		"control_register_schema_json.bin":     {wire.SchemaJSON, wire.ContentJSON},
		"control_register_schema_protobuf.bin": {wire.SchemaProtobuf, wire.ContentProtobuf},
	} {
		t.Run(name, func(t *testing.T) {
			envelope := roundTripFixture[wire.ControlEnvelope](t, name)
			definition := envelope.Command.Schema
			if definition == nil {
				t.Fatal("a schema registration carries its definition")
			}
			if definition.Source.Kind != want.kind {
				t.Errorf("want source %q, got %q", want.kind, definition.Source.Kind)
			}
			if got := definition.ContentType(); got != want.codec {
				t.Errorf("want codec %q, got %q", want.codec, got)
			}
		})
	}
}

// A retention policy or schema source a newer server named degrades to the
// passthrough rather than failing the reply, so an older client keeps reading
// what it cannot fully interpret. It must not re-apply one: re-serializing
// would lose the original kind and its fields.
func TestGivenUnknownControlVocabularyWhenDecodedThenShouldDegradeAndRefuseReapply(t *testing.T) {
	futureRetention, err := wire.Marshal(struct {
		Kind string `cbor:"kind"`
		Days uint64 `cbor:"days"`
	}{Kind: "keep_for_days", Days: 30})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var policy wire.RetentionPolicy
	if err := wire.Unmarshal(futureRetention, &policy); err != nil {
		t.Fatalf("an unknown retention must decode: %v", err)
	}
	if policy.Kind != wire.RetentionUnknown {
		t.Fatalf("want the passthrough policy, got %q", policy.Kind)
	}
	if _, err := wire.Marshal(policy); err == nil {
		t.Error("an unknown retention must refuse to re-encode, since its fields are lost")
	}

	futureSchema, err := wire.Marshal(struct {
		Kind   string `cbor:"kind"`
		Scheme string `cbor:"scheme"`
	}{Kind: "thrift", Scheme: "x"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var source wire.SchemaSource
	if err := wire.Unmarshal(futureSchema, &source); err != nil {
		t.Fatalf("an unknown schema source must decode: %v", err)
	}
	if source.Kind != wire.SchemaUnknown {
		t.Fatalf("want the passthrough source, got %q", source.Kind)
	}
	if _, err := wire.Marshal(source); err == nil {
		t.Error("an unknown schema source must refuse to re-register")
	}
	// It still reports a usable codec for best-effort decoding.
	if (wire.SchemaDef{Source: source}).ContentType() != wire.ContentAny {
		t.Error("an unknown source decodes best-effort")
	}
}

// A projection kind a newer server named passes through as its code rather
// than failing the listing.
func TestGivenAnUnknownProjectionKindWhenDecodedThenShouldPassItsCodeThrough(t *testing.T) {
	encoded, err := wire.Marshal(wire.ProjectionKind(42))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var kind wire.ProjectionKind
	if err := wire.Unmarshal(encoded, &kind); err != nil {
		t.Fatalf("an unknown kind must decode: %v", err)
	}
	if kind.Code() != 42 || kind.IsRecognized() {
		t.Fatalf("want an unrecognized code 42, got %d", kind.Code())
	}
	if wire.ProjectionRow.Code() != 0 || wire.ProjectionGraph.Code() != 1 {
		t.Error("the pinned codes must not move")
	}
	if !wire.ProjectionRow.IsRow() || wire.ProjectionGraph.IsRow() {
		t.Error("only the row kind is the default")
	}
}

// A skip and a gap acceptance are both scoped to one topic incarnation, so a
// recreated topic never inherits either.
func TestGivenASkipOrGapWhenMatchedThenShouldBeScopedToOneIncarnation(t *testing.T) {
	skipEnvelope := roundTripFixture[wire.ControlEnvelope](t, "control_skip_record.bin")
	skip := skipEnvelope.Command.Skip
	if skip == nil {
		t.Fatal("the skip fixture carries a record")
	}
	if !skip.Covers(skip.Source.Stream, skip.Source.Topic, skip.PartitionID,
		skip.TopicCreatedAtMicros, skip.Offset, skip.Target) {
		t.Error("a skip must cover its own record")
	}
	if skip.Covers(skip.Source.Stream, skip.Source.Topic, skip.PartitionID,
		skip.TopicCreatedAtMicros+1, skip.Offset, skip.Target) {
		t.Error("a recreated topic must not inherit a skip")
	}

	gapEnvelope := roundTripFixture[wire.ControlEnvelope](t, "control_accept_gap.bin")
	gap := gapEnvelope.Command.Gap
	if gap == nil {
		t.Fatal("the gap fixture carries an acceptance")
	}
	if !gap.Covers(gap.TopicCreatedAtMicros, gap.FromOffset, gap.ToOffset) {
		t.Error("an acceptance must cover its own range")
	}
	if gap.Covers(gap.TopicCreatedAtMicros+1, gap.FromOffset, gap.ToOffset) {
		t.Error("a recreated topic needs its own acceptance")
	}
	if gap.Covers(gap.TopicCreatedAtMicros, gap.FromOffset-1, gap.ToOffset) {
		t.Error("a resume before the gap is not covered")
	}
}

// A keyed projection replaces the row for a key instead of appending, so the
// key and its optional delete header are what a reader needs to fold it the
// same way everywhere.
func TestGivenAKeyedProjectionWhenDecodedThenShouldNameItsKey(t *testing.T) {
	withDelete := roundTripFixture[wire.ControlEnvelope](t,
		"control_register_projection_keyed.bin")
	projection := withDelete.Command.Projection
	if projection == nil {
		t.Fatal("a registration carries its projection")
	}
	if projection.History.Kind != wire.HistoryKeyed {
		t.Fatalf("want a keyed history, got %q", projection.History.Kind)
	}
	if projection.History.Key.Kind == "" {
		t.Error("a keyed history names where it reads its key")
	}
	if projection.History.DeleteHeader == nil {
		t.Error("this fixture deletes a key on a header")
	}

	// Without a delete header the view never deletes, which is a different
	// fold, so the absent field must stay absent.
	withoutDelete := roundTripFixture[wire.ControlEnvelope](t,
		"control_register_projection_keyed_without_delete.bin")
	if withoutDelete.Command.Projection.History.DeleteHeader != nil {
		t.Error("this fixture never deletes, so it names no header")
	}

	// An append projection is the default, so its history stays off the wire.
	appended := roundTripFixture[wire.ControlEnvelope](t,
		"control_register_projection.bin")
	if !appended.Command.Projection.History.IsAppend() {
		t.Error("the default history appends every record")
	}
}

// A whole-record projection materializes the body, headers and position with
// no extraction, so its kind is what tells a worker to skip the plan.
func TestGivenAWholeRecordProjectionWhenDecodedThenShouldNameItsKind(t *testing.T) {
	envelope := roundTripFixture[wire.ControlEnvelope](t,
		"control_register_projection_record.bin")
	if envelope.Command.Projection.Kind != wire.ProjectionRecord {
		t.Errorf("want the record kind, got %d", envelope.Command.Projection.Kind)
	}
}

// A binding's two refusal policies each ride the wire only when they differ
// from blocking, so a deployment that never set one encodes as it always did.
func TestGivenBindingPoliciesWhenDecodedThenShouldKeepTheirChoice(t *testing.T) {
	configured := roundTripFixture[wire.ControlEnvelope](t,
		"control_apply_binding_policies.bin")
	binding := configured.Command.Binding
	if binding == nil {
		t.Fatal("an apply carries its binding")
	}
	if binding.InvalidRecords.IsBlock() && binding.Oversize.IsBlock() {
		t.Error("this fixture sets at least one policy away from blocking")
	}

	plain := roundTripFixture[wire.ControlEnvelope](t, "control_apply_binding.bin")
	if !plain.Command.Binding.InvalidRecords.IsBlock() ||
		!plain.Command.Binding.Oversize.IsBlock() {
		t.Error("an unset policy blocks, which is what the wire omits")
	}
}

// A generation-scoped removal only takes effect for the incarnation it names,
// so a binding re-applied for a newer stream survives an older removal.
func TestGivenAGenerationScopedRemovalWhenDecodedThenShouldNameItsIncarnation(t *testing.T) {
	scoped := roundTripFixture[wire.ControlEnvelope](t,
		"control_generation_remove_binding.bin")
	if scoped.Command.Generation == nil {
		t.Fatal("the scoped fixture names the incarnation it removes from")
	}
	if scoped.Command.Generation.StreamID == 0 {
		t.Error("a stream generation names its stream")
	}

	plain := roundTripFixture[wire.ControlEnvelope](t, "control_remove_binding.bin")
	if plain.Command.Generation != nil {
		t.Error("an unscoped removal applies whatever the incarnation")
	}
}

// A schema allocation leaves the id to the control fold, so the request names
// only the source. The operation identity keeps the chosen id, which is what
// makes a retry return the first one rather than allocating a second.
func TestGivenASchemaAllocationWhenDecodedThenShouldNameOnlyItsSource(t *testing.T) {
	envelope := roundTripFixture[wire.ControlEnvelope](t, "control_allocate_schema.bin")
	allocation := envelope.Command.Allocation
	if allocation == nil {
		t.Fatal("an allocation carries its schema source")
	}
	if allocation.Source.Kind == "" {
		t.Error("an allocation names the source it registers")
	}
}

// A retention floor says no record below it can be read again, so the skips
// and accepted gaps below it leave the registry.
func TestGivenARetentionFloorWhenDecodedThenShouldNameItsPartition(t *testing.T) {
	envelope := roundTripFixture[wire.ControlEnvelope](t, "control_source_retained.bin")
	retention := envelope.Command.Retention
	if retention == nil {
		t.Fatal("a retention floor carries its source")
	}
	if retention.TopicCreatedAtMicros == 0 {
		t.Error("a floor belongs to one topic incarnation")
	}
}

// The stamp names the client that asked, never the backend itself, so a record
// the backend issued on its own leaves the caller absent rather than reading
// as the root user.
func TestGivenAStampedCallerWhenDecodedThenShouldNameTheClientThatAsked(t *testing.T) {
	stamped := roundTripFixture[wire.ControlEnvelope](t, "control_stamped_caller.bin")
	if stamped.Stamp == nil || stamped.Stamp.Caller == nil {
		t.Fatal("the caller fixture names who asked")
	}

	issued := roundTripFixture[wire.ControlEnvelope](t, "control_stamped_remove_binding.bin")
	if issued.Stamp != nil && issued.Stamp.Caller != nil {
		t.Error("a record the backend issued names no caller")
	}
}
