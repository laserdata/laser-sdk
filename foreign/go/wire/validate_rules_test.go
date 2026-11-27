package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A binding that keeps body references needs mirror-log retention, because a
// referenced body lives only in the source log: a row that outlived its
// record would point at nothing.
func TestGivenAReferencingBindingWhenValidatedThenShouldRequireMirrorLogRetention(t *testing.T) {
	binding := func(retention *wire.RetentionPolicy) wire.ProjectionBinding {
		return wire.ProjectionBinding{
			Source:    wire.NewSourceSelector("fleet", "readings"),
			Index:     "readings_rows",
			Oversize:  wire.OversizeReference,
			Retention: retention,
		}
	}
	mirror := wire.MirrorLogRetention()
	if err := binding(&mirror).Validate(); err != nil {
		t.Errorf("mirror-log retention keeps the record alive: %v", err)
	}
	keep := wire.KeepRetention()
	if err := binding(&keep).Validate(); err == nil {
		t.Error("a referencing binding outliving its records must be refused")
	}
}

// A policy this build decoded as unknown cannot be re-applied: writing it back
// would lose the meaning that was already lost on the way in.
func TestGivenAnUnknownPolicyWhenReappliedThenShouldBeRefused(t *testing.T) {
	for name, binding := range map[string]wire.ProjectionBinding{
		"an unknown invalid-record policy": {
			Source: wire.NewSourceSelector("fleet", "readings"), Index: "rows",
			InvalidRecords: wire.InvalidRecordUnknown,
		},
		"an unknown oversize policy": {
			Source: wire.NewSourceSelector("fleet", "readings"), Index: "rows",
			Oversize: wire.OversizeUnknown,
		},
	} {
		if err := binding.Validate(); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

// A whole-record projection materializes the record as it stands, so an
// extraction plan beside it would describe a second, different row.
func TestGivenAWholeRecordProjectionWhenValidatedThenShouldDeclareNoExtraction(t *testing.T) {
	record := wire.Projection{
		ID: "readings.v1", Name: "readings", Version: 1,
		Kind: wire.ProjectionRecord, ContentType: wire.ContentJSON,
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("a bare record projection validates: %v", err)
	}

	extracting := record
	extracting.Extraction = wire.IndexSchema{
		Fields: []wire.IndexField{wire.NewIndexField("host", "/host")},
	}
	if err := extracting.Validate(); err == nil {
		t.Error("a record projection declaring fields must be refused")
	}

	// A graph has nodes and edges rather than rows, so no row exists for a
	// key to replace.
	graph := record
	graph.Kind = wire.ProjectionGraph
	graph.History = wire.KeyedHistory(wire.HeaderProjectionKey("agdx.key"), nil)
	if err := graph.Validate(); err == nil {
		t.Error("a keyed graph projection must be refused")
	}

	// An index field becomes a column, so it follows the identifier rule.
	badField := wire.Projection{
		ID: "readings.v1", Name: "readings", Version: 1, ContentType: wire.ContentJSON,
		Extraction: wire.IndexSchema{
			Fields: []wire.IndexField{wire.NewIndexField("2host", "/host")},
		},
	}
	if err := badField.Validate(); err == nil {
		t.Error("an index field name starting with a digit must be refused")
	}
}

// A lease lifetime is bounded on both sides by the contract: a floor so a
// grant survives a round trip plus the holder's renewal cadence, a ceiling so
// a crashed holder's ownership always expires.
func TestGivenALeaseRequestWhenValidatedThenShouldBoundItsLifetime(t *testing.T) {
	lease := func(ttl uint64) wire.KvLease {
		return wire.KvLease{
			V: wire.KvLeaseOpVersion, Namespace: "coordination",
			Key: []byte("writer"), LeaseTTLMicros: ttl, HolderID: "process-7",
		}
	}
	if err := lease(wire.MinLeaseTTLMicros).Validate(); err != nil {
		t.Errorf("the shortest supported lease validates: %v", err)
	}
	for name, ttl := range map[string]uint64{
		"below the floor":   wire.MinLeaseTTLMicros - 1,
		"above the ceiling": wire.MaxLeaseTTLMicros + 1,
	} {
		if err := lease(ttl).Validate(); err == nil {
			t.Errorf("a lease %s must be refused", name)
		}
	}

	noHolder := lease(wire.MinLeaseTTLMicros)
	noHolder.HolderID = ""
	if err := noHolder.Validate(); err == nil {
		t.Error("a lease with no holder identity must be refused")
	}
}

// An acknowledgment settles a specific page, so it must name the same policy,
// group and partition that page ran under.
func TestGivenAMismatchedAcknowledgmentWhenValidatedThenShouldBeRefused(t *testing.T) {
	digest := wire.NewDigest32([32]byte{7})
	groupID := uint64(3)
	sound := wire.FilteredAck{
		V:           wire.FilterOpVersion,
		Source:      wire.FilterSource{Stream: "orbit", Topic: "changes"},
		PartitionID: 1,
		Consumer:    wire.NamedGroup("cdc"),
		Generation:  wire.SourceGeneration{PartitionID: 1},
		Digest:      &digest,
		Mode:        wire.ModeFiltered,
		GroupID:     &groupID,
	}
	if err := sound.Validate(); err != nil {
		t.Fatalf("a consistent acknowledgment validates: %v", err)
	}

	// A filtered page ran a specific filter and must name it, so a later
	// acknowledgment matches against the same one.
	noDigest := sound
	noDigest.Digest = nil
	if err := noDigest.Validate(); err == nil {
		t.Error("a filtered acknowledgment naming no digest must be refused")
	}

	// An unfiltered page ran none, so a digest would name a filter that never
	// executed.
	unfiltered := sound
	unfiltered.Mode = wire.ModeUnfiltered
	if err := unfiltered.Validate(); err == nil {
		t.Error("an unfiltered acknowledgment carrying a digest must be refused")
	}

	// The stored progress is keyed by the group identity, so an
	// acknowledgment without it settles against nothing.
	noGroup := sound
	noGroup.GroupID = nil
	if err := noGroup.Validate(); err == nil {
		t.Error("a group acknowledgment dropping its identity must be refused")
	}

	elsewhere := sound
	elsewhere.Generation = wire.SourceGeneration{PartitionID: 2}
	if err := elsewhere.Validate(); err == nil {
		t.Error("an acknowledgment naming another partition must be refused")
	}
}

// A preview joins no consumer group, so it cannot resolve a reference that
// only a group's binding gives meaning to.
func TestGivenAGroupReferenceOutsideAGroupWhenValidatedThenShouldBeRefused(t *testing.T) {
	preview := wire.FilterPreviewRequest{
		V:           wire.FilterOpVersion,
		Source:      wire.FilterSource{Stream: "orbit", Topic: "changes"},
		Filter:      wire.BoundRevision(),
		MaxExamined: 100,
		MaxRecords:  10,
	}
	if err := preview.Validate(); err == nil {
		t.Error("a preview naming a bound filter must be refused")
	}

	test := wire.FilterTestRequest{V: wire.FilterOpVersion, Filter: wire.BoundRevision()}
	if err := test.Validate(); err == nil {
		t.Error("a sample test naming a bound filter must be refused")
	}
}

// A ready announcement that lists backends must hold a ready one, or a client
// routes work to a deployment that serves none.
func TestGivenAReadyAnnounceWithNoReadyBackendWhenValidatedThenShouldBeRefused(t *testing.T) {
	descriptor := func(id byte, ready bool) wire.BackendDescriptor {
		backend := wire.NewBackendDescriptor(
			wire.BackendResourceID{ID: wire.IDFromBytes([16]byte{15: id})},
			wire.BackendOperational, "embedded",
			wire.BackendImplementation{Kind: "embedded", Version: "1"}, 1, 1)
		if ready {
			backend.Readiness = wire.ReadyBackend(1)
		}
		return backend
	}

	none := wire.BackendAnnounce{Ready: true, Backends: []wire.BackendDescriptor{
		descriptor(1, false),
	}}
	if err := none.Validate(); err == nil {
		t.Error("a ready announce with no ready backend must be refused")
	}

	one := wire.BackendAnnounce{Ready: true, Backends: []wire.BackendDescriptor{
		descriptor(1, false), descriptor(2, true),
	}}
	if err := one.Validate(); err != nil {
		t.Errorf("an announce holding a ready backend validates: %v", err)
	}

	// The streaming server relays the set verbatim, so a repeated id would
	// leave two descriptors claiming one backend.
	repeated := wire.BackendAnnounce{Ready: true, Backends: []wire.BackendDescriptor{
		descriptor(1, true), descriptor(1, true),
	}}
	if err := repeated.Validate(); err == nil {
		t.Error("a repeated resource id must be refused")
	}
}

// A reply must not claim it met a freshness level it did not, or a caller
// trusts a read that never met its level.
func TestGivenAWeakerDeliveredConsistencyWhenValidatedThenShouldBeRefused(t *testing.T) {
	context := func(requested, delivered wire.Consistency) wire.QueryContext {
		return wire.QueryContext{
			ExecutionID: wire.QueryExecutionID{ID: wire.IDFromBytes([16]byte{15: 9})},
			Engine:      wire.QueryEngine{Name: "embedded", Version: "1.0"},
			ResolvedTarget: wire.ResolvedQueryTarget{
				Kind: wire.ResolvedOperational,
			},
			RequestedConsistency: requested,
			DeliveredConsistency: delivered,
		}
	}
	weaker := context(wire.ConsistencyReadYourWrites, wire.ConsistencyEventual)
	if err := weaker.Validate(); err == nil {
		t.Error("a weaker delivered consistency must be refused")
	}
}
