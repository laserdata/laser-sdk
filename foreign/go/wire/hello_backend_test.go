package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The backend's capability announcement. The streaming server relays this
// verbatim, so a descriptor that decoded wrongly would advertise a surface the
// backend does not serve.
func TestGivenBackendAnnounceFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("backend_announce.bin", func(t *testing.T) {
		announce := roundTripFixture[wire.BackendAnnounce](t, "backend_announce.bin")
		if !announce.IsReady() {
			t.Error("the fixture announces a ready backend")
		}
		if len(announce.Backends) != 2 {
			t.Fatalf("want two backends, got %d", len(announce.Backends))
		}

		embedded := announce.Backends[0]
		if embedded.Mode != wire.BackendOperational {
			t.Errorf("want an operational backend, got %q", embedded.Mode)
		}
		if embedded.Query == nil {
			t.Fatal("the fixture backend serves queries")
		}
		if !embedded.Query.RawSQL || !embedded.Query.TextSearch {
			t.Error("the fixture query surface serves raw SQL and text search")
		}
		if len(embedded.Query.Aggregates) == 0 {
			t.Error("the fixture backend names the aggregates it computes")
		}
		if embedded.Limits.MaxQueryRows == 0 {
			t.Error("a backend announces the bounds it enforces")
		}

		// Both backends are operational since the lakehouse mode was retired.
		// They differ by engine, which a console shows and a client ignores.
		warehouse := announce.Backends[1]
		if warehouse.Mode != wire.BackendOperational {
			t.Errorf("want an operational backend, got %q", warehouse.Mode)
		}
		if warehouse.Implementation.Kind == embedded.Implementation.Kind {
			t.Error("the fixture announces two distinct engines")
		}

		for _, backend := range announce.Backends {
			if err := backend.Validate(); err != nil {
				t.Errorf("the fixture backend must validate: %v", err)
			}
		}
	})

	t.Run("backend_announce_unavailable.bin", func(t *testing.T) {
		announce := roundTripFixture[wire.BackendAnnounce](t,
			"backend_announce_unavailable.bin")
		// An unready backend must say why, or a client cannot tell whether to
		// wait or to give up.
		for _, backend := range announce.Backends {
			if backend.Readiness.Ready {
				continue
			}
			if len(backend.Readiness.Reasons) == 0 {
				t.Error("an unready backend names a reason")
			}
		}
	})

	t.Run("backend_announce_topology.bin", func(t *testing.T) {
		announce := roundTripFixture[wire.BackendAnnounce](t,
			"backend_announce_topology.bin")
		if announce.Topology == nil {
			t.Error("the fixture advertises a topology")
		}
	})
}

// A ready announce omits the flag, so one written before the flag existed
// still reads as ready rather than unavailable.
func TestGivenAnAnnounceWithoutAReadyFlagWhenDecodedThenShouldReadAsReady(t *testing.T) {
	partial, err := wire.Marshal(struct {
		Versions wire.OpVersions `cbor:"versions"`
	}{Versions: wire.OpVersions{Query: 1}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var announce wire.BackendAnnounce
	if err := wire.Unmarshal(partial, &announce); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !announce.IsReady() {
		t.Fatal("an announce with no ready flag is ready")
	}

	// A ready announce re-encodes without the flag.
	encoded, err := wire.Marshal(announce)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != string(partial) {
		t.Errorf("a ready announce must not emit the flag, got %x", encoded)
	}

	// An unready one does emit it, and reads back unready.
	announce.Ready = false
	unready, err := wire.Marshal(announce)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.BackendAnnounce
	if err := wire.Unmarshal(unready, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.IsReady() {
		t.Error("an explicit false must read as unready")
	}
}

// A client refuses to run a filter the server would evaluate differently,
// which is what keeps a locally previewed filter and a server-side one
// selecting the same records.
func TestGivenAFilterAnnounceWhenComparedThenShouldAgreeOnlyOnAnExactMatch(t *testing.T) {
	served := wire.ServedFilterAnnounce()
	if served.EvaluatorVersion != wire.FilterEvaluatorVersion {
		t.Errorf("this build serves evaluator %d, announced %d",
			wire.FilterEvaluatorVersion, served.EvaluatorVersion)
	}
	for _, codec := range []wire.FilterCodec{
		wire.CodecJSON, wire.CodecCBOR, wire.CodecAvro,
		wire.CodecProtobuf, wire.CodecHeadersOnly,
	} {
		if !served.Evaluates(wire.FilterEvaluatorVersion, codec) {
			t.Errorf("this build evaluates %q", codec)
		}
	}
	// Another evaluator version is not interchangeable, whatever the codec.
	if served.Evaluates(wire.FilterEvaluatorVersion+1, wire.CodecJSON) {
		t.Error("a different evaluator version must not be accepted")
	}
	// A codec the server did not announce is not served.
	narrow := wire.FilterAnnounce{
		EvaluatorVersion: wire.FilterEvaluatorVersion,
		Codecs:           []wire.FilterCodec{wire.CodecJSON},
	}
	if narrow.Evaluates(wire.FilterEvaluatorVersion, wire.CodecCBOR) {
		t.Error("an unannounced codec must not be accepted")
	}
}

// Readiness and its reasons must agree, or a client cannot tell whether it may
// route work to the backend.
func TestGivenBackendReadinessWhenValidatedThenShouldRequireAgreement(t *testing.T) {
	if err := wire.ReadyBackend(1_000).Validate(); err != nil {
		t.Errorf("a ready backend validates: %v", err)
	}
	if err := wire.UnreadyBackend(wire.ReadinessCatalogUnavailable).Validate(); err != nil {
		t.Errorf("an unready backend with a reason validates: %v", err)
	}

	readyWithReasons := wire.ReadyBackend(1_000)
	readyWithReasons.Reasons = []wire.BackendReadinessReason{
		{Code: wire.ReadinessProbeFailed},
	}
	if err := readyWithReasons.Validate(); err == nil {
		t.Error("a ready backend must not carry failure reasons")
	}

	readyWithoutTime := wire.ReadyBackend(0)
	if err := readyWithoutTime.Validate(); err == nil {
		t.Error("a ready backend must name when it was observed")
	}

	silent := wire.BackendReadiness{}
	if err := silent.Validate(); err == nil {
		t.Error("an unready backend must name a reason")
	}
}

// A query surface must announce usable coverage, and must not announce the
// same capability twice.
func TestGivenQueryCapabilitiesWhenValidatedThenShouldRequireUsableCoverage(t *testing.T) {
	complete := wire.QueryCapabilities{
		Dialects:     []wire.SqlDialect{wire.DialectPostgres},
		Consistency:  []wire.Consistency{wire.ConsistencyEventual},
		LogicalTypes: []wire.LogicalTypeKind{wire.KindLong},
		Aggregates:   []wire.AggFunc{wire.AggCount},
	}
	if err := complete.Validate(); err != nil {
		t.Errorf("complete coverage validates: %v", err)
	}

	for name, broken := range map[string]func(*wire.QueryCapabilities){
		"no dialect":      func(c *wire.QueryCapabilities) { c.Dialects = nil },
		"no consistency":  func(c *wire.QueryCapabilities) { c.Consistency = nil },
		"no logical type": func(c *wire.QueryCapabilities) { c.LogicalTypes = nil },
		"a repeated dialect": func(c *wire.QueryCapabilities) {
			c.Dialects = []wire.SqlDialect{wire.DialectPostgres, wire.DialectPostgres}
		},
		"a repeated aggregate": func(c *wire.QueryCapabilities) {
			c.Aggregates = []wire.AggFunc{wire.AggCount, wire.AggCount}
		},
	} {
		capabilities := complete
		broken(&capabilities)
		if err := capabilities.Validate(); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}
