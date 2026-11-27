package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A partial topology announcement from an older peer must decode every missing
// field to its real topic name. A zero value would address nothing, so a client
// would silently publish to "" instead of the ops topic.
func TestGivenAPartialTopologyWhenDecodedThenShouldFillRealDefaults(t *testing.T) {
	partial, err := wire.Marshal(struct {
		OpsStream string `cbor:"ops_stream"`
	}{OpsStream: "custom-ops"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var topology wire.WireTopology
	if err := wire.Unmarshal(partial, &topology); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if topology.OpsStream != "custom-ops" {
		t.Errorf("the named field must win, got %q", topology.OpsStream)
	}
	for name, pair := range map[string][2]string{
		"control": {topology.ControlTopic, wire.ControlTopic},
		"dlq":     {topology.DLQTopic, wire.DLQTopic},
		"changes": {topology.ChangesTopic, wire.ChangesTopic},
		"kv":      {topology.KvMutationsTopic, wire.DefaultKvMutationsTopic},
		"fork":    {topology.ForkMutationsTopic, wire.DefaultForkMutationsTopic},
		"graph":   {topology.GraphMutationsTopic, wire.DefaultGraphMutationsTopic},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: want the default %q, got %q", name, pair[1], pair[0])
		}
	}
}

func TestGivenTheDefaultTopologyWhenBuiltThenShouldMatchTodaysConstants(t *testing.T) {
	topology := wire.DefaultWireTopology()
	if topology.OpsStream != wire.OpsStream || topology.ControlTopic != wire.ControlTopic {
		t.Fatalf("the defaults must be today's constants: %+v", topology)
	}
}

// A batch amortizes the round trip and nothing else, so its bounds are checked
// and a nested batch is refused.
func TestGivenABatchWhenValidatedThenShouldBoundItAndRefuseNesting(t *testing.T) {
	ok := wire.BatchRequest{V: wire.BatchOpVersion, Ops: []wire.BatchItem{
		{Code: wire.AgdxKvGetCode, Payload: []byte{1}},
	}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("a plain batch must validate: %v", err)
	}

	nested := wire.BatchRequest{V: wire.BatchOpVersion, Ops: []wire.BatchItem{
		{Code: wire.AgdxBatchCode},
	}}
	if err := nested.Validate(); err == nil {
		t.Error("a nested batch must be refused")
	}

	oversized := wire.BatchRequest{V: wire.BatchOpVersion,
		Ops: make([]wire.BatchItem, wire.MaxBatchOps+1)}
	if err := oversized.Validate(); err == nil {
		t.Error("a batch past the cap must be refused")
	}

	if err := (wire.BatchRequest{V: 99}).Validate(); err == nil {
		t.Error("an unsupported batch version must be refused")
	}
}

// Every memory record variant must survive the wire, since a tombstone that
// decoded as an item would resurrect what a caller forgot.
func TestGivenMemoryRecordsWhenRoundTrippedThenShouldKeepTheirVariant(t *testing.T) {
	conversation := "01J0Y1ZAG00000000000000002"
	records := map[string]wire.MemoryRecord{
		"item":     wire.RememberItem("mem-1", "fact", []byte("auth is slow")),
		"forget":   wire.ForgetItem("mem-1"),
		"feedback": wire.FeedbackForItem("mem-1", 1.5),
	}
	scoped := wire.ForgetItem("mem-1")
	scoped.Conversation = &conversation
	records["scoped forget"] = scoped

	for name, record := range records {
		encoded, err := wire.Marshal(record)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.MemoryRecord
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back.Kind != record.Kind {
			t.Errorf("%s: variant changed from %d to %d", name, record.Kind, back.Kind)
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

// A key record gates signature verification, so its widths are checked and a
// revoked or out-of-window key is never usable.
func TestGivenAKeyRecordWhenValidatedThenShouldEnforceWidthsAndWindow(t *testing.T) {
	record := wire.NewKeyRecord("planner",
		make([]byte, wire.KeyIDBytes), make([]byte, wire.VerifyingKeyBytes), wire.KeyAgent)
	if err := record.Validate(); err != nil {
		t.Fatalf("a well-formed record must validate: %v", err)
	}

	short := record
	short.KeyID = []byte{1}
	if err := short.Validate(); err == nil {
		t.Error("a short key id must be refused")
	}
	shortKey := record
	shortKey.VerifyingKey = []byte{1}
	if err := shortKey.Validate(); err == nil {
		t.Error("a short public key must be refused")
	}
	anonymous := record
	anonymous.Principal = ""
	if err := anonymous.Validate(); err == nil {
		t.Error("an empty principal must be refused")
	}

	// The validity window is half-open and revocation wins outright.
	record.ValidFromMicros = 100
	until := uint64(200)
	record.ValidToMicros = &until
	if record.ValidAt(99) {
		t.Error("a key is not valid before its window")
	}
	if !record.ValidAt(100) {
		t.Error("a key is valid at the start of its window")
	}
	if record.ValidAt(200) {
		t.Error("a key is not valid at the end of its window")
	}
	revoked := record
	revoked.Revoked = true
	if revoked.ValidAt(150) {
		t.Error("a revoked key is never valid")
	}
}

// The change feed is how a consumer learns a view advanced, so its window must
// survive the wire exactly.
func TestGivenAChangeRecordWhenRoundTrippedThenShouldKeepItsWindow(t *testing.T) {
	record := wire.ChangeRecord{
		V: wire.ChangeOpVersion, Index: "readings_v1", PartitionID: 3,
		FromOffset: 100, ToOffset: 141, Rows: 42,
	}
	encoded, err := wire.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.ChangeRecord
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != record {
		t.Fatalf("round trip changed the record: %+v", back)
	}
	if back.Stream != nil {
		t.Error("an unset stream must stay absent")
	}
}

// A fold snapshot's offsets ride as compact arrays and drive where a fold
// resumes, so an unsorted or duplicated entry makes the resume ambiguous.
func TestGivenAFoldSnapshotWhenReadThenShouldResumePastItsOffsets(t *testing.T) {
	snapshot := wire.FoldSnapshot{
		Stream:       "agents",
		Fold:         "planner",
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		AsOf: []wire.SnapshotOffset{
			wire.NewSnapshotOffset(2, 20, 0, 41),
			wire.NewSnapshotOffset(2, 20, 1, 9),
		},
		State: []byte("folded"),
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("a sorted snapshot must validate: %v", err)
	}

	// Resume is one past the folded offset, and an unknown source starts at 0.
	if got := snapshot.ResumeOffset(2, 20, 0); got != 42 {
		t.Errorf("want to resume at 42, got %d", got)
	}
	if got := snapshot.ResumeOffset(2, 20, 9); got != 0 {
		t.Errorf("an unnamed partition resumes at 0, got %d", got)
	}
	// A different topic incarnation is a different source entirely.
	if got := snapshot.ResumeOffset(2, 21, 0); got != 0 {
		t.Errorf("another incarnation resumes at 0, got %d", got)
	}

	unsorted := snapshot
	unsorted.AsOf = []wire.SnapshotOffset{
		wire.NewSnapshotOffset(2, 20, 1, 9),
		wire.NewSnapshotOffset(2, 20, 0, 41),
	}
	if err := unsorted.Validate(); err == nil {
		t.Error("unsorted offsets must be refused")
	}
	duplicated := snapshot
	duplicated.AsOf = []wire.SnapshotOffset{
		wire.NewSnapshotOffset(2, 20, 0, 41),
		wire.NewSnapshotOffset(2, 20, 0, 42),
	}
	if err := duplicated.Validate(); err == nil {
		t.Error("a duplicated source must be refused")
	}
	anonymous := snapshot
	anonymous.Fold = ""
	if err := anonymous.Validate(); err == nil {
		t.Error("a snapshot must name its fold")
	}

	// The compact array form must survive the wire.
	encoded, err := wire.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.FoldSnapshot
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back.AsOf) != 2 || back.AsOf[0] != snapshot.AsOf[0] {
		t.Fatalf("the offsets must survive: %+v", back.AsOf)
	}
	again, err := wire.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(again) != string(encoded) {
		t.Fatal("a snapshot must re-encode identically")
	}
}

// The Arrow gate admits only a frozen subset, so both the policy and the
// per-message metadata are pinned against the corpus and their bounds checked.
func TestGivenArrowFixturesWhenDecodedThenShouldReEncodeAndValidate(t *testing.T) {
	t.Run("arrow_ipc_metadata.bin", func(t *testing.T) {
		metadata := roundTripFixture[wire.ArrowIpcMessageMetadata](t, "arrow_ipc_metadata.bin")
		if err := metadata.Validate(); err != nil {
			t.Fatalf("the corpus metadata must validate: %v", err)
		}

		for name, mutate := range map[string]func(*wire.ArrowIpcMessageMetadata){
			"no bytes":          func(m *wire.ArrowIpcMessageMetadata) { m.EncodedBytes = 0 },
			"oversized bytes":   func(m *wire.ArrowIpcMessageMetadata) { m.EncodedBytes = wire.MaxArrowIpcMessageBytes + 1 },
			"no fields":         func(m *wire.ArrowIpcMessageMetadata) { m.FieldCount = 0 },
			"too many fields":   func(m *wire.ArrowIpcMessageMetadata) { m.FieldCount = wire.MaxArrowIpcFields + 1 },
			"no batches":        func(m *wire.ArrowIpcMessageMetadata) { m.RecordBatchCount = 0 },
			"too many batches":  func(m *wire.ArrowIpcMessageMetadata) { m.RecordBatchCount = wire.MaxArrowIpcBatches + 1 },
			"too many rows":     func(m *wire.ArrowIpcMessageMetadata) { m.RowCount = wire.MaxArrowIpcRows + 1 },
			"too many dicts":    func(m *wire.ArrowIpcMessageMetadata) { m.DictionaryCount = wire.MaxArrowIpcDictionaries + 1 },
			"wrong version":     func(m *wire.ArrowIpcMessageMetadata) { m.ContractVersion = 2 },
			"short fingerprint": func(m *wire.ArrowIpcMessageMetadata) { m.SchemaFingerprint = wire.SchemaFingerprint{Bytes: []byte{1}} },
		} {
			altered := metadata
			mutate(&altered)
			if err := altered.Validate(); err == nil {
				t.Errorf("%s must be refused", name)
			}
		}
	})

	t.Run("arrow_ipc_policy.bin", func(t *testing.T) {
		policy := roundTripFixture[wire.ArrowIpcPolicy](t, "arrow_ipc_policy.bin")
		if err := policy.Validate(); err != nil {
			t.Fatalf("the corpus policy must validate: %v", err)
		}
		if policy != wire.DefaultArrowIpcPolicy() {
			t.Fatalf("the corpus policy is the frozen default, got %+v", policy)
		}

		// The policy is frozen: anything outside the supported subset is
		// refused rather than best-effort decoded.
		for name, mutate := range map[string]func(*wire.ArrowIpcPolicy){
			"dictionary deltas":        func(p *wire.ArrowIpcPolicy) { p.DictionaryDeltas = true },
			"replacement dictionaries": func(p *wire.ArrowIpcPolicy) { p.ReplacementDictionaries = true },
			"unions":                   func(p *wire.ArrowIpcPolicy) { p.Unions = true },
			"extension types":          func(p *wire.ArrowIpcPolicy) { p.ExtensionTypes = true },
			"file format":              func(p *wire.ArrowIpcPolicy) { p.StreamFormatOnly = false },
			"not self contained":       func(p *wire.ArrowIpcPolicy) { p.SelfContained = false },
			"narrower decimals":        func(p *wire.ArrowIpcPolicy) { p.MaxDecimalBits = 64 },
		} {
			altered := policy
			mutate(&altered)
			if err := altered.Validate(); err == nil {
				t.Errorf("%s must be refused by the frozen policy", name)
			}
		}
	})
}

// A fork id is inlined into a copy-on-write query as an identifier, so the
// charset is a strict safelist rather than a length bound.
func TestGivenForkFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("fork_create.bin", func(t *testing.T) {
		create := roundTripFixture[wire.ForkCreate](t, "fork_create.bin")
		// A client never stamps the freeze points, so an ordinary create
		// leaves them absent for the server that logs the request.
		if len(create.Points) != 0 {
			t.Error("a client-written create names no trunk positions")
		}
	})
	t.Run("fork_create_points.bin", func(t *testing.T) {
		create := roundTripFixture[wire.ForkCreate](t, "fork_create_points.bin")
		// A severed fork is frozen at the positions the logging server
		// stamped, so every replica folds the same fork rather than each
		// reading its own trunk head.
		if len(create.Points) == 0 {
			t.Fatal("the severed fixture names the positions it is frozen at")
		}
		if create.Points[0].Table == "" {
			t.Error("a freeze point names the table it belongs to")
		}
	})
	t.Run("fork_put.bin", func(t *testing.T) {
		roundTripFixture[wire.ForkPut](t, "fork_put.bin")
	})
	t.Run("fork_reply_created.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.ForkReply](t, "fork_reply_created.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.ForkCreated {
			t.Fatalf("want a created outcome, got %+v", reply)
		}
		if reply.Outcome.Info == nil || reply.Outcome.Info.ForkID == "" {
			t.Error("a created fork names itself")
		}
	})
}

func TestGivenForkIDsWhenValidatedThenShouldEnforceTheSafelist(t *testing.T) {
	for _, good := range []string{"experiment-2026-q2", "run_7.v2", "stream:orders/run_7"} {
		if err := wire.ValidateForkID(good); err != nil {
			t.Errorf("%q must be accepted: %v", good, err)
		}
	}
	for name, bad := range map[string]string{
		"empty":         "",
		"space":         "bad id",
		"sql metachars": "o'brien; drop table",
		"slash":         "a/b",
		"quote":         `a"b`,
		"empty local":   "stream:orders/",
		"over cap":      string(make([]byte, wire.MaxForkIDBytes+1)),
	} {
		if err := wire.ValidateForkID(bad); err == nil {
			t.Errorf("%s: %q must be refused", name, bad)
		}
	}
}

// The safelist is shared by every caller-chosen name that reaches a query.
func TestGivenASafelistedNameWhenValidatedThenShouldAllowOnlyTheSafeCharset(t *testing.T) {
	if err := wire.ValidateSafelistedName("name", "a-b_c.1", 64); err != nil {
		t.Errorf("the safe charset must pass: %v", err)
	}
	for _, bad := range []string{"", "a b", "a/b", "a;b", "a'b", "héllo"} {
		if err := wire.ValidateSafelistedName("name", bad, 64); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
	if err := wire.ValidateSafelistedName("name", "aaa", 2); err == nil {
		t.Error("a name past its cap must be refused")
	}
}
