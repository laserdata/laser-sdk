package laser_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// stateServer is a session lane a test can read and write.
type stateServer struct {
	clusterOfOne
	mutex sync.Mutex
	// lane is every record on agent.sessions, in order.
	lane []iggcon.IggyMessage
	// session is the conversation the written records carry.
	session wire.ConversationID
	// fail refuses the next append.
	fail error
	// failAfterAppend commits the next append and then refuses it, which is
	// what a lost acknowledgment looks like to a writer.
	failAfterAppend bool
	// polls counts the lane reads, so a test can tell a cached cursor from
	// one that reseeds every write.
	polls int
}

func (server *stateServer) Close() error { return nil }

func (server *stateServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	return &iggcon.StreamDetails{}, nil
}

func (server *stateServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	return &iggcon.TopicDetails{
		Topic:      iggcon.Topic{PartitionsCount: 1},
		Partitions: []iggcon.PartitionContract{{Id: 0}},
	}, nil
}

func (server *stateServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.polls++
	var out []iggcon.IggyMessage
	for _, message := range server.lane {
		if message.Header.Offset >= strategy.Value &&
			uint32(len(out)) < options.Count {
			out = append(out, message)
		}
	}
	return &iggcon.PolledMessage{Messages: out}, nil
}

func (server *stateServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		failure := server.fail
		server.fail = nil
		return nil, failure
	}
	for _, message := range messages {
		message.Header.Offset = uint64(len(server.lane))
		server.lane = append(server.lane, message)
	}
	if server.failAfterAppend {
		server.failAfterAppend = false
		return nil, errRefused{}
	}
	return &iggcon.SendMessagesResponse{}, nil
}

// appendRecord puts one envelope on the lane without going through a writer.
func (server *stateServer) appendRecord(
	t *testing.T, operation string, body []byte,
) {
	t.Helper()
	message := laneRecord(t, server.session, wire.AgentEnvelope{
		Kind:      wire.KindEvent,
		Source:    agentID(t, "rival"),
		Operation: &operation,
		Body:      body,
	})
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.lane))
	server.lane = append(server.lane, message)
}

// reads is how many lane reads the server answered.
func (server *stateServer) reads() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.polls
}

// deltas reads back the state deltas on the lane, in order.
func (server *stateServer) deltas(t *testing.T) []wire.StateDelta {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var written []wire.StateDelta
	for _, message := range server.lane {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			t.Fatalf("decode the envelope: %v", err)
		}
		if envelope.Operation == nil ||
			*envelope.Operation != wire.OperationStateDelta {
			continue
		}
		var delta wire.StateDelta
		if err := wire.Unmarshal(envelope.Body, &delta); err != nil {
			t.Fatalf("decode the delta: %v", err)
		}
		written = append(written, delta)
	}
	return written
}

// contentTypes lists the stamped content type of each record on the lane.
func (server *stateServer) contentTypes(t *testing.T) []wire.ContentType {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var stamped []wire.ContentType
	for _, message := range server.lane {
		entries, err := iggcon.DeserializeHeaders(message.UserHeaders)
		if err != nil {
			t.Fatalf("decode the headers: %v", err)
		}
		for _, entry := range entries {
			if string(entry.Key.Value) != wire.HeaderContentType ||
				len(entry.Value.Value) != 1 {
				continue
			}
			if contentType, named := wire.ContentTypeFromCode(
				entry.Value.Value[0]); named {
				stamped = append(stamped, contentType)
			}
		}
	}
	return stamped
}

// written is how many records reached the lane.
func (server *stateServer) written() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return len(server.lane)
}

// stateOperations lists the operation of each record on the lane.
func (server *stateServer) stateOperations(t *testing.T) []string {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var operations []string
	for _, message := range server.lane {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			t.Fatalf("decode the envelope: %v", err)
		}
		if envelope.Operation != nil {
			operations = append(operations, *envelope.Operation)
		}
	}
	return operations
}

func stateSession(t *testing.T, server *stateServer) *laser.Session {
	t.Helper()
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	return handle.Sessions().Open(server.session, "").As(agentID(t, "writer"))
}

// text is a DynValue holding one string.
func text(value string) wire.DynValue {
	return wire.NewDynValue(cborform.Text(value))
}

// document builds a DynValue object from ordered key and value pairs.
func document(pairs ...any) wire.DynValue {
	entries := cborform.Map{}
	for index := 0; index+1 < len(pairs); index += 2 {
		entries = append(entries, cborform.Entry{
			Key:   cborform.Text(pairs[index].(string)),
			Value: pairs[index+1].(wire.DynValue).Item(),
		})
	}
	return wire.NewDynValue(entries)
}

// field reads one top-level key of a folded document.
func field(t *testing.T, view wire.SessionStateView, key string) (string, bool) {
	t.Helper()
	value, found := view.Document.Field(key)
	if !found {
		return "", false
	}
	held, ok := value.Text()
	if !ok {
		t.Fatalf("%s is not text", key)
	}
	return held, true
}

func TestGivenAKeyWhenSetThenShouldFoldIntoTheDocument(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	if _, err := state.Set(context.Background(), "status", text("working")); err != nil {
		t.Fatalf("set: %v", err)
	}

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, found := field(t, view, "status"); !found || value != "working" {
		t.Fatalf("got %q %v", value, found)
	}
	if view.Revision != 1 {
		t.Fatalf("one delta is one revision, got %d", view.Revision)
	}
	if !view.Complete {
		t.Fatal("a document written from its beginning is complete")
	}
	if view.HeadOpID == nil {
		t.Fatal("the fold names the delta that reached the revision")
	}
}

func TestGivenAKeyWithPointerCharactersWhenSetThenShouldEscapeThem(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	// A slash and a tilde are the two characters a JSON pointer spells out.
	// Left raw, "a/b" would name a nested path that does not exist.
	if _, err := state.Set(context.Background(), "a/b~c", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, found := field(t, view, "a/b~c"); !found || value != "v" {
		t.Fatalf("got %q %v", value, found)
	}
}

func TestGivenSeveralSetsWhenFoldedThenShouldApplyInLaneOrder(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	for _, value := range []string{"one", "two", "three"} {
		if _, err := state.Set(context.Background(), "step", text(value)); err != nil {
			t.Fatalf("set %s: %v", value, err)
		}
	}

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "step"); value != "three" {
		t.Fatalf("the last write stands, got %q", value)
	}
	if view.Revision != 3 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenADocumentWhenReplacedThenShouldDropTheKeysItOmits(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "keep", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := state.Set(context.Background(), "drop", text("b")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, err := state.Replace(context.Background(),
		document("keep", text("c"), "add", text("d"))); err != nil {
		t.Fatalf("replace: %v", err)
	}

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "keep"); value != "c" {
		t.Fatalf("got %q", value)
	}
	if value, _ := field(t, view, "add"); value != "d" {
		t.Fatalf("got %q", value)
	}
	if _, found := field(t, view, "drop"); found {
		t.Fatal("a key the replacement omits must be gone")
	}
}

func TestGivenSomethingThatIsNotAnObjectWhenReplacedThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "keep", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}
	written := server.written()

	_, err := state.Replace(context.Background(), text("not an object"))
	if err == nil || !strings.Contains(err.Error(), "a state document is a JSON object") {
		t.Fatalf("got %v", err)
	}
	// Read as an object with no fields it would have removed every key the
	// document holds, which is the opposite of refusing it.
	if server.written() != written {
		t.Fatal("nothing must be written")
	}
	view, readErr := state.Get(context.Background())
	if readErr != nil {
		t.Fatalf("get: %v", readErr)
	}
	if value, found := field(t, view, "keep"); !found || value != "a" {
		t.Fatalf("the document must stand, got %q %v", value, found)
	}
}

func TestGivenAnEmptyPatchWhenWrittenThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	_, err := state.Patch(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "operations") {
		t.Fatalf("a patch with no operations changes nothing, got %v", err)
	}
	_, err = state.Replace(context.Background(), document())
	if err == nil || !strings.Contains(err.Error(), "changes nothing") {
		t.Fatalf("got %v", err)
	}
	if server.written() != 0 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenAFailedWriteWhenTheNextOneRunsThenShouldReseedFromTheLane(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "first", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// The append failed, so the lane revision this handle would name next is
	// unknown. Naming it anyway would write a delta against a revision
	// nothing reached.
	server.mutex.Lock()
	server.fail = errRefused{}
	server.mutex.Unlock()
	if _, err := state.Set(context.Background(), "second", text("b")); err == nil {
		t.Fatal("the refused append must be reported")
	}

	if _, err := state.Set(context.Background(), "third", text("c")); err != nil {
		t.Fatalf("the next write must reseed and succeed: %v", err)
	}
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "third"); value != "c" {
		t.Fatalf("got %q", value)
	}
	if _, found := field(t, view, "second"); found {
		t.Fatal("the refused write must not appear")
	}
	if view.Revision != 2 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

// errRefused is an append the server turned away.
type errRefused struct{}

func (errRefused) Error() string { return "refused" }

func TestGivenAStaleHandleWhenSnapshottingThenShouldRefuseRatherThanOverwrite(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)
	state := session.State()
	if _, err := state.Set(context.Background(), "mine", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Another writer moved the session on. This handle's document is no
	// longer the session's, so writing it whole would undo their change.
	another, err := wire.Marshal(wire.StateDelta{
		BaseRevision: 1,
		Patch:        []wire.PatchOperation{wire.AddOperation("/theirs", text("b"))},
		OpID:         "01ARZ3NDEKTSV4RRFFQ69G5FAV",
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.appendRecord(t, wire.OperationStateDelta, another)

	_, err = state.Snapshot(context.Background())
	var failure *laser.SessionError
	if err == nil || !strings.Contains(err.Error(), "changed since") {
		t.Fatalf("got %v", err)
	}
	if !asSessionError(err, &failure) || failure.Failure.Kind != wire.SessionStale {
		t.Fatalf("want a stale session error, got %T: %v", err, err)
	}
	// The rival's change still stands.
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "theirs"); value != "b" {
		t.Fatalf("got %q", value)
	}
}

func TestGivenAHandleThatHoldsTheDocumentWhenSnapshottingThenShouldWriteIt(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "status", text("working")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if _, err := state.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	operations := server.stateOperations(t)
	if len(operations) != 2 || operations[1] != wire.OperationStateSnapshot {
		t.Fatalf("got %v", operations)
	}
	// Folding the snapshot plus the delta before it reaches the same
	// document at the same revision.
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "status"); value != "working" {
		t.Fatalf("got %q", value)
	}
	if view.Revision != 1 {
		t.Fatalf("a snapshot is a checkpoint, not a revision, got %d", view.Revision)
	}
}

func TestGivenAFreshSessionWhenSnapshottingThenShouldWriteTheEmptyDocument(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	// An unseeded handle holds the current document by construction: there
	// is nothing yet for it to be stale against.
	if _, err := state.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if server.written() != 1 {
		t.Fatalf("got %d records", server.written())
	}
}

func TestGivenNoWriterWhenAStateIsReadThenShouldBeEmptyAndIncomplete(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if view.Revision != 0 {
		t.Fatalf("got %d", view.Revision)
	}
	// A session nobody wrote state for is not a session whose beginning was
	// removed by retention, and the two must not read the same.
	if view.Complete {
		t.Fatal("an unwritten document is not a complete one")
	}
}

func TestGivenASessionWithNoAgentWhenStateIsWrittenThenShouldSaySo(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	state := handle.Sessions().Open(server.session, "").State()

	if _, err := state.Set(context.Background(), "status", text("x")); err == nil {
		t.Fatal("a session writes as an agent")
	}
}

// asSessionError narrows an error to the session failure under it.
func asSessionError(err error, target **laser.SessionError) bool {
	for err != nil {
		if failure, ok := err.(*laser.SessionError); ok {
			*target = failure
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

// rivalDelta puts another writer's delta on the lane.
func rivalDelta(
	t *testing.T, server *stateServer,
	base uint64, opID string, baseOpID *string, patch ...wire.PatchOperation,
) {
	t.Helper()
	body, err := wire.Marshal(wire.StateDelta{
		BaseRevision: base, Patch: patch, OpID: opID, BaseOpID: baseOpID,
	})
	if err != nil {
		t.Fatalf("encode the delta: %v", err)
	}
	server.appendRecord(t, wire.OperationStateDelta, body)
}

// rivalSnapshot puts a snapshot on the lane.
func rivalSnapshot(
	t *testing.T, server *stateServer,
	base uint64, baseOpID *string, held wire.DynValue,
) {
	t.Helper()
	body, err := wire.Marshal(wire.StateSnapshot{
		BaseRevision: base, Document: held, BaseOpID: baseOpID,
	})
	if err != nil {
		t.Fatalf("encode the snapshot: %v", err)
	}
	server.appendRecord(t, wire.OperationStateSnapshot, body)
}

func TestGivenAStateAlreadyOnTheLaneWhenWrittenThenShouldContinueFromIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalDelta(t, server, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/theirs", text("a")))
	state := stateSession(t, server).State()

	// A handle that named revision zero here would write a delta the fold
	// skips, and its caller would never learn the write did nothing.
	if _, err := state.Set(context.Background(), "mine", text("b")); err != nil {
		t.Fatalf("set: %v", err)
	}

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "mine"); value != "b" {
		t.Fatalf("got %q", value)
	}
	if value, _ := field(t, view, "theirs"); value != "a" {
		t.Fatalf("the earlier write must stand, got %q", value)
	}
	if view.Revision != 2 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenSuccessiveWritesWhenSentThenShouldChainEachDeltaToTheLast(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	for _, key := range []string{"one", "two", "three"} {
		if _, err := state.Set(context.Background(), key, text("v")); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	// Each delta names the delta it continues from, which is what tells a
	// reader a writer that lost a race from one that did not: two writers
	// can reach the same revision, but only one reached it with this parent.
	written := server.deltas(t)
	if len(written) != 3 {
		t.Fatalf("got %d deltas", len(written))
	}
	if written[0].BaseOpID != nil {
		t.Fatalf("the first delta continues nothing, got %v", written[0].BaseOpID)
	}
	for index := 1; index < len(written); index++ {
		if written[index].BaseOpID == nil ||
			*written[index].BaseOpID != written[index-1].OpID {
			t.Fatalf("delta %d names %v, want %q",
				index, written[index].BaseOpID, written[index-1].OpID)
		}
		if written[index].BaseRevision != uint64(index) {
			t.Fatalf("delta %d names revision %d", index, written[index].BaseRevision)
		}
	}
}

func TestGivenAWriteWhoseAcknowledgmentIsLostWhenRetriedThenShouldReseed(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "first", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// The record reached the log and the acknowledgment did not. The handle
	// cannot tell that from a write that never landed, so it must treat its
	// own revision as unknown rather than name one the lane has moved past.
	server.mutex.Lock()
	server.failAfterAppend = true
	server.mutex.Unlock()
	if _, err := state.Set(context.Background(), "second", text("b")); err == nil {
		t.Fatal("the lost acknowledgment must be reported")
	}

	if _, err := state.Set(context.Background(), "third", text("c")); err != nil {
		t.Fatalf("set: %v", err)
	}
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// All three landed: the second did reach the log, and the third
	// continued from it rather than from a revision that was already gone.
	for key, want := range map[string]string{"first": "a", "second": "b", "third": "c"} {
		if value, found := field(t, view, key); !found || value != want {
			t.Fatalf("%s is %q %v", key, value, found)
		}
	}
	if view.Revision != 3 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenASeededHandleWhenWritingAgainThenShouldNotRereadTheLane(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "first", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}
	seeded := server.reads()

	if _, err := state.Set(context.Background(), "second", text("b")); err != nil {
		t.Fatalf("set: %v", err)
	}
	// A handle that re-read the lane on every write would pay a round trip
	// per key for a revision it already knows.
	if grew := server.reads() - seeded; grew != 0 {
		t.Fatalf("the second write re-read the lane %d times", grew)
	}
}

func TestGivenSixtyFourDeltasWhenWrittenThenShouldCheckpointTheDocument(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	for index := 0; index < 64; index++ {
		if _, err := state.Set(context.Background(), "step", text("v")); err != nil {
			t.Fatalf("set %d: %v", index, err)
		}
	}

	// Without a checkpoint a long-lived session's state costs more to read
	// every time it is written to.
	operations := server.stateOperations(t)
	snapshots := 0
	for _, operation := range operations {
		if operation == wire.OperationStateSnapshot {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("want one checkpoint after 64 deltas, got %d in %d records",
			snapshots, len(operations))
	}
	if operations[len(operations)-1] != wire.OperationStateSnapshot {
		t.Fatalf("the checkpoint follows the delta that triggered it, got %v",
			operations[len(operations)-1])
	}
	// The cadence restarts, so the next 63 deltas write no second one.
	for index := 0; index < 63; index++ {
		if _, err := state.Set(context.Background(), "step", text("w")); err != nil {
			t.Fatalf("set %d: %v", index, err)
		}
	}
	snapshots = 0
	for _, operation := range server.stateOperations(t) {
		if operation == wire.OperationStateSnapshot {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("the cadence must restart, got %d checkpoints", snapshots)
	}
}

func TestGivenARivalDeltaWhenHeldThenShouldBeStaleAndThenReseed(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "mine", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}
	head := server.deltas(t)[0].OpID

	// Another writer continued the chain, so this handle is a revision
	// behind with a document that is no longer the session's.
	rivalDelta(t, server, 1, "01ARZ3NDEKTSV4RRFFQ69G5FAV", &head,
		wire.AddOperation("/theirs", text("b")))

	if _, err := state.Snapshot(context.Background()); err == nil {
		t.Fatal("a handle behind the session must not write its document whole")
	}
	// Having found itself stale, the handle reseeds rather than staying
	// wrong: its next write continues from where the session actually is.
	if _, err := state.Set(context.Background(), "next", text("c")); err != nil {
		t.Fatalf("set: %v", err)
	}
	written := server.deltas(t)
	last := written[len(written)-1]
	if last.BaseRevision != 2 {
		t.Fatalf("the next delta names revision %d, want 2", last.BaseRevision)
	}
	if last.BaseOpID == nil || *last.BaseOpID != "01ARZ3NDEKTSV4RRFFQ69G5FAV" {
		t.Fatalf("the next delta continues %v, want the rival's", last.BaseOpID)
	}
	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	for key, want := range map[string]string{"mine": "a", "theirs": "b", "next": "c"} {
		if value, found := field(t, view, key); !found || value != want {
			t.Fatalf("%s is %q %v", key, value, found)
		}
	}
}

func TestGivenOnlyASnapshotWhenFoldedThenShouldBeItsDocument(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalSnapshot(t, server, 7, nil, document("held", text("a")))
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "held"); value != "a" {
		t.Fatalf("got %q", value)
	}
	if view.Revision != 7 {
		t.Fatalf("a snapshot carries the revision it replaces, got %d", view.Revision)
	}
	if !view.Complete {
		t.Fatal("a snapshot is the document's baseline")
	}
}

func TestGivenAStaleLaterSnapshotWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalSnapshot(t, server, 1, nil, document("held", text("a")))
	// A second writer's checkpoint of a document the session never reached.
	// Taking it would hand a reader a document nobody wrote.
	rivalSnapshot(t, server, 1, nil, document("held", text("wrong")))
	rivalSnapshot(t, server, 5, nil, document("held", text("alsowrong")))
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "held"); value != "a" {
		t.Fatalf("got %q", value)
	}
	if view.Revision != 1 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenADeltaWithAnotherParentWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalDelta(t, server, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/first", text("a")))
	// Same revision, different parent: this writer lost the race and its
	// delta names a revision that was reached, but not the delta that
	// reached it.
	other := "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	rivalDelta(t, server, 1, "01ARZ3NDEKTSV4RRFFQ69G5FAX", &other,
		wire.AddOperation("/lost", text("b")))
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, found := field(t, view, "lost"); found {
		t.Fatal("a delta that continues another chain must not apply")
	}
	if view.Revision != 1 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenAnotherSessionsStateOnTheLaneWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "mine", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// The lane carries every session's records, so another session's state
	// sits beside this one's and must not fold into it.
	body, err := wire.Marshal(wire.StateDelta{
		BaseRevision: 1, OpID: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Patch: []wire.PatchOperation{wire.AddOperation("/theirs", text("b"))},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	operation := wire.OperationStateDelta
	message := laneRecord(t, conversation(2), wire.AgentEnvelope{
		Kind: wire.KindEvent, Source: agentID(t, "rival"),
		Operation: &operation, Body: body,
	})
	server.mutex.Lock()
	message.Header.Offset = uint64(len(server.lane))
	server.lane = append(server.lane, message)
	server.mutex.Unlock()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, found := field(t, view, "theirs"); found {
		t.Fatal("another session's state must not fold into this one")
	}
	if view.Revision != 1 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenAStateRecordWhenWrittenThenShouldStampItAsCBOR(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	if _, err := state.Set(context.Background(), "status", text("x")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := state.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// The stamp is how a reader knows to decode the body without trying. A
	// record stamped JSON carrying CBOR would be read as neither.
	for index, stamped := range server.contentTypes(t) {
		if stamped != wire.ContentCBOR {
			t.Fatalf("record %d is stamped %v", index, stamped)
		}
	}
}

func TestGivenAnOversizedSnapshotWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalSnapshot(t, server, 1, nil, document("held", text("a")))
	// A number no JSON reader holds exactly is a number no other client can
	// read back, so a snapshot carrying one is not a baseline to fold.
	unportable := wire.NewDynValue(cborform.Map{{
		Key: cborform.Text("count"), Value: cborform.Uint(1 << 60),
	}})
	rivalSnapshot(t, server, 1, nil, unportable)
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "held"); value != "a" {
		t.Fatalf("got %q", value)
	}
}

func TestGivenAReplacementWhenBuiltThenShouldOrderItsOperations(t *testing.T) {
	server := &stateServer{}
	state := stateSession(t, server).State()
	for _, key := range []string{"beta", "alpha"} {
		if _, err := state.Set(context.Background(), key, text("v")); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	if _, err := state.Replace(context.Background(),
		document("delta", text("x"), "charlie", text("y"))); err != nil {
		t.Fatalf("replace: %v", err)
	}

	// Removals first, each alphabetical, then the adds. A replacement built
	// from a map would order differently every run, and two clients writing
	// the same replacement would write different patches.
	written := server.deltas(t)
	patch := written[len(written)-1].Patch
	want := []struct {
		op   wire.PatchOp
		path string
	}{
		{wire.PatchRemove, "/alpha"}, {wire.PatchRemove, "/beta"},
		{wire.PatchAdd, "/charlie"}, {wire.PatchAdd, "/delta"},
	}
	if len(patch) != len(want) {
		t.Fatalf("got %d operations, want %d", len(patch), len(want))
	}
	for index, expected := range want {
		if patch[index].Op != expected.op || patch[index].Path != expected.path {
			t.Fatalf("operation %d is %v %q, want %v %q",
				index, patch[index].Op, patch[index].Path, expected.op, expected.path)
		}
	}
}

func TestGivenADeltaOnAnAlreadyReachedRevisionWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	rivalDelta(t, server, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/first", text("a")))
	// A second writer starting from the same empty document. It names no
	// parent, so only the revision says it lost: revision zero was reached
	// by the delta above.
	rivalDelta(t, server, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAW", nil,
		wire.AddOperation("/lost", text("b")))
	state := stateSession(t, server).State()

	view, err := state.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, found := field(t, view, "lost"); found {
		t.Fatal("a delta on a revision already reached must not apply")
	}
	if view.Revision != 1 {
		t.Fatalf("got revision %d", view.Revision)
	}
}

func TestGivenASnapshotThisClientCannotReadWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	for _, probe := range []struct {
		name string
		body []byte
	}{
		{"a body that is not a snapshot", []byte{0xff, 0xff}},
		{"a number outside the exact range", func() []byte {
			body, err := wire.Marshal(wire.StateSnapshot{
				BaseRevision: 3,
				Document: wire.NewDynValue(cborform.Map{{
					Key: cborform.Text("count"), Value: cborform.Uint(1 << 60),
				}}),
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			return body
		}()},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &stateServer{}
			server.session = conversation(1)
			server.appendRecord(t, wire.OperationStateSnapshot, probe.body)
			state := stateSession(t, server).State()

			view, err := state.Get(context.Background())
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			// Taking it as the baseline would hand a reader a document no
			// writer wrote, at a revision nobody reached.
			if view.Revision != 0 || view.Complete {
				t.Fatalf("got %+v", view)
			}
			if _, found := view.Document.Field("count"); found {
				t.Fatal("the unreadable snapshot must not become the document")
			}
		})
	}
}
